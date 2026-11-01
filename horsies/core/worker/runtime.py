"""Shared runtime types and module-level helpers for the worker.

Split out of ``worker.py`` so the concern mixins (claim, dispatch,
finalize, retry, reaper, health, lifecycle) and the composed ``Worker``
class can share them without importing each other.
"""

from __future__ import annotations

import os
import random
from dataclasses import dataclass
from enum import Enum
from typing import Any, Optional

from horsies.core.codec.json_io import loads_json
from horsies.core.logging import get_logger
from horsies.core.models.tasks import BuiltInTaskCode
from horsies.core.types.result import is_err

logger = get_logger('worker')

# Underscore names are deliberate: these are worker-internal symbols whose
# privacy boundary is the worker package, not this module.
__all__ = [
    '_FINALIZE_STAGE_PHASE1',
    '_FINALIZE_STAGE_PHASE2',
    '_FINALIZE_STAGE_FUTURE',
    '_FINALIZE_STAGE_PARENT',
    '_FINALIZE_FUTURE_MAX_RETRIES',
    '_FINALIZE_PHASE1_MAX_RETRIES',
    '_FINALIZE_PHASE2_MAX_RETRIES',
    '_FINALIZE_PARENT_MAX_RETRIES',
    '_FINALIZE_RETRY_BASE_DELAY_S',
    '_FINALIZE_RETRY_MAX_DELAY_S',
    '_REAPER_MAX_PERMANENT_FAILURES',
    'ChildHookFailedError',
    'ChildStartFailedError',
    '_RetryBackoff',
    '_FinalizeError',
    '_RetryError',
    '_RequeueOutcome',
    '_ReaperPassState',
    '_collect_psutil_metrics',
    '_read_current_rss_mb',
    '_parse_timeout_ms',
    'MemoryBaselineExceedsThresholdError',
]

_FINALIZE_STAGE_PHASE1 = 'phase1_persist'
_FINALIZE_STAGE_PHASE2 = 'phase2_workflow'
_FINALIZE_STAGE_FUTURE = 'future'
_FINALIZE_STAGE_PARENT = 'parent_propagation'
_FINALIZE_FUTURE_MAX_RETRIES = 3
_FINALIZE_PHASE1_MAX_RETRIES = 3
_FINALIZE_PHASE2_MAX_RETRIES = 5
_FINALIZE_PARENT_MAX_RETRIES = 5
_FINALIZE_RETRY_BASE_DELAY_S = 0.5
_FINALIZE_RETRY_MAX_DELAY_S = 15.0


@dataclass
class _RetryBackoff:
    initial_ms: int
    max_ms: int
    max_attempts: int
    attempts: int = 0

    def reset(self) -> None:
        self.attempts = 0

    def can_retry(self) -> bool:
        match self.max_attempts:
            case 0:
                return True
            case _:
                return self.attempts < self.max_attempts

    def next_delay_seconds(self) -> float:
        self.attempts += 1
        exponent = max(0, self.attempts - 1)
        base_ms = min(self.max_ms, int(self.initial_ms * (2**exponent)))
        jitter_range = base_ms * 0.25
        delay_ms = base_ms + random.uniform(-jitter_range, jitter_range)
        return max(0.1, delay_ms / 1000.0)


@dataclass(frozen=True)
class _FinalizeError:
    error_code: BuiltInTaskCode | str
    message: str
    stage: str
    task_id: str
    retryable: bool = False
    data: dict[str, Any] | None = None


@dataclass(frozen=True)
class _RetryError:
    """DB failure while deciding or persisting a task retry.

    Owned by the retry concern; callers translate it into their own error
    domain at the seam (finalize -> _FinalizeError, dispatch recovery ->
    _RequeueOutcome.DB_ERROR). ``retryable`` reports whether the underlying
    failure was a transient connection error.
    """

    error_code: BuiltInTaskCode | str
    message: str
    task_id: str
    retryable: bool = False
    data: dict[str, Any] | None = None


class _RequeueOutcome(str, Enum):
    REQUEUED = 'REQUEUED'
    TERMINAL_REPLAYED = 'TERMINAL_REPLAYED'
    NOT_OWNER_OR_NOT_CLAIMED = 'NOT_OWNER_OR_NOT_CLAIMED'
    DB_ERROR = 'DB_ERROR'


# After this many consecutive permanent failures, a reaper operation is
# disabled for the process lifetime to avoid spamming logs every interval.
_REAPER_MAX_PERMANENT_FAILURES = 3


@dataclass
class _ReaperPassState:
    """Per-process reaper failure counters and retention schedule."""

    requeue_permanent_failures: int = 0
    requeue_disabled: bool = False
    mark_failed_permanent_failures: int = 0
    mark_failed_disabled: bool = False
    terminate_orphans_permanent_failures: int = 0
    terminate_orphans_disabled: bool = False
    next_orphan_task_audit_at: float = 0.0
    next_retention_cleanup_at: float = 0.0
    next_partition_maintenance_at: float = 0.0


def _parse_timeout_ms(task_options_json: Any, task_id: str) -> int | None:
    """Extract timeout_ms from a stored task_options JSON string.

    Total: any malformed payload yields None (no timeout) with a warning —
    a corrupt row must not block dispatch.
    """
    if not task_options_json or not isinstance(task_options_json, str):
        return None
    parsed = loads_json(task_options_json)
    if is_err(parsed):
        logger.warning(
            'Task %s task_options unparseable while reading timeout_ms: %s',
            task_id,
            parsed.err_value,
        )
        return None
    options = parsed.ok_value
    if not isinstance(options, dict):
        return None
    raw = options.get('timeout_ms')
    if raw is None:
        return None
    if isinstance(raw, bool) or not isinstance(raw, int) or raw < 1:
        logger.warning(
            'Task %s has invalid timeout_ms %r; ignoring',
            task_id,
            raw,
        )
        return None
    return raw


# Per-child latch so a broken RSS reader logs once, not once per task.
_rss_reader_failed_logged = False


def _read_current_rss_mb() -> Optional[float]:
    """Return the calling process's current RSS in MB, or ``None`` if unknown.

    Fail-open by contract: any measurement failure returns ``None`` (logged
    once per process) so the child worker loop treats RSS as unknown and does
    not recycle. An RSS read must never raise out of the worker loop, because
    a raise there would break the pool.

    Linux fast path reads ``/proc/self/statm`` (resident pages * page size);
    the fallback is ``psutil``. The two readers agree exactly (zero delta) on
    CPython 3.13/Linux.
    """
    global _rss_reader_failed_logged
    try:
        with open('/proc/self/statm', encoding='ascii') as statm:
            resident_pages = int(statm.read().split()[1])
        return resident_pages * os.sysconf('SC_PAGE_SIZE') / 1024 / 1024
    except (OSError, ValueError, IndexError):
        pass
    try:
        import psutil

        return psutil.Process(os.getpid()).memory_info().rss / 1024 / 1024
    except Exception as exc:
        if not _rss_reader_failed_logged:
            _rss_reader_failed_logged = True
            logger.warning(
                'Current RSS unreadable (statm + psutil both failed); memory '
                'recycle / baseline guard disabled for this process: %s',
                exc,
            )
        return None


def _collect_psutil_metrics() -> tuple[float, float, float, float]:
    """Collect process metrics. Blocking — must run in a thread.

    Returns ``(rss_mb, mem_pct, cpu_pct, children_rss_mb)`` for the calling
    (parent) worker process. ``children_rss_mb`` is the summed RSS of every
    descendant process — the executor children where task code runs. The
    parent heartbeat alone hides per-child memory growth; Heroku's R14 quota
    is measured against the whole process tree's footprint, so the children
    must be sampled to see what the dyno sees.

    Raises:
        Exception: psutil import/OS failures for the parent process propagate
            to the sole caller, ``_update_worker_state``, whose broad
            containment logs and skips the snapshot. Per-child sampling errors
            (a child that exits mid-enumeration) are caught and skipped.
    """
    import psutil

    process = psutil.Process()
    memory_info = process.memory_info()
    children_rss_bytes = 0
    for child in process.children(recursive=True):
        try:
            children_rss_bytes += child.memory_info().rss
        except psutil.Error:
            # Child exited between enumeration and sampling (or denied access);
            # drop it from this snapshot rather than failing the whole read.
            continue
    return (
        memory_info.rss / 1024 / 1024,
        process.memory_percent(),
        process.cpu_percent(interval=0.1),
        children_rss_bytes / 1024 / 1024,
    )


class ChildHookFailedError(RuntimeError):
    """A child exited because an on_child_process_start hook failed.

    Deliberately not a retryable error: the hook re-runs on every child
    start, so starting another child would loop on the same failure.
    """


class ChildStartFailedError(RuntimeError):
    """No child could start. Process-fatal.

    A worker with no way to run tasks must not keep claiming them, so this
    ends the worker and the supervisor restarts it. Tasks already running
    drain first.
    """


class ServiceLoopDiedError(RuntimeError):
    """A worker-lifetime service loop died while the worker kept claiming.

    Process-fatal by design: the service loops (claimer heartbeat,
    worker-state snapshot, ping responder, reaper) contain their own
    per-iteration errors, so an escape is a defect — and a worker whose
    heartbeat loop is dead stops renewing claim leases, whose state loop is
    dead disappears from monitoring, and whose reaper is dead contributes no
    recovery passes, all while still claiming work. Logging alone leaves
    that zombie running; escalating crashes the worker non-zero so the
    supervisor restarts it whole.
    """


class MemoryBaselineExceedsThresholdError(RuntimeError):
    """Booted child baseline RSS is at or above max_memory_per_child_mb.

    A threshold below the settled child baseline would recycle after every
    task (the child is over the limit before running any work), so this is a
    misconfiguration, not a tunable mode: the app does not fit the chosen
    per-child budget. Raised by the startup baseline guard to fail the worker
    loudly with both measured numbers rather than thrash silently.
    """
