"""Shared runtime types and module-level helpers for the worker.

Split out of ``worker.py`` so the concern mixins (claim, dispatch,
finalize, retry, reaper, health, lifecycle) and the composed ``Worker``
class can share them without importing each other.
"""

from __future__ import annotations

import os
import random
import time
from dataclasses import dataclass
from enum import Enum
from typing import Any

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
    'ExecutorRestartFailedError',
    '_RetryBackoff',
    '_FinalizeError',
    '_RetryError',
    '_RequeueOutcome',
    '_ReaperPassState',
    '_collect_psutil_metrics',
    '_parse_timeout_ms',
    '_warm_child_process',
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
    next_retention_cleanup_at: float = 0.0


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
            'Task %s has invalid timeout_ms %r; ignoring', task_id, raw,
        )
        return None
    return raw


def _collect_psutil_metrics() -> tuple[float, float, float]:
    """Collect process metrics. Blocking — must run in a thread.

    Raises:
        Exception: psutil import/OS failures propagate to the sole caller,
            ``_update_worker_state``, whose broad containment logs and skips
            the snapshot.
    """
    import psutil

    process = psutil.Process()
    memory_info = process.memory_info()
    return (
        memory_info.rss / 1024 / 1024,
        process.memory_percent(),
        process.cpu_percent(interval=0.1),
    )


class ChildHookFailedError(RuntimeError):
    """A child exited because an on_child_process_start hook failed.

    Deliberately not a retryable error: the hook re-runs on every child
    start, so restarting the executor would loop on the same failure.
    """


class ExecutorRestartFailedError(RuntimeError):
    """Replacing the process pool failed at the OS level.

    Process-fatal by design: the failure modes (fd/pid/memory/semaphore
    exhaustion, children dying during warmup) are host conditions a waiting
    worker cannot fix, and a worker without an executor only looks healthy
    (heartbeat loops keep running) while doing no work. Propagates to the
    main loop and crashes the worker so the supervisor restarts it with a
    clean slate.
    """


def _warm_child_process(delay_seconds: float = 0.1) -> int:
    """No-op child task used to force process startup before parent DB sockets open."""
    if delay_seconds > 0:
        time.sleep(delay_seconds)
    return os.getpid()
