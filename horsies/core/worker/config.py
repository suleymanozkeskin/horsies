"""Worker configuration dataclass."""

from __future__ import annotations

import math
import os
from dataclasses import dataclass, field
from enum import Enum
from typing import TYPE_CHECKING, Optional

from horsies.core.defaults import MAX_CLAIM_RENEW_AGE_MS

if TYPE_CHECKING:
    from horsies.core.models.recovery import RecoveryConfig
    from horsies.core.models.retention import RetentionConfig
    from horsies.core.models.resilience import WorkerResilienceConfig


def _default_str_list() -> list[str]:
    return []


def _default_str_int_dict() -> dict[str, int]:
    return {}


def _default_connect_kwargs() -> dict[str, object]:
    return {}


class ChildPreload(str, Enum):
    """What a preload parent imports before it forks children. Linux only.

    Elsewhere, and when a preload parent is down, children start with ``exec``.
    """

    # Library code only. Each child imports the app itself.
    LIBRARY = 'library'
    # Library and app. Children share module state from the app's import.
    APP = 'app'
    # No preload parent. Every child starts with ``exec``.
    NONE = 'none'


@dataclass
class WorkerConfig:
    dsn: str  # SQLAlchemy async URL (e.g. postgresql+psycopg://...)
    psycopg_dsn: str  # plain psycopg URL for listener
    queues: list[str]  # which queues to serve
    session_dsn: str = ''  # SQLAlchemy async URL for session features
    pgbouncer_transaction_mode: bool = False
    processes: int = os.cpu_count() or 2
    parent_pool_size: int = 3
    parent_max_overflow: int = 2
    child_pool_min_size: int = 0
    child_pool_max_size: int = 2
    child_pool_check: bool = True
    # psycopg connect kwargs for each child-pool connection (TCP keepalives,
    # plus prepare_threshold=None under PgBouncer transaction mode). Built
    # from PostgresConfig.pooled_connect_args.
    child_connect_kwargs: dict[str, object] = field(
        default_factory=_default_connect_kwargs
    )
    # Claiming knobs
    # max_claim_batch: Optional top-level fairness limiter per queue per pass.
    # 0 = auto-fill available local/global capacity. Positive values explicitly cap claims.
    max_claim_batch: int = 0
    # max_claim_per_worker: Per-worker limit on total CLAIMED tasks to prevent over-claiming.
    # 0 = auto (defaults to processes). Increase for deeper prefetch if tasks start very quickly.
    max_claim_per_worker: int = 0
    coalesce_notifies: int = 100  # drain up to N notes after wake
    app_locator: str = ''  # NEW (see _locate_app)
    sys_path_roots: list[str] = field(default_factory=_default_str_list)
    imports: list[str] = field(
        default_factory=_default_str_list
    )  # modules that contain @app.task defs
    # When in CUSTOM mode, provide per-queue settings {name: {priority, max_concurrency}}
    queue_priorities: dict[str, int] = field(default_factory=_default_str_int_dict)
    queue_max_concurrency: dict[str, int] = field(default_factory=_default_str_int_dict)
    cluster_wide_cap: Optional[int] = None
    # Prefetch buffer: 0 = hard cap mode (count RUNNING + CLAIMED), >0 = soft cap with lease
    prefetch_buffer: int = 0
    # Claim lease duration in ms. Required when prefetch_buffer > 0.
    # When None, the worker applies a 60s default internally for crash-recovery safety.
    claim_lease_ms: Optional[int] = None
    # Recovery configuration from AppConfig
    recovery_config: Optional['RecoveryConfig'] = (
        None  # RecoveryConfig, avoid circular import
    )
    # Retention configuration from AppConfig. Separate from recovery
    # because it decides how long records live, not how stalled work is
    # found; the worker needs both.
    retention_config: Optional['RetentionConfig'] = (
        None  # RetentionConfig, avoid circular import
    )
    resilience_config: Optional['WorkerResilienceConfig'] = (
        None  # WorkerResilienceConfig, allow override
    )
    # Maximum CLAIMED age (ms) for heartbeat lease renewal. Tasks claimed
    # longer ago than this stop getting their lease renewed, so they expire
    # naturally and become reclaimable by another worker.
    max_claim_renew_age_ms: int = MAX_CLAIM_RENEW_AGE_MS
    # Log level for worker processes (default: INFO)
    loglevel: int = 20  # logging.INFO
    # Recycle each child process after it completes this many tasks (bounds
    # memory for workloads that retain it). Recycling is per-child and
    # staggered: each child has its own counter, so children rotate
    # independently, never in lockstep.
    #   100 (default): recycle after 100 tasks.
    #   0: disabled — children live for the worker's lifetime.
    #   >= 1: recycle after N tasks.
    max_tasks_per_child: int = 100
    # Recycle a child after its own RSS reaches this many MB (bounds retained
    # memory for leak/fragmentation-prone workloads). The child reports its
    # RSS with each result, and the parent replaces it. This is a retention
    # guardrail, not a hard peak-memory sandbox: a task may exceed the
    # threshold while it is still running.
    #   None (default): disabled.
    #   > 0: recycle threshold in MB.
    # There is no fixed lower floor here — a constant cannot know a deployment's
    # booted child baseline. The real floor is enforced at startup by the
    # baseline guard, which fails loudly when the threshold is below the
    # measured baseline.
    max_memory_per_child_mb: Optional[int] = None
    # Seconds that a stopping worker waits for running tasks. At this deadline
    # it kills the children that still run a task, and each of those tasks
    # takes the crash path. Set it below the supervisor's stop window.
    drain_timeout_s: float = 300.0
    child_preload: ChildPreload = ChildPreload.LIBRARY

    def __post_init__(self) -> None:
        if not math.isfinite(self.drain_timeout_s) or self.drain_timeout_s <= 0:
            raise ValueError(
                'drain_timeout_s must be a finite number of seconds above 0; '
                f'got {self.drain_timeout_s}'
            )
        if self.max_tasks_per_child < 0:
            raise ValueError(
                'max_tasks_per_child must be >= 1, or 0 to disable recycling; '
                f'got {self.max_tasks_per_child}'
            )
        if (
            self.max_memory_per_child_mb is not None
            and self.max_memory_per_child_mb <= 0
        ):
            raise ValueError(
                'max_memory_per_child_mb must be a positive integer (the real '
                'floor is the child baseline, enforced at startup), or '
                f'None to disable; got {self.max_memory_per_child_mb}'
            )
        # PgBouncer transaction pooling cannot carry prepared statements; the
        # child pool must disable them. The CLI builds child_connect_kwargs
        # from PostgresConfig.pooled_connect_args (which already sets this under
        # transaction mode), but a direct WorkerConfig construction may pass
        # only the boolean. Enforce the invariant here so the flag alone is
        # always sufficient, without overriding an explicit value.
        if self.pgbouncer_transaction_mode:
            kwargs = dict(self.child_connect_kwargs)
            kwargs.setdefault('prepare_threshold', None)
            self.child_connect_kwargs = kwargs

    def __repr__(self) -> str:
        """Mask credential-bearing DSNs; the dataclass auto-repr put all
        three in cleartext, one logger.debug('%s', cfg) away from a leak."""
        from horsies.core.utils.url import mask_database_url

        masked = {
            'dsn': mask_database_url(self.dsn),
            'psycopg_dsn': mask_database_url(self.psycopg_dsn),
            'session_dsn': (
                mask_database_url(self.session_dsn) if self.session_dsn else ''
            ),
        }
        public_fields = {
            'queues': self.queues,
            'processes': self.processes,
            'parent_pool_size': self.parent_pool_size,
            'parent_max_overflow': self.parent_max_overflow,
            'prefetch_buffer': self.prefetch_buffer,
            'cluster_wide_cap': self.cluster_wide_cap,
        }
        parts = [f'{k}={v!r}' for k, v in {**masked, **public_fields}.items()]
        return f'WorkerConfig({", ".join(parts)})'
