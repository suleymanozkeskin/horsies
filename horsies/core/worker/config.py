"""Worker configuration dataclass."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Optional

from horsies.core.defaults import MAX_CLAIM_RENEW_AGE_MS

if TYPE_CHECKING:
    from horsies.core.models.recovery import RecoveryConfig
    from horsies.core.models.resilience import WorkerResilienceConfig


def _default_str_list() -> list[str]:
    return []


def _default_str_int_dict() -> dict[str, int]:
    return {}


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
    #   0: disabled — children live for the worker's lifetime (uses 'fork'
    #      on Linux).
    #   >= 2: recycle after N tasks.
    # Any positive value forces the 'spawn' start method (the stdlib value is
    # incompatible with 'fork'), including at initial startup, so children
    # re-import the app instead of fork-cloning the parent. Child warmup
    # (_warm_executor) consumes one executor call, so a child's first
    # generation runs one warmup + (max_tasks_per_child - 1) real tasks; 1
    # would exhaust a child on warmup before any task, hence the >= 2 floor.
    max_tasks_per_child: int = 100

    def __post_init__(self) -> None:
        if self.max_tasks_per_child != 0 and self.max_tasks_per_child < 2:
            raise ValueError(
                'max_tasks_per_child must be >= 2 (child warmup consumes one '
                'executor call), or 0 to disable recycling; got '
                f'{self.max_tasks_per_child}'
            )

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
