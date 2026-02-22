"""Worker configuration dataclass."""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Optional

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
    processes: int = os.cpu_count() or 2
    # Claiming knobs
    # max_claim_batch: Top-level fairness limiter to prevent worker starvation in multi-worker setups.
    # Limits claims per queue per pass, regardless of available capacity. Increase for high-concurrency workloads.
    max_claim_batch: int = 2
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
    # Log level for worker processes (default: INFO)
    loglevel: int = 20  # logging.INFO
