# pyright: reportPrivateUsage=false
"""The two Python callers of the coverage pass, before they called the core.

``ReaperMixin._run_partition_coverage`` (``python/horsies/core/worker/reaper.py``)
ran ``maintain_partition_coverage`` with ``serialize_damaged_work=False`` on
the broker resources' ``partition_maintenance`` database, and
``Worker._ensure_startup_coverage`` (``python/horsies/core/worker/worker.py``)
ran ``ensure_startup_coverage_in_database`` on it (the gated pass, then the
heartbeat rule). The two functions below are those method bodies. Each takes
the worker as its first parameter, so the dual-run rebinding can put it in
the place of the method.
"""

from __future__ import annotations

from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.brokers.resources import BrokerResources
from horsies.core.history.maintenance.coverage import (
    CoverageOutcome,
    StartupCoverageRefused,
    ensure_startup_coverage_in_database,
    maintain_partition_coverage,
)
from horsies.core.models.retention import RetentionConfig
from horsies.core.worker.reaper import ReaperMixin
from horsies.core.worker.worker import Worker


async def run_partition_coverage(
    self: ReaperMixin,
    temp_broker: PostgresBroker,
    resources: BrokerResources,
    retention_cfg: RetentionConfig,
) -> CoverageOutcome:
    """The periodic pass on ``partition_maintenance``, with no coverage gate."""
    return await maintain_partition_coverage(
        resources.partition_maintenance,
        history_horizon_days=retention_cfg.history_leaf_horizon_days,
        heartbeat_horizon_hours=retention_cfg.heartbeat_leaf_horizon_hours,
        declared_classes=retention_cfg.registrable_classes(),
        serialize_damaged_work=False,
    )


async def ensure_startup_coverage(
    self: Worker,
    resources: BrokerResources,
    retention_cfg: RetentionConfig,
) -> CoverageOutcome | StartupCoverageRefused:
    """The startup pass on ``partition_maintenance``: gated, then the heartbeat rule."""
    return await ensure_startup_coverage_in_database(
        resources.partition_maintenance,
        history_horizon_days=retention_cfg.history_leaf_horizon_days,
        heartbeat_horizon_hours=retention_cfg.heartbeat_leaf_horizon_hours,
        declared_classes=retention_cfg.registrable_classes(),
    )
