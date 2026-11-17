"""The Python caller of the pruning pass, before it called the core.

``ReaperMixin._run_reaper_pass`` (``python/horsies/core/worker/reaper.py``)
ran ``prune_expired_partitions`` with ``StagedLoaderPublisher`` on the broker
resources' ``partition_maintenance`` database. The switch moved that call
into ``ReaperMixin._run_partition_pruning``, which returns the summary that
the reaper logs and keeps in its pruning health. The function below is the
call before the switch, with the same summary. It takes the worker as its
first parameter, so the dual-run rebinding can put it in the place of the
method.
"""

from __future__ import annotations

from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.brokers.resources import BrokerResources
from horsies.core.history.maintenance.pruning import (
    PrunePassSummary,
    prune_expired_partitions,
    summary_of_pass,
)
from horsies.core.history.reads.publisher import StagedLoaderPublisher
from horsies.core.worker.reaper import ReaperMixin


async def run_partition_pruning(
    self: ReaperMixin,
    temp_broker: PostgresBroker,
    resources: BrokerResources,
) -> PrunePassSummary:
    """One Python pass on ``partition_maintenance``, summarized."""
    return summary_of_pass(
        await prune_expired_partitions(
            resources.partition_maintenance,
            StagedLoaderPublisher(),
        )
    )
