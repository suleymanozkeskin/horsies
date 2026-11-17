# pyright: reportPrivateUsage=false
"""The workflow retention delete of the reaper, before it called the core.

``ReaperMixin._run_reaper_pass`` (``python/horsies/core/worker/reaper.py``)
ran ``DELETE_EXPIRED_WORKFLOWS_SQL`` through ``_delete_expired_in_batches``
on the broker's SQLAlchemy session factory: one statement per batch, a
commit per batch, until a batch deleted no workflow or the pass deadline had
passed after a batch. ``delete_expired_workflows`` below has the signature of
``ReaperMixin._delete_expired_workflows``, so the dual-run rebinding can put
it in the place of the method.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from horsies.core.types.result import Err, Ok
from horsies.core.worker.reaper import ReaperMixin
from horsies.core.worker.sql import DELETE_EXPIRED_WORKFLOWS_SQL

if TYPE_CHECKING:
    from horsies.core.brokers.postgres import PostgresBroker


async def delete_expired_workflows(
    self: ReaperMixin,
    temp_broker: PostgresBroker,
    *,
    retention_hours: int,
    batch_size: int,
    deadline_monotonic: float,
) -> int:
    """The workflow retention delete on the broker's session factory.

    Raises ``AssertionError`` when the broker has no resource set for this
    loop, and what the driver raises for a batch.
    """
    match await temp_broker.select_resources():
        case Err(loop_limit):
            raise AssertionError(f'no resources for the retention delete: {loop_limit}')
        case Ok(resources):
            pass
    return await self._delete_expired_in_batches(
        resources.session_factory,
        DELETE_EXPIRED_WORKFLOWS_SQL,
        {'retention_hours': retention_hours},
        deadline_monotonic,
        batch_size,
        drained_when='empty_batch',
    )
