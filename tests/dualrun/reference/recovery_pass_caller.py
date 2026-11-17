"""The workflow recovery pass of the reaper, before it called the core.

``ReaperMixin._run_reaper_pass`` (``python/horsies/core/worker/reaper.py``)
ran ``recover_stuck_workflows_global``
(``python/horsies/core/workflows/recovery.py``) on the broker's SQLAlchemy
session factory: the case queries and the audit page, each candidate in its
own transaction with the child starts it owes (children built in Python),
then the count of workflows over the failure limit. ``recover_workflows``
below has the signature of ``ReaperMixin._recover_workflows``, so the
dual-run rebinding can put it in the place of the method.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from horsies.core.types.result import Err, Ok
from horsies.core.worker.reaper import ReaperMixin
from horsies.core.workflows.recovery import RecoveryReport, recover_stuck_workflows_global

if TYPE_CHECKING:
    from horsies.core.brokers.postgres import PostgresBroker


async def recover_workflows(
    self: ReaperMixin,
    temp_broker: PostgresBroker,
    *,
    failure_limit: int,
) -> RecoveryReport:
    """The global pass on the broker's session factory.

    Raises ``AssertionError`` when the broker has no resource set for this
    loop, and what the pass raises (``RecoveryPassFailure``).
    """
    del self
    match await temp_broker.select_resources():
        case Err(loop_limit):
            raise AssertionError(f'no resources for the recovery pass: {loop_limit}')
        case Ok(resources):
            pass
    return await recover_stuck_workflows_global(
        resources.session_factory,
        temp_broker,
        failure_limit=failure_limit,
    )
