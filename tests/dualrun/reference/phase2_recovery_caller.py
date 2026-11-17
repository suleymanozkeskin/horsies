"""The phase-2 recovery of the reaper, before it called the core.

``ReaperMixin._run_reaper_pass`` (``python/horsies/core/worker/reaper.py``)
ran ``drive_phase2_recovery`` (``python/horsies/core/workflows/phase2_recovery.py``)
on the broker's SQLAlchemy session factory: discovery, then one transaction
per record (``horsies_workflow_phase2_recover`` with the child starts it
owes, children built in Python), the parent advances, and the count of
persistent raises. ``recover_phase2`` below has the signature of
``ReaperMixin._recover_phase2``, so the dual-run rebinding can put it in the
place of the method.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

from horsies.core.types.result import Err, Ok
from horsies.core.worker.reaper import ReaperMixin
from horsies.core.workflows.phase2_recovery import (
    Phase2RecoverySummary,
    drive_phase2_recovery,
)

if TYPE_CHECKING:
    from horsies.core.brokers.postgres import PostgresBroker


async def recover_phase2(
    self: ReaperMixin,
    temp_broker: PostgresBroker,
    *,
    grace_ms: int,
    max_rows: int,
    quarantine_after_attempts: int,
) -> Phase2RecoverySummary:
    """The phase-2 recovery pass on the broker's session factory.

    Raises ``AssertionError`` when the broker has no resource set for this
    loop, and what the pass raises.
    """
    del self
    match await temp_broker.select_resources():
        case Err(loop_limit):
            raise AssertionError(f'no resources for the phase-2 recovery: {loop_limit}')
        case Ok(resources):
            pass
    return await drive_phase2_recovery(
        resources.session_factory,
        temp_broker,
        grace_ms=grace_ms,
        max_rows=max_rows,
        quarantine_after_attempts=quarantine_after_attempts,
    )
