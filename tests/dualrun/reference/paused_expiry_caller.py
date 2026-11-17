"""The paused expiry of the reaper, before it called the core.

``ReaperMixin._run_reaper_pass`` (``python/horsies/core/worker/reaper.py``)
ran ``expire_paused_workflows`` (``python/horsies/core/workflows/lifecycle.py``)
on the broker's SQLAlchemy session factory: the candidate read, then one
``horsies_workflow_expire_paused`` per candidate. ``expire_paused_workflows``
below has the signature of ``ReaperMixin._expire_paused_workflows``, so the
dual-run rebinding can put it in the place of the method.
"""

from __future__ import annotations

from datetime import timedelta
from typing import TYPE_CHECKING

from horsies.core.types.result import Err, Ok
from horsies.core.worker.reaper import ReaperMixin
from horsies.core.workflows import lifecycle

if TYPE_CHECKING:
    from horsies.core.brokers.postgres import PostgresBroker


async def expire_paused_workflows(
    self: ReaperMixin,
    temp_broker: PostgresBroker,
    *,
    older_than: timedelta,
) -> int:
    """The paused expiry on the broker's session factory.

    Raises ``AssertionError`` when the broker has no resource set for this
    loop, and what the expiry raises.
    """
    del self
    match await temp_broker.select_resources():
        case Err(loop_limit):
            raise AssertionError(f'no resources for the paused expiry: {loop_limit}')
        case Ok(resources):
            pass
    return await lifecycle.expire_paused_workflows(
        resources.session_factory,
        older_than=older_than,
    )
