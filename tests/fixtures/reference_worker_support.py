"""Helpers of the parent-rule tests that run on the reference copy of the
Python worker parent and its version-1 child
(``tests.dualrun.reference.worker_parent``): the version-1 wire tuple of a
run of the reference child, and the claim of a dispatch of the reference
parent."""

from __future__ import annotations

from datetime import datetime
from typing import assert_never

from tests.dualrun.reference.worker_parent.worker.child_protocol import WireResult
from tests.dualrun.reference.worker_parent.worker.dispatch_claim import DispatchClaim
from tests.dualrun.reference.worker_parent.worker.run_return import (
    RunReturn,
    SetRunning,
    StoppedBeforeRunning,
)


def wire_of(returned: RunReturn) -> WireResult:
    """The wire tuple of ``returned``, from either kind of end."""
    match returned:
        case SetRunning(wire=wire) | StoppedBeforeRunning(wire=wire):
            return wire
        case _ as unreachable:
            assert_never(unreachable)


def claim_at(claimed_at: datetime) -> DispatchClaim:
    """The claim at ``claimed_at`` of a row with retry count 0, no retries
    and no options."""
    return DispatchClaim(
        claimed_at=claimed_at,
        retry_count=0,
        max_retries=0,
        task_options=None,
    )
