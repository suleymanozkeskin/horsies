"""Helpers of the parent-rule tests that run on the reference copy of the
Python worker parent and its version-1 child
(``tests.dualrun.reference.worker_parent``): the version-1 wire tuple of a
run of the reference child, and the claim of a dispatch of the reference
parent (``claim_at``, ``row_claim``, as ``tests.dispatch_claim_support``
gives them for the production parent modules)."""

from __future__ import annotations

from datetime import datetime
from typing import assert_never

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

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


async def row_claim(session: AsyncSession, task_id: str) -> DispatchClaim:
    """The claim of the live row ``task_id``: its claim time and retry facts.

    The row must hold a claim time; a row without one fails the test.
    """
    row = (
        await session.execute(
            text(
                'SELECT claimed_at, retry_count, max_retries, task_options '
                'FROM horsies_tasks WHERE id = CAST(:id AS uuid)'
            ),
            {'id': task_id},
        )
    ).one()
    assert row.claimed_at is not None, f'task {task_id} holds no claim time'
    return DispatchClaim(
        claimed_at=row.claimed_at,
        retry_count=row.retry_count,
        max_retries=row.max_retries,
        task_options=row.task_options,
    )
