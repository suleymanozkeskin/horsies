"""Type pin: every claim-generation fence of the worker needs a claim time.

pyright checks this file through ``pyrightconfig.clean.json``. pytest does
not collect it (no ``test_`` prefix), and nothing imports it. Each wrong use
below leaves out the claim time or passes None, and has an ignore comment.
The config makes an unneeded ignore comment an error, so a fence, a frame or
a write that accepts a call without a claim time again fails the check.
"""

# pyright: reportPrivateUsage=false

from __future__ import annotations

from datetime import datetime

from sqlalchemy.ext.asyncio import AsyncConnection

from horsies.core.lifecycle.fences import OwnedClaim, OwnedClaimBatch
from horsies.core.worker.child_protocol import RunFrame, SeatTask, SharedRunFrame
from horsies.core.worker.child_runner import mark_finalizing_params
from horsies.core.worker.failed_attempt import read_in_flight_attempt
from horsies.core.worker.stored_timeout import NoTimeout
from horsies.core.worker.worker import Worker


def fences(claimed_at: datetime) -> None:
    OwnedClaim(worker_id='w', claimed_at=claimed_at)
    OwnedClaim(worker_id='w')  # pyright: ignore[reportCallIssue]
    OwnedClaim(worker_id='w', claimed_at=None)  # pyright: ignore[reportArgumentType]
    OwnedClaimBatch(worker_id='w', claim_generations=(('t', claimed_at),))
    OwnedClaimBatch('w', (('t', None),))  # pyright: ignore[reportArgumentType]


def frames(claimed_at: datetime) -> None:
    RunFrame('t', 'n', None, None, 'w', False, NoTimeout(), claimed_at)
    RunFrame('t', 'n', None, None, 'w', False, NoTimeout())  # pyright: ignore[reportCallIssue]
    RunFrame('t', 'n', None, None, 'w', False, NoTimeout(), None)  # pyright: ignore[reportArgumentType]
    SharedRunFrame(1, 't', 'n', None, None, 'w', False, claimed_at, SeatTask(1000, 100))
    SharedRunFrame(1, 't', 'n', None, None, 'w', False, None, SeatTask(1000, 100))  # pyright: ignore[reportArgumentType]


async def writes(
    connection: AsyncConnection,
    worker: Worker,
    claimed_at: datetime,
) -> None:
    await read_in_flight_attempt(
        connection,
        task_id='t',
        worker_id='w',
        claimed_at=claimed_at,
    )
    await read_in_flight_attempt(connection, task_id='t', worker_id='w')  # pyright: ignore[reportCallIssue]
    mark_finalizing_params('t', 'w', claimed_at)
    mark_finalizing_params('t', 'w', None)  # pyright: ignore[reportArgumentType]
    await worker._requeue_claimed_task('t', 'reason', claimed_at)
    await worker._requeue_claimed_task('t', 'reason')  # pyright: ignore[reportCallIssue]
    await worker._recover_worker_future_failure('t', 'reason')  # pyright: ignore[reportCallIssue]
