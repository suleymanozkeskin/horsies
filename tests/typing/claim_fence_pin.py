"""Type pin: every claim-generation fence of the worker needs a claim time.

pyright checks this file through ``pyrightconfig.clean.json``. pytest does
not collect it (no ``test_`` prefix), and nothing imports it. Each wrong use
below leaves out the claim time or passes None, and has an ignore comment.
The config makes an unneeded ignore comment an error, so a fence, a frame or
a write that accepts a call without a claim time again fails the check.
"""

# pyright: reportPrivateUsage=false

from __future__ import annotations

import uuid
from datetime import datetime

from horsies.core.lifecycle.fences import OwnedClaim, OwnedClaimBatch
from horsies.core.worker.child_protocol import RunFrame, SeatTask, SharedRunFrame
from horsies.core.worker.child_runner import mark_run_finalizing
from horsies.core.worker.dispatch_claim import DispatchClaim
from horsies.core.worker.stored_timeout import NoTimeout
from horsies.core.worker.worker import Worker


def fences(claimed_at: datetime) -> None:
    OwnedClaim(worker_id='w', claimed_at=claimed_at)
    OwnedClaim(worker_id='w')  # pyright: ignore[reportCallIssue]
    OwnedClaim(worker_id='w', claimed_at=None)  # pyright: ignore[reportArgumentType]
    OwnedClaimBatch(worker_id='w', claim_generations=(('t', claimed_at),))
    OwnedClaimBatch('w', (('t', None),))  # pyright: ignore[reportArgumentType]


def frames(claimed_at: datetime) -> None:
    RunFrame('t', 'n', None, None, 'w', False, NoTimeout(), claimed_at, True)
    RunFrame('t', 'n', None, None, 'w', False, NoTimeout())  # pyright: ignore[reportCallIssue]
    RunFrame('t', 'n', None, None, 'w', False, NoTimeout(), None, True)  # pyright: ignore[reportArgumentType]
    SharedRunFrame(1, 't', 'n', None, None, 'w', False, claimed_at, SeatTask(1000, 100), True)
    SharedRunFrame(1, 't', 'n', None, None, 'w', False, None, SeatTask(1000, 100), True)  # pyright: ignore[reportArgumentType]


async def writes(worker: Worker, claimed_at: datetime) -> None:
    claim = DispatchClaim(
        claimed_at=claimed_at, retry_count=0, max_retries=0, task_options=None,
    )
    DispatchClaim(retry_count=0, max_retries=0, task_options=None)  # pyright: ignore[reportCallIssue]
    DispatchClaim(None, 0, 0, None)  # pyright: ignore[reportArgumentType]
    mark_run_finalizing(uuid.UUID(int=1), 'w', claimed_at)
    mark_run_finalizing(uuid.UUID(int=1), 'w', None)  # pyright: ignore[reportArgumentType]
    await worker._requeue_claimed_task('t', 'reason', claim)
    await worker._requeue_claimed_task('t', 'reason', claimed_at)  # pyright: ignore[reportArgumentType]
    await worker._requeue_claimed_task('t', 'reason')  # pyright: ignore[reportCallIssue]
    await worker._recover_worker_future_failure('t', 'reason')  # pyright: ignore[reportCallIssue]
