# pyright: reportPrivateUsage=false
"""The two worker callers of ``horsies_release_claims``, before they called the core.

``DispatchMixin._requeue_claimed_task`` (``python/horsies/core/worker/dispatch.py``)
and ``ClaimMixin._return_rows_as_isolated`` (``python/horsies/core/worker/claiming.py``)
sent ``release_claims`` (``python/horsies/core/worker/release.py``) on an
autocommit connection of the worker's SQLAlchemy session factory. The two
functions below are those method bodies. Each takes the worker as its first
parameter, so the dual-run rebinding can put it in the place of the method.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any, assert_never

from horsies.core.logging import get_logger
from horsies.core.utils.db import autocommit_connection
from horsies.core.worker.claiming import ClaimMixin, claim_time_of
from horsies.core.worker.dispatch import DispatchMixin
from horsies.core.worker.dispatch_claim import DispatchClaim
from horsies.core.worker.release import (
    HeldClaim,
    ReleaseOutcome,
    ReleasePlacement,
    release_claims,
)
from horsies.core.worker.row_class import RowClassMismatch
from horsies.core.worker.runtime import _RequeueOutcome

logger = get_logger('worker')


async def requeue_claimed_task(
    self: DispatchMixin,
    task_id: str,
    reason: str,
    claim: DispatchClaim,
) -> _RequeueOutcome:
    """Return a claimed row that no child started to the queue.

    One ``horsies_release_claims`` statement in autocommit, fenced on this
    worker and the dispatch's claim generation; the row keeps its execution
    class. ``DB_ERROR``: a database error, logged here.
    """
    held = HeldClaim(task_id=task_id, claimed_at=claim.claimed_at)
    try:
        async with self.sf() as s:
            (released,) = await release_claims(
                await autocommit_connection(s),
                worker_id=self.worker_instance_id,
                claims=(held,),
                placement=ReleasePlacement.SAME_CLASS,
            )
    except Exception as exc:
        logger.error(
            'DB error while requeueing task %s (%s): %s',
            task_id,
            reason,
            exc,
        )
        return _RequeueOutcome.DB_ERROR

    match released.outcome:
        case ReleaseOutcome.RELEASED:
            logger.warning(
                'Requeued worker-owned in-flight task %s: %s',
                task_id,
                reason,
            )
            return _RequeueOutcome.REQUEUED
        case ReleaseOutcome.NOT_HELD:
            logger.warning(
                'Failed to requeue task %s (not owned or not requeueable): %s',
                task_id,
                reason,
            )
            return _RequeueOutcome.NOT_OWNER_OR_NOT_CLAIMED
        case _ as unreachable:
            assert_never(unreachable)


async def return_rows_as_isolated(
    self: ClaimMixin,
    mismatched: Sequence[tuple[dict[str, Any], RowClassMismatch]],
) -> None:
    """Write ISOLATED on each row and return it to PENDING, in one
    ``horsies_release_claims`` statement that also wakes each queue of a
    returned row once. Database errors propagate to the caller.
    """
    if not mismatched:
        return
    async with self.sf() as s:
        released = await release_claims(
            await autocommit_connection(s),
            worker_id=self.worker_instance_id,
            claims=tuple(
                HeldClaim(task_id=str(row['id']), claimed_at=claim_time_of(row))
                for row, _ in mismatched
            ),
            placement=ReleasePlacement.ISOLATED,
        )
    for (row, mismatch), release in zip(mismatched, released, strict=True):
        match release.outcome:
            case ReleaseOutcome.RELEASED:
                disposition = 'wrote ISOLATED on the row and returned it to the queue'
            case ReleaseOutcome.NOT_HELD:
                disposition = 'the claim was lost, so the row was left as it is'
            case _ as unreachable:
                assert_never(unreachable)
        logger.warning(
            'Task %s (%s) stores execution %s, and the registered function '
            'is %s on this worker; %s',
            row['id'],
            row['task_name'],
            mismatch.row_class.value,
            mismatch.registered_class.value,
            disposition,
        )
