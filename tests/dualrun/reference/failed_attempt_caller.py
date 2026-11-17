# pyright: reportPrivateUsage=false
"""The two worker callers of ``horsies_finish_failed_attempt``, before they called the core.

``FinalizeMixin._persist_failed_attempt`` (``python/horsies/core/worker/finalize.py``,
with its ``_after_failed_attempt``) and ``DispatchMixin._write_ended_attempt``
(``python/horsies/core/worker/dispatch.py``) sent ``finish_failed_attempt`` and
``finish_ended_attempt`` (``python/horsies/core/worker/failed_attempt.py``) on
an autocommit connection of the worker's SQLAlchemy session factory. The
functions below are those method bodies. Each takes the worker as its first
parameter, so the dual-run rebinding can put it in the place of the method.
"""

from __future__ import annotations

from typing import assert_never

from sqlalchemy.ext.asyncio import AsyncSession

from horsies.core.codec.error_payload import serialize_error_payload
from horsies.core.lifecycle.classify import (
    ErrorProduced,
    ResultUndecodable,
    WorkerLevelFailure,
)
from horsies.core.lifecycle.commands import FailLockedTask
from horsies.core.lifecycle.fences import OwnedClaim
from horsies.core.lifecycle.outcomes import (
    AlreadyApplied,
    CapacityWoken,
    ClaimReleased,
    FailedAttemptOutcome,
    FailureApplied,
    LostClaim,
    PhaseTwoOwed,
    RetryScheduled,
    SourceStateConflict,
    TaskAbsent,
)
from horsies.core.models.tasks import TaskError, TaskResult
from horsies.core.types.result import Err, Ok, Result
from horsies.core.utils.db import autocommit_connection
from horsies.core.worker.dispatch import (
    DispatchMixin,
    _AttemptFinished,
    _AttemptUnclaimed,
    _EndedAttemptWrite,
    _error_code_text,
)
from horsies.core.worker.dispatch_claim import DispatchClaim
from horsies.core.worker.failed_attempt import (
    AttemptFailure,
    AttemptFailureKind,
    NoRetry,
    RetryRequest,
    finish_ended_attempt,
    finish_failed_attempt,
    finish_for,
    retry_request_for,
)
from horsies.core.worker.finalize import (
    FinalizeMixin,
    _attempt_failure_of,
    _NodeFinishOwed,
    _Phase1Input,
    _phase2_of_a_replay,
)
from horsies.core.worker.runtime import _FinalizeError
from horsies.core.worker.shared_stop import RetryPlacement


async def after_failed_attempt(
    self: FinalizeMixin,
    s: AsyncSession,
    outcome: FailedAttemptOutcome,
    retry: RetryRequest,
) -> Result[_NodeFinishOwed | None, _FinalizeError]:
    """Route the outcome of a failed-attempt finish to phase 2 or to done.

    Database errors of the replay read raise.
    """
    match outcome:
        case FailureApplied(after=after):
            match after:
                case CapacityWoken():
                    return Ok(None)
                case PhaseTwoOwed(queue_name=queue_name, step=step):
                    return Ok(_NodeFinishOwed(queue_name=queue_name, facts=step))
                case _ as unreachable:
                    assert_never(unreachable)
        case RetryScheduled():
            self._wake_when_retry_due(outcome, retry)
            return Ok(None)
        case AlreadyApplied(task_id=task_id):
            return _phase2_of_a_replay(
                await self._load_persisted_terminal(task_id, session=s),
            )
        case LostClaim() | SourceStateConflict() | TaskAbsent():
            return Ok(None)
        case _ as unreachable:
            assert_never(unreachable)


async def persist_failed_attempt(
    self: FinalizeMixin,
    failure: FailLockedTask,
    report: WorkerLevelFailure | ResultUndecodable | ErrorProduced,
    *,
    claim: DispatchClaim,
    child: _Phase1Input,
) -> Result[_NodeFinishOwed | None, _FinalizeError]:
    """Phase 1 of a failed attempt: one statement in autocommit, no read
    before it. Database errors become a phase-1 error."""
    task_id = failure.task_id
    match report:
        case ErrorProduced():
            retry = retry_request_for(
                claim,
                failure.error_code,
                RetryPlacement.SAME_CLASS,
            )
        case WorkerLevelFailure() | ResultUndecodable():
            retry = NoRetry()
        case _ as unreachable:
            assert_never(unreachable)
    try:
        async with self.sf() as s:
            outcome = await finish_failed_attempt(
                await autocommit_connection(s),
                finish_for(
                    failure,
                    retry=retry,
                    attempt=_attempt_failure_of(report),
                ),
            )
            return await after_failed_attempt(self, s, outcome, retry)
    except Exception as exc:
        return Err(
            self._phase1_error(
                task_id,
                'Failed to persist terminal task state',
                exc,
                child,
            )
        )


async def write_ended_attempt(
    self: DispatchMixin,
    task_id: str,
    task_error: TaskError,
    claim: DispatchClaim,
    retry: RetryPlacement,
) -> _EndedAttemptWrite:
    """Write the end of an attempt whose child has ended: one statement in
    autocommit with the ``RELEASE`` rule. Database errors raise."""
    failure = FailLockedTask(
        task_id=task_id,
        fence=OwnedClaim(
            worker_id=self.worker_instance_id,
            claimed_at=claim.claimed_at,
        ),
        result_json=serialize_error_payload(TaskResult(err=task_error)),
        error_code=_error_code_text(task_error),
        failed_reason=None,
    )
    request = retry_request_for(claim, failure.error_code, retry)
    async with self.sf() as s:
        outcome = await finish_ended_attempt(
            await autocommit_connection(s),
            finish_for(
                failure,
                retry=request,
                attempt=AttemptFailure(
                    kind=AttemptFailureKind.FAILED,
                    error_message=task_error.message,
                    failed_reason=task_error.message,
                ),
            ),
        )
    match outcome:
        case ClaimReleased():
            return _AttemptUnclaimed()
        case (
            FailureApplied()
            | RetryScheduled()
            | AlreadyApplied()
            | LostClaim()
            | SourceStateConflict()
            | TaskAbsent()
        ):
            return _AttemptFinished(outcome=outcome, request=request)
        case _ as unreachable:
            assert_never(unreachable)
