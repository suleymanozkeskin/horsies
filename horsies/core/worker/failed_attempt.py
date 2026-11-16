"""A failed attempt of a claimed task: one statement, no read before it.

The failure paths of a finalize, of a timeout and of a crash recovery write
through ``horsies_finish_failed_attempt``. The retry facts of the attempt
(``retry_count``, ``max_retries``, ``task_options``) come with the claim of
the dispatch (``DispatchClaim``): they do not change inside one claim
generation, because the two retry writers are the only writers of
``retry_count`` and both end the generation, and the other two are written
at enqueue. The retry policy and its delay are decided here from those facts
(``retry_request_for``). The function takes the task advisory lock and the
row lock under the full fence, with the retry count of the request as part
of the fence, and writes the attempt row with the retry or the terminal
failure.

Two claimed-row rules (``ClaimedRowRule``): a child that reported a result
held the row RUNNING (``REFUSE``, ``finish_failed_attempt``); a child that
ended without one may have ended before it started the task, so a row still
CLAIMED by this claim goes back to the queue with no attempt row (``RELEASE``,
``finish_ended_attempt``). The statement runs alone in autocommit.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Any, Final, assert_never

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection

from horsies.core.history.terminalization.move import FINISH_FAILED_ATTEMPT_FUNCTION
from horsies.core.lifecycle.commands import FailLockedTask
from horsies.core.lifecycle.fences import OwnedClaim
from horsies.core.lifecycle.outcomes import (
    AlreadyApplied,
    ClaimReleased,
    FailedAttemptOutcome,
    FailureApplied,
    LostClaim,
    RetryNotRequested,
    RetryPastDeadline,
    RetryRefusal,
    RetryScheduled,
    SourceStateConflict,
    TaskAbsent,
    decode_ended_attempt_row,
    decode_failed_attempt_row,
)
from horsies.core.logging import get_logger
from horsies.core.types.status import TaskAttemptOutcome
from horsies.core.utils.retry import (
    admitted_retry_policy,
    calculate_retry_delay,
)
from horsies.core.worker.dispatch_claim import DispatchClaim
from horsies.core.worker.shared_stop import RetryPlacement

logger = get_logger('lifecycle')

# The last argument (wake) is TRUE: the function sends the capacity wake of a
# plain task's queue after its terminal move.
FINISH_FAILED_ATTEMPT_SQL = text(f"""
    SELECT * FROM {FINISH_FAILED_ATTEMPT_FUNCTION}(
        CAST(:task_id AS uuid), :worker_id, CAST(:claimed_at AS TIMESTAMPTZ),
        :retry_kind,
        CAST(:expected_retry_count AS INTEGER),
        CAST(:retry_delay_s AS DOUBLE PRECISION),
        :retry_placement,
        :result, :error_code, :failed_reason,
        :attempt_outcome, :attempt_error_message, :attempt_failed_reason,
        :claimed_row_rule, TRUE
    )
""")


class ClaimedRowRule(Enum):
    """What the function does with a row that this claim still holds CLAIMED."""

    REFUSE = 'REFUSE'
    """The child reported a result, so it started the task: the row must be
    RUNNING, and a CLAIMED row is refused."""

    RELEASE = 'RELEASE'
    """The child ended without a result: a CLAIMED row goes back to the queue
    with no attempt row."""


RETRY_IN_SAME_CLASS: Final = 'SAME_CLASS'
"""The placement value of a retry in the row's own execution class."""

RETRY_ISOLATED: Final = 'ISOLATED'
"""The placement value of a retry that runs ISOLATED."""


# ---------------------------------------------------------------------------
# The retry policy
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class NoRetry:
    """The function writes the terminal failure."""


@dataclass(frozen=True, slots=True)
class RetryAfter:
    """The function writes the retry, unless its next attempt would start
    at or after ``good_until``.

    ``retry_count`` is the count of the claim. The delay belongs to the next
    attempt of that count, so the count is part of the fence.
    """

    retry_count: int
    delay_s: float
    placement: RetryPlacement


type RetryRequest = NoRetry | RetryAfter


def retry_request_for(
    claim: DispatchClaim,
    error_code: str | None,
    placement: RetryPlacement,
) -> RetryRequest:
    """The retry that the policy of the task asks for, with its delay. Pure.

    ``NoRetry`` when the count is spent, the options hold no policy, or the
    policy does not name ``error_code``. ``good_until`` is judged by the
    function, against the database clock, under the lock. A row that the
    claim no longer holds is refused by the fence, whatever the request.
    """
    policy = admitted_retry_policy(
        retry_count=claim.retry_count,
        max_retries=claim.max_retries,
        task_options_json=claim.task_options,
        error_code=error_code,
    )
    match policy:
        case None:
            return NoRetry()
        case dict():
            return RetryAfter(
                retry_count=claim.retry_count,
                delay_s=calculate_retry_delay(claim.retry_count + 1, policy),
                placement=placement,
            )
        case _ as unreachable:
            assert_never(unreachable)


# ---------------------------------------------------------------------------
# The command and its statement
# ---------------------------------------------------------------------------


class AttemptFailureKind(Enum):
    """The attempt outcome of a failed attempt."""

    FAILED = TaskAttemptOutcome.FAILED.value
    """The task failed: an error result, a decode failure, a timeout or a
    crash."""

    WORKER_FAILURE = TaskAttemptOutcome.WORKER_FAILURE.value
    """The worker reported a failure of its own, with a reason."""


@dataclass(frozen=True, slots=True)
class AttemptFailure:
    """What the attempt row records about the failure."""

    kind: AttemptFailureKind
    error_message: str | None
    failed_reason: str | None


@dataclass(frozen=True, slots=True)
class FinishFailedAttempt:
    """A failed attempt of a RUNNING task, for one statement.

    ``result_json``, ``error_code`` and ``failed_reason`` are the terminal
    failure of the task, as ``FailLockedTask`` carries them; ``error_code``
    is also the code of the attempt row. ``attempt`` is the rest of the
    attempt row. ``retry`` decides between the retry and the terminal
    failure.
    """

    task_id: str
    fence: OwnedClaim
    retry: RetryRequest
    result_json: str
    error_code: str | None
    failed_reason: str | None
    attempt: AttemptFailure


def finish_for(
    failure: FailLockedTask,
    *,
    claimed_at: datetime,
    retry: RetryRequest,
    attempt: AttemptFailure,
) -> FinishFailedAttempt:
    """The failed-attempt finish of a terminal failure. Pure."""
    return FinishFailedAttempt(
        task_id=failure.task_id,
        fence=OwnedClaim(worker_id=failure.fence.worker_id, claimed_at=claimed_at),
        retry=retry,
        result_json=failure.result_json,
        error_code=failure.error_code,
        failed_reason=failure.failed_reason,
        attempt=attempt,
    )


def _placement_value(placement: RetryPlacement) -> str:
    """The placement as the function reads it. Pure."""
    match placement:
        case RetryPlacement.SAME_CLASS:
            return RETRY_IN_SAME_CLASS
        case RetryPlacement.ISOLATED:
            return RETRY_ISOLATED
        case _ as unreachable:
            assert_never(unreachable)


def finish_parameters(
    command: FinishFailedAttempt,
    rule: ClaimedRowRule,
) -> dict[str, Any]:
    """The parameters of ``FINISH_FAILED_ATTEMPT_SQL``. Pure."""
    match command.retry:
        case NoRetry():
            retry: dict[str, Any] = {
                'retry_kind': 'NONE',
                'expected_retry_count': None,
                'retry_delay_s': None,
                'retry_placement': None,
            }
        case RetryAfter(retry_count=count, delay_s=delay_s, placement=placement):
            retry = {
                'retry_kind': 'RETRY',
                'expected_retry_count': count,
                'retry_delay_s': delay_s,
                'retry_placement': _placement_value(placement),
            }
        case _ as unreachable:
            assert_never(unreachable)
    return {
        'task_id': command.task_id,
        'worker_id': command.fence.worker_id,
        'claimed_at': command.fence.claimed_at,
        **retry,
        'result': command.result_json,
        'error_code': command.error_code,
        'failed_reason': command.failed_reason,
        'attempt_outcome': command.attempt.kind.value,
        'attempt_error_message': command.attempt.error_message,
        'attempt_failed_reason': command.attempt.failed_reason,
        'claimed_row_rule': rule.value,
    }


async def _finish_row(
    connection: AsyncConnection,
    command: FinishFailedAttempt,
    rule: ClaimedRowRule,
) -> dict[str, Any]:
    """Run the statement and return its one row.

    Raises:
        RuntimeError: the function returned other than one row.
        Exception: database errors, to the caller.
    """
    result = await connection.execute(
        FINISH_FAILED_ATTEMPT_SQL,
        finish_parameters(command, rule),
    )
    rows = result.mappings().all()
    if len(rows) != 1:
        raise RuntimeError(
            f'{FINISH_FAILED_ATTEMPT_FUNCTION} returned {len(rows)} rows for '
            f'task {command.task_id}; it reports exactly one outcome'
        )
    return {str(key): value for key, value in rows[0].items()}


async def finish_failed_attempt(
    connection: AsyncConnection,
    command: FinishFailedAttempt,
) -> FailedAttemptOutcome:
    """Write the failed attempt of a child that reported a result
    (``ClaimedRowRule.REFUSE``) and decode what the function reports.

    On autocommit the statement commits alone; inside a transaction it is
    the caller's to commit. A refusal writes nothing. A lost reply is an
    unknown outcome: a second call of the same command classifies a
    committed failure as ``AlreadyApplied``, and a committed retry as
    ``LostClaim`` (the retry ended the claim generation).

    Raises:
        OutcomeDecodeError: the row does not satisfy the wire contract.
        RuntimeError: the function returned other than one row.
        Exception: database errors, to the caller.
    """
    outcome = decode_failed_attempt_row(
        await _finish_row(connection, command, ClaimedRowRule.REFUSE),
    )
    _log_outcome(command, outcome)
    return outcome


async def finish_ended_attempt(
    connection: AsyncConnection,
    command: FinishFailedAttempt,
) -> FailedAttemptOutcome | ClaimReleased:
    """Write the end of an attempt whose child ended without a result
    (``ClaimedRowRule.RELEASE``) and decode what the function reports.

    A row that this claim still holds CLAIMED goes back to the queue with no
    attempt row (``ClaimReleased``); a RUNNING row gets the failed attempt,
    as ``finish_failed_attempt`` writes it. A lost reply is an unknown
    outcome: a second call classifies a committed release as a refusal
    (the release ended the claim generation).

    Raises:
        OutcomeDecodeError: the row does not satisfy the wire contract.
        RuntimeError: the function returned other than one row.
        Exception: database errors, to the caller.
    """
    outcome = decode_ended_attempt_row(
        await _finish_row(connection, command, ClaimedRowRule.RELEASE),
    )
    match outcome:
        case ClaimReleased():
            logger.debug(
                'failed attempt function=%s released task %s: the claim held it CLAIMED',
                FINISH_FAILED_ATTEMPT_FUNCTION,
                outcome.task_id,
            )
        case (
            FailureApplied()
            | RetryScheduled()
            | AlreadyApplied()
            | LostClaim()
            | SourceStateConflict()
            | TaskAbsent()
        ):
            _log_outcome(command, outcome)
        case _ as unreachable:
            assert_never(unreachable)
    return outcome


def _log_outcome(command: FinishFailedAttempt, outcome: FailedAttemptOutcome) -> None:
    """Log the outcome with the fence and the retry it was asked for.

    A write is debug-level; a retry refused for its deadline is info-level.
    A refusal or a replay is warning-level: its observation cannot be read
    again later.
    """
    match outcome:
        case FailureApplied(retry=refusal):
            _log_retry_refusal(outcome.task_id, refusal)
            level = logging.DEBUG
        case RetryScheduled():
            level = logging.DEBUG
        case AlreadyApplied() | LostClaim() | SourceStateConflict() | TaskAbsent():
            level = logging.WARNING
        case _ as unreachable:
            assert_never(unreachable)
    if not logger.isEnabledFor(level):
        return
    logger.log(
        level,
        'failed attempt function=%s outcome=%r expected_fence=%r retry=%r',
        FINISH_FAILED_ATTEMPT_FUNCTION,
        outcome,
        command.fence,
        command.retry,
    )


def _log_retry_refusal(task_id: str, refusal: RetryRefusal) -> None:
    """Log a retry that its deadline refused; a retry not asked for is quiet."""
    match refusal:
        case RetryPastDeadline(next_retry_at=next_at, good_until=deadline):
            logger.info(
                'Task %s retry skipped: next attempt at %s is not before '
                'good_until %s; the failure is terminal',
                task_id,
                next_at,
                deadline,
            )
        case RetryNotRequested():
            pass
        case _ as unreachable:
            assert_never(unreachable)
