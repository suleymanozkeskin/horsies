"""A failed attempt of a RUNNING task: one unlocked read, one statement.

The failure paths of a finalize, of a timeout and of a crash recovery write
through ``horsies_finish_failed_attempt``. The caller first reads the retry
facts of the attempt (``read_in_flight_attempt``), without a lock and fenced
on the claim generation. ``retry_count``, ``max_retries`` and
``task_options`` do not change inside one claim generation: the two retry
schedulers are the only writers of ``retry_count`` and both end the
generation, and the other two are written at enqueue. The retry policy and
its delay are decided here from those facts (``retry_request_for``). The
function then takes the task advisory lock and the row lock under the full
fence, with the retry count that was read as part of the fence, and writes
the attempt row with the retry or the terminal failure.

Both statements run in autocommit (``autocommit_connection``). Each is atomic
alone, and the fence is the only link between them.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Any, Final, assert_never

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncSession

from horsies.core.history.terminalization.move import (
    FINISH_FAILED_ATTEMPT_FUNCTION,
    RETRY_IN_SAME_CLASS,
    RETRY_ISOLATED,
)
from horsies.core.lifecycle.commands import FailLockedTask
from horsies.core.lifecycle.fences import OwnedClaim
from horsies.core.lifecycle.outcomes import (
    AlreadyApplied,
    FailedAttemptOutcome,
    FailureApplied,
    LostClaim,
    RetryNotRequested,
    RetryPastDeadline,
    RetryRefusal,
    RetryScheduled,
    SourceStateConflict,
    TaskAbsent,
    decode_failed_attempt_row,
)
from horsies.core.logging import get_logger
from horsies.core.types.status import TaskAttemptOutcome
from horsies.core.utils.retry import (
    admitted_retry_policy,
    calculate_retry_delay,
)
from horsies.core.worker.shared_stop import RetryPlacement
from horsies.core.worker.sql import READ_IN_FLIGHT_ATTEMPT_SQL

logger = get_logger('lifecycle')

_AUTOCOMMIT: Final[dict[str, Any]] = {'isolation_level': 'AUTOCOMMIT'}

FINISH_FAILED_ATTEMPT_SQL = text(f"""
    SELECT * FROM {FINISH_FAILED_ATTEMPT_FUNCTION}(
        CAST(:task_id AS uuid), :worker_id, CAST(:claimed_at AS TIMESTAMPTZ),
        CAST(:expected_retry_count AS INTEGER),
        CAST(:retry_delay_s AS DOUBLE PRECISION),
        :retry_placement,
        :result, :error_code, :failed_reason,
        :attempt_outcome, :attempt_error_message, :attempt_failed_reason
    )
""")


# ---------------------------------------------------------------------------
# The unlocked read
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class RunningAttempt:
    """The row is RUNNING under this claim generation: the retry facts."""

    retry_count: int
    max_retries: int
    task_options: str | None


@dataclass(frozen=True, slots=True)
class ClaimedAttempt:
    """The row is CLAIMED under this claim generation: no task code ran."""


@dataclass(frozen=True, slots=True)
class AttemptNotHeld:
    """No CLAIMED or RUNNING row of this claim generation."""


type InFlightAttempt = RunningAttempt | ClaimedAttempt | AttemptNotHeld


async def autocommit_connection(session: AsyncSession) -> AsyncConnection:
    """The connection of a session that has not begun, in autocommit.

    Each statement on it commits alone: no BEGIN and no COMMIT round trip.
    The session close sends nothing, and the pool gives the connection back
    to the next session in transaction mode. Use it only for statements that
    are each complete alone.
    """
    return await session.connection(execution_options=_AUTOCOMMIT)


async def read_in_flight_attempt(
    connection: AsyncConnection,
    *,
    task_id: str,
    worker_id: str,
    claimed_at: datetime | None,
) -> InFlightAttempt:
    """Read the row of this claim generation, without a lock.

    ``claimed_at`` None disables the generation half of the fence. Writes
    nothing. Database errors raise to the caller.
    """
    row = (
        await connection.execute(
            READ_IN_FLIGHT_ATTEMPT_SQL,
            {'id': task_id, 'wid': worker_id, 'claimed_at': claimed_at},
        )
    ).fetchone()
    if row is None:
        return AttemptNotHeld()
    match row.status:
        case 'RUNNING':
            return RunningAttempt(
                retry_count=int(row.retry_count),
                max_retries=int(row.max_retries),
                task_options=row.task_options,
            )
        case 'CLAIMED':
            return ClaimedAttempt()
        case unexpected:
            raise AssertionError(
                f'in-flight read of task {task_id} returned status {unexpected!r}'
            )


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

    ``retry_count`` is the count that was read. The delay belongs to the
    next attempt of that count, so the count is part of the fence.
    """

    retry_count: int
    delay_s: float
    placement: RetryPlacement


type RetryRequest = NoRetry | RetryAfter


def retry_request_for(
    attempt: RunningAttempt,
    error_code: str | None,
    placement: RetryPlacement,
) -> RetryRequest:
    """The retry that the policy of the task asks for, with its delay. Pure.

    ``NoRetry`` when the count is spent, the options hold no policy, or the
    policy does not name ``error_code``. ``good_until`` is judged by the
    function, against the database clock, under the lock.
    """
    policy = admitted_retry_policy(
        retry_count=attempt.retry_count,
        max_retries=attempt.max_retries,
        task_options_json=attempt.task_options,
        error_code=error_code,
    )
    match policy:
        case None:
            return NoRetry()
        case dict():
            return RetryAfter(
                retry_count=attempt.retry_count,
                delay_s=calculate_retry_delay(attempt.retry_count + 1, policy),
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
    claimed_at: datetime | None,
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


def finish_parameters(command: FinishFailedAttempt) -> dict[str, Any]:
    """The parameters of ``FINISH_FAILED_ATTEMPT_SQL``. Pure."""
    match command.retry:
        case NoRetry():
            retry: dict[str, Any] = {
                'expected_retry_count': None,
                'retry_delay_s': None,
                'retry_placement': None,
            }
        case RetryAfter(retry_count=count, delay_s=delay_s, placement=placement):
            retry = {
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
    }


async def finish_failed_attempt(
    connection: AsyncConnection,
    command: FinishFailedAttempt,
) -> FailedAttemptOutcome:
    """Run the failed-attempt finish and decode what it reports.

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
    result = await connection.execute(
        FINISH_FAILED_ATTEMPT_SQL,
        finish_parameters(command),
    )
    rows = result.mappings().all()
    if len(rows) != 1:
        raise RuntimeError(
            f'{FINISH_FAILED_ATTEMPT_FUNCTION} returned {len(rows)} rows for '
            f'task {command.task_id}; it reports exactly one outcome'
        )
    outcome = decode_failed_attempt_row(
        {str(key): value for key, value in rows[0].items()},
    )
    _log_outcome(command, outcome)
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
