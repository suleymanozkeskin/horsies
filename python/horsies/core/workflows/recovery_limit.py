"""The bound on workflow recovery raises.

A recovery candidate that raises is rolled back and selected again next pass.
A raise that repeats would repeat on every pass, and its workflow would stay
RUNNING. This module stops that: a workflow whose recovery raised a persistent
error in N reaper passes fails closed.

A raise is persistent unless it is an expected condition of the database or
the connection. Those clear by themselves and do not count, so an outage
cannot fail a workflow. The count is stored, because a count in memory resets
at each restart and splits across the workers that hold the reaper gate.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING, Any, Final, Literal, assert_never

from sqlalchemy import text
from sqlalchemy.exc import PendingRollbackError, SQLAlchemyError

from horsies.core.codec.json_io import Json, SerializationError, dumps_json
from horsies.core.codec.json_value import escape_lone_surrogates
from horsies.core.codec.typed import encode_task_result
from horsies.core.lifecycle.outcomes import OutcomeDecodeError
from horsies.core.logging import get_logger
from horsies.core.models.tasks import (
    OperationalErrorCode,
    TaskError,
    TaskResult,
)
from horsies.core.models.workflow.enums import WF_TASK_TERMINAL_VALUES
from horsies.core.types.result import Err, Ok, Result
from horsies.core.utils.db import autocommit_connection, is_retryable_connection_error
from horsies.core.workflows.sql import (
    MARK_WORKFLOW_FAILED_SQL,
    MARK_WORKFLOW_TASK_FAILED_SQL,
)

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

logger = get_logger('workflow.recovery')

MAX_RAISE_TEXT_LENGTH: Final = 500
"""Characters of the raise text that the limit error and the log keep."""

MAX_EXCEPTION_TYPE_LENGTH: Final = 255
"""The width of the `last_error_type` column."""


class RecoveryCase(Enum):
    """The recovery path that ran a candidate. The value is stored."""

    PENDING_TERMINAL_DEPS = 'pending_terminal_deps'
    READY_NOT_ENQUEUED = 'ready_not_enqueued'
    READY_SUBWORKFLOW_NOT_STARTED = 'ready_subworkflow_not_started'
    COMPLETED_CHILD_PARENT_NOT_UPDATED = 'completed_child_parent_not_updated'
    TERMINAL_WORKFLOW_RUNNING = 'terminal_workflow_running'
    QUEUED_PARENT_PROPAGATION = 'queued_parent_propagation'
    PHASE2_PROGRESSION = 'phase2_progression'


class RecoveryRaiseClass(Enum):
    """Whether a raise can count toward the limit."""

    TRANSIENT = 'transient'
    PERSISTENT = 'persistent'


class RecoveryCandidateOutcome(Enum):
    """What one recovery candidate did.

    NOT_COUNTED, COUNTED and LIMIT_REACHED all mean that the candidate raised
    and that its SAVEPOINT rolled back. NOT_COUNTED wrote nothing in the
    session. COUNTED and LIMIT_REACHED wrote in the session, so the caller
    must commit to keep the write.
    """

    APPLIED = 'applied'
    REFUSED = 'refused'
    NOT_COUNTED = 'not_counted'
    COUNTED = 'counted'
    LIMIT_REACHED = 'limit_reached'


type RaiseRecord = Literal[
    RecoveryCandidateOutcome.NOT_COUNTED,
    RecoveryCandidateOutcome.COUNTED,
    RecoveryCandidateOutcome.LIMIT_REACHED,
]


class RecoveryLimitOutcome(Enum):
    """What the limit write did.

    WORKFLOW_FAILED: no live node remained, so the workflow is FAILED.
    LIVE_NODES_FINISH: a live node remains, so the workflow stays RUNNING with
    its error stored, and the last node ends it.
    REFUSED: the workflow was not RUNNING. A pause or a cancel won. Nothing
    changed.
    """

    WORKFLOW_FAILED = 'workflow_failed'
    LIVE_NODES_FINISH = 'live_nodes_finish'
    REFUSED = 'refused'


class RecoveryWrite(Enum):
    """The write of this module that a `RecoveryWriteFailed` names."""

    COUNT = 'count'
    LIMIT = 'limit'


@dataclass(frozen=True, slots=True)
class CountFailures:
    """Count persistent raises. One value lives for one reaper pass.

    `pass_id` is why a workflow counts once per pass: every case of the pass
    must get this same value. A fan-out with many poison nodes must not reach
    the limit in one pass.
    """

    pass_id: str
    limit: int


@dataclass(frozen=True, slots=True)
class LogFailuresOnly:
    """Log raises and count nothing. The reaper counts the same candidate."""


type RecoveryFailurePolicy = CountFailures | LogFailuresOnly


@dataclass(frozen=True, slots=True)
class RecoveryRaise:
    """One persistent raise of one candidate."""

    workflow_id: str
    task_index: int | None
    case: RecoveryCase
    exception_type: str
    detail: str

    @classmethod
    def of(
        cls,
        exc: Exception,
        *,
        workflow_id: str,
        task_index: int | None,
        case: RecoveryCase,
    ) -> RecoveryRaise:
        """Build the record of `exc`, with its text in the stored bounds."""
        return cls(
            workflow_id=workflow_id,
            task_index=task_index,
            case=case,
            exception_type=type(exc).__name__[:MAX_EXCEPTION_TYPE_LENGTH],
            detail=escape_lone_surrogates(str(exc)[:MAX_RAISE_TEXT_LENGTH]),
        )


@dataclass(frozen=True, slots=True)
class RecoveryLimitPayloads:
    """The limit error in the two forms that it is stored in."""

    workflow_error_json: str
    node_result_json: str


@dataclass(frozen=True, slots=True)
class PassCounted:
    """This raise added one to the count."""

    failed_passes: int


@dataclass(frozen=True, slots=True)
class PassAlreadyCounted:
    """An earlier raise of this workflow already counted this pass."""


type RecoveryCount = PassCounted | PassAlreadyCounted


@dataclass(frozen=True, slots=True)
class RecoveryWriteFailed:
    """A count or limit write failed.

    Its SAVEPOINT rolled back, so the write left nothing. The candidate is
    selected again next pass, so the next raise tries the write again.
    """

    write: RecoveryWrite
    workflow_id: str
    exception_type: str
    detail: str


COUNT_RECOVERY_RAISE_SQL = text("""
INSERT INTO horsies_workflow_recovery_failures AS failure (
    workflow_id, failed_passes, last_pass_id,
    first_failed_at, last_failed_at,
    last_case, last_task_index, last_error_type
)
VALUES (
    CAST(:workflow_id AS uuid), 1, CAST(:pass_id AS uuid),
    statement_timestamp(), statement_timestamp(),
    :case_name, :task_index, :exception_type
)
ON CONFLICT (workflow_id) DO UPDATE
SET failed_passes = failure.failed_passes + 1,
    last_pass_id = EXCLUDED.last_pass_id,
    last_failed_at = EXCLUDED.last_failed_at,
    last_case = EXCLUDED.last_case,
    last_task_index = EXCLUDED.last_task_index,
    last_error_type = EXCLUDED.last_error_type
WHERE failure.last_pass_id <> EXCLUDED.last_pass_id
RETURNING failed_passes
""")
"""No row returned means that this pass already counted this workflow."""

LOCK_WORKFLOW_STATUS_SQL = text("""
SELECT status FROM horsies_workflows
WHERE id = CAST(:workflow_id AS uuid)
FOR UPDATE
""")

SKIP_UNSTARTED_NODES_SQL = text("""
UPDATE horsies_workflow_tasks
SET status = 'SKIPPED', completed_at = NOW()
WHERE workflow_id = CAST(:workflow_id AS uuid)
  AND status IN ('PENDING', 'READY')
""")

KEEP_FIRST_WORKFLOW_ERROR_SQL = text("""
UPDATE horsies_workflows
SET error = COALESCE(error, :error), updated_at = NOW()
WHERE id = CAST(:workflow_id AS uuid)
  AND status = 'RUNNING'
""")

HAS_LIVE_NODE_SQL = text("""
SELECT EXISTS (
    SELECT 1 FROM horsies_workflow_tasks
    WHERE workflow_id = CAST(:workflow_id AS uuid)
      AND status IN ('ENQUEUED', 'RUNNING')
)
""")

COUNT_OVER_FAILURE_LIMIT_SQL = text("""
SELECT count(*)
FROM horsies_workflow_recovery_failures failure
JOIN horsies_workflows w ON w.id = failure.workflow_id
WHERE failure.failed_passes >= CAST(:failure_limit AS integer)
  AND w.status = 'RUNNING'
""")
"""RUNNING workflows at or over the limit.

A live node keeps a workflow here, and so does a limit write that keeps
failing. A workflow that stays here is visible, not only absent from the log.
"""


RECORD_RECOVERY_RAISE_FUNCTION_SQL: Final = text("""
SELECT outcome, failed_passes, limit_write_error
FROM horsies_workflow_recovery_record_raise(
    CAST(:workflow_id AS uuid),
    CAST(:pass_id AS uuid),
    CAST(:case_name AS text),
    CAST(:task_index AS integer),
    CAST(:exception_type AS text),
    CAST(:detail AS text),
    CAST(:failure_limit AS integer)
)
""")
"""One persistent raise of a recovery candidate, in one statement in
autocommit: the count once per pass, and at the limit the limit writes, with
the limit error built from the stored count."""


@dataclass(frozen=True, slots=True)
class RaiseWorkflowAbsent:
    """The workflow row is gone. Nothing was written."""


@dataclass(frozen=True, slots=True)
class RaiseAlreadyCounted:
    """An earlier raise of this workflow already counted this pass. Nothing
    was written."""


@dataclass(frozen=True, slots=True)
class RaiseCountedUnderLimit:
    """The count moved and is under the limit."""

    failed_passes: int


@dataclass(frozen=True, slots=True)
class RaiseLimitRefused:
    """The count reached the limit and the workflow is not RUNNING: a pause or
    a cancel won. The count stays; the workflow did not change."""

    failed_passes: int


@dataclass(frozen=True, slots=True)
class RaiseLimitWritten:
    """The count reached the limit and the limit write stopped the workflow."""

    failed_passes: int
    outcome: RecoveryLimitOutcome
    """``WORKFLOW_FAILED`` or ``LIVE_NODES_FINISH``."""


@dataclass(frozen=True, slots=True)
class RaiseLimitWriteFailed:
    """The count reached the limit and the limit write raised. The count
    stays; the next raise tries the write again."""

    failed_passes: int
    cause: str
    """The SQLSTATE and the message of the write."""


type RecordedRaise = (
    RaiseWorkflowAbsent
    | RaiseAlreadyCounted
    | RaiseCountedUnderLimit
    | RaiseLimitRefused
    | RaiseLimitWritten
    | RaiseLimitWriteFailed
)


def _failed_passes(value: object) -> int:
    match value:
        case bool():
            raise OutcomeDecodeError(
                f'horsies_workflow_recovery_record_raise returned failed_passes {value!r}; '
                'expected an integer',
            )
        case int():
            return value
        case _:
            raise OutcomeDecodeError(
                f'horsies_workflow_recovery_record_raise returned failed_passes {value!r}; '
                'expected an integer',
            )


def decode_recorded_raise(rows: Sequence[Any]) -> RecordedRaise:
    """The outcome of a ``horsies_workflow_recovery_record_raise`` call. Pure.

    Raises:
        OutcomeDecodeError: not one row, an outcome outside the function's
            contract, or a count or cause that does not have its type.
    """
    match rows:
        case [row]:
            pass
        case _:
            raise OutcomeDecodeError(
                f'horsies_workflow_recovery_record_raise returned {len(rows)} rows; '
                'expected one row',
            )
    match row.outcome:
        case 'WORKFLOW_ABSENT':
            return RaiseWorkflowAbsent()
        case 'ALREADY_COUNTED':
            return RaiseAlreadyCounted()
        case 'COUNTED':
            return RaiseCountedUnderLimit(failed_passes=_failed_passes(row.failed_passes))
        case 'REFUSED':
            return RaiseLimitRefused(failed_passes=_failed_passes(row.failed_passes))
        case 'LIMIT_REACHED_FAILED':
            return RaiseLimitWritten(
                failed_passes=_failed_passes(row.failed_passes),
                outcome=RecoveryLimitOutcome.WORKFLOW_FAILED,
            )
        case 'LIMIT_REACHED_LIVE_NODES':
            return RaiseLimitWritten(
                failed_passes=_failed_passes(row.failed_passes),
                outcome=RecoveryLimitOutcome.LIVE_NODES_FINISH,
            )
        case 'LIMIT_WRITE_FAILED':
            match row.limit_write_error:
                case str() as cause:
                    return RaiseLimitWriteFailed(
                        failed_passes=_failed_passes(row.failed_passes), cause=cause,
                    )
                case other:
                    raise OutcomeDecodeError(
                        'horsies_workflow_recovery_record_raise returned LIMIT_WRITE_FAILED '
                        f'with limit_write_error {other!r}; expected text',
                    )
        case other:
            raise OutcomeDecodeError(
                f'horsies_workflow_recovery_record_raise returned outcome {other!r}',
            )


async def count_recovery_raise(
    session_factory: 'async_sessionmaker[AsyncSession]',
    policy: CountFailures,
    failure: RecoveryRaise,
) -> RaiseRecord:
    """Count one persistent raise with ``horsies_workflow_recovery_record_raise``.

    One statement on an autocommit connection, after the candidate's own
    transaction rolled back. At the limit the function also writes the
    limit, so the workflow fails closed.

    Returns:
        NOT_COUNTED: the workflow is gone, this pass already counted it, or
        the statement failed (logged with its cause; the next pass counts
        again).
        COUNTED: the count moved and the workflow did not change: the count
        is under the limit, or the limit write was refused or failed.
        LIMIT_REACHED: the limit write stopped the workflow.

    Raises:
        OutcomeDecodeError: a row outside the function's contract (a defect).
    """
    try:
        async with session_factory() as session:
            connection = await autocommit_connection(session)
            rows = (
                await connection.execute(
                    RECORD_RECOVERY_RAISE_FUNCTION_SQL,
                    {
                        'workflow_id': failure.workflow_id,
                        'pass_id': policy.pass_id,
                        'case_name': failure.case.value,
                        'task_index': failure.task_index,
                        'exception_type': failure.exception_type,
                        'detail': failure.detail[:MAX_RAISE_TEXT_LENGTH],
                        'failure_limit': policy.limit,
                    },
                )
            ).fetchall()
    except SQLAlchemyError as write_error:
        logger.error(
            'Workflow recovery count write failed: workflow_id=%s '
            'exception_type=%s exception=%s',
            failure.workflow_id,
            type(write_error).__name__,
            str(write_error)[:MAX_RAISE_TEXT_LENGTH],
        )
        return RecoveryCandidateOutcome.NOT_COUNTED
    match decode_recorded_raise(rows):
        case RaiseWorkflowAbsent() | RaiseAlreadyCounted():
            return RecoveryCandidateOutcome.NOT_COUNTED
        case RaiseCountedUnderLimit(failed_passes=failed_passes):
            logger.error(
                'Workflow recovery raise counted: failed_passes=%s/%s case=%s '
                'workflow_id=%s task_index=%s exception_type=%s',
                failed_passes,
                policy.limit,
                failure.case.value,
                failure.workflow_id,
                failure.task_index,
                failure.exception_type,
            )
            return RecoveryCandidateOutcome.COUNTED
        case RaiseLimitRefused(failed_passes=failed_passes):
            logger.warning(
                'Workflow recovery limit refused: workflow_id=%s is not RUNNING '
                '(failed_passes=%s)',
                failure.workflow_id,
                failed_passes,
            )
            return RecoveryCandidateOutcome.COUNTED
        case RaiseLimitWriteFailed(failed_passes=failed_passes, cause=cause):
            logger.critical(
                'Workflow recovery limit write failed: workflow_id=%s '
                'failed_passes=%s cause=%s',
                failure.workflow_id,
                failed_passes,
                cause,
            )
            return RecoveryCandidateOutcome.COUNTED
        case RaiseLimitWritten(failed_passes=failed_passes, outcome=outcome):
            logger.critical(
                'Workflow recovery limit reached: workflow_id=%s '
                'failed_passes=%s outcome=%s case=%s task_index=%s '
                'exception_type=%s exception=%s',
                failure.workflow_id,
                failed_passes,
                outcome.value,
                failure.case.value,
                failure.task_index,
                failure.exception_type,
                failure.detail,
            )
            return RecoveryCandidateOutcome.LIMIT_REACHED
        case _ as unreachable:
            assert_never(unreachable)


def classify_recovery_raise(exc: Exception) -> RecoveryRaiseClass:
    """Say whether a raise of a recovery candidate can count.

    `PendingRollbackError` is transient because it is the mark of an earlier
    connection loss: after a loss, each later candidate in the session gets
    this error. The rule stays here and not in
    `is_retryable_connection_error`, because that helper has callers that
    this rule does not concern.
    """
    match exc:
        case PendingRollbackError():
            return RecoveryRaiseClass.TRANSIENT
        case _ if is_retryable_connection_error(exc):
            return RecoveryRaiseClass.TRANSIENT
        case _:
            return RecoveryRaiseClass.PERSISTENT


def recovery_limit_error(failure: RecoveryRaise, failed_passes: int) -> TaskError:
    """Build the error that the limit stores."""
    return TaskError(
        error_code=OperationalErrorCode.WORKFLOW_RECOVERY_LIMIT_REACHED,
        message=(
            f'Workflow recovery raised a persistent error in {failed_passes} '
            f'passes. Last case: {failure.case.value}. Last raise: '
            f'{failure.exception_type}: {failure.detail}'
        ),
        data={
            'workflow_id': failure.workflow_id,
            'case': failure.case.value,
            'task_index': failure.task_index,
            'failed_passes': failed_passes,
            'exception_type': failure.exception_type,
        },
    )


def _limit_json(value: Json) -> str:
    """Serialize a part of the limit error, or raise.

    Raises:
        SerializationError: the value does not serialize. The limit error is
            built only from escaped text, so this is a defect.
    """
    match dumps_json(value):
        case Ok(serialized):
            return serialized
        case Err(cause):
            raise SerializationError(
                f'Failed to serialize the workflow recovery limit error: {cause}',
            ) from cause
        case _ as unreachable:
            assert_never(unreachable)


def recovery_limit_payloads(error: TaskError) -> RecoveryLimitPayloads:
    """Encode the limit error once, for the workflow row and the node row.

    A defect here raises. It does not fall back to a stored text that hides
    the error.

    Raises:
        StrictJsonError: the error does not encode.
        ValueValidationError: the error does not encode.
        SerializationError: the encoded error does not serialize.
    """
    envelope = encode_task_result(TaskResult(err=error), type(None))
    return RecoveryLimitPayloads(
        workflow_error_json=_limit_json(envelope['err']),
        node_result_json=_limit_json(envelope),
    )


async def _count_recovery_raise(
    session: 'AsyncSession',
    policy: CountFailures,
    failure: RecoveryRaise,
) -> Result[RecoveryCount, RecoveryWriteFailed]:
    """Add this pass to the count of the workflow, in the session of the pass.

    The write rides the transaction of the pass, so a database that refuses
    writes cannot move the count, and a lost pass loses its count.

    Returns:
        `PassCounted` with the new count; `PassAlreadyCounted` when this pass
        already counted; `Err` when the write failed. A failed write changes
        nothing.
    """
    try:
        async with session.begin_nested():
            row = (
                await session.execute(
                    COUNT_RECOVERY_RAISE_SQL,
                    {
                        'workflow_id': failure.workflow_id,
                        'pass_id': policy.pass_id,
                        'case_name': failure.case.value,
                        'task_index': failure.task_index,
                        'exception_type': failure.exception_type,
                    },
                )
            ).first()
    except SQLAlchemyError as exc:
        return Err(_write_failed(RecoveryWrite.COUNT, failure.workflow_id, exc))
    match row:
        case None:
            return Ok(PassAlreadyCounted())
        case _:
            return Ok(PassCounted(failed_passes=int(row.failed_passes)))


def _write_failed(
    write: RecoveryWrite,
    workflow_id: str,
    exc: SQLAlchemyError,
) -> RecoveryWriteFailed:
    return RecoveryWriteFailed(
        write=write,
        workflow_id=workflow_id,
        exception_type=type(exc).__name__[:MAX_EXCEPTION_TYPE_LENGTH],
        detail=escape_lone_surrogates(str(exc)[:MAX_RAISE_TEXT_LENGTH]),
    )


async def _write_limit(
    session: 'AsyncSession',
    failure: RecoveryRaise,
    payloads: RecoveryLimitPayloads,
) -> RecoveryLimitOutcome:
    """Stop the workflow with SQL only, under the workflow row lock.

    Raises:
        SQLAlchemyError: a statement failed. The caller owns the SAVEPOINT.
    """
    workflow_id = failure.workflow_id
    status = (
        await session.execute(
            LOCK_WORKFLOW_STATUS_SQL, {'workflow_id': workflow_id},
        )
    ).scalar_one_or_none()
    if status != 'RUNNING':
        logger.warning(
            'Workflow recovery limit refused: workflow_id=%s status=%s',
            workflow_id,
            status,
        )
        return RecoveryLimitOutcome.REFUSED
    if failure.task_index is not None:
        await session.execute(
            MARK_WORKFLOW_TASK_FAILED_SQL,
            {
                'wf_id': workflow_id,
                'idx': failure.task_index,
                'result': payloads.node_result_json,
                'terminal_states': WF_TASK_TERMINAL_VALUES,
            },
        )
    await session.execute(SKIP_UNSTARTED_NODES_SQL, {'workflow_id': workflow_id})
    await session.execute(
        KEEP_FIRST_WORKFLOW_ERROR_SQL,
        {'workflow_id': workflow_id, 'error': payloads.workflow_error_json},
    )
    has_live_node = (
        await session.execute(HAS_LIVE_NODE_SQL, {'workflow_id': workflow_id})
    ).scalar_one()
    if has_live_node:
        return RecoveryLimitOutcome.LIVE_NODES_FINISH
    # None keeps the stored error: this statement writes COALESCE(:error, error).
    await session.execute(
        MARK_WORKFLOW_FAILED_SQL, {'wf_id': workflow_id, 'error': None},
    )
    return RecoveryLimitOutcome.WORKFLOW_FAILED


async def apply_recovery_limit(
    session: 'AsyncSession',
    failure: RecoveryRaise,
    failed_passes: int,
) -> Result[RecoveryLimitOutcome, RecoveryWriteFailed]:
    """Fail a workflow at the limit, in the transaction of the caller.

    This function runs no engine code, because engine code is what raised. So
    it does not evaluate `success_policy`: that evaluation is one of the paths
    that raise. A stored first failure stays in the workflow error. A live
    node finishes, because a terminal workflow has no node that is not
    terminal. A parent workflow hears through recovery case 1.6.

    After `Err`: the SAVEPOINT rolled back, and the workflow did not change.
    The next raise tries the write again.

    Raises:
        StrictJsonError: from ``recovery_limit_payloads``.
        ValueValidationError: from ``recovery_limit_payloads``.
        SerializationError: from ``recovery_limit_payloads``.
    """
    payloads = recovery_limit_payloads(
        recovery_limit_error(failure, failed_passes),
    )
    try:
        async with session.begin_nested():
            return Ok(await _write_limit(session, failure, payloads))
    except SQLAlchemyError as exc:
        return Err(_write_failed(RecoveryWrite.LIMIT, failure.workflow_id, exc))


async def record_recovery_raise(
    session: 'AsyncSession',
    policy: CountFailures,
    failure: RecoveryRaise,
) -> RaiseRecord:
    """Count one persistent raise, and apply the limit when the count is there.

    Returns:
        NOT_COUNTED: this pass already counted, or the count write failed.
        COUNTED: the count moved, and the workflow did not change. The count
        is under the limit, or the limit write was refused or failed.
        LIMIT_REACHED: the limit write stopped the workflow.
    """
    match await _count_recovery_raise(session, policy, failure):
        case Err(count_failed):
            logger.error(
                'Workflow recovery count write failed: workflow_id=%s '
                'exception_type=%s exception=%s',
                failure.workflow_id,
                count_failed.exception_type,
                count_failed.detail,
            )
            return RecoveryCandidateOutcome.NOT_COUNTED
        case Ok(count):
            pass
        case _ as unreachable:
            assert_never(unreachable)
    match count:
        case PassAlreadyCounted():
            return RecoveryCandidateOutcome.NOT_COUNTED
        case PassCounted(failed_passes=failed_passes):
            pass
        case _ as unreachable:
            assert_never(unreachable)
    logger.error(
        'Workflow recovery raise counted: failed_passes=%s/%s case=%s '
        'workflow_id=%s task_index=%s exception_type=%s',
        failed_passes,
        policy.limit,
        failure.case.value,
        failure.workflow_id,
        failure.task_index,
        failure.exception_type,
    )
    if failed_passes < policy.limit:
        return RecoveryCandidateOutcome.COUNTED
    return report_recovery_limit(
        await apply_recovery_limit(session, failure, failed_passes),
        failure,
        failed_passes,
    )


def report_recovery_limit(
    result: Result[RecoveryLimitOutcome, RecoveryWriteFailed],
    failure: RecoveryRaise,
    failed_passes: int,
) -> RaiseRecord:
    """Log a limit write with its subject and cause, and name what it did."""
    match result:
        case Err(limit_failed):
            logger.critical(
                'Workflow recovery limit write failed: workflow_id=%s '
                'failed_passes=%s exception_type=%s exception=%s',
                failure.workflow_id,
                failed_passes,
                limit_failed.exception_type,
                limit_failed.detail,
            )
            return RecoveryCandidateOutcome.COUNTED
        case Ok(outcome):
            pass
        case _ as unreachable:
            assert_never(unreachable)
    match outcome:
        case RecoveryLimitOutcome.REFUSED:
            return RecoveryCandidateOutcome.COUNTED
        case (
            RecoveryLimitOutcome.WORKFLOW_FAILED
            | RecoveryLimitOutcome.LIVE_NODES_FINISH
        ):
            logger.critical(
                'Workflow recovery limit reached: workflow_id=%s '
                'failed_passes=%s outcome=%s case=%s task_index=%s '
                'exception_type=%s exception=%s',
                failure.workflow_id,
                failed_passes,
                outcome.value,
                failure.case.value,
                failure.task_index,
                failure.exception_type,
                failure.detail,
            )
            return RecoveryCandidateOutcome.LIMIT_REACHED
        case _ as unreachable:
            assert_never(unreachable)


async def count_over_failure_limit(
    session_factory: 'async_sessionmaker[AsyncSession]',
    failure_limit: int,
) -> int:
    """Count the RUNNING workflows at or over the limit.

    Raises:
        Exception: DB errors propagate to the pass, which reports them.
    """
    async with session_factory() as session:
        return int(
            (
                await session.execute(
                    COUNT_OVER_FAILURE_LIMIT_SQL,
                    {'failure_limit': failure_limit},
                )
            ).scalar_one()
        )
