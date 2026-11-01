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

from dataclasses import dataclass
from enum import Enum
from typing import TYPE_CHECKING, Final, Literal, assert_never

from sqlalchemy import text
from sqlalchemy.exc import PendingRollbackError, SQLAlchemyError

from horsies.core.codec.error_payload import serialize_error_payload
from horsies.core.codec.json_io import SerializationError, dumps_json
from horsies.core.codec.json_value import escape_lone_surrogates
from horsies.core.codec.typed import encode_task_error
from horsies.core.logging import get_logger
from horsies.core.models.tasks import (
    OperationalErrorCode,
    TaskError,
    TaskResult,
)
from horsies.core.models.workflow.enums import WF_TASK_TERMINAL_VALUES
from horsies.core.types.result import Err, Ok, Result
from horsies.core.utils.db import is_retryable_connection_error
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
"""The width of the `last_exception_type` column."""


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
    last_case, last_task_index, last_exception_type
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
    last_exception_type = EXCLUDED.last_exception_type
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
    *,
    error_json: str,
    node_result_json: str,
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
                'result': node_result_json,
                'terminal_states': WF_TASK_TERMINAL_VALUES,
            },
        )
    await session.execute(SKIP_UNSTARTED_NODES_SQL, {'workflow_id': workflow_id})
    await session.execute(
        KEEP_FIRST_WORKFLOW_ERROR_SQL,
        {'workflow_id': workflow_id, 'error': error_json},
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
        SerializationError: the limit error does not serialize. It is built
            only from escaped text, so this is a defect.
    """
    error = recovery_limit_error(failure, failed_passes)
    match dumps_json(encode_task_error(error)):
        case Ok(error_json):
            pass
        case Err(cause):
            raise SerializationError(
                f'Failed to serialize the limit error of workflow '
                f'{failure.workflow_id}: {cause}',
            ) from cause
        case _ as unreachable:
            assert_never(unreachable)
    node_result_json = serialize_error_payload(TaskResult(err=error))
    try:
        async with session.begin_nested():
            return Ok(
                await _write_limit(
                    session,
                    failure,
                    error_json=error_json,
                    node_result_json=node_result_json,
                ),
            )
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
