"""The cancel action for individual tasks.

The action is one transaction: lock the target row, apply a
compare-and-set, and — when the CAS matches nothing — diagnose the
reason from the row already locked. Holding the lock is what makes the
diagnosis trustworthy; a concurrent claim either waits behind it or is
rejected by the CAS.

Two boundaries are deliberate:

* Workflow-bound rows are refused. They are managed exclusively by the
  workflow primitives; resetting one would orphan or hang its node.
* Cancelling a RUNNING task flips the row durably, but the task's code keeps
  executing on its worker until it returns, and its side effects still happen.
  There is no safe cross-process kill, and this module does not attempt one.

The manual retry action that reset a terminal row to PENDING in place is
removed: a terminal record is immutable, and re-execution is a new
request through the rerun contract, with new identity and lineage.

``cancel_task`` returns ``Result[TaskCancelled, TaskActionError]``. The
error side is ``TaskActionError``: a union with one frozen variant per
``TaskActionErrorCode`` member. Every variant has ``code``, ``message``,
``retryable`` and ``task_id``. Callers match on the variant.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from enum import Enum
from typing import TYPE_CHECKING, ClassVar, assert_never

from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from sqlalchemy.ext.asyncio import AsyncConnection

from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.history.errors import HistoryContractError
from horsies.core.history.reads.detail import (
    HistoryTaskDetail,
    LiveTaskLocation,
    TaskDetailAbsent,
    read_task_detail,
    staged_detail_published,
)
from horsies.core.lifecycle.commands import CancelLockedTask
from horsies.core.lifecycle.fences import CallerHoldsRowLock
from horsies.core.lifecycle.outcomes import (
    AlreadyApplied,
    Applied,
    LostClaim,
    ObservedTaskState,
    OutcomeDecodeError,
    SourceStateConflict,
    TaskAbsent,
)
from horsies.core.lifecycle.persistence import apply_async
from horsies.core.types.result import Err, Ok, Result
from horsies.core.types.status import TaskStatus
from horsies.core.utils.db import is_retryable_connection_error


if TYPE_CHECKING:
    from horsies.core.brokers.resources import EventLoopLimit


class TaskActionErrorCode(str, Enum):
    """Why a task action did not apply.

    Each member names one variant of ``TaskActionError``.
    """

    TASK_NOT_FOUND = 'TASK_NOT_FOUND'
    TASK_NOT_CANCELLABLE = 'TASK_NOT_CANCELLABLE'
    TASK_IS_WORKFLOW_TASK = 'TASK_IS_WORKFLOW_TASK'
    DB_OPERATION_FAILED = 'DB_OPERATION_FAILED'
    # The broker already holds resource sets for its limit of open event
    # loops, and this call ran on a new one. Nothing changed. It clears when
    # one of those loops closes.
    EVENT_LOOP_LIMIT = 'EVENT_LOOP_LIMIT'
    # Stored task data does not parse. Nothing changed. A repeat gives the
    # same result until the stored row changes.
    STORED_DATA_INVALID = 'STORED_DATA_INVALID'


@dataclass(slots=True, frozen=True)
class TaskActionNotFound:
    """No task has this id. Nothing changed.

    The id is not a UUID, the database has no staged history read (a
    database before the history coverage), or neither the live table nor
    history holds the task (retention can remove it). Also returned when
    the history read finds the task live after the lock read missed it:
    the cancel decides at the lock read, where the row was not live; a
    repeated cancel finds the row.
    """

    code: ClassVar[TaskActionErrorCode] = TaskActionErrorCode.TASK_NOT_FOUND
    retryable: ClassVar[bool] = False
    message: str = field(repr=False)
    task_id: str


@dataclass(slots=True, frozen=True)
class TaskActionNotCancellable:
    """The task's status does not admit the cancel. Nothing changed.

    ``current_status`` is the status that the cancel found: a terminal
    status, or ``RUNNING`` when ``include_running`` is ``False``. A call
    with ``include_running=True`` cancels a ``RUNNING`` task.
    """

    code: ClassVar[TaskActionErrorCode] = TaskActionErrorCode.TASK_NOT_CANCELLABLE
    retryable: ClassVar[bool] = False
    message: str = field(repr=False)
    task_id: str
    current_status: TaskStatus


@dataclass(slots=True, frozen=True)
class TaskActionWorkflowTask:
    """The task belongs to a workflow. Nothing changed.

    Only the workflow primitives drive a workflow task: pause, resume and
    cancel on its workflow. ``current_status`` is the task's status.
    """

    code: ClassVar[TaskActionErrorCode] = TaskActionErrorCode.TASK_IS_WORKFLOW_TASK
    retryable: ClassVar[bool] = False
    message: str = field(repr=False)
    task_id: str
    current_status: TaskStatus


@dataclass(slots=True, frozen=True)
class TaskActionDbOperationFailed:
    """The cancel transaction failed with a database or driver error.

    ``retryable`` is the driver classification of the cause: ``True`` when
    the condition may clear, for example a lost connection. ``exception``
    is the ``SQLAlchemyError``. Before the commit, the transaction rolled
    back and nothing changed. At the commit, the commit outcome is unknown:
    a repeated cancel tells which, because a committed cancel answers
    ``TaskActionNotCancellable`` with ``CANCELLED``.
    """

    code: ClassVar[TaskActionErrorCode] = TaskActionErrorCode.DB_OPERATION_FAILED
    message: str = field(repr=False)
    retryable: bool
    task_id: str
    exception: BaseException = field(repr=False)


@dataclass(slots=True, frozen=True)
class TaskActionEventLoopLimit:
    """The broker holds resource sets for its limit of open event loops. Nothing ran.

    ``loop_limit`` names the limit and the threads of the held loops. A
    call from this loop returns the same error until one of the held loops
    closes.
    """

    code: ClassVar[TaskActionErrorCode] = TaskActionErrorCode.EVENT_LOOP_LIMIT
    retryable: ClassVar[bool] = False
    message: str = field(repr=False)
    task_id: str
    loop_limit: EventLoopLimit


@dataclass(slots=True, frozen=True)
class TaskActionStoredDataInvalid:
    """Stored task data does not parse. Nothing changed: the transaction did not commit.

    ``exception`` is the ``HistoryContractError`` of the history read (for
    example a history status that is not a ``TaskStatus`` member, or an
    attempt snapshot that does not decode), or the ``OutcomeDecodeError``
    of the cancel outcome row. A repeat gives the same result until the
    stored row changes.
    """

    code: ClassVar[TaskActionErrorCode] = TaskActionErrorCode.STORED_DATA_INVALID
    retryable: ClassVar[bool] = False
    message: str = field(repr=False)
    task_id: str
    exception: BaseException = field(repr=False)


type TaskActionError = (
    TaskActionNotFound
    | TaskActionNotCancellable
    | TaskActionWorkflowTask
    | TaskActionDbOperationFailed
    | TaskActionEventLoopLimit
    | TaskActionStoredDataInvalid
)
"""Every failure of a task action: one variant per ``TaskActionErrorCode``."""


@dataclass(frozen=True, slots=True)
class TaskCancelled:
    """A task moved to CANCELLED by this call."""

    task_id: str
    was_status: TaskStatus


# Locks the row for the rest of the transaction. The diagnosis reads
# ``is_workflow_task`` here; the status it reports comes from the cancel
# outcome, which decodes it.
_LOCK_TASK_SQL = text("""
    SELECT is_workflow_task
    FROM horsies_tasks
    WHERE id = CAST(:id AS uuid)
    FOR UPDATE
""")

def _not_found(task_id: str) -> Err[TaskActionNotFound]:
    """The row does not exist — retention may have removed it."""
    return Err(
        TaskActionNotFound(
            message=f'Task {task_id} does not exist.',
            task_id=task_id,
        )
    )


def _workflow_bound(
    task_id: str, current_status: TaskStatus,
) -> Err[TaskActionWorkflowTask]:
    """Workflow-bound rows are driven by the workflow primitives only."""
    return Err(
        TaskActionWorkflowTask(
            message=(
                f'Task {task_id} belongs to a workflow; use the workflow '
                f'actions instead.'
            ),
            task_id=task_id,
            current_status=current_status,
        )
    )


def _not_cancellable(
    task_id: str, current_status: TaskStatus,
) -> Err[TaskActionNotCancellable]:
    """The row exists but its status does not admit the cancel."""
    return Err(
        TaskActionNotCancellable(
            message=(
                f'Task {task_id} cannot be cancelled from status '
                f'{current_status.value}.'
            ),
            task_id=task_id,
            current_status=current_status,
        )
    )


def _db_err(
    action: str, task_id: str, exc: SQLAlchemyError,
) -> Err[TaskActionDbOperationFailed]:
    """Wrap a transaction failure, classifying transient errors."""
    return Err(
        TaskActionDbOperationFailed(
            message=f'{action} failed: {exc}',
            retryable=is_retryable_connection_error(exc),
            task_id=task_id,
            exception=exc,
        )
    )


def _stored_data_err(
    action: str,
    task_id: str,
    exc: HistoryContractError | OutcomeDecodeError,
) -> Err[TaskActionStoredDataInvalid]:
    """Wrap stored data that does not parse. The transaction did not commit."""
    return Err(
        TaskActionStoredDataInvalid(
            message=f'{action} failed: stored data does not parse: {exc}',
            task_id=task_id,
            exception=exc,
        )
    )


async def _diagnose_live_miss(
    connection: AsyncConnection, task_id: str
) -> Err[TaskActionNotFound | TaskActionWorkflowTask | TaskActionNotCancellable]:
    """Classify a task absent from the live table.

    Terminalization moves finished rows to history, so a terminal task
    is a live miss by design: the history read names it, and the answer
    is the same refusal a terminal live row produced before the split —
    a state conflict, or the workflow-bound refusal — never "not
    found" for a task the dashboard is still showing.

    A history read that reports the task live is "not found" too. The
    cancel decides at the lock read, and the row was not live there: the
    history read is a later statement, which saw a row that was inserted
    after the lock read. A repeated cancel finds the row.

    Raises:
        HistoryContractError: the history row does not decode.
    """
    if not await staged_detail_published(connection):
        return _not_found(task_id)
    detail = await read_task_detail(connection, task_id=task_id)
    match detail:
        case HistoryTaskDetail(is_workflow_task=True, status=status):
            return _workflow_bound(task_id, status)
        case HistoryTaskDetail(status=status):
            return _not_cancellable(task_id, status)
        case TaskDetailAbsent():
            return _not_found(task_id)
        case LiveTaskLocation():
            return _not_found(task_id)
        case _ as unreachable:
            assert_never(unreachable)


async def cancel_task(
    broker: PostgresBroker,
    task_id: str,
    *,
    include_running: bool = False,
) -> Result[TaskCancelled, TaskActionError]:
    """Move a non-workflow task to CANCELLED.

    PENDING and CLAIMED tasks are always eligible: neither has begun user
    code, and a claimed task's worker aborts before starting when its
    pre-start ownership check fails. RUNNING requires ``include_running``,
    because the row flips while the process keeps executing — its side
    effects still happen and no attempt record is written for that run.

    Waiting result handles unblock on commit: the terminal-status trigger
    fires ``task_done`` and a CANCELLED row is reported as TASK_CANCELLED,
    so no result payload is written here.

    Raises:
        RuntimeError: the cancel outcome contradicts the caller-held row
            lock, or the cancel function reports no row or no observed
            status for the locked row. These are defects, not outcomes.
    """
    permitted_statuses = (TaskStatus.PENDING, TaskStatus.CLAIMED)
    if include_running:
        permitted_statuses += (TaskStatus.RUNNING,)

    # Identity columns are uuid; text that does not parse as one cannot
    # name any task. Python also accepts forms that the Postgres uuid cast
    # refuses (for example a ``urn:uuid:`` prefix), so every statement gets
    # the canonical text.
    try:
        canonical_id = str(uuid.UUID(task_id))
    except ValueError:
        return _not_found(task_id)
    task_id = canonical_id

    match await broker.select_resources():
        case Err(loop_limit):
            return Err(
                TaskActionEventLoopLimit(
                    message=f'Task cancel of {task_id}: {loop_limit.describe()}',
                    task_id=task_id,
                    loop_limit=loop_limit,
                )
            )
        case Ok(resources):
            pass

    try:
        async with resources.session_factory() as session:
            locked = (await session.execute(_LOCK_TASK_SQL, {'id': task_id})).first()
            if locked is None:
                return await _diagnose_live_miss(
                    await session.connection(), task_id
                )
            (is_workflow_task,) = locked

            outcome = await apply_async(
                await session.connection(),
                CancelLockedTask(
                    task_id=task_id,
                    fence=CallerHoldsRowLock(),
                    permitted_source_statuses=permitted_statuses,
                ),
            )
            # The cancel function reports the locked row's status on each
            # of these outcomes, and the decoder has parsed it.
            match outcome:
                case Applied(
                    observed=ObservedTaskState(status=TaskStatus() as was_status),
                ):
                    await session.commit()
                    return Ok(TaskCancelled(task_id=task_id, was_status=was_status))
                case (
                    AlreadyApplied(
                        observed=ObservedTaskState(
                            status=TaskStatus() as observed_status,
                        ),
                    )
                    | SourceStateConflict(
                        observed=ObservedTaskState(
                            status=TaskStatus() as observed_status,
                        ),
                    )
                ):
                    if is_workflow_task:
                        return _workflow_bound(task_id, observed_status)
                    return _not_cancellable(task_id, observed_status)
                case Applied() | AlreadyApplied() | SourceStateConflict():
                    raise RuntimeError(
                        f'administrative cancellation of task {task_id} '
                        'reported no observed status for the locked row'
                    )
                case LostClaim() | TaskAbsent():
                    raise RuntimeError(
                        'administrative cancellation contradicted the '
                        'caller-held row lock'
                    )
                case _ as unreachable:
                    assert_never(unreachable)
    except SQLAlchemyError as exc:
        return _db_err('Task cancel', task_id, exc)
    except (HistoryContractError, OutcomeDecodeError) as exc:
        return _stored_data_err('Task cancel', task_id, exc)
