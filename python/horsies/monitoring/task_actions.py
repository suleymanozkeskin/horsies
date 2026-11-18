"""The cancel action for individual tasks.

The action is one statement, ``horsies_cancel_task``, that the core runs and
commits (``CoreHandle.cancel_task_async`` of the broker's core handle): it
takes the task advisory lock and the row lock, moves a cancellable row to
history as CANCELLED, or classifies the refusal from the row it locked or,
for a task absent from the live table, from history. A concurrent claim
either waits behind the lock or finds the row gone.

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
from typing import TYPE_CHECKING, ClassVar, assert_never

from sqlalchemy import text

from horsies._catalog.monitoring import TaskActionErrorCode as TaskActionErrorCode
from horsies._core import (
    CancelFailed,
    CancelRefusedStatus,
    CancelRefusedWorkflowTask,
    CancelStoredDataInvalid,
    CancelTaskAbsent,
    CoreError,
    TaskCancelledRecord,
)
from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.lifecycle.outcomes import OutcomeDecodeError
from horsies.core.types.core_failure import core_error_retryable
from horsies.core.types.result import Err, Ok, Result
from horsies.core.types.status import TaskStatus


if TYPE_CHECKING:
    from horsies.core.brokers.resources import EventLoopLimit


@dataclass(slots=True, frozen=True)
class TaskActionNotFound:
    """No task has this id. Nothing changed.

    Neither the live table nor history holds the task (retention can
    remove it).
    """

    code: ClassVar[TaskActionErrorCode] = TaskActionErrorCode.TASK_NOT_FOUND
    retryable: ClassVar[bool] = False
    message: str = field(repr=False)
    task_id: uuid.UUID


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
    task_id: uuid.UUID
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
    task_id: uuid.UUID
    current_status: TaskStatus


@dataclass(slots=True, frozen=True)
class TaskActionDbOperationFailed:
    """The cancel statement failed with a database or driver error, or the
    core of this process could not run it.

    ``retryable`` is the core's classification of the cause: ``True`` when
    the condition may clear, for example a lost connection. ``exception``
    is the core's class of the cause (a ``CoreFailure`` leaf class, with the
    subject ``cancel_task``), or the ``CoreError`` of the core boundary. The statement wrote nothing, or it
    committed and its reply was lost: a repeated cancel tells which,
    because a committed cancel answers ``TaskActionNotCancellable`` with
    ``CANCELLED``.
    """

    code: ClassVar[TaskActionErrorCode] = TaskActionErrorCode.DB_OPERATION_FAILED
    message: str = field(repr=False)
    retryable: bool
    task_id: uuid.UUID
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
    task_id: uuid.UUID
    loop_limit: EventLoopLimit


@dataclass(slots=True, frozen=True)
class TaskActionStoredDataInvalid:
    """Stored task data does not parse. Nothing changed: the cancel refused the task.

    ``exception`` is an ``OutcomeDecodeError`` with the core's message, for
    example a history status that is not a task status. A repeat gives the
    same result until the stored row changes.
    """

    code: ClassVar[TaskActionErrorCode] = TaskActionErrorCode.STORED_DATA_INVALID
    retryable: ClassVar[bool] = False
    message: str = field(repr=False)
    task_id: uuid.UUID
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

    task_id: uuid.UUID
    was_status: TaskStatus


# One statement in autocommit. The function takes the task advisory lock,
# then the row lock, and either moves the row or classifies the refusal; the
# statement commits when it returns. The core sends it; the lock-order tests
# and the Python reference of the dual-run send this text.
CANCEL_TASK_SQL = text("""
    SELECT * FROM horsies_cancel_task(CAST(:task_id AS uuid), :include_running)
""")

def _not_found(task_id: uuid.UUID) -> Err[TaskActionNotFound]:
    """The row does not exist — retention may have removed it."""
    return Err(
        TaskActionNotFound(
            message=f'Task {task_id} does not exist.',
            task_id=task_id,
        )
    )


def _workflow_bound(
    task_id: uuid.UUID, current_status: TaskStatus,
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
    task_id: uuid.UUID, current_status: TaskStatus,
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
    task_id: uuid.UUID,
    message: str,
    retryable: bool,
    exc: BaseException,
) -> Err[TaskActionDbOperationFailed]:
    """The cancel statement failed, or the core could not run it."""
    return Err(
        TaskActionDbOperationFailed(
            message=f'Task cancel failed: {message}',
            retryable=retryable,
            task_id=task_id,
            exception=exc,
        )
    )


def _cancel_answer(
    task_id: uuid.UUID,
    outcome: TaskCancelledRecord
    | CancelRefusedWorkflowTask
    | CancelRefusedStatus
    | CancelTaskAbsent
    | CancelFailed
    | CancelStoredDataInvalid,
) -> Result[TaskCancelled, TaskActionError]:
    """The answer of the cancel to its caller, from the core's record. Pure.

    A finished task is refused as it was when it was live: a workflow task
    with the workflow-bound refusal, a plain task as not cancellable from
    its terminal status (an earlier cancel included).
    """
    match outcome:
        case TaskCancelledRecord(was_status=was_status):
            return Ok(TaskCancelled(task_id=task_id, was_status=TaskStatus(was_status)))
        case CancelRefusedWorkflowTask(current_status=status):
            return _workflow_bound(task_id, TaskStatus(status))
        case CancelRefusedStatus(current_status=status):
            return _not_cancellable(task_id, TaskStatus(status))
        case CancelTaskAbsent():
            return _not_found(task_id)
        case CancelFailed(message=message, retryable=retryable, exception=exception):
            return _db_err(task_id, message, retryable, exception)
        case CancelStoredDataInvalid(message=message):
            return Err(
                TaskActionStoredDataInvalid(
                    message=f'Task cancel failed: stored data does not parse: {message}',
                    task_id=task_id,
                    exception=OutcomeDecodeError(message),
                )
            )
        case _ as unreachable:
            assert_never(unreachable)


async def cancel_task(
    broker: PostgresBroker,
    task_id: uuid.UUID,
    *,
    include_running: bool = False,
) -> Result[TaskCancelled, TaskActionError]:
    """Move a non-workflow task to CANCELLED.

    PENDING and CLAIMED tasks are always eligible: neither has begun user
    code, and a claimed task's worker aborts before starting when its
    pre-start ownership check fails. RUNNING requires ``include_running``,
    because the row flips while the process keeps executing — its side
    effects still happen and no attempt record is written for that run.

    Waiting result handles unblock on commit: the move fires ``task_done``
    and a CANCELLED row is reported as TASK_CANCELLED, so no result payload
    is written here. One ``horsies_cancel_task`` statement that the core
    runs on the broker's core handle; a cancel of the caller does not stop
    it. ``TaskActionEventLoopLimit`` is not an answer of this action: it
    uses no SQLAlchemy resource set.

    Raises:
        PanicException: the cancel outcome contradicts the function (a lost
            claim, or no observed status). These are defects of the core,
            not outcomes.
    """
    try:
        outcome = await broker._core_handle.cancel_task_async(  # pyright: ignore[reportPrivateUsage]
            task_id, include_running=include_running,
        )
    except CoreError as exc:
        return _db_err(task_id, str(exc), core_error_retryable(exc), exc)
    return _cancel_answer(task_id, outcome)
