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
``retryable`` and ``task_id``. Callers match on the variant. The core
builds the variant of a refused or failed cancel (the generated classes of
``horsies._catalog.monitoring_errors``) with its message and its
exception; this module returns it as it is.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, ClassVar, assert_never

from sqlalchemy import text

from horsies._catalog.monitoring import TaskActionErrorCode as TaskActionErrorCode
from horsies._catalog.monitoring_errors import (
    TaskActionCoreError as TaskActionCoreError,
    TaskActionDbOperationFailed as TaskActionDbOperationFailed,
    TaskActionNotCancellable as TaskActionNotCancellable,
    TaskActionNotFound as TaskActionNotFound,
    TaskActionStoredDataInvalid as TaskActionStoredDataInvalid,
    TaskActionWorkflowTask as TaskActionWorkflowTask,
)
from horsies._core import CoreError, TaskCancelledRecord
from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.types.core_failure import call_refusal_or_raise, core_error_retryable
from horsies.core.types.result import Err, Ok, Result
from horsies.core.types.status import TaskStatus


if TYPE_CHECKING:
    from horsies.core.brokers.resources import EventLoopLimit


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


type TaskActionError = TaskActionCoreError | TaskActionEventLoopLimit
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

def _db_err(
    task_id: uuid.UUID,
    message: str,
    retryable: bool,
    exc: BaseException,
) -> Err[TaskActionDbOperationFailed]:
    """The core of this process could not run the cancel."""
    return Err(
        TaskActionDbOperationFailed(
            message=f'Task cancel failed: {message}',
            retryable=retryable,
            task_id=task_id,
            exception=exc,
        )
    )


def _cancel_answer(
    outcome: TaskCancelledRecord | TaskActionCoreError,
) -> Result[TaskCancelled, TaskActionError]:
    """The answer of the cancel to its caller. Pure.

    A refused or failed cancel is the core's error class, returned as it
    is. A finished task is refused as it was when it was live: a workflow
    task with the workflow-bound refusal, a plain task as not cancellable
    from its terminal status (an earlier cancel included).
    """
    match outcome:
        case TaskCancelledRecord(task_id=task_id, was_status=was_status):
            return Ok(TaskCancelled(task_id=task_id, was_status=TaskStatus(was_status)))
        case (
            TaskActionNotFound()
            | TaskActionNotCancellable()
            | TaskActionWorkflowTask()
            | TaskActionDbOperationFailed()
            | TaskActionStoredDataInvalid()
        ):
            return Err(outcome)
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
        ForkedAfterCoreStart, CoreStopped: the core of this process refuses
            every call (``call_refusal_or_raise``). Nothing ran.
        PanicException: the cancel outcome contradicts the function (a lost
            claim, or no observed status). These are defects of the core,
            not outcomes.
    """
    try:
        outcome = await broker._core_handle.cancel_task_async(  # pyright: ignore[reportPrivateUsage]
            task_id, include_running=include_running,
        )
    except CoreError as exc:
        refusal = call_refusal_or_raise(exc)
        return _db_err(task_id, str(refusal), core_error_retryable(refusal), refusal)
    return _cancel_answer(outcome)
