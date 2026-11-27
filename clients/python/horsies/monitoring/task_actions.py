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
* Cancelling a RUNNING task (``policy=CancelPolicy.INCLUDE_RUNNING``) moves
  the row to history at once and records the running attempt with the
  outcome ``CANCELLED`` in the same statement. The worker that runs it gets the
  cancel notice at commit and gives the cancel to the task code: an async
  body gets ``asyncio.CancelledError`` at its next ``await``, and
  ``TaskRuntime.cancel_state()`` reads ``CancelRequested(OPERATOR)``. Code
  that does not stop runs on until it returns; its side effects still
  happen, and what it returns is dropped. An ``ISOLATED`` task with
  ``cancel_grace_ms`` is killed with its child that long after the cancel
  reached it.

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

import enum
import uuid
from dataclasses import dataclass
from typing import assert_never


from horsies._catalog.monitoring import TaskActionErrorCode as TaskActionErrorCode
from horsies._catalog.monitoring_errors import (
    TaskActionCoreError as TaskActionCoreError,
    TaskActionDatabaseOperationFailed as TaskActionDatabaseOperationFailed,
    TaskActionIsWorkflowTask as TaskActionIsWorkflowTask,
    TaskActionNotCancellable as TaskActionNotCancellable,
    TaskActionNotFound as TaskActionNotFound,
    TaskActionStoredDataInvalid as TaskActionStoredDataInvalid,
)
from horsies._core import CoreError, TaskCancelledRecord
from horsies.core.app import Horsies
from horsies.core.types.core_failure import call_refusal_or_raise, core_error_retryable
from horsies.core.types.result import Err, Ok, Result
from horsies.core.types.status import TaskStatus


class CancelPolicy(str, enum.Enum):
    """The task statuses that ``cancel_task`` acts on."""

    PENDING_OR_CLAIMED_ONLY = 'PENDING_OR_CLAIMED_ONLY'
    """A PENDING or CLAIMED task is cancelled; a RUNNING task is not
    cancellable."""
    INCLUDE_RUNNING = 'INCLUDE_RUNNING'
    """A RUNNING task is cancelled too."""


type TaskActionError = TaskActionCoreError
"""Every failure of a task action: one variant per ``TaskActionErrorCode``,
each a class of the core's catalog."""


@dataclass(frozen=True, slots=True)
class TaskCancelled:
    """A task moved to CANCELLED by this call."""

    task_id: uuid.UUID
    was_status: TaskStatus


def _db_err(
    task_id: uuid.UUID,
    message: str,
    retryable: bool,
    exc: BaseException,
) -> Err[TaskActionDatabaseOperationFailed]:
    """The core of this process could not run the cancel."""
    return Err(
        TaskActionDatabaseOperationFailed(
            message=f'Task cancel failed: {message}',
            retryable=retryable,
            task_id=task_id,
            cause=exc,
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
            | TaskActionIsWorkflowTask()
            | TaskActionDatabaseOperationFailed()
            | TaskActionStoredDataInvalid()
        ):
            return Err(outcome)
        case _ as unreachable:
            assert_never(unreachable)


async def cancel_task(
    app: Horsies,
    task_id: uuid.UUID,
    *,
    policy: CancelPolicy = CancelPolicy.PENDING_OR_CLAIMED_ONLY,
) -> Result[TaskCancelled, TaskActionError]:
    """Move a non-workflow task to CANCELLED.

    PENDING and CLAIMED tasks are always eligible: neither has begun user
    code, and a claimed task's worker aborts before starting when its
    pre-start ownership check fails. RUNNING requires
    ``CancelPolicy.INCLUDE_RUNNING``,
    because the code of the run may already have side effects: the same
    statement records the running attempt with the outcome ``CANCELLED``,
    and the worker gives the cancel to the task code (cooperative; a hard
    stop only for an ``ISOLATED`` task with ``cancel_grace_ms``).

    Waiting result handles unblock on commit: the move fires ``task_done``
    and a CANCELLED row is reported as TASK_CANCELLED, so no result payload
    is written here. One ``horsies_cancel_task`` statement that the core
    runs on the core handle of the app's broker; a cancel of the caller does
    not stop it.

    Raises:
        HorsiesError: the app cannot build its broker
            (``DATABASE_INIT_FAILED``, ``Horsies.get_broker``). Nothing ran.
        ForkedAfterCoreStart, CoreStopped: the core of this process refuses
            every call (``call_refusal_or_raise``). Nothing ran.
        PanicException: the cancel outcome contradicts the function (a lost
            claim, or no observed status). These are defects of the core,
            not outcomes.
    """
    try:
        outcome = await app.get_broker()._core_handle.cancel_task_async(  # pyright: ignore[reportPrivateUsage]
            task_id, policy=policy.value,
        )
    except CoreError as exc:
        refusal = call_refusal_or_raise(exc)
        return _db_err(task_id, str(refusal), core_error_retryable(refusal), refusal)
    return _cancel_answer(outcome)
