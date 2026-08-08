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
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from enum import Enum
from typing import assert_never

from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from sqlalchemy.ext.asyncio import AsyncConnection

from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.history.reads.detail import (
    HistoryTaskDetail,
    read_task_detail,
    staged_detail_published,
)
from horsies.core.lifecycle.commands import CancelLockedTask
from horsies.core.lifecycle.fences import CallerHoldsRowLock
from horsies.core.lifecycle.outcomes import (
    AlreadyApplied,
    Applied,
    LostClaim,
    SourceStateConflict,
    TaskAbsent,
)
from horsies.core.lifecycle.persistence import apply_async
from horsies.core.types.result import Err, Ok, Result
from horsies.core.types.status import TaskStatus
from horsies.core.utils.db import is_retryable_connection_error


class TaskActionErrorCode(Enum):
    """Why a task action did not apply."""

    TASK_NOT_FOUND = 'TASK_NOT_FOUND'
    TASK_NOT_CANCELLABLE = 'TASK_NOT_CANCELLABLE'
    TASK_IS_WORKFLOW_TASK = 'TASK_IS_WORKFLOW_TASK'
    DB_OPERATION_FAILED = 'DB_OPERATION_FAILED'


@dataclass(frozen=True, slots=True)
class TaskActionError:
    """Error payload carried inside ``Err(...)`` for a task action.

    Fields:
        code: which failure applies
        message: human-readable description
        retryable: true only for transient database errors
        task_id: the task the action targeted
        current_status: the row's status when the action was refused;
            populated on state-conflict codes, None otherwise or when the
            stored status is not a recognized member
    """

    code: TaskActionErrorCode
    message: str
    retryable: bool
    task_id: str
    current_status: TaskStatus | None


@dataclass(frozen=True, slots=True)
class TaskCancelled:
    """A task moved to CANCELLED by this call."""

    task_id: str
    was_status: TaskStatus


_STATUS_BY_VALUE: dict[str, TaskStatus] = {
    status.value: status for status in TaskStatus
}

# Locks the row for the rest of the transaction and reports everything the
# cancel diagnosis needs; ``expiry_passed`` is evaluated against NOW(), the
# transaction timestamp.
_LOCK_TASK_SQL = text("""
    SELECT status,
           is_workflow_task,
           (good_until IS NOT NULL AND good_until <= NOW()) AS expiry_passed
    FROM horsies_tasks
    WHERE id = CAST(:id AS uuid)
    FOR UPDATE
""")

def _not_found(task_id: str) -> Err[TaskActionError]:
    """The row does not exist — retention may have removed it."""
    return Err(
        TaskActionError(
            code=TaskActionErrorCode.TASK_NOT_FOUND,
            message=f'Task {task_id} does not exist.',
            retryable=False,
            task_id=task_id,
            current_status=None,
        )
    )


def _workflow_bound(task_id: str) -> Err[TaskActionError]:
    """Workflow-bound rows are driven by the workflow primitives only."""
    return Err(
        TaskActionError(
            code=TaskActionErrorCode.TASK_IS_WORKFLOW_TASK,
            message=(
                f'Task {task_id} belongs to a workflow; use the workflow '
                f'actions instead.'
            ),
            retryable=False,
            task_id=task_id,
            current_status=None,
        )
    )


def _state_conflict(
    code: TaskActionErrorCode,
    message: str,
    task_id: str,
    current_status: TaskStatus | None,
) -> Err[TaskActionError]:
    """The row exists but its status does not admit the action."""
    return Err(
        TaskActionError(
            code=code,
            message=message,
            retryable=False,
            task_id=task_id,
            current_status=current_status,
        )
    )


def _db_err(action: str, task_id: str, exc: SQLAlchemyError) -> Err[TaskActionError]:
    """Wrap a transaction failure, classifying transient errors."""
    return Err(
        TaskActionError(
            code=TaskActionErrorCode.DB_OPERATION_FAILED,
            message=f'{action} failed: {exc}',
            retryable=is_retryable_connection_error(exc),
            task_id=task_id,
            current_status=None,
        )
    )


async def _diagnose_live_miss(
    connection: AsyncConnection, task_id: str
) -> Err[TaskActionError]:
    """Classify a task absent from the live table.

    Terminalization moves finished rows to history, so a terminal task
    is a live miss by design: the history read names it, and the answer
    is the same refusal a terminal live row produced before the split —
    a state conflict, or the workflow-bound refusal — never "not
    found" for a task the dashboard is still showing.
    """
    if not await staged_detail_published(connection):
        return _not_found(task_id)
    detail = await read_task_detail(connection, task_id=task_id)
    match detail:
        case HistoryTaskDetail(is_workflow_task=True):
            return _workflow_bound(task_id)
        case HistoryTaskDetail(status=status_value):
            return _state_conflict(
                TaskActionErrorCode.TASK_NOT_CANCELLABLE,
                f'Task {task_id} cannot be cancelled from status '
                f'{status_value}.',
                task_id,
                _STATUS_BY_VALUE.get(status_value),
            )
        case _:
            return _not_found(task_id)


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
    """
    permitted_statuses = (TaskStatus.PENDING, TaskStatus.CLAIMED)
    if include_running:
        permitted_statuses += (TaskStatus.RUNNING,)

    # Identity columns are uuid; text that does not parse as one cannot
    # name any task, and must not reach a CAST that would error instead.
    try:
        uuid.UUID(task_id)
    except ValueError:
        return _not_found(task_id)

    try:
        async with broker.session_factory() as session:
            locked = (await session.execute(_LOCK_TASK_SQL, {'id': task_id})).first()
            if locked is None:
                return await _diagnose_live_miss(
                    await session.connection(), task_id
                )
            status_value, is_workflow_task, _expiry_passed = locked
            current_status = _STATUS_BY_VALUE.get(status_value)

            outcome = await apply_async(
                await session.connection(),
                CancelLockedTask(
                    task_id=task_id,
                    fence=CallerHoldsRowLock(),
                    permitted_source_statuses=permitted_statuses,
                ),
            )
            match outcome:
                case Applied() if current_status is not None:
                    await session.commit()
                case AlreadyApplied() | SourceStateConflict():
                    if is_workflow_task:
                        return _workflow_bound(task_id)
                    return _state_conflict(
                        TaskActionErrorCode.TASK_NOT_CANCELLABLE,
                        f'Task {task_id} cannot be cancelled from status '
                        f'{status_value}.',
                        task_id,
                        current_status,
                    )
                case Applied():
                    raise RuntimeError(
                        'administrative cancellation applied from an '
                        f'unrecognized status {status_value}'
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

    return Ok(TaskCancelled(task_id=task_id, was_status=current_status))
