"""Cancel and retry actions for individual tasks.

Each action is one transaction: lock the target row, apply a compare-and-set,
and — when the CAS matches nothing — diagnose the reason from the row already
locked. Holding the lock is what makes the diagnosis trustworthy; a concurrent
claim either waits behind it or is rejected by the CAS.

Two boundaries are deliberate:

* Workflow-bound rows are refused. They are managed exclusively by the
  workflow primitives; resetting one would orphan or hang its node.
* Cancelling a RUNNING task flips the row durably, but the task's code keeps
  executing on its worker until it returns, and its side effects still happen.
  There is no safe cross-process kill, and this module does not attempt one.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import assert_never

from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from horsies.core.brokers.postgres import PostgresBroker
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
    TASK_NOT_RETRYABLE = 'TASK_NOT_RETRYABLE'
    TASK_EXPIRY_PASSED = 'TASK_EXPIRY_PASSED'
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


@dataclass(frozen=True, slots=True)
class TaskRetried:
    """A task reset to PENDING and re-enqueued by this call.

    ``next_attempt_number`` is the attempt the next run will record.
    """

    task_id: str
    was_status: TaskStatus
    next_attempt_number: int


# Statuses a manual retry accepts. A COMPLETED task is not re-runnable here,
# and a task still in flight must be cancelled before it can be retried.
_RETRYABLE_STATUSES: frozenset[TaskStatus] = frozenset(
    {TaskStatus.FAILED, TaskStatus.EXPIRED, TaskStatus.CANCELLED}
)

_STATUS_BY_VALUE: dict[str, TaskStatus] = {
    status.value: status for status in TaskStatus
}

# Locks the row for the rest of the transaction and reports everything both
# diagnoses need. ``expiry_passed`` is evaluated against NOW(), which is the
# transaction timestamp, so it is the exact negation of the retry CAS's
# expiry predicate rather than a second, skewed reading of the clock.
_LOCK_TASK_SQL = text("""
    SELECT status,
           is_workflow_task,
           (good_until IS NOT NULL AND good_until <= NOW()) AS expiry_passed
    FROM horsies_tasks
    WHERE id = :id
    FOR UPDATE
""")

# Terminal states are never overwritten by claim, finalize, auto-retry or the
# reaper — each of those is itself a CAS on the status it expects — so a
# committed CANCELLED here is final. ``failed_at`` is set so the row carries an
# end timestamp; without one its queue span would be unmeasurable.
_CANCEL_TASK_SQL = text("""
    UPDATE horsies_tasks
    SET status = 'CANCELLED',
        error_code = 'TASK_CANCELLED',
        failed_reason = 'Cancelled via monitoring API',
        failed_at = NOW(),
        claimed = FALSE,
        claimed_at = NULL,
        claimed_by_worker_id = NULL,
        claim_expires_at = NULL,
        finalizing_at = NULL,
        finalizing_by_worker_id = NULL,
        terminal_at = NOW(),
        updated_at = NOW()
    WHERE id = :id
      AND is_workflow_task = FALSE
      AND status = ANY(:allowed)
    RETURNING id
""")

# retry_count is set to the highest attempt already recorded, because the next
# run records attempt retry_count + 1 and the attempt table upserts on
# (task_id, attempt): a lower value would silently overwrite history, a higher
# one would leave a gap. max_retries and good_until are untouched — a manual
# retry grants neither extra automatic retries nor a longer life.
_RETRY_TASK_SQL = text("""
    UPDATE horsies_tasks
    SET status = 'PENDING',
        retry_count = COALESCE(
            (SELECT MAX(attempt) FROM horsies_task_attempts
             WHERE task_id = horsies_tasks.id),
            0
        ),
        next_retry_at = NULL,
        enqueued_at = NOW(),
        claimed = FALSE,
        claimed_at = NULL,
        claimed_by_worker_id = NULL,
        claim_expires_at = NULL,
        started_at = NULL,
        completed_at = NULL,
        failed_at = NULL,
        terminal_at = NULL,
        result = NULL,
        failed_reason = NULL,
        error_code = NULL,
        worker_pid = NULL,
        worker_hostname = NULL,
        worker_process_name = NULL,
        finalizing_at = NULL,
        finalizing_by_worker_id = NULL,
        updated_at = NOW()
    WHERE id = :id
      AND is_workflow_task = FALSE
      AND status IN ('FAILED', 'EXPIRED', 'CANCELLED')
      AND (good_until IS NULL OR good_until > NOW())
    RETURNING id, queue_name, retry_count
""")

# The queue notification fires on INSERT only, so an UPDATE back to PENDING
# wakes nobody. Without this the row waits for the next claim poll.
_NOTIFY_RETRY_SQL = text("""
    SELECT pg_notify('task_queue_' || :queue_name, 'retry:' || :id)
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

    try:
        async with broker.session_factory() as session:
            locked = (await session.execute(_LOCK_TASK_SQL, {'id': task_id})).first()
            if locked is None:
                return _not_found(task_id)
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


async def retry_task(
    broker: PostgresBroker,
    task_id: str,
) -> Result[TaskRetried, TaskActionError]:
    """Reset a settled non-workflow task to PENDING and re-enqueue it.

    The same row is reused with its original payload, queue and priority.
    Attempt history is preserved: ``retry_count`` becomes the highest attempt
    already recorded, so the next run numbers itself one past the last one
    instead of overwriting it.

    A task whose ``good_until`` has passed is refused rather than revived —
    a manual retry does not extend expiry.
    """
    try:
        async with broker.session_factory() as session:
            locked = (await session.execute(_LOCK_TASK_SQL, {'id': task_id})).first()
            if locked is None:
                return _not_found(task_id)
            status_value, is_workflow_task, expiry_passed = locked
            current_status = _STATUS_BY_VALUE.get(status_value)

            retried = (await session.execute(_RETRY_TASK_SQL, {'id': task_id})).first()
            if retried is None or current_status is None:
                if is_workflow_task:
                    return _workflow_bound(task_id)
                if current_status in _RETRYABLE_STATUSES and expiry_passed:
                    return _state_conflict(
                        TaskActionErrorCode.TASK_EXPIRY_PASSED,
                        f'Task {task_id} cannot be retried: its good_until '
                        f'has passed.',
                        task_id,
                        current_status,
                    )
                return _state_conflict(
                    TaskActionErrorCode.TASK_NOT_RETRYABLE,
                    f'Task {task_id} cannot be retried from status ' f'{status_value}.',
                    task_id,
                    current_status,
                )

            _retried_id, queue_name, retry_count = retried
            await session.execute(
                _NOTIFY_RETRY_SQL, {'queue_name': queue_name, 'id': task_id}
            )
            await session.commit()
    except SQLAlchemyError as exc:
        return _db_err('Task retry', task_id, exc)

    return Ok(
        TaskRetried(
            task_id=task_id,
            was_status=current_status,
            next_attempt_number=retry_count + 1,
        )
    )
