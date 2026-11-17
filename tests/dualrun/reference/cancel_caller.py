"""The monitoring cancel caller, before it called the core.

``horsies.monitoring.task_actions.cancel_task`` sent ``CANCEL_TASK_SQL``
(``horsies_cancel_task``) on an autocommit connection of the broker's
SQLAlchemy engine and decoded the one outcome row with the lifecycle outcome
decoder. ``cancel_task`` below is that function's body, with the three
helpers that only it used. It has the signature of the production function,
so the dual-run rebinding can put it in its place.
"""

from __future__ import annotations

import uuid
from typing import assert_never

from sqlalchemy.exc import SQLAlchemyError

from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.lifecycle.outcomes import (
    AlreadyApplied,
    Applied,
    LostClaim,
    ObservedTaskState,
    ObservedWorkflowTask,
    OutcomeDecodeError,
    SourceStateConflict,
    TaskAbsent,
    TerminalizationOutcome,
    decode_outcome_row,
)
from horsies.core.types.result import Err, Ok, Result
from horsies.core.types.status import TaskStatus
from horsies.core.utils.db import autocommit_connection, is_retryable_connection_error
from horsies.monitoring.task_actions import (
    CANCEL_TASK_SQL,
    TaskActionDbOperationFailed,
    TaskActionError,
    TaskActionEventLoopLimit,
    TaskActionNotCancellable,
    TaskActionNotFound,
    TaskActionStoredDataInvalid,
    TaskActionWorkflowTask,
    TaskCancelled,
)


def db_err(
    action: str, task_id: uuid.UUID, exc: SQLAlchemyError,
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


def stored_data_err(
    action: str,
    task_id: uuid.UUID,
    exc: OutcomeDecodeError,
) -> Err[TaskActionStoredDataInvalid]:
    """Wrap stored data that does not parse. The transaction did not commit."""
    return Err(
        TaskActionStoredDataInvalid(
            message=f'{action} failed: stored data does not parse: {exc}',
            task_id=task_id,
            exception=exc,
        )
    )


def cancel_answer(
    task_id: uuid.UUID,
    outcome: TerminalizationOutcome,
) -> Result[
    TaskCancelled,
    TaskActionNotFound | TaskActionWorkflowTask | TaskActionNotCancellable,
]:
    """The answer of the cancel to its caller, from the decoded outcome. Pure.

    Raises:
        RuntimeError: an outcome that contradicts the function: a lost
            claim, or an applied, replayed or refused cancel with no
            observed status.
    """
    match outcome:
        case Applied(observed=ObservedTaskState(status=TaskStatus() as was_status)):
            return Ok(TaskCancelled(task_id=task_id, was_status=was_status))
        case SourceStateConflict(evidence=ObservedWorkflowTask(observed_status=status)):
            return Err(
                TaskActionWorkflowTask(
                    message=(
                        f'Task {task_id} belongs to a workflow; use the workflow '
                        f'actions instead.'
                    ),
                    task_id=task_id,
                    current_status=status,
                )
            )
        case (
            SourceStateConflict(observed=ObservedTaskState(status=TaskStatus() as status))
            | AlreadyApplied(observed=ObservedTaskState(status=TaskStatus() as status))
        ):
            return Err(
                TaskActionNotCancellable(
                    message=(
                        f'Task {task_id} cannot be cancelled from status '
                        f'{status.value}.'
                    ),
                    task_id=task_id,
                    current_status=status,
                )
            )
        case TaskAbsent():
            return Err(
                TaskActionNotFound(
                    message=f'Task {task_id} does not exist.',
                    task_id=task_id,
                )
            )
        case Applied() | AlreadyApplied() | SourceStateConflict():
            raise RuntimeError(
                f'administrative cancellation of task {task_id} '
                'reported no observed status'
            )
        case LostClaim():
            raise RuntimeError(
                f'administrative cancellation of task {task_id} reported a '
                'lost claim; the cancel has no claim fence'
            )
        case _ as unreachable:
            assert_never(unreachable)


async def cancel_task(
    broker: PostgresBroker,
    task_id: uuid.UUID,
    *,
    include_running: bool = False,
) -> Result[TaskCancelled, TaskActionError]:
    """Move a non-workflow task to CANCELLED: one ``horsies_cancel_task``
    statement in autocommit on the broker's SQLAlchemy engine."""
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
            connection = await autocommit_connection(session)
            rows = (
                await connection.execute(
                    CANCEL_TASK_SQL,
                    {'task_id': str(task_id), 'include_running': include_running},
                )
            ).mappings().all()
            assert len(rows) == 1, (
                f'horsies_cancel_task returned {len(rows)} rows for task {task_id}'
            )
            outcome = decode_outcome_row(
                {str(key): value for key, value in rows[0].items()}
            )
    except SQLAlchemyError as exc:
        return db_err('Task cancel', task_id, exc)
    except OutcomeDecodeError as exc:
        return stored_data_err('Task cancel', task_id, exc)
    return cancel_answer(task_id, outcome)
