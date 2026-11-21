"""Terminal writes of test seeds: the baseline function that a run calls for each end.

Each function runs, on the caller's connection, the function of
``core/baseline/0004_lifecycle_functions.sql`` that a run
calls for that end, with no retry, and returns the outcome text of its one
row: ``APPLIED``, or the refusal that the function classified. A seed that
needs a terminal row gets it here, never through an insert of a terminal
state. The functions do not commit; the caller's transaction does.

The fence is the claim of the live row: the worker that the caller names,
and the claim generation (``claimed_at``) that the statement reads from the
row it ends. Every end except the cancel writes the attempt row
``retry_count + 1`` from the process facts of the live row (``worker_pid``,
``worker_hostname``, ``worker_process_name``, NOT NULL in
``horsies_task_attempts``), so a RUNNING seed row carries the facts that
``horsies_start_attempt`` writes (``insert_live_task`` does).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Final

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection

_CLAIM_OF_ROW: Final = (
    '(SELECT t.claimed_at FROM horsies_tasks t WHERE t.id = CAST(:task_id AS uuid))'
)
"""The claim generation of the live row that the statement ends."""

_COMPLETE_PLAIN_TASK_SQL: Final = text(
    'SELECT outcome FROM horsies_complete_task_fused('
    f'CAST(:task_id AS uuid), :worker_id, {_CLAIM_OF_ROW}, :result, TRUE)'
)
"""The worker's plain success, with the capacity wake."""

_COMPLETE_WORKFLOW_TASK_SQL: Final = text(
    'SELECT outcome FROM horsies_finish_workflow_success('
    f'CAST(:task_id AS uuid), :worker_id, {_CLAIM_OF_ROW}, :result)'
)
"""Phase 1 of a workflow task's success."""

_FAIL_RUNNING_TASK_SQL: Final = text(
    'SELECT outcome FROM horsies_finish_failed_attempt('
    f'CAST(:task_id AS uuid), :worker_id, {_CLAIM_OF_ROW}, '
    "'NONE', NULL, NULL, NULL, :result, :error_code, :failed_reason, "
    "'FAILED', :attempt_message, :attempt_reason, 'REFUSE', TRUE)"
)
"""The worker's failure of a task whose child reported, with no retry."""

_FAIL_STALE_TASK_SQL: Final = text(
    'SELECT outcome FROM horsies_finish_stale_attempt('
    "CAST(:task_id AS uuid), 'BEAT_AGE', CAST(:stale_after_ms AS INTEGER), "
    'CAST(:finalizing_stale_after_ms AS INTEGER), NULL, NULL, '
    "'NONE', NULL, NULL, :result, :error_code, :failed_reason, "
    ':failed_reason, :failed_reason)'
)
"""The stale RUNNING pass's failure, with no retry: the attempt message and
reason are the failed reason, as the pass writes them."""

_CANCEL_LIVE_TASK_SQL: Final = text(
    'SELECT outcome FROM horsies_cancel_task('
    'CAST(:task_id AS uuid), CAST(:include_running AS BOOLEAN))'
)
"""The administrative cancel of one plain task."""


@dataclass(frozen=True, slots=True)
class TaskFailure:
    """A terminal failure of a RUNNING task: its stored result, error code
    and failed reason, and the message and reason of its FAILED attempt row,
    which carries the same error code."""

    result_json: str
    error_code: str | None
    failed_reason: str | None
    attempt_message: str | None
    attempt_reason: str | None


def task_failure(
    *,
    result_json: str,
    error_code: str | None,
    failed_reason: str | None,
) -> TaskFailure:
    """A failure of the task: its attempt row has no message and records the
    failed reason of the task."""
    return TaskFailure(
        result_json=result_json,
        error_code=error_code,
        failed_reason=failed_reason,
        attempt_message=None,
        attempt_reason=failed_reason,
    )


@dataclass(frozen=True, slots=True)
class StaleFailure:
    """A stale RUNNING failure, judged on the thresholds in milliseconds."""

    stale_after_ms: int
    finalizing_stale_after_ms: int
    result_json: str
    error_code: str
    failed_reason: str


async def complete_plain_task(
    connection: AsyncConnection,
    task_id: str,
    *,
    worker_id: str,
    result_json: str,
) -> str:
    """The success of the RUNNING plain task that ``worker_id`` holds."""
    return str(
        (
            await connection.execute(
                _COMPLETE_PLAIN_TASK_SQL,
                {'task_id': task_id, 'worker_id': worker_id, 'result': result_json},
            )
        ).scalar_one()
    )


async def complete_workflow_task(
    connection: AsyncConnection,
    task_id: str,
    *,
    worker_id: str,
    result_json: str,
) -> str:
    """Phase 1 of the success of the RUNNING workflow task that
    ``worker_id`` holds. The move writes the phase-2 record of its node, so
    the node row must exist first."""
    return str(
        (
            await connection.execute(
                _COMPLETE_WORKFLOW_TASK_SQL,
                {'task_id': task_id, 'worker_id': worker_id, 'result': result_json},
            )
        ).scalar_one()
    )


async def fail_running_task(
    connection: AsyncConnection,
    task_id: str,
    *,
    worker_id: str,
    failure: TaskFailure,
) -> str:
    """The terminal failure of the RUNNING task that ``worker_id`` holds,
    plain or workflow. The move of a workflow task writes the phase-2 record
    of its node, so the node row must exist first."""
    return str(
        (
            await connection.execute(
                _FAIL_RUNNING_TASK_SQL,
                {
                    'task_id': task_id,
                    'worker_id': worker_id,
                    'result': failure.result_json,
                    'error_code': failure.error_code,
                    'failed_reason': failure.failed_reason,
                    'attempt_message': failure.attempt_message,
                    'attempt_reason': failure.attempt_reason,
                },
            )
        ).scalar_one()
    )


async def fail_stale_task(
    connection: AsyncConnection,
    task_id: str,
    *,
    failure: StaleFailure,
) -> str:
    """The stale RUNNING failure of ``task_id``, judged on its latest runner
    beat and the thresholds of ``failure``."""
    return str(
        (
            await connection.execute(
                _FAIL_STALE_TASK_SQL,
                {
                    'task_id': task_id,
                    'stale_after_ms': failure.stale_after_ms,
                    'finalizing_stale_after_ms': failure.finalizing_stale_after_ms,
                    'result': failure.result_json,
                    'error_code': failure.error_code,
                    'failed_reason': failure.failed_reason,
                },
            )
        ).scalar_one()
    )


async def cancel_live_task(
    connection: AsyncConnection,
    task_id: str,
    *,
    include_running: bool,
) -> str:
    """The administrative cancel of the live plain task ``task_id``; a
    RUNNING row only with ``include_running``."""
    return str(
        (
            await connection.execute(
                _CANCEL_LIVE_TASK_SQL,
                {'task_id': task_id, 'include_running': include_running},
            )
        ).scalar_one()
    )
