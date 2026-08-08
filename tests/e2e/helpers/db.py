"""Database helpers for e2e tests."""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Callable

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from horsies.core.types.status import TaskStatus


@dataclass(frozen=True, slots=True)
class TaskRow:
    """One task as the tests read it, from either lifecycle side.

    A terminal task is no longer a live row, so an ORM load of the live
    entity answers None for exactly the tasks these assertions are about.
    This carries the fields the e2e suites assert on, typed the way the
    entity typed them, and the claim and retry columns keep their live
    meaning: a history record has no claimant, and its retry_count is the
    count it terminalized with.
    """

    id: str
    task_name: str
    queue_name: str
    status: TaskStatus
    result: str | None
    retry_count: int
    max_retries: int
    claimed_by_worker_id: str | None
    claimed_at: datetime | None
    next_retry_at: datetime | None
    sent_at: datetime | None
    enqueued_at: datetime | None
    started_at: datetime | None
    completed_at: datetime | None
    failed_at: datetime | None
    error_code: str | None


_READ_TASK_SQL = text("""
    SELECT id, task_name, queue_name, status, result, retry_count,
           max_retries, claimed_by_worker_id, claimed_at, next_retry_at,
           sent_at, enqueued_at, started_at, completed_at, failed_at,
           error_code
    FROM itest_task_rows
    WHERE id = CAST(:id AS uuid)
""")


async def read_task(session: AsyncSession, task_id: str) -> TaskRow | None:
    """Read one task from whichever lifecycle side holds it.

    ``None`` means the task is on neither side — genuinely absent, not
    merely terminal.
    """
    row = (
        await session.execute(_READ_TASK_SQL, {'id': task_id})
    ).fetchone()
    if row is None:
        return None
    return TaskRow(
        id=str(row.id),
        task_name=row.task_name,
        queue_name=row.queue_name,
        status=TaskStatus(row.status),
        result=row.result,
        retry_count=row.retry_count,
        max_retries=row.max_retries,
        claimed_by_worker_id=row.claimed_by_worker_id,
        claimed_at=row.claimed_at,
        next_retry_at=row.next_retry_at,
        sent_at=row.sent_at,
        enqueued_at=row.enqueued_at,
        started_at=row.started_at,
        completed_at=row.completed_at,
        failed_at=row.failed_at,
        error_code=row.error_code,
    )


async def cleanup_tables(session: AsyncSession) -> None:
    """Truncate task-related tables between tests."""
    await session.execute(
        text("""
            TRUNCATE horsies_tasks, horsies_workflow_tasks, horsies_workflows, horsies_schedule_state, horsies_heartbeats CASCADE
        """),
    )
    await session.commit()


async def poll_max_during(
    session_factory: Callable[[], Any],
    sql: str,
    duration_s: float,
    poll_interval: float = 0.05,
    params: dict[str, Any] | None = None,
) -> int:
    """Poll DB for duration_s and return max COUNT(*) observed."""
    max_count = 0
    deadline = time.time() + duration_s

    while time.time() < deadline:
        async with session_factory() as session:
            result = await session.execute(text(sql), params or {})
            count = result.scalar() or 0
            max_count = max(max_count, count)
        await asyncio.sleep(poll_interval)

    return max_count


async def wait_for_all_terminal(
    session_factory: Callable[[], Any],
    task_ids: list[str],
    timeout_s: float = 30.0,
    poll_interval: float = 0.2,
) -> None:
    """Wait until all tasks reach terminal state (COMPLETED, FAILED, ERROR)."""
    deadline = time.time() + timeout_s

    while time.time() < deadline:
        async with session_factory() as session:
            result = await session.execute(
                text("""
                    SELECT COUNT(*) FROM horsies_tasks
                    WHERE id = ANY(:ids)
                    AND status NOT IN ('COMPLETED', 'FAILED', 'ERROR')
                """),
                {'ids': task_ids},
            )
            pending = result.scalar() or 0
            if pending == 0:
                return
        await asyncio.sleep(poll_interval)

    raise TimeoutError(f'Tasks did not complete within {timeout_s}s')


async def wait_for_status(
    session_factory: Callable[[], Any],
    task_id: str,
    target_status: str,
    timeout_s: float = 15.0,
    poll_interval: float = 0.2,
) -> None:
    """Wait until a single task reaches the target status."""
    deadline = time.time() + timeout_s

    while time.time() < deadline:
        async with session_factory() as session:
            result = await session.execute(
                text("""
                    SELECT status FROM horsies_tasks WHERE id = :id
                """),
                {'id': task_id},
            )
            row = result.fetchone()
            if row is not None and row[0] == target_status:
                return
        await asyncio.sleep(poll_interval)

    raise TimeoutError(
        f'Task {task_id} did not reach status {target_status} within {timeout_s}s'
    )


async def wait_for_any_status(
    session_factory: Callable[[], Any],
    task_ids: list[str],
    target_status: str,
    timeout_s: float = 15.0,
    poll_interval: float = 0.2,
) -> None:
    """Wait until at least one task from the list reaches the target status."""
    deadline = time.time() + timeout_s

    while time.time() < deadline:
        async with session_factory() as session:
            result = await session.execute(
                text("""
                    SELECT COUNT(*) FROM horsies_tasks
                    WHERE id = ANY(:ids) AND status = :status
                """),
                {'ids': task_ids, 'status': target_status},
            )
            count = result.scalar() or 0
            if count > 0:
                return
        await asyncio.sleep(poll_interval)

    raise TimeoutError(
        f'No task reached status {target_status} within {timeout_s}s'
    )
