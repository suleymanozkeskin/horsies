"""Database helpers for e2e tests."""

from __future__ import annotations

import asyncio
import time
from typing import Any, Callable

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession


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
