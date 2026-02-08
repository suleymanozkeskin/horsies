"""Workflow helpers for e2e tests."""

from __future__ import annotations

import asyncio
from typing import Any

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker


async def wait_for_workflow_completion(
    session_factory: async_sessionmaker[AsyncSession],
    workflow_id: str,
    timeout_s: float = 30.0,
    poll_interval: float = 0.2,
) -> str:
    """
    Poll DB until workflow reaches terminal state (COMPLETED, FAILED, CANCELLED).

    Returns the final workflow status.
    Raises TimeoutError if not completed within timeout.
    """
    terminal_statuses = ('COMPLETED', 'FAILED', 'CANCELLED')
    deadline = asyncio.get_event_loop().time() + timeout_s
    last_status = 'UNKNOWN'

    while asyncio.get_event_loop().time() < deadline:
        async with session_factory() as session:
            result = await session.execute(
                text('SELECT status FROM horsies_workflows WHERE id = :wf_id'),
                {'wf_id': workflow_id},
            )
            row = result.fetchone()
            if row is None:
                raise ValueError(f'Workflow {workflow_id} not found')

            last_status = str(row[0])
            if last_status in terminal_statuses:
                return last_status

        await asyncio.sleep(poll_interval)

    raise TimeoutError(
        f'Workflow {workflow_id} did not complete within {timeout_s}s (last status: {last_status})'
    )


async def get_workflow_status(
    session_factory: async_sessionmaker[AsyncSession],
    workflow_id: str,
) -> str:
    """Get current status of a workflow."""
    async with session_factory() as session:
        result = await session.execute(
            text('SELECT status FROM horsies_workflows WHERE id = :wf_id'),
            {'wf_id': workflow_id},
        )
        row = result.fetchone()
        if row is None:
            raise ValueError(f'Workflow {workflow_id} not found')
        return str(row[0])


async def get_workflow_tasks(
    session_factory: async_sessionmaker[AsyncSession],
    workflow_id: str,
) -> list[dict[str, Any]]:
    """
    Fetch all workflow_tasks for a workflow, ordered by task_index.

    Returns list of dicts with: task_index, task_name, status, started_at, completed_at.
    """
    async with session_factory() as session:
        result = await session.execute(
            text("""
                SELECT task_index, task_name, status, started_at, completed_at
                FROM horsies_workflow_tasks
                WHERE workflow_id = :wf_id
                ORDER BY task_index
            """),
            {'wf_id': workflow_id},
        )
        rows = result.fetchall()
        return [
            {
                'task_index': row[0],
                'task_name': row[1],
                'status': row[2],
                'started_at': row[3],
                'completed_at': row[4],
            }
            for row in rows
        ]


async def get_workflow_task_status(
    session_factory: async_sessionmaker[AsyncSession],
    workflow_id: str,
    task_index: int,
) -> str:
    """Get status of a specific workflow task by index."""
    async with session_factory() as session:
        result = await session.execute(
            text("""
                SELECT status FROM horsies_workflow_tasks
                WHERE workflow_id = :wf_id AND task_index = :idx
            """),
            {'wf_id': workflow_id, 'idx': task_index},
        )
        row = result.fetchone()
        if row is None:
            raise ValueError(f'Workflow task {workflow_id}#{task_index} not found')
        return str(row[0])


async def poll_max_running_tasks(
    session_factory: async_sessionmaker[AsyncSession],
    workflow_id: str,
    duration_s: float,
    poll_interval: float = 0.05,
) -> int:
    """
    Poll DB for duration_s and return maximum concurrent RUNNING tasks.

    Queries the `tasks` table (not workflow_tasks) since that's where
    status='RUNNING' is set during execution.

    Used to verify parallelism in fan-out patterns.
    """
    deadline = asyncio.get_event_loop().time() + duration_s
    max_observed = 0

    while asyncio.get_event_loop().time() < deadline:
        async with session_factory() as session:
            # Join workflow_tasks to tasks to find RUNNING tasks for this workflow
            result = await session.execute(
                text("""
                    SELECT COUNT(*) FROM horsies_tasks t
                    JOIN horsies_workflow_tasks wt ON wt.task_id = t.id
                    WHERE wt.workflow_id = :wf_id AND t.status = 'RUNNING'
                """),
                {'wf_id': workflow_id},
            )
            count = result.scalar() or 0
            max_observed = max(max_observed, count)

        await asyncio.sleep(poll_interval)

    return max_observed


# Keep old name for backwards compatibility but mark as deprecated
async def poll_max_running_workflow_tasks(
    session_factory: async_sessionmaker[AsyncSession],
    workflow_id: str,
    duration_s: float,
    poll_interval: float = 0.05,
) -> int:
    """Deprecated: Use poll_max_running_tasks instead."""
    return await poll_max_running_tasks(
        session_factory, workflow_id, duration_s, poll_interval
    )
