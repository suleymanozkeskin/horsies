"""Integration regression test for two-phase worker finalization."""

from __future__ import annotations

import asyncio
import uuid
from unittest.mock import AsyncMock, MagicMock

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from horsies.core.codec.serde import dumps_json
from horsies.core.models.tasks import TaskResult
from horsies.core.types.result import is_err
from horsies.core.worker.config import WorkerConfig
from horsies.core.worker.worker import Worker

pytestmark = [pytest.mark.integration]


async def _insert_running_task(session: AsyncSession) -> str:
    """Insert a minimal horsies_tasks row in RUNNING state and return its id."""
    task_id = str(uuid.uuid4())
    await session.execute(
        text("""
            INSERT INTO horsies_tasks
                (id, task_name, queue_name, priority, args, kwargs,
                 status, sent_at, created_at, updated_at, claimed, retry_count,
                 max_retries, started_at)
            VALUES
                (:id, 'two_phase_finalize_test', 'default', 100, '[]', '{}',
                 'RUNNING', NOW(), NOW(), NOW(), FALSE, 0,
                 3, NOW())
        """),
        {'id': task_id},
    )
    await session.commit()
    return task_id


@pytest.mark.asyncio(loop_scope='function')
async def test_finalize_phase2_failure_keeps_terminal_task_result_durable(
    engine: AsyncEngine,
    session: AsyncSession,
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    """Phase-2 workflow error must not roll back phase-1 task terminal persistence."""
    task_id = await _insert_running_task(session)

    cfg = WorkerConfig(
        dsn='postgresql+psycopg://u:p@localhost/db',
        psycopg_dsn='postgresql://u:p@localhost/db',
        queues=['default'],
    )
    sf = async_sessionmaker(engine, expire_on_commit=False)
    worker = Worker(session_factory=sf, listener=MagicMock(), cfg=cfg)

    # Simulate workflow advancement failure after phase-1 task status persistence.
    worker._handle_workflow_task_if_needed = AsyncMock(
        side_effect=RuntimeError('phase-2 boom')
    )  # type: ignore[method-assign]

    serialized = dumps_json(TaskResult(ok='phase-1-result'))
    assert not is_err(serialized)

    loop = asyncio.get_running_loop()
    fut: asyncio.Future[tuple[bool, str, str | None]] = loop.create_future()
    fut.set_result((True, serialized.ok_value, None))

    # Should return Err (for callback-driven handling) but keep phase-1 durability.
    finalize_r = await worker._finalize_after(fut, task_id)
    assert is_err(finalize_r)

    row = (
        await session.execute(
            text("SELECT status, result FROM horsies_tasks WHERE id = :id"),
            {'id': task_id},
        )
    ).fetchone()
    assert row is not None
    assert row[0] == 'COMPLETED'
    assert row[1] == serialized.ok_value

    # Ensure phase-2 was actually attempted.
    worker._handle_workflow_task_if_needed.assert_awaited()
