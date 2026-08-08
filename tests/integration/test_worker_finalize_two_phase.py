"""Integration regression test for two-phase worker finalization."""

from __future__ import annotations

import asyncio
import uuid
from unittest.mock import AsyncMock, MagicMock

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from horsies.core.app import Horsies
from horsies.core.codec import JsonValue, encode_task_result
from horsies.core.codec.json_io import dumps_json
from horsies.core.models.app import AppConfig
from horsies.core.models.broker import PostgresConfig
from horsies.core.models.tasks import TaskError, TaskResult
from horsies.core.types.result import is_err
from horsies.core.worker.config import WorkerConfig
from horsies.core.worker.worker import Worker
from tests.integration.conftest import compute_test_enqueue_sha

_TEST_APP: Horsies | None = None


def _test_app() -> Horsies:
    """Lazy-built Horsies app registering ``two_phase_finalize_test``.

    Production workers wire ``_app`` via ``_locate_app(cfg.app_locator)``;
    this test bypasses preload and builds ``Worker`` directly, so the
    strict-serde phase-2 replay path needs the task registered to
    resolve ``task_ok_type``.
    """
    global _TEST_APP
    if _TEST_APP is None:
        cfg = AppConfig(
            broker=PostgresConfig(
                database_url='postgresql+psycopg://u:p@localhost/db',
            )
        )
        _TEST_APP = Horsies(cfg)

        @_TEST_APP.task(task_name='two_phase_finalize_test')
        def _two_phase_finalize_test() -> TaskResult[JsonValue, TaskError]:
            return TaskResult(ok=None)

    return _TEST_APP


pytestmark = [pytest.mark.integration]

# Owner id shared by the insert helper and the worker under test so the
# finalize-path ownership guard matches.
TEST_WORKER_ID = 'w-two-phase-test'


async def _insert_running_task(session: AsyncSession) -> str:
    """Insert a minimal horsies_tasks row in RUNNING state and return its id."""
    task_id = str(uuid.uuid4())
    sent_at, sha = compute_test_enqueue_sha(task_name='two_phase_finalize_test')
    await session.execute(
        text("""
            INSERT INTO horsies_tasks
                (id, task_name, queue_name, priority, args, kwargs,
                 status, sent_at, created_at, updated_at, claimed, retry_count,
                 max_retries, started_at, enqueue_sha, claimed_by_worker_id,
                 is_workflow_task,
                 retention_class_key, command_fingerprint_version,
                 command_fingerprint, retain_rerun_input,
                 prepared_rerun_input_disposition)
            VALUES
                (:id, 'two_phase_finalize_test', 'default', 100, '[]', '{}',
                 'RUNNING', :sent_at, NOW(), NOW(), FALSE, 0,
                 3, NOW(), :enqueue_sha, :claimed_by_worker_id, TRUE,
                 'standard_30d', 1,
                 sha256(convert_to(CAST(CAST(:id AS uuid) AS text), 'UTF8')),
                 FALSE, 'DECLINED_BY_POLICY')
        """),
        {
            'id': task_id,
            'sent_at': sent_at,
            'enqueue_sha': sha,
            'claimed_by_worker_id': TEST_WORKER_ID,
        },
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
    worker._app = _test_app()
    worker.worker_instance_id = TEST_WORKER_ID

    # Simulate workflow advancement failure after phase-1 task status persistence.
    worker._handle_workflow_task_if_needed = AsyncMock(
        side_effect=RuntimeError('phase-2 boom')
    )  # type: ignore[method-assign]

    envelope = encode_task_result(
        TaskResult(ok='phase-1-result'),
        JsonValue,
    )
    serialized = dumps_json(envelope)
    assert not is_err(serialized)

    loop = asyncio.get_running_loop()
    fut: asyncio.Future[tuple[bool, str, str | None]] = loop.create_future()
    fut.set_result((True, serialized.ok_value, None))

    # Should return Err (for callback-driven handling) but keep phase-1 durability.
    finalize_r = await worker._finalize_after(
        fut,
        task_id,
        task_name='two_phase_finalize_test',
        executor=MagicMock(),
    )
    assert is_err(finalize_r)

    row = (
        await session.execute(
            text('SELECT status, result FROM horsies_tasks WHERE id = :id'),
            {'id': task_id},
        )
    ).fetchone()
    assert row is not None
    assert row[0] == 'COMPLETED'
    assert row[1] == serialized.ok_value

    # Ensure phase-2 was actually attempted.
    worker._handle_workflow_task_if_needed.assert_awaited()
