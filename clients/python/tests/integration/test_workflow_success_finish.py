"""``horsies_finish_workflow_success`` against real PostgreSQL: each outcome.

The function is called through the worker's own client
(``finish_workflow_success``), in autocommit, as phase 1 calls it, so every
test also runs the decoder. Each test asserts what was written and what was
not: the live row, the attempt rows, the history row, the phase-2 record of
the node and the capacity wake.
"""

from __future__ import annotations

import uuid
from collections.abc import AsyncIterator
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Final

import psycopg
import pytest
import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from horsies import TaskAttemptInfo
from horsies.core.brokers.postgres import PostgresBroker
from tests.integration.conftest import compute_test_command_fingerprint
from tests.integration.history_seeding import read_attempts

pytestmark = [pytest.mark.integration]

WORKER: Final = 'w-workflow-success'
QUEUE: Final = 'workflow_success_q'
TASK_NAME: Final = 'workflow_success_task'
OK_RESULT: Final = '{"__h_task_result__":true,"ok":7,"err":null}'
"""How long a test listens for a capacity wake that must not come."""


@dataclass(frozen=True, slots=True)
class _Seeded:
    task_id: str
    claimed_at: datetime


@pytest_asyncio.fixture
async def seed(
    engine: AsyncEngine,
    broker: PostgresBroker,  # noqa: ARG001 - schema and history leaves
) -> AsyncIterator[Any]:
    """A factory of rows held by ``WORKER``; a workflow task gets its node."""

    async def make(
        *,
        retry_count: int = 0,
        is_workflow_task: bool = True,
        status: str = 'RUNNING',
    ) -> _Seeded:
        task_id = str(uuid.uuid4())
        claimed_at = datetime.now(timezone.utc) - timedelta(seconds=30)
        sent_at, fingerprint = compute_test_command_fingerprint(
            task_name=TASK_NAME, queue_name=QUEUE,
        )
        async with AsyncSession(engine) as session:
            await session.execute(
                text("""
                    INSERT INTO horsies_tasks
                        (
                            id, task_name, queue_name, priority, input, status,
                            sent_at, created_at, updated_at, retry_count, max_retries,
                            started_at, claimed_by_worker_id, claimed_at,
                            is_workflow_task, worker_hostname, worker_pid,
                            worker_process_name, retention_class_key,
                            command_fingerprint_version, command_fingerprint,
                            retain_rerun_input, execution, enqueued_at,
                            claim_expires_at
                        ) VALUES (
                            :id, :task_name, :queue, 100, '{}',
                            CAST(:status AS varchar), :sent_at, NOW(), NOW(),
                            :retry_count, 3,
                            CASE WHEN CAST(:status AS varchar) = 'RUNNING'
                              THEN NOW() END,
                            :worker, :claimed_at, :is_workflow_task, 'host-1', 4242,
                            'child-1', 'standard_30d',
                            CASE WHEN CAST(:is_workflow_task AS boolean) THEN NULL ELSE 1 END,
                            CASE WHEN CAST(:is_workflow_task AS boolean) THEN NULL ELSE :command_fingerprint END,
                            FALSE, 'ISOLATED', NOW(),
                            CASE WHEN CAST(:status AS varchar) = 'CLAIMED' THEN NOW() + INTERVAL '5 minutes' END
                        )
                """),
                {
                    'id': task_id,
                    'task_name': TASK_NAME,
                    'queue': QUEUE,
                    'status': status,
                    'sent_at': sent_at,
                    'retry_count': retry_count,
                    'command_fingerprint': fingerprint,
                    'worker': WORKER,
                    'claimed_at': claimed_at,
                    'is_workflow_task': is_workflow_task,
                },
            )
            if is_workflow_task:
                await _link_workflow_node(session, task_id)
            await session.commit()
        return _Seeded(task_id=task_id, claimed_at=claimed_at)

    yield make


async def _link_workflow_node(session: AsyncSession, task_id: str) -> None:
    workflow_id = str(uuid.uuid4())
    await session.execute(
        text("""
            INSERT INTO horsies_workflows
                (id, name, status, on_error, depth, root_workflow_id,
                 sent_at, created_at, started_at, updated_at)
            VALUES (:wf, 'workflow_success_wf', 'RUNNING', 'fail', 0, :wf,
                    NOW(), NOW(), NOW(), NOW())
        """),
        {'wf': workflow_id},
    )
    await session.execute(
        text("""
            INSERT INTO horsies_workflow_tasks
                (downstream_subworkflow, id, workflow_id, task_index, node_id, task_name, input,
                 queue_name, priority, dependencies,
                 allow_failed_deps, join_type, is_subworkflow, status,
                 task_id, created_at,
                 max_retries, execution, retention_class_key)
            VALUES ('NONE', :id, :wf, 0, 'node-0', :task_name, '{}', :queue,
                    100, '{}', FALSE, 'all', FALSE, 'RUNNING',
                    CAST(:task_id AS uuid), NOW(),
                    0, 'ISOLATED', 'standard_30d')
        """),
        {
            'id': str(uuid.uuid4()),
            'wf': workflow_id,
            'task_name': TASK_NAME,
            'queue': QUEUE,
            'task_id': task_id,
        },
    )


async def _live_row(engine: AsyncEngine, task_id: str) -> Any:
    async with engine.connect() as connection:
        return (
            await connection.execute(
                text("""
                    SELECT status, retry_count, claimed_by_worker_id,
                           claimed_at, started_at, updated_at
                    FROM horsies_tasks WHERE id = CAST(:id AS uuid)
                """),
                {'id': task_id},
            )
        ).one_or_none()


async def _history_row(engine: AsyncEngine, task_id: str) -> Any:
    async with engine.connect() as connection:
        return (
            await connection.execute(
                text("""
                    SELECT status, terminalization_kind, error_code,
                           convert_from(result_payload, 'UTF8') AS result
                    FROM horsies_task_history WHERE task_id = CAST(:id AS uuid)
                """),
                {'id': task_id},
            )
        ).one_or_none()


async def _attempts(broker: PostgresBroker, task_id: str) -> tuple[TaskAttemptInfo, ...]:
    return await read_attempts(broker, task_id)


_DIRECT_SQL: Final = """
    SELECT * FROM horsies_finish_workflow_success(
        CAST(:id AS uuid), :worker,
        (SELECT claimed_at FROM horsies_tasks WHERE id = CAST(:id AS uuid)), :result,
        'NOT_OBSERVED', NULL
    )
"""


@pytest.mark.asyncio(loop_scope='function')
class TestRefusedCall:
    """A call the function cannot serve raises and writes nothing."""

    @pytest.mark.parametrize(
        ('is_workflow_task', 'result', 'sqlstate', 'message'),
        [
            (False, OK_RESULT, '22023', 'is not a workflow task'),
            (True, None, '23502', 'a workflow task success needs a result payload'),
        ],
        ids=['plain_task', 'no_result'],
    )
    async def test_it_raises_and_writes_nothing(
        self,
        engine: AsyncEngine,
        broker: PostgresBroker,
        seed: Any,
        is_workflow_task: bool,
        result: str | None,
        sqlstate: str,
        message: str,
    ) -> None:
        """The function's own refusal, by its SQLSTATE and its message: the
        move would refuse a NULL result too, after the locks and the
        attempt row."""
        seeded = await seed(is_workflow_task=is_workflow_task)
        before = await _live_row(engine, seeded.task_id)

        with pytest.raises(DBAPIError) as raised:
            async with engine.begin() as connection:
                await connection.execute(
                    text(_DIRECT_SQL),
                    {'id': seeded.task_id, 'worker': WORKER, 'result': result},
                )

        original = raised.value.orig
        assert isinstance(original, psycopg.Error)
        assert original.sqlstate == sqlstate
        assert original.diag.message_primary is not None
        assert message in original.diag.message_primary
        assert await _live_row(engine, seeded.task_id) == before
        assert await _attempts(broker, seeded.task_id) == ()
        assert await _history_row(engine, seeded.task_id) is None
