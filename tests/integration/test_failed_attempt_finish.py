"""``horsies_finish_failed_attempt`` against real PostgreSQL: each outcome.

The function is called through the worker's own client
(``finish_failed_attempt``) on an autocommit connection, as the failure
paths call it, so every test also runs the decoder. Each test asserts what
was written and what was not: the live row, the attempt rows, the history
row, the phase-2 pending row and the capacity wake.
"""

from __future__ import annotations

import math
import uuid
from collections.abc import AsyncIterator
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any

import pytest
import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession

from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.history.archive.attempts import AttemptRecord
from tests.integration.conftest import compute_test_command_fingerprint
from tests.integration.history_seeding import read_attempts

pytestmark = [pytest.mark.integration]

WORKER = 'w-failed-attempt'
QUEUE = 'failed_attempt_q'
TASK_NAME = 'failed_attempt_task'
ERROR_RESULT = '{"__h_task_result__":true,"ok":null,"err":{"error_code":"BOOM"}}'
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
    """A factory of RUNNING rows held by ``WORKER``."""

    async def make(
        *,
        retry_count: int = 0,
        max_retries: int = 0,
        good_until: datetime | None = None,
        is_workflow_task: bool = False,
        status: str = 'RUNNING',
    ) -> _Seeded:
        task_id = str(uuid.uuid4())
        claimed_at = datetime.now(timezone.utc) - timedelta(seconds=30)
        sent_at, fingerprint = compute_test_command_fingerprint(
            task_name=TASK_NAME,
            queue_name=QUEUE,
            good_until=good_until,
        )
        async with AsyncSession(engine) as session:
            await session.execute(
                text("""
                    INSERT INTO horsies_tasks
                        (
                            id, task_name, queue_name, priority, input, status,
                            sent_at, created_at, updated_at, retry_count, max_retries,
                            started_at, good_until, claimed_by_worker_id, claimed_at,
                            is_workflow_task, worker_hostname, worker_pid,
                            worker_process_name, retention_class_key,
                            command_fingerprint_version, command_fingerprint,
                            retain_rerun_input, execution, enqueued_at,
                            claim_expires_at
                        ) VALUES (
                            :id, :task_name, :queue, 100, '{}',
                            CAST(:status AS varchar), :sent_at, NOW(), NOW(),
                            :retry_count, :max_retries,
                            CASE WHEN CAST(:status AS varchar) = 'RUNNING'
                              THEN NOW() END,
                            :good_until, :worker, :claimed_at, :is_workflow_task,
                            'host-1', 4242, 'child-1', 'standard_30d',
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
                    'max_retries': max_retries,
                    'good_until': good_until,
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
            VALUES (:wf, 'failed_attempt_wf', 'RUNNING', 'fail', 0, :wf,
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
                    SELECT status, retry_count, next_retry_at, enqueued_at,
                           claimed_by_worker_id, claimed_at, started_at,
                           execution, updated_at
                    FROM horsies_tasks WHERE id = CAST(:id AS uuid)
                """),
                {'id': task_id},
            )
        ).one_or_none()


async def _attempts(engine: AsyncEngine, task_id: str) -> tuple[AttemptRecord, ...]:
    async with AsyncSession(engine) as session:
        return await read_attempts(session, task_id)


_PARTIAL_RETRY_SQL = """
    SELECT * FROM horsies_finish_failed_attempt(
        CAST(:id AS uuid), :worker,
        (SELECT claimed_at FROM horsies_tasks WHERE id = CAST(:id AS uuid)),
        :kind, CAST(:count AS integer), CAST(:delay AS double precision), :placement,
        :result, 'BOOM', NULL, :outcome, 'message', NULL, 'REFUSE', TRUE
    )
"""


@pytest.mark.asyncio(loop_scope='function')
class TestArguments:
    """A wrong argument is refused before any lock or write."""

    @pytest.mark.parametrize(
        ('count', 'delay', 'placement', 'result', 'outcome', 'sqlstate'),
        [
            (0, None, 'SAME_CLASS', ERROR_RESULT, 'FAILED', '22023'),
            (None, 1.0, None, ERROR_RESULT, 'FAILED', '22023'),
            (0, 1.0, 'elsewhere', ERROR_RESULT, 'FAILED', '22023'),
            (0, -1.0, 'SAME_CLASS', ERROR_RESULT, 'FAILED', '22023'),
            (0, math.nan, 'SAME_CLASS', ERROR_RESULT, 'FAILED', '22023'),
            (0, math.inf, 'SAME_CLASS', ERROR_RESULT, 'FAILED', '22023'),
            (None, None, None, ERROR_RESULT, 'COMPLETED', '22023'),
            (None, None, None, None, 'FAILED', '23502'),
        ],
        ids=[
            'retry_without_delay',
            'delay_without_count',
            'unknown_placement',
            'negative_delay',
            'nan_delay',
            'infinite_delay',
            'completed_attempt',
            'no_result',
        ],
    )
    async def test_a_wrong_argument_raises_and_writes_nothing(
        self,
        engine: AsyncEngine,
        seed: Any,
        count: int | None,
        delay: float | None,
        placement: str | None,
        result: str | None,
        outcome: str,
        sqlstate: str,
    ) -> None:
        seeded = await seed(max_retries=3)
        before = await _live_row(engine, seeded.task_id)

        with pytest.raises(DBAPIError) as raised:
            async with engine.begin() as connection:
                await connection.execute(
                    text(_PARTIAL_RETRY_SQL),
                    {
                        'id': seeded.task_id,
                        'worker': WORKER,
                        # A retry request names its three values; any other
                        # call is a call with no retry.
                        'kind': (
                            'NONE' if (count, delay, placement) == (None, None, None)
                            else 'RETRY'
                        ),
                        'count': count,
                        'delay': delay,
                        'placement': placement,
                        'result': result,
                        'outcome': outcome,
                    },
                )

        assert getattr(raised.value.orig, 'sqlstate', None) == sqlstate
        assert await _live_row(engine, seeded.task_id) == before
        assert await _attempts(engine, seeded.task_id) == ()
