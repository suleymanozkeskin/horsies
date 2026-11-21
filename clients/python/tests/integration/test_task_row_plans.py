"""Plan of the row read inside ``horsies_mark_finalizing``, the finalizing
marker of a child, under the statistics that a burst of sends leaves. (The
start is one ``horsies_start_attempt`` call; the plans inside it are recorded
with the function.)

The rows are inserted PENDING and analyzed, then started with no new ANALYZE:
the statistics say that no row is RUNNING. PL/pgSQL runs the statements of a
function with a generic plan after their first executions. The only condition
of the marker's statements on an indexed column of ``horsies_tasks`` is the
id, so the plan reads ``horsies_tasks`` through its primary key alone.
"""

from __future__ import annotations

import re
from collections.abc import AsyncGenerator
from datetime import datetime, timezone
from typing import Any, Final, LiteralString

import psycopg
import pytest
import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from tests.integration.autocommit_support import QUEUE, TASK_NAME, WORKER_ID
from tests.integration.conftest import compute_test_command_fingerprint

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope='function')]

ROWS: Final = 20_000
PRIMARY_KEY: Final = 'horsies_tasks_pkey'
# A claim time for the fence parameter. The plan is generic, so its value
# does not change the plan.
CLAIMED_AT: Final = datetime(2026, 11, 10, 9, 0, tzinfo=timezone.utc)
MARK_FINALIZING_SQL: Final[LiteralString] = (
    'SELECT outcome FROM horsies_mark_finalizing(%s::uuid, %s, %s::timestamptz)'
)
"""The call of the finalizing marker, with the task, the worker and the
claim generation, as the core handle of a child sends it."""


@pytest_asyncio.fixture(loop_scope='function')
async def probe(db_url: str) -> AsyncGenerator[psycopg.AsyncConnection[Any]]:
    connection = await psycopg.AsyncConnection.connect(db_url, autocommit=True)
    yield connection
    await connection.close()


async def _rows_with_drain_start_statistics(session: AsyncSession, status: str) -> str:
    """Insert ``ROWS`` PENDING tasks and ANALYZE them, then move every row to
    ``status``, owned by ``WORKER_ID``, with no new ANALYZE. Returns one id."""
    sent_at, fingerprint = compute_test_command_fingerprint(
        task_name=TASK_NAME, queue_name=QUEUE,
    )
    _ = await session.execute(
        text("""
            INSERT INTO horsies_tasks
                (
                    id, task_name, queue_name, priority, input, status, sent_at,
                    enqueued_at, created_at, updated_at, retry_count, max_retries,
                    is_workflow_task, retention_class_key, command_fingerprint_version,
                    command_fingerprint, retain_rerun_input, execution
                )
                SELECT g.id, :task_name, :queue, 100, '{}', 'PENDING', :sent_at, NOW(),
                    NOW(), NOW(), 0, 0, FALSE, 'standard_30d', 1, :command_fingerprint,
                    FALSE, 'ISOLATED'
                FROM (SELECT gen_random_uuid() AS id FROM generate_series(1, :rows)) AS g
        """),
        {
            'task_name': TASK_NAME,
            'queue': QUEUE,
            'sent_at': sent_at,
            'command_fingerprint': fingerprint,
            'rows': ROWS,
        },
    )
    await session.commit()
    _ = await session.execute(text('ANALYZE horsies_tasks'))
    await session.commit()
    _ = await session.execute(
        text("""
            UPDATE horsies_tasks
            SET status = :status,
                claim_expires_at = CASE
                    WHEN :leased THEN NOW() + INTERVAL '5 minutes'
                END,
                claimed_at = NOW(),
                claimed_by_worker_id = :worker_id,
                started_at = CASE WHEN :started THEN NOW() END
        """),
        {
            'status': status,
            'leased': status == 'CLAIMED',
            'started': status == 'RUNNING',
            'worker_id': WORKER_ID,
        },
    )
    await session.commit()
    task_id = (await session.execute(text('SELECT CAST(id AS text) FROM horsies_tasks LIMIT 1'))).scalar_one()
    await session.commit()
    return str(task_id)


async def _nested_generic_plans(
    connection: psycopg.AsyncConnection[Any],
    statement: str,
    params: tuple[object, ...],
) -> str:
    """The plans of the statements inside the function that ``statement``
    calls, as generic plans, from ``auto_explain`` notices.

    The statement runs once in a transaction that rolls back, with
    ``plan_cache_mode = force_generic_plan``: PL/pgSQL plans each statement
    of the function with its parameters unknown, as after its first
    executions.
    """
    notices: list[str] = []

    def _collect(diagnostic: psycopg.errors.Diagnostic) -> None:
        notices.append(str(diagnostic.message_primary or ''))

    connection.add_notice_handler(_collect)
    try:
        async with connection.transaction(force_rollback=True):
            _ = await connection.execute("LOAD 'auto_explain'")
            for setting in (
                'auto_explain.log_min_duration = 0',
                'auto_explain.log_nested_statements = on',
                "auto_explain.log_level = 'notice'",
                'plan_cache_mode = force_generic_plan',
            ):
                _ = await connection.execute(f'SET LOCAL {setting}')
            _ = await connection.execute(statement, params)  # pyright: ignore[reportArgumentType]
    finally:
        connection.remove_notice_handler(_collect)
    return '\n'.join(notices)


async def _task_indexes_in(connection: psycopg.AsyncConnection[Any], plan: str) -> set[str]:
    cursor = await connection.execute(
        "SELECT indexname FROM pg_indexes WHERE tablename = 'horsies_tasks'",
    )
    names = {str(row[0]) for row in await cursor.fetchall()}
    return {name for name in names if re.search(rf'\b{re.escape(name)}\b', plan)}


async def test_the_finalizing_marker_reads_its_row_through_the_primary_key(
    session: AsyncSession,
    probe: psycopg.AsyncConnection[Any],
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    task_id = await _rows_with_drain_start_statistics(session, 'RUNNING')

    plans = await _nested_generic_plans(
        probe,
        MARK_FINALIZING_SQL,
        (task_id, WORKER_ID, CLAIMED_AT),
    )

    assert 'horsies_tasks t' in plans, plans
    assert await _task_indexes_in(probe, plans) == {PRIMARY_KEY}, plans
