"""Plans of the child statements that write one task row, the start and the
finalizing marker, under the statistics that a burst of sends leaves.

The rows are inserted PENDING and analyzed, then claimed or started with no
new ANALYZE: the statistics say that no row is CLAIMED or RUNNING. A prepared
statement of a child runs a generic plan after its first executions. The only
condition of these statements on an indexed column of ``horsies_tasks`` is the
id, so the plan reads ``horsies_tasks`` through its primary key alone.
"""

from __future__ import annotations

import re
from collections.abc import AsyncGenerator
from typing import Any, Final

import psycopg
import pytest
import pytest_asyncio
from psycopg import sql
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from horsies.core.worker.child_runner import (
    MARK_FINALIZING_SQL,
    mark_finalizing_params,
    set_running_params,
    set_running_sql,
)
from tests.integration.autocommit_support import QUEUE, TASK_NAME, WORKER_ID, psycopg_url
from tests.integration.conftest import compute_test_enqueue_sha

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope='function')]

ROWS: Final = 20_000
PRIMARY_KEY: Final = 'horsies_tasks_pkey'


@pytest_asyncio.fixture(loop_scope='function')
async def probe(db_url: str) -> AsyncGenerator[psycopg.AsyncConnection[Any]]:
    connection = await psycopg.AsyncConnection.connect(psycopg_url(db_url), autocommit=True)
    yield connection
    await connection.close()


async def _rows_with_drain_start_statistics(session: AsyncSession, status: str) -> str:
    """Insert ``ROWS`` PENDING tasks and ANALYZE them, then move every row to
    ``status``, owned by ``WORKER_ID``, with no new ANALYZE. Returns one id."""
    sent_at, sha = compute_test_enqueue_sha(task_name=TASK_NAME, queue_name=QUEUE)
    _ = await session.execute(
        text("""
            INSERT INTO horsies_tasks
                (id, task_name, queue_name, priority, args, kwargs,
                 status, sent_at, enqueued_at, created_at, updated_at,
                 claimed, retry_count, max_retries,
                 enqueue_sha, is_workflow_task, retention_class_key,
                 command_fingerprint_version, command_fingerprint,
                 retain_rerun_input, prepared_rerun_input_disposition)
            SELECT g.id, :task_name, :queue, 100, '[]', '{}',
                   'PENDING', :sent_at, NOW(), NOW(), NOW(),
                   FALSE, 0, 0,
                   :sha, FALSE, 'standard_30d',
                   1, sha256(convert_to(CAST(g.id AS text), 'UTF8')),
                   FALSE, 'DECLINED_BY_POLICY'
            FROM (SELECT gen_random_uuid() AS id FROM generate_series(1, :rows)) AS g
        """),
        {'task_name': TASK_NAME, 'queue': QUEUE, 'sent_at': sent_at, 'sha': sha, 'rows': ROWS},
    )
    await session.commit()
    _ = await session.execute(text('ANALYZE horsies_tasks'))
    await session.commit()
    _ = await session.execute(
        text("""
            UPDATE horsies_tasks
            SET status = :status,
                claimed = :claimed,
                claimed_at = NOW(),
                claimed_by_worker_id = :worker_id,
                started_at = CASE WHEN :started THEN NOW() END
        """),
        {
            'status': status,
            'claimed': status == 'CLAIMED',
            'started': status == 'RUNNING',
            'worker_id': WORKER_ID,
        },
    )
    await session.commit()
    task_id = (await session.execute(text('SELECT CAST(id AS text) FROM horsies_tasks LIMIT 1'))).scalar_one()
    await session.commit()
    return str(task_id)


async def _generic_plan(
    connection: psycopg.AsyncConnection[Any],
    statement: str,
    params: tuple[object, ...],
) -> str:
    """The generic plan of ``statement`` as psycopg prepares it for ``params``.

    The statement runs once, prepared by psycopg as a child runs it, in a
    transaction that rolls back. Its plan is then read with EXPLAIN EXECUTE
    under ``force_generic_plan``, with the values as literals of the
    parameter types that psycopg's preparation gave.
    """
    async with connection.transaction(force_rollback=True):
        _ = await connection.execute('SET LOCAL plan_cache_mode = force_generic_plan')
        _ = await connection.execute(statement, params, prepare=True)  # pyright: ignore[reportArgumentType]
        cursor = await connection.execute(
            'SELECT name, CAST(parameter_types AS text[]) '
            'FROM pg_prepared_statements WHERE NOT from_sql',
        )
        prepared = await cursor.fetchall()
        assert len(prepared) == 1, prepared
        name, types = prepared[0]
        arguments = sql.SQL(', ').join(
            sql.SQL('{}::{}').format(sql.Literal(param), sql.SQL(str(type_name)))  # pyright: ignore[reportArgumentType]
            for param, type_name in zip(params, types, strict=True)
        )
        cursor = await connection.execute(
            sql.SQL('EXPLAIN (COSTS OFF) EXECUTE {}({})').format(
                sql.Identifier(str(name)),
                arguments,
            ),
        )
        return '\n'.join(str(row[0]) for row in await cursor.fetchall())


async def _task_indexes_in(connection: psycopg.AsyncConnection[Any], plan: str) -> set[str]:
    cursor = await connection.execute(
        "SELECT indexname FROM pg_indexes WHERE tablename = 'horsies_tasks'",
    )
    names = {str(row[0]) for row in await cursor.fetchall()}
    return {name for name in names if re.search(rf'\b{re.escape(name)}\b', plan)}


async def test_the_start_reads_its_row_through_the_primary_key(
    session: AsyncSession,
    probe: psycopg.AsyncConnection[Any],
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    task_id = await _rows_with_drain_start_statistics(session, 'CLAIMED')

    plan = await _generic_plan(
        probe,
        set_running_sql(False),
        set_running_params(task_id, WORKER_ID, False, None),
    )

    assert await _task_indexes_in(probe, plan) == {PRIMARY_KEY}, plan


async def test_the_finalizing_marker_reads_its_row_through_the_primary_key(
    session: AsyncSession,
    probe: psycopg.AsyncConnection[Any],
    clean_workflow_tables: None,  # noqa: ARG001
) -> None:
    task_id = await _rows_with_drain_start_statistics(session, 'RUNNING')

    plan = await _generic_plan(
        probe,
        MARK_FINALIZING_SQL,
        mark_finalizing_params(task_id, WORKER_ID, None),
    )

    assert await _task_indexes_in(probe, plan) == {PRIMARY_KEY}, plan
