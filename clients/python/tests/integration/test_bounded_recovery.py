'''Correctness and plan tests for bounded recovery scans.'''

# pyright: reportPrivateUsage=false

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from dataclasses import dataclass
from typing import Final, cast

import pytest
import pytest_asyncio
from pydantic import SecretStr
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.models.broker import PostgresConfig
# The audit statement before the redesign is the body of
# horsies_workflow_recovery_audit_page; the plan and lock tests read it here.
from tests.integration.conftest import DB_URL
from tests.integration.isolated_database import isolated_test_database
from tests.broker_resources_support import current_resources
from tests.sqlalchemy_url import sqlalchemy_url


pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


@pytest_asyncio.fixture
async def bounded_recovery_database() -> AsyncIterator[
    tuple[PostgresBroker, AsyncEngine]
]:
    async with isolated_test_database(DB_URL) as url:
        broker = PostgresBroker(
            PostgresConfig(database_url=SecretStr(url))
        )
        initialized = await broker.ensure_schema_initialized()
        assert initialized.is_ok(), initialized
        from tests.integration.history_seeding import ensure_history_seedable

        async with current_resources(broker).engine.begin() as connection:
            await ensure_history_seedable(connection)
        engine = create_async_engine(sqlalchemy_url(url))
        try:
            yield broker, engine
        finally:
            await engine.dispose()
            await broker.close_async()


_MAX_PLAN_DEPTH: Final = 64


@dataclass(frozen=True, slots=True)
class _NodeBuffers:
    """The shared-buffer counts of one plan node (EXPLAIN BUFFERS).

    ``hit`` plus ``read`` is the number of shared pages the node accessed:
    the plan's own work. ``dirtied`` and ``written`` depend on the server's
    buffer pool. A page that a checkpoint or an eviction cleaned after the
    seed is dirtied again when the scan sets its hint bits, and a backend
    that finds the pool full of dirty pages writes a dirty victim for each
    page it reads.
    """

    node_type: str
    hit: int
    read: int
    dirtied: int
    written: int

    @property
    def pages_accessed(self) -> int:
        return self.hit + self.read


def _plan_buffers(plan: object, depth: int = 0) -> list[_NodeBuffers]:
    """The buffer counts of every node of an EXPLAIN (FORMAT JSON) plan,
    root first. A plan deeper than ``_MAX_PLAN_DEPTH`` fails the test."""
    assert depth <= _MAX_PLAN_DEPTH, f'plan deeper than {_MAX_PLAN_DEPTH} nodes'
    assert isinstance(plan, dict), f'plan node is not an object: {plan!r}'
    fields = cast(dict[str, object], plan)

    def count(name: str) -> int:
        return int(cast(int, fields.get(name, 0)))

    node = _NodeBuffers(
        node_type=str(fields.get('Node Type')),
        hit=count('Shared Hit Blocks'),
        read=count('Shared Read Blocks'),
        dirtied=count('Shared Dirtied Blocks'),
        written=count('Shared Written Blocks'),
    )
    children = cast(list[object], fields.get('Plans', []))
    return [
        node,
        *(
            child_node
            for child in children
            for child_node in _plan_buffers(child, depth + 1)
        ),
    ]


async def _seed_orphan_task_plan_fixture(
    engine: AsyncEngine,
    *,
    rows: int,
) -> None:
    async with engine.begin() as connection:
        await connection.execute(
            text('''
                INSERT INTO horsies_tasks (
                    id, task_name, queue_name, priority, input, status, sent_at,
                    enqueued_at, is_workflow_task, retry_count, max_retries,
                    command_fingerprint_version, command_fingerprint,
                    retention_class_key, retain_rerun_input, created_at, updated_at,
                    execution
                )
                SELECT md5('orphan-plan-task-' || item::text)::uuid, 'orphan_plan_task',
                    'default', 100, '{}', 'PENDING',
                    TIMESTAMPTZ '2026-02-01 00:00:00+00'
                           + item * interval '1 millisecond',
                    TIMESTAMPTZ '2026-02-01 00:00:00+00'
                           + item * interval '1 millisecond',
                    TRUE, 0, 0, NULL, NULL, 'standard_30d', FALSE,
                    TIMESTAMPTZ '2026-02-01 00:00:00+00'
                           + item * interval '1 millisecond',
                    NOW(), 'ISOLATED'
                FROM generate_series(1, :rows) item
            '''),
            {'rows': rows},
        )
        await connection.execute(text('''
            INSERT INTO horsies_workflows (
                id, name, status, on_error, depth, root_workflow_id,
                sent_at, created_at, updated_at
            ) VALUES (
                md5('orphan-plan-workflow')::uuid, 'orphan_plan',
                'RUNNING', 'fail', 0, md5('orphan-plan-workflow')::uuid,
                TIMESTAMPTZ '2026-02-01 00:00:00+00',
                TIMESTAMPTZ '2026-02-01 00:00:00+00', NOW()
            )
        '''))
        await connection.execute(
            text('''
                INSERT INTO horsies_workflow_tasks (downstream_subworkflow,
                    id, workflow_id, task_index, task_name, input, queue_name,
                    priority, dependencies, allow_failed_deps, join_type,
                    status, task_id, is_subworkflow, created_at,
                    max_retries, execution, retention_class_key
                )
                SELECT 'NONE', md5('orphan-plan-node-' || item::text)::uuid,
                       md5('orphan-plan-workflow')::uuid,
                       item, 'orphan_plan_task', '{}', 'default', 100,
                       '{}'::integer[], FALSE, 'all', 'RUNNING',
                       md5('orphan-plan-task-' || item::text)::uuid,
                       FALSE,
                       TIMESTAMPTZ '2026-02-01 00:00:00+00'
                           + item * interval '1 millisecond',
                       0, 'ISOLATED', 'standard_30d'
                FROM generate_series(1, :linked_rows) item
            '''),
            {'linked_rows': rows - 1},
        )
        await connection.execute(text(
            'ANALYZE horsies_tasks, horsies_workflow_tasks'
        ))


async def _insert_new_orphan_task(
    engine: AsyncEngine,
    *,
    item: int,
) -> None:
    async with engine.begin() as connection:
        await connection.execute(
            text('''
                INSERT INTO horsies_tasks (
                    id, task_name, queue_name, priority, input, status, sent_at,
                    enqueued_at, is_workflow_task, retry_count, max_retries,
                    command_fingerprint_version, command_fingerprint,
                    retention_class_key, retain_rerun_input, created_at, updated_at,
                    execution
                ) VALUES (
                    md5('new-orphan-' || CAST(:item AS text))::uuid, 'new_orphan',
                    'default', 100, '{}', 'PENDING',
                    TIMESTAMPTZ '2026-03-01 00:00:00+00'
                        + :item * interval '1 second',
                    TIMESTAMPTZ '2026-03-01 00:00:00+00'
                        + :item * interval '1 second',
                    TRUE, 0, 0, NULL, NULL, 'standard_30d', FALSE,
                    TIMESTAMPTZ '2026-03-01 00:00:00+00'
                        + :item * interval '1 second',
                    NOW(), 'ISOLATED'
                )
            '''),
            {'item': item},
        )


async def test_orphan_cycle_upper_bound_ends_while_new_rows_arrive(
    bounded_recovery_database: tuple[PostgresBroker, AsyncEngine],
) -> None:
    _broker, engine = bounded_recovery_database
    await _seed_orphan_task_plan_fixture(engine, rows=5)

    for item in (1, 2):
        async with engine.begin() as connection:
            moved = int((await connection.execute(text('''
                SELECT count(*) FROM horsies_cancel_orphaned_tasks(2)
            '''))).scalar_one())
        assert moved == 0
        await _insert_new_orphan_task(engine, item=item)

    async with engine.begin() as connection:
        moved = int((await connection.execute(text('''
            SELECT count(*) FROM horsies_cancel_orphaned_tasks(2)
        '''))).scalar_one())
        cursor = (await connection.execute(text('''
            SELECT completed_cycles, last_scan_rows
            FROM horsies_recovery_scan_cursors
            WHERE scan_name = 'orphan_workflow_tasks'
        '''))).one()

    assert moved == 1
    assert cursor.completed_cycles == 1
    assert cursor.last_scan_rows == 1
    async with engine.connect() as connection:
        deferred = int((await connection.execute(text('''
            SELECT count(*) FROM horsies_tasks
            WHERE task_name = 'new_orphan'
        '''))).scalar_one())
    assert deferred == 2


async def test_orphan_cursor_lock_refuses_without_waiting(
    bounded_recovery_database: tuple[PostgresBroker, AsyncEngine],
) -> None:
    broker, engine = bounded_recovery_database
    async with engine.connect() as blocker:
        transaction = await blocker.begin()
        await blocker.execute(text('''
            SELECT 1
            FROM horsies_recovery_scan_cursors
            WHERE scan_name = 'orphan_workflow_tasks'
            FOR UPDATE
        '''))

        result = await asyncio.wait_for(
            broker.audit_orphaned_workflow_tasks(),
            timeout=2,
        )
        assert result.is_err()
        error = result.unwrap_err()
        original = getattr(error.cause, 'orig', error.cause)
        assert getattr(original, 'sqlstate', None) == '55P03'
        await transaction.rollback()


async def test_terminate_cancels_the_orphans_of_one_page_on_demand(
    bounded_recovery_database: tuple[PostgresBroker, AsyncEngine],
) -> None:
    """No worker runs here, so no reaper runs the orphan pass, as with
    ``auto_terminate_orphaned_workflow_tasks=False``: the call runs one page
    on demand. Of three PENDING workflow tasks, the one that no node links
    moves to history as CANCELLED; the two linked tasks stay live."""
    broker, engine = bounded_recovery_database
    await _seed_orphan_task_plan_fixture(engine, rows=3)

    result = await broker.terminate_orphaned_workflow_tasks()

    assert result.is_ok(), result
    assert result.unwrap() == 1
    async with engine.connect() as connection:
        live = int((await connection.execute(text('''
            SELECT count(*) FROM horsies_tasks
            WHERE task_name = 'orphan_plan_task' AND status = 'PENDING'
        '''))).scalar_one())
        archived = (await connection.execute(text('''
            SELECT status, terminalization_kind FROM horsies_task_history
            WHERE task_id = md5('orphan-plan-task-3')::uuid
        '''))).one()
    assert live == 2
    assert (archived.status, archived.terminalization_kind) == (
        'CANCELLED', 'CANCEL_ORPHAN_SWEEP',
    )


async def test_orphan_task_scan_has_a_strict_plan_and_reaches_cycle_end(
    bounded_recovery_database: tuple[PostgresBroker, AsyncEngine],
) -> None:
    _broker, engine = bounded_recovery_database
    rows = 50_001
    await _seed_orphan_task_plan_fixture(engine, rows=rows)
    # The planner reads the statistics of the seeded rows, as autovacuum
    # leaves them in a live database. Without this the plan depends on
    # whether autovacuum analyzed the tables before the EXPLAIN.
    async with engine.begin() as connection:
        await connection.execute(text(
            'ANALYZE horsies_tasks, horsies_workflow_tasks, horsies_workflows'
        ))

    async with engine.begin() as connection:
        await connection.execute(text('''
            UPDATE horsies_recovery_scan_cursors cursor
            SET last_created_at = task.created_at,
                last_id = task.id,
                cycle_upper_created_at = upper_task.created_at,
                cycle_upper_id = upper_task.id,
                completed_cycles = 0,
                last_scan_at = statement_timestamp()
            FROM horsies_tasks task, horsies_tasks upper_task
            WHERE cursor.scan_name = 'orphan_workflow_tasks'
              AND task.id = md5('orphan-plan-task-49500')::uuid
              AND upper_task.id = md5('orphan-plan-task-50001')::uuid
        '''))
        plan_result = await connection.execute(text('''
            EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON)
            SELECT * FROM horsies_cancel_orphaned_tasks(500)
        '''))
        plan = cast(object, plan_result.scalar_one()[0]['Plan'])

    buffers = _plan_buffers(plan)
    assert max(node.pages_accessed for node in buffers) <= 3_500, buffers

    async with engine.begin() as connection:
        await connection.execute(text('''
            UPDATE horsies_recovery_scan_cursors
            SET last_created_at = NULL, last_id = NULL,
                cycle_upper_created_at = NULL, cycle_upper_id = NULL,
                completed_cycles = 0, last_scan_rows = 0,
                last_candidate_rows = 0
            WHERE scan_name = 'orphan_workflow_tasks'
        '''))

    transitioned = 0
    completed_cycles = 0
    for _ in range(102):
        async with engine.begin() as connection:
            transitioned += int((await connection.execute(text('''
                SELECT count(*) FROM horsies_cancel_orphaned_tasks(500)
            '''))).scalar_one())
            completed_cycles = int((await connection.execute(text('''
                SELECT completed_cycles
                FROM horsies_recovery_scan_cursors
                WHERE scan_name = 'orphan_workflow_tasks'
            '''))).scalar_one())
        if completed_cycles > 0:
            break

    assert completed_cycles == 1
    assert transitioned == 1
    async with engine.connect() as connection:
        live = int((await connection.execute(text('''
            SELECT count(*) FROM horsies_tasks
            WHERE id = md5('orphan-plan-task-50001')::uuid
        '''))).scalar_one())
        archived = int((await connection.execute(text('''
            SELECT count(*) FROM horsies_task_history
            WHERE task_id = md5('orphan-plan-task-50001')::uuid
        '''))).scalar_one())
    assert (live, archived) == (0, 1)


