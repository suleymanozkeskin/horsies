'''Correctness and plan tests for bounded recovery scans.'''

# pyright: reportPrivateUsage=false

from __future__ import annotations

import uuid
import asyncio
from collections.abc import AsyncIterator
from typing import cast

import pytest
import pytest_asyncio
from pydantic import SecretStr
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.models.broker import PostgresConfig
from horsies.core.workflows.recovery import (
    FAIL_ORPHANED_WORKFLOW_SQL,
    GLOBAL_SCAN_ROW_CAP,
    GLOBAL_WORKFLOW_AUDIT_CLAIM_TTL_MS,
    GLOBAL_WORKFLOW_AUDIT_SQL,
    LOCK_GLOBAL_WORKFLOW_AUDIT_CLAIM_SQL,
    _GlobalCandidateOutcome,
    _recover_global_orphan_candidate,
    recover_stuck_workflows_global,
)
from horsies.core.schemas import recovery as recovery_schema
from tests.integration.conftest import DB_URL
from tests.integration.isolated_database import isolated_test_database


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

        async with broker.async_engine.begin() as connection:
            await ensure_history_seedable(connection)
        engine = create_async_engine(url)
        try:
            yield broker, engine
        finally:
            await engine.dispose()
            await broker.close_async()


def _relation_rows_examined(plan: object, relation: str) -> float:
    if isinstance(plan, list):
        items = cast(list[object], plan)
        return sum(_relation_rows_examined(item, relation) for item in items)
    if not isinstance(plan, dict):
        return 0.0
    fields = cast(dict[str, object], plan)
    current = 0.0
    if fields.get('Relation Name') == relation:
        rows_per_loop = sum(
            float(cast(int | float, fields.get(name, 0)))
            for name in (
                'Actual Rows',
                'Rows Removed by Filter',
                'Rows Removed by Index Recheck',
            )
        )
        current = rows_per_loop * float(
            cast(int | float, fields.get('Actual Loops', 0))
        )
    return current + sum(
        _relation_rows_examined(value, relation)
        for value in fields.values()
    )


def _maximum_shared_buffers(plan: object) -> int:
    if isinstance(plan, list):
        items = cast(list[object], plan)
        return max((_maximum_shared_buffers(item) for item in items), default=0)
    if not isinstance(plan, dict):
        return 0
    fields = cast(dict[str, object], plan)
    current = sum(
        int(cast(int | float, fields.get(name, 0)))
        for name in (
            'Shared Hit Blocks',
            'Shared Read Blocks',
            'Shared Dirtied Blocks',
            'Shared Written Blocks',
        )
    )
    return max(
        current,
        *(_maximum_shared_buffers(value) for value in fields.values()),
    )


async def _seed_workflow_page_fixture(
    engine: AsyncEngine,
    *,
    rows: int,
    terminal_last: bool,
) -> None:
    async with engine.begin() as connection:
        await connection.execute(
            text('''
                INSERT INTO horsies_workflows (
                    id, name, status, on_error, depth, root_workflow_id,
                    created_at, updated_at
                )
                SELECT md5('bounded-workflow-' || item::text)::uuid,
                       'bounded_' || item::text,
                       'RUNNING', 'fail', 0,
                       md5('bounded-workflow-' || item::text)::uuid,
                       TIMESTAMPTZ '2026-01-01 00:00:00+00'
                           + item * interval '1 millisecond',
                       NOW()
                FROM generate_series(1, :rows) item
            '''),
            {'rows': rows},
        )
        await connection.execute(
            text('''
                INSERT INTO horsies_workflow_tasks (
                    id, workflow_id, task_index, task_name, queue_name,
                    priority, dependencies, allow_failed_deps, join_type,
                    status, is_subworkflow, created_at
                )
                SELECT md5('bounded-node-' || item::text)::uuid,
                       md5('bounded-workflow-' || item::text)::uuid,
                       0, 'bounded_node', 'default', 100, '{}'::integer[],
                       FALSE, 'all',
                       CASE WHEN :terminal_last AND item = :rows
                            THEN 'COMPLETED' ELSE 'RUNNING' END,
                       FALSE,
                       TIMESTAMPTZ '2026-01-01 00:00:00+00'
                           + item * interval '1 millisecond'
                FROM generate_series(1, :rows) item
            '''),
            {'rows': rows, 'terminal_last': terminal_last},
        )


async def test_global_cursor_reaches_a_candidate_after_three_pages(
    bounded_recovery_database: tuple[PostgresBroker, AsyncEngine],
) -> None:
    broker, engine = bounded_recovery_database
    await _seed_workflow_page_fixture(
        engine,
        rows=GLOBAL_SCAN_ROW_CAP * 2 + 50,
        terminal_last=True,
    )

    reports = [
        await recover_stuck_workflows_global(broker.session_factory, broker)
        for _ in range(3)
    ]

    assert [
        report.metrics['case_2_3'].rows_selected for report in reports
    ] == [200, 200, 50]
    assert reports[-1].recovered == 1
    async with engine.connect() as connection:
        cursor = (
            await connection.execute(text('''
                SELECT last_id, cycle_upper_id, completed_cycles
                FROM horsies_recovery_scan_cursors
                WHERE scan_name = 'running_workflows'
            '''))
        ).one()
        assert cursor.last_id is None
        assert cursor.cycle_upper_id is None
        assert cursor.completed_cycles == 1


async def test_global_workflow_page_has_a_strict_production_plan(
    bounded_recovery_database: tuple[PostgresBroker, AsyncEngine],
) -> None:
    _broker, engine = bounded_recovery_database
    await _seed_workflow_page_fixture(
        engine,
        rows=50_000,
        terminal_last=False,
    )
    statement = text(
        'EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) '
        + GLOBAL_WORKFLOW_AUDIT_SQL.text
    )
    async with engine.begin() as connection:
        await connection.execute(text(
            'ANALYZE horsies_workflows, horsies_workflow_tasks'
        ))
        result = await connection.execute(
            statement,
            {
                'max_rows': GLOBAL_SCAN_ROW_CAP,
                'claim_token': str(uuid.uuid4()),
                'claim_ttl_ms': GLOBAL_WORKFLOW_AUDIT_CLAIM_TTL_MS,
                'wf_task_terminal_states': ['COMPLETED', 'FAILED', 'SKIPPED'],
            },
        )
        plan = cast(object, result.scalar_one()[0]['Plan'])

    assert _relation_rows_examined(plan, 'horsies_workflows') <= 201
    assert _relation_rows_examined(plan, 'horsies_workflow_tasks') <= 400


async def test_cycle_upper_bound_defers_a_new_workflow(
    bounded_recovery_database: tuple[PostgresBroker, AsyncEngine],
) -> None:
    broker, engine = bounded_recovery_database
    await _seed_workflow_page_fixture(
        engine,
        rows=GLOBAL_SCAN_ROW_CAP + 1,
        terminal_last=False,
    )
    first = await recover_stuck_workflows_global(
        broker.session_factory,
        broker,
    )
    assert first.metrics['case_2_3'].rows_selected == 200

    new_id = str(uuid.uuid4())
    async with engine.begin() as connection:
        await connection.execute(
            text('''
                INSERT INTO horsies_workflows (
                    id, name, status, on_error, depth, root_workflow_id,
                    created_at, updated_at
                ) VALUES (
                    CAST(:id AS uuid), 'new_orphan', 'RUNNING', 'fail', 0,
                    CAST(:id AS uuid), TIMESTAMPTZ '2026-03-01 00:00:00+00',
                    NOW()
                )
            '''),
            {'id': new_id},
        )

    second = await recover_stuck_workflows_global(
        broker.session_factory,
        broker,
    )
    assert second.metrics['case_4'].rows_selected == 1
    assert second.metrics['case_4'].candidates_returned == 0
    async with engine.connect() as connection:
        status = (await connection.execute(
            text('SELECT status FROM horsies_workflows WHERE id = :id'),
            {'id': new_id},
        )).scalar_one()
    assert status == 'RUNNING'

    third = await recover_stuck_workflows_global(
        broker.session_factory,
        broker,
    )
    assert third.metrics['case_4'].candidates_returned == 0
    fourth = await recover_stuck_workflows_global(
        broker.session_factory,
        broker,
    )
    assert fourth.metrics['case_4'].candidates_returned == 1
    async with engine.connect() as connection:
        status = (await connection.execute(
            text('SELECT status FROM horsies_workflows WHERE id = :id'),
            {'id': new_id},
        )).scalar_one()
    assert status == 'FAILED'


async def test_candidate_share_lock_fences_an_expired_page_claim(
    bounded_recovery_database: tuple[PostgresBroker, AsyncEngine],
) -> None:
    broker, engine = bounded_recovery_database
    workflow_id = str(uuid.uuid4())
    async with engine.begin() as connection:
        await connection.execute(
            text('''
                INSERT INTO horsies_workflows (
                    id, name, status, on_error, depth, root_workflow_id,
                    created_at, updated_at
                ) VALUES (
                    CAST(:id AS uuid), 'fenced_orphan', 'RUNNING', 'fail', 0,
                    CAST(:id AS uuid), NOW(), NOW()
                )
            '''),
            {'id': workflow_id},
        )

    claim_token = str(uuid.uuid4())
    async with broker.session_factory() as discovery:
        audit = (await discovery.execute(
            GLOBAL_WORKFLOW_AUDIT_SQL,
            {
                'max_rows': GLOBAL_SCAN_ROW_CAP,
                'claim_token': claim_token,
                'claim_ttl_ms': 2_000,
                'wf_task_terminal_states': [
                    'COMPLETED', 'FAILED', 'SKIPPED'
                ],
            },
        )).one()
        await discovery.commit()
    assert [str(value) for value in audit.orphan_ids] == [workflow_id]

    async with broker.session_factory() as competitor:
        refused = (await competitor.execute(
            GLOBAL_WORKFLOW_AUDIT_SQL,
            {
                'max_rows': GLOBAL_SCAN_ROW_CAP,
                'claim_token': str(uuid.uuid4()),
                'claim_ttl_ms': 100,
                'wf_task_terminal_states': [
                    'COMPLETED', 'FAILED', 'SKIPPED'
                ],
            },
        )).one_or_none()
        await competitor.commit()
    assert refused is None

    async with broker.session_factory() as mutation:
        held = (await mutation.execute(
            LOCK_GLOBAL_WORKFLOW_AUDIT_CLAIM_SQL,
            {'claim_token': claim_token},
        )).scalar_one_or_none()
        assert held is True
        await asyncio.sleep(2.1)

        async with broker.session_factory() as takeover:
            blocked = (await takeover.execute(
                GLOBAL_WORKFLOW_AUDIT_SQL,
                {
                    'max_rows': GLOBAL_SCAN_ROW_CAP,
                    'claim_token': str(uuid.uuid4()),
                    'claim_ttl_ms': 2_000,
                    'wf_task_terminal_states': [
                        'COMPLETED', 'FAILED', 'SKIPPED'
                    ],
                },
            )).one_or_none()
            await takeover.commit()
        assert blocked is None

        result = await mutation.execute(
            FAIL_ORPHANED_WORKFLOW_SQL,
            {
                'workflow_id': workflow_id,
                'error': '{"error_code":"E400"}',
            },
        )
        assert int(getattr(result, 'rowcount', 0)) == 1
        await mutation.commit()


async def test_stale_page_owner_cannot_mutate_after_claim_takeover(
    bounded_recovery_database: tuple[PostgresBroker, AsyncEngine],
) -> None:
    broker, engine = bounded_recovery_database
    workflow_id = str(uuid.uuid4())
    async with engine.begin() as connection:
        await connection.execute(
            text('''
                INSERT INTO horsies_workflows (
                    id, name, status, on_error, depth, root_workflow_id,
                    created_at, updated_at
                ) VALUES (
                    CAST(:id AS uuid), 'stale_owner_orphan', 'RUNNING',
                    'fail', 0, CAST(:id AS uuid), NOW(), NOW()
                )
            '''),
            {'id': workflow_id},
        )

    stale_token = str(uuid.uuid4())
    async with broker.session_factory() as discovery:
        first = (await discovery.execute(
            GLOBAL_WORKFLOW_AUDIT_SQL,
            {
                'max_rows': GLOBAL_SCAN_ROW_CAP,
                'claim_token': stale_token,
                'claim_ttl_ms': 60_000,
                'wf_task_terminal_states': [
                    'COMPLETED', 'FAILED', 'SKIPPED'
                ],
            },
        )).one()
        await discovery.commit()
    assert [str(value) for value in first.orphan_ids] == [workflow_id]

    replacement_token = str(uuid.uuid4())
    async with broker.session_factory() as takeover:
        await takeover.execute(text('''
            UPDATE horsies_recovery_scan_cursors
            SET claim_expires_at = statement_timestamp() - interval '1 second'
            WHERE scan_name = 'running_workflows'
              AND claim_token = CAST(:claim_token AS uuid)
        '''), {'claim_token': stale_token})
        await takeover.commit()
    async with broker.session_factory() as takeover:
        second = (await takeover.execute(
            GLOBAL_WORKFLOW_AUDIT_SQL,
            {
                'max_rows': GLOBAL_SCAN_ROW_CAP,
                'claim_token': replacement_token,
                'claim_ttl_ms': 60_000,
                'wf_task_terminal_states': [
                    'COMPLETED', 'FAILED', 'SKIPPED'
                ],
            },
        )).one()
        await takeover.commit()
    assert [str(value) for value in second.orphan_ids] == [workflow_id]

    outcome = await _recover_global_orphan_candidate(
        broker.session_factory,
        workflow_id,
        'stale_owner_orphan',
        stale_token,
    )
    assert outcome is _GlobalCandidateOutcome.CLAIM_LOST
    async with engine.connect() as connection:
        status = (await connection.execute(
            text('SELECT status FROM horsies_workflows WHERE id = :id'),
            {'id': workflow_id},
        )).scalar_one()
    assert status == 'RUNNING'


async def _seed_orphan_task_plan_fixture(
    engine: AsyncEngine,
    *,
    rows: int,
) -> None:
    async with engine.begin() as connection:
        await connection.execute(
            text('''
                INSERT INTO horsies_tasks (
                    id, task_name, queue_name, priority, status, sent_at,
                    enqueued_at, claimed, is_workflow_task, retry_count,
                    max_retries, enqueue_sha, command_fingerprint_version,
                    command_fingerprint, retention_class_key, input_digest,
                    retain_rerun_input, prepared_rerun_input_disposition,
                    created_at, updated_at
                )
                SELECT md5('orphan-plan-task-' || item::text)::uuid,
                       'orphan_plan_task', 'default', 100, 'PENDING',
                       TIMESTAMPTZ '2026-02-01 00:00:00+00'
                           + item * interval '1 millisecond',
                       TIMESTAMPTZ '2026-02-01 00:00:00+00'
                           + item * interval '1 millisecond',
                       FALSE, TRUE, 0, 0, repeat('0', 64), 1,
                       decode(repeat('01', 32), 'hex'), 'standard_30d',
                       decode(repeat('02', 32), 'hex'), FALSE,
                       'DECLINED_BY_POLICY',
                       TIMESTAMPTZ '2026-02-01 00:00:00+00'
                           + item * interval '1 millisecond',
                       NOW()
                FROM generate_series(1, :rows) item
            '''),
            {'rows': rows},
        )
        await connection.execute(text('''
            INSERT INTO horsies_workflows (
                id, name, status, on_error, depth, root_workflow_id,
                created_at, updated_at
            ) VALUES (
                md5('orphan-plan-workflow')::uuid, 'orphan_plan',
                'RUNNING', 'fail', 0, md5('orphan-plan-workflow')::uuid,
                TIMESTAMPTZ '2026-02-01 00:00:00+00', NOW()
            )
        '''))
        await connection.execute(
            text('''
                INSERT INTO horsies_workflow_tasks (
                    id, workflow_id, task_index, task_name, queue_name,
                    priority, dependencies, allow_failed_deps, join_type,
                    status, task_id, is_subworkflow, created_at
                )
                SELECT md5('orphan-plan-node-' || item::text)::uuid,
                       md5('orphan-plan-workflow')::uuid,
                       item, 'orphan_plan_task', 'default', 100,
                       '{}'::integer[], FALSE, 'all', 'RUNNING',
                       md5('orphan-plan-task-' || item::text)::uuid,
                       FALSE,
                       TIMESTAMPTZ '2026-02-01 00:00:00+00'
                           + item * interval '1 millisecond'
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
                    id, task_name, queue_name, priority, status, sent_at,
                    enqueued_at, claimed, is_workflow_task, retry_count,
                    max_retries, enqueue_sha, command_fingerprint_version,
                    command_fingerprint, retention_class_key, input_digest,
                    retain_rerun_input, prepared_rerun_input_disposition,
                    created_at, updated_at
                ) VALUES (
                    md5('new-orphan-' || CAST(:item AS text))::uuid,
                    'new_orphan', 'default', 100, 'PENDING',
                    TIMESTAMPTZ '2026-03-01 00:00:00+00'
                        + :item * interval '1 second',
                    TIMESTAMPTZ '2026-03-01 00:00:00+00'
                        + :item * interval '1 second',
                    FALSE, TRUE, 0, 0, repeat('0', 64), 1,
                    decode(repeat('03', 32), 'hex'), 'standard_30d',
                    decode(repeat('04', 32), 'hex'), FALSE,
                    'DECLINED_BY_POLICY',
                    TIMESTAMPTZ '2026-03-01 00:00:00+00'
                        + :item * interval '1 second',
                    NOW()
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
        original = getattr(error.exception, 'orig', error.exception)
        assert getattr(original, 'sqlstate', None) == '55P03'
        await transaction.rollback()


async def test_orphan_task_scan_has_a_strict_plan_and_reaches_cycle_end(
    bounded_recovery_database: tuple[PostgresBroker, AsyncEngine],
) -> None:
    _broker, engine = bounded_recovery_database
    rows = 50_001
    await _seed_orphan_task_plan_fixture(engine, rows=rows)

    async with engine.begin() as connection:
        await connection.execute(text('''
            UPDATE horsies_recovery_scan_cursors cursor
            SET last_created_at = task.created_at,
                last_id = task.id,
                cycle_upper_created_at = upper_task.created_at,
                cycle_upper_id = upper_task.id,
                completed_cycles = 0
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

    assert _maximum_shared_buffers(plan) <= 3_500

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


async def test_recovery_index_upgrade_rebuilds_a_malformed_canonical_index(
    bounded_recovery_database: tuple[PostgresBroker, AsyncEngine],
) -> None:
    _broker, engine = bounded_recovery_database
    async with engine.connect() as connection:
        connection = await connection.execution_options(
            isolation_level='AUTOCOMMIT'
        )
        await connection.execute(text('''
            DROP INDEX CONCURRENTLY
                idx_horsies_workflows_running_recovery_scan
        '''))
        await connection.execute(text('''
            CREATE INDEX idx_horsies_workflows_running_recovery_scan
            ON horsies_workflows (id)
        '''))
    async with engine.begin() as connection:
        await connection.execute(text(
            'DELETE FROM horsies_schema_version WHERE version = 36'
        ))

    url = engine.url.render_as_string(hide_password=False)
    upgrade = PostgresBroker(
        PostgresConfig(database_url=SecretStr(url))
    )
    try:
        initialized = await upgrade.ensure_schema_initialized()
        assert initialized.is_ok(), initialized
    finally:
        await upgrade.close_async()

    async with engine.connect() as connection:
        definition = str((await connection.execute(text('''
            SELECT pg_get_indexdef(
                'idx_horsies_workflows_running_recovery_scan'::regclass
            )
        '''))).scalar_one())
    assert '(created_at, id) INCLUDE (name)' in definition
    assert "WHERE ((status)::text = 'RUNNING'::text)" in definition


async def test_recovery_index_claim_refuses_foreign_name_reuse(
    bounded_recovery_database: tuple[PostgresBroker, AsyncEngine],
) -> None:
    _broker, engine = bounded_recovery_database
    entered = asyncio.Event()
    release = asyncio.Event()

    async def pause(index: recovery_schema.RecoveryIndex) -> None:
        if index.name == 'idx_horsies_workflows_running_recovery_scan':
            entered.set()
            await release.wait()

    recovery_schema._index_inspection_pause = pause  # pyright: ignore[reportPrivateUsage]
    async with engine.begin() as connection:
        await connection.execute(text(
            'DELETE FROM horsies_schema_version WHERE version = 36'
        ))

    url = engine.url.render_as_string(hide_password=False)
    upgrade = PostgresBroker(
        PostgresConfig(database_url=SecretStr(url))
    )
    task = asyncio.create_task(upgrade.ensure_schema_initialized())
    try:
        await asyncio.wait_for(entered.wait(), timeout=5)
        async with engine.begin() as connection:
            await connection.execute(text('''
                ALTER INDEX idx_horsies_workflows_running_recovery_scan
                RENAME TO saved_running_recovery_scan
            '''))
            await connection.execute(text(
                'CREATE TABLE foreign_recovery_index_owner (id uuid)'
            ))
            await connection.execute(text('''
                CREATE INDEX idx_horsies_workflows_running_recovery_scan
                ON foreign_recovery_index_owner (id)
            '''))
        release.set()
        initialized = await asyncio.wait_for(task, timeout=10)
        assert initialized.is_err()
    finally:
        release.set()
        recovery_schema._index_inspection_pause = None  # pyright: ignore[reportPrivateUsage]
        if not task.done():
            task.cancel()
        await upgrade.close_async()

    async with engine.connect() as connection:
        owner = str((await connection.execute(text('''
            SELECT index_state.indrelid::regclass::text
            FROM pg_index index_state
            WHERE index_state.indexrelid =
                'idx_horsies_workflows_running_recovery_scan'::regclass
        '''))).scalar_one())
        saved_owner = str((await connection.execute(text('''
            SELECT index_state.indrelid::regclass::text
            FROM pg_index index_state
            WHERE index_state.indexrelid =
                'saved_running_recovery_scan'::regclass
        '''))).scalar_one())
    assert owner == 'foreign_recovery_index_owner'
    assert saved_owner == 'horsies_workflows'
