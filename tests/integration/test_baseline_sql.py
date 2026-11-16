"""The baseline migration text on a fresh PostgreSQL database.

Each test builds its own database (``isolated_test_database``), so no test
here touches the shared test database.
"""

from __future__ import annotations

from collections.abc import AsyncGenerator
from pathlib import Path
from typing import Final, LiteralString

import psycopg
import pytest
import pytest_asyncio

from horsies.core.baseline.migration import BaselineMigration, load_baseline_migration
from horsies.core.types.result import Err, Ok
from horsies.core.utils.url import to_psycopg_url
from tests.integration.conftest import DB_URL
from tests.integration.isolated_database import isolated_test_database

pytestmark = [pytest.mark.integration]

_FUNCTION_CALLS_SQL = Path(__file__).with_name('baseline_function_calls.sql')

EXPECTED_TABLES = frozenset(
    {
        'horsies_client_family',
        'horsies_heartbeats',
        'horsies_key_reservations',
        'horsies_migrations',
        'horsies_recovery_scan_cursors',
        'horsies_retention_classes',
        'horsies_schedule_state',
        'horsies_schema_version',
        'horsies_task_attempts',
        'horsies_task_history',
        'horsies_task_history_clock_exceptions',
        'horsies_task_history_forever',
        'horsies_task_history_forever_open',
        'horsies_task_history_leaf_catalog',
        'horsies_task_lookup_manifest',
        'horsies_tasks',
        'horsies_worker_states',
        'horsies_workflow_phase2_pending',
        'horsies_workflow_phase2_quarantine',
        'horsies_workflow_recovery_failures',
        'horsies_workflow_tasks',
        'horsies_workflows',
    }
)

EXPECTED_FUNCTIONS = frozenset(
    {
        # 0001_tables.sql: the UUIDv7 helper and the trigger functions.
        'horsies_uuidv7',
        'horsies_notify_task_changes',
        'horsies_notify_task_status_change',
        'horsies_notify_worker_state_change',
        'horsies_notify_workflow_status_change',
        # 0003_interim_functions.sql.
        'horsies_abandon_nodes_of_paused_workflows',
        'horsies_abandon_owned_node',
        'horsies_abandon_owned_nodes',
        'horsies_cancel_locked_task',
        'horsies_cancel_nodes_of_cancelled_workflow',
        'horsies_cancel_orphaned_tasks',
        'horsies_cancel_owned_node',
        'horsies_cancel_owned_nodes',
        'horsies_cancel_owned_orphan',
        'horsies_claim',
        'horsies_claim_candidates',
        'horsies_claim_in_flight',
        'horsies_claim_plan',
        'horsies_complete_locked_task',
        'horsies_complete_task_fused',
        'horsies_complete_tasks_fused',
        'horsies_encode_rerun_input',
        'horsies_encode_task_attempts',
        'horsies_expire_owned_claim',
        'horsies_expire_pending_tasks',
        'horsies_fail_locked_task',
        'horsies_fail_stale_task',
        'horsies_find_non_runnable_workflow_tasks',
        'horsies_finish_failed_attempt',
        'horsies_finish_workflow_success',
        'horsies_key_reservation_claim',
        'horsies_key_reservation_cleanup',
        'horsies_key_reservation_terminalize',
        'horsies_key_reservation_terminalize_batch',
        'horsies_move_task_to_history',
        'horsies_phase2_consume',
        'horsies_phase2_quarantine_one',
        'horsies_task_history_duplicate_staged',
        'horsies_task_history_leaf_lock_key',
        'horsies_terminalization_miss',
        'horsies_uuid7_birth',
    }
)

_TABLES_SQL: Final = """
    SELECT c.relname
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = current_schema()
      AND c.relkind IN ('r', 'p')
      AND c.relname LIKE 'horsies\\_%'
"""

_FUNCTIONS_SQL: Final = """
    SELECT p.proname
    FROM pg_proc p
    JOIN pg_namespace n ON n.oid = p.pronamespace
    WHERE n.nspname = current_schema()
      AND p.proname LIKE 'horsies\\_%'
"""


def _migration() -> BaselineMigration:
    match load_baseline_migration():
        case Ok(loaded):
            return loaded
        case Err(unreadable):
            pytest.fail(f'baseline SQL unreadable: {unreadable}')


@pytest_asyncio.fixture
async def installed() -> AsyncGenerator[psycopg.AsyncConnection[tuple[object, ...]]]:
    """A connection to a fresh database that holds the baseline migration."""
    migration = _migration()
    async with isolated_test_database(DB_URL) as url:
        connection = await psycopg.AsyncConnection.connect(to_psycopg_url(url))
        try:
            # No parameters: psycopg sends the text with the simple query
            # protocol, which runs every statement of it.
            async with connection.transaction():
                await connection.execute(migration.sql.encode('utf-8'))
            yield connection
        finally:
            await connection.close()


async def _names(
    connection: psycopg.AsyncConnection[tuple[object, ...]],
    query: LiteralString,
) -> frozenset[str]:
    cursor = await connection.execute(query)
    return frozenset(str(row[0]) for row in await cursor.fetchall())


@pytest.mark.asyncio(loop_scope='function')
async def test_migration_creates_the_baseline_tables(
    installed: psycopg.AsyncConnection[tuple[object, ...]],
) -> None:
    assert await _names(installed, _TABLES_SQL) == EXPECTED_TABLES


@pytest.mark.asyncio(loop_scope='function')
async def test_migration_creates_the_baseline_functions(
    installed: psycopg.AsyncConnection[tuple[object, ...]],
) -> None:
    assert await _names(installed, _FUNCTIONS_SQL) == EXPECTED_FUNCTIONS


@pytest.mark.asyncio(loop_scope='function')
async def test_migration_writes_the_seed_rows(
    installed: psycopg.AsyncConnection[tuple[object, ...]],
) -> None:
    cursor = await installed.execute(
        """
        SELECT
            (SELECT array_agg(version) FROM horsies_schema_version),
            (SELECT array_agg(class_key || ':' || class_kind) FROM horsies_retention_classes),
            (SELECT array_agg(leaf_name) FROM horsies_task_history_leaf_catalog),
            (SELECT array_agg(scan_name ORDER BY scan_name) FROM horsies_recovery_scan_cursors),
            (SELECT count(*) FROM horsies_migrations),
            (SELECT count(*) FROM horsies_client_family)
        """
    )
    row = await cursor.fetchone()
    assert row == (
        [1000],
        ['forever:FOREVER'],
        ['horsies_task_history_forever_open'],
        ['orphan_workflow_tasks', 'running_workflows'],
        0,
        0,
    )


@pytest.mark.asyncio(loop_scope='function')
async def test_every_interim_function_runs_on_the_baseline_tables(
    installed: psycopg.AsyncConnection[tuple[object, ...]],
) -> None:
    calls = _FUNCTION_CALLS_SQL.read_text(encoding='utf-8')
    async with installed.transaction(force_rollback=True):
        await installed.execute(calls.encode('utf-8'))
