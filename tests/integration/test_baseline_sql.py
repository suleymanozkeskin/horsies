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
        'horsies_cancel_owned_node',
        'horsies_cancel_owned_nodes',
        'horsies_claim_plan',
        'horsies_complete_locked_task',
        'horsies_encode_rerun_input',
        'horsies_encode_task_attempts',
        'horsies_fail_locked_task',
        'horsies_fail_stale_task',
        'horsies_find_non_runnable_workflow_tasks',
        'horsies_key_reservation_claim',
        'horsies_key_reservation_cleanup',
        'horsies_key_reservation_terminalize',
        'horsies_key_reservation_terminalize_batch',
        'horsies_move_task_to_history',
        'horsies_phase2_consume',
        'horsies_phase2_quarantine_one',
        'horsies_task_history_leaf_lock_key',
        'horsies_terminalization_miss',
        'horsies_uuid7_birth',
        # 0004_lifecycle_functions.sql.
        'horsies_cancel_orphaned_tasks',
        'horsies_cancel_owned_orphan',
        'horsies_cancel_task',
        'horsies_claim',
        'horsies_claim_candidates',
        'horsies_claim_in_flight',
        'horsies_complete_task_fused',
        'horsies_complete_tasks_fused',
        'horsies_enqueue_task',
        'horsies_expire_owned_claim',
        'horsies_expire_pending_tasks',
        'horsies_finish_failed_attempt',
        'horsies_finish_stale_attempt',
        'horsies_finish_workflow_success',
        'horsies_insert_task_rows',
        'horsies_mark_finalizing',
        'horsies_node_has_dependents',
        'horsies_node_downstream_subworkflow',
        'horsies_release_claims',
        'horsies_requeue_stale_claims',
        'horsies_stale_running_scan',
        'horsies_start_attempt',
        # 0005_workflow_functions.sql.
        'horsies_task_error_envelope',
        'horsies_task_error_object',
        'horsies_workflow_apply_recovery_limit',
        'horsies_workflow_cancel',
        'horsies_workflow_child_finish',
        'horsies_workflow_child_node_result',
        'horsies_workflow_child_start_rows',
        'horsies_workflow_err_slot',
        'horsies_workflow_explicit_output',
        'horsies_workflow_fail_unbuildable',
        'horsies_workflow_failure_error',
        'horsies_workflow_json_or_null',
        'horsies_workflow_move_descendants',
        'horsies_workflow_node_apply',
        'horsies_workflow_node_finish',
        'horsies_workflow_node_tasks_insert',
        'horsies_workflow_outcome_row',
        'horsies_workflow_output_error_data',
        'horsies_workflow_outputless_result',
        'horsies_workflow_pause',
        'horsies_workflow_phase2_record_raise',
        'horsies_workflow_phase2_recover',
        'horsies_workflow_phase2_source',
        'horsies_workflow_policy_indices_cause',
        'horsies_workflow_progress',
        'horsies_workflow_promote',
        'horsies_workflow_promote_and_complete',
        'horsies_workflow_promote_level',
        'horsies_workflow_record_failure',
        'horsies_workflow_require',
        'horsies_workflow_result_kind',
        'horsies_workflow_resume',
        'horsies_workflow_source_value',
        'horsies_workflow_start',
        'horsies_workflow_start_advance',
        'horsies_workflow_start_check',
        'horsies_workflow_start_child_row',
        'horsies_workflow_start_failed_node',
        'horsies_workflow_start_nodes',
        'horsies_workflow_start_top_row',
        'horsies_workflow_start_tree',
        'horsies_workflow_success_cases',
        'horsies_workflow_task_input',
        'horsies_workflow_try_complete',
        'horsies_workflow_unbuildable_nodes',
        'horsies_workflow_tree_check',
        'horsies_workflow_child_start',
        'horsies_workflow_claimed_tasks_settle',
        'horsies_workflow_task_stop_before_start',
        'horsies_workflow_expire_paused',
        'horsies_workflow_node_reevaluate',
        'horsies_workflow_nodes_enqueue_ready',
        'horsies_workflow_recovery_audit_page',
        'horsies_workflow_audit_claim_renew',
        'horsies_workflow_recover_completion',
        'horsies_workflow_fail_orphaned',
        'horsies_workflow_recovery_record_raise',
        'horsies_workflow_json_type_name',
        'horsies_workflow_json_int',
        'horsies_workflow_json_breaks_strict_rules',
        'horsies_workflow_options_json',
        'horsies_workflow_retry_interval_entries',
        'horsies_workflow_retry_codes_plain',
        'horsies_workflow_retry_code_entries',
        'horsies_workflow_retry_policy_entries',
        'horsies_workflow_retry_policy_value_error',
        'horsies_workflow_retry_policy_refusal',
        'horsies_workflow_execution_refusal',
        'horsies_workflow_task_options_refusal',
        # 0006_history_functions.sql.
        'horsies_begin_leaf_detach',
        'horsies_drop_detached_leaf',
        'horsies_inspect_history_leaf',
        'horsies_move_tasks_to_history',
        'horsies_prune_candidates',
        'horsies_record_leaf_detached',
        'horsies_rerun_source_read',
        'horsies_task_absence_reason',
        # 0007_scheduler_monitoring_functions.sql.
        'horsies_fire_schedule',
        'horsies_list_expired_tasks',
        'horsies_list_stale_tasks',
        'horsies_monitoring_check_scope',
        'horsies_monitoring_schedules',
        'horsies_monitoring_task_breakdown',
        'horsies_monitoring_task_detail',
        'horsies_monitoring_task_facets',
        'horsies_monitoring_task_list',
        'horsies_monitoring_task_stats',
        'horsies_monitoring_workflow_names',
        'horsies_monitoring_workflow_node',
        'horsies_monitoring_workflow_run',
        'horsies_monitoring_workflow_runs',
        'horsies_schedule_slot_task_id',
        # 0008_history_maintenance_functions.sql. horsies_terminalization_miss
        # gains an overload there; its name is listed above.
        'horsies_coverage_probe',
        'horsies_create_heartbeat_leaf',
        'horsies_create_history_leaf',
        'horsies_create_partition_leaf',
        'horsies_heartbeat_leaf_name',
        'horsies_history_absent_leaves',
        'horsies_history_leaf_indexes',
        'horsies_history_leaf_name',
        'horsies_history_leaf_state',
        'horsies_history_reader_manifest',
        'horsies_history_reader_signatures',
        'horsies_history_reader_stamp',
        'horsies_history_readers_current',
        'horsies_publish_history_readers',
        'horsies_register_retention_classes',
        'horsies_render_history_reader',
        'horsies_retention_period_days',
        'horsies_terminalization_family',
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
    return load_baseline_migration()


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


_MOVE_SIGNATURES_SQL: Final = """
    SELECT p.oid::regprocedure::text
    FROM pg_proc p
    JOIN pg_namespace n ON n.oid = p.pronamespace
    WHERE n.nspname = current_schema()
      AND p.proname = 'horsies_move_task_to_history'
"""


@pytest.mark.asyncio(loop_scope='function')
async def test_the_single_move_has_one_signature_that_takes_the_locked_row(
    installed: psycopg.AsyncConnection[tuple[object, ...]],
) -> None:
    assert await _names(installed, _MOVE_SIGNATURES_SQL) == frozenset(
        {
            'horsies_move_task_to_history(horsies_tasks,text,text,'
            'timestamp with time zone,text,text,text)',
        }
    )


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


_LEAF_ROW_SQL: Final = """
    INSERT INTO horsies_task_history_leaf_catalog (
        leaf_name, class_key, lower_bound_kind, lower_anchor, upper_anchor, index_schema_version,
        created_at, detached_at, dropped_at
    ) VALUES (%s, 'forever', %s, %s, %s, 1, now(), NULL, NULL)
"""

_MANIFEST_ROW_SQL: Final = """
    INSERT INTO horsies_task_lookup_manifest (
        leaf_name, probe_position, lower_bound_kind, lower_anchor, upper_anchor, published_at
    ) VALUES (%s, %s, %s, %s, %s, now())
"""

_UPPER: Final = '2030-01-01 00:00:00+00'
_LOWER: Final = '2029-12-31 00:00:00+00'


@pytest.mark.parametrize(
    ('kind', 'lower', 'violated'),
    [
        ('OPEN', None, None),
        ('AT', _LOWER, None),
        ('OPEN', _LOWER, 'horsies_task_history_leaf_catalog_lower_bound_pair'),
        ('AT', None, 'horsies_task_history_leaf_catalog_lower_bound_pair'),
        ('ON', None, 'horsies_task_history_leaf_catalog_lower_bound_pair'),
        ('AT', _UPPER, 'horsies_task_history_leaf_catalog_bounds_ordered'),
    ],
)
@pytest.mark.asyncio(loop_scope='function')
async def test_leaf_catalog_lower_bound_is_typed(
    installed: psycopg.AsyncConnection[tuple[object, ...]],
    kind: str,
    lower: str | None,
    violated: str | None,
) -> None:
    async with installed.transaction(force_rollback=True):
        match violated:
            case None:
                await installed.execute(_LEAF_ROW_SQL, ('leaf_case', kind, lower, _UPPER))
            case constraint:
                with pytest.raises(psycopg.errors.CheckViolation) as raised:
                    await installed.execute(_LEAF_ROW_SQL, ('leaf_case', kind, lower, _UPPER))
                assert raised.value.diag.constraint_name == constraint


@pytest.mark.asyncio(loop_scope='function')
async def test_leaf_catalog_holds_one_open_leaf_per_class_and_upper_anchor(
    installed: psycopg.AsyncConnection[tuple[object, ...]],
) -> None:
    async with installed.transaction(force_rollback=True):
        await installed.execute(_LEAF_ROW_SQL, ('leaf_first', 'OPEN', None, _UPPER))
        with pytest.raises(psycopg.errors.UniqueViolation) as raised:
            await installed.execute(_LEAF_ROW_SQL, ('leaf_second', 'OPEN', None, _UPPER))
        assert raised.value.diag.constraint_name == 'horsies_task_history_leaf_catalog_open_interval_unique'


@pytest.mark.parametrize(
    ('kind', 'lower', 'violated'),
    [
        ('OPEN', None, None),
        ('AT', _LOWER, None),
        ('OPEN', _LOWER, 'horsies_task_lookup_manifest_lower_bound_pair'),
        ('AT', None, 'horsies_task_lookup_manifest_lower_bound_pair'),
        ('ON', None, 'horsies_task_lookup_manifest_lower_bound_pair'),
        ('AT', _UPPER, 'horsies_task_lookup_manifest_bounds_ordered'),
    ],
)
@pytest.mark.asyncio(loop_scope='function')
async def test_manifest_lower_bound_is_typed(
    installed: psycopg.AsyncConnection[tuple[object, ...]],
    kind: str,
    lower: str | None,
    violated: str | None,
) -> None:
    async with installed.transaction(force_rollback=True):
        match violated:
            case None:
                await installed.execute(_MANIFEST_ROW_SQL, ('leaf_case', 0, kind, lower, _UPPER))
            case constraint:
                with pytest.raises(psycopg.errors.CheckViolation) as raised:
                    await installed.execute(_MANIFEST_ROW_SQL, ('leaf_case', 0, kind, lower, _UPPER))
                assert raised.value.diag.constraint_name == constraint


_WORKFLOW_ROW_SQL: Final = """
    INSERT INTO horsies_workflows (
        id, name, status, on_error, depth, root_workflow_id, sent_at, created_at, updated_at
    ) VALUES (
        '0193e5c4-0000-7000-8000-000000000001', 'params_case', 'RUNNING', 'fail', 0,
        '0193e5c4-0000-7000-8000-000000000001', now(), now(), now()
    )
"""

_NODE_ROW_SQL: Final = """
    INSERT INTO horsies_workflow_tasks (downstream_subworkflow,
        id, workflow_id, task_index, node_id, task_name, input, queue_name, priority, dependencies,
        allow_failed_deps, join_type, max_retries, execution, retention_class_key, status,
        is_subworkflow, sub_workflow_params, created_at
    ) VALUES ('NONE',
        '0193e5c4-0000-7000-8000-000000000002', '0193e5c4-0000-7000-8000-000000000001', 0, 'node_0',
        'params.case', '{}', 'default', 100, '{}', FALSE, 'all', %(max_retries)s, %(execution)s,
        %(retention_class_key)s, 'PENDING', %(is_subworkflow)s, %(sub_workflow_params)s, now()
    )
"""


@pytest.mark.parametrize(
    ('is_subworkflow', 'sub_workflow_params', 'violated'),
    [
        (False, None, None),
        (True, 'NOT_GIVEN', None),
        (True, 'GIVEN', None),
        (True, None, 'horsies_workflow_tasks_sub_workflow_params_by_kind'),
        (True, 'SOME', 'horsies_workflow_tasks_sub_workflow_params_by_kind'),
        (False, 'NOT_GIVEN', 'horsies_workflow_tasks_sub_workflow_params_by_kind'),
        (False, 'GIVEN', 'horsies_workflow_tasks_sub_workflow_params_by_kind'),
    ],
)
@pytest.mark.asyncio(loop_scope='function')
async def test_sub_workflow_params_are_set_exactly_on_sub_workflow_nodes(
    installed: psycopg.AsyncConnection[tuple[object, ...]],
    is_subworkflow: bool,
    sub_workflow_params: str | None,
    violated: str | None,
) -> None:
    # The task facts follow the node kind, so only the params CHECK decides.
    params: dict[str, object] = {
        'max_retries': None if is_subworkflow else 0,
        'execution': None if is_subworkflow else 'ISOLATED',
        'retention_class_key': None if is_subworkflow else 'forever',
        'is_subworkflow': is_subworkflow,
        'sub_workflow_params': sub_workflow_params,
    }
    async with installed.transaction(force_rollback=True):
        await installed.execute(_WORKFLOW_ROW_SQL)
        match violated:
            case None:
                await installed.execute(_NODE_ROW_SQL, params)
            case constraint:
                with pytest.raises(psycopg.errors.CheckViolation) as raised:
                    await installed.execute(_NODE_ROW_SQL, params)
                assert raised.value.diag.constraint_name == constraint


_DOWNSTREAM_NODE_SQL: Final = """
    INSERT INTO horsies_workflow_tasks (
        id, workflow_id, task_index, node_id, task_name, input, queue_name, priority, dependencies,
        allow_failed_deps, join_type, max_retries, execution, retention_class_key, downstream_subworkflow,
        status, is_subworkflow, created_at
    ) VALUES (
        '0193e5c4-0000-7000-8000-000000000002', '0193e5c4-0000-7000-8000-000000000001', 0, 'node_0',
        'downstream.case', '{}', 'default', 100, '{}', FALSE, 'all', 0, 'ISOLATED', 'forever',
        %(downstream_subworkflow)s, 'PENDING', FALSE, now()
    )
"""


@pytest.mark.parametrize(
    ('downstream_subworkflow', 'violation'),
    [
        ('NONE', None),
        ('REACHABLE', None),
        ('SOME', psycopg.errors.CheckViolation),
        (None, psycopg.errors.NotNullViolation),
    ],
)
@pytest.mark.asyncio(loop_scope='function')
async def test_downstream_subworkflow_is_one_of_two_named_values(
    installed: psycopg.AsyncConnection[tuple[object, ...]],
    downstream_subworkflow: str | None,
    violation: type[psycopg.errors.IntegrityError] | None,
) -> None:
    async with installed.transaction(force_rollback=True):
        await installed.execute(_WORKFLOW_ROW_SQL)
        match violation:
            case None:
                await installed.execute(
                    _DOWNSTREAM_NODE_SQL, {'downstream_subworkflow': downstream_subworkflow}
                )
            case error:
                with pytest.raises(error):
                    await installed.execute(
                        _DOWNSTREAM_NODE_SQL, {'downstream_subworkflow': downstream_subworkflow}
                    )


@pytest.mark.asyncio(loop_scope='function')
async def test_forever_leaf_starts_at_minvalue_with_an_open_catalog_bound(
    installed: psycopg.AsyncConnection[tuple[object, ...]],
) -> None:
    await installed.execute("SET TimeZone = 'UTC'")
    cursor = await installed.execute(
        """
        SELECT pg_get_expr(c.relpartbound, c.oid), l.lower_bound_kind, l.lower_anchor IS NULL
        FROM pg_class c
        JOIN horsies_task_history_leaf_catalog l ON l.leaf_name = c.relname
        WHERE c.relname = 'horsies_task_history_forever_open'
        """
    )
    assert await cursor.fetchone() == (
        "FOR VALUES FROM (MINVALUE) TO ('9999-01-01 00:00:00+00')",
        'OPEN',
        True,
    )


_TASK_ROW_SQL: Final = """
    INSERT INTO horsies_tasks (
        id, task_name, queue_name, priority, input, task_options, execution, retention_class_key,
        sent_at, created_at, command_fingerprint_version, command_fingerprint, idempotency_key_digest,
        status, enqueued_at, claimed_at, claimed_by_worker_id, claim_expires_at, started_at,
        finalizing_at, finalizing_by_worker_id, worker_pid, worker_hostname, worker_process_name,
        retry_count, max_retries, next_retry_at, good_until, is_workflow_task, updated_at,
        retain_rerun_input, rerun_of_task_id, rerun_root_task_id
    ) VALUES (
        %(id)s, 'notify.task', 'default', 100, '{}', NULL, 'ISOLATED', 'forever',
        now(), now(), 1, sha256('x'::bytea), NULL,
        CAST(%(status)s AS varchar), now(),
        CASE WHEN CAST(%(status)s AS varchar) = 'PENDING' THEN NULL ELSE now() END,
        CASE WHEN CAST(%(status)s AS varchar) = 'PENDING' THEN NULL ELSE 'w1' END,
        CASE WHEN CAST(%(status)s AS varchar) = 'CLAIMED' THEN now() + interval '1 minute' END,
        CASE WHEN CAST(%(status)s AS varchar) = 'RUNNING' THEN now() END,
        NULL, NULL,
        CASE WHEN CAST(%(status)s AS varchar) = 'RUNNING' THEN 1 END,
        CASE WHEN CAST(%(status)s AS varchar) = 'RUNNING' THEN 'host' END,
        CASE WHEN CAST(%(status)s AS varchar) = 'RUNNING' THEN 'proc' END,
        0, 0, NULL, NULL, FALSE, now(), FALSE, NULL, NULL
    )
"""

_INSERT_CHANNELS: Final = ('task_new', 'task_queue_default', 'horsies_task_status')
_NOTIFY_WAIT_S: Final = 0.5
_TASK_ID: Final = '00000000-0000-7000-8000-0000000000c1'


@pytest.mark.parametrize(
    ('status', 'expected'),
    [
        (
            'PENDING',
            [
                ('task_new', _TASK_ID),
                ('task_queue_default', 'new'),
                ('horsies_task_status', _TASK_ID),
            ],
        ),
        ('CLAIMED', [('horsies_task_status', _TASK_ID)]),
        ('RUNNING', [('horsies_task_status', _TASK_ID)]),
    ],
)
@pytest.mark.asyncio(loop_scope='function')
async def test_a_task_insert_wakes_its_queue_only_when_pending(
    installed: psycopg.AsyncConnection[tuple[object, ...]],
    status: str,
    expected: list[tuple[str, str]],
) -> None:
    """Every insert sends the status event; only a PENDING insert sends the
    task_new and task_queue_<queue> wake."""
    await installed.set_autocommit(True)
    for channel in _INSERT_CHANNELS:
        await installed.execute(f'LISTEN {channel}'.encode('utf-8'))
    await installed.execute(_TASK_ROW_SQL, {'id': _TASK_ID, 'status': status})

    received = [
        (notify.channel, notify.payload)
        async for notify in installed.notifies(timeout=_NOTIFY_WAIT_S)
    ]

    assert received == expected
