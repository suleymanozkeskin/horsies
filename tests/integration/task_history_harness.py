"""Disposable-schema harness for task-history integration tests.

Each test gets its own PostgreSQL schema with the baseline migration
installed and `search_path` baked into every connection the engine hands
out — including the autocommit connections the detach path creates for
itself. The baseline SQL names no schema, so every relation and function
resolves into the disposable schema.

Nothing here touches shared relations: setup creates the schema, teardown
drops it cascade and verifies nothing remains.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from hashlib import sha256
from typing import AsyncGenerator, Final

import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine, create_async_engine

from tests.attempt_snapshot_support import encode_attempt_snapshot
from tests.baseline_sql import baseline_sql
from tests.integration.history_seeding import publish_history_readers
from tests.task_ids import mint_task_id
from horsies.core.utils.url import sqlalchemy_url

TASK_HISTORY_PARENT: Final = 'horsies_task_history'
"""The history parent table of the baseline."""

FOREVER_LEAF: Final = 'horsies_task_history_forever_open'
"""The static leaf of the forever class of the baseline."""


def _database_url() -> str:
    return (
        os.environ.get('HORSIES_TEST_DATABASE_URL')
        or os.environ.get('HORSES_E2E_DB_URL')
        or 'postgresql://postgres:'
        f'{os.environ["DB_PASSWORD"]}@localhost:5432/horsies'
    )


@dataclass(frozen=True, slots=True)
class HistorySchema:
    """One disposable schema and the engine whose connections live in it."""

    schema_name: str
    engine: AsyncEngine


async def install_baseline_schema(connection: AsyncConnection) -> None:
    """Run the baseline migration in the connection's current schema.

    The text goes through the driver connection with no parameters, so
    psycopg sends it with the simple query protocol and reads no
    placeholders in it. It joins the caller's transaction.
    """
    raw = await connection.get_raw_connection()
    driver = raw.driver_connection
    assert driver is not None, 'the engine has a driver connection'
    await driver.execute(baseline_sql().encode('utf-8'))


def task_history_schema_fixture(schema_name: str):  # type: ignore[no-untyped-def]
    """Build a function-scoped fixture installing the baseline schema.

    Function scope keeps every test hermetic — each one gets a freshly
    installed schema — and stays inside the repo's strict asyncio mode
    with function-scoped event loops.
    """

    @pytest_asyncio.fixture()
    async def history_schema() -> AsyncGenerator[HistorySchema, None]:
        url = _database_url()
        admin_engine = create_async_engine(
            sqlalchemy_url(url), isolation_level='AUTOCOMMIT',
        )
        async with admin_engine.connect() as connection:
            await connection.execute(
                text(f'DROP SCHEMA IF EXISTS {schema_name} CASCADE')
            )
            await connection.execute(text(f'CREATE SCHEMA {schema_name}'))
        engine = create_async_engine(
            sqlalchemy_url(url),
            connect_args={'options': f'-csearch_path={schema_name}'},
        )
        async with engine.begin() as connection:
            await install_baseline_schema(connection)
        yield HistorySchema(schema_name=schema_name, engine=engine)
        await engine.dispose()
        async with admin_engine.connect() as connection:
            await connection.execute(
                text(f'DROP SCHEMA {schema_name} CASCADE')
            )
            remains = (
                await connection.execute(
                    text(
                        'SELECT EXISTS (SELECT 1 FROM pg_namespace '
                        'WHERE nspname = :name)'
                    ),
                    {'name': schema_name},
                )
            ).scalar_one()
        await admin_engine.dispose()
        if remains:
            raise RuntimeError('disposable schema cleanup left objects behind')

    return history_schema


HEARTBEAT_CLASS_HORIZON_HOURS = 3
"""The heartbeat horizon that a class registration stores."""

_REGISTER_CLASSES_SQL = text(
    'SELECT class_key, outcome FROM horsies_register_retention_classes('
    'CAST(:days AS smallint[]), CAST(:hours AS smallint))'
)
_CREATE_HISTORY_LEAF_SQL = text(
    'SELECT outcome, leaf_name, conflict_kind FROM horsies_create_history_leaf('
    'CAST(:class_key AS text), CAST(:lower_anchor AS timestamptz))'
)


async def register_class(
    connection: AsyncConnection,
    class_key: str,
    *,
    duration_days: int = 30,
) -> str:
    """Register the finite class of ``duration_days`` days
    (``horsies_register_retention_classes``) and return its parent relation
    name. ``class_key`` must be the key of that period."""
    rows = (
        await connection.execute(
            _REGISTER_CLASSES_SQL,
            {'days': [duration_days], 'hours': HEARTBEAT_CLASS_HORIZON_HOURS},
        )
    ).all()
    outcomes = {row.class_key: row.outcome for row in rows}
    assert outcomes.get(class_key) in ('REGISTERED', 'VERIFIED'), (
        f'class registration of {class_key} ({duration_days} days) failed: {outcomes}'
    )
    return f'{TASK_HISTORY_PARENT}_{class_key}'


async def current_forever_leaf(connection: AsyncConnection) -> str:
    """Return the static ``forever`` leaf, after checking its catalog row."""
    leaf_name = (
        await connection.execute(
            text(
                "SELECT leaf_name FROM horsies_task_history_leaf_catalog "
                "WHERE class_key = 'forever' "
                "AND lower_bound_kind = 'OPEN' "
                'AND detached_at IS NULL AND dropped_at IS NULL '
                'AND upper_anchor > statement_timestamp()'
            )
        )
    ).scalar_one()
    assert leaf_name == FOREVER_LEAF
    return str(leaf_name)


def frozen_history_row(
    *,
    task_id: str,
    class_key: str,
    terminal_at: datetime,
    status: str = 'COMPLETED',
) -> dict[str, object]:
    """Complete parameter set for one history insert."""
    payload = b'{}'
    snapshot = encode_attempt_snapshot(())
    return {
        'task_id': task_id,
        'task_name': 'integration.task',
        'queue_name': 'default',
        'priority': 50,
        'command_fingerprint_version': 1,
        'command_fingerprint': sha256(task_id.encode()).digest(),
        'status': status,
        'terminalization_kind': 'COMPLETE_FUSED',
        'terminal_at': terminal_at,
        'retention_class_key': class_key,
        'sent_at': terminal_at - timedelta(minutes=1),
        'enqueued_at': terminal_at - timedelta(minutes=1),
        'created_at': terminal_at - timedelta(minutes=1),
        'retry_count': 0,
        'max_retries': 0,
        'result_envelope_version': 1,
        'result_payload': payload,
        'result_digest': sha256(payload).digest(),
        'is_workflow_task': False,
        'history_schema_version': 1,
        'attempt_archive_version': snapshot.version,
        'attempt_snapshot': snapshot.payload,
        'attempt_snapshot_digest': snapshot.digest,
        'rerun_input_disposition': 'NEVER_ELIGIBLE',
    }


INSERT_HISTORY_ROW_SQL = """
INSERT INTO horsies_task_history (
    task_id, task_name, queue_name, priority,
    command_fingerprint_version, command_fingerprint,
    status, terminalization_kind, terminal_at,
    retention_class_key, sent_at, enqueued_at, created_at,
    retry_count, max_retries,
    result_envelope_version,
    result_payload, result_digest,
    is_workflow_task, history_schema_version,
    attempt_archive_version, attempt_snapshot, attempt_snapshot_digest,
    rerun_input_disposition
) VALUES (
    :task_id, :task_name, :queue_name, :priority,
    :command_fingerprint_version, :command_fingerprint,
    :status, :terminalization_kind, :terminal_at,
    :retention_class_key, :sent_at, :enqueued_at, :created_at,
    :retry_count, :max_retries,
    :result_envelope_version,
    :result_payload, :result_digest,
    :is_workflow_task, :history_schema_version,
    :attempt_archive_version, :attempt_snapshot, :attempt_snapshot_digest,
    :rerun_input_disposition
)
"""


def terminalization_schema_fixture(  # type: ignore[no-untyped-def]
    schema_name: str,
):
    """Fixture for the terminalization suites: the baseline schema, whose
    migration holds every table, the move family and the outcome types.

    The heartbeat parent is RANGE-partitioned and has no leaf until a
    suite registers the heartbeat class and ensures coverage.
    """

    @pytest_asyncio.fixture()
    async def terminalization_schema() -> AsyncGenerator[HistorySchema, None]:
        url = _database_url()
        admin_engine = create_async_engine(
            sqlalchemy_url(url), isolation_level='AUTOCOMMIT',
        )
        async with admin_engine.connect() as connection:
            await connection.execute(
                text(f'DROP SCHEMA IF EXISTS {schema_name} CASCADE')
            )
            await connection.execute(text(f'CREATE SCHEMA {schema_name}'))
        engine = create_async_engine(
            sqlalchemy_url(url),
            connect_args={'options': f'-csearch_path={schema_name}'},
        )
        async with engine.begin() as connection:
            await install_baseline_schema(connection)
        yield HistorySchema(schema_name=schema_name, engine=engine)
        await engine.dispose()
        async with admin_engine.connect() as connection:
            await connection.execute(text(f'DROP SCHEMA {schema_name} CASCADE'))
        await admin_engine.dispose()

    return terminalization_schema


async def prepare_move_storage(
    connection: AsyncConnection,
    class_key: str,
    *,
    duration_days: int = 30,
) -> None:
    """Register a class and publish coverage for terminalization tests: the
    daily leaves of today and the next 2 days (``horsies_create_history_leaf``),
    then the readers."""
    _ = await register_class(connection, class_key, duration_days=duration_days)
    database_now = (await connection.execute(text('SELECT now()'))).scalar_one()
    assert isinstance(database_now, datetime), database_now
    today = database_now.astimezone(timezone.utc).replace(
        hour=0, minute=0, second=0, microsecond=0,
    )
    for day_offset in range(3):
        leaf = (
            await connection.execute(
                _CREATE_HISTORY_LEAF_SQL,
                {'class_key': class_key, 'lower_anchor': today + timedelta(days=day_offset)},
            )
        ).one()
        assert leaf.outcome in ('CREATED', 'ALREADY_CONFORMANT', 'INDEX_REPAIRED'), (
            f'leaf coverage of {class_key} refused: {leaf.outcome} {leaf.leaf_name} '
            f'{leaf.conflict_kind}'
        )
    await publish_history_readers(connection)


async def insert_live_task(
    connection: AsyncConnection,
    *,
    class_key: str,
    status: str = 'RUNNING',
    worker: str | None = 'worker-1',
    key_digest: bytes | None = None,
    retain: bool = False,
    input_text: str = '{}',
    task_options: str | None = None,
    is_workflow_task: bool = False,
    started_at_offset: timedelta = timedelta(0),
    good_until_offset: timedelta | None = None,
    task_id: str | None = None,
) -> str:
    """Insert one live row with every baseline column named, and return its id.

    The id is minted from the process generator unless `task_id` names one.
    A sent task carries a command fingerprint; a workflow task carries none.
    A CLAIMED row carries a lease. With `retain` the move builds the rerun
    envelope from `input_text` and `task_options`.
    """
    task_id = task_id if task_id is not None else str(mint_task_id())
    now = datetime.now(timezone.utc)
    await connection.execute(
        text(
            """
            INSERT INTO horsies_tasks (
                id, task_name, queue_name, priority, input, task_options,
                execution, retention_class_key, sent_at, created_at,
                command_fingerprint_version, command_fingerprint,
                idempotency_key_digest, status, enqueued_at, claimed_at,
                claimed_by_worker_id, claim_expires_at, started_at,
                finalizing_at, finalizing_by_worker_id, worker_pid,
                worker_hostname, worker_process_name, retry_count,
                max_retries, next_retry_at, good_until, is_workflow_task,
                updated_at, retain_rerun_input, rerun_of_task_id,
                rerun_root_task_id
            ) VALUES (
                CAST(:task_id AS uuid), 'it.move', 'default', 50, :input_text,
                :task_options, 'ISOLATED', :class_key, :now, :now,
                :fingerprint_version, :fingerprint,
                :key_digest, :status, :now, :now,
                :worker, :claim_expires_at, :started_at,
                NULL, NULL, 4242,
                'it-host', 'it-proc', 0,
                0, NULL, :good_until, :is_workflow_task,
                :now, :retain, NULL,
                NULL
            )
            """
        ),
        {
            'task_id': task_id,
            'input_text': input_text,
            'task_options': task_options,
            'class_key': class_key,
            'now': now,
            'fingerprint_version': None if is_workflow_task else 1,
            'fingerprint': (
                None if is_workflow_task else sha256(task_id.encode()).digest()
            ),
            'key_digest': key_digest,
            'status': status,
            'worker': worker,
            'claim_expires_at': (
                now + timedelta(minutes=5) if status == 'CLAIMED' else None
            ),
            'started_at': now + started_at_offset,
            'good_until': (
                now + good_until_offset
                if good_until_offset is not None
                else None
            ),
            'is_workflow_task': is_workflow_task,
            'retain': retain,
        },
    )
    return task_id


async def create_workflow(
    connection: AsyncConnection,
    *,
    status: str,
) -> str:
    """Insert one root workflow row with every column named; return its id."""
    from uuid import uuid4

    workflow_id = str(uuid4())
    await connection.execute(
        text(
            """
            INSERT INTO horsies_workflows (
                id, name, definition_key, status, on_error, output_task_index,
                success_policy, result, error, parent_workflow_id,
                parent_task_index, root_workflow_id, depth, sent_at,
                created_at, started_at, completed_at, updated_at
            ) VALUES (
                CAST(:workflow_id AS uuid), 'it.workflow', NULL, :status,
                'fail', NULL, NULL, NULL, NULL, NULL,
                NULL, CAST(:workflow_id AS uuid), 0, now(),
                now(), now(), NULL, now()
            )
            """
        ),
        {'workflow_id': workflow_id, 'status': status},
    )
    return workflow_id


async def link_workflow_node(
    connection: AsyncConnection,
    task_id: str,
    *,
    workflow_id: str,
    node_status: str,
    task_index: int = 0,
    node_key: str | None = 'node-0',
) -> str:
    """Link one task into a workflow through a task node row; returns the node row id."""
    from uuid import uuid4

    node_id = str(uuid4())
    await connection.execute(
        text(
            """
            INSERT INTO horsies_workflow_tasks (downstream_subworkflow,
                id, workflow_id, task_index, node_id, task_name, input,
                queue_name, priority, dependencies, args_from,
                workflow_ctx_from, allow_failed_deps, join_type, min_success,
                task_options, max_retries, execution, good_until,
                retention_class_key, status, task_id, is_subworkflow,
                sub_workflow_id, sub_workflow_name, sub_definition_key,
                sub_workflow_params, sub_workflow_summary, result, error,
                created_at, started_at, completed_at
            ) VALUES ('NONE',
                CAST(:node_id AS uuid), CAST(:workflow_id AS uuid), :task_index,
                :node_key, 'it.move', '{}',
                'default', 50, '{}', NULL,
                NULL, FALSE, 'all', NULL,
                NULL, 0, 'ISOLATED', NULL,
                'forever', :node_status, CAST(:task_id AS uuid), FALSE,
                NULL, NULL, NULL,
                NULL, NULL, NULL, NULL,
                now(), NULL, NULL
            )
            """
        ),
        {
            'node_id': node_id,
            'workflow_id': workflow_id,
            'task_id': task_id,
            'task_index': task_index,
            'node_key': node_key,
            'node_status': node_status,
        },
    )
    return node_id


def day_bounds(day: datetime) -> tuple[datetime, datetime]:
    lower = day.astimezone(timezone.utc).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    return lower, lower + timedelta(days=1)
