"""Disposable-schema harness for task-history integration tests.

Each test module gets its own PostgreSQL schema with the frozen fragment
list installed and `search_path` baked into every connection the engine
hands out — including the autocommit connections the detach path creates
for itself. Unqualified relation names in the production DDL therefore
resolve into the disposable schema, and a minimal stand-in for the live
table (which gains its fingerprint columns only at cutover) shadows the
real one for the generated lookup function.

Nothing here touches shared relations: setup creates the schema, teardown
drops it cascade and verifies nothing remains.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from hashlib import sha256
from typing import AsyncGenerator

import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine, create_async_engine

from horsies.core.history.ddl.classes import (
    ClassRegistered,
    register_finite_retention_class,
)
from horsies.core.history.ddl.fragments import frozen_fragments


def _database_url() -> str:
    return (
        os.environ.get('HORSIES_TEST_DATABASE_URL')
        or os.environ.get('HORSES_E2E_DB_URL')
        or 'postgresql+psycopg://postgres:'
        f'{os.environ["DB_PASSWORD"]}@localhost:5432/horsies'
    )


LIVE_STANDIN_DDL = """
CREATE TABLE horsies_tasks (
    id uuid PRIMARY KEY,
    command_fingerprint_version smallint NOT NULL,
    command_fingerprint bytea NOT NULL
)
"""
"""Stand-in carrying the real identifier column name (`id`) plus the
fingerprint columns the lookup probes — the column name is the point:
a `task_id` stand-in masked a generator defect once."""


@dataclass(frozen=True, slots=True)
class HistorySchema:
    """One disposable schema and the engine whose connections live in it."""

    schema_name: str
    engine: AsyncEngine


def task_history_schema_fixture(schema_name: str):  # type: ignore[no-untyped-def]
    """Build a function-scoped fixture installing the frozen program.

    Function scope keeps every test hermetic — each one gets a freshly
    installed schema — and stays inside the repo's strict asyncio mode
    with function-scoped event loops.
    """

    @pytest_asyncio.fixture()
    async def history_schema() -> AsyncGenerator[HistorySchema, None]:
        url = _database_url()
        admin_engine = create_async_engine(url, isolation_level='AUTOCOMMIT')
        async with admin_engine.connect() as connection:
            await connection.execute(
                text(f'DROP SCHEMA IF EXISTS {schema_name} CASCADE')
            )
            await connection.execute(text(f'CREATE SCHEMA {schema_name}'))
        engine = create_async_engine(
            url,
            connect_args={'options': f'-csearch_path={schema_name}'},
        )
        from horsies.core.history.phase2.quarantine import (
            quarantine_fragments,
        )

        async with engine.begin() as connection:
            for fragment in (
                *frozen_fragments(),
                LIVE_STANDIN_DDL,
                WORKFLOW_TASKS_STANDIN_DDL,
                *quarantine_fragments(),
            ):
                await connection.execute(text(fragment))
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


async def register_class(
    connection: AsyncConnection,
    class_key: str,
    *,
    duration_days: int = 30,
) -> str:
    """Register one finite class and return its parent relation name."""
    outcome = await register_finite_retention_class(
        connection,
        class_key=class_key,
        duration=timedelta(days=duration_days),
    )
    match outcome:
        case ClassRegistered(finite_parent_name=parent_name):
            return parent_name
        case _:
            raise AssertionError(f'class registration failed: {outcome!r}')


def frozen_history_row(
    *,
    task_id: str,
    class_key: str,
    terminal_at: datetime,
    status: str = 'COMPLETED',
) -> dict[str, object]:
    """Complete parameter set for one frozen-projection history insert."""
    payload = b'{}'
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
        'retention_anchor_at': terminal_at,
        'retention_class_key': class_key,
        'enqueued_at': terminal_at - timedelta(minutes=1),
        'created_at': terminal_at - timedelta(minutes=1),
        'retry_count': 0,
        'max_retries': 0,
        'result_envelope_version': 1,
        'result_codec': 'json-utf8',
        'result_content_type': 'application/json',
        'result_payload': payload,
        'result_digest': sha256(payload).digest(),
        'is_workflow_task': False,
        'history_schema_version': 1,
    }


INSERT_HISTORY_ROW_SQL = """
INSERT INTO horsies_task_history (
    task_id, task_name, queue_name, priority,
    command_fingerprint_version, command_fingerprint,
    status, terminalization_kind, terminal_at, retention_anchor_at,
    retention_class_key, enqueued_at, created_at,
    retry_count, max_retries,
    result_envelope_version, result_codec, result_content_type,
    result_payload, result_digest,
    is_workflow_task, history_schema_version
) VALUES (
    :task_id, :task_name, :queue_name, :priority,
    :command_fingerprint_version, :command_fingerprint,
    :status, :terminalization_kind, :terminal_at, :retention_anchor_at,
    :retention_class_key, :enqueued_at, :created_at,
    :retry_count, :max_retries,
    :result_envelope_version, :result_codec, :result_content_type,
    :result_payload, :result_digest,
    :is_workflow_task, :history_schema_version
)
"""


FULL_LIVE_STANDIN_DDL = """
CREATE TABLE horsies_tasks (
    id uuid PRIMARY KEY,
    task_name varchar(255) NOT NULL,
    queue_name varchar(100) NOT NULL,
    priority integer NOT NULL,
    status text NOT NULL CHECK (status IN ('PENDING', 'CLAIMED', 'RUNNING')),
    result text,
    sent_at timestamptz,
    enqueued_at timestamptz NOT NULL,
    claimed_at timestamptz,
    started_at timestamptz,
    finalizing_at timestamptz,
    created_at timestamptz NOT NULL,
    good_until timestamptz,
    retry_count integer NOT NULL,
    max_retries integer NOT NULL,
    claimed_by_worker_id varchar(255),
    worker_hostname varchar(255),
    worker_pid integer,
    worker_process_name varchar(255),
    is_workflow_task boolean NOT NULL,
    command_fingerprint_version smallint NOT NULL,
    command_fingerprint bytea NOT NULL,
    retention_class_key varchar(64) NOT NULL,
    input_digest bytea,
    rerun_of_task_id uuid,
    rerun_root_task_id uuid,
    idempotency_key_digest bytea,
    retain_rerun_input boolean NOT NULL,
    prepared_rerun_input_disposition varchar(32) NOT NULL,
    prepared_rerun_input_version smallint,
    prepared_rerun_input_codec varchar(64),
    prepared_rerun_input_content_type varchar(255),
    prepared_rerun_input_digest bytea,
    prepared_rerun_input_inline bytea,
    prepared_rerun_input_reference varchar(2048)
)
"""
"""Post-cutover live stand-in: every column the move's snapshot reads."""

ATTEMPTS_STANDIN_DDL = """
CREATE TABLE horsies_task_attempts (
    task_id uuid NOT NULL,
    attempt integer NOT NULL,
    outcome text NOT NULL,
    will_retry boolean NOT NULL,
    started_at timestamptz NOT NULL,
    finished_at timestamptz NOT NULL,
    error_code text,
    error_message text,
    failed_reason text,
    worker_id text,
    worker_hostname text,
    worker_pid integer,
    worker_process_name text,
    UNIQUE (task_id, attempt)
)
"""

WORKFLOWS_STANDIN_DDL = """
CREATE TABLE horsies_workflows (
    id uuid PRIMARY KEY,
    status text NOT NULL,
    depth integer,
    root_workflow_id uuid,
    on_error text
)
"""
"""Stand-in with the columns the workflow-scoped sweeps join on."""

HEARTBEATS_STANDIN_DDL = """
CREATE TABLE horsies_heartbeats (
    task_id uuid NOT NULL,
    role text NOT NULL,
    sent_at timestamptz NOT NULL
)
"""
"""Stand-in with the production column names the stale capture reads."""

WORKFLOW_TASKS_STANDIN_DDL = """
CREATE TABLE horsies_workflow_tasks (
    id uuid UNIQUE NOT NULL,
    workflow_id uuid NOT NULL,
    task_id uuid,
    task_index integer NOT NULL,
    node_id varchar(128),
    status text NOT NULL DEFAULT 'RUNNING',
    result text,
    completed_at timestamptz,
    UNIQUE (workflow_id, task_index)
)
"""
"""The default keeps earlier suites' nodes runnable — the deferred-kind
semantics they exercise — while the orphan suites set status explicitly.
A fixture default, not a schema recommendation."""


def terminalization_schema_fixture(schema_name: str):  # type: ignore[no-untyped-def]
    """Fixture for M8 suites: frozen + gated fragments, full stand-ins,
    outcome layer, completion family, and a published lookup pair.

    Gated fragments install through `gated_fragment()` — test-schema
    consumption per the pinned guardrail-2 boundary; production emission
    stays wave-4-sequenced.
    """

    @pytest_asyncio.fixture()
    async def terminalization_schema() -> AsyncGenerator[HistorySchema, None]:
        from horsies.core.history.ddl.conditional import (
            GatedFragment,
            gated_fragment,
        )
        from horsies.core.history.phase2.consumption import (
            consumption_fragments,
        )
        from horsies.core.history.phase2.quarantine import (
            quarantine_fragments,
        )
        from horsies.core.history.terminalization.move import (
            cancellation_family_fragments,
            completion_family_fragments,
            expiry_family_fragments,
            failure_family_fragments,
            workflow_node_family_fragments,
        )
        from horsies.core.history.terminalization.outcome import (
            outcome_fragments,
        )

        url = _database_url()
        admin_engine = create_async_engine(url, isolation_level='AUTOCOMMIT')
        async with admin_engine.connect() as connection:
            await connection.execute(
                text(f'DROP SCHEMA IF EXISTS {schema_name} CASCADE')
            )
            await connection.execute(text(f'CREATE SCHEMA {schema_name}'))
        engine = create_async_engine(
            url,
            connect_args={'options': f'-csearch_path={schema_name}'},
        )
        async with engine.begin() as connection:
            statements = (
                *frozen_fragments(),
                *gated_fragment(GatedFragment.ATTEMPT_SNAPSHOT_COLUMNS),
                *gated_fragment(GatedFragment.RERUN_INPUT_COLUMNS),
                *gated_fragment(GatedFragment.RESERVATION_REGISTRY_INDEXES),
                FULL_LIVE_STANDIN_DDL,
                ATTEMPTS_STANDIN_DDL,
                WORKFLOW_TASKS_STANDIN_DDL,
                WORKFLOWS_STANDIN_DDL,
                HEARTBEATS_STANDIN_DDL,
                *outcome_fragments(),
                *completion_family_fragments(),
                *failure_family_fragments(),
                *expiry_family_fragments(),
                *cancellation_family_fragments(),
                *workflow_node_family_fragments(),
                *consumption_fragments(),
                *quarantine_fragments(),
            )
            for statement in statements:
                await connection.execute(text(statement))
        yield HistorySchema(schema_name=schema_name, engine=engine)
        await engine.dispose()
        async with admin_engine.connect() as connection:
            await connection.execute(text(f'DROP SCHEMA {schema_name} CASCADE'))
        await admin_engine.dispose()

    return terminalization_schema


async def prepare_move_storage(
    connection: AsyncConnection,
    class_key: str,
) -> None:
    """Register a class and publish coverage for terminalization tests."""
    from horsies.core.history.commands import EnsureLeafCoverage
    from horsies.core.history.partitions.manager import ensure_leaf_coverage
    from horsies.core.history.reads.publisher import StagedLoaderPublisher

    await register_class(connection, class_key)
    outcomes = await ensure_leaf_coverage(
        connection,
        EnsureLeafCoverage(class_key=class_key, horizon_days=2),
        StagedLoaderPublisher(),
    )
    assert len(outcomes) == 3


async def insert_live_task(
    connection: AsyncConnection,
    *,
    class_key: str,
    status: str = 'RUNNING',
    worker: str | None = 'worker-1',
    key_digest: bytes | None = None,
    retain: bool = True,
    prepared_disposition: str = 'DECLINED_BY_POLICY',
    prepared_inline: bytes | None = None,
    is_workflow_task: bool = False,
    started_at_offset: timedelta = timedelta(0),
    good_until_offset: timedelta | None = None,
    live_result: str | None = None,
) -> str:
    """Insert one post-cutover live row and return its minted v7 id."""
    from horsies.core.history.identity.uuid7 import mint_task_id

    task_id = mint_task_id()
    now = datetime.now(timezone.utc)
    envelope: dict[str, object] = {
        'version': None,
        'codec': None,
        'content_type': None,
        'digest': None,
        'inline': None,
        'reference': None,
    }
    if prepared_disposition == 'INLINE':
        payload = prepared_inline if prepared_inline is not None else b'{"x":1}'
        envelope = {
            'version': 1,
            'codec': 'json-utf8',
            'content_type': 'application/json',
            'digest': sha256(payload).digest(),
            'inline': payload,
            'reference': None,
        }
    await connection.execute(
        text(
            """
            INSERT INTO horsies_tasks (
                id, task_name, queue_name, priority, status, result,
                enqueued_at, created_at, started_at, claimed_at,
                retry_count, max_retries, good_until,
                claimed_by_worker_id, worker_hostname, worker_pid,
                worker_process_name, is_workflow_task,
                command_fingerprint_version, command_fingerprint,
                retention_class_key, idempotency_key_digest,
                retain_rerun_input, prepared_rerun_input_disposition,
                prepared_rerun_input_version, prepared_rerun_input_codec,
                prepared_rerun_input_content_type,
                prepared_rerun_input_digest, prepared_rerun_input_inline,
                prepared_rerun_input_reference
            ) VALUES (
                CAST(:task_id AS uuid), 'it.move', 'default', 50, :status,
                :live_result,
                :now, :now, :started_at, :now,
                0, 0, :good_until,
                :worker, 'it-host', 4242, 'it-proc', :is_workflow_task,
                1, :fingerprint,
                :class_key, :key_digest,
                :retain, :disposition,
                :env_version, :env_codec, :env_content_type,
                :env_digest, :env_inline, :env_reference
            )
            """
        ),
        {
            'task_id': task_id,
            'status': status,
            'live_result': live_result,
            'now': now,
            'started_at': now + started_at_offset,
            'good_until': (
                now + good_until_offset
                if good_until_offset is not None
                else None
            ),
            'worker': worker,
            'is_workflow_task': is_workflow_task,
            'fingerprint': sha256(task_id.encode()).digest(),
            'class_key': class_key,
            'key_digest': key_digest,
            'retain': retain,
            'disposition': prepared_disposition,
            'env_version': envelope['version'],
            'env_codec': envelope['codec'],
            'env_content_type': envelope['content_type'],
            'env_digest': envelope['digest'],
            'env_inline': envelope['inline'],
            'env_reference': envelope['reference'],
        },
    )
    return task_id


async def create_workflow(
    connection: AsyncConnection,
    *,
    status: str,
) -> str:
    """Insert one workflow stand-in row and return its id."""
    from uuid import uuid4

    workflow_id = str(uuid4())
    await connection.execute(
        text(
            'INSERT INTO horsies_workflows (id, status) VALUES '
            '(CAST(:workflow_id AS uuid), :status)'
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
    """Link one task into a workflow through a node row; returns node id."""
    from uuid import uuid4

    node_id = str(uuid4())
    await connection.execute(
        text(
            'INSERT INTO horsies_workflow_tasks '
            '(id, workflow_id, task_id, task_index, node_id, status) VALUES '
            '(CAST(:node_id AS uuid), CAST(:workflow_id AS uuid), '
            'CAST(:task_id AS uuid), :task_index, :node_key, :node_status)'
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
