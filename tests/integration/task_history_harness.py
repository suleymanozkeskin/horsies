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
    task_id uuid PRIMARY KEY,
    command_fingerprint_version smallint NOT NULL,
    command_fingerprint bytea NOT NULL
)
"""
"""Pre-cutover stand-in carrying exactly the columns the lookup probes."""


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
        async with engine.begin() as connection:
            for fragment in frozen_fragments():
                await connection.execute(text(fragment))
            await connection.execute(text(LIVE_STANDIN_DDL))
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


def day_bounds(day: datetime) -> tuple[datetime, datetime]:
    lower = day.astimezone(timezone.utc).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    return lower, lower + timedelta(days=1)
