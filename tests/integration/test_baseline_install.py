"""The broker's schema install on fresh and foreign databases.

Each test builds its own database (``isolated_test_database``). A refused
database is checked to be unchanged.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncGenerator
from typing import Final, LiteralString

import psycopg
import pytest
import pytest_asyncio
from pydantic import SecretStr

from horsies.core.baseline.migration import BaselineMigration, load_baseline_migration
from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.brokers.result_types import BrokerErrorCode, BrokerOperationFailed
from horsies.core.models.broker import PostgresConfig
from horsies.core.types.result import Err, Ok
from horsies.core.utils.url import to_psycopg_url
from tests.integration.conftest import DB_URL
from tests.integration.isolated_database import isolated_test_database

pytestmark = [pytest.mark.integration]

Connection = psycopg.AsyncConnection[tuple[object, ...]]

_OBJECT_COUNT_SQL: Final[LiteralString] = """
    SELECT
        (SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
         WHERE n.nspname = current_schema() AND c.relname LIKE 'horsies%%'),
        (SELECT count(*) FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
         WHERE n.nspname = current_schema() AND p.proname LIKE 'horsies%%')
"""


def _migration() -> BaselineMigration:
    match load_baseline_migration():
        case Ok(loaded):
            return loaded
        case Err(unreadable):
            pytest.fail(f'baseline SQL unreadable: {unreadable}')


@pytest_asyncio.fixture
async def database_url() -> AsyncGenerator[str]:
    """A fresh, empty database."""
    async with isolated_test_database(DB_URL) as url:
        yield url


async def _connect(url: str) -> Connection:
    return await psycopg.AsyncConnection.connect(to_psycopg_url(url), autocommit=True)


async def _initialize(url: str, *, run_schema_migrations: bool = True) -> Ok[None] | Err[str]:
    """Run the install with a new broker; return the refusal text on failure."""
    broker = PostgresBroker(
        PostgresConfig(database_url=SecretStr(url)),
        run_schema_migrations=run_schema_migrations,
    )
    try:
        match await broker.ensure_schema_initialized():
            case Ok():
                return Ok(None)
            case Err(BrokerOperationFailed(code=BrokerErrorCode.SCHEMA_INIT_FAILED) as failed):
                return Err(failed.message)
            case Err(other):
                pytest.fail(f'unexpected install error: {other!r}')
    finally:
        _ = await broker.close_async()


async def _fetch_one(connection: Connection, query: LiteralString) -> tuple[object, ...]:
    cursor = await connection.execute(query)
    row = await cursor.fetchone()
    assert row is not None
    return row


async def _installed(url: str) -> None:
    assert await _initialize(url) == Ok(None)


@pytest.mark.asyncio(loop_scope='function')
async def test_fresh_database_reaches_version_1000(database_url: str) -> None:
    await _installed(database_url)
    migration = _migration()
    connection = await _connect(database_url)
    try:
        cursor = await connection.execute(
            'SELECT version, description, success, checksum FROM horsies_migrations'
        )
        assert await cursor.fetchall() == [
            (1000, 'baseline', True, migration.checksum),
        ]
        assert await _fetch_one(connection, 'SELECT only_row, family FROM horsies_client_family') == (
            True,
            'PYTHON',
        )
    finally:
        await connection.close()


@pytest.mark.asyncio(loop_scope='function')
async def test_second_install_changes_nothing(database_url: str) -> None:
    await _installed(database_url)
    connection = await _connect(database_url)
    try:
        before = (
            await _fetch_one(connection, 'SELECT installed_on, execution_time FROM horsies_migrations'),
            await _fetch_one(connection, 'SELECT recorded_at FROM horsies_client_family'),
            await _fetch_one(connection, _OBJECT_COUNT_SQL),
        )
        await _installed(database_url)
        after = (
            await _fetch_one(connection, 'SELECT installed_on, execution_time FROM horsies_migrations'),
            await _fetch_one(connection, 'SELECT recorded_at FROM horsies_client_family'),
            await _fetch_one(connection, _OBJECT_COUNT_SQL),
        )
    finally:
        await connection.close()
    assert after == before


@pytest.mark.asyncio(loop_scope='function')
async def test_concurrent_installs_apply_the_migration_once(database_url: str) -> None:
    results = await asyncio.gather(*(_initialize(database_url) for _ in range(3)))
    assert results == [Ok(None)] * 3
    connection = await _connect(database_url)
    try:
        assert await _fetch_one(connection, 'SELECT count(*) FROM horsies_migrations') == (1,)
    finally:
        await connection.close()


@pytest.mark.asyncio(loop_scope='function')
async def test_migrations_off_runs_no_statement(database_url: str) -> None:
    assert await _initialize(database_url, run_schema_migrations=False) == Ok(None)
    connection = await _connect(database_url)
    try:
        assert await _fetch_one(connection, _OBJECT_COUNT_SQL) == (0, 0)
    finally:
        await connection.close()


@pytest.mark.asyncio(loop_scope='function')
async def test_zero_x_database_is_refused_and_unchanged(database_url: str) -> None:
    connection = await _connect(database_url)
    try:
        await connection.execute(
            'CREATE TABLE horsies_schema_version (version integer PRIMARY KEY, applied_at timestamptz)'
        )
        await connection.execute('INSERT INTO horsies_schema_version VALUES (61, now())')
        await connection.execute('CREATE TABLE horsies_tasks (id uuid PRIMARY KEY)')
        before = await _fetch_one(connection, _OBJECT_COUNT_SQL)
        match await _initialize(database_url):
            case Err(message):
                assert 'horsies 0.x schema' in message
                assert 'horsies_schema_version, horsies_tasks' in message
            case Ok():
                pytest.fail('a 0.x database was accepted')
        assert await _fetch_one(connection, _OBJECT_COUNT_SQL) == before
    finally:
        await connection.close()


@pytest.mark.asyncio(loop_scope='function')
async def test_unknown_horsies_tables_are_refused(database_url: str) -> None:
    connection = await _connect(database_url)
    try:
        await connection.execute('CREATE TABLE horsies_tasks (id uuid PRIMARY KEY)')
        match await _initialize(database_url):
            case Err(message):
                assert 'no schema bookkeeping' in message
            case Ok():
                pytest.fail('unknown horsies tables were accepted')
    finally:
        await connection.close()


@pytest.mark.parametrize(
    ('change', 'expected'),
    [
        ("UPDATE horsies_migrations SET checksum = sha256('other'::bytea)", 'applied from another text'),
        ('UPDATE horsies_migrations SET success = FALSE', 'recorded as failed'),
        (
            "INSERT INTO horsies_migrations VALUES (1001, 'later', now(), TRUE, sha256('x'::bytea), 1)",
            'newer than this build supports',
        ),
        (
            "INSERT INTO horsies_migrations VALUES (65, 'rust', now(), TRUE, sha256('x'::bytea), 1)",
            'versions this build does not ship',
        ),
        ("UPDATE horsies_client_family SET family = 'RUST'", 'installed by the RUST client'),
    ],
)
@pytest.mark.asyncio(loop_scope='function')
async def test_installed_database_with_another_state_is_refused(
    database_url: str,
    change: LiteralString,
    expected: str,
) -> None:
    await _installed(database_url)
    connection = await _connect(database_url)
    try:
        await connection.execute(change)
        before = await _fetch_one(connection, _OBJECT_COUNT_SQL)
        match await _initialize(database_url):
            case Err(message):
                assert expected in message
            case Ok():
                pytest.fail(f'install accepted the database after: {change}')
        assert await _fetch_one(connection, _OBJECT_COUNT_SQL) == before
    finally:
        await connection.close()


@pytest.mark.asyncio(loop_scope='function')
async def test_missing_family_row_is_written(database_url: str) -> None:
    await _installed(database_url)
    connection = await _connect(database_url)
    try:
        await connection.execute('DELETE FROM horsies_client_family')
        await _installed(database_url)
        assert await _fetch_one(connection, 'SELECT family FROM horsies_client_family') == ('PYTHON',)
    finally:
        await connection.close()


@pytest.mark.asyncio(loop_scope='function')
async def test_install_holds_the_objects_of_the_0_7_0_schema_checks(database_url: str) -> None:
    """The execution column and its CHECK, the class claim index, class_capacity, the batched success."""
    await _installed(database_url)
    connection = await _connect(database_url)
    try:
        row = await _fetch_one(
            connection,
            """
            SELECT
                (SELECT attnotnull FROM pg_attribute
                 WHERE attrelid = 'horsies_tasks'::regclass AND attname = 'execution'),
                (SELECT pg_get_constraintdef(oid) FROM pg_constraint
                 WHERE conname = 'horsies_tasks_execution_known'),
                (SELECT pg_get_indexdef('horsies_tasks_claim_pending'::regclass)),
                (SELECT attnotnull FROM pg_attribute
                 WHERE attrelid = 'horsies_worker_states'::regclass AND attname = 'class_capacity'),
                to_regprocedure(
                    'horsies_complete_tasks_fused(uuid[], timestamptz[], text[], text, boolean)'
                ) IS NOT NULL
            """,
        )
    finally:
        await connection.close()
    execution_not_null, execution_check, claim_index, class_capacity_not_null, fused = row
    assert execution_not_null is True
    assert "'ISOLATED'" in str(execution_check)
    assert "'SHARED_ASYNC'" in str(execution_check)
    assert "'SHARED_THREAD'" in str(execution_check)
    assert '(queue_name, execution, priority, enqueued_at, id)' in str(claim_index)
    assert "WHERE ((status)::text = 'PENDING'::text)" in str(claim_index)
    assert class_capacity_not_null is True
    assert fused is True
