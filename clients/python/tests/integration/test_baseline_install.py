"""The broker's schema install on fresh and foreign databases.

Each test builds its own database (``isolated_test_database``). A refused
database is checked to be unchanged.
"""

from __future__ import annotations

import asyncio
import uuid
from collections.abc import AsyncGenerator
from datetime import datetime, timedelta, timezone
from typing import Final, LiteralString

import psycopg
import pytest
import pytest_asyncio
from pydantic import SecretStr
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from horsies._core import (
    CoreContractViolation,
    IdMintRefused,
    mint_task_id as core_mint_task_id,
)
from horsies.core.brokers.enqueue_task import SendKind
from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.history.periods import RetentionDays, period_class_key
from horsies.core.brokers.result_types import BrokerErrorCode, BrokerOperationFailed
from horsies.core.models.broker import PostgresConfig
from horsies.core.models.schema_mode import SchemaMode
from horsies.core.models.retention import RetentionConfig
from horsies.core.types.result import Err, Ok, is_ok
from horsies.core.types.status import TaskStatus
from horsies.monitoring import cancel_task
from horsies.core.utils.url import to_psycopg_url
from tests.sqlalchemy_url import sqlalchemy_url
from tests.baseline_sql import migration_checksum, migration_versions
from tests.integration.conftest import DB_URL
from tests.integration.isolated_database import isolated_test_database

pytestmark = [pytest.mark.integration]

Connection = psycopg.AsyncConnection[tuple[object, ...]]

_MIGRATION_DESCRIPTIONS: Final[dict[int, str]] = {
    1000: 'baseline',
    1001: 'lifecycle record',
    1002: 'workflow start command fingerprint',
    1003: 'attempt cancelled',
    1004: 'worker profile name',
    1005: 'claim generation by status',
    1006: 'typescript client family',
}
"""The description that the bookkeeping row of each migration of the chain
stores (``MIGRATION_SOURCES``, ``core/src/broker/migrations.rs``)."""

_BOOKKEEPING_TIMES_SQL: Final[LiteralString] = """
    SELECT array_agg(installed_on ORDER BY version), array_agg(execution_time ORDER BY version)
    FROM horsies_migrations
"""
"""The install time and the run time of every bookkeeping row, in version order."""

_OBJECT_COUNT_SQL: Final[LiteralString] = """
    SELECT
        (SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
         WHERE n.nspname = current_schema() AND c.relname LIKE 'horsies%%'),
        (SELECT count(*) FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
         WHERE n.nspname = current_schema() AND p.proname LIKE 'horsies%%')
"""


def mint_task_id() -> uuid.UUID:
    """A new task id from the core's UUIDv7 source, as a send mints it."""
    match core_mint_task_id():
        case uuid.UUID() as task_id:
            return task_id
        case IdMintRefused(cause=cause):
            pytest.fail(f'the core refused to mint a task id: {cause}')


@pytest_asyncio.fixture
async def database_url() -> AsyncGenerator[str]:
    """A fresh, empty database."""
    async with isolated_test_database(DB_URL) as url:
        yield url


async def _connect(url: str) -> Connection:
    return await psycopg.AsyncConnection.connect(to_psycopg_url(url), autocommit=True)


async def _initialize(
    url: str, *, schema_mode: SchemaMode = SchemaMode.MIGRATE_AND_VALIDATE,
) -> Ok[None] | Err[str]:
    """Run the install with a new broker; return the refusal text on failure."""
    broker = PostgresBroker(
        PostgresConfig(database_url=SecretStr(url)),
        schema_mode=schema_mode,
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


PUBLISHED_READER_SIGNATURES: Final = (
    'horsies_task_history_duplicate_staged(uuid)',
    'horsies_task_provenance_staged(uuid)',
    'horsies_task_detail_staged(uuid)',
    'horsies_task_result_staged(uuid)',
)
"""The readers that ``horsies_publish_history_readers`` publishes, as
``to_regprocedure`` names them."""

_MISSING_READERS_SQL: Final[LiteralString] = """
    SELECT array_agg(reader.signature ORDER BY reader.signature)
    FROM unnest(CAST(%(readers)s AS text[])) AS reader(signature)
    WHERE to_regprocedure(reader.signature) IS NULL
"""


async def _missing_readers(connection: Connection) -> object:
    cursor = await connection.execute(
        _MISSING_READERS_SQL, {'readers': list(PUBLISHED_READER_SIGNATURES)}
    )
    row = await cursor.fetchone()
    assert row is not None
    return row[0]


async def _publication_is_current(url: str) -> bool:
    """The baseline's freshness check (``horsies_history_readers_current``):
    the manifest equals the catalog and every reader is stamped."""
    engine = create_async_engine(sqlalchemy_url(url))
    try:
        async with engine.connect() as connection:
            current = (
                await connection.execute(text('SELECT horsies_history_readers_current()'))
            ).scalar_one()
    finally:
        await engine.dispose()
    assert isinstance(current, bool), current
    return current


def _chain_rows() -> list[tuple[object, ...]]:
    """The bookkeeping row of each migration of the chain: version,
    description, success, checksum."""
    rows: list[tuple[object, ...]] = []
    for version in migration_versions():
        description = _MIGRATION_DESCRIPTIONS.get(version)
        if description is None:
            pytest.fail(f'migration {version} has no description in this test')
        rows.append((version, description, True, migration_checksum(version)))
    return rows


@pytest.mark.asyncio(loop_scope='function')
async def test_fresh_database_reaches_the_newest_version(database_url: str) -> None:
    """A fresh install applies every migration of the chain, migration 1000
    first, each with its bookkeeping row."""
    await _installed(database_url)
    connection = await _connect(database_url)
    try:
        cursor = await connection.execute(
            'SELECT version, description, success, checksum FROM horsies_migrations '
            'ORDER BY version'
        )
        assert await cursor.fetchall() == _chain_rows()
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
            await _fetch_one(connection, _BOOKKEEPING_TIMES_SQL),
            await _fetch_one(connection, 'SELECT recorded_at FROM horsies_client_family'),
            await _fetch_one(connection, _OBJECT_COUNT_SQL),
        )
        await _installed(database_url)
        after = (
            await _fetch_one(connection, _BOOKKEEPING_TIMES_SQL),
            await _fetch_one(connection, 'SELECT recorded_at FROM horsies_client_family'),
            await _fetch_one(connection, _OBJECT_COUNT_SQL),
        )
    finally:
        await connection.close()
    assert after == before


@pytest.mark.asyncio(loop_scope='function')
async def test_concurrent_installs_apply_each_migration_once(database_url: str) -> None:
    results = await asyncio.gather(*(_initialize(database_url) for _ in range(3)))
    assert results == [Ok(None)] * 3
    connection = await _connect(database_url)
    try:
        assert await _fetch_one(connection, 'SELECT count(*) FROM horsies_migrations') == (
            len(migration_versions()),
        )
    finally:
        await connection.close()


@pytest.mark.asyncio(loop_scope='function')
async def test_install_returns_with_the_history_readers_published(database_url: str) -> None:
    await _installed(database_url)
    connection = await _connect(database_url)
    try:
        assert await _missing_readers(connection) is None
        # A task id that is nowhere: the detail reader answers with no row,
        # where a database without it raises undefined_function (42883).
        cursor = await connection.execute(
            "SELECT count(*) FROM horsies_task_detail_staged("
            "'0190a0b0-0000-7000-8000-000000000000'::uuid)"
        )
        assert await cursor.fetchone() == (0,)
        # The manifest holds every history leaf: the static forever leaf, and
        # the daily leaves that the install's coverage pass created.
        manifest = await connection.execute(
            'SELECT leaf_name FROM horsies_task_lookup_manifest ORDER BY leaf_name'
        )
        history_leaves = await connection.execute(
            'SELECT leaf_name FROM horsies_task_history_leaf_catalog '
            "WHERE class_key <> 'heartbeats' ORDER BY leaf_name"
        )
        manifest_leaves = await manifest.fetchall()
        assert ('horsies_task_history_forever_open',) in manifest_leaves
        assert manifest_leaves == await history_leaves.fetchall()
    finally:
        await connection.close()
    assert await _publication_is_current(database_url)


_DEFAULT_CLASS_COVERS_NOW_SQL: Final[LiteralString] = """
    SELECT count(*) FROM horsies_task_history_leaf_catalog
    WHERE class_key = 'standard_30d'
      AND lower_anchor <= now() AND now() < upper_anchor
      AND detached_at IS NULL
"""

_HISTORY_ROWS_SQL: Final[LiteralString] = """
    SELECT task_id, status::text, retention_class_key::text
    FROM horsies_task_history
    WHERE task_id = ANY(%(task_ids)s)
    ORDER BY status
"""


@pytest.mark.asyncio(loop_scope='function')
async def test_a_fresh_install_covers_a_cancel_and_an_expiry_with_no_worker(
    database_url: str,
) -> None:
    """The install ends with the startup coverage pass that the worker start
    runs: with no worker ever started, an administrative cancel (the single
    move to history) and a pending expiry (the batch move) find the leaf of
    the default class for the current UTC day. Without the pass, the cancel
    failed with 23514: no partition for the row."""
    broker = PostgresBroker(PostgresConfig(database_url=SecretStr(database_url)))
    try:
        assert await broker.ensure_schema_initialized() == Ok(None)
        connection = await _connect(database_url)
        try:
            assert await _fetch_one(
                connection, 'SELECT count(*) FROM horsies_worker_states'
            ) == (0,)
            assert await _fetch_one(connection, _DEFAULT_CLASS_COVERS_NOW_SQL) == (1,)

            cancelled = mint_task_id()
            expired = mint_task_id()
            for task_id in (cancelled, expired):
                sent = await broker.enqueue_async(
                    'install.coverage',
                    task_id=task_id,
                    send_kind=SendKind.FIRST,
                    input_json='{}',
                    sent_at=datetime.now(timezone.utc),
                )
                assert is_ok(sent), sent

            cancel = await cancel_task(broker, cancelled)
            assert is_ok(cancel), cancel
            assert cancel.ok_value.was_status is TaskStatus.PENDING

            _ = await connection.execute(
                "UPDATE horsies_tasks SET good_until = now() - interval '1 second' "
                'WHERE id = %(task_id)s',
                {'task_id': expired},
            )
            assert await broker.expire_pending_tasks() == Ok(1)

            history = await connection.execute(
                _HISTORY_ROWS_SQL, {'task_ids': [cancelled, expired]}
            )
            assert await history.fetchall() == [
                (cancelled, 'CANCELLED', 'standard_30d'),
                (expired, 'EXPIRED', 'standard_30d'),
            ]
        finally:
            await connection.close()
    finally:
        _ = await broker.close_async()


@pytest.mark.asyncio(loop_scope='function')
async def test_the_install_covers_the_declared_periods_of_the_retention_config(
    database_url: str,
) -> None:
    """A broker built with its app's retention covers each declared period
    at the install: with no worker ever started, the cancel of a task of the
    class of that period finds its leaf."""
    broker = PostgresBroker(
        PostgresConfig(database_url=SecretStr(database_url)),
        retention=RetentionConfig(retention_periods=(timedelta(days=7),)),
    )
    try:
        assert await broker.ensure_schema_initialized() == Ok(None)
        task_id = mint_task_id()
        sent = await broker.enqueue_async(
            'install.coverage',
            task_id=task_id,
            send_kind=SendKind.FIRST,
            input_json='{}',
            sent_at=datetime.now(timezone.utc),
            retention_class_key=period_class_key(RetentionDays(7)),
        )
        assert is_ok(sent), sent

        cancel = await cancel_task(broker, task_id)
        assert is_ok(cancel), cancel

        connection = await _connect(database_url)
        try:
            history = await connection.execute(_HISTORY_ROWS_SQL, {'task_ids': [task_id]})
            assert await history.fetchall() == [(task_id, 'CANCELLED', 'standard_7d')]
        finally:
            await connection.close()
    finally:
        _ = await broker.close_async()


@pytest.mark.asyncio(loop_scope='function')
async def test_an_install_that_stopped_before_the_publication_is_completed(
    database_url: str,
) -> None:
    await _installed(database_url)
    connection = await _connect(database_url)
    try:
        # The state an install leaves when it stops between the migration
        # and the publication.
        await connection.execute('DROP FUNCTION horsies_task_detail_staged(uuid)')
        await connection.execute('DROP FUNCTION horsies_task_result_staged(uuid)')
        assert await _missing_readers(connection) == [
            'horsies_task_detail_staged(uuid)',
            'horsies_task_result_staged(uuid)',
        ]
        before = await _fetch_one(connection, _BOOKKEEPING_TIMES_SQL)
        await _installed(database_url)
        assert await _missing_readers(connection) is None
        assert await _fetch_one(connection, _BOOKKEEPING_TIMES_SQL) == before
    finally:
        await connection.close()
    assert await _publication_is_current(database_url)


@pytest.mark.asyncio(loop_scope='function')
async def test_migrations_off_runs_no_statement(database_url: str) -> None:
    assert await _initialize(database_url, schema_mode=SchemaMode.OBSERVE_ONLY) == Ok(None)
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
        # A version above every migration that this build ships.
        (
            "INSERT INTO horsies_migrations VALUES (9999, 'later', now(), TRUE, sha256('x'::bytea), 1)",
            'newer than this build supports',
        ),
        (
            "INSERT INTO horsies_migrations VALUES (65, 'rust', now(), TRUE, sha256('x'::bytea), 1)",
            'versions this build does not ship',
        ),
        ("UPDATE horsies_client_family SET family = 'RUST'", 'installed by the RUST client'),
        (
            "UPDATE horsies_client_family SET family = 'TYPESCRIPT'",
            'installed by the TYPESCRIPT client',
        ),
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
async def test_a_family_row_that_holds_no_client_family_fails_closed(database_url: str) -> None:
    """A family value outside PYTHON, RUST and TYPESCRIPT is corrupt stored data.

    The test writes it past the CHECK ``horsies_client_family_family_known``,
    which it drops first. The install fails with the core's contract
    violation, which names the table, the column and the stored value, and
    changes nothing. The process does not crash.
    """
    await _installed(database_url)
    connection = await _connect(database_url)
    try:
        await connection.execute(
            'ALTER TABLE horsies_client_family '
            'DROP CONSTRAINT horsies_client_family_family_known'
        )
        await connection.execute("UPDATE horsies_client_family SET family = 'GO'")
        before = await _fetch_one(connection, _OBJECT_COUNT_SQL)
        broker = PostgresBroker(PostgresConfig(database_url=SecretStr(database_url)))
        try:
            match await broker.ensure_schema_initialized():
                case Err(
                    BrokerOperationFailed(
                        code=BrokerErrorCode.SCHEMA_INIT_FAILED,
                        retryable=False,
                        exception=CoreContractViolation() as exception,
                    )
                ):
                    assert exception.subject == 'install_schema'
                    assert 'horsies_client_family.family holds "GO"' in exception.message
                case other:
                    pytest.fail(f'expected the contract violation of the family row: {other!r}')
        finally:
            _ = await broker.close_async()
        assert await _fetch_one(connection, _OBJECT_COUNT_SQL) == before
        assert await _fetch_one(connection, 'SELECT family FROM horsies_client_family') == ('GO',)
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
    """The execution column and its CHECK, the class claim index, class_capacity, the batched success
    (with the attempt placement arguments of migration 1001)."""
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
                    'horsies_complete_tasks_fused('
                    'uuid[], timestamptz[], text[], text, boolean, text[], bigint[])'
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
