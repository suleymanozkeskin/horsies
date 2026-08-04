"""Upgrading from each version the field can actually be at.

The apply path runs the full desired state and then exits early whenever the
stored version is already at or above the target. That makes the version a
watermark rather than a step counter, and it has a consequence worth testing
rather than remembering: a database that reaches a version never receives a
migration numbered below it. Anything a release adds has to be inside the
artifact that declares the version, or the deployments that skipped ahead lose
it permanently.

Each case runs against its own database, because these assertions are about
what a migration does to a schema, and a shared one would already be at the
end state.
"""

from __future__ import annotations

import uuid

import pytest
import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.models.broker import PostgresConfig
from horsies.core.schemas.migrations import SCHEMA_VERSION
from horsies.core.schemas.terminalization import OUTCOME_COLUMNS, OUTCOME_TYPE
from pydantic import SecretStr
from tests.integration.conftest import DB_URL

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

# The version this artifact's own migrations start from: the released schema a
# deployment upgrading into this change is sitting on.
PUBLISHED_VERSION = 17

# Name and exact argument list. The signature is the half that matters: a
# changed argument list without a matching drop leaves the old overload
# installed and callable, and only the full signature set can see that.
EXPECTED_SIGNATURES = {
    (
        'horsies_terminalization_miss',
        'p_task_id character varying, p_equivalent_kinds text[], '
        'p_worker_id text, p_claimed_at timestamp with time zone',
    ),
    (
        'horsies_complete_locked_task',
        'p_task_id character varying, p_worker_id text, p_result text',
    ),
    (
        'horsies_complete_task_fused',
        'p_task_id character varying, p_worker_id text, '
        'p_claimed_at timestamp with time zone, p_result text, '
        'p_notify_channel text, p_notify_payload text',
    ),
}

EXPECTED_FUNCTIONS = {name for name, _ in EXPECTED_SIGNATURES}


@pytest_asyncio.fixture
async def scratch_database() -> str:
    """A disposable database, dropped however the test ends."""
    name = f'horsies_upgrade_{uuid.uuid4().hex[:12]}'
    admin = create_async_engine(DB_URL, isolation_level='AUTOCOMMIT')
    async with admin.connect() as connection:
        await connection.execute(text(f'CREATE DATABASE {name}'))
    await admin.dispose()

    yield DB_URL.rsplit('/', 1)[0] + f'/{name}'

    admin = create_async_engine(DB_URL, isolation_level='AUTOCOMMIT')
    async with admin.connect() as connection:
        await connection.execute(
            text(f'DROP DATABASE IF EXISTS {name} WITH (FORCE)')
        )
    await admin.dispose()


async def _migrate(url: str) -> None:
    broker = PostgresBroker(PostgresConfig(database_url=SecretStr(url)))
    try:
        await broker.ensure_schema_initialized()
    finally:
        await broker.close_async()


def _engine(url: str) -> AsyncEngine:
    return create_async_engine(url)


async def _stored_version(engine: AsyncEngine) -> int:
    async with engine.connect() as connection:
        return int(
            (
                await connection.execute(
                    text('SELECT MAX(version) FROM horsies_schema_version')
                )
            ).scalar_one()
        )


async def _rewind_to_published(engine: AsyncEngine) -> None:
    """Undo this artifact's own migrations, leaving the released schema.

    Not a downgrade path — nothing in the product offers one. It reproduces
    the state a deployment is actually in when it upgrades into this change,
    which is the only way to assert that upgrading from there works.
    """
    async with engine.connect() as connection:
        await connection.execute(
            text('ALTER TABLE horsies_tasks DROP COLUMN IF EXISTS terminalization_kind')
        )
        await connection.execute(
            text("""
                ALTER TABLE horsies_tasks
                DROP CONSTRAINT IF EXISTS ck_horsies_tasks_terminal_at_terminal_only
            """)
        )
        for function in EXPECTED_FUNCTIONS:
            await connection.execute(
                text(f'DROP FUNCTION IF EXISTS {function} CASCADE')
            )
        await connection.execute(text(f'DROP TYPE IF EXISTS {OUTCOME_TYPE}'))
        await connection.execute(
            text('DELETE FROM horsies_schema_version WHERE version > :v'),
            {'v': PUBLISHED_VERSION},
        )
        await connection.execute(
            text('INSERT INTO horsies_schema_version (version) VALUES (:v) '
                 'ON CONFLICT DO NOTHING'),
            {'v': PUBLISHED_VERSION},
        )
        await connection.commit()


async def _assert_end_state(engine: AsyncEngine) -> None:
    async with engine.connect() as connection:
        assert await _stored_version(engine) == SCHEMA_VERSION

        columns = (
            await connection.execute(
                text(f"""
                    SELECT a.attname,
                           format_type(a.atttypid, a.atttypmod) AS type
                    FROM pg_attribute a
                    JOIN pg_type t ON t.typrelid = a.attrelid
                    WHERE t.typname = '{OUTCOME_TYPE}' AND a.attnum > 0
                    ORDER BY a.attnum
                """),
            )
        ).all()
        assert [(row.attname, row.type) for row in columns] == [
            (name, _pg_spelling(kind)) for name, kind in OUTCOME_COLUMNS
        ]

        installed = {
            (row.proname, row.args)
            for row in (
                await connection.execute(
                    text(f"""
                        SELECT proname,
                               pg_get_function_identity_arguments(oid) AS args
                        FROM pg_proc
                        WHERE prorettype = '{OUTCOME_TYPE}'::regtype
                    """)
                )
            ).all()
        }
        assert installed == EXPECTED_SIGNATURES, (
            'installed operations differ from the expected signatures; an '
            'overload left behind by a changed argument list appears here'
        )

        constraints = {
            row.conname
            for row in (
                await connection.execute(
                    text("""
                        SELECT conname, convalidated FROM pg_constraint
                        WHERE conrelid = 'horsies_tasks'::regclass
                          AND conname LIKE 'ck_horsies_tasks_%'
                    """)
                )
            ).all()
        }
        assert 'ck_horsies_tasks_terminalization_kind' in constraints
        assert 'ck_horsies_tasks_terminal_at_terminal_only' in constraints


def _pg_spelling(declared: str) -> str:
    return {
        'varchar': 'character varying',
        'timestamptz': 'timestamp with time zone',
        'text': 'text',
        'bigint': 'bigint',
        'jsonb': 'jsonb',
    }[declared]


class TestUpgradePaths:
    async def test_fresh_database_reaches_the_current_version(
        self,
        scratch_database: str,
    ) -> None:
        await _migrate(scratch_database)
        engine = _engine(scratch_database)
        try:
            await _assert_end_state(engine)
        finally:
            await engine.dispose()

    async def test_published_version_upgrades_and_backfills(
        self,
        scratch_database: str,
    ) -> None:
        """The case a deployment is actually in, with rows that need the backfill.

        A terminal row written before the instant column existed has no
        terminal_at, and the constraint installed in the same transaction
        would reject it — so the backfill has to run first and has to reach
        every such row.
        """
        await _migrate(scratch_database)
        engine = _engine(scratch_database)
        try:
            await _rewind_to_published(engine)
            async with engine.connect() as connection:
                await connection.execute(
                    text("""
                        INSERT INTO horsies_tasks (
                            id, task_name, queue_name, status, args, kwargs,
                            enqueue_sha, is_workflow_task, claimed,
                            completed_at, terminal_at
                        )
                        VALUES (
                            :id, 'upgrade.test', 'default', 'COMPLETED', '[]',
                            '{}', repeat('0', 64), FALSE, FALSE, NOW(), NULL
                        )
                    """),
                    {'id': str(uuid.uuid4())},
                )
                await connection.commit()

            await _migrate(scratch_database)
            await _assert_end_state(engine)

            async with engine.connect() as connection:
                undated = (
                    await connection.execute(
                        text("""
                            SELECT COUNT(*) FROM horsies_tasks
                            WHERE status IN
                                ('COMPLETED', 'FAILED', 'CANCELLED', 'EXPIRED')
                              AND terminal_at IS NULL
                        """)
                    )
                ).scalar_one()
            assert undated == 0
        finally:
            await engine.dispose()

    async def test_reapplication_changes_nothing(
        self,
        scratch_database: str,
    ) -> None:
        """A database already at this version must not be mutated by a re-run.

        Including the composite type: recreating it would give the same shape
        a new identity underneath everything that depends on it.
        """
        await _migrate(scratch_database)
        engine = _engine(scratch_database)
        try:
            async with engine.connect() as connection:
                type_identity_before = (
                    await connection.execute(
                        text(f"SELECT '{OUTCOME_TYPE}'::regtype::oid")
                    )
                ).scalar_one()

            await _migrate(scratch_database)

            async with engine.connect() as connection:
                type_identity_after = (
                    await connection.execute(
                        text(f"SELECT '{OUTCOME_TYPE}'::regtype::oid")
                    )
                ).scalar_one()
            assert type_identity_after == type_identity_before
            await _assert_end_state(engine)
        finally:
            await engine.dispose()
