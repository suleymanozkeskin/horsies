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
from collections.abc import AsyncIterator

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
    (
        'horsies_fail_locked_task',
        'p_task_id character varying, p_worker_id text, p_result text, '
        'p_error_code text, p_failed_reason text',
    ),
    (
        'horsies_fail_stale_task',
        'p_task_id character varying, p_stale_after_seconds integer, '
        'p_finalizing_stale_after_seconds integer, p_result text, '
        'p_error_code text, p_failed_reason text',
    ),
}

EXPECTED_FUNCTIONS = {name for name, _ in EXPECTED_SIGNATURES}


@pytest_asyncio.fixture
async def scratch_database() -> AsyncIterator[str]:
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
            row.conname: row.convalidated
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
        assert constraints.get('ck_horsies_tasks_terminalization_kind') is True
        assert (
            constraints.get('ck_horsies_tasks_terminal_at_terminal_only') is True
        ), 'a constraint left NOT VALID proves nothing about existing rows'


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
            task_id = str(uuid.uuid4())
            async with engine.connect() as connection:
                # Eight of the writers being consolidated record neither
                # completed_at nor failed_at, so this is the row the backfill
                # has to reach through its later fallbacks. A COMPLETED row
                # with a completed_at would pass without exercising them.
                await connection.execute(
                    text("""
                        INSERT INTO horsies_tasks (
                            id, task_name, queue_name, status, args, kwargs,
                            enqueue_sha, is_workflow_task, claimed,
                            completed_at, failed_at, terminal_at,
                            created_at, updated_at
                        )
                        VALUES (
                            :id, 'upgrade.test', 'default', 'CANCELLED', '[]',
                            '{}', repeat('0', 64), FALSE, FALSE,
                            NULL, NULL, NULL,
                            TIMESTAMPTZ '2026-01-01 00:00:00+00',
                            TIMESTAMPTZ '2026-02-02 00:00:00+00'
                        )
                    """),
                    {'id': task_id},
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

            async with engine.connect() as connection:
                dated = (
                    await connection.execute(
                        text(
                            'SELECT terminal_at, updated_at FROM horsies_tasks '
                            'WHERE id = :id'
                        ),
                        {'id': task_id},
                    )
                ).one()
            # updated_at is the first fallback that has a value here, so it is
            # the instant the row must end up dated by.
            assert dated.terminal_at == dated.updated_at
        finally:
            await engine.dispose()

    async def test_reapplication_changes_nothing(
        self,
        scratch_database: str,
    ) -> None:
        """A database already at this version is left exactly as it was.

        The apply path exits early here, so this asserts that early exit is
        harmless rather than that the installation statements are idempotent —
        identity across a real installation is proven by the restoration test
        below, which lowers the watermark and runs them.
        """
        await _migrate(scratch_database)
        engine = _engine(scratch_database)
        try:
            await _migrate(scratch_database)
            await _assert_end_state(engine)
        finally:
            await engine.dispose()

    async def test_a_version_bump_restores_a_replaced_function_body(
        self,
        scratch_database: str,
    ) -> None:
        """The mechanism the versioning rule rests on, exercised directly.

        Function bodies are reinstalled on every apply, but an apply only
        happens when the stored version is behind. This replaces an installed
        body with one that answers differently, lowers the watermark, and
        migrates: the canonical behaviour must come back. If it does not, then
        a merged change to a function body would reach fresh databases only,
        which is the failure the one-version-per-change rule exists to
        prevent.
        """
        await _migrate(scratch_database)
        engine = _engine(scratch_database)
        try:
            type_identity_before = await _outcome_type_oid(engine)
            async with engine.connect() as connection:
                await connection.execute(
                    text(f"""
                        CREATE OR REPLACE FUNCTION horsies_complete_locked_task(
                            p_task_id varchar,
                            p_worker_id text,
                            p_result text
                        )
                        RETURNS SETOF {OUTCOME_TYPE}
                        LANGUAGE plpgsql
                        AS $sentinel$
                        BEGIN
                            RETURN QUERY SELECT
                                p_task_id, NULL::bigint, 'TASK_ABSENT'::text,
                                NULL::timestamptz, NULL::text,
                                NULL::text, NULL::varchar, NULL::timestamptz,
                                NULL::text, NULL::jsonb;
                        END;
                        $sentinel$
                    """)
                )
                await connection.commit()

            task_id = await _seed_running(engine)
            assert await _completion_outcome(engine, task_id) == 'TASK_ABSENT'

            async with engine.connect() as connection:
                await connection.execute(
                    text('DELETE FROM horsies_schema_version WHERE version >= :v'),
                    {'v': SCHEMA_VERSION},
                )
                await connection.execute(
                    text(
                        'INSERT INTO horsies_schema_version (version) '
                        'VALUES (:v) ON CONFLICT DO NOTHING'
                    ),
                    {'v': SCHEMA_VERSION - 1},
                )
                await connection.commit()

            await _migrate(scratch_database)

            restored = await _seed_running(engine)
            assert await _completion_outcome(engine, restored) == 'APPLIED'
            # The installation statements ran this time, so this is where the
            # create-once design is provable: recreating the type would give
            # the same shape a new identity underneath everything holding it.
            assert await _outcome_type_oid(engine) == type_identity_before
            await _assert_end_state(engine)
        finally:
            await engine.dispose()


async def _seed_running(engine: AsyncEngine) -> str:
    task_id = str(uuid.uuid4())
    async with engine.connect() as connection:
        await connection.execute(
            text("""
                INSERT INTO horsies_tasks (
                    id, task_name, queue_name, status, args, kwargs,
                    enqueue_sha, is_workflow_task, claimed,
                    claimed_by_worker_id, claimed_at, started_at
                )
                VALUES (
                    :id, 'upgrade.test', 'default', 'RUNNING', '[]', '{}',
                    repeat('0', 64), FALSE, TRUE, 'w1', NOW(), NOW()
                )
            """),
            {'id': task_id},
        )
        await connection.commit()
    return task_id


async def _completion_outcome(engine: AsyncEngine, task_id: str) -> str:
    async with engine.connect() as connection:
        outcome = (
            await connection.execute(
                text(
                    'SELECT outcome FROM horsies_complete_locked_task('
                    "CAST(:id AS VARCHAR), 'w1', '{}')"
                ),
                {'id': task_id},
            )
        ).scalar_one()
        await connection.commit()
    return str(outcome)


async def _outcome_type_oid(engine: AsyncEngine) -> int:
    async with engine.connect() as connection:
        return int(
            (
                await connection.execute(
                    text(f"SELECT '{OUTCOME_TYPE}'::regtype::oid")
                )
            ).scalar_one()
        )
