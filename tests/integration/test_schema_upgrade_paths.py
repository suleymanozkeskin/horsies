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
from sqlalchemy.exc import ProgrammingError
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
        'p_task_id character varying, p_stale_after_ms integer, '
        'p_finalizing_stale_after_ms integer, p_result text, '
        'p_error_code text, p_failed_reason text',
    ),
    (
        'horsies_expire_owned_claim',
        'p_task_id character varying, p_worker_id text, p_result text, '
        'p_error_code text',
    ),
    (
        'horsies_expire_pending_tasks',
        'p_batch_size integer, p_result text, p_error_code text',
    ),
    (
        'horsies_cancel_locked_task',
        'p_task_id character varying, p_permitted_source_statuses text[]',
    ),
    (
        'horsies_cancel_owned_orphan',
        'p_task_id character varying, p_worker_id text, '
        'p_claimed_at timestamp with time zone',
    ),
    (
        'horsies_cancel_orphaned_tasks',
        'p_batch_size integer',
    ),
    (
        'horsies_abandon_owned_node',
        'p_task_id character varying, p_worker_id text, '
        'p_claimed_at timestamp with time zone',
    ),
    (
        'horsies_abandon_owned_nodes',
        'p_ids character varying[], '
        'p_claimed_ats timestamp with time zone[], p_worker_id text',
    ),
    (
        'horsies_abandon_nodes_of_paused_workflows',
        'p_workflow_ids character varying[]',
    ),
    (
        'horsies_cancel_owned_node',
        'p_task_id character varying, p_worker_id text, '
        'p_claimed_at timestamp with time zone, '
        'p_accepts_requeued_pending boolean',
    ),
    (
        'horsies_cancel_owned_nodes',
        'p_ids character varying[], '
        'p_claimed_ats timestamp with time zone[], p_worker_id text',
    ),
    (
        'horsies_cancel_nodes_of_cancelled_workflow',
        'p_workflow_ids character varying[]',
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
    which is the only way to assert that upgrading from there works. A
    fresh-born database first demotes to the upgraded world (varchar
    identities, in-place program) so the rewind's premises hold.
    """
    from tests.integration.test_task_history_relocation import (
        demote_to_upgraded_world,
    )

    async with engine.begin() as demote_connection:
        await demote_to_upgraded_world(demote_connection)
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


async def _assert_fresh_end_state(engine: AsyncEngine) -> None:
    """A uuid-born database is born at the cutover's end state."""
    async with engine.connect() as connection:
        assert await _stored_version(engine) == SCHEMA_VERSION
        uuid_born = (
            await connection.execute(
                text(
                    "SELECT atttypid = 'uuid'::regtype FROM pg_attribute "
                    "WHERE attrelid = 'horsies_tasks'::regclass "
                    "AND attname = 'id'"
                )
            )
        ).scalar_one()
        assert bool(uuid_born)
        move_present = (
            await connection.execute(
                text(
                    "SELECT to_regproc('horsies_move_task_to_history') "
                    'IS NOT NULL'
                )
            )
        ).scalar_one()
        assert bool(move_present), 'the move family is the fresh program'
        domain_present = (
            await connection.execute(
                text(
                    'SELECT EXISTS (SELECT 1 FROM pg_constraint '
                    "WHERE conrelid = 'horsies_tasks'::regclass "
                    "AND conname = 'horsies_tasks_live_status_only')"
                )
            )
        ).scalar_one()
        assert bool(domain_present), 'live-only domain applies at birth'
        heartbeats_partitioned = (
            await connection.execute(
                text(
                    "SELECT relkind = 'p' FROM pg_class "
                    "WHERE oid = 'horsies_heartbeats'::regclass"
                )
            )
        ).scalar_one()
        assert bool(heartbeats_partitioned)


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

        postures = {
            (row.proname, row.language, row.prokind)
            for row in (
                await connection.execute(
                    text(f"""
                        SELECT p.proname, l.lanname AS language, p.prokind
                        FROM pg_proc p
                        JOIN pg_language l ON l.oid = p.prolang
                        WHERE p.prorettype = '{OUTCOME_TYPE}'::regtype
                    """)
                )
            ).all()
        }
        assert postures == {
            (name, 'plpgsql', 'f') for name in EXPECTED_FUNCTIONS
        }, 'every terminalization operation is a PL/pgSQL function'

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


async def _tasks_constraint_shape(
    engine: AsyncEngine,
) -> tuple[frozenset[str], frozenset[str]]:
    """(required columns, named constraints) on the live task table."""
    async with engine.connect() as connection:
        required = frozenset(
            row.attname
            for row in (
                await connection.execute(
                    text(
                        """
                        SELECT attname FROM pg_attribute
                        WHERE attrelid = 'horsies_tasks'::regclass
                          AND attnum > 0
                          AND NOT attisdropped
                          AND attnotnull
                        """
                    )
                )
            ).all()
        )
        constraints = frozenset(
            row.conname
            for row in (
                await connection.execute(
                    text(
                        """
                        SELECT conname FROM pg_constraint
                        WHERE conrelid = 'horsies_tasks'::regclass
                          AND contype = 'c'
                        """
                    )
                )
            ).all()
        )
    return required, constraints


class TestFreshWorldParity:
    """A uuid-born database is born at the cutover's end state.

    The arm's contract is a parity claim, and it held for three of its
    four parts by inspection alone — the status domain, the move family
    and the heartbeat shape were each asserted, and the column
    tightening, which no assertion named, was absent. This compares the
    fresh catalog against the SAME structured authority the tighten
    stage renders from, so a column added to that authority enters this
    test by construction rather than by anyone remembering.

    What it does not cover, stated rather than implied: an end-state
    part that is neither a cutover column nor one of the two named
    constraints below would need naming here too. The cutover pipeline
    suite already builds a tightened database, so a cross-database
    structural comparison could close that class from there; the
    tighten cannot be run against a fresh database to the same end,
    because it adds its constraints unconditionally and would error
    rather than report a difference.
    """

    async def test_fresh_database_carries_the_tightened_task_shape(
        self,
        scratch_database: str,
    ) -> None:
        from horsies.core.history.terminalization.live_cutover import (
            CUTOVER_COLUMNS,
        )

        await _migrate(scratch_database)
        engine = _engine(scratch_database)
        try:
            required, constraints = await _tasks_constraint_shape(engine)
        finally:
            await engine.dispose()

        # Presence half: the authority is non-empty in both halves, so
        # neither subset assertion below can pass vacuously.
        declared_required = {
            column.name for column in CUTOVER_COLUMNS if column.not_null
        }
        declared_checks = {
            f'horsies_tasks_{column.name}_cutover'
            for column in CUTOVER_COLUMNS
            if column.check is not None
        }
        assert declared_required
        assert declared_checks

        assert declared_required <= required, (
            'cutover columns that the tighten stage requires are nullable '
            f'on a fresh database: {sorted(declared_required - required)}'
        )
        assert declared_checks <= constraints, (
            'cutover column checks the tighten stage adds are absent on a '
            f'fresh database: {sorted(declared_checks - constraints)}'
        )
        assert 'horsies_tasks_rerun_lineage_pair' in constraints
        assert 'horsies_tasks_live_status_only' in constraints


class TestUpgradePaths:
    async def test_fresh_database_reaches_the_current_version(
        self,
        scratch_database: str,
    ) -> None:
        """The fresh-world characterization: a uuid-born database gets
        the move family, the live-only domain, and the partitioned
        heartbeat shape — never the in-place program."""
        await _migrate(scratch_database)
        engine = _engine(scratch_database)
        try:
            await _assert_fresh_end_state(engine)
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
            await _assert_fresh_end_state(engine)
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
        prevent. Characterized in the varchar world, where the in-place
        program is self-contained; the fresh world's body restoration
        rides the same chain arm and its program is exercised by the
        first-terminalization characterization below.
        """
        await _migrate(scratch_database)
        engine = _engine(scratch_database)
        try:
            from tests.integration.test_task_history_relocation import (
                demote_to_upgraded_world,
            )

            async with engine.begin() as demote_connection:
                await demote_to_upgraded_world(demote_connection)
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

    async def test_fresh_first_terminalization_surfaces_the_wiring_gap(
        self,
        scratch_database: str,
    ) -> None:
        """THE FINDING the fresh-world characterization exists to force.

        A fresh install carries the move family, but the move consults
        the staged provenance function and inserts into a covered leaf
        — and NOTHING on a fresh install publishes the staged readers
        or ensures leaf coverage: publication and coverage happen only
        through the partition manager, which no production wiring
        invokes. The first terminal task on a fresh fleet therefore
        fails. This test pins the CURRENT truth so the gap is a named
        fact with a ruling pending, not a surprise; the fix (startup
        wiring that registers configured retention classes, publishes
        the staged readers, and maintains create-ahead coverage) is
        its own finding-scoped work.
        """
        await _migrate(scratch_database)
        engine = _engine(scratch_database)
        try:
            task_id = await _seed_running(engine)
            # The fresh wire signature is uuid (the varchar harness
            # helper cannot even name it — itself part of the story).
            with pytest.raises(
                ProgrammingError,
                match='horsies_task_provenance_staged',
            ):
                async with engine.connect() as connection:
                    await connection.execute(
                        text(
                            'SELECT outcome FROM '
                            'horsies_complete_locked_task('
                            "CAST(:id AS uuid), 'w1', '{}')"
                        ),
                        {'id': task_id},
                    )
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
                    claimed_by_worker_id, claimed_at, started_at,
                    retention_class_key, command_fingerprint_version,
                    command_fingerprint, retain_rerun_input,
                    prepared_rerun_input_disposition
                )
                VALUES (
                    :id, 'upgrade.test', 'default', 'RUNNING', '[]', '{}',
                    repeat('0', 64), FALSE, TRUE, 'w1', NOW(), NOW(),
                    'standard_30d', 1,
                    -- A literal, not the id: this helper seeds both the
                    -- uuid-born and the rewound varchar world, and one
                    -- parameter used as both types is ambiguous to the
                    -- planner.
                    sha256(convert_to('upgrade.test', 'UTF8')),
                    FALSE, 'DECLINED_BY_POLICY'
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
