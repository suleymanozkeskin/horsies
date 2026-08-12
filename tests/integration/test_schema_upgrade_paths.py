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
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine, create_async_engine

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
        initialized = await broker.ensure_schema_initialized()
        assert initialized.is_ok(), initialized
    finally:
        await broker.close_async()


async def _migrate_pre_cutover(url: str) -> None:
    """Apply migrations to a legacy identity shape and require refusal."""
    broker = PostgresBroker(PostgresConfig(database_url=SecretStr(url)))
    try:
        initialized = await broker.ensure_schema_initialized()
        assert initialized.is_err()
        assert (
            'offline task-history cutover is incomplete'
            in initialized.unwrap_err().message
        )
    finally:
        await broker.close_async()


def _engine(url: str) -> AsyncEngine:
    return create_async_engine(url)


async def _restore_v34_forever_shape(connection: AsyncConnection) -> None:
    """Replace the v35 RANGE parent with v34's unbounded LIST leaf."""
    from horsies.core.history.names import (
        LEAF_CATALOG,
        TASK_HISTORY_FOREVER,
        TASK_HISTORY_PARENT,
    )

    await connection.execute(
        text(f"DELETE FROM {LEAF_CATALOG} WHERE class_key = 'forever'")
    )
    await connection.execute(
        text(
            f'ALTER TABLE {TASK_HISTORY_PARENT} '
            f'DETACH PARTITION {TASK_HISTORY_FOREVER}'
        )
    )
    await connection.execute(text(f'DROP TABLE {TASK_HISTORY_FOREVER} CASCADE'))
    await connection.execute(
        text(
            f'CREATE TABLE {TASK_HISTORY_FOREVER} '
            f'PARTITION OF {TASK_HISTORY_PARENT} '
            "FOR VALUES IN ('forever')"
        )
    )
    await connection.execute(
        text(
            f'CREATE INDEX {TASK_HISTORY_FOREVER}_task_idx '
            f'ON {TASK_HISTORY_FOREVER} (task_id)'
        )
    )
    await connection.execute(
        text(
            f'CREATE INDEX {TASK_HISTORY_FOREVER}_enqueued_idx '
            f'ON {TASK_HISTORY_FOREVER} (enqueued_at)'
        )
    )


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
        from horsies.core.history.partitions.catalog import (
            read_leaf_ordering_index_exists,
        )
        from horsies.core.history.cutover.state import cutover_complete
        from tests.integration.task_history_harness import (
            current_forever_leaf,
        )

        forever_leaf = await current_forever_leaf(connection)
        assert await read_leaf_ordering_index_exists(
            connection, forever_leaf
        ), 'the current forever leaf is born with the enqueue-order index'
        assert await cutover_complete(connection)


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

    async def test_fresh_database_carries_the_locator_contract(
        self,
        scratch_database: str,
    ) -> None:
        """The composite key the phase-2 outbox references.

        Named here because it is an end-state part that is not a cutover
        column — the class the docstring above says would need naming.
        Without it a fresh install's outbox has no referential tie to
        the node, and a deleted workflow leaves pending rows nothing can
        resolve.
        """
        await _migrate(scratch_database)
        engine = _engine(scratch_database)
        try:
            async with engine.connect() as connection:
                names = frozenset(
                    row.conname
                    for row in (
                        await connection.execute(
                            text(
                                """
                                SELECT conname FROM pg_constraint
                                WHERE conname IN (
                                    'horsies_workflow_tasks'
                                    '_node_workflow_key',
                                    'horsies_workflow_phase2_pending'
                                    '_node_fkey'
                                )
                                """
                            )
                        )
                    ).all()
                )
        finally:
            await engine.dispose()
        assert names == {
            'horsies_workflow_tasks_node_workflow_key',
            'horsies_workflow_phase2_pending_node_fkey',
        }, sorted(names)

    async def test_fresh_locator_key_deletes_on_cascade(
        self,
        scratch_database: str,
    ) -> None:
        """Deleting a workflow takes its unconsumed evidence with it.

        The delete ACTION is the pin, not the constraint's presence: a
        constraint that is already there is never rebuilt by adding it,
        so a database born before the cascade would keep the refusing
        form and no existence check would notice.
        """
        await _migrate(scratch_database)
        engine = _engine(scratch_database)
        try:
            async with engine.connect() as connection:
                action = (
                    await connection.execute(
                        text(
                            """
                            SELECT confdeltype FROM pg_constraint
                            WHERE conname =
                                'horsies_workflow_phase2_pending_node_fkey'
                            """
                        )
                    )
                ).scalar_one()
        finally:
            await engine.dispose()
        assert action == 'c', f'expected cascade, catalog says {action!r}'


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

            await _migrate_pre_cutover(scratch_database)
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

    async def test_current_version_with_only_legacy_marker_refuses_startup(
        self,
        scratch_database: str,
    ) -> None:
        """The pre-validation marker cannot authorize the post-cutover fleet."""
        from horsies.core.history.cutover.state import (
            CUTOVER_STATE_TABLE,
            LEGACY_CUTOVER_NAME,
        )

        await _migrate(scratch_database)
        engine = _engine(scratch_database)
        try:
            async with engine.begin() as connection:
                await connection.execute(text(f'DELETE FROM {CUTOVER_STATE_TABLE}'))
                await connection.execute(
                    text(
                        f'INSERT INTO {CUTOVER_STATE_TABLE} (cutover_name) '
                        'VALUES (:cutover_name)'
                    ),
                    {'cutover_name': LEGACY_CUTOVER_NAME},
                )
        finally:
            await engine.dispose()

        broker = PostgresBroker(
            PostgresConfig(database_url=SecretStr(scratch_database))
        )
        try:
            initialized = await broker.ensure_schema_initialized()
            assert initialized.is_err()
            error = initialized.unwrap_err()
            assert 'offline task-history cutover is incomplete' in error.message
            from horsies.web.schema import SchemaProbe, SchemaState

            status = await SchemaProbe(broker, ttl_seconds=0).status()
            assert status.state is SchemaState.CUTOVER_REQUIRED
            assert status.compatible is False
        finally:
            await broker.close_async()

    async def test_failed_validation_revokes_the_startup_attestation(
        self,
        scratch_database: str,
    ) -> None:
        from horsies.core.history.cutover.state import cutover_complete
        from horsies.core.history.cutover.validation import (
            CutoverInvalid,
            validate_cutover,
        )
        from horsies.core.history.names import LEAF_CATALOG

        await _migrate(scratch_database)
        engine = _engine(scratch_database)
        try:
            async with engine.begin() as connection:
                assert await cutover_complete(connection) is True
                removed = (
                    await connection.execute(
                        text(
                            f'DELETE FROM {LEAF_CATALOG} '
                            "WHERE class_key = 'forever' "
                            'AND detached_at IS NULL '
                            'AND dropped_at IS NULL '
                            'RETURNING leaf_name'
                        )
                    )
                ).first()
                assert removed is not None
                validation = await validate_cutover(connection)
                assert isinstance(validation, CutoverInvalid)
                assert any(
                    'forever history leaves are absent' in violation
                    for violation in validation.violations
                )
                assert await cutover_complete(connection) is False
        finally:
            await engine.dispose()

        broker = PostgresBroker(
            PostgresConfig(database_url=SecretStr(scratch_database))
        )
        try:
            initialized = await broker.ensure_schema_initialized()
            assert initialized.is_err()
            assert (
                'offline task-history cutover is incomplete'
                in initialized.unwrap_err().message
            )
        finally:
            await broker.close_async()

    async def test_v34_forever_leaf_converts_without_rewriting_old_rows(
        self,
        scratch_database: str,
    ) -> None:
        """Schema v35 bounds the old forever population and moves only today."""
        from datetime import datetime, timedelta, timezone

        from horsies.core.history.cutover.state import CUTOVER_STATE_TABLE
        from horsies.core.history.names import (
            TASK_HISTORY_FOREVER,
            TASK_HISTORY_PARENT,
        )
        from horsies.core.history.partitions.forever import FOREVER_LEGACY_LEAF
        from tests.integration.task_history_harness import (
            INSERT_HISTORY_ROW_SQL,
            current_forever_leaf,
            frozen_history_row,
        )

        await _migrate(scratch_database)
        engine = _engine(scratch_database)
        old_id = str(uuid.uuid4())
        current_id = str(uuid.uuid4())
        today = datetime.now(timezone.utc).replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        old_anchor = today - timedelta(days=30)
        current_anchor = today + timedelta(hours=1)
        try:
            async with engine.begin() as connection:
                await _restore_v34_forever_shape(connection)
                archive_defaults = {
                    'attempt_archive_version': '1',
                    'attempt_snapshot_codec': "'json-utf8'",
                    'attempt_snapshot_content_type': "'application/json'",
                    'attempt_snapshot': "convert_to('[]', 'UTF8')",
                    'attempt_snapshot_digest': (
                        "sha256(convert_to('[]', 'UTF8'))"
                    ),
                    'rerun_input_disposition': "'NEVER_ELIGIBLE'",
                }
                for column, expression in archive_defaults.items():
                    await connection.execute(
                        text(
                            f'ALTER TABLE {TASK_HISTORY_PARENT} '
                            f'ALTER COLUMN {column} SET DEFAULT {expression}'
                        )
                    )
                for task_id, anchor in (
                    (old_id, old_anchor),
                    (current_id, current_anchor),
                ):
                    await connection.execute(
                        text(INSERT_HISTORY_ROW_SQL),
                        frozen_history_row(
                            task_id=task_id,
                            class_key='forever',
                            terminal_at=anchor,
                        ),
                    )
                for column in archive_defaults:
                    await connection.execute(
                        text(
                            f'ALTER TABLE {TASK_HISTORY_PARENT} '
                            f'ALTER COLUMN {column} DROP DEFAULT'
                        )
                    )
                await connection.execute(text(f'DELETE FROM {CUTOVER_STATE_TABLE}'))
                await connection.execute(
                    text('DELETE FROM horsies_schema_version WHERE version > 34')
                )
                await connection.execute(
                    text(
                        'INSERT INTO horsies_schema_version (version) VALUES (34) '
                        'ON CONFLICT DO NOTHING'
                    )
                )

            await _migrate(scratch_database)

            async with engine.connect() as connection:
                relkind = (
                    await connection.execute(
                        text(
                            "SELECT relkind FROM pg_class "
                            'WHERE oid = CAST(:relation AS regclass)'
                        ),
                        {'relation': TASK_HISTORY_FOREVER},
                    )
                ).scalar_one()
                assert relkind == 'p'
                current_leaf = await current_forever_leaf(connection)
                locations = {
                    str(row.task_id): str(row.relation)
                    for row in (
                        await connection.execute(
                            text(
                                f'SELECT task_id, tableoid::regclass AS relation '
                                f'FROM {TASK_HISTORY_PARENT} '
                                'WHERE task_id IN ('
                                'CAST(:old_id AS uuid), CAST(:current_id AS uuid))'
                            ),
                            {'old_id': old_id, 'current_id': current_id},
                        )
                    ).all()
                }
                assert locations == {
                    old_id: FOREVER_LEGACY_LEAF,
                    current_id: current_leaf,
                }
                plan = '\n'.join(
                    str(row[0])
                    for row in (
                        await connection.execute(
                            text(
                                f'EXPLAIN SELECT count(*) FROM '
                                f'{TASK_HISTORY_PARENT} '
                                "WHERE retention_class_key = 'forever' "
                                'AND retention_anchor_at >= :today'
                            ),
                            {'today': today},
                        )
                    ).all()
                )
                assert FOREVER_LEGACY_LEAF not in plan
        finally:
            await engine.dispose()

    async def test_v34_conversion_and_classless_relocation_compose(
        self,
        scratch_database: str,
    ) -> None:
        from datetime import datetime, timedelta, timezone

        from horsies.core.history.cutover.identity import (
            normalize_attempt_identity,
        )
        from horsies.core.history.cutover.program import install_programs
        from horsies.core.history.cutover.state import CUTOVER_STATE_TABLE
        from horsies.core.history.cutover.relocation import RelocationComplete
        from horsies.core.history.partitions.forever import FOREVER_LEGACY_LEAF
        from tests.integration.test_task_history_preparation import (
            run_preparation_to_complete,
        )
        from tests.integration.test_task_history_relocation import (
            demote_to_upgraded_world,
            insert_legacy_task,
            relocate_all,
        )

        await _migrate(scratch_database)
        engine = _engine(scratch_database)
        old_anchor = datetime.now(timezone.utc) - timedelta(days=30)
        try:
            async with engine.begin() as connection:
                await demote_to_upgraded_world(connection)
                await _restore_v34_forever_shape(connection)
                task_id = await insert_legacy_task(
                    connection,
                    status='COMPLETED',
                    kind=None,
                    class_key=None,
                    disposition=None,
                    retain=None,
                    fingerprinted=False,
                )
                await connection.execute(
                    text(
                        'UPDATE horsies_tasks SET terminal_at = :terminal_at '
                        'WHERE id = CAST(:task_id AS varchar)'
                    ),
                    {'terminal_at': old_anchor, 'task_id': task_id},
                )
                await connection.execute(
                    text(f'DELETE FROM {CUTOVER_STATE_TABLE}')
                )
                await connection.execute(
                    text(
                        'DELETE FROM horsies_schema_version WHERE version > 34'
                    )
                )
                await connection.execute(
                    text(
                        'INSERT INTO horsies_schema_version (version) '
                        'VALUES (34) ON CONFLICT DO NOTHING'
                    )
                )

            await _migrate_pre_cutover(scratch_database)

            async with engine.begin() as connection:
                await normalize_attempt_identity(connection)
                installed = await install_programs(connection)
                assert isinstance(installed, int), installed
                await run_preparation_to_complete(
                    connection, retain_default=True
                )
                relocated = await relocate_all(connection)
                assert isinstance(relocated, RelocationComplete)
                assert relocated.rows_relocated == 1
                relation = (
                    await connection.execute(
                        text(
                            'SELECT tableoid::regclass::text '
                            'FROM horsies_task_history '
                            'WHERE task_id = CAST(:task_id AS uuid)'
                        ),
                        {'task_id': task_id},
                    )
                ).scalar_one()
                assert relation == FOREVER_LEGACY_LEAF
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

            await _migrate_pre_cutover(scratch_database)

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


class TestOrderingIndexMigration:
    """Existing task-history leaves gain the enqueue-order index.

    The walk enumerates the live partition tree and guards on the
    PROPERTY — a non-partial single-key btree on enqueued_at — so a
    leaf whose index was created under another name is left alone and
    a leaf missing the composition gains it, regardless of who created
    the leaf. Fresh leaves are born with the index; these cases are the
    already-deployed ones.
    """

    ORDERING_CLASS = 'upgrade_ordering'

    async def _current_forever_leaf(self, engine: AsyncEngine) -> str:
        from tests.integration.task_history_harness import (
            current_forever_leaf,
        )

        async with engine.connect() as connection:
            return await current_forever_leaf(connection)

    async def _make_finite_leaf(self, engine: AsyncEngine) -> str:
        """One finite daily leaf on a migrated database; returns its name."""
        from datetime import datetime, timedelta, timezone

        from horsies.core.history.commands import (
            CreateDailyHistoryLeaf,
            LeafBounds,
            LeafRef,
        )
        from horsies.core.history.ddl.classes import (
            ClassRegistered,
            register_finite_retention_class,
        )
        from horsies.core.history.outcomes import LeafCreated
        from horsies.core.history.partitions.catalog import daily_leaf_name
        from horsies.core.history.partitions.manager import create_daily_leaf
        from horsies.core.history.partitions.publication import (
            UnpublishedLoader,
        )

        lower = datetime.now(timezone.utc).replace(
            hour=0, minute=0, second=0, microsecond=0
        )
        async with engine.begin() as connection:
            registered = await register_finite_retention_class(
                connection,
                class_key=self.ORDERING_CLASS,
                duration=timedelta(days=30),
            )
            assert isinstance(registered, ClassRegistered)
            parent_name = registered.finite_parent_name
            leaf = LeafRef(
                leaf_name=daily_leaf_name(parent_name, lower),
                class_key=self.ORDERING_CLASS,
                bounds=LeafBounds(
                    lower=lower, upper=lower + timedelta(days=1)
                ),
            )
            outcome = await create_daily_leaf(
                connection,
                CreateDailyHistoryLeaf(leaf=leaf),
                UnpublishedLoader(),
            )
            assert isinstance(outcome, LeafCreated)
        return leaf.leaf_name

    async def _rewind_watermark(
        self, engine: AsyncEngine, version: int
    ) -> None:
        async with engine.connect() as connection:
            await connection.execute(
                text('DELETE FROM horsies_schema_version WHERE version > :v'),
                {'v': version},
            )
            await connection.execute(
                text(
                    'INSERT INTO horsies_schema_version (version) '
                    'VALUES (:v) ON CONFLICT DO NOTHING'
                ),
                {'v': version},
            )
            await connection.commit()

    async def _ordering_index_count(
        self, engine: AsyncEngine, leaf_name: str
    ) -> int:
        """Indexes on the leaf matching the property, by composition."""
        async with engine.connect() as connection:
            return int(
                (
                    await connection.execute(
                        text(
                            """
                            SELECT count(*)
                            FROM pg_index AS i
                            JOIN pg_class AS ic ON ic.oid = i.indexrelid
                            JOIN pg_am AS am ON am.oid = ic.relam
                            WHERE i.indrelid = CAST(:leaf AS regclass)
                              AND am.amname = 'btree'
                              AND i.indpred IS NULL
                              AND i.indnkeyatts = 1
                              AND i.indkey[0] = (
                                  SELECT a.attnum FROM pg_attribute AS a
                                  WHERE a.attrelid = CAST(:leaf AS regclass)
                                    AND a.attname = 'enqueued_at'
                              )
                            """
                        ),
                        {'leaf': leaf_name},
                    )
                ).scalar_one()
            )

    async def test_leaves_without_the_ordering_index_gain_it(
        self,
        scratch_database: str,
    ) -> None:
        """A database whose leaves predate the index is repaired by the walk.

        Covers both nested leaf shapes — a forever daily leaf and a finite
        daily leaf — because the walk must reach leaves below both RANGE
        sub-parents, not only the LIST parent's direct children.
        """
        from horsies.core.history.partitions.catalog import (
            leaf_enqueued_index_name,
        )

        await _migrate(scratch_database)
        engine = _engine(scratch_database)
        try:
            finite_leaf = await self._make_finite_leaf(engine)
            forever_leaf = await self._current_forever_leaf(engine)
            async with engine.connect() as connection:
                for leaf_name in (forever_leaf, finite_leaf):
                    await connection.execute(
                        text(
                            'DROP INDEX '
                            f'{leaf_enqueued_index_name(leaf_name)}'
                        )
                    )
                await connection.commit()
            assert (
                await self._ordering_index_count(engine, forever_leaf)
                == 0
            )
            assert await self._ordering_index_count(engine, finite_leaf) == 0

            await self._rewind_watermark(engine, 33)
            await _migrate(scratch_database)

            assert (
                await self._ordering_index_count(engine, forever_leaf)
                == 1
            )
            assert await self._ordering_index_count(engine, finite_leaf) == 1
        finally:
            await engine.dispose()

    async def test_conformant_leaves_are_not_given_a_second_index(
        self,
        scratch_database: str,
    ) -> None:
        """Re-running the walk against present indexes creates nothing.

        The guard is the property probe, so this is the idempotence
        that matters: watermark lowered, statements re-run, exactly one
        matching index per leaf afterwards.
        """
        await _migrate(scratch_database)
        engine = _engine(scratch_database)
        try:
            finite_leaf = await self._make_finite_leaf(engine)
            forever_leaf = await self._current_forever_leaf(engine)
            await self._rewind_watermark(engine, 33)
            await _migrate(scratch_database)

            assert (
                await self._ordering_index_count(engine, forever_leaf)
                == 1
            )
            assert await self._ordering_index_count(engine, finite_leaf) == 1
        finally:
            await engine.dispose()
