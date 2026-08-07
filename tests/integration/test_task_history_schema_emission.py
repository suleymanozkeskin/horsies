"""Production emission of the history foundation and gated families.

The centerpiece is two-path shape equality: a database installed by the
FIXTURE path (frozen fragments plus every gated family, the sequence
each suite has proven since wave 2) and a database installed by the
MIGRATION path must be indistinguishable by relation schema signature.
The capture is session-independent by construction (its M12 charter),
which is what makes a cross-database comparison legitimate. Both sides
live in `public`: catalog deparse — `pg_get_indexdef` in particular —
always schema-qualifies the relation it renders, so a cross-SCHEMA
comparison would sign the schema name rather than the shape.

The presence half withholds one gated family from the fixture side and
the signatures diverge, proving the equality cannot go vacuous. Both
upgrade-ladder endpoints — a fresh install through the whole chain and
a database that had already taken the prior version — are asserted
against the same fixture shape: fresh and upgraded installs must be
indistinguishable by signature.

Each case runs against its own disposable databases.
"""

from __future__ import annotations

import uuid
from collections.abc import AsyncIterator, Callable, Coroutine
from typing import Any

import pytest
import pytest_asyncio
from pydantic import SecretStr
from sqlalchemy import text
from sqlalchemy.ext.asyncio import (
    AsyncConnection,
    create_async_engine,
)

from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.history.ddl.conditional import (
    RESERVATION_REGISTRY_EXPIRY_INDEX,
    GatedFragment,
    gated_fragment,
)
from horsies.core.history.ddl.fragments import frozen_fragments
from horsies.core.history.names import (
    KEY_RESERVATIONS,
    LEAF_CATALOG,
    LEAF_LOCK_KEY_FUNCTION,
    RETENTION_CLASSES,
    TASK_HISTORY_FOREVER,
    TASK_HISTORY_PARENT,
    TASK_LOOKUP_MANIFEST,
    TASK_LOOKUP_TYPE,
    TASK_PROVENANCE_TYPE,
    WORKFLOW_PHASE2_PENDING,
    WORKFLOW_PHASE2_QUARANTINE,
)
from horsies.core.history.transcode.signature import (
    relation_schema_signature,
)
from horsies.core.models.broker import PostgresConfig
from horsies.core.schemas.migrations import SCHEMA_VERSION
from tests.integration.conftest import DB_URL

pytestmark = [pytest.mark.integration]

ALL_FAMILIES = (
    GatedFragment.ATTEMPT_SNAPSHOT_COLUMNS,
    GatedFragment.RERUN_INPUT_COLUMNS,
    GatedFragment.RESERVATION_REGISTRY_INDEXES,
)

COMPARED_RELATIONS = (
    TASK_HISTORY_PARENT,
    TASK_HISTORY_FOREVER,
    KEY_RESERVATIONS,
)

MakeDatabase = Callable[[], Coroutine[Any, Any, str]]


@pytest_asyncio.fixture()
async def make_database() -> AsyncIterator[MakeDatabase]:
    """Mint disposable databases; all are dropped however the test ends."""
    created: list[str] = []

    async def _create() -> str:
        name = f'horsies_emission_{uuid.uuid4().hex[:12]}'
        admin = create_async_engine(DB_URL, isolation_level='AUTOCOMMIT')
        async with admin.connect() as connection:
            await connection.execute(text(f'CREATE DATABASE {name}'))
        await admin.dispose()
        created.append(name)
        return DB_URL.rsplit('/', 1)[0] + f'/{name}'

    yield _create

    admin = create_async_engine(DB_URL, isolation_level='AUTOCOMMIT')
    async with admin.connect() as connection:
        for name in created:
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


async def _install_fixture_shape(
    url: str,
    *,
    families: tuple[GatedFragment, ...] = ALL_FAMILIES,
) -> None:
    """The proven fixture sequence, into a database's public schema."""
    engine = create_async_engine(url)
    try:
        async with engine.begin() as connection:
            statements = (
                *frozen_fragments(),
                *(
                    statement
                    for family in families
                    for statement in gated_fragment(family)
                ),
            )
            for statement in statements:
                await connection.execute(text(statement))
    finally:
        await engine.dispose()


async def _signatures(
    url: str, relations: tuple[str, ...] = COMPARED_RELATIONS
) -> dict[str, str]:
    engine = create_async_engine(url)
    try:
        async with engine.connect() as connection:
            captured: dict[str, str] = {}
            for relation in relations:
                oid = (
                    await connection.execute(
                        text('SELECT CAST(:name AS regclass)::oid::bigint'),
                        {'name': f'public.{relation}'},
                    )
                ).scalar_one()
                signature = await relation_schema_signature(
                    connection, int(oid)
                )
                assert signature is not None, f'{relation}: no signature'
                captured[relation] = signature
            return captured
    finally:
        await engine.dispose()


async def _stored_version(connection: AsyncConnection) -> int:
    return int(
        (
            await connection.execute(
                text('SELECT MAX(version) FROM horsies_schema_version')
            )
        ).scalar_one()
    )


async def _assert_emitted_state(url: str) -> None:
    engine = create_async_engine(url)
    try:
        async with engine.connect() as connection:
            assert await _stored_version(connection) == SCHEMA_VERSION
            parent_partitions = (
                await connection.execute(
                    text(
                        'SELECT count(*) FROM pg_inherits '
                        'WHERE inhparent = '
                        f"'public.{TASK_HISTORY_PARENT}'::regclass"
                    )
                )
            ).scalar_one()
            # One partition: the frozen forever leaf. No daily leaves —
            # leaf creation is partition-manager runtime owned by the
            # cutover, so the near-empty parent is designed, not
            # defective.
            assert int(parent_partitions) == 1
            index_exists = (
                await connection.execute(
                    text(
                        'SELECT to_regclass('
                        f"'public.{RESERVATION_REGISTRY_EXPIRY_INDEX}') "
                        'IS NOT NULL'
                    )
                )
            ).scalar_one()
            assert bool(index_exists)
    finally:
        await engine.dispose()


async def _fixture_shape_signatures(
    make_database: MakeDatabase,
    *,
    families: tuple[GatedFragment, ...] = ALL_FAMILIES,
) -> dict[str, str]:
    url = await make_database()
    await _install_fixture_shape(url, families=families)
    return await _signatures(url)


class TestFreshInstallEndpoint:
    @pytest.mark.asyncio
    async def test_fresh_database_matches_the_fixture_shape(
        self, make_database: MakeDatabase
    ) -> None:
        migrated_url = await make_database()
        await _migrate(migrated_url)
        await _assert_emitted_state(migrated_url)
        migrated = await _signatures(migrated_url)
        fixture = await _fixture_shape_signatures(make_database)
        assert migrated == fixture, (
            'the migration path and the fixture path produced '
            'different shapes'
        )

    @pytest.mark.asyncio
    async def test_presence_half_a_withheld_family_diverges(
        self, make_database: MakeDatabase
    ) -> None:
        """Omit the rerun-input family from the fixture side: the parent
        signatures must diverge, or the equality assertion is vacuous."""
        migrated_url = await make_database()
        await _migrate(migrated_url)
        migrated = await _signatures(migrated_url, (TASK_HISTORY_PARENT,))
        fixture_url = await make_database()
        await _install_fixture_shape(
            fixture_url,
            families=(
                GatedFragment.ATTEMPT_SNAPSHOT_COLUMNS,
                GatedFragment.RESERVATION_REGISTRY_INDEXES,
            ),
        )
        fixture = await _signatures(fixture_url, (TASK_HISTORY_PARENT,))
        assert migrated[TASK_HISTORY_PARENT] != fixture[TASK_HISTORY_PARENT]


class TestUpgradedInstallEndpoint:
    @pytest.mark.asyncio
    async def test_prior_version_database_reaches_the_same_shape(
        self, make_database: MakeDatabase
    ) -> None:
        """Rewind this artifact's own emission (the state a deployment
        upgrading into this change is actually in), re-migrate, and the
        end state matches the fixture shape exactly like a fresh
        install."""
        url = await make_database()
        await _migrate(url)
        engine = create_async_engine(url)
        try:
            async with engine.begin() as connection:
                await _rewind_emission(connection)
        finally:
            await engine.dispose()
        await _migrate(url)
        await _assert_emitted_state(url)
        migrated = await _signatures(url)
        fixture = await _fixture_shape_signatures(make_database)
        assert migrated == fixture, (
            'an upgraded install and the fixture shape differ'
        )

    @pytest.mark.asyncio
    async def test_reapplication_changes_nothing(
        self, make_database: MakeDatabase
    ) -> None:
        url = await make_database()
        await _migrate(url)
        before = await _signatures(url)
        await _migrate(url)
        after = await _signatures(url)
        assert after == before
        engine = create_async_engine(url)
        try:
            async with engine.connect() as connection:
                assert await _stored_version(connection) == SCHEMA_VERSION
        finally:
            await engine.dispose()


async def _rewind_emission(connection: AsyncConnection) -> None:
    """Undo this artifact's own emission, leaving the released schema.

    Not a downgrade path — nothing in the product offers one. It
    reproduces the state a deployment is in when it upgrades into this
    change: the prior version's registry present, no history objects,
    no registry maintenance index.
    """
    statements = (
        f'DROP INDEX IF EXISTS {RESERVATION_REGISTRY_EXPIRY_INDEX}',
        f'DROP TABLE IF EXISTS {TASK_HISTORY_PARENT} CASCADE',
        f'DROP TABLE IF EXISTS {LEAF_CATALOG} CASCADE',
        f'DROP FUNCTION IF EXISTS {LEAF_LOCK_KEY_FUNCTION} CASCADE',
        f'DROP TABLE IF EXISTS {TASK_LOOKUP_MANIFEST} CASCADE',
        f'DROP TYPE IF EXISTS {TASK_LOOKUP_TYPE} CASCADE',
        f'DROP TYPE IF EXISTS {TASK_PROVENANCE_TYPE} CASCADE',
        f'DROP TABLE IF EXISTS {WORKFLOW_PHASE2_PENDING} CASCADE',
        f'DROP TABLE IF EXISTS {WORKFLOW_PHASE2_QUARANTINE} CASCADE',
        f'DROP TABLE IF EXISTS {RETENTION_CLASSES} CASCADE',
        'DROP FUNCTION IF EXISTS horsies_assert_archive_available CASCADE',
        'DROP TABLE IF EXISTS horsies_archive_maintenance_sessions CASCADE',
        'DROP TABLE IF EXISTS horsies_archive_access_gate CASCADE',
        'DELETE FROM horsies_schema_version WHERE version > 28',
        'INSERT INTO horsies_schema_version (version) VALUES (28) '
        'ON CONFLICT DO NOTHING',
    )
    for statement in statements:
        await connection.execute(text(statement))
