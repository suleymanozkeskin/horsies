"""A second schema install leaves every history relation unchanged.

The capture hashes each relation's structural identity under the
canonical-UTC rendering pin, so two captures compare across sessions.
Each case runs against its own disposable databases; ``make_database``
is shared with other suites that need one.
"""

from __future__ import annotations

import uuid
from collections.abc import AsyncIterator, Callable, Coroutine
from typing import Any

import pytest
import pytest_asyncio
from pydantic import SecretStr
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection, create_async_engine

from horsies.core.baseline.migration import BASELINE_VERSION
from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.history.names import (
    KEY_RESERVATIONS,
    TASK_HISTORY_FOREVER,
    TASK_HISTORY_PARENT,
)
from horsies.core.history.partitions.catalog import execute_with_utc_rendering
from horsies.core.models.broker import PostgresConfig
from tests.integration.conftest import DB_URL
from tests.session_database import database_name_prefix

pytestmark = [pytest.mark.integration]

COMPARED_RELATIONS = (
    TASK_HISTORY_PARENT,
    TASK_HISTORY_FOREVER,
    KEY_RESERVATIONS,
)

MakeDatabase = Callable[[], Coroutine[Any, Any, str]]

RELATION_SCHEMA_SIGNATURE_SQL = """
SELECT encode(sha256(convert_to(
    jsonb_build_object(
        'relation', jsonb_build_array(
            relation.relkind,
            relation.relpersistence,
            relation.relam,
            relation.reloptions
        ),
        'columns', COALESCE((
            SELECT jsonb_agg(
                jsonb_build_array(
                    attribute.attnum,
                    attribute.attname,
                    attribute.atttypid,
                    attribute.atttypmod,
                    attribute.attcollation,
                    attribute.attnotnull,
                    attribute.attidentity,
                    attribute.attgenerated,
                    pg_get_expr(defaults.adbin, defaults.adrelid)
                ) ORDER BY attribute.attnum
            )
            FROM pg_attribute AS attribute
            LEFT JOIN pg_attrdef AS defaults
              ON defaults.adrelid = attribute.attrelid
             AND defaults.adnum = attribute.attnum
            WHERE attribute.attrelid = relation.oid
              AND attribute.attnum > 0
              AND NOT attribute.attisdropped
        ), '[]'::jsonb),
        'constraints', COALESCE((
            SELECT jsonb_agg(
                jsonb_build_array(
                    constraints.conname,
                    constraints.contype,
                    constraints.convalidated,
                    pg_get_constraintdef(constraints.oid, false)
                ) ORDER BY constraints.conname
            )
            FROM pg_constraint AS constraints
            WHERE constraints.conrelid = relation.oid
        ), '[]'::jsonb),
        'indexes', COALESCE((
            SELECT jsonb_agg(
                jsonb_build_array(
                    indexes.indisvalid,
                    indexes.indisready,
                    pg_get_indexdef(indexes.indexrelid)
                ) ORDER BY indexes.indexrelid
            )
            FROM pg_index AS indexes
            WHERE indexes.indrelid = relation.oid
        ), '[]'::jsonb),
        'triggers', COALESCE((
            SELECT jsonb_agg(
                jsonb_build_array(
                    triggers.tgenabled,
                    pg_get_triggerdef(triggers.oid, false)
                ) ORDER BY triggers.tgname
            )
            FROM pg_trigger AS triggers
            WHERE triggers.tgrelid = relation.oid
              AND NOT triggers.tgisinternal
        ), '[]'::jsonb)
    )::text,
    'UTF8'
)), 'hex')
FROM pg_class AS relation
WHERE relation.oid = CAST(:relation_oid AS oid)
"""


@pytest_asyncio.fixture()
async def make_database() -> AsyncIterator[MakeDatabase]:
    """Mint disposable databases; all are dropped however the test ends."""
    created: list[str] = []

    async def _create() -> str:
        name = f'{database_name_prefix()}emission_{uuid.uuid4().hex[:12]}'
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


async def _relation_schema_signature(
    connection: AsyncConnection,
    relation_oid: int,
) -> str:
    """The relation's structural hash, rendered under the UTC pin."""
    result = await execute_with_utc_rendering(
        connection,
        text(RELATION_SCHEMA_SIGNATURE_SQL),
        {'relation_oid': relation_oid},
    )
    value = result.scalar_one()
    assert isinstance(value, str), 'schema signature did not decode as text'
    return value


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
                captured[relation] = await _relation_schema_signature(
                    connection, int(oid)
                )
            return captured
    finally:
        await engine.dispose()


async def _stored_version(connection: AsyncConnection) -> int:
    return int(
        (
            await connection.execute(
                text('SELECT MAX(version) FROM horsies_migrations')
            )
        ).scalar_one()
    )


class TestUpgradedInstallEndpoint:
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
                assert await _stored_version(connection) == BASELINE_VERSION
        finally:
            await engine.dispose()
