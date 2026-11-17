"""The miss classes of the history maintenance functions of the baseline.

The case files of these functions are Rust database tests of the core crate
(``crates/horsies-core/src/sql_tests/history_maintenance_sql.rs``). This test
stays in Python: it compares the database classes with the classes of the
Python lifecycle (``equivalence_class_of``). It runs on an isolated database,
so it does not touch the shared test database.
"""

from __future__ import annotations

from collections.abc import AsyncGenerator
from typing import Final

import psycopg
import pytest
import pytest_asyncio

from horsies.core.baseline.migration import BaselineMigration, load_baseline_migration
from horsies.core.lifecycle.operations import TerminalizationKind, equivalence_class_of
from horsies.core.utils.url import to_psycopg_url
from tests.integration.conftest import DB_URL
from tests.integration.isolated_database import isolated_test_database

pytestmark = [pytest.mark.integration]


def _migration() -> BaselineMigration:
    return load_baseline_migration()


@pytest_asyncio.fixture
async def installed() -> AsyncGenerator[psycopg.AsyncConnection[tuple[object, ...]]]:
    """An autocommit connection to a fresh database with the baseline and dblink."""
    migration = _migration()
    async with isolated_test_database(DB_URL) as url:
        connection_url = to_psycopg_url(url)
        connection = await psycopg.AsyncConnection.connect(connection_url, autocommit=True)
        try:
            # No parameters: psycopg sends each text with the simple query
            # protocol, which runs every statement of it.
            async with connection.transaction():
                await connection.execute(migration.sql.encode('utf-8'))
                await connection.execute('CREATE EXTENSION dblink')
            await connection.execute(
                "SELECT set_config('horsies_test.peer_connstr', %s, false)",
                (connection_url,),
            )
            yield connection
        finally:
            await connection.close()


_FAMILY_SQL: Final = 'SELECT horsies_terminalization_family(%s)'


@pytest.mark.asyncio(loop_scope='function')
async def test_the_miss_classes_of_the_database_are_the_equivalence_classes(
    installed: psycopg.AsyncConnection[tuple[object, ...]],
) -> None:
    """Two kinds share a database class exactly when they share a class of
    ``EQUIVALENCE_CLASSES``: the miss classifier answers ALREADY_APPLIED by
    the database class, and the Python lifecycle by its own."""
    families: dict[TerminalizationKind, str] = {}
    for kind in TerminalizationKind:
        cursor = await installed.execute(_FAMILY_SQL, (kind.value,))
        match await cursor.fetchone():
            case (str() as family,):
                families[kind] = family
            case other:
                pytest.fail(f'{kind.value} has no database class: {other!r}')
    for kind in TerminalizationKind:
        same_family = frozenset(
            other for other, family in families.items() if family == families[kind]
        )
        assert same_family == equivalence_class_of(kind), kind
