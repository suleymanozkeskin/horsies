"""The history maintenance functions of the baseline, through their SQL cases.

``history_maintenance_cases.sql`` holds the one-session cases and runs in one
transaction that is rolled back. ``history_maintenance_concurrency.sql`` holds
the two-session cases (dblink peers) and commits its fixtures. Each file runs
as one message on its own isolated database, so no case touches the shared
test database. Neither file uses stand-in readers: both publish the readers
with ``horsies_publish_history_readers`` and read through them.
"""

from __future__ import annotations

from collections.abc import AsyncGenerator
from pathlib import Path
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

_CASES_SQL: Final = Path(__file__).with_name('history_maintenance_cases.sql')
_CONCURRENCY_SQL: Final = Path(__file__).with_name('history_maintenance_concurrency.sql')

PASSED_CASES: Final = 119
"""The checks of ``history_maintenance_cases.sql``. A check added to the file changes it."""

PASSED_CONCURRENCY_CASES: Final = 40
"""The checks of ``history_maintenance_concurrency.sql``. A check added to the file changes it."""

_PASSED_PREFIX: Final = 'ok: '


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


def _collect_passed(
    connection: psycopg.AsyncConnection[tuple[object, ...]],
) -> list[str]:
    passed: list[str] = []

    def _collect(diagnostic: psycopg.errors.Diagnostic) -> None:
        message = diagnostic.message_primary or ''
        if message.startswith(_PASSED_PREFIX):
            passed.append(message.removeprefix(_PASSED_PREFIX))

    connection.add_notice_handler(_collect)
    return passed


@pytest.mark.asyncio(loop_scope='function')
async def test_history_maintenance_cases_pass(
    installed: psycopg.AsyncConnection[tuple[object, ...]],
) -> None:
    passed = _collect_passed(installed)
    # A failed case raises 'case failed: <name>', which fails this test with
    # the case name in the error.
    async with installed.transaction(force_rollback=True):
        await installed.execute(_CASES_SQL.read_text(encoding='utf-8').encode('utf-8'))
    assert len(passed) == PASSED_CASES, passed
    assert len(set(passed)) == PASSED_CASES, 'two cases share a name'


@pytest.mark.asyncio(loop_scope='function')
async def test_history_maintenance_two_session_cases_pass(
    installed: psycopg.AsyncConnection[tuple[object, ...]],
) -> None:
    passed = _collect_passed(installed)
    await installed.execute(_CONCURRENCY_SQL.read_text(encoding='utf-8').encode('utf-8'))
    assert len(passed) == PASSED_CONCURRENCY_CASES, passed
    assert len(set(passed)) == PASSED_CONCURRENCY_CASES, 'two cases share a name'


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
