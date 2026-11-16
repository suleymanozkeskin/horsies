"""The history and retention functions of the baseline, through their SQL cases.

``history_functions_cases.sql`` holds the one-session cases and runs in one
transaction that is rolled back. ``history_functions_concurrency.sql`` holds
the two-session cases (dblink peers) and commits its fixtures. Each file runs
as one message on its own isolated database, so no case touches the shared
test database. Both files carry stand-ins for the published history readers.
"""

from __future__ import annotations

from collections.abc import AsyncGenerator
from pathlib import Path
from typing import Final

import psycopg
import pytest
import pytest_asyncio

from horsies.core.baseline.migration import BaselineMigration, load_baseline_migration
from horsies.core.types.result import Err, Ok
from horsies.core.utils.url import to_psycopg_url
from tests.integration.conftest import DB_URL
from tests.integration.isolated_database import isolated_test_database

pytestmark = [pytest.mark.integration]

_CASES_SQL: Final = Path(__file__).with_name('history_functions_cases.sql')
_CONCURRENCY_SQL: Final = Path(__file__).with_name('history_functions_concurrency.sql')

PASSED_CASES: Final = 88
"""The checks of ``history_functions_cases.sql``. A check added to the file changes it."""

PASSED_CONCURRENCY_CASES: Final = 25
"""The checks of ``history_functions_concurrency.sql``. A check added to the file changes it."""

_PASSED_PREFIX: Final = 'ok: '


def _migration() -> BaselineMigration:
    match load_baseline_migration():
        case Ok(loaded):
            return loaded
        case Err(unreadable):
            pytest.fail(f'baseline SQL unreadable: {unreadable}')


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
async def test_history_function_cases_pass(
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
async def test_history_function_two_session_cases_pass(
    installed: psycopg.AsyncConnection[tuple[object, ...]],
) -> None:
    passed = _collect_passed(installed)
    await installed.execute(_CONCURRENCY_SQL.read_text(encoding='utf-8').encode('utf-8'))
    assert len(passed) == PASSED_CONCURRENCY_CASES, passed
    assert len(set(passed)) == PASSED_CONCURRENCY_CASES, 'two cases share a name'
