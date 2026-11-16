"""The scheduler and monitoring functions of the baseline, through their SQL cases.

Two files hold the cases: ``baseline_scheduler_monitoring_cases.sql`` (the
scheduler, the stale and expired lists, the task detail, list and facets)
and ``baseline_monitoring_reads_cases.sql`` (the status counts, the
breakdown, the workflow and schedule reads). For each file this module
builds the database that the file expects (a fresh database with the
baseline, the published history readers of the forever leaf, the dblink
extension for the two-session cases) and runs the file as one message. The
database is an isolated one, so no case touches the shared test database.
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

CASE_FILES: Final = {
    'baseline_scheduler_monitoring_cases.sql': 76,
    'baseline_monitoring_reads_cases.sql': 83,
}
"""Each case file with its number of cases. A case added to a file changes it."""

def _migration() -> BaselineMigration:
    match load_baseline_migration():
        case Ok(loaded):
            return loaded
        case Err(unreadable):
            pytest.fail(f'baseline SQL unreadable: {unreadable}')


@pytest_asyncio.fixture
async def prepared() -> AsyncGenerator[psycopg.AsyncConnection[tuple[object, ...]]]:
    """An autocommit connection to a fresh database prepared for the cases."""
    migration = _migration()
    async with isolated_test_database(DB_URL) as url:
        connection_url = to_psycopg_url(url)
        connection = await psycopg.AsyncConnection.connect(
            connection_url,
            autocommit=True,
        )
        try:
            # No parameters: psycopg sends each text with the simple query
            # protocol, which runs every statement of it.
            async with connection.transaction():
                await connection.execute(migration.sql.encode('utf-8'))
                await connection.execute(
                    'SELECT outcome FROM horsies_publish_history_readers()'
                )
                await connection.execute('CREATE EXTENSION dblink')
            await connection.execute(
                "SELECT set_config('horsies_test.peer_connstr', %s, false)",
                (connection_url,),
            )
            yield connection
        finally:
            await connection.close()


@pytest.mark.asyncio(loop_scope='function')
@pytest.mark.parametrize(('case_file', 'case_count'), sorted(CASE_FILES.items()))
async def test_scheduler_and_monitoring_sql_cases_pass(
    prepared: psycopg.AsyncConnection[tuple[object, ...]],
    case_file: str,
    case_count: int,
) -> None:
    passed: list[str] = []

    def _collect(diagnostic: psycopg.errors.Diagnostic) -> None:
        message = diagnostic.message_primary or ''
        if message.startswith('ok '):
            passed.append(message.removeprefix('ok '))

    prepared.add_notice_handler(_collect)
    # A failed case raises 'CASE FAILED <name>: ...', which fails this test
    # with the case name in the error.
    cases_sql = Path(__file__).with_name(case_file).read_text(encoding='utf-8')
    await prepared.execute(cases_sql.encode('utf-8'))
    cursor = await prepared.execute('SELECT last_value FROM test_passed_cases')
    row = await cursor.fetchone()
    assert row == (case_count,), passed
    assert len(passed) == case_count
    assert len(set(passed)) == case_count, 'two cases share a name'
