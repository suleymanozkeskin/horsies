"""The lifecycle functions of the baseline (0004_lifecycle_functions.sql) on a fresh PostgreSQL database.

Each test builds its own database (``isolated_test_database``), so no test
here touches the shared test database. The single-session cases are the SQL
script ``baseline_lifecycle_functions.sql``; the two-session cases are in
``baseline_lifecycle_sessions``.
"""

from __future__ import annotations

from collections.abc import AsyncGenerator, Awaitable, Callable
from pathlib import Path

import psycopg
import pytest
import pytest_asyncio

from horsies.core.baseline.migration import BaselineMigration, load_baseline_migration
from horsies.core.utils.url import to_psycopg_url
from tests.integration import baseline_lifecycle_sessions as sessions
from tests.integration.conftest import DB_URL
from tests.integration.isolated_database import isolated_test_database

pytestmark = [pytest.mark.integration]

_CASES_SQL = Path(__file__).with_name('baseline_lifecycle_functions.sql')


def _migration() -> BaselineMigration:
    return load_baseline_migration()


@pytest_asyncio.fixture
async def baseline_dsn() -> AsyncGenerator[str]:
    """The connection string of a fresh database that holds the baseline migration."""
    migration = _migration()
    async with isolated_test_database(DB_URL) as url:
        dsn = to_psycopg_url(url)
        connection = await psycopg.AsyncConnection.connect(dsn)
        try:
            # No parameters: psycopg sends the text with the simple query
            # protocol, which runs every statement of it.
            async with connection.transaction():
                await connection.execute(migration.sql.encode('utf-8'))
        finally:
            await connection.close()
        yield dsn


@pytest.mark.asyncio(loop_scope='function')
async def test_single_session_cases(baseline_dsn: str) -> None:
    cases = _CASES_SQL.read_text(encoding='utf-8')
    connection = await psycopg.AsyncConnection.connect(baseline_dsn)
    try:
        async with connection.transaction(force_rollback=True):
            await connection.execute(cases.encode('utf-8'))
    finally:
        await connection.close()


@pytest.mark.asyncio(loop_scope='function')
@pytest.mark.parametrize('case', sessions.CASES, ids=lambda case: case.__name__)
async def test_two_session_case(
    baseline_dsn: str,
    case: Callable[[str], Awaitable[None]],
) -> None:
    await sessions.prepare(baseline_dsn)
    await case(baseline_dsn)
