"""The e2e test process and the subprocesses it starts use one database.

``tests/e2e/conftest.py`` binds ``DB_URL`` to the session database before any
task module is imported, and its session fixture compares every e2e source
with it. These tests check the two ends directly: a scheduler or worker
subprocess, started with the environment that the helpers give it, resolves
``DB_URL``; and the session broker of this process is connected to the
database that ``DB_URL`` names.
"""

from __future__ import annotations

import os
import subprocess
import sys
from collections.abc import Callable

import pytest
from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import AsyncSession

from horsies.core.brokers.postgres import PostgresBroker
from tests.e2e.conftest import DB_URL
from tests.e2e.helpers.env import TASK_MODULE_URL_ENV_NAME
from tests.e2e.helpers.worker import scheduler_env, worker_env

pytestmark = [pytest.mark.e2e]

SUBPROCESS_TIMEOUT_S = 30.0

RESOLVE_URL_PROGRAM = (
    'from tests.e2e.helpers.env import e2e_database_url; '
    f'print(e2e_database_url({TASK_MODULE_URL_ENV_NAME!r}))'
)
"""What a task module runs at import to choose its database."""


@pytest.mark.parametrize(
    'subprocess_environment',
    [scheduler_env, worker_env],
    ids=['scheduler', 'worker'],
)
def test_a_subprocess_resolves_the_database_url_of_this_process(
    subprocess_environment: Callable[[], dict[str, str]],
) -> None:
    result = subprocess.run(
        [sys.executable, '-c', RESOLVE_URL_PROGRAM],
        env=subprocess_environment(),
        capture_output=True,
        text=True,
        timeout=SUBPROCESS_TIMEOUT_S,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    assert result.stdout.strip() == DB_URL


@pytest.mark.skipif(
    os.environ.get('HORSIES_PGBOUNCER_TEST') == '1',
    reason='the PgBouncer lane does not initialize the session brokers',
)
@pytest.mark.asyncio(loop_scope='session')
async def test_the_session_broker_is_connected_to_the_database_of_db_url(
    broker: PostgresBroker,  # noqa: ARG001 - the session broker behind `session`
    session: AsyncSession,
) -> None:
    current_database = (
        await session.execute(text('SELECT current_database()'))
    ).scalar_one()

    assert current_database == make_url(DB_URL).database
