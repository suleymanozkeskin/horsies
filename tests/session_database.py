"""One fresh PostgreSQL database per test session.

The install refuses a database that holds a horsies schema other than this
build's baseline, so the database suites cannot use a shared, long-lived
database. ``session_database_url`` creates a database whose name matches
``horsies_test_<32 hex>`` on the server of the base URL, once per process,
and drops it when the process exits. It never drops or changes a database
whose name does not match that pattern; it drops stale matching databases
that no session is connected to.

The created URL is written to ``HORSIES_TEST_DATABASE_URL`` and
``HORSES_E2E_DB_URL``, so worker subprocesses and the helpers that read
those variables use the same database.
"""

from __future__ import annotations

import atexit
import os
import re
import uuid
from collections.abc import Sequence
from pathlib import Path
from typing import Final

import psycopg
from psycopg import sql
from sqlalchemy.engine import make_url

from horsies.core.utils.url import to_psycopg_url

SESSION_DATABASE_PREFIX: Final = 'horsies_test_'
_SESSION_DATABASE_NAME: Final = re.compile(r'^horsies_test_[0-9a-f]{32}$')
_SETUP_LOCK_KEY: Final = 'horsies_test_session_database_setup'
_URL_VARIABLES: Final = ('HORSIES_TEST_DATABASE_URL', 'HORSES_E2E_DB_URL')

_session_url: str | None = None
_guard: psycopg.Connection[tuple[object, ...]] | None = None


def base_database_url() -> str:
    """The configured server URL: an override if set, else the local default."""
    for name in _URL_VARIABLES:
        value = os.environ.get(name)
        if value:
            return value
    password = os.environ.get('DB_PASSWORD', '')
    return f'postgresql+psycopg://postgres:{password}@localhost:5432/horsies'


def _checked_name(name: str) -> str:
    if _SESSION_DATABASE_NAME.fullmatch(name) is None:
        raise RuntimeError(f'refuse to drop a database the test session did not create: {name!r}')
    return name


def _admin_connection(base_url: str) -> psycopg.Connection[tuple[object, ...]]:
    admin_url = make_url(base_url).set(database='postgres').render_as_string(hide_password=False)
    return psycopg.connect(to_psycopg_url(admin_url), autocommit=True)


def _create(base_url: str) -> tuple[str, psycopg.Connection[tuple[object, ...]]]:
    name = f'{SESSION_DATABASE_PREFIX}{uuid.uuid4().hex}'
    url = make_url(base_url).set(database=name).render_as_string(hide_password=False)
    with _admin_connection(base_url) as admin:
        admin.execute('SELECT pg_advisory_lock(hashtext(%s))', (_SETUP_LOCK_KEY,))
        try:
            stale = admin.execute(
                """
                SELECT d.datname FROM pg_database d
                WHERE left(d.datname, length(%s)) = %s
                  AND NOT EXISTS (SELECT 1 FROM pg_stat_activity a WHERE a.datname = d.datname)
                ORDER BY d.datname
                """,
                (SESSION_DATABASE_PREFIX, SESSION_DATABASE_PREFIX),
            ).fetchall()
            for (stale_name,) in stale:
                if _SESSION_DATABASE_NAME.fullmatch(str(stale_name)) is not None:
                    admin.execute(
                        sql.SQL('DROP DATABASE IF EXISTS {}').format(sql.Identifier(str(stale_name)))
                    )
            admin.execute(sql.SQL('CREATE DATABASE {}').format(sql.Identifier(_checked_name(name))))
            # The guard connection marks the database as in use, so another
            # session's stale-database sweep leaves it alone.
            guard = psycopg.connect(to_psycopg_url(url), autocommit=True)
        finally:
            admin.execute('SELECT pg_advisory_unlock(hashtext(%s))', (_SETUP_LOCK_KEY,))
    return url, guard


def _drop_at_exit(base_url: str, url: str) -> None:
    global _guard
    if _guard is not None:
        _guard.close()
        _guard = None
    name = _checked_name(str(make_url(url).database))
    with _admin_connection(base_url) as admin:
        admin.execute(
            sql.SQL('DROP DATABASE IF EXISTS {} WITH (FORCE)').format(sql.Identifier(name))
        )


def session_database_url() -> str:
    """The URL of this process's test database, created on the first call."""
    global _session_url, _guard
    if _session_url is not None:
        return _session_url
    base_url = base_database_url()
    url, guard = _create(base_url)
    _session_url = url
    _guard = guard
    for name in _URL_VARIABLES:
        os.environ[name] = url
    atexit.register(_drop_at_exit, base_url, url)
    return url


def suite_database_url() -> str:
    """The default-topology database URL for the conftest modules.

    The PgBouncer lane (``HORSIES_PGBOUNCER_TEST=1``) has no PostgreSQL
    service at the default URL; its tests use their own URLs to the PgBouncer
    fixture stack. There no session database is created and the base URL is
    returned unchanged.
    """
    match os.environ.get('HORSIES_PGBOUNCER_TEST'):
        case '1':
            return base_database_url()
        case _:
            return session_database_url()


_DATABASE_SUITES: Final = ('integration', 'e2e')
"""Folders under tests/ whose tests need the session database."""


def selects_database_suites(args: Sequence[str], invocation_dir: Path) -> bool:
    """Whether a pytest run names a path in, or above, a database suite folder.

    A node id (``path::name``) counts by its path. ``tests/unit`` and its
    files do not select a database suite.
    """
    tests_dir = Path(__file__).resolve().parent
    suites = tuple(tests_dir / name for name in _DATABASE_SUITES)
    for arg in args:
        path = (invocation_dir / arg.split('::', 1)[0]).resolve()
        for suite in suites:
            if path == suite or suite in path.parents or path in suite.parents:
                return True
    return False
