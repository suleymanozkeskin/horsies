'''Isolated PostgreSQL databases for large integration fixtures.'''

from __future__ import annotations

import re
import uuid
from collections.abc import AsyncGenerator, AsyncIterator, Callable, Coroutine
from contextlib import asynccontextmanager
from typing import Any

import pytest_asyncio

from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine, create_async_engine

from tests.session_database import database_name_prefix, suite_database_url
from horsies.core.utils.url import sqlalchemy_url


_SETUP_LOCK_SQL = text(
    "SELECT pg_advisory_lock(hashtext('horsies_terminalization_database_setup'))"
)
_SETUP_UNLOCK_SQL = text(
    "SELECT pg_advisory_unlock(hashtext('horsies_terminalization_database_setup'))"
)


def isolated_database_prefix() -> str:
    '''The name prefix of isolated databases: the run's prefix and ``iso_``.'''
    return f'{database_name_prefix()}iso_'


def _checked_database_name(name: str) -> str:
    generated = re.compile(rf'^{re.escape(isolated_database_prefix())}[0-9a-f]{{32}}$')
    if generated.fullmatch(name) is None:
        raise RuntimeError(f'refuse to drop non-generated database {name!r}')
    return name


@asynccontextmanager
async def isolated_test_database(base_url: str) -> AsyncGenerator[str]:
    '''Create one isolated database and remove it after the caller closes pools.'''
    base = make_url(base_url)
    # The engines open the database with psycopg 3; the yielded URL is the
    # postgresql:// form that PostgresConfig takes.
    engine_base = make_url(sqlalchemy_url(base_url))
    admin_engine = create_async_engine(
        engine_base.set(database='postgres'),
        isolation_level='AUTOCOMMIT',
    )
    prefix = isolated_database_prefix()
    name = f'{prefix}{uuid.uuid4().hex}'
    created = False
    guard_engine: AsyncEngine | None = None
    guard_connection: AsyncConnection | None = None
    try:
        async with admin_engine.connect() as admin:
            await admin.execute(_SETUP_LOCK_SQL)
            try:
                stale = (
                    await admin.execute(
                        text('''
                            SELECT database.datname
                            FROM pg_database database
                            WHERE left(database.datname, length(:prefix)) = :prefix
                              AND NOT EXISTS (
                                  SELECT 1 FROM pg_stat_activity activity
                                  WHERE activity.datname = database.datname
                              )
                            ORDER BY database.datname
                        '''),
                        {'prefix': prefix},
                    )
                ).scalars()
                for stale_name in stale:
                    checked = _checked_database_name(str(stale_name))
                    await admin.execute(text(f'DROP DATABASE "{checked}"'))
                await admin.execute(text(
                    f'CREATE DATABASE "{_checked_database_name(name)}"'
                ))
                created = True
                guard_engine = create_async_engine(engine_base.set(database=name))
                guard_connection = await guard_engine.connect()
                await guard_connection.execute(text('SELECT 1'))
            finally:
                unlocked = (await admin.execute(_SETUP_UNLOCK_SQL)).scalar_one()
                if not bool(unlocked):
                    raise RuntimeError('isolated database setup lock was lost')

        yield base.set(database=name).render_as_string(hide_password=False)
    finally:
        try:
            if created:
                async with admin_engine.connect() as admin:
                    await admin.execute(_SETUP_LOCK_SQL)
                    try:
                        if guard_connection is not None:
                            await guard_connection.close()
                            guard_connection = None
                        if guard_engine is not None:
                            await guard_engine.dispose()
                            guard_engine = None
                        checked = _checked_database_name(name)
                        await admin.execute(text(
                            f'DROP DATABASE IF EXISTS "{checked}" WITH (FORCE)'
                        ))
                    finally:
                        unlocked = (
                            await admin.execute(_SETUP_UNLOCK_SQL)
                        ).scalar_one()
                        if not bool(unlocked):
                            raise RuntimeError(
                                'isolated database setup lock was lost'
                            )
        finally:
            if guard_connection is not None:
                await guard_connection.close()
            if guard_engine is not None:
                await guard_engine.dispose()
            await admin_engine.dispose()


MakeDatabase = Callable[[], Coroutine[Any, Any, str]]
"""Creates one empty database on the suite's server and returns its URL."""


@pytest_asyncio.fixture()
async def make_database() -> AsyncIterator[MakeDatabase]:
    """Mint disposable databases; all are dropped however the test ends."""
    server_url = suite_database_url()
    created: list[str] = []

    async def _create() -> str:
        name = f'{database_name_prefix()}disposable_{uuid.uuid4().hex[:12]}'
        admin = create_async_engine(sqlalchemy_url(server_url), isolation_level='AUTOCOMMIT')
        async with admin.connect() as connection:
            await connection.execute(text(f'CREATE DATABASE {name}'))
        await admin.dispose()
        created.append(name)
        return server_url.rsplit('/', 1)[0] + f'/{name}'

    yield _create

    admin = create_async_engine(sqlalchemy_url(server_url), isolation_level='AUTOCOMMIT')
    async with admin.connect() as connection:
        for name in created:
            await connection.execute(
                text(f'DROP DATABASE IF EXISTS {name} WITH (FORCE)')
            )
    await admin.dispose()
