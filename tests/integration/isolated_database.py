'''Isolated PostgreSQL databases for large integration fixtures.'''

from __future__ import annotations

import re
import uuid
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine, create_async_engine


_GENERATED_DATABASE = re.compile(r'^horsies_term_iso_[0-9a-f]{32}$')
_PREFIX = 'horsies_term_iso_'
_SETUP_LOCK_SQL = text(
    "SELECT pg_advisory_lock(hashtext('horsies_terminalization_database_setup'))"
)
_SETUP_UNLOCK_SQL = text(
    "SELECT pg_advisory_unlock(hashtext('horsies_terminalization_database_setup'))"
)


def _checked_database_name(name: str) -> str:
    if _GENERATED_DATABASE.fullmatch(name) is None:
        raise RuntimeError(f'refuse to drop non-generated database {name!r}')
    return name


@asynccontextmanager
async def isolated_test_database(base_url: str) -> AsyncGenerator[str]:
    '''Create one isolated database and remove it after the caller closes pools.'''
    base = make_url(base_url)
    admin_engine = create_async_engine(
        base.set(database='postgres'),
        isolation_level='AUTOCOMMIT',
    )
    name = f'{_PREFIX}{uuid.uuid4().hex}'
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
                        {'prefix': _PREFIX},
                    )
                ).scalars()
                for stale_name in stale:
                    checked = _checked_database_name(str(stale_name))
                    await admin.execute(text(f'DROP DATABASE "{checked}"'))
                await admin.execute(text(
                    f'CREATE DATABASE "{_checked_database_name(name)}"'
                ))
                created = True
                guard_engine = create_async_engine(base.set(database=name))
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
