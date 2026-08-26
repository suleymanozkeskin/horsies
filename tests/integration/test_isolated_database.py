'''Tests for isolated large-fixture database ownership and cleanup.'''

# pyright: reportPrivateUsage=false

from __future__ import annotations

import uuid
from contextlib import suppress

import pytest
from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import create_async_engine

from tests.integration.conftest import DB_URL
from tests.integration.isolated_database import (
    _checked_database_name,
    isolated_test_database,
)


pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


async def _database_exists(name: str) -> bool:
    admin = create_async_engine(
        make_url(DB_URL).set(database='postgres'),
        isolation_level='AUTOCOMMIT',
    )
    try:
        async with admin.connect() as connection:
            return bool((await connection.execute(
                text('SELECT EXISTS (SELECT 1 FROM pg_database WHERE datname = :name)'),
                {'name': name},
            )).scalar_one())
    finally:
        await admin.dispose()


async def test_isolated_setup_removes_an_inactive_generated_database() -> None:
    stale_name = f'horsies_term_iso_{uuid.uuid4().hex}'
    admin = create_async_engine(
        make_url(DB_URL).set(database='postgres'),
        isolation_level='AUTOCOMMIT',
    )
    try:
        async with admin.connect() as connection:
            await connection.execute(text(
                f'CREATE DATABASE "{_checked_database_name(stale_name)}"'
            ))

        async with isolated_test_database(DB_URL):
            assert not await _database_exists(stale_name)
    finally:
        async with admin.connect() as connection:
            await connection.execute(text(
                f'DROP DATABASE IF EXISTS '
                f'"{_checked_database_name(stale_name)}" WITH (FORCE)'
            ))
        await admin.dispose()


async def test_owned_isolated_drop_forces_a_remaining_session() -> None:
    lingering_engine = None
    lingering_connection = None
    owned_name = ''
    try:
        async with isolated_test_database(DB_URL) as url:
            owned_name = str(make_url(url).database)
            lingering_engine = create_async_engine(url)
            lingering_connection = await lingering_engine.connect()
            assert (await lingering_connection.execute(text('SELECT 1'))).scalar_one() == 1

        assert not await _database_exists(owned_name)
    finally:
        if lingering_connection is not None:
            with suppress(Exception):
                await lingering_connection.close()
        if lingering_engine is not None:
            await lingering_engine.dispose()


async def test_generated_database_validation_is_exact() -> None:
    valid = f'horsies_term_iso_{uuid.uuid4().hex}'
    assert _checked_database_name(valid) == valid
    for invalid in (
        'horsies_term_iso_',
        f'{valid}0',
        f'horsies_term_iso_{uuid.uuid4().hex.upper()}',
        'horsies_term_iso_../../postgres',
        'horsies_term_isolated_' + uuid.uuid4().hex,
    ):
        with pytest.raises(RuntimeError, match='refuse to drop'):
            _checked_database_name(invalid)
