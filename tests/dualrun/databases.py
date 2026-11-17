"""Fresh ``dualrun_`` databases with the baseline schema, for the integration tests.

The server comes from ``tests.session_database.base_database_url`` (the
``HORSIES_TEST_DATABASE_URL`` override, else the local default). These
functions create and drop only databases whose name matches
``dualrun_<32 hex>``; they never change another database.
"""

from __future__ import annotations

import asyncio
import re
import uuid
from dataclasses import dataclass
from enum import Enum
from typing import Final

import psycopg
from psycopg import sql
from sqlalchemy.engine import make_url

from horsies.core.baseline.install import install_baseline
from horsies.core.baseline.migration import load_baseline_migration
from horsies.core.types.result import Err, Ok, Result
from horsies.core.utils.url import to_psycopg_url

DATABASE_PREFIX: Final = 'dualrun_'
_DATABASE_NAME: Final = re.compile(r'^dualrun_[0-9a-f]{32}$')


class SetupStep(Enum):
    """The setup step that failed."""

    CREATE = 'create the database'
    INSTALL = 'install the baseline schema'
    DROP = 'drop the database'


@dataclass(frozen=True, slots=True)
class DatabaseSetupFailed:
    """A setup step failed. After CREATE or DROP nothing is known to have changed; after INSTALL the database exists and is empty."""

    step: SetupStep
    database: str
    cause: str


@dataclass(frozen=True, slots=True)
class DualRunDatabase:
    """A database this module created."""

    name: str
    url: str
    """libpq URL of the database."""


def _url_for(base_url: str, database: str) -> str:
    rendered = (
        make_url(base_url).set(database=database).render_as_string(hide_password=False)
    )
    return to_psycopg_url(rendered)


def _checked_name(name: str) -> str:
    if _DATABASE_NAME.fullmatch(name) is None:
        raise AssertionError(
            f'refuse to touch a database this module did not create: {name!r}'
        )
    return name


def create_database(base_url: str) -> Result[DualRunDatabase, DatabaseSetupFailed]:
    """Create an empty ``dualrun_<32 hex>`` database on the server of ``base_url``."""
    name = _checked_name(f'{DATABASE_PREFIX}{uuid.uuid4().hex}')
    try:
        with psycopg.connect(_url_for(base_url, 'postgres'), autocommit=True) as admin:
            _ = admin.execute(
                sql.SQL('CREATE DATABASE {}').format(sql.Identifier(name))
            )
    except psycopg.Error as exc:
        return Err(
            DatabaseSetupFailed(step=SetupStep.CREATE, database=name, cause=str(exc))
        )
    return Ok(DualRunDatabase(name=name, url=_url_for(base_url, name)))


async def _install(url: str) -> Result[None, str]:
    migration = load_baseline_migration()
    async with await psycopg.AsyncConnection.connect(
        url, autocommit=True
    ) as connection:
        match await install_baseline(connection, migration):
            case Err(failed):
                return Err(f'{failed.step}: {failed.cause}')
            case Ok():
                return Ok(None)


def install_baseline_schema(
    database: DualRunDatabase,
) -> Result[None, DatabaseSetupFailed]:
    """Install the baseline migration with its bookkeeping and client family rows."""
    try:
        outcome = asyncio.run(_install(database.url))
    except psycopg.Error as exc:
        return Err(
            DatabaseSetupFailed(
                step=SetupStep.INSTALL, database=database.name, cause=str(exc)
            )
        )
    match outcome:
        case Err(cause):
            return Err(
                DatabaseSetupFailed(
                    step=SetupStep.INSTALL, database=database.name, cause=cause
                )
            )
        case Ok():
            return Ok(None)


def drop_database(
    base_url: str, database: DualRunDatabase
) -> Result[None, DatabaseSetupFailed]:
    """Drop a database this module created, closing its other sessions."""
    name = _checked_name(database.name)
    try:
        with psycopg.connect(_url_for(base_url, 'postgres'), autocommit=True) as admin:
            _ = admin.execute(
                sql.SQL('DROP DATABASE IF EXISTS {} WITH (FORCE)').format(
                    sql.Identifier(name)
                ),
            )
    except psycopg.Error as exc:
        return Err(
            DatabaseSetupFailed(step=SetupStep.DROP, database=name, cause=str(exc))
        )
    return Ok(None)
