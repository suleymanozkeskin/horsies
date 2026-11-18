"""Fresh ``dualrun_`` databases with the baseline schema, for the integration tests.

The server comes from ``tests.session_database.base_database_url`` (the
``HORSIES_TEST_DATABASE_URL`` override, else the local default). These
functions create and drop only databases whose name matches
``dualrun_<32 hex>``; they never change another database.
"""

from __future__ import annotations

import hashlib
import re
import time
import uuid
from dataclasses import dataclass
from enum import Enum
from typing import Final, LiteralString

import psycopg
from psycopg import sql
from sqlalchemy.engine import make_url

import horsies._core as _core
from horsies.core.types.result import Err, Ok, Result
from horsies.core.utils.url import to_psycopg_url

DATABASE_PREFIX: Final = 'dualrun_'
_DATABASE_NAME: Final = re.compile(r'^dualrun_[0-9a-f]{32}$')

BASELINE_VERSION: Final = 1000
"""The version of migration 1000 in ``horsies_migrations``."""

_INSERT_MIGRATION_SQL: Final[LiteralString] = """
    INSERT INTO horsies_migrations
        (version, description, installed_on, success, checksum, execution_time)
    VALUES (%(version)s, 'baseline', now(), TRUE, %(checksum)s, %(execution_time)s)
"""

_INSERT_FAMILY_SQL: Final[LiteralString] = """
    INSERT INTO horsies_client_family (only_row, family, recorded_at)
    VALUES (TRUE, 'PYTHON', now())
    ON CONFLICT (only_row) DO NOTHING
"""


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


def install_baseline_schema(
    database: DualRunDatabase,
) -> Result[None, DatabaseSetupFailed]:
    """Install migration 1000 with its bookkeeping row and the PYTHON family row, in one transaction.

    The text is the baseline SQL files of ``horsies._core``, concatenated in
    their run order; the bookkeeping row stores its SHA-384 checksum, as the
    core's install does. Unlike the core's install, this one runs no
    coverage pass and publishes no readers: a fresh database holds the same
    rows at any time of day, so two installs compare equal. On failure
    nothing of the install stays and the database is empty.
    """
    migration = ''.join(text for _, text in _core.baseline_sql_files()).encode('utf-8')
    try:
        with psycopg.connect(database.url, autocommit=True) as connection:
            with connection.transaction():
                started = time.perf_counter_ns()
                # No parameters: psycopg sends the text with the simple query
                # protocol, which runs every statement of it.
                _ = connection.execute(migration)
                elapsed = time.perf_counter_ns() - started
                _ = connection.execute(
                    _INSERT_MIGRATION_SQL,
                    {
                        'version': BASELINE_VERSION,
                        'checksum': hashlib.sha384(migration).digest(),
                        'execution_time': elapsed,
                    },
                )
                _ = connection.execute(_INSERT_FAMILY_SQL)
    except psycopg.Error as exc:
        return Err(
            DatabaseSetupFailed(
                step=SetupStep.INSTALL, database=database.name, cause=str(exc)
            )
        )
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
