"""Fresh ``dualrun_`` databases with the schema of the core's install, for the integration tests.

The server comes from ``tests.session_database.base_database_url`` (the
``HORSIES_TEST_DATABASE_URL`` override, else the local default). These
functions create and drop only databases whose name matches
``dualrun_<32 hex>``; they never change another database.
"""

from __future__ import annotations

import re
import uuid
from dataclasses import dataclass
from enum import Enum
from typing import Final, assert_never

import psycopg
from psycopg import sql
from pydantic import SecretStr
from sqlalchemy.engine import make_url

from horsies._catalog.broker_errors import BrokerOperationFailed
from horsies._catalog.manifest import dump_producer_manifest
from horsies._core import CoreHandle, SchemaInstalled, SchemaInstallNotRun
from horsies.core.history.identity.keys import IDEMPOTENCY_WINDOW_DEFAULT
from horsies.core.manifest import install_coverage_of, to_producer_manifest
from horsies.core.models.broker import PostgresConfig
from horsies.core.models.retention import RetentionConfig
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
    """A setup step failed. After CREATE or DROP nothing is known to have changed; after INSTALL the database exists and holds what the core's install committed before it failed."""

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
    """Install the schema through the core: ``CoreHandle.install_schema`` of a
    producer handle on ``database``, built as the broker of an app with the
    default retention builds its handle.

    The core runs the migrations with their bookkeeping row and the
    ``PYTHON`` family row, publishes the history readers, and ends with the
    startup coverage pass: the daily history leaves of the current UTC day
    and of the lookahead for the default classes, and the hourly heartbeat
    leaves. Leaf names hold the day or the hour of the install; a snapshot
    reads them as offsets from the start of its run window (``DatedName``).
    The handle is closed before the return.

    Raises:
        AssertionError: the install ran nothing, which a handle in the
            ``MIGRATE_AND_VALIDATE`` mode never answers (a defect).
    """
    handle = CoreHandle(
        dump_producer_manifest(
            to_producer_manifest(
                PostgresConfig(database_url=SecretStr(database.url)),
                schema='MIGRATE_AND_VALIDATE',
                idempotency_reservation_window=IDEMPOTENCY_WINDOW_DEFAULT,
            ),
        ),
        pool_profile='PRODUCER',
    ).with_install_coverage(install_coverage_of(RetentionConfig()))
    try:
        outcome = handle.install_schema()
    finally:
        _ = handle.close()
    match outcome:
        case SchemaInstalled():
            return Ok(None)
        case SchemaInstallNotRun():
            raise AssertionError(
                f'the install of {database.name} ran nothing, but its handle migrates',
            )
        case BrokerOperationFailed(message=message):
            return Err(
                DatabaseSetupFailed(
                    step=SetupStep.INSTALL, database=database.name, cause=message,
                ),
            )
        case _ as unreachable:
            assert_never(unreachable)


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
