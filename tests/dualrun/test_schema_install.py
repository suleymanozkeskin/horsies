"""Dual-run of the schema install caller switch.

Reference: ``tests/dualrun/reference/install_caller.py``, the bodies of
``PostgresBroker.ensure_schema_initialized`` and ``_ensure_initialized``
before the switch (the Python install on the loop's SQLAlchemy engine).
Candidate: ``PostgresBroker.ensure_schema_initialized``, which installs
through the broker's ``CoreHandle``.

Each side starts from the harness's database (the baseline migration with
the PYTHON family row, and no published history readers), changes it in the
seed, and installs with a new broker. An install that is not refused ends
with the startup coverage pass of the default retention config, on both
sides, so the snapshots hold the same leaves. The cases:

- the baseline without readers: the install publishes them;
- the baseline without the family row: the install records PYTHON;
- an empty database (the seed drops the schema): the install runs the
  baseline;
- a database of the other client family: the install refuses, not
  retryable, with a message that names both families, and writes nothing.

The snapshots must be equal (the reader publication shows in the lookup
manifest rows), and the reports must be equal.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Final

import psycopg
import pytest
from pydantic import SecretStr

from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.brokers.result_types import (
    BrokerErrorCode,
    BrokerEventLoopLimit,
    BrokerOperationFailed,
)
from horsies.core.history.reads.publisher import PUBLISHED_READER_SIGNATURES
from horsies.core.models.broker import PostgresConfig
from horsies.core.types.result import Err, Ok, Result
from tests.dualrun.capture import Connection
from tests.dualrun.compare import render_diff
from tests.dualrun.databases import DualRunDatabase
from tests.dualrun.operation import OperationDualRun, OperationSide, dual_run
from tests.dualrun.reference import install_caller

pytestmark = [pytest.mark.integration]

MakeDatabase = Callable[[], DualRunDatabase]

type _InstallResult = Result[None, BrokerOperationFailed | BrokerEventLoopLimit]
type _InstallCall = Callable[[PostgresBroker], Awaitable[_InstallResult]]


@dataclass(frozen=True, slots=True)
class _Installed:
    """The install answered ``Ok``; the database's state after it."""

    family: str | None
    readers_published: bool


@dataclass(frozen=True, slots=True)
class _InstallRefused:
    code: BrokerErrorCode
    retryable: bool
    names_both_families: bool
    family: str | None
    """The family row after the refusal: the refusal writes nothing."""


type _InstallReport = _Installed | _InstallRefused


_READERS_SQL: Final = (
    'SELECT bool_and(to_regprocedure(signature) IS NOT NULL) '
    'FROM unnest(%s::text[]) AS reader(signature)'
)


def _family(connection: Connection) -> str | None:
    """The recorded family, or ``None`` with no row. Every case ends with the
    table in place: the install creates it, and a refusal keeps it."""
    match connection.execute(
        'SELECT family FROM horsies_client_family WHERE only_row'
    ).fetchall():
        case [(str() as family,)]:
            return family
        case []:
            return None
        case other:
            raise AssertionError(f'the family read returned {other!r}')


def _readers_published(connection: Connection) -> bool:
    match connection.execute(_READERS_SQL, (list(PUBLISHED_READER_SIGNATURES),)).fetchone():
        case (bool() as published,):
            return published
        case other:
            raise AssertionError(f'the reader read returned {other!r}')


def _report(database: DualRunDatabase, result: _InstallResult) -> _InstallReport:
    with psycopg.connect(database.url, autocommit=True) as connection:
        family = _family(connection)
        match result:
            case Ok():
                return _Installed(family=family, readers_published=_readers_published(connection))
            case Err(failed):
                return _InstallRefused(
                    code=failed.code,
                    retryable=failed.retryable,
                    names_both_families=(
                        'RUST' in failed.message and 'PYTHON' in failed.message
                    ),
                    family=family,
                )


def _side(name: str, call: _InstallCall) -> OperationSide[None, _InstallReport]:
    async def install(database: DualRunDatabase) -> _InstallResult:
        broker = PostgresBroker(PostgresConfig(database_url=SecretStr(database.url)))
        try:
            return await call(broker)
        finally:
            _ = await broker.close_async()

    def act(database: DualRunDatabase, _seed: None) -> _InstallReport:
        return _report(database, asyncio.run(install(database)))

    return OperationSide(name=name, act=act)


async def _core_install(broker: PostgresBroker) -> _InstallResult:
    return await broker.ensure_schema_initialized()


_PYTHON_CALLER: Final = _side(
    'Python install on SQLAlchemy and psycopg', install_caller.ensure_schema_initialized,
)
_CORE_CALLER: Final = _side('CoreHandle.install_schema_async', _core_install)


def _baseline_without_readers(_connection: Connection) -> None:
    return None


def _baseline_without_family_row(connection: Connection) -> None:
    _ = connection.execute('DELETE FROM horsies_client_family')


def _empty_database(connection: Connection) -> None:
    _ = connection.execute('DROP SCHEMA public CASCADE')
    _ = connection.execute('CREATE SCHEMA public')


def _other_family(connection: Connection) -> None:
    _ = connection.execute("UPDATE horsies_client_family SET family = 'RUST'")


def _pair(
    empty_database: MakeDatabase, seed: Callable[[Connection], None],
) -> OperationDualRun[_InstallReport, _InstallReport]:
    match dual_run(empty_database, seed, _PYTHON_CALLER, _CORE_CALLER):
        case Ok(run):
            return run
        case Err(failed):
            pytest.fail(f'{failed.step}: {failed.cause}')


def test_the_core_install_publishes_the_missing_readers(empty_database: MakeDatabase) -> None:
    run = _pair(empty_database, _baseline_without_readers)

    assert run.diff.entries == (), render_diff(run.diff)
    assert run.reference.report == _Installed(family='PYTHON', readers_published=True)
    assert run.candidate.report == run.reference.report


def test_the_core_install_records_the_missing_family(empty_database: MakeDatabase) -> None:
    run = _pair(empty_database, _baseline_without_family_row)

    assert run.diff.entries == (), render_diff(run.diff)
    assert run.reference.report == _Installed(family='PYTHON', readers_published=True)
    assert run.candidate.report == run.reference.report


def test_the_core_install_installs_an_empty_database(empty_database: MakeDatabase) -> None:
    run = _pair(empty_database, _empty_database)

    assert run.diff.entries == (), render_diff(run.diff)
    assert run.reference.report == _Installed(family='PYTHON', readers_published=True)
    assert run.candidate.report == run.reference.report


def test_the_core_install_refuses_the_other_family(empty_database: MakeDatabase) -> None:
    run = _pair(empty_database, _other_family)

    assert run.diff.entries == (), render_diff(run.diff)
    assert run.reference.report == _InstallRefused(
        code=BrokerErrorCode.SCHEMA_INIT_FAILED,
        retryable=False,
        names_both_families=True,
        family='RUST',
    )
    assert run.candidate.report == run.reference.report
