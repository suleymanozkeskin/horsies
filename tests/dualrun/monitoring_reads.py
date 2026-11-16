"""Dual-run of a read: the reference and the candidate read one seeded database.

A monitoring read writes nothing, so both sides read the same database and
their answers compare as they are, with no id labels. The raw snapshot of
every catalog table is read before and after all reads; the two snapshots
are equal when neither side wrote.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass
from typing import Final, LiteralString

import psycopg
from pydantic import SecretStr
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine

from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.models.broker import PostgresConfig
from horsies.core.types.result import Err, Ok
from horsies.core.utils.db import register_identity_text_reads
from tests.dualrun.capture import RawSnapshot, read_raw_snapshot
from tests.dualrun.catalog import BASELINE_CATALOG
from tests.dualrun.databases import DualRunDatabase
from tests.integration.monitoring_seeding import SeedRow, insert_rows

type MakeDatabase = Callable[[], DualRunDatabase]

_SNAPSHOT_ROW_LIMIT: Final = 30_000
"""Rows per table the before-and-after snapshot reads; a bound test seeds 10,001."""


@dataclass(frozen=True, slots=True)
class ReadPair[A]:
    """One read, as the reference runs it and as the candidate runs it."""

    name: str
    reference: Callable[[PostgresBroker], Awaitable[A]]
    candidate: Callable[[PostgresBroker], Awaitable[A]]


@dataclass(frozen=True, slots=True)
class ReadAnswers[A]:
    """The two answers of one read on one database state."""

    name: str
    reference: A
    candidate: A


def _broker_url(database: DualRunDatabase) -> str:
    return database.url.replace('postgresql://', 'postgresql+psycopg://', 1)


def _raw_snapshot(database: DualRunDatabase) -> RawSnapshot:
    with psycopg.connect(database.url, autocommit=True) as connection:
        match read_raw_snapshot(connection, BASELINE_CATALOG, row_limit=_SNAPSHOT_ROW_LIMIT):
            case Err(failed):
                raise AssertionError(f'the snapshot read failed: {failed}')
            case Ok(snapshot):
                return snapshot


async def _seed(database: DualRunDatabase, rows: Sequence[SeedRow]) -> None:
    engine = create_async_engine(_broker_url(database))
    register_identity_text_reads(engine)
    try:
        async with AsyncSession(engine, expire_on_commit=False) as session:
            await insert_rows(session, *rows)
    finally:
        await engine.dispose()


async def _read_all[A](
    broker: PostgresBroker, pairs: Sequence[ReadPair[A]]
) -> tuple[ReadAnswers[A], ...]:
    return tuple(
        [
            ReadAnswers(
                name=pair.name,
                reference=await pair.reference(broker),
                candidate=await pair.candidate(broker),
            )
            for pair in pairs
        ]
    )


async def _install_and_seed(database: DualRunDatabase, rows: Sequence[SeedRow]) -> None:
    broker = PostgresBroker(PostgresConfig(database_url=SecretStr(_broker_url(database))))
    try:
        installed = await broker.ensure_schema_initialized()
        assert installed.is_ok(), installed
        await _seed(database, rows)
    finally:
        await broker.close_async()


async def _read_on[A](
    database: DualRunDatabase, pairs: Sequence[ReadPair[A]]
) -> tuple[ReadAnswers[A], ...]:
    broker = PostgresBroker(PostgresConfig(database_url=SecretStr(_broker_url(database))))
    try:
        return await _read_all(broker, pairs)
    finally:
        await broker.close_async()


def _run_statements(database: DualRunDatabase, statements: Sequence[LiteralString]) -> None:
    with psycopg.connect(database.url, autocommit=True) as connection:
        for statement in statements:
            _ = connection.execute(statement)


def read_dual_run[A](
    make_database: MakeDatabase,
    rows: Sequence[SeedRow],
    pairs: Sequence[ReadPair[A]],
    *,
    seed_statements: Sequence[LiteralString] = (),
) -> tuple[ReadAnswers[A], ...]:
    """Install the baseline, seed ``rows`` and ``seed_statements``, then run each pair on that state.

    Asserts that the raw snapshot after every read equals the one before.
    """
    database = make_database()
    asyncio.run(_install_and_seed(database, rows))
    _run_statements(database, seed_statements)
    before = _raw_snapshot(database)
    answers = asyncio.run(_read_on(database, pairs))
    after = _raw_snapshot(database)
    assert after == before, 'a read changed the database'
    return answers


def differing[A](answers: Sequence[ReadAnswers[A]]) -> list[str]:
    """The names of the reads whose two answers differ."""
    return [each.name for each in answers if each.reference != each.candidate]
