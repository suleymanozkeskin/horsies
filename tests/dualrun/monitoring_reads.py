"""Dual-run of a read: the reference and the candidate read one seeded database.

A monitoring read writes nothing, so both sides read the same database and
their answers compare as they are, with no id labels. The raw snapshot of
every catalog table is read before and after all reads; the two snapshots
are equal when neither side wrote.

Monitoring times are UTC in the public answer (decided). Python's reference
reads wrote the offset of the database session, which follows the server's
``TimeZone``; so a time of the reference report equals the same instant in
UTC. A time of the candidate report is compared as written.
"""

from __future__ import annotations

import asyncio
import typing
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Final, LiteralString

import psycopg
from pydantic import BaseModel, SecretStr, TypeAdapter
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine

from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.models.broker import PostgresConfig
from horsies.core.types.result import Err, Ok
from horsies.core.utils.db import register_identity_text_reads
from horsies.monitoring.errors import (
    MonitoringQueryDbOperationFailed,
    MonitoringQueryEventLoopLimit,
    MonitoringQueryHistoryContractViolation,
)
from horsies.monitoring.models import TaskDetailNotFound
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


def _sqlalchemy_url(database: DualRunDatabase) -> str:
    return database.url.replace('postgresql://', 'postgresql+psycopg://', 1)


def _raw_snapshot(database: DualRunDatabase) -> RawSnapshot:
    with psycopg.connect(database.url, autocommit=True) as connection:
        match read_raw_snapshot(connection, BASELINE_CATALOG, row_limit=_SNAPSHOT_ROW_LIMIT):
            case Err(failed):
                raise AssertionError(f'the snapshot read failed: {failed}')
            case Ok(snapshot):
                return snapshot


async def _seed(database: DualRunDatabase, rows: Sequence[SeedRow]) -> None:
    engine = create_async_engine(_sqlalchemy_url(database))
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
    broker = PostgresBroker(PostgresConfig(database_url=SecretStr(database.url)))
    try:
        installed = await broker.ensure_schema_initialized()
        assert installed.is_ok(), installed
        await _seed(database, rows)
    finally:
        await broker.close_async()


async def _read_on[A](
    database: DualRunDatabase, pairs: Sequence[ReadPair[A]]
) -> tuple[ReadAnswers[A], ...]:
    broker = PostgresBroker(PostgresConfig(database_url=SecretStr(database.url)))
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


COUNTING_SPAN_TOLERANCE_S: Final = 2
"""How many seconds an open span may grow between the reference read and the
candidate read that follows it."""

_REPORT_DEPTH_BOUND: Final = 32
"""The deepest nesting of a monitoring answer report."""


def report(answer: object) -> object:
    """The report of one monitoring answer, as JSON values.

    ``Ok`` holds the ``model_dump(mode='json')`` of each model (a list item by
    item; a ``TaskDetailNotFound`` by its id and reason; ``None`` as it is);
    ``Err`` holds the error by its code and ``retryable`` (the messages name
    each implementation's cause in its own words).
    """
    whole: object = answer
    match answer:
        case Ok(ok_value=BaseModel() as model):
            return {'ok': model.model_dump(mode='json')}
        case Ok(ok_value=list()):
            items: list[object] = getattr(whole, 'ok_value')
            return {'ok': [_item_report(item) for item in items]}
        case Ok(ok_value=TaskDetailNotFound(task_id=task_id, reason=reason)):
            return {'ok': {'not_found': str(task_id), 'reason': reason.value}}
        case Ok(ok_value=None):
            return {'ok': None}
        case Err(
            err_value=MonitoringQueryDbOperationFailed()
            | MonitoringQueryEventLoopLimit()
            | MonitoringQueryHistoryContractViolation() as error
        ):
            return {'err': {'code': error.code.value, 'retryable': error.retryable}}
        case _:
            raise AssertionError(f'no report rule for the answer {answer!r}')


def _item_report(item: object) -> object:
    match item:
        case BaseModel():
            return item.model_dump(mode='json')
        case str() | int():
            return item
        case _:
            raise AssertionError(f'no report rule for the list item {item!r}')


def _open_spans(item: Mapping[str, object]) -> frozenset[str]:
    """The span fields of one report object that count up to the read time.

    A task with no end time (a live task) has open ``queue_s`` and
    ``exec_s``; a run with no ``completed_at`` has an open ``wall_s``; a
    RUNNING node has an open ``exec_s``; a worker's ``snapshot_age_s`` and
    ``uptime_s`` count up. Every other value is exact.
    """
    open_fields: set[str] = {'snapshot_age_s', 'uptime_s'}
    if 'failed_at' in item and item.get('completed_at') is None and item.get('failed_at') is None:
        open_fields |= {'queue_s', 'exec_s'}
    if 'wall_s' in item and item.get('completed_at') is None:
        open_fields.add('wall_s')
    if item.get('node_status') == 'RUNNING':
        open_fields.add('exec_s')
    return frozenset(open_fields)


_TIME_TEXT: Final = TypeAdapter(datetime)
"""pydantic's writer of a datetime: the text of a time in a report."""


def _candidate_is_reference_time_in_utc(reference: str, candidate: str) -> bool:
    """Whether ``reference`` is a time that pydantic wrote at an offset other
    than UTC, and ``candidate`` is the same instant as pydantic writes it in
    UTC. Pure.

    Only the reference side may carry another offset: Python's reference
    read wrote the offset of the database session. A UTC time of the
    reference, and any time of the candidate, are compared as written.
    """
    try:
        written = datetime.fromisoformat(reference)
    except ValueError:
        return False
    match written.utcoffset():
        case None:
            return False
        case offset if offset == timedelta(0):
            return False
        case _:
            pass
    if _TIME_TEXT.dump_python(written, mode='json') != reference:
        return False
    return _TIME_TEXT.dump_python(written.astimezone(UTC), mode='json') == candidate


def report_difference(reference: object, candidate: object) -> list[str]:
    """The paths where two reports differ. An open span of the candidate may
    be up to ``COUNTING_SPAN_TOLERANCE_S`` seconds longer than the
    reference's: the candidate reads after the reference. A time of the
    reference at another offset equals the same instant in UTC on the
    candidate (``_candidate_is_reference_time_in_utc``)."""
    differences: list[str] = []
    _difference('$', reference, candidate, differences, depth=0)
    return differences


def _difference(
    path: str, reference: object, candidate: object, differences: list[str], *, depth: int,
) -> None:
    assert depth <= _REPORT_DEPTH_BOUND, f'{path}: a report nests deeper than {_REPORT_DEPTH_BOUND}'
    match (reference, candidate):
        case (dict(), dict()):
            left = typing.cast(dict[str, object], reference)
            right = typing.cast(dict[str, object], candidate)
            if left.keys() != right.keys():
                differences.append(f'{path}: keys {sorted(left)} != {sorted(right)}')
                return
            open_fields = _open_spans(left)
            for key, left_value in left.items():
                right_value = right[key]
                match (key in open_fields, left_value, right_value):
                    case (True, int(), int()) if 0 <= right_value - left_value <= COUNTING_SPAN_TOLERANCE_S:
                        continue
                    case _:
                        _difference(f'{path}.{key}', left_value, right_value, differences, depth=depth + 1)
        case (list(), list()):
            left_items = typing.cast(list[object], reference)
            right_items = typing.cast(list[object], candidate)
            if len(left_items) != len(right_items):
                differences.append(f'{path}: {len(left_items)} items != {len(right_items)}')
                return
            for index, (left_item, right_item) in enumerate(zip(left_items, right_items, strict=True)):
                _difference(f'{path}[{index}]', left_item, right_item, differences, depth=depth + 1)
        case (str() as left_text, str() as right_text) if _candidate_is_reference_time_in_utc(
            left_text, right_text,
        ):
            return
        case _:
            if reference != candidate or type(reference) is not type(candidate):
                differences.append(f'{path}: {reference!r} != {candidate!r}')


def differing_reports[A](answers: Sequence[ReadAnswers[A]]) -> dict[str, list[str]]:
    """The reads whose two answers differ by report, with the differing paths."""
    found: dict[str, list[str]] = {}
    for each in answers:
        differences = report_difference(report(each.reference), report(each.candidate))
        if differences:
            found[each.name] = differences
    return found
