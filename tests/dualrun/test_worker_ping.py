"""Dual-run of the worker ping switch.

Reference: Python's body before the switch
(``tests/dualrun/reference/broker_monitoring.py``: ``ping_workers_async``),
which sends the NOTIFY on the loop's SQLAlchemy session and waits on the
loop's listener. Candidate: ``PostgresBroker.ping_workers_async``, which the
core runs on the process listener of the broker's core handle. The ping
writes no row, so the snapshot of the database cannot change; the reports
are the comparison.

Each side runs with its own responder: one connection that listens on the
ping channel and answers each ping in one transaction with a payload that
is not JSON, an object that is not a pong, a pong of another round, two
pongs of one worker (the second one is left out) and one pong of a second
worker. A report is the
replying workers in reply order, with each round trip checked to be a
finite, non-negative number of milliseconds; a failure is its code and
``retryable``.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncGenerator, Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Final

import psycopg
import pytest
from pydantic import SecretStr

from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.brokers.result_types import BrokerErrorCode, BrokerResult
from horsies.core.models.broker import PostgresConfig
from horsies.core.models.health import WORKER_PING_CHANNEL, WorkerPong
from horsies.core.types.result import Err, Ok
from tests.dualrun.databases import DualRunDatabase
from tests.dualrun.reference import broker_monitoring as python_reads

pytestmark = [pytest.mark.integration]

MakeDatabase = Callable[[], DualRunDatabase]

_WINDOW_S: Final = 1.0
"""Long enough for the responder's replies on a local server."""


@dataclass(frozen=True, slots=True)
class _Replied:
    """The workers that replied, in reply order, and whether every round trip
    is a finite, non-negative number of milliseconds."""

    workers: tuple[tuple[str, str, int], ...]
    round_trips_known: bool


@dataclass(frozen=True, slots=True)
class _Failed:
    code: BrokerErrorCode
    retryable: bool


type _Report = _Replied | _Failed


def _report(result: BrokerResult[list[WorkerPong]]) -> _Report:
    match result:
        case Ok(pongs):
            return _Replied(
                workers=tuple((pong.worker_id, pong.hostname, pong.pid) for pong in pongs),
                round_trips_known=all(0.0 <= pong.round_trip_ms < float('inf') for pong in pongs),
            )
        case Err(error):
            return _Failed(code=error.code, retryable=error.retryable)


def _replies(request: dict[str, object]) -> list[str]:
    """The payloads that the responder sends for one ping, in order."""
    correlation_id = request['correlation_id']

    def pong(worker_id: str, pid: int, correlation: object = correlation_id) -> str:
        return json.dumps(
            {'correlation_id': correlation, 'worker_id': worker_id, 'hostname': 'host-' + worker_id, 'pid': pid},
        )

    return [
        'not json',
        json.dumps({'correlation_id': correlation_id, 'worker_id': 'w-0'}),
        pong('w-other', 1, correlation='another-round'),
        pong('w-1', 101),
        # PostgreSQL folds equal notifications of one transaction into one;
        # a second pid keeps the second reply of w-1.
        pong('w-1', 102),
        pong('w-2', 202),
    ]


async def _respond(url: str, listening: asyncio.Event) -> None:
    """Answer every ping until cancelled."""
    async with (
        await psycopg.AsyncConnection.connect(url, autocommit=True) as listener,
        await psycopg.AsyncConnection.connect(url, autocommit=False) as sender,
    ):
        _ = await listener.execute(f'LISTEN {WORKER_PING_CHANNEL}')
        listening.set()
        async for notify in listener.notifies():
            request = json.loads(notify.payload)
            async with sender.transaction():
                for payload in _replies(request):
                    _ = await sender.execute(
                        'SELECT pg_notify(%s, %s)', (request['reply_channel'], payload),
                    )


@asynccontextmanager
async def _responder(url: str) -> AsyncGenerator[None]:
    listening = asyncio.Event()
    task = asyncio.create_task(_respond(url, listening))
    await asyncio.wait_for(listening.wait(), timeout=10.0)
    try:
        yield
    finally:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass


@dataclass(frozen=True, slots=True)
class _Ping:
    name: str
    target_worker_id: str | None = None
    timeout_seconds: float = _WINDOW_S
    min_responses: int | None = None


_PINGS: Final = (
    _Ping('every worker'),
    _Ping('first reply', min_responses=1),
    _Ping('two replies', min_responses=2),
    _Ping('target w-2', target_worker_id='w-2'),
    _Ping('absent target', target_worker_id='w-absent', timeout_seconds=0.3),
    _Ping('zero window', timeout_seconds=0.0),
    _Ping('negative window', timeout_seconds=-1.5),
    _Ping('zero replies', min_responses=0),
)


async def _ping(broker: PostgresBroker, ping: _Ping, *, on_core: bool) -> _Report:
    match on_core:
        case True:
            result = await broker.ping_workers_async(
                target_worker_id=ping.target_worker_id,
                timeout_seconds=ping.timeout_seconds,
                min_responses=ping.min_responses,
            )
        case False:
            result = await python_reads.ping_workers_async(
                broker,
                target_worker_id=ping.target_worker_id,
                timeout_seconds=ping.timeout_seconds,
                min_responses=ping.min_responses,
            )
    return _report(result)


async def _side(database: DualRunDatabase, *, on_core: bool) -> tuple[_Report, ...]:
    """Every ping of ``_PINGS`` with a responder running."""
    broker = PostgresBroker(PostgresConfig(database_url=SecretStr(database.url)))
    try:
        installed = await broker.ensure_schema_initialized()
        assert installed.is_ok(), installed
        async with _responder(database.url):
            return tuple([await _ping(broker, ping, on_core=on_core) for ping in _PINGS])
    finally:
        await broker.close_async()


async def _without_listener(database: DualRunDatabase, *, on_core: bool) -> _Report:
    """A ping on a broker built with ``assume_initialized=True``."""
    broker = PostgresBroker(
        PostgresConfig(database_url=SecretStr(database.url)), assume_initialized=True,
    )
    try:
        return await _ping(broker, _Ping('no listener'), on_core=on_core)
    finally:
        await broker.close_async()


def test_the_core_ping_answers_what_python_answered(empty_database: MakeDatabase) -> None:
    database = empty_database()

    python = asyncio.run(_side(database, on_core=False))
    core = asyncio.run(_side(database, on_core=True))

    assert core == python
    both_workers = (('w-1', 'host-w-1', 101), ('w-2', 'host-w-2', 202))
    assert core[:5] == (
        _Replied(workers=both_workers, round_trips_known=True),
        _Replied(workers=both_workers[:1], round_trips_known=True),
        _Replied(workers=both_workers, round_trips_known=True),
        _Replied(workers=both_workers, round_trips_known=True),
        _Replied(workers=both_workers, round_trips_known=True),
    )
    refused = _Failed(code=BrokerErrorCode.INVALID_INPUT, retryable=False)
    assert core[5:] == (refused, refused, refused)


def test_a_broker_without_a_listener_is_refused_alike(empty_database: MakeDatabase) -> None:
    database = empty_database()

    python = asyncio.run(_without_listener(database, on_core=False))
    core = asyncio.run(_without_listener(database, on_core=True))

    assert python == _Failed(code=BrokerErrorCode.LISTENER_DISABLED, retryable=False)
    assert core == python


def test_the_comparison_sees_a_candidate_that_stops_at_the_first_reply(
    empty_database: MakeDatabase,
) -> None:
    """Python waits for every worker and the core stops at the first; the
    reports must differ."""
    database = empty_database()

    async def one(on_core: bool, ping: _Ping) -> _Report:
        broker = PostgresBroker(PostgresConfig(database_url=SecretStr(database.url)))
        try:
            installed = await broker.ensure_schema_initialized()
            assert installed.is_ok(), installed
            async with _responder(database.url):
                return await _ping(broker, ping, on_core=on_core)
        finally:
            await broker.close_async()

    python = asyncio.run(one(False, _Ping('every worker')))
    core = asyncio.run(one(True, _Ping('first reply', min_responses=1)))

    assert core != python
