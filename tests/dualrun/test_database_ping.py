"""Dual-run of the database ping caller switch.

Reference: ``tests/dualrun/reference/ping_caller.py``, the body of
``PostgresBroker.ping_database_async`` before the switch (``SELECT 1`` on the
loop's SQLAlchemy pool). Candidate: ``PostgresBroker.ping_database_async``,
which pings through the broker's ``CoreHandle``. The ping writes nothing, so
the snapshots must be equal; the reports must be equal in everything but the
latency, which each side measures.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Final

import pytest

from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.brokers.result_types import BrokerErrorCode, BrokerResult
from horsies.core.models.health import DatabasePing
from horsies.core.types.result import Err, Ok
from tests.dualrun.capture import Connection
from tests.dualrun.clients import open_broker
from tests.dualrun.compare import render_diff
from tests.dualrun.databases import DualRunDatabase
from tests.dualrun.operation import OperationDualRun, OperationSide, dual_run
from tests.dualrun.reference import ping_caller

pytestmark = [pytest.mark.integration]

MakeDatabase = Callable[[], DualRunDatabase]

type _PingCall = Callable[[PostgresBroker], Awaitable[BrokerResult[DatabasePing]]]


@dataclass(frozen=True, slots=True)
class _Pinged:
    """The ping answered; ``latency_known`` when the latency is a finite,
    non-negative number of milliseconds."""

    latency_known: bool


@dataclass(frozen=True, slots=True)
class _PingFailed:
    code: BrokerErrorCode
    retryable: bool


type _PingReport = _Pinged | _PingFailed


def _report(result: BrokerResult[DatabasePing]) -> _PingReport:
    match result:
        case Ok(ping):
            return _Pinged(latency_known=0.0 <= ping.latency_ms < float('inf'))
        case Err(failed):
            return _PingFailed(code=failed.code, retryable=failed.retryable)


def _no_seed(_connection: Connection) -> None:
    return None


def _side(name: str, call: _PingCall) -> OperationSide[None, _PingReport]:
    async def ping(database: DualRunDatabase) -> _PingReport:
        async with open_broker(database) as broker:
            return _report(await call(broker))

    def act(database: DualRunDatabase, _seed: None) -> _PingReport:
        return asyncio.run(ping(database))

    return OperationSide(name=name, act=act)


async def _core_ping(broker: PostgresBroker) -> BrokerResult[DatabasePing]:
    return await broker.ping_database_async()


_PYTHON_CALLER: Final = _side('Python caller on SQLAlchemy', ping_caller.ping_database_async)
_CORE_CALLER: Final = _side('CoreHandle.ping_database_async', _core_ping)


def _pair(empty_database: MakeDatabase) -> OperationDualRun[_PingReport, _PingReport]:
    match dual_run(empty_database, _no_seed, _PYTHON_CALLER, _CORE_CALLER):
        case Ok(run):
            return run
        case Err(failed):
            pytest.fail(f'{failed.step}: {failed.cause}')


def test_the_core_ping_answers_as_the_python_ping(empty_database: MakeDatabase) -> None:
    run = _pair(empty_database)

    assert run.diff.entries == (), render_diff(run.diff)
    assert run.reference.report == _Pinged(latency_known=True)
    assert run.candidate.report == run.reference.report
