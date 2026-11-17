"""Dual-run of the result read and wait caller switch.

Reference: ``tests/dualrun/reference/result_caller.py``, the bodies of
``PostgresBroker.get_raw_result_record_async`` and ``get_raw_result_record``
before the switch (the probe on SQLAlchemy and the wait on the loop's
``PostgresListener``). Candidate: the two methods, which read and wait
through the broker's ``CoreHandle``.

The seed holds history tasks (COMPLETED and FAILED with a result, CANCELLED
without one, and a COMPLETED task whose stored digest differs from its
payload), a live PENDING task, and two ids that no task holds. Each side
reads every terminal and absent case with no limit, the live task with a
limit of 0 ms and of 300 ms, the live task with 300 ms on a broker built with
``assume_initialized=True`` (no listener: it polls), and one case with the
blocking form. The read writes nothing, so the snapshots must be equal; the
reports must be equal.
"""

from __future__ import annotations

import asyncio
import json
import uuid
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from datetime import timedelta
from typing import Final, LiteralString

import pytest
from pydantic import SecretStr

from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.brokers.result_types import BrokerErrorCode, BrokerResult, RawResultRecord
from horsies.core.history.identity.uuid7 import mint_task_id
from horsies.core.models.broker import PostgresConfig
from horsies.core.types.result import Err, Ok
from tests.dualrun.capture import Connection
from tests.dualrun.compare import render_diff
from tests.dualrun.databases import DualRunDatabase
from tests.dualrun.operation import OperationDualRun, OperationSide, dual_run
from tests.dualrun.reference import install_caller, result_caller
from tests.dualrun.seeds import SeedCoverage, SeedStatus, TaskSeed, insert_task

pytestmark = [pytest.mark.integration]

MakeDatabase = Callable[[], DualRunDatabase]

_SUCCESS_RESULT: Final = '{"__h_task_result__":true,"ok":7,"err":null}'
_FAILURE_RESULT: Final = '{"__h_task_result__":true,"ok":null,"err":{"error_code":"E2"}}'
_SHORT_WAIT_MS: Final = 300

_COMPLETE_SQL: Final[LiteralString] = """
    SELECT outcome FROM horsies_complete_task_fused(
        %(task_id)s, %(worker_id)s, (SELECT claimed_at FROM horsies_tasks WHERE id = %(task_id)s),
        %(result)s, FALSE
    )
"""

_FAIL_SQL: Final[LiteralString] = """
    SELECT outcome FROM horsies_finish_failed_attempt(
        %(task_id)s, %(worker_id)s, (SELECT claimed_at FROM horsies_tasks WHERE id = %(task_id)s),
        'NONE', NULL, NULL, NULL, %(result)s, 'E2', 'failed', 'FAILED', 'boom', 'failed', 'REFUSE', FALSE
    )
"""

_CANCEL_SQL: Final[LiteralString] = 'SELECT outcome FROM horsies_cancel_task(%(task_id)s, FALSE)'

_BREAK_DIGEST_SQL: Final[LiteralString] = """
    UPDATE horsies_task_history SET result_digest = decode(repeat('00', 32), 'hex')
    WHERE task_id = %(task_id)s
"""


@dataclass(frozen=True, slots=True)
class _Seed:
    ids: Mapping[str, uuid.UUID]


def _running(name: str) -> TaskSeed:
    return TaskSeed(
        task_name=name,
        status=SeedStatus.RUNNING,
        worker_id=f'worker-{name}',
        claimed_ago=timedelta(minutes=2),
        started_ago=timedelta(minutes=1),
    )


def _one_outcome(
    connection: Connection, statement: LiteralString, params: Mapping[str, object],
) -> str:
    match connection.execute(statement, params).fetchall():
        case [(str() as outcome,)]:
            return outcome
        case other:
            raise AssertionError(f'{statement.split("(")[0].strip()} returned {other!r}')


def _finished(connection: Connection, name: str, statement: LiteralString, result: str) -> uuid.UUID:
    task_id = insert_task(connection, _running(name)).task_id
    params = {'task_id': task_id, 'worker_id': f'worker-{name}', 'result': result}
    assert _one_outcome(connection, statement, params) == 'APPLIED'
    return task_id


def _seed(connection: Connection) -> _Seed:
    ids: dict[str, uuid.UUID] = {}
    ids['result.completed'] = _finished(connection, 'result.completed', _COMPLETE_SQL, _SUCCESS_RESULT)
    ids['result.failed'] = _finished(connection, 'result.failed', _FAIL_SQL, _FAILURE_RESULT)
    cancelled = insert_task(
        connection, TaskSeed(task_name='result.cancelled', status=SeedStatus.PENDING)
    ).task_id
    assert _one_outcome(connection, _CANCEL_SQL, {'task_id': cancelled}) == 'APPLIED'
    ids['result.cancelled'] = cancelled
    corrupt = _finished(connection, 'result.corrupt', _COMPLETE_SQL, _SUCCESS_RESULT)
    _ = connection.execute(_BREAK_DIGEST_SQL, {'task_id': corrupt})
    ids['result.corrupt'] = corrupt
    ids['absent.uuid7'] = mint_task_id()
    ids['absent.uuid4'] = uuid.uuid4()
    ids['live.pending'] = insert_task(
        connection, TaskSeed(task_name='live.pending', status=SeedStatus.PENDING)
    ).task_id
    return _Seed(ids=ids)


# ---- Reports ----


@dataclass(frozen=True, slots=True)
class _Found:
    task_name: str
    status: str
    raw_result: str | None
    """The envelope as JSON text with sorted keys."""


@dataclass(frozen=True, slots=True)
class _Absent:
    """``Ok(None)``."""


@dataclass(frozen=True, slots=True)
class _ReadFailed:
    code: BrokerErrorCode
    retryable: bool


type _ReadReport = _Found | _Absent | _ReadFailed


def _report(result: BrokerResult[RawResultRecord | None]) -> _ReadReport:
    match result:
        case Ok(None):
            return _Absent()
        case Ok(RawResultRecord() as record):
            return _Found(
                task_name=record.task_name,
                status=record.status.value,
                raw_result=(
                    None
                    if record.raw_result is None
                    else json.dumps(record.raw_result, sort_keys=True)
                ),
            )
        case Err(failed):
            return _ReadFailed(code=failed.code, retryable=failed.retryable)
        case _:
            raise AssertionError(f'a result read returned {result!r}')


# ---- Sides ----

type _InstallCall = Callable[[PostgresBroker], Awaitable[object]]
type _AsyncRead = Callable[[PostgresBroker, uuid.UUID, int | None], Awaitable[BrokerResult[RawResultRecord | None]]]
type _SyncRead = Callable[[PostgresBroker, uuid.UUID, int | None], BrokerResult[RawResultRecord | None]]

_FINISHED_CASES: Final = (
    'result.completed', 'result.failed', 'result.cancelled', 'result.corrupt',
    'absent.uuid7', 'absent.uuid4',
)
_BLOCKING_CASE: Final = 'result.completed'


def _broker(database: DualRunDatabase, *, assume_initialized: bool) -> PostgresBroker:
    return PostgresBroker(
        PostgresConfig(database_url=SecretStr(database.url)),
        assume_initialized=assume_initialized,
    )


async def _async_reads(
    database: DualRunDatabase,
    broker: PostgresBroker,
    install: _InstallCall,
    read: _AsyncRead,
    seed: _Seed,
) -> dict[str, _ReadReport]:
    _ = await install(broker)
    report: dict[str, _ReadReport] = {}
    for case in _FINISHED_CASES:
        report[case] = _report(await read(broker, seed.ids[case], None))
    live = seed.ids['live.pending']
    report['live.pending 0 ms'] = _report(await read(broker, live, 0))
    report[f'live.pending {_SHORT_WAIT_MS} ms'] = _report(await read(broker, live, _SHORT_WAIT_MS))
    polling = _broker(database, assume_initialized=True)
    try:
        report[f'live.pending {_SHORT_WAIT_MS} ms, no listener'] = _report(
            await read(polling, live, _SHORT_WAIT_MS),
        )
    finally:
        _ = await polling.close_async()
    return report


def _side(
    name: str, install: _InstallCall, read: _AsyncRead, read_blocking: _SyncRead,
) -> OperationSide[_Seed, dict[str, _ReadReport]]:
    def act(database: DualRunDatabase, seed: _Seed) -> dict[str, _ReadReport]:
        broker = _broker(database, assume_initialized=False)
        try:
            report = asyncio.run(_async_reads(database, broker, install, read, seed))
            report[f'{_BLOCKING_CASE} blocking'] = _report(
                read_blocking(broker, seed.ids[_BLOCKING_CASE], None),
            )
        finally:
            _ = broker.close()
        return report

    return OperationSide(name=name, act=act)


async def _python_read(
    broker: PostgresBroker, task_id: uuid.UUID, timeout_ms: int | None,
) -> BrokerResult[RawResultRecord | None]:
    return await result_caller.get_raw_result_record_async(broker, task_id, timeout_ms)


def _python_read_blocking(
    broker: PostgresBroker, task_id: uuid.UUID, timeout_ms: int | None,
) -> BrokerResult[RawResultRecord | None]:
    return result_caller.get_raw_result_record(broker, task_id, timeout_ms)


async def _core_install(broker: PostgresBroker) -> object:
    return await broker.ensure_schema_initialized()


async def _core_read(
    broker: PostgresBroker, task_id: uuid.UUID, timeout_ms: int | None,
) -> BrokerResult[RawResultRecord | None]:
    return await broker.get_raw_result_record_async(task_id, timeout_ms)


def _core_read_blocking(
    broker: PostgresBroker, task_id: uuid.UUID, timeout_ms: int | None,
) -> BrokerResult[RawResultRecord | None]:
    return broker.get_raw_result_record(task_id, timeout_ms)


_PYTHON_CALLER: Final = _side(
    'Python caller on SQLAlchemy and PostgresListener',
    install_caller.ensure_schema_initialized,
    _python_read,
    _python_read_blocking,
)
_CORE_CALLER: Final = _side(
    'CoreHandle.raw_result_record_async and raw_result_record',
    _core_install,
    _core_read,
    _core_read_blocking,
)


def _pair(
    empty_database: MakeDatabase,
) -> OperationDualRun[dict[str, _ReadReport], dict[str, _ReadReport]]:
    match dual_run(
        empty_database, _seed, _PYTHON_CALLER, _CORE_CALLER, SeedCoverage.HISTORY_AND_BEATS,
    ):
        case Ok(run):
            return run
        case Err(failed):
            pytest.fail(f'{failed.step}: {failed.cause}')


def test_the_core_result_read_answers_as_the_python_read(empty_database: MakeDatabase) -> None:
    run = _pair(empty_database)

    assert run.diff.entries == (), render_diff(run.diff)
    pending = _Found(task_name='live.pending', status='PENDING', raw_result=None)
    completed = _Found(
        task_name='result.completed',
        status='COMPLETED',
        raw_result=json.dumps(json.loads(_SUCCESS_RESULT), sort_keys=True),
    )
    assert run.reference.report == {
        'result.completed': completed,
        'result.failed': _Found(
            task_name='result.failed',
            status='FAILED',
            raw_result=json.dumps(json.loads(_FAILURE_RESULT), sort_keys=True),
        ),
        'result.cancelled': _Found(task_name='result.cancelled', status='CANCELLED', raw_result=None),
        'result.corrupt': _ReadFailed(code=BrokerErrorCode.INVALID_JSON_PAYLOAD, retryable=False),
        'absent.uuid7': _Absent(),
        'absent.uuid4': _Absent(),
        'live.pending 0 ms': pending,
        f'live.pending {_SHORT_WAIT_MS} ms': pending,
        f'live.pending {_SHORT_WAIT_MS} ms, no listener': pending,
        f'{_BLOCKING_CASE} blocking': completed,
    }
    assert run.candidate.report == run.reference.report
