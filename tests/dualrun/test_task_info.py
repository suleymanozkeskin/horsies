"""Dual-run of the task info caller switch.

Reference: ``tests/dualrun/reference/task_info_caller.py``, the bodies of
``PostgresBroker.get_task_info_async`` and ``get_task_info`` before the
switch (the live read and the staged detail read on SQLAlchemy). Candidate:
the two methods, which read through the broker's ``CoreHandle``.

The seed holds live tasks (PENDING, CLAIMED, RUNNING, and a PENDING task
after a retry, with its attempt), tasks that moved to history (COMPLETED;
FAILED after a retry, with two attempts; CANCELLED; EXPIRED), and two ids
that no task holds (a UUIDv7 and a UUIDv4). Each side reads every case with
no optional part and with all of them, and one case with the blocking form.
The read writes nothing, so the snapshots must be equal; the reports must be
equal.
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
from horsies.core.brokers.result_types import BrokerErrorCode, BrokerResult
from horsies.core.history.identity.uuid7 import mint_task_id
from horsies.core.models.broker import PostgresConfig
from horsies.core.models.tasks import TaskInfo
from horsies.core.types.result import Err, Ok
from tests.dualrun.capture import Connection
from tests.dualrun.compare import render_diff
from tests.dualrun.databases import DualRunDatabase
from tests.dualrun.operation import OperationDualRun, OperationSide, dual_run
from tests.dualrun.reference import install_caller, task_info_caller
from tests.dualrun.seeds import SeedCoverage, SeedStatus, TaskSeed, insert_task

pytestmark = [pytest.mark.integration]

MakeDatabase = Callable[[], DualRunDatabase]

_FAILURE_RESULT: Final = '{"__h_task_result__":true,"ok":null,"err":{"error_code":"E1"}}'
_FINAL_FAILURE_RESULT: Final = '{"__h_task_result__":true,"ok":null,"err":{"error_code":"E2"}}'
_SUCCESS_RESULT: Final = '{"__h_task_result__":true,"ok":3,"err":null}'
_EXPIRED_RESULT: Final = '{"__h_task_result__":true,"ok":null,"err":{"error_code":"TASK_EXPIRED"}}'

_RETRY_SQL: Final[LiteralString] = """
    SELECT outcome FROM horsies_finish_failed_attempt(
        %(task_id)s, %(worker_id)s, (SELECT claimed_at FROM horsies_tasks WHERE id = %(task_id)s),
        'RETRY', 0, 0.0, 'SAME_CLASS', %(result)s, 'E1', 'first try',
        'FAILED', 'boom', 'first try', 'REFUSE', FALSE
    )
"""

_FAIL_SQL: Final[LiteralString] = """
    SELECT outcome FROM horsies_finish_failed_attempt(
        %(task_id)s, %(worker_id)s, (SELECT claimed_at FROM horsies_tasks WHERE id = %(task_id)s),
        'NONE', NULL, NULL, NULL, %(result)s, 'E2', 'second try',
        'FAILED', 'boom again', 'second try', 'REFUSE', FALSE
    )
"""

_RECLAIM_SQL: Final[LiteralString] = """
    UPDATE horsies_tasks
    SET status = 'RUNNING', claimed_at = now(), claimed_by_worker_id = %(worker_id)s, started_at = now(),
        worker_pid = 4242, worker_hostname = 'host-a', worker_process_name = 'worker-4242', updated_at = now()
    WHERE id = %(task_id)s
"""

_COMPLETE_SQL: Final[LiteralString] = """
    SELECT outcome FROM horsies_complete_task_fused(
        %(task_id)s, %(worker_id)s, (SELECT claimed_at FROM horsies_tasks WHERE id = %(task_id)s),
        %(result)s, FALSE
    )
"""

_CANCEL_SQL: Final[LiteralString] = 'SELECT outcome FROM horsies_cancel_task(%(task_id)s, FALSE)'

_EXPIRE_SQL: Final[LiteralString] = """
    SELECT outcome FROM horsies_expire_pending_tasks(100, %(result)s, 'TASK_EXPIRED')
"""


@dataclass(frozen=True, slots=True)
class _Seed:
    ids: Mapping[str, uuid.UUID]
    """The task id of each case, in case order."""


def _running(name: str, *, max_retries: int = 0) -> TaskSeed:
    return TaskSeed(
        task_name=name,
        status=SeedStatus.RUNNING,
        worker_id=f'worker-{name}',
        claimed_ago=timedelta(minutes=2),
        started_ago=timedelta(minutes=1),
        max_retries=max_retries,
    )


def _one_outcome(
    connection: Connection, statement: LiteralString, params: Mapping[str, object],
) -> str:
    match connection.execute(statement, params).fetchall():
        case [(str() as outcome,)]:
            return outcome
        case other:
            raise AssertionError(f'{statement.split("(")[0].strip()} returned {other!r}')


def _seed(connection: Connection) -> _Seed:
    ids: dict[str, uuid.UUID] = {}
    ids['info.pending'] = insert_task(
        connection, TaskSeed(task_name='info.pending', status=SeedStatus.PENDING)
    ).task_id
    ids['info.claimed'] = insert_task(
        connection,
        TaskSeed(
            task_name='info.claimed',
            status=SeedStatus.CLAIMED,
            worker_id='worker-info.claimed',
            claimed_ago=timedelta(seconds=10),
        ),
    ).task_id
    ids['info.running'] = insert_task(connection, _running('info.running')).task_id

    retried = insert_task(connection, _running('info.retried', max_retries=1)).task_id
    retry = {'task_id': retried, 'worker_id': 'worker-info.retried', 'result': _FAILURE_RESULT}
    assert _one_outcome(connection, _RETRY_SQL, retry) == 'RETRY_SCHEDULED'
    ids['info.retried'] = retried

    completed = insert_task(connection, _running('info.completed')).task_id
    complete = {'task_id': completed, 'worker_id': 'worker-info.completed', 'result': _SUCCESS_RESULT}
    assert _one_outcome(connection, _COMPLETE_SQL, complete) == 'APPLIED'
    ids['info.completed'] = completed

    failed = insert_task(connection, _running('info.failed', max_retries=1)).task_id
    first = {'task_id': failed, 'worker_id': 'worker-info.failed', 'result': _FAILURE_RESULT}
    assert _one_outcome(connection, _RETRY_SQL, first) == 'RETRY_SCHEDULED'
    _ = connection.execute(_RECLAIM_SQL, first)
    final = {'task_id': failed, 'worker_id': 'worker-info.failed', 'result': _FINAL_FAILURE_RESULT}
    assert _one_outcome(connection, _FAIL_SQL, final) == 'APPLIED'
    ids['info.failed'] = failed

    cancelled = insert_task(
        connection, TaskSeed(task_name='info.cancelled', status=SeedStatus.PENDING)
    ).task_id
    assert _one_outcome(connection, _CANCEL_SQL, {'task_id': cancelled}) == 'APPLIED'
    ids['info.cancelled'] = cancelled

    expired = insert_task(
        connection,
        TaskSeed(
            task_name='info.expired',
            status=SeedStatus.PENDING,
            good_until_in=timedelta(minutes=-1),
        ),
    ).task_id
    assert _one_outcome(connection, _EXPIRE_SQL, {'result': _EXPIRED_RESULT}) == 'APPLIED'
    ids['info.expired'] = expired

    ids['absent.uuid7'] = mint_task_id()
    ids['absent.uuid4'] = uuid.uuid4()
    return _Seed(ids=ids)


# ---- Reports ----

_TIME_FIELDS: Final = (
    'next_retry_at', 'sent_at', 'claimed_at', 'started_at', 'completed_at', 'failed_at',
)


@dataclass(frozen=True, slots=True)
class _AttemptReport:
    attempt: int
    outcome: str
    will_retry: bool
    error_code: str | None
    error_message: str | None
    failed_reason: str | None
    worker_id: str | None
    worker_hostname: str | None
    worker_pid: int | None
    worker_process_name: str | None


@dataclass(frozen=True, slots=True)
class _InfoReport:
    """The facts of one ``TaskInfo`` that both databases share: the times
    only as the names of the fields that hold one."""

    task_name: str
    status: str
    queue_name: str
    priority: int
    retry_count: int
    max_retries: int
    times_set: tuple[str, ...]
    worker_hostname: str | None
    worker_pid: int | None
    worker_process_name: str | None
    error_code: str | None
    failed_reason: str | None
    raw_result: str | None
    """The envelope as JSON text with sorted keys."""
    decoded: bool
    attempts: tuple[_AttemptReport, ...] | None
    execution: str | None


@dataclass(frozen=True, slots=True)
class _Absent:
    """``Ok(None)``."""


@dataclass(frozen=True, slots=True)
class _ReadFailed:
    code: BrokerErrorCode
    retryable: bool


type _ReadReport = _InfoReport | _Absent | _ReadFailed


def _attempt_report(info: TaskInfo) -> tuple[_AttemptReport, ...] | None:
    if info.attempts is None:
        return None
    return tuple(
        _AttemptReport(
            attempt=attempt.attempt,
            outcome=attempt.outcome.value,
            will_retry=attempt.will_retry,
            error_code=attempt.error_code,
            error_message=attempt.error_message,
            failed_reason=attempt.failed_reason,
            worker_id=attempt.worker_id,
            worker_hostname=attempt.worker_hostname,
            worker_pid=attempt.worker_pid,
            worker_process_name=attempt.worker_process_name,
        )
        for attempt in info.attempts
    )


def _report(result: BrokerResult[TaskInfo | None]) -> _ReadReport:
    match result:
        case Ok(None):
            return _Absent()
        case Ok(TaskInfo() as info):
            return _InfoReport(
                task_name=info.task_name,
                status=info.status.value,
                queue_name=info.queue_name,
                priority=info.priority,
                retry_count=info.retry_count,
                max_retries=info.max_retries,
                times_set=tuple(name for name in _TIME_FIELDS if getattr(info, name) is not None),
                worker_hostname=info.worker_hostname,
                worker_pid=info.worker_pid,
                worker_process_name=info.worker_process_name,
                error_code=info.error_code,
                failed_reason=info.failed_reason,
                raw_result=(
                    None if info.raw_result is None else json.dumps(info.raw_result, sort_keys=True)
                ),
                decoded=info.result_decoded,
                attempts=_attempt_report(info),
                execution=None if info.execution is None else info.execution.value,
            )
        case Err(failed):
            return _ReadFailed(code=failed.code, retryable=failed.retryable)
        case _:
            raise AssertionError(f'a task info read returned {result!r}')


# ---- Sides ----

type _InstallCall = Callable[[PostgresBroker], Awaitable[object]]
type _AsyncRead = Callable[..., Awaitable[BrokerResult[TaskInfo | None]]]
type _SyncRead = Callable[..., BrokerResult[TaskInfo | None]]

_SYNC_CASE: Final = 'info.failed'
"""The case that each side also reads with the blocking form, all parts on."""


async def _async_reads(
    broker: PostgresBroker, install: _InstallCall, read: _AsyncRead, seed: _Seed,
) -> dict[str, _ReadReport]:
    _ = await install(broker)
    report: dict[str, _ReadReport] = {}
    for case, task_id in seed.ids.items():
        report[f'{case} bare'] = _report(await read(broker, task_id))
        report[f'{case} all'] = _report(
            await read(
                broker,
                task_id,
                include_result=True,
                include_failed_reason=True,
                include_attempts=True,
            ),
        )
    return report


def _side(
    name: str, install: _InstallCall, read: _AsyncRead, read_blocking: _SyncRead,
) -> OperationSide[_Seed, dict[str, _ReadReport]]:
    def act(database: DualRunDatabase, seed: _Seed) -> dict[str, _ReadReport]:
        broker = PostgresBroker(PostgresConfig(database_url=SecretStr(database.url)))
        try:
            report = asyncio.run(_async_reads(broker, install, read, seed))
            report[f'{_SYNC_CASE} blocking'] = _report(
                read_blocking(
                    broker,
                    seed.ids[_SYNC_CASE],
                    include_result=True,
                    include_failed_reason=True,
                    include_attempts=True,
                ),
            )
        finally:
            _ = broker.close()
        return report

    return OperationSide(name=name, act=act)


async def _core_install(broker: PostgresBroker) -> object:
    return await broker.ensure_schema_initialized()


async def _core_read(
    broker: PostgresBroker, task_id: uuid.UUID, **parts: bool,
) -> BrokerResult[TaskInfo | None]:
    return await broker.get_task_info_async(task_id, **parts)


def _core_read_blocking(
    broker: PostgresBroker, task_id: uuid.UUID, **parts: bool,
) -> BrokerResult[TaskInfo | None]:
    return broker.get_task_info(task_id, **parts)


_PYTHON_CALLER: Final = _side(
    'Python caller on SQLAlchemy',
    install_caller.ensure_schema_initialized,
    task_info_caller.get_task_info_async,
    task_info_caller.get_task_info,
)
_CORE_CALLER: Final = _side(
    'CoreHandle.task_info_async and task_info', _core_install, _core_read, _core_read_blocking,
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


_EXPECTED_STATUS: Final = {
    'info.pending': 'PENDING',
    'info.claimed': 'CLAIMED',
    'info.running': 'RUNNING',
    'info.retried': 'PENDING',
    'info.completed': 'COMPLETED',
    'info.failed': 'FAILED',
    'info.cancelled': 'CANCELLED',
    'info.expired': 'EXPIRED',
}


def _status_of(report: _ReadReport) -> str | None:
    match report:
        case _InfoReport(status=status):
            return status
        case _Absent():
            return None
        case _ReadFailed() as failed:
            raise AssertionError(f'a read failed: {failed}')


def test_the_core_task_info_reads_what_the_python_read_read(empty_database: MakeDatabase) -> None:
    run = _pair(empty_database)

    assert run.diff.entries == (), render_diff(run.diff)
    reference = run.reference.report
    assert {
        case.rsplit(' ', 1)[0]: _status_of(report)
        for case, report in reference.items()
        if case.endswith(' bare')
    } == {**_EXPECTED_STATUS, 'absent.uuid7': None, 'absent.uuid4': None}
    failed = reference['info.failed all']
    assert isinstance(failed, _InfoReport)
    assert failed.attempts is not None
    assert [attempt.attempt for attempt in failed.attempts] == [2, 1]
    assert reference[f'{_SYNC_CASE} blocking'] == failed
    assert run.candidate.report == reference
