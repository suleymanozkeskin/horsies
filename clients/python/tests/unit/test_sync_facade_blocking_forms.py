# pyright: reportPrivateUsage=false
"""The sync facades call the core's blocking forms on the calling thread.

A sync facade and its async form share one outcome mapping. For each catalog
outcome of the answer that the facade reads, the sync facade returns the
outcome of its async form (``outcome_shape``), calls the blocking form once,
and never calls the awaitable form.
"""

from __future__ import annotations

import asyncio
import datetime
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import timedelta
from typing import Any, Final, TypeVar
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID

import pytest
from pydantic import SecretStr

from horsies._catalog.error_detail import NoCause
from horsies._core import (
    AllSlotsServingRecord,
    BudgetLoadRecord,
    ExecutionClassCoverageRecord,
    ExecutionClassReportRecord,
    CoreConnectionError,
    RuntimeStartFailed,
    DatabasePingRecord,
    ExpiredTaskListComplete,
    ExpiredTaskListTruncated,
    HandleClosed,
    OldestWaitingRecord,
    PendingStreamRecord,
    StaleTaskListComplete,
    StaleTaskListTruncated,
    StreamCoverageRecord,
    StreamServedRecord,
    StreamUnservedRecord,
    TooManyPendingCalls,
    WorkerExecutionClassLoadRecord,
    WorkerPongRecord,
    WorkerPongsRecord,
    WorkerStateAbsent,
    WorkerStatesRecord,
)
from horsies.core.app import Horsies
from horsies.core.brokers.result_types import (
    BrokerErrorCode,
    BrokerInvalidInput,
    BrokerListenerDisabled,
    BrokerOperationFailed,
    BrokerOperationFailedCode,
    BrokerResult,
    RawResultRecord,
)
from horsies.core.codec.json_io import Json
from horsies.core.models.app import AppConfig
from horsies.core.models.broker import PostgresConfig
from horsies.core.models.health_reads import (
    MinResponsesAtLeast,
    PingTargetWorker,
    WorkerHistoryLimitNewest,
)
from horsies.core.models.tasks import TaskError, TaskInfo
from horsies.core.types.result import Err, Ok, Result
from horsies.core.types.status import TaskStatus
from tests.sync_facade_support import outcome_shape
from tests.unit.test_broker_postgres import _make_broker
from tests.unit.test_broker_worker_reads_in_core import (
    _SLOTS_DOWN_RECORD,
    _record as _core_record,
    _state_record,
)

pytestmark = [pytest.mark.unit]

UNREACHABLE_URL: Final = 'postgresql://u:p@127.0.0.1:1/db'
TASK_ID: Final = UUID('0198c0de-0000-7000-8000-0000000000cd')
TIMEOUT_MS: Final = 250
REGISTERED_TASK: Final = 'sync_facade.number'
UNREGISTERED_TASK: Final = 'sync_facade.not_registered'
CONNECTION_LOST: Final = CoreConnectionError('task_info', 'connection lost', True)
CORE_REFUSAL: Final = TooManyPendingCalls('CoreHandle.call', 65_536)
_AT: Final = datetime.datetime(2026, 11, 18, 12, 0, tzinfo=datetime.UTC)


_T = TypeVar('_T')


def _returning(make_answer: Callable[[], _T]) -> Callable[..., _T]:
    """A side effect that answers each call with a new ``make_answer()``."""

    def answer(*_arguments: object, **_keywords: object) -> _T:
        return make_answer()

    return answer


def _app() -> Horsies:
    """An app whose broker cannot connect, with one task whose ok type is int."""
    app = Horsies(AppConfig(broker=PostgresConfig(database_url=SecretStr(UNREACHABLE_URL))))

    @app.task(REGISTERED_TASK)
    def number(*, value: int) -> Result[int, TaskError]:
        return Ok(value)

    _ = number
    return app


def _app_reading(broker: MagicMock) -> Horsies:
    app = _app()
    app.get_broker = MagicMock(return_value=broker)  # type: ignore[method-assign]
    return app


def _err_envelope() -> dict[str, Json]:
    return {
        '__h_task_result__': True,
        'ok': None,
        'err': {
            'error_code': {'__builtin_task_code__': 'TASK_EXCEPTION'},
            'message': 'boom',
            'data': None,
            'exception': None,
        },
    }


def _ok_envelope(value: Json) -> dict[str, Json]:
    return {'__h_task_result__': True, 'ok': value, 'err': None}


def _malformed_envelope() -> dict[str, Json]:
    """The marker and an err slot, with no ok slot."""
    return {
        '__h_task_result__': True,
        'err': {
            'error_code': {'__builtin_task_code__': 'BROKER_ERROR'},
            'message': 'smuggled',
            'data': None,
            'exception': None,
        },
    }


def _undecodable_err_envelope() -> dict[str, Json]:
    """An err slot whose code is no built-in code."""
    return {
        '__h_task_result__': True,
        'ok': None,
        'err': {
            'error_code': {'__builtin_task_code__': 'NO_SUCH_CODE'},
            'message': 'boom',
            'data': None,
            'exception': None,
        },
    }


def _read_failed() -> BrokerResult[Any]:
    return Err(
        BrokerOperationFailed(
            code=BrokerErrorCode.TASK_INFO_QUERY_FAILED,
            message='connection lost',
            retryable=True,
            exception=CONNECTION_LOST,
        ),
    )


def _record(
    status: TaskStatus, raw_result: dict[str, Json] | None, task_name: str,
) -> RawResultRecord:
    return RawResultRecord(
        task_id=TASK_ID,
        task_name=task_name,
        status=status,
        raw_result=raw_result,
    )


# =============================================================================
# Horsies.get_result: PostgresBroker.get_raw_result_record
# =============================================================================

RAW_RESULT_ANSWERS: Final[dict[str, Callable[[], BrokerResult[RawResultRecord | None]]]] = {
    'read_failed': _read_failed,
    'task_absent': lambda: Ok(None),
    'cancelled': lambda: Ok(_record(TaskStatus.CANCELLED, None, REGISTERED_TASK)),
    'terminal_without_payload': lambda: Ok(
        _record(TaskStatus.COMPLETED, None, REGISTERED_TASK),
    ),
    'not_finished_at_the_limit': lambda: Ok(
        _record(TaskStatus.RUNNING, None, REGISTERED_TASK),
    ),
    'malformed_envelope': lambda: Ok(
        _record(TaskStatus.FAILED, _malformed_envelope(), REGISTERED_TASK),
    ),
    'err_slot': lambda: Ok(_record(TaskStatus.FAILED, _err_envelope(), UNREGISTERED_TASK)),
    'err_slot_that_does_not_decode': lambda: Ok(
        _record(TaskStatus.FAILED, _undecodable_err_envelope(), REGISTERED_TASK),
    ),
    'ok_slot_of_an_unregistered_task': lambda: Ok(
        _record(TaskStatus.COMPLETED, _ok_envelope(5), UNREGISTERED_TASK),
    ),
    'ok_slot': lambda: Ok(_record(TaskStatus.COMPLETED, _ok_envelope(5), REGISTERED_TASK)),
    'ok_slot_that_does_not_decode': lambda: Ok(
        _record(TaskStatus.COMPLETED, _ok_envelope('five'), REGISTERED_TASK),
    ),
}


@pytest.mark.parametrize('answer', list(RAW_RESULT_ANSWERS))
def test_get_result_gives_the_outcome_of_get_result_async(answer: str) -> None:
    make_answer = RAW_RESULT_ANSWERS[answer]
    broker = MagicMock()
    broker.get_raw_result_record = MagicMock(side_effect=_returning(make_answer))
    broker.get_raw_result_record_async = AsyncMock(side_effect=_returning(make_answer))
    app = _app_reading(broker)

    blocking = app.get_result(TASK_ID, TIMEOUT_MS)
    broker.get_raw_result_record_async.assert_not_called()
    awaited = asyncio.run(app.get_result_async(TASK_ID, TIMEOUT_MS))

    assert outcome_shape(blocking) == outcome_shape(awaited)
    broker.get_raw_result_record.assert_called_once_with(TASK_ID, TIMEOUT_MS)


# =============================================================================
# Horsies.get_task_info: PostgresBroker.get_task_info
# =============================================================================


def _task_info(task_name: str, raw_result: dict[str, Json] | None) -> TaskInfo:
    return TaskInfo(
        task_id=TASK_ID,
        task_name=task_name,
        status=TaskStatus.FAILED,
        queue_name='default',
        priority=0,
        retry_count=0,
        max_retries=0,
        next_retry_at=None,
        sent_at=_AT,
        enqueued_at=_AT,
        claimed_at=_AT,
        started_at=_AT,
        completed_at=None,
        failed_at=_AT,
        worker_hostname=None,
        worker_pid=None,
        worker_process_name=None,
        raw_result=raw_result,
    )


TASK_INFO_ANSWERS: Final[dict[str, Callable[[], BrokerResult[TaskInfo | None]]]] = {
    'read_failed': _read_failed,
    'task_absent': lambda: Ok(None),
    'no_stored_result': lambda: Ok(_task_info(REGISTERED_TASK, None)),
    'malformed_envelope': lambda: Ok(_task_info(REGISTERED_TASK, _malformed_envelope())),
    'err_slot': lambda: Ok(_task_info(UNREGISTERED_TASK, _err_envelope())),
    'err_slot_that_does_not_decode': lambda: Ok(
        _task_info(REGISTERED_TASK, _undecodable_err_envelope()),
    ),
    'ok_slot_of_an_unregistered_task': lambda: Ok(
        _task_info(UNREGISTERED_TASK, _ok_envelope(5)),
    ),
    'ok_slot': lambda: Ok(_task_info(REGISTERED_TASK, _ok_envelope(5))),
    'ok_slot_that_does_not_decode': lambda: Ok(
        _task_info(REGISTERED_TASK, _ok_envelope('five')),
    ),
}


@pytest.mark.parametrize('include_result', [True, False])
@pytest.mark.parametrize('answer', list(TASK_INFO_ANSWERS))
def test_get_task_info_gives_the_outcome_of_get_task_info_async(
    answer: str, include_result: bool,
) -> None:
    make_answer = TASK_INFO_ANSWERS[answer]
    broker = MagicMock()
    broker.get_task_info = MagicMock(side_effect=_returning(make_answer))
    broker.get_task_info_async = AsyncMock(side_effect=_returning(make_answer))
    app = _app_reading(broker)

    blocking = app.get_task_info(
        TASK_ID, include_result=include_result, include_attempts=True,
    )
    broker.get_task_info_async.assert_not_called()
    awaited = asyncio.run(
        app.get_task_info_async(TASK_ID, include_result=include_result, include_attempts=True),
    )

    assert outcome_shape(blocking) == outcome_shape(awaited)
    broker.get_task_info.assert_called_once_with(
        TASK_ID,
        include_result=include_result,
        include_failed_reason=False,
        include_attempts=True,
    )


# =============================================================================
# Horsies.ping_database: CoreHandle.ping_database
# =============================================================================


def _failure(code: BrokerOperationFailedCode) -> BrokerOperationFailed:
    """The core's class of a failed operation of ``code``."""
    return BrokerOperationFailed(
        code=code,
        message=f'{code}: connection refused',
        retryable=True,
        exception=CONNECTION_LOST,
    )


PING_ANSWERS: Final[dict[str, object]] = {
    'pinged': MagicMock(spec=DatabasePingRecord, latency_ms=1.5),
    'ping_failed': _failure(BrokerErrorCode.DB_PING_FAILED),
    'core_cannot_run_the_call': CORE_REFUSAL,
}


def _app_with_core_handle(handle: MagicMock) -> Horsies:
    """An app whose broker calls ``handle``."""
    app = _app()
    broker = app.get_broker()
    broker._core_handle = handle  # type: ignore[assignment]
    return app


def _answering(method: str, answer: object) -> MagicMock:
    """A core handle whose blocking ``method`` and its awaitable form answer
    ``answer``: a record, or an exception that the call raises."""
    handle = MagicMock()
    match answer:
        case BaseException():
            setattr(handle, method, MagicMock(side_effect=answer))
            setattr(handle, f'{method}_async', AsyncMock(side_effect=answer))
        case _:
            setattr(handle, method, MagicMock(return_value=answer))
            setattr(handle, f'{method}_async', AsyncMock(return_value=answer))
    return handle


@pytest.mark.parametrize('answer', list(PING_ANSWERS))
def test_ping_database_gives_the_outcome_of_ping_database_async(answer: str) -> None:
    handle = _answering('ping_database', PING_ANSWERS[answer])
    app = _app_with_core_handle(handle)

    blocking = app.ping_database()
    handle.ping_database_async.assert_not_called()
    awaited = asyncio.run(app.ping_database_async())

    assert outcome_shape(blocking) == outcome_shape(awaited)
    handle.ping_database.assert_called_once_with()


def test_ping_database_refuses_another_code_as_its_async_form_does() -> None:
    """A failure code outside the ping's catalog is a defect in both forms."""
    handle = _answering('ping_database', _failure(BrokerErrorCode.CLEANUP_FAILED))
    app = _app_with_core_handle(handle)

    with pytest.raises(AssertionError, match='CLEANUP_FAILED') as blocking:
        app.ping_database()
    with pytest.raises(AssertionError, match='CLEANUP_FAILED') as awaited:
        asyncio.run(app.ping_database_async())

    assert str(blocking.value) == str(awaited.value)


# =============================================================================
# PostgresBroker.close: CoreHandle.close
# =============================================================================


def _closing_broker(core_answer: object) -> Any:
    """A broker whose core handle answers the close with ``core_answer``: a
    record, or an exception that it raises."""
    broker = _make_broker()
    broker._core_handle = _answering('close', core_answer)
    return broker


CLOSE_CASES: Final[dict[str, Callable[[], Any]]] = {
    'closed': lambda: _closing_broker(MagicMock(spec=HandleClosed)),
    'core_cannot_run_the_close': lambda: _closing_broker(
        RuntimeStartFailed('the core runtime could not start'),
    ),
}


@pytest.mark.parametrize('case', list(CLOSE_CASES))
def test_close_gives_the_outcome_of_close_async(case: str) -> None:
    blocking_broker = CLOSE_CASES[case]()
    awaited_broker = CLOSE_CASES[case]()

    blocking = blocking_broker.close()
    awaited = asyncio.run(awaited_broker.close_async())

    assert outcome_shape(blocking) == outcome_shape(awaited)
    blocking_broker._core_handle.close.assert_called_once_with()
    blocking_broker._core_handle.close_async.assert_not_called()


def test_close_with_nothing_open_runs_no_event_loop(monkeypatch: pytest.MonkeyPatch) -> None:
    """close runs no event loop: the core's blocking close runs on this thread."""
    broker = _closing_broker(MagicMock(spec=HandleClosed))

    def no_event_loop(coroutine: Any) -> None:
        coroutine.close()
        raise AssertionError('close ran an event loop')

    monkeypatch.setattr('horsies.core.brokers.postgres.asyncio.run', no_event_loop)

    assert broker.close() == Ok(None)


# =============================================================================
# Horsies monitoring reads and the worker ping: the CoreHandle blocking forms
# =============================================================================


def _refused_input() -> BrokerInvalidInput:
    """The core's class of an argument that a check refused."""
    return BrokerInvalidInput(message='INVALID_INPUT: refused', exception=NoCause())


def _listener_disabled() -> BrokerListenerDisabled:
    """The core's class of a ping on a broker with no listener."""
    return BrokerListenerDisabled(message='LISTENER_DISABLED: no listener')


_STATES: Final = _core_record(
    WorkerStatesRecord,
    states=[
        _state_record('w1', _core_record(AllSlotsServingRecord)),
        _state_record('w2', _SLOTS_DOWN_RECORD),
    ],
)
_COVERAGE: Final = _core_record(
    ExecutionClassCoverageRecord,
    checked_at=_AT,
    live_workers=1,
    streams=[
        _core_record(
            StreamCoverageRecord,
            queue='a',
            execution='ISOLATED',
            coverage=_core_record(StreamServedRecord, workers=1, capacity=4),
        ),
    ],
)
_REPORT: Final = _core_record(
    ExecutionClassReportRecord,
    checked_at=_AT,
    live_window=timedelta(seconds=120),
    streams=[
        _core_record(
            PendingStreamRecord,
            queue='a',
            execution='ISOLATED',
            coverage=_core_record(StreamUnservedRecord),
            wait=_core_record(OldestWaitingRecord, oldest_due_at=_AT, wait=timedelta(seconds=90)),
        ),
    ],
    workers=[
        _core_record(
            WorkerExecutionClassLoadRecord,
            worker_id='w1',
            snapshot_at=_AT,
            budgets=[
                _core_record(
                    BudgetLoadRecord,
                    budget='ISOLATED',
                    capacity=4,
                    serves=['ISOLATED'],
                    in_flight=1,
                ),
            ],
        ),
    ],
)


@dataclass(frozen=True)
class MonitoringFacade:
    """A sync facade of ``Horsies``, its async form, the blocking form of the
    core handle that it calls, with the arguments it passes, and each answer
    of its catalog."""

    blocking: Callable[[Horsies], object]
    awaited: Callable[[Horsies], Awaitable[object]]
    blocking_form: str
    arguments: tuple[object, ...]
    keywords: dict[str, object]
    answers: dict[str, object]


MONITORING_FACADES: Final[dict[str, MonitoringFacade]] = {
    'ping_workers': MonitoringFacade(
        blocking=lambda app: app.ping_workers(
            target=PingTargetWorker(worker_id='w1'),
            wait_ms=500,
            min_responses=MinResponsesAtLeast(count=1),
        ),
        awaited=lambda app: app.ping_workers_async(
            target=PingTargetWorker(worker_id='w1'),
            wait_ms=500,
            min_responses=MinResponsesAtLeast(count=1),
        ),
        blocking_form='ping_workers',
        arguments=(),
        keywords={'target_worker_id': 'w1', 'wait_ms': 500, 'min_responses': 1},
        answers={
            'pongs': _core_record(
                WorkerPongsRecord,
                pongs=[
                    _core_record(
                        WorkerPongRecord,
                        worker_id='w1',
                        hostname='host-1',
                        pid=101,
                        round_trip_ms=1.25,
                    ),
                ],
            ),
            'invalid_input': _refused_input(),
            'listener_disabled': _listener_disabled(),
            'ping_failed': _failure(BrokerErrorCode.WORKER_PING_FAILED),
            'core_cannot_run_the_call': CORE_REFUSAL,
        },
    ),
    'list_worker_states': MonitoringFacade(
        blocking=lambda app: app.list_worker_states(),
        awaited=lambda app: app.list_worker_states_async(),
        blocking_form='worker_states',
        arguments=(),
        keywords={},
        answers={
            'states': _STATES,
            'read_failed': _failure(BrokerErrorCode.MONITORING_QUERY_FAILED),
            'core_cannot_run_the_call': CORE_REFUSAL,
        },
    ),
    'get_worker_state': MonitoringFacade(
        blocking=lambda app: app.get_worker_state('w1'),
        awaited=lambda app: app.get_worker_state_async('w1'),
        blocking_form='worker_state',
        arguments=('w1',),
        keywords={},
        answers={
            'state': _state_record('w1', _SLOTS_DOWN_RECORD),
            'absent': _core_record(WorkerStateAbsent),
            'read_failed': _failure(BrokerErrorCode.MONITORING_QUERY_FAILED),
            'core_cannot_run_the_call': CORE_REFUSAL,
        },
    ),
    'get_worker_state_history': MonitoringFacade(
        blocking=lambda app: app.get_worker_state_history(
            'w1', limit=WorkerHistoryLimitNewest(count=5),
        ),
        awaited=lambda app: app.get_worker_state_history_async(
            'w1', limit=WorkerHistoryLimitNewest(count=5),
        ),
        blocking_form='worker_state_history',
        arguments=('w1',),
        keywords={'limit': 5},
        answers={
            'states': _STATES,
            'invalid_input': _refused_input(),
            'read_failed': _failure(BrokerErrorCode.MONITORING_QUERY_FAILED),
            'core_cannot_run_the_call': CORE_REFUSAL,
        },
    ),
    'get_execution_class_coverage': MonitoringFacade(
        blocking=lambda app: app.get_execution_class_coverage(),
        awaited=lambda app: app.get_execution_class_coverage_async(),
        blocking_form='execution_class_coverage',
        arguments=(),
        keywords={},
        answers={
            'coverage': _COVERAGE,
            'read_failed': _failure(BrokerErrorCode.MONITORING_QUERY_FAILED),
            'core_cannot_run_the_call': CORE_REFUSAL,
        },
    ),
    'get_execution_class_report': MonitoringFacade(
        blocking=lambda app: app.get_execution_class_report(),
        awaited=lambda app: app.get_execution_class_report_async(),
        blocking_form='execution_class_report',
        arguments=(),
        keywords={},
        answers={
            'report': _REPORT,
            'read_failed': _failure(BrokerErrorCode.MONITORING_QUERY_FAILED),
            'core_cannot_run_the_call': CORE_REFUSAL,
        },
    ),
    'get_stale_tasks': MonitoringFacade(
        blocking=lambda app: app.get_stale_tasks(7),
        awaited=lambda app: app.get_stale_tasks_async(7),
        blocking_form='stale_tasks',
        arguments=(7,),
        keywords={},
        answers={
            'complete': _core_record(StaleTaskListComplete, rows=[]),
            'truncated': _core_record(StaleTaskListTruncated, rows=[], bound=10_000),
            'invalid_input': _refused_input(),
            'read_failed': _failure(BrokerErrorCode.MONITORING_QUERY_FAILED),
            'core_cannot_run_the_call': CORE_REFUSAL,
        },
    ),
    'get_expired_tasks': MonitoringFacade(
        blocking=lambda app: app.get_expired_tasks(),
        awaited=lambda app: app.get_expired_tasks_async(),
        blocking_form='expired_tasks',
        arguments=(),
        keywords={},
        answers={
            'complete': _core_record(ExpiredTaskListComplete, rows=[]),
            'truncated': _core_record(ExpiredTaskListTruncated, rows=[], bound=10_000),
            'read_failed': _failure(BrokerErrorCode.MONITORING_QUERY_FAILED),
            'core_cannot_run_the_call': CORE_REFUSAL,
        },
    ),
}

MONITORING_CASES: Final = [
    (facade, answer)
    for facade, entry in MONITORING_FACADES.items()
    for answer in entry.answers
]


@pytest.mark.parametrize(
    ('facade', 'answer'), MONITORING_CASES, ids=[f'{f}-{a}' for f, a in MONITORING_CASES],
)
def test_a_monitoring_facade_gives_the_outcome_of_its_async_form(
    facade: str, answer: str,
) -> None:
    entry = MONITORING_FACADES[facade]
    handle = _answering(entry.blocking_form, entry.answers[answer])
    app = _app_with_core_handle(handle)

    blocking = entry.blocking(app)
    getattr(handle, f'{entry.blocking_form}_async').assert_not_called()
    awaited = asyncio.run(_awaited(entry.awaited(app)))

    assert outcome_shape(blocking) == outcome_shape(awaited)
    getattr(handle, entry.blocking_form).assert_called_once_with(
        *entry.arguments, **entry.keywords,
    )


@pytest.mark.parametrize('facade', list(MONITORING_FACADES))
def test_a_monitoring_facade_refuses_another_code_as_its_async_form_does(facade: str) -> None:
    """A failure code outside the facade's catalog is a defect in both forms."""
    entry = MONITORING_FACADES[facade]
    handle = _answering(entry.blocking_form, _failure(BrokerErrorCode.CLEANUP_FAILED))
    app = _app_with_core_handle(handle)

    with pytest.raises(AssertionError, match='CLEANUP_FAILED') as blocking:
        entry.blocking(app)
    with pytest.raises(AssertionError, match='CLEANUP_FAILED') as awaited:
        asyncio.run(_awaited(entry.awaited(app)))

    assert str(blocking.value) == str(awaited.value)


async def _awaited(awaitable: Awaitable[object]) -> object:
    return await awaitable
