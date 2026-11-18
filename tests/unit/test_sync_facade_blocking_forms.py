# pyright: reportPrivateUsage=false
"""The sync facades call the core's blocking forms on the calling thread.

A sync facade and its async form share one outcome mapping. For each catalog
outcome of the answer that the facade reads, the sync facade returns the
outcome of its async form (``outcome_shape``), calls the blocking form once,
and never calls the broker's sync runner.
"""

from __future__ import annotations

import asyncio
import datetime
from collections.abc import Callable
from typing import Any, Final, TypeVar
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID

import pytest
from pydantic import SecretStr

from horsies._core import (
    BrokerFailure,
    CoreConnectionError,
    CoreStopped,
    DatabasePingRecord,
    HandleClosed,
    TooManyPendingCalls,
)
from horsies.core.app import Horsies
from horsies.core.brokers.postgres import (
    _SyncSendEngineNotOpen,
    _SyncSendEngineOpen,
)
from horsies.core.brokers.result_types import (
    BrokerErrorCode,
    BrokerOperationFailed,
    BrokerResult,
    RawResultRecord,
)
from horsies.core.codec.json_io import Json
from horsies.core.models.app import AppConfig
from horsies.core.models.broker import PostgresConfig
from horsies.core.models.tasks import TaskError, TaskInfo, TaskResult
from horsies.core.types.result import Err, Ok
from horsies.core.types.status import TaskStatus
from tests.broker_resources_support import resources_of
from tests.sync_facade_support import outcome_shape
from tests.unit.test_broker_postgres import _make_broker, _pinned

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
    def number(*, value: int) -> TaskResult[int, TaskError]:
        return TaskResult(ok=value)

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
    awaited = asyncio.run(app.get_result_async(TASK_ID, TIMEOUT_MS))

    assert outcome_shape(blocking) == outcome_shape(awaited)
    broker.get_raw_result_record.assert_called_once_with(TASK_ID, TIMEOUT_MS)
    broker.run_sync.assert_not_called()


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
    broker.run_sync.assert_not_called()


# =============================================================================
# Horsies.ping_database: CoreHandle.ping_database
# =============================================================================


def _failure(code: str) -> MagicMock:
    return MagicMock(
        spec=BrokerFailure,
        code=code,
        message=f'{code}: connection refused',
        retryable=True,
        exception=CONNECTION_LOST,
    )


PING_ANSWERS: Final[dict[str, object]] = {
    'pinged': MagicMock(spec=DatabasePingRecord, latency_ms=1.5),
    'ping_failed': _failure('DB_PING_FAILED'),
    'core_cannot_run_the_call': CORE_REFUSAL,
}


def _app_with_core_handle(handle: MagicMock) -> tuple[Horsies, MagicMock]:
    """An app whose broker calls ``handle``, and the broker's runner, a mock."""
    app = _app()
    broker = app.get_broker()
    runner = MagicMock()
    broker._core_handle = handle  # type: ignore[assignment]
    broker._loop_runner = runner
    return app, runner


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
    app, runner = _app_with_core_handle(handle)

    blocking = app.ping_database()
    awaited = asyncio.run(app.ping_database_async())

    assert outcome_shape(blocking) == outcome_shape(awaited)
    handle.ping_database.assert_called_once_with()
    runner.call.assert_not_called()


def test_ping_database_refuses_another_code_as_its_async_form_does() -> None:
    """A failure code outside the ping's catalog is a defect in both forms."""
    handle = _answering('ping_database', _failure('CLEANUP_FAILED'))
    app, _ = _app_with_core_handle(handle)

    with pytest.raises(AssertionError, match='CLEANUP_FAILED') as blocking:
        app.ping_database()
    with pytest.raises(AssertionError, match='CLEANUP_FAILED') as awaited:
        asyncio.run(app.ping_database_async())

    assert str(blocking.value) == str(awaited.value)


# =============================================================================
# PostgresBroker.close: CoreHandle.close
# =============================================================================

DISPOSE_FAILED: Final = OSError('dispose failed')


def _closing_broker(core_answer: object) -> Any:
    """A broker with mocked resource sets whose core handle answers the
    close with ``core_answer``: a record, or an exception that it raises."""
    broker = _make_broker()
    broker._core_handle = _answering('close', core_answer)
    return broker


def _with_closed_loop_set(broker: Any, dispose_error: BaseException | None) -> Any:
    """Build a resource set on an event loop that then closes."""
    match dispose_error:
        case None:
            pass
        case error:
            _pinned(broker).engine.dispose = AsyncMock(side_effect=error)
    asyncio.run(resources_of(broker))
    return broker


def _with_sync_send_engine(broker: Any, dispose_error: BaseException | None) -> Any:
    engine = MagicMock()
    match dispose_error:
        case None:
            engine.dispose = MagicMock(return_value=None)
        case error:
            engine.dispose = MagicMock(side_effect=error)
    broker._sync_send_engine = _SyncSendEngineOpen(engine=engine)
    return broker


CLOSE_CASES: Final[dict[str, Callable[[], Any]]] = {
    'nothing_open': lambda: _closing_broker(MagicMock(spec=HandleClosed)),
    'core_cannot_run_the_close': lambda: _closing_broker(
        CoreStopped('CoreHandle.close', 'a background task panicked'),
    ),
    'a_set_of_a_closed_loop': lambda: _with_closed_loop_set(
        _closing_broker(MagicMock(spec=HandleClosed)), None,
    ),
    'a_set_of_a_closed_loop_that_does_not_dispose': lambda: _with_closed_loop_set(
        _closing_broker(MagicMock(spec=HandleClosed)), DISPOSE_FAILED,
    ),
    'a_sync_send_engine': lambda: _with_sync_send_engine(
        _closing_broker(MagicMock(spec=HandleClosed)), None,
    ),
    'a_sync_send_engine_that_does_not_dispose': lambda: _with_sync_send_engine(
        _closing_broker(MagicMock(spec=HandleClosed)), DISPOSE_FAILED,
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
    for broker in (blocking_broker, awaited_broker):
        assert broker._loop_sets == ()
        assert isinstance(broker._sync_send_engine, _SyncSendEngineNotOpen)


def test_close_with_nothing_open_runs_no_event_loop(monkeypatch: pytest.MonkeyPatch) -> None:
    """With no resource set and no sync send engine, close runs no event loop."""
    broker = _closing_broker(MagicMock(spec=HandleClosed))

    def no_event_loop(coroutine: Any) -> None:
        coroutine.close()
        raise AssertionError('close ran an event loop with nothing to close on it')

    monkeypatch.setattr('horsies.core.brokers.postgres.asyncio.run', no_event_loop)

    assert broker.close() == Ok(None)
