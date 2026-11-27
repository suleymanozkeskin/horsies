# pyright: reportPrivateUsage=false
"""Result waits refuse at once in a process that runs task bodies.

The broker is real and its URL is unreachable; a mock that records the calls
wraps its core handle. The refusal comes right after the check of the wait
limit, which is the first check of each wait, so no database is needed. With a
refusal removed or moved after another check, the call reads the broker or
returns that check's code, and the nothing-ran, code and label assertions fail.
"""

from __future__ import annotations

import uuid

import asyncio
from collections.abc import Callable, Generator
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from pydantic import SecretStr

from horsies.core import process_role as process_role_module
from horsies.core.app import Horsies
from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.brokers.result_types import (
    DatabaseErrorCode,
    DatabaseInvalidInput,
    DatabaseResult,
    DatabaseWaitInsideTask,
)
from horsies.core.models.app import AppConfig
from horsies.core.models.broker import PostgresConfig
from horsies.core.models.outcome import (
    InvalidInput,
    ReadError,
    ReadResult,
    Succeeded,
    TaskNotFound,
    WaitInsideTask,
)
from horsies.core.models.tasks import TaskError
from horsies.core.models.workflow.handle import WorkflowHandle
from horsies.core.process_role import (
    ProcessRole,
    mark_process_runs_task_bodies,
    process_role,
)
from horsies.core.task_decorator import TaskHandle
from horsies.core.types.result import Err, Ok
from horsies.core.worker import child_runner
from tests.fixtures.core_frames import init_frame

pytestmark = [pytest.mark.unit]

UNREACHABLE_URL = 'postgresql://u:p@127.0.0.1:1/db'
WORKFLOW_ID = '0b6f2f0e-6c1d-4f7a-9a52-3d2b1c0e9f81'
TASK_ID = '0b6f2f0e-6c1d-7f7a-9a52-3d2b1c0e9f82'
VALID_TIMEOUT_MS = 0
"""A timeout that each wait accepts."""
REFUSED_TIMEOUT_MS = -1
"""A timeout that each wait refuses with INVALID_INPUT. The wait limit is the
first check, so a task process gets INVALID_INPUT for it, not WAIT_INSIDE_TASK."""


@pytest.fixture
def app() -> Generator[Horsies, None, None]:
    """An app whose broker cannot connect, and whose core handle records each
    call it forwards."""
    app = Horsies(
        AppConfig(database=PostgresConfig(database_url=SecretStr(UNREACHABLE_URL)))
    )
    broker = app.get_broker()
    broker._core_handle = MagicMock(wraps=broker._core_handle)
    yield app


@pytest.fixture
def task_process(monkeypatch: pytest.MonkeyPatch) -> None:
    """Give this process the role of a worker child for one test."""
    monkeypatch.setattr(process_role_module, '_role', ProcessRole.RUNS_TASK_BODIES)


def _assert_nothing_ran(broker: PostgresBroker) -> None:
    """No core call ran."""
    core_handle = broker._core_handle
    assert isinstance(core_handle, MagicMock), 'the app fixture wraps the core handle'
    assert core_handle.mock_calls == []


def _handle(app: Horsies) -> TaskHandle[int]:
    return TaskHandle(TASK_ID, app=app, broker_mode=True, ok_type=int)


def _read_error(result: ReadResult[Any, TaskError]) -> ReadError:
    assert isinstance(result, Err), f'expected a read failure, got {result!r}'
    return result.err_value


def _workflow_handle(app: Horsies) -> WorkflowHandle[Any]:
    return WorkflowHandle(workflow_id=uuid.UUID(WORKFLOW_ID), broker=app.get_broker())


def _task_get(app: Horsies, timeout_ms: int | None) -> ReadResult[Any, TaskError]:
    return _handle(app).get(timeout_ms=timeout_ms)


def _task_get_async(app: Horsies, timeout_ms: int | None) -> ReadResult[Any, TaskError]:
    return asyncio.run(_handle(app).get_async(timeout_ms=timeout_ms))


def _app_get_result(app: Horsies, timeout_ms: int | None) -> ReadResult[Any, TaskError]:
    return app.get_result(uuid.UUID(TASK_ID), timeout_ms)


def _app_get_result_async(
    app: Horsies, timeout_ms: int | None,
) -> ReadResult[Any, TaskError]:
    return asyncio.run(app.get_result_async(uuid.UUID(TASK_ID), timeout_ms))


def _workflow_get(app: Horsies, timeout_ms: int | None) -> ReadResult[Any, TaskError]:
    return _workflow_handle(app).get(timeout_ms=timeout_ms)


def _workflow_get_async(
    app: Horsies, timeout_ms: int | None,
) -> ReadResult[Any, TaskError]:
    return asyncio.run(_workflow_handle(app).get_async(timeout_ms=timeout_ms))


WAITS: dict[str, Callable[[Horsies, int | None], ReadResult[Any, TaskError]]] = {
    'TaskHandle.get': _task_get,
    'TaskHandle.get_async': _task_get_async,
    'app.get_result': _app_get_result,
    'app.get_result_async': _app_get_result_async,
    'WorkflowHandle.get': _workflow_get,
    'WorkflowHandle.get_async': _workflow_get_async,
}
SYNC_WAITS = ('TaskHandle.get', 'app.get_result', 'WorkflowHandle.get')


def _app_get_raw_result(app: Horsies, timeout_ms: int | None) -> DatabaseResult[Any]:
    return app.get_raw_result(TASK_ID, timeout_ms)


def _app_get_raw_result_async(
    app: Horsies, timeout_ms: int | None,
) -> DatabaseResult[Any]:
    return asyncio.run(app.get_raw_result_async(TASK_ID, timeout_ms))


def _broker_raw_record(app: Horsies, timeout_ms: int | None) -> DatabaseResult[Any]:
    return app.get_broker().get_raw_result_record(TASK_ID, timeout_ms)


def _broker_raw_record_async(
    app: Horsies, timeout_ms: int | None,
) -> DatabaseResult[Any]:
    return asyncio.run(app.get_broker().get_raw_result_record_async(TASK_ID, timeout_ms))


RAW_WAITS: dict[str, Callable[[Horsies, int | None], DatabaseResult[Any]]] = {
    'app.get_raw_result': _app_get_raw_result,
    'app.get_raw_result_async': _app_get_raw_result_async,
    'PostgresBroker.get_raw_result_record': _broker_raw_record,
    'PostgresBroker.get_raw_result_record_async': _broker_raw_record_async,
}
"""Keyed by the operation that the refusal message names."""
SYNC_RAW_WAITS = ('app.get_raw_result', 'PostgresBroker.get_raw_result_record')


def _assert_broker_refusal(result: DatabaseResult[Any], operation: str) -> None:
    assert isinstance(result, Err)
    error = result.err_value
    assert isinstance(error, DatabaseWaitInsideTask)
    assert error.code is DatabaseErrorCode.WAIT_INSIDE_TASK
    assert f'{operation}() for task {TASK_ID}' in error.message
    assert 'workflow' in error.message


def test_a_new_process_runs_no_task_bodies() -> None:
    assert process_role() is ProcessRole.RUNS_NO_TASK_BODIES


def test_marking_a_process_records_that_it_runs_task_bodies(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(process_role_module, '_role', ProcessRole.RUNS_NO_TASK_BODIES)

    mark_process_runs_task_bodies()

    assert process_role() is ProcessRole.RUNS_TASK_BODIES


@pytest.mark.parametrize('name', sorted(WAITS))
@pytest.mark.usefixtures('task_process')
def test_a_wait_in_a_task_process_returns_wait_inside_task_and_reads_nothing(
    app: Horsies,
    name: str,
) -> None:
    result = WAITS[name](app, VALID_TIMEOUT_MS)

    error = _read_error(result)
    assert isinstance(error, WaitInsideTask)
    assert 'workflow' in error.message
    _assert_nothing_ran(app.get_broker())


@pytest.mark.parametrize('name', sorted(WAITS))
@pytest.mark.usefixtures('task_process')
def test_a_refused_timeout_in_a_task_process_returns_invalid_input_first(
    app: Horsies,
    name: str,
) -> None:
    """The wait limit is checked before the process role."""
    result = WAITS[name](app, REFUSED_TIMEOUT_MS)

    assert isinstance(_read_error(result), InvalidInput)
    _assert_nothing_ran(app.get_broker())


@pytest.mark.parametrize('name', sorted(RAW_WAITS))
@pytest.mark.usefixtures('task_process')
def test_a_raw_wait_in_a_task_process_returns_the_broker_refusal_and_reads_nothing(
    app: Horsies,
    name: str,
) -> None:
    result = RAW_WAITS[name](app, VALID_TIMEOUT_MS)

    _assert_broker_refusal(result, name)
    _assert_nothing_ran(app.get_broker())


@pytest.mark.parametrize('name', sorted(RAW_WAITS))
@pytest.mark.usefixtures('task_process')
def test_a_refused_timeout_of_a_raw_wait_in_a_task_process_returns_invalid_input_first(
    app: Horsies,
    name: str,
) -> None:
    """The wait limit is checked before the process role."""
    result = RAW_WAITS[name](app, REFUSED_TIMEOUT_MS)

    assert isinstance(result, Err)
    assert isinstance(result.err_value, DatabaseInvalidInput)
    assert result.err_value.code is DatabaseErrorCode.INVALID_INPUT
    _assert_nothing_ran(app.get_broker())


@pytest.mark.parametrize('name', SYNC_WAITS)
@pytest.mark.usefixtures('task_process')
def test_a_sync_wait_inside_a_running_loop_of_a_task_process_is_a_wait_inside_task(
    app: Horsies,
    name: str,
) -> None:
    async def inside() -> ReadResult[Any, TaskError]:
        return WAITS[name](app, VALID_TIMEOUT_MS)

    result = asyncio.run(inside())

    assert isinstance(_read_error(result), WaitInsideTask)


@pytest.mark.parametrize('name', SYNC_RAW_WAITS)
@pytest.mark.usefixtures('task_process')
def test_a_sync_raw_wait_inside_a_running_loop_of_a_task_process_is_refused(
    app: Horsies,
    name: str,
) -> None:
    async def inside() -> DatabaseResult[Any]:
        return RAW_WAITS[name](app, VALID_TIMEOUT_MS)

    result = asyncio.run(inside())

    _assert_broker_refusal(result, name)


@pytest.mark.usefixtures('task_process')
def test_the_refusal_is_not_cached_on_the_handle(
    app: Horsies,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    handle = _handle(app)
    assert isinstance(_read_error(handle.get(timeout_ms=0)), WaitInsideTask)

    monkeypatch.setattr(process_role_module, '_role', ProcessRole.RUNS_NO_TASK_BODIES)
    with patch.object(
        app.get_broker(), 'get_raw_result_record', return_value=Ok(None)
    ) as read:
        outside = handle.get(timeout_ms=0)

    read.assert_called_once()
    assert isinstance(_read_error(outside), TaskNotFound)


@pytest.mark.usefixtures('task_process')
def test_a_handle_without_a_broker_returns_its_result_in_a_task_process(
    app: Horsies,
) -> None:
    """Such a handle holds its result and does not wait, so it is not refused."""
    handle: TaskHandle[int] = TaskHandle(TASK_ID, app=app, broker_mode=False, ok_type=int)
    handle.set_immediate_result(Ok(7))

    result = handle.get(timeout_ms=0)

    assert result == Ok(Succeeded(7))
    _assert_nothing_ran(app.get_broker())


@pytest.mark.usefixtures('task_process')
def test_info_is_not_a_wait_and_reads_the_task_in_a_task_process(app: Horsies) -> None:
    with patch.object(app, 'get_task_info', return_value=Ok(None)) as read:
        result = _handle(app).info()

    read.assert_called_once()
    assert result == Ok(None)


def test_a_child_start_marks_its_process_before_the_start_hooks(
    app: Horsies,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(process_role_module, '_role', ProcessRole.RUNS_NO_TASK_BODIES)
    roles_seen_by_hooks: list[ProcessRole] = []

    def hooks(_app: Horsies) -> None:
        roles_seen_by_hooks.append(process_role())

    with (
        patch.object(child_runner, '_run_child_start_hooks', side_effect=hooks),
        patch.object(child_runner, '_initialize_child_core_handle'),
    ):
        child_runner.finish_child_start(app, init_frame())

    assert roles_seen_by_hooks == [ProcessRole.RUNS_TASK_BODIES]
    assert process_role() is ProcessRole.RUNS_TASK_BODIES
