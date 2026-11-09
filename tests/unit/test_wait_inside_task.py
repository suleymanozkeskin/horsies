# pyright: reportPrivateUsage=false
"""Result waits return WAIT_INSIDE_TASK in a process that runs task bodies.

The broker is real and its URL is unreachable. The refusal returns before
any I/O, so no database is needed. With the refusal removed, a call goes to
the broker, and the nothing-ran and code assertions fail.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Generator
from typing import Any
from unittest.mock import patch

import pytest
from pydantic import SecretStr

from horsies.core import process_role as process_role_module
from horsies.core.app import Horsies
from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.models.app import AppConfig
from horsies.core.models.broker import PostgresConfig
from horsies.core.models.tasks import RetrievalCode, TaskError, TaskResult
from horsies.core.models.workflow.handle import WorkflowHandle
from horsies.core.process_role import (
    ProcessRole,
    mark_process_runs_task_bodies,
    process_role,
)
from horsies.core.task_decorator import TaskHandle
from horsies.core.types.result import Ok
from horsies.core.worker import child_runner

pytestmark = [pytest.mark.unit]

UNREACHABLE_URL = 'postgresql+psycopg://u:p@127.0.0.1:1/db'
WORKFLOW_ID = '0b6f2f0e-6c1d-4f7a-9a52-3d2b1c0e9f81'
TASK_ID = '0b6f2f0e-6c1d-7f7a-9a52-3d2b1c0e9f82'


@pytest.fixture
def app() -> Generator[Horsies, None, None]:
    """An app whose broker cannot connect. Stops the runner if a call started it."""
    app = Horsies(
        AppConfig(broker=PostgresConfig(database_url=SecretStr(UNREACHABLE_URL)))
    )
    yield app
    app.get_broker()._loop_runner.stop()


@pytest.fixture
def task_process(monkeypatch: pytest.MonkeyPatch) -> None:
    """Give this process the role of a worker child for one test."""
    monkeypatch.setattr(process_role_module, '_role', ProcessRole.RUNS_TASK_BODIES)


def _assert_nothing_ran(broker: PostgresBroker) -> None:
    """The sync runner never started and no resource set was built."""
    assert broker._loop_runner._started is False
    assert broker._loop_runner._thread is None
    assert broker._loop_sets == ()


def _handle(app: Horsies) -> TaskHandle[int]:
    return TaskHandle(TASK_ID, app=app, broker_mode=True, ok_type=int)


def _refused_code(result: TaskResult[Any, TaskError]) -> object:
    assert result.is_err()
    return result.unwrap_err().error_code


def _workflow_handle(app: Horsies) -> WorkflowHandle[Any]:
    return WorkflowHandle(workflow_id=WORKFLOW_ID, broker=app.get_broker())


def _task_get(app: Horsies) -> TaskResult[Any, TaskError]:
    return _handle(app).get(timeout_ms=0)


def _task_get_async(app: Horsies) -> TaskResult[Any, TaskError]:
    return asyncio.run(_handle(app).get_async(timeout_ms=0))


def _app_get_result(app: Horsies) -> TaskResult[Any, TaskError]:
    return app.get_result(TASK_ID, 0).unwrap()


def _app_get_result_async(app: Horsies) -> TaskResult[Any, TaskError]:
    return asyncio.run(app.get_result_async(TASK_ID, 0)).unwrap()


def _workflow_get(app: Horsies) -> TaskResult[Any, TaskError]:
    return _workflow_handle(app).get(timeout_ms=0)


def _workflow_get_async(app: Horsies) -> TaskResult[Any, TaskError]:
    return asyncio.run(_workflow_handle(app).get_async(timeout_ms=0))


WAITS: dict[str, Callable[[Horsies], TaskResult[Any, TaskError]]] = {
    'TaskHandle.get': _task_get,
    'TaskHandle.get_async': _task_get_async,
    'app.get_result': _app_get_result,
    'app.get_result_async': _app_get_result_async,
    'WorkflowHandle.get': _workflow_get,
    'WorkflowHandle.get_async': _workflow_get_async,
}


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
    result = WAITS[name](app)

    assert _refused_code(result) is RetrievalCode.WAIT_INSIDE_TASK
    assert 'workflow' in (result.unwrap_err().message or '')
    _assert_nothing_ran(app.get_broker())


@pytest.mark.usefixtures('task_process')
def test_a_wait_inside_a_running_loop_of_a_task_process_is_a_wait_inside_task(
    app: Horsies,
) -> None:
    async def inside() -> TaskResult[int, TaskError]:
        return _handle(app).get(timeout_ms=0)

    result = asyncio.run(inside())

    assert _refused_code(result) is RetrievalCode.WAIT_INSIDE_TASK


@pytest.mark.usefixtures('task_process')
def test_the_refusal_is_not_cached_on_the_handle(
    app: Horsies,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    handle = _handle(app)
    assert _refused_code(handle.get(timeout_ms=0)) is RetrievalCode.WAIT_INSIDE_TASK

    monkeypatch.setattr(process_role_module, '_role', ProcessRole.RUNS_NO_TASK_BODIES)
    with patch.object(
        app.get_broker(), 'get_raw_result_record', return_value=Ok(None)
    ) as read:
        outside = handle.get(timeout_ms=0)

    read.assert_called_once()
    assert _refused_code(outside) is RetrievalCode.TASK_NOT_FOUND


@pytest.mark.usefixtures('task_process')
def test_a_handle_that_holds_its_result_returns_it_in_a_task_process(
    app: Horsies,
) -> None:
    handle = _handle(app)
    handle.set_immediate_result(TaskResult(ok=7))

    result = handle.get(timeout_ms=0)

    assert result.unwrap() == 7
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
        patch.object(
            child_runner,
            '_initialize_worker_pool',
        ),
    ):
        child_runner.finish_child_start(app, UNREACHABLE_URL, None, 0, 2, True)

    assert roles_seen_by_hooks == [ProcessRole.RUNS_TASK_BODIES]
    assert process_role() is ProcessRole.RUNS_TASK_BODIES
