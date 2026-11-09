"""Registered `async def` tasks run by the child runner.

`_run_task_entry` runs real registered tasks here. Only the database steps
around the body are replaced: the RUNNING update, the heartbeat thread and
the finalizing mark. Each test gets its own task loop, as a child gets one.
"""

# pyright: reportPrivateUsage=false

from __future__ import annotations

import asyncio
import json
from collections.abc import Callable, Iterator
from contextlib import ExitStack
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from pydantic import SecretStr

from horsies.core.app import Horsies
from horsies.core.codec.json_io import loads_json
from horsies.core.codec.typed import decode_task_result, encode_task_result
from horsies.core.models.app import AppConfig
from horsies.core.models.broker import PostgresConfig
from horsies.core.models.task_send_types import TaskSendAsyncContext
from horsies.core.models.tasks import (
    ContractCode,
    OperationalErrorCode,
    TaskError,
    TaskResult,
)
from horsies.core.models.workflow import NodeKey, WorkflowContext
from horsies.core.types.result import Err
from horsies.core.utils.loop_runner import in_running_event_loop
from horsies.core.worker import child_runner
from horsies.core.worker.task_loop import LoopOpen, TaskLoop

pytestmark = [pytest.mark.unit]

type WireResult = tuple[bool, str, str | None]


class MappedError(Exception):
    pass


def _app() -> Horsies:
    return Horsies(
        AppConfig(
            broker=PostgresConfig(
                database_url=SecretStr(
                    'postgresql+psycopg://user:password@localhost/unused'
                ),
            ),
            exception_mapper={MappedError: 'MAPPED_BY_APP'},
        ),
    )


@pytest.fixture
def task_loop() -> Iterator[TaskLoop]:
    loop = TaskLoop()
    yield loop
    loop.close()


def _runner(app: Horsies, task_loop: TaskLoop) -> Callable[..., WireResult]:
    """Run one task through `_run_task_entry`, as the child does."""

    def run(task_name: str, kwargs: dict[str, Any] | None = None) -> WireResult:
        with ExitStack() as stack:
            stack.enter_context(
                patch.object(
                    child_runner,
                    '_confirm_ownership_and_set_running',
                    MagicMock(return_value=None),
                ),
            )
            stack.enter_context(
                patch.object(child_runner, '_start_heartbeat_thread', MagicMock()),
            )
            stack.enter_context(
                patch.object(child_runner, '_mark_task_finalizing', MagicMock()),
            )
            stack.enter_context(
                patch.object(
                    child_runner, 'get_current_app', MagicMock(return_value=app)
                ),
            )
            return child_runner._run_task_entry(
                task_name=task_name,
                args_json='[]',
                kwargs_json=json.dumps(kwargs or {}),
                task_id='task-id',
                database_url='postgresql://unused/unused',
                master_worker_id='worker',
                runner_heartbeat_interval_ms=100,
                is_workflow_task=False,
                task_loop=task_loop,
            )

    return run


def _result(wire: WireResult, ok_type: Any) -> TaskResult[Any, TaskError]:
    ok, payload, reason = wire
    assert (ok, reason) == (True, None)
    return decode_task_result(loads_json(payload).unwrap(), ok_type)


def _open_loop(task_loop: TaskLoop) -> asyncio.AbstractEventLoop:
    match task_loop.state:
        case LoopOpen(loop=loop):
            return loop
        case other:
            raise AssertionError(other)


class TestTheTaskLoop:
    def test_an_async_task_runs_on_the_child_task_loop(
        self, task_loop: TaskLoop
    ) -> None:
        app = _app()
        seen: list[asyncio.AbstractEventLoop] = []

        @app.task('async_records_loop')
        async def records_loop() -> TaskResult[int, TaskError]:
            seen.append(asyncio.get_running_loop())
            return TaskResult(ok=1)

        run = _runner(app, task_loop)
        assert _result(run(records_loop.task_name), int).ok_value == 1
        assert seen == [_open_loop(task_loop)]

    def test_two_async_tasks_use_one_loop(self, task_loop: TaskLoop) -> None:
        app = _app()
        seen: list[asyncio.AbstractEventLoop] = []

        @app.task('async_records_loop')
        async def records_loop() -> TaskResult[int, TaskError]:
            seen.append(asyncio.get_running_loop())
            return TaskResult(ok=1)

        run = _runner(app, task_loop)
        run(records_loop.task_name)
        run(records_loop.task_name)
        assert len(seen) == 2
        assert seen[0] is seen[1]

    def test_an_object_bound_to_the_loop_stays_usable_across_tasks(
        self,
        task_loop: TaskLoop,
    ) -> None:
        """A loop-bound object that a module keeps works in the next task.

        A future is bound to the loop that made it. A task that waits for
        it on another loop fails, as a pooled connection of an async client
        does.
        """
        app = _app()
        kept: list[asyncio.Future[int]] = []

        @app.task('async_makes_future')
        async def makes_future() -> TaskResult[None, TaskError]:
            kept.append(asyncio.get_running_loop().create_future())
            return TaskResult(ok=None)

        @app.task('async_awaits_future')
        async def awaits_future(*, value: int) -> TaskResult[int, TaskError]:
            # Set the result later, so that the await waits for it.
            asyncio.get_running_loop().call_soon(kept[0].set_result, value)
            return TaskResult(ok=await kept[0])

        run = _runner(app, task_loop)
        run(makes_future.task_name)
        assert _result(run(awaits_future.task_name, {'value': 7}), int).ok_value == 7

    def test_a_sync_task_after_an_async_task_has_no_running_loop(
        self,
        task_loop: TaskLoop,
    ) -> None:
        app = _app()

        @app.task('async_first')
        async def first() -> TaskResult[int, TaskError]:
            return TaskResult(ok=1)

        @app.task('sync_second')
        def second() -> TaskResult[str, TaskError]:
            ran = asyncio.run(asyncio.sleep(0, 'ran'))
            return TaskResult(ok=f'{in_running_event_loop()}|{ran}')

        run = _runner(app, task_loop)
        run(first.task_name)
        assert _result(run(second.task_name), str).ok_value == 'False|ran'

    def test_the_sync_send_in_an_async_task_refuses_the_running_loop(
        self,
        task_loop: TaskLoop,
    ) -> None:
        """An async task sends with `send_async`. The sync `send` returns
        `TaskSendAsyncContext` and does not reach the broker."""
        app = _app()

        @app.task('target')
        def target() -> TaskResult[int, TaskError]:
            return TaskResult(ok=1)

        @app.task('async_sends_sync')
        async def sends_sync() -> TaskResult[str, TaskError]:
            match target.send():
                case Err(TaskSendAsyncContext() as refused):
                    return TaskResult(ok=refused.code.value)
                case other:
                    return TaskResult(ok=f'unexpected {other!r}')

        run = _runner(app, task_loop)
        assert _result(run(sends_sync.task_name), str).ok_value == 'ASYNC_CONTEXT'


def _failing_tasks(app: Horsies) -> dict[str, tuple[str, str]]:
    """Register a `def` and an `async def` form of each failure.

    Maps each failure to the names of its two tasks.
    """

    @app.task('sync_raises')
    def sync_raises() -> TaskResult[int, TaskError]:
        raise ValueError('boom')

    @app.task('async_raises')
    async def async_raises() -> TaskResult[int, TaskError]:
        await asyncio.sleep(0)
        raise ValueError('boom')

    @app.task('sync_raises_mapped')
    def sync_raises_mapped() -> TaskResult[int, TaskError]:
        raise MappedError('mapped')

    @app.task('async_raises_mapped')
    async def async_raises_mapped() -> TaskResult[int, TaskError]:
        raise MappedError('mapped')

    @app.task('sync_wrong_return')
    def sync_wrong_return() -> TaskResult[int, TaskError]:
        return TaskResult(ok='text')  # type: ignore[arg-type]

    @app.task('async_wrong_return')
    async def async_wrong_return() -> TaskResult[int, TaskError]:
        return TaskResult(ok='text')  # type: ignore[arg-type]

    return {
        'raises': (sync_raises.task_name, async_raises.task_name),
        'raises_mapped': (sync_raises_mapped.task_name, async_raises_mapped.task_name),
        'wrong_return': (sync_wrong_return.task_name, async_wrong_return.task_name),
    }


class TestTheSameOutcomesAsASyncTask:
    """A failure gives the same stored result for both kinds of task."""

    @pytest.mark.parametrize(
        ('failure', 'code'),
        [
            (
                'raises',
                {
                    '__builtin_task_code__': OperationalErrorCode.UNHANDLED_EXCEPTION.value
                },
            ),
            ('raises_mapped', 'MAPPED_BY_APP'),
            (
                'wrong_return',
                {'__builtin_task_code__': ContractCode.RETURN_TYPE_MISMATCH.value},
            ),
        ],
    )
    def test_an_async_task_fails_as_a_sync_task_does(
        self,
        task_loop: TaskLoop,
        failure: str,
        code: object,
    ) -> None:
        app = _app()
        sync_name, async_name = _failing_tasks(app)[failure]
        run = _runner(app, task_loop)

        sync_err = json.loads(run(sync_name)[1])['err']
        async_err = json.loads(run(async_name)[1])['err']

        assert sync_err['error_code'] == async_err['error_code'] == code
        assert sync_err['message'].replace('sync_', 'async_') == async_err['message']


class TestWorkflowContext:
    def test_workflow_ctx_reaches_an_async_task(self, task_loop: TaskLoop) -> None:
        app = _app()

        @app.task('source_str')
        def source_str() -> TaskResult[str, TaskError]:
            return TaskResult(ok='upstream')

        @app.task('async_reads_ctx')
        async def reads_ctx(
            *,
            workflow_ctx: WorkflowContext | None = None,
        ) -> TaskResult[str, TaskError]:
            assert workflow_ctx is not None
            upstream = workflow_ctx.result_for(NodeKey[str]('node_a'))
            return TaskResult(ok=f'{workflow_ctx.workflow_id}:{upstream.ok_value}')

        kwargs = {
            '__h_workflow_ctx__': {
                'workflow_id': 'wf-1',
                'task_index': 1,
                'task_name': 'async_reads_ctx',
                'results_by_id': {
                    'node_a': encode_task_result(TaskResult(ok='upstream'), str),
                },
                'task_name_by_id': {'node_a': source_str.task_name},
            },
        }

        run = _runner(app, task_loop)
        assert (
            _result(run(reads_ctx.task_name, kwargs), str).ok_value == 'wf-1:upstream'
        )
