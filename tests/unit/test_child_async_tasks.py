"""Registered `async def` tasks run by the child runner.

`_run_task_entry` runs real registered tasks here. Only the database steps
around the body are replaced: the RUNNING update, the heartbeat thread and
the finalizing mark. Each test gets its own task loop, as a child gets one.
"""

# pyright: reportPrivateUsage=false

from __future__ import annotations

import asyncio
import errno
import gc
import json
import threading
import warnings
from collections.abc import Callable, Iterator
from enum import Enum
from contextlib import ExitStack
from typing import Any, cast
from unittest.mock import MagicMock, patch

import pytest
from pydantic import SecretStr

from horsies.core import process_role as process_role_module
from horsies.core.app import Horsies
from horsies.core.brokers.postgres import MAX_CALLER_EVENT_LOOPS
from horsies.core.brokers.result_types import BrokerWaitInsideTask
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
from horsies.core.models.workflow.handle import WorkflowHandle
from horsies.core.process_role import ProcessRole
from horsies.core.types.result import Err, Ok, is_ok
from horsies.core.utils.loop_runner import in_running_event_loop
from horsies.core.worker import child_runner
from horsies.core.worker.child_protocol import LoopHoldsNoTask, LoopOpenedForThisTask
from horsies.core.worker.task_loop import (
    LoopCreateRefused,
    LoopNotCreated,
    LoopOpen,
    TaskLoop,
)

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


def _runner(
    app: Horsies,
    task_loop: TaskLoop,
    set_running: MagicMock | None = None,
) -> Callable[..., WireResult]:
    """Run one task through `_run_task_entry`, as the child does.

    ``set_running`` stands in for the RUNNING update; it confirms the claim.
    """

    def run(task_name: str, kwargs: dict[str, Any] | None = None) -> WireResult:
        with ExitStack() as stack:
            stack.enter_context(
                patch.object(
                    child_runner,
                    '_confirm_ownership_and_set_running',
                    set_running or MagicMock(return_value=None),
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


def _no_descriptors() -> asyncio.AbstractEventLoop:
    raise OSError(errno.EMFILE, 'Too many open files')


async def _plain_async_function() -> TaskResult[int, TaskError]:
    """Not decorated, so it is no task function and has no task kind."""
    return TaskResult(ok=1)


class _AttributeReadRaises:
    """A registry entry whose attribute read raises."""

    def __getattr__(self, name: str) -> object:
        raise RuntimeError(f'reading {name} fails')


class TestTheLoopIsCreatedBeforeRunning:
    def test_a_refused_loop_returns_the_task_before_running(
        self,
        task_loop: TaskLoop,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """The row is still CLAIMED and no task code ran: the child reports
        the abort reason, and the worker returns the task to the queue."""
        app = _app()
        ran: list[bool] = []
        set_running = MagicMock(return_value=None)

        @app.task('async_never_runs')
        async def never_runs() -> TaskResult[int, TaskError]:
            ran.append(True)
            return TaskResult(ok=1)

        monkeypatch.setattr(asyncio, 'new_event_loop', _no_descriptors)

        with warnings.catch_warnings():
            warnings.simplefilter('error', RuntimeWarning)
            wire = _runner(app, task_loop, set_running)(never_runs.task_name)
            gc.collect()

        assert wire == (False, '', 'TASK_LOOP_UNAVAILABLE')
        set_running.assert_not_called()
        assert ran == []
        assert isinstance(task_loop.state, LoopCreateRefused)

    def test_a_loop_policy_that_raises_is_a_refusal_before_running(
        self,
        task_loop: TaskLoop,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """A non-OS failure of loop creation takes the same counted path."""
        app = _app()
        set_running = MagicMock(return_value=None)

        @app.task('async_policy_refused')
        async def policy_refused() -> TaskResult[int, TaskError]:
            return TaskResult(ok=1)

        def policy_refuses() -> asyncio.AbstractEventLoop:
            raise RuntimeError('the event loop policy refuses')

        monkeypatch.setattr(asyncio, 'new_event_loop', policy_refuses)

        wire = _runner(app, task_loop, set_running)(policy_refused.task_name)

        assert wire == (False, '', 'TASK_LOOP_UNAVAILABLE')
        set_running.assert_not_called()
        match task_loop.state:
            case LoopCreateRefused(failed=failed):
                assert failed.error_name == 'RuntimeError'
            case other:
                raise AssertionError(other)

    @pytest.mark.parametrize(
        'entry',
        [_plain_async_function, _AttributeReadRaises()],
        ids=['plain-function', 'attribute-read-raises'],
    )
    def test_an_entry_that_is_not_a_task_function_fails_resolution(
        self,
        task_loop: TaskLoop,
        entry: object,
    ) -> None:
        """Nothing escapes before RUNNING: the resolution step after it
        reports the entry, bounded like any task failure."""
        app = _app()
        cast(Any, app.tasks)['not_a_task_function'] = entry
        set_running = MagicMock(return_value=None)

        ok, payload, reason = _runner(app, task_loop, set_running)('not_a_task_function')

        set_running.assert_called_once()
        assert ok is True
        assert reason is not None
        error = decode_task_result(loads_json(payload).unwrap(), int).unwrap_err()
        assert error.error_code is OperationalErrorCode.WORKER_RESOLUTION_ERROR
        assert isinstance(task_loop.state, LoopNotCreated)

    def test_an_async_task_finds_its_loop_open_at_the_running_update(
        self,
        task_loop: TaskLoop,
    ) -> None:
        app = _app()
        states_at_running: list[str] = []

        def record_state(*_args: object, **_kwargs: object) -> None:
            states_at_running.append(type(task_loop.state).__name__)

        @app.task('async_one')
        async def one() -> TaskResult[int, TaskError]:
            return TaskResult(ok=1)

        wire = _runner(app, task_loop, MagicMock(side_effect=record_state))(
            one.task_name,
        )

        assert _result(wire, int).ok_value == 1
        assert states_at_running == ['LoopOpen']

    def test_a_def_task_does_not_touch_the_loop(
        self,
        task_loop: TaskLoop,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        app = _app()
        set_running = MagicMock(return_value=None)

        @app.task('sync_one')
        def one() -> TaskResult[int, TaskError]:
            return TaskResult(ok=1)

        monkeypatch.setattr(asyncio, 'new_event_loop', _no_descriptors)

        wire = _runner(app, task_loop, set_running)(one.task_name)

        assert _result(wire, int).ok_value == 1
        set_running.assert_called_once()
        assert isinstance(task_loop.state, LoopNotCreated)

    def test_a_task_the_registry_lacks_fails_resolution_after_running(
        self,
        task_loop: TaskLoop,
    ) -> None:
        """The lookup before RUNNING does not decide this case: the
        resolution error after the RUNNING update reports it, as before."""
        set_running = MagicMock(return_value=None)

        ok, payload, reason = _runner(_app(), task_loop, set_running)('not_registered')

        set_running.assert_called_once()
        assert ok is True
        assert reason is not None and 'not registered' in reason
        error = decode_task_result(loads_json(payload).unwrap(), int).unwrap_err()
        assert error.error_code is OperationalErrorCode.WORKER_RESOLUTION_ERROR
        assert isinstance(task_loop.state, LoopNotCreated)


UNREACHABLE_URL = 'postgresql+psycopg://u:p@127.0.0.1:1/db'
WORKFLOW_ID = '0b6f2f0e-6c1d-4f7a-9a52-3d2b1c0e9f81'


def _refusal_text(code: object, message: str | None) -> str:
    """The refusing operation, as its message names it, and the code."""
    operation = (message or '').split(' for ', 1)[0]
    return f'{operation}:{code.value if isinstance(code, Enum) else code}'


def _task_refusal(result: TaskResult[Any, TaskError]) -> str:
    error = result.unwrap_err()
    return _refusal_text(error.error_code, error.message)


class TestResultWaitsInsideAnAsyncTask:
    def test_each_async_wait_is_refused_at_once_and_send_async_works(
        self,
        task_loop: TaskLoop,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """The child start marks the process, so a wait in an async task body
        is refused before it reads anything. ``send_async()`` still enqueues."""
        monkeypatch.setattr(process_role_module, '_role', ProcessRole.RUNS_NO_TASK_BODIES)
        app = Horsies(
            AppConfig(broker=PostgresConfig(database_url=SecretStr(UNREACHABLE_URL))),
        )
        broker = app.get_broker()
        enqueued: list[str] = []

        async def enqueue_async(*_args: object, task_id: str, **_kwargs: object) -> Ok[str]:
            enqueued.append(task_id)
            return Ok(task_id)

        @app.task('async_sent')
        async def sent() -> TaskResult[int, TaskError]:
            return TaskResult(ok=1)

        @app.task('async_waits_inside_task')
        async def waits_inside_task() -> TaskResult[list[str], TaskError]:
            match await sent.send_async():
                case Ok(handle):
                    pass
                case Err(send_err):
                    return TaskResult(
                        err=TaskError(error_code='SEND_FAILED', message=str(send_err)),
                    )
            outcomes = [f'send_async:{handle.task_id == enqueued[0]}']
            outcomes.append(_task_refusal(await handle.get_async()))
            read = (await app.get_result_async(handle.task_id)).unwrap()
            outcomes.append(_task_refusal(read))
            match await app.get_raw_result_async(handle.task_id):
                case Err(BrokerWaitInsideTask(code=code, message=message)):
                    outcomes.append(_refusal_text(code, message))
                case other:
                    outcomes.append(f'not refused: {other!r}')
            workflow: WorkflowHandle[Any] = WorkflowHandle(
                workflow_id=WORKFLOW_ID,
                broker=broker,
            )
            outcomes.append(_task_refusal(await workflow.get_async()))
            return TaskResult(ok=outcomes)

        try:
            with (
                patch.object(child_runner, '_run_child_start_hooks'),
                patch.object(child_runner, '_initialize_worker_pool'),
            ):
                child_runner.finish_child_start(app, UNREACHABLE_URL, None, 0, 2, True)
            with patch.object(broker, 'enqueue_async', enqueue_async):
                wire = _runner(app, task_loop)(waits_inside_task.task_name)
        finally:
            broker._loop_runner.stop()

        assert _result(wire, list[str]).ok_value == [
            'send_async:True',
            'TaskHandle.get_async():WAIT_INSIDE_TASK',
            'app.get_result_async():WAIT_INSIDE_TASK',
            'app.get_raw_result_async():WAIT_INSIDE_TASK',
            'WorkflowHandle.get_async():WAIT_INSIDE_TASK',
        ]
        assert len(enqueued) == 1
        # Refused before any read: no resource set, and no sync runner.
        assert broker._loop_sets == ()
        assert broker._loop_runner._started is False


CALLER_THREAD_TIMEOUT_S = 10.0


class TestTheSendPathLeavesNoLoopTask:
    def test_send_async_in_an_async_task_leaves_no_task_on_the_loop(
        self,
        task_loop: TaskLoop,
    ) -> None:
        """A child whose loop holds tasks after an async task is retired, so
        the send path of horsies must leave none. Up to ``enqueue_async``,
        which the test replaces, the send runs its real code."""
        app = Horsies(
            AppConfig(broker=PostgresConfig(database_url=SecretStr(UNREACHABLE_URL))),
        )
        broker = app.get_broker()
        enqueued: list[str] = []

        async def enqueue_async(*_args: object, task_id: str, **_kwargs: object) -> Ok[str]:
            enqueued.append(task_id)
            return Ok(task_id)

        @app.task('async_sent_from_a_task')
        async def sent() -> TaskResult[int, TaskError]:
            return TaskResult(ok=1)

        @app.task('async_sends')
        async def sends() -> TaskResult[str, TaskError]:
            match await sent.send_async():
                case Ok(handle):
                    return TaskResult(ok=handle.task_id)
                case Err(send_err):
                    return TaskResult(
                        err=TaskError(error_code='SEND_FAILED', message=str(send_err)),
                    )

        state_before_task = task_loop.state
        try:
            with patch.object(broker, 'enqueue_async', enqueue_async):
                wire = _runner(app, task_loop)(sends.task_name)
        finally:
            broker._loop_runner.stop()

        assert _result(wire, str).ok_value == enqueued[0]
        assert task_loop.report(state_before_task) == LoopHoldsNoTask(
            opening=LoopOpenedForThisTask(),
        )


class TestTheTaskLoopIsNotACallerLoop:
    def test_a_sync_task_opens_every_caller_loop_after_an_async_broker_call(
        self,
        task_loop: TaskLoop,
    ) -> None:
        """The task loop keeps its resource set for the life of the child. It
        does not take a caller place, so a later sync task still opens
        ``MAX_CALLER_EVENT_LOOPS`` loops of its own at once."""
        app = Horsies(
            AppConfig(broker=PostgresConfig(database_url=SecretStr(UNREACHABLE_URL))),
        )
        broker = app.get_broker()

        @app.task('async_selects_resources')
        async def selects_resources() -> TaskResult[bool, TaskError]:
            return TaskResult(ok=is_ok(await broker.select_resources()))

        @app.task('sync_opens_caller_loops')
        def opens_caller_loops() -> TaskResult[list[str], TaskError]:
            all_selected = threading.Barrier(MAX_CALLER_EVENT_LOOPS)
            outcomes: list[str] = []
            outcomes_lock = threading.Lock()

            async def select_then_hold_the_loop() -> str:
                result = await broker.select_resources()
                # This loop stays open until every thread has selected.
                all_selected.wait(CALLER_THREAD_TIMEOUT_S)
                return 'set' if is_ok(result) else type(result.err_value).__name__

            def in_a_thread() -> None:
                outcome = asyncio.run(select_then_hold_the_loop())
                with outcomes_lock:
                    outcomes.append(outcome)

            threads = [
                threading.Thread(target=in_a_thread)
                for _ in range(MAX_CALLER_EVENT_LOOPS)
            ]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join(CALLER_THREAD_TIMEOUT_S)
            return TaskResult(ok=sorted(outcomes))

        run = _runner(app, task_loop)
        try:
            assert _result(run(selects_resources.task_name), bool).ok_value is True
            outcomes = _result(run(opens_caller_loops.task_name), list[str]).ok_value
        finally:
            broker._loop_runner.stop()

        assert outcomes == ['set'] * MAX_CALLER_EVENT_LOOPS


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
