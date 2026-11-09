"""The one event loop that runs the async tasks of a worker child."""

from __future__ import annotations

import asyncio
import contextvars
import errno
import inspect
import logging
import threading
import time
from collections.abc import AsyncIterator, Iterator

import pytest

from horsies.core.types.result import Err, Ok
from horsies.core.utils.loop_runner import in_running_event_loop
from horsies.core.worker.child_protocol import LoopHoldsNoTask, LoopHoldsTasks
from horsies.core.worker import task_loop as task_loop_module
from horsies.core.worker.task_loop import (
    ExecutorShutDown,
    ExecutorThreadsLeft,
    LoopClosed,
    LoopCreateFailed,
    LoopNotCreated,
    LoopOpen,
    TaskLoop,
)

pytestmark = [pytest.mark.unit]

_REQUEST: contextvars.ContextVar[str] = contextvars.ContextVar(
    'request', default='unset'
)
SHORT_CLOSE_TIMEOUT_S = 0.2
THREAD_RELEASE_TIMEOUT_S = 5.0


@pytest.fixture
def task_loop() -> Iterator[TaskLoop]:
    loop = TaskLoop()
    yield loop
    loop.close()


def _open_loop(task_loop: TaskLoop) -> asyncio.AbstractEventLoop:
    match task_loop.state:
        case LoopOpen(loop=loop):
            return loop
        case other:
            raise AssertionError(other)


async def _running_loop() -> asyncio.AbstractEventLoop:
    return asyncio.get_running_loop()


async def _value(value: int) -> int:
    return value


async def _raise(error: BaseException) -> None:
    raise error


class TestRun:
    def test_the_first_run_creates_the_loop(self, task_loop: TaskLoop) -> None:
        assert task_loop.state == LoopNotCreated()
        task_loop.run(_value(1)).unwrap()
        assert isinstance(task_loop.state, LoopOpen)

    def test_a_run_returns_the_value_of_the_coroutine(
        self, task_loop: TaskLoop
    ) -> None:
        assert task_loop.run(_value(7)).unwrap() == 7

    def test_two_runs_use_one_loop(self, task_loop: TaskLoop) -> None:
        first = task_loop.run(_running_loop()).unwrap()
        second = task_loop.run(_running_loop()).unwrap()
        assert first is second is _open_loop(task_loop)

    def test_after_a_run_no_loop_runs_and_the_loop_stays_open(
        self,
        task_loop: TaskLoop,
    ) -> None:
        task_loop.run(_value(1)).unwrap()
        loop = _open_loop(task_loop)
        assert not in_running_event_loop()
        assert not loop.is_running()
        assert not loop.is_closed()

    def test_asyncio_run_between_two_runs_leaves_the_loop_alone(
        self,
        task_loop: TaskLoop,
    ) -> None:
        first = task_loop.run(_running_loop()).unwrap()
        assert asyncio.run(_value(3)) == 3
        assert task_loop.run(_running_loop()).unwrap() is first

    def test_an_exception_of_the_coroutine_propagates(
        self, task_loop: TaskLoop
    ) -> None:
        with pytest.raises(ValueError, match='boom'):
            task_loop.run(_raise(ValueError('boom'))).unwrap()
        assert task_loop.run(_value(2)).unwrap() == 2

    def test_keyboard_interrupt_propagates_and_the_loop_stays_usable(
        self,
        task_loop: TaskLoop,
    ) -> None:
        with pytest.raises(KeyboardInterrupt):
            task_loop.run(_raise(KeyboardInterrupt())).unwrap()
        assert task_loop.run(_value(2)).unwrap() == 2

    def test_the_coroutine_runs_in_a_copy_of_the_callers_context(
        self,
        task_loop: TaskLoop,
    ) -> None:
        async def read_then_set() -> str:
            seen = _REQUEST.get()
            _REQUEST.set('inside')
            return seen

        token = _REQUEST.set('caller')
        try:
            assert task_loop.run(read_then_set()).unwrap() == 'caller'
            assert _REQUEST.get() == 'caller'
        finally:
            _REQUEST.reset(token)

    def test_a_task_left_on_the_loop_waits_for_the_next_run(
        self,
        task_loop: TaskLoop,
    ) -> None:
        release = asyncio.Event()
        finished: list[str] = []
        kept: list[asyncio.Task[None]] = []

        async def wait_for_release() -> None:
            await release.wait()
            finished.append('done')

        async def start() -> None:
            kept.append(asyncio.get_running_loop().create_task(wait_for_release()))

        async def two_steps() -> None:
            await asyncio.sleep(0)
            await asyncio.sleep(0)

        task_loop.run(start()).unwrap()
        release.set()
        assert finished == []
        task_loop.run(two_steps()).unwrap()
        assert finished == ['done']

    def test_a_run_inside_a_running_loop_is_a_defect(self, task_loop: TaskLoop) -> None:
        coroutine = _value(1)

        async def nested() -> None:
            task_loop.run(coroutine).unwrap()

        with pytest.raises(AssertionError, match='while an event loop runs'):
            asyncio.run(nested())
        assert coroutine.cr_frame is None
        assert task_loop.state == LoopNotCreated()

    def test_a_run_after_close_is_a_defect(self, task_loop: TaskLoop) -> None:
        task_loop.close()
        coroutine = _value(1)
        with pytest.raises(AssertionError, match='after close'):
            task_loop.run(coroutine).unwrap()
        assert coroutine.cr_frame is None


class TestLoopCreateFailure:
    def test_the_coroutine_is_closed_and_a_later_run_tries_again(
        self,
        task_loop: TaskLoop,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        real_new_event_loop = asyncio.new_event_loop
        calls: list[str] = []

        def fails_once() -> asyncio.AbstractEventLoop:
            calls.append('new_event_loop')
            if len(calls) == 1:
                raise OSError(errno.EMFILE, 'Too many open files')
            return real_new_event_loop()

        monkeypatch.setattr(asyncio, 'new_event_loop', fails_once)
        coroutine = _value(1)

        match task_loop.run(coroutine):
            case Err(failed):
                assert failed == LoopCreateFailed(
                    error_name='OSError',
                    detail='[Errno 24] Too many open files',
                )
            case Ok(value):
                raise AssertionError(value)
        assert inspect.getcoroutinestate(coroutine) == inspect.CORO_CLOSED
        assert task_loop.state == LoopNotCreated()

        assert task_loop.run(_value(2)).unwrap() == 2
        assert calls == ['new_event_loop', 'new_event_loop']
        assert isinstance(task_loop.state, LoopOpen)


class TestTasksLeft:
    def test_no_loop_holds_no_task(self, task_loop: TaskLoop) -> None:
        assert task_loop.tasks_left() == LoopHoldsNoTask()

    def test_a_run_that_awaits_its_work_leaves_no_task(self, task_loop: TaskLoop) -> None:
        async def awaits_its_task() -> None:
            await asyncio.get_running_loop().create_task(asyncio.sleep(0))

        task_loop.run(awaits_its_task()).unwrap()
        assert task_loop.tasks_left() == LoopHoldsNoTask()

    def test_tasks_that_a_run_did_not_await_are_counted(self, task_loop: TaskLoop) -> None:
        kept: list[asyncio.Task[bool]] = []

        async def leaves_two() -> None:
            loop = asyncio.get_running_loop()
            kept.extend(loop.create_task(asyncio.Event().wait()) for _ in range(2))
            await asyncio.sleep(0)

        task_loop.run(leaves_two()).unwrap()
        assert task_loop.tasks_left() == LoopHoldsTasks(count=2)

    def test_a_call_after_close_is_a_defect(self, task_loop: TaskLoop) -> None:
        task_loop.close()
        with pytest.raises(AssertionError, match='after close'):
            task_loop.tasks_left()


class TestClose:
    def test_close_before_any_run_creates_no_loop(self, task_loop: TaskLoop) -> None:
        assert task_loop.close() == ExecutorShutDown()
        assert task_loop.state == LoopClosed(ExecutorShutDown())

    def test_close_closes_the_loop(self, task_loop: TaskLoop) -> None:
        task_loop.run(_value(1)).unwrap()
        loop = _open_loop(task_loop)
        assert task_loop.close() == ExecutorShutDown()
        assert loop.is_closed()
        assert task_loop.state == LoopClosed(ExecutorShutDown())

    def test_a_second_close_does_nothing(self, task_loop: TaskLoop) -> None:
        task_loop.run(_value(1)).unwrap()
        assert task_loop.close() == ExecutorShutDown()
        assert task_loop.close() == ExecutorShutDown()
        assert task_loop.state == LoopClosed(ExecutorShutDown())

    def test_close_cancels_a_task_left_on_the_loop(self, task_loop: TaskLoop) -> None:
        cleaned: list[str] = []
        kept: list[asyncio.Task[None]] = []

        async def wait_forever() -> None:
            try:
                await asyncio.Event().wait()
            finally:
                cleaned.append('cancelled')

        async def start() -> None:
            kept.append(asyncio.get_running_loop().create_task(wait_forever()))
            await asyncio.sleep(0)

        task_loop.run(start()).unwrap()
        task_loop.close()
        assert cleaned == ['cancelled']
        assert kept[0].cancelled()

    def test_close_logs_a_leftover_task_that_fails_on_the_cancel(
        self,
        task_loop: TaskLoop,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        kept: list[asyncio.Task[None]] = []

        async def fail_on_cancel() -> None:
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                raise ValueError('cleanup failed') from None

        async def start() -> None:
            kept.append(asyncio.get_running_loop().create_task(fail_on_cancel()))
            await asyncio.sleep(0)

        task_loop.run(start()).unwrap()
        with caplog.at_level(logging.WARNING, logger='horsies.worker'):
            task_loop.close()
        assert 'ValueError: cleanup failed' in caplog.text

    def test_close_finalizes_a_suspended_async_generator(
        self, task_loop: TaskLoop
    ) -> None:
        finalized: list[str] = []

        async def numbers() -> AsyncIterator[int]:
            try:
                yield 1
                yield 2
            finally:
                finalized.append('closed')

        generator = numbers()

        async def take_one() -> int:
            return await anext(generator)

        assert task_loop.run(take_one()).unwrap() == 1
        task_loop.close()
        assert finalized == ['closed']

    def test_close_is_bounded_when_a_task_ignores_the_cancel(
        self,
        task_loop: TaskLoop,
        monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        monkeypatch.setattr(
            task_loop_module, 'LOOP_CLOSE_TIMEOUT_S', SHORT_CLOSE_TIMEOUT_S
        )
        kept: list[asyncio.Task[None]] = []

        async def ignore_cancel() -> None:
            while True:
                try:
                    await asyncio.sleep(60)
                except asyncio.CancelledError:
                    continue

        async def start() -> None:
            kept.append(asyncio.get_running_loop().create_task(ignore_cancel()))
            await asyncio.sleep(0)

        task_loop.run(start()).unwrap()
        loop = _open_loop(task_loop)
        started = time.monotonic()
        with caplog.at_level(logging.WARNING, logger='horsies.worker'):
            task_loop.close()
        assert time.monotonic() - started < 10 * SHORT_CLOSE_TIMEOUT_S
        assert loop.is_closed()
        assert '1 task(s) left on the task loop did not end' in caplog.text
        # The task never ends. Drop it quietly: it is pending on a closed loop.
        kept[0]._log_destroy_pending = False  # pyright: ignore[reportAttributeAccessIssue]

    def test_close_is_bounded_when_executor_work_does_not_end(
        self,
        task_loop: TaskLoop,
        monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        monkeypatch.setattr(
            task_loop_module, 'LOOP_CLOSE_TIMEOUT_S', SHORT_CLOSE_TIMEOUT_S
        )
        release = threading.Event()
        kept: list[asyncio.Task[bool]] = []

        async def start() -> None:
            work = asyncio.to_thread(release.wait, THREAD_RELEASE_TIMEOUT_S)
            kept.append(asyncio.get_running_loop().create_task(work))
            await asyncio.sleep(0)

        task_loop.run(start()).unwrap()
        loop = _open_loop(task_loop)
        started = time.monotonic()
        try:
            with caplog.at_level(logging.WARNING, logger='horsies.worker'):
                executor = task_loop.close()
            elapsed = time.monotonic() - started
            second = task_loop.close()
        finally:
            release.set()
        assert elapsed < 10 * SHORT_CLOSE_TIMEOUT_S
        assert executor == ExecutorThreadsLeft()
        assert second == ExecutorThreadsLeft()
        assert loop.is_closed()
        assert 'default executor of the task loop did not shut down' in caplog.text
