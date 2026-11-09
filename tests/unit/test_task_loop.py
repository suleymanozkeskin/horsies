"""The one event loop that runs the async tasks of a worker child."""

from __future__ import annotations

import asyncio
import contextvars
import logging
import threading
import time
from collections.abc import AsyncIterator, Iterator

import pytest

from horsies.core.utils.loop_runner import in_running_event_loop
from horsies.core.worker import task_loop as task_loop_module
from horsies.core.worker.task_loop import (
    LoopClosed,
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
        task_loop.run(_value(1))
        assert isinstance(task_loop.state, LoopOpen)

    def test_a_run_returns_the_value_of_the_coroutine(
        self, task_loop: TaskLoop
    ) -> None:
        assert task_loop.run(_value(7)) == 7

    def test_two_runs_use_one_loop(self, task_loop: TaskLoop) -> None:
        first = task_loop.run(_running_loop())
        second = task_loop.run(_running_loop())
        assert first is second is _open_loop(task_loop)

    def test_after_a_run_no_loop_runs_and_the_loop_stays_open(
        self,
        task_loop: TaskLoop,
    ) -> None:
        task_loop.run(_value(1))
        loop = _open_loop(task_loop)
        assert not in_running_event_loop()
        assert not loop.is_running()
        assert not loop.is_closed()

    def test_asyncio_run_between_two_runs_leaves_the_loop_alone(
        self,
        task_loop: TaskLoop,
    ) -> None:
        first = task_loop.run(_running_loop())
        assert asyncio.run(_value(3)) == 3
        assert task_loop.run(_running_loop()) is first

    def test_an_exception_of_the_coroutine_propagates(
        self, task_loop: TaskLoop
    ) -> None:
        with pytest.raises(ValueError, match='boom'):
            task_loop.run(_raise(ValueError('boom')))
        assert task_loop.run(_value(2)) == 2

    def test_keyboard_interrupt_propagates_and_the_loop_stays_usable(
        self,
        task_loop: TaskLoop,
    ) -> None:
        with pytest.raises(KeyboardInterrupt):
            task_loop.run(_raise(KeyboardInterrupt()))
        assert task_loop.run(_value(2)) == 2

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
            assert task_loop.run(read_then_set()) == 'caller'
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

        task_loop.run(start())
        release.set()
        assert finished == []
        task_loop.run(two_steps())
        assert finished == ['done']

    def test_a_run_inside_a_running_loop_is_a_defect(self, task_loop: TaskLoop) -> None:
        coroutine = _value(1)

        async def nested() -> None:
            task_loop.run(coroutine)

        with pytest.raises(AssertionError, match='while an event loop runs'):
            asyncio.run(nested())
        assert coroutine.cr_frame is None
        assert task_loop.state == LoopNotCreated()

    def test_a_run_after_close_is_a_defect(self, task_loop: TaskLoop) -> None:
        task_loop.close()
        coroutine = _value(1)
        with pytest.raises(AssertionError, match='after close'):
            task_loop.run(coroutine)
        assert coroutine.cr_frame is None


class TestClose:
    def test_close_before_any_run_creates_no_loop(self, task_loop: TaskLoop) -> None:
        task_loop.close()
        assert task_loop.state == LoopClosed()

    def test_close_closes_the_loop(self, task_loop: TaskLoop) -> None:
        task_loop.run(_value(1))
        loop = _open_loop(task_loop)
        task_loop.close()
        assert loop.is_closed()
        assert task_loop.state == LoopClosed()

    def test_a_second_close_does_nothing(self, task_loop: TaskLoop) -> None:
        task_loop.run(_value(1))
        task_loop.close()
        task_loop.close()
        assert task_loop.state == LoopClosed()

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

        task_loop.run(start())
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

        task_loop.run(start())
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

        assert task_loop.run(take_one()) == 1
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

        task_loop.run(start())
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

        task_loop.run(start())
        loop = _open_loop(task_loop)
        started = time.monotonic()
        try:
            with caplog.at_level(logging.WARNING, logger='horsies.worker'):
                task_loop.close()
            elapsed = time.monotonic() - started
        finally:
            release.set()
        assert elapsed < 10 * SHORT_CLOSE_TIMEOUT_S
        assert loop.is_closed()
        assert 'default executor of the task loop did not shut down' in caplog.text
