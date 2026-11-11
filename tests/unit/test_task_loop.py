"""The one event loop that runs the async tasks of a worker child."""

# pyright: reportPrivateUsage=false

from __future__ import annotations

import asyncio
import contextvars
import errno
import inspect
import logging
import threading
import time
from collections.abc import AsyncIterator, Coroutine, Iterator
from types import SimpleNamespace
from typing import Any, cast

import pytest

from horsies.core.models.boot_failure import (
    ResourceShortage,
    ShortageErrno,
    TaskLoopCreateFailed,
)
from horsies.core.types.result import Err, Ok
from horsies.core.utils.loop_runner import in_running_event_loop
from horsies.core.worker.child_protocol import (
    MAX_LEFTOVER_NAME_CHARS,
    MAX_LEFTOVER_TASK_NAMES,
    LeftWorkEnd,
    LoopHoldsNoTask,
    LoopHoldsTasks,
    NoTaskLoop,
)
from horsies.core.worker import task_loop as task_loop_module
from horsies.core.worker.task_loop import (
    ExecutorShutDown,
    ExecutorThreadsLeft,
    LeftWorkStuck,
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
def new_task_loop() -> Iterator[TaskLoop]:
    """A task loop that ``open`` has not created yet."""
    loop = TaskLoop()
    yield loop
    loop.close()


@pytest.fixture
def task_loop() -> Iterator[TaskLoop]:
    """An open task loop, as an async task finds it."""
    loop = TaskLoop()
    loop.open().unwrap()
    yield loop
    loop.close()


EMFILE_DETAIL = '[Errno 24] Too many open files'


def _no_descriptors() -> asyncio.AbstractEventLoop:
    raise OSError(errno.EMFILE, 'Too many open files')


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


class TestOpen:
    def test_open_creates_the_loop(self, new_task_loop: TaskLoop) -> None:
        assert new_task_loop.state == LoopNotCreated()
        assert new_task_loop.open() == Ok(None)
        assert isinstance(new_task_loop.state, LoopOpen)

    def test_a_second_open_keeps_the_loop(self, new_task_loop: TaskLoop) -> None:
        new_task_loop.open().unwrap()
        loop = _open_loop(new_task_loop)
        assert new_task_loop.open() == Ok(None)
        assert _open_loop(new_task_loop) is loop

    def test_a_refused_loop_is_an_error_and_creates_no_loop(
        self,
        new_task_loop: TaskLoop,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """The child then fails its boot, so nothing runs on this loop."""
        monkeypatch.setattr(asyncio, 'new_event_loop', _no_descriptors)
        failed = LoopCreateFailed(
            cause=ResourceShortage(ShortageErrno.EMFILE),
            error_name='OSError',
            detail=EMFILE_DETAIL,
        )

        assert new_task_loop.open() == Err(failed)
        assert new_task_loop.state == LoopNotCreated()
        assert new_task_loop.report() == NoTaskLoop()
        assert failed.describe() == (
            'the event loop for async tasks could not be created: '
            f'OSError: {EMFILE_DETAIL}'
        )

    def test_a_refused_loop_runs_nothing_and_closes(
        self,
        new_task_loop: TaskLoop,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setattr(asyncio, 'new_event_loop', _no_descriptors)
        assert isinstance(new_task_loop.open(), Err)
        coroutine = _value(1)

        with pytest.raises(AssertionError, match='before open'):
            new_task_loop.run(coroutine)

        assert inspect.getcoroutinestate(coroutine) == inspect.CORO_CLOSED
        assert new_task_loop.close() == ExecutorShutDown()

    @pytest.mark.parametrize(
        'error',
        [RuntimeError('the event loop policy refuses'), MemoryError('no memory')],
        ids=['policy', 'memory'],
    )
    def test_an_exception_of_the_runtime_is_a_refusal(
        self,
        new_task_loop: TaskLoop,
        monkeypatch: pytest.MonkeyPatch,
        error: Exception,
    ) -> None:
        """Not only the OS: an event loop policy can raise too. The child
        then takes the boot failure path, not a crash."""

        def refuses() -> asyncio.AbstractEventLoop:
            raise error

        monkeypatch.setattr(asyncio, 'new_event_loop', refuses)
        failed = LoopCreateFailed(
            cause=TaskLoopCreateFailed(),
            error_name=type(error).__name__,
            detail=str(error),
        )

        assert new_task_loop.open() == Err(failed)
        assert new_task_loop.state == LoopNotCreated()

    def test_keyboard_interrupt_from_the_creation_is_not_caught(
        self,
        new_task_loop: TaskLoop,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        def interrupted() -> asyncio.AbstractEventLoop:
            raise KeyboardInterrupt

        monkeypatch.setattr(asyncio, 'new_event_loop', interrupted)

        with pytest.raises(KeyboardInterrupt):
            new_task_loop.open()
        assert new_task_loop.state == LoopNotCreated()

    def test_open_after_close_is_a_defect(self, new_task_loop: TaskLoop) -> None:
        new_task_loop.close()
        with pytest.raises(AssertionError, match='after close'):
            new_task_loop.open()


class TestRun:
    def test_a_run_before_open_is_a_defect(self, new_task_loop: TaskLoop) -> None:
        coroutine = _value(1)
        with pytest.raises(AssertionError, match='before open'):
            new_task_loop.run(coroutine)
        assert inspect.getcoroutinestate(coroutine) == inspect.CORO_CLOSED
        assert new_task_loop.state == LoopNotCreated()

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
        loop = _open_loop(task_loop)

        async def nested() -> None:
            task_loop.run(coroutine)

        with pytest.raises(AssertionError, match='while an event loop runs'):
            asyncio.run(nested())
        assert coroutine.cr_frame is None
        assert not loop.is_running()

    def test_a_run_after_close_is_a_defect(self, task_loop: TaskLoop) -> None:
        task_loop.close()
        coroutine = _value(1)
        with pytest.raises(AssertionError, match='after close'):
            task_loop.run(coroutine)
        assert coroutine.cr_frame is None


class TestReport:
    def test_no_loop_is_no_task_loop(self, new_task_loop: TaskLoop) -> None:
        assert new_task_loop.report() == NoTaskLoop()

    def test_an_open_loop_with_no_run_holds_no_task(self, task_loop: TaskLoop) -> None:
        assert task_loop.report() == LoopHoldsNoTask()

    def test_a_run_that_awaits_its_work_leaves_no_task(self, task_loop: TaskLoop) -> None:
        async def awaits_its_task() -> None:
            await asyncio.get_running_loop().create_task(asyncio.sleep(0))

        task_loop.run(awaits_its_task())
        assert task_loop.report() == LoopHoldsNoTask()

    def test_tasks_that_a_run_did_not_await_are_counted(self, task_loop: TaskLoop) -> None:
        kept: list[asyncio.Task[bool]] = []

        async def leaves_two() -> None:
            loop = asyncio.get_running_loop()
            kept.extend(loop.create_task(asyncio.Event().wait()) for _ in range(2))
            await asyncio.sleep(0)

        task_loop.run(leaves_two())
        assert task_loop.report() == LoopHoldsTasks(
            count=2,
            names=('Event.wait', 'Event.wait'),
        )

    def test_the_report_names_the_coroutine_of_each_leftover(
        self,
        task_loop: TaskLoop,
    ) -> None:
        kept: list[asyncio.Task[None]] = []

        async def poll_forever() -> None:
            await asyncio.Event().wait()

        async def keep_alive_forever() -> None:
            await asyncio.Event().wait()

        async def leaves_two() -> None:
            loop = asyncio.get_running_loop()
            kept.append(loop.create_task(poll_forever()))
            kept.append(loop.create_task(keep_alive_forever()))
            await asyncio.sleep(0)

        task_loop.run(leaves_two())
        prefix = (
            'TestReport.test_the_report_names_the_coroutine_of_each_leftover.<locals>.'
        )
        report = task_loop.report()
        assert report == LoopHoldsTasks(
            count=2,
            names=(f'{prefix}keep_alive_forever', f'{prefix}poll_forever'),
        )

    def test_the_names_are_bounded_and_say_how_many_they_leave_out(
        self,
        task_loop: TaskLoop,
    ) -> None:
        count = MAX_LEFTOVER_TASK_NAMES + 3
        kept: list[asyncio.Task[bool]] = []

        async def leaves_many() -> None:
            loop = asyncio.get_running_loop()
            kept.extend(loop.create_task(asyncio.Event().wait()) for _ in range(count))
            await asyncio.sleep(0)

        task_loop.run(leaves_many())
        report = task_loop.report()
        match report:
            case LoopHoldsTasks(count=reported, names=names):
                assert reported == count
                assert names == ('Event.wait',) * MAX_LEFTOVER_TASK_NAMES
                assert report.names_text().endswith(', and 3 more')
            case other:
                raise AssertionError(other)

    def test_a_long_or_invalid_name_is_cut_and_made_valid(
        self,
        task_loop: TaskLoop,
    ) -> None:
        kept: list[asyncio.Task[bool]] = []

        def renamed(name: str) -> Coroutine[Any, Any, bool]:
            waiting = asyncio.Event().wait()
            waiting.__qualname__ = name
            return waiting

        async def leaves_two() -> None:
            loop = asyncio.get_running_loop()
            kept.append(loop.create_task(renamed('x' * (MAX_LEFTOVER_NAME_CHARS + 50))))
            kept.append(loop.create_task(renamed('bad \ud800 name')))
            await asyncio.sleep(0)

        task_loop.run(leaves_two())
        match task_loop.report():
            case LoopHoldsTasks(names=names):
                assert sorted(names) == sorted(
                    ['x' * MAX_LEFTOVER_NAME_CHARS, 'bad \\ud800 name'],
                )
            case other:
                raise AssertionError(other)

    def test_a_helper_cancelled_in_finally_and_not_awaited_is_left(
        self,
        task_loop: TaskLoop,
    ) -> None:
        """``cancel()`` only asks. A helper that awaits while it stops is
        still on the loop when the task ends."""

        async def helper() -> None:
            try:
                await asyncio.Event().wait()
            finally:
                await asyncio.sleep(0)  # a close that awaits

        async def cancels_only() -> None:
            running = asyncio.create_task(helper())
            try:
                await asyncio.sleep(0)
            finally:
                running.cancel()

        task_loop.run(cancels_only())
        match task_loop.report():
            case LoopHoldsTasks(count=1, names=(name,)):
                assert name.endswith('.<locals>.helper')
            case other:
                raise AssertionError(other)

    def test_a_helper_cancelled_and_awaited_in_finally_is_not_left(
        self,
        task_loop: TaskLoop,
    ) -> None:
        """The idiom that the async task docs show."""

        async def helper() -> None:
            try:
                await asyncio.Event().wait()
            finally:
                await asyncio.sleep(0)  # a close that awaits

        async def cancels_and_awaits() -> None:
            running = asyncio.create_task(helper())
            try:
                await asyncio.sleep(0)
            finally:
                running.cancel()
                await asyncio.gather(running, return_exceptions=True)

        task_loop.run(cancels_and_awaits())
        assert task_loop.report() == LoopHoldsNoTask()

    def test_a_scheduled_callback_is_not_counted_and_runs_in_the_next_run(
        self,
        task_loop: TaskLoop,
    ) -> None:
        """The report sees asyncio tasks only, as the async task docs say."""
        fired: list[str] = []

        async def schedules_a_callback() -> None:
            asyncio.get_running_loop().call_later(0.05, fired.append, 'fired')

        async def later_task() -> None:
            await asyncio.sleep(0.2)

        task_loop.run(schedules_a_callback())
        report = task_loop.report()
        fired_before_the_later_task = list(fired)
        task_loop.run(later_task())

        assert report == LoopHoldsNoTask()
        assert fired_before_the_later_task == []
        assert fired == ['fired']

    def test_a_coroutine_with_no_qualname_is_named_by_its_task(self) -> None:
        task = SimpleNamespace(get_coro=object, get_name=lambda: 'Task-7')
        assert task_loop_module._coroutine_name(cast(Any, task)) == 'Task-7'

    def test_a_call_after_close_is_a_defect(self, task_loop: TaskLoop) -> None:
        task_loop.close()
        with pytest.raises(AssertionError, match='after close'):
            task_loop.report()


class TestClose:
    def test_close_before_open_creates_no_loop(self, new_task_loop: TaskLoop) -> None:
        assert new_task_loop.close() == ExecutorShutDown()
        assert new_task_loop.state == LoopClosed(ExecutorShutDown())

    def test_close_closes_the_loop(self, task_loop: TaskLoop) -> None:
        task_loop.run(_value(1))
        loop = _open_loop(task_loop)
        assert task_loop.close() == ExecutorShutDown()
        assert loop.is_closed()
        assert task_loop.state == LoopClosed(ExecutorShutDown())

    def test_a_second_close_does_nothing(self, task_loop: TaskLoop) -> None:
        task_loop.run(_value(1))
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
        not_ended = [
            line for line in caplog.text.splitlines() if 'did not end within' in line
        ]
        assert len(not_ended) == 1
        assert '1 task(s) left on the task loop did not end' in not_ended[0]
        assert not_ended[0].endswith('.<locals>.ignore_cancel')
        # The task never ends. Drop it quietly: it is pending on a closed loop.
        kept[0]._log_destroy_pending = False  # pyright: ignore[reportAttributeAccessIssue]

    def test_an_idle_executor_shuts_down_after_a_cancel_that_used_the_deadline(
        self,
        task_loop: TaskLoop,
        monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """A leftover that ignores the cancel uses all of the close deadline.
        The executor ran one call earlier and is idle now, so it still shuts
        down, and the child can exit normally."""
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
            await asyncio.to_thread(int)
            kept.append(asyncio.get_running_loop().create_task(ignore_cancel()))
            await asyncio.sleep(0)

        task_loop.run(start())
        with caplog.at_level(logging.WARNING, logger='horsies.worker'):
            executor = task_loop.close()

        assert executor == ExecutorShutDown()
        assert 'default executor' not in caplog.text
        # The task never ends. Drop it quietly: it is pending on a closed loop.
        kept[0]._log_destroy_pending = False  # pyright: ignore[reportAttributeAccessIssue]

    def test_async_generators_close_after_a_stubborn_leftover(
        self,
        task_loop: TaskLoop,
        monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """A leftover that ignores the cancel uses all of the close deadline.
        The async generator step still gets its minimum time, so the
        ``finally`` of an open generator runs to its end."""
        monkeypatch.setattr(
            task_loop_module, 'LOOP_CLOSE_TIMEOUT_S', SHORT_CLOSE_TIMEOUT_S
        )
        kept: list[object] = []
        closed: list[str] = []

        async def ignore_cancel() -> None:
            while True:
                try:
                    await asyncio.sleep(60)
                except asyncio.CancelledError:
                    continue

        async def open_generator() -> AsyncIterator[int]:
            try:
                yield 1
                await asyncio.sleep(60)
            finally:
                await asyncio.sleep(0.01)
                closed.append('finally ran')

        async def start() -> None:
            generator = open_generator()
            await anext(generator)
            kept.append(generator)
            kept.append(asyncio.get_running_loop().create_task(ignore_cancel()))
            await asyncio.sleep(0)

        task_loop.run(start())
        with caplog.at_level(logging.WARNING, logger='horsies.worker'):
            task_loop.close()

        assert closed == ['finally ran']
        assert 'async generators of the task loop did not shut down' not in caplog.text
        # The task never ends. Drop it quietly: it is pending on a closed loop.
        leftover = cast('asyncio.Task[None]', kept[1])
        leftover._log_destroy_pending = False  # pyright: ignore[reportAttributeAccessIssue]

    def test_close_steps_end_before_the_parent_kills_the_child(self) -> None:
        """The worst case of the close: the leftovers use the whole deadline,
        then each later step uses its minimum. It must end before the parent
        kills a child that has not exited after its stop frame."""
        from horsies.core.worker.slots import CHILD_STOP_TIMEOUT_S

        worst_case_s = (
            task_loop_module.LOOP_CLOSE_TIMEOUT_S
            + task_loop_module.ASYNCGEN_SHUTDOWN_MIN_TIMEOUT_S
            + task_loop_module.EXECUTOR_SHUTDOWN_MIN_TIMEOUT_S
        )
        assert worst_case_s < CHILD_STOP_TIMEOUT_S

    def test_close_is_bounded_when_executor_work_does_not_end(
        self,
        task_loop: TaskLoop,
        monkeypatch: pytest.MonkeyPatch,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        monkeypatch.setattr(
            task_loop_module, 'LOOP_CLOSE_TIMEOUT_S', SHORT_CLOSE_TIMEOUT_S
        )
        monkeypatch.setattr(
            task_loop_module, 'EXECUTOR_SHUTDOWN_MIN_TIMEOUT_S', SHORT_CLOSE_TIMEOUT_S
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


SHORT_GRACE_S = 0.2


def _leave(task_loop: TaskLoop, work: Coroutine[Any, Any, None]) -> asyncio.Task[None]:
    """Run a body that starts ``work`` as a task and returns without it."""
    kept: list[asyncio.Task[None]] = []

    async def body() -> None:
        kept.append(asyncio.get_running_loop().create_task(work))
        await asyncio.sleep(0)

    task_loop.run(body())
    return kept[0]


class TestLeftWorkBounds:
    def test_the_grace_is_one_second(self) -> None:
        """An isolated task has no grace option: this bound is the contract."""
        assert task_loop_module.ISOLATED_LEFTOVER_GRACE_S == 1.0

    def test_the_parent_waits_past_the_grace_and_the_rounds(self) -> None:
        """A child whose loop turns reports the end of the work before the
        parent kills it for reporting nothing."""
        from horsies.core.worker.slots import ISOLATED_LEFTOVER_BACKSTOP_S

        child_bound_s = (
            task_loop_module.ISOLATED_LEFTOVER_GRACE_S
            + (task_loop_module.EXTRA_CANCEL_ROUNDS + 1)
            * task_loop_module.CANCEL_ROUND_INTERVAL_S
        )
        assert ISOLATED_LEFTOVER_BACKSTOP_S > child_bound_s


class TestEndLeftWork:
    """The end of the asyncio tasks that a body left on the loop."""

    @pytest.fixture(autouse=True)
    def _short_grace(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(
            task_loop_module, 'ISOLATED_LEFTOVER_GRACE_S', SHORT_GRACE_S
        )
        monkeypatch.setattr(
            task_loop_module, 'LOOP_CLOSE_TIMEOUT_S', SHORT_CLOSE_TIMEOUT_S
        )

    def test_work_that_ends_within_the_grace_runs_to_its_end(
        self,
        task_loop: TaskLoop,
    ) -> None:
        steps: list[str] = []

        async def flush() -> None:
            await asyncio.sleep(SHORT_GRACE_S / 4)
            steps.append('flushed')

        left = _leave(task_loop, flush())
        started = time.monotonic()
        end = task_loop.end_left_work()
        elapsed = time.monotonic() - started

        assert end is LeftWorkEnd.IN_GRACE
        assert steps == ['flushed']
        assert left.done() and not left.cancelled()
        assert elapsed < SHORT_GRACE_S
        assert task_loop.report() == LoopHoldsNoTask()
        assert not in_running_event_loop()

    def test_a_task_that_the_work_starts_in_the_grace_is_waited_for(
        self,
        task_loop: TaskLoop,
    ) -> None:
        steps: list[str] = []
        kept: list[asyncio.Task[None]] = []

        async def second() -> None:
            await asyncio.sleep(SHORT_GRACE_S / 4)
            steps.append('second')

        async def first() -> None:
            # Start the second task after the wait for the left work began.
            await asyncio.sleep(SHORT_GRACE_S / 8)
            kept.append(asyncio.get_running_loop().create_task(second()))
            steps.append('first')

        _ = _leave(task_loop, first())
        assert task_loop.end_left_work() is LeftWorkEnd.IN_GRACE
        assert steps == ['first', 'second']
        assert task_loop.report() == LoopHoldsNoTask()

    def test_work_alive_after_the_grace_is_cancelled(self, task_loop: TaskLoop) -> None:
        steps: list[str] = []

        async def wait_forever() -> None:
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                steps.append('cancelled')
                raise

        left = _leave(task_loop, wait_forever())
        started = time.monotonic()
        end = task_loop.end_left_work()
        elapsed = time.monotonic() - started

        assert end is LeftWorkEnd.CANCELLED
        assert steps == ['cancelled']
        assert left.cancelled()
        assert (
            SHORT_GRACE_S
            <= elapsed
            < SHORT_GRACE_S + task_loop_module.CANCEL_ROUND_INTERVAL_S
        )
        assert task_loop.report() == LoopHoldsNoTask()

    def test_a_cleanup_after_the_first_cancel_ends_in_the_rounds(
        self,
        task_loop: TaskLoop,
    ) -> None:
        """A client close turns its first cancel into a cleanup shorter than
        one round interval; the next round would cancel that cleanup too."""
        steps: list[str] = []

        async def close_on_cancel() -> None:
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                await asyncio.sleep(task_loop_module.CANCEL_ROUND_INTERVAL_S / 2)
                steps.append('closed')

        _ = _leave(task_loop, close_on_cancel())
        assert task_loop.end_left_work() is LeftWorkEnd.CANCELLED
        assert steps == ['closed']
        assert task_loop.report() == LoopHoldsNoTask()

    def test_work_that_swallows_every_cancel_is_stuck_after_the_rounds(
        self,
        task_loop: TaskLoop,
    ) -> None:
        cancels: list[int] = []

        async def ignore_cancel() -> None:
            while True:
                try:
                    await asyncio.sleep(60)
                except asyncio.CancelledError:
                    cancels.append(1)

        left = _leave(task_loop, ignore_cancel())
        started = time.monotonic()
        end = task_loop.end_left_work()
        elapsed = time.monotonic() - started

        rounds = task_loop_module.EXTRA_CANCEL_ROUNDS + 1
        match end:
            case LeftWorkStuck(still=still):
                assert still.count == 1
                assert still.names[0].endswith('.<locals>.ignore_cancel')
            case other:
                raise AssertionError(other)
        assert len(cancels) == rounds
        assert (
            elapsed >= SHORT_GRACE_S + rounds * task_loop_module.CANCEL_ROUND_INTERVAL_S
        )
        assert (
            elapsed
            < SHORT_GRACE_S + (rounds + 2) * task_loop_module.CANCEL_ROUND_INTERVAL_S
        )
        assert not in_running_event_loop()
        assert isinstance(task_loop.report(), LoopHoldsTasks)
        # The task never ends. Drop it quietly: it is pending on the loop.
        left._log_destroy_pending = False  # pyright: ignore[reportAttributeAccessIssue]

    def test_a_call_with_no_open_loop_is_a_defect(
        self, new_task_loop: TaskLoop
    ) -> None:
        with pytest.raises(AssertionError, match='LoopNotCreated'):
            new_task_loop.end_left_work()

    def test_a_call_inside_a_running_loop_is_a_defect(
        self, task_loop: TaskLoop
    ) -> None:
        async def inside() -> None:
            task_loop.end_left_work()

        with pytest.raises(AssertionError, match='while an event loop runs'):
            asyncio.run(inside())
