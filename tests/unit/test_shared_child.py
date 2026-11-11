"""The seat loop and the frame writer of a shared async child, in process.

The seat loop runs on a real event loop here, fed through the real
``ParentFrameInbox`` (also from another thread). A recording writer stands in
for the socket, and a scripted ``StartedSeats`` for the task runner. The same
loop with real child processes is covered in ``test_slot_pool``.
"""

# pyright: reportPrivateUsage=false

from __future__ import annotations

import asyncio
import concurrent.futures
import contextlib
import contextvars
import dataclasses
import logging
import socket
import threading
import time
from collections.abc import Callable
from typing import Any, NoReturn
from unittest.mock import MagicMock

import pytest

from horsies.core.types.result import Ok, Result
from horsies.core.worker import child_main, run_mark, shared_child
from horsies.core.worker.child_inbox import ParentFrameInbox
from horsies.core.worker.child_main import MAX_FAILED_REASON_CHARS
from horsies.core.worker.child_protocol import (
    BootFailedFrame,
    ChildFrame,
    InitFrame,
    MAX_LEFTOVER_TASK_NAMES,
    DeadlineFiredFrame,
    LeftoverCancelFrame,
    LeftoverStuckFrame,
    ReadyFrame,
    RssMb,
    RssReading,
    RunFrame,
    RunStuckFrame,
    SeatFinished,
    SeatRefusal,
    SeatTask,
    SeatTimedOut,
    SharedAsyncChild,
    SharedResultFrame,
    SharedRunFrame,
    StopFrame,
    WireResult,
)
from horsies.core.models.worker_profile import ResolvedPool
from horsies.core.worker.child_runner import CHILD_HOOK_FAILURE_EXIT_CODE
from horsies.core.worker.shared_child import (
    FRAMES_PER_SEAT,
    FrameWithResult,
    FrameWriter,
    MarkerBeforeDeadline,
    MarkerWindow,
    MarkerWithoutDeadline,
    SeatLoop,
    SeatReturn,
    SetRunning,
    StoppedBeforeRunning,
    bounded_result_frame,
)
from horsies.core.worker.stored_timeout import NoTimeout, TimeoutValueInvalid

EXECUTOR_THREADS = 4
"""The size of the default executor of a test child."""

SIZED_EXECUTOR_THREADS = 3
"""A default executor size below the calls that one test run holds."""


def _kind(seats: int, executor_threads: int) -> SharedAsyncChild:
    return SharedAsyncChild(
        seats=seats,
        pool=ResolvedPool(min_size=0, max_size=2, check=True),
        executor_threads=executor_threads,
        task_send_pool=2,
    )

pytestmark = [pytest.mark.unit]

RUN_TIMEOUT_S = 10.0


class _Exited(BaseException):
    """Raised by the patched exit, as the real one ends the process."""

    def __init__(self, message: str, code: int) -> None:
        super().__init__(message)
        self.code = code


@pytest.fixture
def exits(monkeypatch: pytest.MonkeyPatch) -> list[tuple[str, int]]:
    seen: list[tuple[str, int]] = []

    def record(message: str, exit_code: int) -> NoReturn:
        seen.append((message, exit_code))
        raise _Exited(message, exit_code)

    monkeypatch.setattr(shared_child, '_exit_for_defect', record)
    return seen


class _RecordingWriter:
    """Records frames in order, as the writer thread would write them."""

    def __init__(self) -> None:
        self.frames: list[ChildFrame] = []

    def send(self, frame: ChildFrame) -> None:
        self.frames.append(frame)

    def send_result(self, task_name: str, frame: FrameWithResult) -> None:
        self.frames.append(frame)


class _ScriptedSeats:
    """Each run awaits the behavior registered for its task name, and sets
    its row RUNNING, but a task named in ``not_running``. Each marker is
    recorded with its window, then awaits ``marker`` when one is set."""

    def __init__(self, behaviors: dict[str, Callable[[], Any]]) -> None:
        self._behaviors = behaviors
        self.started: list[int] = []
        self.not_running: set[str] = set()
        self.markers: list[tuple[int, MarkerWindow]] = []
        self.marker: Callable[[MarkerWindow], Any] | None = None

    async def run(self, frame: SharedRunFrame) -> SeatReturn:
        self.started.append(frame.dispatch)
        value = await self._behaviors[frame.task_name]()
        wire: WireResult = (True, str(value), None)
        if frame.task_name in self.not_running:
            return StoppedBeforeRunning(wire)
        return SetRunning(wire)

    async def mark_finalizing(
        self, frame: SharedRunFrame, window: MarkerWindow
    ) -> None:
        self.markers.append((frame.dispatch, window))
        if self.marker is not None:
            await self.marker(window)

    async def stop(self) -> None:
        return None


def _frame(
    dispatch: int,
    task_name: str,
    timeout_ms: int = 60_000,
    grace_ms: int = 1000,
) -> SharedRunFrame:
    return SharedRunFrame(
        dispatch=dispatch,
        task_id=f'task-{dispatch}',
        task_name=task_name,
        args_json=None,
        kwargs_json=None,
        worker_id='worker-1',
        is_workflow_task=False,
        claimed_at=None,
        work=SeatTask(timeout_ms=timeout_ms, cancel_grace_ms=grace_ms),
    )


def _rss() -> RssReading:
    return RssMb(10.0)


class _Harness:
    """A seat loop with a recording writer and scripted seats. ``serve``
    builds the seat loop on the running loop and marks the runs, as
    ``_live`` does."""

    def __init__(self, seats: int, started: _ScriptedSeats) -> None:
        self.seats = seats
        self.writer = _RecordingWriter()
        self.started = started
        self.built: list[SeatLoop] = []

    async def serve(self, inbox: ParentFrameInbox) -> None:
        seat_loop = SeatLoop(self.seats, EXECUTOR_THREADS, self.writer, _rss)  # type: ignore[arg-type]
        seat_loop.install_marks()
        self.built.append(seat_loop)
        await seat_loop.serve(inbox, self.started)


def _seat_loop(
    seats: int,
    behaviors: dict[str, Callable[[], Any]],
) -> tuple[_Harness, _RecordingWriter, _ScriptedSeats]:
    harness = _Harness(seats, _ScriptedSeats(behaviors))
    return harness, harness.writer, harness.started


async def _until(condition: Callable[[], bool]) -> None:
    deadline = time.monotonic() + RUN_TIMEOUT_S
    while not condition():
        assert time.monotonic() < deadline, 'condition not met in time'
        await asyncio.sleep(0.001)


def _results(writer: _RecordingWriter) -> list[SharedResultFrame]:
    return [frame for frame in writer.frames if isinstance(frame, SharedResultFrame)]


class TestSeats:
    def test_every_seat_runs_at_once(self) -> None:
        release = asyncio.Event()

        async def hold() -> int:
            await release.wait()
            return 1

        seat_loop, writer, started = _seat_loop(3, {'hold': hold})
        inbox = ParentFrameInbox()

        async def scenario() -> None:
            serving = asyncio.ensure_future(seat_loop.serve(inbox))
            for dispatch in (1, 2, 3):
                inbox.put(_frame(dispatch, 'hold'))
            await _until(lambda: len(started.started) == 3)
            assert _results(writer) == []
            release.set()
            await _until(lambda: len(_results(writer)) == 3)
            inbox.put(StopFrame())
            await asyncio.wait_for(serving, RUN_TIMEOUT_S)

        asyncio.run(scenario())
        assert sorted(frame.dispatch for frame in _results(writer)) == [1, 2, 3]
        assert all(
            frame.end == SeatFinished(ok=True, result_json='1', failed_reason=None)
            for frame in _results(writer)
        )

    def test_a_frame_from_another_thread_wakes_the_loop(self) -> None:
        async def quick() -> int:
            return 7

        seat_loop, writer, _started = _seat_loop(1, {'quick': quick})
        inbox = ParentFrameInbox()

        async def scenario() -> None:
            serving = asyncio.ensure_future(seat_loop.serve(inbox))
            await asyncio.sleep(0.01)
            putter = threading.Thread(target=inbox.put, args=(_frame(1, 'quick'),))
            putter.start()
            await _until(lambda: len(_results(writer)) == 1)
            putter.join()
            threading.Thread(target=inbox.put, args=(StopFrame(),)).start()
            await asyncio.wait_for(serving, RUN_TIMEOUT_S)

        asyncio.run(scenario())

    def test_a_free_seat_takes_the_next_run_while_others_still_run(self) -> None:
        """A seat is free as soon as its task ends; the others keep running."""
        release_long = asyncio.Event()

        async def long() -> int:
            await release_long.wait()
            return 1

        async def short() -> int:
            return 2

        seat_loop, writer, started = _seat_loop(2, {'long': long, 'short': short})
        inbox = ParentFrameInbox()

        async def scenario() -> None:
            serving = asyncio.ensure_future(seat_loop.serve(inbox))
            inbox.put(_frame(1, 'long'))
            inbox.put(_frame(2, 'short'))
            await _until(lambda: len(_results(writer)) == 1)
            inbox.put(_frame(3, 'short'))
            await _until(lambda: len(_results(writer)) == 2)
            assert [frame.dispatch for frame in _results(writer)] == [2, 3]
            release_long.set()
            await _until(lambda: len(_results(writer)) == 3)
            inbox.put(StopFrame())
            await asyncio.wait_for(serving, RUN_TIMEOUT_S)

        asyncio.run(scenario())
        assert started.started == [1, 2, 3]


class TestDeadline:
    def test_the_deadline_cancels_the_task_and_reports_the_timeout(self) -> None:
        cancelled = asyncio.Event()

        async def hang() -> int:
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                cancelled.set()
                raise
            return 0

        seat_loop, writer, _started = _seat_loop(2, {'hang': hang})
        inbox = ParentFrameInbox()

        async def scenario() -> None:
            serving = asyncio.ensure_future(seat_loop.serve(inbox))
            inbox.put(_frame(1, 'hang', timeout_ms=50))
            await _until(lambda: len(_results(writer)) == 1)
            assert cancelled.is_set()
            inbox.put(StopFrame())
            await asyncio.wait_for(serving, RUN_TIMEOUT_S)

        asyncio.run(scenario())
        assert writer.frames == [
            DeadlineFiredFrame(dispatch=1, task_id='task-1'),
            SharedResultFrame(
                dispatch=1,
                task_id='task-1',
                end=SeatTimedOut(),
                rss=RssMb(10.0),
            ),
        ]

    def test_a_value_after_the_deadline_is_still_the_timeout(self) -> None:
        async def swallow_cancel() -> str:
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                await asyncio.sleep(0.02)
            return 'late value'

        seat_loop, writer, _started = _seat_loop(1, {'swallow': swallow_cancel})
        inbox = ParentFrameInbox()

        async def scenario() -> None:
            serving = asyncio.ensure_future(seat_loop.serve(inbox))
            inbox.put(_frame(1, 'swallow', timeout_ms=30))
            await _until(lambda: len(_results(writer)) == 1)
            inbox.put(StopFrame())
            await asyncio.wait_for(serving, RUN_TIMEOUT_S)

        asyncio.run(scenario())
        assert _results(writer)[0].end == SeatTimedOut()

    def test_the_deadline_of_one_task_leaves_its_neighbours_running(self) -> None:
        release = asyncio.Event()

        async def hang() -> int:
            await asyncio.Event().wait()
            return 0

        async def hold() -> int:
            await release.wait()
            return 5

        seat_loop, writer, _started = _seat_loop(2, {'hang': hang, 'hold': hold})
        inbox = ParentFrameInbox()

        async def scenario() -> None:
            serving = asyncio.ensure_future(seat_loop.serve(inbox))
            inbox.put(_frame(1, 'hang', timeout_ms=30))
            inbox.put(_frame(2, 'hold'))
            await _until(lambda: len(_results(writer)) == 1)
            release.set()
            await _until(lambda: len(_results(writer)) == 2)
            inbox.put(StopFrame())
            await asyncio.wait_for(serving, RUN_TIMEOUT_S)

        asyncio.run(scenario())
        ends = {frame.dispatch: frame.end for frame in _results(writer)}
        assert ends == {
            1: SeatTimedOut(),
            2: SeatFinished(ok=True, result_json='5', failed_reason=None),
        }

    def test_a_task_that_ended_before_its_deadline_keeps_its_result(self) -> None:
        async def quick() -> int:
            return 3

        seat_loop, writer, _started = _seat_loop(1, {'quick': quick})
        inbox = ParentFrameInbox()

        async def scenario() -> None:
            serving = asyncio.ensure_future(seat_loop.serve(inbox))
            inbox.put(_frame(1, 'quick', timeout_ms=100))
            await _until(lambda: len(_results(writer)) == 1)
            await asyncio.sleep(0.2)
            inbox.put(StopFrame())
            await asyncio.wait_for(serving, RUN_TIMEOUT_S)

        asyncio.run(scenario())
        assert writer.frames == [
            SharedResultFrame(
                dispatch=1,
                task_id='task-1',
                end=SeatFinished(ok=True, result_json='3', failed_reason=None),
                rss=RssMb(10.0),
            ),
        ]

    def test_a_refusal_runs_with_no_deadline(
        self,
        exits: list[tuple[str, int]],
    ) -> None:
        """A refusal runs no task code: its seat holds no timer, and it ends
        with its result, never with the timeout. ``exits`` keeps a defect exit
        in the test process when the check fails."""
        release = asyncio.Event()

        async def refuse() -> str:
            await release.wait()
            return 'refused'

        seat_loop, writer, _started = _seat_loop(1, {'refuse': refuse})
        inbox = ParentFrameInbox()
        refusal = dataclasses.replace(
            _frame(1, 'refuse'),
            work=SeatRefusal(refusal=TimeoutValueInvalid(reason='y')),
        )

        async def scenario() -> None:
            serving = asyncio.ensure_future(seat_loop.serve(inbox))
            inbox.put(refusal)
            await _until(
                lambda: bool(seat_loop.built) and 1 in seat_loop.built[0]._seats,
            )
            seat = seat_loop.built[0]._seats[1]
            assert seat.stop is shared_child._Stop.NO_DEADLINE
            assert seat.timer == shared_child._NoTimer()
            release.set()
            await _until(lambda: len(_results(writer)) == 1)
            inbox.put(StopFrame())
            await asyncio.wait_for(serving, RUN_TIMEOUT_S)

        asyncio.run(scenario())
        assert exits == []
        assert writer.frames == [
            SharedResultFrame(
                dispatch=1,
                task_id='task-1',
                end=SeatFinished(ok=True, result_json='refused', failed_reason=None),
                rss=RssMb(10.0),
            ),
        ]


def _kinds(writer: _RecordingWriter, dispatch: int) -> list[str]:
    """The kinds of the frames about run ``dispatch``, in order."""
    return [
        type(frame).__name__
        for frame in writer.frames
        if getattr(frame, 'dispatch', None) == dispatch
    ]


def _run_to_end(
    behaviors: dict[str, Callable[[], Any]],
    frames: list[SharedRunFrame],
    ended: Callable[[_RecordingWriter], bool],
) -> _RecordingWriter:
    """Serve ``frames`` until ``ended`` holds, then stop."""
    harness, writer, _started = _seat_loop(len(frames), behaviors)
    inbox = ParentFrameInbox()

    async def scenario() -> None:
        serving = asyncio.ensure_future(harness.serve(inbox))
        for frame in frames:
            inbox.put(frame)
        await _until(lambda: ended(writer))
        inbox.put(StopFrame())
        await asyncio.wait_for(serving, RUN_TIMEOUT_S)

    asyncio.run(scenario())
    return writer


def _results_of(count: int) -> Callable[[_RecordingWriter], bool]:
    return lambda writer: len(_results(writer)) == count


DEADLINE_MS = 50
GRACE_MS = 80


class TestTheWorkOfARun:
    """A run ends when its coroutine and every task and executor item with
    its mark ended."""

    def test_a_run_waits_for_a_task_it_left(self) -> None:
        left_done = asyncio.Event()

        async def leave() -> int:
            async def later() -> None:
                await asyncio.sleep(0.2)
                left_done.set()

            _ = asyncio.get_running_loop().create_task(later())
            return 1

        started = time.monotonic()
        writer = _run_to_end({'leave': leave}, [_frame(1, 'leave')], _results_of(1))

        assert time.monotonic() - started >= 0.2
        assert left_done.is_set()
        assert _results(writer)[0].end == SeatFinished(True, '1', None)

    def test_a_task_built_directly_is_found_by_the_scan(self) -> None:
        """As aiohttp builds its DNS lookup task: ``asyncio.Task(...)``
        bypasses the task factory, and still carries the mark."""
        left_done = asyncio.Event()

        async def leave_direct() -> int:
            async def later() -> None:
                await asyncio.sleep(0.2)
                left_done.set()

            _ = asyncio.Task(later(), loop=asyncio.get_running_loop())
            return 1

        writer = _run_to_end(
            {'direct': leave_direct},
            [_frame(1, 'direct')],
            _results_of(1),
        )

        assert left_done.is_set()
        assert _results(writer)[0].end == SeatFinished(True, '1', None)

    def test_a_run_waits_for_its_executor_work(self) -> None:
        thread_done = threading.Event()

        def sync_call() -> None:
            time.sleep(0.2)
            thread_done.set()

        async def start_thread() -> int:
            _ = asyncio.get_running_loop().run_in_executor(None, sync_call)
            return 1

        writer = _run_to_end(
            {'thread': start_thread},
            [_frame(1, 'thread')],
            _results_of(1),
        )

        assert thread_done.is_set()
        assert _results(writer)[0].end == SeatFinished(True, '1', None)

    def test_work_with_no_mark_is_not_waited_for(self) -> None:
        """A task with an empty context, as a start hook creates."""
        never = asyncio.Event()

        async def leave_unmarked() -> int:
            _ = asyncio.get_running_loop().create_task(
                never.wait(),
                context=contextvars.Context(),
            )
            return 1

        writer = _run_to_end(
            {'unmarked': leave_unmarked},
            [_frame(1, 'unmarked')],
            _results_of(1),
        )

        assert _results(writer)[0].end == SeatFinished(True, '1', None)

    def test_a_neighbour_does_not_wait_for_the_work_of_another_run(self) -> None:
        release = asyncio.Event()

        async def leave_until_released() -> int:
            _ = asyncio.get_running_loop().create_task(release.wait())
            return 1

        async def quick() -> int:
            return 2

        harness, writer, _started = _seat_loop(
            2,
            {'leave': leave_until_released, 'quick': quick},
        )
        inbox = ParentFrameInbox()

        async def scenario() -> None:
            serving = asyncio.ensure_future(harness.serve(inbox))
            inbox.put(_frame(1, 'leave'))
            inbox.put(_frame(2, 'quick'))
            await _until(lambda: len(_results(writer)) == 1)
            assert _results(writer)[0].dispatch == 2
            release.set()
            await _until(lambda: len(_results(writer)) == 2)
            inbox.put(StopFrame())
            await asyncio.wait_for(serving, RUN_TIMEOUT_S)

        asyncio.run(scenario())


class TestTheStopOfARun:
    """The steps at T, at T + G, and after the cancel rounds."""

    def test_a_cooperating_task_ends_at_the_first_cancel(self) -> None:
        async def hang() -> int:
            await asyncio.Event().wait()
            return 0

        writer = _run_to_end(
            {'hang': hang},
            [_frame(1, 'hang', DEADLINE_MS, GRACE_MS)],
            _results_of(1),
        )

        assert _kinds(writer, 1) == ['DeadlineFiredFrame', 'SharedResultFrame']
        assert _results(writer)[0].end == SeatTimedOut()

    def test_marked_work_after_the_cancel_is_waited_for_within_the_grace(
        self,
    ) -> None:
        """The task ends at the cancel; the shielded call it left ends before
        T + G, so no second cancel is needed."""
        inner_done = asyncio.Event()

        async def shielded() -> int:
            async def inner() -> None:
                await asyncio.sleep(0.04)
                inner_done.set()

            await asyncio.shield(asyncio.get_running_loop().create_task(inner()))
            return 0

        writer = _run_to_end(
            {'shielded': shielded},
            [_frame(1, 'shielded', 20, 200)],
            _results_of(1),
        )

        assert inner_done.is_set()
        assert _kinds(writer, 1) == ['DeadlineFiredFrame', 'SharedResultFrame']
        assert _results(writer)[0].end == SeatTimedOut()

    @pytest.mark.parametrize(
        'case',
        ['suppress_then_work', 'shield_then_wait', 'hung_cleanup'],
    )
    def test_the_second_cancel_ends_what_the_first_did_not(self, case: str) -> None:
        async def suppress_then_work() -> int:
            with contextlib.suppress(asyncio.CancelledError):
                await asyncio.Event().wait()
            await asyncio.sleep(30)
            return 1

        async def shield_then_wait() -> int:
            inner = asyncio.get_running_loop().create_task(asyncio.sleep(30))
            try:
                await asyncio.shield(inner)
            except asyncio.CancelledError:
                await inner
            return 1

        async def hung_cleanup() -> int:
            try:
                await asyncio.Event().wait()
            finally:
                await asyncio.sleep(30)
            return 1

        behaviors: dict[str, Callable[[], Any]] = {
            'suppress_then_work': suppress_then_work,
            'shield_then_wait': shield_then_wait,
            'hung_cleanup': hung_cleanup,
        }
        writer = _run_to_end(
            behaviors,
            [_frame(1, case, DEADLINE_MS, GRACE_MS)],
            _results_of(1),
        )

        assert _kinds(writer, 1) == [
            'DeadlineFiredFrame',
            'SecondCancelFrame',
            'SharedResultFrame',
        ]
        assert _results(writer)[0].end == SeatTimedOut()

    def test_a_later_round_ends_work_that_the_second_cancel_started(self) -> None:
        """As an asyncpg pool does: the cancel of one marked task starts a
        refill task in the run's context. The next round cancels it."""
        rounds_seen: list[str] = []

        async def refill() -> None:
            rounds_seen.append('refill started')
            await asyncio.Event().wait()

        async def release_on_cancel() -> None:
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                _ = asyncio.get_running_loop().create_task(refill())
                raise

        async def with_pool() -> int:
            holder = asyncio.get_running_loop().create_task(release_on_cancel())
            with contextlib.suppress(asyncio.CancelledError):
                await asyncio.Event().wait()
            await holder
            return 1

        writer = _run_to_end(
            {'with_pool': with_pool},
            [_frame(1, 'with_pool', DEADLINE_MS, GRACE_MS)],
            _results_of(1),
        )

        assert rounds_seen == ['refill started']
        assert _kinds(writer, 1) == [
            'DeadlineFiredFrame',
            'SecondCancelFrame',
            'SharedResultFrame',
        ]

    def test_a_task_that_swallows_every_cancel_is_reported_stuck(self) -> None:
        stop = threading.Event()

        async def swallow_all() -> int:
            give_up_at = time.monotonic() + RUN_TIMEOUT_S
            while not stop.is_set() and time.monotonic() < give_up_at:
                try:
                    await asyncio.sleep(0.01)
                except asyncio.CancelledError:
                    continue
            return 1

        def stuck_then_released(writer: _RecordingWriter) -> bool:
            if any(isinstance(frame, RunStuckFrame) for frame in writer.frames):
                stop.set()
            return len(_results(writer)) == 1

        writer = _run_to_end(
            {'swallow_all': swallow_all},
            [_frame(1, 'swallow_all', DEADLINE_MS, GRACE_MS)],
            stuck_then_released,
        )

        assert _kinds(writer, 1) == [
            'DeadlineFiredFrame',
            'SecondCancelFrame',
            'RunStuckFrame',
            'SharedResultFrame',
        ]
        stuck = next(f for f in writer.frames if isinstance(f, RunStuckFrame))
        assert (stuck.tasks, stuck.executor_items) == (1, 0)
        assert stuck.names == ('_ScriptedSeats.run',)
        assert _results(writer)[0].end == SeatTimedOut()

    def test_an_executor_thread_that_still_runs_is_stuck(self) -> None:
        release = threading.Event()

        async def wait_on_thread() -> int:
            await asyncio.to_thread(release.wait, RUN_TIMEOUT_S)
            return 1

        def stuck_then_released(writer: _RecordingWriter) -> bool:
            if any(isinstance(frame, RunStuckFrame) for frame in writer.frames):
                release.set()
            return len(_results(writer)) == 1

        writer = _run_to_end(
            {'thread': wait_on_thread},
            [_frame(1, 'thread', DEADLINE_MS, GRACE_MS)],
            stuck_then_released,
        )

        stuck = next(f for f in writer.frames if isinstance(f, RunStuckFrame))
        assert (stuck.tasks, stuck.executor_items) == (0, 1)
        assert _kinds(writer, 1)[-1] == 'SharedResultFrame'
        assert _results(writer)[0].end == SeatTimedOut()

    def test_work_left_after_a_return_that_outlives_the_deadline_keeps_the_result(
        self,
    ) -> None:
        """The body returned before T, and its marked work outlived T: the
        deadline no longer applies. The work is cancelled after the grace,
        and the run ends with what the body returned."""

        async def leave_forever() -> int:
            _ = asyncio.get_running_loop().create_task(asyncio.Event().wait())
            return 1

        writer = _run_to_end(
            {'leave': leave_forever},
            [_frame(1, 'leave', DEADLINE_MS, GRACE_MS)],
            _results_of(1),
        )

        assert _kinds(writer, 1) == ['LeftoverCancelFrame', 'SharedResultFrame']
        assert _results(writer)[0].end == SeatFinished(True, '1', None)

    def test_the_stop_of_one_run_leaves_its_neighbour_running(self) -> None:
        release = asyncio.Event()
        stop = threading.Event()

        async def swallow_all() -> int:
            give_up_at = time.monotonic() + RUN_TIMEOUT_S
            while not stop.is_set() and time.monotonic() < give_up_at:
                with contextlib.suppress(asyncio.CancelledError):
                    await asyncio.sleep(0.01)
            return 0

        async def hold() -> int:
            await release.wait()
            return 5

        harness, writer, _started = _seat_loop(
            2,
            {'swallow_all': swallow_all, 'hold': hold},
        )
        inbox = ParentFrameInbox()

        async def scenario() -> None:
            serving = asyncio.ensure_future(harness.serve(inbox))
            inbox.put(_frame(1, 'swallow_all', DEADLINE_MS, GRACE_MS))
            inbox.put(_frame(2, 'hold'))
            await _until(
                lambda: any(isinstance(f, RunStuckFrame) for f in writer.frames),
            )
            release.set()
            await _until(lambda: len(_results(writer)) == 1)
            stop.set()
            await _until(lambda: len(_results(writer)) == 2)
            inbox.put(StopFrame())
            await asyncio.wait_for(serving, RUN_TIMEOUT_S)

        asyncio.run(scenario())
        ends = {frame.dispatch: frame.end for frame in _results(writer)}
        assert ends == {1: SeatTimedOut(), 2: SeatFinished(True, '5', None)}
        assert _kinds(writer, 2) == ['SharedResultFrame']


def _left_work_lines(caplog: pytest.LogCaptureFixture) -> list[logging.LogRecord]:
    return [
        record
        for record in caplog.records
        if record.name == 'horsies.worker'
        and 'returned and left work' in record.message
    ]


async def _swallow_every_cancel(stop: threading.Event) -> None:
    """Ignore each cancel until ``stop`` is set, bounded by ``RUN_TIMEOUT_S``."""
    give_up_at = time.monotonic() + RUN_TIMEOUT_S
    while not stop.is_set() and time.monotonic() < give_up_at:
        with contextlib.suppress(asyncio.CancelledError):
            await asyncio.sleep(0.01)


class TestWorkLeftAfterAReturn:
    """A coroutine returned before its deadline while marked work was alive:
    the run ends with what it returned, after that work ended, was
    cancelled, or was reported stuck."""

    def test_work_that_ends_within_the_grace_keeps_the_result(
        self,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        caplog.set_level(logging.INFO, logger='horsies')

        async def flush_later() -> None:
            await asyncio.sleep(0.05)

        async def leave() -> int:
            _ = asyncio.get_running_loop().create_task(flush_later())
            return 1

        writer = _run_to_end(
            {'leave': leave},
            [_frame(1, 'leave', grace_ms=2_000)],
            _results_of(1),
        )

        assert _kinds(writer, 1) == ['SharedResultFrame']
        assert _results(writer)[0].end == SeatFinished(True, '1', None)
        lines = _left_work_lines(caplog)
        assert len(lines) == 1
        assert lines[0].levelno == logging.INFO
        assert 'ended within the cancel grace' in lines[0].message
        assert 'flush_later' in lines[0].message
        assert 'leave (task-1, run 1)' in lines[0].message

    def test_work_alive_after_the_grace_is_cancelled_and_the_result_kept(
        self,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        cancelled = asyncio.Event()

        async def keepalive() -> None:
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                cancelled.set()
                raise

        async def leave() -> int:
            _ = asyncio.get_running_loop().create_task(keepalive())
            return 1

        writer = _run_to_end(
            {'leave': leave},
            [_frame(1, 'leave', grace_ms=GRACE_MS)],
            _results_of(1),
        )

        assert cancelled.is_set()
        assert _kinds(writer, 1) == ['LeftoverCancelFrame', 'SharedResultFrame']
        assert _results(writer)[0].end == SeatFinished(True, '1', None)
        lines = _left_work_lines(caplog)
        assert [line.levelno for line in lines] == [logging.WARNING]
        assert 'was cancelled after the cancel grace' in lines[0].message
        assert 'keepalive' in lines[0].message

    def test_work_that_swallows_every_cancel_is_stuck_with_the_returned_outcome(
        self,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        stop = threading.Event()

        async def leave() -> int:
            _ = asyncio.get_running_loop().create_task(_swallow_every_cancel(stop))
            return 1

        def stuck_then_released(writer: _RecordingWriter) -> bool:
            if any(isinstance(frame, LeftoverStuckFrame) for frame in writer.frames):
                stop.set()
            return len(_results(writer)) == 1

        writer = _run_to_end(
            {'leave': leave},
            [_frame(1, 'leave', grace_ms=GRACE_MS)],
            stuck_then_released,
        )

        assert _kinds(writer, 1) == [
            'LeftoverCancelFrame',
            'LeftoverStuckFrame',
            'SharedResultFrame',
        ]
        stuck = next(f for f in writer.frames if isinstance(f, LeftoverStuckFrame))
        assert stuck.returned == SeatFinished(True, '1', None)
        assert (stuck.tasks, stuck.executor_items) == (1, 0)
        assert stuck.names == ('_swallow_every_cancel',)
        assert _results(writer)[0].end == SeatFinished(True, '1', None)
        lines = _left_work_lines(caplog)
        assert [line.levelno for line in lines] == [logging.WARNING]
        assert 'did not stop after the cancel rounds' in lines[0].message

    def test_an_executor_thread_left_after_the_return_is_stuck(self) -> None:
        release = threading.Event()

        async def leave_thread() -> int:
            _ = asyncio.get_running_loop().run_in_executor(
                None,
                release.wait,
                RUN_TIMEOUT_S,
            )
            return 1

        def stuck_then_released(writer: _RecordingWriter) -> bool:
            if any(isinstance(frame, LeftoverStuckFrame) for frame in writer.frames):
                release.set()
            return len(_results(writer)) == 1

        writer = _run_to_end(
            {'thread': leave_thread},
            [_frame(1, 'thread', grace_ms=GRACE_MS)],
            stuck_then_released,
        )

        stuck = next(f for f in writer.frames if isinstance(f, LeftoverStuckFrame))
        assert (stuck.tasks, stuck.executor_items, stuck.names) == (0, 1, ())
        assert stuck.returned == SeatFinished(True, '1', None)
        assert _results(writer)[0].end == SeatFinished(True, '1', None)

    def test_the_deadline_does_not_fire_after_the_return(
        self,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """The work ends past T and within the grace after the return: no
        deadline report, and the run keeps what it returned. A deadline timer
        left armed would fire on a run past that step, which the loop logs
        as an error."""

        async def flush_past_the_deadline() -> None:
            await asyncio.sleep(0.15)

        async def leave() -> int:
            _ = asyncio.get_running_loop().create_task(flush_past_the_deadline())
            return 1

        writer = _run_to_end(
            {'leave': leave},
            [_frame(1, 'leave', timeout_ms=30, grace_ms=2_000)],
            _results_of(1),
        )

        assert _kinds(writer, 1) == ['SharedResultFrame']
        assert _results(writer)[0].end == SeatFinished(True, '1', None)
        assert [r.message for r in caplog.records if r.levelno >= logging.ERROR] == []

    def test_a_deadline_that_finds_the_coroutine_returned_keeps_the_result(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """The deadline fires after the coroutine returned and before its
        done callback ran: the coroutine returned in time."""
        original_finish = SeatLoop._finish

        def deadline_first(
            seat_loop: SeatLoop,
            dispatch: int,
            task: asyncio.Task[SeatReturn],
        ) -> None:
            seat = seat_loop._seats.get(dispatch)
            if seat is not None and seat.stop is shared_child._Stop.BEFORE_DEADLINE:
                shared_child._cancel_timer(seat)
                seat_loop._fire(dispatch)
            original_finish(seat_loop, dispatch, task)

        monkeypatch.setattr(SeatLoop, '_finish', deadline_first)

        async def leave() -> int:
            _ = asyncio.get_running_loop().create_task(asyncio.sleep(0.05))
            return 1

        writer = _run_to_end(
            {'leave': leave},
            [_frame(1, 'leave', grace_ms=2_000)],
            _results_of(1),
        )

        assert _kinds(writer, 1) == ['SharedResultFrame']
        assert _results(writer)[0].end == SeatFinished(True, '1', None)

    def test_a_neighbour_runs_on_while_the_work_of_a_return_is_cancelled(
        self,
    ) -> None:
        release = asyncio.Event()

        async def leave() -> int:
            _ = asyncio.get_running_loop().create_task(asyncio.Event().wait())
            return 1

        async def hold() -> int:
            await release.wait()
            return 5

        harness, writer, _started = _seat_loop(2, {'leave': leave, 'hold': hold})
        inbox = ParentFrameInbox()

        async def scenario() -> None:
            serving = asyncio.ensure_future(harness.serve(inbox))
            inbox.put(_frame(1, 'leave', grace_ms=GRACE_MS))
            inbox.put(_frame(2, 'hold'))
            await _until(lambda: len(_results(writer)) == 1)
            assert _results(writer)[0].dispatch == 1
            release.set()
            await _until(lambda: len(_results(writer)) == 2)
            inbox.put(StopFrame())
            await asyncio.wait_for(serving, RUN_TIMEOUT_S)

        asyncio.run(scenario())
        ends = {frame.dispatch: frame.end for frame in _results(writer)}
        assert ends == {
            1: SeatFinished(True, '1', None),
            2: SeatFinished(True, '5', None),
        }
        assert _kinds(writer, 2) == ['SharedResultFrame']

    def test_a_refusal_that_leaves_work_waits_for_it_with_no_grace(
        self,
        exits: list[tuple[str, int]],
    ) -> None:
        """A refusal has no deadline and no cancel grace: its run waits for
        its marked work, and nothing is cancelled."""

        async def flush_later() -> None:
            await asyncio.sleep(0.3)

        async def refuse() -> str:
            _ = asyncio.get_running_loop().create_task(flush_later())
            return 'refused'

        refusal = dataclasses.replace(
            _frame(1, 'refuse'),
            work=SeatRefusal(refusal=TimeoutValueInvalid(reason='y')),
        )
        started = time.monotonic()
        writer = _run_to_end({'refuse': refuse}, [refusal], _results_of(1))

        assert exits == []
        assert time.monotonic() - started >= 0.3
        assert _kinds(writer, 1) == ['SharedResultFrame']
        assert _results(writer)[0].end == SeatFinished(True, 'refused', None)

    def test_the_log_line_names_the_work_within_its_bound(
        self,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        caplog.set_level(logging.INFO, logger='horsies')
        extra = 2

        async def leave_many() -> int:
            loop = asyncio.get_running_loop()
            for _ in range(MAX_LEFTOVER_TASK_NAMES + extra):
                _ = loop.create_task(asyncio.sleep(0.05))
            return 1

        _ = _run_to_end(
            {'many': leave_many},
            [_frame(1, 'many', grace_ms=2_000)],
            _results_of(1),
        )

        lines = _left_work_lines(caplog)
        assert len(lines) == 1
        assert lines[0].message.count('sleep') == MAX_LEFTOVER_TASK_NAMES
        assert f'and {extra} more' in lines[0].message


class TestTheLeftoverCancelReport:
    def test_the_report_carries_what_the_task_returned(self) -> None:
        """The worker writes what a task returned at any kill once it holds
        it: the leftover cancel report gives it at the end of the grace."""

        async def leave() -> int:
            async def holds_on() -> None:
                await asyncio.Event().wait()

            _ = asyncio.get_running_loop().create_task(holds_on())
            return 1

        writer = _run_to_end(
            {'leave': leave},
            [_frame(1, 'leave', grace_ms=GRACE_MS)],
            _results_of(1),
        )

        [report] = [
            frame for frame in writer.frames if isinstance(frame, LeftoverCancelFrame)
        ]
        assert report.returned == SeatFinished(True, '1', None)


class TestTheCostOfTheEndChecks:
    def test_runs_that_return_in_one_turn_share_one_scan(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """20 runs return in one turn, beside 200 live tasks of no run: the
        end of each needs the live tasks of its run, and they share one
        scan of the loop, not one each."""
        scans: list[int] = []
        all_tasks = asyncio.all_tasks

        def counted(
            loop: asyncio.AbstractEventLoop | None = None,
        ) -> set[asyncio.Task[Any]]:
            scans.append(1)
            return all_tasks(loop)

        monkeypatch.setattr(run_mark.asyncio, 'all_tasks', counted)
        release = asyncio.Event()
        runs = 20

        async def wait_for_release() -> int:
            await release.wait()
            return 1

        seat_loop, writer, started = _seat_loop(runs, {'wait': wait_for_release})
        inbox = ParentFrameInbox()

        async def scenario() -> None:
            idle = asyncio.Event()
            background = [
                asyncio.get_running_loop().create_task(idle.wait()) for _ in range(200)
            ]
            serving = asyncio.ensure_future(seat_loop.serve(inbox))
            for dispatch in range(1, runs + 1):
                inbox.put(_frame(dispatch, 'wait'))
            await _until(lambda: len(started.started) == runs)
            scans.clear()
            release.set()
            await _until(lambda: len(_results(writer)) == runs)
            assert len(scans) <= 2, scans
            inbox.put(StopFrame())
            await asyncio.wait_for(serving, RUN_TIMEOUT_S)
            idle.set()
            await asyncio.gather(*background)

        asyncio.run(scenario())


class TestTheMarkerAtTheEnd:
    """The finalizing marker of a run comes at its end, after its coroutine
    and its marked work, and before its end report. Only a run that set its
    row RUNNING and ended without a cancel writes it, within the time left
    before its deadline."""

    def _serve(
        self,
        seats: _ScriptedSeats,
        frames: list[SharedRunFrame],
        between: Callable[[SeatLoop, _RecordingWriter], Any] | None = None,
    ) -> _RecordingWriter:
        harness = _Harness(len(frames), seats)
        inbox = ParentFrameInbox()

        async def scenario() -> None:
            serving = asyncio.ensure_future(harness.serve(inbox))
            for frame in frames:
                inbox.put(frame)
            if between is not None:
                await _until(lambda: bool(harness.built))
                await between(harness.built[0], harness.writer)
            await _until(lambda: len(_results(harness.writer)) == len(frames))
            inbox.put(StopFrame())
            await asyncio.wait_for(serving, RUN_TIMEOUT_S)

        asyncio.run(scenario())
        return harness.writer

    def test_the_window_is_the_time_left_before_the_deadline(self) -> None:
        async def quick() -> int:
            return 1

        seats = _ScriptedSeats({'quick': quick})

        self._serve(seats, [_frame(1, 'quick', timeout_ms=2_000)])

        [(dispatch, window)] = seats.markers
        assert dispatch == 1
        assert isinstance(window, MarkerBeforeDeadline)
        assert 1.5 < window.seconds <= 2.0

    def test_the_end_report_follows_the_marker_and_the_run_keeps_its_seat(
        self,
    ) -> None:
        release = asyncio.Event()

        async def quick() -> int:
            return 1

        async def held_marker(_window: MarkerWindow) -> None:
            await release.wait()

        seats = _ScriptedSeats({'quick': quick})
        seats.marker = held_marker

        async def between(seat_loop: SeatLoop, writer: _RecordingWriter) -> None:
            await _until(lambda: len(seats.markers) == 1)
            await asyncio.sleep(0.05)
            assert _results(writer) == []
            assert isinstance(seat_loop._seats[1].stop, shared_child._Finalizing)
            release.set()

        writer = self._serve(seats, [_frame(1, 'quick')], between)

        assert _kinds(writer, 1) == ['SharedResultFrame']
        assert _results(writer)[0].end == SeatFinished(True, '1', None)

    def test_a_refusal_writes_its_marker_without_a_deadline(self) -> None:
        async def refuse() -> str:
            return 'refused'

        seats = _ScriptedSeats({'refuse': refuse})
        refusal = dataclasses.replace(
            _frame(1, 'refuse'),
            work=SeatRefusal(refusal=TimeoutValueInvalid(reason='y')),
        )

        self._serve(seats, [refusal])

        assert seats.markers == [(1, MarkerWithoutDeadline())]

    def test_a_run_that_stopped_before_running_writes_no_marker(self) -> None:
        async def lost() -> str:
            return 'claim lost'

        seats = _ScriptedSeats({'lost': lost})
        seats.not_running.add('lost')

        writer = self._serve(seats, [_frame(1, 'lost')])

        assert seats.markers == []
        assert _results(writer)[0].end == SeatFinished(True, 'claim lost', None)

    def test_a_run_that_timed_out_writes_no_marker(self) -> None:
        async def hang() -> int:
            await asyncio.Event().wait()
            return 0

        seats = _ScriptedSeats({'hang': hang})

        writer = self._serve(
            seats,
            [_frame(1, 'hang', timeout_ms=DEADLINE_MS, grace_ms=GRACE_MS)],
        )

        assert seats.markers == []
        assert _kinds(writer, 1) == ['DeadlineFiredFrame', 'SharedResultFrame']

    def test_a_return_whose_work_needed_a_cancel_writes_no_marker(self) -> None:
        """The run keeps what it returned, but its stop reached the cancel
        rounds: it reports its end at once."""

        async def leave() -> int:
            async def holds_on() -> None:
                await asyncio.Event().wait()

            _ = asyncio.get_running_loop().create_task(holds_on())
            return 1

        seats = _ScriptedSeats({'leave': leave})

        writer = self._serve(
            seats,
            [_frame(1, 'leave', grace_ms=GRACE_MS)],
        )

        assert seats.markers == []
        assert _kinds(writer, 1) == ['LeftoverCancelFrame', 'SharedResultFrame']
        assert _results(writer)[0].end == SeatFinished(True, '1', None)

    def test_a_return_whose_work_ended_in_the_grace_writes_the_marker(self) -> None:
        async def leave() -> int:
            _ = asyncio.get_running_loop().create_task(asyncio.sleep(0.05))
            return 1

        seats = _ScriptedSeats({'leave': leave})

        writer = self._serve(seats, [_frame(1, 'leave', grace_ms=2_000)])

        assert [dispatch for dispatch, _window in seats.markers] == [1]
        assert _kinds(writer, 1) == ['SharedResultFrame']


class TestTheStartOfTheLoop:
    """``_live``, the real caller of the start steps, on a real loop: the
    marks survive a start hook that sets its own default executor."""

    def test_work_in_the_executor_of_a_hook_holds_the_run(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """A run sends a call to the default executor and returns without
        awaiting it. The start hook replaced that executor; the run still
        ends only when the call ends, on a thread of the hook's executor."""
        ran_on: list[str] = []

        def sync_call() -> None:
            ran_on.append(threading.current_thread().name)
            time.sleep(0.3)

        async def leave_a_call() -> int:
            _ = asyncio.get_running_loop().run_in_executor(None, sync_call)
            return 1

        seats = _ScriptedSeats({'leave_a_call': leave_a_call})

        class _HookSetsItsExecutor:
            async def start(
                self,
                init: InitFrame,
                pool: ResolvedPool,
            ) -> Result[shared_child.StartedSeats, BootFailedFrame]:
                _ = init, pool
                asyncio.get_running_loop().set_default_executor(
                    concurrent.futures.ThreadPoolExecutor(
                        max_workers=4,
                        thread_name_prefix='hook-pool',
                    ),
                )
                return Ok(seats)

        monkeypatch.setattr(shared_child, '_read_rss', _rss)
        writer = _RecordingWriter()
        inbox = ParentFrameInbox()
        took_s: list[float] = []
        parent_end, child_end = socket.socketpair()

        async def scenario() -> None:
            living = asyncio.ensure_future(
                shared_child._live(
                    child_end,
                    MagicMock(),
                    inbox,
                    _kind(seats=1, executor_threads=EXECUTOR_THREADS),
                    _HookSetsItsExecutor(),
                    writer,  # type: ignore[arg-type]
                ),
            )
            await _until(lambda: any(isinstance(f, ReadyFrame) for f in writer.frames))
            sent = time.monotonic()
            inbox.put(_frame(1, 'leave_a_call', grace_ms=5_000))
            await _until(lambda: len(_results(writer)) == 1)
            took_s.append(time.monotonic() - sent)
            inbox.put(StopFrame())
            await asyncio.wait_for(living, RUN_TIMEOUT_S)

        try:
            asyncio.run(scenario())
        finally:
            parent_end.close()
            child_end.close()

        assert ran_on and ran_on[0].startswith('hook-pool')
        assert took_s[0] >= 0.25
        assert _results(writer)[0].end == SeatFinished(True, '1', None)

    def test_the_kind_sizes_the_executor_and_the_pool_of_the_start(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """``_live`` sizes the default executor of the loop with
        ``executor_threads`` of the kind, and gives the start steps the pool
        of the kind. A run that holds more calls in the executor than it has
        threads runs only that many at once."""
        calls = 2 * SIZED_EXECUTOR_THREADS
        ran_on: set[str] = set()
        release = threading.Event()
        lock = threading.Lock()

        def sync_call() -> None:
            with lock:
                ran_on.add(threading.current_thread().name)
            _ = release.wait(RUN_TIMEOUT_S)

        async def hold_calls() -> int:
            loop = asyncio.get_running_loop()
            pending = [loop.run_in_executor(None, sync_call) for _ in range(calls)]
            await asyncio.sleep(0.1)
            release.set()
            await asyncio.gather(*pending)
            return len(pending)

        seats = _ScriptedSeats({'hold_calls': hold_calls})
        pools: list[ResolvedPool] = []

        class _RecordsThePool:
            async def start(
                self,
                init: InitFrame,
                pool: ResolvedPool,
            ) -> Result[shared_child.StartedSeats, BootFailedFrame]:
                _ = init
                pools.append(pool)
                return Ok(seats)

        monkeypatch.setattr(shared_child, '_read_rss', _rss)
        writer = _RecordingWriter()
        inbox = ParentFrameInbox()
        parent_end, child_end = socket.socketpair()
        kind = SharedAsyncChild(
            seats=1,
            pool=ResolvedPool(min_size=1, max_size=5, check=False),
            executor_threads=SIZED_EXECUTOR_THREADS,
            task_send_pool=2,
        )

        async def scenario() -> None:
            living = asyncio.ensure_future(
                shared_child._live(
                    child_end,
                    MagicMock(),
                    inbox,
                    kind,
                    _RecordsThePool(),
                    writer,  # type: ignore[arg-type]
                ),
            )
            await _until(lambda: any(isinstance(f, ReadyFrame) for f in writer.frames))
            inbox.put(_frame(1, 'hold_calls', grace_ms=5_000))
            await _until(lambda: len(_results(writer)) == 1)
            inbox.put(StopFrame())
            await asyncio.wait_for(living, RUN_TIMEOUT_S)

        try:
            asyncio.run(scenario())
        finally:
            parent_end.close()
            child_end.close()

        assert pools == [ResolvedPool(min_size=1, max_size=5, check=False)]
        assert _results(writer)[0].end == SeatFinished(True, str(calls), None)
        assert len(ran_on) == SIZED_EXECUTOR_THREADS

    def test_a_default_executor_that_is_not_a_thread_pool_fails_the_start(
        self,
        exits: list[tuple[str, int]],
    ) -> None:
        async def scenario() -> None:
            seat_loop = SeatLoop(1, EXECUTOR_THREADS, _RecordingWriter(), _rss)  # type: ignore[arg-type]
            seat_loop.install_marks()
            loop = asyncio.get_running_loop()
            loop._default_executor = None  # pyright: ignore[reportAttributeAccessIssue]
            with pytest.raises(_Exited):
                seat_loop.keep_marks_after_hooks()

        asyncio.run(scenario())

        [(message, code)] = exits
        assert code == CHILD_HOOK_FAILURE_EXIT_CODE
        assert 'NoneType, not a ThreadPoolExecutor' in message


class TestParentDefects:
    def _serve_until_exit(self, seats: int, frames: list[Any]) -> None:
        async def hold() -> int:
            await asyncio.Event().wait()
            return 0

        seat_loop, _writer, _started = _seat_loop(seats, {'hold': hold})
        inbox = ParentFrameInbox()
        for frame in frames:
            inbox.put(frame)

        async def scenario() -> None:
            await asyncio.wait_for(seat_loop.serve(inbox), RUN_TIMEOUT_S)

        with pytest.raises(_Exited):
            asyncio.run(scenario())

    def test_a_run_beyond_the_seats_ends_the_child(
        self,
        exits: list[tuple[str, int]],
    ) -> None:
        self._serve_until_exit(1, [_frame(1, 'hold'), _frame(2, 'hold')])
        assert exits[0][1] == child_main.BAD_PARENT_FRAME_EXIT_CODE
        assert 'all 1 seats are taken' in exits[0][0]

    def test_a_dispatch_number_sent_twice_ends_the_child(
        self,
        exits: list[tuple[str, int]],
    ) -> None:
        self._serve_until_exit(2, [_frame(1, 'hold'), _frame(1, 'hold')])
        assert 'sent run 1 twice' in exits[0][0]

    def test_a_stop_frame_with_runs_in_flight_ends_the_child(
        self,
        exits: list[tuple[str, int]],
    ) -> None:
        self._serve_until_exit(2, [_frame(1, 'hold'), StopFrame()])
        assert 'runs are in flight' in exits[0][0]

    def test_an_isolated_run_frame_ends_the_child(
        self,
        exits: list[tuple[str, int]],
    ) -> None:
        isolated = RunFrame(
            task_id='t',
            task_name='x',
            args_json=None,
            kwargs_json=None,
            worker_id='w',
            is_workflow_task=False,
            timeout=NoTimeout(),
            claimed_at=None,
        )
        self._serve_until_exit(2, [isolated])
        assert exits[0][1] == child_main.BAD_PARENT_FRAME_EXIT_CODE


class TestFrameWriter:
    def test_send_returns_at_once_while_the_socket_write_blocks(self) -> None:
        """The loop never waits on the parent: a stuck write holds only the
        writer thread."""
        unblock = threading.Event()
        written: list[ChildFrame] = []

        def stuck_write(frame: ChildFrame) -> None:
            unblock.wait(RUN_TIMEOUT_S)
            written.append(frame)

        seats = 4
        writer = FrameWriter(stuck_write, capacity=FRAMES_PER_SEAT * seats + 1)
        assert writer.start().is_ok()
        frames = [ReadyFrame(pid=index, rss=RssMb(1.0)) for index in range(9)]

        started = time.monotonic()
        for frame in frames:
            writer.send(frame)
        elapsed = time.monotonic() - started

        assert elapsed < 0.1
        assert written == []
        unblock.set()
        writer.close(RUN_TIMEOUT_S)
        assert written == frames

    def test_more_frames_than_the_bound_end_the_child(
        self,
        exits: list[tuple[str, int]],
    ) -> None:
        writing = threading.Event()
        unblock = threading.Event()

        def stuck_write(_frame: ChildFrame) -> None:
            writing.set()
            unblock.wait(RUN_TIMEOUT_S)

        writer = FrameWriter(stuck_write, capacity=1)
        assert writer.start().is_ok()
        writer.send(ReadyFrame(pid=1, rss=RssMb(1.0)))
        assert writing.wait(RUN_TIMEOUT_S)
        writer.send(ReadyFrame(pid=2, rss=RssMb(1.0)))
        with pytest.raises(_Exited):
            writer.send(ReadyFrame(pid=3, rss=RssMb(1.0)))
        assert exits[0][1] == shared_child.CHILD_DEFECT_EXIT_CODE
        unblock.set()

    def test_results_are_bounded_in_the_writer_thread(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setattr(child_main, 'MAX_BODY_BYTES', 10)
        written: list[ChildFrame] = []
        writer = FrameWriter(written.append, capacity=2)
        assert writer.start().is_ok()
        big = SharedResultFrame(
            dispatch=4,
            task_id='task-4',
            end=SeatFinished(ok=True, result_json='x' * 11, failed_reason=None),
            rss=RssMb(1.0),
        )
        writer.send_result('fetch', big)
        writer.close(RUN_TIMEOUT_S)

        match written:
            case [SharedResultFrame(end=SeatFinished(ok=True, failed_reason=reason))]:
                assert reason is not None
                assert 'result of task fetch is 11 bytes' in reason
            case _:
                raise AssertionError(written)


class TestBoundedResultFrame:
    def test_a_leftover_cancel_report_cuts_a_long_failure_reason(self) -> None:
        frame = LeftoverCancelFrame(
            dispatch=1,
            task_id='t',
            returned=SeatFinished(
                ok=False,
                result_json='',
                failed_reason='r' * (MAX_FAILED_REASON_CHARS + 10),
            ),
        )

        bounded = bounded_result_frame('x', frame)

        assert isinstance(bounded, LeftoverCancelFrame)
        assert bounded.returned.failed_reason == (
            'r' * MAX_FAILED_REASON_CHARS
            + f' [cut from {MAX_FAILED_REASON_CHARS + 10} characters]'
        )

    def test_a_timed_out_end_passes_unchanged(self) -> None:
        frame = SharedResultFrame(
            dispatch=1,
            task_id='t',
            end=SeatTimedOut(),
            rss=RssMb(1.0),
        )
        assert bounded_result_frame('x', frame) == frame

    def test_a_result_within_the_bound_passes_unchanged(self) -> None:
        frame = SharedResultFrame(
            dispatch=1,
            task_id='t',
            end=SeatFinished(ok=True, result_json='{}', failed_reason=None),
            rss=RssMb(1.0),
        )
        assert bounded_result_frame('x', frame) == frame
