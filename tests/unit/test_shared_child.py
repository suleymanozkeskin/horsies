"""The seat loop and the frame writer of a shared async child, in process.

The seat loop runs on a real event loop here, fed through the real
``ParentFrameInbox`` (also from another thread). A recording writer stands in
for the socket, and a scripted ``StartedSeats`` for the task runner. The same
loop with real child processes is covered in ``test_slot_pool``.
"""

# pyright: reportPrivateUsage=false

from __future__ import annotations

import asyncio
import contextlib
import contextvars
import dataclasses
import threading
import time
from collections.abc import Callable
from typing import Any, NoReturn

import pytest

from horsies.core.worker import child_main, shared_child
from horsies.core.worker.child_inbox import ParentFrameInbox
from horsies.core.worker.child_protocol import (
    ChildFrame,
    DeadlineFiredFrame,
    ReadyFrame,
    RssMb,
    RssReading,
    RunFrame,
    RunStuckFrame,
    SeatFinished,
    SeatRefusal,
    SeatTask,
    SeatTimedOut,
    SharedResultFrame,
    SharedRunFrame,
    StopFrame,
    WireResult,
)
from horsies.core.worker.shared_child import (
    FRAMES_PER_SEAT,
    FrameWriter,
    SeatLoop,
    bounded_result_frame,
)
from horsies.core.worker.stored_timeout import NoTimeout, TimeoutValueInvalid

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

    def send_result(self, task_name: str, frame: SharedResultFrame) -> None:
        self.frames.append(frame)


class _ScriptedSeats:
    """Each run awaits the behavior registered for its task name."""

    def __init__(self, behaviors: dict[str, Callable[[], Any]]) -> None:
        self._behaviors = behaviors
        self.started: list[int] = []

    async def run(self, frame: SharedRunFrame) -> WireResult:
        self.started.append(frame.dispatch)
        value = await self._behaviors[frame.task_name]()
        return True, str(value), None

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
        seat_loop = SeatLoop(self.seats, self.writer, _rss)  # type: ignore[arg-type]
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
            while not stop.is_set():
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

    def test_a_task_left_after_its_return_past_the_deadline_is_the_timeout(
        self,
    ) -> None:
        """The body returned, but its marked work outlived T: the attempt had
        not ended, so the outcome is the timeout."""

        async def leave_forever() -> int:
            _ = asyncio.get_running_loop().create_task(asyncio.Event().wait())
            return 1

        writer = _run_to_end(
            {'leave': leave_forever},
            [_frame(1, 'leave', DEADLINE_MS, GRACE_MS)],
            _results_of(1),
        )

        assert _kinds(writer, 1) == [
            'DeadlineFiredFrame',
            'SecondCancelFrame',
            'SharedResultFrame',
        ]
        assert _results(writer)[0].end == SeatTimedOut()

    def test_the_stop_of_one_run_leaves_its_neighbour_running(self) -> None:
        release = asyncio.Event()
        stop = threading.Event()

        async def swallow_all() -> int:
            while not stop.is_set():
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
