"""The seat loop and the frame writer of a shared async child, in process.

The seat loop runs on a real event loop here, fed through the real
``ParentFrameInbox`` (also from another thread). A recording writer stands in
for the socket, and a scripted ``StartedSeats`` for the task runner. The same
loop with real child processes is covered in ``test_slot_pool``.
"""

# pyright: reportPrivateUsage=false

from __future__ import annotations

import asyncio
import threading
import time
from collections.abc import Callable
from typing import Any, NoReturn

import pytest

from horsies.core.worker import child_main, shared_child
from horsies.core.worker.child_main import ParentFrameInbox
from horsies.core.worker.child_protocol import (
    ChildFrame,
    DeadlineFiredFrame,
    ReadyFrame,
    RssMb,
    RssReading,
    RunFrame,
    SeatFinished,
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
from horsies.core.worker.stored_timeout import NoTimeout

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


def _frame(dispatch: int, task_name: str, timeout_ms: int = 60_000) -> SharedRunFrame:
    return SharedRunFrame(
        dispatch=dispatch,
        task_id=f'task-{dispatch}',
        task_name=task_name,
        args_json=None,
        kwargs_json=None,
        worker_id='worker-1',
        is_workflow_task=False,
        claimed_at=None,
        timeout_ms=timeout_ms,
        cancel_grace_ms=1000,
    )


def _rss() -> RssReading:
    return RssMb(10.0)


def _seat_loop(
    seats: int,
    behaviors: dict[str, Callable[[], Any]],
) -> tuple[SeatLoop, _RecordingWriter, _ScriptedSeats]:
    writer = _RecordingWriter()
    started = _ScriptedSeats(behaviors)
    loop = SeatLoop(seats, started, writer, _rss)  # type: ignore[arg-type]
    return loop, writer, started


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
