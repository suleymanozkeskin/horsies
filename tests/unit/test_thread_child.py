"""The run table, the seat threads and the frame loop of a shared thread child,
in process.

A recording writer stands in for the frame writer, and a scripted program for
the task runner. The same code with real child processes is covered in
``test_shared_thread_slot_pool``.
"""

# pyright: reportPrivateUsage=false

from __future__ import annotations

import threading
import time
from collections.abc import Callable, Iterator
from typing import NoReturn

import pytest

from horsies.core.worker import thread_child
from horsies.core.worker.child_inbox import ParentFrameInbox, WorkFrame
from horsies.core.worker.child_main import BAD_PARENT_FRAME_EXIT_CODE
from horsies.core.worker.child_protocol import (
    ChildFrame,
    DeadlineFiredFrame,
    RssMb,
    RssReading,
    RunFrame,
    SeatFinished,
    SeatRefusal,
    SeatTask,
    SeatTimedOut,
    SharedResultFrame,
    SharedRunFrame,
    SharedWork,
    StopFrame,
    ThreadSeatTask,
    ThreadSeatWork,
    WireResult,
)
from horsies.core.worker.shared_child import CHILD_DEFECT_EXIT_CODE
from horsies.core.worker.stored_timeout import NoTimeout, TimeoutValueInvalid
from horsies.core.worker.thread_child import (
    DEADLINE_WAIT_MAX_S,
    ThreadRunTable,
    ThreadSeats,
    serve_runs,
)

pytestmark = [pytest.mark.unit]

WAIT_S = 10.0


class _Exited(Exception):
    """Raised by the patched exit, as the real one ends the process."""


@pytest.fixture
def exits(monkeypatch: pytest.MonkeyPatch) -> list[tuple[str, int]]:
    """Record each defect exit of ``thread_child`` and stop the caller."""
    seen: list[tuple[str, int]] = []

    def record(message: str, exit_code: int) -> NoReturn:
        seen.append((message, exit_code))
        raise _Exited(message)

    monkeypatch.setattr(thread_child, '_exit_for_defect', record)
    return seen


class _RecordingWriter:
    """Records frames in order, as the writer thread would write them."""

    def __init__(self) -> None:
        self.frames: list[ChildFrame] = []
        self._lock = threading.Lock()

    def send(self, frame: ChildFrame) -> None:
        with self._lock:
            self.frames.append(frame)

    def send_result(self, task_name: str, frame: SharedResultFrame) -> None:
        _ = task_name
        with self._lock:
            self.frames.append(frame)

    def results(self) -> list[SharedResultFrame]:
        with self._lock:
            return [item for item in self.frames if isinstance(item, SharedResultFrame)]


class _Clock:
    """A clock that moves only when the test moves it."""

    def __init__(self) -> None:
        self.now = 100.0

    def __call__(self) -> float:
        return self.now


def _frame(
    dispatch: int,
    task_name: str = 'task',
    work: SharedWork | None = None,
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
        work=work or ThreadSeatTask(timeout_ms=1000),
    )


def _thread_work(frame: SharedRunFrame) -> ThreadSeatWork:
    match frame.work:
        case ThreadSeatTask() | SeatRefusal() as work:
            return work
        case SeatTask():
            raise AssertionError('a thread run in these tests')


def _rss() -> RssReading:
    return RssMb(10.0)


OK_WIRE: WireResult = (True, '"value"', None)


def _until(condition: Callable[[], bool]) -> None:
    deadline = time.monotonic() + WAIT_S
    while not condition():
        assert time.monotonic() < deadline, 'condition not met in time'
        time.sleep(0.005)


class TestRunTable:
    def _table(self, seats: int = 2) -> tuple[ThreadRunTable, _RecordingWriter, _Clock]:
        writer = _RecordingWriter()
        clock = _Clock()
        return ThreadRunTable(seats, writer, clock), writer, clock

    def test_a_deadline_fires_at_its_time_and_once(self) -> None:
        table, writer, clock = self._table()
        frame = _frame(1)
        table.add(frame, _thread_work(frame))

        assert table._fire_due() == pytest.approx(1.0)
        assert writer.frames == []
        clock.now += 1.0
        table._fire_due()
        table._fire_due()

        assert writer.frames == [DeadlineFiredFrame(dispatch=1, task_id='task-1')]

    def test_a_run_that_ends_before_its_deadline_reports_its_result(self) -> None:
        table, writer, clock = self._table()
        frame = _frame(1)
        table.add(frame, _thread_work(frame))
        clock.now += 0.999
        table._fire_due()

        table.finish(1, OK_WIRE, RssMb(5.0))

        assert writer.frames == [
            SharedResultFrame(
                dispatch=1,
                task_id='task-1',
                end=SeatFinished(ok=True, result_json='"value"', failed_reason=None),
                rss=RssMb(5.0),
            ),
        ]
        assert table.count() == 0

    def test_a_value_after_the_deadline_is_the_timeout(self) -> None:
        """The deadline report comes first, then the result, which drops the
        value that the task returned."""
        table, writer, clock = self._table()
        frame = _frame(1)
        table.add(frame, _thread_work(frame))
        clock.now += 1.0
        table._fire_due()

        table.finish(1, OK_WIRE, RssMb(5.0))

        assert writer.frames == [
            DeadlineFiredFrame(dispatch=1, task_id='task-1'),
            SharedResultFrame(
                dispatch=1,
                task_id='task-1',
                end=SeatTimedOut(),
                rss=RssMb(5.0),
            ),
        ]

    def test_an_ended_run_has_no_deadline_left(self) -> None:
        table, writer, clock = self._table()
        frame = _frame(1)
        table.add(frame, _thread_work(frame))
        table.finish(1, OK_WIRE, RssMb(5.0))
        clock.now += 60.0

        table._fire_due()

        assert not any(isinstance(item, DeadlineFiredFrame) for item in writer.frames)

    def test_a_refusal_has_no_deadline(self) -> None:
        table, writer, clock = self._table()
        frame = _frame(1, work=SeatRefusal(refusal=TimeoutValueInvalid(reason='y')))
        table.add(frame, _thread_work(frame))
        clock.now += 10**6

        assert table._fire_due() == DEADLINE_WAIT_MAX_S
        assert writer.frames == []

    def test_the_wait_follows_the_nearest_deadline_within_the_cap(self) -> None:
        table, _writer, _clock = self._table(3)
        for dispatch, timeout_ms in ((1, 60_000), (2, 2_500), (3, 4_000)):
            frame = _frame(dispatch, work=ThreadSeatTask(timeout_ms=timeout_ms))
            table.add(frame, _thread_work(frame))

        assert table._fire_due() == pytest.approx(2.5)

    def test_an_idle_table_waits_the_cap(self) -> None:
        table, _writer, _clock = self._table()
        assert table._fire_due() == DEADLINE_WAIT_MAX_S

    def test_a_run_when_every_seat_is_taken_is_a_parent_defect(
        self,
        exits: list[tuple[str, int]],
    ) -> None:
        table, _writer, _clock = self._table(1)
        first, second = _frame(1), _frame(2)
        table.add(first, _thread_work(first))

        with pytest.raises(_Exited):
            table.add(second, _thread_work(second))

        assert exits == [
            (
                'parent sent run 2 while all 1 seats are taken',
                BAD_PARENT_FRAME_EXIT_CODE,
            ),
        ]

    def test_a_dispatch_number_in_use_is_a_parent_defect(
        self,
        exits: list[tuple[str, int]],
    ) -> None:
        table, _writer, _clock = self._table(2)
        frame = _frame(1)
        table.add(frame, _thread_work(frame))

        with pytest.raises(_Exited):
            table.add(frame, _thread_work(frame))

        assert exits == [('parent sent run 1 twice', BAD_PARENT_FRAME_EXIT_CODE)]

    def test_the_deadline_thread_fires_on_the_real_clock_and_stops(self) -> None:
        writer = _RecordingWriter()
        table = ThreadRunTable(1, writer)
        thread = threading.Thread(target=table.serve_deadlines, daemon=True)
        thread.start()
        frame = _frame(1)
        started = time.monotonic()
        table.add(frame, _thread_work(frame))

        _until(lambda: bool(writer.frames))
        fired_after_s = time.monotonic() - started
        table.stop()
        thread.join(WAIT_S)

        assert writer.frames == [DeadlineFiredFrame(dispatch=1, task_id='task-1')]
        assert 1.0 <= fired_after_s < 1.5
        assert not thread.is_alive()


class _Program:
    """Each run calls the behavior registered for its task name."""

    def __init__(self, behaviors: dict[str, Callable[[], str]]) -> None:
        self._behaviors = behaviors
        self.threads: list[str] = []
        self._lock = threading.Lock()

    def run(self, frame: SharedRunFrame, work: ThreadSeatWork) -> WireResult:
        _ = work
        with self._lock:
            self.threads.append(threading.current_thread().name)
        return True, self._behaviors[frame.task_name](), None


@pytest.fixture
def seats_of() -> (
    Iterator[
        Callable[..., tuple[ThreadSeats, ThreadRunTable, _RecordingWriter, _Program]]
    ]
):
    started: list[ThreadSeats] = []

    def build(
        seats: int,
        behaviors: dict[str, Callable[[], str]],
    ) -> tuple[ThreadSeats, ThreadRunTable, _RecordingWriter, _Program]:
        writer = _RecordingWriter()
        table = ThreadRunTable(seats, writer)
        program = _Program(behaviors)
        seat_threads = ThreadSeats(seats, table, program, _rss)
        seat_threads.start()
        started.append(seat_threads)
        return seat_threads, table, writer, program

    yield build
    for seat_threads in started:
        seat_threads.stop(WAIT_S)


def _give(table: ThreadRunTable, seats: ThreadSeats, frame: SharedRunFrame) -> None:
    work = _thread_work(frame)
    table.add(frame, work)
    seats.give(frame, work)


class TestSeatThreads:
    def test_every_seat_runs_at_once_each_on_its_own_thread(
        self,
        seats_of: Callable[
            ..., tuple[ThreadSeats, ThreadRunTable, _RecordingWriter, _Program]
        ],
    ) -> None:
        release = threading.Event()
        arrived = threading.Semaphore(0)

        def hold() -> str:
            arrived.release()
            assert release.wait(WAIT_S)
            return 'held'

        seats, table, writer, program = seats_of(3, {'hold': hold})
        for dispatch in (1, 2, 3):
            _give(table, seats, _frame(dispatch, 'hold'))
        for _ in range(3):
            assert arrived.acquire(timeout=WAIT_S)

        assert writer.results() == []
        release.set()
        _until(lambda: len(writer.results()) == 3)
        assert len(set(program.threads)) == 3
        assert all(name.startswith('horsies-seat-') for name in program.threads)

    def test_a_free_seat_takes_the_next_run_while_others_still_run(
        self,
        seats_of: Callable[
            ..., tuple[ThreadSeats, ThreadRunTable, _RecordingWriter, _Program]
        ],
    ) -> None:
        release_long = threading.Event()

        def long() -> str:
            assert release_long.wait(WAIT_S)
            return 'long'

        seats, table, writer, _program = seats_of(
            2, {'long': long, 'short': lambda: 'short'}
        )
        _give(table, seats, _frame(1, 'long'))
        _give(table, seats, _frame(2, 'short'))
        _until(lambda: len(writer.results()) == 1)
        _give(table, seats, _frame(3, 'short'))

        _until(lambda: len(writer.results()) == 2)
        assert [frame.dispatch for frame in writer.results()] == [2, 3]
        release_long.set()
        _until(lambda: len(writer.results()) == 3)

    @pytest.mark.filterwarnings('ignore::pytest.PytestUnhandledThreadExceptionWarning')
    def test_a_task_that_escapes_the_seat_runner_ends_the_child(
        self,
        monkeypatch: pytest.MonkeyPatch,
        seats_of: Callable[
            ..., tuple[ThreadSeats, ThreadRunTable, _RecordingWriter, _Program]
        ],
    ) -> None:
        exited = threading.Event()
        seen: list[tuple[str, int]] = []

        def record(message: str, exit_code: int) -> NoReturn:
            seen.append((message, exit_code))
            exited.set()
            raise SystemExit(exit_code)

        monkeypatch.setattr(thread_child, '_exit_for_defect', record)

        def escape() -> str:
            raise RuntimeError('escaped')

        seats, table, writer, _program = seats_of(1, {'escape': escape})
        _give(table, seats, _frame(1, 'escape'))

        assert exited.wait(WAIT_S)
        assert seen == [
            (
                'task task-1 escaped the seat runner: RuntimeError: escaped',
                CHILD_DEFECT_EXIT_CODE,
            ),
        ]
        assert writer.results() == []


class _Taker:
    """Records the runs that ``serve_runs`` hands to the seats."""

    def __init__(self) -> None:
        self.given: list[tuple[int, ThreadSeatWork]] = []

    def give(self, frame: SharedRunFrame, work: ThreadSeatWork) -> None:
        self.given.append((frame.dispatch, work))


class TestServeRuns:
    def _serve(self, taker: _Taker, *frames: WorkFrame) -> ThreadRunTable:
        inbox = ParentFrameInbox()
        for frame in frames:
            inbox.put(frame)
        table = ThreadRunTable(2, _RecordingWriter(), _Clock())
        serve_runs(inbox, table, taker)
        return table

    def test_runs_go_to_the_seats_until_the_stop(self) -> None:
        refusal = SeatRefusal(refusal=TimeoutValueInvalid(reason='y'))
        taker = _Taker()
        table = ThreadRunTable(2, _RecordingWriter(), _Clock())
        inbox = ParentFrameInbox()
        inbox.put(_frame(1))
        inbox.put(_frame(2, work=refusal))

        def end_runs_then_stop() -> None:
            _until(lambda: len(taker.given) == 2)
            table.finish(1, OK_WIRE, RssMb(1.0))
            table.finish(2, OK_WIRE, RssMb(1.0))
            inbox.put(StopFrame())

        stopper = threading.Thread(target=end_runs_then_stop)
        stopper.start()
        serve_runs(inbox, table, taker)
        stopper.join(WAIT_S)

        assert taker.given == [(1, ThreadSeatTask(timeout_ms=1000)), (2, refusal)]
        assert table.count() == 0

    def test_a_stop_while_runs_are_in_flight_is_a_parent_defect(
        self,
        exits: list[tuple[str, int]],
    ) -> None:
        taker = _Taker()
        with pytest.raises(_Exited):
            self._serve(taker, _frame(1), _frame(2), StopFrame())
        assert [dispatch for dispatch, _work in taker.given] == [1, 2]
        assert exits == [
            (
                'parent sent a stop frame while 2 runs are in flight',
                BAD_PARENT_FRAME_EXIT_CODE,
            ),
        ]

    def test_a_stop_with_no_run_in_flight_returns(self) -> None:
        taker = _Taker()
        table = self._serve(taker, StopFrame())
        assert table.count() == 0
        assert taker.given == []

    def test_the_task_work_of_a_shared_async_child_is_a_parent_defect(
        self,
        exits: list[tuple[str, int]],
    ) -> None:
        with pytest.raises(_Exited):
            self._serve(
                _Taker(),
                _frame(1, work=SeatTask(timeout_ms=1000, cancel_grace_ms=100)),
            )
        assert exits == [
            (
                'parent sent run 1 of a shared async child to a shared thread child',
                BAD_PARENT_FRAME_EXIT_CODE,
            ),
        ]

    def test_an_isolated_run_frame_is_a_parent_defect(
        self,
        exits: list[tuple[str, int]],
    ) -> None:
        run = RunFrame(
            task_id='t',
            task_name='task',
            args_json=None,
            kwargs_json=None,
            worker_id='w',
            is_workflow_task=False,
            timeout=NoTimeout(),
            claimed_at=None,
        )
        with pytest.raises(_Exited):
            self._serve(_Taker(), run)
        assert exits == [
            (
                'parent sent an isolated run frame to a shared thread child',
                BAD_PARENT_FRAME_EXIT_CODE,
            ),
        ]
