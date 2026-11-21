"""The run table, the seat threads and the frame loop of a shared thread child,
in process.

A recording writer stands in for the frame writer, and a scripted program for
the task runner. The frames are version-2 frames, as the core decodes them.
"""

# pyright: reportPrivateUsage=false

from __future__ import annotations

import threading
import time
import uuid
from collections.abc import Callable, Iterator
from typing import NoReturn

import pytest

from horsies import _core as core
from horsies.core.models.tasks import MIN_TASK_TIMEOUT_MS
from horsies.core.telemetry.attempt_span import BodyEndOrder, BodyFirst, DeadlineFirst
from horsies.core.worker import thread_child
from horsies.core.worker.child_frames import (
    AttemptStart,
    ChildFrame,
    DeadlineFiredFrame,
    RssMb,
    RssReading,
    SeatEnd,
    SeatTimedOut,
    SharedResultFrame,
    StartNotReported,
    StartReported,
    ThreadSeatWork,
    WorkFrame,
)
from horsies.core.worker.child_inbox import ParentFrameInbox
from horsies.core.worker.child_main import BAD_PARENT_FRAME_EXIT_CODE
from horsies.core.worker.run_return import (
    CoreStoppedAfterBody,
    MarkerDone,
    MarkerMetStoppedCore,
    ResultEnvelope,
)
from horsies.core.worker.shared_child import (
    CHILD_DEFECT_EXIT_CODE,
    MarkerDue,
    NoMarker,
    StoppedBeforeRunning,
)
from horsies.core.worker.child_runner import (
    DeadlineBeforeStart,
    StartAllowed,
    StartCheck,
    StartWithin,
)
from horsies.core.worker.thread_child import (
    DEADLINE_WAIT_MAX_S,
    ThreadRunTable,
    ThreadSeatReturn,
    ThreadSeats,
    _milliseconds_left,
    serve_runs,
)

from tests.fixtures.core_frames import (
    async_work,
    refusal,
    run_frame,
    shared_run_frame,
    stop_frame,
    thread_work,
)
from tests.fixtures.run_return import STARTED_AT, set_running

pytestmark = [pytest.mark.unit]

WAIT_S = 10.0

DEADLINE_S = MIN_TASK_TIMEOUT_MS / 1000
"""The deadline of a run that a test lets pass, in seconds: the smallest
``timeout_ms`` of a run frame. The core codec refuses a smaller one, as the
stored ``task_options`` reader does."""


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


def _task_id(dispatch: int) -> uuid.UUID:
    return uuid.UUID(int=dispatch)


REFUSAL_WORK = refusal('value_invalid', 'y')
"""The work of a run whose stored timeout the worker refused."""


def _frame(
    dispatch: int,
    task_name: str = 'task',
    work: dict[str, object] | None = None,
) -> core.SharedRunFrame:
    return shared_run_frame(
        dispatch=dispatch,
        task_id=_task_id(dispatch),
        task_name=task_name,
        work=work if work is not None else thread_work(1000),
        orphan_self_heal=True,
    )


def _thread_work(frame: core.SharedRunFrame) -> ThreadSeatWork:
    match frame.work:
        case core.ThreadTaskWork() | core.StoredOptionsRefused() as work:
            return work
        case core.AsyncTaskWork():
            raise AssertionError('a thread run in these tests')


type _WorkSeen = tuple[str, int] | tuple[str, str, str]
"""What a test compares of the work of a run: a thread task and its
timeout, or a refusal and its kind and reason."""


def _work_seen(work: ThreadSeatWork) -> _WorkSeen:
    match work:
        case core.ThreadTaskWork(timeout_ms=timeout_ms):
            return ('thread_task', timeout_ms)
        case core.StoredOptionsRefused(kind=kind, reason=reason):
            return ('refusal', kind, reason)


def _rss() -> RssReading:
    return RssMb(10.0)


OK_END = ResultEnvelope('"value"')


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

        assert writer.frames == [DeadlineFiredFrame(dispatch=1, task_id=_task_id(1))]

    def test_a_run_that_ends_before_its_deadline_reports_its_result(self) -> None:
        table, writer, clock = self._table()
        frame = _frame(1)
        table.add(frame, _thread_work(frame))
        clock.now += 0.999
        table._fire_due()

        _ = table.returned(1, set_running(OK_END))
        table.finish(1, RssMb(5.0))

        assert writer.frames == [
            SharedResultFrame(
                dispatch=1,
                task_id=_task_id(1),
                end=OK_END,
                started=StartReported(STARTED_AT),
                rss=RssMb(5.0),
            ),
        ]
        assert table.count() == 0

    def test_a_marker_that_met_the_stopped_core_reports_the_result_of_the_body(
        self,
    ) -> None:
        """The writer ends the child once this frame is written
        (``test_child_core_refusals``)."""
        table, writer, _clock = self._table()
        frame = _frame(1)
        table.add(frame, _thread_work(frame))
        stopped = core.CoreStopped('log forwarder', 'planned panic')

        assert table.returned(1, set_running(OK_END)) == MarkerDue()
        table.marked(1, MarkerMetStoppedCore(stopped))
        table.finish(1, RssMb(5.0))

        assert writer.frames == [
            SharedResultFrame(
                dispatch=1,
                task_id=_task_id(1),
                end=CoreStoppedAfterBody(result=OK_END, stopped=stopped),
                started=StartReported(STARTED_AT),
                rss=RssMb(5.0),
            ),
        ]

    def test_a_written_marker_keeps_the_end_at_the_return(self) -> None:
        table, writer, _clock = self._table()
        frame = _frame(1)
        table.add(frame, _thread_work(frame))

        _ = table.returned(1, set_running(OK_END))
        table.marked(1, MarkerDone())
        table.finish(1, RssMb(5.0))

        assert [item.end for item in writer.frames if isinstance(item, SharedResultFrame)] == [
            OK_END,
        ]

    def test_a_value_after_the_deadline_is_the_timeout(self) -> None:
        """The deadline report comes first, then the result, which drops the
        value that the task returned."""
        table, writer, clock = self._table()
        frame = _frame(1)
        table.add(frame, _thread_work(frame))
        clock.now += 1.0
        table._fire_due()

        _ = table.returned(1, set_running(OK_END))
        table.finish(1, RssMb(5.0))

        assert writer.frames == [
            DeadlineFiredFrame(dispatch=1, task_id=_task_id(1)),
            SharedResultFrame(
                dispatch=1,
                task_id=_task_id(1),
                end=SeatTimedOut(),
                started=StartReported(STARTED_AT),
                rss=RssMb(5.0),
            ),
        ]

    def test_a_body_that_ends_before_its_deadline_fixes_the_result(self) -> None:
        """The steps return before the deadline: ``body_ended`` gives
        ``BodyFirst`` (the order of the attempt span), and the deadline no
        longer fires, also when the task returns past it."""
        table, writer, clock = self._table()
        frame = _frame(1)
        table.add(frame, _thread_work(frame))
        clock.now += 0.999

        assert table.body_ended(1) == BodyFirst()
        clock.now += 1.0
        table._fire_due()
        _ = table.returned(1, set_running(OK_END))
        table.finish(1, RssMb(5.0))

        assert writer.frames == [
            SharedResultFrame(
                dispatch=1,
                task_id=_task_id(1),
                end=OK_END,
                started=StartReported(STARTED_AT),
                rss=RssMb(5.0),
            ),
        ]

    def test_a_body_that_ends_after_its_deadline_fired_is_the_timeout(self) -> None:
        """The deadline fired first: ``body_ended`` gives ``DeadlineFirst``,
        and the run ends with the timeout, the end of the attempt span."""
        table, writer, clock = self._table()
        frame = _frame(1)
        table.add(frame, _thread_work(frame))
        clock.now += 1.0
        table._fire_due()

        assert table.body_ended(1) == DeadlineFirst()
        _ = table.returned(1, set_running(OK_END))
        table.finish(1, RssMb(5.0))

        assert [item.end for item in writer.frames if isinstance(item, SharedResultFrame)] == [
            SeatTimedOut(),
        ]

    def test_a_body_past_its_deadline_that_the_thread_did_not_fire_keeps_its_result(
        self,
    ) -> None:
        """The steps return at or past the deadline before the deadline
        thread fired it: ``body_ended`` fires nothing, and the run keeps
        what it returned."""
        table, writer, clock = self._table()
        frame = _frame(1)
        table.add(frame, _thread_work(frame))
        clock.now += 5.0

        assert table.body_ended(1) == BodyFirst()
        _ = table.returned(1, set_running(OK_END))
        table.finish(1, RssMb(5.0))

        assert [type(item) for item in writer.frames] == [SharedResultFrame]
        assert [item.end for item in writer.frames if isinstance(item, SharedResultFrame)] == [
            OK_END,
        ]

    def test_a_body_that_ends_twice_is_a_defect(self) -> None:
        table, _writer, _clock = self._table()
        frame = _frame(1)
        table.add(frame, _thread_work(frame))
        _ = table.body_ended(1)

        with pytest.raises(AssertionError, match='ended its body twice'):
            _ = table.body_ended(1)

    def test_an_ended_run_has_no_deadline_left(self) -> None:
        table, writer, clock = self._table()
        frame = _frame(1)
        table.add(frame, _thread_work(frame))
        _ = table.returned(1, set_running(OK_END))
        table.finish(1, RssMb(5.0))
        clock.now += 60.0

        table._fire_due()

        assert not any(isinstance(item, DeadlineFiredFrame) for item in writer.frames)

    def test_a_refusal_has_no_deadline(self) -> None:
        table, writer, clock = self._table()
        frame = _frame(1, work=REFUSAL_WORK)
        table.add(frame, _thread_work(frame))
        clock.now += 10**6

        assert table._fire_due() == DEADLINE_WAIT_MAX_S
        assert writer.frames == []

    def test_the_wait_follows_the_nearest_deadline_within_the_cap(self) -> None:
        table, _writer, _clock = self._table(3)
        for dispatch, timeout_ms in ((1, 60_000), (2, 2_500), (3, 4_000)):
            frame = _frame(dispatch, work=thread_work(timeout_ms))
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

    def test_a_return_before_the_deadline_fixes_the_end_and_ends_the_deadline(
        self,
    ) -> None:
        """The marker of the run runs past its deadline: no deadline report
        goes out, and the end stays what the task returned."""
        table, writer, clock = self._table()
        frame = _frame(1)
        table.add(frame, _thread_work(frame))
        clock.now += 0.9

        marker = table.returned(1, set_running(OK_END))
        clock.now += 0.5
        table._fire_due()
        table.finish(1, RssMb(5.0))

        assert marker == MarkerDue()
        assert writer.frames == [
            SharedResultFrame(
                dispatch=1,
                task_id=_task_id(1),
                end=OK_END,
                started=StartReported(STARTED_AT),
                rss=RssMb(5.0),
            ),
        ]

    def test_a_return_after_the_deadline_writes_no_marker(self) -> None:
        table, _writer, clock = self._table()
        frame = _frame(1)
        table.add(frame, _thread_work(frame))
        clock.now += 1.0
        table._fire_due()

        assert table.returned(1, set_running(OK_END)) == NoMarker()

    def test_a_return_at_its_deadline_not_yet_fired_writes_no_marker(self) -> None:
        """The deadline thread has not fired yet: the run keeps what it
        returned, but no time is left for a marker."""
        table, writer, clock = self._table()
        frame = _frame(1)
        table.add(frame, _thread_work(frame))
        clock.now += 1.0

        assert table.returned(1, set_running(OK_END)) == NoMarker()
        table.finish(1, RssMb(5.0))
        assert writer.results()[0].end == OK_END

    @pytest.mark.parametrize('past_deadline_s', [0.0, 0.5])
    @pytest.mark.parametrize(
        ('returned', 'end', 'started'),
        [
            (set_running(OK_END), OK_END, StartReported(STARTED_AT)),
            (
                StoppedBeforeRunning('CLAIM_LOST'),
                StoppedBeforeRunning('CLAIM_LOST'),
                StartNotReported(),
            ),
        ],
    )
    def test_a_return_at_or_past_its_deadline_not_yet_fired_keeps_its_end(
        self,
        past_deadline_s: float,
        returned: ThreadSeatReturn,
        end: SeatEnd,
        started: AttemptStart,
    ) -> None:
        """Only a deadline that fired before the return makes the run a
        timeout. A return at or past a deadline that the deadline thread has
        not fired keeps what the task returned: the return fires no deadline,
        so no deadline report goes out, before or after it."""
        table, writer, clock = self._table()
        frame = _frame(1)
        table.add(frame, _thread_work(frame))
        clock.now += 1.0 + past_deadline_s

        assert table.returned(1, returned) == NoMarker()
        assert writer.frames == []
        table._fire_due()
        table.finish(1, RssMb(5.0))

        assert writer.frames == [
            SharedResultFrame(
                dispatch=1, task_id=_task_id(1), end=end, started=started, rss=RssMb(5.0),
            ),
        ]

    def test_a_run_that_stopped_before_running_writes_no_marker(self) -> None:
        table, _writer, _clock = self._table()
        frame = _frame(1)
        table.add(frame, _thread_work(frame))

        assert table.returned(1, StoppedBeforeRunning('CLAIM_LOST')) == NoMarker()

    def test_a_refusal_writes_its_marker(self) -> None:
        table, _writer, clock = self._table()
        frame = _frame(1, work=REFUSAL_WORK)
        table.add(frame, _thread_work(frame))
        clock.now += 10**6

        assert table.returned(1, set_running(OK_END)) == MarkerDue()

    def test_a_start_before_the_deadline_is_allowed_with_the_time_left(self) -> None:
        """The core checks the time left again once the pool gave the
        connection, before the statement."""
        table, writer, clock = self._table()
        frame = _frame(1)
        table.add(frame, _thread_work(frame))
        clock.now += 0.75

        assert table.start_check(1) == StartWithin(milliseconds=250)
        assert writer.frames == []

    def test_a_start_just_before_the_deadline_is_allowed(self) -> None:
        table, writer, clock = self._table()
        frame = _frame(1)
        table.add(frame, _thread_work(frame))
        clock.now += 0.999

        check = table.start_check(1)
        assert isinstance(check, StartWithin)
        assert check.milliseconds >= 1
        assert writer.frames == []

    def test_a_refusal_starts_with_no_deadline(self) -> None:
        table, _writer, clock = self._table()
        frame = _frame(1, work=REFUSAL_WORK)
        table.add(frame, _thread_work(frame))
        clock.now += 100.0

        assert table.start_check(1) == StartAllowed()

    def test_the_time_left_rounds_up_to_a_whole_millisecond(self) -> None:
        """The check of the core never passes before the deadline of the
        table: the time left rounds up, and is 1 ms at least."""
        assert _milliseconds_left(0.25) == 250
        assert _milliseconds_left(0.0001) == 1
        assert _milliseconds_left(0.0015) == 2
        assert _milliseconds_left(0.0) == 1
        assert _milliseconds_left(-0.5) == 1

    def test_a_start_that_the_core_refused_at_the_deadline_fires_it(self) -> None:
        """The core refused the start at the deadline (DeadlineBeforeStart)
        before the deadline thread reached it: the return fires the deadline
        first, so the deadline report comes first, and the run ends with the
        timeout."""
        table, writer, clock = self._table()
        frame = _frame(1)
        table.add(frame, _thread_work(frame))
        assert table.start_check(1) == StartWithin(milliseconds=1000)
        clock.now += 1.0

        assert table.returned(1, DeadlineBeforeStart()) == NoMarker()
        table.finish(1, RssMb(5.0))

        assert writer.frames == [
            DeadlineFiredFrame(dispatch=1, task_id=_task_id(1)),
            SharedResultFrame(
                dispatch=1,
                task_id=_task_id(1),
                end=SeatTimedOut(),
                started=StartNotReported(),
                rss=RssMb(5.0),
            ),
        ]

    def test_a_start_after_the_deadline_fires_it_and_is_refused(self) -> None:
        """The seat waited for a connection past the deadline, and the
        deadline thread has not fired it yet: the check fires it, so the
        deadline report comes first, and the run ends with the timeout."""
        table, writer, clock = self._table()
        frame = _frame(1)
        table.add(frame, _thread_work(frame))
        clock.now += 1.0

        assert table.start_check(1) == DeadlineBeforeStart()
        assert table.returned(1, DeadlineBeforeStart()) == NoMarker()
        table.finish(1, RssMb(5.0))

        assert writer.frames == [
            DeadlineFiredFrame(dispatch=1, task_id=_task_id(1)),
            SharedResultFrame(
                dispatch=1,
                task_id=_task_id(1),
                end=SeatTimedOut(),
                started=StartNotReported(),
                rss=RssMb(5.0),
            ),
        ]

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

        assert writer.frames == [DeadlineFiredFrame(dispatch=1, task_id=_task_id(1))]
        assert 1.0 <= fired_after_s < 1.5
        assert not thread.is_alive()


class _Program:
    """Each run calls the behavior registered for its task name, and sets
    its row RUNNING, but a task named in ``not_running``. A task named in
    ``checks_start`` asks ``start_check`` first, as the RUNNING update does.
    A run that sets its row RUNNING records the order that ``body_end``
    gives when its behavior returned, as the attempt span reads it. Each
    marker is recorded with its run, then calls ``marker``."""

    def __init__(self, behaviors: dict[str, Callable[[], str]]) -> None:
        self._behaviors = behaviors
        self.threads: list[str] = []
        self.not_running: set[str] = set()
        self.checks_start: set[str] = set()
        self.orders: dict[int, BodyEndOrder] = {}
        self.markers: list[int] = []
        self.marker: Callable[[], None] = lambda: None
        self._lock = threading.Lock()

    def run(
        self,
        frame: core.SharedRunFrame,
        work: ThreadSeatWork,
        start_check: Callable[[], StartCheck],
        body_end: Callable[[], BodyEndOrder],
    ) -> ThreadSeatReturn:
        _ = work
        if frame.task_name in self.checks_start:
            match start_check():
                case DeadlineBeforeStart() as passed:
                    return passed
                case StartAllowed() | StartWithin():
                    pass
        with self._lock:
            self.threads.append(threading.current_thread().name)
        end = ResultEnvelope(self._behaviors[frame.task_name]())
        if frame.task_name in self.not_running:
            return StoppedBeforeRunning('CLAIM_LOST')
        order = body_end()
        with self._lock:
            self.orders[frame.dispatch] = order
        return set_running(end)

    def mark_finalizing(self, frame: core.SharedRunFrame) -> MarkerDone:
        with self._lock:
            self.markers.append(frame.dispatch)
        self.marker()
        return MarkerDone()


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


def _give(table: ThreadRunTable, seats: ThreadSeats, frame: core.SharedRunFrame) -> None:
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
            return 'held' if release.wait(WAIT_S) else 'not released'

        seats, table, writer, program = seats_of(3, {'hold': hold})
        try:
            for dispatch in (1, 2, 3):
                _give(table, seats, _frame(dispatch, 'hold'))
            for _ in range(3):
                assert arrived.acquire(timeout=WAIT_S)
            assert writer.results() == []
        finally:
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
            return 'long' if release_long.wait(WAIT_S) else 'not released'

        seats, table, writer, _program = seats_of(
            2, {'long': long, 'short': lambda: 'short'}
        )
        try:
            _give(table, seats, _frame(1, 'long'))
            _give(table, seats, _frame(2, 'short'))
            _until(lambda: len(writer.results()) == 1)
            _give(table, seats, _frame(3, 'short'))
            _until(lambda: len(writer.results()) == 2)
            assert [frame.dispatch for frame in writer.results()] == [2, 3]
        finally:
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
                f'task {_task_id(1)} escaped the seat runner: RuntimeError: escaped',
                CHILD_DEFECT_EXIT_CODE,
            ),
        ]
        assert writer.results() == []


class TestTheMarkerOfASeatThread:
    """On real seat threads, the end of a run is fixed at its return, then
    the marker runs, then the end is reported."""

    def test_a_return_before_the_deadline_keeps_its_result_when_the_marker_crosses_it(
        self,
        seats_of: Callable[
            ..., tuple[ThreadSeats, ThreadRunTable, _RecordingWriter, _Program]
        ],
    ) -> None:
        def returns_near_the_deadline() -> str:
            time.sleep(DEADLINE_S - 0.05)
            return 'done'

        seats, table, writer, program = seats_of(1, {'near': returns_near_the_deadline})
        program.marker = lambda: time.sleep(0.3)
        deadlines = threading.Thread(target=table.serve_deadlines, daemon=True)
        deadlines.start()
        try:
            _give(
                table,
                seats,
                _frame(1, 'near', work=thread_work(MIN_TASK_TIMEOUT_MS)),
            )
            _until(lambda: len(writer.results()) == 1)
        finally:
            table.stop()
            deadlines.join(WAIT_S)

        assert not any(isinstance(item, DeadlineFiredFrame) for item in writer.frames)
        assert writer.results()[0].end == ResultEnvelope('done')
        assert program.markers == [1]
        assert program.orders == {1: BodyFirst()}

    def test_a_run_that_timed_out_writes_no_marker(
        self,
        seats_of: Callable[
            ..., tuple[ThreadSeats, ThreadRunTable, _RecordingWriter, _Program]
        ],
    ) -> None:
        def late() -> str:
            time.sleep(DEADLINE_S + 0.15)
            return 'late'

        seats, table, writer, program = seats_of(1, {'late': late})
        deadlines = threading.Thread(target=table.serve_deadlines, daemon=True)
        deadlines.start()
        try:
            _give(
                table,
                seats,
                _frame(1, 'late', work=thread_work(MIN_TASK_TIMEOUT_MS)),
            )
            _until(lambda: len(writer.results()) == 1)
        finally:
            table.stop()
            deadlines.join(WAIT_S)

        assert program.markers == []
        assert [type(item).__name__ for item in writer.frames] == [
            'DeadlineFiredFrame',
            'SharedResultFrame',
        ]
        assert writer.results()[0].end == SeatTimedOut()
        assert program.orders == {1: DeadlineFirst()}

    def test_the_end_report_follows_the_marker(
        self,
        seats_of: Callable[
            ..., tuple[ThreadSeats, ThreadRunTable, _RecordingWriter, _Program]
        ],
    ) -> None:
        release = threading.Event()
        results_at_marker: list[int] = []
        seats, table, writer, program = seats_of(1, {'quick': lambda: 'quick'})

        def held() -> None:
            results_at_marker.append(len(writer.results()))
            assert release.wait(WAIT_S)

        program.marker = held
        try:
            _give(table, seats, _frame(1, 'quick'))
            _until(lambda: len(program.markers) == 1)
            time.sleep(0.05)
            assert writer.results() == []
        finally:
            release.set()
        _until(lambda: len(writer.results()) == 1)

        assert results_at_marker == [0]


class _Taker:
    """Records the runs that ``serve_runs`` hands to the seats."""

    def __init__(self) -> None:
        self.given: list[tuple[int, ThreadSeatWork]] = []

    def give(self, frame: core.SharedRunFrame, work: ThreadSeatWork) -> None:
        self.given.append((frame.dispatch, work))


class TestServeRuns:
    def _serve(self, taker: _Taker, *frames: WorkFrame) -> ThreadRunTable:
        """Serve ``frames``. Each test ends them with a stop frame, so a run
        that a defect should refuse ends the loop instead of a wait."""
        inbox = ParentFrameInbox()
        for frame in frames:
            inbox.put(frame)
        table = ThreadRunTable(2, _RecordingWriter(), _Clock())
        serve_runs(inbox, table, taker)
        return table

    def test_runs_go_to_the_seats_until_the_stop(self) -> None:
        taker = _Taker()
        table = ThreadRunTable(2, _RecordingWriter(), _Clock())
        inbox = ParentFrameInbox()
        inbox.put(_frame(1))
        inbox.put(_frame(2, work=REFUSAL_WORK))

        def end_runs_then_stop() -> None:
            try:
                _until(lambda: len(taker.given) == 2)
                for dispatch in (1, 2):
                    _ = table.returned(dispatch, set_running(OK_END))
                    table.finish(dispatch, RssMb(1.0))
            finally:
                inbox.put(stop_frame())

        stopper = threading.Thread(target=end_runs_then_stop)
        stopper.start()
        serve_runs(inbox, table, taker)
        stopper.join(WAIT_S)

        assert [(dispatch, _work_seen(work)) for dispatch, work in taker.given] == [
            (1, ('thread_task', 1000)),
            (2, ('refusal', 'TIMEOUT_VALUE_INVALID', 'y')),
        ]
        assert table.count() == 0

    def test_a_stop_while_runs_are_in_flight_is_a_parent_defect(
        self,
        exits: list[tuple[str, int]],
    ) -> None:
        taker = _Taker()
        with pytest.raises(_Exited):
            self._serve(taker, _frame(1), _frame(2), stop_frame())
        assert [dispatch for dispatch, _work in taker.given] == [1, 2]
        assert exits == [
            (
                'parent sent a stop frame while 2 runs are in flight',
                BAD_PARENT_FRAME_EXIT_CODE,
            ),
        ]

    def test_a_stop_with_no_run_in_flight_returns(self) -> None:
        taker = _Taker()
        table = self._serve(taker, stop_frame())
        assert table.count() == 0
        assert taker.given == []

    def test_the_task_work_of_a_shared_async_child_is_a_parent_defect(
        self,
        exits: list[tuple[str, int]],
    ) -> None:
        taker = _Taker()
        with pytest.raises(_Exited):
            self._serve(
                taker,
                _frame(1, work=async_work(1000, 100)),
                stop_frame(),
            )
        assert taker.given == []
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
        run = run_frame(task_id=_task_id(99))
        with pytest.raises(_Exited):
            self._serve(_Taker(), run, stop_frame())
        assert exits == [
            (
                'parent sent an isolated run frame to a shared thread child',
                BAD_PARENT_FRAME_EXIT_CODE,
            ),
        ]
