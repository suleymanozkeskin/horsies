"""One task run on a seat of a shared thread child (``run_thread_seat_task``).

Real registered tasks run here, on a thread. The database steps around the
body, the RUNNING transition and the finalizing marker, are replaced, or the
pool under them is; with a real database they are covered in
``tests/integration``.
"""

# pyright: reportPrivateUsage=false

from __future__ import annotations

import contextlib
import dataclasses
import json
import threading
import time
from collections.abc import Generator, Iterator
from datetime import datetime, timezone
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from pydantic import SecretStr

from horsies.core.app import Horsies
from horsies.core.codec.json_io import loads_json
from horsies.core.codec.typed import decode_task_result
from horsies.core.models.app import AppConfig
from horsies.core.models.broker import PostgresConfig
from horsies.core.models.tasks import (
    MAX_TASK_TIMEOUT_MS,
    MIN_TASK_TIMEOUT_MS,
    Execution,
    OperationalErrorCode,
    TaskError,
    TaskResult,
)
from horsies.core.worker import child_runner, thread_runner
from horsies.core.worker.child_protocol import (
    ChildFrame,
    RssMb,
    SeatRefusal,
    SeatTimedOut,
    SharedResultFrame,
    SharedRunFrame,
    ThreadSeatTask,
    ThreadSeatWork,
    WireResult,
)
from horsies.core.worker.child_runner import DeadlineBeforeStart, StartAllowed
from horsies.core.worker.shared_child import (
    MarkerBeforeDeadline,
    MarkerWithoutDeadline,
    SeatReturn,
    SetRunning,
    StoppedBeforeRunning,
)
from horsies.core.worker.stored_timeout import TimeoutValueInvalid
from horsies.core.worker.thread_child import ThreadRunTable, ThreadSeats

pytestmark = [pytest.mark.unit]

CLAIMED_AT = datetime(2026, 11, 10, 9, 0, tzinfo=timezone.utc)

APP = Horsies(
    AppConfig(
        broker=PostgresConfig(
            database_url=SecretStr('postgresql+psycopg://u:p@localhost/unused'),
        ),
    ),
)
RAN: list[str] = []


@APP.task(
    task_name='thread_double', execution=Execution.SHARED_THREAD, timeout_ms=10_000
)
def thread_double(*, x: int) -> TaskResult[int, TaskError]:
    RAN.append(threading.current_thread().name)
    return TaskResult(ok=x * 2)


@APP.task(task_name='thread_async', execution=Execution.SHARED_ASYNC, timeout_ms=10_000)
async def thread_async() -> TaskResult[int, TaskError]:
    RAN.append('thread_async')
    return TaskResult(ok=1)


@APP.task(task_name='thread_raises', execution=Execution.SHARED_THREAD, timeout_ms=1000)
def thread_raises() -> TaskResult[int, TaskError]:
    raise ValueError('bad input')


@APP.task(task_name='thread_never', execution=Execution.SHARED_THREAD, timeout_ms=1000)
def thread_never() -> TaskResult[int, TaskError]:
    RAN.append('thread_never')
    return TaskResult(ok=1)


def _frame(task_name: str, kwargs: dict[str, Any] | None = None) -> SharedRunFrame:
    return SharedRunFrame(
        dispatch=1,
        task_id='task-1',
        task_name=task_name,
        args_json='[]',
        kwargs_json=json.dumps(kwargs or {}),
        worker_id='worker-1',
        is_workflow_task=False,
        claimed_at=CLAIMED_AT,
        work=ThreadSeatTask(timeout_ms=60_000),
    )


class _Steps:
    """The replaced database steps, as the seat thread sees them."""

    def __init__(self) -> None:
        self.confirm = MagicMock(return_value=None)
        self.mark = MagicMock(return_value=None)


@pytest.fixture
def steps() -> Iterator[_Steps]:
    replaced = _Steps()
    with (
        patch.object(thread_runner, '_confirm_until_not_resumed', replaced.confirm),
        patch.object(thread_runner, '_mark_task_finalizing', replaced.mark),
    ):
        yield replaced


@pytest.fixture(autouse=True)
def fresh_marks() -> Iterator[None]:
    RAN.clear()
    yield


def _run(frame: SharedRunFrame, work: ThreadSeatWork | None = None) -> SeatReturn:
    """Run on a thread named as a seat, as the child does."""
    results: list[SeatReturn] = []

    def body() -> None:
        with patch.object(child_runner, 'get_current_app', MagicMock(return_value=APP)):
            match frame.work:
                case ThreadSeatTask() | SeatRefusal() as frame_work:
                    pass
                case _:
                    raise AssertionError(frame.work)
            returned = thread_runner.run_thread_seat_task(
                frame,
                work or frame_work,
                StartAllowed,
            )
            match returned:
                case SetRunning() | StoppedBeforeRunning():
                    results.append(returned)
                case DeadlineBeforeStart():
                    raise AssertionError('a start that is always allowed was refused')

    seat = threading.Thread(target=body, name='horsies-seat-0')
    seat.start()
    seat.join(10.0)
    assert results, 'the seat run did not return'
    return results[0]


def _set_running(seat_return: SeatReturn) -> WireResult:
    """The wire of a run that set its row RUNNING."""
    assert isinstance(seat_return, SetRunning), seat_return
    return seat_return.wire


def _result(wire: WireResult, ok_type: Any) -> TaskResult[Any, TaskError]:
    ok, payload, _reason = wire
    assert ok is True
    return decode_task_result(loads_json(payload).unwrap(), ok_type)


def test_a_def_task_runs_on_the_seat_thread(steps: _Steps) -> None:
    """The run ends at the result: the seat writes the marker after the run
    table fixed the end, not the run itself."""
    wire = _set_running(_run(_frame('thread_double', {'x': 21})))

    assert _result(wire, int).unwrap() == 42
    assert RAN == ['horsies-seat-0']
    steps.confirm.assert_called_once_with(
        'task-1',
        'worker-1',
        False,
        CLAIMED_AT,
        start_check=StartAllowed,
    )
    steps.mark.assert_not_called()


def test_an_async_def_task_is_refused_before_it_runs(steps: _Steps) -> None:
    wire = _set_running(_run(_frame('thread_async')))

    error = _result(wire, int).unwrap_err()
    assert error.error_code == OperationalErrorCode.WORKER_RESOLUTION_ERROR
    assert (
        wire[2]
        == 'the task is async def, and a shared thread child runs only def tasks'
    )
    assert RAN == []


def test_a_refusal_fails_the_task_after_running_and_before_resolve(
    steps: _Steps,
) -> None:
    refusal = TimeoutValueInvalid(
        reason='stored timeout_ms is absent for execution SHARED_THREAD',
    )
    frame = dataclasses.replace(
        _frame('thread_double', {'x': 21}),
        work=SeatRefusal(refusal=refusal),
    )

    wire = _set_running(_run(frame))

    assert wire == child_runner._stored_timeout_refusal_response(
        'thread_double', refusal
    )
    error = _result(wire, int).unwrap_err()
    assert error.error_code == OperationalErrorCode.WORKER_SERIALIZATION_ERROR
    assert error.data == {
        'task_name': 'thread_double',
        'field': 'timeout_ms',
        'min_ms': MIN_TASK_TIMEOUT_MS,
        'max_ms': MAX_TASK_TIMEOUT_MS,
    }
    assert RAN == []
    steps.confirm.assert_called_once()


def test_a_stop_before_start_runs_no_body_and_reports_no_running_row(
    steps: _Steps,
) -> None:
    steps.confirm.return_value = (False, '', 'CLAIM_LOST')

    assert _run(_frame('thread_never')) == StoppedBeforeRunning(
        (False, '', 'CLAIM_LOST'),
    )
    assert RAN == []
    steps.mark.assert_not_called()


def test_a_task_that_raises_is_mapped(steps: _Steps) -> None:
    wire = _set_running(_run(_frame('thread_raises')))

    error = _result(wire, int).unwrap_err()
    assert error.error_code == OperationalErrorCode.UNHANDLED_EXCEPTION


def test_an_unknown_task_is_a_resolution_error(steps: _Steps) -> None:
    wire = _set_running(_run(_frame('not_registered')))

    error = _result(wire, int).unwrap_err()
    assert error.error_code == OperationalErrorCode.WORKER_RESOLUTION_ERROR


class TestTheMarkerOfAThreadSeat:
    """``mark_thread_seat_finalizing``: the marker after the return, with
    the wait for a pooled connection bounded by the deadline of the run."""

    def test_a_run_with_a_deadline_waits_for_a_connection_until_it(
        self,
        steps: _Steps,
    ) -> None:
        thread_runner.mark_thread_seat_finalizing(
            _frame('thread_double'),
            MarkerBeforeDeadline(seconds=0.25),
        )

        steps.mark.assert_called_once_with(
            'task-1',
            'worker-1',
            CLAIMED_AT,
            connection_wait_s=0.25,
        )

    def test_a_refusal_keeps_the_wait_of_the_pool(self, steps: _Steps) -> None:
        thread_runner.mark_thread_seat_finalizing(
            _frame('thread_double'),
            MarkerWithoutDeadline(),
        )

        steps.mark.assert_called_once_with('task-1', 'worker-1', CLAIMED_AT)


class _SlowPool:
    """A sync pool that gives a connection ``wait_s`` after the ask, as a
    full pool does. Records each statement run on its connections."""

    def __init__(self, wait_s: float) -> None:
        self.wait_s = wait_s
        self.statements: list[str] = []

    @contextlib.contextmanager
    def connection(self, timeout: float | None = None) -> Generator[_Connection]:
        _ = timeout
        time.sleep(self.wait_s)
        yield _Connection(self.statements)


class _Connection:
    def __init__(self, statements: list[str]) -> None:
        self._statements = statements

    def cursor(self, row_factory: object = None) -> _Cursor:
        _ = row_factory
        return _Cursor(self._statements)

    def commit(self) -> None:
        return None

    def rollback(self) -> None:
        return None


class _Cursor:
    rowcount = 1

    def __init__(self, statements: list[str]) -> None:
        self._statements = statements

    def execute(self, sql: str, params: object = ()) -> None:
        _ = params
        self._statements.append(sql)

    def fetchone(self) -> tuple[str]:
        return ('task-1',)


class _RecordingWriter:
    def __init__(self) -> None:
        self.frames: list[ChildFrame] = []
        self._lock = threading.Lock()

    def send(self, frame: ChildFrame) -> None:
        with self._lock:
            self.frames.append(frame)

    def send_result(self, task_name: str, frame: SharedResultFrame) -> None:
        _ = task_name
        self.send(frame)

    def results(self) -> list[SharedResultFrame]:
        with self._lock:
            return [item for item in self.frames if isinstance(item, SharedResultFrame)]


def test_a_seat_that_gets_its_connection_after_the_deadline_does_not_start(
    fresh_marks: None,
) -> None:
    """The real seat thread, run table and seat program, on a pool that
    answers 0.5 s after the ask: the deadline (0.2 s) fires during the
    wait. The RUNNING update does not run, the body does not run, and the
    run ends with the timeout; its row stays CLAIMED for the worker to
    return to the queue."""
    _ = fresh_marks
    pool = _SlowPool(wait_s=0.5)
    writer = _RecordingWriter()
    table = ThreadRunTable(1, writer)
    deadlines = threading.Thread(target=table.serve_deadlines, daemon=True)
    seats = ThreadSeats(
        1, table, thread_runner.PostgresThreadSeats(), lambda: RssMb(1.0)
    )
    frame = dataclasses.replace(
        _frame('thread_double', {'x': 2}),
        work=ThreadSeatTask(timeout_ms=200),
    )
    with (
        patch.object(child_runner, '_get_worker_pool', MagicMock(return_value=pool)),
        patch.object(child_runner, 'get_current_app', MagicMock(return_value=APP)),
    ):
        deadlines.start()
        assert seats.start().is_ok()
        try:
            table.add(frame, ThreadSeatTask(timeout_ms=200))
            seats.give(frame, ThreadSeatTask(timeout_ms=200))
            ends_by = time.monotonic() + 10.0
            while not writer.results():
                assert time.monotonic() < ends_by, 'the run did not end'
                time.sleep(0.01)
        finally:
            seats.stop(10.0)
            table.stop()
            deadlines.join(10.0)

    assert RAN == []
    assert pool.statements == []
    assert [type(item).__name__ for item in writer.frames] == [
        'DeadlineFiredFrame',
        'SharedResultFrame',
    ]
    assert writer.results()[0].end == SeatTimedOut()
