"""One task run on a seat of a shared thread child (``run_thread_seat_task``).

Real registered tasks run here, on a thread, from version-2 frames as the
core decodes them. The database steps around the body, the RUNNING
transition and the finalizing marker, are replaced, or the core handle under
them is; with a real database they are covered in ``tests/integration``.
"""

# pyright: reportPrivateUsage=false

from __future__ import annotations

import json
import threading
import time
import uuid
from collections.abc import Iterator
from typing import Any, Final
from unittest.mock import MagicMock, call, patch

import pytest
from pydantic import SecretStr

import horsies._core as core
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
from horsies.core.worker.child_frames import (
    ChildFrame,
    RssMb,
    SeatTimedOut,
    SharedResultFrame,
    ThreadSeatWork,
)
from horsies.core.worker.child_runner import (
    DeadlineBeforeStart,
    StartAllowed,
)
from horsies.core.worker.run_return import CoreStoppedBeforeStart, ResultEnvelope
from horsies.core.worker.shared_child import (
    SeatReturn,
    SetRunning,
    StoppedBeforeRunning,
)
from horsies.core.worker.stored_timeout import TimeoutValueInvalid
from horsies.core.worker.thread_child import ThreadRunTable, ThreadSeats
from tests.fixtures.core_frames import (
    CLAIMED_AT,
    refusal,
    shared_run_frame,
    thread_work,
)

pytestmark = [pytest.mark.unit]

TASK_ID: Final = uuid.UUID('0190f2d8-7c1e-7a3b-9f00-000000000041')

APP = Horsies(
    AppConfig(
        broker=PostgresConfig(
            database_url=SecretStr('postgresql://u:p@localhost/unused'),
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


def _frame(
    task_name: str,
    kwargs: dict[str, Any] | None = None,
    *,
    task_id: uuid.UUID = TASK_ID,
    work: dict[str, object] | None = None,
) -> core.SharedRunFrame:
    return shared_run_frame(
        dispatch=1,
        task_id=task_id,
        task_name=task_name,
        work=work if work is not None else thread_work(60_000),
        orphan_self_heal=True,
        input_json=json.dumps(kwargs or {}),
    )


def _seat_work(frame: core.SharedRunFrame) -> ThreadSeatWork:
    """The work of a frame of a thread seat."""
    match frame.work:
        case core.ThreadTaskWork() | core.StoredOptionsRefused() as work:
            return work
        case core.AsyncTaskWork():
            raise AssertionError(f'run {frame.dispatch} is not a thread seat run')


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
        patch.object(thread_runner, 'mark_run_finalizing', replaced.mark),
    ):
        yield replaced


@pytest.fixture(autouse=True)
def fresh_marks() -> Iterator[None]:
    RAN.clear()
    yield


def _run(frame: core.SharedRunFrame) -> SeatReturn:
    """Run on a thread named as a seat, as the child does."""
    results: list[SeatReturn] = []

    def body() -> None:
        with patch.object(child_runner, 'get_current_app', MagicMock(return_value=APP)):
            returned = thread_runner.run_thread_seat_task(
                frame,
                _seat_work(frame),
                StartAllowed,
            )
            match returned:
                case SetRunning() | StoppedBeforeRunning():
                    results.append(returned)
                case DeadlineBeforeStart():
                    raise AssertionError('a start that is always allowed was refused')
                case CoreStoppedBeforeStart(stopped=stopped):
                    raise AssertionError(f'the core of the test process stopped: {stopped}')

    seat = threading.Thread(target=body, name='horsies-seat-0')
    seat.start()
    seat.join(10.0)
    assert results, 'the seat run did not return'
    return results[0]


def _set_running(seat_return: SeatReturn) -> ResultEnvelope:
    """The result envelope of a run that set its row RUNNING."""
    assert isinstance(seat_return, SetRunning), seat_return
    return seat_return.result


def _result(envelope: ResultEnvelope, ok_type: Any) -> TaskResult[Any, TaskError]:
    return decode_task_result(loads_json(envelope.text).unwrap(), ok_type)


def test_a_def_task_runs_on_the_seat_thread(steps: _Steps) -> None:
    """The run ends at the result: the seat writes the marker after the run
    table fixed the end, not the run itself."""
    envelope = _set_running(_run(_frame('thread_double', {'x': 21})))

    assert _result(envelope, int).unwrap() == 42
    assert RAN == ['horsies-seat-0']
    steps.confirm.assert_called_once_with(
        str(TASK_ID),
        'worker-1',
        CLAIMED_AT,
        orphan_self_heal=True,
        start_check=StartAllowed,
    )
    steps.mark.assert_not_called()


def test_an_async_def_task_is_refused_before_it_runs(steps: _Steps) -> None:
    envelope = _set_running(_run(_frame('thread_async')))

    error = _result(envelope, int).unwrap_err()
    assert error.error_code == OperationalErrorCode.WORKER_RESOLUTION_ERROR
    assert error.message == (
        'Failed to resolve thread_async: the task is async def, and a shared '
        'thread child runs only def tasks'
    )
    assert RAN == []


def test_a_refusal_fails_the_task_after_running_and_before_resolve(
    steps: _Steps,
) -> None:
    reason = 'stored timeout_ms is absent for execution SHARED_THREAD'
    frame = _frame(
        'thread_double',
        {'x': 21},
        work=refusal('value_invalid', reason),
    )

    envelope = _set_running(_run(frame))

    assert envelope == child_runner._stored_timeout_refusal_response(
        'thread_double',
        TimeoutValueInvalid(reason=reason),
    )
    error = _result(envelope, int).unwrap_err()
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
    steps.confirm.return_value = StoppedBeforeRunning('CLAIM_LOST')

    assert _run(_frame('thread_never')) == StoppedBeforeRunning('CLAIM_LOST')
    assert RAN == []
    steps.mark.assert_not_called()


def test_a_task_that_raises_is_mapped(steps: _Steps) -> None:
    envelope = _set_running(_run(_frame('thread_raises')))

    error = _result(envelope, int).unwrap_err()
    assert error.error_code == OperationalErrorCode.UNHANDLED_EXCEPTION


def test_an_unknown_task_is_a_resolution_error(steps: _Steps) -> None:
    envelope = _set_running(_run(_frame('not_registered')))

    error = _result(envelope, int).unwrap_err()
    assert error.error_code == OperationalErrorCode.WORKER_RESOLUTION_ERROR


class TestTheMarkerOfAThreadSeat:
    """``mark_thread_seat_finalizing``: the marker after the return goes
    through the core handle of the child, fenced on the claim of the frame,
    with the bound of the core for a task and for a refusal alike."""

    def test_a_task_and_a_refusal_write_the_marker_of_their_claim(
        self,
        steps: _Steps,
    ) -> None:
        seats = thread_runner.PostgresThreadSeats()

        seats.mark_finalizing(_frame('thread_double'))
        seats.mark_finalizing(
            _frame('thread_double', work=refusal('value_invalid', 'no stored timeout')),
        )

        assert steps.mark.call_args_list == [
            call(TASK_ID, 'worker-1', CLAIMED_AT),
            call(TASK_ID, 'worker-1', CLAIMED_AT),
        ]

    def test_the_refusal_writes_are_bounded_as_in_a_shared_async_child(self) -> None:
        """The refusal backstop of the parent relies on this bound: two
        start attempts and the marker, each with the end of a cancelled
        statement."""
        assert thread_runner.THREAD_REFUSAL_WRITES_BOUND_S == 120.0


class _SlowHandle:
    """A core handle of the child whose start gets its pooled connection
    ``wait_s`` after the ask, as a full pool does, then checks the time left
    that the seat sent, as the core does: past it, no statement runs
    (``DeadlineBeforeStart``). Records each start that ran a statement."""

    def __init__(self, wait_s: float) -> None:
        self.wait_s = wait_s
        self.deadlines_in_ms: list[int | None] = []
        self.started: list[uuid.UUID] = []

    def start_attempt(self, task_id: uuid.UUID, **kwargs: Any) -> Any:
        deadline_in_ms = kwargs['deadline_in_ms']
        self.deadlines_in_ms.append(deadline_in_ms)
        time.sleep(self.wait_s)
        if deadline_in_ms is not None and deadline_in_ms < self.wait_s * 1000.0:
            record = MagicMock(spec=core.DeadlineBeforeStart)
            record.task_id = task_id
            return record
        self.started.append(task_id)
        record = MagicMock(spec=core.AttemptStarted)
        record.task_id = task_id
        return record


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
    """The real seat thread, run table and seat program, on a core handle
    whose pool answers 1.5 s after the ask: the deadline (1 s, the smallest
    ``timeout_ms`` of a run frame) fires during the wait. The RUNNING update
    does not run, the body does not run, and the run ends with the timeout;
    its row stays CLAIMED for the worker to return to the queue."""
    _ = fresh_marks
    handle = _SlowHandle(wait_s=MIN_TASK_TIMEOUT_MS / 1000 + 0.5)
    writer = _RecordingWriter()
    table = ThreadRunTable(1, writer)
    deadlines = threading.Thread(target=table.serve_deadlines, daemon=True)
    seats = ThreadSeats(
        1, table, thread_runner.PostgresThreadSeats(), lambda: RssMb(1.0)
    )
    frame = _frame(
        'thread_double',
        {'x': 2},
        task_id=uuid.uuid4(),
        work=thread_work(MIN_TASK_TIMEOUT_MS),
    )
    work = _seat_work(frame)
    with (
        patch.object(child_runner, '_get_child_core_handle', MagicMock(return_value=handle)),
        patch.object(child_runner, 'get_current_app', MagicMock(return_value=APP)),
    ):
        deadlines.start()
        assert seats.start().is_ok()
        try:
            table.add(frame, work)
            seats.give(frame, work)
            ends_by = time.monotonic() + 10.0
            while not writer.results():
                assert time.monotonic() < ends_by, 'the run did not end'
                time.sleep(0.01)
        finally:
            seats.stop(10.0)
            table.stop()
            deadlines.join(10.0)

    assert RAN == []
    assert handle.started == []
    [deadline_in_ms] = handle.deadlines_in_ms
    assert deadline_in_ms is not None
    assert 1 <= deadline_in_ms <= MIN_TASK_TIMEOUT_MS
    assert [type(item).__name__ for item in writer.frames] == [
        'DeadlineFiredFrame',
        'SharedResultFrame',
    ]
    assert writer.results()[0].end == SeatTimedOut()
