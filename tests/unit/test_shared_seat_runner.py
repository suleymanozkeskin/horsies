"""One task run on a seat of a shared async child (``run_seat_task``), and
the finalizing marker that the seat loop asks for at its end
(``mark_seat_finalizing``).

Real registered tasks run here. The database steps, the RUNNING transition
and the finalizing marker, are replaced; their own tests are in
``test_shared_lifecycle``.
"""

# pyright: reportPrivateUsage=false

from __future__ import annotations

import asyncio
import dataclasses
import json
import logging
import time
from collections.abc import Awaitable, Callable, Iterator
from datetime import datetime, timezone
from typing import Any, NoReturn
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from pydantic import SecretStr

from horsies.core.app import Horsies
from horsies.core.codec.json_io import loads_json
from horsies.core.codec.typed import decode_task_result
from horsies.core.models.app import AppConfig
from horsies.core.models.broker import PostgresConfig
from horsies.core.models.tasks import (
    MAX_CANCEL_GRACE_MS,
    MIN_CANCEL_GRACE_MS,
    Execution,
    OperationalErrorCode,
    TaskError,
    TaskResult,
)
from horsies.core.worker import child_runner, shared_child, shared_runner
from horsies.core.worker.child_inbox import ParentFrameInbox
from horsies.core.worker.child_protocol import (
    ChildFrame,
    RssMb,
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
    FrameWithResult,
    MarkerBeforeDeadline,
    MarkerWindow,
    MarkerWithoutDeadline,
    SeatReturn,
    SetRunning,
    StoppedBeforeRunning,
)
from horsies.core.worker.stored_timeout import CancelGraceInvalid, TimeoutValueInvalid

pytestmark = [pytest.mark.unit]

CLAIMED_AT = datetime(2026, 11, 10, 9, 0, tzinfo=timezone.utc)
SCENARIO_TIMEOUT_S = 5.0
"""The longest wait of a test for an event of the code under test."""


APP = Horsies(
    AppConfig(
        broker=PostgresConfig(
            database_url=SecretStr('postgresql+psycopg://u:p@localhost/unused'),
        ),
    ),
)
RAN: list[str] = []
STARTED = asyncio.Event()


@APP.task(task_name='seat_double', execution=Execution.SHARED_ASYNC, timeout_ms=10_000)
async def seat_double(*, x: int) -> TaskResult[int, TaskError]:
    await asyncio.sleep(0)
    return TaskResult(ok=x * 2)


@APP.task(task_name='seat_sync')
def seat_sync() -> TaskResult[int, TaskError]:
    RAN.append('seat_sync')
    return TaskResult(ok=1)


@APP.task(task_name='seat_never', execution=Execution.SHARED_ASYNC, timeout_ms=1000)
async def seat_never() -> TaskResult[int, TaskError]:
    RAN.append('seat_never')
    return TaskResult(ok=1)


@APP.task(task_name='seat_raises', execution=Execution.SHARED_ASYNC, timeout_ms=1000)
async def seat_raises() -> TaskResult[int, TaskError]:
    raise ValueError('bad input')


@APP.task(task_name='seat_waits', execution=Execution.SHARED_ASYNC, timeout_ms=1000)
async def seat_waits() -> TaskResult[int, TaskError]:
    STARTED.set()
    await asyncio.Event().wait()
    return TaskResult(ok=1)


@APP.task(task_name='seat_quick', execution=Execution.SHARED_ASYNC, timeout_ms=1000)
async def seat_quick() -> TaskResult[int, TaskError]:
    return TaskResult(ok=1)


@APP.task(
    task_name='seat_cancels_itself',
    execution=Execution.SHARED_ASYNC,
    timeout_ms=1000,
    exception_mapper={asyncio.CancelledError: 'CANCELLED_BY_BODY'},
)
async def seat_cancels_itself() -> TaskResult[int, TaskError]:
    current = asyncio.current_task()
    assert current is not None
    current.cancel()
    await asyncio.sleep(0)
    return TaskResult(ok=1)


@APP.task(
    task_name='seat_cancels_itself_and_returns',
    execution=Execution.SHARED_ASYNC,
    timeout_ms=1000,
    exception_mapper={asyncio.CancelledError: 'CANCELLED_BY_BODY'},
)
async def seat_cancels_itself_and_returns() -> TaskResult[int, TaskError]:
    """Asks to cancel its own task, then returns before any ``await``: the
    request is still pending when the call returns."""
    current = asyncio.current_task()
    assert current is not None
    current.cancel()
    return TaskResult(ok=1)


@APP.task(
    task_name='seat_raises_generator_exit',
    execution=Execution.SHARED_ASYNC,
    timeout_ms=1000,
    exception_mapper={GeneratorExit: 'GENERATOR_EXIT_OF_BODY'},
)
async def seat_raises_generator_exit() -> TaskResult[int, TaskError]:
    raise GeneratorExit


RELEASE: list[asyncio.Event] = []
"""The event that the work of ``seat_leaves_work`` and ``seat_left_work_ignores_cancel``
waits for. Each test that runs them puts a fresh one, on its own loop."""


async def _wait_for_release() -> None:
    await RELEASE[0].wait()


async def _ignore_cancels_until_release() -> None:
    while not RELEASE[0].is_set():
        try:
            await RELEASE[0].wait()
        except asyncio.CancelledError:
            continue


@APP.task(
    task_name='seat_leaves_work', execution=Execution.SHARED_ASYNC, timeout_ms=10_000
)
async def seat_leaves_work() -> TaskResult[int, TaskError]:
    """Returns while an asyncio task that it started still runs."""
    _ = asyncio.get_running_loop().create_task(_wait_for_release())
    return TaskResult(ok=5)


@APP.task(
    task_name='seat_left_work_ignores_cancel',
    execution=Execution.SHARED_ASYNC,
    timeout_ms=10_000,
)
async def seat_left_work_ignores_cancel() -> TaskResult[int, TaskError]:
    """Waits until its deadline cancel. An asyncio task that it started
    ignores every cancel until ``RELEASE`` is set."""
    _ = asyncio.get_running_loop().create_task(_ignore_cancels_until_release())
    await asyncio.Event().wait()
    return TaskResult(ok=6)


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
        work=SeatTask(timeout_ms=60_000, cancel_grace_ms=1_000),
    )


def _frame_for(
    dispatch: int,
    task_name: str,
    kwargs: dict[str, Any] | None = None,
) -> SharedRunFrame:
    return dataclasses.replace(
        _frame(task_name, kwargs),
        dispatch=dispatch,
        task_id=f'task-{dispatch}',
    )


def _wire_of(end: SeatFinished) -> WireResult:
    return end.ok, end.result_json, end.failed_reason


class _RecordingWriter:
    """Records the frames of a seat loop, as the writer thread would write them."""

    def __init__(self) -> None:
        self.frames: list[ChildFrame] = []

    def send(self, frame: ChildFrame) -> None:
        self.frames.append(frame)

    def send_result(self, task_name: str, frame: FrameWithResult) -> None:
        self.frames.append(frame)

    def results(self) -> list[SharedResultFrame]:
        return [frame for frame in self.frames if isinstance(frame, SharedResultFrame)]


class _Steps:
    """The replaced database steps, as the seat sees them."""

    def __init__(self) -> None:
        self.confirm = AsyncMock(return_value=None)
        self.mark = AsyncMock(return_value=None)


@pytest.fixture
def steps() -> Iterator[_Steps]:
    replaced = _Steps()
    with (
        patch.object(shared_runner, 'confirm_until_not_resumed', replaced.confirm),
        patch.object(shared_runner, 'mark_task_finalizing', replaced.mark),
    ):
        yield replaced


def _run(frame: SharedRunFrame) -> SeatReturn:
    with patch.object(child_runner, 'get_current_app', MagicMock(return_value=APP)):
        return asyncio.run(shared_runner.run_seat_task(MagicMock(), frame))


def _set_running(seat_return: SeatReturn) -> WireResult:
    """The wire of a run that set its row RUNNING."""
    assert isinstance(seat_return, SetRunning), seat_return
    return seat_return.wire


def _result(wire: WireResult, ok_type: Any) -> TaskResult[Any, TaskError]:
    ok, payload, _reason = wire
    assert ok is True
    return decode_task_result(loads_json(payload).unwrap(), ok_type)


@pytest.fixture(autouse=True)
def fresh_marks() -> Iterator[None]:
    RAN.clear()
    STARTED.clear()
    RELEASE.clear()
    yield


def test_an_async_task_runs_on_the_running_loop(steps: _Steps) -> None:
    """The run ends at the result: the seat loop writes the marker at the
    end of the run, not the run itself."""
    wire = _set_running(_run(_frame('seat_double', {'x': 21})))

    assert _result(wire, int).unwrap() == 42
    assert wire[2] is None
    steps.confirm.assert_awaited_once()
    steps.mark.assert_not_awaited()


def test_a_def_task_is_refused_before_it_runs(steps: _Steps) -> None:
    wire = _set_running(_run(_frame('seat_sync')))

    error = _result(wire, int).unwrap_err()
    assert error.error_code == OperationalErrorCode.WORKER_RESOLUTION_ERROR
    assert wire[2] == (
        'the task is def, and a shared async child runs only async def tasks'
    )
    assert RAN == []


def test_a_refusal_fails_the_task_after_running_and_before_resolve(
    steps: _Steps,
) -> None:
    """The response of an isolated child for a refused stored timeout."""
    refusal = CancelGraceInvalid(reason='stored cancel_grace_ms is below 100 ms')
    frame = dataclasses.replace(
        _frame('seat_double', {'x': 21}),
        work=SeatRefusal(refusal=refusal),
    )

    wire = _set_running(_run(frame))

    assert wire == child_runner._stored_timeout_refusal_response('seat_double', refusal)
    error = _result(wire, int).unwrap_err()
    assert error.error_code == OperationalErrorCode.WORKER_SERIALIZATION_ERROR
    assert error.data == {
        'task_name': 'seat_double',
        'field': 'cancel_grace_ms',
        'min_ms': MIN_CANCEL_GRACE_MS,
        'max_ms': MAX_CANCEL_GRACE_MS,
    }
    assert RAN == []
    steps.confirm.assert_awaited_once()
    steps.mark.assert_not_awaited()


def test_a_refusal_after_a_stop_before_start_returns_the_stop(steps: _Steps) -> None:
    steps.confirm.return_value = (False, '', 'CLAIM_LOST')
    frame = dataclasses.replace(
        _frame('seat_never'),
        work=SeatRefusal(refusal=TimeoutValueInvalid(reason='y')),
    )

    assert _run(frame) == StoppedBeforeRunning((False, '', 'CLAIM_LOST'))
    steps.mark.assert_not_awaited()


def test_a_stop_before_start_runs_no_body_and_reports_no_running_row(
    steps: _Steps,
) -> None:
    steps.confirm.return_value = (False, '', 'CLAIM_LOST')

    assert _run(_frame('seat_never')) == StoppedBeforeRunning(
        (False, '', 'CLAIM_LOST'),
    )
    assert RAN == []
    steps.mark.assert_not_awaited()


def test_a_task_that_raises_is_mapped(steps: _Steps) -> None:
    wire = _set_running(_run(_frame('seat_raises')))

    error = _result(wire, int).unwrap_err()
    assert error.error_code == OperationalErrorCode.UNHANDLED_EXCEPTION


def test_a_cancel_of_the_body_ends_the_run(steps: _Steps) -> None:
    """The seat loop turns a cancel of its deadline into the timeout; the run
    itself only has to end."""

    async def cancel_after_start() -> SeatReturn:
        run = asyncio.ensure_future(
            shared_runner.run_seat_task(MagicMock(), _frame('seat_waits')),
        )
        await asyncio.wait_for(STARTED.wait(), SCENARIO_TIMEOUT_S)
        run.cancel()
        return await run

    with patch.object(child_runner, 'get_current_app', MagicMock(return_value=APP)):
        wire = _set_running(asyncio.run(cancel_after_start()))

    error = _result(wire, int).unwrap_err()
    assert error.error_code == OperationalErrorCode.UNHANDLED_EXCEPTION
    assert 'CancelledError' in (error.message or '')


def test_a_body_that_cancels_its_own_task_goes_through_the_mapper(
    steps: _Steps,
) -> None:
    """No deadline fired: the cancel request is the body's own. The worker
    maps it through the exception mapper of the task."""
    wire = _set_running(_run(_frame('seat_cancels_itself')))

    error = _result(wire, int).unwrap_err()
    assert error.error_code == 'CANCELLED_BY_BODY'


def test_a_generator_exit_of_the_body_goes_through_the_mapper(steps: _Steps) -> None:
    """The seat maps what ``await_task_call_in_worker`` maps: a
    ``GeneratorExit`` that leaves the body as well."""
    wire = _set_running(_run(_frame('seat_raises_generator_exit')))

    error = _result(wire, int).unwrap_err()
    assert error.error_code == 'GENERATOR_EXIT_OF_BODY'


class TestTheMarkerOfASeat:
    """``mark_seat_finalizing``: the marker that the seat loop asks for at
    the end of a run, within its window."""

    def test_a_run_without_a_deadline_writes_the_marker(self, steps: _Steps) -> None:
        frame = _frame('seat_quick')

        asyncio.run(
            shared_runner.mark_seat_finalizing(
                MagicMock(),
                frame,
                MarkerWithoutDeadline(),
            ),
        )

        steps.mark.assert_awaited_once()
        assert steps.mark.await_args is not None
        assert steps.mark.await_args.args[1:] == ('task-1', 'worker-1', CLAIMED_AT)

    def test_a_marker_within_its_window_is_written(self, steps: _Steps) -> None:
        asyncio.run(
            shared_runner.mark_seat_finalizing(
                MagicMock(),
                _frame('seat_quick'),
                MarkerBeforeDeadline(seconds=5.0),
            ),
        )

        steps.mark.assert_awaited_once()

    def test_a_marker_past_its_window_is_cut_and_logged(
        self,
        steps: _Steps,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """The deadline of the run comes first: the write is cut there, and
        the call returns, so the end of the run follows at once."""
        cut = asyncio.Event()

        async def waits_for_a_connection(*_args: object) -> None:
            try:
                await asyncio.Event().wait()
            except asyncio.CancelledError:
                cut.set()
                raise

        steps.mark.side_effect = waits_for_a_connection

        async def scenario() -> float:
            started = time.monotonic()
            await shared_runner.mark_seat_finalizing(
                MagicMock(),
                _frame('seat_quick'),
                MarkerBeforeDeadline(seconds=0.05),
            )
            return time.monotonic() - started

        with caplog.at_level(logging.WARNING, logger='horsies.worker'):
            took_s = asyncio.run(scenario())

        assert cut.is_set()
        assert took_s < 1.0
        assert any(
            'Finalizing marker of task task-1 skipped' in record.getMessage()
            for record in caplog.records
        )

    def test_a_cancel_of_the_caller_propagates(self, steps: _Steps) -> None:
        marking = asyncio.Event()

        async def slow_marker(*_args: object) -> None:
            marking.set()
            await asyncio.Event().wait()

        steps.mark.side_effect = slow_marker

        async def cancel_in_marker() -> None:
            run = asyncio.ensure_future(
                shared_runner.mark_seat_finalizing(
                    MagicMock(),
                    _frame('seat_quick'),
                    MarkerBeforeDeadline(seconds=60.0),
                ),
            )
            await asyncio.wait_for(marking.wait(), SCENARIO_TIMEOUT_S)
            run.cancel()
            with pytest.raises(asyncio.CancelledError):
                await run

        asyncio.run(cancel_in_marker())


def test_a_body_that_returns_after_cancelling_its_own_task_goes_through_the_mapper(
    steps: _Steps,
) -> None:
    """The cancel request is still pending when the body returns. The run
    ends as a cancel of the body, through its mapper, and the seat task
    does not end cancelled."""
    wire = _set_running(_run(_frame('seat_cancels_itself_and_returns')))

    error = _result(wire, int).unwrap_err()
    assert error.error_code == 'CANCELLED_BY_BODY'


class _Exited(BaseException):
    """Raised by the patched defect exit, as the real one ends the process."""


class _RealSeatRunner:
    """``StartedSeats`` that runs the real ``run_seat_task`` and the real
    ``mark_seat_finalizing``, on a pool that the patched steps never use."""

    async def run(self, frame: SharedRunFrame) -> SeatReturn:
        return await shared_runner.run_seat_task(MagicMock(), frame)

    async def mark_finalizing(
        self, frame: SharedRunFrame, window: MarkerWindow
    ) -> None:
        await shared_runner.mark_seat_finalizing(MagicMock(), frame, window)

    async def stop(self) -> None:
        return None


@pytest.fixture
def exits(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    """The defect exits of the seat loop, each recorded and raised."""
    seen: list[str] = []

    def record_exit(message: str, exit_code: int) -> NoReturn:
        seen.append(f'{exit_code}: {message}')
        raise _Exited(message)

    monkeypatch.setattr(shared_child, '_exit_for_defect', record_exit)
    return seen


def _timed(frame: SharedRunFrame, timeout_ms: int, grace_ms: int) -> SharedRunFrame:
    return dataclasses.replace(
        frame,
        work=SeatTask(timeout_ms=timeout_ms, cancel_grace_ms=grace_ms),
    )


type _Scenario = Callable[[ParentFrameInbox, _RecordingWriter], Awaitable[None]]


def _on_seat_loop(seats: int, scenario: _Scenario) -> _RecordingWriter:
    """Run ``scenario`` beside a real seat loop that serves with
    ``_RealSeatRunner``, then stop the loop. The scenario returns when every
    run it sent has ended."""
    writer = _RecordingWriter()
    inbox = ParentFrameInbox()

    async def serve() -> None:
        seat_loop = shared_child.SeatLoop(seats, writer, lambda: RssMb(1.0))  # type: ignore[arg-type]
        seat_loop.install_marks()
        serving = asyncio.ensure_future(seat_loop.serve(inbox, _RealSeatRunner()))
        try:
            await asyncio.wait_for(scenario(inbox, writer), SCENARIO_TIMEOUT_S)
        finally:
            # Work that ignores cancels ends here, also when the scenario
            # failed, so the loop can close.
            for release in RELEASE:
                release.set()
        inbox.put(StopFrame())
        await asyncio.wait_for(serving, SCENARIO_TIMEOUT_S)

    with patch.object(child_runner, 'get_current_app', MagicMock(return_value=APP)):
        asyncio.run(serve())
    return writer


async def _until(condition: Callable[[], bool]) -> None:
    while not condition():
        await asyncio.sleep(0.001)


def _kinds(writer: _RecordingWriter, dispatch: int) -> list[str]:
    return [
        type(frame).__name__
        for frame in writer.frames
        if getattr(frame, 'dispatch', None) == dispatch
    ]


def test_a_pending_self_cancel_on_the_seat_loop_ends_only_that_run(
    steps: _Steps,
    exits: list[str],
) -> None:
    """On the real seat loop, a body that leaves a cancel request on its
    seat task and returns ends as its mapped cancel. The child does not exit
    as for a defect, and the neighbour run ends with its own result."""

    async def scenario(inbox: ParentFrameInbox, writer: _RecordingWriter) -> None:
        inbox.put(_frame_for(1, 'seat_double', {'x': 4}))
        inbox.put(_frame_for(2, 'seat_cancels_itself_and_returns'))
        await _until(lambda: len(writer.results()) == 2 or bool(exits))

    writer = _on_seat_loop(2, scenario)

    assert exits == []
    ends = {frame.dispatch: frame.end for frame in writer.results()}
    neighbour = ends[1]
    assert isinstance(neighbour, SeatFinished)
    assert _result(_wire_of(neighbour), int).unwrap() == 8
    cancelled = ends[2]
    assert isinstance(cancelled, SeatFinished)
    error = _result(_wire_of(cancelled), int).unwrap_err()
    assert error.error_code == 'CANCELLED_BY_BODY'


class TestTheMarkerOnTheSeatLoop:
    """The finalizing marker is the last write of a run: at the end of the
    run, after its coroutine and its marked work, only for a run that ended
    without a cancel, and never past its deadline."""

    def test_a_return_before_the_deadline_keeps_its_result_when_the_marker_crosses_it(
        self,
        steps: _Steps,
        exits: list[str],
    ) -> None:
        """The marker waits on the pool past the deadline of the run. The
        run returned in time, so its end is what it returned; the marker is
        cut at the deadline, and no deadline report goes out."""

        async def waits_on_the_pool(*_args: object) -> None:
            await asyncio.sleep(1.5)

        steps.mark.side_effect = waits_on_the_pool
        took_s: list[float] = []

        async def scenario(inbox: ParentFrameInbox, writer: _RecordingWriter) -> None:
            sent = time.monotonic()
            inbox.put(_timed(_frame_for(1, 'seat_quick'), 300, 1000))
            await _until(lambda: len(writer.results()) == 1)
            took_s.append(time.monotonic() - sent)

        writer = _on_seat_loop(1, scenario)

        assert exits == []
        assert _kinds(writer, 1) == ['SharedResultFrame']
        end = writer.results()[0].end
        assert isinstance(end, SeatFinished)
        assert _result(_wire_of(end), int).unwrap() == 1
        steps.mark.assert_awaited_once()
        # The end report came at the deadline (0.3 s), not when the marker
        # would have ended (1.5 s).
        assert 0.25 <= took_s[0] < 1.0

    def test_runs_that_timed_out_write_no_marker_in_their_grace(
        self,
        steps: _Steps,
        exits: list[str],
    ) -> None:
        """Every run reaches its deadline at once, and each lets the cancel
        end it. No marker waits on the pool inside the cancel grace, so no
        run needs a second cancel."""
        one_connection = asyncio.Semaphore(1)

        async def one_pooled_connection(*_args: object) -> None:
            async with one_connection:
                await asyncio.sleep(0.2)

        steps.mark.side_effect = one_pooled_connection
        seats = 8

        async def scenario(inbox: ParentFrameInbox, writer: _RecordingWriter) -> None:
            for dispatch in range(1, seats + 1):
                inbox.put(_timed(_frame_for(dispatch, 'seat_waits'), 200, 100))
            await _until(lambda: len(writer.results()) == seats)

        writer = _on_seat_loop(seats, scenario)

        assert exits == []
        for dispatch in range(1, seats + 1):
            assert _kinds(writer, dispatch) == [
                'DeadlineFiredFrame',
                'SharedResultFrame',
            ]
        assert all(frame.end == SeatTimedOut() for frame in writer.results())
        steps.mark.assert_not_awaited()

    def test_the_marker_waits_for_the_work_that_a_run_left(
        self,
        steps: _Steps,
        exits: list[str],
    ) -> None:
        """The coroutine returned while an asyncio task that it started
        still runs: the attempt has not ended, so no marker yet. The marker
        comes when that work ends, before the end report."""
        results_at_marker: list[int] = []
        writers: list[_RecordingWriter] = []

        async def record(*_args: object) -> None:
            results_at_marker.append(len(writers[0].results()))

        steps.mark.side_effect = record

        async def scenario(inbox: ParentFrameInbox, writer: _RecordingWriter) -> None:
            writers.append(writer)
            RELEASE.append(asyncio.Event())
            inbox.put(_timed(_frame_for(1, 'seat_leaves_work'), 5_000, 2_000))
            await asyncio.sleep(0.2)
            assert results_at_marker == [], 'the marker came before the work ended'
            assert writer.results() == []
            RELEASE[0].set()
            await _until(lambda: len(writer.results()) == 1)

        writer = _on_seat_loop(1, scenario)

        assert exits == []
        assert results_at_marker == [0]
        end = writer.results()[0].end
        assert isinstance(end, SeatFinished)
        assert _result(_wire_of(end), int).unwrap() == 5

    def test_a_run_whose_work_is_stuck_after_its_deadline_writes_no_marker(
        self,
        steps: _Steps,
        exits: list[str],
    ) -> None:
        """The coroutine ends at its deadline cancel, and work that it
        started ignores every cancel: the attempt still runs, so the beats
        of the worker must cover it. No marker comes, also when that work
        ends later."""

        async def scenario(inbox: ParentFrameInbox, writer: _RecordingWriter) -> None:
            RELEASE.append(asyncio.Event())
            inbox.put(
                _timed(_frame_for(1, 'seat_left_work_ignores_cancel'), 200, 100),
            )
            await _until(lambda: 'RunStuckFrame' in _kinds(writer, 1))
            steps.mark.assert_not_awaited()
            RELEASE[0].set()
            await _until(lambda: len(writer.results()) == 1)

        writer = _on_seat_loop(1, scenario)

        assert exits == []
        assert _kinds(writer, 1) == [
            'DeadlineFiredFrame',
            'SecondCancelFrame',
            'RunStuckFrame',
            'SharedResultFrame',
        ]
        assert writer.results()[0].end == SeatTimedOut()
        steps.mark.assert_not_awaited()
