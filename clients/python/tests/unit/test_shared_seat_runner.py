"""One task run on a seat of a shared async child (``run_seat_task``), and
the finalizing marker that the seat loop asks for at its end
(``mark_seat_finalizing``).

Real registered tasks run here, from version-2 frames as the core decodes
them. The database steps, the RUNNING transition and the finalizing marker,
are replaced; their own tests are in ``test_shared_lifecycle``.
"""

# pyright: reportPrivateUsage=false

from __future__ import annotations

import asyncio
import json
import time
import uuid
from collections.abc import Awaitable, Callable, Iterator
from typing import Any, Final, NoReturn
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from pydantic import SecretStr

import horsies._core as core
from horsies.core.app import Horsies
from horsies.core.codec.json_io import loads_json
from horsies.core.codec.typed import decode_task_result
from horsies.core.models.app import AppConfig
from horsies.core.models.broker import PostgresConfig
from horsies.core.models.tasks import (
    MAX_CANCEL_GRACE_MS,
    MIN_CANCEL_GRACE_MS,
    MIN_TASK_TIMEOUT_MS,
    Execution,
    OperationalErrorCode,
    TaskError,
)
from horsies.core.models.outcome import Failed, Succeeded, TaskOutcome
from horsies.core.telemetry.attempt_span import BodyEndOrder, BodyFirst
from horsies.core.types.result import Ok, Result
from horsies.core.worker import child_main, child_runner, shared_child, shared_runner
from horsies.core.worker.child_inbox import ParentFrameInbox
from horsies.core.worker.child_frames import (
    ChildFrame,
    RssMb,
    SeatEnd,
    SeatTimedOut,
    SharedResultFrame,
)
from horsies import CancelCause, CancelRequested, RunCancelState, TaskRuntime
from horsies.core.worker.child_frames import CancelFiredFrame, SeatCancelled, StartReported
from horsies.core.worker.run_cancel import RunCancel
from horsies.core.worker.run_return import (
    CancelledBody,
    MarkerDone,
    MarkerEnd,
    ResultEnvelope,
)
from horsies.core.worker.shared_child import (
    FrameWithResult,
    SeatReturn,
    SetRunning,
    StoppedBeforeRunning,
)
from horsies.core.worker.stored_timeout import CancelGraceInvalid
from tests.fixtures.core_frames import (
    CLAIMED_AT,
    async_work,
    refusal,
    shared_cancel_frame,
    shared_run_frame,
    stop_frame,
)
from tests.fixtures.run_return import STARTED as STARTED_RUN
from tests.fixtures.run_return import STARTED_AT

EXECUTOR_THREADS = 4
"""The size of the default executor of a test child."""

pytestmark = [pytest.mark.unit]

TASK_ID: Final = uuid.UUID('0190f2d8-7c1e-7a3b-9f00-000000000051')
SCENARIO_TIMEOUT_S = 5.0
"""The longest wait of a test for an event of the code under test."""

DEADLINE_MS: Final = MIN_TASK_TIMEOUT_MS
"""The deadline of a run that a test lets pass: the smallest ``timeout_ms``
of a run frame. The core codec refuses a smaller one, as the stored
``task_options`` reader does."""


APP = Horsies(
    AppConfig(
        broker=PostgresConfig(
            database_url=SecretStr('postgresql://u:p@localhost/unused'),
        ),
    ),
)
RAN: list[str] = []


class _StartMark:
    """The start of a body in one test. An ``asyncio.Event`` binds to the
    loop of its first wait that blocks, and each test runs its own loop, so
    ``renew`` gives each test a new event."""

    def __init__(self) -> None:
        self._event = asyncio.Event()

    def renew(self) -> None:
        self._event = asyncio.Event()

    def set(self) -> None:
        self._event.set()

    async def wait(self) -> None:
        _ = await self._event.wait()


STARTED = _StartMark()


@APP.task(name='seat_double', execution=Execution.SHARED_ASYNC, timeout_ms=10_000)
async def seat_double(*, x: int) -> Result[int, TaskError]:
    await asyncio.sleep(0)
    return Ok(x * 2)


@APP.task(name='seat_sync')
def seat_sync() -> Result[int, TaskError]:
    RAN.append('seat_sync')
    return Ok(1)


@APP.task(name='seat_never', execution=Execution.SHARED_ASYNC, timeout_ms=1000)
async def seat_never() -> Result[int, TaskError]:
    RAN.append('seat_never')
    return Ok(1)


@APP.task(
    name='seat_core_stopped', execution=Execution.SHARED_ASYNC, timeout_ms=1000
)
async def seat_core_stopped() -> Result[int, TaskError]:
    """A core call of the body met the stopped core of the child."""
    raise core.CoreStopped('log forwarder', 'planned panic')


@APP.task(name='seat_raises', execution=Execution.SHARED_ASYNC, timeout_ms=1000)
async def seat_raises() -> Result[int, TaskError]:
    raise ValueError('bad input')


@APP.task(name='seat_waits', execution=Execution.SHARED_ASYNC, timeout_ms=1000)
async def seat_waits() -> Result[int, TaskError]:
    STARTED.set()
    await asyncio.Event().wait()
    return Ok(1)


@APP.task(name='seat_quick', execution=Execution.SHARED_ASYNC, timeout_ms=1000)
async def seat_quick() -> Result[int, TaskError]:
    return Ok(1)


@APP.task(
    name='seat_cancels_itself',
    execution=Execution.SHARED_ASYNC,
    timeout_ms=1000,
    exception_mapper={asyncio.CancelledError: 'CANCELLED_BY_BODY'},
)
async def seat_cancels_itself() -> Result[int, TaskError]:
    current = asyncio.current_task()
    assert current is not None
    current.cancel()
    await asyncio.sleep(0)
    return Ok(1)


@APP.task(
    name='seat_cancels_itself_and_returns',
    execution=Execution.SHARED_ASYNC,
    timeout_ms=1000,
    exception_mapper={asyncio.CancelledError: 'CANCELLED_BY_BODY'},
)
async def seat_cancels_itself_and_returns() -> Result[int, TaskError]:
    """Asks to cancel its own task, then returns before any ``await``: the
    request is still pending when the call returns."""
    current = asyncio.current_task()
    assert current is not None
    current.cancel()
    return Ok(1)


@APP.task(
    name='seat_raises_generator_exit',
    execution=Execution.SHARED_ASYNC,
    timeout_ms=1000,
    exception_mapper={GeneratorExit: 'GENERATOR_EXIT_OF_BODY'},
)
async def seat_raises_generator_exit() -> Result[int, TaskError]:
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
    name='seat_leaves_work', execution=Execution.SHARED_ASYNC, timeout_ms=10_000
)
async def seat_leaves_work() -> Result[int, TaskError]:
    """Returns while an asyncio task that it started still runs."""
    _ = asyncio.get_running_loop().create_task(_wait_for_release())
    return Ok(5)


@APP.task(
    name='seat_left_work_ignores_cancel',
    execution=Execution.SHARED_ASYNC,
    timeout_ms=10_000,
)
async def seat_left_work_ignores_cancel() -> Result[int, TaskError]:
    """Waits until its deadline cancel. An asyncio task that it started
    ignores every cancel until ``RELEASE`` is set."""
    _ = asyncio.get_running_loop().create_task(_ignore_cancels_until_release())
    await asyncio.Event().wait()
    return Ok(6)


CANCEL_STATES: list[RunCancelState] = []
"""The cancel state that ``seat_reads_cancel_state`` read after its cancel."""

STARTS: list[asyncio.Event] = []
"""The event that a slow start write of a test waits for."""


@APP.task(
    name='seat_reads_cancel_state', execution=Execution.SHARED_ASYNC, timeout_ms=10_000
)
async def seat_reads_cancel_state(runtime: TaskRuntime) -> Result[int, TaskError]:
    """Waits until it is cancelled, records its cancel state, and returns a
    value, which the operator cancel drops."""
    STARTED.set()
    try:
        await asyncio.Event().wait()
    except asyncio.CancelledError:
        CANCEL_STATES.append(runtime.cancel_state())
    return Ok(7)


@APP.task(
    name='seat_swallows_then_cancels_itself',
    execution=Execution.SHARED_ASYNC,
    timeout_ms=10_000,
    exception_mapper={asyncio.CancelledError: 'CANCELLED_BY_BODY'},
)
async def seat_swallows_then_cancels_itself() -> Result[int, TaskError]:
    """Swallows the cancel that reaches it, then asks to cancel its own task
    and returns before any ``await``: the requests are still pending when
    the call returns, and ``_without_left_cancel`` clears them."""
    STARTED.set()
    try:
        await asyncio.Event().wait()
    except asyncio.CancelledError:
        pass
    current = asyncio.current_task()
    assert current is not None
    current.cancel()
    return Ok(1)


def _frame(
    task_name: str,
    kwargs: dict[str, Any] | None = None,
    *,
    work: dict[str, object] | None = None,
) -> core.SharedRunFrame:
    return shared_run_frame(
        dispatch=1,
        task_id=TASK_ID,
        task_name=task_name,
        work=work if work is not None else async_work(60_000, 1_000),
        orphan_self_heal=True,
        input_json=json.dumps(kwargs or {}),
    )


def _frame_for(
    dispatch: int,
    task_name: str,
    kwargs: dict[str, Any] | None = None,
    *,
    timeout_ms: int = 60_000,
    grace_ms: int = 1_000,
) -> core.SharedRunFrame:
    return shared_run_frame(
        dispatch=dispatch,
        task_id=uuid.UUID(int=dispatch),
        task_name=task_name,
        work=async_work(timeout_ms, grace_ms),
        orphan_self_heal=True,
        input_json=json.dumps(kwargs or {}),
    )


def _envelope_of(end: SeatEnd) -> ResultEnvelope:
    assert isinstance(end, ResultEnvelope), end
    return end


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
        self.confirm = AsyncMock(return_value=STARTED_RUN)
        self.mark = AsyncMock(return_value=MarkerDone())


@pytest.fixture
def steps() -> Iterator[_Steps]:
    replaced = _Steps()
    with (
        patch.object(shared_runner, 'confirm_until_not_resumed', replaced.confirm),
        patch.object(shared_runner, 'mark_task_finalizing', replaced.mark),
    ):
        yield replaced


def _body_first() -> BodyEndOrder:
    """The order of a run whose steps end before its deadline."""
    return BodyFirst()


def _run(frame: core.SharedRunFrame) -> SeatReturn:
    with patch.object(child_runner, 'get_current_app', MagicMock(return_value=APP)):
        return asyncio.run(shared_runner.run_seat_task(frame, _body_first, RunCancel()))


def _set_running(seat_return: SeatReturn) -> ResultEnvelope:
    """The result envelope of a run that set its row RUNNING."""
    assert isinstance(seat_return, SetRunning), seat_return
    return seat_return.result


def _result(envelope: ResultEnvelope, ok_type: Any) -> TaskOutcome[Any, TaskError]:
    return decode_task_result(loads_json(envelope.text).unwrap(), ok_type)


def _failed(outcome: TaskOutcome[Any, TaskError]) -> TaskError:
    """The task error of a failed outcome; any other outcome fails the test."""
    match outcome:
        case Failed(error=error):
            return error
        case Succeeded():
            pytest.fail(f'expected a failed outcome, got {outcome!r}')


@pytest.fixture(autouse=True)
def fresh_marks() -> Iterator[None]:
    RAN.clear()
    STARTED.renew()
    RELEASE.clear()
    CANCEL_STATES.clear()
    STARTS.clear()
    yield


def test_an_async_task_runs_on_the_running_loop(steps: _Steps) -> None:
    """The run ends at the result: the seat loop writes the marker at the
    end of the run, not the run itself."""
    envelope = _set_running(_run(_frame('seat_double', {'x': 21})))

    assert _result(envelope, int) == Succeeded(42)
    steps.confirm.assert_awaited_once_with(
        str(TASK_ID),
        'worker-1',
        CLAIMED_AT,
        orphan_self_heal=True,
    )
    steps.mark.assert_not_awaited()


def test_a_def_task_is_refused_before_it_runs(steps: _Steps) -> None:
    envelope = _set_running(_run(_frame('seat_sync')))

    error = _failed(_result(envelope, int))
    assert error.error_code == OperationalErrorCode.WORKER_RESOLUTION_ERROR
    assert error.message == (
        'Failed to resolve seat_sync: the task is def, and a shared async '
        'child runs only async def tasks'
    )
    assert RAN == []


def test_a_refusal_fails_the_task_after_running_and_before_resolve(
    steps: _Steps,
) -> None:
    """The response of an isolated child for a refused stored timeout."""
    reason = 'stored cancel_grace_ms is below 100 ms'
    frame = _frame(
        'seat_double',
        {'x': 21},
        work=refusal('grace_invalid', reason),
    )

    envelope = _set_running(_run(frame))

    assert envelope == child_runner._stored_timeout_refusal_response(
        'seat_double',
        CancelGraceInvalid(reason=reason),
    ).envelope
    error = _failed(_result(envelope, int))
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
    steps.confirm.return_value = StoppedBeforeRunning('CLAIM_LOST')
    frame = _frame('seat_never', work=refusal('value_invalid', 'y'))

    assert _run(frame) == StoppedBeforeRunning('CLAIM_LOST')
    steps.mark.assert_not_awaited()


def test_a_stop_before_start_runs_no_body_and_reports_no_running_row(
    steps: _Steps,
) -> None:
    steps.confirm.return_value = StoppedBeforeRunning('CLAIM_LOST')

    assert _run(_frame('seat_never')) == StoppedBeforeRunning('CLAIM_LOST')
    assert RAN == []
    steps.mark.assert_not_awaited()


def test_a_task_that_raises_is_mapped(steps: _Steps) -> None:
    envelope = _set_running(_run(_frame('seat_raises')))

    error = _failed(_result(envelope, int))
    assert error.error_code == OperationalErrorCode.UNHANDLED_EXCEPTION


def test_a_body_that_the_stopped_core_ended_ends_the_child_with_no_report(
    steps: _Steps,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """The refusal leaves the body through the exception mapping of the
    task; the child ends before the run returns, so the worker takes the
    crashed-child path."""
    codes: list[int] = []

    def record_exit(code: int) -> NoReturn:
        codes.append(code)
        raise _Exited(code)

    monkeypatch.setattr(child_main.os, '_exit', record_exit)
    monkeypatch.setattr(child_main.logging, 'shutdown', lambda: None)

    with pytest.raises(_Exited):
        _run(_frame('seat_core_stopped'))

    assert codes == [child_main.CORE_STOPPED_EXIT_CODE]
    assert (
        f'During the body of task {TASK_ID}: the core of this child stopped after '
        'a panic in a core background task (log forwarder; planned panic)'
    ) in caplog.text
    steps.mark.assert_not_awaited()


def test_a_cancel_of_the_body_ends_the_run(steps: _Steps) -> None:
    """The seat loop turns a cancel of its deadline into the timeout; the run
    itself only has to end."""

    async def cancel_after_start() -> SeatReturn:
        run = asyncio.ensure_future(
            shared_runner.run_seat_task(_frame('seat_waits'), _body_first, RunCancel()),
        )
        await asyncio.wait_for(STARTED.wait(), SCENARIO_TIMEOUT_S)
        run.cancel()
        return await run

    with patch.object(child_runner, 'get_current_app', MagicMock(return_value=APP)):
        envelope = _set_running(asyncio.run(cancel_after_start()))

    error = _failed(_result(envelope, int))
    assert error.error_code == OperationalErrorCode.UNHANDLED_EXCEPTION
    assert 'CancelledError' in (error.message or '')


def test_a_body_that_cancels_its_own_task_goes_through_the_mapper(
    steps: _Steps,
) -> None:
    """No deadline fired: the cancel request is the body's own. The worker
    maps it through the exception mapper of the task."""
    envelope = _set_running(_run(_frame('seat_cancels_itself')))

    error = _failed(_result(envelope, int))
    assert error.error_code == 'CANCELLED_BY_BODY'


def test_a_generator_exit_of_the_body_goes_through_the_mapper(steps: _Steps) -> None:
    """The seat maps what ``await_task_call_in_worker`` maps: a
    ``GeneratorExit`` that leaves the body as well."""
    envelope = _set_running(_run(_frame('seat_raises_generator_exit')))

    error = _failed(_result(envelope, int))
    assert error.error_code == 'GENERATOR_EXIT_OF_BODY'


class TestTheMarkerOfASeat:
    """``mark_seat_finalizing``: the marker that the seat loop asks for at
    the end of a run, through the core handle of the child, fenced on the
    claim of the frame. Its bound is the core's
    (``test_shared_lifecycle``)."""

    def test_a_run_writes_the_marker_of_its_claim(self, steps: _Steps) -> None:
        asyncio.run(shared_runner.mark_seat_finalizing(_frame('seat_quick')))

        steps.mark.assert_awaited_once_with(TASK_ID, 'worker-1', CLAIMED_AT)

    def test_a_cancel_of_the_caller_propagates(self, steps: _Steps) -> None:
        marking = asyncio.Event()

        async def slow_marker(*_args: object) -> None:
            marking.set()
            await asyncio.Event().wait()

        steps.mark.side_effect = slow_marker

        async def cancel_in_marker() -> None:
            run = asyncio.ensure_future(
                shared_runner.mark_seat_finalizing(_frame('seat_quick')),
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
    envelope = _set_running(_run(_frame('seat_cancels_itself_and_returns')))

    error = _failed(_result(envelope, int))
    assert error.error_code == 'CANCELLED_BY_BODY'


class _Exited(BaseException):
    """Raised by the patched defect exit, as the real one ends the process."""


class _RealSeatRunner:
    """``StartedSeats`` that runs the real ``run_seat_task`` and the real
    ``mark_seat_finalizing``, over the patched steps."""

    async def run(
        self,
        frame: core.SharedRunFrame,
        body_end: Callable[[], BodyEndOrder],
        cancel: RunCancel,
    ) -> SeatReturn:
        return await shared_runner.run_seat_task(frame, body_end, cancel)

    async def mark_finalizing(self, frame: core.SharedRunFrame) -> MarkerEnd:
        return await shared_runner.mark_seat_finalizing(frame)


@pytest.fixture
def exits(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    """The defect exits of the seat loop, each recorded and raised."""
    seen: list[str] = []

    def record_exit(message: str, exit_code: int) -> NoReturn:
        seen.append(f'{exit_code}: {message}')
        raise _Exited(message)

    monkeypatch.setattr(shared_child, '_exit_for_defect', record_exit)
    return seen


type _Scenario = Callable[[ParentFrameInbox, _RecordingWriter], Awaitable[None]]


def _on_seat_loop(seats: int, scenario: _Scenario) -> _RecordingWriter:
    """Run ``scenario`` beside a real seat loop that serves with
    ``_RealSeatRunner``, then stop the loop. The scenario returns when every
    run it sent has ended."""
    writer = _RecordingWriter()
    inbox = ParentFrameInbox()

    async def serve() -> None:
        seat_loop = shared_child.SeatLoop(
            seats,
            EXECUTOR_THREADS,
            writer,  # type: ignore[arg-type]
            lambda: RssMb(1.0),
        )
        seat_loop.install_marks()
        serving = asyncio.ensure_future(seat_loop.serve(inbox, _RealSeatRunner()))
        try:
            await asyncio.wait_for(scenario(inbox, writer), SCENARIO_TIMEOUT_S)
        finally:
            # Work that ignores cancels ends here, also when the scenario
            # failed, so the loop can close.
            for release in RELEASE:
                release.set()
        inbox.put(stop_frame())
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
    assert _result(_envelope_of(ends[1]), int) == Succeeded(8)
    error = _failed(_result(_envelope_of(ends[2]), int))
    assert error.error_code == 'CANCELLED_BY_BODY'


class TestTheMarkerOnTheSeatLoop:
    """The finalizing marker is the last write of a run: at the end of the
    run, after its coroutine and its marked work, only for a run that ended
    without a cancel, before its deadline. It has the bound of the core
    handle, not the deadline of the run."""

    def test_a_marker_that_crosses_the_deadline_keeps_the_result_and_reports_after_it(
        self,
        steps: _Steps,
        exits: list[str],
    ) -> None:
        """The marker waits on the pool past the deadline of the run. The
        run returned in time, so its end is what it returned, and no
        deadline report goes out; the end report follows the marker."""

        async def waits_on_the_pool(*_args: object) -> MarkerEnd:
            await asyncio.sleep(1.5)
            return MarkerDone()

        steps.mark.side_effect = waits_on_the_pool
        took_s: list[float] = []

        async def scenario(inbox: ParentFrameInbox, writer: _RecordingWriter) -> None:
            sent = time.monotonic()
            inbox.put(
                _frame_for(1, 'seat_quick', timeout_ms=DEADLINE_MS, grace_ms=1000),
            )
            await _until(lambda: len(writer.results()) == 1)
            took_s.append(time.monotonic() - sent)

        writer = _on_seat_loop(1, scenario)

        assert exits == []
        assert _kinds(writer, 1) == ['SharedResultFrame']
        assert _result(_envelope_of(writer.results()[0].end), int) == Succeeded(1)
        steps.mark.assert_awaited_once()
        # The end report came when the marker ended (1.5 s), after the
        # deadline (1 s).
        assert took_s[0] >= 1.5

    def test_runs_that_timed_out_write_no_marker_in_their_grace(
        self,
        steps: _Steps,
        exits: list[str],
    ) -> None:
        """Every run reaches its deadline at once, and each lets the cancel
        end it. No marker waits on the pool inside the cancel grace, so no
        run needs a second cancel."""
        one_connection = asyncio.Semaphore(1)

        async def one_pooled_connection(*_args: object) -> MarkerEnd:
            async with one_connection:
                await asyncio.sleep(0.2)
            return MarkerDone()

        steps.mark.side_effect = one_pooled_connection
        seats = 8

        async def scenario(inbox: ParentFrameInbox, writer: _RecordingWriter) -> None:
            for dispatch in range(1, seats + 1):
                inbox.put(
                    _frame_for(
                        dispatch,
                        'seat_waits',
                        timeout_ms=DEADLINE_MS,
                        grace_ms=MIN_CANCEL_GRACE_MS,
                    ),
                )
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

        async def record(*_args: object) -> MarkerEnd:
            results_at_marker.append(len(writers[0].results()))
            return MarkerDone()

        steps.mark.side_effect = record

        async def scenario(inbox: ParentFrameInbox, writer: _RecordingWriter) -> None:
            writers.append(writer)
            RELEASE.append(asyncio.Event())
            inbox.put(
                _frame_for(1, 'seat_leaves_work', timeout_ms=5_000, grace_ms=2_000),
            )
            await asyncio.sleep(0.2)
            assert results_at_marker == [], 'the marker came before the work ended'
            assert writer.results() == []
            RELEASE[0].set()
            await _until(lambda: len(writer.results()) == 1)

        writer = _on_seat_loop(1, scenario)

        assert exits == []
        assert results_at_marker == [0]
        assert _result(_envelope_of(writer.results()[0].end), int) == Succeeded(5)

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
                _frame_for(
                    1,
                    'seat_left_work_ignores_cancel',
                    timeout_ms=DEADLINE_MS,
                    grace_ms=MIN_CANCEL_GRACE_MS,
                ),
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


class TestAnOperatorCancelOnTheSeatLoop:
    """``CancelRunFrame`` on the real seat loop and the real seat runner: the
    cancel reaches the body as ``CancelledError``, the runtime reads
    ``OPERATOR``, and the run ends cancelled with no finalizing marker."""

    def test_the_cancel_reaches_the_body_and_the_run_ends_cancelled(
        self,
        steps: _Steps,
        exits: list[str],
    ) -> None:
        async def scenario(inbox: ParentFrameInbox, writer: _RecordingWriter) -> None:
            inbox.put(_frame_for(1, 'seat_reads_cancel_state'))
            await STARTED.wait()
            inbox.put(shared_cancel_frame(dispatch=1, task_id=uuid.UUID(int=1)))
            await _until(lambda: len(writer.results()) == 1)

        writer = _on_seat_loop(1, scenario)

        assert exits == []
        assert CANCEL_STATES == [CancelRequested(cause=CancelCause.OPERATOR)]
        assert writer.frames[0] == CancelFiredFrame(dispatch=1, task_id=uuid.UUID(int=1))
        assert _kinds(writer, 1) == ['CancelFiredFrame', 'SharedResultFrame']
        [result] = writer.results()
        assert result.end == SeatCancelled(body=CancelledBody.RAN)
        assert result.started == StartReported(STARTED_AT)
        steps.mark.assert_not_awaited()

    def test_an_operator_cancel_is_kept_through_uncancel(
        self,
        steps: _Steps,
        exits: list[str],
    ) -> None:
        """The body swallows the operator cancel, then cancels its own task
        and returns: ``_without_left_cancel`` clears both requests of the
        seat task, and the run still ends as the operator cancel."""

        async def scenario(inbox: ParentFrameInbox, writer: _RecordingWriter) -> None:
            inbox.put(_frame_for(1, 'seat_swallows_then_cancels_itself'))
            await STARTED.wait()
            inbox.put(shared_cancel_frame(dispatch=1, task_id=uuid.UUID(int=1)))
            await _until(lambda: len(writer.results()) == 1)

        writer = _on_seat_loop(1, scenario)

        assert exits == []
        assert writer.results()[0].end == SeatCancelled(body=CancelledBody.RAN)

    def test_a_cancel_during_the_start_write_skips_the_body(
        self,
        steps: _Steps,
        exits: list[str],
    ) -> None:
        """The start write is in flight when the cancel comes: the seat task
        is not cancelled, and the body does not start once the start write
        returns."""

        async def slow_start(*_args: object, **_kwargs: object) -> object:
            STARTED.set()
            await STARTS[0].wait()
            return STARTED_RUN

        steps.confirm.side_effect = slow_start

        async def scenario(inbox: ParentFrameInbox, writer: _RecordingWriter) -> None:
            STARTS.append(asyncio.Event())
            inbox.put(_frame_for(1, 'seat_double', {'x': 2}))
            await STARTED.wait()
            inbox.put(shared_cancel_frame(dispatch=1, task_id=uuid.UUID(int=1)))
            await asyncio.sleep(0.05)
            assert writer.frames == []
            STARTS[0].set()
            await _until(lambda: len(writer.results()) == 1)

        writer = _on_seat_loop(1, scenario)

        assert exits == []
        assert _kinds(writer, 1) == ['CancelFiredFrame', 'SharedResultFrame']
        [result] = writer.results()
        assert result.end == SeatCancelled(body=CancelledBody.NOT_STARTED)
        assert result.started == StartReported(STARTED_AT)
        steps.mark.assert_not_awaited()

    def test_a_cancel_after_the_deadline_is_ignored(
        self,
        steps: _Steps,
        exits: list[str],
    ) -> None:
        async def scenario(inbox: ParentFrameInbox, writer: _RecordingWriter) -> None:
            inbox.put(_frame_for(1, 'seat_waits', timeout_ms=DEADLINE_MS))
            await _until(lambda: 'DeadlineFiredFrame' in _kinds(writer, 1))
            inbox.put(shared_cancel_frame(dispatch=1, task_id=uuid.UUID(int=1)))
            await _until(lambda: len(writer.results()) == 1)

        writer = _on_seat_loop(1, scenario)

        assert exits == []
        assert 'CancelFiredFrame' not in _kinds(writer, 1)
        assert writer.results()[0].end == SeatTimedOut()

    def test_a_cancel_of_a_run_that_the_child_does_not_run_is_ignored(
        self,
        steps: _Steps,
        exits: list[str],
    ) -> None:
        """An unknown dispatch, and a known dispatch with another task: the
        end of a run and its cancel can cross. The run in flight is not
        touched."""

        async def scenario(inbox: ParentFrameInbox, writer: _RecordingWriter) -> None:
            inbox.put(_frame_for(1, 'seat_waits'))
            await STARTED.wait()
            inbox.put(shared_cancel_frame(dispatch=9, task_id=uuid.UUID(int=9)))
            inbox.put(shared_cancel_frame(dispatch=1, task_id=uuid.UUID(int=2)))
            await asyncio.sleep(0.05)
            assert writer.frames == []
            inbox.put(shared_cancel_frame(dispatch=1, task_id=uuid.UUID(int=1)))
            await _until(lambda: len(writer.results()) == 1)

        writer = _on_seat_loop(1, scenario)

        assert exits == []
        assert _kinds(writer, 1) == ['CancelFiredFrame', 'SharedResultFrame']

    def test_the_neighbour_of_a_cancelled_run_keeps_its_result(
        self,
        steps: _Steps,
        exits: list[str],
    ) -> None:
        async def scenario(inbox: ParentFrameInbox, writer: _RecordingWriter) -> None:
            inbox.put(_frame_for(1, 'seat_waits'))
            await STARTED.wait()
            inbox.put(shared_cancel_frame(dispatch=1, task_id=uuid.UUID(int=1)))
            inbox.put(_frame_for(2, 'seat_double', {'x': 3}))
            await _until(lambda: len(writer.results()) == 2)

        writer = _on_seat_loop(2, scenario)

        assert exits == []
        ends = {frame.dispatch: frame.end for frame in writer.results()}
        assert ends[1] == SeatCancelled(body=CancelledBody.RAN)
        assert _result(_envelope_of(ends[2]), int) == Succeeded(6)
        assert 'CancelFiredFrame' not in _kinds(writer, 2)
