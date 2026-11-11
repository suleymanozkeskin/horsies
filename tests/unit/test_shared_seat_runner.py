"""One task run on a seat of a shared async child (``run_seat_task``).

Real registered tasks run here. The database steps around the body, the
RUNNING transition and the finalizing marker, are replaced; their own tests
are in ``test_shared_lifecycle``.
"""

# pyright: reportPrivateUsage=false

from __future__ import annotations

import asyncio
import dataclasses
import json
import time
from collections.abc import Iterator
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
    SharedResultFrame,
    SharedRunFrame,
    StopFrame,
    WireResult,
)
from horsies.core.worker.shared_child import FrameWithResult
from horsies.core.worker.stored_timeout import CancelGraceInvalid, TimeoutValueInvalid

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


def _run(frame: SharedRunFrame) -> WireResult:
    with patch.object(child_runner, 'get_current_app', MagicMock(return_value=APP)):
        return asyncio.run(shared_runner.run_seat_task(MagicMock(), frame))


def _result(wire: WireResult, ok_type: Any) -> TaskResult[Any, TaskError]:
    ok, payload, _reason = wire
    assert ok is True
    return decode_task_result(loads_json(payload).unwrap(), ok_type)


@pytest.fixture(autouse=True)
def fresh_marks() -> Iterator[None]:
    RAN.clear()
    STARTED.clear()
    yield


def test_an_async_task_runs_on_the_running_loop(steps: _Steps) -> None:
    wire = _run(_frame('seat_double', {'x': 21}))

    assert _result(wire, int).unwrap() == 42
    assert wire[2] is None
    steps.confirm.assert_awaited_once()
    steps.mark.assert_awaited_once()


def test_a_def_task_is_refused_before_it_runs(steps: _Steps) -> None:
    wire = _run(_frame('seat_sync'))

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

    wire = _run(frame)

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
    steps.mark.assert_awaited_once()


def test_a_refusal_after_a_stop_before_start_returns_the_stop(steps: _Steps) -> None:
    steps.confirm.return_value = (False, '', 'CLAIM_LOST')
    frame = dataclasses.replace(
        _frame('seat_never'),
        work=SeatRefusal(refusal=TimeoutValueInvalid(reason='y')),
    )

    assert _run(frame) == (False, '', 'CLAIM_LOST')
    steps.mark.assert_not_awaited()


def test_a_stop_before_start_runs_no_body_and_writes_no_marker(steps: _Steps) -> None:
    steps.confirm.return_value = (False, '', 'CLAIM_LOST')

    assert _run(_frame('seat_never')) == (False, '', 'CLAIM_LOST')
    assert RAN == []
    steps.mark.assert_not_awaited()


def test_a_task_that_raises_is_mapped_and_writes_the_marker(steps: _Steps) -> None:
    wire = _run(_frame('seat_raises'))

    error = _result(wire, int).unwrap_err()
    assert error.error_code == OperationalErrorCode.UNHANDLED_EXCEPTION
    steps.mark.assert_awaited_once()


def test_a_cancel_of_the_body_ends_the_run_and_writes_the_marker(
    steps: _Steps,
) -> None:
    """The seat loop turns a cancel of its deadline into the timeout; the run
    itself only has to end, with the marker written."""

    async def cancel_after_start() -> WireResult:
        run = asyncio.ensure_future(
            shared_runner.run_seat_task(MagicMock(), _frame('seat_waits')),
        )
        await STARTED.wait()
        run.cancel()
        return await run

    with patch.object(child_runner, 'get_current_app', MagicMock(return_value=APP)):
        wire = asyncio.run(cancel_after_start())

    error = _result(wire, int).unwrap_err()
    assert error.error_code == OperationalErrorCode.UNHANDLED_EXCEPTION
    assert 'CancelledError' in (error.message or '')
    steps.mark.assert_awaited_once()


def test_a_body_that_cancels_its_own_task_goes_through_the_mapper(
    steps: _Steps,
) -> None:
    """No deadline fired: the cancel request is the body's own. The worker
    maps it through the exception mapper of the task."""
    wire = _run(_frame('seat_cancels_itself'))

    error = _result(wire, int).unwrap_err()
    assert error.error_code == 'CANCELLED_BY_BODY'
    steps.mark.assert_awaited_once()


def test_a_cancel_during_the_marker_propagates(steps: _Steps) -> None:
    marking = asyncio.Event()

    async def slow_marker(*_args: object) -> None:
        marking.set()
        await asyncio.Event().wait()

    steps.mark.side_effect = slow_marker

    async def cancel_in_marker() -> None:
        run = asyncio.ensure_future(
            shared_runner.run_seat_task(MagicMock(), _frame('seat_quick')),
        )
        await marking.wait()
        run.cancel()
        with pytest.raises(asyncio.CancelledError):
            await run

    with patch.object(child_runner, 'get_current_app', MagicMock(return_value=APP)):
        asyncio.run(cancel_in_marker())


def test_a_body_that_returns_after_cancelling_its_own_task_goes_through_the_mapper(
    steps: _Steps,
) -> None:
    """The cancel request is still pending when the body returns. The run
    ends as a cancel of the body, through its mapper, and the seat task
    does not end cancelled."""
    wire = _run(_frame('seat_cancels_itself_and_returns'))

    error = _result(wire, int).unwrap_err()
    assert error.error_code == 'CANCELLED_BY_BODY'


class _Exited(BaseException):
    """Raised by the patched defect exit, as the real one ends the process."""


class _RealSeatRunner:
    """``StartedSeats`` that runs the real ``run_seat_task``."""

    async def run(self, frame: SharedRunFrame) -> WireResult:
        return await shared_runner.run_seat_task(MagicMock(), frame)

    async def stop(self) -> None:
        return None


def test_a_pending_self_cancel_on_the_seat_loop_ends_only_that_run(
    steps: _Steps,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """On the real seat loop, a body that leaves a cancel request on its
    seat task and returns ends as its mapped cancel. The child does not exit
    as for a defect, and the neighbour run ends with its own result."""
    exits: list[str] = []

    def record_exit(message: str, exit_code: int) -> NoReturn:
        exits.append(f'{exit_code}: {message}')
        raise _Exited(message)

    monkeypatch.setattr(shared_child, '_exit_for_defect', record_exit)
    writer = _RecordingWriter()
    inbox = ParentFrameInbox()

    async def scenario() -> None:
        seat_loop = shared_child.SeatLoop(2, writer, lambda: RssMb(1.0))  # type: ignore[arg-type]
        seat_loop.install_marks()
        serving = asyncio.ensure_future(seat_loop.serve(inbox, _RealSeatRunner()))
        inbox.put(_frame_for(1, 'seat_double', {'x': 4}))
        inbox.put(_frame_for(2, 'seat_cancels_itself_and_returns'))
        deadline = time.monotonic() + 10.0
        while len(writer.results()) < 2 and not exits:
            assert time.monotonic() < deadline, 'the runs did not end in time'
            await asyncio.sleep(0.001)
        inbox.put(StopFrame())
        await asyncio.wait_for(serving, 10.0)

    with patch.object(child_runner, 'get_current_app', MagicMock(return_value=APP)):
        asyncio.run(scenario())

    assert exits == []
    ends = {frame.dispatch: frame.end for frame in writer.results()}
    neighbour = ends[1]
    assert isinstance(neighbour, SeatFinished)
    assert _result(_wire_of(neighbour), int).unwrap() == 8
    cancelled = ends[2]
    assert isinstance(cancelled, SeatFinished)
    error = _result(_wire_of(cancelled), int).unwrap_err()
    assert error.error_code == 'CANCELLED_BY_BODY'
