"""The operator cancel of a run in a worker child (``run_cancel``): the
phases of a run, the run of an isolated child that the control thread
cancels, the routing of cancel frames in the inbox, and isolated runs of real
tasks that an operator cancels, in process.

The database step before the body (the RUNNING update) is replaced; the
cancel reaches the body from another thread, as the control thread sends it.
"""

# pyright: reportPrivateUsage=false

from __future__ import annotations

import asyncio
import json
import struct
import threading
import uuid
from collections.abc import Iterator
from datetime import datetime, timezone
from typing import Final
from unittest.mock import MagicMock, patch

import pytest
from pydantic import SecretStr

from horsies import _core as core
from horsies import (
    CancelCause,
    CancelNotRequested,
    CancelRequested,
    RunCancelState,
    TaskRuntime,
)
from horsies.core.app import Horsies
from horsies.core.models.app import AppConfig
from horsies.core.models.broker import PostgresConfig
from horsies.core.models.tasks import TaskError
from horsies.core.types.result import Ok, Result
from horsies.core.worker import child_main, child_runner
from horsies.core.worker.child_frames import (
    CancelFiredFrame,
    ChildFrame,
    LoopHoldsNoTask,
    LoopHoldsTasks,
    NoTraceCarrier,
    RssMb,
    RunCancelledFrame,
    SeatCancelled,
    SharedResultFrame,
    StartNotReported,
    StartReported,
    encode_child_frame,
)
from horsies.core.worker.child_inbox import NoFrameYet, ParentFrameInbox
from horsies.core.worker.child_runner import RunTheTask
from horsies.core.worker.run_cancel import (
    BodyPhase,
    CancelAfterBody,
    CancelAfterEnd,
    CancelEarlierCause,
    CancelKept,
    CancelToBody,
    EndCancelled,
    EndNotCancelled,
    IsolatedRuns,
    NotThisRun,
    RunCancel,
    SkipTheBody,
    StartTheBody,
)
from horsies.core.worker.run_return import (
    CancelledBody,
    NotInWorkflow,
    RunCancelled,
    RunReturn,
    SetRunning,
    StartedRunning,
    StoppedBeforeRunning,
)
from horsies.core.worker.task_loop import TaskLoop
from tests.fixtures.core_frames import isolated_cancel_frame, shared_cancel_frame

pytestmark = [pytest.mark.unit]

TASK_ID: Final = uuid.UUID('0190f2d8-7c1e-7a3b-9f00-0000000000c1')
STARTED_AT: Final = datetime(2026, 11, 24, 9, 0, 1, tzinfo=timezone.utc)
STARTED: Final = StartedRunning(STARTED_AT, NotInWorkflow())
WAIT_S: Final = 10.0

APP = Horsies(
    AppConfig(
        broker=PostgresConfig(
            database_url=SecretStr('postgresql://user:password@localhost/unused'),
        ),
    ),
)

BODY_STARTED = threading.Event()
"""Set by each body of this module when it runs."""

SEEN: list[RunCancelState] = []
"""The cancel state that each body of this module read after its cancel."""


@APP.task('cancel.sync_polls')
def sync_polls(runtime: TaskRuntime) -> Result[str, TaskError]:
    """Runs until its cancel state turns, then returns a value, which the
    cancel drops."""
    BODY_STARTED.set()
    for _ in range(int(WAIT_S / 0.005)):
        match runtime.cancel_state():
            case CancelRequested() as requested:
                SEEN.append(requested)
                return Ok('stopped early')
            case CancelNotRequested():
                threading.Event().wait(0.005)
    return Ok('never cancelled')


@APP.task('cancel.async_waits')
async def async_waits(runtime: TaskRuntime) -> Result[str, TaskError]:
    """Waits until the cancel reaches it at its ``await``."""
    BODY_STARTED.set()
    try:
        await asyncio.Event().wait()
    except asyncio.CancelledError:
        SEEN.append(runtime.cancel_state())
        raise
    return Ok('never cancelled')


@APP.task('cancel.quick')
def quick() -> Result[int, TaskError]:
    BODY_STARTED.set()
    return Ok(1)


@pytest.fixture(autouse=True)
def fresh_marks() -> Iterator[None]:
    BODY_STARTED.clear()
    SEEN.clear()
    yield


@pytest.fixture
def task_loop() -> Iterator[TaskLoop]:
    """The loop of an isolated child whose app has an async task."""
    loop = TaskLoop()
    loop.open().unwrap()
    yield loop
    _ = loop.close()


def _run(
    task_name: str,
    task_loop: TaskLoop,
    cancel: RunCancel,
    started: StartedRunning | StoppedBeforeRunning = STARTED,
) -> RunReturn:
    with (
        patch.object(
            child_runner,
            '_confirm_ownership_and_set_running',
            MagicMock(return_value=started),
        ),
        patch.object(child_runner, 'get_current_app', MagicMock(return_value=APP)),
    ):
        return child_runner.run_isolated_task(
            task_name=task_name,
            input_json='{}',
            task_id=TASK_ID,
            master_worker_id='worker',
            work=RunTheTask(),
            claimed_at=STARTED_AT,
            task_loop=task_loop,
            orphan_self_heal=True,
            queue_name='default',
            attempt=1,
            trace=NoTraceCarrier(),
            cancel=cancel,
        )


def _cancel_once_started(runs: IsolatedRuns) -> threading.Thread:
    """The control thread: once the body runs, cancel the run of
    ``TASK_ID``."""

    def control() -> None:
        assert BODY_STARTED.wait(WAIT_S)
        _ = runs.request(TASK_ID)

    thread = threading.Thread(target=control, name='horsies-test-control')
    thread.start()
    return thread


_HEADER: Final = struct.Struct('>IQ')


def _meta(frame: ChildFrame) -> dict[str, object]:
    """The metadata that the core codec wrote for ``frame``."""
    encoded = encode_child_frame(frame)
    assert isinstance(encoded, bytes), encoded
    meta_bytes, body_bytes = _HEADER.unpack_from(encoded)
    assert body_bytes == 0
    meta = json.loads(encoded[_HEADER.size : _HEADER.size + meta_bytes])
    assert isinstance(meta, dict)
    return meta


class TestTheFramesOfACancel:
    def test_a_cancel_run_frame_names_its_run(self) -> None:
        isolated = isolated_cancel_frame(task_id=TASK_ID)
        shared = shared_cancel_frame(dispatch=4, task_id=TASK_ID)

        assert isinstance(isolated.target, core.IsolatedCancelTarget)
        assert isolated.target.task_id == TASK_ID
        assert isinstance(shared.target, core.SharedCancelTarget)
        assert (shared.target.dispatch, shared.target.task_id) == (4, TASK_ID)

    def test_the_cancel_report_of_a_shared_run(self) -> None:
        assert _meta(CancelFiredFrame(dispatch=4, task_id=TASK_ID)) == {
            'kind': 'cancel_fired',
            'dispatch': 4,
            'task_id': str(TASK_ID),
        }

    @pytest.mark.parametrize(
        ('body', 'wire'),
        [(CancelledBody.RAN, 'ran'), (CancelledBody.NOT_STARTED, 'not_started')],
    )
    def test_the_cancelled_end_of_a_shared_run(self, body: CancelledBody, wire: str) -> None:
        meta = _meta(
            SharedResultFrame(
                dispatch=4,
                task_id=TASK_ID,
                end=SeatCancelled(body=body),
                started=StartReported(STARTED_AT),
                rss=RssMb(2.0),
            ),
        )

        assert meta['end'] == {'kind': 'cancelled', 'body': wire}
        assert meta['child'] == 'serves_on'

    def test_a_cancelled_end_with_no_start_is_a_defect_of_the_child(self) -> None:
        with pytest.raises(ValueError):
            _ = encode_child_frame(
                SharedResultFrame(
                    dispatch=4,
                    task_id=TASK_ID,
                    end=SeatCancelled(body=CancelledBody.RAN),
                    started=StartNotReported(),
                    rss=RssMb(2.0),
                ),
            )

    def test_the_end_of_a_cancelled_isolated_run(self) -> None:
        empty = _meta(
            RunCancelledFrame(
                task_id=TASK_ID,
                body=CancelledBody.RAN,
                started_at=STARTED_AT,
                rss=RssMb(3.0),
                task_loop=LoopHoldsNoTask(),
            ),
        )
        held = _meta(
            RunCancelledFrame(
                task_id=TASK_ID,
                body=CancelledBody.NOT_STARTED,
                started_at=STARTED_AT,
                rss=RssMb(3.0),
                task_loop=LoopHoldsTasks(count=2, names=('a', 'b')),
            ),
        )

        assert empty['kind'] == 'run_cancelled'
        assert empty['body'] == 'ran'
        assert empty['task_loop'] == {'kind': 'empty'}
        assert empty['child'] == 'serves_on'
        assert held['body'] == 'not_started'
        assert held['task_loop'] == {'kind': 'held', 'count': 2, 'names': ['a', 'b']}


class TestThePhasesOfARun:
    def test_a_cancel_before_the_body_is_kept_and_skips_the_body(self) -> None:
        cancel = RunCancel()

        assert cancel.request_operator() == CancelKept()
        assert cancel.begin_body() == SkipTheBody()
        assert cancel.phase() is BodyPhase.SKIPPED
        assert cancel.fix_end() == EndCancelled(body=CancelledBody.NOT_STARTED)

    def test_a_cancel_while_the_body_runs_goes_to_the_body(self) -> None:
        cancel = RunCancel()
        assert cancel.begin_body() == StartTheBody()

        assert cancel.request_operator() == CancelToBody()
        cancel.end_body()
        assert cancel.fix_end() == EndCancelled(body=CancelledBody.RAN)

    def test_a_cancel_after_the_body_ends_the_run_cancelled(self) -> None:
        cancel = RunCancel()
        _ = cancel.begin_body()
        cancel.end_body()

        assert cancel.request_operator() == CancelAfterBody()
        assert cancel.fix_end() == EndCancelled(body=CancelledBody.RAN)

    def test_a_cancel_after_the_end_is_ignored(self) -> None:
        cancel = RunCancel()
        _ = cancel.begin_body()
        cancel.end_body()
        assert cancel.fix_end() == EndNotCancelled()

        assert cancel.request_operator() == CancelAfterEnd()
        assert cancel.cell.state() == CancelNotRequested()

    def test_a_deadline_first_keeps_the_body_and_ignores_the_operator(self) -> None:
        cancel = RunCancel()
        _ = cancel.request_deadline()

        assert cancel.begin_body() == StartTheBody()
        assert cancel.request_operator() == CancelEarlierCause(cause=CancelCause.DEADLINE)
        cancel.end_body()
        assert cancel.fix_end() == EndNotCancelled()

    def test_a_kept_cancel_is_dropped_when_the_start_ends_the_run(self) -> None:
        cancel = RunCancel()
        _ = cancel.request_operator()

        assert cancel.fix_end() == EndNotCancelled()

    def test_a_second_fix_of_the_end_is_a_defect(self) -> None:
        cancel = RunCancel()
        _ = cancel.fix_end()

        with pytest.raises(AssertionError):
            _ = cancel.fix_end()


class TestIsolatedRuns:
    def test_a_cancel_reaches_the_run_of_its_task(self) -> None:
        runs = IsolatedRuns()
        cancel = runs.begin(TASK_ID)

        assert runs.request(TASK_ID) == CancelKept()
        assert cancel.cell.state() == CancelRequested(cause=CancelCause.OPERATOR)

    def test_a_cancel_of_another_task_or_with_no_run_is_ignored(self) -> None:
        runs = IsolatedRuns()

        assert runs.request(TASK_ID) == NotThisRun()
        cancel = runs.begin(TASK_ID)
        assert runs.request(uuid.UUID(int=1)) == NotThisRun()
        runs.end()
        assert runs.request(TASK_ID) == NotThisRun()
        assert cancel.cell.state() == CancelNotRequested()

    def test_a_second_run_while_one_runs_is_a_defect(self) -> None:
        runs = IsolatedRuns()
        _ = runs.begin(TASK_ID)

        with pytest.raises(AssertionError):
            _ = runs.begin(uuid.UUID(int=1))


class TestTheInbox:
    def test_a_cancel_frame_goes_in_the_queue_with_no_route(self) -> None:
        inbox = ParentFrameInbox()
        frame = shared_cancel_frame(dispatch=1, task_id=TASK_ID)

        inbox.put_cancel(frame)

        assert inbox.take() is frame

    def test_a_route_takes_each_cancel_frame_on_the_calling_thread(self) -> None:
        inbox = ParentFrameInbox()
        routed: list[str] = []
        inbox.route_cancels(lambda _frame: routed.append(threading.current_thread().name))

        inbox.put_cancel(isolated_cancel_frame(task_id=TASK_ID))

        assert routed == [threading.current_thread().name]
        assert inbox.take() == NoFrameYet()

    def test_the_control_thread_gives_the_cancel_to_the_isolated_run(self) -> None:
        runs = IsolatedRuns()
        cancel = runs.begin(TASK_ID)

        child_main._cancel_isolated_run(runs, isolated_cancel_frame(task_id=TASK_ID))

        assert cancel.cell.state() == CancelRequested(cause=CancelCause.OPERATOR)

    def test_a_shared_target_on_an_isolated_child_is_a_parent_defect(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        exits: list[int] = []

        def record(code: int) -> None:
            exits.append(code)
            raise SystemExit(code)

        monkeypatch.setattr(child_main.os, '_exit', record)
        with pytest.raises(SystemExit):
            child_main._cancel_isolated_run(
                IsolatedRuns(),
                shared_cancel_frame(dispatch=1, task_id=TASK_ID),
            )

        assert exits == [child_main.BAD_PARENT_FRAME_EXIT_CODE]


class TestAnIsolatedRunThatAnOperatorCancels:
    def test_a_def_body_reads_the_cancel_and_the_run_ends_cancelled(
        self,
        task_loop: TaskLoop,
    ) -> None:
        runs = IsolatedRuns()
        cancel = runs.begin(TASK_ID)
        control = _cancel_once_started(runs)

        returned = _run('cancel.sync_polls', task_loop, cancel)
        control.join(WAIT_S)

        assert SEEN == [CancelRequested(cause=CancelCause.OPERATOR)]
        assert returned == RunCancelled(body=CancelledBody.RAN, started_at=STARTED_AT)

    def test_an_async_def_body_gets_the_cancel_at_its_await(
        self,
        task_loop: TaskLoop,
    ) -> None:
        runs = IsolatedRuns()
        cancel = runs.begin(TASK_ID)
        control = _cancel_once_started(runs)

        returned = _run('cancel.async_waits', task_loop, cancel)
        control.join(WAIT_S)

        assert SEEN == [CancelRequested(cause=CancelCause.OPERATOR)]
        assert returned == RunCancelled(body=CancelledBody.RAN, started_at=STARTED_AT)

    def test_a_cancel_before_the_body_skips_it(self, task_loop: TaskLoop) -> None:
        runs = IsolatedRuns()
        cancel = runs.begin(TASK_ID)
        assert runs.request(TASK_ID) == CancelKept()

        returned = _run('cancel.quick', task_loop, cancel)

        assert not BODY_STARTED.is_set()
        assert returned == RunCancelled(
            body=CancelledBody.NOT_STARTED,
            started_at=STARTED_AT,
        )

    def test_a_start_that_stops_the_run_drops_a_kept_cancel(
        self,
        task_loop: TaskLoop,
    ) -> None:
        runs = IsolatedRuns()
        cancel = runs.begin(TASK_ID)
        _ = runs.request(TASK_ID)

        returned = _run('cancel.quick', task_loop, cancel, StoppedBeforeRunning('CLAIM_LOST'))

        assert returned == StoppedBeforeRunning('CLAIM_LOST')

    def test_a_run_with_no_cancel_returns_what_the_body_returned(
        self,
        task_loop: TaskLoop,
    ) -> None:
        cancel = IsolatedRuns().begin(TASK_ID)

        returned = _run('cancel.quick', task_loop, cancel)

        assert isinstance(returned, SetRunning), returned
        assert cancel.request_operator() == CancelAfterEnd()
