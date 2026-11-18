"""A worker child whose core stopped ends itself.

Two refusals of the core do not clear in the process: ``CoreStopped`` (a
panic in a core background task) and ``ForkedAfterCoreStart`` (this process
is a fork of the child).

- At the start of a run, before user code: ``CoreStopped`` gives
  ``CoreStoppedBeforeStart``; the child reports it as
  ``OWNERSHIP_UNCONFIRMED`` (the worker releases the claim) and then ends
  with ``CORE_STOPPED_EXIT_CODE``. A fork ends at once and writes no frame.
- In the body: a body that the refusal ended (its task error holds the
  refusal) ends the child at once with no report, so the worker takes the
  crashed-child path.
- After a body that finished: the finalizing marker gives
  ``MarkerMetStoppedCore``; the child reports the result of the body
  (``CoreStoppedAfterBody``) and ends once that frame is written. A fork
  ends at once and writes no frame.
- Every other ``CoreError`` keeps its mapping.

The core handle, the socket and ``os._exit`` are replaced.
"""

# pyright: reportPrivateUsage=false

from __future__ import annotations

import asyncio
import json
import struct
import uuid
from datetime import datetime, timezone
from typing import Any, Final, NoReturn
from unittest.mock import MagicMock, patch

import pytest

import horsies._core as core
from horsies.core.models.tasks import TaskError, TaskResult
from horsies.core.worker import child_main, child_runner, shared_child, shared_lifecycle
from horsies.core.worker.child_frames import (
    RssUnreadable,
    SeatTimedOut,
    SharedResultFrame,
)
from horsies.core.worker.run_return import (
    BodyInterrupted,
    CoreStoppedAfterBody,
    CoreStoppedBeforeStart,
    MarkerDone,
    MarkerMetStoppedCore,
    ResultEnvelope,
    SetRunning,
    StoppedBeforeRunning,
)
from horsies.core.worker.task_loop import TaskLoop
from tests.fixtures.core_frames import run_frame

pytestmark = [pytest.mark.unit]

TASK_ID: Final = '0190f2d8-7c1e-7a3b-9f00-0000000000c1'
TASK_UUID: Final = uuid.UUID(TASK_ID)
CLAIMED_AT: Final = datetime(2026, 11, 18, 10, 0, tzinfo=timezone.utc)
ENVELOPE: Final = '{"__h_task_result__":true,"ok":"kept","err":null}'
_HEADER: Final = struct.Struct('>IQ')
WRITER_JOIN_S: Final = 5.0


class _Exited(Exception):
    def __init__(self, code: int) -> None:
        super().__init__(code)
        self.code = code


def _raise_exit(code: int) -> NoReturn:
    raise _Exited(code)


@pytest.fixture
def exits(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    """``os._exit`` raises ``_Exited``; ``logging.shutdown`` only records
    its call, so the handlers of the test stay open."""
    calls: list[str] = []
    monkeypatch.setattr(child_main.os, '_exit', _raise_exit)
    monkeypatch.setattr(
        child_main.logging, 'shutdown', lambda: calls.append('logging.shutdown')
    )
    return calls


def _stopped() -> core.CoreStopped:
    return core.CoreStopped('log forwarder', 'planned panic')


def _forked() -> core.ForkedAfterCoreStart:
    return core.ForkedAfterCoreStart(100, 101, 'start children with spawn')


def _record(record_class: type, **fields: object) -> MagicMock:
    """A stand-in for a record of ``horsies._core``: a class pattern matches it."""
    record = MagicMock(spec=record_class)
    record.task_id = TASK_UUID
    for name, value in fields.items():
        setattr(record, name, value)
    return record


def _start(handle: Any) -> Any:
    with patch.object(child_runner, '_get_child_core_handle', return_value=handle):
        return child_runner._confirm_until_not_resumed(
            TASK_ID,
            'worker-A',
            CLAIMED_AT,
            orphan_self_heal=True,
        )


def _start_async(handle: Any) -> Any:
    with patch.object(shared_lifecycle, '_get_child_core_handle', return_value=handle):
        return asyncio.run(
            shared_lifecycle.confirm_until_not_resumed(
                TASK_ID,
                'worker-A',
                CLAIMED_AT,
                orphan_self_heal=True,
            ),
        )


def _last_message(caplog: pytest.LogCaptureFixture) -> str:
    return caplog.records[-1].getMessage()


class TestTheStartOfAnIsolatedOrThreadRun:
    def test_a_stopped_core_is_a_core_stop_before_start(self) -> None:
        stopped = _stopped()
        handle = MagicMock()
        handle.start_attempt.side_effect = stopped

        assert _start(handle) == CoreStoppedBeforeStart(stopped)
        assert handle.start_attempt.call_count == 1

    def test_a_stopped_core_in_the_miss_path_is_a_core_stop_before_start(self) -> None:
        stopped = _stopped()
        handle = MagicMock()
        handle.start_attempt.return_value = _record(
            core.AttemptMissed, reason='WORKFLOW_PAUSED'
        )
        handle.stop_workflow_task_before_start.side_effect = stopped

        assert _start(handle) == CoreStoppedBeforeStart(stopped)

    def test_a_fork_ends_at_once_with_the_core_stopped_code(
        self,
        exits: list[str],
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        handle = MagicMock()
        handle.start_attempt.side_effect = _forked()

        with pytest.raises(_Exited) as exited:
            _start(handle)

        assert exited.value.code == child_main.CORE_STOPPED_EXIT_CODE
        assert exits == ['logging.shutdown']
        assert _last_message(caplog).startswith(
            f'At the start of task {TASK_ID}: this process is a fork of a child '
            'whose core had started (100; 101; start children with spawn)'
        )

    def test_another_core_error_stays_ownership_unconfirmed(self) -> None:
        handle = MagicMock()
        handle.start_attempt.side_effect = core.TooManyPendingCalls('the queue is full')

        assert _start(handle) == StoppedBeforeRunning('OWNERSHIP_UNCONFIRMED')


class TestTheStartOfASharedAsyncRun:
    def test_a_stopped_core_is_a_core_stop_before_start(self) -> None:
        stopped = _stopped()
        handle = MagicMock()
        handle.start_attempt_async.side_effect = stopped

        assert _start_async(handle) == CoreStoppedBeforeStart(stopped)

    def test_a_fork_ends_at_once_with_the_core_stopped_code(
        self,
        exits: list[str],
    ) -> None:
        handle = MagicMock()
        handle.start_attempt_async.side_effect = _forked()

        with pytest.raises(_Exited) as exited:
            _start_async(handle)

        assert exited.value.code == child_main.CORE_STOPPED_EXIT_CODE

    def test_another_core_error_stays_ownership_unconfirmed(self) -> None:
        handle = MagicMock()
        handle.start_attempt_async.side_effect = core.TooManyPendingCalls(
            'the queue is full'
        )

        assert _start_async(handle) == StoppedBeforeRunning('OWNERSHIP_UNCONFIRMED')


class TestTheMarkerAfterABody:
    def test_a_stopped_core_is_a_marker_that_met_the_stopped_core(
        self,
        exits: list[str],
    ) -> None:
        """The body finished: the child reports its result first, so the
        marker does not end the child."""
        stopped = _stopped()
        handle = MagicMock()
        handle.mark_finalizing.side_effect = stopped

        with patch.object(child_runner, '_get_child_core_handle', return_value=handle):
            marker = child_runner.mark_run_finalizing(TASK_UUID, 'worker-A', CLAIMED_AT)

        assert marker == MarkerMetStoppedCore(stopped)
        assert exits == []

    def test_a_fork_ends_at_once_with_no_report(
        self,
        exits: list[str],
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        handle = MagicMock()
        handle.mark_finalizing.side_effect = _forked()

        with patch.object(child_runner, '_get_child_core_handle', return_value=handle):
            with pytest.raises(_Exited) as exited:
                child_runner.mark_run_finalizing(TASK_UUID, 'worker-A', CLAIMED_AT)

        assert exited.value.code == child_main.CORE_STOPPED_EXIT_CODE
        assert _last_message(caplog).startswith(
            f'After the body of task {TASK_UUID}: this process is a fork of a child '
            'whose core had started'
        )

    def test_another_core_error_is_logged_only(
        self,
        exits: list[str],
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        handle = MagicMock()
        handle.mark_finalizing.side_effect = core.TooManyPendingCalls('the queue is full')

        with patch.object(child_runner, '_get_child_core_handle', return_value=handle):
            marker = child_runner.mark_run_finalizing(TASK_UUID, 'worker-A', CLAIMED_AT)

        assert marker == MarkerDone()
        assert exits == []
        assert f'Failed to mark task {TASK_UUID} finalizing' in caplog.text

    def test_a_stopped_core_is_a_marker_that_met_the_stopped_core_in_a_shared_async_child(
        self,
        exits: list[str],
    ) -> None:
        stopped = _stopped()
        handle = MagicMock()
        handle.mark_finalizing_async.side_effect = stopped

        with patch.object(shared_lifecycle, '_get_child_core_handle', return_value=handle):
            marker = asyncio.run(
                shared_lifecycle.mark_task_finalizing(TASK_UUID, 'worker-A', CLAIMED_AT),
            )

        assert marker == MarkerMetStoppedCore(stopped)
        assert exits == []

    def test_a_fork_ends_a_shared_async_child_at_once(self, exits: list[str]) -> None:
        handle = MagicMock()
        handle.mark_finalizing_async.side_effect = _forked()

        with patch.object(shared_lifecycle, '_get_child_core_handle', return_value=handle):
            with pytest.raises(_Exited) as exited:
                asyncio.run(
                    shared_lifecycle.mark_task_finalizing(
                        TASK_UUID, 'worker-A', CLAIMED_AT
                    ),
                )

        assert exited.value.code == child_main.CORE_STOPPED_EXIT_CODE


def _error_with(exception: BaseException | None) -> TaskResult[Any, TaskError]:
    return TaskResult(err=TaskError(error_code='MAPPED_BY_THE_TASK', exception=exception))


def _ok(value: str) -> TaskResult[Any, TaskError]:
    return TaskResult(ok=value)


class TestTheEndOfABody:
    @pytest.mark.parametrize(
        'refusal',
        [pytest.param(_stopped(), id='stopped'), pytest.param(_forked(), id='forked')],
    )
    def test_a_task_error_that_holds_a_refusal_is_an_interrupted_body(
        self,
        refusal: core.CoreStopped | core.ForkedAfterCoreStart,
    ) -> None:
        with patch.object(child_runner, '_encode_task_outcome') as encode:
            ended = child_runner.encoded_body_end('task', object(), _error_with(refusal))

        assert ended == BodyInterrupted(refusal)
        encode.assert_not_called()

    @pytest.mark.parametrize(
        'out',
        [
            pytest.param(_ok('kept'), id='ok'),
            pytest.param(_error_with(RuntimeError('bad input')), id='other-exception'),
            pytest.param(_error_with(None), id='no-exception'),
            pytest.param(
                _error_with(core.TooManyPendingCalls('the queue is full')),
                id='other-core-error',
            ),
        ],
    )
    def test_every_other_outcome_is_encoded(self, out: TaskResult[Any, TaskError]) -> None:
        task = object()
        with patch.object(
            child_runner,
            '_encode_task_outcome',
            return_value=ResultEnvelope(ENVELOPE),
        ) as encode:
            ended = child_runner.encoded_body_end('task', task, out)

        assert ended == ResultEnvelope(ENVELOPE)
        encode.assert_called_once_with('task', task, out)

    def test_an_interrupted_body_ends_the_child_with_no_report(
        self,
        exits: list[str],
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        with pytest.raises(_Exited) as exited:
            child_runner.set_running_or_end_child(TASK_ID, BodyInterrupted(_stopped()))

        assert exited.value.code == child_main.CORE_STOPPED_EXIT_CODE
        assert exits == ['logging.shutdown']
        assert _last_message(caplog).startswith(
            f'During the body of task {TASK_ID}: the core of this child stopped '
            'after a panic in a core background task (log forwarder; planned panic)'
        )

    def test_a_result_sets_running(self, exits: list[str]) -> None:
        assert child_runner.set_running_or_end_child(
            TASK_ID, ResultEnvelope(ENVELOPE)
        ) == SetRunning(ResultEnvelope(ENVELOPE))
        assert exits == []


class _RecordingSocket:
    """Records the metadata and the body of each frame that the child writes."""

    def __init__(self, metas: list[dict[str, Any]], bodies: list[str] | None = None) -> None:
        self._metas = metas
        self._bodies = bodies

    def sendall(self, data: bytes) -> None:
        meta_bytes, body_bytes = _HEADER.unpack_from(data)
        self._metas.append(json.loads(data[_HEADER.size : _HEADER.size + meta_bytes]))
        if self._bodies is not None:
            start = _HEADER.size + meta_bytes
            self._bodies.append(data[start : start + body_bytes].decode('utf-8'))


class TestTheReportOfAnIsolatedRun:
    def test_the_release_is_written_before_the_child_ends(
        self,
        exits: list[str],
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """The worker gets ``OWNERSHIP_UNCONFIRMED`` and releases the claim;
        then the child ends, with the cause in its last log line. A run that
        stopped before running writes no finalizing marker."""
        metas: list[dict[str, Any]] = []
        markers: list[object] = []
        frame = run_frame(task_id=TASK_UUID)

        def mark_finalizing(marked: object) -> MarkerDone:
            markers.append(marked)
            return MarkerDone()

        with pytest.raises(_Exited) as exited:
            child_main._report_run(
                _RecordingSocket(metas),  # pyright: ignore[reportArgumentType]
                frame,
                CoreStoppedBeforeStart(_stopped()),
                TaskLoop(),
                mark_finalizing,
            )

        assert exited.value.code == child_main.CORE_STOPPED_EXIT_CODE
        assert markers == []
        assert [meta['kind'] for meta in metas] == ['result']
        assert metas[0]['end'] == {
            'kind': 'stopped_before_running',
            'reason': 'OWNERSHIP_UNCONFIRMED',
        }
        assert _last_message(caplog).startswith(
            f'At the start of task {TASK_UUID}: the core of this child stopped'
        )

    def test_the_result_of_a_finished_body_is_written_before_the_child_ends(
        self,
        exits: list[str],
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        """The marker met the stopped core after the body finished: the
        worker gets the result of the body and writes it; then the child
        ends, with the cause in its last log line."""
        metas: list[dict[str, Any]] = []
        bodies: list[str] = []
        markers: list[object] = []
        frame = run_frame(task_id=TASK_UUID)

        def mark_finalizing(marked: object) -> MarkerMetStoppedCore:
            markers.append(marked)
            return MarkerMetStoppedCore(_stopped())

        with pytest.raises(_Exited) as exited:
            child_main._report_run(
                _RecordingSocket(metas, bodies),  # pyright: ignore[reportArgumentType]
                frame,
                SetRunning(ResultEnvelope(ENVELOPE)),
                TaskLoop(),
                mark_finalizing,
            )

        assert exited.value.code == child_main.CORE_STOPPED_EXIT_CODE
        assert markers == [frame]
        assert [meta['kind'] for meta in metas] == ['result']
        assert metas[0]['end'] == {'kind': 'result'}
        assert bodies == [ENVELOPE]
        assert _last_message(caplog).startswith(
            f'After the body of task {TASK_UUID}: the core of this child stopped'
        )


def _shared_result(end: Any) -> SharedResultFrame:
    return SharedResultFrame(dispatch=7, task_id=TASK_UUID, end=end, rss=RssUnreadable())


class TestTheEndAfterAWrittenFrame:
    def test_a_core_stop_before_start_ends_the_child(self, exits: list[str]) -> None:
        with pytest.raises(_Exited) as exited:
            shared_child._end_after_written(
                _shared_result(CoreStoppedBeforeStart(_stopped())),
            )

        assert exited.value.code == child_main.CORE_STOPPED_EXIT_CODE

    def test_a_core_stop_after_a_body_ends_the_child(
        self,
        exits: list[str],
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        with pytest.raises(_Exited) as exited:
            shared_child._end_after_written(
                _shared_result(
                    CoreStoppedAfterBody(result=ResultEnvelope(ENVELOPE), stopped=_stopped()),
                ),
            )

        assert exited.value.code == child_main.CORE_STOPPED_EXIT_CODE
        assert _last_message(caplog).startswith(
            f'After the body of task {TASK_UUID}: the core of this child stopped'
        )

    @pytest.mark.parametrize(
        'end',
        [
            pytest.param(ResultEnvelope(ENVELOPE), id='result'),
            pytest.param(StoppedBeforeRunning('OWNERSHIP_UNCONFIRMED'), id='stopped'),
            pytest.param(SeatTimedOut(), id='timed-out'),
        ],
    )
    def test_every_other_end_returns(self, exits: list[str], end: Any) -> None:
        shared_child._end_after_written(_shared_result(end))

        assert exits == []

    @pytest.mark.parametrize(
        'end',
        [
            pytest.param(CoreStoppedBeforeStart(_stopped()), id='release'),
            pytest.param(
                CoreStoppedAfterBody(result=ResultEnvelope(ENVELOPE), stopped=_stopped()),
                id='result',
            ),
        ],
    )
    def test_the_writer_thread_writes_the_frame_then_ends_the_child(
        self,
        monkeypatch: pytest.MonkeyPatch,
        end: CoreStoppedBeforeStart | CoreStoppedAfterBody,
    ) -> None:
        """The writer owns the order: the frame leaves first."""
        events: list[str] = []

        def recording_exit(code: int) -> NoReturn:
            events.append(f'exit {code}')
            raise SystemExit(code)

        monkeypatch.setattr(child_main.os, '_exit', recording_exit)
        monkeypatch.setattr(child_main.logging, 'shutdown', lambda: None)
        writer = shared_child.FrameWriter(
            lambda frame: events.append(type(frame).__name__),
            capacity=4,
        )
        writer.start().unwrap()
        writer.send_result('task', _shared_result(end))
        writer._thread.join(WRITER_JOIN_S)

        assert not writer._thread.is_alive()
        assert events == [
            'SharedResultFrame',
            f'exit {child_main.CORE_STOPPED_EXIT_CODE}',
        ]


class TestTheFrameOfACoreStop:
    def test_a_stop_before_start_goes_out_as_ownership_unconfirmed(self) -> None:
        encoded = child_main.encode_child_frame(
            _shared_result(CoreStoppedBeforeStart(_stopped())),
        )
        assert isinstance(encoded, bytes)
        meta_bytes, _body_bytes = _HEADER.unpack_from(encoded)
        meta = json.loads(encoded[_HEADER.size : _HEADER.size + meta_bytes])

        assert meta['end'] == {
            'kind': 'stopped_before_running',
            'reason': 'OWNERSHIP_UNCONFIRMED',
        }

    def test_a_stop_after_a_body_goes_out_with_the_result_of_the_body(self) -> None:
        encoded = child_main.encode_child_frame(
            _shared_result(
                CoreStoppedAfterBody(result=ResultEnvelope(ENVELOPE), stopped=_stopped()),
            ),
        )
        assert isinstance(encoded, bytes)
        meta_bytes, body_bytes = _HEADER.unpack_from(encoded)
        meta = json.loads(encoded[_HEADER.size : _HEADER.size + meta_bytes])
        start = _HEADER.size + meta_bytes

        assert meta['end'] == {'kind': 'result'}
        assert encoded[start : start + body_bytes].decode('utf-8') == ENVELOPE


def test_the_exit_code_is_the_core_s() -> None:
    assert child_main.CORE_STOPPED_EXIT_CODE == core.protocol_numbers().core_stopped_exit_code
    assert child_main.CORE_STOPPED_EXIT_CODE == 80
