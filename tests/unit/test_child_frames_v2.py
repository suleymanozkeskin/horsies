"""The version-2 child on the core frame codec: the frames that it sends
(``child_frames``), its frame reader and the checks of its init frame
(``child_main``), the work of a run frame (``child_runner``), and the core
handle of a child from its init frame."""

# pyright: reportPrivateUsage=false

from __future__ import annotations

import json
import struct
import uuid
from typing import Final

import pytest

from horsies import _core as core
from horsies.core.models.boot_failure import AppLoadFailed, ResourceShortage, ShortageErrno
from horsies.core.worker import child_main
from horsies.core.worker.child_frames import (
    MAX_LEFTOVER_TASK_NAMES,
    BlockingRun,
    BodyReturnedFrame,
    ChildForkedFrame,
    CulpritUnknown,
    LeftWorkEnd,
    LeftWorkEndedFrame,
    LoopBlockedFrame,
    LoopHoldsTasks,
    NoTaskLoop,
    PreloadRefusedFrame,
    ReadyFrame,
    ResultFrame,
    RssMb,
    RssUnreadable,
    RunStuckFrame,
    SeatTimedOut,
    SharedResultFrame,
    boot_failed_frame,
    encode_child_frame,
    encode_preload_frame,
    stored_refusal_of,
)
from horsies.core.worker.child_main import ParentFrameReader, python_program_of, read_init_frame
from horsies.core.worker.child_pool import pool_of
from horsies.core.worker.child_runner import RunTheTask, isolated_work_of
from horsies.core.worker.run_return import ResultEnvelope, StoppedBeforeRunning
from horsies.core.worker.stored_timeout import (
    CancelGraceInvalid,
    OptionsUnreadable,
    TimeoutValueInvalid,
)
from tests.fixtures.core_frames import (
    framed,
    init_frame,
    init_meta,
    refusal,
    run_frame,
    run_meta,
    shared_async_kind,
    shared_thread_kind,
)

pytestmark = [pytest.mark.unit]

_HEADER: Final = struct.Struct('>IQ')
TASK_ID: Final = uuid.UUID('0190f2d8-7c1e-7a3b-9f00-000000000001')


def _meta_and_body(encoded: bytes | core.FrameTooLarge) -> tuple[dict[str, object], bytes]:
    assert isinstance(encoded, bytes), encoded
    meta_bytes, body_bytes = _HEADER.unpack_from(encoded)
    meta = json.loads(encoded[_HEADER.size : _HEADER.size + meta_bytes])
    body = encoded[_HEADER.size + meta_bytes :]
    assert len(body) == body_bytes
    return meta, body


class TestChildFrames:
    def test_the_ready_frame_names_the_protocol(self) -> None:
        meta, _ = _meta_and_body(encode_child_frame(ReadyFrame(pid=42, rss=RssMb(30.5))))
        assert meta == {
            'kind': 'ready',
            'protocol': core.protocol_numbers().protocol_version,
            'pid': 42,
            'rss_mb': 30.5,
        }

    def test_a_result_carries_its_envelope_as_the_body(self) -> None:
        envelope = '{"__h_task_result__":true,"ok":3,"err":null}'
        meta, body = _meta_and_body(
            encode_child_frame(
                ResultFrame(
                    task_id=TASK_ID,
                    end=ResultEnvelope(envelope),
                    rss=RssUnreadable(),
                    task_loop=NoTaskLoop(),
                ),
            ),
        )
        assert meta['kind'] == 'result'
        assert meta['end'] == {'kind': 'result'}
        assert meta['rss_mb'] is None
        assert meta['task_loop'] == {'kind': 'none'}
        assert body.decode('utf-8') == envelope

    @pytest.mark.parametrize(
        'reason',
        [
            'CLAIM_LOST',
            'OWNERSHIP_UNCONFIRMED',
            'WORKFLOW_STOPPED',
            'TASK_EXPIRED',
            'NODE_NOT_LINKED',
            'ORPHAN_CANCELLED',
        ],
    )
    def test_a_stop_before_running_names_its_reason(self, reason: str) -> None:
        stopped = StoppedBeforeRunning(reason)  # pyright: ignore[reportArgumentType]
        meta, body = _meta_and_body(
            encode_child_frame(
                ResultFrame(task_id=TASK_ID, end=stopped, rss=RssMb(1.0), task_loop=NoTaskLoop()),
            ),
        )
        assert meta['end'] == {'kind': 'stopped_before_running', 'reason': reason}
        assert body == b''

    def test_a_timed_out_seat_has_no_body(self) -> None:
        meta, body = _meta_and_body(
            encode_child_frame(
                SharedResultFrame(dispatch=3, task_id=TASK_ID, end=SeatTimedOut(), rss=RssMb(1.0)),
            ),
        )
        assert meta['kind'] == 'shared_result'
        assert meta['end'] == {'kind': 'timed_out'}
        assert body == b''

    def test_left_work_reports_name_their_tasks(self) -> None:
        left = LoopHoldsTasks(count=12, names=tuple(f'job_{n}' for n in range(MAX_LEFTOVER_TASK_NAMES)))
        meta, body = _meta_and_body(
            encode_child_frame(
                BodyReturnedFrame(task_id=TASK_ID, returned=ResultEnvelope('{}'), left=left),
            ),
        )
        assert meta['kind'] == 'body_returned'
        assert body == b'{}'
        meta, _ = _meta_and_body(
            encode_child_frame(
                LeftWorkEndedFrame(task_id=TASK_ID, end=LeftWorkEnd.CANCELLED, rss=RssMb(2.0)),
            ),
        )
        assert meta['end'] == 'cancelled'

    def test_a_report_whose_names_do_not_fit_its_count_is_a_defect(self) -> None:
        with pytest.raises(ValueError):
            encode_child_frame(
                RunStuckFrame(dispatch=1, task_id=TASK_ID, tasks=12, executor_items=0, names=('a',)),
            )

    def test_a_boot_failure_names_its_cause(self) -> None:
        meta, _ = _meta_and_body(
            encode_child_frame(
                boot_failed_frame(ResourceShortage(ShortageErrno.EMFILE), 'OSError', 'too many'),
            ),
        )
        assert meta['cause'] == {'kind': 'resource_shortage', 'errno': 'EMFILE'}
        meta, _ = _meta_and_body(
            encode_child_frame(boot_failed_frame(AppLoadFailed(), 'ImportError', 'no module')),
        )
        assert meta['cause'] == {'kind': 'app_load_failed'}

    def test_a_blocked_loop_names_its_culprit(self) -> None:
        meta, _ = _meta_and_body(
            encode_child_frame(
                LoopBlockedFrame(
                    culprit=BlockingRun(dispatch=4, task_id=TASK_ID),
                    blocked_ms=5012,
                    where='app.py:12 in spin',
                ),
            ),
        )
        assert meta['culprit'] == {'kind': 'run', 'dispatch': 4, 'task_id': str(TASK_ID)}
        meta, _ = _meta_and_body(
            encode_child_frame(
                LoopBlockedFrame(culprit=CulpritUnknown(), blocked_ms=1, where=''),
            ),
        )
        assert meta['culprit'] == {'kind': 'unknown'}

    def test_the_preload_frames_have_no_body(self) -> None:
        meta, body = _meta_and_body(encode_preload_frame(ChildForkedFrame(pid=7)))
        assert meta == {'kind': 'child_forked', 'pid': 7}
        assert body == b''
        meta, _ = _meta_and_body(encode_preload_frame(PreloadRefusedFrame(reason='2 OS threads')))
        assert meta == {'kind': 'preload_refused', 'reason': '2 OS threads'}


class TestRunWork:
    def test_a_run_frame_gives_the_run(self) -> None:
        assert isolated_work_of(run_frame(task_id=TASK_ID).work) == RunTheTask()

    @pytest.mark.parametrize(
        ('kind', 'expected'),
        [
            ('options_unreadable', OptionsUnreadable(reason='r')),
            ('value_invalid', TimeoutValueInvalid(reason='r')),
            ('grace_invalid', CancelGraceInvalid(reason='r')),
        ],
    )
    def test_a_refusal_gives_its_stored_options_refusal(
        self,
        kind: str,
        expected: object,
    ) -> None:
        frame = run_frame(task_id=TASK_ID, work=refusal(kind, 'r'))  # pyright: ignore[reportArgumentType]
        match frame.work:
            case core.StoredOptionsRefused() as refused:
                assert stored_refusal_of(refused) == expected
                assert isolated_work_of(refused) == expected
            case 'RUN':
                pytest.fail('a refusal decodes as a refusal')

    def test_the_run_frame_carries_the_orphan_self_heal_and_the_input(self) -> None:
        frame = run_frame(task_id=TASK_ID, orphan_self_heal=True, input_json='{"a":1}')
        assert frame.orphan_self_heal is True
        assert frame.input == '{"a":1}'
        assert frame.task_id == TASK_ID


class _ScriptedSocket:
    """Serves ``data`` in reads of at most ``chunk`` bytes, then the end of
    the stream."""

    def __init__(self, data: bytes, chunk: int) -> None:
        self._data = data
        self._at = 0
        self._chunk = chunk

    def recv(self, size: int) -> bytes:
        count = min(size, self._chunk, len(self._data) - self._at)
        got = self._data[self._at : self._at + count]
        self._at += count
        return got

    def settimeout(self, _timeout: float | None) -> None:
        return None


class _Exited(Exception):
    def __init__(self, code: int) -> None:
        super().__init__(code)
        self.code = code


def _raise_exit(code: int) -> None:
    raise _Exited(code)


class TestFrameReader:
    @pytest.mark.parametrize('chunk', [1, 7, 1 << 16])
    def test_frames_come_whole_and_in_order_at_any_read_size(self, chunk: int) -> None:
        data = (
            framed(init_meta())
            + framed(run_meta(task_id=TASK_ID), b'{"x":1}')
            + framed({'kind': 'stop'})
        )
        reader = ParentFrameReader(_ScriptedSocket(data, chunk))  # pyright: ignore[reportArgumentType]
        assert isinstance(reader.next_frame(), core.InitFrame)
        run = reader.next_frame()
        assert isinstance(run, core.RunFrame)
        assert run.input == '{"x":1}'
        assert isinstance(reader.next_frame(), core.StopFrame)
        with pytest.raises(EOFError):
            reader.next_frame()

    def test_an_init_frame_of_another_version_exits_with_the_mismatch_code(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setattr(child_main.os, '_exit', _raise_exit)
        reader = ParentFrameReader(
            _ScriptedSocket(framed(init_meta(protocol=1)), 1 << 16),  # pyright: ignore[reportArgumentType]
        )
        with pytest.raises(_Exited) as exited:
            reader.next_frame()
        assert exited.value.code == child_main.PROTOCOL_MISMATCH_EXIT_CODE
        assert exited.value.code == core.protocol_numbers().protocol_mismatch_exit_code

    def test_a_malformed_frame_exits_with_the_bad_frame_code(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setattr(child_main.os, '_exit', _raise_exit)
        reader = ParentFrameReader(
            _ScriptedSocket(framed({'kind': 'dance'}), 1 << 16),  # pyright: ignore[reportArgumentType]
        )
        with pytest.raises(_Exited) as exited:
            reader.next_frame()
        assert exited.value.code == child_main.BAD_PARENT_FRAME_EXIT_CODE

    def test_the_first_frame_must_be_the_init_frame_of_a_python_child(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setattr(child_main.os, '_exit', _raise_exit)
        rust_init = init_meta()
        rust_init['program'] = {'kind': 'rust'}
        for data in (framed({'kind': 'stop'}), framed(rust_init)):
            sock = _ScriptedSocket(data, 1 << 16)
            with pytest.raises(_Exited) as exited:
                read_init_frame(sock, ParentFrameReader(sock))  # pyright: ignore[reportArgumentType]
            assert exited.value.code == child_main.BAD_PARENT_FRAME_EXIT_CODE

    def test_the_init_frame_gives_the_program_and_the_pool_of_the_child_kind(self) -> None:
        init = init_frame(child_kind=shared_async_kind(seats=4))
        assert python_program_of(init).app_locator == 'app.main:app'
        match init.child_kind:
            case core.SharedAsyncChildKind() as kind:
                assert kind.seats == 4
                assert pool_of(kind).max_size == 2
            case _:
                pytest.fail('a shared async kind decodes as one')
        assert isinstance(init_frame(child_kind=shared_thread_kind()).child_kind, core.SharedThreadChildKind)


class TestChildHandle:
    def test_the_handle_of_a_child_comes_from_its_init_frame(self) -> None:
        handle = core.CoreChildHandle.from_init(init_frame())
        assert isinstance(handle, core.CoreChildHandle)
