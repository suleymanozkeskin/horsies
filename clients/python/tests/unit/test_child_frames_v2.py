"""The version-3 child on the core frame codec: the frames that it sends
(``child_frames``), its frame reader and the checks of its init frame
(``child_main``), the checks of each parent frame field by the core decoder
(``core.decode_parent_frame``), the work of a run frame (``child_runner``), the
cause of a failed boot (``child_main``), and the core handle of a child from
its init frame."""

# pyright: reportPrivateUsage=false

from __future__ import annotations

import asyncio
import errno
import json
import socket
import struct
import threading
import uuid
from typing import Final

import pytest

from horsies import _core as core
from horsies.core.models.boot_failure import (
    CLEARABLE_ERRNOS,
    AppLoadFailed,
    ChildPoolSetupFailed,
    ResourceShortage,
    ShortageErrno,
    TaskLoopCreateFailed,
    ThreadStartRefused,
    ThreadStartRefusedError,
)
from horsies.core.models.tasks import (
    MAX_CANCEL_GRACE_MS,
    MAX_TASK_TIMEOUT_MS,
    MIN_CANCEL_GRACE_MS,
    MIN_TASK_TIMEOUT_MS,
)
from horsies.core.models.worker_profile import (
    MAX_CHILD_POOL_CONNECTIONS,
    MAX_EXECUTOR_THREADS,
    MAX_SEATS_PER_SHARED_ASYNC_CHILD,
    MAX_SEATS_PER_SHARED_THREAD_CHILD,
)
from horsies.core.types.result import Err, Ok
from horsies.core.worker import child_main
from horsies.core.worker.child_frames import (
    MAX_BOOT_DETAIL_CHARS,
    MAX_BOOT_ERROR_NAME_CHARS,
    MAX_EVENT_HANDLERS,
    MAX_HANDLER_NAME_CHARS,
    MAX_LEFTOVER_TASK_NAMES,
    BlockingRun,
    BodyReturnedFrame,
    BootFailedFrame,
    ChildForkedFrame,
    CulpritUnknown,
    HandlerCounts,
    HandlerStatsFrame,
    LeftoverCancelFrame,
    LeftoverStuckFrame,
    LeftWorkEnd,
    LeftWorkEndedFrame,
    LoopBlockedFrame,
    LoopHoldsTasks,
    NoTaskLoop,
    NoTraceCarrier,
    PreloadRefusedFrame,
    ReadyFrame,
    ResultFrame,
    RssMb,
    RssUnreadable,
    RunStuckFrame,
    SeatTimedOut,
    SharedResultFrame,
    StartNotReported,
    StartReported,
    StoredTraceCarrier,
    boot_failed_frame,
    encode_child_frame,
    encode_preload_frame,
    run_trace_of,
    stored_refusal_of,
)
from horsies.core.worker.child_inbox import ParentFrameInbox
from horsies.core.worker.child_main import ParentFrameReader, python_program_of, read_init_frame
from horsies.core.worker.child_runner import RunTheTask, isolated_work_of
from horsies.core.worker.run_return import ResultEnvelope, StoppedBeforeRunning
from horsies.core.worker.stored_timeout import (
    CancelGraceInvalid,
    OptionsUnreadable,
    TimeoutValueInvalid,
)
from horsies.core.worker.task_loop import AsyncTaskPresence
from tests.fixtures.core_frames import (
    TRACEPARENT,
    async_work,
    carrier_trace,
    events_frame,
    framed,
    handler_kind,
    init_frame,
    init_meta,
    pool,
    refusal,
    run_frame,
    run_meta,
    shared_async_kind,
    shared_run_frame,
    shared_run_meta,
    shared_thread_kind,
    thread_work,
)
from tests.fixtures.run_return import STARTED_AT

pytestmark = [pytest.mark.unit]

_HEADER: Final = struct.Struct('>IQ')
TASK_ID: Final = uuid.UUID('0190f2d8-7c1e-7a3b-9f00-000000000001')

# Two event wire forms of ``core/tests/fixtures/event_wire/vectors.json``.
CLAIM_RELEASED: Final = (
    '["ClaimReleased",1700000000123456789,"0192f6a1-7b3c-7d4e-8f00-0123456789ab",'
    '"send_email","default","QUEUE_FULL"]'
)
SLOT_RECOVERED: Final = '["SlotRecovered",1700000000123456789,"SHARED_ASYNC",1]'
STARTED_TEXT: Final = '2026-11-17T10:00:01.250Z'
"""``STARTED_AT`` as the core writes it."""


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
                    started=StartReported(STARTED_AT),
                    rss=RssUnreadable(),
                    task_loop=NoTaskLoop(),
                ),
            ),
        )
        assert meta['kind'] == 'result'
        assert meta['end'] == {'kind': 'result'}
        assert meta['started_at'] == {'kind': 'reported', 'time': '2026-11-17T10:00:01.250Z'}
        assert meta['rss_mb'] is None
        assert meta['task_loop'] == {'kind': 'none'}
        assert meta['child'] == 'serves_on'
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
                ResultFrame(
                    task_id=TASK_ID,
                    end=stopped,
                    started=StartNotReported(),
                    rss=RssMb(1.0),
                    task_loop=NoTaskLoop(),
                ),
            ),
        )
        assert meta['end'] == {'kind': 'stopped_before_running', 'reason': reason}
        assert body == b''

    def test_a_timed_out_seat_has_no_body(self) -> None:
        meta, body = _meta_and_body(
            encode_child_frame(
                SharedResultFrame(
                    dispatch=3,
                    task_id=TASK_ID,
                    end=SeatTimedOut(),
                    started=StartNotReported(),
                    rss=RssMb(1.0),
                ),
            ),
        )
        assert meta['kind'] == 'shared_result'
        assert meta['end'] == {'kind': 'timed_out'}
        assert meta['child'] == 'serves_on'
        assert body == b''

    def test_left_work_reports_name_their_tasks(self) -> None:
        left = LoopHoldsTasks(count=12, names=tuple(f'job_{n}' for n in range(MAX_LEFTOVER_TASK_NAMES)))
        meta, body = _meta_and_body(
            encode_child_frame(
                BodyReturnedFrame(
                    task_id=TASK_ID,
                    returned=ResultEnvelope('{}'),
                    started_at=STARTED_AT,
                    left=left,
                ),
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
        events = framed({'kind': 'events'}, f'[{SLOT_RECOVERED}]'.encode('utf-8'))
        for data in (framed({'kind': 'stop'}), framed(rust_init), events):
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
                assert kind.pool.max_size == 2
            case _:
                pytest.fail('a shared async kind decodes as one')
        assert isinstance(init_frame(child_kind=shared_thread_kind()).child_kind, core.SharedThreadChildKind)


class TestChildHandle:
    def test_the_handle_of_a_child_comes_from_its_init_frame(self) -> None:
        handle = core.CoreChildHandle.from_init(init_frame())
        assert isinstance(handle, core.CoreChildHandle)

    def test_a_handler_child_has_no_child_handle(self) -> None:
        """A handler child runs no task, so it writes nothing through a
        child handle: its init frame is a defect of the caller."""
        with pytest.raises(ValueError, match='handler child'):
            core.CoreChildHandle.from_init(init_frame(child_kind=handler_kind()))


def _parent(meta: object, body: bytes = b'') -> object:
    """The parent frame that the core decodes from ``meta`` and ``body``, or its
    refusal, as the frame reader of a child gets it."""
    return core.decode_parent_frame(json.dumps(meta).encode('utf-8'), body)


def _refused(meta: object, body: bytes = b'') -> bool:
    return isinstance(_parent(meta, body), core.FrameRefused)


def _init_kind(child_kind: object) -> object:
    """The init frame with ``child_kind`` in place of its own, decoded."""
    return _parent({**init_meta(), 'child_kind': child_kind})


def _shared_run() -> dict[str, object]:
    return shared_run_meta(
        dispatch=7,
        task_id=TASK_ID,
        work=async_work(timeout_ms=30_000, cancel_grace_ms=5_000),
    )


_POOL: Final = pool(min_size=0, max_size=2, check=True)
_TASK_SEND_POOL: Final = 2
_VALID_KINDS: Final[list[dict[str, object]]] = [
    {'kind': 'isolated', 'pool': _POOL, 'task_send_pool': _TASK_SEND_POOL},
    {
        'kind': 'shared_async',
        'seats': 4,
        'pool': _POOL,
        'executor_threads': 4,
        'task_send_pool': _TASK_SEND_POOL,
    },
    {'kind': 'shared_thread', 'seats': 4, 'pool': _POOL, 'task_send_pool': _TASK_SEND_POOL},
]
_ASYNC_KIND: Final = _VALID_KINDS[1]
_THREAD_KIND: Final = _VALID_KINDS[2]


def _without(fields: dict[str, object], key: str) -> dict[str, object]:
    return {name: value for name, value in fields.items() if name != key}


class TestThreadFrames:
    def test_thread_init_frame_round_trip(self) -> None:
        kind = {
            'kind': 'shared_thread',
            'seats': MAX_SEATS_PER_SHARED_THREAD_CHILD,
            'pool': pool(min_size=1, max_size=8, check=False),
            'task_send_pool': 5,
        }
        match _init_kind(kind):
            case core.InitFrame(child_kind=core.SharedThreadChildKind() as decoded):
                assert decoded.seats == MAX_SEATS_PER_SHARED_THREAD_CHILD
                assert (decoded.pool.min_size, decoded.pool.max_size, decoded.pool.check) == (
                    1,
                    8,
                    False,
                )
                assert decoded.task_send_pool == 5
            case other:
                pytest.fail(f'a shared thread kind decodes as one, got {other!r}')

    @pytest.mark.parametrize(
        'work',
        [
            thread_work(timeout_ms=MIN_TASK_TIMEOUT_MS - 1),
            thread_work(timeout_ms=MAX_TASK_TIMEOUT_MS + 1),
            {'kind': 'thread_task', 'timeout_ms': None},
            {'kind': 'thread_task', 'timeout_ms': True},
            {'kind': 'thread_task'},
        ],
    )
    def test_the_thread_task_work_is_checked(self, work: object) -> None:
        assert _refused({**_shared_run(), 'work': work}, b'{}')

    @pytest.mark.parametrize(
        'child_kind',
        [
            _without(_THREAD_KIND, 'seats'),
            {**_THREAD_KIND, 'seats': 0},
            {**_THREAD_KIND, 'seats': MAX_SEATS_PER_SHARED_THREAD_CHILD + 1},
            {**_THREAD_KIND, 'seats': '4'},
        ],
    )
    def test_the_thread_child_kind_is_checked(self, child_kind: object) -> None:
        assert isinstance(_init_kind(child_kind), core.FrameRefused)


class TestSharedRunFrames:
    @pytest.mark.parametrize(
        ('kind', 'reason', 'expected'),
        [
            (
                'options_unreadable',
                'stored task_options do not parse: x',
                OptionsUnreadable(reason='stored task_options do not parse: x'),
            ),
            (
                'value_invalid',
                'stored timeout_ms is below 1000 ms',
                TimeoutValueInvalid(reason='stored timeout_ms is below 1000 ms'),
            ),
            (
                'grace_invalid',
                'stored cancel_grace_ms is below 100 ms',
                CancelGraceInvalid(reason='stored cancel_grace_ms is below 100 ms'),
            ),
        ],
    )
    def test_a_refusal_round_trips(self, kind: str, reason: str, expected: object) -> None:
        frame = shared_run_frame(
            dispatch=7,
            task_id=TASK_ID,
            work=refusal(kind, reason),  # pyright: ignore[reportArgumentType]
        )
        match frame.work:
            case core.StoredOptionsRefused() as refused:
                assert stored_refusal_of(refused) == expected
            case other:
                pytest.fail(f'a refusal decodes as a refusal, got {other!r}')

    @pytest.mark.parametrize(
        ('key', 'value'),
        [
            ('dispatch', 0),
            ('dispatch', True),
            ('dispatch', '7'),
            ('work', None),
            ('work', {'kind': 'unknown'}),
            ('work', async_work(timeout_ms=MIN_TASK_TIMEOUT_MS - 1, cancel_grace_ms=5000)),
            ('work', async_work(timeout_ms=MAX_TASK_TIMEOUT_MS + 1, cancel_grace_ms=5000)),
            ('work', {'kind': 'task', 'timeout_ms': None, 'cancel_grace_ms': 5000}),
            ('work', async_work(timeout_ms=30000, cancel_grace_ms=MIN_CANCEL_GRACE_MS - 1)),
            ('work', async_work(timeout_ms=30000, cancel_grace_ms=MAX_CANCEL_GRACE_MS + 1)),
            ('work', {'kind': 'task', 'timeout_ms': 30000, 'cancel_grace_ms': 1.5}),
            ('work', {'kind': 'refusal', 'refusal': None}),
            ('work', {'kind': 'refusal', 'refusal': {'kind': 'none'}}),
            ('work', {'kind': 'refusal', 'refusal': {'kind': 'ms', 'ms': 30000}}),
            ('work', {'kind': 'refusal', 'refusal': {'kind': 'grace_invalid'}}),
            ('task_id', None),
            ('claimed_at', 'not a date'),
            ('is_workflow_task', 0),
            ('orphan_self_heal', 1),
            ('orphan_self_heal', None),
        ],
    )
    def test_every_shared_run_field_is_checked(self, key: str, value: object) -> None:
        assert _refused({**_shared_run(), key: value}, b'{}')

    @pytest.mark.parametrize(
        'child_kind',
        [
            None,
            'isolated',
            {'kind': 'unknown'},
            _without(_ASYNC_KIND, 'seats'),
            {**_ASYNC_KIND, 'seats': 0},
            {**_ASYNC_KIND, 'seats': MAX_SEATS_PER_SHARED_ASYNC_CHILD + 1},
            {**_ASYNC_KIND, 'seats': True},
        ],
    )
    def test_the_child_kind_is_checked(self, child_kind: object) -> None:
        assert isinstance(_init_kind(child_kind), core.FrameRefused)


class TestChildKindPool:
    """The pool and the task-send pool of each child kind, and the executor
    size of a shared async child, on the wire."""

    @pytest.mark.parametrize('kind', _VALID_KINDS)
    def test_a_complete_kind_decodes(self, kind: dict[str, object]) -> None:
        """The control of the refusals below: each valid shape decodes."""
        match _init_kind(kind):
            case core.InitFrame(
                child_kind=core.IsolatedChildKind()
                | core.SharedAsyncChildKind()
                | core.SharedThreadChildKind() as child_kind,
            ):
                child_pool = child_kind.pool
                assert (child_pool.min_size, child_pool.max_size, child_pool.check) == (
                    0,
                    2,
                    True,
                )
            case other:
                pytest.fail(f'a complete kind decodes, got {other!r}')

    @pytest.mark.parametrize('kind', _VALID_KINDS)
    @pytest.mark.parametrize(
        'child_pool',
        [
            None,
            'pool',
            {'max_size': 2, 'check': True},
            {'min_size': -1, 'max_size': 2, 'check': True},
            {'min_size': MAX_CHILD_POOL_CONNECTIONS + 1, 'max_size': 2, 'check': True},
            {'min_size': 0, 'check': True},
            {'min_size': 0, 'max_size': 0, 'check': True},
            {'min_size': 0, 'max_size': MAX_CHILD_POOL_CONNECTIONS + 1, 'check': True},
            {'min_size': 0, 'max_size': True, 'check': True},
            {'min_size': 0, 'max_size': 2},
            {'min_size': 0, 'max_size': 2, 'check': 1},
        ],
    )
    def test_the_pool_is_checked(
        self,
        kind: dict[str, object],
        child_pool: object,
    ) -> None:
        assert isinstance(_init_kind({**kind, 'pool': child_pool}), core.FrameRefused)

    @pytest.mark.parametrize('threads', [None, 0, MAX_EXECUTOR_THREADS + 1, '4', True])
    def test_the_executor_threads_are_checked(self, threads: object) -> None:
        refused = _init_kind({**_ASYNC_KIND, 'executor_threads': threads})
        assert isinstance(refused, core.FrameRefused)

    @pytest.mark.parametrize('threads', [1, MAX_EXECUTOR_THREADS])
    def test_the_executor_bounds_are_accepted(self, threads: int) -> None:
        match _init_kind({**_ASYNC_KIND, 'executor_threads': threads}):
            case core.InitFrame(child_kind=core.SharedAsyncChildKind() as kind):
                assert kind.executor_threads == threads
            case other:
                pytest.fail(f'the bound is accepted, got {other!r}')

    @pytest.mark.parametrize('kind', _VALID_KINDS)
    @pytest.mark.parametrize(
        'task_send_pool',
        [None, 0, MAX_CHILD_POOL_CONNECTIONS + 1, '2', True],
    )
    def test_the_task_send_pool_is_checked(
        self,
        kind: dict[str, object],
        task_send_pool: object,
    ) -> None:
        refused = _init_kind({**kind, 'task_send_pool': task_send_pool})
        assert isinstance(refused, core.FrameRefused)

    @pytest.mark.parametrize('kind', _VALID_KINDS)
    def test_a_kind_without_a_task_send_pool_is_refused(
        self,
        kind: dict[str, object],
    ) -> None:
        assert isinstance(_init_kind(_without(kind, 'task_send_pool')), core.FrameRefused)

    @pytest.mark.parametrize('kind', _VALID_KINDS)
    @pytest.mark.parametrize('connections', [1, MAX_CHILD_POOL_CONNECTIONS])
    def test_the_task_send_pool_bounds_are_accepted(
        self,
        kind: dict[str, object],
        connections: int,
    ) -> None:
        match _init_kind({**kind, 'task_send_pool': connections}):
            case core.InitFrame(child_kind=child_kind):
                assert child_kind.task_send_pool == connections
            case other:
                pytest.fail(f'the bound is accepted, got {other!r}')

    def test_a_min_size_above_max_size_is_left_to_the_pool(self) -> None:
        """The pool of the child refuses it when it opens, as a pool setup
        failure that the boot reports."""
        kind = {
            'kind': 'isolated',
            'pool': pool(min_size=2, max_size=1, check=False),
            'task_send_pool': _TASK_SEND_POOL,
        }
        match _init_kind(kind):
            case core.InitFrame(child_kind=core.IsolatedChildKind() as decoded):
                assert (decoded.pool.min_size, decoded.pool.max_size, decoded.pool.check) == (
                    2,
                    1,
                    False,
                )
            case other:
                pytest.fail(f'the pool decodes as sent, got {other!r}')


_RUN_KINDS: Final = ['run', 'shared_run']


def _run_of_kind(kind: str) -> dict[str, object]:
    match kind:
        case 'run':
            return run_meta(task_id=TASK_ID)
        case 'shared_run':
            return _shared_run()
        case _:
            raise AssertionError(f'no run frame of kind {kind!r}')


class TestRunFrameClaimTime:
    """Every write of an attempt in the child is fenced on the claim time
    of its run frame. A run frame without one is malformed, and the decoder
    refuses it as it refuses any other bad field."""

    @pytest.mark.parametrize('kind', _RUN_KINDS)
    def test_a_null_claim_time_is_refused(self, kind: str) -> None:
        match _parent({**_run_of_kind(kind), 'claimed_at': None}, b'{}'):
            case core.FrameRefused(reason=reason):
                assert 'claimed_at' in reason
            case other:
                pytest.fail(f'a null claim time is refused, got {other!r}')

    @pytest.mark.parametrize('kind', _RUN_KINDS)
    def test_a_missing_claim_time_is_refused(self, kind: str) -> None:
        match _parent(_without(_run_of_kind(kind), 'claimed_at'), b'{}'):
            case core.FrameRefused(reason=reason):
                assert 'claimed_at' in reason
            case other:
                pytest.fail(f'a missing claim time is refused, got {other!r}')


_PROGRAM: Final[dict[str, object]] = {
    'kind': 'python',
    'app_locator': 'app.main:app',
    'imports': [],
    'sys_path_roots': [],
    'parent_sys_path': [],
    'parent_argv': ['horsies', 'worker'],
}


class TestParentDecoder:
    def test_a_child_frame_is_refused_by_the_parent_decoder(self) -> None:
        ready = {
            'kind': 'ready',
            'protocol': core.protocol_numbers().protocol_version,
            'pid': 1,
            'rss_mb': None,
        }
        assert _refused(ready)

    @pytest.mark.parametrize(
        'meta',
        [b'', b'{', b'[]', b'1', b'{}', b'{"kind":1}', b'{"kind":"unknown"}'],
    )
    def test_metadata_that_is_not_a_frame_is_refused(self, meta: bytes) -> None:
        assert isinstance(core.decode_parent_frame(meta, b''), core.FrameRefused)

    @pytest.mark.parametrize(
        ('key', 'value'),
        [
            ('task_id', 1),
            ('task_name', None),
            ('worker_id', None),
            ('claimed_at', 'not a date'),
            ('claimed_at', 5),
            ('orphan_self_heal', 1),
            ('orphan_self_heal', 'true'),
            ('work', 'x'),
            ('work', None),
            ('work', {'kind': 'later'}),
            ('work', {'kind': 'refusal', 'refusal': {'kind': 'value_invalid'}}),
            ('work', {'kind': 'refusal', 'refusal': {'kind': 'grace_invalid', 'reason': None}}),
            ('work', {'kind': 'refusal', 'refusal': {'kind': 'options_unreadable', 'reason': 5}}),
            ('args_bytes', 'x'),
            ('kwargs_bytes', 1.5),
            ('is_workflow_task', 1),
            ('timeout', {'kind': 'ms', 'ms': 30000}),
        ],
    )
    def test_every_run_field_is_checked(self, key: str, value: object) -> None:
        """A field of a version-1 run frame that version 2 does not have
        (``args_bytes``, ``kwargs_bytes``, ``is_workflow_task``, ``timeout``)
        is refused as a field outside the frame."""
        assert _refused({**run_meta(task_id=TASK_ID), key: value}, b'{}')

    @pytest.mark.parametrize(
        ('key', 'value'),
        [
            ('app_locator', 1),
            ('imports', 'x'),
            ('imports', [1]),
            ('sys_path_roots', None),
            ('parent_sys_path', [None]),
            ('parent_argv', {}),
        ],
    )
    def test_every_program_field_is_checked(self, key: str, value: object) -> None:
        assert _refused({**init_meta(), 'program': {**_PROGRAM, key: value}})

    @pytest.mark.parametrize(
        ('key', 'value'),
        [
            ('log_level', '20'),
            ('database_url', None),
            ('connection', []),
            ('process_name', None),
        ],
    )
    def test_every_init_field_is_checked(self, key: str, value: object) -> None:
        assert _refused({**init_meta(), key: value})


class TestBootFailedFrames:
    @pytest.mark.parametrize(
        ('report', 'text'),
        [
            (LoopHoldsTasks(count=2, names=('a', 'b')), 'a, b'),
            (LoopHoldsTasks(count=12, names=('a', 'b')), 'a, b, and 10 more'),
        ],
        ids=['all-named', 'some-left-out'],
    )
    def test_the_leftover_text_says_how_many_names_it_leaves_out(
        self,
        report: LoopHoldsTasks,
        text: str,
    ) -> None:
        assert report.names_text() == text

    def test_boot_failed_frame_names_subject_and_cause(self) -> None:
        frame = BootFailedFrame(
            cause=TaskLoopCreateFailed(),
            error_name='OSError',
            detail='[Errno 24] Too many open files',
        )
        assert frame.describe() == (
            'the child could not create the event loop for async tasks: '
            'OSError: [Errno 24] Too many open files'
        )

    def test_a_built_boot_failed_frame_is_cut_to_its_bounds(self) -> None:
        frame = boot_failed_frame(
            AppLoadFailed(),
            'E' * (MAX_BOOT_ERROR_NAME_CHARS + 5),
            'bad \ud800' + 'd' * MAX_BOOT_DETAIL_CHARS,
        )
        assert len(frame.error_name) == MAX_BOOT_ERROR_NAME_CHARS
        assert len(frame.detail) == MAX_BOOT_DETAIL_CHARS
        assert frame.detail.startswith('bad \\ud800')
        meta, body = _meta_and_body(encode_child_frame(frame))
        assert meta == {
            'kind': 'boot_failed',
            'cause': {'kind': 'app_load_failed'},
            'error_name': frame.error_name,
            'detail': frame.detail,
        }
        assert body == b''


class TestBootFailureFromException:
    """The child names the cause of an exception that a boot step raised."""

    @pytest.mark.parametrize(
        ('number', 'name'),
        [
            (errno.EMFILE, ShortageErrno.EMFILE),
            (errno.ENFILE, ShortageErrno.ENFILE),
            (errno.ENOMEM, ShortageErrno.ENOMEM),
            (errno.EAGAIN, ShortageErrno.EAGAIN),
        ],
    )
    @pytest.mark.parametrize('step', [AppLoadFailed(), ChildPoolSetupFailed()])
    def test_an_errno_in_the_table_is_a_resource_shortage(
        self,
        number: int,
        name: ShortageErrno,
        step: AppLoadFailed | ChildPoolSetupFailed,
    ) -> None:
        exc = OSError(number, 'short')
        frame = child_main.boot_failure_from_exception(exc, step)
        assert frame.cause == ResourceShortage(name)
        # Python builds a subclass for some errno values, for example
        # BlockingIOError for EAGAIN. The errno decides, not the class.
        assert frame.error_name == type(exc).__name__

    def test_the_table_holds_exactly_the_four_errno_values(self) -> None:
        assert set(CLEARABLE_ERRNOS) == {
            errno.EMFILE,
            errno.ENFILE,
            errno.ENOMEM,
            errno.EAGAIN,
        }

    @pytest.mark.parametrize(
        'exc',
        [
            OSError(errno.ENOENT, 'missing'),
            OSError('no errno'),
            PermissionError(errno.EACCES, 'denied'),
            ImportError('no module'),
            RuntimeError('boom'),
            MemoryError(),
        ],
        ids=['enoent', 'no-errno', 'eacces', 'import', 'runtime', 'memory'],
    )
    @pytest.mark.parametrize('step', [AppLoadFailed(), ChildPoolSetupFailed()])
    def test_any_other_exception_is_the_cause_of_its_step(
        self,
        exc: Exception,
        step: AppLoadFailed | ChildPoolSetupFailed,
    ) -> None:
        frame = child_main.boot_failure_from_exception(exc, step)
        assert frame.cause == step
        assert frame.error_name == type(exc).__name__
        assert frame.detail == str(exc)

    @pytest.mark.parametrize('step', [AppLoadFailed(), ChildPoolSetupFailed()])
    def test_a_refused_thread_of_a_boot_step_is_thread_start_refused(
        self,
        step: AppLoadFailed | ChildPoolSetupFailed,
    ) -> None:
        """The step that raised does not decide: a refused thread can clear,
        whichever step started it."""
        refusal_error = RuntimeError("can't start new thread")
        exc = ThreadStartRefusedError('a thread of the connection pool', refusal_error)

        assert child_main.boot_failure_from_exception(exc, step) == boot_failed_frame(
            ThreadStartRefused(),
            'RuntimeError',
            "a thread of the connection pool: can't start new thread",
        )

    def test_a_refused_control_thread_is_thread_start_refused(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        def refuse(_thread: threading.Thread) -> None:
            raise RuntimeError("can't start new thread")

        monkeypatch.setattr(threading.Thread, 'start', refuse)
        left, right = socket.socketpair()
        try:
            result = child_main.start_control_thread(ParentFrameReader(left), ParentFrameInbox())
        finally:
            left.close()
            right.close()
        assert result == Err(
            boot_failed_frame(
                ThreadStartRefused(),
                'RuntimeError',
                "can't start new thread",
            ),
        )

    def test_a_loop_short_of_descriptors_is_a_resource_shortage(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        def no_descriptors() -> object:
            raise OSError(errno.EMFILE, 'Too many open files')

        monkeypatch.setattr(asyncio, 'new_event_loop', no_descriptors)
        result = child_main.open_task_loop_at_boot(AsyncTaskPresence.HAS_ASYNC_TASKS)
        assert result == Err(
            boot_failed_frame(
                ResourceShortage(ShortageErrno.EMFILE),
                'OSError',
                '[Errno 24] Too many open files',
            ),
        )

    @pytest.mark.parametrize(
        'error',
        [
            NotImplementedError('this policy cannot build a loop here'),
            RuntimeError('the event loop policy refuses'),
            OSError(errno.ENOENT, 'No such file or directory'),
        ],
        ids=['policy-not-implemented', 'policy-runtime', 'enoent'],
    )
    def test_a_loop_that_fails_the_same_way_each_boot_is_task_loop_create_failed(
        self,
        monkeypatch: pytest.MonkeyPatch,
        error: Exception,
    ) -> None:
        """An event loop policy that raises, or an errno outside the table,
        fails every boot the same way, so the cause repeats."""

        def refuses() -> object:
            raise error

        monkeypatch.setattr(asyncio, 'new_event_loop', refuses)
        result = child_main.open_task_loop_at_boot(AsyncTaskPresence.HAS_ASYNC_TASKS)
        assert result == Err(
            boot_failed_frame(TaskLoopCreateFailed(), type(error).__name__, str(error)),
        )

    def test_an_app_with_no_async_task_gets_no_loop(self) -> None:
        match child_main.open_task_loop_at_boot(AsyncTaskPresence.NO_ASYNC_TASK):
            case Ok(task_loop):
                assert task_loop.report() == NoTaskLoop()
            case Err(failed):
                raise AssertionError(failed)


class TestVersionThreeChildFrames:
    """The bytes of each frame and field that version 3 adds, from the
    frames of the child."""

    def test_a_result_frame_carries_the_start_of_its_attempt(self) -> None:
        encoded = encode_child_frame(
            ResultFrame(
                task_id=TASK_ID,
                end=ResultEnvelope('{}'),
                started=StartReported(STARTED_AT),
                rss=RssMb(1.5),
                task_loop=NoTaskLoop(),
            ),
        )
        assert encoded == framed(
            {
                'kind': 'result',
                'task_id': str(TASK_ID),
                'end': {'kind': 'result'},
                'started_at': {'kind': 'reported', 'time': STARTED_TEXT},
                'rss_mb': 1.5,
                'task_loop': {'kind': 'none'},
                'child': 'serves_on',
            },
            b'{}',
        )

    def test_a_shared_result_frame_carries_the_start_of_its_attempt(self) -> None:
        encoded = encode_child_frame(
            SharedResultFrame(
                dispatch=3,
                task_id=TASK_ID,
                end=StoppedBeforeRunning('CLAIM_LOST'),
                started=StartNotReported(),
                rss=RssUnreadable(),
            ),
        )
        assert encoded == framed(
            {
                'kind': 'shared_result',
                'dispatch': 3,
                'task_id': str(TASK_ID),
                'end': {'kind': 'stopped_before_running', 'reason': 'CLAIM_LOST'},
                'started_at': {'kind': 'not_reported'},
                'rss_mb': None,
                'child': 'serves_on',
            },
        )

    def test_the_frames_of_a_body_that_ran_carry_the_time_of_its_start(self) -> None:
        left = LoopHoldsTasks(count=1, names=('poll',))
        assert encode_child_frame(
            BodyReturnedFrame(
                task_id=TASK_ID,
                returned=ResultEnvelope('{}'),
                started_at=STARTED_AT,
                left=left,
            ),
        ) == framed(
            {
                'kind': 'body_returned',
                'task_id': str(TASK_ID),
                'returned': {'kind': 'result'},
                'started_at': STARTED_TEXT,
                'left': {'count': 1, 'names': ['poll']},
            },
            b'{}',
        )
        assert encode_child_frame(
            LeftoverCancelFrame(
                dispatch=2,
                task_id=TASK_ID,
                returned=ResultEnvelope('{}'),
                started_at=STARTED_AT,
            ),
        ) == framed(
            {
                'kind': 'leftover_cancel',
                'dispatch': 2,
                'task_id': str(TASK_ID),
                'returned': {'kind': 'result'},
                'started_at': STARTED_TEXT,
            },
            b'{}',
        )
        assert encode_child_frame(
            LeftoverStuckFrame(
                dispatch=2,
                task_id=TASK_ID,
                tasks=0,
                executor_items=1,
                names=(),
                returned=ResultEnvelope('{}'),
                started_at=STARTED_AT,
            ),
        ) == framed(
            {
                'kind': 'leftover_stuck',
                'dispatch': 2,
                'task_id': str(TASK_ID),
                'tasks': 0,
                'executor_items': 1,
                'names': [],
                'returned': {'kind': 'result'},
                'started_at': STARTED_TEXT,
            },
            b'{}',
        )

    def test_the_handler_stats_frame_carries_the_counts_of_each_handler(self) -> None:
        encoded = encode_child_frame(
            HandlerStatsFrame(
                handlers=(
                    HandlerCounts(handler='app.handlers.on_claim', errors=0, dropped=3),
                    HandlerCounts(handler='app.handlers.on_end', errors=2, dropped=0),
                ),
                rss=RssMb(40.5),
            ),
        )
        assert encoded == framed(
            {
                'kind': 'handler_stats',
                'handlers': [
                    {'handler': 'app.handlers.on_claim', 'errors': 0, 'dropped': 3},
                    {'handler': 'app.handlers.on_end', 'errors': 2, 'dropped': 0},
                ],
                'rss_mb': 40.5,
            },
        )
        assert encode_child_frame(
            HandlerStatsFrame(handlers=(), rss=RssUnreadable()),
        ) == framed({'kind': 'handler_stats', 'handlers': [], 'rss_mb': None})

    @pytest.mark.parametrize(
        'frame',
        [
            pytest.param(
                ResultFrame(
                    task_id=TASK_ID,
                    end=StoppedBeforeRunning('CLAIM_LOST'),
                    started=StartReported(STARTED_AT),
                    rss=RssMb(1.0),
                    task_loop=NoTaskLoop(),
                ),
                id='a-stop-with-a-start',
            ),
            pytest.param(
                ResultFrame(
                    task_id=TASK_ID,
                    end=ResultEnvelope('{}'),
                    started=StartNotReported(),
                    rss=RssMb(1.0),
                    task_loop=NoTaskLoop(),
                ),
                id='a-result-with-no-start',
            ),
            pytest.param(
                SharedResultFrame(
                    dispatch=1,
                    task_id=TASK_ID,
                    end=ResultEnvelope('{}'),
                    started=StartNotReported(),
                    rss=RssMb(1.0),
                ),
                id='a-seat-result-with-no-start',
            ),
            pytest.param(
                HandlerStatsFrame(
                    handlers=(HandlerCounts(handler='', errors=0, dropped=0),),
                    rss=RssMb(1.0),
                ),
                id='an-empty-handler-name',
            ),
            pytest.param(
                HandlerStatsFrame(
                    handlers=(
                        HandlerCounts(
                            handler='h' * (MAX_HANDLER_NAME_CHARS + 1), errors=0, dropped=0,
                        ),
                    ),
                    rss=RssMb(1.0),
                ),
                id='a-handler-name-over-its-bound',
            ),
            pytest.param(
                HandlerStatsFrame(
                    handlers=(
                        HandlerCounts(handler='a', errors=0, dropped=0),
                        HandlerCounts(handler='a', errors=1, dropped=0),
                    ),
                    rss=RssMb(1.0),
                ),
                id='a-handler-counted-twice',
            ),
            pytest.param(
                HandlerStatsFrame(
                    handlers=tuple(
                        HandlerCounts(handler=f'h{n}', errors=0, dropped=0)
                        for n in range(MAX_EVENT_HANDLERS + 1)
                    ),
                    rss=RssMb(1.0),
                ),
                id='too-many-handlers',
            ),
        ],
    )
    def test_a_frame_outside_its_rules_is_a_defect_of_the_child(
        self,
        frame: ResultFrame | SharedResultFrame | HandlerStatsFrame,
    ) -> None:
        with pytest.raises(ValueError):
            encode_child_frame(frame)


class TestVersionThreeParentFrames:
    """The frames and fields that version 3 adds, from the worker parent,
    as the core decodes them for a child."""

    def test_a_run_frame_gives_its_stored_trace_carrier(self) -> None:
        trace = carrier_trace({'traceparent': TRACEPARENT, 'tracestate': 'k=v'})
        assert trace == {
            'kind': 'carrier',
            'text': f'{{"traceparent":"{TRACEPARENT}","tracestate":"k=v"}}',
        }
        frame = run_frame(task_id=TASK_ID, trace=trace)
        assert run_trace_of(frame.trace) == StoredTraceCarrier(
            carrier={'traceparent': TRACEPARENT, 'tracestate': 'k=v'},
        )
        shared = shared_run_frame(
            dispatch=7, task_id=TASK_ID, work=thread_work(5_000), trace=trace,
        )
        assert run_trace_of(shared.trace) == StoredTraceCarrier(
            carrier={'traceparent': TRACEPARENT, 'tracestate': 'k=v'},
        )

    def test_a_run_frame_with_no_carrier_gives_none(self) -> None:
        assert run_trace_of(run_frame(task_id=TASK_ID).trace) == NoTraceCarrier()

    @pytest.mark.parametrize(
        'trace',
        [
            None,
            'no_carrier',
            {'kind': 'remote'},
            {'kind': 'carrier'},
            {'kind': 'carrier', 'text': 5},
            {'kind': 'carrier', 'text': '[1]'},
            {'kind': 'carrier', 'text': '{"a":1}'},
            {'kind': 'carrier', 'text': '{"a":"1","a":"2"}'},
            {'kind': 'carrier', 'text': '{"k":"' + 'v' * 2048 + '"}'},
            {'kind': 'no_carrier', 'text': '{}'},
        ],
    )
    @pytest.mark.parametrize('kind', ['run', 'shared_run'])
    def test_every_malformed_trace_is_refused(self, kind: str, trace: object) -> None:
        assert _refused({**_run_of_kind(kind), 'trace': trace}, b'{}')

    @pytest.mark.parametrize('kind', ['run', 'shared_run'])
    def test_a_run_frame_without_its_trace_is_refused(self, kind: str) -> None:
        match _parent(_without(_run_of_kind(kind), 'trace'), b'{}'):
            case core.FrameRefused(reason=reason):
                assert 'trace' in reason
            case other:
                pytest.fail(f'a run frame without its trace is refused, got {other!r}')

    def test_the_init_frame_of_a_handler_child(self) -> None:
        match _init_kind(handler_kind(task_send_pool=3)):
            case core.InitFrame(child_kind=core.HandlerChildKind() as kind):
                assert kind.task_send_pool == 3
            case other:
                pytest.fail(f'a handler kind decodes as one, got {other!r}')

    @pytest.mark.parametrize(
        'child_kind',
        [
            {'kind': 'handler'},
            {'kind': 'handler', 'task_send_pool': 0},
            {'kind': 'handler', 'task_send_pool': 1, 'pool': _POOL},
        ],
    )
    def test_the_handler_kind_is_checked(self, child_kind: object) -> None:
        assert isinstance(_init_kind(child_kind), core.FrameRefused)

    def test_an_events_frame_gives_the_wire_text_of_each_event(self) -> None:
        frame = events_frame((CLAIM_RELEASED, SLOT_RECOVERED))
        assert frame.events == [CLAIM_RELEASED, SLOT_RECOVERED]

    @pytest.mark.parametrize(
        ('meta', 'body'),
        [
            ({'kind': 'events', 'count': 1}, f'[{SLOT_RECOVERED}]'.encode('utf-8')),
            ({'kind': 'events'}, b'[]'),
            ({'kind': 'events'}, b'{}'),
            ({'kind': 'events'}, b'[1,'),
            ({'kind': 'events'}, b'[\xff]'),
        ],
    )
    def test_every_malformed_events_frame_is_refused(
        self,
        meta: dict[str, object],
        body: bytes,
    ) -> None:
        assert _refused(meta, body)

    def test_an_init_frame_of_version_two_is_a_named_mismatch(self) -> None:
        match _parent(init_meta(protocol=2)):
            case core.ParentProtocolMismatch(received=received, expected=expected):
                assert received == 2
                assert expected == 3
            case other:
                pytest.fail(f'a version-2 init frame is a mismatch, got {other!r}')


class TestATaskChildRefusesHandlerFrames:
    """A task child serves runs: the init frame of a handler child and an
    events frame end it with the bad-frame code."""

    def test_the_init_frame_of_a_handler_child_is_refused(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setattr(child_main.os, '_exit', _raise_exit)
        sock = _ScriptedSocket(framed(init_meta(child_kind=handler_kind())), 1 << 16)
        with pytest.raises(_Exited) as exited:
            read_init_frame(sock, ParentFrameReader(sock))  # pyright: ignore[reportArgumentType]
        assert exited.value.code == child_main.BAD_PARENT_FRAME_EXIT_CODE

    def test_an_events_frame_after_the_init_frame_is_refused(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setattr(child_main.os, '_exit', _raise_exit)
        data = framed({'kind': 'events'}, f'[{SLOT_RECOVERED}]'.encode('utf-8'))
        reader = ParentFrameReader(_ScriptedSocket(data, 1 << 16))  # pyright: ignore[reportArgumentType]
        with pytest.raises(_Exited) as exited:
            child_main._read_parent_frames(reader, ParentFrameInbox())
        assert exited.value.code == child_main.BAD_PARENT_FRAME_EXIT_CODE
