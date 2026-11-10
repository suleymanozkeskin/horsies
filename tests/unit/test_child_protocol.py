"""Tests for the parent-child wire protocol."""

# pyright: reportPrivateUsage=false

from __future__ import annotations

import asyncio
import dataclasses
import errno
import json
import queue
import socket
import threading
from datetime import datetime, timezone

import pytest

from horsies.core.models.boot_failure import (
    CLEARABLE_ERRNOS,
    AppLoadFailed,
    ChildPoolSetupFailed,
    ReportedBootCause,
    ResourceShortage,
    ShortageErrno,
    TaskLoopCreateFailed,
    ThreadStartRefused,
)
from horsies.core.types.result import Err, Ok
from horsies.core.worker import child_main
from horsies.core.worker.child_main import _bounded_result
from horsies.core.worker.child_protocol import (
    DeadlineFiredFrame,
    IsolatedChild,
    SeatFinished,
    SeatTimedOut,
    SharedAsyncChild,
    SharedResultFrame,
    SharedRunFrame,
    FRAME_HEADER,
    MAX_BODY_BYTES,
    MAX_BOOT_DETAIL_CHARS,
    MAX_BOOT_ERROR_NAME_CHARS,
    MAX_LEFTOVER_NAME_CHARS,
    MAX_LEFTOVER_TASK_NAMES,
    MAX_META_BYTES,
    BootFailedFrame,
    ChildForkedFrame,
    EncodedFrame,
    ForkFailedFrame,
    FrameOverBound,
    InitFrame,
    LoopHoldsNoTask,
    LoopHoldsTasks,
    MalformedFrame,
    NoTaskLoop,
    PreloadFrame,
    PreloadReadyFrame,
    PreloadRefusedFrame,
    ReadyFrame,
    ResultFrame,
    RssMb,
    RssUnreadable,
    RunFrame,
    StopFrame,
    TaskLoopReport,
    boot_failed_frame,
    decode_child_frame,
    decode_parent_frame,
    decode_preload_frame,
    encode_child_frame,
    encode_parent_frame,
    encode_preload_frame,
    parse_header,
)
from horsies.core.models.tasks import (
    MAX_CANCEL_GRACE_MS,
    MAX_TASK_TIMEOUT_MS,
    MIN_CANCEL_GRACE_MS,
    MIN_TASK_TIMEOUT_MS,
)
from horsies.core.models.worker_profile import MAX_SEATS_PER_SHARED_ASYNC_CHILD
from horsies.core.worker.task_loop import AsyncTaskPresence
from horsies.core.worker.stored_timeout import (
    NoTimeout,
    OptionsUnreadable,
    StoredTimeout,
    TaskTimeout,
    TimeoutValueInvalid,
)


REPORTED_CAUSES: list[ReportedBootCause] = [
    AppLoadFailed(),
    ChildPoolSetupFailed(),
    TaskLoopCreateFailed(),
    ThreadStartRefused(),
    *(ResourceShortage(name) for name in ShortageErrno),
]


def _split(encoded: EncodedFrame) -> tuple[bytes, bytes]:
    match parse_header(encoded.head[: FRAME_HEADER.size]):
        case Ok(sizes):
            assert sizes.body_bytes == len(encoded.body)
            meta = encoded.head[FRAME_HEADER.size :]
            assert len(meta) == sizes.meta_bytes
            return meta, encoded.body
        case Err(over):
            raise AssertionError(over)


def _init_frame() -> InitFrame:
    return InitFrame(
        app_locator='app.module:app',
        imports=('tasks.a', 'tasks.b'),
        sys_path_roots=('/srv/app',),
        parent_sys_path=('/srv/app', '/usr/lib/python3.13'),
        parent_argv=('horsies', 'worker'),
        loglevel=20,
        database_url='postgresql://user:secret@db/app',
        connect_kwargs={'keepalives': 1, 'prepare_threshold': None},
        child_pool_min_size=0,
        child_pool_max_size=2,
        child_pool_check=True,
        process_name='worker-child',
        child_kind=IsolatedChild(),
    )


def _run_frame(**changes: object) -> RunFrame:
    frame = RunFrame(
        task_id='task-1',
        task_name='add',
        args_json='[1]',
        kwargs_json='{"a":"é"}',
        worker_id='worker-1',
        is_workflow_task=True,
        timeout=TaskTimeout(ms=1000),
        claimed_at=datetime(2026, 11, 1, 12, 30, 15, 250, tzinfo=timezone.utc),
    )
    return dataclasses.replace(frame, **changes)  # type: ignore[arg-type]


def _shared_run_frame(**changes: object) -> SharedRunFrame:
    frame = SharedRunFrame(
        dispatch=7,
        task_id='task-1',
        task_name='fetch',
        args_json=None,
        kwargs_json='{"url":"é"}',
        worker_id='worker-1',
        is_workflow_task=False,
        claimed_at=datetime(2026, 11, 1, 12, 30, 15, 250, tzinfo=timezone.utc),
        timeout_ms=30_000,
        cancel_grace_ms=5_000,
    )
    return dataclasses.replace(frame, **changes)  # type: ignore[arg-type]


def _shared_result(end: SeatFinished | SeatTimedOut) -> SharedResultFrame:
    return SharedResultFrame(dispatch=7, task_id='task-1', end=end, rss=RssMb(80.5))


class TestSharedFrames:
    def test_shared_init_frame_round_trip(self) -> None:
        frame = dataclasses.replace(
            _init_frame(), child_kind=SharedAsyncChild(seats=25)
        )
        meta, body = _split(encode_parent_frame(frame))
        assert decode_parent_frame(meta, body) == Ok(frame)

    def test_shared_run_frame_round_trip(self) -> None:
        frame = _shared_run_frame()
        meta, body = _split(encode_parent_frame(frame))
        assert decode_parent_frame(meta, body) == Ok(frame)

    def test_shared_run_frame_without_claim_or_arguments(self) -> None:
        frame = _shared_run_frame(claimed_at=None, kwargs_json=None)
        meta, body = _split(encode_parent_frame(frame))
        assert body == b''
        assert decode_parent_frame(meta, body) == Ok(frame)

    @pytest.mark.parametrize(
        'end',
        [
            SeatFinished(ok=True, result_json='{"ok":"é"}', failed_reason=None),
            SeatFinished(ok=False, result_json='', failed_reason='CLAIM_LOST'),
            SeatTimedOut(),
        ],
    )
    def test_shared_result_round_trip(self, end: SeatFinished | SeatTimedOut) -> None:
        frame = _shared_result(end)
        meta, body = _split(encode_child_frame(frame))
        assert decode_child_frame(meta, body) == Ok(frame)

    def test_a_timed_out_end_carries_no_body(self) -> None:
        meta, body = _split(encode_child_frame(_shared_result(SeatTimedOut())))
        assert body == b''
        assert isinstance(decode_child_frame(meta, b'stray'), Err)

    def test_deadline_fired_round_trip(self) -> None:
        frame = DeadlineFiredFrame(dispatch=3, task_id='task-9')
        meta, body = _split(encode_child_frame(frame))
        assert decode_child_frame(meta, body) == Ok(frame)

    @pytest.mark.parametrize(
        ('key', 'value'),
        [
            ('dispatch', 0),
            ('dispatch', True),
            ('dispatch', '7'),
            ('timeout_ms', MIN_TASK_TIMEOUT_MS - 1),
            ('timeout_ms', MAX_TASK_TIMEOUT_MS + 1),
            ('timeout_ms', None),
            ('cancel_grace_ms', MIN_CANCEL_GRACE_MS - 1),
            ('cancel_grace_ms', MAX_CANCEL_GRACE_MS + 1),
            ('cancel_grace_ms', 1.5),
            ('task_id', None),
            ('claimed_at', 'not a date'),
            ('is_workflow_task', 0),
        ],
    )
    def test_every_shared_run_field_is_checked(self, key: str, value: object) -> None:
        meta, body = _split(encode_parent_frame(_shared_run_frame()))
        fields = json.loads(meta)
        fields[key] = value
        assert isinstance(decode_parent_frame(json.dumps(fields).encode(), body), Err)

    @pytest.mark.parametrize(
        'child_kind',
        [
            None,
            'isolated',
            {'kind': 'unknown'},
            {'kind': 'shared_async'},
            {'kind': 'shared_async', 'seats': 0},
            {'kind': 'shared_async', 'seats': MAX_SEATS_PER_SHARED_ASYNC_CHILD + 1},
            {'kind': 'shared_async', 'seats': True},
        ],
    )
    def test_the_child_kind_is_checked(self, child_kind: object) -> None:
        meta, body = _split(encode_parent_frame(_init_frame()))
        fields = json.loads(meta)
        fields['child_kind'] = child_kind
        assert isinstance(decode_parent_frame(json.dumps(fields).encode(), body), Err)

    @pytest.mark.parametrize(
        ('key', 'value'),
        [
            ('dispatch', -1),
            ('task_id', 5),
            ('end', None),
            ('end', {'kind': 'unknown'}),
            ('end', {'kind': 'finished', 'ok': 1, 'failed_reason': None}),
            ('end', {'kind': 'finished', 'ok': True, 'failed_reason': 3}),
            ('rss_mb', 'x'),
        ],
    )
    def test_every_shared_result_field_is_checked(
        self,
        key: str,
        value: object,
    ) -> None:
        end = SeatFinished(ok=True, result_json='x', failed_reason=None)
        meta, body = _split(encode_child_frame(_shared_result(end)))
        fields = json.loads(meta)
        fields[key] = value
        assert isinstance(decode_child_frame(json.dumps(fields).encode(), body), Err)

    def test_a_shared_run_frame_is_refused_by_the_child_decoder(self) -> None:
        meta, body = _split(encode_parent_frame(_shared_run_frame()))
        assert isinstance(decode_child_frame(meta, body), Err)

    def test_shared_run_frame_fields(self) -> None:
        names = {field.name for field in dataclasses.fields(SharedRunFrame)}
        wire = set(json.loads(_split(encode_parent_frame(_shared_run_frame()))[0]))
        wire -= {'kind', 'args_bytes', 'kwargs_bytes'}
        assert names - {'args_json', 'kwargs_json'} == wire

    def test_shared_result_frame_fields(self) -> None:
        names = {field.name for field in dataclasses.fields(SharedResultFrame)}
        end = SeatFinished(ok=True, result_json='x', failed_reason=None)
        wire = set(json.loads(_split(encode_child_frame(_shared_result(end)))[0]))
        assert names - {'rss'} == wire - {'kind', 'rss_mb'}


class TestParentFrames:
    def test_init_frame_round_trip(self) -> None:
        frame = _init_frame()
        meta, body = _split(encode_parent_frame(frame))
        assert decode_parent_frame(meta, body) == Ok(frame)

    def test_init_frame_keeps_the_database_url_out_of_the_body(self) -> None:
        encoded = encode_parent_frame(_init_frame())
        assert encoded.body == b''

    def test_run_frame_round_trip(self) -> None:
        frame = _run_frame()
        meta, body = _split(encode_parent_frame(frame))
        assert decode_parent_frame(meta, body) == Ok(frame)

    @pytest.mark.parametrize(
        ('args_json', 'kwargs_json'),
        [(None, None), ('', ''), (None, '{}'), ('[]', None), ('', None)],
    )
    def test_run_frame_keeps_missing_apart_from_empty(
        self,
        args_json: str | None,
        kwargs_json: str | None,
    ) -> None:
        frame = _run_frame(args_json=args_json, kwargs_json=kwargs_json)
        meta, body = _split(encode_parent_frame(frame))
        assert decode_parent_frame(meta, body) == Ok(frame)

    def test_run_frame_without_claim_or_timeout(self) -> None:
        frame = _run_frame(claimed_at=None, timeout=NoTimeout())
        meta, body = _split(encode_parent_frame(frame))
        assert decode_parent_frame(meta, body) == Ok(frame)

    @pytest.mark.parametrize(
        'timeout',
        [
            NoTimeout(),
            TaskTimeout(ms=MIN_TASK_TIMEOUT_MS),
            TaskTimeout(ms=MAX_TASK_TIMEOUT_MS),
            OptionsUnreadable(reason='stored task_options do not parse'),
            TimeoutValueInvalid(reason='stored timeout_ms is a bool, not an int'),
        ],
    )
    def test_each_stored_timeout_form_round_trips(self, timeout: StoredTimeout) -> None:
        frame = _run_frame(timeout=timeout)
        meta, body = _split(encode_parent_frame(frame))
        assert decode_parent_frame(meta, body) == Ok(frame)

    def test_stop_frame_round_trip(self) -> None:
        meta, body = _split(encode_parent_frame(StopFrame()))
        assert decode_parent_frame(meta, body) == Ok(StopFrame())

    def test_body_lengths_must_match_the_body(self) -> None:
        meta, body = _split(encode_parent_frame(_run_frame()))
        match decode_parent_frame(meta, body + b'x'):
            case Err(MalformedFrame(reason=reason)):
                assert 'args_bytes' in reason
            case other:
                raise AssertionError(other)

    def test_child_frame_is_refused_by_the_parent_decoder(self) -> None:
        meta, body = _split(encode_child_frame(ReadyFrame(1, RssUnreadable())))
        assert isinstance(decode_parent_frame(meta, body), Err)


class TestChildFrames:
    @pytest.mark.parametrize('rss', [RssMb(12.5), RssMb(0.0), RssUnreadable()])
    def test_ready_frame_round_trip(self, rss: RssMb | RssUnreadable) -> None:
        frame = ReadyFrame(pid=42, rss=rss)
        meta, body = _split(encode_child_frame(frame))
        assert decode_child_frame(meta, body) == Ok(frame)

    @pytest.mark.parametrize('failed_reason', [None, 'WORKFLOW_STOPPED', ''])
    @pytest.mark.parametrize('ok', [True, False])
    @pytest.mark.parametrize('result_json', ['', '{"é":1}'])
    @pytest.mark.parametrize(
        'task_loop',
        [
            NoTaskLoop(),
            LoopHoldsNoTask(),
            LoopHoldsTasks(count=3, names=('a', 'b', 'Client._keepalive')),
            LoopHoldsTasks(
                count=MAX_LEFTOVER_TASK_NAMES + 5,
                names=('n' * MAX_LEFTOVER_NAME_CHARS,) * MAX_LEFTOVER_TASK_NAMES,
            ),
        ],
        ids=['no-task-loop', 'empty', 'held', 'held-at-the-bounds'],
    )
    def test_result_frame_round_trip(
        self,
        ok: bool,
        failed_reason: str | None,
        result_json: str,
        task_loop: TaskLoopReport,
    ) -> None:
        frame = ResultFrame('task-1', ok, result_json, failed_reason, RssMb(3.0), task_loop)
        meta, body = _split(encode_child_frame(frame))
        assert decode_child_frame(meta, body) == Ok(frame)

    def test_invalid_utf8_body_is_malformed(self) -> None:
        frame = ResultFrame('task-1', True, 'x', None, RssUnreadable(), NoTaskLoop())
        meta, _ = _split(encode_child_frame(frame))
        assert isinstance(decode_child_frame(meta, b'\xff'), Err)

    @pytest.mark.parametrize(
        'detail',
        ['[Errno 24] Too many open files', '', 'é \x00'],
    )
    @pytest.mark.parametrize('cause', REPORTED_CAUSES)
    def test_boot_failed_frame_round_trip(
        self,
        cause: ReportedBootCause,
        detail: str,
    ) -> None:
        frame = BootFailedFrame(cause=cause, error_name='OSError', detail=detail)
        meta, body = _split(encode_child_frame(frame))
        assert body == b''
        assert decode_child_frame(meta, body) == Ok(frame)

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
        meta, body = _split(encode_child_frame(frame))
        assert decode_child_frame(meta, body) == Ok(frame)


class TestPreloadFrames:
    @pytest.mark.parametrize(
        'frame',
        [
            PreloadReadyFrame(pid=7),
            ChildForkedFrame(pid=8),
            ForkFailedFrame(reason='EAGAIN'),
            PreloadRefusedFrame(reason='threads started during import'),
        ],
    )
    def test_round_trip(self, frame: PreloadFrame) -> None:
        encoded = encode_preload_frame(frame)
        assert encoded.body == b''
        assert decode_preload_frame(encoded.head[FRAME_HEADER.size :]) == Ok(frame)

    @pytest.mark.parametrize(
        'meta',
        [
            b'',
            b'{',
            b'{"kind":"unknown"}',
            b'{"kind":"child_forked"}',
            b'{"kind":"child_forked","pid":"1"}',
            b'{"kind":"fork_failed","reason":1}',
        ],
    )
    def test_bad_metadata_is_refused(self, meta: bytes) -> None:
        assert isinstance(decode_preload_frame(meta), Err)


class TestHeader:
    def test_empty_metadata_is_refused(self) -> None:
        """The reader waits for a frame that a zero-length header never completes."""
        raw = FRAME_HEADER.pack(0, 0)
        assert parse_header(raw) == Err(FrameOverBound(0, 0))

    def test_sizes_at_the_bounds_pass(self) -> None:
        raw = FRAME_HEADER.pack(MAX_META_BYTES, MAX_BODY_BYTES)
        match parse_header(raw):
            case Ok(sizes):
                assert (sizes.meta_bytes, sizes.body_bytes) == (
                    MAX_META_BYTES,
                    MAX_BODY_BYTES,
                )
            case Err(over):
                raise AssertionError(over)

    @pytest.mark.parametrize(
        ('meta_bytes', 'body_bytes'),
        [
            (MAX_META_BYTES + 1, 0),
            (0, MAX_BODY_BYTES + 1),
            (2**32 - 1, 2**64 - 1),
        ],
    )
    def test_sizes_over_the_bounds_are_refused(
        self,
        meta_bytes: int,
        body_bytes: int,
    ) -> None:
        raw = FRAME_HEADER.pack(meta_bytes, body_bytes)
        assert parse_header(raw) == Err(FrameOverBound(meta_bytes, body_bytes))


class TestMalformedMetadata:
    @pytest.mark.parametrize(
        'meta',
        [b'', b'{', b'[]', b'1', b'{}', b'{"kind":1}', b'{"kind":"unknown"}'],
    )
    def test_both_decoders_refuse(self, meta: bytes) -> None:
        assert isinstance(decode_parent_frame(meta, b''), Err)
        assert isinstance(decode_child_frame(meta, b''), Err)

    @pytest.mark.parametrize(
        ('key', 'value'),
        [
            ('task_id', 1),
            ('task_name', None),
            ('args_bytes', 'x'),
            ('args_bytes', True),
            ('kwargs_bytes', 1.5),
            ('worker_id', None),
            ('is_workflow_task', 1),
            ('timeout', 'x'),
            ('timeout', None),
            ('timeout', {'kind': 'ms', 'ms': MIN_TASK_TIMEOUT_MS - 1}),
            ('timeout', {'kind': 'ms', 'ms': MAX_TASK_TIMEOUT_MS + 1}),
            ('timeout', {'kind': 'ms', 'ms': True}),
            ('timeout', {'kind': 'ms', 'ms': 1500.0}),
            ('timeout', {'kind': 'value_invalid'}),
            ('timeout', {'kind': 'options_unreadable', 'reason': 5}),
            ('timeout', {'kind': 'later'}),
            ('claimed_at', 'not a date'),
            ('claimed_at', 5),
        ],
    )
    def test_every_run_field_is_checked(self, key: str, value: object) -> None:
        meta, body = _split(encode_parent_frame(_run_frame()))
        fields = json.loads(meta)
        fields[key] = value
        assert isinstance(decode_parent_frame(json.dumps(fields).encode(), body), Err)

    @pytest.mark.parametrize(
        ('key', 'value'),
        [
            ('app_locator', 1),
            ('imports', 'x'),
            ('imports', [1]),
            ('sys_path_roots', None),
            ('parent_sys_path', [None]),
            ('parent_argv', {}),
            ('loglevel', '20'),
            ('database_url', None),
            ('connect_kwargs', []),
            ('child_pool_min_size', None),
            ('child_pool_max_size', False),
            ('child_pool_check', 0),
            ('process_name', None),
        ],
    )
    def test_every_init_field_is_checked(self, key: str, value: object) -> None:
        meta, body = _split(encode_parent_frame(_init_frame()))
        fields = json.loads(meta)
        fields[key] = value
        assert isinstance(decode_parent_frame(json.dumps(fields).encode(), body), Err)

    @pytest.mark.parametrize(
        ('key', 'value'),
        [
            ('task_id', None),
            ('ok', 1),
            ('failed_reason', 1),
            ('rss_mb', 'x'),
            ('rss_mb', True),
            ('task_loop', None),
            ('task_loop', {'kind': 'unknown'}),
            ('task_loop', {'kind': 'held', 'names': ['a']}),
            ('task_loop', {'kind': 'held', 'count': 0, 'names': []}),
            ('task_loop', {'kind': 'held', 'count': True, 'names': ['a']}),
            ('task_loop', {'kind': 'held', 'count': '2', 'names': ['a', 'b']}),
            ('task_loop', {'kind': 'held', 'count': 1}),
            ('task_loop', {'kind': 'held', 'count': 1, 'names': 'a'}),
            ('task_loop', {'kind': 'held', 'count': 1, 'names': [1]}),
            ('task_loop', {'kind': 'held', 'count': 2, 'names': ['a']}),
            ('task_loop', {'kind': 'held', 'count': 1, 'names': ['a', 'b']}),
            (
                'task_loop',
                {
                    'kind': 'held',
                    'count': MAX_LEFTOVER_TASK_NAMES + 1,
                    'names': ['a'] * (MAX_LEFTOVER_TASK_NAMES + 1),
                },
            ),
            (
                'task_loop',
                {
                    'kind': 'held',
                    'count': 1,
                    'names': ['n' * (MAX_LEFTOVER_NAME_CHARS + 1)],
                },
            ),
            ('task_loop', {'kind': 'unavailable', 'detail': 'OSError'}),
        ],
    )
    def test_every_result_field_is_checked(self, key: str, value: object) -> None:
        frame = ResultFrame('task-1', True, 'x', None, RssMb(1.0), NoTaskLoop())
        meta, body = _split(encode_child_frame(frame))
        fields = json.loads(meta)
        fields[key] = value
        assert isinstance(decode_child_frame(json.dumps(fields).encode(), body), Err)

    @pytest.mark.parametrize(
        ('key', 'value'),
        [
            ('error_name', None),
            ('error_name', 24),
            ('error_name', 'E' * (MAX_BOOT_ERROR_NAME_CHARS + 1)),
            ('detail', None),
            ('detail', []),
            ('detail', 'd' * (MAX_BOOT_DETAIL_CHARS + 1)),
            ('cause', None),
            ('cause', 'resource_shortage'),
            ('cause', {'kind': 'unknown'}),
            ('cause', {'kind': 'resource_shortage', 'errno': 'ENOENT'}),
            ('cause', {'kind': 'resource_shortage'}),
            ('cause', {'kind': 'spawn_failed'}),
        ],
        ids=[
            'error-name-missing',
            'error-name-number',
            'error-name-over-bound',
            'detail-missing',
            'detail-list',
            'detail-over-bound',
            'cause-missing',
            'cause-text',
            'cause-unknown-kind',
            'cause-errno-not-in-table',
            'cause-errno-missing',
            'cause-spawn-from-a-child',
        ],
    )
    def test_every_boot_failed_field_is_checked(
        self,
        key: str,
        value: object,
    ) -> None:
        frame = BootFailedFrame(
            cause=ResourceShortage(ShortageErrno.EMFILE),
            error_name='OSError',
            detail='no descriptor',
        )
        meta, body = _split(encode_child_frame(frame))
        fields = json.loads(meta)
        fields[key] = value
        assert isinstance(decode_child_frame(json.dumps(fields).encode(), body), Err)


class TestStrictJson:
    def test_non_finite_rss_is_a_defect_not_a_frame(self) -> None:
        with pytest.raises(AssertionError, match='not JSON'):
            encode_child_frame(ReadyFrame(1, RssMb(float('nan'))))

    def test_non_finite_number_on_the_wire_is_malformed(self) -> None:
        assert isinstance(decode_child_frame(b'{"kind":"ready","pid":1,"rss_mb":NaN}', b''), Err)


class TestFieldCoverage:
    """A new field must not skip the wire: each one needs a round trip above."""

    def test_run_frame_fields(self) -> None:
        names = {field.name for field in dataclasses.fields(RunFrame)}
        wire = set(json.loads(_split(encode_parent_frame(_run_frame()))[0]))
        wire -= {'kind', 'args_bytes', 'kwargs_bytes'}
        assert names - {'args_json', 'kwargs_json'} == wire

    def test_init_frame_fields(self) -> None:
        names = {field.name for field in dataclasses.fields(InitFrame)}
        wire = set(json.loads(_split(encode_parent_frame(_init_frame()))[0]))
        assert names == wire - {'kind'}

    def test_result_frame_fields(self) -> None:
        names = {field.name for field in dataclasses.fields(ResultFrame)}
        held = LoopHoldsTasks(count=1, names=('a',))
        frame = ResultFrame('t', True, 'x', None, RssMb(1.0), held)
        wire = set(json.loads(_split(encode_child_frame(frame))[0]))
        assert names - {'result_json', 'rss'} == wire - {'kind', 'rss_mb'}

    def test_leftover_report_fields(self) -> None:
        names = {field.name for field in dataclasses.fields(LoopHoldsTasks)}
        held = LoopHoldsTasks(count=1, names=('a',))
        frame = ResultFrame('t', True, 'x', None, RssMb(1.0), held)
        wire = json.loads(_split(encode_child_frame(frame))[0])['task_loop']
        assert names == set(wire) - {'kind'}

    def test_boot_failed_frame_fields(self) -> None:
        names = {field.name for field in dataclasses.fields(BootFailedFrame)}
        frame = BootFailedFrame(cause=AppLoadFailed(), error_name='OSError', detail='x')
        wire = set(json.loads(_split(encode_child_frame(frame))[0]))
        assert names == wire - {'kind'}


class TestInitFrameRepr:
    def test_the_database_password_stays_out_of_the_repr(self) -> None:
        assert 'secret' not in repr(_init_frame())


class TestFailureReasonText:
    def test_a_long_reason_is_cut_and_framed(self) -> None:
        wire = _bounded_result('add', (True, 'kept', '\x00' * (2 << 20)))
        assert wire[:2] == (True, 'kept')
        frame = ResultFrame('task-1', wire[0], wire[1], wire[2], RssUnreadable(), NoTaskLoop())
        meta, body = _split(encode_child_frame(frame))
        assert decode_child_frame(meta, body) == Ok(frame)

    def test_lone_surrogate_in_a_reason_can_be_framed(self) -> None:
        wire = _bounded_result('add', (False, '', 'bad \ud800 text'))
        frame = ResultFrame('task-1', wire[0], wire[1], wire[2], RssUnreadable(), NoTaskLoop())
        meta, body = _split(encode_child_frame(frame))
        assert decode_child_frame(meta, body) == Ok(frame)

    def test_result_too_large_to_store_becomes_a_typed_failure(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setattr(child_main, 'MAX_BODY_BYTES', 4)
        ok, result_json, reason = _bounded_result('add', (True, 'x' * 5, None))
        assert ok is True
        assert 'WORKER_SERIALIZATION_ERROR' in result_json
        assert reason is not None and 'above the' in reason


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

    def test_a_refused_control_thread_is_thread_start_refused(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        def refuse(_thread: threading.Thread) -> None:
            raise RuntimeError("can't start new thread")

        monkeypatch.setattr(threading.Thread, 'start', refuse)
        left, right = socket.socketpair()
        try:
            result = child_main.start_control_thread(left, queue.SimpleQueue())
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

    def test_a_loop_that_cannot_be_created_is_task_loop_create_failed(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        def no_descriptors() -> object:
            raise OSError(errno.EMFILE, 'Too many open files')

        monkeypatch.setattr(asyncio, 'new_event_loop', no_descriptors)
        result = child_main.open_task_loop_at_boot(AsyncTaskPresence.HAS_ASYNC_TASKS)
        assert result == Err(
            boot_failed_frame(
                TaskLoopCreateFailed(),
                'OSError',
                '[Errno 24] Too many open files',
            ),
        )

    def test_an_app_with_no_async_task_gets_no_loop(self) -> None:
        match child_main.open_task_loop_at_boot(AsyncTaskPresence.NO_ASYNC_TASK):
            case Ok(task_loop):
                assert task_loop.report() == NoTaskLoop()
            case Err(failed):
                raise AssertionError(failed)
