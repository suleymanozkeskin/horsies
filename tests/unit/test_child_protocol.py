"""Tests for the parent-child wire protocol."""

# pyright: reportPrivateUsage=false

from __future__ import annotations

import dataclasses
import json
from datetime import datetime, timezone

import pytest

from horsies.core.types.result import Err, Ok
from horsies.core.worker import child_main
from horsies.core.worker.child_main import _bounded_result
from horsies.core.worker.child_protocol import (
    FRAME_HEADER,
    MAX_BODY_BYTES,
    MAX_META_BYTES,
    ChildForkedFrame,
    EncodedFrame,
    ForkFailedFrame,
    FrameOverBound,
    InitFrame,
    MalformedFrame,
    PreloadFrame,
    PreloadReadyFrame,
    PreloadRefusedFrame,
    ReadyFrame,
    ResultFrame,
    RssMb,
    RssUnreadable,
    RunFrame,
    StopFrame,
    decode_child_frame,
    decode_parent_frame,
    decode_preload_frame,
    encode_child_frame,
    encode_parent_frame,
    encode_preload_frame,
    parse_header,
)


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
    )


def _run_frame(**changes: object) -> RunFrame:
    frame = RunFrame(
        task_id='task-1',
        task_name='add',
        args_json='[1]',
        kwargs_json='{"a":"é"}',
        worker_id='worker-1',
        runner_heartbeat_interval_ms=30_000,
        is_workflow_task=True,
        timeout_ms=1000,
        claimed_at=datetime(2026, 11, 1, 12, 30, 15, 250, tzinfo=timezone.utc),
    )
    return dataclasses.replace(frame, **changes)  # type: ignore[arg-type]


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
        frame = _run_frame(claimed_at=None, timeout_ms=None)
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
    def test_result_frame_round_trip(
        self,
        ok: bool,
        failed_reason: str | None,
        result_json: str,
    ) -> None:
        frame = ResultFrame('task-1', ok, result_json, failed_reason, RssMb(3.0))
        meta, body = _split(encode_child_frame(frame))
        assert decode_child_frame(meta, body) == Ok(frame)

    def test_invalid_utf8_body_is_malformed(self) -> None:
        frame = ResultFrame('task-1', True, 'x', None, RssUnreadable())
        meta, _ = _split(encode_child_frame(frame))
        assert isinstance(decode_child_frame(meta, b'\xff'), Err)


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
            ('runner_heartbeat_interval_ms', '1'),
            ('is_workflow_task', 1),
            ('timeout_ms', 'x'),
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
        ],
    )
    def test_every_result_field_is_checked(self, key: str, value: object) -> None:
        frame = ResultFrame('task-1', True, 'x', None, RssMb(1.0))
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


class TestInitFrameRepr:
    def test_the_database_password_stays_out_of_the_repr(self) -> None:
        assert 'secret' not in repr(_init_frame())


class TestFailureReasonText:
    def test_a_long_reason_is_cut_and_framed(self) -> None:
        wire = _bounded_result(_run_frame(), (True, 'kept', '\x00' * (2 << 20)))
        assert wire[:2] == (True, 'kept')
        frame = ResultFrame('task-1', wire[0], wire[1], wire[2], RssUnreadable())
        meta, body = _split(encode_child_frame(frame))
        assert decode_child_frame(meta, body) == Ok(frame)

    def test_lone_surrogate_in_a_reason_can_be_framed(self) -> None:
        wire = _bounded_result(_run_frame(), (False, '', 'bad \ud800 text'))
        frame = ResultFrame('task-1', wire[0], wire[1], wire[2], RssUnreadable())
        meta, body = _split(encode_child_frame(frame))
        assert decode_child_frame(meta, body) == Ok(frame)

    def test_result_too_large_to_store_becomes_a_typed_failure(
        self,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setattr(child_main, 'MAX_BODY_BYTES', 4)
        ok, result_json, reason = _bounded_result(_run_frame(), (True, 'x' * 5, None))
        assert ok is True
        assert 'WORKER_SERIALIZATION_ERROR' in result_json
        assert reason is not None and 'above the' in reason
