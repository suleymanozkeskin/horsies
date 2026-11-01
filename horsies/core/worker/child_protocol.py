"""Wire protocol between the worker parent and one child process.

A frame is ``FRAME_HEADER`` (metadata length, body length), JSON metadata, and
a raw body. Large strings travel in the body, so they are not escaped twice.
Nothing on this socket is pickled: the receiver builds each frame from a fixed
set of fields, and every task frame carries its task id.

This module is pure. It does no I/O and holds no state.
"""

from __future__ import annotations

import struct
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Final, TypeAlias, assert_never, cast

from horsies.core.codec.json_io import dumps_json, loads_json_bytes
from horsies.core.types.result import Err, Ok, Result

FRAME_HEADER: Final = struct.Struct('>IQ')

MAX_META_BYTES: Final = 1 << 20
# Postgres cannot store a field of 1 GiB or more, so a larger result could
# never be saved. This bound changes no outcome: it only fails earlier.
MAX_BODY_BYTES: Final = (1 << 30) - 1


@dataclass(frozen=True, slots=True)
class InitFrame:
    """Parent to child, first frame: everything the child needs to boot.

    Carries the database URL here because argv is visible in ``ps``.
    """

    app_locator: str
    imports: tuple[str, ...]
    sys_path_roots: tuple[str, ...]
    parent_sys_path: tuple[str, ...]
    parent_argv: tuple[str, ...]
    loglevel: int
    database_url: str
    connect_kwargs: Mapping[str, object]
    child_pool_min_size: int
    child_pool_max_size: int
    child_pool_check: bool
    process_name: str


@dataclass(frozen=True, slots=True)
class RunFrame:
    """Parent to child: run one task."""

    task_id: str
    task_name: str
    args_json: str | None
    kwargs_json: str | None
    worker_id: str
    runner_heartbeat_interval_ms: int
    is_workflow_task: bool
    timeout_ms: int | None
    claimed_at: datetime | None


@dataclass(frozen=True, slots=True)
class StopFrame:
    """Parent to child: exit normally. Sent only to a child with no task."""


@dataclass(frozen=True, slots=True)
class RssMb:
    """The child's resident set size."""

    value: float


@dataclass(frozen=True, slots=True)
class RssUnreadable:
    """The child could not read its resident set size."""


RssReading: TypeAlias = RssMb | RssUnreadable


@dataclass(frozen=True, slots=True)
class ReadyFrame:
    """Child to parent: boot finished, the child can take a task."""

    pid: int
    rss: RssReading


@dataclass(frozen=True, slots=True)
class ResultFrame:
    """Child to parent: the wire tuple of ``_run_task_entry``, plus the RSS."""

    task_id: str
    ok: bool
    result_json: str
    failed_reason: str | None
    rss: RssReading


ParentFrame: TypeAlias = InitFrame | RunFrame | StopFrame
ChildFrame: TypeAlias = ReadyFrame | ResultFrame


@dataclass(frozen=True, slots=True)
class EncodedFrame:
    """Header and metadata, and the body apart from them, to avoid a copy."""

    head: bytes
    body: bytes


@dataclass(frozen=True, slots=True)
class FrameSizes:
    """Sizes from a header that passed the bounds."""

    meta_bytes: int
    body_bytes: int


@dataclass(frozen=True, slots=True)
class FrameOverBound:
    """A header declared more than ``MAX_META_BYTES`` or ``MAX_BODY_BYTES``."""

    meta_bytes: int
    body_bytes: int


@dataclass(frozen=True, slots=True)
class MalformedFrame:
    """The bytes are not a valid frame of the expected direction."""

    reason: str


class _BadField(Exception):
    """A metadata field is missing or has the wrong type."""


def _pack(meta: Mapping[str, object], body: bytes) -> EncodedFrame:
    match dumps_json(dict(meta)):
        case Ok(text):
            meta_bytes = text.encode('utf-8')
        case Err(exc):
            # Frame fields are built here and in worker config, not from
            # user input, so a field that is not JSON is a defect.
            raise AssertionError(f'frame metadata is not JSON: {exc}')
    return EncodedFrame(
        head=FRAME_HEADER.pack(len(meta_bytes), len(body)) + meta_bytes,
        body=body,
    )


def _rss_field(rss: RssReading) -> float | None:
    match rss:
        case RssMb(value=value):
            return value
        case RssUnreadable():
            return None
        case _ as unreachable:
            assert_never(unreachable)


def encode_parent_frame(frame: ParentFrame) -> EncodedFrame:
    """Encode a parent frame."""
    match frame:
        case InitFrame():
            return _pack(
                {
                    'kind': 'init',
                    'app_locator': frame.app_locator,
                    'imports': list(frame.imports),
                    'sys_path_roots': list(frame.sys_path_roots),
                    'parent_sys_path': list(frame.parent_sys_path),
                    'parent_argv': list(frame.parent_argv),
                    'loglevel': frame.loglevel,
                    'database_url': frame.database_url,
                    'connect_kwargs': dict(frame.connect_kwargs),
                    'child_pool_min_size': frame.child_pool_min_size,
                    'child_pool_max_size': frame.child_pool_max_size,
                    'child_pool_check': frame.child_pool_check,
                    'process_name': frame.process_name,
                },
                b'',
            )
        case RunFrame():
            return _encode_run_frame(frame)
        case StopFrame():
            return _pack({'kind': 'stop'}, b'')
        case _ as unreachable:
            assert_never(unreachable)


def _encode_run_frame(frame: RunFrame) -> EncodedFrame:
    args = frame.args_json.encode('utf-8') if frame.args_json is not None else None
    kwargs = (
        frame.kwargs_json.encode('utf-8') if frame.kwargs_json is not None else None
    )
    return _pack(
        {
            'kind': 'run',
            'task_id': frame.task_id,
            'task_name': frame.task_name,
            'args_bytes': None if args is None else len(args),
            'kwargs_bytes': None if kwargs is None else len(kwargs),
            'worker_id': frame.worker_id,
            'runner_heartbeat_interval_ms': frame.runner_heartbeat_interval_ms,
            'is_workflow_task': frame.is_workflow_task,
            'timeout_ms': frame.timeout_ms,
            'claimed_at': (
                None if frame.claimed_at is None else frame.claimed_at.isoformat()
            ),
        },
        (args or b'') + (kwargs or b''),
    )


def encode_child_frame(frame: ChildFrame) -> EncodedFrame:
    """Encode a child frame."""
    match frame:
        case ReadyFrame():
            return _pack(
                {'kind': 'ready', 'pid': frame.pid, 'rss_mb': _rss_field(frame.rss)},
                b'',
            )
        case ResultFrame():
            return _pack(
                {
                    'kind': 'result',
                    'task_id': frame.task_id,
                    'ok': frame.ok,
                    'failed_reason': frame.failed_reason,
                    'rss_mb': _rss_field(frame.rss),
                },
                frame.result_json.encode('utf-8'),
            )
        case _ as unreachable:
            assert_never(unreachable)


def parse_header(raw: bytes) -> Result[FrameSizes, FrameOverBound]:
    """Check a header against the bounds, before any buffer is allocated.

    ``raw`` must be ``FRAME_HEADER.size`` bytes: the caller reads exactly that.
    """
    meta_bytes, body_bytes = FRAME_HEADER.unpack(raw)
    if meta_bytes > MAX_META_BYTES or body_bytes > MAX_BODY_BYTES:
        return Err(FrameOverBound(meta_bytes=meta_bytes, body_bytes=body_bytes))
    return Ok(FrameSizes(meta_bytes=meta_bytes, body_bytes=body_bytes))


def _text(meta: Mapping[str, Any], key: str) -> str:
    value = meta.get(key)
    if not isinstance(value, str):
        raise _BadField(key)
    return value


def _optional_text(meta: Mapping[str, Any], key: str) -> str | None:
    value = meta.get(key)
    if value is not None and not isinstance(value, str):
        raise _BadField(key)
    return value


def _whole(meta: Mapping[str, Any], key: str) -> int:
    value = meta.get(key)
    if not isinstance(value, int) or isinstance(value, bool):
        raise _BadField(key)
    return value


def _optional_whole(meta: Mapping[str, Any], key: str) -> int | None:
    if meta.get(key) is None:
        return None
    return _whole(meta, key)


def _flag(meta: Mapping[str, Any], key: str) -> bool:
    value = meta.get(key)
    if not isinstance(value, bool):
        raise _BadField(key)
    return value


def _text_tuple(meta: Mapping[str, Any], key: str) -> tuple[str, ...]:
    value = meta.get(key)
    if not isinstance(value, list):
        raise _BadField(key)
    items = cast('list[object]', value)
    if not all(isinstance(item, str) for item in items):
        raise _BadField(key)
    return tuple(cast('list[str]', items))


def _rss(meta: Mapping[str, Any]) -> RssReading:
    value = meta.get('rss_mb')
    if value is None:
        return RssUnreadable()
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise _BadField('rss_mb')
    return RssMb(float(value))


def _load_meta(meta_bytes: bytes) -> Mapping[str, Any]:
    match loads_json_bytes(meta_bytes):
        case Ok(value) if isinstance(value, dict):
            return cast('Mapping[str, Any]', value)
        case Ok(_):
            raise _BadField('metadata is not an object')
        case Err(exc):
            raise _BadField(f'metadata is not JSON: {exc}')


def _decode_init(meta: Mapping[str, Any]) -> InitFrame:
    connect_kwargs = meta.get('connect_kwargs')
    if not isinstance(connect_kwargs, dict):
        raise _BadField('connect_kwargs')
    return InitFrame(
        app_locator=_text(meta, 'app_locator'),
        imports=_text_tuple(meta, 'imports'),
        sys_path_roots=_text_tuple(meta, 'sys_path_roots'),
        parent_sys_path=_text_tuple(meta, 'parent_sys_path'),
        parent_argv=_text_tuple(meta, 'parent_argv'),
        loglevel=_whole(meta, 'loglevel'),
        database_url=_text(meta, 'database_url'),
        connect_kwargs=cast('Mapping[str, object]', connect_kwargs),
        child_pool_min_size=_whole(meta, 'child_pool_min_size'),
        child_pool_max_size=_whole(meta, 'child_pool_max_size'),
        child_pool_check=_flag(meta, 'child_pool_check'),
        process_name=_text(meta, 'process_name'),
    )


def _decode_run(meta: Mapping[str, Any], body: bytes) -> RunFrame:
    args_bytes = _optional_whole(meta, 'args_bytes')
    kwargs_bytes = _optional_whole(meta, 'kwargs_bytes')
    args_end = args_bytes or 0
    if args_end + (kwargs_bytes or 0) != len(body):
        raise _BadField('args_bytes and kwargs_bytes do not match the body')
    claimed_at = _optional_text(meta, 'claimed_at')
    return RunFrame(
        task_id=_text(meta, 'task_id'),
        task_name=_text(meta, 'task_name'),
        args_json=None if args_bytes is None else body[:args_end].decode('utf-8'),
        kwargs_json=None if kwargs_bytes is None else body[args_end:].decode('utf-8'),
        worker_id=_text(meta, 'worker_id'),
        runner_heartbeat_interval_ms=_whole(meta, 'runner_heartbeat_interval_ms'),
        is_workflow_task=_flag(meta, 'is_workflow_task'),
        timeout_ms=_optional_whole(meta, 'timeout_ms'),
        claimed_at=None if claimed_at is None else datetime.fromisoformat(claimed_at),
    )


def decode_parent_frame(
    meta_bytes: bytes,
    body: bytes,
) -> Result[ParentFrame, MalformedFrame]:
    """Build a parent frame from received bytes. Used by the child."""
    try:
        meta = _load_meta(meta_bytes)
        kind = _text(meta, 'kind')
        match kind:
            case 'init':
                return Ok(_decode_init(meta))
            case 'run':
                return Ok(_decode_run(meta, body))
            case 'stop':
                return Ok(StopFrame())
            case _:
                return Err(MalformedFrame(f'unknown parent frame {kind!r}'))
    except (_BadField, ValueError) as exc:
        return Err(MalformedFrame(f'{type(exc).__name__}: {exc}'))


def decode_child_frame(
    meta_bytes: bytes,
    body: bytes,
) -> Result[ChildFrame, MalformedFrame]:
    """Build a child frame from received bytes. Used by the parent."""
    try:
        meta = _load_meta(meta_bytes)
        kind = _text(meta, 'kind')
        match kind:
            case 'ready':
                return Ok(ReadyFrame(pid=_whole(meta, 'pid'), rss=_rss(meta)))
            case 'result':
                return Ok(
                    ResultFrame(
                        task_id=_text(meta, 'task_id'),
                        ok=_flag(meta, 'ok'),
                        result_json=body.decode('utf-8'),
                        failed_reason=_optional_text(meta, 'failed_reason'),
                        rss=_rss(meta),
                    ),
                )
            case _:
                return Err(MalformedFrame(f'unknown child frame {kind!r}'))
    except (_BadField, ValueError) as exc:
        return Err(MalformedFrame(f'{type(exc).__name__}: {exc}'))
