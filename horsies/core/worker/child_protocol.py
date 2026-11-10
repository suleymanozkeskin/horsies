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
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Final, TypeAlias, assert_never, cast

from horsies.core.codec.json_io import dumps_json, loads_json_bytes
from horsies.core.models.boot_failure import (
    AppLoadFailed,
    ChildPoolSetupFailed,
    ReportedBootCause,
    ResourceShortage,
    SpawnFailed,
    TaskLoopCreateFailed,
    ThreadStartRefused,
    boot_cause_json,
    cut_text,
    describe_boot_cause,
    parse_boot_cause,
)
from horsies.core.models.tasks import MAX_TASK_TIMEOUT_MS, MIN_TASK_TIMEOUT_MS
from horsies.core.types.result import Err, Ok, Result
from horsies.core.worker.stored_timeout import (
    NoTimeout,
    OptionsUnreadable,
    StoredTimeout,
    TaskTimeout,
    TimeoutValueInvalid,
)

FRAME_HEADER: Final = struct.Struct('>IQ')

# Metadata is a JSON object, so ``{}`` is the smallest valid value.
MIN_META_BYTES: Final = 2
MAX_META_BYTES: Final = 1 << 20
# Postgres cannot store a field of 1 GiB or more, so a larger result could
# never be saved. This bound changes no outcome: it only fails earlier.
MAX_BODY_BYTES: Final = (1 << 30) - 1
# A report of tasks left on the task loop names at most this many of them,
# each name cut to MAX_LEFTOVER_NAME_CHARS, so the report stays small.
MAX_LEFTOVER_TASK_NAMES: Final = 10
MAX_LEFTOVER_NAME_CHARS: Final = 200
# A boot failure frame names the exception type and its text within these
# bounds. With ': ' between them they fit MAX_BOOT_FAILURE_DETAIL_CHARS.
MAX_BOOT_ERROR_NAME_CHARS: Final = 200
MAX_BOOT_DETAIL_CHARS: Final = 2000


@dataclass(frozen=True, slots=True)
class InitFrame:
    """Parent to child, first frame: everything the child needs to boot.

    Carries the database URL here because argv is visible in ``ps``. The URL
    holds a password, so it stays out of the repr.
    """

    app_locator: str
    imports: tuple[str, ...]
    sys_path_roots: tuple[str, ...]
    parent_sys_path: tuple[str, ...]
    parent_argv: tuple[str, ...]
    loglevel: int
    database_url: str = field(repr=False)
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
    is_workflow_task: bool
    timeout: StoredTimeout
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
class BootFailedFrame:
    """Child to parent, in place of the ready frame: the boot failed.

    The child exits after this frame. ``cause`` decides how the parent
    retries. ``error_name`` and ``detail`` are the type and the text of the
    exception, at most ``MAX_BOOT_ERROR_NAME_CHARS`` and
    ``MAX_BOOT_DETAIL_CHARS`` characters. Build it with ``boot_failed_frame``,
    which cuts them to those bounds.
    """

    cause: ReportedBootCause
    error_name: str
    detail: str

    def describe(self) -> str:
        """Subject and cause, for the boot failure of the parent."""
        return f'{describe_boot_cause(self.cause)}: {self.error_name}: {self.detail}'


def boot_failed_frame(
    cause: ReportedBootCause,
    error_name: str,
    detail: str,
) -> BootFailedFrame:
    """Build a boot failure frame. Each text becomes valid UTF-8, cut to its bound."""
    return BootFailedFrame(
        cause=cause,
        error_name=cut_text(error_name, MAX_BOOT_ERROR_NAME_CHARS),
        detail=cut_text(detail, MAX_BOOT_DETAIL_CHARS),
    )


@dataclass(frozen=True, slots=True)
class NoTaskLoop:
    """After the task, the child has no task loop: its app has no
    ``async def`` task."""


@dataclass(frozen=True, slots=True)
class LoopHoldsNoTask:
    """After the task, the child's task loop holds no task that is not done."""


@dataclass(frozen=True, slots=True)
class LoopHoldsTasks:
    """After the task, asyncio tasks that it started and did not await are
    still on the child's task loop. The parent retires such a child.

    ``names`` holds the coroutine ``__qualname__`` of
    ``min(count, MAX_LEFTOVER_TASK_NAMES)`` of those tasks, sorted, each at
    most ``MAX_LEFTOVER_NAME_CHARS`` characters.
    """

    count: int
    names: tuple[str, ...]

    def names_text(self) -> str:
        """The names, and how many tasks they leave out, for a log line."""
        named = ', '.join(self.names)
        left_out = self.count - len(self.names)
        if left_out == 0:
            return named
        return f'{named}, and {left_out} more'


TaskLoopReport: TypeAlias = NoTaskLoop | LoopHoldsNoTask | LoopHoldsTasks


@dataclass(frozen=True, slots=True)
class ResultFrame:
    """Child to parent: the wire tuple of ``_run_task_entry``, plus the RSS
    and the state of the task loop after the task."""

    task_id: str
    ok: bool
    result_json: str
    failed_reason: str | None
    rss: RssReading
    task_loop: TaskLoopReport


@dataclass(frozen=True, slots=True)
class PreloadReadyFrame:
    """Preload parent to worker: imports are done, so forks can start."""

    pid: int


@dataclass(frozen=True, slots=True)
class ChildForkedFrame:
    """Preload parent to worker: the child exists, and it has left this parent."""

    pid: int


@dataclass(frozen=True, slots=True)
class ForkFailedFrame:
    """Preload parent to worker: this fork failed. The preload parent stays up."""

    reason: str


@dataclass(frozen=True, slots=True)
class PreloadRefusedFrame:
    """Preload parent to worker: it cannot fork safely, and it exits."""

    reason: str


ParentFrame: TypeAlias = InitFrame | RunFrame | StopFrame
ChildFrame: TypeAlias = ReadyFrame | BootFailedFrame | ResultFrame
PreloadFrame: TypeAlias = (
    PreloadReadyFrame | ChildForkedFrame | ForkFailedFrame | PreloadRefusedFrame
)


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


def _task_loop_field(task_loop: TaskLoopReport) -> dict[str, object]:
    match task_loop:
        case NoTaskLoop():
            return {'kind': 'none'}
        case LoopHoldsNoTask():
            return {'kind': 'empty'}
        case LoopHoldsTasks(count=count, names=names):
            return {'kind': 'held', 'count': count, 'names': list(names)}
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
            'is_workflow_task': frame.is_workflow_task,
            'timeout': _encode_timeout(frame.timeout),
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
        case BootFailedFrame():
            return _pack(
                {
                    'kind': 'boot_failed',
                    'cause': boot_cause_json(frame.cause),
                    'error_name': frame.error_name,
                    'detail': frame.detail,
                },
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
                    'task_loop': _task_loop_field(frame.task_loop),
                },
                frame.result_json.encode('utf-8'),
            )
        case _ as unreachable:
            assert_never(unreachable)


def encode_preload_frame(frame: PreloadFrame) -> EncodedFrame:
    """Encode a frame from the preload parent to the worker."""
    match frame:
        case PreloadReadyFrame():
            return _pack({'kind': 'preload_ready', 'pid': frame.pid}, b'')
        case ChildForkedFrame():
            return _pack({'kind': 'child_forked', 'pid': frame.pid}, b'')
        case ForkFailedFrame():
            return _pack({'kind': 'fork_failed', 'reason': frame.reason}, b'')
        case PreloadRefusedFrame():
            return _pack({'kind': 'preload_refused', 'reason': frame.reason}, b'')
        case _ as unreachable:
            assert_never(unreachable)


def parse_header(raw: bytes) -> Result[FrameSizes, FrameOverBound]:
    """Check a header against the bounds, before any buffer is allocated.

    ``raw`` must be ``FRAME_HEADER.size`` bytes: the caller reads exactly that.
    """
    meta_bytes, body_bytes = FRAME_HEADER.unpack(raw)
    if not MIN_META_BYTES <= meta_bytes <= MAX_META_BYTES or body_bytes > MAX_BODY_BYTES:
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


def _encode_timeout(timeout: StoredTimeout) -> dict[str, object]:
    match timeout:
        case NoTimeout():
            return {'kind': 'none'}
        case TaskTimeout(ms=ms):
            return {'kind': 'ms', 'ms': ms}
        case OptionsUnreadable(reason=reason):
            return {'kind': 'options_unreadable', 'reason': reason}
        case TimeoutValueInvalid(reason=reason):
            return {'kind': 'value_invalid', 'reason': reason}
        case _ as unreachable:
            assert_never(unreachable)


def _decode_timeout(meta: Mapping[str, Any]) -> StoredTimeout:
    value = meta.get('timeout')
    if not isinstance(value, dict):
        raise _BadField('timeout')
    timeout = cast('Mapping[str, Any]', value)
    match timeout.get('kind'):
        case 'none':
            return NoTimeout()
        case 'ms':
            ms = timeout.get('ms')
            if (
                not isinstance(ms, int)
                or isinstance(ms, bool)
                or not MIN_TASK_TIMEOUT_MS <= ms <= MAX_TASK_TIMEOUT_MS
            ):
                raise _BadField('timeout.ms')
            return TaskTimeout(ms=ms)
        case 'options_unreadable':
            return OptionsUnreadable(reason=_text(timeout, 'reason'))
        case 'value_invalid':
            return TimeoutValueInvalid(reason=_text(timeout, 'reason'))
        case _:
            raise _BadField('timeout.kind')


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


def _leftover_names(task_loop: Mapping[str, Any], count: int) -> tuple[str, ...]:
    """One name for each leftover task up to the bound, each within its bound."""
    names = _text_tuple(task_loop, 'names')
    if len(names) != min(count, MAX_LEFTOVER_TASK_NAMES):
        raise _BadField('task_loop.names')
    if any(len(name) > MAX_LEFTOVER_NAME_CHARS for name in names):
        raise _BadField('task_loop.names')
    return names


def _task_loop(meta: Mapping[str, Any]) -> TaskLoopReport:
    value = meta.get('task_loop')
    if not isinstance(value, dict):
        raise _BadField('task_loop')
    task_loop = cast('Mapping[str, Any]', value)
    match task_loop.get('kind'):
        case 'none':
            return NoTaskLoop()
        case 'empty':
            return LoopHoldsNoTask()
        case 'held':
            count = task_loop.get('count')
            if not isinstance(count, int) or isinstance(count, bool) or count < 1:
                raise _BadField('task_loop.count')
            names = _leftover_names(task_loop, count)
            return LoopHoldsTasks(count=count, names=names)
        case _:
            raise _BadField('task_loop.kind')


def _bounded_text(meta: Mapping[str, Any], key: str, limit: int) -> str:
    value = _text(meta, key)
    if len(value) > limit:
        raise _BadField(key)
    return value


def _reported_cause(meta: Mapping[str, Any]) -> ReportedBootCause:
    """A cause that a child can report. A spawn failure is the parent's to see."""
    match parse_boot_cause(meta.get('cause')):
        case Ok(cause):
            pass
        case Err(unreadable):
            raise _BadField(f'cause: {unreadable.reason}')
    match cause:
        case SpawnFailed():
            raise _BadField('cause: a child cannot report a spawn failure')
        case (
            AppLoadFailed()
            | ChildPoolSetupFailed()
            | ResourceShortage()
            | TaskLoopCreateFailed()
            | ThreadStartRefused()
        ):
            return cause
        case _ as unreachable:
            assert_never(unreachable)


def _decode_boot_failed(meta: Mapping[str, Any]) -> BootFailedFrame:
    return BootFailedFrame(
        cause=_reported_cause(meta),
        error_name=_bounded_text(meta, 'error_name', MAX_BOOT_ERROR_NAME_CHARS),
        detail=_bounded_text(meta, 'detail', MAX_BOOT_DETAIL_CHARS),
    )


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
        is_workflow_task=_flag(meta, 'is_workflow_task'),
        timeout=_decode_timeout(meta),
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
    except (_BadField, ValueError, OverflowError, RecursionError) as exc:
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
            case 'boot_failed':
                return Ok(_decode_boot_failed(meta))
            case 'result':
                return Ok(
                    ResultFrame(
                        task_id=_text(meta, 'task_id'),
                        ok=_flag(meta, 'ok'),
                        result_json=body.decode('utf-8'),
                        failed_reason=_optional_text(meta, 'failed_reason'),
                        rss=_rss(meta),
                        task_loop=_task_loop(meta),
                    ),
                )
            case _:
                return Err(MalformedFrame(f'unknown child frame {kind!r}'))
    except (_BadField, ValueError, OverflowError, RecursionError) as exc:
        return Err(MalformedFrame(f'{type(exc).__name__}: {exc}'))


def decode_preload_frame(meta_bytes: bytes) -> Result[PreloadFrame, MalformedFrame]:
    """Build a preload frame from received bytes. Preload frames have no body."""
    try:
        meta = _load_meta(meta_bytes)
        kind = _text(meta, 'kind')
        match kind:
            case 'preload_ready':
                return Ok(PreloadReadyFrame(pid=_whole(meta, 'pid')))
            case 'child_forked':
                return Ok(ChildForkedFrame(pid=_whole(meta, 'pid')))
            case 'fork_failed':
                return Ok(ForkFailedFrame(reason=_text(meta, 'reason')))
            case 'preload_refused':
                return Ok(PreloadRefusedFrame(reason=_text(meta, 'reason')))
            case _:
                return Err(MalformedFrame(f'unknown preload frame {kind!r}'))
    except (_BadField, ValueError) as exc:
        return Err(MalformedFrame(f'{type(exc).__name__}: {exc}'))
