"""Version-2 parent frames for tests of the Python child.

The core gives a child its parent frames as core classes, which Python
cannot build: each helper here writes the wire bytes of one frame (the
``>IQ`` header, the JSON metadata, the body) and decodes them with
``core.decode_parent_frame``, as a child receives them. ``framed`` gives the
bytes, for tests of the frame reader.
"""

from __future__ import annotations

import json
import struct
import uuid
from datetime import UTC, datetime
from typing import Final, Literal

from horsies import _core as core

_HEADER: Final = struct.Struct('>IQ')

CLAIMED_AT: Final = datetime(2026, 11, 17, 10, 0, 0, 123456, tzinfo=UTC)
WORKER_ID: Final = 'worker-1'

type RefusalKind = Literal['options_unreadable', 'value_invalid', 'grace_invalid']


def framed(meta: dict[str, object], body: bytes = b'') -> bytes:
    """The wire bytes of a frame: the header, the metadata and the body."""
    meta_bytes = json.dumps(meta, separators=(',', ':')).encode('utf-8')
    return _HEADER.pack(len(meta_bytes), len(body)) + meta_bytes + body


def _decoded(meta: dict[str, object], body: bytes = b'') -> object:
    meta_bytes = json.dumps(meta, separators=(',', ':')).encode('utf-8')
    return core.decode_parent_frame(meta_bytes, body)


def refusal(kind: RefusalKind, reason: str) -> dict[str, object]:
    """The work of a run whose stored options the worker refused."""
    return {'kind': 'refusal', 'refusal': {'kind': kind, 'reason': reason}}


def run_meta(
    *,
    task_id: uuid.UUID,
    task_name: str = 'task',
    worker_id: str = WORKER_ID,
    claimed_at: datetime = CLAIMED_AT,
    orphan_self_heal: bool = False,
    work: dict[str, object] | None = None,
) -> dict[str, object]:
    """The metadata of an isolated run frame; ``work`` defaults to the run."""
    return {
        'kind': 'run',
        'task_id': str(task_id),
        'task_name': task_name,
        'worker_id': worker_id,
        'claimed_at': claimed_at.isoformat(),
        'orphan_self_heal': orphan_self_heal,
        'work': work if work is not None else {'kind': 'run'},
    }


def run_frame(
    *,
    task_id: uuid.UUID,
    task_name: str = 'task',
    worker_id: str = WORKER_ID,
    claimed_at: datetime = CLAIMED_AT,
    orphan_self_heal: bool = False,
    work: dict[str, object] | None = None,
    input_json: str = '{}',
) -> core.RunFrame:
    """An isolated run frame, as the core decodes it."""
    frame = _decoded(
        run_meta(
            task_id=task_id,
            task_name=task_name,
            worker_id=worker_id,
            claimed_at=claimed_at,
            orphan_self_heal=orphan_self_heal,
            work=work,
        ),
        input_json.encode('utf-8'),
    )
    assert isinstance(frame, core.RunFrame), frame
    return frame


def async_work(timeout_ms: int, cancel_grace_ms: int) -> dict[str, object]:
    """The work of a run on a seat of a shared async child."""
    return {'kind': 'task', 'timeout_ms': timeout_ms, 'cancel_grace_ms': cancel_grace_ms}


def thread_work(timeout_ms: int) -> dict[str, object]:
    """The work of a run on a seat of a shared thread child."""
    return {'kind': 'thread_task', 'timeout_ms': timeout_ms}


def shared_run_meta(
    *,
    dispatch: int,
    task_id: uuid.UUID,
    work: dict[str, object],
    task_name: str = 'task',
    worker_id: str = WORKER_ID,
    claimed_at: datetime = CLAIMED_AT,
    orphan_self_heal: bool = False,
) -> dict[str, object]:
    """The metadata of a shared run frame."""
    return {
        'kind': 'shared_run',
        'dispatch': dispatch,
        'task_id': str(task_id),
        'task_name': task_name,
        'worker_id': worker_id,
        'claimed_at': claimed_at.isoformat(),
        'orphan_self_heal': orphan_self_heal,
        'work': work,
    }


def shared_run_frame(
    *,
    dispatch: int,
    task_id: uuid.UUID,
    work: dict[str, object],
    task_name: str = 'task',
    worker_id: str = WORKER_ID,
    claimed_at: datetime = CLAIMED_AT,
    orphan_self_heal: bool = False,
    input_json: str = '{}',
) -> core.SharedRunFrame:
    """A shared run frame, as the core decodes it."""
    frame = _decoded(
        shared_run_meta(
            dispatch=dispatch,
            task_id=task_id,
            work=work,
            task_name=task_name,
            worker_id=worker_id,
            claimed_at=claimed_at,
            orphan_self_heal=orphan_self_heal,
        ),
        input_json.encode('utf-8'),
    )
    assert isinstance(frame, core.SharedRunFrame), frame
    return frame


def stop_frame() -> core.StopFrame:
    """The stop frame, as the core decodes it."""
    frame = _decoded({'kind': 'stop'})
    assert isinstance(frame, core.StopFrame), frame
    return frame


def pool(min_size: int = 0, max_size: int = 2, check: bool = False) -> dict[str, object]:
    """The pool of a child kind."""
    return {'min_size': min_size, 'max_size': max_size, 'check': check}


def isolated_kind(
    task_send_pool: int = 1,
    child_pool: dict[str, object] | None = None,
) -> dict[str, object]:
    """An isolated child kind; ``child_pool`` defaults to ``pool()``."""
    return {
        'kind': 'isolated',
        'pool': child_pool if child_pool is not None else pool(),
        'task_send_pool': task_send_pool,
    }


def shared_async_kind(seats: int = 2, executor_threads: int = 2) -> dict[str, object]:
    return {
        'kind': 'shared_async',
        'seats': seats,
        'pool': pool(),
        'executor_threads': executor_threads,
        'task_send_pool': 1,
    }


def shared_thread_kind(seats: int = 2) -> dict[str, object]:
    return {'kind': 'shared_thread', 'seats': seats, 'pool': pool(), 'task_send_pool': 1}


def init_meta(
    *,
    child_kind: dict[str, object] | None = None,
    app_locator: str = 'app.main:app',
    imports: tuple[str, ...] = (),
    log_level: str = 'info',
    database_url: str = 'postgresql://user:secret@127.0.0.1:1/none',
    protocol: int = 2,
) -> dict[str, object]:
    """The metadata of an init frame of a Python child."""
    return {
        'kind': 'init',
        'protocol': protocol,
        'program': {
            'kind': 'python',
            'app_locator': app_locator,
            'imports': list(imports),
            'sys_path_roots': [],
            'parent_sys_path': [],
            'parent_argv': ['horsies', 'worker'],
        },
        'log_level': log_level,
        'database_url': database_url,
        'connection': {
            'tcp_keepalives': {'kind': 'off'},
            'tcp_user_timeout': {'kind': 'system_default'},
            'server_sessions': {'kind': 'kept'},
        },
        'process_name': 'horsies-child',
        'child_kind': child_kind if child_kind is not None else isolated_kind(),
    }


def init_frame(
    *,
    child_kind: dict[str, object] | None = None,
    app_locator: str = 'app.main:app',
    imports: tuple[str, ...] = (),
    log_level: str = 'info',
    database_url: str = 'postgresql://user:secret@127.0.0.1:1/none',
) -> core.InitFrame:
    """An init frame of a Python child, as the core decodes it."""
    frame = _decoded(
        init_meta(
            child_kind=child_kind,
            app_locator=app_locator,
            imports=imports,
            log_level=log_level,
            database_url=database_url,
        ),
    )
    assert isinstance(frame, core.InitFrame), frame
    return frame
