"""Frame reads and writes of a worker child on its socket to the parent: the
reader of the version-2 child (``child_main.ParentFrameReader`` on
``core.FrameReader``) and its frame writes (``_send_child_frame``)."""

from __future__ import annotations

import json
import logging
import socket
import struct
import uuid
from typing import Final, cast

import pytest

from horsies import _core as core
from horsies.core.worker import child_main
from horsies.core.worker.child_frames import (
    NoTaskLoop,
    ParentFrame,
    ReadyFrame,
    ResultFrame,
    RssUnreadable,
    StartReported,
    encode_child_frame,
)
from horsies.core.worker.child_main import (
    BAD_PARENT_FRAME_EXIT_CODE,
    READ_CHUNK_BYTES,
    ParentFrameReader,
    _send_child_frame,  # pyright: ignore[reportPrivateUsage]
)
from horsies.core.worker.run_return import ResultEnvelope
from tests.fixtures.core_frames import framed, run_meta
from tests.fixtures.run_return import STARTED_AT

pytestmark = [pytest.mark.unit]

_HEADER: Final = struct.Struct('>IQ')
_MAX_META_BYTES: Final = 1 << 20
"""The bound of the metadata of a frame in the core reader."""


class _ScriptedSocket:
    """Serves ``data`` in reads of at most ``chunk`` bytes, then the end of
    the stream. Records the size that each read asked for, and each write."""

    def __init__(self, data: bytes, chunk: int) -> None:
        self._data = data
        self._at = 0
        self._chunk = chunk
        self.read_sizes: list[int] = []
        self.writes: list[bytes] = []

    def recv(self, size: int) -> bytes:
        self.read_sizes.append(size)
        count = min(size, self._chunk, len(self._data) - self._at)
        got = self._data[self._at : self._at + count]
        self._at += count
        return got

    def sendall(self, data: bytes) -> None:
        self.writes.append(bytes(data))


class _Exited(Exception):
    def __init__(self, code: int) -> None:
        super().__init__(code)
        self.code = code


def _raise_exit(code: int) -> None:
    raise _Exited(code)


def _task_id(index: int) -> uuid.UUID:
    return uuid.UUID(int=index + 1)


def _run_bytes(index: int, input_json: str = '{"job_id":1}') -> bytes:
    return framed(run_meta(task_id=_task_id(index)), input_json.encode('utf-8'))


def _stop_bytes() -> bytes:
    return framed({'kind': 'stop'})


type _Seen = tuple[uuid.UUID, str] | str
"""What a test compares of a frame: the task and the input of a run frame,
the kind of any other frame."""


def _seen(frame: ParentFrame) -> _Seen:
    match frame:
        case core.RunFrame(task_id=task_id, input=input_json):
            return (task_id, input_json)
        case core.StopFrame():
            return 'stop'
        case core.InitFrame():
            return 'init'
        case core.SharedRunFrame():
            return 'shared_run'
        case core.CancelRunFrame():
            return 'cancel_run'
        case core.EventsFrame():
            return 'events'


def _reader(data: bytes, chunk: int) -> tuple[ParentFrameReader, _ScriptedSocket]:
    scripted = _ScriptedSocket(data, chunk)
    return ParentFrameReader(cast('socket.socket', scripted)), scripted


def _large_input() -> str:
    return json.dumps({'blob': 'x' * (READ_CHUNK_BYTES * 3)})


def test_frames_that_arrive_together_are_read_in_one_read() -> None:
    reader, scripted = _reader(_run_bytes(0) + _run_bytes(1) + _stop_bytes(), chunk=1 << 20)

    received = [_seen(reader.next_frame()) for _ in range(3)]

    assert received == [
        (_task_id(0), '{"job_id":1}'),
        (_task_id(1), '{"job_id":1}'),
        'stop',
    ]
    assert len(scripted.read_sizes) == 1


@pytest.mark.parametrize('chunk', [1, 7, _HEADER.size, 4096, 1 << 20])
def test_every_chunking_gives_the_same_frames(chunk: int) -> None:
    large = _large_input()
    data = _run_bytes(0) + _run_bytes(1, large) + _run_bytes(2) + _stop_bytes()
    reader, _ = _reader(data, chunk)

    received = [_seen(reader.next_frame()) for _ in range(4)]

    assert received == [
        (_task_id(0), '{"job_id":1}'),
        (_task_id(1), large),
        (_task_id(2), '{"job_id":1}'),
        'stop',
    ]


@pytest.mark.parametrize('chunk', [997, 1 << 20])
def test_frames_that_cross_the_end_of_a_read_are_read_whole(chunk: int) -> None:
    """More small frames than one read takes: a frame cut by the end of a
    read is completed by the next read."""
    data = b''.join(_run_bytes(index) for index in range(1_000))
    assert len(data) > 2 * READ_CHUNK_BYTES
    reader, _ = _reader(data, chunk)

    received = [_seen(reader.next_frame()) for _ in range(1_000)]

    assert received == [(_task_id(index), '{"job_id":1}') for index in range(1_000)]


def test_each_read_asks_for_at_most_the_read_chunk() -> None:
    """A body larger than one read arrives in reads of ``READ_CHUNK_BYTES``
    at most, and the frame comes whole."""
    large = _large_input()
    reader, scripted = _reader(_run_bytes(0, large) + _run_bytes(1), chunk=1 << 20)

    received = [_seen(reader.next_frame()) for _ in range(2)]

    assert received == [(_task_id(0), large), (_task_id(1), '{"job_id":1}')]
    assert len(scripted.read_sizes) > 1
    assert set(scripted.read_sizes) == {READ_CHUNK_BYTES}


def test_a_parent_gone_in_the_middle_of_a_frame_is_the_end_of_the_stream() -> None:
    whole = _run_bytes(0)
    reader, _ = _reader(whole[: len(whole) - 1], chunk=1 << 20)

    with pytest.raises(EOFError):
        reader.next_frame()


def test_a_parent_gone_between_frames_is_the_end_of_the_stream() -> None:
    reader, _ = _reader(_run_bytes(0), chunk=1 << 20)
    assert _seen(reader.next_frame()) == (_task_id(0), '{"job_id":1}')

    with pytest.raises(EOFError):
        reader.next_frame()


def test_a_header_over_the_bounds_ends_the_process(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    monkeypatch.setattr(child_main.os, '_exit', _raise_exit)
    reader, _ = _reader(_HEADER.pack(_MAX_META_BYTES + 1, 0), chunk=1 << 20)

    with (
        caplog.at_level(logging.CRITICAL, logger='horsies.worker'),
        pytest.raises(_Exited) as exited,
    ):
        reader.next_frame()

    assert exited.value.code == BAD_PARENT_FRAME_EXIT_CODE
    assert [record.getMessage() for record in caplog.records] == [
        f'Parent sent a frame over the bounds: {_MAX_META_BYTES + 1} metadata '
        'bytes, 0 body bytes',
    ]


def test_a_run_frame_without_a_claim_time_ends_the_process(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    """The child fences every write of the attempt on the claim time, so a
    run frame without one is a bad parent frame."""
    monkeypatch.setattr(child_main.os, '_exit', _raise_exit)
    meta = run_meta(task_id=_task_id(0))
    del meta['claimed_at']
    reader, _ = _reader(framed(meta, b'{}'), chunk=1 << 20)

    with (
        caplog.at_level(logging.CRITICAL, logger='horsies.worker'),
        pytest.raises(_Exited) as exited,
    ):
        reader.next_frame()

    assert exited.value.code == BAD_PARENT_FRAME_EXIT_CODE
    assert [record.getMessage() for record in caplog.records] == [
        "Parent sent a bad frame: field 'claimed_at' is missing",
    ]


def test_a_small_child_frame_is_one_write() -> None:
    scripted = _ScriptedSocket(b'', chunk=1)
    frame = ReadyFrame(pid=1, rss=RssUnreadable())

    _send_child_frame(cast('socket.socket', scripted), frame)

    assert scripted.writes == [encode_child_frame(frame)]


def test_a_large_child_frame_is_one_write() -> None:
    """The child writes each frame in one write, also a frame whose body is
    larger than a read of the parent."""
    scripted = _ScriptedSocket(b'', chunk=1)
    frame = ResultFrame(
        task_id=_task_id(0),
        end=ResultEnvelope(json.dumps({'ok': 'x' * (READ_CHUNK_BYTES * 2)})),
        started=StartReported(STARTED_AT),
        rss=RssUnreadable(),
        task_loop=NoTaskLoop(),
    )

    _send_child_frame(cast('socket.socket', scripted), frame)

    assert scripted.writes == [encode_child_frame(frame)]
    assert len(scripted.writes[0]) > READ_CHUNK_BYTES * 2
