"""Frame reads and writes of a worker child on its socket to the parent."""

from __future__ import annotations

import json
import socket
from typing import cast

import pytest

from horsies.core.worker import child_main
from horsies.core.worker.child_main import (
    BAD_PARENT_FRAME_EXIT_CODE,
    CONTROL_DIRECT_BODY_BYTES,
    CONTROL_READ_BUFFER_BYTES,
    JOINED_CHILD_WRITE_BODY_BYTES,
    ParentFrameReader,
    _send_child_frame,  # pyright: ignore[reportPrivateUsage]
)
from horsies.core.worker.child_protocol import (
    FRAME_HEADER,
    MAX_META_BYTES,
    NoTaskLoop,
    ParentFrame,
    ReadyFrame,
    ResultFrame,
    RssUnreadable,
    RunFrame,
    StopFrame,
    encode_parent_frame,
)
from horsies.core.worker.stored_timeout import NoTimeout

pytestmark = [pytest.mark.unit]


class _ScriptedSocket:
    """Serves ``data`` in reads of at most ``chunk`` bytes, then the end of
    the stream. Records each write."""

    def __init__(self, data: bytes, chunk: int) -> None:
        self._data = data
        self._at = 0
        self._chunk = chunk
        self.reads = 0
        self.writes: list[bytes] = []

    def recv_into(self, view: memoryview) -> int:
        self.reads += 1
        count = min(len(view), self._chunk, len(self._data) - self._at)
        view[:count] = self._data[self._at : self._at + count]
        self._at += count
        return count

    def sendall(self, data: bytes) -> None:
        self.writes.append(bytes(data))


class _Exited(Exception):
    def __init__(self, code: int) -> None:
        super().__init__(code)
        self.code = code


def _run(task_id: str, kwargs_json: str | None = '{"job_id":1}') -> RunFrame:
    return RunFrame(
        task_id=task_id,
        task_name='frame_io_test',
        args_json=None,
        kwargs_json=kwargs_json,
        worker_id='worker-1',
        is_workflow_task=False,
        timeout=NoTimeout(),
        claimed_at=None,
    )


def _wire(frames: list[ParentFrame]) -> bytes:
    encoded = [encode_parent_frame(frame) for frame in frames]
    return b''.join(part.head + part.body for part in encoded)


def _reader(data: bytes, chunk: int) -> tuple[ParentFrameReader, _ScriptedSocket]:
    scripted = _ScriptedSocket(data, chunk)
    return ParentFrameReader(cast('socket.socket', scripted)), scripted


def _large_kwargs() -> str:
    return json.dumps({'blob': 'x' * (CONTROL_DIRECT_BODY_BYTES * 3)})


def test_frames_that_arrive_together_are_read_in_one_read() -> None:
    frames: list[ParentFrame] = [_run('a'), _run('b'), StopFrame()]
    reader, scripted = _reader(_wire(frames), chunk=1 << 20)

    received = [reader.next_frame() for _ in frames]

    assert received == frames
    assert scripted.reads == 1


@pytest.mark.parametrize('chunk', [1, 7, FRAME_HEADER.size, 4096, 1 << 20])
def test_every_chunking_gives_the_same_frames(chunk: int) -> None:
    frames: list[ParentFrame] = [_run('a'), _run('b', _large_kwargs()), _run('c'), StopFrame()]
    reader, _ = _reader(_wire(frames), chunk)

    received = [reader.next_frame() for _ in frames]

    assert received == frames


@pytest.mark.parametrize('chunk', [997, 1 << 20])
def test_frames_that_cross_the_end_of_the_read_buffer_are_read_whole(chunk: int) -> None:
    """More small frames than one read buffer holds: a frame cut by the end
    of the buffer moves to its front and is completed by the next read."""
    frames: list[ParentFrame] = [_run(f'task-{index}') for index in range(1_000)]
    wire = _wire(frames)
    assert len(wire) > 2 * CONTROL_READ_BUFFER_BYTES
    reader, _ = _reader(wire, chunk)

    received = [reader.next_frame() for _ in frames]

    assert received == frames


def test_a_large_body_does_not_grow_the_read_buffer() -> None:
    frames: list[ParentFrame] = [_run('a', _large_kwargs()), _run('b')]
    reader, _ = _reader(_wire(frames), chunk=1 << 20)

    received = [reader.next_frame() for _ in frames]

    assert received == frames
    assert len(reader._buffer) == CONTROL_READ_BUFFER_BYTES  # pyright: ignore[reportPrivateUsage]


def test_a_parent_gone_in_the_middle_of_a_frame_is_the_end_of_the_stream() -> None:
    whole = _wire([_run('a')])
    reader, _ = _reader(whole[: len(whole) - 1], chunk=1 << 20)

    with pytest.raises(EOFError):
        reader.next_frame()


def test_a_parent_gone_between_frames_is_the_end_of_the_stream() -> None:
    reader, _ = _reader(_wire([_run('a')]), chunk=1 << 20)
    assert reader.next_frame() == _run('a')

    with pytest.raises(EOFError):
        reader.next_frame()


def test_a_header_over_the_bounds_ends_the_process(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def _exit(code: int) -> None:
        raise _Exited(code)

    monkeypatch.setattr(child_main.os, '_exit', _exit)
    reader, _ = _reader(FRAME_HEADER.pack(MAX_META_BYTES + 1, 0), chunk=1 << 20)

    with pytest.raises(_Exited) as exited:
        reader.next_frame()

    assert exited.value.code == BAD_PARENT_FRAME_EXIT_CODE


def test_a_small_child_frame_is_one_write() -> None:
    scripted = _ScriptedSocket(b'', chunk=1)

    _send_child_frame(cast('socket.socket', scripted), ReadyFrame(pid=1, rss=RssUnreadable()))

    assert len(scripted.writes) == 1


def test_a_large_child_frame_body_is_written_on_its_own() -> None:
    scripted = _ScriptedSocket(b'', chunk=1)
    frame = ResultFrame(
        task_id='a',
        ok=True,
        result_json=json.dumps({'ok': 'x' * JOINED_CHILD_WRITE_BODY_BYTES}),
        failed_reason=None,
        rss=RssUnreadable(),
        task_loop=NoTaskLoop(),
    )

    _send_child_frame(cast('socket.socket', scripted), frame)

    assert len(scripted.writes) == 2
    assert len(scripted.writes[1]) > JOINED_CHILD_WRITE_BODY_BYTES
