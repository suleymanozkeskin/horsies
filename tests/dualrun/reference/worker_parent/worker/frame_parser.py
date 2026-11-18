"""Parse a stream of frames in a read buffer, one step at a time.

Both frame readers use these rules: ``slots.FrameProtocol`` reads the child
frames in the worker process, and ``child_main.ParentFrameReader`` reads the
parent frames in a child. A reader keeps a buffer and the range of bytes at
hand in it, from ``start`` to ``end``. ``next_step`` names what those bytes
hold next, and ``plan_read`` names where the next read goes. The reader does
the reads, the copies and the moves.

This module is pure. It does no I/O, changes no buffer, and holds no state.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Final, TypeAlias

from tests.dualrun.reference.worker_parent.worker.child_protocol import (
    FRAME_HEADER,
    MAX_META_BYTES,
    FrameOverBound,
    sizes_in_bounds,
)

FRAME_READ_BUFFER_BYTES: Final = 1 << 16
"""The first size of the read buffer of a frame reader."""

DIRECT_BODY_BYTES: Final = 1 << 16
"""A frame body larger than this is read straight into a buffer of its own
size, so a large body is not first copied into the read buffer."""

READ_BUFFER_BOUND: Final = FRAME_HEADER.size + MAX_META_BYTES + DIRECT_BODY_BYTES
"""The largest size of the read buffer: a header, the largest metadata, and a
body that is not read straight into its own buffer."""


@dataclass(frozen=True, slots=True)
class NeedBytes:
    """The next step needs ``wanted`` bytes from ``start``; fewer are at hand.

    ``wanted`` is a header until the header is at hand. Then it is the header
    and the metadata of a frame whose body is read directly, else the whole
    frame. It is at most ``READ_BUFFER_BOUND``.
    """

    wanted: int


NEED_HEADER: Final = NeedBytes(wanted=FRAME_HEADER.size)
"""The step until a whole header is at hand, also for an empty buffer."""


@dataclass(frozen=True, slots=True)
class WholeFrame:
    """A whole frame is at hand: the metadata is ``buffer[meta_at:body_at]``
    and the body is ``buffer[body_at:frame_end]``. The next frame starts at
    ``frame_end``."""

    meta_at: int
    body_at: int
    frame_end: int


@dataclass(frozen=True, slots=True)
class ReadBodyDirectly:
    """A body over ``DIRECT_BODY_BYTES`` that is not fully at hand.

    The metadata ``buffer[meta_at:body_at]`` is at hand. The bytes from
    ``body_at`` to ``end`` are the first part of the body, which has
    ``body_bytes`` bytes. The reader reads the rest straight into a buffer of
    that size.
    """

    meta_at: int
    body_at: int
    body_bytes: int


FrameStep: TypeAlias = NeedBytes | WholeFrame | ReadBodyDirectly | FrameOverBound
"""The next step for the bytes at hand. ``FrameOverBound``: the header at
``start`` declares sizes over the bounds, and nothing is allocated for its
frame."""


def next_step(buffer: bytes | bytearray, start: int, end: int) -> FrameStep:
    """The next step for ``buffer[start:end]``: bytes at hand that start at a
    frame boundary. Reads ``buffer`` only."""
    meta_at = start + FRAME_HEADER.size
    if end < meta_at:
        return NEED_HEADER
    meta_bytes: int
    body_bytes: int
    meta_bytes, body_bytes = FRAME_HEADER.unpack_from(buffer, start)
    if not sizes_in_bounds(meta_bytes, body_bytes):
        return FrameOverBound(meta_bytes=meta_bytes, body_bytes=body_bytes)
    body_at = meta_at + meta_bytes
    frame_end = body_at + body_bytes
    if end >= frame_end:
        return WholeFrame(meta_at=meta_at, body_at=body_at, frame_end=frame_end)
    if body_bytes <= DIRECT_BODY_BYTES:
        return NeedBytes(wanted=frame_end - start)
    if end < body_at:
        return NeedBytes(wanted=body_at - start)
    return ReadBodyDirectly(meta_at=meta_at, body_at=body_at, body_bytes=body_bytes)


@dataclass(frozen=True, slots=True)
class ReadIntoTail:
    """The bytes that the step needs fit from ``start``: read into the free
    tail after ``end``."""


@dataclass(frozen=True, slots=True)
class ReadFromFront:
    """No byte is at hand: read from the front of the buffer."""


@dataclass(frozen=True, slots=True)
class MoveToFront:
    """The bytes that the step needs do not fit from ``start``: move the bytes
    at hand to the front of the buffer, then read into the free tail."""


@dataclass(frozen=True, slots=True)
class MoveAndGrow:
    """The buffer is smaller than the bytes that the step needs: move the bytes
    at hand to the front, grow the buffer to ``buffer_bytes``, then read into
    the free tail."""

    buffer_bytes: int


ReadPlan: TypeAlias = ReadIntoTail | ReadFromFront | MoveToFront | MoveAndGrow

READ_INTO_TAIL: Final = ReadIntoTail()
READ_FROM_FRONT: Final = ReadFromFront()
MOVE_TO_FRONT: Final = MoveToFront()


def plan_read(buffer_bytes: int, start: int, end: int, wanted: int) -> ReadPlan:
    """Where the next read goes in a buffer of ``buffer_bytes`` when the step
    needs ``wanted`` bytes from ``start`` (``NeedBytes.wanted``).

    The buffer grows only to ``wanted``. ``wanted`` is more than the bytes at
    hand and at most ``READ_BUFFER_BOUND``: ``next_step`` makes both true.
    """
    assert end - start < wanted <= READ_BUFFER_BOUND, (
        f'a read for {wanted} bytes with {end - start} bytes at hand is out of '
        f'the range above the bytes at hand and up to {READ_BUFFER_BOUND} bytes'
    )
    if buffer_bytes < wanted:
        return MoveAndGrow(buffer_bytes=wanted)
    if start == end:
        return READ_FROM_FRONT
    if buffer_bytes - start < wanted:
        return MOVE_TO_FRONT
    return READ_INTO_TAIL
