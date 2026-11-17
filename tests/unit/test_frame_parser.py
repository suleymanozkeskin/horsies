"""The frame parser that both frame readers use: steps and read plans."""

from __future__ import annotations

from typing import assert_never

import pytest

from tests.dualrun.reference.worker_parent.worker.child_protocol import (
    FRAME_HEADER,
    MAX_BODY_BYTES,
    MAX_META_BYTES,
    MIN_META_BYTES,
    FrameOverBound,
)
from tests.dualrun.reference.worker_parent.worker.frame_parser import (
    DIRECT_BODY_BYTES,
    FRAME_READ_BUFFER_BYTES,
    MOVE_TO_FRONT,
    NEED_HEADER,
    READ_BUFFER_BOUND,
    READ_FROM_FRONT,
    READ_INTO_TAIL,
    FrameStep,
    MoveAndGrow,
    NeedBytes,
    ReadBodyDirectly,
    WholeFrame,
    next_step,
    plan_read,
)

pytestmark = [pytest.mark.unit]

HEADER = FRAME_HEADER.size


def _frame(meta_bytes: int, body_bytes: int) -> bytes:
    """A frame with ``meta_bytes`` of metadata and ``body_bytes`` of body. The
    parser does not decode the metadata, so its bytes need not be JSON."""
    return (
        FRAME_HEADER.pack(meta_bytes, body_bytes)
        + b'm' * meta_bytes
        + b'b' * body_bytes
    )


def _steps(buffer: bytes | bytearray, start: int, end: int) -> list[FrameStep]:
    """Every step from ``start``, as a reader takes them: past each whole
    frame, up to the first step that is not a whole frame."""
    steps: list[FrameStep] = []
    for _ in range(end - start + 1):
        step = next_step(buffer, start, end)
        steps.append(step)
        match step:
            case WholeFrame(frame_end=frame_end):
                start = frame_end
            case NeedBytes() | ReadBodyDirectly() | FrameOverBound():
                return steps
            case _ as unreachable:
                assert_never(unreachable)
    raise AssertionError(f'no stop after {len(steps)} steps')


class TestHeader:
    def test_an_empty_buffer_needs_a_header(self) -> None:
        assert next_step(bytearray(FRAME_READ_BUFFER_BYTES), 0, 0) == NEED_HEADER

    @pytest.mark.parametrize('at_hand', range(1, HEADER))
    def test_a_split_header_needs_the_whole_header(self, at_hand: int) -> None:
        frame = _frame(10, 5)

        assert next_step(frame, 0, at_hand) == NeedBytes(wanted=HEADER)

    def test_a_whole_header_names_the_size_of_its_frame(self) -> None:
        frame = _frame(10, 5)

        assert next_step(frame, 0, HEADER) == NeedBytes(wanted=len(frame))


class TestSmallBody:
    def test_split_metadata_needs_the_whole_frame(self) -> None:
        frame = _frame(10, 5)

        assert next_step(frame, 0, HEADER + 4) == NeedBytes(wanted=len(frame))

    def test_a_split_body_needs_the_whole_frame(self) -> None:
        frame = _frame(10, 5)

        assert next_step(frame, 0, len(frame) - 1) == NeedBytes(wanted=len(frame))

    def test_a_whole_frame_gives_where_its_parts_are(self) -> None:
        frame = _frame(10, 5)

        step = next_step(frame, 0, len(frame))

        assert step == WholeFrame(
            meta_at=HEADER, body_at=HEADER + 10, frame_end=len(frame)
        )
        assert frame[HEADER : HEADER + 10] == b'm' * 10
        assert frame[HEADER + 10 : len(frame)] == b'b' * 5

    def test_an_empty_body_is_a_whole_frame_after_its_metadata(self) -> None:
        frame = _frame(MIN_META_BYTES, 0)

        assert next_step(frame, 0, len(frame)) == WholeFrame(
            meta_at=HEADER,
            body_at=HEADER + MIN_META_BYTES,
            frame_end=len(frame),
        )

    def test_a_frame_after_the_front_of_the_buffer_counts_from_start(self) -> None:
        start = 100
        buffer = b'x' * start + _frame(10, 5)

        assert next_step(buffer, start, len(buffer)) == WholeFrame(
            meta_at=start + HEADER,
            body_at=start + HEADER + 10,
            frame_end=len(buffer),
        )
        assert next_step(buffer, start, len(buffer) - 1) == NeedBytes(
            wanted=len(buffer) - start,
        )


class TestDirectBodyThreshold:
    def test_a_body_at_the_threshold_is_read_into_the_read_buffer(self) -> None:
        frame = _frame(10, DIRECT_BODY_BYTES)

        assert next_step(frame, 0, len(frame) - 1) == NeedBytes(wanted=len(frame))

    def test_a_whole_frame_with_a_body_at_the_threshold_is_whole(self) -> None:
        frame = _frame(10, DIRECT_BODY_BYTES)

        assert next_step(frame, 0, len(frame)) == WholeFrame(
            meta_at=HEADER,
            body_at=HEADER + 10,
            frame_end=len(frame),
        )

    def test_a_body_above_the_threshold_needs_only_the_header_and_metadata(
        self,
    ) -> None:
        frame = _frame(10, DIRECT_BODY_BYTES + 1)

        assert next_step(frame, 0, HEADER + 9) == NeedBytes(wanted=HEADER + 10)

    def test_a_body_above_the_threshold_is_read_directly_after_its_metadata(
        self,
    ) -> None:
        frame = _frame(10, DIRECT_BODY_BYTES + 1)

        step = next_step(frame, 0, HEADER + 10)

        assert step == ReadBodyDirectly(
            meta_at=HEADER,
            body_at=HEADER + 10,
            body_bytes=DIRECT_BODY_BYTES + 1,
        )

    def test_a_body_above_the_threshold_is_read_directly_with_part_at_hand(
        self,
    ) -> None:
        frame = _frame(10, DIRECT_BODY_BYTES + 1)

        step = next_step(frame, 0, len(frame) - 1)

        assert step == ReadBodyDirectly(
            meta_at=HEADER,
            body_at=HEADER + 10,
            body_bytes=DIRECT_BODY_BYTES + 1,
        )

    def test_a_body_above_the_threshold_that_is_whole_at_hand_is_a_whole_frame(
        self,
    ) -> None:
        frame = _frame(10, DIRECT_BODY_BYTES + 1)

        assert next_step(frame, 0, len(frame)) == WholeFrame(
            meta_at=HEADER,
            body_at=HEADER + 10,
            frame_end=len(frame),
        )


class TestBounds:
    @pytest.mark.parametrize(
        ('meta_bytes', 'body_bytes'),
        [
            (MIN_META_BYTES - 1, 0),
            (MAX_META_BYTES + 1, 0),
            (MIN_META_BYTES, MAX_BODY_BYTES + 1),
        ],
    )
    def test_a_header_over_the_bounds_is_refused(
        self,
        meta_bytes: int,
        body_bytes: int,
    ) -> None:
        header = FRAME_HEADER.pack(meta_bytes, body_bytes)

        step = next_step(header, 0, HEADER)

        assert step == FrameOverBound(meta_bytes=meta_bytes, body_bytes=body_bytes)

    def test_a_header_at_the_bounds_is_not_refused(self) -> None:
        header = FRAME_HEADER.pack(MAX_META_BYTES, MAX_BODY_BYTES)

        step = next_step(header, 0, HEADER)

        assert step == NeedBytes(wanted=HEADER + MAX_META_BYTES)

    def test_the_largest_frame_in_the_read_buffer_needs_the_buffer_bound(self) -> None:
        header = FRAME_HEADER.pack(MAX_META_BYTES, DIRECT_BODY_BYTES)

        step = next_step(header, 0, HEADER)

        assert step == NeedBytes(wanted=READ_BUFFER_BOUND)

    def test_growth_for_the_largest_frame_stops_at_the_buffer_bound(self) -> None:
        plan = plan_read(FRAME_READ_BUFFER_BYTES, 0, HEADER, READ_BUFFER_BOUND)

        assert plan == MoveAndGrow(buffer_bytes=READ_BUFFER_BOUND)


class TestManyFrames:
    def test_every_whole_frame_of_one_buffer_is_a_step_in_order(self) -> None:
        sizes = [(10, 5), (MIN_META_BYTES, 0), (30, 300), (12, 1)]
        frames = [_frame(meta_bytes, body_bytes) for meta_bytes, body_bytes in sizes]
        partial = _frame(10, 5)[: HEADER + 3]
        buffer = b''.join(frames) + partial

        steps = _steps(buffer, 0, len(buffer))

        ends: list[int] = []
        at = 0
        for frame in frames:
            at += len(frame)
            ends.append(at)
        assert steps[:-1] == [
            WholeFrame(
                meta_at=end - len(frame) + HEADER,
                body_at=end - body_bytes,
                frame_end=end,
            )
            for end, frame, (_, body_bytes) in zip(ends, frames, sizes, strict=True)
        ]
        assert steps[-1] == NeedBytes(wanted=len(_frame(10, 5)))

    def test_a_buffer_of_whole_frames_ends_with_a_header_step(self) -> None:
        buffer = _frame(10, 5) * 3

        steps = _steps(buffer, 0, len(buffer))

        assert len(steps) == 4
        assert steps[-1] == NEED_HEADER

    def test_a_bad_header_after_whole_frames_stops_after_them(self) -> None:
        good = _frame(10, 5)
        bad = FRAME_HEADER.pack(MAX_META_BYTES + 1, 0)
        buffer = good * 2 + bad

        steps = _steps(buffer, 0, len(buffer))

        second_at = len(good)
        assert len(steps) == 3
        assert steps[1] == WholeFrame(
            meta_at=second_at + HEADER,
            body_at=second_at + HEADER + 10,
            frame_end=2 * len(good),
        )
        assert steps[2] == FrameOverBound(meta_bytes=MAX_META_BYTES + 1, body_bytes=0)

    def test_the_parser_does_not_change_the_buffer(self) -> None:
        buffer = bytearray(_frame(10, 5) * 2 + _frame(10, DIRECT_BODY_BYTES + 1)[:100])
        before = bytes(buffer)

        _ = _steps(buffer, 0, len(buffer))

        assert buffer == before


class TestPlanRead:
    def test_room_from_start_reads_into_the_tail(self) -> None:
        assert plan_read(64, 10, 20, 30) == READ_INTO_TAIL

    def test_exact_room_from_start_reads_into_the_tail(self) -> None:
        assert plan_read(64, 34, 40, 30) == READ_INTO_TAIL

    def test_one_byte_too_few_from_start_moves_to_the_front(self) -> None:
        assert plan_read(64, 35, 40, 30) == MOVE_TO_FRONT

    def test_a_full_buffer_moves_to_the_front(self) -> None:
        assert plan_read(64, 50, 64, 20) == MOVE_TO_FRONT

    def test_an_empty_buffer_reads_from_the_front(self) -> None:
        assert plan_read(64, 40, 40, HEADER) == READ_FROM_FRONT

    def test_a_header_split_at_the_buffer_end_moves_to_the_front(self) -> None:
        """2 bytes of a header at hand and 3 free bytes at the buffer end: a
        header does not fit from start, so one read after the move can bring
        the whole header."""
        end = FRAME_READ_BUFFER_BYTES - 3

        plan = plan_read(FRAME_READ_BUFFER_BYTES, end - 2, end, NEED_HEADER.wanted)

        assert plan == MOVE_TO_FRONT

    def test_a_frame_larger_than_the_buffer_grows_it_to_the_frame(self) -> None:
        assert plan_read(64, 10, 20, 100) == MoveAndGrow(buffer_bytes=100)

    def test_a_frame_larger_than_an_empty_buffer_grows_it(self) -> None:
        assert plan_read(64, 0, 0, 100) == MoveAndGrow(buffer_bytes=100)

    def test_a_read_above_the_buffer_bound_is_a_defect(self) -> None:
        with pytest.raises(AssertionError, match='out of the range'):
            plan_read(FRAME_READ_BUFFER_BYTES, 0, HEADER, READ_BUFFER_BOUND + 1)

    def test_a_read_for_bytes_already_at_hand_is_a_defect(self) -> None:
        with pytest.raises(AssertionError, match='out of the range'):
            plan_read(64, 0, 20, 20)
