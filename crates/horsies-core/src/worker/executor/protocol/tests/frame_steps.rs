//! The step parser and the read plan, case by case. Python's version-1
//! parser (`tests/unit/test_frame_parser.py`) had these cases; the core
//! reader is the parser of every child now. Pure: no channel.

use super::*;

/// The header of a frame that declares `meta_bytes` of metadata and
/// `body_bytes` of body, with nothing after it.
fn header(meta_bytes: u32, body_bytes: u64) -> Vec<u8> {
    let mut bytes = Vec::with_capacity(HEADER_BYTES);
    bytes.extend_from_slice(&meta_bytes.to_be_bytes());
    bytes.extend_from_slice(&body_bytes.to_be_bytes());
    bytes
}

/// Every step from `start`, as a reader takes them: past each whole frame,
/// up to the first step that is not a whole frame. At most one step per
/// byte at hand, and one more.
fn steps(buffer: &[u8], mut start: usize, end: usize) -> Vec<FrameStep> {
    let mut taken = Vec::new();
    for _ in 0..=end - start {
        let step = next_step(buffer, start, end);
        taken.push(step);
        match step {
            FrameStep::WholeFrame { frame_end, .. } => start = frame_end,
            FrameStep::NeedBytes { .. }
            | FrameStep::ReadBodyDirectly { .. }
            | FrameStep::OverBound(_) => return taken,
        }
    }
    panic!("no stop after {} steps", taken.len());
}

/// Python `TestSmallBody::test_split_metadata_needs_the_whole_frame`.
#[test]
fn split_metadata_needs_the_whole_frame() {
    let frame = sized_frame(10, 5);
    assert_eq!(
        next_step(&frame, 0, HEADER_BYTES + 4),
        FrameStep::NeedBytes {
            wanted: frame.len()
        }
    );
}

/// Python `TestSmallBody::test_a_split_body_needs_the_whole_frame`.
#[test]
fn a_split_small_body_needs_the_whole_frame() {
    let frame = sized_frame(10, 5);
    assert_eq!(
        next_step(&frame, 0, frame.len() - 1),
        FrameStep::NeedBytes {
            wanted: frame.len()
        }
    );
}

/// Python `TestSmallBody::test_an_empty_body_is_a_whole_frame_after_its_metadata`.
#[test]
fn an_empty_body_is_a_whole_frame_after_its_metadata() {
    let min_meta = MIN_META_BYTES as usize;
    let frame = sized_frame(min_meta, 0);
    assert_eq!(
        next_step(&frame, 0, frame.len()),
        FrameStep::WholeFrame {
            meta_at: HEADER_BYTES,
            body_at: HEADER_BYTES + min_meta,
            frame_end: frame.len(),
        }
    );
}

/// Python `TestSmallBody::test_a_frame_after_the_front_of_the_buffer_counts_from_start`:
/// the bytes that a split frame needs are counted from `start`.
#[test]
fn the_bytes_a_frame_after_the_front_needs_count_from_start() {
    let start = 100;
    let mut buffer = vec![b'x'; start];
    buffer.extend(sized_frame(10, 5));
    assert_eq!(
        next_step(&buffer, start, buffer.len() - 1),
        FrameStep::NeedBytes {
            wanted: buffer.len() - start
        }
    );
}

/// Python `TestDirectBodyThreshold::test_a_whole_frame_with_a_body_at_the_threshold_is_whole`.
#[test]
fn a_whole_frame_with_a_body_at_the_threshold_is_whole() {
    let frame = sized_frame(10, DIRECT_BODY_BYTES);
    assert_eq!(
        next_step(&frame, 0, frame.len()),
        FrameStep::WholeFrame {
            meta_at: HEADER_BYTES,
            body_at: HEADER_BYTES + 10,
            frame_end: frame.len(),
        }
    );
}

/// Python `TestDirectBodyThreshold::test_a_body_above_the_threshold_is_read_directly_after_its_metadata`:
/// with exactly the header and the metadata at hand.
#[test]
fn a_body_above_the_threshold_is_read_directly_after_its_metadata() {
    let frame = sized_frame(10, DIRECT_BODY_BYTES + 1);
    assert_eq!(
        next_step(&frame, 0, HEADER_BYTES + 10),
        FrameStep::ReadBodyDirectly {
            meta_at: HEADER_BYTES,
            body_at: HEADER_BYTES + 10,
            body_bytes: DIRECT_BODY_BYTES + 1,
        }
    );
}

/// Python `TestDirectBodyThreshold::test_a_body_above_the_threshold_that_is_whole_at_hand_is_a_whole_frame`.
#[test]
fn a_body_above_the_threshold_that_is_whole_at_hand_is_a_whole_frame() {
    let frame = sized_frame(10, DIRECT_BODY_BYTES + 1);
    assert_eq!(
        next_step(&frame, 0, frame.len()),
        FrameStep::WholeFrame {
            meta_at: HEADER_BYTES,
            body_at: HEADER_BYTES + 10,
            frame_end: frame.len(),
        }
    );
}

/// Python `TestBounds::test_a_header_over_the_bounds_is_refused`: the step
/// names the declared sizes.
#[test]
fn a_header_over_the_bounds_is_a_step_that_names_its_sizes() {
    for (meta_bytes, body_bytes) in [
        (MIN_META_BYTES - 1, 0),
        (MAX_META_BYTES + 1, 0),
        (MIN_META_BYTES, MAX_BODY_BYTES + 1),
    ] {
        assert_eq!(
            next_step(&header(meta_bytes, body_bytes), 0, HEADER_BYTES),
            FrameStep::OverBound(FrameOverBound {
                meta_bytes,
                body_bytes,
            })
        );
    }
}

/// Python `TestBounds::test_a_header_at_the_bounds_is_not_refused`: the
/// largest body is read directly, so the step needs only the header and
/// the largest metadata.
#[test]
fn a_header_at_the_bounds_needs_its_metadata() {
    assert_eq!(
        next_step(&header(MAX_META_BYTES, MAX_BODY_BYTES), 0, HEADER_BYTES),
        FrameStep::NeedBytes {
            wanted: HEADER_BYTES + MAX_META_BYTES as usize
        }
    );
}

/// Python `TestBounds::test_the_largest_frame_in_the_read_buffer_needs_the_buffer_bound`.
#[test]
fn the_largest_frame_in_the_read_buffer_needs_the_buffer_bound() {
    assert_eq!(
        next_step(
            &header(MAX_META_BYTES, DIRECT_BODY_BYTES as u64),
            0,
            HEADER_BYTES
        ),
        FrameStep::NeedBytes {
            wanted: READ_BUFFER_BOUND
        }
    );
}

/// Python `TestBounds::test_growth_for_the_largest_frame_stops_at_the_buffer_bound`.
#[test]
fn growth_for_the_largest_frame_stops_at_the_buffer_bound() {
    assert_eq!(
        plan_read(FRAME_READ_BUFFER_BYTES, 0, HEADER_BYTES, READ_BUFFER_BOUND),
        ReadPlan::MoveAndGrow {
            buffer_bytes: READ_BUFFER_BOUND
        }
    );
}

/// Python `TestManyFrames::test_every_whole_frame_of_one_buffer_is_a_step_in_order`:
/// each whole frame of one buffer is a step, in order, and a split frame
/// after them needs its whole size.
#[test]
fn every_whole_frame_of_one_buffer_is_a_step_in_order() {
    let min_meta = MIN_META_BYTES as usize;
    let sizes = [(10, 5), (min_meta, 0), (30, 300), (12, 1)];
    let frames: Vec<Vec<u8>> = sizes
        .iter()
        .map(|(meta_bytes, body_bytes)| sized_frame(*meta_bytes, *body_bytes))
        .collect();
    let split = sized_frame(10, 5);
    let mut buffer: Vec<u8> = frames.concat();
    buffer.extend_from_slice(&split[..HEADER_BYTES + 3]);

    let taken = steps(&buffer, 0, buffer.len());

    let mut expected = Vec::new();
    let mut end = 0;
    for (frame, (_, body_bytes)) in frames.iter().zip(sizes) {
        end += frame.len();
        expected.push(FrameStep::WholeFrame {
            meta_at: end - frame.len() + HEADER_BYTES,
            body_at: end - body_bytes,
            frame_end: end,
        });
    }
    expected.push(FrameStep::NeedBytes {
        wanted: split.len(),
    });
    assert_eq!(taken, expected);
}

/// Python `TestManyFrames::test_a_buffer_of_whole_frames_ends_with_a_header_step`.
#[test]
fn a_buffer_of_whole_frames_ends_with_a_header_step() {
    let buffer = sized_frame(10, 5).repeat(3);

    let taken = steps(&buffer, 0, buffer.len());

    assert_eq!(taken.len(), 4);
    assert_eq!(
        taken[3],
        FrameStep::NeedBytes {
            wanted: HEADER_BYTES
        }
    );
}

/// Python `TestManyFrames::test_a_bad_header_after_whole_frames_stops_after_them`.
#[test]
fn a_bad_header_after_whole_frames_stops_after_them() {
    let good = sized_frame(10, 5);
    let mut buffer = good.repeat(2);
    buffer.extend(header(MAX_META_BYTES + 1, 0));

    let taken = steps(&buffer, 0, buffer.len());

    let second_at = good.len();
    assert_eq!(taken.len(), 3);
    assert_eq!(
        taken[1],
        FrameStep::WholeFrame {
            meta_at: second_at + HEADER_BYTES,
            body_at: second_at + HEADER_BYTES + 10,
            frame_end: 2 * good.len(),
        }
    );
    assert_eq!(
        taken[2],
        FrameStep::OverBound(FrameOverBound {
            meta_bytes: MAX_META_BYTES + 1,
            body_bytes: 0,
        })
    );
}

/// Python `TestPlanRead::test_exact_room_from_start_reads_into_the_tail`.
#[test]
fn exact_room_from_start_reads_into_the_tail() {
    assert_eq!(plan_read(64, 34, 40, 30), ReadPlan::ReadIntoTail);
}

/// Python `TestPlanRead::test_one_byte_too_few_from_start_moves_to_the_front`.
#[test]
fn one_byte_too_few_from_start_moves_to_the_front() {
    assert_eq!(plan_read(64, 35, 40, 30), ReadPlan::MoveToFront);
}

/// Python `TestPlanRead::test_a_full_buffer_moves_to_the_front`.
#[test]
fn a_full_buffer_moves_to_the_front() {
    assert_eq!(plan_read(64, 50, 64, 20), ReadPlan::MoveToFront);
}

/// Python `TestPlanRead::test_an_empty_buffer_reads_from_the_front`: no
/// byte at hand, at a start after the front.
#[test]
fn no_byte_at_hand_reads_from_the_front() {
    assert_eq!(plan_read(64, 40, 40, HEADER_BYTES), ReadPlan::ReadFromFront);
}

/// Python `TestPlanRead::test_a_header_split_at_the_buffer_end_moves_to_the_front`:
/// 2 bytes of a header at hand and 3 free bytes at the buffer end. A header
/// does not fit from start, so one read after the move can bring the whole
/// header.
#[test]
fn a_header_split_at_the_buffer_end_moves_to_the_front() {
    let end = FRAME_READ_BUFFER_BYTES - 3;
    assert_eq!(
        plan_read(FRAME_READ_BUFFER_BYTES, end - 2, end, HEADER_BYTES),
        ReadPlan::MoveToFront
    );
}

/// Python `TestPlanRead::test_a_frame_larger_than_an_empty_buffer_grows_it`.
#[test]
fn a_frame_larger_than_an_empty_buffer_grows_it() {
    assert_eq!(
        plan_read(64, 0, 0, 100),
        ReadPlan::MoveAndGrow { buffer_bytes: 100 }
    );
}

/// Python `TestPlanRead::test_a_read_above_the_buffer_bound_is_a_defect`.
#[test]
#[should_panic(expected = "outside the range")]
fn a_read_above_the_buffer_bound_is_a_defect() {
    let _ = plan_read(
        FRAME_READ_BUFFER_BYTES,
        0,
        HEADER_BYTES,
        READ_BUFFER_BOUND + 1,
    );
}

/// Python `TestPlanRead::test_a_read_for_bytes_already_at_hand_is_a_defect`.
#[test]
#[should_panic(expected = "outside the range")]
fn a_read_for_bytes_already_at_hand_is_a_defect() {
    let _ = plan_read(64, 0, 20, 20);
}
