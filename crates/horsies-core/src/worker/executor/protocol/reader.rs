//! Parse a stream of frames in a read buffer, one step at a time, and the
//! read buffer of one channel.
//!
//! [`next_step`] names what the bytes at hand hold next, and [`plan_read`]
//! names where the next read goes; both are pure. [`FrameReader`] owns the
//! buffer and applies them. It does no I/O: the caller reads from its socket
//! into [`FrameReader::read_space`] and reports the count with
//! [`FrameReader::arrived`].

use super::header::{parse_header, FrameOverBound, HEADER_BYTES, MAX_META_BYTES};

/// The first size of the read buffer.
pub const FRAME_READ_BUFFER_BYTES: usize = 1 << 16;

/// A body larger than this is read straight into a buffer of its own size,
/// so a large body is not first copied into the read buffer.
pub const DIRECT_BODY_BYTES: usize = 1 << 16;

/// The largest size of the read buffer: a header, the largest metadata, and
/// a body that is not read straight into its own buffer.
pub const READ_BUFFER_BOUND: usize = HEADER_BYTES + MAX_META_BYTES as usize + DIRECT_BODY_BYTES;

/// The next step for the bytes at hand from `start` to `end`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FrameStep {
    /// The step needs `wanted` bytes from `start`; fewer are at hand. It is
    /// at most [`READ_BUFFER_BOUND`].
    NeedBytes { wanted: usize },
    /// A whole frame: metadata `[meta_at, body_at)`, body
    /// `[body_at, frame_end)`. The next frame starts at `frame_end`.
    WholeFrame {
        meta_at: usize,
        body_at: usize,
        frame_end: usize,
    },
    /// A body over [`DIRECT_BODY_BYTES`] that is not fully at hand: its
    /// metadata `[meta_at, body_at)` is, and the bytes from `body_at` to the
    /// end are the first part of its `body_bytes`.
    ReadBodyDirectly {
        meta_at: usize,
        body_at: usize,
        body_bytes: usize,
    },
    /// The header at `start` is over the bounds; nothing is allocated for
    /// its frame.
    OverBound(FrameOverBound),
}

/// The bytes of one header, copied out of `buffer` at `start`.
fn header_at(buffer: &[u8], start: usize) -> [u8; HEADER_BYTES] {
    let mut header = [0_u8; HEADER_BYTES];
    header.copy_from_slice(&buffer[start..start + HEADER_BYTES]);
    header
}

/// The next step for `buffer[start..end]`: bytes at hand that start at a
/// frame boundary. Reads `buffer` only.
pub fn next_step(buffer: &[u8], start: usize, end: usize) -> FrameStep {
    let meta_at = start + HEADER_BYTES;
    if end < meta_at {
        return FrameStep::NeedBytes {
            wanted: HEADER_BYTES,
        };
    }
    let sizes = match parse_header(header_at(buffer, start)) {
        Ok(sizes) => sizes,
        Err(over) => return FrameStep::OverBound(over),
    };
    let body_at = meta_at + sizes.meta_bytes();
    let frame_end = body_at + sizes.body_bytes();
    let whole_at_hand = end >= frame_end;
    let small_body = sizes.body_bytes() <= DIRECT_BODY_BYTES;
    let meta_at_hand = end >= body_at;
    match (whole_at_hand, small_body, meta_at_hand) {
        (true, _, _) => FrameStep::WholeFrame {
            meta_at,
            body_at,
            frame_end,
        },
        (false, true, _) => FrameStep::NeedBytes {
            wanted: frame_end - start,
        },
        (false, false, false) => FrameStep::NeedBytes {
            wanted: body_at - start,
        },
        (false, false, true) => FrameStep::ReadBodyDirectly {
            meta_at,
            body_at,
            body_bytes: sizes.body_bytes(),
        },
    }
}

/// Where the next read goes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReadPlan {
    /// The bytes the step needs fit from `start`: read into the free tail.
    ReadIntoTail,
    /// No byte is at hand: read from the front of the buffer.
    ReadFromFront,
    /// The bytes do not fit from `start`: move the bytes at hand to the
    /// front, then read into the free tail.
    MoveToFront,
    /// The buffer is smaller than the bytes the step needs: move the bytes
    /// at hand to the front, grow the buffer to `buffer_bytes`, then read.
    MoveAndGrow { buffer_bytes: usize },
}

/// Where the next read goes in a buffer of `buffer_bytes` when the step
/// needs `wanted` bytes from `start` ([`FrameStep::NeedBytes`]). The buffer
/// grows only to `wanted`.
///
/// `wanted` is more than the bytes at hand and at most
/// [`READ_BUFFER_BOUND`]; [`next_step`] makes both true, so another value is
/// a defect of the caller and panics.
pub fn plan_read(buffer_bytes: usize, start: usize, end: usize, wanted: usize) -> ReadPlan {
    assert!(
        end - start < wanted && wanted <= READ_BUFFER_BOUND,
        "a read for {wanted} bytes with {} bytes at hand is outside the range above the \
         bytes at hand and up to {READ_BUFFER_BOUND} bytes",
        end - start,
    );
    let too_small = buffer_bytes < wanted;
    let nothing_at_hand = start == end;
    let no_room_from_start = buffer_bytes.saturating_sub(start) < wanted;
    match (too_small, nothing_at_hand, no_room_from_start) {
        (true, _, _) => ReadPlan::MoveAndGrow {
            buffer_bytes: wanted,
        },
        (false, true, _) => ReadPlan::ReadFromFront,
        (false, false, true) => ReadPlan::MoveToFront,
        (false, false, false) => ReadPlan::ReadIntoTail,
    }
}

/// One frame as it came off the channel, not decoded yet.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RawFrame {
    pub meta: Vec<u8>,
    pub body: Vec<u8>,
}

#[derive(Debug)]
enum ReadState {
    /// Frames are read into the read buffer.
    Buffered,
    /// The large body of one frame is read into `body`; `filled` bytes of
    /// it have arrived.
    DirectBody {
        meta: Vec<u8>,
        body: Vec<u8>,
        filled: usize,
    },
}

/// The read buffer of one channel.
///
/// Use: take every frame with [`Self::next_frame`] until it gives `None`,
/// then read into [`Self::read_space`] and report the count with
/// [`Self::arrived`]. The buffer stays within [`READ_BUFFER_BOUND`]; a large
/// body gets a buffer of its own size, at most the body bound.
#[derive(Debug)]
pub struct FrameReader {
    buffer: Vec<u8>,
    /// The first byte that no delivered frame used.
    start: usize,
    /// The end of the bytes that arrived.
    end: usize,
    /// The bytes from `start` that the next step needs.
    wanted: usize,
    state: ReadState,
    bytes_received: u64,
}

impl Default for FrameReader {
    fn default() -> Self {
        Self::new()
    }
}

impl FrameReader {
    pub fn new() -> Self {
        Self {
            buffer: vec![0; FRAME_READ_BUFFER_BYTES],
            start: 0,
            end: 0,
            wanted: HEADER_BYTES,
            state: ReadState::Buffered,
            bytes_received: 0,
        }
    }

    /// The next whole frame at hand, or `None` when more bytes are needed.
    ///
    /// `Err`: a header over the bounds; the channel must close, and nothing
    /// was allocated for that frame.
    pub fn next_frame(&mut self) -> Result<Option<RawFrame>, FrameOverBound> {
        match std::mem::replace(&mut self.state, ReadState::Buffered) {
            ReadState::DirectBody { meta, body, filled } => match filled == body.len() {
                true => Ok(Some(RawFrame { meta, body })),
                false => {
                    self.state = ReadState::DirectBody { meta, body, filled };
                    Ok(None)
                }
            },
            ReadState::Buffered => self.next_buffered_frame(),
        }
    }

    fn next_buffered_frame(&mut self) -> Result<Option<RawFrame>, FrameOverBound> {
        match next_step(&self.buffer, self.start, self.end) {
            FrameStep::WholeFrame {
                meta_at,
                body_at,
                frame_end,
            } => {
                let meta = self.buffer[meta_at..body_at].to_vec();
                let body = self.buffer[body_at..frame_end].to_vec();
                self.start = frame_end;
                Ok(Some(RawFrame { meta, body }))
            }
            FrameStep::NeedBytes { wanted } => {
                self.wanted = wanted;
                Ok(None)
            }
            FrameStep::ReadBodyDirectly {
                meta_at,
                body_at,
                body_bytes,
            } => {
                let arrived = self.end - body_at;
                let mut body = vec![0; body_bytes];
                body[..arrived].copy_from_slice(&self.buffer[body_at..self.end]);
                let meta = self.buffer[meta_at..body_at].to_vec();
                self.start = 0;
                self.end = 0;
                self.wanted = HEADER_BYTES;
                self.state = ReadState::DirectBody {
                    meta,
                    body,
                    filled: arrived,
                };
                Ok(None)
            }
            FrameStep::OverBound(over) => Err(over),
        }
    }

    /// The free space the next read fills. Call only after
    /// [`Self::next_frame`] gave `None`.
    pub fn read_space(&mut self) -> &mut [u8] {
        match self.state {
            ReadState::Buffered => {
                self.prepare_buffer();
                &mut self.buffer[self.end..]
            }
            ReadState::DirectBody {
                ref mut body,
                filled,
                ..
            } => &mut body[filled..],
        }
    }

    fn prepare_buffer(&mut self) {
        match plan_read(self.buffer.len(), self.start, self.end, self.wanted) {
            ReadPlan::ReadIntoTail => {}
            ReadPlan::ReadFromFront => {
                self.start = 0;
                self.end = 0;
            }
            ReadPlan::MoveToFront => self.move_to_front(),
            ReadPlan::MoveAndGrow { buffer_bytes } => {
                self.move_to_front();
                self.buffer.resize(buffer_bytes, 0);
            }
        }
    }

    fn move_to_front(&mut self) {
        self.buffer.copy_within(self.start..self.end, 0);
        self.end -= self.start;
        self.start = 0;
    }

    /// `count` bytes arrived in the space that [`Self::read_space`] gave.
    /// More than that space is a defect of the caller and panics.
    pub fn arrived(&mut self, count: usize) {
        self.bytes_received = self
            .bytes_received
            .saturating_add(u64::try_from(count).unwrap_or(u64::MAX));
        match &mut self.state {
            ReadState::DirectBody { body, filled, .. } => {
                *filled += count;
                assert!(
                    *filled <= body.len(),
                    "{filled} bytes arrived for a body of {} bytes",
                    body.len(),
                );
            }
            ReadState::Buffered => {
                self.end += count;
                assert!(
                    self.end <= self.buffer.len(),
                    "the read buffer ends at {} bytes, but bytes arrived up to {}",
                    self.buffer.len(),
                    self.end,
                );
            }
        }
    }

    /// Whether bytes of a frame that is not whole yet are at hand.
    pub fn has_partial_frame(&self) -> bool {
        match self.state {
            ReadState::DirectBody { .. } => true,
            ReadState::Buffered => self.end > self.start,
        }
    }

    /// Every byte that arrived on this channel. The verdict after a child's
    /// exit reads it to see whether the child still writes.
    pub fn bytes_received(&self) -> u64 {
        self.bytes_received
    }
}
