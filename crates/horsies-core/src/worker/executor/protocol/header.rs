//! The frame header and the encoded form of a frame.

use serde::Serialize;

use crate::core::codec::to_json_bytes;

/// The header: the metadata length (`u32`) and the body length (`u64`),
/// big-endian (Python `struct.Struct('>IQ')`).
pub const HEADER_BYTES: usize = 12;
/// Metadata is a JSON object, so `{}` is the smallest valid value.
pub const MIN_META_BYTES: u32 = 2;
/// The largest metadata of one frame: 1 MiB.
pub const MAX_META_BYTES: u32 = 1 << 20;
/// PostgreSQL cannot store a field of 1 GiB or more, so a larger input or
/// result could never be saved. This bound changes no outcome: it only fails
/// earlier.
pub const MAX_BODY_BYTES: u64 = (1 << 30) - 1;

/// The body of a frame that carries none.
pub(super) const NO_BODY: &[u8] = &[];

/// The sizes of a header that is inside the bounds.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FrameSizes {
    meta_bytes: usize,
    body_bytes: usize,
}

impl FrameSizes {
    pub fn meta_bytes(self) -> usize {
        self.meta_bytes
    }

    pub fn body_bytes(self) -> usize {
        self.body_bytes
    }
}

/// A header declared more than [`MAX_META_BYTES`] or [`MAX_BODY_BYTES`], or
/// less than [`MIN_META_BYTES`]. Nothing is allocated for its frame.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error(
    "frame header declares {meta_bytes} bytes of metadata and {body_bytes} bytes of body; \
     the bounds are {MIN_META_BYTES} to {MAX_META_BYTES} and at most {MAX_BODY_BYTES}"
)]
pub struct FrameOverBound {
    pub meta_bytes: u32,
    pub body_bytes: u64,
}

/// Check a header against the bounds, before any buffer is allocated.
pub fn parse_header(header: [u8; HEADER_BYTES]) -> Result<FrameSizes, FrameOverBound> {
    let meta_bytes = u32::from_be_bytes([header[0], header[1], header[2], header[3]]);
    let body_bytes = u64::from_be_bytes([
        header[4], header[5], header[6], header[7], header[8], header[9], header[10], header[11],
    ]);
    let meta_in_bounds = (MIN_META_BYTES..=MAX_META_BYTES).contains(&meta_bytes);
    let body_in_bounds = body_bytes <= MAX_BODY_BYTES;
    match (meta_in_bounds, body_in_bounds) {
        // Both bounds are below 2^30, so both sizes fit a usize on every
        // supported target.
        (true, true) => Ok(FrameSizes {
            meta_bytes: meta_bytes as usize,
            body_bytes: body_bytes as usize,
        }),
        _ => Err(FrameOverBound {
            meta_bytes,
            body_bytes,
        }),
    }
}

/// A frame that cannot be sent: it is over a bound of the header.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum FrameEncodeError {
    #[error("frame metadata has {bytes} bytes; the bound is {MAX_META_BYTES}")]
    MetadataOverBound { bytes: usize },
    #[error("frame body has {bytes} bytes; the bound is {MAX_BODY_BYTES}")]
    BodyOverBound { bytes: usize },
}

/// The header and the metadata of a frame, and its body apart from them, so
/// a large body is not copied.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct EncodedFrame<'body> {
    head: Vec<u8>,
    body: &'body [u8],
}

impl<'body> EncodedFrame<'body> {
    /// The header and the metadata.
    pub fn head(&self) -> &[u8] {
        &self.head
    }

    /// The body.
    pub fn body(&self) -> &'body [u8] {
        self.body
    }

    /// The whole frame in one buffer: a copy of the body.
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(self.head.len() + self.body.len());
        bytes.extend_from_slice(&self.head);
        bytes.extend_from_slice(self.body);
        bytes
    }
}

/// Encode `meta` as compact JSON and frame it with `body`.
///
/// The metadata types of this module hold only text, whole numbers, flags
/// and finite numbers, so the JSON encoding cannot fail; a failure is a
/// defect of this module and panics with its cause.
pub(super) fn pack<'body, M: Serialize>(
    meta: &M,
    body: &'body [u8],
) -> Result<EncodedFrame<'body>, FrameEncodeError> {
    let meta_bytes = to_json_bytes(meta)
        .expect("frame metadata holds only JSON-native values, so its encoding cannot fail");
    let meta_len = match u32::try_from(meta_bytes.len()) {
        Ok(len) if len <= MAX_META_BYTES => len,
        _ => {
            return Err(FrameEncodeError::MetadataOverBound {
                bytes: meta_bytes.len(),
            })
        }
    };
    let body_len = match u64::try_from(body.len()) {
        Ok(len) if len <= MAX_BODY_BYTES => len,
        _ => return Err(FrameEncodeError::BodyOverBound { bytes: body.len() }),
    };
    let mut head = Vec::with_capacity(HEADER_BYTES + meta_bytes.len());
    head.extend_from_slice(&meta_len.to_be_bytes());
    head.extend_from_slice(&body_len.to_be_bytes());
    head.extend_from_slice(&meta_bytes);
    Ok(EncodedFrame { head, body })
}
