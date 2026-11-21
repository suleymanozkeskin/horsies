//! Frames from the preload process to the worker parent (Python
//! `core/worker/preload_main.py`). The worker
//! sends the preload the init frame of [`super::ParentFrame`], and asks for a
//! child with [`SPAWN_REQUEST_BYTE`] plus one descriptor through
//! `SCM_RIGHTS`; that request is not a frame.

use serde::Serialize;

use super::header::{pack, EncodedFrame, FrameEncodeError, NO_BODY};
use super::metadata::{no_body, parse_metadata, Fields, MalformedFrame};
use super::values::{PreloadReason, ProcessId};

/// The byte of a spawn request (Python `SPAWN_REQUEST`).
pub const SPAWN_REQUEST_BYTE: u8 = b'S';

/// A frame from the preload process. Preload frames have no body.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PreloadFrame {
    /// Imports are done, so forks can start.
    PreloadReady { pid: ProcessId },
    /// The child exists, and it has left the preload process.
    ChildForked { pid: ProcessId },
    /// This fork failed. The preload process stays up.
    ForkFailed { reason: PreloadReason },
    /// The preload process cannot fork safely, and it exits.
    PreloadRefused { reason: PreloadReason },
}

#[derive(Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum PreloadWire<'a> {
    PreloadReady { pid: u32 },
    ChildForked { pid: u32 },
    ForkFailed { reason: &'a str },
    PreloadRefused { reason: &'a str },
}

/// Encode a preload frame.
pub fn encode_preload_frame(frame: &PreloadFrame) -> Result<EncodedFrame<'_>, FrameEncodeError> {
    let wire = match frame {
        PreloadFrame::PreloadReady { pid } => PreloadWire::PreloadReady { pid: pid.get() },
        PreloadFrame::ChildForked { pid } => PreloadWire::ChildForked { pid: pid.get() },
        PreloadFrame::ForkFailed { reason } => PreloadWire::ForkFailed {
            reason: reason.as_str(),
        },
        PreloadFrame::PreloadRefused { reason } => PreloadWire::PreloadRefused {
            reason: reason.as_str(),
        },
    };
    pack(&wire, NO_BODY)
}

fn decode_pid(fields: &Fields<'_>) -> Result<ProcessId, MalformedFrame> {
    u32::try_from(fields.whole("pid")?)
        .ok()
        .and_then(ProcessId::new)
        .ok_or_else(|| fields.out_of_bounds("pid"))
}

/// Build a preload frame from received bytes. Used by the worker parent.
pub fn decode_preload_frame(meta: &[u8], body: &[u8]) -> Result<PreloadFrame, MalformedFrame> {
    let map = parse_metadata(meta)?;
    let fields = Fields::root(&map);
    let kind = fields.kind()?;
    no_body("preload", body)?;
    match kind {
        "preload_ready" => {
            fields.only(&["kind", "pid"])?;
            Ok(PreloadFrame::PreloadReady {
                pid: decode_pid(&fields)?,
            })
        }
        "child_forked" => {
            fields.only(&["kind", "pid"])?;
            Ok(PreloadFrame::ChildForked {
                pid: decode_pid(&fields)?,
            })
        }
        "fork_failed" => {
            fields.only(&["kind", "reason"])?;
            Ok(PreloadFrame::ForkFailed {
                reason: fields.bounded_text("reason")?,
            })
        }
        "preload_refused" => {
            fields.only(&["kind", "reason"])?;
            Ok(PreloadFrame::PreloadRefused {
                reason: fields.bounded_text("reason")?,
            })
        }
        other => Err(fields.unknown_kind(other)),
    }
}
