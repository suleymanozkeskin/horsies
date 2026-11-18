//! The parent-child frame codec for the Python child program and the
//! preload process: one codec in Rust (`horsies_core::frame_protocol`),
//! called through `horsies._core`.
//!
//! - `decode_parent_frame` and `FrameReader.feed` give the parent frames as
//!   Python classes (`parent`).
//! - The child builds each child frame with its class and sends the bytes of
//!   `encode_child_frame` (`child`); the preload process does the same with
//!   `encode_preload_frame` (`preload`).
//! - `protocol_numbers` gives the protocol version, the exit codes and the
//!   spawn request byte, so the Python side defines none of them.
//!
//! Every call is pure: no runtime starts, no thread, no connection. The calls
//! that decode or encode check only the fork rule of every core call
//! (`process_core::fork_refusal`): the preload process, where the core never
//! starts, may call them before it forks, and a worker child whose core
//! stopped still reports its last frames to the worker before it ends itself
//! (the release of a start it refused). A fork of a child cannot speak on the
//! channel of its parent. A refused frame is a value; a defect of the caller
//! (a constructor value outside its bound) raises `ValueError`.

mod child;
mod parent;
mod preload;

pub use parent::InitFrameRecord;

use std::sync::{Mutex, PoisonError};

use horsies_core::frame_protocol::{
    decode_parent_frame as core_decode_parent_frame, encode_child_frame as core_encode_child_frame,
    encode_preload_frame as core_encode_preload_frame, EncodedFrame, FrameEncodeError,
    FrameOverBound, FrameReader, ParentFrame, ParentFrameRefusal, BAD_ARGUMENTS_EXIT_CODE,
    BAD_PARENT_FRAME_EXIT_CODE, BOOT_FAILED_EXIT_CODE, CHILD_HOOK_FAILED_EXIT_CODE, MAX_BODY_BYTES,
    MAX_LEFTOVER_TASK_NAMES, MAX_META_BYTES, PARENT_GONE_EXIT_CODE, PROTOCOL_MISMATCH_EXIT_CODE,
    PROTOCOL_VERSION, REPARENT_FAILED_EXIT_CODE, SPAWN_REQUEST_BYTE,
    STOPPED_WITH_EXECUTOR_THREADS_EXIT_CODE,
};
use pyo3::prelude::*;
use pyo3::types::PyBytes;
use pyo3::IntoPyObjectExt;

use crate::errors::refusal_error;
use crate::process_core::fork_refusal;

use child::ChildFrameArgument;
use parent::{parent_frame_object, parent_refusal_object};
use preload::PreloadFrameArgument;

/// The numbers that the child, the preload process and the worker parent
/// share: the protocol version, the exit codes of a child, the byte of a
/// spawn request, and the two bounds that the child applies before it builds
/// a frame (the names of a report of left work, the bytes of a body).
#[pyclass(module = "horsies._core", frozen, get_all)]
pub struct ProtocolNumbers {
    protocol_version: u32,
    parent_gone_exit_code: i32,
    bad_parent_frame_exit_code: i32,
    bad_arguments_exit_code: i32,
    stopped_with_executor_threads_exit_code: i32,
    boot_failed_exit_code: i32,
    reparent_failed_exit_code: i32,
    protocol_mismatch_exit_code: i32,
    child_hook_failed_exit_code: i32,
    spawn_request_byte: u8,
    /// A report of left work names `min(count, max_leftover_task_names)`
    /// tasks.
    max_leftover_task_names: usize,
    /// The largest body of a frame, in bytes: the largest result envelope.
    max_body_bytes: u64,
}

/// A header over the bounds: nothing was allocated for its frame, and the
/// channel cannot be read on.
#[pyclass(module = "horsies._core", name = "FrameOverBound", frozen, get_all)]
pub struct FrameOverBoundRecord {
    meta_bytes: u32,
    body_bytes: u64,
}

impl From<FrameOverBound> for FrameOverBoundRecord {
    fn from(over: FrameOverBound) -> Self {
        Self {
            meta_bytes: over.meta_bytes,
            body_bytes: over.body_bytes,
        }
    }
}

/// A frame that cannot be sent: its metadata or its body is over a bound of
/// the header. Nothing was encoded.
#[pyclass(module = "horsies._core", frozen, get_all)]
pub struct FrameTooLarge {
    /// `'METADATA'` or `'BODY'`.
    part: &'static str,
    bytes: u64,
    bound: u64,
}

impl FrameTooLarge {
    fn of(error: FrameEncodeError) -> Self {
        match error {
            FrameEncodeError::MetadataOverBound { bytes } => Self {
                part: "METADATA",
                bytes: u64::try_from(bytes).unwrap_or(u64::MAX),
                bound: u64::from(MAX_META_BYTES),
            },
            FrameEncodeError::BodyOverBound { bytes } => Self {
                part: "BODY",
                bytes: u64::try_from(bytes).unwrap_or(u64::MAX),
                bound: MAX_BODY_BYTES,
            },
        }
    }
}

/// Refuse a call in a process forked without `exec` after the core started
/// in its parent. A process whose core stopped is not refused.
fn refuse_by_fork_rule() -> PyResult<()> {
    match fork_refusal() {
        Some(refusal) => Err(refusal_error(refusal)),
        None => Ok(()),
    }
}

/// The bytes of an encoded frame: its head, then its body, in one `bytes`.
fn frame_bytes(py: Python<'_>, encoded: &EncodedFrame<'_>) -> PyResult<Py<PyAny>> {
    let head = encoded.head();
    let body = encoded.body();
    let bytes = PyBytes::new_with(py, head.len() + body.len(), |buffer: &mut [u8]| {
        buffer[..head.len()].copy_from_slice(head);
        buffer[head.len()..].copy_from_slice(body);
        Ok(())
    })?;
    Ok(bytes.into_any().unbind())
}

/// The protocol version, the exit codes of a child and the spawn request
/// byte. Pure; never refused.
#[pyfunction]
fn protocol_numbers() -> ProtocolNumbers {
    ProtocolNumbers {
        protocol_version: PROTOCOL_VERSION,
        parent_gone_exit_code: PARENT_GONE_EXIT_CODE,
        bad_parent_frame_exit_code: BAD_PARENT_FRAME_EXIT_CODE,
        bad_arguments_exit_code: BAD_ARGUMENTS_EXIT_CODE,
        stopped_with_executor_threads_exit_code: STOPPED_WITH_EXECUTOR_THREADS_EXIT_CODE,
        boot_failed_exit_code: BOOT_FAILED_EXIT_CODE,
        reparent_failed_exit_code: REPARENT_FAILED_EXIT_CODE,
        protocol_mismatch_exit_code: PROTOCOL_MISMATCH_EXIT_CODE,
        child_hook_failed_exit_code: CHILD_HOOK_FAILED_EXIT_CODE,
        spawn_request_byte: SPAWN_REQUEST_BYTE,
        max_leftover_task_names: MAX_LEFTOVER_TASK_NAMES,
        max_body_bytes: MAX_BODY_BYTES,
    }
}

/// Decode one parent frame from its metadata and its body: `InitFrame`,
/// `RunFrame`, `SharedRunFrame`, `StopFrame`; `FrameRefused` for a malformed
/// frame; `ParentProtocolMismatch` for an init frame of another protocol
/// version. Pure.
#[pyfunction]
fn decode_parent_frame(py: Python<'_>, meta: &[u8], body: &[u8]) -> PyResult<Py<PyAny>> {
    refuse_by_fork_rule()?;
    match core_decode_parent_frame(meta, body.to_vec()) {
        Ok(frame) => parent_frame_object(py, frame),
        Err(refusal) => parent_refusal_object(py, &refusal),
    }
}

/// Encode a child frame: its bytes, or `FrameTooLarge` when its metadata or
/// its result is over a bound of the header. Pure.
#[pyfunction]
fn encode_child_frame(py: Python<'_>, frame: ChildFrameArgument<'_>) -> PyResult<Py<PyAny>> {
    refuse_by_fork_rule()?;
    frame.with_frame(|frame| match core_encode_child_frame(frame) {
        Ok(encoded) => frame_bytes(py, &encoded),
        Err(error) => FrameTooLarge::of(error).into_py_any(py),
    })
}

/// Encode a preload frame: its bytes. A preload frame has no body and its
/// reason is cut to its bound, so it is always under the bounds. Pure.
#[pyfunction]
fn encode_preload_frame(py: Python<'_>, frame: PreloadFrameArgument<'_>) -> PyResult<Py<PyAny>> {
    refuse_by_fork_rule()?;
    frame.with_frame(|frame| match core_encode_preload_frame(frame) {
        Ok(encoded) => frame_bytes(py, &encoded),
        Err(error) => unreachable!(
            "a preload frame has no body and a reason cut to its bound, so it is under the \
             bounds of the header: {error}"
        ),
    })
}

/// What the bytes that arrived complete.
enum Fed {
    /// The whole frames, in order; none when no frame is whole yet.
    Frames(Vec<ParentFrame>),
    OverBound(FrameOverBound),
    Refused(ParentFrameRefusal),
}

/// Put `data` into `reader` and decode every whole frame. Bounded by the
/// length of `data`: each pass of the outer loop puts at least one byte in,
/// and each pass of the inner loop takes out one frame of at least a header.
fn take_frames(reader: &mut FrameReader, data: &[u8]) -> Fed {
    let mut frames = Vec::new();
    let mut rest = data;
    loop {
        loop {
            match reader.next_frame() {
                Ok(Some(raw)) => match core_decode_parent_frame(&raw.meta, raw.body) {
                    Ok(frame) => frames.push(frame),
                    Err(refusal) => return Fed::Refused(refusal),
                },
                Ok(None) => break,
                Err(over) => return Fed::OverBound(over),
            }
        }
        if rest.is_empty() {
            return Fed::Frames(frames);
        }
        let space = reader.read_space();
        let count = space.len().min(rest.len());
        assert!(
            count > 0,
            "the frame reader gave no read space after it asked for more bytes"
        );
        space[..count].copy_from_slice(&rest[..count]);
        reader.arrived(count);
        rest = &rest[count..];
    }
}

/// The read buffer of the channel from the worker parent, with the bounds
/// of the core reader: a header is checked before anything is allocated for
/// its frame, and a large body gets a buffer of its own size.
#[pyclass(module = "horsies._core", name = "FrameReader", frozen)]
pub struct FrameReaderHandle {
    reader: Mutex<FrameReader>,
}

#[pymethods]
impl FrameReaderHandle {
    #[new]
    fn new() -> Self {
        Self {
            reader: Mutex::new(FrameReader::new()),
        }
    }

    /// Take `data`, the bytes that arrived on the channel, and give the whole
    /// parent frames it completes, in order: a list, empty when no frame is
    /// whole yet. `FrameOverBound`, `FrameRefused` or
    /// `ParentProtocolMismatch` when the next frame is refused; the channel
    /// cannot be read on then.
    fn feed(&self, py: Python<'_>, data: &[u8]) -> PyResult<Py<PyAny>> {
        refuse_by_fork_rule()?;
        // The lock is held only for Rust work: no Python object is built
        // while it is held.
        let fed = {
            let mut reader = self.reader.lock().unwrap_or_else(PoisonError::into_inner);
            take_frames(&mut reader, data)
        };
        match fed {
            Fed::Frames(frames) => {
                let objects = frames
                    .into_iter()
                    .map(|frame| parent_frame_object(py, frame))
                    .collect::<PyResult<Vec<Py<PyAny>>>>()?;
                objects.into_py_any(py)
            }
            Fed::OverBound(over) => FrameOverBoundRecord::from(over).into_py_any(py),
            Fed::Refused(refusal) => parent_refusal_object(py, &refusal),
        }
    }
}

/// Add the classes and the functions of the codec to `horsies._core`.
pub fn add_frames(module: &Bound<'_, PyModule>) -> PyResult<()> {
    parent::add_parent_frame_classes(module)?;
    child::add_child_frame_classes(module)?;
    preload::add_preload_frame_classes(module)?;
    module.add_class::<ProtocolNumbers>()?;
    module.add_class::<FrameOverBoundRecord>()?;
    module.add_class::<FrameTooLarge>()?;
    module.add_class::<FrameReaderHandle>()?;
    module.add_function(wrap_pyfunction!(protocol_numbers, module)?)?;
    module.add_function(wrap_pyfunction!(decode_parent_frame, module)?)?;
    module.add_function(wrap_pyfunction!(encode_child_frame, module)?)?;
    module.add_function(wrap_pyfunction!(encode_preload_frame, module)?)?;
    Ok(())
}

#[cfg(test)]
mod tests;
