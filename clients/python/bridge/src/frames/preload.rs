//! The frames from the preload process to the worker parent as Python
//! classes. The preload process builds each frame with its constructor and
//! sends the bytes of `encode_preload_frame(frame)`. A reason is cut to its
//! bound; a process id of 0 raises `ValueError`.

use horsies_core::frame_protocol::{PreloadFrame, PreloadReason, ProcessId};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

fn process_id(pid: u32) -> PyResult<ProcessId> {
    ProcessId::new(pid).ok_or_else(|| {
        PyValueError::new_err(format!("pid must be from 1 to {}, not {pid}", i32::MAX))
    })
}

/// Imports are done, so forks can start.
#[pyclass(module = "horsies._core", name = "PreloadReadyFrame", frozen)]
pub struct PreloadReadyFrameRecord {
    frame: PreloadFrame,
}

#[pymethods]
impl PreloadReadyFrameRecord {
    #[new]
    #[pyo3(signature = (*, pid))]
    fn new(pid: u32) -> PyResult<Self> {
        Ok(Self {
            frame: PreloadFrame::PreloadReady {
                pid: process_id(pid)?,
            },
        })
    }
}

/// The child exists, and it has left the preload process.
#[pyclass(module = "horsies._core", name = "ChildForkedFrame", frozen)]
pub struct ChildForkedFrameRecord {
    frame: PreloadFrame,
}

#[pymethods]
impl ChildForkedFrameRecord {
    #[new]
    #[pyo3(signature = (*, pid))]
    fn new(pid: u32) -> PyResult<Self> {
        Ok(Self {
            frame: PreloadFrame::ChildForked {
                pid: process_id(pid)?,
            },
        })
    }
}

/// This fork failed. The preload process stays up.
#[pyclass(module = "horsies._core", name = "ForkFailedFrame", frozen)]
pub struct ForkFailedFrameRecord {
    frame: PreloadFrame,
}

#[pymethods]
impl ForkFailedFrameRecord {
    #[new]
    #[pyo3(signature = (*, reason))]
    fn new(reason: &str) -> Self {
        Self {
            frame: PreloadFrame::ForkFailed {
                reason: PreloadReason::cut(reason),
            },
        }
    }
}

/// The preload process cannot fork safely, and it exits.
#[pyclass(module = "horsies._core", name = "PreloadRefusedFrame", frozen)]
pub struct PreloadRefusedFrameRecord {
    frame: PreloadFrame,
}

#[pymethods]
impl PreloadRefusedFrameRecord {
    #[new]
    #[pyo3(signature = (*, reason))]
    fn new(reason: &str) -> Self {
        Self {
            frame: PreloadFrame::PreloadRefused {
                reason: PreloadReason::cut(reason),
            },
        }
    }
}

/// A preload frame argument: one of the four preload frame classes.
#[derive(FromPyObject)]
pub enum PreloadFrameArgument<'py> {
    Ready(Bound<'py, PreloadReadyFrameRecord>),
    Forked(Bound<'py, ChildForkedFrameRecord>),
    ForkFailed(Bound<'py, ForkFailedFrameRecord>),
    Refused(Bound<'py, PreloadRefusedFrameRecord>),
}

impl PreloadFrameArgument<'_> {
    /// `with_frame` of the core frame that the class holds.
    pub fn with_frame<T>(self, with_frame: impl FnOnce(&PreloadFrame) -> T) -> T {
        match self {
            Self::Ready(record) => with_frame(&record.get().frame),
            Self::Forked(record) => with_frame(&record.get().frame),
            Self::ForkFailed(record) => with_frame(&record.get().frame),
            Self::Refused(record) => with_frame(&record.get().frame),
        }
    }
}

/// Every class of this module, for the module registration.
pub fn add_preload_frame_classes(module: &Bound<'_, PyModule>) -> PyResult<()> {
    module.add_class::<PreloadReadyFrameRecord>()?;
    module.add_class::<ChildForkedFrameRecord>()?;
    module.add_class::<ForkFailedFrameRecord>()?;
    module.add_class::<PreloadRefusedFrameRecord>()?;
    Ok(())
}
