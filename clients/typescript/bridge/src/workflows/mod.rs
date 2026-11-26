//! Workflow start and resume, the operations of a workflow handle, and the
//! child spec source that builds the specs of sub-workflows in JavaScript.
//!
//! - `spec_facts`: the pure facts of the conversion to spec data: the
//!   retention class of each task queue, and the options text of a node.
//! - `start_resume`: the parse of spec data, the start, and the resume.
//! - `handle_ops`: the reads, the cancel and the pause of a workflow, and
//!   the check of each stored text.
//! - `child_spec_source`: the source that calls the package's build function
//!   on the JavaScript thread.
//! - `child_producer_probe`: the test exports of a start and a handle read
//!   in a task child (only with the cargo feature `test-probes`).

#[cfg(feature = "test-probes")]
pub mod child_producer_probe;
pub mod child_spec_source;
pub mod handle_ops;
pub mod spec_facts;
pub mod start_resume;
