//! The Rust client of horsies, a task queue and workflow engine on
//! PostgreSQL.
//!
//! The client runs on `horsies-core`, the engine that the Python client runs
//! on too: one schema, one set of SQL functions and one worker parent serve
//! both. Python is the behaviour reference. Task bodies never run in the
//! worker process: the worker starts this binary again in child mode, for
//! every execution class.

#![cfg_attr(test, allow(clippy::unwrap_used))]

// The expansions of the `horsies` macros name `horsies::...` paths. Inside
// this crate they resolve through this name.
extern crate self as horsies;

mod app;
mod config;
mod error;
mod registry;

// The `pub` items of these modules form the crate root API. Each of them
// keeps its submodules private, so a submodule name never reaches the root.
mod check;
mod child;
mod handle;
mod reads;
mod scheduler;
mod send;
mod task;
mod worker;
mod workflow;

// These modules are public paths of the API.
pub mod blocking;
#[cfg(feature = "cli")]
pub mod cli;
pub mod monitoring;
pub mod rerun;
#[cfg(feature = "web")]
pub mod web;

// The commands of the shipped binary. The binary is a separate target, so
// the module is public, but it is not API.
#[cfg(feature = "cli")]
#[doc(hidden)]
pub mod tools;

pub use crate::app::{AppRole, Horsies, SchemaMode, StateNotProvided, SUPPRESS_SENDS_ENV};
pub use crate::config::*;
pub use crate::error::*;
pub use horsies_core::handle::CoreHandle;

pub use crate::check::*;
pub use crate::child::*;
pub use crate::handle::*;
pub use crate::reads::*;
pub use crate::scheduler::*;
pub use crate::send::*;
pub use crate::task::*;
pub use crate::worker::*;
pub use crate::workflow::*;

pub use horsies_macros::{blocking_task, task, WorkflowInput};

/// Paths that the expansions of the `horsies` macros name. Not API.
#[doc(hidden)]
pub mod __private {
    /// The field token of one field of a workflow node's input type.
    pub const fn input_field<I, F>(name: &'static str) -> crate::InputField<I, F> {
        crate::InputField::raw(name)
    }

    /// Whether `text` may be a user error code: not the name of a built-in
    /// code and not a reserved name. `#[derive(UserErrorCode)]` asserts it
    /// for each code at compile time.
    pub const fn is_free_user_code(text: &str) -> bool {
        crate::task::is_free_user_code(text)
    }
}
