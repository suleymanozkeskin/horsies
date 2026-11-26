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
mod signals;
// The shared golden vectors of the stored JSON forms.
#[cfg(test)]
mod stored_form_vectors;

// The `pub` items of these modules form the crate root API. Each of them
// keeps its submodules private, so a submodule name never reaches the root.
mod check;
mod child;
mod handle;
mod process_state;
mod reads;
mod scheduler;
mod send;
mod task;
// The spans and producer metrics of the sends, the starts and the task
// bodies: `otel` with the feature, nothing without it.
mod telemetry;
mod worker;
mod workflow;

// These modules are public paths of the API.
pub mod blocking;
#[cfg(feature = "cli")]
pub mod cli;
// The lifecycle events of the worker, and the app's handlers of them.
pub mod events;
pub mod monitoring;
#[cfg(feature = "otel")]
pub mod otel;
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
pub use crate::process_state::RuntimeStartFailed;
pub use crate::reads::*;
pub use crate::scheduler::*;
pub use crate::send::*;
pub use crate::task::*;
pub use crate::telemetry::ATTEMPT_SPAN_TARGET;
pub use crate::worker::*;
pub use crate::workflow::*;

pub use horsies_macros::{blocking_task, task, WorkflowInput};

/// Paths that the expansions of the `horsies` macros name. Not API.
#[doc(hidden)]
pub mod __private {
    pub use crate::task::body::{
        body_result, check_input, decode_input, decode_unit_input, StoredTaskError,
    };
    pub use horsies_core::core::task::fn_trait::TaskMeta;
    pub use horsies_core::core::task::{AsyncTaskFn, BlockingTaskFn, RawTaskResult};
    pub use horsies_core::{RegisteredTask, TaskResult};
    /// The generated `Input` struct of a task derives serde through this
    /// path, so the user's crate needs no serde name of its own for it.
    pub use serde;

    /// The field token of one field of a workflow node's input type.
    pub const fn input_field<I, F>(name: &'static str) -> crate::InputField<I, F> {
        crate::InputField::raw(name)
    }

    /// Whether `text` may be a custom error code: not the name of a built-in
    /// code and not a reserved name. `#[derive(CustomErrorCode)]` asserts it
    /// for each code at compile time.
    pub const fn is_free_custom_code(text: &str) -> bool {
        crate::task::is_free_custom_code(text)
    }

    /// Whether `text` may stand in `retry_for` of a task whose code
    /// type has `custom_codes`: a built-in code or one of `custom_codes`. The
    /// task attribute asserts it for each entry at compile time.
    pub const fn is_retry_code(text: &str, custom_codes: &[&str]) -> bool {
        crate::task::is_retry_code(text, custom_codes)
    }

    /// The stored code of a `retry_for` entry that passed
    /// [`is_retry_code`].
    pub fn retry_code(text: &str) -> horsies_core::TaskErrorCode {
        crate::task::stored_retry_code(text)
    }

    /// The `CODES` of the code type `E` of the async task function
    /// `function`, whose input is `I`: the error codes that the task
    /// declares. The argument only names the function's type.
    /// `async_task_fn!` calls it; the registration checks the codes.
    pub fn async_task_codes<I, F, Fut, T, E>(_function: &F) -> &'static [&'static str]
    where
        F: Fn(I) -> Fut,
        Fut: std::future::Future<Output = Result<T, crate::TaskError<E>>>,
        E: crate::CustomErrorCode,
    {
        E::CODES
    }

    /// The `CODES` of the code type `E` of the blocking task function
    /// `function`, whose input is `I`, as [`async_task_codes`].
    /// `blocking_task_fn!` calls it.
    pub fn blocking_task_codes<I, F, T, E>(_function: &F) -> &'static [&'static str]
    where
        F: Fn(I) -> Result<T, crate::TaskError<E>>,
        E: crate::CustomErrorCode,
    {
        E::CODES
    }

    /// The module that defines a task: the parent of the generated module
    /// whose `module_path!()` is `path`.
    pub fn parent_module(path: &'static str) -> &'static str {
        match path.rsplit_once("::") {
            Some((parent, _generated)) => parent,
            None => path,
        }
    }

    /// The typed function of the task `task_name` on `app`, for the
    /// generated `name::send` and `name::with_options(..).send`.
    ///
    /// Errors: `VALIDATION_FAILED` with the cause `PreconditionFailed` when
    /// `app` holds no task of this name with these types; nothing is
    /// written. The message is the text of the [`crate::TaskFunctionNotFound`]
    /// cause.
    pub fn send_function<A, T, E>(
        app: &crate::Horsies,
        task_name: &str,
    ) -> Result<crate::TaskFunction<A, T, E>, crate::TaskSendError>
    where
        A: serde::Serialize + serde::de::DeserializeOwned + 'static,
        T: serde::Serialize + serde::de::DeserializeOwned + 'static,
        E: crate::CustomErrorCode,
    {
        app.functions()
            .get::<A, T, E>(task_name)
            .map_err(|not_found| crate::TaskSendError::ValidationFailed {
                message: not_found.to_string(),
                cause: horsies_core::FailureCause::PreconditionFailed,
            })
    }

    /// A workflow node of the task `task_name` with the output `T`, the
    /// input `I` and an empty input `{}`, for the generated `name::node()`.
    /// The workflow builder resolves the rest from its app when the node is
    /// added.
    pub fn task_node<T, I>(task_name: &str) -> crate::TaskNode<T, I>
    where
        I: serde::Serialize + serde::de::DeserializeOwned + 'static,
    {
        crate::TaskNode::of_task(task_name)
    }
}
