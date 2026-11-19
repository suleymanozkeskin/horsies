//! The app of the Rust end-to-end tests (Python `clients/python/tests/e2e/tasks/`).
//!
//! The same crate is the test binary's library and the tests' harness:
//!
//! - [`instance`]: the app instances and the worker profiles;
//! - [`tasks`]: the task definitions and their registration;
//! - [`markers`]: marker files between a task in a child and its test;
//! - [`harness`]: the database, app and processes of one test.
//!
//! `src/main.rs` builds the app of the instance that its environment names
//! and hands over to `horsies::cli::run`, which serves the worker, its
//! children and the scheduler.

use horsies::{Horsies, HorsiesError};

pub mod harness;
pub mod instance;
pub mod markers;
pub mod tasks;

use instance::{Connection, Instance};

/// The app of `instance` on `connection`, with the instance's tasks. No
/// I/O: the app connects at its first operation.
///
/// Errors: the config refusal of the instance, or the first registration
/// refusal.
pub fn build_app(instance: Instance, connection: &Connection) -> Result<Horsies, HorsiesError> {
    let mut app = Horsies::new(instance.config(connection))?;
    tasks::register(&mut app, instance)?;
    Ok(app)
}
