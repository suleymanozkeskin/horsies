//! Shared infrastructure of the horsies Rust end-to-end and parity tests.
//!
//! - [`database`]: one fresh database per test on the test server, dropped
//!   at the end of the test. The server comes from the environment only;
//!   no test falls back to a local default.
//! - [`process`]: a worker or scheduler process of a test binary, with its
//!   output kept for the failure report, stopped at the end of the test.
//! - [`poll`]: bounded waits on a condition.
//! - [`rows`]: reads of the task and workflow tables for the waits and the
//!   checks that the client API does not offer.
//!
//! Each failure of these helpers is a test failure: they panic with the
//! subject and the cause.

pub mod database;
pub mod poll;
pub mod process;
pub mod rows;

/// The driver of the direct connections, for the tests that read tables.
pub use tokio_postgres;
