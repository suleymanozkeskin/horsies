#![cfg_attr(test, allow(clippy::unwrap_used))]

pub mod backoff;
pub mod cli;
pub mod config;
#[allow(dead_code)] // not yet wired up; will be implemented
pub mod docs_fetcher;
pub mod error;
pub mod execution;
pub mod heartbeat;
pub mod recovery;
pub mod retry;
pub mod scheduler;
pub mod worker;
pub mod worker_state;

pub use error::WorkerError;
