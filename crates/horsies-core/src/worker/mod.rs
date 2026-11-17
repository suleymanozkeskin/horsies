#![cfg_attr(test, allow(clippy::unwrap_used))]

pub mod backoff;
pub mod children;
pub mod claim_pass;
mod class_coverage_log;
pub mod cli;
pub mod config;
pub mod connection_fit;
pub mod error;
pub mod execution;
pub mod executor;
pub mod finalize_batch;
pub mod from_manifest;
pub mod heartbeat;
pub mod memory_fit;
pub mod profile_fit;
pub mod queue_cap_hold;
pub mod recovery;
pub mod retry;
pub mod run_end;
pub mod runner_beats;
pub mod scheduler;
mod supervision;
mod thread_dispatch;
pub mod worker;
pub mod worker_state;

pub use error::WorkerError;
