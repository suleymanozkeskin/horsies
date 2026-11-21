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
pub mod liveness;
pub mod memory_fit;
pub(crate) mod metrics_endpoint;
#[cfg(feature = "metrics-endpoint")]
pub(crate) mod probes;
pub mod profile_fit;
pub mod queue_cap_hold;
pub mod recovery;
pub mod retry;
pub mod run_end;
pub mod runner_beats;
pub mod scheduler;
mod supervision;
mod thread_dispatch;
#[expect(
    clippy::module_inception,
    reason = "the worker loop is the `worker` of the worker module; a rename moves the file, \
              its test folder and every `worker::worker` path that source-reading tests check"
)]
pub mod worker;
pub mod worker_state;

pub use error::WorkerError;
