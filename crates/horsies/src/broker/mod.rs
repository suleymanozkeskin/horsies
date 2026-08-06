#![cfg_attr(test, allow(clippy::unwrap_used))]

pub mod bound_handle;
pub mod error;
pub mod health;
pub mod listener;
pub mod migrations;
pub mod postgres;
pub mod result_types;
pub mod row;
pub mod shared_listener;
// Dark until the per-operation cutovers: the adapter and the installed
// functions ship before any call site moves. The allow comes off with the
// first cutover commit.
#[allow(dead_code)]
pub mod terminalization;
#[cfg(test)]
mod terminalization_matrix;

pub use bound_handle::TaskHandle;
pub use error::{is_retryable_sqlx_error, BrokerError};
pub use health::{
    DatabasePing, WorkerPingRequest, WorkerPong, WorkerPongPayload, WorkerStateSnapshot,
    WORKER_PING_CHANNEL,
};
pub use listener::NotifyListener;
pub use migrations::{run_horsies_migrations, MIGRATIONS_TABLE};
pub use postgres::{compute_enqueue_sha, ClaimPassParams, PostgresBroker, UPSERT_TASK_ATTEMPT_SQL};
pub use result_types::{BrokerErrorCode, BrokerOperationError, BrokerResult};
pub use row::heartbeat::HeartbeatRow;
pub use row::task::{
    ClaimedTaskRow, ExpiredTaskRow, SetRunningRow, StaleTaskRow, TaskAttemptRow, TaskInfoRow,
    TaskResultRow, TaskRunningContextRow,
};
pub use row::worker_state::WorkerStateRow;
pub use row::workflow::{WorkflowRow, WorkflowTaskRow};
pub use shared_listener::SharedNotifyListener;
