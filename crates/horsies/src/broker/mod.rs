#![cfg_attr(test, allow(clippy::unwrap_used))]

pub mod bound_handle;
#[cfg(test)]
pub(crate) mod enqueue_history_tests;
pub mod error;
pub mod health;
pub mod listener;
pub mod migrations;
#[cfg(test)]
mod monitoring_schema_tests;
pub mod postgres;
pub mod result_types;
pub mod row;
pub mod shared_listener;
pub mod terminalization;
#[cfg(test)]
pub(crate) mod terminalization_matrix;

pub use bound_handle::TaskHandle;
pub use error::{is_retryable_sqlx_error, BrokerError};
pub use health::{
    DatabasePing, WorkerPingRequest, WorkerPong, WorkerPongPayload, WorkerStateSnapshot,
    WORKER_PING_CHANNEL,
};
pub use listener::NotifyListener;
pub use migrations::{expected_schema_version, run_horsies_migrations, MIGRATIONS_TABLE};
pub use postgres::{compute_enqueue_sha, ClaimPassParams, PostgresBroker, UPSERT_TASK_ATTEMPT_SQL};
pub use result_types::{BrokerErrorCode, BrokerOperationError, BrokerResult, RawResultRecord};
pub use row::heartbeat::HeartbeatRow;
pub use row::task::{
    ClaimedTaskRow, ExpiredTaskRow, SetRunningRow, StaleTaskRow, TaskAttemptRow, TaskInfoRow,
    TaskResultRow, TaskRunningContextRow,
};
pub use row::worker_state::WorkerStateRow;
pub use row::workflow::{WorkflowRow, WorkflowTaskRow};
pub use shared_listener::SharedNotifyListener;
