#![cfg_attr(test, allow(clippy::unwrap_used))]

pub mod attempt_writes;
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
pub mod process_listener;
pub mod result_types;
pub mod row;
pub mod task_lists;
pub mod terminalization;
#[cfg(test)]
pub(crate) mod terminalization_matrix;
pub mod wait_limit;

pub use bound_handle::TaskHandle;
pub use error::{is_retryable_sqlx_error, BrokerError};
pub use health::{
    DatabasePing, WorkerPingRequest, WorkerPong, WorkerPongPayload, WorkerStateSnapshot,
    WORKER_PING_CHANNEL,
};
pub use migrations::{expected_schema_version, run_horsies_migrations, MIGRATIONS_TABLE};
pub use postgres::{
    ClaimPassParams, PostgresBroker, SchemaInitializationMode, UPSERT_TASK_ATTEMPT_SQL,
};
pub use process_listener::{
    CheckInterruption, ListenError, ListenerDisabledCause, ListenerPlanError, SessionCheckFailure,
};
pub use result_types::{BrokerErrorCode, BrokerOperationError, BrokerResult, RawResultRecord};
pub use row::heartbeat::HeartbeatRow;
pub use row::task::{
    ClaimedTaskRow, ExpiredTaskRow, SetRunningRow, StaleTaskRow, TaskAttemptRow, TaskInfoRow,
    TaskResultRow, TaskRunningContextRow,
};
pub use row::worker_state::WorkerStateRow;
pub use row::workflow::{WorkflowRow, WorkflowTaskRow};
