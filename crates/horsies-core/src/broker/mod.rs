#![cfg_attr(test, allow(clippy::unwrap_used))]

pub mod attempt_writes;
pub mod bound_handle;
pub mod class_budgets;
pub mod class_coverage;
#[cfg(test)]
mod class_coverage_read_tests;
pub mod class_coverage_reads;
#[cfg(test)]
pub(crate) mod enqueue_history_tests;
pub mod error;
pub mod health;
pub mod listener;
pub mod migrations;
#[cfg(test)]
mod monitoring_schema_tests;
#[cfg(test)]
pub(crate) mod monitoring_test_database;
pub mod postgres;
pub mod result_types;
pub mod row;
pub mod shared_listener;
pub mod slot_health;
pub mod task_lists;
pub mod terminalization;
#[cfg(test)]
pub(crate) mod terminalization_matrix;
pub mod wait_limit;
#[cfg(test)]
mod worker_state_read_tests;

pub use bound_handle::TaskHandle;
pub use class_budgets::{ClassBudget, ClassBudgets};
pub use class_coverage::{
    BudgetLoad, ClassCoverage, ClassReport, PendingStream, StreamCoverage, StreamCoverageState,
    StreamWait, WorkerClassLoad,
};
pub use class_coverage_reads::{LIVE_SNAPSHOT_ROW_LIMIT, PENDING_HEAD_LIMIT, PENDING_STREAM_LIMIT};
pub use error::{is_retryable_sqlx_error, BrokerError};
pub use health::{
    DatabasePing, WorkerPingRequest, WorkerPong, WorkerPongPayload, WorkerStateSnapshot,
    WORKER_PING_CHANNEL,
};
pub use listener::NotifyListener;
pub use migrations::{expected_schema_version, run_horsies_migrations, MIGRATIONS_TABLE};
pub use postgres::{
    ClaimPassParams, PostgresBroker, SchemaInitializationMode, UPSERT_TASK_ATTEMPT_SQL,
};
pub use result_types::{BrokerErrorCode, BrokerOperationError, BrokerResult, RawResultRecord};
pub use row::heartbeat::HeartbeatRow;
pub use row::task::{
    ClaimedTaskRow, ExpiredTaskRow, SetRunningRow, StaleTaskRow, TaskAttemptRow, TaskInfoRow,
    TaskResultRow, TaskRunningContextRow,
};
pub use row::worker_state::WorkerStateRow;
pub use row::workflow::{WorkflowRow, WorkflowTaskRow};
pub use shared_listener::SharedNotifyListener;
pub use slot_health::{
    ClearableBootCause, DownSlot, DownSlots, ShortageErrno, SlotHealth,
    MAX_BOOT_FAILURE_DETAIL_CHARS,
};
