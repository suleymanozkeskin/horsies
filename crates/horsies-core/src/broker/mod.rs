#![cfg_attr(test, allow(clippy::unwrap_used))]

pub mod attempt_writes;
pub mod child_writes;
pub mod claims;
pub mod class_budgets;
pub mod class_coverage;
#[cfg(test)]
mod class_coverage_read_tests;
pub mod class_coverage_reads;
pub mod client_family;
pub mod core_rows;
#[cfg(test)]
pub(crate) mod enqueue_history_tests;
#[cfg(test)]
mod enqueue_identity_ports;
pub(crate) mod enqueue_task;
pub mod error;
pub mod expiry_and_orphans;
pub mod health;
pub(crate) mod history_reads;
mod lifecycle_operations;
#[cfg(test)]
pub(crate) mod lifecycle_tests;
pub mod listener;
pub mod migrations;
#[cfg(test)]
mod monitoring_schema_tests;
#[cfg(test)]
pub(crate) mod monitoring_test_database;
pub mod partition_maintenance;
#[cfg(test)]
mod pgbouncer_contract_tests;
pub mod pools;
pub mod postgres;
pub mod process_listener;
pub mod result_types;
pub mod row;
#[cfg(test)]
mod send_path_tests;
#[cfg(test)]
mod session_url_tests;
pub mod slot_health;
pub mod stale_recovery;
#[cfg(test)]
mod start_deadline_tests;
pub mod task_lists;
pub mod terminalization;
#[cfg(test)]
pub(crate) mod terminalization_matrix;
pub mod wait_limit;
#[cfg(test)]
mod worker_state_read_tests;
mod workflow_claim_settle;
mod workflow_operations;
pub mod workflow_phase2;
mod workflow_reads;
pub mod workflow_start;
pub(crate) mod workflow_stop_before_start;

pub use class_budgets::{ClassBudget, ClassBudgets};
pub use class_coverage::{
    BudgetLoad, ClassCoverage, ClassReport, PendingStream, StreamCoverage, StreamCoverageState,
    StreamWait, WorkerClassLoad,
};
pub use class_coverage_reads::{LIVE_SNAPSHOT_ROW_LIMIT, PENDING_HEAD_LIMIT, PENDING_STREAM_LIMIT};
pub use core_rows::RowDecodeError;
pub use enqueue_task::SendKind;
pub use error::{
    is_retryable_sqlx_error, is_retryable_statement_failure, BrokerError, EnqueueRefusal,
    EnqueueRefusalCause,
};
pub use health::{
    checked_history_limit, ping_listener_disabled, ping_min_responses_refused,
    ping_timeout_refused, worker_history_limit_refused, DatabasePing, WorkerPingRequest,
    WorkerPong, WorkerPongPayload, WorkerStateSnapshot, WORKER_PING_CHANNEL,
};
pub use lifecycle_operations::{CancelAnswer, CancelTaskError};
pub use migrations::{expected_schema_version, MIGRATIONS_TABLE};
pub use partition_maintenance::{CoveragePassError, PrunePassError};
pub use pools::{PoolProfile, PoolSettingsError};
pub use postgres::{
    CheckedSend, ConnectivityRefusal, ListenProbeFailure, PostgresBroker, PrestartExpiry,
    SchemaInitializationMode,
};
pub use process_listener::{
    CheckInterruption, ListenError, ListenerPlanError, SessionCheckFailure,
};
pub use result_types::{
    BrokerErrorCode, BrokerOperationError, BrokerResult, RawResultRead, RawResultRecord,
    StoredEnvelopeRefused, StoredEnvelopeText, TaskInfoRead,
};
pub use row::heartbeat::HeartbeatRow;
pub use row::task::{ExpiredTaskRow, StaleTaskRow, TaskAttemptRow, TaskInfoRow};
pub use row::workflow::{WorkflowRow, WorkflowTaskRow};
pub use slot_health::{
    ClearableBootCause, DownSlot, DownSlots, ShortageErrno, SlotHealth,
    MAX_BOOT_FAILURE_DETAIL_CHARS,
};
pub use workflow_operations::{WorkflowRetentionFailed, WorkflowRetentionPass};
pub use workflow_stop_before_start::{OrphanSelfHeal, StopBeforeStart, StopMove};
