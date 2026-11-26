//! Reads on the app, as the Python app offers them: the reads by task id
//! and the health reads.
//!
//! In a worker child a read goes through the child's producer handle, as a
//! send does; anywhere else through the app's handle.

mod by_task_id;
mod health;

pub use by_task_id::StoredResult;
pub use health::{MinResponses, PingTarget, WorkerHistoryLimit};

/// The values that the health reads return, and their failure.
pub use horsies_core::{
    BoundedTaskList, BrokerErrorCode, BrokerOperationError, BrokerOperationFailedCode, BudgetLoad,
    ClearableBootCause, DatabasePing, DownSlot, DownSlots, ExecutionClassBudget,
    ExecutionClassBudgets, ExecutionClassCoverage, ExecutionClassReport, ExpiredTaskRow,
    PendingStream, SlotHealth, SnapshotProfile, StaleMinutes, StaleTaskRow, StaleThresholdRefusal,
    StreamCoverage, StreamCoverageState, StreamWait, WorkerExecutionClassLoad, WorkerPong,
    WorkerStateSnapshot, MAX_STALE_THRESHOLD_MINUTES, TASK_LIST_BOUND,
};
