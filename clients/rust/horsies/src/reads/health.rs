//! Health reads on the app (Python `Horsies.ping_database`,
//! `ping_workers`, `list_worker_states`, `get_worker_state`,
//! `get_worker_state_history`, `get_stale_tasks`, `get_expired_tasks`, and
//! the class reads).
//!
//! Each read connects the app's handle, installs the schema once per handle
//! as a send does, and reads only. A failure is the core's
//! `DatabaseOperationError` with Python's broker code: a failed connect or
//! install has the code of the read (`DATABASE_PING_FAILED`, `WORKER_PING_FAILED`,
//! `MONITORING_QUERY_FAILED`). In a process that was forked without `exec`
//! after the core's process runtime started, each read panics before it
//! connects (`process_state::refuse_in_a_fork`).

use std::num::{NonZeroU32, NonZeroUsize};
use std::sync::Arc;
use std::time::Duration;

use horsies_core::{
    BoundedTaskList, BrokerError, BrokerOperationSource, DatabaseOperationError,
    DatabaseOperationFailedCode, DatabasePing, ExecutionClassCoverage, ExecutionClassReport,
    ExpiredTaskRow, PostgresBroker, StaleMinutes, StaleTaskRow, WorkerPong, WorkerStateSnapshot,
};

use crate::process_state::refuse_in_a_fork;
use crate::Horsies;

/// Which workers a ping asks.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PingTarget {
    /// Every worker that listens.
    EveryWorker,
    /// The worker with this id; the ping returns as soon as it replies.
    Worker(String),
}

/// When a ping returns before its wait ends.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MinResponses {
    /// Wait the full window and list every worker that replied.
    FullWindow,
    /// Return as soon as this many distinct workers replied.
    AtLeast(NonZeroUsize),
}

/// How many state snapshots of a worker a history read returns.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkerHistoryLimit {
    /// Every retained snapshot.
    AllRetained,
    /// The newest `n` snapshots.
    Newest(NonZeroU32),
}

impl Horsies {
    /// A timed `SELECT 1` round trip: the latency of the database.
    ///
    /// Errors: `DATABASE_PING_FAILED` with the retry classification of the cause.
    pub async fn ping_database(&self) -> Result<DatabasePing, DatabaseOperationError> {
        self.health_broker(
            DatabaseOperationFailedCode::DatabasePingFailed,
            "ping_database",
        )
        .await?
        .ping_database()
        .await
    }

    /// Ping the workers and collect the distinct replies by worker id,
    /// until `target` replied, `min_responses` workers replied, or `wait`
    /// passed. A worker that is in `list_worker_states` and not here did not
    /// answer.
    ///
    /// Errors: `INVALID_INPUT` for a zero `wait`, with no statement;
    /// `LISTENER_DISABLED` for an app whose schema mode has no listener;
    /// `WORKER_PING_FAILED`.
    pub async fn ping_workers(
        &self,
        target: PingTarget,
        wait: Duration,
        min_responses: MinResponses,
    ) -> Result<Vec<WorkerPong>, DatabaseOperationError> {
        let target_worker_id = match &target {
            PingTarget::EveryWorker => None,
            PingTarget::Worker(worker_id) => Some(worker_id.as_str()),
        };
        let min_responses = match min_responses {
            MinResponses::FullWindow => None,
            MinResponses::AtLeast(count) => Some(count.get()),
        };
        self.health_broker(
            DatabaseOperationFailedCode::WorkerPingFailed,
            "ping_workers",
        )
        .await?
        .ping_workers(target_worker_id, wait, min_responses)
        .await
    }

    /// The latest state snapshot of every worker that reported one, idle
    /// workers included.
    ///
    /// Errors: `MONITORING_QUERY_FAILED`, also for a stored snapshot of
    /// another shape, which names the worker and the column.
    pub async fn list_worker_states(
        &self,
    ) -> Result<Vec<WorkerStateSnapshot>, DatabaseOperationError> {
        self.monitoring_broker("list_worker_states")
            .await?
            .list_worker_states()
            .await
    }

    /// The latest state snapshot of worker `worker_id`, or `None` when it
    /// never reported one.
    ///
    /// Errors: as `list_worker_states`.
    pub async fn get_worker_state(
        &self,
        worker_id: &str,
    ) -> Result<Option<WorkerStateSnapshot>, DatabaseOperationError> {
        self.monitoring_broker("get_worker_state")
            .await?
            .get_worker_state(worker_id)
            .await
    }

    /// The state snapshots of worker `worker_id`, newest first.
    ///
    /// Errors: as `list_worker_states`.
    pub async fn get_worker_state_history(
        &self,
        worker_id: &str,
        limit: WorkerHistoryLimit,
    ) -> Result<Vec<WorkerStateSnapshot>, DatabaseOperationError> {
        let limit = match limit {
            WorkerHistoryLimit::AllRetained => None,
            WorkerHistoryLimit::Newest(count) => Some(i64::from(count.get())),
        };
        self.monitoring_broker("get_worker_state_history")
            .await?
            .get_worker_state_history(worker_id, limit)
            .await
    }

    /// RUNNING tasks whose latest runner beat, or their start when they
    /// have no beat, is older than `threshold`. At most `TASK_LIST_BOUND`
    /// rows; more rows is `BoundedTaskList::Truncated`. Reads only: the
    /// reaper recovers stale tasks.
    ///
    /// Errors: `MONITORING_QUERY_FAILED`.
    pub async fn get_stale_tasks(
        &self,
        threshold: StaleMinutes,
    ) -> Result<BoundedTaskList<StaleTaskRow>, DatabaseOperationError> {
        self.monitoring_broker("get_stale_tasks")
            .await?
            .get_stale_tasks(threshold.minutes())
            .await
    }

    /// PENDING tasks past their `good_until`, oldest deadline first; no
    /// worker claims them. At most `TASK_LIST_BOUND` rows; more rows is
    /// `BoundedTaskList::Truncated`.
    ///
    /// Errors: `MONITORING_QUERY_FAILED`.
    pub async fn get_expired_tasks(
        &self,
    ) -> Result<BoundedTaskList<ExpiredTaskRow>, DatabaseOperationError> {
        self.monitoring_broker("get_expired_tasks")
            .await?
            .get_expired_tasks()
            .await
    }

    /// For each pending stream of an execution class, whether a live worker
    /// can run it (the fleet check).
    ///
    /// Errors: `MONITORING_QUERY_FAILED`.
    pub async fn get_execution_class_coverage(
        &self,
    ) -> Result<ExecutionClassCoverage, DatabaseOperationError> {
        self.monitoring_broker("get_execution_class_coverage")
            .await?
            .get_execution_class_coverage()
            .await
    }

    /// The load of each execution class over the live workers.
    ///
    /// Errors: `MONITORING_QUERY_FAILED`.
    pub async fn get_execution_class_report(
        &self,
    ) -> Result<ExecutionClassReport, DatabaseOperationError> {
        self.monitoring_broker("get_execution_class_report")
            .await?
            .get_execution_class_report()
            .await
    }

    async fn monitoring_broker(
        &self,
        read: &str,
    ) -> Result<Arc<PostgresBroker>, DatabaseOperationError> {
        self.health_broker(DatabaseOperationFailedCode::MonitoringQueryFailed, read)
            .await
    }

    /// The connected broker of the read handle, with the schema installed
    /// once per handle, for the read `Horsies::<read>`. A failure has the
    /// broker code `code` of the read.
    ///
    /// # Panics
    ///
    /// In a fork, without `exec`, of the process that started the core's
    /// process runtime. Nothing ran.
    async fn health_broker(
        &self,
        code: DatabaseOperationFailedCode,
        read: &str,
    ) -> Result<Arc<PostgresBroker>, DatabaseOperationError> {
        refuse_in_a_fork(format_args!("Horsies::{read}"));
        self.reads_handle()
            .initialized_broker()
            .await
            .map_err(|error| connect_failure(code, read, error))
    }
}

/// The failure of a connect or an install before the health read `read`,
/// with Python's message (`{read} failed: {error}`) and the broker error as
/// its cause. Pure.
fn connect_failure(
    code: DatabaseOperationFailedCode,
    read: &str,
    error: BrokerError,
) -> DatabaseOperationError {
    DatabaseOperationError::operation_failed(
        code,
        format!("{read} failed: {error}"),
        BrokerOperationSource::Broker(error),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_connect_failure_has_the_code_of_the_read_and_the_retry_rule_of_the_cause() {
        let failure = connect_failure(
            DatabaseOperationFailedCode::DatabasePingFailed,
            "ping_database",
            BrokerError::ListenerClosed,
        );
        assert_eq!(
            failure.code(),
            horsies_core::DatabaseErrorCode::DatabasePingFailed
        );
        assert_eq!(
            failure.retryable(),
            BrokerError::ListenerClosed.is_retryable()
        );
        assert_eq!(
            failure.message(),
            "ping_database failed: the broker's listener stopped"
        );
        assert_eq!(
            failure.failure_cause(),
            horsies_core::FailureCause::Connection
        );
    }

    #[test]
    fn a_stale_threshold_outside_one_year_is_refused_before_the_read() {
        assert!(StaleMinutes::parse(-1).is_err());
        assert!(StaleMinutes::parse(i64::from(crate::MAX_STALE_THRESHOLD_MINUTES) + 1).is_err());
        assert_eq!(
            StaleMinutes::parse(0).ok().map(StaleMinutes::minutes),
            Some(0)
        );
    }
}
