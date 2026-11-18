//! Health reads on the app (Python `Horsies.ping_database`,
//! `ping_workers`, `list_worker_states`, `get_worker_state`,
//! `get_worker_state_history`, `get_stale_tasks`, `get_expired_tasks`, and
//! the class reads).
//!
//! Each read connects the app's handle, installs the schema once per handle
//! as a send does, and reads only. A failure is the core's
//! `BrokerOperationError` with Python's broker code: a failed connect or
//! install has the code of the read (`DB_PING_FAILED`, `WORKER_PING_FAILED`,
//! `MONITORING_QUERY_FAILED`).

use std::num::{NonZeroU32, NonZeroUsize};
use std::sync::Arc;
use std::time::Duration;

use horsies_core::{
    BoundedTaskList, BrokerError, BrokerOperationError, BrokerOperationFailedCode, ClassCoverage,
    ClassReport, DatabasePing, ExpiredTaskRow, PostgresBroker, StaleMinutes, StaleTaskRow,
    WorkerPong, WorkerStateSnapshot,
};

use crate::Horsies;

/// Which workers a ping asks.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PingTarget {
    /// Every worker that listens.
    EveryWorker,
    /// The worker with this id; the ping returns as soon as it replies.
    Worker(String),
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
    /// Errors: `DB_PING_FAILED` with the retry classification of the cause.
    pub async fn ping_database(&self) -> Result<DatabasePing, BrokerOperationError> {
        self.health_broker(BrokerOperationFailedCode::DbPingFailed)
            .await?
            .ping_database()
            .await
    }

    /// Ping the workers and collect the distinct replies by worker id,
    /// until `target` replied, `min_responses` workers replied, or
    /// `timeout` passed. A worker that is in `list_worker_states` and not
    /// here did not answer.
    ///
    /// Errors: `INVALID_INPUT` for a zero `timeout`, with no statement;
    /// `LISTENER_DISABLED` for an app whose schema mode has no listener;
    /// `WORKER_PING_FAILED`.
    pub async fn ping_workers(
        &self,
        target: PingTarget,
        timeout: Duration,
        min_responses: Option<NonZeroUsize>,
    ) -> Result<Vec<WorkerPong>, BrokerOperationError> {
        let target_worker_id = match &target {
            PingTarget::EveryWorker => None,
            PingTarget::Worker(worker_id) => Some(worker_id.as_str()),
        };
        self.health_broker(BrokerOperationFailedCode::WorkerPingFailed)
            .await?
            .ping_workers(
                target_worker_id,
                timeout,
                min_responses.map(NonZeroUsize::get),
            )
            .await
    }

    /// The latest state snapshot of every worker that reported one, idle
    /// workers included.
    ///
    /// Errors: `MONITORING_QUERY_FAILED`, also for a stored snapshot of
    /// another shape, which names the worker and the column.
    pub async fn list_worker_states(
        &self,
    ) -> Result<Vec<WorkerStateSnapshot>, BrokerOperationError> {
        self.monitoring_broker().await?.list_worker_states().await
    }

    /// The latest state snapshot of worker `worker_id`, or `None` when it
    /// never reported one.
    ///
    /// Errors: as `list_worker_states`.
    pub async fn get_worker_state(
        &self,
        worker_id: &str,
    ) -> Result<Option<WorkerStateSnapshot>, BrokerOperationError> {
        self.monitoring_broker()
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
    ) -> Result<Vec<WorkerStateSnapshot>, BrokerOperationError> {
        let limit = match limit {
            WorkerHistoryLimit::AllRetained => None,
            WorkerHistoryLimit::Newest(count) => Some(i64::from(count.get())),
        };
        self.monitoring_broker()
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
    ) -> Result<BoundedTaskList<StaleTaskRow>, BrokerOperationError> {
        self.monitoring_broker()
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
    ) -> Result<BoundedTaskList<ExpiredTaskRow>, BrokerOperationError> {
        self.monitoring_broker().await?.get_expired_tasks().await
    }

    /// For each pending stream of an execution class, whether a live worker
    /// can run it (the fleet check).
    ///
    /// Errors: `MONITORING_QUERY_FAILED`.
    pub async fn get_class_coverage(&self) -> Result<ClassCoverage, BrokerOperationError> {
        self.monitoring_broker().await?.get_class_coverage().await
    }

    /// The load of each execution class over the live workers.
    ///
    /// Errors: `MONITORING_QUERY_FAILED`.
    pub async fn get_class_report(&self) -> Result<ClassReport, BrokerOperationError> {
        self.monitoring_broker().await?.get_class_report().await
    }

    async fn monitoring_broker(&self) -> Result<Arc<PostgresBroker>, BrokerOperationError> {
        self.health_broker(BrokerOperationFailedCode::MonitoringQueryFailed)
            .await
    }

    /// The connected broker of the read handle, with the schema installed
    /// once per handle. A failure has the broker code `code` of the read.
    async fn health_broker(
        &self,
        code: BrokerOperationFailedCode,
    ) -> Result<Arc<PostgresBroker>, BrokerOperationError> {
        self.reads_handle()
            .initialized_broker()
            .await
            .map_err(|error| connect_failure(code, &error))
    }
}

/// The failure of a connect or an install before a health read. Pure.
fn connect_failure(code: BrokerOperationFailedCode, error: &BrokerError) -> BrokerOperationError {
    BrokerOperationError::OperationFailed {
        code,
        message: error.to_string(),
        retryable: error.is_retryable(),
        cause: error.failure_cause(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_connect_failure_has_the_code_of_the_read_and_the_retry_rule_of_the_cause() {
        let failure = connect_failure(
            BrokerOperationFailedCode::DbPingFailed,
            &BrokerError::ListenerClosed,
        );
        assert_eq!(failure.code(), horsies_core::BrokerErrorCode::DbPingFailed);
        assert_eq!(
            failure.retryable(),
            BrokerError::ListenerClosed.is_retryable()
        );
        assert!(!failure.message().is_empty());
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
