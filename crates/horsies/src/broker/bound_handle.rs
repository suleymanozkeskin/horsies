use std::sync::Arc;
use std::time::Duration;

use serde::de::DeserializeOwned;
use tokio::sync::Mutex;
use uuid::Uuid;

use crate::core::task::result::TaskResult;
use crate::core::{OperationalErrorCode, TaskError, TaskInfo};

use crate::broker::postgres::PostgresBroker;

/// A task handle bound to a broker, enabling direct result retrieval.
///
/// This is the canonical user-facing task handle in Rust, mirroring Python's
/// single `TaskHandle` concept.
pub struct TaskHandle<T> {
    task_id: Uuid,
    broker: Arc<PostgresBroker>,
    cached_result: Mutex<Option<TaskResult<T>>>,
}

impl<T> TaskHandle<T> {
    /// Create a new task handle for a known task ID and broker.
    pub fn new(task_id: Uuid, broker: Arc<PostgresBroker>) -> Self {
        Self {
            task_id,
            broker,
            cached_result: Mutex::new(None),
        }
    }

    /// Get the underlying task ID.
    pub fn task_id(&self) -> Uuid {
        self.task_id
    }
}

impl<T: DeserializeOwned + Clone> TaskHandle<T> {
    /// Wait for and return the task result.
    ///
    /// Broker-level failures are folded into `TaskResult::Err(TaskError)`
    /// with `OperationalErrorCode::BrokerError`, matching Python's
    /// `TaskHandle.get()` behaviour where exceptions are never raised.
    pub async fn get(&self, timeout: Option<Duration>) -> TaskResult<T> {
        {
            let cache = self.cached_result.lock().await;
            if let Some(ref cached) = *cache {
                return cached.clone();
            }
        }

        let result = match self.broker.get_result::<T>(self.task_id, timeout).await {
            Ok(task_result) => task_result,
            Err(broker_err) => TaskResult::Err(TaskError::builtin(
                OperationalErrorCode::BrokerError,
                broker_err.to_string(),
            )),
        };

        if result.is_terminal() {
            let mut cache = self.cached_result.lock().await;
            if let Some(ref cached) = *cache {
                return cached.clone();
            }
            *cache = Some(result.clone());
        }

        result
    }

    /// Fetch task metadata without waiting for the task to complete.
    pub async fn info(
        &self,
        include_result: bool,
        include_failed_reason: bool,
        include_attempts: bool,
    ) -> crate::broker::result_types::BrokerResult<Option<TaskInfo>> {
        let mut info = self
            .broker
            .get_task_info(self.task_id, include_result, include_failed_reason)
            .await?;

        if include_attempts {
            if let Some(ref mut task_info) = info {
                let rows = self
                    .broker
                    .get_task_attempts(self.task_id)
                    .await
                    .map_err(|e| crate::broker::result_types::BrokerOperationError {
                        code: crate::broker::result_types::BrokerErrorCode::TaskInfoQueryFailed,
                        message: format!("{}", e),
                        retryable: e.is_retryable(),
                    })?;
                let attempts: Vec<crate::core::TaskAttemptInfo> = rows
                    .into_iter()
                    .map(|row| crate::core::TaskAttemptInfo {
                        task_id: row.task_id,
                        attempt: row.attempt,
                        outcome: parse_attempt_outcome(&row.outcome),
                        will_retry: row.will_retry,
                        started_at: row.started_at,
                        finished_at: row.finished_at,
                        error_code: row.error_code,
                        error_message: row.error_message,
                        failed_reason: row.failed_reason,
                        worker_id: row.worker_id,
                        worker_hostname: row.worker_hostname,
                        worker_pid: row.worker_pid,
                        worker_process_name: row.worker_process_name,
                    })
                    .collect();
                task_info.attempts = Some(attempts);
            }
        }

        Ok(info)
    }
}

/// Parse an attempt outcome string from the database into a `TaskAttemptOutcome`.
fn parse_attempt_outcome(s: &str) -> crate::core::TaskAttemptOutcome {
    match s {
        "COMPLETED" => crate::core::TaskAttemptOutcome::Completed,
        "FAILED" => crate::core::TaskAttemptOutcome::Failed,
        "WORKER_FAILURE" => crate::core::TaskAttemptOutcome::WorkerFailure,
        _ => crate::core::TaskAttemptOutcome::Failed,
    }
}

impl<T> std::fmt::Debug for TaskHandle<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TaskHandle")
            .field("task_id", &self.task_id)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn task_handle_implements_debug() {
        fn assert_debug<T: std::fmt::Debug>() {}
        assert_debug::<TaskHandle<i32>>();
        assert_debug::<TaskHandle<String>>();
    }

    #[test]
    fn task_handle_is_send_sync() {
        fn assert_send<T: Send>() {}
        fn assert_sync<T: Sync>() {}
        assert_send::<TaskHandle<i32>>();
        assert_sync::<TaskHandle<i32>>();
    }
}
