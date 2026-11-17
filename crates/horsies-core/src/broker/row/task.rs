use chrono::{DateTime, Utc};
use serde::Serialize;
use sqlx::FromRow;
use uuid::Uuid;

use crate::core::{Execution, ExecutionClass, TaskInfo, TaskStatus};

use crate::broker::error::BrokerError;

/// Columns fetched for result retrieval.
#[derive(Debug, FromRow)]
pub struct TaskResultRow {
    pub id: Uuid,
    pub status: String,
    pub result: Option<String>,
    pub failed_reason: Option<String>,
}

/// Columns fetched for task info.
#[derive(Debug, FromRow)]
pub struct TaskInfoRow {
    pub id: Uuid,
    pub task_name: String,
    pub status: String,
    pub queue_name: String,
    pub priority: i32,
    pub retry_count: i32,
    pub max_retries: i32,
    pub next_retry_at: Option<DateTime<Utc>>,
    pub sent_at: Option<DateTime<Utc>>,
    pub enqueued_at: DateTime<Utc>,
    pub claimed_at: Option<DateTime<Utc>>,
    pub started_at: Option<DateTime<Utc>>,
    pub completed_at: Option<DateTime<Utc>>,
    pub failed_at: Option<DateTime<Utc>>,
    pub worker_hostname: Option<String>,
    pub worker_pid: Option<i32>,
    pub worker_process_name: Option<String>,
    pub error_code: Option<String>,
    pub failed_reason: Option<String>,
    pub result: Option<String>,
    /// The live row's `execution` column.
    pub execution: String,
}

impl TaskInfoRow {
    /// Convert into the core `TaskInfo` type.
    ///
    /// The task is live, so its info reports the stored execution class. A
    /// stored class outside the three class names is refused with
    /// `BrokerError::InvalidExecutionClass`; the column's CHECK holds the
    /// domain, so that refusal means the row was written past it.
    pub fn into_task_info(self) -> Result<TaskInfo, BrokerError> {
        let status = parse_task_status(&self.status)?;
        let execution = Execution::from_stored(&self.execution)
            .ok_or_else(|| BrokerError::InvalidExecutionClass(self.execution.clone()))?;
        // Propagate (not swallow) a present-but-unparseable stored result so it
        // is not reported identically to a genuinely null result. Matches the
        // retrieval path in `parse_task_result_row`.
        let result = match self.result {
            Some(raw) => Some(serde_json::from_str(&raw).map_err(BrokerError::Serialization)?),
            None => None,
        };
        Ok(TaskInfo {
            task_id: self.id,
            task_name: self.task_name,
            status,
            queue_name: self.queue_name,
            priority: self.priority,
            retry_count: self.retry_count as u32,
            max_retries: self.max_retries as u32,
            next_retry_at: self.next_retry_at,
            sent_at: self.sent_at,
            enqueued_at: self.enqueued_at,
            claimed_at: self.claimed_at,
            started_at: self.started_at,
            completed_at: self.completed_at,
            failed_at: self.failed_at,
            worker_hostname: self.worker_hostname,
            worker_pid: self.worker_pid.map(|p| p as u32),
            worker_process_name: self.worker_process_name,
            error_code: self.error_code,
            failed_reason: self.failed_reason,
            result,
            attempts: None,
            execution: ExecutionClass::Live(execution),
        })
    }
}

/// Parse a status string from the database into `TaskStatus`.
pub fn parse_task_status(s: &str) -> Result<TaskStatus, BrokerError> {
    match s {
        "PENDING" => Ok(TaskStatus::Pending),
        "CLAIMED" => Ok(TaskStatus::Claimed),
        "RUNNING" => Ok(TaskStatus::Running),
        "COMPLETED" => Ok(TaskStatus::Completed),
        "FAILED" => Ok(TaskStatus::Failed),
        "CANCELLED" => Ok(TaskStatus::Cancelled),
        "EXPIRED" => Ok(TaskStatus::Expired),
        _ => Err(BrokerError::InvalidStatus(s.to_owned())),
    }
}

// ---------------------------------------------------------------------------
// Task attempt row types
// ---------------------------------------------------------------------------

/// Row returned by SELECT_TASK_ATTEMPTS_SQL.
#[derive(Debug, Clone, Serialize, FromRow)]
pub struct TaskAttemptRow {
    pub task_id: Uuid,
    pub attempt: i32,
    pub outcome: String,
    pub will_retry: bool,
    pub started_at: DateTime<Utc>,
    pub finished_at: DateTime<Utc>,
    pub error_code: Option<String>,
    pub error_message: Option<String>,
    pub failed_reason: Option<String>,
    pub worker_id: Option<String>,
    pub worker_hostname: Option<String>,
    pub worker_pid: Option<i32>,
    pub worker_process_name: Option<String>,
}

// ---------------------------------------------------------------------------
// Monitoring / observability row types
// ---------------------------------------------------------------------------

/// Row returned by the stale-tasks monitoring query.
///
/// Represents a RUNNING task whose worker has not sent a heartbeat within
/// the configured threshold, indicating the worker may have crashed.
/// `started_at` is set: the query selects only rows that started before the
/// threshold. `last_heartbeat` is `None` for a task with no runner beat.
#[derive(Debug, Clone, Serialize, FromRow)]
pub struct StaleTaskRow {
    pub id: Uuid,
    pub task_name: String,
    pub worker_hostname: Option<String>,
    pub worker_pid: Option<i32>,
    pub worker_process_name: Option<String>,
    pub started_at: DateTime<Utc>,
    pub last_heartbeat: Option<DateTime<Utc>>,
}

/// Row returned by the expired-tasks monitoring query.
///
/// Represents a PENDING task whose `good_until` deadline has passed,
/// meaning it will never be claimed by a worker.
#[derive(Debug, Clone, Serialize, FromRow)]
pub struct ExpiredTaskRow {
    pub id: Uuid,
    pub task_name: String,
    pub queue_name: String,
    pub priority: i32,
    pub sent_at: DateTime<Utc>,
    pub enqueued_at: DateTime<Utc>,
    pub good_until: DateTime<Utc>,
    /// How long ago `good_until` passed, in microseconds, on the database
    /// clock: the exact length of Python's `expired_for` interval.
    pub expired_for_us: i64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_all_status_variants() {
        assert_eq!(parse_task_status("PENDING").unwrap(), TaskStatus::Pending);
        assert_eq!(parse_task_status("CLAIMED").unwrap(), TaskStatus::Claimed);
        assert_eq!(parse_task_status("RUNNING").unwrap(), TaskStatus::Running);
        assert_eq!(
            parse_task_status("COMPLETED").unwrap(),
            TaskStatus::Completed
        );
        assert_eq!(parse_task_status("FAILED").unwrap(), TaskStatus::Failed);
        assert_eq!(
            parse_task_status("CANCELLED").unwrap(),
            TaskStatus::Cancelled
        );
        assert_eq!(parse_task_status("EXPIRED").unwrap(), TaskStatus::Expired);
    }

    #[test]
    fn parse_invalid_status() {
        assert!(parse_task_status("UNKNOWN").is_err());
        assert!(parse_task_status("pending").is_err());
        assert!(parse_task_status("").is_err());
    }

    fn task_info_row(execution: &str) -> TaskInfoRow {
        TaskInfoRow {
            id: Uuid::nil(),
            task_name: "row_task".to_owned(),
            status: "PENDING".to_owned(),
            queue_name: "default".to_owned(),
            priority: 100,
            retry_count: 0,
            max_retries: 0,
            next_retry_at: None,
            sent_at: None,
            enqueued_at: Utc::now(),
            claimed_at: None,
            started_at: None,
            completed_at: None,
            failed_at: None,
            worker_hostname: None,
            worker_pid: None,
            worker_process_name: None,
            error_code: None,
            failed_reason: None,
            result: None,
            execution: execution.to_owned(),
        }
    }

    #[test]
    fn live_task_info_reports_the_stored_execution_class() {
        for (stored, execution) in [
            ("ISOLATED", Execution::Isolated),
            ("SHARED_ASYNC", Execution::SharedAsync),
            ("SHARED_THREAD", Execution::SharedThread),
        ] {
            let info = task_info_row(stored).into_task_info().unwrap();
            assert_eq!(info.execution, ExecutionClass::Live(execution));
        }
    }

    #[test]
    fn live_task_info_refuses_an_unknown_execution_class() {
        let refused = task_info_row("PARALLEL").into_task_info().unwrap_err();
        assert!(
            matches!(&refused, BrokerError::InvalidExecutionClass(value) if value == "PARALLEL"),
            "{refused:?}"
        );
    }
}
