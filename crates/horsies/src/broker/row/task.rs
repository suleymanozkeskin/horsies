use chrono::{DateTime, Utc};
use serde::Serialize;
use sqlx::FromRow;

use crate::core::{TaskInfo, TaskStatus};

use crate::broker::error::BrokerError;

/// Minimal row returned by the claim CTE (just the ID).
#[derive(Debug, FromRow)]
pub struct ClaimedId {
    #[allow(dead_code)] // populated by FromRow; used as existence check
    pub id: String,
}

/// Row returned by SET_RUNNING_SQL — includes the DB-assigned started_at.
#[derive(Debug, FromRow)]
pub struct SetRunningRow {
    #[allow(dead_code)] // populated by FromRow; used as existence check
    pub id: String,
    pub started_at: DateTime<Utc>,
}

/// Task columns loaded after claiming.
///
/// This row is a claim-time snapshot threaded through dispatch into finalize
/// with NO re-read: `should_retry`, the retry-delay math, and the
/// expired-vs-reclaimed postcheck all consume it directly. That is sound
/// under three invariants the schema and statements maintain:
///
/// 1. `max_retries`, `good_until`, `task_options`, `is_workflow_task`, and
///    `queue_name` are frozen at enqueue — no statement mutates them.
/// 2. `retry_count` changes only in statements that simultaneously move the
///    row out of RUNNING-owned (the owner's `REQUEUE_SQL`) or clear
///    ownership (reaper requeue).
/// 3. Every terminal/requeue write CASes on
///    `status = 'RUNNING' AND claimed_by_worker_id = <me>`; a CAS miss
///    aborts the finalize.
///
/// Together these make the snapshot provably equal to the row whenever a
/// write lands. A feature that mutates `retry_count` (or any column above)
/// on a live RUNNING-owned row breaks invariant 2 and must revisit the
/// finalize/retry flow, not just its own write.
#[derive(Debug, Clone, FromRow)]
pub struct ClaimedTaskRow {
    pub id: String,
    pub task_name: String,
    pub args: Option<String>,
    pub kwargs: Option<String>,
    pub retry_count: i32,
    pub max_retries: i32,
    pub task_options: Option<String>,
    pub queue_name: String,
    pub good_until: Option<DateTime<Utc>>,
    pub is_workflow_task: bool,
}

/// Columns fetched for result retrieval.
#[derive(Debug, FromRow)]
pub struct TaskResultRow {
    pub id: String,
    pub status: String,
    pub result: Option<String>,
    pub failed_reason: Option<String>,
}

/// Columns fetched for task info.
#[derive(Debug, FromRow)]
pub struct TaskInfoRow {
    pub id: String,
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
}

impl TaskInfoRow {
    /// Convert into the core `TaskInfo` type.
    pub fn into_task_info(self) -> Result<TaskInfo, BrokerError> {
        let status = parse_task_status(&self.status)?;
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

/// Context row from SELECT_RUNNING_TASK_CONTEXT_SQL (locked with FOR UPDATE).
#[derive(Debug, FromRow)]
pub struct TaskRunningContextRow {
    pub retry_count: i32,
    pub started_at: Option<DateTime<Utc>>,
    pub claimed_by_worker_id: Option<String>,
    pub worker_hostname: Option<String>,
    pub worker_pid: Option<i32>,
    pub worker_process_name: Option<String>,
}

/// Row returned by SELECT_TASK_ATTEMPTS_SQL.
#[derive(Debug, Clone, Serialize, FromRow)]
pub struct TaskAttemptRow {
    pub task_id: String,
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
#[derive(Debug, Clone, Serialize, FromRow)]
pub struct StaleTaskRow {
    pub id: String,
    pub task_name: String,
    pub worker_hostname: Option<String>,
    pub worker_pid: Option<i32>,
    pub worker_process_name: Option<String>,
    pub started_at: Option<DateTime<Utc>>,
    pub last_heartbeat: Option<DateTime<Utc>>,
}

/// Row returned by the expired-tasks monitoring query.
///
/// Represents a PENDING task whose `good_until` deadline has passed,
/// meaning it will never be claimed by a worker.
#[derive(Debug, Clone, Serialize, FromRow)]
pub struct ExpiredTaskRow {
    pub id: String,
    pub task_name: String,
    pub queue_name: String,
    pub priority: i32,
    pub sent_at: Option<DateTime<Utc>>,
    pub good_until: Option<DateTime<Utc>>,
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
}
