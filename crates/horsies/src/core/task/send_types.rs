/// Typed error types for task send/schedule operations.
///
/// Mirrors Python's `horsies/core/models/task_send_types.py`.
use chrono::{DateTime, Utc};

/// Categorized task send failure codes.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum TaskSendErrorCode {
    /// Send called while suppression is active (worker/scheduler import phase,
    /// `check()`, or `HORSIES_SUPPRESS_SENDS=1`). Non-retryable.
    SendSuppressed,
    /// Programmer error (bad args, serialization failure, unresolved queue).
    ValidationFailed,
    /// Infrastructure error (schema init, DB transaction). Conditionally retryable.
    EnqueueFailed,
    /// Task ID collision with different payload SHA. Non-retryable.
    PayloadMismatch,
}

impl std::fmt::Display for TaskSendErrorCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::SendSuppressed => write!(f, "SEND_SUPPRESSED"),
            Self::ValidationFailed => write!(f, "VALIDATION_FAILED"),
            Self::EnqueueFailed => write!(f, "ENQUEUE_FAILED"),
            Self::PayloadMismatch => write!(f, "PAYLOAD_MISMATCH"),
        }
    }
}

/// Serialized envelope for idempotent retry.
///
/// All value fields are pre-serialized to avoid round-trip issues.
/// The retry methods extract this payload directly — no user args needed.
#[derive(Debug, Clone)]
pub struct TaskSendPayload {
    pub task_name: String,
    pub queue_name: String,
    pub priority: i32,
    pub args_json: Option<String>,
    pub kwargs_json: Option<String>,
    pub sent_at: DateTime<Utc>,
    pub good_until: Option<DateTime<Utc>>,
    pub enqueue_delay_seconds: Option<i64>,
    pub task_options: Option<String>,
    pub enqueue_sha: String,
}

/// Error from task send/schedule operations.
#[derive(Debug, Clone)]
pub struct TaskSendError {
    /// Which failure category.
    pub code: TaskSendErrorCode,
    /// Human-readable description.
    pub message: String,
    /// Whether the caller can retry with the same task_id.
    pub retryable: bool,
    /// Generated task ID (None for SendSuppressed).
    pub task_id: Option<String>,
    /// Serialized envelope for replay (None when no serialization happened).
    pub payload: Option<TaskSendPayload>,
}

impl std::fmt::Display for TaskSendError {
    /// Redacted display — never leak args/kwargs in logs.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "TaskSendError(code={}, retryable={}, task_id={:?})",
            self.code, self.retryable, self.task_id,
        )
    }
}

impl std::error::Error for TaskSendError {}

/// Result type for task send/schedule operations.
///
/// `Ok(T)` on success (typically `TaskHandle<T>`),
/// `Err(TaskSendError)` on failure.
pub type TaskSendResult<T> = Result<T, TaskSendError>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn error_code_display() {
        assert_eq!(
            TaskSendErrorCode::SendSuppressed.to_string(),
            "SEND_SUPPRESSED"
        );
        assert_eq!(
            TaskSendErrorCode::EnqueueFailed.to_string(),
            "ENQUEUE_FAILED"
        );
    }

    #[test]
    fn task_send_error_display_redacted() {
        let err = TaskSendError {
            code: TaskSendErrorCode::EnqueueFailed,
            message: "connection refused".to_owned(),
            retryable: true,
            task_id: Some("abc-123".to_owned()),
            payload: Some(TaskSendPayload {
                task_name: "my_task".to_owned(),
                queue_name: "default".to_owned(),
                priority: 100,
                args_json: Some("sensitive data".to_owned()),
                kwargs_json: None,
                sent_at: Utc::now(),
                good_until: None,
                enqueue_delay_seconds: None,
                task_options: None,
                enqueue_sha: "deadbeef".to_owned(),
            }),
        };
        let display = format!("{}", err);
        // Must NOT contain the sensitive args
        assert!(!display.contains("sensitive data"));
        assert!(display.contains("ENQUEUE_FAILED"));
        assert!(display.contains("abc-123"));
    }
}
