use serde::{Deserialize, Serialize};

/// Status of a workflow instance.
///
/// State machine:
///   PENDING → RUNNING → COMPLETED
///                      → FAILED (on task failure with on_error=FAIL)
///                      → PAUSED (on task failure with on_error=PAUSE)
///                      → CANCELLED (user requested)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum WorkflowStatus {
    /// Created but not yet started.
    Pending,
    /// At least one task executing or ready.
    Running,
    /// All tasks terminal and success criteria met.
    Completed,
    /// A task failed and on_error=FAIL (or no success case satisfied).
    Failed,
    /// A task failed with on_error=PAUSE; awaiting resume() or cancel().
    Paused,
    /// User cancelled via WorkflowHandle.cancel().
    Cancelled,
}

impl WorkflowStatus {
    /// Whether this status represents a final state (no further transitions).
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Completed | Self::Failed | Self::Cancelled)
    }
}

/// Terminal workflow states (no further transitions).
pub const WORKFLOW_TERMINAL_STATES: &[WorkflowStatus] = &[
    WorkflowStatus::Completed,
    WorkflowStatus::Failed,
    WorkflowStatus::Cancelled,
];

impl std::fmt::Display for WorkflowStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Pending => write!(f, "PENDING"),
            Self::Running => write!(f, "RUNNING"),
            Self::Completed => write!(f, "COMPLETED"),
            Self::Failed => write!(f, "FAILED"),
            Self::Paused => write!(f, "PAUSED"),
            Self::Cancelled => write!(f, "CANCELLED"),
        }
    }
}

/// Status of a single task/node within a workflow.
///
/// State machine:
///   PENDING → READY → ENQUEUED → RUNNING → COMPLETED
///                                         → FAILED
///                   → SKIPPED (deps failed and allow_failed_deps=False)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum WorkflowTaskStatus {
    /// Waiting for dependencies to become terminal.
    Pending,
    /// Dependencies satisfied, waiting to be enqueued.
    Ready,
    /// Task created in tasks table, waiting for worker.
    Enqueued,
    /// Worker is executing the task (or child workflow is running).
    Running,
    /// Task succeeded (result is Ok).
    Completed,
    /// Task failed (result is Err).
    Failed,
    /// Skipped due to upstream failure, condition, or quorum impossible.
    Skipped,
}

impl WorkflowTaskStatus {
    /// Whether this status represents a final state (no further transitions).
    pub fn is_terminal(self) -> bool {
        matches!(self, Self::Completed | Self::Failed | Self::Skipped)
    }
}

/// Terminal workflow task states (no further transitions).
pub const WORKFLOW_TASK_TERMINAL_STATES: &[WorkflowTaskStatus] = &[
    WorkflowTaskStatus::Completed,
    WorkflowTaskStatus::Failed,
    WorkflowTaskStatus::Skipped,
];

/// Terminal workflow task status string values for SQL IN clauses.
pub const WF_TASK_TERMINAL_VALUES: &[&str] = &["COMPLETED", "FAILED", "SKIPPED"];

impl std::fmt::Display for WorkflowTaskStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Pending => write!(f, "PENDING"),
            Self::Ready => write!(f, "READY"),
            Self::Enqueued => write!(f, "ENQUEUED"),
            Self::Running => write!(f, "RUNNING"),
            Self::Completed => write!(f, "COMPLETED"),
            Self::Failed => write!(f, "FAILED"),
            Self::Skipped => write!(f, "SKIPPED"),
        }
    }
}

/// Error handling policy for workflows when a task fails.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OnError {
    /// Continue DAG resolution but mark workflow as will-fail.
    /// Skip tasks without allow_failed_deps.
    Fail,
    /// Pause workflow immediately. No new tasks enqueued until resume().
    Pause,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn workflow_status_terminal() {
        assert!(WorkflowStatus::Completed.is_terminal());
        assert!(WorkflowStatus::Failed.is_terminal());
        assert!(WorkflowStatus::Cancelled.is_terminal());
        assert!(!WorkflowStatus::Pending.is_terminal());
        assert!(!WorkflowStatus::Running.is_terminal());
        assert!(!WorkflowStatus::Paused.is_terminal());
    }

    #[test]
    fn workflow_task_status_terminal() {
        assert!(WorkflowTaskStatus::Completed.is_terminal());
        assert!(WorkflowTaskStatus::Failed.is_terminal());
        assert!(WorkflowTaskStatus::Skipped.is_terminal());
        assert!(!WorkflowTaskStatus::Pending.is_terminal());
        assert!(!WorkflowTaskStatus::Ready.is_terminal());
        assert!(!WorkflowTaskStatus::Enqueued.is_terminal());
        assert!(!WorkflowTaskStatus::Running.is_terminal());
    }

    #[test]
    fn workflow_status_serde() {
        let status = WorkflowStatus::Paused;
        let json = serde_json::to_string(&status).unwrap();
        assert_eq!(json, "\"PAUSED\"");
        let back: WorkflowStatus = serde_json::from_str(&json).unwrap();
        assert_eq!(back, status);
    }

    #[test]
    fn workflow_terminal_states_constant_matches_method() {
        for &s in WORKFLOW_TERMINAL_STATES {
            assert!(s.is_terminal(), "{:?} should be terminal", s);
        }
        let all = [
            WorkflowStatus::Pending,
            WorkflowStatus::Running,
            WorkflowStatus::Completed,
            WorkflowStatus::Failed,
            WorkflowStatus::Paused,
            WorkflowStatus::Cancelled,
        ];
        for s in all {
            assert_eq!(
                s.is_terminal(),
                WORKFLOW_TERMINAL_STATES.contains(&s),
                "{:?} mismatch",
                s,
            );
        }
    }

    #[test]
    fn workflow_task_terminal_states_constant_matches_method() {
        for &s in WORKFLOW_TASK_TERMINAL_STATES {
            assert!(s.is_terminal(), "{:?} should be terminal", s);
        }
        assert_eq!(
            WORKFLOW_TASK_TERMINAL_STATES.len(),
            WF_TASK_TERMINAL_VALUES.len()
        );
        for (&status, &value) in WORKFLOW_TASK_TERMINAL_STATES
            .iter()
            .zip(WF_TASK_TERMINAL_VALUES)
        {
            assert_eq!(status.to_string(), value);
        }
    }

    #[test]
    fn on_error_serde() {
        let policy = OnError::Pause;
        let json = serde_json::to_string(&policy).unwrap();
        assert_eq!(json, "\"pause\"");
        let back: OnError = serde_json::from_str(&json).unwrap();
        assert_eq!(back, policy);
    }
}
