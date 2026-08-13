use serde::{Deserialize, Serialize};

/// Task execution status.
///
/// State machine:
///   PENDING → CLAIMED → RUNNING → COMPLETED
///                                → FAILED
///                                → CANCELLED
///   PENDING → EXPIRED (good_until passed before claim)
///   CLAIMED → EXPIRED (good_until passed before execution started)
///   RUNNING → PENDING (retry on failure/crash)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum TaskStatus {
    /// Awaiting execution. Default status when the task is sent.
    Pending,
    /// Claimed by a worker but not yet started executing.
    Claimed,
    /// Being executed by a worker.
    Running,
    /// Executed successfully.
    Completed,
    /// Failed to execute.
    Failed,
    /// Cancelled by the user or system.
    Cancelled,
    /// good_until passed before execution started. Terminal.
    Expired,
}

impl TaskStatus {
    pub const ALL: [Self; 7] = [
        Self::Pending,
        Self::Claimed,
        Self::Running,
        Self::Completed,
        Self::Failed,
        Self::Cancelled,
        Self::Expired,
    ];

    /// Whether this status represents a final state (no further transitions).
    pub fn is_terminal(self) -> bool {
        matches!(
            self,
            Self::Completed | Self::Failed | Self::Cancelled | Self::Expired
        )
    }
}

/// Terminal task states (no further transitions).
pub const TASK_TERMINAL_STATES: &[TaskStatus] = &[
    TaskStatus::Completed,
    TaskStatus::Failed,
    TaskStatus::Cancelled,
    TaskStatus::Expired,
];

/// Terminal task status string values for SQL IN clauses.
pub const TASK_TERMINAL_VALUES: &[&str] = &["COMPLETED", "FAILED", "CANCELLED", "EXPIRED"];

impl std::fmt::Display for TaskStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Pending => write!(f, "PENDING"),
            Self::Claimed => write!(f, "CLAIMED"),
            Self::Running => write!(f, "RUNNING"),
            Self::Completed => write!(f, "COMPLETED"),
            Self::Failed => write!(f, "FAILED"),
            Self::Cancelled => write!(f, "CANCELLED"),
            Self::Expired => write!(f, "EXPIRED"),
        }
    }
}

impl std::str::FromStr for TaskStatus {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "PENDING" => Ok(Self::Pending),
            "CLAIMED" => Ok(Self::Claimed),
            "RUNNING" => Ok(Self::Running),
            "COMPLETED" => Ok(Self::Completed),
            "FAILED" => Ok(Self::Failed),
            "CANCELLED" => Ok(Self::Cancelled),
            "EXPIRED" => Ok(Self::Expired),
            other => Err(format!("not a task status: {other:?}")),
        }
    }
}

/// Outcome of a single task execution attempt.
///
/// Recorded in `horsies_task_attempts.outcome`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum TaskAttemptOutcome {
    /// Task completed successfully.
    Completed,
    /// Task returned a domain or library error (with a `TaskResult`).
    Failed,
    /// Worker process crashed without producing a `TaskResult`.
    WorkerFailure,
}

impl std::fmt::Display for TaskAttemptOutcome {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Completed => write!(f, "COMPLETED"),
            Self::Failed => write!(f, "FAILED"),
            Self::WorkerFailure => write!(f, "WORKER_FAILURE"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn terminal_states() {
        assert!(TaskStatus::Completed.is_terminal());
        assert!(TaskStatus::Failed.is_terminal());
        assert!(TaskStatus::Cancelled.is_terminal());
        assert!(TaskStatus::Expired.is_terminal());
    }

    #[test]
    fn non_terminal_states() {
        assert!(!TaskStatus::Pending.is_terminal());
        assert!(!TaskStatus::Claimed.is_terminal());
        assert!(!TaskStatus::Running.is_terminal());
    }

    #[test]
    fn serde_round_trip() {
        let status = TaskStatus::Completed;
        let json = serde_json::to_string(&status).unwrap();
        assert_eq!(json, "\"COMPLETED\"");
        let back: TaskStatus = serde_json::from_str(&json).unwrap();
        assert_eq!(back, status);
    }

    #[test]
    fn display() {
        assert_eq!(TaskStatus::Pending.to_string(), "PENDING");
        assert_eq!(TaskStatus::Running.to_string(), "RUNNING");
    }

    #[test]
    fn terminal_states_constant_matches_method() {
        for &s in TASK_TERMINAL_STATES {
            assert!(s.is_terminal(), "{:?} should be terminal", s);
        }
        for s in TaskStatus::ALL {
            assert_eq!(
                s.is_terminal(),
                TASK_TERMINAL_STATES.contains(&s),
                "{:?} mismatch",
                s,
            );
        }
    }

    #[test]
    fn terminal_values_match_states() {
        assert_eq!(TASK_TERMINAL_STATES.len(), TASK_TERMINAL_VALUES.len());
        for (&status, &value) in TASK_TERMINAL_STATES.iter().zip(TASK_TERMINAL_VALUES) {
            assert_eq!(status.to_string(), value);
        }
    }
}
