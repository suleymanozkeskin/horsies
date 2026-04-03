use serde::{Deserialize, Serialize};

use super::error::TaskErrorCode;

/// Backoff strategy for retry intervals.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BackoffStrategy {
    /// Uses intervals list exactly as specified.
    Fixed,
    /// Uses intervals[0] as base, exponentially increases: base * 2^(attempt-1).
    Exponential,
}

/// Retry policy configuration for tasks.
///
/// Two strategies supported:
/// 1. Fixed: uses intervals list exactly as specified
/// 2. Exponential: uses a single base interval, exponentially increases
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RetryPolicy {
    /// Maximum number of retry attempts (1-20). Initial send not counted.
    pub max_retries: u32,
    /// Delay intervals in seconds between retry attempts.
    pub intervals: Vec<u32>,
    /// Backoff strategy.
    pub backoff_strategy: BackoffStrategy,
    /// Whether to add +/-25% randomization to delays.
    pub jitter: bool,
}

/// Validation error for RetryPolicy construction.
#[derive(Debug, Clone, thiserror::Error)]
pub enum RetryPolicyError {
    #[error("max_retries must be between 1 and 20, got {0}")]
    MaxRetriesOutOfRange(u32),

    #[error("intervals must not be empty")]
    EmptyIntervals,

    #[error("interval values must be between 1 and 86400, got {0}")]
    IntervalOutOfRange(u32),

    #[error(
        "fixed backoff requires intervals length ({intervals_len}) to match max_retries ({max_retries})"
    )]
    FixedIntervalsMismatch {
        intervals_len: usize,
        max_retries: u32,
    },

    #[error("exponential backoff requires exactly one base interval, got {0}")]
    ExponentialMultipleIntervals(usize),
}

impl RetryPolicy {
    /// Create a fixed backoff policy where intervals length defines max_retries.
    pub fn fixed(intervals: Vec<u32>, jitter: bool) -> Result<Self, RetryPolicyError> {
        if intervals.is_empty() {
            return Err(RetryPolicyError::EmptyIntervals);
        }
        for &interval in &intervals {
            if interval == 0 || interval > 86400 {
                return Err(RetryPolicyError::IntervalOutOfRange(interval));
            }
        }
        let max_retries = intervals.len() as u32;
        if max_retries > 20 {
            return Err(RetryPolicyError::MaxRetriesOutOfRange(max_retries));
        }
        Ok(Self {
            max_retries,
            intervals,
            backoff_strategy: BackoffStrategy::Fixed,
            jitter,
        })
    }

    /// Create an exponential backoff policy using a single base interval.
    ///
    /// Delay per attempt: base_seconds * 2^(attempt-1).
    pub fn exponential(
        base_seconds: u32,
        max_retries: u32,
        jitter: bool,
    ) -> Result<Self, RetryPolicyError> {
        if base_seconds == 0 || base_seconds > 86400 {
            return Err(RetryPolicyError::IntervalOutOfRange(base_seconds));
        }
        if max_retries == 0 || max_retries > 20 {
            return Err(RetryPolicyError::MaxRetriesOutOfRange(max_retries));
        }
        Ok(Self {
            max_retries,
            intervals: vec![base_seconds],
            backoff_strategy: BackoffStrategy::Exponential,
            jitter,
        })
    }

    /// Validate internal consistency (used after deserialization).
    pub fn validate(&self) -> Result<(), RetryPolicyError> {
        if self.max_retries == 0 || self.max_retries > 20 {
            return Err(RetryPolicyError::MaxRetriesOutOfRange(self.max_retries));
        }
        if self.intervals.is_empty() {
            return Err(RetryPolicyError::EmptyIntervals);
        }
        for &interval in &self.intervals {
            if interval == 0 || interval > 86400 {
                return Err(RetryPolicyError::IntervalOutOfRange(interval));
            }
        }
        match self.backoff_strategy {
            BackoffStrategy::Fixed => {
                if self.intervals.len() != self.max_retries as usize {
                    return Err(RetryPolicyError::FixedIntervalsMismatch {
                        intervals_len: self.intervals.len(),
                        max_retries: self.max_retries,
                    });
                }
            }
            BackoffStrategy::Exponential => {
                if self.intervals.len() != 1 {
                    return Err(RetryPolicyError::ExponentialMultipleIntervals(
                        self.intervals.len(),
                    ));
                }
            }
        }
        Ok(())
    }
}

/// Options for a task definition.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskOptions {
    /// Unique task identifier (decoupled from function names).
    pub task_name: String,

    /// Target queue name (None = default queue).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub queue_name: Option<String>,

    /// Task expiry deadline (task skipped if not claimed by this time).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub good_until: Option<chrono::DateTime<chrono::Utc>>,

    /// Error codes that trigger automatic retries.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub auto_retry_for: Option<Vec<TaskErrorCode>>,

    /// Retry timing and backoff configuration.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retry_policy: Option<RetryPolicy>,
}

impl TaskOptions {
    /// Serialize only the retry-relevant fields (auto_retry_for, retry_policy)
    /// into a JSON string suitable for `AnyNode::task_options_json`.
    ///
    /// Excludes `task_name`, `queue_name`, and `good_until` to avoid conflicts
    /// with the corresponding `AnyNode` fields that are authoritative for those.
    ///
    /// Returns `None` if neither `auto_retry_for` nor `retry_policy` is set.
    pub fn serialize_retry_options(&self) -> Option<String> {
        if self.auto_retry_for.is_none() && self.retry_policy.is_none() {
            return None;
        }

        let mut map = serde_json::Map::new();

        if let Some(ref codes) = self.auto_retry_for {
            let codes_json: Vec<serde_json::Value> = codes
                .iter()
                .map(|c| serde_json::Value::String(c.to_string()))
                .collect();
            map.insert(
                "auto_retry_for".to_owned(),
                serde_json::Value::Array(codes_json),
            );
        }

        if let Some(ref policy) = self.retry_policy {
            if let Ok(policy_val) = serde_json::to_value(policy) {
                map.insert("retry_policy".to_owned(), policy_val);
            }
        }

        serde_json::to_string(&map).ok()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fixed_retry_policy() {
        let policy = RetryPolicy::fixed(vec![60, 300, 900], true).unwrap();
        assert_eq!(policy.max_retries, 3);
        assert_eq!(policy.backoff_strategy, BackoffStrategy::Fixed);
        assert!(policy.validate().is_ok());
    }

    #[test]
    fn exponential_retry_policy() {
        let policy = RetryPolicy::exponential(30, 5, false).unwrap();
        assert_eq!(policy.max_retries, 5);
        assert_eq!(policy.intervals, vec![30]);
        assert_eq!(policy.backoff_strategy, BackoffStrategy::Exponential);
        assert!(policy.validate().is_ok());
    }

    #[test]
    fn fixed_mismatch_rejected() {
        // Directly construct an invalid policy and validate
        let policy = RetryPolicy {
            max_retries: 3,
            intervals: vec![60, 300],
            backoff_strategy: BackoffStrategy::Fixed,
            jitter: true,
        };
        assert!(policy.validate().is_err());
    }

    #[test]
    fn exponential_multiple_intervals_rejected() {
        let policy = RetryPolicy {
            max_retries: 3,
            intervals: vec![30, 60],
            backoff_strategy: BackoffStrategy::Exponential,
            jitter: true,
        };
        assert!(policy.validate().is_err());
    }

    #[test]
    fn zero_max_retries_rejected() {
        let result = RetryPolicy::exponential(30, 0, true);
        assert!(result.is_err());
    }

    #[test]
    fn serde_round_trip() {
        let policy = RetryPolicy::fixed(vec![60, 300, 900], true).unwrap();
        let json = serde_json::to_string(&policy).unwrap();
        let back: RetryPolicy = serde_json::from_str(&json).unwrap();
        assert_eq!(back.max_retries, policy.max_retries);
        assert_eq!(back.intervals, policy.intervals);
        assert!(back.validate().is_ok());
    }

    #[test]
    fn serialize_retry_options_with_both_fields() {
        use super::super::error::TaskErrorCode;

        let opts = TaskOptions {
            task_name: "my_task".to_owned(),
            queue_name: Some("high".to_owned()),
            good_until: None,
            auto_retry_for: Some(vec![TaskErrorCode::User("STORAGE_ERROR".to_owned())]),
            retry_policy: Some(RetryPolicy::fixed(vec![60, 300, 900], true).unwrap()),
        };

        let json = opts.serialize_retry_options().expect("should produce JSON");
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();

        // Must include retry fields.
        assert!(parsed.get("retry_policy").is_some());
        assert!(parsed.get("auto_retry_for").is_some());
        assert_eq!(parsed["auto_retry_for"][0], "STORAGE_ERROR");
        assert_eq!(parsed["retry_policy"]["max_retries"], 3);

        // Must NOT include task_name, queue_name, good_until.
        assert!(parsed.get("task_name").is_none());
        assert!(parsed.get("queue_name").is_none());
        assert!(parsed.get("good_until").is_none());
    }

    #[test]
    fn serialize_retry_options_returns_none_when_empty() {
        let opts = TaskOptions {
            task_name: "my_task".to_owned(),
            queue_name: Some("high".to_owned()),
            good_until: None,
            auto_retry_for: None,
            retry_policy: None,
        };
        assert!(opts.serialize_retry_options().is_none());
    }
}
