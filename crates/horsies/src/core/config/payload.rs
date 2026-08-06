//! Payload-size guardrail applied at the encode boundaries.
//!
//! Producers call [`enforce_payload_policy`] with the length of the
//! already-serialized JSON string; the check is one integer comparison per
//! enqueue/result, no extra serialization pass. The warning is rate-limited
//! to once per (task_name, kind) per process so a task that persistently
//! ships oversized payloads warns once instead of on every enqueue.
//! Rejection semantics belong to the caller: this module reports the
//! violation, the producer decides how to fail.

use std::collections::HashSet;
use std::sync::{Mutex, OnceLock};

use serde::{Deserialize, Serialize};

/// Size guardrail for serialized task payloads.
///
/// Checked against the length of the already-serialized JSON string at the
/// encode boundary. Length is in bytes of the serialized string, which for
/// JSON wire payloads approximates the stored size; the guardrail does not
/// require byte exactness.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PayloadPolicy {
    /// Warn when a serialized payload exceeds this many bytes (once per
    /// task_name and payload kind per process); `None` disables the warning.
    #[serde(default = "default_warn_bytes")]
    pub warn_bytes: Option<u64>,

    /// Reject an enqueue whose serialized payload exceeds this many bytes
    /// (typed `PAYLOAD_TOO_LARGE` error before any row is written); results
    /// are never rejected — the work is done, and destroying it would convert
    /// a size concern into data loss. `None` (default) disables rejection.
    #[serde(default)]
    pub reject_bytes: Option<u64>,
}

fn default_warn_bytes() -> Option<u64> {
    Some(1_048_576) // 1 MiB
}

impl Default for PayloadPolicy {
    fn default() -> Self {
        Self {
            warn_bytes: Some(1_048_576),
            reject_bytes: None,
        }
    }
}

impl PayloadPolicy {
    /// Validate threshold values. Zero is a configuration error — `None` is
    /// the way to disable a threshold.
    pub fn validate(&self) -> Vec<String> {
        let mut errors = Vec::new();
        if self.warn_bytes == Some(0) {
            errors.push("payload.warn_bytes must be >= 1 (use None to disable)".to_owned());
        }
        if self.reject_bytes == Some(0) {
            errors.push("payload.reject_bytes must be >= 1 (use None to disable)".to_owned());
        }
        errors
    }
}

/// Which payload a guardrail check concerns.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PayloadKind {
    /// Serialized enqueue arguments (args + kwargs).
    Kwargs,
    /// Serialized terminal result envelope.
    Result,
}

impl PayloadKind {
    fn as_str(self) -> &'static str {
        match self {
            Self::Kwargs => "kwargs",
            Self::Result => "result",
        }
    }
}

/// Process-wide warn rate-limit registry.
fn warned() -> &'static Mutex<HashSet<(String, &'static str)>> {
    static WARNED: OnceLock<Mutex<HashSet<(String, &'static str)>>> = OnceLock::new();
    WARNED.get_or_init(|| Mutex::new(HashSet::new()))
}

/// Clear the warn rate-limit registry (test isolation).
pub fn reset_payload_warnings() {
    warned().lock().expect("payload warn registry poisoned").clear();
}

/// Apply the payload policy to one serialized payload.
///
/// Emits the rate-limited warning when `encoded_len` exceeds
/// `policy.warn_bytes`. Returns `Some(encoded_len)` when it exceeds
/// `policy.reject_bytes` (the caller fails closed), `None` otherwise.
pub fn enforce_payload_policy(
    policy: &PayloadPolicy,
    task_name: &str,
    kind: PayloadKind,
    encoded_len: usize,
) -> Option<usize> {
    if let Some(warn_bytes) = policy.warn_bytes {
        if encoded_len as u64 > warn_bytes {
            let key = (task_name.to_owned(), kind.as_str());
            let mut registry = warned().lock().expect("payload warn registry poisoned");
            if registry.insert(key) {
                tracing::warn!(
                    task_name,
                    kind = kind.as_str(),
                    encoded_len,
                    warn_bytes,
                    "payload size guardrail: payload exceeds the warn threshold. Every \
                     claim and poll ships this payload; consider passing a reference \
                     instead, or raise payload.warn_bytes if this size is intended. \
                     Further warnings for this task/kind are suppressed for this process.",
                );
            }
        }
    }
    if let Some(reject_bytes) = policy.reject_bytes {
        if encoded_len as u64 > reject_bytes {
            return Some(encoded_len);
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    fn policy(warn: Option<u64>, reject: Option<u64>) -> PayloadPolicy {
        PayloadPolicy {
            warn_bytes: warn,
            reject_bytes: reject,
        }
    }

    #[test]
    fn defaults_warn_1mib_reject_off() {
        let p = PayloadPolicy::default();
        assert_eq!(p.warn_bytes, Some(1_048_576));
        assert_eq!(p.reject_bytes, None);
        let from_empty: PayloadPolicy = serde_json::from_str("{}").expect("defaults");
        assert_eq!(from_empty, p);
    }

    #[test]
    fn zero_thresholds_rejected_by_validate() {
        assert!(policy(Some(1), Some(1)).validate().is_empty());
        assert!(policy(None, None).validate().is_empty());
        assert_eq!(policy(Some(0), None).validate().len(), 1);
        assert_eq!(policy(None, Some(0)).validate().len(), 1);
        assert_eq!(policy(Some(0), Some(0)).validate().len(), 2);
    }

    #[test]
    fn exactly_at_threshold_passes() {
        // Thresholds are exclusive: exactly-at passes both.
        assert_eq!(
            enforce_payload_policy(&policy(Some(10), Some(10)), "t", PayloadKind::Kwargs, 10),
            None,
        );
    }

    #[test]
    fn reject_returns_len_and_disabled_thresholds_pass() {
        assert_eq!(
            enforce_payload_policy(&policy(None, Some(10)), "t", PayloadKind::Kwargs, 11),
            Some(11),
        );
        assert_eq!(
            enforce_payload_policy(&policy(None, None), "t", PayloadKind::Kwargs, usize::MAX),
            None,
        );
    }

    #[test]
    fn warn_rate_limited_per_task_and_kind() {
        reset_payload_warnings();
        let p = policy(Some(1), None);
        // Two oversize calls for the same (task, kind): the registry holds
        // one entry — the second insert returns false (warning suppressed).
        enforce_payload_policy(&p, "warn_rl_task", PayloadKind::Kwargs, 5);
        enforce_payload_policy(&p, "warn_rl_task", PayloadKind::Kwargs, 6);
        enforce_payload_policy(&p, "warn_rl_task", PayloadKind::Result, 7);
        let registry = warned().lock().expect("poisoned");
        assert!(registry.contains(&("warn_rl_task".to_owned(), "kwargs")));
        assert!(registry.contains(&("warn_rl_task".to_owned(), "result")));
        drop(registry);
        reset_payload_warnings();
        assert!(warned().lock().expect("poisoned").is_empty());
    }
}
