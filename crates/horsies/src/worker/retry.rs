use crate::core::task::error::{TaskError, TaskErrorCode};
use chrono::{DateTime, Utc};
use rand::Rng;

/// Calculate the retry delay in seconds for a given attempt.
///
/// Returns `f64` to preserve sub-second precision from jitter.
/// Floored at 1.0 second.
///
/// Reads the retry policy from the task_options JSON string.
/// Falls back to sensible defaults if parsing fails.
pub fn calculate_retry_delay(attempt: u32, task_options_json: Option<&str>) -> f64 {
    let (intervals, backoff_strategy, jitter, max_delay_seconds) =
        parse_retry_policy_fields(task_options_json);

    let base_delay: f64 = match backoff_strategy.as_str() {
        "exponential" => {
            let base = intervals.first().copied().unwrap_or(60) as f64;
            base * 2.0_f64.powi((attempt as i32) - 1)
        }
        _ => {
            // Fixed: use intervals directly, clamp to last element.
            let idx = (attempt as usize).saturating_sub(1);
            let clamped = idx.min(intervals.len().saturating_sub(1));
            intervals.get(clamped).copied().unwrap_or(60) as f64
        }
    };

    // Floor before jitter so 1.0 is the bottom of the spread, not a clamp that
    // destroys uniformity at small base delays. Jitter is applied upward, so the
    // floored value is the bottom of the jitter range (parity with horsies #78).
    let base_delay = base_delay.max(1.0);

    let delay = if jitter {
        let mut rng = rand::thread_rng();
        base_delay + rng.gen_range(0.0..=base_delay * 0.25)
    } else {
        base_delay
    };

    // Apply the opt-in max delay cap after backoff and jitter. The cap is a
    // positive integer >= 1 and base_delay is already >= 1.0, so the result
    // stays >= 1.0 without a trailing floor.
    match max_delay_seconds {
        Some(cap) if cap > 0 => delay.min(cap as f64),
        _ => delay,
    }
}

/// Determine if a failed task should be retried.
///
/// Checks:
/// 1. `retry_count < max_retries` and `max_retries > 0`
/// 2. `good_until` has not already passed (task is not expired)
/// 3. The error code matches one of the `auto_retry_for` entries in task_options
/// 4. The exception type name (from `error.exception`) matches one of the
///    `auto_retry_for` entries -- mirrors Python's support for matching on
///    exception class names (e.g. `"ConnectionError"`, `"ValueError"`)
pub fn should_retry(
    error: &TaskError,
    retry_count: i32,
    max_retries: i32,
    task_options_json: Option<&str>,
    good_until: Option<DateTime<Utc>>,
) -> bool {
    if max_retries == 0 || retry_count >= max_retries {
        return false;
    }

    // Pre-check: if the task has already expired, don't schedule a retry.
    if let Some(deadline) = good_until {
        if Utc::now() >= deadline {
            return false;
        }
    }

    let auto_retry_for = parse_auto_retry_for(task_options_json);
    if auto_retry_for.is_empty() {
        return false;
    }

    // Check 1: match on error code (library or user-defined).
    if let Some(ref error_code) = error.error_code {
        let error_code_str = match error_code {
            TaskErrorCode::BuiltIn(code) => code.to_string(),
            TaskErrorCode::User(code) => code.clone(),
        };
        if auto_retry_for.iter().any(|entry| entry == &error_code_str) {
            return true;
        }
    }

    // Check 2: match on exception type name from the `exception` field.
    // This mirrors Python's behaviour where `auto_retry_for` can contain
    // exception class names (e.g. "ConnectionError"). The exception field
    // can be:
    //   - A JSON object with a "type" or "exception_type" key
    //   - A plain JSON string used directly as the type name
    if let Some(ref cause) = error.cause {
        let exception_type = extract_exception_type(cause);
        if let Some(ref etype) = exception_type {
            if auto_retry_for.iter().any(|entry| entry == etype) {
                return true;
            }
        }
    }

    false
}

/// Extract the exception type name from a serialized exception value.
///
/// Supports three formats:
/// - JSON object with `"type"` key (from Python's `_exception_to_json`)
/// - JSON object with `"exception_type"` key (from task_decorator data field)
/// - Plain JSON string (used directly as the type name)
fn extract_exception_type(exception: &serde_json::Value) -> Option<String> {
    match exception {
        serde_json::Value::Object(map) => {
            // Try "type" first, then "exception_type" -- same order as Python.
            map.get("type")
                .and_then(|v| v.as_str())
                .or_else(|| map.get("exception_type").and_then(|v| v.as_str()))
                .map(String::from)
        }
        serde_json::Value::String(s) => Some(s.clone()),
        _ => None,
    }
}

/// Extract retry policy fields from task_options JSON.
///
/// Returns `(intervals, backoff_strategy, jitter, max_delay_seconds)`.
fn parse_retry_policy_fields(
    task_options_json: Option<&str>,
) -> (Vec<u32>, String, bool, Option<u32>) {
    let defaults = (vec![60, 300, 900], "fixed".to_owned(), true, None);

    let Some(json_str) = task_options_json else {
        return defaults;
    };
    let value = match serde_json::from_str::<serde_json::Value>(json_str) {
        Ok(value) => value,
        Err(e) => {
            tracing::warn!(error = %e, "corrupt task_options JSON; using default retry policy");
            return defaults;
        }
    };
    let Some(policy) = value.get("retry_policy") else {
        return defaults;
    };

    let intervals = policy
        .get("intervals")
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_u64().map(|n| n as u32))
                .collect()
        })
        .unwrap_or_else(|| vec![60, 300, 900]);

    let backoff_strategy = policy
        .get("backoff_strategy")
        .and_then(|v| v.as_str())
        .unwrap_or("fixed")
        .to_owned();

    let jitter = policy
        .get("jitter")
        .and_then(|v| v.as_bool())
        .unwrap_or(true);

    let max_delay_seconds = policy
        .get("max_delay_seconds")
        .and_then(|v| v.as_u64())
        .map(|n| n as u32);

    (intervals, backoff_strategy, jitter, max_delay_seconds)
}

/// Extract the `auto_retry_for` list from task_options JSON.
///
/// Checks both top-level `auto_retry_for` and nested `retry_policy.auto_retry_for`,
/// matching Python's behavior.
fn parse_auto_retry_for(task_options_json: Option<&str>) -> Vec<String> {
    let Some(json_str) = task_options_json else {
        return Vec::new();
    };
    let value = match serde_json::from_str::<serde_json::Value>(json_str) {
        Ok(value) => value,
        Err(e) => {
            tracing::warn!(error = %e, "corrupt task_options JSON; using empty auto_retry_for");
            return Vec::new();
        }
    };

    // Check top-level first, then nested under retry_policy.
    let auto_retry_for = value.get("auto_retry_for").or_else(|| {
        value
            .get("retry_policy")
            .and_then(|rp| rp.get("auto_retry_for"))
    });

    auto_retry_for
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|v| v.as_str().map(String::from))
                .collect()
        })
        .unwrap_or_default()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::task::error::{OperationalErrorCode, RetrievalCode};

    #[test]
    fn fixed_delay_first_attempt() {
        let opts = r#"{"retry_policy": {"intervals": [60, 300, 900], "backoff_strategy": "fixed", "jitter": false}}"#;
        assert_eq!(calculate_retry_delay(1, Some(opts)), 60.0);
    }

    #[test]
    fn fixed_delay_second_attempt() {
        let opts = r#"{"retry_policy": {"intervals": [60, 300, 900], "backoff_strategy": "fixed", "jitter": false}}"#;
        assert_eq!(calculate_retry_delay(2, Some(opts)), 300.0);
    }

    #[test]
    fn fixed_delay_third_attempt() {
        let opts = r#"{"retry_policy": {"intervals": [60, 300, 900], "backoff_strategy": "fixed", "jitter": false}}"#;
        assert_eq!(calculate_retry_delay(3, Some(opts)), 900.0);
    }

    #[test]
    fn fixed_delay_clamped_beyond_intervals() {
        let opts = r#"{"retry_policy": {"intervals": [60, 300], "backoff_strategy": "fixed", "jitter": false}}"#;
        // Attempt 5 clamps to last interval (300).
        assert_eq!(calculate_retry_delay(5, Some(opts)), 300.0);
    }

    #[test]
    fn exponential_delay() {
        let opts = r#"{"retry_policy": {"intervals": [30], "backoff_strategy": "exponential", "jitter": false}}"#;
        assert_eq!(calculate_retry_delay(1, Some(opts)), 30.0); // 30 * 2^0
        assert_eq!(calculate_retry_delay(2, Some(opts)), 60.0); // 30 * 2^1
        assert_eq!(calculate_retry_delay(3, Some(opts)), 120.0); // 30 * 2^2
    }

    // Parity with horsies PR #44: opt-in max retry delay cap.

    #[test]
    fn exponential_without_max_delay_remains_uncapped() {
        let opts = r#"{"retry_policy": {"intervals": [60], "backoff_strategy": "exponential", "jitter": false}}"#;
        // 60 * 2^19 = 31_457_280
        assert_eq!(calculate_retry_delay(20, Some(opts)), 31_457_280.0);
    }

    #[test]
    fn exponential_max_delay_caps_large_attempts() {
        let opts = r#"{"retry_policy": {"intervals": [60], "backoff_strategy": "exponential", "jitter": false, "max_delay_seconds": 600}}"#;
        assert_eq!(calculate_retry_delay(20, Some(opts)), 600.0);
    }

    #[test]
    fn max_delay_caps_after_jitter() {
        let opts = r#"{"retry_policy": {"intervals": [60], "backoff_strategy": "exponential", "jitter": true, "max_delay_seconds": 600}}"#;
        for _ in 0..50 {
            assert!(calculate_retry_delay(5, Some(opts)) <= 600.0);
        }
    }

    #[test]
    fn jitter_within_range() {
        let opts = r#"{"retry_policy": {"intervals": [100], "backoff_strategy": "exponential", "jitter": true}}"#;
        for _ in 0..50 {
            let delay = calculate_retry_delay(1, Some(opts));
            assert!(
                (75.0..=125.0).contains(&delay),
                "delay {} out of jitter range",
                delay
            );
        }
    }

    #[test]
    fn jitter_preserves_sub_second_precision() {
        // With a small base (2s), upward jitter produces fractional values (2.0–2.5s).
        let opts =
            r#"{"retry_policy": {"intervals": [2], "backoff_strategy": "fixed", "jitter": true}}"#;
        let mut saw_fractional = false;
        for _ in 0..100 {
            let delay = calculate_retry_delay(1, Some(opts));
            assert!(delay >= 1.0, "delay {delay} below 1.0s floor");
            if delay.fract() > 0.001 {
                saw_fractional = true;
            }
        }
        assert!(saw_fractional, "jitter should produce fractional seconds");
    }

    #[test]
    fn jitter_does_not_collapse_lower_half_onto_floor() {
        // Parity with horsies #78: previously symmetric ±25% jitter followed by
        // a trailing max(1.0) snapped the entire lower half of a small base
        // delay's jitter window onto exactly 1.0, destroying the spread. With
        // floor-then-upward-jitter, base=1 spreads uniformly over [1.0, 1.25]
        // and is no longer pinned to the 1.0 floor.
        let opts =
            r#"{"retry_policy": {"intervals": [1], "backoff_strategy": "fixed", "jitter": true}}"#;
        let samples: Vec<f64> = (0..400)
            .map(|_| calculate_retry_delay(1, Some(opts)))
            .collect();
        for &d in &samples {
            assert!((1.0..=1.25).contains(&d), "delay {d} outside [1.0, 1.25]");
        }
        let exactly_floor = samples.iter().filter(|&&d| d == 1.0).count();
        assert!(
            exactly_floor < samples.len() / 8,
            "{exactly_floor}/{} samples collapsed onto the 1.0 floor; jitter spread destroyed",
            samples.len(),
        );
    }

    #[test]
    fn delay_floored_at_one_second() {
        // The floor is 1.0s: small base delays are raised to 1.0 before jitter.
        let opts =
            r#"{"retry_policy": {"intervals": [1], "backoff_strategy": "fixed", "jitter": true}}"#;
        for _ in 0..50 {
            let delay = calculate_retry_delay(1, Some(opts));
            assert!(delay >= 1.0, "delay {delay} should be >= 1.0s");
        }
    }

    #[test]
    fn no_options_uses_defaults() {
        // Default: fixed [60, 300, 900] with jitter.
        let delay = calculate_retry_delay(1, None);
        assert!(
            (45.0..=75.0).contains(&delay),
            "delay {} out of default range",
            delay
        );
    }

    #[test]
    fn should_retry_matching_code() {
        let error = TaskError::builtin(OperationalErrorCode::TaskError, "boom");
        let opts = r#"{"auto_retry_for": ["TASK_ERROR"]}"#;
        assert!(should_retry(&error, 0, 3, Some(opts), None));
    }

    #[test]
    fn should_retry_non_matching_code() {
        let error = TaskError::builtin(RetrievalCode::WaitTimeout, "timeout");
        let opts = r#"{"auto_retry_for": ["TASK_ERROR"]}"#;
        assert!(!should_retry(&error, 0, 3, Some(opts), None));
    }

    #[test]
    fn should_retry_exhausted() {
        let error = TaskError::builtin(OperationalErrorCode::TaskError, "boom");
        let opts = r#"{"auto_retry_for": ["TASK_EXCEPTION"]}"#;
        assert!(!should_retry(&error, 3, 3, Some(opts), None));
    }

    #[test]
    fn should_retry_no_max_retries() {
        let error = TaskError::builtin(OperationalErrorCode::TaskError, "boom");
        let opts = r#"{"auto_retry_for": ["TASK_EXCEPTION"]}"#;
        assert!(!should_retry(&error, 0, 0, Some(opts), None));
    }

    #[test]
    fn should_retry_user_code() {
        let error = TaskError::new("TRANSIENT", "try again");
        let opts = r#"{"auto_retry_for": ["TRANSIENT"]}"#;
        assert!(should_retry(&error, 0, 3, Some(opts), None));
    }

    #[test]
    fn should_retry_no_auto_retry_for() {
        let error = TaskError::builtin(OperationalErrorCode::TaskError, "boom");
        let opts = r#"{"retry_policy": {"max_retries": 3}}"#;
        assert!(!should_retry(&error, 0, 3, Some(opts), None));
    }

    #[test]
    fn should_retry_no_error_code() {
        let error = TaskError {
            error_code: None,
            message: Some("mystery".to_owned()),
            cause: None,
            data: None,
        };
        let opts = r#"{"auto_retry_for": ["TASK_EXCEPTION"]}"#;
        assert!(!should_retry(&error, 0, 3, Some(opts), None));
    }

    // --- Exception type matching tests ---

    #[test]
    fn should_retry_exception_type_from_object() {
        // Exception is a JSON object with a "type" key (from Python's _exception_to_json).
        let error = TaskError {
            error_code: Some(TaskErrorCode::from(OperationalErrorCode::TaskError)),
            message: Some("connection refused".to_owned()),
            cause: Some(serde_json::json!({
                "type": "ConnectionError",
                "message": "connection refused"
            })),
            data: None,
        };
        let opts = r#"{"auto_retry_for": ["ConnectionError"]}"#;
        assert!(should_retry(&error, 0, 3, Some(opts), None));
    }

    #[test]
    fn should_retry_exception_type_field() {
        // Exception is a JSON object with an "exception_type" key (alternate format).
        let error = TaskError {
            error_code: Some(TaskErrorCode::from(OperationalErrorCode::TaskError)),
            message: Some("timed out".to_owned()),
            cause: Some(serde_json::json!({
                "exception_type": "TimeoutError",
                "detail": "read timed out"
            })),
            data: None,
        };
        let opts = r#"{"auto_retry_for": ["TimeoutError"]}"#;
        assert!(should_retry(&error, 0, 3, Some(opts), None));
    }

    #[test]
    fn should_retry_exception_string_value() {
        // Exception is a plain JSON string (used directly as type name).
        let error = TaskError {
            error_code: Some(TaskErrorCode::from(OperationalErrorCode::TaskError)),
            message: Some("value error".to_owned()),
            cause: Some(serde_json::Value::String("ValueError".to_owned())),
            data: None,
        };
        let opts = r#"{"auto_retry_for": ["ValueError"]}"#;
        assert!(should_retry(&error, 0, 3, Some(opts), None));
    }

    #[test]
    fn should_retry_exception_type_no_match() {
        // Exception type does not match any entry in auto_retry_for.
        let error = TaskError {
            error_code: Some(TaskErrorCode::from(OperationalErrorCode::TaskError)),
            message: Some("key error".to_owned()),
            cause: Some(serde_json::json!({
                "type": "KeyError",
                "message": "missing key"
            })),
            data: None,
        };
        let opts = r#"{"auto_retry_for": ["ConnectionError", "TimeoutError"]}"#;
        assert!(!should_retry(&error, 0, 3, Some(opts), None));
    }

    #[test]
    fn should_retry_no_error_code_but_exception_matches() {
        // No error code at all, but exception type matches.
        // This mirrors the case where a task produces an error without a code
        // but with exception metadata.
        let error = TaskError {
            error_code: None,
            message: Some("connection refused".to_owned()),
            cause: Some(serde_json::json!({
                "type": "ConnectionError"
            })),
            data: None,
        };
        let opts = r#"{"auto_retry_for": ["ConnectionError"]}"#;
        assert!(should_retry(&error, 0, 3, Some(opts), None));
    }

    #[test]
    fn should_retry_error_code_matches_exception_does_not() {
        // Error code matches, exception type does not -- should still retry.
        let error = TaskError {
            error_code: Some(TaskErrorCode::User("TRANSIENT".to_owned())),
            message: Some("transient failure".to_owned()),
            cause: Some(serde_json::json!({
                "type": "SomeOtherError"
            })),
            data: None,
        };
        let opts = r#"{"auto_retry_for": ["TRANSIENT"]}"#;
        assert!(should_retry(&error, 0, 3, Some(opts), None));
    }

    #[test]
    fn should_retry_mixed_codes_and_exception_types() {
        // auto_retry_for contains both error codes and exception type names.
        let opts = r#"{"auto_retry_for": ["TIMEOUT", "ConnectionError", "ValueError"]}"#;

        // Match on error code.
        let error1 = TaskError::new("TIMEOUT", "timed out");
        assert!(should_retry(&error1, 0, 3, Some(opts), None));

        // Match on exception type.
        let error2 = TaskError {
            error_code: Some(TaskErrorCode::from(OperationalErrorCode::TaskError)),
            message: Some("conn refused".to_owned()),
            cause: Some(serde_json::json!({"type": "ConnectionError"})),
            data: None,
        };
        assert!(should_retry(&error2, 0, 3, Some(opts), None));

        // No match on either.
        let error3 = TaskError {
            error_code: Some(TaskErrorCode::User("OTHER".to_owned())),
            message: Some("other error".to_owned()),
            cause: Some(serde_json::json!({"type": "RuntimeError"})),
            data: None,
        };
        assert!(!should_retry(&error3, 0, 3, Some(opts), None));
    }

    // --- good_until pre-check tests ---

    #[test]
    fn should_retry_blocked_by_expired_good_until() {
        let error = TaskError::new("TRANSIENT", "try again");
        let opts = r#"{"auto_retry_for": ["TRANSIENT"]}"#;
        let expired = Utc::now() - chrono::Duration::seconds(10);
        assert!(
            !should_retry(&error, 0, 3, Some(opts), Some(expired)),
            "should not retry when good_until has already passed",
        );
    }

    #[test]
    fn should_retry_allowed_with_future_good_until() {
        let error = TaskError::new("TRANSIENT", "try again");
        let opts = r#"{"auto_retry_for": ["TRANSIENT"]}"#;
        let future = Utc::now() + chrono::Duration::hours(1);
        assert!(
            should_retry(&error, 0, 3, Some(opts), Some(future)),
            "should retry when good_until is in the future",
        );
    }

    #[test]
    fn should_retry_allowed_with_no_good_until() {
        let error = TaskError::new("TRANSIENT", "try again");
        let opts = r#"{"auto_retry_for": ["TRANSIENT"]}"#;
        assert!(
            should_retry(&error, 0, 3, Some(opts), None),
            "should retry when good_until is None (no deadline)",
        );
    }
}
