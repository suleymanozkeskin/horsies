use chrono::{DateTime, Utc};

/// Check whether a task is eligible for retry after a crash/stale detection.
///
/// Pure function that checks:
/// 1. Retries remaining (`retry_count < max_retries` and `max_retries > 0`)
/// 2. The error code is in the `auto_retry_for` list from task_options
/// 3. `good_until` hasn't passed (using `db_now` from the database)
///
/// Used by the reaper to decide whether to retry or fail stale RUNNING tasks.
pub fn check_retry_eligibility(
    retry_count: i32,
    max_retries: i32,
    task_options_json: Option<&str>,
    error_code: &str,
    good_until: Option<DateTime<Utc>>,
    db_now: DateTime<Utc>,
) -> bool {
    // Check retries remaining.
    if max_retries <= 0 || retry_count >= max_retries {
        return false;
    }

    // Check good_until hasn't passed.
    if let Some(deadline) = good_until {
        if db_now >= deadline {
            return false;
        }
    }

    // Check error_code is in auto_retry_for list.
    let Some(opts_str) = task_options_json else {
        return false;
    };
    let Ok(value) = serde_json::from_str::<serde_json::Value>(opts_str) else {
        return false;
    };

    // Look in both top-level auto_retry_for and retry_policy.auto_retry_for.
    let auto_retry_for = value.get("auto_retry_for").or_else(|| {
        value
            .get("retry_policy")
            .and_then(|rp| rp.get("auto_retry_for"))
    });

    let Some(arr) = auto_retry_for.and_then(|v| v.as_array()) else {
        return false;
    };

    arr.iter().any(|entry| entry.as_str() == Some(error_code))
}

/// Extract `max_retries` from a JSON task_options string.
///
/// Looks for `task_options.retry_policy.max_retries`.
/// If `retry_policy` exists but `max_retries` is absent, defaults to 3.
/// If no `retry_policy` at all (or input is None/invalid JSON), defaults to 0.
pub fn parse_max_retries(task_options: Option<&str>) -> i32 {
    let Some(opts_str) = task_options else {
        return 0;
    };
    let Ok(value) = serde_json::from_str::<serde_json::Value>(opts_str) else {
        return 0;
    };
    let Some(retry_policy) = value.get("retry_policy") else {
        return 0;
    };
    retry_policy
        .get("max_retries")
        .and_then(|v| v.as_i64())
        .map(|n| n as i32)
        .unwrap_or(3)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn none_returns_zero() {
        assert_eq!(parse_max_retries(None), 0);
    }

    #[test]
    fn empty_string_returns_zero() {
        assert_eq!(parse_max_retries(Some("")), 0);
    }

    #[test]
    fn no_retry_policy_returns_zero() {
        assert_eq!(parse_max_retries(Some(r#"{"queue": "default"}"#)), 0);
    }

    #[test]
    fn policy_without_max_retries_defaults_to_three() {
        let opts = r#"{"retry_policy": {"backoff": "exponential"}}"#;
        assert_eq!(parse_max_retries(Some(opts)), 3);
    }

    #[test]
    fn policy_with_explicit_max_retries() {
        let opts = r#"{"retry_policy": {"max_retries": 5}}"#;
        assert_eq!(parse_max_retries(Some(opts)), 5);
    }

    #[test]
    fn policy_with_zero_max_retries() {
        let opts = r#"{"retry_policy": {"max_retries": 0}}"#;
        assert_eq!(parse_max_retries(Some(opts)), 0);
    }

    #[test]
    fn invalid_json_returns_zero() {
        assert_eq!(parse_max_retries(Some("not json")), 0);
    }
}
