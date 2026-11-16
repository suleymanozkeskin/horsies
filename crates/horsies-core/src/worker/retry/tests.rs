//! Pins of the retry rule. Python references: `tests/unit/test_retry_utils.py`,
//! `tests/unit/test_retry_decision.py`, `tests/integration/test_stored_timeout_refusal.py`.

use super::*;

/// A stored options text with this retry policy object.
fn options_with_policy(policy: &str) -> String {
    format!(r#"{{"task_name": "t", "retry_policy": {policy}}}"#)
}

fn fixed_policy(intervals: &str, retries: u32, jitter: bool) -> String {
    format!(
        r#"{{"max_retries": {retries}, "intervals": {intervals}, "backoff_strategy": "fixed",
            "jitter": {jitter}, "max_delay_seconds": null, "auto_retry_for": ["TRANSIENT"]}}"#
    )
}

fn rule_of(policy: &str) -> RetryRule {
    match read_stored_options(Some(&options_with_policy(policy))) {
        StoredOptions::Readable {
            retry: StoredRetry::Policy(rule),
            ..
        } => rule,
        other => panic!("expected a valid policy, got {other:?}"),
    }
}

fn retry_of(options: &str) -> StoredRetry {
    read_stored_options(Some(options)).retry().clone()
}

// --- stored options -------------------------------------------------------

#[test]
fn no_options_and_empty_text_read_as_no_timeout_and_no_policy() {
    for text in [None, Some("")] {
        assert_eq!(
            read_stored_options(text),
            StoredOptions::Readable {
                timeout: StoredTimeout::NoTimeout,
                retry: StoredRetry::NoPolicy,
            }
        );
    }
}

#[test]
fn text_that_does_not_parse_is_unreadable_and_refuses_the_run() {
    let options = read_stored_options(Some("{"));
    assert!(matches!(options, StoredOptions::Unreadable { .. }));
    assert!(matches!(options.run_permit(), RunPermit::Refuse(_)));
    assert_eq!(options.retry(), &StoredRetry::NoPolicy);
}

#[test]
fn text_that_is_not_an_object_is_unreadable() {
    for text in ["[]", "3", "\"x\"", "null", "true"] {
        assert!(
            matches!(
                read_stored_options(Some(text)),
                StoredOptions::Unreadable { .. }
            ),
            "{text} must be unreadable"
        );
    }
}

#[test]
fn a_timeout_inside_the_bounds_is_the_run_limit() {
    for ms in [MIN_TASK_TIMEOUT_MS, 2_000, MAX_TASK_TIMEOUT_MS] {
        let options = read_stored_options(Some(&format!(r#"{{"timeout_ms": {ms}}}"#)));
        assert_eq!(
            options.run_permit(),
            RunPermit::RunWithTimeout(Duration::from_millis(ms))
        );
    }
}

#[test]
fn a_missing_or_null_timeout_runs_without_a_limit() {
    for text in [r#"{"task_name": "t"}"#, r#"{"timeout_ms": null}"#] {
        assert_eq!(
            read_stored_options(Some(text)).run_permit(),
            RunPermit::RunWithoutTimeout
        );
    }
}

#[test]
fn a_timeout_outside_its_rule_refuses_the_run() {
    let cases = [
        (r#"{"timeout_ms": 999}"#, TimeoutRefusal::BelowMinimum),
        (r#"{"timeout_ms": -1}"#, TimeoutRefusal::BelowMinimum),
        (
            r#"{"timeout_ms": 4294967296}"#,
            TimeoutRefusal::AboveMaximum,
        ),
        (r#"{"timeout_ms": 2000.5}"#, TimeoutRefusal::NotAnInteger),
        (r#"{"timeout_ms": 2000.0}"#, TimeoutRefusal::NotAnInteger),
        (r#"{"timeout_ms": "2000"}"#, TimeoutRefusal::NotAnInteger),
        (r#"{"timeout_ms": true}"#, TimeoutRefusal::NotAnInteger),
    ];
    for (text, refusal) in cases {
        let options = read_stored_options(Some(text));
        assert_eq!(
            options,
            StoredOptions::Readable {
                timeout: StoredTimeout::Invalid(refusal),
                retry: StoredRetry::NoPolicy,
            },
            "{text}"
        );
        assert!(
            matches!(options.run_permit(), RunPermit::Refuse(_)),
            "{text}"
        );
    }
}

/// Python `test_a_refused_timeout_retries_until_max_retries_when_asked`: a
/// refused timeout does not hide a policy that fits its rule.
#[test]
fn a_refused_timeout_keeps_a_valid_retry_policy() {
    let text = format!(
        r#"{{"timeout_ms": 4294967296, "retry_policy": {}}}"#,
        fixed_policy("[1, 1]", 2, false)
    );
    let options = read_stored_options(Some(&text));
    assert!(matches!(options.run_permit(), RunPermit::Refuse(_)));
    assert!(matches!(options.retry(), StoredRetry::Policy(_)));
}

#[test]
fn a_valid_policy_reads_as_a_rule() {
    let rule = rule_of(&fixed_policy("[60, 300, 900]", 3, true));
    assert_eq!(rule.intervals, vec![60, 300, 900]);
    assert_eq!(rule.backoff, Backoff::Fixed);
    assert!(rule.jitter);
    assert_eq!(rule.delay_cap, DelayCap::Uncapped);
    assert_eq!(rule.auto_retry_for, vec!["TRANSIENT".to_owned()]);
}

#[test]
fn a_policy_outside_its_rule_refuses_the_run_and_cannot_retry() {
    let cases = [
        // auto_retry_for is required and non-empty.
        r#"{"max_retries": 1, "intervals": [1], "backoff_strategy": "fixed", "jitter": false}"#,
        r#"{"max_retries": 1, "intervals": [1], "backoff_strategy": "fixed", "jitter": false,
            "auto_retry_for": []}"#,
        // Codes are text: the tagged built-in object is not a stored code.
        r#"{"max_retries": 1, "intervals": [1], "backoff_strategy": "fixed", "jitter": false,
            "auto_retry_for": [{"__builtin_task_code__": "TASK_TIMEOUT"}]}"#,
        // No interval list, an empty one, a float, a zero, one above a day.
        r#"{"max_retries": 1, "backoff_strategy": "fixed", "jitter": false,
            "auto_retry_for": ["X"]}"#,
        r#"{"max_retries": 1, "intervals": [], "backoff_strategy": "fixed", "jitter": false,
            "auto_retry_for": ["X"]}"#,
        r#"{"max_retries": 1, "intervals": [1.5], "backoff_strategy": "fixed", "jitter": false,
            "auto_retry_for": ["X"]}"#,
        r#"{"max_retries": 1, "intervals": [0], "backoff_strategy": "fixed", "jitter": false,
            "auto_retry_for": ["X"]}"#,
        r#"{"max_retries": 1, "intervals": [86401], "backoff_strategy": "fixed",
            "jitter": false, "auto_retry_for": ["X"]}"#,
        // A strategy that is not fixed or exponential.
        r#"{"max_retries": 1, "intervals": [1], "backoff_strategy": "linear", "jitter": false,
            "auto_retry_for": ["X"]}"#,
        // Fixed: one interval per retry. Exponential: one base interval.
        r#"{"max_retries": 2, "intervals": [1], "backoff_strategy": "fixed", "jitter": false,
            "auto_retry_for": ["X"]}"#,
        r#"{"max_retries": 2, "intervals": [1, 2], "backoff_strategy": "exponential",
            "jitter": false, "auto_retry_for": ["X"]}"#,
        // max_retries 1..=20, max_delay_seconds positive, no extra keys.
        r#"{"max_retries": 0, "intervals": [1], "backoff_strategy": "exponential",
            "jitter": false, "auto_retry_for": ["X"]}"#,
        r#"{"max_retries": 21, "intervals": [1], "backoff_strategy": "exponential",
            "jitter": false, "auto_retry_for": ["X"]}"#,
        r#"{"max_retries": 1, "intervals": [1], "backoff_strategy": "exponential",
            "jitter": false, "max_delay_seconds": 0, "auto_retry_for": ["X"]}"#,
        r#"{"max_retries": 1, "intervals": [1], "backoff_strategy": "exponential",
            "jitter": false, "auto_retry_for": ["X"], "extra": 1}"#,
        // A policy that is not an object.
        r#"[1, 2]"#,
    ];
    for policy in cases {
        let options = read_stored_options(Some(&options_with_policy(policy)));
        assert!(
            matches!(options.retry(), StoredRetry::Invalid { .. }),
            "{policy} must be refused"
        );
        assert!(
            matches!(options.run_permit(), RunPermit::Refuse(_)),
            "{policy} must refuse the run"
        );
        assert_eq!(
            admit_retry(0, 3, options.retry(), Some("X")),
            RetryAdmission::NoPolicy
        );
    }
}

/// Python reads only `retry_policy.auto_retry_for`: a top-level
/// `auto_retry_for` key is not a policy.
#[test]
fn a_top_level_auto_retry_for_is_not_a_policy() {
    let retry = retry_of(r#"{"auto_retry_for": ["TRANSIENT"]}"#);
    assert_eq!(retry, StoredRetry::NoPolicy);
    assert_eq!(
        admit_retry(0, 3, &retry, Some("TRANSIENT")),
        RetryAdmission::NoPolicy
    );
}

// --- admission ------------------------------------------------------------

fn transient_retry() -> StoredRetry {
    retry_of(&options_with_policy(&fixed_policy("[1, 1, 1]", 3, false)))
}

#[test]
fn a_listed_code_with_retries_left_is_admitted() {
    let retry = transient_retry();
    assert!(matches!(
        admit_retry(0, 3, &retry, Some("TRANSIENT")),
        RetryAdmission::Admitted(_)
    ));
    assert!(matches!(
        admit_retry(2, 3, &retry, Some("TRANSIENT")),
        RetryAdmission::Admitted(_)
    ));
}

#[test]
fn spent_retries_are_not_admitted() {
    let retry = transient_retry();
    assert_eq!(
        admit_retry(3, 3, &retry, Some("TRANSIENT")),
        RetryAdmission::RetriesSpent
    );
    assert_eq!(
        admit_retry(0, 0, &retry, Some("TRANSIENT")),
        RetryAdmission::RetriesSpent
    );
}

#[test]
fn a_code_the_policy_does_not_list_is_not_admitted() {
    let retry = transient_retry();
    for code in [Some("OTHER"), Some(""), None] {
        assert_eq!(
            admit_retry(0, 3, &retry, code),
            RetryAdmission::CodeNotListed,
            "{code:?}"
        );
    }
}

#[test]
fn no_policy_is_not_admitted() {
    assert_eq!(
        admit_retry(0, 3, &StoredRetry::NoPolicy, Some("TRANSIENT")),
        RetryAdmission::NoPolicy
    );
}

/// horsies-rust #68: the reaper and the worker use one rule, and a built-in
/// code is compared by its value. A policy that names `WORKER_CRASHED`
/// admits the reaper's crash retry.
#[test]
fn a_built_in_code_is_compared_by_its_value() {
    let policy = r#"{"max_retries": 2, "intervals": [5], "backoff_strategy": "exponential",
        "jitter": false, "auto_retry_for": ["WORKER_CRASHED", "TASK_TIMEOUT"]}"#;
    let retry = retry_of(&options_with_policy(policy));
    let crashed = crate::core::task::error::TaskErrorCode::from(
        crate::core::task::error::OperationalErrorCode::WorkerCrashed,
    )
    .to_string();
    assert_eq!(crashed, "WORKER_CRASHED");
    assert!(matches!(
        admit_retry(0, 2, &retry, Some(&crashed)),
        RetryAdmission::Admitted(_)
    ));
    let timed_out = crate::core::task::error::TaskErrorCode::from(
        crate::core::task::error::OutcomeCode::TaskTimeout,
    )
    .to_string();
    assert!(matches!(
        admit_retry(0, 2, &retry, Some(&timed_out)),
        RetryAdmission::Admitted(_)
    ));
}

// --- delay ----------------------------------------------------------------

#[test]
fn fixed_backoff_uses_the_interval_of_the_attempt_and_clamps_to_the_last() {
    let rule = rule_of(&fixed_policy("[60, 300, 900]", 3, false));
    assert_eq!(rule.delay_seconds(1), 60.0);
    assert_eq!(rule.delay_seconds(2), 300.0);
    assert_eq!(rule.delay_seconds(3), 900.0);
    assert_eq!(rule.delay_seconds(4), 900.0);
}

#[test]
fn exponential_backoff_doubles_the_base_interval() {
    let rule = rule_of(
        r#"{"max_retries": 5, "intervals": [30], "backoff_strategy": "exponential",
            "jitter": false, "auto_retry_for": ["X"]}"#,
    );
    assert_eq!(rule.delay_seconds(1), 30.0);
    assert_eq!(rule.delay_seconds(2), 60.0);
    assert_eq!(rule.delay_seconds(3), 120.0);
    assert_eq!(rule.delay_seconds(20), 30.0 * 2.0_f64.powi(19));
}

#[test]
fn the_cap_bounds_the_delay_after_backoff_and_jitter() {
    let rule = rule_of(
        r#"{"max_retries": 20, "intervals": [30], "backoff_strategy": "exponential",
            "jitter": true, "max_delay_seconds": 100, "auto_retry_for": ["X"]}"#,
    );
    for attempt in 1..=20 {
        assert!(rule.delay_seconds(attempt) <= 100.0);
    }
    assert_eq!(rule.delay_seconds(10), 100.0);
}

#[test]
fn jitter_adds_up_to_a_quarter_above_the_base() {
    let rule = rule_of(&fixed_policy("[100]", 1, true));
    for _ in 0..200 {
        let delay = rule.delay_seconds(1);
        assert!((100.0..=125.0).contains(&delay), "{delay}");
    }
}

#[test]
fn the_delay_is_at_least_one_second() {
    let rule = rule_of(&fixed_policy("[1]", 1, false));
    assert_eq!(rule.delay_seconds(1), 1.0);
    let jittered = rule_of(&fixed_policy("[1]", 1, true));
    for _ in 0..200 {
        let delay = jittered.delay_seconds(1);
        assert!((1.0..=1.25).contains(&delay), "{delay}");
    }
}
