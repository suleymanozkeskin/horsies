//! Pins of the retry rule.

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
fn no_options_read_as_no_timeout_and_no_policy() {
    assert_eq!(
        read_stored_options(None),
        StoredOptions::Readable {
            timeout: StoredTimeout::NoTimeout,
            cancel_grace: StoredCancelGrace::NotStored,
            retry: StoredRetry::NoPolicy,
        }
    );
}

/// No writer stores an empty options text, so a stored one is corrupt data,
/// not "no options". Python `read_stored_timeout` and
/// `read_stored_retry_policy` refuse it with this reason (`loads_json`
/// refuses an empty text).
#[test]
fn an_empty_text_is_unreadable_and_refuses_the_run_with_python_s_reason() {
    let reason = "stored task_options do not parse: JSON parse failed: the text is empty";

    let options = read_stored_options(Some(""));

    assert_eq!(
        options,
        StoredOptions::Unreadable {
            reason: reason.to_owned()
        }
    );
    assert_eq!(options.run_permit(), RunPermit::Refuse(reason.to_owned()));
    assert_eq!(options.retry(), &StoredRetry::NoPolicy);
    assert_eq!(options.cancel_grace(), StoredCancelGrace::NotStored);
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

fn not_an_int(found: NotAnInt) -> TimeoutRefusal {
    TimeoutRefusal::NotAnInteger { found }
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
        (r#"{"timeout_ms": 2000.5}"#, not_an_int(NotAnInt::Float)),
        (r#"{"timeout_ms": 2000.0}"#, not_an_int(NotAnInt::Float)),
        (r#"{"timeout_ms": 1e3}"#, not_an_int(NotAnInt::Float)),
        (r#"{"timeout_ms": "2000"}"#, not_an_int(NotAnInt::Str)),
        (r#"{"timeout_ms": true}"#, not_an_int(NotAnInt::Bool)),
        (r#"{"timeout_ms": [2000]}"#, not_an_int(NotAnInt::List)),
        (r#"{"timeout_ms": {}}"#, not_an_int(NotAnInt::Dict)),
        // An int of any size is compared with the bounds, as Python compares it.
        (
            r#"{"timeout_ms": 18446744073709551616}"#,
            TimeoutRefusal::AboveMaximum,
        ),
        (
            r#"{"timeout_ms": -18446744073709551616}"#,
            TimeoutRefusal::BelowMinimum,
        ),
    ];
    for (text, refusal) in cases {
        let options = read_stored_options(Some(text));
        assert_eq!(
            options,
            StoredOptions::Readable {
                timeout: StoredTimeout::Invalid(refusal),
                cancel_grace: StoredCancelGrace::NotStored,
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

/// Python `_timeout_value`: the text of each timeout refusal.
#[test]
fn each_timeout_refusal_has_python_s_text() {
    let cases = [
        (
            not_an_int(NotAnInt::Float),
            "stored timeout_ms is a float, not an int",
        ),
        (
            not_an_int(NotAnInt::Bool),
            "stored timeout_ms is a bool, not an int",
        ),
        (
            not_an_int(NotAnInt::Str),
            "stored timeout_ms is a str, not an int",
        ),
        (
            not_an_int(NotAnInt::List),
            "stored timeout_ms is a list, not an int",
        ),
        (
            not_an_int(NotAnInt::Dict),
            "stored timeout_ms is a dict, not an int",
        ),
        (
            TimeoutRefusal::BelowMinimum,
            "stored timeout_ms is below 1000 ms",
        ),
        (
            TimeoutRefusal::AboveMaximum,
            "stored timeout_ms is above the limit of 4294967295 ms (about 49.7 days)",
        ),
    ];
    for (refusal, text) in cases {
        assert_eq!(timeout_refusal_message(refusal), text);
    }
}

/// Python `read_stored_timeout` and `read_stored_retry_policy`: the reason of
/// a text that `loads_json` refuses, and of a JSON value that is not an
/// object. The texts after `JSON parse failed: ` are those of `loads_json`
/// (`core::codec::python_loads`). The dispatch read and the retry limit of
/// enqueue give one reason for each text, an empty text included.
#[test]
fn an_unreadable_text_has_python_s_reason() {
    let cases = [
        (
            "",
            "stored task_options do not parse: JSON parse failed: the text is empty",
        ),
        (
            "{",
            "stored task_options do not parse: JSON parse failed: Expecting property name \
             enclosed in double quotes: line 1 column 2 (char 1)",
        ),
        (
            " ",
            "stored task_options do not parse: JSON parse failed: Expecting value: line 1 \
             column 2 (char 1)",
        ),
        (
            r#"{"a":1,"a":2}"#,
            "stored task_options do not parse: JSON parse failed: JSON object repeats a key",
        ),
        (
            r#"{"timeout_ms": NaN}"#,
            "stored task_options do not parse: JSON parse failed: Non-standard JSON constant \
             is not allowed: NaN",
        ),
        ("[1]", "stored task_options are a JSON list, not an object"),
        (
            "null",
            "stored task_options are a JSON NoneType, not an object",
        ),
        (
            r#""x""#,
            "stored task_options are a JSON str, not an object",
        ),
        ("1", "stored task_options are a JSON int, not an object"),
        ("1.5", "stored task_options are a JSON float, not an object"),
        ("true", "stored task_options are a JSON bool, not an object"),
    ];
    for (text, reason) in cases {
        let options = read_stored_options(Some(text));
        assert_eq!(
            options,
            StoredOptions::Unreadable {
                reason: reason.to_owned()
            },
            "{text}"
        );
        assert_eq!(options.run_permit(), RunPermit::Refuse(reason.to_owned()));
        assert_eq!(
            stored_max_retries(Some(text)),
            Err(RetryLimitRefused::UnreadableOptions {
                reason: reason.to_owned()
            }),
            "{text}"
        );
    }
}

/// Python reads an int literal of up to 4300 digits in any member; the
/// timeout is compared with its bounds, and a member that is not read stays
/// unread. An int of 4301 digits makes the text unreadable.
#[test]
fn a_long_int_reads_in_any_member() {
    let long = "9".repeat(400);
    let other = read_stored_options(Some(&format!(r#"{{"other": {long}, "timeout_ms": 2000}}"#)));
    assert_eq!(
        other.run_permit(),
        RunPermit::RunWithTimeout(Duration::from_millis(2000))
    );
    let above = read_stored_options(Some(&format!(r#"{{"timeout_ms": {long}}}"#)));
    assert_eq!(
        above.run_permit(),
        RunPermit::Refuse(
            "stored timeout_ms is above the limit of 4294967295 ms (about 49.7 days)".to_owned()
        )
    );
    let below = read_stored_options(Some(&format!(r#"{{"timeout_ms": -{long}}}"#)));
    assert_eq!(
        below.run_permit(),
        RunPermit::Refuse("stored timeout_ms is below 1000 ms".to_owned())
    );
    let too_long = "9".repeat(4301);
    assert_eq!(
        read_stored_options(Some(&format!(r#"{{"other": {too_long}}}"#))).run_permit(),
        RunPermit::Refuse(
            "stored task_options do not parse: JSON parse failed: Exceeds the limit (4300 \
             digits) for integer string conversion: value has 4301 digits; use \
             sys.set_int_max_str_digits() to increase the limit"
                .to_owned()
        )
    );
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

/// Python `stored_retry_policy` validates `RetryPolicy` in strict mode: a key
/// that the policy leaves out takes the field default, as at enqueue.
#[test]
fn a_key_the_policy_leaves_out_takes_the_field_default() {
    let defaults = rule_of(r#"{"auto_retry_for": ["X"]}"#);
    assert_eq!(defaults.max_retries, 3);
    assert_eq!(defaults.intervals, vec![60, 300, 900]);
    assert_eq!(defaults.backoff, Backoff::Fixed);
    assert!(defaults.jitter);
    assert_eq!(defaults.delay_cap, DelayCap::Uncapped);

    let partial = rule_of(r#"{"max_retries": 1, "intervals": [5], "auto_retry_for": ["X"]}"#);
    assert_eq!(partial.max_retries, 1);
    assert_eq!(partial.intervals, vec![5]);
    assert_eq!(partial.backoff, Backoff::Fixed);
    assert!(partial.jitter);
}

/// A key that is present with `null` is not left out: strict mode refuses it.
#[test]
fn a_null_value_is_not_a_left_out_key() {
    for key in ["max_retries", "intervals", "backoff_strategy", "jitter"] {
        let policy = format!(r#"{{"{key}": null, "auto_retry_for": ["X"]}}"#);
        assert!(
            matches!(
                retry_of(&options_with_policy(&policy)),
                StoredRetry::Invalid { .. }
            ),
            "{policy}"
        );
    }
    let no_cap = rule_of(
        r#"{"max_retries": 1, "intervals": [5], "max_delay_seconds": null,
            "auto_retry_for": ["X"]}"#,
    );
    assert_eq!(no_cap.delay_cap, DelayCap::Uncapped);
}

/// Python `validate_error_code_string`: each code is `^[A-Z][A-Z0-9_]*$`,
/// and `WORKFLOW_STOPPED` is reserved (#1078).
#[test]
fn each_auto_retry_for_code_must_be_an_error_code_text() {
    let policy = |code: &str| {
        format!(
            r#"{{"max_retries": 1, "intervals": [5], "auto_retry_for": ["X", {}]}}"#,
            serde_json::Value::from(code)
        )
    };
    for refused in [
        "",
        "x",
        "lower_case",
        "MyError",
        "1ABC",
        "A-B",
        "AB ",
        "WORKFLOW_STOPPED",
    ] {
        assert!(
            matches!(
                retry_of(&options_with_policy(&policy(refused))),
                StoredRetry::Invalid { .. }
            ),
            "{refused:?} must be refused"
        );
    }
    for accepted in ["A", "RATE_LIMITED", "E2E_42", "TASK_TIMEOUT", "MY_ERROR"] {
        assert_eq!(
            rule_of(&policy(accepted)).auto_retry_for,
            vec!["X".to_owned(), accepted.to_owned()]
        );
    }
}

/// Python's `max_delay_seconds` is a `PositiveInt` with no upper bound.
#[test]
fn a_delay_cap_above_the_u32_range_reads() {
    let rule = rule_of(
        r#"{"max_retries": 1, "intervals": [5], "max_delay_seconds": 5000000000,
            "auto_retry_for": ["X"]}"#,
    );
    assert_eq!(rule.delay_cap, DelayCap::AtMost(5_000_000_000));
}

/// Every refusal starts with Python's text, `stored retry_policy does not
/// validate: `, and refuses the run with it; a policy that is not an object
/// names its Python type, as Python does.
#[test]
fn every_refusal_starts_with_python_s_text() {
    let not_objects = [
        (r#""text""#, "str"),
        ("[1, 2]", "list"),
        ("5", "int"),
        ("1.5", "float"),
        ("true", "bool"),
    ];
    for (policy, python_type) in not_objects {
        let options = read_stored_options(Some(&options_with_policy(policy)));
        let expected = format!(
            "stored retry_policy does not validate: retry_policy is not an object: {python_type}"
        );
        assert_eq!(
            options.retry(),
            &StoredRetry::Invalid {
                reason: expected.clone()
            }
        );
        assert_eq!(options.run_permit(), RunPermit::Refuse(expected));
    }
    for policy in [
        r#"{"max_retries": 2, "intervals": [], "backoff_strategy": "fixed", "jitter": false,
            "auto_retry_for": ["X"]}"#,
        r#"{"max_retries": 2, "intervals": [5, 10], "backoff_strategy": "quadratic",
            "jitter": false, "auto_retry_for": ["X"]}"#,
        r#"{"max_retries": 3, "intervals": [5, 10], "backoff_strategy": "fixed",
            "jitter": false, "auto_retry_for": ["X"]}"#,
        r#"{"max_retries": 1, "intervals": [5], "auto_retry_for": ["x"]}"#,
        r#"{"max_retries": 1, "intervals": [5], "auto_retry_for": ["X"], "extra": 1}"#,
    ] {
        match read_stored_options(Some(&options_with_policy(policy))).run_permit() {
            RunPermit::Refuse(reason) => assert!(
                reason.starts_with("stored retry_policy does not validate: "),
                "{policy}: {reason}"
            ),
            other => panic!("{policy} must refuse the run, got {other:?}"),
        }
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

// --- stored retry limit ----------------------------------------------------
//
// Python `tests/integration/test_enqueue_retry_limit.py`
// (`test_valid_policies_store_their_limits`,
// `test_a_malformed_policy_is_refused_without_a_row`).

#[test]
fn options_without_a_policy_store_zero_retries() {
    for text in [
        None,
        Some("{}"),
        Some(r#"{"queue": "default"}"#),
        Some(r#"{"retry_policy": null, "timeout_ms": null}"#),
    ] {
        assert_eq!(stored_max_retries(text), Ok(0), "{text:?}");
    }
}

#[test]
fn a_policy_that_fits_the_rule_stores_its_limit() {
    for (policy, limit) in [
        (fixed_policy("[60]", 1, true), 1),
        (fixed_policy("[60, 300, 900]", 3, false), 3),
        (
            r#"{"max_retries": 20, "intervals": [30], "backoff_strategy": "exponential",
                "jitter": true, "auto_retry_for": ["X"]}"#
                .to_owned(),
            20,
        ),
    ] {
        assert_eq!(
            stored_max_retries(Some(&options_with_policy(&policy))),
            Ok(limit),
            "{policy}"
        );
    }
}

#[test]
fn options_that_do_not_parse_or_are_not_an_object_are_refused() {
    for text in ["not json", "{", "[]", "3", "\"x\"", "null"] {
        assert!(
            matches!(
                stored_max_retries(Some(text)),
                Err(RetryLimitRefused::UnreadableOptions { .. })
            ),
            "{text}"
        );
    }
}

/// Policies that a lenient reader would turn into a limit (3 for a policy
/// without an integer `max_retries`, the stored number otherwise), and the
/// other shapes that Python's `RetryPolicy` refuses.
#[test]
fn a_policy_that_does_not_fit_the_rule_is_refused_not_guessed() {
    let exponential = |max_retries: &str| {
        format!(
            r#"{{"max_retries": {max_retries}, "intervals": [60], "backoff_strategy": "exponential",
                "jitter": true, "auto_retry_for": ["X"]}}"#
        )
    };
    let refused = [
        r#"{"backoff_strategy": "exponential"}"#.to_owned(),
        r#"{"max_retries": 5}"#.to_owned(),
        exponential("\"5\""),
        exponential("3.5"),
        exponential("true"),
        exponential("0"),
        exponential("-1"),
        exponential("21"),
        r#"5"#.to_owned(),
        r#"[3]"#.to_owned(),
        r#"{"max_retries": 2, "intervals": [60, 60], "backoff_strategy": "fixed",
            "jitter": true, "auto_retry_for": []}"#
            .to_owned(),
        r#"{"max_retries": 2, "intervals": [60, 60], "backoff_strategy": "fixed",
            "jitter": true, "auto_retry_for": ["X"], "extra": 1}"#
            .to_owned(),
    ];
    for policy in refused {
        assert!(
            matches!(
                stored_max_retries(Some(&options_with_policy(&policy))),
                Err(RetryLimitRefused::InvalidPolicy { .. })
            ),
            "{policy}"
        );
    }
}

/// The refusal is Python's `InvalidRetryPolicy.detail`, with no prefix: the
/// send writes `task_options refused for <task>: <detail>`, as Python's
/// enqueue does (`python/horsies/core/brokers/postgres.py:463-469`). The
/// dispatch refusal adds the prefix (`stored retry_policy does not
/// validate: `).
#[test]
fn the_refusal_is_python_s_detail() {
    let options = options_with_policy(r#"{"max_retries": 5}"#);
    let refusal = stored_max_retries(Some(&options)).expect_err("a partial policy is refused");
    assert_eq!(
        refusal.to_string(),
        "auto_retry_for: Field required [missing]"
    );
    assert_eq!(
        read_stored_options(Some(&options)).run_permit(),
        RunPermit::Refuse(
            "stored retry_policy does not validate: auto_retry_for: Field required [missing]"
                .to_owned()
        )
    );
}

// --- stored cancel grace ---------------------------------------------------

#[test]
fn a_cancel_grace_inside_the_bounds_is_kept() {
    for ms in [MIN_CANCEL_GRACE_MS, 1_000, MAX_CANCEL_GRACE_MS] {
        let options = read_stored_options(Some(&format!(r#"{{"cancel_grace_ms": {ms}}}"#)));
        assert_eq!(
            options.cancel_grace(),
            StoredCancelGrace::Grace(Duration::from_millis(ms))
        );
    }
    assert_eq!(
        read_stored_options(Some(r#"{"cancel_grace_ms": null}"#)).cancel_grace(),
        StoredCancelGrace::NotStored
    );
}

/// Python `shared_async_grace_error`: each refusal and its text.
#[test]
fn a_cancel_grace_outside_its_rule_is_refused_with_python_text() {
    let cases = [
        (
            r#"{"cancel_grace_ms": 99}"#,
            GraceRefusal::BelowMinimum,
            "stored cancel_grace_ms is below 100 ms",
        ),
        (
            r#"{"cancel_grace_ms": -5}"#,
            GraceRefusal::BelowMinimum,
            "stored cancel_grace_ms is below 100 ms",
        ),
        (
            r#"{"cancel_grace_ms": 10001}"#,
            GraceRefusal::AboveMaximum,
            "stored cancel_grace_ms is above 10000 ms",
        ),
        (
            r#"{"cancel_grace_ms": true}"#,
            GraceRefusal::NotAnInteger {
                found: NotAnInt::Bool,
            },
            "stored cancel_grace_ms is a bool, not an int",
        ),
        (
            r#"{"cancel_grace_ms": 500.5}"#,
            GraceRefusal::NotAnInteger {
                found: NotAnInt::Float,
            },
            "stored cancel_grace_ms is a JSON float, not an int",
        ),
        (
            r#"{"cancel_grace_ms": "500"}"#,
            GraceRefusal::NotAnInteger {
                found: NotAnInt::Str,
            },
            "stored cancel_grace_ms is a JSON str, not an int",
        ),
        (
            r#"{"cancel_grace_ms": [500]}"#,
            GraceRefusal::NotAnInteger {
                found: NotAnInt::List,
            },
            "stored cancel_grace_ms is a JSON list, not an int",
        ),
        // An int of any size is compared with the bounds, as Python compares it.
        (
            r#"{"cancel_grace_ms": 18446744073709551616}"#,
            GraceRefusal::AboveMaximum,
            "stored cancel_grace_ms is above 10000 ms",
        ),
        (
            r#"{"cancel_grace_ms": -18446744073709551616}"#,
            GraceRefusal::BelowMinimum,
            "stored cancel_grace_ms is below 100 ms",
        ),
    ];
    for (text, refusal, message) in cases {
        assert_eq!(
            read_stored_options(Some(text)).cancel_grace(),
            StoredCancelGrace::Invalid(refusal),
            "{text}"
        );
        assert_eq!(grace_refusal_message(refusal), message, "{text}");
    }
}
