//! Pins of the refusal text of a stored retry policy. Each expected detail
//! is the `detail` that the Python reader of a stored policy gave for the
//! same policy, with pydantic 2.13.4 and pydantic-core 2.46.4.

use super::super::{read_stored_options, DelayCap, RetryRule, RunPermit, StoredRetry};

/// The refusal detail of `policy`, after the prefix.
fn detail_of(policy: &str) -> String {
    let options = format!(r#"{{"retry_policy": {policy}}}"#);
    match read_stored_options(Some(&options)).retry() {
        StoredRetry::Invalid { reason } => reason
            .strip_prefix("stored retry_policy does not validate: ")
            .unwrap_or_else(|| panic!("{policy}: the refusal has no prefix: {reason}"))
            .to_owned(),
        other => panic!("{policy} must be refused, got {other:?}"),
    }
}

fn assert_python_details(cases: &[(&str, &str)]) {
    for (policy, detail) in cases {
        assert_eq!(detail_of(policy), *detail, "{policy}");
    }
}

fn rule_of(policy: &str) -> RetryRule {
    let options = format!(r#"{{"retry_policy": {policy}}}"#);
    match read_stored_options(Some(&options)).retry() {
        StoredRetry::Policy(rule) => rule.clone(),
        other => panic!("{policy} must read, got {other:?}"),
    }
}

#[test]
fn each_field_error_has_pydantic_s_text() {
    assert_python_details(&FIELD_CASES);
}

/// The fields in declaration order, then the keys that are not fields in the
/// order of the text.
#[test]
fn the_entries_come_in_field_order_then_the_extra_keys() {
    assert_python_details(&ORDER_CASES);
}

/// `MAX_VALIDATION_ERRORS_IN_TEXT`: ten entries, then `(+N more)`.
#[test]
fn a_text_writes_ten_entries_then_counts_the_rest() {
    assert_python_details(&COUNT_CASES);
}

/// pydantic stops at the item past the 20th and keeps only `too_long`.
#[test]
fn a_list_longer_than_twenty_has_only_the_too_long_entry() {
    assert_python_details(&TOO_LONG_CASES);
}

/// The strategy check, then the code check; a field the policy leaves out
/// has its default.
#[test]
fn the_model_validators_run_after_the_fields_in_python_s_order() {
    assert_python_details(&MODEL_CASES);
}

/// Python `validate_error_code_string`, per code in order: not empty, not an
/// exception class name, `UPPER_SNAKE_CASE`, not reserved.
#[test]
fn each_code_is_checked_in_order_with_python_s_text() {
    assert_python_details(&CODE_CASES);
}

/// A number with no fraction and no exponent is an int of any size.
#[test]
fn a_policy_that_is_not_an_object_names_its_python_type() {
    assert_python_details(&NOT_AN_OBJECT_CASES);
}

#[test]
fn the_run_is_refused_with_the_prefix_and_python_s_text() {
    let options =
        r#"{"retry_policy": {"max_retries": 1, "intervals_seconds": [1], "retry_for": ["x"]}}"#;
    assert_eq!(
        read_stored_options(Some(options)).run_permit(),
        RunPermit::Refuse(
            "stored retry_policy does not validate: Value error, retry_for 'x' is invalid; \
             expected UPPER_SNAKE_CASE (e.g. RATE_LIMITED) [value_error]"
                .to_owned()
        )
    );
}

/// A policy stored with the keys of an older release (`intervals`,
/// `auto_retry_for`) does not read: no alias reads an old key. The text
/// names the missing `retry_for` and each old key as an extra input, so the
/// run is refused before user code runs.
#[test]
fn a_policy_with_the_keys_of_an_older_release_is_refused() {
    assert_eq!(
        detail_of(r#"{"max_retries": 1, "intervals": [1], "auto_retry_for": ["X"]}"#),
        "retry_for: Field required [missing]; intervals: Extra inputs are not permitted \
         [extra_forbidden]; auto_retry_for: Extra inputs are not permitted [extra_forbidden]"
    );
}

/// `max_delay_seconds` is a `PositiveInt` with no upper bound: a cap above
/// `u64::MAX` reads, and shortens no delay.
#[test]
fn a_delay_cap_above_u64_reads_and_shortens_no_delay() {
    let policy = |cap: &str| {
        format!(
            r#"{{"max_retries": 20, "intervals_seconds": [86400], "backoff_strategy": "exponential",
                "jitter": false, "max_delay_seconds": {cap}, "retry_for": ["X"]}}"#
        )
    };
    assert_eq!(
        rule_of(&policy("18446744073709551615")).delay_cap,
        DelayCap::AtMost(u64::MAX)
    );
    for cap in ["18446744073709551616", "1000000000000000000000000000000"] {
        let rule = rule_of(&policy(cap));
        assert_eq!(rule.delay_cap, DelayCap::AboveU64Seconds, "{cap}");
        assert_eq!(rule.delay_seconds(20), 86_400.0 * 2.0_f64.powi(19), "{cap}");
    }
}

/// The bounds of each field admit their end values.
#[test]
fn the_end_values_of_each_bound_read() {
    let lowest = rule_of(
        r#"{"max_retries": 1, "intervals_seconds": [1], "max_delay_seconds": 1, "retry_for": ["A"]}"#,
    );
    assert_eq!(lowest.max_retries, 1);
    assert_eq!(lowest.intervals_seconds, vec![1]);
    assert_eq!(lowest.delay_cap, DelayCap::AtMost(1));
    let intervals = vec!["86400"; 20].join(", ");
    let highest = rule_of(&format!(
        r#"{{"max_retries": 20, "intervals_seconds": [{intervals}], "retry_for": ["Z9_"]}}"#
    ));
    assert_eq!(highest.max_retries, 20);
    assert_eq!(highest.intervals_seconds, vec![86_400; 20]);
}

const FIELD_CASES: [(&str, &str); 47] = [
    (
        r#"{"max_retries": "5", "retry_for": ["X"]}"#,
        "max_retries: Input should be a valid integer [int_type]",
    ),
    (
        r#"{"max_retries": 3.0, "retry_for": ["X"]}"#,
        "max_retries: Input should be a valid integer [int_type]",
    ),
    (
        r#"{"max_retries": 1e1, "retry_for": ["X"]}"#,
        "max_retries: Input should be a valid integer [int_type]",
    ),
    (
        r#"{"max_retries": true, "retry_for": ["X"]}"#,
        "max_retries: Input should be a valid integer [int_type]",
    ),
    (
        r#"{"max_retries": null, "retry_for": ["X"]}"#,
        "max_retries: Input should be a valid integer [int_type]",
    ),
    (
        r#"{"max_retries": 0, "retry_for": ["X"]}"#,
        "max_retries: Input should be greater than or equal to 1 [greater_than_equal]",
    ),
    (
        r#"{"max_retries": -0, "retry_for": ["X"]}"#,
        "max_retries: Input should be greater than or equal to 1 [greater_than_equal]",
    ),
    (
        r#"{"max_retries": -3, "retry_for": ["X"]}"#,
        "max_retries: Input should be greater than or equal to 1 [greater_than_equal]",
    ),
    (
        r#"{"max_retries": 21, "retry_for": ["X"]}"#,
        "max_retries: Input should be less than or equal to 20 [less_than_equal]",
    ),
    (
        r#"{"max_retries": 1000000000000000000000000000000, "retry_for": ["X"]}"#,
        "max_retries: Input should be less than or equal to 20 [less_than_equal]",
    ),
    (
        r#"{"max_retries": -1000000000000000000000000000000, "retry_for": ["X"]}"#,
        "max_retries: Input should be greater than or equal to 1 [greater_than_equal]",
    ),
    (
        r#"{"max_retries": 1, "intervals_seconds": "x", "retry_for": ["X"]}"#,
        "intervals_seconds: Input should be a valid list [list_type]",
    ),
    (
        r#"{"max_retries": 1, "intervals_seconds": null, "retry_for": ["X"]}"#,
        "intervals_seconds: Input should be a valid list [list_type]",
    ),
    (
        r#"{"max_retries": 1, "intervals_seconds": [], "retry_for": ["X"]}"#,
        "intervals_seconds: List should have at least 1 item after validation, not 0 [too_short]",
    ),
    (
        r#"{"max_retries": 1, "intervals_seconds": [0], "retry_for": ["X"]}"#,
        "intervals_seconds.0: Input should be greater than 0 [greater_than]",
    ),
    (
        r#"{"max_retries": 1, "intervals_seconds": [-1], "retry_for": ["X"]}"#,
        "intervals_seconds.0: Input should be greater than 0 [greater_than]",
    ),
    (
        r#"{"max_retries": 1, "intervals_seconds": [86401], "retry_for": ["X"]}"#,
        "intervals_seconds.0: Input should be less than or equal to 86400 [less_than_equal]",
    ),
    (
        r#"{"max_retries": 1, "intervals_seconds": [1000000000000000000000000000000], "retry_for": ["X"]}"#,
        "intervals_seconds.0: Input should be less than or equal to 86400 [less_than_equal]",
    ),
    (
        r#"{"max_retries": 1, "intervals_seconds": [1.0], "retry_for": ["X"]}"#,
        "intervals_seconds.0: Input should be a valid integer [int_type]",
    ),
    (
        r#"{"max_retries": 1, "intervals_seconds": [true], "retry_for": ["X"]}"#,
        "intervals_seconds.0: Input should be a valid integer [int_type]",
    ),
    (
        r#"{"max_retries": 1, "intervals_seconds": ["1"], "retry_for": ["X"]}"#,
        "intervals_seconds.0: Input should be a valid integer [int_type]",
    ),
    (
        r#"{"max_retries": 3, "intervals_seconds": [0, 5, 0], "retry_for": ["X"]}"#,
        "intervals_seconds.0: Input should be greater than 0 [greater_than]; intervals_seconds.2: Input should be greater than 0 [greater_than]",
    ),
    (
        r#"{"max_retries": 1, "intervals_seconds": [1], "backoff_strategy": "linear", "retry_for": ["X"]}"#,
        "backoff_strategy: Input should be 'fixed' or 'exponential' [literal_error]",
    ),
    (
        r#"{"max_retries": 1, "intervals_seconds": [1], "backoff_strategy": "FIXED", "retry_for": ["X"]}"#,
        "backoff_strategy: Input should be 'fixed' or 'exponential' [literal_error]",
    ),
    (
        r#"{"max_retries": 1, "intervals_seconds": [1], "backoff_strategy": null, "retry_for": ["X"]}"#,
        "backoff_strategy: Input should be 'fixed' or 'exponential' [literal_error]",
    ),
    (
        r#"{"max_retries": 1, "intervals_seconds": [1], "backoff_strategy": 1, "retry_for": ["X"]}"#,
        "backoff_strategy: Input should be 'fixed' or 'exponential' [literal_error]",
    ),
    (
        r#"{"max_retries": 1, "intervals_seconds": [1], "jitter": 1, "retry_for": ["X"]}"#,
        "jitter: Input should be a valid boolean [bool_type]",
    ),
    (
        r#"{"max_retries": 1, "intervals_seconds": [1], "jitter": "true", "retry_for": ["X"]}"#,
        "jitter: Input should be a valid boolean [bool_type]",
    ),
    (
        r#"{"max_retries": 1, "intervals_seconds": [1], "jitter": null, "retry_for": ["X"]}"#,
        "jitter: Input should be a valid boolean [bool_type]",
    ),
    (
        r#"{"max_retries": 1, "intervals_seconds": [1], "max_delay_seconds": 0, "retry_for": ["X"]}"#,
        "max_delay_seconds: Input should be greater than 0 [greater_than]",
    ),
    (
        r#"{"max_retries": 1, "intervals_seconds": [1], "max_delay_seconds": -0, "retry_for": ["X"]}"#,
        "max_delay_seconds: Input should be greater than 0 [greater_than]",
    ),
    (
        r#"{"max_retries": 1, "intervals_seconds": [1], "max_delay_seconds": -1000000000000000000000000000000, "retry_for": ["X"]}"#,
        "max_delay_seconds: Input should be greater than 0 [greater_than]",
    ),
    (
        r#"{"max_retries": 1, "intervals_seconds": [1], "max_delay_seconds": 1.5, "retry_for": ["X"]}"#,
        "max_delay_seconds: Input should be a valid integer [int_type]",
    ),
    (
        r#"{"max_retries": 1, "intervals_seconds": [1], "max_delay_seconds": 5.0, "retry_for": ["X"]}"#,
        "max_delay_seconds: Input should be a valid integer [int_type]",
    ),
    (
        r#"{"max_retries": 1, "intervals_seconds": [1], "max_delay_seconds": "5", "retry_for": ["X"]}"#,
        "max_delay_seconds: Input should be a valid integer [int_type]",
    ),
    (
        r#"{"max_retries": 1, "intervals_seconds": [1], "max_delay_seconds": true, "retry_for": ["X"]}"#,
        "max_delay_seconds: Input should be a valid integer [int_type]",
    ),
    (
        r#"{"max_retries": 1, "intervals_seconds": [1]}"#,
        "retry_for: Field required [missing]",
    ),
    (
        r#"{"max_retries": 1, "intervals_seconds": [1], "retry_for": null}"#,
        "retry_for: Input should be a valid list [list_type]",
    ),
    (
        r#"{"max_retries": 1, "intervals_seconds": [1], "retry_for": "X"}"#,
        "retry_for: Input should be a valid list [list_type]",
    ),
    (
        r#"{"max_retries": 1, "intervals_seconds": [1], "retry_for": {}}"#,
        "retry_for: Input should be a valid list [list_type]",
    ),
    (
        r#"{"max_retries": 1, "intervals_seconds": [1], "retry_for": []}"#,
        "retry_for: List should have at least 1 item after validation, not 0 [too_short]",
    ),
    (
        r#"{"max_retries": 1, "intervals_seconds": [1], "retry_for": ["X", 5]}"#,
        "retry_for.1.str: Input should be a valid string [string_type]; retry_for.1.union[str-enum[OperationalCode],str-enum[ContractCode],str-enum[RetrievalCode],str-enum[OutcomeCode]].str-enum[OperationalCode]: Input should be an instance of OperationalCode [is_instance_of]; retry_for.1.union[str-enum[OperationalCode],str-enum[ContractCode],str-enum[RetrievalCode],str-enum[OutcomeCode]].str-enum[ContractCode]: Input should be an instance of ContractCode [is_instance_of]; retry_for.1.union[str-enum[OperationalCode],str-enum[ContractCode],str-enum[RetrievalCode],str-enum[OutcomeCode]].str-enum[RetrievalCode]: Input should be an instance of RetrievalCode [is_instance_of]; retry_for.1.union[str-enum[OperationalCode],str-enum[ContractCode],str-enum[RetrievalCode],str-enum[OutcomeCode]].str-enum[OutcomeCode]: Input should be an instance of OutcomeCode [is_instance_of]",
    ),
    (
        r#"{"max_retries": 1, "intervals_seconds": [1], "retry_for": [{"__builtin_task_code__": "TASK_TIMEOUT"}]}"#,
        "retry_for.0.str: Input should be a valid string [string_type]; retry_for.0.union[str-enum[OperationalCode],str-enum[ContractCode],str-enum[RetrievalCode],str-enum[OutcomeCode]].str-enum[OperationalCode]: Input should be an instance of OperationalCode [is_instance_of]; retry_for.0.union[str-enum[OperationalCode],str-enum[ContractCode],str-enum[RetrievalCode],str-enum[OutcomeCode]].str-enum[ContractCode]: Input should be an instance of ContractCode [is_instance_of]; retry_for.0.union[str-enum[OperationalCode],str-enum[ContractCode],str-enum[RetrievalCode],str-enum[OutcomeCode]].str-enum[RetrievalCode]: Input should be an instance of RetrievalCode [is_instance_of]; retry_for.0.union[str-enum[OperationalCode],str-enum[ContractCode],str-enum[RetrievalCode],str-enum[OutcomeCode]].str-enum[OutcomeCode]: Input should be an instance of OutcomeCode [is_instance_of]",
    ),
    (
        r#"{"max_retries": 1, "intervals_seconds": [1], "retry_for": ["X"], "extra": 1}"#,
        "extra: Extra inputs are not permitted [extra_forbidden]",
    ),
    (
        r#"{"max_retries": 1, "intervals_seconds": [1], "retry_for": ["X"], "": 1}"#,
        "Extra inputs are not permitted [extra_forbidden]",
    ),
    (
        r#"{"max_retries": 1, "intervals_seconds": [1], "retry_for": ["X"], "a.b": null}"#,
        "a.b: Extra inputs are not permitted [extra_forbidden]",
    ),
    (
        r#"{}"#,
        "retry_for: Field required [missing]",
    ),
];

const ORDER_CASES: [(&str, &str); 3] = [
    (
        r#"{"zz": 1, "max_retries": 0, "intervals_seconds": [0], "jitter": 1, "aa": 2}"#,
        "max_retries: Input should be greater than or equal to 1 [greater_than_equal]; intervals_seconds.0: Input should be greater than 0 [greater_than]; jitter: Input should be a valid boolean [bool_type]; retry_for: Field required [missing]; zz: Extra inputs are not permitted [extra_forbidden]; aa: Extra inputs are not permitted [extra_forbidden]",
    ),
    (
        r#"{"q": 1, "max_retries": 99, "intervals_seconds": [1], "retry_for": [1], "jitter": 2}"#,
        "max_retries: Input should be less than or equal to 20 [less_than_equal]; jitter: Input should be a valid boolean [bool_type]; retry_for.0.str: Input should be a valid string [string_type]; retry_for.0.union[str-enum[OperationalCode],str-enum[ContractCode],str-enum[RetrievalCode],str-enum[OutcomeCode]].str-enum[OperationalCode]: Input should be an instance of OperationalCode [is_instance_of]; retry_for.0.union[str-enum[OperationalCode],str-enum[ContractCode],str-enum[RetrievalCode],str-enum[OutcomeCode]].str-enum[ContractCode]: Input should be an instance of ContractCode [is_instance_of]; retry_for.0.union[str-enum[OperationalCode],str-enum[ContractCode],str-enum[RetrievalCode],str-enum[OutcomeCode]].str-enum[RetrievalCode]: Input should be an instance of RetrievalCode [is_instance_of]; retry_for.0.union[str-enum[OperationalCode],str-enum[ContractCode],str-enum[RetrievalCode],str-enum[OutcomeCode]].str-enum[OutcomeCode]: Input should be an instance of OutcomeCode [is_instance_of]; q: Extra inputs are not permitted [extra_forbidden]",
    ),
    (
        r#"{"retry_for": [], "max_delay_seconds": 0, "backoff_strategy": "x", "intervals_seconds": [], "max_retries": 0}"#,
        "max_retries: Input should be greater than or equal to 1 [greater_than_equal]; intervals_seconds: List should have at least 1 item after validation, not 0 [too_short]; backoff_strategy: Input should be 'fixed' or 'exponential' [literal_error]; max_delay_seconds: Input should be greater than 0 [greater_than]; retry_for: List should have at least 1 item after validation, not 0 [too_short]",
    ),
];

const COUNT_CASES: [(&str, &str); 5] = [
    (
        r#"{"max_retries": 20, "intervals_seconds": [0, 0, 0, 0, 0, 0, 0, 0, 0, 0], "retry_for": ["X"]}"#,
        "intervals_seconds.0: Input should be greater than 0 [greater_than]; intervals_seconds.1: Input should be greater than 0 [greater_than]; intervals_seconds.2: Input should be greater than 0 [greater_than]; intervals_seconds.3: Input should be greater than 0 [greater_than]; intervals_seconds.4: Input should be greater than 0 [greater_than]; intervals_seconds.5: Input should be greater than 0 [greater_than]; intervals_seconds.6: Input should be greater than 0 [greater_than]; intervals_seconds.7: Input should be greater than 0 [greater_than]; intervals_seconds.8: Input should be greater than 0 [greater_than]; intervals_seconds.9: Input should be greater than 0 [greater_than]",
    ),
    (
        r#"{"max_retries": 20, "intervals_seconds": [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0], "retry_for": ["X"]}"#,
        "intervals_seconds.0: Input should be greater than 0 [greater_than]; intervals_seconds.1: Input should be greater than 0 [greater_than]; intervals_seconds.2: Input should be greater than 0 [greater_than]; intervals_seconds.3: Input should be greater than 0 [greater_than]; intervals_seconds.4: Input should be greater than 0 [greater_than]; intervals_seconds.5: Input should be greater than 0 [greater_than]; intervals_seconds.6: Input should be greater than 0 [greater_than]; intervals_seconds.7: Input should be greater than 0 [greater_than]; intervals_seconds.8: Input should be greater than 0 [greater_than]; intervals_seconds.9: Input should be greater than 0 [greater_than]; (+1 more)",
    ),
    (
        r#"{"max_retries": 20, "intervals_seconds": [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0], "retry_for": ["X"]}"#,
        "intervals_seconds.0: Input should be greater than 0 [greater_than]; intervals_seconds.1: Input should be greater than 0 [greater_than]; intervals_seconds.2: Input should be greater than 0 [greater_than]; intervals_seconds.3: Input should be greater than 0 [greater_than]; intervals_seconds.4: Input should be greater than 0 [greater_than]; intervals_seconds.5: Input should be greater than 0 [greater_than]; intervals_seconds.6: Input should be greater than 0 [greater_than]; intervals_seconds.7: Input should be greater than 0 [greater_than]; intervals_seconds.8: Input should be greater than 0 [greater_than]; intervals_seconds.9: Input should be greater than 0 [greater_than]; (+5 more)",
    ),
    (
        r#"{"max_retries": 1, "intervals_seconds": [1], "retry_for": [1, 2]}"#,
        "retry_for.0.str: Input should be a valid string [string_type]; retry_for.0.union[str-enum[OperationalCode],str-enum[ContractCode],str-enum[RetrievalCode],str-enum[OutcomeCode]].str-enum[OperationalCode]: Input should be an instance of OperationalCode [is_instance_of]; retry_for.0.union[str-enum[OperationalCode],str-enum[ContractCode],str-enum[RetrievalCode],str-enum[OutcomeCode]].str-enum[ContractCode]: Input should be an instance of ContractCode [is_instance_of]; retry_for.0.union[str-enum[OperationalCode],str-enum[ContractCode],str-enum[RetrievalCode],str-enum[OutcomeCode]].str-enum[RetrievalCode]: Input should be an instance of RetrievalCode [is_instance_of]; retry_for.0.union[str-enum[OperationalCode],str-enum[ContractCode],str-enum[RetrievalCode],str-enum[OutcomeCode]].str-enum[OutcomeCode]: Input should be an instance of OutcomeCode [is_instance_of]; retry_for.1.str: Input should be a valid string [string_type]; retry_for.1.union[str-enum[OperationalCode],str-enum[ContractCode],str-enum[RetrievalCode],str-enum[OutcomeCode]].str-enum[OperationalCode]: Input should be an instance of OperationalCode [is_instance_of]; retry_for.1.union[str-enum[OperationalCode],str-enum[ContractCode],str-enum[RetrievalCode],str-enum[OutcomeCode]].str-enum[ContractCode]: Input should be an instance of ContractCode [is_instance_of]; retry_for.1.union[str-enum[OperationalCode],str-enum[ContractCode],str-enum[RetrievalCode],str-enum[OutcomeCode]].str-enum[RetrievalCode]: Input should be an instance of RetrievalCode [is_instance_of]; retry_for.1.union[str-enum[OperationalCode],str-enum[ContractCode],str-enum[RetrievalCode],str-enum[OutcomeCode]].str-enum[OutcomeCode]: Input should be an instance of OutcomeCode [is_instance_of]",
    ),
    (
        r#"{"max_retries": 1, "intervals_seconds": [1], "retry_for": [1, 2, 3]}"#,
        "retry_for.0.str: Input should be a valid string [string_type]; retry_for.0.union[str-enum[OperationalCode],str-enum[ContractCode],str-enum[RetrievalCode],str-enum[OutcomeCode]].str-enum[OperationalCode]: Input should be an instance of OperationalCode [is_instance_of]; retry_for.0.union[str-enum[OperationalCode],str-enum[ContractCode],str-enum[RetrievalCode],str-enum[OutcomeCode]].str-enum[ContractCode]: Input should be an instance of ContractCode [is_instance_of]; retry_for.0.union[str-enum[OperationalCode],str-enum[ContractCode],str-enum[RetrievalCode],str-enum[OutcomeCode]].str-enum[RetrievalCode]: Input should be an instance of RetrievalCode [is_instance_of]; retry_for.0.union[str-enum[OperationalCode],str-enum[ContractCode],str-enum[RetrievalCode],str-enum[OutcomeCode]].str-enum[OutcomeCode]: Input should be an instance of OutcomeCode [is_instance_of]; retry_for.1.str: Input should be a valid string [string_type]; retry_for.1.union[str-enum[OperationalCode],str-enum[ContractCode],str-enum[RetrievalCode],str-enum[OutcomeCode]].str-enum[OperationalCode]: Input should be an instance of OperationalCode [is_instance_of]; retry_for.1.union[str-enum[OperationalCode],str-enum[ContractCode],str-enum[RetrievalCode],str-enum[OutcomeCode]].str-enum[ContractCode]: Input should be an instance of ContractCode [is_instance_of]; retry_for.1.union[str-enum[OperationalCode],str-enum[ContractCode],str-enum[RetrievalCode],str-enum[OutcomeCode]].str-enum[RetrievalCode]: Input should be an instance of RetrievalCode [is_instance_of]; retry_for.1.union[str-enum[OperationalCode],str-enum[ContractCode],str-enum[RetrievalCode],str-enum[OutcomeCode]].str-enum[OutcomeCode]: Input should be an instance of OutcomeCode [is_instance_of]; (+5 more)",
    ),
];

const TOO_LONG_CASES: [(&str, &str); 3] = [
    (
        r#"{"max_retries": 20, "intervals_seconds": [1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1], "retry_for": ["X"]}"#,
        "intervals_seconds: List should have at most 20 items after validation, not 21 [too_long]",
    ),
    (
        r#"{"max_retries": 20, "intervals_seconds": [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0], "retry_for": ["X"]}"#,
        "intervals_seconds: List should have at most 20 items after validation, not 21 [too_long]",
    ),
    (
        r#"{"max_retries": 20, "intervals_seconds": ["a", "a", "a", "a", "a", "a", "a", "a", "a", "a", "a", "a", "a", "a", "a", "a", "a", "a", "a", "a", "a", "a", "a", "a", "a"], "retry_for": ["X"]}"#,
        "intervals_seconds: List should have at most 20 items after validation, not 25 [too_long]",
    ),
];

const MODEL_CASES: [(&str, &str); 5] = [
    (
        r#"{"max_retries": 2, "intervals_seconds": [1], "retry_for": ["X"]}"#,
        "Value error, Fixed backoff strategy requires intervals_seconds length (1) to match max_retries (2). Either adjust intervals_seconds list or use exponential strategy. [value_error]",
    ),
    (
        r#"{"max_retries": 2, "intervals_seconds": [1, 2], "backoff_strategy": "exponential", "retry_for": ["X"]}"#,
        "Value error, Exponential backoff strategy requires exactly one base interval, got 2 intervals. Use intervals_seconds=[base_seconds] for exponential backoff. [value_error]",
    ),
    (
        r#"{"intervals_seconds": [1], "retry_for": ["x"]}"#,
        "Value error, Fixed backoff strategy requires intervals_seconds length (1) to match max_retries (3). Either adjust intervals_seconds list or use exponential strategy. [value_error]",
    ),
    (
        r#"{"backoff_strategy": "exponential", "retry_for": ["X"]}"#,
        "Value error, Exponential backoff strategy requires exactly one base interval, got 3 intervals. Use intervals_seconds=[base_seconds] for exponential backoff. [value_error]",
    ),
    (
        r#"{"max_retries": 2, "intervals_seconds": [1], "retry_for": ["x"]}"#,
        "Value error, Fixed backoff strategy requires intervals_seconds length (1) to match max_retries (2). Either adjust intervals_seconds list or use exponential strategy. [value_error]",
    ),
];

const CODE_CASES: [(&str, &str); 20] = [
    (
        r#"{"max_retries": 1, "intervals_seconds": [1], "retry_for": [""]}"#,
        "Value error, retry_for must be a non-empty string, got '' [value_error]",
    ),
    (
        r#"{"max_retries": 1, "intervals_seconds": [1], "retry_for": ["X", ""]}"#,
        "Value error, retry_for must be a non-empty string, got '' [value_error]",
    ),
    (
        r#"{"max_retries": 1, "intervals_seconds": [1], "retry_for": ["x"]}"#,
        "Value error, retry_for 'x' is invalid; expected UPPER_SNAKE_CASE (e.g. RATE_LIMITED) [value_error]",
    ),
    (
        r#"{"max_retries": 1, "intervals_seconds": [1], "retry_for": ["lower_case"]}"#,
        "Value error, retry_for 'lower_case' is invalid; expected UPPER_SNAKE_CASE (e.g. RATE_LIMITED) [value_error]",
    ),
    (
        r#"{"max_retries": 1, "intervals_seconds": [1], "retry_for": ["1ABC"]}"#,
        "Value error, retry_for '1ABC' is invalid; expected UPPER_SNAKE_CASE (e.g. RATE_LIMITED) [value_error]",
    ),
    (
        r#"{"max_retries": 1, "intervals_seconds": [1], "retry_for": ["A-B"]}"#,
        "Value error, retry_for 'A-B' is invalid; expected UPPER_SNAKE_CASE (e.g. RATE_LIMITED) [value_error]",
    ),
    (
        r#"{"max_retries": 1, "intervals_seconds": [1], "retry_for": ["AB "]}"#,
        "Value error, retry_for 'AB ' is invalid; expected UPPER_SNAKE_CASE (e.g. RATE_LIMITED) [value_error]",
    ),
    (
        r#"{"max_retries": 1, "intervals_seconds": [1], "retry_for": ["AB\n"]}"#,
        "Value error, retry_for 'AB\n' is invalid; expected UPPER_SNAKE_CASE (e.g. RATE_LIMITED) [value_error]",
    ),
    (
        r#"{"max_retries": 1, "intervals_seconds": [1], "retry_for": ["A'B"]}"#,
        "Value error, retry_for 'A'B' is invalid; expected UPPER_SNAKE_CASE (e.g. RATE_LIMITED) [value_error]",
    ),
    (
        r#"{"max_retries": 1, "intervals_seconds": [1], "retry_for": ["\u00c9"]}"#,
        "Value error, retry_for 'É' is invalid; expected UPPER_SNAKE_CASE (e.g. RATE_LIMITED) [value_error]",
    ),
    (
        r#"{"max_retries": 1, "intervals_seconds": [1], "retry_for": ["Error"]}"#,
        "Value error, retry_for 'Error' is invalid; expected UPPER_SNAKE_CASE (e.g. RATE_LIMITED) [value_error]",
    ),
    (
        r#"{"max_retries": 1, "intervals_seconds": [1], "retry_for": ["Exception"]}"#,
        "Value error, retry_for 'Exception' is invalid; expected UPPER_SNAKE_CASE (e.g. RATE_LIMITED) [value_error]",
    ),
    (
        r#"{"max_retries": 1, "intervals_seconds": [1], "retry_for": ["MyError"]}"#,
        "Value error, retry_for 'MyError' looks like an exception class name; retry matching is error-code-only, use UPPER_SNAKE_CASE code names [value_error]",
    ),
    (
        r#"{"max_retries": 1, "intervals_seconds": [1], "retry_for": ["MY_Error"]}"#,
        "Value error, retry_for 'MY_Error' looks like an exception class name; retry matching is error-code-only, use UPPER_SNAKE_CASE code names [value_error]",
    ),
    (
        r#"{"max_retries": 1, "intervals_seconds": [1], "retry_for": ["TimeoutError"]}"#,
        "Value error, retry_for 'TimeoutError' looks like an exception class name; retry matching is error-code-only, use UPPER_SNAKE_CASE code names [value_error]",
    ),
    (
        r#"{"max_retries": 1, "intervals_seconds": [1], "retry_for": ["AException"]}"#,
        "Value error, retry_for 'AException' looks like an exception class name; retry matching is error-code-only, use UPPER_SNAKE_CASE code names [value_error]",
    ),
    (
        r#"{"max_retries": 1, "intervals_seconds": [1], "retry_for": ["Bad-Error"]}"#,
        "Value error, retry_for 'Bad-Error' is invalid; expected UPPER_SNAKE_CASE (e.g. RATE_LIMITED) [value_error]",
    ),
    (
        r#"{"max_retries": 1, "intervals_seconds": [1], "retry_for": ["WORKFLOW_STOPPED"]}"#,
        "Value error, retry_for 'WORKFLOW_STOPPED' is a reserved error code name and is not a task code; use another UPPER_SNAKE_CASE code [value_error]",
    ),
    (
        r#"{"max_retries": 1, "intervals_seconds": [1], "retry_for": ["WORKFLOW_STOPPED", "x"]}"#,
        "Value error, retry_for 'WORKFLOW_STOPPED' is a reserved error code name and is not a task code; use another UPPER_SNAKE_CASE code [value_error]",
    ),
    (
        r#"{"max_retries": 1, "intervals_seconds": [1], "retry_for": ["x", "WORKFLOW_STOPPED"]}"#,
        "Value error, retry_for 'x' is invalid; expected UPPER_SNAKE_CASE (e.g. RATE_LIMITED) [value_error]",
    ),
];

const NOT_AN_OBJECT_CASES: [(&str, &str); 9] = [
    (r#""text""#, "retry_policy is not an object: str"),
    (r#"[1, 2]"#, "retry_policy is not an object: list"),
    (r#"5"#, "retry_policy is not an object: int"),
    (r#"1.5"#, "retry_policy is not an object: float"),
    (r#"1e5"#, "retry_policy is not an object: float"),
    (r#"true"#, "retry_policy is not an object: bool"),
    (r#"false"#, "retry_policy is not an object: bool"),
    (
        r#"100000000000000000000000000"#,
        "retry_policy is not an object: int",
    ),
    (r#"-7"#, "retry_policy is not an object: int"),
];
