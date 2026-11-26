use serde::{Deserialize, Serialize};

use super::error::TaskErrorCode;
use super::execution::{Execution, MAX_CANCEL_GRACE_MS, MIN_CANCEL_GRACE_MS};
use crate::core::python_text::python_isoformat_utc;

/// The cancel grace of a task, from [`MIN_CANCEL_GRACE_MS`] to
/// [`MAX_CANCEL_GRACE_MS`] milliseconds (Python `cancel_grace_ms`):
///
/// - `SHARED_ASYNC`: the time from the cancel (at the deadline, or at an
///   operator cancel) to the second cancel (Rust: the drop of its future).
/// - `ISOLATED`: the time from an operator cancel to the kill of the child
///   when the run has not ended.
///
/// Only [`CancelGraceMs::new`] builds one, and serde reads one through it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(try_from = "u32", into = "u32")]
pub struct CancelGraceMs(u32);

/// A cancel grace outside its bounds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error(
    "cancel_grace_ms must be from {MIN_CANCEL_GRACE_MS} to {MAX_CANCEL_GRACE_MS} ms, got {value}"
)]
pub struct CancelGraceOutOfRange {
    pub value: u32,
}

impl CancelGraceMs {
    /// The grace of a `SHARED_ASYNC` task that names none (Python
    /// `DEFAULT_CANCEL_GRACE_MS`).
    pub const DEFAULT: Self = Self(1_000);

    /// The grace of `milliseconds`.
    ///
    /// Errors: [`CancelGraceOutOfRange`] outside [`MIN_CANCEL_GRACE_MS`] to
    /// [`MAX_CANCEL_GRACE_MS`].
    pub fn new(milliseconds: u32) -> Result<Self, CancelGraceOutOfRange> {
        match (MIN_CANCEL_GRACE_MS..=MAX_CANCEL_GRACE_MS).contains(&i64::from(milliseconds)) {
            true => Ok(Self(milliseconds)),
            false => Err(CancelGraceOutOfRange {
                value: milliseconds,
            }),
        }
    }

    /// The grace in milliseconds.
    pub fn get(self) -> u32 {
        self.0
    }
}

impl TryFrom<u32> for CancelGraceMs {
    type Error = CancelGraceOutOfRange;

    fn try_from(milliseconds: u32) -> Result<Self, Self::Error> {
        Self::new(milliseconds)
    }
}

impl From<CancelGraceMs> for u32 {
    fn from(grace: CancelGraceMs) -> Self {
        grace.0
    }
}

/// The class of a task whose options name none.
fn isolated() -> Execution {
    Execution::Isolated
}

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
    /// Optional maximum retry delay in seconds, applied after backoff and
    /// jitter. When unset, exponential backoff is unbounded. Omitted from
    /// serialized task options when `None`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_delay_seconds: Option<u32>,
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

    #[error("max_delay_seconds must be greater than 0 when set")]
    NonPositiveMaxDelay,
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
            max_delay_seconds: None,
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
            max_delay_seconds: None,
        })
    }

    /// Set an opt-in maximum retry delay (seconds), applied after backoff and
    /// jitter. Caps unbounded exponential growth.
    pub fn with_max_delay_seconds(mut self, max_delay_seconds: u32) -> Self {
        self.max_delay_seconds = Some(max_delay_seconds);
        self
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
        if let Some(0) = self.max_delay_seconds {
            return Err(RetryPolicyError::NonPositiveMaxDelay);
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

    /// Task expiry deadline (task skipped if execution has not started by this time).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub good_until: Option<chrono::DateTime<chrono::Utc>>,

    /// Error codes that trigger automatic retries.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub auto_retry_for: Option<Vec<TaskErrorCode>>,

    /// Retry timing and backoff configuration.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub retry_policy: Option<RetryPolicy>,

    /// Per-task execution time limit in milliseconds (minimum 1000), measured
    /// around user-code execution. On expiry the task fails with
    /// `OutcomeCode::TaskTimeout`; retry is opt-in by listing `"TASK_TIMEOUT"`
    /// in `auto_retry_for`.
    ///
    /// Cancellation semantics differ by task kind:
    /// - **Async tasks** are aborted at their next `.await` point (cleanly
    ///   cancelled).
    /// - **Blocking tasks** (`spawn_blocking`) CANNOT be aborted in-process: the
    ///   worker stops awaiting the thread and finalizes as `TASK_TIMEOUT`, but
    ///   the thread runs to completion and its result is discarded. There is no
    ///   way to forcibly stop synchronous, CPU-bound user code mid-execution
    ///   inside the process. Use `timeout_ms` on blocking tasks as a
    ///   finalize-deadline, not a guaranteed-stop.
    ///
    /// A `SHARED_ASYNC` or `SHARED_THREAD` task must carry one: a new send
    /// refuses a shared class without it (`check_shared_timeout`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub timeout_ms: Option<u32>,

    /// How a worker runs the task: in an isolated child (the default), on a
    /// seat of a shared async child, or on a seat of a shared thread child.
    #[serde(default = "isolated")]
    pub execution: Execution,

    /// The cancel grace of the task. A `SHARED_ASYNC` task carries one, and
    /// it must; an `ISOLATED` task may carry one (none: an operator cancel
    /// kills nothing); a `SHARED_THREAD` task carries none. Enqueue refuses
    /// a grace on `SHARED_THREAD` and a `SHARED_ASYNC` task without one
    /// (`stored_execution`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cancel_grace_ms: Option<CancelGraceMs>,
}

impl TaskOptions {
    /// The stored `task_options` text of this task, in Python's layout
    /// (`clients/python/horsies/core/codec/task_options.py`, `serialize_task_options`), the
    /// one layout the core reads at enqueue and at dispatch:
    ///
    /// ```json
    /// {"retry_policy": {"max_retries": 3, "intervals": [60, 300, 900],
    ///                   "backoff_strategy": "fixed", "jitter": true,
    ///                   "auto_retry_for": ["STORAGE_ERROR"]},
    ///  "good_until": null, "timeout_ms": 2000, "execution": "ISOLATED",
    ///  "cancel_grace_ms": null}
    /// ```
    ///
    /// - `retry_policy` holds the policy with `auto_retry_for` inside it, and
    ///   `max_delay_seconds` only when set; it is `null` with no policy. Codes
    ///   without a policy are not stored: with no policy a task has no retry.
    /// - `good_until` is `null`: a task has no deadline of its own. A send
    ///   with a deadline stores [`Self::stored_send_options_text`]; a workflow
    ///   node merges its deadline into this text.
    /// - `execution` is the class of the task, and `cancel_grace_ms` its
    ///   grace, `null` when it has none. The text holds what the options say:
    ///   a grace on `SHARED_THREAD`, or a `SHARED_ASYNC` task without one, is
    ///   refused at enqueue (`stored_execution`), and a shared class without
    ///   `timeout_ms` is refused at a new send (`check_shared_timeout`).
    ///
    /// Every task gets the text, as every Python task does: an `ISOLATED`
    /// task with no grace, no retry policy and no timeout stores the text
    /// with nulls (`tests/fixtures/stored_forms/task_options.json`).
    pub fn stored_options_text(&self) -> String {
        self.stored_text(None)
    }

    /// The stored `task_options` text of a send of this task with the
    /// deadline `good_until` (Python `serialize_task_options` of the send's
    /// options, `clients/python/horsies/core/task_decorator.py`): the text of
    /// [`Self::stored_options_text`] with `good_until` as Python's
    /// `isoformat()` writes an aware UTC time (`+00:00`, 6 fraction digits
    /// when the microsecond is not 0). The send also stores the deadline in
    /// the row's `good_until` column. Pure.
    pub fn stored_send_options_text(&self, good_until: chrono::DateTime<chrono::Utc>) -> String {
        self.stored_text(Some(python_isoformat_utc(good_until)))
    }

    /// The stored text with `good_until` as given (`None` is `null`).
    fn stored_text(&self, good_until: Option<String>) -> String {
        let stored = StoredTaskOptions {
            retry_policy: self.retry_policy.as_ref().map(|policy| StoredRetryPolicy {
                max_retries: policy.max_retries,
                intervals: &policy.intervals,
                backoff_strategy: policy.backoff_strategy,
                jitter: policy.jitter,
                max_delay_seconds: policy.max_delay_seconds,
                // Python stores each code as its plain name; the tagged serde
                // form of a built-in code is refused by the reader.
                auto_retry_for: self
                    .auto_retry_for
                    .as_deref()
                    .unwrap_or_default()
                    .iter()
                    .map(ToString::to_string)
                    .collect(),
            }),
            good_until,
            timeout_ms: self.timeout_ms,
            execution: self.execution.as_str(),
            cancel_grace_ms: self.cancel_grace_ms.map(CancelGraceMs::get),
        };
        // Every field is a plain number, string, bool or list of them, so the
        // encode cannot fail.
        serde_json::to_string(&stored).expect("stored task options encode to JSON")
    }
}

/// Python's stored task options, in its key order.
#[derive(Serialize)]
struct StoredTaskOptions<'a> {
    retry_policy: Option<StoredRetryPolicy<'a>>,
    good_until: Option<String>,
    timeout_ms: Option<u32>,
    execution: &'static str,
    cancel_grace_ms: Option<u32>,
}

/// Python's `RetryPolicy.model_dump(mode="json", exclude_none=True)`.
#[derive(Serialize)]
struct StoredRetryPolicy<'a> {
    max_retries: u32,
    intervals: &'a [u32],
    backoff_strategy: BackoffStrategy,
    jitter: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    max_delay_seconds: Option<u32>,
    auto_retry_for: Vec<String>,
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
            max_delay_seconds: None,
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
            max_delay_seconds: None,
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
    fn stored_options_text_with_both_fields() {
        use super::super::error::{CustomCode, TaskErrorCode};

        let opts = TaskOptions {
            task_name: "my_task".to_owned(),
            queue_name: Some("high".to_owned()),
            good_until: None,
            auto_retry_for: Some(vec![TaskErrorCode::Custom(
                CustomCode::parse("STORAGE_ERROR").unwrap(),
            )]),
            retry_policy: Some(RetryPolicy::fixed(vec![60, 300, 900], true).unwrap()),
            timeout_ms: None,
            execution: Execution::Isolated,
            cancel_grace_ms: None,
        };

        let json = opts.stored_options_text();
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();

        // Python's layout: auto_retry_for inside retry_policy, the five keys of
        // serialize_task_options, and no identity fields.
        assert_eq!(
            parsed,
            serde_json::json!({
                "retry_policy": {
                    "max_retries": 3,
                    "intervals": [60, 300, 900],
                    "backoff_strategy": "fixed",
                    "jitter": true,
                    "auto_retry_for": ["STORAGE_ERROR"],
                },
                "good_until": null,
                "timeout_ms": null,
                "execution": "ISOLATED",
                "cancel_grace_ms": null,
            })
        );
    }

    /// A built-in code is stored as its plain name, as Python stores it.
    #[test]
    fn a_builtin_retry_code_is_stored_as_its_name() {
        use super::super::error::{BuiltInTaskCode, OutcomeCode, TaskErrorCode};

        let opts = TaskOptions {
            task_name: "my_task".to_owned(),
            queue_name: None,
            good_until: None,
            auto_retry_for: Some(vec![TaskErrorCode::BuiltIn(BuiltInTaskCode::Outcome(
                OutcomeCode::TaskTimeout,
            ))]),
            retry_policy: Some(RetryPolicy::exponential(30, 2, false).unwrap()),
            timeout_ms: Some(5_000),
            execution: Execution::Isolated,
            cancel_grace_ms: None,
        };
        let json = opts.stored_options_text();
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();

        assert_eq!(
            parsed["retry_policy"]["auto_retry_for"],
            serde_json::json!(["TASK_TIMEOUT"])
        );
    }

    /// The core writes the layout its worker reads: the dispatch reader takes
    /// the stored text as a policy and a timeout, not as a refusal. An
    /// `ISOLATED` task with no grace stores none.
    #[test]
    fn the_stored_text_is_what_the_dispatch_reader_accepts() {
        use super::super::error::{CustomCode, TaskErrorCode};
        use crate::worker::retry::{
            read_stored_options, StoredCancelGrace, StoredOptions, StoredRetry, StoredTimeout,
        };

        let opts = TaskOptions {
            task_name: "my_task".to_owned(),
            queue_name: None,
            good_until: None,
            auto_retry_for: Some(vec![TaskErrorCode::Custom(
                CustomCode::parse("STORAGE_ERROR").unwrap(),
            )]),
            retry_policy: Some(
                RetryPolicy::fixed(vec![60, 300], true)
                    .unwrap()
                    .with_max_delay_seconds(600),
            ),
            timeout_ms: Some(2_000),
            execution: Execution::Isolated,
            cancel_grace_ms: None,
        };
        let json = opts.stored_options_text();

        match read_stored_options(Some(&json)) {
            StoredOptions::Readable {
                timeout: StoredTimeout::Limit(limit),
                cancel_grace: StoredCancelGrace::NotStored,
                retry: StoredRetry::Policy(_),
            } => assert_eq!(limit, std::time::Duration::from_millis(2_000)),
            other => panic!("the dispatch reader refused the stored options: {other:?}"),
        }
    }

    fn options_of(
        execution: Execution,
        cancel_grace_ms: Option<CancelGraceMs>,
        timeout_ms: Option<u32>,
    ) -> TaskOptions {
        TaskOptions {
            task_name: "my_task".to_owned(),
            queue_name: None,
            good_until: None,
            auto_retry_for: None,
            retry_policy: None,
            timeout_ms,
            execution,
            cancel_grace_ms,
        }
    }

    fn stored_object(options: &TaskOptions) -> serde_json::Map<String, serde_json::Value> {
        let text = options.stored_options_text();
        match serde_json::from_str(&text).expect("the stored options are JSON") {
            serde_json::Value::Object(object) => object,
            other => panic!("the stored options are an object, got {other}"),
        }
    }

    /// #826: the class and the grace of a task reach its stored options,
    /// and enqueue and dispatch read them back: one fact for the stored text
    /// and the worker manifest.
    #[test]
    fn a_shared_async_task_stores_its_class_and_its_grace() {
        use crate::core::task::execution::stored_execution;
        use crate::worker::retry::{read_stored_options, StoredCancelGrace, StoredOptions};

        let grace = CancelGraceMs::new(500).expect("500 ms is a grace");
        let options = options_of(Execution::SharedAsync, Some(grace), Some(3_000));
        let stored = stored_object(&options);
        assert_eq!(stored["execution"], "SHARED_ASYNC");
        assert_eq!(stored["cancel_grace_ms"], 500);
        assert_eq!(stored["timeout_ms"], 3_000);
        assert_eq!(stored_execution(Some(&stored)), Ok(Execution::SharedAsync));

        let text = options.stored_options_text();
        match read_stored_options(Some(&text)) {
            StoredOptions::Readable {
                cancel_grace: StoredCancelGrace::Grace(grace),
                ..
            } => assert_eq!(grace, std::time::Duration::from_millis(500)),
            other => panic!("the dispatch reader refused the stored options: {other:?}"),
        }
    }

    /// A shared thread task stores its class with no grace; a class with no
    /// policy and no timeout still stores its options.
    #[test]
    fn a_shared_class_stores_its_options_with_no_policy() {
        use crate::core::task::execution::stored_execution;

        let thread = stored_object(&options_of(Execution::SharedThread, None, None));
        assert_eq!(thread["execution"], "SHARED_THREAD");
        assert_eq!(thread["cancel_grace_ms"], serde_json::Value::Null);
        assert_eq!(stored_execution(Some(&thread)), Ok(Execution::SharedThread));
    }

    /// The text holds what the options say; enqueue refuses a grace on
    /// `SHARED_THREAD` and a `SHARED_ASYNC` task with no grace.
    #[test]
    fn enqueue_refuses_a_grace_that_does_not_fit_the_class() {
        use crate::core::task::execution::stored_execution;

        let grace = CancelGraceMs::DEFAULT;
        let on_thread = stored_object(&options_of(Execution::SharedThread, Some(grace), None));
        assert!(stored_execution(Some(&on_thread)).is_err());
        let without = stored_object(&options_of(Execution::SharedAsync, None, Some(1_000)));
        assert!(stored_execution(Some(&without)).is_err());
    }

    /// An `ISOLATED` task stores its grace, and enqueue and dispatch read it
    /// back: the worker kills the child that long after an operator cancel.
    #[test]
    fn an_isolated_task_stores_its_grace() {
        use crate::core::task::execution::stored_execution;
        use crate::worker::retry::{read_stored_options, StoredCancelGrace, StoredOptions};

        let grace = CancelGraceMs::new(2_000).expect("2000 ms is a grace");
        let options = options_of(Execution::Isolated, Some(grace), Some(30_000));
        let stored = stored_object(&options);
        assert_eq!(stored["execution"], "ISOLATED");
        assert_eq!(stored["cancel_grace_ms"], 2_000);
        assert_eq!(stored_execution(Some(&stored)), Ok(Execution::Isolated));

        let text = options.stored_options_text();
        match read_stored_options(Some(&text)) {
            StoredOptions::Readable {
                cancel_grace: StoredCancelGrace::Grace(grace),
                ..
            } => assert_eq!(grace, std::time::Duration::from_millis(2_000)),
            other => panic!("the dispatch reader refused the stored options: {other:?}"),
        }
    }

    #[test]
    fn a_cancel_grace_is_from_100_to_10000_ms() {
        assert_eq!(CancelGraceMs::new(100).map(CancelGraceMs::get), Ok(100));
        assert_eq!(
            CancelGraceMs::new(10_000).map(CancelGraceMs::get),
            Ok(10_000)
        );
        assert_eq!(
            CancelGraceMs::new(99),
            Err(CancelGraceOutOfRange { value: 99 })
        );
        assert_eq!(
            CancelGraceMs::new(10_001),
            Err(CancelGraceOutOfRange { value: 10_001 })
        );
        assert_eq!(CancelGraceMs::DEFAULT.get(), 1_000);
    }

    /// serde reads the grace through its bounds, and options with no class
    /// read as `ISOLATED`.
    #[test]
    fn serde_reads_the_class_and_the_grace_through_their_rules() {
        let isolated: TaskOptions =
            serde_json::from_str(r#"{"task_name": "t"}"#).expect("the class is optional");
        assert_eq!(isolated.execution, Execution::Isolated);
        assert_eq!(isolated.cancel_grace_ms, None);

        let shared: TaskOptions = serde_json::from_str(
            r#"{"task_name": "t", "execution": "SHARED_ASYNC", "cancel_grace_ms": 250}"#,
        )
        .expect("a grace inside its bounds");
        assert_eq!(shared.execution, Execution::SharedAsync);
        assert_eq!(shared.cancel_grace_ms.map(CancelGraceMs::get), Some(250));

        assert!(serde_json::from_str::<TaskOptions>(
            r#"{"task_name": "t", "execution": "SHARED_ASYNC", "cancel_grace_ms": 50}"#,
        )
        .is_err());
    }

    /// Python stores the text for every task: an isolated task with no
    /// options stores the nulls (`serialize_task_options`).
    #[test]
    fn an_isolated_task_with_no_options_stores_the_text_with_nulls() {
        let opts = TaskOptions {
            task_name: "my_task".to_owned(),
            queue_name: Some("high".to_owned()),
            good_until: None,
            auto_retry_for: None,
            retry_policy: None,
            timeout_ms: None,
            execution: Execution::Isolated,
            cancel_grace_ms: None,
        };
        assert_eq!(
            opts.stored_options_text(),
            r#"{"retry_policy":null,"good_until":null,"timeout_ms":null,"execution":"ISOLATED","cancel_grace_ms":null}"#
        );
    }

    #[test]
    fn stored_options_text_emits_timeout_ms_alone() {
        // timeout_ms must reach the wire even with no retry options set — the
        // bug horsies PR #102 fixed (serializer dropped non-retry fields).
        let opts = TaskOptions {
            task_name: "my_task".to_owned(),
            queue_name: None,
            good_until: None,
            auto_retry_for: None,
            retry_policy: None,
            timeout_ms: Some(2000),
            execution: Execution::Isolated,
            cancel_grace_ms: None,
        };
        let json = opts.stored_options_text();
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed["timeout_ms"], 2000);
    }

    // Parity with horsies PR #44: opt-in max retry delay cap.

    #[test]
    fn with_max_delay_seconds_sets_cap() {
        let policy = RetryPolicy::exponential(60, 5, false)
            .unwrap()
            .with_max_delay_seconds(600);
        assert_eq!(policy.max_delay_seconds, Some(600));
        assert!(policy.validate().is_ok());
    }

    #[test]
    fn zero_max_delay_seconds_rejected() {
        let policy = RetryPolicy::exponential(60, 5, false)
            .unwrap()
            .with_max_delay_seconds(0);
        assert!(matches!(
            policy.validate(),
            Err(RetryPolicyError::NonPositiveMaxDelay)
        ));
    }

    #[test]
    fn max_delay_seconds_omitted_when_unset() {
        // An uncapped policy must not serialize the key (matches Python exclude_none).
        let policy = RetryPolicy::exponential(60, 5, false).unwrap();
        let value = serde_json::to_value(&policy).unwrap();
        assert!(value.get("max_delay_seconds").is_none());
    }

    #[test]
    fn max_delay_seconds_serialized_when_set() {
        let policy = RetryPolicy::exponential(60, 5, false)
            .unwrap()
            .with_max_delay_seconds(600);
        let value = serde_json::to_value(&policy).unwrap();
        assert_eq!(value["max_delay_seconds"], 600);

        // Round-trips through task_options JSON.
        let opts = TaskOptions {
            task_name: "capped".to_owned(),
            queue_name: None,
            good_until: None,
            auto_retry_for: None,
            retry_policy: Some(policy),
            timeout_ms: None,
            execution: Execution::Isolated,
            cancel_grace_ms: None,
        };
        let json = opts.stored_options_text();
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed["retry_policy"]["max_delay_seconds"], 600);
    }
}
