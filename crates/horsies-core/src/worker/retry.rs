//! The retry rule and the stored run limits of a claimed task.
//!
//! One rule serves the worker and the reaper. Python reference:
//! `python/horsies/core/utils/retry.py` (the admission and the delay) and
//! `python/horsies/core/worker/stored_timeout.py` (the stored timeout).
//!
//! The stored `task_options` text is read once, when the task is dispatched,
//! into [`StoredOptions`]. A value that does not fit the rule that wrote it is
//! not replaced by a default: the task fails before user code runs, with
//! `WORKER_SERIALIZATION_ERROR` (see [`StoredOptions::run_permit`]). The
//! `retry_policy` is read by Python's `RetryPolicy` rule, with Python's
//! refusal text (`stored_policy`).
//!
//! Layout of the stored options (Python's): a JSON object whose
//! `retry_policy` key holds the policy, with `auto_retry_for` inside it, and
//! whose `timeout_ms` key holds the timeout. Other keys of the object are not
//! read here.
//!
//! Does no I/O and logs nothing.

use std::time::Duration;

use rand::Rng;

mod stored_policy;

use stored_policy::PolicyRead;

/// The smallest stored `timeout_ms` (Python `MIN_TASK_TIMEOUT_MS`).
pub const MIN_TASK_TIMEOUT_MS: u64 = 1_000;
/// The largest stored `timeout_ms`, about 49.7 days (Python `MAX_TASK_TIMEOUT_MS`).
pub const MAX_TASK_TIMEOUT_MS: u64 = 4_294_967_295;
/// The smallest stored `cancel_grace_ms` (Python `MIN_CANCEL_GRACE_MS`).
pub const MIN_CANCEL_GRACE_MS: u64 = 100;
/// The largest stored `cancel_grace_ms` (Python `MAX_CANCEL_GRACE_MS`).
pub const MAX_CANCEL_GRACE_MS: u64 = 10_000;
/// The most retries one task can have (Python `MAX_RETRY_ATTEMPTS`).
pub const MAX_RETRY_ATTEMPTS: u32 = 20;
/// The largest retry interval in seconds (Python `RetryPolicy.intervals`).
pub const MAX_RETRY_INTERVAL_SECONDS: u32 = 86_400;
/// The smallest retry delay in seconds; jitter is added above it.
const MIN_RETRY_DELAY_SECONDS: f64 = 1.0;
/// The jitter adds up to this fraction of the base delay.
const JITTER_FRACTION: f64 = 0.25;
/// The start of every refusal text of a stored retry policy (Python
/// `read_claimed_options`).
const RETRY_POLICY_REFUSAL_PREFIX: &str = "stored retry_policy does not validate: ";

// ---------------------------------------------------------------------------
// Stored options
// ---------------------------------------------------------------------------

/// The `task_options` of a claimed row, read once at dispatch.
#[derive(Debug, Clone, PartialEq)]
pub enum StoredOptions {
    /// The row has no options, or the text is a JSON object.
    Readable {
        timeout: StoredTimeout,
        cancel_grace: StoredCancelGrace,
        retry: StoredRetry,
    },
    /// The text does not parse, or is not a JSON object. The retry policy is
    /// in the options, so such a task cannot retry.
    Unreadable { reason: String },
}

/// The timeout a readable row stores.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StoredTimeout {
    /// No `timeout_ms` key, or `null`.
    NoTimeout,
    /// A `timeout_ms` from [`MIN_TASK_TIMEOUT_MS`] to [`MAX_TASK_TIMEOUT_MS`].
    Limit(Duration),
    /// A `timeout_ms` that is not an int inside the bounds.
    Invalid(TimeoutRefusal),
}

/// Why a stored `timeout_ms` cannot be used.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TimeoutRefusal {
    NotAnInteger,
    BelowMinimum,
    AboveMaximum,
}

/// The cancel grace a readable row stores. Only a `SHARED_ASYNC` row needs
/// one; the dispatch route refuses such a row without a usable grace.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StoredCancelGrace {
    /// No `cancel_grace_ms` key, or `null`.
    NotStored,
    /// A `cancel_grace_ms` from [`MIN_CANCEL_GRACE_MS`] to
    /// [`MAX_CANCEL_GRACE_MS`].
    Grace(Duration),
    /// A `cancel_grace_ms` that is not an int inside the bounds.
    Invalid(GraceRefusal),
}

/// Why a stored `cancel_grace_ms` cannot be used (Python
/// `shared_async_grace_error`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GraceRefusal {
    /// The value is a JSON value of this Python type, not an int.
    NotAnInteger {
        found: &'static str,
    },
    BelowMinimum,
    AboveMaximum,
}

/// The retry policy a readable row stores.
#[derive(Debug, Clone, PartialEq)]
pub enum StoredRetry {
    /// No `retry_policy` key, or `null`.
    NoPolicy,
    /// A policy that fits the rule that wrote it.
    Policy(RetryRule),
    /// A `retry_policy` value that does not fit that rule.
    Invalid { reason: String },
}

/// The backoff of a retry rule.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Backoff {
    /// Attempt `n` waits `intervals[min(n, len) - 1]`.
    Fixed,
    /// Attempt `n` waits `intervals[0] * 2^(n - 1)`.
    Exponential,
}

/// The upper bound of a retry delay. Python's `max_delay_seconds` is a
/// `PositiveInt` with no upper bound.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DelayCap {
    Uncapped,
    /// At most this many seconds, after backoff and jitter.
    AtMost(u64),
    /// More than `u64::MAX` seconds. The largest delay of a rule is
    /// 86,400 × 2^19 × 1.25 s (about 5.7 × 10^10 s: retry 20 of an
    /// exponential rule with the largest interval and full jitter), so this
    /// cap shortens no delay.
    AboveU64Seconds,
}

/// A stored retry policy that fits the rule that wrote it.
///
/// Constructed only by [`read_stored_options`]: `max_retries` from 1 to
/// [`MAX_RETRY_ATTEMPTS`], 1 to [`MAX_RETRY_ATTEMPTS`] intervals of 1 to
/// [`MAX_RETRY_INTERVAL_SECONDS`] seconds (exactly one for exponential
/// backoff, one per retry for fixed backoff) and at least one code in
/// `auto_retry_for`, each an `UPPER_SNAKE_CASE` code text that is not a
/// reserved name and does not look like an exception class name.
#[derive(Debug, Clone, PartialEq)]
pub struct RetryRule {
    max_retries: u32,
    intervals: Vec<u32>,
    backoff: Backoff,
    jitter: bool,
    delay_cap: DelayCap,
    auto_retry_for: Vec<String>,
}

/// Whether the dispatch may run user code, from the stored options.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RunPermit {
    /// Run with no time limit.
    RunWithoutTimeout,
    /// Run, and end the attempt as `TASK_TIMEOUT` after this time.
    RunWithTimeout(Duration),
    /// Fail the task before user code runs, with `WORKER_SERIALIZATION_ERROR`
    /// and this message.
    Refuse(String),
}

/// Read the stored `task_options` of a claimed row.
///
/// `None` and an empty text mean the row has no options, as the enqueue
/// reader treats them.
pub fn read_stored_options(task_options: Option<&str>) -> StoredOptions {
    let text = match task_options {
        None | Some("") => {
            return StoredOptions::Readable {
                timeout: StoredTimeout::NoTimeout,
                cancel_grace: StoredCancelGrace::NotStored,
                retry: StoredRetry::NoPolicy,
            }
        }
        Some(text) => text,
    };
    match options_object(text) {
        Err(reason) => StoredOptions::Unreadable { reason },
        Ok(options) => StoredOptions::Readable {
            timeout: read_timeout(options.get("timeout_ms")),
            cancel_grace: read_cancel_grace(options.get("cancel_grace_ms")),
            // The text is a JSON object: the precondition of the read.
            retry: stored_retry(stored_policy::read_retry_policy(text)),
        },
    }
}

/// A non-empty stored options text as a JSON object, or the reason that it
/// is unreadable: it does not parse, or it is another JSON value.
fn options_object(text: &str) -> Result<serde_json::Map<String, serde_json::Value>, String> {
    match serde_json::from_str::<serde_json::Value>(text) {
        Err(error) => Err(format!("stored task_options do not parse: {error}")),
        Ok(serde_json::Value::Object(options)) => Ok(options),
        Ok(other) => Err(format!(
            "stored task_options are a JSON {}, not an object",
            json_kind(&other)
        )),
    }
}

fn json_kind(value: &serde_json::Value) -> &'static str {
    match value {
        serde_json::Value::Null => "null",
        serde_json::Value::Bool(_) => "bool",
        serde_json::Value::Number(_) => "number",
        serde_json::Value::String(_) => "string",
        serde_json::Value::Array(_) => "array",
        serde_json::Value::Object(_) => "object",
    }
}

/// Check the stored `timeout_ms` against the rule that wrote it (Python
/// `_timeout_value`). A float, a string, a bool or a negative number is
/// not an int.
fn read_timeout(raw: Option<&serde_json::Value>) -> StoredTimeout {
    let number = match raw {
        None | Some(serde_json::Value::Null) => return StoredTimeout::NoTimeout,
        Some(serde_json::Value::Number(number)) => number,
        Some(_) => return StoredTimeout::Invalid(TimeoutRefusal::NotAnInteger),
    };
    match (number.as_u64(), number.as_i64()) {
        (Some(ms), _) if ms < MIN_TASK_TIMEOUT_MS => {
            StoredTimeout::Invalid(TimeoutRefusal::BelowMinimum)
        }
        (Some(ms), _) if ms > MAX_TASK_TIMEOUT_MS => {
            StoredTimeout::Invalid(TimeoutRefusal::AboveMaximum)
        }
        (Some(ms), _) => StoredTimeout::Limit(Duration::from_millis(ms)),
        (None, Some(_)) => StoredTimeout::Invalid(TimeoutRefusal::BelowMinimum),
        (None, None) => StoredTimeout::Invalid(TimeoutRefusal::NotAnInteger),
    }
}

/// Check the stored `cancel_grace_ms` against the enqueue rule of a
/// `SHARED_ASYNC` row (Python `shared_async_grace_error`). A bool, a float,
/// a string, a list or an object is not an int.
fn read_cancel_grace(raw: Option<&serde_json::Value>) -> StoredCancelGrace {
    let number = match raw {
        None | Some(serde_json::Value::Null) => return StoredCancelGrace::NotStored,
        Some(serde_json::Value::Number(number)) => number,
        Some(other) => {
            return StoredCancelGrace::Invalid(GraceRefusal::NotAnInteger {
                found: python_type_name(other),
            })
        }
    };
    match (number.as_u64(), number.as_i64()) {
        (Some(ms), _) if ms < MIN_CANCEL_GRACE_MS => {
            StoredCancelGrace::Invalid(GraceRefusal::BelowMinimum)
        }
        (Some(ms), _) if ms > MAX_CANCEL_GRACE_MS => {
            StoredCancelGrace::Invalid(GraceRefusal::AboveMaximum)
        }
        (Some(ms), _) => StoredCancelGrace::Grace(Duration::from_millis(ms)),
        (None, Some(_)) => StoredCancelGrace::Invalid(GraceRefusal::BelowMinimum),
        (None, None) => StoredCancelGrace::Invalid(GraceRefusal::NotAnInteger { found: "float" }),
    }
}

/// The Python type name of a parsed JSON value, for refusal texts.
fn python_type_name(value: &serde_json::Value) -> &'static str {
    match value {
        serde_json::Value::Null => "NoneType",
        serde_json::Value::Bool(_) => "bool",
        serde_json::Value::Number(number) => match number.is_f64() {
            true => "float",
            false => "int",
        },
        serde_json::Value::String(_) => "str",
        serde_json::Value::Array(_) => "list",
        serde_json::Value::Object(_) => "dict",
    }
}

/// The text of a cancel grace refusal, as Python words it.
pub fn grace_refusal_message(refusal: GraceRefusal) -> String {
    match refusal {
        GraceRefusal::NotAnInteger { found: "bool" } => {
            "stored cancel_grace_ms is a bool, not an int".to_owned()
        }
        GraceRefusal::NotAnInteger { found } => {
            format!("stored cancel_grace_ms is a JSON {found}, not an int")
        }
        GraceRefusal::BelowMinimum => {
            format!("stored cancel_grace_ms is below {MIN_CANCEL_GRACE_MS} ms")
        }
        GraceRefusal::AboveMaximum => {
            format!("stored cancel_grace_ms is above {MAX_CANCEL_GRACE_MS} ms")
        }
    }
}

/// The retry policy of a read. A refusal is [`RETRY_POLICY_REFUSAL_PREFIX`],
/// then Python's detail (Python `read_claimed_options`).
fn stored_retry(read: PolicyRead) -> StoredRetry {
    match read {
        PolicyRead::Absent => StoredRetry::NoPolicy,
        PolicyRead::Valid(rule) => StoredRetry::Policy(rule),
        PolicyRead::Refused { detail } => StoredRetry::Invalid {
            reason: format!("{RETRY_POLICY_REFUSAL_PREFIX}{detail}"),
        },
    }
}

/// Why task options give no `max_retries` for a row. The options are
/// refused, not read as a guessed limit.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum RetryLimitRefused {
    /// The text does not parse, or is not a JSON object.
    #[error("{reason}")]
    UnreadableOptions { reason: String },
    /// `retry_policy` does not fit the policy rule. `detail` is Python's
    /// text (`InvalidRetryPolicy.detail`), with no prefix.
    #[error("{detail}")]
    InvalidPolicy { detail: String },
}

/// The `max_retries` column of a row whose `task_options` are this text
/// (Python `stored_max_retries`, `python/horsies/core/codec/task_options.py:80-103`).
///
/// - No options, an empty text, no `retry_policy` key or a `null` policy: 0.
/// - A policy that fits the rule of [`read_stored_options`]: its
///   `max_retries`, 1 to [`MAX_RETRY_ATTEMPTS`].
/// - Otherwise [`RetryLimitRefused`]: the text does not parse, is not an
///   object, or holds a policy that does not fit the rule (a null,
///   non-integer or out-of-range `max_retries` included). A key that the
///   policy leaves out takes its `RetryPolicy` field default.
///
/// One reader serves the task row of a send and the node and task rows of a
/// workflow, so the worker never meets a stored policy that its dispatch
/// refuses. Other keys of the options are not judged here. Pure.
pub fn stored_max_retries(task_options: Option<&str>) -> Result<i32, RetryLimitRefused> {
    let text = match task_options {
        None | Some("") => return Ok(0),
        Some(text) => text,
    };
    let _object =
        options_object(text).map_err(|reason| RetryLimitRefused::UnreadableOptions { reason })?;
    // The text is a JSON object: the precondition of the read.
    match stored_policy::read_retry_policy(text) {
        PolicyRead::Absent => Ok(0),
        PolicyRead::Refused { detail } => Err(RetryLimitRefused::InvalidPolicy { detail }),
        PolicyRead::Valid(rule) => Ok(i32::try_from(rule.max_retries())
            .expect("a stored retry policy holds at most MAX_RETRY_ATTEMPTS (20) retries")),
    }
}

impl StoredOptions {
    /// Whether the dispatch may run user code (Python `read_stored_timeout`
    /// and the child's refusal path). Unreadable options, a timeout that does
    /// not fit its rule, and a retry policy that does not fit its rule each
    /// refuse the run.
    pub fn run_permit(&self) -> RunPermit {
        match self {
            Self::Unreadable { reason } => RunPermit::Refuse(reason.clone()),
            Self::Readable {
                retry: StoredRetry::Invalid { reason },
                ..
            } => RunPermit::Refuse(reason.clone()),
            Self::Readable {
                timeout: StoredTimeout::Invalid(refusal),
                ..
            } => RunPermit::Refuse(timeout_refusal_message(*refusal).to_owned()),
            Self::Readable {
                timeout: StoredTimeout::NoTimeout,
                ..
            } => RunPermit::RunWithoutTimeout,
            Self::Readable {
                timeout: StoredTimeout::Limit(limit),
                ..
            } => RunPermit::RunWithTimeout(*limit),
        }
    }

    /// The cancel grace the options hold. Unreadable options hold none; the
    /// route refuses them through [`Self::run_permit`] first.
    pub fn cancel_grace(&self) -> StoredCancelGrace {
        match self {
            Self::Readable { cancel_grace, .. } => *cancel_grace,
            Self::Unreadable { .. } => StoredCancelGrace::NotStored,
        }
    }

    /// The retry policy the options hold, when one fits its rule.
    pub fn retry(&self) -> &StoredRetry {
        match self {
            Self::Readable { retry, .. } => retry,
            Self::Unreadable { .. } => &StoredRetry::NoPolicy,
        }
    }
}

/// The text of a timeout refusal, as Python words it.
pub fn timeout_refusal_message(refusal: TimeoutRefusal) -> &'static str {
    match refusal {
        TimeoutRefusal::NotAnInteger => "stored timeout_ms is not an int",
        TimeoutRefusal::BelowMinimum => "stored timeout_ms is below 1000 ms",
        TimeoutRefusal::AboveMaximum => {
            "stored timeout_ms is above the limit of 4294967295 ms (about 49.7 days)"
        }
    }
}

// ---------------------------------------------------------------------------
// Admission and delay
// ---------------------------------------------------------------------------

/// The answer of [`admit_retry`].
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum RetryAdmission<'a> {
    /// The policy names the code and retries remain.
    Admitted(&'a RetryRule),
    /// `max_retries` is 0, or `retry_count` reached it.
    RetriesSpent,
    /// The options hold no policy that fits its rule.
    NoPolicy,
    /// The failure has no code, or the policy does not name it.
    CodeNotListed,
}

/// The retry policy of a task when it retries `error_code` now (Python
/// `admitted_retry_policy`). Pure. The deadline (`good_until`) is not judged
/// here: the worker's failed-attempt write and the reaper judge it on the
/// database clock.
///
/// `error_code` is the code text: a built-in code is compared by its value
/// (`"WORKER_CRASHED"`), as Python compares `BuiltInTaskCode.value`.
pub fn admit_retry<'a>(
    retry_count: i32,
    max_retries: i32,
    retry: &'a StoredRetry,
    error_code: Option<&str>,
) -> RetryAdmission<'a> {
    if max_retries <= 0 || retry_count >= max_retries {
        return RetryAdmission::RetriesSpent;
    }
    let rule = match retry {
        StoredRetry::Policy(rule) => rule,
        StoredRetry::NoPolicy | StoredRetry::Invalid { .. } => return RetryAdmission::NoPolicy,
    };
    match error_code {
        Some(code) if !code.is_empty() && rule.auto_retry_for.iter().any(|entry| entry == code) => {
            RetryAdmission::Admitted(rule)
        }
        Some(_) | None => RetryAdmission::CodeNotListed,
    }
}

impl RetryRule {
    /// The retries the policy allows: 1 to [`MAX_RETRY_ATTEMPTS`].
    pub fn max_retries(&self) -> u32 {
        self.max_retries
    }

    /// The delay in seconds before retry attempt `retry_attempt` (1-based:
    /// `retry_count + 1`), as Python `calculate_retry_delay` computes it for a
    /// valid policy: the backoff, a floor of 1 s, jitter upward by up to 25 %,
    /// then the cap.
    pub fn delay_seconds(&self, retry_attempt: u32) -> f64 {
        let base = match self.backoff {
            Backoff::Exponential => {
                let exponent = i32::try_from(retry_attempt.saturating_sub(1)).unwrap_or(i32::MAX);
                f64::from(self.intervals[0]) * 2.0_f64.powi(exponent)
            }
            Backoff::Fixed => {
                let index =
                    (retry_attempt.saturating_sub(1) as usize).min(self.intervals.len() - 1);
                f64::from(self.intervals[index])
            }
        };
        let floored = base.max(MIN_RETRY_DELAY_SECONDS);
        let jittered = match self.jitter {
            true => floored + rand::thread_rng().gen_range(0.0..=floored * JITTER_FRACTION),
            false => floored,
        };
        match self.delay_cap {
            DelayCap::Uncapped | DelayCap::AboveU64Seconds => jittered,
            // A cap above 2^53 s rounds to a nearby f64, far above any delay.
            DelayCap::AtMost(seconds) => jittered.min(seconds as f64),
        }
    }
}

#[cfg(test)]
mod tests;
