//! The retry rule and the stored run limits of a claimed task.
//!
//! One rule serves the worker and the reaper. Python reference:
//! `horsies/core/utils/retry.py` (the admission and the delay) and
//! `horsies/core/worker/stored_timeout.py` (the stored timeout).
//!
//! The stored `task_options` text is read once, when the task is dispatched,
//! into [`StoredOptions`]. A value that does not fit the rule that wrote it is
//! not replaced by a default: the task fails before user code runs, with
//! `WORKER_SERIALIZATION_ERROR` (see [`StoredOptions::run_permit`]).
//!
//! Layout of the stored options (Python's): a JSON object whose
//! `retry_policy` key holds the policy, with `auto_retry_for` inside it, and
//! whose `timeout_ms` key holds the timeout. Other keys of the object are not
//! read here.
//!
//! Does no I/O and logs nothing.

use std::time::Duration;

use rand::Rng;
use serde::Deserialize;

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

/// The upper bound of a retry delay.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DelayCap {
    Uncapped,
    /// At most this many seconds, after backoff and jitter.
    AtMost(u32),
}

/// A stored retry policy that fits the rule that wrote it.
///
/// Constructed only by [`read_stored_options`]: `max_retries` from 1 to
/// [`MAX_RETRY_ATTEMPTS`], 1 to [`MAX_RETRY_ATTEMPTS`] intervals of 1 to
/// [`MAX_RETRY_INTERVAL_SECONDS`] seconds (exactly one for exponential
/// backoff, one per retry for fixed backoff) and at least one code in
/// `auto_retry_for`.
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

/// The wire form of a stored retry policy (Python `RetryPolicy`, which
/// forbids extra keys).
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct WireRetryPolicy {
    max_retries: u32,
    intervals: Vec<u32>,
    backoff_strategy: WireBackoff,
    jitter: bool,
    #[serde(default)]
    max_delay_seconds: Option<u32>,
    auto_retry_for: Vec<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
enum WireBackoff {
    Fixed,
    Exponential,
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
    match serde_json::from_str::<serde_json::Value>(text) {
        Err(error) => StoredOptions::Unreadable {
            reason: format!("stored task_options do not parse: {error}"),
        },
        Ok(serde_json::Value::Object(options)) => StoredOptions::Readable {
            timeout: read_timeout(options.get("timeout_ms")),
            cancel_grace: read_cancel_grace(options.get("cancel_grace_ms")),
            retry: read_retry_policy(options.get("retry_policy")),
        },
        Ok(other) => StoredOptions::Unreadable {
            reason: format!(
                "stored task_options are a JSON {}, not an object",
                json_kind(&other)
            ),
        },
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

/// Check the stored `retry_policy` against the rule that wrote it (Python
/// `RetryPolicy` and its validators).
fn read_retry_policy(raw: Option<&serde_json::Value>) -> StoredRetry {
    let value = match raw {
        None | Some(serde_json::Value::Null) => return StoredRetry::NoPolicy,
        Some(value) => value,
    };
    let wire = match WireRetryPolicy::deserialize(value) {
        Ok(wire) => wire,
        Err(error) => {
            return StoredRetry::Invalid {
                reason: format!("stored retry_policy does not fit the policy rule: {error}"),
            }
        }
    };
    match retry_rule_from_wire(wire) {
        Ok(rule) => StoredRetry::Policy(rule),
        Err(reason) => StoredRetry::Invalid { reason },
    }
}

fn retry_rule_from_wire(wire: WireRetryPolicy) -> Result<RetryRule, String> {
    if wire.max_retries == 0 || wire.max_retries > MAX_RETRY_ATTEMPTS {
        return Err(format!(
            "stored retry_policy.max_retries is outside 1..={MAX_RETRY_ATTEMPTS}"
        ));
    }
    if wire.intervals.is_empty() || wire.intervals.len() > MAX_RETRY_ATTEMPTS as usize {
        return Err(format!(
            "stored retry_policy.intervals must hold 1 to {MAX_RETRY_ATTEMPTS} entries"
        ));
    }
    if wire
        .intervals
        .iter()
        .any(|interval| *interval == 0 || *interval > MAX_RETRY_INTERVAL_SECONDS)
    {
        return Err(format!(
            "stored retry_policy.intervals must each be 1..={MAX_RETRY_INTERVAL_SECONDS} seconds"
        ));
    }
    let backoff = match wire.backoff_strategy {
        WireBackoff::Fixed if wire.intervals.len() != wire.max_retries as usize => {
            return Err(
                "stored fixed retry_policy has a different number of intervals and retries"
                    .to_owned(),
            )
        }
        WireBackoff::Fixed => Backoff::Fixed,
        WireBackoff::Exponential if wire.intervals.len() != 1 => {
            return Err("stored exponential retry_policy has more than one interval".to_owned())
        }
        WireBackoff::Exponential => Backoff::Exponential,
    };
    let delay_cap = match wire.max_delay_seconds {
        None => DelayCap::Uncapped,
        Some(0) => return Err("stored retry_policy.max_delay_seconds is not positive".to_owned()),
        Some(seconds) => DelayCap::AtMost(seconds),
    };
    if wire.auto_retry_for.is_empty() || wire.auto_retry_for.iter().any(String::is_empty) {
        return Err(
            "stored retry_policy.auto_retry_for must name at least one non-empty code".to_owned(),
        );
    }
    Ok(RetryRule {
        max_retries: wire.max_retries,
        intervals: wire.intervals,
        backoff,
        jitter: wire.jitter,
        delay_cap,
        auto_retry_for: wire.auto_retry_for,
    })
}

/// Why task options give no `max_retries` for a row. The options are
/// refused, not read as a guessed limit.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum RetryLimitRefused {
    /// The text does not parse, or is not a JSON object.
    #[error("{reason}")]
    UnreadableOptions { reason: String },
    /// `retry_policy` does not fit the policy rule.
    #[error("{reason}")]
    InvalidPolicy { reason: String },
}

/// The `max_retries` column of a row whose `task_options` are this text
/// (Python `stored_max_retries`, `horsies/core/codec/task_options.py:80-103`).
///
/// - No options, an empty text, no `retry_policy` key or a `null` policy: 0.
/// - A policy that fits the rule of [`read_stored_options`]: its
///   `max_retries`, 1 to [`MAX_RETRY_ATTEMPTS`].
/// - Otherwise [`RetryLimitRefused`]: the text does not parse, is not an
///   object, or holds a policy that does not fit the rule (a missing,
///   non-integer or out-of-range `max_retries` included).
///
/// One reader serves the task row of a send and the node and task rows of a
/// workflow, so the worker never meets a stored policy that its dispatch
/// refuses. Other keys of the options are not judged here. Pure.
pub fn stored_max_retries(task_options: Option<&str>) -> Result<i32, RetryLimitRefused> {
    match read_stored_options(task_options) {
        StoredOptions::Unreadable { reason } => {
            Err(RetryLimitRefused::UnreadableOptions { reason })
        }
        StoredOptions::Readable {
            retry: StoredRetry::NoPolicy,
            ..
        } => Ok(0),
        StoredOptions::Readable {
            retry: StoredRetry::Invalid { reason },
            ..
        } => Err(RetryLimitRefused::InvalidPolicy { reason }),
        StoredOptions::Readable {
            retry: StoredRetry::Policy(rule),
            ..
        } => Ok(i32::try_from(rule.max_retries())
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
            DelayCap::Uncapped => jittered,
            DelayCap::AtMost(seconds) => jittered.min(f64::from(seconds)),
        }
    }
}

#[cfg(test)]
mod tests;
