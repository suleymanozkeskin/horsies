//! Argument classes of the operations and their conversion at the boundary.
//!
//! A choice with several states (when a task starts, how long its record is
//! kept) is one Python class per state, so a call cannot pass two states at
//! once. A value that the Python client should have refused first raises
//! `TypeError` or `ValueError` here: it is a defect of the client.

use chrono::{DateTime, Utc};
use horsies_core::ThresholdMs;
use pyo3::exceptions::{PyTypeError, PyValueError};
use pyo3::prelude::*;

/// Start the task as soon as a worker can claim it.
#[pyclass(module = "horsies._core", frozen)]
pub struct StartNow {}

#[pymethods]
impl StartNow {
    #[new]
    fn new() -> Self {
        Self {}
    }
}

/// Start the task `seconds` after the enqueue.
#[pyclass(module = "horsies._core", frozen, get_all)]
pub struct StartAfterDelay {
    pub seconds: i64,
}

#[pymethods]
impl StartAfterDelay {
    #[new]
    fn new(seconds: i64) -> Self {
        Self { seconds }
    }
}

/// Start the task at an instant.
#[pyclass(module = "horsies._core", frozen, get_all)]
pub struct StartAt {
    pub at: DateTime<Utc>,
}

#[pymethods]
impl StartAt {
    #[new]
    fn new(at: &Bound<'_, PyAny>) -> PyResult<Self> {
        Ok(Self {
            at: utc_instant(at)?,
        })
    }
}

/// When a task starts.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StartRule {
    Now,
    AfterDelay { seconds: i64 },
    At(DateTime<Utc>),
}

/// The start argument of a call: one of the three start classes.
#[derive(FromPyObject)]
pub enum StartArgument<'py> {
    Now(Bound<'py, StartNow>),
    AfterDelay(Bound<'py, StartAfterDelay>),
    At(Bound<'py, StartAt>),
}

impl StartArgument<'_> {
    pub fn rule(self) -> StartRule {
        match self {
            Self::Now(_) => StartRule::Now,
            Self::AfterDelay(delay) => StartRule::AfterDelay {
                seconds: delay.get().seconds,
            },
            Self::At(at) => StartRule::At(at.get().at),
        }
    }
}

/// Keep the history record for the default period.
#[pyclass(module = "horsies._core", frozen)]
pub struct RetentionDefault {}

#[pymethods]
impl RetentionDefault {
    #[new]
    fn new() -> Self {
        Self {}
    }
}

/// Keep the history record forever.
#[pyclass(module = "horsies._core", frozen)]
pub struct RetentionForever {}

#[pymethods]
impl RetentionForever {
    #[new]
    fn new() -> Self {
        Self {}
    }
}

/// Keep the history record for a period of whole days. The core checks the
/// period (1 to 1825 days) at the enqueue.
#[pyclass(module = "horsies._core", frozen, get_all)]
pub struct RetentionPeriod {
    pub days: u32,
}

#[pymethods]
impl RetentionPeriod {
    #[new]
    fn new(days: u32) -> Self {
        Self { days }
    }
}

/// How long the history record of a task is kept.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RetentionRequest {
    Default,
    Forever,
    PeriodDays(u32),
}

/// The retention argument of a call: one of the three retention classes.
#[derive(FromPyObject)]
pub enum RetentionArgument<'py> {
    Default(Bound<'py, RetentionDefault>),
    Forever(Bound<'py, RetentionForever>),
    Period(Bound<'py, RetentionPeriod>),
}

impl RetentionArgument<'_> {
    pub fn request(self) -> RetentionRequest {
        match self {
            Self::Default(_) => RetentionRequest::Default,
            Self::Forever(_) => RetentionRequest::Forever,
            Self::Period(period) => RetentionRequest::PeriodDays(period.get().days),
        }
    }
}

/// Whether the enqueue keeps the rerun input.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RerunInputPolicy {
    /// The manifest's `retain_rerun_input_default` decides.
    AppDefault,
    Keep,
    Drop,
}

impl RerunInputPolicy {
    /// Parse `APP_DEFAULT`, `KEEP` or `DROP`.
    pub fn parse(text: &str) -> PyResult<Self> {
        match text {
            "APP_DEFAULT" => Ok(Self::AppDefault),
            "KEEP" => Ok(Self::Keep),
            "DROP" => Ok(Self::Drop),
            other => Err(PyValueError::new_err(format!(
                "rerun_input must be 'APP_DEFAULT', 'KEEP' or 'DROP', not {other:?}"
            ))),
        }
    }

    /// The broker argument: `None` lets the manifest default decide.
    pub const fn keep(self) -> Option<bool> {
        match self {
            Self::AppDefault => None,
            Self::Keep => Some(true),
            Self::Drop => Some(false),
        }
    }
}

/// An aware Python `datetime` as a UTC instant. A naive `datetime` is
/// refused: its zone is unknown.
pub fn utc_instant(value: &Bound<'_, PyAny>) -> PyResult<DateTime<Utc>> {
    if value.getattr("tzinfo")?.is_none() {
        return Err(PyTypeError::new_err(
            "a naive datetime is refused; pass an aware datetime",
        ));
    }
    let utc = Utc.into_pyobject(value.py())?;
    value.call_method1("astimezone", (utc,))?.extract()
}

/// A threshold of the recovery functions in whole milliseconds, 0 to
/// 2 147 483 647, or the refusal as text that names the threshold. Pure.
pub fn threshold_ms(name: &str, value_ms: i64) -> Result<ThresholdMs, String> {
    let millis = u64::try_from(value_ms)
        .map_err(|_| format!("the {name} of {value_ms} ms is negative; it must be 0 ms or more"))?;
    ThresholdMs::from_millis(millis).map_err(|refused| format!("the {name}: {refused}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_rerun_input_policy_maps_to_the_broker_argument() {
        assert_eq!(RerunInputPolicy::AppDefault.keep(), None);
        assert_eq!(RerunInputPolicy::Keep.keep(), Some(true));
        assert_eq!(RerunInputPolicy::Drop.keep(), Some(false));
    }
}
