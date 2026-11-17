//! The terminal-history window of one monitoring request through the core:
//! `horsies_core::monitoring::resolve_monitoring_window`.
//!
//! The outcome is `MonitoringWindowRecord` or `WindowRefusedRecord`. The
//! Python client maps them to `MonitoringWindow` and `WindowRefused` of
//! `horsies.monitoring.history_window`. Pure: no I/O, and it starts no
//! runtime.

use chrono::{DateTime, Utc};
use horsies_core::monitoring::resolve_monitoring_window as core_window;
use pyo3::prelude::*;
use pyo3::IntoPyObjectExt;

use crate::errors::refusal_error;
use crate::operations::arguments::utc_instant;
use crate::process_core::pure_call_refusal;

/// The window of one monitoring request: UTC bounds, increasing, at most the
/// core's maximum apart.
#[pyclass(module = "horsies._core", frozen, get_all)]
#[derive(Clone, Debug)]
pub struct MonitoringWindowRecord {
    pub lower: DateTime<Utc>,
    pub upper: DateTime<Utc>,
}

/// The requested window is not servable; `reason` names why.
#[pyclass(module = "horsies._core", frozen, get_all)]
#[derive(Clone, Debug)]
pub struct WindowRefusedRecord {
    pub reason: String,
}

/// The window, or the refusal.
#[derive(Debug)]
pub enum WindowOutcome {
    Window(MonitoringWindowRecord),
    Refused(WindowRefusedRecord),
}

impl<'py> IntoPyObject<'py> for WindowOutcome {
    type Target = PyAny;
    type Output = Bound<'py, PyAny>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        match self {
            Self::Window(window) => window.into_bound_py_any(py),
            Self::Refused(refused) => refused.into_bound_py_any(py),
        }
    }
}

/// One requested bound: absent, an instant, or a naive `datetime`, whose
/// zone is unknown.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum RequestBound {
    Absent,
    At(DateTime<Utc>),
    Naive,
}

/// The bound of a Python value: `None`, or a `datetime`. A value that is not
/// a `datetime` raises `TypeError`.
fn request_bound(value: Option<&Bound<'_, PyAny>>) -> PyResult<RequestBound> {
    match value {
        None => Ok(RequestBound::Absent),
        Some(value) if value.getattr("tzinfo")?.is_none() => Ok(RequestBound::Naive),
        Some(value) => utc_instant(value).map(RequestBound::At),
    }
}

/// The instant of a bound, or the refusal that names a naive bound.
fn bound_instant(
    name: &str,
    bound: RequestBound,
) -> Result<Option<DateTime<Utc>>, WindowRefusedRecord> {
    match bound {
        RequestBound::Absent => Ok(None),
        RequestBound::At(instant) => Ok(Some(instant)),
        RequestBound::Naive => Err(WindowRefusedRecord {
            reason: format!("{name} must be timezone-aware"),
        }),
    }
}

/// The window of `since` and `until`, anchored at `now`. A naive `since` is
/// refused before a naive `until`, as Python checks them. Pure.
fn window_outcome(
    since: RequestBound,
    until: RequestBound,
    now: Option<DateTime<Utc>>,
) -> WindowOutcome {
    let bounds = bound_instant("since", since)
        .and_then(|since| bound_instant("until", until).map(|until| (since, until)));
    let (since, until) = match bounds {
        Ok(bounds) => bounds,
        Err(refused) => return WindowOutcome::Refused(refused),
    };
    match core_window(since, until, now) {
        Ok(window) => WindowOutcome::Window(MonitoringWindowRecord {
            lower: window.lower(),
            upper: window.upper(),
        }),
        Err(refused) => WindowOutcome::Refused(WindowRefusedRecord {
            reason: refused.reason,
        }),
    }
}

/// Resolve the terminal-history window of one monitoring request:
/// `MonitoringWindowRecord` or `WindowRefusedRecord`. Absent bounds take the
/// default: the last 24 hours ending at `until`, or at `now` (the clock of
/// this process when `None`); a lone `since` runs to `now`. A naive `since`
/// or `until` is refused; a naive `now` raises `TypeError`. Pure: no I/O,
/// and it starts no runtime; the fork and stop rules still apply.
#[pyfunction]
#[pyo3(signature = (*, since, until, now))]
pub fn resolve_monitoring_window(
    since: Option<&Bound<'_, PyAny>>,
    until: Option<&Bound<'_, PyAny>>,
    now: Option<&Bound<'_, PyAny>>,
) -> PyResult<WindowOutcome> {
    if let Some(refusal) = pure_call_refusal() {
        return Err(refusal_error(refusal));
    }
    let since = request_bound(since)?;
    let until = request_bound(until)?;
    let now = now.map(utc_instant).transpose()?;
    Ok(window_outcome(since, until, now))
}

#[cfg(test)]
mod tests {
    use chrono::{Duration, TimeZone};

    use super::*;

    fn now() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 11, 17, 12, 0, 0)
            .single()
            .expect("a valid instant")
    }

    fn reason(outcome: WindowOutcome) -> String {
        match outcome {
            WindowOutcome::Refused(refused) => refused.reason,
            WindowOutcome::Window(window) => panic!("expected a refusal, got {window:?}"),
        }
    }

    fn bounds(outcome: WindowOutcome) -> (DateTime<Utc>, DateTime<Utc>) {
        match outcome {
            WindowOutcome::Window(window) => (window.lower, window.upper),
            WindowOutcome::Refused(refused) => panic!("expected a window: {}", refused.reason),
        }
    }

    /// Python `tests/unit/test_monitoring_history_window.py`: the default,
    /// a lone `since`, a lone `until`.
    #[test]
    fn absent_bounds_take_the_default_span_ending_at_until_or_now() {
        assert_eq!(
            bounds(window_outcome(
                RequestBound::Absent,
                RequestBound::Absent,
                Some(now())
            )),
            (now() - Duration::hours(24), now())
        );
        let since = now() - Duration::hours(3);
        assert_eq!(
            bounds(window_outcome(
                RequestBound::At(since),
                RequestBound::Absent,
                Some(now())
            )),
            (since, now())
        );
        let until = now() - Duration::days(2);
        assert_eq!(
            bounds(window_outcome(
                RequestBound::Absent,
                RequestBound::At(until),
                Some(now())
            )),
            (until - Duration::hours(24), until)
        );
    }

    #[test]
    fn a_naive_since_is_refused_before_a_naive_until() {
        assert_eq!(
            reason(window_outcome(
                RequestBound::Naive,
                RequestBound::Naive,
                Some(now())
            )),
            "since must be timezone-aware"
        );
        assert_eq!(
            reason(window_outcome(
                RequestBound::Absent,
                RequestBound::Naive,
                Some(now())
            )),
            "until must be timezone-aware"
        );
    }

    #[test]
    fn an_empty_or_reversed_window_and_a_window_over_the_maximum_are_refused() {
        assert_eq!(
            reason(window_outcome(
                RequestBound::At(now()),
                RequestBound::At(now()),
                Some(now())
            )),
            "the window must be increasing (since < until)"
        );
        assert_eq!(
            reason(window_outcome(
                RequestBound::At(now() - Duration::days(30) - Duration::microseconds(1)),
                RequestBound::At(now()),
                Some(now())
            )),
            "the window exceeds the 30-day maximum"
        );
        assert_eq!(
            bounds(window_outcome(
                RequestBound::At(now() - Duration::days(30)),
                RequestBound::At(now()),
                Some(now())
            )),
            (now() - Duration::days(30), now())
        );
    }
}
