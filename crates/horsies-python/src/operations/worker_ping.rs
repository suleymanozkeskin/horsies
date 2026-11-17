//! The worker ping through the core: `PostgresBroker::ping_workers`, on the
//! process listener of the handle's broker.
//!
//! The outcome is `WorkerPongsRecord` or `BrokerFailure` with
//! `INVALID_INPUT` (a refused argument; nothing ran), `LISTENER_DISABLED`
//! (the broker has no listener: an `ASSUME_INITIALIZED` handle; nothing ran)
//! or `WORKER_PING_FAILED`. The ping sends one NOTIFY and writes no row.

use std::time::Duration;

use horsies_core::{
    ping_min_responses_refused, ping_timeout_refused, BrokerErrorCode, BrokerOperationError,
    WorkerPong,
};
use pyo3::prelude::*;
use pyo3::types::{PyInt, PyList, PyString};
use pyo3::IntoPyObjectExt;

use crate::handle::BrokerConnection;
use crate::records::BrokerFailure;

/// One worker's reply to a ping, with the measured round trip.
#[pyclass(module = "horsies._core", frozen, get_all)]
#[derive(Clone, Debug)]
pub struct WorkerPongRecord {
    pub worker_id: String,
    pub hostname: String,
    pub pid: i32,
    pub round_trip_ms: f64,
}

/// The distinct workers that replied within the window, in reply order.
#[pyclass(module = "horsies._core", frozen, get_all)]
#[derive(Debug)]
pub struct WorkerPongsRecord {
    /// One `WorkerPongRecord` per worker.
    pub pongs: Py<PyList>,
}

/// A ping whose arguments passed Python's checks.
#[derive(Clone, Debug, PartialEq)]
pub struct PingRequest {
    pub target_worker_id: Option<String>,
    pub window: Duration,
    pub min_responses: Option<usize>,
}

/// The outcome of one ping.
pub enum PingOutcome {
    Replied(Vec<WorkerPong>),
    Failed(BrokerFailure),
}

impl<'py> IntoPyObject<'py> for PingOutcome {
    type Target = PyAny;
    type Output = Bound<'py, PyAny>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        match self {
            Self::Replied(pongs) => {
                let records = pongs
                    .into_iter()
                    .map(|pong| {
                        WorkerPongRecord {
                            worker_id: pong.worker_id,
                            hostname: pong.hostname,
                            pid: pong.pid,
                            round_trip_ms: pong.round_trip_ms,
                        }
                        .into_bound_py_any(py)
                    })
                    .collect::<PyResult<Vec<_>>>()?;
                WorkerPongsRecord {
                    pongs: PyList::new(py, records)?.unbind(),
                }
                .into_bound_py_any(py)
            }
            Self::Failed(failed) => failed.into_bound_py_any(py),
        }
    }
}

/// The text of a Python value, as an f-string writes it.
fn shown(value: &Bound<'_, PyAny>) -> PyResult<String> {
    Ok(value.str()?.to_string())
}

/// The reply window of a Python `timeout_seconds`, with Python's answer for
/// each value (`ping_workers_async`):
/// - 0 or less, by Python's comparison: the core's refusal with Python's
///   text. NaN, which Python's comparison lets through, is refused with the
///   same text: Python's window for NaN ended at once, with no reply.
/// - a value that does not convert to a float: `WORKER_PING_FAILED`, not
///   retryable, as Python's deadline sum failed.
/// - infinity, or seconds beyond the range of a `Duration`: the longest
///   window, as Python waited with no deadline.
///
/// A value that Python cannot compare with 0 raises its `TypeError`. Reads
/// the value only.
fn ping_window(value: &Bound<'_, PyAny>) -> PyResult<Result<Duration, BrokerFailure>> {
    if value.le(0)? {
        return Ok(Err(BrokerFailure::from(ping_timeout_refused(&shown(
            value,
        )?))));
    }
    let seconds = match value.extract::<f64>() {
        Ok(seconds) => seconds,
        Err(error) => {
            return Ok(Err(BrokerFailure {
                code: BrokerErrorCode::WorkerPingFailed.to_string(),
                message: format!("ping_workers failed: {error}"),
                retryable: false,
            }))
        }
    };
    if seconds.is_nan() {
        return Ok(Err(BrokerFailure::from(ping_timeout_refused(&shown(
            value,
        )?))));
    }
    Ok(Ok(
        Duration::try_from_secs_f64(seconds).unwrap_or(Duration::MAX)
    ))
}

/// The `min_responses` of a Python value, with Python's answer for each
/// value: `None` waits the full window; below 1, by Python's comparison, is
/// the core's refusal with Python's text; an `int` beyond the range of a
/// count waits for more replies than any fleet sends, so the full window.
/// A value that Python cannot compare with 1 raises its `TypeError`; one
/// that is not an `int` raises `TypeError` (the public type is
/// `int | None`). Reads the value only.
fn min_responses(value: &Bound<'_, PyAny>) -> PyResult<Result<Option<usize>, BrokerFailure>> {
    if value.is_none() {
        return Ok(Ok(None));
    }
    if value.lt(1)? {
        return Ok(Err(BrokerFailure::from(ping_min_responses_refused(
            &shown(value)?,
        ))));
    }
    match value.extract::<usize>() {
        Ok(count) => Ok(Ok(Some(count))),
        Err(error) => match value.is_instance_of::<PyInt>() {
            true => Ok(Ok(Some(usize::MAX))),
            false => Err(error),
        },
    }
}

/// The target of a Python value: `None` or a `str`. Another value is
/// refused as Python's request model refused it: `INVALID_INPUT`. Reads
/// the value only.
fn ping_target(value: &Bound<'_, PyAny>) -> PyResult<Result<Option<String>, BrokerFailure>> {
    if value.is_none() {
        return Ok(Ok(None));
    }
    match value.is_instance_of::<PyString>() {
        true => Ok(Ok(Some(value.extract::<String>()?))),
        false => {
            let type_name = value.get_type().name()?.to_string();
            Ok(Err(BrokerFailure::from(BrokerOperationError {
                code: BrokerErrorCode::InvalidInput,
                message: format!(
                    "ping_workers request refused: target_worker_id must be a str or None, \
                     not {type_name}"
                ),
                retryable: false,
            })))
        }
    }
}

/// The ping of three Python arguments, checked in Python's order: the
/// window, the reply count, the target. A refused argument is the failure
/// to answer at once; nothing runs.
pub fn ping_request(
    target_worker_id: &Bound<'_, PyAny>,
    timeout_seconds: &Bound<'_, PyAny>,
    min_responses_value: &Bound<'_, PyAny>,
) -> PyResult<Result<PingRequest, BrokerFailure>> {
    let window = match ping_window(timeout_seconds)? {
        Ok(window) => window,
        Err(refused) => return Ok(Err(refused)),
    };
    let min_responses = match min_responses(min_responses_value)? {
        Ok(count) => count,
        Err(refused) => return Ok(Err(refused)),
    };
    let target_worker_id = match ping_target(target_worker_id)? {
        Ok(target) => target,
        Err(refused) => return Ok(Err(refused)),
    };
    Ok(Ok(PingRequest {
        target_worker_id,
        window,
        min_responses,
    }))
}

/// Ping the workers: one NOTIFY on the ping channel, then the replies on
/// the reply channel of the handle's process listener until the window
/// ends or a stop condition holds.
pub async fn ping_workers_operation(
    connection: BrokerConnection,
    request: PingRequest,
) -> PingOutcome {
    let broker = match connection.broker().await {
        Ok(broker) => broker,
        Err(error) => {
            return PingOutcome::Failed(BrokerFailure {
                code: BrokerErrorCode::WorkerPingFailed.to_string(),
                message: format!("ping_workers failed: {error}"),
                retryable: error.is_retryable(),
            })
        }
    };
    match broker
        .ping_workers(
            request.target_worker_id.as_deref(),
            request.window,
            request.min_responses,
        )
        .await
    {
        Ok(pongs) => PingOutcome::Replied(pongs),
        Err(error) => PingOutcome::Failed(BrokerFailure::from(error)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request_of(
        target: &str,
        timeout: &str,
        minimum: &str,
    ) -> Result<PingRequest, BrokerFailure> {
        Python::initialize();
        Python::attach(|py| {
            let value = |source: &str| {
                py.eval(&std::ffi::CString::new(source).expect("no NUL"), None, None)
                    .expect("the expression evaluates")
            };
            ping_request(&value(target), &value(timeout), &value(minimum)).expect("the values read")
        })
    }

    fn refusal(target: &str, timeout: &str, minimum: &str) -> (String, String) {
        match request_of(target, timeout, minimum) {
            Ok(request) => panic!("{timeout}, {minimum} read as {request:?}"),
            Err(failed) => (failed.code, failed.message),
        }
    }

    #[test]
    fn a_valid_ping_keeps_its_window_count_and_target() {
        assert_eq!(
            request_of("'w1'", "2.5", "None"),
            Ok(PingRequest {
                target_worker_id: Some("w1".to_owned()),
                window: Duration::from_millis(2_500),
                min_responses: None,
            })
        );
        assert_eq!(
            request_of("None", "2", "1").map(|request| request.min_responses),
            Ok(Some(1))
        );
    }

    #[test]
    fn a_window_of_zero_or_less_or_nan_is_refused_with_python_text() {
        for (timeout, shown) in [
            ("0", "0"),
            ("0.0", "0.0"),
            ("-1.5", "-1.5"),
            ("float('nan')", "nan"),
        ] {
            assert_eq!(
                refusal("None", timeout, "None"),
                (
                    "INVALID_INPUT".to_owned(),
                    format!("timeout_seconds must be positive, got {shown}")
                )
            );
        }
    }

    #[test]
    fn an_endless_window_is_the_longest_window() {
        for timeout in ["float('inf')", "1e300"] {
            assert_eq!(
                request_of("None", timeout, "None").map(|request| request.window),
                Ok(Duration::MAX)
            );
        }
    }

    #[test]
    fn a_count_below_one_is_refused_and_a_huge_count_waits_the_window() {
        for (minimum, shown) in [
            ("0", "0"),
            ("-3", "-3"),
            ("False", "False"),
            ("-(2 ** 70)", ""),
        ] {
            let (code, message) = refusal("None", "1", minimum);
            assert_eq!(code, "INVALID_INPUT");
            assert!(
                message.starts_with(&format!("min_responses must be >= 1 when set, got {shown}")),
                "{message}"
            );
        }
        assert_eq!(
            request_of("None", "1", "2 ** 70").map(|request| request.min_responses),
            Ok(Some(usize::MAX))
        );
    }

    #[test]
    fn a_target_that_is_not_text_is_refused() {
        let (code, message) = refusal("7", "1", "None");
        assert_eq!(code, "INVALID_INPUT");
        assert!(
            message.starts_with("ping_workers request refused: "),
            "{message}"
        );
    }

    #[test]
    fn a_window_that_is_not_a_float_is_a_ping_failure() {
        let (code, message) = refusal("None", "10 ** 400", "None");
        assert_eq!(code, "WORKER_PING_FAILED");
        assert!(message.starts_with("ping_workers failed: "), "{message}");
    }
}
