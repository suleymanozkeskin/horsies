//! The connectivity step of the live check through the core:
//! `PostgresBroker::check_connectivity`, the rule of both clients.
//!
//! The outcome is `ConnectivityPassed` or `ConnectivityRefused` with the
//! step that refused. Nothing is written: the steps read, and the LISTEN
//! probe of PgBouncer transaction mode sends one NOTIFY. No schema step
//! runs.

use horsies_core::ConnectivityRefusal;
use pyo3::prelude::*;
use pyo3::IntoPyObjectExt;

use crate::handle::BrokerConnection;

/// Every step passed: the main URL, the session URL when it differs, and
/// the LISTEN probe in PgBouncer transaction mode.
#[pyclass(module = "horsies._core", frozen)]
#[derive(Clone, Debug)]
pub struct ConnectivityPassed {}

/// The first step that refused, and its cause. No later step ran.
#[pyclass(module = "horsies._core", frozen, get_all)]
#[derive(Clone, Debug)]
pub struct ConnectivityRefused {
    /// `CONNECT` (the handle could not build its broker), `POOL_SETTINGS`,
    /// `MAIN_URL`, `SESSION_URL` or `LISTENER` (the LISTEN probe).
    pub step: String,
    pub message: String,
}

/// The outcome of one connectivity check.
pub enum ConnectivityOutcome {
    Passed(ConnectivityPassed),
    Refused(ConnectivityRefused),
}

impl<'py> IntoPyObject<'py> for ConnectivityOutcome {
    type Target = PyAny;
    type Output = Bound<'py, PyAny>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        match self {
            Self::Passed(passed) => passed.into_bound_py_any(py),
            Self::Refused(refused) => refused.into_bound_py_any(py),
        }
    }
}

/// The record of a refusal of the core's check. Pure.
pub fn refused_record(refused: &ConnectivityRefusal) -> ConnectivityRefused {
    let step = match refused {
        ConnectivityRefusal::PoolSettings(_) => "POOL_SETTINGS",
        ConnectivityRefusal::MainUnreachable(_) => "MAIN_URL",
        ConnectivityRefusal::SessionUnreachable(_) => "SESSION_URL",
        ConnectivityRefusal::ListenerNotCapable(_) => "LISTENER",
    };
    ConnectivityRefused {
        step: step.to_owned(),
        message: refused.to_string(),
    }
}

/// Connect the handle's broker and run the core's connectivity check. A
/// failed connect is the step `CONNECT`.
pub async fn check_connectivity_operation(connection: BrokerConnection) -> ConnectivityOutcome {
    let broker = match connection.broker().await {
        Ok(broker) => broker,
        Err(error) => {
            return ConnectivityOutcome::Refused(ConnectivityRefused {
                step: "CONNECT".to_owned(),
                message: error.to_string(),
            })
        }
    };
    match broker.check_connectivity().await {
        Ok(()) => ConnectivityOutcome::Passed(ConnectivityPassed {}),
        Err(refused) => ConnectivityOutcome::Refused(refused_record(&refused)),
    }
}

#[cfg(test)]
mod tests {
    use horsies_core::{CheckInterruption, ListenProbeFailure};

    use super::*;

    #[test]
    fn a_listener_refusal_is_the_step_listener_with_the_core_cause() {
        let record = refused_record(&ConnectivityRefusal::ListenerNotCapable(
            ListenProbeFailure::Interrupted(CheckInterruption::Reconnected),
        ));
        assert_eq!(record.step, "LISTENER");
        assert_eq!(record.message, "the listener reconnected during the probe");
    }
}
