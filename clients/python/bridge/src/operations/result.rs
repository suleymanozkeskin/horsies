//! The result read and wait: one task's stored result envelope, from the
//! live row or the task history, waiting up to a limit for a task that has
//! not finished (`PostgresBroker::get_raw_result_record`).
//!
//! The Python client checks `timeout_ms` with its own rule
//! (`wait_timeout_seconds`) before the call, so the core gets 0 to
//! 4 294 967 295 ms or no limit. The outcome is `RawResultRecordRecord` (the
//! task's status and its result envelope as JSON text; no result when the
//! limit passed first), `RawResultAbsent`, or the generated broker error class.

use std::time::Duration;

use horsies_core::catalog::CodeEnum;
use horsies_core::{DatabaseOperationError, RawResultRead, RawResultRecord};
use pyo3::prelude::*;
use pyo3::IntoPyObjectExt;
use uuid::Uuid;

use crate::bridge::blocking::{BlockingBound, BLOCKING_CALL_BOUND};
use crate::handle::BrokerConnection;
use crate::records::BrokerFailure;

/// The subject of the exception of a failed read.
const SUBJECT: &str = "raw_result_record";

/// The status of one task and its stored result envelope. `result_json` is
/// the outer envelope as JSON text for a terminal task, and `None` for a
/// task that had not finished when the limit passed.
#[pyclass(module = "horsies._core", frozen, get_all)]
#[derive(Clone, Debug)]
pub struct RawResultRecordRecord {
    pub task_id: Uuid,
    pub task_name: String,
    /// A member of `TaskStatus`, as its wire value.
    pub status: String,
    pub result_json: Option<String>,
}

impl RawResultRecordRecord {
    /// The record of a core `RawResultRecord`: the envelope text as the row
    /// stores it, so an integer outside the `i64` and `u64` range keeps
    /// every digit.
    fn from_core(record: RawResultRecord) -> Self {
        Self {
            task_id: record.task_id,
            task_name: record.task_name,
            status: record.status.wire_value().to_owned(),
            result_json: record
                .raw_result
                .map(|envelope| envelope.as_str().to_owned()),
        }
    }
}

/// Neither the live table nor the task history holds the task. The core's
/// absence reason is not kept: Python's `TASK_NOT_FOUND` carries the task id
/// only.
#[pyclass(module = "horsies._core", frozen, get_all)]
#[derive(Clone, Debug)]
pub struct RawResultAbsent {
    pub task_id: Uuid,
}

/// The outcome of one result read.
pub enum RawResultOutcome {
    Found(RawResultRecordRecord),
    Absent(RawResultAbsent),
    Failed(BrokerFailure),
}

impl<'py> IntoPyObject<'py> for RawResultOutcome {
    type Target = PyAny;
    type Output = Bound<'py, PyAny>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        match self {
            Self::Found(record) => record.into_bound_py_any(py),
            Self::Absent(absent) => absent.into_bound_py_any(py),
            Self::Failed(failure) => failure.into_bound_py_any(py),
        }
    }
}

/// The bound of the blocking form of a wait: its limit plus the bound of
/// any other blocking call, for the last read after the limit; no bound for
/// a wait with no limit, as Python's sync wait. Pure.
pub fn result_wait_bound(timeout_ms: Option<u64>) -> BlockingBound {
    match timeout_ms {
        Some(limit_ms) => BlockingBound::Within(
            Duration::from_millis(limit_ms).saturating_add(BLOCKING_CALL_BOUND),
        ),
        None => BlockingBound::UntilTheOperationEnds,
    }
}

/// The outcome of a read. Pure.
fn read_outcome(
    task_id: Uuid,
    read: Result<RawResultRead, horsies_core::DatabaseOperationError>,
) -> RawResultOutcome {
    match read {
        Ok(RawResultRead::Found(record)) => {
            RawResultOutcome::Found(RawResultRecordRecord::from_core(record))
        }
        Ok(RawResultRead::Absent { reason: _ }) => {
            RawResultOutcome::Absent(RawResultAbsent { task_id })
        }
        Err(error) => RawResultOutcome::Failed(BrokerFailure::of_operation_error(SUBJECT, error)),
    }
}

/// Read the result of one task, waiting up to `timeout_ms` (no limit for
/// `None`) while it has not finished. Installs the schema once per handle
/// first, as Python's `get_raw_result_record_async`; a failed connect or
/// install is `TASK_INFO_QUERY_FAILED`, as Python reports any failure of the
/// read. Then reads only: a caller that leaves drops the wait, and nothing
/// changed.
pub async fn raw_result_record_operation(
    connection: BrokerConnection,
    task_id: Uuid,
    timeout_ms: Option<u64>,
) -> RawResultOutcome {
    let broker = match connection.initialized_broker().await {
        Ok(broker) => broker,
        Err(error) => {
            return RawResultOutcome::Failed(BrokerFailure::of_operation_error(
                SUBJECT,
                DatabaseOperationError::raw_result_query_failed(task_id, error),
            ))
        }
    };
    let read = broker
        .get_raw_result_record(task_id, timeout_ms.map(Duration::from_millis))
        .await;
    read_outcome(task_id, read)
}

#[cfg(test)]
mod tests {
    use horsies_core::{DatabaseOperationError, StoredEnvelopeText, TaskAbsenceReason, TaskStatus};

    use super::*;

    fn task() -> Uuid {
        Uuid::from_u128(7)
    }

    #[test]
    fn a_terminal_task_gives_its_status_and_envelope() {
        let envelope = StoredEnvelopeText::parse(r#"{"ok":3}"#.to_owned())
            .expect("the text is strict JSON")
            .expect("an object is an envelope text");
        let RawResultOutcome::Found(found) = read_outcome(
            task(),
            Ok(RawResultRead::Found(RawResultRecord {
                task_id: task(),
                task_name: "add".to_owned(),
                status: TaskStatus::Completed,
                raw_result: Some(envelope),
            })),
        ) else {
            panic!("a record gives RawResultRecordRecord");
        };
        assert_eq!(found.status, "COMPLETED");
        assert_eq!(found.result_json.as_deref(), Some(r#"{"ok":3}"#));
    }

    #[test]
    fn a_wait_that_ended_at_its_limit_gives_the_status_and_no_result() {
        let RawResultOutcome::Found(found) = read_outcome(
            task(),
            Ok(RawResultRead::Found(RawResultRecord {
                task_id: task(),
                task_name: "add".to_owned(),
                status: TaskStatus::Running,
                raw_result: None,
            })),
        ) else {
            panic!("a record gives RawResultRecordRecord");
        };
        assert_eq!(found.status, "RUNNING");
        assert_eq!(found.result_json, None);
    }

    #[test]
    fn a_task_held_nowhere_is_absent() {
        assert!(matches!(
            read_outcome(
                task(),
                Ok(RawResultRead::Absent {
                    reason: TaskAbsenceReason::PredatesRetainedHistory
                })
            ),
            RawResultOutcome::Absent(RawResultAbsent { task_id }) if task_id == task()
        ));
    }

    #[test]
    fn a_failed_read_keeps_its_code_and_retry_rule() {
        let RawResultOutcome::Failed(failure) = read_outcome(
            task(),
            Err(DatabaseOperationError::InvalidJsonPayload {
                message: "digest mismatch".to_owned(),
                cause: Box::new(horsies_core::BrokerCheckCause::Check),
            }),
        ) else {
            panic!("a failed read gives BrokerFailure");
        };
        assert_eq!(failure.subject, SUBJECT);
        assert_eq!(
            failure.error.code(),
            horsies_core::DatabaseErrorCode::InvalidJsonPayload
        );
        assert!(!failure.error.retryable());
    }

    #[test]
    fn a_wait_with_a_limit_is_bounded_and_one_with_none_is_not() {
        assert_eq!(
            result_wait_bound(Some(1_000)),
            BlockingBound::Within(Duration::from_secs(1) + BLOCKING_CALL_BOUND)
        );
        assert_eq!(
            result_wait_bound(None),
            BlockingBound::UntilTheOperationEnds
        );
    }
}
