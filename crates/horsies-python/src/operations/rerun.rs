//! The rerun in the caller's transaction: three pure functions of
//! `horsies._core` over the core's rerun steps
//! (`horsies_core::{rerun_read, prepare_rerun_write, decode_rerun_write}`).
//! The core builds each statement and decides the outcome; the Python caller
//! runs each statement on its own connection, so the rerun commits or rolls
//! back with the caller's own writes.
//!
//! A statement crosses as a frozen `PreparedStatement`: its text with a named
//! bind and a cast for each parameter (`CAST(:p1 AS uuid)`, the SQLAlchemy
//! `text()` form), one Python value per bind, and its result columns. The
//! caller passes the row back as a tuple in column order; a value of another
//! type than its column is a defect of the caller (`CoreDefect`).
//!
//! The functions do no I/O and start no runtime; the fork and stop rules
//! apply, as for `mint_task_id`.

use chrono::{DateTime, Duration, Utc};
use horsies_core::{
    decode_rerun_write, named_bind, prepare_rerun_write, rerun_read, CallerRow, CallerStatement,
    CallerType, ClientFamily, NotEligibleReason, RecordedFamily, RerunEnqueuePolicy,
    RerunPreparation, RerunRefusal, RerunTask, RerunWrite, RerunWriteOutcome, StatementValue,
};
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{PyBool, PyBytes, PyDict, PyInt, PyString, PyTuple};
use pyo3::IntoPyObjectExt;
use uuid::Uuid;

use crate::errors::{self, CoreDefect};
use crate::operations::arguments::utc_instant;
use crate::process_core;
use crate::IdMintRefused;

/// The family of every rerun that this module prepares.
const OWN_FAMILY: ClientFamily = ClientFamily::Python;

/// One rerun request. A naive `deadline` raises `TypeError`.
#[pyclass(module = "horsies._core", name = "RerunCommandRecord", frozen)]
pub struct RerunCommandArgument {
    command: RerunTask,
}

#[pymethods]
impl RerunCommandArgument {
    #[new]
    #[pyo3(signature = (*, source_task_id, deadline, caller_key))]
    fn new(
        source_task_id: Uuid,
        deadline: Option<&Bound<'_, PyAny>>,
        caller_key: Option<String>,
    ) -> PyResult<Self> {
        let deadline = match deadline {
            Some(value) => Some(utc_instant(value)?),
            None => None,
        };
        Ok(Self {
            command: RerunTask::new(source_task_id, deadline, caller_key),
        })
    }
}

/// The enqueue policy of the new task: its retention class key, whether it
/// keeps its input for a rerun, and the reservation window of a caller key
/// in microseconds. A window that is not above 0 or is above 30 days, or an
/// empty class key, raises `ValueError`.
#[pyclass(module = "horsies._core", name = "RerunPolicyRecord", frozen)]
pub struct RerunPolicyArgument {
    policy: RerunEnqueuePolicy,
}

#[pymethods]
impl RerunPolicyArgument {
    #[new]
    #[pyo3(signature = (*, retention_class_key, retain_rerun_input, reservation_window_us))]
    fn new(
        retention_class_key: String,
        retain_rerun_input: bool,
        reservation_window_us: i64,
    ) -> PyResult<Self> {
        let policy = RerunEnqueuePolicy::new(
            retention_class_key,
            retain_rerun_input,
            Duration::microseconds(reservation_window_us),
        )
        .map_err(|refused| PyValueError::new_err(refused.to_string()))?;
        Ok(Self { policy })
    }
}

/// One statement for the caller's connection: `sql` (named binds with a
/// cast each), `parameters` (one value per bind, `p1` first), `columns`
/// (the result columns as (name, SQL type), in row order).
#[pyclass(module = "horsies._core", name = "PreparedStatement", frozen)]
#[derive(Clone)]
pub struct PreparedStatementRecord {
    statement: CallerStatement,
}

#[pymethods]
impl PreparedStatementRecord {
    #[getter]
    fn name(&self) -> &'static str {
        self.statement.name()
    }

    #[getter]
    fn sql(&self) -> String {
        self.statement.named_sql().to_owned()
    }

    #[getter]
    fn parameters<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyDict>> {
        let parameters = PyDict::new(py);
        for (index, value) in self.statement.parameters().iter().enumerate() {
            parameters.set_item(named_bind(index + 1), python_value(py, value)?)?;
        }
        Ok(parameters)
    }

    #[getter]
    fn columns(&self) -> Vec<(&'static str, &'static str)> {
        self.statement
            .columns()
            .iter()
            .map(|column| (column.name, column.value_type.sql_name()))
            .collect()
    }
}

/// The Python value of one parameter.
fn python_value<'py>(py: Python<'py>, value: &StatementValue) -> PyResult<Bound<'py, PyAny>> {
    match value {
        StatementValue::Bool(value) => (*value).into_bound_py_any(py),
        StatementValue::Int2(value) => (*value).into_bound_py_any(py),
        StatementValue::Int4(value) => (*value).into_bound_py_any(py),
        StatementValue::Int8(value) => (*value).into_bound_py_any(py),
        StatementValue::Text(value) => value.as_str().into_bound_py_any(py),
        StatementValue::Bytea(value) => Ok(PyBytes::new(py, value).into_any()),
        StatementValue::Uuid(value) => (*value).into_bound_py_any(py),
        StatementValue::Timestamptz(value) => (*value).into_bound_py_any(py),
        StatementValue::Null(_) => Ok(py.None().into_bound(py)),
    }
}

/// The row that the caller read for `statement`, by the declared column
/// types. A row of another length, or a value of another type than its
/// column, is a defect of the caller: `CoreDefect` names the statement and
/// the column.
fn caller_row(statement: &CallerStatement, row: &Bound<'_, PyTuple>) -> PyResult<CallerRow> {
    let columns = statement.columns();
    if row.len() != columns.len() {
        return Err(CoreDefect::new_err(format!(
            "statement `{}` has {} result columns; the row has {} values",
            statement.name(),
            columns.len(),
            row.len()
        )));
    }
    let mut values = Vec::with_capacity(columns.len());
    for (cell, column) in row.iter().zip(columns) {
        let value = statement_value(&cell, column.value_type).map_err(|cause| {
            CoreDefect::new_err(format!(
                "statement `{}` column `{}` is {}: {cause}",
                statement.name(),
                column.name,
                column.value_type.sql_name()
            ))
        })?;
        values.push(value);
    }
    Ok(CallerRow::new(values))
}

/// One cell as a value of `value_type`; `None` is SQL `NULL`. The refusal
/// names what the cell holds.
fn statement_value(
    cell: &Bound<'_, PyAny>,
    value_type: CallerType,
) -> Result<StatementValue, String> {
    if cell.is_none() {
        return Ok(StatementValue::Null(value_type));
    }
    let held = || match cell.get_type().name() {
        Ok(name) => format!("the row holds a {name}"),
        Err(_) => "the row holds a value of an unknown type".to_owned(),
    };
    match value_type {
        CallerType::Bool => match cell.is_instance_of::<PyBool>() {
            true => cell
                .extract::<bool>()
                .map(StatementValue::Bool)
                .map_err(|_| held()),
            false => Err(held()),
        },
        CallerType::Int2 | CallerType::Int4 | CallerType::Int8 => {
            match (
                cell.is_instance_of::<PyInt>(),
                cell.is_instance_of::<PyBool>(),
            ) {
                (true, false) => integer_value(cell, value_type).ok_or_else(held),
                (true, true) | (false, _) => Err(held()),
            }
        }
        CallerType::Text => match cell.is_instance_of::<PyString>() {
            true => cell
                .extract::<String>()
                .map(StatementValue::Text)
                .map_err(|_| held()),
            false => Err(held()),
        },
        CallerType::Bytea => match cell.cast::<PyBytes>() {
            Ok(bytes) => Ok(StatementValue::Bytea(bytes.as_bytes().to_vec())),
            Err(_) => Err(held()),
        },
        CallerType::Uuid => cell
            .extract::<Uuid>()
            .map(StatementValue::Uuid)
            .map_err(|_| held()),
        CallerType::Timestamptz => utc_instant(cell)
            .map(StatementValue::Timestamptz)
            .map_err(|_| held()),
    }
}

/// An `int` cell of an integer column, or `None` when it does not fit.
fn integer_value(cell: &Bound<'_, PyAny>, value_type: CallerType) -> Option<StatementValue> {
    match value_type {
        CallerType::Int2 => cell.extract::<i16>().ok().map(StatementValue::Int2),
        CallerType::Int4 => cell.extract::<i32>().ok().map(StatementValue::Int4),
        CallerType::Int8 => cell.extract::<i64>().ok().map(StatementValue::Int8),
        CallerType::Bool
        | CallerType::Text
        | CallerType::Bytea
        | CallerType::Uuid
        | CallerType::Timestamptz => None,
    }
}

/// The write of a rerun: run `statement`, then pass its row to
/// `decode_rerun_write`. `new_task_id` is the id minted for the new task.
#[pyclass(module = "horsies._core", frozen)]
pub struct RerunWriteRecord {
    write: RerunWrite,
}

#[pymethods]
impl RerunWriteRecord {
    #[getter]
    fn statement(&self) -> PreparedStatementRecord {
        PreparedStatementRecord {
            statement: self.write.statement().clone(),
        }
    }

    #[getter]
    fn new_task_id(&self) -> Uuid {
        self.write.new_task_id()
    }
}

/// The source is live; its own lifecycle owns its retry. Nothing written.
#[pyclass(module = "horsies._core", frozen, get_all)]
#[derive(Clone, Debug)]
pub struct RerunSourceLiveRecord {
    pub task_id: Uuid,
}

/// No live or history record has the source id; `reason` is
/// `PREDATES_RETAINED_HISTORY`, `WITHIN_RETAINED_HISTORY` or `NOT_CLASSIFIED`.
/// Nothing written.
#[pyclass(module = "horsies._core", frozen, get_all)]
#[derive(Clone, Debug)]
pub struct RerunSourceAbsentRecord {
    pub task_id: Uuid,
    pub reason: &'static str,
}

/// The source history row does not decode; `detail` names why. Nothing
/// written.
#[pyclass(module = "horsies._core", frozen, get_all)]
#[derive(Clone, Debug)]
pub struct RerunSourceCorruptRecord {
    pub task_id: Uuid,
    pub detail: String,
}

/// The source is not eligible; `reason` is `COMPLETED_SOURCE` or
/// `WORKFLOW_TASK`. Nothing written.
#[pyclass(module = "horsies._core", frozen, get_all)]
#[derive(Clone, Debug)]
pub struct RerunNotEligibleRecord {
    pub task_id: Uuid,
    pub reason: &'static str,
}

/// The source stores no input envelope; `disposition` is its stored reason.
/// Nothing written.
#[pyclass(module = "horsies._core", frozen, get_all)]
#[derive(Clone, Debug)]
pub struct RerunInputUnavailableRecord {
    pub task_id: Uuid,
    pub disposition: String,
}

/// The stored envelope, its input or its options fail their checks;
/// `detail` names why. Nothing written.
#[pyclass(module = "horsies._core", frozen, get_all)]
#[derive(Clone, Debug)]
pub struct RerunInputCorruptRecord {
    pub task_id: Uuid,
    pub detail: String,
}

/// The new task's retention class has no class row. Nothing written.
#[pyclass(module = "horsies._core", frozen, get_all)]
#[derive(Clone, Debug)]
pub struct RerunClassNotRegistered {
    pub class_key: String,
}

/// The database records the other client family (`stored` is `RUST`) or
/// none (`stored` is `None`). Nothing written. `message` names both.
#[pyclass(module = "horsies._core", frozen, get_all)]
#[derive(Clone, Debug)]
pub struct RerunClientFamilyMismatch {
    pub stored: Option<&'static str>,
    pub message: String,
}

/// The caller key is not a valid idempotency key; `cause` names why.
/// Nothing written.
#[pyclass(module = "horsies._core", frozen, get_all)]
#[derive(Clone, Debug)]
pub struct RerunCallerKeyRefused {
    pub cause: String,
}

/// The new task exists: this write, or this write already run in the
/// caller's transaction.
#[pyclass(module = "horsies._core", frozen, get_all)]
#[derive(Clone, Debug)]
pub struct RerunEnqueuedRecord {
    pub new_task_id: Uuid,
    pub source_task_id: Uuid,
    pub rerun_root_task_id: Uuid,
}

/// The caller key already holds this rerun command, as `existing_task_id`.
#[pyclass(module = "horsies._core", frozen, get_all)]
#[derive(Clone, Debug)]
pub struct RerunKeyReplayRecord {
    pub existing_task_id: Uuid,
}

/// The caller key holds another command (the source's own included).
#[pyclass(module = "horsies._core", frozen, get_all)]
#[derive(Clone, Debug)]
pub struct RerunKeyConflictRecord {
    pub task_id: Uuid,
    pub reserved_by_task_id: Uuid,
}

/// What the read's row decides.
pub enum PreparationRecord {
    Write(RerunWriteRecord),
    SourceLive(RerunSourceLiveRecord),
    SourceAbsent(RerunSourceAbsentRecord),
    SourceCorrupt(RerunSourceCorruptRecord),
    NotEligible(RerunNotEligibleRecord),
    InputUnavailable(RerunInputUnavailableRecord),
    InputCorrupt(RerunInputCorruptRecord),
    ClassNotRegistered(RerunClassNotRegistered),
    ClientFamilyMismatch(RerunClientFamilyMismatch),
    CallerKeyRefused(RerunCallerKeyRefused),
    IdMintRefused(IdMintRefused),
}

impl<'py> IntoPyObject<'py> for PreparationRecord {
    type Target = PyAny;
    type Output = Bound<'py, PyAny>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        match self {
            Self::Write(record) => record.into_bound_py_any(py),
            Self::SourceLive(record) => record.into_bound_py_any(py),
            Self::SourceAbsent(record) => record.into_bound_py_any(py),
            Self::SourceCorrupt(record) => record.into_bound_py_any(py),
            Self::NotEligible(record) => record.into_bound_py_any(py),
            Self::InputUnavailable(record) => record.into_bound_py_any(py),
            Self::InputCorrupt(record) => record.into_bound_py_any(py),
            Self::ClassNotRegistered(record) => record.into_bound_py_any(py),
            Self::ClientFamilyMismatch(record) => record.into_bound_py_any(py),
            Self::CallerKeyRefused(record) => record.into_bound_py_any(py),
            Self::IdMintRefused(record) => record.into_bound_py_any(py),
        }
    }
}

/// The outcome of a rerun write.
pub enum WriteOutcomeRecord {
    Enqueued(RerunEnqueuedRecord),
    KeyReplay(RerunKeyReplayRecord),
    KeyConflict(RerunKeyConflictRecord),
}

impl<'py> IntoPyObject<'py> for WriteOutcomeRecord {
    type Target = PyAny;
    type Output = Bound<'py, PyAny>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        match self {
            Self::Enqueued(record) => record.into_bound_py_any(py),
            Self::KeyReplay(record) => record.into_bound_py_any(py),
            Self::KeyConflict(record) => record.into_bound_py_any(py),
        }
    }
}

/// The record of a refusal. Pure.
fn refusal_record(refusal: RerunRefusal) -> PreparationRecord {
    match refusal {
        RerunRefusal::SourceLive { task_id } => {
            PreparationRecord::SourceLive(RerunSourceLiveRecord { task_id })
        }
        RerunRefusal::SourceAbsent { task_id, reason } => {
            PreparationRecord::SourceAbsent(RerunSourceAbsentRecord {
                task_id,
                reason: reason.as_str(),
            })
        }
        RerunRefusal::SourceCorrupt { task_id, detail } => {
            PreparationRecord::SourceCorrupt(RerunSourceCorruptRecord { task_id, detail })
        }
        RerunRefusal::NotEligible { task_id, reason } => {
            PreparationRecord::NotEligible(RerunNotEligibleRecord {
                task_id,
                reason: not_eligible_text(reason),
            })
        }
        RerunRefusal::InputUnavailable {
            task_id,
            disposition,
        } => PreparationRecord::InputUnavailable(RerunInputUnavailableRecord {
            task_id,
            disposition,
        }),
        RerunRefusal::InputCorrupt { task_id, detail } => {
            PreparationRecord::InputCorrupt(RerunInputCorruptRecord { task_id, detail })
        }
    }
}

/// Python's `NotEligibleReason` value. Pure.
fn not_eligible_text(reason: NotEligibleReason) -> &'static str {
    match reason {
        NotEligibleReason::CompletedSource => "COMPLETED_SOURCE",
        NotEligibleReason::WorkflowTask => "WORKFLOW_TASK",
    }
}

/// The record of what the read's row decides. Pure.
fn preparation_record(preparation: RerunPreparation) -> PreparationRecord {
    match preparation {
        RerunPreparation::Write(write) => PreparationRecord::Write(RerunWriteRecord { write }),
        RerunPreparation::Refused(refusal) => refusal_record(refusal),
        RerunPreparation::ClassNotRegistered { class_key } => {
            PreparationRecord::ClassNotRegistered(RerunClassNotRegistered { class_key })
        }
        RerunPreparation::ClientFamilyMismatch { stored, own } => {
            PreparationRecord::ClientFamilyMismatch(RerunClientFamilyMismatch {
                stored: match stored {
                    RecordedFamily::Recorded(family) => Some(family.as_str()),
                    RecordedFamily::NotRecorded => None,
                },
                message: format!(
                    "rerun refused: this database was installed by {stored}; this is the {} \
                     client",
                    own.as_str()
                ),
            })
        }
        RerunPreparation::CallerKeyRefused(refused) => {
            PreparationRecord::CallerKeyRefused(RerunCallerKeyRefused {
                cause: refused.to_string(),
            })
        }
        RerunPreparation::IdMintRefused(refused) => {
            PreparationRecord::IdMintRefused(IdMintRefused {
                cause: refused.to_string(),
            })
        }
    }
}

/// The record of a write outcome. Pure.
fn write_outcome_record(outcome: RerunWriteOutcome) -> WriteOutcomeRecord {
    match outcome {
        RerunWriteOutcome::Enqueued {
            new_task_id,
            source_task_id,
            rerun_root_task_id,
        } => WriteOutcomeRecord::Enqueued(RerunEnqueuedRecord {
            new_task_id,
            source_task_id,
            rerun_root_task_id,
        }),
        RerunWriteOutcome::KeyReplay { existing_task_id } => {
            WriteOutcomeRecord::KeyReplay(RerunKeyReplayRecord { existing_task_id })
        }
        RerunWriteOutcome::KeyConflict {
            task_id,
            reserved_by_task_id,
        } => WriteOutcomeRecord::KeyConflict(RerunKeyConflictRecord {
            task_id,
            reserved_by_task_id,
        }),
    }
}

/// Refuses a pure call in a forked or stopped core process.
fn pure_call_allowed() -> PyResult<()> {
    match process_core::pure_call_refusal() {
        Some(refusal) => Err(errors::refusal_error(refusal)),
        None => Ok(()),
    }
}

/// The source read of a rerun (`horsies_rerun_source_read`) as a
/// `PreparedStatement`. Pure: no I/O, and it starts no runtime.
#[pyfunction(name = "rerun_read")]
pub fn rerun_read_function(
    command: &Bound<'_, RerunCommandArgument>,
    policy: &Bound<'_, RerunPolicyArgument>,
) -> PyResult<PreparedStatementRecord> {
    pure_call_allowed()?;
    Ok(PreparedStatementRecord {
        statement: rerun_read(&command.get().command, &policy.get().policy),
    })
}

/// Decide the rerun from the row of `read` (a tuple in column order):
/// `RerunWriteRecord`, a refusal record, `RerunClassNotRegistered`,
/// `RerunClientFamilyMismatch`, `RerunCallerKeyRefused` or `IdMintRefused`.
/// `sent_at` is the send instant of the new task (aware). Pure apart from
/// the id mint. A row that does not fit the read's columns raises
/// `CoreDefect`.
#[pyfunction(name = "prepare_rerun_write")]
#[pyo3(signature = (read, row, command, policy, *, sent_at))]
pub fn prepare_rerun_write_function(
    read: &Bound<'_, PreparedStatementRecord>,
    row: &Bound<'_, PyTuple>,
    command: &Bound<'_, RerunCommandArgument>,
    policy: &Bound<'_, RerunPolicyArgument>,
    sent_at: &Bound<'_, PyAny>,
) -> PyResult<PreparationRecord> {
    pure_call_allowed()?;
    let sent_at: DateTime<Utc> = utc_instant(sent_at)?;
    let read = &read.get().statement;
    let caller_row = caller_row(read, row)?;
    let preparation = prepare_rerun_write(
        read,
        &caller_row,
        &command.get().command,
        &policy.get().policy,
        OWN_FAMILY,
        sent_at,
    )
    .map_err(|defect| CoreDefect::new_err(defect.to_string()))?;
    Ok(preparation_record(preparation))
}

/// The outcome of the row of `write` (a tuple in column order):
/// `RerunEnqueuedRecord`, `RerunKeyReplayRecord` or `RerunKeyConflictRecord`.
/// Pure. A row that does not fit the write's columns raises `CoreDefect`; an
/// answer that contradicts the minted id raises `PanicException`.
#[pyfunction(name = "decode_rerun_write")]
pub fn decode_rerun_write_function(
    write: &Bound<'_, RerunWriteRecord>,
    row: &Bound<'_, PyTuple>,
) -> PyResult<WriteOutcomeRecord> {
    pure_call_allowed()?;
    let write = &write.get().write;
    let caller_row = caller_row(write.statement(), row)?;
    let outcome = decode_rerun_write(write, &caller_row)
        .map_err(|defect| CoreDefect::new_err(defect.to_string()))?;
    Ok(write_outcome_record(outcome))
}

#[cfg(test)]
mod tests {
    use horsies_core::{IdempotencyKeyError, TaskAbsenceReason, Uuid7Error};

    use super::*;

    const SOURCE: Uuid = Uuid::from_u128(0x0190_f0f0_0000_7000_8000_0000_0000_00e0);

    #[test]
    fn each_refusal_has_its_record_with_python_values() {
        assert!(matches!(
            refusal_record(RerunRefusal::SourceAbsent {
                task_id: SOURCE,
                reason: TaskAbsenceReason::PredatesRetainedHistory,
            }),
            PreparationRecord::SourceAbsent(RerunSourceAbsentRecord {
                reason: "PREDATES_RETAINED_HISTORY",
                ..
            })
        ));
        assert!(matches!(
            refusal_record(RerunRefusal::NotEligible {
                task_id: SOURCE,
                reason: NotEligibleReason::WorkflowTask,
            }),
            PreparationRecord::NotEligible(RerunNotEligibleRecord {
                reason: "WORKFLOW_TASK",
                ..
            })
        ));
        assert_eq!(
            not_eligible_text(NotEligibleReason::CompletedSource),
            "COMPLETED_SOURCE"
        );
        assert!(matches!(
            refusal_record(RerunRefusal::SourceLive { task_id: SOURCE }),
            PreparationRecord::SourceLive(RerunSourceLiveRecord { task_id }) if task_id == SOURCE
        ));
        assert!(matches!(
            refusal_record(RerunRefusal::InputCorrupt {
                task_id: SOURCE,
                detail: "digest".to_owned(),
            }),
            PreparationRecord::InputCorrupt(RerunInputCorruptRecord { ref detail, .. })
                if detail == "digest"
        ));
    }

    #[test]
    fn the_preconditions_have_their_records() {
        assert!(matches!(
            preparation_record(RerunPreparation::ClientFamilyMismatch {
                stored: RecordedFamily::Recorded(ClientFamily::Rust),
                own: ClientFamily::Python,
            }),
            PreparationRecord::ClientFamilyMismatch(RerunClientFamilyMismatch {
                stored: Some("RUST"),
                ref message,
            }) if message.contains("the RUST client") && message.contains("PYTHON")
        ));
        assert!(matches!(
            preparation_record(RerunPreparation::ClientFamilyMismatch {
                stored: RecordedFamily::NotRecorded,
                own: ClientFamily::Python,
            }),
            PreparationRecord::ClientFamilyMismatch(RerunClientFamilyMismatch { stored: None, .. })
        ));
        assert!(matches!(
            preparation_record(RerunPreparation::CallerKeyRefused(
                IdempotencyKeyError::Empty {
                    label: "idempotency key"
                }
            )),
            PreparationRecord::CallerKeyRefused(RerunCallerKeyRefused { ref cause })
                if cause.contains("must be non-empty")
        ));
        assert!(matches!(
            preparation_record(RerunPreparation::IdMintRefused(Uuid7Error::ClockOutOfRange)),
            PreparationRecord::IdMintRefused(_)
        ));
        assert!(matches!(
            preparation_record(RerunPreparation::ClassNotRegistered {
                class_key: "standard_8d".to_owned()
            }),
            PreparationRecord::ClassNotRegistered(RerunClassNotRegistered { ref class_key })
                if class_key == "standard_8d"
        ));
    }

    #[test]
    fn each_write_outcome_has_its_record() {
        assert!(matches!(
            write_outcome_record(RerunWriteOutcome::KeyReplay {
                existing_task_id: SOURCE
            }),
            WriteOutcomeRecord::KeyReplay(RerunKeyReplayRecord { existing_task_id })
                if existing_task_id == SOURCE
        ));
        assert!(matches!(
            write_outcome_record(RerunWriteOutcome::KeyConflict {
                task_id: SOURCE,
                reserved_by_task_id: SOURCE,
            }),
            WriteOutcomeRecord::KeyConflict(_)
        ));
    }

    #[test]
    fn the_read_statement_has_named_binds_and_its_columns() {
        let command = RerunTask::new(SOURCE, None, None);
        let policy =
            RerunEnqueuePolicy::new("forever", true, Duration::hours(24)).expect("a valid policy");
        let record = PreparedStatementRecord {
            statement: rerun_read(&command, &policy),
        };
        assert!(record
            .sql()
            .ends_with("horsies_rerun_source_read(CAST(:p1 AS uuid), CAST(:p2 AS text))"));
        let columns = record.columns();
        assert_eq!(columns.len(), 19);
        assert_eq!(columns[0], ("class_registered", "bool"));
        assert_eq!(columns[1], ("client_family", "text"));
    }

    #[test]
    fn row_cells_convert_by_their_column_type() {
        Python::initialize();
        Python::attach(|py| {
            let none = py.None().into_bound(py);
            assert_eq!(
                statement_value(&none, CallerType::Int4),
                Ok(StatementValue::Null(CallerType::Int4))
            );
            let flag = true.into_bound_py_any(py).expect("a bool");
            assert_eq!(
                statement_value(&flag, CallerType::Bool),
                Ok(StatementValue::Bool(true))
            );
            assert!(statement_value(&flag, CallerType::Int4).is_err());
            let number = 7_i64.into_bound_py_any(py).expect("an int");
            assert_eq!(
                statement_value(&number, CallerType::Int2),
                Ok(StatementValue::Int2(7))
            );
            let large = 70_000_i64.into_bound_py_any(py).expect("an int");
            assert!(statement_value(&large, CallerType::Int2).is_err());
            let text = "x".into_bound_py_any(py).expect("a str");
            assert!(statement_value(&text, CallerType::Bytea).is_err());
            let bytes = PyBytes::new(py, b"ab").into_any();
            assert_eq!(
                statement_value(&bytes, CallerType::Bytea),
                Ok(StatementValue::Bytea(b"ab".to_vec()))
            );
        });
    }
}
