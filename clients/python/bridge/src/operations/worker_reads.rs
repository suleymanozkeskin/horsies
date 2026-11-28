//! The worker state reads and the execution class reads through the core:
//! `PostgresBroker::list_worker_states`, `get_worker_state`,
//! `get_worker_state_history`, `get_execution_class_coverage` and
//! `get_execution_class_report`.
//!
//! Each read answers its record, or the generated broker error class with `INVALID_INPUT` (a
//! history limit of 0 or less; nothing ran) or `MONITORING_QUERY_FAILED`.
//! The records carry the values of the core types field by field, one record
//! class per type and per union member; the Python client builds its frozen
//! dataclasses from them (`horsies.core.models.health`,
//! `horsies.core.models.class_coverage`). Each read sends one statement in
//! autocommit and writes nothing.

use std::collections::BTreeMap;

use chrono::{DateTime, TimeDelta, Utc};
use horsies_core::core::config::worker_profile::input::TaskLimitSetting;
use horsies_core::{
    checked_history_limit, worker_history_limit_over_bound, worker_history_limit_refused,
    BrokerOperationSource, BudgetLoad, ClearableBootCause, DatabaseOperationError, DownSlot,
    Execution, ExecutionClassBudget, ExecutionClassCoverage, ExecutionClassReport, PendingStream,
    SlotHealth, SnapshotProfile, StreamCoverage, StreamCoverageState, StreamWait,
    WorkerExecutionClassLoad, WorkerStateSnapshot,
};
use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;
use pyo3::types::{PyBool, PyInt, PyList};
use pyo3::IntoPyObjectExt;

use crate::handle::BrokerConnection;
use crate::operations::task_lists::connect_failed;
use crate::records::BrokerFailure;

/// The subject of the exception of a refused or failed history read.
const HISTORY_SUBJECT: &str = "get_worker_state_history";

/// The cause of a down slot that ran short of an OS resource.
#[pyclass(module = "horsies._core", frozen, get_all)]
#[derive(Clone, Debug)]
pub struct ResourceShortageRecord {
    /// `EMFILE`, `ENFILE`, `ENOMEM` or `EAGAIN`.
    pub errno: String,
}

/// A worker slot whose last child boot failed for a cause that can clear.
#[pyclass(module = "horsies._core", frozen, get_all)]
#[derive(Debug)]
pub struct DownSlotRecord {
    pub slot: u32,
    /// `ResourceShortageRecord`, or `'THREAD_START_REFUSED'`.
    pub cause: Py<PyAny>,
    pub detail: String,
    pub down_since: DateTime<Utc>,
    /// At least 1.
    pub failed_boots: u64,
}

/// No slot of the worker was down when it wrote the snapshot.
#[pyclass(module = "horsies._core", frozen)]
#[derive(Clone, Debug)]
pub struct AllSlotsServingRecord {}

/// The worker was degraded: these slots were down, in slot order. Never
/// empty.
#[pyclass(module = "horsies._core", frozen, get_all)]
#[derive(Debug)]
pub struct SlotsDownRecord {
    /// One `DownSlotRecord` per down slot.
    pub slots: Py<PyList>,
}

/// One claim budget of a worker, as its snapshot recorded it.
#[pyclass(module = "horsies._core", frozen, get_all)]
#[derive(Clone, Debug)]
pub struct ExecutionClassBudgetRecord {
    /// The execution class of the budget.
    pub budget: String,
    pub capacity: u64,
    /// The execution classes whose rows the budget claims, in `Execution`
    /// order.
    pub serves: Vec<String>,
}

/// The worker ran the profile `name` of `AppConfig.worker_profiles`.
#[pyclass(module = "horsies._core", frozen, get_all)]
#[derive(Clone, Debug)]
pub struct ProfileNamedRecord {
    pub name: String,
}

/// The snapshot holds no profile name: a worker of a build before schema
/// migration 1004 wrote it.
#[pyclass(module = "horsies._core", frozen)]
#[derive(Clone, Debug)]
pub struct ProfileNotRecordedRecord {}

/// One `horsies_worker_states` row: a worker's state at a point in time.
#[pyclass(module = "horsies._core", frozen, get_all)]
#[derive(Debug)]
pub struct WorkerStateRecord {
    pub worker_id: String,
    pub snapshot_at: DateTime<Utc>,
    pub hostname: String,
    pub pid: i32,
    /// The children of the worker.
    pub child_count: i32,
    /// The per queue limit of one claim pass; `None` when the profile sets
    /// no limit.
    pub per_queue_per_pass: Option<u32>,
    /// The claimed limit that the worker resolved; `None` when the snapshot
    /// records no limit.
    pub claimed_per_worker: Option<u32>,
    pub cluster_wide_cap: Option<i32>,
    pub queues: Vec<String>,
    pub queue_priorities: BTreeMap<String, i64>,
    pub queue_max_concurrency: BTreeMap<String, i64>,
    /// The JSON text of the stored object; `{}` when the worker had none.
    pub recovery_config_json: String,
    pub tasks_running: i32,
    pub tasks_claimed: i32,
    pub memory_usage_mb: Option<f64>,
    pub memory_percent: Option<f64>,
    pub cpu_percent: Option<f64>,
    pub children_memory_mb: Option<f64>,
    pub worker_started_at: DateTime<Utc>,
    /// `AllSlotsServingRecord` or `SlotsDownRecord`.
    pub slot_health: Py<PyAny>,
    /// One `ExecutionClassBudgetRecord` per budget, in `Execution` order.
    pub execution_class_budgets: Py<PyList>,
    /// `ProfileNamedRecord` or `ProfileNotRecordedRecord`.
    pub profile: Py<PyAny>,
}

/// The snapshots of a worker state read: the latest per worker, or the
/// history of one worker, newest first.
#[pyclass(module = "horsies._core", frozen, get_all)]
#[derive(Debug)]
pub struct WorkerStatesRecord {
    /// One `WorkerStateRecord` per snapshot.
    pub states: Py<PyList>,
}

/// The worker has written no snapshot.
#[pyclass(module = "horsies._core", frozen, get_all)]
#[derive(Clone, Debug)]
pub struct WorkerStateAbsent {
    pub worker_id: String,
}

/// Live workers serve the stream with capacity in service.
#[pyclass(module = "horsies._core", frozen, get_all)]
#[derive(Clone, Debug)]
pub struct StreamServedRecord {
    pub workers: usize,
    pub capacity: u64,
}

/// Live workers serve the stream, and none has capacity in service.
#[pyclass(module = "horsies._core", frozen, get_all)]
#[derive(Clone, Debug)]
pub struct StreamWithoutCapacityRecord {
    pub workers: usize,
}

/// No live worker serves the stream.
#[pyclass(module = "horsies._core", frozen)]
#[derive(Clone, Debug)]
pub struct StreamUnservedRecord {}

/// The coverage of one pending stream.
#[pyclass(module = "horsies._core", frozen, get_all)]
#[derive(Debug)]
pub struct StreamCoverageRecord {
    pub queue: String,
    pub execution: String,
    /// `StreamServedRecord`, `StreamWithoutCapacityRecord` or
    /// `StreamUnservedRecord`.
    pub coverage: Py<PyAny>,
}

/// The fleet check, read at `checked_at` (database time).
#[pyclass(module = "horsies._core", frozen, get_all)]
#[derive(Debug)]
pub struct ExecutionClassCoverageRecord {
    pub checked_at: DateTime<Utc>,
    pub live_workers: usize,
    /// One `StreamCoverageRecord` per pending stream.
    pub streams: Py<PyList>,
}

/// At least one row of the stream is due.
#[pyclass(module = "horsies._core", frozen, get_all)]
#[derive(Clone, Debug)]
pub struct OldestWaitingRecord {
    pub oldest_due_at: DateTime<Utc>,
    pub wait: TimeDelta,
}

/// Every row of the stream is due later.
#[pyclass(module = "horsies._core", frozen, get_all)]
#[derive(Clone, Debug)]
pub struct NoneDueRecord {
    pub next_due_at: DateTime<Utc>,
}

/// One pending stream of the class report.
#[pyclass(module = "horsies._core", frozen, get_all)]
#[derive(Debug)]
pub struct PendingStreamRecord {
    pub queue: String,
    pub execution: String,
    /// `StreamServedRecord`, `StreamWithoutCapacityRecord` or
    /// `StreamUnservedRecord`.
    pub coverage: Py<PyAny>,
    /// `OldestWaitingRecord` or `NoneDueRecord`.
    pub wait: Py<PyAny>,
}

/// One claim budget of a live worker, with the rows it holds.
#[pyclass(module = "horsies._core", frozen, get_all)]
#[derive(Clone, Debug)]
pub struct BudgetLoadRecord {
    pub budget: String,
    pub capacity: u64,
    pub serves: Vec<String>,
    pub in_flight: u64,
}

/// The claim budgets of one live worker.
#[pyclass(module = "horsies._core", frozen, get_all)]
#[derive(Debug)]
pub struct WorkerExecutionClassLoadRecord {
    pub worker_id: String,
    pub snapshot_at: DateTime<Utc>,
    /// One `BudgetLoadRecord` per budget, in `Execution` order.
    pub budgets: Py<PyList>,
}

/// The execution class report, read at `checked_at` (database time).
#[pyclass(module = "horsies._core", frozen, get_all)]
#[derive(Debug)]
pub struct ExecutionClassReportRecord {
    pub checked_at: DateTime<Utc>,
    /// The most age of the latest snapshot of a live worker.
    pub live_window: TimeDelta,
    /// One `PendingStreamRecord` per pending stream.
    pub streams: Py<PyList>,
    /// One `WorkerExecutionClassLoadRecord` per live worker, by worker id.
    pub workers: Py<PyList>,
}

/// A core value that crosses as a record.
pub trait ToRecord {
    /// The record of the value. Errors only when Python cannot build an
    /// object (memory).
    fn record<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>>;
}

/// The answer of `get_worker_state`.
pub enum WorkerStateLookup {
    Found(Box<WorkerStateSnapshot>),
    Absent { worker_id: String },
}

/// The outcome of one read: its value, or its failure.
pub enum WorkerReadOutcome<T> {
    Read(T),
    Failed(BrokerFailure),
}

impl<'py, T: ToRecord> IntoPyObject<'py> for WorkerReadOutcome<T> {
    type Target = PyAny;
    type Output = Bound<'py, PyAny>;
    type Error = PyErr;

    fn into_pyobject(self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        match self {
            Self::Read(value) => value.record(py),
            Self::Failed(failed) => failed.into_bound_py_any(py),
        }
    }
}

fn execution_names(classes: &[Execution]) -> Vec<String> {
    classes
        .iter()
        .map(|class| class.as_str().to_owned())
        .collect()
}

fn record_list<'py, T, F>(py: Python<'py>, values: &[T], record: F) -> PyResult<Py<PyList>>
where
    F: Fn(Python<'py>, &T) -> PyResult<Bound<'py, PyAny>>,
{
    let records = values
        .iter()
        .map(|value| record(py, value))
        .collect::<PyResult<Vec<_>>>()?;
    Ok(PyList::new(py, records)?.unbind())
}

fn down_slot_record<'py>(py: Python<'py>, slot: &DownSlot) -> PyResult<Bound<'py, PyAny>> {
    let cause = match slot.cause {
        ClearableBootCause::ResourceShortage(errno) => ResourceShortageRecord {
            errno: errno.as_str().to_owned(),
        }
        .into_py_any(py)?,
        ClearableBootCause::ThreadStartRefused => "THREAD_START_REFUSED".into_py_any(py)?,
    };
    DownSlotRecord {
        slot: slot.slot,
        cause,
        detail: slot.detail.clone(),
        down_since: slot.down_since,
        failed_boots: slot.failed_boots.get(),
    }
    .into_bound_py_any(py)
}

fn slot_health_record(py: Python<'_>, health: &SlotHealth) -> PyResult<Py<PyAny>> {
    match health {
        SlotHealth::AllServing => AllSlotsServingRecord {}.into_py_any(py),
        SlotHealth::SlotsDown { slots } => SlotsDownRecord {
            slots: record_list(py, slots.as_slice(), down_slot_record)?,
        }
        .into_py_any(py),
    }
}

/// The limit of a task limit setting; `None` for no limit.
fn task_limit(setting: TaskLimitSetting) -> Option<u32> {
    match setting {
        TaskLimitSetting::TaskLimit { tasks } => Some(tasks),
        TaskLimitSetting::NoTaskLimit {} => None,
    }
}

fn snapshot_profile_record(py: Python<'_>, profile: &SnapshotProfile) -> PyResult<Py<PyAny>> {
    match profile {
        SnapshotProfile::Named { name } => {
            ProfileNamedRecord { name: name.clone() }.into_py_any(py)
        }
        SnapshotProfile::NotRecorded => ProfileNotRecordedRecord {}.into_py_any(py),
    }
}

fn execution_class_budget_record<'py>(
    py: Python<'py>,
    budget: &ExecutionClassBudget,
) -> PyResult<Bound<'py, PyAny>> {
    ExecutionClassBudgetRecord {
        budget: budget.budget.as_str().to_owned(),
        capacity: budget.capacity,
        serves: execution_names(&budget.serves),
    }
    .into_bound_py_any(py)
}

fn worker_state_record<'py>(
    py: Python<'py>,
    snapshot: &WorkerStateSnapshot,
) -> PyResult<Bound<'py, PyAny>> {
    WorkerStateRecord {
        worker_id: snapshot.worker_id.clone(),
        snapshot_at: snapshot.snapshot_at,
        hostname: snapshot.hostname.clone(),
        pid: snapshot.pid,
        child_count: snapshot.child_count,
        per_queue_per_pass: task_limit(snapshot.claim_limits.per_queue_per_pass),
        claimed_per_worker: task_limit(snapshot.claim_limits.claimed_per_worker),
        // The record holds the cap as an int, or None for no cap; the Python
        // reader maps it to `ClusterCap` or `NoClusterCap`.
        cluster_wide_cap: snapshot
            .cluster_wide_cap
            .claim_parameter()
            .map(|tasks| i32::try_from(tasks).expect("a stored cap came from an integer column")),
        queues: snapshot.queues.clone(),
        queue_priorities: snapshot.queue_priorities.clone(),
        queue_max_concurrency: snapshot.queue_max_concurrency.clone(),
        recovery_config_json: serde_json::Value::Object(snapshot.recovery_config.clone())
            .to_string(),
        tasks_running: snapshot.tasks_running,
        tasks_claimed: snapshot.tasks_claimed,
        memory_usage_mb: snapshot.memory_usage_mb,
        memory_percent: snapshot.memory_percent,
        cpu_percent: snapshot.cpu_percent,
        children_memory_mb: snapshot.children_memory_mb,
        worker_started_at: snapshot.worker_started_at,
        slot_health: slot_health_record(py, &snapshot.slot_health)?,
        execution_class_budgets: record_list(
            py,
            snapshot.execution_class_budgets.as_slice(),
            execution_class_budget_record,
        )?,
        profile: snapshot_profile_record(py, &snapshot.profile)?,
    }
    .into_bound_py_any(py)
}

impl ToRecord for Vec<WorkerStateSnapshot> {
    fn record<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        WorkerStatesRecord {
            states: record_list(py, self, worker_state_record)?,
        }
        .into_bound_py_any(py)
    }
}

impl ToRecord for WorkerStateLookup {
    fn record<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        match self {
            Self::Found(snapshot) => worker_state_record(py, snapshot),
            Self::Absent { worker_id } => WorkerStateAbsent {
                worker_id: worker_id.clone(),
            }
            .into_bound_py_any(py),
        }
    }
}

fn coverage_state_record(py: Python<'_>, state: StreamCoverageState) -> PyResult<Py<PyAny>> {
    match state {
        StreamCoverageState::Served { workers, capacity } => {
            StreamServedRecord { workers, capacity }.into_py_any(py)
        }
        StreamCoverageState::NoCapacity { workers } => {
            StreamWithoutCapacityRecord { workers }.into_py_any(py)
        }
        StreamCoverageState::Unserved => StreamUnservedRecord {}.into_py_any(py),
    }
}

fn stream_coverage_record<'py>(
    py: Python<'py>,
    stream: &StreamCoverage,
) -> PyResult<Bound<'py, PyAny>> {
    StreamCoverageRecord {
        queue: stream.queue.clone(),
        execution: stream.execution.as_str().to_owned(),
        coverage: coverage_state_record(py, stream.coverage)?,
    }
    .into_bound_py_any(py)
}

impl ToRecord for ExecutionClassCoverage {
    fn record<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        ExecutionClassCoverageRecord {
            checked_at: self.checked_at,
            live_workers: self.live_workers,
            streams: record_list(py, &self.streams, stream_coverage_record)?,
        }
        .into_bound_py_any(py)
    }
}

fn stream_wait_record(py: Python<'_>, wait: StreamWait) -> PyResult<Py<PyAny>> {
    match wait {
        StreamWait::Due {
            oldest_due_at,
            wait,
        } => OldestWaitingRecord {
            oldest_due_at,
            wait,
        }
        .into_py_any(py),
        StreamWait::NoneDue { next_due_at } => NoneDueRecord { next_due_at }.into_py_any(py),
    }
}

fn pending_stream_record<'py>(
    py: Python<'py>,
    stream: &PendingStream,
) -> PyResult<Bound<'py, PyAny>> {
    PendingStreamRecord {
        queue: stream.queue.clone(),
        execution: stream.execution.as_str().to_owned(),
        coverage: coverage_state_record(py, stream.coverage)?,
        wait: stream_wait_record(py, stream.wait)?,
    }
    .into_bound_py_any(py)
}

fn budget_load_record<'py>(py: Python<'py>, load: &BudgetLoad) -> PyResult<Bound<'py, PyAny>> {
    BudgetLoadRecord {
        budget: load.budget.as_str().to_owned(),
        capacity: load.capacity,
        serves: execution_names(&load.serves),
        in_flight: load.in_flight,
    }
    .into_bound_py_any(py)
}

fn worker_execution_class_load_record<'py>(
    py: Python<'py>,
    worker: &WorkerExecutionClassLoad,
) -> PyResult<Bound<'py, PyAny>> {
    WorkerExecutionClassLoadRecord {
        worker_id: worker.worker_id.clone(),
        snapshot_at: worker.snapshot_at,
        budgets: record_list(py, &worker.budgets, budget_load_record)?,
    }
    .into_bound_py_any(py)
}

impl ToRecord for ExecutionClassReport {
    fn record<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        ExecutionClassReportRecord {
            checked_at: self.checked_at,
            live_window: self.live_window,
            streams: record_list(py, &self.streams, pending_stream_record)?,
            workers: record_list(py, &self.workers, worker_execution_class_load_record)?,
        }
        .into_bound_py_any(py)
    }
}

/// The history limit of a Python value, with the answer of Python's read
/// for each value (`get_worker_state_history_async`), before any connect:
/// `None` reads every retained row; an `int` that fits in 64 bits follows
/// the core's rule ([`checked_history_limit`]: 0 or less, or above
/// `WORKER_HISTORY_ROW_BOUND`, is refused). An `int` below the 64-bit range
/// and `False` are 0 or less, so the core's refusal; an `int` above the
/// range is above the bound, so the core's refusal; `True` is
/// `MONITORING_QUERY_FAILED`, not retryable (PostgreSQL refused the `LIMIT`
/// value, as Python's statement did). Another type raises `TypeError`: the public parameter is
/// `int | None`. Reads the value only.
pub fn history_limit(value: &Bound<'_, PyAny>) -> PyResult<Result<Option<i64>, BrokerFailure>> {
    if value.is_none() {
        return Ok(Ok(None));
    }
    if value.is_instance_of::<PyBool>() {
        return match value.is_truthy()? {
            true => Ok(Err(limit_statement_refused("True"))),
            false => Ok(Err(BrokerFailure::of_operation_error(
                HISTORY_SUBJECT,
                worker_history_limit_refused(&"False"),
            ))),
        };
    }
    if !value.is_instance_of::<PyInt>() {
        let type_name = value.get_type().name()?.to_string();
        return Err(PyTypeError::new_err(format!(
            "limit must be an int or None, not {type_name}"
        )));
    }
    match value.extract::<i64>() {
        Ok(limit) => Ok(checked_history_limit(Some(limit))
            .map_err(|refused| BrokerFailure::of_operation_error(HISTORY_SUBJECT, refused))),
        Err(_) => {
            let text = value.str()?.to_string();
            match value.lt(0)? {
                true => Ok(Err(BrokerFailure::of_operation_error(
                    HISTORY_SUBJECT,
                    worker_history_limit_refused(&text),
                ))),
                false => Ok(Err(BrokerFailure::of_operation_error(
                    HISTORY_SUBJECT,
                    worker_history_limit_over_bound(&text),
                ))),
            }
        }
    }
}

/// A history limit that PostgreSQL refuses as a `bigint` `LIMIT` value:
/// `MONITORING_QUERY_FAILED`, not retryable, with Python's message
/// (`get_worker_state_history failed: limit {limit} is not a bigint value`).
fn limit_statement_refused(limit: &str) -> BrokerFailure {
    BrokerFailure::of_operation_error(
        HISTORY_SUBJECT,
        DatabaseOperationError::monitoring_query_failed(
            HISTORY_SUBJECT,
            BrokerOperationSource::ArgumentUnreadable(format!(
                "limit {limit} is not a bigint value"
            )),
        ),
    )
}

/// The latest snapshot per worker.
pub async fn worker_states_operation(
    connection: BrokerConnection,
) -> WorkerReadOutcome<Vec<WorkerStateSnapshot>> {
    let broker = match connection.broker().await {
        Ok(broker) => broker,
        Err(error) => {
            return WorkerReadOutcome::Failed(connect_failed("list_worker_states", error))
        }
    };
    match broker.list_worker_states().await {
        Ok(states) => WorkerReadOutcome::Read(states),
        Err(error) => WorkerReadOutcome::Failed(BrokerFailure::of_operation_error(
            "list_worker_states",
            error,
        )),
    }
}

/// The latest snapshot of one worker.
pub async fn worker_state_operation(
    connection: BrokerConnection,
    worker_id: String,
) -> WorkerReadOutcome<WorkerStateLookup> {
    let broker = match connection.broker().await {
        Ok(broker) => broker,
        Err(error) => return WorkerReadOutcome::Failed(connect_failed("get_worker_state", error)),
    };
    match broker.get_worker_state(&worker_id).await {
        Ok(Some(snapshot)) => WorkerReadOutcome::Read(WorkerStateLookup::Found(Box::new(snapshot))),
        Ok(None) => WorkerReadOutcome::Read(WorkerStateLookup::Absent { worker_id }),
        Err(error) => {
            WorkerReadOutcome::Failed(BrokerFailure::of_operation_error("get_worker_state", error))
        }
    }
}

/// The snapshots of one worker, newest first: at most `limit`, or every
/// retained one for `None`.
pub async fn worker_state_history_operation(
    connection: BrokerConnection,
    worker_id: String,
    limit: Option<i64>,
) -> WorkerReadOutcome<Vec<WorkerStateSnapshot>> {
    let broker = match connection.broker().await {
        Ok(broker) => broker,
        Err(error) => return WorkerReadOutcome::Failed(connect_failed(HISTORY_SUBJECT, error)),
    };
    match broker.get_worker_state_history(&worker_id, limit).await {
        Ok(states) => WorkerReadOutcome::Read(states),
        Err(error) => {
            WorkerReadOutcome::Failed(BrokerFailure::of_operation_error(HISTORY_SUBJECT, error))
        }
    }
}

/// The fleet check.
pub async fn execution_class_coverage_operation(
    connection: BrokerConnection,
) -> WorkerReadOutcome<ExecutionClassCoverage> {
    let broker = match connection.broker().await {
        Ok(broker) => broker,
        Err(error) => {
            return WorkerReadOutcome::Failed(connect_failed("get_execution_class_coverage", error))
        }
    };
    match broker.get_execution_class_coverage().await {
        Ok(coverage) => WorkerReadOutcome::Read(coverage),
        Err(error) => WorkerReadOutcome::Failed(BrokerFailure::of_operation_error(
            "get_execution_class_coverage",
            error,
        )),
    }
}

/// The execution class report.
pub async fn execution_class_report_operation(
    connection: BrokerConnection,
) -> WorkerReadOutcome<ExecutionClassReport> {
    let broker = match connection.broker().await {
        Ok(broker) => broker,
        Err(error) => {
            return WorkerReadOutcome::Failed(connect_failed("get_execution_class_report", error))
        }
    };
    match broker.get_execution_class_report().await {
        Ok(report) => WorkerReadOutcome::Read(report),
        Err(error) => WorkerReadOutcome::Failed(BrokerFailure::of_operation_error(
            "get_execution_class_report",
            error,
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn limit_of(source: &str) -> Result<Option<i64>, BrokerFailure> {
        Python::initialize();
        Python::attach(|py| {
            let value = py
                .eval(&std::ffi::CString::new(source).expect("no NUL"), None, None)
                .expect("the expression evaluates");
            history_limit(&value).expect("the value reads")
        })
    }

    fn failure_of(source: &str) -> (String, String, bool) {
        match limit_of(source) {
            Ok(limit) => panic!("{source} read as {limit:?}"),
            Err(failed) => (
                failed.error.code().to_string(),
                failed.error.message().to_owned(),
                failed.error.retryable(),
            ),
        }
    }

    #[test]
    fn a_positive_limit_goes_to_the_core_and_none_reads_every_row() {
        assert_eq!(limit_of("None").ok(), Some(None));
        assert_eq!(limit_of("120").ok(), Some(Some(120)));
    }

    #[test]
    fn a_limit_of_zero_or_less_is_the_core_refusal() {
        for (source, shown) in [("0", "0"), ("-5", "-5")] {
            assert_eq!(
                failure_of(source),
                (
                    "INVALID_INPUT".to_owned(),
                    format!("limit must be positive when set, got {shown}"),
                    false
                )
            );
        }
    }

    #[test]
    fn a_limit_below_the_range_and_false_are_the_core_refusal() {
        assert_eq!(
            failure_of("-(2 ** 70)"),
            (
                "INVALID_INPUT".to_owned(),
                format!("limit must be positive when set, got {}", -(1_i128 << 70)),
                false
            )
        );
        assert_eq!(
            failure_of("False"),
            (
                "INVALID_INPUT".to_owned(),
                "limit must be positive when set, got False".to_owned(),
                false
            )
        );
    }

    #[test]
    fn a_limit_above_the_bound_is_the_core_refusal() {
        assert_eq!(limit_of("10000").ok(), Some(Some(10_000)));
        for (source, shown) in [
            ("10001", "10001".to_owned()),
            ("2 ** 70", (1_u128 << 70).to_string()),
        ] {
            assert_eq!(
                failure_of(source),
                (
                    "INVALID_INPUT".to_owned(),
                    format!("limit must be at most 10000 when set, got {shown}"),
                    false
                )
            );
        }
    }

    #[test]
    fn true_is_a_query_failure() {
        let (code, message, retryable) = failure_of("True");
        assert_eq!(code, "MONITORING_QUERY_FAILED");
        assert!(message.starts_with("get_worker_state_history failed: limit "));
        assert!(!retryable);
    }

    #[test]
    fn a_limit_of_another_type_is_a_type_error() {
        Python::initialize();
        Python::attach(|py| {
            let value = py
                .eval(c"'5'", None, None)
                .expect("the expression evaluates");
            let error = history_limit(&value).expect_err("a str is refused");
            assert!(error.is_instance_of::<PyTypeError>(py));
        });
    }
}
