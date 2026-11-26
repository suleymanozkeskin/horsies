//! The records of the health reads: worker pings, worker state snapshots,
//! the stale and expired task lists, and the class coverage and report.
//!
//! The core types of these reads have no serde form, so they cross as napi
//! records, by the boundary rules: an instant as a `BigInt` of epoch
//! microseconds; a 64-bit count as a `BigInt`; a value that may be missing
//! as a tagged variant that names the missing state, never a `null`. The
//! `recovery_config` map of a snapshot crosses as its JSON text: it is
//! untyped JSON that the worker wrote.

use chrono::TimeDelta;
use horsies_core::{
    BoundedTaskList, BudgetLoad, ClassBudget, ClassCoverage, ClassReport, ClearableBootCause,
    DownSlot, Execution, ExpiredTaskRow, PendingStream, ShortageErrno, SlotHealth, SnapshotProfile,
    StaleTaskRow, StreamCoverage, StreamCoverageState, StreamWait, WorkerClassLoad, WorkerPong,
    WorkerStateSnapshot,
};
use napi::bindgen_prelude::BigInt;
use napi_derive::napi;

use crate::values::boundary::{bigint_of_i64, bigint_of_u64, epoch_micros_of, text_of_uuid};

/// One worker's reply to a ping.
#[napi(object, object_from_js = false)]
#[derive(Clone, Debug, PartialEq)]
pub struct WorkerPongRecord {
    pub worker_id: String,
    pub hostname: String,
    pub pid: i32,
    pub round_trip_ms: f64,
}

impl From<WorkerPong> for WorkerPongRecord {
    fn from(pong: WorkerPong) -> Self {
        Self {
            worker_id: pong.worker_id,
            hostname: pong.hostname,
            pid: pong.pid,
            round_trip_ms: pong.round_trip_ms,
        }
    }
}

/// An execution class, by its wire value.
#[napi(string_enum = "UPPER_SNAKE")]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ExecutionName {
    Isolated,
    SharedAsync,
    SharedThread,
}

impl From<Execution> for ExecutionName {
    fn from(execution: Execution) -> Self {
        match execution {
            Execution::Isolated => Self::Isolated,
            Execution::SharedAsync => Self::SharedAsync,
            Execution::SharedThread => Self::SharedThread,
        }
    }
}

/// The worker profile that a snapshot names.
#[napi(
    discriminant = "kind",
    discriminant_case = "camelCase",
    object_from_js = false
)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SnapshotProfileRecord {
    Named {
        name: String,
    },
    /// A worker of a build before schema migration 1004 wrote the row.
    NotRecorded,
}

/// The errno of a resource shortage, by its name.
#[napi(string_enum = "UPPERCASE")]
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ShortageErrnoName {
    Emfile,
    Enfile,
    Enomem,
    Eagain,
}

/// A boot failure cause that can clear by itself.
#[napi(
    discriminant = "kind",
    discriminant_case = "camelCase",
    object_from_js = false
)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BootCauseRecord {
    ResourceShortage { errno: ShortageErrnoName },
    ThreadStartRefused,
}

/// A worker slot whose last child boot failed for a cause that can clear.
#[napi(object, object_from_js = false)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DownSlotRecord {
    pub slot: u32,
    pub cause: BootCauseRecord,
    pub detail: String,
    pub down_since_micros: BigInt,
    pub failed_boots: BigInt,
}

/// Whether any slot of the worker was down when it wrote the snapshot.
#[napi(
    discriminant = "kind",
    discriminant_case = "camelCase",
    object_from_js = false
)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SlotHealthRecord {
    AllSlotsServing,
    /// Never empty, each slot once, in slot order.
    SlotsDown {
        slots: Vec<DownSlotRecord>,
    },
}

/// One claim budget of a worker.
#[napi(object, object_from_js = false)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClassBudgetRecord {
    pub budget: ExecutionName,
    pub capacity: BigInt,
    pub serves: Vec<ExecutionName>,
}

/// One entry of a queue map of a snapshot.
#[napi(object, object_from_js = false)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct QueueNumberRecord {
    pub queue: String,
    pub value: BigInt,
}

/// A measure of a snapshot: memory or CPU.
#[napi(
    discriminant = "kind",
    discriminant_case = "camelCase",
    object_from_js = false
)]
#[derive(Clone, Debug, PartialEq)]
pub enum MeasureRecord {
    Measured {
        value: f64,
    },
    /// The worker could not take the measure.
    NotMeasured,
}

/// The cluster-wide cap of a worker's claims.
#[napi(
    discriminant = "kind",
    discriminant_case = "camelCase",
    object_from_js = false
)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ClusterCapRecord {
    Capped { cap: i32 },
    Uncapped,
}

/// One worker state snapshot.
#[napi(object, object_from_js = false)]
#[derive(Clone, Debug, PartialEq)]
pub struct WorkerStateRecord {
    pub worker_id: String,
    pub snapshot_at_micros: BigInt,
    pub hostname: String,
    pub pid: i32,
    pub processes: i32,
    pub max_claim_batch: i32,
    pub max_claim_per_worker: i32,
    pub cluster_wide_cap: ClusterCapRecord,
    pub queues: Vec<String>,
    /// In queue-name order.
    pub queue_priorities: Vec<QueueNumberRecord>,
    /// In queue-name order.
    pub queue_max_concurrency: Vec<QueueNumberRecord>,
    /// The JSON text of the object that the worker wrote.
    pub recovery_config_json: String,
    pub tasks_running: i32,
    pub tasks_claimed: i32,
    pub memory_usage_mb: MeasureRecord,
    pub memory_percent: MeasureRecord,
    pub cpu_percent: MeasureRecord,
    pub children_memory_mb: MeasureRecord,
    pub worker_started_at_micros: BigInt,
    pub slot_health: SlotHealthRecord,
    pub class_capacity: Vec<ClassBudgetRecord>,
    pub profile: SnapshotProfileRecord,
}

impl From<WorkerStateSnapshot> for WorkerStateRecord {
    fn from(snapshot: WorkerStateSnapshot) -> Self {
        Self {
            recovery_config_json: serde_json::Value::Object(snapshot.recovery_config).to_string(),
            worker_id: snapshot.worker_id,
            snapshot_at_micros: epoch_micros_of(snapshot.snapshot_at),
            hostname: snapshot.hostname,
            pid: snapshot.pid,
            processes: snapshot.processes,
            max_claim_batch: snapshot.max_claim_batch,
            max_claim_per_worker: snapshot.max_claim_per_worker,
            cluster_wide_cap: match snapshot.cluster_wide_cap {
                Some(cap) => ClusterCapRecord::Capped { cap },
                None => ClusterCapRecord::Uncapped,
            },
            queues: snapshot.queues,
            queue_priorities: queue_numbers(snapshot.queue_priorities),
            queue_max_concurrency: queue_numbers(snapshot.queue_max_concurrency),
            tasks_running: snapshot.tasks_running,
            tasks_claimed: snapshot.tasks_claimed,
            memory_usage_mb: measure(snapshot.memory_usage_mb),
            memory_percent: measure(snapshot.memory_percent),
            cpu_percent: measure(snapshot.cpu_percent),
            children_memory_mb: measure(snapshot.children_memory_mb),
            worker_started_at_micros: epoch_micros_of(snapshot.worker_started_at),
            slot_health: slot_health(&snapshot.slot_health),
            class_capacity: snapshot
                .class_capacity
                .as_slice()
                .iter()
                .map(class_budget)
                .collect(),
            profile: match snapshot.profile {
                SnapshotProfile::Named { name } => SnapshotProfileRecord::Named { name },
                SnapshotProfile::NotRecorded => SnapshotProfileRecord::NotRecorded,
            },
        }
    }
}

/// A text column of a stale task that may hold no value.
#[napi(
    discriminant = "kind",
    discriminant_case = "camelCase",
    object_from_js = false
)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RecordedTextRecord {
    Recorded { value: String },
    NotRecorded,
}

/// The process id column of a stale task, which may hold no value.
#[napi(
    discriminant = "kind",
    discriminant_case = "camelCase",
    object_from_js = false
)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RecordedPidRecord {
    Recorded { value: i32 },
    NotRecorded,
}

/// The latest runner beat of a stale task since its start.
#[napi(
    discriminant = "kind",
    discriminant_case = "camelCase",
    object_from_js = false
)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LastBeatRecord {
    Beat {
        at_micros: BigInt,
    },
    /// No runner beat since the start: the start writes no beat.
    NoBeatSinceStart,
}

/// A `RUNNING` task whose latest beat, or its start, is older than the
/// threshold.
#[napi(object, object_from_js = false)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StaleTaskRecord {
    pub id: String,
    pub task_name: String,
    pub worker_hostname: RecordedTextRecord,
    pub worker_pid: RecordedPidRecord,
    pub worker_process_name: RecordedTextRecord,
    pub started_at_micros: BigInt,
    pub last_heartbeat: LastBeatRecord,
}

impl From<StaleTaskRow> for StaleTaskRecord {
    fn from(row: StaleTaskRow) -> Self {
        Self {
            id: text_of_uuid(row.id),
            task_name: row.task_name,
            worker_hostname: recorded_text(row.worker_hostname),
            worker_pid: match row.worker_pid {
                Some(value) => RecordedPidRecord::Recorded { value },
                None => RecordedPidRecord::NotRecorded,
            },
            worker_process_name: recorded_text(row.worker_process_name),
            started_at_micros: epoch_micros_of(row.started_at),
            last_heartbeat: match row.last_heartbeat {
                Some(at) => LastBeatRecord::Beat {
                    at_micros: epoch_micros_of(at),
                },
                None => LastBeatRecord::NoBeatSinceStart,
            },
        }
    }
}

/// A `PENDING` task past its `good_until`.
#[napi(object, object_from_js = false)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExpiredTaskRecord {
    pub id: String,
    pub task_name: String,
    pub queue_name: String,
    pub priority: i32,
    pub sent_at_micros: BigInt,
    pub enqueued_at_micros: BigInt,
    pub good_until_micros: BigInt,
    /// How long ago `good_until` passed, on the database clock.
    pub expired_for_micros: BigInt,
}

impl From<ExpiredTaskRow> for ExpiredTaskRecord {
    fn from(row: ExpiredTaskRow) -> Self {
        Self {
            id: text_of_uuid(row.id),
            task_name: row.task_name,
            queue_name: row.queue_name,
            priority: row.priority,
            sent_at_micros: epoch_micros_of(row.sent_at),
            enqueued_at_micros: epoch_micros_of(row.enqueued_at),
            good_until_micros: epoch_micros_of(row.good_until),
            expired_for_micros: bigint_of_i64(row.expired_for_us),
        }
    }
}

/// The stale task list, and whether it stopped at its bound.
#[napi(
    discriminant = "kind",
    discriminant_case = "camelCase",
    object_from_js = false
)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StaleTaskList {
    Complete {
        rows: Vec<StaleTaskRecord>,
    },
    /// The first `bound` rows; more rows match.
    Truncated {
        rows: Vec<StaleTaskRecord>,
        bound: BigInt,
    },
}

impl From<BoundedTaskList<StaleTaskRow>> for StaleTaskList {
    fn from(list: BoundedTaskList<StaleTaskRow>) -> Self {
        match list {
            BoundedTaskList::Complete(rows) => Self::Complete {
                rows: rows.into_iter().map(StaleTaskRecord::from).collect(),
            },
            BoundedTaskList::Truncated { rows, bound } => Self::Truncated {
                rows: rows.into_iter().map(StaleTaskRecord::from).collect(),
                bound: count_of(bound),
            },
        }
    }
}

/// The expired task list, and whether it stopped at its bound.
#[napi(
    discriminant = "kind",
    discriminant_case = "camelCase",
    object_from_js = false
)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ExpiredTaskList {
    Complete {
        rows: Vec<ExpiredTaskRecord>,
    },
    /// The first `bound` rows; more rows match.
    Truncated {
        rows: Vec<ExpiredTaskRecord>,
        bound: BigInt,
    },
}

impl From<BoundedTaskList<ExpiredTaskRow>> for ExpiredTaskList {
    fn from(list: BoundedTaskList<ExpiredTaskRow>) -> Self {
        match list {
            BoundedTaskList::Complete(rows) => Self::Complete {
                rows: rows.into_iter().map(ExpiredTaskRecord::from).collect(),
            },
            BoundedTaskList::Truncated { rows, bound } => Self::Truncated {
                rows: rows.into_iter().map(ExpiredTaskRecord::from).collect(),
                bound: count_of(bound),
            },
        }
    }
}

/// Whether live workers serve a pending stream.
#[napi(
    discriminant = "kind",
    discriminant_case = "camelCase",
    object_from_js = false
)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StreamStateRecord {
    Served { workers: BigInt, capacity: BigInt },
    WithoutCapacity { workers: BigInt },
    Unserved,
}

/// The coverage of one pending stream.
#[napi(object, object_from_js = false)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StreamCoverageRecord {
    pub queue_name: String,
    pub execution: ExecutionName,
    pub state: StreamStateRecord,
}

/// The fleet check: the coverage of every pending stream.
#[napi(object, object_from_js = false)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClassCoverageRecord {
    pub checked_at_micros: BigInt,
    pub live_workers: BigInt,
    pub streams: Vec<StreamCoverageRecord>,
}

impl From<ClassCoverage> for ClassCoverageRecord {
    fn from(coverage: ClassCoverage) -> Self {
        Self {
            checked_at_micros: epoch_micros_of(coverage.checked_at),
            live_workers: count_of(coverage.live_workers),
            streams: coverage.streams.iter().map(stream_coverage).collect(),
        }
    }
}

/// How long the rows of a pending stream wait.
#[napi(
    discriminant = "kind",
    discriminant_case = "camelCase",
    object_from_js = false
)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StreamWaitRecord {
    OldestWaiting {
        enqueued_at_micros: BigInt,
        wait_micros: BigInt,
    },
    NoneDue {
        next_due_at_micros: BigInt,
    },
}

/// One pending stream.
#[napi(object, object_from_js = false)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PendingStreamRecord {
    pub queue_name: String,
    pub execution: ExecutionName,
    pub wait: StreamWaitRecord,
    pub state: StreamStateRecord,
}

/// One claim budget of a live worker, with its rows in flight.
#[napi(object, object_from_js = false)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BudgetLoadRecord {
    pub budget: ExecutionName,
    pub capacity: BigInt,
    pub serves: Vec<ExecutionName>,
    pub in_flight: BigInt,
}

/// The claim budgets of one live worker.
#[napi(object, object_from_js = false)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WorkerClassLoadRecord {
    pub worker_id: String,
    pub snapshot_at_micros: BigInt,
    pub budgets: Vec<BudgetLoadRecord>,
}

/// The class report: pending streams and the budget load of each live
/// worker.
#[napi(object, object_from_js = false)]
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClassReportRecord {
    pub checked_at_micros: BigInt,
    pub streams: Vec<PendingStreamRecord>,
    pub workers: Vec<WorkerClassLoadRecord>,
}

impl From<ClassReport> for ClassReportRecord {
    fn from(report: ClassReport) -> Self {
        Self {
            checked_at_micros: epoch_micros_of(report.checked_at),
            streams: report.streams.iter().map(pending_stream).collect(),
            workers: report.workers.iter().map(worker_class_load).collect(),
        }
    }
}

/// A `usize` count as a `BigInt`. Pure.
fn count_of(count: usize) -> BigInt {
    match u64::try_from(count) {
        Ok(count) => bigint_of_u64(count),
        Err(_) => unreachable!("a usize fits in a u64 on every target of the addon"),
    }
}

/// The entries of a queue map, in queue-name order. Pure.
fn queue_numbers(map: std::collections::BTreeMap<String, i64>) -> Vec<QueueNumberRecord> {
    map.into_iter()
        .map(|(queue, value)| QueueNumberRecord {
            queue,
            value: bigint_of_i64(value),
        })
        .collect()
}

/// The record of a measure that may be missing. Pure.
fn measure(value: Option<f64>) -> MeasureRecord {
    match value {
        Some(value) => MeasureRecord::Measured { value },
        None => MeasureRecord::NotMeasured,
    }
}

/// The record of a text column that may hold no value. Pure.
fn recorded_text(value: Option<String>) -> RecordedTextRecord {
    match value {
        Some(value) => RecordedTextRecord::Recorded { value },
        None => RecordedTextRecord::NotRecorded,
    }
}

/// The record of a slot health. Pure.
fn slot_health(health: &SlotHealth) -> SlotHealthRecord {
    match health {
        SlotHealth::AllSlotsServing => SlotHealthRecord::AllSlotsServing,
        SlotHealth::SlotsDown(down) => SlotHealthRecord::SlotsDown {
            slots: down.as_slice().iter().map(down_slot).collect(),
        },
    }
}

/// The record of a down slot. Pure.
fn down_slot(slot: &DownSlot) -> DownSlotRecord {
    DownSlotRecord {
        slot: slot.slot,
        cause: boot_cause(slot.cause),
        detail: slot.detail.clone(),
        down_since_micros: epoch_micros_of(slot.down_since),
        failed_boots: bigint_of_u64(slot.failed_boots.get()),
    }
}

/// The record of a boot cause. Pure.
const fn boot_cause(cause: ClearableBootCause) -> BootCauseRecord {
    match cause {
        ClearableBootCause::ResourceShortage(errno) => BootCauseRecord::ResourceShortage {
            errno: match errno {
                ShortageErrno::Emfile => ShortageErrnoName::Emfile,
                ShortageErrno::Enfile => ShortageErrnoName::Enfile,
                ShortageErrno::Enomem => ShortageErrnoName::Enomem,
                ShortageErrno::Eagain => ShortageErrnoName::Eagain,
            },
        },
        ClearableBootCause::ThreadStartRefused => BootCauseRecord::ThreadStartRefused,
    }
}

/// The record of a claim budget. Pure.
fn class_budget(budget: &ClassBudget) -> ClassBudgetRecord {
    ClassBudgetRecord {
        budget: budget.budget.into(),
        capacity: bigint_of_u64(budget.capacity),
        serves: executions(&budget.serves),
    }
}

/// The names of execution classes, in order. Pure.
fn executions(classes: &[Execution]) -> Vec<ExecutionName> {
    classes
        .iter()
        .map(|class| ExecutionName::from(*class))
        .collect()
}

/// The record of a stream state. Pure.
fn stream_state(state: StreamCoverageState) -> StreamStateRecord {
    match state {
        StreamCoverageState::Served { workers, capacity } => StreamStateRecord::Served {
            workers: count_of(workers),
            capacity: bigint_of_u64(capacity),
        },
        StreamCoverageState::WithoutCapacity { workers } => StreamStateRecord::WithoutCapacity {
            workers: count_of(workers),
        },
        StreamCoverageState::Unserved => StreamStateRecord::Unserved,
    }
}

/// The record of the coverage of one stream. Pure.
fn stream_coverage(stream: &StreamCoverage) -> StreamCoverageRecord {
    StreamCoverageRecord {
        queue_name: stream.queue_name.clone(),
        execution: stream.execution.into(),
        state: stream_state(stream.state),
    }
}

/// The record of a stream wait. Pure.
fn stream_wait(wait: StreamWait) -> StreamWaitRecord {
    match wait {
        StreamWait::OldestWaiting { enqueued_at, wait } => StreamWaitRecord::OldestWaiting {
            enqueued_at_micros: epoch_micros_of(enqueued_at),
            wait_micros: bigint_of_i64(wait_microseconds(wait)),
        },
        StreamWait::NoneDue { next_due_at } => StreamWaitRecord::NoneDue {
            next_due_at_micros: epoch_micros_of(next_due_at),
        },
    }
}

/// The microseconds of a wait. A wait between two database instants holds
/// fewer than 2^63 microseconds, so the count does not overflow. Pure.
fn wait_microseconds(wait: TimeDelta) -> i64 {
    match wait.num_microseconds() {
        Some(microseconds) => microseconds,
        None => unreachable!("a wait between two database instants is below 2^63 microseconds"),
    }
}

/// The record of a pending stream. Pure.
fn pending_stream(stream: &PendingStream) -> PendingStreamRecord {
    PendingStreamRecord {
        queue_name: stream.queue_name.clone(),
        execution: stream.execution.into(),
        wait: stream_wait(stream.wait),
        state: stream_state(stream.state),
    }
}

/// The record of the load of one live worker. Pure.
fn worker_class_load(load: &WorkerClassLoad) -> WorkerClassLoadRecord {
    WorkerClassLoadRecord {
        worker_id: load.worker_id.clone(),
        snapshot_at_micros: epoch_micros_of(load.snapshot_at),
        budgets: load.budgets.iter().map(budget_load).collect(),
    }
}

/// The record of the load of one budget. Pure.
fn budget_load(load: &BudgetLoad) -> BudgetLoadRecord {
    BudgetLoadRecord {
        budget: load.budget.into(),
        capacity: bigint_of_u64(load.capacity),
        serves: executions(&load.serves),
        in_flight: bigint_of_u64(load.in_flight),
    }
}

#[cfg(test)]
mod tests {
    use chrono::{TimeZone, Utc};
    use uuid::Uuid;

    use super::*;

    fn instant() -> chrono::DateTime<Utc> {
        Utc.with_ymd_and_hms(2026, 11, 25, 12, 0, 0)
            .single()
            .expect("a valid instant")
    }

    fn stale_row(last_heartbeat: Option<chrono::DateTime<Utc>>) -> StaleTaskRow {
        StaleTaskRow {
            id: Uuid::from_u128(9),
            task_name: "resize".to_owned(),
            worker_hostname: None,
            worker_pid: Some(41),
            worker_process_name: Some("child-1".to_owned()),
            started_at: instant(),
            last_heartbeat,
        }
    }

    #[test]
    fn a_pong_keeps_its_fields() {
        let record = WorkerPongRecord::from(WorkerPong {
            worker_id: "w1".to_owned(),
            hostname: "h".to_owned(),
            pid: 7,
            round_trip_ms: 1.5,
        });
        assert_eq!(record.worker_id, "w1");
        assert_eq!(record.pid, 7);
        assert!((record.round_trip_ms - 1.5).abs() < f64::EPSILON);
    }

    #[test]
    fn each_execution_class_has_its_name() {
        assert_eq!(
            executions(&[
                Execution::Isolated,
                Execution::SharedAsync,
                Execution::SharedThread
            ]),
            [
                ExecutionName::Isolated,
                ExecutionName::SharedAsync,
                ExecutionName::SharedThread
            ]
        );
    }

    #[test]
    fn a_missing_column_of_a_stale_row_is_not_recorded() {
        let record = StaleTaskRecord::from(stale_row(None));
        assert_eq!(record.worker_hostname, RecordedTextRecord::NotRecorded);
        assert_eq!(record.worker_pid, RecordedPidRecord::Recorded { value: 41 });
        assert_eq!(
            record.worker_process_name,
            RecordedTextRecord::Recorded {
                value: "child-1".to_owned()
            }
        );
        assert_eq!(record.last_heartbeat, LastBeatRecord::NoBeatSinceStart);
        assert_eq!(record.started_at_micros, epoch_micros_of(instant()));
    }

    #[test]
    fn a_stale_row_with_a_beat_keeps_its_instant() {
        let record = StaleTaskRecord::from(stale_row(Some(instant())));
        assert_eq!(
            record.last_heartbeat,
            LastBeatRecord::Beat {
                at_micros: epoch_micros_of(instant())
            }
        );
    }

    #[test]
    fn a_truncated_list_keeps_its_rows_and_bound() {
        let list = StaleTaskList::from(BoundedTaskList::Truncated {
            rows: vec![stale_row(None)],
            bound: 10_000,
        });
        match list {
            StaleTaskList::Truncated { rows, bound } => {
                assert_eq!(rows.len(), 1);
                assert_eq!(bound, bigint_of_u64(10_000));
            }
            StaleTaskList::Complete { .. } => panic!("a truncated list"),
        }
    }

    #[test]
    fn an_expired_row_keeps_its_interval_in_microseconds() {
        let record = ExpiredTaskRecord::from(ExpiredTaskRow {
            id: Uuid::from_u128(4),
            task_name: "send".to_owned(),
            queue_name: "default".to_owned(),
            priority: 100,
            sent_at: instant(),
            enqueued_at: instant(),
            good_until: instant(),
            expired_for_us: 1_500_000,
        });
        assert_eq!(record.expired_for_micros, bigint_of_i64(1_500_000));
        let complete = ExpiredTaskList::from(BoundedTaskList::Complete(Vec::new()));
        assert_eq!(complete, ExpiredTaskList::Complete { rows: Vec::new() });
    }

    #[test]
    fn each_stream_state_has_its_record() {
        assert_eq!(
            stream_state(StreamCoverageState::Served {
                workers: 2,
                capacity: 8
            }),
            StreamStateRecord::Served {
                workers: count_of(2),
                capacity: bigint_of_u64(8)
            }
        );
        assert_eq!(
            stream_state(StreamCoverageState::WithoutCapacity { workers: 1 }),
            StreamStateRecord::WithoutCapacity {
                workers: count_of(1)
            }
        );
        assert_eq!(
            stream_state(StreamCoverageState::Unserved),
            StreamStateRecord::Unserved
        );
    }

    #[test]
    fn each_stream_wait_has_its_record() {
        let waiting = stream_wait(StreamWait::OldestWaiting {
            enqueued_at: instant(),
            wait: TimeDelta::milliseconds(1_250),
        });
        assert_eq!(
            waiting,
            StreamWaitRecord::OldestWaiting {
                enqueued_at_micros: epoch_micros_of(instant()),
                wait_micros: bigint_of_i64(1_250_000),
            }
        );
        assert_eq!(
            stream_wait(StreamWait::NoneDue {
                next_due_at: instant()
            }),
            StreamWaitRecord::NoneDue {
                next_due_at_micros: epoch_micros_of(instant())
            }
        );
    }

    #[test]
    fn each_boot_cause_has_its_record() {
        assert_eq!(
            boot_cause(ClearableBootCause::ResourceShortage(ShortageErrno::Enomem)),
            BootCauseRecord::ResourceShortage {
                errno: ShortageErrnoName::Enomem
            }
        );
        assert_eq!(
            boot_cause(ClearableBootCause::ThreadStartRefused),
            BootCauseRecord::ThreadStartRefused
        );
        let names: Vec<ShortageErrnoName> = ShortageErrno::ALL
            .into_iter()
            .map(
                |errno| match boot_cause(ClearableBootCause::ResourceShortage(errno)) {
                    BootCauseRecord::ResourceShortage { errno } => errno,
                    BootCauseRecord::ThreadStartRefused => panic!("a shortage"),
                },
            )
            .collect();
        assert_eq!(
            names,
            [
                ShortageErrnoName::Emfile,
                ShortageErrnoName::Enfile,
                ShortageErrnoName::Enomem,
                ShortageErrnoName::Eagain
            ]
        );
    }

    #[test]
    fn a_missing_measure_is_not_measured() {
        assert_eq!(measure(None), MeasureRecord::NotMeasured);
        assert_eq!(measure(Some(2.0)), MeasureRecord::Measured { value: 2.0 });
    }

    #[test]
    fn queue_numbers_keep_queue_order_and_every_digit() {
        let map =
            std::collections::BTreeMap::from([("b".to_owned(), i64::MAX), ("a".to_owned(), 5)]);
        assert_eq!(
            queue_numbers(map),
            [
                QueueNumberRecord {
                    queue: "a".to_owned(),
                    value: bigint_of_i64(5)
                },
                QueueNumberRecord {
                    queue: "b".to_owned(),
                    value: bigint_of_i64(i64::MAX)
                },
            ]
        );
    }
}
