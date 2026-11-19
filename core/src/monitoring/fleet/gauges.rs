//! The series of the fleet gauges: each part of a [`FleetState`] as the
//! label sets and values of its metrics. Pure.
//!
//! A queue or a schedule name is its own label value when the collector's
//! labels declare it; every other name is the fallback value `(unknown)`.
//! One fallback series stands for every name outside the declared names, so
//! its value combines theirs: counts add up, ages take the largest.
//!
//! - `horsies.queue.tasks{queue, execution, status}`: the `CLAIMED` and
//!   `RUNNING` rows, and the `PENDING` rows of each (queue, class) whose
//!   count is exact.
//! - `horsies.queue.pending.more_than{queue, execution}`: each (queue,
//!   class) whose `PENDING` count passed [`PENDING_COUNT_LIMIT`]. The value
//!   is a lower bound: the count is more than it. Such a (queue, class) has
//!   no `PENDING` series in `horsies.queue.tasks`, so no series shows the
//!   limit as a count.
//! - `horsies.queue.oldest_pending.age{queue, execution}`: the wait of the
//!   oldest due row, in seconds. A (queue, class) whose rows are all due
//!   later has no series: no row waits.
//! - `horsies.fleet.workers{state}`, `horsies.fleet.seats{execution}`,
//!   `horsies.fleet.seats.in_use{execution}`: one series per state and
//!   class, 0 included.
//! - `horsies.fleet.unserved_classes{queue, execution}`: one series per
//!   pending (queue, class); 1 when no live worker serves it, else 0. A
//!   (queue, class) whose serving workers have every seat of the class down
//!   (`WithoutCapacity`) is served: it shows in `horsies.fleet.workers` and
//!   the seats.
//! - `horsies.workflows{status}`: one series per status, 0 included.
//! - `horsies.schedule.last_run.age{schedule}`: the time since the last run,
//!   in seconds. A schedule that never ran has no series.
//! - `horsies.schema.version`: the newest successful migration.

use std::collections::HashMap;

use crate::core::task::Execution;
use crate::telemetry::declared::{DeclaredNames, DeclaredValue, QueueName, ScheduleName};
use crate::telemetry::labels::{FleetWorkerState, LiveTaskStatus, LiveWorkflowStatus};
use crate::telemetry::registry::{LabelSet, SeriesKey};
use crate::{ClassCoverage, StreamCoverageState, StreamWait};

use super::{
    FleetWorkers, LastRun, OldestPending, PendingCount, QueueTasks, ScheduleRuns, SchemaVersion,
    WorkflowCounts, PENDING_COUNT_LIMIT,
};

/// The value of the `queue` label.
pub(crate) type QueueValue = DeclaredValue<QueueName>;

/// The labels of `horsies.queue.tasks`.
pub(crate) type QueueTaskLabels = (QueueValue, Execution, LiveTaskStatus);

/// The labels of a (queue, class) gauge.
pub(crate) type StreamLabels = (QueueValue, Execution);

/// The labels of `horsies.schedule.last_run.age`.
pub(crate) type ScheduleLabels = (DeclaredValue<ScheduleName>,);

/// The series of the queue tasks part.
#[derive(Debug, Default)]
pub(crate) struct QueueTaskSeries {
    /// `horsies.queue.tasks`.
    pub(crate) tasks: Vec<(QueueTaskLabels, f64)>,
    /// `horsies.queue.pending.more_than`.
    pub(crate) pending_more_than: Vec<(StreamLabels, f64)>,
}

/// The series of the workers part.
#[derive(Debug, Default)]
pub(crate) struct WorkerSeries {
    /// `horsies.fleet.workers`.
    pub(crate) workers: Vec<((FleetWorkerState,), f64)>,
    /// `horsies.fleet.seats`.
    pub(crate) seats: Vec<((Execution,), f64)>,
    /// `horsies.fleet.seats.in_use`.
    pub(crate) seats_in_use: Vec<((Execution,), f64)>,
}

/// The `PENDING` rows of the (queue, class) streams that one series stands
/// for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PendingTotal {
    /// Every stream was counted to its end: this many rows.
    Exact(u64),
    /// One or more streams passed [`PENDING_COUNT_LIMIT`]: more than this
    /// many rows.
    MoreThan(u64),
}

impl PendingTotal {
    fn of(count: PendingCount) -> Self {
        match count {
            PendingCount::Exact(rows) => Self::Exact(rows),
            PendingCount::AtLeastLimit => Self::MoreThan(PENDING_COUNT_LIMIT),
        }
    }

    /// The rows of two sets of streams. A sum with a lower bound is a lower
    /// bound.
    fn plus(self, other: Self) -> Self {
        match (self, other) {
            (Self::Exact(left), Self::Exact(right)) => Self::Exact(left.saturating_add(right)),
            (Self::Exact(left), Self::MoreThan(right))
            | (Self::MoreThan(left), Self::Exact(right))
            | (Self::MoreThan(left), Self::MoreThan(right)) => {
                Self::MoreThan(left.saturating_add(right))
            }
        }
    }
}

/// One entry per label set of `series`, in the order of its first entry;
/// the values of a label set given more than once are combined by `merge`.
fn combined<L: LabelSet, V: Copy>(
    series: impl IntoIterator<Item = (L, V)>,
    merge: fn(V, V) -> V,
) -> Vec<(L, V)> {
    let mut positions: HashMap<SeriesKey, usize> = HashMap::new();
    let mut entries: Vec<(L, V)> = Vec::new();
    for (labels, value) in series {
        match positions.get(&labels.key()).copied() {
            Some(position) => match entries.get_mut(position) {
                Some(entry) => entry.1 = merge(entry.1, value),
                None => unreachable!("a position points at an entry of this list"),
            },
            None => {
                let _first = positions.insert(labels.key(), entries.len());
                entries.push((labels, value));
            }
        }
    }
    entries
}

/// A count as a gauge value.
fn count_value(count: u64) -> f64 {
    count as f64
}

/// The series of the queue tasks part.
pub(crate) fn queue_task_series(
    tasks: &QueueTasks,
    queues: &DeclaredNames<QueueName>,
) -> QueueTaskSeries {
    let pending = combined(
        tasks.pending.iter().map(|stream| {
            (
                (queues.value_of(&stream.queue_name), stream.execution),
                PendingTotal::of(stream.count),
            )
        }),
        PendingTotal::plus,
    );
    let held = combined(
        tasks.held.iter().map(|group| {
            (
                (
                    queues.value_of(&group.queue_name),
                    group.execution,
                    group.status.live_status(),
                ),
                group.count,
            )
        }),
        u64::saturating_add,
    );
    let mut series = QueueTaskSeries::default();
    for ((queue, execution), total) in pending {
        match total {
            PendingTotal::Exact(rows) => series.tasks.push((
                (queue, execution, LiveTaskStatus::Pending),
                count_value(rows),
            )),
            PendingTotal::MoreThan(rows) => series
                .pending_more_than
                .push(((queue, execution), count_value(rows))),
        }
    }
    series.tasks.extend(
        held.into_iter()
            .map(|(labels, rows)| (labels, count_value(rows))),
    );
    series
}

/// The series of `horsies.queue.oldest_pending.age`, in seconds.
pub(crate) fn oldest_pending_series(
    oldest: &OldestPending,
    queues: &DeclaredNames<QueueName>,
) -> Vec<(StreamLabels, f64)> {
    let waits = oldest
        .streams
        .iter()
        .filter_map(|stream| match stream.wait {
            StreamWait::OldestWaiting { wait, .. } => Some((
                (queues.value_of(&stream.queue_name), stream.execution),
                wait,
            )),
            StreamWait::NoneDue { .. } => None,
        });
    combined(waits, std::cmp::max)
        .into_iter()
        .map(|(labels, wait)| (labels, wait.as_seconds_f64()))
        .collect()
}

/// The series of the workers part.
pub(crate) fn worker_series(workers: &FleetWorkers) -> WorkerSeries {
    WorkerSeries {
        workers: FleetWorkerState::ALL
            .into_iter()
            .map(|state| ((state,), count_value(workers.workers.of(state))))
            .collect(),
        seats: Execution::ALL
            .into_iter()
            .map(|execution| ((execution,), count_value(workers.seats.of(execution).seats)))
            .collect(),
        seats_in_use: Execution::ALL
            .into_iter()
            .map(|execution| {
                (
                    (execution,),
                    count_value(workers.seats.of(execution).in_use),
                )
            })
            .collect(),
    }
}

/// The unserved classes that one coverage state counts.
fn unserved_count(state: StreamCoverageState) -> u64 {
    match state {
        StreamCoverageState::Unserved => 1,
        StreamCoverageState::Served { .. } | StreamCoverageState::WithoutCapacity { .. } => 0,
    }
}

/// The series of `horsies.fleet.unserved_classes`.
pub(crate) fn unserved_series(
    coverage: &ClassCoverage,
    queues: &DeclaredNames<QueueName>,
) -> Vec<(StreamLabels, f64)> {
    combined(
        coverage.streams.iter().map(|stream| {
            (
                (queues.value_of(&stream.queue_name), stream.execution),
                unserved_count(stream.state),
            )
        }),
        u64::saturating_add,
    )
    .into_iter()
    .map(|(labels, count)| (labels, count_value(count)))
    .collect()
}

/// The series of `horsies.workflows`.
pub(crate) fn workflow_series(workflows: &WorkflowCounts) -> Vec<((LiveWorkflowStatus,), f64)> {
    LiveWorkflowStatus::ALL
        .into_iter()
        .map(|status| ((status,), count_value(workflows.of(status))))
        .collect()
}

/// The series of `horsies.schedule.last_run.age`, in seconds.
pub(crate) fn schedule_series(
    runs: &ScheduleRuns,
    schedules: &DeclaredNames<ScheduleName>,
) -> Vec<(ScheduleLabels, f64)> {
    let ages = runs
        .schedules
        .iter()
        .filter_map(|schedule| match schedule.last_run {
            LastRun::RanAt { age, .. } => {
                Some(((schedules.value_of(&schedule.schedule_name),), age))
            }
            LastRun::NeverRan => None,
        });
    combined(ages, std::cmp::max)
        .into_iter()
        .map(|(labels, age)| (labels, age.as_seconds_f64()))
        .collect()
}

/// The series of `horsies.schema.version`.
pub(crate) fn schema_version_series(version: &SchemaVersion) -> Vec<((), f64)> {
    // A migration version is a small positive number; the gauge holds it
    // exactly up to 2^53.
    vec![((), version.version as f64)]
}
