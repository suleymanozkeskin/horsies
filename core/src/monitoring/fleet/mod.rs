//! The fleet state: the facts of the whole fleet that the fleet collector
//! serves as gauges, read from PostgreSQL by [`read_fleet_state`].
//!
//! The read runs one statement per query of the collector
//! ([`FleetQuery`], the `query` label of its own metrics), one after another
//! on the main core pool: each takes one connection, and the read never
//! holds two. Each statement is one autocommit read, bounded by
//! [`FLEET_QUERY_TIMEOUT`]. A statement that fails leaves only its own part
//! without an answer; the other parts still answer.
//!
//! Every list in the record has a named bound. Every label value is a closed
//! enum ([`Execution`], [`HeldStatus`], [`LastRun`], and the label types of
//! [`crate::telemetry::labels`] through [`LiveWorkerCounts::of`],
//! [`WorkflowCounts::of`] and [`HeldStatus::live_status`]) or a stored name
//! (a queue, a schedule) whose count is bounded.
//!
//! Index use, checked by `EXPLAIN (ANALYZE, BUFFERS)` at 1,274,367 `PENDING`
//! rows, in a steady state and under load:
//! - queue tasks: the `PENDING` streams and their counts on
//!   `horsies_tasks_claim_pending`; the `CLAIMED` and `RUNNING` rows on
//!   `horsies_tasks_status`;
//! - oldest pending: one probe of `horsies_tasks_claim_pending` per
//!   (queue, class, priority);
//! - workers: `horsies_worker_states_snapshot_at`, then
//!   `horsies_tasks_status`;
//! - class coverage: the fleet check statement
//!   (`crate::broker::class_coverage_reads`);
//! - workflows: the partial indexes of `RUNNING` and `PAUSED` workflows;
//! - schedules: the primary key of `horsies_schedule_state`;
//! - schema version: the primary key of `horsies_migrations`.
//!
//! [`FleetCollector`] holds the fleet state as gauges and renders their
//! Prometheus text; [`collect_fleet_state`] reads the state at most once per
//! [`FLEET_COLLECT_INTERVAL`] and records each read.

mod collector;
mod decode;
mod gauges;
mod read;
#[cfg(feature = "metrics-endpoint")]
mod serve;
mod statements;

#[cfg(test)]
mod tests;

use std::time::Duration;

use chrono::{DateTime, TimeDelta, Utc};

use crate::core::task::Execution;
use crate::db::Transience;
use crate::telemetry::labels::{
    FleetQuery, FleetWorkerState, LiveTaskStatus, LiveWorkflowStatus, TelemetryPart,
};
use crate::{ExecutionClassCoverage, StreamWait, PENDING_STREAM_LIMIT};

#[cfg(feature = "web")]
pub(crate) use collector::log_recorded;
pub use collector::{
    collect_fleet_state, collect_fleet_state_through, FleetCollector, FleetLabels, FleetRecorded,
    FLEET_COLLECTOR_METRICS, FLEET_COLLECT_INTERVAL,
};
pub use read::{read_fleet_state, read_fleet_state_through};
#[cfg(feature = "metrics-endpoint")]
pub use serve::serve_fleet_metrics;

/// The bound of each statement of the fleet read. After it the core sends a
/// cancel request; the part answers [`FleetQueryFailure::TimedOut`].
pub const FLEET_QUERY_TIMEOUT: Duration = Duration::from_secs(2);

/// The most `PENDING` rows of one (queue, class) that the count reads. A
/// stream with more rows is [`PendingCount::AtLeastLimit`].
///
/// The count reads the index entries of the stream and, for each entry on a
/// heap page that is not all-visible, the heap row. Under load nearly every
/// page of recent rows is not all-visible, so the cost of the queue tasks
/// statement is about one buffer per counted row: at most
/// `PENDING_COUNT_LIMIT` per stream.
pub const PENDING_COUNT_LIMIT: u64 = 1_000;

/// The most (status, queue, class) groups of `CLAIMED` and `RUNNING` rows
/// that the queue tasks read takes: both statuses of
/// [`PENDING_STREAM_LIMIT`] streams.
pub const HELD_GROUP_LIMIT: usize = 2 * PENDING_STREAM_LIMIT;

/// The most schedules that the schedule read takes.
pub const SCHEDULE_LIMIT: usize = 10_000;

/// A part of the fleet read that has no answer, and the query it belongs to.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("fleet query {query:?}: {failure}")]
pub struct FleetQueryError {
    pub query: FleetQuery,
    pub failure: FleetQueryFailure,
}

/// Why a statement of the fleet read has no answer. Every statement only
/// reads, so after each failure nothing changed in the database.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum FleetQueryFailure {
    /// The statement ran past `bound` ([`FLEET_QUERY_TIMEOUT`]); the core
    /// sent a cancel request, and the server cancelled the statement or gave
    /// no reply within the cancel grace. The next read may answer.
    #[error("no answer within {bound:?}")]
    TimedOut { bound: Duration },
    /// The database did not run the statement: the pool could not give a
    /// connection, the connection failed, or the server refused it.
    /// `transience` says whether the cause may clear.
    #[error("the database did not answer ({transience:?}): {detail}")]
    DatabaseFailed {
        transience: Transience,
        detail: String,
    },
    /// The answer holds stored data that the read cannot decide on, or a
    /// part past its bound. The same rows give the same answer.
    #[error("refused: {reason}")]
    Refused { reason: String },
}

/// One part of the fleet read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FleetPart<T> {
    /// The answer, or why the part has none.
    pub outcome: Result<T, FleetQueryError>,
    /// How long the statement and its decode took, on the monotonic clock.
    pub took: Duration,
}

/// The fleet state: one part per [`FleetQuery`], in its order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FleetState {
    pub queue_tasks: FleetPart<QueueTasks>,
    pub oldest_pending: FleetPart<OldestPending>,
    pub workers: FleetPart<FleetWorkers>,
    /// Whether a live worker serves each pending (queue, class): the fleet
    /// check, the read of `crate::PostgresBroker::get_execution_class_coverage`.
    pub class_coverage: FleetPart<ExecutionClassCoverage>,
    pub workflows: FleetPart<WorkflowCounts>,
    pub schedules: FleetPart<ScheduleRuns>,
    pub schema_version: FleetPart<SchemaVersion>,
}

/// The live rows of `horsies_tasks` per queue, class and status.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueueTasks {
    /// Each (queue, class) with `PENDING` rows, due or due later, ordered
    /// by queue name, then class. At most [`PENDING_STREAM_LIMIT`].
    pub pending: Vec<PendingTasks>,
    /// Each (status, queue, class) with `CLAIMED` or `RUNNING` rows,
    /// ordered by status, queue name, class. At most [`HELD_GROUP_LIMIT`].
    pub held: Vec<HeldTasks>,
}

/// The `PENDING` rows of one (queue, class).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingTasks {
    pub queue_name: String,
    pub execution: Execution,
    pub count: PendingCount,
}

/// The count of the `PENDING` rows of one (queue, class).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PendingCount {
    /// The stream holds this many rows: 1 to [`PENDING_COUNT_LIMIT`].
    Exact(u64),
    /// The stream holds more than [`PENDING_COUNT_LIMIT`] rows. The read
    /// stopped there.
    AtLeastLimit,
}

/// The status of a row that a worker holds.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum HeldStatus {
    Claimed,
    Running,
}

impl HeldStatus {
    /// The value of the `status` label of `horsies.queue.tasks`.
    pub fn live_status(self) -> LiveTaskStatus {
        match self {
            Self::Claimed => LiveTaskStatus::Claimed,
            Self::Running => LiveTaskStatus::Running,
        }
    }
}

/// The `CLAIMED` or `RUNNING` rows of one (queue, class).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeldTasks {
    pub status: HeldStatus,
    pub queue_name: String,
    pub execution: Execution,
    /// 1 or more.
    pub count: u64,
}

/// The wait of the oldest due row of each pending (queue, class), read at
/// `checked_at` (database time).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OldestPending {
    pub checked_at: DateTime<Utc>,
    /// Ordered by queue name, then class. At most one entry per (queue,
    /// class, priority) group of
    /// [`PENDING_HEAD_LIMIT`](crate::PENDING_HEAD_LIMIT).
    pub streams: Vec<StreamOldest>,
}

/// The wait of the oldest due row of one pending (queue, class).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamOldest {
    pub queue_name: String,
    pub execution: Execution,
    /// `Due` with the wait of the oldest due row, or `NoneDue` when
    /// every row is due later.
    pub wait: StreamWait,
}

/// The live workers and their seats, read at `checked_at` (database time).
///
/// A worker is live while its latest state snapshot is at most
/// `WORKER_STATE_LIVE_WINDOW_SECONDS` old.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FleetWorkers {
    pub checked_at: DateTime<Utc>,
    /// The live workers by slot health.
    pub workers: LiveWorkerCounts,
    /// The seats of the live workers, per budget class.
    pub seats: ClassSeats,
    /// The live workers whose telemetry parts are down, per part.
    pub telemetry_down: TelemetryDownCounts,
}

/// The live workers by slot health.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LiveWorkerCounts {
    /// No slot was down at the worker's latest snapshot.
    pub serving: u64,
    /// At least one slot was down at the worker's latest snapshot.
    pub degraded: u64,
}

impl LiveWorkerCounts {
    /// The live workers in `state` (the `state` label of
    /// `horsies.fleet.workers`).
    pub fn of(&self, state: FleetWorkerState) -> u64 {
        match state {
            FleetWorkerState::Serving => self.serving,
            FleetWorkerState::Degraded => self.degraded,
        }
    }
}

/// The live workers whose latest snapshot records a telemetry part as down
/// (`"state": "down"` in its member of the `telemetry` column). A snapshot
/// with another state of the part, or with no member of it, does not count.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TelemetryDownCounts {
    /// The metrics endpoint is down.
    pub endpoint: u64,
    /// The handler child is down.
    pub handler_child: u64,
}

impl TelemetryDownCounts {
    /// The live workers whose `part` is down (the `part` label of
    /// `horsies.fleet.workers.telemetry_down`).
    pub fn of(&self, part: TelemetryPart) -> u64 {
        match part {
            TelemetryPart::Endpoint => self.endpoint,
            TelemetryPart::HandlerChild => self.handler_child,
        }
    }
}

/// The seats of the live workers for each budget class; every class has an
/// entry, 0 when no live worker has a budget of it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ClassSeats {
    pub isolated: SeatCount,
    pub shared_async: SeatCount,
    pub shared_thread: SeatCount,
}

impl ClassSeats {
    /// The seats of the budgets of `execution`.
    pub fn of(&self, execution: Execution) -> SeatCount {
        match execution {
            Execution::Isolated => self.isolated,
            Execution::SharedAsync => self.shared_async,
            Execution::SharedThread => self.shared_thread,
        }
    }
}

/// The seats of the budgets of one class, summed over the live workers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct SeatCount {
    /// The children (`ISOLATED`) or seats (a shared class) in service, as
    /// each worker's latest snapshot recorded them.
    pub seats: u64,
    /// The seats that hold a row: for each worker, the rows that the claim
    /// counts against the budget (`RUNNING`, and `CLAIMED` with a live
    /// lease), at most the budget's seats. A row past the seats waits in the
    /// worker's prefetch buffer and holds no seat.
    pub in_use: u64,
}

/// The workflows that are not terminal. A workflow row is written
/// `RUNNING` and moves only between `RUNNING` and `PAUSED` before its end.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WorkflowCounts {
    pub running: u64,
    pub paused: u64,
}

impl WorkflowCounts {
    /// The workflows in `status` (the `status` label of
    /// `horsies.workflows`).
    pub fn of(&self, status: LiveWorkflowStatus) -> u64 {
        match status {
            LiveWorkflowStatus::Running => self.running,
            LiveWorkflowStatus::Paused => self.paused,
        }
    }
}

/// The last run of each schedule, read at `checked_at` (database time).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScheduleRuns {
    pub checked_at: DateTime<Utc>,
    /// Ordered by schedule name. At most [`SCHEDULE_LIMIT`]. A schedule that
    /// left the config keeps its row, so it stays in this list.
    pub schedules: Vec<ScheduleLastRun>,
}

/// The last run of one schedule.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScheduleLastRun {
    pub schedule_name: String,
    pub last_run: LastRun,
}

/// Whether a schedule ran.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LastRun {
    /// The last run was recorded at `at`, `age` before the read. The fire
    /// writes `at` from the database clock, so `age` is 0 or more.
    RanAt { at: DateTime<Utc>, age: TimeDelta },
    /// No run is recorded.
    NeverRan,
}

/// The newest successful migration that `horsies_migrations` records.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SchemaVersion {
    pub version: i64,
}
