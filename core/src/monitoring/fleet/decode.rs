//! The parts of the fleet record from the decoded statement rows. Pure: no
//! I/O.

use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use serde::Deserialize;
use serde_json::Value;

use crate::broker::class_coverage::{budget_loads, stream_wait, ClassReadRefused, LiveWorker};
use crate::broker::class_coverage_reads::{
    coverage_from_row, decoded_part, in_flight_rows, live_workers, pending_heads, stored_execution,
    CoverageRow,
};
use crate::broker::core_rows::{column, one_row, RowDecodeError};
use crate::broker::slot_health::parse_slot_health;
use crate::core::task::Execution;
use crate::db::Row;
use crate::{BudgetLoad, ClassCoverage, SlotHealth, PENDING_STREAM_LIMIT};

use super::statements::{OLDEST_PENDING, WORKERS};
use super::{
    ClassSeats, FleetWorkers, HeldStatus, HeldTasks, LastRun, LiveWorkerCounts, OldestPending,
    PendingCount, PendingTasks, QueueTasks, ScheduleLastRun, ScheduleRuns, SchemaVersion,
    SeatCount, StreamOldest, TelemetryDownCounts, WorkflowCounts, HELD_GROUP_LIMIT,
    PENDING_COUNT_LIMIT, SCHEDULE_LIMIT,
};

/// One entry of the `pending` part: queue, class, rows counted (1 to
/// [`PENDING_COUNT_LIMIT`] + 1).
pub(super) type PendingEntry = (String, String, u64);

/// One entry of the `held` part: status, queue, class, rows.
pub(super) type HeldEntry = (String, String, String, u64);

/// One entry of the `schedules` part: name, last run (`NULL` when it never
/// ran).
pub(super) type ScheduleEntry = (String, Option<DateTime<Utc>>);

/// Stored data that the fleet read cannot decide on, or a part past its
/// bound. `reason` names the subject and the cause.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct ReadRefused {
    pub(super) reason: String,
}

impl From<ClassReadRefused> for ReadRefused {
    fn from(refusal: ClassReadRefused) -> Self {
        Self {
            reason: refusal.reason,
        }
    }
}

/// The row of the oldest pending statement.
#[derive(Debug)]
pub(super) struct OldestPendingRow {
    pub(super) checked_at: DateTime<Utc>,
    pub(super) heads: Value,
}

/// The row of the workers statement.
#[derive(Debug)]
pub(super) struct WorkersRow {
    pub(super) checked_at: DateTime<Utc>,
    pub(super) recent_rows: i64,
    pub(super) live: Value,
    pub(super) in_flight: Value,
    /// The live workers whose metrics endpoint is down: a `count(*)`.
    pub(super) endpoint_down: i64,
    /// The live workers whose handler child is down: a `count(*)`.
    pub(super) handler_child_down: i64,
}

/// The slot health fields of one entry of the workers statement's `live`
/// part.
#[derive(Debug, Deserialize)]
struct LiveSlotsEntry {
    worker_id: String,
    processes: i32,
    slots_down: Value,
}

/// The queue tasks part, checked in this order: the stream bound, the group
/// bound. `pending` is ordered by queue name, then class; `held` by status,
/// queue name, class.
///
/// Errors: [`ReadRefused`] past [`PENDING_STREAM_LIMIT`] streams or
/// [`HELD_GROUP_LIMIT`] groups.
pub(super) fn queue_tasks(
    pending: Vec<PendingEntry>,
    held: Vec<HeldEntry>,
) -> Result<QueueTasks, ReadRefused> {
    if pending.len() > PENDING_STREAM_LIMIT {
        return Err(refused(format!(
            "more than {PENDING_STREAM_LIMIT} (queue, class) streams have PENDING rows"
        )));
    }
    if held.len() > HELD_GROUP_LIMIT {
        return Err(refused(format!(
            "more than {HELD_GROUP_LIMIT} (status, queue, class) groups have CLAIMED or \
             RUNNING rows"
        )));
    }
    let mut pending: Vec<PendingTasks> = pending.into_iter().map(pending_tasks).collect();
    pending.sort_by(|left, right| {
        (&left.queue_name, left.execution).cmp(&(&right.queue_name, right.execution))
    });
    let mut held: Vec<HeldTasks> = held.into_iter().map(held_tasks).collect();
    held.sort_by(|left, right| {
        (left.status, &left.queue_name, left.execution).cmp(&(
            right.status,
            &right.queue_name,
            right.execution,
        ))
    });
    Ok(QueueTasks { pending, held })
}

fn pending_tasks((queue_name, execution, counted): PendingEntry) -> PendingTasks {
    PendingTasks {
        queue_name,
        execution: stored_execution(&execution),
        count: pending_count(counted),
    }
}

fn held_tasks((status, queue_name, execution, count): HeldEntry) -> HeldTasks {
    HeldTasks {
        status: held_status(&status),
        queue_name,
        execution: stored_execution(&execution),
        count,
    }
}

/// The count of a stream from the rows that the statement counted, which
/// stops one row past [`PENDING_COUNT_LIMIT`].
pub(super) fn pending_count(counted: u64) -> PendingCount {
    if counted <= PENDING_COUNT_LIMIT {
        PendingCount::Exact(counted)
    } else {
        PendingCount::AtLeastLimit
    }
}

/// The status of a `held` entry. The statement reads only `CLAIMED` and
/// `RUNNING` rows, so another value is a defect of the statement, and the
/// read panics with the value.
fn held_status(stored: &str) -> HeldStatus {
    match stored {
        "CLAIMED" => HeldStatus::Claimed,
        "RUNNING" => HeldStatus::Running,
        other => panic!(
            "the held part of fleet_queue_tasks holds status {other:?}; its WHERE admits only \
             CLAIMED and RUNNING"
        ),
    }
}

/// The row of the oldest pending statement from its rows.
///
/// Errors: [`RowDecodeError`] when the statement returned no row, or a
/// column does not decode.
pub(super) fn oldest_pending_row(rows: Vec<Row>) -> Result<OldestPendingRow, RowDecodeError> {
    let statement = &OLDEST_PENDING;
    let row = one_row(rows, statement)?;
    Ok(OldestPendingRow {
        checked_at: column(&row, statement, "checked_at")?,
        heads: column(&row, statement, "heads")?,
    })
}

/// The row of the workers statement from its rows.
///
/// Errors: [`RowDecodeError`] when the statement returned no row, or a
/// column does not decode.
pub(super) fn workers_row(rows: Vec<Row>) -> Result<WorkersRow, RowDecodeError> {
    let statement = &WORKERS;
    let row = one_row(rows, statement)?;
    Ok(WorkersRow {
        checked_at: column(&row, statement, "checked_at")?,
        recent_rows: column(&row, statement, "recent_rows")?,
        live: column(&row, statement, "live")?,
        in_flight: column(&row, statement, "in_flight")?,
        endpoint_down: column(&row, statement, "endpoint_down")?,
        handler_child_down: column(&row, statement, "handler_child_down")?,
    })
}

/// The oldest pending part: per (queue, class), the wait of its oldest due
/// row from the heads of its priorities. Ordered by queue name, then class.
///
/// Errors: [`ReadRefused`] when the heads part does not decode or passes
/// [`PENDING_HEAD_LIMIT`](crate::PENDING_HEAD_LIMIT) groups.
pub(super) fn oldest_pending(row: OldestPendingRow) -> Result<OldestPending, ReadRefused> {
    let heads = pending_heads(row.heads)?;
    let mut grouped: BTreeMap<(String, Execution), Vec<DateTime<Utc>>> = BTreeMap::new();
    for head in heads {
        grouped
            .entry((head.queue_name, head.execution))
            .or_default()
            .push(head.enqueued_at);
    }
    let streams = grouped
        .into_iter()
        .map(|((queue_name, execution), stream_heads)| StreamOldest {
            wait: stream_wait(&stream_heads, row.checked_at),
            queue_name,
            execution,
        })
        .collect();
    Ok(OldestPending {
        checked_at: row.checked_at,
        streams,
    })
}

/// The workers part: the live workers by slot health, the seats of their
/// budgets per class, and the live workers whose telemetry parts are down.
///
/// Errors: [`ReadRefused`] past
/// [`LIVE_SNAPSHOT_ROW_LIMIT`](crate::LIVE_SNAPSHOT_ROW_LIMIT) snapshots,
/// when a live snapshot's `class_capacity` or `slots_down` is unreadable,
/// when a worker holds rows of a class that its budgets do not serve, or when
/// the seats of a class do not fit in 64 bits.
pub(super) fn fleet_workers(row: WorkersRow) -> Result<FleetWorkers, ReadRefused> {
    let workers = live_workers(row.recent_rows, row.live.clone())?;
    let counts = live_worker_counts(row.live)?;
    let held = held_by_worker(in_flight_rows(row.in_flight)?);
    let seats = class_seats(&workers, &held)?;
    Ok(FleetWorkers {
        checked_at: row.checked_at,
        workers: counts,
        seats,
        telemetry_down: TelemetryDownCounts {
            endpoint: stored_count(row.endpoint_down, "live workers whose endpoint is down"),
            handler_child: stored_count(
                row.handler_child_down,
                "live workers whose handler child is down",
            ),
        },
    })
}

/// The class coverage part: the fleet check from its statement row.
///
/// Errors: [`ReadRefused`] as the fleet check
/// (`crate::broker::class_coverage_reads::coverage_from_row`).
pub(super) fn fleet_class_coverage(row: CoverageRow) -> Result<ClassCoverage, ReadRefused> {
    Ok(coverage_from_row(row)?)
}

/// The live workers by slot health. `live` already passed the snapshot
/// bound.
fn live_worker_counts(live: Value) -> Result<LiveWorkerCounts, ReadRefused> {
    let entries: Vec<LiveSlotsEntry> = decoded_part("live", live)?;
    entries.iter().try_fold(
        LiveWorkerCounts {
            serving: 0,
            degraded: 0,
        },
        |counts, entry| match slot_health(entry)? {
            SlotHealth::AllSlotsServing => Ok(LiveWorkerCounts {
                serving: counts.serving + 1,
                ..counts
            }),
            SlotHealth::SlotsDown(_) => Ok(LiveWorkerCounts {
                degraded: counts.degraded + 1,
                ..counts
            }),
        },
    )
}

fn slot_health(entry: &LiveSlotsEntry) -> Result<SlotHealth, ReadRefused> {
    parse_slot_health(&entry.slots_down, entry.processes).map_err(|unreadable| {
        refused(format!(
            "worker {}: slots_down is unreadable: {}",
            entry.worker_id, unreadable.reason
        ))
    })
}

/// The rows in flight of each worker, per class. The statement groups by
/// (worker, class), so each key comes once.
fn held_by_worker(
    in_flight: BTreeMap<(String, Execution), u64>,
) -> BTreeMap<String, BTreeMap<Execution, u64>> {
    let mut by_worker: BTreeMap<String, BTreeMap<Execution, u64>> = BTreeMap::new();
    for ((worker_id, execution), count) in in_flight {
        let _first = by_worker
            .entry(worker_id)
            .or_default()
            .insert(execution, count);
    }
    by_worker
}

/// The seats of every budget of every live worker, per budget class.
///
/// Errors: [`ReadRefused`] when a worker holds rows of a class that none of
/// its budgets serves, or when a sum does not fit in 64 bits. Each stored
/// capacity is below `2^63`, so only absurd stored values reach the second.
fn class_seats(
    workers: &[LiveWorker],
    held: &BTreeMap<String, BTreeMap<Execution, u64>>,
) -> Result<ClassSeats, ReadRefused> {
    let empty = ClassSeats {
        isolated: SeatCount::default(),
        shared_async: SeatCount::default(),
        shared_thread: SeatCount::default(),
    };
    let nothing_held = BTreeMap::new();
    workers.iter().try_fold(empty, |seats, worker| {
        let worker_held = held.get(&worker.worker_id).unwrap_or(&nothing_held);
        budget_loads(worker, worker_held)?
            .iter()
            .try_fold(seats, |seats, budget| {
                let added = with_budget(seats.of(budget.budget), budget).ok_or_else(|| {
                    refused(format!(
                        "the seats of class {} do not fit in 64 bits",
                        budget.budget.as_str()
                    ))
                })?;
                Ok(seats_with(seats, budget.budget, added))
            })
    })
}

/// `sum` with the seats of `budget`: its capacity, and its rows in flight up
/// to its capacity. `None` when a sum does not fit in 64 bits.
fn with_budget(sum: SeatCount, budget: &BudgetLoad) -> Option<SeatCount> {
    Some(SeatCount {
        seats: sum.seats.checked_add(budget.capacity)?,
        in_use: sum
            .in_use
            .checked_add(budget.in_flight.min(budget.capacity))?,
    })
}

fn seats_with(seats: ClassSeats, execution: Execution, count: SeatCount) -> ClassSeats {
    match execution {
        Execution::Isolated => ClassSeats {
            isolated: count,
            ..seats
        },
        Execution::SharedAsync => ClassSeats {
            shared_async: count,
            ..seats
        },
        Execution::SharedThread => ClassSeats {
            shared_thread: count,
            ..seats
        },
    }
}

/// The workflow counts from the two `count(*)` columns. A count is never
/// negative, so a negative value is a defect, and the read panics with it.
pub(super) fn workflow_counts(running: i64, paused: i64) -> WorkflowCounts {
    WorkflowCounts {
        running: stored_count(running, "RUNNING workflows"),
        paused: stored_count(paused, "PAUSED workflows"),
    }
}

fn stored_count(count: i64, subject: &str) -> u64 {
    u64::try_from(count)
        .unwrap_or_else(|_| panic!("count(*) of {subject} is {count}; a count is never negative"))
}

/// The schedules part, ordered by schedule name.
///
/// Errors: [`ReadRefused`] past [`SCHEDULE_LIMIT`] schedules.
pub(super) fn schedule_runs(
    checked_at: DateTime<Utc>,
    entries: Vec<ScheduleEntry>,
) -> Result<ScheduleRuns, ReadRefused> {
    if entries.len() > SCHEDULE_LIMIT {
        return Err(refused(format!(
            "more than {SCHEDULE_LIMIT} schedules have a state row"
        )));
    }
    let mut schedules: Vec<ScheduleLastRun> = entries
        .into_iter()
        .map(|(schedule_name, last_run_at)| ScheduleLastRun {
            schedule_name,
            last_run: last_run(checked_at, last_run_at),
        })
        .collect();
    schedules.sort_by(|left, right| left.schedule_name.cmp(&right.schedule_name));
    Ok(ScheduleRuns {
        checked_at,
        schedules,
    })
}

fn last_run(checked_at: DateTime<Utc>, stored: Option<DateTime<Utc>>) -> LastRun {
    match stored {
        Some(at) => LastRun::RanAt {
            at,
            age: checked_at - at,
        },
        None => LastRun::NeverRan,
    }
}

/// The schema version from the newest successful migration.
///
/// Errors: [`ReadRefused`] when `horsies_migrations` records none. The
/// install writes the bookkeeping row in the transaction of the migration,
/// so a database that the core installed always has one.
pub(super) fn schema_version(newest: Option<i64>) -> Result<SchemaVersion, ReadRefused> {
    match newest {
        Some(version) => Ok(SchemaVersion { version }),
        None => Err(refused(
            "horsies_migrations records no successful migration",
        )),
    }
}

fn refused(reason: impl Into<String>) -> ReadRefused {
    ReadRefused {
        reason: reason.into(),
    }
}

#[cfg(test)]
#[path = "decode_tests.rs"]
mod tests;
