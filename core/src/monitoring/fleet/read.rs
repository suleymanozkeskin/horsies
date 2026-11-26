//! The fleet read against PostgreSQL: one statement per part, run one after
//! another on the main core pool, each bounded by [`FLEET_QUERY_TIMEOUT`].

use std::future::Future;
use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};
use tokio_postgres::types::Json;

use crate::broker::class_coverage_reads::{class_coverage_statement, coverage_row_from};
use crate::broker::core_rows::{column, first_row, one_row, scalar, RowDecodeError};
use crate::db::{
    BoundStatement, CheckoutClass, RefusalCause, Row, StatementBound, StatementFailure, Transience,
    UnknownOutcomeCause,
};
use crate::handle::CoreConnection;
use crate::telemetry::labels::FleetQuery;
use crate::{BrokerError, ClassCoverage, PostgresBroker};

use super::decode::{
    fleet_class_coverage, fleet_workers, oldest_pending, oldest_pending_row, queue_tasks,
    schedule_runs, schema_version, workers_row, workflow_counts, HeldEntry, PendingEntry,
    ReadRefused, ScheduleEntry,
};
use super::statements::{
    oldest_pending_statement, queue_tasks_statement, schedules_statement, schema_version_statement,
    workers_statement, workflows_statement, QUEUE_TASKS, SCHEDULES, SCHEMA_VERSION, WORKFLOWS,
};
use super::{
    FleetPart, FleetQueryError, FleetQueryFailure, FleetState, FleetWorkers, OldestPending,
    QueueTasks, ScheduleRuns, SchemaVersion, WorkflowCounts, FLEET_QUERY_TIMEOUT,
};

/// The fleet state: each [`FleetQuery`] in the order of
/// [`FleetQuery::ALL`], one after another, with how long each took.
///
/// Each statement checks out one connection of the main core pool
/// ([`CheckoutClass::Batch`]), so the read holds at most one connection at a
/// time. Each statement only reads, in autocommit, and is bounded by
/// [`FLEET_QUERY_TIMEOUT`].
///
/// The read itself does not fail: a part whose statement fails holds its
/// [`FleetQueryError`], and the other parts still answer. It does not log;
/// the caller logs the failed parts. It changes nothing in the database.
pub async fn read_fleet_state(broker: &PostgresBroker) -> FleetState {
    FleetState {
        queue_tasks: timed(FleetQuery::QueueTasks, read_queue_tasks(broker)).await,
        oldest_pending: timed(FleetQuery::OldestPending, read_oldest_pending(broker)).await,
        workers: timed(FleetQuery::Workers, read_workers(broker)).await,
        class_coverage: timed(FleetQuery::ClassCoverage, read_class_coverage(broker)).await,
        workflows: timed(FleetQuery::Workflows, read_workflows(broker)).await,
        schedules: timed(FleetQuery::Schedules, read_schedules(broker)).await,
        schema_version: timed(FleetQuery::SchemaVersion, read_schema_version(broker)).await,
    }
}

/// The fleet state through `handle`: its broker, connected at the first
/// call that succeeds ([`CoreConnection::broker`]), then [`read_fleet_state`].
///
/// When the connect fails, every part answers
/// [`FleetQueryFailure::DatabaseFailed`] with the cause of the connect, and
/// `took` of each part is the time of the connect; the next call connects
/// again. The connect runs no schema step.
pub async fn read_fleet_state_through(handle: &CoreConnection) -> FleetState {
    let started = Instant::now();
    match handle.broker().await {
        Ok(broker) => read_fleet_state(&broker).await,
        Err(error) => not_connected(&pools_failed(&error), started.elapsed()),
    }
}

/// The fleet state of a read that could not connect: every part fails with
/// `failure`.
fn not_connected(failure: &FleetQueryFailure, took: Duration) -> FleetState {
    FleetState {
        queue_tasks: failed_part(FleetQuery::QueueTasks, failure, took),
        oldest_pending: failed_part(FleetQuery::OldestPending, failure, took),
        workers: failed_part(FleetQuery::Workers, failure, took),
        class_coverage: failed_part(FleetQuery::ClassCoverage, failure, took),
        workflows: failed_part(FleetQuery::Workflows, failure, took),
        schedules: failed_part(FleetQuery::Schedules, failure, took),
        schema_version: failed_part(FleetQuery::SchemaVersion, failure, took),
    }
}

fn failed_part<T>(query: FleetQuery, failure: &FleetQueryFailure, took: Duration) -> FleetPart<T> {
    FleetPart {
        outcome: Err(FleetQueryError {
            query,
            failure: failure.clone(),
        }),
        took,
    }
}

/// Runs `read` and measures it on the monotonic clock.
async fn timed<T>(
    query: FleetQuery,
    read: impl Future<Output = Result<T, FleetQueryFailure>>,
) -> FleetPart<T> {
    let started = Instant::now();
    let outcome = read
        .await
        .map_err(|failure| FleetQueryError { query, failure });
    FleetPart {
        outcome,
        took: started.elapsed(),
    }
}

/// The queue tasks part. Errors: as [`fleet_rows`]; `Refused` past a bound
/// or for a row that does not decode.
async fn read_queue_tasks(broker: &PostgresBroker) -> Result<QueueTasks, FleetQueryFailure> {
    let rows = fleet_rows(broker, &queue_tasks_statement()).await?;
    let row = one_row(rows, &QUEUE_TASKS).map_err(|error| row_refused(&error))?;
    let pending: Json<Vec<PendingEntry>> =
        column(&row, &QUEUE_TASKS, "pending").map_err(|error| row_refused(&error))?;
    let held: Json<Vec<HeldEntry>> =
        column(&row, &QUEUE_TASKS, "held").map_err(|error| row_refused(&error))?;
    queue_tasks(pending.0, held.0).map_err(refused)
}

/// The oldest pending part. Errors: as [`fleet_rows`]; `Refused` past the
/// head bound or for a row that does not decode.
async fn read_oldest_pending(broker: &PostgresBroker) -> Result<OldestPending, FleetQueryFailure> {
    let rows = fleet_rows(broker, &oldest_pending_statement()).await?;
    let row = oldest_pending_row(rows).map_err(|error| row_refused(&error))?;
    oldest_pending(row).map_err(refused)
}

/// The workers part. Errors: as [`fleet_rows`]; `Refused` past the snapshot
/// bound, for unreadable snapshot values, for held rows that no budget
/// serves, or for a row that does not decode.
async fn read_workers(broker: &PostgresBroker) -> Result<FleetWorkers, FleetQueryFailure> {
    let rows = fleet_rows(broker, &workers_statement()).await?;
    let row = workers_row(rows).map_err(|error| row_refused(&error))?;
    fleet_workers(row).map_err(refused)
}

/// The class coverage part, from the fleet check statement. Errors: as
/// [`fleet_rows`]; `Refused` as the fleet check.
async fn read_class_coverage(broker: &PostgresBroker) -> Result<ClassCoverage, FleetQueryFailure> {
    let rows = fleet_rows(broker, &class_coverage_statement()).await?;
    let row = coverage_row_from(rows).map_err(|error| row_refused(&error))?;
    fleet_class_coverage(row).map_err(refused)
}

/// The workflows part. Errors: as [`fleet_rows`]; `Refused` for a row that
/// does not decode.
async fn read_workflows(broker: &PostgresBroker) -> Result<WorkflowCounts, FleetQueryFailure> {
    let rows = fleet_rows(broker, &workflows_statement()).await?;
    let row = one_row(rows, &WORKFLOWS).map_err(|error| row_refused(&error))?;
    let running: i64 = column(&row, &WORKFLOWS, "running").map_err(|error| row_refused(&error))?;
    let paused: i64 = column(&row, &WORKFLOWS, "paused").map_err(|error| row_refused(&error))?;
    Ok(workflow_counts(running, paused))
}

/// The schedules part. Errors: as [`fleet_rows`]; `Refused` past the bound
/// or for a row that does not decode.
async fn read_schedules(broker: &PostgresBroker) -> Result<ScheduleRuns, FleetQueryFailure> {
    let rows = fleet_rows(broker, &schedules_statement()).await?;
    let row = one_row(rows, &SCHEDULES).map_err(|error| row_refused(&error))?;
    let checked_at: DateTime<Utc> =
        column(&row, &SCHEDULES, "checked_at").map_err(|error| row_refused(&error))?;
    let entries: Json<Vec<ScheduleEntry>> =
        column(&row, &SCHEDULES, "schedules").map_err(|error| row_refused(&error))?;
    schedule_runs(checked_at, entries.0).map_err(refused)
}

/// The schema version part. Errors: as [`fleet_rows`]; `Refused` when no
/// successful migration is recorded, or for a row that does not decode.
async fn read_schema_version(broker: &PostgresBroker) -> Result<SchemaVersion, FleetQueryFailure> {
    let rows = fleet_rows(broker, &schema_version_statement()).await?;
    let newest: Option<i64> = match first_row(rows) {
        Some(row) => Some(scalar(&row, &SCHEMA_VERSION).map_err(|error| row_refused(&error))?),
        None => None,
    };
    schema_version(newest).map_err(refused)
}

/// Runs one statement of the fleet read on the main core pool and reads
/// every row. The statement is bounded by [`FLEET_QUERY_TIMEOUT`]. It only
/// reads, so nothing changed after any failure.
///
/// Errors:
/// - `TimedOut`: the statement ran past the bound;
/// - `DatabaseFailed`: the pools could not open, the checkout or the
///   connection failed, or the server refused the statement.
async fn fleet_rows(
    broker: &PostgresBroker,
    statement: &BoundStatement<'_>,
) -> Result<Vec<Row>, FleetQueryFailure> {
    let open = broker
        .core_pools_for_read()
        .map_err(|error| pools_failed(&error))?;
    broker.statement_log().log(statement.statement());
    open.main()
        .query(
            CheckoutClass::Batch,
            statement,
            StatementBound::CancelAfter(FLEET_QUERY_TIMEOUT),
        )
        .await
        .map_err(|failure| statement_failed(&failure))
}

/// The pools of the handle could not open.
fn pools_failed(error: &BrokerError) -> FleetQueryFailure {
    let transience = match error.is_retryable() {
        true => Transience::Transient,
        false => Transience::Permanent,
    };
    FleetQueryFailure::DatabaseFailed {
        transience,
        detail: error.to_string(),
    }
}

/// A failed statement: past its bound, or not answered by the database.
fn statement_failed(failure: &StatementFailure) -> FleetQueryFailure {
    match failure {
        StatementFailure::StatementRefused {
            cause: RefusalCause::CancelledAfterBound { bound, .. },
            ..
        }
        | StatementFailure::OutcomeUnknown {
            cause: UnknownOutcomeCause::NoReplyWithinBound { bound, .. },
            ..
        } => FleetQueryFailure::TimedOut { bound: *bound },
        StatementFailure::NotSent { .. }
        | StatementFailure::StatementRefused {
            cause: RefusalCause::Server(_),
            ..
        }
        | StatementFailure::OutcomeUnknown {
            cause:
                UnknownOutcomeCause::ConnectionLost(_)
                | UnknownOutcomeCause::SessionTerminated(_)
                | UnknownOutcomeCause::DriverFailed(_),
            ..
        } => FleetQueryFailure::DatabaseFailed {
            transience: failure.transience(),
            detail: failure.to_string(),
        },
    }
}

/// A row of a fleet statement that does not have its shape.
fn row_refused(error: &RowDecodeError) -> FleetQueryFailure {
    FleetQueryFailure::Refused {
        reason: error.to_string(),
    }
}

fn refused(refusal: ReadRefused) -> FleetQueryFailure {
    FleetQueryFailure::Refused {
        reason: refusal.reason,
    }
}
