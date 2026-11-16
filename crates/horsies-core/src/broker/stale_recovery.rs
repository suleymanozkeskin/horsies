//! The stale recovery statements of the reaper, one statement each on a core
//! pool (`horsies/core/baseline/sql/0004_lifecycle_functions.sql`):
//!
//! - `horsies_stale_running_scan`: the candidates of one stale RUNNING pass,
//!   with the facts that the retry decision and the crash result need. It
//!   takes no lock; each candidate is judged again under its locks.
//! - `horsies_finish_stale_attempt`: the recovery of one candidate under the
//!   task advisory lock and the row lock: the crash retry that the caller
//!   asks for, the terminal failure (`FAIL_STALE`), or a refusal that writes
//!   nothing.
//!
//! Each statement runs alone on its connection in autocommit and is checked
//! out as reserved work ([`CheckoutClass::Reserved`]): the reaper's pass runs
//! on the reserve. A failure is one of the statement layer's three outcomes
//! ([`BrokerError::Statement`]).
//!
//! Python reference: `mark_stale_tasks_as_failed`
//! (`horsies/core/brokers/postgres.py:3129-3423`).

use chrono::{DateTime, Utc};
use uuid::Uuid;

use crate::broker::core_rows::{column, read_rows, RowDecodeError, StatementLog};
use crate::core::lifecycle::{decode_stale_attempt_row, StaleAttemptOutcome};
use crate::db::{
    BoundStatement, CheckoutClass, CorePool, ParameterType, Row, StatementBound, TypedStatement,
};

use super::error::BrokerError;

/// The largest batch that a recovery function takes (`p_batch_size`).
const RECOVERY_BATCH_LIMIT: i32 = 10_000;

/// The rows of one recovery statement: 1 to 10 000, the range the recovery
/// functions accept.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct RecoveryBatch(i32);

impl RecoveryBatch {
    /// `None` outside 1 to 10 000.
    pub(crate) const fn new(rows: i32) -> Option<RecoveryBatch> {
        match rows {
            1..=RECOVERY_BATCH_LIMIT => Some(RecoveryBatch(rows)),
            _ => None,
        }
    }

    pub(crate) const fn get(self) -> i32 {
        self.0
    }
}

/// A staleness threshold in whole milliseconds, in the range of the
/// `integer` parameters of the recovery functions (0 to 2 147 483 647).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ThresholdMs(i32);

/// A threshold above the `integer` range of the recovery functions. The
/// recovery config validation keeps the thresholds at 2 hours or less, so
/// only a config that bypassed validation gets here.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error(
    "a threshold of {value_ms} ms is above the limit of 2147483647 ms of the recovery functions"
)]
pub(crate) struct ThresholdOutOfRange {
    pub(crate) value_ms: u64,
}

impl ThresholdMs {
    /// Errors: [`ThresholdOutOfRange`] above `i32::MAX` ms.
    pub(crate) fn from_millis(value_ms: u64) -> Result<ThresholdMs, ThresholdOutOfRange> {
        i32::try_from(value_ms)
            .map(ThresholdMs)
            .map_err(|_| ThresholdOutOfRange { value_ms })
    }

    pub(crate) fn get(self) -> i32 {
        self.0
    }
}

/// The thresholds of one stale RUNNING pass.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct StaleRunningThresholds {
    /// A row is stale when its latest runner beat (its start when it has
    /// none) is older than this.
    pub(crate) stale_after: ThresholdMs,
    /// A row whose finalizing marker is newer than this is not stale.
    pub(crate) finalizing_stale_after: ThresholdMs,
}

/// `horsies_stale_running_scan`. Parameters in order: stale threshold,
/// finalizing threshold (ms), batch size.
static STALE_RUNNING_SCAN: TypedStatement = TypedStatement::new(
    "stale_running_scan",
    "SELECT * FROM horsies_stale_running_scan($1, $2, $3)",
    &[
        ParameterType::Int4,
        ParameterType::Int4,
        ParameterType::Int4,
    ],
);

/// `horsies_finish_stale_attempt`. Parameters in order: task id, stale
/// threshold, finalizing threshold (ms), retry kind, expected retry count,
/// retry delay in seconds, result, error code, failed reason, attempt
/// message, attempt reason.
static FINISH_STALE_ATTEMPT: TypedStatement = TypedStatement::new(
    "finish_stale_attempt",
    "SELECT * FROM horsies_finish_stale_attempt($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)",
    &[
        ParameterType::Uuid,
        ParameterType::Int4,
        ParameterType::Int4,
        ParameterType::Text,
        ParameterType::Int4,
        ParameterType::Float8,
        ParameterType::Text,
        ParameterType::Text,
        ParameterType::Text,
        ParameterType::Text,
        ParameterType::Text,
    ],
);

/// The wake of a retried row's queue. Parameters: channel, payload.
static RETRY_WAKE: TypedStatement = TypedStatement::new(
    "stale_retry_wake",
    "SELECT pg_notify($1, $2)",
    &[ParameterType::Text, ParameterType::Text],
);

/// One candidate of a stale RUNNING pass, as the scan read it with no lock.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StaleCandidate {
    pub(crate) task_id: Uuid,
    /// The retry facts of the row, for the retry decision.
    pub(crate) retry_count: i32,
    pub(crate) max_retries: i32,
    pub(crate) task_options: Option<String>,
    pub(crate) good_until: Option<DateTime<Utc>>,
    /// The silent claim, for the crash result.
    pub(crate) claimed_by_worker_id: Option<String>,
    pub(crate) worker_pid: Option<i32>,
    pub(crate) worker_hostname: Option<String>,
    pub(crate) worker_process_name: Option<String>,
    pub(crate) started_at: DateTime<Utc>,
    /// The database clock at the scan.
    pub(crate) db_now: DateTime<Utc>,
}

/// The crash retry that the task's policy admits, computed from the scanned
/// retry facts.
#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) enum StaleRetry {
    /// The statement writes the terminal failure.
    NoRetry,
    /// The statement writes the retry when the locked row still holds
    /// `expected_retry_count` and the next attempt starts before
    /// `good_until`; else the terminal failure (`PAST_DEADLINE`) or a
    /// refusal (guard `RETRY_COUNT`).
    RetryAfter {
        expected_retry_count: i32,
        delay_seconds: f64,
    },
}

/// The recovery of one stale RUNNING candidate, for one statement.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct FinishStaleAttempt {
    pub(crate) task_id: Uuid,
    pub(crate) thresholds: StaleRunningThresholds,
    pub(crate) retry: StaleRetry,
    /// The terminal result, the task's error code and failed reason: written
    /// when the failure applies.
    pub(crate) result_json: String,
    pub(crate) error_code: String,
    pub(crate) failed_reason: String,
    /// The message and reason of the attempt row.
    pub(crate) attempt_message: String,
    pub(crate) attempt_reason: String,
}

/// Read the candidates of one stale RUNNING pass: at most `batch` rows.
///
/// One statement on `pool`, checked out as reserved work. Reads only.
///
/// Errors: [`BrokerError::Statement`]; [`BrokerError::RowDecode`] for a row
/// that does not have the scan's columns (a NULL `started_at` included).
pub(crate) async fn scan_stale_running(
    pool: &CorePool,
    log: StatementLog,
    thresholds: StaleRunningThresholds,
    batch: RecoveryBatch,
) -> Result<Vec<StaleCandidate>, BrokerError> {
    let statement = &STALE_RUNNING_SCAN;
    let (stale_after, finalizing_stale_after, batch_size) = (
        thresholds.stale_after.get(),
        thresholds.finalizing_stale_after.get(),
        batch.get(),
    );
    let bound = statement
        .bind()
        .value(&stale_after)
        .value(&finalizing_stale_after)
        .value(&batch_size)
        .finish();
    let rows = run_reserved(pool, log, &bound).await?;
    Ok(read_rows(&rows, statement, stale_candidate)?)
}

fn stale_candidate(
    row: &Row,
    statement: &TypedStatement,
) -> Result<StaleCandidate, RowDecodeError> {
    Ok(StaleCandidate {
        task_id: column(row, statement, "task_id")?,
        retry_count: column(row, statement, "retry_count")?,
        max_retries: column(row, statement, "max_retries")?,
        task_options: column(row, statement, "task_options")?,
        good_until: column(row, statement, "good_until")?,
        claimed_by_worker_id: column(row, statement, "claimed_by_worker_id")?,
        worker_pid: column(row, statement, "worker_pid")?,
        worker_hostname: column(row, statement, "worker_hostname")?,
        worker_process_name: column(row, statement, "worker_process_name")?,
        started_at: column(row, statement, "started_at")?,
        db_now: column(row, statement, "db_now")?,
    })
}

/// Recover one stale RUNNING candidate.
///
/// One statement on `pool`, checked out as reserved work. Under the task
/// advisory lock and the row lock it judges the row again on one capture of
/// the row, its latest runner beat and the database clock. It writes the
/// FAILED attempt row (`will_retry` set for a retry) and either the retry
/// (PENDING, every claim and run field cleared, the execution class kept)
/// or the move to history (`FAIL_STALE`, no capacity wake). A refusal
/// writes nothing.
///
/// After `OutcomeUnknown` the same command sent again reports a committed
/// failure as `AlreadyApplied` and a committed retry as a refusal (the row
/// is PENDING).
///
/// Errors: [`BrokerError::Statement`];
/// [`BrokerError::TerminalizationContract`] when the row breaks the wire
/// contract.
pub(crate) async fn finish_stale_attempt(
    pool: &CorePool,
    log: StatementLog,
    command: &FinishStaleAttempt,
) -> Result<StaleAttemptOutcome, BrokerError> {
    let statement = &FINISH_STALE_ATTEMPT;
    let (retry_kind, expected_retry_count, delay_seconds) = match command.retry {
        StaleRetry::NoRetry => ("NONE", None, None),
        StaleRetry::RetryAfter {
            expected_retry_count,
            delay_seconds,
        } => ("RETRY", Some(expected_retry_count), Some(delay_seconds)),
    };
    let stale_after = command.thresholds.stale_after.get();
    let finalizing_stale_after = command.thresholds.finalizing_stale_after.get();
    let bound = statement
        .bind()
        .value(&command.task_id)
        .value(&stale_after)
        .value(&finalizing_stale_after)
        .value(&retry_kind)
        .value(&expected_retry_count)
        .value(&delay_seconds)
        .value(&command.result_json)
        .value(&command.error_code)
        .value(&command.failed_reason)
        .value(&command.attempt_message)
        .value(&command.attempt_reason)
        .finish();
    let mut rows = run_reserved(pool, log, &bound).await?;
    let row = match rows.len() {
        1 => rows.remove(0),
        count => {
            return Err(BrokerError::TerminalizationContract(format!(
                "{} returned {count} rows for task {}; it reports exactly one outcome",
                statement.name(),
                command.task_id,
            )))
        }
    };
    decode_stale_attempt_row(&row).map_err(|error| {
        BrokerError::TerminalizationContract(format!("{}: {error}", statement.name()))
    })
}

/// Wake the queue of a retried row: `pg_notify('task_queue_<queue>',
/// 'retry:<task id>')`, as Python's stale recovery does after its retry
/// commits (`horsies/core/brokers/postgres.py:3307-3323`).
///
/// One statement on `pool`, checked out as reserved work. The wake is
/// best-effort: a worker that misses it finds the row at its next poll.
///
/// Errors: [`BrokerError::Statement`].
pub(crate) async fn wake_retried_queue(
    pool: &CorePool,
    log: StatementLog,
    queue_name: &str,
    task_id: Uuid,
) -> Result<(), BrokerError> {
    let channel = format!("task_queue_{queue_name}");
    let payload = format!("retry:{task_id}");
    let bound = RETRY_WAKE.bind().value(&channel).value(&payload).finish();
    let _rows: Vec<Row> = run_reserved(pool, log, &bound).await?;
    Ok(())
}

/// Run one bound statement alone on `pool` as reserved work.
async fn run_reserved(
    pool: &CorePool,
    log: StatementLog,
    bound: &BoundStatement<'_>,
) -> Result<Vec<Row>, BrokerError> {
    log.log(bound.statement());
    Ok(pool
        .query(
            CheckoutClass::Reserved,
            bound,
            StatementBound::ConnectionTimeouts,
        )
        .await?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_batch_is_one_to_ten_thousand_rows() {
        assert_eq!(RecoveryBatch::new(0), None);
        assert_eq!(RecoveryBatch::new(1).map(RecoveryBatch::get), Some(1));
        assert_eq!(
            RecoveryBatch::new(10_000).map(RecoveryBatch::get),
            Some(10_000)
        );
        assert_eq!(RecoveryBatch::new(10_001), None);
        assert_eq!(RecoveryBatch::new(-1), None);
    }

    #[test]
    fn a_threshold_above_the_integer_range_is_refused() {
        assert_eq!(ThresholdMs::from_millis(0).map(ThresholdMs::get), Ok(0));
        assert_eq!(
            ThresholdMs::from_millis(7_200_000).map(ThresholdMs::get),
            Ok(7_200_000)
        );
        let above = u64::try_from(i32::MAX).expect("i32::MAX fits u64") + 1;
        assert_eq!(
            ThresholdMs::from_millis(above),
            Err(ThresholdOutOfRange { value_ms: above })
        );
    }
}
