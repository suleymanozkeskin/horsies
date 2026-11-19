//! The stale recovery statements of the reaper, one statement each on a core
//! pool (`crates/horsies-core/baseline/0004_lifecycle_functions.sql`):
//!
//! - `horsies_unheld_worker_owners`: the liveness read of one pass, the
//!   owners of CLAIMED and RUNNING rows whose liveness lock no session holds,
//!   with the generation of their latest acquisition. It takes no lock.
//! - `horsies_worker_liveness_generations`: the current generation of the
//!   owners that two liveness reads judged, read after the later one. It
//!   takes no lock.
//! - `horsies_forget_ended_worker_liveness`: one bounded batch of liveness
//!   rows of ended workers, deleted.
//! - `horsies_stale_running_scan`: the candidates of one stale RUNNING pass,
//!   with the facts that the retry decision and the crash result need. It
//!   takes no lock; each candidate is judged again under its locks.
//! - `horsies_finish_stale_attempt`: the recovery of one candidate under the
//!   task advisory lock and the row lock: the crash retry that the caller
//!   asks for, the terminal failure (`FAIL_STALE`), or a refusal that writes
//!   nothing.
//! - `horsies_requeue_stale_claims`: one bounded batch of CLAIMED rows whose
//!   claimer stopped beating, back to PENDING.
//!
//! The last three take the evidence of their judgement (`StaleEvidence`):
//! the beat age (`BEAT_AGE`, Python's rule), or a gone owner (`OWNER_GONE`):
//! a worker whose liveness lock was free in two reads far enough apart
//! (`GoneOwner`).
//!
//! Each statement runs alone on its connection in autocommit and is checked
//! out as reserved work ([`CheckoutClass::Reserved`]): the reaper's pass runs
//! on the reserve. A failure is one of the statement layer's three outcomes
//! ([`BrokerError::Statement`]).
//!
//! Python reference: `mark_stale_tasks_as_failed`
//! (`python/horsies/core/brokers/postgres.py:3129-3423`) and `requeue_stale_claimed`
//! (`:3505-3530`).

use std::collections::BTreeMap;

use chrono::{DateTime, Utc};
use uuid::Uuid;

use crate::broker::core_rows::{column, one_row, read_rows, RowDecodeError, StatementLog};
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
/// Only [`ThresholdMs::from_millis`] constructs it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ThresholdMs(i32);

/// A threshold above the `integer` range of the recovery functions. The
/// recovery config validation keeps the thresholds at 2 hours or less, so
/// only a config that bypassed validation, or a direct call of a client
/// method with any integer, gets here.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error(
    "a threshold of {value_ms} ms is above the limit of 2147483647 ms of the recovery functions"
)]
pub struct ThresholdOutOfRange {
    pub value_ms: u64,
}

impl ThresholdMs {
    /// Errors: [`ThresholdOutOfRange`] above `i32::MAX` ms.
    pub fn from_millis(value_ms: u64) -> Result<ThresholdMs, ThresholdOutOfRange> {
        i32::try_from(value_ms)
            .map(ThresholdMs)
            .map_err(|_| ThresholdOutOfRange { value_ms })
    }

    pub fn get(self) -> i32 {
        self.0
    }
}

/// The thresholds of one stale RUNNING pass.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StaleRunningThresholds {
    /// A row is stale when its latest runner beat (its start when it has
    /// none) is older than this.
    pub stale_after: ThresholdMs,
    /// A row whose finalizing marker is newer than this is not stale.
    pub finalizing_stale_after: ThresholdMs,
}

/// Candidates that one stale RUNNING pass reads at most (Python
/// `STALE_RUNNING_SCAN_BATCH_SIZE`). Each candidate is one statement, so
/// this bounds the length of one pass; a larger backlog drains over
/// successive passes.
pub(crate) const STALE_RUNNING_SCAN_BATCH: RecoveryBatch = match RecoveryBatch::new(1_000) {
    Some(batch) => batch,
    None => panic!("1000 rows is a recovery batch"),
};

/// How a stale RUNNING pass ended. `recovered` counts the candidates whose
/// retry or failure committed; a refused candidate and a candidate whose
/// statement failed are not counted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StaleRunningPass {
    /// The scan read fewer candidates than its batch: every stale row that
    /// it saw was judged.
    Complete { recovered: u64 },
    /// The scan read a full batch of `scan_batch` candidates; stale rows can
    /// remain for the next pass (Python warns then).
    ScanFull { recovered: u64, scan_batch: u32 },
}

impl StaleRunningPass {
    /// The candidates whose retry or failure committed.
    pub const fn recovered(self) -> u64 {
        match self {
            Self::Complete { recovered } | Self::ScanFull { recovered, .. } => recovered,
        }
    }
}

/// The evidence `BEAT_AGE` of the recovery functions.
const BEAT_AGE: &str = "BEAT_AGE";

/// The evidence `OWNER_GONE` of the recovery functions.
const OWNER_GONE: &str = "OWNER_GONE";

/// `horsies_unheld_worker_owners`. Parameter: batch size.
static UNHELD_WORKER_OWNERS: TypedStatement = TypedStatement::new(
    "unheld_worker_owners",
    "SELECT * FROM horsies_unheld_worker_owners($1)",
    &[ParameterType::Int4],
);

/// `horsies_worker_liveness_generations`. Parameter: the worker ids.
static LIVENESS_GENERATIONS: TypedStatement = TypedStatement::new(
    "worker_liveness_generations",
    "SELECT * FROM horsies_worker_liveness_generations($1)",
    &[ParameterType::TextArray],
);

/// `horsies_forget_ended_worker_liveness`. Parameter: batch size.
static FORGET_ENDED_WORKER_LIVENESS: TypedStatement = TypedStatement::new(
    "forget_ended_worker_liveness",
    "SELECT horsies_forget_ended_worker_liveness($1) AS forgotten",
    &[ParameterType::Int4],
);

/// `horsies_stale_running_scan`. Parameters in order: evidence, stale
/// threshold, finalizing threshold (ms), owner, claim bound, batch size.
static STALE_RUNNING_SCAN: TypedStatement = TypedStatement::new(
    "stale_running_scan",
    "SELECT * FROM horsies_stale_running_scan($1, $2, $3, $4, $5, $6)",
    &[
        ParameterType::Text,
        ParameterType::Int4,
        ParameterType::Int4,
        ParameterType::Text,
        ParameterType::Timestamptz,
        ParameterType::Int4,
    ],
);

/// `horsies_finish_stale_attempt`. Parameters in order: task id, evidence,
/// stale threshold, finalizing threshold (ms), owner, claim generation of the
/// owner, retry kind, expected retry count, retry delay in seconds, result,
/// error code, failed reason, attempt message, attempt reason.
static FINISH_STALE_ATTEMPT: TypedStatement = TypedStatement::new(
    "finish_stale_attempt",
    "SELECT * FROM horsies_finish_stale_attempt(\
     $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)",
    &[
        ParameterType::Uuid,
        ParameterType::Text,
        ParameterType::Int4,
        ParameterType::Int4,
        ParameterType::Text,
        ParameterType::Timestamptz,
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

/// `horsies_requeue_stale_claims`. Parameters in order: evidence, stale
/// threshold (ms), owner, claim bound, batch size.
static REQUEUE_STALE_CLAIMS: TypedStatement = TypedStatement::new(
    "requeue_stale_claims",
    "SELECT horsies_requeue_stale_claims($1, $2, $3, $4, $5) AS requeued",
    &[
        ParameterType::Text,
        ParameterType::Int4,
        ParameterType::Text,
        ParameterType::Timestamptz,
        ParameterType::Int4,
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

/// The generation of one acquisition of a worker's liveness lock: a value
/// of the sequence `horsies_worker_liveness_generation`, which no
/// acquisition takes twice.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct LivenessGeneration(pub(crate) i64);

impl std::fmt::Display for LivenessGeneration {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{}", self.0)
    }
}

/// The liveness generation of an owner at one read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum OwnerGeneration {
    /// The generation of the owner's latest recorded acquisition.
    Recorded(LivenessGeneration),
    /// The owner has no liveness row: no acquisition is recorded, or its
    /// row was deleted. No read judges such an owner gone.
    NotRecorded,
}

/// A worker that the reaper judged gone: its liveness lock was free in two
/// reads at least the gone span apart, both at one generation, no read
/// between them found it held or at another generation
/// ([`crate::worker::liveness::OWNER_GONE_SPAN`]), and a read of the
/// generations after the later read still found that generation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct GoneOwner {
    pub(crate) worker_id: String,
    /// The generation of the owner's last acquisition.
    pub(crate) generation: LivenessGeneration,
    /// The database time of the first of those reads. Only rows that the
    /// owner claimed before it are recovered by this evidence.
    pub(crate) unheld_since: DateTime<Utc>,
}

/// One claim of a gone owner: the owner and the claim generation of one
/// RUNNING row, as the scan read it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct OwnerClaim {
    pub(crate) worker_id: String,
    pub(crate) claimed_at: DateTime<Utc>,
}

/// What a stale RUNNING recovery judges its row by (`p_evidence`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum StaleEvidence {
    /// `BEAT_AGE`: the runner stopped beating, by the two thresholds.
    BeatAge(StaleRunningThresholds),
    /// `OWNER_GONE`: the owner is gone. The row must still be RUNNING under
    /// this claim.
    OwnerGone(OwnerClaim),
}

/// One candidate of a gone owner: the facts of the scan, and the claim
/// generation that the recovery fences on.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct GoneOwnerCandidate {
    pub(crate) facts: StaleCandidate,
    pub(crate) claimed_at: DateTime<Utc>,
}

/// One owner of the liveness read: a worker id, its generation in the
/// snapshot of the read (taken before the read of the lock table), and the
/// database time of the read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct UnheldOwner {
    pub(crate) worker_id: String,
    pub(crate) generation: OwnerGeneration,
    pub(crate) read_at: DateTime<Utc>,
}

/// Whether the liveness read returned every unheld owner.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum OwnerListReach {
    /// Fewer owners than the batch: every unheld owner is in the list.
    Complete,
    /// A full batch: owners after the last one, in worker id order, can be
    /// unheld too.
    Cut,
}

/// The liveness read of one pass.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct UnheldOwnerRead {
    /// In worker id order (byte order, the C collation of the read).
    pub(crate) owners: Vec<UnheldOwner>,
    pub(crate) reach: OwnerListReach,
}

/// Owners that one liveness read returns at most.
pub(crate) const UNHELD_OWNER_BATCH: RecoveryBatch = match RecoveryBatch::new(1_000) {
    Some(batch) => batch,
    None => panic!("1000 owners is a recovery batch"),
};

/// The recovery of one stale RUNNING candidate, for one statement.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct FinishStaleAttempt {
    pub(crate) task_id: Uuid,
    pub(crate) evidence: StaleEvidence,
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

/// The liveness read of one pass: at most `batch` owners of CLAIMED and
/// RUNNING rows whose liveness lock no session holds, in worker id order.
///
/// One statement on `pool`, checked out as reserved work. Reads only; it
/// takes no lock.
///
/// Errors: [`BrokerError::Statement`]; [`BrokerError::RowDecode`] for a row
/// that does not have the read's columns.
pub(crate) async fn read_unheld_owners(
    pool: &CorePool,
    log: StatementLog,
    batch: RecoveryBatch,
) -> Result<UnheldOwnerRead, BrokerError> {
    let statement = &UNHELD_WORKER_OWNERS;
    let batch_size = batch.get();
    let bound = statement.bind().value(&batch_size).finish();
    let rows = run_reserved(pool, log, &bound).await?;
    let owners = read_rows(&rows, statement, unheld_owner)?;
    let full = usize::try_from(batch_size).is_ok_and(|limit| owners.len() >= limit);
    let reach = match full {
        true => OwnerListReach::Cut,
        false => OwnerListReach::Complete,
    };
    Ok(UnheldOwnerRead { owners, reach })
}

fn unheld_owner(row: &Row, statement: &TypedStatement) -> Result<UnheldOwner, RowDecodeError> {
    let generation: Option<i64> = column(row, statement, "generation")?;
    Ok(UnheldOwner {
        worker_id: column(row, statement, "worker_id")?,
        generation: match generation {
            Some(value) => OwnerGeneration::Recorded(LivenessGeneration(value)),
            None => OwnerGeneration::NotRecorded,
        },
        read_at: column(row, statement, "db_now")?,
    })
}

/// The current liveness generation of each of `worker_ids` that has a
/// liveness row. A worker with no row is not in the map.
///
/// One statement on `pool`, checked out as reserved work, after the
/// liveness read whose owners it confirms: its snapshot follows that read's
/// read of the lock table. Reads only; it takes no lock.
///
/// Errors: [`BrokerError::Statement`]; [`BrokerError::RowDecode`] for a row
/// that does not have the read's columns.
pub(crate) async fn read_liveness_generations(
    pool: &CorePool,
    log: StatementLog,
    worker_ids: &[String],
) -> Result<BTreeMap<String, LivenessGeneration>, BrokerError> {
    let statement = &LIVENESS_GENERATIONS;
    let bound = statement.bind().value(&worker_ids).finish();
    let rows = run_reserved(pool, log, &bound).await?;
    let generations = read_rows(&rows, statement, worker_generation)?;
    Ok(generations.into_iter().collect())
}

fn worker_generation(
    row: &Row,
    statement: &TypedStatement,
) -> Result<(String, LivenessGeneration), RowDecodeError> {
    Ok((
        column(row, statement, "worker_id")?,
        LivenessGeneration(column(row, statement, "generation")?),
    ))
}

/// Delete one bounded batch of liveness rows of ended workers: at most
/// `batch` rows of workers that own no CLAIMED or RUNNING row and whose
/// liveness lock no session holds. Returns the number of rows deleted.
///
/// One statement on `pool`, checked out as reserved work; it commits alone.
/// An owner with no row is never judged gone, so a deleted row delays no
/// recovery that a live row would allow.
///
/// Errors: [`BrokerError::Statement`]; [`BrokerError::RowDecode`] when the
/// statement returns no count; [`BrokerError::ClaimContract`] for a count
/// outside 0 to `batch`.
pub(crate) async fn forget_ended_worker_liveness(
    pool: &CorePool,
    log: StatementLog,
    batch: RecoveryBatch,
) -> Result<u32, BrokerError> {
    let statement = &FORGET_ENDED_WORKER_LIVENESS;
    let batch_size = batch.get();
    let bound = statement.bind().value(&batch_size).finish();
    let row = one_row(run_reserved(pool, log, &bound).await?, statement)?;
    let forgotten: i32 = column(&row, statement, "forgotten")?;
    match u32::try_from(forgotten) {
        Ok(count) if forgotten <= batch_size => Ok(count),
        Ok(_) | Err(_) => Err(BrokerError::ClaimContract(format!(
            "{}: deleted {forgotten} rows with a batch of {batch_size}",
            statement.name(),
        ))),
    }
}

/// Read the candidates of one stale RUNNING pass by the beat age
/// (`BEAT_AGE`): at most `batch` rows.
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
    let stale_after = Some(thresholds.stale_after.get());
    let finalizing_stale_after = Some(thresholds.finalizing_stale_after.get());
    let owner: Option<&str> = None;
    let claimed_before: Option<DateTime<Utc>> = None;
    let batch_size = batch.get();
    let bound = statement
        .bind()
        .value(&BEAT_AGE)
        .value(&stale_after)
        .value(&finalizing_stale_after)
        .value(&owner)
        .value(&claimed_before)
        .value(&batch_size)
        .finish();
    let rows = run_reserved(pool, log, &bound).await?;
    Ok(read_rows(&rows, statement, stale_candidate)?)
}

/// Read the RUNNING rows of a gone owner (`OWNER_GONE`): at most `batch`
/// started rows that the owner claimed before it was first read unheld.
///
/// One statement on `pool`, checked out as reserved work. Reads only.
///
/// Errors: [`BrokerError::Statement`]; [`BrokerError::RowDecode`] for a row
/// that does not have the scan's columns.
pub(crate) async fn scan_gone_owner_running(
    pool: &CorePool,
    log: StatementLog,
    owner: &GoneOwner,
    batch: RecoveryBatch,
) -> Result<Vec<GoneOwnerCandidate>, BrokerError> {
    let statement = &STALE_RUNNING_SCAN;
    let no_threshold: Option<i32> = None;
    let owner_id = Some(owner.worker_id.as_str());
    let claimed_before = Some(owner.unheld_since);
    let batch_size = batch.get();
    let bound = statement
        .bind()
        .value(&OWNER_GONE)
        .value(&no_threshold)
        .value(&no_threshold)
        .value(&owner_id)
        .value(&claimed_before)
        .value(&batch_size)
        .finish();
    let rows = run_reserved(pool, log, &bound).await?;
    Ok(read_rows(&rows, statement, gone_owner_candidate)?)
}

fn gone_owner_candidate(
    row: &Row,
    statement: &TypedStatement,
) -> Result<GoneOwnerCandidate, RowDecodeError> {
    Ok(GoneOwnerCandidate {
        facts: stale_candidate(row, statement)?,
        claimed_at: column(row, statement, "claimed_at")?,
    })
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
/// the row, its latest runner beat and the database clock: by the beat age,
/// or (a gone owner) by the owner and the claim generation. It writes the
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
    let (evidence, stale_after, finalizing_stale_after, owner, owner_claimed_at): (
        &str,
        Option<i32>,
        Option<i32>,
        Option<&str>,
        Option<DateTime<Utc>>,
    ) = match &command.evidence {
        StaleEvidence::BeatAge(thresholds) => (
            BEAT_AGE,
            Some(thresholds.stale_after.get()),
            Some(thresholds.finalizing_stale_after.get()),
            None,
            None,
        ),
        StaleEvidence::OwnerGone(claim) => (
            OWNER_GONE,
            None,
            None,
            Some(claim.worker_id.as_str()),
            Some(claim.claimed_at),
        ),
    };
    let bound = statement
        .bind()
        .value(&command.task_id)
        .value(&evidence)
        .value(&stale_after)
        .value(&finalizing_stale_after)
        .value(&owner)
        .value(&owner_claimed_at)
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
/// commits (`python/horsies/core/brokers/postgres.py:3307-3323`).
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

/// What one requeue batch judges a CLAIMED row by (`p_evidence`).
#[derive(Debug, Clone, Copy)]
enum RequeueEvidence<'a> {
    /// `BEAT_AGE`: no claimer beat within the threshold, and claimed longer
    /// ago than it.
    BeatAge(ThresholdMs),
    /// `OWNER_GONE`: claimed by the gone owner before it was first read
    /// unheld.
    OwnerGone(&'a GoneOwner),
}

/// Requeue one bounded batch of stale CLAIMED rows: at most `batch` rows
/// whose claim is older than `stale_after` and whose claimer sent no beat
/// within it go back to PENDING with every claim and run field cleared. A
/// workflow task whose node is not runnable stays (the orphan audit cancels
/// it); a row that another transaction holds is skipped. Sends no queue
/// wake (as Python).
///
/// One statement on `pool`, checked out as reserved work; it commits alone.
/// Returns the number of rows requeued.
///
/// Errors: [`BrokerError::Statement`]; [`BrokerError::RowDecode`] when the
/// statement returns no count; [`BrokerError::ClaimContract`] for a count
/// outside 0 to `batch`.
pub(crate) async fn requeue_stale_claims(
    pool: &CorePool,
    log: StatementLog,
    stale_after: ThresholdMs,
    batch: RecoveryBatch,
) -> Result<u32, BrokerError> {
    requeue_batch(pool, log, RequeueEvidence::BeatAge(stale_after), batch).await
}

/// Requeue one bounded batch of the CLAIMED rows of a gone owner: at most
/// `batch` rows that the owner claimed before it was first read unheld go
/// back to PENDING with every claim and run field cleared, whatever their
/// beats. A workflow task whose node is not runnable stays; a row that
/// another transaction holds is skipped. Sends no queue wake.
///
/// One statement on `pool`, checked out as reserved work; it commits alone.
/// Returns the number of rows requeued.
///
/// Errors: as [`requeue_stale_claims`].
pub(crate) async fn requeue_gone_owner_claims(
    pool: &CorePool,
    log: StatementLog,
    owner: &GoneOwner,
    batch: RecoveryBatch,
) -> Result<u32, BrokerError> {
    requeue_batch(pool, log, RequeueEvidence::OwnerGone(owner), batch).await
}

/// One `horsies_requeue_stale_claims` batch with `evidence`.
async fn requeue_batch(
    pool: &CorePool,
    log: StatementLog,
    evidence: RequeueEvidence<'_>,
    batch: RecoveryBatch,
) -> Result<u32, BrokerError> {
    let statement = &REQUEUE_STALE_CLAIMS;
    let (kind, stale_after_ms, owner, claimed_before): (
        &str,
        Option<i32>,
        Option<&str>,
        Option<DateTime<Utc>>,
    ) = match evidence {
        RequeueEvidence::BeatAge(stale_after) => (BEAT_AGE, Some(stale_after.get()), None, None),
        RequeueEvidence::OwnerGone(owner) => (
            OWNER_GONE,
            None,
            Some(owner.worker_id.as_str()),
            Some(owner.unheld_since),
        ),
    };
    let batch_size = batch.get();
    let bound = statement
        .bind()
        .value(&kind)
        .value(&stale_after_ms)
        .value(&owner)
        .value(&claimed_before)
        .value(&batch_size)
        .finish();
    let row = one_row(run_reserved(pool, log, &bound).await?, statement)?;
    let requeued: i32 = column(&row, statement, "requeued")?;
    match u32::try_from(requeued) {
        Ok(count) if requeued <= batch_size => Ok(count),
        Ok(_) | Err(_) => Err(BrokerError::ClaimContract(format!(
            "{}: requeued {requeued} rows with a batch of {batch_size}",
            statement.name(),
        ))),
    }
}

/// Rows per statement of a stale CLAIMED requeue pass (Python
/// `REQUEUE_STALE_CLAIMS_BATCH_SIZE`).
pub(crate) const STALE_CLAIMED_REQUEUE_BATCH: RecoveryBatch = match RecoveryBatch::new(500) {
    Some(batch) => batch,
    None => panic!("500 rows is a recovery batch"),
};

/// Statements per stale CLAIMED requeue pass (Python
/// `REQUEUE_STALE_CLAIMS_MAX_BATCHES_PER_PASS`). A pass that reaches it
/// stops, and the next pass goes on.
pub const STALE_CLAIMED_REQUEUE_MAX_BATCHES_PER_PASS: u32 = 200;

/// How a stale CLAIMED requeue pass ended. Every batch of the pass
/// committed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StaleClaimsRequeuePass {
    /// A batch came back short: no eligible row was left when it ran.
    Drained { requeued: u64 },
    /// The pass ran [`STALE_CLAIMED_REQUEUE_MAX_BATCHES_PER_PASS`] full
    /// batches and stopped. Eligible rows can remain for the next pass.
    BoundReached { requeued: u64 },
}

/// A stale CLAIMED requeue pass stopped at a failure. The batches before
/// the failure committed and requeued `requeued_before` rows; the failed
/// batch wrote nothing, or its outcome is unknown (`error` tells which). No
/// later batch ran. A failure before the first batch (the pools, the client
/// family check) has `requeued_before` 0.
#[derive(Debug, thiserror::Error)]
#[error("the stale CLAIMED requeue failed after requeueing {requeued_before} task(s): {error}")]
pub struct StaleClaimsRequeueFailed {
    pub requeued_before: u64,
    pub error: BrokerError,
}

/// One stale CLAIMED requeue pass on `pool`: `horsies_requeue_stale_claims`
/// in batches of [`STALE_CLAIMED_REQUEUE_BATCH`] rows until a batch is
/// short, and at most [`STALE_CLAIMED_REQUEUE_MAX_BATCHES_PER_PASS`] batches.
/// Each batch is one statement checked out as reserved work, and commits
/// alone. Logs nothing; each caller reports the end of the pass.
///
/// Errors: [`StaleClaimsRequeueFailed`] with the rows of the batches before
/// the failed one.
pub(crate) async fn requeue_stale_claimed_pass(
    pool: &CorePool,
    log: StatementLog,
    stale_after: ThresholdMs,
) -> Result<StaleClaimsRequeuePass, StaleClaimsRequeueFailed> {
    let full_batch = STALE_CLAIMED_REQUEUE_BATCH.get().unsigned_abs();
    requeue_pass_with(
        full_batch,
        STALE_CLAIMED_REQUEUE_MAX_BATCHES_PER_PASS,
        || requeue_stale_claims(pool, log, stale_after, STALE_CLAIMED_REQUEUE_BATCH),
    )
    .await
}

/// One requeue pass over the CLAIMED rows of a gone owner on `pool`:
/// [`requeue_gone_owner_claims`] in batches of
/// [`STALE_CLAIMED_REQUEUE_BATCH`] rows until a batch is short, and at most
/// [`STALE_CLAIMED_REQUEUE_MAX_BATCHES_PER_PASS`] batches. Each batch commits
/// alone. Logs nothing.
///
/// Errors: [`StaleClaimsRequeueFailed`] with the rows of the batches before
/// the failed one.
pub(crate) async fn requeue_gone_owner_pass(
    pool: &CorePool,
    log: StatementLog,
    owner: &GoneOwner,
) -> Result<StaleClaimsRequeuePass, StaleClaimsRequeueFailed> {
    let full_batch = STALE_CLAIMED_REQUEUE_BATCH.get().unsigned_abs();
    requeue_pass_with(
        full_batch,
        STALE_CLAIMED_REQUEUE_MAX_BATCHES_PER_PASS,
        || requeue_gone_owner_claims(pool, log, owner, STALE_CLAIMED_REQUEUE_BATCH),
    )
    .await
}

/// The loop of a requeue pass: at most `max_batches` calls of
/// `requeue_batch`, each of which requeues at most `full_batch` rows and
/// returns how many it requeued. Stops at the first short batch, at the
/// first failure, or after `max_batches` full batches.
async fn requeue_pass_with<F, Fut>(
    full_batch: u32,
    max_batches: u32,
    mut requeue_batch: F,
) -> Result<StaleClaimsRequeuePass, StaleClaimsRequeueFailed>
where
    F: FnMut() -> Fut,
    Fut: std::future::Future<Output = Result<u32, BrokerError>>,
{
    let mut requeued: u64 = 0;
    for _ in 0..max_batches {
        let rows = match requeue_batch().await {
            Ok(rows) => rows,
            Err(error) => {
                return Err(StaleClaimsRequeueFailed {
                    requeued_before: requeued,
                    error,
                })
            }
        };
        requeued += u64::from(rows);
        if rows < full_batch {
            return Ok(StaleClaimsRequeuePass::Drained { requeued });
        }
    }
    Ok(StaleClaimsRequeuePass::BoundReached { requeued })
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

    /// The pass of `requeue_pass_with` over planned batch answers, and the
    /// number of batches it ran.
    async fn planned_pass(
        full_batch: u32,
        max_batches: u32,
        answers: Vec<Result<u32, BrokerError>>,
    ) -> (
        Result<StaleClaimsRequeuePass, StaleClaimsRequeueFailed>,
        usize,
    ) {
        let mut answers = std::collections::VecDeque::from(answers);
        let mut batches = 0;
        let pass = requeue_pass_with(full_batch, max_batches, || {
            batches += 1;
            std::future::ready(answers.pop_front().expect("a planned batch answer"))
        })
        .await;
        (pass, batches)
    }

    #[tokio::test]
    async fn a_short_batch_ends_the_pass_as_drained() {
        let (pass, batches) = planned_pass(500, 200, vec![Ok(500), Ok(500), Ok(7)]).await;
        assert_eq!(
            pass.expect("no batch failed"),
            StaleClaimsRequeuePass::Drained { requeued: 1007 }
        );
        assert_eq!(batches, 3);
    }

    #[tokio::test]
    async fn full_batches_up_to_the_bound_end_the_pass_with_the_bound_reached() {
        let (pass, batches) = planned_pass(2, 3, vec![Ok(2), Ok(2), Ok(2)]).await;
        assert_eq!(
            pass.expect("no batch failed"),
            StaleClaimsRequeuePass::BoundReached { requeued: 6 }
        );
        assert_eq!(batches, 3);
    }

    /// Every column of a `horsies_stale_running_scan` row, with a NULL
    /// `started_at`.
    static SCAN_ROW_WITHOUT_A_START: TypedStatement = TypedStatement::new(
        "scan_row_without_a_start",
        "SELECT gen_random_uuid() AS task_id, 0 AS retry_count, 3 AS max_retries, \
                NULL::text AS task_options, NULL::timestamptz AS good_until, \
                'w-1'::text AS claimed_by_worker_id, 7 AS worker_pid, \
                'host-a'::text AS worker_hostname, 'proc-a'::text AS worker_process_name, \
                NULL::timestamptz AS started_at, now() AS db_now",
        &[],
    );

    /// A scan row without a start is outside the scan's contract; its read
    /// fails closed on `started_at`.
    #[tokio::test]
    #[serial_test::serial]
    async fn a_scan_row_without_a_start_does_not_read() {
        let pool = crate::test_support::migrated_pool().await;
        let core = crate::broker::pools::core_pool_for_test_url(pool.url());
        let rows = run_reserved(
            &core,
            StatementLog::Off,
            &SCAN_ROW_WITHOUT_A_START.bind().finish(),
        )
        .await
        .expect("the row statement runs");

        let refused = read_rows(&rows, &SCAN_ROW_WITHOUT_A_START, stale_candidate)
            .expect_err("a row without a start does not read");

        assert!(
            matches!(
                refused,
                RowDecodeError::Column {
                    column: "started_at",
                    ..
                }
            ),
            "{refused:?}"
        );
        core.close();
    }

    #[tokio::test]
    async fn a_failed_batch_reports_the_rows_of_the_batches_before_it() {
        let failure = BrokerError::ClaimContract("planned failure".to_owned());
        let (pass, batches) = planned_pass(500, 200, vec![Ok(500), Err(failure)]).await;
        let failed = pass.expect_err("the second batch failed");
        assert_eq!(failed.requeued_before, 500);
        assert!(matches!(failed.error, BrokerError::ClaimContract(_)));
        assert_eq!(batches, 2);
    }
}
