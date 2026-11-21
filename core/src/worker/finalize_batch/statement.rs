//! The statement of one batch: `horsies_complete_tasks_fused` of the 0004
//! lifecycle functions, on the worker pool.
//!
//! - One statement in autocommit for 1 to 64 rows of one worker: the task
//!   ids, the claim generations and the result texts as arrays, the worker
//!   id once, and the wake as the constant TRUE. The function calls
//!   `horsies_complete_task_fused` once per row in task id order; each row
//!   keeps its own fence and outcome, and the rows of one queue send one
//!   capacity wake.
//! - The checkout passes the reserve gate of the worker pool
//!   ([`CheckoutClass::Batch`]) and ends with the statement: no flusher holds
//!   a connection between batches.
//! - The function returns one row per input row with its 1-based position
//!   (`ordinality`). The outcomes are put back in input order by that
//!   position; SQL result order is not read.

use std::sync::Arc;

use uuid::Uuid;

use super::{BatchOutcomes, BatchWriteError, SuccessBatch, SuccessBatchWrite};
use crate::broker::core_rows::{column, RowDecodeError, StatementLog};
use crate::broker::terminalization::log_terminalization_outcome;
use crate::broker::PostgresBroker;
use crate::core::lifecycle::outcomes::{decode_raw, RawOutcomeRow};
use crate::core::lifecycle::TerminalizationOutcome;
use crate::db::{
    CheckoutClass, CorePool, ParameterType, Row, StatementBound, Transience, TypedStatement,
};

/// `horsies_complete_tasks_fused`. Parameters in order: task ids, claim
/// generations, result texts (one element per row, in input order), worker
/// id. The wake argument is the constant TRUE (as Python).
pub(crate) static COMPLETE_PLAIN_SUCCESSES: TypedStatement = TypedStatement::new(
    "complete_plain_successes",
    "SELECT * FROM horsies_complete_tasks_fused($1, $2, $3, $4, TRUE)",
    &[
        ParameterType::UuidArray,
        ParameterType::TimestamptzArray,
        ParameterType::TextArray,
        ParameterType::Text,
    ],
);

/// The columns of each returned row, in the order of the function's result.
const OUTCOME_COLUMNS: [&str; 10] = [
    "task_id",
    "ordinality",
    "outcome",
    "terminal_at",
    "terminalization_kind",
    "observed_status",
    "observed_worker_id",
    "observed_claimed_at",
    "guard_kind",
    "observed_guard",
];

/// Write `batch` for `worker_id` in one statement on `pool`.
///
/// Outcomes: `Ok` with one outcome per row in input order. `Applied` rows
/// committed with the statement; a refused row (`AlreadyApplied`,
/// `LostClaim`, `SourceStateConflict`, `TaskAbsent`) wrote nothing.
///
/// Errors: [`BatchWriteError::NotSent`] and [`BatchWriteError::Refused`]
/// (no row changed), [`BatchWriteError::CommitUnknown`] (the reply was
/// lost; a replay classifies each row), [`BatchWriteError::ContractBroken`]
/// (rows that do not read, a position missing or repeated, an outcome of
/// another task). Does not retry.
pub(crate) async fn complete_plain_successes(
    pool: &CorePool,
    log: StatementLog,
    worker_id: &str,
    batch: &SuccessBatch,
) -> Result<BatchOutcomes, BatchWriteError> {
    let task_ids: Vec<Uuid> = batch.rows().iter().map(|row| row.task_id).collect();
    let claimed_ats: Vec<chrono::DateTime<chrono::Utc>> =
        batch.rows().iter().map(|row| row.claimed_at).collect();
    let results: Vec<&str> = batch
        .rows()
        .iter()
        .map(|row| row.result_json.as_str())
        .collect();
    let statement = &COMPLETE_PLAIN_SUCCESSES;
    let bound = statement
        .bind()
        .value(&task_ids)
        .value(&claimed_ats)
        .value(&results)
        .value(&worker_id)
        .finish();
    log.log(statement);
    let rows = pool
        .query(
            CheckoutClass::Batch,
            &bound,
            StatementBound::ConnectionTimeouts,
        )
        .await
        .map_err(|failure| BatchWriteError::from(&failure))?;
    let outcomes = outcomes_in_input_order(&rows, batch.rows().len())?;
    for outcome in &outcomes {
        log_terminalization_outcome("horsies_complete_tasks_fused", outcome);
    }
    BatchOutcomes::for_batch(batch, outcomes)
}

/// Decode every row and put it at its position. Pure.
///
/// Errors: [`BatchWriteError::ContractBroken`] for a row that does not read,
/// a position outside `1..=expected`, a repeated position, or a missing one.
fn outcomes_in_input_order(
    rows: &[Row],
    expected: usize,
) -> Result<Vec<TerminalizationOutcome>, BatchWriteError> {
    let mut by_position: Vec<Option<TerminalizationOutcome>> = vec![None; expected];
    for row in rows {
        let outcome = decode_outcome(row)?;
        let position = outcome
            .ordinality()
            .and_then(|ordinality| usize::try_from(ordinality).ok())
            .filter(|position| (1..=expected).contains(position))
            .ok_or_else(|| BatchWriteError::ContractBroken {
                detail: format!(
                    "{}: task {} at ordinality {:?}, outside 1..={expected}",
                    COMPLETE_PLAIN_SUCCESSES.name(),
                    outcome.task_id(),
                    outcome.ordinality()
                ),
            })?;
        let slot = &mut by_position[position - 1];
        if slot.is_some() {
            return Err(BatchWriteError::ContractBroken {
                detail: format!(
                    "{}: ordinality {position} returned twice",
                    COMPLETE_PLAIN_SUCCESSES.name()
                ),
            });
        }
        *slot = Some(outcome);
    }
    by_position
        .into_iter()
        .enumerate()
        .map(|(index, outcome)| {
            outcome.ok_or_else(|| BatchWriteError::ContractBroken {
                detail: format!(
                    "{}: no row for ordinality {}",
                    COMPLETE_PLAIN_SUCCESSES.name(),
                    index + 1
                ),
            })
        })
        .collect()
}

/// One returned row as its typed outcome. Fails closed on a column that is
/// missing, unexpected or of another type, and on an outcome the decoder
/// does not know.
fn decode_outcome(row: &Row) -> Result<TerminalizationOutcome, BatchWriteError> {
    let names: Vec<&str> = row.columns().iter().map(|column| column.name()).collect();
    if names != OUTCOME_COLUMNS {
        return Err(BatchWriteError::ContractBroken {
            detail: format!(
                "{}: the row has the columns {names:?}, not {OUTCOME_COLUMNS:?}",
                COMPLETE_PLAIN_SUCCESSES.name()
            ),
        });
    }
    let raw = raw_outcome(row).map_err(|error| BatchWriteError::ContractBroken {
        detail: error.to_string(),
    })?;
    decode_raw(&raw).map_err(|error| BatchWriteError::ContractBroken {
        detail: format!("{}: {error}", COMPLETE_PLAIN_SUCCESSES.name()),
    })
}

fn raw_outcome(row: &Row) -> Result<RawOutcomeRow, RowDecodeError> {
    let statement = &COMPLETE_PLAIN_SUCCESSES;
    Ok(RawOutcomeRow {
        task_id: column(row, statement, "task_id")?,
        ordinality: column(row, statement, "ordinality")?,
        outcome: column(row, statement, "outcome")?,
        terminal_at: column(row, statement, "terminal_at")?,
        terminalization_kind: column(row, statement, "terminalization_kind")?,
        observed_status: column(row, statement, "observed_status")?,
        observed_worker_id: column(row, statement, "observed_worker_id")?,
        observed_claimed_at: column(row, statement, "observed_claimed_at")?,
        guard_kind: column(row, statement, "guard_kind")?,
        observed_guard: column(row, statement, "observed_guard")?,
    })
}

/// The batch write of a worker: [`complete_plain_successes`] on the worker
/// pool of its broker, for its worker id.
pub(crate) struct WorkerPoolSuccessWrite {
    broker: Arc<PostgresBroker>,
    worker_id: String,
}

impl WorkerPoolSuccessWrite {
    pub(crate) fn new(broker: Arc<PostgresBroker>, worker_id: String) -> WorkerPoolSuccessWrite {
        WorkerPoolSuccessWrite { broker, worker_id }
    }
}

impl SuccessBatchWrite for WorkerPoolSuccessWrite {
    /// Errors: as [`complete_plain_successes`]; the core pools that cannot
    /// be built are [`BatchWriteError::NotSent`], with nothing sent.
    async fn write(&self, batch: &SuccessBatch) -> Result<BatchOutcomes, BatchWriteError> {
        let (open, _write) =
            self.broker
                .core_pools_for_write()
                .map_err(|error| BatchWriteError::NotSent {
                    transience: match error.is_retryable() {
                        true => Transience::Transient,
                        false => Transience::Permanent,
                    },
                    detail: error.to_string(),
                    cause: error.failure_cause(),
                })?;
        complete_plain_successes(
            open.main(),
            self.broker.statement_log(),
            &self.worker_id,
            batch,
        )
        .await
    }
}
