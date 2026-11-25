//! The read of the runs whose task an operator cancelled: the read of the
//! cancel router of a worker after its listener lost cancel notices.
//!
//! The cancel of a RUNNING task (`horsies_cancel_task` with
//! `include_running`) moves its row to history and sends one notice on
//! `horsies_task_cancel`. A notice that the listener lost (a reconnect, a
//! full payload queue) is found by this read: of the runs that the worker
//! holds, the ones whose row is gone from the live table and whose history
//! row is an administrative cancel (`CANCEL_ADMIN`) at the same claim
//! generation (`horsies_cancelled_runs`).
//!
//! The read is bounded: at most [`MAX_CANCELLED_RUNS_READ`] runs per
//! statement, the wait for a pooled connection is at most
//! [`CANCELLED_RUNS_CHECKOUT_WAIT`], and the statement gets its cancel
//! request after [`CANCELLED_RUNS_STATEMENT_BOUND`].

use std::time::Duration;

use chrono::{DateTime, Utc};
use uuid::Uuid;

use crate::broker::core_rows::{column, StatementLog};
use crate::broker::error::BrokerError;
use crate::broker::postgres::PostgresBroker;
use crate::db::{
    CheckoutClass, CorePool, NotSentCause, ParameterType, Row, StatementBound, StatementFailure,
    TypedStatement,
};

/// The most runs of one read statement. A caller with more runs reads them
/// in parts of this size.
pub const MAX_CANCELLED_RUNS_READ: usize = 1_000;

/// The longest wait of the read for a pooled connection.
pub const CANCELLED_RUNS_CHECKOUT_WAIT: Duration = Duration::from_secs(2);

/// The longest run of the statement of the read before its cancel request.
pub const CANCELLED_RUNS_STATEMENT_BOUND: Duration = Duration::from_secs(5);

/// One run of a task: the task id and the claim generation (`claimed_at`)
/// of the claim that runs it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct RunClaim {
    pub task_id: Uuid,
    pub claimed_at: DateTime<Utc>,
}

/// The runs of `$1` and `$2` (in matching order) that an operator
/// cancelled: `horsies_cancelled_runs`. Reads only; locks nothing.
static CANCELLED_RUNS: TypedStatement = TypedStatement::new(
    "cancelled_runs",
    "SELECT task_id, claimed_at FROM horsies_cancelled_runs($1::uuid[], $2::timestamptz[])",
    &[ParameterType::UuidArray, ParameterType::TimestamptzArray],
);

/// Why a read of cancelled runs did not run.
#[derive(Debug, thiserror::Error)]
pub enum CancelledRunsReadError {
    /// More runs than one statement reads; nothing was sent.
    #[error(
        "a read of cancelled runs takes at most {max} runs, not {count}",
        max = MAX_CANCELLED_RUNS_READ
    )]
    TooManyRuns { count: usize },
    #[error(transparent)]
    Broker(#[from] BrokerError),
}

impl PostgresBroker {
    /// The runs of `runs` whose task an operator cancelled while the run
    /// held its claim: one statement on the main core pool, checked out as
    /// reserved work (the read finds the cancels that a worker must deliver
    /// to running task code), within the bounds of the module docs. A run
    /// of `runs` that is not in the answer was not cancelled at its claim
    /// generation, or its history row was pruned. Reads only.
    ///
    /// Errors: [`CancelledRunsReadError::TooManyRuns`] for more than
    /// [`MAX_CANCELLED_RUNS_READ`] runs (nothing sent);
    /// [`CancelledRunsReadError::Broker`]: [`BrokerError::PoolSettings`]
    /// (nothing sent); [`BrokerError::Statement`]: no connection within
    /// [`CANCELLED_RUNS_CHECKOUT_WAIT`] (nothing sent), a refusal or a cancel
    /// at [`CANCELLED_RUNS_STATEMENT_BOUND`], transient when its cause may
    /// clear; [`BrokerError::RowDecode`] for a row outside the statement's
    /// columns.
    pub async fn read_cancelled_runs(
        &self,
        runs: &[RunClaim],
    ) -> Result<Vec<RunClaim>, CancelledRunsReadError> {
        let open = self.core_pools_for_read()?;
        read_cancelled_runs(open.main(), self.statement_log(), runs).await
    }
}

/// [`PostgresBroker::read_cancelled_runs`] on `pool`.
pub(crate) async fn read_cancelled_runs(
    pool: &CorePool,
    log: StatementLog,
    runs: &[RunClaim],
) -> Result<Vec<RunClaim>, CancelledRunsReadError> {
    match runs.len() {
        0 => return Ok(Vec::new()),
        count if count > MAX_CANCELLED_RUNS_READ => {
            return Err(CancelledRunsReadError::TooManyRuns { count })
        }
        _ => {}
    }
    let statement = &CANCELLED_RUNS;
    let task_ids: Vec<Uuid> = runs.iter().map(|run| run.task_id).collect();
    let claim_generations: Vec<DateTime<Utc>> = runs.iter().map(|run| run.claimed_at).collect();
    let bound = statement
        .bind()
        .value(&task_ids)
        .value(&claim_generations)
        .finish();
    log.log(statement);
    let mut connection = pool
        .checkout_within(CheckoutClass::Reserved, CANCELLED_RUNS_CHECKOUT_WAIT)
        .await
        .map_err(|cause| {
            BrokerError::Statement(StatementFailure::NotSent {
                statement: statement.name(),
                cause: NotSentCause::Checkout(cause),
            })
        })?;
    let rows = connection
        .query(
            &bound,
            StatementBound::CancelAfter(CANCELLED_RUNS_STATEMENT_BOUND),
        )
        .await
        .map_err(BrokerError::from)?;
    rows.iter()
        .map(|row| cancelled_run(row).map_err(CancelledRunsReadError::Broker))
        .collect()
}

/// The run of one row: the task id and the claim generation.
fn cancelled_run(row: &Row) -> Result<RunClaim, BrokerError> {
    let statement = &CANCELLED_RUNS;
    Ok(RunClaim {
        task_id: column::<Uuid>(row, statement, "task_id")?,
        claimed_at: column::<DateTime<Utc>>(row, statement, "claimed_at")?,
    })
}
