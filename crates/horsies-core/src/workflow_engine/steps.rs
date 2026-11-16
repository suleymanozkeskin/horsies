//! The workflow step functions of the baseline (`0005_workflow_functions.sql`)
//! on the core statement layer.
//!
//! Each workflow operation is one call of its SQL function. The function takes
//! every lock, writes every status and decides the outcome; the core binds
//! typed values and reads plain rows of built-in types into a closed outcome.
//!
//! - A step runs on the main core pool of the broker. The caller names the
//!   checkout class: producer and handle operations pass the reserve gate
//!   ([`CheckoutClass::Batch`]); the worker's per-task writes and the reaper
//!   take the reserve ([`CheckoutClass::Reserved`]).
//! - A step holds a write token of the broker handle, so a close of the
//!   broker waits for it.
//! - A failure is a [`BrokerError`]: `Statement` (the three outcomes of the
//!   statement layer), or `PoolSettings` when a closed handle cannot open its
//!   pools again. A row that breaks the outcome contract of its function is a
//!   [`StepContractError`].

use uuid::Uuid;

use crate::broker::core_rows::{column, one_row, query_on, RowDecodeError};
use crate::broker::{BrokerError, PostgresBroker};
use crate::db::{
    BoundStatement, CheckoutClass, CorePool, NotSentCause, ParameterType, PooledConnection, Row,
    StatementFailure, TypedStatement,
};
use crate::workflow_engine::error::WorkflowError;

// ---------------------------------------------------------------------------
// Statements
// ---------------------------------------------------------------------------

/// `horsies_workflow_pause(workflow_id)`: pauses a RUNNING workflow and its
/// RUNNING descendants, abandons the CLAIMED tasks behind their live nodes and
/// puts those nodes back to READY, in one statement.
pub(crate) static PAUSE: TypedStatement = TypedStatement::new(
    "horsies_workflow_pause",
    "SELECT outcome, paused_workflow_ids, abandoned_task_ids
     FROM horsies_workflow_pause($1)",
    &[ParameterType::Uuid],
);

/// `horsies_workflow_cancel(workflow_id)`: cancels a workflow tree, the tasks
/// behind its ENQUEUED nodes, and skips its waiting nodes, in one statement.
pub(crate) static CANCEL: TypedStatement = TypedStatement::new(
    "horsies_workflow_cancel",
    "SELECT outcome, cancelled_workflow_ids, cancelled_task_ids
     FROM horsies_workflow_cancel($1)",
    &[ParameterType::Uuid],
);

// ---------------------------------------------------------------------------
// Outcomes
// ---------------------------------------------------------------------------

/// A row of a step function that does not have the shape of its function's
/// outcome contract: the core and the installed baseline disagree. Not
/// retryable: the same call meets it again.
#[derive(Debug, thiserror::Error)]
pub enum StepContractError {
    /// The function returned no row where its contract requires one, or a
    /// column is missing or does not decode into its Rust type (a NULL where
    /// the outcome requires a value included).
    #[error(transparent)]
    Row(#[from] RowDecodeError),
    /// The outcome text is outside the closed set of the function.
    #[error("`{function}` returned the outcome {outcome:?}, which is outside its contract")]
    UnknownOutcome {
        function: &'static str,
        outcome: String,
    },
}

/// What `horsies_workflow_pause` did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum PauseOutcome {
    /// The workflow and its RUNNING descendants are PAUSED (the root first).
    /// The CLAIMED tasks behind their live nodes were abandoned, and those
    /// nodes are READY with no task.
    Paused {
        paused_workflow_ids: Vec<Uuid>,
        abandoned_task_ids: Vec<Uuid>,
    },
    /// The workflow exists and is not RUNNING. Nothing was written.
    NotRunning,
    /// No workflow has this id. Nothing was written.
    NotFound,
}

/// What `horsies_workflow_cancel` did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum CancelOutcome {
    /// The workflow and its PENDING, RUNNING and PAUSED descendants are
    /// CANCELLED (the root first). The tasks behind their ENQUEUED nodes moved
    /// to history as cancelled; their waiting nodes are SKIPPED.
    Cancelled {
        cancelled_workflow_ids: Vec<Uuid>,
        cancelled_task_ids: Vec<Uuid>,
    },
    /// The workflow is COMPLETED, FAILED, CANCELLED or EXPIRED. Nothing was
    /// written.
    NotCancellable,
    /// No workflow has this id. Nothing was written.
    NotFound,
}

// ---------------------------------------------------------------------------
// Operations
// ---------------------------------------------------------------------------

/// Pauses a workflow tree in one statement in autocommit, as producer work
/// ([`CheckoutClass::Batch`]).
///
/// Errors: [`WorkflowError::Broker`] when the statement failed (nothing of it
/// committed, or its outcome is unknown: `OutcomeUnknown`);
/// [`WorkflowError::StepContract`] for a row outside the function's contract.
/// Does not check the client family; the caller does.
pub(crate) async fn pause(
    broker: &PostgresBroker,
    workflow_id: Uuid,
) -> Result<PauseOutcome, WorkflowError> {
    let rows = run_alone(
        broker,
        CheckoutClass::Batch,
        &PAUSE.bind().value(&workflow_id).finish(),
    )
    .await?;
    Ok(pause_outcome(rows)?)
}

/// Cancels a workflow tree in one statement in autocommit, as producer work
/// ([`CheckoutClass::Batch`]).
///
/// Errors as [`pause`]. Does not check the client family; the caller does.
pub(crate) async fn cancel(
    broker: &PostgresBroker,
    workflow_id: Uuid,
) -> Result<CancelOutcome, WorkflowError> {
    let rows = run_alone(
        broker,
        CheckoutClass::Batch,
        &CANCEL.bind().value(&workflow_id).finish(),
    )
    .await?;
    Ok(cancel_outcome(rows)?)
}

// ---------------------------------------------------------------------------
// Runners
// ---------------------------------------------------------------------------

/// Runs one step statement alone, in autocommit, on the broker's main core
/// pool, and reads every row. The write token of the broker handle is held
/// until the statement ends.
///
/// Errors: [`BrokerError::PoolSettings`] when a closed handle cannot open its
/// pools again; [`BrokerError::Statement`] for a failed checkout (`NotSent`)
/// or the outcome of the statement.
pub(crate) async fn run_alone(
    broker: &PostgresBroker,
    class: CheckoutClass,
    statement: &BoundStatement<'_>,
) -> Result<Vec<Row>, BrokerError> {
    let (open, _write) = broker.core_pools_for_write()?;
    let mut connection = checkout(open.main(), class, statement.statement()).await?;
    Ok(query_on(&mut connection, broker.statement_log(), statement).await?)
}

/// Checks out a connection of `pool` with `class` for a step whose first
/// statement is `first`.
///
/// Errors: the failed checkout as `first`'s `NotSent`; nothing was sent.
pub(crate) async fn checkout(
    pool: &CorePool,
    class: CheckoutClass,
    first: &TypedStatement,
) -> Result<PooledConnection, BrokerError> {
    pool.checkout(class).await.map_err(|cause| {
        BrokerError::Statement(StatementFailure::NotSent {
            statement: first.name(),
            cause: NotSentCause::Checkout(cause),
        })
    })
}

// ---------------------------------------------------------------------------
// Decoders
// ---------------------------------------------------------------------------

/// Decodes the one row of `horsies_workflow_pause`. Pure.
pub(crate) fn pause_outcome(rows: Vec<Row>) -> Result<PauseOutcome, StepContractError> {
    let row = one_row(rows, &PAUSE)?;
    let outcome: String = column(&row, &PAUSE, "outcome")?;
    match outcome.as_str() {
        "PAUSED" => Ok(PauseOutcome::Paused {
            paused_workflow_ids: column(&row, &PAUSE, "paused_workflow_ids")?,
            abandoned_task_ids: column(&row, &PAUSE, "abandoned_task_ids")?,
        }),
        "NOT_RUNNING" => Ok(PauseOutcome::NotRunning),
        "NOT_FOUND" => Ok(PauseOutcome::NotFound),
        _ => Err(StepContractError::UnknownOutcome {
            function: PAUSE.name(),
            outcome,
        }),
    }
}

/// Decodes the one row of `horsies_workflow_cancel`. Pure.
pub(crate) fn cancel_outcome(rows: Vec<Row>) -> Result<CancelOutcome, StepContractError> {
    let row = one_row(rows, &CANCEL)?;
    let outcome: String = column(&row, &CANCEL, "outcome")?;
    match outcome.as_str() {
        "CANCELLED" => Ok(CancelOutcome::Cancelled {
            cancelled_workflow_ids: column(&row, &CANCEL, "cancelled_workflow_ids")?,
            cancelled_task_ids: column(&row, &CANCEL, "cancelled_task_ids")?,
        }),
        "NOT_CANCELLABLE" => Ok(CancelOutcome::NotCancellable),
        "NOT_FOUND" => Ok(CancelOutcome::NotFound),
        _ => Err(StepContractError::UnknownOutcome {
            function: CANCEL.name(),
            outcome,
        }),
    }
}

#[cfg(test)]
mod tests;
