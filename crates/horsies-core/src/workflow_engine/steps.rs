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

use sqlx::PgPool;
use uuid::Uuid;

use crate::broker::core_rows::{column, one_row, query_in, query_on, roll_back, RowDecodeError};
use crate::broker::{BrokerError, PostgresBroker};
use crate::core::config::payload::PayloadPolicy;
use crate::core::config::retention::RetentionConfig;
use crate::core::registry::workflow::WorkflowSpecRegistry;
use crate::db::{
    BoundStatement, CheckoutClass, CorePool, IsolationLevel, NotSentCause, ParameterType,
    PooledConnection, Row, StatementFailure, TypedStatement,
};
use crate::workflow_engine::engine::propagate_terminal_child_in_tx;
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

/// The columns that the core reads from a row of `horsies_workflow_step_row`.
/// `workflow_id` and `task_index` are read on `CHILD_START` rows only.
macro_rules! step_row_select {
    ($call:literal) => {
        concat!(
            "SELECT row_kind, disposition, workflow_id, advance_child_workflow_id, detail, ",
            "task_index FROM ",
            $call
        )
    };
}

/// `horsies_workflow_node_finish(task_id, wake, dependents)`: phase 2 of a
/// finished workflow task. Parameters: the task id, whether the statement
/// sends the capacity wake of the task's queue, and [`Dependents`].
pub(crate) static NODE_FINISH: TypedStatement = TypedStatement::new(
    "horsies_workflow_node_finish",
    step_row_select!("horsies_workflow_node_finish($1, $2, $3)"),
    &[
        ParameterType::Uuid,
        ParameterType::Bool,
        ParameterType::Text,
    ],
);

/// `horsies_workflow_child_finish(child_workflow_id)`: advances the parent
/// node of a terminal child workflow, one level.
pub(crate) static CHILD_FINISH: TypedStatement = TypedStatement::new(
    "horsies_workflow_child_finish",
    step_row_select!("horsies_workflow_child_finish($1)"),
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
    /// A step row has a `row_kind` other than `OUTCOME`, `CHILD_START` and
    /// `SOURCE`.
    #[error("`{function}` returned a row of kind {row_kind:?}, which is outside its contract")]
    UnknownRowKind {
        function: &'static str,
        row_kind: String,
    },
    /// A step call returned `count` `OUTCOME` rows; its contract is exactly
    /// one.
    #[error("`{function}` returned {count} OUTCOME rows; its contract is exactly one")]
    OutcomeRows {
        function: &'static str,
        count: usize,
    },
    /// A step that ran alone in autocommit returned owed child starts. Its
    /// form owes none: the function refuses a child start for a node whose
    /// stored fact says that no sub-workflow node is below it. The step
    /// committed.
    #[error(
        "`{function}` ran in autocommit and returned {count} owed child starts, \
         which commit only inside a transaction"
    )]
    ChildStartsInAutocommit {
        function: &'static str,
        count: usize,
    },
    /// A chain of parent advances did not end within `bound` levels: a
    /// workflow tree has at most that many.
    #[error("a chain of parent advances did not end within {bound} levels")]
    ParentChainTooLong { bound: usize },
}

/// The disposition text of a step function's `OUTCOME` row, as a closed
/// type.
pub(crate) trait StepDisposition: Sized {
    /// The disposition that `text` names, or `None` outside the function's
    /// set.
    fn from_text(text: &str) -> Option<Self>;
}

/// Whether `horsies_workflow_node_finish` reads the dependents of the node.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Dependents {
    /// Phase 1 reported that no node depends on this node: the function
    /// promotes nothing (`NONE`).
    NoneToRead,
    /// The function reads the dependents and promotes them (`READ`).
    Read,
}

impl Dependents {
    /// The value the function takes.
    pub(crate) fn as_sql(self) -> &'static str {
        match self {
            Self::NoneToRead => "NONE",
            Self::Read => "READ",
        }
    }
}

/// Why `horsies_workflow_node_apply` kept a phase-2 record and wrote nothing:
/// the stored result did not pass its check.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SourceFailure {
    /// No history row at the locator and no quarantine copy (`SOURCE_ABSENT`).
    Absent,
    /// The source row belongs to another task (`SOURCE_STATE_CONFLICT`).
    StateConflict,
    /// The history schema version differs (`SOURCE_VERSION_CONFLICT`).
    VersionConflict,
    /// The digest of the stored bytes differs (`SOURCE_DIGEST_MISMATCH`).
    DigestMismatch,
}

impl SourceFailure {
    /// The disposition text.
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::Absent => "SOURCE_ABSENT",
            Self::StateConflict => "SOURCE_STATE_CONFLICT",
            Self::VersionConflict => "SOURCE_VERSION_CONFLICT",
            Self::DigestMismatch => "SOURCE_DIGEST_MISMATCH",
        }
    }
}

/// The disposition of `horsies_workflow_node_finish` and
/// `horsies_workflow_phase2_recover` (the dispositions of
/// `horsies_workflow_node_apply`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum NodeFinishDisposition {
    /// The node was written and the workflow is live: the failure policy,
    /// promotion and completion ran.
    Applied,
    /// The node was written; the workflow had ended, so nothing more ran.
    AppliedWorkflowEnded,
    /// The node was terminal already (a replay): nothing written.
    AlreadyApplied,
    /// No phase-2 record, and the node is not terminal: nothing written.
    PendingAbsent,
    /// The stored result did not pass its check: nothing written, the record
    /// is kept for phase-2 recovery.
    Retained(SourceFailure),
}

impl StepDisposition for NodeFinishDisposition {
    fn from_text(text: &str) -> Option<Self> {
        match text {
            "APPLIED" => Some(Self::Applied),
            "APPLIED_WORKFLOW_ENDED" => Some(Self::AppliedWorkflowEnded),
            "ALREADY_APPLIED" => Some(Self::AlreadyApplied),
            "PENDING_ABSENT" => Some(Self::PendingAbsent),
            "SOURCE_ABSENT" => Some(Self::Retained(SourceFailure::Absent)),
            "SOURCE_STATE_CONFLICT" => Some(Self::Retained(SourceFailure::StateConflict)),
            "SOURCE_VERSION_CONFLICT" => Some(Self::Retained(SourceFailure::VersionConflict)),
            "SOURCE_DIGEST_MISMATCH" => Some(Self::Retained(SourceFailure::DigestMismatch)),
            _ => None,
        }
    }
}

/// The disposition of `horsies_workflow_child_finish`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ChildFinishDisposition {
    /// The parent node was written, then the parent's progression ran.
    Applied,
    /// The parent node was terminal already: nothing written.
    AlreadyApplied,
    /// No workflow has the child id: nothing written.
    ChildAbsent,
    /// The workflow has no parent: nothing written.
    NotAChild,
    /// The child is not terminal (`detail`: its status): nothing written.
    ChildNotTerminal,
    /// The parent workflow row is gone: nothing written.
    ParentAbsent,
}

impl StepDisposition for ChildFinishDisposition {
    fn from_text(text: &str) -> Option<Self> {
        match text {
            "APPLIED" => Some(Self::Applied),
            "ALREADY_APPLIED" => Some(Self::AlreadyApplied),
            "CHILD_ABSENT" => Some(Self::ChildAbsent),
            "NOT_A_CHILD" => Some(Self::NotAChild),
            "CHILD_NOT_TERMINAL" => Some(Self::ChildNotTerminal),
            "PARENT_ABSENT" => Some(Self::ParentAbsent),
            _ => None,
        }
    }
}

/// Whether a step ended a child workflow whose parent node must advance.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ParentAdvance {
    /// The step ended no child workflow that has a parent.
    NotOwed,
    /// The step ended this child workflow. Its parent node advances after the
    /// commit, one transaction per level ([`advance_parents`]).
    Owed { child_workflow_id: Uuid },
}

/// The `OUTCOME` row of a step call.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StepOutcome<D> {
    pub(crate) disposition: D,
    pub(crate) parent_advance: ParentAdvance,
    /// The function's diagnostic text: an observed status, a stored name, or
    /// a quarantine verdict, by function and disposition.
    pub(crate) detail: Option<String>,
}

/// A sub-workflow node that a step promoted and whose child start is owed in
/// the step's transaction (a `CHILD_START` row).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct OwedChildStart {
    pub(crate) workflow_id: Uuid,
    pub(crate) task_index: i32,
}

/// The rows of one step call.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StepRows<D> {
    pub(crate) outcome: StepOutcome<D>,
    pub(crate) owed_child_starts: Vec<OwedChildStart>,
}

/// How a step run in a transaction ended.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum TransactionStep<D> {
    /// The step committed and owed no child start.
    Committed(StepOutcome<D>),
    /// The step owed child starts. `horsies_workflow_child_start` (#751) is
    /// not in the baseline yet, so the transaction was rolled back and nothing
    /// of the step committed. The caller runs the operation on the engine code
    /// that starts the child in the step's own transaction.
    RolledBackForChildStarts(Vec<OwedChildStart>),
}

/// The values the engine code that starts child workflows reads: the
/// registry with the child definitions, and the payload and retention
/// policies of new task rows.
#[derive(Clone, Copy)]
pub(crate) struct EngineContext<'a> {
    pub(crate) registry: &'a WorkflowSpecRegistry,
    pub(crate) payload: &'a PayloadPolicy,
    pub(crate) retention: &'a RetentionConfig,
}

/// A parent advance of a chain failed; the chain goes on from
/// `child_workflow_id`. The levels before it committed.
#[derive(Debug)]
pub(crate) struct ParentAdvanceFailure {
    pub(crate) child_workflow_id: Uuid,
    pub(crate) error: WorkflowError,
}

/// The most parent advances one chain takes: a workflow tree has at most 33
/// levels (depth 0 to 32), so a chain from the deepest child has 32.
pub(crate) const MAX_PARENT_ADVANCES: usize = 33;

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

/// Phase 2 of a finished workflow task in one statement in autocommit, on the
/// worker reserve ([`CheckoutClass::Reserved`]): the form for a node whose
/// phase-1 outcome says the step owes no child start.
///
/// `wake`: the statement sends the capacity wake of the task's queue
/// (`task_queue_<queue>`, payload `capacity`), delivered at its commit.
///
/// Errors: [`WorkflowError::Broker`] when the statement failed (nothing of it
/// committed, or `OutcomeUnknown`); [`WorkflowError::StepContract`] for a row
/// outside the contract, including owed child starts
/// ([`StepContractError::ChildStartsInAutocommit`]; the step committed).
pub(crate) async fn node_finish_alone(
    broker: &PostgresBroker,
    task_id: Uuid,
    wake: bool,
    dependents: Dependents,
) -> Result<StepOutcome<NodeFinishDisposition>, WorkflowError> {
    let dependents_text = dependents.as_sql();
    let rows = run_alone(
        broker,
        CheckoutClass::Reserved,
        &NODE_FINISH
            .bind()
            .value(&task_id)
            .value(&wake)
            .value(&dependents_text)
            .finish(),
    )
    .await?;
    let step = step_rows::<NodeFinishDisposition>(&rows, &NODE_FINISH)?;
    match step.owed_child_starts.len() {
        0 => Ok(step.outcome),
        count => Err(StepContractError::ChildStartsInAutocommit {
            function: NODE_FINISH.name(),
            count,
        }
        .into()),
    }
}

/// Phase 2 of a finished workflow task in a transaction (`BEGIN`, the step
/// with the dependents read, `COMMIT`), on the worker reserve: the form for a
/// node from which a sub-workflow node is reachable, and for a phase 2 whose
/// phase-1 outcome is not at hand.
///
/// Errors as [`node_finish_alone`]; a failed `BEGIN`, `COMMIT` or `ROLLBACK`
/// is [`WorkflowError::Broker`] (a `COMMIT` with a lost reply is
/// `OutcomeUnknown`).
pub(crate) async fn node_finish_in_transaction(
    broker: &PostgresBroker,
    task_id: Uuid,
    wake: bool,
) -> Result<TransactionStep<NodeFinishDisposition>, WorkflowError> {
    let dependents_text = Dependents::Read.as_sql();
    run_in_transaction(
        broker,
        CheckoutClass::Reserved,
        &NODE_FINISH
            .bind()
            .value(&task_id)
            .value(&wake)
            .value(&dependents_text)
            .finish(),
    )
    .await
}

/// Advances the parent nodes above a child workflow that a committed step
/// ended: `horsies_workflow_child_finish`, one transaction per level, while
/// the `OUTCOME` row names the next ended child. Python:
/// `_drive_parent_propagations` (`core/worker/finalize.py:2248-2290`).
///
/// A level that owes child starts is rolled back and runs on the engine code
/// ([`legacy_child_finish`]), which advances that level and every level above
/// it in one sqlx transaction; the chain ends there.
///
/// Errors: [`ParentAdvanceFailure`] names the child whose level failed; the
/// levels before it committed, and that level wrote nothing or its outcome is
/// unknown. A chain longer than [`MAX_PARENT_ADVANCES`] is
/// [`StepContractError::ParentChainTooLong`].
pub(crate) async fn advance_parents(
    broker: &PostgresBroker,
    class: CheckoutClass,
    first_child_workflow_id: Uuid,
    engine: EngineContext<'_>,
) -> Result<(), ParentAdvanceFailure> {
    let mut child_workflow_id = first_child_workflow_id;
    for _level in 0..MAX_PARENT_ADVANCES {
        let advanced = advance_parent(broker, class, child_workflow_id, engine)
            .await
            .map_err(|error| ParentAdvanceFailure {
                child_workflow_id,
                error,
            })?;
        match advanced {
            ParentAdvance::NotOwed => return Ok(()),
            ParentAdvance::Owed {
                child_workflow_id: next,
            } => child_workflow_id = next,
        }
    }
    Err(ParentAdvanceFailure {
        child_workflow_id,
        error: StepContractError::ParentChainTooLong {
            bound: MAX_PARENT_ADVANCES,
        }
        .into(),
    })
}

/// One level of [`advance_parents`]. Returns the next level owed.
async fn advance_parent(
    broker: &PostgresBroker,
    class: CheckoutClass,
    child_workflow_id: Uuid,
    engine: EngineContext<'_>,
) -> Result<ParentAdvance, WorkflowError> {
    let step = run_in_transaction::<ChildFinishDisposition>(
        broker,
        class,
        &CHILD_FINISH.bind().value(&child_workflow_id).finish(),
    )
    .await?;
    match step {
        TransactionStep::Committed(outcome) => {
            match outcome.disposition {
                ChildFinishDisposition::Applied => {}
                ChildFinishDisposition::AlreadyApplied
                | ChildFinishDisposition::ChildAbsent
                | ChildFinishDisposition::NotAChild
                | ChildFinishDisposition::ChildNotTerminal
                | ChildFinishDisposition::ParentAbsent => tracing::debug!(
                    child_workflow_id = %child_workflow_id,
                    disposition = ?outcome.disposition,
                    detail = ?outcome.detail,
                    "parent node not advanced; nothing written",
                ),
            }
            Ok(outcome.parent_advance)
        }
        TransactionStep::RolledBackForChildStarts(owed) => {
            tracing::info!(
                child_workflow_id = %child_workflow_id,
                owed_child_starts = owed.len(),
                "parent advance owes child starts; the engine code advances the parent",
            );
            legacy_child_finish(broker.pool(), child_workflow_id, engine).await?;
            Ok(ParentAdvance::NotOwed)
        }
    }
}

/// The status and stored result of a child workflow, for
/// [`legacy_child_finish`].
const LEGACY_CHILD_STATE_SQL: &str = "SELECT status, result FROM horsies_workflows WHERE id = $1";

/// Advances the parent node of a terminal child workflow on the engine code,
/// in one sqlx transaction with every level above it
/// (`engine::propagate_terminal_child_in_tx`). Used only for a level that owes
/// child starts, until `horsies_workflow_child_start` (#751) is in the
/// baseline. A child that is gone writes nothing.
async fn legacy_child_finish(
    pool: &PgPool,
    child_workflow_id: Uuid,
    engine: EngineContext<'_>,
) -> Result<(), WorkflowError> {
    let mut transaction = pool.begin().await?;
    let child: Option<(String, Option<String>)> = sqlx::query_as(LEGACY_CHILD_STATE_SQL)
        .bind(child_workflow_id)
        .fetch_optional(&mut *transaction)
        .await?;
    let Some((status, result)) = child else {
        transaction.rollback().await?;
        return Ok(());
    };
    propagate_terminal_child_in_tx(
        &mut transaction,
        child_workflow_id,
        &status,
        result.as_deref(),
        engine.registry,
        engine.payload,
        engine.retention,
    )
    .await?;
    transaction.commit().await?;
    Ok(())
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

/// Runs one step statement in a transaction on the broker's main core pool:
/// `BEGIN`, the step, then `COMMIT` when the step owes no child start, else
/// `ROLLBACK` ([`TransactionStep::RolledBackForChildStarts`]). A row outside
/// the contract rolls the transaction back. The write token of the broker
/// handle is held until the transaction ends.
///
/// Errors: [`WorkflowError::Broker`] for a failed checkout, `BEGIN`, step,
/// `COMMIT` or `ROLLBACK` (after a failed statement the connection is
/// discarded and nothing commits; a `COMMIT` with a lost reply is
/// `OutcomeUnknown`); [`WorkflowError::StepContract`] for a row outside the
/// contract (nothing committed).
pub(crate) async fn run_in_transaction<D: StepDisposition>(
    broker: &PostgresBroker,
    class: CheckoutClass,
    statement: &BoundStatement<'_>,
) -> Result<TransactionStep<D>, WorkflowError> {
    let (open, _write) = broker.core_pools_for_write()?;
    let mut connection = checkout(open.main(), class, statement.statement()).await?;
    let mut transaction = connection
        .begin(IsolationLevel::ServerDefault)
        .await
        .map_err(BrokerError::from)?;
    let rows = query_in(&mut transaction, broker.statement_log(), statement)
        .await
        .map_err(BrokerError::from)?;
    let step = match step_rows::<D>(&rows, statement.statement()) {
        Ok(step) => step,
        Err(contract) => {
            roll_back(transaction).await?;
            return Err(contract.into());
        }
    };
    match step.owed_child_starts.is_empty() {
        true => {
            transaction.commit().await.map_err(BrokerError::from)?;
            Ok(TransactionStep::Committed(step.outcome))
        }
        false => {
            roll_back(transaction).await?;
            Ok(TransactionStep::RolledBackForChildStarts(
                step.owed_child_starts,
            ))
        }
    }
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

/// Decodes the rows of a step function (`SETOF horsies_workflow_step_row`):
/// exactly one `OUTCOME` row, any number of `CHILD_START` rows, and the
/// `SOURCE` rows of the owed child starts (not read: the child start that
/// reads them is #751). Row order does not matter. Pure.
pub(crate) fn step_rows<D: StepDisposition>(
    rows: &[Row],
    statement: &TypedStatement,
) -> Result<StepRows<D>, StepContractError> {
    let mut outcomes: Vec<StepOutcome<D>> = Vec::with_capacity(1);
    let mut owed_child_starts = Vec::new();
    for row in rows {
        let row_kind: String = column(row, statement, "row_kind")?;
        match row_kind.as_str() {
            "OUTCOME" => outcomes.push(outcome_row(row, statement)?),
            "CHILD_START" => owed_child_starts.push(OwedChildStart {
                workflow_id: column(row, statement, "workflow_id")?,
                task_index: column(row, statement, "task_index")?,
            }),
            "SOURCE" => {}
            _ => {
                return Err(StepContractError::UnknownRowKind {
                    function: statement.name(),
                    row_kind,
                })
            }
        }
    }
    let count = outcomes.len();
    match (outcomes.pop(), count) {
        (Some(outcome), 1) => Ok(StepRows {
            outcome,
            owed_child_starts,
        }),
        _ => Err(StepContractError::OutcomeRows {
            function: statement.name(),
            count,
        }),
    }
}

/// Decodes one `OUTCOME` row. Pure.
fn outcome_row<D: StepDisposition>(
    row: &Row,
    statement: &TypedStatement,
) -> Result<StepOutcome<D>, StepContractError> {
    let text: String = column(row, statement, "disposition")?;
    let Some(disposition) = D::from_text(&text) else {
        return Err(StepContractError::UnknownOutcome {
            function: statement.name(),
            outcome: text,
        });
    };
    let advance: Option<Uuid> = column(row, statement, "advance_child_workflow_id")?;
    Ok(StepOutcome {
        disposition,
        parent_advance: match advance {
            Some(child_workflow_id) => ParentAdvance::Owed { child_workflow_id },
            None => ParentAdvance::NotOwed,
        },
        detail: column(row, statement, "detail")?,
    })
}

#[cfg(test)]
mod tests;
