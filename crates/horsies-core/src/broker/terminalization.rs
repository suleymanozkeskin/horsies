//! Persistence adapter for the terminalization vocabulary.
//!
//! Each [`TerminalizationCommand`] is one typed statement,
//! `SELECT * FROM horsies_<fn>(…)` with positional parameters. Every returned
//! row goes through the one shared decoder, and the cardinality contract of
//! the command shape is enforced:
//!
//! - single-task commands return exactly one row;
//! - id-keyed batches (`AbandonOwnedNodes`, `CancelOwnedNodes`) return the
//!   exact ordinal set `1..=n`, which is verified and used to restore caller
//!   input order — SQL result order is never trusted;
//! - discovery and workflow-scoped batches return one row per transition
//!   they made; zero rows is a valid answer, and inventing an outcome for
//!   work that did not happen is what the row-per-transition contract exists
//!   to prevent.
//!
//! Three forms run a command:
//! - [`terminalize`] runs the statement alone on a core pool, in autocommit:
//!   the statement is its own transaction and its locks end with it. The
//!   checkout is reserved work ([`CheckoutClass::Reserved`]): a lifecycle
//!   write never waits on the reserve gate.
//! - [`terminalize_and_commit_if`] runs the statement in a transaction of
//!   its own on a core pool, checked out as reserved work, and commits it
//!   only when the caller's check accepts every outcome.
//! - [`terminalize_in_tx`] runs the same SQL in the caller's sqlx
//!   transaction. The functions never commit, so a coupled workflow-node
//!   write belongs in the same transaction as the transition it proves.

use sqlx::postgres::{PgArguments, PgRow};
use sqlx::{Postgres, Transaction};

use crate::broker::core_rows::{roll_back, StatementLog};
use crate::core::lifecycle::operations::function_name_of;
use crate::core::lifecycle::outcomes::{decode_outcome_pg_row, decode_outcome_row};
use crate::core::lifecycle::{TerminalizationCommand, TerminalizationOutcome};
use crate::db::{
    BoundStatement, CheckoutClass, CorePool, IsolationLevel, NotSentCause, ParameterType, Row,
    StatementBound, StatementFailure, TypedStatement,
};
use crate::log_targets::TERMINALIZATION_OUTCOME;

use super::error::BrokerError;

/// The last argument (wake) is TRUE: the function sends the capacity wake of
/// the locked row's queue (as Python).
const COMPLETE_TASK_FUSED_SQL: &str =
    "SELECT * FROM horsies_complete_task_fused($1, $2, $3::timestamptz, $4, TRUE)";
const EXPIRE_OWNED_CLAIM_SQL: &str = "SELECT * FROM horsies_expire_owned_claim($1, $2, $3, $4)";
const EXPIRE_PENDING_TASKS_SQL: &str =
    "SELECT * FROM horsies_expire_pending_tasks($1::integer, $2, $3)";
const CANCEL_OWNED_ORPHAN_SQL: &str =
    "SELECT * FROM horsies_cancel_owned_orphan($1, $2, $3::timestamptz)";
const CANCEL_ORPHANED_TASKS_SQL: &str = "SELECT * FROM horsies_cancel_orphaned_tasks($1::integer)";
const ABANDON_OWNED_NODE_SQL: &str =
    "SELECT * FROM horsies_abandon_owned_node($1, $2, $3::timestamptz)";
const ABANDON_OWNED_NODES_SQL: &str =
    "SELECT * FROM horsies_abandon_owned_nodes($1, $2::timestamptz[], $3)";
const ABANDON_NODES_OF_PAUSED_WORKFLOWS_SQL: &str =
    "SELECT * FROM horsies_abandon_nodes_of_paused_workflows($1)";
const CANCEL_OWNED_NODE_SQL: &str =
    "SELECT * FROM horsies_cancel_owned_node($1, $2, $3::timestamptz, $4::boolean)";
const CANCEL_OWNED_NODES_SQL: &str =
    "SELECT * FROM horsies_cancel_owned_nodes($1, $2::timestamptz[], $3)";
const CANCEL_NODES_OF_CANCELLED_WORKFLOW_SQL: &str =
    "SELECT * FROM horsies_cancel_nodes_of_cancelled_workflow($1)";

/// Task id, worker id, claim generation: the owned-claim fence of one task.
const OWNED_CLAIM_PARAMETERS: &[ParameterType] = &[
    ParameterType::Uuid,
    ParameterType::Text,
    ParameterType::Timestamptz,
];

/// Task ids, claim generations, worker id: the fence of an id-keyed batch.
const OWNED_CLAIM_BATCH_PARAMETERS: &[ParameterType] = &[
    ParameterType::UuidArray,
    ParameterType::TimestamptzArray,
    ParameterType::Text,
];

static COMPLETE_TASK_FUSED: TypedStatement = TypedStatement::new(
    "complete_task_fused",
    COMPLETE_TASK_FUSED_SQL,
    &[
        ParameterType::Uuid,
        ParameterType::Text,
        ParameterType::Timestamptz,
        ParameterType::Text,
    ],
);
static EXPIRE_OWNED_CLAIM: TypedStatement = TypedStatement::new(
    "expire_owned_claim",
    EXPIRE_OWNED_CLAIM_SQL,
    &[
        ParameterType::Uuid,
        ParameterType::Text,
        ParameterType::Text,
        ParameterType::Text,
    ],
);
static EXPIRE_PENDING_TASKS: TypedStatement = TypedStatement::new(
    "expire_pending_tasks",
    EXPIRE_PENDING_TASKS_SQL,
    &[
        ParameterType::Int4,
        ParameterType::Text,
        ParameterType::Text,
    ],
);
static CANCEL_OWNED_ORPHAN: TypedStatement = TypedStatement::new(
    "cancel_owned_orphan",
    CANCEL_OWNED_ORPHAN_SQL,
    OWNED_CLAIM_PARAMETERS,
);
static CANCEL_ORPHANED_TASKS: TypedStatement = TypedStatement::new(
    "cancel_orphaned_tasks",
    CANCEL_ORPHANED_TASKS_SQL,
    &[ParameterType::Int4],
);
static ABANDON_OWNED_NODE: TypedStatement = TypedStatement::new(
    "abandon_owned_node",
    ABANDON_OWNED_NODE_SQL,
    OWNED_CLAIM_PARAMETERS,
);
static ABANDON_OWNED_NODES: TypedStatement = TypedStatement::new(
    "abandon_owned_nodes",
    ABANDON_OWNED_NODES_SQL,
    OWNED_CLAIM_BATCH_PARAMETERS,
);
static ABANDON_NODES_OF_PAUSED_WORKFLOWS: TypedStatement = TypedStatement::new(
    "abandon_nodes_of_paused_workflows",
    ABANDON_NODES_OF_PAUSED_WORKFLOWS_SQL,
    &[ParameterType::UuidArray],
);
static CANCEL_OWNED_NODE: TypedStatement = TypedStatement::new(
    "cancel_owned_node",
    CANCEL_OWNED_NODE_SQL,
    &[
        ParameterType::Uuid,
        ParameterType::Text,
        ParameterType::Timestamptz,
        ParameterType::Bool,
    ],
);
static CANCEL_OWNED_NODES: TypedStatement = TypedStatement::new(
    "cancel_owned_nodes",
    CANCEL_OWNED_NODES_SQL,
    OWNED_CLAIM_BATCH_PARAMETERS,
);
static CANCEL_NODES_OF_CANCELLED_WORKFLOW: TypedStatement = TypedStatement::new(
    "cancel_nodes_of_cancelled_workflow",
    CANCEL_NODES_OF_CANCELLED_WORKFLOW_SQL,
    &[ParameterType::UuidArray],
);

/// The one statement of a command. Both forms run its SQL.
fn statement_of(command: &TerminalizationCommand) -> &'static TypedStatement {
    match command {
        TerminalizationCommand::CompleteTaskFused { .. } => &COMPLETE_TASK_FUSED,
        TerminalizationCommand::ExpireOwnedClaim { .. } => &EXPIRE_OWNED_CLAIM,
        TerminalizationCommand::ExpirePendingTasks { .. } => &EXPIRE_PENDING_TASKS,
        TerminalizationCommand::CancelOwnedOrphan { .. } => &CANCEL_OWNED_ORPHAN,
        TerminalizationCommand::CancelOrphanedTasks { .. } => &CANCEL_ORPHANED_TASKS,
        TerminalizationCommand::AbandonOwnedNode { .. } => &ABANDON_OWNED_NODE,
        TerminalizationCommand::AbandonOwnedNodes { .. } => &ABANDON_OWNED_NODES,
        TerminalizationCommand::AbandonNodesOfPausedWorkflows { .. } => {
            &ABANDON_NODES_OF_PAUSED_WORKFLOWS
        }
        TerminalizationCommand::CancelOwnedNode { .. } => &CANCEL_OWNED_NODE,
        TerminalizationCommand::CancelOwnedNodes { .. } => &CANCEL_OWNED_NODES,
        TerminalizationCommand::CancelNodesOfCancelledWorkflow { .. } => {
            &CANCEL_NODES_OF_CANCELLED_WORKFLOW
        }
    }
}

/// How many rows the command's function must report, per the wire contract.
#[derive(Clone, Copy)]
enum Cardinality {
    ExactlyOne,
    IdKeyedBatch { expected: usize },
    PerTransition,
}

fn cardinality_of(command: &TerminalizationCommand) -> Cardinality {
    match command {
        TerminalizationCommand::CompleteTaskFused { .. }
        | TerminalizationCommand::ExpireOwnedClaim { .. }
        | TerminalizationCommand::CancelOwnedOrphan { .. }
        | TerminalizationCommand::AbandonOwnedNode { .. }
        | TerminalizationCommand::CancelOwnedNode { .. } => Cardinality::ExactlyOne,
        TerminalizationCommand::AbandonOwnedNodes { fence }
        | TerminalizationCommand::CancelOwnedNodes { fence } => Cardinality::IdKeyedBatch {
            expected: fence.len(),
        },
        TerminalizationCommand::ExpirePendingTasks { .. }
        | TerminalizationCommand::CancelOrphanedTasks { .. }
        | TerminalizationCommand::AbandonNodesOfPausedWorkflows { .. }
        | TerminalizationCommand::CancelNodesOfCancelledWorkflow { .. } => {
            Cardinality::PerTransition
        }
    }
}

/// Where the statement of a command runs.
enum StatementTarget<'t, 'c> {
    /// Alone on the pool, in autocommit, on a connection checked out as
    /// reserved work.
    Pool(&'t CorePool),
    /// In an open transaction on a core connection; the owner of the
    /// transaction commits or rolls it back.
    Transaction(&'t mut crate::db::Transaction<'c>),
}

/// Bind the command's arguments to its statement and run it on `target`.
///
/// Bounded by the connection's own timeouts. Errors:
/// [`BrokerError::Statement`].
async fn run_command(
    target: &mut StatementTarget<'_, '_>,
    log: StatementLog,
    command: &TerminalizationCommand,
) -> Result<Vec<Row>, BrokerError> {
    let statement = statement_of(command);
    match command {
        TerminalizationCommand::CompleteTaskFused {
            task_id,
            fence,
            result_json,
        } => {
            let bound = statement.bind().value(task_id).value(&fence.worker_id);
            let bound = bound.value(&fence.claimed_at).value(result_json);
            run_bound(target, log, bound.finish()).await
        }
        TerminalizationCommand::ExpireOwnedClaim {
            task_id,
            fence,
            result_json,
            error_code,
        } => {
            let bound = statement.bind().value(task_id).value(&fence.worker_id);
            run_bound(
                target,
                log,
                bound.value(result_json).value(error_code).finish(),
            )
            .await
        }
        TerminalizationCommand::ExpirePendingTasks {
            batch_size,
            result_json,
            error_code,
        } => {
            let batch_size = batch_size.get();
            let bound = statement.bind().value(&batch_size).value(result_json);
            run_bound(target, log, bound.value(error_code).finish()).await
        }
        TerminalizationCommand::CancelOwnedOrphan { task_id, fence }
        | TerminalizationCommand::AbandonOwnedNode { task_id, fence } => {
            let bound = statement.bind().value(task_id).value(&fence.worker_id);
            run_bound(target, log, bound.value(&fence.claimed_at).finish()).await
        }
        TerminalizationCommand::CancelOwnedNode {
            task_id,
            fence,
            accepts_requeued_pending,
        } => {
            let bound = statement.bind().value(task_id).value(&fence.worker_id);
            let bound = bound
                .value(&fence.claimed_at)
                .value(accepts_requeued_pending);
            run_bound(target, log, bound.finish()).await
        }
        TerminalizationCommand::CancelOrphanedTasks { batch_size } => {
            let batch_size = batch_size.get();
            run_bound(target, log, statement.bind().value(&batch_size).finish()).await
        }
        TerminalizationCommand::AbandonOwnedNodes { fence }
        | TerminalizationCommand::CancelOwnedNodes { fence } => {
            let (task_ids, generations) = (fence.task_ids(), fence.generations());
            let worker_id = fence.worker_id();
            let bound = statement.bind().value(&task_ids).value(&generations);
            run_bound(target, log, bound.value(&worker_id).finish()).await
        }
        TerminalizationCommand::AbandonNodesOfPausedWorkflows { workflow_ids }
        | TerminalizationCommand::CancelNodesOfCancelledWorkflow { workflow_ids } => {
            run_bound(target, log, statement.bind().value(workflow_ids).finish()).await
        }
    }
}

/// Run one bound statement on `target`, bounded by the connection's own
/// timeouts.
async fn run_bound(
    target: &mut StatementTarget<'_, '_>,
    log: StatementLog,
    bound: BoundStatement<'_>,
) -> Result<Vec<Row>, BrokerError> {
    log.log(bound.statement());
    let rows = match target {
        StatementTarget::Pool(pool) => {
            pool.query(
                CheckoutClass::Reserved,
                &bound,
                StatementBound::ConnectionTimeouts,
            )
            .await?
        }
        StatementTarget::Transaction(transaction) => {
            transaction
                .query(&bound, StatementBound::ConnectionTimeouts)
                .await?
        }
    };
    Ok(rows)
}

/// The sqlx form of the command's statement, for [`terminalize_in_tx`].
fn fetch_query(command: &TerminalizationCommand) -> sqlx::query::Query<'_, Postgres, PgArguments> {
    let query = sqlx::query(statement_of(command).sql());
    match command {
        TerminalizationCommand::CompleteTaskFused {
            task_id,
            fence,
            result_json,
        } => query
            .bind(task_id)
            .bind(&fence.worker_id)
            .bind(fence.claimed_at)
            .bind(result_json),
        TerminalizationCommand::ExpireOwnedClaim {
            task_id,
            fence,
            result_json,
            error_code,
        } => query
            .bind(task_id)
            .bind(&fence.worker_id)
            .bind(result_json)
            .bind(error_code),
        TerminalizationCommand::ExpirePendingTasks {
            batch_size,
            result_json,
            error_code,
        } => query
            .bind(batch_size.get())
            .bind(result_json)
            .bind(error_code),
        TerminalizationCommand::CancelOwnedOrphan { task_id, fence }
        | TerminalizationCommand::AbandonOwnedNode { task_id, fence } => query
            .bind(task_id)
            .bind(&fence.worker_id)
            .bind(fence.claimed_at),
        TerminalizationCommand::CancelOrphanedTasks { batch_size } => query.bind(batch_size.get()),
        TerminalizationCommand::AbandonOwnedNodes { fence }
        | TerminalizationCommand::CancelOwnedNodes { fence } => query
            .bind(fence.task_ids())
            .bind(fence.generations())
            .bind(fence.worker_id().to_owned()),
        TerminalizationCommand::AbandonNodesOfPausedWorkflows { workflow_ids }
        | TerminalizationCommand::CancelNodesOfCancelledWorkflow { workflow_ids } => {
            query.bind(workflow_ids)
        }
        TerminalizationCommand::CancelOwnedNode {
            task_id,
            fence,
            accepts_requeued_pending,
        } => query
            .bind(task_id)
            .bind(&fence.worker_id)
            .bind(fence.claimed_at)
            .bind(accepts_requeued_pending),
    }
}

fn decode_rows(rows: &[Row], operation: &str) -> Result<Vec<TerminalizationOutcome>, BrokerError> {
    rows.iter()
        .map(|row| {
            decode_outcome_row(row)
                .map_err(|e| BrokerError::TerminalizationContract(format!("{operation}: {e}")))
        })
        .collect()
}

fn decode_pg_rows(
    rows: &[PgRow],
    operation: &str,
) -> Result<Vec<TerminalizationOutcome>, BrokerError> {
    rows.iter()
        .map(|row| {
            decode_outcome_pg_row(row)
                .map_err(|e| BrokerError::TerminalizationContract(format!("{operation}: {e}")))
        })
        .collect()
}

fn enforce_cardinality(
    outcomes: Vec<TerminalizationOutcome>,
    cardinality: Cardinality,
    operation: &str,
) -> Result<Vec<TerminalizationOutcome>, BrokerError> {
    match cardinality {
        Cardinality::ExactlyOne => {
            if outcomes.len() != 1 {
                return Err(BrokerError::TerminalizationContract(format!(
                    "{operation}: expected exactly one outcome row, got {}",
                    outcomes.len()
                )));
            }
            Ok(outcomes)
        }
        Cardinality::IdKeyedBatch { expected } => reorder_by_ordinal(outcomes, expected, operation),
        Cardinality::PerTransition => Ok(outcomes),
    }
}

/// Verify the ordinal contract and restore caller input order.
///
/// The exact ordinal set proves there is one answer per input and no answer
/// for anything else; result order is then irrelevant.
fn reorder_by_ordinal(
    outcomes: Vec<TerminalizationOutcome>,
    expected: usize,
    operation: &str,
) -> Result<Vec<TerminalizationOutcome>, BrokerError> {
    let mut by_ordinal: Vec<Option<TerminalizationOutcome>> = vec![None; expected];
    for outcome in outcomes {
        let Some(ordinal) = outcome.ordinality() else {
            return Err(BrokerError::TerminalizationContract(format!(
                "{operation}: returned a row without ordinality"
            )));
        };
        if ordinal < 1 || ordinal as usize > expected {
            return Err(BrokerError::TerminalizationContract(format!(
                "{operation}: ordinal {ordinal} outside expected set 1..={expected}"
            )));
        }
        let slot = &mut by_ordinal[(ordinal - 1) as usize];
        if slot.is_some() {
            return Err(BrokerError::TerminalizationContract(format!(
                "{operation}: returned duplicate ordinality {ordinal}"
            )));
        }
        *slot = Some(outcome);
    }
    let mut ordered = Vec::with_capacity(expected);
    for (index, slot) in by_ordinal.into_iter().enumerate() {
        match slot {
            Some(outcome) => ordered.push(outcome),
            None => {
                return Err(BrokerError::TerminalizationContract(format!(
                    "{operation}: ordinal set does not match its input: \
                     missing ordinal {}",
                    index + 1
                )));
            }
        }
    }
    Ok(ordered)
}

/// Emit the operation and what it reported through one tracing boundary.
///
/// Applied transitions are debug-level steady-state traffic. Every refusal
/// or replay is warning-level because its evidence is the only race
/// diagnosis that will ever exist for that moment.
pub fn log_terminalization_outcome(operation: &str, outcome: &TerminalizationOutcome) {
    match outcome {
        TerminalizationOutcome::Applied {
            task_id,
            ordinality,
            terminal_at,
            kind,
            observed,
        } => {
            tracing::debug!(
                target: TERMINALIZATION_OUTCOME,
                operation,
                %task_id,
                ?ordinality,
                %terminal_at,
                kind = kind.as_str(),
                ?observed,
                "terminalization applied"
            );
        }
        TerminalizationOutcome::AlreadyApplied {
            task_id,
            ordinality,
            terminal_at,
            kind,
            observed,
        } => {
            tracing::warn!(
                target: TERMINALIZATION_OUTCOME,
                operation,
                %task_id,
                ?ordinality,
                %terminal_at,
                kind = kind.as_str(),
                ?observed,
                "terminalization already applied"
            );
        }
        TerminalizationOutcome::LostClaim {
            task_id,
            ordinality,
            observed,
        } => {
            tracing::warn!(
                target: TERMINALIZATION_OUTCOME,
                operation,
                %task_id,
                ?ordinality,
                ?observed,
                "terminalization lost claim"
            );
        }
        TerminalizationOutcome::SourceStateConflict {
            task_id,
            ordinality,
            observed,
            evidence,
        } => {
            tracing::warn!(
                target: TERMINALIZATION_OUTCOME,
                operation,
                %task_id,
                ?ordinality,
                ?observed,
                ?evidence,
                "terminalization source-state conflict"
            );
        }
        TerminalizationOutcome::TaskAbsent {
            task_id,
            ordinality,
        } => {
            tracing::warn!(
                target: TERMINALIZATION_OUTCOME,
                operation,
                %task_id,
                ?ordinality,
                "terminalization task absent"
            );
        }
    }
}

/// Run a command as one statement alone on `pool` and decode everything it
/// reports.
///
/// The statement is its own transaction: it commits when it returns, and its
/// locks end with it. The checkout is reserved work; the bound is the
/// connection's own timeouts. Each outcome is logged
/// ([`log_terminalization_outcome`]).
///
/// Errors: [`BrokerError::Statement`] (nothing sent, refused by the server,
/// or an unknown outcome); [`BrokerError::TerminalizationContract`] when a
/// row or the row count breaks the wire contract. After `OutcomeUnknown` the
/// same command sent again reports a committed transition as
/// `AlreadyApplied`.
pub(crate) async fn terminalize(
    pool: &CorePool,
    log: StatementLog,
    command: &TerminalizationCommand,
) -> Result<Vec<TerminalizationOutcome>, BrokerError> {
    let rows = run_command(&mut StatementTarget::Pool(pool), log, command).await?;
    decode_outcomes(&rows, command)
}

/// Run a command in a transaction of its own on `pool` and commit it only
/// when `accept` accepts every outcome it reports. Returns what `accept`
/// returns.
///
/// `BEGIN`, the statement and `COMMIT` run on one connection checked out as
/// reserved work, each bound by the connection's own timeouts: two round
/// trips more than [`terminalize`]. Each outcome is logged
/// ([`log_terminalization_outcome`]) before `accept` reads it.
///
/// Errors (nothing committed, except after a `COMMIT` whose reply was lost):
/// - [`BrokerError::Statement`]: the checkout, `BEGIN`, the statement,
///   `COMMIT` or `ROLLBACK` failed. After a failed statement the connection
///   is discarded and the server rolls the transaction back.
///   `OutcomeUnknown` at `COMMIT`: the transaction may have committed.
/// - [`BrokerError::TerminalizationContract`] when a row or the row count
///   breaks the wire contract; the transaction is rolled back.
/// - The error of `accept`; the transaction is rolled back.
pub(crate) async fn terminalize_and_commit_if<T>(
    pool: &CorePool,
    log: StatementLog,
    command: &TerminalizationCommand,
    accept: impl FnOnce(&[TerminalizationOutcome]) -> Result<T, BrokerError>,
) -> Result<T, BrokerError> {
    let mut connection = pool
        .checkout(CheckoutClass::Reserved)
        .await
        .map_err(|cause| {
            BrokerError::Statement(StatementFailure::NotSent {
                statement: statement_of(command).name(),
                cause: NotSentCause::Checkout(cause),
            })
        })?;
    let mut transaction = connection.begin(IsolationLevel::ServerDefault).await?;
    // A failed statement leaves the transaction aborted or its connection
    // discarded: the transaction drops at the return, and nothing commits.
    let rows = run_command(
        &mut StatementTarget::Transaction(&mut transaction),
        log,
        command,
    )
    .await?;
    match decode_outcomes(&rows, command).and_then(|outcomes| accept(&outcomes)) {
        Ok(accepted) => {
            transaction.commit().await?;
            Ok(accepted)
        }
        Err(refused) => {
            roll_back(transaction).await?;
            Err(refused)
        }
    }
}

/// Run a command in the caller's open transaction on a core connection and
/// decode everything it reports. The caller commits or rolls back; a
/// statement that fails leaves the transaction aborted, so nothing of it
/// commits. Each outcome is logged ([`log_terminalization_outcome`]).
///
/// Errors: [`BrokerError::Statement`]; [`BrokerError::TerminalizationContract`]
/// when a row or the row count breaks the wire contract.
pub(crate) async fn terminalize_in_core_transaction(
    transaction: &mut crate::db::Transaction<'_>,
    log: StatementLog,
    command: &TerminalizationCommand,
) -> Result<Vec<TerminalizationOutcome>, BrokerError> {
    let rows = run_command(&mut StatementTarget::Transaction(transaction), log, command).await?;
    decode_outcomes(&rows, command)
}

/// Decode every row of a command's statement, enforce the cardinality of the
/// command, and log each outcome ([`log_terminalization_outcome`]). Pure but
/// for the log.
///
/// Errors: [`BrokerError::TerminalizationContract`] when a row or the row
/// count breaks the wire contract.
fn decode_outcomes(
    rows: &[Row],
    command: &TerminalizationCommand,
) -> Result<Vec<TerminalizationOutcome>, BrokerError> {
    let operation = function_name_of(command);
    let outcomes = enforce_cardinality(
        decode_rows(rows, operation)?,
        cardinality_of(command),
        operation,
    )?;
    for outcome in &outcomes {
        log_terminalization_outcome(operation, outcome);
    }
    Ok(outcomes)
}

/// Execute a command inside the caller's transaction.
///
/// The functions never commit; a coupled workflow-node write belongs in the
/// same transaction as the transition it proves.
pub async fn terminalize_in_tx(
    tx: &mut Transaction<'_, Postgres>,
    command: &TerminalizationCommand,
) -> Result<Vec<TerminalizationOutcome>, BrokerError> {
    let operation = function_name_of(command);
    let rows = fetch_query(command)
        .fetch_all(tx.as_mut())
        .await
        .map_err(BrokerError::Database)?;
    let outcomes = enforce_cardinality(
        decode_pg_rows(&rows, operation)?,
        cardinality_of(command),
        operation,
    )?;
    for outcome in &outcomes {
        log_terminalization_outcome(operation, outcome);
    }
    Ok(outcomes)
}

/// [`terminalize`] for tests that hold a sqlx pool: the command runs on a
/// main core pool on the same database, built for the call and closed after
/// it, so a test call site keeps its form.
#[cfg(test)]
pub(crate) mod on_test_pool {
    use super::{BrokerError, StatementLog, TerminalizationCommand, TerminalizationOutcome};

    pub(crate) async fn terminalize(
        pool: &sqlx::PgPool,
        command: &TerminalizationCommand,
    ) -> Result<Vec<TerminalizationOutcome>, BrokerError> {
        let core = crate::broker::pools::core_pool_for_test_pool(pool);
        let outcomes = super::terminalize(&core, StatementLog::Off, command).await;
        core.close();
        outcomes
    }
}

#[cfg(test)]
mod catalog_tests {
    //! Catalog conformance: the installed database program must match the
    //! vocabulary exactly — signatures, return shape, language, and the
    //! composite type's column list. A stale overload left behind by a
    //! changed argument list appears here as a difference.
    use super::*;
    use crate::core::lifecycle::TerminalizationKind;
    use serial_test::serial;
    use sqlx::Row;

    async fn migrated_pool() -> sqlx::PgPool {
        crate::broker::terminalization_matrix::migrated_pool().await
    }

    /// A row with two of the ten columns of the wire contract.
    static WRONG_ROW_SHAPE: TypedStatement = TypedStatement::new(
        "wrong_row_shape",
        "SELECT 'x'::varchar AS task_id, 'APPLIED'::text AS outcome",
        &[],
    );

    /// (function, rendered argument list, rendered result type, language):
    /// the eleven command functions, the miss classifier, the attempt
    /// encoder and the shared move.
    pub(super) const EXPECTED_SIGNATURES: [(&str, &str, &str, &str); 14] = [
        (
            "horsies_encode_task_attempts",
            "p_task_id uuid",
            "bytea",
            "sql",
        ),
        (
            "horsies_move_task_to_history",
            "p_task horsies_tasks, p_terminal_status text, p_terminalization_kind text, p_terminal_at timestamp with time zone, p_result text, p_error_code text, p_failed_reason text",
            "void",
            "plpgsql",
        ),
        (
            "horsies_terminalization_miss",
            "p_task_id uuid, p_kind text, p_fence_kind text, p_worker_id text, p_claimed_at timestamp with time zone",
            "SETOF horsies_terminalization_outcome",
            "plpgsql",
        ),
        (
            "horsies_complete_task_fused",
            "p_task_id uuid, p_worker_id text, p_claimed_at timestamp with time zone, p_result text, p_wake boolean",
            "TABLE(task_id uuid, ordinality bigint, outcome text, terminal_at timestamp with time zone, terminalization_kind text, observed_status text, observed_worker_id text, observed_claimed_at timestamp with time zone, guard_kind text, observed_guard jsonb)",
            "plpgsql",
        ),
        (
            "horsies_expire_owned_claim",
            "p_task_id uuid, p_worker_id text, p_result text, p_error_code text",
            "TABLE(task_id uuid, ordinality bigint, outcome text, terminal_at timestamp with time zone, terminalization_kind text, observed_status text, observed_worker_id text, observed_claimed_at timestamp with time zone, guard_kind text, observed_guard jsonb)",
            "plpgsql",
        ),
        (
            "horsies_expire_pending_tasks",
            "p_batch_size integer, p_result text, p_error_code text",
            "TABLE(task_id uuid, ordinality bigint, outcome text, terminal_at timestamp with time zone, terminalization_kind text, observed_status text, observed_worker_id text, observed_claimed_at timestamp with time zone, guard_kind text, observed_guard jsonb)",
            "plpgsql",
        ),
        (
            "horsies_cancel_owned_orphan",
            "p_task_id uuid, p_worker_id text, p_claimed_at timestamp with time zone",
            "TABLE(task_id uuid, ordinality bigint, outcome text, terminal_at timestamp with time zone, terminalization_kind text, observed_status text, observed_worker_id text, observed_claimed_at timestamp with time zone, guard_kind text, observed_guard jsonb)",
            "plpgsql",
        ),
        (
            "horsies_cancel_orphaned_tasks",
            "p_batch_size integer",
            "TABLE(task_id uuid, ordinality bigint, outcome text, terminal_at timestamp with time zone, terminalization_kind text, observed_status text, observed_worker_id text, observed_claimed_at timestamp with time zone, guard_kind text, observed_guard jsonb)",
            "plpgsql",
        ),
        (
            "horsies_abandon_owned_node",
            "p_task_id uuid, p_worker_id text, p_claimed_at timestamp with time zone",
            "SETOF horsies_terminalization_outcome",
            "plpgsql",
        ),
        (
            "horsies_abandon_owned_nodes",
            "p_ids uuid[], p_claimed_ats timestamp with time zone[], p_worker_id text",
            "SETOF horsies_terminalization_outcome",
            "plpgsql",
        ),
        (
            "horsies_abandon_nodes_of_paused_workflows",
            "p_workflow_ids uuid[]",
            "SETOF horsies_terminalization_outcome",
            "plpgsql",
        ),
        (
            "horsies_cancel_owned_node",
            "p_task_id uuid, p_worker_id text, p_claimed_at timestamp with time zone, p_accepts_requeued_pending boolean",
            "SETOF horsies_terminalization_outcome",
            "plpgsql",
        ),
        (
            "horsies_cancel_owned_nodes",
            "p_ids uuid[], p_claimed_ats timestamp with time zone[], p_worker_id text",
            "SETOF horsies_terminalization_outcome",
            "plpgsql",
        ),
        (
            "horsies_cancel_nodes_of_cancelled_workflow",
            "p_workflow_ids uuid[]",
            "SETOF horsies_terminalization_outcome",
            "plpgsql",
        ),
    ];

    #[tokio::test]
    #[serial]
    async fn installed_functions_match_vocabulary_signatures() {
        let pool = migrated_pool().await;
        let names: Vec<String> = EXPECTED_SIGNATURES
            .iter()
            .map(|(name, _, _, _)| (*name).to_owned())
            .collect();
        let rows = sqlx::query(
            "SELECT p.proname,
                    pg_get_function_arguments(p.oid) AS args,
                    pg_get_function_result(p.oid) AS result,
                    p.prokind::text AS prokind,
                    l.lanname
             FROM pg_proc p
             JOIN pg_language l ON l.oid = p.prolang
             WHERE p.proname = ANY($1)
             ORDER BY p.proname",
        )
        .bind(&names)
        .fetch_all(&pool)
        .await
        .expect("catalog query");

        assert_eq!(
            rows.len(),
            EXPECTED_SIGNATURES.len(),
            "one installed definition per function, no stale overloads"
        );
        let mut expected: Vec<(&str, &str, &str, &str)> = EXPECTED_SIGNATURES.to_vec();
        expected.sort_by_key(|(name, _, _, _)| *name);
        for (row, (name, args, result, language)) in rows.iter().zip(expected) {
            assert_eq!(row.get::<String, _>("proname"), name);
            assert_eq!(row.get::<String, _>("args"), args, "{name} arguments");
            assert_eq!(row.get::<String, _>("result"), result, "{name} result");
            assert_eq!(row.get::<String, _>("prokind"), "f", "{name} prokind");
            assert_eq!(row.get::<String, _>("lanname"), language, "{name} language");
        }
    }

    #[tokio::test]
    #[serial]
    async fn outcome_type_matches_wire_contract() {
        let pool = migrated_pool().await;
        let rows = sqlx::query(
            "SELECT a.attname, format_type(a.atttypid, a.atttypmod) AS type
             FROM pg_attribute a
             JOIN pg_type t ON t.typrelid = a.attrelid
             WHERE t.typname = 'horsies_terminalization_outcome'
               AND a.attnum > 0 AND NOT a.attisdropped
             ORDER BY a.attnum",
        )
        .fetch_all(&pool)
        .await
        .expect("type query");
        let columns: Vec<(String, String)> = rows
            .iter()
            .map(|r| (r.get("attname"), r.get("type")))
            .collect();
        let expected: Vec<(String, String)> = [
            ("task_id", "uuid"),
            ("ordinality", "bigint"),
            ("outcome", "text"),
            ("terminal_at", "timestamp with time zone"),
            ("terminalization_kind", "text"),
            ("observed_status", "text"),
            ("observed_worker_id", "character varying"),
            ("observed_claimed_at", "timestamp with time zone"),
            ("guard_kind", "text"),
            ("observed_guard", "jsonb"),
        ]
        .iter()
        .map(|(n, t)| ((*n).to_owned(), (*t).to_owned()))
        .collect();
        assert_eq!(columns, expected);
    }

    #[tokio::test]
    #[serial]
    async fn body_canary_fragments_present() {
        let pool = migrated_pool().await;
        let fused: String = sqlx::query_scalar(
            "SELECT pg_get_functiondef(
                'horsies_complete_task_fused(uuid, text, timestamptz, text, boolean)'::regprocedure
             )",
        )
        .fetch_one(&pool)
        .await
        .expect("fused def");
        // The generation half of the fence is pinned by behaviour
        // (`fused_stale_generation_is_lost_claim`), not by its text: its
        // NULL arm goes (#697). A miss passes the writer's own kind and fence
        // kind; the miss classifier owns the equivalence classes.
        for fragment in [
            "'COMPLETE_FUSED'",
            "v_ctx.claimed_by_worker_id = p_worker_id",
            "p_task_id, 'COMPLETE_FUSED', 'OWNER_AND_GENERATION', p_worker_id, p_claimed_at",
            "pg_notify('task_queue_' || v_ctx.queue_name, 'capacity')",
        ] {
            assert!(
                fused.contains(fragment),
                "fused body must contain {fragment:?}"
            );
        }

        let miss: String = sqlx::query_scalar(
            "SELECT pg_get_functiondef(
                'horsies_terminalization_miss(uuid, text, text, text, timestamptz)'::regprocedure
             )",
        )
        .fetch_one(&pool)
        .await
        .expect("miss def");
        for fragment in [
            "horsies_terminalization_family(v_provenance.terminalization_kind) IS NOT DISTINCT FROM v_family",
            "'ALREADY_APPLIED'",
            "'FOREIGN_TERMINALIZATION'",
            "'TASK_ABSENT'",
            "'LOST_CLAIM'",
        ] {
            assert!(
                miss.contains(fragment),
                "miss body must contain {fragment:?}"
            );
        }
    }

    #[test]
    fn kind_domain_rule_pins_the_frozen_vocabulary() {
        let definition = crate::core::history::column_rules::HISTORY_COLUMN_RULES
            .iter()
            .find(|rule| rule.name == "horsies_task_history_terminalization_kind_check")
            .expect("kind-domain rule")
            .predicate;
        for kind in TerminalizationKind::ALL {
            assert!(
                definition.contains(kind.as_str()),
                "constraint omits {}",
                kind.as_str()
            );
        }
        assert!(!definition.contains("NOT_A_KIND"));
    }

    #[tokio::test]
    #[serial]
    async fn decoder_rejects_wrong_row_shape() {
        let pool = migrated_pool().await;
        let row = sqlx::query(WRONG_ROW_SHAPE.sql())
            .fetch_one(&pool)
            .await
            .expect("query");
        let err = decode_outcome_pg_row(&row).expect_err("a sqlx row of the wrong shape");
        assert!(err.to_string().contains("row shape"), "{err}");

        let core = crate::broker::pools::core_pool_for_test_pool(&pool);
        let rows = core
            .query(
                CheckoutClass::Reserved,
                &WRONG_ROW_SHAPE.bind().finish(),
                StatementBound::ConnectionTimeouts,
            )
            .await
            .expect("query on the core pool");
        core.close();
        let [row] = rows.as_slice() else {
            panic!("expected one row, got {}", rows.len());
        };
        let err = decode_outcome_row(row).expect_err("a core row of the wrong shape");
        assert!(err.to_string().contains("row shape"), "{err}");
    }
}

#[cfg(test)]
mod statement_tests {
    use super::{cardinality_of, statement_of, Cardinality};
    use crate::core::lifecycle::function_name_of;
    use crate::core::lifecycle::tests::sample_commands;

    /// Each command runs the function its vocabulary names, called as
    /// `SELECT * FROM <function>(`, so the composite outcome arrives as ten
    /// columns of built-in types.
    #[test]
    fn each_command_calls_its_function_as_a_row_source() {
        for (variant, command) in sample_commands() {
            let sql = statement_of(&command).sql();
            let call = format!("SELECT * FROM {}(", function_name_of(&command));
            assert!(sql.starts_with(&call), "{variant}: {sql}");
        }
    }

    /// The id-keyed batches expect one row per fenced task.
    #[test]
    fn an_id_keyed_batch_expects_one_row_per_task() {
        for (variant, command) in sample_commands() {
            match (variant, cardinality_of(&command)) {
                (
                    "AbandonOwnedNodes" | "CancelOwnedNodes",
                    Cardinality::IdKeyedBatch { expected },
                ) => {
                    assert_eq!(expected, 1, "{variant}");
                }
                ("AbandonOwnedNodes" | "CancelOwnedNodes", _) => {
                    panic!("{variant} is an id-keyed batch")
                }
                (_, Cardinality::ExactlyOne | Cardinality::PerTransition) => {}
                (_, Cardinality::IdKeyedBatch { .. }) => {
                    panic!("{variant} is not an id-keyed batch")
                }
            }
        }
    }
}

#[cfg(test)]
mod anchor_tests {
    //! The count pin: eleven terminalization commands and four writers of
    //! their own write the fifteen kinds, one kind each, and the catalog
    //! holds fourteen terminalization functions.
    //! `catalog_tests` ties EXPECTED_SIGNATURES to the installed database
    //! program; the vocabulary unit tests tie `function_name_of` to the
    //! variant names; this anchor ties the two ends together so a command,
    //! function, or kind cannot be added or removed alone.
    use super::catalog_tests::EXPECTED_SIGNATURES;
    use crate::core::lifecycle::tests::sample_commands;
    use crate::core::lifecycle::{kind_of, TerminalizationKind};

    const COMMAND_FUNCTIONS: [&str; 11] = [
        "horsies_complete_task_fused",
        "horsies_expire_owned_claim",
        "horsies_expire_pending_tasks",
        "horsies_cancel_owned_orphan",
        "horsies_cancel_orphaned_tasks",
        "horsies_abandon_owned_node",
        "horsies_abandon_owned_nodes",
        "horsies_abandon_nodes_of_paused_workflows",
        "horsies_cancel_owned_node",
        "horsies_cancel_owned_nodes",
        "horsies_cancel_nodes_of_cancelled_workflow",
    ];

    /// The writers of the kinds that no command writes, each with an entry
    /// point of its own (`broker::attempt_writes`, `broker::stale_recovery`,
    /// `broker::lifecycle_operations`), and the kind that each hardcodes.
    const OWN_WRITERS: [(&str, TerminalizationKind); 4] = [
        (
            "horsies_finish_workflow_success",
            TerminalizationKind::CompleteLocked,
        ),
        (
            "horsies_finish_failed_attempt",
            TerminalizationKind::FailRunning,
        ),
        (
            "horsies_finish_stale_attempt",
            TerminalizationKind::FailStale,
        ),
        ("horsies_cancel_task", TerminalizationKind::CancelAdmin),
    ];

    #[test]
    fn commands_functions_and_kinds_agree() {
        assert_eq!(
            COMMAND_FUNCTIONS.len() + OWN_WRITERS.len(),
            TerminalizationKind::ALL.len(),
            "every kind is written by exactly one function"
        );
        let mut kinds: Vec<&str> = sample_commands()
            .iter()
            .map(|(_, command)| kind_of(command).as_str())
            .chain(OWN_WRITERS.iter().map(|(_, kind)| kind.as_str()))
            .collect();
        kinds.sort_unstable();
        kinds.dedup();
        assert_eq!(
            kinds.len(),
            TerminalizationKind::ALL.len(),
            "the commands and the writers of their own write every kind once"
        );

        let mut expected: Vec<&str> = EXPECTED_SIGNATURES
            .iter()
            .map(|(name, _, _, _)| *name)
            .collect();
        assert_eq!(
            expected.len(),
            COMMAND_FUNCTIONS.len() + 3,
            "wire operations + miss classifier + attempt encoder + shared move"
        );
        assert!(expected.contains(&"horsies_terminalization_miss"));
        assert!(expected.contains(&"horsies_encode_task_attempts"));
        assert!(expected.contains(&"horsies_move_task_to_history"));
        expected.retain(|name| {
            !matches!(
                *name,
                "horsies_terminalization_miss"
                    | "horsies_encode_task_attempts"
                    | "horsies_move_task_to_history"
            )
        });
        expected.sort_unstable();

        let mut commands: Vec<&str> = COMMAND_FUNCTIONS.to_vec();
        commands.sort_unstable();
        assert_eq!(
            expected, commands,
            "catalog signatures name exactly the command functions"
        );
    }
}
