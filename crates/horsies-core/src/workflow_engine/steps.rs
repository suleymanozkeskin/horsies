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
//! - A step that owes child starts runs in a transaction: the step, then
//!   `horsies_workflow_child_start` for each owed child start
//!   (`child_start.rs`), then `COMMIT`. The parent node, the child rows and
//!   the link commit together.
//! - The engine awaits a step, a run of child starts, a parent advance, a
//!   recovery pass or a recovery case through `Box::pin`. An `async fn`
//!   holds each future that it awaits inline in its own state, so the
//!   futures of nested steps add up. In a debug build, the poll function of
//!   each level also reserves stack for each future that it builds, and the
//!   deepest workflow paths needed close to the 2 MiB stack of a tokio
//!   worker or test thread. On the heap, a level holds a pointer to the
//!   future of the level below. `port_tests/stack_margin.rs` runs the
//!   deepest paths on a thread with a 1.5 MiB stack in a debug build.

use std::collections::HashMap;

use uuid::Uuid;

use crate::broker::core_rows::{column, one_row, query_in, query_on, roll_back, RowDecodeError};
use crate::broker::{BrokerError, PostgresBroker};
use crate::core::config::retention::RetentionConfig;
use crate::core::registry::workflow::WorkflowSpecRegistry;
use crate::core::workflow::node::SubWorkflowParams;
use crate::db::{
    BoundStatement, CheckoutClass, CorePool, IsolationLevel, NotSentCause, ParameterType,
    PooledConnection, Row, StatementFailure, TypedStatement,
};
use crate::workflow_engine::child_spec::ChildSpecSource;
use crate::workflow_engine::child_start::{run_child_starts, ChildStartsFailure};
use crate::workflow_engine::error::WorkflowError;
use crate::workflow_engine::start::MAX_SUBWORKFLOW_DEPTH;

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

/// The columns that the core reads from a row of `horsies_workflow_step_row`:
/// the `OUTCOME` columns, the `CHILD_START` columns that the host build of a
/// child reads (`workflow_name`: the name of the parent workflow), and the
/// `SOURCE` columns that its `args_from` members take.
macro_rules! step_row_select {
    ($call:literal) => {
        concat!(
            "SELECT row_kind, disposition, workflow_id, workflow_name, ",
            "advance_child_workflow_id, detail, task_index, node_input, args_from, ",
            "sub_workflow_name, sub_definition_key, sub_workflow_params, parent_depth, ",
            "source_task_index, source_node_id, source_status, source_task_name, ",
            "source_definition_key, source_result FROM ",
            $call
        )
    };
}
pub(crate) use step_row_select;

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

/// `horsies_workflow_phase2_recover(task_id, attempt_bound)`: phase-2
/// recovery of one record, in the caller's transaction. A retaining
/// disposition adds one attempt to the record and, at the bound, quarantines
/// its evidence (`detail`: the verdict).
pub(crate) static PHASE2_RECOVER: TypedStatement = TypedStatement::new(
    "horsies_workflow_phase2_recover",
    step_row_select!("horsies_workflow_phase2_recover($1, $2)"),
    &[ParameterType::Uuid, ParameterType::Int4],
);

/// `horsies_workflow_resume(workflow_id)`: resumes a PAUSED workflow tree, in
/// the caller's transaction.
pub(crate) static RESUME: TypedStatement = TypedStatement::new(
    "horsies_workflow_resume",
    step_row_select!("horsies_workflow_resume($1)"),
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
    /// A drive of parent advances did not end within `bound` levels
    /// ([`MAX_PARENT_ADVANCES`]). The levels it ran committed; workflow
    /// recovery advances the rest.
    #[error("a drive of parent advances did not end within {bound} levels")]
    ParentChainTooLong { bound: usize },
    /// A step owed more than `bound` child starts, counting those that its
    /// child starts owed in turn (`MAX_WORKFLOW_NODES`, the most nodes of one
    /// workflow spec). The step's transaction rolled back.
    #[error("a step owed more than {bound} child starts; its transaction rolled back")]
    TooManyChildStarts { bound: usize },
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
/// the stored result did not pass its check. Public through
/// `WorkflowTaskCompletionError::RecordKept`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SourceFailure {
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

/// The disposition of `horsies_workflow_resume`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ResumeDisposition {
    /// The workflow and its PAUSED descendants are RUNNING.
    Resumed,
    /// The workflow exists and is not PAUSED (`detail`: its status); nothing
    /// written.
    NotPaused,
    /// No workflow has this id; nothing written.
    NotFound,
}

impl StepDisposition for ResumeDisposition {
    fn from_text(text: &str) -> Option<Self> {
        match text {
            "RESUMED" => Some(Self::Resumed),
            "NOT_PAUSED" => Some(Self::NotPaused),
            "NOT_FOUND" => Some(Self::NotFound),
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

/// A terminal dependency of an owed child start (a `SOURCE` row): the stored
/// result that an `args_from` member naming it takes, with the names that
/// the member's wrap carries (`source_task_name`, `source_definition_key`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChildStartSource {
    /// The index of the dependency node.
    pub task_index: i32,
    /// The node id of the dependency node; `None` for a node stored without
    /// one. The missing-result entry of a member names the node by it
    /// ([`crate::workflow_engine::args_merge::missing_result_error`]).
    pub node_id: Option<String>,
    /// Its status: `COMPLETED`, `FAILED` or `SKIPPED`.
    pub status: String,
    /// The task name of the dependency node.
    pub task_name: String,
    /// The definition key of a sub-workflow dependency; `None` for a task
    /// node.
    pub definition_key: Option<String>,
    /// Its stored result text, copied; `None` when the node stores none.
    pub result: Option<String>,
}

/// A sub-workflow node that a step promoted and whose child start is owed in
/// the step's transaction: a `CHILD_START` row and its `SOURCE` rows. The
/// node is ENQUEUED with no child.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct OwedChildStart {
    /// The parent workflow.
    pub(crate) workflow_id: Uuid,
    /// The name of the parent workflow. The node errors of a child that
    /// cannot start name the workflow by it (Python's `parent_workflow_name`,
    /// `core/workflows/engine.py:1014-1016`).
    pub(crate) workflow_name: String,
    /// The sub-workflow node.
    pub(crate) task_index: i32,
    /// The node's stored input, as the client encoded it.
    pub(crate) node_input: String,
    /// The node's `args_from` as JSON text (member name to dependency
    /// index); `None` when it has none.
    pub(crate) args_from: Option<String>,
    /// The name of the child workflow; `None` when the row stores none.
    pub(crate) sub_workflow_name: Option<String>,
    /// The definition key that resolves the child; `None` when the row
    /// stores none (the load then fails the node).
    pub(crate) sub_definition_key: Option<String>,
    /// The params choice that the client set on the node.
    pub(crate) sub_workflow_params: SubWorkflowParams,
    /// The depth of the parent workflow.
    pub(crate) parent_depth: i32,
    /// The terminal dependencies of the node.
    pub(crate) sources: Vec<ChildStartSource>,
}

/// The rows of one step call.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StepRows<D> {
    pub(crate) outcome: StepOutcome<D>,
    pub(crate) owed_child_starts: Vec<OwedChildStart>,
}

/// The child workflows that committed steps ended and whose parent nodes
/// advance after the commit ([`advance_parents`]), each named once, in the
/// order the rows named them. Empty when none is owed.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct OwedParentAdvances {
    child_workflow_ids: Vec<Uuid>,
}

impl OwedParentAdvances {
    /// The advance that one `OUTCOME` row names.
    pub(crate) fn of(advance: ParentAdvance) -> Self {
        let mut owed = Self::default();
        owed.add(advance);
        owed
    }

    /// Adds the advance that an `OUTCOME` row names, unless it is named
    /// already.
    pub(crate) fn add(&mut self, advance: ParentAdvance) {
        match advance {
            ParentAdvance::NotOwed => {}
            ParentAdvance::Owed { child_workflow_id } => {
                if !self.child_workflow_ids.contains(&child_workflow_id) {
                    self.child_workflow_ids.push(child_workflow_id);
                }
            }
        }
    }

    /// Adds every advance of `other` that is not named already.
    pub(crate) fn extend(&mut self, other: OwedParentAdvances) {
        for child_workflow_id in other.child_workflow_ids {
            self.add(ParentAdvance::Owed { child_workflow_id });
        }
    }

    /// The owed advances, in order.
    pub(crate) fn child_workflow_ids(&self) -> &[Uuid] {
        &self.child_workflow_ids
    }

    /// Whether no advance is owed.
    pub(crate) fn is_empty(&self) -> bool {
        self.child_workflow_ids.is_empty()
    }
}

/// A step that committed in its transaction, with every child start that it
/// owed (and that those owed in turn).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CommittedStep<D> {
    /// The disposition of the step's `OUTCOME` row.
    pub(crate) disposition: D,
    /// The detail of the step's `OUTCOME` row.
    pub(crate) detail: Option<String>,
    /// The advances that the step and its child starts owe after the commit.
    pub(crate) parent_advances: OwedParentAdvances,
}

/// Where the host build of a child gets the child's spec
/// (`child_spec.rs`), and where the typed facts of the child's task nodes
/// get the retention class of their queue.
#[derive(Clone, Copy)]
pub(crate) enum ChildBuild<'a> {
    /// The registered definitions of the Rust client, and the retention
    /// config of the process.
    Registry {
        registry: &'a WorkflowSpecRegistry,
        retention: &'a RetentionConfig,
    },
    /// A client that builds the child in its own language and sends the
    /// spec as data, with the class of each task queue.
    Client(&'a dyn ChildSpecSource),
}

/// A parent advance failed at the level of `failed_child_workflow_id`. The
/// levels before it committed; that level wrote nothing, or its outcome is
/// unknown. `owed_after` are the advances still owed after it.
#[derive(Debug)]
pub(crate) struct ParentAdvanceFailure {
    pub(crate) failed_child_workflow_id: Uuid,
    pub(crate) owed_after: OwedParentAdvances,
    pub(crate) error: WorkflowError,
}

impl ParentAdvanceFailure {
    /// Every advance still owed, the failed level first: what a retry runs.
    pub(crate) fn still_owed(&self) -> OwedParentAdvances {
        let mut owed = OwedParentAdvances::of(ParentAdvance::Owed {
            child_workflow_id: self.failed_child_workflow_id,
        });
        owed.extend(self.owed_after.clone());
        owed
    }
}

/// The most parent advance levels that one drive of [`advance_parents`]
/// runs: the levels of a tree, depth 0 to [`MAX_SUBWORKFLOW_DEPTH`]. A drive
/// that reaches the bound with an advance still owed fails closed
/// ([`StepContractError::ParentChainTooLong`]) and leaves the rest to
/// workflow recovery (case 1.6: a terminal child under a live parent node).
pub(crate) const MAX_PARENT_ADVANCES: usize = MAX_SUBWORKFLOW_DEPTH as usize + 1;

/// The parent advances of one drive of [`advance_parents`]: a stack whose
/// next advance is the last element, and the count of levels run.
#[derive(Debug)]
struct ParentAdvanceDrive {
    pending: Vec<Uuid>,
    levels: usize,
}

impl ParentAdvanceDrive {
    fn new(owed: &OwedParentAdvances) -> Self {
        Self {
            pending: owed.child_workflow_ids().iter().rev().copied().collect(),
            levels: 0,
        }
    }

    /// The next advance to run; `None` when none is owed. With
    /// [`MAX_PARENT_ADVANCES`] levels run and one more owed:
    /// [`StepContractError::ParentChainTooLong`] at that advance.
    fn next(&mut self) -> Result<Option<Uuid>, ParentAdvanceFailure> {
        let Some(child_workflow_id) = self.pending.pop() else {
            return Ok(None);
        };
        match self.levels < MAX_PARENT_ADVANCES {
            true => {
                self.levels += 1;
                Ok(Some(child_workflow_id))
            }
            false => Err(self.failure(
                child_workflow_id,
                StepContractError::ParentChainTooLong {
                    bound: MAX_PARENT_ADVANCES,
                }
                .into(),
            )),
        }
    }

    /// Adds the advances that a level owes; they run before the rest.
    fn push(&mut self, owed: &OwedParentAdvances) {
        self.pending
            .extend(owed.child_workflow_ids().iter().rev().copied());
    }

    /// The failure of the level at `failed_child_workflow_id`, with the
    /// advances still owed after it.
    fn failure(
        &mut self,
        failed_child_workflow_id: Uuid,
        error: WorkflowError,
    ) -> ParentAdvanceFailure {
        ParentAdvanceFailure {
            failed_child_workflow_id,
            owed_after: owed_from_stack(std::mem::take(&mut self.pending)),
            error,
        }
    }
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

/// Resumes a workflow tree in a transaction with its owed child starts, as
/// producer work ([`CheckoutClass::Batch`]). Errors as
/// [`run_in_transaction`].
pub(crate) async fn resume(
    broker: &PostgresBroker,
    workflow_id: Uuid,
    build: ChildBuild<'_>,
) -> Result<CommittedStep<ResumeDisposition>, WorkflowError> {
    Box::pin(run_in_transaction(
        broker,
        CheckoutClass::Batch,
        &RESUME.bind().value(&workflow_id).finish(),
        build,
    ))
    .await
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

/// Phase 2 of a finished workflow task in a transaction, on the worker
/// reserve: `BEGIN`, the step with the dependents read, one
/// `horsies_workflow_child_start` per owed child start, `COMMIT`. The form
/// for a node from which a sub-workflow node is reachable, and for a phase 2
/// whose phase-1 outcome is not at hand.
///
/// Errors as [`run_in_transaction`].
pub(crate) async fn node_finish_in_transaction(
    broker: &PostgresBroker,
    task_id: Uuid,
    wake: bool,
    build: ChildBuild<'_>,
) -> Result<CommittedStep<NodeFinishDisposition>, WorkflowError> {
    let dependents_text = Dependents::Read.as_sql();
    Box::pin(run_in_transaction(
        broker,
        CheckoutClass::Reserved,
        &NODE_FINISH
            .bind()
            .value(&task_id)
            .value(&wake)
            .value(&dependents_text)
            .finish(),
        build,
    ))
    .await
}

/// Phase-2 recovery of one record in a transaction, on the reserve
/// ([`CheckoutClass::Reserved`], the reaper's class): `BEGIN`,
/// `horsies_workflow_phase2_recover` (the body of the node finish with no
/// wake and the dependents read), one `horsies_workflow_child_start` per
/// owed child start, `COMMIT`. `attempt_bound` is the quarantine bound of a
/// retaining record (3 to 1000; the function refuses others with 22023).
///
/// Errors as [`run_in_transaction`].
pub(crate) async fn phase2_recover(
    broker: &PostgresBroker,
    task_id: Uuid,
    attempt_bound: i32,
    build: ChildBuild<'_>,
) -> Result<CommittedStep<NodeFinishDisposition>, WorkflowError> {
    Box::pin(run_in_transaction(
        broker,
        CheckoutClass::Reserved,
        &PHASE2_RECOVER
            .bind()
            .value(&task_id)
            .value(&attempt_bound)
            .finish(),
        build,
    ))
    .await
}

/// Advances the parent nodes above child workflows that committed steps
/// ended: `horsies_workflow_child_finish`, one transaction per level (with
/// the child starts that the level owes), while the levels name further
/// ended children. Python: `_drive_parent_propagations`
/// (`core/worker/finalize.py:2248-2290`).
///
/// The owed advances run in order; the advances that a level owes run
/// before the next advance of the list. At most [`MAX_PARENT_ADVANCES`]
/// levels run ([`ParentAdvanceDrive`]).
///
/// Errors: [`ParentAdvanceFailure`] names the level that failed and the
/// advances still owed after it; the levels before it committed. A drive
/// that reaches the bound is [`StepContractError::ParentChainTooLong`] at
/// the next level that did not run.
pub(crate) async fn advance_parents(
    broker: &PostgresBroker,
    class: CheckoutClass,
    owed: OwedParentAdvances,
    build: ChildBuild<'_>,
) -> Result<(), ParentAdvanceFailure> {
    let mut drive = ParentAdvanceDrive::new(&owed);
    while let Some(child_workflow_id) = drive.next()? {
        match Box::pin(advance_parent(broker, class, child_workflow_id, build)).await {
            Ok(next) => drive.push(&next),
            Err(error) => return Err(drive.failure(child_workflow_id, error)),
        }
    }
    Ok(())
}

/// The advances of a stack whose next element is the last, in run order.
fn owed_from_stack(stack: Vec<Uuid>) -> OwedParentAdvances {
    let mut owed = OwedParentAdvances::default();
    for child_workflow_id in stack.into_iter().rev() {
        owed.add(ParentAdvance::Owed { child_workflow_id });
    }
    owed
}

/// One level of [`advance_parents`]. Returns the advances that the level
/// owes in turn.
async fn advance_parent(
    broker: &PostgresBroker,
    class: CheckoutClass,
    child_workflow_id: Uuid,
    build: ChildBuild<'_>,
) -> Result<OwedParentAdvances, WorkflowError> {
    let committed = Box::pin(run_in_transaction::<ChildFinishDisposition>(
        broker,
        class,
        &CHILD_FINISH.bind().value(&child_workflow_id).finish(),
        build,
    ))
    .await?;
    match committed.disposition {
        ChildFinishDisposition::Applied => {}
        ChildFinishDisposition::AlreadyApplied
        | ChildFinishDisposition::ChildAbsent
        | ChildFinishDisposition::NotAChild
        | ChildFinishDisposition::ChildNotTerminal
        | ChildFinishDisposition::ParentAbsent => tracing::debug!(
            child_workflow_id = %child_workflow_id,
            disposition = ?committed.disposition,
            detail = ?committed.detail,
            "parent node not advanced; nothing written",
        ),
    }
    Ok(committed.parent_advances)
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
/// `BEGIN`, the step, `horsies_workflow_child_start` for each child start
/// that the step owes (and for each that those owe in turn), then `COMMIT`.
/// The write token of the broker handle is held until the transaction ends.
///
/// Errors (nothing committed, except after a `COMMIT` with a lost reply):
/// - [`WorkflowError::Broker`] for a failed checkout, `BEGIN`, step, child
///   start, `COMMIT` or `ROLLBACK`. After a failed statement the connection
///   is discarded; a `COMMIT` with a lost reply is `OutcomeUnknown`.
/// - [`WorkflowError::StepContract`] for a row outside the contract of the
///   step or of a child start; the transaction rolls back.
/// - The refusal of the host build of a child (a value that does not
///   encode, a stored input that the `args_from` merge refuses, a child spec
///   that the tree arguments refuse); the transaction rolls back.
pub(crate) async fn run_in_transaction<D: StepDisposition>(
    broker: &PostgresBroker,
    class: CheckoutClass,
    statement: &BoundStatement<'_>,
    build: ChildBuild<'_>,
) -> Result<CommittedStep<D>, WorkflowError> {
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
    let mut parent_advances = OwedParentAdvances::of(step.outcome.parent_advance);
    let child_starts = Box::pin(run_child_starts(
        &mut transaction,
        broker.statement_log(),
        step.owed_child_starts,
        build,
    ))
    .await;
    match child_starts {
        Ok(advances) => parent_advances.extend(advances),
        // The transaction holds a failed statement: dropping it discards the
        // connection, and nothing commits.
        Err(ChildStartsFailure::Statement(failure)) => {
            return Err(BrokerError::from(failure).into())
        }
        Err(ChildStartsFailure::Refused(error)) => {
            roll_back(transaction).await?;
            return Err(error);
        }
    }
    transaction.commit().await.map_err(BrokerError::from)?;
    Ok(CommittedStep {
        disposition: step.outcome.disposition,
        detail: step.outcome.detail,
        parent_advances,
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

/// Decodes the rows of a step function (`SETOF horsies_workflow_step_row`):
/// exactly one `OUTCOME` row, any number of `CHILD_START` rows, and the
/// `SOURCE` rows of the owed child starts, each attached to the
/// `CHILD_START` row of its node. Row order does not matter. Pure.
///
/// Errors: [`StepContractError`] for an unknown row kind, a count of
/// `OUTCOME` rows other than one, a column that does not decode, a params
/// choice other than `GIVEN` and `NOT_GIVEN`, and a `SOURCE` row whose node
/// has no `CHILD_START` row.
pub(crate) fn step_rows<D: StepDisposition>(
    rows: &[Row],
    statement: &TypedStatement,
) -> Result<StepRows<D>, StepContractError> {
    let mut outcomes: Vec<StepOutcome<D>> = Vec::with_capacity(1);
    let mut owed_child_starts: Vec<OwedChildStart> = Vec::new();
    let mut sources: Vec<(Uuid, i32, ChildStartSource)> = Vec::new();
    for row in rows {
        let row_kind: String = column(row, statement, "row_kind")?;
        match row_kind.as_str() {
            "OUTCOME" => outcomes.push(outcome_row(row, statement)?),
            "CHILD_START" => owed_child_starts.push(child_start_row(row, statement)?),
            "SOURCE" => sources.push((
                column(row, statement, "workflow_id")?,
                column(row, statement, "task_index")?,
                ChildStartSource {
                    task_index: column(row, statement, "source_task_index")?,
                    node_id: column(row, statement, "source_node_id")?,
                    status: column(row, statement, "source_status")?,
                    task_name: column(row, statement, "source_task_name")?,
                    definition_key: column(row, statement, "source_definition_key")?,
                    result: column(row, statement, "source_result")?,
                },
            )),
            _ => {
                return Err(StepContractError::UnknownRowKind {
                    function: statement.name(),
                    row_kind,
                })
            }
        }
    }
    attach_sources(&mut owed_child_starts, sources, statement)?;
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

/// Decodes one `CHILD_START` row, with no sources yet. A NULL
/// `workflow_name` does not decode ([`RowDecodeError`]). Pure.
fn child_start_row(
    row: &Row,
    statement: &TypedStatement,
) -> Result<OwedChildStart, StepContractError> {
    let params_text: String = column(row, statement, "sub_workflow_params")?;
    let Some(sub_workflow_params) = SubWorkflowParams::from_stored(&params_text) else {
        return Err(RowDecodeError::Shape {
            statement: statement.name(),
            reason: format!(
                "a CHILD_START row has sub_workflow_params {params_text:?}; \
                 the contract is GIVEN or NOT_GIVEN"
            ),
        }
        .into());
    };
    Ok(OwedChildStart {
        workflow_id: column(row, statement, "workflow_id")?,
        workflow_name: column(row, statement, "workflow_name")?,
        task_index: column(row, statement, "task_index")?,
        node_input: column(row, statement, "node_input")?,
        args_from: column(row, statement, "args_from")?,
        sub_workflow_name: column(row, statement, "sub_workflow_name")?,
        sub_definition_key: column(row, statement, "sub_definition_key")?,
        sub_workflow_params,
        parent_depth: column(row, statement, "parent_depth")?,
        sources: Vec::new(),
    })
}

/// Attaches each `SOURCE` row to the owed child start of its node
/// (`workflow_id`, `task_index`). Pure.
fn attach_sources(
    owed_child_starts: &mut [OwedChildStart],
    sources: Vec<(Uuid, i32, ChildStartSource)>,
    statement: &TypedStatement,
) -> Result<(), StepContractError> {
    let positions: HashMap<(Uuid, i32), usize> = owed_child_starts
        .iter()
        .enumerate()
        .map(|(position, owed)| ((owed.workflow_id, owed.task_index), position))
        .collect();
    for (workflow_id, task_index, source) in sources {
        let Some(&position) = positions.get(&(workflow_id, task_index)) else {
            return Err(RowDecodeError::Shape {
                statement: statement.name(),
                reason: format!(
                    "a SOURCE row names node {task_index} of workflow {workflow_id}, \
                     which has no CHILD_START row"
                ),
            }
            .into());
        };
        owed_child_starts[position].sources.push(source);
    }
    Ok(())
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
