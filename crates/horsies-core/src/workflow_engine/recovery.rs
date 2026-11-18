//! Workflow recovery: the reaper's global pass and the tree pass of a resume.
//!
//! Each candidate is one step on the core statement layer (Python
//! `core/workflows/recovery.py`, one `SAVEPOINT` per candidate):
//!
//! - case 0: `horsies_workflow_node_reevaluate`, in a transaction with its owed child starts;
//! - cases 1 and 1.5: `horsies_workflow_nodes_enqueue_ready`, in a transaction with its owed child starts;
//! - case 1.6: `horsies_workflow_child_finish`, in a transaction with its owed child starts;
//! - cases 2/3 and 4 of the global pass: the audit page (`horsies_workflow_recovery_audit_page`), then
//!   `horsies_workflow_recover_completion` or `horsies_workflow_fail_orphaned` per candidate, each one statement
//!   in autocommit that renews the page claim with its work;
//! - cases 2/3 and 4 of the tree pass: the completion check under the workflow row lock, and the orphan write.
//!
//! A candidate commits or rolls back as one transaction. The parent advance of a child workflow that a
//! candidate ended is its own step after the commit ([`advance_parents`]); a failure there is logged, the child
//! stays terminal, and the next pass selects the pair as case 1.6, which counts a persistent failure on the
//! parent workflow (Python `_propagation_candidate`).

use std::time::Instant;

use uuid::Uuid;

use crate::broker::core_rows::{column, first_row, one_row, query_in, roll_back, RowDecodeError};
use crate::broker::{BrokerError, PostgresBroker};
use crate::core::{OperationalErrorCode, TaskError};
use crate::db::{CheckoutClass, IsolationLevel, ParameterType, Row, TypedStatement};
use crate::workflow_engine::child_spec::ChildSpecSource;

use crate::workflow_engine::error::WorkflowError;
use crate::workflow_engine::recovery_limit::{
    count_over_failure_limit, run_catching_panic, run_recovery_candidate, CandidateOutcome,
    CountFailures, FailureRecord, RecoveryCase, RecoveryFailurePolicy,
};
use crate::workflow_engine::steps::{
    advance_parents, checkout, run_alone, run_in_transaction, step_row_select,
    ChildFinishDisposition, OwedParentAdvances, ParentAdvance, StepContractError, StepDisposition,
    CHILD_FINISH,
};
// ---------------------------------------------------------------------------
// Recovery report
// ---------------------------------------------------------------------------

/// Tracks counts of recovered items by case.
#[derive(Debug, Default, serde::Serialize)]
pub struct RecoveryReport {
    /// Case 0: PENDING wf_tasks with all deps terminal.
    pub case0_pending_reevaluated: u32,
    /// Case 1: READY wf_tasks with no task_id.
    pub case1_ready_enqueued: u32,
    /// Case 1.5: READY sub-workflow wf_tasks with no child started.
    pub case1_5_subworkflow_started: u32,
    /// Case 1.6: Non-terminal sub-workflow wf_tasks with terminal child.
    pub case1_6_subworkflow_completed: u32,
    /// Case 2+3: RUNNING workflows with all terminal wf_tasks.
    pub case2_3_workflow_completed: u32,
    /// Case 4: Orphaned RUNNING workflows with zero workflow_tasks.
    pub case4_orphaned_failed: u32,
    /// Non-fatal errors encountered.
    pub errors: u32,
    /// Query and processing metrics for each recovery case.
    pub metrics: RecoveryMetrics,
    /// Completion candidates divided by workflows scanned in the global audit.
    /// Null when no workflows were scanned or no global audit result is available.
    /// Candidates count before processing; this is not the successful completion rate.
    pub completion_candidate_yield: Option<f64>,
    /// RUNNING workflows whose stored recovery failure count is at or over the limit. Counted once per
    /// global pass; 0 in a pass that does not count failures.
    pub over_failure_limit: i64,
    /// The failures of the pass that no case counted (the count of workflows over the limit). Every
    /// other failure is in the errors of its case or cases; `errors` counts each failure once.
    #[serde(skip)]
    pub pass_errors: u32,
}

#[derive(Debug, Default, serde::Serialize)]
pub struct RecoveryMetrics {
    pub case0: RecoveryCaseMetrics,
    pub case1: RecoveryCaseMetrics,
    pub case1_5: RecoveryCaseMetrics,
    pub case1_6: RecoveryCaseMetrics,
    pub case2_3: RecoveryCaseMetrics,
    pub case4: RecoveryCaseMetrics,
}

#[derive(Debug, Default, serde::Serialize)]
pub struct RecoveryCaseMetrics {
    pub rows_selected: u32,
    pub candidates_returned: u32,
    pub duration_ms: u64,
    pub refusals: u32,
    pub errors: u32,
    /// Candidate failures that moved the stored count of their workflow.
    pub failures_counted: u32,
    /// Candidate failures whose count reached the limit and stopped the workflow.
    pub limit_reached: u32,
}

impl RecoveryCaseMetrics {
    /// Notes one failed candidate and what the failure bound did with it.
    fn note_failure(&mut self, record: FailureRecord) {
        self.errors += 1;
        match record {
            FailureRecord::NotCounted => {}
            FailureRecord::Counted => self.failures_counted += 1,
            FailureRecord::LimitReached => {
                self.failures_counted += 1;
                self.limit_reached += 1;
            }
        }
    }
}

impl RecoveryReport {
    /// Total recoveries performed (excluding errors).
    pub fn total(&self) -> u32 {
        self.case0_pending_reevaluated
            + self.case1_ready_enqueued
            + self.case1_5_subworkflow_started
            + self.case1_6_subworkflow_completed
            + self.case2_3_workflow_completed
            + self.case4_orphaned_failed
    }
}

/// A global pass that failed, with its partial report.
#[derive(Debug)]
pub struct RecoveryPassFailure {
    pub report: RecoveryReport,
    pub error: WorkflowError,
}

impl RecoveryPassFailure {
    pub(crate) fn into_health_snapshot(self) -> serde_json::Value {
        let error = self.error.to_string();
        let mut snapshot = serde_json::to_value(self.report)
            .unwrap_or_else(|serialization_error| {
                serde_json::json!({"serialization_error": serialization_error.to_string()})
            });
        if let Some(fields) = snapshot.as_object_mut() {
            fields.insert("state".to_owned(), serde_json::json!("error"));
            fields.insert("error".to_owned(), serde_json::json!(error));
        }
        snapshot
    }
}

// ---------------------------------------------------------------------------
// Discovery statements (Python's case queries, `core/workflows/recovery.py:207-311`)
// ---------------------------------------------------------------------------

/// Case 0: PENDING nodes of a RUNNING workflow whose dependencies are all terminal. `$1`: the row cap, NULL for
/// no cap.
const GLOBAL_CASE0_STUCK_PENDING_SQL: &str = "\
SELECT wt.workflow_id, wt.task_index
FROM horsies_workflow_tasks wt
JOIN horsies_workflows w ON w.id = wt.workflow_id
WHERE wt.status = 'PENDING'
  AND w.status = 'RUNNING'
  AND NOT EXISTS (
    SELECT 1 FROM horsies_workflow_tasks dep
    WHERE dep.workflow_id = wt.workflow_id
      AND wt.dependencies @> ARRAY[dep.task_index]
      AND dep.status IN ('PENDING', 'READY', 'ENQUEUED', 'RUNNING')
  )
LIMIT CAST($1 AS bigint)";

/// Case 0 within the workflows `$1`; `$2`: the row cap, NULL for no cap.
const TREE_CASE0_STUCK_PENDING_SQL: &str = "\
SELECT wt.workflow_id, wt.task_index
FROM horsies_workflow_tasks wt
JOIN horsies_workflows w ON w.id = wt.workflow_id
WHERE wt.status = 'PENDING'
  AND w.status = 'RUNNING'
  AND wt.workflow_id = ANY($1::uuid[])
  AND NOT EXISTS (
    SELECT 1 FROM horsies_workflow_tasks dep
    WHERE dep.workflow_id = wt.workflow_id
      AND wt.dependencies @> ARRAY[dep.task_index]
      AND dep.status IN ('PENDING', 'READY', 'ENQUEUED', 'RUNNING')
  )
LIMIT CAST($2 AS bigint)";

/// Case 1: READY task nodes with no task, in a RUNNING workflow.
const GLOBAL_CASE1_READY_NO_TASK_SQL: &str = "\
SELECT wt.workflow_id, wt.task_index
FROM horsies_workflow_tasks wt
JOIN horsies_workflows w ON w.id = wt.workflow_id
WHERE wt.status = 'READY'
  AND wt.task_id IS NULL
  AND wt.is_subworkflow = FALSE
  AND w.status = 'RUNNING'
LIMIT CAST($1 AS bigint)";

const TREE_CASE1_READY_NO_TASK_SQL: &str = "\
SELECT wt.workflow_id, wt.task_index
FROM horsies_workflow_tasks wt
JOIN horsies_workflows w ON w.id = wt.workflow_id
WHERE wt.status = 'READY'
  AND wt.task_id IS NULL
  AND wt.is_subworkflow = FALSE
  AND w.status = 'RUNNING'
  AND wt.workflow_id = ANY($1::uuid[])
LIMIT CAST($2 AS bigint)";

/// Case 1.5: READY sub-workflow nodes with no child, in a RUNNING workflow.
const GLOBAL_CASE1_5_READY_SUBWORKFLOW_SQL: &str = "\
SELECT wt.workflow_id, wt.task_index
FROM horsies_workflow_tasks wt
JOIN horsies_workflows w ON w.id = wt.workflow_id
WHERE wt.status = 'READY'
  AND wt.sub_workflow_id IS NULL
  AND wt.is_subworkflow = TRUE
  AND w.status = 'RUNNING'
LIMIT CAST($1 AS bigint)";

const TREE_CASE1_5_READY_SUBWORKFLOW_SQL: &str = "\
SELECT wt.workflow_id, wt.task_index
FROM horsies_workflow_tasks wt
JOIN horsies_workflows w ON w.id = wt.workflow_id
WHERE wt.status = 'READY'
  AND wt.sub_workflow_id IS NULL
  AND wt.is_subworkflow = TRUE
  AND w.status = 'RUNNING'
  AND wt.workflow_id = ANY($1::uuid[])
LIMIT CAST($2 AS bigint)";

/// Case 1.6: a RUNNING sub-workflow node whose child workflow is terminal, in a RUNNING parent. Every link
/// path writes the parent node RUNNING, so Python's selection covers every linked node
/// (`GET_COMPLETED_CHILDREN_NOT_UPDATED_SQL`, `core/workflows/recovery.py:266-276`).
const GLOBAL_CASE1_6_STALE_SUBWORKFLOW_SQL: &str = "\
SELECT wt.workflow_id, wt.task_index, wt.sub_workflow_id
FROM horsies_workflow_tasks wt
JOIN horsies_workflows w ON w.id = wt.workflow_id
JOIN horsies_workflows cw ON cw.id = wt.sub_workflow_id
WHERE wt.is_subworkflow = TRUE
  AND wt.sub_workflow_id IS NOT NULL
  AND wt.status = 'RUNNING'
  AND cw.status IN ('COMPLETED', 'FAILED', 'CANCELLED', 'EXPIRED')
  AND w.status = 'RUNNING'
LIMIT CAST($1 AS bigint)";

/// Case 1.6 for the children `$1` of a tree.
const TREE_CASE1_6_STALE_SUBWORKFLOW_SQL: &str = "\
SELECT wt.workflow_id, wt.task_index, wt.sub_workflow_id
FROM horsies_workflow_tasks wt
JOIN horsies_workflows w ON w.id = wt.workflow_id
JOIN horsies_workflows cw ON cw.id = wt.sub_workflow_id
WHERE wt.is_subworkflow = TRUE
  AND wt.sub_workflow_id IS NOT NULL
  AND wt.status = 'RUNNING'
  AND cw.status IN ('COMPLETED', 'FAILED', 'CANCELLED', 'EXPIRED')
  AND w.status = 'RUNNING'
  AND cw.id = ANY($1::uuid[])
LIMIT CAST($2 AS bigint)";

/// Cases 2 and 3 of the tree pass: RUNNING workflows of the tree with nodes, all terminal.
const TREE_CASE2_3_STUCK_WORKFLOW_SQL: &str = "\
SELECT w.id as workflow_id
FROM horsies_workflows w
WHERE w.status = 'RUNNING'
  AND w.id = ANY($1::uuid[])
  AND EXISTS (
    SELECT 1 FROM horsies_workflow_tasks wt
    WHERE wt.workflow_id = w.id
  )
  AND NOT EXISTS (
    SELECT 1 FROM horsies_workflow_tasks wt
    WHERE wt.workflow_id = w.id
      AND wt.status IN ('PENDING', 'READY', 'ENQUEUED', 'RUNNING')
  )
LIMIT CAST($2 AS bigint)";

/// Case 4 of the tree pass: RUNNING workflows of the tree with no node.
const TREE_CASE4_ORPHANED_WORKFLOW_SQL: &str = "\
SELECT w.id as workflow_id, w.name
FROM horsies_workflows w
WHERE w.status = 'RUNNING'
  AND w.id = ANY($1::uuid[])
  AND NOT EXISTS (
    SELECT 1 FROM horsies_workflow_tasks wt
    WHERE wt.workflow_id = w.id
  )
LIMIT CAST($2 AS bigint)";

/// One page of the audit of RUNNING workflows (cases 2/3 and 4 of the global pass):
/// `horsies_workflow_recovery_audit_page(max_rows, claim_token, claim_ttl_ms)`. `$1` (1 to 1000) and `$3` (1 to
/// 3,600,000) are cast to the function's integers.
const GLOBAL_WORKFLOW_AUDIT_SQL: &str = "\
SELECT outcome, scanned_count, completion_ids, orphan_ids, orphan_names, claim_token
FROM horsies_workflow_recovery_audit_page(CAST($1 AS integer), $2, CAST($3 AS integer))";

static CASE0_GLOBAL: TypedStatement = TypedStatement::new(
    "recovery_case0_global",
    GLOBAL_CASE0_STUCK_PENDING_SQL,
    &[ParameterType::Int8],
);
static CASE0_TREE: TypedStatement = TypedStatement::new(
    "recovery_case0_tree",
    TREE_CASE0_STUCK_PENDING_SQL,
    &[ParameterType::UuidArray, ParameterType::Int8],
);
static CASE1_GLOBAL: TypedStatement = TypedStatement::new(
    "recovery_case1_global",
    GLOBAL_CASE1_READY_NO_TASK_SQL,
    &[ParameterType::Int8],
);
static CASE1_TREE: TypedStatement = TypedStatement::new(
    "recovery_case1_tree",
    TREE_CASE1_READY_NO_TASK_SQL,
    &[ParameterType::UuidArray, ParameterType::Int8],
);
static CASE1_5_GLOBAL: TypedStatement = TypedStatement::new(
    "recovery_case1_5_global",
    GLOBAL_CASE1_5_READY_SUBWORKFLOW_SQL,
    &[ParameterType::Int8],
);
static CASE1_5_TREE: TypedStatement = TypedStatement::new(
    "recovery_case1_5_tree",
    TREE_CASE1_5_READY_SUBWORKFLOW_SQL,
    &[ParameterType::UuidArray, ParameterType::Int8],
);
static CASE1_6_GLOBAL: TypedStatement = TypedStatement::new(
    "recovery_case1_6_global",
    GLOBAL_CASE1_6_STALE_SUBWORKFLOW_SQL,
    &[ParameterType::Int8],
);
static CASE1_6_TREE: TypedStatement = TypedStatement::new(
    "recovery_case1_6_tree",
    TREE_CASE1_6_STALE_SUBWORKFLOW_SQL,
    &[ParameterType::UuidArray, ParameterType::Int8],
);
static CASE2_3_TREE: TypedStatement = TypedStatement::new(
    "recovery_case2_3_tree",
    TREE_CASE2_3_STUCK_WORKFLOW_SQL,
    &[ParameterType::UuidArray, ParameterType::Int8],
);
static CASE4_TREE: TypedStatement = TypedStatement::new(
    "recovery_case4_tree",
    TREE_CASE4_ORPHANED_WORKFLOW_SQL,
    &[ParameterType::UuidArray, ParameterType::Int8],
);
static AUDIT_PAGE: TypedStatement = TypedStatement::new(
    "horsies_workflow_recovery_audit_page",
    GLOBAL_WORKFLOW_AUDIT_SQL,
    &[
        ParameterType::Int8,
        ParameterType::Uuid,
        ParameterType::Int8,
    ],
);

/// The workflow and its descendants (Python `GET_WORKFLOW_TREE_IDS_SQL`).
static WORKFLOW_TREE_IDS: TypedStatement = TypedStatement::new(
    "recovery_workflow_tree_ids",
    "WITH RECURSIVE tree AS (
         SELECT id FROM horsies_workflows WHERE id = $1
         UNION ALL
         SELECT child.id
         FROM horsies_workflows child
         JOIN tree parent ON child.parent_workflow_id = parent.id
     )
     SELECT id FROM tree",
    &[ParameterType::Uuid],
);

/// Ends the page claim `$1` (Python `RELEASE_GLOBAL_WORKFLOW_AUDIT_CLAIM_SQL`).
static RELEASE_AUDIT_CLAIM: TypedStatement = TypedStatement::new(
    "recovery_release_audit_claim",
    "UPDATE horsies_recovery_scan_cursors
     SET claim_token = NULL, claim_expires_at = NULL
     WHERE scan_name = 'running_workflows' AND claim_token = $1",
    &[ParameterType::Uuid],
);

// ---------------------------------------------------------------------------
// Step statements
// ---------------------------------------------------------------------------

/// `horsies_workflow_node_reevaluate(workflow_id, task_index)`: recovery case 0, in the caller's transaction.
static NODE_REEVALUATE: TypedStatement = TypedStatement::new(
    "horsies_workflow_node_reevaluate",
    step_row_select!("horsies_workflow_node_reevaluate($1, $2)"),
    &[ParameterType::Uuid, ParameterType::Int4],
);

/// `horsies_workflow_nodes_enqueue_ready(workflow_id, task_indexes)`: recovery cases 1 and 1.5, in the caller's
/// transaction.
static NODES_ENQUEUE_READY: TypedStatement = TypedStatement::new(
    "horsies_workflow_nodes_enqueue_ready",
    step_row_select!("horsies_workflow_nodes_enqueue_ready($1, $2)"),
    &[ParameterType::Uuid, ParameterType::Int4Array],
);

/// `horsies_workflow_recover_completion(workflow_id, claim_token, claim_ttl_ms)`: cases 2/3 of one audit
/// candidate, one statement in autocommit.
static RECOVER_COMPLETION: TypedStatement = TypedStatement::new(
    "horsies_workflow_recover_completion",
    "SELECT outcome, workflow_status, advance_child_workflow_id
     FROM horsies_workflow_recover_completion($1, $2, CAST($3 AS integer))",
    &[
        ParameterType::Uuid,
        ParameterType::Uuid,
        ParameterType::Int8,
    ],
);

/// `horsies_workflow_fail_orphaned(workflow_id, claim_token, claim_ttl_ms, error)`: case 4 of one audit
/// candidate, one statement in autocommit.
static FAIL_ORPHANED: TypedStatement = TypedStatement::new(
    "horsies_workflow_fail_orphaned",
    "SELECT outcome, workflow_status, advance_child_workflow_id
     FROM horsies_workflow_fail_orphaned($1, $2, CAST($3 AS integer), $4)",
    &[
        ParameterType::Uuid,
        ParameterType::Uuid,
        ParameterType::Int8,
        ParameterType::Text,
    ],
);

/// The tree pass's lock of one completion candidate, before its completion check: the workflow row
/// `FOR UPDATE` (the precondition of `horsies_workflow_try_complete`), and its parent.
static LOCK_TREE_CANDIDATE: TypedStatement = TypedStatement::new(
    "recovery_lock_tree_candidate",
    "SELECT w.parent_workflow_id FROM horsies_workflows w WHERE w.id = $1 FOR UPDATE",
    &[ParameterType::Uuid],
);

/// The completion check of a RUNNING workflow with no live node, under the caller's row lock
/// (`horsies_workflow_try_complete`): the status it wrote, or NULL.
static TRY_COMPLETE: TypedStatement = TypedStatement::new(
    "horsies_workflow_try_complete",
    "SELECT horsies_workflow_try_complete($1) AS written",
    &[ParameterType::Uuid],
);

/// Case 4 of the tree pass: a RUNNING workflow with no node goes FAILED with `$2`, while it is RUNNING and has
/// no node (Python `FAIL_ORPHANED_WORKFLOW_SQL` with the node check of its candidate query).
static FAIL_TREE_ORPHAN: TypedStatement = TypedStatement::new(
    "recovery_fail_tree_orphan",
    "UPDATE horsies_workflows w
     SET status = 'FAILED', error = $2, completed_at = NOW(), updated_at = NOW()
     WHERE w.id = $1
       AND w.status = 'RUNNING'
       AND NOT EXISTS (SELECT 1 FROM horsies_workflow_tasks n WHERE n.workflow_id = w.id)
     RETURNING w.parent_workflow_id",
    &[ParameterType::Uuid, ParameterType::Text],
);

// ---------------------------------------------------------------------------
// Row types
// ---------------------------------------------------------------------------

/// A case 0 candidate.
#[derive(Debug, sqlx::FromRow)]
struct StuckPendingRow {
    workflow_id: Uuid,
    task_index: i32,
}

/// A node candidate of cases 1 and 1.5.
#[derive(Debug)]
struct ReadyNodeRow {
    workflow_id: Uuid,
    task_index: i32,
}

/// A case 1.6 candidate: the parent node of a terminal child.
#[derive(Debug)]
struct StaleSubworkflowRow {
    workflow_id: Uuid,
    task_index: i32,
    sub_workflow_id: Uuid,
}

/// A completion candidate of the tree pass.
#[derive(Debug, sqlx::FromRow)]
struct StuckWorkflowRow {
    workflow_id: Uuid,
}

/// An orphan candidate of the tree pass.
#[derive(Debug)]
struct OrphanedWorkflowRow {
    workflow_id: Uuid,
    name: String,
}

/// A `PAGE` row of the audit.
#[derive(Debug, sqlx::FromRow)]
struct GlobalWorkflowAuditRow {
    scanned_count: i64,
    completion_ids: Vec<Uuid>,
    orphan_ids: Vec<Uuid>,
    orphan_names: Vec<String>,
    claim_token: Option<Uuid>,
}

/// What the audit statement returned.
#[derive(Debug)]
enum AuditPage {
    /// One page; `claim_token` is set when the page has candidates.
    Page(GlobalWorkflowAuditRow),
    /// Another reaper holds the cursor row or its live claim; nothing was written.
    CursorHeld,
}

/// What an audit candidate statement did (`horsies_workflow_recover_completion`,
/// `horsies_workflow_fail_orphaned`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AuditAction {
    /// This call wrote the workflow terminal. `advance` names the workflow when it has a parent.
    Written { advance: ParentAdvance },
    /// The workflow did not change (not RUNNING, gone, a live node, or a node for case 4).
    Unchanged,
    /// The page claim is gone; nothing was done.
    ClaimLost,
}

/// The disposition of `horsies_workflow_node_reevaluate`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ReevaluateDisposition {
    /// The node left PENDING (`detail`: its status).
    Applied,
    /// The join is not met; nothing written.
    Waiting,
    /// The node is absent or not PENDING; nothing written.
    Stale,
    /// The workflow is not RUNNING; nothing written.
    NotRunning,
    /// No workflow has this id; nothing written.
    NotFound,
}

impl StepDisposition for ReevaluateDisposition {
    fn from_text(text: &str) -> Option<Self> {
        match text {
            "APPLIED" => Some(Self::Applied),
            "WAITING" => Some(Self::Waiting),
            "STALE" => Some(Self::Stale),
            "NOT_RUNNING" => Some(Self::NotRunning),
            "NOT_FOUND" => Some(Self::NotFound),
            _ => None,
        }
    }
}

/// The disposition of `horsies_workflow_nodes_enqueue_ready`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum EnqueueReadyDisposition {
    /// The nodes left READY (`detail`: their indexes): a task node with its task row, or failed by its build;
    /// a sub-workflow node with its child started, or failed.
    Applied,
    /// None of the nodes is READY with no work; nothing written.
    Stale,
    /// The workflow is not RUNNING; nothing written.
    NotRunning,
    /// No workflow has this id; nothing written.
    NotFound,
}

impl StepDisposition for EnqueueReadyDisposition {
    fn from_text(text: &str) -> Option<Self> {
        match text {
            "APPLIED" => Some(Self::Applied),
            "STALE" => Some(Self::Stale),
            "NOT_RUNNING" => Some(Self::NotRunning),
            "NOT_FOUND" => Some(Self::NotFound),
            _ => None,
        }
    }
}

/// What the tree pass's completion check did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TreeCompletion {
    /// The check wrote COMPLETED or FAILED.
    Written { advance: ParentAdvance },
    /// The workflow is gone, not RUNNING, or has a live node; nothing written.
    Unchanged,
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Rows that one global case query returns, and the size of one audit page (Python `GLOBAL_SCAN_ROW_CAP`).
/// Later cursor pages cover the remaining workflows.
pub(crate) const GLOBAL_SCAN_ROW_CAP: i64 = 200;
/// A caller renews this durable page lease with each candidate action. A stopped caller releases ownership
/// through expiry within five minutes (Python `GLOBAL_WORKFLOW_AUDIT_CLAIM_TTL_MS`).
const GLOBAL_WORKFLOW_AUDIT_CLAIM_TTL_MS: i64 = 300_000;

/// The workflows a pass reads.
#[derive(Clone, Copy)]
enum RecoveryScope<'a> {
    /// Every workflow: each case query returns at most `max_rows` rows, and cases 2/3 and 4 read one audit page
    /// of `max_rows` workflows (the audit function takes 1 to 1000).
    Global { max_rows: i64 },
    /// One workflow tree (the root and its descendants), with no row cap: a resume completes its tree in one
    /// pass.
    WorkflowTree(&'a [Uuid]),
}

fn elapsed_millis(started: Instant) -> u64 {
    u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX)
}

fn saturated_row_count(count: usize) -> u32 {
    u32::try_from(count).unwrap_or(u32::MAX)
}

/// Notes a failed discovery statement of a case in its metrics. The caller adds it to the report's errors.
fn record_discovery_error(metrics: &mut RecoveryCaseMetrics, started: Instant) {
    metrics.duration_ms = elapsed_millis(started);
    metrics.errors += 1;
}

/// The reaper's global pass with [`GLOBAL_SCAN_ROW_CAP`]: one pass id, so a workflow counts at most once per
/// pass toward `failure_limit` (`RecoveryConfig::workflow_recovery_failure_limit`, 3 to 1000).
///
/// The children of the sub-workflow nodes that a case starts come from the client's `source`.
///
/// Errors: a discovery statement, the audit page, or the count of workflows over the limit failed; the cases
/// before it ran. A candidate's own failure is not an error of the pass: it is logged, counted in the report,
/// and counted toward the limit when it is persistent.
pub async fn recover_stuck_workflows(
    broker: &PostgresBroker,
    source: &dyn ChildSpecSource,
    failure_limit: u32,
) -> Result<RecoveryReport, WorkflowError> {
    Box::pin(recover_stuck_workflows_observed_with(
        broker,
        source,
        failure_limit,
    ))
    .await
    .map_err(|failure| failure.error)
}

/// The reaper's global pass, with the children of its cases built by `build`. Outcomes as
/// [`recover_stuck_workflows`]; a failed pass keeps its partial report ([`RecoveryPassFailure`]).
pub(crate) async fn recover_stuck_workflows_observed_with(
    broker: &PostgresBroker,
    build: &dyn ChildSpecSource,
    failure_limit: u32,
) -> Result<RecoveryReport, RecoveryPassFailure> {
    Box::pin(recover_stuck_workflows_in_scope(
        broker,
        build,
        RecoveryScope::Global {
            max_rows: GLOBAL_SCAN_ROW_CAP,
        },
        RecoveryFailurePolicy::Count(CountFailures::new_pass(failure_limit)),
    ))
    .await
}

/// The recovery pass of one workflow tree (the root and its descendants), with no row cap, and the children of
/// its cases built by `build`. A resume runs it after its commit (Python `recover_stuck_workflow_tree`). It logs
/// candidate failures and counts none: the reaper's global pass counts the same candidates.
///
/// Errors: the tree read or a discovery statement failed.
pub(crate) async fn recover_workflow_tree(
    broker: &PostgresBroker,
    build: &dyn ChildSpecSource,
    root_workflow_id: Uuid,
) -> Result<RecoveryReport, WorkflowError> {
    let rows = run_alone(
        broker,
        CheckoutClass::Reserved,
        &WORKFLOW_TREE_IDS.bind().value(&root_workflow_id).finish(),
    )
    .await?;
    let workflow_ids = rows
        .iter()
        .map(|row| column::<Uuid>(row, &WORKFLOW_TREE_IDS, "id"))
        .collect::<Result<Vec<Uuid>, RowDecodeError>>()
        .map_err(StepContractError::from)?;
    Box::pin(recover_stuck_workflows_in_scope(
        broker,
        build,
        RecoveryScope::WorkflowTree(&workflow_ids),
        RecoveryFailurePolicy::LogOnly,
    ))
    .await
    .map_err(|failure| failure.error)
}

/// Each case future is on the heap, as each candidate step and parent advance inside a case is: inline, a
/// debug build needs more than a 2 MiB thread stack for one pass (see the module docs of `steps`).
async fn recover_stuck_workflows_in_scope(
    broker: &PostgresBroker,
    build: &dyn ChildSpecSource,
    scope: RecoveryScope<'_>,
    failure_policy: RecoveryFailurePolicy,
) -> Result<RecoveryReport, RecoveryPassFailure> {
    let mut report = RecoveryReport::default();
    let pass = PassContext {
        broker,
        build,
        scope,
        failure_policy,
    };

    if let Err(error) = Box::pin(recover_case0(&pass, &mut report)).await {
        return Err(RecoveryPassFailure { report, error });
    }
    if let Err(error) = Box::pin(recover_ready_nodes(&pass, ReadyCase::TaskNode, &mut report)).await
    {
        return Err(RecoveryPassFailure { report, error });
    }
    if let Err(error) = Box::pin(recover_ready_nodes(
        &pass,
        ReadyCase::SubWorkflowNode,
        &mut report,
    ))
    .await
    {
        return Err(RecoveryPassFailure { report, error });
    }
    if let Err(error) = Box::pin(recover_case1_6(&pass, &mut report)).await {
        return Err(RecoveryPassFailure { report, error });
    }
    let end_states = match scope {
        RecoveryScope::Global { max_rows } => {
            Box::pin(recover_global_workflow_end_states(
                &pass,
                max_rows,
                &mut report,
            ))
            .await
        }
        RecoveryScope::WorkflowTree(ids) => {
            match Box::pin(recover_tree_case2_3(&pass, ids, &mut report)).await {
                Ok(()) => Box::pin(recover_tree_case4(&pass, ids, &mut report)).await,
                Err(error) => Err(error),
            }
        }
    };
    if let Err(error) = end_states {
        return Err(RecoveryPassFailure { report, error });
    }
    match failure_policy {
        RecoveryFailurePolicy::Count(counting) => {
            match count_over_failure_limit(broker, counting.limit).await {
                Ok(count) => report.over_failure_limit = count,
                Err(error) => {
                    report.errors += 1;
                    report.pass_errors += 1;
                    return Err(RecoveryPassFailure { report, error });
                }
            }
        }
        RecoveryFailurePolicy::LogOnly => {}
    }

    if report.total() > 0 {
        tracing::info!(
            case0 = report.case0_pending_reevaluated,
            case1 = report.case1_ready_enqueued,
            case1_5 = report.case1_5_subworkflow_started,
            case1_6 = report.case1_6_subworkflow_completed,
            case2_3 = report.case2_3_workflow_completed,
            case4 = report.case4_orphaned_failed,
            errors = report.errors,
            "workflow recovery complete",
        );
    }

    Ok(report)
}

/// The facts every case of one pass reads.
#[derive(Clone, Copy)]
struct PassContext<'a> {
    broker: &'a PostgresBroker,
    build: &'a dyn ChildSpecSource,
    scope: RecoveryScope<'a>,
    failure_policy: RecoveryFailurePolicy,
}

impl PassContext<'_> {
    /// The row cap of a case query: `max_rows` for the global pass, none for a tree.
    fn row_cap(&self) -> Option<i64> {
        match self.scope {
            RecoveryScope::Global { max_rows } => Some(max_rows),
            RecoveryScope::WorkflowTree(_) => None,
        }
    }
}

/// Runs the discovery statement of a case on the reserve: the global statement with the row cap, or the tree
/// statement with the tree's ids and no cap.
async fn discover(
    pass: &PassContext<'_>,
    global: &'static TypedStatement,
    tree: &'static TypedStatement,
) -> Result<Vec<Row>, BrokerError> {
    let cap = pass.row_cap();
    match pass.scope {
        RecoveryScope::Global { .. } => {
            run_alone(
                pass.broker,
                CheckoutClass::Reserved,
                &global.bind().value(&cap).finish(),
            )
            .await
        }
        RecoveryScope::WorkflowTree(ids) => {
            let ids = ids.to_vec();
            run_alone(
                pass.broker,
                CheckoutClass::Reserved,
                &tree.bind().value(&ids).value(&cap).finish(),
            )
            .await
        }
    }
}

/// Runs the parent advances that a committed candidate owes, as their own steps on the reserve. A failure or a
/// panic is logged and counted nowhere: the child stays terminal and its parent node live, so recovery case 1.6
/// selects the pair next pass and counts a persistent failure on the parent (Python `_propagation_candidate`
/// with `LogFailuresOnly`).
async fn advance_after_candidate(
    pass: &PassContext<'_>,
    workflow_id: Uuid,
    owed: OwedParentAdvances,
) {
    if owed.is_empty() {
        return;
    }
    let advanced = run_catching_panic(async {
        Box::pin(advance_parents(
            pass.broker,
            CheckoutClass::Reserved,
            owed,
            pass.build,
        ))
        .await
        .map_err(|failure| failure.error)
    })
    .await;
    if let Err(failure) = advanced {
        tracing::error!(
            workflow_id = %workflow_id,
            error_type = failure.error_type(),
            error = %failure.detail(),
            "Parent advance after a workflow recovery candidate failed: \
             workflow_id={workflow_id} exception_type={} exception={}; recovery case 1.6 \
             advances it",
            failure.error_type(),
            failure.detail(),
        );
    }
}

// ---------------------------------------------------------------------------
// Case implementations
// ---------------------------------------------------------------------------

/// Case 0: each PENDING node whose dependencies are all terminal, as its own transaction:
/// `horsies_workflow_node_reevaluate` with its owed child starts.
async fn recover_case0(
    pass: &PassContext<'_>,
    report: &mut RecoveryReport,
) -> Result<(), WorkflowError> {
    let started = Instant::now();
    let rows = match discover(pass, &CASE0_GLOBAL, &CASE0_TREE).await {
        Ok(rows) => rows,
        Err(error) => {
            record_discovery_error(&mut report.metrics.case0, started);
            report.errors += 1;
            return Err(error.into());
        }
    };
    let candidates = match decode_rows(&rows, &CASE0_GLOBAL, |row, statement| {
        Ok(StuckPendingRow {
            workflow_id: column(row, statement, "workflow_id")?,
            task_index: column(row, statement, "task_index")?,
        })
    }) {
        Ok(candidates) => candidates,
        Err(error) => {
            record_discovery_error(&mut report.metrics.case0, started);
            report.errors += 1;
            return Err(error);
        }
    };
    let mut metrics = RecoveryCaseMetrics {
        rows_selected: saturated_row_count(candidates.len()),
        candidates_returned: saturated_row_count(candidates.len()),
        ..RecoveryCaseMetrics::default()
    };

    for row in candidates {
        let case = RecoveryCase::PendingTerminalDeps {
            task_index: row.task_index,
        };
        let candidate = Box::pin(async {
            run_in_transaction::<ReevaluateDisposition>(
                pass.broker,
                CheckoutClass::Reserved,
                &NODE_REEVALUATE
                    .bind()
                    .value(&row.workflow_id)
                    .value(&row.task_index)
                    .finish(),
                pass.build,
            )
            .await
        });
        let outcome = run_recovery_candidate(
            pass.broker,
            pass.failure_policy,
            row.workflow_id,
            case,
            candidate,
        )
        .await;
        match outcome {
            CandidateOutcome::Finished(committed) => {
                match committed.disposition {
                    ReevaluateDisposition::Applied => {
                        report.case0_pending_reevaluated += 1;
                        tracing::debug!(
                            workflow_id = %row.workflow_id,
                            task_index = row.task_index,
                            node_status = ?committed.detail,
                            "recovery case 0: re-evaluated pending node",
                        );
                    }
                    ReevaluateDisposition::Waiting
                    | ReevaluateDisposition::Stale
                    | ReevaluateDisposition::NotRunning
                    | ReevaluateDisposition::NotFound => metrics.refusals += 1,
                }
                Box::pin(advance_after_candidate(
                    pass,
                    row.workflow_id,
                    committed.parent_advances,
                ))
                .await;
            }
            CandidateOutcome::Failed(record) => {
                report.errors += 1;
                metrics.note_failure(record);
            }
        }
    }
    metrics.duration_ms = elapsed_millis(started);
    report.metrics.case0 = metrics;
    Ok(())
}

/// The kind of READY node that a case 1 or 1.5 pass repairs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReadyCase {
    /// Case 1: a READY task node with no task.
    TaskNode,
    /// Case 1.5: a READY sub-workflow node with no child.
    SubWorkflowNode,
}

impl ReadyCase {
    fn statements(self) -> (&'static TypedStatement, &'static TypedStatement) {
        match self {
            Self::TaskNode => (&CASE1_GLOBAL, &CASE1_TREE),
            Self::SubWorkflowNode => (&CASE1_5_GLOBAL, &CASE1_5_TREE),
        }
    }

    fn recovery_case(self, task_index: i32) -> RecoveryCase {
        match self {
            Self::TaskNode => RecoveryCase::ReadyNotEnqueued { task_index },
            Self::SubWorkflowNode => RecoveryCase::ReadySubworkflowNotStarted { task_index },
        }
    }

    fn metrics(self, report: &mut RecoveryReport) -> &mut RecoveryCaseMetrics {
        match self {
            Self::TaskNode => &mut report.metrics.case1,
            Self::SubWorkflowNode => &mut report.metrics.case1_5,
        }
    }

    fn count_recovered(self, report: &mut RecoveryReport) {
        match self {
            Self::TaskNode => report.case1_ready_enqueued += 1,
            Self::SubWorkflowNode => report.case1_5_subworkflow_started += 1,
        }
    }
}

/// Cases 1 and 1.5: each READY node with no work, as its own transaction:
/// `horsies_workflow_nodes_enqueue_ready` with the node's index, then the child start it owes (case 1.5). A
/// node whose build fails is written FAILED by the step and its workflow's `on_error` applies (Python
/// `enqueue_workflow_task` and `enqueue_subworkflow_task`).
async fn recover_ready_nodes(
    pass: &PassContext<'_>,
    ready: ReadyCase,
    report: &mut RecoveryReport,
) -> Result<(), WorkflowError> {
    let started = Instant::now();
    let (global, tree) = ready.statements();
    let rows = match discover(pass, global, tree).await {
        Ok(rows) => rows,
        Err(error) => {
            record_discovery_error(ready.metrics(report), started);
            report.errors += 1;
            return Err(error.into());
        }
    };
    let candidates = match decode_rows(&rows, global, |row, statement| {
        Ok(ReadyNodeRow {
            workflow_id: column(row, statement, "workflow_id")?,
            task_index: column(row, statement, "task_index")?,
        })
    }) {
        Ok(candidates) => candidates,
        Err(error) => {
            record_discovery_error(ready.metrics(report), started);
            report.errors += 1;
            return Err(error);
        }
    };
    let mut metrics = RecoveryCaseMetrics {
        rows_selected: saturated_row_count(candidates.len()),
        candidates_returned: saturated_row_count(candidates.len()),
        ..RecoveryCaseMetrics::default()
    };

    for row in candidates {
        let task_indexes = vec![row.task_index];
        let candidate = Box::pin(async {
            run_in_transaction::<EnqueueReadyDisposition>(
                pass.broker,
                CheckoutClass::Reserved,
                &NODES_ENQUEUE_READY
                    .bind()
                    .value(&row.workflow_id)
                    .value(&task_indexes)
                    .finish(),
                pass.build,
            )
            .await
        });
        let outcome = run_recovery_candidate(
            pass.broker,
            pass.failure_policy,
            row.workflow_id,
            ready.recovery_case(row.task_index),
            candidate,
        )
        .await;
        match outcome {
            CandidateOutcome::Finished(committed) => {
                match committed.disposition {
                    EnqueueReadyDisposition::Applied => {
                        ready.count_recovered(report);
                        tracing::debug!(
                            workflow_id = %row.workflow_id,
                            task_index = row.task_index,
                            case = ?ready,
                            "workflow recovery: READY node left READY",
                        );
                    }
                    EnqueueReadyDisposition::Stale
                    | EnqueueReadyDisposition::NotRunning
                    | EnqueueReadyDisposition::NotFound => metrics.refusals += 1,
                }
                Box::pin(advance_after_candidate(
                    pass,
                    row.workflow_id,
                    committed.parent_advances,
                ))
                .await;
            }
            CandidateOutcome::Failed(record) => {
                report.errors += 1;
                metrics.note_failure(record);
            }
        }
    }
    metrics.duration_ms = elapsed_millis(started);
    *ready.metrics(report) = metrics;
    Ok(())
}

/// Case 1.6: each RUNNING parent node of a terminal child, as its own transaction:
/// `horsies_workflow_child_finish` with its owed child starts. A failure counts on the parent workflow.
async fn recover_case1_6(
    pass: &PassContext<'_>,
    report: &mut RecoveryReport,
) -> Result<(), WorkflowError> {
    let started = Instant::now();
    let rows = match discover(pass, &CASE1_6_GLOBAL, &CASE1_6_TREE).await {
        Ok(rows) => rows,
        Err(error) => {
            record_discovery_error(&mut report.metrics.case1_6, started);
            report.errors += 1;
            return Err(error.into());
        }
    };
    let candidates = match decode_rows(&rows, &CASE1_6_GLOBAL, |row, statement| {
        Ok(StaleSubworkflowRow {
            workflow_id: column(row, statement, "workflow_id")?,
            task_index: column(row, statement, "task_index")?,
            sub_workflow_id: column(row, statement, "sub_workflow_id")?,
        })
    }) {
        Ok(candidates) => candidates,
        Err(error) => {
            record_discovery_error(&mut report.metrics.case1_6, started);
            report.errors += 1;
            return Err(error);
        }
    };
    let mut metrics = RecoveryCaseMetrics {
        rows_selected: saturated_row_count(candidates.len()),
        candidates_returned: saturated_row_count(candidates.len()),
        ..RecoveryCaseMetrics::default()
    };

    for row in candidates {
        let case = RecoveryCase::CompletedChildParentNotUpdated {
            parent_task_index: row.task_index,
        };
        let candidate = Box::pin(async {
            run_in_transaction::<ChildFinishDisposition>(
                pass.broker,
                CheckoutClass::Reserved,
                &CHILD_FINISH.bind().value(&row.sub_workflow_id).finish(),
                pass.build,
            )
            .await
        });
        let outcome = run_recovery_candidate(
            pass.broker,
            pass.failure_policy,
            row.workflow_id,
            case,
            candidate,
        )
        .await;
        match outcome {
            CandidateOutcome::Finished(committed) => {
                match committed.disposition {
                    ChildFinishDisposition::Applied => {
                        report.case1_6_subworkflow_completed += 1;
                        tracing::debug!(
                            workflow_id = %row.workflow_id,
                            task_index = row.task_index,
                            child_workflow_id = %row.sub_workflow_id,
                            "recovery case 1.6: advanced the parent node of a terminal child",
                        );
                    }
                    ChildFinishDisposition::AlreadyApplied
                    | ChildFinishDisposition::ChildAbsent
                    | ChildFinishDisposition::NotAChild
                    | ChildFinishDisposition::ChildNotTerminal
                    | ChildFinishDisposition::ParentAbsent => metrics.refusals += 1,
                }
                Box::pin(advance_after_candidate(
                    pass,
                    row.workflow_id,
                    committed.parent_advances,
                ))
                .await;
            }
            CandidateOutcome::Failed(record) => {
                report.errors += 1;
                metrics.note_failure(record);
            }
        }
    }
    metrics.duration_ms = elapsed_millis(started);
    report.metrics.case1_6 = metrics;
    Ok(())
}

/// Decodes every row of a discovery statement with `read`. A row that does not decode is a
/// [`StepContractError`]: the core and the installed schema disagree.
fn decode_rows<T>(
    rows: &[Row],
    statement: &TypedStatement,
    read: impl Fn(&Row, &TypedStatement) -> Result<T, RowDecodeError>,
) -> Result<Vec<T>, WorkflowError> {
    rows.iter()
        .map(|row| read(row, statement))
        .collect::<Result<Vec<T>, RowDecodeError>>()
        .map_err(|error| WorkflowError::StepContract(StepContractError::from(error)))
}

/// Cases 2/3 and 4 of the global pass: one audit page of `max_rows` RUNNING workflows, then one statement per
/// candidate that renews the page claim with its work (Python `_recover_stuck_workflows_global`,
/// `core/workflows/recovery.py:1229-1390`). The completion of a child that has a parent is followed by its parent
/// advance as its own step. The claim is released at the end while this pass still owns it.
///
/// Errors: the audit statement failed (both case metrics note it).
async fn recover_global_workflow_end_states(
    pass: &PassContext<'_>,
    max_rows: i64,
    report: &mut RecoveryReport,
) -> Result<(), WorkflowError> {
    let started = Instant::now();
    let requested_claim_token = Uuid::new_v4();
    let audited = audit_page(pass.broker, max_rows, requested_claim_token).await;
    let duration_ms = elapsed_millis(started);
    report.metrics.case2_3.duration_ms = duration_ms;
    report.metrics.case4.duration_ms = duration_ms;
    let page = match audited {
        Ok(AuditPage::Page(page)) => page,
        Ok(AuditPage::CursorHeld) => {
            report.metrics.case2_3.refusals += 1;
            report.metrics.case4.refusals += 1;
            return Ok(());
        }
        Err(error) => {
            report.metrics.case2_3.errors += 1;
            report.metrics.case4.errors += 1;
            report.errors += 1;
            return Err(error);
        }
    };

    let GlobalWorkflowAuditRow {
        scanned_count,
        completion_ids,
        orphan_ids,
        orphan_names,
        claim_token,
    } = page;
    let scanned_count = u32::try_from(scanned_count).unwrap_or(u32::MAX);
    report.metrics.case2_3.rows_selected = scanned_count;
    report.metrics.case2_3.candidates_returned = saturated_row_count(completion_ids.len());
    report.completion_candidate_yield = match scanned_count {
        0 => None,
        count => Some(f64::from(report.metrics.case2_3.candidates_returned) / f64::from(count)),
    };
    report.metrics.case4.rows_selected = scanned_count;
    report.metrics.case4.candidates_returned = saturated_row_count(orphan_ids.len());

    let Some(claim_token) = claim_token else {
        // A page with no candidate carries no claim.
        if !completion_ids.is_empty() {
            report.metrics.case2_3.refusals += 1;
        }
        if !orphan_ids.is_empty() {
            report.metrics.case4.refusals += 1;
        }
        return Ok(());
    };

    let mut owns_claim = true;
    for workflow_id in completion_ids {
        match Box::pin(recover_completion_candidate(
            pass,
            workflow_id,
            claim_token,
            report,
        ))
        .await
        {
            ClaimHold::Held => {}
            ClaimHold::Lost => {
                owns_claim = false;
                break;
            }
        }
    }
    if owns_claim {
        for (workflow_id, name) in orphan_ids.into_iter().zip(orphan_names) {
            match Box::pin(recover_orphan_candidate(
                pass,
                workflow_id,
                &name,
                claim_token,
                report,
            ))
            .await
            {
                ClaimHold::Held => {}
                ClaimHold::Lost => {
                    owns_claim = false;
                    break;
                }
            }
        }
    }
    if owns_claim {
        let released = run_alone(
            pass.broker,
            CheckoutClass::Reserved,
            &RELEASE_AUDIT_CLAIM.bind().value(&claim_token).finish(),
        )
        .await;
        if let Err(error) = released {
            tracing::error!(%error, "Workflow audit claim release failed: {error}");
            report.errors += 1;
            report.metrics.case2_3.errors += 1;
            report.metrics.case4.errors += 1;
        }
    }
    let duration_ms = elapsed_millis(started);
    report.metrics.case2_3.duration_ms = duration_ms;
    report.metrics.case4.duration_ms = duration_ms;
    Ok(())
}

/// Whether the pass still holds the page claim after a candidate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ClaimHold {
    Held,
    Lost,
}

/// Cases 2/3 of one audit candidate: `horsies_workflow_recover_completion`, one statement in autocommit, then
/// the parent advance it owes. A failure of the statement counts toward the limit on the workflow
/// (`terminal_workflow_running`).
async fn recover_completion_candidate(
    pass: &PassContext<'_>,
    workflow_id: Uuid,
    claim_token: Uuid,
    report: &mut RecoveryReport,
) -> ClaimHold {
    let candidate = async {
        let rows = run_alone(
            pass.broker,
            CheckoutClass::Reserved,
            &RECOVER_COMPLETION
                .bind()
                .value(&workflow_id)
                .value(&claim_token)
                .value(&GLOBAL_WORKFLOW_AUDIT_CLAIM_TTL_MS)
                .finish(),
        )
        .await?;
        Ok::<AuditAction, WorkflowError>(audit_action(rows, &RECOVER_COMPLETION)?)
    };
    let outcome = run_recovery_candidate(
        pass.broker,
        pass.failure_policy,
        workflow_id,
        RecoveryCase::TerminalWorkflowRunning,
        candidate,
    )
    .await;
    match outcome {
        CandidateOutcome::Finished(AuditAction::Written { advance }) => {
            report.case2_3_workflow_completed += 1;
            tracing::debug!(
                workflow_id = %workflow_id,
                "recovery case 2+3: finalized a workflow whose nodes are all terminal",
            );
            Box::pin(advance_after_candidate(
                pass,
                workflow_id,
                OwedParentAdvances::of(advance),
            ))
            .await;
            ClaimHold::Held
        }
        CandidateOutcome::Finished(AuditAction::Unchanged) => {
            report.metrics.case2_3.refusals += 1;
            ClaimHold::Held
        }
        CandidateOutcome::Finished(AuditAction::ClaimLost) => {
            report.metrics.case2_3.refusals += 1;
            ClaimHold::Lost
        }
        CandidateOutcome::Failed(record) => {
            report.errors += 1;
            report.metrics.case2_3.note_failure(record);
            ClaimHold::Held
        }
    }
}

/// Case 4 of one audit candidate: `horsies_workflow_fail_orphaned`, one statement in autocommit, then the
/// parent advance it owes. A failure is logged and counted as an error of the case, not toward the limit
/// (Python `_recover_global_orphan_candidate`).
async fn recover_orphan_candidate(
    pass: &PassContext<'_>,
    workflow_id: Uuid,
    name: &str,
    claim_token: Uuid,
    report: &mut RecoveryReport,
) -> ClaimHold {
    let failed = async {
        let error = orphaned_workflow_error(workflow_id, name)?;
        let rows = run_alone(
            pass.broker,
            CheckoutClass::Reserved,
            &FAIL_ORPHANED
                .bind()
                .value(&workflow_id)
                .value(&claim_token)
                .value(&GLOBAL_WORKFLOW_AUDIT_CLAIM_TTL_MS)
                .value(&error)
                .finish(),
        )
        .await?;
        Ok::<AuditAction, WorkflowError>(audit_action(rows, &FAIL_ORPHANED)?)
    };
    match failed.await {
        Ok(AuditAction::Written { advance }) => {
            report.case4_orphaned_failed += 1;
            tracing::warn!(
                workflow_id = %workflow_id,
                workflow_name = %name,
                "recovery case 4: failed orphaned workflow (no tasks)",
            );
            Box::pin(advance_after_candidate(
                pass,
                workflow_id,
                OwedParentAdvances::of(advance),
            ))
            .await;
            ClaimHold::Held
        }
        Ok(AuditAction::Unchanged) => {
            report.metrics.case4.refusals += 1;
            ClaimHold::Held
        }
        Ok(AuditAction::ClaimLost) => {
            report.metrics.case4.refusals += 1;
            ClaimHold::Lost
        }
        Err(error) => {
            tracing::error!(
                workflow_id = %workflow_id,
                error = %error,
                "Global orphan workflow recovery failed: workflow_id={workflow_id} \
                 exception={error}",
            );
            report.errors += 1;
            report.metrics.case4.errors += 1;
            ClaimHold::Held
        }
    }
}

/// Reads one audit page on the reserve.
///
/// Errors: the statement failed ([`WorkflowError::Broker`]); a row outside the function's contract
/// ([`WorkflowError::StepContract`]).
async fn audit_page(
    broker: &PostgresBroker,
    max_rows: i64,
    claim_token: Uuid,
) -> Result<AuditPage, WorkflowError> {
    let rows = run_alone(
        broker,
        CheckoutClass::Reserved,
        &AUDIT_PAGE
            .bind()
            .value(&max_rows)
            .value(&claim_token)
            .value(&GLOBAL_WORKFLOW_AUDIT_CLAIM_TTL_MS)
            .finish(),
    )
    .await?;
    Ok(audit_page_row(rows)?)
}

/// Decodes the one row of `horsies_workflow_recovery_audit_page`. Pure.
fn audit_page_row(rows: Vec<Row>) -> Result<AuditPage, StepContractError> {
    let row = one_row(rows, &AUDIT_PAGE)?;
    let outcome: String = column(&row, &AUDIT_PAGE, "outcome")?;
    match outcome.as_str() {
        "PAGE" => Ok(AuditPage::Page(GlobalWorkflowAuditRow {
            scanned_count: column(&row, &AUDIT_PAGE, "scanned_count")?,
            completion_ids: column(&row, &AUDIT_PAGE, "completion_ids")?,
            orphan_ids: column(&row, &AUDIT_PAGE, "orphan_ids")?,
            orphan_names: column(&row, &AUDIT_PAGE, "orphan_names")?,
            claim_token: column(&row, &AUDIT_PAGE, "claim_token")?,
        })),
        "CURSOR_HELD" => Ok(AuditPage::CursorHeld),
        _ => Err(StepContractError::UnknownOutcome {
            function: AUDIT_PAGE.name(),
            outcome,
        }),
    }
}

/// Decodes the one row of `horsies_workflow_recover_completion` or `horsies_workflow_fail_orphaned`
/// (`outcome`, `workflow_status`, `advance_child_workflow_id`). Pure.
fn audit_action(
    rows: Vec<Row>,
    statement: &TypedStatement,
) -> Result<AuditAction, StepContractError> {
    let row = one_row(rows, statement)?;
    let outcome: String = column(&row, statement, "outcome")?;
    match outcome.as_str() {
        "COMPLETED" | "FAILED" => {
            let advance: Option<Uuid> = column(&row, statement, "advance_child_workflow_id")?;
            Ok(AuditAction::Written {
                advance: match advance {
                    Some(child_workflow_id) => ParentAdvance::Owed { child_workflow_id },
                    None => ParentAdvance::NotOwed,
                },
            })
        }
        "UNCHANGED" => Ok(AuditAction::Unchanged),
        "AUDIT_CLAIM_LOST" => Ok(AuditAction::ClaimLost),
        _ => Err(StepContractError::UnknownOutcome {
            function: statement.name(),
            outcome,
        }),
    }
}

/// Cases 2/3 of the tree pass: each RUNNING workflow of the tree whose nodes are all terminal, as its own
/// transaction: the workflow row lock, then the completion check (`horsies_workflow_try_complete`), then the
/// parent advance it owes. The tree pass holds no audit claim, so it does not use
/// `horsies_workflow_recover_completion` (Python's tree pass calls `check_workflow_completion` the same way).
async fn recover_tree_case2_3(
    pass: &PassContext<'_>,
    workflow_ids: &[Uuid],
    report: &mut RecoveryReport,
) -> Result<(), WorkflowError> {
    let started = Instant::now();
    let ids = workflow_ids.to_vec();
    let cap: Option<i64> = None;
    let rows = match run_alone(
        pass.broker,
        CheckoutClass::Reserved,
        &CASE2_3_TREE.bind().value(&ids).value(&cap).finish(),
    )
    .await
    {
        Ok(rows) => rows,
        Err(error) => {
            record_discovery_error(&mut report.metrics.case2_3, started);
            report.errors += 1;
            return Err(error.into());
        }
    };
    let candidates = match decode_rows(&rows, &CASE2_3_TREE, |row, statement| {
        Ok(StuckWorkflowRow {
            workflow_id: column(row, statement, "workflow_id")?,
        })
    }) {
        Ok(candidates) => candidates,
        Err(error) => {
            record_discovery_error(&mut report.metrics.case2_3, started);
            report.errors += 1;
            return Err(error);
        }
    };
    report.metrics.case2_3.rows_selected = saturated_row_count(candidates.len());
    report.metrics.case2_3.candidates_returned = report.metrics.case2_3.rows_selected;

    for row in candidates {
        let outcome = run_recovery_candidate(
            pass.broker,
            pass.failure_policy,
            row.workflow_id,
            RecoveryCase::TerminalWorkflowRunning,
            Box::pin(complete_in_tree(pass.broker, row.workflow_id)),
        )
        .await;
        match outcome {
            CandidateOutcome::Finished(TreeCompletion::Written { advance }) => {
                report.case2_3_workflow_completed += 1;
                tracing::debug!(
                    workflow_id = %row.workflow_id,
                    "recovery case 2+3: finalized a workflow of the tree",
                );
                Box::pin(advance_after_candidate(
                    pass,
                    row.workflow_id,
                    OwedParentAdvances::of(advance),
                ))
                .await;
            }
            CandidateOutcome::Finished(TreeCompletion::Unchanged) => {
                report.metrics.case2_3.refusals += 1;
            }
            CandidateOutcome::Failed(record) => {
                report.errors += 1;
                report.metrics.case2_3.note_failure(record);
            }
        }
    }
    report.metrics.case2_3.duration_ms = elapsed_millis(started);
    Ok(())
}

/// The completion check of one workflow in a transaction on the reserve: `BEGIN`, the row lock, the check,
/// `COMMIT`.
///
/// Errors: [`WorkflowError::Broker`] for a failed statement (nothing committed, or a lost `COMMIT` reply:
/// `OutcomeUnknown`); [`WorkflowError::StepContract`] for a row that does not decode (rolled back).
pub(crate) async fn complete_in_tree(
    broker: &PostgresBroker,
    workflow_id: Uuid,
) -> Result<TreeCompletion, WorkflowError> {
    let (open, _write) = broker.core_pools_for_write()?;
    let mut connection =
        checkout(open.main(), CheckoutClass::Reserved, &LOCK_TREE_CANDIDATE).await?;
    let mut transaction = connection
        .begin(IsolationLevel::ServerDefault)
        .await
        .map_err(BrokerError::from)?;
    let locked = query_in(
        &mut transaction,
        broker.statement_log(),
        &LOCK_TREE_CANDIDATE.bind().value(&workflow_id).finish(),
    )
    .await
    .map_err(BrokerError::from)?;
    let Some(locked) = first_row(locked) else {
        roll_back(transaction).await?;
        return Ok(TreeCompletion::Unchanged);
    };
    let parent: Option<Uuid> = match column(&locked, &LOCK_TREE_CANDIDATE, "parent_workflow_id") {
        Ok(parent) => parent,
        Err(error) => {
            roll_back(transaction).await?;
            return Err(StepContractError::from(error).into());
        }
    };
    let checked = query_in(
        &mut transaction,
        broker.statement_log(),
        &TRY_COMPLETE.bind().value(&workflow_id).finish(),
    )
    .await
    .map_err(BrokerError::from)?;
    let written: Option<String> = match one_row(checked, &TRY_COMPLETE)
        .and_then(|row| column(&row, &TRY_COMPLETE, "written"))
    {
        Ok(written) => written,
        Err(error) => {
            roll_back(transaction).await?;
            return Err(StepContractError::from(error).into());
        }
    };
    transaction.commit().await.map_err(BrokerError::from)?;
    Ok(match (written, parent) {
        (None, _) => TreeCompletion::Unchanged,
        (Some(_), Some(_)) => TreeCompletion::Written {
            advance: ParentAdvance::Owed {
                child_workflow_id: workflow_id,
            },
        },
        (Some(_), None) => TreeCompletion::Written {
            advance: ParentAdvance::NotOwed,
        },
    })
}

/// Case 4 of the tree pass: each RUNNING workflow of the tree with no node goes FAILED with
/// `WORKFLOW_START_INCOMPLETE`, one statement in autocommit, then the parent advance it owes. A failure is
/// logged and counted as an error of the case.
async fn recover_tree_case4(
    pass: &PassContext<'_>,
    workflow_ids: &[Uuid],
    report: &mut RecoveryReport,
) -> Result<(), WorkflowError> {
    let started = Instant::now();
    let ids = workflow_ids.to_vec();
    let cap: Option<i64> = None;
    let rows = match run_alone(
        pass.broker,
        CheckoutClass::Reserved,
        &CASE4_TREE.bind().value(&ids).value(&cap).finish(),
    )
    .await
    {
        Ok(rows) => rows,
        Err(error) => {
            record_discovery_error(&mut report.metrics.case4, started);
            report.errors += 1;
            return Err(error.into());
        }
    };
    let candidates = match decode_rows(&rows, &CASE4_TREE, |row, statement| {
        Ok(OrphanedWorkflowRow {
            workflow_id: column(row, statement, "workflow_id")?,
            name: column(row, statement, "name")?,
        })
    }) {
        Ok(candidates) => candidates,
        Err(error) => {
            record_discovery_error(&mut report.metrics.case4, started);
            report.errors += 1;
            return Err(error);
        }
    };
    report.metrics.case4.rows_selected = saturated_row_count(candidates.len());
    report.metrics.case4.candidates_returned = report.metrics.case4.rows_selected;

    for row in candidates {
        match fail_tree_orphan(pass.broker, row.workflow_id, &row.name).await {
            Ok(Some(advance)) => {
                report.case4_orphaned_failed += 1;
                tracing::warn!(
                    workflow_id = %row.workflow_id,
                    workflow_name = %row.name,
                    "recovery case 4: failed orphaned workflow (no tasks)",
                );
                Box::pin(advance_after_candidate(
                    pass,
                    row.workflow_id,
                    OwedParentAdvances::of(advance),
                ))
                .await;
            }
            Ok(None) => report.metrics.case4.refusals += 1,
            Err(error) => {
                tracing::error!(
                    workflow_id = %row.workflow_id,
                    error = %error,
                    "Orphan workflow recovery failed: workflow_id={} exception={error}",
                    row.workflow_id,
                );
                report.errors += 1;
                report.metrics.case4.errors += 1;
            }
        }
    }
    report.metrics.case4.duration_ms = elapsed_millis(started);
    Ok(())
}

/// The orphan write of the tree pass: the parent advance it owes when it wrote the workflow FAILED, `None` when
/// the workflow did not change.
async fn fail_tree_orphan(
    broker: &PostgresBroker,
    workflow_id: Uuid,
    name: &str,
) -> Result<Option<ParentAdvance>, WorkflowError> {
    let error = orphaned_workflow_error(workflow_id, name)?;
    let rows = run_alone(
        broker,
        CheckoutClass::Reserved,
        &FAIL_TREE_ORPHAN
            .bind()
            .value(&workflow_id)
            .value(&error)
            .finish(),
    )
    .await?;
    let Some(row) = first_row(rows) else {
        return Ok(None);
    };
    let parent: Option<Uuid> =
        column(&row, &FAIL_TREE_ORPHAN, "parent_workflow_id").map_err(StepContractError::from)?;
    Ok(Some(match parent {
        Some(_) => ParentAdvance::Owed {
            child_workflow_id: workflow_id,
        },
        None => ParentAdvance::NotOwed,
    }))
}

/// The stored error of a RUNNING workflow with no node rows (recovery case 4): its start did not finish.
///
/// Python stores `{"error_code": "E400", ...}` (`core/workflows/recovery.py:1001-1008, 1199-1206`), which is
/// not a catalog error; the core stores the catalog code `WORKFLOW_START_INCOMPLETE` (approved rule), with
/// Python's message.
fn orphaned_workflow_error(workflow_id: Uuid, name: &str) -> Result<String, serde_json::Error> {
    serde_json::to_string(&TaskError {
        error_code: OperationalErrorCode::WorkflowStartIncomplete.into(),
        message: Some(format!(
            "Orphaned workflow '{name}': no workflow_tasks found. \
             Workflow task insertion did not complete."
        )),
        exception: None,
        data: Some(crate::core::codec::JsonText::of_engine_value(
            &serde_json::json!({
                "workflow_id": workflow_id.to_string(),
                "recovery": "case_4",
            }),
        )),
        kind: crate::core::TaskErrorKind::Plain,
    })
}

/// Cases 2/3 and 4 of a global pass with `max_rows` per audit page, for the tests of this module,
/// with the children of its cases from `child_specs`.
#[cfg(test)]
async fn audit_end_states(
    broker: &PostgresBroker,
    child_specs: &crate::test_support::child_specs::TestChildSpecs,
    max_rows: i64,
    report: &mut RecoveryReport,
    failure_policy: RecoveryFailurePolicy,
) -> Result<(), WorkflowError> {
    let pass = PassContext {
        broker,
        build: child_specs,
        scope: RecoveryScope::Global { max_rows },
        failure_policy,
    };
    recover_global_workflow_end_states(&pass, max_rows, report).await
}

#[cfg(test)]
mod cap_tests {
    use super::*;
    use crate::test_support::child_specs::TestChildSpecs;
    use crate::workflow_engine::steps::CommittedStep;
    use serial_test::serial;
    use sqlx::PgPool;
    use uuid::Uuid;

    /// A broker on the database of `pool`: the recovery statements run on its core pools.
    fn broker_of(pool: &PgPool) -> PostgresBroker {
        PostgresBroker::from_pool(pool.clone(), crate::ClientFamily::Rust)
    }

    /// A broker whose database refuses every connection (no server listens on port 1): each statement fails
    /// with a retryable connection error.
    fn unreachable_broker() -> PostgresBroker {
        PostgresBroker::from_pool(
            sqlx::postgres::PgPoolOptions::new()
                .max_connections(1)
                .acquire_timeout(std::time::Duration::from_millis(100))
                .connect_lazy("postgresql://horsies@127.0.0.1:1/horsies")
                .expect("a lazy pool opens no connection"),
            crate::ClientFamily::Rust,
        )
    }

    /// The step of a case 1 or 1.5 candidate for node `task_index`: `horsies_workflow_nodes_enqueue_ready` in a
    /// transaction with its owed child starts, on the reserve.
    async fn enqueue_ready_node(
        broker: &PostgresBroker,
        workflow_id: Uuid,
        task_index: i32,
    ) -> Result<CommittedStep<EnqueueReadyDisposition>, WorkflowError> {
        let child_specs = TestChildSpecs::empty();
        let task_indexes = vec![task_index];
        run_in_transaction(
            broker,
            CheckoutClass::Reserved,
            &NODES_ENQUEUE_READY
                .bind()
                .value(&workflow_id)
                .value(&task_indexes)
                .finish(),
            &child_specs,
        )
        .await
    }

    /// The step of a case 0 candidate for node `task_index`: `horsies_workflow_node_reevaluate` in a
    /// transaction with its owed child starts, on the reserve.
    async fn reevaluate_node(
        broker: &PostgresBroker,
        workflow_id: Uuid,
        task_index: i32,
    ) -> Result<CommittedStep<ReevaluateDisposition>, WorkflowError> {
        let child_specs = TestChildSpecs::empty();
        run_in_transaction(
            broker,
            CheckoutClass::Reserved,
            &NODE_REEVALUATE
                .bind()
                .value(&workflow_id)
                .value(&task_index)
                .finish(),
            &child_specs,
        )
        .await
    }

    fn plan_has_sequential_scan(plan: &serde_json::Value, relation: &str) -> bool {
        match plan {
            serde_json::Value::Array(values) => values
                .iter()
                .any(|value| plan_has_sequential_scan(value, relation)),
            serde_json::Value::Object(fields) => {
                let is_target_scan = fields.get("Node Type").and_then(serde_json::Value::as_str)
                    == Some("Seq Scan")
                    && fields
                        .get("Relation Name")
                        .and_then(serde_json::Value::as_str)
                        == Some(relation);
                is_target_scan
                    || fields
                        .values()
                        .any(|value| plan_has_sequential_scan(value, relation))
            }
            _ => false,
        }
    }

    fn relation_rows_examined(plan: &serde_json::Value, relation: &str) -> f64 {
        match plan {
            serde_json::Value::Array(values) => values
                .iter()
                .map(|value| relation_rows_examined(value, relation))
                .sum(),
            serde_json::Value::Object(fields) => {
                let current = if fields
                    .get("Relation Name")
                    .and_then(serde_json::Value::as_str)
                    == Some(relation)
                {
                    let rows = [
                        "Actual Rows",
                        "Rows Removed by Filter",
                        "Rows Removed by Index Recheck",
                    ]
                    .into_iter()
                    .map(|field| {
                        fields
                            .get(field)
                            .and_then(serde_json::Value::as_f64)
                            .unwrap_or(0.0)
                    })
                    .sum::<f64>();
                    let loops = fields
                        .get("Actual Loops")
                        .and_then(serde_json::Value::as_f64)
                        .unwrap_or(0.0);
                    rows * loops
                } else {
                    0.0
                };
                current
                    + fields
                        .values()
                        .map(|value| relation_rows_examined(value, relation))
                        .sum::<f64>()
            }
            _ => 0.0,
        }
    }

    async fn insert_orphaned_workflow(pool: &PgPool, id: Uuid) {
        sqlx::query(
            "INSERT INTO horsies_workflows (
                id, name, status, on_error, output_task_index,
                definition_key, depth, root_workflow_id,
                sent_at, created_at, started_at, updated_at
            ) VALUES (
                $1, 'cap_test_wf', 'RUNNING', 'fail', NULL,
                'test.cap.v1', 0, $1,
                NOW(), NOW(), NOW(), NOW()
            )",
        )
        .bind(id)
        .execute(pool)
        .await
        .unwrap();
    }

    async fn insert_running_workflow_with_pending_task(
        pool: &PgPool,
        workflow_id: Uuid,
        created_at: chrono::DateTime<chrono::Utc>,
    ) {
        sqlx::query(
            "INSERT INTO horsies_workflows (
                 id, name, status, on_error, output_task_index,
                 definition_key, depth, root_workflow_id,
                 sent_at, created_at, started_at, updated_at
             ) VALUES (
                 $1, 'bounded_recovery_watermark', 'RUNNING', 'fail', NULL,
                 $2, 0, $1, $3, $3, $3, $3
             )",
        )
        .bind(workflow_id)
        .bind(format!("test.bounded-recovery.watermark.{workflow_id}"))
        .bind(created_at)
        .execute(pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO horsies_workflow_tasks (
                downstream_subworkflow, id, workflow_id, task_index, node_id, task_name, input, queue_name, priority,
                dependencies, allow_failed_deps, join_type, status, is_subworkflow, created_at,
                max_retries, execution, retention_class_key
            ) VALUES (
                'NONE', $1, $2, 0, 'root', 'bounded_recovery_watermark_task', 'null', 'default', 100,
                '{}'::integer[], FALSE, 'all', 'PENDING', FALSE, $3, 0, 'ISOLATED',
                'standard_30d'
            )",
        )
        .bind(Uuid::new_v4())
        .bind(workflow_id)
        .bind(created_at)
        .execute(pool)
        .await
        .unwrap();
    }

    async fn failed_count(pool: &PgPool, ids: &[Uuid]) -> i64 {
        sqlx::query_scalar(
            "SELECT COUNT(*) FROM horsies_workflows WHERE id = ANY($1) AND status = 'FAILED'",
        )
        .bind(ids)
        .fetch_one(pool)
        .await
        .unwrap()
    }

    #[test]
    fn the_report_publishes_the_failure_bound_fields() {
        let mut report = RecoveryReport {
            over_failure_limit: 2,
            ..RecoveryReport::default()
        };
        report
            .metrics
            .case0
            .note_failure(FailureRecord::LimitReached);
        report.metrics.case1.note_failure(FailureRecord::Counted);
        report
            .metrics
            .case1_5
            .note_failure(FailureRecord::NotCounted);
        let snapshot = serde_json::to_value(&report).unwrap();
        assert_eq!(snapshot["over_failure_limit"], 2);
        assert_eq!(snapshot["metrics"]["case0"]["errors"], 1);
        assert_eq!(snapshot["metrics"]["case0"]["failures_counted"], 1);
        assert_eq!(snapshot["metrics"]["case0"]["limit_reached"], 1);
        assert_eq!(snapshot["metrics"]["case1"]["failures_counted"], 1);
        assert_eq!(snapshot["metrics"]["case1"]["limit_reached"], 0);
        assert_eq!(snapshot["metrics"]["case1_5"]["errors"], 1);
        assert_eq!(snapshot["metrics"]["case1_5"]["failures_counted"], 0);
    }

    #[test]
    fn an_orphaned_workflow_stores_workflow_start_incomplete() {
        let workflow_id = Uuid::new_v4();
        let stored = orphaned_workflow_error(workflow_id, "orphan").unwrap();
        let error: TaskError = serde_json::from_str(&stored).unwrap();
        assert_eq!(error.error_code.to_string(), "WORKFLOW_START_INCOMPLETE");
        assert_eq!(
            error.message.as_deref(),
            Some(
                "Orphaned workflow 'orphan': no workflow_tasks found. \
                 Workflow task insertion did not complete."
            )
        );
        assert_eq!(
            error.data_value(),
            Some(serde_json::json!({
                "workflow_id": workflow_id.to_string(),
                "recovery": "case_4",
            }))
        );
    }

    #[tokio::test]
    #[serial]
    async fn recovery_case_4_fails_an_orphaned_workflow_with_the_catalog_code() {
        let pool = crate::broker::terminalization_matrix::sqlx_migrated_pool().await;
        let workflow_id = Uuid::new_v4();
        insert_orphaned_workflow(&pool, workflow_id).await;
        let report =
            recover_workflow_tree(&broker_of(&pool), &TestChildSpecs::empty(), workflow_id)
                .await
                .unwrap();
        assert_eq!(report.case4_orphaned_failed, 1);
        let (status, error): (String, String) =
            sqlx::query_as("SELECT status, error FROM horsies_workflows WHERE id = $1")
                .bind(workflow_id)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(status, "FAILED");
        let error: TaskError = serde_json::from_str(&error).unwrap();
        assert_eq!(error.error_code.to_string(), "WORKFLOW_START_INCOMPLETE");
    }

    #[tokio::test]
    async fn discovery_query_failure_is_typed() {
        let error = recover_stuck_workflows(&unreachable_broker(), &TestChildSpecs::empty(), 25)
            .await
            .expect_err("candidate discovery failure must cross the containment seam");
        assert!(matches!(error, WorkflowError::Broker(_)), "{error:?}");
    }

    #[tokio::test]
    async fn discovery_query_failure_keeps_case_metrics_in_the_health_snapshot() {
        let failure = recover_stuck_workflows_observed_with(
            &unreachable_broker(),
            &TestChildSpecs::empty(),
            25,
        )
        .await
        .expect_err("candidate discovery failure must keep its partial report");
        assert!(
            matches!(&failure.error, WorkflowError::Broker(_)),
            "{:?}",
            failure.error
        );
        let snapshot = failure.into_health_snapshot();
        assert_eq!(snapshot["state"], "error");
        assert_eq!(snapshot["errors"], 1);
        assert_eq!(snapshot["metrics"]["case0"]["rows_selected"], 0);
        assert_eq!(snapshot["metrics"]["case0"]["candidates_returned"], 0);
        assert_eq!(snapshot["metrics"]["case0"]["errors"], 1);
        assert!(snapshot["metrics"]["case0"]["duration_ms"].is_u64());
    }

    #[tokio::test]
    async fn shared_workflow_audit_failure_marks_both_case_metrics() {
        let mut report = RecoveryReport::default();
        let error = audit_end_states(
            &unreachable_broker(),
            &TestChildSpecs::empty(),
            200,
            &mut report,
            RecoveryFailurePolicy::LogOnly,
        )
        .await
        .expect_err("shared audit query must fail on an unreachable database");
        assert!(matches!(error, WorkflowError::Broker(_)), "{error:?}");
        assert_eq!(report.errors, 1);
        assert_eq!(report.metrics.case2_3.errors, 1);
        assert_eq!(report.metrics.case4.errors, 1);
        assert_eq!(report.metrics.case2_3.rows_selected, 0);
        assert_eq!(report.metrics.case4.candidates_returned, 0);
        assert_eq!(report.completion_candidate_yield, None);
    }

    #[tokio::test]
    #[serial]
    /// A page of zero workflows is outside the audit function's bound (1 to 1000): the audit is refused
    /// (22023), both case metrics note the error, and the report has no completion yield.
    async fn a_zero_page_size_is_refused_and_has_undefined_completion_yield() {
        let pool = crate::broker::terminalization_matrix::sqlx_migrated_pool().await;
        let mut report = RecoveryReport::default();
        let refused = audit_end_states(
            &broker_of(&pool),
            &TestChildSpecs::empty(),
            0,
            &mut report,
            RecoveryFailurePolicy::LogOnly,
        )
        .await;
        assert!(
            matches!(refused, Err(WorkflowError::Broker(_))),
            "{refused:?}"
        );
        assert_eq!(report.metrics.case2_3.errors, 1);
        assert_eq!(report.metrics.case4.errors, 1);
        assert_eq!(report.metrics.case2_3.rows_selected, 0);
        assert_eq!(report.completion_candidate_yield, None);
        assert!(serde_json::to_value(report).unwrap()["completion_candidate_yield"].is_null());
    }

    #[tokio::test]
    #[serial]
    async fn case0_advances_the_selected_root_without_touching_unready_siblings() {
        let broker = PostgresBroker::from_pool(
            crate::broker::terminalization_matrix::sqlx_migrated_pool().await,
            crate::ClientFamily::Rust,
        );
        broker.ensure_schema_initialized().await.unwrap();
        let pool = broker.pool().clone();
        let workflow_id = Uuid::new_v4();
        sqlx::query(
            "INSERT INTO horsies_workflows (
                 id, name, status, on_error, definition_key, depth,
                 root_workflow_id, sent_at, created_at, started_at, updated_at
             ) VALUES ($1, 'p7_case0', 'RUNNING', 'fail', $2, 0, $1,
                       NOW(), NOW(), NOW(), NOW())",
        )
        .bind(workflow_id)
        .bind(format!("test.p7.case0.{workflow_id}"))
        .execute(&pool)
        .await
        .unwrap();
        // A node input is a JSON object text: the task row minted at ENQUEUED
        // refuses any other (horsies_workflow_task_input).
        for (task_index, dependencies) in [(0_i32, vec![]), (1_i32, vec![2_i32])] {
            sqlx::query(
                "INSERT INTO horsies_workflow_tasks (
                    downstream_subworkflow, id, workflow_id, task_index, node_id, task_name, input, queue_name,
                    priority, dependencies, allow_failed_deps, join_type, status,
                    is_subworkflow, created_at, max_retries, execution, retention_class_key
                ) VALUES (
                    'NONE', $1, $2, $3, $4, $5, '{}', 'default', 100, $6, FALSE, 'all', 'PENDING',
                    FALSE, NOW(), 0, 'ISOLATED', 'standard_30d'
                )",
            )
            .bind(Uuid::new_v4())
            .bind(workflow_id)
            .bind(task_index)
            .bind(format!("node_{task_index}"))
            .bind(format!("p7_case0_{task_index}"))
            .bind(dependencies)
            .execute(&pool)
            .await
            .unwrap();
        }
        sqlx::query(
            "INSERT INTO horsies_workflow_tasks (
                downstream_subworkflow, id, workflow_id, task_index, node_id, task_name, input, queue_name, priority,
                dependencies, allow_failed_deps, join_type, status, is_subworkflow, created_at,
                max_retries, execution, retention_class_key
            ) VALUES (
                'NONE', $1, $2, 2, 'blocker', 'p7_case0_blocker', '{}', 'default', 100, '{}', FALSE,
                'all', 'RUNNING', FALSE, NOW(), 0, 'ISOLATED', 'standard_30d'
            )",
        )
        .bind(Uuid::new_v4())
        .bind(workflow_id)
        .execute(&pool)
        .await
        .unwrap();

        let report =
            recover_workflow_tree(&broker_of(&pool), &TestChildSpecs::empty(), workflow_id)
                .await
                .unwrap();
        assert_eq!(report.case0_pending_reevaluated, 1);
        let statuses: Vec<(i32, String)> = sqlx::query_as(
            "SELECT task_index, status FROM horsies_workflow_tasks
             WHERE workflow_id = $1 ORDER BY task_index",
        )
        .bind(workflow_id)
        .fetch_all(&pool)
        .await
        .unwrap();
        assert_eq!(statuses[0].1, "ENQUEUED");
        assert_eq!(statuses[1].1, "PENDING");
        assert_eq!(statuses[2].1, "RUNNING");
    }

    /// The per-query cap bounds how many candidates one global pass processes;
    /// an uncapped (None) pass processes the rest. Parity with horsies PR #103.
    #[tokio::test]
    #[serial]
    async fn recovery_cap_limits_rows_then_uncapped_processes_rest() {
        let broker = PostgresBroker::from_pool(
            crate::broker::terminalization_matrix::sqlx_migrated_pool().await,
            crate::ClientFamily::Rust,
        );
        let pool = broker.pool().clone();

        // Isolate: remove any pre-existing orphaned workflows.
        sqlx::query("DELETE FROM horsies_workflow_tasks")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("DELETE FROM horsies_workflows")
            .execute(&pool)
            .await
            .unwrap();

        sqlx::query(
            "UPDATE horsies_recovery_scan_cursors
             SET last_created_at = NULL, last_id = NULL,
                 cycle_upper_created_at = NULL, cycle_upper_id = NULL,
                 claim_token = NULL, claim_expires_at = NULL,
                 completed_cycles = 0,
                 last_scan_rows = 0, last_candidate_rows = 0,
                 last_scan_at = NULL
             WHERE scan_name = 'running_workflows'",
        )
        .execute(&pool)
        .await
        .unwrap();

        let ids: Vec<Uuid> = (0..3).map(|_| Uuid::new_v4()).collect();
        for id in &ids {
            insert_orphaned_workflow(&pool, *id).await;
        }

        let child_specs = TestChildSpecs::empty();

        // Capped at 2: exactly 2 of the 3 orphans are failed this pass.
        let report = recover_stuck_workflows_in_scope(
            &broker,
            &child_specs,
            RecoveryScope::Global { max_rows: 2 },
            RecoveryFailurePolicy::LogOnly,
        )
        .await
        .map_err(|failure| failure.error)
        .unwrap();
        assert_eq!(report.case4_orphaned_failed, 2, "{report:?}");
        assert_eq!(failed_count(&pool, &ids).await, 2);

        // Uncapped: the remaining orphan is failed.
        let report = recover_stuck_workflows_in_scope(
            &broker,
            &child_specs,
            RecoveryScope::Global {
                max_rows: GLOBAL_SCAN_ROW_CAP,
            },
            RecoveryFailurePolicy::LogOnly,
        )
        .await
        .map_err(|failure| failure.error)
        .unwrap();
        assert_eq!(report.case4_orphaned_failed, 1);
        assert_eq!(failed_count(&pool, &ids).await, 3);
    }

    /// The session settings that send the plan of each statement inside a function to the client as a notice,
    /// with its buffers, for the running transaction.
    const AUTO_EXPLAIN_SETTINGS: &str = "LOAD 'auto_explain';
         SET LOCAL auto_explain.log_min_duration = 0;
         SET LOCAL auto_explain.log_nested_statements = on;
         SET LOCAL auto_explain.log_analyze = on;
         SET LOCAL auto_explain.log_buffers = on;
         SET LOCAL auto_explain.log_format = 'json';
         SET LOCAL client_min_messages = log;";

    /// The text before the JSON plan in an `auto_explain` message.
    const PLAN_MARKER: &str = "plan:\n";

    /// A text that only the page statement of `horsies_workflow_recovery_audit_page` has.
    const AUDIT_PAGE_STATEMENT_MARKER: &str = "cursor_row AS MATERIALIZED";

    /// The workflow-task indexes that a node probe of the audit may read.
    const WORKFLOW_TASK_INDEXES: [&str; 4] = [
        "idx_horsies_workflow_tasks_workflow",
        "uq_horsies_workflow_task_index",
        "idx_horsies_workflow_tasks_wf_status_index",
        "idx_horsies_workflow_tasks_nonterminal",
    ];

    /// The shared buffers (hit plus read) of a plan, counted at its root node.
    fn plan_shared_buffers(plan: &serde_json::Value) -> u64 {
        ["Shared Hit Blocks", "Shared Read Blocks"]
            .into_iter()
            .map(|field| plan[field].as_u64().unwrap_or(0))
            .sum()
    }

    /// One audit page of 200 rows: `horsies_workflow_recovery_audit_page` on `client`, inside the transaction
    /// that the caller opened with [`AUTO_EXPLAIN_SETTINGS`]. Returns the completion ids of the page and the
    /// root plan node of the page statement inside the function. A plain `EXPLAIN` of the call shows a
    /// Function Scan only; `auto_explain` with `log_nested_statements` sends the plan of each statement inside
    /// the function as a notice, which `notices` collects.
    async fn audit_page_with_plan(
        client: &tokio_postgres::Client,
        notices: &std::sync::Mutex<Vec<String>>,
    ) -> (Vec<Uuid>, serde_json::Value) {
        notices
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clear();
        let row = client
            .query_one(
                "SELECT outcome, completion_ids
                 FROM horsies_workflow_recovery_audit_page(200, $1, 30000)",
                &[&Uuid::new_v4()],
            )
            .await
            .expect("one audit page");
        let outcome: String = row.get("outcome");
        assert_eq!(outcome, "PAGE");
        let completion_ids: Vec<Uuid> = row.get("completion_ids");
        let messages = notices
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        let plans: Vec<serde_json::Value> = messages
            .iter()
            .filter_map(|message| message.split_once(PLAN_MARKER))
            .map(|(_, plan)| {
                serde_json::from_str::<serde_json::Value>(plan)
                    .expect("an auto_explain plan is JSON")
            })
            .filter(|document| {
                document["Query Text"]
                    .as_str()
                    .is_some_and(|text| text.contains(AUDIT_PAGE_STATEMENT_MARKER))
            })
            .collect();
        assert_eq!(
            plans.len(),
            1,
            "one plan of the page statement: {messages:?}"
        );
        (completion_ids, plans[0]["Plan"].clone())
    }

    /// The node probes of the audit page read through the partial index of live nodes, for wide and narrow
    /// workflows: 200 RUNNING workflows, one of them with 1,001 nodes, all nodes COMPLETED except nodes 0 and
    /// 1000, which are COMPLETED in the first round and RUNNING in the second. The page statement runs with a
    /// generic plan (the function's `plan_cache_mode`), and examines at most two node rows per workflow.
    #[tokio::test]
    #[serial]
    async fn generic_audit_uses_partial_index_for_wide_and_narrow_workflows() {
        let url = crate::test_support::migrated_database_url().await;
        let (client, notices) = crate::test_support::connect_collecting_notices(&url).await;
        client
            .batch_execute(
                "BEGIN;
                 DELETE FROM horsies_workflow_tasks; DELETE FROM horsies_workflows;
                 UPDATE horsies_recovery_scan_cursors SET last_created_at = NULL, last_id = NULL,
                     cycle_upper_created_at = NULL, cycle_upper_id = NULL,
                     claim_token = NULL, claim_expires_at = NULL;
                 INSERT INTO horsies_workflows (
                     id, name, status, on_error, depth, root_workflow_id, sent_at, created_at, updated_at
                 )
                 SELECT md5('partial-' || n)::uuid, 'partial_index', 'RUNNING', 'fail', 0,
                        md5('partial-' || n)::uuid, NOW(), NOW(), NOW()
                 FROM generate_series(1, 200) n;
                 INSERT INTO horsies_workflow_tasks (
                     downstream_subworkflow, id, workflow_id, task_index, task_name, input, queue_name,
                     priority, dependencies, allow_failed_deps, join_type, status, is_subworkflow,
                     max_retries, execution, retention_class_key, created_at
                 )
                 SELECT 'NONE', gen_random_uuid(), md5('partial-' || n)::uuid, i, 'partial_index', 'null',
                        'default', 100, '{}', FALSE, 'all', 'COMPLETED', FALSE, 0, 'ISOLATED',
                        'standard_30d', NOW()
                 FROM generate_series(1, 200) n
                 CROSS JOIN LATERAL generate_series(0, CASE WHEN n = 200 THEN 1000 ELSE 0 END) i;
                 ANALYZE horsies_workflows, horsies_workflow_tasks;",
            )
            .await
            .expect("seed 200 RUNNING workflows, one of 1,001 nodes");
        client
            .batch_execute(AUTO_EXPLAIN_SETTINGS)
            .await
            .expect("send the nested plans as notices");
        for (node_status, expected_completions) in [("COMPLETED", 200), ("RUNNING", 0)] {
            client
                .batch_execute(&format!(
                    "SAVEPOINT audit_probe;
                     UPDATE horsies_workflow_tasks SET status = '{node_status}'
                     WHERE task_index IN (0, 1000);
                     ANALYZE horsies_workflow_tasks;"
                ))
                .await
                .expect("set the status of nodes 0 and 1000");

            let (completion_ids, plan) = audit_page_with_plan(&client, &notices).await;

            assert_eq!(completion_ids.len(), expected_completions, "{node_status}");
            assert!(
                plan.to_string()
                    .contains("idx_horsies_workflow_tasks_nonterminal"),
                "{node_status}: {plan}"
            );
            assert!(
                relation_rows_examined(&plan, "horsies_workflow_tasks") <= 400.0,
                "{node_status}: {plan}"
            );
            client
                .batch_execute("ROLLBACK TO SAVEPOINT audit_probe")
                .await
                .expect("undo the round");
        }
        client.batch_execute("ROLLBACK").await.expect("rollback");
    }

    #[tokio::test]
    #[serial]
    async fn empty_global_workflow_audit_is_bounded_at_fifty_thousand_rows() {
        let pool = crate::broker::terminalization_matrix::sqlx_migrated_pool().await;
        sqlx::query("DELETE FROM horsies_workflow_tasks")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("DELETE FROM horsies_workflows")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query(
            "UPDATE horsies_recovery_scan_cursors
             SET last_created_at = NULL, last_id = NULL,
                 cycle_upper_created_at = NULL, cycle_upper_id = NULL,
                 claim_token = NULL, claim_expires_at = NULL,
                 completed_cycles = 0,
                 last_scan_rows = 0, last_candidate_rows = 0,
                 last_scan_at = NULL
             WHERE scan_name = 'running_workflows'",
        )
        .execute(&pool)
        .await
        .unwrap();

        sqlx::query(
            "WITH generated AS (
                 SELECT g,
                        ('10000000-0000-7000-8000-' ||
                         lpad(to_hex(g), 12, '0'))::uuid AS workflow_id
                 FROM generate_series(1, 50000) AS g
             )
             INSERT INTO horsies_workflows (
                 id, name, status, on_error, output_task_index,
                 definition_key, depth, root_workflow_id,
                 sent_at, created_at, started_at, completed_at, updated_at
             )
             SELECT workflow_id, 'bounded_recovery_stable_' || g,
                    CASE WHEN g % 4 = 0 THEN 'RUNNING' ELSE 'COMPLETED' END,
                    'fail', NULL,
                    'test.bounded-recovery.stable.' || g, 0, workflow_id,
                    NOW(), NOW(), NOW(),
                    CASE WHEN g % 4 = 0 THEN NULL ELSE NOW() END,
                    NOW()
             FROM generated",
        )
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query(
            "WITH generated AS (
                 SELECT g,
                        ('10000000-0000-7000-8000-' ||
                         lpad(to_hex(g), 12, '0'))::uuid AS workflow_id,
                        ('20000000-0000-7000-8000-' ||
                         lpad(to_hex(g), 12, '0'))::uuid AS node_id
                 FROM generate_series(1, 50000) AS g
             )
             INSERT INTO horsies_workflow_tasks (
                 downstream_subworkflow, id, workflow_id, task_index, node_id, task_name, input,
                 queue_name, priority, dependencies, allow_failed_deps,
                 join_type, status, is_subworkflow, max_retries, execution,
                 retention_class_key, created_at
             )
             SELECT 'NONE', node_id, workflow_id, 0, 'root', 'bounded_recovery_stable_task', 'null',
                    'default', 100, '{}'::integer[], FALSE,
                    'all',
                    CASE WHEN g % 4 = 0 THEN 'PENDING' ELSE 'COMPLETED' END,
                    FALSE, 0, 'ISOLATED', 'standard_30d', NOW()
             FROM generated",
        )
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query("ANALYZE horsies_workflows, horsies_workflow_tasks")
            .execute(&pool)
            .await
            .unwrap();

        let mut report = RecoveryReport::default();
        audit_end_states(
            &broker_of(&pool),
            &TestChildSpecs::empty(),
            200,
            &mut report,
            RecoveryFailurePolicy::LogOnly,
        )
        .await
        .unwrap();
        assert_eq!(report.metrics.case2_3.rows_selected, 200);
        assert_eq!(report.completion_candidate_yield, Some(0.0));
        assert_eq!(report.metrics.case2_3.candidates_returned, 0);
        assert_eq!(report.metrics.case4.rows_selected, 200);
        assert_eq!(report.metrics.case4.candidates_returned, 0);

        // The next page, with the plan of the page statement inside the function. The transaction rolls back,
        // so the cursor stays where the audit left it.
        let url = crate::test_support::migrated_database_url().await;
        let (client, notices) = crate::test_support::connect_collecting_notices(&url).await;
        client.batch_execute("BEGIN").await.expect("begin");
        client
            .batch_execute(AUTO_EXPLAIN_SETTINGS)
            .await
            .expect("send the nested plans as notices");
        let (completion_ids, plan) = audit_page_with_plan(&client, &notices).await;
        client.batch_execute("ROLLBACK").await.expect("rollback");
        assert!(completion_ids.is_empty(), "{completion_ids:?}");
        let rendered = plan.to_string();
        assert!(
            rendered.contains("idx_horsies_workflows_running_recovery_scan"),
            "the audit page must read the running-workflow scan index: {plan}",
        );
        assert!(
            WORKFLOW_TASK_INDEXES
                .iter()
                .any(|index| rendered.contains(index)),
            "the audit page must probe nodes through a workflow-task index: {plan}",
        );
        assert!(
            !plan_has_sequential_scan(&plan, "horsies_workflows")
                && !plan_has_sequential_scan(&plan, "horsies_workflow_tasks"),
            "the audit page must not scan complete workflow tables: {plan}",
        );
        assert!(
            plan_shared_buffers(&plan) <= 10_000,
            "the audit page must stay within its buffer budget: {plan}",
        );
        assert!(
            relation_rows_examined(&plan, "horsies_workflows") <= 201.0,
            "the audit page must examine no more than one page and its upper bound: {plan}",
        );
        assert!(
            relation_rows_examined(&plan, "horsies_workflow_tasks") <= 400.0,
            "the audit page must use at most two node probes per workflow: {plan}",
        );

        let selected_workflow = Uuid::parse_str("10000000-0000-7000-8000-000000000004").unwrap();
        for tree_sql in [
            TREE_CASE2_3_STUCK_WORKFLOW_SQL,
            TREE_CASE4_ORPHANED_WORKFLOW_SQL,
        ] {
            let explain = format!("EXPLAIN (FORMAT JSON) {tree_sql}");
            let tree_plan: serde_json::Value = sqlx::query_scalar(&explain)
                .bind(vec![selected_workflow])
                .bind(200_i64)
                .fetch_one(&pool)
                .await
                .unwrap();
            let rendered = tree_plan.to_string();
            assert!(
                rendered.contains("horsies_workflows_pkey")
                    || rendered.contains("idx_horsies_workflows_running_recovery_scan"),
                "workflow-tree recovery must use an exact workflow index: {tree_plan}",
            );
            assert!(
                rendered.contains("idx_horsies_workflow_tasks_workflow")
                    || rendered.contains("uq_horsies_workflow_task_index")
                    || rendered.contains("idx_horsies_workflow_tasks_wf_status_index")
                    || rendered.contains("idx_horsies_workflow_tasks_nonterminal"),
                "workflow-tree recovery must use workflow-task index probes: {tree_plan}",
            );
            assert!(
                !plan_has_sequential_scan(&tree_plan, "horsies_workflows")
                    && !plan_has_sequential_scan(&tree_plan, "horsies_workflow_tasks"),
                "workflow-tree recovery must not scan complete workflow tables: {tree_plan}",
            );
        }

        sqlx::query("DELETE FROM horsies_workflow_tasks")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("DELETE FROM horsies_workflows")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("ANALYZE horsies_workflows, horsies_workflow_tasks")
            .execute(&pool)
            .await
            .unwrap();
    }

    #[tokio::test]
    #[serial]
    async fn global_workflow_cursor_reaches_a_completion_after_a_stable_page() {
        let pool = crate::broker::terminalization_matrix::sqlx_migrated_pool().await;
        sqlx::query("DELETE FROM horsies_workflow_tasks")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("DELETE FROM horsies_workflows")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query(
            "UPDATE horsies_recovery_scan_cursors
             SET last_created_at = NULL, last_id = NULL,
                 cycle_upper_created_at = NULL, cycle_upper_id = NULL,
                 claim_token = NULL, claim_expires_at = NULL,
                 completed_cycles = 0,
                 last_scan_rows = 0, last_candidate_rows = 0,
                 last_scan_at = NULL
             WHERE scan_name = 'running_workflows'",
        )
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query(
            "WITH generated AS (
                 SELECT g,
                        ('60000000-0000-7000-8000-' ||
                         lpad(to_hex(g), 12, '0'))::uuid AS workflow_id
                 FROM generate_series(1, 201) AS g
             )
             INSERT INTO horsies_workflows (
                 id, name, status, on_error, output_task_index,
                 definition_key, depth, root_workflow_id,
                 sent_at, created_at, started_at, updated_at
             )
             SELECT workflow_id, 'bounded_recovery_cursor_' || g, 'RUNNING', 'fail', NULL,
                    'test.bounded-recovery.cursor.' || g, 0, workflow_id,
                    NOW(), NOW(), NOW(), NOW()
             FROM generated",
        )
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query(
            "WITH generated AS (
                 SELECT g,
                        ('60000000-0000-7000-8000-' ||
                         lpad(to_hex(g), 12, '0'))::uuid AS workflow_id,
                        ('61000000-0000-7000-8000-' ||
                         lpad(to_hex(g), 12, '0'))::uuid AS node_id
                 FROM generate_series(1, 201) AS g
             )
             INSERT INTO horsies_workflow_tasks (
                 downstream_subworkflow, id, workflow_id, task_index, node_id, task_name, input,
                 queue_name, priority, dependencies, allow_failed_deps,
                 join_type, status, is_subworkflow, max_retries, execution,
                 retention_class_key, completed_at, created_at
             )
             SELECT 'NONE', node_id, workflow_id, 0, 'root', 'bounded_recovery_cursor_task', 'null',
                    'default', 100, '{}'::integer[], FALSE, 'all',
                    CASE WHEN g = 201 THEN 'COMPLETED' ELSE 'PENDING' END,
                    FALSE, 0, 'ISOLATED', 'standard_30d',
                    CASE WHEN g = 201 THEN NOW() ELSE NULL END,
                    NOW()
             FROM generated",
        )
        .execute(&pool)
        .await
        .unwrap();

        let child_specs = TestChildSpecs::empty();
        let mut first = RecoveryReport::default();
        audit_end_states(
            &broker_of(&pool),
            &child_specs,
            200,
            &mut first,
            RecoveryFailurePolicy::LogOnly,
        )
        .await
        .unwrap();
        assert_eq!(first.metrics.case2_3.candidates_returned, 0);

        let mut second = RecoveryReport::default();
        audit_end_states(
            &broker_of(&pool),
            &child_specs,
            200,
            &mut second,
            RecoveryFailurePolicy::LogOnly,
        )
        .await
        .unwrap();
        assert_eq!(second.metrics.case2_3.candidates_returned, 1);
        assert_eq!(second.completion_candidate_yield, Some(1.0));
        let completed_id = Uuid::parse_str("60000000-0000-7000-8000-0000000000c9").unwrap();
        let status: String =
            sqlx::query_scalar("SELECT status FROM horsies_workflows WHERE id = $1")
                .bind(completed_id)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(status, "COMPLETED");

        sqlx::query("DELETE FROM horsies_workflow_tasks")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("DELETE FROM horsies_workflows")
            .execute(&pool)
            .await
            .unwrap();
    }

    #[tokio::test]
    #[serial]
    async fn global_cycle_watermark_revisits_old_rows_while_new_rows_arrive() {
        let pool = crate::broker::terminalization_matrix::sqlx_migrated_pool().await;
        sqlx::query("DELETE FROM horsies_workflow_tasks")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("DELETE FROM horsies_workflows")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query(
            "UPDATE horsies_recovery_scan_cursors
             SET last_created_at = NULL, last_id = NULL,
                 cycle_upper_created_at = NULL, cycle_upper_id = NULL,
                 claim_token = NULL, claim_expires_at = NULL,
                 completed_cycles = 0, last_scan_rows = 0,
                 last_candidate_rows = 0, last_scan_at = NULL
             WHERE scan_name = 'running_workflows'",
        )
        .execute(&pool)
        .await
        .unwrap();

        let base = chrono::Utc::now() - chrono::Duration::hours(1);
        let stable_ids = [Uuid::new_v4(), Uuid::new_v4(), Uuid::new_v4()];
        for (offset, workflow_id) in stable_ids.into_iter().enumerate() {
            insert_running_workflow_with_pending_task(
                &pool,
                workflow_id,
                base + chrono::Duration::seconds(i64::try_from(offset).unwrap()),
            )
            .await;
        }

        let child_specs = TestChildSpecs::empty();
        let mut first = RecoveryReport::default();
        audit_end_states(
            &broker_of(&pool),
            &child_specs,
            2,
            &mut first,
            RecoveryFailurePolicy::LogOnly,
        )
        .await
        .unwrap();
        assert_eq!(first.metrics.case2_3.rows_selected, 2);

        sqlx::query(
            "UPDATE horsies_workflow_tasks
             SET status = 'COMPLETED', completed_at = NOW()
             WHERE workflow_id = $1",
        )
        .bind(stable_ids[0])
        .execute(&pool)
        .await
        .unwrap();
        for offset in 0..2_i64 {
            insert_running_workflow_with_pending_task(
                &pool,
                Uuid::new_v4(),
                base + chrono::Duration::minutes(10) + chrono::Duration::seconds(offset),
            )
            .await;
        }

        let mut second = RecoveryReport::default();
        audit_end_states(
            &broker_of(&pool),
            &child_specs,
            2,
            &mut second,
            RecoveryFailurePolicy::LogOnly,
        )
        .await
        .unwrap();
        assert_eq!(second.metrics.case2_3.rows_selected, 1);
        assert_eq!(second.case2_3_workflow_completed, 0);

        for offset in 0..2_i64 {
            insert_running_workflow_with_pending_task(
                &pool,
                Uuid::new_v4(),
                base + chrono::Duration::minutes(20) + chrono::Duration::seconds(offset),
            )
            .await;
        }
        let mut third = RecoveryReport::default();
        audit_end_states(
            &broker_of(&pool),
            &child_specs,
            2,
            &mut third,
            RecoveryFailurePolicy::LogOnly,
        )
        .await
        .unwrap();
        assert_eq!(third.case2_3_workflow_completed, 1);
        let status: String =
            sqlx::query_scalar("SELECT status FROM horsies_workflows WHERE id = $1")
                .bind(stable_ids[0])
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(status, "COMPLETED");

        sqlx::query("DELETE FROM horsies_workflow_tasks")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("DELETE FROM horsies_workflows")
            .execute(&pool)
            .await
            .unwrap();
    }

    #[tokio::test]
    #[serial]
    async fn global_workflow_claim_refuses_a_second_caller_until_the_owner_expires() {
        let pool = crate::broker::terminalization_matrix::sqlx_migrated_pool().await;
        sqlx::query("DELETE FROM horsies_workflow_tasks")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("DELETE FROM horsies_workflows")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query(
            "UPDATE horsies_recovery_scan_cursors
             SET last_created_at = NULL, last_id = NULL,
                 cycle_upper_created_at = NULL, cycle_upper_id = NULL,
                 claim_token = NULL, claim_expires_at = NULL,
                 completed_cycles = 0,
                 last_scan_rows = 0, last_candidate_rows = 0,
                 last_scan_at = NULL
             WHERE scan_name = 'running_workflows'",
        )
        .execute(&pool)
        .await
        .unwrap();

        let workflow_id = Uuid::new_v4();
        sqlx::query(
            "INSERT INTO horsies_workflows (
                 id, name, status, on_error, output_task_index,
                 definition_key, depth, root_workflow_id,
                 sent_at, created_at, started_at, updated_at
             ) VALUES (
                 $1, 'bounded_recovery_retry', 'RUNNING', 'fail', NULL,
                 $2, 0, $1, NOW(), NOW(), NOW(), NOW()
             )",
        )
        .bind(workflow_id)
        .bind(format!("test.bounded-recovery.retry.{workflow_id}"))
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO horsies_workflow_tasks (
                downstream_subworkflow, id, workflow_id, task_index, node_id, task_name, input, queue_name, priority,
                dependencies, allow_failed_deps, join_type, status, is_subworkflow,
                completed_at, created_at, max_retries, execution, retention_class_key
            ) VALUES (
                'NONE', $1, $2, 0, 'root', 'bounded_recovery_retry_task', 'null', 'default', 100,
                '{}'::integer[], FALSE, 'all', 'COMPLETED', FALSE, NOW(), NOW(), 0, 'ISOLATED',
                'standard_30d'
            )",
        )
        .bind(Uuid::new_v4())
        .bind(workflow_id)
        .execute(&pool)
        .await
        .unwrap();

        let abandoned = sqlx::query_as::<_, GlobalWorkflowAuditRow>(GLOBAL_WORKFLOW_AUDIT_SQL)
            .bind(200_i64)
            .bind(Uuid::new_v4())
            .bind(GLOBAL_WORKFLOW_AUDIT_CLAIM_TTL_MS)
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(abandoned.completion_ids, vec![workflow_id]);
        assert!(abandoned.claim_token.is_some());
        let status: String =
            sqlx::query_scalar("SELECT status FROM horsies_workflows WHERE id = $1")
                .bind(workflow_id)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(status, "RUNNING");

        let mut report = RecoveryReport::default();
        audit_end_states(
            &broker_of(&pool),
            &TestChildSpecs::empty(),
            200,
            &mut report,
            RecoveryFailurePolicy::LogOnly,
        )
        .await
        .unwrap();
        assert_eq!(report.metrics.case2_3.refusals, 1);
        assert_eq!(report.case2_3_workflow_completed, 0);

        sqlx::query(
            "UPDATE horsies_recovery_scan_cursors
             SET claim_expires_at = statement_timestamp() - interval '1 second'
             WHERE scan_name = 'running_workflows'",
        )
        .execute(&pool)
        .await
        .unwrap();
        let mut report = RecoveryReport::default();
        audit_end_states(
            &broker_of(&pool),
            &TestChildSpecs::empty(),
            200,
            &mut report,
            RecoveryFailurePolicy::LogOnly,
        )
        .await
        .unwrap();
        assert_eq!(report.case2_3_workflow_completed, 1);
        let status: String =
            sqlx::query_scalar("SELECT status FROM horsies_workflows WHERE id = $1")
                .bind(workflow_id)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(status, "COMPLETED");

        sqlx::query("DELETE FROM horsies_workflow_tasks WHERE workflow_id = $1")
            .bind(workflow_id)
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("DELETE FROM horsies_workflows WHERE id = $1")
            .bind(workflow_id)
            .execute(&pool)
            .await
            .unwrap();
    }

    #[tokio::test]
    #[serial]
    async fn workflow_action_fences_the_claim_after_lease_expiry() {
        let pool = crate::broker::terminalization_matrix::sqlx_migrated_pool().await;
        sqlx::query("DELETE FROM horsies_workflow_tasks")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("DELETE FROM horsies_workflows")
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query(
            "UPDATE horsies_recovery_scan_cursors
             SET last_created_at = NULL, last_id = NULL,
                 cycle_upper_created_at = NULL, cycle_upper_id = NULL,
                 claim_token = NULL, claim_expires_at = NULL,
                 completed_cycles = 0, last_scan_rows = 0,
                 last_candidate_rows = 0, last_scan_at = NULL
             WHERE scan_name = 'running_workflows'",
        )
        .execute(&pool)
        .await
        .unwrap();

        let workflow_id = Uuid::new_v4();
        sqlx::query(
            "INSERT INTO horsies_workflows (
                 id, name, status, on_error, output_task_index,
                 definition_key, depth, root_workflow_id,
                 sent_at, created_at, started_at, updated_at
             ) VALUES (
                 $1, 'bounded_recovery_fenced', 'RUNNING', 'fail', NULL,
                 $2, 0, $1, NOW(), NOW(), NOW(), NOW()
             )",
        )
        .bind(workflow_id)
        .bind(format!("test.bounded-recovery.fenced.{workflow_id}"))
        .execute(&pool)
        .await
        .unwrap();
        sqlx::query(
            "INSERT INTO horsies_workflow_tasks (
                downstream_subworkflow, id, workflow_id, task_index, node_id, task_name, input, queue_name, priority,
                dependencies, allow_failed_deps, join_type, status, is_subworkflow,
                completed_at, created_at, max_retries, execution, retention_class_key
            ) VALUES (
                'NONE', $1, $2, 0, 'root', 'bounded_recovery_fenced_task', 'null', 'default', 100,
                '{}'::integer[], FALSE, 'all', 'COMPLETED', FALSE, NOW(), NOW(), 0, 'ISOLATED',
                'standard_30d'
            )",
        )
        .bind(Uuid::new_v4())
        .bind(workflow_id)
        .execute(&pool)
        .await
        .unwrap();

        let audit = sqlx::query_as::<_, GlobalWorkflowAuditRow>(GLOBAL_WORKFLOW_AUDIT_SQL)
            .bind(200_i64)
            .bind(Uuid::new_v4())
            .bind(500_i64)
            .fetch_one(&pool)
            .await
            .unwrap();
        let claim_token = audit.claim_token.expect("candidate page claim");

        let mut workflow_lock = pool.begin().await.unwrap();
        sqlx::query("SELECT id FROM horsies_workflows WHERE id = $1 FOR UPDATE")
            .bind(workflow_id)
            .execute(workflow_lock.as_mut())
            .await
            .unwrap();
        let action_broker = broker_of(&pool);
        let action = tokio::spawn(async move {
            let rows = run_alone(
                &action_broker,
                CheckoutClass::Reserved,
                &RECOVER_COMPLETION
                    .bind()
                    .value(&workflow_id)
                    .value(&claim_token)
                    .value(&GLOBAL_WORKFLOW_AUDIT_CLAIM_TTL_MS)
                    .finish(),
            )
            .await
            .expect("the completion statement runs");
            audit_action(rows, &RECOVER_COMPLETION)
        });

        let mut action_is_waiting = false;
        for _ in 0..100 {
            action_is_waiting = sqlx::query_scalar(
                "SELECT EXISTS (
                     SELECT 1 FROM pg_stat_activity
                     WHERE datname = current_database()
                       AND pid <> pg_backend_pid()
                       AND wait_event_type = 'Lock'
                       AND query LIKE '%horsies_workflow_recover_completion%'
                 )",
            )
            .fetch_one(&pool)
            .await
            .unwrap();
            if action_is_waiting {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        assert!(
            action_is_waiting,
            "candidate action must reach the workflow lock"
        );
        tokio::time::sleep(std::time::Duration::from_millis(550)).await;
        let lease_expired: bool = sqlx::query_scalar(
            "SELECT claim_expires_at <= statement_timestamp()
             FROM horsies_recovery_scan_cursors
             WHERE scan_name = 'running_workflows'",
        )
        .fetch_one(&pool)
        .await
        .unwrap();
        assert!(lease_expired);

        let mut second = RecoveryReport::default();
        audit_end_states(
            &broker_of(&pool),
            &TestChildSpecs::empty(),
            200,
            &mut second,
            RecoveryFailurePolicy::LogOnly,
        )
        .await
        .unwrap();
        assert_eq!(second.metrics.case2_3.refusals, 1);
        assert_eq!(second.case2_3_workflow_completed, 0);

        workflow_lock.rollback().await.unwrap();
        // The action renewed the claim before its lease expired and holds it: it completes the workflow.
        assert!(matches!(
            action.await.unwrap().unwrap(),
            AuditAction::Written { .. }
        ));
        let status: String =
            sqlx::query_scalar("SELECT status FROM horsies_workflows WHERE id = $1")
                .bind(workflow_id)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(status, "COMPLETED");

        sqlx::query("DELETE FROM horsies_workflow_tasks WHERE workflow_id = $1")
            .bind(workflow_id)
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("DELETE FROM horsies_workflows WHERE id = $1")
            .bind(workflow_id)
            .execute(&pool)
            .await
            .unwrap();
    }

    /// Python `tests/integration/test_bounded_recovery.py::test_stale_page_owner_cannot_mutate_after_claim_takeover`:
    /// the owner of an audit page whose claim expired and was taken over by
    /// a later audit cannot fail the orphaned workflow of that page, and the
    /// workflow stays RUNNING. The Python candidate answers `CLAIM_LOST`; so
    /// does the core's fenced write (`AUDIT_CLAIM_LOST`).
    #[tokio::test]
    #[serial]
    async fn stale_page_owner_cannot_mutate_after_claim_takeover() {
        let pool = crate::broker::terminalization_matrix::sqlx_migrated_pool().await;
        for statement in [
            "DELETE FROM horsies_workflow_tasks",
            "DELETE FROM horsies_workflows",
            "UPDATE horsies_recovery_scan_cursors
             SET last_created_at = NULL, last_id = NULL,
                 cycle_upper_created_at = NULL, cycle_upper_id = NULL,
                 claim_token = NULL, claim_expires_at = NULL,
                 completed_cycles = 0, last_scan_rows = 0,
                 last_candidate_rows = 0, last_scan_at = NULL
             WHERE scan_name = 'running_workflows'",
        ] {
            sqlx::query(statement)
                .execute(&pool)
                .await
                .expect("reset the running-workflows scan");
        }
        let workflow_id = Uuid::new_v4();
        sqlx::query(
            "INSERT INTO horsies_workflows (
                 id, name, status, on_error, output_task_index,
                 definition_key, depth, root_workflow_id,
                 sent_at, created_at, started_at, updated_at
             ) VALUES (
                 $1, 'stale_owner_orphan', 'RUNNING', 'fail', NULL,
                 NULL, 0, $1, NOW(), NOW(), NOW(), NOW()
             )",
        )
        .bind(workflow_id)
        .execute(&pool)
        .await
        .expect("seed the orphaned workflow");

        let stale_token = Uuid::new_v4();
        let first = sqlx::query_as::<_, GlobalWorkflowAuditRow>(GLOBAL_WORKFLOW_AUDIT_SQL)
            .bind(200_i64)
            .bind(stale_token)
            .bind(60_000_i64)
            .fetch_one(&pool)
            .await
            .expect("first audit page");
        assert_eq!(first.orphan_ids, vec![workflow_id]);
        assert_eq!(first.claim_token, Some(stale_token));

        sqlx::query(
            "UPDATE horsies_recovery_scan_cursors
             SET claim_expires_at = statement_timestamp() - interval '1 second'
             WHERE scan_name = 'running_workflows'
               AND claim_token = $1",
        )
        .bind(stale_token)
        .execute(&pool)
        .await
        .expect("expire the first claim");
        let replacement_token = Uuid::new_v4();
        let second = sqlx::query_as::<_, GlobalWorkflowAuditRow>(GLOBAL_WORKFLOW_AUDIT_SQL)
            .bind(200_i64)
            .bind(replacement_token)
            .bind(60_000_i64)
            .fetch_one(&pool)
            .await
            .expect("takeover audit page");
        assert_eq!(second.orphan_ids, vec![workflow_id]);
        assert_eq!(second.claim_token, Some(replacement_token));

        let broker = broker_of(&pool);
        let error = orphaned_workflow_error(workflow_id, "stale_owner_orphan").unwrap();
        let rows = run_alone(
            &broker,
            CheckoutClass::Reserved,
            &FAIL_ORPHANED
                .bind()
                .value(&workflow_id)
                .value(&stale_token)
                .value(&60_000_i64)
                .value(&error)
                .finish(),
        )
        .await
        .expect("the fenced write answers");

        assert_eq!(
            audit_action(rows, &FAIL_ORPHANED).expect("one outcome row"),
            AuditAction::ClaimLost,
            "the stale page owner failed the workflow"
        );
        let status: String =
            sqlx::query_scalar("SELECT status FROM horsies_workflows WHERE id = $1")
                .bind(workflow_id)
                .fetch_one(&pool)
                .await
                .expect("read the workflow status");
        assert_eq!(status, "RUNNING");

        let _released = run_alone(
            &broker,
            CheckoutClass::Reserved,
            &RELEASE_AUDIT_CLAIM
                .bind()
                .value(&replacement_token)
                .finish(),
        )
        .await
        .expect("release the takeover claim");
        sqlx::query("DELETE FROM horsies_workflows WHERE id = $1")
            .bind(workflow_id)
            .execute(&pool)
            .await
            .expect("clean up the workflow");
    }

    #[tokio::test]
    #[serial]
    async fn busy_global_workflow_cursor_refuses_without_waiting() {
        let pool = crate::broker::terminalization_matrix::sqlx_migrated_pool().await;
        let mut holder = pool.begin().await.unwrap();
        sqlx::query(
            "SELECT last_id FROM horsies_recovery_scan_cursors
             WHERE scan_name = 'running_workflows' FOR UPDATE",
        )
        .execute(holder.as_mut())
        .await
        .unwrap();

        let mut report = RecoveryReport::default();
        tokio::time::timeout(
            std::time::Duration::from_secs(1),
            audit_end_states(
                &broker_of(&pool),
                &TestChildSpecs::empty(),
                200,
                &mut report,
                RecoveryFailurePolicy::LogOnly,
            ),
        )
        .await
        .expect("busy cursor must not wait")
        .unwrap();
        assert_eq!(report.metrics.case2_3.refusals, 1);
        assert_eq!(report.metrics.case4.refusals, 1);
        holder.rollback().await.unwrap();
    }

    #[tokio::test]
    #[serial]
    async fn workflow_tree_recovery_does_not_mutate_an_unrelated_orphan() {
        let pool = crate::broker::terminalization_matrix::sqlx_migrated_pool().await;
        let selected = Uuid::new_v4();
        let unrelated = Uuid::new_v4();
        insert_orphaned_workflow(&pool, selected).await;
        insert_orphaned_workflow(&pool, unrelated).await;

        let report = recover_workflow_tree(&broker_of(&pool), &TestChildSpecs::empty(), selected)
            .await
            .unwrap();
        assert_eq!(report.case4_orphaned_failed, 1);
        let statuses: Vec<(Uuid, String)> = sqlx::query_as(
            "SELECT id, status FROM horsies_workflows
             WHERE id = ANY($1) ORDER BY id",
        )
        .bind(vec![selected, unrelated])
        .fetch_all(&pool)
        .await
        .unwrap();
        assert!(statuses
            .iter()
            .any(|row| row.0 == selected && row.1 == "FAILED"));
        assert!(statuses
            .iter()
            .any(|row| row.0 == unrelated && row.1 == "RUNNING"));
        sqlx::query("DELETE FROM horsies_workflows WHERE id = ANY($1)")
            .bind(vec![selected, unrelated])
            .execute(&pool)
            .await
            .unwrap();
    }

    /// Recovery case 1 fails a READY task node whose `args_from` names a member of its static input,
    /// with Python's conflict error and no task row (`core/workflows/engine.py:834-839`). The node left
    /// READY, so the step is `APPLIED` (`horsies_workflow_unbuildable_nodes`).
    #[tokio::test]
    #[serial]
    async fn case_1_fails_a_ready_node_whose_args_from_names_a_static_member() {
        let pool = crate::broker::enqueue_history_tests::sqlx_migrated_pool().await;
        let workflow_id = Uuid::new_v4();
        sqlx::query(
            "INSERT INTO horsies_workflows (
                id, name, status, on_error, output_task_index,
                definition_key, depth, root_workflow_id,
                sent_at, created_at, started_at, updated_at
             ) VALUES (
                $1, 'conflict_recovery_wf', 'RUNNING', 'fail', NULL,
                'conflict.recovery.v1', 0, $1, NOW(), NOW(), NOW(), NOW()
             )",
        )
        .bind(workflow_id)
        .execute(&pool)
        .await
        .expect("insert recovery workflow");
        for (index, status, dependencies, input, args_from) in [
            (0, "COMPLETED", "{}", "null", None),
            (1, "READY", "{0}", r#"{"x": 5}"#, Some(r#"{"x": 0}"#)),
        ] {
            sqlx::query(
                "INSERT INTO horsies_workflow_tasks (
                    downstream_subworkflow, id, workflow_id, task_index, node_id, task_name, input, queue_name, priority,
                    dependencies, args_from, allow_failed_deps, join_type, status, is_subworkflow,
                    created_at, max_retries, execution, retention_class_key
                ) VALUES (
                    'NONE', $1, $2, $3, $4, 'conflict_task', $5, 'default', 100, $6::int[], $7::jsonb,
                    FALSE, 'all', $8, FALSE, NOW(), 0, 'ISOLATED', 'standard_30d'
                )",
            )
            .bind(Uuid::new_v4())
            .bind(workflow_id)
            .bind(index)
            .bind(format!("node_{index}"))
            .bind(input)
            .bind(dependencies)
            .bind(args_from)
            .bind(status)
            .execute(&pool)
            .await
            .expect("insert node");
        }
        let step = enqueue_ready_node(&broker_of(&pool), workflow_id, 1)
            .await
            .expect("recovery enqueue path");
        assert_eq!(step.disposition, EnqueueReadyDisposition::Applied);

        let (status, task_id, result): (String, Option<Uuid>, Option<String>) = sqlx::query_as(
            "SELECT status, task_id, result FROM horsies_workflow_tasks
             WHERE workflow_id = $1 AND task_index = 1",
        )
        .bind(workflow_id)
        .fetch_one(&pool)
        .await
        .expect("read the node");
        assert_eq!(status, "FAILED");
        assert!(task_id.is_none());
        let stored: crate::core::TaskResult<serde_json::Value> =
            serde_json::from_str(&result.expect("the failed node stores its error"))
                .expect("stored node result");
        let crate::core::TaskResult::Err(error) = stored else {
            panic!("the failed node must store an error");
        };
        assert_eq!(error.error_code.to_string(), "WORKFLOW_ENQUEUE_FAILED");
        assert_eq!(
            error.message.as_deref(),
            Some("args_from key 'x' conflicts with static kwarg (task 1)")
        );

        sqlx::query("DELETE FROM horsies_workflows WHERE id = $1")
            .bind(workflow_id)
            .execute(&pool)
            .await
            .ok();
    }

    /// One RUNNING workflow (`on_error = fail`) with one READY task node whose stored options are `options`.
    /// Returns the workflow id.
    async fn seed_ready_node(pool: &PgPool, task_name: &str, options: &str) -> Uuid {
        let workflow_id = Uuid::new_v4();
        sqlx::query(
            "INSERT INTO horsies_workflows (
                id, name, status, on_error, output_task_index,
                definition_key, depth, root_workflow_id,
                sent_at, created_at, started_at, updated_at
             ) VALUES (
                $1::uuid, 'unprepared_ready_wf', 'RUNNING', 'fail', NULL,
                'unprepared.ready.v1', 0, $1::uuid, NOW(), NOW(), NOW(), NOW()
             )",
        )
        .bind(workflow_id)
        .execute(pool)
        .await
        .expect("insert the workflow");
        sqlx::query(
            "INSERT INTO horsies_workflow_tasks (
                downstream_subworkflow, id, workflow_id, task_index, node_id, task_name, input,
                queue_name, priority, dependencies, allow_failed_deps, join_type, status,
                is_subworkflow, created_at, max_retries, execution, retention_class_key,
                task_options
            ) VALUES (
                'NONE', $1::uuid, $2::uuid, 0, 'unprepared', $3, '{}', 'default', 17,
                '{}', FALSE, 'all', 'READY', FALSE, NOW(), 0, 'ISOLATED', 'forever', $4
            )",
        )
        .bind(Uuid::new_v4())
        .bind(workflow_id)
        .bind(task_name)
        .bind(options)
        .execute(pool)
        .await
        .expect("insert the READY node");
        workflow_id
    }

    /// Recovery case 1 enqueues a READY node with the reader of a task row. A stored retry policy that does
    /// not read, or stored options that are not a JSON object, fail that node alone with
    /// `WORKER_SERIALIZATION_ERROR` with Python's message (`horsies_workflow_task_options_refusal`), the node
    /// index and the workflow id in `data`, and no task row (Python's case 1 through `enqueue_workflow_task`,
    /// `python/horsies/core/workflows/engine.py:634-705`, `:798-818`); the
    /// workflow, with `on_error = fail` and no other node, ends FAILED. A reader that guesses a limit
    /// enqueues the node instead.
    #[tokio::test]
    #[serial]
    async fn a_ready_node_that_cannot_be_prepared_fails_alone() {
        let pool = crate::broker::enqueue_history_tests::sqlx_migrated_pool().await;
        for (task_name, options, refusal) in [
            (
                "recovery_unprepared_retry_policy_node",
                r#"{"retry_policy": {"backoff_strategy": "fixed"}}"#,
                "Invalid retry_policy for task 0: ",
            ),
            (
                "recovery_unprepared_options_node",
                "[]",
                "Invalid task_options payload for task 0: expected object",
            ),
        ] {
            let workflow_id = seed_ready_node(&pool, task_name, options).await;
            let step = enqueue_ready_node(&broker_of(&pool), workflow_id, 0)
                .await
                .expect("the node failure is applied");
            assert_eq!(
                step.disposition,
                EnqueueReadyDisposition::Applied,
                "{task_name}"
            );
            let (status, task_id, result): (String, Option<Uuid>, Option<String>) = sqlx::query_as(
                "SELECT status, task_id, result FROM horsies_workflow_tasks
                 WHERE workflow_id = $1 AND task_index = 0",
            )
            .bind(workflow_id)
            .fetch_one(&pool)
            .await
            .unwrap();
            assert_eq!(status, "FAILED", "{task_name}");
            assert_eq!(task_id, None, "{task_name}");
            let stored: crate::core::TaskResult<serde_json::Value> =
                serde_json::from_str(&result.expect("the node stores its failure")).unwrap();
            let crate::core::TaskResult::Err(error) = stored else {
                panic!("{task_name}: the node stores an error");
            };
            assert_eq!(
                error.error_code.to_string(),
                "WORKER_SERIALIZATION_ERROR",
                "{task_name}"
            );
            assert!(
                error
                    .message
                    .as_deref()
                    .is_some_and(|message| message.starts_with(refusal)),
                "{task_name}: {error:?}"
            );
            assert_eq!(
                error.data_value(),
                Some(serde_json::json!({
                    "workflow_id": workflow_id.to_string(),
                    "task_index": 0,
                })),
                "{task_name}"
            );
            let task_rows: i64 =
                sqlx::query_scalar("SELECT count(*) FROM horsies_tasks WHERE task_name = $1")
                    .bind(task_name)
                    .fetch_one(&pool)
                    .await
                    .unwrap();
            assert_eq!(task_rows, 0, "{task_name}");
            let workflow_status: String =
                sqlx::query_scalar("SELECT status FROM horsies_workflows WHERE id = $1")
                    .bind(workflow_id)
                    .fetch_one(&pool)
                    .await
                    .unwrap();
            assert_eq!(workflow_status, "FAILED", "{task_name}");
            sqlx::query("DELETE FROM horsies_workflows WHERE id = $1")
                .bind(workflow_id)
                .execute(&pool)
                .await
                .unwrap();
        }
    }

    /// A stored `good_until` that the database cannot read is not a refusal of the node: the task insert
    /// casts the text and raises (22007), the transaction rolls back, and the node stays READY with no task
    /// while the workflow stays RUNNING. The error is persistent, so the recovery pass counts it toward the
    /// failure limit (Python `_build_enqueued_task_params` passes `str()` of the member to its insert,
    /// `python/horsies/core/workflows/engine.py:831-835`).
    #[tokio::test]
    #[serial]
    async fn a_stored_deadline_that_does_not_read_raises_and_keeps_the_ready_node() {
        use crate::workflow_engine::recovery_limit::{CandidateFailure, FailureClass};

        let pool = crate::broker::enqueue_history_tests::sqlx_migrated_pool().await;
        let task_name = "recovery_unreadable_deadline_node";
        for options in [
            r#"{"good_until": "not-a-date"}"#,
            r#"{"good_until": 12345}"#,
        ] {
            let workflow_id = seed_ready_node(&pool, task_name, options).await;
            let error = enqueue_ready_node(&broker_of(&pool), workflow_id, 0)
                .await
                .expect_err("the task insert raises");
            assert!(
                matches!(error, WorkflowError::Broker(_)),
                "{options}: expected the database refusal, got {error:?}"
            );
            assert_eq!(
                CandidateFailure::Error(error).class(),
                FailureClass::Persistent,
                "{options}"
            );
            let (status, task_id): (String, Option<Uuid>) = sqlx::query_as(
                "SELECT status, task_id FROM horsies_workflow_tasks
                 WHERE workflow_id = $1 AND task_index = 0",
            )
            .bind(workflow_id)
            .fetch_one(&pool)
            .await
            .unwrap();
            assert_eq!(status, "READY", "{options}");
            assert_eq!(task_id, None, "{options}");
            let task_rows: i64 =
                sqlx::query_scalar("SELECT count(*) FROM horsies_tasks WHERE task_name = $1")
                    .bind(task_name)
                    .fetch_one(&pool)
                    .await
                    .unwrap();
            assert_eq!(task_rows, 0, "{options}");
            let workflow_status: String =
                sqlx::query_scalar("SELECT status FROM horsies_workflows WHERE id = $1")
                    .bind(workflow_id)
                    .fetch_one(&pool)
                    .await
                    .unwrap();
            assert_eq!(workflow_status, "RUNNING", "{options}");
            sqlx::query("DELETE FROM horsies_workflows WHERE id = $1")
                .bind(workflow_id)
                .execute(&pool)
                .await
                .unwrap();
        }
    }

    /// Recovery case 0 marks a PENDING node READY with its task row, in one transaction: a task insert that
    /// raises (a stored `good_until` that the database cannot read) rolls the READY mark back, and the node
    /// stays PENDING with no task. Python runs each case 0 candidate in a savepoint
    /// (`python/horsies/core/workflows/recovery.py:93-94`, `:712-760`). A READY node left behind would be selected
    /// again by case 1 in the same pass.
    #[tokio::test]
    #[serial]
    async fn a_case_0_node_whose_task_insert_raises_stays_pending() {
        let pool = crate::broker::enqueue_history_tests::sqlx_migrated_pool().await;
        let task_name = "recovery_case0_raise_node";
        let workflow_id = Uuid::new_v4();
        sqlx::query(
            "INSERT INTO horsies_workflows (
                id, name, status, on_error, output_task_index,
                definition_key, depth, root_workflow_id,
                sent_at, created_at, started_at, updated_at
             ) VALUES (
                $1::uuid, 'case0_raise_wf', 'RUNNING', 'fail', NULL,
                'case0.raise.v1', 0, $1::uuid, NOW(), NOW(), NOW(), NOW()
             )",
        )
        .bind(workflow_id)
        .execute(&pool)
        .await
        .expect("insert the workflow");
        for (task_index, status, dependencies, options) in [
            (0_i32, "COMPLETED", Vec::<i32>::new(), None),
            (
                1_i32,
                "PENDING",
                vec![0_i32],
                Some(r#"{"good_until": "not-a-date"}"#),
            ),
        ] {
            sqlx::query(
                "INSERT INTO horsies_workflow_tasks (
                    downstream_subworkflow, id, workflow_id, task_index, node_id, task_name, input,
                    queue_name, priority, dependencies, allow_failed_deps, join_type, status,
                    is_subworkflow, created_at, max_retries, execution, retention_class_key,
                    task_options
                ) VALUES (
                    'NONE', $1::uuid, $2::uuid, $3, $4, $5, '{}', 'default', 17,
                    $6, FALSE, 'all', $7, FALSE, NOW(), 0, 'ISOLATED', 'forever', $8
                )",
            )
            .bind(Uuid::new_v4())
            .bind(workflow_id)
            .bind(task_index)
            .bind(format!("node_{task_index}"))
            .bind(task_name)
            .bind(dependencies)
            .bind(status)
            .bind(options)
            .execute(&pool)
            .await
            .expect("insert a node");
        }

        let error = reevaluate_node(&broker_of(&pool), workflow_id, 1)
            .await
            .expect_err("the task insert raises");
        assert!(matches!(error, WorkflowError::Broker(_)), "{error:?}");
        let (status, task_id): (String, Option<Uuid>) = sqlx::query_as(
            "SELECT status, task_id FROM horsies_workflow_tasks
             WHERE workflow_id = $1 AND task_index = 1",
        )
        .bind(workflow_id)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(status, "PENDING");
        assert_eq!(task_id, None);
        let task_rows: i64 =
            sqlx::query_scalar("SELECT count(*) FROM horsies_tasks WHERE task_name = $1")
                .bind(task_name)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(task_rows, 0);
        sqlx::query("DELETE FROM horsies_workflows WHERE id = $1")
            .bind(workflow_id)
            .execute(&pool)
            .await
            .unwrap();
    }

    #[tokio::test]
    #[serial]
    async fn p6_workflow_enqueue_recovery_persists_never_eligible_facts() {
        let pool = crate::broker::enqueue_history_tests::sqlx_migrated_pool().await;
        let workflow_id = Uuid::new_v4();
        sqlx::query(
            "INSERT INTO horsies_workflows (
                id, name, status, on_error, output_task_index,
                definition_key, depth, root_workflow_id,
                sent_at, created_at, started_at, updated_at
             ) VALUES (
                $1::uuid, 'p6_recovery_wf', 'RUNNING', 'fail', NULL,
                'p6.recovery.v1', 0, $1::uuid, timestamptz '2026-01-01 00:00:00+00',
                NOW(), NOW(), NOW()
             )",
        )
        .bind(&workflow_id)
        .execute(&pool)
        .await
        .expect("insert recovery workflow");
        sqlx::query(
            "INSERT INTO horsies_workflow_tasks (
                downstream_subworkflow, id, workflow_id, task_index, node_id, task_name, input, queue_name, priority,
                dependencies, allow_failed_deps, join_type, status, is_subworkflow, created_at,
                max_retries, execution, retention_class_key
            ) VALUES (
                'NONE', $1::uuid, $2::uuid, 0, 'p6_recovery', 'p6_recovery_task', '{}', 'bulk', 17,
                '{}', FALSE, 'all', 'READY', FALSE, NOW(), 0, 'ISOLATED', 'standard_7d'
            )",
        )
        .bind(Uuid::new_v4())
        .bind(&workflow_id)
        .execute(&pool)
        .await
        .expect("insert ready recovery node");
        // The node stores the retention class that the start derived from its queue (`standard_7d` for a
        // queue kept 7 days); the task row copies it.
        let step = enqueue_ready_node(&broker_of(&pool), workflow_id, 0)
            .await
            .expect("recovery enqueue path");
        assert_eq!(step.disposition, EnqueueReadyDisposition::Applied);
        // The node holds its enqueue time: the node write and the task insert share a transaction.
        let node_holds_enqueue_time: bool = sqlx::query_scalar(
            "SELECT wt.started_at IS NOT NULL AND wt.started_at = t.enqueued_at
             FROM horsies_workflow_tasks wt
             JOIN horsies_tasks t ON t.id = wt.task_id
             WHERE wt.workflow_id = $1::uuid AND wt.task_index = 0",
        )
        .bind(&workflow_id)
        .fetch_one(&pool)
        .await
        .expect("read the recovery node enqueue time");
        assert!(node_holds_enqueue_time);
        let facts: (String, Option<i32>, Option<i16>, bool) = sqlx::query_as(
            "SELECT t.retention_class_key, octet_length(t.command_fingerprint),
                    t.command_fingerprint_version, t.retain_rerun_input
             FROM horsies_tasks t
             JOIN horsies_workflow_tasks wt ON wt.task_id = t.id
             WHERE wt.workflow_id = $1::uuid AND wt.task_index = 0",
        )
        .bind(&workflow_id)
        .fetch_one(&pool)
        .await
        .expect("read recovery enqueue facts");
        assert_eq!(facts.0, "standard_7d");
        assert!(facts.1.is_none());
        assert!(facts.2.is_none());
        assert!(!facts.3);
        // The task row stores the start call time of its workflow as `sent_at`.
        let sent_at_is_the_start_call_time: bool = sqlx::query_scalar(
            "SELECT t.sent_at = timestamptz '2026-01-01 00:00:00+00'
             FROM horsies_tasks t
             JOIN horsies_workflow_tasks wt ON wt.task_id = t.id
             WHERE wt.workflow_id = $1::uuid AND wt.task_index = 0",
        )
        .bind(&workflow_id)
        .fetch_one(&pool)
        .await
        .expect("read recovery task sent_at");
        assert!(sent_at_is_the_start_call_time);

        sqlx::query(
            "DELETE FROM horsies_tasks WHERE id IN (
                SELECT task_id FROM horsies_workflow_tasks WHERE workflow_id = $1::uuid
             )",
        )
        .bind(&workflow_id)
        .execute(&pool)
        .await
        .ok();
        sqlx::query("DELETE FROM horsies_workflow_tasks WHERE workflow_id = $1::uuid")
            .bind(&workflow_id)
            .execute(&pool)
            .await
            .ok();
        sqlx::query("DELETE FROM horsies_workflows WHERE id = $1::uuid")
            .bind(&workflow_id)
            .execute(&pool)
            .await
            .ok();
    }
}

#[cfg(test)]
mod status_and_yield_tests;
