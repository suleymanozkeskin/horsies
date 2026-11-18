//! Seeds, databases and engine drivers that the workflow engine ports share.
//!
//! Every seed names every column of its baseline table
//! (`crates/horsies-core/baseline/0001_tables.sql`), and the two node columns that
//! #788 adds (`sub_workflow_params`, `downstream_subworkflow`). The node
//! `error` column is not named: the baseline drops it (decision "The node
//! `error` column", rule-following row 13).
//!
//! Times that the engine compares with the database clock are set with
//! `NOW()` in SQL, so the test clock never enters a comparison.

use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use chrono::{DateTime, Utc};
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::PgPool;
use uuid::Uuid;

use crate::broker::attempt_writes::{
    AttemptFailure, AttemptFailureKind, FinishFailedAttempt, RetryRequest,
};
use crate::broker::lifecycle_tests::support::{
    database_options, unpinged_single_connection_pool, WireProxy,
};
use crate::broker::workflow_start::{ClientStartError, ClientStartOutcome};
use crate::broker::PostgresBroker;
use crate::core::history::maintenance::coverage::{
    ensure_partition_coverage_in_pool, CoverageOutcome,
};
use crate::core::lifecycle::{OwnedClaim, TerminalizationOutcome};
use crate::core::workflow::context::WORKFLOW_CTX_KWARG;
use crate::core::workflow::handle_types::HandleResult;
use crate::core::workflow::node::{JoinType, SubWorkflowParams};
use crate::core::workflow::policy::{SuccessCase, SuccessPolicy};
use crate::core::{
    AnyNode, OnError, OutcomeCode, RetrievalCode, SubWorkflowSummary, TaskError, TaskResult,
    WorkflowSpec, WorkflowStatus, WorkflowTaskStatus,
};
use crate::db::CheckoutClass;
use crate::test_support::child_specs::TestChildSpecs;
use crate::test_support::sqlx_terminal_writes::{self as terminal_writes, TerminalFailure};
use crate::test_support::{server_url, with_database, TestDatabase};
use crate::worker::execution::{
    finalize_workflow_phase, persist_failed_attempt, persist_ok_result, FinalizeOutcome,
    PhaseTwoForm,
};
use crate::workflow_engine::engine::{
    on_workflow_task_complete_with, WorkflowTaskCompletion, WorkflowTaskCompletionError,
};
use crate::workflow_engine::error::WorkflowError;
use crate::workflow_engine::lifecycle::resume_client_workflow;
use crate::workflow_engine::query::{
    wait_workflow_end, NodeResultsRead, NodesRead, StoredNode, StoredNodeResult,
    TerminalErrorStatus, WorkflowEnd, WorkflowWait,
};
use crate::workflow_engine::recovery::{complete_in_tree, TreeCompletion};
use crate::workflow_engine::steps::{advance_parents, OwedParentAdvances, ParentAdvance};

/// The worker that holds every task a test runs.
pub(super) const WORKER: &str = "workflow-port-worker";

/// The queue of the task nodes that the ports start. No worker test claims
/// from it, so a live task that a test leaves on the shared database stays
/// where it is.
pub(super) const PORT_QUEUE: &str = "port_workflow_q";

/// Task options that are not a JSON object (Python's `PoisonOptionsChild`
/// uses `[]` too):
/// - stored on a promoted task node, they fail that node alone with
///   `WORKER_SERIALIZATION_ERROR` (Python `_build_enqueued_task_params`,
///   `python/horsies/core/workflows/engine.py:798-805`; the baseline
///   `horsies_workflow_task_options_refusal`);
/// - on the task of a child node that has a `good_until`, the merge of the
///   deadline into them raises at each child start (Python
///   `_child_task_options_json`, `python/horsies/core/workflows/engine.py:425-475`).
pub(super) const POISON_TASK_OPTIONS: &str = "[]";

/// The process facts that the start write (`horsies_start_attempt`) records
/// on a RUNNING task row that [`seed_node_task`] seeds. Every write that
/// moves a row to RUNNING sets them, and the attempt row of a failure copies
/// them into its NOT NULL columns (`horsies_task_attempts`).
const SEEDED_WORKER_PID: i32 = 4242;
const SEEDED_WORKER_HOSTNAME: &str = "port-seed-host";
const SEEDED_WORKER_PROCESS_NAME: &str = "port-seed-process";

/// A stored success policy that does not parse: a case is a number, not an
/// object (Python's `BAD_POLICY`).
pub(super) fn poison_success_policy() -> serde_json::Value {
    serde_json::json!({"cases": [1]})
}

/// The shared migrated database. A test on it must not leave a RUNNING
/// workflow with stuck work, because other tests run global passes on it.
pub(super) async fn shared_pool() -> PgPool {
    crate::broker::terminalization_matrix::sqlx_migrated_pool().await
}

/// Connections of one test pool.
const TEST_POOL_CONNECTIONS: u32 = 5;

/// How long [`IsolatedDatabase::drop`] waits for its pool to close. The
/// close waits for every checked-out connection, so a connection that a
/// test never gives back would stop the whole `#[serial]` run.
const POOL_CLOSE_BOUND: std::time::Duration = std::time::Duration::from_secs(10);

/// History leaves the coverage pass creates ahead, in days and in hours.
const COVERAGE_HISTORY_DAYS: u32 = 2;
const COVERAGE_HEARTBEAT_HOURS: u32 = 2;

/// The maintenance database of the test server: `ALTER DATABASE ...
/// ALLOW_CONNECTIONS false` cannot run on a session of the database it
/// changes.
const ADMIN_DATABASE: &str = "postgres";

// ---------------------------------------------------------------------------
// Isolated database
// ---------------------------------------------------------------------------

/// A migrated database that one test owns. A global recovery or phase-2 pass
/// reads every workflow, so a test that runs one gets its own database, as
/// the Python tests do (`isolated_test_database`).
pub(super) struct IsolatedDatabase {
    admin: tokio_postgres::Client,
    pub(super) pool: PgPool,
    database: TestDatabase,
}

impl IsolatedDatabase {
    /// A copy of the installed test template (`crate::test_support`),
    /// covered by the partition coverage pass.
    pub(super) async fn create() -> Self {
        let database = TestDatabase::create().await;
        let pool = connect(database.url()).await;
        let coverage = ensure_partition_coverage_in_pool(
            &crate::broker::pools::core_pool_for_test_pool(&pool),
            crate::broker::core_rows::StatementLog::Off,
            COVERAGE_HISTORY_DAYS,
            COVERAGE_HEARTBEAT_HOURS,
            &[],
        )
        .await
        .expect("cover the history partitions");
        assert!(
            matches!(coverage, CoverageOutcome::Ensured(_)),
            "partition coverage of the isolated database: {coverage:?}"
        );
        let admin =
            crate::test_support::connect(&with_database(&server_url(), ADMIN_DATABASE)).await;
        Self {
            admin,
            pool,
            database,
        }
    }

    /// A second pool on the same database: a process that did not share any
    /// state in memory with the first one.
    pub(super) async fn other_process_pool(&self) -> PgPool {
        connect(self.database.url()).await
    }

    /// Allows or refuses new connections to the database. Connections that
    /// are open stay open, so a pool keeps working while a new connection,
    /// such as a listener's, is refused.
    pub(super) async fn allow_connections(&mut self, allowed: bool) {
        self.admin
            .batch_execute(&format!(
                "ALTER DATABASE \"{}\" ALLOW_CONNECTIONS {allowed}",
                self.database.name()
            ))
            .await
            .expect("change ALLOW_CONNECTIONS of the isolated database");
    }

    /// Closes the pool and drops the database.
    ///
    /// # Panics
    ///
    /// When the pool does not close within [`POOL_CLOSE_BOUND`]: a
    /// connection of the pool is still checked out (a listener or a
    /// connection that the test still holds), and the close waits for it.
    pub(super) async fn drop(self) {
        let size = self.pool.size();
        let idle = self.pool.num_idle();
        if tokio::time::timeout(POOL_CLOSE_BOUND, self.pool.close())
            .await
            .is_err()
        {
            panic!(
                "the pool of test database {} did not close within {POOL_CLOSE_BOUND:?}: \
                 {size} connections open and {idle} idle at the close, so a connection is \
                 still checked out; drop each listener and held connection first",
                self.database.name()
            );
        }
        self.database.drop().await;
    }
}

async fn connect(url: &str) -> PgPool {
    PgPoolOptions::new()
        .max_connections(TEST_POOL_CONNECTIONS)
        .connect(url)
        .await
        .expect("connect the test pool")
}

// ---------------------------------------------------------------------------
// Seeds
// ---------------------------------------------------------------------------

/// One `horsies_workflows` row. `sent_at`, `created_at`, `started_at` and
/// `updated_at` are `NOW()`; `completed_at` is `NOW()` for a terminal status
/// (every terminal workflow status sets it) and NULL otherwise.
#[derive(Debug, Clone)]
pub(super) struct WorkflowSeed {
    pub(super) id: Uuid,
    pub(super) name: String,
    pub(super) definition_key: Option<String>,
    pub(super) status: &'static str,
    pub(super) on_error: &'static str,
    pub(super) output_task_index: Option<i32>,
    pub(super) success_policy: Option<serde_json::Value>,
    pub(super) result: Option<String>,
    pub(super) error: Option<String>,
    pub(super) parent: Option<ParentLink>,
}

/// The parent of a child workflow.
#[derive(Debug, Clone, Copy)]
pub(super) struct ParentLink {
    pub(super) workflow_id: Uuid,
    pub(super) task_index: i32,
    pub(super) root_workflow_id: Uuid,
    pub(super) depth: i32,
}

impl ParentLink {
    /// The link of a child of the top-level workflow `parent` at node `task_index`.
    pub(super) fn of_root(parent: Uuid, task_index: i32) -> Self {
        Self {
            workflow_id: parent,
            task_index,
            root_workflow_id: parent,
            depth: 1,
        }
    }
}

impl WorkflowSeed {
    /// A RUNNING top-level workflow with `on_error = fail`, no output node and no policy.
    pub(super) fn running() -> Self {
        let id = Uuid::new_v4();
        Self {
            id,
            name: format!("port_{}", id.simple()),
            definition_key: Some(format!("tests.port.{}.v1", id.simple())),
            status: "RUNNING",
            on_error: "fail",
            output_task_index: None,
            success_policy: None,
            result: None,
            error: None,
            parent: None,
        }
    }

    /// A RUNNING child of `link`.
    pub(super) fn child(link: ParentLink) -> Self {
        Self {
            parent: Some(link),
            ..Self::running()
        }
    }

    pub(super) fn status(mut self, status: &'static str) -> Self {
        self.status = status;
        self
    }

    pub(super) fn on_error(mut self, on_error: &'static str) -> Self {
        self.on_error = on_error;
        self
    }

    pub(super) fn output(mut self, task_index: i32) -> Self {
        self.output_task_index = Some(task_index);
        self
    }

    pub(super) fn policy(mut self, policy: serde_json::Value) -> Self {
        self.success_policy = Some(policy);
        self
    }

    pub(super) fn error(mut self, error: String) -> Self {
        self.error = Some(error);
        self
    }

    pub(super) fn result(mut self, result: String) -> Self {
        self.result = Some(result);
        self
    }
}

/// Inserts the workflow row and returns its id.
pub(super) async fn seed_workflow(pool: &PgPool, seed: &WorkflowSeed) -> Uuid {
    let (parent_id, parent_index, root, depth) = match seed.parent {
        None => (None, None, seed.id, 0),
        Some(link) => (
            Some(link.workflow_id),
            Some(link.task_index),
            link.root_workflow_id,
            link.depth,
        ),
    };
    sqlx::query(
        "INSERT INTO horsies_workflows (
             id, name, definition_key, status, on_error, output_task_index,
             success_policy, result, error, parent_workflow_id, parent_task_index,
             root_workflow_id, depth, sent_at, created_at, started_at, completed_at,
             updated_at
         ) VALUES (
             $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13,
             NOW(), NOW(), NOW(),
             CASE WHEN $4 IN ('COMPLETED', 'FAILED', 'CANCELLED', 'EXPIRED') THEN NOW() END,
             NOW()
         )",
    )
    .bind(seed.id)
    .bind(&seed.name)
    .bind(&seed.definition_key)
    .bind(seed.status)
    .bind(seed.on_error)
    .bind(seed.output_task_index)
    .bind(&seed.success_policy)
    .bind(&seed.result)
    .bind(&seed.error)
    .bind(parent_id)
    .bind(parent_index)
    .bind(root)
    .bind(depth)
    .execute(pool)
    .await
    .expect("seed the workflow row");
    seed.id
}

/// What a node is: a task, or a sub-workflow with its child.
#[derive(Debug, Clone)]
pub(super) enum NodeKind {
    /// A task node with its typed facts.
    Task {
        max_retries: i32,
        execution: &'static str,
        retention_class_key: &'static str,
    },
    /// A sub-workflow node. `child` is the started child workflow, if any.
    SubWorkflow {
        definition_key: String,
        child: Option<Uuid>,
        params: SubWorkflowParams,
    },
}

/// One `horsies_workflow_tasks` row. `created_at` is `NOW()`; `started_at` is
/// `NOW()` from ENQUEUED on; `completed_at` is `NOW()` for COMPLETED, FAILED
/// and SKIPPED.
#[derive(Debug, Clone)]
pub(super) struct NodeSeed {
    pub(super) workflow_id: Uuid,
    pub(super) task_index: i32,
    pub(super) node_id: String,
    pub(super) task_name: String,
    pub(super) input: String,
    pub(super) dependencies: Vec<i32>,
    pub(super) args_from: Option<serde_json::Value>,
    pub(super) workflow_ctx_from: Option<Vec<String>>,
    pub(super) allow_failed_deps: bool,
    pub(super) join: JoinType,
    pub(super) min_success: Option<i32>,
    pub(super) task_options: Option<String>,
    pub(super) queue_name: &'static str,
    pub(super) status: &'static str,
    pub(super) task_id: Option<Uuid>,
    pub(super) kind: NodeKind,
    pub(super) downstream_subworkflow: &'static str,
    pub(super) sub_workflow_summary: Option<String>,
    pub(super) result: Option<String>,
}

impl NodeSeed {
    /// A task node `node_<index>` of task `port_task` with no dependency.
    pub(super) fn task(workflow_id: Uuid, task_index: i32, status: &'static str) -> Self {
        Self {
            workflow_id,
            task_index,
            node_id: format!("node_{task_index}"),
            task_name: "port_task".to_owned(),
            input: "{}".to_owned(),
            dependencies: Vec::new(),
            args_from: None,
            workflow_ctx_from: None,
            allow_failed_deps: false,
            join: JoinType::All,
            min_success: None,
            task_options: None,
            queue_name: "default",
            status,
            task_id: None,
            kind: NodeKind::Task {
                max_retries: 0,
                execution: "ISOLATED",
                retention_class_key: "forever",
            },
            downstream_subworkflow: "NONE",
            sub_workflow_summary: None,
            result: None,
        }
    }

    /// A sub-workflow node whose child is registered under `definition_key`.
    pub(super) fn sub_workflow(
        workflow_id: Uuid,
        task_index: i32,
        status: &'static str,
        definition_key: &str,
        child: Option<Uuid>,
    ) -> Self {
        Self {
            task_name: "port_child".to_owned(),
            kind: NodeKind::SubWorkflow {
                definition_key: definition_key.to_owned(),
                child,
                params: SubWorkflowParams::NotGiven,
            },
            ..Self::task(workflow_id, task_index, status)
        }
    }

    pub(super) fn deps(mut self, dependencies: &[i32]) -> Self {
        self.dependencies = dependencies.to_vec();
        self
    }

    pub(super) fn options(mut self, task_options: &str) -> Self {
        self.task_options = Some(task_options.to_owned());
        self
    }

    pub(super) fn task_id(mut self, task_id: Uuid) -> Self {
        self.task_id = Some(task_id);
        self
    }

    /// Injects the result of dependency `index` under input member `key`.
    pub(super) fn args_from(mut self, key: &str, index: i32) -> Self {
        self.args_from = Some(serde_json::json!({ key: index }));
        self
    }

    pub(super) fn allow_failed_deps(mut self) -> Self {
        self.allow_failed_deps = true;
        self
    }

    pub(super) fn result(mut self, result: String) -> Self {
        self.result = Some(result);
        self
    }

    pub(super) fn input(mut self, input: &str) -> Self {
        self.input = input.to_owned();
        self
    }
}

/// Inserts the node row and returns its row id.
pub(super) async fn seed_node(pool: &PgPool, seed: &NodeSeed) -> Uuid {
    let row_id = Uuid::new_v4();
    let (max_retries, execution, retention_class_key) = match &seed.kind {
        NodeKind::Task {
            max_retries,
            execution,
            retention_class_key,
        } => (
            Some(*max_retries),
            Some(*execution),
            Some(*retention_class_key),
        ),
        NodeKind::SubWorkflow { .. } => (None, None, None),
    };
    let (is_subworkflow, sub_workflow_id, sub_workflow_name, sub_definition_key, params) =
        match &seed.kind {
            NodeKind::Task { .. } => (false, None, None, None, None),
            NodeKind::SubWorkflow {
                definition_key,
                child,
                params,
            } => (
                true,
                *child,
                Some(seed.task_name.clone()),
                Some(definition_key.clone()),
                Some(params.as_stored()),
            ),
        };
    let min_success = match seed.join {
        JoinType::Quorum => Some(seed.min_success.expect("a quorum node names min_success")),
        JoinType::All | JoinType::Any => None,
    };
    sqlx::query(
        "INSERT INTO horsies_workflow_tasks (
             id, workflow_id, task_index, node_id, task_name, input, queue_name, priority,
             dependencies, args_from, workflow_ctx_from, allow_failed_deps, join_type,
             min_success, task_options, max_retries, execution, good_until,
             retention_class_key, status, task_id, is_subworkflow, sub_workflow_id,
             sub_workflow_name, sub_definition_key, sub_workflow_params,
             downstream_subworkflow, sub_workflow_summary, result,
             created_at, started_at, completed_at
         ) VALUES (
             $1, $2, $3, $4, $5, $6, $27, 100, $7, $8, $9, $10, $11, $12, $13,
             $14, $15, NULL, $16, $17, $18, $19, $20, $21, $22, $23, $24, $25, $26,
             NOW(),
             CASE WHEN $17 IN ('ENQUEUED', 'RUNNING', 'COMPLETED', 'FAILED') THEN NOW() END,
             CASE WHEN $17 IN ('COMPLETED', 'FAILED', 'SKIPPED') THEN NOW() END
         )",
    )
    .bind(row_id)
    .bind(seed.workflow_id)
    .bind(seed.task_index)
    .bind(&seed.node_id)
    .bind(&seed.task_name)
    .bind(&seed.input)
    .bind(&seed.dependencies)
    .bind(&seed.args_from)
    .bind(&seed.workflow_ctx_from)
    .bind(seed.allow_failed_deps)
    .bind(seed.join.to_string())
    .bind(min_success)
    .bind(&seed.task_options)
    .bind(max_retries)
    .bind(execution)
    .bind(retention_class_key)
    .bind(seed.status)
    .bind(seed.task_id)
    .bind(is_subworkflow)
    .bind(sub_workflow_id)
    .bind(sub_workflow_name)
    .bind(sub_definition_key)
    .bind(params)
    .bind(seed.downstream_subworkflow)
    .bind(&seed.sub_workflow_summary)
    .bind(&seed.result)
    .bind(seed.queue_name)
    .execute(pool)
    .await
    .expect("seed the node row");
    row_id
}

/// The live status of a seeded workflow task row.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum LiveStatus {
    Pending,
    Claimed,
    Running,
}

impl LiveStatus {
    fn as_sql(self) -> &'static str {
        match self {
            Self::Pending => "PENDING",
            Self::Claimed => "CLAIMED",
            Self::Running => "RUNNING",
        }
    }
}

/// Inserts the live task row of a workflow node: an engine-minted row with no
/// fingerprint, key or retained input. A CLAIMED or RUNNING row is held by
/// [`WORKER`] since `NOW()`; a CLAIMED row's claim runs one more minute; a
/// RUNNING row carries the process facts of the start write
/// ([`SEEDED_WORKER_PID`], [`SEEDED_WORKER_HOSTNAME`],
/// [`SEEDED_WORKER_PROCESS_NAME`]).
/// Returns the claim generation (`claimed_at`), `None` for a PENDING row.
pub(super) async fn seed_node_task(
    pool: &PgPool,
    task_id: Uuid,
    status: LiveStatus,
) -> Option<DateTime<Utc>> {
    sqlx::query_scalar(
        "INSERT INTO horsies_tasks (
             id, task_name, queue_name, priority, input, task_options, execution,
             retention_class_key, sent_at, created_at, command_fingerprint_version,
             command_fingerprint, idempotency_key_digest, status, enqueued_at,
             claimed_at, claimed_by_worker_id, claim_expires_at, started_at,
             finalizing_at, finalizing_by_worker_id, worker_pid, worker_hostname,
             worker_process_name, retry_count, max_retries, next_retry_at, good_until,
             is_workflow_task, updated_at, retain_rerun_input, rerun_of_task_id,
             rerun_root_task_id
         ) VALUES (
             $1, 'port_task', 'default', 100, '{}', NULL, 'ISOLATED', 'forever',
             NOW(), NOW(), NULL, NULL, NULL, $2, NOW(),
             CASE WHEN $2 = 'PENDING' THEN NULL ELSE NOW() END,
             CASE WHEN $2 = 'PENDING' THEN NULL ELSE $3 END,
             CASE WHEN $2 = 'CLAIMED' THEN NOW() + INTERVAL '1 minute' END,
             CASE WHEN $2 = 'RUNNING' THEN NOW() END,
             NULL, NULL,
             CASE WHEN $2 = 'RUNNING' THEN $4 END,
             CASE WHEN $2 = 'RUNNING' THEN $5 END,
             CASE WHEN $2 = 'RUNNING' THEN $6 END,
             0, 0, NULL, NULL, TRUE, NOW(), FALSE, NULL, NULL
         )
         RETURNING claimed_at",
    )
    .bind(task_id)
    .bind(status.as_sql())
    .bind(WORKER)
    .bind(SEEDED_WORKER_PID)
    .bind(SEEDED_WORKER_HOSTNAME)
    .bind(SEEDED_WORKER_PROCESS_NAME)
    .fetch_one(pool)
    .await
    .expect("seed the workflow task row")
}

/// The process id that [`start_claimed_task`] records.
const START_WORKER_PID: i32 = 4343;

/// The start write of the child host for a task that [`WORKER`] claimed at
/// `claimed_at` (`broker::claims::start_attempt`, the statement of
/// `worker::executor::child_host::run::start_attempt`). Every task start of
/// the worker goes through it.
pub(super) async fn start_claimed_task(
    pool: &PgPool,
    task_id: Uuid,
    claimed_at: DateTime<Utc>,
) -> crate::broker::claims::StartOutcome {
    let broker = broker(pool);
    let (open, _write) = broker
        .core_pools_for_write()
        .expect("the core pools of the broker");
    crate::broker::claims::start_attempt(
        open.main(),
        broker.statement_log(),
        &crate::broker::claims::StartAttempt {
            task_id,
            fence: crate::core::lifecycle::OwnedClaim {
                worker_id: WORKER.to_owned(),
                claimed_at,
            },
            worker_pid: START_WORKER_PID,
            worker_hostname: SEEDED_WORKER_HOSTNAME.to_owned(),
            worker_process_name: SEEDED_WORKER_PROCESS_NAME.to_owned(),
            beat_sender_id: format!("{WORKER}:{START_WORKER_PID}"),
        },
        crate::broker::child_writes::WriteDeadline::start_attempt(),
    )
    .await
    .expect("the start statement")
}

// ---------------------------------------------------------------------------
// Engine drivers
// ---------------------------------------------------------------------------

/// A task node at `index` of task `task_name`, with its queue and priority
/// resolved, no dependency and the empty input of a Python node (`{}`).
pub(super) fn task_node(index: usize, task_name: &str) -> AnyNode {
    AnyNode {
        task_name: task_name.to_owned(),
        input_json: "{}".to_owned(),
        dependencies: Vec::new(),
        args_from: std::collections::HashMap::new(),
        workflow_ctx_from: None,
        queue: Some(PORT_QUEUE.to_owned()),
        priority: Some(100),
        allow_failed_deps: false,
        join: JoinType::All,
        min_success: None,
        good_until: None,
        index,
        node_id: Some(format!("node_{index}")),
        task_options_json: None,
        is_subworkflow: false,
        sub_definition_key: None,
        sub_workflow_params: SubWorkflowParams::NotGiven,
    }
}

/// A spec of `tasks` with a definition key, `on_error = fail`, no output node
/// and no policy.
pub(super) fn spec(name: &str, tasks: Vec<AnyNode>) -> WorkflowSpec {
    WorkflowSpec {
        name: name.to_owned(),
        definition_key: Some(format!("tests.port.{name}.v1")),
        tasks,
        on_error: OnError::Fail,
        output_index: None,
        success_policy: None,
    }
}

/// A sub-workflow node at `index` whose child is `child`, resolved by the
/// child's definition key. It stores the child's name as its task name and
/// gives no params, as a Python `SubWorkflowNode` without params.
pub(super) fn sub_workflow_node(index: usize, child: &WorkflowSpec) -> AnyNode {
    AnyNode {
        task_name: child.name.clone(),
        queue: None,
        priority: None,
        is_subworkflow: true,
        sub_definition_key: child.definition_key.clone(),
        ..task_node(index, &child.name)
    }
}

/// A policy whose cases each require the listed node indexes.
pub(super) fn required(cases: &[&[usize]]) -> SuccessPolicy {
    SuccessPolicy {
        cases: cases
            .iter()
            .map(|indexes| SuccessCase {
                required_indices: indexes.to_vec(),
            })
            .collect(),
        optional_indices: None,
    }
}

/// The child specs that resolve each of `children` by its definition key.
pub(super) fn child_specs_of(children: &[WorkflowSpec]) -> TestChildSpecs {
    TestChildSpecs::of(children)
}

/// A broker on `pool` for the start and the reads.
pub(super) fn broker(pool: &PgPool) -> Arc<PostgresBroker> {
    Arc::new(PostgresBroker::from_pool(
        pool.clone(),
        crate::ClientFamily::Rust,
    ))
}

/// Starts `spec` through the core's start of a client spec
/// (`PostgresBroker::start_client_workflow`) with `workflow_id`, or a new
/// id, the children of its sub-workflow roots from `child_specs`, and no
/// resend. Returns the id with the outcome.
pub(super) async fn try_start(
    pool: &PgPool,
    spec: &WorkflowSpec,
    workflow_id: Option<Uuid>,
    child_specs: &TestChildSpecs,
) -> (Uuid, Result<ClientStartOutcome, ClientStartError>) {
    let workflow_id = workflow_id.unwrap_or_else(Uuid::new_v4);
    let started = broker(pool)
        .start_client_workflow(
            &child_specs.client_spec(spec),
            workflow_id,
            Utc::now(),
            false,
            child_specs,
        )
        .await;
    (workflow_id, started)
}

/// Starts `spec` with a new id through the core start, and returns the id.
///
/// Panics: a start that is refused or finds an existing workflow.
pub(super) async fn start(
    pool: &PgPool,
    spec: &WorkflowSpec,
    child_specs: &TestChildSpecs,
) -> Uuid {
    match try_start(pool, spec, None, child_specs).await {
        (workflow_id, Ok(ClientStartOutcome::Started)) => workflow_id,
        (workflow_id, Ok(outcome)) => panic!(
            "start of workflow '{}' ({workflow_id}) wrote nothing: {outcome:?}",
            spec.name
        ),
        (workflow_id, Err(error)) => panic!(
            "start of workflow '{}' ({workflow_id}) failed: {error:?}",
            spec.name
        ),
    }
}

/// The time an unreachable pool waits for a connection.
const UNREACHABLE_ACQUIRE_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(1);

/// A pool that connects to a port that refuses connections: every database
/// call fails with a retryable connection error (`PoolTimedOut` after
/// [`UNREACHABLE_ACQUIRE_TIMEOUT`]).
pub(super) fn unreachable_pool() -> PgPool {
    PgPoolOptions::new()
        .acquire_timeout(UNREACHABLE_ACQUIRE_TIMEOUT)
        .connect_lazy_with(
            PgConnectOptions::new()
                .host("127.0.0.1")
                .port(1)
                .username("postgres")
                .database("horsies_unreachable"),
        )
}

/// The reads and operations of one workflow through the core functions
/// that a client's handle calls, on a broker of one pool.
pub(super) struct WorkflowOps {
    workflow_id: Uuid,
    broker: Arc<PostgresBroker>,
}

impl WorkflowOps {
    /// The end of the workflow, waiting up to `timeout`, with its stored
    /// values as text (`query::wait_workflow_end`).
    pub(super) async fn end(
        &self,
        timeout: Option<Duration>,
    ) -> Result<WorkflowWait, WorkflowError> {
        wait_workflow_end(&self.broker, self.workflow_id, timeout).await
    }

    /// The result of the workflow, waiting up to `timeout` for its end
    /// ([`Self::end`]), read here as a client reads it ([`result_of_end`]).
    pub(super) async fn get(
        &self,
        timeout: Option<Duration>,
    ) -> Result<TaskResult<serde_json::Value>, WorkflowError> {
        Ok(result_of_end(self.workflow_id, self.end(timeout).await?))
    }

    /// The status of the workflow (`PostgresBroker::workflow_status`).
    pub(super) async fn status(&self) -> HandleResult<WorkflowStatus> {
        self.broker.workflow_status(self.workflow_id).await
    }

    /// The nodes of the workflow that store a result
    /// (`PostgresBroker::workflow_node_results`).
    pub(super) async fn node_results(&self) -> HandleResult<NodeResultsRead> {
        self.broker.workflow_node_results(self.workflow_id).await
    }

    /// The stored result of each node with a node id ([`Self::node_results`]),
    /// each envelope read. Panics for a missing workflow.
    pub(super) async fn results(
        &self,
    ) -> HandleResult<HashMap<String, TaskResult<serde_json::Value>>> {
        match self.node_results().await? {
            NodeResultsRead::WorkflowAbsent => {
                panic!("no workflow has the id {}", self.workflow_id)
            }
            NodeResultsRead::Nodes(nodes) => Ok(nodes
                .into_iter()
                .filter_map(|node| {
                    let result = stored_result(&node.result);
                    node.node_id.map(|node_id| (node_id, result))
                })
                .collect()),
        }
    }

    /// The stored result of the node `node_id`
    /// (`PostgresBroker::workflow_node_result`).
    pub(super) async fn node_result(
        &self,
        node_id: &str,
    ) -> HandleResult<Option<StoredNodeResult>> {
        self.broker
            .workflow_node_result(self.workflow_id, node_id)
            .await
    }

    /// The result of the node `node_id` ([`Self::node_result`]), its
    /// envelope read and its value read as `V`. Panics for a node that
    /// stores no result, or a value that does not read as `V`.
    pub(super) async fn result_for<V: serde::de::DeserializeOwned>(
        &self,
        node_id: &str,
    ) -> HandleResult<TaskResult<V>> {
        let stored = self.node_result(node_id).await?.unwrap_or_else(|| {
            panic!(
                "node {node_id} of workflow {} stores no result",
                self.workflow_id
            )
        });
        Ok(match stored_result(&stored.result) {
            TaskResult::Ok(value) => {
                TaskResult::Ok(serde_json::from_value(value).unwrap_or_else(|error| {
                    panic!("the result of node {node_id} does not read: {error}")
                }))
            }
            TaskResult::Err(error) => TaskResult::Err(error),
        })
    }

    /// The nodes of the workflow (`PostgresBroker::workflow_nodes`).
    pub(super) async fn nodes(&self) -> HandleResult<NodesRead> {
        self.broker.workflow_nodes(self.workflow_id).await
    }

    /// The nodes of the workflow ([`Self::nodes`]), each read
    /// ([`NodeInfo::of_stored`]). Panics for a missing workflow.
    pub(super) async fn tasks(&self) -> HandleResult<Vec<NodeInfo>> {
        match self.nodes().await? {
            NodesRead::WorkflowAbsent => panic!("no workflow has the id {}", self.workflow_id),
            NodesRead::Nodes(nodes) => Ok(nodes.into_iter().map(NodeInfo::of_stored).collect()),
        }
    }

    /// `PostgresBroker::cancel_workflow`.
    pub(super) async fn cancel(&self) -> HandleResult<bool> {
        self.broker.cancel_workflow(self.workflow_id).await
    }

    /// `PostgresBroker::pause_workflow`.
    pub(super) async fn pause(&self) -> HandleResult<bool> {
        self.broker.pause_workflow(self.workflow_id).await
    }

    /// The resume of a client whose children come from `child_specs`
    /// (`lifecycle::resume_client_workflow`).
    pub(super) async fn resume(&self, child_specs: &TestChildSpecs) -> HandleResult<bool> {
        resume_client_workflow(&self.broker, self.workflow_id, child_specs).await
    }
}

/// The result of the end of `workflow_id`, read as a client reads it: the
/// stored result envelope of a COMPLETED workflow (of an outputless one, the
/// `results_by_id` map of its envelope, each entry an envelope); the stored
/// error of a FAILED, CANCELLED or EXPIRED workflow, or the code of its
/// status when it stores none; the code of a PAUSED, missing or unfinished
/// workflow. The read is the clients'; the tests read the core's stored
/// values through it. Panics for a stored value that does not read.
pub(super) fn result_of_end(
    workflow_id: Uuid,
    wait: WorkflowWait,
) -> TaskResult<serde_json::Value> {
    match wait {
        WorkflowWait::NotFound => TaskResult::Err(TaskError::builtin(
            RetrievalCode::WorkflowNotFound,
            format!("workflow {workflow_id} not found"),
        )),
        WorkflowWait::TimedOut => TaskResult::Err(TaskError::builtin(
            RetrievalCode::WaitTimeout,
            format!("workflow {workflow_id} is not terminal"),
        )),
        WorkflowWait::Ended(WorkflowEnd::Paused) => TaskResult::Err(TaskError::builtin(
            OutcomeCode::WorkflowPaused,
            "workflow is paused awaiting intervention",
        )),
        WorkflowWait::Ended(WorkflowEnd::Ended {
            status,
            error: None,
        }) => TaskResult::Err(TaskError::builtin(
            ended_code(status),
            format!("workflow {workflow_id} is {}", status.wire_value()),
        )),
        WorkflowWait::Ended(WorkflowEnd::Ended {
            error: Some(text), ..
        }) => TaskResult::Err(stored_error(&text)),
        WorkflowWait::Ended(WorkflowEnd::Completed { result }) => {
            let text = result
                .unwrap_or_else(|| panic!("COMPLETED workflow {workflow_id} stores no result"));
            completed_result(&text)
        }
    }
}

/// The code of an ended status with no stored error.
fn ended_code(status: TerminalErrorStatus) -> OutcomeCode {
    match status {
        TerminalErrorStatus::Failed => OutcomeCode::WorkflowFailed,
        TerminalErrorStatus::Cancelled => OutcomeCode::WorkflowCancelled,
        TerminalErrorStatus::Expired => OutcomeCode::WorkflowExpired,
    }
}

/// The stored result of a COMPLETED workflow: its envelope, or for an
/// outputless workflow the `results_by_id` map of its envelope.
fn completed_result(text: &str) -> TaskResult<serde_json::Value> {
    let value: serde_json::Value = serde_json::from_str(text)
        .unwrap_or_else(|error| panic!("the stored result is not JSON: {error}: {text}"));
    match value.get("__h_outputless_terminals__") {
        Some(serde_json::Value::Bool(true)) => TaskResult::Ok(value["ok"]["results_by_id"].clone()),
        Some(_) | None => stored_result(text),
    }
}

/// One stored node, read as a client reads it (`tasks()`): its status, its
/// result envelope and its sub-workflow summary.
#[derive(Debug)]
pub(super) struct NodeInfo {
    pub(super) index: i32,
    pub(super) name: String,
    pub(super) status: WorkflowTaskStatus,
    pub(super) result: Option<TaskResult<serde_json::Value>>,
    pub(super) started_at: Option<DateTime<Utc>>,
    pub(super) completed_at: Option<DateTime<Utc>>,
    pub(super) sub_workflow_id: Option<Uuid>,
    pub(super) sub_workflow_summary: Option<SubWorkflowSummary>,
}

impl NodeInfo {
    /// The node of `stored`. Panics for a stored value that does not read.
    pub(super) fn of_stored(stored: StoredNode) -> Self {
        let status = serde_json::from_value(serde_json::Value::String(stored.status.clone()))
            .unwrap_or_else(|error| {
                panic!(
                    "node {} stores the status {:?}: {error}",
                    stored.task_index, stored.status
                )
            });
        let sub_workflow_summary = stored.sub_workflow_summary.as_deref().map(stored_summary);
        Self {
            index: stored.task_index,
            name: stored.task_name,
            status,
            result: stored.result.as_deref().map(stored_result),
            started_at: stored.started_at,
            completed_at: stored.completed_at,
            sub_workflow_id: stored.sub_workflow_id,
            sub_workflow_summary,
        }
    }
}

/// A stored sub-workflow summary (`horsies_workflow_tasks.sub_workflow_summary`).
pub(super) fn stored_summary(text: &str) -> SubWorkflowSummary {
    SubWorkflowSummary::from_stored_text(text)
        .unwrap_or_else(|refused| panic!("the stored summary does not read: {refused}: {text}"))
}

/// The reads and operations of `workflow_id` on a broker of
/// [`unreachable_pool`].
pub(super) fn unreachable_handle(workflow_id: Uuid) -> WorkflowOps {
    handle(&unreachable_pool(), workflow_id)
}

/// The reads and operations of `workflow_id` on a broker of `pool`.
pub(super) fn handle(pool: &PgPool, workflow_id: Uuid) -> WorkflowOps {
    WorkflowOps {
        workflow_id,
        broker: broker(pool),
    }
}

/// Takes the live task of a node to RUNNING under [`WORKER`], with the process
/// facts of its start ([`SEEDED_WORKER_PID`], [`SEEDED_WORKER_HOSTNAME`],
/// [`SEEDED_WORKER_PROCESS_NAME`]), as the claim and the start handoff do, and
/// the node to RUNNING with it.
async fn hold_running(pool: &PgPool, task_id: Uuid) {
    let held = sqlx::query(
        "UPDATE horsies_tasks
         SET status = 'RUNNING', claimed_at = NOW(), claimed_by_worker_id = $2,
             claim_expires_at = NULL, started_at = NOW(), worker_pid = $3,
             worker_hostname = $4, worker_process_name = $5, updated_at = NOW()
         WHERE id = $1 AND status IN ('PENDING', 'CLAIMED', 'RUNNING')",
    )
    .bind(task_id)
    .bind(WORKER)
    .bind(SEEDED_WORKER_PID)
    .bind(SEEDED_WORKER_HOSTNAME)
    .bind(SEEDED_WORKER_PROCESS_NAME)
    .execute(pool)
    .await
    .expect("hold the task row");
    assert_eq!(held.rows_affected(), 1, "task {task_id} has no live row");
    sqlx::query(
        "UPDATE horsies_workflow_tasks SET status = 'RUNNING'
         WHERE task_id = $1 AND status = 'ENQUEUED'",
    )
    .bind(task_id)
    .execute(pool)
    .await
    .expect("hand the node to its running task");
}

/// Finishes the task of a workflow node the way a worker does: phase 1 moves
/// the task to history with `outcome`, then phase 2 (`on_workflow_task_complete`)
/// writes the node and advances the workflow.
pub(super) async fn finish_task(
    pool: &PgPool,
    task_id: Uuid,
    outcome: &TaskResult<serde_json::Value>,
    child_specs: &TestChildSpecs,
) {
    try_finish_task(pool, task_id, outcome, child_specs)
        .await
        .expect("phase 2 of the workflow task");
}

/// [`finish_task`] that returns the outcome of phase 2. Phase 1 must apply.
pub(super) async fn try_finish_task(
    pool: &PgPool,
    task_id: Uuid,
    outcome: &TaskResult<serde_json::Value>,
    child_specs: &TestChildSpecs,
) -> Result<WorkflowTaskCompletion, WorkflowTaskCompletionError> {
    let _ = finish_phase1(pool, task_id, outcome).await;
    on_workflow_task_complete_with(
        &PostgresBroker::from_pool(pool.clone(), crate::ClientFamily::Rust),
        task_id,
        child_specs,
    )
    .await
}

/// Phase 1 of [`finish_task`] alone: the task moves to history with
/// `outcome` and phase 2 stays owed. Returns the stored result text.
pub(super) async fn finish_phase1(
    pool: &PgPool,
    task_id: Uuid,
    outcome: &TaskResult<serde_json::Value>,
) -> String {
    hold_running(pool, task_id).await;
    let result_json = serde_json::to_string(outcome).expect("encode the task outcome");
    let outcomes = match outcome {
        TaskResult::Ok(_) => {
            terminal_writes::complete_workflow_task(pool, task_id, WORKER, &result_json).await
        }
        TaskResult::Err(error) => {
            let error_code = code(error);
            terminal_writes::fail_running_task(
                pool,
                task_id,
                WORKER,
                &TerminalFailure::of_task(&result_json, error_code.as_deref(), None),
            )
            .await
        }
    }
    .expect("phase 1 of the workflow task");
    assert!(
        matches!(
            outcomes.as_slice(),
            [TerminalizationOutcome::Applied { .. }]
        ),
        "phase 1 of task {task_id}: {outcomes:?}"
    );
    result_json
}

/// Finishes the task of node `task_index` of `workflow_id` (see [`finish_task`])
/// and returns the task id.
pub(super) async fn finish_node(
    pool: &PgPool,
    workflow_id: Uuid,
    task_index: i32,
    outcome: &TaskResult<serde_json::Value>,
    child_specs: &TestChildSpecs,
) -> Uuid {
    let task_id = node(pool, workflow_id, task_index)
        .await
        .task_id
        .unwrap_or_else(|| panic!("node {task_index} of {workflow_id} has no task"));
    finish_task(pool, task_id, outcome, child_specs).await;
    task_id
}

/// The completion check of `workflow_id` as the tree recovery pass runs it
/// (`recovery::complete_in_tree`): the workflow row `FOR UPDATE`, then
/// `horsies_workflow_try_complete`, in one transaction. Then the parent
/// advance that a child it ends owes ([`advance_parent_of`]). Python:
/// `check_workflow_completion`, which queues the parent propagation of a
/// child that it ends.
pub(super) async fn complete_workflow(
    pool: &PgPool,
    workflow_id: Uuid,
    child_specs: &TestChildSpecs,
) {
    let broker = broker(pool);
    let advance = match complete_in_tree(&broker, workflow_id)
        .await
        .expect("the completion check")
    {
        TreeCompletion::Written { advance } => advance,
        TreeCompletion::Unchanged => ParentAdvance::NotOwed,
    };
    drive_parent_advances(&broker, OwedParentAdvances::of(advance), child_specs).await;
}

/// Advances the parent node above the terminal child `child_workflow_id`,
/// and each level above that the advance ends: `horsies_workflow_child_finish`,
/// one transaction per level (`steps::advance_parents`), as phase 2 and
/// recovery case 1.6 run it. Python: `on_subworkflow_complete`.
pub(super) async fn advance_parent_of(
    pool: &PgPool,
    child_workflow_id: Uuid,
    child_specs: &TestChildSpecs,
) {
    drive_parent_advances(
        &broker(pool),
        OwedParentAdvances::of(ParentAdvance::Owed { child_workflow_id }),
        child_specs,
    )
    .await;
}

async fn drive_parent_advances(
    broker: &PostgresBroker,
    owed: OwedParentAdvances,
    child_specs: &TestChildSpecs,
) {
    advance_parents(broker, CheckoutClass::Reserved, owed, child_specs)
        .await
        .unwrap_or_else(|failure| {
            panic!(
                "the parent advance of child workflow {} failed: {:?}",
                failure.failed_child_workflow_id, failure.error
            )
        });
}

/// A failure outcome with a user error code.
pub(super) fn err(error_code: &str, message: &str) -> TaskResult<serde_json::Value> {
    TaskResult::Err(TaskError::new(error_code, message).expect("a user error code"))
}

/// A success outcome with `value`.
pub(super) fn ok(value: serde_json::Value) -> TaskResult<serde_json::Value> {
    TaskResult::Ok(value)
}

// ---------------------------------------------------------------------------
// The worker's finalize
// ---------------------------------------------------------------------------

/// A node task that runs under [`WORKER`]: what the worker holds for its
/// finalize.
#[derive(Debug, Clone)]
pub(super) struct RunningNodeTask {
    pub(super) task_id: Uuid,
    pub(super) queue_name: String,
    /// The claim generation of the fence.
    pub(super) claimed_at: DateTime<Utc>,
}

/// Takes the PENDING task of node `task_index` of `workflow_id` to RUNNING
/// under [`WORKER`], with the process facts of the start write
/// ([`SEEDED_WORKER_PID`], [`SEEDED_WORKER_HOSTNAME`],
/// [`SEEDED_WORKER_PROCESS_NAME`]), and the node to RUNNING, as the claim and
/// the start write leave them (Python's `_run_node_a`).
pub(super) async fn run_node_task(
    pool: &PgPool,
    workflow_id: Uuid,
    task_index: i32,
) -> RunningNodeTask {
    let (task_id, queue_name, claimed_at): (Uuid, String, DateTime<Utc>) = sqlx::query_as(
        "UPDATE horsies_tasks t
         SET status = 'RUNNING', claimed_at = NOW(), claimed_by_worker_id = $3,
             claim_expires_at = NULL, started_at = NOW(), worker_pid = $4,
             worker_hostname = $5, worker_process_name = $6, updated_at = NOW()
         FROM horsies_workflow_tasks n
         WHERE n.workflow_id = $1 AND n.task_index = $2 AND t.id = n.task_id
           AND t.status = 'PENDING'
         RETURNING t.id, t.queue_name::text, t.claimed_at",
    )
    .bind(workflow_id)
    .bind(task_index)
    .bind(WORKER)
    .bind(SEEDED_WORKER_PID)
    .bind(SEEDED_WORKER_HOSTNAME)
    .bind(SEEDED_WORKER_PROCESS_NAME)
    .fetch_one(pool)
    .await
    .unwrap_or_else(|error| {
        panic!("node {task_index} of {workflow_id} has no PENDING task: {error}")
    });
    let started = sqlx::query(
        "UPDATE horsies_workflow_tasks SET status = 'RUNNING', started_at = NOW()
         WHERE workflow_id = $1 AND task_index = $2 AND status = 'ENQUEUED'",
    )
    .bind(workflow_id)
    .bind(task_index)
    .execute(pool)
    .await
    .expect("hand the node to its running task");
    assert_eq!(
        started.rows_affected(),
        1,
        "node {task_index} of {workflow_id} is not ENQUEUED"
    );
    RunningNodeTask {
        task_id,
        queue_name,
        claimed_at,
    }
}

/// What the task of a node reports to the worker.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum NodeReport {
    /// The value `1`.
    Success,
    /// A task error `BOOM`, not retried.
    Failure,
}

/// What phase 1 of the worker's finalize owes to phase 2.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct PhaseOneDone {
    /// Phase 1 sent the capacity wake of the task's queue.
    pub(super) capacity_notified: bool,
    /// The form of the phase-2 statement, from the node facts of phase 1.
    pub(super) phase_two: PhaseTwoForm,
}

/// Phase 1 of the worker's finalize of `task` on `broker`: one statement,
/// `horsies_finish_workflow_success` for a success,
/// `horsies_finish_failed_attempt` with no retry for a failure (the worker's
/// `persist_ok_result` and `persist_failed_attempt`).
///
/// Panics when the write fails or owes no phase 2.
pub(super) async fn worker_phase_one(
    broker: &PostgresBroker,
    task: &RunningNodeTask,
    report: NodeReport,
) -> PhaseOneDone {
    let fence = OwnedClaim {
        worker_id: WORKER.to_owned(),
        claimed_at: task.claimed_at,
    };
    let written = match report {
        NodeReport::Success => persist_ok_result(broker, task.task_id, b"1", fence, true).await,
        NodeReport::Failure => {
            let failure = FinishFailedAttempt {
                task_id: task.task_id,
                fence,
                retry: RetryRequest::NoRetry,
                result_json: serde_json::to_string(&err("BOOM", "boom"))
                    .expect("encode the failure"),
                error_code: Some("BOOM".to_owned()),
                failed_reason: None,
                attempt: AttemptFailure {
                    kind: AttemptFailureKind::Failed,
                    error_message: Some("boom".to_owned()),
                    failed_reason: None,
                },
            };
            persist_failed_attempt(broker, &failure, true).await
        }
    };
    match written {
        Ok(FinalizeOutcome::Terminal {
            capacity_notified,
            phase_two,
        }) => PhaseOneDone {
            capacity_notified,
            phase_two,
        },
        Ok(FinalizeOutcome::Retried | FinalizeOutcome::Finalized | FinalizeOutcome::Refused) => {
            panic!("phase 1 of task {} owes no phase 2", task.task_id)
        }
        Err(error) => panic!("phase 1 of task {}: {error}", task.task_id),
    }
}

/// Phase 2 of the worker's finalize of `task` on `broker`, in the form that
/// phase 1 named (`worker::execution::finalize_workflow_phase`).
///
/// Panics when phase 2 fails or owes a parent advance (the callers run
/// top-level workflows).
pub(super) async fn worker_phase_two(
    broker: &PostgresBroker,
    child_specs: &TestChildSpecs,
    task: &RunningNodeTask,
    done: PhaseOneDone,
) {
    let owed = finalize_workflow_phase(
        broker,
        child_specs,
        task.task_id,
        &task.queue_name,
        true,
        done.capacity_notified,
        done.phase_two,
    )
    .await
    .unwrap_or_else(|error| panic!("phase 2 of task {}: {error}", task.task_id));
    assert!(
        owed.is_empty(),
        "phase 2 of task {} owes a parent advance",
        task.task_id
    );
}

/// A broker whose core pools connect through a proxy that counts round
/// trips (`WireProxy`). The proxy counts the requests of every connection
/// that the broker opens.
pub(super) struct CountedBroker {
    proxy: WireProxy,
    proxied: PgPool,
    pub(super) broker: Arc<PostgresBroker>,
}

impl CountedBroker {
    /// A broker on the database of `pool`, through a new proxy.
    pub(super) async fn open(pool: &PgPool) -> Self {
        let upstream = database_options(pool);
        let proxy = WireProxy::start(&upstream).await;
        let proxied = unpinged_single_connection_pool(proxy.options(&upstream)).await;
        let broker = Arc::new(PostgresBroker::from_pool(
            proxied.clone(),
            crate::ClientFamily::Rust,
        ));
        Self {
            proxy,
            proxied,
            broker,
        }
    }

    /// The round trips that `operation` sends through the proxy, with its
    /// output.
    pub(super) async fn count<T>(&self, operation: impl Future<Output = T>) -> (u32, T) {
        self.proxy.reset_round_trips();
        let output = operation.await;
        (self.proxy.round_trips(), output)
    }

    /// Closes the sqlx pool that the broker was built from.
    pub(super) async fn close(self) {
        self.proxied.close().await;
    }
}

// ---------------------------------------------------------------------------
// Reads
// ---------------------------------------------------------------------------

/// The columns of a node row that the ports assert on.
#[derive(Debug, sqlx::FromRow)]
pub(super) struct NodeRow {
    pub(super) status: String,
    pub(super) task_id: Option<Uuid>,
    pub(super) input: String,
    pub(super) result: Option<String>,
    pub(super) sub_workflow_id: Option<Uuid>,
    pub(super) sub_workflow_summary: Option<String>,
    pub(super) started_at: Option<DateTime<Utc>>,
    pub(super) completed_at: Option<DateTime<Utc>>,
}

pub(super) async fn node(pool: &PgPool, workflow_id: Uuid, task_index: i32) -> NodeRow {
    sqlx::query_as(
        "SELECT status, task_id, input, result, sub_workflow_id, sub_workflow_summary,
                started_at, completed_at
         FROM horsies_workflow_tasks WHERE workflow_id = $1 AND task_index = $2",
    )
    .bind(workflow_id)
    .bind(task_index)
    .fetch_one(pool)
    .await
    .unwrap_or_else(|error| panic!("read node {task_index} of {workflow_id}: {error}"))
}

pub(super) async fn node_status(pool: &PgPool, workflow_id: Uuid, task_index: i32) -> String {
    node(pool, workflow_id, task_index).await.status
}

/// The columns of a workflow row that the ports assert on.
#[derive(Debug, sqlx::FromRow)]
pub(super) struct WorkflowRow {
    pub(super) status: String,
    pub(super) result: Option<String>,
    pub(super) error: Option<String>,
    pub(super) completed_at: Option<DateTime<Utc>>,
}

pub(super) async fn workflow(pool: &PgPool, workflow_id: Uuid) -> WorkflowRow {
    sqlx::query_as(
        "SELECT status, result, error, completed_at FROM horsies_workflows WHERE id = $1",
    )
    .bind(workflow_id)
    .fetch_one(pool)
    .await
    .unwrap_or_else(|error| panic!("read workflow {workflow_id}: {error}"))
}

pub(super) async fn workflow_status(pool: &PgPool, workflow_id: Uuid) -> String {
    workflow(pool, workflow_id).await.status
}

/// The child workflow that node `task_index` of `workflow_id` started.
pub(super) async fn child_of(pool: &PgPool, workflow_id: Uuid, task_index: i32) -> Uuid {
    node(pool, workflow_id, task_index)
        .await
        .sub_workflow_id
        .unwrap_or_else(|| panic!("node {task_index} of {workflow_id} started no child"))
}

/// The input of the live task row of node `task_index`, as JSON.
pub(super) async fn node_task_input(
    pool: &PgPool,
    workflow_id: Uuid,
    task_index: i32,
) -> serde_json::Value {
    let task_id = node(pool, workflow_id, task_index)
        .await
        .task_id
        .unwrap_or_else(|| panic!("node {task_index} of {workflow_id} has no task"));
    let input: String = sqlx::query_scalar("SELECT input FROM horsies_tasks WHERE id = $1")
        .bind(task_id)
        .fetch_one(pool)
        .await
        .unwrap_or_else(|error| panic!("read the input of task {task_id}: {error}"));
    serde_json::from_str(&input)
        .unwrap_or_else(|error| panic!("task input does not decode: {error}: {input}"))
}

/// The result envelope that an `args_from` member carries: Python's wrap
/// (`__h_taskresult_envelope__`) holds it in `inner`.
pub(super) fn args_from_inner(
    input: &serde_json::Value,
    key: &str,
) -> TaskResult<serde_json::Value> {
    serde_json::from_value(input[key]["inner"].clone()).unwrap_or_else(|error| {
        panic!("args_from member '{key}' has no inner envelope: {error}: {input}")
    })
}

/// The entry of `node_id` in the workflow context of a task input.
pub(super) fn ctx_result(
    input: &serde_json::Value,
    node_id: &str,
) -> TaskResult<serde_json::Value> {
    serde_json::from_value(input[WORKFLOW_CTX_KWARG]["results_by_id"][node_id].clone())
        .unwrap_or_else(|error| panic!("context has no envelope for '{node_id}': {error}: {input}"))
}

/// The error of a result that must be an error.
pub(super) fn error_of(result: &TaskResult<serde_json::Value>) -> &TaskError {
    match result {
        TaskResult::Err(error) => error,
        TaskResult::Ok(value) => panic!("expected an error, got ok {value}"),
    }
}

/// The error code of a task error, as text.
pub(super) fn code(error: &TaskError) -> Option<String> {
    Some(error.error_code.to_string())
}

/// A stored workflow error (`horsies_workflows.error`).
pub(super) fn stored_error(text: &str) -> TaskError {
    serde_json::from_str(text)
        .unwrap_or_else(|error| panic!("stored workflow error does not decode: {error}: {text}"))
}

/// A stored task result envelope (node `result`, workflow `result`).
pub(super) fn stored_result(text: &str) -> TaskResult<serde_json::Value> {
    serde_json::from_str(text)
        .unwrap_or_else(|error| panic!("stored result does not decode: {error}: {text}"))
}

/// The error of a stored error envelope.
pub(super) fn stored_result_error(text: &str) -> TaskError {
    match stored_result(text) {
        TaskResult::Err(error) => error,
        TaskResult::Ok(value) => panic!("expected an error envelope, got ok {value}"),
    }
}

/// The error code of the stored workflow error, or `None` when it has none.
pub(super) async fn workflow_error_code(pool: &PgPool, workflow_id: Uuid) -> Option<String> {
    workflow(pool, workflow_id)
        .await
        .error
        .as_deref()
        .map(stored_error)
        .as_ref()
        .and_then(code)
}

/// The status and the terminalization kind of a task's history row, or `None`
/// when the task has no history row.
pub(super) async fn history_row(pool: &PgPool, task_id: Uuid) -> Option<(String, String)> {
    sqlx::query_as(
        "SELECT status, terminalization_kind FROM horsies_task_history WHERE task_id = $1",
    )
    .bind(task_id)
    .fetch_optional(pool)
    .await
    .expect("read the history row")
}

/// The request of one claim pass (`horsies_claim`) of [`WORKER`] over
/// `queue`, with no cap: one `ISOLATED` budget of `budget` rows that every
/// class routes to.
pub(super) fn claim_pass(queue: &str, budget: u32) -> crate::broker::claims::ClaimPassRequest {
    use crate::core::task::Execution;
    crate::broker::claims::ClaimPassRequest {
        worker_id: WORKER.to_owned(),
        queues: vec![queue.to_owned()],
        queue_priority: std::iter::once((queue.to_owned(), 100)).collect(),
        queue_max_concurrency: std::collections::HashMap::new(),
        hard_cap_mode: true,
        classes: crate::broker::claims::ClaimClasses::new(
            std::collections::HashMap::from([(Execution::Isolated, budget)]),
            std::collections::HashMap::from([
                (Execution::Isolated, Execution::Isolated),
                (Execution::SharedAsync, Execution::Isolated),
                (Execution::SharedThread, Execution::Isolated),
            ]),
        )
        .expect("every route names the ISOLATED budget"),
        prefetch_buffer: 0,
        max_claim_per_worker: 0,
        max_claim_batch: 0,
        cluster_wide_cap: None,
        claim_lease_ms: std::num::NonZeroU32::new(60_000).expect("a positive lease"),
        lock_keys: Vec::new(),
    }
}

/// Run one claim pass of `request` on a core pool of `pool`, and return the
/// claimed rows in claim order.
pub(super) async fn claim_once(
    pool: &PgPool,
    request: &crate::broker::claims::ClaimPassRequest,
) -> Vec<crate::broker::claims::ClaimedRow> {
    let core = crate::broker::pools::core_pool_for_test_pool(pool);
    let pass = crate::broker::claims::claim_pass(
        &core,
        crate::broker::core_rows::StatementLog::Off,
        request,
    )
    .await;
    core.close();
    pass.expect("claim pass").claimed
}

/// How long a listener collects notifications after the action.
const NOTIFICATION_WAIT: std::time::Duration = std::time::Duration::from_millis(500);

/// Bound of the notifications one collection reads.
const NOTIFICATION_LIMIT: usize = 100;

/// A connection that listens on `workflow_done` (Python's fixture of the same name).
pub(super) struct WorkflowDoneListener {
    listener: sqlx::postgres::PgListener,
}

impl WorkflowDoneListener {
    pub(super) async fn listen(pool: &PgPool) -> Self {
        let mut listener = sqlx::postgres::PgListener::connect_with(pool)
            .await
            .expect("connect the workflow_done listener");
        listener
            .listen("workflow_done")
            .await
            .expect("LISTEN workflow_done");
        Self { listener }
    }

    /// The payloads that arrive within [`NOTIFICATION_WAIT`] of the last one,
    /// at most [`NOTIFICATION_LIMIT`].
    pub(super) async fn received(&mut self) -> Vec<String> {
        let mut payloads = Vec::new();
        while payloads.len() < NOTIFICATION_LIMIT {
            match tokio::time::timeout(NOTIFICATION_WAIT, self.listener.recv()).await {
                Ok(Ok(notification)) => payloads.push(notification.payload().to_owned()),
                Ok(Err(error)) => panic!("the workflow_done listener failed: {error}"),
                Err(_) => break,
            }
        }
        payloads
    }
}

/// A connection that listens on several channels and keeps each
/// notification as (channel, payload).
pub(super) struct Notifications {
    listener: sqlx::postgres::PgListener,
}

impl Notifications {
    pub(super) async fn listen(pool: &PgPool, channels: &[&str]) -> Self {
        let mut listener = sqlx::postgres::PgListener::connect_with(pool)
            .await
            .expect("connect the notification listener");
        listener
            .listen_all(channels.iter().copied())
            .await
            .unwrap_or_else(|error| panic!("LISTEN {channels:?}: {error}"));
        Self { listener }
    }

    /// The (channel, payload) pairs that arrive within [`NOTIFICATION_WAIT`]
    /// of the last one, at most [`NOTIFICATION_LIMIT`], sorted.
    pub(super) async fn received(&mut self) -> Vec<(String, String)> {
        let mut received = Vec::new();
        while received.len() < NOTIFICATION_LIMIT {
            match tokio::time::timeout(NOTIFICATION_WAIT, self.listener.recv()).await {
                Ok(Ok(notification)) => received.push((
                    notification.channel().to_owned(),
                    notification.payload().to_owned(),
                )),
                Ok(Err(error)) => panic!("the notification listener failed: {error}"),
                Err(_) => break,
            }
        }
        received.sort();
        received
    }
}

/// Ends seeded workflows that a test leaves RUNNING or PAUSED on the shared
/// database, so no global pass of another test works on them.
pub(super) async fn end_workflows(pool: &PgPool, workflow_ids: &[Uuid]) {
    sqlx::query(
        "UPDATE horsies_workflows
         SET status = 'CANCELLED', completed_at = NOW(), updated_at = NOW()
         WHERE id = ANY($1) AND status IN ('PENDING', 'RUNNING', 'PAUSED')",
    )
    .bind(workflow_ids)
    .execute(pool)
    .await
    .expect("end the seeded workflows");
}

/// Runs `statement` on its own and checks that it succeeded.
pub(super) async fn execute(pool: &PgPool, statement: &str) {
    sqlx::query(statement)
        .execute(pool)
        .await
        .unwrap_or_else(|error| panic!("test statement failed: {error}: {statement}"));
}
