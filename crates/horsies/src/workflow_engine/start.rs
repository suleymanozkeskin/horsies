use std::future::Future;
use std::pin::Pin;

use uuid::Uuid;

use std::sync::Arc;

use crate::broker::PostgresBroker;
use crate::core::task::retry_utils::parse_max_retries;
use crate::core::{
    AnyNode, OnError, WorkflowSpec, WorkflowSpecRegistry, WorkflowStartError,
    WorkflowStartErrorCode, WorkflowStartResult,
};

use crate::workflow_engine::bound_handle::WorkflowHandle;
use crate::workflow_engine::error::WorkflowError;
use crate::workflow_engine::parse_good_until_from_options;

// ---------------------------------------------------------------------------
// SQL constants
// ---------------------------------------------------------------------------

const CHECK_WORKFLOW_EXISTS_SQL: &str = "\
SELECT id FROM horsies_workflows WHERE id = $1";

const INSERT_WORKFLOW_SQL: &str = "\
INSERT INTO horsies_workflows (
    id, name, status, on_error, output_task_index, success_policy,
    definition_key,
    depth, root_workflow_id, sent_at, created_at, started_at, updated_at
)
VALUES ($1, $2, 'RUNNING', $3, $4, $5, $6, 0, $1, NOW(), NOW(), NOW(), NOW())
ON CONFLICT (id) DO NOTHING
RETURNING id";

const INSERT_CHILD_WORKFLOW_SQL: &str = "\
INSERT INTO horsies_workflows (
    id, name, status, on_error, output_task_index, success_policy,
    definition_key,
    parent_workflow_id, parent_task_index, depth, root_workflow_id,
    sent_at, created_at, started_at, updated_at
)
VALUES ($1, $2, 'RUNNING', $3, $4, $5, $6, $7, $8, $9, $10, NOW(), NOW(), NOW(), NOW())";

const ENQUEUE_ROOT_TASK_SQL: &str = "\
INSERT INTO horsies_tasks (
    id, task_name, queue_name, priority, args, kwargs,
    status, sent_at, enqueued_at, good_until, max_retries, task_options,
    enqueue_sha, is_workflow_task, created_at, updated_at
)
VALUES ($1, $2, $3, $4, $5, $6, 'PENDING', NOW(), NOW(), $7, $8, $9, $10, TRUE, NOW(), NOW())";

const LINK_WORKFLOW_TASK_SQL: &str = "\
UPDATE horsies_workflow_tasks
SET task_id = $1, status = 'ENQUEUED', started_at = NOW()
WHERE workflow_id = $2 AND task_index = $3";

const LINK_ROOT_SUBWORKFLOW_SQL: &str = "\
UPDATE horsies_workflow_tasks
SET sub_workflow_id = $1, status = 'ENQUEUED', started_at = NOW()
WHERE workflow_id = $2 AND task_index = $3
  AND status = 'READY'";

/// Get parent depth and root workflow ID (used by root sub-workflow launch at start).
const GET_WORKFLOW_DEPTH_SQL: &str = "\
SELECT depth, root_workflow_id
FROM horsies_workflows
WHERE id = $1";

const CHECK_ANCESTOR_WORKFLOW_CHAIN_SQL: &str = "\
WITH RECURSIVE ancestors AS (
    SELECT id, name, definition_key, parent_workflow_id
    FROM horsies_workflows
    WHERE id = $1
  UNION ALL
    SELECT w.id, w.name, w.definition_key, w.parent_workflow_id
    FROM horsies_workflows w
    JOIN ancestors a ON a.parent_workflow_id = w.id
)
SELECT id, name, definition_key FROM ancestors";

// ---------------------------------------------------------------------------
// Row types
// ---------------------------------------------------------------------------

#[derive(Debug, sqlx::FromRow)]
#[allow(dead_code)]
struct ExistsRow {
    id: String,
}

#[derive(Debug, sqlx::FromRow)]
#[allow(dead_code)]
struct DepthRow {
    depth: Option<i32>,
    root_workflow_id: Option<String>,
}

#[derive(Debug, sqlx::FromRow)]
struct AncestorWorkflowRow {
    id: String,
    name: String,
    definition_key: Option<String>,
}

/// A workflow_task row prepared in memory for batched insertion.
///
/// Holds the new identifiers and the per-node values derived once during the
/// prepare phase, so the bulk inserts and the per-row slow path read them without
/// recomputing. Parity with horsies PR #126.
struct PreparedNode<'a> {
    node: &'a AnyNode,
    wt_id: String,
    queue: String,
    priority: i32,
    args_from_json: Option<serde_json::Value>,
    join_str: String,
    merged_task_options: Option<String>,
    sub_workflow_name: Option<String>,
    status: &'static str,
    /// `Some` only for fast-path roots — the `horsies_tasks` id, also written to
    /// the workflow_task's `task_id` so the two link without a follow-up UPDATE.
    task_id: Option<String>,
    is_root: bool,
    fast_path: bool,
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Start a workflow. The returned handle reuses the broker's shared listener
/// for result waits (P2).
pub async fn start_workflow<T>(
    broker: &Arc<PostgresBroker>,
    spec: &WorkflowSpec,
    workflow_id: Option<String>,
    registry: &WorkflowSpecRegistry,
) -> WorkflowStartResult<WorkflowHandle<T>> {
    let wf_id = workflow_id.unwrap_or_else(|| Uuid::new_v4().to_string());
    let wf_name = spec.name.clone();

    start_workflow_inner(broker, spec, &wf_id, registry)
        .await
        .map_err(|e| WorkflowStartError {
            code: classify_workflow_error(&e),
            message: format!("{}", e),
            retryable: is_retryable_workflow_error(&e),
            workflow_name: wf_name,
            workflow_id: wf_id,
        })
}

/// Start a workflow with retry and a separate session-capable pool for result
/// LISTEN/NOTIFY handles.
pub async fn start_workflow_with_retry<T>(
    broker: &Arc<PostgresBroker>,
    spec: &WorkflowSpec,
    workflow_id: Option<String>,
    registry: &WorkflowSpecRegistry,
    resend_on_transient_err: bool,
) -> WorkflowStartResult<WorkflowHandle<T>> {
    if !resend_on_transient_err {
        return start_workflow(broker, spec, workflow_id, registry).await;
    }

    // 1 initial attempt + START_RETRY_COUNT retries = 4 total attempts.
    // Matches Python's `1 + _START_RETRY_COUNT`.
    const START_RETRY_COUNT: u32 = 3;
    const START_RETRY_INITIAL_MS: u64 = 200;
    const START_RETRY_MAX_MS: u64 = 2000;

    let max_attempts = 1 + START_RETRY_COUNT;
    let wf_id = workflow_id.unwrap_or_else(|| Uuid::new_v4().to_string());
    let mut last_err: Option<WorkflowStartError> = None;

    for attempt in 0..max_attempts {
        if attempt > 0 {
            if let Some(ref err) = last_err {
                if !err.retryable {
                    return Err(last_err
                        .take()
                        .expect("retry loop should have recorded the previous start error"));
                }
            }
            let delay_ms = (START_RETRY_INITIAL_MS * 2u64.pow(attempt - 1)).min(START_RETRY_MAX_MS);
            tracing::warn!(
                workflow_name = %spec.name,
                workflow_id = %wf_id,
                attempt = attempt + 1,
                delay_ms,
                "workflow start failed (retryable), retrying",
            );
            tokio::time::sleep(std::time::Duration::from_millis(delay_ms)).await;
        }

        match start_workflow(broker, spec, Some(wf_id.clone()), registry).await {
            Ok(handle) => return Ok(handle),
            Err(e) => {
                if e.retryable && attempt < max_attempts - 1 {
                    last_err = Some(e);
                    continue;
                }
                return Err(e);
            }
        }
    }

    Err(last_err.expect("loop ran at least once"))
}

async fn start_workflow_inner<T>(
    broker: &Arc<PostgresBroker>,
    spec: &WorkflowSpec,
    wf_id: &str,
    registry: &WorkflowSpecRegistry,
) -> Result<WorkflowHandle<T>, WorkflowError> {
    let pool = broker.pool();
    // Idempotent start: if a caller-provided ID already exists, return
    // the existing handle without creating a new workflow.
    let existing: Option<ExistsRow> = sqlx::query_as(CHECK_WORKFLOW_EXISTS_SQL)
        .bind(wf_id)
        .fetch_optional(pool)
        .await?;

    if existing.is_some() {
        tracing::warn!(
            workflow_id = %wf_id,
            "workflow already exists, returning existing handle",
        );
        return Ok(WorkflowHandle::new(
            wf_id.to_owned(),
            Arc::clone(broker),
            Arc::new(registry.clone()),
        ));
    }

    let on_error_str = match spec.on_error {
        OnError::Fail => "fail",
        OnError::Pause => "pause",
    };

    let success_policy_json = spec
        .success_policy
        .as_ref()
        .map(serde_json::to_value)
        .transpose()?;

    // Wrap the entire start operation in a transaction.
    let mut tx = pool.begin().await?;

    // Insert workflow row.
    sqlx::query(INSERT_WORKFLOW_SQL)
        .bind(wf_id)
        .bind(&spec.name)
        .bind(on_error_str)
        .bind(spec.output_index.map(|i| i as i32))
        .bind(&success_policy_json)
        .bind(&spec.definition_key)
        .execute(&mut *tx)
        .await?;

    tracing::debug!(workflow_id = %wf_id, name = %spec.name, "workflow created");

    // Insert all workflow_task rows and enqueue roots.
    insert_workflow_tasks(&mut tx, wf_id, &spec.tasks, registry).await?;

    tx.commit().await?;

    Ok(WorkflowHandle::new(
        wf_id.to_owned(),
        Arc::clone(broker),
        Arc::new(registry.clone()),
    ))
}

fn classify_workflow_error(e: &WorkflowError) -> WorkflowStartErrorCode {
    match e {
        WorkflowError::Serialization(_) | WorkflowError::Validation(_) => {
            WorkflowStartErrorCode::ValidationFailed
        }
        WorkflowError::Database(_) | WorkflowError::Broker(_) => {
            WorkflowStartErrorCode::EnqueueFailed
        }
        _ => WorkflowStartErrorCode::InternalFailed,
    }
}

fn is_retryable_workflow_error(e: &WorkflowError) -> bool {
    match e {
        WorkflowError::Database(sqlx_err) => crate::broker::is_retryable_sqlx_error(sqlx_err),
        WorkflowError::Broker(broker_err) => broker_err.is_retryable(),
        _ => false,
    }
}

/// Retry a failed workflow start using the broker.
pub async fn retry_start<T>(
    broker: &Arc<PostgresBroker>,
    spec: &WorkflowSpec,
    error: &WorkflowStartError,
    registry: &WorkflowSpecRegistry,
) -> WorkflowStartResult<WorkflowHandle<T>> {
    // Reuse the existing validation from horsies-core.
    let workflow_id = crate::core::workflow::start_types::validate_start_retry(error, &spec.name)?;
    start_workflow(broker, spec, Some(workflow_id), registry).await
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

/// Resolve the claimable priority for a node about to be persisted.
///
/// A real task node must already have a resolved priority by insert time —
/// `resolve_node_queue_priority` runs at workflow registration, child-spec
/// materialization, and `check()`, filling it from the queue config. A `None`
/// here means a resolution step was skipped, so it is surfaced (fail-closed)
/// rather than silently persisting a wrong literal — mirroring the queue check
/// in `insert_workflow_tasks` and Python's `WORKFLOW_UNRESOLVED_PRIORITY`.
///
/// Sub-workflow nodes are exempt: their `horsies_workflow_tasks` row is inert
/// bookkeeping (never claimed as a task; `ENQUEUE_SUBWORKFLOW_TASK` copies no
/// priority), and `resolve_node_queue_priority` intentionally skips them, so
/// they keep the historical `100` default.
fn resolve_insert_priority(node: &AnyNode) -> Result<i32, WorkflowError> {
    if node.is_subworkflow {
        return Ok(node.priority.unwrap_or(100));
    }
    node.priority.ok_or_else(|| {
        WorkflowError::Validation(format!(
            "node '{}' (task '{}') has no resolved priority; \
             this indicates a missing resolution step before workflow start",
            node.node_id.as_deref().unwrap_or("?"),
            node.task_name,
        ))
    })
}

/// Insert all workflow_task rows. Root tasks (no deps) are marked READY
/// and immediately enqueued into horsies_tasks. Root sub-workflow nodes
/// are detected and launched as child workflows.
///
/// Returns a boxed future because this function is mutually recursive with
/// `launch_root_subworkflow` → `start_child_workflow_in_tx`.
fn insert_workflow_tasks<'a>(
    tx: &'a mut sqlx::Transaction<'_, sqlx::Postgres>,
    workflow_id: &'a str,
    tasks: &'a [AnyNode],
    registry: &'a WorkflowSpecRegistry,
) -> Pin<Box<dyn Future<Output = Result<(), WorkflowError>> + Send + 'a>> {
    Box::pin(async move {
        if tasks.is_empty() {
            return Ok(());
        }

        // ── Phase 1: prepare every row in memory ──
        let mut prepared: Vec<PreparedNode> = Vec::with_capacity(tasks.len());
        for task in tasks {
            let is_root = task.dependencies.is_empty();
            let queue = task.queue.as_deref().ok_or_else(|| {
                WorkflowError::Validation(format!(
                    "node '{}' (task '{}') has no resolved queue; \
                     this indicates a missing resolution step before workflow start",
                    task.node_id.as_deref().unwrap_or("?"),
                    task.task_name,
                ))
            })?;
            let priority = resolve_insert_priority(task)?;

            // Merge good_until into task_options_json (mirrors Python's lifecycle.py).
            let merged_task_options = crate::workflow_engine::merge_good_until_into_options(
                task.good_until,
                task.task_options_json.as_deref(),
            );

            let args_from_json = if task.args_from.is_empty() {
                None
            } else {
                Some(serde_json::to_value(&task.args_from)?)
            };

            // Extract sub_workflow_name from task_name for sub-workflow nodes.
            let sub_workflow_name: Option<String> = if task.is_subworkflow {
                Some(
                    task.task_name
                        .strip_prefix("__sub_workflow:")
                        .unwrap_or(&task.task_name)
                        .to_owned(),
                )
            } else {
                None
            };

            // Fast-path roots: plain TaskNodes with no inbound args/ctx. They are
            // inserted directly as ENQUEUED with a task_id (no follow-up LINK) and
            // their horsies_tasks rows are bulk-inserted. Skipping the per-row CAS
            // is sound: the node rows are created in this same uncommitted tx, so no
            // concurrent transaction can observe or race them.
            let has_ctx_from = task.workflow_ctx_from.as_ref().is_some_and(|v| !v.is_empty());
            let fast_path =
                is_root && !task.is_subworkflow && task.args_from.is_empty() && !has_ctx_from;
            let task_id = if fast_path {
                Some(Uuid::new_v4().to_string())
            } else {
                None
            };
            let status = if fast_path {
                "ENQUEUED"
            } else if is_root {
                "READY"
            } else {
                "PENDING"
            };

            prepared.push(PreparedNode {
                node: task,
                wt_id: Uuid::new_v4().to_string(),
                queue: queue.to_owned(),
                priority,
                args_from_json,
                join_str: task.join.to_string(),
                merged_task_options,
                sub_workflow_name,
                status,
                task_id,
                is_root,
                fast_path,
            });
        }

        // ── Phase 2: bulk-insert all workflow_task rows ──
        let mut qb: sqlx::QueryBuilder<sqlx::Postgres> = sqlx::QueryBuilder::new(
            "INSERT INTO horsies_workflow_tasks (\
             id, workflow_id, task_index, node_id, task_name, \
             task_args, task_kwargs, queue_name, priority, \
             dependencies, args_from, workflow_ctx_from, \
             allow_failed_deps, join_type, min_success, \
             task_options, status, is_subworkflow, \
             sub_workflow_name, sub_definition_key, \
             task_id, started_at, created_at) ",
        );
        qb.push_values(prepared.iter(), |mut b, p| {
            let deps: Vec<i32> = p.node.dependencies.iter().map(|&d| d as i32).collect();
            b.push_bind(p.wt_id.clone())
                .push_bind(workflow_id.to_owned())
                .push_bind(p.node.index as i32)
                .push_bind(p.node.node_id.clone())
                .push_bind(p.node.task_name.clone())
                .push_bind(p.node.args_json.clone())
                .push_bind(p.node.kwargs_json.clone())
                .push_bind(p.queue.clone())
                .push_bind(p.priority)
                .push_bind(deps)
                .push_bind(p.args_from_json.clone())
                .push_bind(p.node.workflow_ctx_from.clone())
                .push_bind(p.node.allow_failed_deps)
                .push_bind(p.join_str.clone())
                .push_bind(p.node.min_success)
                .push_bind(p.merged_task_options.clone())
                .push_bind(p.status)
                .push_bind(p.node.is_subworkflow)
                .push_bind(p.sub_workflow_name.clone())
                .push_bind(p.node.sub_definition_key.clone())
                .push_bind(p.task_id.clone());
            // started_at: NOW() for the ENQUEUED fast path, NULL otherwise.
            if p.fast_path {
                b.push("NOW()");
            } else {
                b.push("NULL");
            }
            b.push("NOW()"); // created_at
        });
        qb.build().execute(&mut **tx).await?;

        // ── Phase 3: bulk-insert fast-path roots' horsies_tasks rows ──
        if prepared.iter().any(|p| p.fast_path) {
            let mut tq: sqlx::QueryBuilder<sqlx::Postgres> = sqlx::QueryBuilder::new(
                "INSERT INTO horsies_tasks (\
                 id, task_name, queue_name, priority, args, kwargs, \
                 status, sent_at, enqueued_at, good_until, max_retries, task_options, \
                 enqueue_sha, is_workflow_task, created_at, updated_at) ",
            );
            tq.push_values(prepared.iter().filter(|p| p.fast_path), |mut b, p| {
                let task_id = p
                    .task_id
                    .clone()
                    .expect("fast-path root always has a task_id");
                let max_retries = parse_max_retries(p.merged_task_options.as_deref());
                let good_until = parse_good_until_from_options(p.merged_task_options.as_deref());
                let enqueue_sha = format!("wf-{}", task_id);
                b.push_bind(task_id)
                    .push_bind(p.node.task_name.clone())
                    .push_bind(p.queue.clone())
                    .push_bind(p.priority)
                    .push_bind(p.node.args_json.clone())
                    .push_bind(p.node.kwargs_json.clone());
                // status, sent_at, enqueued_at as literals — NOW() is transaction-
                // stable, so claim ordering matches the per-row shape.
                b.push("'PENDING'").push("NOW()").push("NOW()");
                b.push_bind(good_until)
                    .push_bind(max_retries)
                    .push_bind(p.merged_task_options.clone())
                    .push_bind(enqueue_sha);
                b.push("TRUE").push("NOW()").push("NOW()"); // is_workflow_task, created_at, updated_at
            });
            tq.build().execute(&mut **tx).await?;
        }

        // ── Phase 4: per-row slow path (subworkflow roots, args_from/ctx_from roots) ──
        // Node rows already exist (Phase 2, status READY); these promote them.
        for p in &prepared {
            if !p.is_root || p.fast_path {
                continue;
            }
            if p.node.is_subworkflow {
                launch_root_subworkflow(tx, workflow_id, p.node, registry).await?;
            } else {
                let task_id = enqueue_root_task(
                    &mut **tx,
                    p.node,
                    &p.queue,
                    p.priority,
                    p.merged_task_options.as_deref(),
                )
                .await?;
                sqlx::query(LINK_WORKFLOW_TASK_SQL)
                    .bind(&task_id)
                    .bind(workflow_id)
                    .bind(p.node.index as i32)
                    .execute(&mut **tx)
                    .await?;

                tracing::debug!(
                    workflow_id,
                    task_index = p.node.index,
                    task_id = %task_id,
                    "root task enqueued (slow path)",
                );
            }
        }

        Ok(())
    }) // end Box::pin
}

/// Create a horsies_tasks row for a root workflow task.
/// Returns the task_id.
async fn enqueue_root_task(
    executor: impl sqlx::Executor<'_, Database = sqlx::Postgres>,
    node: &AnyNode,
    queue: &str,
    priority: i32,
    merged_task_options: Option<&str>,
) -> Result<String, WorkflowError> {
    let task_id = Uuid::new_v4().to_string();
    let max_retries = parse_max_retries(merged_task_options);

    let enqueue_sha = format!("wf-{}", task_id);

    sqlx::query(ENQUEUE_ROOT_TASK_SQL)
        .bind(&task_id)
        .bind(&node.task_name)
        .bind(queue)
        .bind(priority)
        .bind(&node.args_json)
        .bind(&node.kwargs_json)
        .bind(parse_good_until_from_options(merged_task_options))
        .bind(max_retries)
        .bind(merged_task_options)
        .bind(&enqueue_sha)
        .execute(executor)
        .await?;

    Ok(task_id)
}

/// Launch a child workflow for a root sub-workflow node at start time.
///
/// Resolves the spec from the registry and starts the child workflow,
/// linking the parent workflow_task to it.
async fn launch_root_subworkflow(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    workflow_id: &str,
    task: &AnyNode,
    registry: &WorkflowSpecRegistry,
) -> Result<(), WorkflowError> {
    let spec_name = task
        .task_name
        .strip_prefix("__sub_workflow:")
        .unwrap_or(&task.task_name);

    // Get parent depth and root_workflow_id.
    let depth_row: DepthRow = sqlx::query_as(GET_WORKFLOW_DEPTH_SQL)
        .bind(workflow_id)
        .fetch_one(&mut **tx)
        .await?;

    let parent_depth = depth_row.depth.unwrap_or(0);
    let root_wf_id = depth_row.root_workflow_id.as_deref().unwrap_or(workflow_id);

    // Build the child spec (supports dynamic parameterization if a builder is registered).
    let child_spec = build_child_spec(
        spec_name,
        task.sub_definition_key.as_deref(),
        task,
        registry,
    )?;

    // Start the child workflow within the same transaction.
    let child_id = start_child_workflow_in_tx(
        tx,
        &child_spec,
        workflow_id,
        task.index as i32,
        parent_depth + 1,
        root_wf_id,
        registry,
    )
    .await?;

    // Link parent workflow_task to child workflow.
    sqlx::query(LINK_ROOT_SUBWORKFLOW_SQL)
        .bind(&child_id)
        .bind(workflow_id)
        .bind(task.index as i32)
        .execute(&mut **tx)
        .await?;

    tracing::debug!(
        workflow_id,
        task_index = task.index,
        child_workflow_id = %child_id,
        spec_name,
        "root sub-workflow launched at start",
    );

    Ok(())
}

// ---------------------------------------------------------------------------
// Child workflow start
// ---------------------------------------------------------------------------

/// Start a child workflow within an existing transaction.
///
/// Used by `launch_root_subworkflow` and `enqueue_subworkflow_task` to keep
/// child creation + parent link inside a single transaction.
pub async fn start_child_workflow_in_tx(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    spec: &WorkflowSpec,
    parent_workflow_id: &str,
    parent_task_index: i32,
    depth: i32,
    root_workflow_id: &str,
    registry: &WorkflowSpecRegistry,
) -> Result<String, WorkflowError> {
    let child_id = Uuid::new_v4().to_string();
    ensure_no_runtime_subworkflow_cycle(tx, parent_workflow_id, spec).await?;

    let on_error_str = match spec.on_error {
        OnError::Fail => "fail",
        OnError::Pause => "pause",
    };

    let success_policy_json = spec
        .success_policy
        .as_ref()
        .map(serde_json::to_value)
        .transpose()?;

    sqlx::query(INSERT_CHILD_WORKFLOW_SQL)
        .bind(&child_id)
        .bind(&spec.name)
        .bind(on_error_str)
        .bind(spec.output_index.map(|i| i as i32))
        .bind(&success_policy_json)
        .bind(&spec.definition_key)
        .bind(parent_workflow_id)
        .bind(parent_task_index)
        .bind(depth)
        .bind(root_workflow_id)
        .execute(&mut **tx)
        .await?;

    tracing::debug!(
        child_workflow_id = %child_id,
        parent_workflow_id,
        parent_task_index,
        depth,
        name = %spec.name,
        "child workflow created (in transaction)",
    );

    // Insert all workflow_task rows and enqueue roots within same tx.
    insert_workflow_tasks(tx, &child_id, &spec.tasks, registry).await?;

    Ok(child_id)
}

/// Find the ancestor that the child workflow would close a cycle through, if any.
///
/// Identity is keyed by `definition_key` when both the child and ancestor have
/// one: distinct keys are distinct workflows even if their `name`s collide
/// (parity with horsies PR #33). Only when a key is absent do we fall back to
/// `name` matching, for incomplete definitions.
fn subworkflow_cycle_ancestor<'a>(
    ancestors: &'a [AncestorWorkflowRow],
    child_name: &str,
    child_key: Option<&str>,
) -> Option<&'a AncestorWorkflowRow> {
    ancestors.iter().find(|ancestor| {
        match (child_key, ancestor.definition_key.as_deref()) {
            (Some(ck), Some(ak)) => ck == ak,
            _ => ancestor.name == child_name,
        }
    })
}

async fn ensure_no_runtime_subworkflow_cycle(
    tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
    parent_workflow_id: &str,
    child_spec: &WorkflowSpec,
) -> Result<(), WorkflowError> {
    let ancestors: Vec<AncestorWorkflowRow> = sqlx::query_as(CHECK_ANCESTOR_WORKFLOW_CHAIN_SQL)
        .bind(parent_workflow_id)
        .fetch_all(&mut **tx)
        .await?;

    let child_key = child_spec.definition_key.as_deref();
    if let Some(ancestor) = subworkflow_cycle_ancestor(&ancestors, &child_spec.name, child_key) {
        return Err(WorkflowError::Validation(format!(
            "starting child workflow '{}' would create a nested workflow cycle through ancestor '{}' ({})",
            child_spec.name, ancestor.name, ancestor.id,
        )));
    }

    Ok(())
}

/// Build a child workflow spec, using the spec_builder callback if available,
/// otherwise falling back to the static registered spec.
///
/// This enables dynamic sub-workflow parameterization (Gap 14-7): the
/// parent task's args/kwargs are passed to the builder so the child DAG
/// can vary based on upstream results.
///
/// For dynamically built specs, resolves task retry options from the
/// registry's `task_options_map` so workflow tasks inherit their
/// registered retry configuration.
pub(crate) fn build_child_spec(
    spec_name: &str,
    definition_key: Option<&str>,
    parent_task: &AnyNode,
    registry: &WorkflowSpecRegistry,
) -> Result<WorkflowSpec, WorkflowError> {
    let has_child_inputs = parent_task.args_json.is_some()
        || parent_task.kwargs_json.is_some()
        || !parent_task.args_from.is_empty();
    let resolved = registry
        .resolve_child_registration(spec_name, definition_key)
        .ok_or_else(|| WorkflowError::WorkflowNotFound {
            workflow_id: format!(
                "sub-workflow spec not found (definition_key={:?}, name='{}')",
                definition_key, spec_name,
            ),
        })?;

    materialize_child_spec(
        resolved,
        has_child_inputs,
        parent_task.args_json.as_deref(),
        parent_task.kwargs_json.as_deref(),
        registry,
    )
}

pub(crate) fn materialize_child_spec(
    resolved: crate::core::registry::workflow::ResolvedChildWorkflow<'_>,
    has_child_inputs: bool,
    args_json: Option<&str>,
    kwargs_json: Option<&str>,
    registry: &WorkflowSpecRegistry,
) -> Result<WorkflowSpec, WorkflowError> {
    match resolved {
        crate::core::registry::workflow::ResolvedChildWorkflow::Dynamic(registered) => {
            let mut spec = (registered.spec_builder)(args_json, kwargs_json).map_err(|e| {
                WorkflowError::Validation(format!(
                    "spec_builder for '{}' failed: {}",
                    registered.name, e,
                ))
            })?;
            registry.resolve_spec_task_options(&mut spec);
            registry
                .resolve_and_validate_spec(&mut spec)
                .map_err(|e| WorkflowError::Validation(e.to_string()))?;
            Ok(spec)
        }
        crate::core::registry::workflow::ResolvedChildWorkflow::Static(registered) => {
            if let Some(ref builder) = registered.spec_builder {
                let mut spec = builder(args_json, kwargs_json).map_err(|e| {
                    WorkflowError::Validation(format!(
                        "spec_builder for '{}' failed: {}",
                        registered.spec.name, e,
                    ))
                })?;
                registry.resolve_spec_task_options(&mut spec);
                registry
                    .resolve_and_validate_spec(&mut spec)
                    .map_err(|e| WorkflowError::Validation(e.to_string()))?;
                Ok(spec)
            } else if has_child_inputs {
                Err(WorkflowError::Validation(format!(
                    "child workflow '{}' received params from its parent, but the registered child has no spec_builder",
                    registered.spec.name,
                )))
            } else {
                Ok(registered.spec.clone())
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use crate::core::registry::workflow::{RegisteredWorkflowDefinition, RegisteredWorkflowSpec};
    use crate::core::workflow::spec::WorkflowSpecBuilder;
    use crate::core::workflow::sub_workflow::SubWorkflowNode;
    use crate::TaskNode;

    fn child_registered_static() -> RegisteredWorkflowSpec {
        let mut builder = WorkflowSpecBuilder::new("child_static");
        let node = builder.task(TaskNode::<String>::raw("hello_task").queue("default"));
        builder.output(node);
        RegisteredWorkflowSpec {
            spec: builder.build().unwrap(),
            spec_builder: None,
        }
    }

    fn ancestor(name: &str, key: Option<&str>) -> AncestorWorkflowRow {
        AncestorWorkflowRow {
            id: format!("id-{name}-{}", key.unwrap_or("none")),
            name: name.to_owned(),
            definition_key: key.map(str::to_owned),
        }
    }

    fn node_with(is_subworkflow: bool, priority: Option<i32>) -> AnyNode {
        AnyNode {
            task_name: "t".to_owned(),
            args_json: None,
            kwargs_json: None,
            dependencies: vec![],
            args_from: std::collections::HashMap::new(),
            workflow_ctx_from: None,
            workflow_ctx_from_refs: None,
            queue: Some("default".to_owned()),
            priority,
            allow_failed_deps: false,
            join: crate::core::workflow::node::JoinType::All,
            min_success: None,
            good_until: None,
            index: 0,
            node_id: Some("n".to_owned()),
            task_options_json: None,
            is_subworkflow,
            sub_definition_key: None,
        }
    }

    #[test]
    fn resolve_insert_priority_returns_resolved_task_priority() {
        assert_eq!(
            resolve_insert_priority(&node_with(false, Some(30))).unwrap(),
            30
        );
    }

    #[test]
    fn resolve_insert_priority_fails_closed_for_unresolved_task() {
        let err = resolve_insert_priority(&node_with(false, None)).unwrap_err();
        assert!(
            matches!(err, WorkflowError::Validation(ref m) if m.contains("no resolved priority")),
            "expected fail-closed unresolved-priority error, got: {:?}",
            err
        );
    }

    #[test]
    fn resolve_insert_priority_defaults_subworkflow_bookkeeping() {
        // Subworkflow node with no priority keeps the inert 100 default.
        assert_eq!(
            resolve_insert_priority(&node_with(true, None)).unwrap(),
            100
        );
    }

    #[test]
    fn resolve_insert_priority_preserves_explicit_subworkflow_priority() {
        assert_eq!(
            resolve_insert_priority(&node_with(true, Some(50))).unwrap(),
            50
        );
    }

    #[test]
    fn cycle_same_name_distinct_keys_is_not_a_cycle() {
        // Parity with horsies PR #33: distinct definition_keys are distinct
        // workflows even if their names collide.
        let ancestors = vec![ancestor("shared", Some("key_parent"))];
        assert!(
            subworkflow_cycle_ancestor(&ancestors, "shared", Some("key_child")).is_none(),
            "same name + distinct keys must not be flagged as a cycle",
        );
    }

    #[test]
    fn cycle_same_key_is_detected() {
        let ancestors = vec![ancestor("any_name", Some("key_a"))];
        let hit = subworkflow_cycle_ancestor(&ancestors, "other_name", Some("key_a"));
        assert!(hit.is_some(), "matching definition_key must be a cycle");
    }

    #[test]
    fn cycle_missing_keys_falls_back_to_name() {
        // Incomplete definitions (no key) still detect a cycle by name.
        let ancestors = vec![ancestor("recursive", None)];
        assert!(subworkflow_cycle_ancestor(&ancestors, "recursive", None).is_some());
        // A keyed child against an unkeyed same-name ancestor also falls back.
        assert!(subworkflow_cycle_ancestor(&ancestors, "recursive", Some("key_c")).is_some());
    }

    #[test]
    fn cycle_distinct_name_and_key_is_not_a_cycle() {
        let ancestors = vec![ancestor("parent", Some("key_p"))];
        assert!(subworkflow_cycle_ancestor(&ancestors, "child", Some("key_c")).is_none());
    }

    #[test]
    fn build_child_spec_passes_explicit_kwargs_to_dynamic_definition() {
        let registered = RegisteredWorkflowDefinition {
            name: "child_dynamic".to_owned(),
            definition_key: "tests.child_dynamic.v1".to_owned(),
            declared_children: vec![],
            spec_builder: Arc::new(|_args_json, kwargs_json| {
                let kwargs = serde_json::from_str::<serde_json::Value>(kwargs_json.unwrap())
                    .map_err(|e| crate::HorsiesError::new(e.to_string()))?;
                let region = kwargs["region"].as_str().unwrap().to_owned();

                let mut builder = WorkflowSpecBuilder::new("child_dynamic");
                let node = builder.task(
                    TaskNode::<String>::raw("hello_task")
                        .queue("default")
                        .args_json(serde_json::to_string(&region).unwrap()),
                );
                builder.output(node);
                builder.build()
            }),
        };

        let parent = SubWorkflowNode::<serde_json::Value, String>::typed("child_dynamic")
            .definition_key("tests.child_dynamic.v1")
            .kwargs_json(r#"{"region":"eu"}"#)
            .into_any_node(0);

        let mut registry = WorkflowSpecRegistry::new();
        registry.register_definition(registered).unwrap();
        let child = build_child_spec(
            "child_dynamic",
            parent.sub_definition_key.as_deref(),
            &parent,
            &registry,
        )
        .unwrap();
        assert_eq!(child.tasks[0].args_json.as_deref(), Some("\"eu\""));
    }

    #[test]
    fn build_child_spec_rejects_explicit_input_without_builder() {
        let registered = child_registered_static();
        let parent = SubWorkflowNode::<String, String>::typed("child_static")
            .set_input("eu".to_owned())
            .unwrap()
            .into_any_node(0);

        let mut registry = WorkflowSpecRegistry::new();
        registry.register(registered).unwrap();
        let err = build_child_spec("child_static", None, &parent, &registry).unwrap_err();
        match err {
            WorkflowError::Validation(message) => {
                assert!(message.contains("received params from its parent"));
                assert!(message.contains("has no spec_builder"));
            }
            other => panic!("unexpected error: {}", other),
        }
    }
}
