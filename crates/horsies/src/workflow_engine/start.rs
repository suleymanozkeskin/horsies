use std::future::Future;
use std::pin::Pin;

use sqlx::PgPool;
use uuid::Uuid;

use std::sync::Arc;

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

const INSERT_WORKFLOW_TASK_SQL: &str = "\
INSERT INTO horsies_workflow_tasks (
    id, workflow_id, task_index, node_id, task_name,
    task_args, task_kwargs, queue_name, priority,
    dependencies, args_from, workflow_ctx_from,
    allow_failed_deps, join_type, min_success,
    task_options, status, is_subworkflow,
    sub_workflow_name, sub_definition_key,
    created_at
)
VALUES (
    $1, $2, $3, $4, $5,
    $6, $7, $8, $9,
    $10, $11, $12,
    $13, $14, $15,
    $16, $17, $18,
    $19, $20,
    NOW()
)";

const ENQUEUE_ROOT_TASK_SQL: &str = "\
INSERT INTO horsies_tasks (
    id, task_name, queue_name, priority, args, kwargs,
    status, sent_at, enqueued_at, good_until, max_retries, task_options,
    enqueue_sha, created_at, updated_at
)
VALUES ($1, $2, $3, $4, $5, $6, 'PENDING', NOW(), NOW(), $7, $8, $9, $10, NOW(), NOW())";

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

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Start a workflow from a validated spec.
///
/// Creates the workflow row (RUNNING), inserts all workflow_task rows,
/// and enqueues root tasks (those with no dependencies). Returns a
/// typed handle for result retrieval.
///
/// If a caller-provided `workflow_id` already exists in the database,
/// returns the existing handle (idempotent start), matching Python's
/// behavior.
///
/// All inserts are wrapped in a single database transaction so that a
/// crash mid-start does not leave a partially-created workflow.
pub async fn start_workflow<T>(
    pool: &PgPool,
    spec: &WorkflowSpec,
    workflow_id: Option<String>,
    registry: &WorkflowSpecRegistry,
) -> WorkflowStartResult<WorkflowHandle<T>> {
    let wf_id = workflow_id.unwrap_or_else(|| Uuid::new_v4().to_string());
    let wf_name = spec.name.clone();

    start_workflow_inner(pool, spec, &wf_id, registry)
        .await
        .map_err(|e| WorkflowStartError {
            code: classify_workflow_error(&e),
            message: format!("{}", e),
            retryable: is_retryable_workflow_error(&e),
            workflow_name: wf_name,
            workflow_id: wf_id,
        })
}

/// Start a workflow with automatic retry on transient errors.
///
/// When `resend_on_transient_err` is `true`, retries up to 3 times with
/// exponential backoff (200ms, 400ms, 800ms, capped at 2000ms) on
/// retryable errors. 1 initial attempt + 3 retries = 4 total attempts.
///
/// The workflow_id is generated once and reused across all retry attempts,
/// preserving idempotency. Mirrors Python's `resend_on_transient_err`
/// behavior on `WorkflowSpec.start()`.
pub async fn start_workflow_with_retry<T>(
    pool: &PgPool,
    spec: &WorkflowSpec,
    workflow_id: Option<String>,
    registry: &WorkflowSpecRegistry,
    resend_on_transient_err: bool,
) -> WorkflowStartResult<WorkflowHandle<T>> {
    if !resend_on_transient_err {
        return start_workflow(pool, spec, workflow_id, registry).await;
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

        match start_workflow(pool, spec, Some(wf_id.clone()), registry).await {
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
    pool: &PgPool,
    spec: &WorkflowSpec,
    wf_id: &str,
    registry: &WorkflowSpecRegistry,
) -> Result<WorkflowHandle<T>, WorkflowError> {
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
            pool.clone(),
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
        pool.clone(),
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

/// Retry a failed workflow start using the `WorkflowStartError` from a
/// previous `start_workflow()` call.
///
/// Validates the error before retrying:
/// - Only `ENQUEUE_FAILED` errors are eligible
/// - Cross-workflow retry is rejected (error workflow_name must match spec)
///
/// Best-effort idempotent by workflow_id: if the workflow already exists
/// (e.g., the original start partially succeeded), returns the existing handle.
///
/// Mirrors Python's `WorkflowSpec.retry_start(error)`.
pub async fn retry_start<T>(
    pool: &PgPool,
    spec: &WorkflowSpec,
    error: &WorkflowStartError,
    registry: &WorkflowSpecRegistry,
) -> WorkflowStartResult<WorkflowHandle<T>> {
    // Reuse the existing validation from horsies-core.
    let workflow_id = crate::core::workflow::start_types::validate_start_retry(error, &spec.name)?;
    start_workflow(pool, spec, Some(workflow_id), registry).await
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

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
        for task in tasks {
            let is_root = task.dependencies.is_empty();
            let status = if is_root { "READY" } else { "PENDING" };
            let wt_id = Uuid::new_v4().to_string();

            let join_str = task.join.to_string();
            let args_from_json = if task.args_from.is_empty() {
                None
            } else {
                Some(serde_json::to_value(&task.args_from)?)
            };
            let queue = task.queue.as_deref().ok_or_else(|| {
                WorkflowError::Validation(format!(
                    "node '{}' (task '{}') has no resolved queue; \
                     this indicates a missing resolution step before workflow start",
                    task.node_id.as_deref().unwrap_or("?"),
                    task.task_name,
                ))
            })?;
            let priority = task.priority.unwrap_or(100);

            // Merge good_until into task_options_json (mirrors Python's lifecycle.py).
            let merged_task_options = crate::workflow_engine::merge_good_until_into_options(
                task.good_until,
                task.task_options_json.as_deref(),
            );

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

            sqlx::query(INSERT_WORKFLOW_TASK_SQL)
                .bind(&wt_id)
                .bind(workflow_id)
                .bind(task.index as i32)
                .bind(&task.node_id)
                .bind(&task.task_name)
                .bind(&task.args_json)
                .bind(&task.kwargs_json)
                .bind(queue)
                .bind(priority)
                .bind(
                    task.dependencies
                        .iter()
                        .map(|&d| d as i32)
                        .collect::<Vec<i32>>(),
                )
                .bind(&args_from_json)
                .bind(&task.workflow_ctx_from)
                .bind(task.allow_failed_deps)
                .bind(&join_str)
                .bind(task.min_success)
                .bind(&merged_task_options)
                .bind(status)
                .bind(task.is_subworkflow) // $18
                .bind(&sub_workflow_name) // $19
                .bind(&task.sub_definition_key) // $20
                .execute(&mut **tx)
                .await?;

            // Enqueue root nodes immediately.
            if is_root {
                if task.is_subworkflow {
                    // Root sub-workflow: launch child workflow inline.
                    launch_root_subworkflow(tx, workflow_id, task, registry).await?;
                } else {
                    // Regular root task: enqueue into horsies_tasks.
                    let task_id = enqueue_root_task(
                        &mut **tx,
                        task,
                        queue,
                        priority,
                        merged_task_options.as_deref(),
                    )
                    .await?;
                    // Link workflow_task to the created horsies_tasks row.
                    sqlx::query(LINK_WORKFLOW_TASK_SQL)
                        .bind(&task_id)
                        .bind(workflow_id)
                        .bind(task.index as i32)
                        .execute(&mut **tx)
                        .await?;

                    tracing::debug!(
                        workflow_id,
                        task_index = task.index,
                        task_id = %task_id,
                        "root task enqueued",
                    );
                }
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
    if let Some(ancestor) = ancestors.iter().find(|ancestor| {
        ancestor.name == child_spec.name
            || match (child_key, ancestor.definition_key.as_deref()) {
                (Some(child_key), Some(ancestor_key)) => child_key == ancestor_key,
                _ => false,
            }
    }) {
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

    #[test]
    fn build_child_spec_passes_explicit_kwargs_to_dynamic_definition() {
        let registered = RegisteredWorkflowDefinition {
            name: "child_dynamic".to_owned(),
            definition_key: "tests.child_dynamic.v1".to_owned(),
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
