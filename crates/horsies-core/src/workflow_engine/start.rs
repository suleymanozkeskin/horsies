use chrono::{DateTime, Utc};
use uuid::Uuid;

use std::sync::Arc;

use crate::broker::PostgresBroker;
use crate::core::config::payload::PayloadPolicy;
use crate::core::config::retention::RetentionConfig;
use crate::core::history::enqueue::{prepare_workflow_task_facts, WorkflowTaskFacts};
use crate::core::workflow::node::SubWorkflowParams;
use crate::core::{
    AnyNode, ErrorCategory, HorsiesError, OperationalErrorCode, SpecBuildError, TaskError,
    WorkflowSpec, WorkflowSpecRegistry, WorkflowStartError, WorkflowStartErrorCode,
    WorkflowStartResult,
};

use crate::db::CheckoutClass;
use crate::workflow_engine::bound_handle::WorkflowHandle;
use crate::workflow_engine::error::WorkflowError;
use crate::workflow_engine::start_statement::{start_arguments, start_tree, StartOutcome};
use crate::workflow_engine::steps::{advance_parents, ChildBuild};
use crate::workflow_engine::{node_good_until, node_max_retries};

// ---------------------------------------------------------------------------
// Row types
// ---------------------------------------------------------------------------

/// What the insert of a top-level workflow row found.
#[derive(Debug, Clone, PartialEq, Eq)]
enum WorkflowRowInsert {
    /// The row is new; the start goes on in the same transaction.
    Inserted,
    /// A workflow with this id and this name exists. The start writes nothing and returns its handle.
    Existing,
    /// A workflow with this id exists under another name. The start writes nothing and is refused.
    IdConflict { existing_name: String },
}

/// The typed facts that a task node row stores, parsed once at workflow start
/// (`horsies_workflow_tasks_task_facts_by_kind`). The task row minted from the
/// node carries the same values.
pub(crate) struct NodeTaskFacts {
    pub(crate) max_retries: i32,
    pub(crate) good_until: Option<DateTime<Utc>>,
    pub(crate) facts: WorkflowTaskFacts,
}

/// Parse the typed facts of a task node from its merged task options and the
/// retention class key of its queue (`None`: forever).
pub(crate) fn node_task_facts(
    node: &AnyNode,
    merged_task_options: Option<&str>,
    queue_class_key: Option<&str>,
) -> Result<NodeTaskFacts, WorkflowError> {
    let facts = prepare_workflow_task_facts(&node.input_json, merged_task_options, queue_class_key)
        .map_err(|error| WorkflowError::Validation(error.to_string()))?;
    Ok(NodeTaskFacts {
        max_retries: node_max_retries(node.index, &node.task_name, merged_task_options)?,
        good_until: node_good_until(node.index, &node.task_name, merged_task_options)?,
        facts,
    })
}

// ---------------------------------------------------------------------------
// Public API
// ---------------------------------------------------------------------------

/// Start a workflow. The returned handle reuses the broker's shared listener
/// for result waits (P2).
pub async fn start_workflow<T>(
    broker: &Arc<PostgresBroker>,
    spec: &WorkflowSpec,
    workflow_id: Option<Uuid>,
    registry: &WorkflowSpecRegistry,
    payload: &PayloadPolicy,
    retention: &RetentionConfig,
) -> WorkflowStartResult<WorkflowHandle<T>> {
    let wf_id = workflow_id.unwrap_or_else(Uuid::new_v4);
    start_workflow_attempt(
        broker,
        spec,
        wf_id,
        chrono::Utc::now(),
        registry,
        payload,
        retention,
    )
    .await
}

/// One start attempt with a fixed id and start time.
///
/// - Outcomes: the handle of the new workflow; the handle of an existing workflow with this id and name
///   (nothing written); `WORKFLOW_ID_CONFLICT` when the id belongs to another workflow name (nothing written);
///   `VALIDATION_FAILED` for a spec with no definition key (nothing written, Python
///   `WorkflowSpec._require_definition_key`); `VALIDATION_FAILED` or `ENQUEUE_FAILED` from the transaction,
///   which rolled back.
/// - `sent_at` is the caller's start time; every attempt of one start uses the same value.
async fn start_workflow_attempt<T>(
    broker: &Arc<PostgresBroker>,
    spec: &WorkflowSpec,
    wf_id: Uuid,
    sent_at: chrono::DateTime<chrono::Utc>,
    registry: &WorkflowSpecRegistry,
    payload: &PayloadPolicy,
    retention: &RetentionConfig,
) -> WorkflowStartResult<WorkflowHandle<T>> {
    let refused =
        |code: WorkflowStartErrorCode, message: String, retryable: bool| WorkflowStartError {
            code,
            message,
            retryable,
            workflow_name: spec.name.clone(),
            workflow_id: Some(wf_id),
        };
    if let Err(message) = require_definition_key(spec) {
        return Err(refused(
            WorkflowStartErrorCode::ValidationFailed,
            message,
            false,
        ));
    }
    match start_workflow_inner(broker, spec, wf_id, sent_at, registry, retention).await {
        Ok(WorkflowRowInsert::Inserted | WorkflowRowInsert::Existing) => Ok(WorkflowHandle::new(
            wf_id,
            Arc::clone(broker),
            Arc::new(registry.clone()),
            payload.clone(),
            retention.clone(),
        )),
        Ok(WorkflowRowInsert::IdConflict { existing_name }) => Err(refused(
            WorkflowStartErrorCode::WorkflowIdConflict,
            format!(
                "workflow_id '{wf_id}' already exists for workflow name '{existing_name}', \
                 cannot start workflow name '{}' with the same id",
                spec.name,
            ),
            false,
        )),
        Err(error) => Err(refused(
            classify_workflow_error(&error, &spec.name, wf_id),
            error.to_string(),
            is_retryable_workflow_error(&error),
        )),
    }
}

/// Refuses a top-level spec with no definition key, and a sub-workflow node with no definition key of its
/// child. Python: `WorkflowSpec._require_definition_key` (`core/models/workflow/spec.py:1731-1743`) at start
/// (`core/workflows/lifecycle.py:315`) and for each sub-workflow node (`lifecycle.py:546`).
fn require_definition_key(spec: &WorkflowSpec) -> Result<(), String> {
    let has_key = matches!(
        spec.definition_key.as_deref().map(str::trim),
        Some(key) if !key.is_empty()
    );
    match has_key {
        true => require_subworkflow_node_keys(spec).map_err(|error| error.to_string()),
        false => Err(format!(
            "WorkflowSpec is missing definition_key: workflow '{}' has no stable \
             definition_key; set a definition key such as 'your.workflow.v1'",
            spec.name,
        )),
    }
}

/// The greatest depth of a child workflow. A top-level workflow has depth 0; a child has the depth of its
/// parent plus 1. Python: `MAX_SUBWORKFLOW_DEPTH` (`core/models/workflow/limits.py`); the baseline CHECK
/// `horsies_workflows_depth_range` holds the same bound.
pub(crate) const MAX_SUBWORKFLOW_DEPTH: i32 = 32;

/// The depth of a new child of the workflow `parent_workflow_id` (named `parent_workflow_name`) at
/// `parent_depth`.
///
/// `Err` is the error that the sub-workflow node at `task_index` stores when the child would pass
/// [`MAX_SUBWORKFLOW_DEPTH`]: the child is not started, the node fails, and the parent's `on_error` applies
/// (Python `core/workflows/engine.py:1017-1033`). The message names the workflow by its name; `data` names
/// it by its id, with the node, as Python's `_fail_enqueued_task` does (`core/workflows/engine.py:507-512`).
pub(crate) fn child_workflow_depth(
    parent_depth: i32,
    parent_workflow_id: Uuid,
    parent_workflow_name: &str,
    task_index: i32,
) -> Result<i32, TaskError> {
    let child_depth = parent_depth + 1;
    match child_depth > MAX_SUBWORKFLOW_DEPTH {
        false => Ok(child_depth),
        true => Err(TaskError {
            data: Some(serde_json::json!({
                "workflow_id": parent_workflow_id.to_string(),
                "task_index": task_index,
            })),
            ..TaskError::builtin(
                OperationalErrorCode::SubworkflowDepthLimitReached,
                format!(
                    "Subworkflow not started: the child workflow would have depth {child_depth}, \
                     and the limit is {MAX_SUBWORKFLOW_DEPTH} (workflow {parent_workflow_name}:{task_index})"
                ),
            )
        }),
    }
}

/// Refuses a sub-workflow node with no definition key: a child is resolved only by its key.
pub(crate) fn require_subworkflow_node_keys(spec: &WorkflowSpec) -> Result<(), WorkflowError> {
    let unkeyed = spec.tasks.iter().find(|node| {
        node.is_subworkflow
            && !matches!(
                node.sub_definition_key.as_deref().map(str::trim),
                Some(key) if !key.is_empty()
            )
    });
    match unkeyed {
        None => Ok(()),
        Some(node) => Err(WorkflowError::Validation(format!(
            "sub-workflow node '{}' of workflow '{}' has no definition_key of its child workflow; \
             a child workflow is resolved only by its definition_key",
            node.node_id.as_deref().unwrap_or("?"),
            spec.name,
        ))),
    }
}

/// Start a workflow with retry and a separate session-capable pool for result
/// LISTEN/NOTIFY handles.
pub async fn start_workflow_with_retry<T>(
    broker: &Arc<PostgresBroker>,
    spec: &WorkflowSpec,
    workflow_id: Option<Uuid>,
    registry: &WorkflowSpecRegistry,
    resend_on_transient_err: bool,
    payload: &PayloadPolicy,
    retention: &RetentionConfig,
) -> WorkflowStartResult<WorkflowHandle<T>> {
    if !resend_on_transient_err {
        return start_workflow(broker, spec, workflow_id, registry, payload, retention).await;
    }

    // 1 initial attempt + START_RETRY_COUNT retries = 4 total attempts.
    // Matches Python's `1 + _START_RETRY_COUNT`.
    const START_RETRY_COUNT: u32 = 3;
    const START_RETRY_INITIAL_MS: u64 = 200;
    const START_RETRY_MAX_MS: u64 = 2000;

    let max_attempts = 1 + START_RETRY_COUNT;
    let wf_id = workflow_id.unwrap_or_else(Uuid::new_v4);
    // One start time for every attempt, as Python computes it once before its retry loop.
    let sent_at = chrono::Utc::now();
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

        match start_workflow_attempt(broker, spec, wf_id, sent_at, registry, payload, retention)
            .await
        {
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

/// Writes the workflow tree through `horsies_workflow_start`.
///
/// The tree is built in memory first ([`start_arguments`]): the node rows of the workflow, a pre-built child
/// for every sub-workflow root (recursively), and a failed-node record for each sub-workflow root whose child
/// cannot start (past the depth limit, no registration under its key, a failing builder). A failed root fails
/// only its node and its workflow's `on_error` applies, as Python does (`core/workflows/engine.py:1019-1212`).
///
/// Then the broker handle checks its client family (`ensure_client_family_for_write`). A database of the
/// other family, or with no family row, gives `WorkflowError::Broker` with
/// `BrokerError::ClientFamilyMismatch` (`ENQUEUE_FAILED`, not retryable) and nothing is written.
///
/// The function inserts the top-level row with `ON CONFLICT (id) DO NOTHING`, so a concurrent start with the
/// same id waits for the first one and then sees its row: `EXISTS` with the stored name, and nothing is
/// written. Otherwise it inserts the child rows and every node row, fails the recorded nodes and runs their
/// progression, links each sub-workflow root to its pre-built child, and mints the task rows of the READY task
/// nodes. Without failed-node records that is one statement in autocommit; with them, a transaction that also
/// runs the child starts the progression owes ([`start_tree`]).
///
/// After the commit, the parent advances that those child starts owe run, one transaction per level. A
/// failed advance is logged and left to workflow recovery (case 1.6): the start committed, so the start
/// returns its outcome.
async fn start_workflow_inner(
    broker: &Arc<PostgresBroker>,
    spec: &WorkflowSpec,
    wf_id: Uuid,
    sent_at: chrono::DateTime<chrono::Utc>,
    registry: &WorkflowSpecRegistry,
    retention: &RetentionConfig,
) -> Result<WorkflowRowInsert, WorkflowError> {
    let arguments = start_arguments(spec, wf_id, registry, retention).await?;

    // The handle writes only into a database of its own client family: checked once per handle,
    // before the first write. A mismatch is not retryable and writes nothing.
    broker.ensure_client_family_for_write().await?;

    let build = ChildBuild::Registry {
        registry,
        retention,
    };
    let started = start_tree(broker, &arguments, sent_at, build).await?;
    let outcome = match started.outcome {
        StartOutcome::Started => WorkflowRowInsert::Inserted,
        StartOutcome::Exists { stored_name } => existing_workflow(stored_name, &spec.name),
    };
    match &outcome {
        WorkflowRowInsert::Inserted => {
            tracing::debug!(workflow_id = %wf_id, name = %spec.name, "workflow created");
        }
        WorkflowRowInsert::Existing | WorkflowRowInsert::IdConflict { .. } => tracing::warn!(
            workflow_id = %wf_id,
            outcome = ?outcome,
            "workflow id already exists; nothing was written",
        ),
    }
    if let Err(failure) =
        advance_parents(broker, CheckoutClass::Batch, started.parent_advances, build).await
    {
        tracing::error!(
            workflow_id = %wf_id,
            child_workflow_id = %failure.failed_child_workflow_id,
            error = %failure.error,
            "Parent advance after the start of workflow {wf_id} failed at child workflow {}: \
             {}; workflow recovery advances it",
            failure.failed_child_workflow_id,
            failure.error,
        );
    }
    Ok(outcome)
}

/// What an existing top-level id means for a start of `name`: the same name is the existing workflow; another
/// name is an id conflict. A row that conflicted and then disappeared before the name read (a retention
/// delete) is `Existing`, as in Python (`core/workflows/lifecycle.py:420-458`). Pure.
fn existing_workflow(stored_name: Option<String>, name: &str) -> WorkflowRowInsert {
    match stored_name {
        Some(existing_name) if existing_name != name => {
            WorkflowRowInsert::IdConflict { existing_name }
        }
        Some(_) | None => WorkflowRowInsert::Existing,
    }
}

/// Maps a start error to its catalog code.
///
/// The start path returns only database, broker, serialization and validation errors. Any other variant is a
/// defect, so it panics with the workflow and the error; it is not a catalog code.
fn classify_workflow_error(
    error: &WorkflowError,
    workflow_name: &str,
    workflow_id: Uuid,
) -> WorkflowStartErrorCode {
    match error {
        WorkflowError::Serialization(_) | WorkflowError::Validation(_) => {
            WorkflowStartErrorCode::ValidationFailed
        }
        WorkflowError::Database(_)
        | WorkflowError::Broker(_)
        | WorkflowError::StepContract(_)
        | WorkflowError::ChildSpecSource(_) => WorkflowStartErrorCode::EnqueueFailed,
        WorkflowError::WorkflowNotFound { .. } | WorkflowError::InvalidStatus(_) => panic!(
            "start of workflow '{workflow_name}' ({workflow_id}) returned an error outside the start catalog: {error}"
        ),
    }
}

fn is_retryable_workflow_error(e: &WorkflowError) -> bool {
    e.is_retryable()
}

/// Retry a failed workflow start using the broker.
pub async fn retry_start<T>(
    broker: &Arc<PostgresBroker>,
    spec: &WorkflowSpec,
    error: &WorkflowStartError,
    registry: &WorkflowSpecRegistry,
    payload: &PayloadPolicy,
    retention: &RetentionConfig,
) -> WorkflowStartResult<WorkflowHandle<T>> {
    // Reuse the existing validation from horsies-core.
    let workflow_id = crate::core::workflow::start_types::validate_start_retry(error, &spec.name)?;
    start_workflow(
        broker,
        spec,
        Some(workflow_id),
        registry,
        payload,
        retention,
    )
    .await
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

/// The queue that the row of a sub-workflow node stores. Python: `core/workflows/lifecycle.py:534`.
const SUBWORKFLOW_NODE_QUEUE: &str = "default";

/// The priority that the row of a sub-workflow node stores. Python: `core/workflows/lifecycle.py:535`.
const SUBWORKFLOW_NODE_PRIORITY: i32 = 100;

/// The queue and priority that a node row stores.
///
/// A sub-workflow node is never claimed as a task, so it has no queue and no priority; its row stores the
/// constants of Python. A task node must have both resolved.
pub(crate) fn stored_queue_and_priority(node: &AnyNode) -> Result<(&str, i32), WorkflowError> {
    match node.is_subworkflow {
        true => Ok((SUBWORKFLOW_NODE_QUEUE, SUBWORKFLOW_NODE_PRIORITY)),
        false => {
            let queue = node.queue.as_deref().ok_or_else(|| {
                WorkflowError::Validation(format!(
                    "node '{}' (task '{}') has no resolved queue; \
                     this indicates a missing resolution step before workflow start",
                    node.node_id.as_deref().unwrap_or("?"),
                    node.task_name,
                ))
            })?;
            Ok((queue, resolve_insert_priority(node)?))
        }
    }
}

/// The `workflow_ctx_from` that a node row stores: the node ids of the node's context sources, or
/// `None` (SQL NULL) when the node takes no context.
///
/// A node defined with an empty list takes no context, and its row stores NULL, as for no list
/// (Python `_graph_columns`, `python/horsies/core/workflows/child_build.py:479-485`). The task input of
/// the baseline mint adds the context member for any stored array, an empty one included
/// (`horsies_workflow_task_input`). Pure.
pub(crate) fn stored_workflow_ctx_from(node: &AnyNode) -> Option<&[String]> {
    match node.workflow_ctx_from.as_deref() {
        None | Some([]) => None,
        Some(node_ids) => Some(node_ids),
    }
}

/// Resolve the claimable priority for a task node about to be persisted.
///
/// A real task node must already have a resolved priority by insert time —
/// `resolve_node_queue_priority` runs at workflow registration, child-spec
/// materialization, and `check()`, filling it from the queue config. A `None`
/// here means a resolution step was skipped, so it is surfaced (fail-closed)
/// rather than silently persisting a wrong literal — mirroring the queue check
/// in `stored_queue_and_priority` and Python's `WORKFLOW_UNRESOLVED_PRIORITY`.
fn resolve_insert_priority(node: &AnyNode) -> Result<i32, WorkflowError> {
    node.priority.ok_or_else(|| {
        WorkflowError::Validation(format!(
            "node '{}' (task '{}') has no resolved priority; \
             this indicates a missing resolution step before workflow start",
            node.node_id.as_deref().unwrap_or("?"),
            node.task_name,
        ))
    })
}

/// What names a failed child build in the node error. A builder failure takes the Python exception
/// class, Python's `type(exc).__name__` (`core/workflows/engine.py:1196-1211`): a [`HorsiesError`]
/// takes the class of its error category, the subclass of Python's `HorsiesError` that the category
/// stands for ([`ErrorCategory`]); an error with no code takes the base class. A stored node input
/// that the core refuses ([`Self::StoredInputRefused`]) names no class.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ChildSpecErrorType {
    /// A [`HorsiesError`] with no error code.
    HorsiesError,
    /// A [`HorsiesError`] whose code is of [`ErrorCategory::Workflow`].
    WorkflowValidationError,
    /// A [`HorsiesError`] whose code is of [`ErrorCategory::Task`].
    TaskDefinitionError,
    /// A [`HorsiesError`] whose code is of [`ErrorCategory::Config`].
    ConfigurationError,
    /// A [`HorsiesError`] whose code is of [`ErrorCategory::Registry`].
    RegistryError,
    /// A node that gives params to a child registered without a builder. Python raises
    /// `RuntimeError` for the same node (`core/workflows/engine.py:1185-1189`).
    RuntimeError,
    /// A stored node input from which the core cannot make the builder input
    /// ([`child_build_input`](crate::workflow_engine::args_merge::child_build_input)): the text
    /// does not read, or an `args_from` entry is malformed. Python has no such case: its builder
    /// gets typed values, and the stored input is corrupt data, not a user error.
    StoredInputRefused,
}

impl ChildSpecErrorType {
    /// The class of `error`. Pure.
    pub(crate) fn of(error: &HorsiesError) -> Self {
        match error.category() {
            None => Self::HorsiesError,
            Some(ErrorCategory::Workflow) => Self::WorkflowValidationError,
            Some(ErrorCategory::Task) => Self::TaskDefinitionError,
            Some(ErrorCategory::Config) => Self::ConfigurationError,
            Some(ErrorCategory::Registry) => Self::RegistryError,
        }
    }

    /// The label before the text in the node error: the class name, as Python's
    /// `type(exc).__name__` gives it, or `stored node input refused`, which is no class.
    pub(crate) fn as_str(self) -> &'static str {
        match self {
            Self::HorsiesError => "HorsiesError",
            Self::WorkflowValidationError => "WorkflowValidationError",
            Self::TaskDefinitionError => "TaskDefinitionError",
            Self::ConfigurationError => "ConfigurationError",
            Self::RegistryError => "RegistryError",
            Self::RuntimeError => "RuntimeError",
            Self::StoredInputRefused => "stored node input refused",
        }
    }
}

/// Why a registered child gave no spec: the label and the text. For a builder failure they are
/// Python's `type(exc).__name__` and `str(exc)` (`core/workflows/engine.py:1196-1211`); for a
/// refused stored input, `stored node input refused` and the refusal.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ChildSpecError {
    pub(crate) error_type: ChildSpecErrorType,
    /// The full text; the node error keeps its first 200 characters.
    pub(crate) text: String,
}

impl ChildSpecError {
    /// The error of a builder, or of the check of the spec it built: the class of its category and its
    /// text (`Display`). Pure.
    fn of_builder(error: &HorsiesError) -> Self {
        Self {
            error_type: ChildSpecErrorType::of(error),
            text: error.to_string(),
        }
    }

    /// A stored node input from which the builder input cannot be made. Pure.
    pub(crate) fn of_stored_input(
        refused: crate::workflow_engine::args_merge::StoredInputRefused,
    ) -> Self {
        Self {
            error_type: ChildSpecErrorType::StoredInputRefused,
            text: refused.refusal,
        }
    }
}

/// A child build that fails on a path that refuses the step instead of failing the node: the label and
/// the text in one validation message.
impl From<ChildSpecError> for WorkflowError {
    fn from(error: ChildSpecError) -> Self {
        WorkflowError::Validation(format!("{}: {}", error.error_type.as_str(), error.text))
    }
}

/// Why a registered child gave no spec for a node.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ChildSpecFailure {
    /// The value of the node input member `key` does not decode to the builder parameter of that name;
    /// the builder did not run. `cause` is the decode's text. Python fails the node with
    /// `WORKER_SERIALIZATION_ERROR` (`core/workflows/engine.py:1063-1080`, `:1134-1157`).
    MemberRefused { key: String, cause: String },
    /// The build failed: the builder, the check of its spec, or params given to a child without a
    /// builder. Python fails the node with `WORKFLOW_ENQUEUE_FAILED` (`:1196-1211`).
    BuildFailed(ChildSpecError),
}

/// A child build that fails on a path that refuses the step instead of failing the node.
impl From<ChildSpecFailure> for WorkflowError {
    fn from(failure: ChildSpecFailure) -> Self {
        match failure {
            ChildSpecFailure::MemberRefused { key, cause } => WorkflowError::Validation(format!(
                "the child's params member '{key}' does not decode: {cause}"
            )),
            ChildSpecFailure::BuildFailed(error) => error.into(),
        }
    }
}

/// Build the child spec from the parent node's encoded input (`input_json`,
/// as the client encoded it). A child registered without a builder refuses a
/// node that gives params (`SubWorkflowParams::Given`); the refusal text names
/// the client's builder hook ([`WorkflowSpecRegistry::builder_hook`]: Python
/// `build_with()`, Rust `spec_builder`; Python's text,
/// `core/workflows/engine.py:1185-1189`).
///
/// `input_json` is the text of a [`ChildBuildInput`](crate::workflow_engine::args_merge::ChildBuildInput)
/// ([`child_build_input`](crate::workflow_engine::args_merge::child_build_input)): each `args_from`
/// wrap is already replaced by its `inner` result envelope. This function does
/// not unwrap.
///
/// Errors: [`ChildSpecFailure::MemberRefused`] when the builder's decode
/// refuses the value of one input member; [`ChildSpecFailure::BuildFailed`]
/// when the builder fails, when the built spec does not pass the registry's
/// check, or for params given to a child without a builder
/// ([`ChildSpecErrorType::RuntimeError`]). Does no I/O; it runs the
/// registered builder.
pub(crate) fn materialize_child_spec(
    resolved: crate::core::registry::workflow::ResolvedChildWorkflow<'_>,
    params: SubWorkflowParams,
    input_json: &str,
    registry: &WorkflowSpecRegistry,
) -> Result<WorkflowSpec, ChildSpecFailure> {
    match resolved {
        crate::core::registry::workflow::ResolvedChildWorkflow::Dynamic(registered) => {
            built_child_spec(&registered.spec_builder, input_json, registry)
        }
        crate::core::registry::workflow::ResolvedChildWorkflow::Static(registered) => {
            match (&registered.spec_builder, params) {
                (Some(builder), _) => built_child_spec(builder, input_json, registry),
                (None, SubWorkflowParams::Given) => {
                    Err(ChildSpecFailure::BuildFailed(ChildSpecError {
                        error_type: ChildSpecErrorType::RuntimeError,
                        text: format!(
                            "subworkflow '{}' received runtime parameters but has no {} that accepts them",
                            registered.spec.name,
                            registry.builder_hook().as_str(),
                        ),
                    }))
                }
                (None, SubWorkflowParams::NotGiven) => Ok(registered.spec.clone()),
            }
        }
    }
}

/// Runs a registered builder on `input_json`, then resolves the task options
/// of the built spec and checks it against the registry.
///
/// Errors: [`ChildSpecFailure::MemberRefused`] for the builder's member
/// refusal; [`ChildSpecFailure::BuildFailed`] with the builder's error or the
/// check's error.
fn built_child_spec(
    builder: &crate::core::SpecBuilderFn,
    input_json: &str,
    registry: &WorkflowSpecRegistry,
) -> Result<WorkflowSpec, ChildSpecFailure> {
    let mut spec = builder(input_json).map_err(|error| match error {
        SpecBuildError::MemberRefused { key, cause } => {
            ChildSpecFailure::MemberRefused { key, cause }
        }
        SpecBuildError::Failed(error) => {
            ChildSpecFailure::BuildFailed(ChildSpecError::of_builder(&error))
        }
    })?;
    registry.resolve_spec_task_options(&mut spec);
    registry
        .resolve_and_validate_spec(&mut spec)
        .map_err(|error| ChildSpecFailure::BuildFailed(ChildSpecError::of_builder(&error)))?;
    Ok(spec)
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
    use crate::core::OnError;
    use crate::TaskNode;
    use serial_test::serial;

    fn child_registered_static() -> RegisteredWorkflowSpec {
        let mut builder = WorkflowSpecBuilder::new("child_static");
        builder.definition_key("tests.child_static.v1");
        let node = builder.task(TaskNode::<String>::raw("hello_task", "null").queue("default"));
        builder.output(node);
        RegisteredWorkflowSpec {
            spec: builder.build().unwrap(),
            spec_builder: None,
        }
    }

    /// A node with no input stores `{}`, as Python does: the start and the resume refuse a node input that
    /// is not a JSON object text (`horsies_workflow_task_input`, `0005_workflow_functions.sql:341-343`).
    fn node_with(is_subworkflow: bool, priority: Option<i32>) -> AnyNode {
        AnyNode {
            task_name: "t".to_owned(),
            input_json: "{}".to_owned(),
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
            sub_workflow_params: crate::core::workflow::node::SubWorkflowParams::NotGiven,
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
    fn a_subworkflow_node_row_stores_the_python_constants() {
        let mut node = node_with(true, Some(50));
        node.queue = None;
        assert_eq!(stored_queue_and_priority(&node).unwrap(), ("default", 100));
        node.queue = Some("bulk".to_owned());
        assert_eq!(stored_queue_and_priority(&node).unwrap(), ("default", 100));
    }

    #[test]
    fn a_task_node_row_stores_its_resolved_queue_and_priority() {
        assert_eq!(
            stored_queue_and_priority(&node_with(false, Some(30))).unwrap(),
            ("default", 30)
        );
        let mut unresolved = node_with(false, Some(30));
        unresolved.queue = None;
        let err = stored_queue_and_priority(&unresolved).unwrap_err();
        assert!(
            matches!(err, WorkflowError::Validation(ref m) if m.contains("no resolved queue")),
            "expected fail-closed unresolved-queue error, got: {:?}",
            err
        );
    }

    /// A node defined with no list or with an empty list takes no context, and its row stores NULL
    /// (Python `_graph_columns`, `python/horsies/core/workflows/child_build.py:479-485`).
    #[test]
    fn an_empty_context_list_is_no_context() {
        let mut node = node_with(false, Some(10));
        assert_eq!(stored_workflow_ctx_from(&node), None);
        node.workflow_ctx_from = Some(Vec::new());
        assert_eq!(stored_workflow_ctx_from(&node), None);
        node.workflow_ctx_from = Some(vec!["a".to_owned(), "b".to_owned()]);
        assert_eq!(
            stored_workflow_ctx_from(&node),
            Some(["a".to_owned(), "b".to_owned()].as_slice())
        );
    }

    /// A node of the given kind at `index` with the given dependencies.
    fn graph_node(index: usize, is_subworkflow: bool, dependencies: Vec<usize>) -> AnyNode {
        let mut node = node_with(is_subworkflow, Some(10));
        node.index = index;
        node.node_id = Some(format!("n{index}"));
        node.dependencies = dependencies;
        node.sub_definition_key = match is_subworkflow {
            true => Some("tests.downstream.child.v1".to_owned()),
            false => None,
        };
        node
    }

    /// The start stores each node's `downstream_subworkflow` from the static graph, and
    /// `sub_workflow_params` on the sub-workflow node only.
    #[tokio::test]
    #[serial]
    async fn the_start_stores_the_downstream_subworkflow_fact_of_each_node() {
        let pool = crate::broker::enqueue_history_tests::migrated_pool().await;
        let spec = keyed_spec(
            Some("tests.downstream.start.v1"),
            vec![
                graph_node(0, false, vec![]),
                graph_node(1, true, vec![0]),
                graph_node(2, false, vec![0]),
            ],
        );
        let broker = Arc::new(PostgresBroker::from_pool(pool.clone()));
        let workflow_id = Uuid::new_v4();

        let _: WorkflowHandle<serde_json::Value> = start_workflow(
            &broker,
            &spec,
            Some(workflow_id),
            &WorkflowSpecRegistry::new(crate::core::registry::BuilderHookName::SPEC_BUILDER),
            &PayloadPolicy::default(),
            &RetentionConfig::default(),
        )
        .await
        .expect("start workflow");

        let nodes: Vec<(i32, String, Option<String>)> = sqlx::query_as(
            "SELECT task_index, downstream_subworkflow, sub_workflow_params
             FROM horsies_workflow_tasks WHERE workflow_id = $1 ORDER BY task_index",
        )
        .bind(workflow_id)
        .fetch_all(&pool)
        .await
        .unwrap();
        assert_eq!(
            nodes,
            vec![
                (0, "REACHABLE".to_owned(), None),
                (1, "NONE".to_owned(), Some("NOT_GIVEN".to_owned())),
                (2, "NONE".to_owned(), None),
            ]
        );

        sqlx::query(
            "DELETE FROM horsies_tasks WHERE id IN (
                SELECT task_id FROM horsies_workflow_tasks WHERE workflow_id = $1
             )",
        )
        .bind(workflow_id)
        .execute(&pool)
        .await
        .ok();
        sqlx::query("DELETE FROM horsies_workflows WHERE id = $1")
            .bind(workflow_id)
            .execute(&pool)
            .await
            .ok();
    }

    /// Python `core/workflows/engine.py:1017-1033`: the message names the parent by its name, `data` by
    /// its id.
    #[test]
    fn child_depth_is_bounded_at_32() {
        let parent = Uuid::new_v4();
        assert_eq!(
            child_workflow_depth(0, parent, "deep_parent", 1).unwrap(),
            1
        );
        assert_eq!(
            child_workflow_depth(31, parent, "deep_parent", 1).unwrap(),
            32
        );
        let error = child_workflow_depth(32, parent, "deep_parent", 4).unwrap_err();
        assert_eq!(
            error.error_code.to_string(),
            "SUBWORKFLOW_DEPTH_LIMIT_REACHED"
        );
        assert_eq!(
            error.data,
            Some(serde_json::json!({"workflow_id": parent.to_string(), "task_index": 4}))
        );
        assert_eq!(
            error.message.as_deref(),
            Some(
                "Subworkflow not started: the child workflow would have depth 33, \
                 and the limit is 32 (workflow deep_parent:4)"
            )
        );
    }

    /// A builder error takes the Python class of its error category; an error with no code takes
    /// `HorsiesError` (Python `type(exc).__name__`, `core/workflows/engine.py:1196-1211`).
    #[test]
    fn a_builder_error_takes_the_python_class_of_its_category() {
        use crate::core::ErrorCode;

        for (error, class) in [
            (HorsiesError::new("no code"), "HorsiesError"),
            (
                HorsiesError::new("cycle").with_code(ErrorCode::WorkflowCycleDetected),
                "WorkflowValidationError",
            ),
            (
                HorsiesError::new("options").with_code(ErrorCode::TaskInvalidOptions),
                "TaskDefinitionError",
            ),
            (
                HorsiesError::new("queue mode").with_code(ErrorCode::ConfigInvalidQueueMode),
                "ConfigurationError",
            ),
            (
                HorsiesError::new("not registered").with_code(ErrorCode::TaskNotRegistered),
                "RegistryError",
            ),
        ] {
            let failure = ChildSpecError::of_builder(&error);
            assert_eq!(failure.error_type.as_str(), class, "{error}");
            assert_eq!(failure.text, error.to_string());
        }
    }

    /// A failing builder is a `ChildSpecError` with the class and text of its error; a path that refuses
    /// the step reads it as one validation message.
    #[test]
    fn a_failing_builder_is_a_child_spec_error() {
        let builder: crate::core::SpecBuilderFn =
            Arc::new(|_input: &str| -> Result<WorkflowSpec, SpecBuildError> {
                Err(SpecBuildError::Failed(
                    HorsiesError::new("the builder refuses")
                        .with_code(crate::core::ErrorCode::WorkflowCycleDetected),
                ))
            });
        let mut registry =
            WorkflowSpecRegistry::new(crate::core::registry::BuilderHookName::SPEC_BUILDER);
        let mut registered = child_registered_static();
        registered.spec_builder = Some(builder);
        registry.register(registered).unwrap();
        let resolved = registry
            .resolve_child_registration("tests.child_static.v1")
            .expect("the child is registered");

        let ChildSpecFailure::BuildFailed(failure) =
            materialize_child_spec(resolved, SubWorkflowParams::Given, "{}", &registry)
                .unwrap_err()
        else {
            panic!("a failing builder is a build failure");
        };

        assert_eq!(
            failure.error_type,
            ChildSpecErrorType::WorkflowValidationError
        );
        let expected_text = HorsiesError::new("the builder refuses")
            .with_code(crate::core::ErrorCode::WorkflowCycleDetected)
            .to_string();
        assert_eq!(failure.text, expected_text);
        match WorkflowError::from(failure) {
            WorkflowError::Validation(message) => {
                assert_eq!(message, format!("WorkflowValidationError: {expected_text}"));
            }
            other => panic!("unexpected error: {other}"),
        }
    }

    /// A builder whose decode refuses one input member is a member refusal, not a build failure; a
    /// path that refuses the step reads it as one validation message.
    #[test]
    fn a_builder_s_member_refusal_is_not_a_build_failure() {
        let builder: crate::core::SpecBuilderFn =
            Arc::new(|_input: &str| -> Result<WorkflowSpec, SpecBuildError> {
                Err(SpecBuildError::MemberRefused {
                    key: "count".to_owned(),
                    cause: "count: invalid type".to_owned(),
                })
            });
        let mut registry =
            WorkflowSpecRegistry::new(crate::core::registry::BuilderHookName::SPEC_BUILDER);
        let mut registered = child_registered_static();
        registered.spec_builder = Some(builder);
        registry.register(registered).unwrap();
        let resolved = registry
            .resolve_child_registration("tests.child_static.v1")
            .expect("the child is registered");

        let failure = materialize_child_spec(resolved, SubWorkflowParams::Given, "{}", &registry)
            .unwrap_err();

        assert_eq!(
            failure,
            ChildSpecFailure::MemberRefused {
                key: "count".to_owned(),
                cause: "count: invalid type".to_owned(),
            }
        );
        match WorkflowError::from(failure) {
            WorkflowError::Validation(message) => assert_eq!(
                message,
                "the child's params member 'count' does not decode: count: invalid type"
            ),
            other => panic!("unexpected error: {other}"),
        }
    }

    /// Params given to a child registered without a builder: the `RuntimeError`
    /// text names the builder hook of the client that built the registry.
    /// Python's text names `build_with()` (`core/workflows/engine.py:1185-1189`).
    #[test]
    fn the_params_refusal_names_the_builder_hook_of_the_client() {
        for hook in [
            crate::core::registry::BuilderHookName::SPEC_BUILDER,
            crate::core::registry::BuilderHookName::new("build_with()"),
        ] {
            let mut registry = WorkflowSpecRegistry::new(hook);
            registry.register(child_registered_static()).unwrap();
            let resolved = registry
                .resolve_child_registration("tests.child_static.v1")
                .expect("the child is registered");

            let failure =
                materialize_child_spec(resolved, SubWorkflowParams::Given, "{}", &registry)
                    .unwrap_err();

            assert_eq!(
                failure,
                ChildSpecFailure::BuildFailed(ChildSpecError {
                    error_type: ChildSpecErrorType::RuntimeError,
                    text: format!(
                        "subworkflow 'child_static' received runtime parameters but has no {} \
                         that accepts them",
                        hook.as_str(),
                    ),
                })
            );
        }
    }

    /// A merged input with an `args_from` entry that has no `inner` is a refused stored input: the
    /// builder input is not made, and the node fails with `WORKFLOW_ENQUEUE_FAILED` and the refusal.
    #[test]
    fn a_stored_input_with_a_malformed_entry_is_a_refused_stored_input() {
        let input = r#"{"value": {"__h_taskresult_envelope__": true, "source_task_name": "t"}}"#;
        let args_from = serde_json::json!({"value": 0});

        let refused = crate::workflow_engine::args_merge::child_build_input(
            input.to_owned(),
            Some(&args_from),
        )
        .expect_err("a wrap without inner is refused");
        let failure = ChildSpecError::of_stored_input(refused);

        assert_eq!(failure.error_type, ChildSpecErrorType::StoredInputRefused);
        assert_eq!(
            failure.text,
            "args_from envelope for kwarg 'value' has no inner result"
        );
        let workflow_id = Uuid::new_v4();
        let error = crate::workflow_engine::start_statement::build_failed_error(
            workflow_id,
            "parent_workflow",
            3,
            &failure,
        );
        assert_eq!(error.error_code.to_string(), "WORKFLOW_ENQUEUE_FAILED");
        assert_eq!(
            error.message.as_deref(),
            Some(
                "Subworkflow build failed: stored node input refused: args_from envelope for \
                 kwarg 'value' has no inner result (workflow parent_workflow:3)"
            )
        );
        assert_eq!(
            error.data,
            Some(serde_json::json!({"workflow_id": workflow_id.to_string(), "task_index": 3}))
        );
    }

    /// A start through a handle of the other client family is refused before its first write:
    /// `ENQUEUE_FAILED`, not retryable, and no workflow row.
    #[tokio::test]
    #[serial]
    async fn a_start_of_another_client_family_is_refused_and_writes_nothing() {
        let pool = crate::broker::enqueue_history_tests::migrated_pool().await;
        let broker = Arc::new(
            PostgresBroker::from_pool(pool.clone())
                .with_client_family(crate::broker::client_family::ClientFamily::Python),
        );
        let workflow_id = Uuid::new_v4();
        let spec = keyed_spec(
            Some("tests.family.start.v1"),
            vec![node_with(false, Some(17))],
        );

        let refused = start_workflow::<serde_json::Value>(
            &broker,
            &spec,
            Some(workflow_id),
            &WorkflowSpecRegistry::new(crate::core::registry::BuilderHookName::SPEC_BUILDER),
            &PayloadPolicy::default(),
            &RetentionConfig::default(),
        )
        .await
        .expect_err("a start of another client family");

        assert_eq!(refused.code, WorkflowStartErrorCode::EnqueueFailed);
        assert!(!refused.retryable);
        assert!(
            refused.message.contains("its own family"),
            "{}",
            refused.message
        );
        let written: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM horsies_workflows WHERE id = $1")
                .bind(workflow_id)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(written, 0);
    }

    fn keyed_spec(definition_key: Option<&str>, tasks: Vec<AnyNode>) -> WorkflowSpec {
        WorkflowSpec {
            name: "keyed".to_owned(),
            definition_key: definition_key.map(str::to_owned),
            tasks,
            on_error: OnError::Fail,
            output_index: None,
            success_policy: None,
        }
    }

    #[test]
    fn a_spec_with_no_definition_key_is_refused() {
        let task = node_with(false, Some(10));
        for key in [None, Some(""), Some("   ")] {
            let message = require_definition_key(&keyed_spec(key, vec![task.clone()])).unwrap_err();
            assert!(message.contains("missing definition_key"), "{message}");
        }
        assert!(require_definition_key(&keyed_spec(Some("tests.keyed.v1"), vec![task])).is_ok());
    }

    #[test]
    fn a_subworkflow_node_with_no_definition_key_is_refused() {
        let unkeyed = node_with(true, None);
        let message =
            require_definition_key(&keyed_spec(Some("tests.keyed.v1"), vec![unkeyed.clone()]))
                .unwrap_err();
        assert!(
            message.contains("has no definition_key of its child workflow"),
            "{message}"
        );
        let mut keyed = unkeyed;
        keyed.sub_definition_key = Some("tests.child.v1".to_owned());
        assert!(require_definition_key(&keyed_spec(Some("tests.keyed.v1"), vec![keyed])).is_ok());
    }

    /// Python refuses a node whose stored retry policy does not validate, and the start's transaction rolls
    /// back (`python/horsies/core/workflows/lifecycle.py:152-203`, `root_retry_and_deadline`; the reader is
    /// `stored_max_retries`, `python/horsies/core/codec/task_options.py:80-103`). Each options text here is a JSON
    /// object, so `prepare_workflow_task_facts` accepts it; only the retry limit refuses it. A reader that
    /// guesses a limit (3 for a policy without an integer `max_retries`) lets the start write its rows, and
    /// passes 21 on to the baseline CHECK (`max_retries BETWEEN 0 AND 20`) as a database error.
    #[tokio::test]
    #[serial]
    async fn a_node_whose_retry_policy_does_not_fit_is_refused_before_any_row() {
        let pool = crate::broker::enqueue_history_tests::migrated_pool().await;
        let broker = Arc::new(PostgresBroker::from_pool(pool.clone()));
        let task_name = "start_refused_retry_policy_node";
        let exponential = |max_retries: &str| {
            format!(
                r#"{{"retry_policy": {{"max_retries": {max_retries}, "intervals": [60],
                    "backoff_strategy": "exponential", "jitter": true, "auto_retry_for": ["X"]}}}}"#
            )
        };
        let cases = [
            (
                "no max_retries",
                r#"{"retry_policy": {"backoff_strategy": "exponential"}}"#.to_owned(),
            ),
            ("max_retries is text", exponential("\"5\"")),
            ("max_retries above 20", exponential("21")),
            ("policy is a number", r#"{"retry_policy": 5}"#.to_owned()),
        ];
        for (case, options) in cases {
            let mut root = node_with(false, Some(17));
            root.task_name = task_name.to_owned();
            root.task_options_json = Some(options);
            let spec = keyed_spec(Some("tests.start.refused_retry_policy.v1"), vec![root]);
            let workflow_id = Uuid::new_v4();

            let started = start_workflow::<serde_json::Value>(
                &broker,
                &spec,
                Some(workflow_id),
                &WorkflowSpecRegistry::new(crate::core::registry::BuilderHookName::SPEC_BUILDER),
                &PayloadPolicy::default(),
                &RetentionConfig::default(),
            )
            .await;

            let refused = match started {
                Ok(_) => panic!("{case}: the start must be refused"),
                Err(refused) => refused,
            };
            assert_eq!(
                refused.code,
                WorkflowStartErrorCode::ValidationFailed,
                "{case}"
            );
            assert!(!refused.retryable, "{case}");
            assert!(
                refused.message.contains(&format!(
                    "task_options of workflow node 0 ({task_name}) refused"
                )),
                "{case}: {}",
                refused.message
            );
            let rows: (i64, i64, i64) = sqlx::query_as(
                "SELECT (SELECT count(*) FROM horsies_workflows WHERE id = $1),
                        (SELECT count(*) FROM horsies_workflow_tasks WHERE workflow_id = $1),
                        (SELECT count(*) FROM horsies_tasks WHERE task_name = $2)",
            )
            .bind(workflow_id)
            .bind(task_name)
            .fetch_one(&pool)
            .await
            .unwrap();
            assert_eq!(rows, (0, 0, 0), "{case}");
        }
    }

    /// Python raises when a node's options are not a JSON object and the start rolls back
    /// (`python/horsies/core/workflows/lifecycle.py:576-604`); a deadline the database cannot read fails the
    /// insert. The core refuses both before any row: options that do not read are not replaced when a
    /// deadline is merged in, and a stored `good_until` that is not an RFC 3339 text is not read as no
    /// deadline.
    #[tokio::test]
    #[serial]
    async fn a_node_whose_options_or_deadline_do_not_read_is_refused_before_any_row() {
        let pool = crate::broker::enqueue_history_tests::migrated_pool().await;
        let broker = Arc::new(PostgresBroker::from_pool(pool.clone()));
        let task_name = "start_refused_deadline_node";
        let deadline = chrono::Utc::now() + chrono::Duration::hours(1);
        let cases = [
            (
                "options are a list, with a node deadline",
                Some(deadline),
                "[1]",
            ),
            (
                "good_until is not a time",
                None,
                r#"{"good_until": "not-a-date"}"#,
            ),
            ("good_until is a number", None, r#"{"good_until": 12345}"#),
        ];
        for (case, good_until, options) in cases {
            let mut root = node_with(false, Some(17));
            root.task_name = task_name.to_owned();
            root.good_until = good_until;
            root.task_options_json = Some(options.to_owned());
            let spec = keyed_spec(Some("tests.start.refused_deadline.v1"), vec![root]);
            let workflow_id = Uuid::new_v4();

            let started = start_workflow::<serde_json::Value>(
                &broker,
                &spec,
                Some(workflow_id),
                &WorkflowSpecRegistry::new(crate::core::registry::BuilderHookName::SPEC_BUILDER),
                &PayloadPolicy::default(),
                &RetentionConfig::default(),
            )
            .await;

            let refused = match started {
                Ok(_) => panic!("{case}: the start must be refused"),
                Err(refused) => refused,
            };
            assert_eq!(
                refused.code,
                WorkflowStartErrorCode::ValidationFailed,
                "{case}"
            );
            assert!(!refused.retryable, "{case}");
            assert!(
                refused.message.contains(&format!(
                    "task_options of workflow node 0 ({task_name}) refused"
                )),
                "{case}: {}",
                refused.message
            );
            let rows: (i64, i64, i64) = sqlx::query_as(
                "SELECT (SELECT count(*) FROM horsies_workflows WHERE id = $1),
                        (SELECT count(*) FROM horsies_workflow_tasks WHERE workflow_id = $1),
                        (SELECT count(*) FROM horsies_tasks WHERE task_name = $2)",
            )
            .bind(workflow_id)
            .bind(task_name)
            .fetch_one(&pool)
            .await
            .unwrap();
            assert_eq!(rows, (0, 0, 0), "{case}");
        }
    }

    /// A node whose retry policy fits stores its `max_retries` on the node row and on its task row, from
    /// the same reader as a task row (Python `stored_max_retries`).
    #[tokio::test]
    #[serial]
    async fn a_node_whose_retry_policy_fits_stores_its_limit_on_both_rows() {
        let pool = crate::broker::enqueue_history_tests::migrated_pool().await;
        let broker = Arc::new(PostgresBroker::from_pool(pool.clone()));
        let mut root = node_with(false, Some(17));
        root.task_name = "start_fitting_retry_policy_node".to_owned();
        root.task_options_json = Some(
            r#"{"retry_policy": {"max_retries": 4, "intervals": [60],
                "backoff_strategy": "exponential", "jitter": true, "auto_retry_for": ["X"]}}"#
                .to_owned(),
        );
        let spec = keyed_spec(Some("tests.start.fitting_retry_policy.v1"), vec![root]);
        let workflow_id = Uuid::new_v4();

        let _: WorkflowHandle<serde_json::Value> = start_workflow(
            &broker,
            &spec,
            Some(workflow_id),
            &WorkflowSpecRegistry::new(crate::core::registry::BuilderHookName::SPEC_BUILDER),
            &PayloadPolicy::default(),
            &RetentionConfig::default(),
        )
        .await
        .expect("start a workflow whose node policy fits");

        let limits: (i32, i32) = sqlx::query_as(
            "SELECT wt.max_retries, t.max_retries
             FROM horsies_workflow_tasks wt
             JOIN horsies_tasks t ON t.id = wt.task_id
             WHERE wt.workflow_id = $1 AND wt.task_index = 0",
        )
        .bind(workflow_id)
        .fetch_one(&pool)
        .await
        .unwrap();
        sqlx::query(
            "DELETE FROM horsies_tasks WHERE id IN (
                SELECT task_id FROM horsies_workflow_tasks WHERE workflow_id = $1
             )",
        )
        .bind(workflow_id)
        .execute(&pool)
        .await
        .ok();
        sqlx::query("DELETE FROM horsies_workflows WHERE id = $1")
            .bind(workflow_id)
            .execute(&pool)
            .await
            .ok();
        assert_eq!(limits, (4, 4));
    }

    /// A root node that the start enqueues holds its enqueue time as `started_at`: a root that names no
    /// context source and a root that names one. The start statement writes both node rows and their task
    /// rows, so the time equals `enqueued_at` of the node's task row.
    #[tokio::test]
    #[serial]
    async fn an_enqueued_root_node_holds_its_enqueue_time() {
        let pool = crate::broker::enqueue_history_tests::migrated_pool().await;
        let mut fast = node_with(false, Some(17));
        fast.task_name = "enqueue_time_fast_root".to_owned();
        fast.node_id = Some("fast".to_owned());
        let mut slow = node_with(false, Some(17));
        slow.task_name = "enqueue_time_slow_root".to_owned();
        slow.node_id = Some("slow".to_owned());
        slow.index = 1;
        // A root that names a context source takes the per-row slow path.
        slow.workflow_ctx_from = Some(vec!["fast".to_owned()]);
        let spec = keyed_spec(Some("tests.enqueue_time.root.v1"), vec![fast, slow]);
        let broker = Arc::new(PostgresBroker::from_pool(pool.clone()));
        let workflow_id = Uuid::new_v4();

        let _: WorkflowHandle<serde_json::Value> = start_workflow(
            &broker,
            &spec,
            Some(workflow_id),
            &WorkflowSpecRegistry::new(crate::core::registry::BuilderHookName::SPEC_BUILDER),
            &PayloadPolicy::default(),
            &RetentionConfig::default(),
        )
        .await
        .expect("start workflow with a fast and a slow root");

        let nodes: Vec<(i32, String, bool)> = sqlx::query_as(
            "SELECT wt.task_index, wt.status,
                    wt.started_at IS NOT NULL AND wt.started_at = t.enqueued_at
             FROM horsies_workflow_tasks wt
             JOIN horsies_tasks t ON t.id = wt.task_id
             WHERE wt.workflow_id = $1
             ORDER BY wt.task_index",
        )
        .bind(workflow_id)
        .fetch_all(&pool)
        .await
        .unwrap();
        assert_eq!(
            nodes,
            vec![
                (0, "ENQUEUED".to_owned(), true),
                (1, "ENQUEUED".to_owned(), true),
            ]
        );

        sqlx::query(
            "DELETE FROM horsies_tasks WHERE id IN (
                SELECT task_id FROM horsies_workflow_tasks WHERE workflow_id = $1
             )",
        )
        .bind(workflow_id)
        .execute(&pool)
        .await
        .ok();
        sqlx::query("DELETE FROM horsies_workflows WHERE id = $1")
            .bind(workflow_id)
            .execute(&pool)
            .await
            .ok();
    }

    #[test]
    fn an_existing_id_is_the_workflow_by_its_name_or_a_conflict() {
        assert_eq!(
            existing_workflow(Some("first_name".to_owned()), "first_name"),
            WorkflowRowInsert::Existing
        );
        assert_eq!(
            existing_workflow(Some("first_name".to_owned()), "other_name"),
            WorkflowRowInsert::IdConflict {
                existing_name: "first_name".to_owned()
            }
        );
        // The row was gone by the name read.
        assert_eq!(
            existing_workflow(None, "other_name"),
            WorkflowRowInsert::Existing
        );
    }

    #[tokio::test]
    #[serial]
    async fn an_id_of_another_workflow_name_is_a_conflict_and_writes_nothing() {
        let pool = crate::broker::terminalization_matrix::migrated_pool().await;
        let broker = Arc::new(PostgresBroker::from_pool(pool.clone()));
        let registry =
            WorkflowSpecRegistry::new(crate::core::registry::BuilderHookName::SPEC_BUILDER);
        let retention = RetentionConfig::default();
        let workflow_id = Uuid::new_v4();
        let sent_at = chrono::Utc::now() - chrono::Duration::minutes(5);
        let mut first = keyed_spec(
            Some("tests.start.conflict.v1"),
            vec![node_with(false, Some(100))],
        );
        first.name = "first_name".to_owned();
        let mut other = first.clone();
        other.name = "other_name".to_owned();

        for (spec, expected) in [
            (&first, WorkflowRowInsert::Inserted),
            (&first, WorkflowRowInsert::Existing),
            (
                &other,
                WorkflowRowInsert::IdConflict {
                    existing_name: "first_name".to_owned(),
                },
            ),
        ] {
            assert_eq!(
                start_workflow_inner(&broker, spec, workflow_id, sent_at, &registry, &retention)
                    .await
                    .expect("start"),
                expected
            );
        }

        let (name, stored_sent_at): (String, chrono::DateTime<chrono::Utc>) =
            sqlx::query_as("SELECT name, sent_at FROM horsies_workflows WHERE id = $1")
                .bind(workflow_id)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(name, "first_name");
        // The caller's start time is stored (microsecond precision), not the database clock.
        assert_eq!(
            stored_sent_at.timestamp_micros(),
            sent_at.timestamp_micros()
        );
        let node_rows: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM horsies_workflow_tasks WHERE workflow_id = $1",
        )
        .bind(workflow_id)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(node_rows, 1, "the repeated starts wrote no node row");
    }

    #[tokio::test]
    #[serial]
    async fn p6_workflow_enqueue_start_paths_persist_mapped_class_and_never_eligible_facts() {
        let pool = crate::broker::enqueue_history_tests::migrated_pool().await;
        let workflow_name = "p6_workflow_facts";
        let task_name = "p6_workflow_node_facts";
        sqlx::query("DELETE FROM horsies_workflows WHERE name = $1")
            .bind(workflow_name)
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("DELETE FROM horsies_tasks WHERE task_name = $1")
            .bind(task_name)
            .execute(&pool)
            .await
            .unwrap();

        let mut root = node_with(false, Some(17));
        root.task_name = task_name.to_owned();
        root.queue = Some("bulk".to_owned());
        root.input_json = r#"{"value": 1}"#.to_owned();
        let spec = WorkflowSpec {
            name: workflow_name.to_owned(),
            definition_key: Some("p6.workflow.facts.v1".to_owned()),
            tasks: vec![root],
            on_error: OnError::Fail,
            output_index: Some(0),
            success_policy: None,
        };
        let mut retention = RetentionConfig::default();
        retention
            .queue_retention
            .insert("bulk".to_owned(), Some(chrono::Duration::days(7)));
        let broker = Arc::new(PostgresBroker::from_pool(pool.clone()));
        let registry =
            WorkflowSpecRegistry::new(crate::core::registry::BuilderHookName::SPEC_BUILDER);
        let workflow_id = Uuid::new_v4();

        let _: WorkflowHandle<serde_json::Value> = start_workflow(
            &broker,
            &spec,
            Some(workflow_id),
            &registry,
            &PayloadPolicy::default(),
            &retention,
        )
        .await
        .expect("start workflow with a root task");

        let rows: Vec<(String, String, Option<i32>, Option<i16>, bool)> = sqlx::query_as(
            "SELECT id::text, retention_class_key,
                    octet_length(command_fingerprint), command_fingerprint_version,
                    retain_rerun_input
             FROM horsies_tasks WHERE task_name = $1 ORDER BY id",
        )
        .bind(task_name)
        .fetch_all(&pool)
        .await
        .unwrap();
        assert_eq!(rows.len(), 1);
        for row in rows {
            assert_eq!(row.1, "standard_7d");
            assert!(row.2.is_none());
            assert!(row.3.is_none());
            assert!(!row.4);
            assert_eq!(Uuid::parse_str(&row.0).unwrap().get_version_num(), 7);
        }
    }

    /// A root task row stores the start call time of its workflow as `sent_at`, not the time the start
    /// wrote the row (the start mints every root task row through `horsies_workflow_node_tasks_insert`).
    #[tokio::test]
    #[serial]
    async fn a_root_task_row_stores_the_start_call_time_of_its_workflow() {
        let pool = crate::broker::enqueue_history_tests::migrated_pool().await;
        let task_name = "sent_at_root_task";
        sqlx::query("DELETE FROM horsies_tasks WHERE task_name = $1")
            .bind(task_name)
            .execute(&pool)
            .await
            .unwrap();
        let mut root = node_with(false, Some(17));
        root.task_name = task_name.to_owned();
        let spec = keyed_spec(Some("tests.sent_at.root.v1"), vec![root]);
        let broker = Arc::new(PostgresBroker::from_pool(pool.clone()));
        let workflow_id = Uuid::new_v4();
        let started = chrono::DateTime::parse_from_rfc3339("2026-01-01T00:00:00Z")
            .expect("fixed start call time")
            .with_timezone(&chrono::Utc);

        let _: WorkflowHandle<serde_json::Value> = start_workflow_attempt(
            &broker,
            &spec,
            workflow_id,
            started,
            &WorkflowSpecRegistry::new(crate::core::registry::BuilderHookName::SPEC_BUILDER),
            &PayloadPolicy::default(),
            &RetentionConfig::default(),
        )
        .await
        .expect("start workflow with a root task");

        let stored: Vec<(Uuid, chrono::DateTime<chrono::Utc>)> =
            sqlx::query_as("SELECT id, sent_at FROM horsies_tasks WHERE task_name = $1")
                .bind(task_name)
                .fetch_all(&pool)
                .await
                .unwrap();
        assert_eq!(stored.len(), 1, "{stored:?}");
        for (id, sent_at) in &stored {
            assert_eq!(*sent_at, started, "task {id}");
        }

        sqlx::query("DELETE FROM horsies_tasks WHERE task_name = $1")
            .bind(task_name)
            .execute(&pool)
            .await
            .ok();
        sqlx::query("DELETE FROM horsies_workflow_tasks WHERE workflow_id = $1")
            .bind(workflow_id)
            .execute(&pool)
            .await
            .ok();
        sqlx::query("DELETE FROM horsies_workflows WHERE id = $1")
            .bind(workflow_id)
            .execute(&pool)
            .await
            .ok();
    }

    /// The child spec of the sub-workflow node `parent` as the host build makes it: the registration
    /// under the node's definition key, the builder input of the node input with no `args_from`
    /// ([`child_build_input`](crate::workflow_engine::args_merge::child_build_input)), then
    /// [`materialize_child_spec`].
    fn materialized_child(
        parent: &AnyNode,
        registry: &WorkflowSpecRegistry,
    ) -> Result<WorkflowSpec, ChildSpecFailure> {
        let key = parent
            .sub_definition_key
            .as_deref()
            .expect("the node names its child definition");
        let resolved = registry
            .resolve_child_registration(key)
            .expect("the child definition is registered");
        let input =
            crate::workflow_engine::args_merge::child_build_input(parent.input_json.clone(), None)
                .expect("the node input reads");
        materialize_child_spec(resolved, parent.sub_workflow_params, &input.text, registry)
    }

    /// The refusal of params given to a child registered without a builder (Python
    /// `python/horsies/core/workflows/engine.py:1185-1189`).
    fn params_without_builder() -> ChildSpecFailure {
        ChildSpecFailure::BuildFailed(ChildSpecError {
            error_type: ChildSpecErrorType::RuntimeError,
            text: "subworkflow 'child_static' received runtime parameters \
                   but has no spec_builder that accepts them"
                .to_owned(),
        })
    }

    #[test]
    fn a_dynamic_child_definition_builds_from_the_node_input() {
        let registered = RegisteredWorkflowDefinition {
            name: "child_dynamic".to_owned(),
            definition_key: "tests.child_dynamic.v1".to_owned(),
            declared_children: vec![],
            spec_builder: Arc::new(|input_json| {
                let params = serde_json::from_str::<serde_json::Value>(input_json)
                    .map_err(|e| SpecBuildError::Failed(crate::HorsiesError::new(e.to_string())))?;
                let region = params["region"].as_str().unwrap().to_owned();

                let mut builder = WorkflowSpecBuilder::new("child_dynamic");
                let node = builder.task(
                    TaskNode::<String>::raw("hello_task", "null")
                        .queue("default")
                        .input_json(serde_json::to_string(&region).unwrap()),
                );
                builder.output(node);
                builder.build().map_err(SpecBuildError::Failed)
            }),
        };

        let parent = SubWorkflowNode::<serde_json::Value, String>::typed(
            "child_dynamic",
            "null",
            crate::core::workflow::node::SubWorkflowParams::NotGiven,
        )
        .definition_key("tests.child_dynamic.v1")
        .input_json(r#"{"region":"eu"}"#)
        .into_any_node(0);

        let mut registry =
            WorkflowSpecRegistry::new(crate::core::registry::BuilderHookName::SPEC_BUILDER);
        registry.register_definition(registered).unwrap();
        let child = materialized_child(&parent, &registry).unwrap();
        assert_eq!(child.tasks[0].input_json, "\"eu\"");
    }

    #[test]
    fn a_static_child_definition_follows_the_params_choice() {
        // A child without a spec_builder takes a `NotGiven` node with any
        // text and refuses a `Given` node, even when its text is `null`.
        for (input, params, accepted) in [
            ("null", SubWorkflowParams::NotGiven, true),
            ("{}", SubWorkflowParams::NotGiven, true),
            ("null", SubWorkflowParams::Given, false),
        ] {
            let parent = SubWorkflowNode::<(), String>::typed("child_static", input, params)
                .definition_key("tests.child_static.v1")
                .into_any_node(0);

            let mut registry =
                WorkflowSpecRegistry::new(crate::core::registry::BuilderHookName::SPEC_BUILDER);
            registry.register(child_registered_static()).unwrap();
            match (materialized_child(&parent, &registry), accepted) {
                (Ok(spec), true) => assert_eq!(spec.name, "child_static"),
                (Err(failure), false) => assert_eq!(failure, params_without_builder()),
                (other, _) => panic!("{input} {params:?}: unexpected {other:?}"),
            }
        }
    }

    #[test]
    fn a_static_child_definition_refuses_an_explicit_input() {
        let registered = child_registered_static();
        let parent = SubWorkflowNode::<String, String>::typed(
            "child_static",
            "null",
            crate::core::workflow::node::SubWorkflowParams::NotGiven,
        )
        .definition_key("tests.child_static.v1")
        .set_input("eu".to_owned())
        .unwrap()
        .into_any_node(0);

        let mut registry =
            WorkflowSpecRegistry::new(crate::core::registry::BuilderHookName::SPEC_BUILDER);
        registry.register(registered).unwrap();
        assert_eq!(
            materialized_child(&parent, &registry).unwrap_err(),
            params_without_builder()
        );
    }
}
