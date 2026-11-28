use chrono::{DateTime, Utc};
use uuid::Uuid;

use crate::core::history::enqueue::{prepare_workflow_task_facts, WorkflowTaskFacts};
use crate::core::{
    AnyNode, HorsiesError, HorsiesErrorCategory, OperationalCode, TaskError, WorkflowSpec,
};

use crate::workflow_engine::error::WorkflowError;
use crate::workflow_engine::{node_good_until, node_max_retries};

// ---------------------------------------------------------------------------
// Node facts
// ---------------------------------------------------------------------------

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
///
/// An input or options text that does not read, and options that the reader
/// of a task row refuses, are [`WorkflowError::Validation`] naming the node
/// index and its task (Python's start refusal names the task,
/// `start_task_node_facts`, `core/workflows/lifecycle.py`). Pure.
pub(crate) fn node_task_facts(
    node: &AnyNode,
    merged_task_options: Option<&str>,
    queue_class_key: Option<&str>,
) -> Result<NodeTaskFacts, WorkflowError> {
    let facts = prepare_workflow_task_facts(&node.input_json, merged_task_options, queue_class_key)
        .map_err(|error| {
            WorkflowError::Validation(format!(
                "workflow node {} ({}) refused: {error}",
                node.index, node.task_name,
            ))
        })?;
    Ok(NodeTaskFacts {
        max_retries: node_max_retries(node.index, &node.task_name, merged_task_options)?,
        good_until: node_good_until(node.index, &node.task_name, merged_task_options)?,
        facts,
    })
}

// ---------------------------------------------------------------------------
// Depth and definition keys
// ---------------------------------------------------------------------------

/// The greatest depth of a child workflow. A top-level workflow has depth 0; a child has the depth of its
/// parent plus 1. Python: `MAX_SUBWORKFLOW_DEPTH` (`core/models/workflow/limits.py`); the baseline CHECK
/// `horsies_workflows_depth_range` holds the same bound.
pub(crate) const MAX_SUBWORKFLOW_DEPTH: i32 = 32;

/// The most builds of child workflows in the nesting walk of `check` for one root workflow. The walk builds each
/// child of each build down to [`MAX_SUBWORKFLOW_DEPTH`], so the builds grow as the children per build to the
/// power of the depth; at the bound the walk stops and `check` reports `HRS-039`. Every client uses this one value.
pub const MAX_CHECK_SUBWORKFLOW_BUILDS: usize = 1_000;

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
            data: Some(crate::core::codec::JsonText::of_engine_value(&serde_json::json!({
                "workflow_id": parent_workflow_id.to_string(),
                "task_index": task_index,
            }))),
            ..TaskError::built_in(
                OperationalCode::SubworkflowDepthLimitReached,
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

/// The `workflow_context_from` that a node row stores: the node ids of the node's context
/// sources, or `None` (SQL NULL) when the node takes no context.
///
/// A node defined with an empty list takes no context, and its row stores NULL, as for no list
/// (Python `_graph_columns`, `clients/python/horsies/core/workflows/child_build.py:479-485`). The task input of
/// the baseline mint adds the context member for any stored array, an empty one included
/// (`horsies_workflow_task_input`). Pure.
pub(crate) fn stored_workflow_context_from(node: &AnyNode) -> Option<&[String]> {
    match node.workflow_context_from.as_deref() {
        None | Some([]) => None,
        Some(node_ids) => Some(node_ids),
    }
}

/// Resolve the claimable priority for a task node about to be persisted.
///
/// A task node has a resolved priority by insert time: the client resolves
/// it from its queue config before it sends the spec. A `None` here means
/// that a resolution step was skipped, so it is refused (fail-closed) rather
/// than stored as a wrong literal, as the queue check in
/// `stored_queue_and_priority` and Python's `WORKFLOW_UNRESOLVED_PRIORITY`.
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
/// stands for ([`HorsiesErrorCategory`]); an error with no code takes the base class. A stored node input
/// that the core refuses ([`Self::StoredInputRefused`]) names no class.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ChildSpecErrorType {
    /// A [`HorsiesError`] with no error code.
    HorsiesError,
    /// A [`HorsiesError`] whose code is of [`HorsiesErrorCategory::Workflow`].
    WorkflowValidationError,
    /// A [`HorsiesError`] whose code is of [`HorsiesErrorCategory::Task`].
    TaskDefinitionError,
    /// A [`HorsiesError`] whose code is of [`HorsiesErrorCategory::Config`].
    ConfigurationError,
    /// A [`HorsiesError`] whose code is of [`HorsiesErrorCategory::Registry`].
    RegistryError,
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
            Some(HorsiesErrorCategory::Workflow) => Self::WorkflowValidationError,
            Some(HorsiesErrorCategory::Task) => Self::TaskDefinitionError,
            Some(HorsiesErrorCategory::Config) => Self::ConfigurationError,
            Some(HorsiesErrorCategory::Registry) => Self::RegistryError,
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
            Self::StoredInputRefused => "stored node input refused",
        }
    }
}

/// Why a child gave no spec: the label and the text. For a builder failure they are
/// Python's `type(exc).__name__` and `str(exc)` (`core/workflows/engine.py:1196-1211`); for a
/// refused stored input, `stored node input refused` and the refusal.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ChildSpecError {
    pub(crate) error_type: ChildSpecErrorType,
    /// The full text; the node error keeps its first 200 characters.
    pub(crate) text: String,
}

impl ChildSpecError {
    /// The error of a builder, or of the check of the spec it built: the class of its category and its
    /// text (`Display`). Pure.
    pub fn of_builder(error: &HorsiesError) -> Self {
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

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;

    use crate::broker::workflow_start::{
        ClientStartError, ClientStartOutcome, ExistingRun, IdHolder,
    };
    use crate::broker::PostgresBroker;
    use crate::core::config::retention::RetentionConfig;
    use crate::core::task::execution::Execution;
    use crate::core::OnError;
    use crate::test_support::child_specs::TestChildSpecs;
    use crate::test_support::sql;

    /// Starts `spec` as the workflow `workflow_id` at `sent_at`, through the
    /// core's start of a client spec, with no sub-workflow child and the
    /// task queues classed by `children`.
    async fn start_client(
        broker: &PostgresBroker,
        spec: &WorkflowSpec,
        workflow_id: Uuid,
        sent_at: chrono::DateTime<chrono::Utc>,
        children: &TestChildSpecs,
    ) -> Result<ClientStartOutcome, ClientStartError> {
        broker
            .start_client_workflow(
                &children.client_spec(spec),
                workflow_id,
                sent_at,
                crate::telemetry::trace_carrier::SendTrace::NoCarrier,
                false,
                children,
            )
            .await
    }

    /// Whether `error` is a start refused for its spec (the class that a
    /// client names `VALIDATION_FAILED`).
    fn refused_for_its_spec(error: &ClientStartError) -> bool {
        matches!(
            error,
            ClientStartError::Start(WorkflowError::Validation(_) | WorkflowError::Serialization(_))
        )
    }

    /// A node with no input stores `{}`, as Python does: the start and the resume refuse a node input that
    /// is not a JSON object text (`horsies_workflow_task_input`, `0005_workflow_functions.sql:341-343`).
    fn node_with(is_subworkflow: bool, priority: Option<i32>) -> AnyNode {
        AnyNode {
            task_name: "t".to_owned(),
            input_json: "{}".to_owned(),
            dependencies: vec![],
            args_from: std::collections::HashMap::new(),
            workflow_context_from: None,
            queue: Some("default".to_owned()),
            priority,
            allow_failed_dependencies: false,
            join: crate::core::workflow::node::JoinType::All,
            min_success: None,
            good_until: None,
            index: 0,
            node_id: Some("n".to_owned()),
            task_options_json: None,
            is_subworkflow,
            sub_definition_key: None,
            sub_workflow_params: crate::core::workflow::node::SubWorkflowParameters::NotGiven,
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

    /// A task node stores the execution class of its merged task options: no options store `ISOLATED`, and
    /// a class in the options is read as written (Python
    /// `clients/python/tests/unit/test_execution_class.py::TestWorkflowPaths::test_a_fast_path_root_without_options_is_isolated`,
    /// `::test_a_fast_path_root_reads_its_class`).
    #[test]
    fn a_task_node_stores_the_execution_class_of_its_options() {
        let node = node_with(false, Some(10));
        let without = node_task_facts(&node, None, None).expect("a node with no options reads");
        assert_eq!(without.facts.execution, Execution::Isolated);
        let thread = node_task_facts(
            &node,
            Some(r#"{"execution": "SHARED_THREAD", "cancel_grace_ms": null}"#),
            None,
        )
        .expect("a node of the thread class reads");
        assert_eq!(thread.facts.execution, Execution::SharedThread);
    }

    /// Task options that are not JSON (an empty text is not "no options") and an execution class that is
    /// not a class refuse the node with a validation error that names it; no class is guessed (Python
    /// `clients/python/tests/unit/test_execution_class.py::TestWorkflowPaths::test_a_fast_path_root_with_empty_options_text_raises`,
    /// `::test_a_fast_path_root_with_an_invalid_class_raises`).
    #[test]
    fn a_task_node_whose_options_or_class_do_not_read_is_refused() {
        let mut node = node_with(false, Some(10));
        node.task_name = "class_node".to_owned();
        for (case, options) in [
            ("an empty text", ""),
            ("an unknown class", r#"{"execution": "PARALLEL"}"#),
        ] {
            match node_task_facts(&node, Some(options), None) {
                Ok(_) => panic!("{case}: the node must be refused"),
                Err(WorkflowError::Validation(message)) => assert!(
                    message.contains("workflow node 0 (class_node) refused"),
                    "{case}: {message}"
                ),
                Err(other) => panic!("{case}: expected a validation refusal, got {other:?}"),
            }
        }
    }

    /// A node defined with no list or with an empty list takes no context, and its row stores NULL
    /// (Python `_graph_columns`, `clients/python/horsies/core/workflows/child_build.py:479-485`).
    #[test]
    fn an_empty_context_list_is_no_context() {
        let mut node = node_with(false, Some(10));
        assert_eq!(stored_workflow_context_from(&node), None);
        node.workflow_context_from = Some(Vec::new());
        assert_eq!(stored_workflow_context_from(&node), None);
        node.workflow_context_from = Some(vec!["a".to_owned(), "b".to_owned()]);
        assert_eq!(
            stored_workflow_context_from(&node),
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
        let broker = Arc::new(PostgresBroker::from_test_url(
            pool.url(),
            crate::ClientFamily::Rust,
        ));
        let workflow_id = Uuid::new_v4();

        let started = start_client(
            &broker,
            &spec,
            workflow_id,
            chrono::Utc::now(),
            &TestChildSpecs::empty(),
        )
        .await
        .expect("start workflow");
        assert_eq!(started, ClientStartOutcome::Started);

        let nodes: Vec<(i32, String, Option<String>)> = sql::query_as(
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

        sql::query(
            "DELETE FROM horsies_tasks WHERE id IN (
                SELECT task_id FROM horsies_workflow_tasks WHERE workflow_id = $1
             )",
        )
        .bind(workflow_id)
        .execute(&pool)
        .await
        .ok();
        sql::query("DELETE FROM horsies_workflows WHERE id = $1")
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
            error.data_value(),
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
        use crate::core::HorsiesErrorCode;

        for (error, class) in [
            (HorsiesError::new("no code"), "HorsiesError"),
            (
                HorsiesError::new("cycle").with_code(HorsiesErrorCode::WorkflowCycleDetected),
                "WorkflowValidationError",
            ),
            (
                HorsiesError::new("options").with_code(HorsiesErrorCode::TaskInvalidOptions),
                "TaskDefinitionError",
            ),
            (
                HorsiesError::new("queue mode").with_code(HorsiesErrorCode::ConfigInvalidQueueMode),
                "ConfigurationError",
            ),
            (
                HorsiesError::new("not registered").with_code(HorsiesErrorCode::TaskNotRegistered),
                "RegistryError",
            ),
        ] {
            let failure = ChildSpecError::of_builder(&error);
            assert_eq!(failure.error_type.as_str(), class, "{error}");
            assert_eq!(failure.text, error.to_string());
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
            error.data_value(),
            Some(serde_json::json!({"workflow_id": workflow_id.to_string(), "task_index": 3}))
        );
    }

    /// A start through a handle of the other client family is refused before its first write:
    /// `ENQUEUE_FAILED`, not retryable, and no workflow row.
    #[tokio::test]
    async fn a_start_of_another_client_family_is_refused_and_writes_nothing() {
        let pool = crate::broker::enqueue_history_tests::migrated_pool().await;
        let broker = Arc::new(PostgresBroker::from_test_url(
            pool.url(),
            crate::ClientFamily::Python,
        ));
        let workflow_id = Uuid::new_v4();
        let spec = keyed_spec(
            Some("tests.family.start.v1"),
            vec![node_with(false, Some(17))],
        );

        let refused = start_client(
            &broker,
            &spec,
            workflow_id,
            chrono::Utc::now(),
            &TestChildSpecs::empty(),
        )
        .await
        .expect_err("a start of another client family");

        assert!(
            matches!(
                refused,
                ClientStartError::SchemaInstall(
                    crate::broker::BrokerError::ClientFamilyMismatch { .. }
                )
            ),
            "{refused:?}"
        );
        assert!(!refused.is_retryable());
        assert!(refused.to_string().contains("its own family"), "{refused}");
        let written: i64 =
            sql::query_scalar("SELECT COUNT(*) FROM horsies_workflows WHERE id = $1")
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
    fn a_subworkflow_node_with_no_definition_key_is_refused() {
        let unkeyed = node_with(true, None);
        let refused = require_subworkflow_node_keys(&keyed_spec(
            Some("tests.keyed.v1"),
            vec![unkeyed.clone()],
        ))
        .unwrap_err();
        assert!(
            matches!(
                &refused,
                WorkflowError::Validation(message)
                    if message.contains("has no definition_key of its child workflow")
            ),
            "{refused}"
        );
        let mut keyed = unkeyed;
        keyed.sub_definition_key = Some("tests.child.v1".to_owned());
        assert!(
            require_subworkflow_node_keys(&keyed_spec(Some("tests.keyed.v1"), vec![keyed])).is_ok()
        );
    }

    /// Python refuses a node whose stored retry policy does not validate, and the start's transaction rolls
    /// back (`clients/python/horsies/core/workflows/lifecycle.py:152-203`, `root_retry_and_deadline`; the reader is
    /// `stored_max_retries`, `clients/python/horsies/core/codec/task_options.py:80-103`). Each options text here is a JSON
    /// object, so `prepare_workflow_task_facts` accepts it; only the retry limit refuses it. A reader that
    /// guesses a limit (3 for a policy without an integer `max_retries`) lets the start write its rows, and
    /// passes 21 on to the baseline CHECK (`max_retries BETWEEN 0 AND 20`) as a database error.
    #[tokio::test]
    async fn a_node_whose_retry_policy_does_not_fit_is_refused_before_any_row() {
        let pool = crate::broker::enqueue_history_tests::migrated_pool().await;
        let broker = Arc::new(PostgresBroker::from_test_url(
            pool.url(),
            crate::ClientFamily::Rust,
        ));
        let task_name = "start_refused_retry_policy_node";
        let exponential = |max_retries: &str| {
            format!(
                r#"{{"retry_policy": {{"max_retries": {max_retries}, "intervals_seconds": [60],
                    "backoff_strategy": "exponential", "jitter": true, "retry_for": ["X"]}}}}"#
            )
        };
        // The detail of Python's refusal, where the case pins it
        // (`clients/python/tests/unit/test_stored_max_retries.py::TestWorkflowPaths::test_the_fast_path_root_refuses_a_negative_limit`).
        let cases = [
            (
                "no max_retries",
                r#"{"retry_policy": {"backoff_strategy": "exponential"}}"#.to_owned(),
                None,
            ),
            ("max_retries is text", exponential("\"5\""), None),
            ("max_retries above 20", exponential("21"), None),
            (
                "max_retries is negative",
                exponential("-1"),
                Some(
                    "max_retries: Input should be greater than or equal to 1 [greater_than_equal]",
                ),
            ),
            (
                "policy is a number",
                r#"{"retry_policy": 5}"#.to_owned(),
                None,
            ),
        ];
        for (case, options, detail) in cases {
            let mut root = node_with(false, Some(17));
            root.task_name = task_name.to_owned();
            root.task_options_json = Some(options);
            let spec = keyed_spec(Some("tests.start.refused_retry_policy.v1"), vec![root]);
            let workflow_id = Uuid::new_v4();

            let started = start_client(
                &broker,
                &spec,
                workflow_id,
                chrono::Utc::now(),
                &TestChildSpecs::empty(),
            )
            .await;

            let refused = match started {
                Ok(outcome) => panic!("{case}: the start must be refused, got {outcome:?}"),
                Err(refused) => refused,
            };
            assert!(refused_for_its_spec(&refused), "{case}: {refused:?}");
            assert!(!refused.is_retryable(), "{case}");
            assert!(
                refused.to_string().contains(&format!(
                    "task_options of workflow node 0 ({task_name}) refused"
                )),
                "{case}: {refused}"
            );
            if let Some(detail) = detail {
                assert!(refused.to_string().contains(detail), "{case}: {refused}");
            }
            let rows: (i64, i64, i64) = sql::query_as(
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
    /// (`clients/python/horsies/core/workflows/lifecycle.py:576-604`); a deadline the database cannot read fails the
    /// insert. The core refuses both before any row: options that do not read are not replaced when a
    /// deadline is merged in, and a stored `good_until` that is not an RFC 3339 text is not read as no
    /// deadline.
    #[tokio::test]
    async fn a_node_whose_options_or_deadline_do_not_read_is_refused_before_any_row() {
        let pool = crate::broker::enqueue_history_tests::migrated_pool().await;
        let broker = Arc::new(PostgresBroker::from_test_url(
            pool.url(),
            crate::ClientFamily::Rust,
        ));
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

            let started = start_client(
                &broker,
                &spec,
                workflow_id,
                chrono::Utc::now(),
                &TestChildSpecs::empty(),
            )
            .await;

            let refused = match started {
                Ok(outcome) => panic!("{case}: the start must be refused, got {outcome:?}"),
                Err(refused) => refused,
            };
            assert!(refused_for_its_spec(&refused), "{case}: {refused:?}");
            assert!(!refused.is_retryable(), "{case}");
            assert!(
                refused.to_string().contains(&format!(
                    "task_options of workflow node 0 ({task_name}) refused"
                )),
                "{case}: {refused}"
            );
            let rows: (i64, i64, i64) = sql::query_as(
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
    async fn a_node_whose_retry_policy_fits_stores_its_limit_on_both_rows() {
        let pool = crate::broker::enqueue_history_tests::migrated_pool().await;
        let broker = Arc::new(PostgresBroker::from_test_url(
            pool.url(),
            crate::ClientFamily::Rust,
        ));
        let mut root = node_with(false, Some(17));
        root.task_name = "start_fitting_retry_policy_node".to_owned();
        root.task_options_json = Some(
            r#"{"retry_policy": {"max_retries": 4, "intervals_seconds": [60],
                "backoff_strategy": "exponential", "jitter": true, "retry_for": ["X"]}}"#
                .to_owned(),
        );
        let spec = keyed_spec(Some("tests.start.fitting_retry_policy.v1"), vec![root]);
        let workflow_id = Uuid::new_v4();

        let started = start_client(
            &broker,
            &spec,
            workflow_id,
            chrono::Utc::now(),
            &TestChildSpecs::empty(),
        )
        .await
        .expect("start a workflow whose node policy fits");
        assert_eq!(started, ClientStartOutcome::Started);

        let limits: (i32, i32) = sql::query_as(
            "SELECT wt.max_retries, t.max_retries
             FROM horsies_workflow_tasks wt
             JOIN horsies_tasks t ON t.id = wt.task_id
             WHERE wt.workflow_id = $1 AND wt.task_index = 0",
        )
        .bind(workflow_id)
        .fetch_one(&pool)
        .await
        .unwrap();
        sql::query(
            "DELETE FROM horsies_tasks WHERE id IN (
                SELECT task_id FROM horsies_workflow_tasks WHERE workflow_id = $1
             )",
        )
        .bind(workflow_id)
        .execute(&pool)
        .await
        .ok();
        sql::query("DELETE FROM horsies_workflows WHERE id = $1")
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
        slow.workflow_context_from = Some(vec!["fast".to_owned()]);
        let spec = keyed_spec(Some("tests.enqueue_time.root.v1"), vec![fast, slow]);
        let broker = Arc::new(PostgresBroker::from_test_url(
            pool.url(),
            crate::ClientFamily::Rust,
        ));
        let workflow_id = Uuid::new_v4();

        let started = start_client(
            &broker,
            &spec,
            workflow_id,
            chrono::Utc::now(),
            &TestChildSpecs::empty(),
        )
        .await
        .expect("start workflow with a fast and a slow root");
        assert_eq!(started, ClientStartOutcome::Started);

        let nodes: Vec<(i32, String, bool)> = sql::query_as(
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

        sql::query(
            "DELETE FROM horsies_tasks WHERE id IN (
                SELECT task_id FROM horsies_workflow_tasks WHERE workflow_id = $1
             )",
        )
        .bind(workflow_id)
        .execute(&pool)
        .await
        .ok();
        sql::query("DELETE FROM horsies_workflows WHERE id = $1")
            .bind(workflow_id)
            .execute(&pool)
            .await
            .ok();
    }

    #[tokio::test]
    async fn an_id_of_another_workflow_name_is_a_conflict_and_writes_nothing() {
        let pool = crate::test_support::migrated_pool().await;
        let broker = Arc::new(PostgresBroker::from_test_url(
            pool.url(),
            crate::ClientFamily::Rust,
        ));
        let children = TestChildSpecs::empty();
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
            (&first, ClientStartOutcome::Started),
            (&first, ClientStartOutcome::Exists(ExistingRun::SameCommand)),
            (
                &other,
                ClientStartOutcome::IdHeld(IdHolder::OtherWorkflowName {
                    existing_name: "first_name".to_owned(),
                }),
            ),
        ] {
            assert_eq!(
                start_client(&broker, spec, workflow_id, sent_at, &children)
                    .await
                    .expect("start"),
                expected
            );
        }

        let (name, stored_sent_at): (String, chrono::DateTime<chrono::Utc>) =
            sql::query_as("SELECT name, sent_at FROM horsies_workflows WHERE id = $1")
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
        let node_rows: i64 =
            sql::query_scalar("SELECT count(*) FROM horsies_workflow_tasks WHERE workflow_id = $1")
                .bind(workflow_id)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(node_rows, 1, "the repeated starts wrote no node row");
    }

    #[tokio::test]
    async fn p6_workflow_enqueue_start_paths_persist_mapped_class_and_never_eligible_facts() {
        let pool = crate::broker::enqueue_history_tests::migrated_pool().await;
        let workflow_name = "p6_workflow_facts";
        let task_name = "p6_workflow_node_facts";
        sql::query("DELETE FROM horsies_workflows WHERE name = $1")
            .bind(workflow_name)
            .execute(&pool)
            .await
            .unwrap();
        sql::query("DELETE FROM horsies_tasks WHERE task_name = $1")
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
        let broker = Arc::new(PostgresBroker::from_test_url(
            pool.url(),
            crate::ClientFamily::Rust,
        ));
        let children = TestChildSpecs::empty().with_retention(retention);
        let workflow_id = Uuid::new_v4();

        let started = start_client(&broker, &spec, workflow_id, chrono::Utc::now(), &children)
            .await
            .expect("start workflow with a root task");
        assert_eq!(started, ClientStartOutcome::Started);

        // Id, retention class, fingerprint length, fingerprint version,
        // rerun input kept.
        type TaskFactsRow = (String, String, Option<i32>, Option<i16>, bool);
        let rows: Vec<TaskFactsRow> = sql::query_as(
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
    async fn a_root_task_row_stores_the_start_call_time_of_its_workflow() {
        let pool = crate::broker::enqueue_history_tests::migrated_pool().await;
        let task_name = "sent_at_root_task";
        sql::query("DELETE FROM horsies_tasks WHERE task_name = $1")
            .bind(task_name)
            .execute(&pool)
            .await
            .unwrap();
        let mut root = node_with(false, Some(17));
        root.task_name = task_name.to_owned();
        let spec = keyed_spec(Some("tests.sent_at.root.v1"), vec![root]);
        let broker = Arc::new(PostgresBroker::from_test_url(
            pool.url(),
            crate::ClientFamily::Rust,
        ));
        let workflow_id = Uuid::new_v4();
        let started = chrono::DateTime::parse_from_rfc3339("2026-01-01T00:00:00Z")
            .expect("fixed start call time")
            .with_timezone(&chrono::Utc);

        let outcome = start_client(
            &broker,
            &spec,
            workflow_id,
            started,
            &TestChildSpecs::empty(),
        )
        .await
        .expect("start workflow with a root task");
        assert_eq!(outcome, ClientStartOutcome::Started);

        let stored: Vec<(Uuid, chrono::DateTime<chrono::Utc>)> =
            sql::query_as("SELECT id, sent_at FROM horsies_tasks WHERE task_name = $1")
                .bind(task_name)
                .fetch_all(&pool)
                .await
                .unwrap();
        assert_eq!(stored.len(), 1, "{stored:?}");
        for (id, sent_at) in &stored {
            assert_eq!(*sent_at, started, "task {id}");
        }

        sql::query("DELETE FROM horsies_tasks WHERE task_name = $1")
            .bind(task_name)
            .execute(&pool)
            .await
            .ok();
        sql::query("DELETE FROM horsies_workflow_tasks WHERE workflow_id = $1")
            .bind(workflow_id)
            .execute(&pool)
            .await
            .ok();
        sql::query("DELETE FROM horsies_workflows WHERE id = $1")
            .bind(workflow_id)
            .execute(&pool)
            .await
            .ok();
    }
}
