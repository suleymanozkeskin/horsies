use std::collections::{HashMap, HashSet, VecDeque};

use crate::core::error::{ErrorCode, HorsiesError, ValidationReport};
use crate::core::registry::workflow::RegisteredWorkflowSpec;
use crate::core::workflow::context::WORKFLOW_CTX_KWARG;
use crate::core::workflow::node::{AnyNode, JoinType, NodeRef, TaskNode, TypedNodeRef};
use crate::core::workflow::policy::SuccessPolicy;
use crate::core::workflow::status::OnError;
use crate::core::workflow::sub_workflow::SubWorkflowNode;

// ---------------------------------------------------------------------------
// Slugify helper
// ---------------------------------------------------------------------------

/// Convert a task name to a slug suitable for auto-generated node IDs.
/// Replaces non-alphanumeric characters with `_`.
fn slugify(name: &str) -> String {
    name.chars()
        .map(|c| {
            if c.is_alphanumeric() || c == '_' || c == '-' {
                c
            } else {
                '_'
            }
        })
        .collect()
}

/// Regex-like check for node_id pattern: `[A-Za-z0-9_\-:.]+`
fn is_valid_node_id(id: &str) -> bool {
    !id.is_empty()
        && id.len() <= 128
        && id
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-' || c == ':' || c == '.')
}

// ---------------------------------------------------------------------------
// WorkflowSpec (immutable, validated)
// ---------------------------------------------------------------------------

/// Validated, immutable workflow specification.
///
/// Produced by `WorkflowSpecBuilder::build()` after passing all 6 validation
/// phases. Contains the complete DAG definition ready for engine execution.
#[derive(Debug, Clone)]
pub struct WorkflowSpec {
    /// Workflow name.
    pub name: String,

    /// Stable definition identity for persistence and runtime lookup.
    /// Separate from `name` so the human-readable name can change without
    /// breaking DB identity. Can also serve as a version key in the future.
    pub definition_key: Option<String>,

    /// Ordered list of type-erased task nodes.
    pub tasks: Vec<AnyNode>,

    /// Error handling policy.
    pub on_error: OnError,

    /// Index of the task whose result becomes the workflow output.
    pub output_index: Option<usize>,

    /// Custom success criteria.
    pub success_policy: Option<SuccessPolicy>,
}

// ---------------------------------------------------------------------------
// WorkflowSpecBuilder (mutable)
// ---------------------------------------------------------------------------

/// Builder for constructing and validating a `WorkflowSpec`.
///
/// Add tasks via `task()`, configure policies, then call `build()` to
/// produce a validated `WorkflowSpec`. Validation runs in 5 gated phases.
pub struct WorkflowSpecBuilder {
    name: String,
    definition_key: Option<String>,
    tasks: Vec<AnyNode>,
    on_error: OnError,
    output_ref: Option<NodeRef>,
    success_policy: Option<SuccessPolicy>,
}

impl WorkflowSpecBuilder {
    /// Create a new builder with the given workflow name.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            definition_key: None,
            tasks: Vec::new(),
            on_error: OnError::Fail,
            output_ref: None,
            success_policy: None,
        }
    }

    /// Add a typed task node. Returns a typed node ref for wiring dependencies.
    pub fn task<T, I>(&mut self, node: TaskNode<T, I>) -> TypedNodeRef<T> {
        let index = self.tasks.len();
        let any_node = node.into_any_node(index);
        self.tasks.push(any_node);
        TypedNodeRef::new(index)
    }

    /// Add a sub-workflow node. Returns a typed node ref for wiring dependencies.
    ///
    /// The sub-workflow's spec must be registered in the `WorkflowSpecRegistry`
    /// at runtime for the engine to launch it.
    pub fn sub_workflow<P, T>(&mut self, node: SubWorkflowNode<P, T>) -> TypedNodeRef<T> {
        let index = self.tasks.len();
        let any_node = node.into_any_node(index);
        self.tasks.push(any_node);
        TypedNodeRef::new(index)
    }

    /// Set the stable definition key for persistence identity.
    pub fn definition_key(&mut self, key: impl Into<String>) -> &mut Self {
        self.definition_key = Some(key.into());
        self
    }

    /// Set the error handling policy.
    pub fn on_error(&mut self, policy: OnError) -> &mut Self {
        self.on_error = policy;
        self
    }

    /// Set the output task (whose result becomes the workflow result).
    pub fn output<R>(&mut self, node_ref: R) -> &mut Self
    where
        R: Into<NodeRef>,
    {
        self.output_ref = Some(node_ref.into());
        self
    }

    /// Set a custom success policy.
    pub fn success_policy(&mut self, policy: SuccessPolicy) -> &mut Self {
        self.success_policy = Some(policy);
        self
    }

    /// Validate and build an immutable `WorkflowSpec`.
    ///
    /// Runs 6 gated validation phases. Errors in phase N prevent phase N+1
    /// from executing.
    pub fn build(mut self) -> Result<WorkflowSpec, HorsiesError> {
        // Phase 1: Node IDs
        let mut p1 = ValidationReport::new("node_id_validation");
        self.validate_node_ids(&mut p1);
        if p1.has_errors() {
            return p1.into_result().map(|_| unreachable!());
        }

        // Phase 2: DAG structure
        let mut p2 = ValidationReport::new("dag_validation");
        self.validate_dag_structure(&mut p2);
        if p2.has_errors() {
            return p2.into_result().map(|_| unreachable!());
        }

        // Phase 3: Data flow
        let mut p3 = ValidationReport::new("data_flow_validation");
        self.validate_data_flow(&mut p3);
        if p3.has_errors() {
            return p3.into_result().map(|_| unreachable!());
        }

        // Phase 4: Output and policy
        let mut p4 = ValidationReport::new("output_policy_validation");
        self.validate_output_and_policy(&mut p4);
        if p4.has_errors() {
            return p4.into_result().map(|_| unreachable!());
        }

        // Phase 5: Join semantics
        let mut p5 = ValidationReport::new("join_validation");
        self.validate_join_semantics(&mut p5);
        if p5.has_errors() {
            return p5.into_result().map(|_| unreachable!());
        }

        Ok(WorkflowSpec {
            name: self.name,
            definition_key: self.definition_key,
            tasks: self.tasks,
            on_error: self.on_error,
            output_index: self.output_ref.map(|r| r.index),
            success_policy: self.success_policy,
        })
    }

    /// Validate, build, and return a `RegisteredWorkflowSpec`.
    pub fn build_registered(mut self) -> Result<RegisteredWorkflowSpec, HorsiesError> {
        // Phase 1: Node IDs
        let mut p1 = ValidationReport::new("node_id_validation");
        self.validate_node_ids(&mut p1);
        if p1.has_errors() {
            return p1.into_result().map(|_| unreachable!());
        }

        // Phase 2: DAG structure
        let mut p2 = ValidationReport::new("dag_validation");
        self.validate_dag_structure(&mut p2);
        if p2.has_errors() {
            return p2.into_result().map(|_| unreachable!());
        }

        // Phase 3: Data flow
        let mut p3 = ValidationReport::new("data_flow_validation");
        self.validate_data_flow(&mut p3);
        if p3.has_errors() {
            return p3.into_result().map(|_| unreachable!());
        }

        // Phase 4: Output and policy
        let mut p4 = ValidationReport::new("output_policy_validation");
        self.validate_output_and_policy(&mut p4);
        if p4.has_errors() {
            return p4.into_result().map(|_| unreachable!());
        }

        // Phase 5: Join semantics
        let mut p5 = ValidationReport::new("join_validation");
        self.validate_join_semantics(&mut p5);
        if p5.has_errors() {
            return p5.into_result().map(|_| unreachable!());
        }

        Ok(RegisteredWorkflowSpec {
            spec: WorkflowSpec {
                name: self.name,
                definition_key: self.definition_key,
                tasks: self.tasks,
                on_error: self.on_error,
                output_index: self.output_ref.map(|r| r.index),
                success_policy: self.success_policy,
            },
            spec_builder: None,
        })
    }

    // -----------------------------------------------------------------------
    // Phase 1: Node ID assignment and validation
    // -----------------------------------------------------------------------

    fn validate_node_ids(&mut self, report: &mut ValidationReport) {
        // Check workflow has a name.
        if self.name.is_empty() {
            report.add(
                HorsiesError::new("workflow name must not be empty")
                    .with_code(ErrorCode::WorkflowNoName),
            );
        }

        // Check at least one node.
        if self.tasks.is_empty() {
            report.add(
                HorsiesError::new("workflow must have at least one task")
                    .with_code(ErrorCode::WorkflowNoNodes),
            );
            return;
        }

        // Auto-assign node_ids where missing.
        for task in &mut self.tasks {
            if task.node_id.is_none() {
                task.node_id = Some(format!("{}:{}", slugify(&task.task_name), task.index));
            }
        }

        // Lower builder-layer workflow_ctx refs to stable node_ids now that
        // every node has an assigned identifier.
        let node_ids: Vec<String> = self
            .tasks
            .iter()
            .map(|task| {
                task.node_id
                    .as_ref()
                    .expect("node_id should be assigned before ctx lowering")
                    .clone()
            })
            .collect();
        for task in &mut self.tasks {
            if let Some(refs) = task.workflow_ctx_from_refs.take() {
                let ids = refs
                    .into_iter()
                    .map(|idx| {
                        node_ids.get(idx).cloned().expect(
                            "workflow_ctx_from ref index out of bounds during builder finalization",
                        )
                    })
                    .collect::<Vec<_>>();
                task.workflow_ctx_from = Some(ids);
            }
        }

        // Validate node_id pattern and length.
        for task in &self.tasks {
            if let Some(ref id) = task.node_id {
                if !is_valid_node_id(id) {
                    report.add(
                        HorsiesError::new(format!(
                            "invalid node_id '{}' at index {}",
                            id, task.index,
                        ))
                        .with_code(ErrorCode::WorkflowInvalidNodeId)
                        .with_help("node_id must match [A-Za-z0-9_\\-:.]+, max 128 chars"),
                    );
                }
            }
        }

        // Check uniqueness.
        let mut seen: HashMap<&str, usize> = HashMap::new();
        for task in &self.tasks {
            if let Some(ref id) = task.node_id {
                if let Some(&prev_idx) = seen.get(id.as_str()) {
                    report.add(
                        HorsiesError::new(format!(
                            "duplicate node_id '{}' at indices {} and {}",
                            id, prev_idx, task.index,
                        ))
                        .with_code(ErrorCode::WorkflowDuplicateNodeId),
                    );
                } else {
                    seen.insert(id.as_str(), task.index);
                }
            }
        }
    }

    // -----------------------------------------------------------------------
    // Phase 2: DAG structure validation
    // -----------------------------------------------------------------------

    fn validate_dag_structure(&self, report: &mut ValidationReport) {
        let task_count = self.tasks.len();

        // Check for at least one root (task with no dependencies).
        let has_root = self.tasks.iter().any(|t| t.dependencies.is_empty());
        if !has_root {
            report.add(
                HorsiesError::new("workflow must have at least one root task (no dependencies)")
                    .with_code(ErrorCode::WorkflowNoRootTasks),
            );
        }

        // Validate dependency references are in range.
        for task in &self.tasks {
            for &dep_idx in &task.dependencies {
                if dep_idx >= task_count {
                    report.add(
                        HorsiesError::new(format!(
                            "task at index {} references dependency index {} which is out of range (0..{})",
                            task.index, dep_idx, task_count,
                        ))
                        .with_code(ErrorCode::WorkflowInvalidDependency),
                    );
                }
                if dep_idx == task.index {
                    report.add(
                        HorsiesError::new(format!(
                            "task at index {} has a self-dependency",
                            task.index,
                        ))
                        .with_code(ErrorCode::WorkflowCycleDetected)
                        .with_note("a task cannot depend on itself"),
                    );
                }
            }
        }

        // Cycle detection via Kahn's topological sort.
        if self.detect_cycle(task_count) {
            report.add(
                HorsiesError::new("cycle detected in workflow DAG")
                    .with_code(ErrorCode::WorkflowCycleDetected)
                    .with_help("remove circular dependencies between tasks"),
            );
        }
    }

    /// Returns `true` if the DAG contains a cycle (Kahn's algorithm).
    fn detect_cycle(&self, task_count: usize) -> bool {
        let mut in_degree = vec![0u32; task_count];
        for task in &self.tasks {
            for &dep_idx in &task.dependencies {
                if dep_idx < task_count {
                    in_degree[task.index] += 1;
                }
            }
        }

        // Build adjacency: dep -> dependents
        let mut adj: Vec<Vec<usize>> = vec![Vec::new(); task_count];
        for task in &self.tasks {
            for &dep_idx in &task.dependencies {
                if dep_idx < task_count {
                    adj[dep_idx].push(task.index);
                }
            }
        }

        let mut queue: VecDeque<usize> = VecDeque::new();
        for (i, &deg) in in_degree.iter().enumerate() {
            if deg == 0 {
                queue.push_back(i);
            }
        }

        let mut visited = 0usize;
        while let Some(node) = queue.pop_front() {
            visited += 1;
            for &next in &adj[node] {
                in_degree[next] -= 1;
                if in_degree[next] == 0 {
                    queue.push_back(next);
                }
            }
        }

        visited != task_count
    }

    // -----------------------------------------------------------------------
    // Phase 3: Data flow validation
    // -----------------------------------------------------------------------

    fn validate_data_flow(&self, report: &mut ValidationReport) {
        let task_count = self.tasks.len();

        for task in &self.tasks {
            // args_from values must reference indices that are in dependencies.
            for (kwarg_name, &dep_idx) in &task.args_from {
                if dep_idx >= task_count {
                    report.add(
                        HorsiesError::new(format!(
                            "task at index {} args_from '{}' references index {} which is out of range",
                            task.index, kwarg_name, dep_idx,
                        ))
                        .with_code(ErrorCode::WorkflowInvalidArgsFrom),
                    );
                } else if !task.dependencies.contains(&dep_idx) {
                    report.add(
                        HorsiesError::new(format!(
                            "task at index {} args_from '{}' references index {} which is not in its dependencies",
                            task.index, kwarg_name, dep_idx,
                        ))
                        .with_code(ErrorCode::WorkflowInvalidArgsFrom)
                        .with_help("args_from sources must be listed in waits_for/dependencies"),
                    );
                }
            }

            // workflow_ctx_from node_ids must be in dependencies.
            if let Some(ref ctx_ids) = task.workflow_ctx_from {
                let dep_node_ids: HashSet<&str> = task
                    .dependencies
                    .iter()
                    .filter_map(|&idx| self.tasks.get(idx).and_then(|t| t.node_id.as_deref()))
                    .collect();

                for ctx_id in ctx_ids {
                    if !dep_node_ids.contains(ctx_id.as_str()) {
                        report.add(
                            HorsiesError::new(format!(
                                "task at index {} workflow_ctx_from '{}' is not a dependency node_id",
                                task.index, ctx_id,
                            ))
                            .with_code(ErrorCode::WorkflowInvalidCtxFrom),
                        );
                    }
                }
            }

            // args_json must not be used with args_from or workflow_ctx_from.
            let has_args_from = !task.args_from.is_empty();
            let has_ctx_from = task.workflow_ctx_from.is_some();
            if task.args_json.is_some() && (has_args_from || has_ctx_from) {
                let mut err = HorsiesError::new(
                    "positional args not allowed when using args_from or workflow_ctx_from",
                )
                .with_code(ErrorCode::WorkflowArgsWithInjection)
                .with_help(
                    "move static inputs into kwargs_json when using args_from/workflow_ctx_from",
                );
                if has_args_from {
                    err = err.with_note("args_json is set alongside args_from");
                }
                if has_ctx_from {
                    err = err.with_note("args_json is set alongside workflow_ctx_from");
                }
                report.add(err);
            }

            // Validate kwargs_json is a JSON object when present.
            if let Some(ref kwargs_json) = task.kwargs_json {
                let parsed = serde_json::from_str::<serde_json::Value>(kwargs_json);
                match parsed {
                    Ok(serde_json::Value::Object(map)) => {
                        // Reserved key must not be provided explicitly.
                        if map.contains_key(WORKFLOW_CTX_KWARG) {
                            report.add(
                                HorsiesError::new(format!(
                                    "kwargs contains reserved key '{}'",
                                    WORKFLOW_CTX_KWARG
                                ))
                                .with_code(ErrorCode::WorkflowInvalidKwargKey)
                                .with_help("remove the reserved workflow context key"),
                            );
                        }

                        // Detect collisions between kwargs and args_from keys.
                        let mut collisions: Vec<String> = task
                            .args_from
                            .keys()
                            .filter(|k| map.contains_key(*k))
                            .cloned()
                            .collect();
                        collisions.sort();
                        if !collisions.is_empty() {
                            report.add(
                                HorsiesError::new("kwargs and args_from contain overlapping keys")
                                    .with_code(ErrorCode::WorkflowInvalidKwargKey)
                                    .with_note(format!("overlapping keys: {:?}", collisions))
                                    .with_help(
                                        "use distinct keys; args_from values override kwargs",
                                    ),
                            );
                        }
                    }
                    Ok(_) => {
                        report.add(
                            HorsiesError::new("kwargs_json must be a JSON object")
                                .with_code(ErrorCode::WorkflowInvalidKwargKey)
                                .with_help("pass a JSON object string to .kwargs_json(...)"),
                        );
                    }
                    Err(e) => {
                        report.add(
                            HorsiesError::new(format!("kwargs_json is not valid JSON: {}", e))
                                .with_code(ErrorCode::WorkflowInvalidKwargKey)
                                .with_help("ensure kwargs_json is a valid JSON object string"),
                        );
                    }
                }
            }

            // Reserved workflow ctx key must not be used in args_from.
            if task.args_from.contains_key(WORKFLOW_CTX_KWARG) {
                report.add(
                    HorsiesError::new(format!(
                        "args_from contains reserved key '{}'",
                        WORKFLOW_CTX_KWARG
                    ))
                    .with_code(ErrorCode::WorkflowInvalidKwargKey)
                    .with_help("remove the reserved workflow context key"),
                );
            }
        }
    }

    // -----------------------------------------------------------------------
    // Phase 4: Output and policy validation
    // -----------------------------------------------------------------------

    fn validate_output_and_policy(&self, report: &mut ValidationReport) {
        let task_count = self.tasks.len();

        // Output index in range.
        if let Some(ref output_ref) = self.output_ref {
            if output_ref.index >= task_count {
                report.add(
                    HorsiesError::new(format!(
                        "output task index {} is out of range (0..{})",
                        output_ref.index, task_count,
                    ))
                    .with_code(ErrorCode::WorkflowInvalidOutput),
                );
            }
        }

        // Success policy indices in range.
        if let Some(ref policy) = self.success_policy {
            for (case_idx, case) in policy.cases.iter().enumerate() {
                for &req_idx in &case.required_indices {
                    if req_idx >= task_count {
                        report.add(
                            HorsiesError::new(format!(
                                "success_policy case {} references task index {} which is out of range (0..{})",
                                case_idx, req_idx, task_count,
                            ))
                            .with_code(ErrorCode::WorkflowInvalidSuccessPolicy),
                        );
                    }
                }
            }

            if let Some(ref optional) = policy.optional_indices {
                for &opt_idx in optional {
                    if opt_idx >= task_count {
                        report.add(
                            HorsiesError::new(format!(
                                "success_policy optional index {} is out of range (0..{})",
                                opt_idx, task_count,
                            ))
                            .with_code(ErrorCode::WorkflowInvalidSuccessPolicy),
                        );
                    }
                }
            }
        }
    }

    // -----------------------------------------------------------------------
    // Phase 5: Join semantics validation
    // -----------------------------------------------------------------------

    fn validate_join_semantics(&self, report: &mut ValidationReport) {
        for task in &self.tasks {
            let dep_count = task.dependencies.len() as i32;

            match task.join {
                JoinType::Quorum => match task.min_success {
                    None => {
                        report.add(
                            HorsiesError::new(format!(
                                "task at index {} uses join=quorum but min_success is not set",
                                task.index,
                            ))
                            .with_code(ErrorCode::WorkflowInvalidJoin)
                            .with_help("set min_success when using quorum join"),
                        );
                    }
                    Some(min) if min < 1 => {
                        report.add(
                            HorsiesError::new(format!(
                                "task at index {} has min_success={} which must be >= 1",
                                task.index, min,
                            ))
                            .with_code(ErrorCode::WorkflowInvalidJoin),
                        );
                    }
                    Some(min) if dep_count > 0 && min > dep_count => {
                        report.add(
                            HorsiesError::new(format!(
                                "task at index {} has min_success={} but only {} dependencies",
                                task.index, min, dep_count,
                            ))
                            .with_code(ErrorCode::WorkflowInvalidJoin)
                            .with_help("min_success must be <= dependency count"),
                        );
                    }
                    _ => {}
                },
                JoinType::All | JoinType::Any => {
                    if task.min_success.is_some() {
                        report.add(
                            HorsiesError::new(format!(
                                "task at index {} uses join={} but min_success is set",
                                task.index, task.join,
                            ))
                            .with_code(ErrorCode::WorkflowInvalidJoin)
                            .with_help("min_success is only valid with join=quorum"),
                        );
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::workflow::policy::SuccessCase;

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    fn simple_node(name: &str) -> TaskNode<()> {
        TaskNode::raw(name)
    }

    fn node_with_id(name: &str, id: &str) -> TaskNode<()> {
        TaskNode::raw(name).node_id(id)
    }

    // -----------------------------------------------------------------------
    // Phase 1: Node ID tests
    // -----------------------------------------------------------------------

    #[test]
    fn e001_empty_name() {
        let mut b = WorkflowSpecBuilder::new("");
        b.task(simple_node("a"));
        let err = b.build().unwrap_err();
        assert_eq!(err.code, Some(ErrorCode::WorkflowNoName));
    }

    #[test]
    fn e002_no_nodes() {
        let b = WorkflowSpecBuilder::new("empty");
        let err = b.build().unwrap_err();
        assert_eq!(err.code, Some(ErrorCode::WorkflowNoNodes));
    }

    #[test]
    fn e003_invalid_node_id_pattern() {
        let mut b = WorkflowSpecBuilder::new("wf");
        b.task(node_with_id("a", "has spaces"));
        let err = b.build().unwrap_err();
        assert_eq!(err.code, Some(ErrorCode::WorkflowInvalidNodeId));
    }

    #[test]
    fn e003_node_id_too_long() {
        let mut b = WorkflowSpecBuilder::new("wf");
        let long_id = "a".repeat(129);
        b.task(node_with_id("a", &long_id));
        let err = b.build().unwrap_err();
        assert_eq!(err.code, Some(ErrorCode::WorkflowInvalidNodeId));
    }

    #[test]
    fn e004_duplicate_node_id() {
        let mut b = WorkflowSpecBuilder::new("wf");
        b.task(node_with_id("a", "same_id"));
        b.task(node_with_id("b", "same_id"));
        let err = b.build().unwrap_err();
        assert_eq!(err.code, Some(ErrorCode::WorkflowDuplicateNodeId));
    }

    #[test]
    fn auto_assign_node_ids() {
        let mut b = WorkflowSpecBuilder::new("wf");
        b.task(simple_node("fetch_data"));
        b.task(simple_node("process"));
        let spec = b.build().unwrap();
        assert_eq!(spec.tasks[0].node_id.as_deref(), Some("fetch_data:0"));
        assert_eq!(spec.tasks[1].node_id.as_deref(), Some("process:1"));
    }

    #[test]
    fn valid_node_id_chars() {
        let mut b = WorkflowSpecBuilder::new("wf");
        b.task(node_with_id("a", "step-1:sub_task.v2"));
        let spec = b.build().unwrap();
        assert_eq!(spec.tasks[0].node_id.as_deref(), Some("step-1:sub_task.v2"),);
    }

    #[test]
    fn slugify_special_chars() {
        let mut b = WorkflowSpecBuilder::new("wf");
        b.task(simple_node("my task/v2"));
        let spec = b.build().unwrap();
        assert_eq!(spec.tasks[0].node_id.as_deref(), Some("my_task_v2:0"));
    }

    // -----------------------------------------------------------------------
    // Phase 2: DAG structure tests
    // -----------------------------------------------------------------------

    #[test]
    fn e005_no_root_tasks() {
        // Both tasks depend on each other — no roots and a cycle.
        // Phase 2 collects both errors, so we get a combined error.
        let mut b = WorkflowSpecBuilder::new("wf");
        let a_ref = b.task(simple_node("a"));
        let _b_ref = b.task(TaskNode::<()>::raw("b").waits_for(a_ref));
        // Make a depend on b to remove all roots
        b.tasks[0].dependencies.push(1);
        let err = b.build().unwrap_err();
        let msg = format!("{}", err);
        // Should mention either no root tasks or cycle
        assert!(
            msg.contains("root") || msg.contains("cycle"),
            "expected root/cycle error, got: {}",
            msg,
        );
    }

    #[test]
    fn e006_dependency_out_of_range() {
        // Node "b" depends on index 99 which does not exist.
        // Node "a" is still a valid root, so no root error.
        let mut b = WorkflowSpecBuilder::new("wf");
        b.task(simple_node("a"));
        let _b_ref = b.task(simple_node("b"));
        b.tasks[1].dependencies.push(99);
        let err = b.build().unwrap_err();
        assert_eq!(err.code, Some(ErrorCode::WorkflowInvalidDependency));
    }

    #[test]
    fn e007_self_cycle() {
        // Node "b" depends on itself. Node "a" is the root.
        // Self-dep triggers both the explicit self-dep check and Kahn's cycle detection,
        // so we get a combined error (2 errors, code is None on the wrapper).
        let mut b = WorkflowSpecBuilder::new("wf");
        b.task(simple_node("a"));
        let _b_ref = b.task(simple_node("b"));
        b.tasks[1].dependencies.push(1);
        let err = b.build().unwrap_err();
        let msg = format!("{}", err);
        assert!(
            msg.contains("self-dependency") || msg.contains("cycle"),
            "expected self-dep or cycle error, got: {}",
            msg,
        );
    }

    #[test]
    fn e007_cycle_detected() {
        // root -> a -> b -> (back to a) creates a cycle, but root is still a root.
        let mut b = WorkflowSpecBuilder::new("wf");
        let root = b.task(simple_node("root"));
        let a_ref = b.task(TaskNode::<()>::raw("a").waits_for(root));
        let b_ref = b.task(TaskNode::<()>::raw("b").waits_for(a_ref));
        // Create cycle: a also depends on b
        b.tasks[1].dependencies.push(b_ref.index);
        let err = b.build().unwrap_err();
        assert_eq!(err.code, Some(ErrorCode::WorkflowCycleDetected));
    }

    #[test]
    fn duplicate_waits_for_edge_is_not_a_cycle() {
        // Parity with horsies (Python) PR #22: a dependency declared twice is one
        // DAG edge, not a cycle. Rust dedups by index at insertion time
        // (waits_for / waits_for_all), so the spec builds cleanly.
        let mut b = WorkflowSpecBuilder::new("wf");
        let a = b.task(simple_node("a"));
        let _bb = b.task(TaskNode::<()>::raw("b").waits_for(a).waits_for(a));
        let spec = b.build().unwrap();
        assert_eq!(spec.tasks[1].dependencies, vec![0]);
    }

    #[test]
    fn waits_for_all_dedups_preserving_first_seen_order() {
        // Parity with horsies PR #30: mixed duplicates collapse to first-seen
        // order. [a, b, a] -> dependencies [a, b].
        let mut b = WorkflowSpecBuilder::new("wf");
        let a = b.task(simple_node("a"));
        let bb = b.task(simple_node("b"));
        let c = b.task(TaskNode::<()>::raw("c").waits_for_all(&[a, bb, a]));
        let spec = b.build().unwrap();
        assert_eq!(spec.tasks[c.index].dependencies, vec![a.index, bb.index]);
    }

    #[test]
    fn duplicate_dependencies_do_not_trigger_false_cycle() {
        // Parity with horsies PR #30: cycle detection must be robust to duplicate
        // dependency indices independent of waits_for's insertion-time dedup.
        // Push duplicates directly to bypass the dedup and exercise the raw
        // Kahn's algorithm. Rust's adjacency carries matching multiplicity, so
        // increments and decrements stay balanced (no false cycle).
        let mut b = WorkflowSpecBuilder::new("wf");
        let a = b.task(simple_node("a"));
        b.task(TaskNode::<()>::raw("b").waits_for(a));
        b.tasks[1].dependencies.push(a.index);
        b.tasks[1].dependencies.push(a.index);
        assert_eq!(b.tasks[1].dependencies, vec![0, 0, 0]);
        let spec = b.build().unwrap();
        assert_eq!(spec.tasks.len(), 2);
    }

    #[test]
    fn linear_chain_valid() {
        let mut b = WorkflowSpecBuilder::new("wf");
        let a = b.task(simple_node("a"));
        let bb = b.task(TaskNode::<()>::raw("b").waits_for(a));
        b.task(TaskNode::<()>::raw("c").waits_for(bb));
        let spec = b.build().unwrap();
        assert_eq!(spec.tasks.len(), 3);
    }

    #[test]
    fn diamond_dag_valid() {
        let mut b = WorkflowSpecBuilder::new("wf");
        let a = b.task(simple_node("a"));
        let bb = b.task(TaskNode::<()>::raw("b").waits_for(a));
        let c = b.task(TaskNode::<()>::raw("c").waits_for(a));
        b.task(TaskNode::<()>::raw("d").waits_for_all(&[bb, c]));
        let spec = b.build().unwrap();
        assert_eq!(spec.tasks.len(), 4);
    }

    // -----------------------------------------------------------------------
    // Phase 3: Data flow tests
    // -----------------------------------------------------------------------

    #[test]
    fn e008_args_from_not_in_deps() {
        let mut b = WorkflowSpecBuilder::new("wf");
        let a = b.task(simple_node("a"));
        b.task(simple_node("b"));
        // b has args_from referencing a, but a is not in b's deps
        b.tasks[1].args_from.insert("data".to_owned(), a.index);
        let err = b.build().unwrap_err();
        assert_eq!(err.code, Some(ErrorCode::WorkflowInvalidArgsFrom));
    }

    #[test]
    fn e008_args_from_out_of_range() {
        let mut b = WorkflowSpecBuilder::new("wf");
        b.task(simple_node("a"));
        b.tasks[0].args_from.insert("data".to_owned(), 99);
        let err = b.build().unwrap_err();
        assert_eq!(err.code, Some(ErrorCode::WorkflowInvalidArgsFrom));
    }

    #[test]
    fn args_from_valid() {
        let mut b = WorkflowSpecBuilder::new("wf");
        let a = b.task(simple_node("a"));
        b.task(TaskNode::<()>::raw("b").raw_arg_from("data", a.into()));
        let spec = b.build().unwrap();
        assert_eq!(spec.tasks[1].args_from.get("data"), Some(&0));
    }

    #[test]
    fn e009_ctx_from_not_in_deps() {
        let mut b = WorkflowSpecBuilder::new("wf");
        let a = b.task(node_with_id("a", "step_a"));
        b.task(TaskNode::<()>::raw("b").workflow_ctx_from([a]));
        let err = b.build().unwrap_err();
        assert_eq!(err.code, Some(ErrorCode::WorkflowInvalidCtxFrom));
    }

    #[test]
    fn ctx_from_valid() {
        let mut b = WorkflowSpecBuilder::new("wf");
        let a = b.task(node_with_id("a", "step_a"));
        b.task(TaskNode::<()>::raw("b").waits_for(a).workflow_ctx_from([a]));
        let spec = b.build().unwrap();
        assert_eq!(
            spec.tasks[1].workflow_ctx_from,
            Some(vec!["step_a".to_owned()]),
        );
    }

    #[test]
    fn e016_args_with_args_from_rejected() {
        let mut b = WorkflowSpecBuilder::new("wf");
        let a = b.task(simple_node("a"));
        b.task(
            TaskNode::<()>::raw("b")
                .args(r#"[1]"#)
                .raw_arg_from("input", a.into()),
        );
        let err = b.build().unwrap_err();
        assert_eq!(err.code, Some(ErrorCode::WorkflowArgsWithInjection));
    }

    #[test]
    fn e016_args_with_workflow_ctx_from_rejected() {
        let mut b = WorkflowSpecBuilder::new("wf");
        let a = b.task(node_with_id("a", "step_a"));
        b.task(
            TaskNode::<()>::raw("b")
                .args(r#"[1]"#)
                .waits_for(a)
                .workflow_ctx_from([a]),
        );
        let err = b.build().unwrap_err();
        assert_eq!(err.code, Some(ErrorCode::WorkflowArgsWithInjection));
    }

    #[test]
    fn e019_kwargs_must_be_object() {
        let mut b = WorkflowSpecBuilder::new("wf");
        b.task(TaskNode::<()>::raw("a").kwargs("[1, 2, 3]"));
        let err = b.build().unwrap_err();
        assert_eq!(err.code, Some(ErrorCode::WorkflowInvalidKwargKey));
    }

    #[test]
    fn e019_kwargs_args_from_collision() {
        let mut b = WorkflowSpecBuilder::new("wf");
        let a = b.task(simple_node("a"));
        b.task(
            TaskNode::<()>::raw("b")
                .kwargs(r#"{"data": 1}"#)
                .raw_arg_from("data", a.into()),
        );
        let err = b.build().unwrap_err();
        assert_eq!(err.code, Some(ErrorCode::WorkflowInvalidKwargKey));
    }

    // -----------------------------------------------------------------------
    // Phase 4: Output and policy tests
    // -----------------------------------------------------------------------

    #[test]
    fn e011_output_out_of_range() {
        let mut b = WorkflowSpecBuilder::new("wf");
        b.task(simple_node("a"));
        b.output(NodeRef { index: 99 });
        let err = b.build().unwrap_err();
        assert_eq!(err.code, Some(ErrorCode::WorkflowInvalidOutput));
    }

    #[test]
    fn output_valid() {
        let mut b = WorkflowSpecBuilder::new("wf");
        let a = b.task(simple_node("a"));
        b.output(a);
        let spec = b.build().unwrap();
        assert_eq!(spec.output_index, Some(0));
    }

    #[test]
    fn e012_success_policy_index_out_of_range() {
        let mut b = WorkflowSpecBuilder::new("wf");
        b.task(simple_node("a"));
        b.success_policy(SuccessPolicy {
            cases: vec![SuccessCase {
                required_indices: vec![0, 99],
                name: None,
            }],
            optional_indices: None,
        });
        let err = b.build().unwrap_err();
        assert_eq!(err.code, Some(ErrorCode::WorkflowInvalidSuccessPolicy));
    }

    #[test]
    fn e012_success_policy_optional_out_of_range() {
        let mut b = WorkflowSpecBuilder::new("wf");
        b.task(simple_node("a"));
        b.success_policy(SuccessPolicy {
            cases: vec![SuccessCase {
                required_indices: vec![0],
                name: None,
            }],
            optional_indices: Some(vec![50]),
        });
        let err = b.build().unwrap_err();
        assert_eq!(err.code, Some(ErrorCode::WorkflowInvalidSuccessPolicy));
    }

    // -----------------------------------------------------------------------
    // Phase 5: Join semantics tests
    // -----------------------------------------------------------------------

    #[test]
    fn e013_quorum_without_min_success() {
        let mut b = WorkflowSpecBuilder::new("wf");
        let a = b.task(simple_node("a"));
        b.task(simple_node("b"));
        // Manually set join=quorum without min_success
        b.tasks[1].dependencies.push(a.index);
        b.tasks[1].join = JoinType::Quorum;
        b.tasks[1].min_success = None;
        let err = b.build().unwrap_err();
        assert_eq!(err.code, Some(ErrorCode::WorkflowInvalidJoin));
    }

    #[test]
    fn e013_min_success_zero() {
        let mut b = WorkflowSpecBuilder::new("wf");
        let a = b.task(simple_node("a"));
        b.task(TaskNode::<()>::raw("b").waits_for(a));
        b.tasks[1].join = JoinType::Quorum;
        b.tasks[1].min_success = Some(0);
        let err = b.build().unwrap_err();
        assert_eq!(err.code, Some(ErrorCode::WorkflowInvalidJoin));
    }

    #[test]
    fn e013_min_success_exceeds_deps() {
        let mut b = WorkflowSpecBuilder::new("wf");
        let a = b.task(simple_node("a"));
        b.task(TaskNode::<()>::raw("b").waits_for(a).join_quorum(5));
        let err = b.build().unwrap_err();
        assert_eq!(err.code, Some(ErrorCode::WorkflowInvalidJoin));
    }

    #[test]
    fn e013_all_with_min_success() {
        let mut b = WorkflowSpecBuilder::new("wf");
        let a = b.task(simple_node("a"));
        b.task(TaskNode::<()>::raw("b").waits_for(a));
        b.tasks[1].join = JoinType::All;
        b.tasks[1].min_success = Some(1);
        let err = b.build().unwrap_err();
        assert_eq!(err.code, Some(ErrorCode::WorkflowInvalidJoin));
    }

    #[test]
    fn e013_any_with_min_success() {
        let mut b = WorkflowSpecBuilder::new("wf");
        let a = b.task(simple_node("a"));
        b.task(TaskNode::<()>::raw("b").waits_for(a));
        b.tasks[1].join = JoinType::Any;
        b.tasks[1].min_success = Some(1);
        let err = b.build().unwrap_err();
        assert_eq!(err.code, Some(ErrorCode::WorkflowInvalidJoin));
    }

    #[test]
    fn valid_quorum_join() {
        let mut b = WorkflowSpecBuilder::new("wf");
        let a = b.task(simple_node("a"));
        let bb = b.task(simple_node("b"));
        let c = b.task(simple_node("c"));
        b.task(
            TaskNode::<()>::raw("d")
                .waits_for_all(&[a, bb, c])
                .join_quorum(2),
        );
        let spec = b.build().unwrap();
        assert_eq!(spec.tasks[3].join, JoinType::Quorum);
        assert_eq!(spec.tasks[3].min_success, Some(2));
    }

    // -----------------------------------------------------------------------
    // Phase gating tests
    // -----------------------------------------------------------------------

    #[test]
    fn phase1_errors_block_phase2() {
        let mut b = WorkflowSpecBuilder::new("wf");
        b.task(node_with_id("a", "has spaces")); // Phase 1 error
        b.tasks[0].dependencies.push(99); // Phase 2 error (would be caught)
        let err = b.build().unwrap_err();
        // Should only get phase 1 error
        assert_eq!(err.code, Some(ErrorCode::WorkflowInvalidNodeId));
    }

    #[test]
    fn phase2_errors_block_phase3() {
        // root is valid, but b depends on index 99 (phase 2 error).
        // Also add bad args_from on b (phase 3 error) — should not be reached.
        let mut b = WorkflowSpecBuilder::new("wf");
        b.task(simple_node("root"));
        let _b_ref = b.task(simple_node("b"));
        b.tasks[1].dependencies.push(99); // phase 2: invalid dep
        b.tasks[1].args_from.insert("x".to_owned(), 88); // phase 3: bad args_from
        let err = b.build().unwrap_err();
        // Should get phase 2 error (invalid dep), not phase 3
        assert_eq!(err.code, Some(ErrorCode::WorkflowInvalidDependency));
    }

    // -----------------------------------------------------------------------
    // WorkflowSpec properties
    // -----------------------------------------------------------------------

    #[test]
    fn spec_on_error_default_is_fail() {
        let mut b = WorkflowSpecBuilder::new("wf");
        b.task(simple_node("a"));
        let spec = b.build().unwrap();
        assert_eq!(spec.on_error, OnError::Fail);
    }

    #[test]
    fn spec_on_error_pause() {
        let mut b = WorkflowSpecBuilder::new("wf");
        b.task(simple_node("a"));
        b.on_error(OnError::Pause);
        let spec = b.build().unwrap();
        assert_eq!(spec.on_error, OnError::Pause);
    }

    #[test]
    fn spec_no_output_no_policy() {
        let mut b = WorkflowSpecBuilder::new("wf");
        b.task(simple_node("a"));
        let spec = b.build().unwrap();
        assert!(spec.output_index.is_none());
        assert!(spec.success_policy.is_none());
    }

    #[test]
    fn full_spec_with_everything() {
        let mut b = WorkflowSpecBuilder::new("data_pipeline");
        let fetch = b.task(node_with_id("fetch", "fetch"));
        let parse = b.task(
            TaskNode::<String>::raw("parse")
                .raw_arg_from("raw", fetch.into())
                .node_id("parse"),
        );
        let persist = b.task(
            TaskNode::<()>::raw("persist")
                .raw_arg_from("data", parse.into())
                .node_id("persist"),
        );
        b.on_error(OnError::Fail);
        b.output(persist);
        b.success_policy(SuccessPolicy {
            cases: vec![SuccessCase {
                required_indices: vec![2],
                name: None,
            }],
            optional_indices: Some(vec![0]),
        });

        let spec = b.build().unwrap();
        assert_eq!(spec.name, "data_pipeline");
        assert_eq!(spec.tasks.len(), 3);
        assert_eq!(spec.output_index, Some(2));
        assert!(spec.success_policy.is_some());
        assert_eq!(spec.on_error, OnError::Fail);
    }

    #[test]
    fn build_registered_basic() {
        let mut b = WorkflowSpecBuilder::new("wf");
        b.task(simple_node("a"));
        let registered = b.build_registered().unwrap();
        assert_eq!(registered.spec.name, "wf");
    }

    #[test]
    fn subworkflow_default_accepted() {
        use crate::core::workflow::sub_workflow::SubWorkflowNode;

        let mut b = WorkflowSpecBuilder::new("wf");
        let a = b.task(simple_node("a"));
        b.sub_workflow(
            SubWorkflowNode::new("child_wf")
                .waits_for(a)
                .node_id("child"),
        );
        let spec = b.build().unwrap();
        assert!(spec.tasks[1].is_subworkflow);
    }

    #[derive(serde::Serialize, crate::WorkflowInput)]
    struct ChildWorkflowParams {
        source: crate::TaskResult<String>,
        limit: usize,
    }

    #[test]
    fn subworkflow_kwargs_json_preserved_after_build() {
        use crate::core::workflow::sub_workflow::SubWorkflowNode;

        let mut b = WorkflowSpecBuilder::new("wf");
        b.sub_workflow(
            SubWorkflowNode::<ChildWorkflowParams, ()>::typed("child_wf")
                .set(ChildWorkflowParams::field_limit(), 10)
                .unwrap()
                .node_id("child"),
        );
        let spec = b.build().unwrap();
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(
                spec.tasks[0].kwargs_json.as_deref().unwrap()
            )
            .unwrap(),
            serde_json::json!({"limit":10})
        );
    }

    #[test]
    fn subworkflow_set_and_arg_from_accepted() {
        use crate::core::workflow::sub_workflow::SubWorkflowNode;

        let mut b = WorkflowSpecBuilder::new("wf");
        let producer = b.task(TaskNode::<String>::raw("producer").node_id("producer"));
        b.sub_workflow(
            SubWorkflowNode::<ChildWorkflowParams, ()>::typed("child_wf")
                .set(ChildWorkflowParams::field_limit(), 25)
                .unwrap()
                .arg_from(ChildWorkflowParams::field_source(), producer)
                .node_id("child"),
        );

        let spec = b.build().unwrap();
        assert_eq!(spec.tasks[1].args_from.get("source"), Some(&0));
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(
                spec.tasks[1].kwargs_json.as_deref().unwrap()
            )
            .unwrap(),
            serde_json::json!({"limit":25})
        );
        assert_eq!(spec.tasks[1].dependencies, vec![0]);
    }

    // -- Spec isolation tests (ported from Python test_workflow_spec_isolation.py) --

    #[test]
    fn node_reuse_across_specs_independent_ids() {
        // Building two specs from fresh TaskNodes with the same name produces
        // independent node_ids (auto-assigned from task_name:index).
        let mut b1 = WorkflowSpecBuilder::new("alpha");
        b1.task(TaskNode::<()>::raw("task_a"));
        let spec1 = b1.build().unwrap();

        let mut b2 = WorkflowSpecBuilder::new("beta");
        b2.task(TaskNode::<()>::raw("task_a"));
        let spec2 = b2.build().unwrap();

        assert_eq!(spec1.tasks[0].node_id.as_deref(), Some("task_a:0"));
        assert_eq!(spec2.tasks[0].node_id.as_deref(), Some("task_a:0"));
    }

    #[test]
    fn explicit_node_id_preserved_on_reuse() {
        let mut b1 = WorkflowSpecBuilder::new("alpha");
        b1.task(TaskNode::<()>::raw("task_a").node_id("custom-stable-id"));
        let spec1 = b1.build().unwrap();

        let mut b2 = WorkflowSpecBuilder::new("beta");
        b2.task(TaskNode::<()>::raw("task_a").node_id("custom-stable-id"));
        let spec2 = b2.build().unwrap();

        assert_eq!(spec1.tasks[0].node_id.as_deref(), Some("custom-stable-id"));
        assert_eq!(spec2.tasks[0].node_id.as_deref(), Some("custom-stable-id"));
    }

    #[test]
    fn kwargs_json_preserved_after_build() {
        let mut b = WorkflowSpecBuilder::new("wf");
        b.task(
            TaskNode::<()>::raw("task_a")
                .kwargs(r#"{"url":"https://example.com"}"#)
                .node_id("a"),
        );
        let spec = b.build().unwrap();
        assert_eq!(
            spec.tasks[0].kwargs_json.as_deref(),
            Some(r#"{"url":"https://example.com"}"#),
        );
    }

    #[test]
    fn spec_from_cloned_any_nodes() {
        // Build one spec, clone its AnyNodes, verify independence.
        let mut b1 = WorkflowSpecBuilder::new("first");
        b1.task(simple_node("a").node_id("a"));
        let spec1 = b1.build().unwrap();

        let cloned = spec1.tasks[0].clone();

        let mut b2 = WorkflowSpecBuilder::new("second");
        b2.task(TaskNode::<()>::raw("a").node_id("a"));
        let spec2 = b2.build().unwrap();

        assert_eq!(spec1.tasks[0].task_name, spec2.tasks[0].task_name);
        assert_eq!(spec1.tasks[0].node_id, spec2.tasks[0].node_id);
        assert_eq!(cloned.task_name, "a");
    }

    #[test]
    fn failed_validation_does_not_produce_spec() {
        // An invalid output ref causes build() to fail.
        let mut b = WorkflowSpecBuilder::new("wf");
        b.task(simple_node("a").node_id("a"));
        b.output(NodeRef { index: 99 }); // out of range
        let result = b.build();
        assert!(result.is_err());
    }
}
