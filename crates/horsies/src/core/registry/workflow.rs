use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use crate::core::error::{ErrorCode, HorsiesError};
use crate::core::workflow::spec::WorkflowSpec;

/// Callback type for dynamically building a child WorkflowSpec at runtime.
///
/// Receives the parent sub-workflow node's serialized args and kwargs
/// (from `args_from` merging) and returns a new `WorkflowSpec`.
/// This enables sub-workflows to be parameterized by upstream results.
///
/// The default behavior (when `spec_builder` is `None`) is to use the
/// static registered spec.
pub type SpecBuilderFn = Arc<
    dyn Fn(Option<&str>, Option<&str>) -> Result<WorkflowSpec, HorsiesError>
        + Send
        + Sync
        + 'static,
>;

/// A registered workflow spec with an optional dynamic builder.
#[derive(Clone)]
pub struct RegisteredWorkflowSpec {
    /// The validated, immutable workflow specification.
    pub spec: WorkflowSpec,

    /// Optional callback for dynamically building the child spec at runtime.
    ///
    /// When set, the engine calls this instead of using `spec` directly
    /// when launching a sub-workflow. The parent task's merged args/kwargs
    /// are passed in, allowing the child DAG to vary based on upstream results.
    ///
    /// Equivalent to Python's `workflow_def.build_with(app, *task_args, **kwargs)`.
    pub spec_builder: Option<SpecBuilderFn>,
}

impl std::fmt::Debug for RegisteredWorkflowSpec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RegisteredWorkflowSpec")
            .field("spec_name", &self.spec.name)
            .field("has_spec_builder", &self.spec_builder.is_some())
            .finish()
    }
}

/// Registry mapping workflow names to their registered specs.
///
/// Used by the engine to look up sub-workflow specs at runtime.
/// Thread-safe for shared reads (behind `Arc` in the worker).
pub struct WorkflowSpecRegistry {
    specs: HashMap<String, RegisteredWorkflowSpec>,
    /// Pre-computed map of task_name -> serialized retry options JSON.
    ///
    /// Populated by `Horsies::register_workflow()` from the task registry.
    /// Used by the engine to resolve task options for dynamically built
    /// specs (via `spec_builder`) that bypass `register_workflow()`.
    task_options_map: HashMap<String, String>,
}

impl Clone for WorkflowSpecRegistry {
    fn clone(&self) -> Self {
        Self {
            specs: self.specs.clone(),
            task_options_map: self.task_options_map.clone(),
        }
    }
}

impl WorkflowSpecRegistry {
    /// Create an empty registry.
    pub fn new() -> Self {
        Self {
            specs: HashMap::new(),
            task_options_map: HashMap::new(),
        }
    }

    /// Register a workflow spec. Returns an error if the name is already taken
    /// or if registering this spec would create a sub-workflow cycle.
    pub fn register(&mut self, registered: RegisteredWorkflowSpec) -> Result<(), HorsiesError> {
        let name = registered.spec.name.clone();

        if name.is_empty() {
            return Err(HorsiesError::new("workflow spec name must not be empty")
                .with_code(ErrorCode::WorkflowNoName));
        }

        if self.specs.contains_key(&name) {
            return Err(
                HorsiesError::new(format!("duplicate workflow spec name: '{}'", name))
                    .with_code(ErrorCode::WorkflowDuplicateNodeId)
                    .with_help("each workflow spec must have a unique name"),
            );
        }

        // Check for sub-workflow cycles before inserting.
        // Temporarily insert the new spec, check for cycles, then remove if a
        // cycle is found.
        self.specs.insert(name.clone(), registered);

        if let Some(cycle_path) = self.detect_subworkflow_cycle(&name) {
            // Remove the spec we just inserted since it creates a cycle.
            let removed = self
                .specs
                .remove(&name)
                .expect("spec was just inserted above");
            let cycle_str = cycle_path.join(" -> ");
            return Err(HorsiesError::new("cycle detected in nested workflows")
                .with_code(ErrorCode::WorkflowCycleDetected)
                .with_note(format!(
                    "registering '{}' creates a circular reference: {}",
                    removed.spec.name, cycle_str,
                ))
                .with_note("cycles in nested workflows are not allowed")
                .with_help("remove the circular SubWorkflowNode reference"));
        }

        Ok(())
    }

    /// Detect sub-workflow cycles reachable from `start_name`.
    ///
    /// Uses DFS with a recursion stack. Returns `Some(cycle_path)` if a cycle
    /// is found, `None` otherwise. The cycle_path shows the chain of workflow
    /// names forming the cycle.
    fn detect_subworkflow_cycle(&self, start_name: &str) -> Option<Vec<String>> {
        let mut visited: HashSet<&str> = HashSet::new();
        let mut stack: Vec<&str> = Vec::new();

        fn dfs<'a>(
            current: &'a str,
            specs: &'a HashMap<String, RegisteredWorkflowSpec>,
            visited: &mut HashSet<&'a str>,
            stack: &mut Vec<&'a str>,
        ) -> Option<Vec<String>> {
            if stack.contains(&current) {
                // Found a cycle -- build the cycle path from the first
                // occurrence of `current` in the stack to the end.
                let pos = stack
                    .iter()
                    .position(|&s| s == current)
                    .expect("current must be in stack (cycle detected)");
                let mut cycle_path: Vec<String> =
                    stack[pos..].iter().map(|s| s.to_string()).collect();
                cycle_path.push(current.to_string());
                return Some(cycle_path);
            }

            if visited.contains(current) {
                return None;
            }

            visited.insert(current);
            stack.push(current);

            if let Some(registered) = specs.get(current) {
                for task in &registered.spec.tasks {
                    if task.is_subworkflow {
                        if let Some(child_name) = task.task_name.strip_prefix("__sub_workflow:") {
                            if let Some(cycle) = dfs(child_name, specs, visited, stack) {
                                return Some(cycle);
                            }
                        }
                    }
                }
            }

            stack.pop();
            None
        }

        dfs(start_name, &self.specs, &mut visited, &mut stack)
    }

    /// Look up a registered workflow spec by name.
    pub fn get(&self, name: &str) -> Option<&RegisteredWorkflowSpec> {
        self.specs.get(name)
    }

    /// Look up a registered workflow spec by its `definition_key`.
    ///
    /// Scans all registered specs for a matching `definition_key`. Used by
    /// the engine and recovery to resolve child workflows from persisted DB rows.
    pub fn get_by_definition_key(&self, key: &str) -> Option<&RegisteredWorkflowSpec> {
        self.specs
            .values()
            .find(|r| r.spec.definition_key.as_deref() == Some(key))
    }

    /// Check if a workflow spec is registered.
    pub fn contains(&self, name: &str) -> bool {
        self.specs.contains_key(name)
    }

    /// Number of registered workflow specs.
    pub fn len(&self) -> usize {
        self.specs.len()
    }

    /// Whether the registry is empty.
    pub fn is_empty(&self) -> bool {
        self.specs.is_empty()
    }

    /// Remove a workflow spec from the registry by name.
    ///
    /// Returns the removed `RegisteredWorkflowSpec` if it was present,
    /// or `None` if no spec with that name was registered.
    ///
    /// Equivalent to Python's `unregister_workflow_spec(name)`.
    pub fn unregister(&mut self, name: &str) -> Option<RegisteredWorkflowSpec> {
        self.specs.remove(name)
    }

    /// Look up a specific node by workflow name and task index.
    ///
    /// Returns a reference to the `AnyNode` at the given index within the
    /// named workflow's task list, or `None` if the workflow is not registered
    /// or the index is out of range.
    ///
    /// Equivalent to Python's `get_node(workflow_name, task_index)`.
    pub fn get_node(
        &self,
        workflow_name: &str,
        task_index: usize,
    ) -> Option<&crate::core::workflow::node::AnyNode> {
        self.specs
            .get(workflow_name)
            .and_then(|r| r.spec.tasks.get(task_index))
    }

    /// Look up a task node (non-subworkflow) by workflow name and index.
    ///
    /// Returns `Some(&AnyNode)` only if the node exists and is NOT a
    /// subworkflow node. Returns `None` otherwise.
    ///
    /// Equivalent to Python's `get_task_node(workflow_name, task_index)`.
    pub fn get_task_node(
        &self,
        workflow_name: &str,
        task_index: usize,
    ) -> Option<&crate::core::workflow::node::AnyNode> {
        self.get_node(workflow_name, task_index)
            .filter(|node| !node.is_subworkflow)
    }

    /// Look up a subworkflow node by workflow name and index.
    ///
    /// Returns `Some(&AnyNode)` only if the node exists and IS a
    /// subworkflow node. Returns `None` otherwise.
    ///
    /// Equivalent to Python's `get_subworkflow_node(workflow_name, task_index)`.
    pub fn get_subworkflow_node(
        &self,
        workflow_name: &str,
        task_index: usize,
    ) -> Option<&crate::core::workflow::node::AnyNode> {
        self.get_node(workflow_name, task_index)
            .filter(|node| node.is_subworkflow)
    }

    /// Iterate over registered workflow specs.
    pub fn iter(&self) -> impl Iterator<Item = &RegisteredWorkflowSpec> {
        self.specs.values()
    }

    /// Iterate over registered workflow spec names.
    pub fn spec_names(&self) -> impl Iterator<Item = &str> {
        self.specs.keys().map(String::as_str)
    }

    /// Replace the task options map used for resolving retry options on
    /// dynamically built workflow specs.
    pub fn set_task_options_map(&mut self, map: HashMap<String, String>) {
        self.task_options_map = map;
    }

    /// Look up serialized retry options for a task name.
    ///
    /// Returns the JSON string if the task has retry options, or `None`.
    pub fn task_retry_options(&self, task_name: &str) -> Option<&str> {
        self.task_options_map.get(task_name).map(String::as_str)
    }

    /// Resolve task options on a mutable workflow spec's nodes using the
    /// stored task options map.
    ///
    /// This is the engine-facing entry point for dynamic specs built via
    /// `spec_builder` that bypass `register_workflow()`.
    pub fn resolve_spec_task_options(&self, spec: &mut crate::core::workflow::spec::WorkflowSpec) {
        crate::core::workflow::node::resolve_node_task_options(
            &mut spec.tasks,
            &|task_name| self.task_options_map.get(task_name).cloned(),
        );
    }
}

impl Default for WorkflowSpecRegistry {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Debug for WorkflowSpecRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WorkflowSpecRegistry")
            .field("spec_count", &self.specs.len())
            .field("spec_names", &self.specs.keys().collect::<Vec<_>>())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::workflow::node::TaskNode;
    use crate::core::workflow::spec::WorkflowSpecBuilder;

    fn make_spec(name: &str) -> WorkflowSpec {
        let mut builder = WorkflowSpecBuilder::new(name);
        builder.task(TaskNode::<()>::new("my_task"));
        builder.build().unwrap()
    }

    fn make_registered(name: &str) -> RegisteredWorkflowSpec {
        RegisteredWorkflowSpec {
            spec: make_spec(name),
            spec_builder: None,
        }
    }

    #[test]
    fn register_and_lookup() {
        let mut registry = WorkflowSpecRegistry::new();
        registry.register(make_registered("wf_a")).unwrap();
        assert!(registry.contains("wf_a"));
        assert!(registry.get("wf_a").is_some());
        assert_eq!(registry.len(), 1);
    }

    #[test]
    fn duplicate_name_rejected() {
        let mut registry = WorkflowSpecRegistry::new();
        registry.register(make_registered("wf_a")).unwrap();
        let result = registry.register(make_registered("wf_a"));
        assert!(result.is_err());
    }

    #[test]
    fn missing_returns_none() {
        let registry = WorkflowSpecRegistry::new();
        assert!(registry.get("missing").is_none());
        assert!(!registry.contains("missing"));
    }

    #[test]
    fn empty_registry() {
        let registry = WorkflowSpecRegistry::new();
        assert!(registry.is_empty());
        assert_eq!(registry.len(), 0);
    }

    #[test]
    fn spec_names_iterator() {
        let mut registry = WorkflowSpecRegistry::new();
        registry.register(make_registered("alpha")).unwrap();
        registry.register(make_registered("beta")).unwrap();
        let mut names: Vec<&str> = registry.spec_names().collect();
        names.sort();
        assert_eq!(names, vec!["alpha", "beta"]);
    }

    // -----------------------------------------------------------------------
    // Sub-workflow cycle detection tests
    // -----------------------------------------------------------------------

    /// Helper: create a spec that contains a sub-workflow node referencing
    /// the given child spec name.
    fn make_spec_with_sub(name: &str, child_spec_name: &str) -> WorkflowSpec {
        use crate::core::workflow::sub_workflow::SubWorkflowNode;

        let mut builder = WorkflowSpecBuilder::new(name);
        let root = builder.task(TaskNode::<()>::new("root_task"));
        builder.sub_workflow(
            SubWorkflowNode::new(child_spec_name)
                .waits_for(root)
                .node_id(format!("sub_{}", child_spec_name)),
        );
        builder.build().unwrap()
    }

    fn make_registered_with_sub(name: &str, child_spec_name: &str) -> RegisteredWorkflowSpec {
        RegisteredWorkflowSpec {
            spec: make_spec_with_sub(name, child_spec_name),
            spec_builder: None,
        }
    }

    #[test]
    fn subworkflow_no_cycle_accepted() {
        // A -> B (no cycle, B has no subworkflows)
        let mut registry = WorkflowSpecRegistry::new();
        registry.register(make_registered("wf_b")).unwrap();
        registry
            .register(make_registered_with_sub("wf_a", "wf_b"))
            .unwrap();
        assert_eq!(registry.len(), 2);
    }

    #[test]
    fn subworkflow_direct_cycle_rejected() {
        // A -> B and B -> A creates a direct cycle.
        let mut registry = WorkflowSpecRegistry::new();
        registry
            .register(make_registered_with_sub("wf_a", "wf_b"))
            .unwrap();
        let result = registry.register(make_registered_with_sub("wf_b", "wf_a"));
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.code, Some(ErrorCode::WorkflowCycleDetected));
        let msg = format!("{}", err);
        assert!(msg.contains("cycle"), "expected cycle error, got: {}", msg,);
    }

    #[test]
    fn subworkflow_indirect_cycle_rejected() {
        // A -> B -> C -> A creates an indirect cycle.
        let mut registry = WorkflowSpecRegistry::new();
        registry
            .register(make_registered_with_sub("wf_a", "wf_b"))
            .unwrap();
        registry
            .register(make_registered_with_sub("wf_b", "wf_c"))
            .unwrap();
        let result = registry.register(make_registered_with_sub("wf_c", "wf_a"));
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.code, Some(ErrorCode::WorkflowCycleDetected));
    }

    #[test]
    fn subworkflow_self_cycle_rejected() {
        // A -> A: self-referencing sub-workflow.
        let mut registry = WorkflowSpecRegistry::new();
        let result = registry.register(make_registered_with_sub("wf_a", "wf_a"));
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.code, Some(ErrorCode::WorkflowCycleDetected));
    }

    #[test]
    fn subworkflow_diamond_no_cycle_accepted() {
        // A -> B, A -> C, B -> D, C -> D. No cycle.
        let mut registry = WorkflowSpecRegistry::new();
        registry.register(make_registered("wf_d")).unwrap();
        registry
            .register(make_registered_with_sub("wf_b", "wf_d"))
            .unwrap();
        registry
            .register(make_registered_with_sub("wf_c", "wf_d"))
            .unwrap();
        // wf_a references both wf_b and wf_c
        {
            use crate::core::workflow::sub_workflow::SubWorkflowNode;

            let mut builder = WorkflowSpecBuilder::new("wf_a");
            let root = builder.task(TaskNode::<()>::new("root_task"));
            builder.sub_workflow(
                SubWorkflowNode::new("wf_b")
                    .waits_for(root)
                    .node_id("sub_b"),
            );
            builder.sub_workflow(
                SubWorkflowNode::new("wf_c")
                    .waits_for(root)
                    .node_id("sub_c"),
            );
            let spec = builder.build().unwrap();
            let registered = RegisteredWorkflowSpec {
                spec,
                spec_builder: None,
            };
            registry.register(registered).unwrap();
        }
        assert_eq!(registry.len(), 4);
    }

    #[test]
    fn subworkflow_unregistered_child_no_false_positive() {
        // A -> "unregistered_wf" -- child not yet in registry. No cycle.
        let mut registry = WorkflowSpecRegistry::new();
        registry
            .register(make_registered_with_sub("wf_a", "unregistered_wf"))
            .unwrap();
        assert!(registry.contains("wf_a"));
    }

    // -----------------------------------------------------------------------
    // Unregister tests
    // -----------------------------------------------------------------------

    #[test]
    fn unregister_existing() {
        let mut registry = WorkflowSpecRegistry::new();
        registry.register(make_registered("wf_a")).unwrap();
        assert!(registry.contains("wf_a"));
        let removed = registry.unregister("wf_a");
        assert!(removed.is_some());
        assert!(!registry.contains("wf_a"));
        assert!(registry.is_empty());
    }

    #[test]
    fn unregister_missing_returns_none() {
        let mut registry = WorkflowSpecRegistry::new();
        let removed = registry.unregister("nonexistent");
        assert!(removed.is_none());
    }

    #[test]
    fn unregister_allows_re_register() {
        let mut registry = WorkflowSpecRegistry::new();
        registry.register(make_registered("wf_a")).unwrap();
        registry.unregister("wf_a");
        // Should be able to register again after unregister.
        registry.register(make_registered("wf_a")).unwrap();
        assert!(registry.contains("wf_a"));
    }

    // -----------------------------------------------------------------------
    // Per-node lookup tests
    // -----------------------------------------------------------------------

    #[test]
    fn get_node_returns_node() {
        let mut registry = WorkflowSpecRegistry::new();
        registry.register(make_registered("wf_a")).unwrap();
        let node = registry.get_node("wf_a", 0);
        assert!(node.is_some());
        assert_eq!(node.unwrap().task_name, "my_task");
    }

    #[test]
    fn get_node_out_of_range() {
        let mut registry = WorkflowSpecRegistry::new();
        registry.register(make_registered("wf_a")).unwrap();
        assert!(registry.get_node("wf_a", 99).is_none());
    }

    #[test]
    fn get_node_missing_workflow() {
        let registry = WorkflowSpecRegistry::new();
        assert!(registry.get_node("missing", 0).is_none());
    }

    #[test]
    fn get_task_node_returns_non_subworkflow() {
        let mut registry = WorkflowSpecRegistry::new();
        registry.register(make_registered("wf_a")).unwrap();
        // make_registered creates a simple task (not a subworkflow).
        let node = registry.get_task_node("wf_a", 0);
        assert!(node.is_some());
        assert!(!node.unwrap().is_subworkflow);
    }

    #[test]
    fn get_task_node_rejects_subworkflow() {
        let mut registry = WorkflowSpecRegistry::new();
        registry
            .register(make_registered_with_sub("wf_a", "wf_b"))
            .unwrap();
        // Index 0 is a regular task, index 1 is a subworkflow.
        assert!(registry.get_task_node("wf_a", 0).is_some());
        assert!(registry.get_task_node("wf_a", 1).is_none());
    }

    #[test]
    fn get_subworkflow_node_returns_subworkflow() {
        let mut registry = WorkflowSpecRegistry::new();
        registry
            .register(make_registered_with_sub("wf_a", "wf_b"))
            .unwrap();
        // Index 1 is the subworkflow node.
        let node = registry.get_subworkflow_node("wf_a", 1);
        assert!(node.is_some());
        assert!(node.unwrap().is_subworkflow);
    }

    #[test]
    fn get_subworkflow_node_rejects_regular() {
        let mut registry = WorkflowSpecRegistry::new();
        registry
            .register(make_registered_with_sub("wf_a", "wf_b"))
            .unwrap();
        // Index 0 is a regular task.
        assert!(registry.get_subworkflow_node("wf_a", 0).is_none());
    }

    #[test]
    fn cycle_rejected_does_not_leave_spec_in_registry() {
        // After a cycle rejection, the offending spec should NOT be in the registry.
        let mut registry = WorkflowSpecRegistry::new();
        registry
            .register(make_registered_with_sub("wf_a", "wf_b"))
            .unwrap();
        let result = registry.register(make_registered_with_sub("wf_b", "wf_a"));
        assert!(result.is_err());
        // wf_b should NOT be registered.
        assert!(!registry.contains("wf_b"));
        assert_eq!(registry.len(), 1);
    }
}
