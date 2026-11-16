use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use crate::core::error::{ErrorCode, HorsiesError};
use crate::core::workflow::node::TaskDefaults;
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

/// A registered dynamic workflow definition for runtime-built child workflows.
#[derive(Clone)]
pub struct RegisteredWorkflowDefinition {
    /// Human-readable workflow name used by `SubWorkflowNode`.
    pub name: String,
    /// Stable identity used for persisted/runtime lookup.
    pub definition_key: String,
    /// Runtime builder that decodes parent-provided params and returns a concrete spec.
    pub spec_builder: SpecBuilderFn,
    /// Definition keys of child workflows this definition may reference.
    ///
    /// Used for static cycle detection at registration time. The runtime
    /// ancestor guard in `start.rs` remains as defense-in-depth for cases
    /// where declarations are incomplete or topology varies with params.
    pub declared_children: Vec<String>,
}

impl std::fmt::Debug for RegisteredWorkflowDefinition {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RegisteredWorkflowDefinition")
            .field("name", &self.name)
            .field("definition_key", &self.definition_key)
            .field("declared_children", &self.declared_children)
            .finish()
    }
}

/// A registered workflow spec with an optional dynamic builder.
#[derive(Clone)]
pub struct RegisteredWorkflowSpec {
    /// The validated, immutable workflow specification.
    ///
    /// When `spec_builder` is present, this spec still acts as the
    /// registration-time placeholder used for cycle checks, queue/default
    /// resolution, and other upfront validation. Keep it structurally aligned
    /// with what the runtime builder produces.
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
    dynamic_definitions: HashMap<String, RegisteredWorkflowDefinition>,
    /// Pre-computed map of task_name -> serialized retry options JSON.
    ///
    /// Populated by `Horsies::register_workflow()` from the task registry.
    /// Used by the engine to resolve task options for dynamically built
    /// specs (via `spec_builder`) that bypass `register_workflow()`.
    task_options_map: HashMap<String, String>,
    /// Pre-computed map of task_name -> (queue, priority) defaults.
    ///
    /// Used to resolve workflow node queue/priority from registered task
    /// metadata for dynamic specs that bypass `register_workflow()`.
    task_defaults_map: HashMap<String, TaskDefaults>,
    /// Pre-computed map of queue_name -> priority from app config.
    ///
    /// Used to compute effective priority from the final resolved queue
    /// without needing access to `AppConfig` at start time.
    queue_priority_map: HashMap<String, u32>,
}

impl Clone for WorkflowSpecRegistry {
    fn clone(&self) -> Self {
        Self {
            specs: self.specs.clone(),
            dynamic_definitions: self.dynamic_definitions.clone(),
            task_options_map: self.task_options_map.clone(),
            task_defaults_map: self.task_defaults_map.clone(),
            queue_priority_map: self.queue_priority_map.clone(),
        }
    }
}

/// Identity of a node in the workflow dependency graph.
///
/// Static specs are keyed by name (legacy). Dynamic definitions are keyed
/// by definition_key (stable identity). The enum prevents accidental
/// cross-comparison between the two identity spaces.
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug)]
enum WorkflowVertex<'a> {
    StaticName(&'a str),
    DynamicDefinitionKey(&'a str),
}

impl WorkflowSpecRegistry {
    /// Create an empty registry.
    pub fn new() -> Self {
        Self {
            specs: HashMap::new(),
            dynamic_definitions: HashMap::new(),
            task_options_map: HashMap::new(),
            task_defaults_map: HashMap::new(),
            queue_priority_map: HashMap::new(),
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

        if self.contains_name(&name) {
            return Err(
                HorsiesError::new(format!("duplicate workflow spec name: '{}'", name))
                    .with_code(ErrorCode::WorkflowDuplicateNodeId)
                    .with_help("each workflow spec must have a unique name"),
            );
        }

        if let Some(ref key) = registered.spec.definition_key {
            if self.definition_key_taken(key) {
                return Err(HorsiesError::new(format!(
                    "duplicate workflow definition_key: '{}'",
                    key
                ))
                .with_code(ErrorCode::WorkflowDuplicateNodeId)
                .with_help("each workflow definition_key must be unique"));
            }
        }

        // Check for sub-workflow cycles before inserting.
        // Temporarily insert the new spec, check for cycles, then remove if a
        // cycle is found.
        self.specs.insert(name.clone(), registered);

        if let Some(cycle_path) = self.detect_subworkflow_cycle(WorkflowVertex::StaticName(&name)) {
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

    /// Register a runtime-built workflow definition for child workflow use.
    pub fn register_definition(
        &mut self,
        registered: RegisteredWorkflowDefinition,
    ) -> Result<(), HorsiesError> {
        if registered.name.is_empty() {
            return Err(
                HorsiesError::new("workflow definition name must not be empty")
                    .with_code(ErrorCode::WorkflowNoName),
            );
        }

        if registered.definition_key.is_empty() {
            return Err(
                HorsiesError::new("workflow definition_key must not be empty")
                    .with_code(ErrorCode::WorkflowNoName),
            );
        }

        if self.contains_name(&registered.name) {
            return Err(HorsiesError::new(format!(
                "duplicate workflow spec name: '{}'",
                registered.name
            ))
            .with_code(ErrorCode::WorkflowDuplicateNodeId)
            .with_help("each workflow spec must have a unique name"));
        }

        if self.definition_key_taken(&registered.definition_key) {
            return Err(HorsiesError::new(format!(
                "duplicate workflow definition_key: '{}'",
                registered.definition_key
            ))
            .with_code(ErrorCode::WorkflowDuplicateNodeId)
            .with_help("each workflow definition_key must be unique"));
        }

        let name = registered.name.clone();
        let def_key = registered.definition_key.clone();
        self.dynamic_definitions.insert(name.clone(), registered);

        if let Some(cycle_path) =
            self.detect_subworkflow_cycle(WorkflowVertex::DynamicDefinitionKey(&def_key))
        {
            let removed = self
                .dynamic_definitions
                .remove(&name)
                .expect("definition was just inserted above");
            let cycle_str = cycle_path.join(" -> ");
            return Err(HorsiesError::new("cycle detected in nested workflows")
                .with_code(ErrorCode::WorkflowCycleDetected)
                .with_note(format!(
                    "registering '{}' creates a circular reference: {}",
                    removed.name, cycle_str,
                ))
                .with_note("cycles in nested workflows are not allowed")
                .with_help("remove the circular child workflow reference from declared_children"));
        }

        Ok(())
    }

    /// Resolve a child edge originating from a static spec's sub-workflow node.
    ///
    /// Prefers `sub_definition_key` when present (may resolve to either a
    /// dynamic definition or a static spec). Falls back to the child name.
    fn resolve_child_from_static_edge(
        &self,
        child_name: &str,
        sub_definition_key: Option<&str>,
    ) -> Option<WorkflowVertex<'_>> {
        if let Some(key) = sub_definition_key {
            if let Some(def) = self.get_definition_by_definition_key(key) {
                return Some(WorkflowVertex::DynamicDefinitionKey(&def.definition_key));
            }
            if let Some(spec) = self.get_by_definition_key(key) {
                return Some(WorkflowVertex::StaticName(&spec.spec.name));
            }
        }
        if let Some(def) = self.dynamic_definitions.get(child_name) {
            return Some(WorkflowVertex::DynamicDefinitionKey(&def.definition_key));
        }
        if let Some(spec) = self.specs.get(child_name) {
            return Some(WorkflowVertex::StaticName(&spec.spec.name));
        }
        None
    }

    /// Resolve a child edge originating from a dynamic definition's
    /// `declared_children` (which stores definition keys).
    fn resolve_child_from_definition_key(&self, key: &str) -> Option<WorkflowVertex<'_>> {
        if let Some(def) = self.get_definition_by_definition_key(key) {
            return Some(WorkflowVertex::DynamicDefinitionKey(&def.definition_key));
        }
        if let Some(spec) = self.get_by_definition_key(key) {
            return Some(WorkflowVertex::StaticName(&spec.spec.name));
        }
        None
    }

    /// Human-friendly display name for a vertex (used in error messages).
    fn display_name_for_vertex(&self, vertex: WorkflowVertex<'_>) -> String {
        match vertex {
            WorkflowVertex::StaticName(name) => name.to_owned(),
            WorkflowVertex::DynamicDefinitionKey(key) => {
                if let Some(def) = self.get_definition_by_definition_key(key) {
                    format!("{} [{}]", def.name, key)
                } else {
                    key.to_owned()
                }
            }
        }
    }

    /// Detect sub-workflow cycles reachable from `start`.
    ///
    /// Uses DFS with a recursion stack across both static specs (keyed by
    /// name) and dynamic definitions (keyed by definition_key). Returns
    /// `Some(cycle_path)` with human-readable names if a cycle is found.
    fn detect_subworkflow_cycle(&self, start: WorkflowVertex<'_>) -> Option<Vec<String>> {
        let mut visited: HashSet<WorkflowVertex<'_>> = HashSet::new();
        let mut stack: Vec<WorkflowVertex<'_>> = Vec::new();
        self.dfs_cycle(start, &mut visited, &mut stack)
    }

    fn dfs_cycle<'a>(
        &'a self,
        current: WorkflowVertex<'a>,
        visited: &mut HashSet<WorkflowVertex<'a>>,
        stack: &mut Vec<WorkflowVertex<'a>>,
    ) -> Option<Vec<String>> {
        if let Some(pos) = stack.iter().position(|v| *v == current) {
            let mut cycle_path: Vec<String> = stack[pos..]
                .iter()
                .map(|v| self.display_name_for_vertex(*v))
                .collect();
            cycle_path.push(self.display_name_for_vertex(current));
            return Some(cycle_path);
        }

        if visited.contains(&current) {
            return None;
        }

        visited.insert(current);
        stack.push(current);

        let result = match current {
            WorkflowVertex::StaticName(name) => {
                if let Some(registered) = self.specs.get(name) {
                    for task in &registered.spec.tasks {
                        if task.is_subworkflow {
                            let child_name = task
                                .task_name
                                .strip_prefix("__sub_workflow:")
                                .unwrap_or(&task.task_name);
                            if let Some(child_vertex) = self.resolve_child_from_static_edge(
                                child_name,
                                task.sub_definition_key.as_deref(),
                            ) {
                                if let Some(cycle) = self.dfs_cycle(child_vertex, visited, stack) {
                                    return Some(cycle);
                                }
                            }
                        }
                    }
                }
                None
            }
            WorkflowVertex::DynamicDefinitionKey(key) => {
                if let Some(def) = self.get_definition_by_definition_key(key) {
                    let children: Vec<String> = def.declared_children.clone();
                    for child_key in &children {
                        if let Some(child_vertex) =
                            self.resolve_child_from_definition_key(child_key)
                        {
                            if let Some(cycle) = self.dfs_cycle(child_vertex, visited, stack) {
                                return Some(cycle);
                            }
                        }
                    }
                }
                None
            }
        };

        stack.pop();
        result
    }

    /// Look up a registered workflow spec by name.
    pub fn get(&self, name: &str) -> Option<&RegisteredWorkflowSpec> {
        self.specs.get(name)
    }

    /// Look up a registered dynamic workflow definition by name.
    pub fn get_definition(&self, name: &str) -> Option<&RegisteredWorkflowDefinition> {
        self.dynamic_definitions.get(name)
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

    /// Look up a registered dynamic workflow definition by definition key.
    pub fn get_definition_by_definition_key(
        &self,
        key: &str,
    ) -> Option<&RegisteredWorkflowDefinition> {
        self.dynamic_definitions
            .values()
            .find(|r| r.definition_key == key)
    }

    /// Check if a workflow spec is registered.
    pub fn contains(&self, name: &str) -> bool {
        self.specs.contains_key(name)
    }

    /// Check if a dynamic workflow definition is registered.
    pub fn contains_definition(&self, name: &str) -> bool {
        self.dynamic_definitions.contains_key(name)
    }

    /// Number of registered workflow specs.
    pub fn len(&self) -> usize {
        self.specs.len() + self.dynamic_definitions.len()
    }

    /// Whether the registry is empty.
    pub fn is_empty(&self) -> bool {
        self.specs.is_empty() && self.dynamic_definitions.is_empty()
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

    /// Remove a dynamic workflow definition from the registry by name.
    pub fn unregister_definition(&mut self, name: &str) -> Option<RegisteredWorkflowDefinition> {
        self.dynamic_definitions.remove(name)
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

    /// Iterate over registered dynamic workflow definitions.
    pub fn iter_definitions(&self) -> impl Iterator<Item = &RegisteredWorkflowDefinition> {
        self.dynamic_definitions.values()
    }

    fn contains_name(&self, name: &str) -> bool {
        self.specs.contains_key(name) || self.dynamic_definitions.contains_key(name)
    }

    fn definition_key_taken(&self, key: &str) -> bool {
        self.specs
            .values()
            .any(|r| r.spec.definition_key.as_deref() == Some(key))
            || self
                .dynamic_definitions
                .values()
                .any(|r| r.definition_key == key)
    }

    pub(crate) fn resolve_child_registration<'a>(
        &'a self,
        name: &str,
        definition_key: Option<&str>,
    ) -> Option<ResolvedChildWorkflow<'a>> {
        if let Some(key) = definition_key {
            if let Some(registered) = self.get_definition_by_definition_key(key) {
                return Some(ResolvedChildWorkflow::Dynamic(registered));
            }
            if let Some(registered) = self.get_by_definition_key(key) {
                return Some(ResolvedChildWorkflow::Static(registered));
            }
        }

        if let Some(registered) = self.get_definition(name) {
            return Some(ResolvedChildWorkflow::Dynamic(registered));
        }
        self.get(name).map(ResolvedChildWorkflow::Static)
    }

    /// Iterate over registered workflow spec names.
    pub fn spec_names(&self) -> impl Iterator<Item = &str> {
        self.specs
            .keys()
            .chain(self.dynamic_definitions.keys())
            .map(String::as_str)
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
        crate::core::workflow::node::resolve_node_task_options(&mut spec.tasks, &|task_name| {
            self.task_options_map.get(task_name).cloned()
        });
    }

    /// Replace the task defaults map used for resolving queue/priority on
    /// dynamically built workflow specs.
    pub fn set_task_defaults_map(&mut self, map: HashMap<String, TaskDefaults>) {
        self.task_defaults_map = map;
    }

    /// Replace the queue priority map used for computing effective priority
    /// from the final resolved queue name.
    pub fn set_queue_priority_map(&mut self, map: HashMap<String, u32>) {
        self.queue_priority_map = map;
    }

    /// Compute effective priority for a queue name using the stored config map.
    ///
    /// Falls back to 100 (default priority) for unknown queues.
    pub fn effective_priority(&self, queue_name: &str) -> u32 {
        self.queue_priority_map
            .get(queue_name)
            .copied()
            .unwrap_or(100)
    }

    /// Returns true if the given queue name is known in the stored config map.
    pub fn is_valid_queue(&self, queue_name: &str) -> bool {
        self.queue_priority_map.contains_key(queue_name)
    }

    /// Resolve and validate a workflow spec before start.
    ///
    /// This is the shared validation entry point used by:
    /// - `WorkflowStarter::start` / `start_with_id` (top-level dynamic starts)
    /// - `WorkflowFunction::bound_spec` (registered workflow starts)
    /// - `build_child_spec` in engine/lifecycle/start (child spec_builder paths)
    /// - `check()` registered workflow and builder validation phases
    ///
    /// Performs:
    /// 1. Queue/priority resolution from task defaults
    /// 2. Queue validation (unresolved and invalid)
    /// 3. Missing-input validation (task expects input but node has none)
    pub fn resolve_and_validate_spec(
        &self,
        spec: &mut crate::core::workflow::spec::WorkflowSpec,
    ) -> Result<(), HorsiesError> {
        crate::core::workflow::node::resolve_node_queue_priority(
            &mut spec.tasks,
            &|task_name| self.task_defaults_map.get(task_name).cloned(),
            &|queue_name| self.effective_priority(queue_name),
        );

        for node in &spec.tasks {
            if node.is_subworkflow {
                let spec_name = node
                    .task_name
                    .strip_prefix("__sub_workflow:")
                    .unwrap_or(&node.task_name);
                let has_input = node.args_json.is_some()
                    || node.kwargs_json.is_some()
                    || !node.args_from.is_empty();

                let resolved =
                    self.resolve_child_registration(spec_name, node.sub_definition_key.as_deref());

                match resolved {
                    None => {
                        return Err(HorsiesError::new(format!(
                            "workflow '{}' node '{}' references unregistered child workflow '{}'",
                            spec.name,
                            node.node_id.as_deref().unwrap_or("?"),
                            spec_name,
                        ))
                        .with_code(ErrorCode::WorkflowMissingRequiredParams)
                        .with_help(
                            "register the child workflow before starting this parent workflow",
                        ));
                    }
                    Some(ResolvedChildWorkflow::Static(registered))
                        if has_input && registered.spec_builder.is_none() =>
                    {
                        return Err(HorsiesError::new(format!(
                            "workflow '{}' node '{}' supplies params to child workflow '{}' \
                             but that child has no runtime spec_builder",
                            spec.name,
                            node.node_id.as_deref().unwrap_or("?"),
                            spec_name,
                        ))
                        .with_code(ErrorCode::WorkflowMissingRequiredParams)
                        .with_help(
                            "register the child as a parameterized workflow definition, \
                             or remove .set_input(...) / .set(...) / .arg_from(...) from the sub-workflow node",
                        ));
                    }
                    _ => {}
                }
                continue;
            }

            // Validate queue: resolved and valid.
            match &node.queue {
                None => {
                    return Err(HorsiesError::new(format!(
                        "workflow '{}' node '{}' (task '{}') has no resolved queue",
                        spec.name,
                        node.node_id.as_deref().unwrap_or("?"),
                        node.task_name,
                    ))
                    .with_code(ErrorCode::WorkflowUnresolvedQueue)
                    .with_help(
                        "use TaskFunction::node() or set .queue() explicitly on the TaskNode",
                    ));
                }
                Some(queue)
                    if !self.queue_priority_map.is_empty() && !self.is_valid_queue(queue) =>
                {
                    return Err(HorsiesError::new(format!(
                        "workflow '{}' node '{}' (task '{}') has invalid queue '{}'",
                        spec.name,
                        node.node_id.as_deref().unwrap_or("?"),
                        node.task_name,
                        queue,
                    ))
                    .with_code(ErrorCode::WorkflowUnresolvedQueue)
                    .with_help(
                        "set a valid queue name from configured custom_queues, \
                         or use TaskFunction::node() to inherit the task's registered queue",
                    ));
                }
                _ => {}
            }

            // Validate input: task expects input but node has none.
            if let Some(defaults) = self.task_defaults_map.get(&node.task_name) {
                if defaults.expects_input {
                    let has_input = node.args_json.is_some()
                        || node.kwargs_json.is_some()
                        || !node.args_from.is_empty();
                    if !has_input {
                        let type_name = defaults.input_type_name.unwrap_or("non-unit type");
                        return Err(HorsiesError::new(format!(
                            "workflow '{}' node '{}' (task '{}') requires input `{}` \
                             but has no args, kwargs, or args_from",
                            spec.name,
                            node.node_id.as_deref().unwrap_or("?"),
                            node.task_name,
                            type_name,
                        ))
                        .with_code(ErrorCode::WorkflowMissingRequiredParams)
                        .with_help(
                            "use task_name::node()?.set_input(input) for whole inputs, \
                             or use typed .set(...) / .arg_from(...) on the node",
                        ));
                    }
                }
            }
        }

        Ok(())
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
            .field("dynamic_definition_count", &self.dynamic_definitions.len())
            .field("spec_names", &self.specs.keys().collect::<Vec<_>>())
            .finish()
    }
}

pub(crate) enum ResolvedChildWorkflow<'a> {
    Static(&'a RegisteredWorkflowSpec),
    Dynamic(&'a RegisteredWorkflowDefinition),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::workflow::node::TaskNode;
    use crate::core::workflow::spec::WorkflowSpecBuilder;
    use crate::core::workflow::sub_workflow::SubWorkflowNode;
    use crate::WorkflowInput;

    fn make_spec(name: &str) -> WorkflowSpec {
        let mut builder = WorkflowSpecBuilder::new(name);
        builder.task(TaskNode::<()>::raw("my_task"));
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
        let root = builder.task(TaskNode::<()>::raw("root_task"));
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

    #[derive(serde::Serialize, WorkflowInput)]
    struct ChildParams {
        region: String,
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
            let root = builder.task(TaskNode::<()>::raw("root_task"));
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

    #[test]
    fn subworkflow_explicit_input_requires_spec_builder() {
        let mut registry = WorkflowSpecRegistry::new();
        registry.register(make_registered("child")).unwrap();

        let mut builder = WorkflowSpecBuilder::new("parent");
        builder.sub_workflow(
            SubWorkflowNode::<ChildParams, ()>::typed("child")
                .set_input(ChildParams {
                    region: "eu".to_owned(),
                })
                .unwrap()
                .node_id("child_node"),
        );
        let mut spec = builder.build().unwrap();

        let err = registry.resolve_and_validate_spec(&mut spec).unwrap_err();
        assert!(err
            .to_string()
            .contains("supplies params to child workflow 'child'"));
        assert!(err.to_string().contains("has no runtime spec_builder"));
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

    // -----------------------------------------------------------------------
    // Dynamic definition cycle detection tests
    // -----------------------------------------------------------------------

    fn make_dynamic_def(
        name: &str,
        def_key: &str,
        declared_children: &[&str],
    ) -> RegisteredWorkflowDefinition {
        RegisteredWorkflowDefinition {
            name: name.to_owned(),
            definition_key: def_key.to_owned(),
            spec_builder: Arc::new(|_, _| {
                let mut b = WorkflowSpecBuilder::new("dummy");
                b.task(TaskNode::<()>::raw("dummy_task"));
                b.build()
            }),
            declared_children: declared_children.iter().map(|s| s.to_string()).collect(),
        }
    }

    /// Helper: create a static spec whose sub-workflow node carries a
    /// `sub_definition_key` (the way `WorkflowTemplate::node()` works).
    fn make_spec_with_sub_definition_key(
        name: &str,
        child_name: &str,
        child_def_key: &str,
    ) -> WorkflowSpec {
        let mut builder = WorkflowSpecBuilder::new(name);
        let root = builder.task(TaskNode::<()>::raw("root_task"));
        builder.sub_workflow(
            SubWorkflowNode::new(child_name)
                .definition_key(child_def_key)
                .waits_for(root)
                .node_id(format!("sub_{}", child_name)),
        );
        builder.build().unwrap()
    }

    #[test]
    fn dynamic_self_cycle_rejected() {
        // A declares itself as a child.
        let mut registry = WorkflowSpecRegistry::new();
        let result = registry.register_definition(make_dynamic_def(
            "dyn_a",
            "test.dyn_a.v1",
            &["test.dyn_a.v1"],
        ));
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.code, Some(ErrorCode::WorkflowCycleDetected));
    }

    #[test]
    fn dynamic_direct_cycle_rejected() {
        // A declares child B, B declares child A.
        let mut registry = WorkflowSpecRegistry::new();
        registry
            .register_definition(make_dynamic_def(
                "dyn_a",
                "test.dyn_a.v1",
                &["test.dyn_b.v1"],
            ))
            .unwrap();
        let result = registry.register_definition(make_dynamic_def(
            "dyn_b",
            "test.dyn_b.v1",
            &["test.dyn_a.v1"],
        ));
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.code, Some(ErrorCode::WorkflowCycleDetected));
    }

    #[test]
    fn dynamic_no_cycle_accepted() {
        // A declares child B, B has no children. No cycle.
        let mut registry = WorkflowSpecRegistry::new();
        registry
            .register_definition(make_dynamic_def("dyn_b", "test.dyn_b.v1", &[]))
            .unwrap();
        registry
            .register_definition(make_dynamic_def(
                "dyn_a",
                "test.dyn_a.v1",
                &["test.dyn_b.v1"],
            ))
            .unwrap();
        assert_eq!(registry.len(), 2);
    }

    #[test]
    fn dynamic_empty_declared_children_accepted() {
        // Empty declared_children is the default. No cycle check issues.
        let mut registry = WorkflowSpecRegistry::new();
        registry
            .register_definition(make_dynamic_def("dyn_a", "test.dyn_a.v1", &[]))
            .unwrap();
        assert_eq!(registry.len(), 1);
    }

    #[test]
    fn static_to_dynamic_to_static_cycle_rejected() {
        // Static A -> Dynamic B (via sub_definition_key) -> Static A (via declared_children).
        let mut registry = WorkflowSpecRegistry::new();

        // Register static A referencing dynamic B via definition key.
        let spec_a = make_spec_with_sub_definition_key("wf_a", "dyn_b", "test.dyn_b.v1");
        registry
            .register(RegisteredWorkflowSpec {
                spec: spec_a,
                spec_builder: None,
            })
            .unwrap();

        // Static A has definition_key so dynamic B can reference it.
        // But wait -- static A doesn't have a definition_key set in make_spec_with_sub_definition_key.
        // We need A to have a definition_key for B to declare it as a child.
        // Let's use a different approach: B declares A by A's definition_key.

        // Actually, let's give A a definition_key.
        let mut registry = WorkflowSpecRegistry::new();
        let mut spec_a = make_spec_with_sub_definition_key("wf_a", "dyn_b", "test.dyn_b.v1");
        spec_a.definition_key = Some("test.wf_a.v1".to_owned());
        registry
            .register(RegisteredWorkflowSpec {
                spec: spec_a,
                spec_builder: None,
            })
            .unwrap();

        // Register dynamic B that declares static A as child.
        let result = registry.register_definition(make_dynamic_def(
            "dyn_b",
            "test.dyn_b.v1",
            &["test.wf_a.v1"],
        ));
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.code, Some(ErrorCode::WorkflowCycleDetected));
    }

    #[test]
    fn dynamic_to_static_to_dynamic_cycle_rejected() {
        // Dynamic A declares child Static B, Static B refs Dynamic A (via sub_definition_key).
        let mut registry = WorkflowSpecRegistry::new();

        // Register dynamic A that declares static B as child.
        registry
            .register_definition(make_dynamic_def(
                "dyn_a",
                "test.dyn_a.v1",
                &["test.wf_b.v1"],
            ))
            .unwrap();

        // Register static B that references dynamic A via sub_definition_key.
        let mut spec_b = make_spec_with_sub_definition_key("wf_b", "dyn_a", "test.dyn_a.v1");
        spec_b.definition_key = Some("test.wf_b.v1".to_owned());
        let result = registry.register(RegisteredWorkflowSpec {
            spec: spec_b,
            spec_builder: None,
        });
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.code, Some(ErrorCode::WorkflowCycleDetected));
    }

    #[test]
    fn dynamic_diamond_no_false_positive() {
        // A declares [B, C], B declares [D], C declares [D]. No cycle.
        let mut registry = WorkflowSpecRegistry::new();
        registry
            .register_definition(make_dynamic_def("dyn_d", "test.dyn_d.v1", &[]))
            .unwrap();
        registry
            .register_definition(make_dynamic_def(
                "dyn_b",
                "test.dyn_b.v1",
                &["test.dyn_d.v1"],
            ))
            .unwrap();
        registry
            .register_definition(make_dynamic_def(
                "dyn_c",
                "test.dyn_c.v1",
                &["test.dyn_d.v1"],
            ))
            .unwrap();
        registry
            .register_definition(make_dynamic_def(
                "dyn_a",
                "test.dyn_a.v1",
                &["test.dyn_b.v1", "test.dyn_c.v1"],
            ))
            .unwrap();
        assert_eq!(registry.len(), 4);
    }

    #[test]
    fn dynamic_cycle_rejected_does_not_leave_definition_in_registry() {
        let mut registry = WorkflowSpecRegistry::new();
        registry
            .register_definition(make_dynamic_def(
                "dyn_a",
                "test.dyn_a.v1",
                &["test.dyn_b.v1"],
            ))
            .unwrap();
        let result = registry.register_definition(make_dynamic_def(
            "dyn_b",
            "test.dyn_b.v1",
            &["test.dyn_a.v1"],
        ));
        assert!(result.is_err());
        assert!(!registry.contains_definition("dyn_b"));
        assert_eq!(registry.len(), 1);
    }
}
