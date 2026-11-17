use std::any::TypeId;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::core::config::AppConfig;
use crate::core::config::QueueMode;
use crate::core::error::{ErrorCode, HorsiesError};
use crate::core::registry::task::TaskRegistry;
use crate::core::registry::workflow::{
    BuilderHookName, RegisteredWorkflowDefinition, RegisteredWorkflowSpec, SpecBuildError,
    WorkflowSpecRegistry,
};
use crate::core::workflow::spec::{WorkflowSpec, WorkflowSpecBuilder};

/// Pre-resolved parameters for enqueueing a task.
///
/// Produced by [`Horsies::resolve_enqueue()`], consumed by `broker.enqueue()`.
/// This keeps horsies-core IO-free while still providing the full resolution
/// logic (queue validation, priority resolution) that Python's `.send()` does.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ResolvedEnqueue {
    /// The registered task name.
    pub task_name: String,
    /// The resolved queue name (validated against the current queue mode).
    pub queue_name: String,
    /// The resolved priority (from task, queue config, or default).
    pub priority: u32,
    /// The queue-mapped retention of the send.
    pub retention: crate::core::config::EnqueueRetention,
}

/// What every fired slot of one schedule stores besides its input, resolved
/// once at scheduler start from the task registry (Python
/// `core/scheduler/validation.py` `prepare_schedules`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScheduleSendFacts {
    /// The schedule's queue, else the queue the task declares, else
    /// `default`; validated against the queue mode.
    pub queue_name: String,
    /// The task's stored options text, as a send of the task stores it:
    /// timeout, retry policy and retryable codes. `None` when the task
    /// declares none.
    pub task_options: Option<String>,
}

/// The send facts of each enabled schedule, by schedule name.
pub type PreparedSchedules = std::collections::HashMap<String, ScheduleSendFacts>;

/// The queue a schedule's slots go to: the schedule's own queue, else the
/// queue its task declares, else `default` (Python
/// `Scheduler._resolve_schedule_queue`).
fn schedule_queue<'a>(
    schedule: &'a crate::core::config::TaskSchedule,
    task: Option<&'a crate::core::task::fn_trait::RegisteredTask>,
) -> &'a str {
    match (
        schedule.queue_name.as_deref(),
        task.and_then(|task| task.queue_name()),
    ) {
        (Some(queue), _) => queue,
        (None, Some(task_queue)) => task_queue,
        (None, None) => "default",
    }
}

/// Main horsies application.
///
/// Holds configuration, the task registry, and the workflow spec registry.
/// Entry point for task/workflow registration.
///
/// # Example
///
/// ```ignore
/// use horsies::{Horsies, AppConfig, async_task_fn};
///
/// let config = AppConfig { /* ... */ };
/// let mut app = Horsies::new(config)?;
/// app.register("send_email", async_task_fn!(send_email, EmailArgs))?;
/// ```
pub struct Horsies {
    config: AppConfig,
    registry: TaskRegistry,
    workflow_registry: WorkflowSpecRegistry,
    workflow_builders: Vec<Box<dyn WorkflowBuilderCheck>>,
    /// Current role: "producer", "worker", or "scheduler".
    role: String,
    /// When true, task sends are suppressed (used during import/discovery).
    suppress_sends: Arc<AtomicBool>,
}

/// The environment variable that suppresses sends in this process when it
/// is `1` (Python `HORSIES_SUPPRESS_SENDS`).
pub const SUPPRESS_SENDS_ENV: &str = "HORSIES_SUPPRESS_SENDS";

/// Whether a value of [`SUPPRESS_SENDS_ENV`] asks for suppression: `1`,
/// with surrounding whitespace ignored. Unset and any other value do not.
fn suppression_requested(value: Option<&str>) -> bool {
    value.map(str::trim) == Some("1")
}

impl Horsies {
    /// Create a new Horsies instance with the given configuration.
    ///
    /// Validates the configuration on construction. Returns all validation
    /// errors if any are found.
    pub fn new(config: AppConfig) -> Result<Self, HorsiesError> {
        let errors = config.validate();
        if !errors.is_empty() {
            let mut report = crate::core::error::ValidationReport::new("config");
            for error in errors {
                report.add(error.to_horsies_error());
            }
            return report.into_result().map(|_| unreachable!());
        }

        Ok(Self {
            config,
            registry: TaskRegistry::new(),
            workflow_registry: WorkflowSpecRegistry::new(BuilderHookName::SPEC_BUILDER),
            workflow_builders: Vec::new(),
            role: "producer".to_owned(),
            suppress_sends: Arc::new(AtomicBool::new(false)),
        })
    }

    /// Register a task by name.
    ///
    /// The task can be created with [`async_task_fn!`] or [`blocking_task_fn!`].
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The name is empty
    /// - A task with the same name is already registered
    /// - The task declares a queue that is not valid for the current queue config
    pub fn register(
        &mut self,
        name: impl Into<String>,
        mut task: crate::core::task::fn_trait::RegisteredTask,
    ) -> Result<(), HorsiesError> {
        if let Some(opts) = task.task_options() {
            if opts.good_until.is_some() {
                return Err(HorsiesError::new(
                    "definition-time good_until is not supported; use send-time \
                     TaskSendOptions for ad-hoc sends or workflow-node good_until",
                )
                .with_code(ErrorCode::TaskInvalidOptions));
            }

            match (&self.config.queue_mode, &opts.queue_name) {
                // DEFAULT mode: tasks must not declare a queue_name.
                (QueueMode::Default, Some(queue_name)) if queue_name != "default" => {
                    return Err(HorsiesError::new(format!(
                        "task '{}' declares queue '{}' which is not valid \
                         for the current queue config",
                        opts.task_name, queue_name,
                    ))
                    .with_code(ErrorCode::TaskInvalidOptions)
                    .with_help(
                        "remove queue_name from task_options or switch to Custom queue mode",
                    ));
                }
                // CUSTOM mode: tasks must declare a queue_name.
                (QueueMode::Custom, None) => {
                    return Err(HorsiesError::new(format!(
                        "task '{}' omits queue_name but app uses Custom queue mode; \
                         valid queues: {:?}",
                        opts.task_name,
                        self.get_valid_queue_names(),
                    ))
                    .with_code(ErrorCode::TaskInvalidOptions)
                    .with_help("specify queue_name in task_options"));
                }
                // CUSTOM mode + queue_name: validate it exists.
                (QueueMode::Custom, Some(queue_name)) => {
                    self.validate_queue(queue_name).map_err(|err| {
                        HorsiesError::new(format!(
                            "task '{}' declares queue '{}' which is not in \
                             configured custom_queues; valid queues: {:?}",
                            opts.task_name,
                            queue_name,
                            self.get_valid_queue_names(),
                        ))
                        .with_code(ErrorCode::TaskInvalidOptions)
                        .with_note(err.to_string())
                    })?;
                }
                // DEFAULT + None, DEFAULT + Some("default"): OK.
                _ => {}
            }
        }

        // Populate resolved queue/priority metadata on TaskMeta.
        match &self.config.queue_mode {
            QueueMode::Default => {
                task.set_queue_name("default".to_owned());
                task.set_priority(100);
            }
            QueueMode::Custom => {
                // In Custom mode, extract from TaskOptions if available.
                // Tasks registered via higher-level builders always have this;
                // low-level register() without TaskOptions leaves None, which
                // workflow resolution and check() will catch.
                let queue_from_opts = task.task_options().and_then(|opts| opts.queue_name.clone());
                if let Some(queue_name) = queue_from_opts {
                    let priority = self.effective_priority(&queue_name, None);
                    task.set_queue_name(queue_name);
                    task.set_priority(priority);
                }
            }
        }

        self.registry.register(name, task)?;
        self.refresh_workflow_metadata_maps();
        Ok(())
    }

    /// Register a workflow spec with its conditions.
    ///
    /// Automatically resolves task retry options from the task registry into
    /// workflow nodes that don't already have `task_options_json` set. This
    /// bridges the gap between task-level `TaskOptions` (declared via `#[task]`)
    /// and workflow node options, matching Python's behavior where
    /// `task.fn.task_options_json` is read at workflow start time.
    ///
    /// # Errors
    ///
    /// Returns an error if a workflow with the same name is already registered.
    pub fn register_workflow(
        &mut self,
        mut registered: RegisteredWorkflowSpec,
    ) -> Result<(), HorsiesError> {
        // Resolve task retry options from the task registry into workflow nodes.
        crate::core::workflow::node::resolve_node_task_options(
            &mut registered.spec.tasks,
            &|task_name| {
                self.registry
                    .get(task_name)
                    .ok()
                    .and_then(|t| t.task_options())
                    .map(|opts| opts.stored_options_text())
            },
        );

        // Resolve queue/priority from registered task defaults into workflow nodes.
        crate::core::workflow::node::resolve_node_queue_priority(
            &mut registered.spec.tasks,
            &|task_name| {
                let t = self.registry.get(task_name).ok()?;
                let queue = t.queue_name()?.to_owned();
                let priority = t.priority()?;
                Some(crate::core::workflow::node::TaskDefaults {
                    queue_name: queue,
                    priority,
                })
            },
            &|queue_name| self.effective_priority(queue_name, None),
        );

        // Validate resolved queue names against app config.
        for node in &registered.spec.tasks {
            if node.is_subworkflow {
                continue;
            }
            if let Some(ref queue) = node.queue {
                self.validate_queue(queue).map_err(|_| {
                    HorsiesError::new(format!(
                        "workflow '{}' node '{}' (task '{}') has invalid queue '{}'; valid: {:?}",
                        registered.spec.name,
                        node.node_id.as_deref().unwrap_or("?"),
                        node.task_name,
                        queue,
                        self.get_valid_queue_names(),
                    ))
                    .with_code(ErrorCode::WorkflowUnresolvedQueue)
                    .with_help(
                        "set a valid queue name from configured custom_queues, \
                         or use TaskFunction::node() to inherit the task's registered queue",
                    )
                })?;
            }
        }

        // Refresh all metadata maps so dynamic start paths see current state.
        self.refresh_workflow_metadata_maps();

        self.workflow_registry.register(registered)
    }

    /// Register a workflow spec directly, without conditions.
    ///
    /// This is a convenience wrapper that creates a `RegisteredWorkflowSpec`
    /// with an empty conditions map. Use this when no nodes in the workflow
    /// have `run_when` or `skip_when` conditions.
    ///
    /// For workflows that need custom conditions, either:
    /// - Use `register_workflow()` with a manually constructed
    ///   `RegisteredWorkflowSpec`, or
    /// - Build via `WorkflowSpecBuilder::build_registered()` which preserves
    ///   conditions extracted from nodes.
    ///
    /// # Errors
    ///
    /// Returns an error if a workflow with the same name is already registered.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use horsies::{Horsies, WorkflowSpecBuilder};
    ///
    /// let mut app = Horsies::new(config)?;
    /// let mut builder = WorkflowSpecBuilder::new("my_pipeline");
    /// builder.task(step_a::node()?);
    /// let spec = builder.build()?;
    /// app.register_workflow_spec(spec)?;
    /// ```
    pub fn register_workflow_spec(&mut self, spec: WorkflowSpec) -> Result<(), HorsiesError> {
        let registered = RegisteredWorkflowSpec {
            spec,
            spec_builder: None,
        };
        self.register_workflow(registered)
    }

    /// Register a parameterized workflow definition for child workflow resolution.
    ///
    /// Unlike [`Horsies::register_workflow`], this does not require a representative
    /// placeholder spec. The provided builder is invoked at child-workflow start
    /// time after parent-provided params are decoded into `P`.
    pub fn register_parameterized_workflow<P, F>(
        &mut self,
        name: &str,
        definition_key: &str,
        builder: F,
    ) -> Result<(), HorsiesError>
    where
        P: DeserializeOwned + Serialize + Send + Sync + 'static,
        F: Fn(P) -> Result<WorkflowSpec, HorsiesError> + Send + Sync + 'static,
    {
        let name_owned = name.to_owned();
        let wrapped = Arc::new(move |input_json: &str| {
            let params = decode_parameterized_workflow_input::<P>(&name_owned, input_json)?;
            builder(params).map_err(SpecBuildError::Failed)
        });

        self.workflow_registry
            .register_definition(RegisteredWorkflowDefinition {
                name: name.to_owned(),
                definition_key: definition_key.to_owned(),
                spec_builder: wrapped,
                declared_children: vec![],
            })?;
        self.refresh_workflow_metadata_maps();
        Ok(())
    }

    /// Register a parameterized workflow definition with declared child
    /// workflow edges for static cycle detection.
    ///
    /// Same as [`register_parameterized_workflow`](Self::register_parameterized_workflow)
    /// but also declares which child workflows the builder may reference
    /// (by definition key). This enables preflight cycle detection at
    /// registration time.
    pub fn register_parameterized_workflow_with_children<P, F>(
        &mut self,
        name: &str,
        definition_key: &str,
        declared_children: &[&str],
        builder: F,
    ) -> Result<(), HorsiesError>
    where
        P: DeserializeOwned + Serialize + Send + Sync + 'static,
        F: Fn(P) -> Result<WorkflowSpec, HorsiesError> + Send + Sync + 'static,
    {
        let name_owned = name.to_owned();
        let wrapped = Arc::new(move |input_json: &str| {
            let params = decode_parameterized_workflow_input::<P>(&name_owned, input_json)?;
            builder(params).map_err(SpecBuildError::Failed)
        });

        self.workflow_registry
            .register_definition(RegisteredWorkflowDefinition {
                name: name.to_owned(),
                definition_key: definition_key.to_owned(),
                spec_builder: wrapped,
                declared_children: declared_children.iter().map(|s| s.to_string()).collect(),
            })?;
        self.refresh_workflow_metadata_maps();
        Ok(())
    }

    /// Start building a workflow and register it in one step.
    ///
    /// Returns a `WorkflowRegistrationBuilder` that wraps a
    /// `WorkflowSpecBuilder` and holds a mutable reference to this `Horsies`
    /// instance. Call `.task()`, `.on_error()`, etc. on the returned builder,
    /// then finalize with `.build_and_register()`.
    ///
    /// # Example
    ///
    /// ```ignore
    /// use horsies::Horsies;
    ///
    /// let mut app = Horsies::new(config)?;
    /// let mut wb = app.workflow("my_pipeline");
    /// wb.task(step_a::node()?);
    /// wb.build_and_register()?;
    /// ```
    pub fn workflow(&mut self, name: &str) -> WorkflowRegistrationBuilder<'_> {
        WorkflowRegistrationBuilder {
            app: self,
            builder: WorkflowSpecBuilder::new(name),
        }
    }

    /// Start registering a typed workflow builder for `check()` validation.
    ///
    /// The builder closure receives the current app reference plus a typed
    /// parameter payload. Use `.case(...)` / `.cases(...)` and `.register()`
    /// on the returned builder to finalize registration.
    ///
    /// Parameterized builders must provide at least one case for `check()`;
    /// zero-arg builders use [`Horsies::workflow_builder0()`] for the
    /// ergonomic no-case form.
    pub fn workflow_builder<P, F>(
        &mut self,
        name: &str,
        builder: F,
    ) -> Result<WorkflowBuilderRegistration<'_, P>, HorsiesError>
    where
        P: Send + Sync + 'static,
        F: Fn(&Horsies, &P) -> Result<WorkflowSpec, HorsiesError> + Send + Sync + 'static,
    {
        if name.trim().is_empty() {
            return Err(HorsiesError::new("workflow builder name cannot be empty")
                .with_code(ErrorCode::WorkflowNoName));
        }

        Ok(WorkflowBuilderRegistration::new(
            self,
            name.to_owned(),
            Arc::new(builder),
        ))
    }

    /// Start registering a zero-arg workflow builder for `check()` validation.
    ///
    /// Zero-arg builders are auto-invoked once during `check()`.
    pub fn workflow_builder0<F>(
        &mut self,
        name: &str,
        builder: F,
    ) -> Result<WorkflowBuilderRegistration<'_, ()>, HorsiesError>
    where
        F: Fn(&Horsies) -> Result<WorkflowSpec, HorsiesError> + Send + Sync + 'static,
    {
        self.workflow_builder(name, move |app: &Horsies, _: &()| builder(app))
    }

    /// Get the application configuration.
    pub fn config(&self) -> &AppConfig {
        &self.config
    }

    /// Set the role and log it. Called by CLI after discovery.
    ///
    /// Mirrors Python's `Horsies.set_role()`.
    pub fn set_role(&mut self, role: impl Into<String>) {
        self.role = role.into();
    }

    /// Get the current role.
    pub fn role(&self) -> &str {
        &self.role
    }

    /// Enable or disable send suppression.
    ///
    /// When suppressed, task sends should be skipped (used during
    /// worker/scheduler import phase and `check()` validation).
    /// Mirrors Python's `Horsies.suppress_sends()`.
    pub fn suppress_sends(&self, value: bool) {
        self.suppress_sends.store(value, Ordering::Relaxed);
    }

    /// Whether sends are currently suppressed: [`Self::suppress_sends`] was
    /// called with `true` (import phase, `check()`, builder execution), or the
    /// environment variable [`SUPPRESS_SENDS_ENV`] is `1` (surrounding
    /// whitespace ignored). The variable is read at each call, so a change
    /// takes effect at the next send. Process-local.
    ///
    /// Mirrors Python's `Horsies.are_sends_suppressed()`
    /// (`python/horsies/core/app.py:2269-2280`).
    pub fn are_sends_suppressed(&self) -> bool {
        self.suppress_sends.load(Ordering::Relaxed)
            || suppression_requested(std::env::var(SUPPRESS_SENDS_ENV).ok().as_deref())
    }

    /// Shared suppression flag used by producer-side task facades.
    pub fn suppress_sends_handle(&self) -> Arc<AtomicBool> {
        Arc::clone(&self.suppress_sends)
    }

    /// Get a reference to the task registry.
    pub fn registry(&self) -> &TaskRegistry {
        &self.registry
    }

    /// Get a reference to the workflow spec registry.
    pub fn workflow_registry(&self) -> &WorkflowSpecRegistry {
        &self.workflow_registry
    }

    /// Take ownership of the workflow spec registry.
    ///
    /// Used when constructing the worker, which needs to own the registry.
    pub fn into_workflow_registry(self) -> WorkflowSpecRegistry {
        self.workflow_registry
    }

    /// Consume this Horsies instance and return its constituent parts.
    ///
    /// Useful when constructing a worker that needs config, task registry,
    /// and workflow registry separately.
    pub fn into_parts(self) -> (AppConfig, TaskRegistry, WorkflowSpecRegistry) {
        (self.config, self.registry, self.workflow_registry)
    }

    /// Build a map of task_name -> serialized retry options JSON from the
    /// task registry. Used to populate `WorkflowSpecRegistry::task_options_map`.
    fn build_task_options_map(&self) -> std::collections::HashMap<String, String> {
        let mut map = std::collections::HashMap::new();
        for (name, task) in self.registry.iter() {
            if let Some(opts) = task.task_options() {
                map.insert(name.to_owned(), opts.stored_options_text());
            }
        }
        map
    }

    /// Build a map of task_name -> (queue, priority) defaults from the task
    /// registry. Used to populate `WorkflowSpecRegistry::task_defaults_map`.
    fn build_task_defaults_map(
        &self,
    ) -> std::collections::HashMap<String, crate::core::workflow::node::TaskDefaults> {
        let mut map = std::collections::HashMap::new();
        for (name, task) in self.registry.iter() {
            if let (Some(queue), Some(priority)) = (task.queue_name(), task.priority()) {
                map.insert(
                    name.to_owned(),
                    crate::core::workflow::node::TaskDefaults {
                        queue_name: queue.to_owned(),
                        priority,
                    },
                );
            }
        }
        map
    }

    /// Build a map of queue_name -> priority from the app config.
    /// Used to populate `WorkflowSpecRegistry::queue_priority_map`.
    fn build_queue_priority_map(&self) -> std::collections::HashMap<String, u32> {
        let mut map = std::collections::HashMap::new();
        match self.config.queue_mode {
            QueueMode::Default => {
                map.insert("default".to_owned(), 100);
            }
            QueueMode::Custom => {
                if let Some(ref queues) = self.config.custom_queues {
                    for q in queues {
                        map.insert(q.name.clone(), q.priority);
                    }
                }
            }
        }
        map
    }

    /// Refresh all workflow metadata maps on the workflow registry.
    ///
    /// Called after task or workflow registration so that dynamic start
    /// paths (WorkflowStarter, TaskRuntime, spec_builder) have access
    /// to current task defaults and queue config.
    fn refresh_workflow_metadata_maps(&mut self) {
        let options_map = self.build_task_options_map();
        self.workflow_registry.set_task_options_map(options_map);

        let defaults_map = self.build_task_defaults_map();
        self.workflow_registry.set_task_defaults_map(defaults_map);

        let queue_map = self.build_queue_priority_map();
        self.workflow_registry.set_queue_priority_map(queue_map);
    }

    /// Get list of valid queue names based on configuration.
    ///
    /// In `Default` mode, returns `["default"]`.
    /// In `Custom` mode, returns the names from `custom_queues`.
    ///
    /// Mirrors Python's `Horsies.get_valid_queue_names()`.
    pub fn get_valid_queue_names(&self) -> Vec<String> {
        match self.config.queue_mode {
            QueueMode::Default => vec!["default".to_owned()],
            QueueMode::Custom => self
                .config
                .custom_queues
                .as_ref()
                .map(|queues| queues.iter().map(|q| q.name.clone()).collect())
                .unwrap_or_default(),
        }
    }

    /// Validate that a queue name is allowed by the current configuration.
    ///
    /// In `Default` mode, only `"default"` is valid.
    /// In `Custom` mode, the queue must be in `custom_queues`.
    pub fn validate_queue(&self, queue_name: &str) -> Result<(), HorsiesError> {
        match self.config.queue_mode {
            QueueMode::Default => {
                if queue_name != "default" {
                    return Err(HorsiesError::new(format!(
                        "queue '{}' not allowed in Default queue mode (only 'default' is valid)",
                        queue_name,
                    )));
                }
            }
            QueueMode::Custom => {
                let allowed = self
                    .config
                    .custom_queues
                    .as_ref()
                    .map(|queues| queues.iter().any(|q| q.name == queue_name))
                    .unwrap_or(false);
                if !allowed {
                    return Err(HorsiesError::new(format!(
                        "queue '{}' not in configured custom_queues",
                        queue_name,
                    )));
                }
            }
        }
        Ok(())
    }

    /// Dry-run a task's typed input deserialization for one encoded input,
    /// using the same input path as execution, without running the task.
    ///
    /// Returns the structured deserialize error on mismatch. Shared by
    /// schedule validation and workflow-node validation.
    fn dry_run_task_input(
        task: &crate::core::task::fn_trait::RegisteredTask,
        input: &str,
    ) -> Result<(), crate::core::task::error::TaskError> {
        let task_input = crate::worker::execution::build_task_input(input, false)?;
        task.validate_input(&task_input)
    }

    /// Extract a human-readable detail from a typed-input deserialize error.
    fn input_error_detail(error: &crate::core::task::error::TaskError) -> String {
        error
            .data
            .as_ref()
            .and_then(|d| d.get("validation_error"))
            .and_then(|v| v.as_str())
            .map(str::to_owned)
            .or_else(|| error.message.clone())
            .unwrap_or_else(|| "input type mismatch".to_owned())
    }

    /// Validate a workflow node's static input against the task's declared
    /// input type. Returns `Some(error)` only for a fully-static payload that
    /// fails to deserialize.
    ///
    /// Skipped (returns `None`): subworkflow nodes, unregistered tasks
    /// (reported by registration checks), nodes with runtime-injected fields
    /// (`args_from`, or `workflow_ctx_from` which makes the engine inject the
    /// reserved context kwarg — their static payload is intentionally partial
    /// and would false-positive on the injected fields). A mixed node (static
    /// kwargs plus `args_from`) is therefore skipped entirely; its static
    /// portion is not validated at check-time, and any mismatch still
    /// surfaces at execution. Every other node carries its full input, so a
    /// node input that does not decode as the task's input type (for example
    /// `null` for a task whose input type has fields) is reported here.
    ///
    /// The context skip is gated on the node-level `workflow_ctx_from`, not the
    /// task-level `accepts_workflow_ctx`: the engine injects context only when
    /// `workflow_ctx_from` is non-empty (`engine.rs`), so a context-capable
    /// task whose node sets no `workflow_ctx_from` has a complete static
    /// payload that should be validated.
    fn validate_node_static_input(
        &self,
        workflow_name: &str,
        node: &crate::core::workflow::node::AnyNode,
    ) -> Option<HorsiesError> {
        if node.is_subworkflow {
            return None;
        }
        let task = self.registry.get(&node.task_name).ok()?;
        let injects_ctx = node
            .workflow_ctx_from
            .as_ref()
            .is_some_and(|ids| !ids.is_empty());
        if !node.args_from.is_empty() || injects_ctx {
            return None;
        }
        let input = node.input_json.as_str();
        match Self::dry_run_task_input(task, input) {
            Ok(()) => None,
            Err(task_error) => Some(
                HorsiesError::new(format!(
                    "workflow '{}' node '{}' (task '{}') static input does not match \
                     the task's declared input type",
                    workflow_name,
                    node.node_id.as_deref().unwrap_or("?"),
                    node.task_name,
                ))
                .with_code(ErrorCode::WorkflowInvalidKwargKey)
                .with_note(format!(
                    "input mismatch: {}",
                    Self::input_error_detail(&task_error)
                ))
                .with_help(
                    "ensure the node's input matches the task's declared input type \
                     (names and types)",
                ),
            ),
        }
    }

    /// Validate that all scheduled tasks reference registered task names
    /// and valid queue names.
    ///
    /// Call this after all tasks have been registered. Returns an error
    /// if any schedule references a task name that is not in the registry,
    /// or a queue name that is not valid for the current queue mode.
    pub fn validate_schedules(&self) -> Result<(), HorsiesError> {
        let Some(schedule_config) = &self.config.schedule else {
            return Ok(());
        };

        let mut report = crate::core::error::ValidationReport::new("schedule");

        // The schedule section itself (unique names, patterns, timezones,
        // catch-up bounds) is part of `AppConfig::validate`, which
        // `Horsies::new` runs: a constructed app holds a valid section.
        for task_schedule in &schedule_config.schedules {
            if !task_schedule.enabled {
                continue;
            }

            // 1. Validate task existence.
            if !self.registry.contains(&task_schedule.task_name) {
                report.add(
                    HorsiesError::new(format!(
                        "scheduled task '{}' references unregistered task '{}'",
                        task_schedule.name, task_schedule.task_name,
                    ))
                    .with_code(ErrorCode::TaskNotRegistered)
                    .with_note(format!(
                        "schedule '{}' will fail at runtime because task '{}' is not registered",
                        task_schedule.name, task_schedule.task_name,
                    ))
                    .with_help(
                        "register the task with app.register() before calling validate_schedules()",
                    ),
                );
            }

            // 2. Validate the queue: the schedule's, else the task's.
            let queue_name = schedule_queue(
                task_schedule,
                self.registry.get(&task_schedule.task_name).ok(),
            );
            if let Err(e) = self.validate_queue(queue_name) {
                report.add(
                    HorsiesError::new(format!(
                        "schedule '{}' references invalid queue '{}'",
                        task_schedule.name, queue_name,
                    ))
                    .with_code(ErrorCode::ConfigInvalidSchedule)
                    .with_note(format!("underlying error: {}", e,))
                    .with_help(
                        "ensure the queue_name in the schedule matches a configured queue, \
                         or omit queue_name to use the default queue",
                    ),
                );
            }

            // 3. Validate the input against the task's declared input type.
            // Only when the task is registered (existence reported above).
            if let Ok(task) = self.registry.get(&task_schedule.task_name) {
                let input = task_schedule.input.to_string();
                if let Err(task_error) = Self::dry_run_task_input(task, &input) {
                    report.add(
                        HorsiesError::new(format!(
                            "schedule '{}' input does not match task '{}' declared input type",
                            task_schedule.name, task_schedule.task_name,
                        ))
                        .with_code(ErrorCode::ConfigInvalidSchedule)
                        .with_note(format!(
                            "input mismatch: {}",
                            Self::input_error_detail(&task_error),
                        ))
                        .with_help(
                            "ensure the schedule's input matches the task's declared \
                             input type (names and types)",
                        ),
                    );
                }
            }

            // 4. Refuse input over payload.reject_bytes at start, so a slot has
            // no size refusal of its own (Python `validation.py:259-280`).
            let input_bytes = task_schedule.input.to_string().len();
            if let Some(limit) = self.config.payload.reject_bytes {
                if input_bytes as u64 > limit {
                    report.add(
                        HorsiesError::new(format!(
                            "schedule '{}' input is {} bytes, over payload.reject_bytes={}",
                            task_schedule.name, input_bytes, limit,
                        ))
                        .with_code(ErrorCode::ConfigInvalidSchedule)
                        .with_note(
                            "every slot of the schedule sends the same input, so every \
                             enqueue would be refused",
                        )
                        .with_help(
                            "make the schedule's input smaller, or raise payload.reject_bytes",
                        ),
                    );
                }
            }
        }

        report.into_result()
    }

    /// Check the schedules as [`Horsies::validate_schedules`] does, then
    /// resolve what every fired slot of each enabled schedule stores: its
    /// queue and the task's options text (Python `prepare_schedules`). The
    /// scheduler starts from this value. Errors: every refusal of
    /// `validate_schedules`, collected.
    pub fn prepare_schedules(&self) -> Result<PreparedSchedules, HorsiesError> {
        self.validate_schedules()?;
        let mut prepared = PreparedSchedules::new();
        let Some(schedule_config) = &self.config.schedule else {
            return Ok(prepared);
        };
        for task_schedule in schedule_config.schedules.iter().filter(|s| s.enabled) {
            // validate_schedules refused an unregistered task above.
            let task = self.registry.get(&task_schedule.task_name)?;
            prepared.insert(
                task_schedule.name.clone(),
                ScheduleSendFacts {
                    queue_name: schedule_queue(task_schedule, Some(task)).to_owned(),
                    task_options: task
                        .task_options()
                        .map(|options| options.stored_options_text()),
                },
            );
        }
        Ok(prepared)
    }

    /// Run all offline validation checks.
    ///
    /// Validates config, schedule references, and returns all errors found.
    /// Mirrors the offline portion of Python's `horsies check`.
    ///
    /// For the live broker connectivity check (`check --live`), call
    /// `broker.health_check()` separately.
    pub fn check(&self) -> Result<(), HorsiesError> {
        let mut report = crate::core::error::ValidationReport::new("check");

        // Phase 1: Config validation (already run at construction, but re-check for safety).
        let config_errors = self.config.validate();
        for error in config_errors {
            report.add(error.to_horsies_error());
        }

        // Phase 2: Schedule validation.
        if let Err(e) = self.validate_schedules() {
            report.add(e);
        }

        // Phase 2.5: Task runtime policy validation.
        if let Err(e) = self.validate_task_runtime_policies() {
            report.add(e);
        }

        // Phase 2.6: Task queue metadata validation (Custom mode).
        // In Custom mode, every registered task should have resolved queue
        // metadata. Phase 2.5 catches tasks that have TaskOptions with
        // queue_name=None. This phase catches tasks that have no TaskOptions
        // at all — registered via the low-level register() path.
        if matches!(self.config.queue_mode, QueueMode::Custom) {
            for (task_name, task) in self.registry.iter() {
                if task.queue_name().is_none() && task.task_options().is_none() {
                    report.add(
                        HorsiesError::new(format!(
                            "task '{}' has no queue metadata in Custom queue mode",
                            task_name,
                        ))
                        .with_code(ErrorCode::TaskInvalidOptions)
                        .with_help(
                            "register the task with .queue() or ensure TaskOptions includes queue_name",
                        ),
                    );
                }
            }
        }

        // Phase 2.8: Workflow task registration validation.
        // Every non-subworkflow node in a registered workflow must reference
        // a task that is registered in the task registry. Catches typos and
        // missing registrations at startup rather than at runtime.
        for registered in self.workflow_registry.iter() {
            for node in &registered.spec.tasks {
                if node.is_subworkflow {
                    continue;
                }
                if !self.registry.contains(&node.task_name) {
                    report.add(
                        HorsiesError::new(format!(
                            "workflow '{}' references unregistered task '{}'",
                            registered.spec.name, node.task_name,
                        ))
                        .with_code(ErrorCode::WorkflowUnregisteredTask)
                        .with_help("register the task before registering the workflow, or check for typos in the task name"),
                    );
                }
            }
        }

        // Phase 2.9: Workflow node queue validation.
        // After registration resolves queue/priority, every non-subworkflow
        // node should have a valid queue. Catches unresolved queues (e.g.
        // TaskNode::raw("name", "null") without registered task defaults) and
        // invalid queue overrides (e.g. .queue("wrong")).
        for registered in self.workflow_registry.iter() {
            for node in &registered.spec.tasks {
                if node.is_subworkflow {
                    continue;
                }
                match &node.queue {
                    None => {
                        report.add(
                            HorsiesError::new(format!(
                                "workflow '{}' node '{}' (task '{}') has no resolved queue",
                                registered.spec.name,
                                node.node_id.as_deref().unwrap_or("?"),
                                node.task_name,
                            ))
                            .with_code(ErrorCode::WorkflowUnresolvedQueue)
                            .with_help(
                                "use TaskFunction::node() or set .queue() explicitly on the TaskNode",
                            ),
                        );
                    }
                    Some(queue) => {
                        if let Err(_) = self.validate_queue(queue) {
                            report.add(
                                HorsiesError::new(format!(
                                    "workflow '{}' node '{}' (task '{}') has invalid queue '{}'; valid: {:?}",
                                    registered.spec.name,
                                    node.node_id.as_deref().unwrap_or("?"),
                                    node.task_name,
                                    queue,
                                    self.get_valid_queue_names(),
                                ))
                                .with_code(ErrorCode::WorkflowUnresolvedQueue)
                                .with_help(
                                    "set a valid queue name from configured custom_queues",
                                ),
                            );
                        }
                    }
                }
            }
        }

        // Phase 2.11: Workflow node typed-input validation.
        // Dry-run each fully-static node payload against the task's declared
        // input type. Nodes with runtime-injected fields (args_from /
        // workflow context) are skipped. Surfaces at check what would
        // otherwise only fail at execution.
        for registered in self.workflow_registry.iter() {
            for node in &registered.spec.tasks {
                if let Some(err) = self.validate_node_static_input(&registered.spec.name, node) {
                    report.add(err);
                }
            }
        }

        // Phase 3: Workflow definition_key validation.
        // Every registered workflow must have a definition_key set.
        for registered in self.workflow_registry.iter() {
            if registered.spec.definition_key.is_none() {
                report.add(
                    HorsiesError::new(format!(
                        "workflow '{}' missing definition_key",
                        registered.spec.name,
                    ))
                    .with_code(ErrorCode::WorkflowNoDefinitionKey),
                );
            }
        }

        // Phase 3.1: Dynamic child declaration validation.
        // Parameterized child workflows may declare child workflow edges by
        // definition_key for preflight cycle detection. At check() time the
        // full registry is assembled, so unresolved declared child keys are
        // almost certainly typos or missing registrations.
        for registered in self.workflow_registry.iter_definitions() {
            for child_key in &registered.declared_children {
                if self
                    .workflow_registry
                    .resolve_child_registration(child_key.as_str())
                    .is_none()
                {
                    report.add(
                        HorsiesError::new(format!(
                            "parameterized workflow '{}' declares unknown child definition_key '{}'",
                            registered.name, child_key,
                        ))
                        .with_code(ErrorCode::WorkflowInvalidDependency)
                        .with_help(
                            "register the child workflow with that definition_key before calling check(), or fix the typo in declared_children",
                        ),
                    );
                }
            }
        }

        // Phase 3.2: Explicit workflow builder validation.
        for error in self.check_workflow_builders() {
            report.add(error);
        }

        // Phase 3.5: Duplicate definition_key detection.
        {
            let mut seen: std::collections::HashMap<&str, &str> = std::collections::HashMap::new();
            for registered in self.workflow_registry.iter() {
                if let Some(ref key) = registered.spec.definition_key {
                    if let Some(existing_name) = seen.get(key.as_str()) {
                        report.add(
                            HorsiesError::new(format!(
                                "duplicate definition_key '{}': used by '{}' and '{}'",
                                key, existing_name, registered.spec.name,
                            ))
                            .with_code(ErrorCode::WorkflowDuplicateDefinitionKey),
                        );
                    } else {
                        seen.insert(key.as_str(), &registered.spec.name);
                    }
                }
            }
        }

        // Note: For the live broker connectivity check (`check --live`),
        // call `broker.health_check()` separately. The core crate is IO-free.

        report.into_result()
    }

    fn check_workflow_builders(&self) -> Vec<HorsiesError> {
        // Restore the flag itself, not the environment's verdict, so a set
        // environment variable never leaves the flag on after the check.
        let previous = self.suppress_sends.load(Ordering::Relaxed);
        self.suppress_sends(true);

        let mut errors = Vec::new();
        for builder in &self.workflow_builders {
            errors.extend(builder.run_check(self));
        }

        self.suppress_sends(previous);
        errors
    }

    /// Validate registered task runtime metadata.
    ///
    /// Covers the Rust-equivalent, non-import-based subset of Python's
    /// runtime policy safety checks:
    /// - task_options identity consistency
    /// - queue validity
    /// - retry_policy internal consistency
    /// - retry_policy requiring non-empty auto_retry_for
    /// - user-defined retry codes not colliding with reserved built-in codes
    pub fn validate_task_runtime_policies(&self) -> Result<(), HorsiesError> {
        let mut report = crate::core::error::ValidationReport::new("task_runtime_policy");

        for (task_name, task) in self.registry.iter() {
            let Some(opts) = task.task_options() else {
                continue;
            };

            if opts.task_name.trim().is_empty() {
                report.add(
                    HorsiesError::new("task_options.task_name must not be empty")
                        .with_code(ErrorCode::TaskInvalidOptions)
                        .with_note(format!("task '{}'", task_name))
                        .with_help("set task_options.task_name to the registered task name"),
                );
            } else if opts.task_name != task_name {
                report.add(
                    HorsiesError::new(format!(
                        "task_options.task_name '{}' does not match registered task '{}'",
                        opts.task_name, task_name,
                    ))
                    .with_code(ErrorCode::TaskInvalidOptions)
                    .with_note(format!("task '{}'", task_name))
                    .with_help(
                        "normalize task_options.task_name to match the registered task name",
                    ),
                );
            }

            match (&self.config.queue_mode, &opts.queue_name) {
                (QueueMode::Custom, None) => {
                    report.add(
                        HorsiesError::new(format!(
                            "task '{}' omits queue_name but app uses Custom queue mode",
                            task_name,
                        ))
                        .with_code(ErrorCode::TaskInvalidOptions)
                        .with_note(format!("task '{}'", task_name))
                        .with_help("specify queue_name in task_options"),
                    );
                }
                (_, Some(ref queue_name)) => {
                    if let Err(err) = self.validate_queue(queue_name) {
                        let mut wrapped = HorsiesError::new(format!(
                            "invalid task_options.queue_name '{}' for task '{}'",
                            queue_name, task_name,
                        ))
                        .with_code(ErrorCode::TaskInvalidOptions)
                        .with_note(format!("task '{}'", task_name));

                        if let Some(note) = err.notes.first() {
                            wrapped = wrapped.with_note(format!("underlying error: {}", note));
                        } else {
                            wrapped = wrapped.with_note(format!("underlying error: {}", err));
                        }

                        wrapped = wrapped.with_help(
                            "use a queue name valid for the current app queue mode, \
                             or omit queue_name",
                        );
                        report.add(wrapped);
                    }
                }
                _ => {}
            }

            if let Some(ref retry_policy) = opts.retry_policy {
                if let Err(err) = retry_policy.validate() {
                    report.add(
                        HorsiesError::new(format!("invalid retry policy: {}", err))
                            .with_code(ErrorCode::TaskInvalidOptions)
                            .with_note(format!("task '{}'", task_name))
                            .with_help(
                                "ensure retry_policy uses a valid backoff strategy and intervals",
                            ),
                    );
                }
            }

            match opts.auto_retry_for.as_ref() {
                Some(entries) if entries.is_empty() => {
                    report.add(
                        HorsiesError::new("auto_retry_for must not be empty")
                            .with_code(ErrorCode::TaskInvalidOptions)
                            .with_note(format!("task '{}'", task_name))
                            .with_help("provide at least one retryable error code"),
                    );
                }
                // A user code never has the name of a built-in code: `UserCode::parse`
                // refuses it, so no entry can collide with a built-in.
                Some(_) => {}
                None => {
                    if opts.retry_policy.is_some() {
                        report.add(
                            HorsiesError::new("retry_policy requires auto_retry_for")
                                .with_code(ErrorCode::TaskInvalidOptions)
                                .with_note(format!("task '{}'", task_name))
                                .with_help("set task_options.auto_retry_for to the error codes that should trigger retries"),
                        );
                    }
                }
            }
        }

        report.into_result()
    }

    /// Register a task with an explicit queue, validating the queue first.
    ///
    /// Sets resolved queue and priority on the task's metadata so workflow
    /// node resolution can inherit these defaults.
    pub fn register_with_queue(
        &mut self,
        name: impl Into<String>,
        mut task: crate::core::task::fn_trait::RegisteredTask,
        queue: &str,
    ) -> Result<(), HorsiesError> {
        if task
            .task_options()
            .and_then(|opts| opts.good_until)
            .is_some()
        {
            return Err(HorsiesError::new(
                "definition-time good_until is not supported; use send-time \
                 TaskSendOptions for ad-hoc sends or workflow-node good_until",
            )
            .with_code(ErrorCode::TaskInvalidOptions));
        }

        self.validate_queue(queue)?;
        task.set_queue_name(queue.to_owned());
        task.set_priority(self.effective_priority(queue, None));
        self.registry.register(name, task)?;
        self.refresh_workflow_metadata_maps();
        Ok(())
    }

    /// Resolve effective priority for a task being sent to a specific queue.
    ///
    /// Priority resolution order:
    /// 1. Explicit task priority (if provided)
    /// 2. Queue priority from [`CustomQueueConfig`](crate::core::config::CustomQueueConfig)
    ///    (if in Custom mode and the queue has a config entry)
    /// 3. Default priority (100)
    pub fn effective_priority(&self, queue_name: &str, task_priority: Option<u32>) -> u32 {
        // If the task has an explicit priority, use it.
        if let Some(p) = task_priority {
            return p;
        }
        // If in custom mode, look up the queue's configured priority.
        if let Some(ref queues) = self.config.custom_queues {
            if let Some(q) = queues.iter().find(|q| q.name == queue_name) {
                return q.priority;
            }
        }
        // Fallback default.
        100
    }

    /// Resolve queue and priority for enqueueing a task.
    ///
    /// Validates that the task is registered and the queue is valid, then
    /// resolves the effective priority. Returns a [`ResolvedEnqueue`] that
    /// can be passed directly to `broker.enqueue()`.
    ///
    /// This mirrors Python's `.send()` resolution logic while keeping
    /// horsies-core IO-free (no broker reference needed).
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The task is not registered ([`ErrorCode::TaskNotRegistered`])
    /// - The queue name is not valid for the current queue mode
    pub fn resolve_enqueue(
        &self,
        task_name: &str,
        queue_name: Option<&str>,
        priority: Option<u32>,
    ) -> Result<ResolvedEnqueue, HorsiesError> {
        // 1. Verify task is registered.
        if !self.registry.contains(task_name) {
            return Err(
                HorsiesError::new(format!("task '{}' is not registered", task_name))
                    .with_code(ErrorCode::TaskNotRegistered)
                    .with_help("register the task with app.register() before sending"),
            );
        }

        // 2. Resolve queue name (default to "default").
        let queue = queue_name.unwrap_or("default");

        // 3. Validate queue.
        self.validate_queue(queue)?;

        // 4. Resolve priority.
        let resolved_priority = self.effective_priority(queue, priority);

        Ok(ResolvedEnqueue {
            task_name: task_name.to_string(),
            queue_name: queue.to_string(),
            priority: resolved_priority,
            retention: self.config.retention.resolve_queue_retention(queue),
        })
    }
}

type WorkflowBuilderFn<P> =
    Arc<dyn Fn(&Horsies, &P) -> Result<WorkflowSpec, HorsiesError> + Send + Sync + 'static>;

trait WorkflowBuilderCheck: Send + Sync {
    fn run_check(&self, app: &Horsies) -> Vec<HorsiesError>;
}

struct RegisteredWorkflowBuilder<P> {
    name: String,
    cases: Vec<P>,
    builder: WorkflowBuilderFn<P>,
}

impl<P> WorkflowBuilderCheck for RegisteredWorkflowBuilder<P>
where
    P: Send + Sync + 'static,
{
    fn run_check(&self, app: &Horsies) -> Vec<HorsiesError> {
        if self.cases.is_empty() {
            if TypeId::of::<P>() == TypeId::of::<()>() {
                let unit_case: &P = unsafe {
                    // Safe because the TypeId guard above guarantees P == ().
                    &*((&() as *const ()) as *const P)
                };
                return self.run_case(app, unit_case);
            }

            return vec![HorsiesError::new(format!(
                "workflow builder '{}' requires at least one check case",
                self.name,
            ))
            .with_code(ErrorCode::WorkflowCheckCasesRequired)
            .with_help("register at least one typed case via .case(...) or .cases(...)")];
        }

        let mut errors = Vec::new();
        for case in &self.cases {
            errors.extend(self.run_case(app, case));
        }
        errors
    }
}

impl<P> RegisteredWorkflowBuilder<P>
where
    P: Send + Sync + 'static,
{
    fn run_case(&self, app: &Horsies, case: &P) -> Vec<HorsiesError> {
        let result =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| (self.builder)(app, case)));

        match result {
            Ok(Ok(mut spec)) => {
                let mut errors = Vec::new();

                if spec.definition_key.is_none() {
                    errors.push(
                        HorsiesError::new(format!(
                            "workflow builder '{}' produced a workflow without definition_key",
                            self.name,
                        ))
                        .with_code(ErrorCode::WorkflowNoDefinitionKey),
                    );
                }

                // Validate task registration and resolve queue/priority.
                // Builder-produced specs are not inserted into the workflow
                // registry, so Phase 2.8/2.9 never see them. We must do
                // the same checks here to match Python's builder validation.
                for node in &spec.tasks {
                    if node.is_subworkflow {
                        continue;
                    }
                    if !app.registry().contains(&node.task_name) {
                        errors.push(
                            HorsiesError::new(format!(
                                "workflow builder '{}' references unregistered task '{}'",
                                self.name, node.task_name,
                            ))
                            .with_code(ErrorCode::WorkflowUnregisteredTask)
                            .with_help(
                                "register the task before registering the workflow builder, \
                                 or check for typos in the task name",
                            ),
                        );
                    }
                }

                crate::core::workflow::node::resolve_node_queue_priority(
                    &mut spec.tasks,
                    &|task_name| {
                        let t = app.registry().get(task_name).ok()?;
                        let queue = t.queue_name()?.to_owned();
                        let priority = t.priority()?;
                        Some(crate::core::workflow::node::TaskDefaults {
                            queue_name: queue,
                            priority,
                        })
                    },
                    &|queue_name| app.effective_priority(queue_name, None),
                );

                for node in &spec.tasks {
                    if node.is_subworkflow {
                        continue;
                    }
                    match &node.queue {
                        None => {
                            errors.push(
                                HorsiesError::new(format!(
                                    "workflow builder '{}' node '{}' (task '{}') has no resolved queue",
                                    self.name,
                                    node.node_id.as_deref().unwrap_or("?"),
                                    node.task_name,
                                ))
                                .with_code(ErrorCode::WorkflowUnresolvedQueue)
                                .with_help(
                                    "use TaskFunction::node() or set .queue() explicitly on the TaskNode",
                                ),
                            );
                        }
                        Some(queue) => {
                            if let Err(_) = app.validate_queue(queue) {
                                errors.push(
                                    HorsiesError::new(format!(
                                        "workflow builder '{}' node '{}' (task '{}') has invalid queue '{}'",
                                        self.name,
                                        node.node_id.as_deref().unwrap_or("?"),
                                        node.task_name,
                                        queue,
                                    ))
                                    .with_code(ErrorCode::WorkflowUnresolvedQueue),
                                );
                            }
                        }
                    }
                }

                // Typed-input validation: same as Phase 2.11 for registered specs.
                for node in &spec.tasks {
                    if let Some(err) = app.validate_node_static_input(&self.name, node) {
                        errors.push(err);
                    }
                }

                errors
            }
            Ok(Err(err)) if err.code.is_some() => vec![err],
            Ok(Err(err)) => vec![HorsiesError::new(format!(
                "workflow builder '{}' failed: {}",
                self.name, err,
            ))
            .with_code(ErrorCode::WorkflowCheckBuilderException)],
            Err(_) => vec![HorsiesError::new(format!(
                "workflow builder '{}' panicked during check execution",
                self.name,
            ))
            .with_code(ErrorCode::WorkflowCheckBuilderException)],
        }
    }
}

/// Decode a child workflow's params from the parent node's encoded input, as
/// the client encoded it. The text must pass the core's strict JSON read rule;
/// it is then decoded as stored, with no conversion, in the member order of
/// the text.
///
/// Errors:
/// - [`SpecBuildError::MemberRefused`]: the value of one top-level member
///   does not decode to its field of `P` (the first such member in the text).
/// - [`SpecBuildError::Failed`]: the text fails the strict read rule, or the
///   decode refuses the input as a whole (a missing member, an input that is
///   not an object, text after the value).
fn decode_parameterized_workflow_input<P>(
    workflow_name: &str,
    input_json: &str,
) -> Result<P, SpecBuildError>
where
    P: DeserializeOwned,
{
    crate::core::codec::check_json_text(input_json).map_err(|e| {
        SpecBuildError::Failed(HorsiesError::new(format!(
            "params of workflow '{}' are refused: {}",
            workflow_name, e,
        )))
    })?;
    let mut deserializer = serde_json::Deserializer::from_str(input_json);
    // The strict read rule bounds the depth, so serde_json's own limit of
    // 128 levels is turned off, as `core::codec::from_json_str` does.
    deserializer.disable_recursion_limit();
    let decoded: Result<P, _> = serde_path_to_error::deserialize(&mut deserializer);
    let params = decoded.map_err(|error| params_decode_error(workflow_name, &error))?;
    deserializer
        .end()
        .map_err(|e| whole_input_refused(workflow_name, &e))?;
    Ok(params)
}

/// The error of a params decode that failed at `error.path()`: the first
/// segment names a top-level member ([`SpecBuildError::MemberRefused`]), or
/// the input as a whole failed ([`SpecBuildError::Failed`]). Pure.
fn params_decode_error(
    workflow_name: &str,
    error: &serde_path_to_error::Error<serde_json::Error>,
) -> SpecBuildError {
    match error.path().iter().next() {
        Some(serde_path_to_error::Segment::Map { key }) => SpecBuildError::MemberRefused {
            key: key.clone(),
            cause: error.to_string(),
        },
        Some(
            serde_path_to_error::Segment::Seq { .. }
            | serde_path_to_error::Segment::Enum { .. }
            | serde_path_to_error::Segment::Unknown,
        )
        | None => whole_input_refused(workflow_name, error.inner()),
    }
}

/// The input of `workflow_name` does not decode to its params as a whole.
fn whole_input_refused(workflow_name: &str, error: &serde_json::Error) -> SpecBuildError {
    SpecBuildError::Failed(
        HorsiesError::new(format!(
            "failed to decode params for workflow '{}': {}",
            workflow_name, error,
        ))
        .with_help("ensure the child workflow params type matches the parent-provided input shape"),
    )
}

/// Builder for registering typed workflow builders on the app.
pub struct WorkflowBuilderRegistration<'a, P> {
    app: &'a mut Horsies,
    name: String,
    cases: Vec<P>,
    builder: WorkflowBuilderFn<P>,
}

impl<'a, P> WorkflowBuilderRegistration<'a, P>
where
    P: Send + Sync + 'static,
{
    fn new(app: &'a mut Horsies, name: String, builder: WorkflowBuilderFn<P>) -> Self {
        Self {
            app,
            name,
            cases: Vec::new(),
            builder,
        }
    }

    /// Add one validation case for this builder.
    pub fn case(&mut self, case: P) -> &mut Self {
        self.cases.push(case);
        self
    }

    /// Add multiple validation cases for this builder.
    pub fn cases<I>(&mut self, cases: I) -> &mut Self
    where
        I: IntoIterator<Item = P>,
    {
        self.cases.extend(cases);
        self
    }

    /// Finalize registration of this workflow builder.
    pub fn register(self) -> Result<(), HorsiesError> {
        self.app
            .workflow_builders
            .push(Box::new(RegisteredWorkflowBuilder {
                name: self.name,
                cases: self.cases,
                builder: self.builder,
            }));
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// WorkflowRegistrationBuilder
// ---------------------------------------------------------------------------

/// Builder for constructing and registering a workflow in a single fluent chain.
///
/// Created by [`Horsies::workflow()`]. Wraps a [`WorkflowSpecBuilder`] and
/// holds a mutable reference back to the `Horsies` instance so it can register
/// the result on finalization.
pub struct WorkflowRegistrationBuilder<'a> {
    app: &'a mut Horsies,
    builder: WorkflowSpecBuilder,
}

impl<'a> WorkflowRegistrationBuilder<'a> {
    /// Add a typed task node. Returns a typed node ref for wiring dependencies.
    ///
    /// Delegates to [`WorkflowSpecBuilder::task()`].
    pub fn task<T, I>(
        &mut self,
        node: crate::core::workflow::node::TaskNode<T, I>,
    ) -> crate::core::workflow::node::TypedNodeRef<T> {
        self.builder.task(node)
    }

    /// Add a sub-workflow node. Returns a typed node ref for wiring dependencies.
    ///
    /// Delegates to [`WorkflowSpecBuilder::sub_workflow()`].
    pub fn sub_workflow<P, T>(
        &mut self,
        node: crate::core::workflow::sub_workflow::SubWorkflowNode<P, T>,
    ) -> crate::core::workflow::node::TypedNodeRef<T> {
        self.builder.sub_workflow(node)
    }

    /// Set the error handling policy.
    pub fn on_error(&mut self, policy: crate::core::workflow::status::OnError) -> &mut Self {
        self.builder.on_error(policy);
        self
    }

    /// Set the output task (whose result becomes the workflow result).
    pub fn output(&mut self, node_ref: crate::core::workflow::node::NodeRef) -> &mut Self {
        self.builder.output(node_ref);
        self
    }

    /// Set a custom success policy.
    pub fn success_policy(
        &mut self,
        policy: crate::core::workflow::policy::SuccessPolicy,
    ) -> &mut Self {
        self.builder.success_policy(policy);
        self
    }

    /// Access the inner `WorkflowSpecBuilder` for advanced configuration.
    pub fn inner(&mut self) -> &mut WorkflowSpecBuilder {
        &mut self.builder
    }

    /// Validate, build, and register the workflow spec (without conditions).
    ///
    /// Calls `WorkflowSpecBuilder::build()` and then registers the resulting
    /// spec with an empty conditions map. Use `build_registered_and_register()`
    /// if the workflow has `run_when`/`skip_when` conditions on its nodes.
    ///
    /// # Errors
    ///
    /// Returns an error if validation fails or the workflow name is already
    /// registered.
    pub fn build_and_register(self) -> Result<(), HorsiesError> {
        let spec = self.builder.build()?;
        self.app.register_workflow_spec(spec)
    }

    /// Validate, build with conditions, and register the workflow spec.
    ///
    /// Calls `WorkflowSpecBuilder::build_registered()` which preserves
    /// `run_when`/`skip_when` conditions extracted from nodes, then registers
    /// the result.
    ///
    /// # Errors
    ///
    /// Returns an error if validation fails or the workflow name is already
    /// registered.
    pub fn build_registered_and_register(self) -> Result<(), HorsiesError> {
        let registered = self.builder.build_registered()?;
        self.app.register_workflow(registered)
    }
}

impl std::fmt::Debug for Horsies {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Horsies")
            .field("config", &"AppConfig { ... }")
            .field("registry", &self.registry)
            .field("workflow_registry", &self.workflow_registry)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::config::{
        CustomQueueConfig, PostgresConfig, QueueMode, RecoveryConfig, WorkerResilienceConfig,
    };
    use crate::core::task::error::{
        OperationalErrorCode, TaskErrorCode, UserCode, UserCodeRefused,
    };
    use crate::core::task::fn_trait::{BlockingTaskFn, RawTaskResult, RegisteredTask, TaskMeta};
    use crate::core::task::options::{RetryPolicy, TaskOptions};
    use crate::core::workflow::node::TaskNode;

    /// The params of a child workflow in the decode tests.
    #[derive(Debug, serde::Deserialize)]
    struct ChildParams {
        count: i64,
        label: String,
    }

    /// A member whose value does not fit its field is refused by its key,
    /// with the path in the cause; the builder would not run.
    #[test]
    fn a_params_member_that_does_not_decode_is_refused_by_its_key() {
        let refused = decode_parameterized_workflow_input::<ChildParams>(
            "child",
            r#"{"count":"seven","label":"a"}"#,
        );
        match refused {
            Err(SpecBuildError::MemberRefused { key, cause }) => {
                assert_eq!(key, "count");
                assert!(cause.starts_with("count: invalid type: string"), "{cause}");
            }
            other => panic!("expected a member refusal, got {other:?}"),
        }
    }

    /// The decode reads the members in the order of the text: of two
    /// refused members, the first in the text is named, not the first in key
    /// order.
    #[test]
    fn the_first_refused_member_in_the_text_is_named() {
        let refused = decode_parameterized_workflow_input::<ChildParams>(
            "child",
            r#"{"label":5,"count":"seven"}"#,
        );
        match refused {
            Err(SpecBuildError::MemberRefused { key, .. }) => assert_eq!(key, "label"),
            other => panic!("expected a member refusal, got {other:?}"),
        }
    }

    /// A missing member is not at one member: the input as a whole is
    /// refused, as a failed build.
    #[test]
    fn a_missing_params_member_fails_the_build() {
        let refused = decode_parameterized_workflow_input::<ChildParams>("child", r#"{"count":1}"#);
        match refused {
            Err(SpecBuildError::Failed(error)) => {
                let text = error.to_string();
                assert!(
                    text.starts_with(
                        "error: failed to decode params for workflow 'child': missing field `label`"
                    ),
                    "{text}"
                );
            }
            other => panic!("expected a failed build, got {other:?}"),
        }
    }

    #[test]
    fn params_that_fit_decode() {
        let params = decode_parameterized_workflow_input::<ChildParams>(
            "child",
            r#"{"count":7,"label":"a"}"#,
        )
        .expect("params that fit");
        assert_eq!((params.count, params.label.as_str()), (7, "a"));
    }

    fn valid_config() -> AppConfig {
        AppConfig {
            payload: crate::core::config::payload::PayloadPolicy::default(),
            queue_mode: QueueMode::Default,
            custom_queues: None,
            broker: PostgresConfig {
                database_url: "postgresql://localhost/test".to_owned(),
                session_database_url: None,
                pgbouncer_transaction_mode: false,
                pool_pre_ping: true,
                worker_pool_pre_ping: false,
                pool_size: 30,
                max_overflow: 30,
                retain_rerun_input_default: false,
                pool_timeout: 30,
                pool_recycle: crate::manifest::PoolRecycle::after_seconds(1800),
                echo: false,
                tcp_keepalives: Default::default(),
                tcp_user_timeout: Default::default(),
            },
            cluster_wide_cap: None,
            prefetch_buffer: 0,
            claim_lease_ms: None,
            max_claim_renew_age_ms: 180_000,
            recovery: RecoveryConfig::default(),
            retention: crate::core::RetentionConfig::default(),
            resilience: WorkerResilienceConfig::default(),
            worker_profiles: std::collections::BTreeMap::new(),
            schedule: None,
            resend_on_transient_err: false,
        }
    }

    #[test]
    fn retention_config_refusals_keep_hrs_216_for_single_and_collected_errors() {
        let mut single = valid_config();
        single.retention.history_leaf_horizon_days = 1;
        let error = Horsies::new(single).expect_err("invalid retention must refuse startup");
        assert_eq!(error.code, Some(ErrorCode::ConfigInvalidRetention));

        let mut collected = valid_config();
        collected.retention.history_leaf_horizon_days = 1;
        collected.retention.heartbeat_leaf_horizon_hours = 1;
        let error = Horsies::new(collected).expect_err("all retention errors are collected");
        assert_eq!(error.code, Some(ErrorCode::ConfigInvalidRetention));
        assert_eq!(error.notes.len(), 2);
    }

    fn custom_config() -> AppConfig {
        let mut config = valid_config();
        config.queue_mode = QueueMode::Custom;
        config.custom_queues = Some(vec![
            CustomQueueConfig {
                name: "fast".to_owned(),
                priority: 1,
                max_concurrency: Some(10),
            },
            CustomQueueConfig {
                name: "slow".to_owned(),
                priority: 50,
                max_concurrency: Some(5),
            },
        ]);
        config
    }

    struct DummyTask;
    impl BlockingTaskFn for DummyTask {
        fn execute(&self, args: &[u8]) -> RawTaskResult {
            crate::core::task::result::TaskResult::Ok(args.to_vec())
        }
    }

    fn dummy_registered() -> RegisteredTask {
        RegisteredTask::Blocking {
            task: std::sync::Arc::new(DummyTask),
            meta: TaskMeta::default(),
        }
    }

    fn registered_with_options(opts: TaskOptions) -> RegisteredTask {
        dummy_registered().with_task_options(opts)
    }

    // -- Real typed tasks for input-validation tests. Unlike DummyTask (which
    // uses the default no-op validate_input), these go through the macro's
    // decode_task_input::<T> dry-run, so their validate_input is the real one.

    #[derive(serde::Serialize, serde::Deserialize)]
    struct CountInput {
        count: i64,
    }

    #[allow(clippy::needless_pass_by_value)]
    fn count_task(_input: CountInput) -> Result<i64, crate::core::task::error::TaskError> {
        Ok(0)
    }

    fn unit_task(_input: ()) -> Result<i64, crate::core::task::error::TaskError> {
        Ok(0)
    }

    /// A task whose declared input is the typed `CountInput`.
    fn typed_count_task() -> RegisteredTask {
        crate::blocking_task_fn!(count_task, CountInput)
    }

    /// A task whose declared input is unit `()`.
    fn typed_unit_task() -> RegisteredTask {
        crate::blocking_task_fn!(unit_task, ())
    }

    #[test]
    fn create_app_with_valid_config() {
        let app = Horsies::new(valid_config());
        assert!(app.is_ok());
    }

    #[test]
    fn create_app_with_invalid_config() {
        let mut config = valid_config();
        config.broker.database_url = "mysql://localhost/db".to_owned();
        let result = Horsies::new(config);
        assert!(result.is_err());
    }

    #[test]
    fn register_task() {
        let mut app = Horsies::new(valid_config()).unwrap();
        let result = app.register(
            "my_task",
            RegisteredTask::Blocking {
                task: std::sync::Arc::new(DummyTask),
                meta: TaskMeta::default(),
            },
        );
        assert!(result.is_ok());
        assert!(app.registry().contains("my_task"));
    }

    #[test]
    fn register_duplicate_rejected() {
        let mut app = Horsies::new(valid_config()).unwrap();
        app.register(
            "task_a",
            RegisteredTask::Blocking {
                task: std::sync::Arc::new(DummyTask),
                meta: TaskMeta::default(),
            },
        )
        .unwrap();
        let result = app.register(
            "task_a",
            RegisteredTask::Blocking {
                task: std::sync::Arc::new(DummyTask),
                meta: TaskMeta::default(),
            },
        );
        assert!(result.is_err());
    }

    #[test]
    fn access_config() {
        let app = Horsies::new(valid_config()).unwrap();
        assert_eq!(app.config().queue_mode, QueueMode::Default);
    }

    #[test]
    fn validate_queue_default_mode_accepts_default() {
        let app = Horsies::new(valid_config()).unwrap();
        assert!(app.validate_queue("default").is_ok());
    }

    #[test]
    fn validate_queue_default_mode_rejects_custom() {
        let app = Horsies::new(valid_config()).unwrap();
        assert!(app.validate_queue("high_priority").is_err());
    }

    #[test]
    fn validate_queue_custom_mode() {
        use crate::core::config::CustomQueueConfig;
        let mut config = valid_config();
        config.queue_mode = QueueMode::Custom;
        config.custom_queues = Some(vec![CustomQueueConfig {
            name: "fast".to_owned(),
            priority: 1,
            max_concurrency: Some(10),
        }]);
        let app = Horsies::new(config).unwrap();
        assert!(app.validate_queue("fast").is_ok());
        assert!(app.validate_queue("slow").is_err());
    }

    #[test]
    fn register_with_queue_validates() {
        let mut app = Horsies::new(valid_config()).unwrap();
        let result = app.register_with_queue(
            "task_a",
            RegisteredTask::Blocking {
                task: std::sync::Arc::new(DummyTask),
                meta: TaskMeta::default(),
            },
            "nonexistent",
        );
        assert!(result.is_err());
    }

    // --- effective_priority tests ---

    #[test]
    fn effective_priority_explicit_task_priority_wins() {
        let app = Horsies::new(custom_config()).unwrap();
        // Even though "fast" queue has priority 1, explicit task priority should win.
        assert_eq!(app.effective_priority("fast", Some(42)), 42);
    }

    #[test]
    fn effective_priority_falls_back_to_queue_config() {
        let app = Horsies::new(custom_config()).unwrap();
        // No explicit priority, should use queue's configured priority.
        assert_eq!(app.effective_priority("fast", None), 1);
        assert_eq!(app.effective_priority("slow", None), 50);
    }

    #[test]
    fn effective_priority_falls_back_to_default() {
        let app = Horsies::new(valid_config()).unwrap();
        // Default mode, no custom queues configured, no explicit priority.
        assert_eq!(app.effective_priority("default", None), 100);
    }

    #[test]
    fn effective_priority_unknown_queue_falls_back_to_default() {
        let app = Horsies::new(custom_config()).unwrap();
        // Queue not in custom_queues list, should fall back to 100.
        assert_eq!(app.effective_priority("unknown", None), 100);
    }

    // --- resolve_enqueue tests ---

    #[test]
    fn resolve_enqueue_unregistered_task() {
        let app = Horsies::new(valid_config()).unwrap();
        let result = app.resolve_enqueue("nonexistent", None, None);
        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.code, Some(ErrorCode::TaskNotRegistered));
    }

    #[test]
    fn resolve_enqueue_default_queue() {
        let mut app = Horsies::new(valid_config()).unwrap();
        app.register("my_task", dummy_registered()).unwrap();
        let resolved = app.resolve_enqueue("my_task", None, None).unwrap();
        assert_eq!(resolved.task_name, "my_task");
        assert_eq!(resolved.queue_name, "default");
        assert_eq!(resolved.priority, 100);
    }

    #[test]
    fn resolve_enqueue_explicit_queue_and_priority() {
        let mut app = Horsies::new(custom_config()).unwrap();
        app.register("my_task", dummy_registered()).unwrap();
        let resolved = app
            .resolve_enqueue("my_task", Some("fast"), Some(5))
            .unwrap();
        assert_eq!(resolved.queue_name, "fast");
        assert_eq!(resolved.priority, 5);
    }

    #[test]
    fn resolve_enqueue_uses_queue_priority_when_no_explicit() {
        let mut app = Horsies::new(custom_config()).unwrap();
        app.register("my_task", dummy_registered()).unwrap();
        let resolved = app.resolve_enqueue("my_task", Some("slow"), None).unwrap();
        assert_eq!(resolved.queue_name, "slow");
        assert_eq!(resolved.priority, 50);
    }

    #[test]
    fn resolve_enqueue_invalid_queue_rejected() {
        let mut app = Horsies::new(valid_config()).unwrap();
        app.register("my_task", dummy_registered()).unwrap();
        let result = app.resolve_enqueue("my_task", Some("nonexistent"), None);
        assert!(result.is_err());
    }

    // --- register_workflow_spec tests ---

    #[test]
    fn register_workflow_spec_simple() {
        let mut app = Horsies::new(valid_config()).unwrap();
        let mut builder = WorkflowSpecBuilder::new("my_pipeline");
        builder.task(TaskNode::<()>::raw("step_a", "null"));
        let spec = builder.build().unwrap();
        assert!(app.register_workflow_spec(spec).is_ok());
        assert!(app.workflow_registry().contains("my_pipeline"));
    }

    #[test]
    fn register_workflow_resolves_task_options_from_registry() {
        let mut app = Horsies::new(valid_config()).unwrap();

        // Register a task with retry options.
        let opts = TaskOptions {
            task_name: "retryable_task".to_owned(),
            queue_name: Some("default".to_owned()),
            good_until: None,
            auto_retry_for: Some(vec![TaskErrorCode::User(
                UserCode::parse("STORAGE_ERROR").unwrap(),
            )]),
            retry_policy: Some(RetryPolicy::fixed(vec![60, 300, 900], true).unwrap()),
            timeout_ms: None,
            execution: crate::core::task::Execution::Isolated,
            cancel_grace_ms: None,
        };
        app.register("retryable_task", registered_with_options(opts))
            .unwrap();

        // Register a task without retry options.
        app.register("plain_task", dummy_registered()).unwrap();

        // Build a workflow using both tasks WITHOUT .task_options() on nodes.
        let mut builder = WorkflowSpecBuilder::new("pipeline");
        let a = builder.task(TaskNode::<()>::raw("retryable_task", "null"));
        builder.task(TaskNode::<()>::raw("plain_task", "null").waits_for(a));
        let spec = builder.build().unwrap();

        // Verify task_options_json is None before registration.
        assert!(spec.tasks[0].task_options_json.is_none());
        assert!(spec.tasks[1].task_options_json.is_none());

        app.register_workflow_spec(spec).unwrap();

        // After registration, the retryable task's node should have task_options_json set.
        let registered = app.workflow_registry().get("pipeline").unwrap();
        let node_0 = &registered.spec.tasks[0];
        let node_1 = &registered.spec.tasks[1];

        assert!(
            node_0.task_options_json.is_some(),
            "retryable_task node should have task_options_json resolved from registry"
        );
        // Verify the JSON contains retry-relevant fields only.
        let opts_json: serde_json::Value =
            serde_json::from_str(node_0.task_options_json.as_ref().unwrap()).unwrap();
        assert!(opts_json.get("retry_policy").is_some());
        assert!(opts_json["retry_policy"].get("auto_retry_for").is_some());
        assert!(
            opts_json.get("task_name").is_none(),
            "task_name should be excluded"
        );
        assert!(
            opts_json.get("queue_name").is_none(),
            "queue_name should be excluded"
        );

        // plain_task has no retry options, so task_options_json stays None.
        assert!(
            node_1.task_options_json.is_none(),
            "plain_task node should remain None (no retry options)"
        );
    }

    #[test]
    fn register_workflow_preserves_explicit_task_options() {
        let mut app = Horsies::new(valid_config()).unwrap();

        // Register a task with retry options.
        let opts = TaskOptions {
            task_name: "retryable_task".to_owned(),
            queue_name: Some("default".to_owned()),
            good_until: None,
            auto_retry_for: Some(vec![TaskErrorCode::User(
                UserCode::parse("STORAGE_ERROR").unwrap(),
            )]),
            retry_policy: Some(RetryPolicy::fixed(vec![60, 300, 900], true).unwrap()),
            timeout_ms: None,
            execution: crate::core::task::Execution::Isolated,
            cancel_grace_ms: None,
        };
        app.register("retryable_task", registered_with_options(opts))
            .unwrap();

        // Build a workflow with EXPLICIT task_options on the node (override).
        let custom_opts = r#"{"retry_policy":{"max_retries":1,"intervals":[10],"backoff_strategy":"fixed","jitter":false}}"#;
        let mut builder = WorkflowSpecBuilder::new("pipeline_explicit");
        builder.task(TaskNode::<()>::raw("retryable_task", "null").task_options(custom_opts));
        let spec = builder.build().unwrap();

        app.register_workflow_spec(spec).unwrap();

        // The explicit override should be preserved, not replaced.
        let registered = app.workflow_registry().get("pipeline_explicit").unwrap();
        let node = &registered.spec.tasks[0];
        assert_eq!(node.task_options_json.as_deref(), Some(custom_opts));
    }

    #[test]
    fn register_workflow_spec_duplicate_rejected() {
        let mut app = Horsies::new(valid_config()).unwrap();

        let mut b1 = WorkflowSpecBuilder::new("dup_wf");
        b1.task(TaskNode::<()>::raw("a", "null"));
        app.register_workflow_spec(b1.build().unwrap()).unwrap();

        let mut b2 = WorkflowSpecBuilder::new("dup_wf");
        b2.task(TaskNode::<()>::raw("b", "null"));
        let result = app.register_workflow_spec(b2.build().unwrap());
        assert!(result.is_err());
    }

    // --- workflow() builder tests ---

    #[test]
    fn workflow_builder_registers_in_app() {
        let mut app = Horsies::new(valid_config()).unwrap();
        {
            let mut wb = app.workflow("pipeline");
            let a = wb.task(TaskNode::<()>::raw("fetch_data", "null"));
            wb.task(TaskNode::<()>::raw("process", "null").waits_for(a));
            wb.build_and_register().unwrap();
        }
        assert!(app.workflow_registry().contains("pipeline"));
        assert_eq!(app.workflow_registry().len(), 1);
    }

    #[test]
    fn workflow_builder_validation_error_propagates() {
        let mut app = Horsies::new(valid_config()).unwrap();
        // Empty workflow name should fail validation.
        let wb = app.workflow("");
        let result = wb.build_and_register();
        assert!(result.is_err());
    }

    #[test]
    fn workflow_builder_duplicate_rejected() {
        let mut app = Horsies::new(valid_config()).unwrap();
        {
            let mut wb = app.workflow("wf_dup");
            wb.task(TaskNode::<()>::raw("a", "null"));
            wb.build_and_register().unwrap();
        }
        {
            let mut wb = app.workflow("wf_dup");
            wb.task(TaskNode::<()>::raw("b", "null"));
            let result = wb.build_and_register();
            assert!(result.is_err());
        }
    }

    #[test]
    fn registered_zero_arg_workflow_builder_runs_during_check() {
        let mut app = Horsies::new(valid_config()).unwrap();
        app.register("step_a", dummy_registered()).unwrap();
        let call_count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let counter = Arc::clone(&call_count);

        app.workflow_builder0("build_zero", move |_app| {
            counter.fetch_add(1, Ordering::Relaxed);
            let mut builder = WorkflowSpecBuilder::new("zero_pipeline");
            builder.definition_key("tests.zero.v1");
            builder.task(TaskNode::<()>::raw("step_a", "null"));
            builder.build()
        })
        .unwrap()
        .register()
        .unwrap();

        assert!(app.check().is_ok());
        assert_eq!(call_count.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn parameterized_workflow_builder_requires_cases() {
        let mut app = Horsies::new(valid_config()).unwrap();
        app.register("step_a", dummy_registered()).unwrap();

        app.workflow_builder("build_param", |_app, region: &String| {
            let mut builder = WorkflowSpecBuilder::new(format!("wf_{region}"));
            builder.definition_key(format!("tests.{region}.v1"));
            builder.task(TaskNode::<()>::raw("step_a", "null"));
            builder.build()
        })
        .unwrap()
        .register()
        .unwrap();

        let err = app.check().unwrap_err();
        let rendered = err.to_string();
        assert!(rendered.contains("HRS-027"));
        assert!(rendered.contains("build_param"));
    }

    #[test]
    fn parameterized_workflow_builder_runs_each_case() {
        let mut app = Horsies::new(valid_config()).unwrap();
        app.register("step_a", dummy_registered()).unwrap();
        let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
        let seen_in_builder = Arc::clone(&seen);

        let mut registration = app
            .workflow_builder("build_regional", move |_app, region: &String| {
                seen_in_builder.lock().unwrap().push(region.clone());
                let mut builder = WorkflowSpecBuilder::new(format!("wf_{region}"));
                builder.definition_key(format!("tests.{region}.v1"));
                builder.task(TaskNode::<()>::raw("step_a", "null"));
                builder.build()
            })
            .unwrap();
        registration.cases(["us-east".to_owned(), "eu-west".to_owned()]);
        registration.register().unwrap();

        assert!(app.check().is_ok());
        let mut values = seen.lock().unwrap().clone();
        values.sort();
        assert_eq!(values, vec!["eu-west".to_owned(), "us-east".to_owned()]);
    }

    #[test]
    fn workflow_builder_missing_definition_key_is_reported() {
        let mut app = Horsies::new(valid_config()).unwrap();

        app.workflow_builder0("missing_key", |_app| {
            Ok(WorkflowSpec {
                name: "raw_spec".to_owned(),
                definition_key: None,
                tasks: vec![],
                on_error: crate::core::workflow::status::OnError::Fail,
                output_index: None,
                success_policy: None,
            })
        })
        .unwrap()
        .register()
        .unwrap();

        let err = app.check().unwrap_err();
        let rendered = err.to_string();
        assert!(rendered.contains("HRS-016"));
        assert!(rendered.contains("missing_key"));
    }

    #[test]
    fn check_reports_unknown_declared_child_definition_key() {
        let mut app = Horsies::new(valid_config()).unwrap();

        app.register_parameterized_workflow_with_children::<(), _>(
            "parent_dynamic",
            "tests.parent_dynamic.v1",
            &["tests.missing_child.v1"],
            |_params| {
                let mut builder = WorkflowSpecBuilder::new("parent_dynamic");
                builder.definition_key("tests.parent_dynamic.v1");
                builder.build()
            },
        )
        .unwrap();

        let err = app.check().unwrap_err();
        let rendered = err.to_string();
        assert!(rendered.contains("HRS-006"));
        assert!(rendered.contains("parent_dynamic"));
        assert!(rendered.contains("tests.missing_child.v1"));
    }

    #[test]
    fn check_allows_declared_child_registered_later() {
        let mut app = Horsies::new(valid_config()).unwrap();

        app.register_parameterized_workflow_with_children::<(), _>(
            "parent_dynamic",
            "tests.parent_dynamic.v1",
            &["tests.child_dynamic.v1"],
            |_params| {
                let mut builder = WorkflowSpecBuilder::new("parent_dynamic");
                builder.definition_key("tests.parent_dynamic.v1");
                builder.build()
            },
        )
        .unwrap();

        app.register_parameterized_workflow::<(), _>(
            "child_dynamic",
            "tests.child_dynamic.v1",
            |_params| {
                let mut builder = WorkflowSpecBuilder::new("child_dynamic");
                builder.definition_key("tests.child_dynamic.v1");
                builder.build()
            },
        )
        .unwrap();

        assert!(app.check().is_ok());
    }

    #[test]
    fn workflow_builder_panics_are_wrapped_as_e029() {
        let mut app = Horsies::new(valid_config()).unwrap();

        app.workflow_builder0("panics", |_app| -> Result<WorkflowSpec, HorsiesError> {
            panic!("boom");
        })
        .unwrap()
        .register()
        .unwrap();

        let err = app.check().unwrap_err();
        let rendered = err.to_string();
        assert!(rendered.contains("HRS-029"));
        assert!(rendered.contains("panicked"));
    }

    #[test]
    fn workflow_builder_horsies_error_passes_through() {
        let mut app = Horsies::new(valid_config()).unwrap();

        app.workflow_builder0("config_err", |_app| {
            Err(HorsiesError::new("bad config").with_code(ErrorCode::BrokerInvalidUrl))
        })
        .unwrap()
        .register()
        .unwrap();

        let err = app.check().unwrap_err();
        let rendered = err.to_string();
        assert!(rendered.contains("HRS-203"));
        assert!(rendered.contains("bad config"));
    }

    /// Python `test_suppress_sends_env.py`: only `1` suppresses, with
    /// surrounding whitespace ignored.
    #[test]
    fn only_the_value_one_suppresses_sends() {
        for value in ["1", " 1 ", "1\n"] {
            assert!(suppression_requested(Some(value)), "{value:?}");
        }
        for value in [
            None,
            Some(""),
            Some("0"),
            Some("true"),
            Some("yes"),
            Some("11"),
        ] {
            assert!(!suppression_requested(value), "{value:?}");
        }
    }

    #[test]
    fn workflow_builder_runs_under_send_suppression() {
        let mut app = Horsies::new(valid_config()).unwrap();
        app.register("step_a", dummy_registered()).unwrap();
        let seen = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let seen_in_builder = Arc::clone(&seen);

        app.workflow_builder0("suppressed", move |app| {
            seen_in_builder.store(app.are_sends_suppressed(), Ordering::Relaxed);
            let mut builder = WorkflowSpecBuilder::new("suppressed_pipeline");
            builder.definition_key("tests.suppressed.v1");
            builder.task(TaskNode::<()>::raw("step_a", "null"));
            builder.build()
        })
        .unwrap()
        .register()
        .unwrap();

        assert!(!app.are_sends_suppressed());
        assert!(app.check().is_ok());
        assert!(seen.load(Ordering::Relaxed));
        assert!(!app.are_sends_suppressed());
    }

    // --- validate_schedules tests ---

    #[test]
    fn validate_schedules_ok_when_no_schedules() {
        let app = Horsies::new(valid_config()).unwrap();
        // config.schedule is None by default.
        assert!(app.validate_schedules().is_ok());
    }

    #[test]
    fn validate_schedules_ok_when_all_tasks_registered() {
        use crate::core::config::schedule::{
            IntervalSchedule, ScheduleConfig, SchedulePattern, TaskSchedule,
        };

        let mut config = valid_config();
        config.schedule = Some(ScheduleConfig {
            enabled: true,
            schedules: vec![TaskSchedule {
                name: "hourly_cleanup".to_owned(),
                task_name: "cleanup".to_owned(),
                pattern: SchedulePattern::Interval(IntervalSchedule {
                    seconds: Some(3600),
                    minutes: None,
                    hours: None,
                    days: None,
                }),
                input: serde_json::Value::Null,
                queue_name: None,
                enabled: true,
                timezone: "UTC".to_owned(),
                catch_up_missed: false,
                max_catch_up_runs: 100,
            }],
            check_interval_seconds: 1,
        });
        let mut app = Horsies::new(config).unwrap();
        app.register("cleanup", dummy_registered()).unwrap();
        assert!(app.validate_schedules().is_ok());
    }

    #[test]
    fn validate_schedules_err_when_task_unregistered() {
        use crate::core::config::schedule::{
            IntervalSchedule, ScheduleConfig, SchedulePattern, TaskSchedule,
        };

        let mut config = valid_config();
        config.schedule = Some(ScheduleConfig {
            enabled: true,
            schedules: vec![TaskSchedule {
                name: "nightly_report".to_owned(),
                task_name: "generate_report".to_owned(),
                pattern: SchedulePattern::Interval(IntervalSchedule {
                    seconds: Some(60),
                    minutes: None,
                    hours: None,
                    days: None,
                }),
                input: serde_json::Value::Null,
                queue_name: None,
                enabled: true,
                timezone: "UTC".to_owned(),
                catch_up_missed: false,
                max_catch_up_runs: 100,
            }],
            check_interval_seconds: 1,
        });
        let app = Horsies::new(config).unwrap();
        // "generate_report" is not registered.
        let result = app.validate_schedules();
        assert!(result.is_err());
    }

    #[test]
    fn validate_schedules_error_includes_schedule_and_task_name() {
        use crate::core::config::schedule::{
            IntervalSchedule, ScheduleConfig, SchedulePattern, TaskSchedule,
        };

        let mut config = valid_config();
        config.schedule = Some(ScheduleConfig {
            enabled: true,
            schedules: vec![TaskSchedule {
                name: "my_schedule".to_owned(),
                task_name: "missing_task".to_owned(),
                pattern: SchedulePattern::Interval(IntervalSchedule {
                    seconds: Some(10),
                    minutes: None,
                    hours: None,
                    days: None,
                }),
                input: serde_json::Value::Null,
                queue_name: None,
                enabled: true,
                timezone: "UTC".to_owned(),
                catch_up_missed: false,
                max_catch_up_runs: 100,
            }],
            check_interval_seconds: 1,
        });
        let app = Horsies::new(config).unwrap();
        let err = app.validate_schedules().unwrap_err();
        let display = format!("{}", err);
        assert!(
            display.contains("my_schedule"),
            "error should contain the schedule name, got: {}",
            display,
        );
        assert!(
            display.contains("missing_task"),
            "error should contain the task name, got: {}",
            display,
        );
    }

    // --- validate_schedules: queue validation tests (gap 12-3) ---

    fn interval_schedule(name: &str, queue: Option<&str>) -> crate::core::config::TaskSchedule {
        use crate::core::config::schedule::{IntervalSchedule, SchedulePattern, TaskSchedule};
        TaskSchedule {
            name: name.to_owned(),
            task_name: "my_task".to_owned(),
            pattern: SchedulePattern::Interval(IntervalSchedule {
                seconds: Some(60),
                minutes: None,
                hours: None,
                days: None,
            }),
            input: serde_json::Value::Null,
            queue_name: queue.map(str::to_owned),
            enabled: true,
            timezone: "UTC".to_owned(),
            catch_up_missed: false,
            max_catch_up_runs: 100,
        }
    }

    /// Python `Scheduler._resolve_schedule_queue` and #480: a schedule
    /// without a queue fires on the task's declared queue, and every fired
    /// slot carries the task's options text. The text is the one a send of
    /// the task stores: the five keys of Python `serialize_task_options`
    /// (`python/horsies/core/codec/task_options.py:27-50`), as
    /// `TaskOptions::stored_options_text` writes them since #622.
    #[test]
    fn prepare_schedules_uses_the_task_queue_and_options() {
        use crate::core::config::schedule::ScheduleConfig;
        const STORED_OPTIONS: &str = "{\"retry_policy\":null,\"good_until\":null,\
                                      \"timeout_ms\":5000,\"execution\":\"ISOLATED\",\
                                      \"cancel_grace_ms\":null}";
        let mut config = custom_config();
        config.schedule = Some(ScheduleConfig::new(vec![
            interval_schedule("on_task_queue", None),
            interval_schedule("on_own_queue", Some("fast")),
        ]));
        let mut app = Horsies::new(config).unwrap();
        let options = TaskOptions {
            task_name: "my_task".to_owned(),
            queue_name: None,
            good_until: None,
            auto_retry_for: None,
            retry_policy: None,
            timeout_ms: Some(5_000),
            execution: crate::core::task::Execution::Isolated,
            cancel_grace_ms: None,
        };
        app.register_with_queue("my_task", registered_with_options(options), "slow")
            .unwrap();

        let prepared = app.prepare_schedules().unwrap();
        assert_eq!(
            prepared["on_task_queue"],
            ScheduleSendFacts {
                queue_name: "slow".to_owned(),
                task_options: Some(STORED_OPTIONS.to_owned()),
            },
        );
        assert_eq!(prepared["on_own_queue"].queue_name, "fast");
        assert_eq!(
            prepared["on_own_queue"].task_options.as_deref(),
            Some(STORED_OPTIONS)
        );
    }

    /// Python `validation.py:259-280`: input over `payload.reject_bytes`
    /// refuses the scheduler start, so no slot is refused for its size.
    #[test]
    fn validate_schedules_refuses_input_over_reject_bytes() {
        use crate::core::config::schedule::ScheduleConfig;
        let mut config = valid_config();
        config.payload.reject_bytes = Some(8);
        let mut schedule = interval_schedule("too_big", None);
        schedule.input = serde_json::json!({"payload": "over the limit"});
        config.schedule = Some(ScheduleConfig::new(vec![schedule]));
        let mut app = Horsies::new(config).unwrap();
        app.register("my_task", dummy_registered()).unwrap();

        let error = app.validate_schedules().unwrap_err();
        assert!(
            error.to_string().contains("over payload.reject_bytes=8"),
            "{error}"
        );
        assert!(app.prepare_schedules().is_err());
    }

    #[test]
    fn validate_schedules_ok_with_valid_queue_in_custom_mode() {
        use crate::core::config::schedule::{
            IntervalSchedule, ScheduleConfig, SchedulePattern, TaskSchedule,
        };

        let mut config = custom_config();
        config.schedule = Some(ScheduleConfig {
            enabled: true,
            schedules: vec![TaskSchedule {
                name: "fast_job".to_owned(),
                task_name: "my_task".to_owned(),
                pattern: SchedulePattern::Interval(IntervalSchedule {
                    seconds: Some(60),
                    minutes: None,
                    hours: None,
                    days: None,
                }),
                input: serde_json::Value::Null,
                queue_name: Some("fast".to_owned()),
                enabled: true,
                timezone: "UTC".to_owned(),
                catch_up_missed: false,
                max_catch_up_runs: 100,
            }],
            check_interval_seconds: 1,
        });
        let mut app = Horsies::new(config).unwrap();
        app.register("my_task", dummy_registered()).unwrap();
        assert!(app.validate_schedules().is_ok());
    }

    #[test]
    fn validate_schedules_err_with_invalid_queue_in_custom_mode() {
        use crate::core::config::schedule::{
            IntervalSchedule, ScheduleConfig, SchedulePattern, TaskSchedule,
        };

        let mut config = custom_config();
        config.schedule = Some(ScheduleConfig {
            enabled: true,
            schedules: vec![TaskSchedule {
                name: "bad_queue_job".to_owned(),
                task_name: "my_task".to_owned(),
                pattern: SchedulePattern::Interval(IntervalSchedule {
                    seconds: Some(60),
                    minutes: None,
                    hours: None,
                    days: None,
                }),
                input: serde_json::Value::Null,
                queue_name: Some("nonexistent_queue".to_owned()),
                enabled: true,
                timezone: "UTC".to_owned(),
                catch_up_missed: false,
                max_catch_up_runs: 100,
            }],
            check_interval_seconds: 1,
        });
        let mut app = Horsies::new(config).unwrap();
        app.register("my_task", dummy_registered()).unwrap();
        let result = app.validate_schedules();
        assert!(result.is_err());
        let err = result.unwrap_err();
        let display = format!("{}", err);
        assert!(
            display.contains("bad_queue_job"),
            "error should contain the schedule name, got: {}",
            display,
        );
        assert!(
            display.contains("nonexistent_queue"),
            "error should mention the invalid queue, got: {}",
            display,
        );
    }

    #[test]
    fn validate_schedules_err_with_non_default_queue_in_default_mode() {
        use crate::core::config::schedule::{
            IntervalSchedule, ScheduleConfig, SchedulePattern, TaskSchedule,
        };

        let mut config = valid_config();
        config.schedule = Some(ScheduleConfig {
            enabled: true,
            schedules: vec![TaskSchedule {
                name: "wrong_mode".to_owned(),
                task_name: "my_task".to_owned(),
                pattern: SchedulePattern::Interval(IntervalSchedule {
                    seconds: Some(60),
                    minutes: None,
                    hours: None,
                    days: None,
                }),
                input: serde_json::Value::Null,
                queue_name: Some("high_priority".to_owned()),
                enabled: true,
                timezone: "UTC".to_owned(),
                catch_up_missed: false,
                max_catch_up_runs: 100,
            }],
            check_interval_seconds: 1,
        });
        let mut app = Horsies::new(config).unwrap();
        app.register("my_task", dummy_registered()).unwrap();
        let result = app.validate_schedules();
        assert!(result.is_err());
    }

    #[test]
    fn validate_schedules_ok_with_default_queue_fallback() {
        use crate::core::config::schedule::{
            IntervalSchedule, ScheduleConfig, SchedulePattern, TaskSchedule,
        };

        // In Default mode, a schedule with queue_name=None should resolve to "default" and pass.
        let mut config = valid_config();
        config.schedule = Some(ScheduleConfig {
            enabled: true,
            schedules: vec![TaskSchedule {
                name: "implicit_default".to_owned(),
                task_name: "my_task".to_owned(),
                pattern: SchedulePattern::Interval(IntervalSchedule {
                    seconds: Some(60),
                    minutes: None,
                    hours: None,
                    days: None,
                }),
                input: serde_json::Value::Null,
                queue_name: None,
                enabled: true,
                timezone: "UTC".to_owned(),
                catch_up_missed: false,
                max_catch_up_runs: 100,
            }],
            check_interval_seconds: 1,
        });
        let mut app = Horsies::new(config).unwrap();
        app.register("my_task", dummy_registered()).unwrap();
        assert!(app.validate_schedules().is_ok());
    }

    #[test]
    fn validate_schedules_skips_disabled_schedules() {
        use crate::core::config::schedule::{
            IntervalSchedule, ScheduleConfig, SchedulePattern, TaskSchedule,
        };

        // A disabled schedule with an invalid queue or unregistered task should not error.
        let mut config = valid_config();
        config.schedule = Some(ScheduleConfig {
            enabled: true,
            schedules: vec![TaskSchedule {
                name: "disabled_bad".to_owned(),
                task_name: "nonexistent_task".to_owned(),
                pattern: SchedulePattern::Interval(IntervalSchedule {
                    seconds: Some(60),
                    minutes: None,
                    hours: None,
                    days: None,
                }),
                input: serde_json::Value::Null,
                queue_name: Some("nonexistent_queue".to_owned()),
                enabled: false,
                timezone: "UTC".to_owned(),
                catch_up_missed: false,
                max_catch_up_runs: 100,
            }],
            check_interval_seconds: 1,
        });
        let app = Horsies::new(config).unwrap();
        assert!(app.validate_schedules().is_ok());
    }

    // --- validate_task_runtime_policies / check tests ---

    #[test]
    fn check_rejects_retry_policy_without_auto_retry_for() {
        let mut app = Horsies::new(valid_config()).unwrap();
        let opts = TaskOptions {
            task_name: "retry_me".to_owned(),
            queue_name: Some("default".to_owned()),
            good_until: None,
            auto_retry_for: None,
            retry_policy: Some(RetryPolicy::fixed(vec![60, 300], true).unwrap()),
            timeout_ms: None,
            execution: crate::core::task::Execution::Isolated,
            cancel_grace_ms: None,
        };

        app.register("retry_me", registered_with_options(opts))
            .unwrap();

        let err = app.check().unwrap_err();
        assert_eq!(err.code, Some(ErrorCode::TaskInvalidOptions));
        assert!(err
            .to_string()
            .contains("retry_policy requires auto_retry_for"));
    }

    #[test]
    fn check_rejects_empty_auto_retry_for() {
        let mut app = Horsies::new(valid_config()).unwrap();
        let opts = TaskOptions {
            task_name: "empty_retry_codes".to_owned(),
            queue_name: Some("default".to_owned()),
            good_until: None,
            auto_retry_for: Some(Vec::new()),
            retry_policy: None,
            timeout_ms: None,
            execution: crate::core::task::Execution::Isolated,
            cancel_grace_ms: None,
        };

        app.register("empty_retry_codes", registered_with_options(opts))
            .unwrap();

        let err = app.check().unwrap_err();
        assert_eq!(err.code, Some(ErrorCode::TaskInvalidOptions));
        assert!(err.to_string().contains("auto_retry_for must not be empty"));
    }

    /// An `auto_retry_for` user code cannot have a built-in name: the user code
    /// type refuses it before any option is built (Python
    /// `core/models/tasks.py:304-311`).
    #[test]
    fn an_auto_retry_for_user_code_cannot_have_a_built_in_name() {
        let refused = UserCode::parse(OperationalErrorCode::BrokerError.to_string());
        assert!(matches!(refused, Err(UserCodeRefused::BuiltInName(_))));
    }

    #[test]
    // --- Queue mismatch guard (matches Python test_queue_mismatch_guard.py) ---

    // 1. DEFAULT mode + task with queue_name → hard fail
    fn register_rejects_task_with_queue_in_default_mode() {
        let mut app = Horsies::new(valid_config()).unwrap(); // QueueMode::Default
        let opts = TaskOptions {
            task_name: "queue_mismatch".to_owned(),
            queue_name: Some("critical".to_owned()),
            good_until: None,
            auto_retry_for: None,
            retry_policy: None,
            timeout_ms: None,
            execution: crate::core::task::Execution::Isolated,
            cancel_grace_ms: None,
        };

        let err = app
            .register("queue_mismatch", registered_with_options(opts))
            .unwrap_err();
        assert_eq!(err.code, Some(ErrorCode::TaskInvalidOptions));
        assert!(err
            .to_string()
            .contains("not valid for the current queue config"));
    }

    // 2. CUSTOM mode + task without queue_name → hard fail
    #[test]
    fn register_rejects_task_without_queue_in_custom_mode() {
        let mut app = Horsies::new(custom_config()).unwrap();
        let opts = TaskOptions {
            task_name: "missing_queue".to_owned(),
            queue_name: None,
            good_until: None,
            auto_retry_for: None,
            retry_policy: None,
            timeout_ms: None,
            execution: crate::core::task::Execution::Isolated,
            cancel_grace_ms: None,
        };

        let err = app
            .register("missing_queue", registered_with_options(opts))
            .unwrap_err();
        assert_eq!(err.code, Some(ErrorCode::TaskInvalidOptions));
        assert!(err.to_string().contains("Custom queue mode"));
    }

    // 3. CUSTOM mode + task with non-existent queue → hard fail
    #[test]
    fn register_rejects_task_with_unknown_queue_in_custom_mode() {
        let mut app = Horsies::new(custom_config()).unwrap(); // has "fast", "slow"
        let opts = TaskOptions {
            task_name: "bad_queue".to_owned(),
            queue_name: Some("analytics".to_owned()),
            good_until: None,
            auto_retry_for: None,
            retry_policy: None,
            timeout_ms: None,
            execution: crate::core::task::Execution::Isolated,
            cancel_grace_ms: None,
        };

        let err = app
            .register("bad_queue", registered_with_options(opts))
            .unwrap_err();
        assert_eq!(err.code, Some(ErrorCode::TaskInvalidOptions));
        assert!(err.to_string().contains("analytics"));
    }

    // 4. CUSTOM mode + task with valid queue → OK
    #[test]
    fn register_accepts_task_with_valid_queue_in_custom_mode() {
        let mut app = Horsies::new(custom_config()).unwrap();
        let opts = TaskOptions {
            task_name: "good_task".to_owned(),
            queue_name: Some("fast".to_owned()),
            good_until: None,
            auto_retry_for: None,
            retry_policy: None,
            timeout_ms: None,
            execution: crate::core::task::Execution::Isolated,
            cancel_grace_ms: None,
        };

        app.register("good_task", registered_with_options(opts))
            .unwrap();
    }

    // 5. DEFAULT mode + task without queue_name → OK
    #[test]
    fn register_accepts_task_without_queue_in_default_mode() {
        let mut app = Horsies::new(valid_config()).unwrap();
        let opts = TaskOptions {
            task_name: "no_queue".to_owned(),
            queue_name: None,
            good_until: None,
            auto_retry_for: None,
            retry_policy: None,
            timeout_ms: None,
            execution: crate::core::task::Execution::Isolated,
            cancel_grace_ms: None,
        };

        app.register("no_queue", registered_with_options(opts))
            .unwrap();
    }

    // 6. check() also catches CUSTOM + missing queue (defense in depth)
    #[test]
    fn check_catches_custom_mode_missing_queue() {
        let mut app = Horsies::new(custom_config()).unwrap();
        // Use register on the registry directly to bypass register() guard.
        app.registry
            .register(
                "sneaky",
                registered_with_options(TaskOptions {
                    task_name: "sneaky".to_owned(),
                    queue_name: None,
                    good_until: None,
                    auto_retry_for: None,
                    retry_policy: None,
                    timeout_ms: None,
                    execution: crate::core::task::Execution::Isolated,
                    cancel_grace_ms: None,
                }),
            )
            .unwrap();

        let err = app.check().unwrap_err();
        assert_eq!(err.code, Some(ErrorCode::TaskInvalidOptions));
        assert!(err.to_string().contains("Custom queue mode"));
    }

    // 8. CUSTOM mode + one valid task + one invalid task → hard fail
    //    The single bad task poisons the whole startup.
    #[test]
    fn register_one_bad_task_poisons_startup() {
        let mut app = Horsies::new(custom_config()).unwrap(); // has "fast", "slow"

        // First task: valid queue → OK
        let good_opts = TaskOptions {
            task_name: "good_task".to_owned(),
            queue_name: Some("fast".to_owned()),
            good_until: None,
            auto_retry_for: None,
            retry_policy: None,
            timeout_ms: None,
            execution: crate::core::task::Execution::Isolated,
            cancel_grace_ms: None,
        };
        app.register("good_task", registered_with_options(good_opts))
            .unwrap();

        // Second task: invalid queue → must fail, poisoning the whole app.
        let bad_opts = TaskOptions {
            task_name: "bad_task".to_owned(),
            queue_name: Some("analytics".to_owned()),
            good_until: None,
            auto_retry_for: None,
            retry_policy: None,
            timeout_ms: None,
            execution: crate::core::task::Execution::Isolated,
            cancel_grace_ms: None,
        };
        let err = app
            .register("bad_task", registered_with_options(bad_opts))
            .unwrap_err();
        assert_eq!(err.code, Some(ErrorCode::TaskInvalidOptions));
        assert!(err.to_string().contains("analytics"));
    }

    // 7. Error messages list valid queues for actionability
    #[test]
    fn register_error_lists_valid_queues() {
        let mut app = Horsies::new(custom_config()).unwrap();
        let opts = TaskOptions {
            task_name: "bad".to_owned(),
            queue_name: Some("nope".to_owned()),
            good_until: None,
            auto_retry_for: None,
            retry_policy: None,
            timeout_ms: None,
            execution: crate::core::task::Execution::Isolated,
            cancel_grace_ms: None,
        };

        let err = app
            .register("bad", registered_with_options(opts))
            .unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("fast"), "error should list valid queue 'fast'");
        assert!(msg.contains("slow"), "error should list valid queue 'slow'");
    }

    #[test]
    fn check_accepts_valid_registered_task_options() {
        let mut app = Horsies::new(valid_config()).unwrap();
        let opts = TaskOptions {
            task_name: "valid_retry_task".to_owned(),
            queue_name: Some("default".to_owned()),
            good_until: None,
            auto_retry_for: Some(vec![TaskErrorCode::User(
                UserCode::parse("FETCH_ERROR").unwrap(),
            )]),
            retry_policy: Some(RetryPolicy::fixed(vec![60, 300], true).unwrap()),
            timeout_ms: None,
            execution: crate::core::task::Execution::Isolated,
            cancel_grace_ms: None,
        };

        app.register("valid_retry_task", registered_with_options(opts))
            .unwrap();

        assert!(app.check().is_ok());
    }

    // --- get_valid_queue_names tests ---

    #[test]
    fn get_valid_queue_names_default_mode() {
        let app = Horsies::new(valid_config()).unwrap();
        let names = app.get_valid_queue_names();
        assert_eq!(names, vec!["default"]);
    }

    #[test]
    fn get_valid_queue_names_custom_mode() {
        let app = Horsies::new(custom_config()).unwrap();
        let names = app.get_valid_queue_names();
        assert_eq!(names.len(), 2);
        assert!(names.contains(&"fast".to_owned()));
        assert!(names.contains(&"slow".to_owned()));
    }

    // -----------------------------------------------------------------------
    // Node inputs are checked as given: no default, no conversion
    // -----------------------------------------------------------------------

    #[test]
    fn check_catches_a_null_input_for_a_task_with_fields() {
        // A node carries the input it was given; `null` for a task whose input
        // type has fields does not decode, and check reports it.
        let mut app = Horsies::new(valid_config()).unwrap();
        app.register("counter", typed_count_task()).unwrap();
        app.register("ping", typed_unit_task()).unwrap();

        let mut builder = WorkflowSpecBuilder::new("null_input_wf");
        builder.definition_key("tests.null_input.v1");
        let bad_node = builder.task(
            TaskNode::<()>::raw("counter", "null")
                .node_id("bad")
                .queue("default"),
        );
        // A unit task takes `null`, its own encoding.
        builder.task(
            TaskNode::<()>::raw("ping", "null")
                .node_id("ok")
                .queue("default")
                .waits_for(bad_node),
        );
        let spec = builder.build().unwrap();
        app.register_workflow_spec(spec).unwrap();

        let err = app.check().unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("does not match"), "got: {}", msg);
        assert!(msg.contains("node 'bad'"), "got: {}", msg);
        assert!(!msg.contains("node 'ok'"), "got: {}", msg);
    }

    #[test]
    fn check_refuses_an_empty_object_for_a_unit_task() {
        // `{}` is never read as `null`: a unit task given `{}` does not
        // decode, and check reports it.
        let mut app = Horsies::new(valid_config()).unwrap();
        app.register("ping", typed_unit_task()).unwrap();

        let mut builder = WorkflowSpecBuilder::new("empty_object_wf");
        builder.definition_key("tests.empty_object.v1");
        builder.task(
            TaskNode::<()>::raw("ping", "{}")
                .node_id("ping")
                .queue("default"),
        );
        let spec = builder.build().unwrap();
        app.register_workflow_spec(spec).unwrap();

        let err = app.check().unwrap_err();
        assert!(err.to_string().contains("does not match"), "got: {}", err);
    }

    #[test]
    fn check_catches_a_null_input_in_builder_produced_spec() {
        let mut app = Horsies::new(valid_config()).unwrap();
        app.register("counter", typed_count_task()).unwrap();

        app.workflow_builder0("bad_builder", |_app| {
            let mut builder = WorkflowSpecBuilder::new("builder_null_input");
            builder.definition_key("tests.builder_null_input.v1");
            builder.task(
                TaskNode::<()>::raw("counter", "null")
                    .node_id("bad")
                    .queue("default"),
            );
            builder.build()
        })
        .unwrap()
        .register()
        .unwrap();

        let err = app.check().unwrap_err();
        assert!(
            err.to_string().contains("does not match"),
            "builder check should report the input mismatch, got: {}",
            err
        );
    }

    // -- Schedule typed-input validation --

    fn schedule_with_kwargs(task_name: &str, kwargs: serde_json::Value) -> AppConfig {
        use crate::core::config::schedule::{
            IntervalSchedule, ScheduleConfig, SchedulePattern, TaskSchedule,
        };
        let mut config = valid_config();
        config.schedule = Some(ScheduleConfig {
            enabled: true,
            schedules: vec![TaskSchedule {
                name: "s1".to_owned(),
                task_name: task_name.to_owned(),
                pattern: SchedulePattern::Interval(IntervalSchedule {
                    seconds: Some(3600),
                    minutes: None,
                    hours: None,
                    days: None,
                }),
                input: kwargs,
                queue_name: None,
                enabled: true,
                timezone: "UTC".to_owned(),
                catch_up_missed: false,
                max_catch_up_runs: 100,
            }],
            check_interval_seconds: 1,
        });
        config
    }

    #[test]
    fn validate_schedules_ok_when_kwargs_match_typed_input() {
        let mut app = Horsies::new(schedule_with_kwargs(
            "counter",
            serde_json::json!({"count": 5}),
        ))
        .unwrap();
        app.register("counter", typed_count_task()).unwrap();
        assert!(app.validate_schedules().is_ok());
    }

    #[test]
    fn validate_schedules_err_when_kwargs_mismatch_typed_input() {
        let mut app = Horsies::new(schedule_with_kwargs(
            "counter",
            serde_json::json!({"count": "not-an-int"}),
        ))
        .unwrap();
        app.register("counter", typed_count_task()).unwrap();
        let err = app.validate_schedules().unwrap_err();
        assert!(
            err.to_string().contains("does not match"),
            "expected input-mismatch error, got: {}",
            err
        );
    }

    #[test]
    fn validate_schedules_ok_for_unit_task_without_payload() {
        let mut app = Horsies::new(schedule_with_kwargs("ping", serde_json::Value::Null)).unwrap();
        app.register("ping", typed_unit_task()).unwrap();
        assert!(app.validate_schedules().is_ok());
    }

    #[test]
    fn validate_schedules_err_for_unit_task_with_kwargs() {
        let mut app =
            Horsies::new(schedule_with_kwargs("ping", serde_json::json!({"x": 1}))).unwrap();
        app.register("ping", typed_unit_task()).unwrap();
        assert!(app.validate_schedules().is_err());
    }

    // -- Workflow node typed-input validation --

    #[test]
    fn check_catches_node_static_kwargs_type_mismatch() {
        let mut app = Horsies::new(valid_config()).unwrap();
        app.register("counter", typed_count_task()).unwrap();

        let mut builder = WorkflowSpecBuilder::new("node_mismatch_wf");
        builder.definition_key("tests.node_mismatch.v1");
        builder.task(
            TaskNode::<()>::raw("counter", "null")
                .node_id("c")
                .queue("default")
                .input_json(r#"{"count": "not-an-int"}"#.to_owned()),
        );
        let spec = builder.build().unwrap();
        app.register_workflow_spec(spec).unwrap();

        let err = app.check().unwrap_err();
        assert!(
            err.to_string().contains("does not match"),
            "expected node input-mismatch error, got: {}",
            err
        );
    }

    #[test]
    fn check_ok_when_node_static_kwargs_match() {
        let mut app = Horsies::new(valid_config()).unwrap();
        app.register("counter", typed_count_task()).unwrap();

        let mut builder = WorkflowSpecBuilder::new("node_ok_wf");
        builder.definition_key("tests.node_ok.v1");
        builder.task(
            TaskNode::<()>::raw("counter", "null")
                .node_id("c")
                .queue("default")
                .input_json(r#"{"count": 7}"#.to_owned()),
        );
        let spec = builder.build().unwrap();
        app.register_workflow_spec(spec).unwrap();

        assert!(app.check().is_ok());
    }

    #[test]
    fn check_skips_node_typed_validation_when_args_from_present() {
        // A node fed by a dependency carries only a partial static payload;
        // typed validation must skip it (no false positive on the injected
        // field). Here the static kwargs alone would mismatch CountInput.
        let mut app = Horsies::new(valid_config()).unwrap();
        app.register("counter", typed_count_task()).unwrap();
        app.register("producer", dummy_registered()).unwrap();

        let mut builder = WorkflowSpecBuilder::new("node_args_from_wf");
        builder.definition_key("tests.node_args_from.v1");
        let producer = builder.task(
            TaskNode::<()>::raw("producer", "null")
                .node_id("p")
                .queue("default"),
        );
        builder.task(
            TaskNode::<()>::raw("counter", "{}")
                .node_id("c")
                .queue("default")
                .waits_for(producer)
                .raw_arg_from("count", producer.into()),
        );
        let spec = builder.build().unwrap();
        app.register_workflow_spec(spec).unwrap();

        assert!(app.check().is_ok());
    }

    #[test]
    fn check_validates_ctx_capable_task_node_without_ctx_from() {
        // A context-capable task whose node sets no workflow_ctx_from receives
        // no runtime injection, so its static payload is complete and must be
        // validated (gate is node-level workflow_ctx_from, not task capability).
        let mut app = Horsies::new(valid_config()).unwrap();
        app.register("counter_ctx", typed_count_task().with_workflow_ctx())
            .unwrap();

        let mut builder = WorkflowSpecBuilder::new("ctx_node_no_ctx_from_wf");
        builder.definition_key("tests.ctx_node_no_ctx_from.v1");
        builder.task(
            TaskNode::<()>::raw("counter_ctx", "null")
                .node_id("c")
                .queue("default")
                .input_json(r#"{"count": "not-an-int"}"#.to_owned()),
        );
        let spec = builder.build().unwrap();
        app.register_workflow_spec(spec).unwrap();

        let err = app.check().unwrap_err();
        assert!(
            err.to_string().contains("does not match"),
            "ctx-capable node without workflow_ctx_from should still be validated, got: {}",
            err
        );
    }
}
