use std::any::TypeId;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use crate::core::config::AppConfig;
use crate::core::config::QueueMode;
use crate::core::error::{ErrorCode, HorsiesError};
use crate::core::registry::task::TaskRegistry;
use crate::core::registry::workflow::{RegisteredWorkflowSpec, WorkflowSpecRegistry};
use crate::core::task::error::{BuiltInTaskCode, TaskErrorCode};
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
                report.add(HorsiesError::new(error.to_string()));
            }
            return report.into_result().map(|_| unreachable!());
        }

        Ok(Self {
            config,
            registry: TaskRegistry::new(),
            workflow_registry: WorkflowSpecRegistry::new(),
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
        task: crate::core::task::fn_trait::RegisteredTask,
    ) -> Result<(), HorsiesError> {
        if let Some(opts) = task.task_options() {
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
        self.registry.register(name, task)
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
                    .and_then(|opts| opts.serialize_retry_options())
            },
        );

        // Update the task_options_map on the workflow registry so the engine
        // can resolve options for dynamically built specs (spec_builder).
        let map = self.build_task_options_map();
        self.workflow_registry.set_task_options_map(map);

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
    /// use horsies::{Horsies, WorkflowSpecBuilder, TaskNode};
    ///
    /// let mut app = Horsies::new(config)?;
    /// let mut builder = WorkflowSpecBuilder::new("my_pipeline");
    /// builder.task(TaskNode::<()>::new("step_a"));
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
    /// use horsies::{Horsies, TaskNode};
    ///
    /// let mut app = Horsies::new(config)?;
    /// let mut wb = app.workflow("my_pipeline");
    /// wb.task(TaskNode::<()>::new("step_a"));
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

    /// Whether sends are currently suppressed.
    ///
    /// Mirrors Python's `Horsies.are_sends_suppressed()`.
    pub fn are_sends_suppressed(&self) -> bool {
        self.suppress_sends.load(Ordering::Relaxed)
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
                if let Some(json) = opts.serialize_retry_options() {
                    map.insert(name.to_owned(), json);
                }
            }
        }
        map
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

        // 0. Validate schedule config (unique names, pattern constraints).
        if let Err(e) = schedule_config.validate() {
            report.add(
                HorsiesError::new(e)
                    .with_code(ErrorCode::ConfigInvalidSchedule)
                    .with_help(
                        "check schedule names for duplicates and pattern fields for valid ranges",
                    ),
            );
        }

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

            // 2. Validate queue name.
            let queue_name = task_schedule.queue_name.as_deref().unwrap_or("default");
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
        }

        report.into_result()
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
            report.add(HorsiesError::new(error.to_string()));
        }

        // Phase 2: Schedule validation.
        if let Err(e) = self.validate_schedules() {
            report.add(e);
        }

        // Phase 2.5: Task runtime policy validation.
        if let Err(e) = self.validate_task_runtime_policies() {
            report.add(e);
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
        let previous = self.are_sends_suppressed();
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
                            wrapped =
                                wrapped.with_note(format!("underlying error: {}", note));
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
                Some(entries) => {
                    for entry in entries {
                        if let TaskErrorCode::User(code) = entry {
                            if let Some(builtin) = BuiltInTaskCode::parse(code) {
                                report.add(
                                    HorsiesError::new(format!(
                                        "auto_retry_for value '{}' collides with reserved built-in {}",
                                        code, builtin,
                                    ))
                                    .with_code(ErrorCode::CheckReservedCodeCollision)
                                    .with_note(format!("task '{}'", task_name))
                                    .with_help(
                                        "use TaskErrorCode::BuiltIn(...) for built-in retry codes, or rename the custom user code",
                                    ),
                                );
                            }
                        }
                    }
                }
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
    pub fn register_with_queue(
        &mut self,
        name: impl Into<String>,
        task: crate::core::task::fn_trait::RegisteredTask,
        queue: &str,
    ) -> Result<(), HorsiesError> {
        self.validate_queue(queue)?;
        self.registry.register(name, task)
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
            Ok(Ok(spec)) => {
                if spec.definition_key.is_none() {
                    vec![HorsiesError::new(format!(
                        "workflow builder '{}' produced a workflow without definition_key",
                        self.name,
                    ))
                    .with_code(ErrorCode::WorkflowNoDefinitionKey)]
                } else {
                    Vec::new()
                }
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
    /// Add a typed task node. Returns a `NodeRef` for wiring dependencies.
    ///
    /// Delegates to [`WorkflowSpecBuilder::task()`].
    pub fn task<T>(
        &mut self,
        node: crate::core::workflow::node::TaskNode<T>,
    ) -> crate::core::workflow::node::NodeRef {
        self.builder.task(node)
    }

    /// Add a sub-workflow node. Returns a `NodeRef` for wiring dependencies.
    ///
    /// Delegates to [`WorkflowSpecBuilder::sub_workflow()`].
    pub fn sub_workflow(
        &mut self,
        node: crate::core::workflow::sub_workflow::SubWorkflowNode,
    ) -> crate::core::workflow::node::NodeRef {
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
    pub fn success_policy(&mut self, policy: crate::core::workflow::policy::SuccessPolicy) -> &mut Self {
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
    use crate::core::task::error::OperationalErrorCode;
    use crate::core::task::fn_trait::{BlockingTaskFn, RawTaskResult, RegisteredTask, TaskMeta};
    use crate::core::task::options::{RetryPolicy, TaskOptions};
    use crate::core::workflow::node::TaskNode;

    fn valid_config() -> AppConfig {
        AppConfig {
            queue_mode: QueueMode::Default,
            custom_queues: None,
            broker: PostgresConfig {
                database_url: "postgresql://localhost/test".to_owned(),
                pool_pre_ping: true,
                pool_size: 30,
                max_overflow: 30,
                pool_timeout: 30,
                pool_recycle: 1800,
                echo: false,
            },
            cluster_wide_cap: None,
            prefetch_buffer: 0,
            claim_lease_ms: None,
            max_claim_renew_age_ms: 180_000,
            recovery: RecoveryConfig::default(),
            resilience: WorkerResilienceConfig::default(),
            schedule: None,
            resend_on_transient_err: false,
        }
    }

    fn custom_config() -> AppConfig {
        let mut config = valid_config();
        config.queue_mode = QueueMode::Custom;
        config.custom_queues = Some(vec![
            CustomQueueConfig {
                name: "fast".to_owned(),
                priority: 1,
                max_concurrency: 10,
            },
            CustomQueueConfig {
                name: "slow".to_owned(),
                priority: 50,
                max_concurrency: 5,
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
            max_concurrency: 10,
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
        builder.task(TaskNode::<()>::new("step_a"));
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
            auto_retry_for: Some(vec![TaskErrorCode::User("STORAGE_ERROR".to_owned())]),
            retry_policy: Some(RetryPolicy::fixed(vec![60, 300, 900], true).unwrap()),
        };
        app.register("retryable_task", registered_with_options(opts))
            .unwrap();

        // Register a task without retry options.
        app.register("plain_task", dummy_registered()).unwrap();

        // Build a workflow using both tasks WITHOUT .task_options() on nodes.
        let mut builder = WorkflowSpecBuilder::new("pipeline");
        let a = builder.task(TaskNode::<()>::new("retryable_task"));
        builder.task(TaskNode::<()>::new("plain_task").waits_for(a));
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
        assert!(opts_json.get("auto_retry_for").is_some());
        assert!(opts_json.get("task_name").is_none(), "task_name should be excluded");
        assert!(opts_json.get("queue_name").is_none(), "queue_name should be excluded");

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
            auto_retry_for: Some(vec![TaskErrorCode::User("STORAGE_ERROR".to_owned())]),
            retry_policy: Some(RetryPolicy::fixed(vec![60, 300, 900], true).unwrap()),
        };
        app.register("retryable_task", registered_with_options(opts))
            .unwrap();

        // Build a workflow with EXPLICIT task_options on the node (override).
        let custom_opts = r#"{"retry_policy":{"max_retries":1,"intervals":[10],"backoff_strategy":"fixed","jitter":false}}"#;
        let mut builder = WorkflowSpecBuilder::new("pipeline_explicit");
        builder.task(
            TaskNode::<()>::new("retryable_task").task_options(custom_opts),
        );
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
        b1.task(TaskNode::<()>::new("a"));
        app.register_workflow_spec(b1.build().unwrap()).unwrap();

        let mut b2 = WorkflowSpecBuilder::new("dup_wf");
        b2.task(TaskNode::<()>::new("b"));
        let result = app.register_workflow_spec(b2.build().unwrap());
        assert!(result.is_err());
    }

    // --- workflow() builder tests ---

    #[test]
    fn workflow_builder_registers_in_app() {
        let mut app = Horsies::new(valid_config()).unwrap();
        {
            let mut wb = app.workflow("pipeline");
            let a = wb.task(TaskNode::<()>::new("fetch_data"));
            wb.task(TaskNode::<()>::new("process").waits_for(a));
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
            wb.task(TaskNode::<()>::new("a"));
            wb.build_and_register().unwrap();
        }
        {
            let mut wb = app.workflow("wf_dup");
            wb.task(TaskNode::<()>::new("b"));
            let result = wb.build_and_register();
            assert!(result.is_err());
        }
    }

    #[test]
    fn registered_zero_arg_workflow_builder_runs_during_check() {
        let mut app = Horsies::new(valid_config()).unwrap();
        let call_count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let counter = Arc::clone(&call_count);

        app.workflow_builder0("build_zero", move |_app| {
            counter.fetch_add(1, Ordering::Relaxed);
            let mut builder = WorkflowSpecBuilder::new("zero_pipeline");
            builder.definition_key("tests.zero.v1");
            builder.task(TaskNode::<()>::new("step_a"));
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

        app.workflow_builder("build_param", |_app, region: &String| {
            let mut builder = WorkflowSpecBuilder::new(format!("wf_{region}"));
            builder.definition_key(format!("tests.{region}.v1"));
            builder.task(TaskNode::<()>::new("step_a"));
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
        let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
        let seen_in_builder = Arc::clone(&seen);

        let mut registration = app
            .workflow_builder("build_regional", move |_app, region: &String| {
                seen_in_builder.lock().unwrap().push(region.clone());
                let mut builder = WorkflowSpecBuilder::new(format!("wf_{region}"));
                builder.definition_key(format!("tests.{region}.v1"));
                builder.task(TaskNode::<()>::new("step_a"));
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

    #[test]
    fn workflow_builder_runs_under_send_suppression() {
        let mut app = Horsies::new(valid_config()).unwrap();
        let seen = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let seen_in_builder = Arc::clone(&seen);

        app.workflow_builder0("suppressed", move |app| {
            seen_in_builder.store(app.are_sends_suppressed(), Ordering::Relaxed);
            let mut builder = WorkflowSpecBuilder::new("suppressed_pipeline");
            builder.definition_key("tests.suppressed.v1");
            builder.task(TaskNode::<()>::new("step_a"));
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
                args: serde_json::Value::Null,
                kwargs: serde_json::Value::Null,
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
                args: serde_json::Value::Null,
                kwargs: serde_json::Value::Null,
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
                args: serde_json::Value::Null,
                kwargs: serde_json::Value::Null,
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
                args: serde_json::Value::Null,
                kwargs: serde_json::Value::Null,
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
                args: serde_json::Value::Null,
                kwargs: serde_json::Value::Null,
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
                args: serde_json::Value::Null,
                kwargs: serde_json::Value::Null,
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
                args: serde_json::Value::Null,
                kwargs: serde_json::Value::Null,
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
                args: serde_json::Value::Null,
                kwargs: serde_json::Value::Null,
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
        };

        app.register("empty_retry_codes", registered_with_options(opts))
            .unwrap();

        let err = app.check().unwrap_err();
        assert_eq!(err.code, Some(ErrorCode::TaskInvalidOptions));
        assert!(err.to_string().contains("auto_retry_for must not be empty"));
    }

    #[test]
    fn check_rejects_built_in_collision_in_auto_retry_for_user_code() {
        let mut app = Horsies::new(valid_config()).unwrap();
        let opts = TaskOptions {
            task_name: "collision_task".to_owned(),
            queue_name: Some("default".to_owned()),
            good_until: None,
            auto_retry_for: Some(vec![TaskErrorCode::User(
                OperationalErrorCode::BrokerError.to_string(),
            )]),
            retry_policy: Some(RetryPolicy::fixed(vec![60], true).unwrap()),
        };

        app.register("collision_task", registered_with_options(opts))
            .unwrap();

        let err = app.check().unwrap_err();
        assert_eq!(err.code, Some(ErrorCode::CheckReservedCodeCollision));
        assert!(err.to_string().contains("collides with reserved built-in"));
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
            auto_retry_for: Some(vec![TaskErrorCode::User("FETCH_ERROR".to_owned())]),
            retry_policy: Some(RetryPolicy::fixed(vec![60, 300], true).unwrap()),
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
}
