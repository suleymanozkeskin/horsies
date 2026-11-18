//! The app: its configuration, its task registrations, its state, and the
//! core handle that connects at the first operation.
//!
//! Every process of a horsies deployment builds the same app in `main`: a
//! producer, the worker, each worker child and the scheduler. Building it does
//! no I/O.

use std::any::{type_name, Any, TypeId};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use horsies_core::handle::CoreHandle;
use horsies_core::manifest::ProducerManifest;
use horsies_core::{ClientFamily, NamedChildStartHook, PoolProfile, RegisteredTask, TaskRegistry};

pub use horsies_core::manifest::SchemaMode;

use crate::config::{producer_manifest, rust_config_refusals};
use crate::registry::{check_queue, queue_priority, register_task, valid_queue_names, QueueSource};
use crate::send::TaskFunctions;
use crate::workflow::WorkflowRegistry;
use crate::{AppConfig, HorsiesError, TaskRuntime, ValidationReport};

/// The environment variable that suppresses sends in this process when its
/// value is `1`, with surrounding whitespace ignored (Python
/// `HORSIES_SUPPRESS_SENDS`).
pub const SUPPRESS_SENDS_ENV: &str = "HORSIES_SUPPRESS_SENDS";

/// What the process of an app does (Python's role text `producer`, `worker`,
/// `scheduler`).
///
/// A worker skips the schedule checks: it never sends a scheduled task.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AppRole {
    /// Sends tasks and reads their state. The role of a new app.
    Producer,
    /// Runs a worker.
    Worker,
    /// Runs the scheduler.
    Scheduler,
}

impl AppRole {
    /// Python's role text.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Producer => "producer",
            Self::Worker => "worker",
            Self::Scheduler => "scheduler",
        }
    }
}

/// The horsies app.
///
/// ```ignore
/// let mut app = horsies::Horsies::new(config)?;
/// send_email::register(&mut app)?;
/// app.provide(Mailer::from_env())?;
/// ```
pub struct Horsies {
    config: AppConfig,
    handle: Arc<CoreHandle>,
    tasks: TaskRegistry,
    state: Arc<AppState>,
    role: AppRole,
    sends: SendSuppression,
    functions: Arc<TaskFunctions>,
    workflows: Arc<WorkflowRegistry>,
    child_start_hooks: Vec<ChildStartFn>,
}

/// One start hook of a worker child, with its name, called once in the
/// child.
pub(crate) type ChildStartFn = NamedChildStartHook;

/// What child mode takes from the app: the tasks, the start hooks in
/// registration order, and the producer manifest of the app's handle.
pub(crate) struct ChildParts {
    pub(crate) tasks: TaskRegistry,
    pub(crate) start_hooks: Vec<ChildStartFn>,
    pub(crate) producer_manifest: ProducerManifest,
}

impl Horsies {
    /// An app whose handle installs the schema at its first operation
    /// ([`SchemaMode::MigrateAndValidate`], Python's default).
    ///
    /// Validates `config` and builds the producer manifest. No I/O: the
    /// handle connects at the first operation. Refuses an invalid config
    /// with every refusal of the validation in one error.
    pub fn new(config: AppConfig) -> Result<Self, HorsiesError> {
        Self::with_schema_mode(config, SchemaMode::MigrateAndValidate)
    }

    /// An app whose handle has the schema mode `mode`. `ObserveOnly` and
    /// `AssumeInitialized` never run schema DDL (monitoring tools); the
    /// second also uses no listener.
    pub fn with_schema_mode(config: AppConfig, mode: SchemaMode) -> Result<Self, HorsiesError> {
        validate_config(&config)?;
        let manifest = producer_manifest(&config.broker, mode)
            .map_err(|refused| refused.to_horsies_error())?;
        let handle = CoreHandle::new(manifest, PoolProfile::Producer, ClientFamily::Rust);
        Ok(Self::from_parts(config, Arc::new(handle)))
    }

    /// An app that uses `handle`, which another app or the caller shares.
    /// Validates `config`; the schema mode and the connection settings are
    /// those that `handle` was built with.
    pub fn with_handle(config: AppConfig, handle: Arc<CoreHandle>) -> Result<Self, HorsiesError> {
        validate_config(&config)?;
        Ok(Self::from_parts(config, handle))
    }

    fn from_parts(config: AppConfig, handle: Arc<CoreHandle>) -> Self {
        Self {
            workflows: Arc::new(WorkflowRegistry::new(config.clone())),
            config,
            handle,
            tasks: TaskRegistry::new(),
            state: Arc::new(AppState::new()),
            role: AppRole::Producer,
            sends: SendSuppression::new(),
            functions: Arc::new(TaskFunctions::new()),
            child_start_hooks: Vec::new(),
        }
    }

    /// The validated configuration.
    pub fn config(&self) -> &AppConfig {
        &self.config
    }

    /// The role of this process.
    pub fn role(&self) -> AppRole {
        self.role
    }

    /// Set the role of this process and log it. The CLI runner sets
    /// `Worker` or `Scheduler` before it starts the service.
    pub fn set_role(&mut self, role: AppRole) {
        self.role = role;
        tracing::info!(role = role.as_str(), "horsies running as {}", role.as_str());
    }

    /// Suppress the sends of this process (`true`) or stop suppressing them
    /// (`false`). A suppressed send returns its suppression outcome and
    /// writes nothing.
    pub fn suppress_sends(&self, value: bool) {
        self.sends.set(value);
    }

    /// Whether the sends of this process are suppressed: by
    /// [`Self::suppress_sends`], or by [`SUPPRESS_SENDS_ENV`] set to `1`. The
    /// variable is read at each call, so a change takes effect at the next
    /// send.
    pub fn are_sends_suppressed(&self) -> bool {
        self.sends.is_active()
    }

    /// The queue names that the app admits: `default` in `Default` mode, the
    /// configured queues in `Custom` mode.
    pub fn get_valid_queue_names(&self) -> Vec<String> {
        valid_queue_names(&self.config)
    }

    /// Whether the app admits `queue_name` as the queue of a send. Refuses
    /// with `TaskInvalidQueue` and the valid names.
    pub fn validate_queue(&self, queue_name: &str) -> Result<(), HorsiesError> {
        check_queue(&self.config, queue_name)
    }

    /// The priority of a send to `queue_name`: `task_priority` when given,
    /// else the configured priority of the queue, else 100.
    pub fn effective_priority(&self, queue_name: &str, task_priority: Option<u32>) -> u32 {
        task_priority.unwrap_or_else(|| queue_priority(&self.config, queue_name))
    }

    /// Register `task` as `name`, with the queue that its options name.
    /// `async_task_fn!` and `blocking_task_fn!` build the task; the
    /// `#[horsies::task]` attribute generates a `register` function that
    /// calls this.
    ///
    /// Refuses an empty or repeated name, a definition-time `good_until`, and
    /// a queue that the queue mode does not admit. After a refusal nothing is
    /// registered.
    pub fn register(
        &mut self,
        name: impl Into<String>,
        task: RegisteredTask,
    ) -> Result<(), HorsiesError> {
        register_task(
            &mut self.tasks,
            &self.config,
            name.into(),
            task,
            QueueSource::TaskOptions,
        )
    }

    /// Register `task` as `name` on `queue`, in place of the queue that its
    /// options name. The refusals are those of [`Self::register`].
    pub fn register_with_queue(
        &mut self,
        name: impl Into<String>,
        task: RegisteredTask,
        queue: &str,
    ) -> Result<(), HorsiesError> {
        register_task(
            &mut self.tasks,
            &self.config,
            name.into(),
            task,
            QueueSource::Named(queue),
        )
    }

    /// Provide a value of type `T` to task bodies, which read it with
    /// `TaskRuntime::state::<T>()`.
    ///
    /// Each process builds its own value: every worker child runs `main`
    /// again. Refuses a second value of the same type (wrap it in a newtype)
    /// and a value provided after a task body read the state.
    pub fn provide<T>(&mut self, value: T) -> Result<(), HorsiesError>
    where
        T: Send + Sync + 'static,
    {
        self.state
            .provide(value)
            .map_err(|refused| HorsiesError::new(refused.to_string()))
    }

    /// Run each registrar on the app, in order, and stop at the first
    /// refusal. A registrar registers tasks and workflows of one module.
    pub fn discover<I, F>(&mut self, registrars: I) -> Result<(), HorsiesError>
    where
        I: IntoIterator<Item = F>,
        F: Fn(&mut Self) -> Result<(), HorsiesError>,
    {
        registrars
            .into_iter()
            .try_for_each(|registrar| registrar(self))
    }

    /// The runtime that task bodies of this app take as their first
    /// parameter: it reads the app's state. The `#[horsies::task]` attribute
    /// calls it when the task registers.
    pub fn task_runtime(&self) -> TaskRuntime {
        TaskRuntime::new(
            Arc::clone(self.state()),
            Arc::clone(&self.functions),
            crate::workflow::WorkflowStarter::of(self),
        )
    }

    /// The core handle of the app.
    pub(crate) fn handle(&self) -> &Arc<CoreHandle> {
        &self.handle
    }

    /// The registered tasks.
    pub(crate) fn tasks(&self) -> &TaskRegistry {
        &self.tasks
    }

    /// The state that task bodies read.
    pub(crate) fn state(&self) -> &Arc<AppState> {
        &self.state
    }

    /// The typed functions that the task attributes registered on this app.
    pub(crate) fn functions(&self) -> &TaskFunctions {
        &self.functions
    }

    /// The workflow definitions registered on this app.
    pub(crate) fn workflows(&self) -> &Arc<WorkflowRegistry> {
        &self.workflows
    }

    /// The findings of the workflow check (Python's check of the registered
    /// workflows): each definition without params and each check case of a
    /// workflow builder, built, checked against the registered definitions
    /// and the app's queues, and walked into its sub-workflows; a nesting
    /// is reported only when it reaches the depth limit for certain. Empty
    /// when every workflow passes.
    ///
    /// The sends of this process are suppressed while the check runs, so a
    /// check case that sends writes nothing (Python
    /// `_check_workflow_builders`). The flag is then set back to its value
    /// before the check; a set [`SUPPRESS_SENDS_ENV`] does not change it.
    pub(crate) fn workflow_check_findings(&self) -> Vec<HorsiesError> {
        let before = self.sends.flag.load(Ordering::Relaxed);
        self.sends.set(true);
        let findings = self.workflows.check_findings();
        self.sends.set(before);
        findings
    }

    /// The suppression of this process's sends, for a send path that lives
    /// apart from the app.
    pub(crate) fn send_suppression(&self) -> SendSuppression {
        self.sends.clone()
    }

    /// Keep `hook` as the last start hook of each worker child.
    pub(crate) fn add_child_start_hook(&mut self, hook: ChildStartFn) {
        self.child_start_hooks.push(hook);
    }

    /// The number of child start hooks.
    #[cfg(test)]
    pub(crate) fn child_start_hook_count(&self) -> usize {
        self.child_start_hooks.len()
    }

    /// The parts of the app that child mode serves from. Consumes the app:
    /// a child process serves until its end.
    pub(crate) fn into_child_parts(self) -> ChildParts {
        ChildParts {
            producer_manifest: self.handle.manifest().clone(),
            tasks: self.tasks,
            start_hooks: self.child_start_hooks,
        }
    }
}

impl std::fmt::Debug for Horsies {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Horsies")
            .field("role", &self.role)
            .field("queues", &self.get_valid_queue_names())
            .field("tasks", &self.tasks.len())
            .field("workflows", &self.workflows)
            .field("child_start_hooks", &self.child_start_hooks.len())
            .finish()
    }
}

/// Validate `config` with the core's rules and the Rust rules; every refusal
/// in one error (`ValidationReport::into_result`).
fn validate_config(config: &AppConfig) -> Result<(), HorsiesError> {
    let mut report = ValidationReport::new("config");
    for refusal in config.validate() {
        report.add(refusal.to_horsies_error());
    }
    for refusal in rust_config_refusals(config) {
        report.add(refusal);
    }
    report.into_result()
}

/// Whether the sends of this process are suppressed. Clones share one flag.
#[derive(Debug, Clone)]
pub(crate) struct SendSuppression {
    flag: Arc<AtomicBool>,
}

impl SendSuppression {
    fn new() -> Self {
        Self {
            flag: Arc::new(AtomicBool::new(false)),
        }
    }

    fn set(&self, value: bool) {
        self.flag.store(value, Ordering::Relaxed);
    }

    /// The flag is set, or [`SUPPRESS_SENDS_ENV`] is `1`. Reads the
    /// environment at each call.
    pub(crate) fn is_active(&self) -> bool {
        self.flag.load(Ordering::Relaxed)
            || suppression_requested(std::env::var(SUPPRESS_SENDS_ENV).ok().as_deref())
    }
}

/// Whether a value of [`SUPPRESS_SENDS_ENV`] asks for suppression: `1`, with
/// surrounding whitespace ignored. No value, a value that is not UTF-8, and
/// any other value do not.
fn suppression_requested(value: Option<&str>) -> bool {
    value.map(str::trim) == Some("1")
}

type StateValue = Arc<dyn Any + Send + Sync>;

/// The values that the app provides to task bodies, one per type.
///
/// The set is open until the first read; the first read freezes it, so every
/// body of a process sees the same set.
#[derive(Debug)]
pub(crate) struct AppState {
    stage: Mutex<StateStage>,
}

#[derive(Debug)]
enum StateStage {
    /// `provide` adds values.
    Open(HashMap<TypeId, StateValue>),
    /// A body read the state; the set no longer changes.
    Frozen(Arc<HashMap<TypeId, StateValue>>),
}

/// Why `provide` did not add a value.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub(crate) enum StateRefused {
    #[error(
        "app state of type {type_name} is already provided; wrap the second value in a newtype"
    )]
    AlreadyProvided { type_name: &'static str },
    #[error(
        "app state of type {type_name} is provided after a task body read the state; provide \
         every value before the app runs"
    )]
    AfterFirstRead { type_name: &'static str },
}

/// The app provides no value of the asked type.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("the app provides no state of type {type_name}; call app.provide(value) in main")]
pub struct StateNotProvided {
    /// The type that was asked for.
    pub type_name: &'static str,
}

impl AppState {
    fn new() -> Self {
        Self {
            stage: Mutex::new(StateStage::Open(HashMap::new())),
        }
    }

    fn provide<T>(&self, value: T) -> Result<(), StateRefused>
    where
        T: Send + Sync + 'static,
    {
        let mut stage = self.lock();
        match &mut *stage {
            StateStage::Frozen(_) => Err(StateRefused::AfterFirstRead {
                type_name: type_name::<T>(),
            }),
            StateStage::Open(values) => match values.contains_key(&TypeId::of::<T>()) {
                true => Err(StateRefused::AlreadyProvided {
                    type_name: type_name::<T>(),
                }),
                false => {
                    let _previous = values.insert(TypeId::of::<T>(), Arc::new(value));
                    Ok(())
                }
            },
        }
    }

    /// The value of type `T`. The first read freezes the set.
    pub(crate) fn get<T>(&self) -> Result<Arc<T>, StateNotProvided>
    where
        T: Send + Sync + 'static,
    {
        let value = self.frozen().get(&TypeId::of::<T>()).cloned();
        match value {
            None => Err(StateNotProvided {
                type_name: type_name::<T>(),
            }),
            Some(value) => Ok(Arc::downcast::<T>(value)
                .unwrap_or_else(|_| unreachable!("app state is keyed by the TypeId of its value"))),
        }
    }

    fn frozen(&self) -> Arc<HashMap<TypeId, StateValue>> {
        let mut stage = self.lock();
        let values = match &mut *stage {
            StateStage::Frozen(values) => return Arc::clone(values),
            StateStage::Open(values) => Arc::new(std::mem::take(values)),
        };
        *stage = StateStage::Frozen(Arc::clone(&values));
        values
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, StateStage> {
        // No code that holds the lock can panic: it inserts into or moves a
        // map. A poisoned lock is a defect.
        self.stage
            .lock()
            .expect("the app state lock is never held across a panic")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ChildPreload, ErrorCode, PostgresConfig, WorkerProfile};

    fn config() -> AppConfig {
        AppConfig::for_database_url("postgresql://localhost/horsies")
    }

    #[derive(Debug, PartialEq)]
    struct Settings(u32);

    #[test]
    fn a_new_app_is_a_producer_with_no_task() {
        let app = Horsies::new(config()).unwrap();
        assert_eq!(app.role(), AppRole::Producer);
        assert!(app.tasks().is_empty());
        assert_eq!(app.get_valid_queue_names(), vec!["default"]);
    }

    #[test]
    fn an_invalid_config_is_refused_with_every_refusal() {
        let mut invalid = AppConfig {
            broker: PostgresConfig::from_url("mysql://localhost/horsies"),
            ..config()
        };
        invalid.worker_profiles.insert(
            "web".to_owned(),
            WorkerProfile {
                preload: ChildPreload::App,
                ..WorkerProfile::default()
            },
        );
        let refused = Horsies::new(invalid).unwrap_err();
        // Two or more refusals are folded into one error with a note each.
        assert!(refused.notes.len() >= 2, "{refused}");
        assert!(refused.notes.iter().any(|note| note.contains("preload")));
    }

    #[test]
    fn a_pool_size_outside_the_manifest_bounds_refuses_the_app() {
        let invalid = AppConfig {
            broker: PostgresConfig {
                pool_size: 0,
                ..PostgresConfig::from_url("postgresql://localhost/horsies")
            },
            ..config()
        };
        let refused = Horsies::new(invalid).unwrap_err();
        assert!(refused.message.contains("broker.pool_size"), "{refused}");
    }

    #[test]
    fn with_handle_shares_one_handle() {
        let first = Horsies::new(config()).unwrap();
        let second = Horsies::with_handle(config(), Arc::clone(first.handle())).unwrap();
        assert!(Arc::ptr_eq(first.handle(), second.handle()));
    }

    #[test]
    fn set_role_changes_the_role() {
        let mut app = Horsies::new(config()).unwrap();
        app.set_role(AppRole::Worker);
        assert_eq!(app.role(), AppRole::Worker);
        assert_eq!(AppRole::Scheduler.as_str(), "scheduler");
    }

    #[test]
    fn suppress_sends_reaches_every_clone_of_the_suppression() {
        let app = Horsies::new(config()).unwrap();
        let suppression = app.send_suppression();
        app.suppress_sends(true);
        assert!(suppression.is_active());
        app.suppress_sends(false);
        assert!(!suppression.flag.load(Ordering::Relaxed));
    }

    #[test]
    fn only_the_value_1_requests_suppression() {
        for value in ["1", " 1 ", "1\n", " 1\n"] {
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
    fn effective_priority_prefers_the_task_priority() {
        let app = Horsies::new(config()).unwrap();
        assert_eq!(app.effective_priority("default", Some(7)), 7);
        assert_eq!(app.effective_priority("default", None), 100);
    }

    #[test]
    fn provided_state_is_read_by_type() {
        let mut app = Horsies::new(config()).unwrap();
        app.provide(Settings(3)).unwrap();
        assert_eq!(*app.state().get::<Settings>().unwrap(), Settings(3));
    }

    #[test]
    fn a_second_value_of_one_type_is_refused() {
        let mut app = Horsies::new(config()).unwrap();
        app.provide(Settings(1)).unwrap();
        let refused = app.provide(Settings(2)).unwrap_err();
        assert!(refused.message.contains("already provided"));
        assert_eq!(*app.state().get::<Settings>().unwrap(), Settings(1));
    }

    #[test]
    fn a_value_after_the_first_read_is_refused() {
        let mut app = Horsies::new(config()).unwrap();
        let missing = app.state().get::<Settings>().unwrap_err();
        assert!(missing.type_name.contains("Settings"));
        let refused = app.provide(Settings(1)).unwrap_err();
        assert!(refused.message.contains("after a task body read"));
    }

    #[test]
    fn discover_stops_at_the_first_refusal() {
        let mut app = Horsies::new(config()).unwrap();
        let calls = std::cell::Cell::new(0);
        let registrar = |app: &mut Horsies| {
            calls.set(calls.get() + 1);
            match calls.get() {
                2 => Err(HorsiesError::new("refused").with_code(ErrorCode::TaskInvalidOptions)),
                _ => app.provide(calls.get()),
            }
        };
        let refused = app
            .discover([&registrar, &registrar, &registrar])
            .unwrap_err();
        assert_eq!(refused.code, Some(ErrorCode::TaskInvalidOptions));
        assert_eq!(calls.get(), 2);
    }
}
