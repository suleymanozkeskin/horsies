#![cfg_attr(test, allow(clippy::unwrap_used))]

extern crate self as horsies;

// Internal modules (merged from formerly separate crates).
// `core` is `pub` (rather than `pub(crate)`) because `#[macro_export]` macros
// in `core::task::macros` use `$crate::core::…` paths that must be
// resolvable when the macro is expanded in downstream crates.
pub(crate) mod broker;
#[doc(hidden)]
pub mod core;
pub(crate) mod worker;
pub(crate) mod workflow_engine;

// Facade-level modules.
mod error;
mod lazy_broker;
mod runtime;
mod task;
mod workflow;

use std::sync::Arc;
use std::sync::RwLock;

use serde::de::DeserializeOwned;
use serde::Serialize;
use tokio_util::sync::CancellationToken;

use crate::core::{Horsies as CoreHorsies, RegisteredWorkflowSpec};

// ---------------------------------------------------------------------------
// Public re-exports
// ---------------------------------------------------------------------------

pub use error::{AppError, AppResult};
pub use runtime::TaskRuntime;
pub use task::{TaskFunction, TaskRegistrationBuilder};
pub use workflow::{
    WorkflowBuilderRegistration, WorkflowFunction, WorkflowRegistrationBuilder, WorkflowStarter,
    WorkflowTemplate,
};

// horsies-macros
pub use horsies_macros::{blocking_task, task};

// core re-exports
pub use crate::core::RegisteredTask;
pub use crate::core::{
    mask_database_url, resolve_node_task_options, AnyNode, AppConfigError, BackoffStrategy,
    BuiltInTaskCode, ContractCode, CustomQueueConfig, CustomQueueConfigError, DailySchedule,
    ErrorCategory, ErrorCode, HandleErrorCode, HandleOperationError, HandleResult, HourlySchedule,
    IntervalSchedule, JoinType, MonthlySchedule, NodeKey, NodeRef, OnError, OperationalErrorCode,
    OutcomeCode, PostgresConfig, PostgresConfigError, QueueMode, RecoveryConfig,
    RecoveryConfigError, RegisteredWorkflowSpec as CoreRegisteredWorkflowSpec,
    ResilienceConfigError, ResolvedEnqueue, RetrievalCode, RetryPolicy, RetryPolicyError,
    ScheduleConfig, SchedulePattern, SpecBuilderFn, SubWorkflowError, SubWorkflowNode,
    SubWorkflowSummary, SuccessCase, SuccessPolicy, TaskAttemptInfo, TaskAttemptOutcome, TaskError,
    TaskErrorCode, TaskInfo, TaskNode, TaskOptions, TaskRegistry, TaskResult, TaskSchedule,
    TaskSendError, TaskSendErrorCode, TaskSendPayload, TaskSendResult, TaskStatus,
    ValidationReport, Weekday, WeeklySchedule, WorkerResilienceConfig, WorkflowContext,
    WorkflowDefConfig, WorkflowDefinition, WorkflowMeta, WorkflowSpec, WorkflowSpecBuilder,
    WorkflowSpecRegistry, WorkflowStartError, WorkflowStartErrorCode, WorkflowStartResult,
    WorkflowStatus, WorkflowTaskStatus, TASK_TERMINAL_STATES, WF_TASK_TERMINAL_VALUES,
    WORKFLOW_TASK_TERMINAL_STATES, WORKFLOW_TERMINAL_STATES,
};
pub use crate::core::{AppConfig, HorsiesError};

// broker re-exports
pub use crate::broker::{
    compute_enqueue_sha, BrokerError, BrokerErrorCode, BrokerOperationError, BrokerResult,
    ClaimedTaskRow, ExpiredTaskRow, HeartbeatRow, NotifyListener, PostgresBroker,
    SharedNotifyListener, StaleTaskRow, TaskAttemptRow, TaskHandle, TaskInfoRow, TaskResultRow,
    TaskRunningContextRow, WorkerStateRow, WorkerStatsRow, WorkflowRow, WorkflowTaskRow,
};
/// Alias for [`PostgresBroker`].
pub type Broker = PostgresBroker;

// worker re-exports
pub use crate::worker::{
    cli::{init_tracing, LogLevel},
    scheduler::service::spawn_scheduler,
    Worker, WorkerConfig, WorkerError,
};

// workflow engine re-exports
#[deprecated(
    since = "0.1.0-alpha.2",
    note = "advanced plumbing only; prefer WorkflowFunction::start(), WorkflowTemplate::start(...), app.start(spec), or WorkflowStarter"
)]
pub use crate::workflow_engine::start_workflow;
#[deprecated(
    since = "0.1.0-alpha.2",
    note = "advanced plumbing only; prefer WorkflowFunction::start(), WorkflowTemplate::start(...), app.start(spec), or WorkflowStarter"
)]
pub use crate::workflow_engine::BoundWorkflowSpec;
pub use crate::workflow_engine::{
    cancel_workflow, get_workflow_result, on_workflow_task_complete, pause_workflow,
    recover_stuck_workflows, resume_workflow, WorkflowError, WorkflowHandle, WorkflowSpecExt,
    WorkflowTaskInfo,
};

use crate::lazy_broker::LazyBroker;

pub struct Horsies {
    pub(crate) core: CoreHorsies,
    pub(crate) broker: Arc<LazyBroker>,
    pub(crate) workflow_builders: Vec<Box<dyn workflow::WorkflowBuilderCheck>>,
    pub(crate) workflow_registry_cache: Arc<RwLock<WorkflowSpecRegistry>>,
}

impl Horsies {
    pub fn new(config: AppConfig) -> Result<Self, HorsiesError> {
        let broker_config = config.broker.clone();
        let core = CoreHorsies::new(config)?;
        Ok(Self {
            workflow_registry_cache: Arc::new(RwLock::new(core.workflow_registry().clone())),
            core,
            broker: Arc::new(LazyBroker::new(broker_config)),
            workflow_builders: Vec::new(),
        })
    }

    pub fn from_core(core: CoreHorsies) -> Self {
        let broker_config = core.config().broker.clone();
        Self {
            workflow_registry_cache: Arc::new(RwLock::new(core.workflow_registry().clone())),
            core,
            broker: Arc::new(LazyBroker::new(broker_config)),
            workflow_builders: Vec::new(),
        }
    }

    pub fn with_broker(
        config: AppConfig,
        broker: Arc<PostgresBroker>,
    ) -> Result<Self, HorsiesError> {
        let app = Self::new(config)?;
        let _ = app.bind_broker(broker);
        Ok(app)
    }

    pub fn bind_broker(&self, broker: Arc<PostgresBroker>) -> Result<(), Arc<PostgresBroker>> {
        self.broker.set(broker)
    }

    pub fn broker_if_initialized(&self) -> Option<Arc<PostgresBroker>> {
        self.broker.get_if_initialized()
    }

    pub async fn get_broker(&self) -> AppResult<Arc<PostgresBroker>> {
        Ok(self.broker.get().await?)
    }

    pub fn config(&self) -> &AppConfig {
        self.core.config()
    }

    pub fn role(&self) -> &str {
        self.core.role()
    }

    pub fn set_role(&mut self, role: impl Into<String>) {
        self.core.set_role(role);
    }

    pub(crate) fn suppress_sends(&self, suppress: bool) {
        self.core.suppress_sends(suppress);
    }

    pub(crate) fn are_sends_suppressed(&self) -> bool {
        self.core.are_sends_suppressed()
    }

    pub fn registry(&self) -> &crate::core::TaskRegistry {
        self.core.registry()
    }

    pub fn workflow_registry(&self) -> &crate::core::WorkflowSpecRegistry {
        self.core.workflow_registry()
    }

    pub fn get_valid_queue_names(&self) -> Vec<String> {
        self.core.get_valid_queue_names()
    }

    pub fn validate_queue(&self, queue_name: &str) -> Result<(), HorsiesError> {
        self.core.validate_queue(queue_name)
    }

    pub fn validate_schedules(&self) -> Result<(), HorsiesError> {
        self.core.validate_schedules()
    }

    pub fn effective_priority(&self, queue_name: &str, task_priority: Option<u32>) -> u32 {
        self.core.effective_priority(queue_name, task_priority)
    }

    pub fn resolve_enqueue(
        &self,
        task_name: &str,
        queue_name: Option<&str>,
        priority: Option<u32>,
    ) -> Result<crate::core::ResolvedEnqueue, HorsiesError> {
        self.core.resolve_enqueue(task_name, queue_name, priority)
    }

    pub fn register(
        &mut self,
        name: impl Into<String>,
        task: RegisteredTask,
    ) -> Result<(), HorsiesError> {
        self.core.register(name, task)
    }

    pub fn register_with_queue(
        &mut self,
        name: impl Into<String>,
        task: RegisteredTask,
        queue_name: &str,
    ) -> Result<(), HorsiesError> {
        self.core.register_with_queue(name, task, queue_name)
    }

    pub fn task<A: Serialize, T: DeserializeOwned + Clone>(
        &mut self,
        name: &str,
        task: RegisteredTask,
    ) -> Result<TaskRegistrationBuilder<'_, A, T>, HorsiesError> {
        if name.trim().is_empty() {
            return Err(HorsiesError::new("task name cannot be empty"));
        }
        Ok(TaskRegistrationBuilder::new(self, name.to_owned(), task))
    }

    pub fn workflow<T: DeserializeOwned + Clone>(
        &mut self,
        name: &str,
    ) -> WorkflowRegistrationBuilder<'_, T> {
        WorkflowRegistrationBuilder::new(self, name)
    }

    pub fn register_workflow_spec<T: DeserializeOwned + Clone>(
        &mut self,
        spec: WorkflowSpec,
    ) -> Result<WorkflowFunction<T>, HorsiesError> {
        self.core.register_workflow_spec(spec.clone())?;
        self.refresh_workflow_registry_cache();
        Ok(WorkflowFunction::new(
            spec,
            Arc::clone(&self.broker),
            Arc::clone(&self.workflow_registry_cache),
            self.core.config().resend_on_transient_err,
        ))
    }

    /// Start a workflow from a runtime-built [`WorkflowSpec`].
    ///
    /// Registers the spec with the app and starts it in one step.
    /// This is the primary path for dynamic/parameterized workflows
    /// where the DAG is built at runtime.
    ///
    /// ```ignore
    /// let spec = WorkflowSpecBuilder::new("pipeline")
    ///     .task(node_a)
    ///     .task(node_b)
    ///     .definition_key("myapp.pipeline.v1")
    ///     .build()?;
    ///
    /// let handle = app.start::<MyOutput>(spec).await?;
    /// let result = handle.get(None).await?;
    /// ```
    pub async fn start<T: DeserializeOwned + Clone>(
        &mut self,
        spec: WorkflowSpec,
    ) -> WorkflowStartResult<WorkflowHandle<T>> {
        let workflow_name = spec.name.clone();
        let wf = self
            .register_workflow_spec::<T>(spec)
            .map_err(|err| WorkflowStartError {
                code: WorkflowStartErrorCode::ValidationFailed,
                message: err.to_string(),
                retryable: false,
                workflow_name,
                workflow_id: String::new(),
            })?;
        wf.start().await
    }

    /// Create a [`WorkflowStarter`] for starting workflows after the app is consumed.
    ///
    /// Call this before [`run_worker_with`](Self::run_worker_with) so that tasks
    /// running inside the worker can start dynamic workflows at runtime.
    ///
    /// ```ignore
    /// let starter = app.workflow_starter();
    /// app.run_worker_with(config).await?;
    ///
    /// // Inside a task (via global or dependency injection):
    /// let handle = starter.start::<T>(spec).await?;
    /// ```
    pub fn workflow_starter(&self) -> WorkflowStarter {
        WorkflowStarter::new(
            Arc::clone(&self.broker),
            Arc::clone(&self.workflow_registry_cache),
            self.core.config().resend_on_transient_err,
        )
    }

    /// Create a [`TaskRuntime`] for task-time dynamic workflow starts.
    ///
    /// The generated `#[horsies::task]` wrappers capture this automatically
    /// when a task signature includes `TaskRuntime`.
    pub fn task_runtime(&self) -> TaskRuntime {
        TaskRuntime::new(self.workflow_starter())
    }

    pub fn register_workflow<T: DeserializeOwned + Clone>(
        &mut self,
        registered: RegisteredWorkflowSpec,
    ) -> Result<WorkflowFunction<T>, HorsiesError> {
        let spec = registered.spec.clone();
        self.core.register_workflow(registered)?;
        self.refresh_workflow_registry_cache();
        Ok(WorkflowFunction::new(
            spec,
            Arc::clone(&self.broker),
            Arc::clone(&self.workflow_registry_cache),
            self.core.config().resend_on_transient_err,
        ))
    }

    pub fn register_workflow_definition<D>(
        &mut self,
    ) -> Result<WorkflowFunction<D::Output>, HorsiesError>
    where
        D: WorkflowDefinition<Params = ()>,
        D::Output: DeserializeOwned + Clone,
    {
        let registered = D::build_registered()?;
        self.register_workflow::<D::Output>(registered)
    }

    pub fn workflow_template<D>(&self) -> WorkflowTemplate<D::Params, D::Output>
    where
        D: WorkflowDefinition + 'static,
        D::Output: DeserializeOwned + Clone,
    {
        WorkflowTemplate::from_definition::<D>(self.workflow_starter())
    }

    #[deprecated(
        since = "0.1.0-alpha.2",
        note = "advanced validation-only builder registration; prefer WorkflowDefinition for reusable workflows and workflow_template(...) for parameterized reusable workflows"
    )]
    pub fn workflow_builder<P, F>(
        &mut self,
        name: &str,
        builder: F,
    ) -> Result<workflow::WorkflowBuilderRegistration<'_, P>, HorsiesError>
    where
        P: Send + Sync + 'static,
        F: Fn(&Self, &P) -> Result<WorkflowSpec, HorsiesError> + Send + Sync + 'static,
    {
        if name.trim().is_empty() {
            return Err(HorsiesError::new("workflow builder name cannot be empty")
                .with_code(ErrorCode::WorkflowNoName));
        }
        Ok(workflow::WorkflowBuilderRegistration::new(
            self,
            name.to_owned(),
            Arc::new(builder),
        ))
    }

    #[deprecated(
        since = "0.1.0-alpha.2",
        note = "advanced validation-only builder registration; prefer WorkflowDefinition for reusable workflows"
    )]
    pub fn workflow_builder0<F>(
        &mut self,
        name: &str,
        builder: F,
    ) -> Result<workflow::WorkflowBuilderRegistration<'_, ()>, HorsiesError>
    where
        F: Fn(&Self) -> Result<WorkflowSpec, HorsiesError> + Send + Sync + 'static,
    {
        if name.trim().is_empty() {
            return Err(HorsiesError::new("workflow builder name cannot be empty")
                .with_code(ErrorCode::WorkflowNoName));
        }
        Ok(workflow::WorkflowBuilderRegistration::new_zero_arg(
            self,
            name.to_owned(),
            Arc::new(builder),
        ))
    }

    pub fn check(&self) -> Result<(), HorsiesError> {
        self.core.check()?;
        self.check_workflow_builders()
    }

    pub async fn check_live(&self) -> AppResult<()> {
        self.check()?;
        let broker = self.broker.get().await?;
        broker.health_check().await?;
        Ok(())
    }

    pub async fn check_with(&self, live: bool) -> AppResult<()> {
        if live {
            self.check_live().await
        } else {
            self.check()?;
            Ok(())
        }
    }

    pub fn discover<I, F>(&mut self, registrars: I) -> Result<(), HorsiesError>
    where
        I: IntoIterator<Item = F>,
        F: Fn(&mut Self) -> Result<(), HorsiesError>,
    {
        for registrar in registrars {
            registrar(self)?;
        }
        Ok(())
    }

    pub async fn run_worker(self) -> AppResult<()> {
        self.run_worker_with(WorkerConfig::default()).await
    }

    pub async fn run_worker_with(self, mut worker_config: WorkerConfig) -> AppResult<()> {
        let Self { core, broker, .. } = self;
        let broker = broker.get().await?;
        let (app_config, registry, workflow_registry) = core.into_parts();
        worker_config.apply_queue_config(&app_config);
        worker_config
            .validate()
            .map_err(AppError::InvalidWorkerConfig)?;
        let worker = Worker::new(
            broker,
            Arc::new(registry),
            Arc::new(workflow_registry),
            app_config,
            worker_config,
        )?;
        worker.run_with_signals().await?;
        Ok(())
    }

    pub async fn run_scheduler(self) -> AppResult<()> {
        let Self { core, broker, .. } = self;
        let app_config = core.config().clone();
        let schedule_config = app_config.schedule.clone().ok_or_else(|| {
            AppError::SchedulerConfig("schedule config is not enabled".to_owned())
        })?;
        let broker = broker.get().await?;
        let cancel = CancellationToken::new();

        {
            let cancel = cancel.clone();
            tokio::spawn(async move {
                tokio::signal::ctrl_c().await.ok();
                cancel.cancel();
            });
        }

        #[cfg(unix)]
        {
            let cancel = cancel.clone();
            tokio::spawn(async move {
                use tokio::signal::unix::{signal, SignalKind};
                match signal(SignalKind::terminate()) {
                    Ok(mut sig) => {
                        sig.recv().await;
                        cancel.cancel();
                    }
                    Err(err) => {
                        tracing::warn!(error = %err, "failed to register SIGTERM handler");
                    }
                }
            });
        }

        let join = crate::worker::scheduler::service::spawn_scheduler(
            broker,
            schedule_config,
            app_config,
            cancel,
        );
        join.await?;
        Ok(())
    }

    pub fn into_core(self) -> CoreHorsies {
        self.core
    }

    /// Advanced decomposition API.
    ///
    /// This exposes broker and registry plumbing directly. Prefer
    /// `WorkflowFunction`, `WorkflowTemplate`, `app.start(spec)`, or
    /// `WorkflowStarter` unless you specifically need low-level control.
    pub async fn into_parts(
        self,
    ) -> AppResult<(
        AppConfig,
        crate::core::TaskRegistry,
        crate::core::WorkflowSpecRegistry,
        Arc<PostgresBroker>,
    )> {
        let Self { core, broker, .. } = self;
        let broker = broker.get().await?;
        let (config, registry, workflow_registry) = core.into_parts();
        Ok((config, registry, workflow_registry, broker))
    }
}

impl std::fmt::Debug for Horsies {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Horsies")
            .field("role", &self.role())
            .field("queues", &self.get_valid_queue_names())
            .field("workflow_builder_count", &self.workflow_builders.len())
            .finish()
    }
}

impl Horsies {
    fn refresh_workflow_registry_cache(&self) {
        *self
            .workflow_registry_cache
            .write()
            .expect("workflow registry lock poisoned") = self.core.workflow_registry().clone();
    }

    fn check_workflow_builders(&self) -> Result<(), HorsiesError> {
        let previous = self.are_sends_suppressed();
        self.suppress_sends(true);

        let mut report = ValidationReport::new("workflow_builder_check");
        for builder in &self.workflow_builders {
            for error in builder.run_check(self) {
                report.add(error);
            }
        }

        self.suppress_sends(previous);
        report.into_result()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};
    use sqlx::postgres::PgPoolOptions;

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

    #[test]
    fn discover_runs_registrars() {
        fn register(app: &mut Horsies) -> Result<(), HorsiesError> {
            app.set_role("discovered");
            Ok(())
        }

        let mut app = Horsies::new(valid_config()).unwrap();
        app.discover([register]).unwrap();
        assert_eq!(app.role(), "discovered");
    }

    #[tokio::test]
    async fn check_live_surfaces_connectivity_failure() {
        let app = Horsies::new(valid_config()).unwrap();
        let pool = PgPoolOptions::new()
            .connect_lazy("postgresql://postgres@127.0.0.1:1/test")
            .unwrap();
        assert!(app
            .bind_broker(Arc::new(PostgresBroker::from_pool(pool)))
            .is_ok());

        let err = app.check_live().await.unwrap_err();
        match err {
            AppError::Broker(_) => {}
            other => panic!("unexpected error: {}", other),
        }
    }

    #[derive(Serialize, Deserialize)]
    struct DummyArgs;

    async fn dummy_task(_: DummyArgs) -> Result<(), horsies::TaskError> {
        Ok(())
    }

    #[tokio::test]
    async fn check_live_surfaces_core_check_failure_before_ping() {
        let mut app = Horsies::new(valid_config()).unwrap();
        app.register(
            "bad_retry",
            async_task_fn!(dummy_task, DummyArgs).with_task_options(TaskOptions {
                task_name: "bad_retry".to_owned(),
                queue_name: Some("default".to_owned()),
                good_until: None,
                auto_retry_for: None,
                retry_policy: Some(RetryPolicy::fixed(vec![60], true).unwrap()),
            }),
        )
        .unwrap();

        let pool = PgPoolOptions::new()
            .connect_lazy("postgresql://postgres@127.0.0.1:1/test")
            .unwrap();
        assert!(app
            .bind_broker(Arc::new(PostgresBroker::from_pool(pool)))
            .is_ok());

        let err = app.check_live().await.unwrap_err();
        match err {
            AppError::Validation(inner) => {
                assert_eq!(inner.code, Some(ErrorCode::TaskInvalidOptions));
                assert!(inner
                    .to_string()
                    .contains("retry_policy requires auto_retry_for"));
            }
            other => panic!("unexpected error: {}", other),
        }
    }

    // --- Proc macro tests ---

    #[derive(Serialize, Deserialize)]
    pub struct MacroAddArgs {
        pub a: i32,
        pub b: i32,
    }

    #[horsies::task("macro_add")]
    async fn macro_add(args: MacroAddArgs) -> Result<i32, horsies::TaskError> {
        Ok(args.a + args.b)
    }

    #[test]
    fn proc_macro_register_returns_task_function() {
        let mut app = Horsies::new(valid_config()).unwrap();
        let task = macro_add::register(&mut app).unwrap();
        assert_eq!(task.task_name(), "macro_add");
        assert_eq!(task.queue_name(), "default");
    }

    #[horsies::task("macro_queued", queue = "critical")]
    async fn macro_queued(_: ()) -> Result<String, horsies::TaskError> {
        Ok("done".into())
    }

    #[test]
    fn proc_macro_with_queue() {
        let config = AppConfig {
            queue_mode: QueueMode::Custom,
            custom_queues: Some(vec![crate::core::CustomQueueConfig {
                name: "critical".into(),
                priority: 1,
                max_concurrency: 10,
            }]),
            ..valid_config()
        };
        let mut app = Horsies::new(config).unwrap();
        let task = macro_queued::register(&mut app).unwrap();
        assert_eq!(task.task_name(), "macro_queued");
        assert_eq!(task.queue_name(), "critical");
    }

    #[horsies::task("macro_a")]
    async fn macro_task_a(_: ()) -> Result<String, horsies::TaskError> {
        Ok("a".into())
    }

    #[horsies::task("macro_b")]
    async fn macro_task_b(_: ()) -> Result<String, horsies::TaskError> {
        Ok("b".into())
    }

    #[horsies::task("macro_c")]
    async fn macro_task_c(_: ()) -> Result<i32, horsies::TaskError> {
        Ok(42)
    }

    #[horsies::task("macro_rt_sum")]
    async fn macro_rt_sum(
        rt: crate::TaskRuntime,
        args: MacroAddArgs,
    ) -> Result<i32, horsies::TaskError> {
        let _ = rt;
        Ok(args.a + args.b)
    }

    #[horsies::task("macro_rt_no_args")]
    async fn macro_rt_no_args(rt: crate::TaskRuntime) -> Result<String, horsies::TaskError> {
        let _ = rt;
        Ok("runtime".into())
    }

    #[test]
    fn proc_macro_sequential_registration() {
        let mut app = Horsies::new(valid_config()).unwrap();
        let a = macro_task_a::register(&mut app).unwrap();
        let b = macro_task_b::register(&mut app).unwrap();
        let c = macro_task_c::register(&mut app).unwrap();
        assert_eq!(a.task_name(), "macro_a");
        assert_eq!(b.task_name(), "macro_b");
        assert_eq!(c.task_name(), "macro_c");
    }

    #[tokio::test]
    async fn proc_macro_runtime_injection_with_args_executes() {
        let mut app = Horsies::new(valid_config()).unwrap();
        macro_rt_sum::register(&mut app).unwrap();

        let task = app.registry().get("macro_rt_sum").unwrap().clone();
        let RegisteredTask::Async { task, .. } = task else {
            panic!("expected async task");
        };

        let envelope = serde_json::json!({"args": [MacroAddArgs { a: 2, b: 5 }], "kwargs": {}});
        let args = serde_json::to_vec(&envelope).unwrap();
        let result = task.execute(&args).await.unwrap();
        let value: i32 = serde_json::from_slice(&result).unwrap();
        assert_eq!(value, 7);
    }

    #[tokio::test]
    async fn proc_macro_runtime_injection_without_user_args_executes() {
        let mut app = Horsies::new(valid_config()).unwrap();
        macro_rt_no_args::register(&mut app).unwrap();

        let task = app.registry().get("macro_rt_no_args").unwrap().clone();
        let RegisteredTask::Async { task, .. } = task else {
            panic!("expected async task");
        };

        let envelope = serde_json::json!({"args": [], "kwargs": {}});
        let args = serde_json::to_vec(&envelope).unwrap();
        let result = task.execute(&args).await.unwrap();
        let value: String = serde_json::from_slice(&result).unwrap();
        assert_eq!(value, "runtime");
    }

    #[horsies::task(
        "macro_with_options",
        queue = "critical",
        auto_retry_for = ["RATE_LIMITED", "TIMEOUT"],
    )]
    async fn macro_with_options(_: ()) -> Result<String, horsies::TaskError> {
        Ok("done".into())
    }

    #[test]
    fn proc_macro_with_task_options() {
        let config = AppConfig {
            queue_mode: QueueMode::Custom,
            custom_queues: Some(vec![crate::core::CustomQueueConfig {
                name: "critical".into(),
                priority: 1,
                max_concurrency: 10,
            }]),
            ..valid_config()
        };
        let mut app = Horsies::new(config).unwrap();

        let task = macro_with_options::register(&mut app).unwrap();
        assert_eq!(task.task_name(), "macro_with_options");
        assert_eq!(task.queue_name(), "critical");
        assert!(task.task_options().is_some());
    }

    #[test]
    fn proc_macro_node_carries_queue_and_priority() {
        let config = AppConfig {
            queue_mode: QueueMode::Custom,
            custom_queues: Some(vec![crate::core::CustomQueueConfig {
                name: "critical".into(),
                priority: 1,
                max_concurrency: 10,
            }]),
            ..valid_config()
        };
        let mut app = Horsies::new(config).unwrap();
        let task = macro_with_options::register(&mut app).unwrap();
        let _node = task.node();
    }

    #[test]
    fn proc_macro_two_tasks_same_module_no_collision() {
        let mut app = Horsies::new(valid_config()).unwrap();
        let a = macro_task_a::register(&mut app).unwrap();
        let b = macro_task_b::register(&mut app).unwrap();
        assert_ne!(a.task_name(), b.task_name());
    }

    #[test]
    fn proc_macro_duplicate_name_rejected_by_registry() {
        let mut app = Horsies::new(valid_config()).unwrap();
        let _first = macro_task_a::register(&mut app).unwrap();
        let result = macro_task_a::register(&mut app);
        assert!(result.is_err(), "duplicate task name should be rejected");
    }

    #[horsies::task("macro_no_options")]
    async fn macro_no_options(_: ()) -> Result<(), horsies::TaskError> {
        Ok(())
    }

    #[test]
    fn proc_macro_without_options_has_no_task_options() {
        let mut app = Horsies::new(valid_config()).unwrap();
        let task = macro_no_options::register(&mut app).unwrap();
        assert!(task.task_options().is_none());
        assert_eq!(task.queue_name(), "default");
    }
}
