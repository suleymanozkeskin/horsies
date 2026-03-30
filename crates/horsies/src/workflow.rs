use std::marker::PhantomData;
use std::sync::{Arc, RwLock};

use serde::de::DeserializeOwned;

use crate::broker::BrokerError;
use crate::core::{
    AnyNode, HorsiesError, NodeRef, OnError, SubWorkflowNode, SuccessPolicy, TaskNode,
    WorkflowSpec, WorkflowSpecBuilder, WorkflowStartError, WorkflowStartErrorCode,
    WorkflowStartResult,
};
use crate::workflow_engine::{BoundWorkflowSpec, WorkflowHandle};

use crate::lazy_broker::LazyBroker;

pub struct WorkflowRegistrationBuilder<'a, T> {
    pub(crate) app: &'a mut crate::Horsies,
    pub(crate) builder: WorkflowSpecBuilder,
    pub(crate) _phantom: PhantomData<T>,
}

impl<'a, T: DeserializeOwned + Clone> WorkflowRegistrationBuilder<'a, T> {
    pub(crate) fn new(app: &'a mut crate::Horsies, name: &str) -> Self {
        Self {
            app,
            builder: WorkflowSpecBuilder::new(name),
            _phantom: PhantomData,
        }
    }

    pub fn task<V>(&mut self, node: TaskNode<V>) -> NodeRef {
        self.builder.task(node)
    }

    pub fn sub_workflow(&mut self, node: SubWorkflowNode) -> NodeRef {
        self.builder.sub_workflow(node)
    }

    pub fn definition_key(&mut self, key: impl Into<String>) -> &mut Self {
        self.builder.definition_key(key);
        self
    }

    pub fn on_error(&mut self, policy: OnError) -> &mut Self {
        self.builder.on_error(policy);
        self
    }

    pub fn output(&mut self, node_ref: NodeRef) -> &mut Self {
        self.builder.output(node_ref);
        self
    }

    pub fn success_policy(&mut self, policy: SuccessPolicy) -> &mut Self {
        self.builder.success_policy(policy);
        self
    }

    pub fn inner(&mut self) -> &mut WorkflowSpecBuilder {
        &mut self.builder
    }

    pub fn build(self) -> Result<WorkflowFunction<T>, HorsiesError> {
        let registered = self.builder.build_registered()?;
        self.app.register_workflow::<T>(registered)
    }
}

#[derive(Clone)]
pub struct WorkflowFunction<T> {
    spec: WorkflowSpec,
    broker: Arc<LazyBroker>,
    registry: Arc<RwLock<crate::core::WorkflowSpecRegistry>>,
    resend_on_transient_err: bool,
    _phantom: PhantomData<T>,
}

impl<T: DeserializeOwned + Clone> WorkflowFunction<T> {
    pub(crate) fn new(
        spec: WorkflowSpec,
        broker: Arc<LazyBroker>,
        registry: Arc<RwLock<crate::core::WorkflowSpecRegistry>>,
        resend_on_transient_err: bool,
    ) -> Self {
        Self {
            spec,
            broker,
            registry,
            resend_on_transient_err,
            _phantom: PhantomData,
        }
    }

    pub fn name(&self) -> &str {
        &self.spec.name
    }

    pub fn definition_key(&self) -> Option<&str> {
        self.spec.definition_key.as_deref()
    }

    pub fn spec(&self) -> &WorkflowSpec {
        &self.spec
    }

    pub fn tasks(&self) -> &[AnyNode] {
        &self.spec.tasks
    }

    pub fn resend_on_transient_err(&self) -> bool {
        self.resend_on_transient_err
    }

    pub async fn start(&self) -> WorkflowStartResult<WorkflowHandle<T>> {
        let bound = self
            .bound_spec()
            .await
            .map_err(|err| self.wrap_broker_error(&err, String::new()))?;
        bound.start().await
    }

    pub async fn start_with_id(
        &self,
        workflow_id: impl Into<String>,
    ) -> WorkflowStartResult<WorkflowHandle<T>> {
        let workflow_id = workflow_id.into();
        let bound = self
            .bound_spec()
            .await
            .map_err(|err| self.wrap_broker_error(&err, workflow_id.clone()))?;
        bound.start_with_id(workflow_id).await
    }

    pub async fn retry_start(
        &self,
        error: &WorkflowStartError,
    ) -> WorkflowStartResult<WorkflowHandle<T>> {
        let bound = self
            .bound_spec()
            .await
            .map_err(|err| self.wrap_broker_error(&err, error.workflow_id.clone()))?;
        bound.retry_start(error).await
    }

    pub async fn handle(
        &self,
        workflow_id: impl Into<String>,
    ) -> Result<WorkflowHandle<T>, crate::AppError> {
        Ok(self.bound_spec().await?.handle(workflow_id))
    }

    async fn bound_spec(&self) -> Result<BoundWorkflowSpec<T>, BrokerError> {
        let broker = self.broker.get().await?;
        let registry = self
            .registry
            .read()
            .expect("workflow registry lock poisoned")
            .clone();
        Ok(BoundWorkflowSpec::from_broker(
            self.spec.clone(),
            &broker,
            Arc::new(registry),
            self.resend_on_transient_err,
        ))
    }

    fn wrap_broker_error(&self, err: &BrokerError, workflow_id: String) -> WorkflowStartError {
        WorkflowStartError {
            code: WorkflowStartErrorCode::EnqueueFailed,
            message: err.to_string(),
            retryable: err.is_retryable(),
            workflow_name: self.spec.name.clone(),
            workflow_id,
        }
    }
}

/// Lightweight, cloneable handle for starting workflows after the app is consumed.
///
/// Extract this before calling [`Horsies::run_worker_with`] so that tasks
/// running inside the worker can start dynamic workflows at runtime without
/// needing direct access to the app or manual broker/registry plumbing.
///
/// ```ignore
/// // Before consuming the app:
/// let starter = app.workflow_starter();
///
/// // After app is consumed by the worker:
/// // (e.g. inside a task, via a global or dependency injection)
/// let spec = WorkflowSpecBuilder::new("enrichment")
///     .task(node_a)
///     .definition_key("myapp.enrichment.v1")
///     .build()?;
/// let handle = starter.start::<MyOutput>(spec).await?;
/// ```
///
/// Sub-workflows referenced by a dynamically-built spec must be registered
/// before the worker starts (i.e. before the app is consumed).
#[derive(Clone)]
pub struct WorkflowStarter {
    broker: Arc<LazyBroker>,
    registry: Arc<RwLock<crate::core::WorkflowSpecRegistry>>,
    resend_on_transient_err: bool,
}

impl WorkflowStarter {
    pub(crate) fn new(
        broker: Arc<LazyBroker>,
        registry: Arc<RwLock<crate::core::WorkflowSpecRegistry>>,
        resend_on_transient_err: bool,
    ) -> Self {
        Self {
            broker,
            registry,
            resend_on_transient_err,
        }
    }

    /// Start a workflow from a runtime-built [`WorkflowSpec`].
    pub async fn start<T: DeserializeOwned + Clone>(
        &self,
        spec: WorkflowSpec,
    ) -> WorkflowStartResult<WorkflowHandle<T>> {
        let workflow_name = spec.name.clone();
        let broker = self.broker.get().await.map_err(|err| WorkflowStartError {
            code: WorkflowStartErrorCode::EnqueueFailed,
            message: err.to_string(),
            retryable: err.is_retryable(),
            workflow_name: workflow_name.clone(),
            workflow_id: String::new(),
        })?;
        let registry = self
            .registry
            .read()
            .expect("workflow registry lock poisoned")
            .clone();
        let bound = BoundWorkflowSpec::<T>::from_broker(
            spec,
            &broker,
            Arc::new(registry),
            self.resend_on_transient_err,
        );
        bound.start().await
    }

    /// Start a workflow with a caller-provided ID (idempotent).
    pub async fn start_with_id<T: DeserializeOwned + Clone>(
        &self,
        spec: WorkflowSpec,
        workflow_id: impl Into<String>,
    ) -> WorkflowStartResult<WorkflowHandle<T>> {
        let workflow_name = spec.name.clone();
        let workflow_id = workflow_id.into();
        let broker = self.broker.get().await.map_err(|err| WorkflowStartError {
            code: WorkflowStartErrorCode::EnqueueFailed,
            message: err.to_string(),
            retryable: err.is_retryable(),
            workflow_name: workflow_name.clone(),
            workflow_id: workflow_id.clone(),
        })?;
        let registry = self
            .registry
            .read()
            .expect("workflow registry lock poisoned")
            .clone();
        let bound = BoundWorkflowSpec::<T>::from_broker(
            spec,
            &broker,
            Arc::new(registry),
            self.resend_on_transient_err,
        );
        bound.start_with_id(workflow_id).await
    }
}

impl std::fmt::Debug for WorkflowStarter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WorkflowStarter")
            .field("resend_on_transient_err", &self.resend_on_transient_err)
            .finish()
    }
}

impl<T> std::fmt::Debug for WorkflowFunction<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WorkflowFunction")
            .field("name", &self.spec.name)
            .field("definition_key", &self.spec.definition_key)
            .field("task_count", &self.spec.tasks.len())
            .finish()
    }
}

type WorkflowBuilderFn<P> =
    Arc<dyn Fn(&crate::Horsies, &P) -> Result<WorkflowSpec, HorsiesError> + Send + Sync + 'static>;
type ZeroArgWorkflowBuilderFn =
    Arc<dyn Fn(&crate::Horsies) -> Result<WorkflowSpec, HorsiesError> + Send + Sync + 'static>;

pub(crate) trait WorkflowBuilderCheck: Send + Sync {
    fn run_check(&self, app: &crate::Horsies) -> Vec<HorsiesError>;
}

struct RegisteredWorkflowBuilder<P> {
    name: String,
    cases: Vec<P>,
    builder: WorkflowBuilderFn<P>,
}

struct RegisteredZeroArgWorkflowBuilder {
    name: String,
    builder: ZeroArgWorkflowBuilderFn,
}

impl<P> WorkflowBuilderCheck for RegisteredWorkflowBuilder<P>
where
    P: Send + Sync + 'static,
{
    fn run_check(&self, app: &crate::Horsies) -> Vec<HorsiesError> {
        if self.cases.is_empty() {
            return vec![HorsiesError::new(format!(
                "workflow builder '{}' requires at least one check case",
                self.name,
            ))
            .with_code(crate::core::ErrorCode::WorkflowCheckCasesRequired)
            .with_help("register at least one typed case via .case(...) or .cases(...)")];
        }

        let mut errors = Vec::new();
        for case in &self.cases {
            errors.extend(self.run_case(app, case));
        }
        errors
    }
}

impl WorkflowBuilderCheck for RegisteredZeroArgWorkflowBuilder {
    fn run_check(&self, app: &crate::Horsies) -> Vec<HorsiesError> {
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| (self.builder)(app)));

        match result {
            Ok(Ok(spec)) => {
                if spec.definition_key.is_none() {
                    vec![HorsiesError::new(format!(
                        "workflow builder '{}' produced a workflow without definition_key",
                        self.name,
                    ))
                    .with_code(crate::core::ErrorCode::WorkflowNoDefinitionKey)]
                } else {
                    Vec::new()
                }
            }
            Ok(Err(err)) if err.code.is_some() => vec![err],
            Ok(Err(err)) => vec![HorsiesError::new(format!(
                "workflow builder '{}' failed: {}",
                self.name, err,
            ))
            .with_code(crate::core::ErrorCode::WorkflowCheckBuilderException)],
            Err(_) => vec![HorsiesError::new(format!(
                "workflow builder '{}' panicked during check execution",
                self.name,
            ))
            .with_code(crate::core::ErrorCode::WorkflowCheckBuilderException)],
        }
    }
}

impl<P> RegisteredWorkflowBuilder<P>
where
    P: Send + Sync + 'static,
{
    fn run_case(&self, app: &crate::Horsies, case: &P) -> Vec<HorsiesError> {
        let result =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| (self.builder)(app, case)));

        match result {
            Ok(Ok(spec)) => {
                if spec.definition_key.is_none() {
                    vec![HorsiesError::new(format!(
                        "workflow builder '{}' produced a workflow without definition_key",
                        self.name,
                    ))
                    .with_code(crate::core::ErrorCode::WorkflowNoDefinitionKey)]
                } else {
                    Vec::new()
                }
            }
            Ok(Err(err)) if err.code.is_some() => vec![err],
            Ok(Err(err)) => vec![HorsiesError::new(format!(
                "workflow builder '{}' failed: {}",
                self.name, err,
            ))
            .with_code(crate::core::ErrorCode::WorkflowCheckBuilderException)],
            Err(_) => vec![HorsiesError::new(format!(
                "workflow builder '{}' panicked during check execution",
                self.name,
            ))
            .with_code(crate::core::ErrorCode::WorkflowCheckBuilderException)],
        }
    }
}

pub struct WorkflowBuilderRegistration<'a, P> {
    app: &'a mut crate::Horsies,
    name: String,
    cases: Vec<P>,
    kind: WorkflowBuilderRegistrationKind<P>,
}

enum WorkflowBuilderRegistrationKind<P> {
    Parameterized(WorkflowBuilderFn<P>),
    ZeroArg(ZeroArgWorkflowBuilderFn),
}

impl<'a, P> WorkflowBuilderRegistration<'a, P>
where
    P: Send + Sync + 'static,
{
    pub(crate) fn new(
        app: &'a mut crate::Horsies,
        name: String,
        builder: WorkflowBuilderFn<P>,
    ) -> Self {
        Self {
            app,
            name,
            cases: Vec::new(),
            kind: WorkflowBuilderRegistrationKind::Parameterized(builder),
        }
    }

    pub(crate) fn new_zero_arg(
        app: &'a mut crate::Horsies,
        name: String,
        builder: ZeroArgWorkflowBuilderFn,
    ) -> Self {
        Self {
            app,
            name,
            cases: Vec::new(),
            kind: WorkflowBuilderRegistrationKind::ZeroArg(builder),
        }
    }

    pub fn case(&mut self, case: P) -> &mut Self {
        self.cases.push(case);
        self
    }

    pub fn cases<I>(&mut self, cases: I) -> &mut Self
    where
        I: IntoIterator<Item = P>,
    {
        self.cases.extend(cases);
        self
    }

    pub fn register(self) -> Result<(), HorsiesError> {
        match self.kind {
            WorkflowBuilderRegistrationKind::Parameterized(builder) => {
                self.app
                    .workflow_builders
                    .push(Box::new(RegisteredWorkflowBuilder {
                        name: self.name,
                        cases: self.cases,
                        builder,
                    }));
            }
            WorkflowBuilderRegistrationKind::ZeroArg(builder) => {
                self.app
                    .workflow_builders
                    .push(Box::new(RegisteredZeroArgWorkflowBuilder {
                        name: self.name,
                        builder,
                    }));
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc as StdArc;

    use crate::core::{
        AppConfig, PostgresConfig, QueueMode, RecoveryConfig, TaskNode, WorkerResilienceConfig,
    };

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
    fn build_registers_workflow_and_returns_function() {
        let mut app = crate::Horsies::new(valid_config()).unwrap();
        let workflow = {
            let mut builder = app.workflow::<String>("greet");
            builder.definition_key("tests.greet.v1");
            let node = builder.task(TaskNode::<String>::new("say_hello"));
            builder.output(node);
            builder.build().unwrap()
        };

        assert_eq!(workflow.name(), "greet");
        assert_eq!(workflow.definition_key(), Some("tests.greet.v1"));
        assert!(app.workflow_registry().contains("greet"));
    }

    #[test]
    fn register_workflow_spec_returns_runtime_object() {
        let mut builder = WorkflowSpecBuilder::new("sum");
        builder.definition_key("tests.sum.v1");
        let node = builder.task(TaskNode::<i32>::new("sum_task"));
        builder.output(node);
        let spec = builder.build().unwrap();

        let mut app = crate::Horsies::new(valid_config()).unwrap();
        let workflow = app.register_workflow_spec::<i32>(spec).unwrap();
        assert_eq!(workflow.name(), "sum");
        assert!(app.workflow_registry().contains("sum"));
    }

    #[test]
    fn workflow_function_sees_later_registry_updates() {
        let mut app = crate::Horsies::new(valid_config()).unwrap();

        let first = {
            let mut builder = app.workflow::<String>("first");
            builder.definition_key("tests.first.v1");
            let node = builder.task(TaskNode::<String>::new("hello_task"));
            builder.output(node);
            builder.build().unwrap()
        };

        {
            let mut builder = app.workflow::<String>("second");
            builder.definition_key("tests.second.v1");
            let node = builder.task(TaskNode::<String>::new("hello_task"));
            builder.output(node);
            builder.build().unwrap();
        }

        let current_registry = first
            .registry
            .read()
            .expect("workflow registry lock poisoned")
            .clone();
        assert!(current_registry.contains("second"));
    }

    #[test]
    fn unified_zero_arg_workflow_builder_runs_during_check() {
        let mut app = crate::Horsies::new(valid_config()).unwrap();
        let ran = StdArc::new(AtomicBool::new(false));
        let ran_clone = StdArc::clone(&ran);

        let registration = app
            .workflow_builder0("hello_builder", move |_app| {
                ran_clone.store(true, Ordering::Relaxed);
                let mut builder = WorkflowSpecBuilder::new("hello");
                builder.definition_key("tests.hello.v1");
                let node = builder.task(TaskNode::<String>::new("hello_task"));
                builder.output(node);
                builder.build()
            })
            .unwrap();
        registration.register().unwrap();

        app.check().unwrap();
        assert!(ran.load(Ordering::Relaxed));
    }

    #[test]
    fn unified_parameterized_workflow_builder_requires_cases() {
        let mut app = crate::Horsies::new(valid_config()).unwrap();
        let registration = app
            .workflow_builder("regional", |_app, region: &String| {
                let mut builder = WorkflowSpecBuilder::new(format!("regional_{region}"));
                builder.definition_key(format!("tests.regional.{region}.v1"));
                let node = builder.task(TaskNode::<String>::new("hello_task"));
                builder.output(node);
                builder.build()
            })
            .unwrap();
        registration.register().unwrap();

        let err = app.check().unwrap_err();
        let rendered = err.to_string();
        assert!(rendered.contains("HRS-027"));
    }

    #[test]
    fn unified_workflow_builder_missing_definition_key_is_reported() {
        let mut app = crate::Horsies::new(valid_config()).unwrap();
        let registration = app
            .workflow_builder0("missing_key", |_app| {
                let mut builder = WorkflowSpecBuilder::new("missing_key");
                let node = builder.task(TaskNode::<String>::new("hello_task"));
                builder.output(node);
                builder.build()
            })
            .unwrap();
        registration.register().unwrap();

        let err = app.check().unwrap_err();
        let rendered = err.to_string();
        assert!(rendered.contains("HRS-016"));
    }

    #[test]
    fn workflow_starter_is_cloneable_and_shares_registry() {
        let mut app = crate::Horsies::new(valid_config()).unwrap();

        // Register a workflow before extracting the starter.
        {
            let mut builder = app.workflow::<String>("pre_existing");
            builder.definition_key("tests.pre_existing.v1");
            let node = builder.task(TaskNode::<String>::new("hello_task"));
            builder.output(node);
            builder.build().unwrap();
        }

        let starter = app.workflow_starter();
        let starter_clone = starter.clone();

        // Both the starter and its clone see the pre-existing workflow.
        let reg = starter
            .registry
            .read()
            .expect("lock")
            .clone();
        assert!(reg.contains("pre_existing"));

        let reg_clone = starter_clone
            .registry
            .read()
            .expect("lock")
            .clone();
        assert!(reg_clone.contains("pre_existing"));
    }

    #[test]
    fn workflow_starter_sees_workflows_registered_after_extraction() {
        let mut app = crate::Horsies::new(valid_config()).unwrap();

        // Extract starter first.
        let starter = app.workflow_starter();

        // Register a workflow after extraction.
        {
            let mut builder = app.workflow::<String>("late_registered");
            builder.definition_key("tests.late_registered.v1");
            let node = builder.task(TaskNode::<String>::new("hello_task"));
            builder.output(node);
            builder.build().unwrap();
        }

        // Starter shares the Arc<RwLock<..>> so it sees the update.
        let reg = starter
            .registry
            .read()
            .expect("lock")
            .clone();
        assert!(reg.contains("late_registered"));
    }

    #[tokio::test]
    async fn workflow_starter_start_returns_enqueue_failed_without_db() {
        let mut app = crate::Horsies::new(valid_config()).unwrap();

        // Bind a broker that points at a non-existent DB so start() fails at connect.
        let pool = sqlx::postgres::PgPoolOptions::new()
            .connect_lazy("postgresql://postgres@127.0.0.1:1/nonexistent")
            .unwrap();
        assert!(app
            .bind_broker(std::sync::Arc::new(crate::PostgresBroker::from_pool(pool)))
            .is_ok());

        let starter = app.workflow_starter();

        let mut builder = WorkflowSpecBuilder::new("starter_test");
        builder.definition_key("tests.starter_test.v1");
        let node = builder.task(TaskNode::<String>::new("some_task"));
        builder.output(node);
        let spec = builder.build().unwrap();

        let err = starter.start::<String>(spec).await.unwrap_err();
        assert_eq!(err.code, WorkflowStartErrorCode::EnqueueFailed);
        assert_eq!(err.workflow_name, "starter_test");
    }

    #[tokio::test]
    async fn app_start_rejects_duplicate_name() {
        let mut app = crate::Horsies::new(valid_config()).unwrap();

        // Register a workflow first.
        let mut b1 = WorkflowSpecBuilder::new("taken_name");
        b1.definition_key("tests.taken.v1");
        let node = b1.task(TaskNode::<String>::new("some_task"));
        b1.output(node);
        app.register_workflow_spec::<String>(b1.build().unwrap())
            .unwrap();

        // Trying to start a new spec with the same name should fail validation.
        let mut b2 = WorkflowSpecBuilder::new("taken_name");
        b2.definition_key("tests.taken.v2");
        let node = b2.task(TaskNode::<String>::new("other_task"));
        b2.output(node);
        let spec = b2.build().unwrap();

        let err = app.start::<String>(spec).await.unwrap_err();
        assert_eq!(err.code, WorkflowStartErrorCode::ValidationFailed);
        assert_eq!(err.workflow_name, "taken_name");
        assert!(!err.retryable);
    }

    #[test]
    fn unified_workflow_builder_runs_under_send_suppression() {
        let mut app = crate::Horsies::new(valid_config()).unwrap();
        let observed = StdArc::new(AtomicBool::new(false));
        let observed_clone = StdArc::clone(&observed);

        let registration = app
            .workflow_builder0("suppressed", move |app| {
                observed_clone.store(app.are_sends_suppressed(), Ordering::Relaxed);
                let mut builder = WorkflowSpecBuilder::new("suppressed");
                builder.definition_key("tests.suppressed.v1");
                let node = builder.task(TaskNode::<String>::new("hello_task"));
                builder.output(node);
                builder.build()
            })
            .unwrap();
        registration.register().unwrap();

        app.check().unwrap();
        assert!(observed.load(Ordering::Relaxed));
    }
}
