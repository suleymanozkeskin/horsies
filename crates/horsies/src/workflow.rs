use std::marker::PhantomData;
use std::sync::{Arc, RwLock};

use serde::de::DeserializeOwned;

use crate::core::{
    AnyNode, HorsiesError, NodeRef, OnError, SubWorkflowNode, SuccessPolicy, TaskNode,
    WorkflowDefinition, WorkflowSpec, WorkflowSpecBuilder, WorkflowStartError,
    WorkflowStartErrorCode, WorkflowStartResult,
};
use crate::core::app as core_app;
use crate::workflow_engine::bound_handle::WorkflowHandle;
use crate::workflow_engine::bound_spec::BoundWorkflowSpec;

use crate::lazy_broker::LazyBroker;

pub struct WorkflowRegistrationBuilder<'a, T> {
    pub(crate) app: &'a mut crate::Horsies,
    pub(crate) builder: WorkflowSpecBuilder,
    pub(crate) _phantom: PhantomData<T>,
}

/// Builder for registering representative dynamic workflow cases for `app.check()`.
///
/// This is the public check-time validation path for runtime-built or
/// parameterized workflow specs whose shape depends on input cases.
pub struct WorkflowCheckBuilderRegistration<'a, P>
where
    P: Send + Sync + 'static,
{
    pub(crate) inner: core_app::WorkflowBuilderRegistration<'a, P>,
}

impl<'a, P> WorkflowCheckBuilderRegistration<'a, P>
where
    P: Send + Sync + 'static,
{
    pub(crate) fn new(inner: core_app::WorkflowBuilderRegistration<'a, P>) -> Self {
        Self { inner }
    }

    /// Add one representative validation case.
    pub fn case(&mut self, case: P) -> &mut Self {
        self.inner.case(case);
        self
    }

    /// Add multiple representative validation cases.
    pub fn cases<I>(&mut self, cases: I) -> &mut Self
    where
        I: IntoIterator<Item = P>,
    {
        self.inner.cases(cases);
        self
    }

    /// Finalize registration so `app.check()` runs these cases.
    pub fn register(self) -> Result<(), HorsiesError> {
        self.inner.register()
    }
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
            .map_err(|err| self.wrap_app_error(err, String::new()))?;
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
            .map_err(|err| self.wrap_app_error(err, workflow_id.clone()))?;
        bound.start_with_id(workflow_id).await
    }

    pub async fn retry_start(
        &self,
        error: &WorkflowStartError,
    ) -> WorkflowStartResult<WorkflowHandle<T>> {
        let bound = self
            .bound_spec()
            .await
            .map_err(|err| self.wrap_app_error(err, error.workflow_id.clone()))?;
        bound.retry_start(error).await
    }

    pub async fn handle(
        &self,
        workflow_id: impl Into<String>,
    ) -> Result<WorkflowHandle<T>, crate::AppError> {
        Ok(self.bound_spec().await?.handle(workflow_id))
    }

    async fn bound_spec(&self) -> crate::AppResult<BoundWorkflowSpec<T>> {
        let registry = self
            .registry
            .read()
            .expect("workflow registry lock poisoned")
            .clone();
        // Belt-and-suspenders: resolve queue/priority in case the spec
        // was constructed before task defaults were fully populated.
        // Registered WorkflowFunctions already hold resolved specs, but
        // this guards against edge cases.
        // Validate before acquiring the broker connection so spec errors
        // surface without a network round-trip.
        let mut spec = self.spec.clone();
        registry.resolve_and_validate_spec_queue_priority(&mut spec)?;
        let broker = self.broker.get().await?;
        Ok(BoundWorkflowSpec::from_broker(
            spec,
            &broker,
            Arc::new(registry),
            self.resend_on_transient_err,
        ))
    }

    fn wrap_app_error(&self, err: crate::AppError, workflow_id: String) -> WorkflowStartError {
        match err {
            crate::AppError::Validation(err) => WorkflowStartError {
                code: WorkflowStartErrorCode::ValidationFailed,
                message: err.to_string(),
                retryable: false,
                workflow_name: self.spec.name.clone(),
                workflow_id,
            },
            crate::AppError::Broker(err) => WorkflowStartError {
                code: WorkflowStartErrorCode::EnqueueFailed,
                message: err.to_string(),
                retryable: err.is_retryable(),
                workflow_name: self.spec.name.clone(),
                workflow_id,
            },
            other => WorkflowStartError {
                code: WorkflowStartErrorCode::InternalFailed,
                message: other.to_string(),
                retryable: false,
                workflow_name: self.spec.name.clone(),
                workflow_id,
            },
        }
    }
}

#[derive(Clone)]
pub struct WorkflowTemplate<P, T> {
    workflow_name: String,
    definition_key: String,
    starter: WorkflowStarter,
    builder: Arc<dyn Fn(P) -> Result<WorkflowSpec, HorsiesError> + Send + Sync + 'static>,
    _phantom: PhantomData<fn(P) -> T>,
}

impl<P, T: DeserializeOwned + Clone> WorkflowTemplate<P, T> {
    pub(crate) fn from_definition<D>(starter: WorkflowStarter) -> Self
    where
        D: WorkflowDefinition<Output = T, Params = P> + 'static,
    {
        Self {
            workflow_name: D::name().to_owned(),
            definition_key: D::definition_key().to_owned(),
            starter,
            builder: Arc::new(D::build_with),
            _phantom: PhantomData,
        }
    }

    pub fn name(&self) -> &str {
        &self.workflow_name
    }

    pub fn definition_key(&self) -> &str {
        &self.definition_key
    }

    pub fn build(&self, params: P) -> Result<WorkflowSpec, HorsiesError> {
        (self.builder)(params)
    }

    pub async fn start(&self, params: P) -> WorkflowStartResult<WorkflowHandle<T>> {
        let spec = self.build(params).map_err(|err| WorkflowStartError {
            code: WorkflowStartErrorCode::ValidationFailed,
            message: err.to_string(),
            retryable: false,
            workflow_name: self.workflow_name.clone(),
            workflow_id: String::new(),
        })?;
        self.starter.start(spec).await
    }

    pub async fn start_with_id(
        &self,
        params: P,
        workflow_id: impl Into<String>,
    ) -> WorkflowStartResult<WorkflowHandle<T>> {
        let workflow_id = workflow_id.into();
        let spec = self.build(params).map_err(|err| WorkflowStartError {
            code: WorkflowStartErrorCode::ValidationFailed,
            message: err.to_string(),
            retryable: false,
            workflow_name: self.workflow_name.clone(),
            workflow_id: workflow_id.clone(),
        })?;
        self.starter.start_with_id(spec, workflow_id).await
    }
}

#[derive(Clone)]
pub(crate) struct WorkflowStarter {
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

    pub(crate) async fn start<T: DeserializeOwned + Clone>(
        &self,
        mut spec: WorkflowSpec,
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

        // Resolve queue/priority from task defaults for dynamic specs.
        registry
            .resolve_and_validate_spec_queue_priority(&mut spec)
            .map_err(|err| WorkflowStartError {
                code: WorkflowStartErrorCode::ValidationFailed,
                message: err.to_string(),
                retryable: false,
                workflow_name: workflow_name.clone(),
                workflow_id: String::new(),
            })?;

        let bound = BoundWorkflowSpec::<T>::from_broker(
            spec,
            &broker,
            Arc::new(registry),
            self.resend_on_transient_err,
        );
        bound.start().await
    }

    pub(crate) async fn start_with_id<T: DeserializeOwned + Clone>(
        &self,
        mut spec: WorkflowSpec,
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

        // Resolve queue/priority from task defaults for dynamic specs.
        registry
            .resolve_and_validate_spec_queue_priority(&mut spec)
            .map_err(|err| WorkflowStartError {
                code: WorkflowStartErrorCode::ValidationFailed,
                message: err.to_string(),
                retryable: false,
                workflow_name: workflow_name.clone(),
                workflow_id: workflow_id.clone(),
            })?;

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

impl<P, T> std::fmt::Debug for WorkflowTemplate<P, T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WorkflowTemplate")
            .field("name", &self.workflow_name)
            .field("definition_key", &self.definition_key)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::core::{
        AppConfig, HorsiesError, PostgresConfig, QueueMode, RecoveryConfig, TaskNode,
        WorkerResilienceConfig,
    };
    use crate::{ErrorCode, WorkflowDefConfig, WorkflowDefinition};

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

    struct DefinedWorkflow;

    impl WorkflowDefinition for DefinedWorkflow {
        type Output = String;
        type Params = ();

        fn name() -> &'static str {
            "defined_greet"
        }

        fn definition_key() -> &'static str {
            "tests.defined_greet.v1"
        }

        fn define(builder: &mut WorkflowSpecBuilder) -> Result<WorkflowDefConfig, HorsiesError> {
            let node = builder.task(TaskNode::<String>::new("say_hello").node_id("greet"));
            Ok(WorkflowDefConfig::new().output(node))
        }
    }

    #[test]
    fn register_workflow_definition_returns_startable_workflow_function() {
        let mut app = crate::Horsies::new(valid_config()).unwrap();
        let workflow = app
            .register_workflow_definition::<DefinedWorkflow>()
            .unwrap();

        assert_eq!(workflow.name(), "defined_greet");
        assert_eq!(workflow.definition_key(), Some("tests.defined_greet.v1"));
        assert!(app.workflow_registry().contains("defined_greet"));
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
        let reg = starter.registry.read().expect("lock").clone();
        assert!(reg.contains("pre_existing"));

        let reg_clone = starter_clone.registry.read().expect("lock").clone();
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
        let reg = starter.registry.read().expect("lock").clone();
        assert!(reg.contains("late_registered"));
    }

    #[derive(Clone)]
    struct RegionalParams {
        region: String,
    }

    struct RegionalWorkflow;

    impl WorkflowDefinition for RegionalWorkflow {
        type Output = String;
        type Params = RegionalParams;

        fn name() -> &'static str {
            "regional_template"
        }

        fn definition_key() -> &'static str {
            "tests.regional_template.v1"
        }

        fn define(builder: &mut WorkflowSpecBuilder) -> Result<WorkflowDefConfig, HorsiesError> {
            let node = builder.task(TaskNode::<String>::new("hello_task").node_id("hello"));
            Ok(WorkflowDefConfig::new().output(node))
        }

        fn build_with(params: Self::Params) -> Result<WorkflowSpec, HorsiesError> {
            let mut builder = WorkflowSpecBuilder::new(format!("regional_{}", params.region));
            builder.definition_key(format!("tests.regional_template.{}.v1", params.region));
            let node = builder.task(
                TaskNode::<String>::new("hello_task")
                    .node_id("hello")
                    .args_json(serde_json::to_string(&params.region).unwrap()),
            );
            builder.output(node);
            builder.build()
        }
    }

    #[test]
    fn workflow_template_build_uses_typed_params() {
        let app = crate::Horsies::new(valid_config()).unwrap();
        let template = app.workflow_template::<RegionalWorkflow>();
        let spec = template
            .build(RegionalParams {
                region: "eu".to_owned(),
            })
            .unwrap();

        assert_eq!(template.name(), "regional_template");
        assert_eq!(template.definition_key(), "tests.regional_template.v1");
        assert_eq!(spec.name, "regional_eu");
        assert_eq!(
            spec.definition_key.as_deref(),
            Some("tests.regional_template.eu.v1")
        );
        assert_eq!(spec.tasks[0].args_json.as_deref(), Some("\"eu\""));
    }

    struct InvalidTemplateWorkflow;

    impl WorkflowDefinition for InvalidTemplateWorkflow {
        type Output = String;
        type Params = String;

        fn name() -> &'static str {
            "invalid_template"
        }

        fn definition_key() -> &'static str {
            "tests.invalid_template.v1"
        }

        fn define(builder: &mut WorkflowSpecBuilder) -> Result<WorkflowDefConfig, HorsiesError> {
            let node = builder.task(TaskNode::<String>::new("hello_task").node_id("hello"));
            Ok(WorkflowDefConfig::new().output(node))
        }

        fn build_with(params: Self::Params) -> Result<WorkflowSpec, HorsiesError> {
            Err(HorsiesError::new(format!("bad region: {}", params))
                .with_code(ErrorCode::WorkflowCheckCaseInvalid))
        }
    }

    #[tokio::test]
    async fn workflow_template_start_maps_build_errors_to_validation_failed() {
        let app = crate::Horsies::new(valid_config()).unwrap();
        let template = app.workflow_template::<InvalidTemplateWorkflow>();

        let err = template.start("antarctica".to_owned()).await.unwrap_err();
        assert_eq!(err.code, WorkflowStartErrorCode::ValidationFailed);
        assert_eq!(err.workflow_name, "invalid_template");
        assert!(!err.retryable);
        assert!(err.message.contains("bad region: antarctica"));
    }

    #[tokio::test]
    async fn workflow_starter_start_returns_enqueue_failed_without_db() {
        let app = crate::Horsies::new(valid_config()).unwrap();

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
        // Explicitly set queue so node resolution passes and the error
        // surfaces from the actual DB connection attempt.
        let node = builder.task(TaskNode::<String>::new("some_task").queue("default"));
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

}
