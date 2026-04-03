use std::marker::PhantomData;
use std::sync::Arc;

use serde::de::DeserializeOwned;
use sqlx::PgPool;

use crate::broker::PostgresBroker;
use crate::core::registry::workflow::WorkflowSpecRegistry;
use crate::core::{WorkflowSpec, WorkflowStartError, WorkflowStartResult};

use crate::workflow_engine::bound_handle::WorkflowHandle;

/// A low-level executable wrapper around a definition-only workflow spec.
///
/// It closes over the database pool, workflow registry, and retry policy so
/// callers can invoke `.start()` / `.retry_start()` directly without
/// threading those dependencies through every call.
#[derive(Clone)]
pub struct BoundWorkflowSpec<T> {
    spec: WorkflowSpec,
    pool: PgPool,
    registry: Arc<WorkflowSpecRegistry>,
    resend_on_transient_err: bool,
    _phantom: PhantomData<T>,
}

impl<T: DeserializeOwned> BoundWorkflowSpec<T> {
    /// Create a bound workflow spec from a spec, pool, and registry.
    pub fn new(
        spec: WorkflowSpec,
        pool: PgPool,
        registry: Arc<WorkflowSpecRegistry>,
        resend_on_transient_err: bool,
    ) -> Self {
        Self {
            spec,
            pool,
            registry,
            resend_on_transient_err,
            _phantom: PhantomData,
        }
    }

    /// Create a bound workflow spec from a broker.
    ///
    /// Uses the broker's underlying pool for start/retry/handle operations.
    pub fn from_broker(
        spec: WorkflowSpec,
        broker: &PostgresBroker,
        registry: Arc<WorkflowSpecRegistry>,
        resend_on_transient_err: bool,
    ) -> Self {
        Self::new(
            spec,
            broker.pool().clone(),
            registry,
            resend_on_transient_err,
        )
    }

    /// The underlying immutable workflow spec.
    pub fn spec(&self) -> &WorkflowSpec {
        &self.spec
    }

    /// Consume the bound wrapper and return the inner workflow spec.
    pub fn into_spec(self) -> WorkflowSpec {
        self.spec
    }

    /// The bound database pool.
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// The bound workflow registry.
    pub fn registry(&self) -> &Arc<WorkflowSpecRegistry> {
        &self.registry
    }

    /// Whether workflow start should auto-retry transient enqueue failures.
    pub fn resend_on_transient_err(&self) -> bool {
        self.resend_on_transient_err
    }

    /// Start the workflow with an auto-generated workflow ID.
    pub async fn start(&self) -> WorkflowStartResult<WorkflowHandle<T>> {
        crate::workflow_engine::start::start_workflow_with_retry::<T>(
            &self.pool,
            &self.spec,
            None,
            &self.registry,
            self.resend_on_transient_err,
        )
        .await
    }

    /// Start the workflow with a caller-provided workflow ID.
    pub async fn start_with_id(
        &self,
        workflow_id: impl Into<String>,
    ) -> WorkflowStartResult<WorkflowHandle<T>> {
        crate::workflow_engine::start::start_workflow_with_retry::<T>(
            &self.pool,
            &self.spec,
            Some(workflow_id.into()),
            &self.registry,
            self.resend_on_transient_err,
        )
        .await
    }

    /// Retry a failed workflow start using the stored workflow resources.
    pub async fn retry_start(
        &self,
        error: &WorkflowStartError,
    ) -> WorkflowStartResult<WorkflowHandle<T>> {
        crate::workflow_engine::start::retry_start::<T>(
            &self.pool,
            &self.spec,
            error,
            &self.registry,
        )
        .await
    }

    /// Reconnect to an already-known workflow ID using the bound resources.
    pub fn handle(&self, workflow_id: impl Into<String>) -> WorkflowHandle<T> {
        WorkflowHandle::new(
            workflow_id.into(),
            self.pool.clone(),
            std::sync::Arc::clone(&self.registry),
        )
    }
}

impl<T> std::fmt::Debug for BoundWorkflowSpec<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BoundWorkflowSpec")
            .field("name", &self.spec.name)
            .field("definition_key", &self.spec.definition_key)
            .field("resend_on_transient_err", &self.resend_on_transient_err)
            .finish()
    }
}

/// Extension trait that binds a definition-only `WorkflowSpec` to runtime resources.
///
/// This keeps the internal core workflow types IO-free while still enabling
/// the advanced `spec.bind(...).start()` flow when needed.
pub trait WorkflowSpecExt {
    /// Bind a workflow spec to a pool and workflow registry.
    fn bind<T: DeserializeOwned>(
        &self,
        pool: PgPool,
        registry: Arc<WorkflowSpecRegistry>,
        resend_on_transient_err: bool,
    ) -> BoundWorkflowSpec<T>;

    /// Bind a workflow spec using a broker's underlying pool.
    fn bind_with_broker<T: DeserializeOwned>(
        &self,
        broker: &PostgresBroker,
        registry: Arc<WorkflowSpecRegistry>,
        resend_on_transient_err: bool,
    ) -> BoundWorkflowSpec<T>;
}

impl WorkflowSpecExt for WorkflowSpec {
    fn bind<T: DeserializeOwned>(
        &self,
        pool: PgPool,
        registry: Arc<WorkflowSpecRegistry>,
        resend_on_transient_err: bool,
    ) -> BoundWorkflowSpec<T> {
        BoundWorkflowSpec::new(self.clone(), pool, registry, resend_on_transient_err)
    }

    fn bind_with_broker<T: DeserializeOwned>(
        &self,
        broker: &PostgresBroker,
        registry: Arc<WorkflowSpecRegistry>,
        resend_on_transient_err: bool,
    ) -> BoundWorkflowSpec<T> {
        BoundWorkflowSpec::from_broker(self.clone(), broker, registry, resend_on_transient_err)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn workflow_spec_ext_bind_contract() {
        fn _assert_bind(spec: &WorkflowSpec, pool: PgPool, registry: Arc<WorkflowSpecRegistry>) {
            let _bound: BoundWorkflowSpec<String> = spec.bind(pool, registry, true);
        }

        let _ = _assert_bind;
    }

    #[test]
    fn workflow_spec_ext_bind_with_broker_contract() {
        fn _assert_bind_with_broker(
            spec: &WorkflowSpec,
            broker: &PostgresBroker,
            registry: Arc<WorkflowSpecRegistry>,
        ) {
            let _bound: BoundWorkflowSpec<String> = spec.bind_with_broker(broker, registry, true);
        }

        let _ = _assert_bind_with_broker;
    }

    #[test]
    fn bound_workflow_spec_from_broker_contract() {
        fn _assert_from_broker(
            spec: WorkflowSpec,
            broker: &PostgresBroker,
            registry: Arc<WorkflowSpecRegistry>,
        ) {
            let _bound: BoundWorkflowSpec<String> =
                BoundWorkflowSpec::from_broker(spec, broker, registry, true);
        }

        let _ = _assert_from_broker;
    }
}
