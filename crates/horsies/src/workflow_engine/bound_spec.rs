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
    listener_pool: PgPool,
    registry: Arc<WorkflowSpecRegistry>,
    resend_on_transient_err: bool,
    _phantom: PhantomData<T>,
}

impl<T: DeserializeOwned> BoundWorkflowSpec<T> {
    /// Create a bound workflow spec from a spec, pool, and registry.
    #[allow(dead_code)]
    pub fn new(
        spec: WorkflowSpec,
        pool: PgPool,
        registry: Arc<WorkflowSpecRegistry>,
        resend_on_transient_err: bool,
    ) -> Self {
        Self::new_with_listener_pool(spec, pool.clone(), pool, registry, resend_on_transient_err)
    }

    /// Create a bound workflow spec with a separate session-capable pool for
    /// workflow result LISTEN/NOTIFY.
    pub fn new_with_listener_pool(
        spec: WorkflowSpec,
        pool: PgPool,
        listener_pool: PgPool,
        registry: Arc<WorkflowSpecRegistry>,
        resend_on_transient_err: bool,
    ) -> Self {
        Self {
            spec,
            pool,
            listener_pool,
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
        Self::new_with_listener_pool(
            spec,
            broker.pool().clone(),
            broker.session_pool().clone(),
            registry,
            resend_on_transient_err,
        )
    }

    /// Start the workflow with an auto-generated workflow ID.
    pub async fn start(&self) -> WorkflowStartResult<WorkflowHandle<T>> {
        crate::workflow_engine::start::start_workflow_with_retry_and_listener_pool::<T>(
            &self.pool,
            &self.listener_pool,
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
        crate::workflow_engine::start::start_workflow_with_retry_and_listener_pool::<T>(
            &self.pool,
            &self.listener_pool,
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
        crate::workflow_engine::start::retry_start_with_listener_pool::<T>(
            &self.pool,
            &self.listener_pool,
            &self.spec,
            error,
            &self.registry,
        )
        .await
    }

    /// Reconnect to an already-known workflow ID using the bound resources.
    pub fn handle(&self, workflow_id: impl Into<String>) -> WorkflowHandle<T> {
        WorkflowHandle::new_with_listener_pool(
            workflow_id.into(),
            self.pool.clone(),
            self.listener_pool.clone(),
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

#[cfg(test)]
mod tests {
    use super::*;

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
