use std::marker::PhantomData;
use std::sync::Arc;

use serde::de::DeserializeOwned;

use crate::broker::PostgresBroker;
use crate::core::config::payload::PayloadPolicy;
use crate::core::registry::workflow::WorkflowSpecRegistry;
use crate::core::{WorkflowSpec, WorkflowStartError, WorkflowStartResult};

use crate::workflow_engine::bound_handle::WorkflowHandle;

/// A low-level executable wrapper around a definition-only workflow spec.
///
/// It closes over the broker, workflow registry, and retry policy so callers
/// can invoke `.start()` / `.retry_start()` directly without threading those
/// dependencies through every call.
#[derive(Clone)]
pub struct BoundWorkflowSpec<T> {
    spec: WorkflowSpec,
    broker: Arc<PostgresBroker>,
    registry: Arc<WorkflowSpecRegistry>,
    resend_on_transient_err: bool,
    payload: PayloadPolicy,
    _phantom: PhantomData<T>,
}

impl<T: DeserializeOwned> BoundWorkflowSpec<T> {
    /// Create a bound workflow spec from a broker.
    ///
    /// Uses the broker's main pool for start/retry/handle operations and its
    /// process-wide shared listener for result waits (P2).
    pub fn from_broker(
        spec: WorkflowSpec,
        broker: Arc<PostgresBroker>,
        registry: Arc<WorkflowSpecRegistry>,
        resend_on_transient_err: bool,
        payload: PayloadPolicy,
    ) -> Self {
        Self {
            spec,
            broker,
            registry,
            resend_on_transient_err,
            payload,
            _phantom: PhantomData,
        }
    }

    /// Start the workflow with an auto-generated workflow ID.
    pub async fn start(&self) -> WorkflowStartResult<WorkflowHandle<T>> {
        crate::workflow_engine::start::start_workflow_with_retry::<T>(
            &self.broker,
            &self.spec,
            None,
            &self.registry,
            self.resend_on_transient_err,
            &self.payload,
        )
        .await
    }

    /// Start the workflow with a caller-provided workflow ID.
    pub async fn start_with_id(
        &self,
        workflow_id: impl Into<String>,
    ) -> WorkflowStartResult<WorkflowHandle<T>> {
        crate::workflow_engine::start::start_workflow_with_retry::<T>(
            &self.broker,
            &self.spec,
            Some(workflow_id.into()),
            &self.registry,
            self.resend_on_transient_err,
            &self.payload,
        )
        .await
    }

    /// Retry a failed workflow start using the stored workflow resources.
    pub async fn retry_start(
        &self,
        error: &WorkflowStartError,
    ) -> WorkflowStartResult<WorkflowHandle<T>> {
        crate::workflow_engine::start::retry_start::<T>(
            &self.broker,
            &self.spec,
            error,
            &self.registry,
            &self.payload,
        )
        .await
    }

    /// Reconnect to an already-known workflow ID using the bound resources.
    pub fn handle(&self, workflow_id: impl Into<String>) -> WorkflowHandle<T> {
        WorkflowHandle::new(
            workflow_id.into(),
            Arc::clone(&self.broker),
            Arc::clone(&self.registry),
            self.payload.clone(),
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
            broker: Arc<PostgresBroker>,
            registry: Arc<WorkflowSpecRegistry>,
        ) {
            let _bound: BoundWorkflowSpec<String> =
                BoundWorkflowSpec::from_broker(spec, broker, registry, true, PayloadPolicy::default());
        }

        let _ = _assert_from_broker;
    }
}
