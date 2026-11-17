//! Starting a workflow (Python `WorkflowSpec.start`, `start_workflow`).
//!
//! A start, in order: the definition key of the spec (`VALIDATION_FAILED`
//! without one), the output type against the handle's type, the spec
//! checked against the app's registered definitions and converted to the
//! core's spec data, the connect of the handle, then the core's start of
//! the tree. With `AppConfig.resend_on_transient_err` the core runs a
//! retryable failure of the start again, with the same workflow id, up to 3
//! times. In a worker child the start goes through the child's producer
//! handle, as a send does.

use std::sync::Arc;

use horsies_core::core::workflow::validate_start_retry;
use horsies_core::handle::CoreHandle;
use horsies_core::{
    child_producer, start_workflow_with_retry, ChildProducer, WorkflowStartError,
    WorkflowStartErrorCode,
};
use uuid::Uuid;

use super::handle::WorkflowHandle;
use super::registry::WorkflowRegistry;
use super::spec::WorkflowSpec;
use crate::{Horsies, HorsiesError};

/// What a start sends with: the app's handle, its workflow registry and
/// its resend setting. Clones share one app.
#[derive(Clone)]
pub(crate) struct WorkflowStarter {
    handle: Arc<CoreHandle>,
    workflows: Arc<WorkflowRegistry>,
}

impl std::fmt::Debug for WorkflowStarter {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("WorkflowStarter")
            .finish_non_exhaustive()
    }
}

impl WorkflowStarter {
    /// The starter of `app`. No I/O.
    pub(crate) fn of(app: &Horsies) -> Self {
        Self {
            handle: Arc::clone(app.handle()),
            workflows: Arc::clone(app.workflows()),
        }
    }

    /// The handle that a start and its reads go through: the child's
    /// producer handle in a worker child, else the app's.
    fn operation_handle(&self) -> Arc<CoreHandle> {
        match child_producer() {
            ChildProducer::InChild(handle) => handle,
            ChildProducer::NotInAChild => Arc::clone(&self.handle),
        }
    }

    /// The handle of `workflow_id` with the result type `T`. No I/O.
    pub(crate) fn handle_of<T>(&self, workflow_id: Uuid) -> WorkflowHandle<T> {
        WorkflowHandle::new(
            workflow_id,
            self.operation_handle(),
            Arc::clone(&self.workflows),
        )
    }

    /// Start `spec` as the workflow `workflow_id`, with the result type `T`.
    ///
    /// Outcomes: the handle of the new workflow, or of the workflow that
    /// already has this id and this name (nothing written).
    ///
    /// Errors: `VALIDATION_FAILED` for a spec without a definition key, an
    /// output type that is not `T`, and a spec that the registry or the
    /// conversion refuses, before any I/O; `WORKFLOW_ID_CONFLICT` when the
    /// id belongs to another workflow name (nothing written);
    /// `ENQUEUE_FAILED` with `retryable` for a connect, a schema install or
    /// a start that failed (nothing committed, or the commit reply was
    /// lost: a start again with the same id finds the workflow).
    pub(crate) async fn start<T: 'static>(
        &self,
        spec: &WorkflowSpec,
        workflow_id: Uuid,
    ) -> Result<WorkflowHandle<T>, WorkflowStartError> {
        let refused =
            |code: WorkflowStartErrorCode, message: String, retryable: bool| WorkflowStartError {
                code,
                message,
                retryable,
                workflow_name: spec.name().to_owned(),
                workflow_id: Some(workflow_id),
            };
        let validation = |error: HorsiesError| {
            refused(
                WorkflowStartErrorCode::ValidationFailed,
                error.to_string(),
                false,
            )
        };
        if spec
            .definition_key()
            .map(str::trim)
            .unwrap_or_default()
            .is_empty()
        {
            return Err(refused(
                WorkflowStartErrorCode::ValidationFailed,
                format!(
                    "WorkflowSpec is missing definition_key: workflow '{}' has no stable \
                     definition_key; set a definition key such as 'your.workflow.v1'",
                    spec.name()
                ),
                false,
            ));
        }
        spec.check_output_type::<T>().map_err(validation)?;
        let client = self.workflows.prepare(spec).map_err(validation)?;
        let handle = self.operation_handle();
        let broker = handle.initialized_broker().await.map_err(|error| {
            refused(
                WorkflowStartErrorCode::EnqueueFailed,
                format!("Schema initialization failed: {error}"),
                error.is_retryable(),
            )
        })?;
        let config = self.workflows.config();
        let started = start_workflow_with_retry::<serde_json::Value>(
            &broker,
            client.spec(),
            Some(workflow_id),
            &self.workflows.core_registry(),
            config.resend_on_transient_err,
            &config.payload,
            &config.retention,
        )
        .await?;
        Ok(WorkflowHandle::new(
            started.workflow_id(),
            handle,
            Arc::clone(&self.workflows),
        ))
    }

    /// Start `spec` again after `error`, with its workflow id (Python
    /// `retry_start`).
    ///
    /// Errors: `VALIDATION_FAILED` for an error that is not
    /// `ENQUEUE_FAILED`, an error of another workflow name, and an error
    /// without a workflow id; else the errors of [`Self::start`].
    pub(crate) async fn retry_start<T: 'static>(
        &self,
        spec: &WorkflowSpec,
        error: &WorkflowStartError,
    ) -> Result<WorkflowHandle<T>, WorkflowStartError> {
        let workflow_id = validate_start_retry(error, spec.name())?;
        self.start(spec, workflow_id).await
    }
}

/// A new workflow id.
pub(crate) fn new_workflow_id() -> Uuid {
    Uuid::new_v4()
}
