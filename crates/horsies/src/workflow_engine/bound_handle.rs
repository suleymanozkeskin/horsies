use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use serde::de::DeserializeOwned;
use sqlx::PgPool;

use crate::broker::SharedNotifyListener;
use crate::core::registry::workflow::WorkflowSpecRegistry;
use crate::core::task::result::TaskResult;
use crate::core::workflow::handle_types::{HandleErrorCode, HandleOperationError, HandleResult};
use crate::core::WorkflowStatus;

use crate::workflow_engine::error::WorkflowError;
use crate::workflow_engine::info::WorkflowTaskInfo;

/// A workflow handle bound to the runtime resources needed for direct
/// operations.
///
/// This is the canonical user-facing workflow handle in Rust, mirroring
/// Python's single `WorkflowHandle` concept.
pub struct WorkflowHandle<T> {
    workflow_id: String,
    pool: PgPool,
    registry: Arc<WorkflowSpecRegistry>,
    listener: tokio::sync::OnceCell<SharedNotifyListener>,
    _phantom: std::marker::PhantomData<T>,
}

impl<T> WorkflowHandle<T> {
    /// Create a new workflow handle for a known workflow ID.
    pub fn new(workflow_id: String, pool: PgPool, registry: Arc<WorkflowSpecRegistry>) -> Self {
        Self {
            workflow_id,
            pool,
            registry,
            listener: tokio::sync::OnceCell::new(),
            _phantom: std::marker::PhantomData,
        }
    }

    /// The workflow instance ID.
    pub fn workflow_id(&self) -> &str {
        &self.workflow_id
    }
}

impl<T: DeserializeOwned> WorkflowHandle<T> {
    /// Wait for the workflow to complete and return the typed result.
    pub async fn get(&self, timeout: Option<Duration>) -> HandleResult<TaskResult<T>> {
        let listener = self
            .listener
            .get_or_try_init(|| SharedNotifyListener::new(&self.pool, "workflow_done"))
            .await
            .map_err(|e| self.wrap_error(&WorkflowError::Broker(e)))?;

        crate::workflow_engine::query::get_workflow_result::<T>(
            &self.pool,
            listener,
            &self.workflow_id,
            timeout,
        )
        .await
        .map_err(|e| self.wrap_error(&e))
    }

    /// Get the current workflow status.
    pub async fn status(&self) -> HandleResult<WorkflowStatus> {
        crate::workflow_engine::query::get_workflow_status(&self.pool, &self.workflow_id)
            .await
            .map_err(|e| self.wrap_error(&e))
    }

    /// Get all task results keyed by node_id.
    pub async fn results(&self) -> HandleResult<HashMap<String, TaskResult<serde_json::Value>>> {
        crate::workflow_engine::query::get_workflow_results(&self.pool, &self.workflow_id)
            .await
            .map_err(|e| self.wrap_error(&e))
    }

    /// Get a single node's result by node_id.
    pub async fn result_for<V: DeserializeOwned>(
        &self,
        node_id: &str,
    ) -> HandleResult<TaskResult<V>> {
        crate::workflow_engine::query::get_workflow_result_for(
            &self.pool,
            &self.workflow_id,
            node_id,
        )
        .await
        .map_err(|e| self.wrap_error(&e))
    }

    /// Get a single node's result using a typed `NodeKey<V>`.
    pub async fn result_for_key<V: DeserializeOwned>(
        &self,
        key: &crate::core::NodeKey<V>,
    ) -> HandleResult<TaskResult<V>> {
        self.result_for(key.node_id()).await
    }

    /// Get task info for all workflow tasks.
    pub async fn tasks(&self) -> HandleResult<Vec<WorkflowTaskInfo>> {
        crate::workflow_engine::query::get_workflow_tasks(&self.pool, &self.workflow_id)
            .await
            .map_err(|e| self.wrap_error(&e))
    }

    /// Cancel the workflow.
    pub async fn cancel(&self) -> HandleResult<()> {
        crate::workflow_engine::lifecycle::cancel_workflow(&self.pool, &self.workflow_id)
            .await
            .map(|_| ())
    }

    /// Pause the workflow (RUNNING -> PAUSED).
    pub async fn pause(&self) -> HandleResult<bool> {
        crate::workflow_engine::lifecycle::pause_workflow(&self.pool, &self.workflow_id).await
    }

    /// Resume the workflow (PAUSED -> RUNNING).
    pub async fn resume(&self) -> HandleResult<bool> {
        crate::workflow_engine::lifecycle::resume_workflow(
            &self.pool,
            &self.workflow_id,
            &self.registry,
        )
        .await
    }

    fn wrap_error(&self, e: &WorkflowError) -> HandleOperationError {
        let (code, retryable) = match &e {
            WorkflowError::WorkflowNotFound { .. } => (HandleErrorCode::WorkflowNotFound, false),
            WorkflowError::Database(_) | WorkflowError::Broker(_) => {
                (HandleErrorCode::DbOperationFailed, true)
            }
            _ => (HandleErrorCode::InternalFailed, false),
        };
        HandleOperationError {
            code,
            message: e.to_string(),
            retryable,
            workflow_id: self.workflow_id.clone(),
        }
    }
}

impl<T> std::fmt::Debug for WorkflowHandle<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WorkflowHandle")
            .field("workflow_id", &self.workflow_id)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn workflow_handle_implements_debug() {
        fn assert_debug<T: std::fmt::Debug>() {}
        assert_debug::<WorkflowHandle<serde_json::Value>>();
    }
}
