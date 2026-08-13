use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use serde::de::DeserializeOwned;
use uuid::Uuid;

use crate::broker::PostgresBroker;
use crate::core::config::payload::PayloadPolicy;
use crate::core::config::retention::RetentionConfig;
use crate::core::registry::workflow::WorkflowSpecRegistry;
use crate::core::task::{TaskError, TaskResult};
use crate::core::workflow::handle_types::{HandleErrorCode, HandleOperationError, HandleResult};
use crate::core::{OperationalErrorCode, RetrievalCode, WorkflowStatus};

use crate::workflow_engine::error::WorkflowError;
use crate::workflow_engine::info::WorkflowTaskInfo;

/// A workflow handle bound to the runtime resources needed for direct
/// operations.
///
/// This is the canonical user-facing workflow handle in Rust, mirroring
/// Python's single `WorkflowHandle` concept.
///
/// Error handling follows a split strategy:
///
/// - `get()` and `result_for()` return `TaskResult<T>` and fold
///   infrastructure/query failures into `TaskResult::Err(TaskError)`.
/// - `status()`, `results()`, `tasks()`, `cancel()`, `pause()`, and
///   `resume()` return `HandleResult<_>`.
pub struct WorkflowHandle<T> {
    workflow_id: Uuid,
    broker: Arc<PostgresBroker>,
    registry: Arc<WorkflowSpecRegistry>,
    payload: PayloadPolicy,
    retention: RetentionConfig,
    _phantom: std::marker::PhantomData<T>,
}

impl<T> WorkflowHandle<T> {
    /// Create a new workflow handle for a known workflow ID.
    ///
    /// Queries run on the broker's main pool; result waits (`get`) reuse the
    /// broker's process-wide `workflow_done_listener`, so many concurrently
    /// waiting handles share one LISTEN connection instead of pinning one each
    /// (P2).
    pub fn new(
        workflow_id: Uuid,
        broker: Arc<PostgresBroker>,
        registry: Arc<WorkflowSpecRegistry>,
        payload: PayloadPolicy,
        retention: RetentionConfig,
    ) -> Self {
        Self {
            workflow_id,
            broker,
            registry,
            payload,
            retention,
            _phantom: std::marker::PhantomData,
        }
    }

    /// The workflow instance ID.
    pub fn workflow_id(&self) -> Uuid {
        self.workflow_id
    }
}

impl<T: DeserializeOwned> WorkflowHandle<T> {
    /// Wait for the workflow to complete and return the typed result.
    ///
    /// Mirrors Python's fold strategy: task/workflow outcome and
    /// infrastructure retrieval failures are both returned as `TaskResult`.
    ///
    /// Typical call shape:
    ///
    /// ```ignore
    /// let result = handle.get(Some(Duration::from_secs(60))).await;
    /// match result {
    ///     TaskResult::Ok(value) => { /* completed successfully */ }
    ///     TaskResult::Err(err) => { /* task/workflow/retrieval/infra error */ }
    /// }
    /// ```
    pub async fn get(&self, timeout: Option<Duration>) -> TaskResult<T> {
        // Reuse the broker's process-wide shared listener instead of pinning a
        // per-handle LISTEN connection (P2).
        let listener = match self.broker.workflow_done_listener().await {
            Ok(listener) => listener,
            Err(e) => return self.fold_task_result_error(&WorkflowError::Broker(e)),
        };

        match crate::workflow_engine::query::get_workflow_result::<T>(
            self.broker.pool(),
            listener,
            self.workflow_id,
            timeout,
        )
        .await
        {
            Ok(result) => result,
            Err(e) => self.fold_task_result_error(&e),
        }
    }

    /// Get the current workflow status.
    ///
    /// This is a wrap-strategy method: infrastructure/query failures are
    /// returned as `HandleResult::Err(HandleOperationError)`.
    pub async fn status(&self) -> HandleResult<WorkflowStatus> {
        crate::workflow_engine::query::get_workflow_status(self.broker.pool(), self.workflow_id)
            .await
            .map_err(|e| self.wrap_error(&e))
    }

    /// Get all task results keyed by node_id.
    ///
    /// This is a wrap-strategy method: DB/query failures return
    /// `HandleResult::Err(...)`.
    pub async fn results(&self) -> HandleResult<HashMap<String, TaskResult<serde_json::Value>>> {
        crate::workflow_engine::query::get_workflow_results(self.broker.pool(), self.workflow_id)
            .await
            .map_err(|e| self.wrap_error(&e))
    }

    /// Get a single node's result by node_id.
    ///
    /// This is a fold-strategy method like `get()`: missing workflow,
    /// not-ready results, and infrastructure/query failures are returned as
    /// `TaskResult::Err(TaskError)`.
    pub async fn result_for<V: DeserializeOwned>(&self, node_id: &str) -> TaskResult<V> {
        match crate::workflow_engine::query::get_workflow_result_for(
            self.broker.pool(),
            self.workflow_id,
            node_id,
        )
        .await
        {
            Ok(result) => result,
            Err(e) => self.fold_task_result_error(&e),
        }
    }

    /// Get a single node's result using a typed `NodeKey<V>`.
    pub async fn result_for_key<V: DeserializeOwned>(
        &self,
        key: &crate::core::NodeKey<V>,
    ) -> TaskResult<V> {
        self.result_for(key.node_id()).await
    }

    /// Get task info for all workflow tasks.
    ///
    /// This is a wrap-strategy method: DB/query failures return
    /// `HandleResult::Err(...)`.
    pub async fn tasks(&self) -> HandleResult<Vec<WorkflowTaskInfo>> {
        crate::workflow_engine::query::get_workflow_tasks(self.broker.pool(), self.workflow_id)
            .await
            .map_err(|e| self.wrap_error(&e))
    }

    /// Cancel the workflow.
    ///
    /// This is a wrap-strategy method.
    pub async fn cancel(&self) -> HandleResult<()> {
        crate::workflow_engine::lifecycle::cancel_workflow(self.broker.pool(), self.workflow_id)
            .await
            .map(|_| ())
    }

    /// Pause the workflow (RUNNING -> PAUSED).
    ///
    /// This is a wrap-strategy method.
    pub async fn pause(&self) -> HandleResult<bool> {
        crate::workflow_engine::lifecycle::pause_workflow(self.broker.pool(), self.workflow_id)
            .await
    }

    /// Resume the workflow (PAUSED -> RUNNING).
    ///
    /// This is a wrap-strategy method.
    pub async fn resume(&self) -> HandleResult<bool> {
        crate::workflow_engine::lifecycle::resume_workflow(
            self.broker.pool(),
            self.workflow_id,
            &self.registry,
            &self.payload,
            &self.retention,
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

    fn fold_task_result_error<V>(&self, e: &WorkflowError) -> TaskResult<V> {
        match e {
            WorkflowError::WorkflowNotFound { .. } => TaskResult::Err(TaskError::builtin(
                RetrievalCode::WorkflowNotFound,
                format!("workflow {} not found", self.workflow_id),
            )),
            _ => TaskResult::Err(TaskError::builtin(
                OperationalErrorCode::BrokerError,
                e.to_string(),
            )),
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
    use crate::core::task::{BuiltInTaskCode, TaskErrorCode};
    use crate::workflow_engine::error::WorkflowError;

    #[test]
    fn workflow_handle_implements_debug() {
        fn assert_debug<T: std::fmt::Debug>() {}
        assert_debug::<WorkflowHandle<serde_json::Value>>();
    }

    #[tokio::test]
    async fn fold_task_result_error_maps_missing_workflow_to_retrieval_error() {
        let handle = WorkflowHandle::<serde_json::Value>::new(
            Uuid::new_v4(),
            Arc::new(PostgresBroker::from_pool(
                sqlx::PgPool::connect_lazy("postgresql://localhost/test").expect("lazy pool"),
            )),
            Arc::new(WorkflowSpecRegistry::new()),
            PayloadPolicy::default(),
            RetentionConfig::default(),
        );

        let result =
            handle.fold_task_result_error::<serde_json::Value>(&WorkflowError::WorkflowNotFound {
                workflow_id: Uuid::new_v4(),
            });

        match result {
            TaskResult::Ok(_) => panic!("expected folded error"),
            TaskResult::Err(err) => {
                assert_eq!(
                    err.error_code,
                    Some(TaskErrorCode::BuiltIn(BuiltInTaskCode::Retrieval(
                        RetrievalCode::WorkflowNotFound,
                    )))
                );
            }
        }
    }

    #[tokio::test]
    async fn fold_task_result_error_maps_db_failures_to_broker_error() {
        let handle = WorkflowHandle::<serde_json::Value>::new(
            Uuid::new_v4(),
            Arc::new(PostgresBroker::from_pool(
                sqlx::PgPool::connect_lazy("postgresql://localhost/test").expect("lazy pool"),
            )),
            Arc::new(WorkflowSpecRegistry::new()),
            PayloadPolicy::default(),
            RetentionConfig::default(),
        );

        let result = handle.fold_task_result_error::<serde_json::Value>(
            &WorkflowError::Validation("boom".to_owned()),
        );

        match result {
            TaskResult::Ok(_) => panic!("expected folded error"),
            TaskResult::Err(err) => {
                assert_eq!(
                    err.error_code,
                    Some(TaskErrorCode::BuiltIn(BuiltInTaskCode::Operational(
                        OperationalErrorCode::BrokerError,
                    )))
                );
            }
        }
    }
}

#[cfg(test)]
mod shared_listener_tests {
    //! P2: many concurrently-waiting `WorkflowHandle`s must share the broker's
    //! one process-wide listener, not pin a session-pool connection each. With
    //! per-handle listeners, more handles than SESSION_POOL_MAX_CONNECTIONS(4)
    //! exhaust the session pool and `get()` folds a broker error; the shared
    //! listener lets all of them time out cleanly instead.
    use super::*;
    use crate::broker::PostgresBroker;
    use crate::core::task::{BuiltInTaskCode, TaskErrorCode};
    use serial_test::serial;
    use uuid::Uuid;

    #[tokio::test]
    #[serial]
    async fn many_waiting_handles_share_one_listener() {
        let broker = Arc::new(PostgresBroker::from_pool(
            crate::broker::terminalization_matrix::migrated_pool().await,
        ));
        let pool = broker.pool().clone();
        let registry = Arc::new(WorkflowSpecRegistry::new());
        let wf_id = Uuid::new_v4();

        // A RUNNING workflow that never completes (a RUNNING, non-terminal node).
        sqlx::query(
            "INSERT INTO horsies_workflows (
                id, name, status, on_error, output_task_index, definition_key, depth,
                root_workflow_id, sent_at, created_at, started_at, updated_at
            ) VALUES ($1, 'p2_wf', 'RUNNING', 'fail', NULL, 'test.p2.v1', 0, $1,
                      NOW(), NOW(), NOW(), NOW())",
        )
        .bind(&wf_id)
        .execute(&pool)
        .await
        .expect("insert workflow");

        // 8 concurrent waiters — twice the session-pool connection limit.
        let mut futures = Vec::new();
        for _ in 0..8 {
            let handle = WorkflowHandle::<serde_json::Value>::new(
                wf_id.clone(),
                Arc::clone(&broker),
                Arc::clone(&registry),
                PayloadPolicy::default(),
                RetentionConfig::default(),
            );
            futures.push(async move { handle.get(Some(Duration::from_millis(400))).await });
        }
        let results = futures::future::join_all(futures).await;

        for result in results {
            match result {
                TaskResult::Err(err) => assert_eq!(
                    err.error_code,
                    Some(TaskErrorCode::BuiltIn(BuiltInTaskCode::Retrieval(
                        RetrievalCode::WaitTimeout,
                    ))),
                    "each waiter must time out via the shared listener, not fail on pool exhaustion: {:?}",
                    err.error_code,
                ),
                TaskResult::Ok(_) => panic!("workflow must not complete"),
            }
        }

        sqlx::query("DELETE FROM horsies_workflows WHERE id = $1")
            .bind(&wf_id)
            .execute(&pool)
            .await
            .ok();
    }
}
