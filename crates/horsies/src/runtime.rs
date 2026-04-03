use std::any::{type_name, Any, TypeId};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use serde::de::DeserializeOwned;

use crate::core::task::error::OperationalErrorCode;
use crate::workflow::WorkflowStarter;
use crate::workflow_engine::WorkflowHandle;
use crate::{TaskError, WorkflowSpec, WorkflowStartResult};

pub(crate) type StateValue = Arc<dyn Any + Send + Sync>;
pub(crate) type SharedTaskStateMap = Arc<RwLock<HashMap<TypeId, StateValue>>>;
pub(crate) type SharedTaskHandleMap = Arc<RwLock<HashMap<String, StateValue>>>;

/// Runtime capabilities made available to tasks by the horsies worker.
///
/// `TaskRuntime` covers the two primary task-time needs:
/// - start a dynamically-built workflow spec
/// - retrieve registered task handles and typed app-provided runtime state
#[derive(Clone)]
pub struct TaskRuntime {
    workflow_starter: WorkflowStarter,
    state: SharedTaskStateMap,
    task_handles: SharedTaskHandleMap,
}

impl TaskRuntime {
    pub(crate) fn new(
        workflow_starter: WorkflowStarter,
        state: SharedTaskStateMap,
        task_handles: SharedTaskHandleMap,
    ) -> Self {
        Self {
            workflow_starter,
            state,
            task_handles,
        }
    }

    /// Start a dynamically-built workflow spec from inside a running task.
    pub async fn start<T: DeserializeOwned + Clone>(
        &self,
        spec: WorkflowSpec,
    ) -> WorkflowStartResult<WorkflowHandle<T>> {
        self.workflow_starter.start(spec).await
    }

    /// Start a dynamically-built workflow with an explicit idempotent ID.
    pub async fn start_with_id<T: DeserializeOwned + Clone>(
        &self,
        spec: WorkflowSpec,
        workflow_id: impl Into<String>,
    ) -> WorkflowStartResult<WorkflowHandle<T>> {
        self.workflow_starter.start_with_id(spec, workflow_id).await
    }

    /// Access the underlying starter for advanced workflow-start operations.
    pub fn workflow_starter(&self) -> &WorkflowStarter {
        &self.workflow_starter
    }

    /// Retrieve an internally-registered typed task handle by task name.
    ///
    /// This powers the macro-generated `task_name::handle/send/schedule`
    /// helpers. Most users should prefer those generated helpers directly.
    #[doc(hidden)]
    pub fn task_handle<A, T>(&self, task_name: &str) -> Result<crate::TaskFunction<A, T>, TaskError>
    where
        A: serde::Serialize + 'static,
        T: DeserializeOwned + Clone + 'static,
    {
        let store = self.task_handles.read().map_err(|_| {
            TaskError::builtin(
                OperationalErrorCode::UnhandledError,
                format!(
                    "task runtime handle store is poisoned while retrieving {}",
                    task_name
                ),
            )
        })?;

        let value = store.get(task_name).cloned().ok_or_else(|| {
            TaskError::user(
                "TASK_HANDLE_NOT_REGISTERED",
                format!(
                    "task runtime does not contain a registered handle for {}",
                    task_name
                ),
            )
        })?;

        let typed = Arc::downcast::<crate::TaskFunction<A, T>>(value).map_err(|_| {
            TaskError::builtin(
                OperationalErrorCode::UnhandledError,
                format!(
                    "task runtime handle type mismatch while retrieving {}",
                    task_name
                ),
            )
        })?;

        Ok((*typed).clone())
    }

    /// Retrieve app-provided typed runtime state from inside a running task.
    pub fn state<T>(&self) -> Result<Arc<T>, TaskError>
    where
        T: Send + Sync + 'static,
    {
        let store = self.state.read().map_err(|_| {
            TaskError::builtin(
                OperationalErrorCode::UnhandledError,
                format!(
                    "task runtime state store is poisoned while retrieving {}",
                    type_name::<T>()
                ),
            )
        })?;

        let value = store.get(&TypeId::of::<T>()).cloned().ok_or_else(|| {
            TaskError::user(
                "STATE_NOT_PROVIDED",
                format!(
                    "task runtime is missing provided state of type {}",
                    type_name::<T>()
                ),
            )
        })?;

        Arc::downcast::<T>(value).map_err(|_| {
            TaskError::builtin(
                OperationalErrorCode::UnhandledError,
                format!(
                    "task runtime state type mismatch while retrieving {}",
                    type_name::<T>()
                ),
            )
        })
    }
}

impl From<TaskRuntime> for WorkflowStarter {
    fn from(value: TaskRuntime) -> Self {
        value.workflow_starter
    }
}

impl std::fmt::Debug for TaskRuntime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TaskRuntime").finish()
    }
}
