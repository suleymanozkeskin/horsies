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

/// Runtime capabilities made available to tasks by the horsies worker.
///
/// `TaskRuntime` covers the two primary task-time needs:
/// - start a dynamically-built workflow spec
/// - retrieve typed app-provided runtime state such as task-handle groups
#[derive(Clone)]
pub struct TaskRuntime {
    workflow_starter: WorkflowStarter,
    state: SharedTaskStateMap,
}

impl TaskRuntime {
    pub(crate) fn new(workflow_starter: WorkflowStarter, state: SharedTaskStateMap) -> Self {
        Self {
            workflow_starter,
            state,
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
