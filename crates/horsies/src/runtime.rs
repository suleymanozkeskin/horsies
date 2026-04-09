use std::any::{type_name, Any, TypeId};
use std::collections::HashMap;
use std::sync::{Arc, OnceLock, RwLock};

use serde::de::DeserializeOwned;

use crate::core::task::error::OperationalErrorCode;
use crate::workflow::WorkflowStarter;
use crate::workflow_engine::bound_handle::WorkflowHandle;
use crate::{HorsiesError, TaskError, WorkflowSpec, WorkflowStartResult};

pub(crate) type StateValue = Arc<dyn Any + Send + Sync>;

#[derive(Debug)]
struct FrozenRuntimeCatalog {
    state: Arc<HashMap<TypeId, StateValue>>,
    task_handles: Arc<HashMap<String, StateValue>>,
}

#[derive(Debug)]
pub(crate) struct RuntimeCatalog {
    state_setup: RwLock<HashMap<TypeId, StateValue>>,
    task_handles_setup: RwLock<HashMap<String, StateValue>>,
    frozen: OnceLock<Result<Arc<FrozenRuntimeCatalog>, TaskError>>,
}

pub(crate) type SharedRuntimeCatalog = Arc<RuntimeCatalog>;

impl RuntimeCatalog {
    pub(crate) fn new() -> Self {
        Self {
            state_setup: RwLock::new(HashMap::new()),
            task_handles_setup: RwLock::new(HashMap::new()),
            frozen: OnceLock::new(),
        }
    }

    pub(crate) fn is_frozen(&self) -> bool {
        self.frozen.get().is_some()
    }

    pub(crate) fn provide_state<T>(&self, value: T) -> Result<(), HorsiesError>
    where
        T: Send + Sync + 'static,
    {
        if self.is_frozen() {
            return Err(HorsiesError::new(format!(
                "runtime state is frozen; cannot provide {} after task runtime use has begun",
                type_name::<T>()
            )));
        }

        let mut store = self
            .state_setup
            .write()
            .map_err(|_| HorsiesError::new("runtime state store is poisoned"))?;
        let type_id = TypeId::of::<T>();
        if store.contains_key(&type_id) {
            return Err(HorsiesError::new(format!(
                "runtime state for type {} is already provided; wrap values in a newtype if you need multiple instances",
                type_name::<T>()
            )));
        }
        store.insert(type_id, Arc::new(value));
        Ok(())
    }

    pub(crate) fn store_task_handle<A, T>(
        &self,
        handle: &crate::TaskFunction<A, T>,
    ) -> Result<(), HorsiesError>
    where
        A: serde::Serialize + 'static,
        T: DeserializeOwned + Clone + 'static,
    {
        if self.is_frozen() {
            return Err(HorsiesError::new(format!(
                "runtime task handles are frozen; cannot register {} after task runtime use has begun",
                handle.task_name()
            )));
        }

        let mut store = self
            .task_handles_setup
            .write()
            .map_err(|_| HorsiesError::new("task handle store is poisoned"))?;
        store.insert(handle.task_name().to_owned(), Arc::new(handle.clone()));
        Ok(())
    }

    fn frozen(&self) -> Result<Arc<FrozenRuntimeCatalog>, TaskError> {
        self.frozen
            .get_or_init(|| {
                let state = self.state_setup.read().map_err(|_| {
                    TaskError::builtin(
                        OperationalErrorCode::UnhandledError,
                        "runtime state store is poisoned while freezing task runtime",
                    )
                })?;
                let task_handles = self.task_handles_setup.read().map_err(|_| {
                    TaskError::builtin(
                        OperationalErrorCode::UnhandledError,
                        "task handle store is poisoned while freezing task runtime",
                    )
                })?;

                Ok(Arc::new(FrozenRuntimeCatalog {
                    state: Arc::new(state.clone()),
                    task_handles: Arc::new(task_handles.clone()),
                }))
            })
            .clone()
    }
}

/// Runtime capabilities made available to tasks by the horsies worker.
///
/// `TaskRuntime` covers the two primary task-time needs:
/// - start a dynamically-built workflow spec
/// - retrieve registered task handles and typed app-provided runtime state
#[derive(Clone)]
pub struct TaskRuntime {
    workflow_starter: WorkflowStarter,
    catalog: SharedRuntimeCatalog,
}

impl TaskRuntime {
    pub(crate) fn new(workflow_starter: WorkflowStarter, catalog: SharedRuntimeCatalog) -> Self {
        Self {
            workflow_starter,
            catalog,
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
        let frozen = self.catalog.frozen()?;

        let value = frozen.task_handles.get(task_name).cloned().ok_or_else(|| {
            TaskError::new(
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
        let frozen = self.catalog.frozen()?;

        let value = frozen
            .state
            .get(&TypeId::of::<T>())
            .cloned()
            .ok_or_else(|| {
                TaskError::new(
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

impl std::fmt::Debug for TaskRuntime {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TaskRuntime").finish()
    }
}
