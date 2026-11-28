//! The typed functions that an app's task attributes registered, by task
//! name: the generated sends (`name::send(&app, ...)`), the generated
//! `name::function(&runtime)`, `TaskRuntime::task_function` and the task nodes
//! of the app's workflow builders read them.
//!
//! Each app has its own catalog, and no catalog is process-wide: a send, a
//! handle and a node find a task only in the app that they name.

use std::any::{type_name, Any, TypeId};
use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard};

use horsies_core::TaskOptions;
use serde::de::DeserializeOwned;
use serde::Serialize;

use super::function::TaskFunction;
use crate::workflow::NodeRegistration;
use crate::{CustomErrorCode, TaskNode};

/// Why the app gives no typed function, send or node for a task name.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum TaskFunctionNotFound {
    /// The app registered no task of this name through `#[horsies::task]`
    /// (or `TaskFunction::__registered`).
    #[error("task '{task_name}' has no typed function on this app; register it in main")]
    NotRegistered { task_name: String },
    /// The task was registered with other input, output or code types.
    #[error("task '{task_name}' is registered as {registered}, not {asked}")]
    OtherTypes {
        task_name: String,
        registered: &'static str,
        asked: &'static str,
    },
}

/// One stored function, the name of its type, the input and output types
/// of its task, and what a workflow node of its task takes from the
/// registration.
struct StoredFunction {
    function: Arc<dyn Any + Send + Sync>,
    type_name: &'static str,
    input: TypeId,
    output: TypeId,
    node: NodeRegistration,
}

/// The typed functions of one app. The app, its task runtime and its
/// workflow builders share it.
pub(crate) struct TaskFunctions {
    functions: Mutex<HashMap<String, StoredFunction>>,
}

impl TaskFunctions {
    pub(crate) fn new() -> Self {
        Self {
            functions: Mutex::new(HashMap::new()),
        }
    }

    /// Keep `function` as the function of `task_name`, with the queue, the
    /// options and the workflow context flag of its registration for the
    /// task nodes. The app refuses a repeated task name before this, so an
    /// earlier entry is replaced only by a function of the same
    /// registration.
    pub(crate) fn insert<A, T, E>(&self, task_name: &str, function: TaskFunction<A, T, E>)
    where
        A: Serialize + DeserializeOwned + 'static,
        T: Serialize + DeserializeOwned + 'static,
        E: CustomErrorCode,
    {
        let node = NodeRegistration {
            queue: function.queue().to_owned(),
            task_options: function
                .task_options()
                .map(TaskOptions::stored_options_text),
            accepts_workflow_context: function.accepts_workflow_context(),
        };
        let stored = StoredFunction {
            type_name: type_name::<TaskFunction<A, T, E>>(),
            input: TypeId::of::<A>(),
            output: TypeId::of::<T>(),
            node,
            function: Arc::new(function),
        };
        let _previous = self.lock().insert(task_name.to_owned(), stored);
    }

    /// The function of `task_name` with the types `A`, `T`, `E`.
    ///
    /// Errors: [`TaskFunctionNotFound`].
    pub(crate) fn get<A, T, E>(
        &self,
        task_name: &str,
    ) -> Result<TaskFunction<A, T, E>, TaskFunctionNotFound>
    where
        A: 'static,
        T: 'static,
        E: 'static,
    {
        let (function, registered) = match self.lock().get(task_name) {
            Some(stored) => (Arc::clone(&stored.function), stored.type_name),
            None => {
                return Err(TaskFunctionNotFound::NotRegistered {
                    task_name: task_name.to_owned(),
                })
            }
        };
        match Arc::downcast::<TaskFunction<A, T, E>>(function) {
            Ok(typed) => Ok(TaskFunction::clone(&typed)),
            Err(_other) => Err(TaskFunctionNotFound::OtherTypes {
                task_name: task_name.to_owned(),
                registered,
                asked: type_name::<TaskFunction<A, T, E>>(),
            }),
        }
    }

    /// What a workflow node of `task_name` with the output `T` and the
    /// input `I` takes from the registration of its task. Any code type
    /// matches: a node does not read the codes of its task.
    ///
    /// Errors: [`TaskFunctionNotFound::NotRegistered`];
    /// [`TaskFunctionNotFound::OtherTypes`] when the task was registered
    /// with another input or output type.
    pub(crate) fn node_registration<T, I>(
        &self,
        task_name: &str,
    ) -> Result<NodeRegistration, TaskFunctionNotFound>
    where
        T: 'static,
        I: 'static,
    {
        let functions = self.lock();
        let Some(stored) = functions.get(task_name) else {
            return Err(TaskFunctionNotFound::NotRegistered {
                task_name: task_name.to_owned(),
            });
        };
        match stored.input == TypeId::of::<I>() && stored.output == TypeId::of::<T>() {
            true => Ok(stored.node.clone()),
            false => Err(TaskFunctionNotFound::OtherTypes {
                task_name: task_name.to_owned(),
                registered: stored.type_name,
                asked: type_name::<TaskNode<T, I>>(),
            }),
        }
    }

    fn lock(&self) -> MutexGuard<'_, HashMap<String, StoredFunction>> {
        // No code that holds the lock can panic: it inserts into or reads a
        // map. A poisoned lock is a defect.
        self.functions
            .lock()
            .expect("the task function catalog lock is never held across a panic")
    }
}

impl std::fmt::Debug for TaskFunctions {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("TaskFunctions")
            .field("count", &self.lock().len())
            .finish()
    }
}
