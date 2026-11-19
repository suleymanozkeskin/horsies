//! The typed functions that an app's task attributes registered, by task
//! name: `TaskRuntime::task_handle` and `name::handle(&runtime)` read them.

use std::any::{type_name, Any};
use std::collections::HashMap;
use std::sync::{Arc, Mutex, MutexGuard};

use super::function::TaskFunction;

/// Why a task body got no typed function for a task name.
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

/// One stored function and the name of its type.
struct StoredFunction {
    function: Arc<dyn Any + Send + Sync>,
    type_name: &'static str,
}

/// The typed functions of one app. Clones of the app's runtime share it.
pub(crate) struct TaskFunctions {
    functions: Mutex<HashMap<String, StoredFunction>>,
}

impl TaskFunctions {
    pub(crate) fn new() -> Self {
        Self {
            functions: Mutex::new(HashMap::new()),
        }
    }

    /// Keep `function` as the function of `task_name`. The app refuses a
    /// repeated task name before this, so an earlier entry is replaced only
    /// by a function of the same registration.
    pub(crate) fn insert<A, T, E>(&self, task_name: &str, function: TaskFunction<A, T, E>)
    where
        A: 'static,
        T: 'static,
        E: 'static,
    {
        let stored = StoredFunction {
            function: Arc::new(function),
            type_name: type_name::<TaskFunction<A, T, E>>(),
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
