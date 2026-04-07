use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use crate::core::task::options::TaskOptions;
use crate::core::task::result::TaskResult;

/// Type-erased result from task execution: serialized Ok bytes or TaskError.
pub type RawTaskResult = TaskResult<Vec<u8>>;

/// Async task function trait.
///
/// Implemented by task functions that are async (IO-bound work).
/// The serde boundary lives here: args come in as `&[u8]` (JSON),
/// result goes out as `Vec<u8>` (JSON) or `TaskError`.
pub trait AsyncTaskFn: Send + Sync + 'static {
    /// Execute the task with serialized arguments, returning serialized result.
    fn execute(&self, args: &[u8]) -> Pin<Box<dyn Future<Output = RawTaskResult> + Send + '_>>;
}

/// Blocking task function trait.
///
/// Implemented by task functions that are synchronous (CPU-bound work).
/// These run on tokio's blocking thread pool via `spawn_blocking`.
pub trait BlockingTaskFn: Send + Sync + 'static {
    /// Execute the task with serialized arguments, returning serialized result.
    fn execute(&self, args: &[u8]) -> RawTaskResult;
}

/// Metadata describing a registered task.
#[derive(Debug, Clone, Default)]
pub struct TaskMeta {
    /// Whether this task opts into workflow context injection.
    pub accepts_workflow_ctx: bool,
    /// Definition-time task options, if the task was registered through
    /// a producer/builder path that carries them.
    pub task_options: Option<TaskOptions>,
    /// Resolved queue name from task registration.
    pub queue_name: Option<String>,
    /// Resolved priority from task registration.
    pub priority: Option<u32>,
}

/// A registered task: either async or blocking.
///
/// The worker dispatches based on this variant:
/// - `Async` → `tokio::spawn`
/// - `Blocking` → `tokio::task::spawn_blocking`
///
/// Uses `Arc` internally so the worker can clone task functions
/// into spawned tasks without moving them out of the registry.
pub enum RegisteredTask {
    Async {
        task: Arc<dyn AsyncTaskFn>,
        meta: TaskMeta,
    },
    Blocking {
        task: Arc<dyn BlockingTaskFn>,
        meta: TaskMeta,
    },
}

impl Clone for RegisteredTask {
    fn clone(&self) -> Self {
        match self {
            Self::Async { task, meta } => Self::Async {
                task: Arc::clone(task),
                meta: meta.clone(),
            },
            Self::Blocking { task, meta } => Self::Blocking {
                task: Arc::clone(task),
                meta: meta.clone(),
            },
        }
    }
}

impl RegisteredTask {
    /// Returns true if this is an async task.
    pub fn is_async(&self) -> bool {
        matches!(self, Self::Async { .. })
    }

    /// Returns true if this is a blocking task.
    pub fn is_blocking(&self) -> bool {
        matches!(self, Self::Blocking { .. })
    }

    /// Returns true if this task accepts workflow context injection.
    pub fn accepts_workflow_ctx(&self) -> bool {
        match self {
            Self::Async { meta, .. } | Self::Blocking { meta, .. } => meta.accepts_workflow_ctx,
        }
    }

    /// Mark this task as accepting workflow context injection.
    pub fn with_workflow_ctx(self) -> Self {
        match self {
            Self::Async { task, mut meta } => {
                meta.accepts_workflow_ctx = true;
                Self::Async { task, meta }
            }
            Self::Blocking { task, mut meta } => {
                meta.accepts_workflow_ctx = true;
                Self::Blocking { task, meta }
            }
        }
    }

    /// Attach definition-time task options metadata.
    pub fn with_task_options(self, task_options: TaskOptions) -> Self {
        match self {
            Self::Async { task, mut meta } => {
                meta.task_options = Some(task_options);
                Self::Async { task, meta }
            }
            Self::Blocking { task, mut meta } => {
                meta.task_options = Some(task_options);
                Self::Blocking { task, meta }
            }
        }
    }

    /// Access definition-time task options metadata, if present.
    pub fn task_options(&self) -> Option<&TaskOptions> {
        match self {
            Self::Async { meta, .. } | Self::Blocking { meta, .. } => meta.task_options.as_ref(),
        }
    }

    /// Set the resolved queue name on this task's metadata.
    pub fn set_queue_name(&mut self, queue: String) {
        match self {
            Self::Async { meta, .. } | Self::Blocking { meta, .. } => {
                meta.queue_name = Some(queue);
            }
        }
    }

    /// Set the resolved priority on this task's metadata.
    pub fn set_priority(&mut self, priority: u32) {
        match self {
            Self::Async { meta, .. } | Self::Blocking { meta, .. } => {
                meta.priority = Some(priority);
            }
        }
    }

    /// The resolved queue name, if set during registration.
    pub fn queue_name(&self) -> Option<&str> {
        match self {
            Self::Async { meta, .. } | Self::Blocking { meta, .. } => {
                meta.queue_name.as_deref()
            }
        }
    }

    /// The resolved priority, if set during registration.
    pub fn priority(&self) -> Option<u32> {
        match self {
            Self::Async { meta, .. } | Self::Blocking { meta, .. } => meta.priority,
        }
    }
}

impl std::fmt::Debug for RegisteredTask {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Async { .. } => write!(f, "RegisteredTask::Async(...)"),
            Self::Blocking { .. } => write!(f, "RegisteredTask::Blocking(...)"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::task::error::{OperationalErrorCode, TaskError};

    struct DummyAsync;

    impl AsyncTaskFn for DummyAsync {
        fn execute(&self, args: &[u8]) -> Pin<Box<dyn Future<Output = RawTaskResult> + Send + '_>> {
            let args = args.to_vec();
            Box::pin(async move { TaskResult::Ok(args) })
        }
    }

    struct DummyBlocking;

    impl BlockingTaskFn for DummyBlocking {
        fn execute(&self, args: &[u8]) -> RawTaskResult {
            TaskResult::Ok(args.to_vec())
        }
    }

    #[test]
    fn registered_task_variants() {
        let async_task = RegisteredTask::Async {
            task: Arc::new(DummyAsync),
            meta: TaskMeta::default(),
        };
        assert!(async_task.is_async());
        assert!(!async_task.is_blocking());

        let blocking_task = RegisteredTask::Blocking {
            task: Arc::new(DummyBlocking),
            meta: TaskMeta::default(),
        };
        assert!(blocking_task.is_blocking());
        assert!(!blocking_task.is_async());
    }

    #[test]
    fn async_task_executes() {
        let task = DummyAsync;
        let rt = tokio::runtime::Builder::new_current_thread()
            .build()
            .unwrap();
        let result = rt.block_on(task.execute(b"hello"));
        assert_eq!(result.unwrap(), b"hello");
    }

    #[test]
    fn blocking_task_executes() {
        let task = DummyBlocking;
        let result = task.execute(b"world");
        assert_eq!(result.unwrap(), b"world");
    }

    #[test]
    fn blocking_task_can_return_error() {
        struct FailingTask;
        impl BlockingTaskFn for FailingTask {
            fn execute(&self, _args: &[u8]) -> RawTaskResult {
                TaskResult::Err(TaskError::builtin(
                    OperationalErrorCode::TaskError,
                    "something broke",
                ))
            }
        }

        let task = FailingTask;
        let result = task.execute(b"");
        assert!(result.is_err());
    }
}
