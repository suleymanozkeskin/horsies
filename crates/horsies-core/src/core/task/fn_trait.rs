use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use crate::core::task::error::TaskError;
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

    /// Dry-run the typed input deserialization without executing the task.
    ///
    /// `envelope` is the worker args/kwargs envelope (`{"args": [...],
    /// "kwargs": {...}}`). Returns `Ok(())` if the payload deserializes into the
    /// task's declared input type via the same path `execute` uses, otherwise
    /// the structured deserialize error.
    ///
    /// The default returns `Ok(())` for implementations that cannot introspect
    /// their input type (e.g. hand-written trait impls). Macro-generated tasks
    /// override this to perform the real typed check, enabling `app.check()` to
    /// validate schedule and workflow-node payloads at startup.
    fn validate_input(&self, envelope: &[u8]) -> Result<(), TaskError> {
        let _ = envelope;
        Ok(())
    }
}

/// Blocking task function trait.
///
/// Implemented by task functions that are synchronous (CPU-bound work).
/// These run on tokio's blocking thread pool via `spawn_blocking`.
pub trait BlockingTaskFn: Send + Sync + 'static {
    /// Execute the task with serialized arguments, returning serialized result.
    fn execute(&self, args: &[u8]) -> RawTaskResult;

    /// Dry-run the typed input deserialization without executing the task.
    ///
    /// See [`AsyncTaskFn::validate_input`] for semantics. The default returns
    /// `Ok(())`; macro-generated tasks override it.
    fn validate_input(&self, envelope: &[u8]) -> Result<(), TaskError> {
        let _ = envelope;
        Ok(())
    }
}

/// Metadata describing a registered task.
///
/// Use [`TaskMeta::for_input::<A>()`] in task construction macros to
/// automatically populate `expects_input` and `input_type_name`.
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
    /// Whether this task expects non-unit input (i.e. `A` is not `()`).
    pub expects_input: bool,
    /// Type name of the task's input type, for diagnostics.
    pub input_type_name: Option<&'static str>,
}

impl TaskMeta {
    /// Create metadata with input type information pre-populated.
    ///
    /// Use this in `async_task_fn!` / `blocking_task_fn!` macros so that
    /// low-level registered tasks also carry `expects_input` and
    /// `input_type_name` for `check()` validation.
    pub fn for_input<A: 'static>() -> Self {
        Self {
            expects_input: std::any::TypeId::of::<A>() != std::any::TypeId::of::<()>(),
            input_type_name: Some(std::any::type_name::<A>()),
            ..Self::default()
        }
    }
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

    /// Dry-run the typed input deserialization for `envelope` without executing.
    ///
    /// Delegates to the inner task fn. Returns `Ok(())` for tasks that cannot
    /// introspect their input type. See [`AsyncTaskFn::validate_input`].
    pub fn validate_input(&self, envelope: &[u8]) -> Result<(), TaskError> {
        match self {
            Self::Async { task, .. } => task.validate_input(envelope),
            Self::Blocking { task, .. } => task.validate_input(envelope),
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

    /// Attach task options metadata.
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

    /// Access task options metadata, if present.
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

    /// Set whether this task expects non-unit input.
    pub fn set_expects_input(&mut self, expects: bool) {
        match self {
            Self::Async { meta, .. } | Self::Blocking { meta, .. } => {
                meta.expects_input = expects;
            }
        }
    }

    /// Set the input type name for diagnostics.
    pub fn set_input_type_name(&mut self, name: &'static str) {
        match self {
            Self::Async { meta, .. } | Self::Blocking { meta, .. } => {
                meta.input_type_name = Some(name);
            }
        }
    }

    /// The resolved queue name, if set during registration.
    pub fn queue_name(&self) -> Option<&str> {
        match self {
            Self::Async { meta, .. } | Self::Blocking { meta, .. } => meta.queue_name.as_deref(),
        }
    }

    /// The resolved priority, if set during registration.
    pub fn priority(&self) -> Option<u32> {
        match self {
            Self::Async { meta, .. } | Self::Blocking { meta, .. } => meta.priority,
        }
    }

    /// Whether this task expects non-unit input.
    pub fn expects_input(&self) -> bool {
        match self {
            Self::Async { meta, .. } | Self::Blocking { meta, .. } => meta.expects_input,
        }
    }

    /// The input type name, for diagnostics.
    pub fn input_type_name(&self) -> Option<&'static str> {
        match self {
            Self::Async { meta, .. } | Self::Blocking { meta, .. } => meta.input_type_name,
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
