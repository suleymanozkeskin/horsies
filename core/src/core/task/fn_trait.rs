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
    /// `input` is the task input: the stored input value as one JSON value.
    /// Returns `Ok(())` if it deserializes into the task's declared input type
    /// via the same path `execute` uses, otherwise the structured deserialize
    /// error.
    ///
    /// The default returns `Ok(())` for implementations that cannot introspect
    /// their input type (e.g. hand-written trait impls). Macro-generated tasks
    /// override this to perform the real typed check, enabling `app.check()` to
    /// validate schedule and workflow-node payloads at startup. The Rust
    /// client's tasks also refuse an input that does not read back to itself,
    /// as its send does.
    fn validate_input(&self, input: &[u8]) -> Result<(), TaskError> {
        let _ = input;
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
    fn validate_input(&self, input: &[u8]) -> Result<(), TaskError> {
        let _ = input;
        Ok(())
    }
}

/// Metadata describing a registered task.
///
/// Use [`TaskMetadata::for_input::<A>()`] in task construction macros to
/// automatically populate `expects_input` and `input_type_name`.
#[derive(Debug, Clone, Default)]
pub struct TaskMetadata {
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
    /// The module that defines the task: the `module` of the exception
    /// record of a panic of its body.
    pub module: TaskModule,
    /// The user error codes that the task declares: the `CODES` of its code
    /// type, which the Rust client's task macros fill. Empty for a task that
    /// declares none, such as a hand-built [`RegisteredTask`]. The client
    /// checks them when it registers the task
    /// ([`crate::core::task::DeclaredErrorCodes::parse`]).
    pub error_codes: &'static [&'static str],
}

/// The Rust module that defines a task.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum TaskModule {
    /// `module_path!()` where the task is defined.
    Path(&'static str),
    /// A registration that names no module, such as a hand-written
    /// [`RegisteredTask`]. The exception record of a panic then has an
    /// empty `module`.
    #[default]
    NotGiven,
}

impl TaskMetadata {
    /// The same metadata, for a task defined in `module_path`. The task
    /// macros pass `module_path!()`.
    pub fn in_module(self, module_path: &'static str) -> Self {
        Self {
            module: TaskModule::Path(module_path),
            ..self
        }
    }

    /// The same metadata, for a task that declares the user error codes
    /// `codes`. The task macros pass `UserErrorCode::CODES` of the task's
    /// code type.
    pub fn with_error_codes(self, codes: &'static [&'static str]) -> Self {
        Self {
            error_codes: codes,
            ..self
        }
    }

    /// Create metadata with input type information pre-populated.
    ///
    /// A client's task macros use this (the Rust client's `async_task_fn!`
    /// and `blocking_task_fn!`), so that a registered task also carries
    /// `expects_input` and `input_type_name` for the client's checks.
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
        metadata: TaskMetadata,
    },
    Blocking {
        task: Arc<dyn BlockingTaskFn>,
        metadata: TaskMetadata,
    },
}

impl Clone for RegisteredTask {
    fn clone(&self) -> Self {
        match self {
            Self::Async { task, metadata } => Self::Async {
                task: Arc::clone(task),
                metadata: metadata.clone(),
            },
            Self::Blocking { task, metadata } => Self::Blocking {
                task: Arc::clone(task),
                metadata: metadata.clone(),
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
            Self::Async { metadata, .. } | Self::Blocking { metadata, .. } => {
                metadata.accepts_workflow_ctx
            }
        }
    }

    /// Dry-run the typed input deserialization for `input` without executing.
    ///
    /// Delegates to the inner task fn. Returns `Ok(())` for tasks that cannot
    /// introspect their input type. See [`AsyncTaskFn::validate_input`].
    pub fn validate_input(&self, input: &[u8]) -> Result<(), TaskError> {
        match self {
            Self::Async { task, .. } => task.validate_input(input),
            Self::Blocking { task, .. } => task.validate_input(input),
        }
    }

    /// Mark this task as accepting workflow context injection.
    pub fn with_workflow_ctx(self) -> Self {
        match self {
            Self::Async { task, mut metadata } => {
                metadata.accepts_workflow_ctx = true;
                Self::Async { task, metadata }
            }
            Self::Blocking { task, mut metadata } => {
                metadata.accepts_workflow_ctx = true;
                Self::Blocking { task, metadata }
            }
        }
    }

    /// Attach task options metadata.
    pub fn with_task_options(self, task_options: TaskOptions) -> Self {
        match self {
            Self::Async { task, mut metadata } => {
                metadata.task_options = Some(task_options);
                Self::Async { task, metadata }
            }
            Self::Blocking { task, mut metadata } => {
                metadata.task_options = Some(task_options);
                Self::Blocking { task, metadata }
            }
        }
    }

    /// The user error codes that the task declares ([`TaskMetadata::error_codes`]).
    pub fn error_codes(&self) -> &'static [&'static str] {
        match self {
            Self::Async { metadata, .. } | Self::Blocking { metadata, .. } => metadata.error_codes,
        }
    }

    /// Access task options metadata, if present.
    pub fn task_options(&self) -> Option<&TaskOptions> {
        match self {
            Self::Async { metadata, .. } | Self::Blocking { metadata, .. } => {
                metadata.task_options.as_ref()
            }
        }
    }

    /// Set the resolved queue name on this task's metadata.
    pub fn set_queue_name(&mut self, queue: String) {
        match self {
            Self::Async { metadata, .. } | Self::Blocking { metadata, .. } => {
                metadata.queue_name = Some(queue);
            }
        }
    }

    /// Set the resolved priority on this task's metadata.
    pub fn set_priority(&mut self, priority: u32) {
        match self {
            Self::Async { metadata, .. } | Self::Blocking { metadata, .. } => {
                metadata.priority = Some(priority);
            }
        }
    }

    /// Set whether this task expects non-unit input.
    pub fn set_expects_input(&mut self, expects: bool) {
        match self {
            Self::Async { metadata, .. } | Self::Blocking { metadata, .. } => {
                metadata.expects_input = expects;
            }
        }
    }

    /// Set the input type name for diagnostics.
    pub fn set_input_type_name(&mut self, name: &'static str) {
        match self {
            Self::Async { metadata, .. } | Self::Blocking { metadata, .. } => {
                metadata.input_type_name = Some(name);
            }
        }
    }

    /// The resolved queue name, if set during registration.
    pub fn queue_name(&self) -> Option<&str> {
        match self {
            Self::Async { metadata, .. } | Self::Blocking { metadata, .. } => {
                metadata.queue_name.as_deref()
            }
        }
    }

    /// The resolved priority, if set during registration.
    pub fn priority(&self) -> Option<u32> {
        match self {
            Self::Async { metadata, .. } | Self::Blocking { metadata, .. } => metadata.priority,
        }
    }

    /// Whether this task expects non-unit input.
    pub fn expects_input(&self) -> bool {
        match self {
            Self::Async { metadata, .. } | Self::Blocking { metadata, .. } => {
                metadata.expects_input
            }
        }
    }

    /// The input type name, for diagnostics.
    pub fn input_type_name(&self) -> Option<&'static str> {
        match self {
            Self::Async { metadata, .. } | Self::Blocking { metadata, .. } => {
                metadata.input_type_name
            }
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
            metadata: TaskMetadata::default(),
        };
        assert!(async_task.is_async());
        assert!(!async_task.is_blocking());

        let blocking_task = RegisteredTask::Blocking {
            task: Arc::new(DummyBlocking),
            metadata: TaskMetadata::default(),
        };
        assert!(blocking_task.is_blocking());
        assert!(!blocking_task.is_async());
    }

    #[test]
    fn a_task_declares_no_error_code_unless_its_meta_names_some() {
        let plain = RegisteredTask::Blocking {
            task: Arc::new(DummyBlocking),
            metadata: TaskMetadata::default(),
        };
        assert!(plain.error_codes().is_empty());
        let declared = RegisteredTask::Async {
            task: Arc::new(DummyAsync),
            metadata: TaskMetadata::for_input::<()>()
                .with_error_codes(&["RATE_LIMITED", "DECLINED"]),
        };
        assert_eq!(declared.error_codes(), ["RATE_LIMITED", "DECLINED"]);
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
                    OperationalErrorCode::TaskException,
                    "something broke",
                ))
            }
        }

        let task = FailingTask;
        let result = task.execute(b"");
        assert!(result.is_err());
    }
}
