//! The handle of a sent task: the result read as
//! `Result<TaskOutcome<T, E>, ReadError>`, and the task info.

use std::marker::PhantomData;
use std::sync::Arc;

use horsies_core::handle::CoreHandle;
use uuid::Uuid;

/// The handle of one sent task, with its output type `T` and code type `E`.
/// Clones share one task.
pub struct TaskHandle<T, E> {
    task_id: Uuid,
    reads: Arc<CoreHandle>,
    types: PhantomData<fn() -> (T, E)>,
}

impl<T, E> TaskHandle<T, E> {
    /// The handle of `task_id`, read through `reads`: the handle that sent
    /// the task.
    pub(crate) fn new(task_id: Uuid, reads: Arc<CoreHandle>) -> Self {
        Self {
            task_id,
            reads,
            types: PhantomData,
        }
    }

    /// The id of the task.
    pub fn task_id(&self) -> Uuid {
        self.task_id
    }
}

impl<T, E> Clone for TaskHandle<T, E> {
    fn clone(&self) -> Self {
        Self {
            task_id: self.task_id,
            reads: Arc::clone(&self.reads),
            types: PhantomData,
        }
    }
}

impl<T, E> std::fmt::Debug for TaskHandle<T, E> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("TaskHandle")
            .field("task_id", &self.task_id)
            .finish_non_exhaustive()
    }
}
