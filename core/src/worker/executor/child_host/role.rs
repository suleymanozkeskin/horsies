//! Whether this process runs task bodies (Python `core/process_role.py`).
//!
//! A task child runs task bodies only. The role belongs to the process, so
//! it holds for every thread and every runtime in the child, also for the
//! threads and tasks that a body starts, and for the start hook. A handler
//! child runs no task body: a handler holds no task capacity, so a result
//! wait in a handler is not refused.
//!
//! A result wait fails closed in a process that runs task bodies
//! (`WAIT_INSIDE_TASK`). A task that waits for another task keeps its own
//! capacity while it waits; when enough tasks wait, no capacity is left for
//! the tasks that they wait for, and nothing moves until timeouts end the
//! waits. Workflows compose tasks without a wait. The client reads
//! [`process_role`] before it sends any statement of a wait.

use std::cell::Cell;
use std::sync::atomic::{AtomicBool, Ordering};

/// Whether this process runs task bodies.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProcessRole {
    /// A task child: the Rust child host booted a task kind in this
    /// process.
    RunsTaskBodies,
    /// Any other process: a producer, a worker parent, a scheduler, a
    /// handler child.
    RunsNoTaskBodies,
}

/// Set once, by the child host of a task child, before the start hook and
/// the first run.
static RUNS_TASK_BODIES: AtomicBool = AtomicBool::new(false);

/// The role of this process. Pure read; starts nothing.
pub fn process_role() -> ProcessRole {
    match RUNS_TASK_BODIES.load(Ordering::Acquire) {
        true => ProcessRole::RunsTaskBodies,
        false => ProcessRole::RunsNoTaskBodies,
    }
}

/// Record that this process runs task bodies. The child host calls it when
/// it boots a task child; the role never goes back.
pub(super) fn mark_process_runs_task_bodies() {
    RUNS_TASK_BODIES.store(true, Ordering::Release);
}

/// What the calling thread runs.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ThreadRole {
    /// The body of a blocking task: an isolated `fn` task on a thread of
    /// the child runtime's blocking pool, or a `SHARED_THREAD` task on a seat
    /// thread. The thread drives no async task, so a blocking call of a
    /// client may wait on it, also where the runtime's handle is set.
    BlockingTaskBody,
    /// Any other thread.
    Other,
}

thread_local! {
    /// Set while this thread runs the body of a blocking task.
    static RUNS_BLOCKING_BODY: Cell<bool> = const { Cell::new(false) };
}

/// The role of the calling thread. Pure read.
pub fn thread_role() -> ThreadRole {
    match RUNS_BLOCKING_BODY.with(Cell::get) {
        true => ThreadRole::BlockingTaskBody,
        false => ThreadRole::Other,
    }
}

/// Run `body` with the calling thread marked as a blocking task body. The
/// mark is removed when `body` ends, also when it panics.
pub(super) fn as_blocking_body<R>(body: impl FnOnce() -> R) -> R {
    struct Unmark;
    impl Drop for Unmark {
        fn drop(&mut self) {
            RUNS_BLOCKING_BODY.with(|mark| mark.set(false));
        }
    }
    RUNS_BLOCKING_BODY.with(|mark| mark.set(true));
    let _unmark = Unmark;
    body()
}

/// The message of a result wait that a process running task bodies refuses
/// (Python `wait_inside_task_message`). `operation` names the call.
pub fn wait_inside_task_message(operation: &str) -> String {
    format!(
        "{operation} was called where task bodies run. A task does not wait for the result of \
         another task: the wait keeps this task's capacity, and when enough tasks wait, nothing \
         is left for the tasks that they wait for. Compose the steps as a workflow: TaskNode \
         waits_for and args_from, or a SubWorkflowNode."
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The role is the process's: once the child host marks it, every read
    /// gives `RunsTaskBodies`. No other core test reads the role.
    #[test]
    fn the_child_host_mark_sets_the_role_of_the_process() {
        mark_process_runs_task_bodies();
        assert_eq!(process_role(), ProcessRole::RunsTaskBodies);
        mark_process_runs_task_bodies();
        assert_eq!(process_role(), ProcessRole::RunsTaskBodies);
    }

    /// A blocking body marks its thread while it runs; the mark goes with
    /// its end, also with a panic, and never reaches another thread.
    #[test]
    fn a_blocking_body_marks_its_thread_while_it_runs() {
        assert_eq!(thread_role(), ThreadRole::Other);
        let inside = as_blocking_body(thread_role);
        assert_eq!(inside, ThreadRole::BlockingTaskBody);
        assert_eq!(thread_role(), ThreadRole::Other);
        let other_thread = as_blocking_body(|| {
            std::thread::spawn(thread_role)
                .join()
                .expect("the thread ends")
        });
        assert_eq!(other_thread, ThreadRole::Other);
        let panicked = std::panic::catch_unwind(|| as_blocking_body(|| panic!("body panic")));
        assert!(panicked.is_err());
        assert_eq!(thread_role(), ThreadRole::Other);
    }

    /// Python `wait_inside_task_message` (`clients/python/tests/unit/test_wait_inside_task.py`):
    /// the message names the call and the workflow alternative, word for
    /// word.
    #[test]
    fn the_wait_message_is_python_s() {
        assert_eq!(
            wait_inside_task_message("TaskHandle.get"),
            "TaskHandle.get was called where task bodies run. A task does not wait for the result \
             of another task: the wait keeps this task's capacity, and when enough tasks wait, \
             nothing is left for the tasks that they wait for. Compose the steps as a workflow: \
             TaskNode waits_for and args_from, or a SubWorkflowNode."
        );
    }
}
