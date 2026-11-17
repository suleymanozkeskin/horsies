//! Whether this process runs task bodies (Python `core/process_role.py`).
//!
//! A worker child runs task bodies only. The role belongs to the process, so
//! it holds for every thread and every runtime in the child, also for the
//! threads and tasks that a body starts, and for the start hook.
//!
//! A result wait fails closed in a process that runs task bodies
//! (`WAIT_INSIDE_TASK`). A task that waits for another task keeps its own
//! capacity while it waits; when enough tasks wait, no capacity is left for
//! the tasks that they wait for, and nothing moves until timeouts end the
//! waits. Workflows compose tasks without a wait. The client reads
//! [`process_role`] before it sends any statement of a wait.

use std::sync::atomic::{AtomicBool, Ordering};

/// Whether this process runs task bodies.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProcessRole {
    /// A worker child: the Rust child host started in this process.
    RunsTaskBodies,
    /// Any other process: a producer, a worker parent, a scheduler.
    RunsNoTaskBodies,
}

/// Set once, by the child host, before the start hook and the first run.
static RUNS_TASK_BODIES: AtomicBool = AtomicBool::new(false);

/// The role of this process. Pure read; starts nothing.
pub fn process_role() -> ProcessRole {
    match RUNS_TASK_BODIES.load(Ordering::Acquire) {
        true => ProcessRole::RunsTaskBodies,
        false => ProcessRole::RunsNoTaskBodies,
    }
}

/// Record that this process runs task bodies. The child host calls it at its
/// start; the role never goes back.
pub(super) fn mark_process_runs_task_bodies() {
    RUNS_TASK_BODIES.store(true, Ordering::Release);
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

    /// Python `wait_inside_task_message` (`tests/unit/test_wait_inside_task.py`):
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
