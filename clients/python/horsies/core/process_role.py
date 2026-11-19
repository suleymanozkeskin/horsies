"""Whether this process runs task bodies.

A worker child process runs task bodies only. The role belongs to the
process, so it holds for every thread and every event loop in the child,
also for the threads that a task starts.

Result waits fail closed in a process that runs task bodies. A task that
waits for another task keeps its own capacity while it waits. When enough
tasks wait, no capacity is left for the tasks that they wait for, and nothing
moves until timeouts end the waits. Workflows compose tasks without a wait:
a node that waits for its dependencies is only a row.
"""

from __future__ import annotations

from enum import Enum


class ProcessRole(Enum):
    """Whether this process runs task bodies."""

    RUNS_TASK_BODIES = 'runs_task_bodies'
    RUNS_NO_TASK_BODIES = 'runs_no_task_bodies'


_role = ProcessRole.RUNS_NO_TASK_BODIES


def mark_process_runs_task_bodies() -> None:
    """Record that this process runs task bodies. A worker child calls this at start."""
    global _role
    _role = ProcessRole.RUNS_TASK_BODIES


def process_role() -> ProcessRole:
    """Return the role of this process."""
    return _role


def wait_inside_task_message(operation: str) -> str:
    """Return the message of a result wait that a task body cannot make."""
    return (
        f'{operation} was called where task bodies run. A task does not wait '
        "for the result of another task: the wait keeps this task's capacity, "
        'and when enough tasks wait, nothing is left for the tasks that they '
        'wait for. Compose the steps as a workflow: TaskNode waits_for and '
        'args_from, or a SubWorkflowNode.'
    )
