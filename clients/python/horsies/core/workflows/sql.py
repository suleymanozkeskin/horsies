"""The typed facts of the nodes of a workflow start: the task node facts and
the params choice of a sub-workflow node."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum

from ..history.ddl.classes import resolve_retention_class_key
from ..history.periods import RetentionClassKey


@dataclass(frozen=True, slots=True)
class TaskNodeFacts:
    """The fact of one task node that the client resolves: the retention
    class key of the node's queue.

    The core parses the other facts of the node row (the retry limit, the
    execution class and the deadline) from the node's task options
    (``node_task_facts``, ``core/src/workflow_engine/start.rs``).

    Attributes:
        retention_class_key: The stored retention class key of the node's
            queue: ``forever`` or ``standard_<days>d``.
    """

    retention_class_key: str


def task_node_facts(
    *,
    queue_retention_class: RetentionClassKey | None,
) -> TaskNodeFacts:
    """The facts of one task node, with its queue's class key resolved.

    ``queue_retention_class`` is the class that the node's queue resolves
    to; ``None`` is the explicit forever choice and stores ``forever``, as
    on the send path. The caller reads the class from app configuration.
    Does no I/O.
    """
    return TaskNodeFacts(
        retention_class_key=resolve_retention_class_key(queue_retention_class),
    )


class SubWorkflowParams(Enum):
    """The client's choice for the params of a sub-workflow node.

    Stored in ``horsies_workflow_tasks.sub_workflow_params`` on every
    sub-workflow node row, and NULL on task node rows
    (``horsies_workflow_tasks_sub_workflow_params_by_kind``).
    """

    NOT_GIVEN = 'NOT_GIVEN'
    """The node definition carries no params for the child's build."""

    GIVEN = 'GIVEN'
    """The node definition carries params for the child's build."""
