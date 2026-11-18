"""The typed facts of the nodes of a workflow start: the task node facts and
the params choice of a sub-workflow node."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum

from ..history.ddl.classes import resolve_retention_class_key
from ..history.periods import RetentionClassKey
from ..models.tasks import Execution


@dataclass(frozen=True, slots=True)
class TaskNodeFacts:
    """The typed facts of one task node, parsed from its task options.

    A workflow start writes them on the node row
    (``horsies_workflow_tasks_task_facts_by_kind``), and a task row minted
    for the node stores the same values.

    Attributes:
        max_retries: The retry limit of the node's retry policy; 0 without one.
        execution: The execution class of the node's task.
        good_until: The ``good_until`` text of the task options, or ``None``.
            The database parses it into the timestamp column, on the node row
            and on the task row alike.
        retention_class_key: The stored retention class key of the node's
            queue: ``forever`` or ``standard_<days>d``.
    """

    max_retries: int
    execution: Execution
    good_until: str | None
    retention_class_key: str


def task_node_facts(
    *,
    max_retries: int,
    execution: Execution,
    good_until: str | None,
    queue_retention_class: RetentionClassKey | None,
) -> TaskNodeFacts:
    """The facts of one task node, with its queue's class key resolved.

    ``queue_retention_class`` is the class that the node's queue resolves
    to; ``None`` is the explicit forever choice and stores ``forever``, as
    on the send path. The caller reads the class from app configuration,
    because this module builds statements. Does no I/O.
    """
    return TaskNodeFacts(
        max_retries=max_retries,
        execution=execution,
        good_until=good_until,
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
