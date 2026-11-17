"""Workflow cancel, before ``horsies_workflow_cancel``.

``horsies/core/models/workflow/handle.py`` (``WorkflowHandle.cancel_async``)
sent, in one session transaction, for the root: the workflow row lock, the
backing task locks, the node locks, the backing task locks again, the
CANCELLED write, the status read; when the root is CANCELLED, the cancel of
the tasks behind its ENQUEUED nodes
(``horsies_cancel_nodes_of_cancelled_workflow``), the skip of its PENDING
and READY nodes and of the ENQUEUED nodes of the cancelled tasks. Then the
same sequence for each PENDING, RUNNING or PAUSED descendant. The CANCELLED
write and the PENDING/READY skip set ``completed_at``, as the commits before
the switch made them.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from typing import Final, LiteralString

from tests.dualrun.capture import Connection

_WF_TASK_TERMINAL: Final = ['COMPLETED', 'FAILED', 'SKIPPED']
_TASK_TERMINAL: Final = ['COMPLETED', 'FAILED', 'CANCELLED', 'EXPIRED']

LOCK_WORKFLOW_ROW_SQL: Final[LiteralString] = """
    SELECT id FROM horsies_workflows WHERE id = %(wf_id)s FOR UPDATE
"""

LOCK_BACKING_TASKS_SQL: Final[LiteralString] = """
    SELECT t.id
    FROM horsies_tasks t
    JOIN horsies_workflow_tasks wt ON wt.task_id = t.id
    WHERE wt.workflow_id = %(wf_id)s
      AND NOT (wt.status = ANY(%(wf_task_terminal_states)s))
      AND NOT (t.status = ANY(%(task_terminal_states)s))
    FOR UPDATE OF t
"""

LOCK_NODES_SQL: Final[LiteralString] = """
    SELECT id
    FROM horsies_workflow_tasks
    WHERE workflow_id = %(wf_id)s
      AND NOT (status = ANY(%(wf_task_terminal_states)s))
    FOR UPDATE
"""

CANCEL_WORKFLOW_SQL: Final[LiteralString] = """
    UPDATE horsies_workflows
    SET status = 'CANCELLED', completed_at = NOW(), updated_at = NOW()
    WHERE id = %(wf_id)s AND status IN ('PENDING', 'RUNNING', 'PAUSED')
"""

GET_WORKFLOW_STATUS_SQL: Final[LiteralString] = """
    SELECT status FROM horsies_workflows WHERE id = %(wf_id)s
"""

CANCEL_NODES_OF_CANCELLED_WORKFLOW_SQL: Final[LiteralString] = """
    SELECT task_id, outcome FROM horsies_cancel_nodes_of_cancelled_workflow(
        CAST(%(workflow_ids)s AS uuid[])
    )
"""

SKIP_PENDING_READY_SQL: Final[LiteralString] = """
    UPDATE horsies_workflow_tasks
    SET status = 'SKIPPED', completed_at = NOW()
    WHERE workflow_id = %(wf_id)s AND status IN ('PENDING', 'READY')
"""

SKIP_CANCELLED_ENQUEUED_SQL: Final[LiteralString] = """
    UPDATE horsies_workflow_tasks wt
    SET status = 'SKIPPED',
        completed_at = NOW()
    WHERE wt.workflow_id = %(wf_id)s
      AND wt.status = 'ENQUEUED'
      AND wt.task_id = ANY(CAST(%(cancelled_task_ids)s AS uuid[]))
"""

CANCELLABLE_DESCENDANTS_SQL: Final[LiteralString] = """
    WITH RECURSIVE descendants AS (
        SELECT id, status
        FROM horsies_workflows
        WHERE parent_workflow_id = %(wf_id)s
        UNION ALL
        SELECT child.id, child.status
        FROM horsies_workflows child
        JOIN descendants parent ON child.parent_workflow_id = parent.id
    )
    SELECT id FROM descendants
    WHERE status IN ('PENDING', 'RUNNING', 'PAUSED')
"""


@dataclass(frozen=True, slots=True)
class Cancelled:
    """The tree is cancelled, or the workflow was terminal already."""


@dataclass(frozen=True, slots=True)
class NotFound:
    """No workflow has the id. Nothing was written."""


type CancelReport = Cancelled | NotFound


def _lock_and_flip(connection: Connection, workflow_id: uuid.UUID) -> None:
    params = {
        'wf_id': workflow_id,
        'wf_task_terminal_states': _WF_TASK_TERMINAL,
        'task_terminal_states': _TASK_TERMINAL,
    }
    _ = connection.execute(LOCK_WORKFLOW_ROW_SQL, {'wf_id': workflow_id})
    _ = connection.execute(LOCK_BACKING_TASKS_SQL, params)
    _ = connection.execute(LOCK_NODES_SQL, params)
    _ = connection.execute(LOCK_BACKING_TASKS_SQL, params)
    _ = connection.execute(CANCEL_WORKFLOW_SQL, {'wf_id': workflow_id})


def _cancel_nodes(connection: Connection, workflow_id: uuid.UUID) -> None:
    outcomes = connection.execute(
        CANCEL_NODES_OF_CANCELLED_WORKFLOW_SQL, {'workflow_ids': [workflow_id]},
    ).fetchall()
    cancelled: list[uuid.UUID] = []
    for task_id, outcome in outcomes:
        assert outcome == 'APPLIED', f'a workflow-scoped cancel returned {outcome}'
        assert isinstance(task_id, uuid.UUID)
        cancelled.append(task_id)
    _ = connection.execute(SKIP_PENDING_READY_SQL, {'wf_id': workflow_id})
    _ = connection.execute(
        SKIP_CANCELLED_ENQUEUED_SQL,
        {'wf_id': workflow_id, 'cancelled_task_ids': cancelled},
    )


def cancel(connection: Connection, workflow_id: uuid.UUID) -> CancelReport:
    """The cancel of ``workflow_id`` and its tree, in one transaction."""
    with connection.transaction():
        _lock_and_flip(connection, workflow_id)
        record = connection.execute(GET_WORKFLOW_STATUS_SQL, {'wf_id': workflow_id}).fetchone()
        match record:
            case None:
                return NotFound()
            case (status,) if status != 'CANCELLED':
                return Cancelled()
            case _:
                pass
        _cancel_nodes(connection, workflow_id)
        rows = connection.execute(CANCELLABLE_DESCENDANTS_SQL, {'wf_id': workflow_id}).fetchall()
        for row in rows:
            descendant = row[0]
            assert isinstance(descendant, uuid.UUID)
            _lock_and_flip(connection, descendant)
            _cancel_nodes(connection, descendant)
        return Cancelled()
