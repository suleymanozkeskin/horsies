"""Workflow pause, before ``horsies_workflow_pause``.

``python/horsies/core/workflows/lifecycle.py`` (``pause_workflow`` with
``cascade_pause_to_children``) sent, in one session transaction: the root
RUNNING to PAUSED; on no row, the existence read; else, breadth first, the
RUNNING children of each paused workflow and their pause; the abandon of
the claimed tasks of the paused workflows
(``horsies_abandon_nodes_of_paused_workflows``); the reset of the nodes of
the abandoned tasks to READY.
"""

from __future__ import annotations

import uuid
from collections import deque
from dataclasses import dataclass
from typing import Final, LiteralString

from tests.dualrun.capture import Connection

PAUSE_WORKFLOW_SQL: Final[LiteralString] = """
    UPDATE horsies_workflows
    SET status = 'PAUSED', updated_at = NOW()
    WHERE id = %(wf_id)s AND status = 'RUNNING'
    RETURNING id
"""

CHECK_WORKFLOW_EXISTS_SQL: Final[LiteralString] = """
    SELECT id FROM horsies_workflows WHERE id = %(wf_id)s
"""

GET_RUNNING_CHILD_WORKFLOWS_SQL: Final[LiteralString] = """
    SELECT id FROM horsies_workflows
    WHERE parent_workflow_id = %(wf_id)s AND status = 'RUNNING'
"""

PAUSE_CHILD_WORKFLOW_SQL: Final[LiteralString] = """
    UPDATE horsies_workflows
    SET status = 'PAUSED', updated_at = NOW()
    WHERE id = %(wf_id)s AND status = 'RUNNING'
"""

ABANDON_NODES_OF_PAUSED_WORKFLOWS_SQL: Final[LiteralString] = """
    SELECT task_id, outcome FROM horsies_abandon_nodes_of_paused_workflows(
        CAST(%(workflow_ids)s AS uuid[])
    )
"""

RESET_ABANDONED_WORKFLOW_TASKS_SQL: Final[LiteralString] = """
    UPDATE horsies_workflow_tasks
    SET status = 'READY',
        task_id = NULL,
        started_at = NULL
    WHERE task_id = ANY(%(task_ids)s)
      AND status IN ('ENQUEUED', 'RUNNING')
"""


@dataclass(frozen=True, slots=True)
class Paused:
    """The workflow was RUNNING and is PAUSED now."""


@dataclass(frozen=True, slots=True)
class NotRunning:
    """The workflow exists and was not RUNNING. Nothing was written."""


@dataclass(frozen=True, slots=True)
class NotFound:
    """No workflow has the id. Nothing was written."""


type PauseReport = Paused | NotRunning | NotFound


def _running_children(connection: Connection, workflow_id: uuid.UUID) -> list[uuid.UUID]:
    rows = connection.execute(GET_RUNNING_CHILD_WORKFLOWS_SQL, {'wf_id': workflow_id}).fetchall()
    children: list[uuid.UUID] = []
    for row in rows:
        child = row[0]
        assert isinstance(child, uuid.UUID)
        children.append(child)
    return children


def _cascade(connection: Connection, workflow_id: uuid.UUID) -> list[uuid.UUID]:
    queue: deque[uuid.UUID] = deque([workflow_id])
    paused: list[uuid.UUID] = []
    while queue:
        current = queue.popleft()
        for child in _running_children(connection, current):
            paused.append(child)
            _ = connection.execute(PAUSE_CHILD_WORKFLOW_SQL, {'wf_id': child})
            queue.append(child)
    return paused


def pause(connection: Connection, workflow_id: uuid.UUID) -> PauseReport:
    """The pause of ``workflow_id`` and its tree, in one transaction."""
    with connection.transaction():
        if connection.execute(PAUSE_WORKFLOW_SQL, {'wf_id': workflow_id}).fetchone() is None:
            exists = connection.execute(CHECK_WORKFLOW_EXISTS_SQL, {'wf_id': workflow_id})
            match exists.fetchone():
                case None:
                    return NotFound()
                case _:
                    return NotRunning()
        paused = [workflow_id, *_cascade(connection, workflow_id)]
        outcomes = connection.execute(
            ABANDON_NODES_OF_PAUSED_WORKFLOWS_SQL, {'workflow_ids': paused},
        ).fetchall()
        abandoned: list[uuid.UUID] = []
        for task_id, outcome in outcomes:
            assert outcome == 'APPLIED', f'a workflow-scoped abandon returned {outcome}'
            assert isinstance(task_id, uuid.UUID)
            abandoned.append(task_id)
        if abandoned:
            _ = connection.execute(RESET_ABANDONED_WORKFLOW_TASKS_SQL, {'task_ids': abandoned})
        return Paused()
