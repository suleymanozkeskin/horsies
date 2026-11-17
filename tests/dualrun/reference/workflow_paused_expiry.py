"""Paused workflow expiry, before ``horsies_workflow_expire_paused``.

``python/horsies/core/workflows/lifecycle.py`` (``expire_paused_workflows``)
read the candidates, then sent per candidate, in one session transaction:
the workflow row lock, the backing task locks, the node locks, the backing
task locks again, the EXPIRED write (PAUSED and older than the age only);
on no row, a rollback. Else the cancel of the tasks behind its ENQUEUED
nodes (``horsies_cancel_nodes_of_cancelled_workflow``), the skip of its
PENDING and READY nodes and of the ENQUEUED nodes of the cancelled tasks,
and the commit.
"""

from __future__ import annotations

import uuid
from datetime import timedelta
from typing import Final, LiteralString

from horsies.core.codec.json_io import dumps_json
from horsies.core.codec.typed import encode_task_error
from horsies.core.models.tasks import OutcomeCode, TaskError
from horsies.core.types.result import Err, Ok
from tests.dualrun.capture import Connection

_WF_TASK_TERMINAL: Final = ['COMPLETED', 'FAILED', 'SKIPPED']
_TASK_TERMINAL: Final = ['COMPLETED', 'FAILED', 'CANCELLED', 'EXPIRED']

CANDIDATES_SQL: Final[LiteralString] = """
    SELECT id FROM horsies_workflows
    WHERE status = 'PAUSED'
      AND updated_at < NOW()
          - CAST(%(age_us)s AS bigint) * interval '1 microsecond'
    ORDER BY updated_at
    LIMIT %(batch_size)s
"""

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

EXPIRE_WORKFLOW_SQL: Final[LiteralString] = """
    UPDATE horsies_workflows
    SET status = 'EXPIRED',
        error = %(error)s,
        completed_at = NOW(),
        updated_at = NOW()
    WHERE id = %(wf_id)s
      AND status = 'PAUSED'
      AND updated_at < NOW()
          - CAST(%(age_us)s AS bigint) * interval '1 microsecond'
    RETURNING id
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


def _expire_one(
    connection: Connection, workflow_id: uuid.UUID, age_us: int, error_json: str,
) -> bool:
    """The expiry of one candidate in one transaction: True when it wrote EXPIRED."""
    params = {
        'wf_id': workflow_id,
        'wf_task_terminal_states': _WF_TASK_TERMINAL,
        'task_terminal_states': _TASK_TERMINAL,
    }
    with connection.transaction():
        _ = connection.execute(LOCK_WORKFLOW_ROW_SQL, {'wf_id': workflow_id})
        _ = connection.execute(LOCK_BACKING_TASKS_SQL, params)
        _ = connection.execute(LOCK_NODES_SQL, params)
        _ = connection.execute(LOCK_BACKING_TASKS_SQL, params)
        expired = connection.execute(
            EXPIRE_WORKFLOW_SQL,
            {'wf_id': workflow_id, 'error': error_json, 'age_us': age_us},
        ).fetchone()
        if expired is None:
            return False
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
        return True


def expire_paused(
    connection: Connection, *, age_us: int, batch_size: int, error_json: str,
) -> int:
    """The expiry pass: the candidate read, then one transaction per candidate.

    ``connection`` is in autocommit. Returns the number of workflows expired.
    """
    rows = connection.execute(
        CANDIDATES_SQL, {'age_us': age_us, 'batch_size': batch_size},
    ).fetchall()
    expired = 0
    for row in rows:
        workflow_id = row[0]
        assert isinstance(workflow_id, uuid.UUID)
        if _expire_one(connection, workflow_id, age_us, error_json):
            expired += 1
    return expired


def expiry_error_json(older_than: timedelta) -> str:
    """The stored error of an expired workflow, as the pass built it."""
    encoded = dumps_json(
        encode_task_error(
            TaskError(
                error_code=OutcomeCode.WORKFLOW_EXPIRED,
                message=f'paused_workflow_auto_cancel_after elapsed: {older_than}',
                data={
                    'policy': 'paused_workflow_auto_cancel_after',
                    'older_than_seconds': older_than.total_seconds(),
                },
            )
        )
    )
    match encoded:
        case Ok(text):
            return text
        case Err(error):
            raise AssertionError(f'the expired workflow error does not encode: {error}')
