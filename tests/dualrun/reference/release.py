"""Release and return as ISOLATED, before ``horsies_release_claims``.

``python/horsies/core/worker/dispatch.py`` (``_requeue_claimed_task``) sent
``UNCLAIM_CLAIMED_TASK_SQL`` in a session transaction per row.
``python/horsies/core/worker/claiming.py`` (``_return_row_as_isolated``) sent
``RETURN_CLAIMED_TASK_AS_ISOLATED_SQL`` per row and, when it updated a row,
the ``class:<id>`` wake of its queue, in one transaction per row.
"""

from __future__ import annotations

from datetime import datetime
from typing import Final, LiteralString

from tests.dualrun.capture import Connection

UNCLAIM_CLAIMED_TASK_SQL: Final[LiteralString] = """
    UPDATE horsies_tasks
    SET status = 'PENDING',
        claimed_at = NULL,
        claimed_by_worker_id = NULL,
        claim_expires_at = NULL,
        started_at = NULL,
        worker_pid = NULL,
        worker_hostname = NULL,
        worker_process_name = NULL,
        finalizing_at = NULL,
        finalizing_by_worker_id = NULL,
        updated_at = NOW()
    WHERE id = %(id)s
      AND status = 'CLAIMED'
      AND claimed_by_worker_id = CAST(%(wid)s AS VARCHAR)
      AND claimed_at = CAST(%(claimed_at)s AS TIMESTAMPTZ)
"""

RETURN_CLAIMED_TASK_AS_ISOLATED_SQL: Final[LiteralString] = """
    UPDATE horsies_tasks
    SET execution = 'ISOLATED',
        status = 'PENDING',
        claimed_at = NULL,
        claimed_by_worker_id = NULL,
        claim_expires_at = NULL,
        started_at = NULL,
        worker_pid = NULL,
        worker_hostname = NULL,
        worker_process_name = NULL,
        finalizing_at = NULL,
        finalizing_by_worker_id = NULL,
        updated_at = NOW()
    WHERE id = %(id)s
      AND status = 'CLAIMED'
      AND claimed_by_worker_id = CAST(%(wid)s AS VARCHAR)
      AND claimed_at = CAST(%(claimed_at)s AS TIMESTAMPTZ)
"""

NOTIFY_TASK_QUEUE_SQL: Final[LiteralString] = """
    SELECT pg_notify(%(c2)s, %(p)s)
"""


def unclaim(connection: Connection, task_id: str, worker_id: str, claimed_at: datetime) -> bool:
    """One row, in its own transaction. Whether the update matched the row."""
    with connection.transaction():
        cursor = connection.execute(
            UNCLAIM_CLAIMED_TASK_SQL,
            {'id': task_id, 'wid': worker_id, 'claimed_at': claimed_at},
        )
        return cursor.rowcount > 0


def return_as_isolated(
    connection: Connection,
    task_id: str,
    queue_name: str,
    worker_id: str,
    claimed_at: datetime,
) -> bool:
    """One row and its wake, in one transaction. Whether the update matched the row."""
    with connection.transaction():
        cursor = connection.execute(
            RETURN_CLAIMED_TASK_AS_ISOLATED_SQL,
            {'id': task_id, 'wid': worker_id, 'claimed_at': claimed_at},
        )
        returned = cursor.rowcount > 0
        if returned:
            _ = connection.execute(
                NOTIFY_TASK_QUEUE_SQL,
                {'c2': f'task_queue_{queue_name}', 'p': f'class:{task_id}'},
            )
        return returned
