"""Finalizing marker of a child, before ``horsies_mark_finalizing``.

``_mark_task_finalizing`` (``python/horsies/core/worker/child_runner.py``)
and its async copy ``mark_task_finalizing`` (``shared_lifecycle.py``) sent
this statement in autocommit: the row read by id alone in a MATERIALIZED CTE
that locks it, the fence (RUNNING, this worker, this claim generation)
checked as one boolean on the locked row, and the update by that id. The
caller read the changed row count: 0 meant the claim changed.
"""

from __future__ import annotations

from datetime import datetime
from typing import Final, LiteralString

from tests.dualrun.capture import Connection

MARK_FINALIZING_SQL: Final[LiteralString] = """
                WITH target AS MATERIALIZED (
                    SELECT id,
                           (
                               status = 'RUNNING'
                               AND claimed_by_worker_id = %s
                               AND claimed_at = %s::timestamptz
                           ) IS TRUE AS fenced
                    FROM horsies_tasks
                    WHERE id = %s
                    FOR NO KEY UPDATE
                )
                UPDATE horsies_tasks t
                SET finalizing_at = NOW(),
                    finalizing_by_worker_id = %s,
                    updated_at = NOW()
                FROM target
                WHERE t.id = target.id
                  AND target.fenced
                """


def mark(
    connection: Connection, task_id: str, worker_id: str, claimed_at: datetime
) -> str:
    """The old marker on an autocommit connection: ``MARKED`` when it changed
    the row, ``NOT_HELD`` when it changed none."""
    changed = connection.execute(
        MARK_FINALIZING_SQL, (worker_id, claimed_at, task_id, worker_id)
    ).rowcount
    match changed:
        case 1:
            return 'MARKED'
        case 0:
            return 'NOT_HELD'
        case _:
            raise AssertionError(f'the marker of task {task_id} changed {changed} rows')
