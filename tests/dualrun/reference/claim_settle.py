"""Claim-time workflow check, before ``horsies_workflow_claimed_tasks_settle``.

``python/horsies/core/worker/claiming.py``
(``ClaimMixin._filter_nonrunnable_workflow_tasks``) sent, in one session
transaction: the lookup of the workflow status of each claimed workflow task
(``horsies_find_non_runnable_workflow_tasks``); for the tasks of PAUSED
workflows, the abandon (``horsies_abandon_owned_nodes``) and the reset of the
nodes of the applied abandons to READY; for the tasks of CANCELLED workflows,
the cancel (``horsies_cancel_owned_nodes``) and the skip of the nodes of the
applied cancels. A pause abandon refused because the workflow is not PAUSED
and not CANCELLED (a resume committed after the lookup) left the task to be
dispatched.
"""

from __future__ import annotations

import uuid
from collections.abc import Sequence
from datetime import datetime
from typing import Final, LiteralString

from tests.dualrun.capture import Connection

LOOKUP_SQL: Final[LiteralString] = """
    SELECT id, status
    FROM horsies_find_non_runnable_workflow_tasks(CAST(%(ids)s AS uuid[]))
"""

ABANDON_OWNED_NODES_SQL: Final[LiteralString] = """
    SELECT task_id, outcome, observed_guard
    FROM horsies_abandon_owned_nodes(
        CAST(%(ids)s AS uuid[]), CAST(%(claimed_ats)s AS timestamptz[]), %(worker_id)s
    )
"""

CANCEL_OWNED_NODES_SQL: Final[LiteralString] = """
    SELECT task_id, outcome, observed_guard
    FROM horsies_cancel_owned_nodes(
        CAST(%(ids)s AS uuid[]), CAST(%(claimed_ats)s AS timestamptz[]), %(worker_id)s
    )
"""

RESET_PAUSED_WORKFLOW_TASKS_SQL: Final[LiteralString] = """
    UPDATE horsies_workflow_tasks
    SET status = 'READY', task_id = NULL, started_at = NULL
    WHERE task_id = ANY(CAST(%(ids)s AS uuid[]))
      AND status IN ('ENQUEUED', 'RUNNING')
"""

SKIP_CANCELLED_WORKFLOW_TASKS_SQL: Final[LiteralString] = """
    UPDATE horsies_workflow_tasks
    SET status = 'SKIPPED',
        completed_at = NOW()
    WHERE task_id = ANY(CAST(%(ids)s AS uuid[]))
      AND status IN ('PENDING', 'READY', 'ENQUEUED')
"""


def _move(
    connection: Connection,
    statement: LiteralString,
    task_ids: Sequence[uuid.UUID],
    claimed_at: dict[uuid.UUID, datetime],
    worker_id: str,
) -> tuple[list[uuid.UUID], set[uuid.UUID]]:
    """One fenced batch move: the applied task ids, and the ids whose pause
    abandon was refused for a workflow that is not CANCELLED."""
    rows = connection.execute(
        statement,
        {
            'ids': list(task_ids),
            'claimed_ats': [claimed_at[task_id] for task_id in task_ids],
            'worker_id': worker_id,
        },
    ).fetchall()
    applied: list[uuid.UUID] = []
    runnable_again: set[uuid.UUID] = set()
    for task_id, outcome, observed_guard in rows:
        assert isinstance(task_id, uuid.UUID)
        match outcome:
            case 'APPLIED':
                applied.append(task_id)
            case 'SOURCE_STATE_CONFLICT':
                match observed_guard:
                    case {'workflow_status': str() as status} if status != 'CANCELLED':
                        runnable_again.add(task_id)
                    case _:
                        pass
            case _:
                pass
    return applied, runnable_again


def settle(
    connection: Connection,
    *,
    worker_id: str,
    claims: Sequence[tuple[uuid.UUID, datetime]],
) -> list[uuid.UUID]:
    """The check of the claimed workflow tasks ``claims`` of ``worker_id``:
    the task ids to dispatch, in input order. ``connection`` is in
    autocommit; the writes run in one transaction."""
    claimed_at = dict(claims)
    task_ids = [task_id for task_id, _ in claims]
    paused: list[uuid.UUID] = []
    cancelled: list[uuid.UUID] = []
    with connection.transaction():
        for task_id, status in connection.execute(LOOKUP_SQL, {'ids': task_ids}).fetchall():
            assert isinstance(task_id, uuid.UUID)
            match status:
                case 'PAUSED':
                    paused.append(task_id)
                case 'CANCELLED':
                    cancelled.append(task_id)
                case _:
                    pass
        blocked = set(paused) | set(cancelled)
        if paused:
            abandoned, resumed = _move(
                connection, ABANDON_OWNED_NODES_SQL, paused, claimed_at, worker_id,
            )
            blocked -= resumed
            if abandoned:
                _ = connection.execute(RESET_PAUSED_WORKFLOW_TASKS_SQL, {'ids': abandoned})
        if cancelled:
            cancelled_owned, _ = _move(
                connection, CANCEL_OWNED_NODES_SQL, cancelled, claimed_at, worker_id,
            )
            if cancelled_owned:
                _ = connection.execute(
                    SKIP_CANCELLED_WORKFLOW_TASKS_SQL, {'ids': cancelled_owned},
                )
    return [task_id for task_id in task_ids if task_id not in blocked]
