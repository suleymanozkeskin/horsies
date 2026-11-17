"""Start of a claimed task, before ``horsies_start_attempt``.

``_confirm_ownership_and_set_running`` (``python/horsies/core/worker/child_runner.py``,
and its async copy in ``shared_lifecycle.py``) sent the RUNNING update with its
first runner beat (a workflow task in a transaction, with the node write
``START_WORKFLOW_NODE_SQL`` after it). A start that matched no row rolled its
transaction back and ran the miss path: the expiry, then for a workflow task
the workflow status read and the abandon or the cancel of the task with the
node write. The miss path's commands (``ExpireOwnedClaim``,
``AbandonOwnedNode``, ``CancelOwnedNode``) are not part of this switch and are
called through the production adapter.
"""

from __future__ import annotations

import os
import socket

import psycopg
from datetime import datetime
from typing import Any, Final, LiteralString

from horsies.core.codec.error_payload import serialize_error_payload
from horsies.core.lifecycle.commands import AbandonOwnedNode, CancelOwnedNode, ExpireOwnedClaim
from horsies.core.lifecycle.fences import OwnedClaim, WorkerOwned
from horsies.core.lifecycle.outcomes import AlreadyApplied, Applied
from horsies.core.lifecycle.persistence import apply_sync
from horsies.core.models.tasks import OutcomeCode, TaskError, TaskResult
from tests.dualrun.capture import Connection

WORKFLOW_STATUS_OF_TASK_SQL: Final[LiteralString] = """
        SELECT w.status FROM horsies_workflows w
        JOIN horsies_workflow_tasks wt ON wt.workflow_id = w.id
        WHERE wt.task_id = %s
        """

_SET_RUNNING_HEAD_SQL: Final[LiteralString] = """
                WITH target AS MATERIALIZED (
                    SELECT id,
                           (
                               status = 'CLAIMED'
                               AND claimed_by_worker_id = %s
                               AND claimed_at = %s::timestamptz
                               AND (claim_expires_at IS NULL OR claim_expires_at > now())
                               AND (good_until IS NULL OR good_until > now())
                           ) IS TRUE AS startable
                    FROM horsies_tasks
                    WHERE id = %s
                    FOR NO KEY UPDATE
                ),
                started AS (
                    UPDATE horsies_tasks t
                    SET status = 'RUNNING',
                        claim_expires_at = NULL,
                        started_at = NOW(),
                        worker_pid = %s,
                        worker_hostname = %s,
                        worker_process_name = %s,
                        updated_at = NOW()
                    FROM target
                    WHERE t.id = target.id
                      AND target.startable
                """

_SET_RUNNING_WORKFLOW_GUARD_SQL: Final[LiteralString] = """
                  AND NOT EXISTS (
                      SELECT 1
                      FROM horsies_workflow_tasks wt
                      JOIN horsies_workflows w ON w.id = wt.workflow_id
                      WHERE wt.task_id = %s
                        AND w.status IN ('PAUSED', 'CANCELLED')
                  )
            """

_SET_RUNNING_TAIL_SQL: Final[LiteralString] = """
                    RETURNING t.id
                ),
                first_beat AS (
                    INSERT INTO horsies_heartbeats (
                        task_id, sender_id, role, sent_at, hostname, pid
                    )
                    SELECT id, %s, 'runner', NOW(), %s, %s
                    FROM started
                )
                SELECT id FROM started
                """

START_WORKFLOW_NODE_SQL: Final[LiteralString] = """
                    WITH linked AS MATERIALIZED (
                        SELECT id, status
                        FROM horsies_workflow_tasks
                        WHERE task_id = %s
                          AND status IN ('ENQUEUED', 'READY', 'PENDING', 'RUNNING')
                        FOR UPDATE
                    ),
                    changed AS (
                        UPDATE horsies_workflow_tasks node
                        SET status = 'RUNNING',
                            started_at = NOW()
                        FROM linked
                        WHERE node.id = linked.id
                          AND linked.status <> 'RUNNING'
                        RETURNING node.id
                    )
                    SELECT EXISTS (SELECT 1 FROM linked) AS node_linked
                    """

RESET_PAUSED_NODE_SQL: Final[LiteralString] = """
    UPDATE horsies_workflow_tasks
    SET status = 'READY', task_id = NULL, started_at = NULL
    WHERE task_id = %s AND status IN ('ENQUEUED', 'RUNNING')
"""

SKIP_CANCELLED_NODE_SQL: Final[LiteralString] = """
    UPDATE horsies_workflow_tasks
    SET status = 'SKIPPED', completed_at = NOW()
    WHERE task_id = %s AND status IN ('ENQUEUED', 'READY')
"""


def _set_running_sql(is_workflow_task: bool) -> LiteralString:
    if is_workflow_task:
        return _SET_RUNNING_HEAD_SQL + _SET_RUNNING_WORKFLOW_GUARD_SQL + _SET_RUNNING_TAIL_SQL
    return _SET_RUNNING_HEAD_SQL + _SET_RUNNING_TAIL_SQL


def _set_running_params(
    task_id: str, worker_id: str, is_workflow_task: bool, claimed_at: datetime
) -> tuple[object, ...]:
    pid = os.getpid()
    hostname = socket.gethostname()
    params: list[object] = [worker_id, claimed_at, task_id, pid, hostname, f'worker-{pid}']
    if is_workflow_task:
        params.append(task_id)
    params += [f'{worker_id}:{pid}', hostname, pid]
    return tuple(params)


def _expire(connection: Connection, task_id: str, worker_id: str) -> bool:
    """The expiry of the miss path, in autocommit. Whether it applied."""
    result = TaskResult[None, TaskError](
        err=TaskError(
            error_code=OutcomeCode.TASK_EXPIRED,
            message='Task expired: good_until deadline passed before execution started',
            data={'task_id': task_id, 'worker_id': worker_id},
        ),
    )
    outcome = apply_sync(
        connection.cursor(),
        ExpireOwnedClaim(
            task_id=task_id,
            fence=WorkerOwned(worker_id=worker_id),
            result_json=serialize_error_payload(result),
            error_code=OutcomeCode.TASK_EXPIRED.value,
        ),
    )
    return isinstance(outcome, Applied | AlreadyApplied)


def _stop(connection: Connection, task_id: str, worker_id: str, claimed_at: datetime) -> str | None:
    """The workflow part of the miss path, in one transaction."""
    fence = OwnedClaim(worker_id=worker_id, claimed_at=claimed_at)
    with connection.transaction():
        row = connection.execute(WORKFLOW_STATUS_OF_TASK_SQL, (task_id,)).fetchone()
        status: Any = None if row is None else row[0]
        cursor: Any = connection.cursor()
        match status:
            case 'PAUSED':
                outcome = apply_sync(cursor, AbandonOwnedNode(task_id=task_id, fence=fence))
                if isinstance(outcome, Applied | AlreadyApplied):
                    _ = connection.execute(RESET_PAUSED_NODE_SQL, (task_id,))
                return 'WORKFLOW_STOPPED'
            case 'CANCELLED':
                outcome = apply_sync(
                    cursor,
                    CancelOwnedNode(task_id=task_id, fence=fence, accepts_requeued_pending=True),
                )
                if isinstance(outcome, Applied | AlreadyApplied):
                    _ = connection.execute(SKIP_CANCELLED_NODE_SQL, (task_id,))
                return 'WORKFLOW_STOPPED'
            case _:
                return None


def start(
    connection: Connection,
    task_id: str,
    worker_id: str,
    is_workflow_task: bool,
    claimed_at: datetime,
) -> str:
    """The old start and its miss path. The wire reason, or STARTED."""
    node_refused = False
    with connection.transaction():
        started = connection.execute(
            _set_running_sql(is_workflow_task),
            _set_running_params(task_id, worker_id, is_workflow_task, claimed_at),
        ).fetchone()
        if started is not None and is_workflow_task:
            linked = connection.execute(START_WORKFLOW_NODE_SQL, (task_id,)).fetchone()
            node_refused = linked is None or not linked[0]
        if started is not None and not node_refused:
            return 'STARTED'
        raise psycopg.Rollback()
    if node_refused:
        return 'WORKFLOW_CHECK_FAILED'
    if _expire(connection, task_id, worker_id):
        return OutcomeCode.TASK_EXPIRED.value
    if is_workflow_task:
        stopped = _stop(connection, task_id, worker_id, claimed_at)
        if stopped is not None:
            return stopped
    return 'CLAIM_LOST'
