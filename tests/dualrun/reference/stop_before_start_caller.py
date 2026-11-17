"""The start of a run in a worker's child and its miss path, before the
child called the core.

``_confirm_ownership_and_set_running``
(``python/horsies/core/worker/child_runner.py``, and its async copy in
``shared_lifecycle.py``) sent ``horsies_start_attempt`` in autocommit on the
sync pool of the child. A miss ran its path by its reason, after the start
statement ended:

- ``DEADLINE_PASSED``: ``horsies_expire_owned_claim`` (``ExpireOwnedClaim``);
  a refused expiry was a lost claim;
- ``NOT_HELD``, ``LEASE_EXPIRED``: a lost claim, no statement;
- ``WORKFLOW_PAUSED``: ``BEGIN``, ``horsies_abandon_owned_node`` and, when the
  abandon applied, the node reset to READY, then ``COMMIT``; an abandon
  refused because a resume came first rolled back, and the start ran once
  more;
- ``WORKFLOW_CANCELLED``: ``BEGIN``, ``horsies_cancel_owned_node`` and, when
  the cancel applied, the node skip, then ``COMMIT``;
- ``NODE_NOT_LINKED``: no statement in the child, which reported
  ``WORKFLOW_CHECK_FAILED``. The worker's finalize then cancelled the owned
  orphan with ``horsies_cancel_owned_orphan`` in autocommit when the app's
  ``auto_terminate_orphaned_workflow_tasks`` was on
  (``FinalizeMixin._cancel_owned_orphan``).

The functions below are those bodies, with the cancel of the finalize folded
in: with ``orphan_self_heal`` the reference sends it right after the miss and
reports ``ORPHAN_CANCELLED``, so a run of the reference ends in the same rows
as the child of the switch. Their signatures are those of the production
functions, so the dual-run rebinding puts them in place of
``child_runner._confirm_ownership_and_set_running`` and
``shared_lifecycle.confirm_until_not_resumed``. The reference of the shared
async child runs the same statements on the sync pool of the child, in a
thread.
"""

# pyright: reportPrivateUsage=false

from __future__ import annotations

import asyncio
import os
import socket
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Final, LiteralString, assert_never

from psycopg.rows import namedtuple_row

from horsies.core.codec.error_payload import serialize_error_payload
from horsies.core.lifecycle.commands import (
    AbandonOwnedNode,
    CancelOwnedNode,
    CancelOwnedOrphan,
    ExpireOwnedClaim,
)
from horsies.core.lifecycle.fences import OwnedClaim, WorkerOwned
from horsies.core.lifecycle.outcomes import (
    AlreadyApplied,
    Applied,
    LostClaim,
    ObservedWorkflowState,
    SourceStateConflict,
    TaskAbsent,
)
from horsies.core.lifecycle.persistence import apply_sync
from horsies.core.models.tasks import OutcomeCode, TaskError, TaskResult
from horsies.core.worker.child_pool import _get_worker_pool
from horsies.core.worker.child_protocol import WireResult
from horsies.core.worker.child_runner import (
    _PRE_START_CONFIRM_ATTEMPTS,
    DeadlineBeforeStart,
    StartAllowed,
    StartCheck,
    StartWithin,
    _WorkflowResumedBeforeStart,
    start_always_allowed,
)

START_ATTEMPT_SQL: Final[LiteralString] = """
                SELECT outcome, miss_reason
                FROM horsies_start_attempt(
                    %s::uuid, %s, %s::timestamptz, %s, %s, %s, %s
                )
                """

RESET_PAUSED_NODE_SQL: Final[LiteralString] = """
                        UPDATE horsies_workflow_tasks
                        SET status = 'READY',
                            task_id = NULL,
                            started_at = NULL
                        WHERE task_id = %s
                          AND status IN ('ENQUEUED', 'RUNNING')
                        """

SKIP_CANCELLED_NODE_SQL: Final[LiteralString] = """
                        UPDATE horsies_workflow_tasks
                        SET status = 'SKIPPED',
                            completed_at = NOW()
                        WHERE task_id = %s
                          AND status IN ('ENQUEUED', 'READY')
                        """

BEGIN_SQL: Final[LiteralString] = 'BEGIN'

_MISS_REASONS: Final = frozenset(
    {
        'DEADLINE_PASSED',
        'NOT_HELD',
        'LEASE_EXPIRED',
        'WORKFLOW_PAUSED',
        'WORKFLOW_CANCELLED',
        'NODE_NOT_LINKED',
    },
)


@dataclass(frozen=True, slots=True)
class _Started:
    """The row is RUNNING with its first runner beat."""


@dataclass(frozen=True, slots=True)
class _Missed:
    """Nothing was written; ``reason`` names the guard."""

    reason: str


def _start_attempt_params(
    task_id: str,
    worker_id: str,
    claimed_at: datetime,
) -> tuple[object, ...]:
    pid = os.getpid()
    return (
        task_id,
        worker_id,
        claimed_at,
        pid,
        socket.gethostname(),
        f'worker-{pid}',
        f'{worker_id}:{pid}',
    )


def _read_start_outcome(row: Sequence[object] | None, task_id: str) -> _Started | _Missed:
    match row:
        case ('STARTED', None):
            return _Started()
        case ('MISSED', str() as reason) if reason in _MISS_REASONS:
            return _Missed(reason=reason)
        case _:
            raise AssertionError(f'horsies_start_attempt returned {row!r} for task {task_id}')


def _claim_lost() -> WireResult:
    return (False, '', 'CLAIM_LOST')


def _expire_claimed_task_before_start(
    cursor: Any,
    conn: Any,
    task_id: str,
    worker_id: str,
) -> WireResult | None:
    task_result: TaskResult[None, TaskError] = TaskResult(
        err=TaskError(
            error_code=OutcomeCode.TASK_EXPIRED,
            message='Task expired: good_until deadline passed before execution started',
            data={'task_id': task_id, 'worker_id': worker_id},
        ),
    )
    outcome = apply_sync(
        cursor,
        ExpireOwnedClaim(
            task_id=task_id,
            fence=WorkerOwned(worker_id=worker_id),
            result_json=serialize_error_payload(task_result),
            error_code=OutcomeCode.TASK_EXPIRED.value,
        ),
    )
    match outcome:
        case Applied() | AlreadyApplied():
            conn.commit()
            return (False, '', OutcomeCode.TASK_EXPIRED.value)
        case LostClaim() | SourceStateConflict() | TaskAbsent():
            return None
        case _ as unreachable:
            assert_never(unreachable)


def _handle_workflow_stop_before_start(
    cursor: Any,
    conn: Any,
    task_id: str,
    workflow_status: str,
    worker_id: str,
    claimed_at: datetime,
) -> WireResult | _WorkflowResumedBeforeStart:
    fence = OwnedClaim(worker_id=worker_id, claimed_at=claimed_at)
    match workflow_status:
        case 'PAUSED':
            outcome = apply_sync(cursor, AbandonOwnedNode(task_id=task_id, fence=fence))
            match outcome:
                case SourceStateConflict(evidence=ObservedWorkflowState()):
                    conn.rollback()
                    return _WorkflowResumedBeforeStart()
                case Applied() | AlreadyApplied():
                    cursor.execute(RESET_PAUSED_NODE_SQL, (task_id,))
                case LostClaim() | SourceStateConflict() | TaskAbsent():
                    pass
                case _ as unreachable:
                    assert_never(unreachable)
            conn.commit()
            return (False, '', 'WORKFLOW_STOPPED')
        case 'CANCELLED':
            outcome = apply_sync(
                cursor,
                CancelOwnedNode(task_id=task_id, fence=fence, accepts_requeued_pending=True),
            )
            match outcome:
                case Applied() | AlreadyApplied():
                    cursor.execute(SKIP_CANCELLED_NODE_SQL, (task_id,))
                case LostClaim() | SourceStateConflict() | TaskAbsent():
                    pass
                case _ as unreachable:
                    assert_never(unreachable)
            conn.commit()
            return (False, '', 'WORKFLOW_STOPPED')
        case _:
            return (False, '', 'WORKFLOW_CHECK_FAILED')


def _node_not_linked(
    cursor: Any,
    task_id: str,
    worker_id: str,
    claimed_at: datetime,
    *,
    orphan_self_heal: bool,
) -> WireResult:
    """The child's WORKFLOW_CHECK_FAILED, then the cancel of the worker's
    finalize when the self-heal was on: ``horsies_cancel_owned_orphan`` in
    autocommit, fenced to this claim."""
    match orphan_self_heal:
        case False:
            return (False, '', 'WORKFLOW_CHECK_FAILED')
        case True:
            _ = apply_sync(
                cursor,
                CancelOwnedOrphan(
                    task_id=task_id,
                    fence=OwnedClaim(worker_id=worker_id, claimed_at=claimed_at),
                ),
            )
            return (False, '', 'ORPHAN_CANCELLED')


def _after_missed_start(
    cursor: Any,
    conn: Any,
    task_id: str,
    worker_id: str,
    reason: str,
    claimed_at: datetime,
    *,
    orphan_self_heal: bool,
) -> WireResult | _WorkflowResumedBeforeStart:
    match reason:
        case 'DEADLINE_PASSED':
            expired = _expire_claimed_task_before_start(cursor, conn, task_id, worker_id)
            if expired is not None:
                return expired
            return _claim_lost()
        case 'NOT_HELD' | 'LEASE_EXPIRED':
            return _claim_lost()
        case 'WORKFLOW_PAUSED':
            cursor.execute(BEGIN_SQL)
            return _handle_workflow_stop_before_start(
                cursor, conn, task_id, 'PAUSED', worker_id, claimed_at,
            )
        case 'WORKFLOW_CANCELLED':
            cursor.execute(BEGIN_SQL)
            return _handle_workflow_stop_before_start(
                cursor, conn, task_id, 'CANCELLED', worker_id, claimed_at,
            )
        case 'NODE_NOT_LINKED':
            return _node_not_linked(
                cursor, task_id, worker_id, claimed_at, orphan_self_heal=orphan_self_heal,
            )
        case _:
            raise AssertionError(f'a miss reason outside the function: {reason!r}')


def _confirm_ownership_and_set_running(
    task_id: str,
    worker_id: str,
    is_workflow_task: bool = True,
    *,
    claimed_at: datetime,
    orphan_self_heal: bool,
    start_check: Callable[[], StartCheck] = start_always_allowed,
) -> WireResult | None | _WorkflowResumedBeforeStart | DeadlineBeforeStart:
    """One ``horsies_start_attempt`` in autocommit on the sync pool of the
    child, then the miss path. ``start_check`` runs once the pool gave the
    connection; a database error is OWNERSHIP_UNCONFIRMED."""
    _ = is_workflow_task
    try:
        with _get_worker_pool().connection() as conn:
            match start_check():
                case StartAllowed() | StartWithin():
                    pass
                case DeadlineBeforeStart() as passed:
                    return passed
                case _ as unreachable:
                    assert_never(unreachable)
            cursor = conn.cursor(row_factory=namedtuple_row)
            cursor.execute(START_ATTEMPT_SQL, _start_attempt_params(task_id, worker_id, claimed_at))
            match _read_start_outcome(cursor.fetchone(), task_id):
                case _Started():
                    return None
                case _Missed(reason=reason):
                    return _after_missed_start(
                        cursor,
                        conn,
                        task_id,
                        worker_id,
                        reason,
                        claimed_at,
                        orphan_self_heal=orphan_self_heal,
                    )
                case _ as unreachable:
                    assert_never(unreachable)
    except Exception:
        return (False, '', 'OWNERSHIP_UNCONFIRMED')


def _confirm_until_not_resumed(
    task_id: str,
    worker_id: str,
    is_workflow_task: bool,
    claimed_at: datetime,
    *,
    orphan_self_heal: bool,
) -> WireResult | None:
    for _ in range(_PRE_START_CONFIRM_ATTEMPTS):
        result = _confirm_ownership_and_set_running(
            task_id,
            worker_id,
            is_workflow_task,
            claimed_at=claimed_at,
            orphan_self_heal=orphan_self_heal,
        )
        match result:
            case _WorkflowResumedBeforeStart():
                continue
            case None | tuple():
                return result
            case DeadlineBeforeStart():
                raise AssertionError('a start with no deadline ended at its deadline')
            case _ as unreachable:
                assert_never(unreachable)
    return (False, '', 'OWNERSHIP_UNCONFIRMED')


async def confirm_until_not_resumed(
    task_id: str,
    worker_id: str,
    is_workflow_task: bool,
    claimed_at: datetime,
    *,
    orphan_self_heal: bool,
) -> WireResult | None:
    """The start of a shared async child before the switch: the same
    statements, here on the sync pool of the child in a thread."""
    return await asyncio.to_thread(
        _confirm_until_not_resumed,
        task_id,
        worker_id,
        is_workflow_task,
        claimed_at,
        orphan_self_heal=orphan_self_heal,
    )
