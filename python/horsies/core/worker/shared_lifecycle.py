"""Lifecycle writes of a shared async child, on its event loop.

A shared async child runs many tasks on one event loop, so no write of the
child may block that loop. These functions are the async form of the writes
that an isolated child makes in ``child_runner``:

- the RUNNING transition with the first runner heartbeat
  (``horsies_start_attempt``), and its miss path (the expiry before start,
  the stop of a workflow task, a lost claim), through the awaitable methods
  of the core handle of the child (``CoreChildHandle``), with the bound of
  one start attempt in the core;
- the finalizing marker after the task body, with the SQL text of
  ``child_runner`` on a psycopg ``AsyncConnectionPool`` that the child opens
  on its loop. The pool is in autocommit (``child_pool``).

Each function mirrors the sync function of the same name in ``child_runner``,
and the records of the core read the same way (``expired_before_start_of``,
``stopped_before_start_of``): a change to the behavior of one is a change to
both.
"""

# pyright: reportPrivateUsage=false

from __future__ import annotations

import asyncio
import os
import socket
import time
import uuid
from datetime import datetime
from typing import Any, Final, assert_never

from psycopg import AsyncConnection
from psycopg_pool import AsyncConnectionPool

import horsies._core as core
from horsies._core import CoreChildHandle, CoreError
from horsies.core.lifecycle.core_records import task_row_end_of
from horsies.core.logging import get_logger
from horsies.core.types.core_failure import CoreOperationFailed
from horsies.core.worker.child_pool import (
    SHARED_POOL_CONNECTION_WAIT_S,
    _get_child_core_handle,
)
from horsies.core.worker.child_protocol import WireResult
from horsies.core.worker.child_runner import (
    MARK_FINALIZING_SQL,
    _PRE_START_CONFIRM_ATTEMPTS,
    FinalizingMark,
    StartMissReason,
    _claim_lost,
    _start_unconfirmed,
    _WorkflowResumedBeforeStart,
    attempt_elapsed_ms,
    expired_before_start_of,
    mark_finalizing_params,
    read_finalizing_mark,
    stopped_before_start_of,
)

logger = get_logger('worker')

FINALIZING_MARKER_TIMEOUT_S: Final = 10.0
"""The longest time a seat waits to write the finalizing marker. The marker
is best effort: past this time the seat logs and reports its end, and the
runner heartbeat of the task ends when the parent reads that end."""

RUNNING_STATEMENTS_TIMEOUT_S: Final = 10.0
"""The longest run of one statement of an attempt of the RUNNING transition
before the core sends its cancel request: the start and, when it missed,
the statement of its miss path (the core's ``CHILD_STATEMENT_BOUND``)."""

RUNNING_ATTEMPT_TIMEOUT_S: Final = (
    SHARED_POOL_CONNECTION_WAIT_S + RUNNING_STATEMENTS_TIMEOUT_S
)
"""The bound of one attempt of the RUNNING transition: the wait for a pooled
connection and the statements (the core's ``START_ATTEMPT_BOUND``). The
core ends each write of the attempt by it, and a statement past it gets a
cancel request, so the start does not commit after the bound. A seat also
ends its wait for the attempt here; past it the attempt ends with
``OWNERSHIP_UNCONFIRMED``."""

CORE_CANCEL_REQUEST_BOUND_S: Final = 5.0
"""The longest time of the cancel request that the core sends for a
statement past its bound: the connection that carries the request and the
request itself (the core's ``CANCEL_REQUEST_BOUND``)."""

CORE_CANCEL_GRACE_S: Final = 5.0
"""The time that the core waits for the reply of a cancelled statement after
its cancel request; past it the connection is discarded and the outcome is
unknown (the core's ``CANCEL_GRACE``)."""

CANCELLED_WRITE_END_S: Final = CORE_CANCEL_REQUEST_BOUND_S + CORE_CANCEL_GRACE_S
"""The longest time that a statement which a bound cancelled takes to end:
the cancel request and the wait for its reply (the core's
``CANCELLED_WRITE_END``). The finalizing marker of this module takes the
same time: psycopg asks the server to cancel the statement and waits for the
reply."""

_EXPIRE_BEFORE_START_SUBJECT: Final = 'CoreChildHandle.expire_owned_claim_async'
"""The subject of a failed expiry of the miss path."""

REFUSAL_WRITES_BOUND_S: Final = (
    _PRE_START_CONFIRM_ATTEMPTS * (RUNNING_ATTEMPT_TIMEOUT_S + CANCELLED_WRITE_END_S)
    + FINALIZING_MARKER_TIMEOUT_S
    + CANCELLED_WRITE_END_S
)
"""The longest time that the writes of a run take in a shared async child
before it reports a refusal: each attempt of the RUNNING transition and the
finalizing marker, each with the end of a cancelled write. The refusal
backstop of the parent (``slots.SHARED_REFUSAL_BACKSTOP_S``) comes after it."""


async def _after_missed_start(
    handle: CoreChildHandle,
    task_id: str,
    worker_id: str,
    reason: StartMissReason,
    claimed_at: datetime,
    *,
    orphan_self_heal: bool,
    attempt_began: float,
) -> WireResult | _WorkflowResumedBeforeStart:
    """The path of a start that missed, by its reason. Same contract as the
    sync function of this name in ``child_runner``: the path ends by the end
    of the start attempt that began at ``attempt_began``."""
    match reason:
        case 'WORKFLOW_PAUSED' | 'WORKFLOW_CANCELLED' | 'NODE_NOT_LINKED':
            logger.info(
                f'Blocking task {task_id} execution before start - the start '
                f'missed with {reason}'
            )
            return stopped_before_start_of(
                task_id,
                await handle.stop_workflow_task_before_start_async(
                    uuid.UUID(task_id),
                    worker_id=worker_id,
                    claimed_at=claimed_at,
                    orphan_self_heal=orphan_self_heal,
                    attempt_elapsed_ms=attempt_elapsed_ms(attempt_began),
                ),
            )
        case 'DEADLINE_PASSED':
            return expired_before_start_of(
                task_id,
                task_row_end_of(
                    await handle.expire_owned_claim_async(
                        uuid.UUID(task_id),
                        worker_id=worker_id,
                        attempt_elapsed_ms=attempt_elapsed_ms(attempt_began),
                    ),
                    _EXPIRE_BEFORE_START_SUBJECT,
                ),
            )
        case 'NOT_HELD' | 'LEASE_EXPIRED':
            return _claim_lost(task_id)
        case _ as unreachable:
            assert_never(unreachable)


async def _confirm_ownership_and_set_running(
    task_id: str,
    worker_id: str,
    is_workflow_task: bool,
    claimed_at: datetime,
    *,
    orphan_self_heal: bool,
) -> WireResult | None | _WorkflowResumedBeforeStart:
    """Set the row RUNNING with its first runner heartbeat. None: started.

    Same contract as the sync function of this name in ``child_runner``. A
    failure record of the core and a ``CoreError`` of the core boundary
    become ``OWNERSHIP_UNCONFIRMED``. The core bounds each write of the
    attempt by ``RUNNING_ATTEMPT_TIMEOUT_S`` (its ``START_ATTEMPT_BOUND``): a
    start that waits on a row lock gets a cancel request at its bound and
    ends with a failure record, and it does not commit after the bound. The
    wait of the seat ends at ``RUNNING_ATTEMPT_TIMEOUT_S`` too, also when
    the core call does not answer; the write then ends at the bound of the
    core. A cancel of the calling task propagates.
    """
    try:
        async with asyncio.timeout(RUNNING_ATTEMPT_TIMEOUT_S):
            return await _set_running(
                _get_child_core_handle(),
                task_id,
                worker_id,
                is_workflow_task,
                claimed_at,
                orphan_self_heal=orphan_self_heal,
            )
    except TimeoutError:
        logger.error(
            'Failed to transition task %s to RUNNING: the attempt did not '
            'finish within %ss',
            task_id,
            RUNNING_ATTEMPT_TIMEOUT_S,
        )
        return (False, '', 'OWNERSHIP_UNCONFIRMED')
    except (CoreError, CoreOperationFailed) as exc:
        return _start_unconfirmed(task_id, exc)


async def _set_running(
    handle: CoreChildHandle,
    task_id: str,
    worker_id: str,
    is_workflow_task: bool,
    claimed_at: datetime,
    *,
    orphan_self_heal: bool,
) -> WireResult | None | _WorkflowResumedBeforeStart:
    """One attempt of ``_confirm_ownership_and_set_running``: one
    ``horsies_start_attempt`` statement through ``handle``, then the miss
    path, each within the bound of the attempt in the core.
    ``is_workflow_task`` is not sent: the function reads it from the locked
    row. A shared async seat starts the deadline of its task after the
    start, so the start has no deadline.

    Raises:
        CoreError: the core of this process cannot run the call.
        CoreOperationFailed: the expiry of the miss path failed.
    """
    attempt_began = time.monotonic()
    record = await handle.start_attempt_async(
        uuid.UUID(task_id),
        worker_id=worker_id,
        claimed_at=claimed_at,
        worker_pid=os.getpid(),
        worker_hostname=socket.gethostname(),
        deadline_in_ms=None,
    )
    match record:
        case core.AttemptStarted():
            return None
        case core.AttemptMissed(reason=reason):
            return await _after_missed_start(
                handle,
                task_id,
                worker_id,
                reason,
                claimed_at,
                orphan_self_heal=orphan_self_heal,
                attempt_began=attempt_began,
            )
        case core.DeadlineBeforeStart():
            raise AssertionError(
                f'the start of task {task_id} sent no deadline and the core '
                'answered DeadlineBeforeStart',
            )
        case core.StartFailed(message=message):
            return _start_unconfirmed(task_id, message)
        case _ as unreachable:
            assert_never(unreachable)


async def confirm_until_not_resumed(
    task_id: str,
    worker_id: str,
    is_workflow_task: bool,
    claimed_at: datetime,
    *,
    orphan_self_heal: bool,
) -> WireResult | None:
    """Run the RUNNING transition, again when a resume came before the stop.

    None: the task starts. A wire tuple: the task stops before user code.
    Same contract as ``_confirm_until_not_resumed`` in ``child_runner``.
    """
    for _ in range(_PRE_START_CONFIRM_ATTEMPTS):
        result = await _confirm_ownership_and_set_running(
            task_id,
            worker_id,
            is_workflow_task,
            claimed_at,
            orphan_self_heal=orphan_self_heal,
        )
        match result:
            case _WorkflowResumedBeforeStart():
                continue
            case None | tuple():
                return result
            case _ as unreachable:
                assert_never(unreachable)
    logger.warning(
        f'Task {task_id} workflow was resumed on each of '
        f'{_PRE_START_CONFIRM_ATTEMPTS} start attempts; the worker releases '
        f'the claim'
    )
    return (False, '', 'OWNERSHIP_UNCONFIRMED')


async def mark_task_finalizing(
    pool: AsyncConnectionPool[AsyncConnection[Any]],
    task_id: str,
    worker_id: str,
    claimed_at: datetime,
) -> None:
    """Mark the row of a task whose body ended as waiting for the parent.

    Same contract as ``_mark_task_finalizing`` in ``child_runner``, bounded
    by ``FINALIZING_MARKER_TIMEOUT_S``. A failed or late write is logged
    only. A cancel of the calling task propagates.
    """
    try:
        async with asyncio.timeout(FINALIZING_MARKER_TIMEOUT_S):
            async with pool.connection() as conn:
                # One statement on an autocommit connection: committed when
                # it returns.
                cursor = conn.cursor()
                await cursor.execute(
                    MARK_FINALIZING_SQL,
                    mark_finalizing_params(task_id, worker_id, claimed_at),
                )
                row = await cursor.fetchone()
    except Exception as exc:
        logger.warning('Failed to mark task %s finalizing: %s', task_id, exc)
        return
    match read_finalizing_mark(row, task_id):
        case FinalizingMark.MARKED:
            pass
        case FinalizingMark.NOT_HELD:
            logger.debug(
                'Finalizing marker for task %s skipped: the claim changed',
                task_id,
            )
