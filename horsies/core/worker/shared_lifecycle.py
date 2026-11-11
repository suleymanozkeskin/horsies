"""Lifecycle writes of a shared async child, on its event loop.

A shared async child runs many tasks on one event loop, so no write of the
child may block that loop. These functions are the async form of the writes
that an isolated child makes on a sync connection in ``child_runner``:

- the RUNNING transition with the first runner heartbeat, and its miss path
  (expiry before start, a paused or cancelled workflow, a lost claim);
- the finalizing marker after the task body.

They run the same SQL text, from the constants in ``child_runner``, on a
psycopg ``AsyncConnectionPool`` that the child opens on its loop. The pool is
in autocommit (``child_pool``): a one-statement write is its own transaction.
Each function mirrors the sync function of the same name in ``child_runner``:
a change to the behavior of one is a change to both.
"""

# pyright: reportPrivateUsage=false

from __future__ import annotations

import asyncio
from datetime import datetime
from typing import Any, Final, assert_never

from psycopg import AsyncConnection, AsyncCursor
from psycopg.rows import namedtuple_row
from psycopg_pool import AsyncConnectionPool

from horsies.core.codec.error_payload import serialize_error_payload
from horsies.core.lifecycle.commands import (
    AbandonOwnedNode,
    CancelOwnedNode,
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
from horsies.core.lifecycle.persistence import apply_psycopg_async
from horsies.core.logging import get_logger
from horsies.core.models.tasks import OutcomeCode, TaskError, TaskResult
from horsies.core.worker.child_pool import SHARED_POOL_CONNECTION_WAIT_S
from horsies.core.worker.child_protocol import WireResult
from horsies.core.worker.child_runner import (
    BEGIN_SQL,
    CLAIM_STILL_STARTABLE_SQL,
    MARK_FINALIZING_SQL,
    RESET_PAUSED_NODE_SQL,
    SKIP_CANCELLED_NODE_SQL,
    START_WORKFLOW_NODE_SQL,
    WORKFLOW_STATUS_OF_TASK_SQL,
    _PRE_START_CONFIRM_ATTEMPTS,
    _WorkflowResumedBeforeStart,
    set_running_params,
    set_running_sql,
)

logger = get_logger('worker')

FINALIZING_MARKER_TIMEOUT_S: Final = 10.0
"""The longest time a seat waits to write the finalizing marker. The marker
is best effort: past this time the seat logs and reports its end, and the
runner heartbeat of the task ends when the parent reads that end."""

RUNNING_STATEMENTS_TIMEOUT_S: Final = 10.0
"""The longest time that the statements of one attempt of the RUNNING
transition take once the attempt holds its connection: the UPDATE and, when
it matched no row, the statements that find which guard fired. An UPDATE
that waits on a row lock, or a server that does not answer, ends here."""

RUNNING_ATTEMPT_TIMEOUT_S: Final = (
    SHARED_POOL_CONNECTION_WAIT_S + RUNNING_STATEMENTS_TIMEOUT_S
)
"""The bound of one attempt of the RUNNING transition: the wait for a pooled
connection and the statements. Past it the attempt ends with
``OWNERSHIP_UNCONFIRMED``."""

CANCELLED_WRITE_END_S: Final = 10.0
"""The longest time that a write which a bound cancelled takes to end:
psycopg asks the server to cancel the statement and waits for the reply,
each for at most 5 s, before the cancel leaves the write."""

REFUSAL_WRITES_BOUND_S: Final = (
    _PRE_START_CONFIRM_ATTEMPTS * (RUNNING_ATTEMPT_TIMEOUT_S + CANCELLED_WRITE_END_S)
    + FINALIZING_MARKER_TIMEOUT_S
    + CANCELLED_WRITE_END_S
)
"""The longest time that the writes of a run take in a shared async child
before it reports a refusal: each attempt of the RUNNING transition and the
finalizing marker, each with the end of a cancelled write. The refusal
backstop of the parent (``slots.SHARED_REFUSAL_BACKSTOP_S``) comes after it."""


async def _get_workflow_status_for_task(
    cursor: AsyncCursor[Any],
    task_id: str,
) -> str | None:
    """Read the owning workflow's status for a task (None if not in one)."""
    await cursor.execute(WORKFLOW_STATUS_OF_TASK_SQL, (task_id,))
    row = await cursor.fetchone()
    if row is None:
        return None
    status = row.status
    if isinstance(status, str):
        return status
    return None


async def _claim_still_startable(
    cursor: AsyncCursor[Any],
    task_id: str,
    worker_id: str,
    claimed_at: datetime | None,
) -> bool:
    """Whether every guard of the RUNNING transition but the workflow holds."""
    await cursor.execute(
        CLAIM_STILL_STARTABLE_SQL,
        (task_id, worker_id, claimed_at, claimed_at),
    )
    row = await cursor.fetchone()
    assert row is not None, 'SELECT EXISTS returns one row'
    return bool(row[0])


async def _abandon_paused_node(
    cursor: AsyncCursor[Any],
    conn: AsyncConnection[Any],
    task_id: str,
    worker_id: str,
    claimed_at: datetime | None,
) -> WireResult | _WorkflowResumedBeforeStart:
    """The PAUSED branch of ``_handle_workflow_stop_before_start``."""
    outcome = await apply_psycopg_async(
        cursor,
        AbandonOwnedNode(
            task_id=task_id,
            fence=OwnedClaim(worker_id=worker_id, claimed_at=claimed_at),
        ),
    )
    match outcome:
        case SourceStateConflict(
            evidence=ObservedWorkflowState(
                workflow_id=workflow_id,
                workflow_status=resumed_status,
            ),
        ):
            await conn.rollback()
            logger.info(
                f'Task {task_id} abandon refused: workflow {workflow_id} '
                f'is {resumed_status}, no longer PAUSED'
            )
            return _WorkflowResumedBeforeStart(workflow_status=resumed_status)
        case Applied() | AlreadyApplied():
            await cursor.execute(RESET_PAUSED_NODE_SQL, (task_id,))
        case LostClaim() | SourceStateConflict() | TaskAbsent():
            pass
        case _ as unreachable:
            assert_never(unreachable)
    await conn.commit()
    return (False, '', 'WORKFLOW_STOPPED')


async def _cancel_node(
    cursor: AsyncCursor[Any],
    conn: AsyncConnection[Any],
    task_id: str,
    worker_id: str,
    claimed_at: datetime | None,
) -> WireResult:
    """The CANCELLED branch of ``_handle_workflow_stop_before_start``."""
    outcome = await apply_psycopg_async(
        cursor,
        CancelOwnedNode(
            task_id=task_id,
            fence=OwnedClaim(worker_id=worker_id, claimed_at=claimed_at),
            accepts_requeued_pending=True,
        ),
    )
    match outcome:
        case Applied() | AlreadyApplied():
            await cursor.execute(SKIP_CANCELLED_NODE_SQL, (task_id,))
        case LostClaim() | SourceStateConflict() | TaskAbsent():
            pass
        case _ as unreachable:
            assert_never(unreachable)
    await conn.commit()
    return (False, '', 'WORKFLOW_STOPPED')


async def _handle_workflow_stop_before_start(
    cursor: AsyncCursor[Any],
    conn: AsyncConnection[Any],
    task_id: str,
    workflow_status: str,
    worker_id: str,
    claimed_at: datetime | None,
) -> WireResult | _WorkflowResumedBeforeStart:
    """Skip a task whose workflow is PAUSED or CANCELLED. Same contract as
    the sync function of this name in ``child_runner``."""
    logger.info(
        f'Blocking task {task_id} execution before start - workflow is {workflow_status}'
    )
    match workflow_status:
        case 'PAUSED':
            return await _abandon_paused_node(
                cursor,
                conn,
                task_id,
                worker_id,
                claimed_at,
            )
        case 'CANCELLED':
            return await _cancel_node(cursor, conn, task_id, worker_id, claimed_at)
        case _:
            return (False, '', 'WORKFLOW_CHECK_FAILED')


async def _expire_claimed_task_before_start(
    cursor: AsyncCursor[Any],
    conn: AsyncConnection[Any],
    task_id: str,
    worker_id: str,
) -> WireResult | None:
    """Expire a claimed task whose ``good_until`` passed. None: not expired."""
    task_result: TaskResult[None, TaskError] = TaskResult(
        err=TaskError(
            error_code=OutcomeCode.TASK_EXPIRED,
            message='Task expired: good_until deadline passed before execution started',
            data={'task_id': task_id, 'worker_id': worker_id},
        ),
    )
    outcome = await apply_psycopg_async(
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
            await conn.commit()
            logger.info(
                f'Task {task_id} expired before actual execution start; marked EXPIRED.',
            )
            return (False, '', OutcomeCode.TASK_EXPIRED.value)
        case LostClaim() | SourceStateConflict() | TaskAbsent():
            return None
        case _ as unreachable:
            assert_never(unreachable)


async def _after_missed_start(
    cursor: AsyncCursor[Any],
    conn: AsyncConnection[Any],
    task_id: str,
    worker_id: str,
    is_workflow_task: bool,
    claimed_at: datetime | None,
) -> WireResult | _WorkflowResumedBeforeStart:
    """The RUNNING transition matched no row: find which guard fired."""
    expired_result = await _expire_claimed_task_before_start(
        cursor,
        conn,
        task_id,
        worker_id,
    )
    if expired_result is not None:
        return expired_result
    if is_workflow_task:
        workflow_status = await _get_workflow_status_for_task(cursor, task_id)
        match workflow_status:
            case 'PAUSED' | 'CANCELLED':
                return await _handle_workflow_stop_before_start(
                    cursor,
                    conn,
                    task_id,
                    workflow_status,
                    worker_id,
                    claimed_at,
                )
            case 'RUNNING' if await _claim_still_startable(
                cursor,
                task_id,
                worker_id,
                claimed_at,
            ):
                # The transition missed on the paused guard, and a resume
                # committed before this read.
                await conn.rollback()
                return _WorkflowResumedBeforeStart(workflow_status=workflow_status)
            case _:
                pass
    await conn.rollback()
    logger.warning(
        f'Task {task_id} ownership lost - claim was reclaimed or status changed. '
        f'Aborting execution to prevent double-execution.'
    )
    return (False, '', 'CLAIM_LOST')


async def _start_workflow_node(
    cursor: AsyncCursor[Any],
    conn: AsyncConnection[Any],
    task_id: str,
) -> WireResult | None:
    """Set the workflow node RUNNING, unless it is already. None: done."""
    await cursor.execute(START_WORKFLOW_NODE_SQL, (task_id,))
    handoff_row = await cursor.fetchone()
    assert handoff_row is not None, 'SELECT EXISTS returns one row'
    if bool(handoff_row[0]):
        return None
    await conn.rollback()
    logger.error(f'Failed to update workflow_tasks to RUNNING for task {task_id}')
    return (False, '', 'WORKFLOW_CHECK_FAILED')


async def _begin_when_workflow(
    cursor: AsyncCursor[Any],
    is_workflow_task: bool,
) -> None:
    """Open the transaction of the RUNNING transition of a workflow task.
    Same contract as the sync function of this name in ``child_runner``."""
    match is_workflow_task:
        case True:
            await cursor.execute(BEGIN_SQL)
        case False:
            pass


async def _confirm_ownership_and_set_running(
    pool: AsyncConnectionPool[AsyncConnection[Any]],
    task_id: str,
    worker_id: str,
    is_workflow_task: bool,
    claimed_at: datetime | None,
) -> WireResult | None | _WorkflowResumedBeforeStart:
    """Set the row RUNNING with its first runner heartbeat. None: started.

    Same contract as the sync function of this name in ``child_runner``. An
    ``Exception`` (database, pool wait) becomes ``OWNERSHIP_UNCONFIRMED``,
    and so does an attempt that runs past ``RUNNING_ATTEMPT_TIMEOUT_S``: the
    bound cancels its write. A cancelled write rolls back when the
    connection returns to the pool, or it committed before the cancel, as
    after a database error. A cancel of the calling task propagates.
    """
    try:
        async with asyncio.timeout(RUNNING_ATTEMPT_TIMEOUT_S):
            return await _set_running_on_a_connection(
                pool,
                task_id,
                worker_id,
                is_workflow_task,
                claimed_at,
            )
    except TimeoutError:
        logger.error(
            'Failed to transition task %s to RUNNING: the attempt did not '
            'finish within %ss',
            task_id,
            RUNNING_ATTEMPT_TIMEOUT_S,
        )
        return (False, '', 'OWNERSHIP_UNCONFIRMED')
    except Exception as e:
        logger.error(f'Failed to transition task {task_id} to RUNNING: {e}')
        return (False, '', 'OWNERSHIP_UNCONFIRMED')


async def _set_running_on_a_connection(
    pool: AsyncConnectionPool[AsyncConnection[Any]],
    task_id: str,
    worker_id: str,
    is_workflow_task: bool,
    claimed_at: datetime | None,
) -> WireResult | None | _WorkflowResumedBeforeStart:
    """One attempt of ``_confirm_ownership_and_set_running``, unbounded.
    Raises what the pool and the statements raise."""
    async with pool.connection() as conn:
        cursor = conn.cursor(row_factory=namedtuple_row)
        await _begin_when_workflow(cursor, is_workflow_task)
        await cursor.execute(
            set_running_sql(is_workflow_task),
            set_running_params(task_id, worker_id, is_workflow_task, claimed_at),
        )
        if await cursor.fetchone() is None:
            return await _after_missed_start(
                cursor,
                conn,
                task_id,
                worker_id,
                is_workflow_task,
                claimed_at,
            )
        if is_workflow_task:
            node_refused = await _start_workflow_node(cursor, conn, task_id)
            if node_refused is not None:
                return node_refused
        await conn.commit()
    return None


async def confirm_until_not_resumed(
    pool: AsyncConnectionPool[AsyncConnection[Any]],
    task_id: str,
    worker_id: str,
    is_workflow_task: bool,
    claimed_at: datetime | None,
) -> WireResult | None:
    """Run the RUNNING transition, again when a resume refused the abandon.

    None: the task starts. A wire tuple: the task stops before user code.
    Same contract as ``_confirm_until_not_resumed`` in ``child_runner``.
    """
    for _ in range(_PRE_START_CONFIRM_ATTEMPTS):
        result = await _confirm_ownership_and_set_running(
            pool,
            task_id,
            worker_id,
            is_workflow_task,
            claimed_at,
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
        f'{_PRE_START_CONFIRM_ATTEMPTS} start attempts; leaving the claim '
        f'to its lease'
    )
    return (False, '', 'OWNERSHIP_UNCONFIRMED')


async def mark_task_finalizing(
    pool: AsyncConnectionPool[AsyncConnection[Any]],
    task_id: str,
    worker_id: str,
    claimed_at: datetime | None,
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
                    (worker_id, task_id, worker_id, claimed_at, claimed_at),
                )
                marked_rows = cursor.rowcount
    except Exception as exc:
        logger.warning('Failed to mark task %s finalizing: %s', task_id, exc)
        return
    if marked_rows == 0:
        logger.debug(
            'Finalizing marker for task %s skipped: the claim changed',
            task_id,
        )
