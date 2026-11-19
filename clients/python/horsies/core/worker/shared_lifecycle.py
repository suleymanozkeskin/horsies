"""Lifecycle writes of a shared async child, on its event loop.

A shared async child runs many tasks on one event loop, so no write of the
child may block that loop. These functions are the async form of the writes
that an isolated child makes in ``child_runner``:

- the RUNNING transition (``horsies_start_attempt``), and its miss path
  (the expiry before start, the stop of a workflow task, a lost claim),
  through the awaitable methods of the core handle of the child
  (``CoreChildHandle``), with the bound of one start attempt in the core;
- the finalizing marker after the task body, through the awaitable
  ``CoreChildHandle.mark_finalizing_async``.

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
from typing import Final, assert_never

import horsies._core as core
from horsies._core import CoreChildHandle, CoreError, CoreFailure
from horsies.core.lifecycle.core_records import task_row_end_of
from horsies.core.logging import get_logger
from horsies.core.worker.child_pool import _get_child_core_handle
from horsies.core.worker.child_runner import (
    FINALIZING_MARKER_TIMEOUT_S as FINALIZING_MARKER_TIMEOUT_S,
    _PRE_START_CONFIRM_ATTEMPTS,
    StartMissReason,
    _claim_lost,
    _start_unconfirmed,
    _WorkflowResumedBeforeStart,
    attempt_elapsed_ms,
    expired_before_start_of,
    log_finalizing_record,
    marker_refused,
    start_ended_by,
    stopped_before_start_of,
)
from horsies.core.worker.run_return import (
    CoreStoppedBeforeStart,
    MarkerDone,
    MarkerEnd,
    StartedRunning,
    StoppedBeforeRunning,
)

logger = get_logger('worker')

CHILD_WRITE_CHECKOUT_WAIT_S: Final = 30.0
"""The longest wait of one write of the core handle for a pooled connection
(the core's ``CHILD_WRITE_CHECKOUT_WAIT``)."""

RUNNING_STATEMENTS_TIMEOUT_S: Final = 10.0
"""The longest run of one statement of an attempt of the RUNNING transition
before the core sends its cancel request: the start and, when it missed,
the statement of its miss path (the core's ``CHILD_STATEMENT_BOUND``)."""

RUNNING_ATTEMPT_TIMEOUT_S: Final = (
    CHILD_WRITE_CHECKOUT_WAIT_S + RUNNING_STATEMENTS_TIMEOUT_S
)
"""The bound of one attempt of the RUNNING transition: the wait for a pooled
connection and the statements (the core's ``START_ATTEMPT_BOUND``). The
core ends each write of the attempt by it, and a statement past it gets a
cancel request, so the start does not commit after the bound. A seat also
ends its wait for the attempt here; past it the attempt ends with
``OWNERSHIP_UNCONFIRMED``."""


async def _after_missed_start(
    handle: CoreChildHandle,
    task_id: str,
    worker_id: str,
    reason: StartMissReason,
    claimed_at: datetime,
    *,
    orphan_self_heal: bool,
    attempt_began: float,
) -> StoppedBeforeRunning | _WorkflowResumedBeforeStart:
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
                ),
            )
        case 'NOT_HELD' | 'LEASE_EXPIRED':
            return _claim_lost(task_id)
        case _ as unreachable:
            assert_never(unreachable)


async def _confirm_ownership_and_set_running(
    task_id: str,
    worker_id: str,
    claimed_at: datetime,
    *,
    orphan_self_heal: bool,
) -> StartedRunning | StoppedBeforeRunning | _WorkflowResumedBeforeStart | CoreStoppedBeforeStart:
    """Set the row RUNNING. ``StartedRunning``: started, at the time that
    the start write returned.

    Same contract as the sync function of this name in ``child_runner``. A
    failure record of the core and a ``CoreError`` of the core boundary
    become ``OWNERSHIP_UNCONFIRMED``, except ``CoreStopped`` and
    ``ForkedAfterCoreStart`` (``start_ended_by``). The core bounds each write of the
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
        return StoppedBeforeRunning('OWNERSHIP_UNCONFIRMED')
    except (CoreError, CoreFailure) as exc:
        return start_ended_by(task_id, exc)


async def _set_running(
    handle: CoreChildHandle,
    task_id: str,
    worker_id: str,
    claimed_at: datetime,
    *,
    orphan_self_heal: bool,
) -> StartedRunning | StoppedBeforeRunning | _WorkflowResumedBeforeStart:
    """One attempt of ``_confirm_ownership_and_set_running``: one
    ``horsies_start_attempt`` statement through ``handle``, then the miss
    path, each within the bound of the attempt in the core. The function
    reads whether the task is a workflow task from the locked row. A shared
    async seat starts the deadline of its task after the start, so the start
    has no deadline.

    Raises:
        CoreError: the core of this process cannot run the call.
        CoreFailure: the expiry of the miss path failed (the core's class of
            the cause).
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
        case core.AttemptStarted(started_at=started_at):
            return StartedRunning(started_at)
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
    claimed_at: datetime,
    *,
    orphan_self_heal: bool,
) -> StartedRunning | StoppedBeforeRunning | CoreStoppedBeforeStart:
    """Run the RUNNING transition, again when a resume came before the stop.

    ``StartedRunning``: the task starts. ``StoppedBeforeRunning`` or
    ``CoreStoppedBeforeStart``: the task stops before user code.
    Same contract as ``_confirm_until_not_resumed`` in ``child_runner``.
    """
    for _ in range(_PRE_START_CONFIRM_ATTEMPTS):
        result = await _confirm_ownership_and_set_running(
            task_id,
            worker_id,
            claimed_at,
            orphan_self_heal=orphan_self_heal,
        )
        match result:
            case _WorkflowResumedBeforeStart():
                continue
            case StartedRunning() | StoppedBeforeRunning() | CoreStoppedBeforeStart():
                return result
            case _ as unreachable:
                assert_never(unreachable)
    logger.warning(
        f'Task {task_id} workflow was resumed on each of '
        f'{_PRE_START_CONFIRM_ATTEMPTS} start attempts; the worker releases '
        f'the claim'
    )
    return StoppedBeforeRunning('OWNERSHIP_UNCONFIRMED')


async def mark_task_finalizing(
    task_id: uuid.UUID,
    worker_id: str,
    claimed_at: datetime,
) -> MarkerEnd:
    """Mark the row of a task whose body ended as waiting for the parent.

    Same contract as ``mark_run_finalizing`` in ``child_runner``, through
    ``CoreChildHandle.mark_finalizing_async``. The core bounds the write by
    ``FINALIZING_MARKER_TIMEOUT_S``; the wait of the seat ends at that bound
    too, also when the core call does not answer. A failed or late write is
    logged only (``MarkerDone``). ``CoreStopped`` returns
    ``MarkerMetStoppedCore``, and ``ForkedAfterCoreStart`` ends the child at
    once (``marker_refused``). A cancel of the calling task propagates.
    """
    try:
        async with asyncio.timeout(FINALIZING_MARKER_TIMEOUT_S):
            record = await _get_child_core_handle().mark_finalizing_async(
                task_id,
                worker_id=worker_id,
                claimed_at=claimed_at,
            )
    except TimeoutError:
        logger.warning(
            'Failed to mark task %s finalizing: the marker did not finish '
            'within %ss',
            task_id,
            FINALIZING_MARKER_TIMEOUT_S,
        )
        return MarkerDone()
    except CoreError as exc:
        return marker_refused(task_id, exc)
    log_finalizing_record(record)
    return MarkerDone()
