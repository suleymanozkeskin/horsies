"""Finalize the runs of a shared async child that the worker killed.

The slot answers each run of a child that the worker killed, and whose exit
it saw, with a verdict (``shared_stop``). This mixin writes it, after that
exit, so the attempt has ended before its row moves:

- ``TimedOutAtKill``: ``TASK_TIMEOUT`` through ``_record_task_timeout``, the
  writer of every timeout;
- ``BlockedTheLoop``: ``TASK_BLOCKED_LOOP``, with its retry in an isolated
  child;
- ``BesideBlockedLoop``: ``NEIGHBOUR_BLOCKED_LOOP``, with its retry in its
  class.

The last two go through ``_recover_worker_future_failure``, the writer of
crash recovery: a row that is still CLAIMED (no user code ran) returns to
the queue, and a RUNNING row gets its retry when its policy names the code,
else fails with it. Worker-internal mixin: the ``TYPE_CHECKING`` block
declares the slice of ``Worker`` it relies on.
"""

# pyright: reportPrivateUsage=false

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING, Any, Optional, assert_never

from horsies.core.logging import get_logger
from horsies.core.models.tasks import OperationalErrorCode
from horsies.core.types.result import Err, Result
from horsies.core.worker.child_protocol import SeatRefusal, SeatTask, SeatWork
from horsies.core.worker.runtime import (
    _FINALIZE_STAGE_FUTURE,
    _FinalizeError,
    _RequeueOutcome,
)
from horsies.core.worker.shared_stop import (
    BesideBlockedLoop,
    BlockedTheLoop,
    RetryPlacement,
    TimedOutAtKill,
)
from horsies.core.worker.slots import SeatKilled, describe_exit

if TYPE_CHECKING:
    from horsies.core.models.tasks import BuiltInTaskCode

logger = get_logger('worker')


class SharedKillFinalizeMixin:
    """The writes for the runs of a shared async child that the worker killed."""

    if TYPE_CHECKING:
        # Cross-concern methods provided by sibling mixins.
        async def _finalize_shared_timeout(
            self,
            task_id: str,
            timeout_ms: int,
            claimed_at: Optional[datetime],
            retry: RetryPlacement,
        ) -> Result[None, _FinalizeError]: ...
        async def _recover_worker_future_failure(
            self,
            task_id: str,
            reason: str,
            claimed_at: Optional[datetime] = None,
            *,
            failure_code: OperationalErrorCode = OperationalErrorCode.WORKER_CRASHED,
            retry: RetryPlacement = RetryPlacement.SAME_CLASS,
        ) -> _RequeueOutcome: ...
        def _make_finalize_error(
            self,
            *,
            task_id: str,
            stage: str,
            message: str,
            retryable: bool,
            data: dict[str, Any] | None = None,
            error_code: BuiltInTaskCode | str = OperationalErrorCode.BROKER_ERROR,
        ) -> _FinalizeError: ...

    async def _finalize_killed_run(
        self,
        task_id: str,
        killed: SeatKilled,
        work: SeatWork,
        claimed_at: Optional[datetime],
    ) -> Result[None, _FinalizeError]:
        """Write the verdict of a run whose child the worker killed and saw
        exit. A refusal has no deadline, so the slot never gives it
        ``TimedOutAtKill``: ``AssertionError``."""
        killed_by = f'pid={killed.pid}, {describe_exit(killed.child_exit)}'
        match killed.verdict:
            case TimedOutAtKill(retry=retry):
                match work:
                    case SeatTask(timeout_ms=timeout_ms):
                        pass
                    case SeatRefusal():
                        raise AssertionError(
                            f'the refused task {task_id} was killed past a '
                            'deadline it does not have',
                        )
                    case _ as unreachable:
                        assert_never(unreachable)
                logger.warning(
                    'Task %s was past its timeout_ms=%s when the worker killed its '
                    'shared child (%s); it fails with TASK_TIMEOUT, retry %s',
                    task_id,
                    timeout_ms,
                    killed_by,
                    retry.value,
                )
                return await self._finalize_shared_timeout(
                    task_id,
                    timeout_ms,
                    claimed_at,
                    retry,
                )
            case BlockedTheLoop(blocked_ms=blocked_ms, where=where):
                reason = (
                    f'Task held the event loop of its shared async child for '
                    f'{blocked_ms} ms, past the block limit, at {where}; the worker '
                    f'killed the child ({killed_by})'
                )
                return await self._fail_killed_run(
                    task_id,
                    reason,
                    claimed_at,
                    OperationalErrorCode.TASK_BLOCKED_LOOP,
                    RetryPlacement.ISOLATED,
                )
            case BesideBlockedLoop(holder_task_id=holder, blocked_ms=blocked_ms):
                reason = (
                    f'Task {holder} held the event loop of the shared async child '
                    f'of this task for {blocked_ms} ms, past the block limit; the '
                    f'worker killed the child ({killed_by})'
                )
                return await self._fail_killed_run(
                    task_id,
                    reason,
                    claimed_at,
                    OperationalErrorCode.NEIGHBOUR_BLOCKED_LOOP,
                    RetryPlacement.SAME_CLASS,
                )
            case _ as unreachable:
                assert_never(unreachable)

    async def _fail_killed_run(
        self,
        task_id: str,
        reason: str,
        claimed_at: Optional[datetime],
        failure_code: OperationalErrorCode,
        retry: RetryPlacement,
    ) -> Result[None, _FinalizeError]:
        """Fail or retry a killed run with ``failure_code``, as crash recovery
        does. The result is a future-stage error, retryable only when the
        write hit a database error, as for a lost child."""
        logger.warning('Task %s: %s', task_id, reason)
        requeue_outcome = await self._recover_worker_future_failure(
            task_id,
            reason,
            claimed_at=claimed_at,
            failure_code=failure_code,
            retry=retry,
        )
        return Err(
            self._make_finalize_error(
                task_id=task_id,
                stage=_FINALIZE_STAGE_FUTURE,
                message=reason,
                retryable=requeue_outcome is _RequeueOutcome.DB_ERROR,
                data={
                    'failure_code': failure_code.value,
                    'requeue_outcome': requeue_outcome.value,
                    'claimed_at': claimed_at,
                },
            )
        )
