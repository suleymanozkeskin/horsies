"""The wake-up of a scheduled retry.

The retry decision is made from the unlocked read of the retry facts and the
policy of the task (``failed_attempt.retry_request_for``), and the retry is
written by ``horsies_finish_failed_attempt``. RetryMixin wakes the queue of
a scheduled retry when it is due. Worker-internal mixin: the
``TYPE_CHECKING`` block declares the slice of ``Worker`` it relies on.
"""

# pyright: reportPrivateUsage=false

from __future__ import annotations

import asyncio
from typing import TYPE_CHECKING, Any, assert_never

from horsies.core.lifecycle.outcomes import RetryScheduled
from horsies.core.logging import get_logger
from horsies.core.utils.db import SingleStatement, run_in_autocommit
from horsies.core.worker.failed_attempt import NoRetry, RetryAfter, RetryRequest
from horsies.core.worker.sql import NOTIFY_DELAYED_SQL

if TYPE_CHECKING:
    from collections.abc import Coroutine

    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

logger = get_logger('worker')


class RetryMixin:
    """Delayed notification of a scheduled retry."""

    if TYPE_CHECKING:
        # Worker state this mixin reads.
        sf: async_sessionmaker[AsyncSession]
        worker_instance_id: str

        # Cross-concern methods provided by Worker.
        def _spawn_background(
            self,
            coro: Coroutine[Any, Any, Any],
            *,
            name: str,
            finalizer: bool = False,
        ) -> asyncio.Task[Any]: ...

    def _wake_when_retry_due(
        self,
        scheduled: RetryScheduled,
        request: RetryRequest,
    ) -> None:
        """Wake the queue of a scheduled retry after the delay of its request.

        The retry runs at ``scheduled.next_retry_at``, which is the delay
        after the write locked the row. A retry scheduled for a ``NoRetry``
        request contradicts the function: ``AssertionError``.
        """
        match request:
            case RetryAfter(delay_s=delay_s):
                pass
            case NoRetry():
                raise AssertionError(
                    f'task {scheduled.task_id} was scheduled for a retry that '
                    'was not requested',
                )
            case _ as unreachable:
                assert_never(unreachable)
        self._spawn_background(
            self._schedule_delayed_notification(
                delay_s,
                f'task_queue_{scheduled.queue_name}',
                f'retry:{scheduled.task_id}',
            ),
            name=f'delayed-notify-{scheduled.task_id}',
        )
        logger.info(
            'Scheduled task %s for retry #%s at %s',
            scheduled.task_id,
            scheduled.retry_count,
            scheduled.next_retry_at,
        )

    async def _schedule_delayed_notification(
        self, delay_seconds: float, channel: str, payload: str
    ) -> None:
        """Wake the workers of ``channel`` when a retry comes due.

        The NOTIFY is one statement in autocommit (``run_in_autocommit``). A
        failed wake is logged: the retried row is PENDING with its
        ``next_retry_at``, and the claim loop also finds it by polling.
        """
        try:
            await asyncio.sleep(delay_seconds)

            # Send notification to trigger retry processing
            _ = await run_in_autocommit(
                self.sf,
                SingleStatement(
                    statement=NOTIFY_DELAYED_SQL,
                    parameters={'channel': channel, 'payload': payload},
                ),
            )

            logger.debug(f'Sent delayed notification for retry: {payload}')
        except asyncio.CancelledError:
            logger.debug(f'Delayed notification cancelled for: {payload}')
        except Exception as e:
            logger.error(f'Error sending delayed notification for {payload}: {e}')
