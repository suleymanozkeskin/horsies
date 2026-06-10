"""Retry policy evaluation and scheduling for failed tasks.

RetryMixin decides whether a failed task retries (auto_retry_for,
max_retries, good_until) and persists the retry schedule plus the
delayed wake-up notification. Worker-internal mixin: the
``TYPE_CHECKING`` block declares the slice of ``Worker`` it relies on.
"""

# pyright: reportPrivateUsage=false

from __future__ import annotations

import asyncio
from datetime import timedelta, timezone
from enum import Enum
from typing import TYPE_CHECKING, Any, Literal

from horsies.core.codec.json_io import loads_json
from horsies.core.logging import get_logger
from horsies.core.types.result import is_err
from horsies.core.worker.sql import (
    GET_TASK_RETRY_CONFIG_SQL,
    GET_TASK_RETRY_INFO_SQL,
    GET_TASK_RETRY_POSTCHECK_SQL,
    NOTIFY_DELAYED_SQL,
    SCHEDULE_TASK_RETRY_SQL,
)

if TYPE_CHECKING:
    from collections.abc import Coroutine

    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

    from horsies.core.models.tasks import TaskError

logger = get_logger('worker')


class RetryMixin:
    """Retry decision, schedule persistence, delayed notification."""

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

    async def _should_retry_task(
        self,
        task_id: str,
        error: TaskError,
        session: AsyncSession,
    ) -> bool:
        """Check if a task should be retried based on its configuration and current retry count."""
        result = await session.execute(
            GET_TASK_RETRY_INFO_SQL,
            {'id': task_id},
        )
        row = result.fetchone()

        if not row:
            return False

        retry_count = row.retry_count or 0
        max_retries = row.max_retries or 0

        if retry_count >= max_retries or max_retries == 0:
            return False

        # Parse task options to check auto_retry_for (nested in retry_policy)
        try:
            if not row.task_options:
                return False
            opts_r = loads_json(row.task_options)
            if is_err(opts_r):
                return False
            task_options_data = opts_r.ok_value
            if not isinstance(task_options_data, dict):
                return False
            retry_policy_raw = task_options_data.get('retry_policy', {})
            if not isinstance(retry_policy_raw, dict):
                return False
            auto_retry_for = retry_policy_raw.get('auto_retry_for')
        except Exception as exc:
            # Corrupt task_options must not silently disable retries.
            logger.error(
                'Task %s retry decision failed parsing task_options; '
                'treating as non-retryable: %s',
                task_id,
                exc,
            )
            return False

        if not isinstance(auto_retry_for, list) or not auto_retry_for:
            return False

        # Match error code against auto_retry_for (enum value or string)
        code = (
            error.error_code.value
            if isinstance(error.error_code, Enum)
            else error.error_code
        )
        if code and code in auto_retry_for:
            good_until = row.good_until
            if good_until is not None:
                db_now = row.db_now
                if good_until.tzinfo is None:
                    good_until = good_until.replace(tzinfo=timezone.utc)
                if db_now.tzinfo is None:
                    db_now = db_now.replace(tzinfo=timezone.utc)
                if good_until <= db_now:
                    logger.info(
                        'Task %s retry skipped: good_until (%s) already passed',
                        task_id,
                        good_until,
                    )
                    return False
            return True

        return False

    async def _schedule_retry(
        self,
        task_id: str,
        session: AsyncSession,
        queue_name: str,
    ) -> Literal['scheduled', 'reaper_reclaimed', 'expired']:
        """Schedule a task for retry by updating its status and next retry time.

        ``queue_name`` comes from the caller's already-locked task row; the
        delayed wake-up notification targets that queue. Fetching it here
        would require a second pooled session while the caller holds one plus
        a row lock — a pool-starvation deadlock under mass failure.

        Returns:
        - 'scheduled' when retry was persisted
        - 'reaper_reclaimed' when task is no longer RUNNING
        - 'expired' when retry would land at/after good_until
        """
        # Get current retry configuration
        result = await session.execute(
            GET_TASK_RETRY_CONFIG_SQL,
            {'id': task_id},
        )
        row = result.fetchone()

        if not row:
            return 'reaper_reclaimed'

        retry_count = (row.retry_count or 0) + 1

        # Parse retry policy from task options
        try:
            if not row.task_options:
                retry_policy_data = {}
            else:
                opts_r = loads_json(row.task_options)
                task_options_data = opts_r.ok_value if not is_err(opts_r) else {}
                if not isinstance(task_options_data, dict):
                    retry_policy_data = {}
                else:
                    retry_policy_data = task_options_data.get('retry_policy', {})
                    if not isinstance(retry_policy_data, dict):
                        retry_policy_data = {}
        except Exception as exc:
            logger.warning(
                'Task %s retry policy unparseable; falling back to default '
                'intervals: %s',
                task_id,
                exc,
            )
            retry_policy_data = {}

        # Calculate retry delay
        delay_seconds = self._calculate_retry_delay(retry_count, retry_policy_data)
        db_now = row.db_now
        if db_now.tzinfo is None:
            db_now = db_now.replace(tzinfo=timezone.utc)
        next_retry_at = db_now + timedelta(seconds=delay_seconds)

        # Do not schedule retries that will be unclaimable due to expiry.
        good_until = row.good_until
        if good_until is not None:
            if good_until.tzinfo is None:
                good_until = good_until.replace(tzinfo=timezone.utc)
            if next_retry_at >= good_until:
                logger.info(
                    'Task %s retry expired: next_retry_at (%s) would exceed good_until (%s)',
                    task_id,
                    next_retry_at,
                    good_until,
                )
                return 'expired'

        # Update task for retry — guarded by status = 'RUNNING' + ownership
        res = await session.execute(
            SCHEDULE_TASK_RETRY_SQL,
            {
                'id': task_id,
                'wid': self.worker_instance_id,
                'retry_count': retry_count,
                'next_retry_at': next_retry_at,
            },
        )
        if res.fetchone() is None:
            # Distinguish expiry guard rejections from true reaper reclaim races.
            postcheck = await session.execute(
                GET_TASK_RETRY_POSTCHECK_SQL,
                {'id': task_id},
            )
            postcheck_row = postcheck.fetchone()
            if postcheck_row is not None:
                status_value = postcheck_row.status
                is_running = (
                    status_value == 'RUNNING'
                    or getattr(status_value, 'value', None) == 'RUNNING'
                )
                if is_running:
                    post_good_until = postcheck_row.good_until
                    if post_good_until is not None:
                        if post_good_until.tzinfo is None:
                            post_good_until = post_good_until.replace(
                                tzinfo=timezone.utc
                            )
                        if next_retry_at >= post_good_until:
                            logger.info(
                                'Task %s retry expired at SQL guard: next_retry_at (%s) '
                                'would exceed good_until (%s)',
                                task_id,
                                next_retry_at,
                                post_good_until,
                            )
                            return 'expired'
            logger.warning(
                f'Task {task_id} retry aborted: status/ownership changed '
                f'(reaper reclaim or re-claim by another worker). '
                f'Skipping to prevent double-execution.'
            )
            return 'reaper_reclaimed'

        # Schedule a delayed notification using asyncio for the task's actual queue
        self._spawn_background(
            self._schedule_delayed_notification(
                delay_seconds,
                f'task_queue_{queue_name}',
                f'retry:{task_id}',
            ),
            name=f'delayed-notify-{task_id}',
        )

        logger.info(
            f'Scheduled task {task_id} for retry #{retry_count} at {next_retry_at}'
        )
        return 'scheduled'

    def _calculate_retry_delay(
        self,
        retry_attempt: int,
        retry_policy_data: dict[str, Any],
    ) -> float:
        """Calculate the delay in seconds for a retry attempt."""
        from horsies.core.utils.retry import calculate_retry_delay

        return calculate_retry_delay(retry_attempt, retry_policy_data)

    async def _schedule_delayed_notification(
        self, delay_seconds: float, channel: str, payload: str
    ) -> None:
        """Schedule a delayed notification to wake up the worker for retry."""
        try:
            await asyncio.sleep(delay_seconds)

            # Send notification to trigger retry processing
            async with self.sf() as session:
                await session.execute(
                    NOTIFY_DELAYED_SQL,
                    {'channel': channel, 'payload': payload},
                )
                await session.commit()

            logger.debug(f'Sent delayed notification for retry: {payload}')
        except asyncio.CancelledError:
            logger.debug(f'Delayed notification cancelled for: {payload}')
        except Exception as e:
            logger.error(f'Error sending delayed notification for {payload}: {e}')
