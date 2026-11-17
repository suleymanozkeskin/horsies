# pyright: reportPrivateUsage=false
"""The stale RUNNING recovery caller, before it called the core.

``PostgresBroker.mark_stale_tasks_as_failed`` (``python/horsies/core/brokers/postgres.py``)
read the candidates with ``horsies_stale_running_scan`` and finished each one
with ``horsies_finish_stale_attempt``, each statement on an autocommit
connection of its SQLAlchemy engine, with the crash result that Python built
(``crash_result_json``). ``mark_stale_tasks_as_failed`` below is that
method's body, and the two helpers are its two private methods. It takes the
broker as its first parameter, so the dual-run rebinding can put it in the
place of the method.
"""

from __future__ import annotations

from typing import assert_never

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection

from horsies.core.brokers.postgres import (
    _DRIVER_ERRORS,
    PostgresBroker,
    _loop_limit_err,
    _operation_failed,
)
from horsies.core.brokers.resources import BrokerResources
from horsies.core.brokers.result_types import BrokerErrorCode, BrokerResult
from horsies.core.brokers.stale_running import (
    STALE_RUNNING_SCAN_BATCH_SIZE,
    StaleCandidate,
    StaleThresholds,
    crash_result_json,
    finish_stale_attempt,
    scan_stale_running,
)
from horsies.core.lifecycle.outcomes import (
    AlreadyApplied,
    LostClaim,
    RetryScheduled,
    SourceStateConflict,
    StaleFailureApplied,
    TaskAbsent,
)
from horsies.core.types.result import Err, Ok
from horsies.core.utils.db import autocommit_connection


async def wake_stale_retry(
    self: PostgresBroker,
    connection: AsyncConnection,
    task_id: str,
    queue_name: str,
) -> None:
    """Wake the queue of a scheduled crash retry. Best effort: a failure is
    logged at debug level."""
    try:
        await connection.execute(
            text('SELECT pg_notify(:ch, :p)'),
            {'ch': f'task_queue_{queue_name}', 'p': f'retry:{task_id}'},
        )
    except _DRIVER_ERRORS as exc:
        self.logger.debug('Wake of stale retry %s failed: %s', task_id, exc)


async def finish_stale_candidate(
    self: PostgresBroker,
    resources: BrokerResources,
    candidate: StaleCandidate,
    thresholds: StaleThresholds,
) -> bool:
    """Recover one candidate. True when its retry or its failure committed.

    Raises what the database raises.
    """
    match crash_result_json(candidate, thresholds):
        case Err(serialization_error):
            self.logger.error(
                'Failed to serialize crash result for task %s: %s',
                candidate.task_id,
                serialization_error,
            )
            return False
        case Ok(result_json):
            pass
    async with resources.session_factory() as session:
        connection = await autocommit_connection(session)
        outcome = await finish_stale_attempt(
            connection, candidate, thresholds, result_json,
        )
        match outcome:
            case RetryScheduled(
                retry_count=retry_count,
                next_retry_at=next_retry_at,
                queue_name=queue_name,
            ):
                self.logger.info(
                    'Reaper scheduled retry #%d for stale task %s at %s',
                    retry_count,
                    candidate.task_id,
                    next_retry_at,
                )
                await wake_stale_retry(self, connection, candidate.task_id, queue_name)
                return True
            case StaleFailureApplied():
                return True
            case AlreadyApplied() | LostClaim() | SourceStateConflict() | TaskAbsent():
                self.logger.debug(
                    'Reaper left stale candidate %s: %r', candidate.task_id, outcome,
                )
                return False
            case _ as unreachable:
                assert_never(unreachable)


async def mark_stale_tasks_as_failed(
    self: PostgresBroker,
    stale_threshold_ms: int = 300_000,
    finalizing_stale_threshold_ms: int = 300_000,
) -> BrokerResult[int]:
    """One scan, then one statement per candidate; the number of tasks
    retried or failed. A failed scan reports ``CLEANUP_FAILED``."""
    match await self.select_resources():
        case Err(loop_limit):
            return _loop_limit_err('mark_stale_tasks_as_failed', loop_limit)
        case Ok(resources):
            pass
    thresholds = StaleThresholds(
        stale_after_ms=stale_threshold_ms,
        finalizing_stale_after_ms=finalizing_stale_threshold_ms,
    )
    try:
        async with resources.session_factory() as session:
            candidates = await scan_stale_running(
                await autocommit_connection(session),
                thresholds,
                STALE_RUNNING_SCAN_BATCH_SIZE,
            )
    except Exception as exc:
        return _operation_failed(
            BrokerErrorCode.CLEANUP_FAILED,
            f'mark_stale_tasks_as_failed failed: {exc}',
            exc,
        )
    if len(candidates) == STALE_RUNNING_SCAN_BATCH_SIZE:
        self.logger.warning(
            'mark_stale_tasks_as_failed read a full scan batch of %s '
            'candidates; the rest is recovered next pass',
            STALE_RUNNING_SCAN_BATCH_SIZE,
        )
    processed = 0
    for candidate in candidates:
        try:
            recovered = await finish_stale_candidate(
                self, resources, candidate, thresholds,
            )
        except Exception as task_exc:
            self.logger.error(
                'Reaper failed to process stale task %s: %s',
                candidate.task_id,
                task_exc,
            )
            continue
        if recovered:
            processed += 1
    return Ok(processed)
