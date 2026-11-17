# pyright: reportPrivateUsage=false
"""The callers of the pending expiry and the orphan audit, before they
called the core.

- ``PostgresBroker.expire_pending_tasks`` and
  ``PostgresBroker.audit_orphaned_workflow_tasks``
  (``python/horsies/core/brokers/postgres.py``) sent
  ``horsies_expire_pending_tasks`` and ``horsies_cancel_orphaned_tasks``
  through ``apply_batch_async`` on a session of the broker's SQLAlchemy
  engine: the expiry one transaction per batch, until a batch was short, at
  most ``_EXPIRE_MAX_BATCHES_PER_PASS`` batches; the audit one page and its
  cursor counts in one transaction.

The orphan self-heal of the worker's finalize is not a caller any more: the
child's stop before start cancels the orphan
(``tests/dualrun/reference/stop_before_start_caller.py`` keeps the parent's
cancel of before the switch).

The functions below are those bodies. Each takes the broker as its first
parameter, so the dual-run rebinding can put it in the place of the method.
"""

from __future__ import annotations

import time
from typing import assert_never

from horsies.core.brokers.postgres import (
    _EXPIRE_BATCH_SIZE,
    _EXPIRE_MAX_BATCHES_PER_PASS,
    _ORPHAN_BATCH_SIZE,
    READ_ORPHAN_TASK_SCAN_STATS_SQL,
    PostgresBroker,
    _loop_limit_err,
    _operation_failed,
)
from horsies.core.brokers.result_types import BrokerErrorCode, BrokerResult
from horsies.core.lifecycle.commands import (
    CancelOrphanedTasks,
    ExpirePendingTasks,
)
from horsies.core.lifecycle.outcomes import (
    AlreadyApplied,
    Applied,
    LostClaim,
    SourceStateConflict,
    TaskAbsent,
)
from horsies.core.lifecycle.persistence import apply_batch_async
from horsies.core.models.recovery import OrphanTaskAuditReport
from horsies.core.types.result import Err, Ok, is_err


async def expire_pending_tasks(self: PostgresBroker) -> BrokerResult[int]:
    """Transition PENDING tasks whose good_until deadline has passed to EXPIRED.

    Only affects tasks that were never claimed. Writes a TaskResult with
    TASK_EXPIRED outcome code so callers have a meaningful result payload.
    No attempt row is written (the task never started).
    """
    match await self.select_resources():
        case Err(loop_limit):
            return _loop_limit_err('expire_pending_tasks', loop_limit)
        case Ok(resources):
            pass
    try:
        from horsies.core.models.tasks import TaskResult, TaskError, OutcomeCode
        from horsies.core.codec.json_io import dumps_json
        from horsies.core.codec.typed import encode_task_result

        task_error = TaskError(
            error_code=OutcomeCode.TASK_EXPIRED,
            message='Task expired: good_until deadline passed before execution started',
        )
        task_result: TaskResult[None, TaskError] = TaskResult(err=task_error)
        # Strict-serde phase 7: emit the wire envelope so downstream
        # readers see ``__h_task_result__`` and the err slot decodes
        # cleanly. Expired tasks carry no ok payload.
        ser_r = dumps_json(encode_task_result(task_result, type(None)))
        if is_err(ser_r):
            return _operation_failed(
                BrokerErrorCode.CLEANUP_FAILED,
                f'expire_pending_tasks: failed to serialize TASK_EXPIRED result: {ser_r.err_value}',
                ser_r.err_value,
            )
        result_json = ser_r.ok_value

        total_expired = 0
        for _ in range(_EXPIRE_MAX_BATCHES_PER_PASS):
            async with resources.session_factory() as session:
                outcomes = await apply_batch_async(
                    await session.connection(),
                    ExpirePendingTasks(
                        batch_size=_EXPIRE_BATCH_SIZE,
                        result_json=result_json,
                        error_code=OutcomeCode.TASK_EXPIRED.value,
                    ),
                )
                for outcome in outcomes:
                    match outcome:
                        case Applied():
                            continue
                        case (
                            AlreadyApplied()
                            | LostClaim()
                            | SourceStateConflict()
                            | TaskAbsent()
                        ):
                            raise RuntimeError(
                                'pending expiry operation returned '
                                f'{type(outcome).__name__}; discovery batches '
                                'report transitioned rows only'
                            )
                        case _ as unreachable:
                            assert_never(unreachable)
                await session.commit()
            batch_expired = len(outcomes)
            total_expired += batch_expired
            if batch_expired < _EXPIRE_BATCH_SIZE:
                return Ok(total_expired)
        self.logger.warning(
            'expire_pending_tasks reached the per-pass batch cap after '
            'expiring %s task(s); the remainder expires next interval',
            total_expired,
        )
        return Ok(total_expired)
    except Exception as exc:
        return _operation_failed(
            BrokerErrorCode.CLEANUP_FAILED,
            f'expire_pending_tasks failed: {exc}',
            exc,
        )


async def audit_orphaned_workflow_tasks(
    self: PostgresBroker,
) -> BrokerResult[OrphanTaskAuditReport]:
    """Scan and cancel one bounded page of orphan workflow tasks."""
    match await self.select_resources():
        case Err(loop_limit):
            return _loop_limit_err('audit_orphaned_workflow_tasks', loop_limit)
        case Ok(resources):
            pass
    started = time.monotonic()
    try:
        async with resources.session_factory() as session:
            outcomes = await apply_batch_async(
                await session.connection(),
                CancelOrphanedTasks(batch_size=_ORPHAN_BATCH_SIZE),
            )
            for outcome in outcomes:
                match outcome:
                    case Applied():
                        continue
                    case (
                        AlreadyApplied()
                        | LostClaim()
                        | SourceStateConflict()
                        | TaskAbsent()
                    ):
                        raise RuntimeError(
                            'orphan sweep returned '
                            f'{type(outcome).__name__}; discovery '
                            'batches report transitioned rows only'
                        )
                    case _ as unreachable:
                        assert_never(unreachable)
            stats = (
                await session.execute(READ_ORPHAN_TASK_SCAN_STATS_SQL)
            ).one()
            rows_selected = int(stats.last_scan_rows)
            candidates_returned = int(stats.last_candidate_rows)
            if rows_selected < 0 or candidates_returned < 0:
                raise RuntimeError(
                    'orphan sweep returned negative cursor metrics'
                )
            if candidates_returned != len(outcomes):
                raise RuntimeError(
                    'orphan sweep cursor candidate count differs from '
                    'the applied outcome count'
                )
            await session.commit()
        return Ok(OrphanTaskAuditReport(
            rows_selected=rows_selected,
            candidates_returned=candidates_returned,
            cancelled=len(outcomes),
            duration_ms=int((time.monotonic() - started) * 1000),
        ))
    except Exception as exc:
        return _operation_failed(
            BrokerErrorCode.CLEANUP_FAILED,
            f'audit_orphaned_workflow_tasks failed: {exc}',
            exc,
        )
