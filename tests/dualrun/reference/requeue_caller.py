# pyright: reportPrivateUsage=false
"""The stale CLAIMED requeue caller, before it called the core.

``PostgresBroker.requeue_stale_claimed`` (``python/horsies/core/brokers/postgres.py``)
sent ``REQUEUE_STALE_CLAIMS_SQL`` (``horsies_requeue_stale_claims``) on one
autocommit connection of its SQLAlchemy engine, in batches of
``REQUEUE_STALE_CLAIMS_BATCH_SIZE``, until a batch was short, at most
``REQUEUE_STALE_CLAIMS_MAX_BATCHES_PER_PASS`` batches. The function below is
that method's body. It takes the broker as its first parameter, so the
dual-run rebinding can put it in the place of the method.
"""

from __future__ import annotations

from horsies.core.brokers.postgres import (
    REQUEUE_STALE_CLAIMS_BATCH_SIZE,
    REQUEUE_STALE_CLAIMS_MAX_BATCHES_PER_PASS,
    REQUEUE_STALE_CLAIMS_SQL,
    PostgresBroker,
    _loop_limit_err,
    _operation_failed,
)
from horsies.core.brokers.result_types import BrokerErrorCode, BrokerResult
from horsies.core.types.result import Err, Ok
from horsies.core.utils.db import autocommit_connection


async def requeue_stale_claimed(
    self: PostgresBroker,
    stale_threshold_ms: int = 120_000,
) -> BrokerResult[int]:
    """One pass of bounded batches; the number of rows requeued.

    A failure reports ``CLEANUP_FAILED``; the batches before it committed.
    """
    match await self.select_resources():
        case Err(loop_limit):
            return _loop_limit_err('requeue_stale_claimed', loop_limit)
        case Ok(resources):
            pass
    total_requeued = 0
    try:
        async with resources.session_factory() as session:
            connection = await autocommit_connection(session)
            for _ in range(REQUEUE_STALE_CLAIMS_MAX_BATCHES_PER_PASS):
                requeued = (
                    await connection.execute(
                        REQUEUE_STALE_CLAIMS_SQL,
                        {
                            'stale_after_ms': stale_threshold_ms,
                            'batch_size': REQUEUE_STALE_CLAIMS_BATCH_SIZE,
                        },
                    )
                ).scalar_one()
                total_requeued += requeued
                if requeued < REQUEUE_STALE_CLAIMS_BATCH_SIZE:
                    return Ok(total_requeued)
    except Exception as exc:
        return _operation_failed(
            BrokerErrorCode.CLEANUP_FAILED,
            f'requeue_stale_claimed failed after requeueing '
            f'{total_requeued} task(s): {exc}',
            exc,
        )
    self.logger.warning(
        'requeue_stale_claimed reached the per-pass batch cap after '
        'requeueing %s task(s); the remainder is requeued next pass',
        total_requeued,
    )
    return Ok(total_requeued)
