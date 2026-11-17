# pyright: reportPrivateUsage=false
"""The database ping caller, before it called the core.

``PostgresBroker.ping_database_async`` (``python/horsies/core/brokers/postgres.py``)
sent ``SELECT 1`` through a session of the running event loop's SQLAlchemy
resource set and measured the round trip. The function below is that
method's body. It takes the broker as its first parameter, so the dual-run
rebinding can put it in the place of the method.
"""

from __future__ import annotations

import asyncio

from sqlalchemy import text

from horsies.core.brokers.postgres import (
    PostgresBroker,
    _loop_limit_err,
    _operation_failed,
)
from horsies.core.brokers.result_types import BrokerErrorCode, BrokerResult
from horsies.core.models.health import DatabasePing
from horsies.core.types.result import Err, Ok


async def ping_database_async(self: PostgresBroker) -> BrokerResult[DatabasePing]:
    """``SELECT 1`` through the loop's pool; the round-trip latency.

    A failure reports ``DB_PING_FAILED``; nothing changed.
    """
    match await self.select_resources():
        case Err(loop_limit):
            return _loop_limit_err('ping_database', loop_limit)
        case Ok(resources):
            pass
    loop = asyncio.get_running_loop()
    try:
        start = loop.time()
        async with resources.session_factory() as session:
            await session.execute(text('SELECT 1'))
        latency_ms = (loop.time() - start) * 1000.0
        return Ok(DatabasePing(latency_ms=latency_ms))
    except Exception as exc:
        return _operation_failed(
            BrokerErrorCode.DB_PING_FAILED,
            f'ping_database failed: {exc}',
            exc,
        )
