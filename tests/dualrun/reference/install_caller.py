# pyright: reportPrivateUsage=false
"""The schema install callers, before they called the core.

``PostgresBroker.ensure_schema_initialized`` and ``_ensure_initialized``
(``python/horsies/core/brokers/postgres.py``) ran the Python install of the
broker once per broker: ``_maybe_run_schema_init`` on the session URL's
SQLAlchemy engine of the running event loop's resource set. With migrations
on, that is the fleet install: the state read, the schema advisory lock, the
baseline migration with the family row (``install_baseline``), the history
reader publication (``publish_history_readers``) and the family record
(``record_client_family``), with retries on a deadlock. The install methods
stay on ``PostgresBroker``; the functions below are the two callers' bodies.
Each takes the broker as its first parameter, so the dual-run rebinding can
put it in the place of the method.
"""

from __future__ import annotations

from horsies.core.brokers.postgres import (
    PostgresBroker,
    _loop_limit_err,
    _operation_failed,
)
from horsies.core.brokers.result_types import (
    BrokerErrorCode,
    BrokerEventLoopLimit,
    BrokerOperationFailed,
)
from horsies.core.types.result import Err, Ok, Result


async def ensure_initialized(self: PostgresBroker) -> None:
    """Run the Python install once per broker.

    The callers before the switch passed the resource set they had selected
    on this loop; selecting again on the same loop gives the same set.

    Raises what the install raises: ``RuntimeError`` for a database the
    install refuses, ``psycopg.Error`` for a failed statement.
    """
    if self._initialized:
        return
    match await self.select_resources():
        case Err(loop_limit):
            raise RuntimeError(f'schema install: {loop_limit.describe()}')
        case Ok(resources):
            pass
    await self._run_with_schema_engine(resources, self._maybe_run_schema_init)
    self._initialized = True


async def ensure_schema_initialized(
    self: PostgresBroker,
) -> Result[None, BrokerOperationFailed | BrokerEventLoopLimit]:
    """The public install; ``SCHEMA_INIT_FAILED`` with its cause on a failure."""
    match await self.select_resources():
        case Err(loop_limit):
            return _loop_limit_err('ensure_schema_initialized', loop_limit)
        case Ok():
            pass
    try:
        await ensure_initialized(self)
        return Ok(None)
    except Exception as exc:
        return _operation_failed(
            BrokerErrorCode.SCHEMA_INIT_FAILED,
            f'Schema initialization failed: {exc}',
            exc,
        )
