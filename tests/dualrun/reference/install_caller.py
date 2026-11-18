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

The install now ends with the startup coverage pass that the worker start
runs, so a producer leaves the leaves of the current UTC day and of the
lookahead with or without a worker. The reference runs the Python pass of
the worker start before the switch (``ensure_startup_coverage_in_database``
on the resource set's partition maintenance database) after the Python
install, with the retention config of the broker: its app's, or the default
retention config for a broker that no app configures.

One install per broker: after the Python install, the broker gets a core
handle that runs no schema step (``OBSERVE_ONLY``). The core operations of
the broker (the workflow engine, the monitoring reads) would otherwise
install again through their handle at their first call, with the core's
coverage pass, after whatever the test wrote between the two calls. The
core run installs once, in ``ensure_schema_initialized``.
"""

from __future__ import annotations

from horsies._catalog.manifest import dump_producer_manifest
from horsies.core.brokers.postgres import (
    PostgresBroker,
    _loop_limit_err,
    _operation_failed,
)
from horsies.core.brokers.resources import BrokerResources
from horsies.core.brokers.result_types import (
    BrokerErrorCode,
    BrokerEventLoopLimit,
    BrokerOperationFailed,
)
from horsies.core.history.maintenance.coverage import (
    ensure_startup_coverage_in_database,
)
from horsies.core.manifest import to_producer_manifest
from horsies.core.models.retention import RetentionConfig
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
    match self.run_schema_migrations:
        case True:
            await cover_after_install(resources, self._install_retention)
            await observe_only_core_handle(self)
        case False:
            pass
    self._initialized = True


async def observe_only_core_handle(self: PostgresBroker) -> None:
    """Put a core handle that runs no schema step in the place of the
    broker's handle, and close the old one.

    The new handle has the manifest of the old one with the schema mode
    ``OBSERVE_ONLY`` (a listener, no install) and the broker's child spec
    source. Raises what ``close_async`` of the old handle raises.
    """
    from horsies.core.workflows.child_spec_source import child_spec_source_of

    manifest = dump_producer_manifest(
        to_producer_manifest(
            self.config,
            schema='OBSERVE_ONLY',
            idempotency_reservation_window=self.idempotency_reservation_window,
        ),
    )
    replaced = self._core_handle
    observing = type(replaced)(manifest, pool_profile=self._CORE_POOL_PROFILE)
    observing.register_child_spec_source(child_spec_source_of(self))
    self._core_handle = observing
    _ = await replaced.close_async()


async def cover_after_install(
    resources: BrokerResources, retention: RetentionConfig
) -> None:
    """The startup coverage pass at the end of the install: the worker
    start's pass with the broker's retention config.

    The outcome is discarded, as the core install logs a contained refusal
    and ends. Raises what the pass raises for a pass with no outcome.
    """
    _ = await ensure_startup_coverage_in_database(
        resources.partition_maintenance,
        history_horizon_days=retention.history_leaf_horizon_days,
        heartbeat_horizon_hours=retention.heartbeat_leaf_horizon_hours,
        declared_classes=retention.registrable_classes(),
    )


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
