"""Resolve the Pydantic broker config into the core's producer manifest.

`to_producer_manifest` is the one place where the user config (Pydantic)
meets the core's input (the manifest generated from horsies-core). It maps
`PostgresConfig` field by field. The typed values of the database section
(the session database, `pool_recycle`, the TCP keepalive and user timeout
settings) follow the rules of `clients/python/horsies/core/manifest_database.py`, which the
worker manifest builder shares. The two inputs that `PostgresConfig` does
not hold, the schema mode and the idempotency reservation window, are
parameters: the caller decides them. `schema_mode_of` gives the schema mode
of the broker flags `run_schema_migrations` and `assume_initialized`.
`install_coverage_of` gives the core the coverage request of the handle's
schema install from the app's `RetentionConfig`.

Pydantic refuses every user error first, so a manifest built here is one
that the core accepts. A refusal by the core is a defect of this function.
"""

from __future__ import annotations

from datetime import timedelta
from typing import Final

from horsies._catalog.manifest import (
    MANIFEST_VERSION,
    RESERVATION_WINDOW_MICROS_MAX,
    RESERVATION_WINDOW_MICROS_MIN,
    DatabaseManifest,
    PoolRecycle,
    PoolRecycleAfterSeconds,
    PoolRecycleNever,
    ProducerManifest,
    SchemaMode,
    SendManifest,
    SessionDatabase,
    SessionDatabaseSameAsDatabaseUrl,
    SessionDatabaseSeparate,
    TcpKeepalivesConfig,
    TcpKeepalivesConfigOff,
    TcpKeepalivesConfigOn,
    TcpUserTimeoutConfig,
    TcpUserTimeoutConfigAfter,
    TcpUserTimeoutConfigSystemDefault,
)
from horsies.core.manifest_database import (
    core_database_url,
    pool_recycle_with,
    session_database_with,
    tcp_keepalives_with,
    tcp_user_timeout_with,
)
from horsies._core import InstallCoverage
from horsies.core.models.broker import PostgresConfig
from horsies.core.models.retention import RetentionConfig

_ONE_MICROSECOND: Final = timedelta(microseconds=1)


def to_producer_manifest(
    config: PostgresConfig,
    *,
    schema: SchemaMode,
    idempotency_reservation_window: timedelta,
) -> ProducerManifest:
    """The producer manifest of `config`.

    Pure: no I/O. `schema` and `idempotency_reservation_window` come from the
    caller; the window is one that `validate_reservation_window` accepted
    (`clients/python/horsies/core/history/identity/keys.py`).
    """
    return ProducerManifest(
        manifest_version=MANIFEST_VERSION,
        database=_database_manifest(config),
        schema=schema,
        send=SendManifest(
            retain_rerun_input_default=config.retain_rerun_input_default,
            idempotency_reservation_window=reservation_window_micros(
                idempotency_reservation_window,
            ),
        ),
    )


def schema_mode_of(
    *,
    run_schema_migrations: bool,
    assume_initialized: bool,
) -> SchemaMode:
    """The schema mode of the `PostgresBroker` flags of the same names.

    A broker built with `assume_initialized=True` never runs its schema step
    and has no listener, whatever `run_schema_migrations` says
    (`PostgresBroker.__init__`, `_build_resources`). Pure.
    """
    match (assume_initialized, run_schema_migrations):
        case (True, _):
            return 'ASSUME_INITIALIZED'
        case (False, True):
            return 'MIGRATE_AND_VALIDATE'
        case (False, False):
            return 'OBSERVE_ONLY'


def install_coverage_of(retention: RetentionConfig) -> InstallCoverage:
    """The coverage request of the schema install of a broker whose app has
    `retention`: its two horizons and every period that a send can stamp,
    the request of the worker start's pass. The core install runs it gated,
    after the migrations, so the history leaves of the current UTC day and of
    the lookahead exist for each class with or without a worker.

    Pure: no I/O. `retention` passed its Pydantic validation, so the core
    accepts every value; a refusal (`ValueError`) is a defect of this
    function.
    """
    return InstallCoverage(
        history_horizon_days=retention.history_leaf_horizon_days,
        heartbeat_horizon_hours=retention.heartbeat_leaf_horizon_hours,
        declared_period_days=[int(days) for days in sorted(retention.sendable_periods())],
    )


def reservation_window_micros(window: timedelta) -> int:
    """`window` in whole microseconds, the resolution of `timedelta`, so the
    count is exact.

    `validate_reservation_window` admits only a window above zero and at most
    30 days, so a count outside `RESERVATION_WINDOW_MICROS_MIN` to
    `RESERVATION_WINDOW_MICROS_MAX` is a defect and raises `AssertionError`.
    """
    micros = window // _ONE_MICROSECOND
    assert RESERVATION_WINDOW_MICROS_MIN <= micros <= RESERVATION_WINDOW_MICROS_MAX, (
        f'a validated reservation window is from {RESERVATION_WINDOW_MICROS_MIN} to '
        f'{RESERVATION_WINDOW_MICROS_MAX} microseconds; got {micros}'
    )
    return micros


def _database_manifest(config: PostgresConfig) -> DatabaseManifest:
    return DatabaseManifest(
        database_url=core_database_url(config.database_url.get_secret_value()),
        session_database=_session_database(config),
        pgbouncer_transaction_mode=config.pgbouncer_transaction_mode,
        pool_pre_ping=config.pool_pre_ping,
        pool_size=config.pool_size,
        max_overflow=config.max_overflow,
        pool_timeout=config.pool_timeout,
        pool_recycle=pool_recycle_of(config.pool_recycle),
        echo=config.echo,
        tcp_keepalives=tcp_keepalives_of(config),
        tcp_user_timeout=tcp_user_timeout_of(config.tcp_user_timeout_ms),
    )


def pool_recycle_of(value: int) -> PoolRecycle:
    """`pool_recycle_with` with the classes of the producer manifest."""
    return pool_recycle_with(
        value,
        never=PoolRecycleNever,
        after_seconds=PoolRecycleAfterSeconds,
    )


def tcp_keepalives_of(config: PostgresConfig) -> TcpKeepalivesConfig:
    """`tcp_keepalives_with` with the classes of the producer manifest."""
    return tcp_keepalives_with(
        config,
        off=TcpKeepalivesConfigOff,
        on=TcpKeepalivesConfigOn,
    )


def tcp_user_timeout_of(milliseconds: int) -> TcpUserTimeoutConfig:
    """`tcp_user_timeout_with` with the classes of the producer manifest."""
    return tcp_user_timeout_with(
        milliseconds,
        system_default=TcpUserTimeoutConfigSystemDefault,
        after=TcpUserTimeoutConfigAfter,
    )


def _session_database(config: PostgresConfig) -> SessionDatabase:
    return session_database_with(
        config,
        same_as_database_url=SessionDatabaseSameAsDatabaseUrl,
        separate=SessionDatabaseSeparate,
    )
