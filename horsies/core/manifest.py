"""Resolve the Pydantic broker config into the core's producer manifest.

`to_producer_manifest` is the one place where the user config (Pydantic)
meets the core's input (the manifest generated from horsies-core). It maps
`PostgresConfig` field by field, and turns SQLAlchemy's `pool_recycle=-1`
into the typed "never" value. The two inputs that `PostgresConfig` does
not hold, the schema mode and the idempotency reservation window, are
parameters: the caller decides them. `schema_mode_of` gives the schema mode
of the broker flags `run_schema_migrations` and `assume_initialized`.

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
)
from horsies.core.models.broker import SQLALCHEMY_NEVER_RECYCLE, PostgresConfig
from horsies.core.types.result import Err, Ok
from horsies.core.utils.url import check_database_url_scheme

CORE_URL_SCHEME: Final = 'postgresql://'
"""The scheme that the core's connection layer reads."""

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
    (`horsies/core/history/identity/keys.py`).
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


def core_database_url(url: str) -> str:
    """`url` with the scheme that the core reads: `postgres://` becomes
    `postgresql://`, and the rest is kept as it is.

    The `PostgresConfig` validators admit only `postgresql://` and
    `postgres://` (`check_database_url_scheme`), so another URL here is a
    defect: it raises `AssertionError`, which names the refusal and never
    the rest of the URL (it can hold a password).
    """
    match check_database_url_scheme(url):
        case Ok():
            _, _, rest = url.partition('://')
            return CORE_URL_SCHEME + rest
        case Err(refusal):
            raise AssertionError(
                f'a validated database URL has the scheme postgresql or '
                f'postgres; got {refusal!r}'
            )


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
    )


def pool_recycle_of(value: int) -> PoolRecycle:
    """The typed form of a validated `pool_recycle`: `-1` is never, any other
    value a number of seconds.

    The `PostgresConfig` validator admits only these, so another value is a
    defect and raises `AssertionError`.
    """
    match value:
        case _ if value == SQLALCHEMY_NEVER_RECYCLE:
            return PoolRecycleNever()
        case _:
            assert value >= 0, (
                f'pool_recycle={value} passed the PostgresConfig validator, which '
                'admits only -1 and a number of seconds'
            )
            return PoolRecycleAfterSeconds(seconds=value)


def _session_database(config: PostgresConfig) -> SessionDatabase:
    match config.session_database_url:
        case None:
            return SessionDatabaseSameAsDatabaseUrl()
        case session_url:
            return SessionDatabaseSeparate(
                url=core_database_url(session_url.get_secret_value()),
            )
