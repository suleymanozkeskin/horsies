"""Resolve the Pydantic broker config into the core's producer manifest.

`to_producer_manifest` is the one place where the user config (Pydantic)
meets the core's input (the manifest generated from horsies-core). It maps
`PostgresConfig` field by field, and turns SQLAlchemy's `pool_recycle=-1`
into the typed "never" value. The two inputs that `PostgresConfig` does
not hold, the schema mode and the idempotency reservation window, are
parameters: the caller decides them.

Pydantic refuses every user error first, so a manifest built here is one
that the core accepts. A refusal by the core is a defect of this function.
"""

from __future__ import annotations

from typing import Final

from horsies._catalog.manifest import (
    MANIFEST_VERSION,
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

PSYCOPG_URL_SCHEME: Final = 'postgresql+psycopg://'
"""The scheme that the `PostgresConfig` validators require."""

CORE_URL_SCHEME: Final = 'postgresql://'
"""The scheme that the core's connection layer reads."""


def to_producer_manifest(
    config: PostgresConfig,
    *,
    schema: SchemaMode,
    idempotency_reservation_window_seconds: int,
) -> ProducerManifest:
    """The producer manifest of `config`.

    Pure: no I/O. `schema` and `idempotency_reservation_window_seconds` come
    from the caller; the window must already be inside
    `RESERVATION_WINDOW_SECONDS_MIN` to `RESERVATION_WINDOW_SECONDS_MAX`.
    """
    return ProducerManifest(
        manifest_version=MANIFEST_VERSION,
        database=_database_manifest(config),
        schema=schema,
        send=SendManifest(
            retain_rerun_input_default=config.retain_rerun_input_default,
            idempotency_reservation_window=idempotency_reservation_window_seconds,
        ),
    )


def core_database_url(url: str) -> str:
    """`url` with the scheme that the core reads.

    The `PostgresConfig` validators admit only `postgresql+psycopg://`, so
    another scheme here is a defect: it raises `AssertionError`, which names
    the scheme and never the rest of the URL (it can hold a password).
    """
    scheme_end = url.find('://')
    scheme = url[: scheme_end + 3] if scheme_end >= 0 else '(none)'
    assert url.startswith(PSYCOPG_URL_SCHEME), (
        f'a validated database URL starts with {PSYCOPG_URL_SCHEME!r}, '
        f'not with {scheme!r}'
    )
    return CORE_URL_SCHEME + url.removeprefix(PSYCOPG_URL_SCHEME)


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
