# python/horsies/core/utils/db.py
"""Shared database helpers: the classification of transient errors, the
engine hooks of the broker's engines (the text reads of identity columns,
the session settings, the idle ping) and the read of a uuid column value."""

from __future__ import annotations

import time
from enum import Enum
from typing import TYPE_CHECKING, Any, Final, cast
from uuid import UUID

import psycopg
from psycopg import InterfaceError, OperationalError
from psycopg.types.string import TextLoader
from sqlalchemy import event
from sqlalchemy.engine import Dialect, Engine
from sqlalchemy.exc import (
    DBAPIError,
    InvalidatePoolError,
    OperationalError as SAOperationalError,
    TimeoutError as SATimeoutError,
)
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
)
from sqlalchemy.pool import PoolProxiedConnection

from horsies._core import (
    CoreContractViolation,
    CoreDatabaseError,
    CoreError,
    CorePreconditionFailed,
)
from horsies.core.types.core_failure import core_error_retryable

if TYPE_CHECKING:
    from sqlalchemy.engine.interfaces import DBAPIModule


def register_identity_text_reads(engine: AsyncEngine) -> None:
    """Make every connection of ``engine`` read uuid columns as text.

    Identity columns are uuid in the database and strings at the Python
    boundary. The ORM's ``Uuid(as_uuid=False)`` already presents them as
    strings; raw ``text()`` reads would otherwise hand back
    ``uuid.UUID`` objects, and every consumer written against the
    string presentation — kwargs serialization, log slicing, JSON
    payloads — would have to defend against the drift one seam at a
    time. One loader on the driver keeps the presentation single-owner.

    Scope: horsies registers this on the engines IT creates (the
    broker's runtime engine; the integration conftest engine). It is
    never assumed onto engines a consumer application builds — their
    uuid reads keep whatever presentation their driver configuration
    chooses.

    A test double standing in for the engine cannot host driver
    events; registration is a no-op for anything that is not a real
    ``AsyncEngine``.
    """
    if not isinstance(engine, AsyncEngine):
        return

    @event.listens_for(engine.sync_engine, 'connect')
    def _register_uuid_text_loader(
        dbapi_connection: Any, _connection_record: Any
    ) -> None:
        dbapi_connection.driver_connection.adapters.register_loader(
            'uuid', TextLoader
        )


def uuid_column_value(value: object, *, column: str) -> UUID:
    """The ``uuid.UUID`` of a non-null uuid column value as a driver reads it.

    An engine with ``register_identity_text_reads`` reads the value as its
    text; any other psycopg engine reads a ``uuid.UUID``. A connection
    that a caller passes in (for example to ``rerun_task``) can be either.

    Raises:
        TypeError: the value is neither; ``column`` names the column.
        ValueError: the text is not a UUID; ``column`` names the column.
    """
    match value:
        case UUID():
            return value
        case str():
            try:
                return UUID(value)
            except ValueError as exc:
                raise ValueError(
                    f'uuid column {column} read text that is not a UUID: '
                    f'{value!r}'
                ) from exc
        case _:
            raise TypeError(
                f'uuid column {column} read a {type(value).__name__}, '
                'not text or uuid.UUID'
            )


def _sync_engine_of(engine: AsyncEngine | Engine) -> Engine | None:
    """The sync engine that SQLAlchemy events attach to: an ``AsyncEngine``'s
    ``sync_engine``, or the ``Engine`` itself. None for anything else (a test
    double), so that registration is a no-op for it."""
    match engine:
        case AsyncEngine():
            return engine.sync_engine
        case Engine():
            return engine
        case _:
            return None


class SessionSettings(Enum):
    """Whether a pooled server session keeps a client's session settings.

    KEPT: a direct or session-pooled connection; a session-level SET stays
    with the client that issued it. NOT_KEPT: a PgBouncer transaction-mode
    connection; server sessions are shared between clients, so a session
    SET would leak to other clients.
    """

    KEPT = 'KEPT'
    NOT_KEPT = 'NOT_KEPT'


# The partition coverage probe's cost estimate is above the default
# jit_above_cost, so a JIT-enabled server compiles it on every call.
DISABLE_JIT_SQL: Final = 'SET jit = off'


def register_session_settings(
    engine: AsyncEngine | Engine,
    settings: SessionSettings,
) -> None:
    """Run ``SET jit = off`` once on each new connection that keeps it.

    The SET runs in autocommit, so a rollback of the connection's first
    transaction cannot undo it. A NOT_KEPT engine keeps the server's JIT
    configuration. Registration is a no-op for anything that is not a real
    ``AsyncEngine`` or ``Engine``.
    """
    sync_engine = _sync_engine_of(engine)
    if sync_engine is None:
        return
    match settings:
        case SessionSettings.NOT_KEPT:
            return
        case SessionSettings.KEPT:
            pass

    def _disable_jit(dbapi_connection: Any, _connection_record: Any) -> None:
        autocommit = dbapi_connection.autocommit
        dbapi_connection.autocommit = True
        try:
            cursor = dbapi_connection.cursor()
            try:
                cursor.execute(DISABLE_JIT_SQL)
            finally:
                cursor.close()
        finally:
            dbapi_connection.autocommit = autocommit

    event.listen(sync_engine, 'connect', _disable_jit)


CHECKED_IN_AT_KEY: Final = 'horsies_checked_in_at'
"""The key in a pooled connection's ``info`` that holds the monotonic time of
its last check-in. SQLAlchemy clears ``info`` when it replaces the
connection, so a new connection has no entry."""


def register_idle_ping(engine: AsyncEngine | Engine, after_idle_s: float) -> None:
    """Ping a pooled connection of ``engine`` at checkout when it waited in
    the pool for longer than ``after_idle_s``. This replaces SQLAlchemy's
    ``pool_pre_ping``, which pings at every checkout.

    Contract:

    - A connection checked in within ``after_idle_s``, and a new connection,
      are used with no ping.
    - The ping is the dialect's own ``do_ping``, as with ``pool_pre_ping``.
      A ping that fails with a disconnect raises ``InvalidatePoolError``
      from the checkout listener: the pool replaces that connection and
      every connection checked in before it, and the checkout takes a new
      one.
    - A ping that fails without a disconnect raises its driver error from
      the checkout.

    It does not ping a connection in use, and it does not retry a statement:
    a connection that dies within ``after_idle_s`` of its check-in fails the
    next statement on it, as a connection that dies after a ping does.
    Registration is a no-op for anything that is not a real ``AsyncEngine``
    or ``Engine``, as with ``register_session_settings``.
    """
    sync_engine = _sync_engine_of(engine)
    if sync_engine is None:
        return
    dialect = sync_engine.dialect

    def _mark_checkin(dbapi_connection: Any, connection_record: Any) -> None:
        if dbapi_connection is None:
            return  # invalidated: SQLAlchemy replaces it
        connection_record.info[CHECKED_IN_AT_KEY] = time.monotonic()

    def _ping_after_idle(
        dbapi_connection: Any,
        connection_record: Any,
        connection_proxy: PoolProxiedConnection,
    ) -> None:
        match connection_record.info.get(CHECKED_IN_AT_KEY):
            case None:
                return  # a new connection
            case float() as checked_in_at:
                if time.monotonic() - checked_in_at <= after_idle_s:
                    return
            case unexpected:
                raise AssertionError(f'{CHECKED_IN_AT_KEY} holds {unexpected!r}')
        try:
            _ = dialect.do_ping(dbapi_connection)
        except psycopg.Error as exc:
            if _dialect_sees_disconnect(dialect, exc, connection_proxy):
                raise InvalidatePoolError(
                    f'a pooled connection idle for more than {after_idle_s} s '
                    f'failed its ping: {exc!r}',
                ) from exc
            raise

    event.listen(sync_engine.pool, 'checkin', _mark_checkin)
    event.listen(sync_engine.pool, 'checkout', _ping_after_idle)


def is_dbapi_disconnect(exc: DBAPIError) -> bool:
    """Check whether a SQLAlchemy DBAPIError represents a connection disconnect."""
    connection_invalidated = bool(getattr(exc, 'connection_invalidated', False))
    is_disconnect = bool(getattr(exc, 'is_disconnect', False))
    return connection_invalidated or is_disconnect


def is_retryable_connection_error(exc: BaseException) -> bool:
    """Check whether an exception is a transient connection error worth retrying.

    A core failure class carries the core's classification of its cause
    (``retryable``); a contract violation never clears. A ``CoreError`` is
    retryable when its condition clears in this process
    (``core_error_retryable``).
    """
    match exc:
        case OperationalError() | InterfaceError() | SAOperationalError():
            return True
        case CoreDatabaseError(retryable=retryable) | CorePreconditionFailed(retryable=retryable):
            return retryable
        case CoreContractViolation():
            return False
        case CoreError() as core_error:
            return core_error_retryable(core_error)
        # Engine pool checkout timeout ("QueuePool limit reached"): transient
        # infrastructure pressure, same class as a failed connect. It only
        # subclasses SQLAlchemyError, so the arms above never match it;
        # misclassifying it as permanent latched the reaper's requeue breaker
        # off for the process lifetime during a connection-slot exhaustion.
        case SATimeoutError():
            return True
        case DBAPIError() as db_exc if is_dbapi_disconnect(db_exc):
            return True
        case _:
            return False


def _dialect_sees_disconnect(
    dialect: Dialect,
    exc: psycopg.Error,
    pooled: PoolProxiedConnection,
) -> bool:
    """The dialect's disconnect check, as SQLAlchemy runs it on the per-call
    path (for psycopg: the connection is closed or broken)."""
    # SQLAlchemy types the error as the class of its DBAPI protocol; at run
    # time it passes the driver's own error, as here.
    return bool(
        dialect.is_disconnect(cast('DBAPIModule.Error', exc), pooled, None),
    )


