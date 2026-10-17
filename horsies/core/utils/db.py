# horsies/core/utils/db.py
"""Shared helpers for classifying transient database errors."""

from __future__ import annotations

from enum import Enum
from typing import Any, Final

from psycopg import InterfaceError, OperationalError
from psycopg.types.string import TextLoader
from sqlalchemy import event
from sqlalchemy.exc import (
    DBAPIError,
    OperationalError as SAOperationalError,
    TimeoutError as SATimeoutError,
)
from sqlalchemy.ext.asyncio import AsyncEngine


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
    engine: AsyncEngine,
    settings: SessionSettings,
) -> None:
    """Run ``SET jit = off`` once on each new connection that keeps it.

    The SET runs in autocommit, so a rollback of the connection's first
    transaction cannot undo it. A NOT_KEPT engine keeps the server's JIT
    configuration. Like ``register_identity_text_reads``, registration is
    a no-op for anything that is not a real ``AsyncEngine``.
    """
    if not isinstance(engine, AsyncEngine):  # pyright: ignore[reportUnnecessaryIsInstance]
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

    event.listen(engine.sync_engine, 'connect', _disable_jit)


def is_dbapi_disconnect(exc: DBAPIError) -> bool:
    """Check whether a SQLAlchemy DBAPIError represents a connection disconnect."""
    connection_invalidated = bool(getattr(exc, 'connection_invalidated', False))
    is_disconnect = bool(getattr(exc, 'is_disconnect', False))
    return connection_invalidated or is_disconnect


def is_retryable_connection_error(exc: BaseException) -> bool:
    """Check whether an exception is a transient connection error worth retrying."""
    match exc:
        case OperationalError() | InterfaceError() | SAOperationalError():
            return True
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
