# horsies/core/utils/db.py
"""Shared helpers for classifying transient database errors."""

from __future__ import annotations

from psycopg import InterfaceError, OperationalError
from sqlalchemy.exc import (
    DBAPIError,
    OperationalError as SAOperationalError,
    TimeoutError as SATimeoutError,
)


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
