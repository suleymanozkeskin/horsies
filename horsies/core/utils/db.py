# horsies/core/utils/db.py
"""Shared helpers for classifying transient database errors."""

from __future__ import annotations

from psycopg import InterfaceError, OperationalError
from sqlalchemy.exc import DBAPIError, OperationalError as SAOperationalError


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
        case DBAPIError() as db_exc if is_dbapi_disconnect(db_exc):
            return True
        case _:
            return False
