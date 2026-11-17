"""Time helpers shared across horsies internals."""

from __future__ import annotations

from datetime import datetime, timezone


def utc_now() -> datetime:
    """Current UTC instant as a tz-aware datetime.

    Use as the callable in SQLAlchemy `default=`/`onupdate=` arguments so
    the value is evaluated per row, not frozen at module import.
    """
    return datetime.now(timezone.utc)
