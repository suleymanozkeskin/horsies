"""Deterministic UUIDv7 helpers for disposable history prototypes."""

from __future__ import annotations

import hashlib
from datetime import datetime, timezone
from uuid import UUID


_MAX_UNIX_MILLISECONDS = (1 << 48) - 1


def uuid7_for_row(
    birth_at: datetime,
    *,
    domain: str,
    row_number: int,
    sequence_within_millisecond: int = 0,
) -> str:
    """Return a valid deterministic UUIDv7 for one measured row."""
    if birth_at.tzinfo is None:
        raise ValueError('UUIDv7 birth time must be timezone-aware')
    if not domain:
        raise ValueError('UUIDv7 entropy domain must not be empty')
    if row_number <= 0:
        raise ValueError('UUIDv7 row number must be positive')
    if not 0 <= sequence_within_millisecond < 1 << 12:
        raise ValueError(
            'UUIDv7 within-millisecond sequence must fit rand_a'
        )
    unix_milliseconds = int(birth_at.timestamp() * 1_000)
    if not 0 <= unix_milliseconds <= _MAX_UNIX_MILLISECONDS:
        raise ValueError('UUIDv7 birth time is outside the 48-bit range')
    entropy = hashlib.md5(
        f'{domain}-{row_number}'.encode(),
        usedforsecurity=False,
    ).hexdigest()
    value = UUID(
        hex=(
            f'{unix_milliseconds:012x}'
            f'7{sequence_within_millisecond:03x}'
            f'8{entropy[4:19]}'
        )
    )
    if value.version != 7:
        raise AssertionError('constructed UUID does not carry version 7')
    return str(value)


def uuid7_birth_at(value: str | UUID) -> datetime | None:
    """Decode the UTC millisecond timestamp, or return None for another UUID."""
    parsed = value if isinstance(value, UUID) else UUID(value)
    if parsed.version != 7:
        return None
    unix_milliseconds = parsed.int >> 80
    return datetime.fromtimestamp(
        unix_milliseconds / 1_000,
        tz=timezone.utc,
    )
