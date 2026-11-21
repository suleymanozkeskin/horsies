"""Task ids as a send mints them: UUIDv7 from the core's id source.

``mint_task_id`` calls ``horsies._core.mint_task_id``, the source of every
send (``task_decorator._mint_send_task_id``). ``uuid7_birth_at`` decodes the
birth time that a UUIDv7 carries in its first 48 bits.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone

import horsies._core as core


def mint_task_id() -> uuid.UUID:
    """A new task id from the core's UUIDv7 source. A refused mint fails the
    caller with an ``AssertionError``."""
    match core.mint_task_id():
        case uuid.UUID() as task_id:
            return task_id
        case core.IdMintRefused() as refused:
            raise AssertionError(f'the core refused to mint a task id: {refused!r}')


def uuid7_birth_at(value: str | uuid.UUID) -> datetime | None:
    """The UTC birth time of a UUIDv7, or ``None`` for another version."""
    parsed = value if isinstance(value, uuid.UUID) else uuid.UUID(value)
    if parsed.version != 7:
        return None
    unix_milliseconds = parsed.int >> 80
    return datetime.fromtimestamp(unix_milliseconds / 1_000, tz=timezone.utc)
