"""The deterministic task id of a schedule slot."""

from __future__ import annotations

import hashlib
import uuid
from datetime import datetime, timedelta, timezone
from typing import Final


# The hash domain of the schedule slot id, as ``horsies_schedule_slot_task_id``
# writes it. A change makes every slot id new, so a fired slot could fire again.
_SLOT_ID_DOMAIN: Final = b'horsies.schedule_slot.v1'
_UNIX_EPOCH: Final = datetime(1970, 1, 1, tzinfo=timezone.utc)
_MAX_SLOT_MICROSECONDS: Final = (1 << 48) * 1000


def schedule_slot_task_id(schedule_name: str, slot_time: datetime) -> uuid.UUID:
    """The task id of one schedule slot: the rule of ``horsies_schedule_slot_task_id``.

    A pure function of the schedule name and the slot instant, with the
    UUIDv7 layout, so the slot time is the id's birth and the history
    readers prune their probes by it:

    - bytes 0-5: the slot time in Unix milliseconds (48 bits, big endian);
    - 4 bits version 7, 2 bits variant 10;
    - the other 74 bits: the first bits of ``sha256(_SLOT_ID_DOMAIN || 0x00
      || slot time as a big-endian int8 of Unix microseconds || name as
      UTF-8)``.

    The same name and instant give the same id in any time zone; another
    name, or another microsecond, gives another id.

    Raises:
        ValueError: ``slot_time`` is naive. A slot computed by the scheduler
            is always aware, so this is a defect of the caller.
        AssertionError: the slot is before 1970 or past the 48-bit
            millisecond range; no schedule computes such a slot.
    """
    microseconds = _unix_microseconds(slot_time)
    assert 0 <= microseconds < _MAX_SLOT_MICROSECONDS, (
        f'slot time {slot_time!r} of schedule {schedule_name!r} is outside the '
        'UUIDv7 time range'
    )
    tail = bytearray(
        hashlib.sha256(
            _SLOT_ID_DOMAIN
            + b'\x00'
            + microseconds.to_bytes(8, 'big', signed=True)
            + schedule_name.encode('utf-8')
        ).digest()[:10]
    )
    tail[0] = 0x70 | (tail[0] & 0x0F)
    tail[2] = 0x80 | (tail[2] & 0x3F)
    milliseconds = microseconds // 1000
    return uuid.UUID(bytes=milliseconds.to_bytes(6, 'big') + bytes(tail))


def _unix_microseconds(value: datetime) -> int:
    """Whole microseconds since 1970-01-01 UTC, exact (no float)."""
    if value.tzinfo is None:
        raise ValueError(f'a slot time must be timezone-aware, got naive: {value!r}')
    return (value - _UNIX_EPOCH) // timedelta(microseconds=1)
