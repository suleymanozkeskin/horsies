"""Deterministic hashing and ID functions for idempotent enqueue.

Both the broker and the scheduler import from this module.
"""

from __future__ import annotations

import hashlib
import json
import uuid
from datetime import datetime, timezone


# Fixed namespace for schedule-derived deterministic UUIDs.
# Never change this value — it would invalidate all existing schedule idempotency.
_SCHEDULE_NAMESPACE = uuid.UUID('3c01f3f5-afd6-4363-b726-a5dab51a81c7')


def _canon_dt(dt: datetime) -> str:
    """Canonical UTC string for hashing. Normalizes to UTC offset +00:00.

    Guarantees the same instant always produces the same string regardless
    of the original tzinfo representation (datetime.timezone.utc,
    zoneinfo.ZoneInfo('UTC'), psycopg/asyncpg-returned tzinfo, etc.).

    Raises ValueError if dt is naive (tzinfo is None). Naive datetimes are
    ambiguous — we cannot know which instant they represent, so hashing
    them would produce silently wrong results.
    """
    if dt.tzinfo is None:
        raise ValueError(
            f'_canon_dt requires timezone-aware datetime, got naive: {dt!r}',
        )
    utc = dt.astimezone(timezone.utc)
    return utc.strftime('%Y-%m-%dT%H:%M:%S.%f+00:00')


def enqueue_fingerprint(
    task_name: str,
    queue_name: str,
    priority: int,
    args_json: str | None,
    kwargs_json: str | None,
    sent_at: datetime,
    good_until: datetime | None,
    enqueue_delay_seconds: int | None,
    task_options: str | None,
) -> str:
    """Deterministic SHA-256 fingerprint for enqueue payload identity.

    Computed once from Python values before INSERT. Stored in the row.
    On conflict, the stored hash is compared directly — no recomputation
    from DB values, no datetime round-trip risk.
    """
    canonical = json.dumps(
        [
            task_name,
            queue_name,
            priority,
            args_json,
            kwargs_json,
            _canon_dt(sent_at),
            _canon_dt(good_until) if good_until is not None else None,
            enqueue_delay_seconds,
            task_options,
        ],
        sort_keys=False,
        separators=(',', ':'),
    )
    return hashlib.sha256(canonical.encode()).hexdigest()


def schedule_slot_task_id(schedule_name: str, slot_time: datetime) -> str:
    """Deterministic task_id for a schedule name + slot time.

    Same schedule + same slot -> same UUID5 -> idempotent on conflict.
    Different schedule or different slot -> different UUID5 -> no collision.
    """
    return str(uuid.uuid5(
        _SCHEDULE_NAMESPACE,
        f"{schedule_name}:{_canon_dt(slot_time)}",
    ))
