"""Command fingerprint, version 1.

The fingerprint decides replay versus conflict for keyed enqueue: the same
scoped key with the same fingerprint returns the committed request, a
different fingerprint is a typed conflict. Every field that changes what
the request asks for participates — including the effective
`retain_rerun_input` snapshot, because accepting a replay that requests a
different value would silently change the request's retained-input promise
while returning the original identity.

The digest is SHA-256 over a canonical compact JSON array whose element
order is part of version 1. Adding, removing, or reordering a field is
version 2, never an in-place change: stored fingerprints are compared
byte-for-byte forever.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Final


COMMAND_FINGERPRINT_VERSION: Final = 1


@dataclass(frozen=True, slots=True)
class EnqueueCommandV1:
    """The version-1 fingerprint projection of one enqueue request.

    Serialized inputs travel as their canonical JSON strings — the
    fingerprint covers the bytes the request actually carries, not a
    re-serialization.
    """

    task_name: str
    queue_name: str
    priority: int
    args_json: str | None
    kwargs_json: str | None
    good_until: datetime | None
    enqueue_delay_seconds: int | None
    task_options_json: str | None
    retention_class_key: str
    retain_rerun_input: bool
    rerun_of_task_id: str | None
    rerun_root_task_id: str | None

    def __post_init__(self) -> None:
        if not self.task_name:
            raise ValueError('task_name must be non-empty')
        if not self.queue_name:
            raise ValueError('queue_name must be non-empty')
        if not 1 <= self.priority <= 100:
            raise ValueError('priority must be between 1 and 100')
        if self.good_until is not None and self.good_until.tzinfo is None:
            raise ValueError('good_until must be timezone-aware')
        if self.enqueue_delay_seconds is not None and self.enqueue_delay_seconds < 0:
            raise ValueError('enqueue_delay_seconds must be non-negative')
        if not self.retention_class_key:
            raise ValueError('retention_class_key must be non-empty')
        if (self.rerun_of_task_id is None) != (self.rerun_root_task_id is None):
            raise ValueError('rerun source and root must be present together')

    @property
    def fingerprint(self) -> bytes:
        """The 32-byte version-1 command fingerprint."""
        canonical = json.dumps(
            [
                COMMAND_FINGERPRINT_VERSION,
                self.task_name,
                self.queue_name,
                self.priority,
                self.args_json,
                self.kwargs_json,
                _canonical_datetime(self.good_until),
                self.enqueue_delay_seconds,
                self.task_options_json,
                self.retention_class_key,
                self.retain_rerun_input,
                self.rerun_of_task_id,
                self.rerun_root_task_id,
            ],
            ensure_ascii=False,
            separators=(',', ':'),
        ).encode()
        return hashlib.sha256(canonical).digest()


def _canonical_datetime(value: datetime | None) -> str | None:
    if value is None:
        return None
    utc = value.astimezone(timezone.utc)
    return utc.strftime('%Y-%m-%dT%H:%M:%S.%f+00:00')
