"""Python mirror of the core's version-1 command fingerprint, for seeding tests only.

The core computes the fingerprint of every send
(``EnqueueCommandV1``, ``core/src/core/history/identity/fingerprint.rs``)
and pins its digests. Tests that insert a task row with test-side SQL use
this mirror to store the fingerprint that a send of the same values stores.

The digest is SHA-256 over a compact JSON array whose element order is part
of version 1: the version, the task name, the queue name, the priority, the
positional and keyword input texts, ``good_until`` as
``YYYY-MM-DDTHH:MM:SS.ffffff+00:00`` in UTC, the enqueue delay, the task
options text, the retention class key, ``retain_rerun_input``, and the rerun
source and root ids.
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
    """The version-1 fingerprint projection of one send.

    A send stores its input in the one ``input`` column and passes that text
    as ``input_json``; ``positional_args_json`` is ``None`` for every send.
    """

    task_name: str
    queue_name: str
    priority: int
    positional_args_json: str | None
    input_json: str | None
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
                self.positional_args_json,
                self.input_json,
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
            allow_nan=False,
        ).encode()
        return hashlib.sha256(canonical).digest()


def _canonical_datetime(value: datetime | None) -> str | None:
    if value is None:
        return None
    utc = value.astimezone(timezone.utc)
    return utc.strftime('%Y-%m-%dT%H:%M:%S.%f+00:00')
