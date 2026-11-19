"""Python mirror of the SQL attempt-snapshot writer, for seeding tests only.

``horsies_encode_task_attempts`` (0006_history_functions.sql) writes the
stored bytes of a history row's attempt snapshot, and the core decodes them
(``core/src/core/history/archive/attempts.rs``). Tests use
this mirror to seed history rows. A test that checks what the core wrote
reads the attempts through ``PostgresBroker.get_task_info_async`` (see
``tests/integration/history_seeding.py::read_attempts``); the mirror has no
decoder.

Format version 1: canonical compact UTF-8 JSON, an array of 12-element
positional rows, one per attempt, timestamps as signed integer Unix-epoch
microseconds; the digest is SHA-256 of the bytes.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Final

_ARCHIVE_VERSION: Final = 1
_UTC_EPOCH: Final = datetime(1970, 1, 1, tzinfo=timezone.utc)


@dataclass(frozen=True, slots=True)
class AttemptRecord:
    """One attempt as a seeded snapshot holds it.

    ``attempt`` numbers from 1 and the sequence is contiguous; the
    timestamps are timezone-aware.
    """

    attempt: int
    outcome: str
    will_retry: bool
    started_at: datetime
    finished_at: datetime
    error_code: str | None
    error_message: str | None
    failed_reason: str | None
    worker_id: str | None
    worker_hostname: str | None
    worker_pid: int | None
    worker_process_name: str | None

    def __post_init__(self) -> None:
        if self.attempt < 1:
            raise ValueError('attempt numbers start at 1')
        if not self.outcome:
            raise ValueError('attempt outcome must be non-empty')
        if self.started_at.tzinfo is None or self.finished_at.tzinfo is None:
            raise ValueError('attempt timestamps must be timezone-aware')


@dataclass(frozen=True, slots=True)
class StoredAttemptSnapshot:
    """An encoded snapshot with the envelope facts a history row stores."""

    version: int
    payload: bytes
    digest: bytes


def snapshot_digest(payload: bytes) -> bytes:
    """The stored digest of snapshot bytes: SHA-256, 32 bytes."""
    return hashlib.sha256(payload).digest()


def encode_attempt_snapshot(
    attempts: tuple[AttemptRecord, ...],
) -> StoredAttemptSnapshot:
    """Encode a complete ordered sequence as canonical version-1 bytes.

    Rejects a sequence that is not contiguous from 1, so a fixture cannot
    seed a row that the decoder would refuse.
    """
    expected = tuple(range(1, len(attempts) + 1))
    actual = tuple(record.attempt for record in attempts)
    if actual != expected:
        raise ValueError('attempts must be ordered and contiguous from 1')
    payload = json.dumps(
        [_positional_row(record) for record in attempts],
        ensure_ascii=False,
        separators=(',', ':'),
    ).encode()
    return StoredAttemptSnapshot(
        version=_ARCHIVE_VERSION,
        payload=payload,
        digest=snapshot_digest(payload),
    )


def _positional_row(record: AttemptRecord) -> list[Any]:
    return [
        record.attempt,
        record.outcome,
        record.will_retry,
        _datetime_to_epoch_us(record.started_at),
        _datetime_to_epoch_us(record.finished_at),
        record.error_code,
        record.error_message,
        record.failed_reason,
        record.worker_id,
        record.worker_hostname,
        record.worker_pid,
        record.worker_process_name,
    ]


def _datetime_to_epoch_us(value: datetime) -> int:
    delta = value.astimezone(timezone.utc) - _UTC_EPOCH
    return (
        delta.days * 86_400_000_000
        + delta.seconds * 1_000_000
        + delta.microseconds
    )
