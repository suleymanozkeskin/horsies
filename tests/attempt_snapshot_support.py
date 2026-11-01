"""Python mirror of the SQL attempt-snapshot writer, for tests only.

`horsies_encode_task_attempts` (`terminalization/move.py`) writes the stored
bytes. Tests use this mirror to seed history rows and to build decoder input.
`tests/unit/test_task_history_archive_codecs.py` pins its exact bytes.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Final

from horsies.core.history.archive.attempts import AttemptRecord
from horsies.core.history.archive.versions import (
    JSON_CONTENT_TYPE,
    JSON_UTF8_CODEC,
    archive_digest,
)

_ARCHIVE_VERSION: Final = 1
_UTC_EPOCH: Final = datetime(1970, 1, 1, tzinfo=timezone.utc)


@dataclass(frozen=True, slots=True)
class StoredAttemptSnapshot:
    """An encoded snapshot with the envelope facts a history row stores."""

    version: int
    codec: str
    content_type: str
    payload: bytes
    digest: bytes


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
        codec=JSON_UTF8_CODEC,
        content_type=JSON_CONTENT_TYPE,
        payload=payload,
        digest=archive_digest(payload),
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
