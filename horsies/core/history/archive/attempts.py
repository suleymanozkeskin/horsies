"""Attempt-snapshot codec, version 1.

One terminal history row carries its complete ordered attempt sequence as
canonical compact UTF-8 JSON: an array of 12-element positional rows, one
per attempt, timestamps as signed integer Unix-epoch microseconds. The
field order is part of the version — adding, removing, or reordering a
field requires version 2 and a retained decoder, never an in-place change.

The SQL terminalization function writes these bytes, and this module decodes
them. The canonical form is compact separators, no ASCII escaping of non-ASCII
text, and the positional layout below. The digest covers the bytes, so this
form cannot change within version 1.

Decoding validates shape exhaustively before constructing records: arity,
per-field types (booleans are rejected where integers are required, since
Python bools are ints), timestamp integrality and range, and the
contiguous-from-1 attempt numbering that the complete-history contract
requires. Every rejection is a typed corrupt-value failure naming what was
wrong.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Final, cast

from horsies.core.codec.json_io import loads_json_bytes
from horsies.core.types.result import is_err

from .versions import (
    ArchiveDecodeResult,
    ArchiveDomain,
    CorruptArchiveValue,
    DecodedArchiveValue,
    validate_envelope_contract,
    verify_payload_digest,
)


ATTEMPT_FIELD_COUNT: Final = 12
_UTC_EPOCH: Final = datetime(1970, 1, 1, tzinfo=timezone.utc)


@dataclass(frozen=True, slots=True)
class AttemptRecord:
    """One attempt as the snapshot preserves it.

    `attempt` numbers from 1 and the sequence is contiguous; `will_retry`
    records whether another attempt was scheduled when this one finished.
    Worker attribution may be absent for attempts that never reached a
    worker.
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


def decode_attempt_snapshot(
    *,
    version: int,
    codec: str,
    content_type: str,
    payload: bytes,
    digest: bytes,
) -> ArchiveDecodeResult[tuple[AttemptRecord, ...]]:
    """Decode a stored snapshot, failing closed on every malformation."""
    contract_failure = validate_envelope_contract(
        domain=ArchiveDomain.ATTEMPTS,
        version=version,
        codec=codec,
        content_type=content_type,
    )
    if contract_failure is not None:
        return contract_failure
    digest_failure = verify_payload_digest(
        domain=ArchiveDomain.ATTEMPTS, payload=payload, digest=digest
    )
    if digest_failure is not None:
        return digest_failure
    loaded = loads_json_bytes(payload)
    if is_err(loaded):
        return _corrupt('invalid_json')
    parsed: Any = loaded.ok_value
    if not isinstance(parsed, list):
        return _corrupt('expected_array')
    items = cast(list[Any], parsed)

    records: list[AttemptRecord] = []
    for item in items:
        record = _decode_positional_row(item)
        match record:
            case AttemptRecord():
                records.append(record)
            case CorruptArchiveValue():
                return record
    expected = tuple(range(1, len(records) + 1))
    actual = tuple(record.attempt for record in records)
    if actual != expected:
        return _corrupt('non_contiguous_attempts')
    return DecodedArchiveValue(tuple(records))


def _decode_positional_row(item: Any) -> AttemptRecord | CorruptArchiveValue:
    if not isinstance(item, list):
        return _corrupt('expected_positional_row')
    row = cast(list[Any], item)
    if len(row) != ATTEMPT_FIELD_COUNT:
        return _corrupt('wrong_field_count')

    attempt = row[0]
    if not _is_plain_int(attempt) or attempt < 1:
        return _corrupt('invalid_attempt_number')
    outcome = row[1]
    if not isinstance(outcome, str) or not outcome:
        return _corrupt('invalid_outcome')
    will_retry = row[2]
    if not isinstance(will_retry, bool):
        return _corrupt('invalid_will_retry')

    started_at = _decode_epoch_us(row[3])
    if started_at is None:
        return _corrupt('invalid_started_at')
    finished_at = _decode_epoch_us(row[4])
    if finished_at is None:
        return _corrupt('invalid_finished_at')

    optional_text: list[str | None] = []
    for index in (5, 6, 7, 8, 9):
        value = row[index]
        if value is not None and not isinstance(value, str):
            return _corrupt(f'invalid_text_field_{index}')
        optional_text.append(value)
    worker_pid = row[10]
    if worker_pid is not None and not _is_plain_int(worker_pid):
        return _corrupt('invalid_worker_pid')
    worker_process_name = row[11]
    if worker_process_name is not None and not isinstance(worker_process_name, str):
        return _corrupt('invalid_worker_process_name')

    return AttemptRecord(
        attempt=attempt,
        outcome=outcome,
        will_retry=will_retry,
        started_at=started_at,
        finished_at=finished_at,
        error_code=optional_text[0],
        error_message=optional_text[1],
        failed_reason=optional_text[2],
        worker_id=optional_text[3],
        worker_hostname=optional_text[4],
        worker_pid=worker_pid,
        worker_process_name=worker_process_name,
    )


def _is_plain_int(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def _decode_epoch_us(value: Any) -> datetime | None:
    """Signed integer epoch microseconds to an aware datetime, or None.

    Fractional timestamps are rejected by the integer check: version 1
    stores microsecond integers, and a float in that position means the
    writer did not follow the version it claimed.
    """
    if not _is_plain_int(value):
        return None
    try:
        return _UTC_EPOCH + timedelta(microseconds=value)
    except OverflowError:
        return None


def _corrupt(detail: str) -> CorruptArchiveValue:
    return CorruptArchiveValue(domain=ArchiveDomain.ATTEMPTS, detail=detail)
