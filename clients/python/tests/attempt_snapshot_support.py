"""Python mirror of the SQL attempt-archive writer, for seeding tests only.

``horsies_encode_attempt_archive`` (``core/migrations/1001_lifecycle_record.sql``)
writes the stored bytes of a history row's attempt archive, and the core
decodes them (``core/src/core/history/archive/attempts.rs``). Tests use this
mirror to seed history rows. A test that checks what the core wrote reads
the attempts through ``PostgresBroker.get_task_info_async`` (see
``tests/integration/history_seeding.py::read_attempts``); the mirror has no
decoder.

Both formats are canonical compact UTF-8 JSON, timestamps as signed integer
Unix-epoch microseconds; the digest is SHA-256 of the bytes.

- Version 1 (``encode_attempt_snapshot``): an array of 12-element positional
  rows, one per attempt. The core still reads it; the migration chain no
  longer writes it.
- Version 2 (``encode_attempt_archive``): ``[attempts, open]``. Each attempt
  is the 12 values of version 1, then the 9 values of its phase record
  (``phase_record_version``, ``enqueued_at``, ``claimed_at``,
  ``attempt_claims``, ``first_claimed_at``, ``placed_at``, ``finalizing_at``,
  ``recorded_by``, ``last_seen_at``; all null with no phase record). ``open``
  is null when the last attempt ended with its row, else
  ``[attempt_claims, first_claimed_at, finalizing_at]`` of the task row.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Final

_ARCHIVE_VERSION: Final = 1
_ARCHIVE_VERSION_2: Final = 2
_PHASE_RECORD_VERSION: Final = 1
"""The form of the phase record that migration 1001 writes."""
_PHASE_FIELD_COUNT: Final = 9
_RECORDED_BY_WORKER: Final = 'WORKER'
_RECORDED_BY_RECOVERY: Final = 'RECOVERY'
_UTC_EPOCH: Final = datetime(1970, 1, 1, tzinfo=timezone.utc)


@dataclass(frozen=True, slots=True)
class ClaimsNotCounted:
    """The attempt began before migration 1001: its claims were not counted."""


@dataclass(frozen=True, slots=True)
class ClaimsCounted:
    """``count`` claims ran for the attempt (1 or more); the first at ``first_claimed_at``."""

    count: int
    first_claimed_at: datetime

    def __post_init__(self) -> None:
        if self.count < 1:
            raise ValueError('a counted attempt has 1 claim or more')


@dataclass(frozen=True, slots=True)
class NotClaimed:
    """The open attempt was never claimed (``attempt_claims`` 0)."""


@dataclass(frozen=True, slots=True)
class Placed:
    """A child took the task at ``at``."""

    at: datetime


@dataclass(frozen=True, slots=True)
class PlacementNotObserved:
    """The writer of the attempt row observed no placement."""


@dataclass(frozen=True, slots=True)
class FinalizingMarked:
    """The child marked the end of the body at ``at``."""

    at: datetime


@dataclass(frozen=True, slots=True)
class FinalizingNotMarked:
    """No finalizing marker was written."""


@dataclass(frozen=True, slots=True)
class RecordedByWorker:
    """The worker wrote the end of the attempt, with the placement it observed."""

    placement: Placed | PlacementNotObserved


@dataclass(frozen=True, slots=True)
class RecordedByRecovery:
    """Stale RUNNING recovery wrote the attempt row; ``last_seen_at`` is the
    last time the attempt was known alive."""

    last_seen_at: datetime


@dataclass(frozen=True, slots=True)
class PhasesNotCaptured:
    """The attempt row has no phase record."""


@dataclass(frozen=True, slots=True)
class PhasesCaptured:
    """The phase record (version 1) of one recorded attempt."""

    enqueued_at: datetime
    claimed_at: datetime
    claims: ClaimsNotCounted | ClaimsCounted
    finalizing: FinalizingMarked | FinalizingNotMarked
    recorded_by: RecordedByWorker | RecordedByRecovery


type PhaseRecord = PhasesNotCaptured | PhasesCaptured


@dataclass(frozen=True, slots=True)
class AttemptEnded:
    """The last attempt ended with its row: the archive stores ``null``."""


@dataclass(frozen=True, slots=True)
class OpenAttempt:
    """The attempt ``retry_count + 1`` has no row: the claim facts of the task row."""

    claims: ClaimsNotCounted | NotClaimed | ClaimsCounted
    finalizing: FinalizingMarked | FinalizingNotMarked


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
    phases: PhaseRecord = PhasesNotCaptured()
    """Version 1 stores no phase record; ``encode_attempt_snapshot`` refuses one."""

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
    _check_contiguous(attempts)
    if any(isinstance(record.phases, PhasesCaptured) for record in attempts):
        raise ValueError('version 1 stores no phase record')
    return _stored(_ARCHIVE_VERSION, [_positional_row(record) for record in attempts])


def encode_attempt_archive(
    attempts: tuple[AttemptRecord, ...],
    open_attempt: AttemptEnded | OpenAttempt,
) -> StoredAttemptSnapshot:
    """Encode a complete ordered sequence and the open attempt as canonical
    version-2 bytes, the bytes of ``horsies_encode_attempt_archive``.

    Rejects a sequence that is not contiguous from 1.
    """
    _check_contiguous(attempts)
    rows = [[*_positional_row(record), *_phase_values(record.phases)] for record in attempts]
    match open_attempt:
        case AttemptEnded():
            open_value: list[Any] | None = None
        case OpenAttempt(claims=claims, finalizing=finalizing):
            open_value = [*_open_claim_values(claims), _finalizing_value(finalizing)]
    return _stored(_ARCHIVE_VERSION_2, [rows, open_value])


def _check_contiguous(attempts: tuple[AttemptRecord, ...]) -> None:
    expected = tuple(range(1, len(attempts) + 1))
    actual = tuple(record.attempt for record in attempts)
    if actual != expected:
        raise ValueError('attempts must be ordered and contiguous from 1')


def _stored(version: int, document: list[Any]) -> StoredAttemptSnapshot:
    payload = json.dumps(
        document,
        ensure_ascii=False,
        separators=(',', ':'),
    ).encode()
    return StoredAttemptSnapshot(
        version=version,
        payload=payload,
        digest=snapshot_digest(payload),
    )


def _phase_values(phases: PhaseRecord) -> list[Any]:
    match phases:
        case PhasesNotCaptured():
            return [None] * _PHASE_FIELD_COUNT
        case PhasesCaptured(
            enqueued_at=enqueued_at,
            claimed_at=claimed_at,
            claims=claims,
            finalizing=finalizing,
            recorded_by=recorded_by,
        ):
            pass
    match claims:
        case ClaimsNotCounted():
            claim_values: list[Any] = [None, None]
        case ClaimsCounted(count=count, first_claimed_at=first_claimed_at):
            claim_values = [count, _datetime_to_epoch_us(first_claimed_at)]
    match recorded_by:
        case RecordedByWorker(placement=Placed(at=placed_at)):
            recorder: list[Any] = [_datetime_to_epoch_us(placed_at), _RECORDED_BY_WORKER, None]
        case RecordedByWorker(placement=PlacementNotObserved()):
            recorder = [None, _RECORDED_BY_WORKER, None]
        case RecordedByRecovery(last_seen_at=last_seen_at):
            recorder = [None, _RECORDED_BY_RECOVERY, _datetime_to_epoch_us(last_seen_at)]
    placed, recorder_name, last_seen = recorder
    return [
        _PHASE_RECORD_VERSION,
        _datetime_to_epoch_us(enqueued_at),
        _datetime_to_epoch_us(claimed_at),
        *claim_values,
        placed,
        _finalizing_value(finalizing),
        recorder_name,
        last_seen,
    ]


def _open_claim_values(claims: ClaimsNotCounted | NotClaimed | ClaimsCounted) -> list[Any]:
    match claims:
        case ClaimsNotCounted():
            return [None, None]
        case NotClaimed():
            return [0, None]
        case ClaimsCounted(count=count, first_claimed_at=first_claimed_at):
            return [count, _datetime_to_epoch_us(first_claimed_at)]


def _finalizing_value(finalizing: FinalizingMarked | FinalizingNotMarked) -> int | None:
    match finalizing:
        case FinalizingMarked(at=at):
            return _datetime_to_epoch_us(at)
        case FinalizingNotMarked():
            return None


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
