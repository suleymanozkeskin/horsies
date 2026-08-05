"""Versioned values used by the task-history storage prototypes."""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from enum import StrEnum
from typing import Any

from pydantic import TypeAdapter, ValidationError


ARCHIVE_CODEC = 'json-utf8'
ARCHIVE_VERSION = 1
ARCHIVE_CODEC_V2 = 'framed-json-v2'
ARCHIVE_FRAME_V2 = b'H2'


class ArchiveDomain(StrEnum):
    HISTORY_ROW = 'history_row'
    RESULT = 'result'
    ATTEMPTS = 'attempts'
    RERUN_INPUT = 'rerun_input'


@dataclass(frozen=True, slots=True)
class UnknownArchiveVersion:
    domain: ArchiveDomain
    version: int


@dataclass(frozen=True, slots=True)
class UnknownArchiveCodec:
    domain: ArchiveDomain
    codec: str


@dataclass(frozen=True, slots=True)
class CorruptArchiveValue:
    domain: ArchiveDomain
    detail: str


@dataclass(frozen=True, slots=True)
class ArchiveDigestMismatch:
    domain: ArchiveDomain


type ArchiveDecodeFailure = (
    UnknownArchiveVersion
    | UnknownArchiveCodec
    | CorruptArchiveValue
    | ArchiveDigestMismatch
)


@dataclass(frozen=True, slots=True)
class DecodedArchiveValue[T]:
    value: T


type ArchiveDecodeResult[T] = DecodedArchiveValue[T] | ArchiveDecodeFailure


def decode_history_row_version(version: int) -> ArchiveDecodeResult[int]:
    match version:
        case 1 | 2:
            return DecodedArchiveValue(version)
        case _:
            return UnknownArchiveVersion(ArchiveDomain.HISTORY_ROW, version)


@dataclass(frozen=True, slots=True)
class AttemptRecord:
    attempt: int
    outcome: str
    will_retry: bool
    started_at: str
    finished_at: str
    error_code: str | None
    error_message: str | None
    failed_reason: str | None
    worker_id: str | None
    worker_hostname: str | None
    worker_pid: int | None
    worker_process_name: str | None


def prototype_attempts(count: int) -> tuple[AttemptRecord, ...]:
    """Build a contiguous attempt sequence for storage prototype fixtures."""
    if count < 1:
        raise ValueError('count must be positive')
    base = datetime(2026, 8, 5, tzinfo=timezone.utc)
    return tuple(
        AttemptRecord(
            attempt=number,
            outcome='FAILED' if number < count else 'COMPLETED',
            will_retry=number < count,
            started_at=(base + timedelta(seconds=number * 2)).isoformat(),
            finished_at=(base + timedelta(seconds=number * 2 + 1)).isoformat(),
            error_code='RETRYABLE' if number < count else None,
            error_message='retry' if number < count else None,
            failed_reason='worker failure' if number < count else None,
            worker_id=f'worker-{number % 3}',
            worker_hostname='test-host',
            worker_pid=1000 + number,
            worker_process_name='test-process',
        )
        for number in range(1, count + 1)
    )


_ATTEMPTS_ADAPTER = TypeAdapter(tuple[AttemptRecord, ...])


@dataclass(frozen=True, slots=True)
class StoredArchiveValue:
    version: int
    codec: str
    payload: bytes
    digest: bytes


class RerunInputForm(StrEnum):
    INLINE = 'INLINE'
    REFERENCE = 'REFERENCE'


@dataclass(frozen=True, slots=True)
class InlineRerunInput:
    payload: bytes
    digest: bytes


@dataclass(frozen=True, slots=True)
class ReferencedRerunInput:
    reference: str
    digest: bytes


type RerunInput = InlineRerunInput | ReferencedRerunInput


@dataclass(frozen=True, slots=True)
class StoredRerunInput:
    version: int
    codec: str
    form: RerunInputForm
    digest: bytes
    inline_payload: bytes | None
    reference: str | None


def archive_digest(payload: bytes) -> bytes:
    return hashlib.sha256(payload).digest()


def encode_json_value(value: Any) -> StoredArchiveValue:
    payload = json.dumps(
        value,
        ensure_ascii=False,
        separators=(',', ':'),
        sort_keys=True,
    ).encode()
    return StoredArchiveValue(
        version=ARCHIVE_VERSION,
        codec=ARCHIVE_CODEC,
        payload=payload,
        digest=archive_digest(payload),
    )


def decode_json_value(
    *,
    domain: ArchiveDomain,
    version: int,
    codec: str,
    payload: bytes,
    digest: bytes,
) -> ArchiveDecodeResult[Any]:
    decoded_payload = _decode_payload_contract(
        domain=domain,
        version=version,
        codec=codec,
        payload=payload,
        digest=digest,
    )
    match decoded_payload:
        case DecodedArchiveValue(value=unframed):
            try:
                return DecodedArchiveValue(json.loads(unframed))
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                return CorruptArchiveValue(
                    domain=domain,
                    detail=type(exc).__name__,
                )
        case failure:
            return failure


def encode_attempts(attempts: tuple[AttemptRecord, ...]) -> StoredArchiveValue:
    expected = tuple(range(1, len(attempts) + 1))
    actual = tuple(attempt.attempt for attempt in attempts)
    if actual != expected:
        raise ValueError('attempts must be ordered and contiguous from 1')
    return encode_json_value([asdict(attempt) for attempt in attempts])


def decode_attempts(
    *,
    version: int,
    codec: str,
    payload: bytes,
    digest: bytes,
) -> ArchiveDecodeResult[tuple[AttemptRecord, ...]]:
    decoded = decode_json_value(
        domain=ArchiveDomain.ATTEMPTS,
        version=version,
        codec=codec,
        payload=payload,
        digest=digest,
    )
    match decoded:
        case DecodedArchiveValue(value=value) if isinstance(value, list):
            try:
                attempts = _ATTEMPTS_ADAPTER.validate_python(value)
            except ValidationError as exc:
                return CorruptArchiveValue(
                    domain=ArchiveDomain.ATTEMPTS,
                    detail=type(exc).__name__,
                )
            expected = tuple(range(1, len(attempts) + 1))
            actual = tuple(attempt.attempt for attempt in attempts)
            if actual != expected:
                return CorruptArchiveValue(
                    domain=ArchiveDomain.ATTEMPTS,
                    detail='non_contiguous_attempts',
                )
            return DecodedArchiveValue(attempts)
        case DecodedArchiveValue():
            return CorruptArchiveValue(
                domain=ArchiveDomain.ATTEMPTS,
                detail='expected_array',
            )
        case failure:
            return failure


def store_inline_rerun_input(payload: bytes) -> StoredRerunInput:
    return StoredRerunInput(
        version=ARCHIVE_VERSION,
        codec=ARCHIVE_CODEC,
        form=RerunInputForm.INLINE,
        digest=archive_digest(payload),
        inline_payload=payload,
        reference=None,
    )


def store_referenced_rerun_input(*, reference: str, payload: bytes) -> StoredRerunInput:
    if not reference:
        raise ValueError('reference must be non-empty')
    return StoredRerunInput(
        version=ARCHIVE_VERSION,
        codec=ARCHIVE_CODEC,
        form=RerunInputForm.REFERENCE,
        digest=archive_digest(payload),
        inline_payload=None,
        reference=reference,
    )


def decode_rerun_input(
    *,
    version: int,
    codec: str,
    form: str,
    digest: bytes,
    inline_payload: bytes | None,
    reference: str | None,
) -> ArchiveDecodeResult[RerunInput]:
    contract_error = _validate_version_and_codec(
        domain=ArchiveDomain.RERUN_INPUT,
        version=version,
        codec=codec,
    )
    if contract_error is not None:
        return contract_error
    match form, inline_payload, reference:
        case RerunInputForm.INLINE, bytes() as payload, None:
            decoded_payload = _decode_payload_contract(
                domain=ArchiveDomain.RERUN_INPUT,
                version=version,
                codec=codec,
                payload=payload,
                digest=digest,
            )
            match decoded_payload:
                case DecodedArchiveValue(value=unframed):
                    return DecodedArchiveValue(
                        InlineRerunInput(payload=unframed, digest=digest)
                    )
                case failure:
                    return failure
        case (
            RerunInputForm.REFERENCE,
            None,
            str() as object_reference,
        ) if object_reference:
            if len(digest) != 32:
                return CorruptArchiveValue(
                    domain=ArchiveDomain.RERUN_INPUT,
                    detail='invalid_reference_digest',
                )
            return DecodedArchiveValue(
                ReferencedRerunInput(reference=object_reference, digest=digest)
            )
        case _:
            return CorruptArchiveValue(
                domain=ArchiveDomain.RERUN_INPUT,
                detail='invalid_discriminated_value',
            )


def _decode_payload_contract(
    *,
    domain: ArchiveDomain,
    version: int,
    codec: str,
    payload: bytes,
    digest: bytes,
) -> ArchiveDecodeResult[bytes]:
    contract_error = _validate_version_and_codec(
        domain=domain,
        version=version,
        codec=codec,
    )
    if contract_error is not None:
        return contract_error
    if archive_digest(payload) != digest:
        return ArchiveDigestMismatch(domain=domain)
    match version:
        case 1:
            return DecodedArchiveValue(payload)
        case 2 if payload.startswith(ARCHIVE_FRAME_V2):
            return DecodedArchiveValue(payload[len(ARCHIVE_FRAME_V2) :])
        case 2:
            return CorruptArchiveValue(
                domain=domain,
                detail='invalid_frame',
            )
        case _:
            raise AssertionError('version validation was not exhaustive')


def _validate_version_and_codec(
    *,
    domain: ArchiveDomain,
    version: int,
    codec: str,
) -> ArchiveDecodeFailure | None:
    match version:
        case 1 if codec == ARCHIVE_CODEC:
            return None
        case 2 if codec == ARCHIVE_CODEC_V2:
            return None
        case 1 | 2:
            return UnknownArchiveCodec(domain=domain, codec=codec)
        case _:
            return UnknownArchiveVersion(domain=domain, version=version)
