"""Fail-closed contracts for task-history archive codec candidates."""

from __future__ import annotations

import hashlib
import json
from typing import Any, cast

import pytest

from tests.task_history_prototypes.archive import (
    ARCHIVE_CODEC,
    ARCHIVE_CODEC_V2,
    ARCHIVE_FRAME_V2,
    ARCHIVE_VERSION,
    ArchiveDigestMismatch,
    ArchiveDomain,
    AttemptRecord,
    CorruptArchiveValue,
    DecodedArchiveValue,
    InlineRerunInput,
    UnknownArchiveCodec,
    UnknownArchiveVersion,
    archive_digest,
    decode_attempts,
    decode_history_row_version,
    decode_json_value,
    decode_rerun_input,
    encode_attempts,
    encode_json_value,
    store_inline_rerun_input,
)
from tests.task_history_prototypes.schema import PrototypeSchema


def _attempt(number: int) -> AttemptRecord:
    return AttemptRecord(
        attempt=number,
        outcome='COMPLETED',
        will_retry=False,
        started_at='2026-08-05T00:00:00+00:00',
        finished_at='2026-08-05T00:00:01+00:00',
        error_code=None,
        error_message=None,
        failed_reason=None,
        worker_id='worker',
        worker_hostname='host',
        worker_pid=1,
        worker_process_name='process',
    )


def test_result_decoder_accepts_current_contract() -> None:
    stored = encode_json_value({'ok': {'value': 42}})
    assert decode_json_value(
        domain=ArchiveDomain.RESULT,
        version=stored.version,
        codec=stored.codec,
        payload=stored.payload,
        digest=stored.digest,
    ) == DecodedArchiveValue({'ok': {'value': 42}})


def test_result_decoder_accepts_retained_version_two_contract() -> None:
    stored = encode_json_value({'ok': {'value': 42}})
    payload = ARCHIVE_FRAME_V2 + stored.payload
    assert decode_json_value(
        domain=ArchiveDomain.RESULT,
        version=2,
        codec=ARCHIVE_CODEC_V2,
        payload=payload,
        digest=hashlib.sha256(payload).digest(),
    ) == DecodedArchiveValue({'ok': {'value': 42}})


def test_history_row_decoder_tracks_its_version_independently() -> None:
    assert decode_history_row_version(1) == DecodedArchiveValue(1)
    assert decode_history_row_version(2) == DecodedArchiveValue(2)
    assert decode_history_row_version(99) == UnknownArchiveVersion(
        ArchiveDomain.HISTORY_ROW,
        99,
    )


@pytest.mark.parametrize(
    ('version', 'codec', 'digest', 'expected_type'),
    [
        (99, ARCHIVE_CODEC, None, UnknownArchiveVersion),
        (ARCHIVE_VERSION, 'unknown', None, UnknownArchiveCodec),
        (ARCHIVE_VERSION, ARCHIVE_CODEC, b'0' * 32, ArchiveDigestMismatch),
    ],
)
def test_result_decoder_rejects_unknown_or_mismatched_contract(
    version: int,
    codec: str,
    digest: bytes | None,
    expected_type: type[object],
) -> None:
    stored = encode_json_value({'ok': True})
    decoded = decode_json_value(
        domain=ArchiveDomain.RESULT,
        version=version,
        codec=codec,
        payload=stored.payload,
        digest=digest if digest is not None else stored.digest,
    )
    assert isinstance(decoded, expected_type)


def test_result_decoder_rejects_corrupt_json() -> None:
    payload = b'not-json'
    decoded = decode_json_value(
        domain=ArchiveDomain.RESULT,
        version=ARCHIVE_VERSION,
        codec=ARCHIVE_CODEC,
        payload=payload,
        digest=hashlib.sha256(payload).digest(),
    )
    assert isinstance(decoded, CorruptArchiveValue)


def test_attempt_decoder_rejects_non_contiguous_sequence() -> None:
    with pytest.raises(ValueError, match='ordered and contiguous'):
        encode_attempts((_attempt(2),))

    stored = encode_json_value([{'attempt': 2}])
    decoded = decode_attempts(
        version=stored.version,
        codec=stored.codec,
        payload=stored.payload,
        digest=stored.digest,
    )
    assert isinstance(decoded, CorruptArchiveValue)


def test_attempt_snapshot_uses_typed_positional_rows() -> None:
    attempts = (_attempt(1), _attempt(2))
    stored = encode_attempts(attempts)

    encoded = cast(list[list[Any]], json.loads(stored.payload))
    assert all(isinstance(item, list) and len(item) == 12 for item in encoded)
    assert b'"attempt"' not in stored.payload
    assert decode_attempts(
        version=stored.version,
        codec=stored.codec,
        payload=stored.payload,
        digest=stored.digest,
    ) == DecodedArchiveValue(attempts)


def test_attempt_decoder_rejects_fractional_epoch_microseconds() -> None:
    stored = encode_attempts((_attempt(1),))
    positional = cast(list[list[Any]], json.loads(stored.payload))
    positional[0][3] = 1.5
    malformed = json.dumps(positional, separators=(',', ':')).encode()

    decoded = decode_attempts(
        version=stored.version,
        codec=stored.codec,
        payload=malformed,
        digest=archive_digest(malformed),
    )
    assert isinstance(decoded, CorruptArchiveValue)


def test_rerun_input_decoder_rejects_invalid_discriminant_and_digest() -> None:
    invalid = decode_rerun_input(
        version=ARCHIVE_VERSION,
        codec=ARCHIVE_CODEC,
        form='INLINE',
        digest=b'0' * 32,
        inline_payload=None,
        reference=None,
    )
    assert isinstance(invalid, CorruptArchiveValue)

    stored = store_inline_rerun_input(b'payload')
    mismatched = decode_rerun_input(
        version=stored.version,
        codec=stored.codec,
        form=stored.form,
        digest=b'0' * 32,
        inline_payload=stored.inline_payload,
        reference=stored.reference,
    )
    assert isinstance(mismatched, ArchiveDigestMismatch)

    decoded = decode_rerun_input(
        version=stored.version,
        codec=stored.codec,
        form=stored.form,
        digest=stored.digest,
        inline_payload=stored.inline_payload,
        reference=stored.reference,
    )
    assert isinstance(decoded, DecodedArchiveValue)
    assert isinstance(decoded.value, InlineRerunInput)
    assert decoded.value.payload == b'payload'


@pytest.mark.parametrize(
    'name',
    ['unsafe-name', 'UPPERCASE', '1starts_with_digit', 'a' * 64],
)
def test_prototype_schema_rejects_unsafe_identifiers(name: str) -> None:
    with pytest.raises(ValueError, match='safe PostgreSQL identifier'):
        PrototypeSchema(name)
