"""Archive codecs: canonical encoding, exhaustive fail-closed decoding.

The attempt payload bytes are pinned exactly because two writers must
produce them — this module and the SQL terminalization encoder — and the
digest covers the bytes, so any divergence is a permanent integrity fault.
Decode tests enumerate every documented corruption, and the discriminant
tests enumerate the ratified value sets so a drive-by addition has to
justify itself here.
"""

from __future__ import annotations

import dataclasses
import json
from datetime import datetime, timezone
from typing import get_args

import pytest

from horsies.core.history.archive.attempts import (
    ATTEMPT_FIELD_COUNT,
    AttemptRecord,
    StoredAttemptSnapshot,
    decode_attempt_snapshot,
    encode_attempt_snapshot,
)
from horsies.core.history.archive.registry import (
    RETAINED_ARCHIVE_VERSIONS,
    is_retained,
    retained_versions,
)
from horsies.core.history.archive.results import (
    AdministrativePriorResult,
    CanonicalResultPayload,
    NoResultPayload,
    decode_result_envelope,
    encode_result_envelope,
    select_result_payload,
)
from horsies.core.history.archive.versions import (
    ArchiveDecodeFailure,
    ArchiveDigestMismatch,
    ArchiveDomain,
    CorruptArchiveValue,
    DecodedArchiveValue,
    UnknownArchiveCodec,
    UnknownArchiveContentType,
    UnknownArchiveVersion,
    archive_digest,
    decode_history_row_version,
)

pytestmark = [pytest.mark.unit]


UTC = timezone.utc


def epoch_us(value: datetime) -> int:
    return int(value.timestamp()) * 1_000_000 + value.microsecond


def make_attempt(number: int, total: int) -> AttemptRecord:
    base = datetime(2026, 8, 5, tzinfo=UTC)
    failed = number < total
    return AttemptRecord(
        attempt=number,
        outcome='FAILED' if failed else 'COMPLETED',
        will_retry=failed,
        started_at=base.replace(second=0, microsecond=number),
        finished_at=base.replace(second=1, microsecond=number),
        error_code='RETRYABLE' if failed else None,
        error_message='retry — später' if failed else None,
        failed_reason='worker failure' if failed else None,
        worker_id=f'worker-{number % 3}',
        worker_hostname='test-host',
        worker_pid=1000 + number,
        worker_process_name='test-process',
    )


def make_sequence(count: int) -> tuple[AttemptRecord, ...]:
    return tuple(make_attempt(number, count) for number in range(1, count + 1))


def decode_stored(
    stored: StoredAttemptSnapshot,
    **overrides: object,
) -> object:
    values: dict[str, object] = {
        'version': stored.version,
        'codec': stored.codec,
        'content_type': stored.content_type,
        'payload': stored.payload,
        'digest': stored.digest,
    }
    values.update(overrides)
    return decode_attempt_snapshot(**values)  # type: ignore[arg-type]


class TestAttemptRecordConstruction:
    def test_rejects_zero_attempt_number(self) -> None:
        with pytest.raises(ValueError, match='start at 1'):
            make_attempt(0, 1)

    def test_rejects_empty_outcome(self) -> None:
        good = make_attempt(1, 1)
        with pytest.raises(ValueError, match='non-empty'):
            dataclasses.replace(good, outcome='')

    def test_rejects_naive_timestamps(self) -> None:
        good = make_attempt(1, 1)
        with pytest.raises(ValueError, match='timezone-aware'):
            dataclasses.replace(
                good, started_at=good.started_at.replace(tzinfo=None)
            )


class TestAttemptEncoding:
    @pytest.mark.parametrize('count', [1, 4, 21])
    def test_roundtrip_preserves_the_sequence(self, count: int) -> None:
        attempts = make_sequence(count)
        stored = encode_attempt_snapshot(attempts)
        decoded = decode_stored(stored)
        assert decoded == DecodedArchiveValue(attempts)

    def test_canonical_bytes_are_pinned(self) -> None:
        """The exact wire bytes the SQL encoder must reproduce."""
        started = datetime(2026, 8, 5, tzinfo=UTC)
        finished = datetime(2026, 8, 5, 0, 0, 1, tzinfo=UTC)
        attempt = AttemptRecord(
            attempt=1,
            outcome='COMPLETED',
            will_retry=False,
            started_at=started,
            finished_at=finished,
            error_code=None,
            error_message='später',
            failed_reason=None,
            worker_id='w-1',
            worker_hostname='host',
            worker_pid=7,
            worker_process_name='proc',
        )
        stored = encode_attempt_snapshot((attempt,))
        expected = (
            f'[[1,"COMPLETED",false,{epoch_us(started)},{epoch_us(finished)},'
            f'null,"später",null,"w-1","host",7,"proc"]]'
        ).encode()
        assert stored.payload == expected
        assert stored.digest == archive_digest(expected)
        assert stored.version == 1
        assert stored.codec == 'json-utf8'
        assert stored.content_type == 'application/json'

    def test_rejects_non_contiguous_sequence(self) -> None:
        attempts = (make_attempt(1, 3), make_attempt(3, 3))
        with pytest.raises(ValueError, match='contiguous'):
            encode_attempt_snapshot(attempts)

    def test_rejects_sequence_not_starting_at_one(self) -> None:
        attempts = (make_attempt(2, 2),)
        with pytest.raises(ValueError, match='contiguous'):
            encode_attempt_snapshot(attempts)

    def test_empty_sequence_encodes_empty_array(self) -> None:
        stored = encode_attempt_snapshot(())
        assert stored.payload == b'[]'


class TestAttemptDecodeFailures:
    def test_unknown_version(self) -> None:
        stored = encode_attempt_snapshot(make_sequence(1))
        assert decode_stored(stored, version=2) == UnknownArchiveVersion(
            ArchiveDomain.ATTEMPTS, 2
        )

    def test_unknown_codec(self) -> None:
        stored = encode_attempt_snapshot(make_sequence(1))
        assert decode_stored(stored, codec='cbor') == UnknownArchiveCodec(
            ArchiveDomain.ATTEMPTS, 'cbor'
        )

    def test_unknown_content_type(self) -> None:
        stored = encode_attempt_snapshot(make_sequence(1))
        assert decode_stored(
            stored, content_type='text/plain'
        ) == UnknownArchiveContentType(ArchiveDomain.ATTEMPTS, 'text/plain')

    def test_digest_mismatch(self) -> None:
        stored = encode_attempt_snapshot(make_sequence(1))
        assert decode_stored(stored, digest=bytes(32)) == ArchiveDigestMismatch(
            ArchiveDomain.ATTEMPTS
        )

    def test_contract_is_judged_before_digest(self) -> None:
        stored = encode_attempt_snapshot(make_sequence(1))
        assert decode_stored(
            stored, version=9, digest=bytes(32)
        ) == UnknownArchiveVersion(ArchiveDomain.ATTEMPTS, 9)

    @pytest.mark.parametrize(
        ('payload', 'detail'),
        [
            (b'not json', 'JSONDecodeError'),
            (b'\xff\xfe', 'JSONDecodeError'),
            (b'{}', 'expected_array'),
            (b'[{}]', 'expected_positional_row'),
            (b'[[1,"OK",true]]', 'wrong_field_count'),
        ],
    )
    def test_malformed_payloads(self, payload: bytes, detail: str) -> None:
        result = decode_attempt_snapshot(
            version=1,
            codec='json-utf8',
            content_type='application/json',
            payload=payload,
            digest=archive_digest(payload),
        )
        assert result == CorruptArchiveValue(ArchiveDomain.ATTEMPTS, detail)

    def _mutated_payload(self, index: int, value: object) -> bytes:
        stored = encode_attempt_snapshot(make_sequence(1))
        rows = json.loads(stored.payload)
        rows[0][index] = value
        return json.dumps(rows, ensure_ascii=False, separators=(',', ':')).encode()

    @pytest.mark.parametrize(
        ('index', 'value', 'detail'),
        [
            (0, 0, 'invalid_attempt_number'),
            (0, True, 'invalid_attempt_number'),
            (1, '', 'invalid_outcome'),
            (1, 3, 'invalid_outcome'),
            (2, 1, 'invalid_will_retry'),
            (3, True, 'invalid_started_at'),
            (3, 1.5, 'invalid_started_at'),
            (3, '2026-08-05T00:00:00Z', 'invalid_started_at'),
            (3, 10**30, 'invalid_started_at'),
            (4, None, 'invalid_finished_at'),
            (5, 7, 'invalid_text_field_5'),
            (10, True, 'invalid_worker_pid'),
            (11, 4, 'invalid_worker_process_name'),
        ],
    )
    def test_per_field_type_enforcement(
        self, index: int, value: object, detail: str
    ) -> None:
        payload = self._mutated_payload(index, value)
        result = decode_attempt_snapshot(
            version=1,
            codec='json-utf8',
            content_type='application/json',
            payload=payload,
            digest=archive_digest(payload),
        )
        assert result == CorruptArchiveValue(ArchiveDomain.ATTEMPTS, detail)

    def test_non_contiguous_decoded_sequence(self) -> None:
        stored = encode_attempt_snapshot(make_sequence(2))
        rows = json.loads(stored.payload)
        rows[1][0] = 3
        payload = json.dumps(
            rows, ensure_ascii=False, separators=(',', ':')
        ).encode()
        result = decode_attempt_snapshot(
            version=1,
            codec='json-utf8',
            content_type='application/json',
            payload=payload,
            digest=archive_digest(payload),
        )
        assert result == CorruptArchiveValue(
            ArchiveDomain.ATTEMPTS, 'non_contiguous_attempts'
        )

    def test_field_count_is_the_version_1_constant(self) -> None:
        assert ATTEMPT_FIELD_COUNT == 12


class TestResultEnvelope:
    def test_roundtrip(self) -> None:
        stored = encode_result_envelope('{"answer":42,"text":"später"}')
        decoded = decode_result_envelope(
            version=stored.version,
            codec=stored.codec,
            content_type=stored.content_type,
            payload=stored.payload,
            digest=stored.digest,
        )
        assert decoded == DecodedArchiveValue({'answer': 42, 'text': 'später'})

    def test_encode_stores_the_exact_caller_bytes(self) -> None:
        result_json = '{"b": 1, "a": 2}'
        stored = encode_result_envelope(result_json)
        assert stored.payload == result_json.encode()

    def test_encode_rejects_malformed_json(self) -> None:
        with pytest.raises(ValueError, match='not well-formed JSON'):
            encode_result_envelope('{"unterminated": ')

    def test_digest_mismatch(self) -> None:
        stored = encode_result_envelope('{}')
        assert decode_result_envelope(
            version=stored.version,
            codec=stored.codec,
            content_type=stored.content_type,
            payload=stored.payload,
            digest=bytes(32),
        ) == ArchiveDigestMismatch(ArchiveDomain.RESULT)

    def test_corrupt_payload_with_matching_digest(self) -> None:
        payload = b'not json'
        result = decode_result_envelope(
            version=1,
            codec='json-utf8',
            content_type='application/json',
            payload=payload,
            digest=archive_digest(payload),
        )
        assert result == CorruptArchiveValue(
            ArchiveDomain.RESULT, 'JSONDecodeError'
        )

    def test_unknown_discriminants(self) -> None:
        stored = encode_result_envelope('{}')
        assert decode_result_envelope(
            version=5,
            codec=stored.codec,
            content_type=stored.content_type,
            payload=stored.payload,
            digest=stored.digest,
        ) == UnknownArchiveVersion(ArchiveDomain.RESULT, 5)
        assert decode_result_envelope(
            version=1,
            codec='pickle',
            content_type=stored.content_type,
            payload=stored.payload,
            digest=stored.digest,
        ) == UnknownArchiveCodec(ArchiveDomain.RESULT, 'pickle')


class TestResultPayloadSelection:
    def test_canonical_only(self) -> None:
        assert select_result_payload(
            canonical=b'{}', prior=None
        ) == CanonicalResultPayload(payload=b'{}')

    def test_prior_only(self) -> None:
        assert select_result_payload(
            canonical=None, prior=b'{}'
        ) == AdministrativePriorResult(payload=b'{}')

    def test_neither(self) -> None:
        assert select_result_payload(canonical=None, prior=None) == NoResultPayload()

    def test_both_is_corruption(self) -> None:
        assert select_result_payload(
            canonical=b'{}', prior=b'{}'
        ) == CorruptArchiveValue(
            ArchiveDomain.RESULT, 'canonical_and_prior_both_present'
        )


class TestDiscriminantSets:
    """The ratified value sets, pinned."""

    def test_decode_failure_union_membership(self) -> None:
        assert set(get_args(ArchiveDecodeFailure.__value__)) == {
            UnknownArchiveVersion,
            UnknownArchiveCodec,
            UnknownArchiveContentType,
            CorruptArchiveValue,
            ArchiveDigestMismatch,
        }


class TestRegistry:
    def test_every_domain_retains_exactly_version_one(self) -> None:
        assert set(RETAINED_ARCHIVE_VERSIONS) == set(ArchiveDomain)
        for domain in ArchiveDomain:
            assert retained_versions(domain) == frozenset({1})

    def test_is_retained(self) -> None:
        assert is_retained(ArchiveDomain.ATTEMPTS, 1)
        assert not is_retained(ArchiveDomain.ATTEMPTS, 2)

    def test_history_row_version_decode_matches_registry(self) -> None:
        assert decode_history_row_version(1) == DecodedArchiveValue(1)
        assert decode_history_row_version(2) == UnknownArchiveVersion(
            ArchiveDomain.HISTORY_ROW, 2
        )
