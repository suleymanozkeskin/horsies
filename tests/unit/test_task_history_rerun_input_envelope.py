"""Input-envelope content v1: canonical bytes, fail-closed decode.

The digest covers bytes, so the serializer is pinned canonical — equal
content in any construction order produces identical bytes — and the
decoder verifies the digest before parsing, refuses foreign shapes, and
never reads an absent options key as defaults.
"""

from __future__ import annotations

from hashlib import sha256

import pytest

from horsies.core.history.rerun.input_envelope import (
    InputEnvelopeCorrupt,
    InputEnvelopeVersionUnknown,
    ReconstructedInput,
    decode_input_envelope,
    encode_input_envelope_v1,
)

pytestmark = [pytest.mark.unit]


def roundtrip(payload: bytes) -> object:
    return decode_input_envelope(
        version=1, payload=payload, digest=sha256(payload).digest()
    )


class TestCanonicalSerialization:
    def test_equal_content_any_order_produces_identical_bytes(self) -> None:
        first = encode_input_envelope_v1(
            args=[1, 'x'],
            kwargs={'b': 2, 'a': 1},
            options={'timeout_ms': 5, 'queue': 'q'},
        )
        second = encode_input_envelope_v1(
            args=(1, 'x'),
            kwargs={'a': 1, 'b': 2},
            options={'queue': 'q', 'timeout_ms': 5},
        )
        assert first == second

    def test_bytes_are_compact_and_key_sorted(self) -> None:
        payload = encode_input_envelope_v1(
            args=[], kwargs={'z': 1, 'a': 2}, options=None
        )
        assert payload == b'{"args":[],"kwargs":{"a":2,"z":1},"options":null}'

    def test_non_ascii_stays_utf8(self) -> None:
        payload = encode_input_envelope_v1(
            args=['ü'], kwargs={}, options=None
        )
        assert 'ü'.encode('utf-8') in payload


class TestDecode:
    def test_roundtrip(self) -> None:
        payload = encode_input_envelope_v1(
            args=[1], kwargs={'k': 'v'}, options={'o': 1}
        )
        decoded = roundtrip(payload)
        assert decoded == ReconstructedInput(
            args=(1,), kwargs={'k': 'v'}, options={'o': 1}
        )

    def test_null_options_means_defaults(self) -> None:
        payload = encode_input_envelope_v1(args=[], kwargs={}, options=None)
        decoded = roundtrip(payload)
        assert isinstance(decoded, ReconstructedInput)
        assert decoded.options is None

    def test_absent_options_key_fails_closed(self) -> None:
        payload = b'{"args":[],"kwargs":{}}'
        decoded = roundtrip(payload)
        assert isinstance(decoded, InputEnvelopeCorrupt)
        assert 'options' in decoded.detail

    def test_digest_verifies_before_parse(self) -> None:
        # Unparseable bytes with a WRONG digest must report the digest,
        # proving order: integrity first, syntax second.
        decoded = decode_input_envelope(
            version=1, payload=b'not json', digest=b'\x00' * 32
        )
        assert isinstance(decoded, InputEnvelopeCorrupt)
        assert 'digest' in decoded.detail

    def test_unknown_version_is_typed(self) -> None:
        payload = encode_input_envelope_v1(args=[], kwargs={}, options=None)
        decoded = decode_input_envelope(
            version=2, payload=payload, digest=sha256(payload).digest()
        )
        assert decoded == InputEnvelopeVersionUnknown(version=2)

    @pytest.mark.parametrize(
        'payload',
        [
            b'[]',
            b'{"args":{},"kwargs":{},"options":null}',
            b'{"args":[],"kwargs":[],"options":null}',
            b'{"args":[],"kwargs":{},"options":7}',
            b'{"args":[],"kwargs":{},"options":null,"extra":1}',
        ],
    )
    def test_foreign_shapes_fail_closed(self, payload: bytes) -> None:
        assert isinstance(roundtrip(payload), InputEnvelopeCorrupt)