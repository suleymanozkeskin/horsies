"""Input-envelope content v1: canonical bytes, fail-closed decode.

The digest covers bytes, so the serializer is pinned canonical — equal
content in any construction order produces identical bytes — and the
decoder verifies the digest before parsing, refuses foreign shapes, and
never reads an absent options key as defaults. Stored bytes are strict
JSON: a non-finite number never enters them and never leaves them.
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
from horsies.core.history.rerun.operations import (
    _canonical_json,  # pyright: ignore[reportPrivateUsage]
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

    @pytest.mark.parametrize(
        'payload',
        [
            b'{"args":[Infinity],"kwargs":{},"options":null}',
            b'{"args":[],"kwargs":{"x":-Infinity},"options":null}',
            b'{"args":[],"kwargs":{},"options":{"x":NaN}}',
            b'{"args":[1e9999],"kwargs":{},"options":null}',
            b'{"args":[],"kwargs":{"x":-1e9999},"options":null}',
            b'{"args":[],"kwargs":{},"options":{"x":[{"y":1e9999}]}}',
        ],
    )
    def test_non_finite_number_fails_closed(self, payload: bytes) -> None:
        decoded = roundtrip(payload)
        assert isinstance(decoded, InputEnvelopeCorrupt)
        assert 'payload is not JSON' in decoded.detail

    def test_lone_surrogate_fails_closed(self) -> None:
        payload = rb'{"args":[],"kwargs":{"x":"\ud800"},"options":null}'
        decoded = roundtrip(payload)
        assert isinstance(decoded, InputEnvelopeCorrupt)
        assert 'lone surrogate' in decoded.detail

    def test_integer_over_the_parse_limit_fails_closed(self) -> None:
        payload = b'{"args":[' + b'1' * 5000 + b'],"kwargs":{},"options":null}'
        decoded = roundtrip(payload)
        assert isinstance(decoded, InputEnvelopeCorrupt)
        assert 'payload is not JSON' in decoded.detail

    @pytest.mark.parametrize('payload', [b'not json', b'\xff'])
    def test_malformed_bytes_with_a_matching_digest_fail_closed(
        self, payload: bytes
    ) -> None:
        decoded = roundtrip(payload)
        assert isinstance(decoded, InputEnvelopeCorrupt)
        assert 'payload is not JSON' in decoded.detail

    def test_empty_payload_with_a_matching_digest_fails_closed(self) -> None:
        assert isinstance(roundtrip(b''), InputEnvelopeCorrupt)

    def test_largest_finite_float_roundtrips_byte_identical(self) -> None:
        payload = encode_input_envelope_v1(
            args=[1.7976931348623157e308], kwargs={}, options=None
        )
        decoded = roundtrip(payload)
        assert isinstance(decoded, ReconstructedInput)
        assert (
            encode_input_envelope_v1(
                args=decoded.args,
                kwargs=decoded.kwargs,
                options=decoded.options,
            )
            == payload
        )


class TestNonFiniteValueIsADefect:
    """Callers hand over parsed, finite values; `inf` here is a code defect."""

    @pytest.mark.parametrize('value', [float('inf'), float('-inf'), float('nan')])
    def test_encode_crashes_loud(self, value: float) -> None:
        with pytest.raises(ValueError, match='not JSON compliant'):
            encode_input_envelope_v1(args=[], kwargs={'x': value}, options=None)

    @pytest.mark.parametrize('value', [float('inf'), float('-inf'), float('nan')])
    def test_canonical_json_crashes_loud(self, value: float) -> None:
        with pytest.raises(ValueError, match='not JSON compliant'):
            _canonical_json({'x': value})
