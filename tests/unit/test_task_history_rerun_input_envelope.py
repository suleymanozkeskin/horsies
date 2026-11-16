"""The rerun input envelope: exact-text framing, fail-closed decode.

The move frames the stored input and options texts byte for byte, and the
decoder hands the same texts back, so a rerun that stores them gets the
same envelope and digest. The decoder verifies the digest before parsing,
refuses any other framing, and never reads an absent options member as
defaults. Stored bytes are strict JSON: a non-finite number never enters
them and never leaves them.

The prepared enqueue envelope writer, ``encode_input_envelope_v1``, keeps
its canonical-serialization pins.
"""

from __future__ import annotations

from hashlib import sha256

import pytest

from horsies.core.history.rerun.input_envelope import (
    InputEnvelopeCorrupt,
    InputEnvelopeVersionUnknown,
    ReconstructedInput,
    StoredTaskOptions,
    decode_input_envelope,
    encode_input_envelope_v1,
)
from tests.rerun_envelope_support import frame_rerun_envelope

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


class TestFraming:
    def test_the_mirror_frames_input_and_options_byte_for_byte(self) -> None:
        assert frame_rerun_envelope('{"k":"v"}', '{"o":1}') == (
            b'{"input":{"k":"v"},"options":{"o":1}}'
        )

    def test_absent_options_frame_as_null(self) -> None:
        assert frame_rerun_envelope('{}', None) == (
            b'{"input":{},"options":null}'
        )


class TestDecode:
    def test_roundtrip_returns_the_stored_texts(self) -> None:
        payload = frame_rerun_envelope('{"k":"v"}', '{"o":1}')
        decoded = roundtrip(payload)
        assert decoded == ReconstructedInput(
            input_json='{"k":"v"}',
            options=StoredTaskOptions(text='{"o":1}', value={'o': 1}),
        )

    def test_texts_come_back_exactly_as_stored(self) -> None:
        """Spacing and key order are part of the bytes the digest covers."""
        input_json = ' {"b": 2, "a": [1, 2]} '
        options_json = '{"timeout_ms": 5, "execution": "ISOLATED"}'
        decoded = roundtrip(frame_rerun_envelope(input_json, options_json))
        assert isinstance(decoded, ReconstructedInput)
        assert decoded.input_json == input_json
        assert decoded.options is not None
        assert decoded.options.text == options_json
        assert frame_rerun_envelope(
            decoded.input_json, decoded.options.text
        ) == frame_rerun_envelope(input_json, options_json)

    def test_a_member_name_inside_the_input_is_not_a_boundary(self) -> None:
        input_json = '{"s":",\\"options\\":","options":{}}'
        decoded = roundtrip(frame_rerun_envelope(input_json, None))
        assert decoded == ReconstructedInput(input_json=input_json, options=None)

    def test_non_ascii_stays_utf8(self) -> None:
        decoded = roundtrip(frame_rerun_envelope('{"k":"ü"}', None))
        assert decoded == ReconstructedInput(input_json='{"k":"ü"}', options=None)

    def test_null_options_means_defaults(self) -> None:
        decoded = roundtrip(frame_rerun_envelope('{}', None))
        assert isinstance(decoded, ReconstructedInput)
        assert decoded.options is None

    def test_absent_options_member_fails_closed(self) -> None:
        decoded = roundtrip(b'{"input":{}}')
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
        payload = frame_rerun_envelope('{}', None)
        decoded = decode_input_envelope(
            version=2, payload=payload, digest=sha256(payload).digest()
        )
        assert decoded == InputEnvelopeVersionUnknown(version=2)

    @pytest.mark.parametrize(
        ('payload', 'detail'),
        [
            (b'[]', 'not framed'),
            (b'{"options":null,"input":{}}', 'not framed'),
            (b'{"input":{},"options":null,"extra":1}', 'not framed'),
            (b'{"args":[],"kwargs":{},"options":null}', 'not framed'),
            (b'{ "input":{},"options":null}', 'not framed'),
            (b'{"input":[],"options":null}', 'input is not an object'),
            (b'{"input":"text","options":null}', 'input is not an object'),
            (b'{"input":{},"options":7}', 'neither an object nor null'),
            (b'{"input":{},"options":[]}', 'neither an object nor null'),
        ],
    )
    def test_foreign_shapes_fail_closed(
        self, payload: bytes, detail: str
    ) -> None:
        decoded = roundtrip(payload)
        assert isinstance(decoded, InputEnvelopeCorrupt)
        assert detail in decoded.detail

    @pytest.mark.parametrize(
        'payload',
        [
            b'{"input":{"x":Infinity},"options":null}',
            b'{"input":{"x":-Infinity},"options":null}',
            b'{"input":{},"options":{"x":NaN}}',
            b'{"input":{"x":1e9999},"options":null}',
            b'{"input":{"x":-1e9999},"options":null}',
            b'{"input":{},"options":{"x":[{"y":1e9999}]}}',
        ],
    )
    def test_non_finite_number_fails_closed(self, payload: bytes) -> None:
        decoded = roundtrip(payload)
        assert isinstance(decoded, InputEnvelopeCorrupt)
        assert 'payload is not JSON' in decoded.detail

    def test_lone_surrogate_fails_closed(self) -> None:
        payload = rb'{"input":{"x":"\ud800"},"options":null}'
        decoded = roundtrip(payload)
        assert isinstance(decoded, InputEnvelopeCorrupt)
        assert 'lone surrogate' in decoded.detail

    def test_nesting_past_the_depth_bound_fails_closed(self) -> None:
        payload = (
            b'{"input":{"x":' + b'[' * 1_100 + b']' * 1_100
            + b'},"options":null}'
        )
        decoded = roundtrip(payload)
        assert isinstance(decoded, InputEnvelopeCorrupt)
        assert 'nested deeper than' in decoded.detail

    def test_integer_over_the_parse_limit_fails_closed(self) -> None:
        payload = b'{"input":{"x":' + b'1' * 5000 + b'},"options":null}'
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
        decoded = roundtrip(b'')
        assert isinstance(decoded, InputEnvelopeCorrupt)
        assert 'payload is not JSON' in decoded.detail

    def test_largest_finite_float_comes_back_byte_identical(self) -> None:
        input_json = '{"x":1.7976931348623157e308}'
        decoded = roundtrip(frame_rerun_envelope(input_json, None))
        assert decoded == ReconstructedInput(input_json=input_json, options=None)


class TestNonFiniteValueIsADefect:
    """Callers hand over parsed, finite values; `inf` here is a code defect."""

    @pytest.mark.parametrize('value', [float('inf'), float('-inf'), float('nan')])
    def test_encode_crashes_loud(self, value: float) -> None:
        with pytest.raises(ValueError, match='not JSON compliant'):
            encode_input_envelope_v1(args=[], kwargs={'x': value}, options=None)
