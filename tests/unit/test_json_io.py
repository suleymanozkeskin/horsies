"""Tests for the strict JSON I/O boundary (codec/json_io.py).

dumps_json is strict: it rejects non-JSON-native values (tuple, set, non-str
dict keys, lone surrogates, non-finite floats) rather than coercing them, and
emits no class tags. loads_json rejects non-RFC-8259 constants (NaN/Infinity)
numeric literals outside the finite binary64 range, and lone surrogates.
"""

from __future__ import annotations

import pytest

from horsies.core.codec.json_io import dumps_json, loads_json
from horsies.core.types.result import is_err


@pytest.mark.unit
class TestDumpsJsonStrictMapping:
    """Non-string mapping keys are rejected, not str()-coerced."""

    def test_int_key_rejected(self) -> None:
        result = dumps_json({1: 'a'})
        assert is_err(result)
        assert 'key must be str' in str(result.err_value).lower()

    def test_mixed_int_and_str_keys_rejected(self) -> None:
        result = dumps_json({1: 'a', '1': 'b'})
        assert is_err(result)
        assert 'key must be str' in str(result.err_value).lower()

    def test_tuple_key_rejected(self) -> None:
        result = dumps_json({(1, 2): 'a'})
        assert is_err(result)
        assert 'key must be str' in str(result.err_value).lower()

    def test_nested_non_str_key_rejected(self) -> None:
        result = dumps_json({'outer': {1: 'x'}})
        assert is_err(result)
        assert 'key must be str' in str(result.err_value).lower()

    def test_string_keys_ok(self) -> None:
        result = dumps_json({'a': 1, 'b': 2})
        assert not is_err(result)
        assert result.ok_value == '{"a":1,"b":2}'

    def test_empty_mapping_ok(self) -> None:
        result = dumps_json({})
        assert not is_err(result)
        assert result.ok_value == '{}'


@pytest.mark.unit
class TestDumpsJsonStrictSequence:
    """Only lists become JSON arrays; tuple/set are rejected, not coerced."""

    def test_tuple_rejected(self) -> None:
        result = dumps_json((1, 2))
        assert is_err(result)
        assert 'tuple' in str(result.err_value).lower()

    def test_empty_tuple_rejected(self) -> None:
        result = dumps_json(())
        assert is_err(result)
        assert 'tuple' in str(result.err_value).lower()

    def test_set_rejected(self) -> None:
        result = dumps_json({1, 2, 3})
        assert is_err(result)

    def test_nested_tuple_in_list_rejected(self) -> None:
        result = dumps_json([1, (2, 3)])
        assert is_err(result)
        assert 'tuple' in str(result.err_value).lower()

    def test_list_ok(self) -> None:
        result = dumps_json([1, 2, 3])
        assert not is_err(result)
        assert result.ok_value == '[1,2,3]'


@pytest.mark.unit
class TestDumpsJsonPrimitives:
    """Primitives and nesting serialize; non-finite floats are rejected."""

    def test_primitives(self) -> None:
        assert dumps_json(None).ok_value == 'null'
        assert dumps_json(True).ok_value == 'true'
        assert dumps_json(42).ok_value == '42'
        assert dumps_json('hi').ok_value == '"hi"'

    def test_non_finite_float_rejected(self) -> None:
        assert is_err(dumps_json(float('nan')))
        assert is_err(dumps_json(float('inf')))


@pytest.mark.unit
class TestDumpsJsonSurrogate:
    """dumps_json fails closed on text that is not UTF-8 encodable.

    ensure_ascii=False lets json.dumps emit lone UTF-16 surrogates verbatim;
    those would otherwise pass as Ok and crash far downstream in the Postgres
    TEXT insert.
    """

    def test_lone_surrogate_in_value_rejected(self) -> None:
        result = dumps_json({'s': '\ud800test'})
        assert is_err(result)
        assert 'utf-8' in str(result.err_value).lower()

    def test_lone_surrogate_in_key_rejected(self) -> None:
        result = dumps_json({'\udfff': 'v'})
        assert is_err(result)
        assert 'utf-8' in str(result.err_value).lower()

    def test_lone_surrogate_in_list_rejected(self) -> None:
        result = dumps_json(['ok', '\ud83d'])  # lone half of a surrogate pair
        assert is_err(result)
        assert 'utf-8' in str(result.err_value).lower()

    def test_valid_unicode_round_trips(self) -> None:
        result = dumps_json({'s': 'héllo \U0001F600 日本語'})
        assert not is_err(result)
        assert result.ok_value.encode('utf-8')  # encodable, no raise


@pytest.mark.unit
class TestLoadsJson:
    """loads_json basics and non-RFC-8259 constant rejection."""

    def test_empty_returns_none(self) -> None:
        assert loads_json(None).ok_value is None
        assert loads_json('').ok_value is None

    def test_round_trip(self) -> None:
        result = loads_json('{"a":[1,2],"b":"x"}')
        assert not is_err(result)
        assert result.ok_value == {'a': [1, 2], 'b': 'x'}

    def test_nan_rejected(self) -> None:
        assert is_err(loads_json('NaN'))

    def test_infinity_rejected(self) -> None:
        assert is_err(loads_json('Infinity'))

    def test_malformed_rejected(self) -> None:
        assert is_err(loads_json('{bad'))

    @pytest.mark.parametrize(
        'document',
        [
            '[1e9999]',
            '[-1e9999]',
            '[1E400]',
            '{"a":[{"b":1.8e308}]}',
            '[' + '1' * 400 + '.0]',
        ],
    )
    def test_number_outside_finite_binary64_rejected(self, document: str) -> None:
        result = loads_json(document)
        assert is_err(result)
        assert 'finite binary64' in str(result.err_value)

    def test_overflow_rejection_text_holds_no_input_value(self) -> None:
        result = loads_json('[1234e9999]')
        assert is_err(result)
        assert '1234e9999' not in str(result.err_value)

    @pytest.mark.parametrize(
        ('document', 'expected'),
        [
            ('[1.5]', 1.5),
            ('[1.7976931348623157e308]', 1.7976931348623157e308),
            ('[-1.7976931348623157e308]', -1.7976931348623157e308),
            ('[5e-324]', 5e-324),
            ('[1e-9999]', 0.0),
        ],
    )
    def test_finite_float_keeps_its_value(
        self, document: str, expected: float,
    ) -> None:
        result = loads_json(document)
        assert not is_err(result)
        assert result.ok_value == [expected]

    @pytest.mark.parametrize(
        'document',
        [
            r'"\ud800"',
            r'"\uDFFF"',
            r'{"x":"\ud800"}',
            r'{"\ud800":1}',
            r'[["a\ud800b"]]',
            r'"\ude00\ud83d"',
            r'"\ud83d\u0041"',
            r'"\\\ud800"',
            '"\ud800"',
            '[' * 900 + r'"\ud800"' + ']' * 900,
        ],
    )
    def test_lone_surrogate_rejected(self, document: str) -> None:
        result = loads_json(document)
        assert is_err(result)
        assert 'lone surrogate' in str(result.err_value)

    def test_lone_surrogate_rejection_text_holds_no_input_value(self) -> None:
        result = loads_json(r'{"secret":"abc\ud800"}')
        assert is_err(result)
        assert 'abc' not in str(result.err_value)
        assert 'secret' not in str(result.err_value)

    @pytest.mark.parametrize(
        ('document', 'expected'),
        [
            (r'"\ud83d\ude00"', '😀'),
            (r'"\uD83D\uDE00"', '😀'),
            ('"😀"', '😀'),
            (r'"\u00e9"', 'é'),
            (r'"\\ud800"', '\\ud800'),
            (r'{"\ud83d\ude00":"\ud83d\ude00"}', {'😀': '😀'}),
        ],
    )
    def test_valid_text_keeps_its_value(
        self, document: str, expected: object,
    ) -> None:
        result = loads_json(document)
        assert not is_err(result)
        assert result.ok_value == expected

    def test_integer_literal_stays_an_integer_past_the_float_range(self) -> None:
        digits = '1' * 400
        result = loads_json(f'[{digits}]')
        assert not is_err(result)
        assert result.ok_value == [int(digits)]
