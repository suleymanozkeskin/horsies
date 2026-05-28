"""Tests for the strict JSON I/O boundary (codec/json_io.py).

dumps_json is strict: it rejects non-JSON-native values (tuple, set, non-str
dict keys, lone surrogates, non-finite floats) rather than coercing them, and
emits no class tags. loads_json rejects non-RFC-8259 constants (NaN/Infinity).
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
