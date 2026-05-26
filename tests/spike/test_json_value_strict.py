"""Strict JsonValue producer/decoder spike — phase 0.

Covers all cases from `ignored-content/design/strict-serde.md` §12 phase 0.

Producer-side `_validate_json_native`:
- bytes -> rejected (not coerced to str)
- Decimal nested in dict -> rejected (not coerced to float)
- float('nan'), float('inf'), float('-inf') -> rejected
- tuples (top-level and nested) -> rejected
- arbitrary objects -> rejected
- non-string dict keys -> rejected
- bool isinstance int ordering correctness
- valid nested JSON shapes accepted and round-trip cleanly

Decode-side `_reject_nonstandard_json_constant`:
- json.loads('NaN', parse_constant=...) raises StrictJsonError
- same for 'Infinity', '-Infinity'
- normal payloads pass through unchanged
"""

from __future__ import annotations

import json
from decimal import Decimal
from typing import cast

import pytest
from pydantic import TypeAdapter

from horsies.core.codec.json_value import (
    JsonValue,
    StrictJsonError,
    _reject_nonstandard_json_constant,
    _validate_json_native,
)


_ADAPTER: TypeAdapter[JsonValue] = TypeAdapter(JsonValue)


# ---------------------------------------------------------------------------
# Producer-side: _validate_json_native rejections
# ---------------------------------------------------------------------------


class TestValidateJsonNativeRejects:
    """Each test names the surprise that TypeAdapter alone would let through."""

    def test_bytes_rejected_not_coerced_to_str(self) -> None:
        with pytest.raises(StrictJsonError, match='non-JSON-native value: bytes'):
            _validate_json_native(b'abc')

    def test_bytes_inside_dict_rejected(self) -> None:
        with pytest.raises(StrictJsonError, match='non-JSON-native value: bytes'):
            _validate_json_native({'x': b'abc'})

    def test_decimal_nested_in_dict_rejected_not_coerced_to_float(self) -> None:
        with pytest.raises(StrictJsonError, match='non-JSON-native value: Decimal'):
            _validate_json_native({'price': Decimal('1.2')})

    def test_nan_rejected(self) -> None:
        with pytest.raises(StrictJsonError, match='non-RFC-8259 float'):
            _validate_json_native(float('nan'))

    def test_positive_infinity_rejected(self) -> None:
        with pytest.raises(StrictJsonError, match='non-RFC-8259 float'):
            _validate_json_native(float('inf'))

    def test_negative_infinity_rejected(self) -> None:
        with pytest.raises(StrictJsonError, match='non-RFC-8259 float'):
            _validate_json_native(float('-inf'))

    def test_top_level_tuple_rejected(self) -> None:
        with pytest.raises(StrictJsonError, match='non-JSON-native value: tuple'):
            _validate_json_native((1, 2, 3))

    def test_nested_tuple_rejected(self) -> None:
        with pytest.raises(StrictJsonError, match='non-JSON-native value: tuple'):
            _validate_json_native({'coords': (1, 2)})

    def test_set_rejected(self) -> None:
        with pytest.raises(StrictJsonError, match='non-JSON-native value: set'):
            _validate_json_native({1, 2, 3})

    def test_arbitrary_object_rejected(self) -> None:
        class Foo:
            pass

        with pytest.raises(StrictJsonError, match='non-JSON-native value: Foo'):
            _validate_json_native(Foo())

    def test_non_string_dict_key_rejected_int(self) -> None:
        with pytest.raises(StrictJsonError, match='dict key must be str, got int'):
            _validate_json_native({1: 'a'})

    def test_non_string_dict_key_rejected_tuple(self) -> None:
        with pytest.raises(StrictJsonError, match='dict key must be str, got tuple'):
            _validate_json_native({(1, 2): 'a'})

    def test_non_string_dict_key_rejected_none(self) -> None:
        with pytest.raises(
            StrictJsonError,
            match='dict key must be str, got NoneType',
        ):
            _validate_json_native({None: 'a'})

    def test_nested_invalid_inside_list_rejected(self) -> None:
        with pytest.raises(StrictJsonError, match='non-JSON-native value: bytes'):
            _validate_json_native([1, 'ok', b'bad'])

    def test_deeply_nested_invalid_rejected(self) -> None:
        payload = {'a': [{'b': [{'c': Decimal('1.0')}]}]}
        with pytest.raises(StrictJsonError, match='non-JSON-native value: Decimal'):
            _validate_json_native(payload)


# ---------------------------------------------------------------------------
# Producer-side: bool ordering correctness
# ---------------------------------------------------------------------------


class TestValidateJsonNativeBoolOrdering:
    """`True isinstance int` is True — bool check must come first or booleans
    get silently demoted to int in the typed contract."""

    def test_true_accepted_as_bool(self) -> None:
        _validate_json_native(True)

    def test_false_accepted_as_bool(self) -> None:
        _validate_json_native(False)

    def test_bool_inside_list_accepted(self) -> None:
        _validate_json_native([True, False])

    def test_bool_inside_dict_accepted(self) -> None:
        _validate_json_native({'flag': True})


# ---------------------------------------------------------------------------
# Producer-side: happy path
# ---------------------------------------------------------------------------


class TestValidateJsonNativeAccepts:
    """Valid JSON-native shapes pass through and round-trip cleanly."""

    @pytest.mark.parametrize(
        'value',
        [
            None,
            True,
            False,
            0,
            42,
            -7,
            0.0,
            3.14,
            -2.5,
            '',
            'hello',
            [],
            {},
            [1, 'two', None],
            {'a': 1, 'b': 'two'},
            {'nested': {'list': [1, {'deep': [None, 'x', True]}]}},
        ],
    )
    def test_accepts_and_round_trips(self, value: object) -> None:
        _validate_json_native(value)
        # Parametrize widens to `object`; each entry is hand-curated to satisfy
        # `JsonValue`, so the cast is honest at the test boundary.
        dumped = _ADAPTER.dump_python(cast(JsonValue, value), mode='json')
        assert _ADAPTER.validate_python(dumped) == value


# ---------------------------------------------------------------------------
# Decode-side: _reject_nonstandard_json_constant
# ---------------------------------------------------------------------------


class TestRejectNonstandardJsonConstant:
    """Every raw `json.loads(s, parse_constant=...)` site must fail closed
    on `NaN`, `Infinity`, `-Infinity`."""

    @pytest.mark.parametrize('constant', ['NaN', 'Infinity', '-Infinity'])
    def test_raises_for_top_level_constant(self, constant: str) -> None:
        with pytest.raises(
            StrictJsonError,
            match='Non-standard JSON constant is not allowed',
        ):
            json.loads(
                constant,
                parse_constant=_reject_nonstandard_json_constant,
            )

    def test_nested_nan_rejected(self) -> None:
        with pytest.raises(StrictJsonError):
            json.loads(
                '{"x": NaN}',
                parse_constant=_reject_nonstandard_json_constant,
            )

    def test_nested_infinity_in_list_rejected(self) -> None:
        with pytest.raises(StrictJsonError):
            json.loads(
                '[1, Infinity, 3]',
                parse_constant=_reject_nonstandard_json_constant,
            )

    def test_normal_payload_unaffected(self) -> None:
        payload = '{"a": 1, "b": "two", "c": null, "d": true, "e": [1, 2.5]}'
        result = json.loads(
            payload,
            parse_constant=_reject_nonstandard_json_constant,
        )
        assert result == {
            'a': 1,
            'b': 'two',
            'c': None,
            'd': True,
            'e': [1, 2.5],
        }

    def test_direct_call_includes_constant_name(self) -> None:
        with pytest.raises(
            StrictJsonError,
            match='Non-standard JSON constant is not allowed: NaN',
        ):
            _reject_nonstandard_json_constant('NaN')


# ---------------------------------------------------------------------------
# Sanity: TypeAdapter alone is NOT strict — motivates `_validate_json_native`
# ---------------------------------------------------------------------------


class TestTypeAdapterCoercionMotivation:
    """Documents the surprise that motivates `_validate_json_native`.

    If a future Pydantic release makes `TypeAdapter(JsonValue)` strict on
    its own, these tests will fail loudly — at which point the validator's
    role narrows to bool-ordering correctness and non-finite floats, and
    this class can be retired.
    """

    def test_type_adapter_silently_coerces_bytes_to_str(self) -> None:
        result = _ADAPTER.validate_python(b'abc')
        assert result == 'abc'

    def test_type_adapter_silently_coerces_decimal_in_dict(self) -> None:
        result = _ADAPTER.validate_python({'x': Decimal('1.2')})
        assert result == {'x': 1.2}


# ---------------------------------------------------------------------------
# loads_json parse hardening: rejects NaN / Infinity / -Infinity at parse
# ---------------------------------------------------------------------------


class TestLoadsJsonParseHardening:
    """`serde.loads_json` routes every raw `json.loads` site through
    `parse_constant=_reject_nonstandard_json_constant` so Python's
    lenient acceptance of `NaN` / `Infinity` / `-Infinity` (not RFC
    8259) fails closed end-to-end. Without this, payloads written by
    other languages, by horsies versions predating the strict
    producer, or by manual writes to the result column could smuggle
    non-finite floats past the producer-side fence."""

    def test_nan_rejected(self) -> None:
        from horsies.core.codec.serde import loads_json
        from horsies.core.types.result import is_err

        result = loads_json('{"x": NaN}')
        assert is_err(result)
        assert 'NaN' in str(result.err_value)

    def test_infinity_rejected(self) -> None:
        from horsies.core.codec.serde import loads_json
        from horsies.core.types.result import is_err

        result = loads_json('{"x": Infinity}')
        assert is_err(result)
        assert 'Infinity' in str(result.err_value)

    def test_neg_infinity_rejected(self) -> None:
        from horsies.core.codec.serde import loads_json
        from horsies.core.types.result import is_err

        result = loads_json('{"x": -Infinity}')
        assert is_err(result)
        assert '-Infinity' in str(result.err_value)

    def test_normal_payload_passes(self) -> None:
        from horsies.core.codec.serde import loads_json
        from horsies.core.types.result import is_err

        result = loads_json('{"x": 1.0, "y": [null, true, "s"]}')
        assert not is_err(result)
        assert result.ok_value == {'x': 1.0, 'y': [None, True, 's']}
