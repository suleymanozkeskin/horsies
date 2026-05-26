"""Unit tests for horsies.core.codec.typed.

Covers phase 2 of the strict-serde redesign:

- `encode_value` / `decode_value` round-trip for every allowed type.
- `_scan_wire_json` rejects non-finite floats anywhere in dump output.
- `_scan_reserved_keys` rejects `__h_*` / `__builtin_task_code__` at any depth.
- JsonValue producer-side fence (`_validate_json_native`) rejects bytes,
  Decimal, etc. even though `TypeAdapter(JsonValue)` would coerce them.
- TypeAdapter cache: same `expected_type` returns the same adapter.
"""

from __future__ import annotations

import dataclasses
import datetime
import decimal
import enum
import uuid
from typing import (
    Annotated,
    Literal,
    Optional,
    TypeAlias,
    Union,
    cast,
)

import pytest
from pydantic import BaseModel, Field, ValidationError

from horsies.core.codec.json_value import (
    JsonValue,
    StrictJsonError,
)
from horsies.core.codec.typed import (
    Json,
    _get_adapter,  # pyright: ignore[reportPrivateUsage]
    _scan_reserved_keys,  # pyright: ignore[reportPrivateUsage]
    _scan_wire_json,  # pyright: ignore[reportPrivateUsage]
    decode_value,
    encode_value,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


class _User(BaseModel):
    name: str
    age: int


@dataclasses.dataclass
class _Order:
    order_id: str
    total: float


class _Color(enum.Enum):
    RED = 'red'
    GREEN = 'green'
    BLUE = 'blue'


class _Status(enum.IntEnum):
    ACTIVE = 1
    INACTIVE = 0


class _Cat(BaseModel):
    kind: Literal['cat'] = 'cat'
    name: str


class _Dog(BaseModel):
    kind: Literal['dog'] = 'dog'
    name: str


_Pet: TypeAlias = Annotated[Union[_Cat, _Dog], Field(discriminator='kind')]


# ---------------------------------------------------------------------------
# Primitives + datetime + UUID + Decimal + Enum
# ---------------------------------------------------------------------------


class TestRoundTripScalars:
    @pytest.mark.parametrize(
        'value, type_',
        [
            (None, type(None)),
            (True, bool),
            (False, bool),
            (0, int),
            (42, int),
            (-7, int),
            (0.0, float),
            (3.14, float),
            ('', str),
            ('hello', str),
        ],
    )
    def test_primitive_round_trip(
        self,
        value: object,
        type_: type,
    ) -> None:
        encoded = encode_value(value, type_)
        decoded = decode_value(encoded, type_)
        assert decoded == value

    def test_datetime_round_trip(self) -> None:
        value = datetime.datetime(2026, 5, 26, 12, 30, 45)
        encoded = encode_value(value, datetime.datetime)
        assert isinstance(encoded, str)
        decoded = decode_value(encoded, datetime.datetime)
        assert decoded == value

    def test_date_round_trip(self) -> None:
        value = datetime.date(2026, 5, 26)
        encoded = encode_value(value, datetime.date)
        decoded = decode_value(encoded, datetime.date)
        assert decoded == value

    def test_time_round_trip(self) -> None:
        value = datetime.time(14, 30, 0)
        encoded = encode_value(value, datetime.time)
        decoded = decode_value(encoded, datetime.time)
        assert decoded == value

    def test_uuid_round_trip(self) -> None:
        value = uuid.UUID('12345678-1234-5678-1234-567812345678')
        encoded = encode_value(value, uuid.UUID)
        assert isinstance(encoded, str)
        decoded = decode_value(encoded, uuid.UUID)
        assert decoded == value

    def test_decimal_round_trip_no_float_coercion(self) -> None:
        # The spike confirmed Decimal dumps as string and decodes back exactly.
        value = decimal.Decimal('0.1')
        encoded = encode_value(value, decimal.Decimal)
        assert encoded == '0.1'
        decoded = decode_value(encoded, decimal.Decimal)
        assert decoded == value
        assert isinstance(decoded, decimal.Decimal)

    def test_enum_str_round_trip(self) -> None:
        encoded = encode_value(_Color.RED, _Color)
        decoded = decode_value(encoded, _Color)
        assert decoded is _Color.RED

    def test_intenum_round_trip(self) -> None:
        encoded = encode_value(_Status.ACTIVE, _Status)
        decoded = decode_value(encoded, _Status)
        assert decoded is _Status.ACTIVE


# ---------------------------------------------------------------------------
# BaseModel + dataclass
# ---------------------------------------------------------------------------


class TestRoundTripStructured:
    def test_basemodel_round_trip(self) -> None:
        value = _User(name='Alice', age=30)
        encoded = encode_value(value, _User)
        assert encoded == {'name': 'Alice', 'age': 30}
        decoded = decode_value(encoded, _User)
        assert decoded == value

    def test_dataclass_round_trip(self) -> None:
        value = _Order(order_id='ord-1', total=42.50)
        encoded = encode_value(value, _Order)
        assert encoded == {'order_id': 'ord-1', 'total': 42.50}
        decoded = decode_value(encoded, _Order)
        assert decoded == value


# ---------------------------------------------------------------------------
# Containers
# ---------------------------------------------------------------------------


class TestRoundTripContainers:
    def test_list_of_primitive(self) -> None:
        value = [1, 2, 3]
        encoded = encode_value(value, list[int])
        decoded = decode_value(encoded, list[int])
        assert decoded == value

    def test_dict_str_primitive(self) -> None:
        value = {'a': 1, 'b': 2}
        encoded = encode_value(value, dict[str, int])
        decoded = decode_value(encoded, dict[str, int])
        assert decoded == value

    def test_tuple_variadic(self) -> None:
        value = (1, 2, 3, 4)
        encoded = encode_value(value, tuple[int, ...])
        # tuples encode as lists; declared type round-trips back to tuple.
        decoded = decode_value(encoded, tuple[int, ...])
        assert decoded == value

    def test_tuple_fixed_shape(self) -> None:
        value = (1, 'two', True)
        encoded = encode_value(value, tuple[int, str, bool])
        decoded = decode_value(encoded, tuple[int, str, bool])
        assert decoded == value

    def test_nested_containers(self) -> None:
        value = {'items': [1, 2, 3], 'meta': [{'k': 'v'}]}
        annotation = dict[str, list[dict[str, str] | int]]
        encoded = encode_value(value, annotation)
        decoded = decode_value(encoded, annotation)
        assert decoded == value


# ---------------------------------------------------------------------------
# Optional, Annotated, Literal
# ---------------------------------------------------------------------------


class TestRoundTripOptionalAnnotatedLiteral:
    def test_optional_primitive_some(self) -> None:
        encoded = encode_value(42, Optional[int])
        decoded = decode_value(encoded, Optional[int])
        assert decoded == 42

    def test_optional_primitive_none(self) -> None:
        encoded = encode_value(None, Optional[int])
        assert encoded is None
        decoded = decode_value(encoded, Optional[int])
        assert decoded is None

    def test_annotated_passthrough(self) -> None:
        annotation = Annotated[int, 'positive']
        encoded = encode_value(7, annotation)
        decoded = decode_value(encoded, annotation)
        assert decoded == 7

    def test_literal_str(self) -> None:
        annotation = Literal['a', 'b', 'c']
        encoded = encode_value('b', annotation)
        decoded = decode_value(encoded, annotation)
        assert decoded == 'b'

    def test_literal_int(self) -> None:
        annotation = Literal[1, 2, 3]
        encoded = encode_value(2, annotation)
        decoded = decode_value(encoded, annotation)
        assert decoded == 2


# ---------------------------------------------------------------------------
# Unions: primitive + discriminated
# ---------------------------------------------------------------------------


class TestRoundTripUnions:
    def test_primitive_union_int_branch(self) -> None:
        annotation = int | str
        encoded = encode_value(42, annotation)
        decoded = decode_value(encoded, annotation)
        assert decoded == 42

    def test_primitive_union_str_branch(self) -> None:
        annotation = int | str
        encoded = encode_value('hello', annotation)
        decoded = decode_value(encoded, annotation)
        assert decoded == 'hello'

    def test_discriminated_union_cat(self) -> None:
        cat = _Cat(name='Whiskers')
        encoded = encode_value(cat, _Pet)
        decoded = decode_value(encoded, _Pet)
        assert decoded == cat

    def test_discriminated_union_dog(self) -> None:
        dog = _Dog(name='Rex')
        encoded = encode_value(dog, _Pet)
        decoded = decode_value(encoded, _Pet)
        assert decoded == dog


# ---------------------------------------------------------------------------
# JsonValue at boundary
# ---------------------------------------------------------------------------


class TestJsonValueBoundary:
    @pytest.mark.parametrize(
        'value',
        [
            None,
            True,
            42,
            3.14,
            'hello',
            [1, 'two', None],
            {'nested': {'list': [1, {'deep': [None, 'x']}]}},
        ],
    )
    def test_jsonvalue_round_trip(self, value: object) -> None:
        encoded = encode_value(value, JsonValue)
        decoded = decode_value(encoded, JsonValue)
        assert decoded == value

    def test_jsonvalue_producer_fence_rejects_bytes(self) -> None:
        # TypeAdapter(JsonValue) alone would coerce bytes->str silently;
        # the producer-side fence rejects.
        with pytest.raises(StrictJsonError, match='bytes'):
            encode_value(b'abc', JsonValue)

    def test_jsonvalue_producer_fence_rejects_decimal(self) -> None:
        with pytest.raises(StrictJsonError, match='Decimal'):
            encode_value({'price': decimal.Decimal('1.2')}, JsonValue)

    def test_jsonvalue_producer_fence_rejects_nan(self) -> None:
        with pytest.raises(StrictJsonError, match='non-RFC-8259 float'):
            encode_value(float('nan'), JsonValue)


# ---------------------------------------------------------------------------
# Non-finite floats outside JsonValue — caught by _scan_wire_json
# ---------------------------------------------------------------------------


class TestWireScanNonFiniteFloats:
    def test_nan_in_float_field_rejected(self) -> None:
        with pytest.raises(StrictJsonError, match='non-RFC-8259 float'):
            encode_value(float('nan'), float)

    def test_inf_in_float_field_rejected(self) -> None:
        with pytest.raises(StrictJsonError, match='non-RFC-8259 float'):
            encode_value(float('inf'), float)

    def test_neg_inf_in_float_field_rejected(self) -> None:
        with pytest.raises(StrictJsonError, match='non-RFC-8259 float'):
            encode_value(float('-inf'), float)

    def test_nan_in_basemodel_field_rejected(self) -> None:
        class HasFloat(BaseModel):
            score: float

        with pytest.raises(StrictJsonError, match='non-RFC-8259 float'):
            encode_value(HasFloat(score=float('nan')), HasFloat)

    def test_nan_inside_list_rejected(self) -> None:
        with pytest.raises(StrictJsonError, match='non-RFC-8259 float'):
            encode_value([1.0, float('nan'), 3.0], list[float])

    def test_nan_inside_dict_rejected(self) -> None:
        with pytest.raises(StrictJsonError, match='non-RFC-8259 float'):
            encode_value({'a': float('inf')}, dict[str, float])


# ---------------------------------------------------------------------------
# Reserved-key scan
# ---------------------------------------------------------------------------


class TestReservedKeyScan:
    def test_h_prefix_key_rejected_in_user_dict(self) -> None:
        class Bag(BaseModel):
            metadata: dict[str, str]

        bag = Bag(metadata={'__h_smuggled__': 'oops'})
        with pytest.raises(StrictJsonError, match='reserved key'):
            encode_value(bag, Bag)

    def test_builtin_task_code_key_rejected_in_user_dict(self) -> None:
        class Bag(BaseModel):
            metadata: dict[str, str]

        bag = Bag(metadata={'__builtin_task_code__': 'oops'})
        with pytest.raises(StrictJsonError, match='reserved key'):
            encode_value(bag, Bag)

    def test_h_prefix_key_nested_in_list_rejected(self) -> None:
        class Wrapper(BaseModel):
            items: list[dict[str, str]]

        wrapper = Wrapper(items=[{'ok': 'fine'}, {'__h_oops__': 'no'}])
        with pytest.raises(StrictJsonError, match='reserved key'):
            encode_value(wrapper, Wrapper)

    def test_non_reserved_key_starting_with_h_is_fine(self) -> None:
        # Reserved prefix is `__h_` (two leading underscores); `_h_x` is fine.
        encode_value({'_h_label': 'ok'}, dict[str, str])

    def test_jsonvalue_with_reserved_key_rejected(self) -> None:
        with pytest.raises(StrictJsonError, match='reserved key'):
            encode_value({'__h_evil__': 'oops'}, JsonValue)


# ---------------------------------------------------------------------------
# Lower-level scans tested directly
# ---------------------------------------------------------------------------


class TestScanWireJsonDirect:
    def test_accepts_valid_shape(self) -> None:
        _scan_wire_json({'a': 1, 'b': [2.5, None, 'x', True]})

    def test_rejects_nan_at_top_level(self) -> None:
        with pytest.raises(StrictJsonError, match='non-RFC-8259 float'):
            _scan_wire_json(float('nan'))

    def test_rejects_inf_nested(self) -> None:
        with pytest.raises(StrictJsonError, match='non-RFC-8259 float'):
            _scan_wire_json({'x': [1, float('inf')]})


class TestScanReservedKeysDirect:
    def test_accepts_clean_payload(self) -> None:
        _scan_reserved_keys({'a': 1, 'b': [2, {'c': 'ok'}]})

    def test_rejects_h_prefix(self) -> None:
        with pytest.raises(StrictJsonError, match='reserved key'):
            _scan_reserved_keys({'__h_workflow_ctx__': True})

    def test_rejects_builtin_task_code(self) -> None:
        with pytest.raises(StrictJsonError, match='reserved key'):
            _scan_reserved_keys({'__builtin_task_code__': 'INTERNAL'})

    def test_rejects_nested_reserved(self) -> None:
        with pytest.raises(StrictJsonError, match='reserved key'):
            _scan_reserved_keys({'outer': [{'__h_inner__': 1}]})


# ---------------------------------------------------------------------------
# Decode-side: ValidationError for type mismatch
# ---------------------------------------------------------------------------


class TestDecodeValidation:
    def test_decode_type_mismatch_raises(self) -> None:
        with pytest.raises(ValidationError):
            decode_value(cast(Json, 'not-an-int'), int)

    def test_decode_missing_required_field_raises(self) -> None:
        with pytest.raises(ValidationError):
            decode_value(cast(Json, {'name': 'Alice'}), _User)


# ---------------------------------------------------------------------------
# TypeAdapter cache
# ---------------------------------------------------------------------------


class TestAdapterCache:
    def test_same_type_returns_same_adapter(self) -> None:
        a1 = _get_adapter(int)
        a2 = _get_adapter(int)
        assert a1 is a2

    def test_different_types_return_different_adapters(self) -> None:
        a_int = _get_adapter(int)
        a_str = _get_adapter(str)
        assert a_int is not a_str

    def test_parameterized_container_cached(self) -> None:
        a1 = _get_adapter(list[int])
        a2 = _get_adapter(list[int])
        assert a1 is a2
