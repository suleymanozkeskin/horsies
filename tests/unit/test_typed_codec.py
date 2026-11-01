"""Unit tests for horsies.core.codec.typed.

Covers phase 2 of the strict-serde redesign:

- `encode_value` / `decode_value` round-trip for every allowed type.
- `_scan_wire_json` rejects non-finite floats and lone surrogates anywhere in
  dump output, and `encode_value` refuses each string that `dumps_json` refuses.
- `_scan_reserved_keys` rejects `__h_*` / `__builtin_task_code__` at any depth.
- JsonValue producer-side fence (`_validate_json_native`) rejects bytes,
  Decimal, etc. even though `TypeAdapter(JsonValue)` would coerce them.
- TypeAdapter cache: same `expected_type` returns the same adapter.
"""

from __future__ import annotations

import collections
import dataclasses
import datetime
import decimal
import enum
import uuid
from collections.abc import Iterable, Iterator
from typing import (
    Annotated,
    Any,
    Literal,
    NamedTuple,
    Optional,
    TypeAlias,
    Union,
    cast,
)

import pytest
from pydantic import AfterValidator, BaseModel, Field, computed_field
from pydantic_core import PydanticCustomError

from horsies.core.codec.json_io import dumps_json
from horsies.core.codec.json_value import (
    JsonValue,
    StrictJsonError,
)
from horsies.core.codec.validation_errors import ValueValidationError
from horsies.core.codec.typed import (
    Json,
    _get_adapter,  # pyright: ignore[reportPrivateUsage]
    _scan_reserved_keys,  # pyright: ignore[reportPrivateUsage]
    _scan_wire_json,  # pyright: ignore[reportPrivateUsage]
    decode_task_error,
    decode_task_result,
    decode_value,
    encode_task_result,
    encode_value,
    validate_task_result_envelope,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


LONE_SURROGATE_TEXT = f'keep-out{chr(0xd800)}'


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


class TestJsonValueDerivativePositions:
    """`dict[str, JsonValue]` / `list[JsonValue]` / `Optional[JsonValue]` /
    JsonValue inside BaseModel fields all carry the same raw-JSON
    contract as the literal `JsonValue` boundary. Without the recursive
    fence, TypeAdapter silently coerces (bytes -> str, Decimal -> float)
    in these positions, defeating the strict-mode promise."""

    def test_dict_str_jsonvalue_rejects_bytes(self) -> None:
        with pytest.raises(StrictJsonError, match='bytes'):
            encode_value({'data': b'abc'}, dict[str, JsonValue])

    def test_dict_str_jsonvalue_rejects_decimal(self) -> None:
        with pytest.raises(StrictJsonError, match='Decimal'):
            encode_value({'price': decimal.Decimal('1.2')}, dict[str, JsonValue])

    def test_list_jsonvalue_rejects_bytes(self) -> None:
        with pytest.raises(StrictJsonError, match='bytes'):
            encode_value([b'abc', b'def'], list[JsonValue])

    def test_optional_jsonvalue_none_ok(self) -> None:
        # `Optional[JsonValue]` with a None value short-circuits the fence
        # walker (nothing to validate).
        encoded = encode_value(None, Optional[JsonValue])
        assert encoded is None

    def test_optional_jsonvalue_rejects_bytes(self) -> None:
        with pytest.raises(StrictJsonError, match='bytes'):
            encode_value(b'abc', Optional[JsonValue])

    def test_nested_dict_in_list_jsonvalue_rejects_decimal(self) -> None:
        with pytest.raises(StrictJsonError, match='Decimal'):
            encode_value(
                [{'price': decimal.Decimal('1.2')}],
                list[dict[str, JsonValue]],
            )

    def test_jsonvalue_inside_basemodel_field_via_model_construct(self) -> None:
        # `WithBag(bag=b'oops')` would silently coerce bytes -> str at
        # Pydantic construction time (before encode_value ever sees the
        # value), so the fence can't catch normal-construction smuggle.
        # `model_construct` bypasses validation, leaving raw bytes on the
        # field — that's the case the BaseModel walk in
        # `_apply_json_value_fence` actually protects.
        class WithBag(BaseModel):
            name: str
            bag: JsonValue

        smuggled = WithBag.model_construct(name='x', bag=b'oops')  # pyright: ignore[reportArgumentType]
        with pytest.raises(StrictJsonError, match='bytes'):
            encode_value(smuggled, WithBag)

    def test_dict_str_jsonvalue_with_clean_payload_round_trips(self) -> None:
        payload = {'a': 1, 'b': 'two', 'c': None, 'd': [1, 2]}
        encoded = encode_value(payload, dict[str, JsonValue])
        decoded = decode_value(encoded, dict[str, JsonValue])
        assert decoded == payload


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

    def test_legacy_horsies_prefix_rejected_in_user_dict(self) -> None:
        # `__horsies_*` is the legacy transport prefix (still in active
        # engine use). User-originated data carrying it would smuggle
        # an envelope past the worker's args_from handler into legacy
        # `task_result_from_json` / `rehydrate_value` (the old
        # class-identity importer). Reject at encode time.
        with pytest.raises(StrictJsonError, match='reserved key'):
            encode_value(
                {'__h_taskresult_envelope__': True, 'data': 'evil'},
                JsonValue,
            )

    def test_legacy_horsies_prefix_rejected_nested(self) -> None:
        with pytest.raises(StrictJsonError, match='reserved key'):
            encode_value(
                {'metadata': {'__h_workflow_ctx__': 'oops'}},
                dict[str, JsonValue],
            )


# ---------------------------------------------------------------------------
# TaskError: path-aware encode/decode (the validator + codec must agree).
# ---------------------------------------------------------------------------


class TestTaskErrorRoundTrip:
    """TaskError is in INTERNAL_CODEC_TYPES (the validator skips its
    field walk). Encode/decode must also handle it cleanly — TaskError's
    `error_code` field legitimately emits `{"__builtin_task_code__":
    "..."}` for built-in codes; the generic scan would falsely reject
    that. Path-aware allowance scans only the user-controlled fields."""

    def test_builtin_code_round_trips(self) -> None:
        from horsies.core.models.tasks import OperationalErrorCode, TaskError

        err = TaskError(
            error_code=OperationalErrorCode.BROKER_ERROR,
            message='m',
            data={'k': 'v'},
        )
        encoded = encode_value(err, TaskError)
        decoded = decode_value(encoded, TaskError)
        assert isinstance(decoded, TaskError)
        assert decoded.error_code == OperationalErrorCode.BROKER_ERROR
        assert decoded.data == {'k': 'v'}
        assert decoded.message == 'm'

    def test_user_string_error_code_round_trips(self) -> None:
        from horsies.core.models.tasks import TaskError

        err = TaskError(error_code='my_user_code', message='ok')
        encoded = encode_value(err, TaskError)
        decoded = decode_value(encoded, TaskError)
        assert isinstance(decoded, TaskError)
        assert decoded.error_code == 'my_user_code'

    def test_reserved_key_in_data_still_rejected(self) -> None:
        from horsies.core.models.tasks import OperationalErrorCode, TaskError

        # `data` is user-controlled and gets the full reserved-key scan.
        err = TaskError(
            error_code=OperationalErrorCode.BROKER_ERROR,
            data={'__h_evil__': 1},
        )
        with pytest.raises(StrictJsonError, match='reserved key'):
            encode_value(err, TaskError)

    def test_legacy_prefix_in_data_still_rejected(self) -> None:
        from horsies.core.models.tasks import OperationalErrorCode, TaskError

        err = TaskError(
            error_code=OperationalErrorCode.BROKER_ERROR,
            data={'__h_taskresult_envelope__': True},
        )
        with pytest.raises(StrictJsonError, match='reserved key'):
            encode_value(err, TaskError)

    def test_smuggled_reserved_key_alongside_error_code_discriminator_rejected(
        self,
    ) -> None:
        # The error_code path-aware allowance is for the EXACT
        # `{"__builtin_task_code__": "<str>"}` shape only. A payload
        # that pairs the discriminator with another reserved key
        # (cross-version producer, manual write to the result column)
        # would otherwise smuggle past the scan because TypeAdapter
        # silently drops unknown dict entries during error_code
        # validation.
        smuggled: Json = cast(Json, {
            'error_code': {
                '__builtin_task_code__': 'BROKER_ERROR',
                '__h_extra__': 1,
            },
            'message': 'x',
            'data': None,
            'exception': None,
        })
        from horsies.core.models.tasks import TaskError

        with pytest.raises(StrictJsonError, match='reserved key'):
            decode_value(smuggled, TaskError)

    def test_smuggled_legacy_prefix_alongside_error_code_discriminator_rejected(
        self,
    ) -> None:
        smuggled: Json = cast(Json, {
            'error_code': {
                '__builtin_task_code__': 'BROKER_ERROR',
                '__horsies_evil__': 1,
            },
            'message': None,
            'data': None,
            'exception': None,
        })
        from horsies.core.models.tasks import TaskError

        with pytest.raises(StrictJsonError, match='reserved key'):
            decode_value(smuggled, TaskError)

    def test_lone_reserved_key_under_error_code_rejected(self) -> None:
        # No legitimate discriminator at all — just a reserved key
        # smuggled under error_code.
        smuggled: Json = cast(Json, {
            'error_code': {'__h_evil__': 1},
            'message': None,
            'data': None,
            'exception': None,
        })
        from horsies.core.models.tasks import TaskError

        with pytest.raises(StrictJsonError, match='reserved key'):
            decode_value(smuggled, TaskError)

    def test_extra_nonreserved_key_under_error_code_rejected(self) -> None:
        # Pydantic would silently DROP the extra `"other"` key during
        # error_code validation (TypeAdapter ignores unknown keys), so
        # the typed scan has to enforce the shape itself. If we only
        # rejected reserved-key extras, `{"__builtin_task_code__": "X",
        # "other": "y"}` would round-trip with `"other"` quietly gone.
        bogus: Json = cast(Json, {
            'error_code': {
                '__builtin_task_code__': 'BROKER_ERROR',
                'other': 'y',
            },
            'message': None,
            'data': None,
            'exception': None,
        })
        from horsies.core.models.tasks import TaskError

        with pytest.raises(
            StrictJsonError,
            match='invalid TaskError.error_code shape',
        ):
            decode_value(bogus, TaskError)

    def test_empty_dict_under_error_code_rejected(self) -> None:
        # `{}` isn't the discriminator shape either.
        bogus: Json = cast(Json, {
            'error_code': {},
            'message': None,
            'data': None,
            'exception': None,
        })
        from horsies.core.models.tasks import TaskError

        with pytest.raises(
            StrictJsonError,
            match='invalid TaskError.error_code shape',
        ):
            decode_value(bogus, TaskError)

    def test_non_string_discriminator_value_rejected(self) -> None:
        # `{"__builtin_task_code__": 42}` is single-key but the value
        # isn't a string; not the documented shape.
        bogus: Json = cast(Json, {
            'error_code': {'__builtin_task_code__': 42},
            'message': None,
            'data': None,
            'exception': None,
        })
        from horsies.core.models.tasks import TaskError

        with pytest.raises(
            StrictJsonError,
            match='invalid TaskError.error_code shape',
        ):
            decode_value(bogus, TaskError)


# ---------------------------------------------------------------------------
# Lower-level scans tested directly
# ---------------------------------------------------------------------------


class _Labelled(BaseModel):
    name: str
    labels: dict[str, int]


class _Pair(NamedTuple):
    labels: dict[str, int]


class TestEncodeValueRefusesALoneSurrogate:
    """`encode_value` refuses each string that `dumps_json` refuses."""

    @pytest.mark.parametrize(
        ('value', 'expected_type', 'subject'),
        [
            (LONE_SURROGATE_TEXT, str, 'a string'),
            (['ok', LONE_SURROGATE_TEXT], list[str], 'a string'),
            ({'key': LONE_SURROGATE_TEXT}, dict[str, str], 'a string'),
            ({LONE_SURROGATE_TEXT: 1}, dict[str, int], 'a dict key'),
            (
                _Labelled(name=LONE_SURROGATE_TEXT, labels={}),
                _Labelled,
                'a string',
            ),
        ],
        ids=['top-level-str', 'str-in-list', 'str-in-dict-value', 'dict-key', 'model-field'],
    )
    def test_each_position_is_refused(
        self,
        value: object,
        expected_type: Any,
        subject: str,
    ) -> None:
        with pytest.raises(StrictJsonError) as raised:
            encode_value(value, expected_type)

        assert str(raised.value) == (
            f'{subject} holds a lone surrogate, which UTF-8 cannot encode'
        )

    @pytest.mark.parametrize(
        ('value', 'expected_type'),
        [
            ({'x': {LONE_SURROGATE_TEXT: 1}}, dict[str, dict[str, int]]),
            ({LONE_SURROGATE_TEXT: 1}, JsonValue),
            ([{'x': {LONE_SURROGATE_TEXT: 1}}], JsonValue),
            ({LONE_SURROGATE_TEXT: 1}, dict[str, JsonValue]),
            (_Labelled(name='n', labels={LONE_SURROGATE_TEXT: 1}), _Labelled),
            ([{LONE_SURROGATE_TEXT: 1}], list[dict[str, int]]),
            (({LONE_SURROGATE_TEXT: 1},), tuple[dict[str, int], ...]),
            (
                collections.deque([{LONE_SURROGATE_TEXT: 1}]),
                collections.deque[dict[str, int]],
            ),
        ],
        ids=[
            'nested-dict', 'json-value', 'nested-json-value', 'json-value-slot',
            'model-field', 'in-list', 'in-tuple', 'in-deque',
        ],
    )
    def test_a_key_that_the_dump_replaces_is_refused(
        self,
        value: object,
        expected_type: Any,
    ) -> None:
        """`mode='json'` writes U+FFFD in place of the surrogate of these keys."""
        with pytest.raises(StrictJsonError, match='a dict key holds a lone surrogate'):
            encode_value(value, expected_type)

    def test_two_keys_that_the_dump_makes_equal_are_refused(self) -> None:
        """The dump alone loses one entry: both keys become the same text."""
        value = {f'a{chr(0xd800)}': 1, f'a{chr(0xd801)}': 2}

        with pytest.raises(StrictJsonError, match='a dict key holds a lone surrogate'):
            encode_value(value, dict[str, int])

    @pytest.mark.parametrize(
        ('value', 'expected_type'),
        [
            ({LONE_SURROGATE_TEXT: 1}, Any),
            ({LONE_SURROGATE_TEXT: 1}, dict),
            ({'x': {LONE_SURROGATE_TEXT: 1}}, dict[str, Any]),
            (_Pair(labels={LONE_SURROGATE_TEXT: 1}), _Pair),
        ],
        ids=['any', 'bare-dict', 'any-value', 'named-tuple'],
    )
    def test_a_key_that_the_dump_raises_for_is_refused(
        self,
        value: object,
        expected_type: Any,
    ) -> None:
        """The dump raises `UnicodeEncodeError` for a key without a declared
        type. That is not in the `encode_value` catalog, so it is refused."""
        with pytest.raises(StrictJsonError) as raised:
            encode_value(value, expected_type, location=('payload',))

        assert str(raised.value) == (
            'payload: a dict key holds a lone surrogate, which UTF-8 cannot encode'
        )
        assert raised.value.__context__ is None

    def test_a_python_surrogate_pair_is_refused_as_dumps_json_refuses_it(self) -> None:
        """Only JSON text joins a pair: a `str` keeps two code points."""
        pair = chr(0xd83d) + chr(0xde00)

        assert dumps_json(pair).is_err()
        with pytest.raises(StrictJsonError, match='a string holds a lone surrogate'):
            encode_value(pair, str)

    def test_the_location_names_the_kwarg(self) -> None:
        with pytest.raises(StrictJsonError) as raised:
            encode_value(LONE_SURROGATE_TEXT, str, location=('note',))

        assert str(raised.value) == (
            'note: a string holds a lone surrogate, which UTF-8 cannot encode'
        )

    @pytest.mark.parametrize(
        ('value', 'expected_type'),
        [
            (LONE_SURROGATE_TEXT, str),
            ({LONE_SURROGATE_TEXT: 1}, dict[str, int]),
        ],
        ids=['string', 'key'],
    )
    def test_the_refusal_holds_no_input_value(
        self,
        value: object,
        expected_type: Any,
    ) -> None:
        """The `UnicodeEncodeError` holds the input, so it is not chained."""
        with pytest.raises(StrictJsonError) as raised:
            encode_value(value, expected_type)

        assert 'keep-out' not in str(raised.value)
        assert raised.value.__context__ is None
        assert raised.value.__cause__ is None


def _echo_in_value_error(value: str) -> str:
    raise ValueError(f'bad name {value}')


def _echo_in_assertion_error(value: str) -> str:
    raise AssertionError(f'bad name {value}')


def _echo_in_custom_error_context(value: str) -> str:
    raise PydanticCustomError('bad_name', 'bad name {name}', {'name': value})


def _refuse_with_constant_lone_surrogate(value: str) -> str:
    raise ValueError(f'bad name {value}{chr(0xd800)}')


_VALIDATORS_THAT_ECHO = [
    _echo_in_value_error,
    _echo_in_assertion_error,
    _echo_in_custom_error_context,
]
_VALIDATOR_IDS = ['value-error', 'assertion-error', 'custom-error-context']


class TestValidatorTextWithALoneSurrogate:
    """Pydantic cannot build the error text, and raises `UnicodeEncodeError`."""

    @pytest.mark.parametrize('validator', _VALIDATORS_THAT_ECHO, ids=_VALIDATOR_IDS)
    def test_encode_value_refuses_it(self, validator: Any) -> None:
        with pytest.raises(StrictJsonError) as raised:
            encode_value(
                LONE_SURROGATE_TEXT,
                Annotated[str, AfterValidator(validator)],
                location=('note',),
            )

        assert str(raised.value) == (
            'note: a validation error holds a lone surrogate, which UTF-8 '
            'cannot encode'
        )

    @pytest.mark.parametrize('validator', _VALIDATORS_THAT_ECHO, ids=_VALIDATOR_IDS)
    def test_the_refusal_holds_no_input_value(self, validator: Any) -> None:
        """The `UnicodeEncodeError` holds the text, so it is not chained."""
        with pytest.raises(StrictJsonError) as raised:
            encode_value(
                LONE_SURROGATE_TEXT, Annotated[str, AfterValidator(validator)],
            )

        assert 'keep-out' not in str(raised.value)
        assert raised.value.__context__ is None
        assert raised.value.__cause__ is None

    def test_decode_value_refuses_a_lone_surrogate_in_the_validator_text(self) -> None:
        """A stored value cannot hold a lone surrogate: `loads_json` refuses
        it. The validator text can, because the validator writes that text."""
        with pytest.raises(StrictJsonError) as raised:
            decode_value(
                'plain',
                Annotated[str, AfterValidator(_refuse_with_constant_lone_surrogate)],
            )

        assert str(raised.value) == (
            'a validation error holds a lone surrogate, which UTF-8 cannot encode'
        )
        assert raised.value.__context__ is None

    @pytest.mark.parametrize('validator', _VALIDATORS_THAT_ECHO, ids=_VALIDATOR_IDS)
    def test_a_validator_refusal_without_a_surrogate_is_unchanged(
        self, validator: Any,
    ) -> None:
        with pytest.raises(ValueValidationError, match='bad name plain'):
            encode_value('plain', Annotated[str, AfterValidator(validator)])


class _ListValued(enum.Enum):
    MEMBER = [1]


class _DictValued(enum.Enum):
    MEMBER = {'k': 1}


class _SetValued(enum.Enum):
    MEMBER = {1}


class _FrozensetValued(enum.Enum):
    MEMBER = frozenset({1})


class TestDictKeyWithoutAJsonForm:
    """`dump_python` raises a plain `TypeError` for these keys."""

    @pytest.mark.parametrize(
        ('value', 'expected_type', 'type_name'),
        [
            ({_ListValued.MEMBER: 1}, dict[_ListValued, int], 'list'),
            ({_DictValued.MEMBER: 1}, dict[_DictValued, int], 'dict'),
            ({_SetValued.MEMBER: 1}, dict[_SetValued, int], 'set'),
            ({_FrozensetValued.MEMBER: 1}, dict[_FrozensetValued, int], 'frozenset'),
            ({frozenset({1}): 1}, dict[frozenset[int], int], 'frozenset'),
            ({(frozenset({1}),): 1}, dict[tuple[frozenset[int]], int], 'frozenset'),
            ({frozenset({1}): 1}, Any, 'frozenset'),
            ({'k': {_ListValued.MEMBER: 1}}, dict[str, dict[_ListValued, int]], 'list'),
        ],
        ids=[
            'enum-list-value', 'enum-dict-value', 'enum-set-value',
            'enum-frozenset-value', 'frozenset', 'tuple-with-frozenset',
            'frozenset-at-any', 'nested',
        ],
    )
    def test_it_is_a_catalog_refusal(
        self,
        value: object,
        expected_type: Any,
        type_name: str,
    ) -> None:
        with pytest.raises(ValueValidationError) as raised:
            encode_value(value, expected_type, location=('labels',))

        assert str(raised.value) == (
            f'labels: serialization failed: `{type_name}` not valid as object key'
        )
        assert raised.value.__context__ is None

    def test_the_text_has_no_location_part_without_a_location(self) -> None:
        with pytest.raises(ValueValidationError) as raised:
            encode_value({frozenset({1}): 1}, dict[frozenset[int], int])

        assert str(raised.value) == (
            'serialization failed: `frozenset` not valid as object key'
        )


class _TypeErrorInComputedField(BaseModel):
    text: str

    @computed_field
    @property
    def derived(self) -> int:
        raise TypeError(self.text)


def _yield_then_raise_type_error() -> Iterator[int]:
    yield 1
    raise TypeError('defect in a generator')


class TestTypeErrorThatIsADefect:
    """Only the full text of the key refusal is a catalog refusal."""

    @pytest.mark.parametrize(
        'text',
        [
            'defect in a computed field',
            'wrapped: `list` not valid as object key',
            '`list` not valid as object key: wrapped',
            'list not valid as object key',
        ],
        ids=['other-text', 'text-in-front', 'text-behind', 'no-backticks'],
    )
    def test_a_type_error_from_a_computed_field_propagates(self, text: str) -> None:
        with pytest.raises(TypeError) as raised:
            encode_value(_TypeErrorInComputedField(text=text), _TypeErrorInComputedField)

        assert str(raised.value) == text

    def test_a_type_error_from_a_generator_propagates(self) -> None:
        with pytest.raises(TypeError, match='defect in a generator'):
            encode_value(_yield_then_raise_type_error(), Iterable[int])


class _Linked(BaseModel):
    """A self-referencing model: registration accepts it in a task signature."""

    next: Optional['_Linked'] = None


def _linked_to_itself() -> _Linked:
    node = _Linked()
    node.next = node
    return node


def _list_that_holds_itself() -> list[Any]:
    value: list[Any] = []
    value.append(value)
    return value


def _dict_that_holds_itself() -> dict[str, Any]:
    value: dict[str, Any] = {}
    value['self'] = value
    return value


def _nested_list(depth: int) -> list[Any]:
    """A finite list with `depth` levels of nesting and a number inside."""
    value: list[Any] = [1]
    for _ in range(depth - 1):
        value = [value]
    return value


def _nested_dict(depth: int) -> dict[str, Any]:
    """A finite dict with `depth` levels of nesting and a number inside."""
    value: dict[str, Any] = {'key': 1}
    for _ in range(depth - 1):
        value = {'key': value}
    return value


def _linked_chain(length: int) -> _Linked:
    """A finite chain of `length` instances of a self-referencing model."""
    node = _Linked()
    for _ in range(length - 1):
        node = _Linked(next=node)
    return node


class _ValueErrorInComputedField(BaseModel):
    text: str

    @computed_field
    @property
    def derived(self) -> int:
        raise ValueError(self.text)


def _yield_then_raise_value_error() -> Iterator[int]:
    yield 1
    raise ValueError('defect in a generator')


class TestValueThatHoldsItself:
    """`dump_python` raises a plain `ValueError` for these values."""

    @pytest.mark.parametrize(
        ('make_value', 'expected_type'),
        [
            (_list_that_holds_itself, Any),
            (_dict_that_holds_itself, Any),
            (lambda: {'key': _list_that_holds_itself()}, dict[str, Any]),
            (lambda: [_list_that_holds_itself()], list[Any]),
            (_linked_to_itself, _Linked),
            (_linked_to_itself, Any),
        ],
        ids=[
            'list', 'dict', 'in-dict-value', 'in-list',
            'typed-model', 'model-at-any',
        ],
    )
    def test_it_is_a_catalog_refusal(
        self,
        make_value: Any,
        expected_type: Any,
    ) -> None:
        with pytest.raises(ValueValidationError) as raised:
            encode_value(make_value(), expected_type, location=('payload',))

        assert str(raised.value) == (
            'payload: serialization failed: '
            'Circular reference detected (id repeated)'
        )
        assert raised.value.__context__ is None


class TestValueThatIsTooDeep:
    """`dump_python` raises a plain `ValueError` for a value nested too deep.

    The text says circular, but the value is finite.
    """

    @pytest.mark.parametrize(
        ('value', 'expected_type'),
        [
            (_nested_list(300), Any),
            (_nested_dict(300), Any),
            ({'key': _nested_list(300)}, dict[str, Any]),
            (_linked_chain(300), _Linked),
            (_linked_chain(300), Any),
        ],
        ids=['list', 'dict', 'in-dict-value', 'typed-model', 'model-at-any'],
    )
    def test_it_is_a_catalog_refusal(
        self,
        value: object,
        expected_type: Any,
    ) -> None:
        with pytest.raises(ValueValidationError) as raised:
            encode_value(value, expected_type, location=('payload',))

        assert str(raised.value) == (
            'payload: serialization failed: '
            'Circular reference detected (depth exceeded)'
        )
        assert raised.value.__context__ is None

    @pytest.mark.parametrize(
        ('depth', 'accepted'),
        [(254, True), (255, False)],
        ids=['deepest-accepted', 'first-refused'],
    )
    def test_the_limit_is_the_limit_of_the_dump(
        self,
        depth: int,
        accepted: bool,
    ) -> None:
        match accepted:
            case True:
                assert encode_value(_nested_list(depth), Any) is not None
            case False:
                with pytest.raises(ValueValidationError):
                    encode_value(_nested_list(depth), Any)


class TestValueErrorThatStaysRaw:
    """Only the full text of a refusal is in the catalog."""

    @pytest.mark.parametrize(
        'text',
        [
            'defect in a computed field',
            'wrapped: Circular reference detected (id repeated)',
            'Circular reference detected (depth exceeded): wrapped',
            'Circular reference detected',
        ],
        ids=['other-text', 'text-in-front', 'text-behind', 'shared-prefix'],
    )
    def test_a_value_error_from_a_computed_field_propagates(self, text: str) -> None:
        with pytest.raises(ValueError) as raised:
            encode_value(
                _ValueErrorInComputedField(text=text), _ValueErrorInComputedField,
            )

        assert type(raised.value) is ValueError
        assert str(raised.value) == text

    def test_a_value_error_from_a_generator_propagates(self) -> None:
        with pytest.raises(ValueError) as raised:
            encode_value(_yield_then_raise_value_error(), Iterable[int])

        assert type(raised.value) is ValueError
        assert str(raised.value) == 'defect in a generator'

    def test_a_unicode_decode_error_propagates(self) -> None:
        """It is a `ValueError` subclass with another text."""
        with pytest.raises(UnicodeDecodeError):
            encode_value(b'\xff', Any)

    def test_the_subclass_refusals_keep_their_own_arms(self) -> None:
        """`PydanticSerializationError` and `UnicodeEncodeError` are a
        `ValueError` too. Their arms must come first."""
        with pytest.raises(ValueValidationError, match='Unable to serialize unknown type'):
            encode_value(object(), Any)
        with pytest.raises(StrictJsonError, match='a dict key holds a lone surrogate'):
            encode_value({LONE_SURROGATE_TEXT: 1}, Any)


class TestEncodeValueKeepsValidText:
    @pytest.mark.parametrize(
        ('value', 'expected_type'),
        [
            (chr(0x1f600), str),
            ('caf\u00e9', str),
            ('a\ufffdb', str),
            ({'caf\u00e9': 1}, dict[str, int]),
            ({'a\ufffdb': 1}, dict[str, int]),
            ({'a\ufffdb': 1}, JsonValue),
            ([{'a\ufffdb': {'b\ufffdc': 1}}], JsonValue),
        ],
        ids=[
            'astral', 'non-ascii', 'replacement-char', 'non-ascii-key',
            'replacement-char-key', 'replacement-char-json-value', 'nested',
        ],
    )
    def test_text_that_utf8_can_encode_is_kept(
        self,
        value: object,
        expected_type: Any,
    ) -> None:
        """U+FFFD in a key is only a sign: a key that truly holds it is kept."""
        encoded = encode_value(value, expected_type)

        assert encoded == value
        assert dumps_json(encoded).is_ok()


class TestScanWireJsonDirect:
    def test_accepts_valid_shape(self) -> None:
        _scan_wire_json({'a': 1, 'b': [2.5, None, 'x', True]})

    def test_a_key_without_the_replacement_char_gives_no_sign(self) -> None:
        assert _scan_wire_json({'caf\u00e9': [{'b': 'c\ufffd'}]}) is False

    @pytest.mark.parametrize(
        'dumped',
        [
            {'a\ufffd': 1},
            {'x': {'a\ufffd': 1}},
            [{'a\ufffd': 1}],
            ({'a\ufffd': 1},),
            collections.deque([{'a\ufffd': 1}]),
        ],
        ids=['dict', 'nested-dict', 'list', 'tuple', 'deque'],
    )
    def test_a_key_with_the_replacement_char_gives_the_sign(
        self, dumped: object,
    ) -> None:
        assert _scan_wire_json(dumped) is True

    @pytest.mark.parametrize(
        'dumped',
        [
            {'a\ufffd': 1, 'b': float('nan')},
            [{'a\ufffd': 1}, float('nan')],
        ],
        ids=['dict', 'list'],
    )
    def test_the_sign_does_not_stop_the_scan(self, dumped: object) -> None:
        with pytest.raises(StrictJsonError, match='non-RFC-8259 float'):
            _scan_wire_json(dumped)

    @pytest.mark.parametrize(
        'dumped',
        [
            (LONE_SURROGATE_TEXT,),
            {LONE_SURROGATE_TEXT},
            frozenset({LONE_SURROGATE_TEXT}),
            collections.deque([LONE_SURROGATE_TEXT]),
        ],
        ids=['tuple', 'set', 'frozenset', 'deque'],
    )
    def test_each_container_of_a_python_mode_dump_is_walked(
        self, dumped: object,
    ) -> None:
        with pytest.raises(StrictJsonError, match='a string holds a lone surrogate'):
            _scan_wire_json(dumped)

    def test_a_leaf_of_a_python_mode_dump_gives_no_sign(self) -> None:
        assert _scan_wire_json({'at': datetime.date(2026, 1, 1)}) is False

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
# Decode-side: ValueValidationError for type mismatch
# ---------------------------------------------------------------------------


class TestEncodeValidation:
    """`encode_value` validates before serializing.

    `dump_python` alone only serializes — a mistyped value (dict/int/list
    for a `str` slot) passes through with a `PydanticSerializationUnexpectedValue`
    warning instead of raising. Validating first makes the producer fail
    closed, symmetric with `decode_value` on the consumer.
    """

    def test_dict_for_str_raises(self) -> None:
        with pytest.raises(ValueValidationError):
            encode_value({'not': 'a string'}, str)

    def test_int_for_str_raises(self) -> None:
        with pytest.raises(ValueValidationError):
            encode_value(123, str)

    def test_list_for_str_raises(self) -> None:
        with pytest.raises(ValueValidationError):
            encode_value(['a', 'b'], str)

    def test_missing_required_model_field_raises(self) -> None:
        with pytest.raises(ValueValidationError):
            encode_value(cast(object, {'name': 'Alice'}), _User)

    def test_harmless_coercion_preserved(self) -> None:
        # Lax validation keeps the boundary coercions the wire already
        # round-trips: numeric-string to int, int to float.
        assert encode_value('5', int) == 5
        assert encode_value(5, float) == 5.0

    def test_task_error_instance_unaffected(self) -> None:
        # A model instance is not revalidated, so live TaskError encoding
        # is unchanged.
        from horsies.core.models.tasks import TaskError

        err = TaskError(error_code='MY_CODE', message='boom')
        encoded = cast(dict[str, object], encode_value(err, TaskError))
        assert encoded['error_code'] == 'MY_CODE'
        assert encoded['message'] == 'boom'


class TestDecodeValidation:
    def test_decode_type_mismatch_raises(self) -> None:
        with pytest.raises(ValueValidationError):
            decode_value(cast(Json, 'not-an-int'), int)

    def test_decode_missing_required_field_raises(self) -> None:
        with pytest.raises(ValueValidationError):
            decode_value(cast(Json, {'name': 'Alice'}), _User)


class TestDecodeReservedKeyScan:
    """Symmetric to the encode-side scan: cross-version / cross-language
    producers (the same threat model `_reject_nonstandard_json_constant`
    addresses on decode) could land reserved keys at user-controlled
    positions. The decoder must reject before TypeAdapter validates."""

    def test_decode_rejects_h_prefix_at_top_level(self) -> None:
        with pytest.raises(StrictJsonError, match='reserved key'):
            decode_value(
                cast(Json, {'__h_evil__': 1}),
                dict[str, JsonValue],
            )

    def test_decode_rejects_builtin_task_code_in_user_dict(self) -> None:
        with pytest.raises(StrictJsonError, match='reserved key'):
            decode_value(
                cast(Json, {'__builtin_task_code__': 'X'}),
                dict[str, JsonValue],
            )

    def test_decode_rejects_nested_reserved_key(self) -> None:
        with pytest.raises(StrictJsonError, match='reserved key'):
            decode_value(
                cast(Json, {'outer': [{'__h_inner__': 1}]}),
                dict[str, JsonValue],
            )

    def test_decode_accepts_clean_payload(self) -> None:
        decoded = decode_value(
            cast(Json, {'name': 'Alice', 'age': 30}),
            _User,
        )
        assert isinstance(decoded, _User)
        assert decoded.name == 'Alice'


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


# ---------------------------------------------------------------------------
# TaskResult envelope primitives — phase 5/6 regression coverage
# ---------------------------------------------------------------------------


class TestValidateTaskResultEnvelope:
    """Shape-only validation used by every err-fast / typed-decode path.

    A malformed envelope must fail closed *before* per-slot decoding so
    callers can't smuggle the wrong shape through the err-fast route.
    """

    def test_accepts_canonical_ok_envelope(self) -> None:
        envelope = validate_task_result_envelope(
            cast(Json, {'__h_task_result__': True, 'ok': 1, 'err': None}),
        )
        assert envelope.get('ok') == 1
        assert envelope.get('err') is None

    def test_accepts_canonical_err_envelope(self) -> None:
        envelope = validate_task_result_envelope(
            cast(Json, {
                '__h_task_result__': True,
                'ok': None,
                'err': {
                    'error_code': {'__builtin_task_code__': 'BROKER_ERROR'},
                    'message': 'm',
                    'data': None,
                    'exception': None,
                },
            }),
        )
        assert envelope.get('ok') is None
        assert isinstance(envelope.get('err'), dict)

    def test_rejects_non_dict(self) -> None:
        with pytest.raises(StrictJsonError):
            validate_task_result_envelope(cast(Json, 42))

    def test_rejects_missing_marker(self) -> None:
        with pytest.raises(StrictJsonError):
            validate_task_result_envelope(
                cast(Json, {'ok': 1, 'err': None}),
            )

    def test_rejects_marker_false(self) -> None:
        with pytest.raises(StrictJsonError):
            validate_task_result_envelope(
                cast(Json, {'__h_task_result__': False, 'ok': 1, 'err': None}),
            )

    def test_rejects_missing_ok_key(self) -> None:
        with pytest.raises(StrictJsonError):
            validate_task_result_envelope(
                cast(Json, {'__h_task_result__': True, 'err': None}),
            )

    def test_rejects_missing_err_key(self) -> None:
        with pytest.raises(StrictJsonError):
            validate_task_result_envelope(
                cast(Json, {'__h_task_result__': True, 'ok': 1}),
            )

    def test_rejects_both_slots_populated(self) -> None:
        with pytest.raises(StrictJsonError):
            validate_task_result_envelope(
                cast(Json, {
                    '__h_task_result__': True,
                    'ok': 1,
                    'err': {
                        'error_code': {'__builtin_task_code__': 'BROKER_ERROR'},
                        'message': 'm',
                    },
                }),
            )


class TestTaskResultEnvelopeRoundTrip:
    """`encode_task_result` / `decode_task_result` round-trip with shape
    enforcement. Covers the strict-serde phase 5 primitive used by
    worker persistence, engine emit, handle decode."""

    def test_ok_envelope_round_trips(self) -> None:
        from horsies.core.models.tasks import TaskError, TaskResult

        tr: TaskResult[int, TaskError] = TaskResult(ok=42)
        encoded = encode_task_result(tr, int)
        assert encoded == {
            '__h_task_result__': True,
            'ok': 42,
            'err': None,
        }
        decoded = decode_task_result(encoded, int)
        assert decoded.ok == 42
        assert decoded.err is None

    def test_decode_rejects_missing_marker(self) -> None:
        # User-visible regression of the strict shape check — a payload
        # that looks like an envelope but lacks the marker must fail.
        with pytest.raises(StrictJsonError):
            decode_task_result(cast(Json, {'ok': 1, 'err': None}), int)

    def test_decode_rejects_partial_envelope(self) -> None:
        # ``decode_task_result({"__h_task_result__": True}, type(None))``
        # previously returned ``ok=None`` instead of raising; the missing
        # ``ok`` / ``err`` keys must be a hard error.
        with pytest.raises(StrictJsonError):
            decode_task_result(
                cast(Json, {'__h_task_result__': True}),
                type(None),
            )

    def test_err_envelope_flattens_live_exception(self) -> None:
        """Live ``BaseException`` on ``TaskError.exception`` is flattened.

        Regression: ``task_decorator`` builds
        ``TaskResult(err=TaskError(exception=<live exc>, ...))`` for
        unhandled exceptions (and specifically for
        ``WorkflowContextMissingIdError``). The worker hands that
        TaskResult to ``encode_task_result`` directly; without the
        in-encoder flatten the pydantic serializer rejects the live
        exception with ``PydanticSerializationError`` and the wrapper
        folds the failure to ``OperationalErrorCode.TASK_EXCEPTION``,
        clobbering the producer's declared ``error_code``.
        """
        from horsies.core.models.tasks import TaskError, TaskResult

        err = TaskError(
            exception=ValueError('boom'),
            error_code='X',
            message='m',
        )
        tr: TaskResult[None, TaskError] = TaskResult(err=err)

        encoded = encode_task_result(tr, type(None))
        assert encoded['ok'] is None
        err_slot = cast('dict[str, Json]', encoded['err'])
        flat = err_slot['exception']
        assert isinstance(flat, dict)
        assert flat['type'] == 'ValueError'
        assert flat['message'] == 'boom'
        assert 'traceback' in flat
        assert err_slot['error_code'] == 'X'
        assert err_slot['message'] == 'm'

        decoded = decode_task_result(encoded, type(None))
        assert decoded.err is not None
        assert decoded.err.error_code == 'X'
        # Round-trip preserves the flattened dict shape (not a live exc).
        assert isinstance(decoded.err.exception, dict)
        assert decoded.err.exception['type'] == 'ValueError'


class TestDecodeTaskErrorPolymorphic:
    """`decode_task_error` must preserve SubWorkflowError subclass fields.

    Plain ``TypeAdapter(TaskError)`` would silently drop
    ``sub_workflow_id`` / ``sub_workflow_summary`` on read. The
    discriminator-by-payload routing keeps the err-slot useful for
    workflow code that pattern-matches on the subclass.
    """

    def test_plain_task_error_decodes(self) -> None:
        from horsies.core.models.tasks import (
            OperationalErrorCode,
            SubWorkflowError,
            TaskError,
        )

        payload: Json = cast(Json, {
            'error_code': {'__builtin_task_code__': 'BROKER_ERROR'},
            'message': 'plain',
            'data': None,
            'exception': None,
        })
        decoded = decode_task_error(payload)
        assert isinstance(decoded, TaskError)
        assert not isinstance(decoded, SubWorkflowError)
        assert decoded.error_code == OperationalErrorCode.BROKER_ERROR
        assert decoded.message == 'plain'

    def test_sub_workflow_error_preserves_subclass_fields(self) -> None:
        from horsies.core.models.tasks import (
            OperationalErrorCode,
            SubWorkflowError,
            TaskError,
        )
        from horsies.core.models.workflow.context import SubWorkflowSummary
        from horsies.core.models.workflow.enums import WorkflowStatus

        summary: SubWorkflowSummary[Any] = SubWorkflowSummary(
            status=WorkflowStatus.FAILED,
            output=None,
            total_tasks=3,
            completed_tasks=1,
            failed_tasks=2,
            skipped_tasks=0,
            error_summary='child boom',
        )
        original = SubWorkflowError(
            error_code=OperationalErrorCode.UNHANDLED_EXCEPTION,
            message='subworkflow failed',
            sub_workflow_id='wf-abc',
            sub_workflow_summary=summary,
        )

        encoded = encode_value(original, SubWorkflowError)
        decoded = decode_task_error(encoded)

        # Polymorphic routing must return the concrete subclass, not
        # the base TaskError.
        assert isinstance(decoded, SubWorkflowError)
        assert isinstance(decoded, TaskError)
        assert decoded.sub_workflow_id == 'wf-abc'
        assert decoded.sub_workflow_summary.status == WorkflowStatus.FAILED
        assert decoded.sub_workflow_summary.failed_tasks == 2
        assert decoded.sub_workflow_summary.error_summary == 'child boom'

    def test_sub_workflow_error_round_trips_via_engine_emit_shape(self) -> None:
        """End-to-end: SubWorkflowError survives the engine's emit path
        through the consumer-side decode used by WorkflowHandle.

        Mirrors `engine.on_subworkflow_complete`, which writes the err
        slot via ``error.model_dump(mode='json')``. The handle reads it
        back via ``validate_task_result_envelope`` + ``decode_task_error``.
        Locks the engine-shape contract here so a future refactor
        cannot silently break sub-workflow err propagation.

        Note: ``encode_task_result`` now also preserves the subclass
        (routes through ``encode_task_error``), so the engine's direct
        ``model_dump`` and the codec path are interchangeable; see
        ``test_encode_task_result_preserves_sub_workflow_error_subclass``.
        """
        from horsies.core.models.tasks import (
            OperationalErrorCode,
            SubWorkflowError,
        )
        from horsies.core.models.workflow.context import SubWorkflowSummary
        from horsies.core.models.workflow.enums import WorkflowStatus

        summary: SubWorkflowSummary[Any] = SubWorkflowSummary(
            status=WorkflowStatus.FAILED,
            output=None,
            total_tasks=1,
            completed_tasks=0,
            failed_tasks=1,
            skipped_tasks=0,
            error_summary='child failed',
        )
        original_err = SubWorkflowError(
            error_code=OperationalErrorCode.UNHANDLED_EXCEPTION,
            message='boom',
            sub_workflow_id='wf-xyz',
            sub_workflow_summary=summary,
        )
        # Mirror the engine: dump the err directly and wrap into the
        # envelope by hand. ``ok=None`` is the outputless shape; the
        # err slot carries the full subclass dump.
        wire: dict[str, Any] = {
            '__h_task_result__': True,
            'ok': None,
            'err': original_err.model_dump(mode='json'),
        }

        envelope = validate_task_result_envelope(cast(Json, wire))
        err_slot = envelope.get('err')
        assert isinstance(err_slot, dict)
        decoded = decode_task_error(cast(Json, err_slot))

        assert isinstance(decoded, SubWorkflowError)
        assert decoded.sub_workflow_id == 'wf-xyz'
        assert decoded.sub_workflow_summary.failed_tasks == 1
        assert decoded.sub_workflow_summary.error_summary == 'child failed'

    def test_encode_task_result_preserves_sub_workflow_error_subclass(self) -> None:
        """``encode_task_result`` routes through ``encode_task_error``
        so a ``TaskResult(err=SubWorkflowError(...))`` round-trips with
        ``sub_workflow_id`` / ``sub_workflow_summary`` intact.

        Regression: pre-fix the encode side called
        ``encode_value(err, TaskError)`` which silently stripped the
        subclass-only fields, breaking pattern matching on
        ``SubWorkflowError`` for any code path that round-tripped
        through ``encode_task_result`` (notably ``args_from`` /
        outputless-ctx envelopes and ``serialize_error_payload``).
        """
        from horsies.core.codec.typed import (
            decode_task_result,
            encode_task_result,
        )
        from horsies.core.models.tasks import (
            OperationalErrorCode,
            SubWorkflowError,
            TaskResult,
        )
        from horsies.core.models.workflow.context import SubWorkflowSummary
        from horsies.core.models.workflow.enums import WorkflowStatus

        summary: SubWorkflowSummary[Any] = SubWorkflowSummary(
            status=WorkflowStatus.FAILED,
            output=None,
            total_tasks=2,
            completed_tasks=0,
            failed_tasks=2,
            skipped_tasks=0,
            error_summary='both children boom',
        )
        original_err = SubWorkflowError(
            error_code=OperationalErrorCode.UNHANDLED_EXCEPTION,
            message='subworkflow failed',
            sub_workflow_id='wf-pq',
            sub_workflow_summary=summary,
        )
        envelope = encode_task_result(
            TaskResult(err=original_err),
            ok_type=int,
        )
        decoded = decode_task_result(cast(Json, envelope), ok_type=int)

        assert decoded.is_err()
        assert isinstance(decoded.err, SubWorkflowError)
        assert decoded.err.sub_workflow_id == 'wf-pq'
        assert decoded.err.sub_workflow_summary.status == WorkflowStatus.FAILED
        assert decoded.err.sub_workflow_summary.failed_tasks == 2
        assert decoded.err.sub_workflow_summary.error_summary == 'both children boom'
