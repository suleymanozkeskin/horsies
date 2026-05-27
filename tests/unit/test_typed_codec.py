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
    Any,
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
        slot via ``error.model_dump(mode='json')`` rather than
        ``encode_task_result`` so subclass fields are preserved on the
        wire. The handle reads them back via
        ``validate_task_result_envelope`` + ``decode_task_error``.

        The ``encode_task_result(_, TaskError)`` path deliberately strips
        subclass fields (statically typed at the base class) — that is
        why the engine bypasses it for sub-workflow failures. Locking
        the engine-shape contract here prevents a future refactor from
        silently breaking sub-workflow err propagation.
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
