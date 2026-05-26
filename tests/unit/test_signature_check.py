"""Unit tests for horsies.core.codec.signature_check.

Covers the strict signature validator per design-doc §2:

- Per-allowed-type acceptance.
- Per-rejected-type rejection (Any, object, bare containers, bytes, set,
  TypedDict, TypeVar, Path, Callable, ...).
- TaskResult[OkT, TaskError] return enforcement + custom-err rejection.
- BaseModel / dataclass recursive field walk; INTERNAL_CODEC_TYPES exempt.
- JsonValue position rules (boundary vs. generic-parameter).
- Discriminated union acceptance via Annotated[..., Field(discriminator=...)].
- *args / **kwargs rejection.
- Error message shape: task name, position, banned, fix, docs URL.
"""

from __future__ import annotations

import dataclasses
import datetime
import decimal
import enum
import pathlib
import uuid
from typing import (
    Annotated,
    Any,
    Callable,
    Generic,
    Literal,
    Optional,
    TypeAlias,
    TypeVar,
    TypedDict,
    Union,
)

import pytest
from pydantic import BaseModel, Field

from horsies.core.codec.json_value import JsonValue
from horsies.core.codec.signature_check import (
    INTERNAL_CODEC_TYPES,
    SignatureValidationError,
    check_task_signature,
)
from horsies.core.models.tasks import TaskError, TaskResult
from horsies.core.models.workflow.context import (
    SubWorkflowSummary,
    WorkflowContext,
    WorkflowMeta,
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


# `from __future__ import annotations` makes annotations string-lazy;
# `get_type_hints` evaluates them in module globalns. TypeVars and TypedDicts
# referenced from a test function MUST live at module scope to be resolvable.
_UnboundedTV = TypeVar('_UnboundedTV')
_BoundedTV = TypeVar('_BoundedTV', bound=BaseModel)


class _UserTypedDict(TypedDict):
    name: str


def _check(fn: Callable[..., Any], *, task_name: str = 'test_task') -> None:
    """Thin wrapper to reduce per-test boilerplate."""
    check_task_signature(fn, task_name=task_name)


# ---------------------------------------------------------------------------
# Allowed: primitives, datetime, UUID, Decimal, Enum
# ---------------------------------------------------------------------------


class TestAllowedScalars:
    def test_str_param(self) -> None:
        def f(x: str) -> TaskResult[int, TaskError]: ...
        _check(f)

    def test_int_param(self) -> None:
        def f(x: int) -> TaskResult[int, TaskError]: ...
        _check(f)

    def test_float_param(self) -> None:
        def f(x: float) -> TaskResult[int, TaskError]: ...
        _check(f)

    def test_bool_param(self) -> None:
        def f(x: bool) -> TaskResult[int, TaskError]: ...
        _check(f)

    def test_datetime_param(self) -> None:
        def f(x: datetime.datetime) -> TaskResult[int, TaskError]: ...
        _check(f)

    def test_date_param(self) -> None:
        def f(x: datetime.date) -> TaskResult[int, TaskError]: ...
        _check(f)

    def test_time_param(self) -> None:
        def f(x: datetime.time) -> TaskResult[int, TaskError]: ...
        _check(f)

    def test_uuid_param(self) -> None:
        def f(x: uuid.UUID) -> TaskResult[int, TaskError]: ...
        _check(f)

    def test_decimal_param(self) -> None:
        def f(x: decimal.Decimal) -> TaskResult[int, TaskError]: ...
        _check(f)

    def test_enum_param(self) -> None:
        def f(x: _Color) -> TaskResult[int, TaskError]: ...
        _check(f)

    def test_intenum_param(self) -> None:
        def f(x: _Status) -> TaskResult[int, TaskError]: ...
        _check(f)


# ---------------------------------------------------------------------------
# Allowed: BaseModel, dataclass, generic containers
# ---------------------------------------------------------------------------


class TestAllowedStructured:
    def test_basemodel_param(self) -> None:
        def f(u: _User) -> TaskResult[int, TaskError]: ...
        _check(f)

    def test_dataclass_param(self) -> None:
        def f(o: _Order) -> TaskResult[int, TaskError]: ...
        _check(f)

    def test_list_of_primitive(self) -> None:
        def f(xs: list[int]) -> TaskResult[int, TaskError]: ...
        _check(f)

    def test_dict_str_primitive(self) -> None:
        def f(d: dict[str, int]) -> TaskResult[int, TaskError]: ...
        _check(f)

    def test_tuple_variadic(self) -> None:
        def f(t: tuple[int, ...]) -> TaskResult[int, TaskError]: ...
        _check(f)

    def test_tuple_fixed_shape(self) -> None:
        def f(t: tuple[int, str, bool]) -> TaskResult[int, TaskError]: ...
        _check(f)

    def test_nested_containers(self) -> None:
        def f(xs: list[dict[str, list[int]]]) -> TaskResult[int, TaskError]: ...
        _check(f)


# ---------------------------------------------------------------------------
# Allowed: Optional, Annotated, Literal
# ---------------------------------------------------------------------------


class TestAllowedOptionalAnnotatedLiteral:
    def test_optional_primitive(self) -> None:
        def f(x: Optional[int]) -> TaskResult[int, TaskError]: ...
        _check(f)

    def test_pipe_none(self) -> None:
        def f(x: int | None) -> TaskResult[int, TaskError]: ...
        _check(f)

    def test_optional_model(self) -> None:
        def f(u: Optional[_User]) -> TaskResult[int, TaskError]: ...
        _check(f)

    def test_annotated_passthrough(self) -> None:
        def f(x: Annotated[int, 'positive']) -> TaskResult[int, TaskError]: ...
        _check(f)

    def test_literal_primitive(self) -> None:
        def f(x: Literal['a', 'b', 'c']) -> TaskResult[int, TaskError]: ...
        _check(f)

    def test_literal_int(self) -> None:
        def f(x: Literal[1, 2, 3]) -> TaskResult[int, TaskError]: ...
        _check(f)

    def test_literal_with_enum(self) -> None:
        def f(x: Literal[_Color.RED, _Color.GREEN]) -> TaskResult[int, TaskError]: ...
        _check(f)


# ---------------------------------------------------------------------------
# Allowed: primitive unions, discriminated unions
# ---------------------------------------------------------------------------


class TestAllowedUnions:
    def test_int_or_str(self) -> None:
        def f(x: int | str) -> TaskResult[int, TaskError]: ...
        _check(f)

    def test_int_or_str_or_none(self) -> None:
        def f(x: int | str | None) -> TaskResult[int, TaskError]: ...
        _check(f)

    def test_all_primitives_union(self) -> None:
        def f(x: bool | int | float | str) -> TaskResult[int, TaskError]: ...
        _check(f)

    def test_discriminated_union(self) -> None:
        def f(p: _Pet) -> TaskResult[int, TaskError]: ...
        _check(f)


# ---------------------------------------------------------------------------
# Allowed: TaskResult ok-slot recursion, JsonValue at boundary
# ---------------------------------------------------------------------------


class TestAllowedReturnAndJsonValue:
    def test_return_with_model_ok(self) -> None:
        def f() -> TaskResult[_User, TaskError]: ...
        _check(f)

    def test_return_with_list_ok(self) -> None:
        def f() -> TaskResult[list[int], TaskError]: ...
        _check(f)

    def test_return_with_jsonvalue_ok(self) -> None:
        def f() -> TaskResult[JsonValue, TaskError]: ...
        _check(f)

    def test_return_with_dict_jsonvalue_ok(self) -> None:
        def f() -> TaskResult[dict[str, JsonValue], TaskError]: ...
        _check(f)

    def test_return_with_list_jsonvalue_ok(self) -> None:
        def f() -> TaskResult[list[JsonValue], TaskError]: ...
        _check(f)

    def test_jsonvalue_param(self) -> None:
        def f(payload: JsonValue) -> TaskResult[int, TaskError]: ...
        _check(f)

    def test_jsonvalue_in_dict_param(self) -> None:
        def f(d: dict[str, JsonValue]) -> TaskResult[int, TaskError]: ...
        _check(f)

    def test_taskresult_as_kwarg(self) -> None:
        # Common workflow pattern: downstream consumes upstream's TaskResult.
        def f(upstream: TaskResult[_User, TaskError]) -> TaskResult[int, TaskError]: ...
        _check(f)


# ---------------------------------------------------------------------------
# Allowed: internal types as params
# ---------------------------------------------------------------------------


class TestInternalTypesExempt:
    def test_workflow_context_param(self) -> None:
        def f(ctx: WorkflowContext) -> TaskResult[int, TaskError]: ...
        _check(f)

    def test_workflow_meta_param(self) -> None:
        def f(meta: WorkflowMeta) -> TaskResult[int, TaskError]: ...
        _check(f)

    def test_internal_codec_types_contains_expected(self) -> None:
        assert TaskError in INTERNAL_CODEC_TYPES
        assert WorkflowContext in INTERNAL_CODEC_TYPES
        assert WorkflowMeta in INTERNAL_CODEC_TYPES
        assert SubWorkflowSummary in INTERNAL_CODEC_TYPES

    def test_internal_codec_types_does_not_contain_taskresult(self) -> None:
        # TaskResult is a structural codec primitive, NOT in INTERNAL_CODEC_TYPES.
        assert TaskResult not in INTERNAL_CODEC_TYPES


# ---------------------------------------------------------------------------
# Rejected: Any, object, bare containers, smuggled Any
# ---------------------------------------------------------------------------


class TestRejectedAnyObject:
    def test_any_param_rejected(self) -> None:
        def f(x: Any) -> TaskResult[int, TaskError]: ...
        with pytest.raises(SignatureValidationError, match='banned type `Any`'):
            _check(f)

    def test_object_param_rejected(self) -> None:
        def f(x: object) -> TaskResult[int, TaskError]: ...
        with pytest.raises(SignatureValidationError, match='banned type `object`'):
            _check(f)

    def test_any_in_return_ok_slot_rejected(self) -> None:
        def f() -> TaskResult[Any, TaskError]: ...
        with pytest.raises(SignatureValidationError, match='banned type `Any`'):
            _check(f)

    def test_dict_str_any_rejected(self) -> None:
        def f(d: dict[str, Any]) -> TaskResult[int, TaskError]: ...
        with pytest.raises(SignatureValidationError, match='banned type `Any`'):
            _check(f)

    def test_list_any_rejected(self) -> None:
        def f(xs: list[Any]) -> TaskResult[int, TaskError]: ...
        with pytest.raises(SignatureValidationError, match='banned type `Any`'):
            _check(f)

    def test_taskresult_dict_str_any_rejected(self) -> None:
        def f() -> TaskResult[dict[str, Any], TaskError]: ...
        with pytest.raises(SignatureValidationError, match='banned type `Any`'):
            _check(f)


class TestRejectedBareContainers:
    def test_bare_dict_rejected(self) -> None:
        def f(d: dict) -> TaskResult[int, TaskError]: ...  # pyright: ignore[reportMissingTypeArgument, reportUnknownParameterType]
        with pytest.raises(SignatureValidationError, match='bare dict'):
            _check(f)  # pyright: ignore[reportUnknownArgumentType]

    def test_bare_list_rejected(self) -> None:
        def f(xs: list) -> TaskResult[int, TaskError]: ...  # pyright: ignore[reportMissingTypeArgument, reportUnknownParameterType]
        with pytest.raises(SignatureValidationError, match='bare list'):
            _check(f)  # pyright: ignore[reportUnknownArgumentType]

    def test_bare_tuple_rejected(self) -> None:
        def f(t: tuple) -> TaskResult[int, TaskError]: ...  # pyright: ignore[reportMissingTypeArgument, reportUnknownParameterType]
        with pytest.raises(SignatureValidationError, match='bare tuple'):
            _check(f)  # pyright: ignore[reportUnknownArgumentType]

    def test_dict_nonstr_key_rejected(self) -> None:
        def f(d: dict[int, str]) -> TaskResult[int, TaskError]: ...
        with pytest.raises(
            SignatureValidationError,
            match='dict key type must be str',
        ):
            _check(f)


# ---------------------------------------------------------------------------
# Rejected: bytes, set, frozenset, Path, TypedDict, TypeVar, Callable
# ---------------------------------------------------------------------------


class TestRejectedExoticTypes:
    def test_bytes_rejected(self) -> None:
        def f(x: bytes) -> TaskResult[int, TaskError]: ...
        with pytest.raises(SignatureValidationError, match='bytes'):
            _check(f)

    def test_set_rejected(self) -> None:
        def f(xs: set[int]) -> TaskResult[int, TaskError]: ...
        with pytest.raises(SignatureValidationError, match='set'):
            _check(f)

    def test_frozenset_rejected(self) -> None:
        def f(xs: frozenset[int]) -> TaskResult[int, TaskError]: ...
        with pytest.raises(SignatureValidationError, match='frozenset'):
            _check(f)

    def test_path_rejected(self) -> None:
        def f(p: pathlib.Path) -> TaskResult[int, TaskError]: ...
        with pytest.raises(SignatureValidationError, match='filesystem-local'):
            _check(f)

    def test_purepath_rejected(self) -> None:
        def f(p: pathlib.PurePath) -> TaskResult[int, TaskError]: ...
        with pytest.raises(SignatureValidationError, match='filesystem-local'):
            _check(f)

    def test_typeddict_rejected(self) -> None:
        def f(u: _UserTypedDict) -> TaskResult[int, TaskError]: ...
        with pytest.raises(SignatureValidationError, match='TypedDict'):
            _check(f)

    def test_unbounded_typevar_rejected(self) -> None:
        def f(x: _UnboundedTV) -> TaskResult[int, TaskError]: ...  # pyright: ignore[reportInvalidTypeVarUse]
        with pytest.raises(SignatureValidationError, match='TypeVar'):
            _check(f)

    def test_bounded_typevar_rejected(self) -> None:
        def f(x: _BoundedTV) -> TaskResult[int, TaskError]: ...  # pyright: ignore[reportInvalidTypeVarUse]
        with pytest.raises(SignatureValidationError, match='TypeVar'):
            _check(f)

    def test_callable_rejected(self) -> None:
        def f(cb: Callable[[int], int]) -> TaskResult[int, TaskError]: ...
        with pytest.raises(SignatureValidationError, match='Callable'):
            _check(f)

    def test_bare_basemodel_rejected(self) -> None:
        def f(m: BaseModel) -> TaskResult[int, TaskError]: ...
        with pytest.raises(SignatureValidationError, match='bare'):
            _check(f)


# ---------------------------------------------------------------------------
# Rejected: union shapes
# ---------------------------------------------------------------------------


class TestRejectedUnions:
    def test_bare_model_union_rejected(self) -> None:
        def f(p: _Cat | _Dog) -> TaskResult[int, TaskError]: ...
        with pytest.raises(SignatureValidationError, match='discriminator'):
            _check(f)

    def test_mixed_union_model_int_rejected(self) -> None:
        def f(x: _User | int) -> TaskResult[int, TaskError]: ...
        with pytest.raises(SignatureValidationError, match='discriminator'):
            _check(f)

    def test_mixed_primitive_with_datetime_rejected(self) -> None:
        def f(x: datetime.datetime | str) -> TaskResult[int, TaskError]: ...
        with pytest.raises(SignatureValidationError):
            _check(f)


# ---------------------------------------------------------------------------
# Rejected: TaskResult return shape
# ---------------------------------------------------------------------------


class TestRejectedReturnShape:
    def test_non_taskresult_return_rejected(self) -> None:
        def f() -> int: ...
        with pytest.raises(
            SignatureValidationError,
            match='return type must be TaskResult',
        ):
            _check(f)

    def test_taskresult_wrong_err_type_rejected(self) -> None:
        def f() -> TaskResult[int, ValueError]: ...
        with pytest.raises(
            SignatureValidationError,
            match='error parameter must be TaskError',
        ):
            _check(f)

    def test_taskresult_with_object_err_type_rejected(self) -> None:
        def f() -> TaskResult[int, Exception]: ...
        with pytest.raises(SignatureValidationError):
            _check(f)


# ---------------------------------------------------------------------------
# Variadics with concrete element types are accepted; element type is walked.
# ---------------------------------------------------------------------------


class TestVariadics:
    def test_var_positional_with_concrete_type_accepted(self) -> None:
        def f(*values: int) -> TaskResult[int, TaskError]: ...
        _check(f)

    def test_var_keyword_with_concrete_type_accepted(self) -> None:
        def f(**values: int) -> TaskResult[int, TaskError]: ...
        _check(f)

    def test_var_positional_with_banned_element_rejected(self) -> None:
        def f(*values: Any) -> TaskResult[int, TaskError]: ...
        with pytest.raises(SignatureValidationError, match='Any'):
            _check(f)

    def test_var_keyword_with_banned_element_rejected(self) -> None:
        def f(**values: Any) -> TaskResult[int, TaskError]: ...
        with pytest.raises(SignatureValidationError, match='Any'):
            _check(f)


# ---------------------------------------------------------------------------
# BaseModel/dataclass recursive walk catches smuggled Any
# ---------------------------------------------------------------------------


class _ModelWithAnyField(BaseModel):
    name: str
    payload: Any


@dataclasses.dataclass
class _DataclassWithAnyField:
    name: str
    payload: Any


class _ModelWithDictAnyField(BaseModel):
    metadata: dict[str, Any]


class TestRecursiveFieldWalk:
    def test_basemodel_field_with_any_rejected(self) -> None:
        def f(m: _ModelWithAnyField) -> TaskResult[int, TaskError]: ...
        with pytest.raises(
            SignatureValidationError,
            match="BaseModel field '_ModelWithAnyField.payload'",
        ):
            _check(f)

    def test_dataclass_field_with_any_rejected(self) -> None:
        def f(m: _DataclassWithAnyField) -> TaskResult[int, TaskError]: ...
        with pytest.raises(
            SignatureValidationError,
            match="dataclass field '_DataclassWithAnyField.payload'",
        ):
            _check(f)

    def test_basemodel_field_dict_str_any_rejected(self) -> None:
        def f(m: _ModelWithDictAnyField) -> TaskResult[int, TaskError]: ...
        with pytest.raises(SignatureValidationError, match='Any'):
            _check(f)

    def test_taskerror_walk_skipped(self) -> None:
        # TaskError has `data: Optional[Any]` etc. — only safe because it's
        # exempt. If the walk weren't skipped, this would fail.
        def f(err: TaskError) -> TaskResult[int, TaskError]: ...
        _check(f)

    def test_workflow_context_walk_skipped(self) -> None:
        def f(ctx: WorkflowContext) -> TaskResult[int, TaskError]: ...
        _check(f)


# ---------------------------------------------------------------------------
# JsonValue position rules
# ---------------------------------------------------------------------------


_T = TypeVar('_T')


class _UserContainer(BaseModel, Generic[_T]):
    payload: _T


class TestJsonValuePositions:
    def test_jsonvalue_as_user_generic_param_rejected(self) -> None:
        # JsonValue inside a user-defined generic — banned by §3.
        # Either the JsonValue boundary check fires (if the generic-arg loop
        # catches it first) or the TypeVar field walk fires when the model
        # field's unsubstituted `payload: _T` is inspected — both are valid
        # rejections of the same misuse.
        def f(c: _UserContainer[JsonValue]) -> TaskResult[int, TaskError]: ...
        with pytest.raises(SignatureValidationError):
            _check(f)


# ---------------------------------------------------------------------------
# Error message shape
# ---------------------------------------------------------------------------


class TestErrorMessageShape:
    def test_error_names_task_position_banned_fix_docs(self) -> None:
        def f(x: Any) -> TaskResult[int, TaskError]: ...
        with pytest.raises(SignatureValidationError) as exc_info:
            check_task_signature(f, task_name='my_task')
        msg = str(exc_info.value)
        assert "task 'my_task'" in msg
        assert "parameter 'x'" in msg
        assert 'banned type `Any`' in msg
        assert 'Fix:' in msg
        assert 'See:' in msg
        assert 'strict-serde' in msg

    def test_error_for_return_names_return_position(self) -> None:
        def f() -> TaskResult[Any, TaskError]: ...
        with pytest.raises(SignatureValidationError) as exc_info:
            check_task_signature(f, task_name='my_task')
        assert 'return' in str(exc_info.value).lower()

    def test_error_for_banned_var_positional_element_names_param(self) -> None:
        def f(*args: Any) -> TaskResult[int, TaskError]: ...
        with pytest.raises(SignatureValidationError) as exc_info:
            check_task_signature(f, task_name='my_task')
        assert "parameter 'args'" in str(exc_info.value)


# ---------------------------------------------------------------------------
# Missing annotations — handled by existing checks, NOT this validator
# ---------------------------------------------------------------------------


class TestMissingAnnotationsDeferred:
    def test_missing_param_annotation_skipped(self) -> None:
        # Existing `TASK_PARAM_NO_TYPE` check handles missing annotation.
        # Our validator must not double-error on it.
        def f(x) -> TaskResult[int, TaskError]: ...  # pyright: ignore[reportUnknownParameterType, reportMissingParameterType]
        _check(f)  # pyright: ignore[reportUnknownArgumentType]

    def test_missing_return_annotation_skipped(self) -> None:
        def f(x: int): ...  # pyright: ignore[reportUnknownParameterType, reportMissingReturnType]
        _check(f)  # pyright: ignore[reportUnknownArgumentType]


