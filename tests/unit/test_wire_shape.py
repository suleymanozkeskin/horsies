"""The definition check: what `@app.task` and a `SubWorkflowNode` refuse.

A shape can be spelled in many ways. Each rule is tested on each spelling, and on
the valid shapes beside it, so that a rule does not refuse too much.
"""

from __future__ import annotations

import dataclasses
import datetime
import decimal
import enum
import json
import types
from typing import Annotated, Any, ClassVar, Generic, Literal, Optional, TypeVar
from unittest.mock import MagicMock, patch

import annotated_types
import pydantic.dataclasses as pydantic_dataclasses
import pytest
from pydantic import (
    AfterValidator,
    AliasChoices,
    AliasPath,
    Base64Str,
    BaseModel,
    BeforeValidator,
    ConfigDict,
    Field,
    GetPydanticSchema,
    Json,
    PlainSerializer,
    PlainValidator,
    PositiveInt,
    RootModel,
    SerializeAsAny,
    SkipValidation,
    StringConstraints,
    UUID4,
    WrapSerializer,
    WrapValidator,
    computed_field,
    field_serializer,
    field_validator,
    model_validator,
)
from pydantic.alias_generators import to_camel
from pydantic_core import core_schema

from horsies import (
    AppConfig,
    Horsies,
    JsonValue,
    PostgresConfig,
    TaskError,
    TaskResult,
)
from horsies.core.codec import wire_shape
from horsies.core.codec.kwargs import (
    decode_subworkflow_kwargs,
    fill_omitted_defaults,
    resolve_producer_kwarg_binding,
    resolve_hints_and_sig,
)
from horsies.core.codec.signature_check import (
    INTERNAL_CODEC_TYPES,
    SignatureValidationError,
    check_task_signature,
)
from horsies.core.codec.validation_errors import ValueValidationError
from horsies.core.codec.wire_shape import (
    ShapeRule,
    parameter_default_refusal,
    shape_refusals,
    verify_shape_rules,
)
from horsies.core.errors import ErrorCode, WorkflowValidationError
from horsies.core.models.payload import PayloadPolicy
from horsies.core.models.task_send_types import TaskSendValidationFailed
from horsies.core.models.workflow import (
    SubWorkflowNode,
    TaskNode,
    WorkflowContext,
    WorkflowDefinition,
    WorkflowSpec,
)
from horsies.core.models.schedule import IntervalSchedule, TaskSchedule
from horsies.core.scheduler.validation import _encode_schedule_kwargs
from horsies.core.task_decorator import create_task_wrapper
from horsies.core.types.result import is_err, is_ok
from horsies.core.worker import child_runner
from horsies.core.worker.task_loop import TaskLoop
from tests.fixtures.run_return import wire_of

Positive = Annotated[int, Field(gt=0)]
Upper = Annotated[str, AfterValidator(str.upper)]
T = TypeVar('T')


def _refuse(value: int) -> int:
    raise ValueError('no')


_Refused = Annotated[int, AfterValidator(_refuse)]


def _rules(annotation: object) -> set[ShapeRule]:
    return {
        refusal.rule
        for refusal in shape_refusals(annotation, internal_types=INTERNAL_CODEC_TYPES)
    }


def _signature_refuses(annotation: object) -> bool:
    def task(*, x: Any) -> TaskResult[int, TaskError]:
        return TaskResult(ok=1)

    task.__annotations__ = {'x': annotation, 'return': TaskResult[int, TaskError]}
    try:
        check_task_signature(task, task_name='task')
    except SignatureValidationError:
        return True
    return False


class Inner(BaseModel):
    n: int = 1


class Box(BaseModel, Generic[T]):
    item: T


class Color(enum.Enum):
    RED = 'red'


class Text(str, enum.Enum):
    A = 'a'


class Number(enum.IntEnum):
    A = 1


class ExcludeField(BaseModel):
    secret: str = Field(default='d', exclude=True)


class ExcludeRequired(BaseModel):
    secret: str = Field(exclude=True)


class ExcludeAnnotated(BaseModel):
    secret: Annotated[str, Field(exclude=True)] = 'd'


class ExcludeInherited(ExcludeField):
    other: int = 0


class ExcludeIf(BaseModel):
    note: str = Field(default='d', exclude_if=lambda value: value == 'hide')


class ExcludeNested(BaseModel):
    inner: ExcludeField


class ExcludeBeforeValidator(BaseModel):
    secret: str = Field(default='d', exclude=True)

    @model_validator(mode='before')
    @classmethod
    def read(cls, value: object) -> object:
        return value


class ExcludeAfterValidator(BaseModel):
    secret: str = Field(default='d', exclude=True)

    @model_validator(mode='after')
    def check(self) -> ExcludeAfterValidator:
        return self


class ExcludeRecursive(BaseModel):
    secret: str = Field(default='d', exclude=True)
    child: Optional['ExcludeRecursive'] = None


@dataclasses.dataclass
class StdExcludeDefault:
    secret: str = Field(default='d', exclude=True)


@dataclasses.dataclass
class StdExcludeAnnotated:
    secret: Annotated[str, Field(exclude=True)] = 'd'


@pydantic_dataclasses.dataclass
class PydExclude:
    secret: str = Field(default='d', exclude=True)


class Open(BaseModel):
    model_config = ConfigDict(extra='allow')
    n: int = 1


class OpenKeyword(BaseModel, extra='allow'):
    n: int = 1


class OpenPlainConfig(BaseModel):
    model_config = {'extra': 'allow'}
    n: int = 1


class OpenInherited(Open):
    m: int = 0


class OpenTyped(BaseModel):
    model_config = ConfigDict(extra='allow')
    __pydantic_extra__: dict[str, int] = Field(init=False)
    n: int = 1


class OpenNested(BaseModel):
    inner: Open


class OpenDeferred(BaseModel):
    model_config = ConfigDict(defer_build=True, extra='allow')
    n: int = 1


@dataclasses.dataclass
class StdOpen:
    __pydantic_config__ = ConfigDict(extra='allow')
    n: int = 1


@pydantic_dataclasses.dataclass(config=ConfigDict(extra='allow'))
class PydOpen:
    n: int = 1


@dataclasses.dataclass
class StdRevalidateAlways:
    __pydantic_config__ = ConfigDict(revalidate_instances='always')
    n: int = 1


@dataclasses.dataclass
class StdRevalidateSubclass:
    __pydantic_config__ = ConfigDict(revalidate_instances='subclass-instances')
    n: int = 1


@pydantic_dataclasses.dataclass(config=ConfigDict(revalidate_instances='always'))
class PydRevalidate:
    n: int = 1


class Alias(BaseModel):
    n: int = Field(alias='count')


class ValidationAlias(BaseModel):
    n: int = Field(validation_alias='count')


class Camel(BaseModel):
    model_config = ConfigDict(alias_generator=to_camel)
    user_id: int


class Pathed(BaseModel):
    n: int = Field(validation_alias=AliasPath('a', 0))


class BothAliases(BaseModel):
    n: int = Field(validation_alias='c', serialization_alias='c')


class AliasBehindValidator(BaseModel):
    n: int = Field(alias='c')

    @model_validator(mode='before')
    @classmethod
    def read(cls, value: object) -> object:
        return value


class AliasWritten(BaseModel):
    model_config = ConfigDict(serialize_by_alias=True)
    n: int = Field(alias='count')


class AliasName(BaseModel):
    n: int = Field(alias='n')


class CamelOneWord(BaseModel):
    model_config = ConfigDict(alias_generator=to_camel)
    count: int


class CamelWritten(BaseModel):
    model_config = ConfigDict(serialize_by_alias=True, alias_generator=to_camel)
    user_id: int


class CamelByName(BaseModel):
    model_config = ConfigDict(alias_generator=to_camel, populate_by_name=True)
    user_id: int


class ByNameOnly(BaseModel):
    model_config = ConfigDict(validate_by_name=True, validate_by_alias=False)
    n: int = Field(alias='c')


class ChoicesWithName(BaseModel):
    n: int = Field(validation_alias=AliasChoices('n', 'count'))


class ChoicesWithPath(BaseModel):
    n: int = Field(validation_alias=AliasChoices('n', AliasPath('a', 0)))


class ForbidComputed(BaseModel):
    model_config = ConfigDict(extra='forbid')
    n: int = 1

    @computed_field
    @property
    def double(self) -> int:
        return self.n * 2


class Computed(BaseModel):
    n: int = 1

    @computed_field
    @property
    def double(self) -> int:
        return self.n * 2


class ComputedAny(BaseModel):
    n: int = 1

    @computed_field
    @property
    def extra(self) -> dict[str, Any]:
        return {}


class PlainField(BaseModel):
    n: Annotated[int, PlainValidator(lambda value: value)]


class PlainDecorator(BaseModel):
    n: int

    @field_validator('n', mode='plain')
    @classmethod
    def read(cls, value: object) -> object:
        return value


class WrapDecorator(BaseModel):
    n: int

    @field_validator('n', mode='wrap')
    @classmethod
    def read(cls, value: object, handler: Any) -> object:
        return handler(value)


class WrapModel(BaseModel):
    n: int = 1

    @model_validator(mode='wrap')
    @classmethod
    def read(cls, value: object, handler: Any) -> Any:
        return handler(value)


class SkipField(BaseModel):
    n: SkipValidation[int]


class Serialized(BaseModel):
    when: datetime.datetime

    @field_serializer('when')
    def dump(self, value: datetime.datetime) -> str:
        return value.isoformat()


class JsonField(BaseModel):
    data: JsonValue = None

    @field_serializer('data')
    def dump(self, value: JsonValue) -> JsonValue:
        return value


class Validated(BaseModel):
    n: int = 1

    @field_validator('n', mode='after')
    @classmethod
    def check(cls, value: int) -> int:
        return value

    @model_validator(mode='after')
    def whole(self) -> Validated:
        return self


class BeforeField(BaseModel):
    n: int = 1

    @field_validator('n', mode='before')
    @classmethod
    def read(cls, value: object) -> object:
        return value


class Tree(BaseModel):
    value: int = 0
    child: Optional['Tree'] = None


class KindA(BaseModel):
    kind: Literal['a']


class KindB(BaseModel):
    kind: Literal['b']


Tagged = Annotated[KindA | KindB, Field(discriminator='kind')]


@dataclasses.dataclass
class Plain:
    n: int = 1


@pydantic_dataclasses.dataclass
class PydPlain:
    n: int = 1


class ValueThatLooksLikeANode(BaseModel):
    json_like: dict[str, str] = {'type': 'json'}
    any_like: dict[str, str] = {'type': 'any'}
    fields_like: dict[str, str] = {'type': 'model-fields'}
    literal_like: dict[str, str] = {'type': 'literal'}
    list_like: list[dict[str, str]] = [{'type': 'function-plain'}]
    extra_like: int = Field(default=1, json_schema_extra={'type': 'any'})
    examples_like: int = Field(default=1, examples=[{'type': 'function-plain'}])


class FieldNamedType(BaseModel):
    type: str = 'x'
    fields: int = 0


@pytest.mark.unit
class TestRefusedShapes:
    @pytest.mark.parametrize(
        'annotation',
        [
            ExcludeField,
            ExcludeRequired,
            ExcludeAnnotated,
            ExcludeInherited,
            ExcludeIf,
            ExcludeNested,
            ExcludeBeforeValidator,
            ExcludeAfterValidator,
            ExcludeRecursive,
            list[ExcludeField],
            Box[ExcludeField],
            StdExcludeDefault,
            StdExcludeAnnotated,
            PydExclude,
        ],
    )
    def test_an_excluded_field(self, annotation: object) -> None:
        assert ShapeRule.EXCLUDED_FIELD in _rules(annotation)

    @pytest.mark.parametrize(
        'annotation',
        [
            Open,
            OpenKeyword,
            OpenPlainConfig,
            OpenInherited,
            OpenTyped,
            OpenNested,
            OpenDeferred,
            list[Open],
            Optional[Open],
            Box[Open],
            RootModel[dict[str, Open]],
            StdOpen,
            PydOpen,
        ],
    )
    def test_extra_allow(self, annotation: object) -> None:
        assert ShapeRule.EXTRA_ALLOW in _rules(annotation)

    @pytest.mark.parametrize(
        'annotation',
        [StdRevalidateAlways, StdRevalidateSubclass, PydRevalidate],
    )
    def test_a_dataclass_that_revalidates(self, annotation: object) -> None:
        assert ShapeRule.DATACLASS_REVALIDATION in _rules(annotation)

    @pytest.mark.parametrize(
        'annotation',
        [Alias, ValidationAlias, Camel, Pathed, BothAliases, AliasBehindValidator],
    )
    def test_a_written_key_that_the_read_does_not_accept(
        self,
        annotation: object,
    ) -> None:
        assert ShapeRule.UNREADABLE_KEY in _rules(annotation)

    def test_a_computed_field_with_extra_forbid(self) -> None:
        assert ShapeRule.COMPUTED_WITH_FORBID in _rules(ForbidComputed)

    def test_json(self) -> None:
        assert ShapeRule.JSON_STRING_TYPE in _rules(Json[int])

    def test_literal_with_a_plain_enum(self) -> None:
        assert ShapeRule.PLAIN_ENUM_LITERAL in _rules(Literal[Color.RED])

    @pytest.mark.parametrize(
        'annotation',
        [
            Annotated[int, PlainValidator(lambda value: value)],
            PlainField,
            PlainDecorator,
            Annotated[int, WrapValidator(lambda value, handler: handler(value))],
            WrapDecorator,
            WrapModel,
        ],
    )
    def test_a_validator_that_replaces_the_check(self, annotation: object) -> None:
        assert ShapeRule.VALIDATOR_REPLACES_CHECK in _rules(annotation)

    @pytest.mark.parametrize(
        'annotation',
        [SkipValidation[int], SkipField, list[SkipValidation[int]], Any],
    )
    def test_a_position_with_no_type(self, annotation: object) -> None:
        assert ShapeRule.NO_TYPE in _rules(annotation)


@pytest.mark.unit
class TestValidShapes:
    @pytest.mark.parametrize(
        'annotation',
        [
            int,
            Positive,
            decimal.Decimal,
            datetime.datetime,
            Color,
            Optional[int],
            list[int],
            JsonValue,
            dict[str, JsonValue],
            Inner,
            Box[Inner],
            Box[int],
            Tree,
            Tagged,
            Plain,
            PydPlain,
            Computed,
            ComputedAny,
            Serialized,
            JsonField,
            Validated,
            BeforeField,
            Upper,
            Annotated[int, BeforeValidator(lambda value: value)],
            Annotated[int, PlainSerializer(str, return_type=str)],
            Annotated[int, WrapSerializer(lambda value, handler: handler(value))],
            AliasWritten,
            AliasName,
            CamelOneWord,
            CamelWritten,
            CamelByName,
            ByNameOnly,
            ChoicesWithName,
            ChoicesWithPath,
            Literal[Text.A],
            Literal[Number.A],
            Literal['a'],
            SerializeAsAny[Inner],
            UUID4,
            Base64Str,
            PositiveInt,
            Annotated[str, StringConstraints(max_length=3)],
            Annotated[int, annotated_types.Gt(0)],
            ValueThatLooksLikeANode,
            FieldNamedType,
            TaskError,
            list[TaskError],
        ],
    )
    def test_no_refusal(self, annotation: object) -> None:
        assert _rules(annotation) == set()


class _Shared(BaseModel):
    n: int = 1

    @model_validator(mode='after')
    def check(self) -> _Shared:
        return self


class _SharedRecursive(BaseModel):
    n: int = 1
    child: Optional['_SharedRecursive'] = None

    @model_validator(mode='after')
    def check(self) -> _SharedRecursive:
        return self


class _SharedTwice(BaseModel):
    first: _Shared
    second: _Shared


class _SharedOptional(BaseModel):
    first: Optional[_Shared] = None
    second: Optional[_Shared] = None


@pydantic_dataclasses.dataclass
class _RecursiveDataclass:
    n: int = 1
    child: Optional['_RecursiveDataclass'] = None

    @model_validator(mode='after')
    def check(self) -> _RecursiveDataclass:
        return self


@pytest.mark.unit
class TestAfterValidatorOnASharedModel:
    @pytest.mark.parametrize(
        'annotation',
        [_SharedRecursive, _SharedTwice, _SharedOptional, _RecursiveDataclass],
    )
    def test_it_registers(self, annotation: object) -> None:
        assert _rules(annotation) == set()
        assert _signature_refuses(annotation) is False

    def test_a_wrong_default_beside_it_is_refused(self) -> None:
        class Holder(BaseModel):
            first: _Shared
            second: _Shared
            n: int = 'x'  # type: ignore[assignment]

        assert ShapeRule.INVALID_DEFAULT in _rules(Holder)


@pytest.mark.unit
class TestFieldDefaults:
    def test_a_wrong_type_is_refused(self) -> None:
        class Wrong(BaseModel):
            n: int = 'x'  # type: ignore[assignment]

        assert ShapeRule.INVALID_DEFAULT in _rules(Wrong)

    def test_none_for_a_required_type_is_refused(self) -> None:
        class NoneDefault(BaseModel):
            n: int = None  # type: ignore[assignment]

        assert ShapeRule.INVALID_DEFAULT in _rules(NoneDefault)

    def test_a_constraint_on_the_default_is_refused(self) -> None:
        class Below(BaseModel):
            n: Positive = 0

        assert ShapeRule.INVALID_DEFAULT in _rules(Below)

    def test_a_dataclass_default_is_refused(self) -> None:
        @dataclasses.dataclass
        class Wrong:
            n: int = 'x'  # type: ignore[assignment]

        assert ShapeRule.INVALID_DEFAULT in _rules(Wrong)

    def test_a_config_rule_applies(self) -> None:
        class Short(BaseModel):
            model_config = ConfigDict(str_max_length=2)
            s: str = 'abc'

        assert ShapeRule.INVALID_DEFAULT in _rules(Short)

    def test_a_cross_field_validator_runs_no_user_code(self) -> None:
        calls: list[str] = []

        class Range(BaseModel):
            start: int = 0
            end: int = 10

            @field_validator('end')
            @classmethod
            def check(cls, value: int, info: Any) -> int:
                calls.append('ran')
                assert info.data['start'] <= value
                return value

        assert _rules(Range) == set()
        assert calls == []

    def test_a_dict_default_for_a_model_runs_no_user_code(self) -> None:
        calls: list[str] = []

        class Nested(BaseModel):
            n: int

            @field_validator('n')
            @classmethod
            def check(cls, value: int) -> int:
                calls.append('ran')
                raise KeyError('service down')

        class Holder(BaseModel):
            inner: Nested = {'n': 1}  # type: ignore[assignment]

        assert ShapeRule.INVALID_DEFAULT in _rules(Holder)
        assert calls == []

    def test_a_plain_validator_on_a_default_runs_no_user_code(self) -> None:
        calls: list[str] = []

        def boom(value: object) -> object:
            calls.append('ran')
            raise KeyError('service down')

        class Holder(BaseModel):
            n: Annotated[int, PlainValidator(boom)] = 1

        assert ShapeRule.VALIDATOR_REPLACES_CHECK in _rules(Holder)
        assert calls == []

    def test_validate_default_is_left_to_pydantic(self) -> None:
        class Parsed(BaseModel):
            items: Annotated[
                list[str],
                BeforeValidator(lambda value: value.split(',')),
            ] = Field(default='a,b', validate_default=True)

        assert _rules(Parsed) == set()

    def test_a_factory_is_not_called(self) -> None:
        calls: list[str] = []

        def factory() -> int:
            calls.append('ran')
            return 1

        class Factory(BaseModel):
            n: int = Field(default_factory=factory)

        assert _rules(Factory) == set()
        assert calls == []

    def test_instances_and_self_reference_pass(self) -> None:
        class Good(BaseModel):
            inner: Inner = Inner()
            items: list[Inner] = [Inner()]
            maybe: Optional[Inner] = None
            plain: Plain = Plain()
            when: Optional[datetime.datetime] = None
            color: Color = Color.RED
            price: decimal.Decimal = decimal.Decimal('1.50')

        assert _rules(Good) == set()
        assert _rules(Tree) == set()


@pytest.mark.unit
class TestParameterDefaults:
    @pytest.mark.parametrize(
        ('annotation', 'default'),
        [
            (int, 'x'),
            (int, None),
            (int, 1.0),
            (int, True),
            (datetime.datetime, '2026-01-01'),
            (Color, 'red'),
            (tuple[str, ...], []),
            (Positive, 0),
            (int, Field(default=5)),
            (Inner, {}),
            (JsonValue, (1, 2)),
        ],
    )
    def test_refused(self, annotation: object, default: object) -> None:
        refusal = parameter_default_refusal(annotation, default)

        assert refusal is not None
        assert refusal.rule is ShapeRule.INVALID_DEFAULT

    @pytest.mark.parametrize(
        ('annotation', 'default'),
        [
            (int, 1),
            (float, 1),
            (float, float('inf')),
            (float, float('nan')),
            (Optional[int], None),
            (Positive, 5),
            (Color, Color.RED),
            (Literal['a'], 'a'),
            (str, Text.A),
            (decimal.Decimal, decimal.Decimal('1.50')),
            (datetime.datetime, datetime.datetime(2026, 1, 1)),
            (list[int], []),
            (Inner, Inner()),
            (Plain, Plain()),
            (JsonValue, None),
            (JsonValue, {'a': 1}),
        ],
    )
    def test_accepted(self, annotation: object, default: object) -> None:
        assert parameter_default_refusal(annotation, default) is None

    def test_a_transforming_type_holds_its_type_and_runs_no_validator(self) -> None:
        calls: list[str] = []

        def upper(value: str) -> str:
            calls.append('ran')
            return value.upper()

        annotation = Annotated[str, AfterValidator(upper)]

        assert parameter_default_refusal(annotation, 'abc') is None
        assert calls == []

    def test_at_task_definition(self) -> None:
        def task(*, n: int = 'x') -> TaskResult[int, TaskError]:  # type: ignore[assignment]
            return TaskResult(ok=n)

        with pytest.raises(SignatureValidationError, match='default'):
            check_task_signature(task, task_name='task')

    def test_workflow_ctx_and_args_from_slots_are_exempt(self) -> None:
        def task(
            *,
            workflow_ctx: WorkflowContext | None = None,
            value: TaskResult[int, TaskError] | None = None,
        ) -> TaskResult[int, TaskError]:
            return TaskResult(ok=1)

        check_task_signature(task, task_name='task')


class _HookMarker:
    @classmethod
    def __get_pydantic_core_schema__(cls, source: Any, handler: Any) -> Any:
        return core_schema.str_schema()


class _HookSubclass(_HookMarker):
    pass


class _HookInstance:
    def __init__(self) -> None:
        self.__get_pydantic_core_schema__ = (
            lambda source, handler: core_schema.str_schema()
        )


@dataclasses.dataclass
class _HookDataclass:
    n: int = 1

    @classmethod
    def __get_pydantic_core_schema__(cls, source: Any, handler: Any) -> Any:
        return core_schema.str_schema()


class _HookEnum(enum.Enum):
    A = 'a'

    @classmethod
    def __get_pydantic_core_schema__(cls, source: Any, handler: Any) -> Any:
        return core_schema.str_schema()


class _HookModel(BaseModel):
    n: int = 1

    @classmethod
    def __get_pydantic_core_schema__(cls, source: Any, handler: Any) -> Any:
        return core_schema.any_schema()


class _HookInField(BaseModel):
    n: Annotated[int, _HookMarker]


class _HookInNested(BaseModel):
    item: _HookInField


@dataclasses.dataclass
class _HookInDataclass:
    n: Annotated[int, _HookMarker()] = 1


@pytest.mark.unit
class TestSchemaHooks:
    @pytest.mark.parametrize(
        'annotation',
        [
            Annotated[int, GetPydanticSchema(lambda s, h: core_schema.str_schema())],
            Annotated[int, _HookMarker],
            Annotated[int, _HookMarker()],
            Annotated[int, _HookSubclass],
            Annotated[int, _HookInstance()],
            Annotated[
                int,
                types.SimpleNamespace(
                    __get_pydantic_core_schema__=lambda s, h: core_schema.str_schema(),
                ),
            ],
            _HookDataclass,
            _HookEnum,
            _HookModel,
            _HookInField,
            _HookInNested,
            _HookInDataclass,
            list[Annotated[int, _HookMarker]],
            Optional[Annotated[int, _HookMarker]],
        ],
    )
    def test_refused(self, annotation: object) -> None:
        assert _signature_refuses(annotation) is True

    @pytest.mark.parametrize(
        'annotation',
        [
            UUID4,
            Base64Str,
            PositiveInt,
            Annotated[str, StringConstraints(max_length=3)],
            Annotated[int, Field(gt=0)],
            Annotated[int, annotated_types.Gt(0)],
            Upper,
            Annotated[int, 'a note'],
            Inner,
            Plain,
            Color,
        ],
    )
    def test_accepted(self, annotation: object) -> None:
        assert _signature_refuses(annotation) is False


@dataclasses.dataclass
class _Private:
    n: int = 1
    _kept: int = 0


@dataclasses.dataclass
class _WithClassVar:
    limit: ClassVar[Any] = 1
    n: int = 1


@pytest.mark.unit
class TestDataclassFields:
    @pytest.mark.parametrize(
        'annotation',
        [bytes, set[int], datetime.timedelta, Any, dict],
    )
    def test_a_leading_underscore_is_a_wire_field(self, annotation: object) -> None:
        fields = [('n', int, 1), ('_hidden', annotation, None)]
        shape = dataclasses.make_dataclass('Hidden', fields)

        assert _signature_refuses(shape) is True

    def test_a_valid_private_field_passes(self) -> None:
        assert _signature_refuses(_Private) is False

    def test_a_class_var_is_not_a_field(self) -> None:
        assert _signature_refuses(_WithClassVar) is False

    def test_an_init_var_stays_refused(self) -> None:
        @dataclasses.dataclass
        class WithInitVar:
            n: int = 1
            seed: dataclasses.InitVar[int] = 0

        assert _signature_refuses(WithInitVar) is True


@pytest.mark.unit
class TestNestedTaskResult:
    @pytest.mark.parametrize(
        'annotation',
        [
            list[TaskResult[int, TaskError]],
            dict[str, TaskResult[int, TaskError]],
            tuple[TaskResult[int, TaskError], ...],
            int | TaskResult[int, TaskError],
        ],
    )
    def test_below_the_top_is_refused(self, annotation: object) -> None:
        def task(*, x: Any) -> TaskResult[int, TaskError]:
            return TaskResult(ok=1)

        task.__annotations__ = {'x': annotation, 'return': TaskResult[int, TaskError]}

        with pytest.raises(SignatureValidationError, match='fits only|mixed union'):
            check_task_signature(task, task_name='task')

    @pytest.mark.parametrize(
        'annotation',
        [
            TaskResult[int, TaskError],
            TaskResult[int, TaskError] | None,
            Annotated[TaskResult[int, TaskError], 'a note'],
            Annotated[TaskResult[int, TaskError] | None, 'a note'],
        ],
    )
    def test_a_slot_passes(self, annotation: object) -> None:
        assert _signature_refuses(annotation) is False

    def test_a_slot_ok_type_is_checked(self) -> None:
        assert _signature_refuses(TaskResult[Open, TaskError]) is True

    def test_a_wrong_error_type_is_refused(self) -> None:
        assert _signature_refuses(TaskResult[int, str]) is True

    def test_a_slot_default_other_than_none_is_refused(self) -> None:
        def task(
            *,
            x: TaskResult[int, TaskError] | None = 'not none',  # type: ignore[assignment]
        ) -> TaskResult[int, TaskError]:
            return TaskResult(ok=1)

        with pytest.raises(SignatureValidationError, match='must be None'):
            check_task_signature(task, task_name='task')

    @pytest.mark.parametrize(
        'annotation',
        [
            TaskResult[int, TaskError] | None,
            Annotated[TaskResult[int, TaskError], 'a note'],
        ],
    )
    def test_a_producer_cannot_supply_a_slot(self, annotation: object) -> None:
        def task(*, x: Any = None) -> TaskResult[int, TaskError]:
            return TaskResult(ok=1)

        task.__annotations__['x'] = annotation
        hints, sig = resolve_hints_and_sig(task) or ({}, None)
        assert sig is not None

        binding = resolve_producer_kwarg_binding(task, 'x', hints, sig)

        assert binding.kind == 'error'


def _mock_app() -> MagicMock:
    app = MagicMock()
    app.config.queue_mode.name = 'DEFAULT'
    app.config.resend_on_transient_err = False
    app.config.payload = PayloadPolicy()
    app.config.exception_mapper = {}
    app.config.default_unhandled_error_code = 'UNHANDLED_EXCEPTION'
    app.are_sends_suppressed.return_value = False
    app.validate_queue_name.return_value = 'default'
    return app


@pytest.mark.unit
class TestFillOmittedDefaults:
    def test_the_function_gets_the_validated_default(self) -> None:
        def task(*, s: Upper = 'abc') -> TaskResult[int, TaskError]:
            return TaskResult(ok=1)

        assert fill_omitted_defaults(task, {}) == {'s': 'ABC'}

    def test_a_given_value_stays(self) -> None:
        def task(*, s: Upper = 'abc') -> TaskResult[int, TaskError]:
            return TaskResult(ok=1)

        assert fill_omitted_defaults(task, {'s': 'x'}) == {'s': 'x'}

    def test_a_list_default_is_a_fresh_copy(self) -> None:
        def task(*, items: list[int] = []) -> TaskResult[int, TaskError]:
            return TaskResult(ok=1)

        filled = fill_omitted_defaults(task, {})

        assert filled == {'items': []}
        assert filled['items'] is not task.__kwdefaults__['items']

    def test_a_validator_that_refuses_its_default_raises(self) -> None:
        def task(*, n: _Refused = 1) -> TaskResult[int, TaskError]:
            return TaskResult(ok=n)

        with pytest.raises(ValueValidationError):
            fill_omitted_defaults(task, {})

    def test_engine_filled_parameters_are_skipped(self) -> None:
        def task(
            *,
            workflow_ctx: WorkflowContext | None = None,
            value: TaskResult[int, TaskError] | None = None,
        ) -> TaskResult[int, TaskError]:
            return TaskResult(ok=1)

        assert fill_omitted_defaults(task, {}) == {}

    def test_a_refused_default_stops_the_send(self) -> None:
        def task(*, n: _Refused = 1) -> TaskResult[int, TaskError]:
            return TaskResult(ok=n)

        app = _mock_app()
        result = create_task_wrapper(task, app, 'test.task').send()

        assert is_err(result)
        assert isinstance(result.err_value, TaskSendValidationFailed)
        app.get_broker.assert_not_called()


def _run_child(app: Horsies, task_name: str) -> tuple[bool, str, str | None]:
    with (
        patch.object(
            child_runner,
            '_confirm_ownership_and_set_running',
            MagicMock(return_value=None),
        ),
        patch.object(child_runner, '_mark_task_finalizing', MagicMock()),
        patch.object(child_runner, 'get_current_app', MagicMock(return_value=app)),
    ):
        return wire_of(
            child_runner._run_task_entry(
                task_name=task_name,
                args_json='[]',
                kwargs_json='{}',
                task_id='task-id',
                master_worker_id='worker',
                is_workflow_task=False,
                task_loop=TaskLoop(),
            )
        )


@pytest.mark.unit
class TestOmittedDefaultInTheWorker:
    def test_the_body_gets_the_validated_default(self) -> None:
        seen: list[object] = []
        app = _real_app()

        @app.task('child_default')
        def task(*, s: Upper = 'abc') -> TaskResult[int, TaskError]:
            seen.append(s)
            return TaskResult(ok=1)

        _run_child(app, 'child_default')

        assert seen == ['ABC']

    def test_an_optional_slot_stays_none(self) -> None:
        seen: list[object] = []
        app = _real_app()

        @app.task('child_slot')
        def task(
            *,
            value: TaskResult[int, TaskError] | None = None,
        ) -> TaskResult[int, TaskError]:
            seen.append(value)
            return TaskResult(ok=1)

        ok, payload, _ = _run_child(app, 'child_slot')

        assert ok is True
        assert seen == [None]
        assert json.loads(payload)['err'] is None

    def test_a_default_that_holds_the_envelope_key_is_a_value(self) -> None:
        seen: list[object] = []
        app = _real_app()

        @app.task('child_envelope')
        def task(
            *,
            data: dict[str, JsonValue] = {'__h_taskresult_envelope__': True},
        ) -> TaskResult[int, TaskError]:
            seen.append(data)
            return TaskResult(ok=1)

        _run_child(app, 'child_envelope')

        assert seen == [{'__h_taskresult_envelope__': True}]

    def test_a_refused_default_stops_the_body(self) -> None:
        seen: list[object] = []
        app = _real_app()

        @app.task('child_refused')
        def task(*, n: _Refused = 1) -> TaskResult[int, TaskError]:
            seen.append(n)
            return TaskResult(ok=1)

        _, payload, reason = _run_child(app, 'child_refused')

        assert seen == []
        assert reason is not None and 'SerializationError' in reason
        assert json.loads(payload)['err']['error_code'] == {
            '__builtin_task_code__': 'WORKER_SERIALIZATION_ERROR',
        }


@pytest.mark.unit
class TestDefaultOnASchedule:
    def _schedule(self, task_name: str) -> TaskSchedule:
        return TaskSchedule(
            name='nightly',
            task_name=task_name,
            pattern=IntervalSchedule(hours=1),
        )

    def test_a_refused_default_is_a_boot_refusal(self) -> None:
        app = _real_app()

        @app.task('schedule_refused')
        def task(*, n: _Refused = 1) -> TaskResult[int, TaskError]:
            return TaskResult(ok=n)

        result = _encode_schedule_kwargs(app, self._schedule('schedule_refused'))

        assert is_err(result)
        assert result.err_value.code is ErrorCode.CONFIG_INVALID_SCHEDULE

    def test_a_valid_default_gives_no_stored_kwargs(self) -> None:
        app = _real_app()

        @app.task('schedule_valid')
        def task(*, n: int = 1) -> TaskResult[int, TaskError]:
            return TaskResult(ok=n)

        result = _encode_schedule_kwargs(app, self._schedule('schedule_valid'))

        assert is_ok(result)
        assert result.ok_value is None


def _real_app() -> Horsies:
    return Horsies(
        AppConfig(
            broker=PostgresConfig(
                database_url='postgresql+psycopg://user:password@localhost/unused',
            ),
        ),
    )


def _child(annotation: object, *, key: str) -> Any:
    class Child(WorkflowDefinition[int]):
        name = key
        definition_key = f'tests.wire_shape.{key}'

        @classmethod
        def build_with(cls, app: Any, *, value: Any = None) -> Any:
            raise AssertionError('not built')

    Child._original_build_with.__annotations__ = {'value': annotation}
    return Child


@pytest.mark.unit
class TestBuildWithBoundary:
    @pytest.mark.parametrize(
        ('annotation', 'key'),
        [
            (Any, 'any'),
            (dict, 'bare_dict'),
            (object, 'object'),
            (ExcludeField, 'exclude'),
            (Alias, 'alias'),
            (Open, 'extra_allow'),
        ],
    )
    def test_a_banned_type_is_refused_at_the_node(
        self,
        annotation: object,
        key: str,
    ) -> None:
        with pytest.raises(WorkflowValidationError) as refused:
            WorkflowSpec(
                name='parent',
                tasks=[SubWorkflowNode(workflow_def=_child(annotation, key=key))],
            )

        assert refused.value.code is ErrorCode.WORKFLOW_KWARGS_NOT_SERIALIZABLE

    def test_a_valid_type_passes(self) -> None:
        WorkflowSpec(
            name='parent',
            tasks=[SubWorkflowNode(workflow_def=_child(Optional[int], key='valid'))],
        )

    def test_a_bad_default_is_refused_at_the_node(self) -> None:
        class Child(WorkflowDefinition[int]):
            name = 'bad_default_child'
            definition_key = 'tests.wire_shape.bad_default'

            @classmethod
            def build_with(cls, app: Any, *, value: int = 'x') -> Any:  # type: ignore[assignment]
                raise AssertionError('not built')

        with pytest.raises(WorkflowValidationError):
            WorkflowSpec(name='parent', tasks=[SubWorkflowNode(workflow_def=Child)])

    def test_variadics_and_positional_parameters_pass(self) -> None:
        class Child(WorkflowDefinition[int]):
            name = 'variadic_child'
            definition_key = 'tests.wire_shape.variadic'

            @classmethod
            def build_with(cls, app: Any, value: int = 1, *args: Any, **params: Any) -> Any:
                raise AssertionError('not built')

        WorkflowSpec(name='parent', tasks=[SubWorkflowNode(workflow_def=Child)])

    def test_a_top_level_build_with_is_not_checked(self) -> None:
        class Client:
            pass

        class Child(WorkflowDefinition[int]):
            name = 'top_level_child'
            definition_key = 'tests.wire_shape.top_level'

            @classmethod
            def build_with(cls, app: Any, client: Client | None = None) -> Any:
                raise AssertionError('not built')

        assert issubclass(Child, WorkflowDefinition)

    def test_hints_that_do_not_resolve_are_refused_at_the_node(self) -> None:
        class Child(WorkflowDefinition[int]):
            name = 'unresolved_child'
            definition_key = 'tests.wire_shape.unresolved'

            @classmethod
            def build_with(cls, app: 'Horsies', *, when: 'NotDefinedHere') -> Any:  # type: ignore[name-defined]  # noqa: F821
                raise AssertionError('not built')

        with pytest.raises(WorkflowValidationError) as refused:
            WorkflowSpec(
                name='parent',
                tasks=[SubWorkflowNode(workflow_def=Child, kwargs={'when': 'x'})],
            )

        assert 'NotDefinedHere' in ' '.join(refused.value.notes)

    def test_hints_that_do_not_resolve_do_not_fall_to_json(self) -> None:
        class Child(WorkflowDefinition[int]):
            name = 'unresolved_wire_child'
            definition_key = 'tests.wire_shape.unresolved_wire'

            @classmethod
            def build_with(cls, app: Any, *, when: 'NotDefinedHere') -> Any:  # type: ignore[name-defined]  # noqa: F821
                raise AssertionError('not built')

        with pytest.raises(Exception, match='do not resolve'):
            decode_subworkflow_kwargs(Child, {'when': 'not a date'})

    def test_the_app_type_resolves_under_type_checking(self) -> None:
        class Child(WorkflowDefinition[int]):
            name = 'type_checking_child'
            definition_key = 'tests.wire_shape.type_checking'

            @classmethod
            def build_with(cls, app: 'WorkflowSpec', *, when: datetime.datetime) -> Any:
                raise AssertionError('not built')

        with patch.dict(globals()):
            del globals()['WorkflowSpec']
            with pytest.raises(Exception, match='when'):
                decode_subworkflow_kwargs(Child, {'when': 'not a date'})

    def test_a_return_annotation_under_type_checking_passes(self) -> None:
        class Child(WorkflowDefinition[int]):
            name = 'return_only_child'
            definition_key = 'tests.wire_shape.return_only'

            @classmethod
            def build_with(cls, app: Any, *, value: int = 1) -> 'NotDefinedHere':  # type: ignore[name-defined]  # noqa: F821
                raise AssertionError('not built')

        WorkflowSpec(name='parent', tasks=[SubWorkflowNode(workflow_def=Child)])

    def test_a_decorator_from_another_module_is_followed(self) -> None:
        decorators = types.ModuleType('wire_shape_decorators')
        exec(
            'import functools\n'
            'def keep(fn):\n'
            '    @functools.wraps(fn)\n'
            '    def wrapper(*args, **kwargs):\n'
            '        return fn(*args, **kwargs)\n'
            '    return wrapper\n',
            decorators.__dict__,
        )
        source = types.ModuleType('wire_shape_child')
        source.__dict__['keep'] = decorators.keep
        exec(
            'from __future__ import annotations\n'
            'from typing import Any\n'
            '@keep\n'
            'def build_with(cls, app: Any, *, value: int = 1) -> Any:\n'
            '    raise AssertionError\n',
            source.__dict__,
        )

        class Child(WorkflowDefinition[int]):
            name = 'decorated_child'
            definition_key = 'tests.wire_shape.decorated'
            build_with = classmethod(source.build_with)

        WorkflowSpec(name='parent', tasks=[SubWorkflowNode(workflow_def=Child)])

    def test_the_app_parameter_can_have_any_name(self) -> None:
        class Child(WorkflowDefinition[int]):
            name = 'renamed_app_child'
            definition_key = 'tests.wire_shape.renamed_app'

            @classmethod
            def build_with(cls, horsies_app: Any, *, value: int = 1) -> Any:
                raise AssertionError('not built')

        WorkflowSpec(name='parent', tasks=[SubWorkflowNode(workflow_def=Child)])

    def test_the_first_parameter_can_have_any_name(self) -> None:
        class Child(WorkflowDefinition[int]):
            name = 'renamed_cls_child'
            definition_key = 'tests.wire_shape.renamed_cls'

            @classmethod
            def build_with(klass, app: Any, *, value: int = 1) -> Any:
                raise AssertionError('not built')

        WorkflowSpec(name='parent', tasks=[SubWorkflowNode(workflow_def=Child)])

    def test_the_boot_check_finds_a_class_level_node(self) -> None:
        class Child(WorkflowDefinition[int]):
            name = 'boot_child'
            definition_key = 'tests.wire_shape.boot_child'

            @classmethod
            def build_with(cls, app: Any, *, value: Any = None) -> Any:
                raise AssertionError('not built')

        class Parent:
            child = SubWorkflowNode(workflow_def=Child)

        app = _real_app()
        with patch.object(app, '_discovered_task_modules', ['boot_module']), patch.object(
            app,
            '_resolve_imported_module',
            return_value=types.ModuleType('boot_module'),
        ), patch.object(
            app,
            '_collect_module_workflow_definitions',
            return_value=[Parent],
        ), patch.object(Parent, '_require_definition_key', create=True):
            errors = app._check_workflow_definitions()

        assert [error.code for error in errors] == [
            ErrorCode.WORKFLOW_KWARGS_NOT_SERIALIZABLE,
        ]


@pytest.mark.unit
class TestDefinitionOfAny:
    def _child(self, key: str) -> Any:
        class Child(WorkflowDefinition[Any]):
            name = key
            definition_key = f'tests.wire_shape.{key}'

            @classmethod
            def build_with(cls, app: Any, *args: Any, **params: Any) -> Any:
                raise AssertionError('not built')

        return Child

    def test_it_cannot_feed_args_from(self) -> None:
        app = _real_app()

        @app.task('any_target')
        def target(*, value: TaskResult[int, TaskError]) -> TaskResult[int, TaskError]:
            return value

        source = SubWorkflowNode(workflow_def=self._child('any_source'))
        node = TaskNode(fn=target, waits_for=[source], args_from={'value': source})

        with pytest.raises(WorkflowValidationError) as refused:
            WorkflowSpec(name='parent', tasks=[source, node])

        assert refused.value.code is ErrorCode.WORKFLOW_ARGS_FROM_TYPE_MISMATCH

    def test_it_cannot_be_an_output(self) -> None:
        node = SubWorkflowNode(workflow_def=self._child('any_output'))

        with pytest.raises(WorkflowValidationError) as refused:
            WorkflowSpec(name='parent', tasks=[node], output=node)

        assert refused.value.code is ErrorCode.WORKFLOW_OUTPUT_TYPE_MISMATCH

    def test_annotated_any_cannot_feed_args_from_or_be_an_output(self) -> None:
        class Child(WorkflowDefinition[Annotated[Any, 'note']]):
            name = 'annotated_any'
            definition_key = 'tests.wire_shape.annotated_any'

            @classmethod
            def build_with(cls, app: Any, *args: Any, **params: Any) -> Any:
                raise AssertionError('not built')

        app = _real_app()

        @app.task('annotated_any_target')
        def target(*, value: TaskResult[int, TaskError]) -> TaskResult[int, TaskError]:
            return value

        source = SubWorkflowNode(workflow_def=Child)
        node = TaskNode(fn=target, waits_for=[source], args_from={'value': source})

        with pytest.raises(WorkflowValidationError) as refused:
            WorkflowSpec(name='parent', tasks=[source, node])
        assert refused.value.code is ErrorCode.WORKFLOW_ARGS_FROM_TYPE_MISMATCH

        output = SubWorkflowNode(workflow_def=Child)
        with pytest.raises(WorkflowValidationError) as refused:
            WorkflowSpec(name='parent', tasks=[output], output=output)
        assert refused.value.code is ErrorCode.WORKFLOW_OUTPUT_TYPE_MISMATCH

    def test_it_can_run_with_no_consumer(self) -> None:
        WorkflowSpec(
            name='parent',
            tasks=[SubWorkflowNode(workflow_def=self._child('any_alone'))],
        )


@pytest.mark.unit
class TestSelfCheck:
    def test_it_passes_on_this_pydantic(self) -> None:
        verify_shape_rules()

    def test_the_definition_check_runs_it(self) -> None:
        with (
            patch.object(wire_shape._RuleWord, 'verified', False),
            patch.object(
                wire_shape,
                '_known_shapes',
                return_value=((ShapeRule.EXCLUDED_FIELD, int),),
            ),
            pytest.raises(RuntimeError, match='EXCLUDED_FIELD'),
        ):
            shape_refusals(int, internal_types=frozenset())

    def test_a_rule_that_finds_nothing_crashes_loud(self) -> None:
        with (
            patch.object(wire_shape._RuleWord, 'verified', False),
            patch.object(
                wire_shape,
                '_known_shapes',
                return_value=((ShapeRule.EXCLUDED_FIELD, int),),
            ),
            pytest.raises(RuntimeError, match='EXCLUDED_FIELD'),
        ):
            verify_shape_rules()

    def test_the_text_names_the_pydantic_version(self) -> None:
        import pydantic

        with (
            patch.object(wire_shape._RuleWord, 'verified', False),
            patch.object(
                wire_shape,
                '_known_shapes',
                return_value=((ShapeRule.JSON_STRING_TYPE, int),),
            ),
            pytest.raises(RuntimeError, match=pydantic.VERSION),
        ):
            verify_shape_rules()
