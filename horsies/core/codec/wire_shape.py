"""What the core schema of a declared type says about the wire.

`check_task_signature` walks annotations and holds the allow-list. A shape can be
spelled in many ways: `Annotated`, a `Field` default, a decorator, a stdlib or a
pydantic dataclass. Each spelling reaches the same node of the core schema, so this
module reads that schema once and refuses a shape that cannot cross the wire or
that cancels a check.

The walk reads the schema and runs no user code. A default is checked against the
type only, for the same reason: a validator can need a model, a context, or a
service, and none exists at import.

Core schema key names belong to pydantic. `verify_shape_rules` runs each rule on a
known shape once per process, so a renamed key fails loud and does not switch a
rule off.

Public API:
- `ShapeRule`, `ShapeRefusal`: the catalog of refusals.
- `shape_refusals`: the refusals of a declared type.
- `parameter_default_refusal`: the refusal of a parameter default.
- `verify_shape_rules`: the self-check.
"""

from __future__ import annotations

import dataclasses
import enum
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Annotated, Any, Final, Literal, cast

import pydantic
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    Json,
    PlainValidator,
    TypeAdapter,
    ValidationError,
    WrapValidator,
    computed_field,
    model_validator,
)
from pydantic.errors import PydanticSchemaGenerationError, PydanticUndefinedAnnotation
from pydantic_core import SchemaValidator, core_schema

from horsies.core.codec.json_value import StrictJsonError
from horsies.core.codec.typed import SCHEMA_VALUE_KEYS, check_json_value_positions
from horsies.core.codec.validation_errors import ValueValidationError


__all__ = [
    'ShapeRefusal',
    'ShapeRule',
    'parameter_default_refusal',
    'shape_refusals',
    'verify_shape_rules',
]


_USER_FUNCTION_NODES: Final = frozenset({
    'function-before',
    'function-after',
    'function-wrap',
})


class ShapeRule(enum.Enum):
    """One rule of the definition check."""

    EXCLUDED_FIELD = 'EXCLUDED_FIELD'
    UNREADABLE_KEY = 'UNREADABLE_KEY'
    EXTRA_ALLOW = 'EXTRA_ALLOW'
    DATACLASS_REVALIDATION = 'DATACLASS_REVALIDATION'
    COMPUTED_WITH_FORBID = 'COMPUTED_WITH_FORBID'
    JSON_STRING_TYPE = 'JSON_STRING_TYPE'
    PLAIN_ENUM_LITERAL = 'PLAIN_ENUM_LITERAL'
    VALIDATOR_REPLACES_CHECK = 'VALIDATOR_REPLACES_CHECK'
    NO_TYPE = 'NO_TYPE'
    INVALID_DEFAULT = 'INVALID_DEFAULT'
    NO_SCHEMA = 'NO_SCHEMA'


@dataclass(frozen=True, slots=True)
class ShapeRefusal:
    """Why a declared type cannot be used at a task boundary.

    Attributes:
        rule: The rule that refused.
        subject: What was refused, for example `Order.note`. It names types and
            fields, never a value.
        reason: The cause.
        fix: What to write instead.
    """

    rule: ShapeRule
    subject: str
    reason: str
    fix: str


@dataclass(frozen=True, slots=True)
class _SchemaFacts:
    """The result of one walk of a schema."""

    refusals: tuple[ShapeRefusal, ...]
    defaults: tuple[tuple[Mapping[str, Any], Mapping[str, Any]], ...]


_Node = dict[str, Any]


def _is_plain_enum_member(value: object) -> bool:
    return isinstance(value, enum.Enum) and not isinstance(value, (str, int))


def _accepted_alias_keys(validation_alias: object) -> set[str]:
    """The keys that a `validation_alias` accepts, each path read on its own.

    A multi-step `AliasPath` reads inside a value, so it accepts no flat key.
    """
    if isinstance(validation_alias, str):
        return {validation_alias}
    if not isinstance(validation_alias, list):
        return set()
    items = cast('list[object]', validation_alias)
    if all(isinstance(item, list) for item in items):
        paths = cast('list[list[object]]', items)
        return {cast('str', path[0]) for path in paths if len(path) == 1}
    return {cast('str', items[0])} if len(items) == 1 else set()


def _field_refusal(
    owner: str,
    name: str,
    field: Mapping[str, Any],
    config: Mapping[str, Any],
) -> ShapeRefusal | None:
    """Apply the rules of one field of a model or a dataclass."""
    subject = f'{owner}.{name}'
    if field.get('serialization_exclude') or field.get('serialization_exclude_if'):
        return ShapeRefusal(
            rule=ShapeRule.EXCLUDED_FIELD,
            subject=subject,
            reason='an excluded field is not written, so the reader gets its default',
            fix='remove the exclude setting, or keep the field out of this type',
        )
    validation_alias = field.get('validation_alias')
    if validation_alias is None:
        return None
    serialization_alias = field.get('serialization_alias')
    written = (
        serialization_alias
        if config.get('serialize_by_alias') and serialization_alias
        else name
    )
    accepted: set[str] = set()
    if config.get('validate_by_name') or config.get('populate_by_name'):
        accepted.add(name)
    if config.get('validate_by_alias', True):
        accepted |= _accepted_alias_keys(validation_alias)
    if written in accepted:
        return None
    return ShapeRefusal(
        rule=ShapeRule.UNREADABLE_KEY,
        subject=subject,
        reason=f'the key {written!r} is written, and the read does not accept it',
        fix=(
            "set populate_by_name=True, set serialize_by_alias=True, or add the "
            'field name to AliasChoices'
        ),
    )


def _container_refusals(
    node: Mapping[str, Any],
    config: Mapping[str, Any],
) -> list[ShapeRefusal]:
    """Apply the rules of a `model-fields` or `dataclass-args` node."""
    refusals: list[ShapeRefusal] = []
    owner = str(node.get('model_name') or node.get('dataclass_name') or 'type')
    extra = node.get('extra_behavior') or config.get('extra_fields_behavior')
    if extra == 'allow':
        refusals.append(ShapeRefusal(
            rule=ShapeRule.EXTRA_ALLOW,
            subject=owner,
            reason="extra='allow' carries fields that have no declared type",
            fix='declare each field, or add a dict[str, T] field',
        ))
    if node.get('computed_fields') and extra == 'forbid':
        refusals.append(ShapeRefusal(
            rule=ShapeRule.COMPUTED_WITH_FORBID,
            subject=owner,
            reason="a computed field is written, and extra='forbid' refuses it on read",
            fix="remove extra='forbid', or remove the computed field",
        ))
    fields: object = node['fields']
    named: list[tuple[str, _Node]] = (
        list(cast('dict[str, _Node]', fields).items())
        if isinstance(fields, dict)
        else [(field['name'], field) for field in cast('list[_Node]', fields)]
    )
    for name, field in named:
        refusal = _field_refusal(owner, name, field, config)
        if refusal is not None:
            refusals.append(refusal)
    return refusals


def _node_refusal(node: Mapping[str, Any]) -> ShapeRefusal | None:
    """Apply the rules that one node decides alone."""
    match node.get('type'):
        case 'function-plain':
            return ShapeRefusal(
                rule=ShapeRule.VALIDATOR_REPLACES_CHECK,
                subject='PlainValidator',
                reason='a plain validator replaces the check of the declared type',
                fix='use AfterValidator or BeforeValidator, which keep the check',
            )
        case 'function-wrap':
            return ShapeRefusal(
                rule=ShapeRule.VALIDATOR_REPLACES_CHECK,
                subject='WrapValidator',
                reason='a wrap validator can skip the check of the declared type',
                fix='use AfterValidator or BeforeValidator, which keep the check',
            )
        case 'any':
            return ShapeRefusal(
                rule=ShapeRule.NO_TYPE,
                subject='a value with no type',
                reason='this position accepts any value, so the declared type does not hold',
                fix='use a concrete type, or JsonValue for raw JSON data',
            )
        case 'json':
            return ShapeRefusal(
                rule=ShapeRule.JSON_STRING_TYPE,
                subject='Json',
                reason='Json reads a JSON string, and the value is written as JSON',
                fix='declare the inner type directly',
            )
        case 'literal' if any(_is_plain_enum_member(item) for item in node['expected']):
            return ShapeRefusal(
                rule=ShapeRule.PLAIN_ENUM_LITERAL,
                subject='Literal',
                reason='a plain Enum member is written as its value, and Literal refuses that',
                fix='use the Enum type, or a str or int Enum',
            )
        case _:
            return None


def _record_refusal(
    node: Mapping[str, Any],
    config: Mapping[str, Any],
) -> ShapeRefusal | None:
    """Apply the rule that a `dataclass` node decides."""
    if node.get('type') != 'dataclass':
        return None
    revalidate = node.get('revalidate_instances') or config.get('revalidate_instances')
    if revalidate in (None, 'never'):
        return None
    return ShapeRefusal(
        rule=ShapeRule.DATACLASS_REVALIDATION,
        subject=getattr(node.get('cls'), '__name__', 'dataclass'),
        reason='revalidation replaces a subclass instance and drops its fields',
        fix="remove revalidate_instances, or set it to 'never'",
    )


def _walk(schema: Mapping[str, Any], internal_types: frozenset[type]) -> _SchemaFacts:
    """Read a schema. Each dict is read once, so the walk ends on a recursive type."""
    refusals: list[ShapeRefusal] = []
    defaults: list[tuple[Mapping[str, Any], Mapping[str, Any]]] = []
    seen: set[int] = set()
    stack: list[tuple[object, Mapping[str, Any]]] = [(schema, {})]
    while stack:
        item, config = stack.pop()
        if isinstance(item, (list, tuple)):
            stack.extend((child, config) for child in cast('Sequence[object]', item))
            continue
        if not isinstance(item, dict):
            continue
        node = cast('_Node', item)
        if id(node) in seen:
            continue
        seen.add(id(node))
        kind = node.get('type')
        if kind in ('model', 'dataclass'):
            if node.get('cls') in internal_types:
                continue
            config = cast('Mapping[str, Any]', node.get('config') or {})
            refusal = _record_refusal(node, config)
            if refusal is not None:
                refusals.append(refusal)
        if kind in ('model-fields', 'dataclass-args'):
            refusals.extend(_container_refusals(node, config))
        if kind == 'default' and 'default' in node:
            defaults.append((node, config))
        refusal = _node_refusal(node)
        if refusal is not None:
            refusals.append(refusal)
        children: list[object] = [
            value for key, value in node.items() if key not in SCHEMA_VALUE_KEYS
        ]
        stack.extend((child, config) for child in children)
    return _SchemaFacts(tuple(refusals), tuple(defaults))


def _type_only(node: object) -> object:
    """Return a schema that checks the type and runs no user code.

    Function nodes go, and their inner schema stays. A constraint sits on the type
    node, so it stays too. A `model` or `dataclass` node becomes `is-instance` of
    its class: pydantic reuses the prebuilt validator of a model, so a dict default
    would run the validators of that model.
    """
    if isinstance(node, (list, tuple)):
        return [_type_only(item) for item in cast('Sequence[object]', node)]
    if not isinstance(node, dict):
        return node
    source = cast('_Node', node)
    kind = source.get('type')
    if kind in ('model', 'dataclass'):
        replaced: _Node = dict(core_schema.is_instance_schema(source['cls']))
        if 'ref' in source:
            replaced['ref'] = source['ref']
        return replaced
    if kind in _USER_FUNCTION_NODES:
        # An after validator holds the ref of a shared or recursive model on this
        # node, so the ref must stay or a reference points at nothing.
        inner = cast('_Node', _type_only(source['schema']))
        return {**inner, 'ref': source['ref']} if 'ref' in source else inner
    return {
        key: value if key in SCHEMA_VALUE_KEYS else _type_only(value)
        for key, value in source.items()
    }


def _default_problem(
    value_schema: Mapping[str, Any],
    value: object,
    definitions: object,
    config: Mapping[str, Any],
) -> str | None:
    """Check one default against the type only. Return the cause of a refusal."""
    schema = _type_only({
        'type': 'definitions',
        'schema': value_schema,
        'definitions': definitions,
    })
    validator = SchemaValidator(cast('Any', schema), cast('Any', dict(config) or None))
    try:
        validated = validator.validate_python(value, strict=True)
    except ValidationError as error:
        return f"it does not hold the declared type ({error.errors()[0]['type']})"
    if validated is value or validated == value:
        return None
    return 'it is not in the form that the declared type gives'


def _field_default_refusals(
    facts: _SchemaFacts,
    definitions: object,
) -> list[ShapeRefusal]:
    refusals: list[ShapeRefusal] = []
    for node, config in facts.defaults:
        if node.get('validate_default'):
            # pydantic runs the real validators on this default at read.
            continue
        problem = _default_problem(node['schema'], node['default'], definitions, config)
        if problem is not None:
            refusals.append(ShapeRefusal(
                rule=ShapeRule.INVALID_DEFAULT,
                subject=f'default of type {type(node["default"]).__name__}',
                reason=f'a field default is not valid: {problem}',
                fix='write the default in its final form, as a value of the field type',
            ))
    return refusals


def _schema_of(
    annotation: object,
) -> Mapping[str, Any] | ShapeRefusal:
    """The core schema of `list[annotation]`.

    `defer_build` gives a mock schema for the type itself. In a list the schema is
    real.
    """
    try:
        schema = TypeAdapter(list[annotation]).core_schema  # type: ignore[valid-type]
    except (PydanticSchemaGenerationError, PydanticUndefinedAnnotation) as error:
        return ShapeRefusal(
            rule=ShapeRule.NO_SCHEMA,
            subject=repr(annotation),
            reason=f'pydantic cannot build a schema for this type: {type(error).__name__}',
            fix='use a type from the allow list',
        )
    assert isinstance(schema, dict), f'no real core schema for {annotation!r}'
    return cast('Mapping[str, Any]', schema)


def shape_refusals(
    annotation: object,
    *,
    internal_types: frozenset[type],
) -> tuple[ShapeRefusal, ...]:
    """Return the refusals of one declared type.

    Args:
        annotation: A type that passed the allow-list.
        internal_types: Library types with a hand-maintained schema. The walk
            stops at them.

    Returns:
        Each refusal. An empty tuple means that the type can cross the wire. The
        field defaults are checked only when the shape itself is clean, so one
        cause gives one report and no user code runs.
    """
    verify_shape_rules()
    schema = _schema_of(annotation)
    if isinstance(schema, ShapeRefusal):
        return (schema,)
    facts = _walk(schema, internal_types)
    if facts.refusals:
        return facts.refusals
    return tuple(_field_default_refusals(facts, schema.get('definitions', [])))


def parameter_default_refusal(
    annotation: object,
    default: object,
) -> ShapeRefusal | None:
    """Check a parameter default against its declared type.

    The default must hold the type. A validator does not run here, because an
    omitted parameter is validated before the task body runs.

    Args:
        annotation: The parameter type. `shape_refusals` passed it.
        default: The default value.

    Returns:
        The refusal, or None when the default is valid.
    """
    schema = _schema_of(annotation)
    if isinstance(schema, ShapeRefusal):
        return schema
    element = schema['schema'] if schema['type'] == 'definitions' else schema
    try:
        check_json_value_positions(default, annotation)
    except (StrictJsonError, ValueValidationError):
        problem: str | None = 'it is not JSON-native'
    else:
        problem = _default_problem(
            element['items_schema'],
            default,
            schema.get('definitions', []),
            {},
        )
    if problem is None:
        return None
    return ShapeRefusal(
        rule=ShapeRule.INVALID_DEFAULT,
        subject=f'default of type {type(default).__name__}',
        reason=f'the default is not valid: {problem}',
        fix='write a default that is a value of the declared type',
    )


# ---------------------------------------------------------------------------
# Self-check
# ---------------------------------------------------------------------------


class _RuleWord:
    """Holds the state of the self-check. It runs once per process."""

    verified: bool = False


def _known_shapes() -> tuple[tuple[ShapeRule, object], ...]:
    """One known shape per rule, built only when the self-check runs."""

    class Excluded(BaseModel):
        note: str = Field(default='d', exclude=True)

    class ExcludedIf(BaseModel):
        note: str = Field(default='d', exclude_if=lambda value: value == 'hide')

    class ExcludedBehindValidator(BaseModel):
        note: str = Field(default='d', exclude=True)

        @model_validator(mode='before')
        @classmethod
        def read(cls, value: object) -> object:
            return value

    class Aliased(BaseModel):
        number: int = Field(alias='count')

    class Open(BaseModel):
        model_config = ConfigDict(extra='allow')

    class DeferredOpen(BaseModel):
        model_config = ConfigDict(defer_build=True, extra='allow')

    @dataclasses.dataclass
    class Revalidated:
        __pydantic_config__ = ConfigDict(revalidate_instances='always')
        number: int = 0

    class Forbidden(BaseModel):
        model_config = ConfigDict(extra='forbid')

        @computed_field
        @property
        def total(self) -> int:
            return 1

    class Color(enum.Enum):
        RED = 'red'

    class WrongDefault(BaseModel):
        number: int = cast('int', 'x')

    class Plain(BaseModel):
        number: int = 0

    class DictDefault(BaseModel):
        inner: Plain = cast('Plain', {'number': 1})

    @dataclasses.dataclass
    class ExcludedDataclass:
        note: str = Field(default='d', exclude=True)

    class Shared(BaseModel):
        number: int = 0

        @model_validator(mode='after')
        def check(self) -> Shared:
            return self

    class SharedDefault(BaseModel):
        first: Shared
        second: Shared
        number: int = cast('int', 'x')

    return (
        (ShapeRule.EXCLUDED_FIELD, Excluded),
        (ShapeRule.EXCLUDED_FIELD, ExcludedIf),
        (ShapeRule.EXCLUDED_FIELD, ExcludedBehindValidator),
        (ShapeRule.EXCLUDED_FIELD, ExcludedDataclass),
        (ShapeRule.INVALID_DEFAULT, SharedDefault),
        (ShapeRule.UNREADABLE_KEY, Aliased),
        (ShapeRule.EXTRA_ALLOW, Open),
        (ShapeRule.EXTRA_ALLOW, DeferredOpen),
        (ShapeRule.DATACLASS_REVALIDATION, Revalidated),
        (ShapeRule.COMPUTED_WITH_FORBID, Forbidden),
        (ShapeRule.JSON_STRING_TYPE, Json[int]),
        (ShapeRule.PLAIN_ENUM_LITERAL, Literal[Color.RED]),
        (ShapeRule.VALIDATOR_REPLACES_CHECK, Annotated[int, PlainValidator(int)]),
        (
            ShapeRule.VALIDATOR_REPLACES_CHECK,
            Annotated[int, WrapValidator(lambda value, handler: handler(value))],
        ),
        (ShapeRule.NO_TYPE, Any),
        (ShapeRule.INVALID_DEFAULT, WrongDefault),
        (ShapeRule.INVALID_DEFAULT, DictDefault),
    )


def verify_shape_rules() -> None:
    """Run each rule on a known shape, once per process.

    A rule reads key names that pydantic owns. A renamed key must not switch a rule
    off, because a silent miss loses data. A miss here is a defect, so it crashes
    loud with the pydantic version, the rule and the fix. It finds a renamed key,
    not a new spelling beside the old one. The CI job on the newest pydantic and
    one test per rule cover that.
    """
    if _RuleWord.verified:
        return
    _RuleWord.verified = True
    for rule, shape in _known_shapes():
        found = shape_refusals(shape, internal_types=frozenset())
        if not any(refusal.rule is rule for refusal in found):
            _RuleWord.verified = False
            raise RuntimeError(
                f'horsies definition check: rule {rule.name} found nothing on a '
                f'known shape under pydantic {pydantic.VERSION}. This pydantic '
                f'changed the core schema. Upgrade horsies, or pin pydantic to a '
                f'version that passes.',
            )
