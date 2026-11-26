"""The wire contract of the monitoring read models, from their Pydantic schemas.

The core returns the JSON of its serde models, and ``horsies.monitoring``
validates it into the Pydantic models of ``horsies.monitoring.models``. This
module reduces the Pydantic JSON schema of each model to its contract: the
field names, and per field the kind of value it holds. The core compares the
JSON schema of each Rust model with this contract
(``core/src/monitoring/model_contract_tests.rs``), with the
known compatible differences named there. The contract is committed as
``core/src/monitoring/model_contract.json``;
``tests/unit/test_monitoring_model_contract.py`` checks that the file is the
contract of the models of today.

Write the file after a change of the models:
``uv run python -m tests.monitoring_model_contract``.

A kind is one of:

- ``"string"``, ``"integer"``, ``"number"``, ``"boolean"``, ``"date-time"``,
  ``"uuid"``;
- ``{"nullable": kind}``, ``{"list": kind}``, ``{"map": kind}`` (an object
  with text keys);
- ``{"enum": {"name": name, "values": [value, ...]}}`` (values sorted),
  ``{"const": value}``;
- ``{"model": {"name": name, "fields": {field: kind, ...}}}``;
- ``{"union": {"name": name, "tag": tag, "variants": {tag value: {"name":
  name, "fields": {field: kind, ...}}}}}``: a union of models told apart by
  the value of one field, the tag, which the variant fields leave out.

Each model, variant, union and enum has its name, so that a client renders
one type per name:

- a model, a variant and an enum class: the class name;
- a union: the name of its alias in ``horsies.monitoring.models``
  (``AttemptPhasesInfo``), found by the class names of its variants;
- a union or an enum that a field writes inline, with no Python name: the
  name of its Rust type, from ``INLINE_KIND_NAMES``.

The core test checks each name against the Rust type.

This module is pure. It does no I/O but in ``write_contract``.
"""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Annotated, Final, cast, get_args, get_origin

from pydantic import BaseModel
from pydantic.fields import FieldInfo

from horsies.monitoring import models

type Schema = dict[str, object]
type Kind = str | dict[str, object]
type Place = tuple[str, str]
"""A field: the name of the model or variant that has it, and its key."""

CONTRACT_PATH: Final = (
    Path(__file__).resolve().parents[3]
    / 'core'
    / 'src'
    / 'monitoring'
    / 'model_contract.json'
)

# The models whose JSON the core writes for a read or a web response, by the
# name of both the Pydantic class and the Rust type.
CONTRACT_MODELS: Final[tuple[type[BaseModel], ...]] = (
    models.TaskAttemptDetail,
    models.TaskRowInfo,
    models.StatusCount,
    models.FacetValue,
    models.ErrorFacet,
    models.Facets,
    models.BreakdownCounts,
    models.GroupRow,
    models.Breakdown,
    models.TaskSummary,
    models.TaskListPage,
    models.TaskDetail,
    models.WorkflowRunSummary,
    models.WorkflowNodeSummary,
    models.WorkflowEdge,
    models.WorkflowRunDetail,
    models.WorkflowNodeDetail,
    models.DownSlotInfo,
    models.WorkerStateInfo,
    models.BudgetLoadInfo,
    models.WorkerClassLoadInfo,
    models.PendingStreamInfo,
    models.ClassReportInfo,
    models.WorkerPingInfo,
    models.LivenessReport,
    models.WorkerHistoryPoint,
    models.ScheduleStateInfo,
)

_MAX_DEPTH: Final = 16
"""The deepest kind that a contract model reaches is at depth 9: the placement
of the claim of a segment of the timing of a node of a run
(``WorkflowRunDetail``). A deeper schema is a defect of the reduction."""

INLINE_KIND_NAMES: Final[Mapping[Place, str]] = {
    ('EndpointTerminalInfo', 'status'): 'TerminalNodeStatus',
    ('PendingStreamInfo', 'coverage'): 'StreamCoverageInfo',
    ('PendingStreamInfo', 'wait'): 'StreamWaitInfo',
    ('WorkerStateInfo', 'slot_health'): 'SlotHealthKind',
}
"""The name of each union and enum that a contract model writes inline, by
its field: the name of the Rust type."""


@dataclass(frozen=True)
class KindNames:
    """The names of the kinds that a JSON schema does not name."""

    unions: Mapping[frozenset[str], str]
    """The name of each union alias, by the class names of its variants."""
    inline: Mapping[Place, str]
    """The name of a union with no alias or of an inline enum, by its field."""


@dataclass(frozen=True)
class _Reduction:
    """The definitions of one model's schema, and the names of its kinds."""

    defs: Schema
    names: KindNames


_PRIMITIVES: Final[dict[str, str]] = {
    'integer': 'integer',
    'number': 'number',
    'boolean': 'boolean',
}


def _mapping(value: object) -> Schema:
    assert isinstance(value, dict), f'a schema is an object, not {value!r}'
    return cast(Schema, value)


def _items(value: object) -> list[object]:
    assert isinstance(value, list), f'a schema list is an array, not {value!r}'
    return cast(list[object], value)


def _definition(defs: Schema, reference: object) -> tuple[str, Schema]:
    """The name and the schema of the definition that ``reference`` names."""
    assert isinstance(reference, str) and reference.startswith('#/$defs/'), reference
    name = reference.removeprefix('#/$defs/')
    assert name.isidentifier(), f'the definition name {name!r} is not a class name'
    return name, _mapping(defs[name])


def _inline_name(reduction: _Reduction, place: Place, shape: str) -> str:
    """The name of the inline union or enum of the field ``place``."""
    owner, field = place
    name = reduction.names.inline.get(place)
    assert name is not None, (
        f'{owner}.{field}: an inline {shape} has no name in INLINE_KIND_NAMES'
    )
    return name


def _fields(
    schema: Schema,
    reduction: _Reduction,
    depth: int,
    *,
    owner: str,
    without: str | None,
) -> dict[str, Kind]:
    properties = _mapping(schema.get('properties', {}))
    return {
        name: _kind(_mapping(field), reduction, depth + 1, (owner, name))
        for name, field in properties.items()
        if name != without
    }


def _union(schema: Schema, reduction: _Reduction, depth: int, place: Place) -> Kind:
    discriminator = _mapping(schema['discriminator'])
    tag = discriminator['propertyName']
    assert isinstance(tag, str), discriminator
    variants: dict[str, object] = {}
    variant_names: set[str] = set()
    for member in _items(schema['oneOf']):
        variant_name, variant = _definition(reduction.defs, _mapping(member)['$ref'])
        tag_schema = _mapping(_mapping(variant['properties'])[tag])
        value = tag_schema['const']
        assert isinstance(value, str), tag_schema
        variants[value] = {
            'name': variant_name,
            'fields': _fields(
                variant, reduction, depth, owner=variant_name, without=tag
            ),
        }
        variant_names.add(variant_name)
    match reduction.names.unions.get(frozenset(variant_names)):
        case str() as alias:
            name = alias
        case None:
            name = _inline_name(reduction, place, 'union')
    return {'union': {'name': name, 'tag': tag, 'variants': variants}}


def _kind(schema: Schema, reduction: _Reduction, depth: int, place: Place) -> Kind:
    """The kind of the schema of the field ``place``. Raises
    ``AssertionError`` for a schema form that no contract model has, and for
    a union or an enum with no name."""
    assert depth <= _MAX_DEPTH, f'the schema nests deeper than {_MAX_DEPTH}'
    if '$ref' in schema:
        name, target = _definition(reduction.defs, schema['$ref'])
        if 'enum' in target:
            return {
                'enum': {
                    'name': name,
                    'values': sorted(cast(list[str], target['enum'])),
                }
            }
        fields = _fields(target, reduction, depth, owner=name, without=None)
        return {'model': {'name': name, 'fields': fields}}
    if 'discriminator' in schema:
        return _union(schema, reduction, depth, place)
    if 'anyOf' in schema:
        members = [_mapping(member) for member in _items(schema['anyOf'])]
        others = [member for member in members if member.get('type') != 'null']
        assert len(members) == 2 and len(others) == 1, schema
        return {'nullable': _kind(others[0], reduction, depth + 1, place)}
    if 'const' in schema:
        return {'const': schema['const']}
    if 'enum' in schema:
        values = sorted(cast(list[str], schema['enum']))
        return {
            'enum': {'name': _inline_name(reduction, place, 'enum'), 'values': values}
        }
    match schema.get('type'):
        case 'string':
            match schema.get('format'):
                case 'date-time':
                    return 'date-time'
                case 'uuid':
                    return 'uuid'
                case None:
                    return 'string'
                case other:
                    raise AssertionError(f'a string of format {other!r}')
        case 'array':
            return {
                'list': _kind(_mapping(schema['items']), reduction, depth + 1, place)
            }
        case 'object':
            values = _mapping(schema['additionalProperties'])
            return {'map': _kind(values, reduction, depth + 1, place)}
        case str() as primitive if primitive in _PRIMITIVES:
            return _PRIMITIVES[primitive]
        case other:
            raise AssertionError(f'a schema of type {other!r}: {schema}')


def union_names(namespace: Mapping[str, object]) -> dict[frozenset[str], str]:
    """The name of each discriminated union alias of ``namespace`` (an
    ``Annotated`` union with a ``discriminator``), by the class names of its
    variants. Raises ``AssertionError`` when two aliases have one variant
    set."""
    names: dict[frozenset[str], str] = {}
    for name, value in namespace.items():
        if get_origin(value) is not Annotated:
            continue
        union, *metadata = get_args(value)
        if not any(
            isinstance(item, FieldInfo) and item.discriminator is not None
            for item in metadata
        ):
            continue
        variants = frozenset(cast(type, member).__name__ for member in get_args(union))
        assert variants not in names, (
            f'{name} and {names[variants]} are aliases of one union'
        )
        names[variants] = name
    return names


MODEL_KIND_NAMES: Final = KindNames(
    unions=union_names(vars(models)), inline=INLINE_KIND_NAMES
)
"""The kind names of the models of ``horsies.monitoring.models``."""


def model_contract(
    model: type[BaseModel], names: KindNames = MODEL_KIND_NAMES
) -> dict[str, Kind]:
    """The contract of one model: its fields and their kinds."""
    schema = model.model_json_schema(mode='serialization')
    reduction = _Reduction(defs=_mapping(schema.get('$defs', {})), names=names)
    return _fields(schema, reduction, 0, owner=model.__name__, without=None)


def contract() -> dict[str, dict[str, Kind]]:
    """The contract of every model of ``CONTRACT_MODELS``, by model name."""
    return {model.__name__: model_contract(model) for model in CONTRACT_MODELS}


def contract_text() -> str:
    """The contract as the text of the committed file."""
    return json.dumps(contract(), indent=2, sort_keys=True) + '\n'


def write_contract() -> None:
    """Write the contract file of the models of today."""
    _ = CONTRACT_PATH.write_text(contract_text(), encoding='utf-8')


if __name__ == '__main__':
    write_contract()
