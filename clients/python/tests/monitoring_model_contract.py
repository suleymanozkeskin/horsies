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
- ``{"enum": [value, ...]}`` (sorted), ``{"const": value}``;
- ``{"model": {field: kind, ...}}``;
- ``{"union": {"tag": name, "variants": {tag value: {field: kind, ...}}}}``:
  a union of models told apart by the value of one field, the tag, which the
  variant fields leave out.

This module is pure. It does no I/O but in ``write_contract``.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Final, cast

from pydantic import BaseModel

from horsies.monitoring import models

type Schema = dict[str, object]
type Kind = str | dict[str, object]

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
    models.TaskAttemptInfo,
    models.LeafTaskInfo,
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
    models.WorkflowNodeInfo,
    models.WorkflowEdge,
    models.WorkflowRunDetail,
    models.WorkflowTaskDetail,
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


def _definition(defs: Schema, reference: object) -> Schema:
    assert isinstance(reference, str) and reference.startswith('#/$defs/'), reference
    return _mapping(defs[reference.removeprefix('#/$defs/')])


def _fields(schema: Schema, defs: Schema, depth: int, *, without: str | None) -> dict[str, Kind]:
    properties = _mapping(schema.get('properties', {}))
    return {
        name: _kind(_mapping(field), defs, depth + 1)
        for name, field in properties.items()
        if name != without
    }


def _union(schema: Schema, defs: Schema, depth: int) -> Kind:
    discriminator = _mapping(schema['discriminator'])
    tag = discriminator['propertyName']
    assert isinstance(tag, str), discriminator
    variants: dict[str, object] = {}
    for member in _items(schema['oneOf']):
        variant = _definition(defs, _mapping(member)['$ref'])
        tag_schema = _mapping(_mapping(variant['properties'])[tag])
        value = tag_schema['const']
        assert isinstance(value, str), tag_schema
        variants[value] = _fields(variant, defs, depth, without=tag)
    return {'union': {'tag': tag, 'variants': variants}}


def _kind(schema: Schema, defs: Schema, depth: int) -> Kind:
    """The kind of one schema. Raises ``AssertionError`` for a schema form
    that no contract model has."""
    assert depth <= _MAX_DEPTH, f'the schema nests deeper than {_MAX_DEPTH}'
    if '$ref' in schema:
        target = _definition(defs, schema['$ref'])
        if 'enum' in target:
            return {'enum': sorted(cast(list[str], target['enum']))}
        return {'model': _fields(target, defs, depth, without=None)}
    if 'discriminator' in schema:
        return _union(schema, defs, depth)
    if 'anyOf' in schema:
        members = [_mapping(member) for member in _items(schema['anyOf'])]
        others = [member for member in members if member.get('type') != 'null']
        assert len(members) == 2 and len(others) == 1, schema
        return {'nullable': _kind(others[0], defs, depth + 1)}
    if 'const' in schema:
        return {'const': schema['const']}
    if 'enum' in schema:
        return {'enum': sorted(cast(list[str], schema['enum']))}
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
            return {'list': _kind(_mapping(schema['items']), defs, depth + 1)}
        case 'object':
            return {'map': _kind(_mapping(schema['additionalProperties']), defs, depth + 1)}
        case str() as primitive if primitive in _PRIMITIVES:
            return _PRIMITIVES[primitive]
        case other:
            raise AssertionError(f'a schema of type {other!r}: {schema}')


def model_contract(model: type[BaseModel]) -> dict[str, Kind]:
    """The contract of one model: its fields and their kinds."""
    schema = model.model_json_schema(mode='serialization')
    defs = _mapping(schema.get('$defs', {}))
    return _fields(schema, defs, 0, without=None)


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
