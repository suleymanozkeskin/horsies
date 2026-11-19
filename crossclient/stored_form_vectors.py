"""Python's side of the shared golden vectors of the stored JSON forms.

``core/tests/fixtures/stored_forms/`` holds one file per stored
form. Each vector is a value and the exact text that Python stores for it. The
Python tests (``tests/test_stored_form_*.py``) encode each value to the
text and decode the text to the value; the core and the Rust client test the
same files. The folder's ``README.md`` names the encoder and the decoder of
each form in each client.

A typed value is written in a neutral notation, so that each client builds it
with its own types: ``{"type": <type name>, "value": <notation>}``. This
module turns the notation into a Python value and its annotation. Integers are
decimal text, so that a reader of the file never rounds one. The type names:

- ``none``: ``null``; Python ``None``, Rust ``()``.
- ``bool``: a JSON boolean.
- ``i64``, ``u64``, ``i128``, ``u128``, ``big_int``: decimal text; the name is
  the Rust type that holds the value (``big_int``: none does).
- ``f64``: the text of a float, ``nan``, ``inf`` or ``-inf``.
- ``str``: a JSON string.
- ``datetime_utc``, ``datetime_offset``, ``datetime_naive``: ISO 8601 text.
- ``date``: ISO 8601 date text.
- ``uuid``: hyphenated UUID text.
- ``decimal``: decimal text.
- ``bytes``: the bytes as UTF-8 text.
- ``str_enum``: a value of ``Color``; ``int_enum``: a value of ``Level``, as
  decimal text.
- ``optional_i64``: ``null`` or decimal text.
- ``list_i64``: a list of decimal texts.
- ``map_str_i64``: an object of decimal texts; the member order is the order
  of the value.
- ``json``: the JSON value itself.
- ``record_order``: an ``Order`` as ``{"id": <decimal text>, "amount": <float
  text>}``.

A task error is written as ``{"error_code": {"built_in": <name>} | {"user":
<text>}, "message": <text or null>, "data": <JSON or null>, "exception":
<the five texts of a flattened exception, or null>}``, with ``"sub_workflow":
{"id": <UUID text>, "summary": <the summary's fields>}`` for a sub-workflow
error.

This module is pure. It does no I/O but in ``load_form``.
"""

from __future__ import annotations

import datetime as dt
import json
from decimal import Decimal
from enum import Enum, IntEnum
from pathlib import Path
from typing import Final, cast
from uuid import UUID

from pydantic import BaseModel, ConfigDict

from horsies.core.models.tasks import (
    BUILTIN_CODE_REGISTRY,
    BuiltInTaskCode,
    FlattenedException,
    SubWorkflowError,
    TaskError,
)
from horsies.core.models.workflow import SubWorkflowSummary, WorkflowStatus
from horsies.core.types.json_value import JsonValue

VECTORS_DIR: Final = (
    Path(__file__).resolve().parents[1]
    / 'core'
    / 'tests'
    / 'fixtures'
    / 'stored_forms'
)

type Notation = object
"""A value in the neutral notation of the vector files."""


class Color(str, Enum):
    """The enum with text values of the vectors."""

    RED = 'red'
    GREEN = 'green'


class Level(IntEnum):
    """The enum with integer values of the vectors."""

    LOW = 1
    HIGH = 3


class Order(BaseModel):
    """The record of the vectors: two fields, in this order."""

    model_config = ConfigDict(extra='forbid')

    id: int
    amount: float


def load_form(file_name: str) -> dict[str, object]:
    """The parsed vector file ``file_name`` of the vector folder."""
    document: object = json.loads((VECTORS_DIR / file_name).read_text(encoding='utf-8'))
    assert isinstance(document, dict), f'{file_name} is not a JSON object'
    return cast('dict[str, object]', document)


def rows(document: dict[str, object], key: str) -> list[dict[str, object]]:
    """The list of objects at ``key`` of a vector file."""
    listed = document[key]
    assert isinstance(listed, list), f'{key} is not a list'
    found: list[dict[str, object]] = []
    for row in cast('list[object]', listed):
        assert isinstance(row, dict), f'a row of {key} is not an object'
        found.append(cast('dict[str, object]', row))
    return found


def text_of(row: dict[str, object], key: str) -> str:
    """The string at ``key`` of a row."""
    value = row[key]
    assert isinstance(value, str), f'{key} is not a string'
    return value


def _decimal_text(notation: Notation) -> str:
    assert isinstance(notation, str), f'expected decimal text, got {notation!r}'
    return notation


def _integer(notation: Notation) -> int:
    return int(_decimal_text(notation))


def _datetime(notation: Notation) -> dt.datetime:
    return dt.datetime.fromisoformat(_decimal_text(notation))


def annotation_of(type_name: str) -> object:
    """The Python annotation of the vector type ``type_name``."""
    match type_name:
        case 'none':
            return type(None)
        case 'bool':
            return bool
        case 'i64' | 'u64' | 'i128' | 'u128' | 'big_int':
            return int
        case 'f64':
            return float
        case 'str':
            return str
        case 'datetime_utc' | 'datetime_offset' | 'datetime_naive':
            return dt.datetime
        case 'date':
            return dt.date
        case 'uuid':
            return UUID
        case 'decimal':
            return Decimal
        case 'bytes':
            return bytes
        case 'str_enum':
            return Color
        case 'int_enum':
            return Level
        case 'optional_i64':
            return int | None
        case 'list_i64':
            return list[int]
        case 'map_str_i64':
            return dict[str, int]
        case 'json':
            return JsonValue
        case 'record_order':
            return Order
        case _:
            raise AssertionError(f'unknown vector type {type_name!r}')


def value_of(type_name: str, notation: Notation) -> object:
    """The Python value of ``notation`` under the vector type ``type_name``."""
    match type_name:
        case 'none':
            assert notation is None
            return None
        case 'bool':
            assert isinstance(notation, bool)
            return notation
        case 'i64' | 'u64' | 'i128' | 'u128' | 'big_int':
            return _integer(notation)
        case 'f64':
            return float(_decimal_text(notation))
        case 'str':
            assert isinstance(notation, str)
            return notation
        case 'datetime_utc' | 'datetime_offset' | 'datetime_naive':
            return _datetime(notation)
        case 'date':
            return dt.date.fromisoformat(_decimal_text(notation))
        case 'uuid':
            return UUID(_decimal_text(notation))
        case 'decimal':
            return Decimal(_decimal_text(notation))
        case 'bytes':
            return _decimal_text(notation).encode('utf-8')
        case 'str_enum':
            return Color(_decimal_text(notation))
        case 'int_enum':
            return Level(_integer(notation))
        case 'optional_i64':
            return None if notation is None else _integer(notation)
        case 'list_i64':
            assert isinstance(notation, list)
            return [_integer(item) for item in cast('list[object]', notation)]
        case 'map_str_i64':
            assert isinstance(notation, dict)
            members = cast('dict[str, object]', notation)
            return {key: _integer(item) for key, item in members.items()}
        case 'json':
            return notation
        case 'record_order':
            assert isinstance(notation, dict)
            fields = cast('dict[str, object]', notation)
            return Order(
                id=_integer(fields['id']),
                amount=float(_decimal_text(fields['amount'])),
            )
        case _:
            raise AssertionError(f'unknown vector type {type_name!r}')


def typed(row: dict[str, object]) -> tuple[object, object]:
    """The value and the annotation of a typed row ``{"type": ..., "value": ...}``."""
    type_name = text_of(row, 'type')
    return value_of(type_name, row['value']), annotation_of(type_name)


def _members(notation: object, what: str) -> dict[str, object]:
    assert isinstance(notation, dict), f'{what} is not an object'
    return cast('dict[str, object]', notation)


def _error_code(notation: object) -> BuiltInTaskCode | str:
    match _members(notation, 'error_code'):
        case {'built_in': str() as name}:
            return BUILTIN_CODE_REGISTRY[name]
        case {'user': str() as text}:
            return text
        case other:
            raise AssertionError(f'unknown error code notation {other!r}')


def _exception(notation: object) -> FlattenedException | None:
    if notation is None:
        return None
    fields = _members(notation, 'exception')
    return FlattenedException(
        type=text_of(fields, 'type'),
        module=text_of(fields, 'module'),
        message=text_of(fields, 'message'),
        repr=text_of(fields, 'repr'),
        traceback=text_of(fields, 'traceback'),
    )


def _optional_text(fields: dict[str, object], key: str) -> str | None:
    value = fields[key]
    assert value is None or isinstance(value, str), f'{key} is not text or null'
    return value


def _count(fields: dict[str, object], key: str) -> int:
    value = fields[key]
    assert isinstance(value, int), f'{key} is not an integer'
    return value


def _summary(notation: object) -> SubWorkflowSummary[object]:
    fields = _members(notation, 'summary')
    return SubWorkflowSummary(
        status=WorkflowStatus(text_of(fields, 'status')),
        output=fields['output'],
        total_tasks=_count(fields, 'total_tasks'),
        completed_tasks=_count(fields, 'completed_tasks'),
        failed_tasks=_count(fields, 'failed_tasks'),
        skipped_tasks=_count(fields, 'skipped_tasks'),
        error_summary=_optional_text(fields, 'error_summary'),
    )


def task_error_of(notation: object) -> TaskError:
    """The ``TaskError`` (or ``SubWorkflowError``) of a task error notation."""
    fields = _members(notation, 'error')
    code = _error_code(fields['error_code'])
    message = _optional_text(fields, 'message')
    exception = _exception(fields['exception'])
    match fields.get('sub_workflow'):
        case None:
            return TaskError(
                error_code=code,
                message=message,
                data=cast('JsonValue', fields['data']),
                exception=exception,
            )
        case sub_workflow:
            sub = _members(sub_workflow, 'sub_workflow')
            return SubWorkflowError(
                error_code=code,
                message=message,
                data=cast('JsonValue', fields['data']),
                exception=exception,
                sub_workflow_id=text_of(sub, 'id'),
                sub_workflow_summary=_summary(sub['summary']),
            )
