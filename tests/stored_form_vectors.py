"""Python's side of the shared golden vectors of the stored JSON forms.

``crates/horsies-core/tests/fixtures/stored_forms/`` holds one file per stored
form. Each vector is a value and the exact text that Python stores for it. The
Python tests (``tests/unit/test_stored_form_*.py``) encode each value to the
text and decode the text to the value; the core and the Rust client test the
same files. The folder's ``README.md`` names the encoder and the decoder of
each form in each client.

A typed value is written in a neutral notation, so that each client builds it
with its own types: ``{"type": <type name>, "value": <notation>}``. This
module turns the notation into a Python value and its annotation. Integers are
decimal text, so that a reader of the file never rounds one. The type names:

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

from horsies.core.types.json_value import JsonValue

VECTORS_DIR: Final = (
    Path(__file__).resolve().parents[1]
    / 'crates'
    / 'horsies-core'
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
