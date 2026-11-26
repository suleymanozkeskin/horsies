"""Test support for the sync facades that call the core's blocking forms.

A sync facade and its async form share one outcome mapping, so for the same
core answer they return the same outcome. ``outcome_shape`` turns an outcome
into a value that ``==`` compares field by field: the outcome types keep
some fields out of ``repr`` and ``==`` (the ``message`` and ``exception`` of
an error), and an exception compares by identity, so a fresh exception of
the same type and text in each run would not compare equal.
"""

from __future__ import annotations

import dataclasses
from enum import Enum
from typing import Final, cast
from unittest.mock import Mock

from pydantic import BaseModel

from horsies.core.types.result import Err, Ok

SHAPE_DEPTH_BOUND: Final = 32
"""The deepest nesting of an outcome that ``outcome_shape`` walks. An outcome
nested deeper is a defect of the test, and fails an assertion."""


def outcome_shape(value: object) -> object:
    """A form of ``value`` that ``==`` compares field by field.

    - ``Ok`` and ``Err``: the tag and the shape of the value.
    - an exception: its type and the shapes of its ``args``.
    - a dataclass or a Pydantic model: its type and the shape of each field
      (``Succeeded``, ``Failed`` and each read failure are dataclasses).
    - a list, tuple or dict: the shapes of its items, in order.
    - a mock: its identity; the same mock in both runs compares equal.
    - any other value (``str``, ``int``, ``UUID``, ``datetime``, an enum
      member, ``None``): the value itself.
    """
    return _shape(value, 0)


def _shape(value: object, depth: int) -> object:
    assert depth <= SHAPE_DEPTH_BOUND, (
        f'an outcome nested deeper than {SHAPE_DEPTH_BOUND} levels: {value!r}'
    )
    deeper = depth + 1
    match value:
        case Mock():
            return ('mock', id(value))
        case Ok():
            return ('Ok', _shape(cast('Ok[object]', value).ok_value, deeper))
        case Err():
            return ('Err', _shape(cast('Err[object]', value).err_value, deeper))
        case BaseException():
            return (
                type(value).__qualname__,
                tuple(_shape(argument, deeper) for argument in value.args),
            )
        case Enum():
            return value
        case BaseModel():
            return (
                type(value).__qualname__,
                tuple((name, _shape(field, deeper)) for name, field in value),
            )
        case list() | tuple():
            items = cast('list[object] | tuple[object, ...]', value)
            return tuple(_shape(item, deeper) for item in items)
        case dict():
            pairs = cast('dict[object, object]', value)
            return tuple((_shape(key, deeper), _shape(item, deeper)) for key, item in pairs.items())
        case _ if dataclasses.is_dataclass(value) and not isinstance(value, type):
            return (
                type(value).__qualname__,
                tuple(
                    (field.name, _shape(getattr(value, field.name), deeper))
                    for field in dataclasses.fields(value)
                ),
            )
        case _:
            return value
