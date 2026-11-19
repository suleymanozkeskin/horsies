"""The members of a union that a ``type`` alias names, for the tests that
walk an error catalog."""

from __future__ import annotations

from types import UnionType
from typing import Any, Final, TypeAliasType, Union, get_args, get_origin

ALIAS_DEPTH_BOUND: Final = 8
"""The most levels of ``type`` aliases inside one union."""


def union_members(alias: TypeAliasType) -> tuple[Any, ...]:
    """The members of the union that ``alias`` names, in order. A member that
    is itself a ``type`` alias (a generated catalog union such as
    ``BrokerCoreError``) gives its own members in its place. An alias whose
    value is one class, or another alias, gives that class, or the members of
    that alias. Pure."""
    return _members(alias, 0)


def _members(alias: TypeAliasType, depth: int) -> tuple[Any, ...]:
    assert depth <= ALIAS_DEPTH_BOUND, (
        f'type aliases nested deeper than {ALIAS_DEPTH_BOUND} levels: {alias}'
    )
    return _value_members(alias.__value__, depth)


def _value_members(value: Any, depth: int) -> tuple[Any, ...]:
    """The members of one alias value: another alias, a union, or one class."""
    match value:
        case TypeAliasType():
            return _members(value, depth + 1)
        case _ if get_origin(value) in (UnionType, Union):
            members: list[Any] = []
            for member in get_args(value):
                members.extend(_value_members(member, depth))
            return tuple(members)
        case _:
            return (value,)
