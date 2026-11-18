"""The members of a union that a ``type`` alias names, for the tests that
walk an error catalog."""

from __future__ import annotations

from typing import Any, Final, TypeAliasType, get_args

ALIAS_DEPTH_BOUND: Final = 8
"""The most levels of ``type`` aliases inside one union."""


def union_members(alias: TypeAliasType) -> tuple[Any, ...]:
    """The members of the union that ``alias`` names, in order. A member that
    is itself a ``type`` alias (a generated catalog union such as
    ``BrokerCoreError``) gives its own members in its place. Pure."""
    return _members(alias, 0)


def _members(alias: TypeAliasType, depth: int) -> tuple[Any, ...]:
    assert depth <= ALIAS_DEPTH_BOUND, (
        f'type aliases nested deeper than {ALIAS_DEPTH_BOUND} levels: {alias}'
    )
    members: list[Any] = []
    for member in get_args(alias.__value__):
        match member:
            case TypeAliasType():
                members.extend(_members(member, depth + 1))
            case _:
                members.append(member)
    return tuple(members)
