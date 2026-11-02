"""The `JsonValue` alias. This module imports nothing from the library."""

from __future__ import annotations


__all__ = ['JsonValue']


type JsonValue = (
    None
    | bool
    | int
    | float
    | str
    | list['JsonValue']
    | dict[str, 'JsonValue']
)
