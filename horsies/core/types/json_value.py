"""The two JSON types: what a reader gets, and what a position accepts.

`JsonValue` is what a reader gets: an exact `list` or `dict`, so a match over them is
exhaustive. A `list[str]` is not a `list[JsonValue]` to a type checker, because a
`list` is mutable and the receiver could add an int to it.

`JsonInput` is what a position accepts. It holds the same values as `JsonValue`, and
a `list[str]` or a `dict[str, int]` fits it. It exists only for a type checker: at
run time it is `JsonValue`, and the checks of `JsonValue` apply.

This module imports nothing from the library.
"""

from __future__ import annotations

from collections.abc import ItemsView, Iterator, KeysView, ValuesView
from typing import TYPE_CHECKING, Protocol, SupportsIndex


__all__ = ['JsonDict', 'JsonInput', 'JsonList', 'JsonValue']


type JsonValue = (
    None
    | bool
    | int
    | float
    | str
    | list['JsonValue']
    | dict[str, 'JsonValue']
)


class JsonList[T](Protocol):
    """A `list`, with an element type that is covariant.

    `pop` is here for its shape. Only a `list` has a `pop` that takes an index, so a
    tuple, bytes, a deque and a set do not match. The run time refuses each of them.
    """

    def __len__(self) -> int: ...

    def __iter__(self) -> Iterator[T]: ...

    def pop(self, index: SupportsIndex = ..., /) -> T: ...


class JsonDict[T](Protocol):
    """A `dict` with str keys, with a value type that is covariant.

    `__reversed__` and `popitem` are here for their shape. Only a `dict` has both, so
    a `mappingproxy`, a `UserDict`, a `ChainMap` and any other `Mapping` do not match.
    The run time refuses each of them. No member takes a key as an argument, so a
    `dict[Literal['a'], int]` matches.
    """

    def __len__(self) -> int: ...

    def __iter__(self) -> Iterator[str]: ...

    def __reversed__(self) -> Iterator[str]: ...

    def keys(self) -> KeysView[str]: ...

    def values(self) -> ValuesView[T]: ...

    def items(self) -> ItemsView[str, T]: ...

    def popitem(self) -> tuple[str, T]: ...


if TYPE_CHECKING:
    type JsonInput = (
        None
        | bool
        | int
        | float
        | str
        | JsonList[JsonInput]
        | JsonDict[JsonInput]
    )
else:
    JsonInput = JsonValue
