"""Typed values that several error catalogs share.

Each value is a plain frozen type. The type lives here so that no error
catalog depends on another one.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(slots=True, frozen=True)
class NoCause:
    """No Python exception caused this failure: a check or a comparison found it.

    It has no fields, so every instance is equal and ``case NoCause():``
    matches it. Match on the cause field before a ``raise ... from``, an
    ``exc_info=`` or an error tracker call.
    """
