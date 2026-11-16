"""The baseline migration text, for tests that check the installed SQL."""

from __future__ import annotations

import re
from typing import Final

import pytest

from horsies.core.baseline.migration import load_baseline_migration
from horsies.core.types.result import Err, Ok

_FUNCTION_HEAD: Final = re.compile(r'^CREATE FUNCTION (\w+)\(', re.MULTILINE)
_DOLLAR_QUOTE: Final = '$$'


def baseline_sql() -> str:
    """The SQL text that the install runs; fails the test if it is unreadable."""
    match load_baseline_migration():
        case Ok(migration):
            return migration.sql
        case Err(unreadable):
            pytest.fail(f'baseline SQL unreadable: {unreadable}')


def baseline_functions() -> dict[str, str]:
    """Every ``CREATE FUNCTION`` statement of the baseline, by function name.

    A statement runs from ``CREATE FUNCTION`` through the ``;`` after the
    closing ``$$`` of its body. Every baseline body is quoted with ``$$`` and
    holds no other ``$$``. Fails the test when a statement has no closing
    quote or a name is defined twice.
    """
    sql = baseline_sql()
    functions: dict[str, str] = {}
    for head in _FUNCTION_HEAD.finditer(sql):
        name = head.group(1)
        body_open = sql.find(_DOLLAR_QUOTE, head.end())
        body_close = sql.find(_DOLLAR_QUOTE, body_open + len(_DOLLAR_QUOTE))
        statement_end = sql.find(';', body_close)
        if body_open < 0 or body_close < 0 or statement_end < 0:
            pytest.fail(f'baseline function {name}: no closing $$; after its body')
        if name in functions:
            pytest.fail(f'baseline function {name} is defined twice')
        functions[name] = sql[head.start() : statement_end + 1]
    return functions


def baseline_function(name: str) -> str:
    """The ``CREATE FUNCTION`` statement of one baseline function."""
    functions = baseline_functions()
    if name not in functions:
        pytest.fail(f'the baseline defines no function {name}')
    return functions[name]
