"""The baseline migration text, for tests that check the installed SQL."""

from __future__ import annotations

import re
from typing import Final

import pytest

from horsies.core.baseline.migration import load_baseline_migration
from horsies.core.types.result import Err, Ok

_FUNCTION_HEAD: Final = re.compile(r'^CREATE FUNCTION (\w+)\(([^,)]*)', re.MULTILINE)
_DOLLAR_QUOTE: Final = '$$'


def baseline_sql() -> str:
    """The SQL text that the install runs; fails the test if it is unreadable."""
    match load_baseline_migration():
        case Ok(migration):
            return migration.sql
        case Err(unreadable):
            pytest.fail(f'baseline SQL unreadable: {unreadable}')


def _function_statements() -> list[tuple[str, str, str]]:
    """``(name, first parameter, statement)`` of every ``CREATE FUNCTION``, in install order.

    A statement runs from ``CREATE FUNCTION`` through the ``;`` after the
    closing ``$$`` of its body. Every baseline body is quoted with ``$$`` and
    holds no other ``$$``. Fails the test when a statement has no closing
    quote, or when two statements share a name and a first parameter.
    """
    sql = baseline_sql()
    statements: list[tuple[str, str, str]] = []
    for head in _FUNCTION_HEAD.finditer(sql):
        name, first_parameter = head.group(1), head.group(2).strip()
        body_open = sql.find(_DOLLAR_QUOTE, head.end())
        body_close = sql.find(_DOLLAR_QUOTE, body_open + len(_DOLLAR_QUOTE))
        statement_end = sql.find(';', body_close)
        if body_open < 0 or body_close < 0 or statement_end < 0:
            pytest.fail(f'baseline function {name}: no closing $$; after its body')
        if any((name, first_parameter) == (n, p) for n, p, _ in statements):
            pytest.fail(f'baseline function {name}({first_parameter}, ...) is defined twice')
        statements.append((name, first_parameter, sql[head.start() : statement_end + 1]))
    return statements


def baseline_function_statements() -> list[tuple[str, str]]:
    """``(name, statement)`` of every ``CREATE FUNCTION`` of the baseline, in install order.

    An overloaded name appears once per overload.
    """
    return [(name, statement) for name, _, statement in _function_statements()]


def baseline_function(name: str) -> str:
    """The ``CREATE FUNCTION`` statement of a baseline function with one overload.

    Fails the test when the baseline defines no function of that name, or
    more than one: an overloaded name is read with ``baseline_overload``.
    """
    matches = [statement for n, _, statement in _function_statements() if n == name]
    match len(matches):
        case 0:
            pytest.fail(f'the baseline defines no function {name}')
        case 1:
            return matches[0]
        case count:
            pytest.fail(f'the baseline defines {count} overloads of {name}; use baseline_overload')


def baseline_overload(name: str, first_parameter: str) -> str:
    """The ``CREATE FUNCTION`` statement of one overload, named by its first parameter.

    ``first_parameter`` is the declaration text, for example
    ``'p_task_id uuid'``. Fails the test when no overload matches.
    """
    for n, parameter, statement in _function_statements():
        if (n, parameter) == (name, first_parameter):
            return statement
    pytest.fail(f'the baseline defines no function {name}({first_parameter}, ...)')


def baseline_type(name: str) -> str:
    """The ``CREATE TYPE`` statement of one baseline composite type."""
    match = re.search(
        rf'^CREATE TYPE {re.escape(name)} AS \(.*?^\);$',
        baseline_sql(),
        re.DOTALL | re.MULTILINE,
    )
    if match is None:
        pytest.fail(f'the baseline defines no type {name}')
    return match.group(0)


def baseline_table(name: str) -> str:
    """The ``CREATE TABLE`` statement of one baseline table, partition key included."""
    match = re.search(
        rf'^CREATE TABLE {re.escape(name)} \(.*?^\)[^;]*;',
        baseline_sql(),
        re.DOTALL | re.MULTILINE,
    )
    if match is None:
        pytest.fail(f'the baseline defines no table {name}')
    return match.group(0)
