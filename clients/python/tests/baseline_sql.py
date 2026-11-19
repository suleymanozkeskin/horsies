"""The baseline migration text, for tests that check the installed SQL.

The text is the baseline SQL files that ``horsies._core`` compiles in
(``baseline_sql_files``), concatenated in their run order with no
separator: migration 1000 as the core installs it.
"""

from __future__ import annotations

import hashlib
import re
from typing import Final

import pytest

import horsies._core as _core

_FUNCTION_HEAD: Final = re.compile(r'^CREATE FUNCTION (\w+)\(', re.MULTILINE)
_DOLLAR_QUOTE: Final = '$$'
_RETURNS_TABLE: Final = 'RETURNS TABLE('


def baseline_sql() -> str:
    """The SQL text of migration 1000, the text that the install runs."""
    return ''.join(text for _, text in _core.baseline_sql_files())


def baseline_checksum() -> bytes:
    """The checksum that the ``horsies_migrations`` row of migration 1000
    stores: SHA-384 of the UTF-8 bytes of ``baseline_sql()``."""
    return hashlib.sha384(baseline_sql().encode('utf-8')).digest()


def _parameter_list(name: str, sql: str, list_start: int) -> tuple[tuple[str, ...], int]:
    """The parameter declarations of the list that starts at ``list_start``, and the index after its ``)``.

    ``list_start`` is the index just after the opening ``(``. The list is
    split at commas outside parentheses; whitespace in each declaration is
    collapsed to one space. Fails the test when the list does not close.
    """
    depth = 0
    pieces: list[str] = []
    piece_start = list_start
    for index in range(list_start, len(sql)):
        match sql[index]:
            case '(':
                depth += 1
            case ')' if depth > 0:
                depth -= 1
            case ')':
                pieces.append(sql[piece_start:index])
                declarations = tuple(' '.join(piece.split()) for piece in pieces)
                return tuple(d for d in declarations if d), index + 1
            case ',' if depth == 0:
                pieces.append(sql[piece_start:index])
                piece_start = index + 1
            case _:
                pass
    pytest.fail(f'baseline function {name}: its parameter list does not close')


def _function_statements() -> list[tuple[str, tuple[str, ...], str]]:
    """``(name, parameter declarations, statement)`` of every ``CREATE FUNCTION``, in install order.

    A statement runs from ``CREATE FUNCTION`` through the ``;`` after the
    closing ``$$`` of its body. Every baseline body is quoted with ``$$`` and
    holds no other ``$$``. Fails the test when a statement has no closing
    quote, or when two statements share a name and a parameter list.
    """
    sql = baseline_sql()
    statements: list[tuple[str, tuple[str, ...], str]] = []
    for head in _FUNCTION_HEAD.finditer(sql):
        name = head.group(1)
        parameters, list_end = _parameter_list(name, sql, head.end())
        body_open = sql.find(_DOLLAR_QUOTE, list_end)
        body_close = sql.find(_DOLLAR_QUOTE, body_open + len(_DOLLAR_QUOTE))
        statement_end = sql.find(';', body_close)
        if body_open < 0 or body_close < 0 or statement_end < 0:
            pytest.fail(f'baseline function {name}: no closing $$; after its body')
        if any((name, parameters) == (n, p) for n, p, _ in statements):
            pytest.fail(f'baseline function {name}({", ".join(parameters)}) is defined twice')
        statements.append((name, parameters, sql[head.start() : statement_end + 1]))
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


def baseline_returned_columns(name: str) -> tuple[tuple[str, str], ...]:
    """``(column, type)`` of each column of the ``RETURNS TABLE`` of a baseline function with one overload, in order.

    Fails the test when the function returns no table.
    """
    statement = baseline_function(name)
    start = statement.find(_RETURNS_TABLE)
    if start < 0:
        pytest.fail(f'baseline function {name} returns no table')
    declarations, _ = _parameter_list(name, statement, start + len(_RETURNS_TABLE))
    return tuple(
        (column, column_type)
        for column, _, column_type in (
            declaration.partition(' ') for declaration in declarations
        )
    )


def baseline_overload(name: str, first_parameter: str, *later_parameters: str) -> str:
    """The ``CREATE FUNCTION`` statement of one overload, named by its leading parameters.

    Each parameter is the declaration text, for example ``'p_task_id uuid'``.
    Two overloads that share a first parameter are told apart by naming more
    parameters. Fails the test when no overload, or more than one, starts
    with the given parameters.
    """
    leading = (first_parameter, *later_parameters)
    matches = [
        statement
        for n, parameters, statement in _function_statements()
        if n == name and parameters[: len(leading)] == leading
    ]
    match len(matches):
        case 0:
            pytest.fail(f'the baseline defines no function {name}({", ".join(leading)}, ...)')
        case 1:
            return matches[0]
        case count:
            pytest.fail(
                f'{count} overloads of {name} start with ({", ".join(leading)}); '
                'name more parameters',
            )


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
