"""The SQL text of the migration chain, for tests that check the installed SQL.

``horsies._core`` compiles in every SQL file of the chain
(``migration_sql_files``: migration version, file name, text, in run
order). Two readings of it:

- The baseline: the files of migration 1000, concatenated in their run
  order with no separator, as the core installs it (``baseline_sql``). Its
  tables and types are read here (``baseline_table``, ``baseline_type``): a
  later migration changes a table with ``ALTER TABLE`` and keeps the
  ``CREATE TABLE`` text of the baseline.
- The chain: the text of every migration in run order (``chain_sql``). A
  function is read as the chain leaves it installed (``installed_function``
  and the helpers next to it): a later ``CREATE OR REPLACE FUNCTION``
  replaces the definition with the same parameters, and a ``DROP FUNCTION``
  removes the overload with its argument types.
"""

from __future__ import annotations

import hashlib
import re
from typing import Final

import pytest

import horsies._core as _core

BASELINE_MIGRATION_VERSION: Final = 1000
"""The version of the baseline migration (``core/src/broker/migrations.rs``)."""

_FUNCTION_EVENT: Final = re.compile(
    r'^(?P<verb>CREATE FUNCTION|CREATE OR REPLACE FUNCTION|DROP FUNCTION) (?P<name>\w+)\(',
    re.MULTILINE,
)
_DOLLAR_QUOTE: Final = '$$'
_RETURNS_TABLE: Final = 'RETURNS TABLE('
_OUT_MODE: Final = 'OUT'


def migration_files() -> list[tuple[int, str, str]]:
    """``(migration version, file name, text)`` of every file of the chain, in run order."""
    return [
        (version, name, text) for version, name, text in _core.migration_sql_files()
    ]


def migration_versions() -> tuple[int, ...]:
    """The versions of the chain, in apply order."""
    versions: list[int] = []
    for version, _, _ in migration_files():
        if version not in versions:
            versions.append(version)
    return tuple(versions)


def migration_sql(version: int) -> str:
    """The text of one migration: its files in run order, with no separator.

    Fails the test when the chain has no migration of that version.
    """
    texts = [text for file_version, _, text in migration_files() if file_version == version]
    if not texts:
        pytest.fail(f'the chain has no migration {version}')
    return ''.join(texts)


def migration_checksum(version: int) -> bytes:
    """The checksum that the ``horsies_migrations`` row of one migration
    stores: SHA-384 of the UTF-8 bytes of its text."""
    return hashlib.sha384(migration_sql(version).encode('utf-8')).digest()


def baseline_sql() -> str:
    """The SQL text of migration 1000, the text that the install runs first."""
    return ''.join(text for _, text in _core.baseline_sql_files())


def baseline_checksum() -> bytes:
    """The checksum that the ``horsies_migrations`` row of migration 1000
    stores: SHA-384 of the UTF-8 bytes of ``baseline_sql()``."""
    return hashlib.sha384(baseline_sql().encode('utf-8')).digest()


def chain_sql() -> str:
    """The text of every migration of the chain, in run order, with no
    separator: the text that installs the schema of this build."""
    return ''.join(text for _, _, text in migration_files())


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
    pytest.fail(f'function {name}: its parameter list does not close')


def _argument_types(declarations: tuple[str, ...]) -> tuple[str, ...]:
    """The argument types of a parameter list: each declaration less its
    parameter name, OUT parameters left out (they are not part of the
    function's identity, as ``DROP FUNCTION`` names it)."""
    types: list[str] = []
    for declaration in declarations:
        match declaration.split(' ', 1):
            case [mode, _] if mode == _OUT_MODE:
                continue
            case [_, declared_type]:
                types.append(declared_type)
            case _:
                pytest.fail(f'parameter {declaration!r} has no name and type')
    return tuple(types)


def _create_statement(name: str, sql: str, start: int, list_end: int) -> str:
    """The ``CREATE`` statement from ``start`` through the ``;`` after the
    closing ``$$`` of its body. Every body of the chain is quoted with ``$$``
    and holds no other ``$$``. Fails the test when there is no closing quote."""
    body_open = sql.find(_DOLLAR_QUOTE, list_end)
    body_close = sql.find(_DOLLAR_QUOTE, body_open + len(_DOLLAR_QUOTE))
    statement_end = sql.find(';', body_close)
    if body_open < 0 or body_close < 0 or statement_end < 0:
        pytest.fail(f'function {name}: no closing $$; after its body')
    return sql[start : statement_end + 1]


def _function_statements() -> list[tuple[str, tuple[str, ...], str]]:
    """``(name, parameter declarations, statement)`` of every function that the chain leaves installed, in install order.

    The chain is read in run order:
    - ``CREATE FUNCTION`` adds a function; a second one with the same name and
      argument types fails the test.
    - ``CREATE OR REPLACE FUNCTION`` replaces the statement of the function
      with the same name and argument types in its place, or adds it.
    - ``DROP FUNCTION`` removes the function with that name and argument
      types; a drop of a function that is not installed fails the test.
    """
    sql = chain_sql()
    installed: dict[tuple[str, tuple[str, ...]], tuple[str, tuple[str, ...], str]] = {}
    for event in _FUNCTION_EVENT.finditer(sql):
        name = event.group('name')
        parameters, list_end = _parameter_list(name, sql, event.end())
        match event.group('verb'):
            case 'DROP FUNCTION':
                key = (name, parameters)
                if key not in installed:
                    pytest.fail(f'the chain drops {name}({", ".join(parameters)}), which it does not install')
                del installed[key]
            case 'CREATE FUNCTION':
                key = (name, _argument_types(parameters))
                if key in installed:
                    pytest.fail(f'function {name}({", ".join(parameters)}) is created twice')
                installed[key] = (name, parameters, _create_statement(name, sql, event.start(), list_end))
            case 'CREATE OR REPLACE FUNCTION':
                key = (name, _argument_types(parameters))
                installed[key] = (name, parameters, _create_statement(name, sql, event.start(), list_end))
            case verb:
                pytest.fail(f'unknown function statement {verb!r}')
    return list(installed.values())


def installed_function_statements() -> list[tuple[str, str]]:
    """``(name, statement)`` of every function that the chain installs, in install order.

    An overloaded name appears once per overload.
    """
    return [(name, statement) for name, _, statement in _function_statements()]


def installed_function(name: str) -> str:
    """The ``CREATE`` statement of an installed function with one overload: its last definition in the chain.

    Fails the test when the chain installs no function of that name, or
    more than one: an overloaded name is read with ``installed_overload``.
    """
    matches = [statement for n, _, statement in _function_statements() if n == name]
    match len(matches):
        case 0:
            pytest.fail(f'the chain installs no function {name}')
        case 1:
            return matches[0]
        case count:
            pytest.fail(f'the chain installs {count} overloads of {name}; use installed_overload')


def installed_function_position(name: str) -> int:
    """The install position of an installed function with one overload: its
    index in ``installed_function_statements``."""
    positions = [
        index for index, (n, _) in enumerate(installed_function_statements()) if n == name
    ]
    match positions:
        case [position]:
            return position
        case _:
            pytest.fail(f'the chain installs {len(positions)} functions named {name}')


def installed_returned_columns(name: str) -> tuple[tuple[str, str], ...]:
    """``(column, type)`` of each column of the ``RETURNS TABLE`` of an installed function with one overload, in order.

    Fails the test when the function returns no table.
    """
    statement = installed_function(name)
    start = statement.find(_RETURNS_TABLE)
    if start < 0:
        pytest.fail(f'function {name} returns no table')
    declarations, _ = _parameter_list(name, statement, start + len(_RETURNS_TABLE))
    return tuple(
        (column, column_type)
        for column, _, column_type in (
            declaration.partition(' ') for declaration in declarations
        )
    )


def installed_overload(name: str, first_parameter: str, *later_parameters: str) -> str:
    """The ``CREATE`` statement of one installed overload, named by its leading parameters.

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
            pytest.fail(f'the chain installs no function {name}({", ".join(leading)}, ...)')
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
    """The ``CREATE TABLE`` statement of one baseline table, partition key included.

    A later migration that changes the table does so with ``ALTER TABLE``,
    which this text does not hold.
    """
    match = re.search(
        rf'^CREATE TABLE {re.escape(name)} \(.*?^\)[^;]*;',
        baseline_sql(),
        re.DOTALL | re.MULTILINE,
    )
    if match is None:
        pytest.fail(f'the baseline defines no table {name}')
    return match.group(0)
