"""Scans of the terminal writes of the baseline SQL functions, for the
lifecycle matrix and the frozen history schema tests.

``FROZEN_TERMINAL_WRITERS`` names the history inserts of the baseline
functions: a task reaches a terminal status only by leaving
``horsies_tasks``, through the single-row or the batch move. The helpers
read the history inserts of a function body (``history_insert_statuses``,
``history_insert_columns``) and the SET clauses of the UPDATEs of
``horsies_tasks`` in a statement (``update_clauses``).
"""

from __future__ import annotations

import re

_BASELINE = 'baseline'
"""Inventory key of a baseline function, in place of a module path."""


_UPDATE_RE = re.compile(r'\bUPDATE\s+horsies_tasks\b', re.IGNORECASE)


_SET_RE = re.compile(r'\bSET\b', re.IGNORECASE)


# Parentheses are tracked alongside the clause-enders so a keyword inside a
# scalar subquery does not close the SET clause.
_CLAUSE_SCAN_RE = re.compile(
    r'[()]|\bWHERE\b|\bRETURNING\b|\bFROM\b',
    re.IGNORECASE,
)


# One inventory row: (module path, statement or function name, statuses).
# ``statuses`` is the sorted, comma-joined terminal literals the SET clause
# assigns, with '+PARAM' appended when status is assigned from a bound
# parameter (terminal-capable regardless of literals).
_InventoryKey = tuple[str, str, str]


# Keys are stable across line moves: ``baseline`` plus the function that holds
# the history insert, and the status it writes. The count catches an insert
# being duplicated or removed within the same function.
FROZEN_TERMINAL_WRITERS: dict[_InventoryKey, int] = {
    # The single-row move of a locked row, which every single-row terminal
    # operation calls with its own status literal.
    (_BASELINE, 'horsies_move_task_to_history', 'PARAM'): 1,
    # The batch move of locked rows, by task ids, which every batch terminal
    # operation calls with its own status literal.
    (_BASELINE, 'horsies_move_tasks_to_history', 'PARAM'): 1,
}


_HISTORY_INSERT_RE = re.compile(
    r'\bINSERT\s+INTO\s+horsies_task_history\s*\(',
    re.IGNORECASE,
)
"""An insert into the history parent (not into its side tables)."""


_STATUS_VALUE_LITERAL_RE = re.compile(r"^'(\w+)'$")


def _parenthesised(text: str, open_at: int) -> tuple[str, int]:
    """The text inside the parentheses opening at ``open_at``, and the end offset."""
    depth = 0
    for offset in range(open_at, len(text)):
        match text[offset]:
            case '(':
                depth += 1
            case ')':
                depth -= 1
                if depth == 0:
                    return text[open_at + 1 : offset], offset + 1
            case _:
                continue
    raise AssertionError(f'unbalanced parentheses after offset {open_at}')


def _top_level_items(text: str) -> list[str]:
    """The comma-separated items of ``text`` at parenthesis depth zero."""
    items: list[str] = []
    depth = 0
    current: list[str] = []
    for char in text:
        match char:
            case '(':
                depth += 1
            case ')':
                depth -= 1
            case ',' if depth == 0:
                items.append(''.join(current).strip())
                current = []
                continue
            case _:
                pass
        current.append(char)
    items.append(''.join(current).strip())
    return items


def _select_list(text: str) -> str:
    """The select list of the SELECT that starts ``text``: up to its FROM."""
    depth = 0
    for match in re.finditer(r'[()]|\bFROM\b', text, re.IGNORECASE):
        match match.group(0):
            case '(':
                depth += 1
            case ')':
                depth -= 1
            case _ if depth == 0:
                return text[: match.start()]
            case _:
                continue
    return text


def history_insert_statuses(body: str) -> list[str]:
    """The status each history insert of a function body writes.

    A quoted literal gives the status; any other expression gives ``PARAM``.
    """
    statuses: list[str] = []
    for insert in _HISTORY_INSERT_RE.finditer(body):
        columns_text, after_columns = _parenthesised(body, insert.end() - 1)
        columns = [column.strip() for column in columns_text.split(',')]
        assert columns.count('status') == 1, columns
        rest = body[after_columns:].lstrip()
        if rest.upper().startswith('VALUES'):
            values_text, _ = _parenthesised(rest, rest.index('('))
            values = _top_level_items(values_text)
        else:
            select_at = re.search(r'\bSELECT\b', rest, re.IGNORECASE)
            assert select_at is not None, rest[:200]
            values = _top_level_items(_select_list(rest[select_at.end() :]))
        assert len(values) == len(columns), (len(values), len(columns))
        value = values[columns.index('status')]
        literal = _STATUS_VALUE_LITERAL_RE.match(value)
        statuses.append('PARAM' if literal is None else literal.group(1).upper())
    return statuses


def history_insert_columns(body: str) -> list[list[str]]:
    """The column list of each history insert of a function body."""
    lists: list[list[str]] = []
    for insert in _HISTORY_INSERT_RE.finditer(body):
        columns_text, _ = _parenthesised(body, insert.end() - 1)
        lists.append([column.strip() for column in columns_text.split(',')])
    return lists


def update_clauses(sql: str) -> list[tuple[str, str]]:
    """(SET window, remainder) for every UPDATE horsies_tasks in a statement.

    The window runs from SET to the first WHERE, RETURNING or FROM, so it
    holds the assignments alone; the remainder carries the predicate, where
    a status literal is a source-state guard rather than a write.
    """
    clauses: list[tuple[str, str]] = []
    for update_match in _UPDATE_RE.finditer(sql):
        segment = sql[update_match.end():]
        set_match = _SET_RE.search(segment)
        if set_match is None:
            continue
        segment = segment[set_match.end():]
        end = _clause_end(segment)
        if end is None:
            clauses.append((segment, ''))
            continue
        clauses.append((segment[:end], segment[end:]))
    return clauses


def _clause_end(segment: str) -> int | None:
    """Offset of the keyword ending a SET clause, ignoring subqueries.

    A SET assignment may contain a scalar subquery carrying its own FROM and
    WHERE — manual retry recomputes retry_count that way. Only a keyword at
    parenthesis depth zero closes the clause.
    """
    depth = 0
    for match in _CLAUSE_SCAN_RE.finditer(segment):
        token = match.group(0)
        match token:
            case '(':
                depth += 1
            case ')':
                depth -= 1
            case _ if depth == 0:
                return match.start()
            case _:
                continue
    return None
