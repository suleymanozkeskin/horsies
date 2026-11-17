"""Frozen inventory of the terminal writes: the history rows.

A task reaches a terminal status only by leaving ``horsies_tasks``: a baseline
SQL function inserts its history row and deletes the live row. The live table
refuses a terminal status (CHECK ``horsies_tasks_status_live``). So the
terminal writers are the history inserts of the baseline functions, and this
inventory pins them: an added, removed, moved, or status-drifted history
insert fails here and must be reviewed before the inventory changes.

``tests/lifecycle_matrix.py`` describes the sixteen terminal operations that
reach these inserts, and is cross-checked against this allowlist.

Two scans:

- The baseline functions (``tests/baseline_sql.baseline_function_statements``): their
  history inserts, with the status each one writes (a literal, or ``PARAM``
  for a status bound from a parameter), and their UPDATEs of
  ``horsies_tasks``, which must assign no terminal status.
- Every string literal of the runtime package, as before the baseline:
  UPDATE statements assigning ``status`` on ``horsies_tasks``. Writers that
  assign only live statuses (PENDING/CLAIMED/RUNNING) are out of scope; a
  parameterized ``status = :param`` assignment is terminal-capable. No
  Python string may hold a terminal writer or a history insert.

Known limit of static per-string scanning: SQL assembled across separate
string constants (``HEAD + TAIL`` concatenation, ``.join`` of fragments)
is not reassembled, so a writer split that way evades the Python scan. No
runtime SQL is built that way today.
"""

from __future__ import annotations

import ast
import re
from collections import Counter
from pathlib import Path

import pytest

from horsies.core.history.names import CLOCK_SKEW_EXCEPTIONS, LEAF_CATALOG
from tests.baseline_sql import baseline_function_statements

pytestmark = [pytest.mark.unit]

_REPO_ROOT = Path(__file__).resolve().parents[2]
_RUNTIME_ROOT = _REPO_ROOT / 'python' / 'horsies'
_BASELINE = 'baseline'
"""Inventory key of a baseline function, in place of a module path."""

_TERMINAL_STATUSES = frozenset({'COMPLETED', 'FAILED', 'CANCELLED', 'EXPIRED'})

_UPDATE_RE = re.compile(r'\bUPDATE\s+horsies_tasks\b', re.IGNORECASE)
_SET_RE = re.compile(r'\bSET\b', re.IGNORECASE)
# Parentheses are tracked alongside the clause-enders so a keyword inside a
# scalar subquery does not close the SET clause.
_CLAUSE_SCAN_RE = re.compile(
    r'[()]|\bWHERE\b|\bRETURNING\b|\bFROM\b',
    re.IGNORECASE,
)
# Any quoted word, used to read status literals out of a predicate where they
# appear as IN-lists rather than assignments.
_STATUS_WORD_RE = re.compile(r"'(\w+)'")
# Case-insensitive, quoted-identifier-tolerant: SET STATUS = / "status" =
# are writers too — this test exists for the writer that does not follow
# house style. The literal's own case is preserved by upper() at use sites.
_STATUS_LITERAL_RE = re.compile(r'"?\bstatus"?\s*=\s*\'(\w+)\'', re.IGNORECASE)
# Bound parameters (:x, %(x)s, %s, $1) and bare identifiers (PL/pgSQL
# variables, NEW.x, CASE ...) are all terminal-capable: the value is not
# statically knowable, so the writer must be allowlisted and reviewed.
_STATUS_PARAM_RE = re.compile(
    r'"?\bstatus"?\s*=\s*(?::\w+|%\(\w+\)s|%s|\$\d+|[A-Za-z_][\w.]*)',
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

_SOURCE_HISTORY_INSERT_RE = re.compile(
    r'\bINSERT\s+INTO\s+(?:horsies_task_history|\{TASK_HISTORY_PARENT\})\s*\(',
    re.IGNORECASE,
)
"""A history insert in Python source, named or interpolated."""

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


def _scan_baseline_history_inserts() -> Counter[_InventoryKey]:
    """History inserts of the baseline functions, keyed by the status written."""
    found: Counter[_InventoryKey] = Counter()
    for name, body in baseline_function_statements():
        for status in history_insert_statuses(body):
            found[(_BASELINE, name, status)] += 1
    return found


def _constant_text(node: ast.expr) -> str:
    """Concatenated constant string parts under a node (f-string safe).

    Interpolated expressions contribute nothing; their constant SQL
    skeleton (keywords, column assignments) survives, which is what the
    detection regexes need.
    """
    parts: list[str] = []
    for sub in ast.walk(node):
        if isinstance(sub, ast.Constant) and isinstance(sub.value, str):
            parts.append(sub.value)
    return '\n'.join(parts)


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


def _set_clause_windows(sql: str) -> list[str]:
    """SET-clause text of every UPDATE horsies_tasks in one statement."""
    return [window for window, _ in update_clauses(sql)]


def _set_clause_statuses(sql: str) -> tuple[frozenset[str], bool]:
    """Terminal-relevant status assignments in UPDATE horsies_tasks SETs.

    Returns the set of status literals assigned and whether any SET clause
    assigns status from a bound parameter.
    """
    literals: set[str] = set()
    parameterized = False
    for window in _set_clause_windows(sql):
        literals.update(
            m.group(1).upper() for m in _STATUS_LITERAL_RE.finditer(window)
        )
        if _STATUS_PARAM_RE.search(window) is not None:
            parameterized = True
    return frozenset(literals), parameterized


def _statement_contexts(tree: ast.Module) -> dict[int, str]:
    """Map string-node line numbers to their statement or function name.

    Module-level ``NAME = <expr containing the string>`` wins over the
    enclosing function; otherwise the innermost enclosing function names
    the context.
    """
    contexts: dict[int, str] = {}
    for stmt in tree.body:
        target: ast.expr | None = None
        value: ast.expr | None = None
        match stmt:
            case ast.Assign(targets=[t], value=v):
                target, value = t, v
            case ast.AnnAssign(target=t, value=v) if v is not None:
                target, value = t, v
            case _:
                continue
        if isinstance(target, ast.Name):
            for sub in ast.walk(value):
                if isinstance(sub, (ast.Constant, ast.JoinedStr)):
                    contexts[sub.lineno] = target.id
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            end = node.end_lineno if node.end_lineno is not None else node.lineno
            for sub in ast.walk(node):
                if isinstance(sub, (ast.Constant, ast.JoinedStr)):
                    if sub.lineno not in contexts and node.lineno <= sub.lineno <= end:
                        contexts[sub.lineno] = node.name
    return contexts


def _task_update_strings(tree: ast.Module) -> list[tuple[int, str]]:
    """(line number, SQL text) for every string holding an UPDATE of tasks."""
    # Constants nested inside an f-string are covered by their JoinedStr;
    # visiting them again would double-count the statement.
    fstring_parts: set[int] = {
        id(part)
        for node in ast.walk(tree)
        if isinstance(node, ast.JoinedStr)
        for part in ast.walk(node)
        if isinstance(part, ast.Constant)
    }
    found: list[tuple[int, str]] = []
    for node in ast.walk(tree):
        if not isinstance(node, (ast.Constant, ast.JoinedStr)):
            continue
        if isinstance(node, ast.Constant) and (
            not isinstance(node.value, str) or id(node) in fstring_parts
        ):
            continue
        text = _constant_text(node)
        if _UPDATE_RE.search(text) is None:
            continue
        found.append((node.lineno, text))
    return found


def _scan_module(tree: ast.Module, module_path: str) -> Counter[_InventoryKey]:
    """Terminal-capable status writers found in one parsed module."""
    contexts = _statement_contexts(tree)
    found: Counter[_InventoryKey] = Counter()
    for lineno, text in _task_update_strings(tree):
        statuses, parameterized = _set_clause_statuses(text)
        terminal_literals = statuses & _TERMINAL_STATUSES
        if not terminal_literals and not parameterized:
            continue
        described = ','.join(sorted(terminal_literals))
        if parameterized:
            described = f'{described}+PARAM' if described else 'PARAM'
        context = contexts.get(lineno, '<module>')
        found[(module_path, context, described)] += 1
    return found


def _scan_runtime_package() -> Counter[_InventoryKey]:
    """Terminal-capable status writers across the whole runtime package."""
    found: Counter[_InventoryKey] = Counter()
    for path in sorted(_RUNTIME_ROOT.rglob('*.py')):
        rel = str(path.relative_to(_REPO_ROOT))
        tree = ast.parse(path.read_text(encoding='utf-8'))
        found.update(_scan_module(tree, rel))
    return found


def _revives(window: str, predicate: str) -> bool:
    """Whether one UPDATE moves a terminal row back to a live status.

    A revival assigns a live status while its predicate restricts the source
    to terminal statuses — the shape of manual in-place retry. Automatic
    retry and requeue are live-to-live and do not match.
    """
    assigned = {m.group(1).upper() for m in _STATUS_LITERAL_RE.finditer(window)}
    if assigned & _TERMINAL_STATUSES or not assigned:
        return False
    # In a function body the remainder runs on past the statement; only the
    # text before its ';' is the predicate.
    statement_predicate = predicate.split(';', 1)[0]
    guarded = {m.upper() for m in _STATUS_WORD_RE.findall(statement_predicate)}
    return bool(guarded & _TERMINAL_STATUSES)


def _revival_set_windows(
    tree: ast.Module,
    module_path: str,
) -> list[tuple[str, str, str]]:
    """(module, context, SET window) per terminal-to-live transition."""
    contexts = _statement_contexts(tree)
    windows: list[tuple[str, str, str]] = []
    for lineno, text in _task_update_strings(tree):
        context = contexts.get(lineno, '<module>')
        for window, predicate in update_clauses(text):
            if _revives(window, predicate):
                windows.append((module_path, context, window))
    return windows


def _scan_runtime_revival_windows() -> list[tuple[str, str, str]]:
    """Terminal-to-live SET windows across the whole runtime package."""
    windows: list[tuple[str, str, str]] = []
    for path in sorted(_RUNTIME_ROOT.rglob('*.py')):
        rel = str(path.relative_to(_REPO_ROOT))
        tree = ast.parse(path.read_text(encoding='utf-8'))
        windows.extend(_revival_set_windows(tree, rel))
    return windows



_HISTORY_SIDE_TABLES = (LEAF_CATALOG, CLOCK_SKEW_EXCEPTIONS)
"""Tables under the history name prefix that hold no history record: the
leaf catalog and the clock-skew exceptions."""

_HISTORY_UPDATE_RE = re.compile(
    r'UPDATE\s+(?:(?!(?:' + '|'.join(_HISTORY_SIDE_TABLES) + r')\b)horsies_task_history\w*'
    r'|\{TASK_HISTORY_PARENT\})',
    re.IGNORECASE,
)
"""An UPDATE against the history relation, one of its class parents or
leaves, named or interpolated; not against a side table.

The DDL builds its statements by interpolating TASK_HISTORY_PARENT, so a
scan for the literal name alone would read as clean while the generated
SQL said otherwise."""


def _scan_history_updates(text: str) -> list[str]:
    """Every UPDATE-against-history window in one blob of source."""
    return [m.group(0) for m in _HISTORY_UPDATE_RE.finditer(text)]


def _scan_runtime_history_updates() -> list[tuple[str, str]]:
    """(module, matched window) per UPDATE against the history relation."""
    found: list[tuple[str, str]] = []
    for path in sorted(_RUNTIME_ROOT.rglob('*.py')):
        rel = str(path.relative_to(_REPO_ROOT))
        for window in _scan_history_updates(path.read_text(encoding='utf-8')):
            found.append((rel, window))
    return found


class TestTerminalWriterInventory:
    """The reviewed terminal-writer set and the scanner that enforces it."""

    def test_inventory_is_frozen(self) -> None:
        """Every history insert of the baseline is in the allowlist, exactly.

        A failure means a terminal writer was added, removed, moved, or
        changed which status it writes. Do not update the allowlist to
        make this pass without establishing that the new write is correct:
        every entry here is a statement that can end a task's life.
        ``tests/lifecycle_matrix.py`` must be updated in step.
        """
        discovered = _scan_baseline_history_inserts()
        assert discovered == Counter(FROZEN_TERMINAL_WRITERS), (
            'Terminal writer inventory drifted.\n'
            f'Unexpected: {sorted(discovered - Counter(FROZEN_TERMINAL_WRITERS))}\n'
            f'Missing: {sorted(Counter(FROZEN_TERMINAL_WRITERS) - discovered)}'
        )

    def test_terminal_writes_are_owned_by_the_database_program(self) -> None:
        """Callers cannot grow a second terminalization implementation.

        No Python string assigns a terminal status to a live row or inserts
        a history row: both belong to the baseline functions.
        """
        assert _scan_runtime_package() == Counter()
        history_inserts = sorted(
            str(path.relative_to(_REPO_ROOT))
            for path in _RUNTIME_ROOT.rglob('*.py')
            if _SOURCE_HISTORY_INSERT_RE.search(path.read_text(encoding='utf-8'))
        )
        assert not history_inserts, (
            f'History rows must be inserted by the baseline functions: {history_inserts}'
        )

    def test_no_baseline_function_assigns_a_terminal_status_to_a_live_row(
        self,
    ) -> None:
        """The baseline functions update live rows with live statuses only.

        CHECK ``horsies_tasks_status_live`` refuses the write at run time;
        this finds it in the text, for every function, whether or not a test
        calls it.
        """
        writers = sorted(
            (name, sorted(statuses), parameterized)
            for name, body in baseline_function_statements()
            for statuses, parameterized in (_set_clause_statuses(body),)
            if statuses & _TERMINAL_STATUSES or parameterized
        )
        assert not writers, writers

    def test_the_history_insert_scan_reads_literal_and_bound_statuses(
        self,
    ) -> None:
        """Presence half: the status reader reads both insert forms."""
        values_form = (
            'INSERT INTO horsies_task_history (task_id, status, terminal_at)\n'
            'VALUES (p_task_id, p_terminal_status, NOW());'
        )
        select_form = (
            'INSERT INTO horsies_task_history (task_id, status, terminal_at)\n'
            "SELECT t.id, 'EXPIRED', coalesce(t.good_until, NOW())\n"
            'FROM horsies_tasks t;'
        )
        side_table = (
            'INSERT INTO horsies_task_history_clock_exceptions (task_id)\n'
            'SELECT t.id FROM horsies_tasks t;'
        )
        assert history_insert_statuses(values_form) == ['PARAM']
        assert history_insert_statuses(select_form) == ['EXPIRED']
        assert history_insert_statuses(side_table) == []

    def test_scanner_detects_terminal_literal(self) -> None:
        """A terminal literal in a SET clause is detected."""
        source = (
            'STMT = """\n'
            '    UPDATE horsies_tasks\n'
            "    SET status = 'CANCELLED', updated_at = NOW()\n"
            "    WHERE id = :id AND status = 'CLAIMED'\n"
            '"""\n'
        )
        found = _scan_module(ast.parse(source), 'synthetic.py')
        assert found == Counter({('synthetic.py', 'STMT', 'CANCELLED'): 1})

    def test_scanner_detects_parameterized_status(self) -> None:
        """A bound-parameter status assignment is terminal-capable."""
        source = (
            'STMT = """\n'
            '    UPDATE horsies_tasks SET status = :status WHERE id = :id\n'
            '"""\n'
        )
        found = _scan_module(ast.parse(source), 'synthetic.py')
        assert found == Counter({('synthetic.py', 'STMT', 'PARAM'): 1})

    def test_scanner_is_case_and_quote_insensitive(self) -> None:
        """SET STATUS / "status" and lowercase literals are still writers."""
        source = (
            'STMT = """\n'
            '    UPDATE HORSIES_TASKS\n'
            '    SET "STATUS" = \'failed\'\n'
            '    WHERE id = :id\n'
            '"""\n'
        )
        found = _scan_module(ast.parse(source), 'synthetic.py')
        assert found == Counter({('synthetic.py', 'STMT', 'FAILED'): 1})

    def test_scanner_detects_bare_identifier_assignment(self) -> None:
        """PL/pgSQL-style ``status = some_variable`` is terminal-capable."""
        source = (
            'DDL = """\n'
            '    CREATE FUNCTION f(p_status text) RETURNS void AS $$\n'
            '    BEGIN\n'
            '        UPDATE horsies_tasks SET status = p_status WHERE id = p_id;\n'
            '    END $$ LANGUAGE plpgsql;\n'
            '"""\n'
        )
        found = _scan_module(ast.parse(source), 'synthetic.py')
        assert found == Counter({('synthetic.py', 'DDL', 'PARAM'): 1})

    def test_scanner_ignores_live_only_writers(self) -> None:
        """Assigning only live statuses is out of scope."""
        source = (
            'STMT = """\n'
            "    UPDATE horsies_tasks SET status = 'PENDING', claimed = FALSE\n"
            "    WHERE status = 'CLAIMED'\n"
            '"""\n'
        )
        found = _scan_module(ast.parse(source), 'synthetic.py')
        assert not found

    def test_scanner_ignores_predicate_only_mentions(self) -> None:
        """Terminal statuses in WHERE clauses alone are not writes."""
        source = (
            'STMT = """\n'
            '    UPDATE horsies_tasks SET claimed = FALSE\n'
            "    WHERE status = 'FAILED'\n"
            '"""\n'
        )
        found = _scan_module(ast.parse(source), 'synthetic.py')
        assert not found

    def test_scanner_sees_fstring_statements(self) -> None:
        """f-string SQL keeps its constant skeleton and is scanned."""
        source = (
            'LITERALS = "x"\n'
            'STMT = f"""\n'
            '    UPDATE horsies_tasks\n'
            "    SET status = 'EXPIRED'\n"
            '    WHERE status IN ({LITERALS})\n'
            '"""\n'
        )
        found = _scan_module(ast.parse(source), 'synthetic.py')
        assert found == Counter({('synthetic.py', 'STMT', 'EXPIRED'): 1})

    def test_scanner_names_function_contexts(self) -> None:
        """Raw SQL inside a function is keyed by the function name."""
        source = (
            'def _cancel_in_child(conn: object) -> None:\n'
            '    sql = """\n'
            '        UPDATE horsies_tasks\n'
            "        SET status = 'CANCELLED'\n"
            '        WHERE id = %(id)s\n'
            '    """\n'
        )
        found = _scan_module(ast.parse(source), 'synthetic.py')
        assert found == Counter(
            {('synthetic.py', '_cancel_in_child', 'CANCELLED'): 1},
        )

    def test_terminal_writers_set_terminal_at(self) -> None:
        """Every history insert writes ``terminal_at``.

        The frozen inventory proves which statuses are written and where;
        it does not prove that each insert carries the terminal time.
        """
        inserts = [
            (name, columns)
            for name, body in baseline_function_statements()
            for columns in history_insert_columns(body)
        ]
        assert len(inserts) == sum(FROZEN_TERMINAL_WRITERS.values()), (
            'the inventory and this assertion disagree'
        )
        missing = sorted(name for name, columns in inserts if 'terminal_at' not in columns)
        assert not missing, (
            f'History inserts that do not write terminal_at: {missing}'
        )

    def test_no_terminal_to_live_transition_exists(self) -> None:
        """A terminal record is immutable: no runtime writer revives it.

        The manual in-place retry was the only such transition;
        re-execution is a new request through the rerun contract, so any
        statement matched here is a regression against terminal
        immutability. Automatic retry and requeue are live-to-live and
        are not matched.
        """
        windows = _scan_runtime_revival_windows() + [
            (_BASELINE, name, window)
            for name, body in baseline_function_statements()
            for window, predicate in update_clauses(body)
            if _revives(window, predicate)
        ]
        assert not windows, (
            'Statements that move a terminal status back to a live one: '
            f'{sorted((module, context) for module, context, _ in windows)}'
        )

    def test_the_scanner_finds_a_history_update_when_one_exists(self) -> None:
        """Presence half: the detector detects, before its absence means anything.

        A scan that cannot find what it is looking for reports every
        codebase as clean. Both spellings are checked because the DDL
        interpolates the relation name rather than writing it.
        """
        assert _scan_history_updates(
            "UPDATE horsies_task_history SET result_payload = NULL"
        )
        assert _scan_history_updates(
            'f"UPDATE {TASK_HISTORY_PARENT} SET status = \'PENDING\'"'
        )
        assert not _scan_history_updates(
            "UPDATE horsies_tasks SET status = 'PENDING'"
        )
        assert _scan_history_updates(
            'UPDATE horsies_task_history_forever_open SET result_payload = NULL'
        )
        assert not _scan_history_updates(
            'UPDATE horsies_task_history_leaf_catalog AS c SET dropped_at = NULL'
        )

    def test_no_runtime_statement_updates_a_history_record(self) -> None:
        """A terminal record is immutable IN THE RELATION THAT HOLDS IT.

        The revival pin above covers the live table: nothing walks a
        terminal status back to a live one there. This covers the other
        half, which the split created — once a record moves to history,
        no runtime statement writes to it at all. Rerun mints a new task
        with recorded lineage; retention drops whole partitions. Neither
        is an UPDATE.

        This is why the phase-1 replay could not race a revival and why
        the lock-contention test that asserted it was retired: the race
        needs a row that can be changed, and there is none.
        """
        updates = _scan_runtime_history_updates() + [
            (f'{_BASELINE} {name}', window)
            for name, body in baseline_function_statements()
            for window in _scan_history_updates(body)
        ]
        assert not updates, (
            'Runtime statements that UPDATE a history record: '
            f'{sorted(updates)}'
        )
