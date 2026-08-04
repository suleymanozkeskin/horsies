"""Frozen allowlist of terminal-status writers on ``horsies_tasks``.

Sixteen SQL statements in the runtime package can move a task row to a
terminal status. Until terminal writes are consolidated behind a single
persistence module that can be enforced structurally, this inventory is the
tripwire: a seventeenth terminal writer — or a removed, moved, or
status-drifted one — fails here and must be reviewed before the allowlist
changes.

``tests/lifecycle_matrix.py`` describes what each of these writers does, and
is cross-checked against this allowlist.

Scope: UPDATE statements assigning ``status`` on ``horsies_tasks`` in any
string literal of the runtime package — SQLAlchemy ``text()`` statements,
raw psycopg SQL, and DDL function bodies alike. Writers assigning only
live statuses (PENDING/CLAIMED/RUNNING) are out of scope. A parameterized
``status = :param`` assignment is terminal-capable and must be listed.
INSERT-shaped terminal writes are out of scope (none exist; a structural
module boundary closes that door for good).

Known limit of static per-string scanning: SQL assembled across separate
string constants (``HEAD + TAIL`` concatenation, ``.join`` of fragments)
is not reassembled, so a writer split that way evades this test. No
runtime SQL is built that way today, and routing terminal writes through a
single persistence module closes this class structurally.
"""

from __future__ import annotations

import ast
import re
from collections import Counter
from pathlib import Path

import pytest

pytestmark = [pytest.mark.unit]

_RUNTIME_ROOT = Path(__file__).resolve().parents[2] / 'horsies'

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

# The frozen sixteen. Keys are stable across line moves: module path plus
# the assigned statement name (module-level ``NAME = text(...)``) or the
# enclosing function name (raw SQL in child paths). The count catches a
# writer being duplicated or removed within the same context.
FROZEN_TERMINAL_WRITERS: dict[_InventoryKey, int] = {
    # T01
    ('horsies/monitoring/task_actions.py', '_CANCEL_TASK_SQL', 'CANCELLED'): 1,
    # T02
    ('horsies/core/worker/sql.py', 'UNCLAIM_PAUSED_TASKS_SQL', 'CANCELLED'): 1,
    # T03
    (
        'horsies/core/worker/sql.py',
        'CANCEL_CANCELLED_WORKFLOW_TASKS_SQL',
        'CANCELLED',
    ): 1,
    # T04
    ('horsies/core/worker/sql.py', 'MARK_TASK_FAILED_WORKER_SQL', 'FAILED'): 1,
    # T05
    ('horsies/core/worker/sql.py', 'MARK_TASK_FAILED_SQL', 'FAILED'): 1,
    # T06
    ('horsies/core/worker/sql.py', 'MARK_TASK_COMPLETED_SQL', 'COMPLETED'): 1,
    # T07
    ('horsies/core/worker/sql.py', 'FINALIZE_TASK_COMPLETED_SQL', 'COMPLETED'): 1,
    # T08
    (
        'horsies/core/worker/sql.py',
        'TERMINATE_ORPHANED_WORKFLOW_TASK_SQL',
        'CANCELLED',
    ): 1,
    # T09
    (
        'horsies/core/workflows/sql.py',
        'CANCEL_CLAIMED_TASKS_FOR_PAUSED_WORKFLOWS_SQL',
        'CANCELLED',
    ): 1,
    # T10 + T11: both branches live in the child pre-start handler.
    (
        'horsies/core/worker/child_runner.py',
        '_handle_workflow_stop_before_start',
        'CANCELLED',
    ): 2,
    # T12
    (
        'horsies/core/worker/child_runner.py',
        '_expire_claimed_task_before_start',
        'EXPIRED',
    ): 1,
    # T13
    ('horsies/core/brokers/postgres.py', 'MARK_STALE_TASK_FAILED_SQL', 'FAILED'): 1,
    # T14
    ('horsies/core/brokers/postgres.py', 'EXPIRE_PENDING_TASKS_SQL', 'EXPIRED'): 1,
    # T15
    (
        'horsies/core/brokers/postgres.py',
        'TERMINATE_ORPHANED_CLAIMED_WORKFLOW_TASKS_SQL',
        'CANCELLED',
    ): 1,
    # T16
    (
        'horsies/core/models/workflow/handle.py',
        'MARK_ENQUEUED_NOT_STARTED_TASKS_CANCELLED_SQL',
        'CANCELLED',
    ): 1,
    # The database-owned operations. Not new legacy writers: these are what the
    # statements above are being migrated to, and they live in a migration
    # rather than a runtime module. Each was verified against a real server for
    # every outcome its contract defines before it was added here.
    (
        'horsies/core/schemas/terminalization.py',
        'CREATE_COMPLETE_LOCKED_TASK_SQL',
        'COMPLETED',
    ): 1,
    (
        'horsies/core/schemas/terminalization.py',
        'CREATE_COMPLETE_TASK_FUSED_SQL',
        'COMPLETED',
    ): 1,
}


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


def _update_clauses(sql: str) -> list[tuple[str, str]]:
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
    return [window for window, _ in _update_clauses(sql)]


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
        rel = str(path.relative_to(_RUNTIME_ROOT.parent))
        tree = ast.parse(path.read_text(encoding='utf-8'))
        found.update(_scan_module(tree, rel))
    return found


def _terminal_set_windows(
    tree: ast.Module,
    module_path: str,
) -> list[tuple[str, str, str]]:
    """(module, context, SET window) per terminal-status-assigning window.

    One entry per SET clause that assigns a terminal status literal, so a
    statement with two such UPDATEs yields two entries.
    """
    contexts = _statement_contexts(tree)
    windows: list[tuple[str, str, str]] = []
    for lineno, text in _task_update_strings(tree):
        context = contexts.get(lineno, '<module>')
        for window in _set_clause_windows(text):
            assigned = {
                m.group(1).upper() for m in _STATUS_LITERAL_RE.finditer(window)
            }
            if assigned & _TERMINAL_STATUSES:
                windows.append((module_path, context, window))
    return windows


def _scan_runtime_terminal_windows() -> list[tuple[str, str, str]]:
    """Terminal-assigning SET windows across the whole runtime package."""
    windows: list[tuple[str, str, str]] = []
    for path in sorted(_RUNTIME_ROOT.rglob('*.py')):
        rel = str(path.relative_to(_RUNTIME_ROOT.parent))
        tree = ast.parse(path.read_text(encoding='utf-8'))
        windows.extend(_terminal_set_windows(tree, rel))
    return windows


def _revival_set_windows(
    tree: ast.Module,
    module_path: str,
) -> list[tuple[str, str, str]]:
    """(module, context, SET window) per terminal-to-live transition.

    A revival assigns a live status while its predicate restricts the source
    to terminal statuses — the shape of manual in-place retry. Automatic
    retry and requeue are live-to-live and do not match.
    """
    contexts = _statement_contexts(tree)
    windows: list[tuple[str, str, str]] = []
    for lineno, text in _task_update_strings(tree):
        context = contexts.get(lineno, '<module>')
        for window, predicate in _update_clauses(text):
            assigned = {
                m.group(1).upper() for m in _STATUS_LITERAL_RE.finditer(window)
            }
            if assigned & _TERMINAL_STATUSES or not assigned:
                continue
            guarded = {m.upper() for m in _STATUS_WORD_RE.findall(predicate)}
            if guarded & _TERMINAL_STATUSES:
                windows.append((module_path, context, window))
    return windows


def _scan_runtime_revival_windows() -> list[tuple[str, str, str]]:
    """Terminal-to-live SET windows across the whole runtime package."""
    windows: list[tuple[str, str, str]] = []
    for path in sorted(_RUNTIME_ROOT.rglob('*.py')):
        rel = str(path.relative_to(_RUNTIME_ROOT.parent))
        tree = ast.parse(path.read_text(encoding='utf-8'))
        windows.extend(_revival_set_windows(tree, rel))
    return windows


class TestTerminalWriterInventory:
    """The frozen sixteen, and the scanner that enforces them."""

    def test_inventory_is_frozen(self) -> None:
        """Every terminal-capable writer is in the allowlist, exactly.

        A failure means a terminal writer was added, removed, moved, or
        changed which statuses it assigns. Do not update the allowlist to
        make this pass without establishing that the new write is correct:
        every entry here is a statement that can end a task's life.
        ``tests/lifecycle_matrix.py`` must be updated in step.
        """
        discovered = _scan_runtime_package()
        assert discovered == Counter(FROZEN_TERMINAL_WRITERS), (
            'Terminal-status writer inventory drifted.\n'
            f'Unexpected: {sorted(discovered - Counter(FROZEN_TERMINAL_WRITERS))}\n'
            f'Missing: {sorted(Counter(FROZEN_TERMINAL_WRITERS) - discovered)}'
        )

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
        """Every terminal writer assigns terminal_at in the same SET clause.

        The frozen inventory proves which statuses are assigned and where;
        it does not prove the terminal_at assignment is present and spelled
        correctly. A missed or misspelled writer would otherwise surface
        only when the CHECK constraint lands, as a finalize failure on
        whichever path the tests exercise least.
        """
        windows = _scan_runtime_terminal_windows()
        assert len(windows) == 18, (
            f'Expected eighteen terminal-assigning SET clauses, found '
            f'{len(windows)}; the inventory and this assertion disagree.'
        )
        missing = [
            (module, context)
            for module, context, window in windows
            if 'terminal_at' not in window
        ]
        assert not missing, (
            'Terminal writers that do not assign terminal_at in the same '
            f'SET clause as their terminal status: {sorted(missing)}'
        )

    def test_terminal_to_live_transitions_clear_terminal_at(self) -> None:
        """A row returning from terminal to live clears terminal_at.

        Manual in-place retry is the only such transition today. Automatic
        retry and requeue are live-to-live and are not matched, so they are
        not required to clear a column that is already NULL.
        """
        windows = _scan_runtime_revival_windows()
        assert windows, 'No terminal-to-live transition found; expected retry.'
        missing = [
            (module, context)
            for module, context, window in windows
            if 'terminal_at' not in window
        ]
        assert not missing, (
            'Transitions from a terminal status back to a live one that do '
            f'not clear terminal_at: {sorted(missing)}'
        )
