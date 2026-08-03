"""Frozen allowlist of terminal-status writers on ``horsies_tasks``.

Sixteen SQL statements in the runtime package can move a task row to a
terminal status (T01-T16 in the terminalization decision record,
``roadmap/terminalization-decisions-2026-08-03.md`` Appendix A). Until the
terminalization consolidation installs module-boundary enforcement (plan
Phase T8), this inventory is the tripwire: a seventeenth terminal writer —
or a removed, moved, or status-drifted one — fails here and must be
reviewed against the terminalization plan before the allowlist changes.

Scope: UPDATE statements assigning ``status`` on ``horsies_tasks`` in any
string literal of the runtime package — SQLAlchemy ``text()`` statements,
raw psycopg SQL, and DDL function bodies alike. Writers assigning only
live statuses (PENDING/CLAIMED/RUNNING) are out of scope. A parameterized
``status = :param`` assignment is terminal-capable and must be listed.
INSERT-shaped terminal writes are out of scope (none exist; the Phase T8
module boundary closes that door for good).

Known limit of static per-string scanning: SQL assembled across separate
string constants (``HEAD + TAIL`` concatenation, ``.join`` of fragments)
is not reassembled, so a writer split that way evades this test. No
runtime SQL is built that way today; the Phase T8 module boundary closes
this class structurally.
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
_CLAUSE_END_RE = re.compile(r'\bWHERE\b|\bRETURNING\b|\bFROM\b', re.IGNORECASE)
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


def _set_clause_statuses(sql: str) -> tuple[frozenset[str], bool]:
    """Terminal-relevant status assignments in UPDATE horsies_tasks SETs.

    Returns the set of status literals assigned and whether any SET clause
    assigns status from a bound parameter.
    """
    literals: set[str] = set()
    parameterized = False
    for update_match in _UPDATE_RE.finditer(sql):
        segment = sql[update_match.end():]
        set_match = _SET_RE.search(segment)
        if set_match is None:
            continue
        segment = segment[set_match.end():]
        end_match = _CLAUSE_END_RE.search(segment)
        window = segment[: end_match.start()] if end_match else segment
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


def _scan_module(tree: ast.Module, module_path: str) -> Counter[_InventoryKey]:
    """Terminal-capable status writers found in one parsed module."""
    contexts = _statement_contexts(tree)
    found: Counter[_InventoryKey] = Counter()
    # Constants nested inside an f-string are covered by their JoinedStr;
    # visiting them again would double-count the statement.
    fstring_parts: set[int] = {
        id(part)
        for node in ast.walk(tree)
        if isinstance(node, ast.JoinedStr)
        for part in ast.walk(node)
        if isinstance(part, ast.Constant)
    }
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
        statuses, parameterized = _set_clause_statuses(text)
        terminal_literals = statuses & _TERMINAL_STATUSES
        if not terminal_literals and not parameterized:
            continue
        described = ','.join(sorted(terminal_literals))
        if parameterized:
            described = f'{described}+PARAM' if described else 'PARAM'
        context = contexts.get(node.lineno, '<module>')
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


class TestTerminalWriterInventory:
    """The frozen sixteen, and the scanner that enforces them."""

    def test_inventory_is_frozen(self) -> None:
        """Every terminal-capable writer is in the allowlist, exactly.

        A failure means a terminal writer was added, removed, moved, or
        changed which statuses it assigns. Do not update the allowlist
        without reviewing the change against
        roadmap/terminalization-decisions-2026-08-03.md (Appendix A) and
        the terminalization consolidation plan.
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
