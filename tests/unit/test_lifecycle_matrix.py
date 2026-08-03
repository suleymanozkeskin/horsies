"""The transition matrix is checked against the statements it describes.

`tests/lifecycle_matrix.py` declares what each terminal writer does. A
declaration nobody executes drifts from the code silently, so these tests read
the statement text back and assert the declared guards, shape, and notification
behavior are actually there.

Statements defined as module-level SQL constants are checked directly. The
child-process writers embed their SQL inside a function and are covered by
characterization tests instead; they are asserted here only for the structural
properties the matrix can know without the text.

The matrix and the frozen writer allowlist are independent declarations of the
same sixteen statements. They are cross-checked against each other, so a change
recorded in one but not the other fails rather than diverging quietly.
"""

from __future__ import annotations

import ast
from collections import Counter
from pathlib import Path

import pytest

from horsies.core.worker import child_runner

from horsies.core.brokers.postgres import (
    EXPIRE_PENDING_TASKS_SQL,
    MARK_STALE_TASK_FAILED_SQL,
    TERMINATE_ORPHANED_CLAIMED_WORKFLOW_TASKS_SQL,
)
from horsies.core.models.workflow.handle import (
    MARK_ENQUEUED_NOT_STARTED_TASKS_CANCELLED_SQL,
)
from horsies.core.worker.sql import (
    CANCEL_CANCELLED_WORKFLOW_TASKS_SQL,
    FINALIZE_TASK_COMPLETED_SQL,
    MARK_TASK_COMPLETED_SQL,
    MARK_TASK_FAILED_SQL,
    MARK_TASK_FAILED_WORKER_SQL,
    TERMINATE_ORPHANED_WORKFLOW_TASK_SQL,
    UNCLAIM_PAUSED_TASKS_SQL,
)
from horsies.core.workflows.sql import (
    CANCEL_CLAIMED_TASKS_FOR_PAUSED_WORKFLOWS_SQL,
)
from horsies.monitoring.task_actions import _CANCEL_TASK_SQL
from tests.lifecycle_matrix import (
    MATRIX,
    Attempt,
    Driver,
    EmitsNotify,
    Fence,
    Guard,
    Shape,
    TERMINAL_STATUSES,
    TerminalWriter,
)
from tests.unit.test_terminal_writer_inventory import (
    FROZEN_TERMINAL_WRITERS,
    _statement_contexts,
    _task_update_strings,
    _update_clauses,
)

pytestmark = [pytest.mark.unit]


def _child_statement_texts() -> dict[str, str]:
    """SQL for the child writers, which have no importable constant.

    Their statements are string literals inside functions, so the text comes
    from the same AST extraction the writer inventory uses. T10 and T11 share
    a function and both target CANCELLED; they are told apart by source state,
    since only the cancelled-workflow branch also accepts PENDING.
    """
    source = Path(child_runner.__file__).read_text(encoding='utf-8')
    tree = ast.parse(source)
    contexts = _statement_contexts(tree)
    found: dict[str, str] = {}
    for lineno, text in _task_update_strings(tree):
        match contexts.get(lineno):
            case '_expire_claimed_task_before_start':
                found['T12'] = text
            case '_handle_workflow_stop_before_start':
                found["T11" if "'PENDING'" in text else 'T10'] = text
            case _:
                continue
    return found


# Statement text for every writer the matrix describes as a SQL constant.
_STATEMENT_TEXT: dict[str, str] = {
    'T01': _CANCEL_TASK_SQL.text,
    'T02': UNCLAIM_PAUSED_TASKS_SQL.text,
    'T03': CANCEL_CANCELLED_WORKFLOW_TASKS_SQL.text,
    'T04': MARK_TASK_FAILED_WORKER_SQL.text,
    'T05': MARK_TASK_FAILED_SQL.text,
    'T06': MARK_TASK_COMPLETED_SQL.text,
    'T07': FINALIZE_TASK_COMPLETED_SQL.text,
    'T08': TERMINATE_ORPHANED_WORKFLOW_TASK_SQL.text,
    'T09': CANCEL_CLAIMED_TASKS_FOR_PAUSED_WORKFLOWS_SQL.text,
    'T13': MARK_STALE_TASK_FAILED_SQL.text,
    'T14': EXPIRE_PENDING_TASKS_SQL.text,
    'T15': TERMINATE_ORPHANED_CLAIMED_WORKFLOW_TASKS_SQL.text,
    'T16': MARK_ENQUEUED_NOT_STARTED_TASKS_CANCELLED_SQL.text,
    **_child_statement_texts(),
}

_TEXT_ROWS = [row for row in MATRIX if row.writer_id in _STATEMENT_TEXT]


def _normalised(writer_id: str) -> str:
    return ' '.join(_STATEMENT_TEXT[writer_id].split())


def _predicate(writer_id: str) -> str:
    """The statement with its SET assignments removed — where guards live.

    A fence is a predicate, and searching whole statement text confuses it with
    the SET clause clearing the same columns: every unfenced writer assigns
    `claimed_by_worker_id = NULL` while carrying no ownership predicate at all.

    Removing the SET windows rather than keeping only the UPDATE remainder
    matters for the fused path, whose fence lives in a locking CTE ahead of the
    UPDATE rather than in the UPDATE's own WHERE.
    """
    text = _STATEMENT_TEXT[writer_id]
    for window, _ in _update_clauses(text):
        text = text.replace(window, ' ')
    return ' '.join(text.split())


def _ids(rows: list[TerminalWriter]) -> list[str]:
    return [row.writer_id for row in rows]


class TestMatrixShape:
    """The matrix itself is well-formed."""

    def test_covers_sixteen_writers_with_unique_ids(self) -> None:
        assert len(MATRIX) == 16
        ids = [row.writer_id for row in MATRIX]
        assert len(set(ids)) == 16
        assert ids == sorted(ids), 'rows are ordered by writer id'

    def test_every_target_status_is_terminal(self) -> None:
        for row in MATRIX:
            assert row.target_status in TERMINAL_STATUSES, row.writer_id

    def test_agrees_with_the_frozen_writer_allowlist(self) -> None:
        """Two independent declarations of the same sixteen statements.

        The allowlist keys on (module, statement, statuses) with a count; the
        matrix carries one row per writer. T10 and T11 share a function, so
        they collapse to a single allowlist entry with count 2.
        """
        from_matrix = Counter(
            (row.module, row.statement, row.target_status) for row in MATRIX
        )
        from_allowlist = Counter(
            {key: count for key, count in FROZEN_TERMINAL_WRITERS.items()},
        )
        assert from_matrix == from_allowlist, (
            'matrix and allowlist disagree.\n'
            f'matrix only: {sorted(from_matrix - from_allowlist)}\n'
            f'allowlist only: {sorted(from_allowlist - from_matrix)}'
        )


class TestDeclaredGuardsMatchTheStatements:
    """Each declared property is present in the statement that carries it."""

    @pytest.mark.parametrize('row', _TEXT_ROWS, ids=_ids(_TEXT_ROWS))
    def test_target_status_is_assigned(self, row: TerminalWriter) -> None:
        sql = _normalised(row.writer_id)
        assert (
            f"status = '{row.target_status}'" in sql
            or f"status='{row.target_status}'" in sql
        ), row.writer_id

    @pytest.mark.parametrize('row', _TEXT_ROWS, ids=_ids(_TEXT_ROWS))
    def test_declared_source_statuses_appear(self, row: TerminalWriter) -> None:
        sql = _normalised(row.writer_id)
        for status in row.source_statuses:
            assert f"'{status}'" in sql, f'{row.writer_id}: {status}'

    @pytest.mark.parametrize('row', _TEXT_ROWS, ids=_ids(_TEXT_ROWS))
    def test_fence_markers(self, row: TerminalWriter) -> None:
        sql = _predicate(row.writer_id)
        match row.fence:
            case Fence.NONE | Fence.CALLER_ROW_LOCK:
                assert 'claimed_by_worker_id =' not in sql, row.writer_id
            case Fence.WORKER:
                assert 'claimed_by_worker_id =' in sql, row.writer_id
                # Deliberately generation-free: the deadline guard makes the
                # outcome correct for whichever generation holds the row.
                assert 'claimed_at' not in sql, row.writer_id
            case Fence.PRIOR_LOCKED_SELECT:
                assert 'claimed_by_worker_id =' in sql, row.writer_id
            case Fence.WORKER_AND_GENERATION:
                assert 'claimed_by_worker_id =' in sql, row.writer_id
                assert 'claimed_at' in sql, row.writer_id
            case Fence.WORKER_AND_GENERATION_PAIRWISE:
                assert 'claimed_by_worker_id =' in sql, row.writer_id
                assert 'unnest(' in sql, row.writer_id
                assert 'g.claimed_at' in sql, row.writer_id

    @pytest.mark.parametrize('row', _TEXT_ROWS, ids=_ids(_TEXT_ROWS))
    def test_guard_markers(self, row: TerminalWriter) -> None:
        sql = _predicate(row.writer_id)
        for guard in row.guards:
            match guard:
                case Guard.DEADLINE:
                    assert 'good_until' in sql, row.writer_id
                case Guard.STALENESS:
                    assert 'finalizing_at' in sql, row.writer_id
                    assert 'stale_threshold' in sql, row.writer_id
                case Guard.WORKFLOW_STATUS:
                    assert 'w.status =' in sql, row.writer_id
                case Guard.WORKFLOW_LINK_ABSENT:
                    assert 'NOT EXISTS' in sql, row.writer_id
                case Guard.WORKFLOW_LINK_STATE:
                    assert 'wt.status' in sql, row.writer_id
                case Guard.NONE:
                    pass

    @pytest.mark.parametrize('row', _TEXT_ROWS, ids=_ids(_TEXT_ROWS))
    def test_shape_markers(self, row: TerminalWriter) -> None:
        sql = _normalised(row.writer_id)
        match row.shape:
            case Shape.SET_WISE_SKIP_LOCKED:
                assert 'SKIP LOCKED' in sql, row.writer_id
            case Shape.FUSED_CTE:
                assert sql.lstrip().upper().startswith('WITH'), row.writer_id
            case Shape.SINGLE | Shape.SET_WISE:
                assert 'SKIP LOCKED' not in sql, row.writer_id

    @pytest.mark.parametrize('row', _TEXT_ROWS, ids=_ids(_TEXT_ROWS))
    def test_result_writing(self, row: TerminalWriter) -> None:
        sql = _normalised(row.writer_id)
        assert ('result =' in sql) is row.writes_result, row.writer_id

    @pytest.mark.parametrize('row', _TEXT_ROWS, ids=_ids(_TEXT_ROWS))
    def test_statement_level_notify(self, row: TerminalWriter) -> None:
        """Only the fused path emits its own NOTIFY; triggers do the rest."""
        sql = _normalised(row.writer_id)
        expected = row.emits_notify is EmitsNotify.FUSED_CAPACITY_WAKE
        assert ('pg_notify' in sql) is expected, row.writer_id

    @pytest.mark.parametrize('row', _TEXT_ROWS, ids=_ids(_TEXT_ROWS))
    def test_fused_attempt_upsert(self, row: TerminalWriter) -> None:
        sql = _normalised(row.writer_id)
        fused = row.attempt is Attempt.FUSED_UPSERT
        assert ('horsies_task_attempts' in sql) is fused, row.writer_id

    @pytest.mark.parametrize('row', MATRIX, ids=_ids(list(MATRIX)))
    def test_every_writer_assigns_terminal_at(
        self,
        row: TerminalWriter,
    ) -> None:
        """Carried from 0.4.5; the matrix must not describe a writer without it."""
        assert 'terminal_at = NOW()' in _normalised(row.writer_id), row.writer_id


class TestChildWritersAreDeclaredConsistently:
    """The psycopg writers cannot be text-checked here; pin their shape."""

    def test_child_writers_are_sync_psycopg_and_single_row(self) -> None:
        child = [row for row in MATRIX if row.driver is Driver.SYNC_PSYCOPG]
        assert {row.writer_id for row in child} == {'T10', 'T11', 'T12'}
        for row in child:
            assert row.shape is Shape.SINGLE, row.writer_id
            assert row.attempt is Attempt.NONE, row.writer_id

    def test_shared_function_writers_declare_the_same_statement(self) -> None:
        """T10 and T11 are branches of one function, as the allowlist records."""
        assert (
            MATRIX[9].statement
            == MATRIX[10].statement
            == '_handle_workflow_stop_before_start'
        )
