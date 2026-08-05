"""The transition matrix is checked against the statements it describes.

`tests/lifecycle_matrix.py` declares what each terminal writer does. A
declaration nobody executes drifts from the code silently, so these tests read
the statement text back and assert the declared guards, shape, and notification
behavior are actually there.

The matrix preserves the contracts of the original sixteen writers. Every row
is anchored to the database-owned operation that now implements that contract;
T04 and T05 deliberately share one function.
"""

from __future__ import annotations

from collections import Counter

import pytest

from horsies.core.schemas.terminalization import (
    CREATE_ABANDON_NODES_OF_PAUSED_WORKFLOWS_SQL,
    CREATE_ABANDON_OWNED_NODE_SQL,
    CREATE_ABANDON_OWNED_NODES_SQL,
    CREATE_CANCEL_LOCKED_TASK_SQL,
    CREATE_CANCEL_NODES_OF_CANCELLED_WORKFLOW_SQL,
    CREATE_CANCEL_ORPHANED_TASKS_SQL,
    CREATE_CANCEL_OWNED_NODE_SQL,
    CREATE_CANCEL_OWNED_NODES_SQL,
    CREATE_CANCEL_OWNED_ORPHAN_SQL,
    CREATE_COMPLETE_LOCKED_TASK_SQL,
    CREATE_COMPLETE_TASK_FUSED_SQL,
    CREATE_EXPIRE_OWNED_CLAIM_SQL,
    CREATE_EXPIRE_PENDING_TASKS_SQL,
    CREATE_FAIL_LOCKED_TASK_SQL,
    CREATE_FAIL_STALE_TASK_SQL,
)
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
    _update_clauses,
)

pytestmark = [pytest.mark.unit]


# Database function text for every original writer contract.
_STATEMENT_TEXT: dict[str, str] = {
    'T01': CREATE_CANCEL_LOCKED_TASK_SQL.text,
    'T02': CREATE_ABANDON_OWNED_NODES_SQL.text,
    'T03': CREATE_CANCEL_OWNED_NODES_SQL.text,
    'T04': CREATE_FAIL_LOCKED_TASK_SQL.text,
    'T05': CREATE_FAIL_LOCKED_TASK_SQL.text,
    'T06': CREATE_COMPLETE_LOCKED_TASK_SQL.text,
    'T07': CREATE_COMPLETE_TASK_FUSED_SQL.text,
    'T08': CREATE_CANCEL_OWNED_ORPHAN_SQL.text,
    'T09': CREATE_ABANDON_NODES_OF_PAUSED_WORKFLOWS_SQL.text,
    'T10': CREATE_ABANDON_OWNED_NODE_SQL.text,
    'T11': CREATE_CANCEL_OWNED_NODE_SQL.text,
    'T12': CREATE_EXPIRE_OWNED_CLAIM_SQL.text,
    'T13': CREATE_FAIL_STALE_TASK_SQL.text,
    'T14': CREATE_EXPIRE_PENDING_TASKS_SQL.text,
    'T15': CREATE_CANCEL_ORPHANED_TASKS_SQL.text,
    'T16': CREATE_CANCEL_NODES_OF_CANCELLED_WORKFLOW_SQL.text,
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
        """The active runtime inventory agrees with non-retired matrix rows.

        The allowlist keys on (module, statement, statuses) with a count; the
        matrix carries one row per original writer. Retired runtime writers
        remain matrix rows because their behavior is the migration contract.

        The allowlist also guards the database-owned operations these
        statements are being migrated to. Those are not matrix rows — the
        matrix describes what exists to be replaced — so the comparison is
        scoped to the runtime modules the matrix covers.
        """
        retired_runtime_ids = frozenset({'T10', 'T11'})
        from_matrix = Counter(
            (row.module, row.statement, row.target_status)
            for row in MATRIX
            if row.writer_id not in retired_runtime_ids
        )
        from_allowlist = Counter({
            key: count
            for key, count in FROZEN_TERMINAL_WRITERS.items()
            if not key[0].endswith('schemas/terminalization.py')
        })
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
                assert 'p_claimed_at' not in sql, row.writer_id
            case Fence.PRIOR_LOCKED_SELECT:
                assert 'claimed_by_worker_id =' in sql, row.writer_id
            case Fence.WORKER_AND_GENERATION:
                assert 'claimed_by_worker_id =' in sql, row.writer_id
                assert 'claimed_at' in sql, row.writer_id
            case Fence.WORKER_AND_GENERATION_PAIRWISE:
                assert 'claimed_by_worker_id =' in sql, row.writer_id
                assert 'unnest(' in sql, row.writer_id
                assert 'input.claimed_at' in sql, row.writer_id

    @pytest.mark.parametrize('row', _TEXT_ROWS, ids=_ids(_TEXT_ROWS))
    def test_guard_markers(self, row: TerminalWriter) -> None:
        sql = _predicate(row.writer_id)
        for guard in row.guards:
            match guard:
                case Guard.DEADLINE:
                    assert 'good_until' in sql, row.writer_id
                case Guard.STALENESS:
                    assert 'finalizing_at' in sql, row.writer_id
                    assert 'p_stale_after_ms' in sql, row.writer_id
                case Guard.WORKFLOW_STATUS:
                    assert 'w.status =' in sql, row.writer_id
                case Guard.WORKFLOW_LINK_ABSENT:
                    assert (
                        'NOT EXISTS' in sql or 'ctx.node_status IS NULL' in sql
                    ), row.writer_id
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
                assert 'WITH ctx AS (' in sql, row.writer_id
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


class TestWriterVariantGrouping:
    """How many distinct operations the sixteen writers actually are.

    A writer needs its own persistence variant when it differs in fence,
    cardinality, coupled write, target status, or guards. Writers agreeing on
    all five differ only in payload and must share one.

    This is the check that a consolidation unified something rather than
    renaming sixteen writers as sixteen commands — and, equally, the check that
    it did not over-merge writers whose transitions genuinely differ.
    """

    @staticmethod
    def _signature(row: TerminalWriter) -> tuple[object, ...]:
        return (
            row.fence,
            row.shape,
            row.coupled_write,
            row.target_status,
            row.guards,
        )

    def test_writers_group_into_fifteen_variants(self) -> None:
        groups: dict[tuple[object, ...], list[str]] = {}
        for row in MATRIX:
            groups.setdefault(self._signature(row), []).append(row.writer_id)

        assert len(groups) == 15, (
            'the number of structurally distinct writers changed; the '
            f'persistence model must follow.\n{sorted(groups.values())}'
        )

    def test_the_only_mandated_merge_is_the_two_failure_writers(self) -> None:
        """Exactly one pair differs only in payload — the rule has teeth."""
        groups: dict[tuple[object, ...], list[str]] = {}
        for row in MATRIX:
            groups.setdefault(self._signature(row), []).append(row.writer_id)

        merged = sorted(v for v in groups.values() if len(v) > 1)
        assert merged == [['T04', 'T05']], merged

    def test_target_status_is_load_bearing_in_the_signature(self) -> None:
        """Dropping it merges a failure transition with a completion one.

        Nothing else in the signature separates them: same fence, same
        cardinality, no coupled write, no guards.
        """
        without_status: dict[tuple[object, ...], list[str]] = {}
        for row in MATRIX:
            key = (row.fence, row.shape, row.coupled_write, row.guards)
            without_status.setdefault(key, []).append(row.writer_id)

        over_merged = sorted(v for v in without_status.values() if len(v) > 1)
        assert over_merged == [['T04', 'T05', 'T06']], over_merged
        statuses = {
            row.target_status for row in MATRIX if row.writer_id in {'T04', 'T06'}
        }
        assert statuses == {'FAILED', 'COMPLETED'}

    def test_status_and_guards_separate_the_batch_reapers_jointly(self) -> None:
        """Either field alone keeps pending expiry apart from orphan cleanup.

        They agree on fence, cardinality and coupled write, so with both fields
        dropped the rule would merge a deadline-driven expiry with a linkage-
        driven cancellation. Guards are redundant against target status here,
        but a future pair differing only in guard would still need them.
        """
        without_either: dict[tuple[object, ...], list[str]] = {}
        for row in MATRIX:
            key = (row.fence, row.shape, row.coupled_write)
            without_either.setdefault(key, []).append(row.writer_id)

        over_merged = sorted(v for v in without_either.values() if len(v) > 1)
        assert ['T14', 'T15'] in over_merged, over_merged

        reapers = {row.writer_id: row for row in MATRIX if row.writer_id in {'T14', 'T15'}}
        assert reapers['T14'].target_status != reapers['T15'].target_status
        assert reapers['T14'].guards != reapers['T15'].guards


class TestChildWritersAreDeclaredConsistently:
    """The child call sites remain sync psycopg and single-row operations."""

    def test_child_writers_are_sync_psycopg_and_single_row(self) -> None:
        child = [row for row in MATRIX if row.driver is Driver.SYNC_PSYCOPG]
        assert {row.writer_id for row in child} == {'T10', 'T11', 'T12'}
        for row in child:
            assert row.shape is Shape.SINGLE, row.writer_id
            assert row.attempt is Attempt.NONE, row.writer_id

    def test_shared_function_writers_declare_the_same_statement(self) -> None:
        """T10 and T11 remain two branches of the child lifecycle handler."""
        assert (
            MATRIX[9].statement
            == MATRIX[10].statement
            == '_handle_workflow_stop_before_start'
        )
