"""The transition matrix is checked against the functions it describes.

`tests/lifecycle_matrix.py` declares what each terminal writer does. A
declaration nobody executes drifts from the code silently, so these tests read
the body of each writer's SQL function, as the migration chain installs it,
and assert the declared guards, shape, and notification behavior are
actually there.

Every writer moves its row to history: a single-row function calls
`horsies_move_task_to_history` with its target status, and a batch function
inserts the history rows itself. Sixteen contracts are fifteen functions;
T04 and T05 deliberately share one.
"""

from __future__ import annotations

import re
from typing import Final

import pytest

from tests.baseline_sql import installed_function
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
from tests.terminal_writer_scan import (
    FROZEN_TERMINAL_WRITERS,
    history_insert_statuses,
    update_clauses,
)

pytestmark = [pytest.mark.unit]


FUNCTION_OF_WRITER: Final[dict[str, str]] = {
    'T01': 'horsies_cancel_task',
    'T02': 'horsies_abandon_owned_nodes',
    'T03': 'horsies_cancel_owned_nodes',
    'T04': 'horsies_finish_failed_attempt',
    'T05': 'horsies_finish_failed_attempt',
    'T06': 'horsies_finish_workflow_success',
    'T07': 'horsies_complete_task_fused',
    'T08': 'horsies_cancel_owned_orphan',
    'T09': 'horsies_abandon_nodes_of_paused_workflows',
    'T10': 'horsies_abandon_owned_node',
    'T11': 'horsies_cancel_owned_node',
    'T12': 'horsies_expire_owned_claim',
    'T13': 'horsies_finish_stale_attempt',
    'T14': 'horsies_expire_pending_tasks',
    'T15': 'horsies_cancel_orphaned_tasks',
    'T16': 'horsies_cancel_nodes_of_cancelled_workflow',
}
"""The installed function that implements each writer contract."""

_MOVE_FUNCTION: Final = 'horsies_move_task_to_history'
_BATCH_MOVE_FUNCTION: Final = 'horsies_move_tasks_to_history'
_MOVE_CALL_RE: Final = re.compile(
    r'\bhorsies_move_tasks?_to_history\(\s*[\w.]+,\s*\'(\w+)\'',
)
"""A call of the single-row or the batch move, capturing the status it is given."""

_WORKER_FENCE_RE: Final = re.compile(
    r'= CAST\(p_worker_id AS VARCHAR\)|\.claimed_by_worker_id = p_worker_id\b',
)
"""The ownership comparison: the locked row's worker against the caller's.
The interim functions cast the parameter; the lifecycle functions compare
the locked row's column with it."""

_GENERATION_FENCE_RE: Final = re.compile(r'claimed_at = p_claimed_at\b')
_PAIRWISE_GENERATION_FENCE: Final = '= input.expected_claimed_at'
_DEADLINE_GUARD_RE: Final = re.compile(r'good_until\s*<=')
_FINALIZING_STALE_GUARD_RE: Final = re.compile(r'finalizing_at\s*<')
_HEARTBEAT_STALE_GUARD: Final = 'secs => p_stale_after_ms / 1000.0'
_WORKFLOW_STATUS_GUARD_RE: Final = re.compile(r'\bw\.status\s*(?:=|IN\b)')
_RUNNABLE_LINK: Final = "wt.status IN ('ENQUEUED', 'READY', 'PENDING', 'RUNNING')"
_LINK_ABSENT_RE: Final = re.compile(
    r'\b(?:v_node_status|v_capture\.node_status|runnable_link\.found) IS NULL\b',
)
_CAPACITY_WAKE: Final = "pg_notify('task_queue_' || v_ctx.queue_name, 'capacity')"
_MOVE_EVENTS: Final = (
    "pg_notify('task_done',",
    "pg_notify('horsies_task_status',",
)
_ATTEMPT_UPSERT: Final = 'INSERT INTO horsies_task_attempts'
_RUNNING_ATTEMPT_INSERT: Final = "IF v_row.status = 'RUNNING' THEN INSERT INTO horsies_task_attempts"
"""The attempt insert of a writer that records the attempt of a RUNNING row only."""
_TERMINAL_TIME: Final = 'v_terminal_at := NOW()'
_LINE_COMMENT_RE: Final = re.compile(r'--[^\n]*')


def _body(writer_id: str) -> str:
    """The writer's function with its ``--`` comments removed.

    A comment may name a marker ("SKIP LOCKED never waits") that the code
    does not hold.
    """
    return _LINE_COMMENT_RE.sub('', installed_function(FUNCTION_OF_WRITER[writer_id]))


def _normalised(writer_id: str) -> str:
    return ' '.join(_body(writer_id).split())


def _signature(writer_id: str) -> str:
    """The parameter list of the writer's function."""
    body = _body(writer_id)
    return body[: body.index(') RETURNS')]


def _predicate(writer_id: str) -> str:
    """The function text with its live-row SET assignments removed.

    A fence is a predicate, and searching whole function text confuses it with
    a SET clause clearing the same columns. Only the live-status updates of a
    function hold SET windows on `horsies_tasks`; a move holds none.
    """
    text = _body(writer_id)
    for window, _ in update_clauses(text):
        text = text.replace(window, ' ')
    return ' '.join(text.split())


def _moved_statuses(writer_id: str) -> set[str]:
    """The statuses the function gives its moved rows.

    A writer passes a literal to the single-row or the batch move; a function
    that inserts history rows itself writes one in its history insert.
    """
    body = _body(writer_id)
    return set(_MOVE_CALL_RE.findall(body)) | {
        status for status in history_insert_statuses(body) if status != 'PARAM'
    }


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
        """Sixteen contracts resolve to fifteen functions, each of them a
        history writer of the allowlist or a caller of a move."""
        assert set(FUNCTION_OF_WRITER) == {row.writer_id for row in MATRIX}
        functions = set(FUNCTION_OF_WRITER.values())
        assert len(functions) == 15
        history_writers = {name for _, name, _ in FROZEN_TERMINAL_WRITERS}
        assert {_MOVE_FUNCTION, _BATCH_MOVE_FUNCTION} <= history_writers
        for row in MATRIX:
            name = FUNCTION_OF_WRITER[row.writer_id]
            calls_move = _MOVE_CALL_RE.search(_body(row.writer_id)) is not None
            assert (name in history_writers) != calls_move, row.writer_id


class TestDeclaredGuardsMatchTheStatements:
    """Each declared property is present in the function that carries it."""

    @pytest.mark.parametrize('row', MATRIX, ids=_ids(list(MATRIX)))
    def test_target_status_is_written(self, row: TerminalWriter) -> None:
        assert _moved_statuses(row.writer_id) == {row.target_status}, row.writer_id

    @pytest.mark.parametrize('row', MATRIX, ids=_ids(list(MATRIX)))
    def test_declared_source_statuses_appear(self, row: TerminalWriter) -> None:
        sql = _normalised(row.writer_id)
        for status in row.source_statuses:
            assert f"'{status}'" in sql, f'{row.writer_id}: {status}'

    @pytest.mark.parametrize('row', MATRIX, ids=_ids(list(MATRIX)))
    def test_fence_markers(self, row: TerminalWriter) -> None:
        sql = _predicate(row.writer_id)
        match row.fence:
            case Fence.NONE:
                assert _WORKER_FENCE_RE.search(sql) is None, row.writer_id
            case Fence.WORKER:
                assert _WORKER_FENCE_RE.search(sql) is not None, row.writer_id
                # Deliberately generation-free: the deadline guard makes the
                # outcome correct for whichever generation holds the row.
                assert 'p_claimed_at' not in sql, row.writer_id
            case Fence.WORKER_AND_GENERATION:
                assert _WORKER_FENCE_RE.search(sql) is not None, row.writer_id
                assert 'FOR UPDATE' in sql, row.writer_id
                assert _GENERATION_FENCE_RE.search(sql) is not None, row.writer_id
            case Fence.WORKER_AND_GENERATION_PAIRWISE:
                assert _WORKER_FENCE_RE.search(sql) is not None, row.writer_id
                assert 'unnest(' in sql, row.writer_id
                assert _PAIRWISE_GENERATION_FENCE in sql, row.writer_id

    @pytest.mark.parametrize('row', MATRIX, ids=_ids(list(MATRIX)))
    def test_guard_markers(self, row: TerminalWriter) -> None:
        sql = _predicate(row.writer_id)
        for guard in row.guards:
            match guard:
                case Guard.DEADLINE:
                    assert _DEADLINE_GUARD_RE.search(sql) is not None, row.writer_id
                case Guard.STALENESS:
                    assert _FINALIZING_STALE_GUARD_RE.search(sql) is not None, (
                        row.writer_id
                    )
                    assert _HEARTBEAT_STALE_GUARD in sql, row.writer_id
                case Guard.WORKFLOW_STATUS:
                    assert _WORKFLOW_STATUS_GUARD_RE.search(sql) is not None, row.writer_id
                case Guard.WORKFLOW_LINK_ABSENT:
                    assert _RUNNABLE_LINK in sql, row.writer_id
                    assert _LINK_ABSENT_RE.search(sql) is not None, row.writer_id
                case Guard.WORKFLOW_LINK_STATE:
                    assert 'wt.status' in sql, row.writer_id
                case Guard.NONE:
                    pass

    @pytest.mark.parametrize('row', MATRIX, ids=_ids(list(MATRIX)))
    def test_shape_markers(self, row: TerminalWriter) -> None:
        sql = _normalised(row.writer_id)
        match row.shape:
            case Shape.SET_WISE_SKIP_LOCKED:
                assert 'SKIP LOCKED' in sql, row.writer_id
            case Shape.FUSED:
                assert 'SKIP LOCKED' not in sql, row.writer_id
                assert _ATTEMPT_UPSERT in sql, row.writer_id
                assert _MOVE_CALL_RE.search(sql) is not None, row.writer_id
                assert _CAPACITY_WAKE in sql, row.writer_id
            case Shape.SINGLE | Shape.SET_WISE:
                assert 'SKIP LOCKED' not in sql, row.writer_id

    @pytest.mark.parametrize('row', MATRIX, ids=_ids(list(MATRIX)))
    def test_result_writing(self, row: TerminalWriter) -> None:
        """A writer stores a caller's result exactly when it takes one."""
        assert ('p_result' in _signature(row.writer_id)) is row.writes_result, (
            row.writer_id
        )

    @pytest.mark.parametrize('row', MATRIX, ids=_ids(list(MATRIX)))
    def test_statement_level_notify(self, row: TerminalWriter) -> None:
        """Only the fused path and the failed attempt send the capacity wake;
        every move sends its status events, from the move or from the batch
        itself."""
        sql = _normalised(row.writer_id)
        expected = row.emits_notify is EmitsNotify.FUSED_CAPACITY_WAKE
        assert (_CAPACITY_WAKE in sql) is expected, row.writer_id
        calls_move = _MOVE_CALL_RE.search(sql) is not None
        sends_events = all(event in sql for event in _MOVE_EVENTS)
        assert calls_move or sends_events, row.writer_id

    @pytest.mark.parametrize('function', [_MOVE_FUNCTION, _BATCH_MOVE_FUNCTION])
    def test_the_moves_send_the_status_events(self, function: str) -> None:
        sql = ' '.join(installed_function(function).split())
        for event in _MOVE_EVENTS:
            assert event in sql, event

    @pytest.mark.parametrize('row', MATRIX, ids=_ids(list(MATRIX)))
    def test_attempt_writes(self, row: TerminalWriter) -> None:
        sql = _normalised(row.writer_id)
        match row.attempt:
            case Attempt.FUSED_UPSERT:
                assert _ATTEMPT_UPSERT in sql, row.writer_id
            case Attempt.RUNNING_INSERT:
                assert _RUNNING_ATTEMPT_INSERT in sql, row.writer_id
            case Attempt.NONE:
                assert _ATTEMPT_UPSERT not in sql, row.writer_id

    @pytest.mark.parametrize('row', MATRIX, ids=_ids(list(MATRIX)))
    def test_every_writer_takes_terminal_at_from_the_database_clock(
        self,
        row: TerminalWriter,
    ) -> None:
        """Carried from 0.4.5; the matrix must not describe a writer without it."""
        assert _TERMINAL_TIME in _normalised(row.writer_id), row.writer_id


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
