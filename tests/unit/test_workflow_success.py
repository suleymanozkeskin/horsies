# pyright: reportPrivateUsage=false
"""The success of a workflow task: its row decoder, its command, its DDL."""

from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Any

import pytest

from horsies.core.history.terminalization.move import (
    FINISH_WORKFLOW_SUCCESS_FUNCTION,
)
from horsies.core.lifecycle.commands import CompleteLockedTask
from horsies.core.lifecycle.fences import OwnedClaim, PriorLockedRead
from horsies.core.lifecycle.operations import TerminalizationKind
from horsies.core.lifecycle.outcomes import (
    AlreadyApplied,
    LostClaim,
    ObservedTaskState,
    OutcomeDecodeError,
    PhaseTwoOwed,
    PhaseTwoStep,
    SourceStateConflict,
    SuccessApplied,
    TaskAbsent,
    decode_workflow_success_row,
)
from horsies.core.types.status import TaskStatus
from horsies.core.worker.workflow_success import (
    FINISH_WORKFLOW_SUCCESS_SQL,
    FinishWorkflowSuccess,
    success_for,
    success_statement,
)
from tests.baseline_sql import (
    baseline_function,
    baseline_returned_columns,
    baseline_type,
)

pytestmark = [pytest.mark.unit]

TASK_ID = '0b4d8a52-7d0e-4a0e-9d11-6b2f1f7c9e02'
NOW = datetime(2026, 11, 11, 12, 0, tzinfo=timezone.utc)


def _row(**overrides: Any) -> dict[str, Any]:
    """An APPLIED row."""
    row: dict[str, Any] = {
        'task_id': TASK_ID,
        'ordinality': None,
        'outcome': 'APPLIED',
        'terminal_at': NOW,
        'terminalization_kind': 'COMPLETE_LOCKED',
        'observed_status': 'RUNNING',
        'observed_worker_id': 'w-1',
        'observed_claimed_at': NOW,
        'guard_kind': None,
        'observed_guard': None,
        'queue_name': 'q1',
        'task_name': 'task_a',
        'node_has_dependents': True,
        'downstream_subworkflow': 'NONE',
    }
    row.update(overrides)
    return row


def _miss_row(outcome: str, **overrides: Any) -> dict[str, Any]:
    row = _row(
        outcome=outcome,
        terminal_at=None,
        terminalization_kind=None,
        queue_name=None,
        task_name=None,
        node_has_dependents=None,
        downstream_subworkflow=None,
    )
    row.update(overrides)
    return row


_OBSERVED = ObservedTaskState(
    status=TaskStatus.RUNNING, worker_id='w-1', claimed_at=NOW
)


class TestDecode:
    def test_an_applied_success_owes_phase_two(self) -> None:
        assert decode_workflow_success_row(_row()) == SuccessApplied(
            task_id=TASK_ID,
            terminal_at=NOW,
            observed=_OBSERVED,
            phase_two=PhaseTwoOwed(
                task_name='task_a',
                queue_name='q1',
                step=PhaseTwoStep.DEPENDENTS_WITHOUT_SUBWORKFLOW,
            ),
        )

    def test_a_reachable_subworkflow_owes_a_transaction_step(self) -> None:
        decoded = decode_workflow_success_row(_row(downstream_subworkflow='REACHABLE'))

        assert isinstance(decoded, SuccessApplied)
        assert decoded.phase_two.step is PhaseTwoStep.SUBWORKFLOW_REACHABLE

    def test_a_node_without_dependents_owes_a_finish_only(self) -> None:
        decoded = decode_workflow_success_row(_row(node_has_dependents=False))

        assert isinstance(decoded, SuccessApplied)
        assert decoded.phase_two.step is PhaseTwoStep.NO_DEPENDENTS

    def test_a_replay_is_already_applied(self) -> None:
        row = _miss_row(
            'ALREADY_APPLIED',
            terminal_at=NOW,
            terminalization_kind='COMPLETE_LOCKED',
            observed_status='COMPLETED',
            observed_worker_id=None,
            observed_claimed_at=None,
        )

        outcome = decode_workflow_success_row(row)

        match outcome:
            case AlreadyApplied(terminal_at=terminal_at, kind=kind):
                assert terminal_at == NOW
                assert kind is TerminalizationKind.COMPLETE_LOCKED
            case other:
                raise AssertionError(other)

    def test_a_lost_claim(self) -> None:
        assert isinstance(
            decode_workflow_success_row(_miss_row('LOST_CLAIM')), LostClaim
        )

    def test_a_conflict(self) -> None:
        outcome = decode_workflow_success_row(
            _miss_row('SOURCE_STATE_CONFLICT', observed_status='CLAIMED'),
        )

        assert isinstance(outcome, SourceStateConflict)
        assert outcome.observed.status is TaskStatus.CLAIMED

    def test_an_absent_task(self) -> None:
        row = _miss_row(
            'TASK_ABSENT',
            observed_status=None,
            observed_worker_id=None,
            observed_claimed_at=None,
        )

        assert decode_workflow_success_row(row) == TaskAbsent(
            task_id=TASK_ID, ordinality=None
        )


class TestDecodeFailsClosed:
    @pytest.mark.parametrize(
        'row',
        [
            {key: value for key, value in _row().items() if key != 'task_name'},
            {**_row(), 'retry_count': None},
            {key: value for key, value in _row().items() if key != 'guard_kind'},
        ],
        ids=['missing_routing_column', 'unknown_column', 'missing_shared_column'],
    )
    def test_a_row_of_another_shape(self, row: dict[str, Any]) -> None:
        with pytest.raises(OutcomeDecodeError, match='row shape'):
            decode_workflow_success_row(row)

    @pytest.mark.parametrize(
        'row',
        [
            _row(terminalization_kind='COMPLETE_FUSED'),
            _row(terminalization_kind='FAIL_RUNNING'),
            _row(task_name=None),
            _row(queue_name=None),
            _row(node_has_dependents=None),
            _row(node_has_dependents=False, downstream_subworkflow='REACHABLE'),
            _row(downstream_subworkflow=None),
            _miss_row('LOST_CLAIM', node_has_dependents=False),
            _miss_row('LOST_CLAIM', downstream_subworkflow='NONE'),
            _miss_row('LOST_CLAIM', queue_name='q1'),
            _miss_row('LOST_CLAIM', task_name='task_a'),
            _row(outcome='RETRY_SCHEDULED'),
        ],
        ids=[
            'applied_fused_kind',
            'applied_failure_kind',
            'applied_without_task_name',
            'applied_without_queue_name',
            'applied_without_the_dependents_fact',
            'no_dependents_and_a_reachable_subworkflow',
            'dependents_and_no_downstream_fact',
            'refusal_with_a_dependents_fact',
            'refusal_with_a_downstream_fact',
            'refusal_with_queue_name',
            'refusal_with_task_name',
            'unknown_outcome',
        ],
    )
    def test_a_column_against_its_outcome(self, row: dict[str, Any]) -> None:
        with pytest.raises(OutcomeDecodeError):
            decode_workflow_success_row(row)


class TestCommand:
    def test_the_success_of_a_locked_completion_carries_the_full_fence(self) -> None:
        completion = CompleteLockedTask(
            task_id=TASK_ID,
            fence=PriorLockedRead(worker_id='w-1'),
            result_json='{"ok": 1}',
        )

        assert success_for(completion, claimed_at=NOW) == FinishWorkflowSuccess(
            task_id=TASK_ID,
            fence=OwnedClaim(worker_id='w-1', claimed_at=NOW),
            result_json='{"ok": 1}',
        )

    def test_the_statement_binds_each_field(self) -> None:
        statement = success_statement(
            FinishWorkflowSuccess(
                task_id=TASK_ID,
                fence=OwnedClaim(worker_id='w-1', claimed_at=NOW),
                result_json='{"ok": 1}',
            ),
        )

        assert statement.statement is FINISH_WORKFLOW_SUCCESS_SQL
        assert dict(statement.parameters) == {
            'task_id': TASK_ID,
            'worker_id': 'w-1',
            'claimed_at': NOW,
            'result': '{"ok": 1}',
        }


_COLUMN = re.compile(r'^\s+(\w+) (\w[\w ]*?),?$', re.M)
OUTCOME_TYPE_DDL = baseline_type('horsies_terminalization_outcome')
_BODY = baseline_function(FINISH_WORKFLOW_SUCCESS_FUNCTION)


class TestProgram:
    def test_the_first_ten_columns_are_the_shared_outcome_row(self) -> None:
        """By name: the decoders read the columns by name. The routing facts
        of phase 2 follow them."""
        shared = [column for column, _ in _COLUMN.findall(OUTCOME_TYPE_DDL)]
        success = baseline_returned_columns(FINISH_WORKFLOW_SUCCESS_FUNCTION)

        assert len(shared) == 10
        assert [column for column, _ in success[:10]] == shared
        assert success[10:] == (
            ('queue_name', 'text'),
            ('task_name', 'text'),
            ('node_has_dependents', 'boolean'),
            ('downstream_subworkflow', 'text'),
        )

    def test_the_result_is_checked_before_any_lock(self) -> None:
        assert _BODY.index('not_null_violation') < _BODY.index('pg_advisory_xact_lock')

    def test_the_advisory_lock_comes_before_the_row_lock(self) -> None:
        assert _BODY.index('pg_advisory_xact_lock') < _BODY.index('FROM horsies_tasks')
        assert _BODY.index('FROM horsies_tasks') < _BODY.index('FOR UPDATE')

    def test_the_fence_holds_the_worker_and_the_generation(self) -> None:
        """The row is read and locked by its id alone; the claim is checked
        on the locked row, and a NULL comparison counts as a miss. A NULL
        generation is refused before the lock, so the fence has no NULL arm."""
        read = _BODY[_BODY.index('FROM horsies_tasks') : _BODY.index('FOR UPDATE')]
        fence = _BODY[_BODY.index('FOR UPDATE') : _BODY.index('RETURN QUERY')]

        assert 'WHERE t.id = p_task_id' in read
        assert 'status' not in read
        assert "v_ctx.status = 'RUNNING'" in fence
        assert 'v_ctx.claimed_by_worker_id = p_worker_id' in fence
        assert 'AND v_ctx.claimed_at = p_claimed_at' in fence
        assert 'p_claimed_at IS NULL' not in fence
        assert ') IS NOT TRUE THEN' in fence
        assert _BODY.index('num_nulls(p_task_id, p_worker_id, p_claimed_at)') < (
            _BODY.index('pg_advisory_xact_lock')
        )

    def test_a_plain_task_is_refused_before_any_write(self) -> None:
        refusal = _BODY.index('IF NOT v_ctx.is_workflow_task THEN')

        assert _BODY.index('FOR UPDATE') < refusal
        assert refusal < _BODY.index('INSERT INTO horsies_task_attempts')

    def test_the_attempt_row_comes_before_the_move(self) -> None:
        assert _BODY.index('INSERT INTO horsies_task_attempts') < _BODY.index(
            'horsies_move_task_to_history(',
        )

    def test_the_move_writes_the_kind_that_owes_phase_two(self) -> None:
        move = _BODY[_BODY.index('horsies_move_task_to_history(') :]

        assert "'COMPLETED', 'COMPLETE_LOCKED'" in move

    def test_it_sends_no_wake(self) -> None:
        assert 'pg_notify' not in _BODY

