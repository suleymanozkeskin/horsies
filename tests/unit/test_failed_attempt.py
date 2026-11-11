# pyright: reportPrivateUsage=false
"""The failed-attempt finish: its row decoder, its parameters, its DDL."""

from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Any

import pytest

from horsies.core.history.cutover.program import (
    installation_fragments,
    teardown_statements,
)
from horsies.core.history.terminalization.move import (
    FAILED_ATTEMPT_OUTCOME_TYPE,
    FAILED_ATTEMPT_OUTCOME_TYPE_DDL,
    FINISH_FAILED_ATTEMPT_FUNCTION,
    LOCK_RUNNING_TASK_FUNCTION,
    RETRY_IN_SAME_CLASS,
    RETRY_ISOLATED,
    failed_attempt_fragments,
    failed_attempt_program_ddl,
)
from horsies.core.history.terminalization.outcome import OUTCOME_TYPE_DDL
from horsies.core.lifecycle.fences import OwnedClaim
from horsies.core.lifecycle.operations import TerminalizationKind
from horsies.core.lifecycle.outcomes import (
    AlreadyApplied,
    CapacityWoken,
    FailureApplied,
    LostClaim,
    ObservedTaskState,
    OutcomeDecodeError,
    PhaseTwoOwed,
    RetryNotRequested,
    RetryPastDeadline,
    RetryScheduled,
    SourceStateConflict,
    TaskAbsent,
    decode_failed_attempt_row,
)
from horsies.core.models.tasks import OperationalErrorCode, OutcomeCode, TaskError
from horsies.core.types.status import TaskStatus
from horsies.core.worker.dispatch import _error_code_text
from horsies.core.worker.failed_attempt import (
    AttemptFailure,
    AttemptFailureKind,
    FinishFailedAttempt,
    NoRetry,
    RetryAfter,
    _placement_value,
    finish_parameters,
)
from horsies.core.worker.finalize import _attempt_failure_of
from horsies.core.lifecycle.classify import (
    ErrorProduced,
    ResultUndecodable,
    WorkerLevelFailure,
)
from horsies.core.worker.shared_stop import RetryPlacement

pytestmark = [pytest.mark.unit]

TASK_ID = '0b4d8a52-7d0e-4a0e-9d11-6b2f1f7c9e01'
NOW = datetime(2026, 11, 11, 12, 0, tzinfo=timezone.utc)
LATER = datetime(2026, 11, 11, 12, 5, tzinfo=timezone.utc)


def _row(**overrides: Any) -> dict[str, Any]:
    """An APPLIED row of a plain task, with nothing past its deadline."""
    row: dict[str, Any] = {
        'task_id': TASK_ID,
        'ordinality': None,
        'outcome': 'APPLIED',
        'terminal_at': NOW,
        'terminalization_kind': 'FAIL_RUNNING',
        'observed_status': 'RUNNING',
        'observed_worker_id': 'w-1',
        'observed_claimed_at': NOW,
        'guard_kind': None,
        'observed_guard': None,
        'retry_disposition': 'NOT_REQUESTED',
        'retry_count': None,
        'next_retry_at': None,
        'good_until': None,
        'is_workflow_task': False,
        'queue_name': 'q1',
        'task_name': 'task_a',
    }
    row.update(overrides)
    return row


def _retry_row(**overrides: Any) -> dict[str, Any]:
    row = _row(
        outcome='RETRY_SCHEDULED',
        terminal_at=None,
        terminalization_kind=None,
        retry_disposition=None,
        retry_count=2,
        next_retry_at=LATER,
        is_workflow_task=None,
        task_name=None,
    )
    row.update(overrides)
    return row


def _miss_row(outcome: str, **overrides: Any) -> dict[str, Any]:
    row = _row(
        outcome=outcome,
        terminal_at=None,
        terminalization_kind=None,
        retry_disposition=None,
        is_workflow_task=None,
        queue_name=None,
        task_name=None,
    )
    row.update(overrides)
    return row


_OBSERVED = ObservedTaskState(
    status=TaskStatus.RUNNING,
    worker_id='w-1',
    claimed_at=NOW,
)


class TestDecodeWrites:
    def test_a_plain_failure_is_applied_and_woken(self) -> None:
        assert decode_failed_attempt_row(_row()) == FailureApplied(
            task_id=TASK_ID,
            terminal_at=NOW,
            observed=_OBSERVED,
            retry=RetryNotRequested(),
            after=CapacityWoken(),
        )

    def test_a_workflow_failure_owes_phase_two(self) -> None:
        decoded = decode_failed_attempt_row(_row(is_workflow_task=True))

        assert isinstance(decoded, FailureApplied)
        assert decoded.after == PhaseTwoOwed(task_name='task_a', queue_name='q1')

    def test_a_retry_past_its_deadline_carries_both_instants(self) -> None:
        decoded = decode_failed_attempt_row(
            _row(
                retry_disposition='PAST_DEADLINE',
                next_retry_at=LATER,
                good_until=NOW,
            ),
        )

        assert isinstance(decoded, FailureApplied)
        assert decoded.retry == RetryPastDeadline(next_retry_at=LATER, good_until=NOW)

    def test_a_scheduled_retry(self) -> None:
        assert decode_failed_attempt_row(_retry_row()) == RetryScheduled(
            task_id=TASK_ID,
            retry_count=2,
            next_retry_at=LATER,
            queue_name='q1',
            observed=_OBSERVED,
        )


class TestDecodeRefusals:
    def test_a_replay_is_already_applied(self) -> None:
        decoded = decode_failed_attempt_row(
            _miss_row(
                'ALREADY_APPLIED',
                terminal_at=NOW,
                terminalization_kind='FAIL_RUNNING',
                observed_status='FAILED',
                observed_worker_id=None,
                observed_claimed_at=None,
            ),
        )

        assert isinstance(decoded, AlreadyApplied)
        assert decoded.kind is TerminalizationKind.FAIL_RUNNING

    def test_a_lost_claim(self) -> None:
        assert isinstance(decode_failed_attempt_row(_miss_row('LOST_CLAIM')), LostClaim)

    def test_a_conflict(self) -> None:
        decoded = decode_failed_attempt_row(
            _miss_row('SOURCE_STATE_CONFLICT', observed_status='CLAIMED'),
        )

        assert isinstance(decoded, SourceStateConflict)

    def test_an_absent_task(self) -> None:
        decoded = decode_failed_attempt_row(
            _miss_row(
                'TASK_ABSENT',
                observed_status=None,
                observed_worker_id=None,
                observed_claimed_at=None,
            ),
        )

        assert decoded == TaskAbsent(task_id=TASK_ID, ordinality=None)


class TestDecodeFailsClosed:
    @pytest.mark.parametrize(
        'row',
        [
            {key: value for key, value in _row().items() if key != 'task_name'},
            {**_row(), 'extra': 1},
        ],
        ids=['missing_column', 'unknown_column'],
    )
    def test_a_row_of_another_shape(self, row: dict[str, Any]) -> None:
        with pytest.raises(OutcomeDecodeError, match='row shape'):
            decode_failed_attempt_row(row)

    @pytest.mark.parametrize(
        'row',
        [
            _row(retry_disposition=None),
            _row(retry_disposition='LATER'),
            _row(retry_disposition='PAST_DEADLINE'),
            _row(next_retry_at=LATER),
            _row(retry_count=1),
            _row(terminalization_kind='FAIL_STALE'),
            _row(is_workflow_task=None),
            _row(is_workflow_task=True, task_name=None),
            _retry_row(terminal_at=NOW),
            _retry_row(retry_count=None),
            _retry_row(queue_name=None),
            _retry_row(good_until=NOW),
            _miss_row('LOST_CLAIM', retry_count=1),
            _miss_row('LOST_CLAIM', queue_name='q1'),
            _miss_row('APPLIED_ELSEWHERE'),
        ],
        ids=[
            'applied_without_disposition',
            'unknown_disposition',
            'past_deadline_without_instants',
            'not_requested_with_a_retry_time',
            'applied_with_a_retry_count',
            'applied_of_another_kind',
            'applied_without_the_task_kind',
            'phase_two_without_a_task_name',
            'retry_with_a_terminal_time',
            'retry_without_a_count',
            'retry_without_a_queue',
            'retry_with_a_deadline',
            'refusal_with_a_retry_count',
            'refusal_with_a_queue',
            'unknown_outcome',
        ],
    )
    def test_a_column_against_its_outcome(self, row: dict[str, Any]) -> None:
        with pytest.raises(OutcomeDecodeError):
            decode_failed_attempt_row(row)


def _command(retry: NoRetry | RetryAfter) -> FinishFailedAttempt:
    return FinishFailedAttempt(
        task_id=TASK_ID,
        fence=OwnedClaim(worker_id='w-1', claimed_at=NOW),
        retry=retry,
        result_json='{}',
        error_code='BOOM',
        failed_reason='final',
        attempt=AttemptFailure(
            kind=AttemptFailureKind.WORKER_FAILURE,
            error_message='message',
            failed_reason='reason',
        ),
    )


class TestParameters:
    def test_no_retry_sends_no_retry_argument(self) -> None:
        parameters = finish_parameters(_command(NoRetry()))

        assert parameters == {
            'task_id': TASK_ID,
            'worker_id': 'w-1',
            'claimed_at': NOW,
            'expected_retry_count': None,
            'retry_delay_s': None,
            'retry_placement': None,
            'result': '{}',
            'error_code': 'BOOM',
            'failed_reason': 'final',
            'attempt_outcome': 'WORKER_FAILURE',
            'attempt_error_message': 'message',
            'attempt_failed_reason': 'reason',
        }

    @pytest.mark.parametrize(
        ('placement', 'value'),
        [
            (RetryPlacement.SAME_CLASS, RETRY_IN_SAME_CLASS),
            (RetryPlacement.ISOLATED, RETRY_ISOLATED),
        ],
    )
    def test_a_retry_sends_its_count_delay_and_placement(
        self,
        placement: RetryPlacement,
        value: str,
    ) -> None:
        parameters = finish_parameters(
            _command(RetryAfter(retry_count=3, delay_s=12.5, placement=placement)),
        )

        assert (
            parameters['expected_retry_count'],
            parameters['retry_delay_s'],
            parameters['retry_placement'],
        ) == (3, 12.5, value)

    def test_every_placement_has_a_value_the_function_accepts(self) -> None:
        body = failed_attempt_fragments()[1]

        for placement in RetryPlacement:
            assert f"'{_placement_value(placement)}'" in body


class TestAttemptOfAReport:
    def test_a_worker_failure(self) -> None:
        assert _attempt_failure_of(WorkerLevelFailure(detail=None)) == AttemptFailure(
            kind=AttemptFailureKind.WORKER_FAILURE,
            error_message='Worker failure',
            failed_reason='Worker failure',
        )

    def test_an_undecodable_result(self) -> None:
        assert _attempt_failure_of(ResultUndecodable(detail='bad')) == AttemptFailure(
            kind=AttemptFailureKind.FAILED,
            error_message='bad',
            failed_reason=None,
        )

    def test_a_task_error(self) -> None:
        report = ErrorProduced(error=TaskError(error_code='X', message='boom'))

        assert _attempt_failure_of(report) == AttemptFailure(
            kind=AttemptFailureKind.FAILED,
            error_message='boom',
            failed_reason=None,
        )


class TestErrorCodeText:
    @pytest.mark.parametrize(
        ('code', 'text'),
        [
            (OutcomeCode.TASK_TIMEOUT, 'TASK_TIMEOUT'),
            (OperationalErrorCode.WORKER_CRASHED, 'WORKER_CRASHED'),
            ('MY_CODE', 'MY_CODE'),
        ],
    )
    def test_the_stored_spelling(self, code: Any, text: str) -> None:
        assert _error_code_text(TaskError(error_code=code, message='m')) == text


_COLUMN = re.compile(r'^\s+(\w+) (\w[\w ]*?),?$', re.M)


class TestProgram:
    def test_the_first_ten_columns_are_the_shared_outcome_row(self) -> None:
        shared = _COLUMN.findall(OUTCOME_TYPE_DDL)
        failed = _COLUMN.findall(FAILED_ATTEMPT_OUTCOME_TYPE_DDL)

        assert len(shared) == 10
        assert failed[:10] == shared

    def test_the_arguments_are_checked_before_any_lock(self) -> None:
        body = failed_attempt_fragments()[1]

        assert body.index('invalid_parameter_value') < body.index(
            'pg_advisory_xact_lock',
        )

    def test_the_advisory_lock_comes_before_the_row_lock(self) -> None:
        body = failed_attempt_fragments()[1]

        assert body.index('pg_advisory_xact_lock') < body.index('FROM horsies_tasks')
        assert body.index('FROM horsies_tasks') < body.index('FOR UPDATE')

    def test_the_attempt_row_comes_before_the_move(self) -> None:
        body = failed_attempt_fragments()[1]

        assert body.index('INSERT INTO horsies_task_attempts') < body.index(
            'horsies_move_task_to_history',
        )

    def test_the_wake_is_sent_only_after_the_move_of_a_plain_task(self) -> None:
        body = failed_attempt_fragments()[1]

        move = body.index('horsies_move_task_to_history(')
        assert body.index('pg_notify') > move
        assert 'IF NOT v_ctx.is_workflow_task THEN' in body[move:]

    def test_installation_has_the_type_before_the_function(self) -> None:
        fragments = installation_fragments()
        type_at = next(
            index
            for index, fragment in enumerate(fragments)
            if f'CREATE TYPE {FAILED_ATTEMPT_OUTCOME_TYPE}' in fragment
        )
        function_at = next(
            index
            for index, fragment in enumerate(fragments)
            if f'CREATE FUNCTION {FINISH_FAILED_ATTEMPT_FUNCTION}' in fragment
        )

        assert type_at < function_at
        assert any(
            f'CREATE FUNCTION {LOCK_RUNNING_TASK_FUNCTION}' in fragment
            for fragment in fragments
        )

    def test_teardown_drops_both_programs(self) -> None:
        statements = teardown_statements()

        assert (
            f'DROP TYPE IF EXISTS {FAILED_ATTEMPT_OUTCOME_TYPE} CASCADE' in statements
        )
        assert any(LOCK_RUNNING_TASK_FUNCTION in statement for statement in statements)

    def test_the_upgrade_drops_before_it_creates(self) -> None:
        statements = failed_attempt_program_ddl()

        drops = [index for index, s in enumerate(statements) if s.startswith('DROP')]
        creates = [index for index, s in enumerate(statements) if 'CREATE' in s]
        assert max(drops) < min(creates)
        assert len(creates) == 3
