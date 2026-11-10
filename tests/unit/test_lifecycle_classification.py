"""What a finished child means, decided without a database.

Every decision the worker makes before it writes anything is exercised here
against typed reports. No fixtures, no session, no engine — if a case in this
file needed one, the classification would not be pure and the split this layer
exists for would already have failed.

The cases mirror the arms the worker takes today, so the two can be compared
directly when the call sites move over.
"""

from __future__ import annotations

import ast
import dataclasses
from datetime import datetime, timezone
from pathlib import Path

import pytest

from horsies.core.lifecycle.classify import (
    AbortReason,
    AbortedBeforeResult,
    ApplyTerminalization,
    ChildReport,
    ErrorProduced,
    FinalizeContext,
    FinalizeDecision,
    FinalizeNoOpReason,
    NoTerminalAction,
    ReplayWorkflowPhase2,
    ResultProduced,
    ResultUndecodable,
    ScheduleAutomaticRetry,
    WorkerLevelFailure,
    classify,
    parse_abort_reason,
    terminalization_for_refused_retry,
)
from horsies.core.lifecycle.commands import (
    CancelOwnedOrphan,
    CompleteLockedTask,
    CompleteTaskFused,
    FailLockedTask,
)
from horsies.core.lifecycle.fences import OwnedClaim, PriorLockedRead
from horsies.core.models.tasks import (
    OperationalErrorCode,
    OutcomeCode,
    TaskError,
)

pytestmark = [pytest.mark.unit]

_GENERATION = datetime(2026, 8, 4, 12, 0, tzinfo=timezone.utc)
_RESULT = '{"ok": 42}'


def _context(**overrides: object) -> FinalizeContext:
    base: dict[str, object] = {
        'task_id': 't1',
        'worker_id': 'w1',
        'claimed_at': _GENERATION,
        'is_workflow_task': False,
        'queue_name': 'default',
        'result_json': _RESULT,
        'orphan_self_heal_enabled': True,
    }
    base.update(overrides)
    return FinalizeContext(**base)  # type: ignore[arg-type]


def _command(decision: FinalizeDecision) -> object:
    assert isinstance(decision, ApplyTerminalization), decision
    return decision.command


class TestSentinelParsing:
    """The one place a wire reason is compared as a string."""

    @pytest.mark.parametrize(
        'raw,expected',
        [
            ('CLAIM_LOST', AbortReason.CLAIM_LOST),
            ('OWNERSHIP_UNCONFIRMED', AbortReason.OWNERSHIP_UNCONFIRMED),
            ('WORKFLOW_STOPPED', AbortReason.WORKFLOW_STOPPED),
            ('WORKFLOW_CHECK_FAILED', AbortReason.WORKFLOW_CHECK_FAILED),
            ('TASK_EXPIRED', AbortReason.TASK_EXPIRED),
        ],
    )
    def test_every_child_sentinel_parses(
        self,
        raw: str,
        expected: AbortReason,
    ) -> None:
        assert parse_abort_reason(raw) is expected

    def test_expired_sentinel_matches_the_code_the_child_writes(self) -> None:
        """The child sends the enum's value, not a parallel spelling."""
        assert parse_abort_reason(OutcomeCode.TASK_EXPIRED.value) is (
            AbortReason.TASK_EXPIRED
        )

    def test_prose_is_not_a_sentinel(self) -> None:
        """A worker-level failure's text is prose and must survive as prose."""
        assert parse_abort_reason('Worker process crashed (no heartbeat)') is None

    def test_absent_reason_is_not_a_sentinel(self) -> None:
        assert parse_abort_reason(None) is None


class TestAbortsThatWriteNothing:
    @pytest.mark.parametrize(
        'reason,expected',
        [
            (AbortReason.CLAIM_LOST, FinalizeNoOpReason.CLAIM_LOST),
            (
                AbortReason.OWNERSHIP_UNCONFIRMED,
                FinalizeNoOpReason.OWNERSHIP_UNCONFIRMED,
            ),
            (AbortReason.WORKFLOW_STOPPED, FinalizeNoOpReason.WORKFLOW_STOPPED),
        ],
    )
    def test_aborts_are_no_ops(
        self,
        reason: AbortReason,
        expected: FinalizeNoOpReason,
    ) -> None:
        """Another party owns the row's next state; touching it clobbers."""
        decision = classify(AbortedBeforeResult(reason=reason), _context())
        assert decision == NoTerminalAction(reason=expected)

    def test_workflow_stopped_inside_a_payload_is_also_a_no_op(self) -> None:
        """The same sentinel travels two ways and must mean one thing."""
        error = TaskError(
            error_code='WORKFLOW_STOPPED', message='stopped', data={},
        )
        decision = classify(ErrorProduced(error=error), _context())
        assert decision == NoTerminalAction(
            reason=FinalizeNoOpReason.WORKFLOW_STOPPED,
        )


class TestExpiryRoutesByTaskKind:
    def test_plain_task_is_finished(self) -> None:
        """The child already wrote the terminal row and owes nothing more."""
        decision = classify(
            AbortedBeforeResult(reason=AbortReason.TASK_EXPIRED),
            _context(is_workflow_task=False),
        )
        assert decision == NoTerminalAction(
            reason=FinalizeNoOpReason.EXPIRED_PLAIN_TASK,
        )

    def test_workflow_node_still_needs_phase_two(self) -> None:
        """Otherwise its node sits enqueued against a terminal task row."""
        decision = classify(
            AbortedBeforeResult(reason=AbortReason.TASK_EXPIRED),
            _context(is_workflow_task=True),
        )
        assert decision == ReplayWorkflowPhase2(task_id='t1')


class TestOrphanedWorkflowTask:
    def test_self_heal_enabled_cancels_the_orphan(self) -> None:
        decision = classify(
            AbortedBeforeResult(reason=AbortReason.WORKFLOW_CHECK_FAILED),
            _context(is_workflow_task=True),
        )
        assert _command(decision) == CancelOwnedOrphan(
            task_id='t1',
            fence=OwnedClaim(worker_id='w1', claimed_at=_GENERATION),
        )

    def test_self_heal_disabled_leaves_it_claimed(self) -> None:
        """Left for inspection: the reaper declines orphans as well."""
        decision = classify(
            AbortedBeforeResult(reason=AbortReason.WORKFLOW_CHECK_FAILED),
            _context(is_workflow_task=True, orphan_self_heal_enabled=False),
        )
        assert decision == NoTerminalAction(
            reason=FinalizeNoOpReason.ORPHAN_SELF_HEAL_DISABLED,
        )

    def test_orphan_cancel_fences_on_the_dispatched_generation(self) -> None:
        """A re-claim since this dispatch is a different generation's row."""
        decision = classify(
            AbortedBeforeResult(reason=AbortReason.WORKFLOW_CHECK_FAILED),
            _context(claimed_at=None),
        )
        command = _command(decision)
        assert isinstance(command, CancelOwnedOrphan)
        assert command.fence == OwnedClaim(worker_id='w1', claimed_at=None)


class TestSuccess:
    def test_plain_task_completion_fuses_and_wakes_capacity(self) -> None:
        decision = classify(ResultProduced(), _context())
        assert _command(decision) == CompleteTaskFused(
            task_id='t1',
            fence=OwnedClaim(worker_id='w1', claimed_at=_GENERATION),
            result_json=_RESULT,
            notify_channel='task_queue_default',
            notify_payload='capacity:t1',
        )

    def test_queue_name_falls_back_to_default_channel(self) -> None:
        decision = classify(ResultProduced(), _context(queue_name=''))
        command = _command(decision)
        assert isinstance(command, CompleteTaskFused)
        assert command.notify_channel == 'task_queue_default'

    def test_workflow_node_completion_cannot_fuse(self) -> None:
        """Phase 2 still owes work on this row, so the fast path is unsound."""
        decision = classify(ResultProduced(), _context(is_workflow_task=True))
        assert _command(decision) == CompleteLockedTask(
            task_id='t1',
            fence=PriorLockedRead(worker_id='w1'),
            result_json=_RESULT,
        )


class TestFailures:
    def test_task_error_goes_to_retry_policy_first(self) -> None:
        """Eligibility is a database read, so this layer defers it."""
        error = TaskError(
            error_code=OperationalErrorCode.TASK_EXCEPTION,
            message='boom',
            data={},
        )
        decision = classify(ErrorProduced(error=error), _context())
        assert decision == ScheduleAutomaticRetry(task_id='t1', error=error)

    def test_refused_retry_records_the_task_own_payload(self) -> None:
        """The task's account of its failure outlives the retry decision."""
        error = TaskError(
            error_code=OperationalErrorCode.TASK_EXCEPTION,
            message='boom',
            data={},
        )
        decision = terminalization_for_refused_retry(error, _context())
        assert decision.command == FailLockedTask(
            task_id='t1',
            fence=PriorLockedRead(worker_id='w1'),
            result_json=_RESULT,
            error_code='TASK_EXCEPTION',
            failed_reason=None,
        )

    def test_worker_failure_manufactures_its_own_payload(self) -> None:
        """The task produced nothing, so the worker's account is the result."""
        decision = classify(
            WorkerLevelFailure(detail='pool died'), _context(),
        )
        command = _command(decision)
        assert isinstance(command, FailLockedTask)
        assert command.error_code == OperationalErrorCode.BROKER_ERROR.value
        assert command.failed_reason == 'pool died'
        assert 'pool died' in command.result_json

    def test_worker_failure_without_detail_still_records_a_reason(self) -> None:
        decision = classify(WorkerLevelFailure(detail=None), _context())
        command = _command(decision)
        assert isinstance(command, FailLockedTask)
        assert command.failed_reason == 'Worker failure'

    def test_undecodable_payload_fails_as_a_serialization_error(self) -> None:
        decision = classify(
            ResultUndecodable(detail='Result JSON corrupt: unexpected token'),
            _context(),
        )
        command = _command(decision)
        assert isinstance(command, FailLockedTask)
        assert command.error_code == (
            OperationalErrorCode.WORKER_SERIALIZATION_ERROR.value
        )

    def test_undecodable_payload_leaves_the_reason_column_alone(self) -> None:
        """A reason from an earlier attempt is not this transition's to erase."""
        decision = classify(ResultUndecodable(detail='corrupt'), _context())
        command = _command(decision)
        assert isinstance(command, FailLockedTask)
        assert command.failed_reason is None

    def test_string_error_codes_survive_unchanged(self) -> None:
        """A caller's own code is not a registered enum and must not be lost."""
        error = TaskError(error_code='MY_OWN_CODE', message='x', data={})
        decision = terminalization_for_refused_retry(error, _context())
        command = decision.command
        assert isinstance(command, FailLockedTask)
        assert command.error_code == 'MY_OWN_CODE'


class TestDecisionsAreDataOnly:
    """The properties that make this layer testable without a database."""

    _REPORTS: tuple[ChildReport, ...] = (
        AbortedBeforeResult(reason=AbortReason.CLAIM_LOST),
        AbortedBeforeResult(reason=AbortReason.WORKFLOW_CHECK_FAILED),
        AbortedBeforeResult(reason=AbortReason.TASK_EXPIRED),
        WorkerLevelFailure(detail='x'),
        ResultProduced(),
        ErrorProduced(
            error=TaskError(error_code='X', message='x', data={}),
        ),
        ResultUndecodable(detail='x'),
    )

    @pytest.mark.parametrize('report', _REPORTS, ids=lambda r: type(r).__name__)
    def test_every_report_shape_classifies(self, report: ChildReport) -> None:
        """No variant falls through; the checker rejects an omitted case."""
        assert classify(report, _context()) is not None

    @pytest.mark.parametrize('report', _REPORTS, ids=lambda r: type(r).__name__)
    def test_classification_is_repeatable(self, report: ChildReport) -> None:
        """Same input, same decision — no clock, no counter, no read."""
        assert classify(report, _context()) == classify(report, _context())

    def test_decisions_are_frozen(self) -> None:
        decision = classify(ResultProduced(), _context())
        with pytest.raises(dataclasses.FrozenInstanceError):
            setattr(decision, 'command', None)

    def test_this_module_uses_no_database_fixture(self) -> None:
        """The purity claim, checked rather than asserted in prose.

        A fixture creeping in here would mean a decision had started
        depending on stored state, which is the split this layer exists to
        make impossible. Read off this file's own AST so it cannot be
        satisfied by intent.
        """
        database_fixtures = {
            'broker',
            'engine',
            'session',
            'session_factory',
            'complete_task',
            'get_task_status',
            'app',
        }
        tree = ast.parse(Path(__file__).read_text(encoding='utf-8'))
        used: set[str] = set()
        for node in ast.walk(tree):
            if not isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef):
                continue
            if not node.name.startswith('test_'):
                continue
            used |= {
                argument.arg
                for argument in node.args.args
                if argument.arg in database_fixtures
            }
        assert not used, f'classification tests took database fixtures: {sorted(used)}'

    def test_context_carries_no_field_that_could_contradict_a_command(
        self,
    ) -> None:
        """Field-first: every context field is a fact the worker holds.

        A status, a disposition, or a target here would be a second opinion
        the variant already implies — the shape that has twice reached review
        before being caught.
        """
        permitted = {
            'task_id',
            'worker_id',
            'claimed_at',
            'is_workflow_task',
            'queue_name',
            'result_json',
            'orphan_self_heal_enabled',
        }
        fields = {f.name for f in dataclasses.fields(_context())}
        assert fields == permitted
