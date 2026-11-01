"""The workflow recovery limit: classification, outcomes, and the error text.

The real writes are in tests/integration/test_workflow_recovery_limit.py. An
action is an input of `_run_recovery_candidate`, so a raising action patches
nothing.
"""

# pyright: reportPrivateUsage=false

from __future__ import annotations

from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import psycopg.errors
import pytest
from pydantic import ValidationError
from sqlalchemy.dialects.postgresql import psycopg as sa_psycopg
from sqlalchemy.exc import (
    DBAPIError,
    PendingRollbackError,
    ProgrammingError,
    TimeoutError as SATimeoutError,
)

from horsies.core.codec.json_io import SerializationError, dumps_json, loads_json
from horsies.core.codec.json_value import StrictJsonError
from horsies.core.codec.typed import decode_task_error, encode_task_error
from horsies.core.models.recovery import (
    DEFAULT_WORKFLOW_RECOVERY_FAILURE_LIMIT,
    RecoveryConfig,
)
from horsies.core.models.tasks import OperationalErrorCode, TaskError
from horsies.core.types.result import Err, Ok, Result, is_ok
from horsies.core.workflows import recovery
from horsies.core.workflows.recovery import (
    RecoveryCaseMetrics,
    RecoveryReport,
    _GlobalCandidateOutcome,
    _recovered_count,
    _run_recovery_candidate,
)
from horsies.core.workflows.recovery_limit import (
    MAX_EXCEPTION_TYPE_LENGTH,
    MAX_RAISE_TEXT_LENGTH,
    CountFailures,
    LogFailuresOnly,
    RaiseRecord,
    RecoveryCandidateOutcome,
    RecoveryCase,
    RecoveryLimitOutcome,
    RecoveryRaise,
    RecoveryRaiseClass,
    RecoveryWrite,
    RecoveryWriteFailed,
    _limit_json,
    classify_recovery_raise,
    recovery_limit_error,
    recovery_limit_payloads,
    report_recovery_limit,
)

pytestmark = pytest.mark.unit

_WORKFLOW_ID = '00000000-0000-0000-0000-000000000001'
_COUNTING = CountFailures(pass_id='pass', limit=3)


def _wrapped(sqlstate: str) -> Exception:
    """A psycopg error for `sqlstate`, wrapped as SQLAlchemy wraps it."""
    error_class = psycopg.errors.lookup(sqlstate)
    return DBAPIError.instance(
        'SELECT 1',
        {},
        error_class('refused'),
        psycopg.errors.Error,
        dialect=sa_psycopg.dialect(),
    )


def _failure(detail: str = 'refused') -> RecoveryRaise:
    return RecoveryRaise(
        workflow_id=_WORKFLOW_ID,
        task_index=4,
        case=RecoveryCase.READY_NOT_ENQUEUED,
        exception_type='DataError',
        detail=detail,
    )


class TestClassifyRecoveryRaise:
    @pytest.mark.parametrize(
        'sqlstate',
        ['40P01', '40001', '55P03', '57014', '57P01', '57P03', '53300', '53100'],
    )
    def test_an_operational_class_is_transient(self, sqlstate: str) -> None:
        assert classify_recovery_raise(_wrapped(sqlstate)) is RecoveryRaiseClass.TRANSIENT

    def test_a_pool_timeout_is_transient(self) -> None:
        assert (
            classify_recovery_raise(SATimeoutError('pool'))
            is RecoveryRaiseClass.TRANSIENT
        )

    def test_the_raise_after_a_connection_loss_is_transient(self) -> None:
        """The shared helper calls this error permanent. Here it means that
        an earlier candidate lost the session, not that this one is poison."""
        assert (
            classify_recovery_raise(PendingRollbackError('rollback first'))
            is RecoveryRaiseClass.TRANSIENT
        )

    @pytest.mark.parametrize(
        'sqlstate',
        ['25006', '26000', '42P01', '42703', '23505', '22P02', '22007', 'P0001', 'XX000'],
    )
    def test_any_other_database_class_is_persistent(self, sqlstate: str) -> None:
        assert (
            classify_recovery_raise(_wrapped(sqlstate))
            is RecoveryRaiseClass.PERSISTENT
        )

    @pytest.mark.parametrize(
        'error',
        [
            SerializationError('does not serialize'),
            AssertionError('a defect'),
            AttributeError('a defect'),
            RuntimeError('a defect'),
            ValueError('a defect'),
        ],
        ids=lambda error: type(error).__name__,
    )
    def test_a_defect_is_persistent(self, error: Exception) -> None:
        assert classify_recovery_raise(error) is RecoveryRaiseClass.PERSISTENT


class TestRecoveryRaise:
    def test_text_is_bounded_and_escaped(self) -> None:
        text = 'x' * MAX_RAISE_TEXT_LENGTH + 'cut'
        record = RecoveryRaise.of(
            RuntimeError(text),
            workflow_id=_WORKFLOW_ID,
            task_index=None,
            case=RecoveryCase.TERMINAL_WORKFLOW_RUNNING,
        )
        assert record.detail == 'x' * MAX_RAISE_TEXT_LENGTH
        assert record.exception_type == 'RuntimeError'

    def test_a_long_class_name_fits_its_column(self) -> None:
        long_type = type('L' * (MAX_EXCEPTION_TYPE_LENGTH + 9), (Exception,), {})
        record = RecoveryRaise.of(
            long_type('x'),
            workflow_id=_WORKFLOW_ID,
            task_index=None,
            case=RecoveryCase.TERMINAL_WORKFLOW_RUNNING,
        )
        assert len(record.exception_type) == MAX_EXCEPTION_TYPE_LENGTH


class TestRecoveryLimitError:
    def test_it_carries_subject_and_cause(self) -> None:
        error = recovery_limit_error(_failure(), 25)

        assert error.error_code is OperationalErrorCode.WORKFLOW_RECOVERY_LIMIT_REACHED
        assert error.data == {
            'workflow_id': _WORKFLOW_ID,
            'case': 'ready_not_enqueued',
            'task_index': 4,
            'failed_passes': 25,
            'exception_type': 'DataError',
        }
        assert error.message is not None
        assert 'refused' in error.message

    def test_a_lone_surrogate_nul_and_letters_survive_a_round_trip(self) -> None:
        """The error reports a failure, so its text must not fail the same
        way. A lone surrogate is escaped. All other text stays."""
        record = RecoveryRaise.of(
            RuntimeError('bad \ud800 Müller 日本 \x00 end'),
            workflow_id=_WORKFLOW_ID,
            task_index=0,
            case=RecoveryCase.READY_NOT_ENQUEUED,
        )
        error = recovery_limit_error(record, 3)

        serialized = dumps_json(encode_task_error(error))

        assert is_ok(serialized)
        parsed = loads_json(serialized.ok_value)
        assert is_ok(parsed)
        decoded = decode_task_error(parsed.ok_value)
        assert decoded.error_code is OperationalErrorCode.WORKFLOW_RECOVERY_LIMIT_REACHED
        assert decoded.message == error.message
        assert decoded.message is not None
        assert 'Müller 日本' in decoded.message
        assert '\\ud800' in decoded.message


def _session() -> AsyncMock:
    """A session with a real `info` dict: the propagation queue lives there."""
    session = AsyncMock()
    session.info = {}
    return session


def _raising(error: Exception) -> Any:
    async def action() -> bool:
        raise error

    return action


async def _returning(value: bool) -> bool:
    return value


@pytest.mark.asyncio
class TestRunRecoveryCandidate:
    """The session is a mock, so the SAVEPOINT is not exercised here."""

    async def test_an_action_that_applied(self) -> None:
        metrics = RecoveryCaseMetrics()
        outcome = await _run_recovery_candidate(
            _session(),
            case=RecoveryCase.READY_NOT_ENQUEUED,
            action=lambda: _returning(True),
            metrics=metrics,
            failure_policy=LogFailuresOnly(),
        )
        assert outcome is RecoveryCandidateOutcome.APPLIED
        assert metrics == RecoveryCaseMetrics()

    async def test_an_action_that_refused(self) -> None:
        metrics = RecoveryCaseMetrics()
        outcome = await _run_recovery_candidate(
            _session(),
            case=RecoveryCase.READY_NOT_ENQUEUED,
            action=lambda: _returning(False),
            metrics=metrics,
            failure_policy=LogFailuresOnly(),
        )
        assert outcome is RecoveryCandidateOutcome.REFUSED
        assert metrics.refusals == 1
        assert metrics.errors == 0

    async def test_a_log_only_pass_writes_nothing(self) -> None:
        metrics = RecoveryCaseMetrics()
        with patch.object(recovery, 'record_recovery_raise', AsyncMock()) as record:
            outcome = await _run_recovery_candidate(
                _session(),
                case=RecoveryCase.READY_NOT_ENQUEUED,
                workflow_id=_WORKFLOW_ID,
                action=_raising(RuntimeError('a defect')),
                metrics=metrics,
                failure_policy=LogFailuresOnly(),
            )
        assert outcome is RecoveryCandidateOutcome.NOT_COUNTED
        record.assert_not_awaited()
        assert metrics.errors == 1
        assert metrics.failures_counted == 0

    async def test_a_transient_raise_writes_nothing(self) -> None:
        metrics = RecoveryCaseMetrics()
        with patch.object(recovery, 'record_recovery_raise', AsyncMock()) as record:
            outcome = await _run_recovery_candidate(
                _session(),
                case=RecoveryCase.READY_NOT_ENQUEUED,
                workflow_id=_WORKFLOW_ID,
                action=_raising(_wrapped('57P01')),
                metrics=metrics,
                failure_policy=_COUNTING,
            )
        assert outcome is RecoveryCandidateOutcome.NOT_COUNTED
        record.assert_not_awaited()
        assert metrics.errors == 1

    async def test_a_persistent_raise_is_recorded(self) -> None:
        session = _session()
        with patch.object(
            recovery,
            'record_recovery_raise',
            AsyncMock(return_value=RecoveryCandidateOutcome.COUNTED),
        ) as record:
            outcome = await _run_recovery_candidate(
                session,
                case=RecoveryCase.READY_NOT_ENQUEUED,
                workflow_id=_WORKFLOW_ID,
                task_index=2,
                action=_raising(AssertionError('a defect')),
                failure_policy=_COUNTING,
            )
        assert outcome is RecoveryCandidateOutcome.COUNTED
        record.assert_awaited_once()
        assert record.await_args is not None
        recorded_session, policy, failure = record.await_args.args
        assert recorded_session is session
        assert policy is _COUNTING
        assert failure == RecoveryRaise(
            workflow_id=_WORKFLOW_ID,
            task_index=2,
            case=RecoveryCase.READY_NOT_ENQUEUED,
            exception_type='AssertionError',
            detail='a defect',
        )

    @pytest.mark.parametrize(
        ('record', 'counted', 'limit_reached'),
        [
            (RecoveryCandidateOutcome.NOT_COUNTED, 0, 0),
            (RecoveryCandidateOutcome.COUNTED, 1, 0),
            (RecoveryCandidateOutcome.LIMIT_REACHED, 1, 1),
        ],
    )
    async def test_metrics_follow_the_record(
        self,
        record: RaiseRecord,
        counted: int,
        limit_reached: int,
    ) -> None:
        metrics = RecoveryCaseMetrics()
        with patch.object(
            recovery, 'record_recovery_raise', AsyncMock(return_value=record),
        ):
            _ = await _run_recovery_candidate(
                _session(),
                case=RecoveryCase.READY_NOT_ENQUEUED,
                workflow_id=_WORKFLOW_ID,
                action=_raising(RuntimeError('a defect')),
                metrics=metrics,
                failure_policy=_COUNTING,
            )
        assert metrics.failures_counted == counted
        assert metrics.limit_reached == limit_reached
        assert metrics.errors == 1

    async def test_counting_a_raise_with_no_workflow_is_a_defect(self) -> None:
        with pytest.raises(AssertionError, match='no workflow id'):
            _ = await _run_recovery_candidate(
                _session(),
                case=RecoveryCase.QUEUED_PARENT_PROPAGATION,
                action=_raising(RuntimeError('a defect')),
                failure_policy=_COUNTING,
            )


class TestReportRecoveryLimit:
    @pytest.mark.parametrize(
        ('result', 'expected'),
        [
            (
                Ok(RecoveryLimitOutcome.WORKFLOW_FAILED),
                RecoveryCandidateOutcome.LIMIT_REACHED,
            ),
            (
                Ok(RecoveryLimitOutcome.LIVE_NODES_FINISH),
                RecoveryCandidateOutcome.LIMIT_REACHED,
            ),
            (Ok(RecoveryLimitOutcome.REFUSED), RecoveryCandidateOutcome.COUNTED),
            (
                Err(
                    RecoveryWriteFailed(
                        write=RecoveryWrite.LIMIT,
                        workflow_id=_WORKFLOW_ID,
                        exception_type='ProgrammingError',
                        detail='refused',
                    ),
                ),
                RecoveryCandidateOutcome.COUNTED,
            ),
        ],
        ids=['failed', 'live_nodes', 'refused', 'write_failed'],
    )
    def test_each_outcome(
        self,
        result: Result[RecoveryLimitOutcome, RecoveryWriteFailed],
        expected: RecoveryCandidateOutcome,
    ) -> None:
        assert report_recovery_limit(result, _failure(), 25) is expected


class TestRecoveredCount:
    @pytest.mark.parametrize('outcome', list(RecoveryCandidateOutcome))
    def test_only_an_applied_candidate_adds_one(
        self, outcome: RecoveryCandidateOutcome,
    ) -> None:
        expected = 1 if outcome is RecoveryCandidateOutcome.APPLIED else 0
        assert _recovered_count(outcome) == expected


class TestHealth:
    def test_the_report_publishes_the_limit_fields(self) -> None:
        report = RecoveryReport(over_failure_limit=2)
        report.metrics['case_1'].failures_counted = 3
        report.metrics['case_1'].limit_reached = 1

        health = report.health()

        assert health['over_failure_limit'] == 2
        cases = health['cases']
        assert isinstance(cases, dict)
        assert cases['case_1']['failures_counted'] == 3
        assert cases['case_1']['limit_reached'] == 1


class TestRecoveryLimitPayloads:
    def test_both_forms_hold_the_same_error(self) -> None:
        error = recovery_limit_error(_failure(), 25)

        payloads = recovery_limit_payloads(error)

        workflow_error = loads_json(payloads.workflow_error_json)
        node_result = loads_json(payloads.node_result_json)
        assert is_ok(workflow_error)
        assert is_ok(node_result)
        envelope = node_result.ok_value
        assert isinstance(envelope, dict)
        assert sorted(envelope) == ['__h_task_result__', 'err', 'ok']
        assert envelope['err'] == workflow_error.ok_value
        assert (
            decode_task_error(workflow_error.ok_value).error_code
            is OperationalErrorCode.WORKFLOW_RECOVERY_LIMIT_REACHED
        )

    def test_an_error_that_does_not_encode_raises(self) -> None:
        """A defect is loud. It does not store a fallback text."""
        error = TaskError(
            error_code=OperationalErrorCode.WORKFLOW_RECOVERY_LIMIT_REACHED,
            message='unescaped \ud800',
        )
        with pytest.raises(StrictJsonError):
            recovery_limit_payloads(error)

    def test_a_value_that_does_not_serialize_raises(self) -> None:
        with pytest.raises(SerializationError):
            _limit_json({'text': 'unescaped \ud800'})


class TestTopLevelErrors:
    """The published count adds the pass-level count and every case."""

    def test_it_adds_the_stored_count_and_each_case(self) -> None:
        report = RecoveryReport(errors=1)
        for number, metrics in enumerate(report.metrics.values(), start=1):
            metrics.errors = number

        assert report.health()['errors'] == 1 + 1 + 2 + 3 + 4 + 5 + 6

    def test_a_case_error_alone_is_published(self) -> None:
        report = RecoveryReport()
        report.metrics['case_1_6'].errors = 1

        assert report.health()['errors'] == 1


def _session_factory(audit_row: Any) -> MagicMock:
    session = AsyncMock()
    session.__aenter__ = AsyncMock(return_value=session)
    session.__aexit__ = AsyncMock(return_value=None)
    result = MagicMock()
    result.one_or_none = MagicMock(return_value=audit_row)
    session.execute = AsyncMock(return_value=result)
    return MagicMock(return_value=session)


def _audit_row(*, completion: bool, orphan: bool) -> SimpleNamespace:
    return SimpleNamespace(
        scanned_count=1,
        completion_ids=['completion'] if completion else [],
        orphan_ids=['orphan'] if orphan else [],
        orphan_names=['name'] if orphan else [],
        claim_token='token',
    )


@pytest.mark.asyncio
class TestEachFailureCountsOnce:
    """Each step that adds to a case must not add to the pass-level count."""

    @staticmethod
    async def _health(audit_row: Any, **patches: Any) -> dict[str, object]:
        report = RecoveryReport()
        with (
            patch.object(recovery, 'recover_stuck_workflows', AsyncMock(return_value=0)),
            patch.multiple(recovery, **patches),
        ):
            await recovery._recover_stuck_workflows_global(
                _session_factory(audit_row),
                None,
                report,
                _COUNTING,
            )
        return report.health()

    async def test_a_failed_renewal_for_a_completion(self) -> None:
        health = await self._health(
            _audit_row(completion=True, orphan=False),
            _renew_global_claim=AsyncMock(side_effect=RuntimeError('lost')),
        )
        assert health['errors'] == 1

    async def test_a_failed_renewal_for_an_orphan(self) -> None:
        health = await self._health(
            _audit_row(completion=False, orphan=True),
            _renew_global_claim=AsyncMock(side_effect=RuntimeError('lost')),
            _release_global_claim=AsyncMock(),
        )
        assert health['errors'] == 1

    async def test_a_failed_orphan_candidate(self) -> None:
        health = await self._health(
            _audit_row(completion=False, orphan=True),
            _renew_global_claim=AsyncMock(return_value=True),
            _release_global_claim=AsyncMock(),
            _recover_global_orphan_candidate=AsyncMock(
                return_value=_GlobalCandidateOutcome.ERROR,
            ),
        )
        assert health['errors'] == 1

    async def test_a_failed_release_counts_in_each_case_that_it_serves(self) -> None:
        health = await self._health(
            _audit_row(completion=False, orphan=False),
            _release_global_claim=AsyncMock(side_effect=RuntimeError('lost')),
        )
        assert health['errors'] == 2

    async def test_a_failed_audit_counts_in_each_case_that_it_serves(self) -> None:
        session = AsyncMock()
        session.__aenter__ = AsyncMock(return_value=session)
        session.__aexit__ = AsyncMock(return_value=None)
        session.execute = AsyncMock(side_effect=RuntimeError('audit failed'))
        with (
            patch.object(recovery, 'recover_stuck_workflows', AsyncMock(return_value=0)),
            pytest.raises(recovery.RecoveryPassFailure) as raised,
        ):
            await recovery.recover_stuck_workflows_global(
                MagicMock(return_value=session),
            )
        assert raised.value.health()['errors'] == 2
        assert str(raised.value.error) == 'audit failed'

    async def test_a_failure_that_no_case_counted_is_pass_level(self) -> None:
        with (
            patch.object(
                recovery,
                '_recover_stuck_workflows_global',
                AsyncMock(side_effect=RuntimeError('commit failed')),
            ),
            pytest.raises(recovery.RecoveryPassFailure) as raised,
        ):
            await recovery.recover_stuck_workflows_global(MagicMock())
        assert raised.value.health()['errors'] == 1
        assert raised.value.report.errors == 1


class TestConfig:
    def test_the_default_matches_the_named_constant(self) -> None:
        assert (
            RecoveryConfig().workflow_recovery_failure_limit
            == DEFAULT_WORKFLOW_RECOVERY_FAILURE_LIMIT
            == 25
        )

    @pytest.mark.parametrize('limit', [3, 1_000])
    def test_each_end_of_the_range_is_accepted(self, limit: int) -> None:
        config = RecoveryConfig(workflow_recovery_failure_limit=limit)
        assert config.workflow_recovery_failure_limit == limit

    @pytest.mark.parametrize('limit', [2, 1_001])
    def test_a_value_out_of_range_is_refused(self, limit: int) -> None:
        with pytest.raises(ValidationError):
            RecoveryConfig(workflow_recovery_failure_limit=limit)


@pytest.mark.asyncio
async def test_each_pass_makes_a_new_pass_id() -> None:
    """The integration suite shows that one id serves all cases of a pass."""
    seen: list[CountFailures] = []

    async def _capture(
        _factory: Any,
        _broker: Any,
        report: RecoveryReport,
        policy: Any,
    ) -> RecoveryReport:
        seen.append(policy)
        return report

    with (
        patch.object(recovery, '_recover_stuck_workflows_global', _capture),
        patch.object(
            recovery, 'count_over_failure_limit', AsyncMock(return_value=0),
        ),
    ):
        await recovery.recover_stuck_workflows_global(MagicMock(), failure_limit=7)
        await recovery.recover_stuck_workflows_global(MagicMock(), failure_limit=7)

    assert [policy.limit for policy in seen] == [7, 7]
    assert seen[0].pass_id != seen[1].pass_id


def test_a_programming_error_keeps_its_class_after_wrapping() -> None:
    """The class 42 rule rests on this wrapping."""
    assert isinstance(_wrapped('42P01'), ProgrammingError)
