"""The failed-attempt write through the core: the request it sends and the
outcome it builds from each record of ``horsies._core``.

The records have no Python constructor, so each test stands in a record with
a ``MagicMock`` of its class: a class pattern matches it, and its fields are
set as attributes.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

import horsies._core as core
from horsies.core.lifecycle.fences import OwnedClaim
from horsies.core.lifecycle.operations import TerminalizationKind
from horsies.core.lifecycle.outcomes import (
    AlreadyApplied,
    CapacityWoken,
    ClaimReleased,
    EvidenceLoggedByCore,
    FailureApplied,
    GuardKind,
    LostClaim,
    ObservedClaim,
    ObservedTaskState,
    OutcomeDecodeError,
    PhaseTwoOwed,
    PhaseTwoStep,
    RetryNotRequested,
    RetryPastDeadline,
    RetryScheduled,
    SourceStateConflict,
    TaskAbsent,
)
from horsies.core.types.status import TaskStatus
from horsies.core.worker.failed_attempt import (
    AttemptFailure,
    AttemptFailureKind,
    FinishFailedAttempt,
    NoRetry,
    RetryAfter,
    finish_ended_attempt_in_core,
    finish_failed_attempt_in_core,
)
from horsies.core.worker.shared_stop import RetryPlacement

pytestmark = [pytest.mark.unit]

TASK_ID = '0b4d8a52-7d0e-4a0e-9d11-6b2f1f7c9e01'
CLAIMED_AT = datetime(2026, 11, 11, 11, 0, tzinfo=timezone.utc)
NOW = datetime(2026, 11, 11, 12, 0, tzinfo=timezone.utc)
LATER = datetime(2026, 11, 11, 12, 5, tzinfo=timezone.utc)
WORKER_ID = 'worker-1'
OBSERVED = ObservedTaskState(status=TaskStatus.RUNNING, worker_id=WORKER_ID, claimed_at=CLAIMED_AT)


def _record(record_class: type, **fields: object) -> MagicMock:
    """A stand-in for a core record of ``record_class``, with the task id and
    the observed row of ``OBSERVED`` unless ``fields`` names them."""
    record = MagicMock(spec=record_class)
    defaults: dict[str, object] = {
        'task_id': uuid.UUID(TASK_ID),
        'observed_status': 'RUNNING',
        'observed_worker_id': WORKER_ID,
        'observed_claimed_at': CLAIMED_AT,
    }
    for name, value in (defaults | fields).items():
        setattr(record, name, value)
    return record


def _command(retry: NoRetry | RetryAfter = NoRetry()) -> FinishFailedAttempt:
    return FinishFailedAttempt(
        task_id=TASK_ID,
        fence=OwnedClaim(worker_id=WORKER_ID, claimed_at=CLAIMED_AT),
        retry=retry,
        result_json='{"err": 1}',
        error_code='E1',
        failed_reason='boom',
        attempt=AttemptFailure(
            kind=AttemptFailureKind.FAILED,
            error_message='boom',
            failed_reason='boom',
        ),
    )


def _handle(answer: object) -> MagicMock:
    handle = MagicMock()
    handle.finish_failed_attempt_async = AsyncMock(return_value=answer)
    handle.finish_ended_attempt_async = AsyncMock(return_value=answer)
    return handle


async def _failed(answer: object, command: FinishFailedAttempt | None = None) -> object:
    with patch('horsies.core.worker.failed_attempt.core.FinishFailedAttemptRequest'):
        return await finish_failed_attempt_in_core(_handle(answer), command or _command())


class TestTheRequest:
    @pytest.mark.asyncio
    async def test_a_retry_request_carries_its_count_delay_and_placement(self) -> None:
        handle = _handle(_record(core.AttemptTaskAbsent))
        command = _command(RetryAfter(retry_count=2, delay_s=30.0, placement=RetryPlacement.ISOLATED))

        with (
            patch('horsies.core.worker.failed_attempt.core.FinishFailedAttemptRequest') as request,
            patch('horsies.core.worker.failed_attempt.core.RetryAfter') as retry_after,
        ):
            _ = await finish_failed_attempt_in_core(handle, command)

        retry_after.assert_called_once_with(
            expected_retry_count=2, delay_s=30.0, placement='ISOLATED',
        )
        request.assert_called_once_with(
            task_id=uuid.UUID(TASK_ID),
            worker_id=WORKER_ID,
            claimed_at=CLAIMED_AT,
            retry=retry_after.return_value,
            result_json='{"err": 1}',
            error_code='E1',
            failed_reason='boom',
            attempt_outcome='FAILED',
            attempt_error_message='boom',
            attempt_failed_reason='boom',
        )
        handle.finish_failed_attempt_async.assert_awaited_once_with(request.return_value)


class TestTheOutcome:
    @pytest.mark.asyncio
    async def test_an_applied_failure_of_a_workflow_task_owes_phase_2(self) -> None:
        owed = _record(
            core.PhaseTwoOwedRecord,
            task_name='t',
            queue_name='q',
            step='SUBWORKFLOW_REACHABLE',
        )
        refusal = _record(core.RetryPastDeadline, next_retry_at=LATER, good_until=NOW)

        outcome = await _failed(
            _record(core.AttemptFailureApplied, terminal_at=NOW, retry=refusal, after=owed),
        )

        assert outcome == FailureApplied(
            task_id=TASK_ID,
            terminal_at=NOW,
            observed=OBSERVED,
            retry=RetryPastDeadline(next_retry_at=LATER, good_until=NOW),
            after=PhaseTwoOwed(
                task_name='t', queue_name='q', step=PhaseTwoStep.SUBWORKFLOW_REACHABLE,
            ),
        )

    @pytest.mark.asyncio
    async def test_an_applied_failure_of_a_plain_task_woke_its_queue(self) -> None:
        outcome = await _failed(
            _record(
                core.AttemptFailureApplied,
                terminal_at=NOW,
                retry=_record(core.RetryNotRequested),
                after=_record(core.CapacityWoken),
            ),
        )

        assert isinstance(outcome, FailureApplied)
        assert outcome.retry == RetryNotRequested()
        assert outcome.after == CapacityWoken()

    @pytest.mark.asyncio
    async def test_a_scheduled_retry_keeps_its_count_time_and_queue(self) -> None:
        outcome = await _failed(
            _record(
                core.AttemptRetryScheduled,
                retry_count=1,
                next_retry_at=LATER,
                queue_name='q',
            ),
        )

        assert outcome == RetryScheduled(
            task_id=TASK_ID,
            retry_count=1,
            next_retry_at=LATER,
            queue_name='q',
            observed=OBSERVED,
        )

    @pytest.mark.asyncio
    async def test_a_replay_keeps_its_kind(self) -> None:
        outcome = await _failed(
            _record(core.AttemptAlreadyApplied, terminal_at=NOW, kind='FAIL_RUNNING'),
        )

        assert outcome == AlreadyApplied(
            task_id=TASK_ID,
            ordinality=None,
            terminal_at=NOW,
            kind=TerminalizationKind.FAIL_RUNNING,
            observed=OBSERVED,
        )

    @pytest.mark.asyncio
    async def test_a_lost_claim_and_an_absent_task(self) -> None:
        assert await _failed(_record(core.AttemptLostClaim)) == LostClaim(
            task_id=TASK_ID, ordinality=None, observed=OBSERVED,
        )
        assert await _failed(_record(core.AttemptTaskAbsent)) == TaskAbsent(
            task_id=TASK_ID, ordinality=None,
        )

    @pytest.mark.asyncio
    async def test_a_claim_guard_keeps_the_claim_evidence(self) -> None:
        outcome = await _failed(_record(core.AttemptSourceStateConflict, guard='CLAIM'))

        assert outcome == SourceStateConflict(
            task_id=TASK_ID,
            ordinality=None,
            observed=OBSERVED,
            evidence=ObservedClaim(worker_id=WORKER_ID, claimed_at=CLAIMED_AT),
        )

    @pytest.mark.asyncio
    async def test_another_guard_names_the_guard_the_core_logged(self) -> None:
        outcome = await _failed(_record(core.AttemptSourceStateConflict, guard='RETRY_COUNT'))

        assert isinstance(outcome, SourceStateConflict)
        assert outcome.evidence == EvidenceLoggedByCore(guard=GuardKind.RETRY_COUNT)

    @pytest.mark.asyncio
    async def test_a_failed_statement_raises_with_its_classification(self) -> None:
        cause = core.CoreConnectionError('finish_failed_attempt', 'connection lost', True)

        with pytest.raises(core.CoreConnectionError) as raised:
            _ = await _failed(
                _record(
                    core.AttemptWriteFailed,
                    message='connection lost',
                    retryable=True,
                    exception=cause,
                ),
            )

        assert raised.value is cause

    @pytest.mark.parametrize(
        'record',
        [
            _record(core.AttemptLostClaim, observed_status='WAITING'),
            _record(core.AttemptSourceStateConflict, guard='NO_SUCH_GUARD'),
            _record(core.AttemptAlreadyApplied, terminal_at=NOW, kind='NO_SUCH_KIND'),
        ],
        ids=['status', 'guard', 'kind'],
    )
    @pytest.mark.asyncio
    async def test_a_text_outside_the_catalogs_is_refused(self, record: MagicMock) -> None:
        with pytest.raises(OutcomeDecodeError):
            _ = await _failed(record)


class TestTheEndedAttempt:
    @pytest.mark.asyncio
    async def test_a_claimed_row_is_released(self) -> None:
        handle = _handle(
            _record(core.AttemptClaimReleased, observed_status='CLAIMED', queue_name='q'),
        )

        with patch('horsies.core.worker.failed_attempt.core.FinishFailedAttemptRequest'):
            outcome = await finish_ended_attempt_in_core(handle, _command())

        assert outcome == ClaimReleased(
            task_id=TASK_ID,
            queue_name='q',
            observed=ObservedTaskState(
                status=TaskStatus.CLAIMED, worker_id=WORKER_ID, claimed_at=CLAIMED_AT,
            ),
        )
        handle.finish_ended_attempt_async.assert_awaited_once()
        handle.finish_failed_attempt_async.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_a_running_row_gets_the_failed_attempt_outcomes(self) -> None:
        handle = _handle(_record(core.AttemptLostClaim))

        with patch('horsies.core.worker.failed_attempt.core.FinishFailedAttemptRequest'):
            outcome = await finish_ended_attempt_in_core(handle, _command())

        assert outcome == LostClaim(task_id=TASK_ID, ordinality=None, observed=OBSERVED)
