# pyright: reportPrivateUsage=false
"""The reaper's workflow calls through the core: the call each makes and the
answer it builds from each record of ``horsies._core``.

The records have no Python constructor, so each test stands in a record with
a ``MagicMock`` of its class: a class pattern matches it, and its fields are
set as attributes.
"""

from __future__ import annotations

import time
from datetime import timedelta
from unittest.mock import AsyncMock, MagicMock

import pytest

import horsies._core as core
from horsies.core.worker.reaper import ReaperMixin
from horsies.core.workflows.lifecycle import PAUSED_EXPIRY_BATCH_SIZE

pytestmark = [pytest.mark.unit]


class _Reaper(ReaperMixin):
    """The reaper mixin alone."""


def _record(record_class: type, **fields: object) -> MagicMock:
    record = MagicMock(spec=record_class)
    for name, value in fields.items():
        setattr(record, name, value)
    return record


def _broker(method: str, answer: object) -> MagicMock:
    broker = MagicMock()
    setattr(broker._core_handle, method, AsyncMock(return_value=answer))
    return broker


@pytest.mark.asyncio
async def test_the_paused_expiry_sends_the_age_in_microseconds_and_counts() -> None:
    broker = _broker('expire_paused_workflows_async', _record(core.PausedExpired, expired=3))

    expired = await _Reaper()._expire_paused_workflows(
        broker, older_than=timedelta(hours=1, microseconds=7),
    )

    assert expired == 3
    broker._core_handle.expire_paused_workflows_async.assert_awaited_once_with(
        3_600_000_007, PAUSED_EXPIRY_BATCH_SIZE,
    )


@pytest.mark.asyncio
async def test_a_failed_paused_expiry_raises_with_its_cause() -> None:
    cause = core.CoreConnectionError('expire_paused_workflows', 'connection refused', True)
    broker = _broker(
        'expire_paused_workflows_async',
        _record(
            core.PausedExpiryFailed,
            message='connection refused',
            retryable=True,
            exception=cause,
        ),
    )

    with pytest.raises(core.CoreConnectionError) as raised:
        await _Reaper()._expire_paused_workflows(broker, older_than=timedelta(hours=1))

    assert raised.value is cause


@pytest.mark.asyncio
async def test_the_retention_delete_sends_the_time_left_and_counts() -> None:
    broker = _broker('delete_expired_workflows_async', _record(core.WorkflowsDeleted, deleted=5))

    deleted = await _Reaper()._delete_expired_workflows(
        broker,
        retention_hours=48,
        batch_size=500,
        deadline_monotonic=time.monotonic() + 30.0,
    )

    assert deleted == 5
    assert broker._core_handle.delete_expired_workflows_async.await_args is not None
    hours, batch, budget_ms = broker._core_handle.delete_expired_workflows_async.await_args.args
    assert (hours, batch) == (48, 500)
    assert 0 < budget_ms <= 30_000


@pytest.mark.asyncio
async def test_a_passed_deadline_sends_a_budget_of_zero() -> None:
    broker = _broker('delete_expired_workflows_async', _record(core.WorkflowsDeleted, deleted=0))

    _ = await _Reaper()._delete_expired_workflows(
        broker,
        retention_hours=48,
        batch_size=500,
        deadline_monotonic=time.monotonic() - 5.0,
    )

    assert broker._core_handle.delete_expired_workflows_async.await_args is not None
    assert broker._core_handle.delete_expired_workflows_async.await_args.args[2] == 0


@pytest.mark.asyncio
async def test_a_failed_retention_delete_raises_its_cause_and_logs_the_count_before_it(
    caplog: pytest.LogCaptureFixture,
) -> None:
    cause = core.CoreServerError(
        'delete_expired_workflows', '57014', 'canceling statement due to statement timeout', True,
    )
    broker = _broker(
        'delete_expired_workflows_async',
        _record(
            core.WorkflowDeleteFailed,
            deleted_before=4,
            message='statement timeout',
            retryable=True,
            exception=cause,
        ),
    )

    with caplog.at_level('WARNING'), pytest.raises(core.CoreServerError) as raised:
        await _Reaper()._delete_expired_workflows(
            broker,
            retention_hours=48,
            batch_size=500,
            deadline_monotonic=time.monotonic() + 30.0,
        )

    assert raised.value is cause
    assert 'deleted 4 workflow(s) before the failure' in caplog.text


@pytest.mark.asyncio
async def test_the_phase2_recovery_maps_the_pass_and_folds_pending_absent() -> None:
    broker = _broker(
        'recover_workflow_phase2_async',
        _record(
            core.Phase2RecoveryRecord,
            considered=6,
            applied=2,
            already_applied=1,
            superseded=1,
            pending_absent=1,
            retained=1,
            failed=0,
            quarantined=1,
            raised_counted=0,
            over_attempt_bound=4,
            retained_details=['t1: SOURCE_ABSENT'],
            quarantine_refusals=[],
        ),
    )

    summary = await _Reaper()._recover_phase2(
        broker, grace_ms=30_000, max_rows=100, quarantine_after_attempts=3,
    )

    assert summary.considered == 6
    assert summary.applied == 2
    # The Python pass counted a record gone with its node not terminal as
    # already applied.
    assert summary.already_applied == 2
    assert summary.over_attempt_bound == 4
    assert summary.retained_details == ('t1: SOURCE_ABSENT',)
    assert summary.quarantine_refusals == ()
    broker._core_handle.recover_workflow_phase2_async.assert_awaited_once_with(
        grace_ms=30_000, max_rows=100, quarantine_after_attempts=3,
    )


@pytest.mark.asyncio
async def test_a_failed_phase2_recovery_raises_with_its_cause() -> None:
    cause = core.CoreConnectionError('recover_workflow_phase2', 'discovery failed', True)
    broker = _broker(
        'recover_workflow_phase2_async',
        _record(
            core.Phase2RecoveryFailed,
            message='discovery failed',
            retryable=True,
            exception=cause,
        ),
    )

    with pytest.raises(core.CoreConnectionError) as raised:
        await _Reaper()._recover_phase2(
            broker, grace_ms=0, max_rows=100, quarantine_after_attempts=3,
        )

    assert raised.value is cause


def _case(**fields: int) -> MagicMock:
    defaults = {
        'rows_selected': 0,
        'candidates_returned': 0,
        'duration_ms': 0,
        'refusals': 0,
        'errors': 0,
        'failures_counted': 0,
        'limit_reached': 0,
    }
    return _record(core.RecoveryCaseRecord, **{**defaults, **fields})


def _pass_record(record_class: type, **fields: object) -> MagicMock:
    cases = {
        name: _case() for name in ('case_0', 'case_1', 'case_1_5', 'case_1_6', 'case_4')
    }
    defaults: dict[str, object] = {
        'recovered': 0,
        'pass_errors': 0,
        'over_failure_limit': 0,
        'case_2_3': _case(),
        **cases,
    }
    return _record(record_class, **{**defaults, **fields})


@pytest.mark.asyncio
async def test_the_recovery_pass_builds_the_published_health() -> None:
    broker = _broker(
        'recover_workflows_async',
        _pass_record(
            core.RecoveryPassRecord,
            recovered=3,
            over_failure_limit=1,
            case_2_3=_case(rows_selected=10, candidates_returned=2, errors=1),
            case_4=_case(rows_selected=10, errors=1),
        ),
    )

    report = await _Reaper()._recover_workflows(broker, failure_limit=5)
    health = report.health()

    assert health['recovered'] == 3
    # The audit failure counts in each case it serves, as the Python pass
    # counted it.
    assert health['errors'] == 2
    assert health['over_failure_limit'] == 1
    assert health['completion_candidate_yield'] == 0.2
    broker._core_handle.recover_workflows_async.assert_awaited_once_with(failure_limit=5)


@pytest.mark.asyncio
async def test_a_failed_recovery_pass_raises_with_its_partial_report() -> None:
    from horsies.core.workflows.recovery import RecoveryPassFailure

    report = _pass_record(core.RecoveryPassRecord, pass_errors=1)
    broker = _broker(
        'recover_workflows_async',
        _record(
            core.RecoveryPassFailed,
            report=report,
            message='count failed',
            retryable=True,
            exception=core.CoreConnectionError('recover_workflows', 'count failed', True),
        ),
    )

    with pytest.raises(RecoveryPassFailure) as raised:
        await _Reaper()._recover_workflows(broker, failure_limit=5)

    health = raised.value.health()
    assert health['state'] == 'error'
    assert health['errors'] == 1
    assert health['completion_candidate_yield'] is None
