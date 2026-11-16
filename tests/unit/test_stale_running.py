"""``horsies.core.brokers.stale_running`` and ``decode_stale_attempt_row``."""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from typing import Any

import pytest

from horsies.core.brokers.stale_running import (
    CrashRetry,
    NoCrashRetry,
    StaleCandidate,
    StaleThresholds,
    crash_reason,
    crash_result_json,
    crash_retry_request,
    finish_stale_parameters,
    read_stale_candidate,
)
from horsies.core.lifecycle.outcomes import (
    ObservedRetryCount,
    OutcomeDecodeError,
    RetryNotRequested,
    RetryPastDeadline,
    RetryScheduled,
    SourceStateConflict,
    StaleFailureApplied,
    TaskAbsent,
    decode_stale_attempt_row,
)
from horsies.core.types.result import Ok

pytestmark = [pytest.mark.unit]

TASK_ID = '019a8f3e-0000-7000-8000-000000000001'
STARTED_AT = datetime(2026, 11, 16, 9, 0, tzinfo=timezone.utc)
DB_NOW = datetime(2026, 11, 16, 9, 10, tzinfo=timezone.utc)
THRESHOLDS = StaleThresholds(stale_after_ms=30_000, finalizing_stale_after_ms=60_000)
_POLICY = json.dumps(
    {
        'retry_policy': {
            'max_retries': 3,
            'intervals': [5, 10, 20],
            'backoff_strategy': 'fixed',
            'jitter': False,
            'auto_retry_for': ['WORKER_CRASHED'],
        },
    },
)


def _scan_row(**overrides: Any) -> dict[str, Any]:
    row: dict[str, Any] = {
        'task_id': TASK_ID,
        'retry_count': 0,
        'max_retries': 3,
        'task_options': _POLICY,
        'good_until': None,
        'claimed_by_worker_id': 'w-1',
        'worker_pid': 7,
        'worker_hostname': 'host-a',
        'worker_process_name': 'proc-a',
        'started_at': STARTED_AT,
        'db_now': DB_NOW,
    }
    row.update(overrides)
    return row


def _candidate(**overrides: Any) -> StaleCandidate:
    return read_stale_candidate(_scan_row(**overrides))


class TestCandidate:
    def test_a_scan_row_reads_as_a_candidate(self) -> None:
        assert _candidate() == StaleCandidate(
            task_id=TASK_ID,
            retry_count=0,
            max_retries=3,
            task_options=_POLICY,
            good_until=None,
            claimed_by_worker_id='w-1',
            worker_pid=7,
            worker_hostname='host-a',
            worker_process_name='proc-a',
            started_at=STARTED_AT,
            db_now=DB_NOW,
        )

    def test_a_row_without_a_start_is_a_defect(self) -> None:
        with pytest.raises(AssertionError, match='outside its contract'):
            read_stale_candidate(_scan_row(started_at=None))


class TestCrashRetryRequest:
    def test_a_policy_that_names_the_crash_retries_with_its_delay(self) -> None:
        assert crash_retry_request(_candidate(retry_count=1)) == CrashRetry(
            retry_count=1, delay_s=10.0,
        )

    def test_a_spent_count_does_not_retry(self) -> None:
        assert crash_retry_request(_candidate(retry_count=3)) == NoCrashRetry()

    def test_no_policy_does_not_retry(self) -> None:
        assert crash_retry_request(_candidate(task_options=None)) == NoCrashRetry()

    def test_a_policy_for_other_codes_does_not_retry(self) -> None:
        options = json.dumps(
            {'retry_policy': {'max_retries': 3, 'intervals': [5], 'auto_retry_for': ['TIMEOUT']}},
        )
        assert crash_retry_request(_candidate(task_options=options)) == NoCrashRetry()


class TestFinishParameters:
    def test_a_retry_carries_the_count_and_the_delay(self) -> None:
        parameters = finish_stale_parameters(
            _candidate(), THRESHOLDS, CrashRetry(retry_count=2, delay_s=5.0), '{}',
        )

        assert parameters == {
            'task_id': TASK_ID,
            'stale_after_ms': 30_000,
            'finalizing_stale_after_ms': 60_000,
            'retry_kind': 'RETRY',
            'expected_retry_count': 2,
            'retry_delay_s': 5.0,
            'result': '{}',
            'error_code': 'WORKER_CRASHED',
            'failed_reason': crash_reason(THRESHOLDS),
            'attempt_message': crash_reason(THRESHOLDS),
            'attempt_reason': crash_reason(THRESHOLDS),
        }

    def test_no_retry_leaves_the_retry_slots_empty(self) -> None:
        parameters = finish_stale_parameters(_candidate(), THRESHOLDS, NoCrashRetry(), '{}')

        assert (
            parameters['retry_kind'],
            parameters['expected_retry_count'],
            parameters['retry_delay_s'],
        ) == ('NONE', None, None)

    def test_the_crash_result_names_the_silent_attempt(self) -> None:
        match crash_result_json(_candidate(), THRESHOLDS):
            case Ok(text):
                document = json.loads(text)
            case failed:
                raise AssertionError(failed)

        assert document['err']['message'] == crash_reason(THRESHOLDS)
        assert document['err']['data'] == {
            'stale_threshold_ms': 30_000,
            'stale_threshold_seconds': 30.0,
            'worker_pid': 7,
            'worker_hostname': 'host-a',
            'worker_id': 'w-1',
            'started_at': STARTED_AT.isoformat(),
            'detected_at': DB_NOW.isoformat(),
        }


def _finish_row(**overrides: Any) -> dict[str, Any]:
    """An APPLIED row of a plain task."""
    row: dict[str, Any] = {
        'task_id': TASK_ID,
        'ordinality': None,
        'outcome': 'APPLIED',
        'terminal_at': DB_NOW,
        'terminalization_kind': 'FAIL_STALE',
        'observed_status': 'RUNNING',
        'observed_worker_id': 'w-1',
        'observed_claimed_at': STARTED_AT,
        'guard_kind': None,
        'observed_guard': None,
        'retry_disposition': 'NOT_REQUESTED',
        'retry_count': None,
        'next_retry_at': None,
        'good_until': None,
        'is_workflow_task': False,
        'queue_name': 'q1',
        'task_name': 'task_a',
        'node_has_dependents': None,
    }
    row.update(overrides)
    return row


class TestDecodeStaleAttemptRow:
    def test_a_failure_of_a_plain_task(self) -> None:
        outcome = decode_stale_attempt_row(_finish_row())

        assert isinstance(outcome, StaleFailureApplied)
        assert outcome.retry == RetryNotRequested()
        assert outcome.is_workflow_task is False

    def test_a_failure_past_the_deadline(self) -> None:
        outcome = decode_stale_attempt_row(
            _finish_row(
                retry_disposition='PAST_DEADLINE',
                next_retry_at=DB_NOW + timedelta(seconds=5),
                good_until=DB_NOW,
            ),
        )

        assert isinstance(outcome, StaleFailureApplied)
        assert outcome.retry == RetryPastDeadline(
            next_retry_at=DB_NOW + timedelta(seconds=5), good_until=DB_NOW,
        )

    def test_a_failure_of_a_workflow_task_needs_the_dependents_fact(self) -> None:
        outcome = decode_stale_attempt_row(
            _finish_row(is_workflow_task=True, node_has_dependents=False),
        )
        assert isinstance(outcome, StaleFailureApplied)
        assert outcome.is_workflow_task is True

        with pytest.raises(OutcomeDecodeError, match='node_has_dependents'):
            decode_stale_attempt_row(_finish_row(is_workflow_task=True))

    def test_a_failure_of_another_kind_is_refused(self) -> None:
        with pytest.raises(OutcomeDecodeError, match='not FAIL_STALE'):
            decode_stale_attempt_row(_finish_row(terminalization_kind='FAIL_RUNNING'))

    def test_a_scheduled_retry(self) -> None:
        outcome = decode_stale_attempt_row(
            _finish_row(
                outcome='RETRY_SCHEDULED',
                terminal_at=None,
                terminalization_kind=None,
                retry_disposition=None,
                retry_count=1,
                next_retry_at=DB_NOW,
                is_workflow_task=None,
                task_name=None,
            ),
        )

        assert isinstance(outcome, RetryScheduled)
        assert (outcome.retry_count, outcome.queue_name) == (1, 'q1')

    def test_a_changed_retry_count_is_a_conflict_with_both_counts(self) -> None:
        outcome = decode_stale_attempt_row(
            _finish_row(
                outcome='SOURCE_STATE_CONFLICT',
                terminal_at=None,
                terminalization_kind=None,
                guard_kind='RETRY_COUNT',
                observed_guard={'expected_retry_count': 0, 'retry_count': 1},
                retry_disposition=None,
                is_workflow_task=None,
                queue_name=None,
                task_name=None,
            ),
        )

        assert isinstance(outcome, SourceStateConflict)
        assert outcome.evidence == ObservedRetryCount(expected_retry_count=0, retry_count=1)

    def test_a_refusal_with_a_routing_column_is_refused(self) -> None:
        with pytest.raises(OutcomeDecodeError, match='does not set'):
            decode_stale_attempt_row(
                _finish_row(
                    outcome='TASK_ABSENT',
                    terminal_at=None,
                    terminalization_kind=None,
                    observed_status=None,
                    observed_worker_id=None,
                    observed_claimed_at=None,
                    retry_disposition=None,
                ),
            )

    def test_an_absent_task(self) -> None:
        outcome = decode_stale_attempt_row(
            {
                **_finish_row(),
                **{
                    column: None
                    for column in (
                        'terminal_at',
                        'terminalization_kind',
                        'observed_status',
                        'observed_worker_id',
                        'observed_claimed_at',
                        'retry_disposition',
                        'is_workflow_task',
                        'queue_name',
                        'task_name',
                    )
                },
                'outcome': 'TASK_ABSENT',
            },
        )

        assert isinstance(outcome, TaskAbsent)
