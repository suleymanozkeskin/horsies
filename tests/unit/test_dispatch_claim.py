"""The claim of a dispatch (``dispatch_claim_of``) and the row of a released claim (``decode_ended_attempt_row``)."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

import pytest

from horsies.core.lifecycle.outcomes import (
    ClaimReleased,
    FailureApplied,
    ObservedTaskState,
    OutcomeDecodeError,
    decode_ended_attempt_row,
)
from horsies.core.types.status import TaskStatus
from horsies.core.worker.dispatch_claim import DispatchClaim, dispatch_claim_of

pytestmark = [pytest.mark.unit]

TASK_ID = '0b4d8a52-7d0e-4a0e-9d11-6b2f1f7c9e01'
NOW = datetime(2026, 11, 11, 12, 0, tzinfo=timezone.utc)


def _claim_row(**overrides: Any) -> dict[str, Any]:
    row: dict[str, Any] = {
        'row_kind': 'CLAIMED',
        'id': TASK_ID,
        'claimed_at': NOW,
        'retry_count': 1,
        'max_retries': 3,
        'task_options': '{"retry_policy": {}}',
    }
    row.update(overrides)
    return row


class TestDispatchClaimOf:
    def test_a_claimed_row_gives_its_generation_and_retry_facts(self) -> None:
        assert dispatch_claim_of(_claim_row()) == DispatchClaim(
            claimed_at=NOW,
            retry_count=1,
            max_retries=3,
            task_options='{"retry_policy": {}}',
        )

    def test_a_row_without_options_has_none(self) -> None:
        assert dispatch_claim_of(_claim_row(task_options=None)).task_options is None

    @pytest.mark.parametrize(
        'overrides',
        [
            {'claimed_at': None},
            {'retry_count': None},
            {'max_retries': '3'},
        ],
        ids=['no_claim_time', 'no_retry_count', 'retry_limit_as_text'],
    )
    def test_a_row_outside_the_claim_contract_is_a_defect(self, overrides: dict[str, Any]) -> None:
        with pytest.raises(AssertionError, match=TASK_ID):
            dispatch_claim_of(_claim_row(**overrides))


def _released_row(**overrides: Any) -> dict[str, Any]:
    row: dict[str, Any] = {
        'task_id': TASK_ID,
        'ordinality': None,
        'outcome': 'RELEASED',
        'terminal_at': None,
        'terminalization_kind': None,
        'observed_status': 'CLAIMED',
        'observed_worker_id': 'w-1',
        'observed_claimed_at': NOW,
        'guard_kind': None,
        'observed_guard': None,
        'retry_disposition': None,
        'retry_count': None,
        'next_retry_at': None,
        'good_until': None,
        'is_workflow_task': None,
        'queue_name': 'q1',
        'task_name': None,
        'node_has_dependents': None,
    }
    row.update(overrides)
    return row


class TestEndedAttemptRow:
    def test_a_released_row_is_a_released_claim(self) -> None:
        assert decode_ended_attempt_row(_released_row()) == ClaimReleased(
            task_id=TASK_ID,
            queue_name='q1',
            observed=ObservedTaskState(
                status=TaskStatus.CLAIMED,
                worker_id='w-1',
                claimed_at=NOW,
            ),
        )

    def test_another_outcome_decodes_as_a_failed_attempt(self) -> None:
        row = _released_row(
            outcome='APPLIED',
            terminal_at=NOW,
            terminalization_kind='FAIL_RUNNING',
            observed_status='RUNNING',
            retry_disposition='NOT_REQUESTED',
            is_workflow_task=False,
            task_name='task_a',
        )

        assert isinstance(decode_ended_attempt_row(row), FailureApplied)

    @pytest.mark.parametrize(
        'overrides',
        [
            {'observed_status': 'RUNNING'},
            {'terminal_at': NOW},
            {'retry_count': 1},
            {'task_name': 'task_a'},
            {'queue_name': None},
        ],
        ids=['not_claimed', 'terminal_at', 'retry_count', 'task_name', 'no_queue'],
    )
    def test_a_released_row_outside_its_contract_is_refused(
        self, overrides: dict[str, Any],
    ) -> None:
        with pytest.raises(OutcomeDecodeError):
            decode_ended_attempt_row(_released_row(**overrides))
