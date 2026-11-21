"""The single-row end records of the core and the orphan self-heal of the
child's stop before start: the outcome Python builds from each record of
``horsies._core`` (``task_row_end_of``), and the wire reason of each step of
``horsies_workflow_task_stop_before_start`` (``stopped_before_start_of``).
The worker's finalize sends no orphan cancel: the child's stop cancels the
orphan when the self-heal of its run is on.

The records have no Python constructor, so each test stands in a record with
a ``MagicMock`` of its class: a class pattern matches it, and its fields are
set as attributes.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest

import horsies._core as core
from horsies.core.lifecycle.core_records import task_row_end_of
from horsies.core.lifecycle.operations import TerminalizationKind
from horsies.core.lifecycle.outcomes import (
    AlreadyApplied,
    Applied,
    EvidenceLoggedByCore,
    GuardKind,
    LostClaim,
    ObservedTaskState,
    OutcomeDecodeError,
    SourceStateConflict,
    TaskAbsent,
)
from horsies.core.types.status import TaskStatus
from horsies.core.worker.run_return import StoppedBeforeRunning
from horsies.core.worker.child_runner import (
    _WorkflowResumedBeforeStart,  # pyright: ignore[reportPrivateUsage]
    stopped_before_start_of,
)

pytestmark = [pytest.mark.unit]

TASK_ID = '0b4d8a52-7d0e-4a0e-9d11-6b2f1f7c9e02'
CLAIMED_AT = datetime(2026, 11, 17, 11, 0, tzinfo=timezone.utc)
NOW = datetime(2026, 11, 17, 12, 0, tzinfo=timezone.utc)
WORKER_ID = 'worker-1'
OBSERVED = ObservedTaskState(status=TaskStatus.CLAIMED, worker_id=WORKER_ID, claimed_at=CLAIMED_AT)


def _record(record_class: type, **fields: object) -> MagicMock:
    """A stand-in for a core record of ``record_class``, with the task id and
    the observed row of ``OBSERVED`` unless ``fields`` names them."""
    record = MagicMock(spec=record_class)
    defaults: dict[str, object] = {
        'task_id': uuid.UUID(TASK_ID),
        'observed_status': 'CLAIMED',
        'observed_worker_id': WORKER_ID,
        'observed_claimed_at': CLAIMED_AT,
    }
    for name, value in (defaults | fields).items():
        setattr(record, name, value)
    return record


def test_an_ended_row_is_applied_with_its_kind_and_observed_row() -> None:
    outcome = task_row_end_of(
        _record(core.TaskRowEnded, terminal_at=NOW, kind='CANCEL_ORPHAN'),
    )

    assert outcome == Applied(
        task_id=TASK_ID,
        ordinality=None,
        terminal_at=NOW,
        kind=TerminalizationKind.CANCEL_ORPHAN,
        observed=OBSERVED,
    )


def test_each_refusal_keeps_its_outcome() -> None:
    assert isinstance(
        task_row_end_of(
            _record(core.AttemptAlreadyApplied, terminal_at=NOW, kind='CANCEL_ORPHAN_SWEEP'),
        ),
        AlreadyApplied,
    )
    assert task_row_end_of(_record(core.AttemptLostClaim)) == LostClaim(
        task_id=TASK_ID, ordinality=None, observed=OBSERVED,
    )
    conflict = task_row_end_of(
        _record(core.AttemptSourceStateConflict, guard='WORKFLOW_LINK_STATE'),
    )
    assert conflict == SourceStateConflict(
        task_id=TASK_ID,
        ordinality=None,
        observed=OBSERVED,
        evidence=EvidenceLoggedByCore(guard=GuardKind.WORKFLOW_LINK_STATE),
    )
    assert task_row_end_of(_record(core.AttemptTaskAbsent)) == TaskAbsent(
        task_id=TASK_ID, ordinality=None,
    )


def test_a_failed_write_raises_the_record_exception() -> None:
    cause = core.CoreConnectionError('expire_owned_claim', 'connection lost', True)

    with pytest.raises(core.CoreConnectionError) as raised:
        task_row_end_of(
            _record(
                core.AttemptWriteFailed,
                message='connection lost',
                retryable=True,
                exception=cause,
            ),
        )

    assert raised.value is cause


def test_a_kind_outside_the_catalog_is_a_decode_error() -> None:
    with pytest.raises(OutcomeDecodeError):
        task_row_end_of(_record(core.TaskRowEnded, terminal_at=NOW, kind='NOT_A_KIND'))


@pytest.mark.parametrize(
    ('outcome', 'move_outcome', 'stop'),
    [
        ('EXPIRED', 'APPLIED', StoppedBeforeRunning('TASK_EXPIRED')),
        ('EXPIRED', 'ALREADY_APPLIED', StoppedBeforeRunning('TASK_EXPIRED')),
        ('ABANDONED', 'APPLIED', StoppedBeforeRunning('WORKFLOW_STOPPED')),
        ('ABANDONED', 'LOST_CLAIM', StoppedBeforeRunning('WORKFLOW_STOPPED')),
        ('CANCELLED', 'APPLIED', StoppedBeforeRunning('WORKFLOW_STOPPED')),
        ('CANCELLED', 'TASK_ABSENT', StoppedBeforeRunning('WORKFLOW_STOPPED')),
        ('ORPHAN_CANCELLED', 'APPLIED', StoppedBeforeRunning('ORPHAN_CANCELLED')),
        ('ORPHAN_CANCELLED', 'SOURCE_STATE_CONFLICT', StoppedBeforeRunning('ORPHAN_CANCELLED')),
        ('NODE_NOT_LINKED', None, StoppedBeforeRunning('NODE_NOT_LINKED')),
        ('NOT_HELD', 'LOST_CLAIM', StoppedBeforeRunning('CLAIM_LOST')),
    ],
)
def test_each_step_of_the_stop_gives_its_stop_reason(
    outcome: str,
    move_outcome: str | None,
    stop: StoppedBeforeRunning,
) -> None:
    record = _record(core.StoppedBeforeStart, outcome=outcome, move_outcome=move_outcome)

    assert stopped_before_start_of(TASK_ID, record) == stop


def test_a_resume_that_came_first_runs_the_start_again() -> None:
    assert (
        stopped_before_start_of(TASK_ID, _record(core.ResumedBeforeStart))
        == _WorkflowResumedBeforeStart()
    )


def test_a_failed_stop_is_an_unconfirmed_start() -> None:
    record = _record(core.StopBeforeStartFailed, message='connection lost', retryable=True)

    assert stopped_before_start_of(TASK_ID, record) == StoppedBeforeRunning(
        'OWNERSHIP_UNCONFIRMED',
    )
