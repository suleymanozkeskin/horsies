"""The orphan self-heal of the worker's finalize through the core: the call it
sends and the outcome it builds from each record of ``horsies._core``.

The records have no Python constructor, so each test stands in a record with
a ``MagicMock`` of its class: a class pattern matches it, and its fields are
set as attributes.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock

import pytest

import horsies._core as core
from horsies.core.lifecycle.commands import CancelOwnedOrphan
from horsies.core.lifecycle.core_records import task_row_end_of
from horsies.core.lifecycle.fences import OwnedClaim
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
from horsies.core.types.core_failure import CoreOperationFailed
from horsies.core.types.status import TaskStatus
from horsies.core.worker.finalize import FinalizeMixin

pytestmark = [pytest.mark.unit]

TASK_ID = '0b4d8a52-7d0e-4a0e-9d11-6b2f1f7c9e02'
CLAIMED_AT = datetime(2026, 11, 17, 11, 0, tzinfo=timezone.utc)
NOW = datetime(2026, 11, 17, 12, 0, tzinfo=timezone.utc)
WORKER_ID = 'worker-1'
SUBJECT = 'FinalizeMixin._cancel_owned_orphan'
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
        _record(core.TaskRowEnded, terminal_at=NOW, kind='CANCEL_ORPHAN'), SUBJECT,
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
            SUBJECT,
        ),
        AlreadyApplied,
    )
    assert task_row_end_of(_record(core.AttemptLostClaim), SUBJECT) == LostClaim(
        task_id=TASK_ID, ordinality=None, observed=OBSERVED,
    )
    conflict = task_row_end_of(
        _record(core.AttemptSourceStateConflict, guard='WORKFLOW_LINK_STATE'), SUBJECT,
    )
    assert conflict == SourceStateConflict(
        task_id=TASK_ID,
        ordinality=None,
        observed=OBSERVED,
        evidence=EvidenceLoggedByCore(guard=GuardKind.WORKFLOW_LINK_STATE),
    )
    assert task_row_end_of(_record(core.AttemptTaskAbsent), SUBJECT) == TaskAbsent(
        task_id=TASK_ID, ordinality=None,
    )


def test_a_failed_write_raises_with_the_core_s_retry_rule() -> None:
    with pytest.raises(CoreOperationFailed) as raised:
        task_row_end_of(
            _record(core.AttemptWriteFailed, message='connection lost', retryable=True),
            SUBJECT,
        )

    assert raised.value.subject == SUBJECT
    assert raised.value.message == 'connection lost'
    assert raised.value.retryable is True


def test_a_kind_outside_the_catalog_is_a_decode_error() -> None:
    with pytest.raises(OutcomeDecodeError):
        task_row_end_of(_record(core.TaskRowEnded, terminal_at=NOW, kind='NOT_A_KIND'), SUBJECT)


class _Probe(FinalizeMixin):
    def __init__(self, handle: MagicMock) -> None:
        self._handle = handle

    @property
    def core_handle(self) -> core.CoreHandle:
        return self._handle  # pyright: ignore[reportReturnType]


@pytest.mark.asyncio
async def test_the_self_heal_sends_the_task_and_its_claim_fence() -> None:
    handle = MagicMock()
    handle.cancel_owned_orphan_async = AsyncMock(
        return_value=_record(core.TaskRowEnded, terminal_at=NOW, kind='CANCEL_ORPHAN'),
    )
    command = CancelOwnedOrphan(
        task_id=TASK_ID,
        fence=OwnedClaim(worker_id=WORKER_ID, claimed_at=CLAIMED_AT),
    )

    outcome = await _Probe(handle)._cancel_owned_orphan(command)  # pyright: ignore[reportPrivateUsage]

    assert isinstance(outcome, Applied)
    handle.cancel_owned_orphan_async.assert_awaited_once_with(
        uuid.UUID(TASK_ID), worker_id=WORKER_ID, claimed_at=CLAIMED_AT,
    )
