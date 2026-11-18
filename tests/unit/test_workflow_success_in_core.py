"""The workflow success through the core: the call it makes and the outcome
it builds from each record of ``horsies._core``.

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
from horsies.core.lifecycle.fences import OwnedClaim
from horsies.core.lifecycle.operations import TerminalizationKind
from horsies.core.lifecycle.outcomes import (
    AlreadyApplied,
    ObservedTaskState,
    PhaseTwoOwed,
    PhaseTwoStep,
    SuccessApplied,
    TaskAbsent,
)
from horsies.core.types.status import TaskStatus
from horsies.core.worker.workflow_success import (
    FinishWorkflowSuccess,
    finish_workflow_success_in_core,
)

pytestmark = [pytest.mark.unit]

TASK_ID = '0b4d8a52-7d0e-4a0e-9d11-6b2f1f7c9e02'
CLAIMED_AT = datetime(2026, 11, 11, 11, 0, tzinfo=timezone.utc)
NOW = datetime(2026, 11, 11, 12, 0, tzinfo=timezone.utc)
WORKER_ID = 'worker-1'
OBSERVED = ObservedTaskState(status=TaskStatus.RUNNING, worker_id=WORKER_ID, claimed_at=CLAIMED_AT)
COMMAND = FinishWorkflowSuccess(
    task_id=TASK_ID,
    fence=OwnedClaim(worker_id=WORKER_ID, claimed_at=CLAIMED_AT),
    result_json='{"ok": 1}',
)


def _record(record_class: type, **fields: object) -> MagicMock:
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


def _handle(answer: object) -> MagicMock:
    handle = MagicMock()
    handle.finish_workflow_success_async = AsyncMock(return_value=answer)
    return handle


@pytest.mark.asyncio
async def test_an_applied_success_owes_phase_2_in_the_form_of_its_node() -> None:
    owed = _record(
        core.PhaseTwoOwedRecord, task_name='t', queue_name='q', step='NO_DEPENDENTS',
    )
    handle = _handle(_record(core.WorkflowSuccessApplied, terminal_at=NOW, phase_two=owed))

    outcome = await finish_workflow_success_in_core(handle, COMMAND)

    assert outcome == SuccessApplied(
        task_id=TASK_ID,
        terminal_at=NOW,
        observed=OBSERVED,
        phase_two=PhaseTwoOwed(task_name='t', queue_name='q', step=PhaseTwoStep.NO_DEPENDENTS),
    )
    handle.finish_workflow_success_async.assert_awaited_once_with(
        uuid.UUID(TASK_ID),
        worker_id=WORKER_ID,
        claimed_at=CLAIMED_AT,
        result_json='{"ok": 1}',
    )


@pytest.mark.asyncio
async def test_a_replay_and_an_absent_task_are_the_shared_outcomes() -> None:
    replay = await finish_workflow_success_in_core(
        _handle(_record(core.AttemptAlreadyApplied, terminal_at=NOW, kind='COMPLETE_LOCKED')),
        COMMAND,
    )
    absent = await finish_workflow_success_in_core(
        _handle(_record(core.AttemptTaskAbsent)), COMMAND,
    )

    assert replay == AlreadyApplied(
        task_id=TASK_ID,
        ordinality=None,
        terminal_at=NOW,
        kind=TerminalizationKind.COMPLETE_LOCKED,
        observed=OBSERVED,
    )
    assert absent == TaskAbsent(task_id=TASK_ID, ordinality=None)


@pytest.mark.asyncio
async def test_a_failed_statement_raises_with_its_classification() -> None:
    cause = core.CoreConnectionError('finish_workflow_success', 'connection lost', True)
    handle = _handle(
        _record(
            core.AttemptWriteFailed, message='connection lost', retryable=True, exception=cause,
        ),
    )

    with pytest.raises(core.CoreConnectionError) as raised:
        _ = await finish_workflow_success_in_core(handle, COMMAND)

    assert raised.value is cause
