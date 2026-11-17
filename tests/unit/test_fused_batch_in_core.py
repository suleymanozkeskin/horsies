"""The plain-success batch through the core: the request it sends and the
outcomes it builds from each record of ``horsies._core``.

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
from horsies.core.lifecycle.commands import CompleteTaskFused
from horsies.core.lifecycle.fences import OwnedClaim
from horsies.core.lifecycle.operations import TerminalizationKind
from horsies.core.lifecycle.outcomes import (
    Applied,
    LostClaim,
    ObservedTaskState,
    TaskAbsent,
)
from horsies.core.lifecycle.persistence import (
    BatchCommitUnknown,
    BatchRolledBack,
    CoreNotSent,
    CoreOutcomeUnknown,
    CoreStatementRefused,
    FusedCompletionBatch,
    complete_fused_batch_in_core,
)
from horsies.core.types.result import Err, Ok
from horsies.core.types.status import TaskStatus

pytestmark = [pytest.mark.unit]

TASK_IDS = (
    '0b4d8a52-7d0e-4a0e-9d11-6b2f1f7c9e01',
    '0b4d8a52-7d0e-4a0e-9d11-6b2f1f7c9e02',
    '0b4d8a52-7d0e-4a0e-9d11-6b2f1f7c9e03',
)
CLAIMED_AT = datetime(2026, 11, 11, 11, 0, tzinfo=timezone.utc)
NOW = datetime(2026, 11, 11, 12, 0, tzinfo=timezone.utc)
WORKER_ID = 'worker-1'
OBSERVED = ObservedTaskState(status=TaskStatus.RUNNING, worker_id=WORKER_ID, claimed_at=CLAIMED_AT)


def _command(task_id: str) -> CompleteTaskFused:
    return CompleteTaskFused(
        task_id=task_id,
        fence=OwnedClaim(worker_id=WORKER_ID, claimed_at=CLAIMED_AT),
        result_json=f'{{"ok": "{task_id[-1]}"}}',
    )


def _batch(*task_ids: str) -> FusedCompletionBatch:
    return FusedCompletionBatch(commands=tuple(_command(task_id) for task_id in task_ids))


def _record(record_class: type, task_id: str, **fields: object) -> MagicMock:
    record = MagicMock(spec=record_class)
    defaults: dict[str, object] = {
        'task_id': uuid.UUID(task_id),
        'observed_status': 'RUNNING',
        'observed_worker_id': WORKER_ID,
        'observed_claimed_at': CLAIMED_AT,
    }
    for name, value in (defaults | fields).items():
        setattr(record, name, value)
    return record


def _written(*rows: MagicMock) -> MagicMock:
    written = MagicMock(spec=core.SuccessesWritten)
    written.rows = list(rows)
    return written


def _failure(record_class: type, **fields: object) -> MagicMock:
    record = MagicMock(spec=record_class)
    for name, value in fields.items():
        setattr(record, name, value)
    return record


def _handle(answer: object) -> MagicMock:
    handle = MagicMock()
    handle.complete_successes_async = AsyncMock(return_value=answer)
    return handle


@pytest.mark.asyncio
async def test_the_request_names_the_worker_and_each_row_in_order() -> None:
    batch = _batch(TASK_IDS[1], TASK_IDS[0])
    handle = _handle(
        _written(
            _record(core.SuccessApplied, TASK_IDS[1], terminal_at=NOW),
            _record(core.SuccessApplied, TASK_IDS[0], terminal_at=NOW),
        ),
    )

    with patch.object(core, 'SuccessBatchRequest') as request_class:
        _ = await complete_fused_batch_in_core(handle, batch)

    request_class.assert_called_once_with(
        worker_id=WORKER_ID,
        rows=[
            (uuid.UUID(TASK_IDS[1]), CLAIMED_AT, '{"ok": "2"}'),
            (uuid.UUID(TASK_IDS[0]), CLAIMED_AT, '{"ok": "1"}'),
        ],
    )
    handle.complete_successes_async.assert_awaited_once_with(request_class.return_value)


@pytest.mark.asyncio
async def test_each_row_record_is_its_outcome_at_its_position() -> None:
    handle = _handle(
        _written(
            _record(core.SuccessApplied, TASK_IDS[0], terminal_at=NOW),
            _record(core.AttemptLostClaim, TASK_IDS[1], observed_claimed_at=NOW),
            _record(core.AttemptTaskAbsent, TASK_IDS[2]),
        ),
    )

    written = await complete_fused_batch_in_core(handle, _batch(*TASK_IDS))

    assert written == Ok(
        (
            Applied(
                task_id=TASK_IDS[0],
                ordinality=1,
                terminal_at=NOW,
                kind=TerminalizationKind.COMPLETE_FUSED,
                observed=OBSERVED,
            ),
            LostClaim(
                task_id=TASK_IDS[1],
                ordinality=2,
                observed=ObservedTaskState(
                    status=TaskStatus.RUNNING, worker_id=WORKER_ID, claimed_at=NOW,
                ),
            ),
            TaskAbsent(task_id=TASK_IDS[2], ordinality=3),
        ),
    )


@pytest.mark.asyncio
async def test_a_batch_of_one_row_has_no_ordinality() -> None:
    """As the single fused completion that ``apply_fused_batch`` sends for
    one row."""
    handle = _handle(_written(_record(core.SuccessApplied, TASK_IDS[0], terminal_at=NOW)))

    written = await complete_fused_batch_in_core(handle, _batch(TASK_IDS[0]))

    match written:
        case Ok((Applied(ordinality=ordinality),)):
            assert ordinality is None
        case _:
            raise AssertionError(written)


@pytest.mark.asyncio
async def test_a_batch_that_was_not_sent_is_rolled_back_with_the_core_cause() -> None:
    cause = core.CoreConnectionError('complete_successes', 'pool timed out', True)
    handle = _handle(
        _failure(core.BatchNotSent, message='pool timed out', retryable=True, exception=cause),
    )

    written = await complete_fused_batch_in_core(handle, _batch(*TASK_IDS))

    match written:
        case Err(BatchRolledBack(cause=CoreNotSent(error=error)) as rolled_back):
            assert error is cause
            assert rolled_back.retryable is True
        case _:
            raise AssertionError(written)


@pytest.mark.asyncio
async def test_a_refused_batch_is_rolled_back_with_the_core_refusal() -> None:
    """The batcher writes each row of a refused batch of several alone."""
    cause = core.CoreServerError('complete_successes', '23514', 'check violation', False)
    handle = _handle(
        _failure(core.BatchRefused, message='23514', retryable=False, exception=cause),
    )

    written = await complete_fused_batch_in_core(handle, _batch(*TASK_IDS))

    match written:
        case Err(BatchRolledBack(cause=CoreStatementRefused(error=error)) as rolled_back):
            assert error is cause
            assert rolled_back.retryable is False
        case _:
            raise AssertionError(written)


@pytest.mark.asyncio
async def test_a_lost_reply_is_a_commit_unknown_with_the_core_cause() -> None:
    cause = core.CoreConnectionError('complete_successes', 'connection lost', True)
    handle = _handle(
        _failure(core.BatchCommitUnknown, message='connection lost', exception=cause),
    )

    written = await complete_fused_batch_in_core(handle, _batch(*TASK_IDS))

    match written:
        case Err(BatchCommitUnknown(cause=CoreOutcomeUnknown(error=error))):
            assert error is cause
        case _:
            raise AssertionError(written)


@pytest.mark.asyncio
async def test_an_outcome_of_another_task_at_a_position_is_a_defect() -> None:
    handle = _handle(
        _written(
            _record(core.SuccessApplied, TASK_IDS[1], terminal_at=NOW),
            _record(core.SuccessApplied, TASK_IDS[0], terminal_at=NOW),
        ),
    )

    with pytest.raises(RuntimeError, match='at the position of task'):
        _ = await complete_fused_batch_in_core(handle, _batch(TASK_IDS[0], TASK_IDS[1]))


@pytest.mark.asyncio
async def test_a_row_count_other_than_the_batch_is_a_defect() -> None:
    handle = _handle(_written(_record(core.SuccessApplied, TASK_IDS[0], terminal_at=NOW)))

    with pytest.raises(RuntimeError, match='answered 1 rows for a batch of 2'):
        _ = await complete_fused_batch_in_core(handle, _batch(TASK_IDS[0], TASK_IDS[1]))
