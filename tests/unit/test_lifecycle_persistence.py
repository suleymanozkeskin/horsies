"""Adapter-side enforcement of the id-keyed batch wire contract."""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from horsies.core.lifecycle.commands import (
    AbandonOwnedNodes,
    CompleteLockedTask,
    ExpireOwnedClaim,
)
from horsies.core.lifecycle.fences import (
    OwnedClaim,
    OwnedClaimBatch,
    PriorLockedRead,
    WorkerOwned,
)
from horsies.core.lifecycle.operations import TerminalizationKind
from horsies.core.lifecycle.outcomes import (
    AlreadyApplied,
    Applied,
    LostClaim,
    ObservedDeadline,
    ObservedTaskState,
    SourceStateConflict,
)
from horsies.core.lifecycle.persistence import (
    _log_outcome,  # pyright: ignore[reportPrivateUsage]
    _reconstruct_id_keyed_batch,  # pyright: ignore[reportPrivateUsage]
    apply_async,
    apply_batch_async,
    apply_sync,
    classify_locked_read_miss_async,
)
from horsies.core.types.status import TaskStatus

pytestmark = pytest.mark.unit

_NOW = datetime(2026, 8, 4, tzinfo=timezone.utc)


def _command() -> AbandonOwnedNodes:
    return AbandonOwnedNodes(
        fence=OwnedClaimBatch(
            worker_id='worker',
            claim_generations=(('one', _NOW), ('two', _NOW)),
        ),
    )


def _outcome(task_id: str, ordinality: int | None) -> Applied:
    return Applied(
        task_id=task_id,
        ordinality=ordinality,
        terminal_at=_NOW,
        kind=TerminalizationKind.PAUSE_ABANDON_CLAIM_BATCH,
        observed=ObservedTaskState(
            status=TaskStatus.CLAIMED,
            worker_id='worker',
            claimed_at=_NOW,
        ),
    )


def _wire_row(**overrides: object) -> dict[str, object]:
    row: dict[str, object] = {
        'task_id': 'one',
        'ordinality': None,
        'outcome': 'ALREADY_APPLIED',
        'terminal_at': _NOW,
        'terminalization_kind': 'COMPLETE_FUSED',
        'observed_status': 'COMPLETED',
        'observed_worker_id': 'worker',
        'observed_claimed_at': _NOW,
        'guard_kind': None,
        'observed_guard': None,
    }
    row.update(overrides)
    return row


def _result_with_rows(rows: list[dict[str, object]]) -> MagicMock:
    result = MagicMock()
    result.mappings.return_value.all.return_value = rows
    return result


def test_reconstructs_by_ordinal_instead_of_result_order() -> None:
    outcomes = _reconstruct_id_keyed_batch(
        [_outcome('two', 2), _outcome('one', 1)],
        expected_count=2,
        command=_command(),
    )

    assert [outcome.task_id for outcome in outcomes] == ['one', 'two']


@pytest.mark.parametrize(
    ('outcomes', 'message'),
    [
        ([_outcome('one', None), _outcome('two', 2)], 'without ordinality'),
        ([_outcome('one', 1), _outcome('two', 1)], 'duplicate ordinality'),
        ([_outcome('one', 1)], 'ordinal set does not match'),
        ([_outcome('one', 1), _outcome('two', 3)], 'ordinal set does not match'),
    ],
)
def test_rejects_an_invalid_ordinal_contract(
    outcomes: list[Applied],
    message: str,
) -> None:
    with pytest.raises(RuntimeError, match=message):
        _reconstruct_id_keyed_batch(
            outcomes,
            expected_count=2,
            command=_command(),
        )


@pytest.mark.asyncio
async def test_locked_read_miss_keeps_and_logs_the_dispatched_generation(
    caplog: pytest.LogCaptureFixture,
) -> None:
    """The read-only classifier gets the fence half absent from the command."""
    result = MagicMock()
    result.mappings.return_value.all.return_value = [_wire_row()]
    connection = MagicMock()
    connection.execute = AsyncMock(return_value=result)
    command = CompleteLockedTask(
        task_id='one',
        fence=PriorLockedRead(worker_id='worker'),
        result_json='{"ok": 1}',
    )

    with caplog.at_level(logging.WARNING, logger='horsies.lifecycle'):
        outcome = await classify_locked_read_miss_async(
            connection,
            command,
            claimed_at=_NOW,
        )

    assert isinstance(outcome, AlreadyApplied)
    parameters = connection.execute.await_args.args[1]
    assert parameters == {
        'task_id': 'one',
        'equivalent_kinds': ['COMPLETE_FUSED', 'COMPLETE_LOCKED'],
        'worker_id': 'worker',
        'claimed_at': _NOW,
    }
    expected_fence = OwnedClaim(worker_id='worker', claimed_at=_NOW)
    assert f'expected_fence={expected_fence!r}' in caplog.records[-1].getMessage()


@pytest.mark.asyncio
async def test_async_adapter_logs_its_decoded_outcome(
    caplog: pytest.LogCaptureFixture,
) -> None:
    connection = MagicMock()
    connection.execute = AsyncMock(return_value=_result_with_rows([_wire_row()]))
    command = CompleteLockedTask(
        task_id='one',
        fence=PriorLockedRead(worker_id='worker'),
        result_json='{"ok": 1}',
    )

    with caplog.at_level(logging.WARNING, logger='horsies.lifecycle'):
        outcome = await apply_async(connection, command)

    assert isinstance(outcome, AlreadyApplied)
    message = caplog.records[-1].getMessage()
    assert 'outcome=AlreadyApplied' in message
    assert 'committed_kind=COMPLETE_FUSED' in message
    assert 'terminal_at=' in message


def test_sync_adapter_logs_its_decoded_outcome(
    caplog: pytest.LogCaptureFixture,
) -> None:
    row = _wire_row()
    cursor = MagicMock()
    cursor.description = [SimpleNamespace(name=column) for column in row]
    cursor.fetchall.return_value = [tuple(row.values())]
    command = CompleteLockedTask(
        task_id='one',
        fence=PriorLockedRead(worker_id='worker'),
        result_json='{"ok": 1}',
    )

    with caplog.at_level(logging.WARNING, logger='horsies.lifecycle'):
        outcome = apply_sync(cursor, command)

    assert isinstance(outcome, AlreadyApplied)
    assert 'outcome=AlreadyApplied' in caplog.records[-1].getMessage()


@pytest.mark.asyncio
async def test_batch_adapter_validates_then_logs_in_input_order(
    caplog: pytest.LogCaptureFixture,
) -> None:
    rows = [
        _wire_row(
            task_id='two',
            ordinality=2,
            outcome='APPLIED',
            terminalization_kind='PAUSE_ABANDON_CLAIM_BATCH',
            observed_status='CLAIMED',
        ),
        _wire_row(
            task_id='one',
            ordinality=1,
            outcome='APPLIED',
            terminalization_kind='PAUSE_ABANDON_CLAIM_BATCH',
            observed_status='CLAIMED',
        ),
    ]
    connection = MagicMock()
    connection.execute = AsyncMock(return_value=_result_with_rows(rows))

    with caplog.at_level(logging.DEBUG, logger='horsies.lifecycle'):
        outcomes = await apply_batch_async(connection, _command())

    assert [outcome.task_id for outcome in outcomes] == ['one', 'two']
    messages = [record.getMessage() for record in caplog.records[-2:]]
    assert 'task_id=one' in messages[0]
    assert 'task_id=two' in messages[1]


def test_refusal_log_contains_operation_fence_and_locked_observation(
    caplog: pytest.LogCaptureFixture,
) -> None:
    command = CompleteLockedTask(
        task_id='one',
        fence=PriorLockedRead(worker_id='expected-worker'),
        result_json='{"ok": 1}',
    )
    outcome = LostClaim(
        task_id='one',
        ordinality=None,
        observed=ObservedTaskState(
            status=TaskStatus.RUNNING,
            worker_id='actual-worker',
            claimed_at=_NOW,
        ),
    )

    with caplog.at_level(logging.WARNING, logger='horsies.lifecycle'):
        _log_outcome(command, outcome)

    message = caplog.records[-1].getMessage()
    assert 'operation=CompleteLockedTask' in message
    assert 'function=horsies_complete_locked_task' in message
    assert "worker_id='expected-worker'" in message
    assert "worker_id='actual-worker'" in message
    assert 'claimed_at=' in message


def test_guard_refusal_log_contains_typed_evidence(
    caplog: pytest.LogCaptureFixture,
) -> None:
    command = ExpireOwnedClaim(
        task_id='one',
        fence=WorkerOwned(worker_id='worker'),
        result_json='{"ok": 1}',
        error_code='TASK_EXPIRED',
    )
    evidence = ObservedDeadline(
        good_until=_NOW,
        evaluated_at=_NOW,
    )
    outcome = SourceStateConflict(
        task_id='one',
        ordinality=None,
        observed=ObservedTaskState(
            status=TaskStatus.RUNNING,
            worker_id='worker',
            claimed_at=_NOW,
        ),
        evidence=evidence,
    )

    with caplog.at_level(logging.WARNING, logger='horsies.lifecycle'):
        _log_outcome(command, outcome)

    message = caplog.records[-1].getMessage()
    assert 'outcome=SourceStateConflict' in message
    assert f'evidence={evidence!r}' in message


def test_batch_outcome_log_contains_only_that_tasks_expected_fence(
    caplog: pytest.LogCaptureFixture,
) -> None:
    command = AbandonOwnedNodes(
        fence=OwnedClaimBatch(
            worker_id='worker',
            claim_generations=(
                ('one', _NOW),
                ('other-task-must-not-repeat', None),
            ),
        ),
    )

    with caplog.at_level(logging.DEBUG, logger='horsies.lifecycle'):
        _log_outcome(command, _outcome('one', 1))

    message = caplog.records[-1].getMessage()
    assert 'fence_type=OwnedClaimBatch' in message
    assert "'worker_id': 'worker'" in message
    assert "'claimed_at':" in message
    assert 'other-task-must-not-repeat' not in message
