"""Adapter-side enforcement of the id-keyed batch wire contract."""

from __future__ import annotations

import json
import logging
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from horsies.core.lifecycle.commands import (
    AbandonOwnedNodes,
    CompleteLockedTask,
    CompleteTaskFused,
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
    FusedCompletionBatch,
    _fused_batch_parameters,  # pyright: ignore[reportPrivateUsage]
    _fused_outcomes,  # pyright: ignore[reportPrivateUsage]
    _fused_statement,  # pyright: ignore[reportPrivateUsage]
    _log_outcome,  # pyright: ignore[reportPrivateUsage]
    _reconstruct_id_keyed_batch,  # pyright: ignore[reportPrivateUsage]
    apply_async,
    apply_batch_async,
    apply_sync,
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
        operation=type(_command()).__name__,
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
            operation=type(_command()).__name__,
        )


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


def _fused_command(task_id: str) -> CompleteTaskFused:
    return CompleteTaskFused(
        task_id=task_id,
        fence=OwnedClaim(worker_id='worker-1', claimed_at=None),
        result_json='{"ok":null}',
        notify_channel='task_queue_default',
        notify_payload=f'capacity:{task_id}',
    )


def test_a_batch_of_one_row_runs_the_single_fused_completion() -> None:
    statement = _fused_statement(FusedCompletionBatch(commands=(_fused_command('a'),)))

    assert 'horsies_complete_task_fused(' in str(statement.statement)
    assert 'horsies_complete_tasks_fused(' not in str(statement.statement)
    assert statement.parameters['task_id'] == 'a'


def test_a_batch_of_several_rows_runs_the_batch_function() -> None:
    statement = _fused_statement(
        FusedCompletionBatch(commands=(_fused_command('a'), _fused_command('b'))),
    )

    assert 'horsies_complete_tasks_fused(' in str(statement.statement)
    batch_text = statement.parameters['batch']
    assert isinstance(batch_text, str)
    assert [fields[0] for fields in json.loads(batch_text)] == ['a', 'b']


def test_the_batch_parameter_holds_each_command_in_input_order() -> None:
    """Six text-or-null fields per command; the generation keeps its
    microseconds and its UTC offset."""
    generation = datetime(
        2026, 11, 12, 9, 30, 0, 123456, tzinfo=timezone(timedelta(hours=5, minutes=30)),
    )
    later = CompleteTaskFused(
        task_id='b',
        fence=OwnedClaim(worker_id='worker-2', claimed_at=generation),
        result_json='{"ok":"quote \\" é 𝄞"}',
        notify_channel='task_queue_other',
        notify_payload='capacity:b',
    )

    parameters = _fused_batch_parameters(
        FusedCompletionBatch(commands=(later, _fused_command('a'))),
    )

    assert list(parameters) == ['batch']
    batch_text = parameters['batch']
    assert isinstance(batch_text, str)
    assert json.loads(batch_text) == [
        [
            'b',
            'worker-2',
            '2026-11-12T09:30:00.123456+05:30',
            '{"ok":"quote \\" é 𝄞"}',
            'task_queue_other',
            'capacity:b',
        ],
        ['a', 'worker-1', None, '{"ok":null}', 'task_queue_default', 'capacity:a'],
    ]


def test_the_single_fused_completion_answers_without_ordinality() -> None:
    command = _fused_command('one')

    outcomes = _fused_outcomes(
        FusedCompletionBatch(commands=(command,)),
        [_wire_row(task_id='one', outcome='APPLIED', observed_status='RUNNING')],
    )

    assert [outcome.task_id for outcome in outcomes] == ['one']
