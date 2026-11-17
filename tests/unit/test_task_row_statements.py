"""The statements of a child that write one task row: the start and the
finalizing marker.

The start is one ``horsies_start_attempt`` call and the marker one
``horsies_mark_finalizing`` call; each function reads its row by id alone
(their SQL cases pin the outcomes, and ``test_task_row_plans.py`` the plan of
the marker's row read).
"""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

import os
import socket

from horsies.core.worker.child_runner import (
    MARK_FINALIZING_SQL,
    START_ATTEMPT_SQL,
    AttemptMissed,
    AttemptStarted,
    FinalizingMark,
    StartMiss,
    mark_finalizing_params,
    read_finalizing_mark,
    read_start_outcome,
    start_attempt_params,
)

pytestmark = [pytest.mark.unit]

CLAIMED_AT = datetime(2026, 11, 10, 9, 0, tzinfo=timezone.utc)


def _words(sql: str) -> str:
    return ' '.join(sql.split())


def test_the_start_is_one_function_call_with_one_parameter_per_placeholder() -> None:
    params = start_attempt_params('task-1', 'worker-1', CLAIMED_AT)

    assert 'FROM horsies_start_attempt(' in _words(START_ATTEMPT_SQL)
    assert START_ATTEMPT_SQL.count('%s') == len(params)


def test_the_start_names_the_claim_generation_and_this_process() -> None:
    pid = os.getpid()

    params = start_attempt_params('task-1', 'worker-1', CLAIMED_AT)

    assert params == (
        'task-1',
        'worker-1',
        CLAIMED_AT,
        pid,
        socket.gethostname(),
        f'worker-{pid}',
        f'worker-1:{pid}',
    )


@pytest.mark.parametrize('reason', list(StartMiss), ids=[each.value for each in StartMiss])
def test_each_miss_reason_reads_as_its_member(reason: StartMiss) -> None:
    assert read_start_outcome(('MISSED', reason.value), 'task-1') == AttemptMissed(reason=reason)


def test_a_started_row_reads_as_started() -> None:
    assert read_start_outcome(('STARTED', None), 'task-1') == AttemptStarted()


@pytest.mark.parametrize(
    'row',
    [None, ('MISSED', 'SOMETHING_ELSE'), ('MISSED', None), ('STARTED', 'NOT_HELD')],
    ids=['no row', 'unknown reason', 'miss without reason', 'start with reason'],
)
def test_a_row_outside_the_contract_is_a_defect(row: tuple[object, ...] | None) -> None:
    with pytest.raises(AssertionError, match='horsies_start_attempt returned'):
        read_start_outcome(row, 'task-1')


def test_the_marker_is_one_function_call_with_one_parameter_per_placeholder() -> None:
    params = mark_finalizing_params('task-1', 'worker-1', CLAIMED_AT)

    assert 'FROM horsies_mark_finalizing(' in _words(MARK_FINALIZING_SQL)
    assert MARK_FINALIZING_SQL.count('%s') == len(params)


def test_the_marker_takes_the_task_the_worker_then_the_claim_time() -> None:
    params = mark_finalizing_params('task-1', 'worker-1', CLAIMED_AT)

    assert params == ('task-1', 'worker-1', CLAIMED_AT)


@pytest.mark.parametrize('mark', list(FinalizingMark), ids=[each.value for each in FinalizingMark])
def test_each_marker_outcome_reads_as_its_member(mark: FinalizingMark) -> None:
    assert read_finalizing_mark((mark.value,), 'task-1') is mark


@pytest.mark.parametrize(
    'row',
    [None, ('SOMETHING_ELSE',), ('MARKED', None)],
    ids=['no row', 'unknown outcome', 'extra column'],
)
def test_a_marker_row_outside_the_contract_is_a_defect(row: tuple[object, ...] | None) -> None:
    with pytest.raises(AssertionError, match='horsies_mark_finalizing returned'):
        read_finalizing_mark(row, 'task-1')
