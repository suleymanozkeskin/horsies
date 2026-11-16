"""The statements of a child that write one task row: the start and the
finalizing marker.

The start is one ``horsies_start_attempt`` call; the function reads its row
by id alone (its SQL cases pin that). The marker reads its row by id alone,
in a MATERIALIZED CTE that locks it FOR NO KEY UPDATE and computes the fence
as one boolean, and updates by that id. So only the primary key serves it,
whatever the statistics say.
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
    StartMiss,
    mark_finalizing_params,
    read_start_outcome,
    start_attempt_params,
)

pytestmark = [pytest.mark.unit]

CLAIMED_AT = datetime(2026, 11, 10, 9, 0, tzinfo=timezone.utc)


def _words(sql: str) -> str:
    return ' '.join(sql.split())


def test_the_marker_row_is_read_by_id_alone_and_updated_by_that_id() -> None:
    words = _words(MARK_FINALIZING_SQL)

    assert 'FROM horsies_tasks WHERE id = %s FOR NO KEY UPDATE' in words
    assert 'FROM target WHERE t.id = target.id AND target.' in words


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


def test_the_marker_has_one_parameter_per_placeholder() -> None:
    params = mark_finalizing_params('task-1', 'worker-1', CLAIMED_AT)

    assert MARK_FINALIZING_SQL.count('%s') == len(params)


def test_the_marker_fence_checks_the_claim_time_with_no_null_branch() -> None:
    words = _words(MARK_FINALIZING_SQL)

    assert 'AND claimed_by_worker_id = %s AND claimed_at = %s::timestamptz' in words
    assert 'IS NULL OR claimed_at' not in words


def test_the_marker_fence_takes_the_worker_then_the_claim_time() -> None:
    params = mark_finalizing_params('task-1', 'worker-1', CLAIMED_AT)

    assert params == ('worker-1', CLAIMED_AT, 'task-1', 'worker-1')
