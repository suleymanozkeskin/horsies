"""The statements of a child that write one task row: the start and the
finalizing marker.

Each reads its row by id alone, in a MATERIALIZED CTE that locks it FOR NO
KEY UPDATE and computes the fence as one boolean, and updates by that id. So
only the primary key serves them, whatever the statistics say.
"""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from horsies.core.worker.child_runner import (
    CLAIM_STILL_STARTABLE_SQL,
    MARK_FINALIZING_SQL,
    mark_finalizing_params,
    set_running_params,
    set_running_sql,
)

pytestmark = [pytest.mark.unit]

CLAIMED_AT = datetime(2026, 11, 10, 9, 0, tzinfo=timezone.utc)


def _words(sql: str) -> str:
    return ' '.join(sql.split())


@pytest.mark.parametrize(
    'sql',
    [set_running_sql(False), set_running_sql(True), MARK_FINALIZING_SQL],
    ids=['start', 'workflow start', 'marker'],
)
def test_the_row_is_read_by_id_alone_and_updated_by_that_id(sql: str) -> None:
    words = _words(sql)

    assert 'FROM horsies_tasks WHERE id = %s FOR NO KEY UPDATE' in words
    assert 'FROM target WHERE t.id = target.id AND target.' in words


@pytest.mark.parametrize('is_workflow_task', [False, True])
def test_the_start_has_one_parameter_per_placeholder(is_workflow_task: bool) -> None:
    sql = set_running_sql(is_workflow_task)

    params = set_running_params('task-1', 'worker-1', is_workflow_task, CLAIMED_AT)

    assert sql.count('%s') == len(params)


def test_the_marker_has_one_parameter_per_placeholder() -> None:
    params = mark_finalizing_params('task-1', 'worker-1', CLAIMED_AT)

    assert MARK_FINALIZING_SQL.count('%s') == len(params)


@pytest.mark.parametrize(
    'sql',
    [
        set_running_sql(False),
        set_running_sql(True),
        CLAIM_STILL_STARTABLE_SQL,
        MARK_FINALIZING_SQL,
    ],
    ids=['start', 'workflow start', 'still startable', 'marker'],
)
def test_the_fence_checks_the_claim_time_with_no_null_branch(sql: str) -> None:
    words = _words(sql)

    assert 'AND claimed_by_worker_id = %s AND claimed_at = %s::timestamptz' in words
    assert 'IS NULL OR claimed_at' not in words


@pytest.mark.parametrize('is_workflow_task', [False, True])
def test_the_start_fence_takes_the_worker_then_the_claim_time(
    is_workflow_task: bool,
) -> None:
    params = set_running_params('task-1', 'worker-1', is_workflow_task, CLAIMED_AT)

    assert params[:3] == ('worker-1', CLAIMED_AT, 'task-1')


def test_the_marker_fence_takes_the_worker_then_the_claim_time() -> None:
    params = mark_finalizing_params('task-1', 'worker-1', CLAIMED_AT)

    assert params == ('worker-1', CLAIMED_AT, 'task-1', 'worker-1')
