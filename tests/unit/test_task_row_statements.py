"""The statements of a child that write one task row: the start and the
finalizing marker.

Each reads its row by id alone, in a MATERIALIZED CTE that locks it FOR NO
KEY UPDATE and computes the fence as one boolean, and updates by that id. So
only the primary key serves them, whatever the statistics say.
"""

from __future__ import annotations

import pytest

from horsies.core.worker.child_runner import (
    MARK_FINALIZING_SQL,
    mark_finalizing_params,
    set_running_params,
    set_running_sql,
)

pytestmark = [pytest.mark.unit]


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

    params = set_running_params('task-1', 'worker-1', is_workflow_task, None)

    assert sql.count('%s') == len(params)


def test_the_marker_has_one_parameter_per_placeholder() -> None:
    params = mark_finalizing_params('task-1', 'worker-1', None)

    assert MARK_FINALIZING_SQL.count('%s') == len(params)
