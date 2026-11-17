"""The statement of a child that writes one task row on its own pool: the
finalizing marker.

The marker is one ``horsies_mark_finalizing`` call, which reads its row by id
alone (its SQL cases pin the outcomes, and ``test_task_row_plans.py`` the
plan of the row read). The start goes through the core handle of the child
(``CoreChildHandle.start_attempt``): its statement, its parameters (the
claim generation, the pid, the host, ``worker-<pid>`` and
``<worker_id>:<pid>``) and its miss reasons are tested in the core
(``crates/horsies-core/src/broker/claims/tests.rs``), and the call that the
child sends in ``test_worker_child_runner.py`` and
``test_shared_lifecycle.py``.
"""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from horsies.core.worker.child_runner import (
    MARK_FINALIZING_SQL,
    FinalizingMark,
    mark_finalizing_params,
    read_finalizing_mark,
)

pytestmark = [pytest.mark.unit]

CLAIMED_AT = datetime(2026, 11, 10, 9, 0, tzinfo=timezone.utc)


def _words(sql: str) -> str:
    return ' '.join(sql.split())


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
