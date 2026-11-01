"""``RerunSourceAbsent.reason`` is the ``TaskAbsenceReason`` of the staged read.

A source id that no table holds gives ``RerunSourceAbsent``. Its ``reason``
is the same classification that ``get_task_detail`` reports.
"""

from __future__ import annotations

from datetime import timedelta
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from horsies.core.history.reads.detail import TaskAbsenceReason, TaskDetailAbsent
from horsies.core.history.rerun import operations
from horsies.core.history.rerun.operations import (
    RerunEnqueuePolicy,
    RerunSourceAbsent,
    RerunTask,
    rerun_task,
)

pytestmark = [pytest.mark.unit]

SOURCE_ID = '0192f0a0-0000-7000-8000-000000000001'


def _connection() -> Any:
    registered = MagicMock()
    registered.scalar_one.return_value = True
    connection = AsyncMock()
    connection.execute = AsyncMock(return_value=registered)
    return connection


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ('predates', 'reason'),
    [
        (True, TaskAbsenceReason.PREDATES_RETAINED_HISTORY),
        (False, TaskAbsenceReason.WITHIN_RETAINED_HISTORY),
        (None, TaskAbsenceReason.NOT_CLASSIFIED),
    ],
    ids=['predates', 'within', 'unclassified'],
)
async def test_the_reason_is_the_staged_classification(
    monkeypatch: pytest.MonkeyPatch,
    predates: bool | None,
    reason: TaskAbsenceReason,
) -> None:
    monkeypatch.setattr(
        operations,
        'read_task_detail',
        AsyncMock(
            return_value=TaskDetailAbsent(
                task_id=SOURCE_ID, predates_retained_floor=predates,
            ),
        ),
    )

    outcome = await rerun_task(
        _connection(),
        RerunTask(source_task_id=SOURCE_ID, deadline=None),
        RerunEnqueuePolicy(
            retention_class_key='standard_30d',
            retain_rerun_input=True,
            reservation_window=timedelta(hours=24),
        ),
    )

    assert outcome == RerunSourceAbsent(task_id=SOURCE_ID, reason=reason)
