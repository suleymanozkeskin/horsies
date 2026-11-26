"""``RerunSourceAbsent.reason`` is the ``TaskAbsenceReason`` of the staged read.

A source id that no table holds gives ``RerunSourceAbsent``. Its ``reason``
is the value of ``horsies_task_absence_reason`` in the row of
``horsies_rerun_source_read``, the classification that ``get_task_detail``
reports. A value outside ``TaskAbsenceReason`` is a corrupt source.
"""

from __future__ import annotations

from datetime import timedelta
from typing import Any, Final
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID

import pytest

from horsies import TaskAbsenceReason
from horsies.core.history.rerun.operations import (
    RerunCommand,
    RerunEnqueuePolicy,
    RerunSourceAbsent,
    RerunSourceCorrupt,
    rerun_task_in_async,
)

pytestmark = [pytest.mark.unit]

SOURCE_ID = UUID('0192f0a0-0000-7000-8000-000000000001')


_READ_COLUMNS: Final = (
    'class_registered', 'client_family', 'location', 'absence_reason', 'task_id',
    'task_name', 'queue_name', 'priority', 'max_retries', 'status', 'is_workflow_task',
    'rerun_root_task_id', 'attempt_archive_version', 'attempt_snapshot',
    'attempt_snapshot_digest', 'rerun_input_disposition', 'rerun_input_version',
    'rerun_input_digest', 'rerun_input_inline',
)
"""The result columns of the core's source read, in row order."""


def _read_row(**values: object) -> tuple[object, ...]:
    """A source read row of a database of the Python family: ``values`` by
    column name, every other column ``None``."""
    named = {'client_family': 'PYTHON', **values}
    return tuple(named.get(column) for column in _READ_COLUMNS)


def _connection(absence_reason: str) -> Any:
    """A connection whose ``horsies_rerun_source_read`` row is an absent source."""
    result = MagicMock()
    result.one.return_value = _read_row(
        class_registered=True, location='ABSENT', absence_reason=absence_reason,
    )
    connection = AsyncMock()
    connection.execute = AsyncMock(return_value=result)
    return connection


def _policy() -> RerunEnqueuePolicy:
    return RerunEnqueuePolicy(
        retention=timedelta(days=30),
        retain_rerun_input=True,
        reservation_window=timedelta(hours=24),
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    'reason',
    list(TaskAbsenceReason),
    ids=lambda reason: reason.value,
)
async def test_the_reason_is_the_staged_classification(
    reason: TaskAbsenceReason,
) -> None:
    outcome = await rerun_task_in_async(
        _connection(reason.value),
        RerunCommand(source_task_id=SOURCE_ID, good_until=None),
        _policy(),
    )

    assert outcome == RerunSourceAbsent(task_id=SOURCE_ID, reason=reason)


@pytest.mark.asyncio
async def test_a_reason_outside_the_set_is_a_corrupt_source() -> None:
    outcome = await rerun_task_in_async(
        _connection('PURGED'),
        RerunCommand(source_task_id=SOURCE_ID, good_until=None),
        _policy(),
    )

    match outcome:
        case RerunSourceCorrupt(task_id=task_id, detail=detail):
            assert task_id == SOURCE_ID
            assert detail == (
                f"absence reason of {SOURCE_ID} is 'PURGED', which is not a task absence reason"
            )
        case other:
            pytest.fail(f'expected RerunSourceCorrupt, got {other!r}')
