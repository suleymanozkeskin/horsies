"""``rerun_task`` through the core's rerun steps: the read it sends on the
caller's connection, and the preconditions it raises for.

The connection is a mock whose source read returns a row in the core's
column order; the core functions of ``horsies._core`` run as shipped.
"""

from __future__ import annotations

from datetime import timedelta
from typing import Final
from unittest.mock import AsyncMock, MagicMock
from uuid import UUID

import pytest

from horsies.core.history.rerun.operations import (
    RerunEnqueuePolicy,
    RerunSourceLive,
    RerunTask,
    rerun_task,
)
from horsies._core import CorePreconditionFailed

pytestmark = [pytest.mark.unit]

SOURCE_ID: Final = UUID('0192f0a0-0000-7000-8000-000000000001')
_READ_COLUMNS: Final = (
    'class_registered', 'client_family', 'location', 'absence_reason', 'task_id',
    'task_name', 'queue_name', 'priority', 'max_retries', 'status', 'is_workflow_task',
    'rerun_root_task_id', 'attempt_archive_version', 'attempt_snapshot',
    'attempt_snapshot_digest', 'rerun_input_disposition', 'rerun_input_version',
    'rerun_input_digest', 'rerun_input_inline',
)


def _connection(**values: object) -> AsyncMock:
    """A connection whose source read row holds ``values`` by column name
    (the Python family unless given), every other column ``None``."""
    named = {'client_family': 'PYTHON', **values}
    result = MagicMock()
    result.one.return_value = tuple(named.get(column) for column in _READ_COLUMNS)
    connection = AsyncMock()
    connection.execute = AsyncMock(return_value=result)
    return connection


def _policy() -> RerunEnqueuePolicy:
    return RerunEnqueuePolicy(
        retention=timedelta(days=30),
        retain_rerun_input=True,
        reservation_window=timedelta(hours=24),
    )


async def _rerun(connection: AsyncMock) -> object:
    return await rerun_task(
        connection, RerunTask(source_task_id=SOURCE_ID, deadline=None), _policy()
    )


@pytest.mark.asyncio
async def test_the_read_binds_the_source_and_the_class_on_the_callers_connection() -> None:
    connection = _connection(class_registered=True, location='LIVE')

    outcome = await _rerun(connection)

    assert outcome == RerunSourceLive(task_id=SOURCE_ID)
    assert connection.execute.await_count == 1
    statement, parameters = connection.execute.await_args.args
    assert 'horsies_rerun_source_read(CAST(:p1 AS uuid), CAST(:p2 AS text))' in str(statement)
    assert parameters == {'p1': SOURCE_ID, 'p2': 'standard_30d'}


@pytest.mark.asyncio
async def test_an_unregistered_class_raises_before_any_write() -> None:
    connection = _connection(class_registered=False, location='LIVE')

    with pytest.raises(ValueError, match="retention class 'standard_30d' is not registered"):
        _ = await _rerun(connection)

    assert connection.execute.await_count == 1


@pytest.mark.asyncio
@pytest.mark.parametrize('stored', ['RUST', 'TYPESCRIPT', None])
async def test_a_database_of_another_family_raises_before_any_write(stored: str | None) -> None:
    connection = _connection(class_registered=True, client_family=stored, location='LIVE')

    with pytest.raises(CorePreconditionFailed) as raised:
        _ = await _rerun(connection)

    assert raised.value.subject == 'rerun_task'
    assert raised.value.retryable is False
    assert 'PYTHON' in raised.value.message
    assert connection.execute.await_count == 1
