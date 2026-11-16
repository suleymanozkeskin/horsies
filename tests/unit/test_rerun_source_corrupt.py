"""``rerun_task`` returns ``RerunSourceCorrupt`` for a source row that does not decode.

The source read (``horsies_rerun_source_read``) gives ``RerunSourceCorrupt``
with a ``HistoryContractError`` for a stored status that is not a
``TaskStatus`` member, an attempt snapshot that does not decode, or an
unknown location. Stored data that does not parse is an outcome with its
cause, so the rerun returns it and writes nothing.
"""

from __future__ import annotations

from datetime import timedelta
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from horsies.core.history.errors import HistoryContractError
from horsies.core.history.rerun.operations import (
    RerunEnqueuePolicy,
    RerunOutcome,
    RerunSourceCorrupt,
    RerunTask,
    rerun_task,
)

pytestmark = [pytest.mark.unit]

SOURCE_TASK_ID = '0190a1b2-c3d4-7e5f-8a6b-7c8d9e0f1a2b'


def _connection_with_row(row: SimpleNamespace) -> AsyncMock:
    """A connection whose ``horsies_rerun_source_read`` returns ``row``."""
    result = MagicMock()
    result.one.return_value = row
    connection = AsyncMock()
    connection.execute = AsyncMock(return_value=result)
    return connection


def _policy() -> RerunEnqueuePolicy:
    return RerunEnqueuePolicy(
        retention=timedelta(days=30),
        retain_rerun_input=True,
        reservation_window=timedelta(hours=24),
    )


def _cause_of(outcome: RerunOutcome) -> str:
    match outcome:
        case RerunSourceCorrupt(task_id=task_id, exception=HistoryContractError() as exc):
            assert task_id == SOURCE_TASK_ID
            return str(exc)
        case other:
            pytest.fail(f'expected RerunSourceCorrupt, got {other!r}')


class TestSourceRowDoesNotDecode:
    @pytest.mark.asyncio
    async def test_it_is_source_corrupt_with_the_cause(self) -> None:
        connection = _connection_with_row(
            SimpleNamespace(
                class_registered=True,
                location='HISTORY',
                absence_reason=None,
                task_id=SOURCE_TASK_ID,
                status='NOT_A_STATUS',
            )
        )

        outcome = await rerun_task(
            connection,
            RerunTask(source_task_id=SOURCE_TASK_ID, deadline=None),
            _policy(),
        )

        assert _cause_of(outcome) == (
            f"history row {SOURCE_TASK_ID} has status 'NOT_A_STATUS', which is "
            'not a task status'
        )
        # Only the source read ran: nothing was written.
        assert connection.execute.await_count == 1


class TestUnknownLocation:
    @pytest.mark.asyncio
    async def test_it_is_source_corrupt_with_the_location(self) -> None:
        connection = _connection_with_row(
            SimpleNamespace(class_registered=True, location='ELSEWHERE', absence_reason=None)
        )

        outcome = await rerun_task(
            connection,
            RerunTask(source_task_id=SOURCE_TASK_ID, deadline=None),
            _policy(),
        )

        assert _cause_of(outcome) == "staged detail returned unknown location 'ELSEWHERE'"
        assert connection.execute.await_count == 1
