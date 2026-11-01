"""``rerun_task`` returns ``RerunSourceCorrupt`` for a source row that does not decode.

The source read raises ``HistoryContractError`` for a stored status that is
not a ``TaskStatus`` member, an attempt snapshot that does not decode, or an
unknown location. Stored data that does not parse is an outcome with its
cause, so the rerun returns it and writes nothing.
"""

from __future__ import annotations

from datetime import timedelta
from unittest.mock import AsyncMock, MagicMock

import pytest

from horsies.core.history.errors import HistoryContractError
from horsies.core.history.rerun import operations
from horsies.core.history.rerun.operations import (
    RerunEnqueuePolicy,
    RerunSourceCorrupt,
    RerunTask,
    rerun_task,
)

pytestmark = [pytest.mark.unit]

SOURCE_TASK_ID = '0190a1b2-c3d4-7e5f-8a6b-7c8d9e0f1a2b'


def _connection_with_a_registered_class() -> AsyncMock:
    """A connection whose retention class check finds the class."""
    registered = MagicMock()
    registered.scalar_one.return_value = True
    connection = AsyncMock()
    connection.execute = AsyncMock(return_value=registered)
    return connection


def _policy() -> RerunEnqueuePolicy:
    return RerunEnqueuePolicy(
        retention_class_key='standard',
        retain_rerun_input=True,
        reservation_window=timedelta(hours=24),
    )


class TestSourceRowDoesNotDecode:
    @pytest.mark.asyncio
    async def test_it_is_source_corrupt_with_the_cause(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        cause = HistoryContractError(
            f"history row {SOURCE_TASK_ID} has status 'NOT_A_STATUS', which is "
            'not a task status'
        )
        monkeypatch.setattr(
            operations, 'read_task_detail', AsyncMock(side_effect=cause),
        )
        connection = _connection_with_a_registered_class()

        outcome = await rerun_task(
            connection,
            RerunTask(source_task_id=SOURCE_TASK_ID, deadline=None),
            _policy(),
        )

        assert outcome == RerunSourceCorrupt(
            task_id=SOURCE_TASK_ID, exception=cause,
        )
        # Only the retention class check ran: nothing was written.
        assert connection.execute.await_count == 1
