"""Unit tests for ``read_task_state``: the two-column read of the staged detail function."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from horsies.core.history.errors import HistoryContractError
from horsies.core.history.reads.detail import (
    HistoryTaskState,
    LiveTaskLocation,
    TaskStateAbsent,
    read_task_state,
)
from horsies.core.types.status import TaskStatus

pytestmark = [pytest.mark.unit]

TASK_ID = '3f9c1a2b-4d5e-4f60-8a71-b2c3d4e5f607'


def _connection(row: Any) -> tuple[AsyncMock, AsyncMock]:
    result = MagicMock()
    result.one_or_none.return_value = row
    connection = AsyncMock()
    connection.execute = AsyncMock(return_value=result)
    return connection, connection.execute


class TestReadTaskState:
    @pytest.mark.asyncio
    async def test_a_history_row_gives_its_status_and_workflow_flag(self) -> None:
        connection, _ = _connection(
            SimpleNamespace(location='HISTORY', status='FAILED', is_workflow_task=True),
        )

        state = await read_task_state(connection, task_id=TASK_ID)

        assert state == HistoryTaskState(
            task_id=TASK_ID, status=TaskStatus.FAILED, is_workflow_task=True,
        )

    @pytest.mark.asyncio
    async def test_a_live_row_gives_the_live_location(self) -> None:
        connection, _ = _connection(
            SimpleNamespace(location='LIVE', status=None, is_workflow_task=None),
        )

        assert await read_task_state(connection, task_id=TASK_ID) == LiveTaskLocation(
            task_id=TASK_ID,
        )

    @pytest.mark.asyncio
    async def test_no_row_gives_absent_without_a_floor_query(self) -> None:
        connection, execute = _connection(None)

        assert await read_task_state(connection, task_id=TASK_ID) == TaskStateAbsent(
            task_id=TASK_ID,
        )
        assert execute.await_count == 1

    @pytest.mark.asyncio
    async def test_a_status_that_is_not_a_member_is_a_contract_error(self) -> None:
        connection, _ = _connection(
            SimpleNamespace(location='HISTORY', status='BOGUS', is_workflow_task=False),
        )

        with pytest.raises(HistoryContractError, match='BOGUS'):
            await read_task_state(connection, task_id=TASK_ID)

    @pytest.mark.asyncio
    async def test_an_unknown_location_is_a_contract_error(self) -> None:
        connection, _ = _connection(
            SimpleNamespace(location='MOON', status='FAILED', is_workflow_task=False),
        )

        with pytest.raises(HistoryContractError, match='MOON'):
            await read_task_state(connection, task_id=TASK_ID)

    @pytest.mark.asyncio
    async def test_the_statement_reads_two_task_row_columns_only(self) -> None:
        connection, execute = _connection(None)

        await read_task_state(connection, task_id=TASK_ID)

        statement = str(execute.await_args.args[0])  # pyright: ignore[reportOptionalMemberAccess]
        assert '(detail.task_row).status' in statement
        assert '(detail.task_row).is_workflow_task' in statement
        assert '.*' not in statement
