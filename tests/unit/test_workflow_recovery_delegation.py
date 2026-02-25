"""Unit tests for recovery delegation to canonical workflow completion logic."""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from horsies.core.workflows import recovery


def _rows_result(rows: list[tuple[Any, ...]]) -> MagicMock:
    result = MagicMock()
    result.fetchall.return_value = rows
    return result


@pytest.mark.unit
class TestWorkflowRecoveryDelegation:
    """Recovery case 2+3 should reuse engine.check_workflow_completion()."""

    @pytest.mark.asyncio
    async def test_case_2_3_delegates_to_check_workflow_completion(self) -> None:
        session = AsyncMock()

        async def _execute(stmt: Any, *_args: Any, **_kwargs: Any) -> MagicMock:
            if stmt is recovery.GET_PENDING_WITH_TERMINAL_DEPS_SQL:
                return _rows_result([])
            if stmt is recovery.GET_READY_NOT_ENQUEUED_SQL:
                return _rows_result([])
            if stmt is recovery.GET_READY_SUBWORKFLOWS_NOT_STARTED_SQL:
                return _rows_result([])
            if stmt is recovery.GET_COMPLETED_CHILDREN_NOT_UPDATED_SQL:
                return _rows_result([])
            if stmt is recovery.GET_CRASHED_WORKER_TASKS_SQL:
                return _rows_result([])
            if stmt is recovery.GET_TERMINAL_WORKFLOW_CANDIDATES_SQL:
                return _rows_result([('wf-1', None, None, 0)])
            return _rows_result([])

        session.execute = AsyncMock(side_effect=_execute)
        broker = MagicMock()

        with (
            patch(
                'horsies.core.workflows.engine.try_make_ready_and_enqueue',
                new=AsyncMock(),
            ),
            patch(
                'horsies.core.workflows.engine.get_dependency_results',
                new=AsyncMock(return_value={}),
            ),
            patch(
                'horsies.core.workflows.engine.check_workflow_completion',
                new=AsyncMock(),
            ) as mock_check_completion,
        ):
            recovered = await recovery.recover_stuck_workflows(session, broker)

        assert recovered == 1
        mock_check_completion.assert_awaited_once_with(session, 'wf-1', broker)
