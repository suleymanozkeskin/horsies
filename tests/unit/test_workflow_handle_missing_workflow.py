"""``results()`` and ``tasks()`` tell a missing workflow from an empty one.

Each method reads the workflow row LEFT JOIN its nodes in one statement. No
row means no workflow has the id: ``HandleWorkflowNotFound``. One row whose
``workflow_task_id`` is NULL means the workflow exists and no node matched:
an empty ``Ok``.
"""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from horsies.core.brokers.resources import ListenerDisabled
from horsies.core.models.workflow.handle import (
    GET_WORKFLOW_TASK_RESULTS_SQL,
    GET_WORKFLOW_TASKS_SQL,
    WorkflowHandle,
)
from horsies.core.models.workflow.handle_types import HandleWorkflowNotFound
from horsies.core.types.result import is_err, is_ok
from tests.broker_resources_support import attach_resources, fake_resources

pytestmark = [pytest.mark.unit]

WORKFLOW_ID = '5d1e2f3a-4b5c-4d6e-8f70-8192a3b4c5d6'

# The row of a workflow that has no matching node: every node column of the
# LEFT JOIN is NULL.
NO_NODE_ROW = SimpleNamespace(
    workflow_task_id=None,
    node_id=None,
    task_index=None,
    task_name=None,
    status=None,
    result=None,
    started_at=None,
    completed_at=None,
    sub_workflow_id=None,
    sub_workflow_summary=None,
    is_subworkflow=None,
    sub_definition_key=None,
)


def _handle(statement: Any, rows: list[Any]) -> WorkflowHandle[Any]:
    """A handle whose read of ``statement`` returns ``rows``."""
    node_result = MagicMock()
    node_result.fetchall.return_value = rows

    def execute(executed: Any, *_args: Any, **_kwargs: Any) -> MagicMock:
        assert executed is statement, f'unexpected statement: {executed}'
        return node_result

    session = AsyncMock()
    session.execute = AsyncMock(side_effect=execute)
    context = AsyncMock()
    context.__aenter__ = AsyncMock(return_value=session)
    context.__aexit__ = AsyncMock(return_value=False)
    broker = MagicMock()
    attach_resources(
        broker,
        fake_resources(
            session_factory=MagicMock(return_value=context),
            listener=ListenerDisabled(),
        ),
    )
    return WorkflowHandle(workflow_id=WORKFLOW_ID, broker=broker)


class TestResults:
    @pytest.mark.asyncio
    async def test_no_workflow_row_is_workflow_not_found(self) -> None:
        handle = _handle(GET_WORKFLOW_TASK_RESULTS_SQL, rows=[])

        result = await handle.results_async()

        assert is_err(result)
        error = result.err_value
        assert isinstance(error, HandleWorkflowNotFound)
        assert error.workflow_id == WORKFLOW_ID

    @pytest.mark.asyncio
    async def test_a_workflow_without_a_stored_result_is_an_empty_map(self) -> None:
        handle = _handle(GET_WORKFLOW_TASK_RESULTS_SQL, rows=[NO_NODE_ROW])

        result = await handle.results_async()

        assert is_ok(result)
        assert result.ok_value == {}


class TestTasks:
    @pytest.mark.asyncio
    async def test_no_workflow_row_is_workflow_not_found(self) -> None:
        handle = _handle(GET_WORKFLOW_TASKS_SQL, rows=[])

        result = await handle.tasks_async()

        assert is_err(result)
        error = result.err_value
        assert isinstance(error, HandleWorkflowNotFound)
        assert error.workflow_id == WORKFLOW_ID

    @pytest.mark.asyncio
    async def test_a_workflow_without_a_node_is_an_empty_list(self) -> None:
        handle = _handle(GET_WORKFLOW_TASKS_SQL, rows=[NO_NODE_ROW])

        result = await handle.tasks_async()

        assert is_ok(result)
        assert result.ok_value == []


class TestStatements:
    @pytest.mark.parametrize(
        'statement',
        [GET_WORKFLOW_TASK_RESULTS_SQL, GET_WORKFLOW_TASKS_SQL],
        ids=['results', 'tasks'],
    )
    def test_the_node_read_starts_from_the_workflow_row(self, statement: Any) -> None:
        sql = ' '.join(str(statement).split())

        assert 'FROM horsies_workflows w LEFT JOIN horsies_workflow_tasks wt' in sql
        assert 'wt.id AS workflow_task_id' in sql
        assert 'WHERE w.id = :wf_id' in sql
