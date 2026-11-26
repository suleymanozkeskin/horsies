"""``results()`` and ``tasks()`` tell a missing workflow from an empty one.

The core reads the workflow row LEFT JOIN its nodes in one statement and
answers ``WorkflowAbsent`` when no workflow has the id, or the node records
(none when the workflow exists and no node matched). The handle maps
``WorkflowAbsent`` to ``HandleWorkflowNotFound`` and no node to an empty
``Ok``.
"""

from __future__ import annotations

import uuid

from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

import horsies._core as core
from horsies.core.models.workflow.handle import WorkflowHandle
from horsies.core.models.workflow.handle_types import HandleWorkflowNotFound
from horsies.core.types.result import is_err, is_ok

pytestmark = [pytest.mark.unit]

WORKFLOW_ID = '5d1e2f3a-4b5c-4d6e-8f70-8192a3b4c5d6'


def _record(record_class: type, **fields: object) -> MagicMock:
    """A stand-in for a record of ``horsies._core``: a class pattern matches it."""
    record = MagicMock(spec=record_class)
    for name, value in fields.items():
        setattr(record, name, value)
    return record


def _handle(method: str, answer: object) -> WorkflowHandle[Any]:
    """A handle whose core handle answers ``method`` with ``answer``."""
    broker = MagicMock()
    setattr(broker._core_handle, method, AsyncMock(return_value=answer))
    return WorkflowHandle(workflow_id=uuid.UUID(WORKFLOW_ID), broker=broker)


class TestResults:
    @pytest.mark.asyncio
    async def test_no_workflow_row_is_workflow_not_found(self) -> None:
        handle = _handle('workflow_node_results_async', _record(core.WorkflowAbsent))

        result = await handle.results_async()

        assert is_err(result)
        error = result.err_value
        assert isinstance(error, HandleWorkflowNotFound)
        assert error.workflow_id == WORKFLOW_ID

    @pytest.mark.asyncio
    async def test_a_workflow_without_a_stored_result_is_an_empty_map(self) -> None:
        handle = _handle(
            'workflow_node_results_async',
            _record(core.WorkflowNodeResultsRead, nodes=[]),
        )

        result = await handle.results_async()

        assert is_ok(result)
        assert result.ok_value == {}


class TestTasks:
    @pytest.mark.asyncio
    async def test_no_workflow_row_is_workflow_not_found(self) -> None:
        handle = _handle('workflow_nodes_async', _record(core.WorkflowAbsent))

        result = await handle.nodes_async()

        assert is_err(result)
        error = result.err_value
        assert isinstance(error, HandleWorkflowNotFound)
        assert error.workflow_id == WORKFLOW_ID

    @pytest.mark.asyncio
    async def test_a_workflow_without_a_node_is_an_empty_list(self) -> None:
        handle = _handle('workflow_nodes_async', _record(core.WorkflowNodesRead, nodes=[]))

        result = await handle.nodes_async()

        assert is_ok(result)
        assert result.ok_value == []
