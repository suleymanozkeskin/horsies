"""The stored node status is a ``WorkflowTaskStatus`` or a crash.

The database CHECK ``horsies_workflow_tasks_status_check`` allows only the
enum values. Both lists are read from code and must stay equal.
"""

from __future__ import annotations

import inspect
import re
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from horsies.core.brokers.resources import ListenerDisabled

from horsies.core.models.workflow.enums import WorkflowTaskStatus
from horsies.core.models.workflow.handle import WorkflowHandle
from horsies.core.schemas import node_status
from horsies.core.types.result import is_ok
from tests.broker_resources_support import attach_resources, fake_resources

pytestmark = [pytest.mark.unit]

_STATUS_IN_LIST = re.compile(r'status\s+(?:NOT\s+)?IN\s*\(([^)]*)\)')


def _quoted_values(in_list: str) -> frozenset[str]:
    return frozenset(re.findall(r"'([^']*)'", in_list))


class TestCheckMatchesEnum:
    def test_every_status_list_in_the_migration_equals_the_enum(self) -> None:
        source = inspect.getsource(node_status.close_workflow_node_status_set)
        lists = _STATUS_IN_LIST.findall(source)
        assert len(lists) == 2, 'expected the row scan and the CHECK'
        expected = frozenset(status.value for status in WorkflowTaskStatus)
        for in_list in lists:
            assert _quoted_values(in_list) == expected


def _node_row(status: str) -> SimpleNamespace:
    return SimpleNamespace(
        workflow_task_id='wt-1',
        node_id='node-9',
        task_index=0,
        task_name='task',
        status=status,
        result=None,
        started_at=None,
        completed_at=None,
        sub_workflow_id=None,
        sub_workflow_summary=None,
        is_subworkflow=False,
        sub_definition_key=None,
    )


def _handle_with_rows(rows: list[SimpleNamespace]) -> WorkflowHandle[Any]:
    node_result = MagicMock()
    node_result.fetchall.return_value = rows
    session = AsyncMock()
    session.execute = AsyncMock(return_value=node_result)
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
    return WorkflowHandle(workflow_id='wf-1', broker=broker)


class TestTasksAsync:
    @pytest.mark.asyncio
    @pytest.mark.parametrize('status', list(WorkflowTaskStatus))
    async def test_a_stored_member_value_is_the_node_status(
        self, status: WorkflowTaskStatus,
    ) -> None:
        handle = _handle_with_rows([_node_row(status.value)])

        result = await handle.tasks_async()

        assert is_ok(result)
        assert [info.status for info in result.ok_value] == [status]

    @pytest.mark.asyncio
    async def test_another_stored_value_crashes_with_its_subject(self) -> None:
        handle = _handle_with_rows([_node_row('BOGUS')])

        with pytest.raises(AssertionError) as raised:
            await handle.tasks_async()

        message = str(raised.value)
        assert 'wf-1' in message
        assert 'node-9' in message
        assert "'BOGUS'" in message
