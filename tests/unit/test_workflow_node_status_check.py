"""The stored node status is a ``WorkflowTaskStatus`` or a crash.

The database CHECK ``horsies_workflow_tasks_status_known`` allows only the
enum values. Both lists are read from code and must stay equal.
"""

from __future__ import annotations

import re
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

import horsies._core as core
from horsies.core.models.workflow.enums import WorkflowTaskStatus
from horsies.core.models.workflow.handle import WorkflowHandle
from horsies.core.types.result import is_ok
from tests.baseline_sql import baseline_sql

pytestmark = [pytest.mark.unit]

WORKFLOW_ID = '0196f3a2-7c41-7d55-9e2b-4a1c3d5e6f70'

_STATUS_IN_LIST = re.compile(r'status\s+(?:NOT\s+)?IN\s*\(([^)]*)\)')


def _quoted_values(in_list: str) -> frozenset[str]:
    return frozenset(re.findall(r"'([^']*)'", in_list))


class TestCheckMatchesEnum:
    def test_the_baseline_check_equals_the_enum(self) -> None:
        check = re.search(
            r'CONSTRAINT horsies_workflow_tasks_status_known CHECK \((.*?)\n    \)',
            baseline_sql(),
            re.DOTALL,
        )
        assert check is not None, 'horsies_workflow_tasks_status_known'
        lists = _STATUS_IN_LIST.findall(check.group(1))
        assert len(lists) == 1, lists
        expected = frozenset(status.value for status in WorkflowTaskStatus)
        assert _quoted_values(lists[0]) == expected


def _record(record_class: type, **fields: object) -> MagicMock:
    """A stand-in for a record of ``horsies._core``, which has no Python
    constructor: a class pattern matches it, and its fields are attributes."""
    record = MagicMock(spec=record_class)
    for name, value in fields.items():
        setattr(record, name, value)
    return record


def _node_row(status: str) -> MagicMock:
    return _record(
        core.StoredNodeRecord,
        node_id='node-9',
        task_index=0,
        task_name='task',
        status=status,
        result_json=None,
        started_at=None,
        completed_at=None,
        sub_workflow_id=None,
        sub_workflow_summary_json=None,
        is_subworkflow=False,
        sub_definition_key=None,
    )


def _handle_with_rows(rows: list[MagicMock]) -> WorkflowHandle[Any]:
    """A handle whose core read of the nodes (``workflow_nodes_async``)
    answers ``rows``."""
    broker = MagicMock()
    broker._core_handle.workflow_nodes_async = AsyncMock(
        return_value=_record(core.WorkflowNodesRead, nodes=rows),
    )
    return WorkflowHandle(workflow_id=WORKFLOW_ID, broker=broker)


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
        assert WORKFLOW_ID in message
        assert 'node-9' in message
        assert "'BOGUS'" in message
