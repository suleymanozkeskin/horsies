"""Container kwargs on a workflow node reach the database through the real start."""

from __future__ import annotations

import dataclasses
from typing import Any

import pytest
import pytest_asyncio
from pydantic import BaseModel
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from horsies import JsonValue
from horsies.core.app import Horsies
from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.codec.json_io import loads_json
from horsies.core.models.tasks import TaskError, TaskResult
from horsies.core.models.workflow import (
    OnError,
    SubWorkflowNode,
    TaskNode,
    WorkflowDefinition,
)
from horsies.core.workflows.registry import (
    register_workflow_definition,
    unregister_workflow_definition,
)

from .conftest import start_ok


class _Item(BaseModel):
    n: int


@dataclasses.dataclass
class _Point:
    x: int


_ROWS: list[tuple[str, Any, Any, Any]] = [
    ('list_int', list[int], [1, 2], [1, 2]),
    ('dict_int', dict[str, int], {'a': 1}, {'a': 1}),
    ('json_list', JsonValue, [1, 'a', None], [1, 'a', None]),
    ('json_dict', JsonValue, {'a': [1]}, {'a': [1]}),
    ('list_model', list[_Item], [_Item(n=1)], [{'n': 1}]),
    ('list_dataclass', list[_Point], [_Point(x=1)], [{'x': 1}]),
    ('dict_of_list', dict[str, list[int]], {'a': [1]}, {'a': [1]}),
]


def _task(app: Horsies, name: str, annotation: Any) -> Any:
    def take(*, value: Any) -> TaskResult[int, TaskError]:
        return TaskResult(ok=1)

    take.__annotations__ = {
        'value': annotation,
        'return': TaskResult[int, TaskError],
    }
    return app.task(task_name=name)(take)


def _stored(value: object) -> object:
    return loads_json(value).unwrap() if isinstance(value, str) else value


async def _node_kwargs(
    session: AsyncSession,
    workflow_id: str,
    task_index: int,
) -> object:
    row = (
        await session.execute(
            text("""
                SELECT task_kwargs FROM horsies_workflow_tasks
                WHERE workflow_id = :wf_id AND task_index = :idx
            """),
            {'wf_id': workflow_id, 'idx': task_index},
        )
    ).fetchone()
    assert row is not None
    return _stored(row[0])


async def _enqueued_kwargs(
    session: AsyncSession,
    workflow_id: str,
    task_index: int,
) -> object:
    row = (
        await session.execute(
            text("""
                SELECT t.kwargs FROM horsies_tasks t
                JOIN horsies_workflow_tasks wt ON wt.task_id = t.id
                WHERE wt.workflow_id = :wf_id AND wt.task_index = :idx
            """),
            {'wf_id': workflow_id, 'idx': task_index},
        )
    ).fetchone()
    assert row is not None
    stored = _stored(row[0])
    assert isinstance(stored, dict)
    return {
        key: value for key, value in stored.items() if key != '__h_workflow_meta__'
    }


class _GrandchildWorkflow(WorkflowDefinition[int]):
    name = 'container_kwargs_grandchild'
    definition_key = 'tests.container_kwargs_grandchild.v1'

    @classmethod
    def build_with(cls, app: Horsies, values: list[int]) -> Any:
        @app.task(task_name='container_kwargs_grandchild_task')
        def grandchild_task(*, values: list[int]) -> TaskResult[int, TaskError]:
            return TaskResult(ok=len(values))

        node = TaskNode(fn=grandchild_task, kwargs={'values': values})
        return app.workflow(
            name=cls.name,
            tasks=[node],
            output=node,
            on_error=OnError.FAIL,
            definition_key=cls.definition_key,
        )


class _ChildWorkflow(WorkflowDefinition[int]):
    name = 'container_kwargs_child'
    definition_key = 'tests.container_kwargs_child.v1'

    @classmethod
    def build_with(cls, app: Horsies, values: list[int]) -> Any:
        @app.task(task_name='container_kwargs_child_task')
        def child_task(*, values: list[int]) -> TaskResult[int, TaskError]:
            return TaskResult(ok=len(values))

        node = TaskNode(fn=child_task, kwargs={'values': values})
        nested: SubWorkflowNode[int] = SubWorkflowNode(
            workflow_def=_GrandchildWorkflow,
            kwargs={'values': values},
        )
        return app.workflow(
            name=cls.name,
            tasks=[node, nested],
            output=node,
            on_error=OnError.FAIL,
            definition_key=cls.definition_key,
        )


@pytest.mark.integration
@pytest.mark.asyncio(loop_scope='function')
class TestContainerKwargsAtStart:
    @pytest_asyncio.fixture
    async def setup(
        self,
        session: AsyncSession,
        broker: PostgresBroker,
        app: Horsies,
    ) -> tuple[AsyncSession, PostgresBroker, Horsies]:
        await session.execute(
            text('TRUNCATE horsies_workflow_tasks, horsies_workflows, horsies_tasks CASCADE'),
        )
        await session.commit()
        return session, broker, app

    @pytest.mark.parametrize(
        ('label', 'annotation', 'value', 'wire'),
        _ROWS,
        ids=[row[0] for row in _ROWS],
    )
    async def test_a_root_task_node_stores_its_kwargs(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
        label: str,
        annotation: Any,
        value: Any,
        wire: Any,
    ) -> None:
        session, broker, app = setup
        node: TaskNode[int] = TaskNode(
            fn=_task(app, f'container_{label}', annotation),
            kwargs={'value': value},
        )
        spec = app.workflow(
            name=f'container_{label}',
            tasks=[node],
            output=node,
            definition_key=f'tests.container.{label}.v1',
        )

        handle = await start_ok(spec, broker)

        assert await _node_kwargs(session, handle.workflow_id, 0) == {'value': wire}
        assert await _enqueued_kwargs(session, handle.workflow_id, 0) == {'value': wire}

    async def test_a_sub_workflow_node_and_its_children_store_their_kwargs(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """The three start paths: a root sub node, a child task node and a child
        sub node."""
        session, broker, app = setup
        register_workflow_definition(_GrandchildWorkflow)
        register_workflow_definition(_ChildWorkflow)
        try:
            node: SubWorkflowNode[int] = SubWorkflowNode(
                workflow_def=_ChildWorkflow,
                kwargs={'values': [1, 2]},
            )
            spec = app.workflow(
                name='container_parent',
                tasks=[node],
                output=node,
                definition_key='tests.container.parent.v1',
            )

            handle = await start_ok(spec, broker)

            assert await _node_kwargs(session, handle.workflow_id, 0) == {'values': [1, 2]}
            child_row = (
                await session.execute(
                    text("""
                        SELECT sub_workflow_id FROM horsies_workflow_tasks
                        WHERE workflow_id = :wf_id AND task_index = 0
                    """),
                    {'wf_id': handle.workflow_id},
                )
            ).fetchone()
            assert child_row is not None and child_row[0] is not None
            child_id = child_row[0]

            assert await _node_kwargs(session, child_id, 0) == {'values': [1, 2]}
            assert await _enqueued_kwargs(session, child_id, 0) == {'values': [1, 2]}
            assert await _node_kwargs(session, child_id, 1) == {'values': [1, 2]}
        finally:
            unregister_workflow_definition(_ChildWorkflow.definition_key)
            unregister_workflow_definition(_GrandchildWorkflow.definition_key)
