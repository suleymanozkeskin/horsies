"""The params choice that a workflow start stores on a sub-workflow node.

``horsies_workflow_tasks.sub_workflow_params`` is ``GIVEN`` when the node
definition was given params for the child's ``build_with`` (its ``kwargs``),
``NOT_GIVEN`` when it was given none, and NULL on a task node. ``args_from``
does not count: the engine injects those values. The workflow start and the
child workflow start write it from the node definition.
"""

from __future__ import annotations

import uuid
from collections.abc import Callable
from typing import Any

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from horsies.core.app import Horsies
from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.models.outcome import Failed, Succeeded, TaskOutcome
from horsies.core.models.tasks import TaskError
from horsies.core.models.workflow import SubWorkflowNode, TaskNode, WorkflowDefinition
from horsies.core.types.result import Ok, Result

from tests.integration.conftest import start_ok

pytestmark = [pytest.mark.integration]


def _task(app: Horsies, prefix: str) -> Any:
    @app.task(name=f'{prefix}_{uuid.uuid4().hex[:8]}')
    def fn(*, value: int) -> Result[int, TaskError]:
        return Ok(value)

    return fn


def _child_with_param(app: Horsies) -> type[WorkflowDefinition[int]]:
    child_fn = _task(app, 'params_child_value')
    suffix = uuid.uuid4().hex[:8]

    class ChildWithParam(WorkflowDefinition[int]):
        name = f'params_child_value_{suffix}'
        definition_key = f'params-child-value-{suffix}'

        @classmethod
        def build_with(  # pyright: ignore[reportIncompatibleMethodOverride]
            cls, app: Horsies, value: int,
        ) -> Any:
            node = TaskNode(function=child_fn, kwargs={'value': value})
            return app.workflow(cls.name, [node], output=node)

    return ChildWithParam


def _child_with_result(app: Horsies) -> type[WorkflowDefinition[int]]:
    child_fn = _task(app, 'params_child_result')
    suffix = uuid.uuid4().hex[:8]

    class ChildWithResult(WorkflowDefinition[int]):
        name = f'params_child_result_{suffix}'
        definition_key = f'params-child-result-{suffix}'

        @classmethod
        def build_with(  # pyright: ignore[reportIncompatibleMethodOverride]
            cls, app: Horsies, value: TaskOutcome[int, TaskError],
        ) -> Any:
            match value:
                case Succeeded(value=succeeded_value):
                    node = TaskNode(function=child_fn, kwargs={'value': succeeded_value})
                    return app.workflow(cls.name, [node], output=node)
                case Failed(error=error):
                    raise AssertionError(f'build_with got a failed upstream: {error!r}')

    return ChildWithResult


type _ChildNodes = Callable[[Any], list[TaskNode[Any] | SubWorkflowNode[Any]]]


def _static_child(
    app: Horsies, nodes: _ChildNodes | None = None,
) -> type[WorkflowDefinition[int]]:
    child_fn = _task(app, 'params_child_static')
    suffix = uuid.uuid4().hex[:8]

    class StaticChild(WorkflowDefinition[int]):
        name = f'params_child_static_{suffix}'
        definition_key = f'params-child-static-{suffix}'

        @classmethod
        def build_with(cls, app: Horsies, *args: Any, **params: Any) -> Any:
            _ = (args, params)
            match nodes:
                case None:
                    node = TaskNode(function=child_fn, kwargs={'value': 1})
                    return app.workflow(cls.name, [node], output=node)
                case build_nodes:
                    spec_nodes = build_nodes(child_fn)
                    return app.workflow(cls.name, spec_nodes, output=spec_nodes[-1])

    return StaticChild


async def _params(session: AsyncSession, workflow_id: str) -> dict[int, str | None]:
    rows = (
        await session.execute(
            text("""
                SELECT task_index, sub_workflow_params FROM horsies_workflow_tasks
                WHERE workflow_id = :wf
            """),
            {'wf': workflow_id},
        )
    ).fetchall()
    return {row.task_index: row.sub_workflow_params for row in rows}


@pytest.mark.asyncio(loop_scope='function')
class TestSubWorkflowParams:
    async def test_the_start_stores_the_choice_of_each_node(
        self,
        clean_workflow_tables: None,
        app: Horsies,
        broker: PostgresBroker,
        session: AsyncSession,
    ) -> None:
        producer = TaskNode(function=_task(app, 'params_producer'), kwargs={'value': 1})
        given = SubWorkflowNode(workflow_def=_child_with_param(app), kwargs={'value': 2})
        not_given = SubWorkflowNode(workflow_def=_static_child(app))
        injected = SubWorkflowNode(
            workflow_def=_child_with_result(app),
            waits_for=[producer],
            args_from={'value': producer},
        )
        spec = app.workflow(
            f'params_{uuid.uuid4().hex[:6]}', [producer, given, not_given, injected],
            definition_key=f'params-{uuid.uuid4().hex[:6]}',
        )
        handle = await start_ok(spec, broker)

        assert await _params(session, str(handle.workflow_id)) == {
            0: None,
            1: 'GIVEN',
            2: 'NOT_GIVEN',
            3: 'NOT_GIVEN',
        }

    async def test_the_child_start_stores_the_choice_of_each_child_node(
        self,
        clean_workflow_tables: None,
        app: Horsies,
        broker: PostgresBroker,
        session: AsyncSession,
    ) -> None:
        grandchild = _child_with_param(app)

        def child_nodes(child_fn: Any) -> list[TaskNode[Any] | SubWorkflowNode[Any]]:
            root = TaskNode(function=child_fn, kwargs={'value': 1})
            nested = SubWorkflowNode(
                workflow_def=grandchild, kwargs={'value': 3}, waits_for=[root],
            )
            return [root, nested]

        parent = SubWorkflowNode(workflow_def=_static_child(app, child_nodes))
        spec = app.workflow(
            f'params_parent_{uuid.uuid4().hex[:6]}', [parent],
            definition_key=f'params-parent-{uuid.uuid4().hex[:6]}',
        )
        handle = await start_ok(spec, broker)
        child_id = (
            await session.execute(
                text('SELECT id FROM horsies_workflows WHERE parent_workflow_id = :p'),
                {'p': handle.workflow_id},
            )
        ).scalar_one()

        assert await _params(session, str(handle.workflow_id)) == {0: 'NOT_GIVEN'}
        assert await _params(session, str(child_id)) == {0: None, 1: 'GIVEN'}
