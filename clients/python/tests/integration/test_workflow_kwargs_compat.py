"""Regression tests for kwargs-only workflow engine writes.

Verifies:
- A workflow node stores its kwargs as the JSON object in ``input``.
- Subworkflow build_with receives only kwargs.
- Recovery handles kwargs-only rows.
"""

from __future__ import annotations

from typing import Any

import pytest
import pytest_asyncio
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from horsies.core.app import Horsies
from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.codec.json_io import loads_json
from horsies.core.models.tasks import TaskError
from horsies.core.models.workflow import (
    TaskNode,
    SubWorkflowNode,
    WorkflowDefinition,
    OnError,
    WorkflowValidationError,
)
from horsies.core.errors import HorsiesErrorCode
from horsies.core.types.result import Ok, Result

from .conftest import make_simple_task, start_ok


# =============================================================================
# Child workflow for subworkflow kwargs tests
# =============================================================================


def _workflow(
    app: Horsies,
    *,
    name: str,
    tasks: list[Any],
    **kwargs: Any,
) -> Any:
    return app.workflow(
        name=name,
        tasks=tasks,
        definition_key=f'tests.integration.{name}.v1',
        **kwargs,
    )


class StaticKwargsChildWorkflow(WorkflowDefinition[int]):
    """Child workflow for the static-kwargs persistence-shape contract:
    ``build_with`` takes a plain ``int`` supplied by the producer via
    ``SubWorkflowNode(kwargs={'value': <int>})``.

    The sibling ``KwargsChildWorkflow`` is the args_from variant — those
    two contracts are deliberately separated under strict-serde so
    ``TaskOutcome[...]`` only ever means engine-injected.
    """

    name = 'static_kwargs_child_workflow'
    definition_key = 'tests.static_kwargs_child.v1'

    @classmethod
    def build_with(cls, app: Horsies, value: int) -> Any:
        @app.task(task_name='static_kwargs_child_task')
        def child_task(*, value: int) -> Result[int, TaskError]:
            return Ok(value)

        node = TaskNode(fn=child_task, kwargs={'value': value})
        return app.workflow(
            name=cls.name,
            tasks=[node],
            output=node,
            on_error=OnError.FAIL,
        )


# =============================================================================
# Tests
# =============================================================================


@pytest.mark.integration
@pytest.mark.asyncio(loop_scope='function')
class TestKwargsOnlyWrites:
    """Verify that new workflow writes use kwargs-only persistence."""

    @pytest_asyncio.fixture
    async def setup(
        self,
        session: AsyncSession,
        broker: PostgresBroker,
        app: Horsies,
    ) -> tuple[AsyncSession, PostgresBroker, Horsies]:
        await session.execute(
            text(
                'TRUNCATE horsies_workflow_tasks, horsies_workflows, horsies_tasks CASCADE'
            )
        )
        await session.commit()
        return session, broker, app

    async def test_new_tasknode_persists_empty_task_args(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """A TaskNode stores its kwargs as the JSON object in input."""
        session, broker, app = setup
        task_fn = make_simple_task(app, 'kwargs_simple')

        node = TaskNode(fn=task_fn, kwargs={'value': 42})
        spec = _workflow(app, 
            name='kwargs_write_test',
            tasks=[node],
            output=node,
        )

        handle = await start_ok(spec, broker)

        # The node input is the kwargs object.
        result = await session.execute(
            text("""
                SELECT input
                FROM horsies_workflow_tasks
                WHERE workflow_id = :wf_id AND task_index = 0
            """),
            {'wf_id': handle.workflow_id},
        )
        row = result.fetchone()
        assert row is not None

        node_input = loads_json(row[0]).unwrap()

        assert isinstance(node_input, dict)
        assert node_input.get('value') == 42

    async def test_new_subworkflow_node_persists_empty_task_args(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """A SubWorkflowNode stores its kwargs as the JSON object in input."""
        session, broker, app = setup

        node_child: SubWorkflowNode[int] = SubWorkflowNode(
            workflow_def=StaticKwargsChildWorkflow,
            kwargs={'value': 99},
        )

        spec = _workflow(app, 
            name='kwargs_subwf_write_test',
            tasks=[node_child],
            output=node_child,
        )

        handle = await start_ok(spec, broker)

        result = await session.execute(
            text("""
                SELECT input
                FROM horsies_workflow_tasks
                WHERE workflow_id = :wf_id AND task_index = 0
            """),
            {'wf_id': handle.workflow_id},
        )
        row = result.fetchone()
        assert row is not None

        node_input = loads_json(row[0]).unwrap()

        assert isinstance(node_input, dict)
        assert node_input.get('value') == 99

    async def test_positional_args_guard_raises(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """TaskNode with non-empty args fails spec validation immediately."""
        session, broker, app = setup
        task_fn = make_simple_task(app, 'kwargs_guard_task')
        _ = (session, broker)

        # Manually set args to bypass any constructor ban
        node = TaskNode(fn=task_fn, kwargs={'value': 1})
        node.args = (5,)  # Force positional args

        with pytest.raises(WorkflowValidationError) as exc:
            _workflow(app, 
                name='kwargs_guard_test',
                tasks=[node],
                output=node,
            )

        assert exc.value.code == HorsiesErrorCode.WORKFLOW_POSITIONAL_ARGS_NOT_SUPPORTED


