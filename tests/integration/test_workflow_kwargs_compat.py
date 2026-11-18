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
from horsies.core.models.tasks import TaskResult, TaskError
from horsies.core.models.workflow import (
    TaskNode,
    SubWorkflowNode,
    WorkflowDefinition,
    OnError,
    WorkflowValidationError,
)
from horsies.core.errors import ErrorCode

from .conftest import finish_task, make_simple_task, start_ok


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


class KwargsChildWorkflow(WorkflowDefinition[int]):
    """Child workflow for the args_from contract: ``build_with`` receives a
    ``TaskResult`` injected by the engine when an upstream node completes.

    Static producer kwargs are rejected for this signature under strict-serde
    (``TaskResult``-typed params are engine-populated); the sibling
    ``StaticKwargsChildWorkflow`` is the persistence-shape variant.
    """

    name = 'kwargs_child_workflow'
    definition_key = 'tests.kwargs_child.v1'

    # Track last received params for test assertions
    last_params: dict[str, Any] = {}

    @classmethod
    def build_with(
        cls,
        app: Horsies,
        value: TaskResult[int, TaskError],
    ) -> Any:
        cls.last_params = {'value': value}

        @app.task(task_name='kwargs_child_task')
        def child_task(*, value: int) -> TaskResult[int, TaskError]:
            return TaskResult(ok=value)

        raw_value = value.unwrap() if value.is_ok() else 0

        node = TaskNode(fn=child_task, kwargs={'value': raw_value})
        return app.workflow(
            name=cls.name,
            tasks=[node],
            output=node,
            on_error=OnError.FAIL,
        )


class StaticKwargsChildWorkflow(WorkflowDefinition[int]):
    """Child workflow for the static-kwargs persistence-shape contract:
    ``build_with`` takes a plain ``int`` supplied by the producer via
    ``SubWorkflowNode(kwargs={'value': <int>})``.

    The sibling ``KwargsChildWorkflow`` is the args_from variant — those
    two contracts are deliberately separated under strict-serde so
    ``TaskResult[...]`` only ever means engine-injected.
    """

    name = 'static_kwargs_child_workflow'
    definition_key = 'tests.static_kwargs_child.v1'

    @classmethod
    def build_with(cls, app: Horsies, value: int) -> Any:
        @app.task(task_name='static_kwargs_child_task')
        def child_task(*, value: int) -> TaskResult[int, TaskError]:
            return TaskResult(ok=value)

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

        assert exc.value.code == ErrorCode.WORKFLOW_POSITIONAL_ARGS_NOT_SUPPORTED


@pytest.mark.integration
@pytest.mark.asyncio(loop_scope='function')
class TestSubworkflowKwargsOnly:
    """Verify subworkflow build_with receives kwargs for new writes."""

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
        KwargsChildWorkflow.last_params = {}
        return session, broker, app

    async def test_subworkflow_build_with_receives_kwargs(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """When upstream completes, subworkflow build_with gets args_from as kwargs."""
        session, broker, app = setup

        producer = make_simple_task(app, 'kwargs_producer')

        node_a: TaskNode[int] = TaskNode(fn=producer, kwargs={'value': 7})
        node_child: SubWorkflowNode[int] = SubWorkflowNode(
            workflow_def=KwargsChildWorkflow,
            waits_for=[node_a],
            args_from={'value': node_a},
        )

        spec = _workflow(app, 
            name='kwargs_subwf_params_test',
            tasks=[node_a, node_child],
            output=node_child,
        )

        handle = await start_ok(spec, broker)

        # Complete upstream task to trigger subworkflow start
        await finish_task(session, broker, str(handle.workflow_id), 0, TaskResult(ok=7))
        await session.commit()

        # Subworkflow should be RUNNING
        child_row = await session.execute(
            text("""
                SELECT status, sub_workflow_id
                FROM horsies_workflow_tasks
                WHERE workflow_id = :wf_id AND task_index = 1
            """),
            {'wf_id': handle.workflow_id},
        )
        child_status_row = child_row.fetchone()
        assert child_status_row is not None
        assert child_status_row[0] == 'RUNNING'
        assert child_status_row[1] is not None  # child workflow created

        # Verify build_with received kwargs (value key present)
        assert 'value' in KwargsChildWorkflow.last_params


@pytest.mark.integration
@pytest.mark.asyncio(loop_scope='function')
class TestRecoveryKwargsCompat:
    """Verify recovery works with kwargs-only rows."""

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

    async def test_recovery_kwargs_only_workflow(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """Recovery handles a stuck workflow with kwargs-only rows."""
        session, broker, app = setup
        task_fn = make_simple_task(app, 'recovery_kwargs')

        node = TaskNode(fn=task_fn, kwargs={'value': 5})
        spec = _workflow(app, 
            name='recovery_kwargs_test',
            tasks=[node],
            output=node,
        )

        handle = await start_ok(spec, broker)

        # Simulate stuck: all tasks done, workflow not finalized
        await session.execute(
            text("""
                UPDATE horsies_workflow_tasks
                SET status = 'COMPLETED', result = '{"ok": 10}'
                WHERE workflow_id = :wf_id AND task_index = 0
            """),
            {'wf_id': handle.workflow_id},
        )
        await session.execute(
            text("""
                UPDATE horsies_workflows
                SET status = 'RUNNING', completed_at = NULL, result = NULL
                WHERE id = :wf_id
            """),
            {'wf_id': handle.workflow_id},
        )
        await session.commit()

        from horsies.core.workflows.recovery import recover_stuck_workflows_global
        from tests.broker_resources_support import current_resources

        recovered = (
            await recover_stuck_workflows_global(
                current_resources(broker).session_factory, broker,
            )
        ).recovered
        await session.commit()

        assert recovered == 1

        wf_result = await session.execute(
            text('SELECT status FROM horsies_workflows WHERE id = :wf_id'),
            {'wf_id': handle.workflow_id},
        )
        wf_row = wf_result.fetchone()
        assert wf_row is not None
        assert wf_row[0] == 'COMPLETED'

    async def test_recovery_ready_kwargs_task_enqueued(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """Recovery re-enqueues a READY kwargs-only task that wasn't dispatched."""
        session, broker, app = setup
        task_fn = make_simple_task(app, 'recovery_ready_kwargs')

        node = TaskNode(fn=task_fn, kwargs={'value': 3})
        spec = _workflow(app, 
            name='recovery_ready_kwargs_test',
            tasks=[node],
            output=node,
        )

        handle = await start_ok(spec, broker)

        # Simulate crash: set task to READY but clear task_id
        await session.execute(
            text("""
                UPDATE horsies_workflow_tasks
                SET status = 'READY', task_id = NULL
                WHERE workflow_id = :wf_id AND task_index = 0
            """),
            {'wf_id': handle.workflow_id},
        )
        await session.commit()

        from horsies.core.workflows.recovery import recover_stuck_workflows_global
        from tests.broker_resources_support import current_resources

        recovered = (
            await recover_stuck_workflows_global(
                current_resources(broker).session_factory, broker,
            )
        ).recovered
        await session.commit()

        assert recovered == 1

        # Task should now be ENQUEUED
        result = await session.execute(
            text("""
                SELECT status, task_id FROM horsies_workflow_tasks
                WHERE workflow_id = :wf_id AND task_index = 0
            """),
            {'wf_id': handle.workflow_id},
        )
        row = result.fetchone()
        assert row is not None
        assert row[0] == 'ENQUEUED'
        assert row[1] is not None

        # Verify the dispatched task's input holds the kwargs with value
        task_result = await session.execute(
            text('SELECT input FROM horsies_tasks WHERE id = :tid'),
            {'tid': row[1]},
        )
        task_row = task_result.fetchone()
        assert task_row is not None

        dispatched_input = loads_json(task_row[0]).unwrap()

        assert isinstance(dispatched_input, dict)
        assert dispatched_input.get('value') == 3
