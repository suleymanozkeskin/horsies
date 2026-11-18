"""Integration tests for SubWorkflowNode behavior."""

from __future__ import annotations

import asyncio
from typing import Annotated, Any

import pytest
import pytest_asyncio
from pydantic import Field
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from horsies.core.app import Horsies
from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.codec.json_io import loads_json
from horsies.core.codec.typed import (
    decode_task_result,
)
from horsies.core.errors import WorkflowValidationError
from horsies.core.models.tasks import (
    TaskError,
    TaskResult,
)
from horsies.core.models.workflow import (
    TaskNode,
    SubWorkflowNode,
    WorkflowDefinition,
    OnError,
)
from horsies.core.workflows.registry import (
    unregister_workflow_spec,
)

from .conftest import finish_task, make_simple_task, start_ok


# =============================================================================
# Helpers
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


def _decode_task_result(
    value: Any,
    ok_type: Any = Any,
) -> TaskResult[Any, TaskError] | None:
    """Decode a stored task-result JSON column into a TaskResult.

    Strict-serde phase 7: replaces the legacy ``task_result_from_json``
    helper. The wire is the ``__h_task_result__`` envelope; ``ok_type``
    selects the typed decode path for the ok slot (defaults to ``Any``
    when the test only inspects discriminator / error fields).
    """
    if isinstance(value, str):
        return decode_task_result(loads_json(value).unwrap(), ok_type)
    return None


def _extract_taskresult_value(
    value: TaskResult[int, TaskError],
    default: int = 0,
) -> int:
    """Unwrap a TaskResult into the underlying int for build_with paths.

    Strict-serde phase 7 (B3a): the engine passes the typed
    ``TaskResult`` instance straight to ``build_with`` for args_from
    kwargs. This helper just unwraps it.
    """
    return value.unwrap() if value.is_ok() else default


# =============================================================================
# Child WorkflowDefinitions
# =============================================================================


class ImportableChildWorkflow(WorkflowDefinition[int]):
    """Child workflow used for registry fallback tests."""

    name = 'importable_child_workflow'
    definition_key = 'tests.importable_child.v1'

    @classmethod
    def build_with(cls, app: Horsies, *args: Any, **params: Any) -> Any:
        @app.task(task_name='importable_child_task')
        def child_task() -> TaskResult[int, TaskError]:
            return TaskResult(ok=5)

        node = TaskNode(fn=child_task)
        return app.workflow(
            name=cls.name,
            tasks=[node],
            output=node,
            on_error=OnError.FAIL,
        )


class ParamChildWorkflow(WorkflowDefinition[int]):
    """Child workflow that uses build_with parameters."""

    name = 'param_child_workflow'
    definition_key = 'tests.param_child.v1'

    @classmethod
    def build_with(
        cls,
        app: Horsies,
        value: TaskResult[int, TaskError],
    ) -> Any:
        raw_value = _extract_taskresult_value(value)

        @app.task(task_name='param_child_task')
        def child_task(*, value: int) -> TaskResult[int, TaskError]:
            return TaskResult(ok=value)

        node = TaskNode(fn=child_task, kwargs={'value': raw_value})
        return app.workflow(
            name=cls.name,
            tasks=[node],
            output=node,
            on_error=OnError.FAIL,
        )


class ConstrainedChildWorkflow(WorkflowDefinition[int]):
    """Child workflow whose build_with declares a constraint on its input."""

    name = 'constrained_child_workflow'
    definition_key = 'tests.constrained_child.v1'

    @classmethod
    def build_with(
        cls,
        app: Horsies,
        value: TaskResult[Annotated[int, Field(gt=0)], TaskError],
    ) -> Any:
        raw_value = _extract_taskresult_value(value)

        @app.task(task_name='constrained_child_task')
        def child_task(*, value: int) -> TaskResult[int, TaskError]:
            return TaskResult(ok=value)

        node = TaskNode(fn=child_task, kwargs={'value': raw_value})
        return app.workflow(
            name=cls.name,
            tasks=[node],
            output=node,
            on_error=OnError.FAIL,
        )


class StartChildWorkflow(WorkflowDefinition[int]):
    name = 'child_workflow_start'
    definition_key = 'tests.start_child.v1'

    @classmethod
    def build_with(cls, app: Horsies, *args: Any, **params: Any) -> Any:
        @app.task(task_name='sub_child_task')
        def child_task() -> TaskResult[int, TaskError]:
            return TaskResult(ok=1)

        node = TaskNode(fn=child_task)
        return app.workflow(
            name=cls.name,
            tasks=[node],
            output=node,
            on_error=OnError.FAIL,
        )


class CollisionChildWorkflow(WorkflowDefinition[int]):
    """Child workflow whose ``.name`` deliberately collides with a task name.

    Used to prove that SubWorkflowNode OkT resolution goes through the
    unique ``definition_key`` (→ ``int``), not the ambiguous ``task_name``
    that also matches a registered task returning ``str``.
    """

    name = 'collision_shared_name'
    definition_key = 'tests.collision_child.v1'

    @classmethod
    def build_with(cls, app: Horsies, *args: Any, **params: Any) -> Any:
        @app.task(task_name='collision_child_inner_task')
        def child_task() -> TaskResult[int, TaskError]:
            return TaskResult(ok=42)

        node = TaskNode(fn=child_task)
        return app.workflow(
            name=cls.name,
            tasks=[node],
            output=node,
            on_error=OnError.FAIL,
        )


class MiddleWorkflow(WorkflowDefinition[int]):
    """Intermediate workflow containing a SubWorkflowNode for 3-level nesting."""

    name = 'middle_workflow'
    definition_key = 'tests.middle_workflow.v1'

    @classmethod
    def build_with(cls, app: Horsies, *args: Any, **params: Any) -> Any:
        node_child: SubWorkflowNode[int] = SubWorkflowNode(
            workflow_def=StartChildWorkflow,
        )
        return app.workflow(
            name=cls.name,
            tasks=[node_child],
            output=node_child,
            on_error=OnError.FAIL,
        )


# =============================================================================
# TestSubworkflowIntegration
# =============================================================================


@pytest.mark.integration
@pytest.mark.asyncio(loop_scope='function')
class TestSubworkflowIntegration:
    """Integration tests for subworkflow start, fallback, and summaries."""

    @pytest_asyncio.fixture
    async def setup(
        self,
        session: AsyncSession,
        broker: PostgresBroker,
        app: Horsies,
    ) -> tuple[AsyncSession, PostgresBroker, Horsies]:
        await session.execute(text('TRUNCATE horsies_workflow_tasks, horsies_workflows, horsies_tasks CASCADE'))
        await session.commit()
        return session, broker, app

    async def _get_workflow_task_row(
        self,
        session: AsyncSession,
        workflow_id: str,
        task_index: int,
    ) -> tuple[Any, ...] | None:
        res = await session.execute(
            text("""
                SELECT status, sub_workflow_id, result
                FROM horsies_workflow_tasks
                WHERE workflow_id = :wf_id AND task_index = :idx
            """),
            {'wf_id': workflow_id, 'idx': task_index},
        )
        row = res.fetchone()
        if row is None:
            return None
        return tuple(row)

    async def _complete_child_workflow(
        self,
        session: AsyncSession,
        broker: PostgresBroker,
        parent_workflow_id: str,
        parent_task_index: int,
        child_result: TaskResult[Any, TaskError],
    ) -> str:
        """Complete a child workflow's first task and return the child workflow ID."""
        row = await self._get_workflow_task_row(session, parent_workflow_id, parent_task_index)
        assert row is not None
        _, child_id, _ = row
        assert isinstance(child_id, str)

        await finish_task(session, broker, child_id, 0, child_result)
        return child_id

    # -----------------------------------------------------------------
    # Existing happy-path tests
    # -----------------------------------------------------------------


    async def test_subworkflow_okt_resolves_by_definition_key_not_colliding_task_name(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """A task whose name collides with the child workflow's ``.name``
        must NOT hijack the SubWorkflowNode's OkT resolution.

        The persisted ``task_name`` for the subworkflow row equals the
        workflow's ``.name`` (``collision_shared_name``). A task is also
        registered under that exact name returning ``str``. Pre-fix,
        ``result_for`` resolved OkT via the task registry first → wrong
        ``str`` decode. With ``sub_definition_key`` routing, it resolves
        via the unique ``definition_key`` → the workflow's ``int`` output.
        """
        session, broker, app = setup

        # Register a task whose name collides with the child workflow's name.
        @app.task(task_name='collision_shared_name')
        def colliding_task() -> TaskResult[str, TaskError]:
            return TaskResult(ok='i-am-a-string-task')

        node_child: SubWorkflowNode[int] = SubWorkflowNode(
            workflow_def=CollisionChildWorkflow,
        )
        spec = _workflow(
            app,
            name='parent_collision_workflow',
            tasks=[node_child],
            output=node_child,
        )

        handle = await start_ok(spec, broker)

        # Sanity: the persisted subworkflow row's task_name collides with
        # the registered task name, and sub_definition_key is the unique key.
        row = await session.execute(
            text(
                'SELECT task_name, sub_definition_key, is_subworkflow '
                'FROM horsies_workflow_tasks '
                'WHERE workflow_id = :wf_id AND task_index = 0'
            ),
            {'wf_id': handle.workflow_id},
        )
        meta = row.fetchone()
        assert meta is not None
        assert meta[0] == 'collision_shared_name'  # collides with the task
        assert meta[1] == 'tests.collision_child.v1'  # unique key
        assert meta[2] is True

        # Complete the child workflow's inner task (returns int 42).
        await self._complete_child_workflow(
            session, broker, handle.workflow_id, 0, TaskResult(ok=42),
        )

        # result_for must decode via the workflow's definition_key (int),
        # not the colliding task's str type.
        decoded = await asyncio.to_thread(handle.result_for, node_child.key())
        assert decoded.is_ok(), f'expected Ok(int), got {decoded}'
        assert decoded.unwrap() == 42

    async def test_subworkflow_registry_fallback(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        session, broker, app = setup

        parent_task = make_simple_task(app, 'sub_parent_task_fallback')

        node_a: TaskNode[int] = TaskNode(fn=parent_task, kwargs={'value': 2})
        node_child: SubWorkflowNode[int] = SubWorkflowNode(
            workflow_def=ImportableChildWorkflow,
            waits_for=[node_a],
        )

        spec = _workflow(app, 
            name='parent_workflow_fallback',
            tasks=[node_a, node_child],
            output=node_child,
        )

        handle = await start_ok(spec, broker)
        unregister_workflow_spec(spec.name)

        await finish_task(
            session, broker, handle.workflow_id, 0, TaskResult(ok=4)
        )

        row = await self._get_workflow_task_row(session, handle.workflow_id, 1)
        assert row is not None
        status, child_id, _ = row
        assert status == 'RUNNING'
        assert isinstance(child_id, str)


    async def test_build_with_refuses_a_value_outside_its_constraint(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """The value of `args_from` is held to the type that `build_with`
        declares, so the child never starts."""
        session, broker, app = setup

        producer_task = make_simple_task(app, 'sub_constrained_producer')

        node_a: TaskNode[int] = TaskNode(fn=producer_task, kwargs={'value': 7})
        node_child: SubWorkflowNode[int] = SubWorkflowNode(
            workflow_def=ConstrainedChildWorkflow,
            waits_for=[node_a],
            args_from={'value': node_a},
        )
        spec = _workflow(app, 
            name='parent_workflow_constrained',
            tasks=[node_a, node_child],
            output=node_child,
        )

        handle = await start_ok(spec, broker)
        await finish_task(
            session, broker, handle.workflow_id, 0, TaskResult(ok=-1)
        )

        row = await self._get_workflow_task_row(session, handle.workflow_id, 1)
        assert row is not None
        status, child_id, result_json = row
        assert status == 'FAILED'
        assert child_id is None
        failed = _decode_task_result(result_json)
        assert failed is not None and failed.is_err()
        assert 'does not fit the type' in (failed.err_value.message or '')


    async def test_depth_tracking_in_nested_workflows(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """Child workflows have correct depth values."""
        session, broker, app = setup

        node_child: SubWorkflowNode[int] = SubWorkflowNode(
            workflow_def=StartChildWorkflow,
        )

        spec = _workflow(app, 
            name='parent_depth_test',
            tasks=[node_child],
            output=node_child,
        )

        handle = await start_ok(spec, broker)

        # Check parent depth
        parent_depth = await session.execute(
            text('SELECT depth FROM horsies_workflows WHERE id = :wf_id'),
            {'wf_id': handle.workflow_id},
        )
        assert parent_depth.scalar_one() == 0

        # Check child depth
        row = await self._get_workflow_task_row(session, handle.workflow_id, 0)
        assert row is not None
        child_id = row[1]
        assert isinstance(child_id, str)

        child_depth = await session.execute(
            text('SELECT depth FROM horsies_workflows WHERE id = :wf_id'),
            {'wf_id': child_id},
        )
        assert child_depth.scalar_one() == 1


    async def test_root_workflow_id_propagation(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """Root workflow ID is correctly set for child workflows."""
        session, broker, app = setup

        node_child: SubWorkflowNode[int] = SubWorkflowNode(
            workflow_def=StartChildWorkflow,
        )

        spec = _workflow(app, 
            name='parent_root_id_test',
            tasks=[node_child],
            output=node_child,
        )

        handle = await start_ok(spec, broker)

        # Get child workflow ID
        row = await self._get_workflow_task_row(session, handle.workflow_id, 0)
        assert row is not None
        child_id = row[1]
        assert isinstance(child_id, str)

        # Check root_workflow_id
        child_row = await session.execute(
            text('SELECT root_workflow_id FROM horsies_workflows WHERE id = :wf_id'),
            {'wf_id': child_id},
        )
        root_id = child_row.scalar_one()
        assert root_id == handle.workflow_id

    # -----------------------------------------------------------------
    # New: error / non-happy paths
    # -----------------------------------------------------------------


    async def test_deeply_nested_depth_and_root_id(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """3-level nesting: parent(0) → middle(1) → child(2), depth and root_workflow_id propagate."""
        session, broker, app = setup

        node_middle: SubWorkflowNode[int] = SubWorkflowNode(
            workflow_def=MiddleWorkflow,
        )

        spec = _workflow(app, 
            name='parent_deep_nest',
            tasks=[node_middle],
            output=node_middle,
        )

        handle = await start_ok(spec, broker)

        # Parent depth = 0
        parent_depth_row = await session.execute(
            text('SELECT depth FROM horsies_workflows WHERE id = :wf_id'),
            {'wf_id': handle.workflow_id},
        )
        assert parent_depth_row.scalar_one() == 0

        # Middle workflow: depth = 1
        row = await self._get_workflow_task_row(session, handle.workflow_id, 0)
        assert row is not None
        middle_id = row[1]
        assert isinstance(middle_id, str)

        middle_depth_row = await session.execute(
            text('SELECT depth, root_workflow_id FROM horsies_workflows WHERE id = :wf_id'),
            {'wf_id': middle_id},
        )
        middle_info = middle_depth_row.fetchone()
        assert middle_info is not None
        assert middle_info[0] == 1
        assert middle_info[1] == handle.workflow_id

        # Grandchild workflow: depth = 2, same root
        grandchild_row = await session.execute(
            text("""
                SELECT sub_workflow_id FROM horsies_workflow_tasks
                WHERE workflow_id = :wf_id AND task_index = 0
            """),
            {'wf_id': middle_id},
        )
        grandchild_id_row = grandchild_row.fetchone()
        assert grandchild_id_row is not None
        grandchild_id = grandchild_id_row[0]
        assert isinstance(grandchild_id, str)

        grandchild_depth_row = await session.execute(
            text('SELECT depth, root_workflow_id FROM horsies_workflows WHERE id = :wf_id'),
            {'wf_id': grandchild_id},
        )
        grandchild_info = grandchild_depth_row.fetchone()
        assert grandchild_info is not None
        assert grandchild_info[0] == 2
        assert grandchild_info[1] == handle.workflow_id


# =============================================================================
# TestSubworkflowValidation
# =============================================================================


@pytest.mark.integration
class TestSubworkflowValidation:
    """Tests for subworkflow validation during spec creation."""

    def test_cycle_detection_mutual_reference(self, app: Horsies) -> None:
        """Mutual cycle: A references B, B references A → WorkflowValidationError."""
        # CycleB defined first (empty body → metaclass sets _workflow_nodes = [])
        class CycleB(WorkflowDefinition[int]):
            name = 'cycle_b'
            definition_key = 'tests.cycle_b.v1'

        # CycleA references CycleB via class attribute → metaclass picks this up naturally
        class CycleA(WorkflowDefinition[int]):
            name = 'cycle_a'
            definition_key = 'tests.cycle_a.v1'
            sub_b: SubWorkflowNode[int] = SubWorkflowNode(workflow_def=CycleB)

        # Back-reference: CycleB → CycleA. Must override _workflow_nodes because
        # CycleA didn't exist when CycleB's metaclass ran. This is the only way to
        # create mutual references in Python's top-down class definition order.
        CycleB._workflow_nodes = [('sub_a', SubWorkflowNode(workflow_def=CycleA))]

        parent_node: SubWorkflowNode[int] = SubWorkflowNode(workflow_def=CycleA)

        with pytest.raises(WorkflowValidationError) as exc:
            _workflow(app, 
                name='parent_cyclic',
                tasks=[parent_node],
                output=parent_node,
            )

        assert 'cycle' in str(exc.value).lower()

    def test_args_from_without_waits_for_rejected(self, app: Horsies) -> None:
        """args_from referencing a node not in waits_for raises WORKFLOW_INVALID_ARGS_FROM."""
        task_a_fn = make_simple_task(app, 'args_from_no_dep_a')

        node_a: TaskNode[int] = TaskNode(fn=task_a_fn, kwargs={'value': 1})
        node_child: SubWorkflowNode[int] = SubWorkflowNode(
            workflow_def=ParamChildWorkflow,
            # waits_for is empty, but args_from references node_a
            args_from={'value': node_a},
        )

        with pytest.raises(WorkflowValidationError) as exc:
            _workflow(app, 
                name='args_from_no_dep',
                tasks=[node_a, node_child],
                output=node_child,
            )

        assert 'args_from' in str(exc.value).lower()


# =============================================================================
# TestSubworkflowRecovery
# =============================================================================


# =============================================================================
# SubWorkflowNode.kwargs typed serde — strict-serde phase 6+ integration
# =============================================================================


from pydantic import BaseModel  # noqa: E402


class _TypedPayload(BaseModel):
    """Pydantic payload used to verify typed kwargs round-trip through DB."""

    stream_id: str
    count: int


class TypedKwargsChildWorkflow(WorkflowDefinition[int]):
    """Child workflow whose build_with takes a typed Pydantic kwarg.

    Captures the received payload on a class attribute so an integration
    test can assert the kwargs survived the lifecycle.py → DB →
    enqueue_subworkflow_task round-trip as a typed instance.
    """

    name = 'typed_kwargs_child_workflow'
    definition_key = 'tests.typed_kwargs_child.v1'

    # Set by ``build_with`` each time the engine instantiates the child.
    received_payload: '_TypedPayload | None' = None

    @classmethod
    def build_with(  # pyright: ignore[reportIncompatibleMethodOverride]
        cls,
        app: Horsies,
        *,
        payload: _TypedPayload,
    ) -> Any:
        cls.received_payload = payload

        @app.task(task_name='typed_kwargs_child_task')
        def child_task() -> TaskResult[int, TaskError]:
            return TaskResult(ok=payload.count)

        node = TaskNode(fn=child_task)
        return app.workflow(
            name=cls.name,
            tasks=[node],
            output=node,
            on_error=OnError.FAIL,
        )


@pytest.mark.integration
@pytest.mark.asyncio(loop_scope='function')
class TestSubworkflowKwargsTypedSerde:
    """SubWorkflowNode.kwargs survive DB persistence with typed shape."""

    @pytest_asyncio.fixture
    async def setup(
        self,
        session: AsyncSession,
        broker: PostgresBroker,
        app: Horsies,
    ) -> tuple[AsyncSession, PostgresBroker, Horsies]:
        await session.execute(
            text(
                'TRUNCATE horsies_workflow_tasks, horsies_workflows, '
                'horsies_tasks CASCADE',
            ),
        )
        await session.commit()
        TypedKwargsChildWorkflow.received_payload = None
        return session, broker, app

    async def test_typed_pydantic_kwarg_round_trips_to_build_with(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """Typed Pydantic kwarg flows producer → DB → engine decode → child build_with.

        Pre-fix the child's ``build_with`` received a raw ``dict``
        because lifecycle.py wrote kwargs via untyped ``dumps_json`` and
        engine.py read them back as raw. Now ``encode_subworkflow_kwargs``
        / ``decode_subworkflow_kwargs`` route the value through
        ``encode_value(payload, _TypedPayload)`` and back, so the child
        sees the model instance.
        """
        session, broker, app = setup
        original_payload = _TypedPayload(stream_id='stream-abc', count=7)

        node_child: SubWorkflowNode[int] = SubWorkflowNode(
            workflow_def=TypedKwargsChildWorkflow,
            kwargs={'payload': original_payload},
        )

        spec = _workflow(
            app,
            name='parent_typed_kwargs',
            tasks=[node_child],
            output=node_child,
        )

        await start_ok(spec, broker)

        # build_with fires at enqueue_subworkflow_task time. The child
        # workflow's build_with captured the received value.
        assert TypedKwargsChildWorkflow.received_payload is not None
        assert isinstance(
            TypedKwargsChildWorkflow.received_payload, _TypedPayload,
        )
        assert TypedKwargsChildWorkflow.received_payload == original_payload

    async def test_kwargs_persisted_as_pydantic_dump_not_dict_passthrough(
        self,
        setup: tuple[AsyncSession, PostgresBroker, Horsies],
    ) -> None:
        """The persisted ``kwargs`` JSON column holds the Pydantic dump
        of the payload (i.e. the typed encode actually fired)."""
        session, broker, app = setup
        original_payload = _TypedPayload(stream_id='stream-xyz', count=42)

        node_child: SubWorkflowNode[int] = SubWorkflowNode(
            workflow_def=TypedKwargsChildWorkflow,
            kwargs={'payload': original_payload},
        )

        spec = _workflow(
            app,
            name='parent_typed_kwargs_persistence',
            tasks=[node_child],
            output=node_child,
        )

        handle = await start_ok(spec, broker)

        res = await session.execute(
            text(
                'SELECT input FROM horsies_workflow_tasks '
                'WHERE workflow_id = :wf_id AND task_index = 0',
            ),
            {'wf_id': handle.workflow_id},
        )
        row = res.fetchone()
        assert row is not None
        input_json = row[0]
        parsed = loads_json(input_json).unwrap()
        assert isinstance(parsed, dict)
        # The Pydantic model is dumped under its declared shape.
        assert parsed == {
            'payload': {
                'stream_id': 'stream-xyz',
                'count': 42,
            },
        }
