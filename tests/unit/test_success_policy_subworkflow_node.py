"""A ``SubWorkflowNode`` is valid in ``SuccessCase.required`` and ``SuccessPolicy.optional``.

Each reader of the success policy accepts the node: spec validation, the copy
that remaps references to node copies (``WorkflowSpec`` and
``WorkflowDefinition.build``), the serializer at workflow start, the
serializer at child workflow start, and ``evaluate_workflow_success``.
"""

from __future__ import annotations

import json
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from pydantic import SecretStr

from horsies.core.app import Horsies
from horsies.core.errors import ErrorCode, WorkflowValidationError
from horsies.core.models.app import AppConfig
from horsies.core.models.broker import PostgresConfig
from horsies.core.models.tasks import TaskError, TaskResult
from horsies.core.models.workflow import (
    SubWorkflowNode,
    SuccessCase,
    SuccessPolicy,
    TaskNode,
    WorkflowDefinition,
    WorkflowDefinitionConfig,
    WorkflowSpec,
)
from horsies.core.task_decorator import TaskFunction
from horsies.core.types.result import Ok
from horsies.core.workflows.engine import (
    enqueue_subworkflow_task,
    evaluate_workflow_success,
)
from horsies.core.workflows.lifecycle import start_workflow_async
from horsies.core.workflows.sql import (
    ENQUEUE_SUBWORKFLOW_TASK_SQL,
    GET_TASK_STATUSES_SQL,
    INSERT_CHILD_WORKFLOW_SQL,
    INSERT_WORKFLOW_SQL,
)
from tests.broker_resources_support import attach_resources, fake_resources

pytestmark = [pytest.mark.unit]

DATABASE_URL = 'postgresql+psycopg://u:p@localhost:5432/db'
WORKFLOW_ID = '9f1b7a2c-0d4e-4f61-8a23-5c6d7e8f9a0b'
_key_counter = 0


def _key(label: str) -> str:
    global _key_counter
    _key_counter += 1
    return f'tests.unit.test_success_policy_subworkflow_node.{label}.v{_key_counter}'


def _app_with_tasks() -> tuple[Horsies, TaskFunction[..., int]]:
    app = Horsies(AppConfig(
        broker=PostgresConfig(database_url=SecretStr(DATABASE_URL)),
    ))

    @app.task('success_policy_sub.work')
    def work() -> TaskResult[int, TaskError]:
        return TaskResult(ok=1)

    return app, work


def _child_definition(work: TaskFunction[..., int]) -> type[WorkflowDefinition[int]]:
    class Child(WorkflowDefinition[int]):
        name = 'success_policy_sub_child'
        definition_key = _key('child')
        step = TaskNode(fn=work)

        config = WorkflowDefinitionConfig(output=step)

    return Child


class TestSpecValidation:
    def test_a_subworkflow_node_in_required_and_optional_is_accepted(self) -> None:
        app, work = _app_with_tasks()
        child = SubWorkflowNode(workflow_def=_child_definition(work))
        other = TaskNode(fn=work)
        policy = SuccessPolicy(
            cases=[SuccessCase(required=[child])],
            optional=[other],
        )

        spec = app.workflow(
            name='sp_accept',
            tasks=[child, other],
            success_policy=policy,
            definition_key=_key('accept'),
        )

        assert spec.success_policy is not None

    def test_a_subworkflow_node_outside_the_workflow_is_refused(self) -> None:
        app, work = _app_with_tasks()
        inside = TaskNode(fn=work)
        outside = SubWorkflowNode(workflow_def=_child_definition(work))
        policy = SuccessPolicy(cases=[SuccessCase(required=[outside])])

        with pytest.raises(WorkflowValidationError, match='not in workflow') as raised:
            app.workflow(
                name='sp_refuse',
                tasks=[inside],
                success_policy=policy,
                definition_key=_key('refuse'),
            )

        assert raised.value.code == ErrorCode.WORKFLOW_INVALID_SUCCESS_POLICY


class TestSpecCopy:
    def test_the_policy_points_to_the_copy_of_a_subworkflow_node(self) -> None:
        app, work = _app_with_tasks()
        child = SubWorkflowNode(workflow_def=_child_definition(work))
        optional_child = SubWorkflowNode(workflow_def=_child_definition(work))
        policy = SuccessPolicy(
            cases=[SuccessCase(required=[child])],
            optional=[optional_child],
        )

        spec: WorkflowSpec[Any] = WorkflowSpec(
            name='sp_copy', tasks=[child, optional_child], success_policy=policy,
        )

        assert spec.success_policy is not None
        assert spec.success_policy.cases[0].required[0] is spec.tasks[0]
        assert spec.success_policy.cases[0].required[0] is not child
        assert spec.success_policy.optional is not None
        assert spec.success_policy.optional[0] is spec.tasks[1]
        _ = app


class TestDefinitionCopy:
    def test_build_remaps_a_subworkflow_node_in_the_policy_to_the_copy(self) -> None:
        app, work = _app_with_tasks()
        child_definition = _child_definition(work)

        class Parent(WorkflowDefinition[None]):
            name = 'success_policy_sub_parent'
            definition_key = _key('parent')
            child = SubWorkflowNode(workflow_def=child_definition)
            extra = TaskNode(fn=work)

            config = WorkflowDefinitionConfig(
                success_policy=SuccessPolicy(
                    cases=[SuccessCase(required=[child])],
                    optional=[child],
                ),
            )

        spec: WorkflowSpec[Any] = Parent.build(app)

        assert spec.success_policy is not None
        copied_child = next(
            node for node in spec.tasks if isinstance(node, SubWorkflowNode)
        )
        assert copied_child is not Parent.child
        assert spec.success_policy.cases[0].required[0] is copied_child
        assert spec.success_policy.optional is not None
        assert spec.success_policy.optional[0] is copied_child


class _StopAfterInsert(Exception):
    """Raised by the recording session to end the start after the insert."""


class TestStartSerialization:
    @pytest.mark.asyncio
    async def test_start_stores_the_index_of_a_subworkflow_node(self) -> None:
        app, work = _app_with_tasks()
        task_node = TaskNode(fn=work)
        child = SubWorkflowNode(workflow_def=_child_definition(work))
        optional_child = SubWorkflowNode(workflow_def=_child_definition(work))
        policy = SuccessPolicy(
            cases=[SuccessCase(required=[task_node, child])],
            optional=[optional_child],
        )
        spec = app.workflow(
            name='sp_start',
            definition_key=_key('start'),
            tasks=[task_node, child, optional_child],
            success_policy=policy,
        )
        params_by_statement: dict[Any, Any] = {}

        async def execute(statement: Any, params: Any = None) -> MagicMock:
            params_by_statement[statement] = params
            raise _StopAfterInsert

        session = AsyncMock()
        session.execute = AsyncMock(side_effect=execute)
        session_context = MagicMock()
        session_context.__aenter__ = AsyncMock(return_value=session)
        session_context.__aexit__ = AsyncMock(return_value=False)
        broker = MagicMock()
        broker.ensure_schema_initialized = AsyncMock(return_value=Ok(True))
        attach_resources(
            broker,
            fake_resources(session_factory=MagicMock(return_value=session_context)),
        )

        with pytest.raises(_StopAfterInsert):
            await start_workflow_async(spec, broker, WORKFLOW_ID)

        stored = json.loads(params_by_statement[INSERT_WORKFLOW_SQL]['success_policy'])
        assert stored == {
            'cases': [{'required_indices': [0, 1]}],
            'optional_indices': [2],
        }


class _ParamRecordingSession:
    def __init__(self, rows: dict[Any, Any]) -> None:
        self.rows = rows
        self.params: dict[Any, Any] = {}
        self.session = AsyncMock()
        self.session.execute = AsyncMock(side_effect=self._dispatch)
        self.session.info = {}

    async def _dispatch(self, statement: Any, params: Any = None) -> MagicMock:
        self.params[statement] = params
        result = MagicMock()
        configured = self.rows.get(statement, SimpleNamespace())
        match configured:
            case list():
                result.fetchone.return_value = configured.pop(0)
            case row:
                result.fetchone.return_value = row
        return result


def _grandchild_node(index: int) -> MagicMock:
    node = MagicMock(spec=SubWorkflowNode)
    node.name = f'child_sub_{index}'
    node.args = ()
    node.kwargs = {}
    node.waits_for = []
    node.args_from = {}
    node.workflow_ctx_from = None
    node.index = index
    node.node_id = f'child-{index}'
    node.allow_failed_deps = False
    node.join = 'all'
    node.min_success = None
    node.workflow_def = MagicMock()
    node.workflow_def.name = 'GrandchildWorkflow'
    return node


class TestChildStartSerialization:
    @pytest.mark.asyncio
    async def test_child_start_stores_the_index_of_a_subworkflow_node(self) -> None:
        required_node = _grandchild_node(0)
        optional_node = _grandchild_node(1)
        child_spec = MagicMock()
        child_spec.tasks = [required_node, optional_node]
        child_spec.success_policy = SuccessPolicy(
            cases=[SuccessCase(required=[required_node])],
            optional=[optional_node],
        )
        child_spec.output = None
        child_spec.on_error = MagicMock(value='fail')
        child_spec.definition_key = 'test.child.v1'
        child_spec.name = 'child_wf'
        workflow_def = MagicMock()
        workflow_def.build_with.return_value = child_spec
        workflow_def.name = 'ChildWorkflow'
        workflow_def._original_build_with = workflow_def.build_with  # pyright: ignore[reportPrivateUsage]
        parent_row = SimpleNamespace(
            id='wt-1',
            sub_workflow_name='child_wf',
            task_args='[]',
            task_kwargs=None,
            args_from=None,
            node_id='node-0',
            sub_definition_key='test.child.v1',
            parent_workflow_name='test_wf',
        )
        recorder = _ParamRecordingSession(
            # A SubWorkflowNode root starts its own child after the insert;
            # the second row is None, so that start stops.
            {ENQUEUE_SUBWORKFLOW_TASK_SQL: [parent_row, None, None]},
        )
        broker = MagicMock()
        broker.app = MagicMock()
        broker.app.config.queue_mode.name = 'DEFAULT'

        with patch(
            'horsies.core.workflows.engine._load_workflow_def_from_key',
            return_value=workflow_def,
        ), patch(
            'horsies.core.workflows.engine.validate_workflow_generic_output_match',
        ), patch(
            'horsies.core.workflows.engine.encode_subworkflow_kwargs',
            return_value={},
        ), patch(
            'horsies.core.workflows.engine._fail_enqueued_task', new=AsyncMock(),
        ):
            _ = await enqueue_subworkflow_task(
                recorder.session, broker, 'parent-wf', 0, {},
                parent_depth=0, root_workflow_id='parent-wf',
            )

        stored = json.loads(recorder.params[INSERT_CHILD_WORKFLOW_SQL]['success_policy'])
        assert stored == {
            'cases': [{'required_indices': [0]}],
            'optional_indices': [1],
        }


class TestEvaluateSuccess:
    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ('status', 'expected'),
        [('COMPLETED', True), ('FAILED', False), ('SKIPPED', False)],
    )
    async def test_the_status_of_a_subworkflow_node_row_decides_the_case(
        self, status: str, expected: bool,
    ) -> None:
        # Index 1 is the row of a SubWorkflowNode; the stored policy names it
        # in the same `required_indices` list as a TaskNode row.
        policy = {'cases': [{'required_indices': [0, 1]}]}

        async def execute(statement: Any, params: Any) -> MagicMock:
            assert statement is GET_TASK_STATUSES_SQL
            result = MagicMock()
            result.fetchall.return_value = [
                SimpleNamespace(task_index=0, status='COMPLETED'),
                SimpleNamespace(task_index=1, status=status),
            ]
            return result

        session = AsyncMock()
        session.execute = AsyncMock(side_effect=execute)

        assert await evaluate_workflow_success(
            session, 'wf-1', policy, False, 0,
        ) is expected
