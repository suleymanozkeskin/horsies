"""A ``SubWorkflowNode`` is valid in ``SuccessCase.required`` and ``SuccessPolicy.optional``.

Each reader of the success policy accepts the node: spec validation, the copy
that remaps references to node copies (``WorkflowSpec`` and
``WorkflowDefinition.build``), the serializer at workflow start, the
serializer at child workflow start, and ``evaluate_workflow_success``.
"""

from __future__ import annotations

from typing import Any

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


pytestmark = [pytest.mark.unit]

DATABASE_URL = 'postgresql://u:p@localhost:5432/db'
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


