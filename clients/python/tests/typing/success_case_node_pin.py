"""Type pin: ``SuccessCase.required`` and ``SuccessPolicy.optional`` take any node.

pyright checks this file through ``pyrightconfig.clean.json``. pytest does
not collect it (no ``test_`` prefix), and nothing imports it. A ``SubWorkflowNode``
goes into both fields with no cast and no ignore comment. The config makes an
unneeded ignore comment an error, so the last line, which passes a value that
is not a node, must keep its ignore.
"""

from __future__ import annotations

from horsies import (
    SubWorkflowNode,
    SuccessCase,
    SuccessPolicy,
    TaskNode,
    WorkflowDefinition,
    WorkflowDefinitionConfig,
)
from horsies.core.task_decorator import TaskFunction


def define(produce: TaskFunction[[], int]) -> None:
    class Child(WorkflowDefinition[int]):
        name = 'typing_pin_success_child'
        definition_key = 'tests.typing.success_case_node_pin.child.v1'
        step = TaskNode(fn=produce)

        config = WorkflowDefinitionConfig(output=step)

    task_node = TaskNode(fn=produce)
    sub_node = SubWorkflowNode(workflow_def=Child)

    SuccessCase(required=[sub_node])
    SuccessCase(required=[task_node, sub_node])
    SuccessPolicy(cases=[SuccessCase(required=[task_node])], optional=[sub_node])
    SuccessCase(required=['not a node'])  # pyright: ignore[reportArgumentType]
