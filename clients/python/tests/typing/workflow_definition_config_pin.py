"""Type pin: ``WorkflowDefinition.config`` takes only a ``WorkflowDefinitionConfig``.

pyright checks this file through ``pyrightconfig.clean.json``. pytest does
not collect it (no ``test_`` prefix), and nothing imports it. The config makes
an unneeded ignore comment an error, so a ``config`` declaration that accepts
another type fails the check. Class creation refuses the same value at run
time with HRS-033.
"""

from __future__ import annotations

from typing import assert_type

from horsies import (
    OnError,
    TaskNode,
    WorkflowDefinition,
    WorkflowDefinitionConfig,
)
from horsies.core.task_decorator import TaskFunction


def define(produce: TaskFunction[[], int]) -> None:
    node = TaskNode(fn=produce)
    assert_type(WorkflowDefinitionConfig(output=node), WorkflowDefinitionConfig[int])

    class Typed(WorkflowDefinition[int]):
        name = 'typing_pin_typed'
        definition_key = 'tests.typing.workflow_definition_config_pin.typed.v1'
        produce_node = node

        config = WorkflowDefinitionConfig(output=produce_node, on_error=OnError.FAIL)

    class Untyped(WorkflowDefinition[int]):
        name = 'typing_pin_untyped'
        definition_key = 'tests.typing.workflow_definition_config_pin.untyped.v1'
        produce_node = node

        config = {'output': produce_node}  # pyright: ignore[reportAssignmentType]

    del Typed, Untyped
