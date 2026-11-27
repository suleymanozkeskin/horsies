"""A workflow whose only node is a sub-workflow, as a ``WorkflowStartRequest``,
and a child spec source that panics.

The start asks the child spec source of the handle for the child of that
node. The spec data comes from the Python client's builder; the app is not
connected and builds no handle of its own for the start.
"""

from __future__ import annotations

import uuid
from datetime import UTC, datetime
from typing import Any, Final, assert_never

from pydantic import SecretStr

from horsies._core import (
    ChildSpecBuilt,
    ChildSpecRefused,
    ChildSpecRequest,
    NoTraceCarrier,
    PanicException,
    WorkflowStartRequest,
)
from horsies.core.app import Horsies
from horsies.core.models.app import AppConfig
from horsies.core.models.broker import PostgresConfig
from horsies.core.models.queues import QueueMode
from horsies.core.models.tasks import TaskError
from horsies.core.models.workflow import SubWorkflowNode, TaskNode, WorkflowDefinition, WorkflowSpec
from horsies.core.types.result import Err, Ok, Result
from horsies.core.workflows.spec_data import workflow_spec_json
from horsies.core.workflows.start_tree import top_node_rows

PARENT_NAME: Final = 'tests.extension.parent'

APP: Final = Horsies(
    AppConfig(
        queue_mode=QueueMode.DEFAULT,
        database=PostgresConfig(
            database_url=SecretStr('postgresql://extension@127.0.0.1:1/never_connected'),
        ),
    ),
)


@APP.task(name='tests.extension.value')
def value_task(*, value: int) -> Result[int, TaskError]:
    return Ok(value)


class ExtensionChild(WorkflowDefinition[int]):
    """The child definition of the sub-workflow node."""

    name = 'tests.extension.child'
    definition_key = 'tests.extension.child.v1'

    @classmethod
    def build_with(cls, app: Horsies, *args: Any, **params: Any) -> WorkflowSpec[int]:
        _ = (args, params)
        node = TaskNode(function=value_task, kwargs={'value': 1})
        return app.workflow(cls.name, [node], output=node, definition_key=cls.definition_key)


def subworkflow_start_request(workflow_id: uuid.UUID) -> WorkflowStartRequest:
    """The start of ``PARENT_NAME`` under ``workflow_id``: one sub-workflow
    node of ``ExtensionChild``, which is also the output.

    Raises:
        RuntimeError: the spec data does not encode (a defect of the builder).
    """
    child: SubWorkflowNode[int] = SubWorkflowNode(workflow_def=ExtensionChild)
    spec = APP.workflow(
        PARENT_NAME,
        [child],
        output=child,
        definition_key='tests.extension.parent.v1',
    )
    match workflow_spec_json(spec, top_node_rows(APP.get_broker(), spec)):
        case Ok(ok_value=spec_json):
            pass
        case Err(err_value=refused):
            raise RuntimeError(f'the spec data of {PARENT_NAME} does not encode: {refused}')
        case _ as unreachable:
            assert_never(unreachable)
    return WorkflowStartRequest(
        spec_json=spec_json,
        workflow_id=workflow_id,
        sent_at=datetime.now(UTC),
        resend_on_transient_error=False,
        trace=NoTraceCarrier(),
    )


class PanickingChildSpecSource:
    """A child spec source that raises ``PanicException(text)`` on each call:
    a defect of the client, so the operation that asked panics. Counts its
    calls."""

    def __init__(self, text: str) -> None:
        self._text = text
        self.calls = 0

    def __call__(self, request: ChildSpecRequest) -> ChildSpecBuilt | ChildSpecRefused:
        _ = request
        self.calls += 1
        raise PanicException(self._text)
