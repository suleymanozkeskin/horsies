"""``start_workflow_async`` on the core: what it sends and how it maps each answer.

The spec crosses as workflow spec data in a ``WorkflowStartRequest``; the
core answers ``WorkflowStarted``, ``WorkflowExists``, ``WorkflowIdTaken`` or
``WorkflowStartFailed``. Prevalidation stays in Python, before any core
call. The Python start statements are tested on the reference caller
(``test_workflow_start_catches.py``, ``test_workflow_start_retry_loop.py``).
"""

from __future__ import annotations

import json
import logging
import uuid
from datetime import datetime, timezone
from typing import Any, assert_never
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from pydantic import SecretStr

import horsies._core as core
from horsies._catalog.workflow_spec import SpecOutputNode
from horsies.core.app import Horsies
from horsies.core.models.app import AppConfig
from horsies.core.models.broker import PostgresConfig
from horsies.core.models.queues import QueueMode
from horsies.core.models.tasks import TaskError, TaskResult
from horsies.core.models.workflow import (
    OnError,
    SubWorkflowNode,
    SuccessCase,
    SuccessPolicy,
    TaskNode,
    WorkflowDefinition,
    WorkflowSpec,
)
from horsies.core.types.core_failure import CoreOperationFailed
from horsies.core.codec.json_io import SerializationError
from horsies.core.types.result import Err, Ok, is_err, is_ok
from horsies.core.workflows.lifecycle import start_workflow_async
from horsies.core.workflows.registry import register_workflow_definition
from horsies.core.workflows.spec_data import workflow_spec_data, workflow_spec_json
from horsies.core.workflows.start_tree import top_node_rows
from horsies.core.workflows.start_types import (
    WorkflowStartEnqueueFailed,
    WorkflowStartIdConflict,
    WorkflowStartValidationFailed,
)

pytestmark = [pytest.mark.unit]

APP = Horsies(
    AppConfig(
        queue_mode=QueueMode.DEFAULT,
        broker=PostgresConfig(database_url=SecretStr('postgresql://u@127.0.0.1:1/unused')),
    ),
)

WORKFLOW_ID = '3c1f5e7a-9b2d-4c6e-8f0a-1b3d5f7a9c2e'
SENT_AT = datetime(2026, 11, 17, 12, 0, tzinfo=timezone.utc)


@APP.task(task_name='start_core.value')
def value_task(*, value: int) -> TaskResult[int, TaskError]:
    return TaskResult(ok=value)


class StartCoreChild(WorkflowDefinition[int]):
    """A child with one task node."""

    name = 'start_core.child'
    definition_key = 'start_core.child.v1'

    @classmethod
    def build_with(cls, app: Horsies, *args: Any, **params: Any) -> WorkflowSpec[int]:
        _ = (args, params)
        node = TaskNode(fn=value_task, kwargs={'value': 1})
        return app.workflow(cls.name, [node], output=node, definition_key=cls.definition_key)


@pytest.fixture(autouse=True)
def registered_child() -> None:
    """The child definition, registered again: other tests clear the registry."""
    register_workflow_definition(StartCoreChild)


def _spec() -> WorkflowSpec[Any]:
    first = TaskNode(fn=value_task, kwargs={'value': 1})
    second = TaskNode(fn=value_task, kwargs={'value': 2})
    child = SubWorkflowNode(workflow_def=StartCoreChild, waits_for=[first])
    joined = TaskNode(
        fn=value_task,
        kwargs={'value': 3},
        waits_for=[first, second],
        join='quorum',
        min_success=1,
        allow_failed_deps=True,
    )
    return APP.workflow(
        'start_core.flow',
        [first, second, child, joined],
        output=joined,
        on_error=OnError.PAUSE,
        success_policy=SuccessPolicy(cases=[SuccessCase(required=[joined])], optional=[child]),
        definition_key='start_core.flow.v1',
    )


def _spec_json(spec: WorkflowSpec[Any]) -> str:
    """The spec data text of ``spec`` with the app's node rows."""
    match workflow_spec_json(spec, top_node_rows(APP.get_broker(), spec)):
        case Ok(ok_value=text):
            return text
        case Err(err_value=refused):
            raise AssertionError(f'the spec data does not encode: {refused}')
        case _ as unreachable:
            assert_never(unreachable)


def _broker(answer: object = None, *, raises: BaseException | None = None) -> Any:
    """The app's broker with a core handle that answers the start with ``answer``."""
    broker = APP.get_broker()
    handle = MagicMock()
    match raises:
        case None:
            handle.start_workflow_async = AsyncMock(return_value=answer)
        case _:
            handle.start_workflow_async = AsyncMock(side_effect=raises)
    broker._core_handle = handle  # pyright: ignore[reportPrivateUsage]
    return broker


def _record(record_class: type, **fields: object) -> MagicMock:
    """A stand-in for a record of ``horsies._core``: a class pattern matches it."""
    record = MagicMock(spec=record_class)
    record.workflow_id = uuid.UUID(WORKFLOW_ID)
    for name, value in fields.items():
        setattr(record, name, value)
    return record


class TestSpecData:
    def test_the_spec_data_holds_the_rows_that_the_start_stores(self) -> None:
        spec = _spec()
        data = workflow_spec_data(spec, top_node_rows(APP.get_broker(), spec))

        assert data.name == 'start_core.flow'
        assert data.on_error == 'PAUSE'
        assert data.output == SpecOutputNode(index=3)
        assert [node.index for node in data.nodes] == [0, 1, 2, 3]
        assert data.nodes[2].body.kind == 'SUB_WORKFLOW'
        assert data.nodes[3].join.kind == 'QUORUM'
        assert data.nodes[3].allow_failed_deps is True
        assert data.queue_classes == {'default': 'standard_30d'}
        policy = json.loads(_spec_json(spec))['success_policy']
        assert policy == {'kind': 'CASES', 'cases': [{'required': [3]}], 'optional': [2]}

    def test_the_core_reads_the_spec_data(self) -> None:
        spec = _spec()
        request = core.WorkflowStartRequest(
            spec_json=_spec_json(spec),
            workflow_id=uuid.UUID(WORKFLOW_ID),
            sent_at=SENT_AT,
            resend_on_transient_err=False,
        )
        assert isinstance(request, core.WorkflowStartRequest)

    def test_spec_data_with_missing_fields_is_refused_at_the_root(self) -> None:
        """serde reports a missing field at its container, so the path is
        the root. The refusal holds the error class and its position, never
        a text of the data."""
        with pytest.raises(ValueError) as refused:
            core.WorkflowStartRequest(
                spec_json='{"data_version": 1}',
                workflow_id=uuid.UUID(WORKFLOW_ID),
                sent_at=SENT_AT,
                resend_on_transient_err=False,
            )

        assert str(refused.value) == (
            'workflow spec data refused at .: '
            'value does not fit the field at line 1 column 19'
        )

    def test_a_field_of_the_wrong_type_is_refused_at_its_path(self) -> None:
        with pytest.raises(ValueError) as refused:
            core.WorkflowStartRequest(
                spec_json='{"data_version": 1, "nodes": "one"}',
                workflow_id=uuid.UUID(WORKFLOW_ID),
                sent_at=SENT_AT,
                resend_on_transient_err=False,
            )

        assert str(refused.value) == (
            'workflow spec data refused at nodes: '
            'value does not fit the field at line 1 column 34'
        )


class TestStartAnswers:
    @pytest.mark.asyncio(loop_scope='function')
    async def test_started_is_the_handle_and_the_request_carries_the_start(self) -> None:
        broker = _broker(_record(core.WorkflowStarted))
        with patch('horsies._core.WorkflowStartRequest') as request_class:
            result = await start_workflow_async(
                _spec(), broker, WORKFLOW_ID, SENT_AT, resend_on_transient_err=True,
            )

        assert is_ok(result)
        assert result.ok_value.workflow_id == WORKFLOW_ID
        arguments = request_class.call_args.kwargs
        assert arguments['workflow_id'] == uuid.UUID(WORKFLOW_ID)
        assert arguments['sent_at'] == SENT_AT
        assert arguments['resend_on_transient_err'] is True
        assert json.loads(arguments['spec_json'])['name'] == 'start_core.flow'
        broker._core_handle.start_workflow_async.assert_awaited_once_with(  # pyright: ignore[reportPrivateUsage]
            request_class.return_value,
        )

    @pytest.mark.asyncio(loop_scope='function')
    async def test_an_existing_workflow_of_the_same_name_is_its_handle(
        self, caplog: pytest.LogCaptureFixture,
    ) -> None:
        broker = _broker(_record(core.WorkflowExists))
        with caplog.at_level(logging.WARNING):
            result = await start_workflow_async(_spec(), broker, WORKFLOW_ID, SENT_AT)

        assert is_ok(result)
        assert f'Workflow {WORKFLOW_ID} already exists' in caplog.text

    @pytest.mark.asyncio(loop_scope='function')
    async def test_an_id_taken_by_another_name_is_the_id_conflict(self) -> None:
        broker = _broker(_record(core.WorkflowIdTaken, existing_name='other.flow'))
        result = await start_workflow_async(_spec(), broker, WORKFLOW_ID, SENT_AT)

        assert is_err(result)
        error = result.err_value
        assert isinstance(error, WorkflowStartIdConflict)
        assert error.existing_workflow_name == 'other.flow'
        assert error.workflow_id == WORKFLOW_ID

    @pytest.mark.asyncio(loop_scope='function')
    async def test_a_failure_record_is_the_enqueue_failure_with_its_retry_rule(self) -> None:
        broker = _broker(
            _record(core.WorkflowStartFailed, message='connection refused', retryable=True),
        )
        result = await start_workflow_async(_spec(), broker, WORKFLOW_ID, SENT_AT)

        assert is_err(result)
        error = result.err_value
        assert isinstance(error, WorkflowStartEnqueueFailed)
        assert error.retryable is True
        assert error.message == 'connection refused'
        assert isinstance(error.exception, CoreOperationFailed)

    @pytest.mark.asyncio(loop_scope='function')
    async def test_a_core_boundary_error_is_the_enqueue_failure(self) -> None:
        broker = _broker(raises=core.TooManyPendingCalls('too many calls'))
        result = await start_workflow_async(_spec(), broker, WORKFLOW_ID, SENT_AT)

        assert is_err(result)
        error = result.err_value
        assert isinstance(error, WorkflowStartEnqueueFailed)
        assert error.retryable is True
        assert isinstance(error.exception, core.TooManyPendingCalls)

    @pytest.mark.asyncio(loop_scope='function')
    async def test_spec_data_that_does_not_encode_is_refused_with_no_core_call(self) -> None:
        broker = _broker(_record(core.WorkflowStarted))
        node = TaskNode(fn=value_task, kwargs={'value': 1})
        spec = APP.workflow(
            'start_core.\ud800', [node], output=node, definition_key='start_core.surrogate.v1',
        )
        result = await start_workflow_async(spec, broker, WORKFLOW_ID, SENT_AT)

        assert is_err(result)
        error = result.err_value
        assert isinstance(error, WorkflowStartValidationFailed)
        assert isinstance(error.exception, SerializationError)
        broker._core_handle.start_workflow_async.assert_not_awaited()  # pyright: ignore[reportPrivateUsage]

    @pytest.mark.asyncio(loop_scope='function')
    async def test_a_refused_prevalidation_makes_no_core_call(self) -> None:
        broker = _broker(_record(core.WorkflowStarted))
        result = await start_workflow_async(_spec(), broker, 'not-a-uuid', SENT_AT)

        assert is_err(result)
        assert isinstance(result.err_value, WorkflowStartValidationFailed)
        broker._core_handle.start_workflow_async.assert_not_awaited()  # pyright: ignore[reportPrivateUsage]
