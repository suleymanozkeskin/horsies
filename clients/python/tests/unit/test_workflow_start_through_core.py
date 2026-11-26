"""``start_workflow_async`` on the core: what it sends and how it maps each answer.

The spec crosses as workflow spec data in a ``WorkflowStartRequest``; the
core answers ``WorkflowStarted`` or ``WorkflowExists``, or the generated
``WorkflowStartIdConflict``, ``WorkflowStartCommandConflict`` or
``WorkflowStartEnqueueFailed`` that it built, which the start returns as they
are. Prevalidation stays in Python, before
any core call. The core writes the start and resends it
(``core/src/broker/workflow_start.rs``).
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
from horsies.core.models.tasks import TaskError
from horsies.core.models.workflow import (
    OnError,
    SubWorkflowNode,
    SuccessCase,
    SuccessPolicy,
    TaskNode,
    WorkflowDefinition,
    WorkflowSpec,
)
from horsies.core.codec.json_io import SerializationError
from horsies.core.types.result import Err, Ok, Result, is_err, is_ok
from horsies.core.workflows.lifecycle import start_workflow_async
from horsies.core.workflows.registry import register_workflow_definition
from horsies.core.workflows.spec_data import workflow_spec_data, workflow_spec_json
from horsies.core.workflows.start_tree import top_node_rows
from horsies.core.workflows.start_types import (
    WorkflowStartCommandConflict,
    WorkflowStartEnqueueFailed,
    WorkflowStartErrorCode,
    WorkflowStartIdConflict,
    WorkflowStartValidationFailed,
)

pytestmark = [pytest.mark.unit]

APP = Horsies(
    AppConfig(
        queue_mode=QueueMode.DEFAULT,
        database=PostgresConfig(database_url=SecretStr('postgresql://u@127.0.0.1:1/unused')),
    ),
)

WORKFLOW_ID = '3c1f5e7a-9b2d-4c6e-8f0a-1b3d5f7a9c2e'
SENT_AT = datetime(2026, 11, 17, 12, 0, tzinfo=timezone.utc)


@APP.task(task_name='start_core.value')
def value_task(*, value: int) -> Result[int, TaskError]:
    return Ok(value)


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
        allow_failed_dependencies=True,
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
        assert data.nodes[3].allow_failed_dependencies is True
        assert data.queue_classes == {'default': 'standard_30d'}
        policy = json.loads(_spec_json(spec))['success_policy']
        assert policy == {'kind': 'CASES', 'cases': [{'required': [3]}], 'optional': [2]}

    def test_the_core_reads_the_spec_data(self) -> None:
        spec = _spec()
        request = core.WorkflowStartRequest(
            spec_json=_spec_json(spec),
            workflow_id=uuid.UUID(WORKFLOW_ID),
            sent_at=SENT_AT,
            resend_on_transient_error=False,
            trace=core.NoTraceCarrier(),
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
                resend_on_transient_error=False,
                trace=core.NoTraceCarrier(),
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
                resend_on_transient_error=False,
                trace=core.NoTraceCarrier(),
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
                _spec(), broker, WORKFLOW_ID, SENT_AT, resend_on_transient_error=True,
            )

        assert is_ok(result)
        assert result.ok_value.workflow_id == uuid.UUID(WORKFLOW_ID)
        arguments = request_class.call_args.kwargs
        assert arguments['workflow_id'] == uuid.UUID(WORKFLOW_ID)
        assert arguments['sent_at'] == SENT_AT
        assert arguments['resend_on_transient_error'] is True
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
        conflict = WorkflowStartIdConflict(
            message=(
                f"workflow_id '{WORKFLOW_ID}' already exists for workflow name "
                "'other.flow', cannot start workflow name 'start_core.flow' with the same id"
            ),
            workflow_name='start_core.flow',
            workflow_id=WORKFLOW_ID,
            existing_workflow_name='other.flow',
        )
        broker = _broker(conflict)
        result = await start_workflow_async(_spec(), broker, WORKFLOW_ID, SENT_AT)

        assert is_err(result)
        error = result.err_value
        assert error is conflict
        assert isinstance(error, WorkflowStartIdConflict)
        assert error.existing_workflow_name == 'other.flow'
        assert error.workflow_id == WORKFLOW_ID

    @pytest.mark.asyncio(loop_scope='function')
    async def test_an_id_held_by_another_start_command_is_the_command_conflict(self) -> None:
        conflict = WorkflowStartCommandConflict(
            message=(
                f"workflow_id '{WORKFLOW_ID}' already exists for workflow name "
                "'start_core.flow' with another start command; a start with this id must "
                'send the same command'
            ),
            workflow_name='start_core.flow',
            workflow_id=WORKFLOW_ID,
        )
        broker = _broker(conflict)
        result = await start_workflow_async(_spec(), broker, WORKFLOW_ID, SENT_AT)

        assert is_err(result)
        error = result.err_value
        assert error is conflict
        assert isinstance(error, WorkflowStartCommandConflict)
        assert error.code is WorkflowStartErrorCode.WORKFLOW_COMMAND_CONFLICT
        assert error.retryable is False
        assert error.workflow_id == WORKFLOW_ID

    @pytest.mark.asyncio(loop_scope='function')
    async def test_a_failed_start_is_the_enqueue_failure_with_its_retry_rule(self) -> None:
        cause = core.CoreConnectionError('start_workflow', 'connection refused', True)
        failed = WorkflowStartEnqueueFailed(
            message='connection refused',
            retryable=True,
            workflow_name='start_core.flow',
            workflow_id=WORKFLOW_ID,
            exception=cause,
        )
        broker = _broker(failed)
        result = await start_workflow_async(_spec(), broker, WORKFLOW_ID, SENT_AT)

        assert is_err(result)
        error = result.err_value
        assert error is failed
        assert isinstance(error, WorkflowStartEnqueueFailed)
        assert error.retryable is True
        assert error.message == 'connection refused'
        assert error.exception is cause

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


class TestNodeRowAsserts:
    """The node rows assert what spec construction guarantees, before any core
    call (``top_node_rows``)."""

    @pytest.mark.asyncio(loop_scope='function')
    async def test_a_task_node_with_positional_args_fails_an_assert_that_names_it(
        self,
    ) -> None:
        broker = _broker(_record(core.WorkflowStarted))
        node = TaskNode(fn=value_task, kwargs={'value': 1})
        spec = APP.workflow(
            'start_core.task_args', [node], output=node,
            definition_key='start_core.task_args.v1',
        )
        object.__setattr__(spec.tasks[0], 'args', (1,))

        with pytest.raises(AssertionError) as exc_info:
            await start_workflow_async(spec, broker, WORKFLOW_ID, SENT_AT)

        message = str(exc_info.value)
        assert "TaskNode 'start_core.value'" in message
        assert '1 positional args' in message
        broker._core_handle.start_workflow_async.assert_not_awaited()  # pyright: ignore[reportPrivateUsage]

    @pytest.mark.asyncio(loop_scope='function')
    async def test_a_subworkflow_node_with_positional_args_fails_an_assert_that_names_it(
        self,
    ) -> None:
        broker = _broker(_record(core.WorkflowStarted))
        spec = APP.workflow(
            'start_core.subworkflow_args',
            [SubWorkflowNode(workflow_def=StartCoreChild)],
            definition_key='start_core.subworkflow_args.v1',
        )
        object.__setattr__(spec.tasks[0], 'args', (1, 2))

        with pytest.raises(AssertionError) as exc_info:
            await start_workflow_async(spec, broker, WORKFLOW_ID, SENT_AT)

        message = str(exc_info.value)
        assert "SubWorkflowNode 'start_core.child'" in message
        assert '2 positional args' in message
        broker._core_handle.start_workflow_async.assert_not_awaited()  # pyright: ignore[reportPrivateUsage]
