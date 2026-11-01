"""Workflow start: what prevalidation and the start transaction catch.

Prevalidation returns ``WorkflowStartValidationFailed`` only for a
``WorkflowValidationError``. The start transaction returns it only for a
``SerializationError``. Any other exception from either part is a defect and
propagates. The transaction asserts the facts that spec construction and
prevalidation guarantee, and it takes the node queue from the rule that
``app.workflow()`` uses.
"""

from __future__ import annotations

import uuid
from typing import Any
from unittest.mock import AsyncMock, MagicMock, PropertyMock, patch

import pytest
from pydantic import SecretStr

from horsies.core.app import Horsies
from horsies.core.codec.json_io import SerializationError
from horsies.core.errors import (
    ErrorCode,
    MultipleValidationErrors,
    ValidationReport,
    WorkflowValidationError,
)
from horsies.core.models.app import AppConfig
from horsies.core.models.broker import PostgresConfig
from horsies.core.models.queues import CustomQueueConfig, QueueMode
from horsies.core.models.tasks import TaskError, TaskResult
from horsies.core.models.workflow import (
    SubWorkflowNode,
    TaskNode,
    WorkflowDefinition,
    WorkflowDefinitionConfig,
    WorkflowSpec,
)
from horsies.core.task_decorator import TaskFunction
from horsies.core.types.result import Ok, is_ok
from horsies.core.workflows.lifecycle import start_workflow_async
from horsies.core.workflows.sql import INSERT_WORKFLOW_TASKS_BULK_SQL
from tests.broker_resources_support import attach_resources, fake_resources

pytestmark = [pytest.mark.unit]

DATABASE_URL = 'postgresql+psycopg://u:p@localhost:5432/db'
WORKFLOW_ID = '3c7e9a10-5b2d-4f8e-a1c3-7d9e0f1a2b3c'
DEFECT_MARKER = 'defect-marker-7d41'


def _default_app() -> Horsies:
    return Horsies(AppConfig(broker=PostgresConfig(database_url=SecretStr(DATABASE_URL))))


def _custom_app(queue_name: str) -> Horsies:
    return Horsies(AppConfig(
        queue_mode=QueueMode.CUSTOM,
        broker=PostgresConfig(database_url=SecretStr(DATABASE_URL)),
        custom_queues=[CustomQueueConfig(name=queue_name, priority=40)],
    ))


def _register_echo(app: Horsies, queue_name: str | None = None) -> TaskFunction[..., int]:
    """Register a task ``start_catches.echo``; ``queue_name`` is for CUSTOM mode."""
    if queue_name is None:
        @app.task('start_catches.echo')
        def echo(*, value: int) -> TaskResult[int, TaskError]:
            return TaskResult(ok=value)
        return echo

    @app.task('start_catches.echo', queue_name=queue_name)
    def echo_on_queue(*, value: int) -> TaskResult[int, TaskError]:
        return TaskResult(ok=value)
    return echo_on_queue


def _accepting_session(app: Horsies) -> MagicMock:
    """Give the app's broker a session whose workflow INSERT inserts a new row.

    Nothing reaches a database. Returns the session, to read what the start
    sent to it.
    """
    broker = app.get_broker()
    insert_result = MagicMock()
    insert_result.scalar_one_or_none.return_value = WORKFLOW_ID
    session = MagicMock()
    session.execute = AsyncMock(return_value=insert_result)
    session.commit = AsyncMock()
    session_ctx = MagicMock()
    session_ctx.__aenter__ = AsyncMock(return_value=session)
    session_ctx.__aexit__ = AsyncMock(return_value=False)
    attach_resources(
        broker, fake_resources(session_factory=MagicMock(return_value=session_ctx)),
    )
    setattr(broker, 'ensure_schema_initialized', AsyncMock(return_value=Ok(True)))
    return session


def _workflow_task_rows(session: MagicMock) -> list[dict[str, Any]]:
    """The node rows of the one ``INSERT_WORKFLOW_TASKS_BULK_SQL`` call."""
    calls = [
        call for call in session.execute.await_args_list
        if call.args[0] is INSERT_WORKFLOW_TASKS_BULK_SQL
    ]
    assert len(calls) == 1, calls
    rows: list[dict[str, Any]] = calls[0].args[1]
    return rows


def _definition_key(label: str) -> str:
    return f'tests.unit.test_workflow_start_catches.{label}.{uuid.uuid4().hex}'


# =============================================================================
# Prevalidation (zone 1)
# =============================================================================


def _prevalidation_defects() -> list[BaseException]:
    report = ValidationReport('workflow')
    report.add(WorkflowValidationError('first', code=ErrorCode.WORKFLOW_NO_NAME))
    report.add(WorkflowValidationError('second', code=ErrorCode.WORKFLOW_NO_NODES))
    defects: list[BaseException] = [
        ValueError(DEFECT_MARKER),
        MultipleValidationErrors(message=DEFECT_MARKER, report=report),
        SerializationError(DEFECT_MARKER),
    ]
    return defects


class TestPrevalidationCatch:
    """Prevalidation catches only ``WorkflowValidationError``."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        'defect', _prevalidation_defects(), ids=lambda exc: type(exc).__name__,
    )
    async def test_another_exception_from_a_prevalidation_check_propagates(
        self, defect: BaseException,
    ) -> None:
        spec = MagicMock()
        spec.name = 'zone-one-wf'
        spec.tasks = []
        spec.workflow_def_cls = object()
        broker = MagicMock()
        attach_resources(broker, fake_resources())

        with patch(
            'horsies.core.workflows.lifecycle.validate_workflow_generic_output_match',
            side_effect=defect,
        ):
            with pytest.raises(type(defect)) as exc_info:
                await start_workflow_async(spec, broker, WORKFLOW_ID)

        assert exc_info.value is defect
        broker.select_resources.assert_not_awaited()


# =============================================================================
# Start transaction
# =============================================================================


class TestTransactionValueError:
    """The start transaction has no ``ValueError`` arm."""

    @pytest.mark.asyncio
    async def test_a_value_error_in_the_transaction_propagates_and_nothing_commits(
        self,
    ) -> None:
        app = _default_app()
        echo = _register_echo(app)
        spec = app.workflow(
            name='value-error-wf',
            tasks=[echo.node()(value=1)],
            definition_key=_definition_key('value_error'),
        )
        session = _accepting_session(app)
        defect = ValueError(DEFECT_MARKER)

        with patch(
            'horsies.core.workflows.lifecycle.enqueue_fingerprint',
            side_effect=defect,
        ):
            with pytest.raises(ValueError) as exc_info:
                await start_workflow_async(spec, app.get_broker(), WORKFLOW_ID)

        assert exc_info.value is defect
        session.commit.assert_not_awaited()


def _child_workflow(echo: TaskFunction[..., int]) -> type[WorkflowDefinition[int]]:
    class ChildWorkflow(WorkflowDefinition[int]):
        name = 'start_catches_child'
        definition_key = _definition_key('child')
        only = TaskNode(fn=echo, kwargs={'value': 1})
        config = WorkflowDefinitionConfig(output=only)

    return ChildWorkflow


class TestTransactionAsserts:
    """The transaction asserts what construction and prevalidation guarantee."""

    @pytest.mark.asyncio
    async def test_a_task_node_with_positional_args_fails_an_assert_that_names_it(
        self,
    ) -> None:
        app = _default_app()
        echo = _register_echo(app)
        spec = app.workflow(
            name='task-args-wf',
            tasks=[echo.node()(value=1)],
            definition_key=_definition_key('task_args'),
        )
        object.__setattr__(spec.tasks[0], 'args', (1,))
        session = _accepting_session(app)

        with pytest.raises(AssertionError) as exc_info:
            await start_workflow_async(spec, app.get_broker(), WORKFLOW_ID)

        message = str(exc_info.value)
        assert "TaskNode 'start_catches.echo'" in message
        assert '1 positional args' in message
        session.commit.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_a_subworkflow_node_with_positional_args_fails_an_assert_that_names_it(
        self,
    ) -> None:
        app = _default_app()
        echo = _register_echo(app)
        spec = app.workflow(
            name='subworkflow-args-wf',
            tasks=[SubWorkflowNode(workflow_def=_child_workflow(echo))],
            definition_key=_definition_key('subworkflow_args'),
        )
        object.__setattr__(spec.tasks[0], 'args', (1, 2))
        session = _accepting_session(app)

        with pytest.raises(AssertionError) as exc_info:
            await start_workflow_async(spec, app.get_broker(), WORKFLOW_ID)

        message = str(exc_info.value)
        assert "SubWorkflowNode 'start_catches_child'" in message
        assert '2 positional args' in message
        session.commit.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_a_priority_that_turns_none_after_prevalidation_fails_an_assert(
        self,
    ) -> None:
        app = _default_app()
        echo = _register_echo(app)
        spec = app.workflow(
            name='priority-none-wf',
            tasks=[echo.node()(value=1)],
            definition_key=_definition_key('priority_none'),
        )
        session = _accepting_session(app)

        # Prevalidation reads 50; the transaction reads None.
        with patch.object(
            TaskNode, 'priority', new_callable=PropertyMock, side_effect=[50, None],
        ):
            with pytest.raises(AssertionError) as exc_info:
                await start_workflow_async(spec, app.get_broker(), WORKFLOW_ID)

        message = str(exc_info.value)
        assert "TaskNode 'start_catches.echo'" in message
        assert 'priority=None' in message
        session.commit.assert_not_awaited()


class TestTransactionQueueRule:
    """An empty node queue stores the queue that ``app.workflow()`` resolves."""

    @pytest.mark.asyncio
    async def test_a_direct_spec_with_an_empty_queue_stores_the_task_queue(
        self,
    ) -> None:
        app = _custom_app('emails')
        echo = _register_echo(app, queue_name='emails')
        spec: WorkflowSpec[Any] = WorkflowSpec(
            name='direct-empty-queue-wf',
            tasks=[TaskNode(fn=echo, kwargs={'value': 1}, queue='', priority=50)],
            broker=app.get_broker(),
            definition_key=_definition_key('direct_task_queue'),
        )
        session = _accepting_session(app)

        result = await start_workflow_async(spec, app.get_broker(), WORKFLOW_ID)

        assert is_ok(result)
        assert [row['queue'] for row in _workflow_task_rows(session)] == ['emails']

    @pytest.mark.asyncio
    async def test_a_direct_spec_with_an_empty_queue_and_no_task_queue_stores_default(
        self,
    ) -> None:
        app = _default_app()
        echo = _register_echo(app)
        spec: WorkflowSpec[Any] = WorkflowSpec(
            name='direct-default-queue-wf',
            tasks=[TaskNode(fn=echo, kwargs={'value': 1}, queue='', priority=50)],
            broker=app.get_broker(),
            definition_key=_definition_key('direct_default_queue'),
        )
        session = _accepting_session(app)

        result = await start_workflow_async(spec, app.get_broker(), WORKFLOW_ID)

        assert is_ok(result)
        assert [row['queue'] for row in _workflow_task_rows(session)] == ['default']

    @pytest.mark.asyncio
    async def test_a_frozen_node_with_an_empty_queue_reused_in_app_workflow(
        self,
    ) -> None:
        app = _custom_app('emails')
        echo = _register_echo(app, queue_name='emails')
        direct: WorkflowSpec[Any] = WorkflowSpec(
            name='frozen-source-wf',
            tasks=[TaskNode(fn=echo, kwargs={'value': 1}, queue='', priority=50)],
            broker=app.get_broker(),
            definition_key=_definition_key('frozen_source'),
        )
        frozen_node = direct.tasks[0]
        assert isinstance(frozen_node, TaskNode)
        assert frozen_node.queue == ''
        spec = app.workflow(
            name='frozen-reuse-wf',
            tasks=[frozen_node],
            definition_key=_definition_key('frozen_reuse'),
        )
        session = _accepting_session(app)

        result = await start_workflow_async(spec, app.get_broker(), WORKFLOW_ID)

        assert is_ok(result)
        assert [row['queue'] for row in _workflow_task_rows(session)] == ['emails']
