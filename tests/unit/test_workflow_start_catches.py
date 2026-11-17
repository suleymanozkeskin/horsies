"""Workflow start: what prevalidation and the start statement catch.

Prevalidation returns ``WorkflowStartValidationFailed`` only for a
``WorkflowValidationError``. The build of the start statement returns no
``WorkflowStartValidationFailed``: it encodes only values that the process
built and validated before the start, so a ``SerializationError`` there is a
defect too, raised before any statement is sent. Any other exception from
either part, except a database or driver error of the statement, is a defect
and propagates. The build asserts the facts that spec construction and
prevalidation guarantee, and it takes the node queue from the rule that
``app.workflow()`` uses.

The tests drive the Python start caller before the core switch
(``tests/dualrun/reference/workflow_start_caller.py``), the reference side of
the start's dual-run; ``test_workflow_start_through_core.py`` tests the
production start.
"""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
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
from tests.dualrun.reference.workflow_start_caller import start_workflow_async
from tests.broker_resources_support import attach_resources, fake_resources
from tests.workflow_start_support import (
    node_rows,
    outcome_row,
    start_parameters,
    start_session,
)

pytestmark = [pytest.mark.unit]

DATABASE_URL = 'postgresql://u:p@localhost:5432/db'
WORKFLOW_ID = '3c7e9a10-5b2d-4f8e-a1c3-7d9e0f1a2b3c'
DEFECT_MARKER = 'defect-marker-7d41'
GOOD_UNTIL = datetime(2099, 1, 1, tzinfo=timezone.utc)


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
    """Give the app's broker a session whose start statement writes a new tree.

    Nothing reaches a database. Returns the session; the statement goes to
    its autocommit connection (``session.connection``).
    """
    broker = app.get_broker()
    session, session_ctx, _ = start_session([outcome_row('STARTED')])
    attach_resources(
        broker, fake_resources(session_factory=MagicMock(return_value=session_ctx)),
    )
    setattr(broker, 'ensure_schema_initialized', AsyncMock(return_value=Ok(True)))
    return session


def _workflow_task_rows(session: MagicMock) -> list[dict[str, Any]]:
    """The node rows of the one start statement."""
    return node_rows(start_parameters(session.connection.return_value))


def _assert_nothing_sent(session: MagicMock) -> None:
    """The start sent no statement."""
    session.connection.assert_not_awaited()
    session.execute.assert_not_awaited()


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
            'tests.dualrun.reference.workflow_start_caller.validate_workflow_generic_output_match',
            side_effect=defect,
        ):
            with pytest.raises(type(defect)) as exc_info:
                await start_workflow_async(spec, broker, WORKFLOW_ID)

        assert exc_info.value is defect
        broker.select_resources.assert_not_awaited()


# =============================================================================
# The start statement
# =============================================================================


class TestTransactionValueError:
    """The build of the start statement has no ``ValueError`` arm."""

    @pytest.mark.asyncio
    async def test_a_value_error_in_the_statement_build_propagates_and_nothing_is_sent(
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
            'tests.dualrun.reference.workflow_start_caller.start_parameters',
            side_effect=defect,
        ):
            with pytest.raises(ValueError) as exc_info:
                await start_workflow_async(spec, app.get_broker(), WORKFLOW_ID)

        assert exc_info.value is defect
        _assert_nothing_sent(session)


class TestTransactionSerializationError:
    """The build of the start statement has no ``SerializationError`` arm."""

    @pytest.mark.asyncio
    async def test_corrupt_task_options_of_a_root_propagate_and_nothing_is_sent(
        self,
    ) -> None:
        app = _default_app()
        echo = _register_echo(app)
        # Only a defect or a write to the private attribute makes this.
        setattr(echo, 'task_options_json', '{not-json')
        spec = app.workflow(
            name='corrupt-options-wf',
            tasks=[echo.node()(value=1)],
            definition_key=_definition_key('corrupt_options'),
        )
        session = _accepting_session(app)

        with pytest.raises(SerializationError) as exc_info:
            await start_workflow_async(spec, app.get_broker(), WORKFLOW_ID)

        assert 'start_catches.echo' in str(exc_info.value)
        _assert_nothing_sent(session)

    @pytest.mark.asyncio
    async def test_corrupt_task_options_of_a_dependent_propagate_and_nothing_is_sent(
        self,
    ) -> None:
        """The start writes the task facts of every task node, so it parses
        the options of a node with dependencies too, not only of a root."""
        app = _default_app()
        echo = _register_echo(app)

        @app.task('start_catches.corrupt_dependent')
        def corrupt_dependent(*, value: int) -> TaskResult[int, TaskError]:
            return TaskResult(ok=value)

        # Only a defect or a write to the private attribute makes this.
        setattr(corrupt_dependent, 'task_options_json', '{not-json')
        root = echo.node()(value=1)
        spec = app.workflow(
            name='corrupt-dependent-options-wf',
            tasks=[root, corrupt_dependent.node(waits_for=[root])(value=2)],
            definition_key=_definition_key('corrupt_dependent_options'),
        )
        session = _accepting_session(app)

        with pytest.raises(SerializationError) as exc_info:
            await start_workflow_async(spec, app.get_broker(), WORKFLOW_ID)

        assert 'start_catches.corrupt_dependent' in str(exc_info.value)
        _assert_nothing_sent(session)

    @pytest.mark.asyncio
    async def test_a_serialization_error_in_the_statement_build_propagates_and_nothing_is_sent(
        self,
    ) -> None:
        app = _default_app()
        echo = _register_echo(app)
        spec = app.workflow(
            name='serialization-error-wf',
            tasks=[echo.node()(value=1)],
            definition_key=_definition_key('serialization_error'),
        )
        session = _accepting_session(app)
        defect = SerializationError(DEFECT_MARKER)

        with patch(
            'horsies.core.workflows.child_build.stored_kwargs',
            side_effect=defect,
        ):
            with pytest.raises(SerializationError) as exc_info:
                await start_workflow_async(spec, app.get_broker(), WORKFLOW_ID)

        assert exc_info.value is defect
        _assert_nothing_sent(session)


class TestTransactionGoodUntilMerge:
    """The tree build merges a node's ``good_until`` into the task options.

    The options are the task decorator's serialization of a validated
    ``TaskOptions``. Options that are JSON but not an object are a defect,
    as options that do not parse are: the merge raises with the node name,
    and nothing commits.
    """

    @pytest.mark.asyncio
    async def test_the_merged_options_carry_good_until(self) -> None:
        # Control for the test below: the same start with valid options.
        app = _default_app()
        echo = _register_echo(app)
        spec = app.workflow(
            name='good-until-merge-wf',
            tasks=[TaskNode(fn=echo, kwargs={'value': 1}, good_until=GOOD_UNTIL)],
            definition_key=_definition_key('good_until_merge'),
        )
        session = _accepting_session(app)

        result = await start_workflow_async(spec, app.get_broker(), WORKFLOW_ID)

        assert is_ok(result)
        [row] = _workflow_task_rows(session)
        stored_options = json.loads(row['task_options'])
        assert stored_options['good_until'] == GOOD_UNTIL.isoformat()

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ('options_json', 'type_name'),
        [('"just a string"', 'str'), ('[1, 2]', 'list'), ('null', 'NoneType')],
        ids=['string', 'array', 'null'],
    )
    async def test_options_that_are_not_an_object_raise_and_nothing_is_sent(
        self,
        options_json: str,
        type_name: str,
    ) -> None:
        app = _default_app()
        echo = _register_echo(app)
        # Only a defect or a write to the private attribute makes this.
        setattr(echo, 'task_options_json', options_json)
        spec = app.workflow(
            name='good-until-not-an-object-wf',
            tasks=[TaskNode(fn=echo, kwargs={'value': 1}, good_until=GOOD_UNTIL)],
            definition_key=_definition_key('good_until_not_an_object'),
        )
        session = _accepting_session(app)

        with pytest.raises(SerializationError) as exc_info:
            await start_workflow_async(spec, app.get_broker(), WORKFLOW_ID)

        assert str(exc_info.value) == (
            'Task options for node start_catches.echo are not a JSON object: '
            f'got {type_name}'
        )
        _assert_nothing_sent(session)


def _child_workflow(echo: TaskFunction[..., int]) -> type[WorkflowDefinition[int]]:
    class ChildWorkflow(WorkflowDefinition[int]):
        name = 'start_catches_child'
        definition_key = _definition_key('child')
        only = TaskNode(fn=echo, kwargs={'value': 1})
        config = WorkflowDefinitionConfig(output=only)

    return ChildWorkflow


class TestTransactionAsserts:
    """The tree build asserts what construction and prevalidation guarantee."""

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
        _assert_nothing_sent(session)

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
        _assert_nothing_sent(session)

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

        # Prevalidation reads 50; the tree build reads None.
        with patch.object(
            TaskNode, 'priority', new_callable=PropertyMock, side_effect=[50, None],
        ):
            with pytest.raises(AssertionError) as exc_info:
                await start_workflow_async(spec, app.get_broker(), WORKFLOW_ID)

        message = str(exc_info.value)
        assert "TaskNode 'start_catches.echo'" in message
        assert 'priority=None' in message
        _assert_nothing_sent(session)


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
        assert [row['queue_name'] for row in _workflow_task_rows(session)] == ['emails']

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
        assert [row['queue_name'] for row in _workflow_task_rows(session)] == ['default']

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
        assert [row['queue_name'] for row in _workflow_task_rows(session)] == ['emails']
