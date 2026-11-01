"""Task options that do not parse: the child start and the promotion path.

The two paths read the options from different places, so they handle a
value that does not parse in different ways:

- The child workflow start (``enqueue_subworkflow_task``) reads
  ``fn.task_options_json``, the task decorator's own serialization of a
  validated ``TaskOptions``. A value that does not parse there is a defect:
  it raises ``SerializationError`` before any INSERT, as the workflow start
  transaction does, and the caller's transaction rolls back.
- The promotion path (``enqueue_workflow_task``) reads the stored copy on
  the node row. A stored value that does not parse is stored data: it fails
  only that node, with ``WORKER_SERIALIZATION_ERROR``.
"""

from __future__ import annotations

# pyright: reportPrivateUsage=false

import json
from datetime import datetime, timezone
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from horsies.core.codec.json_io import SerializationError
from horsies.core.models.tasks import OperationalErrorCode
from horsies.core.models.workflow import TaskNode
from horsies.core.types.result import Err
from horsies.core.workflows import engine
from horsies.core.workflows.engine import (
    _child_task_options_json,
    enqueue_subworkflow_task,
    enqueue_workflow_task,
)
from horsies.core.workflows.sql import (
    ENQUEUE_SUBWORKFLOW_TASK_SQL,
    ENQUEUE_WORKFLOW_TASK_SQL,
    INSERT_CHILD_WORKFLOW_SQL,
    INSERT_WORKFLOW_TASKS_BULK_SQL,
)

pytestmark = [pytest.mark.unit]

UNPARSEABLE_OPTIONS = '{not-json'
NOT_AN_OBJECT_OPTIONS = '"just a string"'
VALID_OPTIONS = '{"retry_policy": null}'
GOOD_UNTIL = datetime(2030, 1, 1, tzinfo=timezone.utc)


def _child_task(
    *, options_json: str | None, good_until: datetime | None,
) -> MagicMock:
    """A child TaskNode with no dependencies (a root of the child workflow)."""
    child_task = MagicMock()
    child_task.name = 'child_task'
    child_task.args = ()
    child_task.kwargs = {}
    child_task.waits_for = []
    child_task.args_from = {}
    child_task.workflow_ctx_from = None
    child_task.index = 0
    child_task.node_id = 'child-0'
    child_task.good_until = good_until
    child_task.fn = MagicMock()
    child_task.fn.task_options_json = options_json
    child_task.fn.task_queue_name = 'default'
    child_task.allow_failed_deps = False
    child_task.join = 'all'
    child_task.min_success = None
    child_task.queue = None
    child_task.priority = None
    return child_task


def _as_task_node(child_task: MagicMock) -> TaskNode[Any]:
    return cast('TaskNode[Any]', child_task)


class _ChildStart:
    """``enqueue_subworkflow_task`` for a parent node whose child spec has
    the one node ``child_task``.

    ``executed`` records each statement, and ``fail_mock`` stands for
    ``_fail_enqueued_task``. The test reads both after ``run()``, also when
    ``run()`` raises.
    """

    def __init__(self, child_task: MagicMock) -> None:
        self.child_task = child_task
        self.executed: list[Any] = []
        self.fail_mock = AsyncMock()

    async def _dispatch(
        self, statement: Any, *_args: Any, **_kwargs: Any,
    ) -> MagicMock:
        self.executed.append(statement)
        result = MagicMock()
        result.fetchone.return_value = (
            SimpleNamespace(
                id='wt-1',
                sub_workflow_name='child_wf',
                task_args='[]',
                task_kwargs=None,
                args_from=None,
                node_id='node-0',
                sub_definition_key='test.child.v1',
                parent_workflow_name='test_wf',
            )
            if statement is ENQUEUE_SUBWORKFLOW_TASK_SQL
            else SimpleNamespace()
        )
        return result

    async def run(self) -> str | None:
        session = AsyncMock()
        session.execute = AsyncMock(side_effect=self._dispatch)
        broker = MagicMock()
        broker.app = MagicMock()
        broker.app.config.queue_mode.name = 'DEFAULT'

        child_spec = MagicMock()
        child_spec.tasks = [self.child_task]
        child_spec.success_policy = None
        child_spec.output = None
        child_spec.on_error = MagicMock(value='fail')
        child_spec.definition_key = 'test.child.v1'
        child_spec.name = 'child_wf'
        workflow_def = MagicMock()
        workflow_def.build_with.return_value = child_spec
        workflow_def.name = 'ChildWorkflow'
        workflow_def._original_build_with = workflow_def.build_with

        with patch(
            'horsies.core.workflows.registry.get_subworkflow_node',
            return_value=None,
        ), patch(
            'horsies.core.workflows.engine._load_workflow_def_from_key',
            return_value=workflow_def,
        ), patch(
            'horsies.core.workflows.engine.validate_workflow_generic_output_match',
        ), patch(
            'horsies.core.workflows.engine.guard_no_positional_args',
        ), patch(
            'horsies.core.workflows.engine._fail_enqueued_task',
            new=self.fail_mock,
        ):
            return await enqueue_subworkflow_task(
                session, broker, 'parent-wf', 0, {},
                parent_depth=0, root_workflow_id='parent-wf',
            )


class TestChildTaskOptionsJson:
    def test_no_good_until_returns_the_task_function_options(self) -> None:
        child_task = _child_task(options_json=VALID_OPTIONS, good_until=None)

        assert _child_task_options_json(_as_task_node(child_task)) == VALID_OPTIONS

    def test_good_until_is_set_in_the_options(self) -> None:
        child_task = _child_task(
            options_json=VALID_OPTIONS, good_until=GOOD_UNTIL,
        )

        merged = _child_task_options_json(_as_task_node(child_task))

        assert merged is not None
        assert json.loads(merged) == {
            'retry_policy': None,
            'good_until': GOOD_UNTIL.isoformat(),
        }

    def test_good_until_without_options_is_the_only_option(self) -> None:
        child_task = _child_task(options_json=None, good_until=GOOD_UNTIL)

        merged = _child_task_options_json(_as_task_node(child_task))

        assert merged is not None
        assert json.loads(merged) == {'good_until': GOOD_UNTIL.isoformat()}

    def test_unparseable_options_raise(self) -> None:
        child_task = _child_task(
            options_json=UNPARSEABLE_OPTIONS, good_until=GOOD_UNTIL,
        )

        with pytest.raises(
            SerializationError,
            match='Corrupt task_options for child node child_task',
        ):
            _child_task_options_json(_as_task_node(child_task))

    def test_options_that_are_not_an_object_raise(self) -> None:
        child_task = _child_task(
            options_json=NOT_AN_OBJECT_OPTIONS, good_until=GOOD_UNTIL,
        )

        with pytest.raises(
            SerializationError,
            match=(
                'Task options for child node child_task are not a JSON '
                'object: got str'
            ),
        ):
            _child_task_options_json(_as_task_node(child_task))

    def test_merged_options_that_do_not_serialize_raise(self) -> None:
        child_task = _child_task(
            options_json=VALID_OPTIONS, good_until=GOOD_UNTIL,
        )

        with patch.object(
            engine,
            'dumps_json',
            return_value=Err(SerializationError('dump refused')),
        ):
            with pytest.raises(
                SerializationError,
                match=(
                    'Failed to serialize task_options for child node '
                    'child_task: dump refused'
                ),
            ):
                _child_task_options_json(_as_task_node(child_task))


class TestChildStartReadsTheTaskFunctionOptions:
    @pytest.mark.asyncio
    async def test_valid_options_of_a_fast_root_reach_the_insert(self) -> None:
        # Control for the tests below: the same harness reaches the INSERT.
        child_start = _ChildStart(
            _child_task(options_json=VALID_OPTIONS, good_until=None),
        )

        child_workflow_id = await child_start.run()

        assert child_workflow_id is not None
        assert INSERT_CHILD_WORKFLOW_SQL in child_start.executed
        assert INSERT_WORKFLOW_TASKS_BULK_SQL in child_start.executed
        child_start.fail_mock.assert_not_awaited()

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        'options_json',
        [UNPARSEABLE_OPTIONS, NOT_AN_OBJECT_OPTIONS],
        ids=['unparseable', 'not-an-object'],
    )
    async def test_options_of_a_fast_root_raise_before_any_insert(
        self,
        options_json: str,
    ) -> None:
        child_start = _ChildStart(
            _child_task(options_json=options_json, good_until=None),
        )

        with pytest.raises(
            SerializationError,
            match='Corrupt task_options for root node child_task',
        ):
            await child_start.run()

        assert ENQUEUE_SUBWORKFLOW_TASK_SQL in child_start.executed
        assert INSERT_CHILD_WORKFLOW_SQL not in child_start.executed
        assert INSERT_WORKFLOW_TASKS_BULK_SQL not in child_start.executed
        child_start.fail_mock.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_options_merged_with_good_until_raise_before_any_insert(
        self,
    ) -> None:
        child_start = _ChildStart(
            _child_task(options_json=UNPARSEABLE_OPTIONS, good_until=GOOD_UNTIL),
        )

        with pytest.raises(
            SerializationError,
            match='Corrupt task_options for child node child_task',
        ):
            await child_start.run()

        assert ENQUEUE_SUBWORKFLOW_TASK_SQL in child_start.executed
        assert INSERT_CHILD_WORKFLOW_SQL not in child_start.executed
        child_start.fail_mock.assert_not_awaited()


class TestPromotionReadsTheStoredOptions:
    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ('options_json', 'message'),
        [
            (UNPARSEABLE_OPTIONS, 'Corrupt task_options JSON for task 0'),
            (
                NOT_AN_OBJECT_OPTIONS,
                'Invalid task_options payload for task 0: expected object',
            ),
        ],
        ids=['unparseable', 'not-an-object'],
    )
    async def test_stored_options_fail_only_that_node(
        self,
        options_json: str,
        message: str,
    ) -> None:
        node_row = SimpleNamespace(
            id='wt-1',
            task_name='child_task',
            task_args='[]',
            task_kwargs=None,
            queue_name='default',
            priority=100,
            args_from=None,
            workflow_ctx_from=None,
            task_options=options_json,
        )

        async def dispatch(statement: Any, *_args: Any, **_kwargs: Any) -> MagicMock:
            result = MagicMock()
            result.fetchone.return_value = (
                node_row if statement is ENQUEUE_WORKFLOW_TASK_SQL
                else SimpleNamespace()
            )
            return result

        session = AsyncMock()
        session.execute = AsyncMock(side_effect=dispatch)
        fail_mock = AsyncMock()

        with patch(
            'horsies.core.workflows.engine._fail_enqueued_task',
            new=fail_mock,
        ):
            task_id = await enqueue_workflow_task(session, 'child-wf', 0, {})

        assert task_id is None
        fail_mock.assert_awaited_once()
        call = fail_mock.await_args
        assert call is not None
        assert call.args[:4] == (session, 'child-wf', 0, message)
        assert (
            call.kwargs['error_code']
            is OperationalErrorCode.WORKER_SERIALIZATION_ERROR
        )
