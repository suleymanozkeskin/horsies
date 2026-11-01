"""A value that the workflow engine built and that does not serialize raises.

The engine serializes only values that it built in the process: kwargs,
results, errors and summaries. It builds some of them from values that it
parsed from stored rows, and a parsed JSON value serializes again. So a
serialization failure is a defect. Each writer raises ``SerializationError``
with the subject in the message, before the statement that would store the
value: no ``'null'`` text and no NULL goes into a result or error column.

The child workflow start also encodes the kwargs of each child node. Spec
construction in ``build_with()`` encoded and decoded every one of them, so
an encode refusal there is a defect too, and raises in the same way.
"""

from __future__ import annotations

# pyright: reportPrivateUsage=false

import json
import uuid
from collections.abc import Callable
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from pydantic import BaseModel, SecretStr, field_validator

from horsies.core.app import Horsies
from horsies.core.codec.error_payload import serialize_error_payload
from horsies.core.codec.json_io import SerializationError
from horsies.core.codec.json_value import StrictJsonError
from horsies.core.errors import ErrorCode, WorkflowValidationError
from horsies.core.models.app import AppConfig
from horsies.core.models.broker import PostgresConfig
from horsies.core.models.tasks import (
    OperationalErrorCode,
    TaskError,
    TaskResult,
)
from horsies.core.models.workflow import (
    SubWorkflowNode,
    TaskNode,
    WorkflowDefinition,
    WorkflowSpec,
)
from horsies.core.task_decorator import TaskFunction
from horsies.core.types.result import Err, Ok
from horsies.core.workflows.engine import (
    _handle_workflow_task_failure,
    _ser_or_raise,
    enqueue_subworkflow_task,
    get_workflow_failure_error,
    on_subworkflow_complete,
    on_workflow_task_complete,
)
from horsies.core.workflows.sql import (
    COMPLETE_WORKFLOW_TASK_SQL,
    ENQUEUE_SUBWORKFLOW_TASK_SQL,
    GET_CHILD_WORKFLOW_INFO_SQL,
    GET_FIRST_FAILED_REQUIRED_TASK_SQL,
    GET_FIRST_FAILED_TASK_RESULT_SQL,
    GET_WORKFLOW_ON_ERROR_SQL,
    INSERT_CHILD_WORKFLOW_SQL,
    INSERT_WORKFLOW_TASKS_BULK_SQL,
    LOCK_WORKFLOW_FOR_COMPLETION_CHECK_SQL,
    PAUSE_WORKFLOW_ON_ERROR_SQL,
    SET_WORKFLOW_ERROR_SQL,
    UPDATE_PARENT_NODE_RESULT_SQL,
)

pytestmark = [pytest.mark.unit]

DUMPS_JSON = 'horsies.core.workflows.engine.dumps_json'
REFUSED = Err(SerializationError('value is not JSON-native: forced'))
OUTPUTLESS_CHILD_RESULT = (
    '{"__h_task_result__":true,"__h_outputless_terminals__":true,'
    '"ok":{"results_by_id":{},"task_name_by_id":{},'
    '"definition_key_by_id":{}},"err":null}'
)
CHILD_OUTPUT_RESULT = '{"__h_task_result__":true,"ok":7,"err":null}'
STORED_TASK_ERROR = serialize_error_payload(
    TaskResult(err=TaskError(error_code='FIRST_ERROR', message='first')),
)
LONE_SURROGATE_TEXT = f'name{chr(0xdc80)}'
DATABASE_URL = 'postgresql+psycopg://u:p@localhost:5432/db'


class _RefusedOk(BaseModel):
    """A declared result type whose validator raises with a lone surrogate.

    Pydantic does not wrap a `RuntimeError` from a validator, so the text
    reaches the caller as written.
    """

    value: int

    @field_validator('value')
    @classmethod
    def refuse(cls, value: int) -> int:
        raise RuntimeError(f'cannot check {LONE_SURROGATE_TEXT}: {value}')


class _RecordingSession:
    """An ``AsyncSession`` stand-in that records each statement.

    ``rows`` maps a statement to the row that its ``fetchone()`` returns, or
    to a list of rows that successive executions return in order. Every
    other statement returns an empty namespace row.
    """

    def __init__(self, rows: dict[Any, Any]) -> None:
        self.rows = rows
        self.executed: list[Any] = []
        self.session = AsyncMock()
        self.session.execute = AsyncMock(side_effect=self._dispatch)
        # The engine queues parent propagations in ``session.info``.
        self.session.info = {}

    async def _dispatch(
        self, statement: Any, *_args: Any, **_kwargs: Any,
    ) -> MagicMock:
        self.executed.append(statement)
        result = MagicMock()
        configured = self.rows.get(statement, SimpleNamespace())
        match configured:
            case list():
                queued_rows = cast('list[Any]', configured)
                result.fetchone.return_value = queued_rows.pop(0)
            case row:
                result.fetchone.return_value = row
        return result


class TestSerOrRaise:
    def test_ok_returns_the_serialized_text(self) -> None:
        assert _ser_or_raise(Ok('{"a":1}'), 'the payload') == '{"a":1}'

    def test_err_raises_with_the_context_and_the_cause(self) -> None:
        cause = SerializationError('value is not JSON-native: tuple')

        with pytest.raises(SerializationError) as raised:
            _ser_or_raise(Err(cause), 'the payload')

        assert str(raised.value) == (
            'Failed to serialize the payload: value is not JSON-native: tuple'
        )
        assert raised.value.__cause__ is cause


class TestCompletionResult:
    @pytest.mark.asyncio
    async def test_raises_before_the_node_write(self) -> None:
        recorder = _RecordingSession({})

        with patch(DUMPS_JSON, return_value=REFUSED):
            with pytest.raises(
                SerializationError,
                match='Failed to serialize completion result for task task-1',
            ):
                await on_workflow_task_complete(
                    recorder.session, 'task-1', TaskResult(ok='done'), None,
                    task_name='task-a',
                )

        assert recorder.executed == []

    @pytest.mark.asyncio
    async def test_the_text_of_an_encode_failure_is_stored_escaped(self) -> None:
        """The node fails with a serialization error, and that error is
        encoded again. A validator can raise an exception that pydantic does
        not wrap, so its text reaches the error as written. Without the
        escape, the second encode refuses it and the completion raises on
        each finalize retry and each recovery pass."""
        app = _real_app()
        _ = _register_refused_task(app)
        broker = MagicMock()
        broker.app = app
        recorder = _RecordingSession({COMPLETE_WORKFLOW_TASK_SQL: None})

        await on_workflow_task_complete(
            recorder.session, 'task-1', TaskResult(ok={'value': 1}), broker,
            task_name='surrogate.refused',
        )

        assert recorder.executed == [COMPLETE_WORKFLOW_TASK_SQL]
        node_write = recorder.session.execute.await_args
        assert node_write is not None
        assert node_write.args[1]['status'] == 'FAILED'
        error = json.loads(node_write.args[1]['result'])['err']
        assert error['error_code'] == {
            '__builtin_task_code__': 'WORKER_SERIALIZATION_ERROR',
        }
        assert error['message'] == (
            'Failed to encode completion result: cannot check name\\udc80: 1'
        )


class TestSubworkflowCompletion:
    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ('child_status', 'child_result', 'child_error', 'subject'),
        [
            (
                'COMPLETED', OUTPUTLESS_CHILD_RESULT, None,
                'outputless result of child workflow child-1',
            ),
            (
                'COMPLETED', CHILD_OUTPUT_RESULT, None,
                'summary of child workflow child-1',
            ),
            (
                'FAILED', None, None,
                'parent node error for child workflow child-1',
            ),
        ],
        ids=['outputless-result', 'summary', 'parent-node-error'],
    )
    async def test_raises_before_the_parent_node_write(
        self,
        child_status: str,
        child_result: str | None,
        child_error: str | None,
        subject: str,
    ) -> None:
        recorder = _RecordingSession({
            GET_CHILD_WORKFLOW_INFO_SQL: SimpleNamespace(
                status=child_status,
                result=child_result,
                error=child_error,
                parent_workflow_id='wf-parent',
                parent_task_index=0,
                total=1, completed=1, failed=0, skipped=0,
            ),
            LOCK_WORKFLOW_FOR_COMPLETION_CHECK_SQL: SimpleNamespace(
                id='wf-parent',
            ),
        })

        with patch(DUMPS_JSON, return_value=REFUSED):
            with pytest.raises(
                SerializationError, match=f'Failed to serialize {subject}',
            ):
                await on_subworkflow_complete(recorder.session, 'child-1')

        assert UPDATE_PARENT_NODE_RESULT_SQL not in recorder.executed


class TestWorkflowFailureError:
    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ('success_policy', 'rows', 'subject'),
        [
            (
                None,
                {GET_FIRST_FAILED_TASK_RESULT_SQL: SimpleNamespace(
                    result=STORED_TASK_ERROR,
                )},
                'first task error of workflow wf-1',
            ),
            (
                {'cases': [{'required_indices': [0]}]},
                {GET_FIRST_FAILED_REQUIRED_TASK_SQL: SimpleNamespace(
                    result=STORED_TASK_ERROR,
                )},
                'first task error of workflow wf-1',
            ),
            (
                {'cases': [{'required_indices': [0]}]},
                {GET_FIRST_FAILED_REQUIRED_TASK_SQL: None},
                'success case not met error of workflow wf-1',
            ),
        ],
        ids=['first-failed-task', 'first-failed-required-task', 'no-case-met'],
    )
    async def test_raises_instead_of_returning_null_text(
        self,
        success_policy: dict[str, Any] | None,
        rows: dict[Any, Any],
        subject: str,
    ) -> None:
        recorder = _RecordingSession(rows)

        with patch(DUMPS_JSON, return_value=REFUSED):
            with pytest.raises(
                SerializationError, match=f'Failed to serialize {subject}',
            ):
                await get_workflow_failure_error(
                    recorder.session, 'wf-1', success_policy,
                )


class TestTaskFailureErrorPayload:
    @pytest.mark.asyncio
    @pytest.mark.parametrize('on_error', ['fail', 'pause'])
    async def test_raises_before_the_workflow_error_write(
        self, on_error: str,
    ) -> None:
        recorder = _RecordingSession({})
        failure: TaskResult[Any, TaskError] = TaskResult(
            err=TaskError(error_code='TASK_ERR', message='failed'),
        )

        with patch(DUMPS_JSON, return_value=REFUSED):
            with pytest.raises(
                SerializationError,
                match='Failed to serialize task error of workflow wf-1',
            ):
                await _handle_workflow_task_failure(
                    recorder.session, 'wf-1', failure,
                    known_on_error=on_error,
                )

        assert PAUSE_WORKFLOW_ON_ERROR_SQL not in recorder.executed
        assert recorder.executed == []


def _child_task_node() -> MagicMock:
    """A child TaskNode with no dependencies and no task options."""
    child_task = MagicMock()
    child_task.name = 'child_task'
    child_task.args = ()
    child_task.kwargs = {'value': 1}
    child_task.waits_for = []
    child_task.args_from = {}
    child_task.workflow_ctx_from = None
    child_task.index = 0
    child_task.node_id = 'child-0'
    child_task.good_until = None
    child_task.fn = MagicMock()
    child_task.fn.task_options_json = None
    child_task.fn.task_queue_name = 'default'
    child_task.allow_failed_deps = False
    child_task.join = 'all'
    child_task.min_success = None
    child_task.queue = None
    child_task.priority = None
    return child_task


def _child_sub_node() -> MagicMock:
    """A child SubWorkflowNode with no dependencies."""
    child_sub = MagicMock(spec=SubWorkflowNode)
    child_sub.name = 'child_sub'
    child_sub.args = ()
    child_sub.kwargs = {'value': 1}
    child_sub.waits_for = []
    child_sub.args_from = {}
    child_sub.workflow_ctx_from = None
    child_sub.index = 0
    child_sub.node_id = 'child-0'
    child_sub.allow_failed_deps = False
    child_sub.join = 'all'
    child_sub.min_success = None
    child_sub.workflow_def = MagicMock()
    child_sub.workflow_def.name = 'GrandchildWorkflow'
    return child_sub


async def _start_child(
    child_node: MagicMock, recorder: _RecordingSession, fail_mock: AsyncMock,
) -> None:
    """Run ``enqueue_subworkflow_task`` for a parent node whose child spec
    has the one node ``child_node``. ``fail_mock`` stands for
    ``_fail_enqueued_task``."""
    broker = MagicMock()
    broker.app = MagicMock()
    broker.app.config.queue_mode.name = 'DEFAULT'
    child_spec = MagicMock()
    child_spec.tasks = [child_node]
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
        'horsies.core.workflows.engine._load_workflow_def_from_key',
        return_value=workflow_def,
    ), patch(
        'horsies.core.workflows.engine.validate_workflow_generic_output_match',
    ), patch(
        'horsies.core.workflows.engine._fail_enqueued_task', new=fail_mock,
    ):
        _ = await enqueue_subworkflow_task(
            recorder.session, broker, 'parent-wf', 0, {},
            parent_depth=0, root_workflow_id='parent-wf',
        )


def _parent_node_row() -> SimpleNamespace:
    return SimpleNamespace(
        id='wt-1',
        sub_workflow_name='child_wf',
        task_args='[]',
        task_kwargs=None,
        args_from=None,
        node_id='node-0',
        sub_definition_key='test.child.v1',
        parent_workflow_name='test_wf',
    )


class TestChildStartKwargsEncode:
    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        ('make_node', 'encoder', 'subject'),
        [
            (
                _child_task_node,
                'horsies.core.workflows.engine.encode_kwargs',
                'child task child_task',
            ),
            (
                _child_sub_node,
                'horsies.core.workflows.engine.encode_subworkflow_kwargs',
                'child sub child_sub',
            ),
        ],
        ids=['child-task', 'child-sub'],
    )
    async def test_an_encode_refusal_raises_before_any_insert(
        self,
        make_node: Callable[[], MagicMock],
        encoder: str,
        subject: str,
    ) -> None:
        recorder = _RecordingSession(
            {ENQUEUE_SUBWORKFLOW_TASK_SQL: _parent_node_row()},
        )
        refusal = StrictJsonError('reserved key __h_x__')
        fail_mock = AsyncMock()

        with patch(encoder, side_effect=refusal):
            with pytest.raises(SerializationError) as raised:
                await _start_child(make_node(), recorder, fail_mock)

        assert str(raised.value) == (
            f'Failed to encode kwargs for {subject}: reserved key __h_x__'
        )
        assert raised.value.__cause__ is refusal
        assert INSERT_CHILD_WORKFLOW_SQL not in recorder.executed
        assert INSERT_WORKFLOW_TASKS_BULK_SQL not in recorder.executed
        fail_mock.assert_not_awaited()

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        'make_node', [_child_task_node, _child_sub_node],
        ids=['child-task', 'child-sub'],
    )
    async def test_kwargs_that_encode_reach_the_insert(
        self, make_node: Callable[[], MagicMock],
    ) -> None:
        # Control for the test above: the same harness without a refusal.
        # A SubWorkflowNode root starts its own child after the INSERT; the
        # second ENQUEUE_SUBWORKFLOW_TASK_SQL returns no row, so it stops.
        recorder = _RecordingSession(
            {ENQUEUE_SUBWORKFLOW_TASK_SQL: [_parent_node_row(), None]},
        )
        fail_mock = AsyncMock()

        with patch(
            'horsies.core.workflows.engine.encode_kwargs',
            return_value={'value': 1},
        ), patch(
            'horsies.core.workflows.engine.encode_subworkflow_kwargs',
            return_value={'value': 1},
        ):
            await _start_child(make_node(), recorder, fail_mock)

        assert INSERT_CHILD_WORKFLOW_SQL in recorder.executed
        assert INSERT_WORKFLOW_TASKS_BULK_SQL in recorder.executed
        fail_mock.assert_not_awaited()


def _real_app() -> Horsies:
    return Horsies(AppConfig(
        broker=PostgresConfig(database_url=SecretStr(DATABASE_URL)),
    ))


def _register_note_task(app: Horsies) -> TaskFunction[..., int]:
    @app.task('surrogate.note')
    def note(*, text: str, labels: dict[str, int]) -> TaskResult[int, TaskError]:
        return TaskResult(ok=len(text) + len(labels))
    return note


def _register_refused_task(app: Horsies) -> TaskFunction[..., _RefusedOk]:
    @app.task('surrogate.refused')
    def refused() -> TaskResult[_RefusedOk, TaskError]:
        return TaskResult(ok=_RefusedOk.model_construct(value=1))
    return refused


def _key(label: str) -> str:
    return f'tests.unit.surrogate.{label}.{uuid.uuid4().hex}'


class TestSpecConstructionRefusesALoneSurrogate:
    """Spec construction encodes every kwarg, so it refuses a lone surrogate.

    The value does not reach the workflow start transaction or a child start,
    where a failure to serialize is a defect.
    """

    @pytest.mark.parametrize(
        ('kwargs', 'subject'),
        [
            ({'text': LONE_SURROGATE_TEXT, 'labels': {}}, 'a string'),
            ({'text': 'ok', 'labels': {LONE_SURROGATE_TEXT: 1}}, 'a dict key'),
        ],
        ids=['string', 'key'],
    )
    def test_a_task_node_kwarg_is_hrs_031(
        self,
        kwargs: dict[str, Any],
        subject: str,
    ) -> None:
        app = _real_app()
        note = _register_note_task(app)

        with pytest.raises(WorkflowValidationError) as raised:
            _ = app.workflow(
                name='surrogate-task-node',
                tasks=[TaskNode(fn=note, kwargs=kwargs)],
                definition_key=_key('task_node'),
            )

        assert raised.value.code is ErrorCode.WORKFLOW_KWARGS_NOT_SERIALIZABLE
        assert raised.value.code.value == 'HRS-031'
        assert (
            f'{subject} holds a lone surrogate' in ' '.join(raised.value.notes)
        )

    def test_a_subworkflow_node_kwarg_is_hrs_031(self) -> None:
        app = _real_app()

        class ChildWorkflow(WorkflowDefinition[int]):
            name = 'surrogate-child'
            definition_key = _key('child')

            @classmethod
            def build_with(
                cls, app: Horsies, *args: Any, text: str = '', **params: Any,
            ) -> Any:
                raise AssertionError('spec construction refuses before this')

        with pytest.raises(WorkflowValidationError) as raised:
            _ = app.workflow(
                name='surrogate-sub-node',
                tasks=[SubWorkflowNode(
                    workflow_def=ChildWorkflow,
                    kwargs={'text': LONE_SURROGATE_TEXT},
                )],
                definition_key=_key('sub_node'),
            )

        assert raised.value.code is ErrorCode.WORKFLOW_KWARGS_NOT_SERIALIZABLE


def _surrogate_child_workflow(
    note: TaskFunction[..., int],
    kwargs: dict[str, Any],
) -> type[WorkflowDefinition[int]]:
    class ChildWorkflow(WorkflowDefinition[int]):
        name = 'surrogate-child-build'
        definition_key = _key('child_build')

        @classmethod
        def build_with(cls, app: Horsies, *args: Any, **params: Any) -> Any:
            _ = app, args, params
            node = TaskNode(fn=note, kwargs=kwargs)
            return WorkflowSpec(name=cls.name, tasks=[node], output=node)

    return ChildWorkflow


class TestChildStartOfALoneSurrogate:
    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        'kwargs',
        [
            {'text': LONE_SURROGATE_TEXT, 'labels': {}},
            {'text': 'ok', 'labels': {LONE_SURROGATE_TEXT: 1}},
        ],
        ids=['string', 'key'],
    )
    async def test_a_child_kwarg_fails_the_node_through_the_build_with_catch(
        self,
        kwargs: dict[str, Any],
    ) -> None:
        """Spec construction runs in `build_with()`, so the refusal is a node
        failure with WORKFLOW_ENQUEUE_FAILED, not a defect that raises."""
        app = _real_app()
        note = _register_note_task(app)
        child_workflow = _surrogate_child_workflow(note, kwargs)
        recorder = _RecordingSession(
            {ENQUEUE_SUBWORKFLOW_TASK_SQL: _parent_node_row()},
        )
        broker = MagicMock()
        broker.app = app
        fail_mock = AsyncMock()

        with patch(
            'horsies.core.workflows.engine._load_workflow_def_from_key',
            return_value=child_workflow,
        ), patch(
            'horsies.core.workflows.engine._fail_enqueued_task', new=fail_mock,
        ):
            child_workflow_id = await enqueue_subworkflow_task(
                recorder.session, broker, 'parent-wf', 0, {},
                parent_depth=0, root_workflow_id='parent-wf',
            )

        assert child_workflow_id is None
        fail_mock.assert_awaited_once()
        call = fail_mock.await_args
        assert call is not None
        assert call.args[3].startswith('Subworkflow build failed: ')
        assert (
            call.kwargs['error_code']
            is OperationalErrorCode.WORKFLOW_ENQUEUE_FAILED
        )
        assert INSERT_CHILD_WORKFLOW_SQL not in recorder.executed

    @pytest.mark.asyncio
    async def test_an_exception_text_from_build_with_fails_the_node(self) -> None:
        """The text comes from user code, so it can hold a lone surrogate, for
        example a file name. The failure handler encodes the error and raises
        for a value that does not encode, so the text is escaped first:
        without that, each recovery pass raises and the node never fails."""
        recorder = _RecordingSession({
            ENQUEUE_SUBWORKFLOW_TASK_SQL: _parent_node_row(),
            GET_WORKFLOW_ON_ERROR_SQL: SimpleNamespace(on_error='fail'),
            GET_FIRST_FAILED_TASK_RESULT_SQL: None,
        })
        workflow_def = MagicMock()
        workflow_def.name = 'ChildWorkflow'
        workflow_def.build_with.side_effect = ValueError(
            f'cannot open {LONE_SURROGATE_TEXT}',
        )
        broker = MagicMock()
        mark_failed = AsyncMock(return_value=True)

        with patch(
            'horsies.core.workflows.engine._load_workflow_def_from_key',
            return_value=workflow_def,
        ), patch(
            'horsies.core.workflows.engine._mark_workflow_task_failed_if_nonterminal',
            new=mark_failed,
        ), patch(
            'horsies.core.workflows.engine._process_dependents', new=AsyncMock(),
        ), patch(
            'horsies.core.workflows.engine.check_workflow_completion',
            new=AsyncMock(),
        ):
            child_workflow_id = await enqueue_subworkflow_task(
                recorder.session, broker, 'parent-wf', 0, {},
                parent_depth=0, root_workflow_id='parent-wf',
            )

        assert child_workflow_id is None
        mark_call = mark_failed.await_args
        assert mark_call is not None
        node_error = json.loads(mark_call.args[3])['err']
        assert 'cannot open name\\udc80' in node_error['message']
        workflow_errors = [
            call.args[1]['error']
            for call in recorder.session.execute.await_args_list
            if call.args[0] is SET_WORKFLOW_ERROR_SQL
        ]
        assert len(workflow_errors) == 1
        assert 'cannot open name\\udc80' in json.loads(workflow_errors[0])['message']
