# pyright: reportPrivateUsage=false
"""The handle error variants: one per code, redacted, and the sites that map them.

``HandleOperationError`` is a union with one frozen variant per
``HandleErrorCode`` member. A variant's repr hides the message and the
cause. Every variant carries ``workflow_id``. Stored data that does not
parse is ``HandleStoredDataInvalid`` with its parse error. ``get()`` folds a
status read failure into a ``TaskResult``, and the web routes map each
variant onto one response.
"""

from __future__ import annotations

import asyncio
import json
import uuid
from typing import Any, get_args
from unittest.mock import AsyncMock, MagicMock

import pytest
from sqlalchemy.exc import OperationalError

import horsies._core as core
from horsies.core.brokers.resources import EventLoopLimit
from horsies.core.codec.json_io import EmptyJsonTextError, SerializationError
from horsies.core.models.tasks import OperationalErrorCode, RetrievalCode
from horsies.core.models.workflow.enums import WorkflowStatus
from horsies.core.models.workflow.handle import WorkflowHandle
from horsies.core.models.workflow.handle_types import (
    HandleAsyncContext,
    HandleDbOperationFailed,
    HandleErrorCode,
    HandleEventLoopLimit,
    HandleLoopRunnerFailed,
    HandleOperationError,
    HandleResumeRecoveryFailed,
    HandleStoredDataInvalid,
    HandleWorkflowNotFound,
)
from horsies.core.types.result import Err, Ok, is_err, is_ok
from tests.dualrun.reference.web_routes import actions as actions_module
from tests.dualrun.reference.web_routes.actions import (
    RESUME_RECOVERY_WARNING,
    _handle_failed,
    build_router,
)

pytestmark = [pytest.mark.unit]

MESSAGE_MARKER = 'message-marker-7d2e'
CAUSE_MARKER = 'cause-marker-7d2e'
WORKFLOW_ID = '3c9e1f4a-6b2d-4c58-8e71-0a1b2c3d4e5f'
UNKNOWN_STATUS = 'NOT_A_WORKFLOW_STATUS'
_UNKNOWN_STATUS_CAUSE = core.CoreContractViolation(
    'workflow_status', f'invalid workflow status: {UNKNOWN_STATUS}',
)
"""The exception of a stored workflow status that does not parse."""


def _every_variant(cause: BaseException) -> list[HandleOperationError]:
    """One value of each variant; each cause field holds ``cause``."""
    message = f'failed: {MESSAGE_MARKER}'
    return [
        HandleWorkflowNotFound(message=message, workflow_id=WORKFLOW_ID),
        HandleDbOperationFailed(
            message=message, retryable=True, workflow_id=WORKFLOW_ID, exception=cause,
        ),
        HandleStoredDataInvalid(
            message=message, workflow_id=WORKFLOW_ID, exception=cause,
        ),
        HandleResumeRecoveryFailed(
            message=message, workflow_id=WORKFLOW_ID, exception=cause,
        ),
        HandleLoopRunnerFailed(
            message=message, workflow_id=WORKFLOW_ID, exception=cause,
        ),
        HandleEventLoopLimit(
            message=message,
            workflow_id=WORKFLOW_ID,
            loop_limit=EventLoopLimit(
                limit=5,
                held_thread_names=('MainThread',),
                current_thread_name='worker-1',
            ),
        ),
        HandleAsyncContext(message=message, workflow_id=WORKFLOW_ID),
    ]


def _variant_id(error: HandleOperationError) -> str:
    return type(error).__name__


def _db_error() -> OperationalError:
    return OperationalError('SELECT 1', {}, Exception('connection refused'))


class TestVariantCatalog:
    def test_each_code_names_exactly_one_variant(self) -> None:
        codes = [variant.code for variant in get_args(HandleOperationError.__value__)]

        assert len(codes) == len(set(codes))
        assert set(codes) == set(HandleErrorCode)

    def test_the_fixture_builds_every_variant(self) -> None:
        built = {type(error) for error in _every_variant(RuntimeError('x'))}

        assert built == set(get_args(HandleOperationError.__value__))

    @pytest.mark.parametrize(
        'error', _every_variant(RuntimeError(CAUSE_MARKER)), ids=_variant_id,
    )
    def test_repr_hides_message_and_cause(self, error: HandleOperationError) -> None:
        text = repr(error)

        assert MESSAGE_MARKER not in text
        assert CAUSE_MARKER not in text
        assert type(error).__name__ in text
        assert WORKFLOW_ID in text

    @pytest.mark.parametrize(
        'error', _every_variant(RuntimeError('x')), ids=_variant_id,
    )
    def test_every_variant_has_code_retryable_and_the_workflow_id(
        self,
        error: HandleOperationError,
    ) -> None:
        assert isinstance(error.code, HandleErrorCode)
        assert isinstance(error.retryable, bool)
        assert error.workflow_id == WORKFLOW_ID

    @pytest.mark.parametrize(
        'error', _every_variant(RuntimeError('x')), ids=_variant_id,
    )
    def test_only_a_database_failure_can_be_retryable(
        self,
        error: HandleOperationError,
    ) -> None:
        match error:
            case HandleDbOperationFailed():
                assert error.retryable is True
            case _:
                assert error.retryable is False


# =============================================================================
# Sites that read the stored workflow status
# =============================================================================


def _core_record(record_class: type, **fields: object) -> MagicMock:
    """A stand-in for a record of ``horsies._core``: a class pattern matches it."""
    record = MagicMock(spec=record_class)
    for name, value in fields.items():
        setattr(record, name, value)
    return record


def _handle_answering(method: str, answer: object) -> WorkflowHandle[Any]:
    """A handle whose core handle answers ``method`` with ``answer``."""
    broker = MagicMock()
    setattr(broker._core_handle, method, AsyncMock(return_value=answer))
    return WorkflowHandle(workflow_id=WORKFLOW_ID, broker=broker)


class TestStoredWorkflowStatus:
    @pytest.mark.asyncio
    async def test_a_known_status_parses(self) -> None:
        handle = _handle_answering(
            'workflow_status_async',
            _core_record(core.WorkflowStatusRead, status='RUNNING'),
        )

        result = await handle.status_async()

        assert is_ok(result)
        assert result.ok_value is WorkflowStatus.RUNNING

    @pytest.mark.asyncio
    async def test_an_unknown_status_is_stored_data_invalid(self) -> None:
        handle = _handle_answering(
            'workflow_status_async',
            _core_record(
                core.HandleFailed,
                code='STORED_DATA_INVALID',
                message=f'invalid workflow status: {UNKNOWN_STATUS}',
                retryable=False,
                exception=_UNKNOWN_STATUS_CAUSE,
            ),
        )

        result = await handle.status_async()

        assert is_err(result)
        err = result.err_value
        assert isinstance(err, HandleStoredDataInvalid)
        assert err.exception is _UNKNOWN_STATUS_CAUSE
        assert err.workflow_id == WORKFLOW_ID
        assert UNKNOWN_STATUS in err.message


# =============================================================================
# cancel: the outcome of horsies_workflow_cancel
# =============================================================================


def _cancel_record(record_class: type, **fields: object) -> MagicMock:
    """A stand-in for a record of ``horsies._core``: a class pattern matches it."""
    record = MagicMock(spec=record_class)
    record.workflow_id = uuid.UUID(WORKFLOW_ID)
    for name, value in fields.items():
        setattr(record, name, value)
    return record


def _core_broker(answer: object = None, *, raises: BaseException | None = None) -> MagicMock:
    """A broker whose core handle answers the cancel with ``answer``, or raises."""
    broker = MagicMock()
    match raises:
        case None:
            broker._core_handle.cancel_workflow_async = AsyncMock(return_value=answer)
        case _:
            broker._core_handle.cancel_workflow_async = AsyncMock(side_effect=raises)
    return broker


def _handle_with_core_answer(
    answer: object = None, *, raises: BaseException | None = None,
) -> WorkflowHandle[Any]:
    """A handle on ``_core_broker``."""
    return WorkflowHandle(workflow_id=WORKFLOW_ID, broker=_core_broker(answer, raises=raises))


class TestCancelOutcome:
    """cancel_async runs one horsies_workflow_cancel in the core; its record decides."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        'record_class', [core.WorkflowCancelled, core.WorkflowNotCancellable],
    )
    async def test_a_cancelled_or_terminal_workflow_is_ok(self, record_class: type) -> None:
        broker = _core_broker(_cancel_record(record_class))
        handle: WorkflowHandle[Any] = WorkflowHandle(workflow_id=WORKFLOW_ID, broker=broker)

        result = await handle.cancel_async()

        assert result == Ok(None)
        broker._core_handle.cancel_workflow_async.assert_awaited_once_with(
            uuid.UUID(WORKFLOW_ID),
        )

    @pytest.mark.asyncio
    async def test_an_unknown_workflow_is_not_found(self) -> None:
        handle = _handle_with_core_answer(_cancel_record(core.WorkflowAbsent))

        result = await handle.cancel_async()

        assert is_err(result)
        assert isinstance(result.err_value, HandleWorkflowNotFound)
        assert result.err_value.workflow_id == WORKFLOW_ID

    @pytest.mark.asyncio
    async def test_a_failure_record_is_db_operation_failed(self) -> None:
        cause = core.CoreConnectionError('cancel_workflow', CAUSE_MARKER, True)
        handle = _handle_with_core_answer(_cancel_record(
            core.HandleFailed,
            code='DB_OPERATION_FAILED',
            message=CAUSE_MARKER,
            retryable=True,
            exception=cause,
        ))

        result = await handle.cancel_async()

        assert is_err(result)
        err = result.err_value
        assert isinstance(err, HandleDbOperationFailed)
        assert err.exception is cause
        assert err.retryable is True
        assert CAUSE_MARKER in err.message

    @pytest.mark.asyncio
    async def test_a_core_boundary_error_is_db_operation_failed(self) -> None:
        not_started = core.CoreStartFailed('the core runtime could not start')
        handle = _handle_with_core_answer(raises=not_started)

        result = await handle.cancel_async()

        assert is_err(result)
        assert isinstance(result.err_value, HandleDbOperationFailed)
        assert result.err_value.exception is not_started
        assert result.err_value.retryable is False


# =============================================================================
# get() folds a status read failure into a TaskResult
# =============================================================================


def _wait_failures() -> list[tuple[Any, object]]:
    """Each failure record of the core wait, with its folded code."""
    return [
        (_core_record(core.WorkflowAbsent), RetrievalCode.WORKFLOW_NOT_FOUND),
        (
            _core_record(
                core.HandleFailed,
                code='DB_OPERATION_FAILED',
                message=CAUSE_MARKER,
                retryable=True,
                exception=core.CoreConnectionError('wait_workflow_result', CAUSE_MARKER, True),
            ),
            OperationalErrorCode.BROKER_ERROR,
        ),
        (
            _core_record(
                core.HandleFailed,
                code='STORED_DATA_INVALID',
                message=CAUSE_MARKER,
                retryable=False,
                exception=core.CoreContractViolation('wait_workflow_result', CAUSE_MARKER),
            ),
            OperationalErrorCode.BROKER_ERROR,
        ),
    ]


@pytest.mark.parametrize(
    ('answer', 'folded_code'),
    _wait_failures(),
    ids=['absent', 'db_operation_failed', 'stored_data_invalid'],
)
def test_get_folds_each_wait_failure(answer: Any, folded_code: object) -> None:
    handle = _handle_answering('wait_workflow_result_async', answer)

    result = asyncio.run(handle.get_async(timeout_ms=0))

    assert result.is_err()
    task_error = result.unwrap_err()
    assert task_error.error_code is folded_code


# =============================================================================
# tasks(): the stored subworkflow summary
# =============================================================================


def _task_row(summary: str | None, result_json: str | None = None) -> MagicMock:
    return _core_record(
        core.StoredNodeRecord,
        node_id='parent:0',
        task_index=0,
        task_name='child_wf',
        status='COMPLETED',
        result_json=result_json,
        started_at=None,
        completed_at=None,
        sub_workflow_id=uuid.UUID('00000000-0000-0000-0000-0000000000c1'),
        sub_workflow_summary_json=summary,
        is_subworkflow=True,
        sub_definition_key=None,
    )


def _tasks_handle(
    summary: str | None, result_json: str | None = None,
) -> WorkflowHandle[Any]:
    return _handle_answering(
        'workflow_nodes_async',
        _core_record(core.WorkflowNodesRead, nodes=[_task_row(summary, result_json)]),
    )


class TestStoredSubworkflowSummary:
    @pytest.mark.asyncio
    async def test_a_valid_summary_parses(self) -> None:
        handle = _tasks_handle(json.dumps({
            'status': 'COMPLETED',
            'total_tasks': 1,
            'completed_tasks': 1,
            'failed_tasks': 0,
            'skipped_tasks': 0,
        }))

        result = await handle.tasks_async()

        assert is_ok(result)
        summary = result.ok_value[0].sub_workflow_summary
        assert summary is not None
        assert summary.status is WorkflowStatus.COMPLETED

    @pytest.mark.asyncio
    async def test_a_stored_null_is_no_summary(self) -> None:
        handle = _tasks_handle(None)

        result = await handle.tasks_async()

        assert is_ok(result)
        assert result.ok_value[0].sub_workflow_summary is None

    @pytest.mark.asyncio
    async def test_an_empty_summary_is_stored_data_invalid(self) -> None:
        handle = _tasks_handle('')

        result = await handle.tasks_async()

        assert is_err(result)
        err = result.err_value
        assert isinstance(err, HandleStoredDataInvalid)
        assert err.code is HandleErrorCode.STORED_DATA_INVALID
        assert not err.retryable
        assert isinstance(err.exception, EmptyJsonTextError)
        assert err.message == (
            'sub_workflow_summary JSON corrupt for node parent:0: '
            'JSON parse failed: the text is empty'
        )

    @pytest.mark.asyncio
    async def test_corrupt_summary_json_is_stored_data_invalid(self) -> None:
        handle = _tasks_handle('{not valid json')

        result = await handle.tasks_async()

        assert is_err(result)
        err = result.err_value
        assert isinstance(err, HandleStoredDataInvalid)
        assert isinstance(err.exception, SerializationError)
        assert 'sub_workflow_summary JSON corrupt for node parent:0' in err.message

    @pytest.mark.asyncio
    async def test_a_summary_past_the_depth_bound_is_stored_data_invalid(self) -> None:
        handle = _tasks_handle('{"status":' + '[' * 1_100 + ']' * 1_100 + '}')

        result = await handle.tasks_async()

        assert is_err(result)
        err = result.err_value
        assert isinstance(err, HandleStoredDataInvalid)
        assert isinstance(err.exception, SerializationError)
        assert 'nested deeper than' in err.message

    @pytest.mark.asyncio
    async def test_an_invalid_summary_is_stored_data_invalid(self) -> None:
        handle = _tasks_handle('{"status": "NOT_A_STATUS", "total_tasks": 1}')

        result = await handle.tasks_async()

        assert is_err(result)
        err = result.err_value
        assert isinstance(err, HandleStoredDataInvalid)
        assert isinstance(err.exception, ValueError)
        assert not isinstance(err.exception, SerializationError)
        assert 'sub_workflow_summary invalid for node parent:0' in err.message
        assert 'NOT_A_STATUS' in str(err.exception)


class TestStoredNodeResult:
    """tasks(): NULL is a node with no result; an empty text is corrupt data."""

    @pytest.mark.asyncio
    async def test_a_stored_null_is_no_result(self) -> None:
        handle = _tasks_handle(None, result_json=None)

        result = await handle.tasks_async()

        assert is_ok(result)
        assert result.ok_value[0].result is None

    @pytest.mark.asyncio
    async def test_an_empty_result_is_a_deserialization_error(self) -> None:
        handle = _tasks_handle(None, result_json='')

        result = await handle.tasks_async()

        assert is_ok(result)
        node_result = result.ok_value[0].result
        assert node_result is not None
        assert node_result.is_err()
        error = node_result.unwrap_err()
        assert error.error_code is OperationalErrorCode.RESULT_DESERIALIZATION_ERROR
        assert error.message == (
            'Result JSON corrupt for node parent:0: '
            'JSON parse failed: the text is empty'
        )

    @pytest.mark.asyncio
    async def test_a_result_that_is_not_json_names_the_node(self) -> None:
        handle = _tasks_handle(None, result_json='{not valid json')

        result = await handle.tasks_async()

        assert is_ok(result)
        node_result = result.ok_value[0].result
        assert node_result is not None
        error = node_result.unwrap_err()
        assert error.error_code is OperationalErrorCode.RESULT_DESERIALIZATION_ERROR
        assert error.message is not None
        assert error.message.startswith(
            'Result JSON corrupt for node parent:0: JSON parse failed: ',
        )


# =============================================================================
# Web: each variant maps to one response
# =============================================================================


WEB_STATUS_CODES: dict[type[Any], int] = {
    HandleWorkflowNotFound: 404,
    HandleDbOperationFailed: 503,
    HandleLoopRunnerFailed: 503,
    HandleEventLoopLimit: 503,
    HandleStoredDataInvalid: 500,
}


class TestStoredWorkflowResult:
    @pytest.mark.asyncio
    async def test_a_result_past_the_depth_bound_is_a_deserialization_error(
        self,
    ) -> None:
        """Before, `RecursionError` left `get()`."""
        stored = (
            '{"__h_task_result__":true,"ok":'
            + '[' * 1_100 + ']' * 1_100
            + ',"err":null}'
        )
        broker = MagicMock()
        handle: WorkflowHandle[Any] = WorkflowHandle(workflow_id=WORKFLOW_ID, broker=broker)
        handle.out_type = int

        result = handle._completed_result(stored)

        assert result.err is not None
        assert (
            result.err.error_code
            == OperationalErrorCode.RESULT_DESERIALIZATION_ERROR
        )
        assert result.err.message is not None
        assert 'nested deeper than' in result.err.message


class TestWebMapping:
    def test_every_variant_has_a_web_answer(self) -> None:
        answered = set(WEB_STATUS_CODES) | {HandleAsyncContext, HandleResumeRecoveryFailed}

        assert answered == set(get_args(HandleOperationError.__value__))

    @pytest.mark.parametrize(
        'error',
        [
            error
            for error in _every_variant(RuntimeError('x'))
            if type(error) in WEB_STATUS_CODES
        ],
        ids=_variant_id,
    )
    def test_variant_maps_to_its_status_code(self, error: HandleOperationError) -> None:
        outcome = _handle_failed(error)

        assert outcome.status_code == WEB_STATUS_CODES[type(error)]
        assert outcome.content == {'detail': error.message}

    def test_a_resume_recovery_failure_outside_the_resume_route_fails_an_assertion(
        self,
    ) -> None:
        error = HandleResumeRecoveryFailed(
            message='recovery failed', workflow_id=WORKFLOW_ID, exception=_db_error(),
        )

        with pytest.raises(AssertionError, match='RESUME_RECOVERY_FAILED'):
            _handle_failed(error)


def _resume_route(broker: Any) -> Any:
    router = build_router(broker)
    for route in router.routes:
        if getattr(route, 'path', None) == '/workflows/{workflow_id}/resume':
            return getattr(route, 'endpoint')
    raise AssertionError('the resume route is not registered')


class TestResumeRoute:
    """The resume route answers from the variant; it does not read the status."""

    def test_recovery_failure_is_resumed_with_a_warning(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        broker = MagicMock()
        error = HandleResumeRecoveryFailed(
            message='recovery failed', workflow_id=WORKFLOW_ID, exception=_db_error(),
        )
        monkeypatch.setattr(
            actions_module, 'resume_workflow', AsyncMock(return_value=Err(error)),
        )

        response = asyncio.run(_resume_route(broker)(workflow_id=WORKFLOW_ID))

        assert response.status_code == 200
        body = json.loads(response.body)
        assert body['outcome'] == 'resumed'
        assert body['warning'] == RESUME_RECOVERY_WARNING
        broker.select_resources.assert_not_called()

    def test_failure_before_the_commit_is_service_unavailable(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        broker = MagicMock()
        error = HandleDbOperationFailed(
            message='resume failed',
            retryable=True,
            workflow_id=WORKFLOW_ID,
            exception=_db_error(),
        )
        monkeypatch.setattr(
            actions_module, 'resume_workflow', AsyncMock(return_value=Err(error)),
        )

        response = asyncio.run(_resume_route(broker)(workflow_id=WORKFLOW_ID))

        assert response.status_code == 503
        assert json.loads(response.body) == {'detail': 'resume failed'}
        broker.select_resources.assert_not_called()

    def test_a_resumed_run_is_resumed_without_a_warning(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        broker = MagicMock()
        monkeypatch.setattr(
            actions_module, 'resume_workflow', AsyncMock(return_value=Ok(True)),
        )

        response = asyncio.run(_resume_route(broker)(workflow_id=WORKFLOW_ID))

        assert response.status_code == 200
        body = json.loads(response.body)
        assert body['outcome'] == 'resumed'
        assert body['warning'] is None
