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
from typing import Any, get_args
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from sqlalchemy.exc import OperationalError

from horsies.core.brokers.resources import EventLoopLimit, ListenerDisabled
from horsies.core.codec.json_io import SerializationError
from horsies.core.models.tasks import OperationalErrorCode, RetrievalCode
from horsies.core.models.workflow.enums import WorkflowStatus
from horsies.core.models.workflow.handle import (
    GET_WORKFLOW_STATUS_SQL,
    WorkflowHandle,
)
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
from horsies.web.routes import actions as actions_module
from horsies.web.routes.actions import (
    RESUME_RECOVERY_WARNING,
    _handle_failed,
    build_router,
)
from tests.broker_resources_support import (
    attach_resources,
    fake_resources,
    resources_of,
)

pytestmark = [pytest.mark.unit]

MESSAGE_MARKER = 'message-marker-7d2e'
CAUSE_MARKER = 'cause-marker-7d2e'
WORKFLOW_ID = '3c9e1f4a-6b2d-4c58-8e71-0a1b2c3d4e5f'
UNKNOWN_STATUS = 'NOT_A_WORKFLOW_STATUS'


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


def _status_row(status: str) -> MagicMock:
    row = MagicMock()
    row.status = status
    return row


def _handle_with_session(session: AsyncMock) -> WorkflowHandle[Any]:
    session.info = {}
    session.commit = AsyncMock()
    cm = AsyncMock()
    cm.__aenter__ = AsyncMock(return_value=session)
    cm.__aexit__ = AsyncMock(return_value=False)
    broker = MagicMock()
    attach_resources(
        broker,
        fake_resources(
            session_factory=MagicMock(return_value=cm),
            listener=ListenerDisabled(),
        ),
    )
    return WorkflowHandle(workflow_id=WORKFLOW_ID, broker=broker)


def _status_session(stored_status: str) -> AsyncMock:
    """A session whose workflow status read returns ``stored_status``."""
    status_result = MagicMock()
    status_result.fetchone.return_value = _status_row(stored_status)

    def execute(statement: Any, *_args: Any, **_kwargs: Any) -> MagicMock:
        if statement is GET_WORKFLOW_STATUS_SQL:
            return status_result
        return MagicMock()

    session = AsyncMock()
    session.execute = AsyncMock(side_effect=execute)
    return session


class TestStoredWorkflowStatus:
    @pytest.mark.asyncio
    async def test_a_known_status_parses(self) -> None:
        handle = _handle_with_session(_status_session('RUNNING'))

        result = await handle.status_async()

        assert is_ok(result)
        assert result.ok_value is WorkflowStatus.RUNNING

    @pytest.mark.asyncio
    async def test_an_unknown_status_is_stored_data_invalid(self) -> None:
        handle = _handle_with_session(_status_session(UNKNOWN_STATUS))

        result = await handle.status_async()

        assert is_err(result)
        err = result.err_value
        assert isinstance(err, HandleStoredDataInvalid)
        assert isinstance(err.exception, ValueError)
        assert err.workflow_id == WORKFLOW_ID
        assert UNKNOWN_STATUS in err.message

    @pytest.mark.asyncio
    async def test_cancel_of_an_unknown_status_is_stored_data_invalid(self) -> None:
        """The cancel transaction does not commit."""
        session = _status_session(UNKNOWN_STATUS)
        handle = _handle_with_session(session)

        result = await handle.cancel_async()

        assert is_err(result)
        err = result.err_value
        assert isinstance(err, HandleStoredDataInvalid)
        assert isinstance(err.exception, ValueError)
        assert UNKNOWN_STATUS in err.message
        session.commit.assert_not_awaited()


# =============================================================================
# get() folds a status read failure into a TaskResult
# =============================================================================


def _status_read_failures() -> list[tuple[Any, object]]:
    """Each variant that ``_status`` can return, with its folded code."""
    message = 'status read failed'
    return [
        (
            HandleWorkflowNotFound(message=message, workflow_id=WORKFLOW_ID),
            RetrievalCode.WORKFLOW_NOT_FOUND,
        ),
        (
            HandleDbOperationFailed(
                message=message,
                retryable=True,
                workflow_id=WORKFLOW_ID,
                exception=_db_error(),
            ),
            OperationalErrorCode.BROKER_ERROR,
        ),
        (
            HandleStoredDataInvalid(
                message=message,
                workflow_id=WORKFLOW_ID,
                exception=ValueError(UNKNOWN_STATUS),
            ),
            OperationalErrorCode.BROKER_ERROR,
        ),
    ]


@pytest.mark.parametrize(
    ('status_error', 'folded_code'),
    _status_read_failures(),
    ids=lambda value: type(value).__name__,
)
def test_get_folds_each_status_read_failure(
    status_error: Any, folded_code: object,
) -> None:
    broker = MagicMock()
    attach_resources(broker, fake_resources(listener=ListenerDisabled()))
    handle: WorkflowHandle[Any] = WorkflowHandle(
        workflow_id=WORKFLOW_ID, broker=broker,
    )

    with patch.object(
        handle, '_status', AsyncMock(return_value=Err(status_error)),
    ):
        result = asyncio.run(handle.get_async(timeout_ms=0))

    assert result.is_err()
    task_error = result.unwrap_err()
    assert task_error.error_code is folded_code
    assert task_error.message == 'status read failed'


# =============================================================================
# tasks(): the stored subworkflow summary
# =============================================================================


def _task_row(summary: str) -> MagicMock:
    row = MagicMock()
    row.node_id = 'parent:0'
    row.task_index = 0
    row.task_name = 'child_wf'
    row.status = 'COMPLETED'
    row.result = None
    row.started_at = None
    row.completed_at = None
    row.sub_workflow_id = 'child-id'
    row.sub_workflow_summary = summary
    row.is_subworkflow = True
    row.sub_definition_key = None
    return row


def _tasks_handle(summary: str) -> WorkflowHandle[Any]:
    tasks_result = MagicMock()
    tasks_result.fetchall.return_value = [_task_row(summary)]
    session = AsyncMock()
    session.execute = AsyncMock(return_value=tasks_result)
    return _handle_with_session(session)


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
        row = MagicMock()
        row.result = (
            '{"__h_task_result__":true,"ok":'
            + '[' * 1_100 + ']' * 1_100
            + ',"err":null}'
        )
        read = MagicMock()
        read.fetchone.return_value = row
        session = AsyncMock()
        session.execute = AsyncMock(return_value=read)
        handle = _handle_with_session(session)
        handle.out_type = int

        result = await handle._get_result(await resources_of(handle.broker))

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
