"""The task action error variants, the cancel outcomes that build them, and the web mapping.

``TaskActionError`` is a union with one frozen variant per
``TaskActionErrorCode`` member. A variant's repr hides the message and the
cause. Every variant carries ``task_id``. The cancel tests run on a mocked
core handle: the record of ``cancel_task_async`` is the seam. The records
that contradict the function (a lost claim, no observed status) are refused
by the core with a panic; its own tests cover them.
"""

from __future__ import annotations

import uuid
from typing import Any, get_args
from unittest.mock import AsyncMock, MagicMock

import pytest

from horsies._core import (
    CancelFailed,
    CoreConnectionError,
    CancelRefusedStatus,
    CancelRefusedWorkflowTask,
    CancelStoredDataInvalid,
    CancelTaskAbsent,
    CoreStartFailed,
    TaskCancelledRecord,
    TooManyPendingCalls,
)
from horsies.core.brokers.resources import EventLoopLimit
from horsies.core.lifecycle.outcomes import OutcomeDecodeError
from horsies.core.types.result import Ok, Result, is_err
from horsies.core.types.status import TaskStatus
from horsies.monitoring.task_actions import (
    TaskActionDbOperationFailed,
    TaskActionError,
    TaskActionErrorCode,
    TaskActionEventLoopLimit,
    TaskActionNotCancellable,
    TaskActionNotFound,
    TaskActionStoredDataInvalid,
    TaskActionWorkflowTask,
    TaskCancelled,
    cancel_task,
)
from tests.dualrun.reference.web_routes.actions import (
    _succeeded,  # pyright: ignore[reportPrivateUsage]
    _task_action_failed,  # pyright: ignore[reportPrivateUsage]
)

pytestmark = [pytest.mark.unit]

MESSAGE_MARKER = 'message-marker-7d2e'
CAUSE_MARKER = 'cause-marker-7d2e'
TASK_ID = uuid.UUID('3f9c1a2b-4d5e-4f60-8a71-b2c3d4e5f607')
LOOP_LIMIT = EventLoopLimit(
    limit=5,
    held_thread_names=('MainThread',),
    current_thread_name='worker-1',
)


def _every_variant(cause: BaseException) -> list[TaskActionError]:
    """One value of each variant; each cause field holds ``cause``."""
    message = f'failed: {MESSAGE_MARKER}'
    return [
        TaskActionNotFound(message=message, task_id=TASK_ID),
        TaskActionNotCancellable(
            message=message, task_id=TASK_ID, current_status=TaskStatus.COMPLETED,
        ),
        TaskActionWorkflowTask(
            message=message, task_id=TASK_ID, current_status=TaskStatus.FAILED,
        ),
        TaskActionDbOperationFailed(
            message=message, retryable=True, task_id=TASK_ID, exception=cause,
        ),
        TaskActionEventLoopLimit(
            message=message, task_id=TASK_ID, loop_limit=LOOP_LIMIT,
        ),
        TaskActionStoredDataInvalid(message=message, task_id=TASK_ID, exception=cause),
    ]


def _variant_id(error: TaskActionError) -> str:
    return type(error).__name__


class TestVariantCatalog:
    def test_each_code_names_exactly_one_variant(self) -> None:
        codes = [variant.code for variant in get_args(TaskActionError.__value__)]

        assert len(codes) == len(set(codes))
        assert set(codes) == set(TaskActionErrorCode)

    def test_the_fixture_builds_every_variant(self) -> None:
        built = {type(error) for error in _every_variant(RuntimeError('x'))}

        assert built == set(get_args(TaskActionError.__value__))

    def test_the_code_is_a_string(self) -> None:
        assert TaskActionErrorCode.TASK_NOT_FOUND == 'TASK_NOT_FOUND'
        assert TaskActionErrorCode('STORED_DATA_INVALID') is (
            TaskActionErrorCode.STORED_DATA_INVALID
        )

    @pytest.mark.parametrize(
        'error', _every_variant(RuntimeError(CAUSE_MARKER)), ids=_variant_id,
    )
    def test_repr_hides_message_and_cause(self, error: TaskActionError) -> None:
        text = repr(error)

        assert MESSAGE_MARKER not in text
        assert CAUSE_MARKER not in text
        assert type(error).__name__ in text
        assert str(TASK_ID) in text

    @pytest.mark.parametrize(
        'error', _every_variant(RuntimeError('x')), ids=_variant_id,
    )
    def test_repr_shows_the_status_and_the_limit(self, error: TaskActionError) -> None:
        text = repr(error)

        match error:
            case TaskActionNotCancellable() | TaskActionWorkflowTask():
                assert repr(error.current_status) in text
            case TaskActionEventLoopLimit():
                assert repr(LOOP_LIMIT) in text
            case _:
                assert 'current_status' not in text

    @pytest.mark.parametrize(
        'error', _every_variant(RuntimeError('x')), ids=_variant_id,
    )
    def test_only_a_database_failure_can_be_retryable(
        self, error: TaskActionError,
    ) -> None:
        match error:
            case TaskActionDbOperationFailed():
                assert error.retryable is True
            case _:
                assert error.retryable is False


def _broker_answering(answer: object) -> tuple[MagicMock, AsyncMock]:
    """A broker whose core handle answers the cancel with ``answer``: a
    record, or an exception that the call raises."""
    handle = MagicMock()
    match answer:
        case BaseException():
            handle.cancel_task_async = AsyncMock(side_effect=answer)
        case _:
            handle.cancel_task_async = AsyncMock(return_value=answer)
    broker = MagicMock()
    broker._core_handle = handle
    return broker, handle.cancel_task_async


async def _cancel(
    answer: object,
    *,
    include_running: bool = True,
) -> tuple[Result[TaskCancelled, TaskActionError], AsyncMock]:
    broker, call = _broker_answering(answer)
    result = await cancel_task(broker, TASK_ID, include_running=include_running)
    return result, call


def _record(record_class: type, **fields: object) -> MagicMock:
    """A stand-in for a core record: an instance of ``record_class`` for a
    class pattern, with ``task_id`` and ``fields``."""
    record = MagicMock(spec=record_class)
    record.task_id = TASK_ID
    for name, value in fields.items():
        setattr(record, name, value)
    return record


class TestCancelOutcome:
    """The record of ``cancel_task_async`` decides the answer."""

    @pytest.mark.asyncio
    async def test_an_applied_cancel_reports_the_status_it_left(self) -> None:
        result, call = await _cancel(_record(TaskCancelledRecord, was_status='RUNNING'))

        assert result == Ok(TaskCancelled(task_id=TASK_ID, was_status=TaskStatus.RUNNING))
        call.assert_awaited_once_with(TASK_ID, include_running=True)

    @pytest.mark.asyncio
    async def test_the_running_choice_reaches_the_core(self) -> None:
        _, call = await _cancel(
            _record(CancelRefusedStatus, current_status='RUNNING'),
            include_running=False,
        )

        call.assert_awaited_once_with(TASK_ID, include_running=False)

    @pytest.mark.asyncio
    async def test_a_live_status_outside_the_choice_is_not_cancellable(self) -> None:
        result, _ = await _cancel(
            _record(CancelRefusedStatus, current_status='RUNNING'),
            include_running=False,
        )

        assert is_err(result)
        assert result.err_value == TaskActionNotCancellable(
            message=f'Task {TASK_ID} cannot be cancelled from status RUNNING.',
            task_id=TASK_ID,
            current_status=TaskStatus.RUNNING,
        )

    @pytest.mark.parametrize('status', ['COMPLETED', 'CANCELLED'])
    @pytest.mark.asyncio
    async def test_a_finished_plain_task_is_not_cancellable_with_its_status(
        self, status: str,
    ) -> None:
        result, _ = await _cancel(_record(CancelRefusedStatus, current_status=status))

        assert is_err(result)
        error = result.err_value
        assert isinstance(error, TaskActionNotCancellable)
        assert error.current_status is TaskStatus(status)

    @pytest.mark.parametrize('status', ['PENDING', 'FAILED'], ids=['live', 'finished'])
    @pytest.mark.asyncio
    async def test_a_workflow_task_carries_its_status(self, status: str) -> None:
        result, _ = await _cancel(_record(CancelRefusedWorkflowTask, current_status=status))

        assert is_err(result)
        error = result.err_value
        assert isinstance(error, TaskActionWorkflowTask)
        assert error.current_status is TaskStatus(status)

    @pytest.mark.asyncio
    async def test_an_absent_task_is_not_found(self) -> None:
        result, _ = await _cancel(_record(CancelTaskAbsent))

        assert is_err(result)
        assert result.err_value == TaskActionNotFound(
            message=f'Task {TASK_ID} does not exist.', task_id=TASK_ID,
        )

    @pytest.mark.asyncio
    async def test_stored_data_that_does_not_parse_is_stored_data_invalid(self) -> None:
        result, _ = await _cancel(
            _record(CancelStoredDataInvalid, message=f'history status {CAUSE_MARKER}'),
        )

        assert is_err(result)
        error = result.err_value
        assert isinstance(error, TaskActionStoredDataInvalid)
        assert isinstance(error.exception, OutcomeDecodeError)
        assert error.task_id == TASK_ID
        assert CAUSE_MARKER in error.message


class TestCancelFailures:
    @pytest.mark.asyncio
    async def test_a_failure_record_keeps_its_message_and_classification(self) -> None:
        cause = CoreConnectionError('cancel_task', CAUSE_MARKER, True)
        result, _ = await _cancel(
            _record(
                CancelFailed, message=f'cancel: {CAUSE_MARKER}', retryable=True, exception=cause,
            ),
        )

        assert is_err(result)
        error = result.err_value
        assert isinstance(error, TaskActionDbOperationFailed)
        assert error.retryable is True
        assert error.task_id == TASK_ID
        assert CAUSE_MARKER in error.message
        assert error.exception is cause

    @pytest.mark.parametrize(
        ('error', 'retryable'),
        [
            (TooManyPendingCalls('CoreHandle.cancel_task_async', 65_536), True),
            (CoreStartFailed('the core runtime could not start'), False),
        ],
        ids=['too-many-calls', 'core-start-failed'],
    )
    @pytest.mark.asyncio
    async def test_a_core_that_cannot_run_the_cancel_is_a_database_failure(
        self, error: Exception, retryable: bool,
    ) -> None:
        result, _ = await _cancel(error)

        assert is_err(result)
        failed = result.err_value
        assert isinstance(failed, TaskActionDbOperationFailed)
        assert failed.exception is error
        assert failed.retryable is retryable


def _web_cases() -> list[tuple[TaskActionError, int, dict[str, Any]]]:
    message = 'refused'
    return [
        (TaskActionNotFound(message=message, task_id=TASK_ID), 404, {'detail': message}),
        (
            TaskActionWorkflowTask(
                message=message, task_id=TASK_ID, current_status=TaskStatus.PENDING,
            ),
            400,
            {'code': 'TASK_IS_WORKFLOW_TASK'},
        ),
        (
            TaskActionNotCancellable(
                message=message, task_id=TASK_ID, current_status=TaskStatus.COMPLETED,
            ),
            409,
            {'code': 'TASK_NOT_CANCELLABLE', 'current_status': 'COMPLETED'},
        ),
        (
            TaskActionDbOperationFailed(
                message=message,
                retryable=True,
                task_id=TASK_ID,
                exception=RuntimeError('x'),
            ),
            503,
            {'detail': message},
        ),
        (
            TaskActionEventLoopLimit(
                message=message, task_id=TASK_ID, loop_limit=LOOP_LIMIT,
            ),
            503,
            {'detail': message},
        ),
        (
            TaskActionStoredDataInvalid(
                message=message, task_id=TASK_ID, exception=RuntimeError('x'),
            ),
            500,
            {'detail': message},
        ),
    ]


class TestWebMapping:
    def test_the_cases_cover_every_variant(self) -> None:
        covered = {type(error) for error, _, _ in _web_cases()}

        assert covered == set(get_args(TaskActionError.__value__))

    @pytest.mark.parametrize(
        'error,status_code,content',
        _web_cases(),
        ids=[type(error).__name__ for error, _, _ in _web_cases()],
    )
    def test_each_variant_maps_to_its_response(
        self,
        error: TaskActionError,
        status_code: int,
        content: dict[str, Any],
    ) -> None:
        outcome = _task_action_failed(error)

        assert outcome.status_code == status_code
        assert outcome.content == content


class TestSuccessEnvelope:
    def test_the_success_response_has_outcome_status_and_warning_only(self) -> None:
        outcome = _succeeded('cancelled', was_status='RUNNING')

        assert outcome.status_code == 200
        assert outcome.content == {
            'outcome': 'cancelled',
            'was_status': 'RUNNING',
            'warning': None,
        }
