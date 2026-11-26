"""The task action error variants, the cancel outcomes that build them, and the web mapping.

``TaskActionError`` is a union with one frozen variant per
``TaskActionErrorCode`` member. A variant's repr hides the message and the
cause. Every variant carries ``task_id``. The cancel tests run on a mocked
core handle: the answer of ``cancel_task_async`` is the seam. The core
builds a refused or failed cancel as its generated class, with Python's
message and exception; ``tests/integration/test_monitoring_task_actions.py``
pins those values through the built module, and the core's own tests pin
each text. The records that contradict the function (a lost claim, no
observed status) are refused by the core with a panic; its own tests cover
them.
"""

from __future__ import annotations

import uuid
from unittest.mock import AsyncMock, MagicMock

import pytest

from horsies._core import (
    CoreConnectionError,
    CoreStartFailed,
    TaskCancelledRecord,
    TooManyPendingCalls,
)
from horsies.core.lifecycle.outcomes import OutcomeDecodeError
from horsies.core.types.result import Err, Ok, Result, is_err
from horsies.core.types.status import TaskStatus
from horsies.monitoring.task_actions import (
    TaskActionCoreError,
    TaskActionDatabaseOperationFailed,
    TaskActionError,
    TaskActionErrorCode,
    TaskActionNotCancellable,
    TaskActionNotFound,
    TaskActionStoredDataInvalid,
    TaskActionWorkflowTask,
    TaskCancelled,
    cancel_task,
)
from tests.union_members import union_members

pytestmark = [pytest.mark.unit]

MESSAGE_MARKER = 'message-marker-7d2e'
CAUSE_MARKER = 'cause-marker-7d2e'
TASK_ID = uuid.UUID('3f9c1a2b-4d5e-4f60-8a71-b2c3d4e5f607')


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
        TaskActionDatabaseOperationFailed(
            message=message, retryable=True, task_id=TASK_ID, exception=cause,
        ),
        TaskActionStoredDataInvalid(message=message, task_id=TASK_ID, exception=cause),
    ]


def _variant_id(error: TaskActionError) -> str:
    return type(error).__name__


class TestVariantCatalog:
    def test_each_code_names_exactly_one_variant(self) -> None:
        codes = [variant.code for variant in union_members(TaskActionError)]

        assert len(codes) == len(set(codes))
        assert set(codes) == set(TaskActionErrorCode)

    def test_the_fixture_builds_every_variant(self) -> None:
        built = {type(error) for error in _every_variant(RuntimeError('x'))}

        assert built == set(union_members(TaskActionError))

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
    def test_repr_shows_the_status(self, error: TaskActionError) -> None:
        text = repr(error)

        match error:
            case TaskActionNotCancellable() | TaskActionWorkflowTask():
                assert repr(error.current_status) in text
            case _:
                assert 'current_status' not in text

    @pytest.mark.parametrize(
        'error', _every_variant(RuntimeError('x')), ids=_variant_id,
    )
    def test_only_a_database_failure_can_be_retryable(
        self, error: TaskActionError,
    ) -> None:
        match error:
            case TaskActionDatabaseOperationFailed():
                assert error.retryable is True
            case _:
                assert error.retryable is False


def _broker_answering(answer: object) -> tuple[MagicMock, AsyncMock]:
    """A broker whose core handle answers the cancel with ``answer``: a
    record or a core error class, or an exception that the call raises."""
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


def _core_errors() -> list[TaskActionError]:
    """One value of each class that the core builds for a refused or failed
    cancel, with the core's texts."""
    return [
        TaskActionNotFound(message=f'Task {TASK_ID} does not exist.', task_id=TASK_ID),
        TaskActionNotCancellable(
            message=f'Task {TASK_ID} cannot be cancelled from status RUNNING.',
            task_id=TASK_ID,
            current_status=TaskStatus.RUNNING,
        ),
        TaskActionWorkflowTask(
            message=(
                f'Task {TASK_ID} belongs to a workflow; use the workflow actions instead.'
            ),
            task_id=TASK_ID,
            current_status=TaskStatus.PENDING,
        ),
        TaskActionDatabaseOperationFailed(
            message=f'Task cancel failed: connection failed: {CAUSE_MARKER}',
            retryable=True,
            task_id=TASK_ID,
            exception=CoreConnectionError(
                'cancel_task', f'connection failed: {CAUSE_MARKER}', True,
            ),
        ),
        TaskActionStoredDataInvalid(
            message=(
                'Task cancel failed: stored data does not parse: '
                f'terminalization outcome decode failed: {CAUSE_MARKER}'
            ),
            task_id=TASK_ID,
            exception=OutcomeDecodeError(
                f'terminalization outcome decode failed: {CAUSE_MARKER}',
            ),
        ),
    ]


class TestCancelOutcome:
    """The answer of ``cancel_task_async`` decides the result."""

    @pytest.mark.asyncio
    async def test_an_applied_cancel_reports_the_status_it_left(self) -> None:
        result, call = await _cancel(_record(TaskCancelledRecord, was_status='RUNNING'))

        assert result == Ok(TaskCancelled(task_id=TASK_ID, was_status=TaskStatus.RUNNING))
        call.assert_awaited_once_with(TASK_ID, include_running=True)

    @pytest.mark.asyncio
    async def test_the_running_choice_reaches_the_core(self) -> None:
        _, call = await _cancel(_core_errors()[1], include_running=False)

        call.assert_awaited_once_with(TASK_ID, include_running=False)

    def test_the_core_errors_cover_every_class_of_the_core(self) -> None:
        built = {type(error) for error in _core_errors()}

        assert built == set(union_members(TaskActionCoreError))

    @pytest.mark.parametrize('error', _core_errors(), ids=_variant_id)
    @pytest.mark.asyncio
    async def test_a_core_error_is_the_answer_as_it_is(
        self, error: TaskActionError,
    ) -> None:
        result, _ = await _cancel(error)

        assert result == Err(error)
        assert is_err(result)
        assert result.err_value is error


class TestCancelFailures:
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
        assert isinstance(failed, TaskActionDatabaseOperationFailed)
        assert failed.exception is error
        assert failed.retryable is retryable


