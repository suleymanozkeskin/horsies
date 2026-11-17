"""The task action error variants, the cancel outcomes that build them, and the web mapping.

``TaskActionError`` is a union with one frozen variant per
``TaskActionErrorCode`` member. A variant's repr hides the message and the
cause. Every variant carries ``task_id``. The cancel tests run on a mocked
autocommit connection: the one ``horsies_cancel_task`` row is the seam, and
the real outcome decoder reads it.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any, get_args
from unittest.mock import AsyncMock, MagicMock

import pytest
from sqlalchemy.exc import OperationalError

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
from horsies.web.routes.actions import (
    _succeeded,  # pyright: ignore[reportPrivateUsage]
    _task_action_failed,  # pyright: ignore[reportPrivateUsage]
)
from tests.broker_resources_support import attach_resources, fake_resources

pytestmark = [pytest.mark.unit]

MESSAGE_MARKER = 'message-marker-7d2e'
CAUSE_MARKER = 'cause-marker-7d2e'
TASK_ID = uuid.UUID('3f9c1a2b-4d5e-4f60-8a71-b2c3d4e5f607')
NOW = datetime(2026, 11, 1, 12, 0, tzinfo=timezone.utc)
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


def _row(
    outcome: str,
    *,
    status: str | None = None,
    guard: str | None = None,
    kind: str | None = None,
) -> dict[str, Any]:
    """One row of ``horsies_cancel_task`` as the driver returns it."""
    return {
        'task_id': TASK_ID,
        'ordinality': None,
        'outcome': outcome,
        'terminal_at': NOW if kind is not None else None,
        'terminalization_kind': kind,
        'observed_status': status,
        'observed_worker_id': None,
        'observed_claimed_at': None,
        'guard_kind': guard,
        'observed_guard': None,
    }


def _broker_with_rows(
    rows: list[dict[str, Any]] | BaseException,
) -> tuple[MagicMock, AsyncMock, AsyncMock]:
    """A broker whose session's autocommit connection answers the cancel with ``rows``."""
    connection = AsyncMock()
    match rows:
        case BaseException():
            connection.execute = AsyncMock(side_effect=rows)
        case list():
            result = MagicMock()
            result.mappings.return_value.all.return_value = rows
            connection.execute = AsyncMock(return_value=result)
    session = AsyncMock()
    session.commit = AsyncMock()
    session.connection = AsyncMock(return_value=connection)
    session_ctx = AsyncMock()
    session_ctx.__aenter__ = AsyncMock(return_value=session)
    session_ctx.__aexit__ = AsyncMock(return_value=False)
    broker = MagicMock()
    attach_resources(
        broker, fake_resources(session_factory=MagicMock(return_value=session_ctx)),
    )
    return broker, session, connection


async def _cancel(
    rows: list[dict[str, Any]] | BaseException,
    *,
    include_running: bool = True,
) -> tuple[Result[TaskCancelled, TaskActionError], AsyncMock, AsyncMock]:
    broker, session, connection = _broker_with_rows(rows)
    result = await cancel_task(broker, TASK_ID, include_running=include_running)
    return result, session, connection


class TestCancelOutcome:
    """One ``horsies_cancel_task`` row decides the answer."""

    @pytest.mark.asyncio
    async def test_an_applied_cancel_reports_the_observed_status(self) -> None:
        result, session, connection = await _cancel(
            [_row('APPLIED', status='RUNNING', kind='CANCEL_ADMIN')],
        )

        assert result == Ok(TaskCancelled(task_id=TASK_ID, was_status=TaskStatus.RUNNING))
        assert connection.execute.await_args is not None
        assert connection.execute.await_args.args[1] == {
            'task_id': str(TASK_ID),
            'include_running': True,
        }
        session.commit.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_the_running_choice_reaches_the_statement(self) -> None:
        _, _, connection = await _cancel(
            [_row('SOURCE_STATE_CONFLICT', status='RUNNING')],
            include_running=False,
        )

        assert connection.execute.await_args is not None
        assert connection.execute.await_args.args[1]['include_running'] is False

    @pytest.mark.asyncio
    async def test_a_live_status_outside_the_choice_is_not_cancellable(self) -> None:
        result, _, _ = await _cancel(
            [_row('SOURCE_STATE_CONFLICT', status='RUNNING')],
            include_running=False,
        )

        assert is_err(result)
        assert result.err_value == TaskActionNotCancellable(
            message=f'Task {TASK_ID} cannot be cancelled from status RUNNING.',
            task_id=TASK_ID,
            current_status=TaskStatus.RUNNING,
        )

    @pytest.mark.asyncio
    async def test_a_finished_plain_task_is_not_cancellable_with_its_status(self) -> None:
        result, _, _ = await _cancel(
            [
                _row(
                    'SOURCE_STATE_CONFLICT',
                    status='COMPLETED',
                    guard='FOREIGN_TERMINALIZATION',
                    kind='COMPLETE_FUSED',
                ),
            ],
        )

        assert is_err(result)
        error = result.err_value
        assert isinstance(error, TaskActionNotCancellable)
        assert error.current_status is TaskStatus.COMPLETED

    @pytest.mark.asyncio
    async def test_an_earlier_cancel_is_not_cancellable(self) -> None:
        result, _, _ = await _cancel(
            [_row('ALREADY_APPLIED', status='CANCELLED', kind='CANCEL_ADMIN')],
        )

        assert is_err(result)
        error = result.err_value
        assert isinstance(error, TaskActionNotCancellable)
        assert error.current_status is TaskStatus.CANCELLED

    @pytest.mark.parametrize(
        ('status', 'kind'),
        [('PENDING', None), ('FAILED', 'FAIL_RUNNING')],
        ids=['live', 'finished'],
    )
    @pytest.mark.asyncio
    async def test_a_workflow_task_carries_its_status(
        self, status: str, kind: str | None,
    ) -> None:
        result, _, _ = await _cancel(
            [_row('SOURCE_STATE_CONFLICT', status=status, guard='WORKFLOW_TASK', kind=kind)],
        )

        assert is_err(result)
        error = result.err_value
        assert isinstance(error, TaskActionWorkflowTask)
        assert error.current_status is TaskStatus(status)

    @pytest.mark.asyncio
    async def test_an_absent_task_is_not_found(self) -> None:
        result, _, _ = await _cancel([_row('TASK_ABSENT')])

        assert is_err(result)
        assert result.err_value == TaskActionNotFound(
            message=f'Task {TASK_ID} does not exist.', task_id=TASK_ID,
        )

    @pytest.mark.asyncio
    async def test_a_row_that_does_not_decode_is_stored_data_invalid(self) -> None:
        result, _, _ = await _cancel(
            [_row('SOURCE_STATE_CONFLICT', status=CAUSE_MARKER, guard='FOREIGN_TERMINALIZATION')],
        )

        assert is_err(result)
        error = result.err_value
        assert isinstance(error, TaskActionStoredDataInvalid)
        assert isinstance(error.exception, OutcomeDecodeError)
        assert error.task_id == TASK_ID
        assert CAUSE_MARKER in error.message

    @pytest.mark.asyncio
    async def test_an_outcome_without_an_observed_status_is_a_defect(self) -> None:
        with pytest.raises(RuntimeError, match='no observed status'):
            await _cancel([_row('APPLIED', kind='CANCEL_ADMIN')])

    @pytest.mark.asyncio
    async def test_a_lost_claim_is_a_defect(self) -> None:
        with pytest.raises(RuntimeError, match='no claim fence'):
            await _cancel([_row('LOST_CLAIM', status='CLAIMED')])

    @pytest.mark.asyncio
    async def test_a_row_count_other_than_one_is_a_defect(self) -> None:
        with pytest.raises(AssertionError, match='returned 0 rows'):
            await _cancel([])


class TestCancelBoundaries:
    @pytest.mark.asyncio
    async def test_the_cancel_statement_gets_the_canonical_text_of_the_id(self) -> None:
        broker, _, connection = _broker_with_rows([_row('TASK_ABSENT')])

        result = await cancel_task(broker, TASK_ID)

        assert is_err(result)
        assert result.err_value.task_id == TASK_ID
        assert connection.execute.await_args is not None
        assert connection.execute.await_args.args[1]['task_id'] == str(TASK_ID)

    @pytest.mark.asyncio
    async def test_a_database_failure_keeps_the_cause(self) -> None:
        cause = OperationalError('SELECT 1', {}, Exception('connection lost'))
        broker, _, _ = _broker_with_rows(cause)
        task_id = uuid.uuid4()

        result = await cancel_task(broker, task_id)

        assert is_err(result)
        error = result.err_value
        assert isinstance(error, TaskActionDbOperationFailed)
        assert error.exception is cause
        assert error.retryable is True
        assert error.task_id == task_id


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
