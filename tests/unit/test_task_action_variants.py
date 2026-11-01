"""The task action error variants, the cancel outcomes that build them, and the web mapping.

``TaskActionError`` is a union with one frozen variant per
``TaskActionErrorCode`` member. A variant's repr hides the message and the
cause. Every variant carries ``task_id``. The cancel tests run on a mocked
session: the lock read, the history read and the cancel outcome are the
seams.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any, get_args
from unittest.mock import AsyncMock, MagicMock

import pytest
from sqlalchemy.exc import OperationalError

from horsies.core.brokers.resources import EventLoopLimit
from horsies.core.history.errors import HistoryContractError
from horsies.core.history.reads.detail import (
    HistoryTaskState,
    LiveTaskLocation,
    TaskStateAbsent,
    TaskStateResult,
)
from horsies.core.lifecycle.operations import TerminalizationKind
from horsies.core.lifecycle.outcomes import (
    AlreadyApplied,
    Applied,
    LostClaim,
    ObservedClaim,
    ObservedTaskState,
    OutcomeDecodeError,
    SourceStateConflict,
    TerminalizationOutcome,
)
from horsies.core.types.result import Ok, Result, is_err
from horsies.core.types.status import TaskStatus
from horsies.monitoring import task_actions as task_actions_module
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
TASK_ID = '3f9c1a2b-4d5e-4f60-8a71-b2c3d4e5f607'
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
        assert TASK_ID in text

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


def _history_state(*, status: TaskStatus, is_workflow_task: bool) -> HistoryTaskState:
    return HistoryTaskState(
        task_id=TASK_ID, status=status, is_workflow_task=is_workflow_task,
    )


def _observed(status: TaskStatus | None) -> ObservedTaskState:
    return ObservedTaskState(status=status, worker_id=None, claimed_at=None)


def _locked_row(*, is_workflow_task: bool) -> MagicMock:
    result = MagicMock()
    result.first.return_value = (is_workflow_task,)
    return result


def _missed_row() -> MagicMock:
    result = MagicMock()
    result.first.return_value = None
    return result


def _broker_with_session(execute: AsyncMock) -> tuple[MagicMock, AsyncMock]:
    session = AsyncMock()
    session.execute = execute
    session.commit = AsyncMock()
    session.connection = AsyncMock(return_value=MagicMock())
    session_ctx = AsyncMock()
    session_ctx.__aenter__ = AsyncMock(return_value=session)
    session_ctx.__aexit__ = AsyncMock(return_value=False)
    broker = MagicMock()
    attach_resources(
        broker, fake_resources(session_factory=MagicMock(return_value=session_ctx)),
    )
    return broker, session


def _patch_history(
    monkeypatch: pytest.MonkeyPatch,
    *,
    published: bool,
    read: TaskStateResult | BaseException,
) -> None:
    monkeypatch.setattr(
        task_actions_module,
        'staged_detail_published',
        AsyncMock(return_value=published),
    )
    match read:
        case BaseException():
            reader = AsyncMock(side_effect=read)
        case _:
            reader = AsyncMock(return_value=read)
    monkeypatch.setattr(task_actions_module, 'read_task_state', reader)


def _patch_outcome(
    monkeypatch: pytest.MonkeyPatch,
    outcome: TerminalizationOutcome | BaseException,
) -> None:
    match outcome:
        case BaseException():
            apply = AsyncMock(side_effect=outcome)
        case _:
            apply = AsyncMock(return_value=outcome)
    monkeypatch.setattr(task_actions_module, 'apply_async', apply)


async def _cancel_missed(
    monkeypatch: pytest.MonkeyPatch,
    *,
    published: bool,
    read: TaskStateResult | BaseException,
) -> Result[TaskCancelled, TaskActionError]:
    broker, _ = _broker_with_session(AsyncMock(return_value=_missed_row()))
    _patch_history(monkeypatch, published=published, read=read)
    return await cancel_task(broker, TASK_ID)


class TestCancelLiveMiss:
    """The lock read finds no live row; the history read decides."""

    @pytest.mark.asyncio
    async def test_a_database_without_the_history_read_is_not_found(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        result = await _cancel_missed(
            monkeypatch, published=False, read=RuntimeError('not called'),
        )

        assert is_err(result)
        assert result.err_value == TaskActionNotFound(
            message=f'Task {TASK_ID} does not exist.', task_id=TASK_ID,
        )

    @pytest.mark.asyncio
    async def test_an_absent_task_is_not_found(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        result = await _cancel_missed(
            monkeypatch,
            published=True,
            read=TaskStateAbsent(task_id=TASK_ID),
        )

        assert is_err(result)
        assert isinstance(result.err_value, TaskActionNotFound)

    @pytest.mark.asyncio
    async def test_a_task_found_live_after_the_lock_read_is_not_found(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        result = await _cancel_missed(
            monkeypatch, published=True, read=LiveTaskLocation(task_id=TASK_ID),
        )

        assert is_err(result)
        assert isinstance(result.err_value, TaskActionNotFound)

    @pytest.mark.asyncio
    async def test_a_terminal_history_task_is_not_cancellable_with_its_status(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        result = await _cancel_missed(
            monkeypatch,
            published=True,
            read=_history_state(status=TaskStatus.COMPLETED, is_workflow_task=False),
        )

        assert is_err(result)
        assert result.err_value == TaskActionNotCancellable(
            message=f'Task {TASK_ID} cannot be cancelled from status COMPLETED.',
            task_id=TASK_ID,
            current_status=TaskStatus.COMPLETED,
        )

    @pytest.mark.asyncio
    async def test_a_workflow_history_task_carries_its_status(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        result = await _cancel_missed(
            monkeypatch,
            published=True,
            read=_history_state(status=TaskStatus.FAILED, is_workflow_task=True),
        )

        assert is_err(result)
        error = result.err_value
        assert isinstance(error, TaskActionWorkflowTask)
        assert error.current_status is TaskStatus.FAILED

    @pytest.mark.asyncio
    async def test_a_history_row_that_does_not_decode_is_stored_data_invalid(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        cause = HistoryContractError(f'history row {TASK_ID} has status {CAUSE_MARKER!r}')

        result = await _cancel_missed(monkeypatch, published=True, read=cause)

        assert is_err(result)
        error = result.err_value
        assert isinstance(error, TaskActionStoredDataInvalid)
        assert error.exception is cause
        assert error.task_id == TASK_ID
        assert CAUSE_MARKER in error.message


async def _cancel_locked(
    monkeypatch: pytest.MonkeyPatch,
    *,
    is_workflow_task: bool,
    outcome: TerminalizationOutcome | BaseException,
) -> tuple[Result[TaskCancelled, TaskActionError], AsyncMock]:
    broker, session = _broker_with_session(
        AsyncMock(return_value=_locked_row(is_workflow_task=is_workflow_task)),
    )
    _patch_outcome(monkeypatch, outcome)
    result = await cancel_task(broker, TASK_ID, include_running=True)
    return result, session


def _applied(status: TaskStatus | None) -> Applied:
    return Applied(
        task_id=TASK_ID,
        ordinality=None,
        terminal_at=NOW,
        kind=TerminalizationKind.CANCEL_ADMIN,
        observed=_observed(status),
    )


def _conflict(status: TaskStatus) -> SourceStateConflict:
    return SourceStateConflict(
        task_id=TASK_ID,
        ordinality=None,
        observed=_observed(status),
        evidence=ObservedClaim(worker_id=None, claimed_at=None),
    )


class TestCancelLockedRow:
    """The lock read finds the live row; the cancel outcome decides."""

    @pytest.mark.asyncio
    async def test_an_applied_cancel_commits_and_reports_the_observed_status(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        result, session = await _cancel_locked(
            monkeypatch, is_workflow_task=False, outcome=_applied(TaskStatus.RUNNING),
        )

        assert result == Ok(TaskCancelled(task_id=TASK_ID, was_status=TaskStatus.RUNNING))
        session.commit.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_a_conflict_is_not_cancellable_with_the_observed_status(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        result, session = await _cancel_locked(
            monkeypatch, is_workflow_task=False, outcome=_conflict(TaskStatus.RUNNING),
        )

        assert is_err(result)
        error = result.err_value
        assert isinstance(error, TaskActionNotCancellable)
        assert error.current_status is TaskStatus.RUNNING
        session.commit.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_an_already_applied_cancel_is_not_cancellable(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        outcome = AlreadyApplied(
            task_id=TASK_ID,
            ordinality=None,
            terminal_at=NOW,
            kind=TerminalizationKind.CANCEL_ADMIN,
            observed=_observed(TaskStatus.CANCELLED),
        )

        result, _ = await _cancel_locked(
            monkeypatch, is_workflow_task=False, outcome=outcome,
        )

        assert is_err(result)
        error = result.err_value
        assert isinstance(error, TaskActionNotCancellable)
        assert error.current_status is TaskStatus.CANCELLED

    @pytest.mark.asyncio
    async def test_a_live_workflow_task_carries_its_status(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        result, _ = await _cancel_locked(
            monkeypatch, is_workflow_task=True, outcome=_conflict(TaskStatus.PENDING),
        )

        assert is_err(result)
        error = result.err_value
        assert isinstance(error, TaskActionWorkflowTask)
        assert error.current_status is TaskStatus.PENDING

    @pytest.mark.asyncio
    async def test_an_outcome_row_that_does_not_decode_is_stored_data_invalid(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        cause = OutcomeDecodeError(f'observed_status is not a task status: {CAUSE_MARKER!r}')

        result, session = await _cancel_locked(
            monkeypatch, is_workflow_task=False, outcome=cause,
        )

        assert is_err(result)
        error = result.err_value
        assert isinstance(error, TaskActionStoredDataInvalid)
        assert error.exception is cause
        session.commit.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_an_outcome_without_an_observed_status_is_a_defect(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        with pytest.raises(RuntimeError, match='no observed status'):
            await _cancel_locked(
                monkeypatch, is_workflow_task=False, outcome=_applied(None),
            )

    @pytest.mark.asyncio
    async def test_a_lost_claim_under_the_row_lock_is_a_defect(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        outcome = LostClaim(
            task_id=TASK_ID, ordinality=None, observed=_observed(TaskStatus.CLAIMED),
        )

        with pytest.raises(RuntimeError, match='caller-held row lock'):
            await _cancel_locked(monkeypatch, is_workflow_task=False, outcome=outcome)


class TestCancelBoundaries:
    @pytest.mark.asyncio
    async def test_an_id_that_is_not_a_uuid_is_not_found(self) -> None:
        broker = MagicMock()

        result = await cancel_task(broker, 'not-a-uuid')

        assert is_err(result)
        assert isinstance(result.err_value, TaskActionNotFound)
        assert result.err_value.task_id == 'not-a-uuid'

    @pytest.mark.asyncio
    async def test_a_uuid_form_the_database_refuses_reaches_it_as_canonical_text(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        execute = AsyncMock(return_value=_missed_row())
        broker, _ = _broker_with_session(execute)
        _patch_history(monkeypatch, published=False, read=RuntimeError('not called'))

        result = await cancel_task(broker, f'urn:uuid:{TASK_ID.upper()}')

        assert is_err(result)
        assert isinstance(result.err_value, TaskActionNotFound)
        assert result.err_value.task_id == TASK_ID
        assert execute.await_args is not None
        assert execute.await_args.args[1] == {'id': TASK_ID}

    @pytest.mark.asyncio
    async def test_a_canonical_id_reaches_the_cancel_statement(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        broker, _ = _broker_with_session(
            AsyncMock(return_value=_locked_row(is_workflow_task=False)),
        )
        apply = AsyncMock(return_value=_applied(TaskStatus.PENDING))
        monkeypatch.setattr(task_actions_module, 'apply_async', apply)

        result = await cancel_task(broker, '{' + TASK_ID.replace('-', '') + '}')

        assert result == Ok(TaskCancelled(task_id=TASK_ID, was_status=TaskStatus.PENDING))
        assert apply.await_args is not None
        assert apply.await_args.args[1].task_id == TASK_ID

    @pytest.mark.asyncio
    async def test_a_database_failure_keeps_the_cause(self) -> None:
        cause = OperationalError('SELECT 1', {}, Exception('connection lost'))
        broker, _ = _broker_with_session(AsyncMock(side_effect=cause))
        task_id = str(uuid.uuid4())

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
