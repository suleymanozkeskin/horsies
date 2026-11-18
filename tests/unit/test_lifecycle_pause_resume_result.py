"""Unit tests for lifecycle pause/resume Result typing.

pause_workflow / resume_workflow return ``HandleResult[bool]``. A database
error is a ``HandleOperationError`` variant; any other exception propagates.
The pause is one ``horsies_workflow_pause`` statement that the core runs;
its record decides the result.
The resume runs in the core too (``CoreHandle.resume_workflow_async``): its
record decides the result, and a recovery that failed after the commit is
``HandleResumeRecoveryFailed``. The sync wrappers return a
core boundary error of the blocking call as ``HandleDbOperationFailed`` and
let other exceptions propagate.
"""

from __future__ import annotations

import uuid
from unittest.mock import AsyncMock, MagicMock

import pytest

import horsies._core as core

from horsies.core.types.result import is_ok, is_err
from horsies.core.models.workflow.handle_types import (
    HandleDbOperationFailed,
    HandleResumeRecoveryFailed,
    HandleStoredDataInvalid,
    HandleWorkflowNotFound,
)

WORKFLOW_ID = 'wf-1'


# ===========================================================================
# pause_workflow async
# ===========================================================================


PAUSE_WORKFLOW_ID = '0b4d8a52-7d0e-4a0e-9d11-6b2f1f7c9e03'


def _record(record_class: type, **fields: object) -> MagicMock:
    """A stand-in for a record of ``horsies._core``: a class pattern matches it."""
    record = MagicMock(spec=record_class)
    record.workflow_id = uuid.UUID(PAUSE_WORKFLOW_ID)
    for name, value in fields.items():
        setattr(record, name, value)
    return record


def _pause_broker(answer: object = None, *, raises: BaseException | None = None) -> MagicMock:
    """A broker whose core handle answers the pause with ``answer``, or raises."""
    broker = MagicMock()
    match raises:
        case None:
            broker._core_handle.pause_workflow_async = AsyncMock(return_value=answer)
        case _:
            broker._core_handle.pause_workflow_async = AsyncMock(side_effect=raises)
    return broker


@pytest.mark.unit
class TestPauseWorkflowResult:
    """pause_workflow returns HandleResult[bool] from the core's record."""

    @pytest.mark.asyncio(loop_scope='function')
    async def test_pause_running_returns_ok_true(self) -> None:
        """WorkflowPaused -> Ok(True); one call with the id as a UUID."""
        from horsies.core.workflows.lifecycle import pause_workflow

        broker = _pause_broker(_record(core.WorkflowPaused))
        result = await pause_workflow(broker, PAUSE_WORKFLOW_ID)

        assert is_ok(result)
        assert result.ok_value is True
        broker._core_handle.pause_workflow_async.assert_awaited_once_with(
            uuid.UUID(PAUSE_WORKFLOW_ID),
        )

    @pytest.mark.asyncio(loop_scope='function')
    async def test_pause_wrong_state_returns_ok_false(self) -> None:
        """WorkflowNotRunning -> Ok(False)."""
        from horsies.core.workflows.lifecycle import pause_workflow

        broker = _pause_broker(_record(core.WorkflowNotRunning))
        result = await pause_workflow(broker, PAUSE_WORKFLOW_ID)

        assert is_ok(result)
        assert result.ok_value is False

    @pytest.mark.asyncio(loop_scope='function')
    async def test_pause_not_found_returns_err(self) -> None:
        """WorkflowAbsent -> Err(WORKFLOW_NOT_FOUND)."""
        from horsies.core.workflows.lifecycle import pause_workflow

        broker = _pause_broker(_record(core.WorkflowAbsent))
        result = await pause_workflow(broker, PAUSE_WORKFLOW_ID)

        assert is_err(result)
        err = result.err_value
        assert isinstance(err, HandleWorkflowNotFound)
        assert err.workflow_id == PAUSE_WORKFLOW_ID
        assert err.retryable is False

    @pytest.mark.asyncio(loop_scope='function')
    async def test_pause_db_failure_returns_db_operation_failed(self) -> None:
        """HandleFailed with DB_OPERATION_FAILED -> Err(HandleDbOperationFailed)
        with the record's cause and retry rule."""
        from horsies.core.workflows.lifecycle import pause_workflow

        cause = core.CoreConnectionError('pause_workflow', 'connection refused', True)
        broker = _pause_broker(_record(
            core.HandleFailed,
            code='DB_OPERATION_FAILED',
            message='connection refused',
            retryable=True,
            exception=cause,
        ))
        result = await pause_workflow(broker, PAUSE_WORKFLOW_ID)

        assert is_err(result)
        err = result.err_value
        assert isinstance(err, HandleDbOperationFailed)
        assert err.workflow_id == PAUSE_WORKFLOW_ID
        assert err.retryable is True
        assert 'connection refused' in err.message
        assert err.exception is cause

    @pytest.mark.asyncio(loop_scope='function')
    async def test_pause_stored_data_invalid_returns_its_error(self) -> None:
        """HandleFailed with STORED_DATA_INVALID -> Err(HandleStoredDataInvalid)."""
        from horsies.core.workflows.lifecycle import pause_workflow

        broker = _pause_broker(_record(
            core.HandleFailed,
            code='STORED_DATA_INVALID',
            message='status is not a workflow status',
            retryable=False,
        ))
        result = await pause_workflow(broker, PAUSE_WORKFLOW_ID)

        assert is_err(result)
        assert isinstance(result.err_value, HandleStoredDataInvalid)

    @pytest.mark.asyncio(loop_scope='function')
    async def test_pause_core_boundary_error_returns_db_operation_failed(self) -> None:
        """A core boundary error raised by the call -> Err(HandleDbOperationFailed),
        retryable by the core's rule."""
        from horsies.core.workflows.lifecycle import pause_workflow

        refused = core.TooManyPendingCalls('CoreHandle.pause_workflow_async', 64)
        broker = _pause_broker(raises=refused)
        result = await pause_workflow(broker, PAUSE_WORKFLOW_ID)

        assert is_err(result)
        err = result.err_value
        assert isinstance(err, HandleDbOperationFailed)
        assert err.exception is refused
        assert err.retryable is True

    @pytest.mark.asyncio(loop_scope='function')
    async def test_pause_id_that_is_not_a_uuid_calls_nothing(self) -> None:
        """An id text that is not a UUID -> Err(HandleDbOperationFailed), not
        retryable, and no core call."""
        from horsies.core.workflows.lifecycle import pause_workflow

        broker = _pause_broker(_record(core.WorkflowPaused))
        result = await pause_workflow(broker, WORKFLOW_ID)

        assert is_err(result)
        err = result.err_value
        assert isinstance(err, HandleDbOperationFailed)
        assert err.retryable is False
        broker._core_handle.pause_workflow_async.assert_not_awaited()


# ===========================================================================
# resume_workflow through the core
# ===========================================================================


def _resume_broker(answer: object = None, *, raises: BaseException | None = None) -> MagicMock:
    """A broker whose core handle answers the resume with ``answer``, or raises."""
    broker = MagicMock()
    match raises:
        case None:
            broker._core_handle.resume_workflow_async = AsyncMock(return_value=answer)
        case _:
            broker._core_handle.resume_workflow_async = AsyncMock(side_effect=raises)
    return broker


@pytest.mark.unit
class TestResumeThroughCore:
    """resume_workflow returns HandleResult[bool] from the core's record."""

    @pytest.mark.asyncio(loop_scope='function')
    async def test_resumed_returns_ok_true_with_the_id_as_a_uuid(self) -> None:
        from horsies.core.workflows.lifecycle import resume_workflow

        broker = _resume_broker(_record(core.WorkflowResumed))
        result = await resume_workflow(broker, PAUSE_WORKFLOW_ID)

        assert is_ok(result)
        assert result.ok_value is True
        broker._core_handle.resume_workflow_async.assert_awaited_once_with(
            uuid.UUID(PAUSE_WORKFLOW_ID),
        )

    @pytest.mark.asyncio(loop_scope='function')
    async def test_not_paused_returns_ok_false(self) -> None:
        from horsies.core.workflows.lifecycle import resume_workflow

        broker = _resume_broker(_record(core.WorkflowNotPaused))
        result = await resume_workflow(broker, PAUSE_WORKFLOW_ID)

        assert is_ok(result)
        assert result.ok_value is False

    @pytest.mark.asyncio(loop_scope='function')
    async def test_absent_returns_workflow_not_found(self) -> None:
        from horsies.core.workflows.lifecycle import resume_workflow

        broker = _resume_broker(_record(core.WorkflowAbsent))
        result = await resume_workflow(broker, PAUSE_WORKFLOW_ID)

        assert is_err(result)
        assert isinstance(result.err_value, HandleWorkflowNotFound)

    @pytest.mark.asyncio(loop_scope='function')
    async def test_recovery_failed_after_the_commit_returns_its_error(self) -> None:
        from horsies.core.workflows.lifecycle import resume_workflow

        cause = core.CoreConnectionError(
            'resume_workflow', 'the recovery pass for its tree failed', False,
        )
        broker = _resume_broker(_record(
            core.WorkflowResumeRecoveryFailed,
            message='the recovery pass for its tree failed',
            exception=cause,
        ))
        result = await resume_workflow(broker, PAUSE_WORKFLOW_ID)

        assert is_err(result)
        err = result.err_value
        assert isinstance(err, HandleResumeRecoveryFailed)
        assert err.retryable is False
        assert err.exception is cause

    @pytest.mark.asyncio(loop_scope='function')
    async def test_a_failure_record_returns_db_operation_failed(self) -> None:
        from horsies.core.workflows.lifecycle import resume_workflow

        broker = _resume_broker(_record(
            core.HandleFailed,
            code='DB_OPERATION_FAILED',
            message='child build did not run',
            retryable=True,
            exception=core.CorePreconditionFailed(
                'resume_workflow', 'child build did not run', True,
            ),
        ))
        result = await resume_workflow(broker, PAUSE_WORKFLOW_ID)

        assert is_err(result)
        err = result.err_value
        assert isinstance(err, HandleDbOperationFailed)
        assert err.retryable is True
        assert err.message.startswith('Resume failed for workflow')

    @pytest.mark.asyncio(loop_scope='function')
    async def test_an_id_that_is_not_a_uuid_makes_no_core_call(self) -> None:
        from horsies.core.workflows.lifecycle import resume_workflow

        broker = _resume_broker(_record(core.WorkflowResumed))
        result = await resume_workflow(broker, 'wf-1')

        assert is_err(result)
        assert isinstance(result.err_value, HandleDbOperationFailed)
        assert result.err_value.retryable is False
        broker._core_handle.resume_workflow_async.assert_not_awaited()

    @pytest.mark.asyncio(loop_scope='function')
    async def test_a_core_boundary_error_returns_db_operation_failed(self) -> None:
        from horsies.core.workflows.lifecycle import resume_workflow

        broker = _resume_broker(raises=core.TooManyPendingCalls('too many calls'))
        result = await resume_workflow(broker, PAUSE_WORKFLOW_ID)

        assert is_err(result)
        assert isinstance(result.err_value, HandleDbOperationFailed)
        assert result.err_value.retryable is True


# ===========================================================================
# resume_workflow async
# ===========================================================================


# ===========================================================================
# resume_workflow: the commit splits the failure states
# ===========================================================================


# ===========================================================================
# Sync wrappers: the blocking form, a core boundary error, other exceptions
# ===========================================================================


def _blocking_broker(method: str, answer: object) -> MagicMock:
    """A broker whose core handle answers the blocking ``method`` with
    ``answer``: a record, or an exception that the call raises."""
    broker = MagicMock()
    match answer:
        case BaseException():
            setattr(broker._core_handle, method, MagicMock(side_effect=answer))
        case _:
            setattr(broker._core_handle, method, MagicMock(return_value=answer))
    return broker


@pytest.mark.unit
class TestPauseSyncWrapper:
    """pause_workflow_sync calls CoreHandle.pause_workflow on the calling
    thread: a core boundary error is HandleDbOperationFailed, and other
    exceptions propagate."""

    def test_the_record_decides_the_result(self) -> None:
        from horsies.core.workflows.lifecycle import pause_workflow_sync

        broker = _blocking_broker('pause_workflow', _record(core.WorkflowPaused))
        result = pause_workflow_sync(broker, PAUSE_WORKFLOW_ID)

        assert is_ok(result)
        assert result.ok_value is True
        broker._core_handle.pause_workflow.assert_called_once_with(
            uuid.UUID(PAUSE_WORKFLOW_ID),
        )
        broker.run_sync.assert_not_called()

    def test_core_boundary_error_returns_err(self) -> None:
        """A CoreError of the blocking call -> Err(HandleDbOperationFailed)."""
        from horsies.core.workflows.lifecycle import pause_workflow_sync

        refused = core.TooManyPendingCalls('CoreHandle.pause_workflow', 64)
        broker = _blocking_broker('pause_workflow', refused)
        result = pause_workflow_sync(broker, PAUSE_WORKFLOW_ID)

        assert is_err(result)
        err = result.err_value
        assert isinstance(err, HandleDbOperationFailed)
        assert err.workflow_id == PAUSE_WORKFLOW_ID
        assert err.retryable is True
        assert err.exception is refused

    def test_unexpected_exception_propagates(self) -> None:
        """An exception that is not a CoreError propagates unchanged."""
        from horsies.core.workflows.lifecycle import pause_workflow_sync

        defect = RuntimeError('boom')
        broker = _blocking_broker('pause_workflow', defect)
        with pytest.raises(RuntimeError) as exc_info:
            pause_workflow_sync(broker, PAUSE_WORKFLOW_ID)

        assert exc_info.value is defect


@pytest.mark.unit
class TestResumeSyncWrapper:
    """resume_workflow_sync calls CoreHandle.resume_workflow on the calling
    thread: a core boundary error is HandleDbOperationFailed, and other
    exceptions propagate."""

    def test_the_record_decides_the_result(self) -> None:
        from horsies.core.workflows.lifecycle import resume_workflow_sync

        broker = _blocking_broker('resume_workflow', _record(core.WorkflowNotPaused))
        result = resume_workflow_sync(broker, PAUSE_WORKFLOW_ID)

        assert is_ok(result)
        assert result.ok_value is False
        broker._core_handle.resume_workflow.assert_called_once_with(
            uuid.UUID(PAUSE_WORKFLOW_ID),
        )
        broker.run_sync.assert_not_called()

    def test_core_boundary_error_returns_err(self) -> None:
        """A CoreError of the blocking call -> Err(HandleDbOperationFailed)."""
        from horsies.core.workflows.lifecycle import resume_workflow_sync

        refused = core.TooManyPendingCalls('CoreHandle.resume_workflow', 64)
        broker = _blocking_broker('resume_workflow', refused)
        result = resume_workflow_sync(broker, PAUSE_WORKFLOW_ID)

        assert is_err(result)
        err = result.err_value
        assert isinstance(err, HandleDbOperationFailed)
        assert err.workflow_id == PAUSE_WORKFLOW_ID
        assert err.retryable is True
        assert err.exception is refused

    def test_unexpected_exception_propagates(self) -> None:
        """An exception that is not a CoreError propagates unchanged."""
        from horsies.core.workflows.lifecycle import resume_workflow_sync

        defect = RuntimeError('boom')
        broker = _blocking_broker('resume_workflow', defect)
        with pytest.raises(RuntimeError) as exc_info:
            resume_workflow_sync(broker, PAUSE_WORKFLOW_ID)

        assert exc_info.value is defect
