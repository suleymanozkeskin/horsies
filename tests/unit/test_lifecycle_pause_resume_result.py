"""Unit tests for lifecycle pause/resume Result typing.

pause_workflow / resume_workflow return ``HandleResult[bool]``. A database
error is a ``HandleOperationError`` variant; any other exception propagates.
The pause is one ``horsies_workflow_pause`` statement that the core runs;
its record decides the result.
Resume runs its recovery pass in a second transaction after the commit, so a
database error there is ``HandleResumeRecoveryFailed``. The sync wrappers
return a ``LoopRunnerError`` as ``HandleLoopRunnerFailed`` and let other
exceptions propagate.
"""

from __future__ import annotations

import uuid
from collections.abc import Generator
from contextlib import contextmanager
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from sqlalchemy.exc import OperationalError

import horsies._core as core
from tests.broker_resources_support import attach_resources, fake_resources
from tests.workflow_start_support import outcome_row

from horsies.core.types.result import is_ok, is_err
from horsies.core.models.workflow.handle_types import (
    HandleDbOperationFailed,
    HandleLoopRunnerFailed,
    HandleResumeRecoveryFailed,
    HandleStoredDataInvalid,
    HandleWorkflowNotFound,
)
from horsies.core.types.core_failure import CoreOperationFailed

WORKFLOW_ID = 'wf-1'


def _db_error() -> OperationalError:
    return OperationalError('SELECT 1', {}, Exception('connection refused'))


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _empty_result() -> MagicMock:
    """A mock DB result that returns no rows."""
    r = MagicMock()
    r.fetchone.return_value = None
    r.fetchall.return_value = []
    return r


def _mock_broker(
    *,
    transition_row: tuple[Any, ...] | None = None,
    exists_row: tuple[Any, ...] | None = None,
    side_effect: Exception | None = None,
) -> MagicMock:
    """Build a mock broker whose session executes return pre-canned rows.

    Args:
        transition_row: row returned by the UPDATE (PAUSE/RESUME) SQL.
            None means the UPDATE matched nothing.
        exists_row: row returned by the existence-check SELECT.
            None means the workflow does not exist.
        side_effect: if set, the session context manager raises this
            instead of executing normally.
    """
    session = AsyncMock()

    if side_effect is not None:
        # Make the session_factory context manager raise immediately
        cm = AsyncMock()
        cm.__aenter__ = AsyncMock(side_effect=side_effect)
        cm.__aexit__ = AsyncMock(return_value=False)
        broker = MagicMock()
        attach_resources(broker, fake_resources(session_factory=MagicMock(return_value=cm)))
        return broker

    # Build a call counter so we can return different results for
    # sequential execute() calls.
    call_results: list[Any] = []

    # First call: the UPDATE SQL
    transition_result = MagicMock()
    transition_result.fetchone.return_value = transition_row
    call_results.append(transition_result)

    if transition_row is None:
        # Second call: the existence check
        exists_result = MagicMock()
        exists_result.fetchone.return_value = exists_row
        call_results.append(exists_result)

    # After the core calls, cascade/notify/etc. may fire more executes.
    # Return empty results for any subsequent calls.
    def _execute_side_effect(*args: Any, **kwargs: Any) -> MagicMock:
        if call_results:
            return call_results.pop(0)
        return _empty_result()

    session.execute = AsyncMock(side_effect=_execute_side_effect)
    session.commit = AsyncMock()

    cm = AsyncMock()
    cm.__aenter__ = AsyncMock(return_value=session)
    cm.__aexit__ = AsyncMock(return_value=False)

    broker = MagicMock()
    attach_resources(broker, fake_resources(session_factory=MagicMock(return_value=cm)))
    return broker


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

        broker = _pause_broker(_record(
            core.HandleFailed,
            code='DB_OPERATION_FAILED',
            message='connection refused',
            retryable=True,
        ))
        result = await pause_workflow(broker, PAUSE_WORKFLOW_ID)

        assert is_err(result)
        err = result.err_value
        assert isinstance(err, HandleDbOperationFailed)
        assert err.workflow_id == PAUSE_WORKFLOW_ID
        assert err.retryable is True
        assert 'connection refused' in err.message
        assert isinstance(err.exception, CoreOperationFailed)
        assert err.exception.subject == 'pause_workflow'

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
# resume_workflow async
# ===========================================================================


def _resume_sessions(
    disposition: str = 'RESUMED',
) -> tuple[MagicMock, AsyncMock, MagicMock]:
    """A broker whose resume transaction answers ``horsies_workflow_resume``
    with one OUTCOME row of ``disposition``, its session, and its session
    factory, which the recovery pass of the tree receives.
    """
    resume_result = MagicMock()
    resume_result.fetchall.return_value = [outcome_row(disposition)]
    resume_session = AsyncMock()
    resume_session.info = {}
    resume_session.execute = AsyncMock(return_value=resume_result)
    resume_session.commit = AsyncMock()

    context = AsyncMock()
    context.__aenter__ = AsyncMock(return_value=resume_session)
    context.__aexit__ = AsyncMock(return_value=False)
    session_factory = MagicMock(return_value=context)
    broker = MagicMock()
    attach_resources(broker, fake_resources(session_factory=session_factory))
    return broker, resume_session, session_factory


@contextmanager
def _recovery_stubbed(recover: AsyncMock) -> Generator[None, None, None]:
    """Stub the recovery pass of the tree after the resume commit."""
    with patch('horsies.core.workflows.recovery.recover_stuck_workflow_tree', new=recover):
        yield


@pytest.mark.unit
class TestResumeWorkflowResult:
    """resume_workflow returns HandleResult[bool], from the function's outcome."""

    @pytest.mark.asyncio(loop_scope='function')
    async def test_resume_wrong_state_returns_ok_false(self) -> None:
        """NOT_PAUSED -> Ok(False); no recovery pass."""
        from horsies.core.workflows.lifecycle import resume_workflow

        broker, _, _ = _resume_sessions('NOT_PAUSED')
        recover = AsyncMock(return_value=None)
        with _recovery_stubbed(recover):
            result = await resume_workflow(broker, WORKFLOW_ID)

        assert is_ok(result)
        assert result.ok_value is False
        recover.assert_not_awaited()

    @pytest.mark.asyncio(loop_scope='function')
    async def test_resume_not_found_returns_err(self) -> None:
        """NOT_FOUND -> Err(WORKFLOW_NOT_FOUND)."""
        from horsies.core.workflows.lifecycle import resume_workflow

        broker, _, _ = _resume_sessions('NOT_FOUND')
        result = await resume_workflow(broker, WORKFLOW_ID)

        assert is_err(result)
        err = result.err_value
        assert isinstance(err, HandleWorkflowNotFound)
        assert err.workflow_id == WORKFLOW_ID
        assert err.retryable is False

    @pytest.mark.asyncio(loop_scope='function')
    async def test_resume_db_failure_returns_db_operation_failed(self) -> None:
        """SQLAlchemy error -> Err(HandleDbOperationFailed) with the cause."""
        from horsies.core.workflows.lifecycle import resume_workflow

        db_exc = _db_error()
        broker = _mock_broker(side_effect=db_exc)
        result = await resume_workflow(broker, 'wf-1')

        assert is_err(result)
        err = result.err_value
        assert isinstance(err, HandleDbOperationFailed)
        assert err.workflow_id == 'wf-1'
        assert err.exception is db_exc
        assert err.retryable is True

    @pytest.mark.asyncio(loop_scope='function')
    async def test_an_outcome_outside_the_contract_raises(self) -> None:
        from horsies.core.lifecycle.outcomes import OutcomeDecodeError
        from horsies.core.workflows.lifecycle import resume_workflow

        broker, resume_session, _ = _resume_sessions('RESUMED_TWICE')
        with pytest.raises(OutcomeDecodeError, match='RESUMED_TWICE'):
            await resume_workflow(broker, WORKFLOW_ID)

        resume_session.commit.assert_not_awaited()


# ===========================================================================
# resume_workflow: the commit splits the failure states
# ===========================================================================


@pytest.mark.unit
class TestResumeCommitSplit:
    """A database error before the resume commit and one after it are two states."""

    @pytest.mark.asyncio(loop_scope='function')
    async def test_resume_runs_the_recovery_pass_after_the_commit(self) -> None:
        from horsies.core.workflows.lifecycle import resume_workflow

        broker, resume_session, session_factory = _resume_sessions()
        recover = AsyncMock(return_value=None)
        with _recovery_stubbed(recover):
            result = await resume_workflow(broker, WORKFLOW_ID)

        assert is_ok(result)
        assert result.ok_value is True
        resume_session.commit.assert_awaited_once()
        recover.assert_awaited_once_with(session_factory, broker, WORKFLOW_ID)

    @pytest.mark.asyncio(loop_scope='function')
    async def test_recovery_db_error_returns_resume_recovery_failed(self) -> None:
        """The resume committed; only the second transaction failed."""
        from horsies.core.workflows.lifecycle import resume_workflow

        db_exc = _db_error()
        broker, resume_session, _ = _resume_sessions()
        with _recovery_stubbed(AsyncMock(side_effect=db_exc)):
            result = await resume_workflow(broker, WORKFLOW_ID)

        assert is_err(result)
        err = result.err_value
        assert isinstance(err, HandleResumeRecoveryFailed)
        assert err.workflow_id == WORKFLOW_ID
        assert err.exception is db_exc
        assert err.retryable is False
        resume_session.commit.assert_awaited_once()

    @pytest.mark.asyncio(loop_scope='function')
    async def test_a_failed_parent_advance_returns_resume_recovery_failed(self) -> None:
        """The resume committed and ended a child; the advance of its parent failed."""
        from horsies.core.workflows.lifecycle import resume_workflow
        from horsies.core.workflows.steps import ParentAdvanceFailure

        db_exc = _db_error()
        broker, resume_session, _ = _resume_sessions()
        failure = ParentAdvanceFailure(
            failed_child_workflow_id='child-wf', still_owed=('child-wf',), cause=db_exc,
        )
        recover = AsyncMock(return_value=None)
        with (
            _recovery_stubbed(recover),
            patch(
                'horsies.core.workflows.lifecycle.advance_parents',
                new=AsyncMock(return_value=failure),
            ),
        ):
            result = await resume_workflow(broker, WORKFLOW_ID)

        assert is_err(result)
        err = result.err_value
        assert isinstance(err, HandleResumeRecoveryFailed)
        assert err.exception is db_exc
        resume_session.commit.assert_awaited_once()
        recover.assert_not_awaited()

    @pytest.mark.asyncio(loop_scope='function')
    async def test_commit_db_error_returns_db_operation_failed(self) -> None:
        """A failed resume commit is not a recovery failure; no recovery pass runs."""
        from horsies.core.workflows.lifecycle import resume_workflow

        db_exc = _db_error()
        broker, resume_session, _ = _resume_sessions()
        resume_session.commit = AsyncMock(side_effect=db_exc)
        recover = AsyncMock(return_value=0)
        with _recovery_stubbed(recover):
            result = await resume_workflow(broker, WORKFLOW_ID)

        assert is_err(result)
        err = result.err_value
        assert isinstance(err, HandleDbOperationFailed)
        assert err.exception is db_exc
        assert err.retryable is True
        recover.assert_not_awaited()

    @pytest.mark.asyncio(loop_scope='function')
    @pytest.mark.parametrize(
        'defect', [RuntimeError('logic bug'), AssertionError('invariant')],
        ids=type,
    )
    async def test_defect_in_the_resume_body_propagates(
        self, defect: Exception,
    ) -> None:
        from horsies.core.workflows.lifecycle import resume_workflow

        broker, resume_session, _ = _resume_sessions()
        recover = AsyncMock(return_value=None)
        with (
            _recovery_stubbed(recover),
            patch(
                'horsies.core.workflows.steps.decode_step_rows',
                side_effect=defect,
            ),
            pytest.raises(type(defect)) as exc_info,
        ):
            await resume_workflow(broker, WORKFLOW_ID)

        assert exc_info.value is defect
        resume_session.commit.assert_not_awaited()
        recover.assert_not_awaited()

    @pytest.mark.asyncio(loop_scope='function')
    async def test_defect_in_the_recovery_pass_propagates(self) -> None:
        from horsies.core.workflows.lifecycle import resume_workflow

        defect = RuntimeError('logic bug')
        broker, resume_session, _ = _resume_sessions()
        with (
            _recovery_stubbed(AsyncMock(side_effect=defect)),
            pytest.raises(RuntimeError) as exc_info,
        ):
            await resume_workflow(broker, WORKFLOW_ID)

        assert exc_info.value is defect
        resume_session.commit.assert_awaited_once()


# ===========================================================================
# Sync wrappers: LoopRunnerError and other exceptions
# ===========================================================================


@pytest.mark.unit
class TestPauseSyncWrapper:
    """pause_workflow_sync returns LoopRunnerError as an Err and lets other exceptions propagate."""

    def test_loop_runner_error_returns_err(self) -> None:
        """LoopRunnerError -> Err(HandleLoopRunnerFailed)."""
        from horsies.core.workflows.lifecycle import pause_workflow_sync
        from horsies.core.utils.loop_runner import LoopRunnerError

        broker = MagicMock()
        with patch.object(
            broker, 'run_sync',
        ) as mock_call:
            runner_error = LoopRunnerError('dead')
            mock_call.side_effect = runner_error
            result = pause_workflow_sync(broker, 'wf-1')

        assert is_err(result)
        err = result.err_value
        assert isinstance(err, HandleLoopRunnerFailed)
        assert err.workflow_id == 'wf-1'
        assert err.retryable is False
        assert err.exception is runner_error

    def test_unexpected_exception_propagates(self) -> None:
        """An exception that is not a LoopRunnerError propagates unchanged."""
        from horsies.core.workflows.lifecycle import pause_workflow_sync

        broker = MagicMock()
        defect = RuntimeError('boom')
        with (
            patch.object(broker, 'run_sync', side_effect=defect),
            pytest.raises(RuntimeError) as exc_info,
        ):
            pause_workflow_sync(broker, 'wf-1')

        assert exc_info.value is defect


@pytest.mark.unit
class TestResumeSyncWrapper:
    """resume_workflow_sync returns LoopRunnerError as an Err and lets other exceptions propagate."""

    def test_loop_runner_error_returns_err(self) -> None:
        """LoopRunnerError -> Err(HandleLoopRunnerFailed)."""
        from horsies.core.workflows.lifecycle import resume_workflow_sync
        from horsies.core.utils.loop_runner import LoopRunnerError

        broker = MagicMock()
        with patch.object(
            broker, 'run_sync',
        ) as mock_call:
            runner_error = LoopRunnerError('dead')
            mock_call.side_effect = runner_error
            result = resume_workflow_sync(broker, 'wf-1')

        assert is_err(result)
        err = result.err_value
        assert isinstance(err, HandleLoopRunnerFailed)
        assert err.workflow_id == 'wf-1'
        assert err.retryable is False
        assert err.exception is runner_error

    def test_unexpected_exception_propagates(self) -> None:
        """An exception that is not a LoopRunnerError propagates unchanged."""
        from horsies.core.workflows.lifecycle import resume_workflow_sync

        broker = MagicMock()
        defect = RuntimeError('boom')
        with (
            patch.object(broker, 'run_sync', side_effect=defect),
            pytest.raises(RuntimeError) as exc_info,
        ):
            resume_workflow_sync(broker, 'wf-1')

        assert exc_info.value is defect
