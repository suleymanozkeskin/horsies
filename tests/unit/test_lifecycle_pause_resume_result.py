"""Unit tests for lifecycle pause/resume Result typing.

Tests that pause_workflow / resume_workflow return typed
LifecycleResult[bool] instead of the previous bool | None sentinel,
and that sync wrappers catch LoopRunnerError / unexpected exceptions.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from horsies.core.types.result import is_ok, is_err
from horsies.core.workflows.lifecycle_types import (
    LifecycleErrorCode,
    LifecycleOperationError,
)


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
        broker.session_factory.return_value = cm
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
    broker.session_factory.return_value = cm
    return broker


# ===========================================================================
# pause_workflow async
# ===========================================================================


@pytest.mark.unit
class TestPauseWorkflowResult:
    """pause_workflow returns LifecycleResult[bool]."""

    @pytest.mark.asyncio(loop_scope='function')
    async def test_pause_running_returns_ok_true(self) -> None:
        """Workflow in RUNNING state -> Ok(True)."""
        from horsies.core.workflows.lifecycle import pause_workflow

        broker = _mock_broker(transition_row=('some-id',))
        result = await pause_workflow(broker, 'wf-1')

        assert is_ok(result)
        assert result.ok_value is True

    @pytest.mark.asyncio(loop_scope='function')
    async def test_pause_wrong_state_returns_ok_false(self) -> None:
        """Workflow exists but not RUNNING -> Ok(False)."""
        from horsies.core.workflows.lifecycle import pause_workflow

        broker = _mock_broker(transition_row=None, exists_row=('wf-1',))
        result = await pause_workflow(broker, 'wf-1')

        assert is_ok(result)
        assert result.ok_value is False

    @pytest.mark.asyncio(loop_scope='function')
    async def test_pause_not_found_returns_err(self) -> None:
        """Workflow does not exist -> Err(WORKFLOW_NOT_FOUND)."""
        from horsies.core.workflows.lifecycle import pause_workflow

        broker = _mock_broker(transition_row=None, exists_row=None)
        result = await pause_workflow(broker, 'wf-1')

        assert is_err(result)
        err = result.err_value
        assert isinstance(err, LifecycleOperationError)
        assert err.code == LifecycleErrorCode.WORKFLOW_NOT_FOUND
        assert err.operation == 'pause'
        assert err.stage == 'existence_check'
        assert err.workflow_id == 'wf-1'
        assert err.retryable is False

    @pytest.mark.asyncio(loop_scope='function')
    async def test_pause_db_failure_returns_db_operation_failed(self) -> None:
        """SQLAlchemy error -> Err(DB_OPERATION_FAILED)."""
        from sqlalchemy.exc import OperationalError

        from horsies.core.workflows.lifecycle import pause_workflow

        db_exc = OperationalError('SELECT 1', {}, Exception('connection refused'))
        broker = _mock_broker(side_effect=db_exc)
        result = await pause_workflow(broker, 'wf-1')

        assert is_err(result)
        err = result.err_value
        assert isinstance(err, LifecycleOperationError)
        assert err.code == LifecycleErrorCode.DB_OPERATION_FAILED
        assert err.operation == 'pause'
        assert err.stage == 'state_transition'
        assert err.workflow_id == 'wf-1'
        assert err.exception is not None

    @pytest.mark.asyncio(loop_scope='function')
    async def test_pause_non_db_failure_returns_internal_failed(self) -> None:
        """Non-DB exception -> Err(INTERNAL_FAILED), not DB_OPERATION_FAILED."""
        from horsies.core.workflows.lifecycle import pause_workflow

        broker = _mock_broker(side_effect=RuntimeError('logic bug'))
        result = await pause_workflow(broker, 'wf-1')

        assert is_err(result)
        err = result.err_value
        assert isinstance(err, LifecycleOperationError)
        assert err.code == LifecycleErrorCode.INTERNAL_FAILED
        assert err.retryable is False
        assert err.exception is not None


# ===========================================================================
# resume_workflow async
# ===========================================================================


@pytest.mark.unit
class TestResumeWorkflowResult:
    """resume_workflow returns LifecycleResult[bool]."""

    @pytest.mark.asyncio(loop_scope='function')
    async def test_resume_wrong_state_returns_ok_false(self) -> None:
        """Workflow exists but not PAUSED -> Ok(False)."""
        from horsies.core.workflows.lifecycle import resume_workflow

        broker = _mock_broker(transition_row=None, exists_row=('wf-1',))
        result = await resume_workflow(broker, 'wf-1')

        assert is_ok(result)
        assert result.ok_value is False

    @pytest.mark.asyncio(loop_scope='function')
    async def test_resume_not_found_returns_err(self) -> None:
        """Workflow does not exist -> Err(WORKFLOW_NOT_FOUND)."""
        from horsies.core.workflows.lifecycle import resume_workflow

        broker = _mock_broker(transition_row=None, exists_row=None)
        result = await resume_workflow(broker, 'wf-1')

        assert is_err(result)
        err = result.err_value
        assert isinstance(err, LifecycleOperationError)
        assert err.code == LifecycleErrorCode.WORKFLOW_NOT_FOUND
        assert err.operation == 'resume'
        assert err.stage == 'existence_check'
        assert err.workflow_id == 'wf-1'
        assert err.retryable is False

    @pytest.mark.asyncio(loop_scope='function')
    async def test_resume_db_failure_returns_db_operation_failed(self) -> None:
        """SQLAlchemy error -> Err(DB_OPERATION_FAILED)."""
        from sqlalchemy.exc import OperationalError

        from horsies.core.workflows.lifecycle import resume_workflow

        db_exc = OperationalError('SELECT 1', {}, Exception('connection refused'))
        broker = _mock_broker(side_effect=db_exc)
        result = await resume_workflow(broker, 'wf-1')

        assert is_err(result)
        err = result.err_value
        assert isinstance(err, LifecycleOperationError)
        assert err.code == LifecycleErrorCode.DB_OPERATION_FAILED
        assert err.operation == 'resume'
        assert err.stage == 'state_transition'
        assert err.workflow_id == 'wf-1'
        assert err.exception is not None

    @pytest.mark.asyncio(loop_scope='function')
    async def test_resume_non_db_failure_returns_internal_failed(self) -> None:
        """Non-DB exception -> Err(INTERNAL_FAILED), not DB_OPERATION_FAILED."""
        from horsies.core.workflows.lifecycle import resume_workflow

        broker = _mock_broker(side_effect=RuntimeError('logic bug'))
        result = await resume_workflow(broker, 'wf-1')

        assert is_err(result)
        err = result.err_value
        assert isinstance(err, LifecycleOperationError)
        assert err.code == LifecycleErrorCode.INTERNAL_FAILED
        assert err.retryable is False
        assert err.exception is not None


# ===========================================================================
# Sync wrappers: LoopRunnerError / unexpected exception handling
# ===========================================================================


@pytest.mark.unit
class TestPauseSyncWrapper:
    """pause_workflow_sync catches LoopRunnerError and unexpected exceptions."""

    def test_loop_runner_error_returns_err(self) -> None:
        """LoopRunnerError -> Err(LOOP_RUNNER_FAILED)."""
        from horsies.core.workflows.lifecycle import pause_workflow_sync
        from horsies.core.utils.loop_runner import LoopRunnerError

        broker = MagicMock()
        with patch(
            'horsies.core.utils.loop_runner.get_shared_runner',
        ) as mock_runner_fn:
            mock_runner_fn.return_value.call.side_effect = LoopRunnerError('dead')
            result = pause_workflow_sync(broker, 'wf-1')

        assert is_err(result)
        err = result.err_value
        assert isinstance(err, LifecycleOperationError)
        assert err.code == LifecycleErrorCode.LOOP_RUNNER_FAILED
        assert err.operation == 'pause'
        assert err.stage == 'sync_bridge'
        assert err.workflow_id == 'wf-1'
        assert err.retryable is False

    def test_unexpected_exception_returns_internal_failed(self) -> None:
        """Unexpected exception -> Err(INTERNAL_FAILED)."""
        from horsies.core.workflows.lifecycle import pause_workflow_sync

        broker = MagicMock()
        with patch(
            'horsies.core.utils.loop_runner.get_shared_runner',
        ) as mock_runner_fn:
            mock_runner_fn.return_value.call.side_effect = RuntimeError('boom')
            result = pause_workflow_sync(broker, 'wf-1')

        assert is_err(result)
        err = result.err_value
        assert isinstance(err, LifecycleOperationError)
        assert err.code == LifecycleErrorCode.INTERNAL_FAILED
        assert err.operation == 'pause'
        assert err.stage == 'sync_bridge'


@pytest.mark.unit
class TestResumeSyncWrapper:
    """resume_workflow_sync catches LoopRunnerError and unexpected exceptions."""

    def test_loop_runner_error_returns_err(self) -> None:
        """LoopRunnerError -> Err(LOOP_RUNNER_FAILED)."""
        from horsies.core.workflows.lifecycle import resume_workflow_sync
        from horsies.core.utils.loop_runner import LoopRunnerError

        broker = MagicMock()
        with patch(
            'horsies.core.utils.loop_runner.get_shared_runner',
        ) as mock_runner_fn:
            mock_runner_fn.return_value.call.side_effect = LoopRunnerError('dead')
            result = resume_workflow_sync(broker, 'wf-1')

        assert is_err(result)
        err = result.err_value
        assert isinstance(err, LifecycleOperationError)
        assert err.code == LifecycleErrorCode.LOOP_RUNNER_FAILED
        assert err.operation == 'resume'
        assert err.stage == 'sync_bridge'
        assert err.workflow_id == 'wf-1'
        assert err.retryable is False

    def test_unexpected_exception_returns_internal_failed(self) -> None:
        """Unexpected exception -> Err(INTERNAL_FAILED)."""
        from horsies.core.workflows.lifecycle import resume_workflow_sync

        broker = MagicMock()
        with patch(
            'horsies.core.utils.loop_runner.get_shared_runner',
        ) as mock_runner_fn:
            mock_runner_fn.return_value.call.side_effect = RuntimeError('boom')
            result = resume_workflow_sync(broker, 'wf-1')

        assert is_err(result)
        err = result.err_value
        assert isinstance(err, LifecycleOperationError)
        assert err.code == LifecycleErrorCode.INTERNAL_FAILED
        assert err.operation == 'resume'
        assert err.stage == 'sync_bridge'
