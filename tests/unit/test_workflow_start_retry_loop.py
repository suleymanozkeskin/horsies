"""Unit tests for the built-in retry loop in start_workflow_async.

Tests that transient errors trigger automatic retries with exponential
backoff when resend_on_transient_err=True, and that non-retryable errors
exit immediately.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from horsies.core.types.result import Ok, Err, is_ok, is_err
from horsies.core.workflows.lifecycle import (
    start_workflow_async,
    _START_RETRY_COUNT,
    _START_RETRY_INITIAL_MS,
    _START_RETRY_MAX_MS,
)
from horsies.core.workflows.start_types import (
    WorkflowStartErrorCode,
)


def _make_spec(name: str = 'test-wf') -> Any:
    """Create a minimal mock spec for start_workflow_async."""
    spec = MagicMock()
    spec.name = name
    spec.tasks = []
    spec.workflow_def_cls = None
    spec.output = None
    spec.success_policy = None
    spec.on_error.value = 'FAIL_WORKFLOW'
    spec.definition_key = None
    return spec


def _make_broker(
    *,
    schema_ok: bool = True,
    schema_retryable: bool = False,
) -> Any:
    """Create a mock broker.

    Args:
        schema_ok: If True, ensure_schema_initialized returns Ok.
        schema_retryable: If False and schema_ok is False, the schema error
            is non-retryable.
    """
    broker = MagicMock()

    if schema_ok:
        broker.ensure_schema_initialized = AsyncMock(return_value=Ok(True))
    else:
        schema_err = MagicMock()
        schema_err.message = 'Schema init failed'
        schema_err.retryable = schema_retryable
        schema_err.exception = None
        broker.ensure_schema_initialized = AsyncMock(return_value=Err(schema_err))

    return broker


# =============================================================================
# Retry disabled (resend_on_transient_err=False)
# =============================================================================


@pytest.mark.unit
class TestRetryDisabled:
    """When resend_on_transient_err=False, no retries happen."""

    @pytest.mark.asyncio
    async def test_single_attempt_on_db_error(self) -> None:
        """DB error with retry disabled returns Err immediately."""
        spec = _make_spec()
        broker = _make_broker()

        # session_factory raises on first call
        db_exc = Exception('connection refused')
        session_ctx = AsyncMock()
        session_ctx.__aenter__ = AsyncMock(side_effect=db_exc)
        broker.session_factory.return_value = session_ctx

        result = await start_workflow_async(
            spec, broker, 'wf-1', resend_on_transient_err=False,
        )

        assert is_err(result)
        assert result.err_value.code == WorkflowStartErrorCode.ENQUEUE_FAILED
        # ensure_schema_initialized called only once (no retry)
        assert broker.ensure_schema_initialized.call_count == 1

    @pytest.mark.asyncio
    async def test_single_attempt_on_schema_error(self) -> None:
        """Schema init error with retry disabled returns Err immediately."""
        spec = _make_spec()
        broker = _make_broker(schema_ok=False, schema_retryable=True)

        result = await start_workflow_async(
            spec, broker, 'wf-1', resend_on_transient_err=False,
        )

        assert is_err(result)
        assert result.err_value.code == WorkflowStartErrorCode.ENQUEUE_FAILED
        assert broker.ensure_schema_initialized.call_count == 1


# =============================================================================
# Retry enabled — transient errors
# =============================================================================


@pytest.mark.unit
class TestRetryOnTransientErrors:
    """When resend_on_transient_err=True, transient errors are retried."""

    @pytest.mark.asyncio
    async def test_transient_db_error_retries_then_succeeds(self) -> None:
        """Transient DB error retries and eventually succeeds."""
        spec = _make_spec()
        broker = _make_broker()

        # First two calls raise, third succeeds
        mock_session = AsyncMock()
        insert_result = MagicMock()
        insert_result.scalar_one_or_none.return_value = 'wf-1'
        mock_session.execute = AsyncMock(return_value=insert_result)
        mock_session.commit = AsyncMock()

        session_ctx = AsyncMock()
        call_count = 0

        async def session_enter(*_args: Any, **_kwargs: Any) -> Any:
            nonlocal call_count
            call_count += 1
            if call_count <= 2:
                raise Exception('connection refused')
            return mock_session

        session_ctx.__aenter__ = session_enter
        session_ctx.__aexit__ = AsyncMock(return_value=False)
        broker.session_factory.return_value = session_ctx

        with patch('horsies.core.workflows.lifecycle.is_retryable_connection_error', return_value=True):
            with patch('horsies.core.workflows.lifecycle.asyncio.sleep', new_callable=AsyncMock) as mock_sleep:
                result = await start_workflow_async(
                    spec, broker, 'wf-1', resend_on_transient_err=True,
                )

        assert is_ok(result)
        # 2 failed + 1 success = 3 schema init calls
        assert broker.ensure_schema_initialized.call_count == 3
        # 2 sleeps between attempts
        assert mock_sleep.call_count == 2

    @pytest.mark.asyncio
    async def test_transient_schema_error_retries_then_succeeds(self) -> None:
        """Schema init transient failure retries and eventually succeeds."""
        spec = _make_spec()
        broker = MagicMock()

        # Schema init: fail retryable twice, then succeed
        schema_err = MagicMock()
        schema_err.message = 'Schema init timeout'
        schema_err.retryable = True
        schema_err.exception = None

        mock_session = AsyncMock()
        insert_result = MagicMock()
        insert_result.scalar_one_or_none.return_value = 'wf-1'
        mock_session.execute = AsyncMock(return_value=insert_result)
        mock_session.commit = AsyncMock()

        session_ctx = AsyncMock()
        session_ctx.__aenter__ = AsyncMock(return_value=mock_session)
        session_ctx.__aexit__ = AsyncMock(return_value=False)
        broker.session_factory.return_value = session_ctx

        broker.ensure_schema_initialized = AsyncMock(
            side_effect=[Err(schema_err), Err(schema_err), Ok(True)],
        )

        with patch('horsies.core.workflows.lifecycle.asyncio.sleep', new_callable=AsyncMock) as mock_sleep:
            result = await start_workflow_async(
                spec, broker, 'wf-1', resend_on_transient_err=True,
            )

        assert is_ok(result)
        assert broker.ensure_schema_initialized.call_count == 3
        assert mock_sleep.call_count == 2

    @pytest.mark.asyncio
    async def test_exhausted_retries_returns_last_error(self) -> None:
        """When all retry attempts fail, returns Err with last error."""
        spec = _make_spec()
        broker = _make_broker()

        # All session opens fail
        session_ctx = AsyncMock()
        session_ctx.__aenter__ = AsyncMock(side_effect=Exception('connection refused'))
        broker.session_factory.return_value = session_ctx

        with patch('horsies.core.workflows.lifecycle.is_retryable_connection_error', return_value=True):
            with patch('horsies.core.workflows.lifecycle.asyncio.sleep', new_callable=AsyncMock) as mock_sleep:
                result = await start_workflow_async(
                    spec, broker, 'wf-1', resend_on_transient_err=True,
                )

        assert is_err(result)
        assert result.err_value.code == WorkflowStartErrorCode.ENQUEUE_FAILED
        assert result.err_value.retryable is True
        # 1 initial + 3 retries = 4 total, so 3 sleeps
        assert mock_sleep.call_count == _START_RETRY_COUNT


# =============================================================================
# Non-retryable errors stop immediately
# =============================================================================


@pytest.mark.unit
class TestNonRetryableErrorsNoRetry:
    """Non-retryable errors do not trigger retries even with the flag on."""

    @pytest.mark.asyncio
    async def test_non_retryable_db_error_no_retry(self) -> None:
        """Non-retryable DB error stops immediately despite flag."""
        spec = _make_spec()
        broker = _make_broker()

        session_ctx = AsyncMock()
        session_ctx.__aenter__ = AsyncMock(side_effect=Exception('auth failed'))
        broker.session_factory.return_value = session_ctx

        with patch('horsies.core.workflows.lifecycle.is_retryable_connection_error', return_value=False):
            with patch('horsies.core.workflows.lifecycle.asyncio.sleep', new_callable=AsyncMock) as mock_sleep:
                result = await start_workflow_async(
                    spec, broker, 'wf-1', resend_on_transient_err=True,
                )

        assert is_err(result)
        assert result.err_value.code == WorkflowStartErrorCode.ENQUEUE_FAILED
        assert result.err_value.retryable is False
        # No retries should happen
        mock_sleep.assert_not_called()

    @pytest.mark.asyncio
    async def test_non_retryable_schema_error_no_retry(self) -> None:
        """Non-retryable schema error stops immediately despite flag."""
        spec = _make_spec()
        broker = _make_broker(schema_ok=False, schema_retryable=False)

        with patch('horsies.core.workflows.lifecycle.asyncio.sleep', new_callable=AsyncMock) as mock_sleep:
            result = await start_workflow_async(
                spec, broker, 'wf-1', resend_on_transient_err=True,
            )

        assert is_err(result)
        assert result.err_value.code == WorkflowStartErrorCode.ENQUEUE_FAILED
        mock_sleep.assert_not_called()

    @pytest.mark.asyncio
    async def test_validation_error_never_retried(self) -> None:
        """Prevalidation errors (Zone 1) are never retried."""
        spec = _make_spec()
        # Add a task node with unresolved queue to trigger Zone 1 validation failure
        bad_task = MagicMock()
        bad_task.queue = None
        bad_task.priority = 100
        bad_task.name = 'bad-task'
        # Not a SubWorkflowNode
        type(bad_task).__name__ = 'TaskNode'
        spec.tasks = [bad_task]

        from horsies.core.models.workflow import SubWorkflowNode
        with patch('horsies.core.workflows.lifecycle.isinstance', side_effect=lambda obj, cls: isinstance.__wrapped__(obj, cls) if hasattr(isinstance, '__wrapped__') else False):
            pass

        # SubWorkflowNode check: our bad_task is not a SubWorkflowNode, so
        # the isinstance check will be False and it will check queue.
        # We need to make isinstance(bad_task, SubWorkflowNode) return False
        broker = _make_broker()

        with patch('horsies.core.workflows.lifecycle.asyncio.sleep', new_callable=AsyncMock) as mock_sleep:
            result = await start_workflow_async(
                spec, broker, 'wf-1', resend_on_transient_err=True,
            )

        assert is_err(result)
        assert result.err_value.code == WorkflowStartErrorCode.VALIDATION_FAILED
        assert result.err_value.retryable is False
        mock_sleep.assert_not_called()


# =============================================================================
# Exponential backoff values
# =============================================================================


@pytest.mark.unit
class TestBackoffValues:
    """Verify exponential backoff timing."""

    @pytest.mark.asyncio
    async def test_backoff_doubles_each_attempt(self) -> None:
        """Sleep durations follow exponential backoff with cap."""
        spec = _make_spec()
        broker = _make_broker()

        session_ctx = AsyncMock()
        session_ctx.__aenter__ = AsyncMock(side_effect=Exception('timeout'))
        broker.session_factory.return_value = session_ctx

        with patch('horsies.core.workflows.lifecycle.is_retryable_connection_error', return_value=True):
            with patch('horsies.core.workflows.lifecycle.asyncio.sleep', new_callable=AsyncMock) as mock_sleep:
                await start_workflow_async(
                    spec, broker, 'wf-1', resend_on_transient_err=True,
                )

        sleep_args = [call.args[0] for call in mock_sleep.call_args_list]
        expected = [
            min(_START_RETRY_INITIAL_MS * (2 ** i), _START_RETRY_MAX_MS) / 1000.0
            for i in range(_START_RETRY_COUNT)
        ]
        assert sleep_args == expected


# =============================================================================
# Sync wrapper forwards resend_on_transient_err
# =============================================================================


@pytest.mark.unit
class TestSyncWrapperForwarding:
    """start_workflow forwards resend_on_transient_err to start_workflow_async."""

    def test_sync_forwards_flag_to_async(self) -> None:
        """start_workflow passes resend_on_transient_err through to async."""
        from horsies.core.workflows.lifecycle import start_workflow

        spec = _make_spec()
        broker = MagicMock()
        mock_handle = MagicMock()

        with patch(
            'horsies.core.utils.loop_runner.get_shared_runner',
        ) as mock_get_runner:
            mock_runner = MagicMock()
            mock_runner.call.return_value = Ok(mock_handle)
            mock_get_runner.return_value = mock_runner

            result = start_workflow(
                spec, broker, 'wf-1', resend_on_transient_err=True,
            )

        assert is_ok(result)
        # Verify the async function was called with the flag
        call_kwargs = mock_runner.call.call_args
        assert call_kwargs.kwargs.get('resend_on_transient_err') is True
