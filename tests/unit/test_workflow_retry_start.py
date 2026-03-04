"""Unit tests for WorkflowSpec.retry_start / retry_start_async.

Tests that _validate_start_retry correctly gates retry eligibility
and that retry_start / retry_start_async delegate to start / start_async
with the stored workflow_id.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock, patch, AsyncMock

import pytest

from horsies.core.models.workflow.spec import WorkflowSpec
from horsies.core.types.result import Ok, Err, is_ok, is_err
from horsies.core.workflows.start_types import (
    WorkflowStartError,
    WorkflowStartErrorCode,
)


def _make_spec(name: str = 'test-wf') -> WorkflowSpec[Any]:
    """Create a minimal WorkflowSpec bypassing __post_init__ validation."""
    spec = object.__new__(WorkflowSpec)
    # Set only the fields needed for retry_start / _validate_start_retry
    object.__setattr__(spec, 'name', name)
    object.__setattr__(spec, 'broker', MagicMock())
    object.__setattr__(spec, 'tasks', [])
    object.__setattr__(spec, 'resend_on_transient_err', False)
    return spec


# =============================================================================
# _validate_start_retry
# =============================================================================


@pytest.mark.unit
class TestValidateStartRetry:
    """Tests for _validate_start_retry validation logic."""

    def test_accepts_enqueue_failed_error(self) -> None:
        """ENQUEUE_FAILED error is accepted for retry."""
        spec = _make_spec()
        error = WorkflowStartError(
            code=WorkflowStartErrorCode.ENQUEUE_FAILED,
            message='DB connection lost',
            retryable=True,
            workflow_name='test-wf',
            workflow_id='wf-123',
        )
        result = spec._validate_start_retry(error)

        assert is_ok(result)
        assert result.ok_value == 'wf-123'

    def test_rejects_broker_not_configured(self) -> None:
        """BROKER_NOT_CONFIGURED errors cannot be retried."""
        spec = _make_spec()
        error = WorkflowStartError(
            code=WorkflowStartErrorCode.BROKER_NOT_CONFIGURED,
            message='No broker',
            retryable=False,
            workflow_name='test-wf',
            workflow_id='wf-123',
        )
        result = spec._validate_start_retry(error)

        assert is_err(result)
        assert result.err_value.code == WorkflowStartErrorCode.VALIDATION_FAILED
        assert 'ENQUEUE_FAILED' in result.err_value.message

    def test_rejects_validation_failed(self) -> None:
        """VALIDATION_FAILED errors cannot be retried."""
        spec = _make_spec()
        error = WorkflowStartError(
            code=WorkflowStartErrorCode.VALIDATION_FAILED,
            message='Bad DAG',
            retryable=False,
            workflow_name='test-wf',
            workflow_id='wf-123',
        )
        result = spec._validate_start_retry(error)

        assert is_err(result)
        assert result.err_value.code == WorkflowStartErrorCode.VALIDATION_FAILED

    def test_rejects_internal_failed(self) -> None:
        """INTERNAL_FAILED errors cannot be retried."""
        spec = _make_spec()
        error = WorkflowStartError(
            code=WorkflowStartErrorCode.INTERNAL_FAILED,
            message='Bug',
            retryable=False,
            workflow_name='test-wf',
            workflow_id='wf-123',
        )
        result = spec._validate_start_retry(error)

        assert is_err(result)
        assert result.err_value.code == WorkflowStartErrorCode.VALIDATION_FAILED

    def test_rejects_cross_workflow_name(self) -> None:
        """Error from a different workflow name is rejected."""
        spec = _make_spec(name='my-workflow')
        error = WorkflowStartError(
            code=WorkflowStartErrorCode.ENQUEUE_FAILED,
            message='DB connection lost',
            retryable=True,
            workflow_name='other-workflow',
            workflow_id='wf-123',
        )
        result = spec._validate_start_retry(error)

        assert is_err(result)
        assert result.err_value.code == WorkflowStartErrorCode.VALIDATION_FAILED
        assert 'cross-workflow' in result.err_value.message


# =============================================================================
# retry_start (sync)
# =============================================================================


@pytest.mark.unit
class TestRetryStart:
    """Tests for WorkflowSpec.retry_start sync method."""

    def test_happy_path_delegates_to_start(self) -> None:
        """Successful retry delegates to start() with stored workflow_id."""
        spec = _make_spec()
        error = WorkflowStartError(
            code=WorkflowStartErrorCode.ENQUEUE_FAILED,
            message='DB timeout',
            retryable=True,
            workflow_name='test-wf',
            workflow_id='wf-retry-123',
        )

        mock_handle = MagicMock()
        with patch.object(spec, 'start', return_value=Ok(mock_handle)) as mock_start:
            result = spec.retry_start(error)

        assert is_ok(result)
        assert result.ok_value is mock_handle
        mock_start.assert_called_once_with(workflow_id='wf-retry-123')

    def test_validation_failure_does_not_call_start(self) -> None:
        """Invalid error code skips start() entirely."""
        spec = _make_spec()
        error = WorkflowStartError(
            code=WorkflowStartErrorCode.VALIDATION_FAILED,
            message='Bad spec',
            retryable=False,
            workflow_name='test-wf',
            workflow_id='wf-123',
        )

        with patch.object(spec, 'start') as mock_start:
            result = spec.retry_start(error)

        assert is_err(result)
        mock_start.assert_not_called()


# =============================================================================
# retry_start_async
# =============================================================================


@pytest.mark.unit
class TestRetryStartAsync:
    """Tests for WorkflowSpec.retry_start_async method."""

    @pytest.mark.asyncio
    async def test_happy_path_delegates_to_start_async(self) -> None:
        """Successful async retry delegates to start_async() with stored workflow_id."""
        spec = _make_spec()
        error = WorkflowStartError(
            code=WorkflowStartErrorCode.ENQUEUE_FAILED,
            message='DB timeout',
            retryable=True,
            workflow_name='test-wf',
            workflow_id='wf-retry-456',
        )

        mock_handle = MagicMock()
        with patch.object(
            spec, 'start_async', new_callable=AsyncMock, return_value=Ok(mock_handle),
        ) as mock_start_async:
            result = await spec.retry_start_async(error)

        assert is_ok(result)
        assert result.ok_value is mock_handle
        mock_start_async.assert_called_once_with(workflow_id='wf-retry-456')

    @pytest.mark.asyncio
    async def test_validation_failure_does_not_call_start_async(self) -> None:
        """Invalid error code skips start_async() entirely."""
        spec = _make_spec()
        error = WorkflowStartError(
            code=WorkflowStartErrorCode.INTERNAL_FAILED,
            message='Bug',
            retryable=False,
            workflow_name='test-wf',
            workflow_id='wf-123',
        )

        with patch.object(spec, 'start_async', new_callable=AsyncMock) as mock_start_async:
            result = await spec.retry_start_async(error)

        assert is_err(result)
        mock_start_async.assert_not_called()
