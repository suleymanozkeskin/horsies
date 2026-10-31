"""Unit tests for WorkflowSpec.retry_start / retry_start_async.

Tests that _validate_start_retry accepts the ``WorkflowStartRetryable``
variants, fails an assertion on another variant, refuses another workflow's
error, and that retry_start / retry_start_async delegate to start /
start_async with the stored workflow_id.
"""

from __future__ import annotations

from typing import Any, cast
from unittest.mock import MagicMock, patch, AsyncMock

import pytest

from horsies.core.brokers.resources import EventLoopLimit
from horsies.core.models.workflow.spec import WorkflowSpec
from horsies.core.types.error_detail import NoCause
from horsies.core.types.result import Ok, is_ok, is_err
from horsies.core.utils.loop_runner import LoopRunnerError
from horsies.core.workflows.start_types import (
    WorkflowStartAsyncContext,
    WorkflowStartBrokerNotConfigured,
    WorkflowStartEnqueueFailed,
    WorkflowStartError,
    WorkflowStartEventLoopLimit,
    WorkflowStartIdConflict,
    WorkflowStartLoopRunnerFailed,
    WorkflowStartRetryable,
    WorkflowStartValidationFailed,
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


def _enqueue_failed(
    workflow_id: str = 'wf-123', workflow_name: str = 'test-wf',
) -> WorkflowStartEnqueueFailed:
    return WorkflowStartEnqueueFailed(
        message='DB connection lost',
        retryable=True,
        workflow_name=workflow_name,
        workflow_id=workflow_id,
        exception=ConnectionError('lost'),
    )


def _retryable_variants(workflow_id: str) -> list[WorkflowStartRetryable]:
    return [
        _enqueue_failed(workflow_id),
        WorkflowStartAsyncContext(
            message='sync start in a running loop',
            workflow_name='test-wf',
            workflow_id=workflow_id,
        ),
        WorkflowStartLoopRunnerFailed(
            message='Loop runner is not running',
            workflow_name='test-wf',
            workflow_id=workflow_id,
            exception=LoopRunnerError('stopped'),
        ),
    ]


def _refused_variants() -> list[WorkflowStartError]:
    return [
        WorkflowStartBrokerNotConfigured(
            message='No broker', workflow_name='test-wf', workflow_id='wf-123',
        ),
        WorkflowStartValidationFailed(
            message='Bad DAG',
            workflow_name='test-wf',
            workflow_id='wf-123',
            exception=NoCause(),
        ),
        WorkflowStartIdConflict(
            message='id taken',
            workflow_name='test-wf',
            workflow_id='wf-123',
            existing_workflow_name='other-wf',
        ),
        WorkflowStartEventLoopLimit(
            message='loop limit',
            workflow_name='test-wf',
            workflow_id='wf-123',
            loop_limit=EventLoopLimit(
                limit=5,
                held_thread_names=('MainThread',),
                current_thread_name='worker-1',
            ),
        ),
    ]


def _variant_id(error: object) -> str:
    return type(error).__name__


# =============================================================================
# _validate_start_retry
# =============================================================================


@pytest.mark.unit
class TestValidateStartRetry:
    """Tests for _validate_start_retry validation logic."""

    @pytest.mark.parametrize('error', _retryable_variants('wf-123'), ids=_variant_id)
    def test_accepts_each_retryable_variant(self, error: WorkflowStartRetryable) -> None:
        spec = _make_spec()

        result = spec._validate_start_retry(error)

        assert is_ok(result)
        assert result.ok_value == 'wf-123'

    @pytest.mark.parametrize('error', _refused_variants(), ids=_variant_id)
    def test_another_variant_fails_an_assertion_that_names_it(
        self, error: WorkflowStartError,
    ) -> None:
        spec = _make_spec()

        with pytest.raises(AssertionError, match=type(error).__name__):
            spec._validate_start_retry(cast(WorkflowStartRetryable, error))

    def test_rejects_cross_workflow_name(self) -> None:
        """Error from a different workflow name is rejected."""
        spec = _make_spec(name='my-workflow')
        error = _enqueue_failed(workflow_name='other-workflow')

        result = spec._validate_start_retry(error)

        assert is_err(result)
        assert isinstance(result.err_value, WorkflowStartValidationFailed)
        assert 'cross-workflow' in result.err_value.message
        assert result.err_value.exception == NoCause()
        assert result.err_value.workflow_id == 'wf-123'


# =============================================================================
# retry_start (sync)
# =============================================================================


@pytest.mark.unit
class TestRetryStart:
    """Tests for WorkflowSpec.retry_start sync method."""

    @pytest.mark.parametrize(
        'error', _retryable_variants('wf-retry-123'), ids=_variant_id,
    )
    def test_happy_path_delegates_to_start(self, error: WorkflowStartRetryable) -> None:
        """Successful retry delegates to start() with stored workflow_id."""
        spec = _make_spec()

        mock_handle = MagicMock()
        with patch.object(spec, 'start', return_value=Ok(mock_handle)) as mock_start:
            result = spec.retry_start(error)

        assert is_ok(result)
        assert result.ok_value is mock_handle
        mock_start.assert_called_once_with(workflow_id='wf-retry-123')

    def test_cross_workflow_error_does_not_call_start(self) -> None:
        """Another workflow's error skips start() entirely."""
        spec = _make_spec()
        error = _enqueue_failed(workflow_name='other-wf')

        with patch.object(spec, 'start') as mock_start:
            result = spec.retry_start(error)

        assert is_err(result)
        assert isinstance(result.err_value, WorkflowStartValidationFailed)
        mock_start.assert_not_called()


# =============================================================================
# retry_start_async
# =============================================================================


@pytest.mark.unit
class TestRetryStartAsync:
    """Tests for WorkflowSpec.retry_start_async method."""

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        'error', _retryable_variants('wf-retry-456'), ids=_variant_id,
    )
    async def test_happy_path_delegates_to_start_async(
        self, error: WorkflowStartRetryable,
    ) -> None:
        """Successful async retry delegates to start_async() with stored workflow_id."""
        spec = _make_spec()

        mock_handle = MagicMock()
        with patch.object(
            spec, 'start_async', new_callable=AsyncMock, return_value=Ok(mock_handle),
        ) as mock_start_async:
            result = await spec.retry_start_async(error)

        assert is_ok(result)
        assert result.ok_value is mock_handle
        mock_start_async.assert_called_once_with(workflow_id='wf-retry-456')

    @pytest.mark.asyncio
    async def test_cross_workflow_error_does_not_call_start_async(self) -> None:
        """Another workflow's error skips start_async() entirely."""
        spec = _make_spec()
        error = _enqueue_failed(workflow_name='other-wf')

        with patch.object(spec, 'start_async', new_callable=AsyncMock) as mock_start_async:
            result = await spec.retry_start_async(error)

        assert is_err(result)
        mock_start_async.assert_not_called()
