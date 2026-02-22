"""Unit tests for WorkflowHandle sync wrapper error mapping.

Tests that _sync_call and _sync_task_result_call correctly translate
LoopRunnerError and unexpected exceptions into typed error payloads.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from horsies.core.models.tasks import TaskResult, TaskError, LibraryErrorCode
from horsies.core.models.workflow.handle import WorkflowHandle
from horsies.core.models.workflow.handle_types import HandleErrorCode
from horsies.core.types.result import is_ok, is_err
from horsies.core.utils.loop_runner import LoopRunnerError


def _make_handle() -> WorkflowHandle[Any]:
    """Create a WorkflowHandle with a mock broker."""
    broker = MagicMock()
    return WorkflowHandle(workflow_id='test-wf-id', broker=broker)


# =============================================================================
# Wrap-strategy sync wrappers (_sync_call)
# =============================================================================


@pytest.mark.unit
class TestSyncCallWrapStrategy:
    """Tests for _sync_call used by wrap-strategy sync methods."""

    def test_status_loop_runner_failure_returns_loop_runner_failed(self) -> None:
        """status() returns Err(LOOP_RUNNER_FAILED) when LoopRunner fails."""
        handle = _make_handle()

        with patch(
            'horsies.core.models.workflow.handle.get_shared_runner',
        ) as mock_runner:
            mock_runner.return_value.call.side_effect = LoopRunnerError(
                'Thread dead',
            )
            result = handle.status()

        assert is_err(result)
        assert result.err_value.code == HandleErrorCode.LOOP_RUNNER_FAILED
        assert result.err_value.operation == 'status'
        assert result.err_value.stage == 'loop_runner'
        assert result.err_value.retryable is False
        assert result.err_value.workflow_id == 'test-wf-id'

    def test_cancel_loop_runner_failure_returns_loop_runner_failed(self) -> None:
        """cancel() returns Err(LOOP_RUNNER_FAILED) when LoopRunner fails."""
        handle = _make_handle()

        with patch(
            'horsies.core.models.workflow.handle.get_shared_runner',
        ) as mock_runner:
            mock_runner.return_value.call.side_effect = LoopRunnerError(
                'Thread dead',
            )
            result = handle.cancel()

        assert is_err(result)
        assert result.err_value.code == HandleErrorCode.LOOP_RUNNER_FAILED
        assert result.err_value.operation == 'cancel'

    def test_pause_loop_runner_failure_returns_loop_runner_failed(self) -> None:
        """pause() returns Err(LOOP_RUNNER_FAILED) when LoopRunner fails."""
        handle = _make_handle()

        with patch(
            'horsies.core.models.workflow.handle.get_shared_runner',
        ) as mock_runner:
            mock_runner.return_value.call.side_effect = LoopRunnerError(
                'Thread dead',
            )
            result = handle.pause()

        assert is_err(result)
        assert result.err_value.code == HandleErrorCode.LOOP_RUNNER_FAILED
        assert result.err_value.operation == 'pause'

    def test_resume_loop_runner_failure_returns_loop_runner_failed(self) -> None:
        """resume() returns Err(LOOP_RUNNER_FAILED) when LoopRunner fails."""
        handle = _make_handle()

        with patch(
            'horsies.core.models.workflow.handle.get_shared_runner',
        ) as mock_runner:
            mock_runner.return_value.call.side_effect = LoopRunnerError(
                'Thread dead',
            )
            result = handle.resume()

        assert is_err(result)
        assert result.err_value.code == HandleErrorCode.LOOP_RUNNER_FAILED
        assert result.err_value.operation == 'resume'

    def test_results_loop_runner_failure_returns_loop_runner_failed(self) -> None:
        """results() returns Err(LOOP_RUNNER_FAILED) when LoopRunner fails."""
        handle = _make_handle()

        with patch(
            'horsies.core.models.workflow.handle.get_shared_runner',
        ) as mock_runner:
            mock_runner.return_value.call.side_effect = LoopRunnerError(
                'Thread dead',
            )
            result = handle.results()

        assert is_err(result)
        assert result.err_value.code == HandleErrorCode.LOOP_RUNNER_FAILED
        assert result.err_value.operation == 'results'

    def test_tasks_loop_runner_failure_returns_loop_runner_failed(self) -> None:
        """tasks() returns Err(LOOP_RUNNER_FAILED) when LoopRunner fails."""
        handle = _make_handle()

        with patch(
            'horsies.core.models.workflow.handle.get_shared_runner',
        ) as mock_runner:
            mock_runner.return_value.call.side_effect = LoopRunnerError(
                'Thread dead',
            )
            result = handle.tasks()

        assert is_err(result)
        assert result.err_value.code == HandleErrorCode.LOOP_RUNNER_FAILED
        assert result.err_value.operation == 'tasks'

    def test_status_unexpected_exception_returns_internal_failed(self) -> None:
        """status() returns Err(INTERNAL_FAILED) on unexpected exception."""
        handle = _make_handle()

        with patch(
            'horsies.core.models.workflow.handle.get_shared_runner',
        ) as mock_runner:
            mock_runner.return_value.call.side_effect = RuntimeError(
                'something unexpected',
            )
            result = handle.status()

        assert is_err(result)
        assert result.err_value.code == HandleErrorCode.INTERNAL_FAILED
        assert result.err_value.operation == 'status'
        assert result.err_value.retryable is False

    def test_sync_call_propagates_ok_result(self) -> None:
        """Wrap-strategy sync wrapper passes through Ok result from async."""
        handle = _make_handle()

        from horsies.core.types.result import Ok

        with patch(
            'horsies.core.models.workflow.handle.get_shared_runner',
        ) as mock_runner:
            mock_runner.return_value.call.return_value = Ok('RUNNING')
            result = handle.status()

        assert is_ok(result)
        assert result.ok_value == 'RUNNING'


# =============================================================================
# Fold-strategy sync wrappers (_sync_task_result_call)
# =============================================================================


@pytest.mark.unit
class TestSyncTaskResultCallFoldStrategy:
    """Tests for _sync_task_result_call used by fold-strategy sync methods."""

    def test_get_loop_runner_failure_returns_broker_error(self) -> None:
        """get() returns TaskResult(err=BROKER_ERROR) when LoopRunner fails."""
        handle = _make_handle()

        with patch(
            'horsies.core.models.workflow.handle.get_shared_runner',
        ) as mock_runner:
            mock_runner.return_value.call.side_effect = LoopRunnerError(
                'Thread dead',
            )
            result = handle.get(timeout_ms=100)

        assert result.is_err()
        assert result.unwrap_err().error_code == LibraryErrorCode.BROKER_ERROR
        assert 'Loop runner failed' in (result.unwrap_err().message or '')

    def test_result_for_loop_runner_failure_returns_broker_error(self) -> None:
        """result_for() returns TaskResult(err=BROKER_ERROR) when LoopRunner fails."""
        handle = _make_handle()

        # Create a mock node with a node_id
        mock_node = MagicMock()
        mock_node.node_id = 'test-node'

        with patch(
            'horsies.core.models.workflow.handle.get_shared_runner',
        ) as mock_runner:
            mock_runner.return_value.call.side_effect = LoopRunnerError(
                'Thread dead',
            )
            result = handle.result_for(mock_node)

        assert result.is_err()
        assert result.unwrap_err().error_code == LibraryErrorCode.BROKER_ERROR

    def test_get_unexpected_exception_returns_broker_error(self) -> None:
        """get() returns TaskResult(err=BROKER_ERROR) on unexpected exception."""
        handle = _make_handle()

        with patch(
            'horsies.core.models.workflow.handle.get_shared_runner',
        ) as mock_runner:
            mock_runner.return_value.call.side_effect = RuntimeError(
                'something unexpected',
            )
            result = handle.get(timeout_ms=100)

        assert result.is_err()
        assert result.unwrap_err().error_code == LibraryErrorCode.BROKER_ERROR
        assert 'Unexpected error' in (result.unwrap_err().message or '')

    def test_fold_sync_call_propagates_task_result(self) -> None:
        """Fold-strategy sync wrapper passes through TaskResult from async."""
        handle = _make_handle()

        expected = TaskResult(ok=42)

        with patch(
            'horsies.core.models.workflow.handle.get_shared_runner',
        ) as mock_runner:
            mock_runner.return_value.call.return_value = expected
            result = handle.get(timeout_ms=100)

        assert result.is_ok()
        assert result.unwrap() == 42
