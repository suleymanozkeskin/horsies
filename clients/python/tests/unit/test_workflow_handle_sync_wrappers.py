"""Unit tests for the error mapping of WorkflowHandle's sync methods.

Each sync method calls a blocking form of the core handle. A core boundary
error of that call (``CoreError``) is the method's own failure: the
wrap-strategy methods return ``Err(HandleDbOperationFailed)`` with the error,
retryable by the core's rule; the read-strategy methods return
``Err(BrokerError)`` with the error, its type and text, retryable by the same
rule. Any other exception propagates unchanged, and a record passes to the
method's answer.
"""

from __future__ import annotations

import uuid
from collections.abc import Callable
from typing import Any, Final
from unittest.mock import MagicMock

import pytest

import horsies._core as core
from horsies.core.models.outcome import BrokerError, Succeeded
from horsies.core.models.workflow import NodeKey
from horsies.core.models.workflow.handle import WorkflowHandle
from horsies.core.models.workflow.handle_types import (
    HandleDbOperationFailed,
    HandleErrorCode,
)
from horsies.core.models.workflow.enums import WorkflowStatus
from horsies.core.types.result import Err, Ok, is_err, is_ok

WORKFLOW_ID: Final = '2b4d6f8a-0c2e-4a6c-9e1a-3c5e7a9c1e3a'
WORKFLOW_KEY: Final = uuid.UUID(WORKFLOW_ID)
NODE_KEY: Final[NodeKey[Any]] = NodeKey('test-node')


def _make_handle(method: str, answer: object) -> tuple[WorkflowHandle[Any], MagicMock]:
    """A WorkflowHandle, and its broker (a mock) whose blocking ``method``
    answers ``answer``: a record, or an exception that the call raises."""
    broker = MagicMock()
    match answer:
        case BaseException():
            setattr(broker._core_handle, method, MagicMock(side_effect=answer))
        case _:
            setattr(broker._core_handle, method, MagicMock(return_value=answer))
    return WorkflowHandle(workflow_id=WORKFLOW_KEY, broker=broker), broker


# =============================================================================
# Wrap strategy
# =============================================================================

WRAP_METHODS: Final[dict[str, tuple[str, Callable[[WorkflowHandle[Any]], Any]]]] = {
    'status': ('workflow_status', lambda handle: handle.status()),
    'cancel': ('cancel_workflow', lambda handle: handle.cancel()),
    'pause': ('pause_workflow', lambda handle: handle.pause()),
    'resume': ('resume_workflow', lambda handle: handle.resume()),
    'results': ('workflow_node_results', lambda handle: handle.results()),
    'nodes': ('workflow_nodes', lambda handle: handle.nodes()),
}


@pytest.mark.unit
class TestWrapStrategy:
    """The wrap-strategy sync methods and a core boundary error."""

    @pytest.mark.parametrize('name', list(WRAP_METHODS))
    def test_a_core_boundary_error_is_db_operation_failed(self, name: str) -> None:
        method, call = WRAP_METHODS[name]
        refused = core.TooManyPendingCalls(f'CoreHandle.{method}', 65_536)
        handle, broker = _make_handle(method, refused)

        result = call(handle)

        assert is_err(result)
        error = result.err_value
        assert isinstance(error, HandleDbOperationFailed)
        assert error.code == HandleErrorCode.DB_OPERATION_FAILED
        assert error.retryable is True
        assert error.workflow_id == WORKFLOW_ID
        assert error.exception is refused
        getattr(broker._core_handle, f'{method}_async').assert_not_called()

    @pytest.mark.parametrize('name', list(WRAP_METHODS))
    def test_an_unexpected_exception_propagates(self, name: str) -> None:
        method, call = WRAP_METHODS[name]
        defect = RuntimeError('something unexpected')
        handle, _ = _make_handle(method, defect)

        with pytest.raises(RuntimeError) as exc_info:
            call(handle)

        assert exc_info.value is defect

    def test_a_record_passes_to_the_answer(self) -> None:
        read = MagicMock(spec=core.WorkflowStatusRead)
        read.status = 'RUNNING'
        handle, broker = _make_handle('workflow_status', read)

        result = handle.status()

        assert is_ok(result)
        assert result.ok_value is WorkflowStatus.RUNNING
        broker._core_handle.workflow_status.assert_called_once_with(WORKFLOW_KEY)


# =============================================================================
# Read strategy
# =============================================================================

READ_METHODS: Final[dict[str, tuple[str, Callable[[WorkflowHandle[Any]], Any]]]] = {
    'get': ('wait_workflow_result', lambda handle: handle.get(timeout_ms=100)),
    'result_for': ('workflow_node_result', lambda handle: handle.result_for(NODE_KEY)),
}


@pytest.mark.unit
class TestReadStrategy:
    """The read-strategy sync methods and a core boundary error."""

    @pytest.mark.parametrize('name', list(READ_METHODS))
    def test_a_core_boundary_error_is_broker_error(self, name: str) -> None:
        method, call = READ_METHODS[name]
        refused = core.TooManyPendingCalls(f'CoreHandle.{method}', 65_536)
        handle, broker = _make_handle(method, refused)

        result = call(handle)

        assert isinstance(result, Err)
        error = result.err_value
        assert isinstance(error, BrokerError)
        assert 'TooManyPendingCalls' in error.message
        assert error.retryable is True
        assert error.exception is refused
        getattr(broker._core_handle, f'{method}_async').assert_not_called()

    @pytest.mark.parametrize('name', list(READ_METHODS))
    def test_an_unexpected_exception_propagates(self, name: str) -> None:
        method, call = READ_METHODS[name]
        defect = RuntimeError('something unexpected')
        handle, _ = _make_handle(method, defect)

        with pytest.raises(RuntimeError) as exc_info:
            call(handle)

        assert exc_info.value is defect

    def test_a_record_passes_to_the_answer(self) -> None:
        completed = MagicMock(spec=core.WorkflowCompletedRead)
        completed.result_json = '{"__h_task_result__": true, "ok": 42, "err": null}'
        handle, broker = _make_handle('wait_workflow_result', completed)
        handle.output_type = int

        result = handle.get(timeout_ms=100)

        assert result == Ok(Succeeded(42))
        broker._core_handle.wait_workflow_result.assert_called_once_with(
            WORKFLOW_KEY, timeout_ms=100,
        )
