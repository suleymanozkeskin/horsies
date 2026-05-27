"""Assertion helpers for e2e tests."""

from __future__ import annotations

from typing import Any

import pytest

from horsies.core.brokers.result_types import BrokerResult
from horsies.core.models.tasks import TaskResult, TaskError
from horsies.core.models.task_send_types import TaskSendResult
from horsies.core.models.workflow import WorkflowHandle, WorkflowSpec
from horsies.core.task_decorator import TaskHandle
from horsies.core.types.result import is_err, is_ok


def start_ok_sync(
    spec: WorkflowSpec[Any],
    workflow_id: str | None = None,
) -> WorkflowHandle[Any]:
    """Unwrap sync start result or fail test with error details."""
    r = spec.start(workflow_id)
    if is_err(r):
        pytest.fail(f'spec.start() failed: {r.err_value}')
    return r.ok_value


def unwrap_send(result: TaskSendResult[TaskHandle[Any]]) -> TaskHandle[Any]:
    """Unwrap a send result, failing the test on error."""
    if is_err(result):
        pytest.fail(f'send failed: {result.err_value}')
    return result.ok_value


def unwrap_get_result(
    result: BrokerResult[TaskResult[Any, TaskError]],
) -> TaskResult[Any, TaskError]:
    """Unwrap the outer BrokerResult from app.get_result; fail-fast on outer Err.

    Strict-serde retrieval surfaces infrastructure decode failures as
    Err(BrokerOperationError). Tests that expect a domain-level
    TaskResult (ok/err) use this helper to peel the outer envelope and
    fail loudly if the broker layer itself errored.
    """
    if is_err(result):
        pytest.fail(f'app.get_result outer Err: {result.err_value}')
    return result.ok_value


def assert_ok(
    result: TaskResult[Any, TaskError],
    expected_value: Any | None = None,
) -> None:
    """Assert result is Ok and optionally check value."""
    assert result.is_ok(), f'Expected Ok, got Err: {result.err}'
    if expected_value is not None:
        assert result.unwrap() == expected_value


def assert_err(
    result: TaskResult[Any, TaskError],
    expected_code: str | None = None,
) -> None:
    """Assert result is Err and optionally check error code."""
    assert result.is_err(), f'Expected Err, got Ok: {result.ok}'
    if expected_code is not None:
        assert result.err is not None
        assert result.err.error_code == expected_code
