"""Assertion helpers for e2e tests."""

from __future__ import annotations

from typing import Any

from horsies.core.models.tasks import TaskResult, TaskError


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
