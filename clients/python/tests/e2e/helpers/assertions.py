"""Assertion helpers for e2e tests.

A read of a task or a workflow (``handle.get``, ``app.get_result``,
``result_for``) is a ``ReadResult``: ``Ok(Succeeded(value))``,
``Ok(Failed(task_error))`` or ``Err(read_error)``. The helpers below take
one read and return the part that the test expects, or fail the test with
the read that came instead.
"""

from __future__ import annotations

import asyncio
from typing import Any

import pytest

from horsies.core.models.outcome import (
    Failed,
    ReadError,
    ReadResult,
    Succeeded,
    TaskOutcome,
)
from horsies.core.models.tasks import TaskError
from horsies.core.models.task_send_types import TaskSendResult
from horsies.core.models.workflow import WorkflowHandle, WorkflowSpec
from horsies.core.task_decorator import TaskHandle
from horsies.core.types.result import Err, Ok, is_err


async def start_ok_in_thread(
    spec: WorkflowSpec[Any],
    workflow_id: str | None = None,
) -> WorkflowHandle[Any]:
    """Start with the sync ``spec.start()`` and unwrap it, or fail the test.

    The sync start blocks its thread, and on the test's event-loop thread it
    returns ASYNC_CONTEXT. It runs on a worker thread, so the e2e suite
    keeps its coverage of the sync start path.
    """
    r = await asyncio.to_thread(spec.start, workflow_id)
    if is_err(r):
        pytest.fail(f'spec.start() failed: {r.err_value}')
    return r.ok_value


def unwrap_send(result: TaskSendResult[TaskHandle[Any]]) -> TaskHandle[Any]:
    """Unwrap a send result, failing the test on error."""
    if is_err(result):
        pytest.fail(f'send failed: {result.err_value}')
    return result.ok_value


def read_succeeded(read: ReadResult[Any, TaskError]) -> bool:
    """Whether ``read`` is ``Ok(Succeeded(...))``. A readiness probe polls
    with it: a failed task and a read failure are both "not yet"."""
    match read:
        case Ok(Succeeded()):
            return True
        case _:
            return False


def succeeded_value[T](read: ReadResult[T, TaskError]) -> T:
    """The value of ``Ok(Succeeded(value))``. Any other read fails the test."""
    match read:
        case Ok(Succeeded(value)):
            return value
        case _:
            pytest.fail(f'expected Ok(Succeeded(...)), got {read!r}')


def failed_error(read: ReadResult[Any, TaskError]) -> TaskError:
    """The task error of ``Ok(Failed(task_error))``. Any other read fails the
    test."""
    match read:
        case Ok(Failed(task_error)):
            return task_error
        case _:
            pytest.fail(f'expected Ok(Failed(...)), got {read!r}')


def outcome_of[T](read: ReadResult[T, TaskError]) -> TaskOutcome[T, TaskError]:
    """The outcome of ``Ok(outcome)``. ``Err(read_error)`` fails the test."""
    match read:
        case Ok(ok_value=outcome):
            return outcome
        case Err(err_value=error):
            pytest.fail(f'expected an outcome, got Err({error!r})')


def read_error(read: ReadResult[Any, TaskError]) -> ReadError:
    """The read failure of ``Err(read_error)``. An outcome fails the test."""
    match read:
        case Err(err_value=error):
            return error
        case _:
            pytest.fail(f'expected Err(<read error>), got {read!r}')


def assert_succeeded(
    read: ReadResult[Any, TaskError],
    expected_value: object | None = None,
) -> None:
    """Assert ``read`` is ``Ok(Succeeded(...))``; check the value when
    ``expected_value`` is not None."""
    value = succeeded_value(read)
    if expected_value is not None:
        assert value == expected_value, f'expected {expected_value!r}, got {value!r}'


def assert_failed(
    read: ReadResult[Any, TaskError],
    expected_code: str | None = None,
) -> None:
    """Assert ``read`` is ``Ok(Failed(task_error))``; check the error code
    when ``expected_code`` is not None."""
    task_error = failed_error(read)
    if expected_code is not None:
        assert task_error.error_code == expected_code, (
            f'expected error code {expected_code!r}, got {task_error!r}'
        )


def outcome_value[T](outcome: TaskOutcome[T, TaskError] | None) -> T:
    """The value of ``Succeeded(value)``. ``Failed`` or no outcome fails the
    test."""
    match outcome:
        case Succeeded(value):
            return value
        case _:
            pytest.fail(f'expected Succeeded(...), got {outcome!r}')


def outcome_error(outcome: TaskOutcome[Any, TaskError] | None) -> TaskError:
    """The task error of ``Failed(task_error)``. ``Succeeded`` or no outcome
    fails the test."""
    match outcome:
        case Failed(task_error):
            return task_error
        case _:
            pytest.fail(f'expected Failed(...), got {outcome!r}')
