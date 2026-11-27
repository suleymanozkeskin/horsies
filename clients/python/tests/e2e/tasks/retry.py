"""Retry policy task definitions for e2e tests."""

from __future__ import annotations

import os
from enum import Enum
from pathlib import Path

from horsies.core.models.tasks import TaskError, RetryPolicy
from horsies.core.types.result import Err, Ok, Result

from tests.e2e.tasks.instance import app


class RetryCode(str, Enum):
    """The codes that the retry tasks retry on."""

    TRANSIENT = 'TRANSIENT'
    RETRYABLE_ERROR = 'RETRYABLE_ERROR'
    MAPPED_VALUE_ERROR = 'MAPPED_VALUE_ERROR'


def _get_state_path(env_key: str) -> Path:
    value = os.environ.get(env_key)
    if not value:
        raise RuntimeError(f'Missing environment variable: {env_key}')
    return Path(value)


@app.task(
    name='e2e_retry_exhausted',
    error_codes=RetryCode,
    retry_policy=RetryPolicy.fixed([1, 1, 1], retry_for=[RetryCode.TRANSIENT]),
)
def retry_exhausted_task() -> Result[str, TaskError]:
    return Err(TaskError(code='TRANSIENT', message='always fails'))


@app.task(
    name='e2e_retry_success',
    error_codes=RetryCode,
    retry_policy=RetryPolicy.fixed([1, 1, 1], retry_for=[RetryCode.TRANSIENT]),
)
def retry_success_task() -> Result[str, TaskError]:
    state_path = _get_state_path('E2E_RETRY_SUCCESS_PATH')
    try:
        count = int(state_path.read_text())
    except FileNotFoundError:
        count = 0

    count += 1
    state_path.write_text(str(count))

    if count < 3:
        return Err(
            TaskError(code='TRANSIENT', message=f'attempt {count}')
        )
    return Ok(f'succeeded_on_attempt_{count}')


@app.task(
    name='e2e_exponential',
    error_codes=RetryCode,
    retry_policy=RetryPolicy.exponential(
        base_seconds=1, max_retries=3, retry_for=[RetryCode.TRANSIENT],
    ),
)
def exponential_task() -> Result[str, TaskError]:
    return Err(TaskError(code='TRANSIENT', message='fail'))


@app.task(
    name='e2e_retry_error_code',
    error_codes=RetryCode,
    retry_policy=RetryPolicy.fixed([1], retry_for=[RetryCode.RETRYABLE_ERROR]),
)
def retry_error_code_task() -> Result[str, TaskError]:
    return Err(TaskError(code='RETRYABLE_ERROR', message='Retryable error'))


@app.task(
    name='e2e_retry_mapped_exception',
    error_codes=RetryCode,
    retry_policy=RetryPolicy.fixed([1], retry_for=[RetryCode.MAPPED_VALUE_ERROR]),
    exception_mapper={ValueError: 'MAPPED_VALUE_ERROR'},
)
def retry_mapped_exception_task() -> Result[str, TaskError]:
    raise ValueError('This should be mapped and retried')
