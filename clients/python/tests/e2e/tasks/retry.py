"""Retry policy task definitions for e2e tests."""

from __future__ import annotations

import os
from pathlib import Path

from horsies.core.models.tasks import TaskResult, TaskError, RetryPolicy

from tests.e2e.tasks.instance import app


def _get_state_path(env_key: str) -> Path:
    value = os.environ.get(env_key)
    if not value:
        raise RuntimeError(f'Missing environment variable: {env_key}')
    return Path(value)


@app.task(
    task_name='e2e_retry_exhausted',
    retry_policy=RetryPolicy.fixed([1, 1, 1], auto_retry_for=['TRANSIENT']),
)
def retry_exhausted_task() -> TaskResult[str, TaskError]:
    return TaskResult(err=TaskError(error_code='TRANSIENT', message='always fails'))


@app.task(
    task_name='e2e_retry_success',
    retry_policy=RetryPolicy.fixed([1, 1, 1], auto_retry_for=['TRANSIENT']),
)
def retry_success_task() -> TaskResult[str, TaskError]:
    state_path = _get_state_path('E2E_RETRY_SUCCESS_PATH')
    try:
        count = int(state_path.read_text())
    except FileNotFoundError:
        count = 0

    count += 1
    state_path.write_text(str(count))

    if count < 3:
        return TaskResult(
            err=TaskError(error_code='TRANSIENT', message=f'attempt {count}')
        )
    return TaskResult(ok=f'succeeded_on_attempt_{count}')


@app.task(
    task_name='e2e_exponential',
    retry_policy=RetryPolicy.exponential(base_seconds=1, max_retries=3, auto_retry_for=['TRANSIENT']),
)
def exponential_task() -> TaskResult[str, TaskError]:
    return TaskResult(err=TaskError(error_code='TRANSIENT', message='fail'))


@app.task(
    task_name='e2e_retry_error_code',
    retry_policy=RetryPolicy.fixed([1], auto_retry_for=['RETRYABLE_ERROR']),
)
def retry_error_code_task() -> TaskResult[str, TaskError]:
    return TaskResult(err=TaskError(error_code='RETRYABLE_ERROR', message='Retryable error'))


@app.task(
    task_name='e2e_retry_mapped_exception',
    retry_policy=RetryPolicy.fixed([1], auto_retry_for=['MAPPED_VALUE_ERROR']),
    exception_mapper={ValueError: 'MAPPED_VALUE_ERROR'},
)
def retry_mapped_exception_task() -> TaskResult[str, TaskError]:
    raise ValueError('This should be mapped and retried')
