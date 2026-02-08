"""Basic task definitions for e2e tests (Layer 1)."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, cast

from pydantic import BaseModel

from horsies.core.models.tasks import TaskResult, TaskError, LibraryErrorCode

from tests.e2e.tasks.instance import app


@app.task(task_name='e2e_healthcheck')
def healthcheck() -> TaskResult[str, TaskError]:
    return TaskResult(ok='ready')


@app.task(task_name='e2e_simple')
def simple_task(x: int) -> TaskResult[int, TaskError]:
    return TaskResult(ok=x * 2)


@app.task(task_name='e2e_primitives')
def primitives_task(
    i: int,
    f: float,
    s: str,
    b: bool,
    n: None,
) -> TaskResult[dict[str, Any], TaskError]:
    return TaskResult(ok={'i': i, 'f': f, 's': s, 'b': b, 'n': n})


@app.task(task_name='e2e_collections')
def collections_task(
    lst: list[int],
    dct: dict[str, int],
    tpl: tuple[int, ...],
) -> TaskResult[dict[str, Any], TaskError]:
    return TaskResult(ok={'lst': lst, 'dct': dct, 'tpl': list(tpl)})


class UserInput(BaseModel):
    name: str
    age: int


@app.task(task_name='e2e_pydantic')
def pydantic_task(user: UserInput) -> TaskResult[str, TaskError]:
    return TaskResult(ok=f'{user.name} is {user.age}')


@dataclass
class DataInput:
    x: int
    y: int


@app.task(task_name='e2e_dataclass')
def dataclass_task(data: DataInput) -> TaskResult[int, TaskError]:
    return TaskResult(ok=data.x + data.y)


@app.task(task_name='e2e_kwargs')
def kwargs_task(
    required: int,
    optional: str = 'default',
    multiplier: int = 1,
) -> TaskResult[str, TaskError]:
    return TaskResult(ok=f'{required * multiplier}_{optional}')


@app.task(task_name='e2e_error')
def error_task() -> TaskResult[int, TaskError]:
    return TaskResult(
        err=TaskError(
            error_code='DELIBERATE_ERROR',
            message='This is intentional',
            data={'key': 'value'},
        )
    )


@app.task(task_name='e2e_exception')
def exception_task() -> TaskResult[int, TaskError]:
    raise ValueError('Deliberate exception')


@app.task(task_name='e2e_type_mismatch')
def type_mismatch_task() -> TaskResult[int, TaskError]:
    return TaskResult(ok=cast(int, 'string_not_int'))


class OutputModel(BaseModel):
    value: int
    nested: dict[str, list[int]]


@app.task(task_name='e2e_complex_result')
def complex_result_task() -> TaskResult[OutputModel, TaskError]:
    return TaskResult(ok=OutputModel(value=42, nested={'a': [1, 2, 3], 'b': [4, 5]}))


@app.task(task_name='e2e_no_retry')
def no_retry_task() -> TaskResult[str, TaskError]:
    return TaskResult(err=TaskError(error_code='PERMANENT', message='not retryable'))


@app.task(task_name='e2e_return_none')
def return_none_task() -> TaskResult[int, TaskError]:
    return None  # type: ignore[return-value]


@app.task(task_name='e2e_error_code')
def error_code_task() -> TaskResult[str, TaskError]:
    return TaskResult(
        err=TaskError(error_code=LibraryErrorCode.TASK_EXCEPTION, message='boom')
    )


@app.task(task_name='e2e_slow')
def slow_task(duration_ms: int) -> TaskResult[str, TaskError]:
    """Task that sleeps for specified duration."""
    import time

    time.sleep(duration_ms / 1000)
    return TaskResult(ok=f'slept_{duration_ms}')


@app.task(task_name='e2e_unserializable')
def unserializable_result_task() -> TaskResult[Any, TaskError]:
    """Task returning unserializable value for error handling tests."""

    def identity(x: Any) -> Any:
        return x

    return TaskResult(ok=identity)


@app.task(task_name='e2e_idempotent')
def idempotent_task(token: str) -> TaskResult[str, TaskError]:
    """
    Task that uses atomic file creation to detect double execution.
    If the file already exists (token already used), returns an error.
    """
    import os

    log_dir = os.environ.get('E2E_IDEMPOTENT_LOG_DIR')
    if not log_dir:
        return TaskResult(
            err=TaskError(
                error_code='CONFIG_ERROR', message='E2E_IDEMPOTENT_LOG_DIR not set'
            )
        )

    token_file = os.path.join(log_dir, token)
    try:
        # O_CREAT | O_EXCL: create file exclusively, fails if exists
        fd = os.open(token_file, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        os.write(fd, b'executed')
        os.close(fd)
        return TaskResult(ok=f'executed:{token}')
    except FileExistsError:
        return TaskResult(
            err=TaskError(
                error_code='DOUBLE_EXECUTION', message=f'Token {token} already executed'
            )
        )
