"""Basic task definitions for e2e tests (Layer 1)."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, cast

from pydantic import BaseModel

from horsies.core.codec.json_value import JsonValue
from horsies.core.models.tasks import TaskResult, TaskError, OperationalErrorCode

from tests.e2e.tasks.instance import app

# Per-child retained allocations: appended to (never freed) so child RSS
# ratchets up across tasks, driving the max_memory_per_child_mb recycle.
_RETAINED_BUFFERS: list[bytearray] = []


@app.task(task_name='e2e_healthcheck')
def healthcheck() -> TaskResult[str, TaskError]:
    return TaskResult(ok='ready')


@app.task(task_name='e2e_simple')
def simple_task(*, x: int) -> TaskResult[int, TaskError]:
    return TaskResult(ok=x * 2)


@app.task(task_name='e2e_primitives')
def primitives_task(
    *, i: int,
    f: float,
    s: str,
    b: bool,
    n: None,
) -> TaskResult[dict[str, JsonValue], TaskError]:
    return TaskResult(ok={'i': i, 'f': f, 's': s, 'b': b, 'n': n})


@app.task(task_name='e2e_collections')
def collections_task(
    *, lst: list[int],
    dct: dict[str, int],
    tpl: tuple[int, ...],
) -> TaskResult[dict[str, JsonValue], TaskError]:
    return TaskResult(ok={'lst': lst, 'dct': dct, 'tpl': list(tpl)})


class UserInput(BaseModel):
    name: str
    age: int


@app.task(task_name='e2e_pydantic')
def pydantic_task(*, user: UserInput) -> TaskResult[str, TaskError]:
    return TaskResult(ok=f'{user.name} is {user.age}')


@dataclass
class DataInput:
    x: int
    y: int


@app.task(task_name='e2e_dataclass')
def dataclass_task(*, data: DataInput) -> TaskResult[int, TaskError]:
    return TaskResult(ok=data.x + data.y)


@app.task(task_name='e2e_kwargs')
def kwargs_task(
    *, required: int,
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
        err=TaskError(error_code=OperationalErrorCode.TASK_EXCEPTION, message='boom')
    )


@app.task(task_name='e2e_pid')
def pid_task(*, sleep_ms: int = 0) -> TaskResult[int, TaskError]:
    """Return the executing child process PID (for recycle/rotation tests)."""
    import os
    import time

    if sleep_ms:
        time.sleep(sleep_ms / 1000)
    return TaskResult(ok=os.getpid())


@app.task(task_name='e2e_alloc_pid')
def alloc_pid_task(*, mb: int, sleep_ms: int = 0) -> TaskResult[int, TaskError]:
    """Retain `mb` MB in the child, then return its PID.

    Retained allocations accumulate per child, so child RSS ratchets up across
    calls until it crosses ``max_memory_per_child_mb`` and the child recycles.
    """
    import os
    import time

    _RETAINED_BUFFERS.append(bytearray(mb * 1024 * 1024))
    if sleep_ms:
        time.sleep(sleep_ms / 1000)
    return TaskResult(ok=os.getpid())


@app.task(task_name='e2e_alloc_then_raise')
def alloc_then_raise_task(*, mb: int) -> TaskResult[int, TaskError]:
    """Retain `mb` MB then raise — exercises the recycle/exception branch."""
    _RETAINED_BUFFERS.append(bytearray(mb * 1024 * 1024))
    raise ValueError('deliberate exception after allocation')


@app.task(task_name='e2e_slow')
def slow_task(*, duration_ms: int) -> TaskResult[str, TaskError]:
    """Task that sleeps for specified duration."""
    import time

    time.sleep(duration_ms / 1000)
    return TaskResult(ok=f'slept_{duration_ms}')


@app.task(task_name='e2e_timeout_sleeper', timeout_ms=2_000)
def timeout_sleeper(*, duration_ms: int) -> TaskResult[str, TaskError]:
    """Sleeps past its 2s timeout_ms so the parent-side deadline kill fires."""
    import time

    time.sleep(duration_ms / 1000)
    return TaskResult(ok=f'slept_{duration_ms}')


@app.task(task_name='e2e_pool_breaker')
def pool_breaker() -> TaskResult[str, TaskError]:
    """SIGKILLs its own child process: every execution breaks the pool.

    The kill happens after the child has confirmed RUNNING, so crash
    recovery classifies the row WORKER_CRASHED deterministically.
    """
    import os
    import signal

    os.kill(os.getpid(), signal.SIGKILL)
    return TaskResult(ok='unreachable')


@app.task(task_name='e2e_unserializable')
def unserializable_result_task() -> TaskResult[int, TaskError]:
    """Task that declares int return but actually returns a callable.

    Exists to drive RETURN_TYPE_MISMATCH error-handling tests: the runtime
    `ok_type_adapter.validate_python` rejects the callable against the
    declared `int` slot, returning an Err.
    """

    def identity(x: Any) -> Any:
        return x

    return TaskResult(ok=identity)  # type: ignore[arg-type]


@app.task(task_name='e2e_idempotent')
def idempotent_task(*, token: str) -> TaskResult[str, TaskError]:
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
