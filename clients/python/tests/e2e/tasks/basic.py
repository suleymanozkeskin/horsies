"""Basic task definitions for e2e tests (Layer 1)."""

from __future__ import annotations

import threading
from dataclasses import dataclass
from typing import Any, cast

from pydantic import BaseModel

from horsies.core.brokers.result_types import BrokerWaitInsideTask
from horsies.core.codec.json_value import JsonValue
from horsies.core.models.outcome import ReadResult, ReadWaitInsideTask
from horsies.core.models.tasks import (
    OperationalErrorCode,
    TaskError,
)
from horsies.core.types.result import Err, Ok, Result, is_err, is_ok

from tests.e2e.tasks.instance import app

# Per-child retained allocations: appended to (never freed) so child RSS
# ratchets up across tasks, driving the max_memory_per_child_mb recycle.
_RETAINED_BUFFERS: list[bytearray] = []


@app.task(name='e2e_healthcheck')
def healthcheck() -> Result[str, TaskError]:
    return Ok('ready')


@app.task(name='e2e_task_send_pool')
def task_send_pool() -> Result[str, TaskError]:
    """The pool of the app broker that task code in this child opens when it
    sends: ``pool_size:max_overflow``."""
    config = app.get_broker().config
    return Ok(f'{config.pool_size}:{config.max_overflow}')


@app.task(name='e2e_simple')
def simple_task(*, x: int) -> Result[int, TaskError]:
    return Ok(x * 2)


@app.task(name='e2e_primitives')
def primitives_task(
    *, i: int,
    f: float,
    s: str,
    b: bool,
    n: None,
) -> Result[dict[str, JsonValue], TaskError]:
    return Ok({'i': i, 'f': f, 's': s, 'b': b, 'n': n})


@app.task(name='e2e_collections')
def collections_task(
    *, lst: list[int],
    dct: dict[str, int],
    tpl: tuple[int, ...],
) -> Result[dict[str, JsonValue], TaskError]:
    return Ok({'lst': lst, 'dct': dct, 'tpl': list(tpl)})


class UserInput(BaseModel):
    name: str
    age: int


@app.task(name='e2e_pydantic')
def pydantic_task(*, user: UserInput) -> Result[str, TaskError]:
    return Ok(f'{user.name} is {user.age}')


@dataclass
class DataInput:
    x: int
    y: int


@app.task(name='e2e_dataclass')
def dataclass_task(*, data: DataInput) -> Result[int, TaskError]:
    return Ok(data.x + data.y)


@app.task(name='e2e_kwargs')
def kwargs_task(
    *, required: int,
    optional: str = 'default',
    multiplier: int = 1,
) -> Result[str, TaskError]:
    return Ok(f'{required * multiplier}_{optional}')


@app.task(name='e2e_error')
def error_task() -> Result[int, TaskError]:
    return Err(
        TaskError(
            code='DELIBERATE_ERROR',
            message='This is intentional',
            data={'key': 'value'},
        )
    )


@app.task(name='e2e_exception')
def exception_task() -> Result[int, TaskError]:
    raise ValueError('Deliberate exception')


@app.task(name='e2e_type_mismatch')
def type_mismatch_task() -> Result[int, TaskError]:
    return Ok(cast(int, 'string_not_int'))


class OutputModel(BaseModel):
    value: int
    nested: dict[str, list[int]]


@app.task(name='e2e_complex_result')
def complex_result_task() -> Result[OutputModel, TaskError]:
    return Ok(OutputModel(value=42, nested={'a': [1, 2, 3], 'b': [4, 5]}))


@app.task(name='e2e_no_retry')
def no_retry_task() -> Result[str, TaskError]:
    return Err(TaskError(code='PERMANENT', message='not retryable'))


@app.task(name='e2e_return_none')
def return_none_task() -> Result[int, TaskError]:
    return None  # type: ignore[return-value]


@app.task(name='e2e_error_code')
def error_code_task() -> Result[str, TaskError]:
    return Err(
        TaskError(code=OperationalErrorCode.TASK_EXCEPTION, message='boom')
    )


@app.task(name='e2e_pid')
def pid_task(*, sleep_ms: int = 0) -> Result[int, TaskError]:
    """Return the executing child process PID (for recycle/rotation tests)."""
    import os
    import time

    if sleep_ms:
        time.sleep(sleep_ms / 1000)
    return Ok(os.getpid())


@app.task(name='e2e_alloc_pid')
def alloc_pid_task(*, mb: int, sleep_ms: int = 0) -> Result[int, TaskError]:
    """Retain `mb` MB in the child, then return its PID.

    Retained allocations accumulate per child, so child RSS ratchets up across
    calls until it crosses ``max_memory_per_child_mb`` and the child recycles.
    """
    import os
    import time

    _RETAINED_BUFFERS.append(bytearray(mb * 1024 * 1024))
    if sleep_ms:
        time.sleep(sleep_ms / 1000)
    return Ok(os.getpid())


@app.task(name='e2e_alloc_then_raise')
def alloc_then_raise_task(*, mb: int) -> Result[int, TaskError]:
    """Retain `mb` MB then raise — exercises the recycle/exception branch."""
    _RETAINED_BUFFERS.append(bytearray(mb * 1024 * 1024))
    raise ValueError('deliberate exception after allocation')


@app.task(name='e2e_slow')
def slow_task(*, duration_ms: int) -> Result[str, TaskError]:
    """Task that sleeps for specified duration."""
    import time

    time.sleep(duration_ms / 1000)
    return Ok(f'slept_{duration_ms}')


@app.task(name='e2e_timeout_sleeper', timeout_ms=2_000)
def timeout_sleeper(*, duration_ms: int) -> Result[str, TaskError]:
    """Sleeps past its 2s timeout_ms so the parent-side deadline kill fires."""
    import time

    time.sleep(duration_ms / 1000)
    return Ok(f'slept_{duration_ms}')


@app.task(name='e2e_pool_breaker')
def pool_breaker() -> Result[str, TaskError]:
    """SIGKILLs its own child process: every execution breaks the pool.

    The kill happens after the child has confirmed RUNNING, so crash
    recovery classifies the row WORKER_CRASHED deterministically.
    """
    import os
    import signal

    os.kill(os.getpid(), signal.SIGKILL)
    return Ok('unreachable')


@app.task(name='e2e_unserializable')
def unserializable_result_task() -> Result[int, TaskError]:
    """Task that declares int return but actually returns a callable.

    Exists to drive RETURN_TYPE_MISMATCH error-handling tests: the runtime
    `ok_type_adapter.validate_python` rejects the callable against the
    declared `int` slot, returning an Err. The Err names the actual type,
    not the callable, so the worker can encode and store it.
    """

    def identity(x: Any) -> Any:
        return x

    return Ok(identity)  # type: ignore[arg-type]


@app.task(name='e2e_idempotent')
def idempotent_task(*, token: str) -> Result[str, TaskError]:
    """
    Task that uses atomic file creation to detect double execution.
    If the file already exists (token already used), returns an error.
    """
    import os

    log_dir = os.environ.get('E2E_IDEMPOTENT_LOG_DIR')
    if not log_dir:
        return Err(
            TaskError(
                code='CONFIG_ERROR', message='E2E_IDEMPOTENT_LOG_DIR not set'
            )
        )

    token_file = os.path.join(log_dir, token)
    try:
        # O_CREAT | O_EXCL: create file exclusively, fails if exists
        fd = os.open(token_file, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
        os.write(fd, b'executed')
        os.close(fd)
        return Ok(f'executed:{token}')
    except FileExistsError:
        return Err(
            TaskError(
                code='DOUBLE_EXECUTION', message=f'Token {token} already executed'
            )
        )


WAIT_PROBE_JOIN_TIMEOUT_S = 10.0
"""Bound on the wait for the probe thread. A refused wait returns at once."""


def _refused(read: ReadResult[Any, TaskError]) -> bool:
    match read:
        case Err(err_value=ReadWaitInsideTask()):
            return True
        case _:
            return False


@app.task(name='e2e_wait_inside_task')
def wait_inside_task() -> Result[dict[str, bool], TaskError]:
    """Send a task and start a workflow, then try the waits that a task cannot make.

    One wait runs on a thread that the task starts: the refusal belongs to the
    process, not to the thread.
    """
    from tests.e2e.tasks.workflows import spec_single_node

    sent = simple_task.send(x=1)
    if is_err(sent):
        return Err(
            TaskError(code='E2E_SEND_FAILED', message=str(sent.err_value)),
        )
    handle = sent.ok_value
    thread_refusals: list[bool] = []

    def wait_on_a_thread() -> None:
        thread_refusals.append(_refused(handle.get(wait_ms=1000)))

    thread = threading.Thread(target=wait_on_a_thread, name='e2e-wait-on-a-thread')
    thread.start()
    thread.join(timeout=WAIT_PROBE_JOIN_TIMEOUT_S)
    read = app.get_result(handle.task_id, 1000)
    raw = app.get_raw_result(handle.task_id, 1000)
    started = spec_single_node.start()
    return Ok(
        {
            'get_refused': _refused(handle.get(wait_ms=1000)),
            'thread_get_refused': thread_refusals == [True],
            'app_get_result_refused': _refused(read),
            'raw_refused': is_err(raw)
            and isinstance(raw.err_value, BrokerWaitInsideTask),
            'workflow_started': is_ok(started),
            'workflow_get_refused': is_ok(started)
            and _refused(started.ok_value.get(wait_ms=1000)),
            'info_read': is_ok(handle.info()),
        },
    )
