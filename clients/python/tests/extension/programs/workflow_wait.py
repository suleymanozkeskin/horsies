"""A sync ``WorkflowHandle.get()`` waits in the core's blocking form, and
Ctrl+C ends the wait.

The case starts a workflow of one task node with the sync start; no worker
runs, so the workflow stays RUNNING. The main thread then calls
``handle.get()`` with no limit: the wait has no call bound, so only a signal
can end it. A second thread sends SIGINT to the main thread after the wait
has run for ``WAIT_BEFORE_SIGNAL_S``; Python's default SIGINT handler raises
``KeyboardInterrupt``, as Ctrl+C does in a terminal.
"""

from __future__ import annotations

import queue
import signal
import threading
import time
from typing import Final, assert_never, cast

from pydantic import SecretStr

from horsies.core.app import Horsies
from horsies.core.models.app import AppConfig
from horsies.core.models.broker import PostgresConfig
from horsies.core.models.queues import QueueMode
from horsies.core.models.tasks import TaskError, TaskResult
from horsies.core.models.workflow import TaskNode
from horsies.core.types.result import Err, Ok
from tests.extension.programs.report import Report, run_case
from tests.extension.programs.support import database_url

WAIT_BEFORE_SIGNAL_S: Final = 0.5
"""How long the wait runs before the second thread sends SIGINT."""

THREAD_RESULT_BOUND_S: Final = 30.0
"""The longest wait for the second thread's send time after the call ended."""

TASK_NAME: Final = 'tests.extension.workflow_wait.value'
WORKFLOW_NAME: Final = 'tests.extension.workflow_wait'


def _result_text(result: object) -> str:
    """``Ok``, or ``Err`` and the type of the error, of a ``Result``, for the
    report."""
    match result:
        case Ok():
            return 'Ok'
        case Err():
            error = cast('Err[object]', result).err_value
            return f'Err {type(error).__name__}'
        case _:
            raise AssertionError(f'expected a Result, got {result!r}')


def ctrl_c_ends_a_blocking_get() -> Report:
    """Start a workflow that stays RUNNING, wait for it with no limit on the
    main thread, and send SIGINT to the main thread while it waits. Then
    read its status (the left wait changed nothing), cancel it, and close
    the broker."""
    url = database_url()
    _ = signal.signal(signal.SIGINT, signal.default_int_handler)
    app = Horsies(
        AppConfig(
            queue_mode=QueueMode.DEFAULT,
            broker=PostgresConfig(database_url=SecretStr(url)),
        ),
    )

    @app.task(task_name=TASK_NAME)
    def value_task(*, value: int) -> TaskResult[int, TaskError]:
        return TaskResult(ok=value)

    node: TaskNode[int] = TaskNode(fn=value_task, kwargs={'value': 1})
    spec = app.workflow(
        WORKFLOW_NAME, [node], output=node, definition_key=f'{WORKFLOW_NAME}.v1',
    )
    started = spec.start()
    match started:
        case Ok(ok_value=handle):
            pass
        case Err(err_value=error):
            raise RuntimeError(f'the start of the workflow failed: {error!r}')
        case _ as unreachable:
            assert_never(unreachable)
    main_thread_id = threading.main_thread().ident
    assert main_thread_id is not None, 'the main thread runs, so it has an id'
    waiting = threading.Event()
    sent: queue.Queue[float] = queue.Queue()

    def interrupt_the_wait() -> None:
        _ = waiting.wait(THREAD_RESULT_BOUND_S)
        time.sleep(WAIT_BEFORE_SIGNAL_S)
        sent.put(time.monotonic())
        signal.pthread_kill(main_thread_id, signal.SIGINT)

    sender = threading.Thread(target=interrupt_the_wait, name='ctrl-c')
    sender.start()
    waiting.set()
    call_started = time.monotonic()
    left_at: float | None = None
    try:
        outcome = handle.get()
        ended = f'returned {outcome!r}'
    except KeyboardInterrupt:
        left_at = time.monotonic()
        ended = 'left'
    signal_sent_at = sent.get(timeout=THREAD_RESULT_BOUND_S)
    sender.join(timeout=THREAD_RESULT_BOUND_S)
    match handle.status():
        case Ok(ok_value=status):
            status_after = status.value
        case Err(err_value=status_error):
            status_after = f'Err {status_error.code}'
        case _ as unreachable:
            assert_never(unreachable)
    cancelled = handle.cancel()
    closed = app.get_broker().close()
    return {
        'started': _result_text(started),
        'ended': ended,
        'wait_before_signal_s': WAIT_BEFORE_SIGNAL_S,
        'waited_before_signal_s': signal_sent_at - call_started,
        'seconds_after_signal': None if left_at is None else left_at - signal_sent_at,
        'status_after': status_after,
        'cancelled': _result_text(cancelled),
        'closed': _result_text(closed),
    }


if __name__ == '__main__':
    run_case({'ctrl_c_ends_a_blocking_get': ctrl_c_ends_a_blocking_get})
