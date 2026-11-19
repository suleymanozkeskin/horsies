"""Cases of the blocking call form: the GIL is released while the call runs,
and a signal handler that raises ends the call between two 100 ms slices.

A held ACCESS EXCLUSIVE lock on ``horsies_tasks`` keeps a call in flight; a
second thread sees the call's backend wait for the lock and then acts. The
handles use the ``ASSUME_INITIALIZED`` schema mode: the database has the
schema (``conftest.extension_database``), and no call installs it.
"""

from __future__ import annotations

import logging
import queue
import signal
import threading
import time
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from types import FrameType
from typing import Final

from horsies._core import CoreHandle
from tests.extension.programs.report import JsonValue, Report, run_case
from tests.extension.programs.support import (
    RECORD_WAIT_BOUND_S,
    TasksTableLock,
    WaiterSeen,
    collect_records,
    database_url,
    delete_tasks,
    manifest_json,
    minted_task_id,
    send_request,
    task_row_exists,
    wait_for_new_waiter,
    wait_until,
    waiter_report,
)

PROGRESS_S: Final = 0.2
"""How long the second thread counts while the blocking call waits."""

THREAD_RESULT_BOUND_S: Final = 30.0
"""The longest wait for the second thread's result after the call ended."""

SETTLE_S: Final = 1.0
"""The wait after a released lock before the case reads the bridge records:
a record of a left read would come within it, as the write's record did."""

WRITE_LEFT_MESSAGE: Final = 'a write of CoreHandle.enqueue ended after its caller left'


def _handle(url: str) -> CoreHandle:
    return CoreHandle(
        manifest_json(url, schema='ASSUME_INITIALIZED', echo=False),
        pool_profile='PRODUCER',
    )


def _task_info(handle: CoreHandle, task_id: uuid.UUID) -> object:
    return handle.task_info(
        task_id,
        include_result=False,
        include_failed_reason=False,
        include_attempts=False,
    )


@dataclass(frozen=True)
class ReleaserResult:
    """What the second thread of the GIL case saw and did."""

    waiter: WaiterSeen
    ticks: int


def gil_released() -> Report:
    """A blocking read waits for the held lock; a second Python thread sees
    it wait, counts for ``PROGRESS_S``, and releases the lock. The read can
    end only after that thread ran Python code."""
    url = database_url()
    handle = _handle(url)
    warm_up = handle.ping_database()
    lock = TasksTableLock(url)
    known = lock.waiting_backends()
    lock.take()
    results: queue.Queue[ReleaserResult] = queue.Queue()

    def count_then_release() -> None:
        waiter = wait_for_new_waiter(lock, known)
        ticks = 0
        until = time.monotonic() + PROGRESS_S
        while time.monotonic() < until:
            ticks += 1
        lock.release()
        results.put(ReleaserResult(waiter=waiter, ticks=ticks))

    releaser = threading.Thread(target=count_then_release, name='lock-releaser')
    releaser.start()
    started = time.monotonic()
    outcome = _task_info(handle, minted_task_id())
    call_s = time.monotonic() - started
    result = results.get(timeout=THREAD_RESULT_BOUND_S)
    releaser.join(timeout=THREAD_RESULT_BOUND_S)
    lock.close()
    return {
        'warm_up': type(warm_up).__name__,
        'outcome': type(outcome).__name__,
        'call_s': call_s,
        'waiter': waiter_report(result.waiter),
        'ticks': result.ticks,
    }


class LeftByHandler(Exception):
    """Raised by the case's SIGUSR1 handler while a call is armed."""


@dataclass(frozen=True)
class SignalSent:
    """The second thread sent SIGUSR1 at ``sent_at`` (monotonic seconds)."""

    waiter: WaiterSeen
    sent_at: float


def _leave_blocked_call(lock: TasksTableLock, call: Callable[[], object]) -> Report:
    """Run ``call`` on the main thread; a second thread waits until a new
    backend waits for the lock and then sends SIGUSR1 to the main thread,
    whose handler raises ``LeftByHandler`` while the call runs."""
    known = lock.waiting_backends()
    armed = threading.Event()
    sent: queue.Queue[SignalSent] = queue.Queue()
    main_thread_id = threading.main_thread().ident
    assert main_thread_id is not None, 'the main thread runs, so it has an id'

    def raise_while_armed(signum: int, frame: FrameType | None) -> None:
        _ = (signum, frame)
        if armed.is_set():
            raise LeftByHandler()

    def signal_when_waiting() -> None:
        waiter = wait_for_new_waiter(lock, known)
        sent.put(SignalSent(waiter=waiter, sent_at=time.monotonic()))
        signal.pthread_kill(main_thread_id, signal.SIGUSR1)

    _ = signal.signal(signal.SIGUSR1, raise_while_armed)
    sender = threading.Thread(target=signal_when_waiting, name='signal-sender')
    armed.set()
    sender.start()
    left_at: float | None = None
    try:
        outcome = call()
        ended = f'returned {type(outcome).__name__}'
    except LeftByHandler:
        left_at = time.monotonic()
        ended = 'left'
    armed.clear()
    signal_sent = sent.get(timeout=THREAD_RESULT_BOUND_S)
    sender.join(timeout=THREAD_RESULT_BOUND_S)
    seconds_after_signal: JsonValue = (
        None if left_at is None else left_at - signal_sent.sent_at
    )
    return {
        'ended': ended,
        'waiter': waiter_report(signal_sent.waiter),
        'seconds_after_signal': seconds_after_signal,
    }


def signal_leaves_calls() -> Report:
    """A blocking read, then a blocking write, each left by a raising signal
    handler while it waits for the held lock. Then the lock is released: the
    write runs to its end and logs that its caller left; the read was
    dropped and logs nothing. Each call has its own handle, so the write
    does not reuse the connection of the dropped read."""
    url = database_url()
    bridge = collect_records('horsies.bridge', logging.DEBUG)
    read_handle = _handle(url)
    write_handle = _handle(url)
    warm_up_id = minted_task_id()
    warm_up_ping = read_handle.ping_database()
    warm_up_send = write_handle.enqueue(send_request(warm_up_id))
    lock = TasksTableLock(url)
    lock.take()
    read = _leave_blocked_call(lock, lambda: _task_info(read_handle, minted_task_id()))
    write_id = minted_task_id()
    write = _leave_blocked_call(lock, lambda: write_handle.enqueue(send_request(write_id)))
    lock.release()
    write_ended = wait_until(
        lambda: task_row_exists(url, write_id) and WRITE_LEFT_MESSAGE in bridge.messages(),
        RECORD_WAIT_BOUND_S,
    )
    time.sleep(SETTLE_S)
    messages: list[JsonValue] = [message for message in bridge.messages()]
    ping_after = read_handle.ping_database()
    lock.close()
    delete_tasks(url, [warm_up_id, write_id])
    return {
        'warm_up_ping': type(warm_up_ping).__name__,
        'warm_up_send': type(warm_up_send).__name__,
        'read': read,
        'write': write,
        'write_ended': write_ended,
        'bridge_messages': messages,
        'ping_after': type(ping_after).__name__,
    }


if __name__ == '__main__':
    run_case({'gil_released': gil_released, 'signal_leaves_calls': signal_leaves_calls})
