"""Cases of the async call form: the future of a call belongs to the calling
loop, on asyncio and on uvloop; 1,000 calls in flight add no OS thread; an
idle producer after its first send keeps the threads of the core only; a
cancelled read is dropped and a cancelled write runs to its end.

The handles use the ``ASSUME_INITIALIZED`` schema mode: the database has the
schema (``conftest.extension_database``), and no call installs it.
"""

from __future__ import annotations

import asyncio
import importlib
import importlib.util
import logging
import uuid
from collections import Counter
from collections.abc import Callable, Coroutine
from dataclasses import dataclass
from typing import Any, Final, Literal, cast

from horsies._core import CoreHandle
from tests.extension.programs.report import JsonValue, Report, run_case
from tests.extension.programs.support import (
    RECORD_WAIT_BOUND_S,
    TasksTableLock,
    address_url,
    collect_records,
    database_url,
    delete_tasks,
    manifest_json,
    minted_task_id,
    os_thread_count,
    send_request,
    task_row_exists,
    wait_for_new_waiter_async,
    wait_until_async,
    waiter_report,
)

type LoopKind = Literal['asyncio', 'uvloop']

CONCURRENT_AWAITS: Final = 1_000
"""The calls in flight at once in the thread count case."""

THREAD_SAMPLE_INTERVAL_S: Final = 0.001
"""The pause between two thread count samples while the calls are in flight."""

PRODUCER_IDLE_S: Final = 2.0
"""How long the producer case makes no call after its first send before its
last thread count."""

SETTLE_S: Final = 1.0
"""The wait after a released lock before the case reads the bridge records:
a record of a cancelled read would come within it, as the write's record did."""

WRITE_DISCARDED_MESSAGE: Final = (
    'CoreHandle.enqueue_async ended after its awaiting future was cancelled; '
    'the outcome was discarded'
)


@dataclass(frozen=True)
class LoopUnavailable:
    """The loop's package is not installed in this environment."""

    package: str


def _loop_factory(kind: LoopKind) -> Callable[[], asyncio.AbstractEventLoop] | LoopUnavailable:
    match kind:
        case 'asyncio':
            return asyncio.new_event_loop
        case 'uvloop':
            if importlib.util.find_spec('uvloop') is None:
                return LoopUnavailable(package='uvloop')
            factory: object = getattr(importlib.import_module('uvloop'), 'new_event_loop')
            assert callable(factory), 'uvloop.new_event_loop is a function'
            return cast(Callable[[], asyncio.AbstractEventLoop], factory)


def _run_on(kind: LoopKind, main: Callable[[], Coroutine[Any, Any, Report]]) -> Report:
    """The report of ``main`` run on a new loop of ``kind``, or the report of
    a loop that is not installed."""
    match _loop_factory(kind):
        case LoopUnavailable(package=package):
            return {'loop': kind, 'loop_available': False, 'missing_package': package}
        case factory:
            with asyncio.Runner(loop_factory=factory) as runner:
                report = runner.run(main())
            return {'loop': kind, 'loop_available': True, **report}


def _handle(url: str) -> CoreHandle:
    return CoreHandle(
        manifest_json(url, schema='ASSUME_INITIALIZED', echo=False),
        pool_profile='PRODUCER',
    )


async def _future_on_calling_loop() -> Report:
    handle = _handle(database_url())
    running_loop = asyncio.get_running_loop()
    future = handle.ping_database_async()
    is_future = asyncio.isfuture(future)
    on_calling_loop = future.get_loop() is running_loop
    outcome = await future
    return {
        'is_future': is_future,
        'on_calling_loop': on_calling_loop,
        'outcome': type(outcome).__name__,
    }


def future_on_calling_loop_asyncio() -> Report:
    """``ping_database_async`` on an asyncio loop."""
    return _run_on('asyncio', _future_on_calling_loop)


def future_on_calling_loop_uvloop() -> Report:
    """``ping_database_async`` on a uvloop loop."""
    return _run_on('uvloop', _future_on_calling_loop)


async def _sample_threads(samples: list[int]) -> None:
    """Append the OS thread count every ``THREAD_SAMPLE_INTERVAL_S`` until cancelled."""
    while True:
        samples.append(os_thread_count())
        await asyncio.sleep(THREAD_SAMPLE_INTERVAL_S)


async def _thousand_awaits() -> Report:
    # An IP address in the URL: a host name lookup would start a thread of
    # the runtime's blocking pool, which is not a thread of the call bridge.
    handle = _handle(address_url(database_url()))
    threads_before_start = os_thread_count()
    first = await handle.ping_database_async()
    threads_after_start = os_thread_count()
    futures = [handle.ping_database_async() for _ in range(CONCURRENT_AWAITS)]
    samples = [os_thread_count()]
    sampler = asyncio.create_task(_sample_threads(samples))
    outcomes = await asyncio.gather(*futures)
    _ = sampler.cancel()
    _ = await asyncio.gather(sampler, return_exceptions=True)
    samples.append(os_thread_count())
    names = Counter(type(outcome).__name__ for outcome in outcomes)
    outcome_counts: dict[str, JsonValue] = {name: count for name, count in names.items()}
    return {
        'first': type(first).__name__,
        'threads_before_start': threads_before_start,
        'threads_after_start': threads_after_start,
        'samples': len(samples),
        'max_threads_in_flight': max(samples),
        'min_threads_in_flight': min(samples),
        'outcomes': outcome_counts,
    }


def thousand_awaits_asyncio() -> Report:
    """1,000 concurrent ``ping_database_async`` awaits on an asyncio loop."""
    return _run_on('asyncio', _thousand_awaits)


def thousand_awaits_uvloop() -> Report:
    """1,000 concurrent ``ping_database_async`` awaits on a uvloop loop."""
    return _run_on('uvloop', _thousand_awaits)


async def _idle_producer_after_first_send() -> Report:
    # An IP address in the URL, as in the thousand awaits case.
    url = address_url(database_url())
    handle = _handle(url)
    task_id = minted_task_id()
    threads_before_send = os_thread_count()
    sent = await handle.enqueue_async(send_request(task_id))
    threads_after_send = os_thread_count()
    await asyncio.sleep(PRODUCER_IDLE_S)
    threads_idle = os_thread_count()
    delete_tasks(url, [task_id])
    return {
        'sent': type(sent).__name__,
        'threads_before_send': threads_before_send,
        'threads_after_send': threads_after_send,
        'threads_idle': threads_idle,
    }


def idle_producer_after_first_send() -> Report:
    """The OS thread count of a producer process before its first
    ``enqueue_async``, after it, and after ``PRODUCER_IDLE_S`` with no call,
    on an asyncio loop. The first send starts the core."""
    return _run_on('asyncio', _idle_producer_after_first_send)


async def _cancel_when_waiting(
    lock: TasksTableLock, start: Callable[[], asyncio.Future[Any]],
) -> Report:
    """Start a call, wait until a new backend waits for the held lock, and
    cancel the call's future."""
    known = lock.waiting_backends()
    future = start()
    waiter = await wait_for_new_waiter_async(lock, known)
    cancel_accepted = future.cancel()
    # The done callbacks of the future run on the next turn of the loop.
    await asyncio.sleep(0)
    return {
        'waiter': waiter_report(waiter),
        'cancel_accepted': cancel_accepted,
        'cancelled': future.cancelled(),
    }


async def _cancel_leaves_calls() -> Report:
    url = database_url()
    bridge = collect_records('horsies.bridge', logging.DEBUG)
    read_handle = _handle(url)
    write_handle = _handle(url)
    warm_up_id = minted_task_id()
    warm_up_ping = await read_handle.ping_database_async()
    warm_up_send = await write_handle.enqueue_async(send_request(warm_up_id))
    lock = TasksTableLock(url)
    lock.take()
    read_id: uuid.UUID = minted_task_id()
    read = await _cancel_when_waiting(
        lock,
        lambda: read_handle.task_info_async(
            read_id,
            include_result=False,
            include_failed_reason=False,
            include_attempts=False,
        ),
    )
    write_id = minted_task_id()
    write = await _cancel_when_waiting(
        lock, lambda: write_handle.enqueue_async(send_request(write_id)),
    )
    lock.release()
    write_ended = await wait_until_async(
        lambda: task_row_exists(url, write_id) and WRITE_DISCARDED_MESSAGE in bridge.messages(),
        RECORD_WAIT_BOUND_S,
    )
    await asyncio.sleep(SETTLE_S)
    messages: list[JsonValue] = [message for message in bridge.messages()]
    ping_after = await read_handle.ping_database_async()
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


def cancel_leaves_calls() -> Report:
    """A read, then a write, each cancelled while it waits for the held lock.
    Then the lock is released: the write runs to its end and its discarded
    outcome is logged; the read was dropped and logs nothing. Each call has
    its own handle, so the write does not reuse the connection of the
    dropped read."""
    return _run_on('asyncio', _cancel_leaves_calls)


if __name__ == '__main__':
    run_case(
        {
            'future_on_calling_loop_asyncio': future_on_calling_loop_asyncio,
            'future_on_calling_loop_uvloop': future_on_calling_loop_uvloop,
            'thousand_awaits_asyncio': thousand_awaits_asyncio,
            'thousand_awaits_uvloop': thousand_awaits_uvloop,
            'idle_producer_after_first_send': idle_producer_after_first_send,
            'cancel_leaves_calls': cancel_leaves_calls,
        },
    )
