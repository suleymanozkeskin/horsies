"""The ``ASYNC_CONTEXT`` refusal of the sync facades.

A sync facade calls a blocking form of the core on the calling thread. On a
thread that runs an event loop, that wait stalls every coroutine on the
loop, so the sync facades refuse there with ``ASYNC_CONTEXT``.
"""

from __future__ import annotations

import asyncio


def in_running_event_loop() -> bool:
    """True when called from a thread with a running asyncio event loop.

    A sync facade blocks its calling thread until the core completes the
    call. On a thread that runs an event loop, that wait stalls every
    coroutine on the loop, so the sync facades fail closed with
    ``ASYNC_CONTEXT`` there (the ``*_async`` variants are the correct call).
    A loop that exists but does not run does not count.
    """
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return False
    return True


def async_context_message(sync_call: str, async_call: str) -> str:
    """The ``ASYNC_CONTEXT`` message: what was refused, why, and what to call."""
    return (
        f'sync {sync_call} was called inside a running event loop; its '
        f'blocking wait would stall the loop, so nothing ran. Use '
        f'`await {async_call}` instead.'
    )
