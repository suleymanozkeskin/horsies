# app/core/utils/looprunner.py
from __future__ import annotations
import asyncio
import contextlib
import threading
from concurrent.futures import Future
from typing import Any, Awaitable, Callable
from horsies.core.logging import get_logger


class LoopRunnerError(RuntimeError):
    """Infrastructure failure in the sync->async bridge."""


def in_running_event_loop() -> bool:
    """True when called from a thread with a running asyncio event loop.

    A sync facade blocks its calling thread until the broker's runner
    loop completes the call. On a thread that runs an event loop, that
    wait stalls every coroutine on the loop, so the sync facades fail
    closed with ``ASYNC_CONTEXT`` there (the ``*_async`` variants are the
    correct call). A loop that exists but does not run does not count.
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


class LoopRunner:
    """Run async callables from sync code on a dedicated event loop thread."""

    def __init__(self) -> None:
        self.logger = get_logger('loop_runner')
        self._loop: asyncio.AbstractEventLoop | None = None
        self._thread: threading.Thread | None = None
        self._started = False
        self._closed = False
        self._state_lock = threading.RLock()

    def _ensure_loop(self) -> asyncio.AbstractEventLoop:
        """Create the event loop and thread on first use."""
        with self._state_lock:
            if self._closed:
                raise LoopRunnerError(
                    'Loop runner has been stopped and cannot be restarted'
                )
            if self._loop is None:
                self._loop = asyncio.new_event_loop()
                self._thread = threading.Thread(
                    target=self._loop.run_forever, name='horsies-loop', daemon=True,
                )
            return self._loop

    def start(self) -> None:
        """Start the runner thread if not already running.

        Raises:
            LoopRunnerError: runner already stopped, or the thread/loop
                failed to start (state reset so retry is possible).
                Broker/handle/lifecycle sync facades fold this into Err.
        """
        with self._state_lock:
            if self._closed:
                raise LoopRunnerError(
                    'Loop runner has been stopped and cannot be restarted'
                )
            if self._started:
                return
            self._ensure_loop()
            assert self._thread is not None
            try:
                self._thread.start()
            except Exception as exc:
                self._loop = None
                self._thread = None
                raise LoopRunnerError(
                    f'Failed to start loop runner thread: {type(exc).__name__}: {exc}',
                ) from exc
            self._started = True

    def runs_on(self, loop: asyncio.AbstractEventLoop) -> bool:
        """Whether ``loop`` is this runner's event loop.

        Reads the loop reference without the state lock: ``stop()`` holds
        that lock while it joins the runner thread, so a coroutine on the
        runner thread that waited for it would delay the stop.
        """
        return self._loop is loop

    def stop(self) -> None:
        with self._state_lock:
            if self._started and self._loop is not None:
                self._loop.call_soon_threadsafe(self._loop.stop)
                if self._thread is not None:
                    self._thread.join(timeout=2)
                    if self._thread.is_alive():
                        self.logger.warning(
                            'Loop runner thread did not stop within timeout; leaving '
                            'loop open but marking runner stopped (not reusable)'
                        )
                        self._started = False
                        self._closed = True
                        return
                self._loop.close()
                self._loop = None
                self._thread = None
                self._started = False
            self._closed = True

    def call(
        self, coro_fn: Callable[..., Awaitable[Any]], *args: Any, **kwargs: Any
    ) -> Any:
        """Run an async function and block until it completes, from sync code.

        Raises:
            LoopRunnerError: runner stopped or scheduling onto the loop
                failed. Broker/handle/lifecycle sync facades fold this
                into Err.
            Exception: the awaited coroutine's own exception, re-raised
                verbatim by fut.result().
        """
        if not self._started:
            self.start()
        with self._state_lock:
            loop = self._loop
            if self._closed or not self._started or loop is None:
                raise LoopRunnerError('Loop runner is not running')
        self.logger.debug(
            f'Calling {coro_fn.__name__} with args: {args} and kwargs: {kwargs}'
        )
        coro: Awaitable[Any] | None = None
        try:
            coro = coro_fn(*args, **kwargs)
            fut: Future[Any] = asyncio.run_coroutine_threadsafe(coro, loop)
        except Exception as exc:
            # If scheduling fails, close the created coroutine to avoid
            # "coroutine was never awaited" RuntimeWarnings.
            if asyncio.iscoroutine(coro):
                with contextlib.suppress(RuntimeError):
                    coro.close()
            raise LoopRunnerError(
                f'Failed to schedule coroutine on loop runner: {type(exc).__name__}: {exc}',
            ) from exc
        return fut.result()  # propagate exceptions
