# app/core/utils/looprunner.py
from __future__ import annotations
import asyncio
import atexit
import contextlib
import threading
from concurrent.futures import Future
from typing import Any, Awaitable, Callable
from horsies.core.logging import get_logger


class LoopRunnerError(RuntimeError):
    """Infrastructure failure in the sync->async bridge."""


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

    def stop(self) -> None:
        with self._state_lock:
            if self._started and self._loop is not None:
                self._loop.call_soon_threadsafe(self._loop.stop)
                if self._thread is not None:
                    self._thread.join(timeout=2)
                    if self._thread.is_alive():
                        self.logger.warning(
                            'Loop runner thread did not stop within timeout; leaving loop open'
                        )
                        return
                self._loop.close()
                self._loop = None
                self._thread = None
                self._started = False
            self._closed = True

    def call(
        self, coro_fn: Callable[..., Awaitable[Any]], *args: Any, **kwargs: Any
    ) -> Any:
        """Run an async function and block until it completes, from sync code."""
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


_shared_runner: LoopRunner | None = None
_shared_lock = threading.Lock()


def _shutdown_shared_runner() -> None:
    global _shared_runner
    if _shared_runner is not None:
        _shared_runner.stop()
        _shared_runner = None


def get_shared_runner() -> LoopRunner:
    """Return a lazily-created, process-wide LoopRunner."""
    global _shared_runner
    if _shared_runner is not None:
        return _shared_runner
    with _shared_lock:
        if _shared_runner is not None:
            return _shared_runner
        runner = LoopRunner()
        runner.start()
        atexit.register(_shutdown_shared_runner)
        _shared_runner = runner
        return runner
