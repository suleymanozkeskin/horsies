# app/core/utils/looprunner.py
from __future__ import annotations
import asyncio
import atexit
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
        self._loop = asyncio.new_event_loop()
        self._thread = threading.Thread(
            target=self._loop.run_forever, name='horsies-loop', daemon=True
        )
        self._started = False

    def start(self) -> None:
        if not self._started:
            try:
                self._thread.start()
            except Exception as exc:
                raise LoopRunnerError(
                    f'Failed to start loop runner thread: {type(exc).__name__}: {exc}',
                ) from exc
            self._started = True

    def stop(self) -> None:
        if self._started:
            self._loop.call_soon_threadsafe(self._loop.stop)
            self._thread.join(timeout=2)
            self._started = False

    def call(
        self, coro_fn: Callable[..., Awaitable[Any]], *args: Any, **kwargs: Any
    ) -> Any:
        """Run an async function and block until it completes, from sync code."""
        if not self._started:
            self.start()
        self.logger.debug(
            f'Calling {coro_fn.__name__} with args: {args} and kwargs: {kwargs}'
        )
        try:
            fut: Future[Any] = asyncio.run_coroutine_threadsafe(
                coro_fn(*args, **kwargs), self._loop
            )
        except Exception as exc:
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
