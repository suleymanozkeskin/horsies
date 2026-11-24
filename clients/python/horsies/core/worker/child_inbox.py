"""The frames that the control thread of a child hands to its main thread.

This module is never the program of a process. The child program
(``child_main``) runs as ``__main__``, and the shared async seat loop imports
``child_main`` again under its package name: a class defined in
``child_main`` would then exist twice, and a ``match`` on the one copy would
not match a value of the other. Both import these types from here.
"""

from __future__ import annotations

import queue
import threading
from collections.abc import Callable
from dataclasses import dataclass
from typing import assert_never

from horsies._core import CancelRunFrame
from horsies.core.worker.child_frames import WorkFrame


@dataclass(frozen=True, slots=True)
class NoFrameYet:
    """The inbox holds no frame now."""


@dataclass(frozen=True, slots=True)
class _QueueCancels:
    """A cancel frame goes in the queue, as every other frame: the serve
    loop of a shared child reads it between its runs."""


@dataclass(frozen=True, slots=True)
class _RouteCancels:
    """A cancel frame goes to ``on_cancel`` on the control thread: the main
    thread of an isolated child runs the task and reads no frame then."""

    on_cancel: Callable[[CancelRunFrame], None]


def _nothing_to_wake() -> None:
    return None


class ParentFrameInbox:
    """Frames from the control thread to the main thread. Safe between threads.

    The isolated main thread waits in ``get``. A shared async child reads with
    ``take`` on its event loop, and ``call_on_put`` makes each later ``put``
    wake that loop. Holds at most the frames the parent sent and the main
    thread has not read: the parent sends a run only to a free seat.
    """

    def __init__(self) -> None:
        self._frames: queue.SimpleQueue[WorkFrame] = queue.SimpleQueue()
        self._lock = threading.Lock()
        self._on_put: Callable[[], None] = _nothing_to_wake
        self._cancels: _QueueCancels | _RouteCancels = _QueueCancels()

    def put(self, frame: WorkFrame) -> None:
        """Control thread: hand over one frame, then run the wake callback."""
        self._frames.put(frame)
        with self._lock:
            on_put = self._on_put
        on_put()

    def put_cancel(self, frame: CancelRunFrame) -> None:
        """Control thread: hand over one cancel frame. It goes to the route
        that ``route_cancels`` set, on this thread; with none, it goes in
        the queue as ``put`` does."""
        with self._lock:
            cancels = self._cancels
        match cancels:
            case _QueueCancels():
                self.put(frame)
            case _RouteCancels(on_cancel=on_cancel):
                on_cancel(frame)
            case _ as unreachable:
                assert_never(unreachable)

    def route_cancels(self, on_cancel: Callable[[CancelRunFrame], None]) -> None:
        """Run ``on_cancel`` for each later cancel frame, on the control
        thread, in place of the queue. An isolated child sets it before its
        ready frame: its main thread reads no frame while a task runs."""
        with self._lock:
            self._cancels = _RouteCancels(on_cancel=on_cancel)

    def get(self) -> WorkFrame:
        """Wait for the next frame. Blocks the calling thread."""
        return self._frames.get()

    def take(self) -> WorkFrame | NoFrameYet:
        """The next frame, or ``NoFrameYet`` at once. Never blocks."""
        try:
            return self._frames.get_nowait()
        except queue.Empty:
            return NoFrameYet()

    def call_on_put(self, on_put: Callable[[], None]) -> None:
        """Run ``on_put`` after each later ``put``, on the control thread."""
        with self._lock:
            self._on_put = on_put
