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
from typing import TypeAlias

from horsies.core.worker.child_protocol import RunFrame, SharedRunFrame, StopFrame

WorkFrame: TypeAlias = RunFrame | SharedRunFrame | StopFrame
"""The frames that the control thread hands to the main thread."""


@dataclass(frozen=True, slots=True)
class NoFrameYet:
    """The inbox holds no frame now."""


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

    def put(self, frame: WorkFrame) -> None:
        """Control thread: hand over one frame, then run the wake callback."""
        self._frames.put(frame)
        with self._lock:
            on_put = self._on_put
        on_put()

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
