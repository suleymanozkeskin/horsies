"""The wire tuple of a run, for tests that call ``_run_task_entry``."""

from __future__ import annotations

from typing import assert_never

from horsies.core.worker.child_protocol import WireResult
from horsies.core.worker.run_return import RunReturn, SetRunning, StoppedBeforeRunning


def wire_of(returned: RunReturn) -> WireResult:
    """The wire tuple of ``returned``, from either kind of end."""
    match returned:
        case SetRunning(wire=wire) | StoppedBeforeRunning(wire=wire):
            return wire
        case _ as unreachable:
            assert_never(unreachable)
