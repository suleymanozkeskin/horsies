"""What one run of a task returned, and whether it set its row RUNNING.

Every kind of child uses these: an isolated child (``child_runner``), a seat
of a shared async child, and a seat of a shared thread child. The end of a
run writes the finalizing marker only after a ``SetRunning`` run, and only
when the run has ended. This module is pure.
"""

from __future__ import annotations

from dataclasses import dataclass

from horsies.core.worker.child_protocol import WireResult


@dataclass(frozen=True, slots=True)
class SetRunning:
    """The run set its row RUNNING, then ended with ``wire``: the body ran,
    or a refusal or a resolve error ended it after the RUNNING update. The
    end of the run writes the finalizing marker when one is due."""

    wire: WireResult


@dataclass(frozen=True, slots=True)
class StoppedBeforeRunning:
    """The run ended with ``wire`` before it set its row RUNNING: a lost
    claim, an expiry, a stopped workflow, or a failed RUNNING update. No
    marker follows."""

    wire: WireResult


type RunReturn = SetRunning | StoppedBeforeRunning
"""What the run of one task returned."""


@dataclass(frozen=True, slots=True)
class MarkerAtReturn:
    """The isolated run left no asyncio task on its task loop: its marker
    follows the return at once, with the bounds of the pool. The deadline of
    the task bounds the run until its result frame."""


@dataclass(frozen=True, slots=True)
class MarkerAfterLeftWork:
    """The work that the isolated run left on its task loop ended: its
    marker follows that end. The deadline of the task no longer applies, so
    the marker has bounds of its own
    (``child_runner.LEFT_WORK_MARKER_BOUND_S``), within the backstop of the
    parent."""


type IsolatedMarkerWindow = MarkerAtReturn | MarkerAfterLeftWork
"""When the finalizing marker of an isolated run is written, and so its
bounds."""
