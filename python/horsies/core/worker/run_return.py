"""What one run of a task returned, and whether it set its row RUNNING.

Every kind of child uses these: an isolated child (``child_runner``), a seat
of a shared async child, and a seat of a shared thread child. The end of a
run writes the finalizing marker only after a ``SetRunning`` run, and only
when the run has ended, through the core handle of the child
(``child_runner.mark_run_finalizing``). This module is pure.

A child whose core stopped ends itself, at the first core call that meets
the stop:

- At the start of a run: ``CoreStoppedBeforeStart``. The child reports it as
  ``OWNERSHIP_UNCONFIRMED`` and then ends itself.
- In the body: a body that the refusal ended (``BodyInterrupted``) has no
  result. The child ends itself at once and reports nothing.
- At the finalizing marker after a body that finished
  (``MarkerMetStoppedCore``): the run reports its result
  (``CoreStoppedAfterBody``), and then the child ends itself.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal, assert_never

from horsies._core import CoreStopped, ForkedAfterCoreStart

type StopReason = Literal[
    'CLAIM_LOST',
    'OWNERSHIP_UNCONFIRMED',
    'WORKFLOW_STOPPED',
    'TASK_EXPIRED',
    'NODE_NOT_LINKED',
    'ORPHAN_CANCELLED',
]
"""Why a run stopped before it set its row RUNNING, as the frame names it:

- ``CLAIM_LOST``: the claim is gone (another owner, generation or status,
  or a lapsed lease).
- ``OWNERSHIP_UNCONFIRMED``: the start failed or did not answer; the worker
  releases the claim.
- ``WORKFLOW_STOPPED``: the workflow of the task is paused or cancelled.
- ``TASK_EXPIRED``: the deadline of the task passed before the start.
- ``NODE_NOT_LINKED``: no node links the task, and the orphan self-heal is
  off; the row stays CLAIMED.
- ``ORPHAN_CANCELLED``: no node links the task, and the stop cancelled it.
"""


@dataclass(frozen=True, slots=True)
class ResultEnvelope:
    """The run produced its result envelope: the task's value or its task
    error, as the JSON text that the worker stores."""

    text: str


@dataclass(frozen=True, slots=True)
class SetRunning:
    """The run set its row RUNNING, then ended with ``result``: the body ran,
    or a refusal or a resolve error ended it after the RUNNING update. The
    end of the run writes the finalizing marker when one is due."""

    result: ResultEnvelope


@dataclass(frozen=True, slots=True)
class StoppedBeforeRunning:
    """The run ended before it set its row RUNNING, for ``reason``. No task
    code ran, and no marker follows."""

    reason: StopReason


@dataclass(frozen=True, slots=True)
class CoreStoppedBeforeStart:
    """The core of this child stopped before the run set its row RUNNING:
    the start raised ``CoreStopped``, the error of a panic in a core
    background task. No task code ran, and no marker follows.

    The frame reports the stop as ``OWNERSHIP_UNCONFIRMED``, so the worker
    releases the claim with no attempt charged. Once that frame is written,
    the child ends itself with ``CORE_STOPPED_EXIT_CODE``
    (``child_main.end_child_after_core_refusal``), and the worker starts a
    new child."""

    stopped: CoreStopped


type RunReturn = SetRunning | StoppedBeforeRunning | CoreStoppedBeforeStart
"""What the run of one task returned."""


type CoreRefusal = CoreStopped | ForkedAfterCoreStart
"""A refusal of the core of this process that no later call clears: the
core stopped after a panic in one of its background tasks, or this process
is a fork of the process where it started."""


@dataclass(frozen=True, slots=True)
class BodyInterrupted:
    """A refusal of the core of this child ended the body of the run after
    its RUNNING update: the task error of the body holds ``refusal`` as its
    exception, because a core call of the body raised it and the body let
    it out, or returned it as its error. The body has no result of its own.

    The child ends itself at once with ``CORE_STOPPED_EXIT_CODE`` and
    reports nothing, so the worker takes the crashed-child path
    (``WORKER_CRASHED``) and the retry policy of the task applies."""

    refusal: CoreRefusal


@dataclass(frozen=True, slots=True)
class MarkerDone:
    """The finalizing marker of a run was written, or the child logged why
    it was skipped or failed. The child serves on."""


@dataclass(frozen=True, slots=True)
class MarkerMetStoppedCore:
    """The finalizing marker after a body that finished raised
    ``CoreStopped``: the core of the child stopped during the body or after
    it. The run reports its result (``CoreStoppedAfterBody``), and then the
    child ends itself."""

    stopped: CoreStopped


type MarkerEnd = MarkerDone | MarkerMetStoppedCore
"""How the finalizing marker of a run ended. A fork of a child
(``ForkedAfterCoreStart``) does not return from the marker: it ends at once
and writes no frame."""


@dataclass(frozen=True, slots=True)
class CoreStoppedAfterBody:
    """The body of the run finished with ``result``, and the finalizing
    marker after it met the stopped core of the child (``stopped``).

    The frame reports ``result`` as for any run, and the worker writes it.
    Once that frame is written, the child ends itself with
    ``CORE_STOPPED_EXIT_CODE``, and the worker starts a new child."""

    result: ResultEnvelope
    stopped: CoreStopped


def end_after_marker(
    result: ResultEnvelope,
    marker: MarkerEnd,
) -> ResultEnvelope | CoreStoppedAfterBody:
    """The end that a run whose body finished with ``result`` reports after
    its finalizing marker ended with ``marker``. Pure."""
    match marker:
        case MarkerDone():
            return result
        case MarkerMetStoppedCore(stopped=stopped):
            return CoreStoppedAfterBody(result=result, stopped=stopped)
        case _ as unreachable:
            assert_never(unreachable)
