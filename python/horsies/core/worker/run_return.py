"""What one run of a task returned, and whether it set its row RUNNING.

Every kind of child uses these: an isolated child (``child_runner``), a seat
of a shared async child, and a seat of a shared thread child. The end of a
run writes the finalizing marker only after a ``SetRunning`` run, and only
when the run has ended, through the core handle of the child
(``child_runner.mark_run_finalizing``). This module is pure.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

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


type RunReturn = SetRunning | StoppedBeforeRunning
"""What the run of one task returned."""
