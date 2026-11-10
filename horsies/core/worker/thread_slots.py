"""Parent-side values of shared thread children.

A shared thread child runs ``SHARED_THREAD`` tasks on seat threads. A thread
cannot be cancelled, so the first deadline that the child reports closes it:
it takes no new task (``ClosedAtDeadline``). The worker kills it once every
task in it is past its own deadline, and each of those tasks ends with the
timeout. The run whose deadline closed the child is the culprit
(``RunCulprit``).

This module is pure. The slot pool (``slots``) holds the states and acts.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import assert_never

from horsies.core.models.tasks import MAX_TASK_TIMEOUT_MS, MIN_TASK_TIMEOUT_MS
from horsies.core.worker.child_protocol import (
    SeatRefusal,
    SharedRunFrame,
    ThreadSeatTask,
    ThreadSeatWork,
)


@dataclass(frozen=True, slots=True)
class RunCulprit:
    """The run whose deadline closed its shared child: its dispatch number
    in the pool and its task id."""

    dispatch: int
    task_id: str


@dataclass(frozen=True, slots=True)
class ClosedAtDeadline:
    """Why a shared thread child takes no new task: the deadline of
    ``culprit`` passed, and its thread cannot be cancelled."""

    culprit: RunCulprit


@dataclass(frozen=True, slots=True)
class ThreadRunRequest:
    """A ``SHARED_THREAD`` task to run, before the pool numbers its run.

    The ``timeout_ms`` of a ``ThreadSeatTask`` is within the bounds of
    ``SharedRunFrame``.
    """

    task_id: str
    task_name: str
    args_json: str | None
    kwargs_json: str | None
    worker_id: str
    is_workflow_task: bool
    claimed_at: datetime | None
    work: ThreadSeatWork

    def __post_init__(self) -> None:
        match self.work:
            case ThreadSeatTask(timeout_ms=timeout_ms):
                assert (
                    MIN_TASK_TIMEOUT_MS <= timeout_ms <= MAX_TASK_TIMEOUT_MS
                ), f'timeout_ms {timeout_ms} of task {self.task_id} is out of bounds'
            case SeatRefusal():
                pass
            case _ as unreachable:
                assert_never(unreachable)

    def frame_for(self, dispatch: int) -> SharedRunFrame:
        """The run frame of run ``dispatch``."""
        return SharedRunFrame(
            dispatch=dispatch,
            task_id=self.task_id,
            task_name=self.task_name,
            args_json=self.args_json,
            kwargs_json=self.kwargs_json,
            worker_id=self.worker_id,
            is_workflow_task=self.is_workflow_task,
            claimed_at=self.claimed_at,
            work=self.work,
        )
