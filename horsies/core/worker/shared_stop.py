"""What the worker writes for each run of a shared child that it kills.

A shared child that has no task with time left is killed by the close and
kill path of the slot pool (``ChildKilledPastDeadlines``): every task of a
thread child is past its deadline, every task of an async child is stuck.
Its culprit retries isolated (``culprit_retry``). The worker also kills a
shared child for these causes:

- ``BlockedLoopKill``: the watchdog of an async child reported that the
  event loop has not turned for the block limit. No task of the child can
  move, so the kill comes at once.
- ``NoNamedKill``: a kill with no named cause: the backstop of a frozen child,
  the drain deadline of a stopping worker, a closed heartbeat registry.

The verdict of each run at such a kill, whose exit the worker saw
(``verdict_at_kill``):

- the run that blocked the loop: ``TASK_BLOCKED_LOOP`` before its deadline,
  ``TASK_TIMEOUT`` past it; its retry runs in an isolated child;
- another run past its own deadline: ``TASK_TIMEOUT``;
- another run before its deadline, when the loop was blocked and the
  watchdog named the run that blocked it: ``NEIGHBOUR_BLOCKED_LOOP``;
- any other run: the crash path (``WORKER_CRASHED``, or its retry).

A kill with no named cause isolates no run, and neither does a crash of the
child that the worker did not cause. Pure.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import assert_never

from horsies.core.worker.thread_slots import RunCulprit


class RetryPlacement(Enum):
    """Where the retry of a failed attempt runs, when its policy retries it."""

    SAME_CLASS = 'same_class'
    """In the execution class that the row has."""

    ISOLATED = 'isolated'
    """In an isolated child: the row gets ``execution = 'ISOLATED'`` in the
    write that schedules the retry."""


@dataclass(frozen=True, slots=True)
class LoopHeldBy:
    """The run ``dispatch`` (task ``task_id``) held the blocked loop."""

    dispatch: int
    task_id: str


@dataclass(frozen=True, slots=True)
class LoopHolderUnknown:
    """The blocked loop ran code of no run."""


type LoopHolder = LoopHeldBy | LoopHolderUnknown


@dataclass(frozen=True, slots=True)
class BlockedLoopKill:
    """The loop of the child did not turn for ``blocked_ms`` milliseconds.
    ``where`` names the source line that it ran."""

    holder: LoopHolder
    blocked_ms: int
    where: str


@dataclass(frozen=True, slots=True)
class NoNamedKill:
    """No kill with a named cause was sent to this child."""


type ChildKill = BlockedLoopKill | NoNamedKill


class DeadlineState(Enum):
    """Whether the deadline of a run passed at the kill."""

    PENDING = 'pending'
    PASSED = 'passed'


class StopState(Enum):
    """Whether the child reported a run as stuck."""

    NOT_STUCK = 'not_stuck'
    STUCK = 'stuck'


@dataclass(frozen=True, slots=True)
class TimedOutAtKill:
    """``TASK_TIMEOUT``. ``retry`` is where its retry runs."""

    retry: RetryPlacement


@dataclass(frozen=True, slots=True)
class BlockedTheLoop:
    """``TASK_BLOCKED_LOOP``: this run held the loop past the block limit,
    before its deadline. Its retry runs isolated."""

    blocked_ms: int
    where: str


@dataclass(frozen=True, slots=True)
class BesideBlockedLoop:
    """``NEIGHBOUR_BLOCKED_LOOP``: run ``holder_task_id`` held the loop of
    this run's child past the block limit, before this run's deadline."""

    holder_task_id: str
    blocked_ms: int


type KilledRunVerdict = TimedOutAtKill | BlockedTheLoop | BesideBlockedLoop


@dataclass(frozen=True, slots=True)
class CrashPath:
    """The run takes the crash path: ``WORKER_CRASHED``, or its retry."""


def retry_placement(stop: StopState) -> RetryPlacement:
    """A stuck run retries isolated; another run in its class."""
    match stop:
        case StopState.STUCK:
            return RetryPlacement.ISOLATED
        case StopState.NOT_STUCK:
            return RetryPlacement.SAME_CLASS
        case _ as unreachable:
            assert_never(unreachable)


def culprit_retry(culprit: RunCulprit, dispatch: int) -> RetryPlacement:
    """The retry of run ``dispatch`` of a child killed when no task in it had
    time left: the culprit retries isolated, every other run in its class."""
    if culprit.dispatch == dispatch:
        return RetryPlacement.ISOLATED
    return RetryPlacement.SAME_CLASS


def verdict_at_kill(
    kill: ChildKill,
    dispatch: int,
    deadline: DeadlineState,
) -> KilledRunVerdict | CrashPath:
    """The verdict of run ``dispatch`` at a kill of its child by the worker,
    whose exit was seen. Pure."""
    match kill:
        case BlockedLoopKill(holder=LoopHeldBy(dispatch=holder)) if holder == dispatch:
            return _verdict_of_holder(kill, deadline)
        case BlockedLoopKill() | NoNamedKill():
            pass
        case _ as unreachable:
            assert_never(unreachable)
    match deadline, kill:
        case DeadlineState.PASSED, _:
            return TimedOutAtKill(retry=RetryPlacement.SAME_CLASS)
        case (
            DeadlineState.PENDING,
            BlockedLoopKill(holder=LoopHeldBy(task_id=holder_task_id)),
        ):
            return BesideBlockedLoop(
                holder_task_id=holder_task_id,
                blocked_ms=kill.blocked_ms,
            )
        case DeadlineState.PENDING, BlockedLoopKill() | NoNamedKill():
            return CrashPath()
        case _ as unreachable:
            assert_never(unreachable)


def _verdict_of_holder(
    kill: BlockedLoopKill,
    deadline: DeadlineState,
) -> KilledRunVerdict:
    """The run that held the blocked loop retries isolated."""
    match deadline:
        case DeadlineState.PASSED:
            return TimedOutAtKill(retry=RetryPlacement.ISOLATED)
        case DeadlineState.PENDING:
            return BlockedTheLoop(blocked_ms=kill.blocked_ms, where=kill.where)
        case _ as unreachable:
            assert_never(unreachable)
