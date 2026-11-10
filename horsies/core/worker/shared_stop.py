"""What the worker writes for each run of a shared async child that it kills.

The worker kills a shared async child for a named cause, or for none:

- ``StuckRunsKill``: every run left in the child is stuck. A stuck run did
  not stop after its second cancel and its cancel rounds; the child closed
  when the first one was reported, and the worker kills it only when no run
  in it has time left.
- ``BlockedLoopKill``: the watchdog of the child reported that the event loop
  has not turned for the block limit. No task of the child can move, so the
  kill comes at once.
- ``NoNamedKill``: a kill with no named cause: the backstop of a frozen child,
  the drain deadline of a stopping worker, a closed heartbeat registry.

The verdict of each run, at a kill whose exit the worker saw:

- a run past its own deadline: ``TASK_TIMEOUT``;
- the run that blocked the loop, before its deadline: ``TASK_BLOCKED_LOOP``;
- another run, before its deadline, when the loop was blocked and the
  watchdog named the run that blocked it: ``NEIGHBOUR_BLOCKED_LOOP``;
- any other run: the crash path (``WORKER_CRASHED``, or its retry).

A run that was reported stuck, and the run that blocked the loop, retry in an
isolated child when their policy retries them. A crash of the child that the
worker did not cause takes the crash path for every run, and isolates no
run. Pure.
"""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import assert_never


class RetryPlacement(Enum):
    """Where the retry of a failed attempt runs, when its policy retries it."""

    SAME_CLASS = 'same_class'
    """In the execution class that the row has."""

    ISOLATED = 'isolated'
    """In an isolated child: the row gets ``execution = 'ISOLATED'`` in the
    write that schedules the retry."""


@dataclass(frozen=True, slots=True)
class StuckRunsKill:
    """Every run left in the child was reported stuck."""


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


type ChildKill = StuckRunsKill | BlockedLoopKill | NoNamedKill


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


def verdict_at_kill(
    kill: ChildKill,
    dispatch: int,
    deadline: DeadlineState,
    stop: StopState,
) -> KilledRunVerdict | CrashPath:
    """The verdict of run ``dispatch`` at a kill of its child by the worker,
    whose exit was seen. Pure."""
    match kill:
        case BlockedLoopKill(holder=LoopHeldBy(dispatch=holder)) if holder == dispatch:
            return _verdict_of_holder(kill, deadline)
        case BlockedLoopKill() | StuckRunsKill() | NoNamedKill():
            pass
        case _ as unreachable:
            assert_never(unreachable)
    match deadline, kill:
        case DeadlineState.PASSED, _:
            return TimedOutAtKill(retry=retry_placement(stop))
        case (
            DeadlineState.PENDING,
            BlockedLoopKill(holder=LoopHeldBy(task_id=holder_task_id)),
        ):
            return BesideBlockedLoop(
                holder_task_id=holder_task_id,
                blocked_ms=kill.blocked_ms,
            )
        case (
            DeadlineState.PENDING,
            BlockedLoopKill()
            | StuckRunsKill()
            | NoNamedKill(),
        ):
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
