"""Which worker child holds which task, for the runner heartbeat thread.

The slot pool writes this registry on its event loop:

- a child starts: ``register_kill_mark``;
- a child takes a task: ``hold``. An isolated child holds one task, a
  shared async child up to its seats;
- the attempt ends, because the slot read the child's answer: ``release``;
- a kill goes to a child, or its exit is seen: ``release_child``.

The runner heartbeat thread reads ``snapshot`` and writes one heartbeat for
each held task. Its watchdog calls ``close`` when the worker fails closed,
and ``run_kill_mark`` before it kills each child that held a task.

One lock guards every read and every write. No caller holds it across I/O.
This module does no I/O.
"""

from __future__ import annotations

import threading
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import TypeAlias, assert_never

from horsies.core.worker.child_protocol import RunFrame, SharedRunFrame


@dataclass(frozen=True, slots=True)
class LiveDispatch:
    """One task that a live worker child holds.

    ``claimed_at`` is the claim generation of the dispatch. None disables the
    generation half of the heartbeat fence, as in ``OwnedClaim``.
    """

    ticket: int
    task_id: str
    claimed_at: datetime | None
    pid: int


@dataclass(frozen=True, slots=True)
class Held:
    """The registry holds the dispatch under this ticket."""

    ticket: int


@dataclass(frozen=True, slots=True)
class RegistryClosed:
    """The worker failed closed. No child may hold a new task."""


HoldOutcome: TypeAlias = Held | RegistryClosed


@dataclass(frozen=True, slots=True)
class Released:
    """The dispatch left the registry."""

    dispatch: LiveDispatch


@dataclass(frozen=True, slots=True)
class NotHeld:
    """No dispatch has this ticket. A kill, an exit, or a close took it first."""


ReleaseOutcome: TypeAlias = Released | NotHeld


class _RegistryState(Enum):
    OPEN = 'open'
    CLOSED = 'closed'


class LiveDispatches:
    """The tasks that live children hold. Safe to share between threads.

    ``capacity`` is the number of seats of the pool: one per isolated slot,
    and the seats of each shared async slot. A child holds at most its own
    seats, so the registry never holds more entries than that.
    """

    def __init__(self, capacity: int) -> None:
        assert capacity >= 1, f'a slot pool has at least one seat, got {capacity}'
        self._capacity = capacity
        self._lock = threading.Lock()
        self._by_ticket: dict[int, LiveDispatch] = {}
        self._held_by_pid: dict[int, int] = {}
        """The entries of ``_by_ticket`` per child pid, written with it under
        the lock. ``hold`` checks the seats of a child with one read, not with
        a scan of every held task."""
        self._kill_marks: dict[int, Callable[[], None]] = {}
        self._next_ticket = 1
        self._state = _RegistryState.OPEN

    def register_kill_mark(self, pid: int, mark: Callable[[], None]) -> None:
        """Register ``mark`` for child ``pid``, which has started.

        ``run_kill_mark`` runs it. ``release_child`` drops it, at a kill by
        the slot pool or at the exit of the child.
        """
        with self._lock:
            self._kill_marks[pid] = mark

    def run_kill_mark(self, pid: int) -> None:
        """Run the mark of child ``pid``: the heartbeat watchdog kills it next.

        The mark records the kill as one by the worker, as a kill by the slot
        pool is. It runs outside the lock, on the caller's thread. A child
        whose mark was dropped already has a kill or an exit of its own, and
        nothing runs.
        """
        with self._lock:
            mark = self._kill_marks.get(pid)
        if mark is not None:
            mark()

    def hold(
        self,
        frame: RunFrame | SharedRunFrame,
        pid: int,
        *,
        seats: int,
    ) -> HoldOutcome:
        """Record that child ``pid``, which has ``seats`` seats, took the task
        of ``frame``.

        Returns ``RegistryClosed`` after ``close``: the caller must then kill
        the child, because no heartbeat covers it. A child that already holds
        ``seats`` tasks, or a registry at capacity, is a defect in the caller:
        ``AssertionError``.
        """
        with self._lock:
            match self._state:
                case _RegistryState.CLOSED:
                    return RegistryClosed()
                case _RegistryState.OPEN:
                    pass
                case _ as unreachable:
                    assert_never(unreachable)
            held_by_child = self._held_by_pid.get(pid, 0)
            assert held_by_child < seats, (
                f'child pid={pid} took task {frame.task_id} while it holds '
                f'{held_by_child} of its {seats} seats'
            )
            assert (
                len(self._by_ticket) < self._capacity
            ), f'more held tasks than the {self._capacity} seats'
            ticket = self._next_ticket
            self._next_ticket += 1
            self._by_ticket[ticket] = LiveDispatch(
                ticket=ticket,
                task_id=frame.task_id,
                claimed_at=frame.claimed_at,
                pid=pid,
            )
            self._held_by_pid[pid] = held_by_child + 1
            return Held(ticket)

    def release(self, ticket: int) -> ReleaseOutcome:
        """Remove the dispatch with this ticket. The attempt has ended."""
        with self._lock:
            dispatch = self._by_ticket.pop(ticket, None)
            if dispatch is not None:
                self._count_one_less(dispatch.pid)
        match dispatch:
            case None:
                return NotHeld()
            case LiveDispatch():
                return Released(dispatch)
            case _ as unreachable:
                assert_never(unreachable)

    def release_child(self, pid: int) -> tuple[LiveDispatch, ...]:
        """Remove every dispatch that child ``pid`` holds, and return them.

        Called when a kill goes to the child and when its exit is seen. No
        heartbeat covers these tasks after this call returns. Drops the kill
        mark of the child.
        """
        with self._lock:
            removed = tuple(
                dispatch for dispatch in self._by_ticket.values() if dispatch.pid == pid
            )
            for dispatch in removed:
                del self._by_ticket[dispatch.ticket]
            counted = self._held_by_pid.pop(pid, 0)
            assert counted == len(removed), (
                f'child pid={pid} was counted with {counted} held tasks, '
                f'the registry held {len(removed)}'
            )
            _ = self._kill_marks.pop(pid, None)
        return removed

    def snapshot(self) -> tuple[LiveDispatch, ...]:
        """The dispatches held now, in ticket order. Empty after ``close``."""
        with self._lock:
            return tuple(self._by_ticket[ticket] for ticket in sorted(self._by_ticket))

    def close(self) -> tuple[LiveDispatch, ...]:
        """Refuse every later ``hold``, and return and remove what is held.

        The caller kills the children of the returned dispatches. A second
        call returns an empty tuple.
        """
        with self._lock:
            self._state = _RegistryState.CLOSED
            held = tuple(self._by_ticket[ticket] for ticket in sorted(self._by_ticket))
            self._by_ticket.clear()
            self._held_by_pid.clear()
        return held

    def _count_one_less(self, pid: int) -> None:
        """Count one held task less for child ``pid``. The caller holds the
        lock. A count that is not positive here means ``_held_by_pid`` and
        ``_by_ticket`` disagree: a defect, ``AssertionError``."""
        counted = self._held_by_pid.get(pid, 0)
        assert counted > 0, (
            f'child pid={pid} released a task while it was counted with '
            f'{counted} held tasks'
        )
        if counted == 1:
            del self._held_by_pid[pid]
        else:
            self._held_by_pid[pid] = counted - 1
