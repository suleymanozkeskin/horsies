"""Which worker child holds which task, for the runner heartbeat thread.

The slot pool writes this registry on its event loop:

- a child takes a task: ``hold``;
- the attempt ends, because the slot read the child's answer: ``release``;
- a kill goes to a child, or its exit is seen: ``release_child``.

The runner heartbeat thread reads ``snapshot`` and writes one heartbeat for
each held task. Its watchdog calls ``close`` when the worker fails closed.

One lock guards every read and every write. No caller holds it across I/O.
This module does no I/O.
"""

from __future__ import annotations

import threading
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import TypeAlias, assert_never

from horsies.core.worker.child_protocol import RunFrame


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

    ``capacity`` is the number of slots: each child holds at most one task, so
    the registry never holds more entries than that.
    """

    def __init__(self, capacity: int) -> None:
        assert capacity >= 1, f'a slot pool has at least one slot, got {capacity}'
        self._capacity = capacity
        self._lock = threading.Lock()
        self._by_ticket: dict[int, LiveDispatch] = {}
        self._next_ticket = 1
        self._state = _RegistryState.OPEN

    def hold(self, frame: RunFrame, pid: int) -> HoldOutcome:
        """Record that child ``pid`` took the task of ``frame``.

        Returns ``RegistryClosed`` after ``close``: the caller must then kill
        the child, because no heartbeat covers it. A child that already holds
        a task, or a registry at capacity, is a defect in the caller:
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
            assert all(
                held.pid != pid for held in self._by_ticket.values()
            ), f'child pid={pid} took task {frame.task_id} while it holds another'
            assert (
                len(self._by_ticket) < self._capacity
            ), f'more held tasks than the {self._capacity} slots'
            ticket = self._next_ticket
            self._next_ticket += 1
            self._by_ticket[ticket] = LiveDispatch(
                ticket=ticket,
                task_id=frame.task_id,
                claimed_at=frame.claimed_at,
                pid=pid,
            )
            return Held(ticket)

    def release(self, ticket: int) -> ReleaseOutcome:
        """Remove the dispatch with this ticket. The attempt has ended."""
        with self._lock:
            dispatch = self._by_ticket.pop(ticket, None)
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
        heartbeat covers these tasks after this call returns.
        """
        with self._lock:
            removed = tuple(
                dispatch for dispatch in self._by_ticket.values() if dispatch.pid == pid
            )
            for dispatch in removed:
                del self._by_ticket[dispatch.ticket]
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
        return held
