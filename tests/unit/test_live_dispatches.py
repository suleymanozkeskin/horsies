"""The registry of tasks that worker children hold, read by the heartbeat thread."""

from __future__ import annotations

import dataclasses
import threading
from datetime import datetime, timezone

import pytest

from horsies.core.worker.child_protocol import RunFrame
from horsies.core.worker.live_dispatches import (
    Held,
    LiveDispatch,
    LiveDispatches,
    NotHeld,
    RegistryClosed,
    Released,
)
from horsies.core.worker.stored_timeout import NoTimeout

pytestmark = [pytest.mark.unit]

CLAIMED_AT = datetime(2026, 11, 10, 9, 30, 0, 125, tzinfo=timezone.utc)


def _frame(task_id: str, claimed_at: datetime | None = CLAIMED_AT) -> RunFrame:
    return RunFrame(
        task_id=task_id,
        task_name='registry_test',
        args_json=None,
        kwargs_json=None,
        worker_id='worker-1',
        is_workflow_task=False,
        timeout=NoTimeout(),
        claimed_at=claimed_at,
    )


def _held(outcome: Held | RegistryClosed) -> int:
    match outcome:
        case Held(ticket=ticket):
            return ticket
        case RegistryClosed():
            raise AssertionError('the registry is open')


def test_a_dispatch_carries_only_its_fence_and_its_child() -> None:
    """The beat fence needs the task, the claim generation, and the child."""
    assert {f.name for f in dataclasses.fields(LiveDispatch)} == {
        'ticket',
        'task_id',
        'claimed_at',
        'pid',
    }


def test_hold_records_the_task_its_generation_and_its_child() -> None:
    live = LiveDispatches(capacity=2)

    ticket = _held(live.hold(_frame('task-a'), pid=101, seats=1))

    assert live.snapshot() == (
        LiveDispatch(ticket=ticket, task_id='task-a', claimed_at=CLAIMED_AT, pid=101),
    )


def test_two_generations_of_one_task_are_two_dispatches() -> None:
    """A re-claimed row can be dispatched while the old child still holds it."""
    live = LiveDispatches(capacity=2)
    later = datetime(2026, 11, 10, 9, 31, tzinfo=timezone.utc)

    _held(live.hold(_frame('task-a'), pid=101, seats=1))
    _held(live.hold(_frame('task-a', claimed_at=later), pid=102, seats=1))

    assert [(d.pid, d.claimed_at) for d in live.snapshot()] == [
        (101, CLAIMED_AT),
        (102, later),
    ]


def test_release_removes_the_dispatch_once() -> None:
    live = LiveDispatches(capacity=1)
    ticket = _held(live.hold(_frame('task-a'), pid=101, seats=1))

    first = live.release(ticket)
    second = live.release(ticket)

    assert isinstance(first, Released) and first.dispatch.task_id == 'task-a'
    assert second == NotHeld()
    assert live.snapshot() == ()


def test_release_child_removes_only_that_childs_dispatch() -> None:
    live = LiveDispatches(capacity=2)
    _held(live.hold(_frame('task-a'), pid=101, seats=1))
    _held(live.hold(_frame('task-b'), pid=102, seats=1))

    removed = live.release_child(101)

    assert [d.task_id for d in removed] == ['task-a']
    assert [d.task_id for d in live.snapshot()] == ['task-b']
    assert live.release_child(101) == ()


def test_a_child_can_hold_its_next_task_after_a_release() -> None:
    live = LiveDispatches(capacity=1)
    ticket = _held(live.hold(_frame('task-a'), pid=101, seats=1))
    _ = live.release(ticket)

    second = _held(live.hold(_frame('task-b'), pid=101, seats=1))

    assert second != ticket
    assert [d.task_id for d in live.snapshot()] == ['task-b']


def test_run_kill_mark_runs_the_mark_of_that_child_only() -> None:
    live = LiveDispatches(capacity=2)
    marked: list[int] = []
    live.register_kill_mark(101, lambda: marked.append(101))
    live.register_kill_mark(102, lambda: marked.append(102))

    live.run_kill_mark(101)

    assert marked == [101]


def test_release_child_drops_the_kill_mark() -> None:
    """After a kill by the slot pool or an exit, the child has its own end."""
    live = LiveDispatches(capacity=1)
    marked: list[int] = []
    live.register_kill_mark(101, lambda: marked.append(101))

    _ = live.release_child(101)
    live.run_kill_mark(101)

    assert marked == []


def test_a_child_with_no_kill_mark_runs_nothing() -> None:
    LiveDispatches(capacity=1).run_kill_mark(101)


def test_close_returns_what_is_held_and_refuses_later_holds() -> None:
    live = LiveDispatches(capacity=2)
    _held(live.hold(_frame('task-a'), pid=101, seats=1))

    closed = live.close()

    assert [d.task_id for d in closed] == ['task-a']
    assert live.snapshot() == ()
    assert live.hold(_frame('task-b'), pid=102, seats=1) == RegistryClosed()
    assert live.close() == ()


def test_a_child_that_holds_a_task_cannot_take_another() -> None:
    live = LiveDispatches(capacity=2)
    _held(live.hold(_frame('task-a'), pid=101, seats=1))

    with pytest.raises(AssertionError, match='holds 1 of its 1 seats'):
        live.hold(_frame('task-b'), pid=101, seats=1)


def test_a_shared_child_holds_one_task_per_seat() -> None:
    live = LiveDispatches(capacity=3)
    for name in ('task-a', 'task-b', 'task-c'):
        _held(live.hold(_frame(name), pid=101, seats=3))

    assert [d.pid for d in live.snapshot()] == [101, 101, 101]


def test_a_shared_child_with_every_seat_held_cannot_take_another() -> None:
    live = LiveDispatches(capacity=4)
    _held(live.hold(_frame('task-a'), pid=101, seats=2))
    _held(live.hold(_frame('task-b'), pid=101, seats=2))

    with pytest.raises(AssertionError, match='holds 2 of its 2 seats'):
        live.hold(_frame('task-c'), pid=101, seats=2)


def test_release_child_removes_every_seat_of_a_shared_child() -> None:
    """A kill of a shared child ends the heartbeats of all its tasks."""
    live = LiveDispatches(capacity=4)
    for name in ('task-a', 'task-b', 'task-c'):
        _held(live.hold(_frame(name), pid=101, seats=3))
    _held(live.hold(_frame('task-d'), pid=102, seats=1))

    removed = live.release_child(101)

    assert sorted(d.task_id for d in removed) == ['task-a', 'task-b', 'task-c']
    assert [d.task_id for d in live.snapshot()] == ['task-d']


def test_a_shared_child_can_hold_again_after_one_seat_is_released() -> None:
    """The seat count of a child follows its releases, not only its holds."""
    live = LiveDispatches(capacity=3)
    first = _held(live.hold(_frame('task-a'), pid=101, seats=2))
    _held(live.hold(_frame('task-b'), pid=101, seats=2))
    _ = live.release(first)

    _held(live.hold(_frame('task-c'), pid=101, seats=2))

    with pytest.raises(AssertionError, match='holds 2 of its 2 seats'):
        live.hold(_frame('task-d'), pid=101, seats=2)


def test_a_reused_pid_starts_with_no_held_tasks() -> None:
    """The OS can give a new child the pid of a child whose exit was seen."""
    live = LiveDispatches(capacity=4)
    for name in ('task-a', 'task-b'):
        _held(live.hold(_frame(name), pid=101, seats=2))
    _ = live.release_child(101)

    for name in ('task-c', 'task-d'):
        _held(live.hold(_frame(name), pid=101, seats=2))

    assert [d.task_id for d in live.snapshot()] == ['task-c', 'task-d']


def test_releasing_a_ticket_twice_frees_one_seat() -> None:
    """The second release of a ticket is NotHeld and frees nothing."""
    live = LiveDispatches(capacity=3)
    first = _held(live.hold(_frame('task-a'), pid=101, seats=2))
    _held(live.hold(_frame('task-b'), pid=101, seats=2))
    _ = live.release(first)
    assert live.release(first) == NotHeld()

    _held(live.hold(_frame('task-c'), pid=101, seats=2))

    with pytest.raises(AssertionError, match='holds 2 of its 2 seats'):
        live.hold(_frame('task-d'), pid=101, seats=2)


def test_more_dispatches_than_seats_is_a_defect() -> None:
    live = LiveDispatches(capacity=1)
    _held(live.hold(_frame('task-a'), pid=101, seats=1))

    with pytest.raises(AssertionError, match='more held tasks than the 1 seats'):
        live.hold(_frame('task-b'), pid=102, seats=1)


def test_a_pool_has_at_least_one_seat() -> None:
    with pytest.raises(AssertionError):
        LiveDispatches(capacity=0)


def test_snapshots_from_another_thread_see_whole_dispatches() -> None:
    """The beat thread reads while the loop writes; each read is consistent."""
    live = LiveDispatches(capacity=1)
    stop = threading.Event()
    seen: list[tuple[LiveDispatch, ...]] = []

    def read() -> None:
        while not stop.is_set():
            seen.append(live.snapshot())

    reader = threading.Thread(target=read)
    reader.start()
    for index in range(2_000):
        ticket = _held(live.hold(_frame(f'task-{index}'), pid=101, seats=1))
        _ = live.release(ticket)
    stop.set()
    reader.join(5)

    assert all(len(snapshot) <= 1 for snapshot in seen)
    assert all(d.pid == 101 for snapshot in seen for d in snapshot)
