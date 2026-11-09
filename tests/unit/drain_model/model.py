"""Reference model of drains and drain reservations for worker children.

An exclusive task needs a child process of its own. Shared tasks share a
child, up to a seat count, and one child runs one kind of shared task only:
async or thread. A shared child fills each free seat with a new task of its
kind, so under steady load it may never become idle. To free a child for a
task that needs another kind of child, the worker drains a shared child: the
child takes no new task, its running tasks end normally, and it then becomes
idle.

Every worker sees every waiting task. Workers coordinate drains through a
reservation on the waiting task: "worker W drains for this task until T".
The designed rules:

- a worker renews its reservations at each claim;
- a worker keeps a drain only while it holds a reservation that the drain
  serves;
- a draining child still takes a task of its kind that ranks above every task
  the drain serves;
- a reservation never stops a claim: a worker with a free place takes the
  task at once.

Time moves in rounds. In each round, running tasks and reservations age
first. Then events happen: a task arrives, a worker dies, or a worker misses
its claim. Then each live worker claims once. The search explores every
order of events in a small case and reports each rule that fails, each task
that can wait forever, and the longest wait of each task.
"""

from __future__ import annotations

import itertools
from collections import deque
from collections.abc import Iterable, Sequence
from dataclasses import dataclass, replace
from enum import Enum
from typing import assert_never

MAX_STATES = 3_000_000
"""Default limit on explored states. The search fails closed at the limit."""


class TaskClass(Enum):
    """How a task declares where it runs."""

    EXCLUSIVE = 'exclusive'
    SHARED_ASYNC = 'shared_async'
    SHARED_THREAD = 'shared_thread'


class SeatKind(Enum):
    """The one kind of shared task that a shared child runs."""

    ASYNC = 'async'
    THREAD = 'thread'


class Arrival(Enum):
    """When a tracked task enters the queue."""

    WAITING_AT_START = 'waiting_at_start'
    ANY_ROUND = 'any_round'


class LoadMode(Enum):
    """What happens to an anonymous load unit when it ends or is lost."""

    FINITE = 'finite'
    CLOSED_LOOP = 'closed_loop'


class Life(Enum):
    """Whether a worker still runs."""

    ALIVE = 'alive'
    DEAD = 'dead'


class Renewal(Enum):
    """When a worker renews the reservations it holds."""

    EVERY_CLAIM = 'every_claim'
    NEVER = 'never'


class DrainKeep(Enum):
    """While which condition a worker keeps a drain."""

    WHILE_HOLDING_RESERVATION = 'while_holding_reservation'
    WHILE_SERVED_TASK_WAITS = 'while_served_task_waits'


class DrainAdmission(Enum):
    """Which new task a draining child still takes."""

    HIGHER_RANKED_TASKS = 'higher_ranked_tasks'
    NONE = 'none'


class Rule(Enum):
    """A rule that the search checks."""

    PLACEMENT_COMPLETE = 'after a claim, no waiting task fits a child of that worker'
    DRAIN_HELD = 'after a claim, each drain serves only tasks that its worker reserves'
    DRAIN_COUNT = 'after a claim, a worker has no more drains of a class than its reserved tasks need'
    OCCUPANCY = (
        'a child holds only tasks that fit it, and each running task has one place'
    )
    DRAIN_SHAPE = (
        'a drain serves 1 to seat-count tasks of one class that its child cannot run'
    )


@dataclass(frozen=True, slots=True)
class Rules:
    """One rule set for renewal, keeping drains, and admission to drains."""

    renewal: Renewal
    keep: DrainKeep
    admission: DrainAdmission


DESIGNED_RULES = Rules(
    renewal=Renewal.EVERY_CLAIM,
    keep=DrainKeep.WHILE_HOLDING_RESERVATION,
    admission=DrainAdmission.HIGHER_RANKED_TASKS,
)


@dataclass(frozen=True, slots=True)
class TaskSpec:
    """A tracked task. A lower `task_id` ranks higher in claim order."""

    task_id: int
    task_class: TaskClass
    run_rounds: int
    arrival: Arrival


@dataclass(frozen=True, slots=True)
class NoLoad:
    """The case has no anonymous load."""


@dataclass(frozen=True, slots=True)
class AsyncLoad:
    """Anonymous async tasks that rank below every tracked task.

    A load unit never needs a drain in the cases that the model accepts. The
    claim fails loud when waiting load could ask for one.
    """

    units: int
    run_choices: tuple[int, ...]
    mode: LoadMode


Load = NoLoad | AsyncLoad


@dataclass(frozen=True, slots=True)
class Unreserved:
    """No worker drains for the task."""


@dataclass(frozen=True, slots=True)
class Reserved:
    """`worker` drains for the task for `rounds_left` more rounds."""

    worker: int
    rounds_left: int


Reservation = Unreserved | Reserved


@dataclass(frozen=True, slots=True)
class NotArrived:
    """The task is not in the queue yet."""


@dataclass(frozen=True, slots=True)
class Waiting:
    """The task is in the queue and no child runs it."""

    reservation: Reservation


@dataclass(frozen=True, slots=True)
class Running:
    """A child runs the task."""


@dataclass(frozen=True, slots=True)
class Done:
    """The task finished, or it ended with its worker."""


TaskStatus = NotArrived | Waiting | Running | Done


@dataclass(frozen=True, slots=True)
class TrackedSeat:
    """A seat that runs a tracked task."""

    task_id: int
    rounds_left: int


@dataclass(frozen=True, slots=True)
class LoadSeat:
    """A seat that runs one anonymous load unit."""

    rounds_left: int


Seat = TrackedSeat | LoadSeat


@dataclass(frozen=True, slots=True)
class Idle:
    """The child runs nothing and takes any class."""


@dataclass(frozen=True, slots=True)
class Exclusive:
    """The child runs one exclusive task."""

    task_id: int
    rounds_left: int


@dataclass(frozen=True, slots=True)
class Shared:
    """The child runs 1 to seat-count tasks of one kind and takes more."""

    kind: SeatKind
    seats: tuple[Seat, ...]


@dataclass(frozen=True, slots=True)
class Draining:
    """The child empties for the waiting tasks in `serves`."""

    kind: SeatKind
    seats: tuple[Seat, ...]
    serves: frozenset[int]


@dataclass(frozen=True, slots=True)
class Gone:
    """The child ended with its worker."""


ChildState = Idle | Exclusive | Shared | Draining | Gone


@dataclass(frozen=True, slots=True)
class WorkerState:
    """One worker and its children, in a fixed order."""

    life: Life
    children: tuple[ChildState, ...]


@dataclass(frozen=True, slots=True)
class ModelState:
    """The whole system at the start of a round."""

    tasks: tuple[TaskStatus, ...]
    workers: tuple[WorkerState, ...]
    load_waiting: int
    deaths_left: int
    stalls_left: int


@dataclass(frozen=True, slots=True)
class Scenario:
    """One small case: its tasks, load, starting children and event limits."""

    name: str
    seats_per_child: int
    reservation_rounds: int
    tasks: tuple[TaskSpec, ...]
    load: Load
    initial_workers: tuple[WorkerState, ...]
    max_deaths: int
    max_stalls: int


@dataclass(frozen=True, slots=True)
class Violation:
    """One failed rule and a short description of where it failed."""

    rule: Rule
    detail: str


@dataclass(frozen=True, slots=True)
class Starvation:
    """A cycle of reachable states keeps the task waiting forever."""

    task_id: int
    states_on_or_after_cycles: int


@dataclass(frozen=True, slots=True)
class LongestWait:
    """The task starts at the latest in round `rounds` of its wait."""

    task_id: int
    rounds: int


@dataclass(frozen=True, slots=True)
class Explored:
    """The search visited every reachable state."""

    state_count: int
    violations: tuple[Violation, ...]
    starvations: tuple[Starvation, ...]
    longest_waits: tuple[LongestWait, ...]


@dataclass(frozen=True, slots=True)
class StateLimitReached:
    """The search stopped at the state limit and checked nothing more."""

    limit: int


ExploreResult = Explored | StateLimitReached


@dataclass(frozen=True, slots=True)
class PlaceAt:
    """Put the child at `index` into the state `child`."""

    index: int
    child: ChildState


@dataclass(frozen=True, slots=True)
class NoPlace:
    """No child of the worker can take the task."""


Placement = PlaceAt | NoPlace


@dataclass(frozen=True, slots=True)
class SlotAt:
    """One load unit goes into the child at `index`."""

    index: int


LoadSlot = SlotAt | NoPlace


@dataclass(frozen=True, slots=True)
class Contents:
    """The tracked tasks and load units that a child holds."""

    tracked: tuple[int, ...]
    load_units: int


@dataclass(frozen=True, slots=True)
class Aged:
    """A child after one round, with the tasks and load units that ended."""

    child: ChildState
    ended: Contents


@dataclass(frozen=True, slots=True)
class NobodyDies:
    """No worker dies in this round."""


@dataclass(frozen=True, slots=True)
class WorkerDies:
    """`worker` dies at the start of this round."""

    worker: int


Death = NobodyDies | WorkerDies


@dataclass(frozen=True, slots=True)
class NobodyStalls:
    """Every live worker claims in this round."""


@dataclass(frozen=True, slots=True)
class WorkerStalls:
    """`worker` misses its claim in this round."""

    worker: int


Stall = NobodyStalls | WorkerStalls


@dataclass(frozen=True, slots=True)
class RoundEvents:
    """The events of one round, before the claims."""

    arrivals: tuple[int, ...]
    death: Death
    stall: Stall


@dataclass(frozen=True, slots=True)
class AfterEvents:
    """The state after the events, and the workers that claim, in no order."""

    state: ModelState
    claimers: tuple[int, ...]


@dataclass(frozen=True, slots=True)
class Walked:
    """The state after the placement walk, and the tasks it skipped."""

    state: ModelState
    skipped: tuple[int, ...]


@dataclass(frozen=True, slots=True)
class Step:
    """A state after claims, and the rules that failed on the way."""

    state: ModelState
    violations: tuple[Violation, ...]


def seat_sort_key(seat: Seat) -> tuple[int, int, int]:
    """Return the key that gives seats one canonical order."""
    match seat:
        case TrackedSeat(task_id=task_id, rounds_left=rounds_left):
            return (0, task_id, rounds_left)
        case LoadSeat(rounds_left=rounds_left):
            return (1, 0, rounds_left)
        case _:
            assert_never(seat)


def sorted_seats(seats: Iterable[Seat]) -> tuple[Seat, ...]:
    """Return the seats in canonical order, so equal states compare equal."""
    return tuple(sorted(seats, key=seat_sort_key))


def kind_runs(kind: SeatKind, task_class: TaskClass) -> bool:
    """Return whether a shared child of `kind` can run a task of `task_class`."""
    match task_class:
        case TaskClass.EXCLUSIVE:
            return False
        case TaskClass.SHARED_ASYNC:
            return kind is SeatKind.ASYNC
        case TaskClass.SHARED_THREAD:
            return kind is SeatKind.THREAD
        case _:
            assert_never(task_class)


def seat_count(scenario: Scenario, task_class: TaskClass) -> int:
    """Return how many tasks of `task_class` one freed child can take."""
    match task_class:
        case TaskClass.EXCLUSIVE:
            return 1
        case TaskClass.SHARED_ASYNC | TaskClass.SHARED_THREAD:
            return scenario.seats_per_child
        case _:
            assert_never(task_class)


def seats_used(child: ChildState) -> int:
    """Return how many tasks the child runs."""
    match child:
        case Idle() | Gone():
            return 0
        case Exclusive():
            return 1
        case Shared(seats=seats) | Draining(seats=seats):
            return len(seats)
        case _:
            assert_never(child)


def holds_reservation(status: TaskStatus, worker: int) -> bool:
    """Return whether `worker` holds a live reservation on a waiting task."""
    match status:
        case Waiting(reservation=Reserved(worker=holder)):
            return holder == worker
        case NotArrived() | Waiting() | Running() | Done():
            return False
        case _:
            assert_never(status)


def may_drain_for(status: TaskStatus, worker: int) -> bool:
    """Return whether `worker` may drain for the task: no other worker holds it."""
    match status:
        case Waiting(reservation=reservation):
            match reservation:
                case Unreserved():
                    return True
                case Reserved(worker=holder):
                    return holder == worker
                case _:
                    assert_never(reservation)
        case NotArrived() | Running() | Done():
            return False
        case _:
            assert_never(status)


def load_back_in_queue(load: Load, units: int) -> int:
    """Return how many ended or lost load units enter the queue again."""
    match load:
        case NoLoad():
            return 0
        case AsyncLoad(mode=mode):
            match mode:
                case LoadMode.CLOSED_LOOP:
                    return units
                case LoadMode.FINITE:
                    return 0
                case _:
                    assert_never(mode)
        case _:
            assert_never(load)


def seat_contents(seats: Sequence[Seat]) -> Contents:
    """Return the tracked tasks and load units in `seats`."""
    tracked: list[int] = []
    load_units = 0
    for seat in seats:
        match seat:
            case TrackedSeat(task_id=task_id):
                tracked.append(task_id)
            case LoadSeat():
                load_units += 1
            case _:
                assert_never(seat)
    return Contents(tuple(tracked), load_units)


def child_contents(child: ChildState) -> Contents:
    """Return the tracked tasks and load units that the child holds."""
    match child:
        case Idle() | Gone():
            return Contents((), 0)
        case Exclusive(task_id=task_id):
            return Contents((task_id,), 0)
        case Shared(seats=seats) | Draining(seats=seats):
            return seat_contents(seats)
        case _:
            assert_never(child)


def age_seats(seats: tuple[Seat, ...]) -> tuple[tuple[Seat, ...], Contents]:
    """Age each seat one round. Return the seats left and what ended."""
    kept: list[Seat] = []
    ended: list[Seat] = []
    for seat in seats:
        match seat:
            case TrackedSeat(
                task_id=task_id, rounds_left=rounds_left
            ) if rounds_left > 1:
                kept.append(TrackedSeat(task_id, rounds_left - 1))
            case LoadSeat(rounds_left=rounds_left) if rounds_left > 1:
                kept.append(LoadSeat(rounds_left - 1))
            case TrackedSeat() | LoadSeat():
                ended.append(seat)
            case _:
                assert_never(seat)
    return sorted_seats(kept), seat_contents(ended)


def advance_child(child: ChildState) -> Aged:
    """Age the child one round. An emptied child becomes idle."""
    match child:
        case Idle() | Gone():
            return Aged(child, Contents((), 0))
        case Exclusive(task_id=task_id, rounds_left=rounds_left) if rounds_left > 1:
            return Aged(Exclusive(task_id, rounds_left - 1), Contents((), 0))
        case Exclusive(task_id=task_id):
            return Aged(Idle(), Contents((task_id,), 0))
        case Shared(kind=kind, seats=seats):
            kept, ended = age_seats(seats)
            return Aged(Shared(kind, kept) if kept else Idle(), ended)
        case Draining(kind=kind, seats=seats, serves=serves):
            kept, ended = age_seats(seats)
            return Aged(Draining(kind, kept, serves) if kept else Idle(), ended)
        case _:
            assert_never(child)


def age_reservation(status: TaskStatus) -> TaskStatus:
    """Age a reservation one round. It ends when no round is left."""
    match status:
        case Waiting(
            reservation=Reserved(worker=worker, rounds_left=rounds_left)
        ) if rounds_left > 1:
            return Waiting(Reserved(worker, rounds_left - 1))
        case Waiting(reservation=Reserved()):
            return Waiting(Unreserved())
        case NotArrived() | Waiting() | Running() | Done():
            return status
        case _:
            assert_never(status)


def advance(scenario: Scenario, state: ModelState) -> ModelState:
    """Move every running task and every reservation one round forward."""
    tasks = list(state.tasks)
    load_waiting = state.load_waiting
    workers: list[WorkerState] = []
    for worker in state.workers:
        children: list[ChildState] = []
        for child in worker.children:
            aged = advance_child(child)
            children.append(aged.child)
            for task_id in aged.ended.tracked:
                tasks[task_id] = Done()
            load_waiting += load_back_in_queue(scenario.load, aged.ended.load_units)
        workers.append(WorkerState(worker.life, tuple(children)))
    return ModelState(
        tasks=tuple(age_reservation(status) for status in tasks),
        workers=tuple(workers),
        load_waiting=load_waiting,
        deaths_left=state.deaths_left,
        stalls_left=state.stalls_left,
    )


def live_workers(state: ModelState) -> tuple[int, ...]:
    """Return the indexes of the workers that still run."""
    return tuple(
        index for index, worker in enumerate(state.workers) if worker.life is Life.ALIVE
    )


def same_worker(death: Death, stall: Stall) -> bool:
    """Return whether one worker would both die and miss its claim."""
    match (death, stall):
        case (WorkerDies(worker=dying), WorkerStalls(worker=stalling)):
            return dying == stalling
        case _:
            return False


def round_events(state: ModelState) -> list[RoundEvents]:
    """Return every combination of events that can happen in the next round."""
    pending = [
        task_id
        for task_id, status in enumerate(state.tasks)
        if isinstance(status, NotArrived)
    ]
    arrival_sets = [
        combination
        for size in range(len(pending) + 1)
        for combination in itertools.combinations(pending, size)
    ]
    deaths: list[Death] = [NobodyDies()]
    stalls: list[Stall] = [NobodyStalls()]
    if state.deaths_left > 0:
        deaths.extend(WorkerDies(worker) for worker in live_workers(state))
    if state.stalls_left > 0:
        stalls.extend(WorkerStalls(worker) for worker in live_workers(state))
    return [
        RoundEvents(arrivals, death, stall)
        for arrivals in arrival_sets
        for death in deaths
        for stall in stalls
        if not same_worker(death, stall)
    ]


def kill_worker(scenario: Scenario, state: ModelState, worker: int) -> ModelState:
    """End `worker` and its children. Its reservations stay until they end."""
    tasks = list(state.tasks)
    load_waiting = state.load_waiting
    for child in state.workers[worker].children:
        lost = child_contents(child)
        for task_id in lost.tracked:
            tasks[task_id] = Done()
        load_waiting += load_back_in_queue(scenario.load, lost.load_units)
    workers = list(state.workers)
    gone: tuple[ChildState, ...] = tuple(Gone() for _ in state.workers[worker].children)
    workers[worker] = WorkerState(Life.DEAD, gone)
    return ModelState(
        tasks=tuple(tasks),
        workers=tuple(workers),
        load_waiting=load_waiting,
        deaths_left=state.deaths_left - 1,
        stalls_left=state.stalls_left,
    )


def apply_events(
    scenario: Scenario, state: ModelState, events: RoundEvents
) -> AfterEvents:
    """Apply arrivals, a death and a missed claim. Return who claims."""
    tasks = list(state.tasks)
    for task_id in events.arrivals:
        tasks[task_id] = Waiting(Unreserved())
    after = replace(state, tasks=tuple(tasks))
    match events.death:
        case NobodyDies():
            pass
        case WorkerDies(worker=dying):
            after = kill_worker(scenario, after, dying)
        case _:
            assert_never(events.death)
    match events.stall:
        case NobodyStalls():
            return AfterEvents(after, live_workers(after))
        case WorkerStalls(worker=stalling):
            claimers = tuple(
                worker for worker in live_workers(after) if worker != stalling
            )
            return AfterEvents(
                replace(after, stalls_left=after.stalls_left - 1), claimers
            )
        case _:
            assert_never(events.stall)


def with_children(
    state: ModelState, worker: int, children: tuple[ChildState, ...]
) -> ModelState:
    """Return the state with new children for `worker`."""
    workers = list(state.workers)
    workers[worker] = WorkerState(state.workers[worker].life, children)
    return replace(state, workers=tuple(workers))


def renew(
    scenario: Scenario, rules: Rules, state: ModelState, worker: int
) -> ModelState:
    """Renew the reservations that `worker` holds, if the rules renew."""
    match rules.renewal:
        case Renewal.NEVER:
            return state
        case Renewal.EVERY_CLAIM:
            renewed = Waiting(Reserved(worker, scenario.reservation_rounds))
            tasks = tuple(
                renewed if holds_reservation(status, worker) else status
                for status in state.tasks
            )
            return replace(state, tasks=tasks)
        case _:
            assert_never(rules.renewal)


def fullest_first(children: Sequence[ChildState]) -> list[int]:
    """Return child indexes, the child with the most tasks first."""
    return sorted(
        range(len(children)), key=lambda index: (-seats_used(children[index]), index)
    )


def find_idle(children: Sequence[ChildState], new_child: ChildState) -> Placement:
    """Place into the first idle child."""
    for index, child in enumerate(children):
        if isinstance(child, Idle):
            return PlaceAt(index, new_child)
    return NoPlace()


def admission_allows(
    admission: DrainAdmission, task_id: int, serves: frozenset[int]
) -> bool:
    """Return whether a draining child that serves `serves` takes the task."""
    match admission:
        case DrainAdmission.HIGHER_RANKED_TASKS:
            return task_id < min(serves)
        case DrainAdmission.NONE:
            return False
        case _:
            assert_never(admission)


def find_draining_seat(
    scenario: Scenario,
    admission: DrainAdmission,
    children: Sequence[ChildState],
    spec: TaskSpec,
    kind: SeatKind,
) -> Placement:
    """Place into a draining child of `kind` that admits the task."""
    seat = TrackedSeat(spec.task_id, spec.run_rounds)
    for index in fullest_first(children):
        match children[index]:
            case Draining(kind=child_kind, seats=seats, serves=serves) if (
                child_kind is kind
                and len(seats) < scenario.seats_per_child
                and admission_allows(admission, spec.task_id, serves)
            ):
                return PlaceAt(
                    index, Draining(kind, sorted_seats((*seats, seat)), serves)
                )
            case _:
                pass
    return NoPlace()


def find_shared_place(
    scenario: Scenario,
    admission: DrainAdmission,
    children: Sequence[ChildState],
    spec: TaskSpec,
    kind: SeatKind,
) -> Placement:
    """Place a shared task: a shared child, then a draining child, then an idle one.

    A draining child comes before an idle child, so that an idle child stays
    free for a task that needs a whole child.
    """
    seat = TrackedSeat(spec.task_id, spec.run_rounds)
    for index in fullest_first(children):
        match children[index]:
            case Shared(kind=child_kind, seats=seats) if (
                child_kind is kind and len(seats) < scenario.seats_per_child
            ):
                return PlaceAt(index, Shared(kind, sorted_seats((*seats, seat))))
            case _:
                pass
    draining = find_draining_seat(scenario, admission, children, spec, kind)
    match draining:
        case PlaceAt():
            return draining
        case NoPlace():
            return find_idle(children, Shared(kind, (seat,)))
        case _:
            assert_never(draining)


def find_place(
    scenario: Scenario,
    admission: DrainAdmission,
    children: Sequence[ChildState],
    spec: TaskSpec,
) -> Placement:
    """Return where a child of the worker takes the task, if any child can."""
    match spec.task_class:
        case TaskClass.EXCLUSIVE:
            return find_idle(children, Exclusive(spec.task_id, spec.run_rounds))
        case TaskClass.SHARED_ASYNC:
            return find_shared_place(
                scenario, admission, children, spec, SeatKind.ASYNC
            )
        case TaskClass.SHARED_THREAD:
            return find_shared_place(
                scenario, admission, children, spec, SeatKind.THREAD
            )
        case _:
            assert_never(spec.task_class)


def find_load_slot(scenario: Scenario, children: Sequence[ChildState]) -> LoadSlot:
    """Return where one load unit goes: a shared async child, then an idle child.

    Load ranks below every tracked task, so no draining child takes it.
    """
    for index in fullest_first(children):
        match children[index]:
            case Shared(kind=SeatKind.ASYNC, seats=seats) if len(
                seats
            ) < scenario.seats_per_child:
                return SlotAt(index)
            case _:
                pass
    for index, child in enumerate(children):
        if isinstance(child, Idle):
            return SlotAt(index)
    return NoPlace()


def add_load_seat(child: ChildState, seat: LoadSeat) -> ChildState:
    """Return the child with one more load seat."""
    match child:
        case Idle():
            return Shared(SeatKind.ASYNC, (seat,))
        case Shared(kind=SeatKind.ASYNC, seats=seats):
            return Shared(SeatKind.ASYNC, sorted_seats((*seats, seat)))
        case other:
            raise AssertionError(f'a load slot is not an idle or async child: {other}')


def load_run_choices(load: Load) -> tuple[int, ...]:
    """Return the run lengths that one placed load unit may have."""
    match load:
        case NoLoad():
            return ()
        case AsyncLoad(run_choices=run_choices):
            return run_choices
        case _:
            assert_never(load)


def load_options(
    scenario: Scenario, children: Sequence[ChildState], waiting: int
) -> tuple[tuple[tuple[ChildState, ...], int], ...]:
    """Place waiting load. Return each distinct result and the units placed.

    Where the units go depends only on seat counts. Each placed unit then
    takes each of its possible run lengths.
    """
    slots: list[int] = []
    counting = list(children)
    for _ in range(waiting):
        slot = find_load_slot(scenario, counting)
        match slot:
            case SlotAt(index=index):
                slots.append(index)
                counting[index] = add_load_seat(counting[index], LoadSeat(0))
            case NoPlace():
                break
            case _:
                assert_never(slot)
    options: dict[tuple[ChildState, ...], None] = {}
    for lengths in itertools.product(
        load_run_choices(scenario.load), repeat=len(slots)
    ):
        placed = list(children)
        for index, rounds in zip(slots, lengths, strict=True):
            placed[index] = add_load_seat(placed[index], LoadSeat(rounds))
        options.setdefault(tuple(placed), None)
    return tuple((option, len(slots)) for option in options)


def walk(
    scenario: Scenario, rules: Rules, state: ModelState, worker: int
) -> list[Walked]:
    """Place waiting tasks in claim order, then load. Return each outcome."""
    children = list(state.workers[worker].children)
    tasks = list(state.tasks)
    skipped: list[int] = []
    for spec in scenario.tasks:
        if not isinstance(tasks[spec.task_id], Waiting):
            continue
        placement = find_place(scenario, rules.admission, children, spec)
        match placement:
            case PlaceAt(index=index, child=child):
                children[index] = child
                tasks[spec.task_id] = Running()
            case NoPlace():
                skipped.append(spec.task_id)
            case _:
                assert_never(placement)
    walked: list[Walked] = []
    for option, placed_units in load_options(scenario, children, state.load_waiting):
        placed = replace(
            state, tasks=tuple(tasks), load_waiting=state.load_waiting - placed_units
        )
        walked.append(Walked(with_children(placed, worker, option), tuple(skipped)))
    return walked


def assert_load_needs_no_drain(state: ModelState, worker: int) -> None:
    """Fail loud when waiting load could ask for a drain.

    A thread child cannot run async load. When load waits beside one, the
    designed rules would drain it for the load, and load has no row that a
    reservation could mark. Such a case is outside what the model covers.
    """
    if state.load_waiting == 0:
        return
    for child in state.workers[worker].children:
        match child:
            case Shared(kind=SeatKind.THREAD):
                raise AssertionError(
                    'load waits beside a thread child; the model cannot reserve load'
                )
            case _:
                pass


def drain_keeps_task(keep: DrainKeep, status: TaskStatus, worker: int) -> bool:
    """Return whether the drain still serves a task under the keep rule."""
    match keep:
        case DrainKeep.WHILE_HOLDING_RESERVATION:
            return holds_reservation(status, worker)
        case DrainKeep.WHILE_SERVED_TASK_WAITS:
            return isinstance(status, Waiting)
        case _:
            assert_never(keep)


def kept_drain(
    rules: Rules, tasks: Sequence[TaskStatus], worker: int, child: ChildState
) -> ChildState:
    """Keep or stop one drain. A stopped drain becomes a shared child again."""
    match child:
        case Draining(kind=kind, seats=seats, serves=serves):
            still = frozenset(
                task_id
                for task_id in serves
                if drain_keeps_task(rules.keep, tasks[task_id], worker)
            )
            return Draining(kind, seats, still) if still else Shared(kind, seats)
        case Idle() | Exclusive() | Shared() | Gone():
            return child
        case _:
            assert_never(child)


def keep_drains(rules: Rules, state: ModelState, worker: int) -> ModelState:
    """Apply the keep rule to every drain of `worker`."""
    children = tuple(
        kept_drain(rules, state.tasks, worker, child)
        for child in state.workers[worker].children
    )
    return with_children(state, worker, children)


def served_by(children: Sequence[ChildState], task_id: int) -> bool:
    """Return whether a drain of these children already serves the task."""
    for child in children:
        match child:
            case Draining(serves=serves) if task_id in serves:
                return True
            case _:
                pass
    return False


def drain_class(scenario: Scenario, serves: frozenset[int]) -> TaskClass:
    """Return the class of the tasks that a drain serves."""
    return scenario.tasks[min(serves)].task_class


def drain_place(
    scenario: Scenario, children: Sequence[ChildState], spec: TaskSpec
) -> Placement:
    """Add the task to a drain of its class with room, or start a new drain.

    A new drain goes to the shared child that cannot run the class and has
    the fewest tasks.
    """
    for index, child in enumerate(children):
        match child:
            case Draining(kind=kind, seats=seats, serves=serves) if (
                drain_class(scenario, serves) is spec.task_class
                and len(serves) < seat_count(scenario, spec.task_class)
            ):
                return PlaceAt(index, Draining(kind, seats, serves | {spec.task_id}))
            case _:
                pass
    candidates = [
        index
        for index, child in enumerate(children)
        if isinstance(child, Shared) and not kind_runs(child.kind, spec.task_class)
    ]
    if not candidates:
        return NoPlace()
    chosen = min(candidates, key=lambda index: (seats_used(children[index]), index))
    match children[chosen]:
        case Shared(kind=kind, seats=seats):
            return PlaceAt(chosen, Draining(kind, seats, frozenset({spec.task_id})))
        case other:
            raise AssertionError(f'drain candidate is not a shared child: {other}')


def start_drains(
    scenario: Scenario,
    state: ModelState,
    worker: int,
    skipped: Sequence[int],
) -> ModelState:
    """Reserve skipped tasks that no other worker holds, and drain for them."""
    tasks = list(state.tasks)
    children = list(state.workers[worker].children)
    reservation = Waiting(Reserved(worker, scenario.reservation_rounds))
    for task_id in skipped:
        if not may_drain_for(tasks[task_id], worker) or served_by(children, task_id):
            continue
        placement = drain_place(scenario, children, scenario.tasks[task_id])
        match placement:
            case PlaceAt(index=index, child=child):
                children[index] = child
                tasks[task_id] = reservation
            case NoPlace():
                pass
            case _:
                assert_never(placement)
    reserved = replace(state, tasks=tuple(tasks))
    return with_children(reserved, worker, tuple(children))


def placement_violations(
    scenario: Scenario, state: ModelState, worker: int
) -> tuple[Violation, ...]:
    """Check, right after the placement walk, that no waiting task fits a child.

    The check uses the designed admission rule for draining children, under
    every rule set: a task must not wait for a seat that is kept empty only
    for tasks that rank below it. Seats that a stopped drain frees after the
    walk fill at the next claim.
    """
    children = state.workers[worker].children
    violations: list[Violation] = []
    for spec in scenario.tasks:
        waits = isinstance(state.tasks[spec.task_id], Waiting)
        fits = find_place(scenario, DrainAdmission.HIGHER_RANKED_TASKS, children, spec)
        if waits and isinstance(fits, PlaceAt):
            detail = f'worker {worker}: task {spec.task_id} waits, child {fits.index} fits it: {children}'
            violations.append(Violation(Rule.PLACEMENT_COMPLETE, detail))
    if state.load_waiting > 0 and isinstance(
        find_load_slot(scenario, children), SlotAt
    ):
        detail = f'worker {worker}: load waits beside a free place: {children}'
        violations.append(Violation(Rule.PLACEMENT_COMPLETE, detail))
    return tuple(violations)


def drain_held_violations(state: ModelState, worker: int) -> tuple[Violation, ...]:
    """Check, after a claim, that each drain serves only tasks its worker holds."""
    violations: list[Violation] = []
    for index, child in enumerate(state.workers[worker].children):
        match child:
            case Draining(serves=serves) if not all(
                holds_reservation(state.tasks[task_id], worker) for task_id in serves
            ):
                detail = f'worker {worker}: child {index} drains for {sorted(serves)}: {state.tasks}'
                violations.append(Violation(Rule.DRAIN_HELD, detail))
            case _:
                pass
    return tuple(violations)


def drain_count_violations(
    scenario: Scenario, state: ModelState, worker: int
) -> tuple[Violation, ...]:
    """Check, after a claim, that drains per class do not exceed the need.

    A class needs (its reserved waiting tasks ÷ seat count), rounded up,
    drains.
    """
    drains: dict[TaskClass, int] = {}
    for child in state.workers[worker].children:
        match child:
            case Draining(serves=serves):
                task_class = drain_class(scenario, serves)
                drains[task_class] = drains.get(task_class, 0) + 1
            case _:
                pass
    violations: list[Violation] = []
    for task_class, count in drains.items():
        held = sum(
            1
            for spec in scenario.tasks
            if spec.task_class is task_class
            and holds_reservation(state.tasks[spec.task_id], worker)
        )
        needed = -(-held // seat_count(scenario, task_class))
        if count > needed:
            detail = f'worker {worker}: {count} drains for {held} reserved {task_class} tasks: {state}'
            violations.append(Violation(Rule.DRAIN_COUNT, detail))
    return tuple(violations)


def claim(
    scenario: Scenario, rules: Rules, state: ModelState, worker: int
) -> list[Step]:
    """Run one claim of `worker`.

    The worker renews its reservations, which also tells it which tasks it no
    longer holds, and stops the drains for those tasks. It then places tasks,
    stops the drains for tasks it just placed, starts drains for skipped tasks,
    and the checks run.
    """
    renewed = keep_drains(rules, renew(scenario, rules, state, worker), worker)
    steps: list[Step] = []
    for walked in walk(scenario, rules, renewed, worker):
        assert_load_needs_no_drain(walked.state, worker)
        placement = placement_violations(scenario, walked.state, worker)
        kept = keep_drains(rules, walked.state, worker)
        drained = start_drains(scenario, kept, worker, walked.skipped)
        after_drains = drain_held_violations(drained, worker) + drain_count_violations(
            scenario, drained, worker
        )
        steps.append(Step(drained, placement + after_drains))
    return steps


def run_claims(
    scenario: Scenario, rules: Rules, state: ModelState, order: Sequence[int]
) -> list[Step]:
    """Run one claim per worker in `order`. Return every outcome."""
    steps = [Step(state, ())]
    for worker in order:
        steps = [
            Step(outcome.state, step.violations + outcome.violations)
            for step in steps
            for outcome in claim(scenario, rules, step.state, worker)
        ]
    return steps


def next_steps(scenario: Scenario, rules: Rules, state: ModelState) -> list[Step]:
    """Return every state that one round can reach from `state`."""
    advanced = advance(scenario, state)
    steps: list[Step] = []
    for events in round_events(advanced):
        after = apply_events(scenario, advanced, events)
        for order in itertools.permutations(after.claimers):
            steps.extend(run_claims(scenario, rules, after.state, order))
    return steps


def occupancy_violations(
    scenario: Scenario, worker: int, child: ChildState
) -> list[Violation]:
    """Check that one child holds only tasks that fit it."""

    def fail(detail: str) -> Violation:
        return Violation(Rule.OCCUPANCY, f'worker {worker}: {detail}: {child}')

    match child:
        case Idle() | Gone():
            return []
        case Exclusive(task_id=task_id):
            fits = scenario.tasks[task_id].task_class is TaskClass.EXCLUSIVE
            return [] if fits else [fail('an exclusive child runs a shared task')]
        case Shared(kind=kind, seats=seats) | Draining(kind=kind, seats=seats):
            violations: list[Violation] = []
            if not 1 <= len(seats) <= scenario.seats_per_child:
                violations.append(fail('a shared child holds a wrong number of tasks'))
            for seat in seats:
                match seat:
                    case TrackedSeat(task_id=task_id) if not kind_runs(
                        kind, scenario.tasks[task_id].task_class
                    ):
                        violations.append(
                            fail(f'task {task_id} runs in a child of another kind')
                        )
                    case LoadSeat() if kind is not SeatKind.ASYNC:
                        violations.append(fail('load runs in a thread child'))
                    case TrackedSeat() | LoadSeat():
                        pass
                    case _:
                        assert_never(seat)
            return violations
        case _:
            assert_never(child)


def drain_shape_violations(
    scenario: Scenario, worker: int, child: ChildState, seen: set[int]
) -> list[Violation]:
    """Check one drain: its tasks, their class, and that no other drain serves them."""
    match child:
        case Draining(kind=kind, serves=serves):
            violations: list[Violation] = []
            classes = {scenario.tasks[task_id].task_class for task_id in serves}
            if not serves or len(classes) != 1:
                detail = (
                    f'worker {worker}: drain serves no task or mixes classes: {child}'
                )
                violations.append(Violation(Rule.DRAIN_SHAPE, detail))
            for task_class in classes:
                if kind_runs(kind, task_class) or len(serves) > seat_count(
                    scenario, task_class
                ):
                    detail = f'worker {worker}: drain does not fit its class {task_class}: {child}'
                    violations.append(Violation(Rule.DRAIN_SHAPE, detail))
            if seen & serves:
                detail = f'worker {worker}: two drains serve {sorted(seen & serves)}'
                violations.append(Violation(Rule.DRAIN_SHAPE, detail))
            seen.update(serves)
            return violations
        case Idle() | Exclusive() | Shared() | Gone():
            return []
        case _:
            assert_never(child)


def place_count_violations(scenario: Scenario, state: ModelState) -> list[Violation]:
    """Check that each running task has exactly one place, and others none."""
    places = [0] * len(scenario.tasks)
    for worker in state.workers:
        for child in worker.children:
            for task_id in child_contents(child).tracked:
                places[task_id] += 1
    violations: list[Violation] = []
    for task_id, status in enumerate(state.tasks):
        expected = 1 if isinstance(status, Running) else 0
        if places[task_id] != expected:
            detail = f'task {task_id} is {status} and has {places[task_id]} places'
            violations.append(Violation(Rule.OCCUPANCY, detail))
    return violations


def check_state(scenario: Scenario, state: ModelState) -> tuple[Violation, ...]:
    """Check the rules that must hold in every reachable state."""
    violations = place_count_violations(scenario, state)
    for index, worker in enumerate(state.workers):
        seen: set[int] = set()
        for child in worker.children:
            violations.extend(occupancy_violations(scenario, index, child))
            violations.extend(drain_shape_violations(scenario, index, child, seen))
    return tuple(violations)


def initial_status(spec: TaskSpec) -> TaskStatus:
    """Return the status of a task before the first round."""
    match spec.arrival:
        case Arrival.WAITING_AT_START:
            return Waiting(Unreserved())
        case Arrival.ANY_ROUND:
            return NotArrived()
        case _:
            assert_never(spec.arrival)


def initial_state(scenario: Scenario) -> ModelState:
    """Build the first state. Fail loud on a case that the model does not accept."""
    for task_id, spec in enumerate(scenario.tasks):
        assert spec.task_id == task_id, 'task ids must be 0..n-1 in order'
        assert spec.run_rounds >= 1, 'a task runs at least one round'
    assert scenario.seats_per_child >= 1 and scenario.reservation_rounds >= 1
    assert (
        0 <= scenario.max_deaths < len(scenario.initial_workers)
    ), 'one worker must survive'
    placed_load = 0
    for worker in scenario.initial_workers:
        assert worker.life is Life.ALIVE
        for child in worker.children:
            contents = child_contents(child)
            assert contents.tracked == (), 'tracked tasks start outside children'
            assert not isinstance(
                child, Draining | Exclusive
            ), 'children start idle or shared'
            placed_load += contents.load_units
    match scenario.load:
        case NoLoad():
            load_waiting = 0
        case AsyncLoad(units=units, run_choices=run_choices):
            assert run_choices and min(run_choices) >= 1, 'load runs at least one round'
            load_waiting = units - placed_load
        case _:
            assert_never(scenario.load)
    assert load_waiting >= 0, 'more load is placed than the load has units'
    return ModelState(
        tasks=tuple(initial_status(spec) for spec in scenario.tasks),
        workers=scenario.initial_workers,
        load_waiting=load_waiting,
        deaths_left=scenario.max_deaths,
        stalls_left=scenario.max_stalls,
    )


def wait_outcome(
    states: Sequence[ModelState], edges: Sequence[set[int]], task_id: int
) -> Starvation | LongestWait:
    """Find a cycle that keeps the task waiting, or else its longest wait.

    The states where the task waits form a graph. Removing states with no
    incoming edge from that graph, one at a time, removes all of it unless it
    holds a cycle. Without a cycle, the removal order gives the longest path.
    """
    member = [isinstance(state.tasks[task_id], Waiting) for state in states]
    incoming = [0] * len(states)
    for source, targets in enumerate(edges):
        if member[source]:
            for target in targets:
                incoming[target] += 1 if member[target] else 0
    longest = [1] * len(states)
    ready = deque(
        index for index in range(len(states)) if member[index] and incoming[index] == 0
    )
    removed = 0
    while ready:
        source = ready.popleft()
        removed += 1
        for target in edges[source]:
            if member[target]:
                longest[target] = max(longest[target], longest[source] + 1)
                incoming[target] -= 1
                if incoming[target] == 0:
                    ready.append(target)
    waiting_states = sum(member)
    if removed < waiting_states:
        return Starvation(task_id, waiting_states - removed)
    rounds = max(
        (longest[index] for index in range(len(states)) if member[index]), default=0
    )
    return LongestWait(task_id, rounds)


def record(found: dict[Rule, Violation], violations: Iterable[Violation]) -> None:
    """Keep the first violation of each rule."""
    for violation in violations:
        found.setdefault(violation.rule, violation)


def explore(
    scenario: Scenario, rules: Rules, max_states: int = MAX_STATES
) -> ExploreResult:
    """Visit every state that the case can reach under `rules`, then check waits."""
    initial = initial_state(scenario)
    index: dict[ModelState, int] = {initial: 0}
    states: list[ModelState] = [initial]
    edges: list[set[int]] = [set()]
    found: dict[Rule, Violation] = {}
    record(found, check_state(scenario, initial))
    queue: deque[int] = deque([0])
    while queue:
        current = queue.popleft()
        for step in next_steps(scenario, rules, states[current]):
            record(found, step.violations)
            target = index.get(step.state)
            if target is None:
                if len(states) >= max_states:
                    return StateLimitReached(max_states)
                target = len(states)
                index[step.state] = target
                states.append(step.state)
                edges.append(set())
                record(found, check_state(scenario, step.state))
                queue.append(target)
            edges[current].add(target)
    outcomes = [wait_outcome(states, edges, spec.task_id) for spec in scenario.tasks]
    return Explored(
        state_count=len(states),
        violations=tuple(found.values()),
        starvations=tuple(
            outcome for outcome in outcomes if isinstance(outcome, Starvation)
        ),
        longest_waits=tuple(
            outcome for outcome in outcomes if isinstance(outcome, LongestWait)
        ),
    )
