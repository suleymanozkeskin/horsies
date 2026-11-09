"""Exhaustive checks of the drain and drain-reservation rules.

Each test explores every order of events in one small case. The designed
rules must hold in every case. Three rule sets that each drop one designed
rule must fail on the rule they drop, which shows that the checks can see
each failure. A reservation must also last at least two renewal intervals: a
worker renews at each claim, so a reservation of one claim interval ends
before its holder can renew it.
"""

from dataclasses import replace

import pytest

from tests.unit.drain_model.model import (
    DESIGNED_RULES,
    Arrival,
    AsyncLoad,
    DrainAdmission,
    DrainKeep,
    Explored,
    Life,
    LoadMode,
    LoadSeat,
    Renewal,
    Rule,
    Rules,
    Scenario,
    SeatKind,
    Shared,
    StateLimitReached,
    TaskClass,
    TaskSpec,
    WorkerState,
    explore,
    sorted_seats,
)

SEATS_PER_CHILD = 2
RENEWAL_ROUNDS = 1
RESERVATION_ROUNDS = 2 * RENEWAL_ROUNDS
LOAD_RUN_CHOICES = (2, 4)
STEADY_LOAD_UNITS = 12
LONGEST_LOAD_RUN = max(LOAD_RUN_CHOICES)


def load_child(*rounds: int) -> Shared:
    """Return a shared async child that runs load units with these rounds left."""
    return Shared(SeatKind.ASYNC, sorted_seats(LoadSeat(left) for left in rounds))


BUSY_WORKERS = (
    WorkerState(Life.ALIVE, (load_child(1, 3), load_child(2, 4))),
    WorkerState(Life.ALIVE, (load_child(1, 2), load_child(3, 4))),
)


def exclusive(task_id: int, arrival: Arrival = Arrival.WAITING_AT_START) -> TaskSpec:
    """Return an exclusive task that runs one round."""
    return TaskSpec(task_id, TaskClass.EXCLUSIVE, 1, arrival)


def steady_load_case(
    tasks: tuple[TaskSpec, ...], max_deaths: int, max_stalls: int
) -> Scenario:
    """Two workers whose children stay busy: load ends and enters the queue again."""
    return Scenario(
        name='steady async load',
        seats_per_child=SEATS_PER_CHILD,
        reservation_rounds=RESERVATION_ROUNDS,
        tasks=tasks,
        load=AsyncLoad(STEADY_LOAD_UNITS, LOAD_RUN_CHOICES, LoadMode.CLOSED_LOOP),
        initial_workers=BUSY_WORKERS,
        max_deaths=max_deaths,
        max_stalls=max_stalls,
    )


def higher_ranked_arrival_case() -> Scenario:
    """One worker drains for an exclusive task; a higher-ranked async task may arrive."""
    return Scenario(
        name='higher-ranked arrival during a drain',
        seats_per_child=SEATS_PER_CHILD,
        reservation_rounds=RESERVATION_ROUNDS,
        tasks=(
            TaskSpec(0, TaskClass.SHARED_ASYNC, 1, Arrival.ANY_ROUND),
            exclusive(1),
        ),
        load=AsyncLoad(6, LOAD_RUN_CHOICES, LoadMode.CLOSED_LOOP),
        initial_workers=(
            WorkerState(Life.ALIVE, (load_child(2, 3), load_child(1, 4))),
        ),
        max_deaths=0,
        max_stalls=0,
    )


def mixed_classes_case() -> Scenario:
    """An exclusive task and thread tasks wait behind async work; one worker may die."""
    return Scenario(
        name='exclusive and thread tasks behind async work',
        seats_per_child=SEATS_PER_CHILD,
        reservation_rounds=RESERVATION_ROUNDS,
        tasks=(
            exclusive(0),
            TaskSpec(1, TaskClass.SHARED_THREAD, 2, Arrival.WAITING_AT_START),
            TaskSpec(2, TaskClass.SHARED_THREAD, 2, Arrival.WAITING_AT_START),
            TaskSpec(3, TaskClass.SHARED_THREAD, 2, Arrival.ANY_ROUND),
        ),
        load=AsyncLoad(8, LOAD_RUN_CHOICES, LoadMode.FINITE),
        initial_workers=BUSY_WORKERS,
        max_deaths=1,
        max_stalls=1,
    )


def explored(result: Explored | StateLimitReached) -> Explored:
    """Return the full exploration, or fail when the search hit its limit."""
    assert isinstance(result, Explored), f'the search stopped early: {result}'
    return result


def failed_rules(result: Explored) -> set[Rule]:
    """Return the rules that failed somewhere in the exploration."""
    return {violation.rule for violation in result.violations}


def test_designed_rules_start_an_exclusive_task_after_one_drain() -> None:
    result = explored(explore(steady_load_case((exclusive(0),), 0, 0), DESIGNED_RULES))

    assert result.violations == ()
    assert result.starvations == ()
    assert [(wait.task_id, wait.rounds) for wait in result.longest_waits] == [
        (0, LONGEST_LOAD_RUN + 1)
    ]


def test_designed_rules_hold_when_a_worker_dies() -> None:
    result = explored(explore(steady_load_case((exclusive(0),), 1, 0), DESIGNED_RULES))

    assert result.violations == ()
    assert result.starvations == ()


def test_designed_rules_hold_when_a_worker_misses_a_claim() -> None:
    result = explored(explore(steady_load_case((exclusive(0),), 0, 1), DESIGNED_RULES))

    assert result.violations == ()
    assert result.starvations == ()


def test_designed_rules_give_each_exclusive_task_at_most_one_drain_of_waiting() -> None:
    tasks = (exclusive(0), exclusive(1))

    result = explored(explore(steady_load_case(tasks, 0, 0), DESIGNED_RULES))

    assert result.violations == ()
    assert result.starvations == ()
    assert all(wait.rounds <= LONGEST_LOAD_RUN + 1 for wait in result.longest_waits)


def test_designed_rules_let_a_higher_ranked_task_into_a_draining_child() -> None:
    result = explored(explore(higher_ranked_arrival_case(), DESIGNED_RULES))

    assert result.violations == ()
    assert result.starvations == ()


def test_designed_rules_hold_for_exclusive_and_thread_tasks_behind_async_work() -> None:
    result = explored(explore(mixed_classes_case(), DESIGNED_RULES))

    assert result.violations == ()
    assert result.starvations == ()


def test_without_renewal_a_worker_keeps_a_drain_after_its_reservation_ends() -> None:
    rules = Rules(
        renewal=Renewal.NEVER,
        keep=DrainKeep.WHILE_SERVED_TASK_WAITS,
        admission=DrainAdmission.HIGHER_RANKED_TASKS,
    )

    result = explored(explore(steady_load_case((exclusive(0),), 0, 0), rules))

    assert failed_rules(result) == {Rule.DRAIN_HELD, Rule.DRAIN_COUNT}
    assert result.starvations == ()


def test_without_renewal_drains_move_between_workers_and_the_task_can_wait_forever() -> (
    None
):
    rules = Rules(
        renewal=Renewal.NEVER,
        keep=DrainKeep.WHILE_HOLDING_RESERVATION,
        admission=DrainAdmission.HIGHER_RANKED_TASKS,
    )

    result = explored(explore(steady_load_case((exclusive(0),), 0, 0), rules))

    assert result.violations == ()
    assert [starvation.task_id for starvation in result.starvations] == [0]


def test_without_admission_a_higher_ranked_task_waits_beside_a_free_draining_seat() -> (
    None
):
    rules = Rules(
        renewal=Renewal.EVERY_CLAIM,
        keep=DrainKeep.WHILE_HOLDING_RESERVATION,
        admission=DrainAdmission.NONE,
    )

    result = explored(explore(higher_ranked_arrival_case(), rules))

    assert failed_rules(result) == {Rule.PLACEMENT_COMPLETE}
    assert result.starvations == ()


def test_a_reservation_of_one_claim_interval_lets_the_task_wait_forever() -> None:
    case = replace(
        steady_load_case((exclusive(0),), 0, 0), reservation_rounds=RENEWAL_ROUNDS
    )

    result = explored(explore(case, DESIGNED_RULES))

    assert result.violations == ()
    assert [starvation.task_id for starvation in result.starvations] == [0]


def test_search_stops_at_the_state_limit() -> None:
    result = explore(
        steady_load_case((exclusive(0),), 0, 0), DESIGNED_RULES, max_states=50
    )

    assert result == StateLimitReached(50)


def test_waiting_load_beside_a_thread_child_is_outside_the_model() -> None:
    tasks = (TaskSpec(0, TaskClass.SHARED_THREAD, 4, Arrival.WAITING_AT_START),)
    idle_worker = WorkerState(Life.ALIVE, (load_child(1), load_child(2)))
    case = Scenario(
        name='load beside a thread child',
        seats_per_child=SEATS_PER_CHILD,
        reservation_rounds=RESERVATION_ROUNDS,
        tasks=tasks,
        load=AsyncLoad(6, LOAD_RUN_CHOICES, LoadMode.CLOSED_LOOP),
        initial_workers=(idle_worker,),
        max_deaths=0,
        max_stalls=0,
    )

    with pytest.raises(AssertionError, match='cannot reserve load'):
        explore(case, DESIGNED_RULES)
