"""The default sizes that a child derives from its seats: the rule, and the
constants that each kind binds to it."""

from __future__ import annotations

import pytest

from horsies.core.models import child_sizing
from horsies.core.models.child_sizing import (
    EXECUTOR_THREADS_FLOOR,
    POOL_CONNECTIONS_FLOOR,
    SHARED_ASYNC_EXECUTOR_THREADS_CAP,
    SHARED_ASYNC_POOL_CONNECTIONS_CAP,
    SHARED_ASYNC_SEATS_PER_EXECUTOR_THREAD,
    SHARED_ASYNC_SEATS_PER_POOL_CONNECTION,
    SHARED_THREAD_POOL_CONNECTIONS_CAP,
    SHARED_THREAD_SEATS_PER_POOL_CONNECTION,
    shared_async_executor_threads,
    shared_async_pool_connections,
    shared_thread_pool_connections,
    size_from_seats,
)
from horsies.core.models.worker_profile import (
    MAX_CHILD_POOL_CONNECTIONS,
    MAX_EXECUTOR_THREADS,
    MAX_SEATS_PER_SHARED_ASYNC_CHILD,
    MAX_SEATS_PER_SHARED_THREAD_CHILD,
)

pytestmark = [pytest.mark.unit]


class TestSizeFromSeats:
    @pytest.mark.parametrize(
        ('seats', 'expected'),
        [
            (1, 2),  # ceil(1 / 10) = 1, raised to the floor
            (20, 2),  # ceil(20 / 10) = 2, at the floor
            (21, 3),  # ceil(21 / 10) = 3: a part of a unit counts as a unit
            (50, 5),
            (80, 8),  # at the cap
            (81, 8),  # ceil(81 / 10) = 9, cut to the cap
            (1000, 8),
        ],
    )
    def test_the_rule(self, seats: int, expected: int) -> None:
        assert size_from_seats(seats, seats_per_unit=10, floor=2, cap=8) == expected

    def test_one_seat_per_unit_follows_the_seats(self) -> None:
        assert [
            size_from_seats(seats, 1, 1, 100) for seats in (1, 2, 99, 100, 101)
        ] == [
            1,
            2,
            99,
            100,
            100,
        ]

    def test_a_floor_equal_to_the_cap_is_one_size(self) -> None:
        assert {size_from_seats(seats, 3, 4, 4) for seats in range(1, 50)} == {4}

    @pytest.mark.parametrize(
        ('seats', 'seats_per_unit', 'floor', 'cap'),
        [
            (0, 1, 1, 1),
            (1, 0, 1, 1),
            (1, 1, 0, 1),
            (1, 1, 3, 2),
        ],
    )
    def test_an_input_out_of_its_bounds_is_a_defect(
        self,
        seats: int,
        seats_per_unit: int,
        floor: int,
        cap: int,
    ) -> None:
        with pytest.raises(AssertionError):
            size_from_seats(seats, seats_per_unit, floor, cap)


class TestEachKindBindsItsConstants:
    """Each kind applies the rule with its own constants. Checked over every
    valid seat count, so a constant that one function does not use fails
    here."""

    def test_shared_async_pool(self) -> None:
        for seats in range(1, MAX_SEATS_PER_SHARED_ASYNC_CHILD + 1):
            assert shared_async_pool_connections(seats) == size_from_seats(
                seats,
                SHARED_ASYNC_SEATS_PER_POOL_CONNECTION,
                POOL_CONNECTIONS_FLOOR,
                SHARED_ASYNC_POOL_CONNECTIONS_CAP,
            )

    def test_shared_thread_pool(self) -> None:
        for seats in range(1, MAX_SEATS_PER_SHARED_THREAD_CHILD + 1):
            assert shared_thread_pool_connections(seats) == size_from_seats(
                seats,
                SHARED_THREAD_SEATS_PER_POOL_CONNECTION,
                POOL_CONNECTIONS_FLOOR,
                SHARED_THREAD_POOL_CONNECTIONS_CAP,
            )

    def test_shared_async_executor(self) -> None:
        for seats in range(1, MAX_SEATS_PER_SHARED_ASYNC_CHILD + 1):
            assert shared_async_executor_threads(seats) == size_from_seats(
                seats,
                SHARED_ASYNC_SEATS_PER_EXECUTOR_THREAD,
                EXECUTOR_THREADS_FLOOR,
                SHARED_ASYNC_EXECUTOR_THREADS_CAP,
            )

    def test_the_cap_is_reached_within_the_seat_bound(self) -> None:
        """Each cap is a size that some valid seat count derives, so no cap
        is above what the seats can ask for."""
        assert (
            shared_async_pool_connections(MAX_SEATS_PER_SHARED_ASYNC_CHILD)
            == SHARED_ASYNC_POOL_CONNECTIONS_CAP
        )
        assert (
            shared_thread_pool_connections(MAX_SEATS_PER_SHARED_THREAD_CHILD)
            == SHARED_THREAD_POOL_CONNECTIONS_CAP
        )
        assert (
            shared_async_executor_threads(MAX_SEATS_PER_SHARED_ASYNC_CHILD)
            == SHARED_ASYNC_EXECUTOR_THREADS_CAP
        )


class TestConstantsFitTheBounds:
    def test_a_derived_pool_has_at_least_two_connections(self) -> None:
        assert POOL_CONNECTIONS_FLOOR == 2

    def test_each_pool_cap_is_within_the_pool_bound(self) -> None:
        for cap in (
            SHARED_ASYNC_POOL_CONNECTIONS_CAP,
            SHARED_THREAD_POOL_CONNECTIONS_CAP,
        ):
            assert POOL_CONNECTIONS_FLOOR <= cap <= MAX_CHILD_POOL_CONNECTIONS

    def test_the_executor_cap_is_within_the_executor_bound(self) -> None:
        assert (
            1
            <= EXECUTOR_THREADS_FLOOR
            <= SHARED_ASYNC_EXECUTOR_THREADS_CAP
            <= MAX_EXECUTOR_THREADS
        )

    def test_every_sizing_constant_is_a_positive_whole_number(self) -> None:
        names = [name for name in vars(child_sizing) if name.isupper()]
        assert names, 'the module holds the sizing constants'
        for name in names:
            value = getattr(child_sizing, name)
            assert isinstance(value, int) and not isinstance(value, bool), name
            assert value >= 1, name
