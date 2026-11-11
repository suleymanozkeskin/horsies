"""Worker profiles: the bounds of each type, each named variant, and the
profile checks that ``AppConfig`` runs with the profile name and field path."""

from __future__ import annotations

from datetime import timedelta
from typing import Any

import pytest
from pydantic import BaseModel, SecretStr, ValidationError

import horsies
from horsies.core.errors import (
    ConfigurationError,
    ErrorCode,
    HorsiesError,
    MultipleValidationErrors,
)
from horsies.core.models.app import AppConfig
from horsies.core.models.broker import PostgresConfig
from horsies.core.models.child_sizing import (
    DEFAULT_ISOLATED_POOL_CONNECTIONS,
    DEFAULT_TASK_SEND_POOL_CONNECTIONS,
    PARENT_POOL_CONNECTIONS_FLOOR,
    parent_pool_connections,
    shared_async_executor_threads,
    shared_async_pool_connections,
    shared_thread_pool_connections,
)
from horsies.core.models.worker_profile import (
    DEFAULT_DRAIN_TIMEOUT,
    DEFAULT_RECYCLE_AFTER_TASKS,
    DEFAULT_RECYCLE_AFTER_TASKS_PER_SEAT,
    MAX_CHILD_POOL_CONNECTIONS,
    MAX_DRAIN_TIMEOUT,
    MAX_EXECUTOR_THREADS,
    MAX_ISOLATED_CHILDREN,
    MAX_MEMORY_LIMIT_MB,
    MAX_SEATS_PER_SHARED_ASYNC_CHILD,
    MAX_SEATS_PER_SHARED_THREAD_CHILD,
    MAX_SHARED_ASYNC_CHILDREN,
    MAX_SHARED_THREAD_CHILDREN,
    MAX_TASK_LIMIT,
    PROFILE_NAME_MAX_LENGTH,
    ClaimLimits,
    ExecutorThreads,
    ExecutorThreadsFromSeats,
    IsolatedChildren,
    IsolatedPool,
    IsolatedRecycle,
    MemoryLimit,
    NoIsolatedChildren,
    NoMemoryLimit,
    NoSharedAsyncChildren,
    NoSharedThreadChildren,
    NoTaskLimit,
    ParentPool,
    PoolSize,
    PoolSizeFromSeats,
    ResolvedPool,
    SharedAsyncChildren,
    SharedPool,
    SharedRecycle,
    SharedThreadChildren,
    TaskLimit,
    WorkerProfile,
    describe_worker_profile,
)
from horsies.core.worker.config import ChildPreload

pytestmark = [pytest.mark.unit]


def _app_config(
    profiles: dict[str, WorkerProfile],
    **overrides: Any,
) -> AppConfig:
    return AppConfig(
        broker=PostgresConfig(
            database_url=SecretStr('postgresql+psycopg://user:pass@localhost/db'),
        ),
        worker_profiles=profiles,
        **overrides,
    )


def _errors_of(raised: HorsiesError) -> list[HorsiesError]:
    match raised:
        case MultipleValidationErrors(report=report):
            return list(report.errors)
        case _:
            return [raised]


def _isolated(count: int, **claim: TaskLimit | NoTaskLimit) -> WorkerProfile:
    return WorkerProfile(
        isolated=IsolatedChildren(count=count),
        claim=ClaimLimits(**claim),
    )


class TestTaskLimitBounds:
    @pytest.mark.parametrize('tasks', [0, -3])
    def test_below_one_is_refused(self, tasks: int) -> None:
        with pytest.raises(ValidationError, match='greater than or equal to 1'):
            TaskLimit(tasks=tasks)

    @pytest.mark.parametrize('tasks', [1, 2, 50, MAX_TASK_LIMIT])
    def test_one_to_max_is_accepted(self, tasks: int) -> None:
        assert TaskLimit(tasks=tasks).tasks == tasks

    def test_above_max_is_refused(self) -> None:
        with pytest.raises(ValidationError, match='less than or equal to'):
            TaskLimit(tasks=MAX_TASK_LIMIT + 1)


class TestMemoryLimitBounds:
    @pytest.mark.parametrize('mb', [0, -1])
    def test_below_one_is_refused(self, mb: int) -> None:
        with pytest.raises(ValidationError, match='greater than or equal to 1'):
            MemoryLimit(mb=mb)

    @pytest.mark.parametrize('mb', [1, 1024, MAX_MEMORY_LIMIT_MB])
    def test_one_to_max_is_accepted(self, mb: int) -> None:
        assert MemoryLimit(mb=mb).mb == mb

    def test_above_max_is_refused(self) -> None:
        with pytest.raises(ValidationError, match='less than or equal to'):
            MemoryLimit(mb=MAX_MEMORY_LIMIT_MB + 1)


class TestIsolatedChildrenBounds:
    @pytest.mark.parametrize('count', [0, -2])
    def test_below_one_is_refused(self, count: int) -> None:
        with pytest.raises(ValidationError, match='greater than or equal to 1'):
            IsolatedChildren(count=count)

    @pytest.mark.parametrize('count', [1, 4, MAX_ISOLATED_CHILDREN])
    def test_one_to_max_is_accepted(self, count: int) -> None:
        assert IsolatedChildren(count=count).count == count

    def test_above_max_is_refused(self) -> None:
        with pytest.raises(ValidationError, match='less than or equal to'):
            IsolatedChildren(count=MAX_ISOLATED_CHILDREN + 1)

    def test_count_is_required(self) -> None:
        """There is no default number of children."""
        with pytest.raises(ValidationError, match='count'):
            IsolatedChildren()  # pyright: ignore[reportCallIssue]


class TestDrainTimeoutBounds:
    def test_seconds_are_accepted(self) -> None:
        profile = WorkerProfile(drain_timeout=timedelta(seconds=2.5))
        assert profile.drain_timeout == timedelta(seconds=2.5)

    @pytest.mark.parametrize(
        'value',
        [timedelta(0), timedelta(seconds=-1), float('nan'), float('inf')],
    )
    def test_a_value_with_no_deadline_is_refused(self, value: Any) -> None:
        with pytest.raises(ValidationError, match='drain_timeout'):
            WorkerProfile(drain_timeout=value)

    def test_max_is_accepted(self) -> None:
        assert WorkerProfile(drain_timeout=MAX_DRAIN_TIMEOUT).drain_timeout == (
            MAX_DRAIN_TIMEOUT
        )

    def test_above_max_is_refused(self) -> None:
        with pytest.raises(ValidationError, match='less than or equal to'):
            WorkerProfile(drain_timeout=MAX_DRAIN_TIMEOUT + timedelta(seconds=1))


class TestSharedAsyncChildrenBounds:
    @pytest.mark.parametrize('count', [0, -1])
    def test_a_count_below_one_is_refused(self, count: int) -> None:
        with pytest.raises(ValidationError, match='greater than or equal to 1'):
            SharedAsyncChildren(count=count, seats=1)

    def test_a_count_above_max_is_refused(self) -> None:
        with pytest.raises(ValidationError, match='less than or equal to'):
            SharedAsyncChildren(count=MAX_SHARED_ASYNC_CHILDREN + 1, seats=1)

    @pytest.mark.parametrize('seats', [0, -2])
    def test_seats_below_one_are_refused(self, seats: int) -> None:
        with pytest.raises(ValidationError, match='greater than or equal to 1'):
            SharedAsyncChildren(count=1, seats=seats)

    def test_seats_above_max_are_refused(self) -> None:
        with pytest.raises(ValidationError, match='less than or equal to'):
            SharedAsyncChildren(count=1, seats=MAX_SEATS_PER_SHARED_ASYNC_CHILD + 1)

    def test_the_bounds_are_accepted(self) -> None:
        shared = SharedAsyncChildren(
            count=MAX_SHARED_ASYNC_CHILDREN,
            seats=MAX_SEATS_PER_SHARED_ASYNC_CHILD,
        )
        assert (shared.count, shared.seats) == (1024, 1000)

    def test_seats_are_required(self) -> None:
        with pytest.raises(ValidationError, match='seats'):
            SharedAsyncChildren.model_validate({'count': 1})


class TestSharedThreadChildrenBounds:
    @pytest.mark.parametrize('count', [0, -1])
    def test_a_count_below_one_is_refused(self, count: int) -> None:
        with pytest.raises(ValidationError, match='greater than or equal to 1'):
            SharedThreadChildren(count=count, seats=1)

    def test_a_count_above_max_is_refused(self) -> None:
        with pytest.raises(ValidationError, match='less than or equal to'):
            SharedThreadChildren(count=MAX_SHARED_THREAD_CHILDREN + 1, seats=1)

    @pytest.mark.parametrize('seats', [0, -2])
    def test_seats_below_one_are_refused(self, seats: int) -> None:
        with pytest.raises(ValidationError, match='greater than or equal to 1'):
            SharedThreadChildren(count=1, seats=seats)

    def test_seats_above_max_are_refused(self) -> None:
        with pytest.raises(ValidationError, match='less than or equal to'):
            SharedThreadChildren(count=1, seats=MAX_SEATS_PER_SHARED_THREAD_CHILD + 1)

    def test_the_bounds_are_accepted(self) -> None:
        shared = SharedThreadChildren(
            count=MAX_SHARED_THREAD_CHILDREN,
            seats=MAX_SEATS_PER_SHARED_THREAD_CHILD,
        )
        assert (shared.count, shared.seats) == (1024, 256)

    def test_seats_are_required(self) -> None:
        with pytest.raises(ValidationError, match='seats'):
            SharedThreadChildren.model_validate({'count': 1})

    def test_the_default_recycle_is_the_shared_default(self) -> None:
        assert SharedThreadChildren(count=1, seats=2).recycle == SharedRecycle()

    @pytest.mark.parametrize(
        ('data', 'expected'),
        [
            (
                {'kind': 'shared_thread_children', 'count': 2, 'seats': 8},
                SharedThreadChildren(count=2, seats=8),
            ),
            ({'kind': 'no_shared_thread_children'}, NoSharedThreadChildren()),
        ],
    )
    def test_the_variants_parse_by_kind(
        self, data: dict[str, Any], expected: Any
    ) -> None:
        profile = WorkerProfile.model_validate({'shared_thread': data})
        assert profile.shared_thread == expected

    def test_a_profile_starts_no_thread_children_by_default(self) -> None:
        assert WorkerProfile().shared_thread == NoSharedThreadChildren()


class TestDefaults:
    def test_shared_recycle_defaults_to_100_tasks_per_seat_and_no_memory_limit(
        self,
    ) -> None:
        recycle = SharedRecycle()
        assert recycle.after_per_seat == TaskLimit(
            tasks=DEFAULT_RECYCLE_AFTER_TASKS_PER_SEAT,
        )
        assert DEFAULT_RECYCLE_AFTER_TASKS_PER_SEAT == 100
        assert recycle.above == NoMemoryLimit()

    def test_profile_starts_no_shared_async_children_by_default(self) -> None:
        assert WorkerProfile().shared_async == NoSharedAsyncChildren()

    def test_recycle_defaults_to_after_100_tasks_and_no_memory_limit(self) -> None:
        recycle = IsolatedRecycle()
        assert recycle.after == TaskLimit(tasks=DEFAULT_RECYCLE_AFTER_TASKS)
        assert DEFAULT_RECYCLE_AFTER_TASKS == 100
        assert recycle.above == NoMemoryLimit()

    def test_claim_limits_default_to_no_limit(self) -> None:
        claim = ClaimLimits()
        assert claim.per_queue_per_pass == NoTaskLimit()
        assert claim.claimed_per_worker == NoTaskLimit()

    def test_profile_defaults(self) -> None:
        profile = WorkerProfile()
        assert profile.isolated == NoIsolatedChildren()
        assert profile.drain_timeout == DEFAULT_DRAIN_TIMEOUT
        assert DEFAULT_DRAIN_TIMEOUT == timedelta(seconds=300)
        assert profile.preload is ChildPreload.LIBRARY


class TestNamedVariants:
    """Each variant parses from its own ``kind``, so a dict config (for
    example from a file) reaches the same type as a Python constructor."""

    @pytest.mark.parametrize(
        ('data', 'expected'),
        [
            ({'kind': 'task_limit', 'tasks': 5}, TaskLimit(tasks=5)),
            ({'kind': 'no_task_limit'}, NoTaskLimit()),
        ],
    )
    def test_task_limit_variants(self, data: dict[str, Any], expected: Any) -> None:
        claim = ClaimLimits.model_validate({'claimed_per_worker': data})
        assert claim.claimed_per_worker == expected

    @pytest.mark.parametrize(
        ('data', 'expected'),
        [
            ({'kind': 'memory_limit', 'mb': 512}, MemoryLimit(mb=512)),
            ({'kind': 'no_memory_limit'}, NoMemoryLimit()),
        ],
    )
    def test_memory_limit_variants(
        self,
        data: dict[str, Any],
        expected: Any,
    ) -> None:
        recycle = IsolatedRecycle.model_validate({'above': data})
        assert recycle.above == expected

    @pytest.mark.parametrize(
        ('data', 'expected'),
        [
            (
                {'kind': 'isolated_children', 'count': 3},
                IsolatedChildren(count=3),
            ),
            ({'kind': 'no_isolated_children'}, NoIsolatedChildren()),
        ],
    )
    def test_isolated_variants(self, data: dict[str, Any], expected: Any) -> None:
        profile = WorkerProfile.model_validate({'isolated': data})
        assert profile.isolated == expected

    @pytest.mark.parametrize(
        ('data', 'expected'),
        [
            (
                {'kind': 'shared_async_children', 'count': 2, 'seats': 9},
                SharedAsyncChildren(count=2, seats=9),
            ),
            ({'kind': 'no_shared_async_children'}, NoSharedAsyncChildren()),
        ],
    )
    def test_shared_async_variants(self, data: dict[str, Any], expected: Any) -> None:
        profile = WorkerProfile.model_validate({'shared_async': data})
        assert profile.shared_async == expected

    def test_a_dict_error_names_the_field_path(self) -> None:
        with pytest.raises(ValidationError) as raised:
            WorkerProfile.model_validate(
                {'isolated': {'kind': 'isolated_children', 'count': 0}},
            )
        assert raised.value.errors()[0]['loc'] == (
            'isolated',
            'isolated_children',
            'count',
        )

    @pytest.mark.parametrize(
        'model',
        [
            TaskLimit(tasks=1),
            NoTaskLimit(),
            MemoryLimit(mb=1),
            NoMemoryLimit(),
            IsolatedRecycle(),
            IsolatedChildren(count=1),
            NoIsolatedChildren(),
            SharedRecycle(),
            SharedAsyncChildren(count=1, seats=1),
            NoSharedAsyncChildren(),
            SharedThreadChildren(count=1, seats=1),
            NoSharedThreadChildren(),
            ClaimLimits(),
            WorkerProfile(),
            PoolSize(connections=1),
            PoolSizeFromSeats(),
            IsolatedPool(),
            SharedPool(),
            ParentPool(),
            ExecutorThreads(threads=1),
            ExecutorThreadsFromSeats(),
        ],
    )
    def test_every_profile_type_is_frozen(self, model: BaseModel) -> None:
        name = next(iter(type(model).model_fields))
        with pytest.raises(ValidationError, match='frozen'):
            setattr(model, name, getattr(model, name))

    def test_child_count_is_the_isolated_count(self) -> None:
        assert WorkerProfile(isolated=IsolatedChildren(count=3)).child_count() == 3

    def test_child_count_of_no_isolated_children_is_zero(self) -> None:
        assert WorkerProfile().child_count() == 0

    def test_child_count_is_the_sum_over_the_modes(self) -> None:
        profile = WorkerProfile(
            isolated=IsolatedChildren(count=3),
            shared_async=SharedAsyncChildren(count=2, seats=50),
        )
        assert profile.child_count() == 5

    def test_seat_count_is_one_per_isolated_child_and_seats_per_shared_child(
        self,
    ) -> None:
        profile = WorkerProfile(
            isolated=IsolatedChildren(count=3),
            shared_async=SharedAsyncChildren(count=2, seats=50),
        )
        assert profile.seat_count() == 103

    def test_counts_include_the_shared_thread_children(self) -> None:
        profile = WorkerProfile(
            isolated=IsolatedChildren(count=3),
            shared_async=SharedAsyncChildren(count=2, seats=50),
            shared_thread=SharedThreadChildren(count=4, seats=8),
        )
        assert profile.child_count() == 9
        assert profile.seat_count() == 3 + 100 + 32

    def test_a_profile_with_only_thread_children_has_children(self) -> None:
        profile = WorkerProfile(shared_thread=SharedThreadChildren(count=1, seats=4))
        assert (profile.child_count(), profile.seat_count()) == (1, 4)

    def test_seat_count_of_isolated_children_is_their_count(self) -> None:
        assert WorkerProfile(isolated=IsolatedChildren(count=4)).seat_count() == 4

    def test_public_names_are_exported(self) -> None:
        for name in (
            'WorkerProfile',
            'IsolatedChildren',
            'NoIsolatedChildren',
            'IsolatedRecycle',
            'SharedAsyncChildren',
            'NoSharedAsyncChildren',
            'SharedThreadChildren',
            'NoSharedThreadChildren',
            'SharedRecycle',
            'TaskLimit',
            'NoTaskLimit',
            'MemoryLimit',
            'NoMemoryLimit',
            'ClaimLimits',
            'ChildPreload',
            'IsolatedPool',
            'SharedPool',
            'ParentPool',
            'PoolSize',
            'PoolSizeFromSeats',
            'ExecutorThreads',
            'ExecutorThreadsFromSeats',
        ):
            assert name in horsies.__all__
            assert getattr(horsies, name) is not None


class TestPoolSizeBounds:
    @pytest.mark.parametrize('connections', [0, -1])
    def test_below_one_is_refused(self, connections: int) -> None:
        with pytest.raises(ValidationError, match='greater than or equal to 1'):
            PoolSize(connections=connections)

    @pytest.mark.parametrize('connections', [1, 2, MAX_CHILD_POOL_CONNECTIONS])
    def test_one_to_max_is_accepted(self, connections: int) -> None:
        assert PoolSize(connections=connections).connections == connections

    def test_above_max_is_refused(self) -> None:
        with pytest.raises(ValidationError, match='less than or equal to'):
            PoolSize(connections=MAX_CHILD_POOL_CONNECTIONS + 1)

    def test_connections_are_required(self) -> None:
        with pytest.raises(ValidationError, match='connections'):
            PoolSize.model_validate({})


class TestExecutorThreadsBounds:
    @pytest.mark.parametrize('threads', [0, -1])
    def test_below_one_is_refused(self, threads: int) -> None:
        with pytest.raises(ValidationError, match='greater than or equal to 1'):
            ExecutorThreads(threads=threads)

    @pytest.mark.parametrize('threads', [1, 32, MAX_EXECUTOR_THREADS])
    def test_one_to_max_is_accepted(self, threads: int) -> None:
        assert ExecutorThreads(threads=threads).threads == threads

    def test_above_max_is_refused(self) -> None:
        with pytest.raises(ValidationError, match='less than or equal to'):
            ExecutorThreads(threads=MAX_EXECUTOR_THREADS + 1)


class TestChildPoolMinSizeBounds:
    @pytest.mark.parametrize('pool_type', [IsolatedPool, SharedPool])
    def test_below_zero_is_refused(
        self,
        pool_type: type[IsolatedPool] | type[SharedPool],
    ) -> None:
        with pytest.raises(ValidationError, match='greater than or equal to 0'):
            pool_type(min_size=-1)

    @pytest.mark.parametrize('pool_type', [IsolatedPool, SharedPool])
    @pytest.mark.parametrize('min_size', [0, 1, MAX_CHILD_POOL_CONNECTIONS])
    def test_zero_to_max_is_accepted(
        self,
        pool_type: type[IsolatedPool] | type[SharedPool],
        min_size: int,
    ) -> None:
        assert pool_type(min_size=min_size).min_size == min_size

    @pytest.mark.parametrize('pool_type', [IsolatedPool, SharedPool])
    def test_above_max_is_refused(
        self,
        pool_type: type[IsolatedPool] | type[SharedPool],
    ) -> None:
        with pytest.raises(ValidationError, match='less than or equal to'):
            pool_type(min_size=MAX_CHILD_POOL_CONNECTIONS + 1)


class TestChildPoolDefaults:
    def test_an_isolated_child_keeps_a_pool_of_two(self) -> None:
        """The isolated default is unchanged from 0.6: lazy, at most 2,
        checked on checkout."""
        assert DEFAULT_ISOLATED_POOL_CONNECTIONS == 2
        assert IsolatedChildren(count=1).pool == IsolatedPool(
            min_size=0,
            max_size=PoolSize(connections=2),
            check=True,
        )
        assert IsolatedChildren(count=1).resolved_pool() == ResolvedPool(
            min_size=0,
            max_size=2,
            check=True,
        )

    def test_a_shared_async_child_derives_its_sizes_from_its_seats(self) -> None:
        children = SharedAsyncChildren(count=1, seats=40)
        assert children.pool == SharedPool(
            min_size=0,
            max_size=PoolSizeFromSeats(),
            check=True,
        )
        assert children.executor == ExecutorThreadsFromSeats()

    def test_a_shared_thread_child_derives_its_pool_from_its_seats(self) -> None:
        children = SharedThreadChildren(count=1, seats=40)
        assert children.pool == SharedPool(
            min_size=0,
            max_size=PoolSizeFromSeats(),
            check=True,
        )


class TestChildPoolVariants:
    @pytest.mark.parametrize(
        ('data', 'expected'),
        [
            ({'kind': 'pool_size', 'connections': 3}, PoolSize(connections=3)),
            ({'kind': 'pool_size_from_seats'}, PoolSizeFromSeats()),
        ],
    )
    def test_a_shared_pool_size_parses_by_kind(
        self,
        data: dict[str, Any],
        expected: PoolSize | PoolSizeFromSeats,
    ) -> None:
        assert SharedPool.model_validate({'max_size': data}).max_size == expected

    def test_an_isolated_pool_has_no_size_from_seats(self) -> None:
        """An isolated child has one seat: its pool size is always stated."""
        with pytest.raises(ValidationError, match='pool_size'):
            IsolatedPool.model_validate({'max_size': {'kind': 'pool_size_from_seats'}})

    @pytest.mark.parametrize(
        ('data', 'expected'),
        [
            (
                {'kind': 'executor_threads', 'threads': 8},
                ExecutorThreads(threads=8),
            ),
            ({'kind': 'executor_threads_from_seats'}, ExecutorThreadsFromSeats()),
        ],
    )
    def test_the_executor_parses_by_kind(
        self,
        data: dict[str, Any],
        expected: ExecutorThreads | ExecutorThreadsFromSeats,
    ) -> None:
        children = SharedAsyncChildren.model_validate(
            {'count': 1, 'seats': 4, 'executor': data},
        )
        assert children.executor == expected

    def test_a_shared_thread_child_has_no_executor_field(self) -> None:
        with pytest.raises(ValidationError, match='executor'):
            SharedThreadChildren.model_validate(
                {
                    'count': 1,
                    'seats': 4,
                    'executor': {'kind': 'executor_threads', 'threads': 8},
                },
            )


class TestChildPoolResolution:
    @pytest.mark.parametrize(
        'seats',
        [1, 9, 10, 11, 95, MAX_SEATS_PER_SHARED_ASYNC_CHILD],
    )
    def test_a_shared_async_pool_from_seats_is_derived(self, seats: int) -> None:
        children = SharedAsyncChildren(
            count=1,
            seats=seats,
            pool=SharedPool(min_size=1, check=False),
        )
        assert children.resolved_pool() == ResolvedPool(
            min_size=1,
            max_size=shared_async_pool_connections(seats),
            check=False,
        )

    @pytest.mark.parametrize(
        'seats',
        [1, 3, 4, 5, 39, MAX_SEATS_PER_SHARED_THREAD_CHILD],
    )
    def test_a_shared_thread_pool_from_seats_is_derived(self, seats: int) -> None:
        children = SharedThreadChildren(count=1, seats=seats)
        assert children.resolved_pool() == ResolvedPool(
            min_size=0,
            max_size=shared_thread_pool_connections(seats),
            check=True,
        )

    def test_a_set_pool_size_is_kept_whatever_the_seats(self) -> None:
        pool = SharedPool(max_size=PoolSize(connections=17))
        assert (
            SharedAsyncChildren(count=1, seats=1, pool=pool).resolved_pool().max_size
            == 17
        )
        assert (
            SharedThreadChildren(count=1, seats=200, pool=pool).resolved_pool().max_size
            == 17
        )

    @pytest.mark.parametrize(
        'seats',
        [1, 2, 31, 32, 33, MAX_SEATS_PER_SHARED_ASYNC_CHILD],
    )
    def test_the_executor_from_seats_is_derived(self, seats: int) -> None:
        children = SharedAsyncChildren(count=1, seats=seats)
        assert children.resolved_executor_threads() == shared_async_executor_threads(
            seats,
        )

    def test_a_set_executor_size_is_kept(self) -> None:
        children = SharedAsyncChildren(
            count=1,
            seats=500,
            executor=ExecutorThreads(threads=3),
        )
        assert children.resolved_executor_threads() == 3

    def test_an_isolated_pool_resolves_to_its_set_values(self) -> None:
        pool = IsolatedPool(min_size=2, max_size=PoolSize(connections=5), check=False)
        assert IsolatedChildren(count=1, pool=pool).resolved_pool() == ResolvedPool(
            min_size=2,
            max_size=5,
            check=False,
        )


class TestParentPool:
    """``WorkerProfile.pool``: the pool of the worker process. Its default
    derives the size from the seats of the profile."""

    def test_the_default_derives_from_the_seats(self) -> None:
        assert WorkerProfile().pool == ParentPool(max_size=PoolSizeFromSeats())

    @pytest.mark.parametrize(
        ('data', 'expected'),
        [
            ({'kind': 'pool_size', 'connections': 12}, PoolSize(connections=12)),
            ({'kind': 'pool_size_from_seats'}, PoolSizeFromSeats()),
        ],
    )
    def test_the_size_parses_by_kind(
        self,
        data: dict[str, Any],
        expected: PoolSize | PoolSizeFromSeats,
    ) -> None:
        profile = WorkerProfile.model_validate({'pool': {'max_size': data}})
        assert profile.pool.max_size == expected

    def test_an_unknown_field_is_refused(self) -> None:
        with pytest.raises(ValidationError, match='max_overflow'):
            ParentPool.model_validate({'max_overflow': 2})

    @pytest.mark.parametrize(
        ('profile', 'seats'),
        [
            (WorkerProfile(isolated=IsolatedChildren(count=8)), 8),
            (WorkerProfile(isolated=IsolatedChildren(count=64)), 64),
            (
                WorkerProfile(shared_async=SharedAsyncChildren(count=1, seats=100)),
                100,
            ),
            (
                WorkerProfile(shared_async=SharedAsyncChildren(count=1, seats=1000)),
                1000,
            ),
            (
                WorkerProfile(
                    isolated=IsolatedChildren(count=2),
                    shared_async=SharedAsyncChildren(count=3, seats=40),
                    shared_thread=SharedThreadChildren(count=2, seats=16),
                ),
                2 + 120 + 32,
            ),
        ],
    )
    def test_a_size_from_seats_counts_every_seat_of_the_profile(
        self,
        profile: WorkerProfile,
        seats: int,
    ) -> None:
        assert profile.seat_count() == seats
        assert profile.resolved_parent_pool() == parent_pool_connections(seats)

    def test_the_examples_of_the_rule(self) -> None:
        """64 isolated: 7; one async child of 100 seats: 10; one of 1000
        seats: 20; one thread child of 256 seats: 20."""
        assert [
            profile.resolved_parent_pool()
            for profile in (
                WorkerProfile(isolated=IsolatedChildren(count=64)),
                WorkerProfile(shared_async=SharedAsyncChildren(count=1, seats=100)),
                WorkerProfile(shared_async=SharedAsyncChildren(count=1, seats=1000)),
                WorkerProfile(shared_thread=SharedThreadChildren(count=1, seats=256)),
            )
        ] == [7, 10, 20, 20]

    def test_a_set_size_is_kept_whatever_the_seats(self) -> None:
        pool = ParentPool(max_size=PoolSize(connections=3))
        profiles = [
            WorkerProfile(isolated=IsolatedChildren(count=count), pool=pool)
            for count in (1, 500)
        ]
        assert [profile.resolved_parent_pool() for profile in profiles] == [3, 3]

    def test_a_set_size_can_go_above_the_derived_cap(self) -> None:
        pool = ParentPool(max_size=PoolSize(connections=MAX_CHILD_POOL_CONNECTIONS))
        profile = WorkerProfile(isolated=IsolatedChildren(count=1), pool=pool)
        assert profile.resolved_parent_pool() == MAX_CHILD_POOL_CONNECTIONS

    def test_a_profile_with_no_children_resolves_to_the_floor(self) -> None:
        assert WorkerProfile().resolved_parent_pool() == PARENT_POOL_CONNECTIONS_FLOOR


class TestTaskSendPool:
    """``task_send_pool`` of each child kind: the pool that task code opens
    in a child when it sends."""

    @pytest.mark.parametrize(
        'children',
        [
            IsolatedChildren(count=1),
            SharedAsyncChildren(count=1, seats=1000),
            SharedThreadChildren(count=1, seats=256),
        ],
    )
    def test_each_kind_defaults_to_the_fixed_size(
        self,
        children: IsolatedChildren | SharedAsyncChildren | SharedThreadChildren,
    ) -> None:
        """The default does not follow the seats or the parent pool."""
        assert DEFAULT_TASK_SEND_POOL_CONNECTIONS == 2
        assert children.task_send_pool == PoolSize(
            connections=DEFAULT_TASK_SEND_POOL_CONNECTIONS,
        )

    @pytest.mark.parametrize(
        ('kind', 'fields'),
        [
            (IsolatedChildren, {'count': 1}),
            (SharedAsyncChildren, {'count': 1, 'seats': 4}),
            (SharedThreadChildren, {'count': 1, 'seats': 4}),
        ],
    )
    def test_a_set_size_parses_from_a_dict(
        self,
        kind: type[IsolatedChildren | SharedAsyncChildren | SharedThreadChildren],
        fields: dict[str, Any],
    ) -> None:
        children = kind.model_validate(
            {**fields, 'task_send_pool': {'kind': 'pool_size', 'connections': 6}},
        )
        assert children.task_send_pool == PoolSize(connections=6)

    def test_a_size_from_seats_is_refused(self) -> None:
        """The task-send pool has a stated size: no variant derives it."""
        with pytest.raises(ValidationError, match='pool_size'):
            SharedAsyncChildren.model_validate(
                {
                    'count': 1,
                    'seats': 4,
                    'task_send_pool': {'kind': 'pool_size_from_seats'},
                },
            )


class TestAppConfigPoolChecks:
    """A pool whose ``min_size`` is above its resolved ``max_size`` is
    refused with the profile name and the field path."""

    def test_an_isolated_min_size_above_max_size_names_the_path(self) -> None:
        profile = WorkerProfile(
            isolated=IsolatedChildren(count=1, pool=IsolatedPool(min_size=3)),
        )
        with pytest.raises(ConfigurationError) as exc_info:
            _app_config({'p': profile})

        error = exc_info.value
        assert error.code == ErrorCode.CONFIG_INVALID_WORKER_PROFILE
        assert error.message == (
            "worker_profiles['p'].isolated.pool.min_size: 3 is above the pool "
            'max_size (2)'
        )
        assert error.notes == [
            'max_size=PoolSize(connections=2)',
            'a pool keeps min_size connections open and never opens more than '
            'max_size',
        ]
        assert error.help_text == (
            'set min_size to 2 or less, or set max_size=PoolSize(connections=3) '
            'or more'
        )

    def test_a_min_size_equal_to_max_size_is_accepted(self) -> None:
        profile = WorkerProfile(
            isolated=IsolatedChildren(
                count=1,
                pool=IsolatedPool(min_size=2, max_size=PoolSize(connections=2)),
            ),
        )
        assert _app_config({'p': profile}).worker_profiles == {'p': profile}

    def test_a_shared_async_min_size_above_the_derived_size_names_the_seats(
        self,
    ) -> None:
        derived = shared_async_pool_connections(1)
        profile = WorkerProfile(
            shared_async=SharedAsyncChildren(
                count=1,
                seats=1,
                pool=SharedPool(min_size=derived + 1),
            ),
        )
        with pytest.raises(ConfigurationError) as exc_info:
            _app_config({'p': profile})

        error = exc_info.value
        assert error.code == ErrorCode.CONFIG_INVALID_WORKER_PROFILE
        assert error.message == (
            f"worker_profiles['p'].shared_async.pool.min_size: {derived + 1} is "
            f'above the pool max_size ({derived})'
        )
        assert error.notes[0] == (
            f'max_size=PoolSizeFromSeats() resolves to {derived} connections '
            'for 1 seats'
        )

    def test_a_shared_async_min_size_at_the_derived_size_is_accepted(self) -> None:
        derived = shared_async_pool_connections(1)
        profile = WorkerProfile(
            shared_async=SharedAsyncChildren(
                count=1,
                seats=1,
                pool=SharedPool(min_size=derived),
            ),
        )
        assert _app_config({'p': profile}).worker_profiles == {'p': profile}

    def test_a_shared_thread_min_size_above_a_set_size_names_the_path(self) -> None:
        profile = WorkerProfile(
            shared_thread=SharedThreadChildren(
                count=1,
                seats=4,
                pool=SharedPool(min_size=5, max_size=PoolSize(connections=4)),
            ),
        )
        with pytest.raises(ConfigurationError) as exc_info:
            _app_config({'p': profile})

        assert exc_info.value.message == (
            "worker_profiles['p'].shared_thread.pool.min_size: 5 is above the "
            'pool max_size (4)'
        )
        assert exc_info.value.notes[0] == 'max_size=PoolSize(connections=4)'

    def test_each_mode_is_reported_at_once(self) -> None:
        profile = WorkerProfile(
            isolated=IsolatedChildren(count=1, pool=IsolatedPool(min_size=3)),
            shared_async=SharedAsyncChildren(
                count=1,
                seats=4,
                pool=SharedPool(min_size=6, max_size=PoolSize(connections=5)),
            ),
            shared_thread=SharedThreadChildren(
                count=1,
                seats=4,
                pool=SharedPool(min_size=6, max_size=PoolSize(connections=5)),
            ),
        )
        with pytest.raises(MultipleValidationErrors) as exc_info:
            _app_config({'p': profile})

        assert [
            error.message.split(':')[0] for error in _errors_of(exc_info.value)
        ] == [
            "worker_profiles['p'].isolated.pool.min_size",
            "worker_profiles['p'].shared_async.pool.min_size",
            "worker_profiles['p'].shared_thread.pool.min_size",
        ]


class TestAppConfigProfileChecks:
    def test_no_profiles_is_valid(self) -> None:
        """An app that runs no worker (a producer, the web UI) needs none."""
        assert _app_config({}).worker_profiles == {}

    def test_valid_profiles_are_kept(self) -> None:
        profiles = {'default': _isolated(4), 'batch': _isolated(1)}
        assert _app_config(profiles).worker_profiles == profiles

    def test_a_profile_with_no_children_names_the_profile(self) -> None:
        with pytest.raises(ConfigurationError) as raised:
            _app_config({'empty': WorkerProfile()})
        assert raised.value.code == ErrorCode.CONFIG_INVALID_WORKER_PROFILE
        assert raised.value.message == (
            "worker_profiles['empty']: the profile starts no children"
        )

    @pytest.mark.parametrize(
        'name',
        ['', 'has space', '-flag', '.hidden', 'a/b', 'x' * (PROFILE_NAME_MAX_LENGTH + 1)],
    )
    def test_an_invalid_name_is_refused(self, name: str) -> None:
        with pytest.raises(ConfigurationError) as raised:
            _app_config({name: _isolated(1)})
        assert raised.value.code == ErrorCode.CONFIG_INVALID_WORKER_PROFILE
        assert raised.value.message == (
            f'worker_profiles[{name!r}]: the profile name is not valid'
        )

    @pytest.mark.parametrize(
        'name',
        ['default', 'batch-high-memory', 'v1.2', '_internal', 'x' * PROFILE_NAME_MAX_LENGTH],
    )
    def test_a_valid_name_is_accepted(self, name: str) -> None:
        assert name in _app_config({name: _isolated(1)}).worker_profiles

    @pytest.mark.parametrize('field', ['per_queue_per_pass', 'claimed_per_worker'])
    def test_a_claim_limit_above_capacity_names_the_field_path(
        self,
        field: str,
    ) -> None:
        with pytest.raises(ConfigurationError) as raised:
            _app_config({'small': _isolated(2, **{field: TaskLimit(tasks=3)})})
        assert raised.value.code == ErrorCode.CONFIG_INVALID_WORKER_PROFILE
        assert raised.value.message == (
            f"worker_profiles['small'].claim.{field}: 3 tasks is above the "
            'capacity of the worker (2)'
        )

    @pytest.mark.parametrize('field', ['per_queue_per_pass', 'claimed_per_worker'])
    def test_a_claim_limit_equal_to_capacity_is_accepted(self, field: str) -> None:
        config = _app_config({'small': _isolated(2, **{field: TaskLimit(tasks=2)})})
        assert 'small' in config.worker_profiles

    def test_a_profile_with_only_shared_children_is_valid(self) -> None:
        config = _app_config(
            {'io': WorkerProfile(shared_async=SharedAsyncChildren(count=1, seats=4))},
        )
        assert 'io' in config.worker_profiles

    def test_capacity_counts_the_seats_of_shared_children(self) -> None:
        profile = WorkerProfile(
            isolated=IsolatedChildren(count=1),
            shared_async=SharedAsyncChildren(count=2, seats=10),
            claim=ClaimLimits(claimed_per_worker=TaskLimit(tasks=21)),
        )
        assert 'split' in _app_config({'split': profile}).worker_profiles

    def test_a_claim_limit_above_the_seats_names_each_term(self) -> None:
        profile = WorkerProfile(
            isolated=IsolatedChildren(count=1),
            shared_async=SharedAsyncChildren(count=2, seats=10),
            claim=ClaimLimits(claimed_per_worker=TaskLimit(tasks=22)),
        )
        with pytest.raises(ConfigurationError) as raised:
            _app_config({'split': profile})
        assert raised.value.message == (
            "worker_profiles['split'].claim.claimed_per_worker: 22 tasks is "
            'above the capacity of the worker (21)'
        )
        assert raised.value.notes[0] == (
            'capacity = 1 isolated children + 2 shared async children x 10 '
            'seats + prefetch_buffer 0'
        )

    def test_a_profile_with_only_thread_children_is_valid(self) -> None:
        config = _app_config(
            {
                'cpu': WorkerProfile(
                    shared_thread=SharedThreadChildren(count=2, seats=4)
                )
            },
        )
        assert 'cpu' in config.worker_profiles

    def test_a_claim_limit_above_the_seats_names_the_thread_term(self) -> None:
        profile = WorkerProfile(
            isolated=IsolatedChildren(count=1),
            shared_async=SharedAsyncChildren(count=1, seats=10),
            shared_thread=SharedThreadChildren(count=2, seats=4),
            claim=ClaimLimits(claimed_per_worker=TaskLimit(tasks=20)),
        )
        with pytest.raises(ConfigurationError) as raised:
            _app_config({'split': profile})
        assert raised.value.notes[0] == (
            'capacity = 1 isolated children + 1 shared async children x 10 '
            'seats + 2 shared thread children x 4 seats + prefetch_buffer 0'
        )

    def test_capacity_includes_the_prefetch_buffer(self) -> None:
        config = _app_config(
            {'soft': _isolated(2, claimed_per_worker=TaskLimit(tasks=5))},
            prefetch_buffer=3,
            claim_lease_ms=60_000,
        )
        assert 'soft' in config.worker_profiles

    def test_capacity_with_prefetch_buffer_is_still_a_bound(self) -> None:
        with pytest.raises(ConfigurationError) as raised:
            _app_config(
                {'soft': _isolated(2, claimed_per_worker=TaskLimit(tasks=6))},
                prefetch_buffer=3,
                claim_lease_ms=60_000,
            )
        assert 'capacity of the worker (5)' in raised.value.message

    def test_every_profile_is_reported_in_name_order(self) -> None:
        """`horsies check` reports all profiles at once, each with its path."""
        with pytest.raises(MultipleValidationErrors) as raised:
            _app_config(
                {
                    'zeta': _isolated(1, per_queue_per_pass=TaskLimit(tasks=9)),
                    'alpha': WorkerProfile(),
                    'ok': _isolated(4),
                }
            )
        messages = [error.message for error in _errors_of(raised.value)]
        assert messages == [
            "worker_profiles['alpha']: the profile starts no children",
            "worker_profiles['zeta'].claim.per_queue_per_pass: 9 tasks is above "
            'the capacity of the worker (1)',
        ]

    def test_claim_limits_are_not_compared_when_prefetch_buffer_is_invalid(
        self,
    ) -> None:
        """The prefetch error is reported; no capacity is computed from it."""
        with pytest.raises(ConfigurationError) as raised:
            _app_config(
                {'p': _isolated(1, claimed_per_worker=TaskLimit(tasks=1))},
                prefetch_buffer=-1,
            )
        assert raised.value.code == ErrorCode.CONFIG_INVALID_PREFETCH

    def test_log_config_lists_each_profile(self) -> None:
        config = _app_config({'default': _isolated(2)})
        formatted = config._format_for_logging()  # pyright: ignore[reportPrivateUsage]
        assert '  worker_profiles:' in formatted
        assert f'    default: {describe_worker_profile(_isolated(2))}' in formatted


class TestDescribeWorkerProfile:
    def test_every_setting_is_named(self) -> None:
        profile = WorkerProfile(
            isolated=IsolatedChildren(
                count=8,
                recycle=IsolatedRecycle(
                    after=NoTaskLimit(),
                    above=MemoryLimit(mb=1024),
                ),
            ),
            claim=ClaimLimits(per_queue_per_pass=TaskLimit(tasks=4)),
            drain_timeout=timedelta(seconds=90),
            preload=ChildPreload.APP,
        )
        assert describe_worker_profile(profile) == (
            'isolated=8 (recycle after no limit, above 1024 MB; pool 0 to 2 '
            'connections, check on; task_send_pool 2 connections), '
            'shared_async=none, shared_thread=none, '
            'pool=5 connections (derived from 8 seats), '
            'claim per queue per pass: 4 tasks, claimed per worker: no limit, '
            'drain_timeout=90s, preload=app'
        )

    def test_shared_async_children_are_named(self) -> None:
        profile = WorkerProfile(
            shared_async=SharedAsyncChildren(
                count=3,
                seats=25,
                recycle=SharedRecycle(
                    after_per_seat=TaskLimit(tasks=40),
                    above=MemoryLimit(mb=2048),
                ),
            ),
        )
        assert (
            'shared_async=3x25 seats (recycle after 40 tasks per seat, '
            'above 2048 MB; '
            f'pool 0 to {shared_async_pool_connections(25)} connections, '
            'derived from seats, check on; '
            f'executor {shared_async_executor_threads(25)} threads, derived '
            'from seats; task_send_pool 2 connections), '
        ) in describe_worker_profile(profile)

    def test_shared_thread_children_are_named(self) -> None:
        profile = WorkerProfile(
            shared_thread=SharedThreadChildren(
                count=2,
                seats=8,
                recycle=SharedRecycle(
                    after_per_seat=NoTaskLimit(),
                    above=MemoryLimit(mb=1024),
                ),
            ),
        )
        assert (
            'shared_thread=2x8 seats (recycle after no limit per seat, '
            'above 1024 MB; '
            f'pool 0 to {shared_thread_pool_connections(8)} connections, '
            'derived from seats, check on; task_send_pool 2 connections), '
        ) in describe_worker_profile(profile)

    def test_set_sizes_are_named_without_their_origin(self) -> None:
        """The start log shows the resolved values of each kind; a size set
        in the profile carries no "derived from seats"."""
        profile = WorkerProfile(
            isolated=IsolatedChildren(
                count=1,
                pool=IsolatedPool(
                    min_size=1,
                    max_size=PoolSize(connections=4),
                    check=False,
                ),
                task_send_pool=PoolSize(connections=3),
            ),
            shared_async=SharedAsyncChildren(
                count=1,
                seats=50,
                pool=SharedPool(min_size=2, max_size=PoolSize(connections=9)),
                executor=ExecutorThreads(threads=7),
                task_send_pool=PoolSize(connections=5),
            ),
            shared_thread=SharedThreadChildren(
                count=1,
                seats=6,
                pool=SharedPool(max_size=PoolSize(connections=3), check=False),
                task_send_pool=PoolSize(connections=1),
            ),
            pool=ParentPool(max_size=PoolSize(connections=11)),
        )
        described = describe_worker_profile(profile)
        assert (
            'isolated=1 (recycle after 100 tasks, above no limit; pool 1 to 4 '
            'connections, check off; task_send_pool 3 connections), '
        ) in described
        assert (
            'shared_async=1x50 seats (recycle after 100 tasks per seat, above '
            'no limit; pool 2 to 9 connections, check on; executor 7 threads; '
            'task_send_pool 5 connections), '
        ) in described
        assert (
            'shared_thread=1x6 seats (recycle after 100 tasks per seat, above '
            'no limit; pool 0 to 3 connections, check off; task_send_pool 1 '
            'connections), '
        ) in described
        assert ', pool=11 connections, claim per queue per pass: ' in described

    def test_no_isolated_children(self) -> None:
        assert describe_worker_profile(WorkerProfile()).startswith('isolated=none, ')

    def test_the_parent_pool_names_the_seats_that_it_derives_from(self) -> None:
        profile = WorkerProfile(
            isolated=IsolatedChildren(count=4),
            shared_async=SharedAsyncChildren(count=2, seats=100),
        )
        assert (
            ', shared_thread=none, pool=20 connections (derived from 204 '
            'seats), claim per queue per pass: '
        ) in describe_worker_profile(profile)
