"""The shared TTL cache behind ``/api/tasks/stats``."""

from __future__ import annotations

import asyncio

import pytest

from horsies.core.types.result import Err, Ok
from horsies.monitoring import MonitoringResult, StatusCount
from horsies.monitoring.errors import (
    MonitoringQueryError,
    MonitoringQueryErrorCode,
)
from horsies.web.task_stats_cache import TaskStatsCache, TaskStatsScope

pytestmark = [pytest.mark.unit, pytest.mark.asyncio]

TTL_SECONDS = 10.0


class FakeClock:
    def __init__(self) -> None:
        self.now = 0.0

    def __call__(self) -> float:
        return self.now


class CountingLoad:
    """A stats load that answers ``count`` and records each call."""

    def __init__(self, count: int = 1) -> None:
        self.calls = 0
        self.count = count

    async def __call__(self) -> MonitoringResult[list[StatusCount]]:
        self.calls += 1
        return Ok([StatusCount(status='PENDING', count=self.count)])


def _scope(task_name: str = 'a') -> TaskStatsScope:
    return TaskStatsScope(
        since=None,
        until=None,
        task_names=(task_name,),
        queues=(),
        workers=(),
        error_codes=(),
        error_categories=(),
        retried_only=False,
    )


def _cache(clock: FakeClock, max_scopes: int = 256) -> TaskStatsCache:
    return TaskStatsCache(
        ttl_seconds=TTL_SECONDS, max_scopes=max_scopes, clock=clock,
    )


async def test_a_hit_within_the_ttl_does_not_load() -> None:
    clock = FakeClock()
    cache = _cache(clock)
    load = CountingLoad()
    first = await cache.get(_scope(), load)
    clock.now = TTL_SECONDS - 0.001
    second = await cache.get(_scope(), load)
    assert load.calls == 1
    assert first == second


async def test_an_expired_entry_loads_again() -> None:
    clock = FakeClock()
    cache = _cache(clock)
    load = CountingLoad()
    await cache.get(_scope(), load)
    clock.now = TTL_SECONDS
    await cache.get(_scope(), load)
    assert load.calls == 2


async def test_scopes_are_cached_separately() -> None:
    clock = FakeClock()
    cache = _cache(clock)
    load_a, load_b = CountingLoad(count=1), CountingLoad(count=2)
    a = await cache.get(_scope('a'), load_a)
    b = await cache.get(_scope('b'), load_b)
    assert a != b
    assert (load_a.calls, load_b.calls) == (1, 1)


async def test_a_failed_load_is_returned_and_not_cached() -> None:
    clock = FakeClock()
    cache = _cache(clock)
    failure: MonitoringResult[list[StatusCount]] = Err(
        MonitoringQueryError(
            code=MonitoringQueryErrorCode.DB_OPERATION_FAILED,
            message='task stats query failed',
            retryable=True,
            exception=RuntimeError('boom'),
        ),
    )
    calls = 0

    async def failing_load() -> MonitoringResult[list[StatusCount]]:
        nonlocal calls
        calls += 1
        return failure

    assert await cache.get(_scope(), failing_load) is failure
    assert await cache.get(_scope(), failing_load) is failure
    assert calls == 2
    assert len(cache) == 0


async def test_concurrent_misses_share_one_load() -> None:
    clock = FakeClock()
    cache = _cache(clock)
    release = asyncio.Event()
    calls = 0

    async def slow_load() -> MonitoringResult[list[StatusCount]]:
        nonlocal calls
        calls += 1
        await release.wait()
        return Ok([StatusCount(status='PENDING', count=3)])

    waiters = [asyncio.create_task(cache.get(_scope(), slow_load)) for _ in range(5)]
    await asyncio.sleep(0)
    release.set()
    results = await asyncio.gather(*waiters)
    assert calls == 1
    assert all(result == results[0] for result in results)


async def test_a_cancelled_caller_leaves_the_shared_load_running() -> None:
    clock = FakeClock()
    cache = _cache(clock)
    release = asyncio.Event()
    calls = 0

    async def slow_load() -> MonitoringResult[list[StatusCount]]:
        nonlocal calls
        calls += 1
        await release.wait()
        return Ok([StatusCount(status='PENDING', count=4)])

    cancelled = asyncio.create_task(cache.get(_scope(), slow_load))
    survivor = asyncio.create_task(cache.get(_scope(), slow_load))
    await asyncio.sleep(0)
    cancelled.cancel()
    release.set()
    result = await survivor
    assert calls == 1
    assert result == Ok([StatusCount(status='PENDING', count=4)])
    assert len(cache) == 1


async def test_the_bound_evicts_the_entry_that_expires_first() -> None:
    clock = FakeClock()
    cache = _cache(clock, max_scopes=2)
    loads = {name: CountingLoad() for name in ('a', 'b', 'c')}
    for offset, name in enumerate(('a', 'b', 'c')):
        clock.now = float(offset)
        await cache.get(_scope(name), loads[name])
    assert len(cache) == 2
    await cache.get(_scope('b'), loads['b'])
    await cache.get(_scope('c'), loads['c'])
    await cache.get(_scope('a'), loads['a'])
    assert {name: load.calls for name, load in loads.items()} == {
        'a': 2, 'b': 1, 'c': 1,
    }


@pytest.mark.parametrize(
    ('ttl_seconds', 'max_scopes'), [(0.0, 1), (1.0, 0)], ids=['ttl', 'bound'],
)
async def test_non_positive_settings_are_rejected(
    ttl_seconds: float, max_scopes: int,
) -> None:
    with pytest.raises(ValueError):
        TaskStatsCache(ttl_seconds=ttl_seconds, max_scopes=max_scopes)
