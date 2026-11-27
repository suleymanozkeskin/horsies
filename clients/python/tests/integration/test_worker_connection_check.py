"""``horsies check --live`` against a real server: one worker of a profile
counts the resolved pool ``max_size`` of the worker process and of each child
kind, and states the task-send pool of each child kind.

Enters at ``app.check(live=True)``, whose worker connection step is the
core's check (``CoreHandle.check_worker_connections``): it reads
``max_connections`` and the reserved slots from the server, and the test
asserts on the ``HRS-219`` error. The core's worker opens one listener
connection and one liveness lock connection; the session pool is counted only
when the session URL differs (it does not here).
"""

from __future__ import annotations

import asyncio

import pytest
from pydantic import SecretStr
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from horsies.core.app import Horsies
from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.errors import HorsiesErrorCode
from horsies.core.models.app import AppConfig
from horsies.core.models.broker import PostgresConfig
from horsies.core.models.child_sizing import (
    DEFAULT_ISOLATED_POOL_CONNECTIONS,
    shared_thread_pool_connections,
)
from horsies.core.models.worker_profile import (
    MAX_CHILD_POOL_CONNECTIONS,
    IsolatedChildren,
    ParentPool,
    PoolSize,
    SharedAsyncChildren,
    SharedPool,
    SharedThreadChildren,
    WorkerProfile,
)
from tests.sqlalchemy_url import sqlalchemy_url

pytestmark = [pytest.mark.integration]

THREAD_SEATS = 8


_CONNECTION_LIMIT_SQL = text(
    """
    SELECT
        current_setting('max_connections')::int,
        current_setting('superuser_reserved_connections')::int,
        COALESCE(current_setting('reserved_connections', true), '0')::int
    """
)


async def _usable_slots(db_url: str) -> int:
    """The slots that an ordinary role can use on the test server:
    ``max_connections`` less the superuser and the role reserved slots
    (``reserved_connections`` exists from Postgres 16 on)."""
    engine = create_async_engine(sqlalchemy_url(db_url))
    try:
        async with async_sessionmaker(engine, class_=AsyncSession)() as session:
            result = await session.execute(_CONNECTION_LIMIT_SQL)
            max_connections, superuser_reserved, reserved = result.one()
    finally:
        await engine.dispose()
    return max(0, int(max_connections) - int(superuser_reserved) - int(reserved))


@pytest.mark.asyncio(loop_scope='function')
async def test_each_child_kind_counts_its_own_pool_against_the_server(
    db_url: str,
    broker: PostgresBroker,
) -> None:
    _ = broker
    usable = await _usable_slots(db_url)
    per_async_child = min(usable, MAX_CHILD_POOL_CONNECTIONS)
    async_children = usable // per_async_child + 1
    thread_pool = shared_thread_pool_connections(THREAD_SEATS)
    big = WorkerProfile(
        isolated=IsolatedChildren(count=1),
        shared_async=SharedAsyncChildren(
            count=async_children,
            seats=4,
            pool=SharedPool(max_size=PoolSize(connections=per_async_child)),
        ),
        shared_thread=SharedThreadChildren(count=1, seats=THREAD_SEATS),
    )
    small = WorkerProfile(isolated=IsolatedChildren(count=1))
    app = Horsies(
        AppConfig(
            broker=PostgresConfig(database_url=SecretStr(db_url)),
            worker_profiles={'big': big, 'small': small},
        ),
    )

    errors = await asyncio.to_thread(app.check, live=True)

    parent_pool = big.resolved_parent_pool()
    isolated_pool = DEFAULT_ISOLATED_POOL_CONNECTIONS
    total = (
        parent_pool
        + 1
        + 1
        + 1
        + 1 * isolated_pool
        + async_children * per_async_child
        + thread_pool
    )
    assert total > usable
    assert [error.code for error in errors] == [HorsiesErrorCode.CHECK_WORKER_CONNECTIONS]
    assert errors[0].message.startswith(
        f"worker_profiles['big']: one worker opens up to {total} Postgres "
        'connections itself',
    )
    assert errors[0].notes[0] == (
        f'parent pool max_size {parent_pool} + listener 1 + runner heartbeat 1 '
        f'+ liveness lock 1 + 1 isolated children x isolated.pool max_size {isolated_pool} '
        f'+ {async_children} '
        f'shared async children x shared_async.pool max_size {per_async_child} '
        f'+ 1 shared thread children x shared_thread.pool max_size '
        f'{thread_pool} = {total}'
    )
    sends = 2 * (1 + async_children + 1)
    assert errors[0].notes[1] == (
        'not counted: task code that sends opens a pool of the task_send_pool '
        'of its child kind in its child, for each event loop that it sends '
        'from; with one such loop in each child, 1 isolated children x '
        f'isolated.task_send_pool 2 + {async_children} shared async children x '
        'shared_async.task_send_pool 2 + 1 shared thread children x '
        f'shared_thread.task_send_pool 2 = {sends} more, {total + sends} in all'
    )


@pytest.mark.asyncio(loop_scope='function')
async def test_the_parent_pool_of_the_profile_is_counted_against_the_server(
    db_url: str,
    broker: PostgresBroker,
) -> None:
    """A parent pool above the usable slots alone fails the check, and the
    formula names it: the pool of PostgresConfig plays no part."""
    _ = broker
    usable = await _usable_slots(db_url)
    parent_pool = min(usable + 1, MAX_CHILD_POOL_CONNECTIONS)
    profile = WorkerProfile(
        isolated=IsolatedChildren(count=1),
        pool=ParentPool(max_size=PoolSize(connections=parent_pool)),
    )
    app = Horsies(
        AppConfig(
            broker=PostgresConfig(
                database_url=SecretStr(db_url),
                pool_size=1,
                max_overflow=0,
            ),
            worker_profiles={'parent': profile},
        ),
    )

    errors = await asyncio.to_thread(app.check, live=True)

    isolated_pool = DEFAULT_ISOLATED_POOL_CONNECTIONS
    total = parent_pool + 1 + 1 + 1 + 1 * isolated_pool
    assert total > usable
    assert [error.code for error in errors] == [HorsiesErrorCode.CHECK_WORKER_CONNECTIONS]
    assert errors[0].notes[0] == (
        f'parent pool max_size {parent_pool} + listener 1 + runner heartbeat 1 '
        f'+ liveness lock 1 + 1 isolated children x isolated.pool max_size {isolated_pool} '
        f'= {total}'
    )
