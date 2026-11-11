"""``horsies check --live`` against a real server: one worker of a profile
counts the resolved pool ``max_size`` of each child kind.

Enters at ``app.check(live=True)``, which reads ``max_connections`` and the
reserved slots from the server, and asserts on the ``HRS-219`` error.
"""

from __future__ import annotations

import asyncio

import pytest
from pydantic import SecretStr
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from horsies.core.app import Horsies
from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.errors import ErrorCode
from horsies.core.models.app import AppConfig
from horsies.core.models.broker import PostgresConfig
from horsies.core.models.child_sizing import shared_thread_pool_connections
from horsies.core.models.worker_profile import (
    MAX_CHILD_POOL_CONNECTIONS,
    IsolatedChildren,
    PoolSize,
    SharedAsyncChildren,
    SharedPool,
    SharedThreadChildren,
    WorkerProfile,
)
from horsies.core.worker.profile_fit import (
    ServerConnectionLimit,
    ServerConnectionLimitUnreadable,
    read_server_connection_limit,
)

pytestmark = [pytest.mark.integration]

THREAD_SEATS = 8


async def _usable_slots(db_url: str) -> int:
    """The slots that an ordinary role can use on the test server."""
    engine = create_async_engine(db_url)
    try:
        factory = async_sessionmaker(engine, class_=AsyncSession)
        match await read_server_connection_limit(factory):
            case ServerConnectionLimit() as limit:
                return limit.usable()
            case ServerConnectionLimitUnreadable(reason=reason):
                raise AssertionError(f'the connection limit is unreadable: {reason}')
    finally:
        await engine.dispose()


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

    total = 5 + 2 + 1 + 1 * 2 + async_children * per_async_child + thread_pool
    assert total > usable
    assert [error.code for error in errors] == [ErrorCode.CHECK_WORKER_CONNECTIONS]
    assert errors[0].message.startswith(
        f"worker_profiles['big']: one worker opens up to {total} Postgres "
        'connections itself',
    )
    assert errors[0].notes[0] == (
        'parent pool 5 + listener 2 + runner heartbeat 1 + 1 isolated children '
        f'x isolated.pool max_size 2 + {async_children} shared async children x '
        f'shared_async.pool max_size {per_async_child} + 1 shared thread '
        f'children x shared_thread.pool max_size {thread_pool} = {total}'
    )
