"""``HeldConnection`` on a real pool: what it holds, gives back and drops.

Each test reads the pool (``Pool.checkedout``, the checkout event) and the
backend that serves a statement (``pg_backend_pid``):

- statements reuse the held connection; a refused statement keeps it;
- a broken connection is ``OutcomeUnknown``, and SQLAlchemy's disconnect
  handling replaces every connection of the pool;
- the connection goes back after an idle period, at the recycle age, on
  ``release``, and is dropped when its statement is cancelled;
- the worker's claim passes share one checkout.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncGenerator, Callable
from typing import Any, Final

import pytest
import pytest_asyncio
from sqlalchemy import event, text
from sqlalchemy.exc import DataError
from sqlalchemy.pool import QueuePool
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.types.result import Err, Ok, Result
from horsies.core.utils.db import (
    AutocommitFailure,
    HeldConnection,
    NoRecycle,
    OutcomeUnknown,
    RecycleAfter,
    SingleStatement,
    StatementRefused,
    StatementRows,
)
from tests.integration.autocommit_support import ProxiedWorker, open_proxied_worker

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope='function')]

POOL_SIZE: Final = 3
IDLE_RELEASE_S: Final = 0.05
WAIT_BOUND_S: Final = 5.0
BACKEND_PID: Final = SingleStatement(
    statement=text('SELECT pg_backend_pid() AS pid, CAST(:tag AS text) AS tag'),
    parameters={'tag': 'held'},
)


@pytest_asyncio.fixture(loop_scope='function')
async def engine(db_url: str) -> AsyncGenerator[AsyncEngine]:
    created = create_async_engine(db_url, pool_size=POOL_SIZE, max_overflow=0)
    yield created
    await created.dispose()


@pytest.fixture
def session_factory(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    return async_sessionmaker(engine, expire_on_commit=False)


@pytest.fixture
def checkouts(engine: AsyncEngine) -> list[int]:
    """One entry per checkout of the pool."""
    seen: list[int] = []

    def _count(*_: Any) -> None:
        seen.append(1)

    event.listen(engine.sync_engine.pool, 'checkout', _count)
    return seen


def _pid(outcome: Result[StatementRows, AutocommitFailure]) -> int:
    match outcome:
        case Ok(rows):
            (row,) = rows.rows
            return int(row[0])
        case Err(failure):
            raise AssertionError(f'expected rows, got {failure!r}')


def _checked_out(engine: AsyncEngine) -> int:
    """The connections of the pool that are checked out now."""
    pool = engine.sync_engine.pool
    assert isinstance(pool, QueuePool), type(pool)
    return pool.checkedout()


async def _pooled_pid(engine: AsyncEngine) -> int:
    async with engine.connect() as connection:
        return int((await connection.execute(text('SELECT pg_backend_pid()'))).scalar_one())


async def _terminate(engine: AsyncEngine, pid: int) -> None:
    async with engine.connect() as connection:
        _ = await connection.execute(
            text('SELECT pg_terminate_backend(:pid)'),
            {'pid': pid},
        )
        await connection.commit()


async def _until(condition: Callable[[], bool]) -> None:
    """Wait until ``condition()`` holds; fail after ``WAIT_BOUND_S``."""
    async with asyncio.timeout(WAIT_BOUND_S):
        while not condition():
            await asyncio.sleep(0.01)


async def test_statements_reuse_the_held_connection(
    engine: AsyncEngine,
    session_factory: async_sessionmaker[AsyncSession],
    checkouts: list[int],
) -> None:
    held = HeldConnection(session_factory, NoRecycle())

    first = _pid(await held.run(BACKEND_PID))
    second = _pid(await held.run(BACKEND_PID))

    assert first == second
    assert len(checkouts) == 1
    assert _checked_out(engine) == 1  # pyright: ignore[reportAttributeAccessIssue]
    await held.release()
    assert _checked_out(engine) == 0  # pyright: ignore[reportAttributeAccessIssue]


async def test_a_refused_statement_keeps_the_held_connection(
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    held = HeldConnection(session_factory, NoRecycle())
    before = _pid(await held.run(BACKEND_PID))

    refused = await held.run(
        SingleStatement(statement=text('SELECT CAST(:v AS integer)'), parameters={'v': 'x'}),
    )

    match refused:
        case Err(StatementRefused(error=error)):
            assert isinstance(error, DataError)
            assert not error.connection_invalidated
        case _:
            raise AssertionError(f'expected StatementRefused, got {refused!r}')
    assert _pid(await held.run(BACKEND_PID)) == before
    await held.release()


async def test_a_broken_connection_is_unknown_and_the_pool_is_replaced(
    engine: AsyncEngine,
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """The held backend dies. Its statement is OutcomeUnknown; SQLAlchemy's
    disconnect handling replaces the idle pooled connection too."""
    held = HeldConnection(session_factory, NoRecycle())
    held_pid = _pid(await held.run(BACKEND_PID))
    idle_pid = await _pooled_pid(engine)
    await _terminate(engine, held_pid)

    broken = await held.run(BACKEND_PID)

    match broken:
        case Err(OutcomeUnknown(error=error)):
            assert error.connection_invalidated
        case _:
            raise AssertionError(f'expected OutcomeUnknown, got {broken!r}')
    assert _checked_out(engine) == 0  # pyright: ignore[reportAttributeAccessIssue]
    assert await _pooled_pid(engine) not in {held_pid, idle_pid}
    assert _pid(await held.run(BACKEND_PID)) not in {held_pid, idle_pid}
    await held.release()


async def test_an_idle_held_connection_goes_back_to_the_pool(
    engine: AsyncEngine,
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    held = HeldConnection(session_factory, NoRecycle(), idle_release_s=IDLE_RELEASE_S)
    _ = _pid(await held.run(BACKEND_PID))

    assert _checked_out(engine) == 1
    await _until(lambda: _checked_out(engine) == 0)
    _ = _pid(await held.run(BACKEND_PID))
    assert _checked_out(engine) == 1
    await held.release()


async def test_the_recycle_age_gives_the_connection_back(
    session_factory: async_sessionmaker[AsyncSession],
    checkouts: list[int],
) -> None:
    """At age 0 every statement takes a new checkout; with no recycle one
    checkout serves all of them."""
    every_statement = HeldConnection(session_factory, RecycleAfter(seconds=0.0))
    for _ in range(3):
        _ = _pid(await every_statement.run(BACKEND_PID))
    await every_statement.release()
    assert len(checkouts) == 3

    checkouts.clear()
    kept = HeldConnection(session_factory, NoRecycle())
    for _ in range(3):
        _ = _pid(await kept.run(BACKEND_PID))
    await kept.release()
    assert len(checkouts) == 1


async def test_a_cancelled_statement_drops_its_connection(
    engine: AsyncEngine,
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    held = HeldConnection(session_factory, NoRecycle())
    before = _pid(await held.run(BACKEND_PID))
    sleeping = asyncio.create_task(
        held.run(
            SingleStatement(
                statement=text('SELECT pg_sleep(CAST(:s AS float))'),
                parameters={'s': 2.0},
            ),
        ),
    )
    await asyncio.sleep(0.2)

    _ = sleeping.cancel()
    with pytest.raises(asyncio.CancelledError):
        await sleeping

    assert _checked_out(engine) == 0  # pyright: ignore[reportAttributeAccessIssue]
    assert _pid(await held.run(BACKEND_PID)) != before
    await held.release()


@pytest_asyncio.fixture(loop_scope='function')
async def worker(
    db_url: str,
    broker: PostgresBroker,  # noqa: ARG001 - the schema
) -> AsyncGenerator[ProxiedWorker]:
    opened = await open_proxied_worker(db_url)
    yield opened
    _ = await opened.broker.close_async()


async def test_the_claim_passes_of_a_worker_share_one_checkout(
    worker: ProxiedWorker,
) -> None:
    """Entered through the claim pass: two passes, one checkout of the
    worker pool, which the worker's stop gives back."""
    engine = worker.worker.sf.kw['bind']
    assert isinstance(engine, AsyncEngine)
    seen: list[int] = []

    def _count(*_: Any) -> None:
        seen.append(1)

    event.listen(engine.sync_engine.pool, 'checkout', _count)
    worker.worker._claim_slot_count = lambda: 1  # type: ignore[method-assign]  # pyright: ignore[reportPrivateUsage]

    _ = await worker.worker._claim_and_dispatch_all()  # pyright: ignore[reportPrivateUsage]
    _ = await worker.worker._claim_and_dispatch_all()  # pyright: ignore[reportPrivateUsage]

    assert len(seen) == 1
    await worker.worker._release_held_connections()  # pyright: ignore[reportPrivateUsage]
    assert _checked_out(engine) == 0  # pyright: ignore[reportAttributeAccessIssue]
