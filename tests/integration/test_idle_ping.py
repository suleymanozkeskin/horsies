"""``register_idle_ping`` on a real pool: which checkouts ping, and what a
dead connection does.

A ping is the dialect's own ``do_ping``: the empty statement in autocommit,
one client turn that sends ``Q:;``. ``pings`` counts the calls of that ping
and calls it through. The broker tests build the engine as the broker does
for each ``pool_pre_ping`` value.
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncGenerator, Callable, Generator
from typing import Any, Final

import pytest
import pytest_asyncio
from pydantic import SecretStr
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from horsies.core.brokers import postgres as postgres_module
from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.models.broker import PostgresConfig
from horsies.core.types.result import Err, Ok
from horsies.core.utils.db import register_idle_ping
from tests.integration.autocommit_support import proxied_url, upstream_of
from tests.integration.round_trip_proxy import RoundTripProxy, Turn

pytestmark = [pytest.mark.integration, pytest.mark.asyncio(loop_scope='function')]

POOL_SIZE: Final = 2
SHORT_IDLE_S: Final = 0.2
PAST_SHORT_IDLE_S: Final = 0.6
LONG_IDLE_S: Final = 600.0
PING_TURN: Final = 'Q:;'
BACKEND_EXIT_POLLS: Final = 100
BACKEND_EXIT_POLL_S: Final = 0.02
BACKEND_PID: Final = text('SELECT pg_backend_pid()')


@pytest.fixture
def proxy(db_url: str) -> Generator[RoundTripProxy]:
    host, port = upstream_of(db_url)
    started = RoundTripProxy(host, port)
    started.start()
    yield started
    started.stop()


@pytest_asyncio.fixture(loop_scope='function')
async def admin(db_url: str) -> AsyncGenerator[AsyncEngine]:
    """A direct engine that terminates backends."""
    created = create_async_engine(db_url, pool_size=1, max_overflow=0)
    yield created
    await created.dispose()


@pytest_asyncio.fixture(loop_scope='function')
async def engine(db_url: str, proxy: RoundTripProxy) -> AsyncGenerator[AsyncEngine]:
    """The engine under test, through the proxy, with SQLAlchemy's own
    pre-ping off, as the broker builds its engines."""
    created = create_async_engine(
        proxied_url(db_url, proxy),
        pool_size=POOL_SIZE,
        max_overflow=0,
    )
    yield created
    await created.dispose()


def _count_pings(engine: AsyncEngine, monkeypatch: pytest.MonkeyPatch) -> list[int]:
    """One entry per call of the dialect's ping; each call runs the ping."""
    dialect = engine.sync_engine.dialect
    real_ping: Callable[[Any], bool] = dialect.do_ping
    calls: list[int] = []

    def _counted(dbapi_connection: Any) -> bool:
        calls.append(1)
        return real_ping(dbapi_connection)

    monkeypatch.setattr(dialect, 'do_ping', _counted)
    return calls


async def _pid(engine: AsyncEngine) -> int:
    async with engine.connect() as connection:
        return int((await connection.execute(BACKEND_PID)).scalar_one())


async def _two_pids(engine: AsyncEngine) -> tuple[int, int]:
    """Check out two connections at once, then give both back."""
    async with engine.connect() as first, engine.connect() as second:
        return (
            int((await first.execute(BACKEND_PID)).scalar_one()),
            int((await second.execute(BACKEND_PID)).scalar_one()),
        )


async def _terminate(admin: AsyncEngine, pids: tuple[int, ...]) -> None:
    """Terminate ``pids`` and wait until each backend is gone."""
    async with admin.connect() as connection:
        for pid in pids:
            terminated = (
                await connection.execute(
                    text('SELECT pg_terminate_backend(:pid)'),
                    {'pid': pid},
                )
            ).scalar_one()
            assert terminated is True, pid
        await connection.commit()
        for _ in range(BACKEND_EXIT_POLLS):
            alive = (
                await connection.execute(
                    text('SELECT count(*) FROM pg_stat_activity WHERE pid = ANY(:pids)'),
                    {'pids': list(pids)},
                )
            ).scalar_one()
            await connection.commit()
            if alive == 0:
                return
            await asyncio.sleep(BACKEND_EXIT_POLL_S)
    raise AssertionError(f'backends {pids} still run after pg_terminate_backend')


def _ping_turns(turns: tuple[Turn, ...]) -> int:
    return sum(1 for turn in turns if PING_TURN in turn.messages)


async def test_a_connection_reused_within_the_limit_is_not_pinged(
    engine: AsyncEngine,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    register_idle_ping(engine, LONG_IDLE_S)
    pings = _count_pings(engine, monkeypatch)
    first = await _pid(engine)

    second = await _pid(engine)

    assert second == first
    assert pings == []


async def test_a_new_connection_is_not_pinged(
    engine: AsyncEngine,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """At a limit of 0 every reused connection is pinged; a new one has no
    check-in yet."""
    register_idle_ping(engine, 0.0)
    pings = _count_pings(engine, monkeypatch)

    _ = await _pid(engine)

    assert pings == []


async def test_a_connection_idle_past_the_limit_is_pinged_first(
    engine: AsyncEngine,
    proxy: RoundTripProxy,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """One ping, sent as the first turn of the checkout: one round trip."""
    register_idle_ping(engine, SHORT_IDLE_S)
    pings = _count_pings(engine, monkeypatch)
    first = await _pid(engine)
    await asyncio.sleep(PAST_SHORT_IDLE_S)

    proxy.start_recording()
    second = await _pid(engine)
    turns = proxy.stop_recording()

    assert second == first
    assert pings == [1]
    assert _ping_turns(turns) == 1, turns
    assert PING_TURN in turns[0].messages, turns


async def test_dead_idle_connections_are_replaced_with_the_pool(
    engine: AsyncEngine,
    admin: AsyncEngine,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Both pooled connections die while idle. The first checkout pings,
    the ping fails, and the pool replaces every connection checked in
    before it: the second dead connection is replaced at its checkout,
    with no ping. No statement fails."""
    register_idle_ping(engine, SHORT_IDLE_S)
    dead = await _two_pids(engine)
    await _terminate(admin, dead)
    await asyncio.sleep(PAST_SHORT_IDLE_S)
    pings = _count_pings(engine, monkeypatch)

    first = await _pid(engine)
    pair = await _two_pids(engine)

    assert first not in dead
    assert not set(pair) & set(dead)
    assert pings == [1]


async def test_a_connection_that_dies_within_the_limit_fails_its_statement(
    engine: AsyncEngine,
    admin: AsyncEngine,
) -> None:
    """The contract's other side: a connection checked in within the limit
    is used with no ping, so its death shows at the statement, as a
    disconnect."""
    register_idle_ping(engine, LONG_IDLE_S)
    pid = await _pid(engine)
    await _terminate(admin, (pid,))

    with pytest.raises(DBAPIError) as raised:
        _ = await _pid(engine)

    assert raised.value.connection_invalidated


@pytest_asyncio.fixture(loop_scope='function')
async def broker_engine(
    db_url: str,
    request: pytest.FixtureRequest,
    monkeypatch: pytest.MonkeyPatch,
) -> AsyncGenerator[AsyncEngine]:
    """The runtime engine of a broker built with ``request.param``:
    ``pool_pre_ping``, and the idle limit in seconds (None: the library's).
    The broker reads the limit when it builds the engine."""
    pool_pre_ping, idle_limit_s = request.param
    match idle_limit_s:
        case float():
            monkeypatch.setattr(postgres_module, 'PRE_PING_AFTER_IDLE_S', idle_limit_s)
        case None:
            pass
        case unexpected:
            raise AssertionError(unexpected)
    broker = PostgresBroker(
        PostgresConfig(
            database_url=SecretStr(db_url),
            pool_pre_ping=pool_pre_ping,
        ),
        assume_initialized=True,
    )
    match await broker.select_resources():
        case Ok(resources):
            pass
        case Err(error):
            raise AssertionError(error)
    yield resources.engine
    _ = await broker.close_async()


@pytest.mark.parametrize(
    ('broker_engine', 'replaced'),
    [((True, SHORT_IDLE_S), True), ((False, SHORT_IDLE_S), False)],
    ids=['pool_pre_ping_on', 'pool_pre_ping_off'],
    indirect=['broker_engine'],
)
async def test_the_broker_replaces_a_dead_idle_connection_when_pool_pre_ping_is_on(
    broker_engine: AsyncEngine,
    admin: AsyncEngine,
    replaced: bool,
) -> None:
    pid = await _pid(broker_engine)
    await _terminate(admin, (pid,))
    await asyncio.sleep(PAST_SHORT_IDLE_S)

    match replaced:
        case True:
            assert await _pid(broker_engine) != pid
        case False:
            with pytest.raises(DBAPIError) as raised:
                _ = await _pid(broker_engine)
            assert raised.value.connection_invalidated


@pytest.mark.parametrize(
    'broker_engine',
    [(True, None)],
    ids=['pool_pre_ping_on'],
    indirect=True,
)
async def test_the_broker_does_not_ping_a_connection_reused_within_the_limit(
    broker_engine: AsyncEngine,
    admin: AsyncEngine,
) -> None:
    """With ``pool_pre_ping`` on and the library's limit, a connection that
    died right after its check-in fails the next statement: no checkout
    pings it."""
    pid = await _pid(broker_engine)
    await _terminate(admin, (pid,))

    with pytest.raises(DBAPIError) as raised:
        _ = await _pid(broker_engine)

    assert raised.value.connection_invalidated
