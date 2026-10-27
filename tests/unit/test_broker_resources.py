"""The broker builds one typed set of loop-bound resources.

Construction does no I/O: engines and the listener connect on first use,
so these brokers never reach a database.
"""

from __future__ import annotations

import asyncio

import pytest
from pydantic import SecretStr

from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.brokers.resources import (
    BrokerResources,
    EventLoopConflict,
    ListenerDisabled,
    ListenerEnabled,
    SessionEngineSeparate,
    SessionEngineShared,
)
from horsies.core.models.broker import PostgresConfig
from horsies.core.types.result import Result, is_ok

RUNTIME_URL = 'postgresql+psycopg://u:p@localhost:5432/db'
SESSION_URL = 'postgresql+psycopg://u:p@localhost:6543/db'


def _broker(
    *,
    session_url: str | None = None,
    assume_initialized: bool = False,
) -> PostgresBroker:
    config = PostgresConfig(
        database_url=SecretStr(RUNTIME_URL),
        session_database_url=SecretStr(session_url) if session_url else None,
    )
    return PostgresBroker(config, assume_initialized=assume_initialized)


def _selected(broker: PostgresBroker) -> BrokerResources:
    async def select() -> Result[BrokerResources, EventLoopConflict]:
        return await broker.select_resources()

    result = asyncio.run(select())
    assert is_ok(result), result
    return result.ok_value


@pytest.mark.unit
class TestBuildResources:
    def test_same_session_url_shares_the_runtime_engine(self) -> None:
        resources = _selected(_broker())

        assert isinstance(resources.session_engine, SessionEngineShared)
        assert resources.session_async_engine() is resources.engine

    def test_separate_session_url_gets_its_own_engine(self) -> None:
        resources = _selected(_broker(session_url=SESSION_URL))

        match resources.session_engine:
            case SessionEngineSeparate(engine=separate):
                assert separate is not resources.engine
                assert separate.url.port == 6543
                assert resources.session_async_engine() is separate
            case SessionEngineShared():
                pytest.fail('a different session URL must get its own engine')

    def test_listener_enabled_by_default(self) -> None:
        resources = _selected(_broker())

        assert isinstance(resources.listener, ListenerEnabled)

    def test_listener_disabled_when_assume_initialized(self) -> None:
        resources = _selected(_broker(assume_initialized=True))

        assert isinstance(resources.listener, ListenerDisabled)


@pytest.mark.unit
class TestSelectResources:
    def test_returns_the_same_set_on_every_call(self) -> None:
        broker = _broker()

        assert _selected(broker) is _selected(broker)


@pytest.mark.unit
class TestEventLoopConflict:
    def test_describe_names_both_threads_and_the_retry_condition(self) -> None:
        conflict = EventLoopConflict(
            bound_thread_name='MainThread', current_thread_name='worker-2',
        )

        text = conflict.describe()

        assert "'MainThread'" in text
        assert "'worker-2'" in text
        assert 'until that loop closes' in text
