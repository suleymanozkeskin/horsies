"""The broker builds one typed set of loop-bound resources.

Construction does no I/O: engines and the listener connect on first use,
so these brokers never reach a database.
"""

from __future__ import annotations

import asyncio
from unittest.mock import patch

import pytest
from pydantic import SecretStr

from horsies.core.app import Horsies
from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.brokers.resources import (
    BrokerResources,
    EventLoopConflict,
    ListenerDisabled,
    ListenerEnabled,
    SessionEngineSeparate,
    SessionEngineShared,
)
from horsies.core.models.app import AppConfig
from horsies.core.models.broker import PostgresConfig
from horsies.core.types.result import Result, is_ok
from horsies.core.worker.child_runner import _discard_inherited_broker

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


def _app_with(broker: PostgresBroker | None) -> Horsies:
    app = Horsies(AppConfig(broker=PostgresConfig(database_url=SecretStr(RUNTIME_URL))))
    app._broker = broker
    return app


@pytest.mark.unit
class TestDiscardInheritedBroker:
    """A forked child drops the parent's pools without closing their connections."""

    def test_disposes_every_engine_with_close_false(self) -> None:
        broker = _broker(session_url=SESSION_URL)
        resources = _selected(broker)
        separate = resources.session_engine
        assert isinstance(separate, SessionEngineSeparate)
        app = _app_with(broker)

        with (
            patch.object(resources.engine.sync_engine, 'dispose') as runtime_dispose,
            patch.object(separate.engine.sync_engine, 'dispose') as session_dispose,
        ):
            _discard_inherited_broker(app)

        runtime_dispose.assert_called_once_with(close=False)
        session_dispose.assert_called_once_with(close=False)
        assert app._broker is None

    def test_shared_session_engine_is_disposed_once(self) -> None:
        broker = _broker()
        resources = _selected(broker)
        app = _app_with(broker)

        with patch.object(resources.engine.sync_engine, 'dispose') as runtime_dispose:
            _discard_inherited_broker(app)

        runtime_dispose.assert_called_once_with(close=False)
        assert app._broker is None

    def test_without_a_broker_does_nothing(self) -> None:
        app = _app_with(None)

        _discard_inherited_broker(app)

        assert app._broker is None
