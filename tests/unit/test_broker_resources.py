"""The broker keeps one typed set of loop-bound resources per open event loop.

Construction and selection do no I/O: engines and the listener connect on
first use, so these brokers never reach a database. Close and release are
observed through recording fakes of ``PostgresListener`` and
``AsyncEngine.dispose``.
"""

from __future__ import annotations

import asyncio
from unittest.mock import patch

import pytest
from pydantic import SecretStr
from sqlalchemy import Engine

from horsies.core.app import Horsies
from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.brokers.resources import (
    BrokerResources,
    EventLoopLimit,
    SessionEngineSeparate,
)
from horsies.core.models.app import AppConfig
from horsies.core.models.broker import PostgresConfig
from horsies.core.types.result import Result, is_ok
from horsies.core.worker.child_runner import _discard_inherited_broker

RUNTIME_URL = 'postgresql://u:p@localhost:5432/db'
SESSION_URL = 'postgresql://u:p@localhost:6543/db'


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


def _select_on(
    broker: PostgresBroker, loop: asyncio.AbstractEventLoop,
) -> Result[BrokerResources, EventLoopLimit]:
    return loop.run_until_complete(broker.select_resources())


def _selected(broker: PostgresBroker) -> BrokerResources:
    """Select on a fresh loop that closes afterwards."""
    async def select() -> Result[BrokerResources, EventLoopLimit]:
        return await broker.select_resources()

    result = asyncio.run(select())
    assert is_ok(result), result
    return result.ok_value


def _app_with(broker: PostgresBroker | None) -> Horsies:
    app = Horsies(AppConfig(broker=PostgresConfig(database_url=SecretStr(RUNTIME_URL))))
    app._broker = broker
    return app


@pytest.mark.unit
class TestDiscardInheritedBroker:
    """A forked child drops the parent's pools without closing their connections."""

    def test_disposes_every_engine_of_every_set_with_close_false(self) -> None:
        broker = _broker(session_url=SESSION_URL)
        loops = [asyncio.new_event_loop() for _ in range(2)]
        sets: list[BrokerResources] = []
        for loop in loops:
            selected = _select_on(broker, loop)
            assert is_ok(selected)
            sets.append(selected.ok_value)
        app = _app_with(broker)
        engines: list[Engine] = []
        for resources in sets:
            separate = resources.session_engine
            assert isinstance(separate, SessionEngineSeparate)
            engines.extend([resources.engine.sync_engine, separate.engine.sync_engine])

        try:
            with patch('sqlalchemy.engine.Engine.dispose', autospec=True) as dispose:
                _discard_inherited_broker(app)

            assert [call.args[0] for call in dispose.call_args_list] == engines
            assert all(call.kwargs == {'close': False} for call in dispose.call_args_list)
            assert broker._loop_sets == ()
            assert app._broker is None
        finally:
            for loop in loops:
                loop.close()

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
