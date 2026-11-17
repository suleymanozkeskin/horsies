"""Every engine horsies creates hides bound values in statement text.

Bound values are task arguments and results. ``hide_parameters=True`` keeps
them out of statement error text and ``echo`` logs. Each engine gets the flag
from ``PostgresConfig.sqlalchemy_engine_kwargs()``; an engine built without
those kwargs fails here.
"""

# pyright: reportPrivateUsage=false

from __future__ import annotations

from unittest import mock

import pytest
from pydantic import SecretStr

from horsies.core.app import Horsies
from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.models.app import AppConfig
from horsies.core.models.broker import PostgresConfig
from horsies.core.models.worker_profile import IsolatedChildren, WorkerProfile
from horsies.core.worker.profile_fit import ServerConnectionLimit

RUNTIME_URL = SecretStr('postgresql://u:p@pooler:6432/db')
SESSION_URL = SecretStr('postgresql://u:p@direct:5432/db')


@pytest.mark.unit
class TestEngineKwargsHideParameters:
    def test_engine_kwargs_set_hide_parameters(self) -> None:
        config = PostgresConfig(database_url=RUNTIME_URL)

        assert config.sqlalchemy_engine_kwargs()['hide_parameters'] is True

    @pytest.mark.asyncio
    async def test_runtime_and_session_engines_hide_parameters(self) -> None:
        broker = PostgresBroker(
            PostgresConfig(
                database_url=RUNTIME_URL,
                session_database_url=SESSION_URL,
                pgbouncer_transaction_mode=True,
            )
        )
        resources = broker._build_resources()
        session_engine = resources.session_async_engine()
        try:
            assert session_engine is not resources.engine
            assert resources.engine.sync_engine.hide_parameters is True
            assert session_engine.sync_engine.hide_parameters is True
        finally:
            await resources.engine.dispose()
            await session_engine.dispose()

    @pytest.mark.asyncio
    async def test_single_connection_engine_hides_parameters(self) -> None:
        broker = PostgresBroker(PostgresConfig(database_url=RUNTIME_URL))
        engine = broker.create_single_connection_engine()
        try:
            assert engine.sync_engine.hide_parameters is True
        finally:
            await engine.dispose()

    def test_live_check_engine_hides_parameters(self) -> None:
        """The live check's one engine of its own reads the server
        connection limit for the worker profiles; the connectivity step runs
        in the core and builds no engine."""
        app = Horsies(
            config=AppConfig(
                broker=PostgresConfig(database_url=RUNTIME_URL),
                worker_profiles={
                    'small': WorkerProfile(isolated=IsolatedChildren(count=1)),
                },
            ),
        )
        mock_engine = mock.MagicMock()
        mock_engine.dispose = mock.AsyncMock()
        reader = mock.AsyncMock(
            return_value=ServerConnectionLimit(max_connections=100, reserved=3),
        )

        with (
            mock.patch(
                'sqlalchemy.ext.asyncio.create_async_engine',
                return_value=mock_engine,
            ) as mock_create_engine,
            mock.patch.object(app, '_check_broker_connectivity', return_value=[]),
            mock.patch('horsies.core.app.read_server_connection_limit', reader),
        ):
            _ = app.check(live=True)

        assert mock_create_engine.call_args_list
        for engine_call in mock_create_engine.call_args_list:
            assert engine_call.kwargs['hide_parameters'] is True
