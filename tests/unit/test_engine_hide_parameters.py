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

    def test_live_check_opens_no_engine(self) -> None:
        """Both live phases run in the core on one core handle: the
        connectivity step and the worker connection step build no engine."""
        app = Horsies(
            config=AppConfig(
                broker=PostgresConfig(database_url=RUNTIME_URL),
                worker_profiles={
                    'small': WorkerProfile(isolated=IsolatedChildren(count=1)),
                },
            ),
        )

        class _Passed:
            """Stands for ``horsies._core.ConnectivityPassed``."""

        class _Handle:
            def __init__(self, manifest_json: str, *, pool_profile: str) -> None:
                _ = (manifest_json, pool_profile)

            def check_connectivity(self) -> _Passed:
                return _Passed()

            def check_worker_connections(self, profiles_json: str) -> list[object]:
                _ = profiles_json
                return []

            def close(self) -> None:
                return None

        with (
            mock.patch('sqlalchemy.ext.asyncio.create_async_engine') as mock_create_engine,
            mock.patch('horsies.core.app.CoreHandle', _Handle),
            mock.patch('horsies.core.app.ConnectivityPassed', _Passed),
        ):
            assert app.check(live=True) == []

        mock_create_engine.assert_not_called()
