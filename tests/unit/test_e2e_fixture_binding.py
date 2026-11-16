"""Topology guards for the shared E2E broker fixtures."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from horsies.core.brokers.resources import ListenerEnabled
from horsies.core.types.result import Ok
from tests.broker_resources_support import fake_resources
from tests.e2e.helpers.session_broker import initialize_session_broker


@pytest.mark.unit
@pytest.mark.asyncio
async def test_pgbouncer_lane_does_not_initialize_default_brokers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv('HORSIES_PGBOUNCER_TEST', '1')
    broker = MagicMock()
    broker.ensure_schema_initialized = AsyncMock()
    listener = MagicMock()
    listener.start = AsyncMock()
    broker._resources_for_loop.return_value = Ok(
        fake_resources(listener=ListenerEnabled(listener=listener)),
    )

    await initialize_session_broker(broker, seed_history=True)

    broker.ensure_schema_initialized.assert_not_awaited()
    listener.start.assert_not_awaited()
