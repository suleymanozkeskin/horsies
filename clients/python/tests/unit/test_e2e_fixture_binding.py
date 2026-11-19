"""Topology guards for the shared E2E broker fixtures."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from tests.e2e.helpers import session_broker
from tests.e2e.helpers.session_broker import initialize_session_broker


@pytest.mark.unit
@pytest.mark.asyncio
async def test_pgbouncer_lane_does_not_initialize_default_brokers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv('HORSIES_PGBOUNCER_TEST', '1')
    broker = MagicMock()
    broker.ensure_schema_initialized = AsyncMock()
    database_access = MagicMock()
    monkeypatch.setattr(session_broker, 'current_resources', database_access)

    await initialize_session_broker(broker, seed_history=True)

    broker.ensure_schema_initialized.assert_not_awaited()
    database_access.assert_not_called()
