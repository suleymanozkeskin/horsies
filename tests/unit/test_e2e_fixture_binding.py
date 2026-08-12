"""Topology guards for the shared E2E broker fixtures."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from tests.e2e.conftest import (
    _initialize_session_broker,  # pyright: ignore[reportPrivateUsage]
)


@pytest.mark.unit
@pytest.mark.asyncio
async def test_pgbouncer_lane_does_not_initialize_default_brokers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv('HORSIES_PGBOUNCER_TEST', '1')
    broker = MagicMock()
    broker.ensure_schema_initialized = AsyncMock()
    broker.listener.start = AsyncMock()

    await _initialize_session_broker(broker, seed_history=True)

    broker.ensure_schema_initialized.assert_not_awaited()
    broker.listener.start.assert_not_awaited()
