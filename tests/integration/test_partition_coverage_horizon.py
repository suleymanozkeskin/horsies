"""A heartbeat-horizon retune is a coverage success, not a failure.

`register_heartbeat_class` performs the duration UPDATE and then returns
`HeartbeatHorizonUpdated` — the class is correct from that moment. The
coverage pass must treat it like any other success: proceed to leaf
creation and report `CoverageEnsured`, so a documented retune of
`heartbeat_leaf_horizon_hours` never logs an error, never publishes a
failed health state, and never skips a tick of leaf creation.
"""

from __future__ import annotations

from datetime import timedelta

import pytest
from sqlalchemy import text

from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.history.maintenance.coverage import (
    CoverageEnsured,
    ensure_partition_coverage,
)
from horsies.core.history.names import RETENTION_CLASSES
from horsies.core.history.heartbeats.partitioning import HEARTBEAT_CLASS_KEY

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


async def _stored_horizon(broker: PostgresBroker) -> timedelta:
    async with broker.async_engine.connect() as connection:
        return (
            await connection.execute(
                text(
                    f'SELECT duration FROM {RETENTION_CLASSES} '
                    'WHERE class_key = :key'
                ),
                {'key': HEARTBEAT_CLASS_KEY},
            )
        ).scalar_one()


async def test_horizon_retune_is_a_success_not_a_failure(
    broker: PostgresBroker,
) -> None:
    async with broker.async_engine.begin() as connection:
        first = await ensure_partition_coverage(
            connection, history_horizon_days=2, heartbeat_horizon_hours=3
        )
    assert isinstance(first, CoverageEnsured), first

    # The second pass changes the configured horizon, forcing the
    # update path deterministically regardless of the fixture's state.
    async with broker.async_engine.begin() as connection:
        second = await ensure_partition_coverage(
            connection, history_horizon_days=2, heartbeat_horizon_hours=6
        )
    assert isinstance(second, CoverageEnsured), (
        'a horizon retune must be a coverage success; the update has '
        f'already run when it is reported: {second!r}'
    )
    assert await _stored_horizon(broker) == timedelta(hours=6)
