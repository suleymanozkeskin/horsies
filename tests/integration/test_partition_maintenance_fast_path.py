"""Set-based partition coverage through the maintenance database."""

from __future__ import annotations

from datetime import timedelta

import pytest
from sqlalchemy import event
from sqlalchemy import text

from horsies.core.history.heartbeats.partitioning import hourly_leaf_ref
from horsies.core.history.maintenance.coverage import (
    CoverageEnsured,
    CoverageEnsureFailed,
    StartupCoverageRefused,
    ensure_startup_coverage_in_database,
    maintain_partition_coverage,
)
from horsies.core.history.maintenance.database import (
    PartitionMaintenanceDatabase,
)
from horsies.core.history.names import HEARTBEAT_CLASS_KEY
from horsies.core.history.partitions.catalog import database_now
from tests.integration.task_history_harness import (
    HistorySchema,
    terminalization_schema_fixture,
)

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

partition_schema = terminalization_schema_fixture(
    'task_history_it_maintenance_fast_path',
    partitioned_heartbeats=True,
)


async def test_healthy_complete_set_has_constant_statement_budget(
    partition_schema: HistorySchema,
) -> None:
    declared = tuple(
        (f'budget_{number:02d}', timedelta(days=number + 1)) for number in range(10)
    )
    database = PartitionMaintenanceDatabase(partition_schema.engine)
    first = await maintain_partition_coverage(
        database,
        history_horizon_days=3,
        heartbeat_horizon_hours=6,
        declared_classes=declared,
    )
    assert isinstance(first, CoverageEnsured), first

    statements: list[str] = []

    def record_statement(
        _connection,
        _cursor,
        statement: str,
        _parameters,
        _context,
        _executemany,
    ) -> None:
        statements.append(statement)

    engine = partition_schema.engine
    event.listen(engine.sync_engine, 'before_cursor_execute', record_statement)
    try:
        healthy = await maintain_partition_coverage(
            database,
            history_horizon_days=3,
            heartbeat_horizon_hours=6,
            declared_classes=declared,
        )
    finally:
        event.remove(
            engine.sync_engine,
            'before_cursor_execute',
            record_statement,
        )

    assert isinstance(healthy, CoverageEnsured), healthy
    assert healthy.created_history_leaves == 0
    assert healthy.created_heartbeat_leaves == 0
    assert len(statements) <= 3, statements
    assert not any('advisory' in statement.lower() for statement in statements)


async def test_startup_refuses_when_current_heartbeat_leaf_is_busy(
    partition_schema: HistorySchema,
) -> None:
    database = PartitionMaintenanceDatabase(partition_schema.engine)
    holder = await partition_schema.engine.connect()
    transaction = await holder.begin()
    try:
        now = await database_now(holder)
        current = hourly_leaf_ref(now.replace(minute=0, second=0, microsecond=0))
        await holder.execute(
            text(
                'SELECT pg_advisory_xact_lock('
                'horsies_task_history_leaf_lock_key(:class_key, :anchor))'
            ),
            {
                'class_key': HEARTBEAT_CLASS_KEY,
                'anchor': current.bounds.lower,
            },
        )
        outcome = await ensure_startup_coverage_in_database(
            database,
            history_horizon_days=3,
            heartbeat_horizon_hours=6,
        )
        assert isinstance(outcome, StartupCoverageRefused), outcome
        refused = outcome.outcome
        assert isinstance(refused, CoverageEnsureFailed)
        assert not refused.heartbeat_covered_now
        assert 'LeafMaintenanceBusy' in refused.refusal
    finally:
        await transaction.rollback()
        await holder.close()
