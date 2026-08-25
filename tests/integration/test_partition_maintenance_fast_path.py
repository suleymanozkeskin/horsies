"""Set-based partition coverage through the maintenance database."""

from __future__ import annotations

from datetime import timedelta

import pytest
from sqlalchemy import event
from sqlalchemy import text

from horsies.core.history.commands import LeafRef
from horsies.core.history.heartbeats.partitioning import hourly_leaf_ref
from horsies.core.history.maintenance import coverage as coverage_module
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
from horsies.core.history.errors import HistoryContractError
from horsies.core.history.names import (
    HEARTBEAT_CLASS_KEY,
    LEAF_CATALOG,
    RETENTION_CLASSES,
)
from horsies.core.history.outcomes import LeafCreation
from horsies.core.history.partitions.catalog import database_now
from horsies.core.history.reads.publisher import StagedLoaderPublisher
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


async def test_fast_path_rejects_stale_index_schema_version(
    partition_schema: HistorySchema,
) -> None:
    database = PartitionMaintenanceDatabase(partition_schema.engine)
    first = await maintain_partition_coverage(
        database,
        history_horizon_days=3,
        heartbeat_horizon_hours=6,
    )
    assert isinstance(first, CoverageEnsured), first

    async with partition_schema.engine.begin() as connection:
        update = await connection.execute(
            text(
                f'UPDATE {LEAF_CATALOG} '
                'SET index_schema_version = 0 '
                'WHERE class_key = :class_key '
                'AND lower_anchor <= statement_timestamp() '
                'AND upper_anchor > statement_timestamp()'
            ),
            {'class_key': HEARTBEAT_CLASS_KEY},
        )
        assert update.rowcount == 1

    outcome = await maintain_partition_coverage(
        database,
        history_horizon_days=3,
        heartbeat_horizon_hours=6,
    )
    assert isinstance(outcome, CoverageEnsureFailed), outcome
    assert outcome.class_key == HEARTBEAT_CLASS_KEY
    assert 'METADATA_MISMATCH' in outcome.refusal


async def test_fast_path_rejects_null_heartbeat_registration(
    partition_schema: HistorySchema,
) -> None:
    database = PartitionMaintenanceDatabase(partition_schema.engine)
    first = await maintain_partition_coverage(
        database,
        history_horizon_days=3,
        heartbeat_horizon_hours=6,
    )
    assert isinstance(first, CoverageEnsured), first

    async with partition_schema.engine.begin() as connection:
        await connection.execute(
            text(
                f'UPDATE {RETENTION_CLASSES} '
                'SET duration = NULL, partition_interval = NULL, '
                'finite_parent_name = NULL '
                'WHERE class_key = :class_key'
            ),
            {'class_key': HEARTBEAT_CLASS_KEY},
        )

    with pytest.raises(HistoryContractError):
        await maintain_partition_coverage(
            database,
            history_horizon_days=3,
            heartbeat_horizon_hours=6,
        )


async def test_fast_path_compares_fractional_duration_exactly(
    partition_schema: HistorySchema,
) -> None:
    database = PartitionMaintenanceDatabase(partition_schema.engine)
    declared = (('fractional_duration', timedelta(seconds=1, microseconds=1)),)
    first = await maintain_partition_coverage(
        database,
        history_horizon_days=3,
        heartbeat_horizon_hours=6,
        declared_classes=declared,
    )
    assert isinstance(first, CoverageEnsured), first

    healthy = await maintain_partition_coverage(
        database,
        history_horizon_days=3,
        heartbeat_horizon_hours=6,
        declared_classes=declared,
    )
    assert isinstance(healthy, CoverageEnsured), healthy


async def test_history_error_does_not_stop_heartbeat_repair(
    partition_schema: HistorySchema,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database = PartitionMaintenanceDatabase(partition_schema.engine)
    first = await maintain_partition_coverage(
        database,
        history_horizon_days=3,
        heartbeat_horizon_hours=6,
    )
    assert isinstance(first, CoverageEnsured), first

    async with partition_schema.engine.begin() as connection:
        history_index = (
            await connection.execute(
                text(
                    f'SELECT id_index_name FROM {LEAF_CATALOG} '
                    'WHERE class_key <> :heartbeat_class '
                    'AND detached_at IS NULL AND dropped_at IS NULL '
                    'AND lower_anchor <= statement_timestamp() '
                    'AND upper_anchor > statement_timestamp() '
                    'ORDER BY class_key LIMIT 1'
                ),
                {'heartbeat_class': HEARTBEAT_CLASS_KEY},
            )
        ).scalar_one()
        heartbeat_index = (
            await connection.execute(
                text(
                    f'SELECT id_index_name FROM {LEAF_CATALOG} '
                    'WHERE class_key = :heartbeat_class '
                    'AND lower_anchor <= statement_timestamp() '
                    'AND upper_anchor > statement_timestamp()'
                ),
                {'heartbeat_class': HEARTBEAT_CLASS_KEY},
            )
        ).scalar_one()
        await connection.execute(text(f'DROP INDEX {history_index}'))
        await connection.execute(text(f'DROP INDEX {heartbeat_index}'))

    original_heartbeat = coverage_module._maintain_heartbeat_leaf
    heartbeat_repairs = 0

    async def fail_history(
        _database: PartitionMaintenanceDatabase,
        _leaf: LeafRef,
        _publisher: StagedLoaderPublisher,
    ) -> LeafCreation:
        raise RuntimeError('history maintenance failed')

    async def record_heartbeat(
        maintenance_database: PartitionMaintenanceDatabase,
        leaf: LeafRef,
    ) -> LeafCreation:
        nonlocal heartbeat_repairs
        heartbeat_repairs += 1
        return await original_heartbeat(maintenance_database, leaf)

    monkeypatch.setattr(
        coverage_module,
        '_maintain_history_leaf',
        fail_history,
    )
    monkeypatch.setattr(
        coverage_module,
        '_maintain_heartbeat_leaf',
        record_heartbeat,
    )

    outcome = await maintain_partition_coverage(
        database,
        history_horizon_days=3,
        heartbeat_horizon_hours=6,
    )
    assert isinstance(outcome, CoverageEnsureFailed), outcome
    assert heartbeat_repairs == 1
    assert outcome.heartbeat_covered_now

    async with partition_schema.engine.connect() as connection:
        assert (
            await connection.execute(
                text('SELECT to_regclass(:index_name) IS NOT NULL'),
                {'index_name': heartbeat_index},
            )
        ).scalar_one()


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
