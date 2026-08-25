"""Set-based partition coverage through the maintenance database."""

from __future__ import annotations

import asyncio
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
    HEARTBEATS_TABLE,
    HEARTBEAT_CLASS_KEY,
    LEAF_CATALOG,
    RETENTION_CLASSES,
    TASK_LOOKUP_MANIFEST,
)
from horsies.core.history.outcomes import LeafCreation
from horsies.core.history.partitions import manager as partition_manager
from horsies.core.history.partitions.catalog import (
    capture_partition_bound_utc,
    database_now,
    leaf_enqueued_index_name,
)
from horsies.core.history.partitions.locks import (
    IndexRelationState,
    IndexRemovalOutcome,
    remove_attached_index_for_repair,
)
from horsies.core.history.reads.publisher import (
    StagedLoaderPublisher,
    published_manifest_matches_catalog,
)
from tests.integration.task_history_harness import (
    HistorySchema,
    terminalization_schema_fixture,
)

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

partition_schema = terminalization_schema_fixture(
    'task_history_it_maintenance_fast_path',
    partitioned_heartbeats=True,
)


async def test_complete_probe_uses_24_hour_heartbeat_leaf_names() -> None:
    assert "'YYYY_MM_DD_HH24'" in coverage_module._COVERAGE_PROBE_SQL
    assert "'YYYY_MM_DD_HH'" not in coverage_module._COVERAGE_PROBE_SQL


@pytest.mark.parametrize(
    ('class_count', 'history_horizon', 'heartbeat_horizon'),
    ((1, 2, 2), (10, 8, 8), (50, 2, 8)),
)
async def test_healthy_complete_set_has_constant_statement_budget(
    partition_schema: HistorySchema,
    class_count: int,
    history_horizon: int,
    heartbeat_horizon: int,
) -> None:
    declared = tuple(
        (
            f'budget_{class_count:02d}_{number:02d}',
            timedelta(days=number + 1),
        )
        for number in range(class_count)
    )
    database = PartitionMaintenanceDatabase(
        partition_schema.engine, connection_capacity=2
    )
    first = await maintain_partition_coverage(
        database,
        history_horizon_days=history_horizon,
        heartbeat_horizon_hours=heartbeat_horizon,
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
            history_horizon_days=history_horizon,
            heartbeat_horizon_hours=heartbeat_horizon,
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


async def test_current_heartbeat_wrong_physical_range_refuses_startup(
    partition_schema: HistorySchema,
) -> None:
    database = PartitionMaintenanceDatabase(
        partition_schema.engine,
        connection_capacity=2,
    )
    first = await maintain_partition_coverage(
        database,
        history_horizon_days=2,
        heartbeat_horizon_hours=2,
    )
    assert isinstance(first, CoverageEnsured), first

    async with partition_schema.engine.begin() as connection:
        row = (
            await connection.execute(
                text(
                    f'SELECT leaf_name, id_index_name, lower_anchor, upper_anchor '
                    f'FROM {LEAF_CATALOG} '
                    'WHERE class_key = :heartbeat_class '
                    'AND lower_anchor <= statement_timestamp() '
                    'AND upper_anchor > statement_timestamp()'
                ),
                {'heartbeat_class': HEARTBEAT_CLASS_KEY},
            )
        ).one()
        await connection.execute(
            text(f'ALTER TABLE {HEARTBEATS_TABLE} ' f'DETACH PARTITION {row.leaf_name}')
        )
        await connection.execute(text(f'DROP TABLE {row.leaf_name}'))
        wrong_lower = row.lower_anchor - timedelta(days=100)
        wrong_upper = row.upper_anchor - timedelta(days=100)
        await connection.execute(
            text(
                f'CREATE TABLE {row.leaf_name} '
                f'PARTITION OF {HEARTBEATS_TABLE} '
                f"FOR VALUES FROM ('{wrong_lower.isoformat()}') "
                f"TO ('{wrong_upper.isoformat()}')"
            )
        )
        await connection.execute(
            text(
                f'CREATE INDEX {row.id_index_name} ON {row.leaf_name} '
                '(task_id, role, sent_at DESC)'
            )
        )
        wrong_bound = await capture_partition_bound_utc(
            connection,
            str(row.leaf_name),
        )
        await connection.execute(
            text(
                f'UPDATE {LEAF_CATALOG} SET partition_bound = :bound '
                'WHERE leaf_name = :leaf_name'
            ),
            {'bound': wrong_bound, 'leaf_name': row.leaf_name},
        )

    startup = await ensure_startup_coverage_in_database(
        database,
        history_horizon_days=2,
        heartbeat_horizon_hours=2,
    )
    assert isinstance(startup, StartupCoverageRefused), startup
    assert isinstance(startup.outcome, CoverageEnsureFailed)
    assert not startup.outcome.heartbeat_covered_now
    assert 'requested bounds' in startup.outcome.refusal


async def test_coverage_repairs_noncanonical_index_metadata(
    partition_schema: HistorySchema,
) -> None:
    database = PartitionMaintenanceDatabase(
        partition_schema.engine,
        connection_capacity=2,
    )
    first = await maintain_partition_coverage(
        database,
        history_horizon_days=2,
        heartbeat_horizon_hours=2,
    )
    assert isinstance(first, CoverageEnsured), first

    async with partition_schema.engine.begin() as connection:
        heartbeat = (
            await connection.execute(
                text(
                    f'SELECT leaf_name, id_index_name FROM {LEAF_CATALOG} '
                    'WHERE class_key = :heartbeat_class '
                    'AND lower_anchor <= statement_timestamp() '
                    'AND upper_anchor > statement_timestamp()'
                ),
                {'heartbeat_class': HEARTBEAT_CLASS_KEY},
            )
        ).one()
        history = (
            await connection.execute(
                text(
                    f'SELECT leaf_name, id_index_name FROM {LEAF_CATALOG} '
                    'WHERE class_key <> :heartbeat_class '
                    'AND detached_at IS NULL AND dropped_at IS NULL '
                    'ORDER BY lower_anchor, leaf_name LIMIT 1'
                ),
                {'heartbeat_class': HEARTBEAT_CLASS_KEY},
            )
        ).one()
        ordering_name = leaf_enqueued_index_name(str(history.leaf_name))
        for index_name in (
            heartbeat.id_index_name,
            history.id_index_name,
            ordering_name,
        ):
            await connection.execute(text(f'DROP INDEX {index_name}'))
        await connection.execute(
            text(
                f'CREATE INDEX {heartbeat.id_index_name} '
                f'ON {heartbeat.leaf_name} '
                '(task_id, role varchar_pattern_ops, sent_at DESC)'
            )
        )
        await connection.execute(
            text(
                f'CREATE INDEX {history.id_index_name} '
                f'ON {history.leaf_name} (task_id DESC)'
            )
        )
        await connection.execute(
            text(
                f'CREATE INDEX {ordering_name} '
                f'ON {history.leaf_name} (enqueued_at DESC)'
            )
        )

    outcome = await maintain_partition_coverage(
        database,
        history_horizon_days=2,
        heartbeat_horizon_hours=2,
    )
    assert isinstance(outcome, CoverageEnsured), outcome
    async with partition_schema.engine.connect() as connection:
        definitions = {
            str(row.index_name): str(row.definition)
            for row in (
                await connection.execute(
                    text(
                        'SELECT relation.relname AS index_name, '
                        'pg_get_indexdef(relation.oid) AS definition '
                        'FROM pg_class AS relation '
                        'WHERE relation.oid IN ('
                        'to_regclass(:heartbeat_index), '
                        'to_regclass(:history_index), '
                        'to_regclass(:ordering_index))'
                    ),
                    {
                        'heartbeat_index': heartbeat.id_index_name,
                        'history_index': history.id_index_name,
                        'ordering_index': ordering_name,
                    },
                )
            ).all()
        }
    assert definitions[str(heartbeat.id_index_name)].endswith(
        'USING btree (task_id, role, sent_at DESC)'
    )
    assert definitions[str(history.id_index_name)].endswith('USING btree (task_id)')
    assert definitions[ordering_name].endswith('USING btree (enqueued_at)')


async def test_coverage_repairs_an_invalid_index(
    partition_schema: HistorySchema,
) -> None:
    database = PartitionMaintenanceDatabase(
        partition_schema.engine,
        connection_capacity=2,
    )
    first = await maintain_partition_coverage(
        database,
        history_horizon_days=2,
        heartbeat_horizon_hours=2,
    )
    assert isinstance(first, CoverageEnsured), first
    async with partition_schema.engine.begin() as connection:
        index_name = (
            await connection.execute(
                text(
                    f'SELECT id_index_name FROM {LEAF_CATALOG} '
                    'WHERE class_key <> :heartbeat_class '
                    'AND detached_at IS NULL AND dropped_at IS NULL '
                    'ORDER BY lower_anchor, leaf_name LIMIT 1'
                ),
                {'heartbeat_class': HEARTBEAT_CLASS_KEY},
            )
        ).scalar_one()
        await connection.execute(
            text(
                'UPDATE pg_index SET indisvalid = FALSE '
                'WHERE indexrelid = to_regclass(:index_name)'
            ),
            {'index_name': index_name},
        )

    outcome = await maintain_partition_coverage(
        database,
        history_horizon_days=2,
        heartbeat_horizon_hours=2,
    )
    assert isinstance(outcome, CoverageEnsured), outcome
    async with partition_schema.engine.connect() as connection:
        valid = (
            await connection.execute(
                text(
                    'SELECT indisvalid FROM pg_index '
                    'WHERE indexrelid = to_regclass(:index_name)'
                ),
                {'index_name': index_name},
            )
        ).scalar_one()
    assert valid is True


async def test_stale_manifest_metadata_is_republished(
    partition_schema: HistorySchema,
) -> None:
    database = PartitionMaintenanceDatabase(
        partition_schema.engine,
        connection_capacity=2,
    )
    first = await maintain_partition_coverage(
        database,
        history_horizon_days=2,
        heartbeat_horizon_hours=2,
    )
    assert isinstance(first, CoverageEnsured), first
    async with partition_schema.engine.begin() as connection:
        await connection.execute(
            text(
                f'UPDATE {TASK_LOOKUP_MANIFEST} '
                'SET probe_position = probe_position + 100'
            )
        )

    outcome = await maintain_partition_coverage(
        database,
        history_horizon_days=2,
        heartbeat_horizon_hours=2,
    )
    assert isinstance(outcome, CoverageEnsured), outcome
    assert outcome.republished
    async with partition_schema.engine.connect() as connection:
        assert await published_manifest_matches_catalog(connection)


async def test_failed_final_publication_is_retried(
    partition_schema: HistorySchema,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database = PartitionMaintenanceDatabase(
        partition_schema.engine,
        connection_capacity=2,
    )
    first = await maintain_partition_coverage(
        database,
        history_horizon_days=1,
        heartbeat_horizon_hours=1,
    )
    assert isinstance(first, CoverageEnsured), first
    original = StagedLoaderPublisher.republish
    calls = 0

    async def fail_first_publication(
        self: StagedLoaderPublisher,
        connection,
    ):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise RuntimeError('injected final publication failure')
        return await original(self, connection)

    monkeypatch.setattr(
        StagedLoaderPublisher,
        'republish',
        fail_first_publication,
    )
    with pytest.raises(RuntimeError, match='injected final publication failure'):
        await maintain_partition_coverage(
            database,
            history_horizon_days=2,
            heartbeat_horizon_hours=2,
        )

    async with partition_schema.engine.connect() as connection:
        committed_unpublished = (
            await connection.execute(
                text(
                    f'SELECT count(*) FROM {LEAF_CATALOG} AS catalog '
                    'WHERE catalog.class_key <> :heartbeat_class '
                    'AND catalog.detached_at IS NULL '
                    'AND catalog.dropped_at IS NULL '
                    'AND to_regclass(catalog.leaf_name) IS NOT NULL '
                    f'AND NOT EXISTS (SELECT 1 FROM {TASK_LOOKUP_MANIFEST} '
                    'AS manifest WHERE manifest.leaf_name = catalog.leaf_name)'
                ),
                {'heartbeat_class': HEARTBEAT_CLASS_KEY},
            )
        ).scalar_one()
    assert committed_unpublished > 0

    retry = await maintain_partition_coverage(
        database,
        history_horizon_days=2,
        heartbeat_horizon_hours=2,
    )
    assert isinstance(retry, CoverageEnsured), retry
    assert retry.republished
    assert calls == 2


async def test_index_name_reuse_after_ownership_read_does_not_drop_foreign_index(
    partition_schema: HistorySchema,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database = PartitionMaintenanceDatabase(
        partition_schema.engine,
        connection_capacity=2,
    )
    first = await maintain_partition_coverage(
        database,
        history_horizon_days=2,
        heartbeat_horizon_hours=2,
    )
    assert isinstance(first, CoverageEnsured), first
    async with partition_schema.engine.begin() as connection:
        history = (
            await connection.execute(
                text(
                    f'SELECT leaf_name, id_index_name FROM {LEAF_CATALOG} '
                    'WHERE class_key <> :heartbeat_class '
                    'AND detached_at IS NULL AND dropped_at IS NULL '
                    'ORDER BY lower_anchor, leaf_name LIMIT 1'
                ),
                {'heartbeat_class': HEARTBEAT_CLASS_KEY},
            )
        ).one()
        await connection.execute(text(f'DROP INDEX {history.id_index_name}'))
        await connection.execute(
            text(
                f'CREATE INDEX {history.id_index_name} '
                f'ON {history.leaf_name} (task_id DESC)'
            )
        )
        await connection.execute(
            text('CREATE TABLE horsies_index_race_foreign (value integer)')
        )

    ownership_read = asyncio.Event()
    resume_repair = asyncio.Event()
    original_state_reader = partition_manager.read_index_relation_state
    paused = False

    async def pause_after_ownership_read(
        connection,
        *,
        leaf_name: str,
        index_name: str,
    ) -> IndexRelationState:
        nonlocal paused
        state = await original_state_reader(
            connection,
            leaf_name=leaf_name,
            index_name=index_name,
        )
        if index_name == history.id_index_name and not paused:
            paused = True
            ownership_read.set()
            await resume_repair.wait()
        return state

    monkeypatch.setattr(
        partition_manager,
        'read_index_relation_state',
        pause_after_ownership_read,
    )
    repair = asyncio.create_task(
        maintain_partition_coverage(
            database,
            history_horizon_days=2,
            heartbeat_horizon_hours=2,
        )
    )
    await asyncio.wait_for(ownership_read.wait(), timeout=5)
    async with partition_schema.engine.begin() as connection:
        await connection.execute(
            text(
                f'ALTER INDEX {history.id_index_name} '
                'RENAME TO horsies_index_race_original'
            )
        )
        await connection.execute(
            text(
                f'CREATE INDEX {history.id_index_name} '
                'ON horsies_index_race_foreign (value)'
            )
        )
    resume_repair.set()
    outcome = await asyncio.wait_for(repair, timeout=10)
    assert isinstance(outcome, CoverageEnsureFailed), outcome
    assert 'belongs to another relation' in outcome.refusal

    async with partition_schema.engine.connect() as connection:
        owners = {
            str(row.index_name): str(row.owner)
            for row in (
                await connection.execute(
                    text(
                        'SELECT index_relation.relname AS index_name, '
                        'index_state.indrelid::regclass::text AS owner '
                        'FROM pg_index AS index_state '
                        'JOIN pg_class AS index_relation '
                        'ON index_relation.oid = index_state.indexrelid '
                        'WHERE index_relation.relname IN ('
                        ':canonical_name, :original_name)'
                    ),
                    {
                        'canonical_name': history.id_index_name,
                        'original_name': 'horsies_index_race_original',
                    },
                )
            ).all()
        }
    assert owners[str(history.id_index_name)] == 'horsies_index_race_foreign'
    assert owners['horsies_index_race_original'] == str(history.leaf_name)


async def test_index_relation_lock_wait_is_bounded_and_restores_timeout(
    partition_schema: HistorySchema,
) -> None:
    async with partition_schema.engine.begin() as connection:
        await connection.execute(
            text('CREATE TABLE horsies_index_lock_target (value integer)')
        )
        await connection.execute(
            text(
                'CREATE INDEX horsies_index_lock_canonical '
                'ON horsies_index_lock_target (value DESC)'
            )
        )

    holder = await partition_schema.engine.connect()
    holder_transaction = await holder.begin()
    try:
        await holder.execute(
            text('ALTER INDEX horsies_index_lock_canonical ' 'SET (fillfactor = 70)')
        )
        async with partition_schema.engine.begin() as contender:
            await contender.execute(text("SET LOCAL lock_timeout = '7s'"))
            started_at = asyncio.get_running_loop().time()
            removal = await asyncio.wait_for(
                remove_attached_index_for_repair(
                    contender,
                    leaf_name='horsies_index_lock_target',
                    index_name='horsies_index_lock_canonical',
                ),
                timeout=3,
            )
            elapsed = asyncio.get_running_loop().time() - started_at
            restored_timeout = (
                await contender.execute(text("SELECT current_setting('lock_timeout')"))
            ).scalar_one()
        assert removal is IndexRemovalOutcome.BUSY
        assert elapsed < 2.75
        assert restored_timeout == '7s'
    finally:
        await holder_transaction.rollback()
        await holder.close()


async def test_fast_path_rejects_stale_index_schema_version(
    partition_schema: HistorySchema,
) -> None:
    database = PartitionMaintenanceDatabase(
        partition_schema.engine, connection_capacity=2
    )
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
    database = PartitionMaintenanceDatabase(
        partition_schema.engine, connection_capacity=2
    )
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
    database = PartitionMaintenanceDatabase(
        partition_schema.engine, connection_capacity=2
    )
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
    database = PartitionMaintenanceDatabase(
        partition_schema.engine, connection_capacity=2
    )
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
    database = PartitionMaintenanceDatabase(
        partition_schema.engine, connection_capacity=2
    )
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


async def test_busy_coverage_gate_is_nonfatal_with_current_heartbeat(
    partition_schema: HistorySchema,
) -> None:
    database = PartitionMaintenanceDatabase(
        partition_schema.engine,
        connection_capacity=2,
    )
    first = await maintain_partition_coverage(
        database,
        history_horizon_days=2,
        heartbeat_horizon_hours=2,
    )
    assert isinstance(first, CoverageEnsured), first
    async with partition_schema.engine.begin() as connection:
        damaged_index = (
            await connection.execute(
                text(
                    f'SELECT id_index_name FROM {LEAF_CATALOG} '
                    'WHERE class_key <> :heartbeat_class '
                    'AND detached_at IS NULL AND dropped_at IS NULL '
                    'ORDER BY lower_anchor, leaf_name LIMIT 1'
                ),
                {'heartbeat_class': HEARTBEAT_CLASS_KEY},
            )
        ).scalar_one()
        await connection.execute(text(f'DROP INDEX {damaged_index}'))

    holder = await partition_schema.engine.connect()
    transaction = await holder.begin()
    try:
        await holder.execute(
            text(
                'SELECT pg_advisory_xact_lock(hashtextextended('
                "'horsies:partition-coverage:v1', 1601))"
            )
        )
        outcome = await asyncio.wait_for(
            maintain_partition_coverage(
                database,
                history_horizon_days=2,
                heartbeat_horizon_hours=2,
            ),
            timeout=2,
        )
        assert isinstance(outcome, CoverageEnsureFailed), outcome
        assert outcome.stage == 'coverage_gate_busy'
        assert outcome.heartbeat_covered_now
    finally:
        await transaction.rollback()
        await holder.close()


async def test_busy_coverage_gate_refuses_startup_without_current_heartbeat(
    partition_schema: HistorySchema,
) -> None:
    database = PartitionMaintenanceDatabase(
        partition_schema.engine,
        connection_capacity=2,
    )
    first = await maintain_partition_coverage(
        database,
        history_horizon_days=2,
        heartbeat_horizon_hours=2,
    )
    assert isinstance(first, CoverageEnsured), first
    async with partition_schema.engine.begin() as connection:
        current_index = (
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
        await connection.execute(text(f'DROP INDEX {current_index}'))

    holder = await partition_schema.engine.connect()
    transaction = await holder.begin()
    try:
        await holder.execute(
            text(
                'SELECT pg_advisory_xact_lock(hashtextextended('
                "'horsies:partition-coverage:v1', 1601))"
            )
        )
        startup = await asyncio.wait_for(
            ensure_startup_coverage_in_database(
                database,
                history_horizon_days=2,
                heartbeat_horizon_hours=2,
            ),
            timeout=2,
        )
        assert isinstance(startup, StartupCoverageRefused), startup
        assert isinstance(startup.outcome, CoverageEnsureFailed)
        assert startup.outcome.stage == 'coverage_gate_busy'
        assert not startup.outcome.heartbeat_covered_now
    finally:
        await transaction.rollback()
        await holder.close()
