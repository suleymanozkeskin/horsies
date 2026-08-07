"""Operational qualification for registry and partition maintenance."""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from uuid import uuid4

from sqlalchemy import text
from sqlalchemy.exc import DBAPIError
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

from tests.perf.statistics import percentile_ms
from tests.task_history_prototypes.evidence import (
    EvidenceConditions,
    EvidenceRunKind,
    collect_operational_conditions,
    refresh_cumulative_statistics,
)
from tests.task_history_prototypes.identity_schema import (
    install_identity_candidates,
)
from tests.task_history_prototypes.partition_manager import (
    LeafState,
    create_daily_history_leaf,
    detach_history_leaf_concurrently,
    drop_detached_history_leaf,
    finalize_interrupted_detach,
    install_partition_manager_prototype,
)
from tests.task_history_prototypes.qualification_io import (
    QualificationProgress,
    QualificationProgressReporter,
)
from tests.task_history_prototypes.schema import (
    PrototypeSchema,
    install_archive_candidates,
    remove_archive_candidates,
)
from tests.task_history_prototypes.workflow_schema import (
    install_workflow_recovery_prototype,
)


_CLASS_KEY = 'finite_30d_v1'
_OLD_LEAF = 'history_aggregate_finite_2026_06_01'
_OLD_LOWER = datetime(2026, 6, 1, tzinfo=timezone.utc)
_OLD_UPPER = datetime(2026, 6, 2, tzinfo=timezone.utc)
_CURRENT_LEAF = 'history_aggregate_finite_2026_08_05'
_CURRENT_AT = datetime(2026, 8, 5, 12, tzinfo=timezone.utc)


@dataclass(frozen=True, slots=True)
class RegistryMaintenanceMeasurement:
    seeded_rows: int
    batch_size: int
    cleanup_seconds: float
    cleanup_rows_per_second: float
    cleanup_wal_bytes: int
    remaining_rows: int
    dead_tuples_after_observation: int
    live_tuples_after_observation: int
    autovacuum_count_delta: int
    observation_seconds: float
    bounded_growth_passed: bool
    vacuum_keep_up_passed: bool


@dataclass(frozen=True, slots=True)
class PartitionLifecycleMeasurement:
    attached_leaves: int
    inventory_seconds: float
    inventory_passed: bool
    create_leaf_seconds: float
    create_leaf_passed: bool
    detach_seconds: float
    detach_passed: bool
    drop_seconds: float
    drop_wal_bytes: int
    drop_passed: bool
    long_reader_baseline_p99_ms: float
    long_reader_contended_p99_ms: float
    long_reader_writer_p99_ms: float
    long_reader_writer_passed: bool
    interrupted_detach_finalized: bool
    observed_blockers: tuple[dict[str, object], ...]
    forever_rows: int
    forever_dead_tuples: int
    forever_freeze_age_before: int
    forever_freeze_age_after: int
    forever_vacuum_overlapped_reads: bool
    forever_read_p99_ms: float
    forever_freeze_passed: bool
    forever_read_passed: bool



# Dropping a 512-leaf schema in one statement asks PostgreSQL to hold a lock on
# every relation and index it will remove, which exhausts the lock table at
# production-shaped settings. Raising `max_locks_per_transaction` to make the
# single statement fit would change the measured environment, so the disposal
# is bounded instead: leaves go first, in batches, each batch its own
# transaction, and only the remainder reaches `DROP SCHEMA`.
_DISPOSAL_BATCH = 32
_DISPOSAL_MAX_PASSES = 64


@dataclass(frozen=True, slots=True)
class CleanupOutcome:
    """What disposing of the disposable schema achieved, and what it did not.

    Cleanup is harness hygiene, not a measured property, so a failure here is
    recorded rather than raised: evidence that was already collected must
    survive a schema that would not drop.
    """

    schema: str
    leaves_dropped: int
    batches: int
    passes: int
    schema_dropped: bool
    warning: str | None

    @property
    def clean(self) -> bool:
        return self.schema_dropped and self.warning is None


async def _leaf_partitions(
    connection: AsyncConnection,
    schema: PrototypeSchema,
) -> tuple[str, ...]:
    """Partitions in this schema that are not themselves partitioned.

    Leaf-most first: dropping a sub-partitioned parent would cascade to
    children and take the same wide lock set the batching exists to avoid.
    """
    rows = (
        await connection.execute(
            text(
                """
                SELECT child.relname AS name
                FROM pg_inherits AS inheritance
                JOIN pg_class AS child ON child.oid = inheritance.inhrelid
                JOIN pg_namespace AS namespace
                    ON namespace.oid = child.relnamespace
                WHERE namespace.nspname = :schema
                  AND child.relkind = 'r'
                  AND NOT EXISTS (
                      SELECT 1 FROM pg_inherits AS grandchild
                      WHERE grandchild.inhparent = child.oid
                  )
                ORDER BY child.relname
                """
            ),
            {'schema': schema.name},
        )
    ).all()
    return tuple(str(row.name) for row in rows)


async def dispose_evidence_schema(
    engine: AsyncEngine,
    schema: PrototypeSchema,
    *,
    batch_size: int = _DISPOSAL_BATCH,
) -> CleanupOutcome:
    """Remove the disposable schema without ever discarding evidence.

    Every failure mode returns an outcome carrying the reason. The caller has
    already measured what it came to measure by the time this runs, and a
    cleanup that raised would destroy that work — which is exactly what
    happened when a single `DROP SCHEMA ... CASCADE` over 512 leaves exhausted
    the lock table on both supported majors.
    """
    leaves_dropped = 0
    batches = 0
    passes = 0
    try:
        for _ in range(_DISPOSAL_MAX_PASSES):
            async with engine.connect() as connection:
                leaves = await _leaf_partitions(connection, schema)
            if not leaves:
                break
            passes += 1
            for start in range(0, len(leaves), batch_size):
                batch = leaves[start : start + batch_size]
                async with engine.connect() as connection:
                    for name in batch:
                        await connection.execute(
                            text(
                                f'DROP TABLE IF EXISTS '
                                f'{schema.sql}."{name}" CASCADE'
                            )
                        )
                    await connection.commit()
                leaves_dropped += len(batch)
                batches += 1
        else:
            return CleanupOutcome(
                schema=schema.name,
                leaves_dropped=leaves_dropped,
                batches=batches,
                passes=passes,
                schema_dropped=False,
                warning=(
                    f'{_DISPOSAL_MAX_PASSES} disposal passes did not exhaust '
                    f'the partitions in {schema.name}'
                ),
            )
        async with engine.connect() as connection:
            await remove_archive_candidates(connection, schema)
            await connection.commit()
    except Exception as error:
        # Deliberately broad. The contract of this function is that evidence
        # already collected survives, so no failure of the disposal may leave
        # here as an exception — a narrower clause would let an unanticipated
        # error reinstate exactly the defect this replaced. The class name is
        # recorded so the artifact says what went wrong rather than only that
        # something did.
        return CleanupOutcome(
            schema=schema.name,
            leaves_dropped=leaves_dropped,
            batches=batches,
            passes=passes,
            schema_dropped=False,
            warning=(
                f'disposal of {schema.name} failed after {leaves_dropped} '
                f'leaves in {batches} batches: {error.__class__.__name__}: '
                f'{error}'
            ),
        )
    return CleanupOutcome(
        schema=schema.name,
        leaves_dropped=leaves_dropped,
        batches=batches,
        passes=passes,
        schema_dropped=True,
        warning=None,
    )


@dataclass(frozen=True, slots=True)
class OperationalMaintenanceEvidence:
    conditions: EvidenceConditions
    workload: dict[str, int]
    registry: RegistryMaintenanceMeasurement
    partitions: PartitionLifecycleMeasurement
    cleanup: CleanupOutcome
    verdict: bool


async def collect_operational_maintenance_evidence(
    engine: AsyncEngine,
    *,
    commit: str,
    run_kind: EvidenceRunKind,
    server_image: str,
    host_description: str,
    storage_description: str,
    demo_quiesced: bool,
    registry_rows: int,
    registry_batch_size: int,
    history_rows: int,
    forever_rows: int,
    attached_leaves: int,
    observation_seconds: int,
    progress: QualificationProgressReporter | None,
) -> OperationalMaintenanceEvidence:
    _validate_workload(
        run_kind=run_kind,
        registry_rows=registry_rows,
        history_rows=history_rows,
        forever_rows=forever_rows,
        attached_leaves=attached_leaves,
        observation_seconds=observation_seconds,
    )
    reporter = progress or QualificationProgressReporter()
    schema = PrototypeSchema(f'operational_evidence_{uuid4().hex[:10]}')
    async with engine.connect() as connection:
        conditions = await collect_operational_conditions(
            connection,
            commit=commit,
            run_kind=run_kind,
            server_image=server_image,
            host_description=host_description,
            storage_description=storage_description,
            demo_quiesced=demo_quiesced,
            cache_posture=(
                'operational autovacuum; explicit analyze before inventory; '
                'explicit checkpoint around drop WAL'
            ),
            prepared_posture=(
                'bounded registry cleanup batches and catalog-owned partition '
                'maintenance operations'
            ),
        )
        await install_archive_candidates(connection, schema)
        await install_workflow_recovery_prototype(connection, schema)
        await install_partition_manager_prototype(connection, schema)
        await install_identity_candidates(connection, schema)
        await connection.execute(
            text(
                f"""
                ALTER TABLE {schema.sql}.key_reservations SET (
                    autovacuum_vacuum_threshold = 1000,
                    autovacuum_vacuum_scale_factor = 0.01,
                    autovacuum_analyze_threshold = 1000,
                    autovacuum_analyze_scale_factor = 0.01
                )
                """
            )
        )
        await connection.commit()
    try:
        reporter.emit(
            QualificationProgress(
                scenario='operational-maintenance',
                phase='registry',
                status='started',
                observations=0,
                observation_target=registry_rows,
            )
        )
        registry = await _measure_registry(
            engine,
            schema,
            rows=registry_rows,
            batch_size=registry_batch_size,
            observation_seconds=observation_seconds,
            progress=reporter,
        )
        reporter.emit(
            QualificationProgress(
                scenario='operational-maintenance',
                phase='partitions',
                status='started',
                observations=0,
                observation_target=attached_leaves,
            )
        )
        partitions = await _measure_partitions(
            engine,
            schema,
            history_rows=history_rows,
            forever_rows=forever_rows,
            attached_leaves=attached_leaves,
            progress=reporter,
        )
        verdict = (
            registry.bounded_growth_passed
            and registry.vacuum_keep_up_passed
            and partitions.inventory_passed
            and partitions.create_leaf_passed
            and partitions.detach_passed
            and partitions.drop_passed
            and partitions.long_reader_writer_passed
            and partitions.interrupted_detach_finalized
            and partitions.forever_dead_tuples == 0
            and partitions.forever_freeze_passed
            and partitions.forever_read_passed
        )
    finally:
        # Disposal runs after measurement and cannot discard it. The previous
        # shape returned the evidence from inside the `try` and cleaned up in
        # `finally`, so a cleanup that raised replaced a completed return value
        # with an exception: the measurements ran, a verdict was computed, and
        # the evidence was destroyed on the way out.
        cleanup = await dispose_evidence_schema(engine, schema)

    reporter.emit(
        QualificationProgress(
            scenario='operational-maintenance',
            phase='cleanup',
            status='clean' if cleanup.clean else 'warned',
            observations=cleanup.leaves_dropped,
        )
    )
    return OperationalMaintenanceEvidence(
        conditions=conditions,
        workload={
            'registry_rows': registry_rows,
            'registry_batch_size': registry_batch_size,
            'history_rows': history_rows,
            'forever_rows': forever_rows,
            'attached_leaves': attached_leaves,
            'observation_seconds': observation_seconds,
        },
        registry=registry,
        partitions=partitions,
        cleanup=cleanup,
        verdict=verdict,
    )


async def _measure_registry(
    engine: AsyncEngine,
    schema: PrototypeSchema,
    *,
    rows: int,
    batch_size: int,
    observation_seconds: int,
    progress: QualificationProgressReporter,
) -> RegistryMaintenanceMeasurement:
    async with engine.begin() as connection:
        await connection.execute(
            text(
                f"""
                INSERT INTO {schema.sql}.key_reservations (
                    idempotency_key_digest, key_scope_version,
                    fingerprint_version, command_fingerprint, task_id,
                    disposition, reservation_window, expires_at
                )
                SELECT sha256(convert_to('key-' || series::text, 'UTF8')),
                       1, 1,
                       sha256(convert_to('command-' || series::text, 'UTF8')),
                       md5('task-' || series::text)::uuid::text,
                       'TERMINAL', interval '24 hours',
                       statement_timestamp() - interval '1 second'
                FROM generate_series(1, CAST(:rows AS bigint)) AS series
                """
            ),
            {'rows': rows},
        )
    async with engine.connect() as connection:
        before = await _relation_statistics(connection, schema, 'key_reservations')
        wal_start = (
            await connection.execute(text('SELECT pg_current_wal_insert_lsn()'))
        ).scalar_one()
    started = time.perf_counter()
    cleaned = 0
    while cleaned < rows:
        async with engine.begin() as connection:
            returned = (
                await connection.execute(
                    text(
                        f"""
                        SELECT count(*)
                        FROM {schema.sql}.cleanup_key_reservations(:batch_size)
                        """
                    ),
                    {'batch_size': batch_size},
                )
            ).scalar_one()
        if returned == 0:
            break
        cleaned += int(returned)
        if cleaned % max(batch_size, rows // 10) == 0 or cleaned == rows:
            progress.emit(
                QualificationProgress(
                    scenario='operational-maintenance',
                    phase='registry-cleanup',
                    status='running',
                    observations=cleaned,
                    observation_target=rows,
                )
            )
    cleanup_seconds = time.perf_counter() - started
    async with engine.connect() as connection:
        wal_end = (
            await connection.execute(text('SELECT pg_current_wal_insert_lsn()'))
        ).scalar_one()
        wal_bytes = int(
            (
                await connection.execute(
                    text('SELECT pg_wal_lsn_diff(:end, :start)'),
                    {'end': wal_end, 'start': wal_start},
                )
            ).scalar_one()
        )
        remaining = int(
            (
                await connection.execute(
                    text(f'SELECT count(*) FROM {schema.sql}.key_reservations')
                )
            ).scalar_one()
        )
    observation_started = time.perf_counter()
    after = before
    while time.perf_counter() - observation_started < observation_seconds:
        async with engine.connect() as connection:
            after = await _relation_statistics(
                connection,
                schema,
                'key_reservations',
            )
        if after[1] <= max(1, int(after[0] * 0.2)) and after[2] > before[2]:
            break
        await asyncio.sleep(min(5, observation_seconds))
    observed_seconds = time.perf_counter() - observation_started
    live_tuples, dead_tuples, autovacuum_count = after
    return RegistryMaintenanceMeasurement(
        seeded_rows=rows,
        batch_size=batch_size,
        cleanup_seconds=cleanup_seconds,
        cleanup_rows_per_second=cleaned / cleanup_seconds,
        cleanup_wal_bytes=wal_bytes,
        remaining_rows=remaining,
        dead_tuples_after_observation=dead_tuples,
        live_tuples_after_observation=live_tuples,
        autovacuum_count_delta=autovacuum_count - before[2],
        observation_seconds=observed_seconds,
        bounded_growth_passed=remaining == 0 and cleaned == rows,
        vacuum_keep_up_passed=(
            dead_tuples <= max(1, int(live_tuples * 0.2))
            and autovacuum_count > before[2]
        ),
    )


async def _measure_partitions(
    engine: AsyncEngine,
    schema: PrototypeSchema,
    *,
    history_rows: int,
    forever_rows: int,
    attached_leaves: int,
    progress: QualificationProgressReporter,
) -> PartitionLifecycleMeasurement:
    create_lower = datetime(2026, 8, 6, tzinfo=timezone.utc)
    create_upper = create_lower + timedelta(days=1)
    create_name = 'history_aggregate_finite_2026_08_06'
    async with engine.connect() as connection:
        started = time.perf_counter()
        await create_daily_history_leaf(
            connection,
            schema,
            leaf_name=create_name,
            class_key=_CLASS_KEY,
            lower=create_lower,
            upper=create_upper,
        )
        await connection.commit()
        create_seconds = time.perf_counter() - started

    existing = 3
    remaining = max(0, attached_leaves - existing)
    first_future = datetime(2026, 8, 7, tzinfo=timezone.utc)
    async with engine.connect() as connection:
        for offset in range(remaining):
            lower = first_future + timedelta(days=offset)
            upper = lower + timedelta(days=1)
            await create_daily_history_leaf(
                connection,
                schema,
                leaf_name=f'history_aggregate_finite_{lower:%Y_%m_%d}',
                class_key=_CLASS_KEY,
                lower=lower,
                upper=upper,
            )
            if (offset + 1) % 32 == 0 or offset + 1 == remaining:
                await connection.commit()
                progress.emit(
                    QualificationProgress(
                        scenario='operational-maintenance',
                        phase='leaf-create',
                        status='running',
                        observations=existing + offset + 1,
                        observation_target=attached_leaves,
                    )
                )
        await connection.commit()

    async with engine.connect() as connection:
        await connection.execute(
            text(f'ANALYZE {schema.sql}.history_leaf_catalog')
        )
        started = time.perf_counter()
        inventory = (
            await connection.execute(
                text(
                    f"""
                    SELECT count(*) FILTER (WHERE inherited.inhrelid IS NOT NULL)
                               AS attached,
                           count(*) FILTER (WHERE catalog.detached_at IS NOT NULL)
                               AS detached,
                           count(*) FILTER (WHERE catalog.dropped_at IS NOT NULL)
                               AS dropped,
                           count(*) FILTER (WHERE relation.oid IS NULL)
                               AS missing
                    FROM {schema.sql}.history_leaf_catalog AS catalog
                    LEFT JOIN pg_class AS relation
                      ON relation.oid = to_regclass(
                           '{schema.name}.' || quote_ident(catalog.leaf_name)
                       )
                    LEFT JOIN pg_inherits AS inherited
                      ON inherited.inhrelid = relation.oid
                     AND inherited.inhparent =
                         '{schema.name}.history_aggregate_finite'::regclass
                    """
                )
            )
        ).one()
        inventory_seconds = time.perf_counter() - started

    await _seed_history_leaf(
        engine,
        schema,
        relation=_OLD_LEAF,
        rows=history_rows,
        terminal_at=datetime(2026, 6, 1, 12, tzinfo=timezone.utc),
        domain='old',
    )
    started = time.perf_counter()
    detached = await detach_history_leaf_concurrently(
        engine,
        schema,
        leaf_name=_OLD_LEAF,
        class_key=_CLASS_KEY,
        lower=_OLD_LOWER,
        upper=_OLD_UPPER,
    )
    detach_seconds = time.perf_counter() - started
    async with engine.connect() as connection:
        await connection.execute(text('CHECKPOINT'))
        wal_start = (
            await connection.execute(text('SELECT pg_current_wal_insert_lsn()'))
        ).scalar_one()
        started = time.perf_counter()
        dropped = await drop_detached_history_leaf(
            connection,
            schema,
            leaf_name=_OLD_LEAF,
            class_key=_CLASS_KEY,
            lower=_OLD_LOWER,
            upper=_OLD_UPPER,
        )
        await connection.commit()
        drop_seconds = time.perf_counter() - started
        wal_end = (
            await connection.execute(text('SELECT pg_current_wal_insert_lsn()'))
        ).scalar_one()
        drop_wal = int(
            (
                await connection.execute(
                    text('SELECT pg_wal_lsn_diff(:end, :start)'),
                    {'end': wal_end, 'start': wal_start},
                )
            ).scalar_one()
        )

    long_reader = await _measure_long_reader(engine, schema)
    forever = await _measure_forever_freeze(
        engine,
        schema,
        rows=forever_rows,
    )
    return PartitionLifecycleMeasurement(
        attached_leaves=int(inventory.attached),
        inventory_seconds=inventory_seconds,
        inventory_passed=(
            int(inventory.attached) == attached_leaves
            and int(inventory.detached) == 0
            and int(inventory.dropped) == 0
            and int(inventory.missing) == 0
            and inventory_seconds <= 2
        ),
        create_leaf_seconds=create_seconds,
        create_leaf_passed=create_seconds <= 1,
        detach_seconds=detach_seconds,
        detach_passed=(
            detached.state is LeafState.DETACHED and detach_seconds <= 2
        ),
        drop_seconds=drop_seconds,
        drop_wal_bytes=drop_wal,
        drop_passed=(
            dropped.state is LeafState.DROPPED
            and drop_seconds <= 5
            and drop_wal <= 4 * 1024 * 1024
        ),
        long_reader_baseline_p99_ms=long_reader[0],
        long_reader_contended_p99_ms=long_reader[1],
        long_reader_writer_p99_ms=long_reader[2],
        long_reader_writer_passed=long_reader[2] <= 100,
        interrupted_detach_finalized=long_reader[3],
        observed_blockers=long_reader[4],
        forever_rows=forever_rows,
        forever_dead_tuples=forever[0],
        forever_freeze_age_before=forever[1],
        forever_freeze_age_after=forever[2],
        forever_vacuum_overlapped_reads=forever[3],
        forever_read_p99_ms=forever[4],
        forever_freeze_passed=(
            forever[2] <= forever[1] and forever[3]
        ),
        forever_read_passed=forever[4] <= 100,
    )


async def _measure_long_reader(
    engine: AsyncEngine,
    schema: PrototypeSchema,
) -> tuple[float, float, float, bool, tuple[dict[str, object], ...]]:
    lower = datetime(2026, 5, 1, tzinfo=timezone.utc)
    upper = lower + timedelta(days=1)
    leaf = 'history_aggregate_finite_2026_05_01'
    async with engine.connect() as connection:
        await create_daily_history_leaf(
            connection,
            schema,
            leaf_name=leaf,
            class_key=_CLASS_KEY,
            lower=lower,
            upper=upper,
        )
        await connection.commit()
    await _seed_history_leaf(
        engine,
        schema,
        relation=_CURRENT_LEAF,
        rows=1,
        terminal_at=_CURRENT_AT,
        domain='writer-warmup',
    )
    baseline = await _measure_unrelated_writes(
        engine,
        schema,
        domain='writer-baseline',
    )
    reader = await engine.connect()
    transaction = await reader.begin()
    await reader.execute(
        text(f'SELECT count(*) FROM {schema.sql}.history_aggregate')
    )

    async def detach() -> str:
        try:
            await detach_history_leaf_concurrently(
                engine,
                schema,
                leaf_name=leaf,
                class_key=_CLASS_KEY,
                lower=lower,
                upper=upper,
                statement_timeout_ms=500,
            )
        except DBAPIError as error:
            return str(error)
        return 'completed'

    task = asyncio.create_task(detach())
    await asyncio.sleep(0.25)
    blockers = await _blocking_sessions(engine)
    contended = await _measure_unrelated_writes(
        engine,
        schema,
        domain='writer-contended',
    )
    await task
    await transaction.rollback()
    await reader.close()
    finalized = await finalize_interrupted_detach(
        engine,
        schema,
        leaf_name=leaf,
        class_key=_CLASS_KEY,
        lower=lower,
        upper=upper,
    )
    added_lock_ms = [
        max(0.0, contended_ms - baseline_ms)
        for baseline_ms, contended_ms in zip(baseline, contended, strict=True)
    ]
    return (
        percentile_ms(baseline, 99),
        percentile_ms(contended, 99),
        percentile_ms(added_lock_ms, 99),
        finalized.state is LeafState.DETACHED,
        blockers,
    )


async def _measure_unrelated_writes(
    engine: AsyncEngine,
    schema: PrototypeSchema,
    *,
    domain: str,
) -> list[float]:
    latencies: list[float] = []
    for index in range(20):
        started = time.perf_counter()
        await _seed_history_leaf(
            engine,
            schema,
            relation=_CURRENT_LEAF,
            rows=1,
            terminal_at=_CURRENT_AT,
            domain=f'{domain}-{index}',
        )
        latencies.append((time.perf_counter() - started) * 1_000)
    return latencies


async def _measure_forever_freeze(
    engine: AsyncEngine,
    schema: PrototypeSchema,
    *,
    rows: int,
) -> tuple[int, int, int, bool, float]:
    relation = 'history_aggregate_forever'
    await _seed_history_leaf(
        engine,
        schema,
        relation=relation,
        rows=rows,
        terminal_at=_CURRENT_AT,
        domain='forever',
        retention_class='forever',
    )
    async with engine.connect() as connection:
        age_before = int(
            (
                await connection.execute(
                    text(
                        "SELECT age(relfrozenxid) FROM pg_class "
                        "WHERE oid = to_regclass(:relation)"
                    ),
                    {'relation': f'{schema.name}.{relation}'},
                )
            ).scalar_one()
        )

    admin = engine.execution_options(isolation_level='AUTOCOMMIT')

    async def vacuum() -> None:
        async with admin.connect() as connection:
            await connection.execute(
                text(f'VACUUM (FREEZE, ANALYZE) {schema.sql}.{relation}')
            )

    vacuum_task = asyncio.create_task(vacuum())
    await asyncio.sleep(0)
    overlapped_reads = not vacuum_task.done()
    latencies: list[float] = []
    async with engine.connect() as connection:
        for row_number in range(1, 101):
            started = time.perf_counter()
            await connection.execute(
                text(
                    f"""
                    SELECT task_id FROM {schema.sql}.{relation}
                    WHERE task_id = md5(
                        :domain || CAST(:row_number AS text)
                    )::uuid::text
                    """
                ),
                {'domain': 'forever', 'row_number': row_number},
            )
            latencies.append((time.perf_counter() - started) * 1_000)
    await vacuum_task
    async with engine.connect() as connection:
        stats = await _relation_statistics(connection, schema, relation)
        age_after = int(
            (
                await connection.execute(
                    text(
                        "SELECT age(relfrozenxid) FROM pg_class "
                        "WHERE oid = to_regclass(:relation)"
                    ),
                    {'relation': f'{schema.name}.{relation}'},
                )
            ).scalar_one()
        )
    return (
        stats[1],
        age_before,
        age_after,
        overlapped_reads,
        percentile_ms(latencies, 99),
    )


async def _seed_history_leaf(
    engine: AsyncEngine,
    schema: PrototypeSchema,
    *,
    relation: str,
    rows: int,
    terminal_at: datetime,
    domain: str,
    retention_class: str = _CLASS_KEY,
) -> None:
    async with engine.begin() as connection:
        await connection.execute(
            text(
                f"""
                INSERT INTO {schema.sql}.{relation} (
                    task_id, task_name, queue_name, priority,
                    command_fingerprint_version, command_fingerprint, status,
                    terminalization_kind, terminal_at, retention_anchor_at,
                    retention_class_key, enqueued_at, created_at,
                    result_envelope_version, result_codec, result_content_type,
                    result_payload, result_digest, retry_count, max_retries,
                    is_workflow_task, history_schema_version,
                    attempt_archive_version, attempt_snapshot_codec,
                    attempt_snapshot_content_type, attempt_snapshot,
                    attempt_snapshot_digest
                )
                SELECT md5(:domain || series::text)::uuid::text,
                       'prototype.task', 'default', 100, 1,
                       sha256(convert_to(:domain || series::text, 'UTF8')),
                       'COMPLETED', 'COMPLETE_LOCKED', :terminal_at, :terminal_at,
                       :retention_class, :terminal_at, :terminal_at,
                       1, 'json-utf8', 'application/json', '{{}}'::bytea,
                       sha256('{{}}'::bytea), 0, 0, FALSE, 1, 1,
                       'json-utf8', 'application/json', '[]'::bytea,
                       sha256('[]'::bytea)
                FROM generate_series(1, CAST(:rows AS bigint)) AS series
                ON CONFLICT DO NOTHING
                """
            ),
            {
                'domain': domain,
                'rows': rows,
                'terminal_at': terminal_at,
                'retention_class': retention_class,
            },
        )


async def _relation_statistics(
    connection: AsyncConnection,
    schema: PrototypeSchema,
    relation: str,
) -> tuple[int, int, int]:
    await refresh_cumulative_statistics(connection)
    row = (
        await connection.execute(
            text(
                """
                SELECT n_live_tup, n_dead_tup, autovacuum_count
                FROM pg_stat_user_tables
                WHERE schemaname = :schema_name AND relname = :relation
                """
            ),
            {'schema_name': schema.name, 'relation': relation},
        )
    ).one()
    return int(row.n_live_tup), int(row.n_dead_tup), int(row.autovacuum_count)


async def _blocking_sessions(
    engine: AsyncEngine,
) -> tuple[dict[str, object], ...]:
    async with engine.connect() as connection:
        rows = (
            await connection.execute(
                text(
                    """
                    SELECT activity.pid, activity.state,
                           extract(epoch FROM statement_timestamp()
                               - activity.xact_start) AS xact_age_seconds,
                           activity.query,
                           pg_blocking_pids(activity.pid) AS blocking_pids
                    FROM pg_stat_activity AS activity
                    WHERE cardinality(pg_blocking_pids(activity.pid)) > 0
                    ORDER BY activity.pid
                    """
                )
            )
        ).all()
    return tuple(
        {
            'pid': int(row.pid),
            'state': row.state,
            'xact_age_seconds': (
                float(row.xact_age_seconds)
                if row.xact_age_seconds is not None
                else None
            ),
            'query': row.query,
            'blocking_pids': tuple(int(pid) for pid in row.blocking_pids),
        }
        for row in rows
    )


def _validate_workload(
    *,
    run_kind: EvidenceRunKind,
    registry_rows: int,
    history_rows: int,
    forever_rows: int,
    attached_leaves: int,
    observation_seconds: int,
) -> None:
    if min(
        registry_rows,
        history_rows,
        forever_rows,
        attached_leaves,
        observation_seconds,
    ) <= 0:
        raise ValueError('operational workload sizes must be positive')
    if run_kind is EvidenceRunKind.GATE and (
        registry_rows < 1_000_000
        or history_rows < 1_000_000
        or forever_rows < 100_000
        or attached_leaves != 512
        or observation_seconds < 120
    ):
        raise ValueError(
            'operational gate evidence requires one million registry rows, '
            'one million detached-leaf rows, 100,000 forever rows, 512 '
            'attached leaves, and a two-cycle observation window'
        )
