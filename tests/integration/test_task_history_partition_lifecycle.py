"""Partition manager lifecycle against real PostgreSQL.

Exercises the accepted checkpoint-1 contract end to end in a disposable
schema: class registration, create-ahead, conformance repair, pending
blockers refusing detach, concurrent detach with catalog reconciliation,
drop-after-detach, and the health pass. Every refusal is asserted as its
typed outcome, never as an exception.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from hashlib import sha256
from time import monotonic
from uuid import uuid4

import pytest
from sqlalchemy import event, text
from sqlalchemy.ext.asyncio import AsyncConnection, create_async_engine

from horsies.core.history.commands import (
    CollectPartitionHealth,
    CreateDailyHistoryLeaf,
    DetachExpiredHistoryLeaf,
    DropDetachedHistoryLeaf,
    EnsureLeafCoverage,
    InspectHistoryLeaf,
    LeafBounds,
    LeafRef,
)
from horsies.core.history.ddl.classes import (
    ClassAlreadyRegistered,
    ClassConflict,
    register_finite_retention_class,
)
from horsies.core.history.outcomes import (
    CoverageBelowFloor,
    ForeverClassLeaf,
    LeafAlreadyConformant,
    LeafCatalogConflict,
    LeafCreated,
    LeafDetachable,
    LeafDetached,
    LeafDropped,
    LeafIndexRepaired,
    LeafMaintenanceBusy,
    LeafNotExpired,
    LeafPendingBlocked,
    RetentionClassAbsent,
)
from horsies.core.history.partitions.catalog import daily_leaf_name
from horsies.core.history.partitions.health import collect_partition_health
from horsies.core.history.partitions import manager as partition_manager
from horsies.core.history.partitions.manager import (
    create_daily_leaf,
    detach_expired_leaf,
    drop_detached_leaf,
    ensure_leaf_coverage,
    inspect_leaf,
)
from horsies.core.history.partitions.publication import UnpublishedLoader
from horsies.core.history.maintenance.database import PartitionMaintenanceDatabase

from horsies.core.history.phase2.quarantine import QuarantineRefused

from tests.integration.task_history_harness import (
    INSERT_HISTORY_ROW_SQL,
    HistorySchema,
    day_bounds,
    frozen_history_row,
    register_class,
    task_history_schema_fixture,
)

pytestmark = [pytest.mark.integration]

UTC = timezone.utc
CLASS_KEY = 'it_lifecycle'

history_schema = task_history_schema_fixture('task_history_it_lifecycle')


def leaf_ref(parent_name: str, lower: datetime) -> LeafRef:
    return LeafRef(
        leaf_name=daily_leaf_name(parent_name, lower),
        class_key=CLASS_KEY,
        bounds=LeafBounds(lower=lower, upper=lower + timedelta(days=1)),
    )


async def make_expired_leaf(
    schema: HistorySchema,
    parent_name: str,
    *,
    days_ago: int = 40,
) -> LeafRef:
    lower, _ = day_bounds(datetime.now(UTC) - timedelta(days=days_ago))
    ref = leaf_ref(parent_name, lower)
    async with schema.engine.begin() as connection:
        outcome = await create_daily_leaf(
            connection, CreateDailyHistoryLeaf(leaf=ref), UnpublishedLoader()
        )
        assert isinstance(outcome, LeafCreated)
    return ref


async def seed_stale_locator(
    connection: AsyncConnection,
    ref: LeafRef,
    *,
    task_id: str,
    with_history_row: bool,
    node_key: str | None = 'node-0',
) -> None:
    """One over-horizon pending locator on `ref`, with its node row and
    (optionally) the history row the locator names."""
    anchor = ref.bounds.lower + timedelta(hours=1)
    node_row_id = str(uuid4())
    workflow_id = str(uuid4())
    if with_history_row:
        await connection.execute(
            text(INSERT_HISTORY_ROW_SQL),
            frozen_history_row(
                task_id=task_id, class_key=CLASS_KEY, terminal_at=anchor
            ),
        )
    await connection.execute(
        text(
            'INSERT INTO horsies_workflow_tasks '
            '(id, workflow_id, task_id, task_index, node_id) VALUES '
            '(CAST(:node_row_id AS uuid), CAST(:workflow_id AS uuid), '
            'CAST(:task_id AS uuid), 0, :node_key)'
        ),
        {
            'node_row_id': node_row_id,
            'workflow_id': workflow_id,
            'task_id': task_id,
            'node_key': node_key,
        },
    )
    await connection.execute(
        text(
            """
            INSERT INTO horsies_workflow_phase2_pending (
                task_id, workflow_id, workflow_node_row_id,
                terminal_status, terminal_at, terminalization_kind,
                recovery_source, history_class, history_anchor,
                history_schema_version, result_digest,
                phase2_generation, created_at, attempt_count
            ) VALUES (
                :task_id, :workflow_id, :node_row_id,
                'COMPLETED', :anchor, 'COMPLETE_FUSED',
                'HISTORY', :class_key, :anchor,
                1, :digest, :generation,
                statement_timestamp() - interval '8 days', 0
            )
            """
        ),
        {
            'task_id': task_id,
            'workflow_id': workflow_id,
            'node_row_id': node_row_id,
            'anchor': anchor,
            'class_key': CLASS_KEY,
            'digest': sha256(b'{}').digest(),
            'generation': str(uuid4()),
        },
    )


class TestClassRegistration:
    @pytest.mark.asyncio
    async def test_register_verify_and_conflict(
        self, history_schema: HistorySchema
    ) -> None:
        async with history_schema.engine.begin() as connection:
            parent = await register_class(connection, CLASS_KEY)
            assert parent == f'horsies_task_history_{CLASS_KEY}'
            again = await register_finite_retention_class(
                connection, class_key=CLASS_KEY, duration=timedelta(days=30)
            )
            assert again == ClassAlreadyRegistered(class_key=CLASS_KEY)
            conflict = await register_finite_retention_class(
                connection, class_key=CLASS_KEY, duration=timedelta(days=7)
            )
            assert isinstance(conflict, ClassConflict)
            assert conflict.existing_duration == timedelta(days=30)


class TestCreateAndCoverage:
    @pytest.mark.asyncio
    async def test_create_ahead_is_idempotent(
        self, history_schema: HistorySchema
    ) -> None:
        async with history_schema.engine.begin() as connection:
            await register_class(connection, CLASS_KEY)
            first = await ensure_leaf_coverage(
                connection,
                EnsureLeafCoverage(class_key=CLASS_KEY, horizon_days=3),
                UnpublishedLoader(),
            )
            assert [type(outcome) for outcome in first] == [LeafCreated] * 4
            second = await ensure_leaf_coverage(
                connection,
                EnsureLeafCoverage(class_key=CLASS_KEY, horizon_days=3),
                UnpublishedLoader(),
            )
            assert [type(outcome) for outcome in second] == (
                [LeafAlreadyConformant] * 4
            )

    @pytest.mark.asyncio
    async def test_missing_index_is_repaired(
        self, history_schema: HistorySchema
    ) -> None:
        async with history_schema.engine.begin() as connection:
            parent = await register_class(connection, CLASS_KEY)
        ref = await make_expired_leaf(history_schema, parent)
        async with history_schema.engine.begin() as connection:
            await connection.execute(text(f'DROP INDEX {ref.leaf_name}_task_idx'))
            outcome = await create_daily_leaf(
                connection, CreateDailyHistoryLeaf(leaf=ref), UnpublishedLoader()
            )
            assert outcome == LeafIndexRepaired(
                leaf_name=ref.leaf_name,
                id_index_name=f'{ref.leaf_name}_task_idx',
            )

    @pytest.mark.asyncio
    async def test_mismatched_request_is_a_catalog_conflict(
        self, history_schema: HistorySchema
    ) -> None:
        async with history_schema.engine.begin() as connection:
            parent = await register_class(connection, CLASS_KEY)
        ref = await make_expired_leaf(history_schema, parent)
        shifted = LeafRef(
            leaf_name=ref.leaf_name,
            class_key=CLASS_KEY,
            bounds=LeafBounds(
                lower=ref.bounds.lower - timedelta(days=1),
                upper=ref.bounds.upper - timedelta(days=1),
            ),
        )
        async with history_schema.engine.begin() as connection:
            outcome = await create_daily_leaf(
                connection,
                CreateDailyHistoryLeaf(leaf=shifted),
                UnpublishedLoader(),
            )
            assert isinstance(outcome, LeafCatalogConflict)


class TestInspection:
    @pytest.mark.asyncio
    async def test_unknown_class_and_forever_class(
        self, history_schema: HistorySchema
    ) -> None:
        lower, _ = day_bounds(datetime.now(UTC))
        async with history_schema.engine.connect() as connection:
            absent = await inspect_leaf(
                connection,
                InspectHistoryLeaf(
                    leaf=LeafRef(
                        leaf_name='horsies_task_history_ghost_2026_01_01',
                        class_key='ghost',
                        bounds=LeafBounds(
                            lower=lower, upper=lower + timedelta(days=1)
                        ),
                    )
                ),
            )
            assert absent == RetentionClassAbsent(class_key='ghost')
            forever = await inspect_leaf(
                connection,
                InspectHistoryLeaf(
                    leaf=LeafRef(
                        leaf_name='horsies_task_history_forever_x',
                        class_key='forever',
                        bounds=LeafBounds(
                            lower=lower, upper=lower + timedelta(days=1)
                        ),
                    )
                ),
            )
            assert forever == ForeverClassLeaf(class_key='forever')

    @pytest.mark.asyncio
    async def test_fresh_leaf_is_not_expired(
        self, history_schema: HistorySchema
    ) -> None:
        async with history_schema.engine.begin() as connection:
            parent = await register_class(connection, CLASS_KEY)
            lower, _ = day_bounds(datetime.now(UTC))
            ref = leaf_ref(parent, lower)
            created = await create_daily_leaf(
                connection, CreateDailyHistoryLeaf(leaf=ref), UnpublishedLoader()
            )
            assert isinstance(created, LeafCreated)
            inspection = await inspect_leaf(
                connection, InspectHistoryLeaf(leaf=ref)
            )
            assert isinstance(inspection, LeafNotExpired)


class TestDetachAndDrop:
    @pytest.mark.asyncio
    async def test_expired_leaf_detaches_then_drops(
        self, history_schema: HistorySchema
    ) -> None:
        async with history_schema.engine.begin() as connection:
            parent = await register_class(connection, CLASS_KEY)
        ref = await make_expired_leaf(history_schema, parent)
        async with history_schema.engine.connect() as connection:
            inspection = await inspect_leaf(
                connection, InspectHistoryLeaf(leaf=ref)
            )
            assert isinstance(inspection, LeafDetachable)
        detached = await detach_expired_leaf(
            PartitionMaintenanceDatabase(
                history_schema.engine, connection_capacity=2
            ),
            DetachExpiredHistoryLeaf(
                leaf=ref, quarantine_horizon=None, statement_timeout_ms=None
            ),
            UnpublishedLoader(),
        )
        assert isinstance(detached, LeafDetached)
        async with history_schema.engine.begin() as connection:
            dropped = await drop_detached_leaf(
                connection,
                DropDetachedHistoryLeaf(leaf=ref),
                UnpublishedLoader(),
            )
            assert dropped == LeafDropped(leaf_name=ref.leaf_name)
            after = await inspect_leaf(connection, InspectHistoryLeaf(leaf=ref))
            assert after == LeafDropped(leaf_name=ref.leaf_name)

    @pytest.mark.asyncio
    async def test_pending_locator_blocks_detach_until_resolved(
        self, history_schema: HistorySchema
    ) -> None:
        async with history_schema.engine.begin() as connection:
            parent = await register_class(connection, CLASS_KEY)
        ref = await make_expired_leaf(history_schema, parent)
        async with history_schema.engine.begin() as connection:
            await connection.execute(
                text(
                    """
                    INSERT INTO horsies_workflow_phase2_pending (
                        task_id, workflow_id, workflow_node_row_id,
                        terminal_status, terminal_at, terminalization_kind,
                        recovery_source, history_class, history_anchor,
                        history_schema_version, result_digest,
                        phase2_generation, created_at, attempt_count
                    ) VALUES (
                        :task_id, :workflow_id, :node_row_id,
                        'COMPLETED', :anchor, 'COMPLETE_FUSED',
                        'HISTORY', :class_key, :anchor,
                        1, :digest, :generation, statement_timestamp(), 0
                    )
                    """
                ),
                {
                    'task_id': str(uuid4()),
                    'workflow_id': str(uuid4()),
                    'node_row_id': str(uuid4()),
                    'anchor': ref.bounds.lower + timedelta(hours=1),
                    'class_key': CLASS_KEY,
                    'digest': sha256(b'result').digest(),
                    'generation': str(uuid4()),
                },
            )
        refused = await detach_expired_leaf(
            PartitionMaintenanceDatabase(
                history_schema.engine, connection_capacity=2
            ),
            DetachExpiredHistoryLeaf(
                leaf=ref, quarantine_horizon=None, statement_timeout_ms=None
            ),
            UnpublishedLoader(),
        )
        assert isinstance(refused, LeafPendingBlocked)
        assert refused.blocker_count == 1
        async with history_schema.engine.begin() as connection:
            await connection.execute(
                text('DELETE FROM horsies_workflow_phase2_pending')
            )
        detached = await detach_expired_leaf(
            PartitionMaintenanceDatabase(
                history_schema.engine, connection_capacity=2
            ),
            DetachExpiredHistoryLeaf(
                leaf=ref, quarantine_horizon=None, statement_timeout_ms=None
            ),
            UnpublishedLoader(),
        )
        assert isinstance(detached, LeafDetached)

    @pytest.mark.asyncio
    async def test_detach_horizon_quarantines_stale_locator_then_detaches(
        self, history_schema: HistorySchema
    ) -> None:
        async with history_schema.engine.begin() as connection:
            parent = await register_class(connection, CLASS_KEY)
        ref = await make_expired_leaf(history_schema, parent)
        task_id = str(uuid4())
        async with history_schema.engine.begin() as connection:
            await seed_stale_locator(
                connection, ref, task_id=task_id, with_history_row=True
            )
        outcome = await detach_expired_leaf(
            PartitionMaintenanceDatabase(
                history_schema.engine, connection_capacity=2
            ),
            DetachExpiredHistoryLeaf(
                leaf=ref, quarantine_horizon=timedelta(days=7),
                statement_timeout_ms=None,
            ),
            UnpublishedLoader(),
        )
        assert isinstance(outcome, LeafDetached)
        async with history_schema.engine.begin() as connection:
            pending = (
                await connection.execute(
                    text(
                        'SELECT recovery_source, quarantine_task_id, '
                        'history_class '
                        'FROM horsies_workflow_phase2_pending '
                        'WHERE task_id = CAST(:task_id AS uuid)'
                    ),
                    {'task_id': task_id},
                )
            ).one()
            assert pending.recovery_source == 'QUARANTINE'
            assert str(pending.quarantine_task_id) == task_id
            assert pending.history_class is None
            quarantined = (
                await connection.execute(
                    text(
                        'SELECT count(*) '
                        'FROM horsies_workflow_phase2_quarantine '
                        'WHERE task_id = CAST(:task_id AS uuid)'
                    ),
                    {'task_id': task_id},
                )
            ).scalar_one()
            assert quarantined == 1

    @pytest.mark.asyncio
    async def test_detach_horizon_refusal_keeps_the_leaf_pinned(
        self, history_schema: HistorySchema
    ) -> None:
        async with history_schema.engine.begin() as connection:
            parent = await register_class(connection, CLASS_KEY)
        ref = await make_expired_leaf(history_schema, parent)
        task_id = str(uuid4())
        async with history_schema.engine.begin() as connection:
            # Locator with no history row behind it: the copy refuses
            # and the evidence must keep the leaf pinned.
            await seed_stale_locator(
                connection, ref, task_id=task_id, with_history_row=False
            )
        outcome = await detach_expired_leaf(
            PartitionMaintenanceDatabase(
                history_schema.engine, connection_capacity=2
            ),
            DetachExpiredHistoryLeaf(
                leaf=ref, quarantine_horizon=timedelta(days=7),
                statement_timeout_ms=None,
            ),
            UnpublishedLoader(),
        )
        match outcome:
            case QuarantineRefused(repointed=0, refusals=(refusal,)):
                assert refusal.task_id == task_id
                assert refusal.verdict == 'SOURCE_ABSENT'
            case _:
                raise AssertionError(f'unexpected outcome: {outcome!r}')
        async with history_schema.engine.begin() as connection:
            pending = (
                await connection.execute(
                    text(
                        'SELECT recovery_source '
                        'FROM horsies_workflow_phase2_pending '
                        'WHERE task_id = CAST(:task_id AS uuid)'
                    ),
                    {'task_id': task_id},
                )
            ).one()
            assert pending.recovery_source == 'HISTORY'
        still_blocked = await detach_expired_leaf(
            PartitionMaintenanceDatabase(
                history_schema.engine, connection_capacity=2
            ),
            DetachExpiredHistoryLeaf(
                leaf=ref, quarantine_horizon=None, statement_timeout_ms=None
            ),
            UnpublishedLoader(),
        )
        assert isinstance(still_blocked, LeafPendingBlocked)

    @pytest.mark.asyncio
    async def test_refusal_reason_rides_the_detach_outcome(
        self, history_schema: HistorySchema
    ) -> None:
        """An operator diagnosing an undetachable leaf sees the named
        per-task reason on the detach return itself, without reading
        pending rows."""
        async with history_schema.engine.begin() as connection:
            parent = await register_class(connection, CLASS_KEY)
        ref = await make_expired_leaf(history_schema, parent)
        task_id = str(uuid4())
        async with history_schema.engine.begin() as connection:
            await seed_stale_locator(
                connection,
                ref,
                task_id=task_id,
                with_history_row=True,
                node_key=None,
            )
        outcome = await detach_expired_leaf(
            PartitionMaintenanceDatabase(
                history_schema.engine, connection_capacity=2
            ),
            DetachExpiredHistoryLeaf(
                leaf=ref, quarantine_horizon=timedelta(days=7),
                statement_timeout_ms=None,
            ),
            UnpublishedLoader(),
        )
        match outcome:
            case QuarantineRefused(refusals=(refusal,)):
                assert refusal.task_id == task_id
                assert refusal.verdict == 'NODE_IDENTITY_ABSENT'
                assert refusal.detail is not None
            case _:
                raise AssertionError(f'unexpected outcome: {outcome!r}')


class TestLockContention:
    @pytest.mark.asyncio
    async def test_conformant_leaf_skips_busy_advisory_lock(
        self, history_schema: HistorySchema
    ) -> None:
        lower, _ = day_bounds(datetime.now(UTC))
        async with history_schema.engine.begin() as connection:
            parent_name = await register_class(connection, CLASS_KEY)
            ref = leaf_ref(parent_name, lower)
            created = await create_daily_leaf(
                connection,
                CreateDailyHistoryLeaf(leaf=ref),
                UnpublishedLoader(),
            )
            assert isinstance(created, LeafCreated)

        holder = await history_schema.engine.connect()
        transaction = await holder.begin()
        try:
            await holder.execute(
                text(
                    'SELECT pg_advisory_xact_lock('
                    'horsies_task_history_leaf_lock_key(:class_key, :anchor))'
                ),
                {'class_key': CLASS_KEY, 'anchor': lower},
            )
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

            event.listen(
                history_schema.engine.sync_engine,
                'before_cursor_execute',
                record_statement,
            )
            try:
                async with asyncio.timeout(1):
                    async with history_schema.engine.begin() as connection:
                        outcome = await create_daily_leaf(
                            connection,
                            CreateDailyHistoryLeaf(leaf=ref),
                            UnpublishedLoader(),
                        )
            finally:
                event.remove(
                    history_schema.engine.sync_engine,
                    'before_cursor_execute',
                    record_statement,
                )
            assert isinstance(outcome, LeafAlreadyConformant)
            assert not any(
                'pg_try_advisory' in statement for statement in statements
            )
        finally:
            await transaction.rollback()
            await holder.close()

    @pytest.mark.asyncio
    async def test_missing_leaf_returns_busy_without_waiting(
        self, history_schema: HistorySchema
    ) -> None:
        lower, _ = day_bounds(datetime.now(UTC) + timedelta(days=2))
        async with history_schema.engine.begin() as connection:
            parent_name = await register_class(connection, CLASS_KEY)
        ref = leaf_ref(parent_name, lower)

        holder = await history_schema.engine.connect()
        transaction = await holder.begin()
        try:
            await holder.execute(
                text(
                    'SELECT pg_advisory_xact_lock('
                    'horsies_task_history_leaf_lock_key(:class_key, :anchor))'
                ),
                {'class_key': CLASS_KEY, 'anchor': lower},
            )
            async with asyncio.timeout(1):
                async with history_schema.engine.begin() as connection:
                    outcome = await create_daily_leaf(
                        connection,
                        CreateDailyHistoryLeaf(leaf=ref),
                        UnpublishedLoader(),
                    )
            assert isinstance(outcome, LeafMaintenanceBusy)
        finally:
            await transaction.rollback()
            await holder.close()

    @pytest.mark.asyncio
    async def test_parent_relation_lock_returns_busy_through_nowait(
        self, history_schema: HistorySchema
    ) -> None:
        lower, _ = day_bounds(datetime.now(UTC) + timedelta(days=2))
        async with history_schema.engine.begin() as connection:
            parent_name = await register_class(connection, CLASS_KEY)
        ref = leaf_ref(parent_name, lower)

        holder = await history_schema.engine.connect()
        transaction = await holder.begin()
        try:
            await holder.execute(
                text(f'LOCK TABLE {parent_name} IN ACCESS EXCLUSIVE MODE')
            )
            async with asyncio.timeout(1):
                async with history_schema.engine.begin() as connection:
                    outcome = await create_daily_leaf(
                        connection,
                        CreateDailyHistoryLeaf(leaf=ref),
                        UnpublishedLoader(),
                    )
            assert isinstance(outcome, LeafMaintenanceBusy)
            async with history_schema.engine.begin() as connection:
                acquired = (
                    await connection.execute(
                        text(
                            'SELECT pg_try_advisory_xact_lock('
                            'horsies_task_history_leaf_lock_key('
                            ':class_key, :anchor))'
                        ),
                        {'class_key': CLASS_KEY, 'anchor': lower},
                    )
                ).scalar_one()
            assert acquired
        finally:
            await transaction.rollback()
            await holder.close()

    @pytest.mark.asyncio
    async def test_detach_relation_wait_is_bounded_and_restores_timeouts(
        self, history_schema: HistorySchema
    ) -> None:
        async with history_schema.engine.begin() as connection:
            parent_name = await register_class(connection, CLASS_KEY)
        ref = await make_expired_leaf(history_schema, parent_name)
        direct_engine = create_async_engine(
            history_schema.engine.url.render_as_string(hide_password=False),
            pool_size=1,
            max_overflow=0,
            connect_args={
                'options': f'-csearch_path={history_schema.schema_name}'
            },
        )
        database = PartitionMaintenanceDatabase(
            direct_engine, connection_capacity=2
        )
        try:
            async with database.autocommit() as connection:
                await connection.execute(text("SET statement_timeout = '13s'"))
                await connection.execute(text("SET lock_timeout = '17s'"))

            holder = await history_schema.engine.connect()
            transaction = await holder.begin()
            try:
                await holder.execute(
                    text(f'LOCK TABLE {parent_name} IN SHARE MODE')
                )
                started = monotonic()
                outcome = await detach_expired_leaf(
                    database,
                    DetachExpiredHistoryLeaf(
                        leaf=ref,
                        quarantine_horizon=None,
                        statement_timeout_ms=9000,
                    ),
                    UnpublishedLoader(),
                )
                elapsed = monotonic() - started
            finally:
                await transaction.rollback()
                await holder.close()

            assert isinstance(outcome, LeafMaintenanceBusy)
            assert elapsed < 3
            async with database.autocommit() as connection:
                statement_timeout = (
                    await connection.execute(text('SHOW statement_timeout'))
                ).scalar_one()
                lock_timeout = (
                    await connection.execute(text('SHOW lock_timeout'))
                ).scalar_one()
            assert statement_timeout == '13s'
            assert lock_timeout == '17s'
        finally:
            await direct_engine.dispose()

    @pytest.mark.asyncio
    async def test_cancelled_detach_releases_session_lock(
        self, history_schema: HistorySchema
    ) -> None:
        async with history_schema.engine.begin() as connection:
            parent_name = await register_class(connection, CLASS_KEY)
        ref = await make_expired_leaf(history_schema, parent_name)
        database = PartitionMaintenanceDatabase(
            history_schema.engine, connection_capacity=2
        )

        holder = await history_schema.engine.connect()
        transaction = await holder.begin()
        try:
            await holder.execute(
                text(f'LOCK TABLE {parent_name} IN SHARE MODE')
            )
            detach_task = asyncio.create_task(
                detach_expired_leaf(
                    database,
                    DetachExpiredHistoryLeaf(
                        leaf=ref,
                        quarantine_horizon=None,
                        statement_timeout_ms=9000,
                    ),
                    UnpublishedLoader(),
                )
            )
            lock_observed = False
            async with history_schema.engine.connect() as probe:
                for _ in range(100):
                    acquired = (
                        await probe.execute(
                            text(
                                'SELECT pg_try_advisory_lock('
                                'horsies_task_history_leaf_lock_key('
                                ':class_key, :anchor))'
                            ),
                            {'class_key': CLASS_KEY, 'anchor': ref.bounds.lower},
                        )
                    ).scalar_one()
                    if acquired:
                        await probe.execute(
                            text(
                                'SELECT pg_advisory_unlock('
                                'horsies_task_history_leaf_lock_key('
                                ':class_key, :anchor))'
                            ),
                            {'class_key': CLASS_KEY, 'anchor': ref.bounds.lower},
                        )
                        await asyncio.sleep(0.01)
                        continue
                    lock_observed = True
                    break
            assert lock_observed
            detach_task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await detach_task

            async with history_schema.engine.connect() as probe:
                acquired = (
                    await probe.execute(
                        text(
                            'SELECT pg_try_advisory_lock('
                            'horsies_task_history_leaf_lock_key('
                            ':class_key, :anchor))'
                        ),
                        {'class_key': CLASS_KEY, 'anchor': ref.bounds.lower},
                    )
                ).scalar_one()
                assert acquired
                await probe.execute(
                    text(
                        'SELECT pg_advisory_unlock('
                        'horsies_task_history_leaf_lock_key('
                        ':class_key, :anchor))'
                    ),
                    {'class_key': CLASS_KEY, 'anchor': ref.bounds.lower},
                )
        finally:
            await transaction.rollback()
            await holder.close()

    @pytest.mark.asyncio
    async def test_cancelled_lock_acquisition_invalidates_session(
        self,
        history_schema: HistorySchema,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        async with history_schema.engine.begin() as connection:
            parent_name = await register_class(connection, CLASS_KEY)
        ref = await make_expired_leaf(history_schema, parent_name)
        direct_engine = create_async_engine(
            history_schema.engine.url.render_as_string(hide_password=False),
            pool_size=1,
            max_overflow=0,
            connect_args={
                'options': f'-csearch_path={history_schema.schema_name}'
            },
        )
        database = PartitionMaintenanceDatabase(
            direct_engine, connection_capacity=2
        )
        original = partition_manager.try_lock_leaf_for_session

        async def acquire_then_cancel(
            connection: AsyncConnection,
            *,
            class_key: str,
            anchor: datetime,
        ) -> None:
            await original(
                connection,
                class_key=class_key,
                anchor=anchor,
            )
            raise asyncio.CancelledError

        monkeypatch.setattr(
            partition_manager,
            'try_lock_leaf_for_session',
            acquire_then_cancel,
        )
        try:
            with pytest.raises(asyncio.CancelledError):
                await detach_expired_leaf(
                    database,
                    DetachExpiredHistoryLeaf(
                        leaf=ref,
                        quarantine_horizon=None,
                        statement_timeout_ms=9000,
                    ),
                    UnpublishedLoader(),
                )

            async with history_schema.engine.connect() as probe:
                acquired = (
                    await probe.execute(
                        text(
                            'SELECT pg_try_advisory_lock('
                            'horsies_task_history_leaf_lock_key('
                            ':class_key, :anchor))'
                        ),
                        {'class_key': CLASS_KEY, 'anchor': ref.bounds.lower},
                    )
                ).scalar_one()
                assert acquired
                await probe.execute(
                    text(
                        'SELECT pg_advisory_unlock('
                        'horsies_task_history_leaf_lock_key('
                        ':class_key, :anchor))'
                    ),
                    {'class_key': CLASS_KEY, 'anchor': ref.bounds.lower},
                )
        finally:
            await direct_engine.dispose()


class TestHealth:
    @pytest.mark.asyncio
    async def test_forever_class_has_daily_coverage_health(
        self, history_schema: HistorySchema
    ) -> None:
        async with history_schema.engine.begin() as connection:
            creations = await ensure_leaf_coverage(
                connection,
                EnsureLeafCoverage(class_key='forever', horizon_days=3),
                UnpublishedLoader(),
            )
            assert not any(
                isinstance(creation, ForeverClassLeaf)
                for creation in creations
            )
            report = await collect_partition_health(
                connection,
                CollectPartitionHealth(
                    class_key='forever', application_managed=True
                ),
            )
            assert report.is_healthy
            assert report.coverage is not None
            assert report.coverage.complete_future_intervals >= 2
            assert report.coverage.detachable_leaf_count == 0

    @pytest.mark.asyncio
    async def test_covered_class_is_healthy(
        self, history_schema: HistorySchema
    ) -> None:
        async with history_schema.engine.begin() as connection:
            await register_class(connection, CLASS_KEY)
            await ensure_leaf_coverage(
                connection,
                EnsureLeafCoverage(class_key=CLASS_KEY, horizon_days=3),
                UnpublishedLoader(),
            )
            report = await collect_partition_health(
                connection,
                CollectPartitionHealth(
                    class_key=CLASS_KEY, application_managed=True
                ),
            )
            assert report.is_healthy
            assert report.coverage is not None
            assert report.coverage.attached_leaf_count == 4
            assert report.coverage.complete_future_intervals >= 2

    @pytest.mark.asyncio
    async def test_uncovered_class_goes_red_before_terminalization_would(
        self, history_schema: HistorySchema
    ) -> None:
        async with history_schema.engine.begin() as connection:
            await register_class(connection, CLASS_KEY)
            report = await collect_partition_health(
                connection,
                CollectPartitionHealth(
                    class_key=CLASS_KEY, application_managed=True
                ),
            )
            assert not report.is_healthy
            assert any(
                isinstance(fault, CoverageBelowFloor)
                and fault.complete_future_intervals == 0
                for fault in report.faults
            )
