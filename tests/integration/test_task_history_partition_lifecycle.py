"""Partition manager lifecycle against real PostgreSQL.

Exercises the accepted checkpoint-1 contract end to end in a disposable
schema: class registration, create-ahead, conformance repair, pending
blockers refusing detach, concurrent detach with catalog reconciliation,
drop-after-detach, and the health pass. Every refusal is asserted as its
typed outcome, never as an exception.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from hashlib import sha256
from uuid import uuid4

import pytest
from sqlalchemy import text

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
    LeafNotExpired,
    LeafPendingBlocked,
    RetentionClassAbsent,
)
from horsies.core.history.partitions.catalog import daily_leaf_name
from horsies.core.history.partitions.health import collect_partition_health
from horsies.core.history.partitions.manager import (
    create_daily_leaf,
    detach_expired_leaf,
    drop_detached_leaf,
    ensure_leaf_coverage,
    inspect_leaf,
)
from horsies.core.history.partitions.publication import UnpublishedLoader

from tests.integration.task_history_harness import (
    HistorySchema,
    day_bounds,
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
            history_schema.engine,
            DetachExpiredHistoryLeaf(leaf=ref),
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
                        :task_id, :workflow_id, 1,
                        'COMPLETED', :anchor, 'COMPLETE_FUSED',
                        'HISTORY', :class_key, :anchor,
                        1, :digest, :generation, statement_timestamp(), 0
                    )
                    """
                ),
                {
                    'task_id': str(uuid4()),
                    'workflow_id': str(uuid4()),
                    'anchor': ref.bounds.lower + timedelta(hours=1),
                    'class_key': CLASS_KEY,
                    'digest': sha256(b'result').digest(),
                    'generation': str(uuid4()),
                },
            )
        refused = await detach_expired_leaf(
            history_schema.engine,
            DetachExpiredHistoryLeaf(leaf=ref),
            UnpublishedLoader(),
        )
        assert isinstance(refused, LeafPendingBlocked)
        assert refused.blocker_count == 1
        async with history_schema.engine.begin() as connection:
            await connection.execute(
                text('DELETE FROM horsies_workflow_phase2_pending')
            )
        detached = await detach_expired_leaf(
            history_schema.engine,
            DetachExpiredHistoryLeaf(leaf=ref),
            UnpublishedLoader(),
        )
        assert isinstance(detached, LeafDetached)


class TestHealth:
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
