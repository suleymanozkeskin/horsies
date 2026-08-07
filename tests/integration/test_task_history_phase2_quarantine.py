"""Detach-horizon quarantine against real PostgreSQL.

The end-to-end protocol from real pending rows created by the real failure
move: an over-horizon locator is copied, verified, and repointed; the
pending row swaps to the quarantine source with its history locator
cleared; consumption then serves from quarantine and deletes both rows at
disposition. Refusals are proven to retain evidence — a broken locator
keeps its history source untouched — and under-horizon locators are
treated as in-flight drain traffic, not stalled evidence.
"""

from __future__ import annotations

from datetime import timedelta
from typing import Any
from uuid import uuid4

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection

from horsies.core.history.commands import LeafBounds, LeafRef
from horsies.core.history.partitions.catalog import (
    daily_leaf_name,
    database_now,
)
from horsies.core.history.phase2.consumption import consume_phase2
from horsies.core.history.phase2.quarantine import (
    BlockersQuarantined,
    NoOverHorizonBlockers,
    QuarantineLeafBlockers,
    QuarantineRefused,
    quarantine_over_horizon_blockers,
)

from tests.integration.task_history_harness import (
    HistorySchema,
    create_workflow,
    insert_live_task,
    link_workflow_node,
    prepare_move_storage,
    terminalization_schema_fixture,
)

pytestmark = [pytest.mark.integration]

CLASS_KEY = 'it_p2q'
WORKER = 'worker-p2q-1'
FAILURE_RESULT = '{"error":{"code":"BOOM"}}'
HORIZON = timedelta(days=7)

terminalization_schema = terminalization_schema_fixture(
    'task_history_it_p2_quarantine'
)


async def seed_pending(
    connection: AsyncConnection,
    *,
    node_key: str | None = 'node-0',
) -> tuple[str, str, str]:
    """Fail a workflow-backing task for real; returns (task, workflow, node)."""
    task_id = await insert_live_task(
        connection,
        class_key=CLASS_KEY,
        worker=WORKER,
        is_workflow_task=True,
    )
    workflow_id = await create_workflow(connection, status='RUNNING')
    node_id = await link_workflow_node(
        connection,
        task_id,
        workflow_id=workflow_id,
        node_status='RUNNING',
        node_key=node_key,
    )
    outcome = (
        await connection.execute(
            text(
                'SELECT outcome FROM horsies_fail_locked_task('
                'CAST(:task_id AS uuid), :worker, :result, '
                "'BOOM', 'exploded')"
            ),
            {'task_id': task_id, 'worker': WORKER, 'result': FAILURE_RESULT},
        )
    ).one()
    assert outcome.outcome == 'APPLIED'
    return task_id, workflow_id, node_id


async def age_pending(connection: AsyncConnection, task_id: str) -> None:
    await connection.execute(
        text(
            'UPDATE horsies_workflow_phase2_pending '
            "SET created_at = created_at - interval '8 days' "
            'WHERE task_id = CAST(:task_id AS uuid)'
        ),
        {'task_id': task_id},
    )


async def pending_leaf_ref(
    connection: AsyncConnection, task_id: str
) -> LeafRef:
    """Build the LeafRef the pending row's history locator names."""
    row = (
        await connection.execute(
            text(
                'SELECT history_class, history_anchor '
                'FROM horsies_workflow_phase2_pending '
                'WHERE task_id = CAST(:task_id AS uuid)'
            ),
            {'task_id': task_id},
        )
    ).one()
    parent_name = (
        await connection.execute(
            text(
                'SELECT finite_parent_name FROM horsies_retention_classes '
                'WHERE class_key = :class_key'
            ),
            {'class_key': row.history_class},
        )
    ).scalar_one()
    lower = row.history_anchor.replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    return LeafRef(
        leaf_name=daily_leaf_name(parent_name, lower),
        class_key=row.history_class,
        bounds=LeafBounds(lower=lower, upper=lower + timedelta(days=1)),
    )


async def pending_row(connection: AsyncConnection, task_id: str) -> Any:
    return (
        await connection.execute(
            text(
                'SELECT recovery_source, history_class, history_anchor, '
                'quarantine_task_id '
                'FROM horsies_workflow_phase2_pending '
                'WHERE task_id = CAST(:task_id AS uuid)'
            ),
            {'task_id': task_id},
        )
    ).one()


class TestRepoint:
    @pytest.mark.asyncio
    async def test_over_horizon_locator_is_copied_verified_repointed(
        self, terminalization_schema: HistorySchema
    ) -> None:
        async with terminalization_schema.engine.begin() as connection:
            await prepare_move_storage(connection, CLASS_KEY)
            task_id, _, _ = await seed_pending(connection)
            await age_pending(connection, task_id)
            leaf = await pending_leaf_ref(connection, task_id)

            outcome = await quarantine_over_horizon_blockers(
                connection,
                QuarantineLeafBlockers(leaf=leaf, horizon=HORIZON),
            )
            assert outcome == BlockersQuarantined(
                leaf_name=leaf.leaf_name, repointed=1, drained=0
            )

            pending = await pending_row(connection, task_id)
            assert pending.recovery_source == 'QUARANTINE'
            assert str(pending.quarantine_task_id) == task_id
            assert pending.history_class is None
            assert pending.history_anchor is None

            copy = (
                await connection.execute(
                    text(
                        'SELECT node_id, terminal_status, '
                        'source_history_class, quarantine_reason, '
                        "convert_from(result_payload, 'UTF8') AS payload "
                        'FROM horsies_workflow_phase2_quarantine '
                        'WHERE task_id = CAST(:task_id AS uuid)'
                    ),
                    {'task_id': task_id},
                )
            ).one()
            assert copy.node_id == 'node-0'
            assert copy.terminal_status == 'FAILED'
            assert copy.source_history_class == CLASS_KEY
            assert leaf.leaf_name in copy.quarantine_reason
            assert copy.payload == FAILURE_RESULT

    @pytest.mark.asyncio
    async def test_consumption_serves_from_quarantine_and_deletes_both(
        self, terminalization_schema: HistorySchema
    ) -> None:
        async with terminalization_schema.engine.begin() as connection:
            await prepare_move_storage(connection, CLASS_KEY)
            task_id, _, node_id = await seed_pending(connection)
            await age_pending(connection, task_id)
            leaf = await pending_leaf_ref(connection, task_id)
            await quarantine_over_horizon_blockers(
                connection,
                QuarantineLeafBlockers(leaf=leaf, horizon=HORIZON),
            )

            verdict = await consume_phase2(
                connection, task_id=task_id, terminal_node_status='FAILED'
            )
            assert verdict.disposition == 'APPLIED_TO_NODE'
            node = (
                await connection.execute(
                    text(
                        'SELECT status, result FROM horsies_workflow_tasks '
                        'WHERE id = CAST(:node_id AS uuid)'
                    ),
                    {'node_id': node_id},
                )
            ).one()
            assert node.status == 'FAILED'
            assert node.result == FAILURE_RESULT
            for table in (
                'horsies_workflow_phase2_pending',
                'horsies_workflow_phase2_quarantine',
            ):
                count = (
                    await connection.execute(
                        text(
                            f'SELECT count(*) FROM {table} '
                            'WHERE task_id = CAST(:task_id AS uuid)'
                        ),
                        {'task_id': task_id},
                    )
                ).scalar_one()
                assert count == 0, table

    @pytest.mark.asyncio
    async def test_repointed_locator_no_longer_counts_as_leaf_blocker(
        self, terminalization_schema: HistorySchema
    ) -> None:
        async with terminalization_schema.engine.begin() as connection:
            await prepare_move_storage(connection, CLASS_KEY)
            task_id, _, _ = await seed_pending(connection)
            await age_pending(connection, task_id)
            leaf = await pending_leaf_ref(connection, task_id)
            blocker_query = text(
                'SELECT count(*) FROM horsies_workflow_phase2_pending '
                "WHERE recovery_source = 'HISTORY' "
                'AND history_class = :class_key '
                'AND history_anchor >= :lower AND history_anchor < :upper'
            )
            params = {
                'class_key': leaf.class_key,
                'lower': leaf.bounds.lower,
                'upper': leaf.bounds.upper,
            }
            before = (
                await connection.execute(blocker_query, params)
            ).scalar_one()
            assert before == 1
            await quarantine_over_horizon_blockers(
                connection,
                QuarantineLeafBlockers(leaf=leaf, horizon=HORIZON),
            )
            after = (
                await connection.execute(blocker_query, params)
            ).scalar_one()
            assert after == 0


class TestRestraintAndRefusal:
    @pytest.mark.asyncio
    async def test_under_horizon_locator_is_left_alone(
        self, terminalization_schema: HistorySchema
    ) -> None:
        async with terminalization_schema.engine.begin() as connection:
            await prepare_move_storage(connection, CLASS_KEY)
            task_id, _, _ = await seed_pending(connection)
            leaf = await pending_leaf_ref(connection, task_id)
            outcome = await quarantine_over_horizon_blockers(
                connection,
                QuarantineLeafBlockers(leaf=leaf, horizon=HORIZON),
            )
            assert outcome == NoOverHorizonBlockers(leaf_name=leaf.leaf_name)
            pending = await pending_row(connection, task_id)
            assert pending.recovery_source == 'HISTORY'

    @pytest.mark.asyncio
    async def test_broken_locator_refuses_and_retains_history_source(
        self, terminalization_schema: HistorySchema
    ) -> None:
        async with terminalization_schema.engine.begin() as connection:
            await prepare_move_storage(connection, CLASS_KEY)
            task_id, _, _ = await seed_pending(connection)
            await age_pending(connection, task_id)
            leaf = await pending_leaf_ref(connection, task_id)
            await connection.execute(
                text(
                    'UPDATE horsies_workflow_phase2_pending '
                    "SET history_anchor = history_anchor "
                    "+ interval '6 hours' "
                    'WHERE task_id = CAST(:task_id AS uuid)'
                ),
                {'task_id': task_id},
            )
            outcome = await quarantine_over_horizon_blockers(
                connection,
                QuarantineLeafBlockers(leaf=leaf, horizon=HORIZON),
            )
            match outcome:
                case QuarantineRefused(repointed=0, refusals=(refusal,)):
                    assert refusal.task_id == task_id
                    assert refusal.verdict == 'SOURCE_ABSENT'
                case _:
                    raise AssertionError(f'unexpected outcome: {outcome!r}')
            pending = await pending_row(connection, task_id)
            assert pending.recovery_source == 'HISTORY'
            assert pending.history_class == CLASS_KEY
            quarantine_count = (
                await connection.execute(
                    text(
                        'SELECT count(*) '
                        'FROM horsies_workflow_phase2_quarantine '
                        'WHERE task_id = CAST(:task_id AS uuid)'
                    ),
                    {'task_id': task_id},
                )
            ).scalar_one()
            assert quarantine_count == 0

    @pytest.mark.asyncio
    async def test_null_node_identity_refuses_typed(
        self, terminalization_schema: HistorySchema
    ) -> None:
        async with terminalization_schema.engine.begin() as connection:
            await prepare_move_storage(connection, CLASS_KEY)
            task_id, _, _ = await seed_pending(connection, node_key=None)
            await age_pending(connection, task_id)
            leaf = await pending_leaf_ref(connection, task_id)
            outcome = await quarantine_over_horizon_blockers(
                connection,
                QuarantineLeafBlockers(leaf=leaf, horizon=HORIZON),
            )
            match outcome:
                case QuarantineRefused(refusals=(refusal,)):
                    assert refusal.verdict == 'NODE_IDENTITY_ABSENT'
                case _:
                    raise AssertionError(f'unexpected outcome: {outcome!r}')
            pending = await pending_row(connection, task_id)
            assert pending.recovery_source == 'HISTORY'

    @pytest.mark.asyncio
    async def test_unknown_task_is_pending_gone(
        self, terminalization_schema: HistorySchema
    ) -> None:
        async with terminalization_schema.engine.begin() as connection:
            await prepare_move_storage(connection, CLASS_KEY)
            row = (
                await connection.execute(
                    text(
                        'SELECT * FROM horsies_phase2_quarantine_one('
                        "CAST(:task_id AS uuid), 'test')"
                    ),
                    {'task_id': str(uuid4())},
                )
            ).one()
            assert row.verdict == 'PENDING_GONE'


class TestStatementAtATime:
    @pytest.mark.asyncio
    async def test_operation_completes_on_an_autocommit_connection(
        self, terminalization_schema: HistorySchema
    ) -> None:
        """The detach path runs this op on its session-lock connection,
        which is autocommit; every repoint must be durable statement by
        statement, not via a wrapping transaction."""
        async with terminalization_schema.engine.begin() as connection:
            await prepare_move_storage(connection, CLASS_KEY)
            task_id, _, _ = await seed_pending(connection)
            await age_pending(connection, task_id)
            leaf = await pending_leaf_ref(connection, task_id)
        autocommit_engine = terminalization_schema.engine.execution_options(
            isolation_level='AUTOCOMMIT'
        )
        async with autocommit_engine.connect() as connection:
            now = await database_now(connection)
            assert now is not None
            outcome = await quarantine_over_horizon_blockers(
                connection,
                QuarantineLeafBlockers(leaf=leaf, horizon=HORIZON),
            )
            assert outcome == BlockersQuarantined(
                leaf_name=leaf.leaf_name, repointed=1, drained=0
            )
        async with terminalization_schema.engine.begin() as connection:
            pending = await pending_row(connection, task_id)
            assert pending.recovery_source == 'QUARANTINE'
