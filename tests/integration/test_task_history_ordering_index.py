"""Every task-history leaf carries the enqueue-order index.

The property under test is column composition read from ``pg_index`` —
a non-partial single-key btree on ``enqueued_at`` — never an index
name: the transcode swap attaches relations whose index names differ
from the canonical derivation, so a name assertion would pass against
a leaf the planner cannot use and fail against one it can.

The plan test is the consumer-side proof: the monitoring list's
default sort must ride the index and stop at its LIMIT instead of
sorting every matched row. It EXPLAINs the exact statement production
executes, and it goes red when leaf creation stops emitting the index.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from uuid import uuid4

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection

from horsies.core.history.commands import (
    CreateDailyHistoryLeaf,
    LeafBounds,
    LeafRef,
)
from horsies.core.history.outcomes import (
    LeafAlreadyConformant,
    LeafCreated,
    LeafIndexRepaired,
)
from horsies.core.history.partitions.catalog import (
    daily_leaf_name,
    leaf_enqueued_index_name,
    read_leaf_ordering_index_exists,
)
from horsies.core.history.partitions.manager import create_daily_leaf
from horsies.core.history.partitions.publication import UnpublishedLoader
from horsies.core.history.reads.pages import (
    HistoryPageQuery,
    HistoryScope,
    HistoryWindow,
    history_page_statement,
    history_sort_expression,
)
from horsies.core.history.names import TASK_HISTORY_FOREVER

from tests.integration.task_history_harness import (
    INSERT_HISTORY_ROW_SQL,
    HistorySchema,
    day_bounds,
    frozen_history_row,
    register_class,
    task_history_schema_fixture,
)

pytestmark = [
    pytest.mark.integration,
    pytest.mark.asyncio(loop_scope='function'),
]

UTC = timezone.utc
CLASS_KEY = 'it_ordering_idx'

history_schema = task_history_schema_fixture('task_history_it_ordering_idx')


def _leaf_ref(parent_name: str, lower: datetime) -> LeafRef:
    return LeafRef(
        leaf_name=daily_leaf_name(parent_name, lower),
        class_key=CLASS_KEY,
        bounds=LeafBounds(lower=lower, upper=lower + timedelta(days=1)),
    )


async def _create_leaf(
    connection: AsyncConnection, parent_name: str, lower: datetime
) -> LeafRef:
    ref = _leaf_ref(parent_name, lower)
    outcome = await create_daily_leaf(
        connection, CreateDailyHistoryLeaf(leaf=ref), UnpublishedLoader()
    )
    assert isinstance(outcome, LeafCreated)
    return ref


class TestLeafBirth:
    """Forever and daily leaves are born with the index."""

    async def test_forever_leaf_carries_the_ordering_index(
        self, history_schema: HistorySchema
    ) -> None:
        async with history_schema.engine.connect() as connection:
            assert await read_leaf_ordering_index_exists(
                connection, TASK_HISTORY_FOREVER
            )

    async def test_created_daily_leaf_carries_the_ordering_index(
        self, history_schema: HistorySchema
    ) -> None:
        lower, _ = day_bounds(datetime.now(UTC))
        async with history_schema.engine.begin() as connection:
            parent_name = await register_class(connection, CLASS_KEY)
            ref = await _create_leaf(connection, parent_name, lower)
            assert await read_leaf_ordering_index_exists(
                connection, ref.leaf_name
            )

    async def test_absent_relation_reports_no_index(
        self, history_schema: HistorySchema
    ) -> None:
        async with history_schema.engine.connect() as connection:
            assert not await read_leaf_ordering_index_exists(
                connection, 'horsies_task_history_never_created'
            )


class TestLeafRepair:
    """The conformance pass restores a missing ordering index."""

    async def test_repair_recreates_a_dropped_ordering_index(
        self, history_schema: HistorySchema
    ) -> None:
        lower, _ = day_bounds(datetime.now(UTC))
        async with history_schema.engine.begin() as connection:
            parent_name = await register_class(connection, CLASS_KEY)
            ref = await _create_leaf(connection, parent_name, lower)
            await connection.execute(
                text(
                    f'DROP INDEX {leaf_enqueued_index_name(ref.leaf_name)}'
                )
            )
            assert not await read_leaf_ordering_index_exists(
                connection, ref.leaf_name
            )

            outcome = await create_daily_leaf(
                connection,
                CreateDailyHistoryLeaf(leaf=ref),
                UnpublishedLoader(),
            )
            assert isinstance(outcome, LeafIndexRepaired)
            assert await read_leaf_ordering_index_exists(
                connection, ref.leaf_name
            )

    async def test_conformant_leaf_is_not_re_repaired(
        self, history_schema: HistorySchema
    ) -> None:
        lower, _ = day_bounds(datetime.now(UTC))
        async with history_schema.engine.begin() as connection:
            parent_name = await register_class(connection, CLASS_KEY)
            ref = await _create_leaf(connection, parent_name, lower)

            outcome = await create_daily_leaf(
                connection,
                CreateDailyHistoryLeaf(leaf=ref),
                UnpublishedLoader(),
            )
            assert isinstance(outcome, LeafAlreadyConformant)


class TestDefaultSortPlan:
    """The list's default sort rides the index and stops at the LIMIT."""

    async def test_default_sort_plans_without_a_sort_node(
        self, history_schema: HistorySchema
    ) -> None:
        """No Sort node above the append; backward index scans below it.

        Seeds two populated leaves — the forever leaf and one daily
        finite leaf — so ordered consumption across leaves (Merge
        Append) is required, not just a single-partition special case.
        ANALYZE is committed before the EXPLAIN so the plan reflects
        the seeded statistics (the retention plan tests' precedent).
        """
        now = datetime.now(UTC)
        lower, _ = day_bounds(now - timedelta(days=1))
        async with history_schema.engine.begin() as connection:
            parent_name = await register_class(connection, CLASS_KEY)
            ref = await _create_leaf(connection, parent_name, lower)
            for offset_minutes in range(200):
                anchor = lower + timedelta(minutes=offset_minutes)
                await connection.execute(
                    text(INSERT_HISTORY_ROW_SQL),
                    frozen_history_row(
                        task_id=str(uuid4()),
                        class_key=CLASS_KEY,
                        terminal_at=anchor,
                    ),
                )
                await connection.execute(
                    text(INSERT_HISTORY_ROW_SQL),
                    frozen_history_row(
                        task_id=str(uuid4()),
                        class_key='forever',
                        terminal_at=anchor,
                    ),
                )
            await connection.execute(
                text(f'ANALYZE horsies_task_history, {ref.leaf_name}')
            )

        window = HistoryWindow(
            lower=lower - timedelta(days=1), upper=now + timedelta(days=1)
        )
        page_sql, page_params = history_page_statement(
            HistoryPageQuery(
                window=window,
                limit=50,
                scope=HistoryScope(),
                order_by=history_sort_expression(
                    'enqueued_at', descending=True
                ),
            )
        )

        async with history_schema.engine.connect() as connection:
            await connection.execute(text('SET enable_seqscan = off'))
            plan_rows = (
                await connection.execute(
                    text('EXPLAIN ' + page_sql), page_params
                )
            ).fetchall()
            await connection.rollback()
        plan = '\n'.join(str(row[0]) for row in plan_rows)

        # A Merge Append annotates its ordering as "Sort Key:", so the
        # assertion targets Sort NODES: none may appear anywhere in the
        # tree — a sorted-consumption plan has index scans below an
        # ordered append, never a Sort operator.
        assert '->  Sort' not in plan, plan
        assert not plan.lstrip().startswith('Sort'), plan
        assert 'Merge Append' in plan, plan
        assert 'Index Scan Backward' in plan, plan
        assert '_enqueued_idx' in plan, plan
