"""The static forever leaf of the baseline schema, as maintenance sees it."""

from __future__ import annotations

import pytest
from sqlalchemy import text

from horsies.core.history.commands import OPEN_END_ANCHOR, OpenLowerBound
from horsies.core.history.names import FOREVER_LEAF
from horsies.core.history.outcomes import (
    CatalogConflictKind,
    LeafAlreadyConformant,
    LeafCatalogConflict,
)
from horsies.core.history.partitions.catalog import (
    capture_partition_bound_utc,
    read_leaf_catalog_row,
)
from horsies.core.history.partitions.manager import verify_forever_leaf
from tests.integration.task_history_harness import (
    HistorySchema,
    task_history_schema_fixture,
)

pytestmark = [pytest.mark.integration]

history_schema = task_history_schema_fixture('task_history_it_forever_leaf')


@pytest.mark.asyncio(loop_scope='function')
async def test_catalog_row_reads_as_an_open_lower_bound(
    history_schema: HistorySchema,
) -> None:
    async with history_schema.engine.begin() as connection:
        row = await read_leaf_catalog_row(connection, FOREVER_LEAF)
        physical_bound = await capture_partition_bound_utc(connection, FOREVER_LEAF)
    assert row is not None
    assert row.lower_bound == OpenLowerBound()
    assert row.upper_anchor == OPEN_END_ANCHOR
    assert row.partition_bound == physical_bound


@pytest.mark.asyncio(loop_scope='function')
async def test_fresh_baseline_leaf_is_conformant(
    history_schema: HistorySchema,
) -> None:
    async with history_schema.engine.begin() as connection:
        outcome = await verify_forever_leaf(connection)
    assert outcome == LeafAlreadyConformant(leaf_name=FOREVER_LEAF)


@pytest.mark.asyncio(loop_scope='function')
async def test_missing_index_is_a_conflict_and_is_not_repaired(
    history_schema: HistorySchema,
) -> None:
    async with history_schema.engine.begin() as connection:
        await connection.execute(text(f'DROP INDEX {FOREVER_LEAF}_task_idx'))
        outcome = await verify_forever_leaf(connection)
        restored = (
            await connection.execute(
                text('SELECT to_regclass(:name) IS NOT NULL'),
                {'name': f'{FOREVER_LEAF}_task_idx'},
            )
        ).scalar_one()
    match outcome:
        case LeafCatalogConflict(kind=kind):
            assert kind is CatalogConflictKind.PHYSICAL_NONCONFORMANT
        case other:
            pytest.fail(f'expected a conflict, got {other!r}')
    assert restored is False


@pytest.mark.asyncio(loop_scope='function')
async def test_missing_catalog_row_is_a_conflict(
    history_schema: HistorySchema,
) -> None:
    async with history_schema.engine.begin() as connection:
        await connection.execute(
            text('DELETE FROM horsies_task_history_leaf_catalog WHERE leaf_name = :name'),
            {'name': FOREVER_LEAF},
        )
        outcome = await verify_forever_leaf(connection)
    match outcome:
        case LeafCatalogConflict(kind=kind):
            assert kind is CatalogConflictKind.RELATION_WITHOUT_CATALOG
        case other:
            pytest.fail(f'expected a conflict, got {other!r}')
