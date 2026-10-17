"""The leaf catalog's active-record index keeps retention scans off dropped leaves.

A dropped leaf's catalog record stays, so the catalog grows by one record per
class per day. The expired-leaf scan reads only records whose leaf is not
dropped; with the partial index it never examines a dropped record.
"""

from __future__ import annotations

from typing import cast

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.history.maintenance.pruning import EXPIRED_HISTORY_LEAVES_SQL
from horsies.core.history.names import HEARTBEAT_CLASS_KEY, LEAF_CATALOG
from tests.integration.task_history_harness import register_class

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

CLASS_KEY = 'catalog_active_probe'
DROPPED_RECORDS = 6_000
ACTIVE_RECORDS = 20

SEED_CATALOG_SQL = text(f"""
    INSERT INTO {LEAF_CATALOG} (
        leaf_name, parent_name, class_key, lower_anchor, upper_anchor,
        index_schema_version, id_index_name, partition_bound,
        min_birth_at, min_birth_verified, created_at, detached_at, dropped_at
    )
    SELECT 'catalog_probe_leaf_' || day, :parent, :class_key,
           TIMESTAMPTZ '2000-01-01 00:00:00+00' + day * interval '1 day',
           TIMESTAMPTZ '2000-01-02 00:00:00+00' + day * interval '1 day',
           1, 'catalog_probe_idx_' || day, 'probe', NULL, FALSE, NOW(),
           CASE WHEN day < :dropped THEN NOW() END,
           CASE WHEN day < :dropped THEN NOW() END
    FROM generate_series(0, :total - 1) AS day
""")


def _catalog_rows_examined(plan: object) -> float:
    match plan:
        case list():
            return sum(_catalog_rows_examined(item) for item in cast(list[object], plan))
        case dict():
            fields = cast(dict[str, object], plan)
            current = 0.0
            if fields.get('Relation Name') == LEAF_CATALOG:
                per_loop = sum(
                    float(cast(int | float, fields.get(name, 0)))
                    for name in ('Actual Rows', 'Rows Removed by Filter')
                )
                current = per_loop * float(cast(int | float, fields.get('Actual Loops', 0)))
            return current + sum(_catalog_rows_examined(value) for value in fields.values())
        case _:
            return 0.0


def _index_names(plan: object) -> set[str]:
    names: set[str] = set()
    match plan:
        case list():
            for item in cast(list[object], plan):
                names |= _index_names(item)
        case dict():
            fields = cast(dict[str, object], plan)
            if 'Index Name' in fields:
                names.add(str(fields['Index Name']))
            for value in fields.values():
                names |= _index_names(value)
        case _:
            pass
    return names


async def test_expired_leaf_scan_examines_only_active_records(
    broker: PostgresBroker,
    engine: AsyncEngine,
) -> None:
    _ = broker
    async with engine.connect() as connection:
        transaction = await connection.begin()
        try:
            parent = await register_class(connection, CLASS_KEY)
            await connection.execute(SEED_CATALOG_SQL, {
                'parent': parent,
                'class_key': CLASS_KEY,
                'dropped': DROPPED_RECORDS,
                'total': DROPPED_RECORDS + ACTIVE_RECORDS,
            })
            await connection.execute(text(f'ANALYZE {LEAF_CATALOG}'))
            active_in_catalog = int((await connection.execute(text(
                f'SELECT count(*) FROM {LEAF_CATALOG} WHERE dropped_at IS NULL'
            ))).scalar_one())
            plan = (await connection.execute(
                text('EXPLAIN (ANALYZE, FORMAT JSON) ' + EXPIRED_HISTORY_LEAVES_SQL),
                {'heartbeat_class_key': HEARTBEAT_CLASS_KEY},
            )).scalar_one()[0]['Plan']
        finally:
            await transaction.rollback()

    assert 'idx_horsies_history_catalog_active' in _index_names(plan)
    # Every class's active records, and none of the dropped ones.
    assert active_in_catalog >= ACTIVE_RECORDS
    assert _catalog_rows_examined(plan) <= active_in_catalog
