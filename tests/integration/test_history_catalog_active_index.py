"""The leaf catalog's active-record index keeps retention scans off dropped leaves.

A dropped leaf's catalog record stays, so the catalog grows by one record per
class per day. The expired-leaf scan reads only records whose leaf is not
dropped; with the partial index it never examines a dropped record.
"""

from __future__ import annotations

from typing import Final, cast

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
ACTIVE_INDEX: Final = 'idx_horsies_history_catalog_active'
MAX_PLAN_NODES: Final = 256
"""Upper bound of the plan nodes the walk reads; a larger plan fails the test."""

type PlanNode = dict[str, object]

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

INDEX_DEFINITION_SQL = text('SELECT pg_get_indexdef(to_regclass(:index_name))')


def _plan_nodes(root: PlanNode) -> list[PlanNode]:
    """Return every node of the plan tree."""
    nodes: list[PlanNode] = []
    pending = [root]
    while pending:
        assert len(nodes) < MAX_PLAN_NODES, f'plan has more than {MAX_PLAN_NODES} nodes'
        node = pending.pop()
        nodes.append(node)
        pending.extend(cast(list[PlanNode], node.get('Plans', [])))
    return nodes


def _catalog_scan_indexes(root: PlanNode) -> list[frozenset[str]]:
    """Return, for each catalog scan in the plan, the indexes it reads through.

    A scan that reads the table without an index (a Seq Scan) reads through none.
    """
    scans: list[frozenset[str]] = []
    for node in _plan_nodes(root):
        if node.get('Relation Name') != LEAF_CATALOG:
            continue
        match node.get('Node Type'):
            case 'Index Scan' | 'Index Only Scan':
                scans.append(frozenset({str(node['Index Name'])}))
            case 'Bitmap Heap Scan':
                scans.append(frozenset(
                    str(inner['Index Name'])
                    for inner in _plan_nodes(node)
                    if 'Index Name' in inner
                ))
            case _:
                scans.append(frozenset())
    return scans


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
            definition = cast(str | None, (await connection.execute(
                INDEX_DEFINITION_SQL, {'index_name': ACTIVE_INDEX},
            )).scalar_one())
            plan = cast(PlanNode, (await connection.execute(
                text('EXPLAIN (ANALYZE, FORMAT JSON) ' + EXPIRED_HISTORY_LEAVES_SQL),
                {'heartbeat_class_key': HEARTBEAT_CLASS_KEY},
            )).scalar_one()[0]['Plan'])
        finally:
            await transaction.rollback()

    # The index holds only records whose leaf is not dropped, and every
    # catalog scan reads through it: the scan examines no dropped record.
    # Rows are not counted. EXPLAIN gives the rows of a scan as a mean per
    # loop, rounded, so the mean times the loops is not the exact count.
    assert definition is not None, f'{ACTIVE_INDEX} does not exist'
    assert definition.endswith(' WHERE (dropped_at IS NULL)'), definition
    scans = _catalog_scan_indexes(plan)
    assert scans, plan
    assert all(scan == {ACTIVE_INDEX} for scan in scans), plan
    # The seeded active records are expired, so the scan returns them.
    assert cast(float, plan['Actual Rows']) >= ACTIVE_RECORDS
