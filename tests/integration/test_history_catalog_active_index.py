"""The leaf catalog's active-record index keeps retention scans off dropped leaves.

A dropped leaf's catalog record stays, so the catalog grows by one record per
class per day. The expired-leaf read (``horsies_prune_candidates``) reads only
records whose leaf is not dropped; with the partial index it never examines a
dropped record.

A plain ``EXPLAIN`` of a PL/pgSQL call shows a Function Scan only. The plan of
the statement inside the function comes from ``auto_explain`` with
``log_nested_statements``; ``client_min_messages = log`` sends it to this
session as a notice.
"""

from __future__ import annotations

import json
from typing import Final, cast

import psycopg
import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.history.commands import PRUNE_BOUNDS
from horsies.core.history.names import LEAF_CATALOG
from tests.integration.task_history_harness import register_class

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]

CLASS_KEY = 'standard_30d'
DROPPED_RECORDS = 6_000
ACTIVE_RECORDS = 20
ACTIVE_INDEX: Final = 'horsies_task_history_leaf_catalog_active'
MAX_PLAN_NODES: Final = 256
"""Upper bound of the plan nodes the walk reads; a larger plan fails the test."""
PLAN_MARKER: Final = 'plan:\n'
"""The text before the JSON plan in an ``auto_explain`` message."""

type PlanNode = dict[str, object]

SEED_CATALOG_SQL = text(f"""
    INSERT INTO {LEAF_CATALOG} (
        leaf_name, class_key, lower_bound_kind, lower_anchor, upper_anchor,
        index_schema_version, created_at, detached_at, dropped_at
    )
    SELECT 'catalog_probe_leaf_' || day, :class_key, 'AT',
           TIMESTAMPTZ '2000-01-01 00:00:00+00' + day * interval '1 day',
           TIMESTAMPTZ '2000-01-02 00:00:00+00' + day * interval '1 day',
           1, NOW(),
           CASE WHEN day < :dropped THEN NOW() END,
           CASE WHEN day < :dropped THEN NOW() END
    FROM generate_series(0, :total - 1) AS day
""")

INDEX_DEFINITION_SQL = text('SELECT pg_get_indexdef(to_regclass(:index_name))')

AUTO_EXPLAIN_SETTINGS: Final = (
    "LOAD 'auto_explain'",
    'SET LOCAL auto_explain.log_min_duration = 0',
    'SET LOCAL auto_explain.log_nested_statements = on',
    'SET LOCAL auto_explain.log_analyze = on',
    "SET LOCAL auto_explain.log_format = 'json'",
    'SET LOCAL client_min_messages = log',
)

CANDIDATES_SQL = text('SELECT count(*) FROM horsies_prune_candidates(:limit)')


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


def _logged_plans(messages: list[str]) -> list[PlanNode]:
    """The root plan node of every ``auto_explain`` message."""
    plans: list[PlanNode] = []
    for message in messages:
        if PLAN_MARKER not in message:
            continue
        document = json.loads(message.split(PLAN_MARKER, 1)[1])
        plans.append(cast(PlanNode, document['Plan']))
    return plans


async def test_expired_leaf_scan_examines_only_active_records(
    broker: PostgresBroker,
    engine: AsyncEngine,
) -> None:
    _ = broker
    messages: list[str] = []

    def collect(diagnostic: psycopg.errors.Diagnostic) -> None:
        if diagnostic.message_primary is not None:
            messages.append(diagnostic.message_primary)

    async with engine.connect() as connection:
        raw = await connection.get_raw_connection()
        driver = cast(psycopg.AsyncConnection[object], raw.driver_connection)
        driver.add_notice_handler(collect)
        transaction = await connection.begin()
        try:
            await register_class(connection, CLASS_KEY)
            await connection.execute(SEED_CATALOG_SQL, {
                'class_key': CLASS_KEY,
                'dropped': DROPPED_RECORDS,
                'total': DROPPED_RECORDS + ACTIVE_RECORDS,
            })
            await connection.execute(text(f'ANALYZE {LEAF_CATALOG}'))
            definition = cast(str | None, (await connection.execute(
                INDEX_DEFINITION_SQL, {'index_name': ACTIVE_INDEX},
            )).scalar_one())
            for setting in AUTO_EXPLAIN_SETTINGS:
                await connection.execute(text(setting))
            candidates = (await connection.execute(
                CANDIDATES_SQL, {'limit': PRUNE_BOUNDS.candidates + 1},
            )).scalar_one()
        finally:
            await transaction.rollback()
            driver.remove_notice_handler(collect)

    # The index holds only records whose leaf is not dropped, and every
    # catalog scan inside the function reads through it: the read examines
    # no dropped record. Rows are not counted. EXPLAIN gives the rows of a
    # scan as a mean per loop, rounded, so the mean times the loops is not
    # the exact count.
    assert definition is not None, f'{ACTIVE_INDEX} does not exist'
    assert definition.endswith(' WHERE (dropped_at IS NULL)'), definition
    scans = [
        scan for plan in _logged_plans(messages) for scan in _catalog_scan_indexes(plan)
    ]
    assert scans, messages
    assert all(scan == {ACTIVE_INDEX} for scan in scans), messages
    # The seeded active records are expired, so the read returns them.
    assert cast(int, candidates) >= ACTIVE_RECORDS
