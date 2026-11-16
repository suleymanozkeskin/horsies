"""The coverage pass statements, before the 0008 coverage functions.

``maintain_partition_coverage`` (``horsies/core/history/maintenance/coverage.py``)
sent, before the switch:

- the probe: ``SET LOCAL TIME ZONE 'UTC'`` and one set-based statement
  (``COVERAGE_PROBE_SQL``) with the expected registrations as JSON;
- the registration: ``register_heartbeat_class`` and
  ``register_finite_retention_class`` for the default and each declared class;
- one transaction per damaged leaf: ``create_daily_leaf`` (or
  ``verify_forever_leaf``) with ``UnpublishedLoader``, or
  ``create_hourly_heartbeat_leaf``; a busy leaf retried 3 times.

The orchestration around them (the gate, the repair loop, the final
publication) is unchanged by the switch, so the reference side runs the
production ``maintain_partition_coverage`` with these four steps in place of
the switched ones (``statements_before``).
"""

from __future__ import annotations

import asyncio
import json
import random
from collections.abc import Generator, Sequence
from contextlib import contextmanager
from datetime import timedelta
from typing import Final

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection

from horsies.core.history.commands import (
    OPEN_END_ANCHOR,
    CreateDailyHistoryLeaf,
    ForeverLeafRef,
    LeafRef,
)
from horsies.core.history.ddl.classes import (
    DEFAULT_RETENTION_CLASS_KEY,
    DEFAULT_RETENTION_DURATION,
    ClassAlreadyRegistered,
    ClassRegistered,
    register_finite_retention_class,
)
from horsies.core.history.ddl.tables import FOREVER_CLASS_KEY
from horsies.core.history.heartbeats.partitioning import (
    CreateHourlyHeartbeatLeaf,
    HeartbeatClassRegistered,
    HeartbeatClassVerified,
    HeartbeatHorizonUpdated,
    create_hourly_heartbeat_leaf,
    register_heartbeat_class,
)
from horsies.core.history.maintenance import coverage
from horsies.core.history.maintenance.database import PartitionMaintenanceDatabase
from horsies.core.history.names import (
    FOREVER_LEAF,
    HEARTBEAT_CLASS_KEY,
    HEARTBEATS_TABLE,
    LEAF_CATALOG,
    RETENTION_CLASSES,
    TASK_HISTORY_FOREVER,
    TASK_HISTORY_PARENT,
)
from horsies.core.history.outcomes import LeafCreation, LeafMaintenanceBusy
from horsies.core.history.partitions.catalog import INDEX_SCHEMA_VERSION
from horsies.core.history.partitions.manager import create_daily_leaf, verify_forever_leaf
from horsies.core.history.partitions.publication import UnpublishedLoader

_LEAF_MAINTENANCE_ATTEMPTS: Final = 3
_BUSY_RETRY_MIN_SECONDS: Final = 0.02
_BUSY_RETRY_MAX_SECONDS: Final = 0.08

COVERAGE_PROBE_SQL: Final = f"""
WITH clock AS (
    SELECT statement_timestamp() AS checked_at
),
expected_registration AS (
    SELECT *
    FROM jsonb_to_recordset(CAST(:registrations AS jsonb)) AS expected(
        class_key text,
        class_kind text,
        retention_days integer,
        heartbeat_horizon_hours integer
    )
),
registration_health AS (
    -- The kind CHECK of the table holds every row to its kind's shape, so
    -- the probe compares only the expected rows and the forever row.
    SELECT
        NOT EXISTS (
            SELECT 1
            FROM expected_registration AS expected
            LEFT JOIN {RETENTION_CLASSES} AS registered
              ON registered.class_key = expected.class_key
            WHERE registered.class_key IS NULL
               OR registered.class_kind IS DISTINCT FROM expected.class_kind
               OR registered.retention_days
                    IS DISTINCT FROM expected.retention_days
               OR registered.heartbeat_horizon_hours
                    IS DISTINCT FROM expected.heartbeat_horizon_hours
        )
        AND EXISTS (
            SELECT 1
            FROM {RETENTION_CLASSES} AS registered
            WHERE registered.class_key = :forever_class
              AND registered.class_kind = 'FOREVER'
        ) AS healthy
),
history_classes AS (
    SELECT
        registered.class_key,
        CASE
            WHEN registered.class_kind = 'FOREVER'
                THEN CAST(:forever_parent AS text)
            ELSE :history_parent || '_' || registered.class_key
        END AS parent_name
    FROM {RETENTION_CLASSES} AS registered
    WHERE registered.class_kind IN ('FOREVER', 'PERIOD')
),
expected_history AS (
    SELECT
        'history'::text AS leaf_kind,
        classes.class_key,
        classes.parent_name,
        classes.parent_name || '_' ||
            to_char(day_bounds.lower_anchor AT TIME ZONE 'UTC', 'YYYY_MM_DD')
            AS leaf_name,
        'AT'::text AS lower_bound_kind,
        day_bounds.lower_anchor,
        day_bounds.lower_anchor + interval '1 day' AS upper_anchor
    FROM history_classes AS classes
    CROSS JOIN clock
    CROSS JOIN LATERAL (
        SELECT
            (
                date_trunc('day', clock.checked_at AT TIME ZONE 'UTC')
                AT TIME ZONE 'UTC'
            ) + day_offset * interval '1 day' AS lower_anchor
        FROM generate_series(0, :history_horizon) AS day_offset
    ) AS day_bounds
    WHERE classes.class_key <> :forever_class
),
-- The static forever leaf covers every time of `forever`: an OPEN lower
-- bound (MINVALUE) and the open end.
expected_forever AS (
    SELECT
        'history'::text AS leaf_kind,
        classes.class_key,
        classes.parent_name,
        CAST(:forever_leaf AS text) AS leaf_name,
        'OPEN'::text AS lower_bound_kind,
        NULL::timestamptz AS lower_anchor,
        CAST(:open_end AS timestamptz) AS upper_anchor
    FROM history_classes AS classes
    WHERE classes.class_key = :forever_class
),
expected_heartbeats AS (
    SELECT
        'heartbeat'::text AS leaf_kind,
        CAST(:heartbeat_class AS text) AS class_key,
        CAST(:heartbeat_parent AS text) AS parent_name,
        :heartbeat_parent || '_' ||
            to_char(
                hour_bounds.lower_anchor AT TIME ZONE 'UTC',
                'YYYY_MM_DD_HH24'
            )
            AS leaf_name,
        'AT'::text AS lower_bound_kind,
        hour_bounds.lower_anchor,
        hour_bounds.lower_anchor + interval '1 hour' AS upper_anchor
    FROM clock
    CROSS JOIN LATERAL (
        SELECT
            (
                date_trunc('hour', clock.checked_at AT TIME ZONE 'UTC')
                AT TIME ZONE 'UTC'
            ) + hour_offset * interval '1 hour' AS lower_anchor
        FROM generate_series(0, :heartbeat_horizon) AS hour_offset
    ) AS hour_bounds
),
expected_leaves AS (
    SELECT * FROM expected_history
    UNION ALL
    SELECT * FROM expected_forever
    UNION ALL
    SELECT * FROM expected_heartbeats
),
leaf_health AS (
    SELECT
        expected.*,
        child.oid AS child_oid,
        (
            catalog.leaf_name IS NOT NULL
            AND catalog.class_key = expected.class_key
            AND catalog.lower_bound_kind = expected.lower_bound_kind
            AND catalog.lower_anchor IS NOT DISTINCT FROM expected.lower_anchor
            AND catalog.upper_anchor = expected.upper_anchor
            AND catalog.index_schema_version = :index_schema_version
            AND catalog.detached_at IS NULL
            AND catalog.dropped_at IS NULL
            AND child.oid IS NOT NULL
            AND parent.oid IS NOT NULL
            AND inheritance.inhrelid IS NOT NULL
            AND NOT inheritance.inhdetachpending
            AND pg_get_expr(child.relpartbound, child.oid) = format(
                'FOR VALUES FROM (%s) TO (%L)',
                CASE expected.lower_bound_kind
                    WHEN 'OPEN' THEN 'MINVALUE'
                    ELSE quote_literal(expected.lower_anchor)
                END,
                expected.upper_anchor
            )
            AND EXISTS (
                SELECT 1
                FROM pg_index AS task_index
                JOIN pg_class AS task_index_relation
                  ON task_index_relation.oid = task_index.indexrelid
                JOIN pg_am AS task_index_method
                  ON task_index_method.oid = task_index_relation.relam
                WHERE task_index.indexrelid = to_regclass(
                        expected.leaf_name || CASE expected.leaf_kind
                            WHEN 'heartbeat' THEN '_probe_idx'
                            ELSE '_task_idx'
                        END
                    )
                  AND task_index.indrelid = child.oid
                  AND task_index_method.amname = 'btree'
                  AND task_index.indisvalid
                  AND task_index.indisready
                  AND task_index.indislive
                  AND NOT task_index.indisunique
                  AND NOT task_index.indisprimary
                  AND NOT task_index.indisexclusion
                  AND task_index.indpred IS NULL
                  AND task_index.indexprs IS NULL
                  AND (
                      (
                          expected.leaf_kind = 'history'
                          AND task_index.indnkeyatts = 1
                          AND task_index.indnatts = 1
                          AND task_index.indkey[0] = (
                              SELECT attribute.attnum
                              FROM pg_attribute AS attribute
                              WHERE attribute.attrelid = child.oid
                                AND attribute.attname = 'task_id'
                          )
                          AND task_index.indcollation[0] = (
                              SELECT attribute.attcollation
                              FROM pg_attribute AS attribute
                              WHERE attribute.attrelid = child.oid
                                AND attribute.attname = 'task_id'
                          )
                          AND pg_get_indexdef(
                              task_index.indexrelid, 1, false
                          ) = 'task_id'
                          AND pg_get_indexdef(task_index.indexrelid)
                              LIKE '% USING btree (task_id)'
                          AND task_index.indoption[0] = 0
                      )
                      OR (
                          expected.leaf_kind = 'heartbeat'
                          AND task_index.indnkeyatts = 3
                          AND task_index.indnatts = 3
                          AND task_index.indkey[0] = (
                              SELECT attribute.attnum
                              FROM pg_attribute AS attribute
                              WHERE attribute.attrelid = child.oid
                                AND attribute.attname = 'task_id'
                          )
                          AND task_index.indkey[1] = (
                              SELECT attribute.attnum
                              FROM pg_attribute AS attribute
                              WHERE attribute.attrelid = child.oid
                                AND attribute.attname = 'role'
                          )
                          AND task_index.indkey[2] = (
                              SELECT attribute.attnum
                              FROM pg_attribute AS attribute
                              WHERE attribute.attrelid = child.oid
                                AND attribute.attname = 'sent_at'
                          )
                          AND task_index.indcollation[0] = (
                              SELECT attribute.attcollation
                              FROM pg_attribute AS attribute
                              WHERE attribute.attrelid = child.oid
                                AND attribute.attname = 'task_id'
                          )
                          AND task_index.indcollation[1] = (
                              SELECT attribute.attcollation
                              FROM pg_attribute AS attribute
                              WHERE attribute.attrelid = child.oid
                                AND attribute.attname = 'role'
                          )
                          AND task_index.indcollation[2] = (
                              SELECT attribute.attcollation
                              FROM pg_attribute AS attribute
                              WHERE attribute.attrelid = child.oid
                                AND attribute.attname = 'sent_at'
                          )
                          AND pg_get_indexdef(
                              task_index.indexrelid, 1, false
                          ) = 'task_id'
                          AND pg_get_indexdef(
                              task_index.indexrelid, 2, false
                          ) = 'role'
                          AND pg_get_indexdef(
                              task_index.indexrelid, 3, false
                          ) = 'sent_at'
                          AND pg_get_indexdef(task_index.indexrelid)
                              LIKE '% USING btree '
                                   '(task_id, role, sent_at DESC)'
                          AND task_index.indoption[0] = 0
                          AND task_index.indoption[1] = 0
                          AND task_index.indoption[2] = 3
                      )
                  )
            )
            AND (
                expected.leaf_kind = 'heartbeat'
                OR EXISTS (
                    SELECT 1
                    FROM pg_index AS ordering_index
                    JOIN pg_class AS ordering_class
                      ON ordering_class.oid = ordering_index.indexrelid
                    JOIN pg_am AS ordering_method
                      ON ordering_method.oid = ordering_class.relam
                    JOIN pg_attribute AS ordering_attribute
                      ON ordering_attribute.attrelid = ordering_index.indrelid
                     AND ordering_attribute.attnum
                         = ordering_index.indkey[0]
                    WHERE ordering_index.indrelid = child.oid
                      AND ordering_method.amname = 'btree'
                      AND ordering_index.indisvalid
                      AND ordering_index.indisready
                      AND ordering_index.indislive
                      AND NOT ordering_index.indisunique
                      AND NOT ordering_index.indisprimary
                      AND NOT ordering_index.indisexclusion
                      AND ordering_index.indpred IS NULL
                      AND ordering_index.indexprs IS NULL
                      AND ordering_index.indnkeyatts = 1
                      AND ordering_index.indnatts = 1
                      AND ordering_attribute.attname = 'enqueued_at'
                      AND ordering_index.indcollation[0]
                          = ordering_attribute.attcollation
                      AND pg_get_indexdef(
                          ordering_index.indexrelid, 1, false
                      ) = 'enqueued_at'
                      AND pg_get_indexdef(ordering_index.indexrelid)
                          LIKE '% USING btree (enqueued_at)'
                      AND ordering_index.indoption[0] = 0
                )
            )
        ) AS conformant
    FROM expected_leaves AS expected
    LEFT JOIN {LEAF_CATALOG} AS catalog
      ON catalog.leaf_name = expected.leaf_name
    LEFT JOIN pg_class AS child
      ON child.oid = to_regclass(expected.leaf_name)
    LEFT JOIN pg_class AS parent
      ON parent.oid = to_regclass(expected.parent_name)
    LEFT JOIN pg_inherits AS inheritance
      ON inheritance.inhrelid = child.oid
     AND inheritance.inhparent = parent.oid
),
publication_health AS (
    -- The readers serve the current catalog: horsies_history_readers_current()
    -- compares the published manifest with the attached history leaves and
    -- checks the stamp of every published reader.
    SELECT
        horsies_history_readers_current() AS healthy,
        ARRAY(
            SELECT catalog.leaf_name
            FROM {LEAF_CATALOG} AS catalog
            WHERE catalog.class_key <> :heartbeat_class
              AND catalog.detached_at IS NULL
              AND catalog.dropped_at IS NULL
              AND to_regclass(catalog.leaf_name) IS NULL
            ORDER BY catalog.leaf_name
        ) AS absent_leaves
)
SELECT
    clock.checked_at,
    leaves.leaf_kind,
    leaves.class_key,
    leaves.parent_name,
    leaves.leaf_name,
    leaves.lower_bound_kind,
    leaves.lower_anchor,
    leaves.upper_anchor,
    leaves.conformant,
    registration_health.healthy AS registration_healthy,
    publication_health.healthy AS publication_healthy,
    publication_health.absent_leaves
FROM clock
CROSS JOIN registration_health
CROSS JOIN publication_health
JOIN leaf_health AS leaves ON TRUE
ORDER BY leaves.leaf_kind, leaves.class_key, leaves.lower_anchor NULLS FIRST
"""


def _expected_registrations(
    *,
    heartbeat_horizon_hours: int,
    declared_classes: Sequence[tuple[str, timedelta]],
) -> str:
    classes = [
        {
            'class_key': HEARTBEAT_CLASS_KEY,
            'class_kind': 'HEARTBEAT',
            'retention_days': None,
            'heartbeat_horizon_hours': heartbeat_horizon_hours,
        },
        {
            'class_key': DEFAULT_RETENTION_CLASS_KEY,
            'class_kind': 'PERIOD',
            'retention_days': DEFAULT_RETENTION_DURATION.days,
            'heartbeat_horizon_hours': None,
        },
    ]
    classes.extend(
        {
            'class_key': class_key,
            'class_kind': 'PERIOD',
            'retention_days': duration.days,
            'heartbeat_horizon_hours': None,
        }
        for class_key, duration in declared_classes
    )
    return json.dumps(classes, separators=(',', ':'), allow_nan=False)


async def probe_before(
    connection: AsyncConnection,
    *,
    history_horizon_days: int,
    heartbeat_horizon_hours: int,
    declared_classes: Sequence[tuple[str, timedelta]],
) -> coverage._CoverageProbe:  # pyright: ignore[reportPrivateUsage]
    """The probe: the session zone and the set-based statement."""
    await connection.execute(text("SET LOCAL TIME ZONE 'UTC'"))
    rows = (
        await connection.execute(
            text(COVERAGE_PROBE_SQL),
            {
                'registrations': _expected_registrations(
                    heartbeat_horizon_hours=heartbeat_horizon_hours,
                    declared_classes=declared_classes,
                ),
                'history_horizon': history_horizon_days,
                'heartbeat_horizon': heartbeat_horizon_hours,
                'index_schema_version': INDEX_SCHEMA_VERSION,
                'heartbeat_class': HEARTBEAT_CLASS_KEY,
                'heartbeat_parent': HEARTBEATS_TABLE,
                'forever_class': FOREVER_CLASS_KEY,
                'forever_parent': TASK_HISTORY_FOREVER,
                'forever_leaf': FOREVER_LEAF,
                'history_parent': TASK_HISTORY_PARENT,
                'open_end': OPEN_END_ANCHOR,
            },
        )
    ).all()
    return coverage._coverage_probe_of(rows)  # pyright: ignore[reportPrivateUsage]


async def register_before(
    connection: AsyncConnection,
    *,
    heartbeat_horizon_hours: int,
    declared_classes: Sequence[tuple[str, timedelta]],
) -> None:
    """The Python registration. A refusal outcome raises ``AssertionError``:
    the dual-run seeds none."""
    heartbeat = await register_heartbeat_class(
        connection, horizon=timedelta(hours=heartbeat_horizon_hours)
    )
    match heartbeat:
        case HeartbeatClassRegistered() | HeartbeatClassVerified() | HeartbeatHorizonUpdated():
            pass
        case refused:
            raise AssertionError(f'heartbeat class registration refused: {refused!r}')
    for class_key, duration in (
        (DEFAULT_RETENTION_CLASS_KEY, DEFAULT_RETENTION_DURATION),
        *declared_classes,
    ):
        match await register_finite_retention_class(
            connection, class_key=class_key, duration=duration
        ):
            case ClassRegistered() | ClassAlreadyRegistered():
                pass
            case refused:
                raise AssertionError(f'class {class_key} registration refused: {refused!r}')


async def _create_history_leaf(
    connection: AsyncConnection, leaf: LeafRef | ForeverLeafRef
) -> LeafCreation:
    match leaf:
        case ForeverLeafRef():
            return await verify_forever_leaf(connection)
        case LeafRef():
            return await create_daily_leaf(
                connection, CreateDailyHistoryLeaf(leaf=leaf), UnpublishedLoader()
            )


async def maintain_history_leaf_before(
    database: PartitionMaintenanceDatabase, leaf: LeafRef | ForeverLeafRef
) -> LeafCreation:
    outcome: LeafCreation = LeafMaintenanceBusy(leaf_name=leaf.leaf_name)
    for attempt in range(_LEAF_MAINTENANCE_ATTEMPTS):
        async with database.begin() as connection:
            outcome = await _create_history_leaf(connection, leaf)
        match outcome:
            case LeafMaintenanceBusy() if attempt + 1 < _LEAF_MAINTENANCE_ATTEMPTS:
                await asyncio.sleep(random.uniform(_BUSY_RETRY_MIN_SECONDS, _BUSY_RETRY_MAX_SECONDS))
            case _:
                return outcome
    return outcome


async def maintain_heartbeat_leaf_before(
    database: PartitionMaintenanceDatabase, leaf: LeafRef
) -> LeafCreation:
    outcome: LeafCreation = LeafMaintenanceBusy(leaf_name=leaf.leaf_name)
    for attempt in range(_LEAF_MAINTENANCE_ATTEMPTS):
        async with database.begin() as connection:
            outcome = await create_hourly_heartbeat_leaf(
                connection, CreateHourlyHeartbeatLeaf(leaf=leaf)
            )
        match outcome:
            case LeafMaintenanceBusy() if attempt + 1 < _LEAF_MAINTENANCE_ATTEMPTS:
                await asyncio.sleep(random.uniform(_BUSY_RETRY_MIN_SECONDS, _BUSY_RETRY_MAX_SECONDS))
            case _:
                return outcome
    return outcome


_STEPS_BEFORE: Final = {
    '_probe_complete_coverage': probe_before,
    '_register_required_classes': register_before,
    '_maintain_history_leaf': maintain_history_leaf_before,
    '_maintain_heartbeat_leaf': maintain_heartbeat_leaf_before,
}


@contextmanager
def statements_before() -> Generator[None]:
    """Run ``maintain_partition_coverage`` with the four steps before the switch."""
    switched = {name: getattr(coverage, name) for name in _STEPS_BEFORE}
    for name, step in _STEPS_BEFORE.items():
        setattr(coverage, name, step)
    try:
        yield
    finally:
        for name, step in switched.items():
            setattr(coverage, name, step)
