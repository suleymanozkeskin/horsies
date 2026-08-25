"""The coverage/publication maintenance owner.

Partitioned writes need their partitions before the write arrives:
heartbeat leaves for the worker's own liveness rows, history leaves for
every terminalization, and the staged readers published over whatever
leaves exist. Nothing else owns that sequence — the offline cutover
ensures it once for upgraded installs and then hands off — so workers
own it here: once at startup (before the first claim) and periodically
under the reaper's cluster-wide gate.

Every step is idempotent. Republication runs only when a leaf was
actually created, a staged reader is absent, or the published readers
probe a relation that no longer exists; a healthy fleet's maintenance
tick performs reads only.

The fatal line is a measured fact, not a birth-state guess: after the
ensure attempt, heartbeat coverage for the present instant either
exists or it does not. Absent coverage guarantees the next heartbeat
write fails, so a worker refuses to start against it; an established
fleet with live coverage degrades to a typed outcome on the same
predicate when a later ensure fails.
"""

from __future__ import annotations

import asyncio
import json
import random
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection

from ..commands import EnsureLeafCoverage
from ..ddl.classes import (
    ClassAlreadyRegistered,
    ClassRegistered,
    DEFAULT_RETENTION_CLASS_KEY,
    DEFAULT_RETENTION_DURATION,
    finite_class_parent_name,
    register_finite_retention_class,
)
from ..ddl.tables import FOREVER_CLASS_KEY
from ..heartbeats.partitioning import (
    EnsureHeartbeatCoverage,
    HeartbeatClassRegistered,
    HeartbeatClassVerified,
    HeartbeatHorizonUpdated,
    ensure_heartbeat_coverage,
    hourly_leaf_ref,
    create_hourly_heartbeat_leaf,
    CreateHourlyHeartbeatLeaf,
    register_heartbeat_class,
)
from ..names import (
    HEARTBEAT_CLASS_KEY,
    HEARTBEATS_TABLE,
    LEAF_CATALOG,
    RETENTION_CLASSES,
    TASK_DETAIL_FUNCTION,
    TASK_HISTORY_FOREVER,
    TASK_LOOKUP_MANIFEST,
)
from ..outcomes import (
    LeafAlreadyConformant,
    LeafCreated,
    LeafCreation,
    LeafIndexRepaired,
    LeafMaintenanceBusy,
)
from ..partitions.catalog import INDEX_SCHEMA_VERSION, database_now
from ..partitions.manager import create_daily_leaf, ensure_leaf_coverage
from ..commands import CreateDailyHistoryLeaf, LeafBounds, LeafRef
from ..reads.detail import staged_detail_published
from ..reads.publisher import (
    StagedLoaderPublisher,
    published_manifest_absent_leaves,
)
from .database import PartitionMaintenanceDatabase


_LEAF_MAINTENANCE_ATTEMPTS = 3
_BUSY_RETRY_MIN_SECONDS = 0.02
_BUSY_RETRY_MAX_SECONDS = 0.08

_COVERAGE_PROBE_SQL = f"""
WITH clock AS (
    SELECT statement_timestamp() AS checked_at
),
expected_registration AS (
    SELECT *
    FROM jsonb_to_recordset(CAST(:registrations AS jsonb)) AS expected(
        class_key text,
        duration_microseconds bigint,
        interval_microseconds bigint,
        parent_name text
    )
),
registration_health AS (
    SELECT
        NOT EXISTS (
            SELECT 1
            FROM expected_registration AS expected
            LEFT JOIN {RETENTION_CLASSES} AS registered
              ON registered.class_key = expected.class_key
            WHERE registered.class_key IS NULL
               OR (extract(epoch FROM registered.duration) * 1000000)::bigint
                    IS DISTINCT FROM expected.duration_microseconds
               OR (extract(epoch FROM registered.partition_interval)
                    * 1000000)::bigint
                    IS DISTINCT FROM expected.interval_microseconds
               OR registered.finite_parent_name
                    IS DISTINCT FROM expected.parent_name
        )
        AND NOT EXISTS (
            SELECT 1
            FROM {RETENTION_CLASSES} AS registered
            WHERE registered.class_key <> :heartbeat_class
              AND NOT (
                    (
                        registered.class_key = :forever_class
                        AND registered.duration IS NULL
                        AND registered.partition_interval IS NULL
                        AND registered.finite_parent_name IS NULL
                    )
                    OR (
                        registered.class_key <> :forever_class
                        AND registered.duration IS NOT NULL
                        AND registered.duration > interval '0 seconds'
                        AND registered.partition_interval IS NOT NULL
                        AND registered.partition_interval = interval '1 day'
                        AND registered.finite_parent_name IS NOT NULL
                    )
              )
        )
        AND EXISTS (
            SELECT 1
            FROM {RETENTION_CLASSES} AS registered
            WHERE registered.class_key = :forever_class
              AND registered.duration IS NULL
              AND registered.partition_interval IS NULL
              AND registered.finite_parent_name IS NULL
        ) AS healthy
),
history_classes AS (
    SELECT
        registered.class_key,
        CASE
            WHEN registered.class_key = :forever_class
                THEN :forever_parent
            ELSE registered.finite_parent_name
        END AS parent_name
    FROM {RETENTION_CLASSES} AS registered
    WHERE registered.class_key <> :heartbeat_class
      AND (
            registered.duration IS NOT NULL
            OR registered.class_key = :forever_class
      )
),
expected_history AS (
    SELECT
        'history'::text AS leaf_kind,
        classes.class_key,
        classes.parent_name,
        classes.parent_name || '_' ||
            to_char(day_bounds.lower_anchor AT TIME ZONE 'UTC', 'YYYY_MM_DD')
            AS leaf_name,
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
    SELECT * FROM expected_heartbeats
),
leaf_health AS (
    SELECT
        expected.*,
        catalog.id_index_name,
        child.oid AS child_oid,
        (
            catalog.leaf_name IS NOT NULL
            AND catalog.parent_name = expected.parent_name
            AND catalog.class_key = expected.class_key
            AND catalog.lower_anchor = expected.lower_anchor
            AND catalog.upper_anchor = expected.upper_anchor
            AND catalog.index_schema_version = :index_schema_version
            AND catalog.dropped_at IS NULL
            AND child.oid IS NOT NULL
            AND parent.oid IS NOT NULL
            AND inheritance.inhrelid IS NOT NULL
            AND NOT inheritance.inhdetachpending
            AND pg_get_expr(child.relpartbound, child.oid, true)
                = catalog.partition_bound
            AND EXISTS (
                SELECT 1
                FROM pg_index AS task_index
                WHERE task_index.indexrelid = to_regclass(catalog.id_index_name)
                  AND task_index.indrelid = child.oid
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
                      AND ordering_index.indpred IS NULL
                      AND ordering_index.indnkeyatts = 1
                      AND ordering_attribute.attname = 'enqueued_at'
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
attached_history AS (
    SELECT catalog.leaf_name
    FROM {LEAF_CATALOG} AS catalog
    WHERE catalog.class_key <> :heartbeat_class
      AND catalog.detached_at IS NULL
      AND catalog.dropped_at IS NULL
      AND to_regclass(catalog.leaf_name) IS NOT NULL
),
publication_health AS (
    SELECT
        to_regprocedure(:detail_function) IS NOT NULL
        AND NOT EXISTS (
            SELECT 1
            FROM {TASK_LOOKUP_MANIFEST} AS manifest
            WHERE to_regclass(manifest.leaf_name) IS NULL
        )
        AND NOT EXISTS (
            SELECT leaf_name FROM attached_history
            EXCEPT
            SELECT leaf_name FROM {TASK_LOOKUP_MANIFEST}
        )
        AND NOT EXISTS (
            SELECT leaf_name FROM {TASK_LOOKUP_MANIFEST}
            EXCEPT
            SELECT leaf_name FROM attached_history
        ) AS healthy,
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
ORDER BY leaves.leaf_kind, leaves.class_key, leaves.lower_anchor
"""


@dataclass(frozen=True, slots=True)
class CoverageEnsured:
    """One completed ensure pass, what it changed, and how far
    coverage now reaches — the operator's answer to "how long do
    existing leaves absorb writes if maintenance starts failing".

    `absent_leaves` names catalog rows still called attached whose
    relation no longer exists. Non-empty means someone dropped a leaf
    behind the manager's back: the readers have been regenerated without
    it so reads keep working, and the rows it held are gone. It is
    reported rather than repaired — the catalog keeps the evidence, and
    only an operator can decide what an unexpected drop means."""

    created_history_leaves: int
    created_heartbeat_leaves: int
    republished: bool
    heartbeat_covered_now: bool
    history_covered_through: datetime
    heartbeats_covered_through: datetime
    absent_leaves: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class CoverageEnsureFailed:
    """The pass stopped at a refusal; the fields name where and why.

    `absent_leaves` carries the same report as on the success outcome:
    a vanished leaf must not be swallowed because some unrelated class
    also failed in the same pass. Empty means none were OBSERVED by this
    pass — the stages that run before republication stop early and never
    reach the check, and they say so through `stage`."""

    stage: str
    class_key: str | None
    refusal: str
    heartbeat_covered_now: bool
    absent_leaves: tuple[str, ...]


type CoverageOutcome = CoverageEnsured | CoverageEnsureFailed


@dataclass(frozen=True, slots=True)
class _CoverageProbe:
    checked_at: datetime
    damaged_history: tuple[LeafRef, ...]
    damaged_heartbeats: tuple[LeafRef, ...]
    registration_healthy: bool
    publication_healthy: bool
    heartbeat_covered_now: bool
    history_covered_through: datetime
    heartbeats_covered_through: datetime
    absent_leaves: tuple[str, ...]

    @property
    def healthy(self) -> bool:
        return bool(
            self.registration_healthy
            and self.publication_healthy
            and not self.damaged_history
            and not self.damaged_heartbeats
        )


def _expected_registrations(
    *,
    heartbeat_horizon_hours: int,
    declared_classes: Sequence[tuple[str, timedelta]],
) -> str:
    classes = [
        {
            'class_key': HEARTBEAT_CLASS_KEY,
            'duration_microseconds': heartbeat_horizon_hours * 3_600_000_000,
            'interval_microseconds': 3_600_000_000,
            'parent_name': HEARTBEATS_TABLE,
        },
        {
            'class_key': DEFAULT_RETENTION_CLASS_KEY,
            'duration_microseconds': _timedelta_microseconds(
                DEFAULT_RETENTION_DURATION
            ),
            'interval_microseconds': 86_400_000_000,
            'parent_name': finite_class_parent_name(DEFAULT_RETENTION_CLASS_KEY),
        },
    ]
    classes.extend(
        {
            'class_key': class_key,
            'duration_microseconds': _timedelta_microseconds(duration),
            'interval_microseconds': 86_400_000_000,
            'parent_name': finite_class_parent_name(class_key),
        }
        for class_key, duration in declared_classes
    )
    return json.dumps(classes, separators=(',', ':'))


def _timedelta_microseconds(value: timedelta) -> int:
    return (
        (value.days * 86_400 + value.seconds) * 1_000_000
        + value.microseconds
    )


async def _probe_complete_coverage(
    connection: AsyncConnection,
    *,
    history_horizon_days: int,
    heartbeat_horizon_hours: int,
    declared_classes: Sequence[tuple[str, timedelta]],
) -> _CoverageProbe:
    await connection.execute(text("SET LOCAL TIME ZONE 'UTC'"))
    rows = (
        await connection.execute(
            text(_COVERAGE_PROBE_SQL),
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
                'detail_function': f'{TASK_DETAIL_FUNCTION}(uuid)',
            },
        )
    ).all()
    if not rows:
        raise AssertionError('coverage probe returned no expected leaves')
    damaged_history: list[LeafRef] = []
    damaged_heartbeats: list[LeafRef] = []
    heartbeat_covered_now = False
    history_covered_through: datetime | None = None
    heartbeats_covered_through: datetime | None = None
    for row in rows:
        leaf = LeafRef(
            leaf_name=str(row.leaf_name),
            class_key=str(row.class_key),
            bounds=LeafBounds(
                lower=row.lower_anchor,
                upper=row.upper_anchor,
            ),
        )
        match str(row.leaf_kind), bool(row.conformant):
            case 'history', False:
                damaged_history.append(leaf)
                history_covered_through = max(
                    history_covered_through or row.upper_anchor,
                    row.upper_anchor,
                )
            case 'heartbeat', False:
                damaged_heartbeats.append(leaf)
                heartbeats_covered_through = max(
                    heartbeats_covered_through or row.upper_anchor,
                    row.upper_anchor,
                )
            case 'heartbeat', True:
                heartbeats_covered_through = max(
                    heartbeats_covered_through or row.upper_anchor,
                    row.upper_anchor,
                )
                if row.lower_anchor <= row.checked_at < row.upper_anchor:
                    heartbeat_covered_now = True
            case 'history', True:
                history_covered_through = max(
                    history_covered_through or row.upper_anchor,
                    row.upper_anchor,
                )
            case unknown, _:
                raise AssertionError(f'unknown coverage leaf kind {unknown!r}')
    first = rows[0]
    if history_covered_through is None or heartbeats_covered_through is None:
        raise AssertionError('coverage probe did not return both leaf types')
    absent = tuple(str(name) for name in first.absent_leaves)
    return _CoverageProbe(
        checked_at=first.checked_at,
        damaged_history=tuple(damaged_history),
        damaged_heartbeats=tuple(damaged_heartbeats),
        registration_healthy=bool(first.registration_healthy),
        publication_healthy=bool(first.publication_healthy),
        heartbeat_covered_now=heartbeat_covered_now,
        history_covered_through=history_covered_through,
        heartbeats_covered_through=heartbeats_covered_through,
        absent_leaves=absent,
    )


async def heartbeat_coverage_present(connection: AsyncConnection) -> bool:
    """Whether a heartbeat leaf covers the present instant.

    The fatal predicate for worker startup: this probe reporting False
    after an ensure attempt guarantees the next heartbeat write fails.
    """
    now = await database_now(connection)
    hour_lower = now.replace(minute=0, second=0, microsecond=0)
    leaf = hourly_leaf_ref(hour_lower)
    present = (
        await connection.execute(
            text('SELECT to_regclass(:leaf) IS NOT NULL'),
            {'leaf': leaf.leaf_name},
        )
    ).scalar_one()
    return bool(present)


async def _history_class_keys(connection: AsyncConnection) -> list[str]:
    rows = (
        await connection.execute(
            text(
                f"""
                SELECT class_key FROM {RETENTION_CLASSES}
                WHERE (duration IS NOT NULL OR class_key = :forever_class)
                  AND class_key <> :heartbeat_class
                ORDER BY class_key
                """
            ),
            {
                'heartbeat_class': HEARTBEAT_CLASS_KEY,
                'forever_class': FOREVER_CLASS_KEY,
            },
        )
    ).all()
    return [row.class_key for row in rows]


async def ensure_partition_coverage(
    connection: AsyncConnection,
    *,
    history_horizon_days: int,
    heartbeat_horizon_hours: int,
    declared_classes: Sequence[tuple[str, timedelta]] = (),
) -> CoverageOutcome:
    """Register classes, cover leaves, publish readers — one pass.

    The caller owns the transaction.

    A refusal is reported, never retried into silence — that is the
    failure mode this owner exists to prevent. What changed is the
    SCOPE of the stop. A per-class failure no longer ends the pass: the
    class is named, the remaining classes are served, heartbeat coverage
    runs regardless, and the pass reports every class that failed. A
    hole in one class's coverage is not a reason to open one in the
    rest, and heartbeat leaves gate worker startup, so stopping early
    turned one poisoned class row into a fleet that cannot restart.

    Failures at the steps BEFORE the per-class loop — heartbeat class
    registration, default class registration, declared registration —
    still stop the pass, because nothing after them can be correct.
    """
    heartbeat_registration = await register_heartbeat_class(
        connection, horizon=timedelta(hours=heartbeat_horizon_hours)
    )
    match heartbeat_registration:
        case (
            HeartbeatClassRegistered()
            | HeartbeatClassVerified()
            | HeartbeatHorizonUpdated()
        ):
            # A horizon update is a success: the UPDATE has already
            # run and the class carries the configured duration, so the
            # pass must proceed to leaf creation like any other success.
            pass
        case _:
            return CoverageEnsureFailed(
                stage='register_heartbeat_class',
                class_key=HEARTBEAT_CLASS_KEY,
                refusal=repr(heartbeat_registration),
                heartbeat_covered_now=await heartbeat_coverage_present(connection),
                absent_leaves=(),
            )

    default_registration = await register_finite_retention_class(
        connection,
        class_key=DEFAULT_RETENTION_CLASS_KEY,
        duration=DEFAULT_RETENTION_DURATION,
    )
    match default_registration:
        case ClassRegistered() | ClassAlreadyRegistered():
            pass
        case _:
            return CoverageEnsureFailed(
                stage='register_default_class',
                class_key=DEFAULT_RETENTION_CLASS_KEY,
                refusal=repr(default_registration),
                heartbeat_covered_now=await heartbeat_coverage_present(connection),
                absent_leaves=(),
            )

    for declared_key, declared_duration in declared_classes:
        declared_registration = await register_finite_retention_class(
            connection,
            class_key=declared_key,
            duration=declared_duration,
        )
        match declared_registration:
            case ClassRegistered() | ClassAlreadyRegistered():
                pass
            case _:
                # ClassConflict lands here: classes are immutable, so a
                # redeclaration with a different duration is refused and
                # named rather than silently ignored or applied.
                return CoverageEnsureFailed(
                    stage='register_declared_class',
                    class_key=declared_key,
                    refusal=repr(declared_registration),
                    heartbeat_covered_now=await heartbeat_coverage_present(connection),
                    absent_leaves=(),
                )

    publisher = StagedLoaderPublisher()
    created_history = 0
    # One class must not cost the others their coverage, and "the others"
    # includes heartbeats. Classes are served in key order, so an
    # unbounded failure denied leaves to every class sorting after it --
    # and every queue-derived key begins `q_`, which sorts before
    # `standard_30d`, so the feature guarantees the adverse ordering
    # rather than leaving it to chance.
    #
    # Each class runs inside a SAVEPOINT. A Python-level error needs only
    # the try/except, but a DATABASE error aborts the enclosing
    # transaction the caller owns, and every later statement -- including
    # the health probe this function ends on -- would then fail with
    # InFailedSqlTransaction and raise out of the pass entirely. Rolling
    # back to the savepoint leaves the caller's transaction usable, so
    # containment survives the failure kind that most needs it.
    #
    # Refusals are RETURNED rather than raised, so they are collected
    # here too. Both kinds name their class; neither stops the loop.
    failures: list[str] = []
    for class_key in await _history_class_keys(connection):
        try:
            # The savepoint contains DATABASE ERRORS -- the
            # transaction-aborting kind that would raise the pass out
            # from under its caller -- and NEVER OUTCOMES. One aborted
            # transaction fails every later statement, including the
            # health probe this function ends on; rolling back to the
            # savepoint leaves the caller's transaction usable, so
            # containment survives the failure kind that most needs it.
            async with connection.begin_nested():
                creations = await ensure_leaf_coverage(
                    connection,
                    EnsureLeafCoverage(
                        class_key=class_key, horizon_days=history_horizon_days
                    ),
                    publisher,
                )
        except Exception as class_error:
            failures.append(f'{class_key}: {class_error!r}')
            continue

        # A RETURNED refusal is a clean outcome: the connection is fine
        # and whatever leaves were created before it are real. They are
        # kept deliberately. Leaf coverage stops at its first refusal, so
        # discarding the leaves it made first would mean a permanently
        # refusing leaf blocked every earlier day of that class forever,
        # re-doing and re-discarding the same work on every pass.
        created_history += sum(
            1 for creation in creations if isinstance(creation, LeafCreated)
        )
        refusals = [
            creation
            for creation in creations
            if not isinstance(
                creation,
                (LeafCreated, LeafAlreadyConformant, LeafIndexRepaired),
            )
        ]
        if refusals:
            failures.append(
                f'{class_key}: ' + '; '.join(repr(item) for item in refusals)
            )

    created_heartbeats = 0
    heartbeat_creations = await ensure_heartbeat_coverage(
        connection,
        EnsureHeartbeatCoverage(horizon_hours=heartbeat_horizon_hours),
    )
    for creation in heartbeat_creations:
        match creation:
            case LeafCreated():
                created_heartbeats += 1
            case LeafAlreadyConformant() | LeafIndexRepaired():
                continue
            case _:
                return CoverageEnsureFailed(
                    stage='ensure_heartbeat_coverage',
                    class_key=HEARTBEAT_CLASS_KEY,
                    refusal=repr(creation),
                    heartbeat_covered_now=(
                        await heartbeat_coverage_present(connection)
                    ),
                    absent_leaves=(),
                )

    # One probe stands in for all three staged readers because
    # republication is atomic across the triple; the dependency is
    # pinned by a test beside this logic.
    #
    # The third trigger is a leaf that vanished behind the manager's
    # back. It creates nothing and publishes nothing, so neither of the
    # other two conditions fires, and the published readers keep naming
    # a relation that is gone -- which kills every finalize fleet-wide
    # until something forces a regeneration. Republication is now
    # self-correcting (the probe list requires the relation to exist),
    # so this converges in one pass rather than rebuilding the same
    # broken function.
    republished = False
    absent_leaves: tuple[str, ...] = ()
    if (
        created_history > 0
        or await published_manifest_absent_leaves(connection)
        or not await staged_detail_published(connection)
    ):
        republication = await publisher.republish(connection)
        republished = True
        absent_leaves = republication.absent_leaves

    if failures:
        # Reported only after heartbeat coverage and republication have
        # run: those serve every class, and a failure in one class is no
        # reason to withhold them. Every failed class is named, so the
        # health surface carries the whole picture rather than whichever
        # key sorted first.
        return CoverageEnsureFailed(
            stage='ensure_leaf_coverage',
            class_key=failures[0].split(':', 1)[0],
            refusal=(f'{len(failures)} class(es) failed: ' + '; '.join(failures)),
            heartbeat_covered_now=(await heartbeat_coverage_present(connection)),
            absent_leaves=absent_leaves,
        )

    now = await database_now(connection)
    day_lower = now.replace(hour=0, minute=0, second=0, microsecond=0)
    hour_lower = now.replace(minute=0, second=0, microsecond=0)
    return CoverageEnsured(
        created_history_leaves=created_history,
        created_heartbeat_leaves=created_heartbeats,
        republished=republished,
        heartbeat_covered_now=await heartbeat_coverage_present(connection),
        history_covered_through=(day_lower + timedelta(days=history_horizon_days + 1)),
        heartbeats_covered_through=(
            hour_lower + timedelta(hours=heartbeat_horizon_hours + 1)
        ),
        absent_leaves=absent_leaves,
    )


async def maintain_partition_coverage(
    database: PartitionMaintenanceDatabase,
    *,
    history_horizon_days: int,
    heartbeat_horizon_hours: int,
    declared_classes: Sequence[tuple[str, timedelta]] = (),
) -> CoverageOutcome:
    """Ensure the complete set through the maintenance database.

    A healthy set uses one set-based probe. A damaged set opens one
    transaction for each changed leaf. A busy lock ends that transaction
    before the retry delay starts.
    """
    async with database.begin() as connection:
        probe = await _probe_complete_coverage(
            connection,
            history_horizon_days=history_horizon_days,
            heartbeat_horizon_hours=heartbeat_horizon_hours,
            declared_classes=declared_classes,
        )
    if probe.healthy:
        return _coverage_ensured_from_probe(probe)

    if not probe.registration_healthy:
        async with database.begin() as connection:
            registration_failure = await _register_required_classes(
                connection,
                heartbeat_horizon_hours=heartbeat_horizon_hours,
                declared_classes=declared_classes,
                heartbeat_covered_now=probe.heartbeat_covered_now,
            )
        if registration_failure is not None:
            return registration_failure
        async with database.begin() as connection:
            probe = await _probe_complete_coverage(
                connection,
                history_horizon_days=history_horizon_days,
                heartbeat_horizon_hours=heartbeat_horizon_hours,
                declared_classes=declared_classes,
            )
        if not probe.registration_healthy:
            return CoverageEnsureFailed(
                stage='verify_registration',
                class_key=None,
                refusal='registered retention class shape is not conformant',
                heartbeat_covered_now=probe.heartbeat_covered_now,
                absent_leaves=probe.absent_leaves,
            )

    publisher = StagedLoaderPublisher()
    created_history = 0
    created_heartbeats = 0
    failures: list[str] = []
    failed_history_classes: set[str] = set()
    for leaf in probe.damaged_history:
        if leaf.class_key in failed_history_classes:
            continue
        try:
            outcome = await _maintain_history_leaf(database, leaf, publisher)
        except Exception as leaf_error:
            failures.append(f'{leaf.class_key}: {leaf_error!r}')
            failed_history_classes.add(leaf.class_key)
            continue
        match outcome:
            case LeafCreated():
                created_history += 1
            case LeafAlreadyConformant() | LeafIndexRepaired():
                pass
            case _:
                failures.append(f'{leaf.class_key}: {outcome!r}')
                failed_history_classes.add(leaf.class_key)

    for leaf in probe.damaged_heartbeats:
        try:
            outcome = await _maintain_heartbeat_leaf(database, leaf)
        except Exception as leaf_error:
            failures.append(f'{HEARTBEAT_CLASS_KEY}: {leaf_error!r}')
            break
        match outcome:
            case LeafCreated():
                created_heartbeats += 1
            case LeafAlreadyConformant() | LeafIndexRepaired():
                pass
            case _:
                failures.append(f'{HEARTBEAT_CLASS_KEY}: {outcome!r}')
                break

    republished = False
    async with database.begin() as connection:
        final_probe = await _probe_complete_coverage(
            connection,
            history_horizon_days=history_horizon_days,
            heartbeat_horizon_hours=heartbeat_horizon_hours,
            declared_classes=declared_classes,
        )
        if not final_probe.publication_healthy:
            await publisher.republish(connection)
            republished = True
            final_probe = await _probe_complete_coverage(
                connection,
                history_horizon_days=history_horizon_days,
                heartbeat_horizon_hours=heartbeat_horizon_hours,
                declared_classes=declared_classes,
            )

    if final_probe.healthy:
        ensured = _coverage_ensured_from_probe(final_probe)
        return CoverageEnsured(
            created_history_leaves=created_history,
            created_heartbeat_leaves=created_heartbeats,
            republished=republished,
            heartbeat_covered_now=ensured.heartbeat_covered_now,
            history_covered_through=ensured.history_covered_through,
            heartbeats_covered_through=ensured.heartbeats_covered_through,
            absent_leaves=ensured.absent_leaves,
        )

    unresolved = [
        *(leaf.leaf_name for leaf in final_probe.damaged_history),
        *(leaf.leaf_name for leaf in final_probe.damaged_heartbeats),
    ]
    detail = failures or ['unresolved coverage leaves: ' + ', '.join(unresolved)]
    return CoverageEnsureFailed(
        stage='ensure_leaf_coverage',
        class_key=(
            final_probe.damaged_history[0].class_key
            if final_probe.damaged_history
            else HEARTBEAT_CLASS_KEY
        ),
        refusal='; '.join(detail),
        heartbeat_covered_now=final_probe.heartbeat_covered_now,
        absent_leaves=final_probe.absent_leaves,
    )


async def _register_required_classes(
    connection: AsyncConnection,
    *,
    heartbeat_horizon_hours: int,
    declared_classes: Sequence[tuple[str, timedelta]],
    heartbeat_covered_now: bool,
) -> CoverageEnsureFailed | None:
    heartbeat_registration = await register_heartbeat_class(
        connection, horizon=timedelta(hours=heartbeat_horizon_hours)
    )
    match heartbeat_registration:
        case (
            HeartbeatClassRegistered()
            | HeartbeatClassVerified()
            | HeartbeatHorizonUpdated()
        ):
            pass
        case _:
            return CoverageEnsureFailed(
                stage='register_heartbeat_class',
                class_key=HEARTBEAT_CLASS_KEY,
                refusal=repr(heartbeat_registration),
                heartbeat_covered_now=heartbeat_covered_now,
                absent_leaves=(),
            )

    registrations = (
        (DEFAULT_RETENTION_CLASS_KEY, DEFAULT_RETENTION_DURATION),
        *declared_classes,
    )
    for class_key, duration in registrations:
        registration = await register_finite_retention_class(
            connection,
            class_key=class_key,
            duration=duration,
        )
        match registration:
            case ClassRegistered() | ClassAlreadyRegistered():
                pass
            case _:
                return CoverageEnsureFailed(
                    stage=(
                        'register_default_class'
                        if class_key == DEFAULT_RETENTION_CLASS_KEY
                        else 'register_declared_class'
                    ),
                    class_key=class_key,
                    refusal=repr(registration),
                    heartbeat_covered_now=heartbeat_covered_now,
                    absent_leaves=(),
                )
    return None


async def _maintain_history_leaf(
    database: PartitionMaintenanceDatabase,
    leaf: LeafRef,
    publisher: StagedLoaderPublisher,
) -> LeafCreation:
    outcome = LeafMaintenanceBusy(leaf_name=leaf.leaf_name)
    for attempt in range(_LEAF_MAINTENANCE_ATTEMPTS):
        async with database.begin() as connection:
            outcome = await create_daily_leaf(
                connection,
                CreateDailyHistoryLeaf(leaf=leaf),
                publisher,
            )
        match outcome:
            case LeafMaintenanceBusy() if (attempt + 1 < _LEAF_MAINTENANCE_ATTEMPTS):
                await asyncio.sleep(
                    random.uniform(
                        _BUSY_RETRY_MIN_SECONDS,
                        _BUSY_RETRY_MAX_SECONDS,
                    )
                )
            case _:
                return outcome
    return outcome


async def _maintain_heartbeat_leaf(
    database: PartitionMaintenanceDatabase,
    leaf: LeafRef,
) -> LeafCreation:
    outcome = LeafMaintenanceBusy(leaf_name=leaf.leaf_name)
    for attempt in range(_LEAF_MAINTENANCE_ATTEMPTS):
        async with database.begin() as connection:
            outcome = await create_hourly_heartbeat_leaf(
                connection,
                CreateHourlyHeartbeatLeaf(leaf=leaf),
            )
        match outcome:
            case LeafMaintenanceBusy() if (attempt + 1 < _LEAF_MAINTENANCE_ATTEMPTS):
                await asyncio.sleep(
                    random.uniform(
                        _BUSY_RETRY_MIN_SECONDS,
                        _BUSY_RETRY_MAX_SECONDS,
                    )
                )
            case _:
                return outcome
    return outcome


def _coverage_ensured_from_probe(probe: _CoverageProbe) -> CoverageEnsured:
    return CoverageEnsured(
        created_history_leaves=0,
        created_heartbeat_leaves=0,
        republished=False,
        heartbeat_covered_now=probe.heartbeat_covered_now,
        history_covered_through=probe.history_covered_through,
        heartbeats_covered_through=probe.heartbeats_covered_through,
        absent_leaves=probe.absent_leaves,
    )


@dataclass(frozen=True, slots=True)
class StartupCoverageRefused:
    """The worker must not start: no heartbeat leaf covers now.

    Raised as a hard startup error by the worker — starting anyway guarantees
    the first RUNNING transition fails on a missing heartbeat partition.
    """

    outcome: CoverageOutcome


async def ensure_startup_coverage(
    connection: AsyncConnection,
    *,
    history_horizon_days: int,
    heartbeat_horizon_hours: int,
    declared_classes: Sequence[tuple[str, timedelta]] = (),
) -> CoverageOutcome | StartupCoverageRefused:
    """The worker-startup ensure: fatal exactly when now is uncovered.

    A ``CoverageEnsureFailed`` report can describe one contained history
    class after heartbeat coverage and reader republication succeeded. That
    report remains non-fatal so unaffected classes retain partial availability.
    """
    outcome = await ensure_partition_coverage(
        connection,
        history_horizon_days=history_horizon_days,
        heartbeat_horizon_hours=heartbeat_horizon_hours,
        declared_classes=declared_classes,
    )
    if not outcome.heartbeat_covered_now:
        return StartupCoverageRefused(outcome=outcome)
    return outcome


async def ensure_startup_coverage_in_database(
    database: PartitionMaintenanceDatabase,
    *,
    history_horizon_days: int,
    heartbeat_horizon_hours: int,
    declared_classes: Sequence[tuple[str, timedelta]] = (),
) -> CoverageOutcome | StartupCoverageRefused:
    """Run startup coverage on the session-capable database path."""
    outcome = await maintain_partition_coverage(
        database,
        history_horizon_days=history_horizon_days,
        heartbeat_horizon_hours=heartbeat_horizon_hours,
        declared_classes=declared_classes,
    )
    if not outcome.heartbeat_covered_now:
        return StartupCoverageRefused(outcome=outcome)
    return outcome
