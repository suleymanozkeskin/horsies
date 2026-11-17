"""The coverage/publication maintenance owner.

Partitioned writes need their partitions before the write arrives:
heartbeat leaves for the worker's own liveness rows, history leaves for
every terminalization, and the staged readers published over whatever
leaves exist. Nothing else owns that sequence — the offline cutover
ensures it once for upgraded installs and then hands off — so workers
own it here: once at startup (before the first claim) and periodically
under the reaper's cluster-wide gate.

Every step is idempotent. Republication runs only when the published
manifest differs from the attached catalog or a staged reader is absent.
A healthy fleet's maintenance tick performs reads only.

The fatal line is a measured fact, not a birth-state guess: after the
ensure attempt, heartbeat coverage for the present instant either
exists or it does not. Absent coverage guarantees the next heartbeat
write fails, so a worker refuses to start against it; an established
fleet with live coverage degrades to a typed outcome on the same
predicate when a later ensure fails.
"""

from __future__ import annotations

import asyncio
import random
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Final, assert_never

from sqlalchemy import text
from sqlalchemy.engine import Row
from sqlalchemy.ext.asyncio import AsyncConnection

from horsies._core import (
    CoreHandle,
    CoverageEnsuredRecord,
    CoverageEnsureFailedRecord,
    CoverageFailed,
    CoverageRequest,
    CoveredForeverRecord,
    CoveredUntilRecord,
)
from horsies.core.types.core_failure import CoreOperationFailed
from horsies.core.types.result import Err, Ok

from ..commands import (
    EnsureLeafCoverage,
    ForeverLeafRef,
    LeafBounds,
    LeafRef,
)
from ..ddl.classes import (
    ClassAlreadyRegistered,
    ClassRegistered,
    DEFAULT_RETENTION_CLASS_KEY,
    DEFAULT_RETENTION_DURATION,
    register_finite_retention_class,
)
from ..errors import HistoryContractError
from ..heartbeats.partitioning import (
    EnsureHeartbeatCoverage,
    HeartbeatClassRegistered,
    HeartbeatClassVerified,
    HeartbeatHorizonUpdated,
    ensure_heartbeat_coverage,
    hourly_leaf_ref,
    register_heartbeat_class,
)
from ..names import (
    FOREVER_LEAF,
    HEARTBEAT_CLASS_KEY,
    RETENTION_CLASSES,
)
from ..outcomes import (
    CatalogConflictKind,
    CoveredByOpenEndedLeaf,
    LeafAlreadyConformant,
    LeafCatalogConflict,
    LeafCreated,
    LeafCreation,
    LeafIndexRepaired,
    LeafMaintenanceBusy,
    RetentionClassAbsent,
)
from ..partitions.catalog import (
    database_now,
    heartbeat_probe_index_name,
    leaf_id_index_name,
    required_utc_timestamp,
)
from ..partitions.manager import ensure_leaf_coverage
from ..partitions.locks import (
    LeafLockAttempt,
    try_lock_coverage_for_transaction,
)
from ..partitions.publication import UnpublishedLoader
from ..periods import parse_retention_period, period_class_key
from ..reads.publisher import StagedLoaderPublisher
from .database import PartitionMaintenanceDatabase


_LEAF_MAINTENANCE_ATTEMPTS = 3
_BUSY_RETRY_MIN_SECONDS = 0.02
_BUSY_RETRY_MAX_SECONDS = 0.08

_COVERAGE_PROBE_SQL: Final = text(
    'SELECT checked_at, leaf_kind, class_key, parent_name, leaf_name, lower_bound_kind, '
    'lower_anchor, upper_bound_kind, upper_anchor, conformant, registration_healthy, '
    'publication_healthy, absent_leaves '
    'FROM horsies_coverage_probe(CAST(:declared_days AS smallint[]), '
    'CAST(:history_horizon AS smallint), CAST(:heartbeat_horizon AS smallint))'
)
"""The check of one coverage pass (0008_history_maintenance_functions.sql)."""

_REGISTER_CLASSES_SQL: Final = text(
    'SELECT class_key, class_kind, outcome '
    'FROM horsies_register_retention_classes(CAST(:declared_days AS smallint[]), '
    'CAST(:heartbeat_horizon AS smallint))'
)
"""Registration of the default, declared and heartbeat classes, in the caller's transaction."""

_REGISTRATION_OUTCOMES: Final = frozenset({'REGISTERED', 'VERIFIED', 'HORIZON_UPDATED'})

_CREATE_HISTORY_LEAF_SQL: Final = text(
    'SELECT outcome, leaf_name, conflict_kind '
    'FROM horsies_create_history_leaf(:class_key, :lower_anchor)'
)
_CREATE_HEARTBEAT_LEAF_SQL: Final = text(
    'SELECT outcome, leaf_name, conflict_kind '
    'FROM horsies_create_heartbeat_leaf(:lower_anchor)'
)


@dataclass(frozen=True, slots=True)
class CoveredForever:
    """No history class has an end of coverage.

    Only the forever class has leaves, and its leaf has an open upper bound.
    """


@dataclass(frozen=True, slots=True)
class CoveredUntil:
    """The first instant (UTC) at which a history class has no leaf.

    The earliest, over the history classes with daily leaves, of the upper
    bound of each class's newest leaf. A write of that class at or after
    `at` finds no leaf.
    """

    at: datetime


type HistoryCoverage = CoveredForever | CoveredUntil
"""How far the history leaves reach: until an instant, or forever."""


def history_coverage_payload(coverage: HistoryCoverage) -> dict[str, str]:
    """The form of a `HistoryCoverage` in the worker health payload.

    `{'kind': 'FOREVER'}` or `{'kind': 'UNTIL', 'at': <ISO 8601 instant>}`.
    """
    match coverage:
        case CoveredForever():
            return {'kind': 'FOREVER'}
        case CoveredUntil(at=at):
            return {'kind': 'UNTIL', 'at': at.isoformat()}


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
    history_covered_through: HistoryCoverage
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
    damaged_history: tuple[LeafRef | ForeverLeafRef, ...]
    damaged_heartbeats: tuple[LeafRef, ...]
    registration_healthy: bool
    publication_healthy: bool
    heartbeat_covered_now: bool
    history_covered_through: HistoryCoverage
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


def _first_damaged_class_key(probe: _CoverageProbe) -> str | None:
    match probe.damaged_history, probe.damaged_heartbeats:
        case (first_history, *_), _:
            return first_history.class_key
        case (), (first_heartbeat, *_):
            return first_heartbeat.class_key
        case (), ():
            return None
        case _:
            raise AssertionError('unexpected coverage damage shape')


def _unresolved_coverage_detail(probe: _CoverageProbe) -> str:
    unresolved: list[str] = []
    if not probe.registration_healthy:
        unresolved.append('retention class registration')
    if not probe.publication_healthy:
        unresolved.append('loader publication')
    unresolved.extend(leaf.leaf_name for leaf in probe.damaged_history)
    unresolved.extend(leaf.leaf_name for leaf in probe.damaged_heartbeats)
    return 'unresolved coverage state: ' + ', '.join(unresolved)


def declared_days(
    declared_classes: Sequence[tuple[str, timedelta]],
) -> list[int]:
    """The day counts of the declared period classes, for the 0008 functions.

    Each declared class is `(period_class_key(days), timedelta(days=days))`
    (`RetentionConfig.registrable_classes`). A period that is not whole days
    in bounds, or a key that is not the key of its period, raises
    ``ValueError``: the functions derive every key from its day count.
    """
    days: list[int] = []
    for class_key, duration in declared_classes:
        match parse_retention_period(duration):
            case Err(err_value=refused):
                raise ValueError(f'declared class {class_key!r}: {refused.reason}')
            case Ok(ok_value=period_days):
                pass
        if class_key != period_class_key(period_days):
            raise ValueError(
                f'declared class {class_key!r} is not the class of its period '
                f'({period_class_key(period_days)!r})'
            )
        days.append(period_days)
    return days


async def _probe_complete_coverage(
    connection: AsyncConnection,
    *,
    history_horizon_days: int,
    heartbeat_horizon_hours: int,
    declared_classes: Sequence[tuple[str, timedelta]],
) -> _CoverageProbe:
    """One probe statement (``horsies_coverage_probe``) and its rows as a
    `_CoverageProbe`. Reads only; a database error raises."""
    rows = (
        await connection.execute(
            _COVERAGE_PROBE_SQL,
            {
                'declared_days': declared_days(declared_classes),
                'history_horizon': history_horizon_days,
                'heartbeat_horizon': heartbeat_horizon_hours,
            },
        )
    ).all()
    return _coverage_probe_of(rows)


def _probe_leaf_of(row: Row[Any]) -> LeafRef | ForeverLeafRef:
    """The leaf of one probe row.

    OPEN lower and upper bounds are the forever leaf, which has no anchors;
    AT bounds carry both anchors. Another pair breaks the probe's contract.
    """
    match str(row.lower_bound_kind), str(row.upper_bound_kind):
        case 'OPEN', 'OPEN':
            return ForeverLeafRef(
                leaf_name=str(row.leaf_name),
                class_key=str(row.class_key),
            )
        case 'AT', 'AT':
            # The driver decodes timestamptz in the session time zone; the
            # bounds are UTC instants.
            return LeafRef(
                leaf_name=str(row.leaf_name),
                class_key=str(row.class_key),
                bounds=LeafBounds(
                    lower=required_utc_timestamp(row.lower_anchor, 'lower_anchor'),
                    upper=required_utc_timestamp(row.upper_anchor, 'upper_anchor'),
                ),
            )
        case lower_kind, upper_kind:
            raise AssertionError(
                f'probe row {row.leaf_name}: bound kinds {lower_kind!r} and '
                f'{upper_kind!r} are not a pair of the probe'
            )


def _history_coverage_of(
    leaves: Sequence[LeafRef | ForeverLeafRef],
) -> HistoryCoverage:
    """How far the expected history leaves reach.

    A class's coverage ends at the upper bound of its newest leaf; the
    forever leaf has an open upper bound, so the forever class has no end.
    The answer is the earliest end over the classes, or `CoveredForever`
    when no class has an end. Pure.
    """
    newest_ends: dict[str, datetime] = {}
    for leaf in leaves:
        match leaf:
            case ForeverLeafRef():
                pass
            case LeafRef(class_key=class_key, bounds=bounds):
                newest_ends[class_key] = max(
                    newest_ends.get(class_key, bounds.upper),
                    bounds.upper,
                )
    if not newest_ends:
        return CoveredForever()
    return CoveredUntil(at=min(newest_ends.values()))


def _coverage_probe_of(rows: Sequence[Row[Any]]) -> _CoverageProbe:
    """The probe of the rows of one coverage probe statement, in probe order."""
    if not rows:
        raise AssertionError('coverage probe returned no expected leaves')
    first = rows[0]
    checked_at = required_utc_timestamp(first.checked_at, 'checked_at')
    damaged_history: list[LeafRef | ForeverLeafRef] = []
    damaged_heartbeats: list[LeafRef] = []
    history_leaves: list[LeafRef | ForeverLeafRef] = []
    heartbeat_covered_now = False
    heartbeats_covered_through: datetime | None = None
    for row in rows:
        leaf = _probe_leaf_of(row)
        match str(row.leaf_kind), bool(row.conformant), leaf:
            case 'history', False, _:
                damaged_history.append(leaf)
                history_leaves.append(leaf)
            case 'history', True, _:
                history_leaves.append(leaf)
            case 'heartbeat', _, ForeverLeafRef():
                raise AssertionError('a heartbeat leaf has an open bound')
            case 'heartbeat', False, LeafRef() as hourly:
                damaged_heartbeats.append(hourly)
                heartbeats_covered_through = max(
                    heartbeats_covered_through or hourly.bounds.upper,
                    hourly.bounds.upper,
                )
            case 'heartbeat', True, LeafRef() as hourly:
                heartbeats_covered_through = max(
                    heartbeats_covered_through or hourly.bounds.upper,
                    hourly.bounds.upper,
                )
                if hourly.bounds.lower <= checked_at < hourly.bounds.upper:
                    heartbeat_covered_now = True
            case unknown, _, _:
                raise AssertionError(f'unknown coverage leaf kind {unknown!r}')
    if not history_leaves or heartbeats_covered_through is None:
        raise AssertionError('coverage probe did not return both leaf types')
    absent = tuple(str(name) for name in first.absent_leaves)
    return _CoverageProbe(
        checked_at=checked_at,
        damaged_history=tuple(damaged_history),
        damaged_heartbeats=tuple(damaged_heartbeats),
        registration_healthy=bool(first.registration_healthy),
        publication_healthy=bool(first.publication_healthy),
        heartbeat_covered_now=heartbeat_covered_now,
        history_covered_through=_history_coverage_of(history_leaves),
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
                WHERE class_kind IN ('FOREVER', 'PERIOD')
                ORDER BY class_key
                """
            ),
            {},
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
    # and registries written before 0.6.1 hold `q_` keys, which sort
    # before every `standard_` key.
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
            # Leaves are created unpublished: the caller's transaction
            # commits once, and the publication after this loop (any
            # created leaf triggers it) covers every class's leaves.
            async with connection.begin_nested():
                creations = await ensure_leaf_coverage(
                    connection,
                    EnsureLeafCoverage(
                        class_key=class_key, horizon_days=history_horizon_days
                    ),
                    UnpublishedLoader(),
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
                (
                    LeafCreated,
                    LeafAlreadyConformant,
                    LeafIndexRepaired,
                    CoveredByOpenEndedLeaf,
                ),
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

    # The stamp check covers every rendered reader: each one carries the
    # stamp of the manifest it was rendered from.
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
    if created_history > 0 or not await publisher.is_current(connection):
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
        history_covered_through=CoveredUntil(
            at=day_lower + timedelta(days=history_horizon_days + 1),
        ),
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
    serialize_damaged_work: bool = True,
) -> CoverageOutcome:
    """Ensure the complete set through the maintenance database.

    A healthy set uses one probe statement (``horsies_coverage_probe``). A
    damaged set registers the classes (``horsies_register_retention_classes``)
    when the probe finds the registration unhealthy, then opens one
    transaction for each damaged leaf: one call of
    ``horsies_create_history_leaf`` or ``horsies_create_heartbeat_leaf``,
    which publishes a created history leaf in that transaction. The last
    step publishes the readers only when the probe still finds them stale.
    A busy lock ends the leaf's transaction before the retry delay starts. Startup serializes damaged passes with
    the coverage gate. The periodic reaper passes ``False`` because it
    already holds one pool connection for its cluster gate. Per-leaf try
    locks contain any overlap between a reaper pass and worker startup.
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

    if not serialize_damaged_work or not database.coverage_gate_enabled:
        return await _repair_partition_coverage(
            database,
            probe=probe,
            history_horizon_days=history_horizon_days,
            heartbeat_horizon_hours=heartbeat_horizon_hours,
            declared_classes=declared_classes,
        )

    async with database.begin() as gate_connection:
        gate_attempt = await try_lock_coverage_for_transaction(gate_connection)
        if gate_attempt is LeafLockAttempt.BUSY:
            async with database.begin() as connection:
                busy_probe = await _probe_complete_coverage(
                    connection,
                    history_horizon_days=history_horizon_days,
                    heartbeat_horizon_hours=heartbeat_horizon_hours,
                    declared_classes=declared_classes,
                )
            if busy_probe.healthy:
                return _coverage_ensured_from_probe(busy_probe)
            return CoverageEnsureFailed(
                stage='coverage_gate_busy',
                class_key=_first_damaged_class_key(busy_probe),
                refusal='another worker owns the partition coverage gate',
                heartbeat_covered_now=busy_probe.heartbeat_covered_now,
                absent_leaves=busy_probe.absent_leaves,
            )

        async with database.begin() as connection:
            locked_probe = await _probe_complete_coverage(
                connection,
                history_horizon_days=history_horizon_days,
                heartbeat_horizon_hours=heartbeat_horizon_hours,
                declared_classes=declared_classes,
            )
        if locked_probe.healthy:
            return _coverage_ensured_from_probe(locked_probe)
        return await _repair_partition_coverage(
            database,
            probe=locked_probe,
            history_horizon_days=history_horizon_days,
            heartbeat_horizon_hours=heartbeat_horizon_hours,
            declared_classes=declared_classes,
        )


async def _repair_partition_coverage(
    database: PartitionMaintenanceDatabase,
    *,
    probe: _CoverageProbe,
    history_horizon_days: int,
    heartbeat_horizon_hours: int,
    declared_classes: Sequence[tuple[str, timedelta]],
) -> CoverageOutcome:
    """Repair one damaged probe while an optional outer gate is held."""

    if not probe.registration_healthy:
        async with database.begin() as connection:
            await _register_required_classes(
                connection,
                heartbeat_horizon_hours=heartbeat_horizon_hours,
                declared_classes=declared_classes,
            )
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

    created_history = 0
    created_heartbeats = 0
    failures: list[str] = []
    failed_history_classes: set[str] = set()
    for leaf in probe.damaged_history:
        if leaf.class_key in failed_history_classes:
            continue
        try:
            outcome = await _maintain_history_leaf(database, leaf)
        except Exception as leaf_error:
            failures.append(f'{leaf.class_key}: {leaf_error!r}')
            failed_history_classes.add(leaf.class_key)
            continue
        match outcome:
            case LeafCreated():
                created_history += 1
            case (
                LeafAlreadyConformant()
                | LeafIndexRepaired()
                | CoveredByOpenEndedLeaf()
            ):
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
            publisher = StagedLoaderPublisher()
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

    detail = failures or [_unresolved_coverage_detail(final_probe)]
    return CoverageEnsureFailed(
        stage='ensure_leaf_coverage',
        class_key=_first_damaged_class_key(final_probe),
        refusal='; '.join(detail),
        heartbeat_covered_now=final_probe.heartbeat_covered_now,
        absent_leaves=final_probe.absent_leaves,
    )


async def _register_required_classes(
    connection: AsyncConnection,
    *,
    heartbeat_horizon_hours: int,
    declared_classes: Sequence[tuple[str, timedelta]],
) -> None:
    """Register the default, declared and heartbeat classes in the caller's transaction.

    One call of ``horsies_register_retention_classes``: each class is
    REGISTERED, VERIFIED or (heartbeat) HORIZON_UPDATED. The function
    refuses illegal input with 22023 (the config bounds keep it legal); a
    database error raises. A row with another outcome raises
    ``HistoryContractError``. The coverage pass reads the result back
    through the probe's ``registration_healthy``.
    """
    rows = (
        await connection.execute(
            _REGISTER_CLASSES_SQL,
            {
                'declared_days': declared_days(declared_classes),
                'heartbeat_horizon': heartbeat_horizon_hours,
            },
        )
    ).all()
    for row in rows:
        if row.outcome not in _REGISTRATION_OUTCOMES:
            raise HistoryContractError(
                f'class registration of {row.class_key!r} returned {row.outcome!r}'
            )


_CONFLICT_KINDS: Final = {kind.value: kind for kind in CatalogConflictKind}


def leaf_creation_of(
    class_key: str, leaf_name: object, outcome: object, conflict_kind: object
) -> LeafCreation:
    """The outcome type of one ``horsies_create_history_leaf`` or
    ``horsies_create_heartbeat_leaf`` row; an unknown value raises
    ``HistoryContractError``."""
    match outcome, leaf_name:
        case 'CLASS_ABSENT', _:
            return RetentionClassAbsent(class_key=class_key)
        case _, name if not isinstance(name, str):
            raise HistoryContractError(
                f'leaf creation of class {class_key!r} returned {outcome!r} '
                f'with leaf name {name!r}'
            )
        case 'CREATED', str() as name:
            return LeafCreated(leaf_name=name, id_index_name=_id_index_name(class_key, name))
        case 'INDEX_REPAIRED', str() as name:
            return LeafIndexRepaired(
                leaf_name=name, id_index_name=_id_index_name(class_key, name)
            )
        case 'ALREADY_CONFORMANT', str() as name:
            return LeafAlreadyConformant(leaf_name=name)
        case 'COVERED_BY_OPEN_ENDED', str() as name:
            return CoveredByOpenEndedLeaf(leaf_name=name, open_ended_leaf_name=FOREVER_LEAF)
        case 'BUSY', str() as name:
            return LeafMaintenanceBusy(leaf_name=name)
        case 'CATALOG_CONFLICT', str() as name:
            kind = _CONFLICT_KINDS.get(conflict_kind) if isinstance(conflict_kind, str) else None
            if kind is None:
                raise HistoryContractError(
                    f'leaf {name}: conflict_kind {conflict_kind!r} is not a known kind'
                )
            return LeafCatalogConflict(
                leaf_name=name, kind=kind, detail=f'leaf creation found {kind.value}'
            )
        case _, name:
            raise HistoryContractError(f'leaf creation of {name} returned {outcome!r}')


def _id_index_name(class_key: str, leaf_name: str) -> str:
    """The task-ID index of a history leaf, the probe index of a heartbeat leaf."""
    match class_key:
        case value if value == HEARTBEAT_CLASS_KEY:
            return heartbeat_probe_index_name(leaf_name)
        case _:
            return leaf_id_index_name(leaf_name)


async def _create_history_leaf(
    connection: AsyncConnection,
    leaf: LeafRef | ForeverLeafRef,
) -> LeafCreation:
    """Create, verify or repair one expected history leaf in the caller's
    transaction: one call of ``horsies_create_history_leaf``.

    A created leaf is published inside the call. The static forever leaf
    (``ForeverLeafRef``) is verified, never created or repaired.
    """
    match leaf:
        case ForeverLeafRef():
            lower_anchor: datetime | None = None
        case LeafRef(bounds=bounds):
            lower_anchor = bounds.lower
    row = (
        await connection.execute(
            _CREATE_HISTORY_LEAF_SQL,
            {'class_key': leaf.class_key, 'lower_anchor': lower_anchor},
        )
    ).one()
    return leaf_creation_of(leaf.class_key, row.leaf_name, row.outcome, row.conflict_kind)


async def _create_heartbeat_leaf(connection: AsyncConnection, leaf: LeafRef) -> LeafCreation:
    """Create, verify or repair one expected hourly heartbeat leaf in the
    caller's transaction: one call of ``horsies_create_heartbeat_leaf``."""
    row = (
        await connection.execute(
            _CREATE_HEARTBEAT_LEAF_SQL, {'lower_anchor': leaf.bounds.lower}
        )
    ).one()
    return leaf_creation_of(leaf.class_key, row.leaf_name, row.outcome, row.conflict_kind)


async def _maintain_history_leaf(
    database: PartitionMaintenanceDatabase,
    leaf: LeafRef | ForeverLeafRef,
) -> LeafCreation:
    """One expected history leaf in its own transaction; `LeafMaintenanceBusy`
    is retried `_LEAF_MAINTENANCE_ATTEMPTS` times with a random delay."""
    outcome: LeafCreation = LeafMaintenanceBusy(leaf_name=leaf.leaf_name)
    for attempt in range(_LEAF_MAINTENANCE_ATTEMPTS):
        async with database.begin() as connection:
            outcome = await _create_history_leaf(connection, leaf)
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
    """One expected heartbeat leaf in its own transaction; `LeafMaintenanceBusy`
    is retried `_LEAF_MAINTENANCE_ATTEMPTS` times with a random delay."""
    outcome: LeafCreation = LeafMaintenanceBusy(leaf_name=leaf.leaf_name)
    for attempt in range(_LEAF_MAINTENANCE_ATTEMPTS):
        async with database.begin() as connection:
            outcome = await _create_heartbeat_leaf(connection, leaf)
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


class CoverageSerialization(Enum):
    """How a damaged coverage pass of the core is serialized against other passes."""

    GATED = 'GATED'
    """Worker startup: a damaged pass takes the coverage gate when the core
    pool has two connections or more; a pass that finds the gate busy repairs
    nothing (``serialize_damaged_work=True``)."""

    PER_LEAF_LOCKS = 'PER_LEAF_LOCKS'
    """The reaper's periodic pass: no gate; the per-leaf locks contain an
    overlap with a startup pass (``serialize_damaged_work=False``)."""


COVERAGE_SUBJECT: Final = 'CoreHandle.ensure_partition_coverage_async'
"""The subject of a ``CoreOperationFailed`` of the core coverage pass."""


def _history_coverage_of_record(
    record: CoveredForeverRecord | CoveredUntilRecord,
) -> HistoryCoverage:
    """The ``HistoryCoverage`` of a core record. Pure."""
    match record:
        case CoveredForeverRecord():
            return CoveredForever()
        case CoveredUntilRecord(at=at):
            return CoveredUntil(at=at)
        case _ as unreachable:
            assert_never(unreachable)


async def maintain_partition_coverage_in_core(
    handle: CoreHandle,
    *,
    history_horizon_days: int,
    heartbeat_horizon_hours: int,
    declared_classes: Sequence[tuple[str, timedelta]],
    serialization: CoverageSerialization,
) -> CoverageOutcome:
    """One coverage pass in the core (``CoreHandle.ensure_partition_coverage_async``):
    the steps of ``maintain_partition_coverage`` on the session-work core
    pool of ``handle``, whose connections carry the TCP keepalives and the
    TCP user timeout of the config.

    Returns ``CoverageEnsured`` or ``CoverageEnsureFailed`` (a contained
    refusal), with the facts of the core's record. Raises:

    - ``ValueError`` before any core call: a declared class that is not the
      class of a period of whole days (``declared_days``), or a horizon that
      the core request refuses;
    - ``CoreOperationFailed`` when the pass has no outcome (the connect, the
      client family check, a probe, the gate, the registration or the final
      publication failed); the leaves that committed before stay;
    - ``CoreError`` when the core of this process cannot run the pass.

    A cancel of the caller does not stop the pass: the core runs it to its
    end.
    """
    request = CoverageRequest(
        history_horizon_days=history_horizon_days,
        heartbeat_horizon_hours=heartbeat_horizon_hours,
        declared_period_days=declared_days(declared_classes),
        serialization=serialization.value,
    )
    record = await handle.ensure_partition_coverage_async(request)
    match record:
        case CoverageEnsuredRecord():
            return CoverageEnsured(
                created_history_leaves=record.created_history_leaves,
                created_heartbeat_leaves=record.created_heartbeat_leaves,
                republished=record.republished,
                heartbeat_covered_now=record.heartbeat_covered_now,
                history_covered_through=_history_coverage_of_record(
                    record.history_covered_through
                ),
                heartbeats_covered_through=record.heartbeats_covered_through,
                absent_leaves=tuple(record.absent_leaves),
            )
        case CoverageEnsureFailedRecord():
            return CoverageEnsureFailed(
                stage=record.stage,
                class_key=record.class_key,
                refusal=record.refusal,
                heartbeat_covered_now=record.heartbeat_covered_now,
                absent_leaves=tuple(record.absent_leaves),
            )
        case CoverageFailed(message=message, retryable=retryable):
            raise CoreOperationFailed(COVERAGE_SUBJECT, message, retryable)
        case _ as unreachable:
            assert_never(unreachable)


async def ensure_startup_coverage_in_core(
    handle: CoreHandle,
    *,
    history_horizon_days: int,
    heartbeat_horizon_hours: int,
    declared_classes: Sequence[tuple[str, timedelta]],
) -> CoverageOutcome | StartupCoverageRefused:
    """The worker-startup ensure in the core: the ``GATED`` pass of
    ``maintain_partition_coverage_in_core``, fatal exactly when no heartbeat
    leaf covers the present instant (``StartupCoverageRefused``). Raises as
    ``maintain_partition_coverage_in_core``.
    """
    outcome = await maintain_partition_coverage_in_core(
        handle,
        history_horizon_days=history_horizon_days,
        heartbeat_horizon_hours=heartbeat_horizon_hours,
        declared_classes=declared_classes,
        serialization=CoverageSerialization.GATED,
    )
    if not outcome.heartbeat_covered_now:
        return StartupCoverageRefused(outcome=outcome)
    return outcome
