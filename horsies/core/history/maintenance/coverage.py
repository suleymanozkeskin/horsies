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

from dataclasses import dataclass
from collections.abc import Sequence
from datetime import datetime, timedelta

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection

from ..commands import EnsureLeafCoverage
from ..ddl.classes import (
    ClassAlreadyRegistered,
    ClassRegistered,
    DEFAULT_RETENTION_CLASS_KEY,
    DEFAULT_RETENTION_DURATION,
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
    register_heartbeat_class,
)
from ..names import HEARTBEAT_CLASS_KEY, RETENTION_CLASSES
from ..outcomes import (
    LeafAlreadyConformant,
    LeafCreated,
    LeafIndexRepaired,
)
from ..partitions.catalog import database_now
from ..partitions.manager import ensure_leaf_coverage
from ..reads.detail import staged_detail_published
from ..reads.publisher import (
    StagedLoaderPublisher,
    published_manifest_absent_leaves,
)


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
                heartbeat_covered_now=await heartbeat_coverage_present(
                    connection
                ),
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
                heartbeat_covered_now=await heartbeat_coverage_present(
                    connection
                ),
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
                    heartbeat_covered_now=await heartbeat_coverage_present(
                        connection
                    ),
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
                f'{class_key}: '
                + '; '.join(repr(item) for item in refusals)
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
            refusal=(
                f'{len(failures)} class(es) failed: ' + '; '.join(failures)
            ),
            heartbeat_covered_now=(
                await heartbeat_coverage_present(connection)
            ),
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
        history_covered_through=(
            day_lower + timedelta(days=history_horizon_days + 1)
        ),
        heartbeats_covered_through=(
            hour_lower + timedelta(hours=heartbeat_horizon_hours + 1)
        ),
        absent_leaves=absent_leaves,
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
