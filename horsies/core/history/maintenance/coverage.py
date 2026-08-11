"""The coverage/publication maintenance owner.

Partitioned writes need their partitions before the write arrives:
heartbeat leaves for the worker's own liveness rows, history leaves for
every terminalization, and the staged readers published over whatever
leaves exist. Nothing else owns that sequence — the offline cutover
ensures it once for upgraded installs and then hands off — so workers
own it here: once at startup (before the first claim) and periodically
under the reaper's cluster-wide gate.

Every step is idempotent. Republication runs only when a leaf was
actually created or a staged reader is absent; a healthy fleet's
maintenance tick performs reads only.

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
from ..reads.publisher import StagedLoaderPublisher


@dataclass(frozen=True, slots=True)
class CoverageEnsured:
    """One completed ensure pass, what it changed, and how far
    coverage now reaches — the operator's answer to "how long do
    existing leaves absorb writes if maintenance starts failing"."""

    created_history_leaves: int
    created_heartbeat_leaves: int
    republished: bool
    heartbeat_covered_now: bool
    history_covered_through: datetime
    heartbeats_covered_through: datetime


@dataclass(frozen=True, slots=True)
class CoverageEnsureFailed:
    """The pass stopped at a refusal; the fields name where and why."""

    stage: str
    class_key: str | None
    refusal: str
    heartbeat_covered_now: bool


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


async def _finite_class_keys(connection: AsyncConnection) -> list[str]:
    rows = (
        await connection.execute(
            text(
                f"""
                SELECT class_key FROM {RETENTION_CLASSES}
                WHERE duration IS NOT NULL
                  AND class_key <> :heartbeat_class
                ORDER BY class_key
                """
            ),
            {'heartbeat_class': HEARTBEAT_CLASS_KEY},
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

    The caller owns the transaction. On a refusal the pass stops and
    reports it verbatim — a hole in the middle of coverage is not
    coverage, and a refusal retried into silence is the failure mode
    this owner exists to prevent.
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
                )

    publisher = StagedLoaderPublisher()
    created_history = 0
    # One class must not cost the others their coverage. The loop reads
    # registered classes in key order, so an unhandled failure here
    # silently denied leaves to every class sorting after it, and denied
    # them from a database row that removing a declaration cannot reach.
    # Containment mirrors the pruning pass, which bounds per leaf for the
    # same reason, and it also keeps the failure visible: the caller
    # matches on the returned refusal to set coverage health, and a raise
    # left that health reporting whatever the previous pass had put there.
    raised: list[str] = []
    for class_key in await _finite_class_keys(connection):
        try:
            creations = await ensure_leaf_coverage(
                connection,
                EnsureLeafCoverage(
                    class_key=class_key, horizon_days=history_horizon_days
                ),
                publisher,
            )
        except Exception as class_error:
            raised.append(f'{class_key}: {class_error!r}')
            continue
        for creation in creations:
            match creation:
                case LeafCreated():
                    created_history += 1
                case LeafAlreadyConformant() | LeafIndexRepaired():
                    continue
                case _:
                    return CoverageEnsureFailed(
                        stage='ensure_leaf_coverage',
                        class_key=class_key,
                        refusal=repr(creation),
                        heartbeat_covered_now=(
                            await heartbeat_coverage_present(connection)
                        ),
                    )

    if raised:
        # Every class is attempted before reporting, so the refusal names
        # all of them rather than only the one that sorted first.
        return CoverageEnsureFailed(
            stage='ensure_leaf_coverage',
            class_key=raised[0].split(':', 1)[0],
            refusal=f'{len(raised)} class(es) failed: ' + '; '.join(raised),
            heartbeat_covered_now=(
                await heartbeat_coverage_present(connection)
            ),
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
                )

    # One probe stands in for all three staged readers because
    # republication is atomic across the triple; the dependency is
    # pinned by a test beside this logic.
    republished = False
    if created_history > 0 or not await staged_detail_published(connection):
        await publisher.republish(connection)
        republished = True

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
    )


@dataclass(frozen=True, slots=True)
class StartupCoverageRefused:
    """The worker must not start: no heartbeat leaf covers now.

    Raised as a hard startup error by the worker — starting anyway
    guarantees the first RUNNING transition fails on a missing
    heartbeat partition.
    """

    outcome: CoverageOutcome


async def ensure_startup_coverage(
    connection: AsyncConnection,
    *,
    history_horizon_days: int,
    heartbeat_horizon_hours: int,
    declared_classes: Sequence[tuple[str, timedelta]] = (),
) -> CoverageOutcome | StartupCoverageRefused:
    """The worker-startup ensure: fatal exactly when now is uncovered."""
    outcome = await ensure_partition_coverage(
        connection,
        history_horizon_days=history_horizon_days,
        heartbeat_horizon_hours=heartbeat_horizon_hours,
        declared_classes=declared_classes,
    )
    if not outcome.heartbeat_covered_now:
        return StartupCoverageRefused(outcome=outcome)
    return outcome
