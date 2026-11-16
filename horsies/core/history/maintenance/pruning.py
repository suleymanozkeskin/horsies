"""Retention pruning: the maintenance tick's detach-and-drop pass.

Partition-drop retention has two halves: coverage creates leaves ahead
of writes, and pruning removes leaves whose class duration has elapsed.
This module owns the second half — discovery from the leaf catalog
joined to the retention classes, then the shared manager machinery per
leaf. An interrupted detach is finalized before anything else, because
PostgreSQL refuses every further detach on a parent while one is
pending finalization; then each expired leaf is detached and dropped.
Forever is never a candidate: discovery selects ``PERIOD`` and
``HEARTBEAT`` classes by kind, and the static forever leaf has no
anchored lower bound.

Refusals are outcomes, not errors: a pending locator pinning a leaf, a
loader still referencing a relation, or a blocked finalization each
skip the leaf and surface in the pass report — one leaf's refusal or
database error never stops the pass, and a leaf skipped this pass is
the next pass's candidate. Detach and finalize manage their own autocommit
connections (the manager's invariant); the drop joins a transaction
opened here. The concurrent detach carries a statement timeout so a
single long reader stalls one leaf's detach, never the maintenance
tick.
"""

from __future__ import annotations

from dataclasses import dataclass, replace
from typing import Final

from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncConnection

from ..commands import (
    DETACH_STATEMENT_TIMEOUT_MS,
    PRUNE_BOUNDS,
    DetachExpiredHistoryLeaf,
    DropDetachedHistoryLeaf,
    FinalizeInterruptedLeafDetach,
    InspectHistoryLeaf,
    LeafLifecycle,
    LeafRef,
    PruneBounds,
)
from .database import PartitionMaintenanceDatabase
from ..heartbeats.partitioning import (
    HeartbeatLeafSwept,
    sweep_expired_heartbeat_leaves,
)
from ..names import LEAF_CATALOG, RETENTION_CLASSES
from ..outcomes import (
    LeafDetached,
    LeafDetachInterrupted,
    LeafDrop,
    LeafDropped,
    LeafInspection,
    LeafMaintenanceBusy,
    LeafSweep,
    PruneReach,
    counts_toward_prune_bound,
)
from ..partitions.catalog import decode_leaf_bounds
from ..partitions.manager import (
    detach_expired_leaf,
    drop_detached_leaf,
    finalize_interrupted_detach,
    inspect_leaf,
)
from ..partitions.publication import LoaderPublication

__all__ = [
    'DETACH_STATEMENT_TIMEOUT_MS',
    'HistoryLeafSwept',
    'PRUNE_BOUNDS',
    'PrunePass',
    'prune_expired_partitions',
    'sweep_expired_history_leaves',
]


# Period classes past their retention. Forever is excluded by kind; the
# heartbeat class has its own sweep.
EXPIRED_HISTORY_LEAVES_SQL: Final = f"""
SELECT c.leaf_name, c.class_key, c.lower_anchor, c.upper_anchor
FROM {LEAF_CATALOG} AS c
JOIN {RETENTION_CLASSES} AS r ON r.class_key = c.class_key
WHERE r.class_kind = 'PERIOD'
  AND c.lower_bound_kind = 'AT'
  AND c.dropped_at IS NULL
  AND c.upper_anchor + r.retention_days * interval '1 day'
      <= statement_timestamp()
ORDER BY c.lower_anchor
LIMIT :limit
"""

# Every finite-class candidate, heartbeats included: the finalize
# pre-phase must see all parents, because one pending finalization
# blocks every further detach on its parent.
EXPIRED_FINITE_LEAVES_SQL: Final = f"""
SELECT c.leaf_name, c.class_key, c.lower_anchor, c.upper_anchor
FROM {LEAF_CATALOG} AS c
JOIN {RETENTION_CLASSES} AS r ON r.class_key = c.class_key
WHERE r.class_kind IN ('PERIOD', 'HEARTBEAT')
  AND c.lower_bound_kind = 'AT'
  AND c.dropped_at IS NULL
  AND c.upper_anchor
      + COALESCE(
          r.retention_days * interval '1 day',
          r.heartbeat_horizon_hours * interval '1 hour'
      )
      <= statement_timestamp()
ORDER BY c.lower_anchor
LIMIT :limit
"""


@dataclass(frozen=True, slots=True)
class HistoryLeafSwept:
    """One expired task-history leaf's sweep result: the detach-side
    outcome and, when the leaf reached the detached state, the drop
    outcome."""

    leaf_name: str
    class_key: str
    detach: LeafInspection | LeafMaintenanceBusy
    drop: LeafDrop | None


@dataclass(frozen=True, slots=True)
class PrunePass:
    """One pruning pass's accounting, health-surface-ready.

    `refusals` are typed outcomes that skipped a leaf, rendered as
    `leaf: OutcomeName`; `errors` are operational failures the pass
    contained. Both leave the leaf as the next pass's candidate.
    `reach` is `TRUNCATED` when a per-pass bound left expired leaves for
    the next pass.
    """

    finalized_leaves: tuple[str, ...]
    heartbeat_swept: tuple[HeartbeatLeafSwept, ...]
    history_swept: tuple[HistoryLeafSwept, ...]
    refusals: tuple[str, ...]
    errors: tuple[str, ...]
    reach: PruneReach

    @property
    def detached_count(self) -> int:
        return sum(
            1
            for entry in (*self.heartbeat_swept, *self.history_swept)
            if isinstance(entry.detach, LeafDetached)
        )

    @property
    def dropped_count(self) -> int:
        return sum(
            1
            for entry in (*self.heartbeat_swept, *self.history_swept)
            if isinstance(entry.drop, LeafDropped)
        )

    @property
    def acted(self) -> bool:
        """Whether the pass has anything worth a log line."""
        return bool(
            self.finalized_leaves
            or self.heartbeat_swept
            or self.history_swept
            or self.refusals
            or self.errors
        )


@dataclass(frozen=True, slots=True)
class _Candidates:
    leaves: tuple[LeafRef, ...]
    reach: PruneReach


async def _expired_candidates(
    connection: AsyncConnection,
    statement: str,
    bounds: PruneBounds,
) -> _Candidates:
    """The oldest expired leaves that `statement` selects, at most
    `bounds.candidates`, and whether more remain."""
    rows = (
        await connection.execute(text(statement), {'limit': bounds.candidates + 1})
    ).all()
    return _Candidates(
        leaves=tuple(
            LeafRef(
                leaf_name=row.leaf_name,
                class_key=row.class_key,
                bounds=decode_leaf_bounds(row.lower_anchor, row.upper_anchor),
            )
            for row in rows[: bounds.candidates]
        ),
        reach=(
            PruneReach.TRUNCATED
            if len(rows) > bounds.candidates
            else PruneReach.WITHIN_BOUNDS
        ),
    )


@dataclass(frozen=True, slots=True)
class _Finalized:
    leaves: tuple[str, ...]
    refusals: tuple[str, ...]
    errors: tuple[str, ...]
    reach: PruneReach


async def _finalize_interrupted_detaches(
    database: PartitionMaintenanceDatabase,
    publisher: LoaderPublication,
    bounds: PruneBounds,
) -> _Finalized:
    """Finalize every interrupted detach among the expired candidates.

    A blocked finalization is a refusal — the manager returns the leaf
    re-inspected and it stays pending until its blockers drain.
    Finalizations do not count toward `bounds.acted_leaves`.
    """
    async with database.connect() as connection:
        candidates = await _expired_candidates(
            connection, EXPIRED_FINITE_LEAVES_SQL, bounds
        )
        interrupted: list[LeafRef] = []
        for ref in candidates.leaves:
            inspection = await inspect_leaf(
                connection,
                InspectHistoryLeaf(leaf=ref, lifecycle=LeafLifecycle.RETENTION),
            )
            match inspection:
                case LeafDetachInterrupted():
                    interrupted.append(ref)
                case _:
                    pass
    finalized: list[str] = []
    refusals: list[str] = []
    errors: list[str] = []
    for ref in interrupted:
        try:
            outcome = await finalize_interrupted_detach(
                database,
                FinalizeInterruptedLeafDetach(
                    leaf=ref,
                    statement_timeout_ms=DETACH_STATEMENT_TIMEOUT_MS,
                    lifecycle=LeafLifecycle.RETENTION,
                ),
                publisher,
            )
        except SQLAlchemyError as error:
            errors.append(f'finalize {ref.leaf_name}: {error}')
            continue
        match outcome:
            case LeafDetached():
                finalized.append(ref.leaf_name)
            case _:
                refusals.append(
                    f'finalize {ref.leaf_name}: {type(outcome).__name__}'
                )
    return _Finalized(
        leaves=tuple(finalized),
        refusals=tuple(refusals),
        errors=tuple(errors),
        reach=candidates.reach,
    )


async def sweep_expired_history_leaves(
    database: PartitionMaintenanceDatabase,
    publisher: LoaderPublication,
    bounds: PruneBounds = PRUNE_BOUNDS,
) -> LeafSweep[HistoryLeafSwept]:
    """Detach and drop the oldest finite task-history leaves past their horizon.

    Discovery reads at most `bounds.candidates` expired leaves, oldest
    first. Mirrors the heartbeat sweep's sequence per leaf — detach,
    re-inspect for the crashed-between-detach-and-drop state, drop — with
    a statement timeout on the concurrent detach. The sweep stops when
    `bounds.acted_leaves` leaves count toward the bound
    (`counts_toward_prune_bound`). A database error of one leaf is
    reported as `leaf: cause`, counts toward the bound, and the sweep
    goes on with the next leaf; a database error of the discovery raises.
    """
    async with database.connect() as connection:
        candidates = await _expired_candidates(
            connection, EXPIRED_HISTORY_LEAVES_SQL, bounds
        )
    swept: list[HistoryLeafSwept] = []
    errors: list[str] = []
    acted = 0
    reach = candidates.reach
    for ref in candidates.leaves:
        if acted >= bounds.acted_leaves:
            reach = PruneReach.TRUNCATED
            break
        try:
            entry = await _sweep_history_leaf(database, publisher, ref)
        except SQLAlchemyError as error:
            acted += 1
            errors.append(f'{ref.leaf_name}: {error}')
            continue
        acted += int(counts_toward_prune_bound(entry.detach, entry.drop))
        swept.append(entry)
    return LeafSweep(
        swept=tuple(swept), errors=tuple(errors), acted=acted, reach=reach
    )


async def _sweep_history_leaf(
    database: PartitionMaintenanceDatabase,
    publisher: LoaderPublication,
    ref: LeafRef,
) -> HistoryLeafSwept:
    """Detach one expired history leaf and drop it once detached.

    A leaf that an earlier crash left detached is dropped too. A database
    error raises.
    """
    detach_outcome = await detach_expired_leaf(
        database,
        DetachExpiredHistoryLeaf(
            leaf=ref,
            statement_timeout_ms=DETACH_STATEMENT_TIMEOUT_MS,
        ),
        publisher,
    )
    drop_outcome: LeafDrop | None = None
    detached = isinstance(detach_outcome, LeafDetached)
    if not detached:
        # A crash between detach and drop leaves a detached leaf the next
        # sweep must still drop; re-inspect for that state.
        async with database.connect() as connection:
            inspection = await inspect_leaf(
                connection,
                InspectHistoryLeaf(leaf=ref, lifecycle=LeafLifecycle.RETENTION),
            )
        detached = isinstance(inspection, LeafDetached)
    if detached:
        async with database.begin() as connection:
            drop_outcome = await drop_detached_leaf(
                connection,
                DropDetachedHistoryLeaf(leaf=ref, lifecycle=LeafLifecycle.RETENTION),
                publisher,
            )
    return HistoryLeafSwept(
        leaf_name=ref.leaf_name,
        class_key=ref.class_key,
        detach=detach_outcome,
        drop=drop_outcome,
    )


def _sweep_refusals(
    heartbeat_swept: tuple[HeartbeatLeafSwept, ...],
    history_swept: tuple[HistoryLeafSwept, ...],
) -> tuple[str, ...]:
    """Every swept entry that did not end in a drop, named by outcome."""
    refusals: list[str] = []
    for entry in (*heartbeat_swept, *history_swept):
        match entry.drop:
            case LeafDropped():
                continue
            case None:
                refusals.append(
                    f'{entry.leaf_name}: {type(entry.detach).__name__}'
                )
            case _:
                refusals.append(
                    f'{entry.leaf_name}: {type(entry.drop).__name__}'
                )
    return tuple(refusals)


async def prune_expired_partitions(
    database: PartitionMaintenanceDatabase,
    publisher: LoaderPublication,
    bounds: PruneBounds = PRUNE_BOUNDS,
) -> PrunePass:
    """One pruning pass: finalize interrupted detaches, then the heartbeat
    and finite-history sweeps.

    The finalize phase runs first across every finite class because one
    pending finalization blocks all further detaches on its parent. Both
    sweeps contain a database error per leaf: the leaf is reported and
    the sweep goes on with the next leaf. A failed heartbeat discovery is
    one pass error (`heartbeat sweep: cause`) and the task-history sweep
    still runs. The sweeps act on at most `bounds.acted_leaves` leaves,
    heartbeat leaves first; each candidate read returns at most
    `bounds.candidates` leaves.
    """
    finalized = await _finalize_interrupted_detaches(database, publisher, bounds)
    errors: list[str] = list(finalized.errors)
    heartbeat_swept: tuple[HeartbeatLeafSwept, ...] = ()
    heartbeat_acted = 0
    heartbeat_reach = PruneReach.WITHIN_BOUNDS
    try:
        heartbeat = await sweep_expired_heartbeat_leaves(database, publisher, bounds)
    except SQLAlchemyError as error:
        errors.append(f'heartbeat sweep: {error}')
    else:
        heartbeat_swept = heartbeat.swept
        heartbeat_acted = heartbeat.acted
        heartbeat_reach = heartbeat.reach
        errors.extend(f'heartbeat {error}' for error in heartbeat.errors)
    history = await sweep_expired_history_leaves(
        database,
        publisher,
        replace(bounds, acted_leaves=bounds.acted_leaves - heartbeat_acted),
    )
    errors.extend(history.errors)
    match finalized.reach, heartbeat_reach, history.reach:
        case (
            PruneReach.WITHIN_BOUNDS,
            PruneReach.WITHIN_BOUNDS,
            PruneReach.WITHIN_BOUNDS,
        ):
            reach = PruneReach.WITHIN_BOUNDS
        case _:
            reach = PruneReach.TRUNCATED
    return PrunePass(
        finalized_leaves=finalized.leaves,
        heartbeat_swept=heartbeat_swept,
        history_swept=history.swept,
        refusals=(
            *finalized.refusals,
            *_sweep_refusals(heartbeat_swept, history.swept),
        ),
        errors=tuple(errors),
        reach=reach,
    )
