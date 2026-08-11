"""Retention pruning: the maintenance tick's detach-and-drop pass.

Partition-drop retention has two halves: coverage creates leaves ahead
of writes, and pruning removes leaves whose class duration has elapsed.
This module owns the second half — discovery from the leaf catalog
joined to the retention classes, then the shared manager machinery per
leaf. An interrupted detach is finalized before anything else, because
PostgreSQL refuses every further detach on a parent while one is
pending finalization; then each expired leaf is detached and dropped.
Forever is never a candidate: its class row carries a NULL duration,
and the discovery filter requires a non-NULL one explicitly rather
than leaning on NULL interval arithmetic.

Refusals are outcomes, not errors: a pending locator pinning a leaf, a
loader still referencing a relation, or a blocked finalization each
skip the leaf and surface in the pass report — one leaf's refusal or
failure never stops the pass, and a leaf skipped this pass is the next
pass's candidate. Detach and finalize manage their own autocommit
connections (the manager's invariant); the drop joins a transaction
opened here. The concurrent detach carries a statement timeout so a
single long reader stalls one leaf's detach, never the maintenance
tick.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Final

from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

from ..commands import (
    DETACH_STATEMENT_TIMEOUT_MS,
    DetachExpiredHistoryLeaf,
    DropDetachedHistoryLeaf,
    FinalizeInterruptedLeafDetach,
    InspectHistoryLeaf,
    LeafBounds,
    LeafRef,
)
from ..heartbeats.partitioning import (
    HeartbeatLeafSwept,
    sweep_expired_heartbeat_leaves,
)
from ..names import HEARTBEAT_CLASS_KEY, LEAF_CATALOG, RETENTION_CLASSES
from ..outcomes import (
    LeafDetached,
    LeafDetachInterrupted,
    LeafDrop,
    LeafDropped,
    LeafInspection,
)
from ..partitions.manager import (
    detach_expired_leaf,
    drop_detached_leaf,
    finalize_interrupted_detach,
    inspect_leaf,
)
from ..partitions.publication import LoaderPublication
from ..phase2.quarantine import QuarantineRefused

__all__ = [
    'DETACH_STATEMENT_TIMEOUT_MS',
    'HistoryLeafSwept',
    'PrunePass',
    'prune_expired_partitions',
    'sweep_expired_history_leaves',
]


# Finite task-history classes past horizon. `duration IS NOT NULL`
# excludes forever explicitly; the heartbeat class has its own sweep.
EXPIRED_HISTORY_LEAVES_SQL: Final = f"""
SELECT c.leaf_name, c.class_key, c.lower_anchor, c.upper_anchor
FROM {LEAF_CATALOG} AS c
JOIN {RETENTION_CLASSES} AS r ON r.class_key = c.class_key
WHERE c.class_key <> :heartbeat_class_key
  AND r.duration IS NOT NULL
  AND c.dropped_at IS NULL
  AND c.upper_anchor + r.duration <= statement_timestamp()
ORDER BY c.lower_anchor
"""

# Every finite-class candidate, heartbeats included: the finalize
# pre-phase must see all parents, because one pending finalization
# blocks every further detach on its parent.
EXPIRED_FINITE_LEAVES_SQL: Final = f"""
SELECT c.leaf_name, c.class_key, c.lower_anchor, c.upper_anchor
FROM {LEAF_CATALOG} AS c
JOIN {RETENTION_CLASSES} AS r ON r.class_key = c.class_key
WHERE r.duration IS NOT NULL
  AND c.dropped_at IS NULL
  AND c.upper_anchor + r.duration <= statement_timestamp()
ORDER BY c.lower_anchor
"""


@dataclass(frozen=True, slots=True)
class HistoryLeafSwept:
    """One expired task-history leaf's sweep result: the detach-side
    outcome and, when the leaf reached the detached state, the drop
    outcome."""

    leaf_name: str
    class_key: str
    detach: LeafInspection | QuarantineRefused
    drop: LeafDrop | None


@dataclass(frozen=True, slots=True)
class PrunePass:
    """One pruning pass's accounting, health-surface-ready.

    `refusals` are typed outcomes that skipped a leaf, rendered as
    `leaf: OutcomeName`; `errors` are operational failures the pass
    contained. Both leave the leaf as the next pass's candidate.
    """

    finalized_leaves: tuple[str, ...]
    heartbeat_swept: tuple[HeartbeatLeafSwept, ...]
    history_swept: tuple[HistoryLeafSwept, ...]
    refusals: tuple[str, ...]
    errors: tuple[str, ...]

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


async def _expired_candidates(
    connection: AsyncConnection,
    statement: str,
    parameters: dict[str, str],
) -> tuple[LeafRef, ...]:
    rows = (await connection.execute(text(statement), parameters)).all()
    return tuple(
        LeafRef(
            leaf_name=row.leaf_name,
            class_key=row.class_key,
            bounds=LeafBounds(lower=row.lower_anchor, upper=row.upper_anchor),
        )
        for row in rows
    )


async def _finalize_interrupted_detaches(
    engine: AsyncEngine,
    publisher: LoaderPublication,
) -> tuple[tuple[str, ...], tuple[str, ...], tuple[str, ...]]:
    """Finalize every interrupted detach among expired candidates.

    Returns (finalized leaf names, refusal strings, error strings).
    A blocked finalization is a refusal — the manager returns the leaf
    re-inspected and it stays pending until its blockers drain.
    """
    async with engine.connect() as connection:
        candidates = await _expired_candidates(
            connection, EXPIRED_FINITE_LEAVES_SQL, {}
        )
        interrupted: list[LeafRef] = []
        for ref in candidates:
            inspection = await inspect_leaf(
                connection, InspectHistoryLeaf(leaf=ref)
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
                engine,
                FinalizeInterruptedLeafDetach(
                    leaf=ref,
                    statement_timeout_ms=DETACH_STATEMENT_TIMEOUT_MS,
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
    return tuple(finalized), tuple(refusals), tuple(errors)


async def sweep_expired_history_leaves(
    engine: AsyncEngine,
    publisher: LoaderPublication,
) -> tuple[tuple[HistoryLeafSwept, ...], tuple[str, ...]]:
    """Detach and drop every finite task-history leaf past its horizon.

    Mirrors the heartbeat sweep's sequence per leaf — detach, re-inspect
    for the crashed-between-detach-and-drop state, drop — with two
    driver-grade additions: a statement timeout on the concurrent
    detach, and per-leaf error containment so one leaf's failure never
    stops the pass. Returns (swept entries, error strings).
    """
    async with engine.connect() as connection:
        candidates = await _expired_candidates(
            connection,
            EXPIRED_HISTORY_LEAVES_SQL,
            {'heartbeat_class_key': HEARTBEAT_CLASS_KEY},
        )
    swept: list[HistoryLeafSwept] = []
    errors: list[str] = []
    for ref in candidates:
        try:
            detach_outcome = await detach_expired_leaf(
                engine,
                DetachExpiredHistoryLeaf(
                    leaf=ref,
                    quarantine_horizon=None,
                    statement_timeout_ms=DETACH_STATEMENT_TIMEOUT_MS,
                ),
                publisher,
            )
            drop_outcome: LeafDrop | None = None
            detached = isinstance(detach_outcome, LeafDetached)
            if not detached:
                # A crash between detach and drop leaves a detached leaf
                # the next sweep must still drop; re-inspect for that
                # state.
                async with engine.connect() as connection:
                    inspection = await inspect_leaf(
                        connection, InspectHistoryLeaf(leaf=ref)
                    )
                detached = isinstance(inspection, LeafDetached)
            if detached:
                async with engine.begin() as connection:
                    drop_outcome = await drop_detached_leaf(
                        connection,
                        DropDetachedHistoryLeaf(leaf=ref),
                        publisher,
                    )
            swept.append(
                HistoryLeafSwept(
                    leaf_name=ref.leaf_name,
                    class_key=ref.class_key,
                    detach=detach_outcome,
                    drop=drop_outcome,
                )
            )
        except SQLAlchemyError as error:
            errors.append(f'{ref.leaf_name}: {error}')
    return tuple(swept), tuple(errors)


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
    engine: AsyncEngine,
    publisher: LoaderPublication,
) -> PrunePass:
    """One pruning pass: finalize interrupted detaches, then both sweeps.

    The finalize phase runs first across every finite class because one
    pending finalization blocks all further detaches on its parent. The
    heartbeat sweep runs as the tested unit it is; a failure inside it
    is contained as one pass error and the task-history sweep still
    runs, with per-leaf containment of its own.
    """
    finalized, finalize_refusals, errors_list = (
        await _finalize_interrupted_detaches(engine, publisher)
    )
    errors: list[str] = list(errors_list)
    heartbeat_swept: tuple[HeartbeatLeafSwept, ...] = ()
    try:
        heartbeat_swept = await sweep_expired_heartbeat_leaves(
            engine, publisher
        )
    except SQLAlchemyError as error:
        errors.append(f'heartbeat sweep: {error}')
    history_swept, history_errors = await sweep_expired_history_leaves(
        engine, publisher
    )
    errors.extend(history_errors)
    return PrunePass(
        finalized_leaves=finalized,
        heartbeat_swept=heartbeat_swept,
        history_swept=history_swept,
        refusals=(
            *finalize_refusals,
            *_sweep_refusals(heartbeat_swept, history_swept),
        ),
        errors=tuple(errors),
    )
