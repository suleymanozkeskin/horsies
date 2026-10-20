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
from enum import Enum
from typing import Final

from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncConnection

from ..commands import (
    DETACH_STATEMENT_TIMEOUT_MS,
    DetachEmptyForeverLeaf,
    DetachExpiredHistoryLeaf,
    DropDetachedHistoryLeaf,
    FinalizeInterruptedLeafDetach,
    InspectHistoryLeaf,
    LeafLifecycle,
    LeafRef,
)
from ..cutover.state import cutover_complete
from ..ddl.tables import FOREVER_CLASS_KEY
from .database import PartitionMaintenanceDatabase
from .gate import ARCHIVE_MAINTENANCE_SESSIONS
from ..heartbeats.partitioning import (
    HeartbeatLeafSwept,
    sweep_expired_heartbeat_leaves,
)
from ..names import HEARTBEAT_CLASS_KEY, LEAF_CATALOG, RETENTION_CLASSES
from ..outcomes import (
    LeafDetachable,
    LeafDetached,
    LeafDetachInterrupted,
    LeafDrop,
    LeafDropped,
    LeafInspection,
    LeafKeptNonEmpty,
    LeafMaintenanceBusy,
)
from ..partitions.catalog import decode_leaf_bounds
from ..partitions.forever import FOREVER_LEGACY_LEAF
from ..partitions.manager import (
    detach_empty_forever_leaf,
    detach_expired_leaf,
    drop_detached_leaf,
    finalize_interrupted_detach,
    inspect_leaf,
)
from ..partitions.publication import LoaderPublication
from ..phase2.quarantine import QuarantineRefused

__all__ = [
    'DETACH_STATEMENT_TIMEOUT_MS',
    'MAX_FOREVER_DETACHES_PER_PASS',
    'ForeverLeafSwept',
    'ForeverSweep',
    'ForeverSweepFailed',
    'ForeverSweepRan',
    'ForeverSweepSkipReason',
    'ForeverSweepSkipped',
    'HistoryLeafSwept',
    'PrunePass',
    'prune_expired_partitions',
    'sweep_empty_forever_leaves',
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


MAX_FOREVER_DETACHES_PER_PASS: Final = 32
"""New detaches per pass. The candidate list is not bounded by it, so
non-empty old leaves cannot hide empty ones behind them."""

MAX_FOREVER_SWEEP_CANDIDATES: Final = 4_096
"""Upper bound of the closed daily `forever` candidate list."""

# Closed daily `forever` leaves, oldest first. The open-ended leaf and the
# pre-v35 legacy leaf are never candidates.
CLOSED_FOREVER_LEAVES_SQL: Final = f"""
SELECT c.leaf_name, c.lower_anchor, c.upper_anchor
FROM {LEAF_CATALOG} AS c
WHERE c.class_key = :forever_class_key
  AND c.dropped_at IS NULL
  AND c.leaf_name <> :legacy_leaf
  AND c.upper_anchor - c.lower_anchor = interval '1 day'
  AND c.upper_anchor <= date_trunc('day', statement_timestamp(), 'UTC')
ORDER BY c.lower_anchor
LIMIT :candidate_limit
"""

ARCHIVE_MAINTENANCE_ACTIVE_SQL: Final = f"""
SELECT EXISTS (
    SELECT 1 FROM {ARCHIVE_MAINTENANCE_SESSIONS} WHERE ended_at IS NULL
)
"""


class ForeverSweepSkipReason(Enum):
    """Why a pass did not sweep closed daily `forever` leaves."""

    CUTOVER_NOT_COMPLETE = 'cutover_not_complete'
    """Cutover relocation can still insert legacy rows into closed leaves."""

    ARCHIVE_MAINTENANCE_ACTIVE = 'archive_maintenance_active'
    """Transcode can still write replacement partitions."""


@dataclass(frozen=True, slots=True)
class ForeverLeafSwept:
    """What the sweep did with one closed daily `forever` leaf.

    `outcome` is the last typed outcome the leaf reached: `LeafDropped`
    when the sweep removed it, else the state or refusal that kept it.
    """

    leaf_name: str
    outcome: LeafDrop


@dataclass(frozen=True, slots=True)
class ForeverSweepRan:
    """One completed sweep. `kept_non_empty` names closed leaves that hold
    rows; they are never detached and are not an action. `errors` are
    per-leaf failures; each failed leaf is the next pass's candidate."""

    swept: tuple[ForeverLeafSwept, ...]
    kept_non_empty: tuple[str, ...]
    errors: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class ForeverSweepSkipped:
    """The pass did not inspect or change any `forever` leaf."""

    reason: ForeverSweepSkipReason


@dataclass(frozen=True, slots=True)
class ForeverSweepFailed:
    """The gate check or the discovery failed; no leaf was changed."""

    error: str


type ForeverSweep = ForeverSweepRan | ForeverSweepSkipped | ForeverSweepFailed


@dataclass(frozen=True, slots=True)
class HistoryLeafSwept:
    """One expired task-history leaf's sweep result: the detach-side
    outcome and, when the leaf reached the detached state, the drop
    outcome."""

    leaf_name: str
    class_key: str
    detach: LeafInspection | LeafMaintenanceBusy | QuarantineRefused
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
    forever_sweep: ForeverSweep
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
    def forever_swept(self) -> tuple[ForeverLeafSwept, ...]:
        match self.forever_sweep:
            case ForeverSweepRan(swept=swept):
                return swept
            case ForeverSweepSkipped() | ForeverSweepFailed():
                return ()

    @property
    def dropped_count(self) -> int:
        return sum(
            1
            for entry in (*self.heartbeat_swept, *self.history_swept)
            if isinstance(entry.drop, LeafDropped)
        ) + sum(
            1
            for entry in self.forever_swept
            if isinstance(entry.outcome, LeafDropped)
        )

    @property
    def acted(self) -> bool:
        """Whether the pass has anything worth a log line."""
        return bool(
            self.finalized_leaves
            or self.heartbeat_swept
            or self.history_swept
            or self.forever_swept
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
            bounds=decode_leaf_bounds(row.lower_anchor, row.upper_anchor),
        )
        for row in rows
    )


async def _finalize_interrupted_detaches(
    database: PartitionMaintenanceDatabase,
    publisher: LoaderPublication,
) -> tuple[tuple[str, ...], tuple[str, ...], tuple[str, ...]]:
    """Finalize every interrupted detach among expired candidates.

    Returns (finalized leaf names, refusal strings, error strings).
    A blocked finalization is a refusal — the manager returns the leaf
    re-inspected and it stays pending until its blockers drain.
    """
    async with database.connect() as connection:
        candidates = await _expired_candidates(
            connection, EXPIRED_FINITE_LEAVES_SQL, {}
        )
        interrupted: list[LeafRef] = []
        for ref in candidates:
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
    return tuple(finalized), tuple(refusals), tuple(errors)


async def sweep_expired_history_leaves(
    database: PartitionMaintenanceDatabase,
    publisher: LoaderPublication,
) -> tuple[tuple[HistoryLeafSwept, ...], tuple[str, ...]]:
    """Detach and drop every finite task-history leaf past its horizon.

    Mirrors the heartbeat sweep's sequence per leaf — detach, re-inspect
    for the crashed-between-detach-and-drop state, drop — with two
    driver-grade additions: a statement timeout on the concurrent
    detach, and per-leaf error containment so one leaf's failure never
    stops the pass. Returns (swept entries, error strings).
    """
    async with database.connect() as connection:
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
                database,
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
                async with database.connect() as connection:
                    inspection = await inspect_leaf(
                        connection,
                        InspectHistoryLeaf(
                            leaf=ref, lifecycle=LeafLifecycle.RETENTION
                        ),
                    )
                detached = isinstance(inspection, LeafDetached)
            if detached:
                async with database.begin() as connection:
                    drop_outcome = await drop_detached_leaf(
                        connection,
                        DropDetachedHistoryLeaf(
                            leaf=ref, lifecycle=LeafLifecycle.RETENTION
                        ),
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


class _ForeverAction(Enum):
    """What the sweep does with one closed daily `forever` leaf."""

    DETACH = 'detach'
    FINALIZE = 'finalize'
    DROP = 'drop'


@dataclass(frozen=True, slots=True)
class _ForeverCandidate:
    leaf: LeafRef
    action: _ForeverAction


@dataclass(frozen=True, slots=True)
class _ForeverPlan:
    actions: tuple[_ForeverCandidate, ...]
    refused: tuple[ForeverLeafSwept, ...]
    kept_non_empty: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class _ForeverSweepAllowed:
    """The gate allows the sweep."""


@dataclass(frozen=True, slots=True)
class _ForeverActed:
    swept: ForeverLeafSwept
    new_detach: bool


async def _forever_sweep_gate(
    connection: AsyncConnection,
) -> _ForeverSweepAllowed | ForeverSweepSkipped:
    """Skip the sweep before cutover completes or while archive
    maintenance is active: both can still write rows into closed leaves."""
    if not await cutover_complete(connection):
        return ForeverSweepSkipped(
            reason=ForeverSweepSkipReason.CUTOVER_NOT_COMPLETE
        )
    active = (
        await connection.execute(text(ARCHIVE_MAINTENANCE_ACTIVE_SQL))
    ).scalar_one()
    if active:
        return ForeverSweepSkipped(
            reason=ForeverSweepSkipReason.ARCHIVE_MAINTENANCE_ACTIVE
        )
    return _ForeverSweepAllowed()


async def _plan_forever_sweep(connection: AsyncConnection) -> _ForeverPlan:
    """Inspect every closed daily `forever` leaf on one autocommit
    connection, so no lock outlives its statement. Empty eligible leaves
    become detaches, interrupted detaches finalizations, detached leaves
    drops. Reads only."""
    rows = (
        await connection.execute(
            text(CLOSED_FOREVER_LEAVES_SQL),
            {
                'forever_class_key': FOREVER_CLASS_KEY,
                'legacy_leaf': FOREVER_LEGACY_LEAF,
                'candidate_limit': MAX_FOREVER_SWEEP_CANDIDATES,
            },
        )
    ).all()
    actions: list[_ForeverCandidate] = []
    refused: list[ForeverLeafSwept] = []
    kept: list[str] = []
    for row in rows:
        leaf = LeafRef(
            leaf_name=row.leaf_name,
            class_key=FOREVER_CLASS_KEY,
            bounds=decode_leaf_bounds(row.lower_anchor, row.upper_anchor),
        )
        inspection = await inspect_leaf(
            connection,
            InspectHistoryLeaf(
                leaf=leaf, lifecycle=LeafLifecycle.CLOSED_EMPTY_FOREVER
            ),
        )
        match inspection:
            case LeafKeptNonEmpty():
                kept.append(leaf.leaf_name)
            case LeafDetachable():
                actions.append(_ForeverCandidate(leaf, _ForeverAction.DETACH))
            case LeafDetachInterrupted():
                actions.append(_ForeverCandidate(leaf, _ForeverAction.FINALIZE))
            case LeafDetached():
                actions.append(_ForeverCandidate(leaf, _ForeverAction.DROP))
            case _:
                refused.append(ForeverLeafSwept(leaf.leaf_name, inspection))
    return _ForeverPlan(tuple(actions), tuple(refused), tuple(kept))


async def _drop_closed_forever_leaf(
    database: PartitionMaintenanceDatabase,
    leaf: LeafRef,
    publisher: LoaderPublication,
) -> LeafDrop:
    async with database.begin() as connection:
        return await drop_detached_leaf(
            connection,
            DropDetachedHistoryLeaf(
                leaf=leaf, lifecycle=LeafLifecycle.CLOSED_EMPTY_FOREVER
            ),
            publisher,
        )


async def _act_on_forever_leaf(
    database: PartitionMaintenanceDatabase,
    candidate: _ForeverCandidate,
    publisher: LoaderPublication,
) -> _ForeverActed:
    """Run one planned action, then drop the leaf once it is detached."""
    leaf = candidate.leaf
    match candidate.action:
        case _ForeverAction.DETACH:
            reached = await detach_empty_forever_leaf(
                database,
                DetachEmptyForeverLeaf(
                    leaf=leaf, statement_timeout_ms=DETACH_STATEMENT_TIMEOUT_MS
                ),
                publisher,
            )
            new_detach = isinstance(reached, LeafDetached)
        case _ForeverAction.FINALIZE:
            reached = await finalize_interrupted_detach(
                database,
                FinalizeInterruptedLeafDetach(
                    leaf=leaf,
                    statement_timeout_ms=DETACH_STATEMENT_TIMEOUT_MS,
                    lifecycle=LeafLifecycle.CLOSED_EMPTY_FOREVER,
                ),
                publisher,
            )
            new_detach = False
        case _ForeverAction.DROP:
            dropped = await _drop_closed_forever_leaf(database, leaf, publisher)
            return _ForeverActed(ForeverLeafSwept(leaf.leaf_name, dropped), False)
    match reached:
        case LeafDetached():
            dropped = await _drop_closed_forever_leaf(database, leaf, publisher)
            return _ForeverActed(
                ForeverLeafSwept(leaf.leaf_name, dropped), new_detach
            )
        case _:
            return _ForeverActed(ForeverLeafSwept(leaf.leaf_name, reached), False)


async def sweep_empty_forever_leaves(
    database: PartitionMaintenanceDatabase,
    publisher: LoaderPublication,
) -> ForeverSweep:
    """Detach and drop closed daily `forever` leaves that hold no rows,
    and finish interrupted detaches of such leaves.

    At most `MAX_FOREVER_DETACHES_PER_PASS` new detaches per pass; drops
    and finalizations are not bounded by it. A leaf with rows is kept and
    reported. A per-leaf failure is contained in `errors` and never stops
    the pass. It does not touch the open-ended leaf or the legacy leaf.
    """
    try:
        async with database.autocommit() as connection:
            gate = await _forever_sweep_gate(connection)
            match gate:
                case ForeverSweepSkipped():
                    return gate
                case _ForeverSweepAllowed():
                    pass
            plan = await _plan_forever_sweep(connection)
    except SQLAlchemyError as error:
        return ForeverSweepFailed(error=f'forever sweep discovery: {error}')
    swept: list[ForeverLeafSwept] = list(plan.refused)
    errors: list[str] = []
    detaches = 0
    for candidate in plan.actions:
        if (
            candidate.action is _ForeverAction.DETACH
            and detaches >= MAX_FOREVER_DETACHES_PER_PASS
        ):
            continue
        try:
            acted = await _act_on_forever_leaf(database, candidate, publisher)
        except SQLAlchemyError as error:
            errors.append(f'{candidate.leaf.leaf_name}: {error}')
            continue
        detaches += int(acted.new_detach)
        swept.append(acted.swept)
    return ForeverSweepRan(
        swept=tuple(swept),
        kept_non_empty=plan.kept_non_empty,
        errors=tuple(errors),
    )


def _forever_refusals_and_errors(
    sweep: ForeverSweep,
) -> tuple[tuple[str, ...], tuple[str, ...]]:
    """The sweep's non-drop outcomes as refusals, and its errors."""
    match sweep:
        case ForeverSweepRan(swept=swept, errors=errors):
            refusals = tuple(
                f'{entry.leaf_name}: {type(entry.outcome).__name__}'
                for entry in swept
                if not isinstance(entry.outcome, LeafDropped)
            )
            return refusals, errors
        case ForeverSweepSkipped():
            return (), ()
        case ForeverSweepFailed(error=error):
            return (), (error,)


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
) -> PrunePass:
    """One pruning pass: finalize interrupted detaches, then the heartbeat,
    finite-history and closed-empty-forever sweeps.

    The finalize phase runs first across every finite class because one
    pending finalization blocks all further detaches on its parent. The
    heartbeat sweep runs as the tested unit it is; a failure inside it
    is contained as one pass error and the task-history sweep still
    runs, with per-leaf containment of its own.
    """
    finalized, finalize_refusals, errors_list = (
        await _finalize_interrupted_detaches(database, publisher)
    )
    errors: list[str] = list(errors_list)
    heartbeat_swept: tuple[HeartbeatLeafSwept, ...] = ()
    try:
        heartbeat_swept = await sweep_expired_heartbeat_leaves(
            database, publisher
        )
    except SQLAlchemyError as error:
        errors.append(f'heartbeat sweep: {error}')
    history_swept, history_errors = await sweep_expired_history_leaves(
        database, publisher
    )
    errors.extend(history_errors)
    forever_sweep = await sweep_empty_forever_leaves(database, publisher)
    forever_refusals, forever_errors = _forever_refusals_and_errors(forever_sweep)
    errors.extend(forever_errors)
    return PrunePass(
        finalized_leaves=finalized,
        heartbeat_swept=heartbeat_swept,
        history_swept=history_swept,
        forever_sweep=forever_sweep,
        refusals=(
            *finalize_refusals,
            *_sweep_refusals(heartbeat_swept, history_swept),
            *forever_refusals,
        ),
        errors=tuple(errors),
    )
