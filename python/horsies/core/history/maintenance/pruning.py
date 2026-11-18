"""Retention pruning: the maintenance tick's detach-and-drop pass.

Partition-drop retention has two halves: coverage creates leaves ahead
of writes, and pruning removes leaves whose class duration has elapsed.
This module owns the second half — one candidate read
(``horsies_prune_candidates``: the expired PERIOD and HEARTBEAT leaves
and their attachment), then the shared manager machinery per leaf. An
interrupted detach (attachment ``DETACH_PENDING``) is finalized before
anything else, because PostgreSQL refuses every further detach on a
parent while one is pending finalization; then each expired leaf is
detached and dropped. Forever is never a candidate: the candidate read
selects ``PERIOD`` and ``HEARTBEAT`` classes by kind.

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

from dataclasses import dataclass
from typing import assert_never

from sqlalchemy.exc import SQLAlchemyError

from horsies._core import CoreHandle, PruneFailed, PrunePassRecord

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
    sweep_heartbeat_leaves,
)
from ..outcomes import (
    LeafDetached,
    LeafDrop,
    LeafDropped,
    LeafInspection,
    LeafMaintenanceBusy,
    LeafSweep,
    PruneReach,
    counts_toward_prune_bound,
)
from ..partitions.manager import (
    detach_expired_leaf,
    drop_detached_leaf,
    finalize_interrupted_detach,
    inspect_leaf,
)
from ..partitions.publication import LoaderPublication
from ..partitions.retirement import (
    CandidateAttachment,
    PruneCandidates,
    RetiredClassKind,
    read_prune_candidates,
)

__all__ = [
    'DETACH_STATEMENT_TIMEOUT_MS',
    'HistoryLeafSwept',
    'PRUNE_BOUNDS',
    'PrunePass',
    'PrunePassSummary',
    'prune_expired_partitions',
    'prune_expired_partitions_in_core',
    'summary_of_pass',
    'sweep_expired_history_leaves',
]


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
class _Finalized:
    leaves: tuple[str, ...]
    refusals: tuple[str, ...]
    errors: tuple[str, ...]


async def _finalize_interrupted_detaches(
    database: PartitionMaintenanceDatabase,
    publisher: LoaderPublication,
    candidates: PruneCandidates,
) -> _Finalized:
    """Finalize every candidate whose concurrent detach is pending.

    A finalization that does not end detached is a refusal — for example a
    blocked leaf stays pending until its blockers drain. A database error
    of one leaf is reported as `finalize leaf: cause`. Finalizations do not
    count toward the acted-leaf bound.
    """
    finalized: list[str] = []
    refusals: list[str] = []
    errors: list[str] = []
    for candidate in candidates.leaves:
        match candidate.attachment:
            case CandidateAttachment.DETACH_PENDING:
                pass
            case (
                CandidateAttachment.ATTACHED
                | CandidateAttachment.DETACHED
                | CandidateAttachment.MISSING
            ):
                continue
        ref = candidate.leaf
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
        leaves=tuple(finalized), refusals=tuple(refusals), errors=tuple(errors)
    )


async def sweep_history_leaves(
    database: PartitionMaintenanceDatabase,
    publisher: LoaderPublication,
    leaves: tuple[LeafRef, ...],
    max_acted_leaves: int,
) -> LeafSweep[HistoryLeafSwept]:
    """Detach and drop expired task-history leaves, in the given order.

    Per leaf: detach, re-inspect for the crashed-between-detach-and-drop
    state, drop, with a statement timeout on the concurrent detach. The
    sweep stops when `max_acted_leaves` leaves count toward the bound
    (`counts_toward_prune_bound`); it is then `TRUNCATED`. A database
    error of one leaf is reported as `leaf: cause`, counts toward the
    bound, and the sweep goes on with the next leaf.
    """
    swept: list[HistoryLeafSwept] = []
    errors: list[str] = []
    acted = 0
    reach = PruneReach.WITHIN_BOUNDS
    for ref in leaves:
        if acted >= max_acted_leaves:
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


async def sweep_expired_history_leaves(
    database: PartitionMaintenanceDatabase,
    publisher: LoaderPublication,
    bounds: PruneBounds = PRUNE_BOUNDS,
) -> LeafSweep[HistoryLeafSwept]:
    """Read the candidates of one pass and sweep its task-history leaves.

    `TRUNCATED` when the candidate read or the leaf bound left expired
    leaves for the next pass. A database error of the candidate read
    raises.
    """
    async with database.connect() as connection:
        candidates = await read_prune_candidates(connection, bounds)
    sweep = await sweep_history_leaves(
        database,
        publisher,
        candidates.of_kind(RetiredClassKind.PERIOD),
        bounds.acted_leaves,
    )
    return LeafSweep(
        swept=sweep.swept,
        errors=sweep.errors,
        acted=sweep.acted,
        reach=_combined_reach(candidates.reach, sweep.reach),
    )


def _combined_reach(*reaches: PruneReach) -> PruneReach:
    """`TRUNCATED` when any part of a pass was truncated."""
    match [reach for reach in reaches if reach is PruneReach.TRUNCATED]:
        case []:
            return PruneReach.WITHIN_BOUNDS
        case _:
            return PruneReach.TRUNCATED


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
    """One pruning pass: one candidate read, finalize interrupted
    detaches, then the heartbeat and finite-history sweeps.

    The candidate read returns at most `bounds.candidates` expired leaves,
    oldest first. The finalize phase runs first across every finite class
    because one pending finalization blocks all further detaches on its
    parent. Both sweeps contain a database error per leaf: the leaf is
    reported and the sweep goes on with the next leaf. The sweeps act on
    at most `bounds.acted_leaves` leaves, heartbeat leaves first. A
    database error of the candidate read raises.
    """
    async with database.connect() as connection:
        candidates = await read_prune_candidates(connection, bounds)
    finalized = await _finalize_interrupted_detaches(database, publisher, candidates)
    heartbeat = await sweep_heartbeat_leaves(
        database,
        publisher,
        candidates.of_kind(RetiredClassKind.HEARTBEAT),
        bounds.acted_leaves,
    )
    history = await sweep_history_leaves(
        database,
        publisher,
        candidates.of_kind(RetiredClassKind.PERIOD),
        bounds.acted_leaves - heartbeat.acted,
    )
    return PrunePass(
        finalized_leaves=finalized.leaves,
        heartbeat_swept=heartbeat.swept,
        history_swept=history.swept,
        refusals=(
            *finalized.refusals,
            *_sweep_refusals(heartbeat.swept, history.swept),
        ),
        errors=(
            *finalized.errors,
            *(f'heartbeat {error}' for error in heartbeat.errors),
            *history.errors,
        ),
        reach=_combined_reach(candidates.reach, heartbeat.reach, history.reach),
    )


@dataclass(frozen=True, slots=True)
class PrunePassSummary:
    """The facts of one pruning pass that the reaper logs and keeps in its
    pruning health (``ReaperMixin._run_reaper_pass``).

    ``refusals`` and ``errors`` are the entries of ``PrunePass``: one per
    leaf, ``leaf: OutcomeName`` and ``leaf: cause``. ``detached_count``
    counts the swept leaves left detached, by this pass or an earlier one;
    ``dropped_count`` the swept leaves this pass dropped.
    """

    finalized_leaves: tuple[str, ...]
    detached_count: int
    dropped_count: int
    refusals: tuple[str, ...]
    errors: tuple[str, ...]
    reach: PruneReach

    @property
    def acted(self) -> bool:
        """Whether the pass has anything worth a log line. A swept leaf is
        either dropped or listed as a refusal, so this equals
        ``PrunePass.acted``."""
        return bool(
            self.finalized_leaves
            or self.dropped_count
            or self.refusals
            or self.errors
        )


def summary_of_pass(prune: PrunePass) -> PrunePassSummary:
    """The summary of one Python pass. Pure."""
    return PrunePassSummary(
        finalized_leaves=prune.finalized_leaves,
        detached_count=prune.detached_count,
        dropped_count=prune.dropped_count,
        refusals=prune.refusals,
        errors=prune.errors,
        reach=prune.reach,
    )


async def prune_expired_partitions_in_core(handle: CoreHandle) -> PrunePassSummary:
    """One pruning pass in the core (``CoreHandle.prune_expired_partitions_async``):
    the steps of ``prune_expired_partitions`` with ``StagedLoaderPublisher``
    on the session-work core pool of ``handle``, whose connections carry the
    TCP keepalives and the TCP user timeout of the config. The candidate
    read, the finalize phase, then the heartbeat and the period sweeps; each
    leaf's detach and drop commit alone, and a leaf's error or refusal is
    listed in the summary.

    Returns the summary of the core's record. Raises:

    - the record's ``CoreFailure`` (the core's class of the cause, with the
      subject ``prune_expired_partitions``) when the pass has no report (the
      connect, the client family check or the candidate read failed);
      nothing was detached or dropped;
    - ``CoreError`` when the core of this process cannot run the pass.

    A cancel of the caller does not stop the pass: the core runs it to its
    end, within its bound of acted leaves.
    """
    record = await handle.prune_expired_partitions_async()
    match record:
        case PrunePassRecord():
            return PrunePassSummary(
                finalized_leaves=tuple(record.finalized_leaves),
                detached_count=record.detached,
                dropped_count=record.dropped,
                refusals=tuple(record.refusals),
                errors=tuple(record.errors),
                reach=PruneReach(record.reach),
            )
        case PruneFailed(exception=exception):
            raise exception
        case _ as unreachable:
            assert_never(unreachable)
