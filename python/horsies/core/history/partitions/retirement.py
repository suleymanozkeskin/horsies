"""Retention retirement of PERIOD and HEARTBEAT leaves through the baseline functions.

The functions of ``0006_history_functions.sql`` hold the rule; this module
sends one statement per function and parses its row:

- ``horsies_prune_candidates(p_limit)``: the expired leaves of one pass and
  their attachment, in one read;
- ``horsies_begin_leaf_detach(p_leaf_name)``: the inspection, the session
  leaf lock (for ``DETACHABLE`` and ``DETACH_INTERRUPTED`` only) and the
  inspection again under the lock;
- ``horsies_record_leaf_detached(p_leaf_name)``: the catalog record of a
  detach, and whether the published reader manifest still names the leaf;
- ``horsies_drop_detached_leaf(p_leaf_name)``: the drop, inside the
  caller's transaction.

The ``ALTER TABLE ... DETACH PARTITION ... CONCURRENTLY | FINALIZE``
statement stays with the caller: it cannot run in a function. A row
outside the documented values raises ``HistoryContractError``.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Any, Final

from sqlalchemy import text
from sqlalchemy.engine import Row
from sqlalchemy.ext.asyncio import AsyncConnection

from ..commands import LeafRef, PruneBounds
from ..errors import HistoryContractError
from ..outcomes import (
    CatalogConflictKind,
    DropRefusedLoaderReferences,
    LeafAttachment,
    LeafCatalogConflict,
    LeafDetachable,
    LeafDetached,
    LeafDetachInterrupted,
    LeafDrop,
    LeafDropped,
    LeafInspection,
    LeafMaintenanceBusy,
    LeafMissing,
    LeafNotExpired,
    LeafPendingBlocked,
    PruneReach,
)
from .catalog import decode_leaf_bounds, optional_utc_timestamp, required_utc_timestamp
from .locks import unlock_leaf_for_session

_PRUNE_CANDIDATES_SQL: Final = text(
    'SELECT leaf_name, class_key, class_kind, lower_anchor, upper_anchor, attachment '
    'FROM horsies_prune_candidates(:limit)'
)
_BEGIN_LEAF_DETACH_SQL: Final = text(
    'SELECT outcome, class_key, parent_name, lower_anchor, upper_anchor, expires_at, '
    'blocker_count, attachment, conflict_kind '
    'FROM horsies_begin_leaf_detach(:leaf_name)'
)
_RECORD_LEAF_DETACHED_SQL: Final = text(
    'SELECT outcome, manifest_names_leaf FROM horsies_record_leaf_detached(:leaf_name)'
)
_DROP_DETACHED_LEAF_SQL: Final = text(
    'SELECT outcome, class_key, parent_name, lower_anchor, upper_anchor, expires_at, '
    'blocker_count, attachment, conflict_kind '
    'FROM horsies_drop_detached_leaf(:leaf_name)'
)


# ---------------------------------------------------------------------------
# Candidates
# ---------------------------------------------------------------------------


class RetiredClassKind(Enum):
    """The class kinds that retention detaches and drops."""

    PERIOD = 'PERIOD'
    HEARTBEAT = 'HEARTBEAT'


class CandidateAttachment(Enum):
    """Where an expired leaf stands relative to its parent at the candidate read."""

    ATTACHED = 'ATTACHED'
    DETACH_PENDING = 'DETACH_PENDING'
    """A concurrent detach is pending finalization."""
    DETACHED = 'DETACHED'
    """The relation exists and has no parent."""
    MISSING = 'MISSING'
    """No relation has the leaf's name."""


@dataclass(frozen=True, slots=True)
class PruneCandidate:
    """One expired leaf of one pruning pass."""

    leaf: LeafRef
    class_kind: RetiredClassKind
    attachment: CandidateAttachment


@dataclass(frozen=True, slots=True)
class PruneCandidates:
    """The candidates of one pass, oldest first, and whether the read was truncated."""

    leaves: tuple[PruneCandidate, ...]
    reach: PruneReach

    def of_kind(self, kind: RetiredClassKind) -> tuple[LeafRef, ...]:
        """The candidate leaves of one class kind, in candidate order."""
        return tuple(
            candidate.leaf for candidate in self.leaves if candidate.class_kind is kind
        )


def _enum_value[E: Enum](kind: type[E], value: Any, column: str) -> E:
    """The member of `kind` whose value is `value`; anything else raises."""
    for member in kind:
        if member.value == value:
            return member
    raise HistoryContractError(f'{column} {value!r} is not a {kind.__name__} value')


async def read_prune_candidates(
    connection: AsyncConnection, bounds: PruneBounds
) -> PruneCandidates:
    """The oldest expired PERIOD and HEARTBEAT leaves, at most `bounds.candidates`.

    One statement: ``horsies_prune_candidates(bounds.candidates + 1)``; a
    row beyond the bound makes the read `TRUNCATED`. Reads only. A database
    error raises.
    """
    rows = (
        await connection.execute(_PRUNE_CANDIDATES_SQL, {'limit': bounds.candidates + 1})
    ).all()
    return PruneCandidates(
        leaves=tuple(
            PruneCandidate(
                leaf=LeafRef(
                    leaf_name=row.leaf_name,
                    class_key=row.class_key,
                    bounds=decode_leaf_bounds(row.lower_anchor, row.upper_anchor),
                ),
                class_kind=_enum_value(RetiredClassKind, row.class_kind, 'class_kind'),
                attachment=_enum_value(CandidateAttachment, row.attachment, 'attachment'),
            )
            for row in rows[: bounds.candidates]
        ),
        reach=(
            PruneReach.TRUNCATED
            if len(rows) > bounds.candidates
            else PruneReach.WITHIN_BOUNDS
        ),
    )


# ---------------------------------------------------------------------------
# Inspection rows
# ---------------------------------------------------------------------------


_CONFLICT_KINDS: Final = {
    'RELATION_WITHOUT_CATALOG': CatalogConflictKind.RELATION_WITHOUT_CATALOG,
    'OPEN_LOWER_BOUND': CatalogConflictKind.OPEN_LOWER_BOUND,
    'DROPPED_RELATION_EXISTS': CatalogConflictKind.DROPPED_RELATION_EXISTS,
    'FOREIGN_PARENT': CatalogConflictKind.FOREIGN_PARENT,
    'DETACH_RECORDED_WHILE_ATTACHED': CatalogConflictKind.DETACH_RECORDED_WHILE_ATTACHED,
    'PHYSICAL_NONCONFORMANT': CatalogConflictKind.PHYSICAL_NONCONFORMANT,
}

_CONFLICT_DETAILS: Final = {
    CatalogConflictKind.RELATION_WITHOUT_CATALOG: (
        'relation exists but the leaf catalog has no row for it'
    ),
    CatalogConflictKind.OPEN_LOWER_BOUND: 'a finite leaf has an open lower bound',
    CatalogConflictKind.DROPPED_RELATION_EXISTS: (
        'the catalog recorded a drop and the relation exists'
    ),
    CatalogConflictKind.FOREIGN_PARENT: 'the leaf is attached to another parent',
    CatalogConflictKind.DETACH_RECORDED_WHILE_ATTACHED: (
        'the catalog recorded a detach and the leaf is attached'
    ),
    CatalogConflictKind.PHYSICAL_NONCONFORMANT: (
        'attached leaf bound or task-ID index disagrees with catalog'
    ),
}

_ATTACHMENTS: Final = {
    'ATTACHED': LeafAttachment.ATTACHED,
    'DETACH_PENDING': LeafAttachment.DETACH_INTERRUPTED,
    'DETACHED': LeafAttachment.DETACHED,
}


def _conflict(leaf_name: str, value: Any) -> LeafCatalogConflict:
    kind = _CONFLICT_KINDS.get(value) if isinstance(value, str) else None
    if kind is None:
        raise HistoryContractError(
            f'leaf {leaf_name}: conflict_kind {value!r} is not a known conflict kind'
        )
    return LeafCatalogConflict(leaf_name=leaf_name, kind=kind, detail=_CONFLICT_DETAILS[kind])


def _blocker_count(leaf_name: str, value: Any) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 1:
        raise HistoryContractError(
            f'leaf {leaf_name}: blocker_count {value!r} is not a positive integer'
        )
    return value


def _attachment(leaf_name: str, value: Any) -> LeafAttachment:
    attachment = _ATTACHMENTS.get(value) if isinstance(value, str) else None
    if attachment is None:
        raise HistoryContractError(
            f'leaf {leaf_name}: attachment {value!r} is not ATTACHED, DETACH_PENDING or DETACHED'
        )
    return attachment


@dataclass(frozen=True, slots=True)
class InspectionColumns:
    """The inspection columns of a ``horsies_begin_leaf_detach`` or
    ``horsies_drop_detached_leaf`` row, as the driver decoded them."""

    expires_at: object
    blocker_count: object
    attachment: object
    conflict_kind: object


def _inspection_columns(row: Row[Any]) -> InspectionColumns:
    return InspectionColumns(
        expires_at=row.expires_at,
        blocker_count=row.blocker_count,
        attachment=row.attachment,
        conflict_kind=row.conflict_kind,
    )


def inspection_of(leaf_name: str, row: InspectionColumns, state: str) -> LeafInspection:
    """The outcome type of one ``horsies_inspect_history_leaf`` state.

    A state, conflict kind, attachment or blocker count outside the
    documented values raises ``HistoryContractError``.
    """

    def expires_at() -> datetime:
        return required_utc_timestamp(row.expires_at, 'expires_at')

    match state:
        case 'NOT_CATALOGED':
            return LeafMissing(leaf_name=leaf_name, cataloged=False, expires_at=None)
        case 'CATALOG_CONFLICT':
            return _conflict(leaf_name, row.conflict_kind)
        case 'ALREADY_DROPPED':
            return LeafDropped(leaf_name=leaf_name)
        case 'MISSING':
            return LeafMissing(
                leaf_name=leaf_name,
                cataloged=True,
                expires_at=optional_utc_timestamp(row.expires_at, 'expires_at'),
            )
        case 'PENDING_BLOCKED':
            return LeafPendingBlocked(
                leaf_name=leaf_name,
                blocker_count=_blocker_count(leaf_name, row.blocker_count),
                expires_at=expires_at(),
                attachment=_attachment(leaf_name, row.attachment),
            )
        case 'DETACHED':
            return LeafDetached(leaf_name=leaf_name, expires_at=expires_at())
        case 'DETACH_INTERRUPTED':
            return LeafDetachInterrupted(leaf_name=leaf_name, expires_at=expires_at())
        case 'NOT_EXPIRED':
            return LeafNotExpired(leaf_name=leaf_name, expires_at=expires_at())
        case 'DETACHABLE':
            return LeafDetachable(leaf_name=leaf_name, expires_at=expires_at())
        case _:
            raise HistoryContractError(
                f'leaf {leaf_name}: inspection state {state!r} is not a known state'
            )


def _catalog_facet_mismatch(leaf: LeafRef, row: Row[Any]) -> str | None:
    """Why the row's class and bounds are not the ones `leaf` names, or None.

    A function row carries the catalog's facets of the leaf name (NULL
    when the catalog has no row). Every caller builds its `LeafRef` from
    that catalog row, and the catalog never reuses a leaf name, so a
    difference is a caller defect.
    """
    if row.class_key is None:
        return None
    catalog_bounds = decode_leaf_bounds(row.lower_anchor, row.upper_anchor)
    if row.class_key == leaf.class_key and catalog_bounds == leaf.bounds:
        return None
    return (
        f'leaf {leaf.leaf_name}: the caller names class {leaf.class_key!r} and bounds '
        f'{leaf.bounds}, the catalog row has class {row.class_key!r} and bounds '
        f'{catalog_bounds}'
    )


# ---------------------------------------------------------------------------
# Detach
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class DetachLocked:
    """The session holds the leaf lock; the leaf is ready for its detach statement."""

    parent_name: str
    inspection: LeafDetachable | LeafDetachInterrupted


type BeginDetach = DetachLocked | LeafInspection | LeafMaintenanceBusy
"""`DetachLocked` holds the session lock; every other outcome holds none."""

_LOCKED_OUTCOMES: Final = frozenset({'DETACHABLE', 'DETACH_INTERRUPTED'})
"""The begin outcomes after which the session holds the leaf lock."""


async def begin_leaf_detach(connection: AsyncConnection, leaf: LeafRef) -> BeginDetach:
    """Inspect `leaf` and take its session lock when it is ready to detach or finalize.

    One statement: ``horsies_begin_leaf_detach``. A database error raises;
    the function releases a lock it took before the error propagates. A
    caller defect (a `LeafRef` whose class or bounds differ from the
    catalog) releases the lock the call took and raises
    ``HistoryContractError``.
    """
    row = (
        await connection.execute(_BEGIN_LEAF_DETACH_SQL, {'leaf_name': leaf.leaf_name})
    ).one()
    mismatch = _catalog_facet_mismatch(leaf, row)
    if mismatch is not None:
        if row.outcome in _LOCKED_OUTCOMES:
            await unlock_leaf_for_session(
                connection,
                class_key=row.class_key,
                anchor=required_utc_timestamp(row.lower_anchor, 'lower_anchor'),
            )
        raise HistoryContractError(mismatch)
    match row.outcome:
        case 'BUSY':
            return LeafMaintenanceBusy(leaf_name=leaf.leaf_name)
        case state:
            inspection = inspection_of(leaf.leaf_name, _inspection_columns(row), str(state))
    match inspection:
        case LeafDetachable() | LeafDetachInterrupted():
            if not isinstance(row.parent_name, str):
                raise HistoryContractError(
                    f'leaf {leaf.leaf_name}: a locked detach row has no parent name'
                )
            return DetachLocked(parent_name=row.parent_name, inspection=inspection)
        case _:
            return inspection


class DetachRecord(Enum):
    """The outcome of ``horsies_record_leaf_detached``."""

    RECORDED = 'RECORDED'
    ALREADY_RECORDED = 'ALREADY_RECORDED'
    STILL_ATTACHED = 'STILL_ATTACHED'
    MISSING = 'MISSING'
    NOT_CATALOGED = 'NOT_CATALOGED'


@dataclass(frozen=True, slots=True)
class DetachRecorded:
    """One detach record and whether the published manifest still names the leaf."""

    record: DetachRecord
    manifest_names_leaf: bool


async def record_leaf_detached(
    connection: AsyncConnection, leaf_name: str
) -> DetachRecorded:
    """Record a detach in the catalog, inside the caller's transaction.

    One statement: ``horsies_record_leaf_detached``. When the result says
    the manifest names the leaf, the caller publishes the readers in the
    same transaction. A database error raises.
    """
    row = (
        await connection.execute(_RECORD_LEAF_DETACHED_SQL, {'leaf_name': leaf_name})
    ).one()
    if not isinstance(row.manifest_names_leaf, bool):
        raise HistoryContractError(
            f'leaf {leaf_name}: manifest_names_leaf {row.manifest_names_leaf!r} is not a boolean'
        )
    return DetachRecorded(
        record=_enum_value(DetachRecord, row.outcome, 'detach record outcome'),
        manifest_names_leaf=row.manifest_names_leaf,
    )


# ---------------------------------------------------------------------------
# Drop
# ---------------------------------------------------------------------------


async def drop_detached_retention_leaf(
    connection: AsyncConnection, leaf: LeafRef
) -> LeafDrop:
    """Drop one detached PERIOD or HEARTBEAT leaf inside the caller's transaction.

    One statement: ``horsies_drop_detached_leaf``. The drop, the delete of
    the clock exceptions of its interval and the catalog update commit
    with the caller. A leaf that the published manifest names is
    `DropRefusedLoaderReferences`; a held leaf or relation lock is
    `LeafMaintenanceBusy`; any other state than detached is returned
    unchanged. A database error raises. A caller defect (a `LeafRef` whose
    class or bounds differ from the catalog) raises
    ``HistoryContractError``, so the caller's transaction rolls the drop
    back.
    """
    row = (
        await connection.execute(_DROP_DETACHED_LEAF_SQL, {'leaf_name': leaf.leaf_name})
    ).one()
    mismatch = _catalog_facet_mismatch(leaf, row)
    if mismatch is not None:
        raise HistoryContractError(mismatch)
    match row.outcome:
        case 'DROPPED':
            return LeafDropped(leaf_name=leaf.leaf_name)
        case 'BUSY':
            return LeafMaintenanceBusy(leaf_name=leaf.leaf_name)
        case 'REFERENCED_BY_MANIFEST':
            return DropRefusedLoaderReferences(leaf_name=leaf.leaf_name)
        case state:
            return inspection_of(leaf.leaf_name, _inspection_columns(row), str(state))
