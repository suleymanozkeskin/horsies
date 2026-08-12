"""What a history maintenance operation reports back.

Outcomes are data, and refusals are outcomes. A detach that finds the leaf
still blocked by pending recovery evidence has not failed — it has learned
which fact currently prevents retention progress, and the maintenance loop
needs that fact to schedule its next pass. Only a broken contract between
this code and the database raises (`horsies.core.history.errors`).

Each variant carries the evidence that justified its classification, so a
maintenance log line is reconstructible without re-reading the database:
a blocked leaf says how many locators pin it, a conflict says which facet
disagreed, an unexpired leaf says when it expires.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import Enum


# ---------------------------------------------------------------------------
# Leaf lifecycle classification
# ---------------------------------------------------------------------------


class CatalogConflictKind(Enum):
    """Which facet of leaf identity disagreed.

    A conflict is never auto-repaired: the catalog is the manager's memory,
    and a disagreement means either the caller is stale or someone changed
    the database behind the manager's back. Both need an operator, not a
    heuristic.
    """

    RELATION_WITHOUT_CATALOG = 'RELATION_WITHOUT_CATALOG'
    METADATA_MISMATCH = 'METADATA_MISMATCH'
    PHYSICAL_NONCONFORMANT = 'PHYSICAL_NONCONFORMANT'


@dataclass(frozen=True, slots=True)
class LeafDetachable:
    """Attached, conformant, expired, and unreferenced: detach may proceed."""

    leaf_name: str
    expires_at: datetime


@dataclass(frozen=True, slots=True)
class LeafNotExpired:
    """Attached and healthy; its retention window has not elapsed."""

    leaf_name: str
    expires_at: datetime


class LeafAttachment(Enum):
    """How a leaf currently relates to its parent."""

    ATTACHED = 'ATTACHED'
    DETACH_INTERRUPTED = 'DETACH_INTERRUPTED'
    DETACHED = 'DETACHED'


@dataclass(frozen=True, slots=True)
class LeafPendingBlocked:
    """Expiry alone does not release a leaf: recovery evidence pins it.

    The attachment facet distinguishes a blocked attached leaf (retry after
    the pending work drains or the detach horizon quarantines it) from one
    whose concurrent detach was interrupted while locators still reference
    it, and from a detached leaf that recovery evidence still points into —
    each needs different handling and none of them may be dropped.
    """

    leaf_name: str
    blocker_count: int
    expires_at: datetime
    attachment: LeafAttachment


@dataclass(frozen=True, slots=True)
class LeafDetachInterrupted:
    """A concurrent detach began and did not finish; FINALIZE is owed."""

    leaf_name: str
    expires_at: datetime


@dataclass(frozen=True, slots=True)
class LeafDetached:
    """Detached and standing alone; eligible for drop once unreferenced."""

    leaf_name: str
    expires_at: datetime


@dataclass(frozen=True, slots=True)
class LeafDropped:
    """The catalog remembers a leaf whose relation was dropped."""

    leaf_name: str


@dataclass(frozen=True, slots=True)
class LeafMissing:
    """No relation exists.

    `cataloged` is the difference between a leaf that was never created and
    one the catalog believes is attached — the second is a coverage hole the
    health contract must surface before a terminal transition finds it.
    """

    leaf_name: str
    cataloged: bool
    expires_at: datetime | None


@dataclass(frozen=True, slots=True)
class RetentionClassAbsent:
    """The named retention class has no metadata row."""

    class_key: str


@dataclass(frozen=True, slots=True)
class ForeverClassLeaf:
    """Finite-leaf maintenance was addressed at the forever class."""

    class_key: str


@dataclass(frozen=True, slots=True)
class LeafCatalogConflict:
    """Request, catalog, and relation do not tell one story."""

    leaf_name: str
    kind: CatalogConflictKind
    detail: str


type LeafInspection = (
    LeafDetachable
    | LeafNotExpired
    | LeafPendingBlocked
    | LeafDetachInterrupted
    | LeafDetached
    | LeafDropped
    | LeafMissing
    | RetentionClassAbsent
    | ForeverClassLeaf
    | LeafCatalogConflict
)
"""Exhaustive classification of one leaf's lifecycle state."""


# ---------------------------------------------------------------------------
# Creation
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class LeafCreated:
    """The leaf, its catalog row, and its task-ID index now exist."""

    leaf_name: str
    id_index_name: str


@dataclass(frozen=True, slots=True)
class LeafAlreadyConformant:
    """The leaf already exists and matches its catalog row exactly."""

    leaf_name: str


@dataclass(frozen=True, slots=True)
class LeafIndexRepaired:
    """The leaf existed but its cataloged task-ID index did not; it does now."""

    leaf_name: str
    id_index_name: str


@dataclass(frozen=True, slots=True)
class ClassIntervalMismatch:
    """The retention class does not use the requested partition interval."""

    class_key: str
    partition_interval_days: int | None


type LeafCreation = (
    LeafCreated
    | LeafAlreadyConformant
    | LeafIndexRepaired
    | RetentionClassAbsent
    | ForeverClassLeaf
    | ClassIntervalMismatch
    | LeafCatalogConflict
)
"""Outcome of one idempotent daily-leaf creation."""


# ---------------------------------------------------------------------------
# Drop
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class DropRefusedLoaderReferences:
    """The published staged loader still probes this leaf.

    Dropping now would turn lookups of rows the loader believes retained
    into errors. Regenerate and republish the loader first; the drop is
    refused, never forced.
    """

    leaf_name: str


type LeafDrop = LeafDropped | DropRefusedLoaderReferences | LeafInspection
"""Outcome of one drop attempt; non-detached inspections pass through."""


# ---------------------------------------------------------------------------
# Coverage and health
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class CoverageBelowFloor:
    """Fewer than two complete future leaf intervals remain attached.

    This is the red line the health contract fails at — early enough that
    create-ahead can run before any terminal transition needs the missing
    leaf.
    """

    class_key: str
    complete_future_intervals: int
    coverage_until: datetime | None


@dataclass(frozen=True, slots=True)
class MissingDdlPrivilege:
    """The current role cannot perform application-managed partition DDL."""

    schema_create: bool
    owns_parent: bool


@dataclass(frozen=True, slots=True)
class LeafNonconformant:
    """An attached leaf disagrees with its catalog row."""

    leaf_name: str
    kind: CatalogConflictKind
    detail: str


@dataclass(frozen=True, slots=True)
class DetachAwaitingFinalize:
    """An interrupted concurrent detach blocks further detaches on the parent."""

    leaf_name: str


type HealthFault = (
    CoverageBelowFloor
    | MissingDdlPrivilege
    | LeafNonconformant
    | DetachAwaitingFinalize
    | RetentionClassAbsent
)


@dataclass(frozen=True, slots=True)
class ClassCoverage:
    """Coverage arithmetic for one daily-partitioned history class."""

    class_key: str
    attached_leaf_count: int
    coverage_until: datetime | None
    complete_future_intervals: int
    detachable_leaf_count: int
    pending_blocked_leaf_count: int


@dataclass(frozen=True, slots=True)
class PartitionHealthReport:
    """One health pass over one retention class.

    Healthy means the fault tuple is empty. The report never averages or
    ranks faults; every fault is actionable on its own and the caller
    decides urgency.
    """

    class_key: str
    checked_at: datetime
    coverage: ClassCoverage | None
    faults: tuple[HealthFault, ...]

    @property
    def is_healthy(self) -> bool:
        return not self.faults
