"""Every history maintenance operation, as a typed command.

The vocabulary follows `horsies.core.lifecycle.commands`: a command variant
holds exactly the data its operation needs, states that cannot occur cannot
be written down, and the accepted type at each executor widens only as the
operation lands. Partition maintenance commands are the first family; read
and terminalization families join in their own build waves.

Bounds travel as a value object rather than loose datetimes, so a command
with inverted or naive bounds cannot be constructed at all — the constructor
is the validator, and every executor downstream may rely on it.
"""

from __future__ import annotations

from typing import Final

import re
from dataclasses import dataclass
from datetime import datetime, timedelta


_IDENTIFIER = re.compile(r'^[a-z][a-z0-9_]{0,62}$')


def is_safe_identifier(name: str) -> bool:
    """True when `name` may be interpolated as a PostgreSQL identifier."""
    return _IDENTIFIER.fullmatch(name) is not None


@dataclass(frozen=True, slots=True)
class LeafBounds:
    """Half-open `[lower, upper)` anchor bounds of one finite leaf.

    Construction rejects naive or non-increasing bounds; a `LeafBounds` that
    exists is well-formed everywhere it travels.
    """

    lower: datetime
    upper: datetime

    def __post_init__(self) -> None:
        if self.lower.tzinfo is None or self.upper.tzinfo is None:
            raise ValueError('leaf bounds must be timezone-aware')
        if self.lower >= self.upper:
            raise ValueError('leaf bounds must be increasing')

    @property
    def spans_one_day(self) -> bool:
        return self.upper - self.lower == timedelta(days=1)


@dataclass(frozen=True, slots=True)
class LeafRef:
    """One finite leaf, addressed by name, class, and anchor bounds.

    Every maintenance command targets a leaf through this reference; the
    executor verifies the catalog agrees with all three facets before acting,
    so a stale caller cannot detach or drop a leaf that has been reused.
    """

    leaf_name: str
    class_key: str
    bounds: LeafBounds

    def __post_init__(self) -> None:
        if not is_safe_identifier(self.leaf_name):
            raise ValueError('leaf name must be a safe PostgreSQL identifier')
        if not self.class_key:
            raise ValueError('class key must be non-empty')


# ---------------------------------------------------------------------------
# Partition maintenance commands
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class InspectHistoryLeaf:
    """Classify one leaf's lifecycle state without changing anything."""

    leaf: LeafRef


@dataclass(frozen=True, slots=True)
class CreateDailyHistoryLeaf:
    """Create one daily leaf, or verify and repair an existing one.

    Idempotent: an already-attached conformant leaf is an outcome, not an
    error, and a missing per-leaf task-ID index is recreated rather than
    reported. Bounds must span exactly one day; the retention class must be
    finite with a daily partition interval.
    """

    leaf: LeafRef

    def __post_init__(self) -> None:
        if not self.leaf.bounds.spans_one_day:
            raise ValueError('daily leaf bounds must span exactly one day')


@dataclass(frozen=True, slots=True)
class EnsureLeafCoverage:
    """Create every missing daily leaf from today through the horizon.

    `horizon_days` counts complete future daily intervals that must exist
    after this command succeeds, today's leaf excluded.
    """

    class_key: str
    horizon_days: int

    def __post_init__(self) -> None:
        if not self.class_key:
            raise ValueError('class key must be non-empty')
        if self.horizon_days < 2:
            raise ValueError(
                'coverage horizon below two future leaves is already the '
                'health-contract red line and cannot be a target'
            )


DETACH_STATEMENT_TIMEOUT_MS: Final = 5_000
"""Bound on a concurrent detach's wait for conflicting readers.

One open transaction stalls one leaf, never the maintenance tick; the
skipped leaf is the next pass's candidate. Defined here because both
sweeps need it and the pruning module already imports the heartbeat one.
"""


@dataclass(frozen=True, slots=True)
class DetachExpiredHistoryLeaf:
    """Concurrently detach one expired, unblocked leaf.

    Runs on a dedicated autocommit connection because
    `DETACH PARTITION ... CONCURRENTLY` cannot run inside a transaction
    block. The statement timeout bounds the wait on long readers.

    `quarantine_horizon` is stated at every call site: None means a
    pending locator on the leaf always refuses the detach; a duration
    means locators older than it (by the pending row's `created_at`) are
    first moved through the quarantine protocol — copy, verify,
    repoint — and only a leaf whose blockers all resolved proceeds to
    detach. A quarantine refusal keeps the leaf pinned.

    `statement_timeout_ms` carries no default. It once defaulted to
    None, and a call site that simply omitted it waited on a lock with
    no bound while holding the cluster-wide maintenance gate — the
    omission read as "nothing to say here" rather than as a choice.
    Stating it is now mandatory, exactly as `quarantine_horizon` is;
    None remains available and means unbounded, but only where someone
    wrote it.
    """

    leaf: LeafRef
    quarantine_horizon: timedelta | None
    statement_timeout_ms: int | None

    def __post_init__(self) -> None:
        if self.statement_timeout_ms is not None and self.statement_timeout_ms <= 0:
            raise ValueError('statement timeout must be positive')
        if self.quarantine_horizon is not None and self.quarantine_horizon <= timedelta(0):
            raise ValueError('quarantine horizon must be positive')


@dataclass(frozen=True, slots=True)
class FinalizeInterruptedLeafDetach:
    """Complete a concurrent detach that a crash left half-done.

    Must run before any further detach on the same parent; PostgreSQL
    refuses new detaches while one is pending finalization. That is what
    makes an unbounded wait here worse than a slow leaf: the statement
    that is stuck is also the statement blocking every future detach on
    the parent, and the pass holding it holds the cluster-wide
    maintenance gate.

    `statement_timeout_ms` carries no default for the same reason
    `quarantine_horizon` does not: unbounded is a decision, and a
    default lets a call site take it without making it. None still
    means unbounded, but only where someone wrote None.
    """

    leaf: LeafRef
    statement_timeout_ms: int | None

    def __post_init__(self) -> None:
        if self.statement_timeout_ms is not None and self.statement_timeout_ms <= 0:
            raise ValueError('statement timeout must be positive')


@dataclass(frozen=True, slots=True)
class DropDetachedHistoryLeaf:
    """Drop one already-detached leaf after the loader no longer sees it.

    Refused while the published staged loader still references the leaf:
    dropping a relation the loader probes would turn retained-row lookups
    into errors, and a regeneration gap must block the drop, not the reader.
    """

    leaf: LeafRef


@dataclass(frozen=True, slots=True)
class CollectPartitionHealth:
    """Report coverage, conformance, blockers, and privileges for one class.

    Read-only over the catalog and PostgreSQL catalogs; never locks task
    writers. Health is the gate that fails before a terminal transition
    finds missing coverage.

    `application_managed` states which DDL privilege mode this deployment
    runs: application-managed mode requires the current role to own
    partition DDL and reports a missing privilege as a fault; in
    operator-managed mode the application role is not expected to hold DDL
    privileges and none are checked. The caller states the mode explicitly
    because health cannot infer a deployment contract from database state.
    """

    class_key: str
    application_managed: bool

    def __post_init__(self) -> None:
        if not self.class_key:
            raise ValueError('class key must be non-empty')


type PartitionMaintenanceCommand = (
    InspectHistoryLeaf
    | CreateDailyHistoryLeaf
    | EnsureLeafCoverage
    | DetachExpiredHistoryLeaf
    | FinalizeInterruptedLeafDetach
    | DropDetachedHistoryLeaf
    | CollectPartitionHealth
)
