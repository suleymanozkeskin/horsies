"""Typed vocabulary for the replacement-transcode executor.

Five stages, one job per archive component: plan, bounded committing
copy, full verification before any lock, the non-queuing binding swap,
and finalize. Every refusal is an outcome; only contract violations
raise. The exhaustion outcome carries the blocking-session diagnostic
payload — NEW surface, not qualified conformance: the retry contract is
qualified, the diagnostics are a recorded requirement. Two rules govern
that payload: the current-query text is truncated to a declared bound,
and diagnostics NEVER mask the outcome — capture is best-effort inside
its own guard, and a capture failure returns the exhaustion outcome
with the capture-failed marker set, never replaced.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Final


BLOCKER_QUERY_TRUNCATION_CHARS: Final = 1024
"""The declared bound on captured query text; never unbounded."""

SWAP_LOCK_ATTEMPTS_MAXIMUM: Final = 120
SWAP_RETRY_BACKOFF_SECONDS: Final = 0.25
SWAP_LOCK_SECONDS_MAXIMUM: Final = 2.0
MAINTENANCE_SECONDS_MAXIMUM: Final = 600.0
"""The qualified ceilings, unchanged: at most 120 non-queuing attempts
at a fixed 250 ms backoff, inside a 2 s locked window per successful
attempt and a 600 s complete job."""


class ArchiveComponent(Enum):
    """The four transcodable archive components."""

    HISTORY_ROW = 'HISTORY_ROW'
    RESULT = 'RESULT'
    ATTEMPTS = 'ATTEMPTS'
    RERUN_INPUT = 'RERUN_INPUT'


class TranscodeJobState(Enum):
    PLANNED = 'PLANNED'
    COPYING = 'COPYING'
    COPIED = 'COPIED'
    VERIFIED = 'VERIFIED'
    SWAPPED = 'SWAPPED'
    COMPLETE = 'COMPLETE'


class TranscodeCopyRejectionKind(Enum):
    SOURCE_CORRUPT = 'SOURCE_CORRUPT'
    SOURCE_SET_CHANGED = 'SOURCE_SET_CHANGED'


class SwapLockMode(Enum):
    """Parents lock ACCESS EXCLUSIVE; leaves lock SHARE — both NOWAIT."""

    PARENT = 'ACCESS_EXCLUSIVE'
    LEAVES = 'SHARE'


@dataclass(frozen=True, slots=True)
class TranscodePlan:
    """A durable, reversible job: budgets forward and backward."""

    job_id: str
    component: ArchiveComponent
    source_version: int
    target_version: int
    transformed_rows: int
    copied_rows: int
    payload_bytes: int
    projected_payload_bytes: int
    affected_relation_bytes: int
    relation_count: int
    peak_additional_disk_budget_bytes: int
    wal_budget_bytes: int
    rollback_wal_budget_bytes: int
    rollback_peak_additional_disk_budget_bytes: int
    reversible: bool


@dataclass(frozen=True, slots=True)
class TranscodePlanRejected:
    """Planning refused; nothing was written."""

    component: ArchiveComponent
    reason: str
    affected_rows: int


type TranscodePlanOutcome = TranscodePlan | TranscodePlanRejected


@dataclass(frozen=True, slots=True)
class TranscodeCopyBatch:
    """One committed batch; the cursor advanced durably."""

    job_id: str
    relation_ordinal: int
    batch_number: int
    rows_copied: int
    copied_rows_completed: int
    copied_rows_total: int


@dataclass(frozen=True, slots=True)
class TranscodeCopyRejected:
    """The copy refused; the job stops before verification."""

    job_id: str
    relation_ordinal: int
    kind: TranscodeCopyRejectionKind
    observed_rows: int


@dataclass(frozen=True, slots=True)
class TranscodeReadyForVerification:
    """Every relation's copy is complete and committed."""

    job_id: str
    copied_rows_total: int


@dataclass(frozen=True, slots=True)
class TranscodeLeafBusy:
    """Partition maintenance owns this source leaf; retry later."""

    job_id: str
    leaf_name: str


type TranscodeCopyOutcome = (
    TranscodeCopyBatch
    | TranscodeCopyRejected
    | TranscodeReadyForVerification
    | TranscodeLeafBusy
)


@dataclass(frozen=True, slots=True)
class TranscodeVerification:
    """Full content verification, committed BEFORE any lock.

    On the zero-mismatch path the target-validity scan never runs —
    validity follows from a validated source, a deterministic
    transformation, and exact equality; the scan exists only to
    classify a mismatch.
    """

    job_id: str
    verified: bool
    source_relations_changed: int
    replacement_row_mismatches: int
    invalid_target_rows: int
    copied_rows_total: int
    wal_bytes: int | None


type TranscodeVerificationOutcome = TranscodeVerification | TranscodeLeafBusy


@dataclass(frozen=True, slots=True)
class TranscodeSwap:
    """Bindings swapped inside the locked window; identity re-checked,
    content never rescanned there."""

    job_id: str
    relations_swapped: int


@dataclass(frozen=True, slots=True)
class SwapBlocker:
    """One session observed blocking the final failed attempt.

    `query` is truncated to `BLOCKER_QUERY_TRUNCATION_CHARS`; every
    field beyond `pid` is best-effort and may be None when the session
    vanished between lock conflict and capture.
    """

    pid: int
    state: str | None
    transaction_age_seconds: float | None
    wait_event: str | None
    query: str | None
    relation_name: str
    held_lock_mode: str
    granted: bool


@dataclass(frozen=True, slots=True)
class TranscodeSwapBusy:
    """One non-queuing attempt found the lock held; retry follows."""

    job_id: str
    lock_mode: SwapLockMode
    relation_names: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class TranscodeSwapExhausted:
    """Every attempt found the lock held; the job remains VERIFIED.

    `blockers` is the diagnostic payload captured ONCE at the final
    failure. `blocker_capture_failed` marks a best-effort capture that
    itself failed — the exhaustion outcome is returned regardless,
    because observability must never destroy the result it observes.
    """

    job_id: str
    lock_mode: SwapLockMode
    relation_names: tuple[str, ...]
    attempts: int
    retry_sleep_seconds: float
    blockers: tuple[SwapBlocker, ...] = ()
    blocker_capture_failed: bool = field(default=False)


type TranscodeSwapOutcome = (
    TranscodeSwap | TranscodeSwapBusy | TranscodeSwapExhausted
)


@dataclass(frozen=True, slots=True)
class TranscodeFinalized:
    """Decoder-retirement inventory recorded; the job is COMPLETE."""

    job_id: str
    retired_source_version: int
    decoder_retirement_ready: bool
