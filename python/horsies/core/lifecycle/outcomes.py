"""The outcome of a terminal write of the core, as the child reads it.

The core answers each write with one record class per outcome
(``horsies._core``); ``core_records.py`` converts the records that several
writes share into these types: ``Applied``, ``AlreadyApplied``,
``LostClaim``, ``SourceStateConflict`` and ``TaskAbsent``, each with the row
as the statement saw it under its locks (``ObservedTaskState``) and, for a
refusal, the evidence of its guard. A text outside these catalogs means that
the core and this client disagree on the contract: ``OutcomeDecodeError``.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from enum import Enum

from horsies._catalog.lifecycle_errors import OutcomeDecodeError as OutcomeDecodeError

from ..types.status import TaskStatus
from .operations import TerminalizationKind


class GuardKind(Enum):
    """Which guard's evidence a diagnostic payload carries.

    Absent (a NULL column) means the evidence is claim-shaped and lives in the
    uniform observed columns rather than in a payload.
    """

    DEADLINE = 'DEADLINE'
    STALENESS = 'STALENESS'
    WORKFLOW_STATUS = 'WORKFLOW_STATUS'
    WORKFLOW_LINK_ABSENT = 'WORKFLOW_LINK_ABSENT'
    WORKFLOW_LINK_STATE = 'WORKFLOW_LINK_STATE'
    FOREIGN_TERMINALIZATION = 'FOREIGN_TERMINALIZATION'
    WORKFLOW_TASK = 'WORKFLOW_TASK'
    RETRY_COUNT = 'RETRY_COUNT'


# ---------------------------------------------------------------------------
# Observed evidence
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class ObservedTaskState:
    """The locked pre-transition image, for every outcome alike.

    On an applied transition this is what the guarded update matched; on a
    refusal it is what it found instead. One rule rather than a per-outcome
    convention, so a log line means the same thing wherever it came from.
    """

    status: TaskStatus | None
    worker_id: str | None
    claimed_at: datetime | None


@dataclass(frozen=True, slots=True)
class ObservedClaim:
    """Claim-shaped evidence: the fence could not match this row."""

    worker_id: str | None
    claimed_at: datetime | None


@dataclass(frozen=True, slots=True)
class ObservedDeadline:
    """A deadline guard's evidence, as the database evaluated it."""

    good_until: datetime | None
    evaluated_at: datetime


@dataclass(frozen=True, slots=True)
class ObservedStaleness:
    """A staleness guard's evidence, captured in the snapshot that judged it.

    Every value the two arms compared travels together with the instant they
    were compared at, so both comparisons are reconstructible from the log
    exactly — an active finalizer's refusal is explained by `finalizing_at`,
    a live runner's by `last_heartbeat_at`, and `evaluated_at` is the NOW()
    the guard actually used.
    """

    last_heartbeat_at: datetime | None
    started_at: datetime | None
    finalizing_at: datetime | None
    stale_after_ms: int
    finalizing_stale_after_ms: int
    evaluated_at: datetime


@dataclass(frozen=True, slots=True)
class ObservedWorkflowState:
    """A workflow-status guard's evidence."""

    workflow_id: str
    workflow_status: str


@dataclass(frozen=True, slots=True)
class ObservedWorkflowLink:
    """A workflow-link guard's evidence; None means the link is gone."""

    node_status: str | None


@dataclass(frozen=True, slots=True)
class ObservedForeignTerminalization:
    """The row is terminal, but another operation put it there.

    Claim-shaped evidence would be all-NULL on a terminal row — precisely
    where the log has to name who won. A committed kind of None is a row
    written before the kind column existed: unknown provenance, never
    inferred.
    """

    observed_status: TaskStatus
    committed_kind: TerminalizationKind | None
    terminal_at: datetime | None


@dataclass(frozen=True, slots=True)
class ObservedWorkflowTask:
    """The task belongs to a workflow, live or finished, and the operation
    acts on plain tasks only. ``observed_status`` is the task's status."""

    observed_status: TaskStatus


@dataclass(frozen=True, slots=True)
class ObservedRetryCount:
    """A retry request was computed from ``expected_retry_count``, and the
    locked row holds ``retry_count``: another writer ended that attempt."""

    expected_retry_count: int
    retry_count: int


@dataclass(frozen=True, slots=True)
class EvidenceLoggedByCore:
    """A guard refused the write in the core. The core logged the guard's
    evidence with the refusal; its record names the guard only."""

    guard: GuardKind


type GuardEvidence = (
    ObservedClaim
    | ObservedDeadline
    | ObservedStaleness
    | ObservedWorkflowState
    | ObservedWorkflowLink
    | ObservedForeignTerminalization
    | ObservedWorkflowTask
    | ObservedRetryCount
    | EvidenceLoggedByCore
)


# ---------------------------------------------------------------------------
# Outcomes
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class Applied:
    """The transition committed. `terminal_at` and `kind` are the row's now."""

    task_id: str
    ordinality: int | None
    terminal_at: datetime
    kind: TerminalizationKind
    observed: ObservedTaskState


@dataclass(frozen=True, slots=True)
class AlreadyApplied:
    """This operation's own effect was already committed.

    Equivalent kind, not merely equal status: five operations write CANCELLED,
    and only a kind in the same class proves the coupled workflow-node write
    committed too.
    """

    task_id: str
    ordinality: int | None
    terminal_at: datetime
    kind: TerminalizationKind
    observed: ObservedTaskState


@dataclass(frozen=True, slots=True)
class LostClaim:
    """The row is live but this caller's fence cannot match it.

    Includes a row requeued to PENDING, whose claim fields are cleared: the
    generation that held it is gone, which is what the caller must act on.
    """

    task_id: str
    ordinality: int | None
    observed: ObservedTaskState


@dataclass(frozen=True, slots=True)
class SourceStateConflict:
    """The row exists and is not this operation's to end.

    Carries the guard's own evidence, so a refusal is diagnosable from the log
    without re-reading the row — by which time it has moved on.
    """

    task_id: str
    ordinality: int | None
    observed: ObservedTaskState
    evidence: GuardEvidence


@dataclass(frozen=True, slots=True)
class TaskAbsent:
    """No such row. Observed columns are empty because there was nothing to see."""

    task_id: str
    ordinality: int | None


type TerminalizationOutcome = (
    Applied | AlreadyApplied | LostClaim | SourceStateConflict | TaskAbsent
)
