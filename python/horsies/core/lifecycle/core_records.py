"""The lifecycle outcomes of Python from the records of ``horsies._core``.

The attempt writes that run in the core (the failed attempt, the successes,
the workflow success) answer with one record class per outcome. The records
that several writes share are converted here into the outcome types of
``horsies.core.lifecycle.outcomes``, so the worker's callers keep their
matches.

Each record carries the row as the statement saw it under its locks, as
text (``observed_status``) and typed values. A text outside the Python
catalogs (a status, a terminalization kind, a guard) means that the core
and this client disagree on the contract: ``OutcomeDecodeError``.

A source-state conflict names its guard only: ``CLAIM`` keeps its evidence
in the observed columns (``ObservedClaim``); for every other guard the core
logged the evidence with the refusal (``EvidenceLoggedByCore``).
"""

from __future__ import annotations

from datetime import datetime
from typing import Final

import horsies._core as core
from horsies.core.lifecycle.operations import TerminalizationKind
from horsies.core.lifecycle.outcomes import (
    AlreadyApplied,
    EvidenceLoggedByCore,
    GuardEvidence,
    GuardKind,
    LostClaim,
    ObservedClaim,
    ObservedTaskState,
    OutcomeDecodeError,
    PhaseTwoOwed,
    PhaseTwoStep,
    SourceStateConflict,
    TaskAbsent,
)
from horsies.core.types.status import TaskStatus

CLAIM_GUARD: Final = 'CLAIM'
"""The guard text of a claim-shaped refusal: its evidence is the observed
worker and claim generation."""


def observed_of(
    status: str | None,
    worker_id: str | None,
    claimed_at: datetime | None,
) -> ObservedTaskState:
    """The observed row of a record. Pure.

    Raises:
        OutcomeDecodeError: ``status`` is not a task status.
    """
    match status:
        case None:
            task_status = None
        case str():
            try:
                task_status = TaskStatus(status)
            except ValueError as exc:
                raise OutcomeDecodeError(
                    f'observed_status is not a task status: {status!r}'
                ) from exc
    return ObservedTaskState(status=task_status, worker_id=worker_id, claimed_at=claimed_at)


def guard_evidence_of(guard: str, observed: ObservedTaskState) -> GuardEvidence:
    """The evidence of a source-state conflict that names ``guard``. Pure.

    Raises:
        OutcomeDecodeError: ``guard`` is neither ``CLAIM`` nor a guard kind.
    """
    if guard == CLAIM_GUARD:
        return ObservedClaim(worker_id=observed.worker_id, claimed_at=observed.claimed_at)
    try:
        return EvidenceLoggedByCore(guard=GuardKind(guard))
    except ValueError as exc:
        raise OutcomeDecodeError(f'guard is not a guard kind: {guard!r}') from exc


def terminalization_kind_of(kind: str) -> TerminalizationKind:
    """The terminalization kind of a record. Pure.

    Raises:
        OutcomeDecodeError: ``kind`` is not a terminalization kind.
    """
    try:
        return TerminalizationKind(kind)
    except ValueError as exc:
        raise OutcomeDecodeError(f'kind is not a terminalization kind: {kind!r}') from exc


def phase_two_owed_of(record: core.PhaseTwoOwedRecord) -> PhaseTwoOwed:
    """The phase 2 that a workflow task owes. Pure."""
    return PhaseTwoOwed(
        task_name=record.task_name,
        queue_name=record.queue_name,
        step=PhaseTwoStep(record.step),
    )


def already_applied_of(record: core.AttemptAlreadyApplied) -> AlreadyApplied:
    """A replay of the writer's committed effect. Pure."""
    return AlreadyApplied(
        task_id=str(record.task_id),
        ordinality=None,
        terminal_at=record.terminal_at,
        kind=terminalization_kind_of(record.kind),
        observed=observed_of(
            record.observed_status, record.observed_worker_id, record.observed_claimed_at,
        ),
    )


def lost_claim_of(record: core.AttemptLostClaim) -> LostClaim:
    """A live row that the fence does not match. Pure."""
    return LostClaim(
        task_id=str(record.task_id),
        ordinality=None,
        observed=observed_of(
            record.observed_status, record.observed_worker_id, record.observed_claimed_at,
        ),
    )


def source_state_conflict_of(record: core.AttemptSourceStateConflict) -> SourceStateConflict:
    """A row that is not the writer's to end. Pure."""
    observed = observed_of(
        record.observed_status, record.observed_worker_id, record.observed_claimed_at,
    )
    return SourceStateConflict(
        task_id=str(record.task_id),
        ordinality=None,
        observed=observed,
        evidence=guard_evidence_of(record.guard, observed),
    )


def task_absent_of(record: core.AttemptTaskAbsent) -> TaskAbsent:
    """No live or history row. Pure."""
    return TaskAbsent(task_id=str(record.task_id), ordinality=None)
