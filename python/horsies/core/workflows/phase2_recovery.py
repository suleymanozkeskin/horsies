"""Outbox-driven phase-2 recovery: the crashed-worker case.

A worker that dies between phase 1 (terminalize the task) and phase 2
(advance its workflow node) leaves the node behind, and the phase-2 record
that the terminal move wrote. This module discovers the records and
recovers each one with ``horsies_workflow_phase2_recover``
(``0005_workflow_functions.sql``), the body of the worker's phase 2 with no
wake: in one transaction (``BEGIN``, the function, one
``horsies_workflow_child_start`` per owed child start, ``COMMIT``) it writes
the node from the stored result, runs the failure policy, promotion and
the completion check, and deletes the record. A record whose stored result
does not pass its check is kept: the function adds one attempt, and at the
attempt bound it quarantines the evidence. After the commit, the parent
nodes of the child workflows that the step ended advance.

Bounds are the ones the retired scan carried, deliberately: oldest
first, a grace window that leaves recent terminalizations to the healthy
finalizer still presumed in flight, a per-pass row cap, and one holder
per interval under the reaper's cluster-wide gate.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import TYPE_CHECKING, Final, assert_never

from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from horsies.core.history.names import WORKFLOW_PHASE2_PENDING
from horsies.core.history.phase2.quarantine import (
    DRAINED_VERDICTS,
    REPOINTED,
)
from horsies.core.logging import get_logger
from horsies.core.utils.db import autocommit_connection
from horsies.core.workflows.functions import (
    PHASE2_RECORD_RAISE_FUNCTION_SQL,
    RaiseCounted,
    RaiseLimitReached,
    RaiseNotRecorded,
    RaisePendingAbsent,
    RecoveryLimitWrite,
    decode_phase2_raise,
    quarantine_parts,
)
from horsies.core.workflows.recovery_limit import (
    RecoveryCase,
    RecoveryLimitOutcome,
    RecoveryRaise,
    RecoveryRaiseClass,
    classify_recovery_raise,
    recovery_limit_error,
    recovery_limit_payloads,
)
from horsies.core.workflows.steps import (
    PHASE2_RECOVER_FUNCTION_SQL,
    NodeFinishDisposition,
    advance_parents,
    run_in_transaction,
)

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

    from horsies.core.brokers.postgres import PostgresBroker

logger = get_logger('phase2_recovery')

DISCOVER_PENDING_SQL = text(f"""
    SELECT pending.task_id, pending.workflow_id, node.task_index
    FROM {WORKFLOW_PHASE2_PENDING} pending
    JOIN horsies_workflow_tasks node ON node.id = pending.workflow_node_row_id
    WHERE pending.created_at < NOW()
          - (CAST(:grace_ms AS double precision) / 1000.0)
            * INTERVAL '1 second'
      AND pending.attempt_count < CAST(:attempt_bound AS integer)
    ORDER BY pending.created_at, pending.task_id
    LIMIT CAST(:max_rows AS bigint)
""")
"""Oldest first, past the grace window, under the attempt bound, capped,
with the workflow and the node of each record (the limit error of a raised
record names them).

The ordering and the cap ride the outbox's (created_at, task_id) index.
The grace window is why a healthy finalizer's in-flight phase 2 is not
raced: a row younger than the window is left alone. The attempt bound is
why an unresolvable row cannot retry forever: once a row has retained
through that many passes it is quarantined and discovery stops selecting
it — the exclusion holds even if the quarantine transition itself
refused, so a stuck row costs a bounded number of passes either way."""


RAISED_FAILURE_CLASS = 'RAISED'
"""`last_failure_class` of a row whose progression raised."""


COUNT_OVER_ATTEMPT_BOUND_SQL = text(f"""
    SELECT count(*) FROM {WORKFLOW_PHASE2_PENDING}
    WHERE attempt_count >= CAST(:attempt_bound AS integer)
""")
"""The standing population discovery no longer retries.

Published on the health surface every pass so a quarantined population
is visible rather than merely absent from the log."""


@dataclass(frozen=True, slots=True)
class Phase2RecoverySummary:
    """What one pass did, in the shape the health surface publishes.

    Quarantine preserves evidence rather than dropping it: an
    unresolvable retained row is an impossible state that occurred, and
    an impossible state that occurs is evidence — terminal-and-drop was
    rejected on that principle. The bounded outcome is that the row's
    recovery material moves to the quarantine table, discovery stops
    retrying it, and the population stays countable here.
    """

    considered: int = 0
    applied: int = 0
    already_applied: int = 0
    superseded: int = 0
    retained: int = 0
    failed: int = 0
    quarantined: int = 0
    """Rows whose attempt count reached the bound this pass and whose
    evidence was repointed at the quarantine table (or already was)."""
    raised_counted: int = 0
    """Rows whose progression raised a persistent error this pass, counted
    toward the attempt bound."""
    over_attempt_bound: int = 0
    """Standing population at or over the bound — the rows discovery no
    longer retries, visible as a count rather than as silence."""
    retained_details: tuple[str, ...] = field(default_factory=tuple)
    """One entry per retaining disposition, carrying the function's own
    detail verbatim. A population that never disposes is visible here
    rather than only in the log."""
    quarantine_refusals: tuple[str, ...] = field(default_factory=tuple)
    """Transitions the quarantine function refused, verbatim. The row is
    still excluded from discovery by the attempt bound; the refusal
    names why its evidence could not be relocated."""


@dataclass
class _PassCounts:
    """The counts of one pass, written row by row."""

    applied: int = 0
    already_applied: int = 0
    superseded: int = 0
    retained: int = 0
    failed: int = 0
    quarantined: int = 0
    raised_counted: int = 0
    details: list[str] = field(default_factory=lambda: [])
    quarantine_refusals: list[str] = field(default_factory=lambda: [])


def _count_outcome(
    counts: _PassCounts,
    task_id: str,
    disposition: NodeFinishDisposition,
    detail: str | None,
    quarantine_after_attempts: int,
) -> None:
    """Count the outcome of one recovered record.

    A kept record (``SOURCE_*``) is logged with its disposition; ``detail``
    is the quarantine verdict when the call reached the attempt bound.
    """
    match disposition:
        case NodeFinishDisposition.APPLIED:
            counts.applied += 1
        case NodeFinishDisposition.APPLIED_WORKFLOW_ENDED:
            counts.superseded += 1
        case NodeFinishDisposition.ALREADY_APPLIED:
            counts.already_applied += 1
        case NodeFinishDisposition.PENDING_ABSENT:
            logger.warning(
                'Phase-2 recovery found no record for task %s and its node not '
                'terminal; nothing written',
                task_id,
            )
            counts.already_applied += 1
        case (
            NodeFinishDisposition.SOURCE_ABSENT
            | NodeFinishDisposition.SOURCE_STATE_CONFLICT
            | NodeFinishDisposition.SOURCE_VERSION_CONFLICT
            | NodeFinishDisposition.SOURCE_DIGEST_MISMATCH
        ):
            counts.retained += 1
            entry = f'{task_id}: {disposition.value}'
            counts.details.append(entry)
            # Verbatim: the disposition is what an operator needs to act on,
            # and a count alone hides a population that never disposes.
            logger.error('Phase-2 recovery retained evidence — %s', entry)
            match detail:
                case None:
                    pass
                case verdict_text:
                    verdict, verdict_detail = quarantine_parts(verdict_text)
                    quarantined, refusal = _quarantine_report(
                        task_id=task_id,
                        attempt_count=quarantine_after_attempts,
                        disposition=disposition.value,
                        verdict=verdict,
                        detail=verdict_detail,
                    )
                    counts.quarantined += int(quarantined)
                    if refusal is not None:
                        counts.quarantine_refusals.append(refusal)
        case _ as unreachable:
            assert_never(unreachable)


async def drive_phase2_recovery(
    session_factory: 'async_sessionmaker[AsyncSession]',
    broker: 'PostgresBroker',
    *,
    grace_ms: int,
    max_rows: int,
    quarantine_after_attempts: int,
) -> Phase2RecoverySummary:
    """One bounded pass over the phase-2 outbox.

    Each record is recovered in its OWN transaction (``BEGIN``,
    ``horsies_workflow_phase2_recover``, the owed child starts, ``COMMIT``):
    batching them would let a single bad record poison every good one
    behind it while holding workflow locks across the whole set. After the
    commit, the parent nodes of the child workflows that the record's step
    ended advance; a failed advance is logged and left to workflow recovery
    (a terminal child under a live parent node).

    A kept record (``SOURCE_*``) gets one attempt in the same statement; at
    the bound the function quarantines its evidence, and discovery stops
    selecting it either way. A transient raise does NOT count toward the
    bound: a database outage must not march healthy records toward
    quarantine. A persistent raise counts, in a new statement because its
    own transaction rolled back; at the bound the workflow also fails
    closed, because discovery stops and no other path would end it.

    Caller owns the one-holder guarantee. The reaper runs this inside a
    pass already gated by the cluster-wide advisory lock, so no second
    gate is taken here.
    """
    async with session_factory() as discovery_session:
        candidates = (
            await discovery_session.execute(
                DISCOVER_PENDING_SQL,
                {
                    'grace_ms': grace_ms,
                    'max_rows': max_rows,
                    'attempt_bound': quarantine_after_attempts,
                },
            )
        ).fetchall()
        over_bound = (
            await discovery_session.execute(
                COUNT_OVER_ATTEMPT_BOUND_SQL,
                {'attempt_bound': quarantine_after_attempts},
            )
        ).scalar_one()

    counts = _PassCounts()
    for candidate in candidates:
        task_id = str(candidate.task_id)
        try:
            async with session_factory() as session:
                committed = await run_in_transaction(
                    session,
                    broker,
                    PHASE2_RECOVER_FUNCTION_SQL,
                    {'task_id': task_id, 'attempt_bound': quarantine_after_attempts},
                    'horsies_workflow_phase2_recover',
                    NodeFinishDisposition,
                )
        except Exception as exc:
            # This record keeps its evidence and is seen again next pass;
            # the records behind it still run. A transient raise stays
            # uncounted toward the attempt bound: an outage is not the
            # record's fault.
            counts.failed += 1
            logger.error(
                'Phase-2 recovery failed for task %s: %s: %s',
                task_id,
                type(exc).__name__,
                exc,
            )
            match classify_recovery_raise(exc):
                case RecoveryRaiseClass.TRANSIENT:
                    pass
                case RecoveryRaiseClass.PERSISTENT:
                    match await _record_raised_row(
                        session_factory,
                        task_id=task_id,
                        workflow_id=str(candidate.workflow_id),
                        task_index=int(candidate.task_index),
                        exc=exc,
                        quarantine_after_attempts=quarantine_after_attempts,
                    ):
                        case _RaisedRowNotRecorded():
                            pass
                        case _RaisedRowRecorded(
                            quarantined=row_quarantined,
                            quarantine_refusal=row_refusal,
                        ):
                            counts.raised_counted += 1
                            counts.quarantined += int(row_quarantined)
                            if row_refusal is not None:
                                counts.quarantine_refusals.append(row_refusal)
                        case _ as unreachable:
                            assert_never(unreachable)
                case _ as unreachable:
                    assert_never(unreachable)
            continue

        _count_outcome(
            counts,
            task_id,
            committed.outcome.disposition,
            committed.outcome.detail,
            quarantine_after_attempts,
        )
        match await advance_parents(broker, session_factory, committed.parent_advances):
            case None:
                pass
            case failure:
                logger.error(
                    'Phase-2 recovery of task %s: the parent advance of child '
                    'workflow %s failed: %s: %s; workflow recovery advances it',
                    task_id,
                    failure.failed_child_workflow_id,
                    type(failure.cause).__name__,
                    failure.cause,
                )

    return Phase2RecoverySummary(
        considered=len(candidates),
        applied=counts.applied,
        already_applied=counts.already_applied,
        superseded=counts.superseded,
        retained=counts.retained,
        failed=counts.failed,
        quarantined=counts.quarantined,
        raised_counted=counts.raised_counted,
        over_attempt_bound=int(over_bound),
        retained_details=tuple(counts.details),
        quarantine_refusals=tuple(counts.quarantine_refusals),
    )


@dataclass(frozen=True, slots=True)
class _RaisedRowRecorded:
    """The attempt of a raised row is stored."""

    quarantined: bool
    quarantine_refusal: str | None


@dataclass(frozen=True, slots=True)
class _RaisedRowNotRecorded:
    """Nothing changed. The row is seen again next pass."""


type _RaisedRow = _RaisedRowRecorded | _RaisedRowNotRecorded


async def _record_raised_row(
    session_factory: 'async_sessionmaker[AsyncSession]',
    *,
    task_id: str,
    workflow_id: str,
    task_index: int,
    exc: Exception,
    quarantine_after_attempts: int,
) -> _RaisedRow:
    """Count one persistent raise of a row, and stop its workflow at the bound.

    Runs on an autocommit connection, because the raise rolled back the
    transaction of the row: one ``horsies_workflow_phase2_record_raise``
    statement. ``workflow_id`` and ``task_index`` are the record's, from
    discovery; the limit error names them. Discovery stops at
    the bound, so the bound is reached only with the limit applied: the
    statement writes nothing when a PAUSED workflow refuses the limit, and a
    limit write that fails fails the statement. A later pass, after a
    resume, raises again and applies the limit to a RUNNING workflow.

    The limit error names ``quarantine_after_attempts`` failed passes:
    discovery selects a row only under the bound, and the statement applies
    the limit only when the count reaches it, so the count at the limit is
    the bound.

    Returns:
        `_RaisedRowRecorded` when the count committed;
        `_RaisedRowNotRecorded` when a statement failed, when the limit was
        refused, or when the row is gone. After that outcome nothing changed.
    """
    failure = RecoveryRaise.of(
        exc,
        workflow_id=workflow_id,
        task_index=task_index,
        case=RecoveryCase.PHASE2_PROGRESSION,
    )
    payloads = recovery_limit_payloads(
        recovery_limit_error(failure, quarantine_after_attempts),
    )
    try:
        async with session_factory() as session:
            connection = await autocommit_connection(session)
            rows = (
                await connection.execute(
                    PHASE2_RECORD_RAISE_FUNCTION_SQL,
                    {
                        'task_id': task_id,
                        'attempt_bound': quarantine_after_attempts,
                        'limit_error': payloads.workflow_error_json,
                        'limit_node_result': payloads.node_result_json,
                    },
                )
            ).fetchall()
    except SQLAlchemyError as write_error:
        logger.error(
            'Phase-2 recovery could not record the raise of task %s: %s: %s',
            task_id,
            type(write_error).__name__,
            write_error,
        )
        return _RaisedRowNotRecorded()
    match decode_phase2_raise(rows):
        case RaisePendingAbsent():
            return _RaisedRowNotRecorded()
        case RaiseNotRecorded():
            logger.warning(
                'Workflow recovery limit refused: workflow_id=%s is not RUNNING',
                failure.workflow_id,
            )
            return _RaisedRowNotRecorded()
        case RaiseCounted():
            return _RaisedRowRecorded(quarantined=False, quarantine_refusal=None)
        case RaiseLimitReached(
            attempt_count=attempt_count,
            limit=limit,
            quarantine_verdict=verdict,
            quarantine_detail=detail,
        ):
            logger.critical(
                'Workflow recovery limit reached: workflow_id=%s '
                'failed_passes=%s outcome=%s case=%s task_index=%s '
                'exception_type=%s exception=%s',
                failure.workflow_id,
                attempt_count,
                _LIMIT_LOG_OUTCOME[limit].value,
                failure.case.value,
                failure.task_index,
                failure.exception_type,
                failure.detail,
            )
            quarantined, refusal = _quarantine_report(
                task_id=task_id,
                attempt_count=attempt_count,
                disposition=RAISED_FAILURE_CLASS,
                verdict=verdict,
                detail=detail,
            )
            return _RaisedRowRecorded(
                quarantined=quarantined, quarantine_refusal=refusal,
            )
        case _ as unreachable:
            assert_never(unreachable)


_LIMIT_LOG_OUTCOME: Final[Mapping[RecoveryLimitWrite, RecoveryLimitOutcome]] = (
    MappingProxyType({
        RecoveryLimitWrite.WORKFLOW_FAILED: RecoveryLimitOutcome.WORKFLOW_FAILED,
        RecoveryLimitWrite.LIVE_NODES_FINISH: RecoveryLimitOutcome.LIVE_NODES_FINISH,
    })
)
"""The limit outcome that the log names, as the session path named it."""


def _quarantine_report(
    *,
    task_id: str,
    attempt_count: int,
    disposition: str,
    verdict: str,
    detail: str | None,
) -> tuple[bool, str | None]:
    """Log one quarantine verdict; return (quarantined, refusal_detail).

    A REPOINTED or already-drained verdict counts as quarantined; any other
    verdict is a refusal, reported verbatim. The row is excluded from
    discovery by the attempt bound either way.
    """
    if verdict == REPOINTED or verdict in DRAINED_VERDICTS:
        logger.error(
            'Phase-2 recovery quarantined task %s: attempt bound %s reached; '
            'last disposition %s',
            task_id,
            attempt_count,
            disposition,
        )
        return True, None
    refusal = (
        f'{task_id}: quarantine refused with {verdict}'
        f'{f": {detail}" if detail else ""}'
    )
    logger.error('Phase-2 recovery %s', refusal)
    return False, refusal
