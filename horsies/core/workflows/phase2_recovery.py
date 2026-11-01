"""Outbox-driven phase-2 recovery: the crashed-worker case, post-split.

A worker that dies between phase 1 (terminalize the task) and phase 2
(advance its workflow node) leaves the node behind. Before the live and
history sides were split, that was found by scanning the live table for
terminal tasks whose node had not moved. Terminalization now deletes the
live row and records the pending progression in the outbox instead, so
the evidence is written precisely rather than rediscovered — and this
module is its consumer.

The database-owned consumption function does the hard part: it takes the
locks in the engine's order, loads the authoritative material, validates
identity and integrity, classifies into the exhaustive disposition set,
applies the node write when progression is still valid, and deletes the
evidence only on a durable disposition. This module discovers the rows,
supplies the node status the engine's policy dictates, and applies the
engine's own remaining progression steps — failure policy, paused guard,
dependents, workflow completion — by calling them, never by restating
them.

Bounds are the ones the retired scan carried, deliberately: oldest
first, a grace window that leaves recent terminalizations to the healthy
finalizer still presumed in flight, a per-pass row cap, and one holder
per interval under the reaper's cluster-wide gate.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, assert_never

from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from horsies.core.codec.json_io import loads_json
from horsies.core.codec.json_value import StrictJsonError
from horsies.core.codec.typed import (
    decode_task_error,
    validate_task_result_envelope,
)
from horsies.core.codec.validation_errors import ValueValidationError
from horsies.core.history.names import WORKFLOW_PHASE2_PENDING
from horsies.core.history.phase2.consumption import (
    Phase2Disposition,
    consume_phase2,
)
from horsies.core.history.phase2.quarantine import (
    DRAINED_VERDICTS,
    PHASE2_QUARANTINE_FUNCTION,
    REPOINTED,
)
from horsies.core.logging import get_logger
from horsies.core.models.tasks import (
    OperationalErrorCode,
    OutcomeCode,
    RetrievalCode,
    TaskError,
    TaskResult,
)
from horsies.core.types.result import is_err
from horsies.core.workflows.engine import (
    apply_node_progression,
    node_status_for_terminal_task,
)
from horsies.core.workflows.recovery_limit import (
    RecoveryCandidateOutcome,
    RecoveryCase,
    RecoveryRaise,
    RecoveryRaiseClass,
    apply_recovery_limit,
    classify_recovery_raise,
    report_recovery_limit,
)

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

    from horsies.core.brokers.postgres import PostgresBroker

logger = get_logger('phase2_recovery')

DURABLE_DISPOSITIONS: frozenset[str] = frozenset(
    {
        'APPLIED_TO_NODE',
        'ALREADY_APPLIED',
        'SUPERSEDED_BY_WORKFLOW_TERMINAL',
    }
)
"""The dispositions that commit with the evidence deleted.

Every other disposition RETAINS its pending row by the consumption
function's own contract, so the next pass sees it again; this module
never deletes evidence itself."""


DISCOVER_PENDING_SQL = text(f"""
    SELECT task_id, terminal_status
    FROM {WORKFLOW_PHASE2_PENDING}
    WHERE created_at < NOW()
          - (CAST(:grace_ms AS double precision) / 1000.0)
            * INTERVAL '1 second'
      AND attempt_count < CAST(:attempt_bound AS integer)
    ORDER BY created_at, task_id
    LIMIT CAST(:max_rows AS bigint)
""")
"""Oldest first, past the grace window, under the attempt bound, capped.

The ordering and the cap ride the outbox's (created_at, task_id) index.
The grace window is why a healthy finalizer's in-flight phase 2 is not
raced: a row younger than the window is left alone. The attempt bound is
why an unresolvable row cannot retry forever: once a row has retained
through that many passes it is quarantined and discovery stops selecting
it — the exclusion holds even if the quarantine transition itself
refused, so a stuck row costs a bounded number of passes either way."""


RECORD_RETAINING_ATTEMPT_SQL = text(f"""
    UPDATE {WORKFLOW_PHASE2_PENDING}
    SET attempt_count = attempt_count + 1,
        last_attempt_at = statement_timestamp(),
        last_failure_class = :failure_class
    WHERE task_id = CAST(:task_id AS uuid)
    RETURNING attempt_count
""")
"""One retaining disposition, recorded on the row it retained.

The count rides the pending row as a column, never process memory: a
worker restart, a different holder next pass, or a crashed pass all see
the same count. Written in the same transaction as the consumption call
that produced the disposition. A row whose progression raised writes it
in a new transaction, because the raise rolled back the first one."""


RAISED_ROW_CONTEXT_SQL = text(f"""
    SELECT pending.workflow_id, node.task_index
    FROM {WORKFLOW_PHASE2_PENDING} pending
    JOIN horsies_workflow_tasks node
      ON node.id = pending.workflow_node_row_id
    WHERE pending.task_id = CAST(:task_id AS uuid)
""")
"""The workflow and the node of a row whose transaction rolled back."""


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


def _recovered_failure_result(
    terminal_status: str,
    node_result_json: str | None,
) -> TaskResult[Any, TaskError]:
    """The failed task's error, for the workflow-level failure policy.

    The node's own result was written from the history record by the
    consumption function; this reads the error out of it so the
    workflow's recorded error is the task's real one. Only the err slot
    is decoded — TaskError has a fixed schema and needs no ok-type — so
    recovery never depends on the local task registry.

    A terminal task with no stored error still has to say something
    true, so the status names what happened. A stored envelope or error
    that does not decode is stored data, and it gets the same constructed
    error: ``validate_task_result_envelope`` refuses with
    ``StrictJsonError``, and ``decode_task_error`` refuses with
    ``StrictJsonError`` or ``ValueValidationError``. Any other exception is
    a defect and propagates.
    """
    if node_result_json is not None:
        parsed = loads_json(node_result_json)
        if not is_err(parsed):
            try:
                envelope = validate_task_result_envelope(parsed.ok_value)
                err_slot = envelope.get('err')
                if err_slot is not None:
                    return TaskResult(err=decode_task_error(err_slot))
            except (StrictJsonError, ValueValidationError) as exc:
                # A stored envelope or error that will not decode is
                # reported through the constructed error below rather
                # than failing the recovery of a task that has already
                # run.
                logger.error(
                    'Phase-2 recovery could not decode the stored result '
                    'envelope (%s: %s); recording the terminal status instead',
                    type(exc).__name__,
                    exc,
                )

    error_code: OutcomeCode | RetrievalCode | OperationalErrorCode
    match terminal_status:
        case 'CANCELLED':
            error_code = OutcomeCode.TASK_CANCELLED
            message = 'Task was cancelled before producing a result'
        case 'EXPIRED':
            error_code = OutcomeCode.TASK_EXPIRED
            message = (
                'Task expired before execution started (good_until passed)'
            )
        case 'COMPLETED':
            error_code = RetrievalCode.RESULT_NOT_AVAILABLE
            message = 'Task completed but result is missing'
        case _:
            error_code = OperationalErrorCode.WORKER_CRASHED
            message = (
                'Worker crashed during task execution '
                f'(task_status={terminal_status}, no result stored)'
            )
    return TaskResult(
        err=TaskError(
            error_code=error_code,
            message=message,
            data={'task_status': terminal_status},
        ),
    )


NODE_RESULT_SQL = text("""
    SELECT result FROM horsies_workflow_tasks WHERE id = CAST(:id AS uuid)
""")


async def _apply_progression(
    session: 'AsyncSession',
    disposition: Phase2Disposition,
    broker: 'PostgresBroker | None',
) -> None:
    """Hand one applied node to the engine's shared progression body.

    The consumption function has already written the node and returned
    the progression context; the engine owns everything after that, and
    the in-process completion path enters the same body with the same
    arguments.
    """
    workflow_id = disposition.workflow_id
    if workflow_id is None or disposition.task_index is None:
        return

    failure: TaskResult[Any, TaskError] | None = None
    if disposition.node_status == 'FAILED':
        node_result = (
            await session.execute(
                NODE_RESULT_SQL, {'id': disposition.node_row_id},
            )
        ).scalar_one_or_none()
        failure = _recovered_failure_result(
            disposition.terminal_status or '', node_result,
        )

    await apply_node_progression(
        session,
        broker,
        workflow_id=workflow_id,
        task_index=disposition.task_index,
        failure=failure,
        on_error=disposition.on_error,
        workflow_status=disposition.workflow_status,
        depth=disposition.workflow_depth,
        root_workflow_id=disposition.root_workflow_id,
    )


async def drive_phase2_recovery(
    session_factory: 'async_sessionmaker[AsyncSession]',
    broker: 'PostgresBroker | None' = None,
    *,
    grace_ms: int,
    max_rows: int,
    quarantine_after_attempts: int,
) -> Phase2RecoverySummary:
    """One bounded pass over the phase-2 outbox.

    Each row is consumed in its OWN transaction: the consumption
    function is one transaction from pending to durable disposition, and
    batching them would let a single bad row poison every good row
    behind it while holding workflow locks across the whole set.

    A retaining disposition increments the row's attempt count in the
    same transaction; at the bound the row's evidence is quarantined —
    copied, verified, and repointed by the database-owned quarantine
    function — and the discovery predicate stops selecting it either
    way. A transient raise does NOT count toward the bound: a database
    outage must not march healthy rows toward quarantine. A persistent
    raise counts, in a new transaction because its own rolled back; at the
    bound the workflow also fails closed, because discovery stops and no
    other path would end it.

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

    considered = 0
    applied = 0
    already_applied = 0
    superseded = 0
    retained = 0
    failed = 0
    quarantined = 0
    raised_counted = 0
    details: list[str] = []
    quarantine_refusals: list[str] = []

    for candidate in candidates:
        considered += 1
        task_id = str(candidate.task_id)
        terminal_status = str(candidate.terminal_status)
        try:
            node_status = node_status_for_terminal_task(terminal_status)
        except ValueError as exc:
            failed += 1
            logger.error(
                'Phase-2 recovery refused task %s: %s', task_id, exc,
            )
            continue

        quarantined_this_row = False
        quarantine_refusal: str | None = None
        try:
            async with session_factory() as session:
                connection = await session.connection()
                disposition = await consume_phase2(
                    connection,
                    task_id=task_id,
                    terminal_node_status=node_status,
                )
                if disposition.disposition == 'APPLIED_TO_NODE':
                    await _apply_progression(session, disposition, broker)
                elif disposition.disposition not in DURABLE_DISPOSITIONS:
                    attempt_count = (
                        await session.execute(
                            RECORD_RETAINING_ATTEMPT_SQL,
                            {
                                'task_id': task_id,
                                'failure_class': disposition.disposition,
                            },
                        )
                    ).scalar_one()
                    if int(attempt_count) >= quarantine_after_attempts:
                        (
                            quarantined_this_row,
                            quarantine_refusal,
                        ) = await _quarantine_exhausted_row(
                            session,
                            task_id=task_id,
                            attempt_count=int(attempt_count),
                            disposition=disposition.disposition,
                        )
                await session.commit()
        except Exception as exc:
            # This row keeps its evidence and is seen again next pass;
            # the rows behind it still run. A transient raise stays
            # uncounted toward the attempt bound: an outage is not the
            # row's fault.
            failed += 1
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
                        exc=exc,
                        quarantine_after_attempts=quarantine_after_attempts,
                    ):
                        case _RaisedRowNotRecorded():
                            pass
                        case _RaisedRowRecorded(
                            quarantined=row_quarantined,
                            quarantine_refusal=row_refusal,
                        ):
                            raised_counted += 1
                            quarantined += int(row_quarantined)
                            if row_refusal is not None:
                                quarantine_refusals.append(row_refusal)
                        case _ as unreachable:
                            assert_never(unreachable)
                case _ as unreachable:
                    assert_never(unreachable)
            continue

        match disposition.disposition:
            case 'APPLIED_TO_NODE':
                applied += 1
            case 'ALREADY_APPLIED':
                already_applied += 1
            case 'SUPERSEDED_BY_WORKFLOW_TERMINAL':
                superseded += 1
            case _:
                retained += 1
                detail = (
                    f'{task_id}: {disposition.disposition}'
                    f'{f": {disposition.detail}" if disposition.detail else ""}'
                )
                details.append(detail)
                # Verbatim, never reworded: the function's own words are
                # what an operator needs to act on, and a count alone
                # hides a population that never disposes.
                logger.error('Phase-2 recovery retained evidence — %s', detail)
                if quarantined_this_row:
                    quarantined += 1
                if quarantine_refusal is not None:
                    quarantine_refusals.append(quarantine_refusal)

    return Phase2RecoverySummary(
        considered=considered,
        applied=applied,
        already_applied=already_applied,
        superseded=superseded,
        retained=retained,
        failed=failed,
        quarantined=quarantined,
        raised_counted=raised_counted,
        over_attempt_bound=int(over_bound),
        retained_details=tuple(details),
        quarantine_refusals=tuple(quarantine_refusals),
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
    exc: Exception,
    quarantine_after_attempts: int,
) -> _RaisedRow:
    """Count one persistent raise of a row, and stop its workflow at the bound.

    Runs in a new transaction, because the raise rolled back the transaction
    of the row. Discovery stops at the bound, so the bound must not be
    reached unless the limit applied. A limit write that fails, or that a
    PAUSED workflow refuses, rolls this attempt back. The row then stays
    under the bound. A later pass, after a resume, raises again and applies
    the limit to a RUNNING workflow.

    Returns:
        `_RaisedRowRecorded` when the attempt committed;
        `_RaisedRowNotRecorded` when a write failed, when the limit was
        refused, or when the row is gone. After that outcome nothing changed.
    """
    try:
        async with session_factory() as session:
            context = (
                await session.execute(
                    RAISED_ROW_CONTEXT_SQL, {'task_id': task_id},
                )
            ).first()
            attempt_count = (
                await session.execute(
                    RECORD_RETAINING_ATTEMPT_SQL,
                    {'task_id': task_id, 'failure_class': RAISED_FAILURE_CLASS},
                )
            ).scalar_one_or_none()
            if context is None or attempt_count is None:
                await session.rollback()
                return _RaisedRowNotRecorded()
            if int(attempt_count) < quarantine_after_attempts:
                await session.commit()
                return _RaisedRowRecorded(
                    quarantined=False,
                    quarantine_refusal=None,
                )
            failure = RecoveryRaise.of(
                exc,
                workflow_id=str(context.workflow_id),
                task_index=int(context.task_index),
                case=RecoveryCase.PHASE2_PROGRESSION,
            )
            record = report_recovery_limit(
                await apply_recovery_limit(
                    session, failure, int(attempt_count),
                ),
                failure,
                int(attempt_count),
            )
            match record:
                case RecoveryCandidateOutcome.LIMIT_REACHED:
                    quarantined, refusal = await _quarantine_exhausted_row(
                        session,
                        task_id=task_id,
                        attempt_count=int(attempt_count),
                        disposition=RAISED_FAILURE_CLASS,
                    )
                    await session.commit()
                    return _RaisedRowRecorded(
                        quarantined=quarantined,
                        quarantine_refusal=refusal,
                    )
                case (
                    RecoveryCandidateOutcome.COUNTED
                    | RecoveryCandidateOutcome.NOT_COUNTED
                ):
                    await session.rollback()
                    return _RaisedRowNotRecorded()
                case _ as unreachable:
                    assert_never(unreachable)
    except SQLAlchemyError as write_error:
        logger.error(
            'Phase-2 recovery could not record the raise of task %s: %s: %s',
            task_id,
            type(write_error).__name__,
            write_error,
        )
        return _RaisedRowNotRecorded()


async def _quarantine_exhausted_row(
    session: 'AsyncSession',
    *,
    task_id: str,
    attempt_count: int,
    disposition: str,
) -> tuple[bool, str | None]:
    """Move one exhausted row's evidence to the quarantine table.

    Runs in the same transaction as the consumption call that produced
    the final retaining disposition; the quarantine function locks only
    the pending row, which this transaction already holds, so no new
    lock tier is introduced. Returns (quarantined, refusal_detail): a
    REPOINTED or already-drained verdict counts as quarantined, and a
    refusal is reported verbatim — the row is excluded from discovery by
    the attempt bound regardless, so a refusal costs visibility of the
    relocation, never an unbounded retry.
    """
    reason = (
        f'attempt bound {attempt_count} reached; '
        f'last disposition {disposition}'
    )
    row = (
        await session.execute(
            text(
                f'SELECT * FROM {PHASE2_QUARANTINE_FUNCTION}('
                'CAST(:task_id AS uuid), :reason)'
            ),
            {'task_id': task_id, 'reason': reason},
        )
    ).one()
    verdict = str(row.verdict)
    if verdict == REPOINTED or verdict in DRAINED_VERDICTS:
        logger.error(
            'Phase-2 recovery quarantined task %s: %s', task_id, reason,
        )
        return True, None
    refusal = (
        f'{task_id}: quarantine refused with {verdict}'
        f'{f": {row.detail}" if row.detail else ""}'
    )
    logger.error('Phase-2 recovery %s', refusal)
    return False, refusal
