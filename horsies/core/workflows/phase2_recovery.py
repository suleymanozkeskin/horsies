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
from typing import TYPE_CHECKING, Any

from sqlalchemy import text

from horsies.core.codec.json_io import loads_json
from horsies.core.codec.json_value import StrictJsonError
from horsies.core.codec.typed import (
    decode_task_error,
    validate_task_result_envelope,
)
from horsies.core.history.names import WORKFLOW_PHASE2_PENDING
from horsies.core.history.phase2.consumption import (
    Phase2Disposition,
    consume_phase2,
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
    ORDER BY created_at, task_id
    LIMIT CAST(:max_rows AS bigint)
""")
"""Oldest first, past the grace window, capped.

The ordering and the cap ride the outbox's (created_at, task_id) index.
The grace window is why a healthy finalizer's in-flight phase 2 is not
raced: a row younger than the window is left alone."""


@dataclass(frozen=True, slots=True)
class Phase2RecoverySummary:
    """What one pass did, in the shape the health surface publishes."""

    considered: int = 0
    applied: int = 0
    already_applied: int = 0
    superseded: int = 0
    retained: int = 0
    failed: int = 0
    retained_details: tuple[str, ...] = field(default_factory=tuple)
    """One entry per retaining disposition, carrying the function's own
    detail verbatim. A population that never disposes is visible here
    rather than only in the log."""


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
    true, so the status names what happened.
    """
    if node_result_json is not None:
        parsed = loads_json(node_result_json)
        if not is_err(parsed):
            try:
                envelope = validate_task_result_envelope(parsed.ok_value)
                err_slot = envelope.get('err')
                if err_slot is not None:
                    return TaskResult(err=decode_task_error(err_slot))
            except StrictJsonError:
                # A stored envelope that will not validate is reported
                # through the constructed error below rather than
                # failing the recovery of a task that has already run.
                logger.error(
                    'Phase-2 recovery could not validate the stored result '
                    'envelope; recording the terminal status instead',
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
) -> Phase2RecoverySummary:
    """One bounded pass over the phase-2 outbox.

    Each row is consumed in its OWN transaction: the consumption
    function is one transaction from pending to durable disposition, and
    batching them would let a single bad row poison every good row
    behind it while holding workflow locks across the whole set.

    Caller owns the one-holder guarantee. The reaper runs this inside a
    pass already gated by the cluster-wide advisory lock, so no second
    gate is taken here.
    """
    async with session_factory() as discovery_session:
        candidates = (
            await discovery_session.execute(
                DISCOVER_PENDING_SQL,
                {'grace_ms': grace_ms, 'max_rows': max_rows},
            )
        ).fetchall()

    considered = 0
    applied = 0
    already_applied = 0
    superseded = 0
    retained = 0
    failed = 0
    details: list[str] = []

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
                await session.commit()
        except Exception as exc:
            # This row keeps its evidence and is seen again next pass;
            # the rows behind it still run.
            failed += 1
            logger.error(
                'Phase-2 recovery failed for task %s: %s: %s',
                task_id,
                type(exc).__name__,
                exc,
            )
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

    return Phase2RecoverySummary(
        considered=considered,
        applied=applied,
        already_applied=already_applied,
        superseded=superseded,
        retained=retained,
        failed=failed,
        retained_details=tuple(details),
    )
