"""Phase-2 recovery, before ``horsies_workflow_phase2_recover``.

``drive_phase2_recovery`` (``horsies/core/workflows/phase2_recovery.py``)
discovered the records, then for each one, in its own session transaction:
``horsies_phase2_consume`` with the node status of the record's kind; on
``APPLIED_TO_NODE`` the engine's progression (``apply_node_progression``,
with the task's error read back from the node for a FAILED node); on a
disposition that keeps the record, one attempt on the record and, at the
attempt bound, ``horsies_phase2_quarantine_one``. A raise is outside this
copy (the seeds raise none).

The progression is copied in ``workflow_progression.py``; this module
keeps the recovery's own statements and its order.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Final

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from horsies.core.brokers.postgres import PostgresBroker
from horsies.core.codec.json_io import loads_json
from horsies.core.codec.typed import decode_task_error, validate_task_result_envelope
from horsies.core.history.phase2.consumption import Phase2Disposition, consume_phase2
from horsies.core.history.phase2.quarantine import DRAINED_VERDICTS, REPOINTED
from horsies.core.models.tasks import OperationalErrorCode, TaskError, TaskResult
from horsies.core.types.result import is_err

DISCOVER_PENDING_SQL: Final = text("""
    SELECT task_id, terminalization_kind
    FROM horsies_workflow_phase2_pending
    WHERE created_at < NOW()
          - (CAST(:grace_ms AS double precision) / 1000.0)
            * INTERVAL '1 second'
      AND attempt_count < CAST(:attempt_bound AS integer)
    ORDER BY created_at, task_id
    LIMIT CAST(:max_rows AS bigint)
""")

RECORD_RETAINING_ATTEMPT_SQL: Final = text("""
    UPDATE horsies_workflow_phase2_pending
    SET attempt_count = attempt_count + 1,
        last_attempt_at = statement_timestamp(),
        last_failure_class = :failure_class
    WHERE task_id = CAST(:task_id AS uuid)
    RETURNING attempt_count
""")

NODE_RESULT_SQL: Final = text("""
    SELECT result FROM horsies_workflow_tasks WHERE id = CAST(:id AS uuid)
""")

QUARANTINE_SQL: Final = text("""
    SELECT * FROM horsies_phase2_quarantine_one(CAST(:task_id AS uuid))
""")

DURABLE_DISPOSITIONS: Final = frozenset(
    {'APPLIED_TO_NODE', 'ALREADY_APPLIED', 'SUPERSEDED_BY_WORKFLOW_TERMINAL'},
)

_NODE_STATUS_OF_KIND: Final = {
    'COMPLETE_LOCKED': ('COMPLETED', 'COMPLETED'),
    'FAIL_RUNNING': ('FAILED', 'FAILED'),
    'FAIL_STALE': ('FAILED', 'FAILED'),
    'EXPIRE_CLAIMED': ('EXPIRED', 'FAILED'),
    'EXPIRE_PENDING': ('EXPIRED', 'FAILED'),
}
"""Kind → (task status, node status)."""


@dataclass(frozen=True, slots=True)
class RecoverReport:
    """The counts of one pass."""

    applied: int
    already_applied: int
    superseded: int
    retained: int
    failed: int
    quarantined: int


def _failure(node_result: str | None, terminal_status: str) -> TaskResult[Any, TaskError]:
    """The task's error read back from the node (``_recovered_failure_result``,
    the decodable branch; the seeds store decodable errors)."""
    if node_result is not None:
        parsed = loads_json(node_result)
        assert not is_err(parsed), node_result
        err_slot = validate_task_result_envelope(parsed.ok_value).get('err')
        if err_slot is not None:
            return TaskResult(err=decode_task_error(err_slot))
    return TaskResult(
        err=TaskError(
            error_code=OperationalErrorCode.WORKER_CRASHED,
            message=(
                'Worker crashed during task execution '
                f'(task_status={terminal_status}, no result stored)'
            ),
            data={'task_status': terminal_status},
        ),
    )


async def _progress(
    session: AsyncSession,
    broker: PostgresBroker,
    disposition: Phase2Disposition,
    terminal_status: str,
) -> None:
    from tests.dualrun.reference.workflow_progression import (
        NodeMayHaveDependents,
        apply_node_progression,
    )

    assert disposition.workflow_id is not None and disposition.task_index is not None
    failure: TaskResult[Any, TaskError] | None = None
    if disposition.node_status == 'FAILED':
        node_result = (
            await session.execute(NODE_RESULT_SQL, {'id': disposition.node_row_id})
        ).scalar_one_or_none()
        failure = _failure(node_result, terminal_status)
    await apply_node_progression(
        session,
        broker,
        workflow_id=disposition.workflow_id,
        task_index=disposition.task_index,
        failure=failure,
        on_error=disposition.on_error,
        workflow_status=disposition.workflow_status,
        depth=disposition.workflow_depth,
        root_workflow_id=disposition.root_workflow_id,
        dependents=NodeMayHaveDependents(),
    )


async def recover(
    session_factory: async_sessionmaker[AsyncSession],
    broker: PostgresBroker,
    *,
    grace_ms: int,
    max_rows: int,
    quarantine_after_attempts: int,
) -> RecoverReport:
    """One pass of the session statements over the phase-2 records."""
    async with session_factory() as discovery:
        candidates = (
            await discovery.execute(
                DISCOVER_PENDING_SQL,
                {
                    'grace_ms': grace_ms,
                    'max_rows': max_rows,
                    'attempt_bound': quarantine_after_attempts,
                },
            )
        ).fetchall()
    counts = {'applied': 0, 'already': 0, 'superseded': 0, 'retained': 0, 'quarantined': 0}
    for candidate in candidates:
        task_id = str(candidate.task_id)
        terminal_status, node_status = _NODE_STATUS_OF_KIND[str(candidate.terminalization_kind)]
        async with session_factory() as session:
            connection = await session.connection()
            disposition = await consume_phase2(
                connection, task_id=task_id, terminal_node_status=node_status,
            )
            match disposition.disposition:
                case 'APPLIED_TO_NODE':
                    await _progress(session, broker, disposition, terminal_status)
                    counts['applied'] += 1
                case 'ALREADY_APPLIED':
                    counts['already'] += 1
                case 'SUPERSEDED_BY_WORKFLOW_TERMINAL':
                    counts['superseded'] += 1
                case kept:
                    counts['retained'] += 1
                    attempts = (
                        await session.execute(
                            RECORD_RETAINING_ATTEMPT_SQL,
                            {'task_id': task_id, 'failure_class': kept},
                        )
                    ).scalar_one()
                    if int(attempts) >= quarantine_after_attempts:
                        verdict = (
                            await session.execute(QUARANTINE_SQL, {'task_id': task_id})
                        ).one()
                        if verdict.verdict == REPOINTED or verdict.verdict in DRAINED_VERDICTS:
                            counts['quarantined'] += 1
            await session.commit()
    return RecoverReport(
        applied=counts['applied'],
        already_applied=counts['already'],
        superseded=counts['superseded'],
        retained=counts['retained'],
        failed=0,
        quarantined=counts['quarantined'],
    )
