"""The record of a persistent phase-2 raise, before ``horsies_workflow_phase2_record_raise``.

``horsies/core/workflows/phase2_recovery.py`` (``_record_raised_row``) sent,
in one session transaction: the read of the record's workflow and node; the
attempt count update; under the bound, the commit. At the bound, in a
savepoint (``apply_recovery_limit``): the workflow row lock, and when the
workflow is RUNNING the node FAILED with the limit result, the skip of its
PENDING and READY nodes, the first error kept, the live node read and, with
no live node, the workflow FAILED; then the quarantine function and the
commit. A workflow that is not RUNNING rolled the count back.
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from typing import Final, LiteralString, assert_never

import psycopg

from horsies.core.history.phase2.quarantine import DRAINED_VERDICTS, REPOINTED
from horsies.core.workflows.recovery_limit import (
    RecoveryCase,
    RecoveryRaise,
    recovery_limit_error,
    recovery_limit_payloads,
)
from tests.dualrun.capture import Connection

RAISED_ROW_CONTEXT_SQL: Final[LiteralString] = """
    SELECT pending.workflow_id, node.task_index
    FROM horsies_workflow_phase2_pending pending
    JOIN horsies_workflow_tasks node
      ON node.id = pending.workflow_node_row_id
    WHERE pending.task_id = CAST(%(task_id)s AS uuid)
"""

RECORD_RETAINING_ATTEMPT_SQL: Final[LiteralString] = """
    UPDATE horsies_workflow_phase2_pending
    SET attempt_count = attempt_count + 1,
        last_attempt_at = statement_timestamp(),
        last_failure_class = %(failure_class)s
    WHERE task_id = CAST(%(task_id)s AS uuid)
    RETURNING attempt_count
"""

LOCK_WORKFLOW_STATUS_SQL: Final[LiteralString] = """
    SELECT status FROM horsies_workflows
    WHERE id = CAST(%(workflow_id)s AS uuid)
    FOR UPDATE
"""

MARK_WORKFLOW_TASK_FAILED_SQL: Final[LiteralString] = """
    UPDATE horsies_workflow_tasks
    SET status = 'FAILED', result = %(result)s, completed_at = NOW()
    WHERE workflow_id = %(wf_id)s AND task_index = %(idx)s
      AND NOT (status = ANY(%(terminal_states)s))
    RETURNING task_index
"""

SKIP_UNSTARTED_NODES_SQL: Final[LiteralString] = """
    UPDATE horsies_workflow_tasks
    SET status = 'SKIPPED', completed_at = NOW()
    WHERE workflow_id = CAST(%(workflow_id)s AS uuid)
      AND status IN ('PENDING', 'READY')
"""

KEEP_FIRST_WORKFLOW_ERROR_SQL: Final[LiteralString] = """
    UPDATE horsies_workflows
    SET error = COALESCE(error, %(error)s), updated_at = NOW()
    WHERE id = CAST(%(workflow_id)s AS uuid)
      AND status = 'RUNNING'
"""

HAS_LIVE_NODE_SQL: Final[LiteralString] = """
    SELECT EXISTS (
        SELECT 1 FROM horsies_workflow_tasks
        WHERE workflow_id = CAST(%(workflow_id)s AS uuid)
          AND status IN ('ENQUEUED', 'RUNNING')
    )
"""

MARK_WORKFLOW_FAILED_SQL: Final[LiteralString] = """
    UPDATE horsies_workflows
    SET status = 'FAILED', result = NULL,
        error = COALESCE(%(error)s, error),
        completed_at = NOW(), updated_at = NOW()
    WHERE id = %(wf_id)s AND status = 'RUNNING' AND completed_at IS NULL
"""

QUARANTINE_SQL: Final[LiteralString] = """
    SELECT verdict, detail FROM horsies_phase2_quarantine_one(CAST(%(task_id)s AS uuid))
"""

_RAISED: Final = 'RAISED'
_NODE_TERMINAL: Final = ['COMPLETED', 'FAILED', 'SKIPPED']


@dataclass(frozen=True, slots=True)
class Recorded:
    """The count committed; at the bound the limit and the quarantine ran."""

    quarantined: bool
    quarantine_refusal: str | None


@dataclass(frozen=True, slots=True)
class NotRecorded:
    """Nothing changed."""


type RaiseReport = Recorded | NotRecorded


def _limit(
    connection: Connection, task_id: uuid.UUID, workflow_id: str, task_index: int, count: int,
) -> RaiseReport:
    failure = RecoveryRaise.of(
        RuntimeError('boom'),
        workflow_id=workflow_id,
        task_index=task_index,
        case=RecoveryCase.PHASE2_PROGRESSION,
    )
    payloads = recovery_limit_payloads(recovery_limit_error(failure, count))
    status = connection.execute(LOCK_WORKFLOW_STATUS_SQL, {'workflow_id': workflow_id}).fetchone()
    match status:
        case ('RUNNING',):
            pass
        case _:
            return NotRecorded()
    _ = connection.execute(
        MARK_WORKFLOW_TASK_FAILED_SQL,
        {
            'wf_id': uuid.UUID(workflow_id),
            'idx': task_index,
            'result': payloads.node_result_json,
            'terminal_states': _NODE_TERMINAL,
        },
    )
    _ = connection.execute(SKIP_UNSTARTED_NODES_SQL, {'workflow_id': workflow_id})
    _ = connection.execute(
        KEEP_FIRST_WORKFLOW_ERROR_SQL,
        {'workflow_id': workflow_id, 'error': payloads.workflow_error_json},
    )
    live = connection.execute(HAS_LIVE_NODE_SQL, {'workflow_id': workflow_id}).fetchone()
    match live:
        case (False,):
            _ = connection.execute(
                MARK_WORKFLOW_FAILED_SQL, {'wf_id': uuid.UUID(workflow_id), 'error': None},
            )
        case _:
            pass
    verdict = connection.execute(QUARANTINE_SQL, {'task_id': task_id}).fetchone()
    assert verdict is not None
    match verdict:
        case (str() as name, _) if name == REPOINTED or name in DRAINED_VERDICTS:
            return Recorded(quarantined=True, quarantine_refusal=None)
        case (name, detail):
            return Recorded(
                quarantined=False,
                quarantine_refusal=(
                    f'{task_id}: quarantine refused with {name}{f": {detail}" if detail else ""}'
                ),
            )
        case _:
            raise AssertionError(f'the quarantine function returned {verdict!r}')


def _record(connection: Connection, task_id: uuid.UUID, bound: int) -> RaiseReport:
    context = connection.execute(RAISED_ROW_CONTEXT_SQL, {'task_id': task_id}).fetchone()
    count = connection.execute(
        RECORD_RETAINING_ATTEMPT_SQL, {'task_id': task_id, 'failure_class': _RAISED},
    ).fetchone()
    match context, count:
        case (workflow_id, int() as task_index), (int() as attempts,):
            pass
        case _:
            return NotRecorded()
    if attempts < bound:
        return Recorded(quarantined=False, quarantine_refusal=None)
    return _limit(connection, task_id, str(workflow_id), task_index, attempts)


def record_raise(connection: Connection, task_id: uuid.UUID, bound: int) -> RaiseReport:
    """One raise of the record of ``task_id``, in one transaction; nothing commits on NotRecorded."""
    report: RaiseReport = NotRecorded()
    with connection.transaction():
        report = _record(connection, task_id, bound)
        match report:
            case NotRecorded():
                raise psycopg.Rollback()
            case Recorded():
                pass
            case _ as unreachable:
                assert_never(unreachable)
    return report
