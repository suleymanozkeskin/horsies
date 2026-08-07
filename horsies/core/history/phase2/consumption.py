"""Phase-2 consumption: one transaction from pending to durable disposition.

The database-owned function acquires locks in the engine's N6 order —
workflow row, node row, then the pending row — loads the authoritative
recovery material from the source the locator names, validates identity
and integrity, classifies into the exhaustive disposition set, applies
the node write when progression remains valid, and deletes recovery
evidence only on a durable disposition. Every integrity or state problem
is a typed disposition that RETAINS pending; no branch deletes recovery
evidence on a miss.

The caller supplies the terminal NODE status to apply — the task-status
to node-status mapping is engine policy, and keeping it out of this
function keeps the function mechanical. The returned row carries the
progression context the engine needs to drive dependent promotion and
workflow-completion effects inside the same caller-owned transaction,
the same shape its completion statement returns today.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Final

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection

from ..errors import HistoryContractError
from ..names import (
    TASK_HISTORY_PARENT,
    WORKFLOW_PHASE2_PENDING,
    WORKFLOW_PHASE2_QUARANTINE,
)

PHASE2_DISPOSITION_TYPE: Final = 'horsies_phase2_disposition'
PHASE2_CONSUME_FUNCTION: Final = 'horsies_phase2_consume'

PHASE2_DISPOSITION_TYPE_DDL = f"""
CREATE TYPE {PHASE2_DISPOSITION_TYPE} AS (
    disposition text,
    workflow_id uuid,
    node_row_id uuid,
    task_index integer,
    workflow_status text,
    workflow_depth integer,
    root_workflow_id uuid,
    on_error text,
    node_status text,
    terminal_status text,
    detail text
)
"""

PHASE2_CONSUME_FUNCTION_DDL = f"""
CREATE FUNCTION {PHASE2_CONSUME_FUNCTION}(
    p_task_id uuid,
    p_terminal_node_status text
) RETURNS {PHASE2_DISPOSITION_TYPE}
LANGUAGE plpgsql
AS $function$
DECLARE
    v_pending {WORKFLOW_PHASE2_PENDING}%ROWTYPE;
    v_wf record;
    v_node record;
    v_payload bytea;
    v_digest bytea;
    v_version smallint;
    v_source_task uuid;
    v_cas_won boolean;
BEGIN
    IF p_terminal_node_status NOT IN ('COMPLETED', 'FAILED', 'CANCELLED') THEN
        RAISE EXCEPTION
            'terminal node status must be COMPLETED, FAILED, or CANCELLED'
            USING ERRCODE = 'invalid_parameter_value';
    END IF;

    SELECT * INTO v_pending
    FROM {WORKFLOW_PHASE2_PENDING}
    WHERE task_id = p_task_id;

    IF NOT FOUND THEN
        -- Idempotent replay after an uncertain commit: the first commit
        -- deleted pending. Classify from the node the task backs.
        SELECT wt.id, wt.workflow_id, wt.task_index, wt.status
        INTO v_node
        FROM horsies_workflow_tasks wt
        WHERE wt.task_id = p_task_id
        ORDER BY wt.id
        LIMIT 1;
        IF NOT FOUND THEN
            RETURN ROW('PENDING_ABSENT', NULL, NULL, NULL, NULL, NULL,
                       NULL, NULL, NULL, NULL,
                       'no pending row and no node linkage')
                ::{PHASE2_DISPOSITION_TYPE};
        END IF;
        SELECT w.status, w.depth, w.root_workflow_id, w.on_error
        INTO v_wf
        FROM horsies_workflows w
        WHERE w.id = v_node.workflow_id;
        IF v_node.status IN ('COMPLETED', 'FAILED', 'CANCELLED', 'SKIPPED')
        THEN
            RETURN ROW('ALREADY_APPLIED', v_node.workflow_id, v_node.id,
                       v_node.task_index, v_wf.status, v_wf.depth,
                       v_wf.root_workflow_id, v_wf.on_error,
                       v_node.status, NULL, NULL)
                ::{PHASE2_DISPOSITION_TYPE};
        END IF;
        RETURN ROW('PENDING_ABSENT', v_node.workflow_id, v_node.id,
                   v_node.task_index, v_wf.status, v_wf.depth,
                   v_wf.root_workflow_id, v_wf.on_error,
                   v_node.status, NULL,
                   'no pending row; node not terminal')
            ::{PHASE2_DISPOSITION_TYPE};
    END IF;

    -- N6 order: workflow row first, node row second, pending third.
    SELECT w.status, w.depth, w.root_workflow_id, w.on_error
    INTO v_wf
    FROM horsies_workflows w
    WHERE w.id = v_pending.workflow_id
    FOR UPDATE;
    IF NOT FOUND THEN
        RETURN ROW('SOURCE_STATE_CONFLICT', v_pending.workflow_id,
                   v_pending.workflow_node_row_id, NULL, NULL, NULL,
                   NULL, NULL, NULL, v_pending.terminal_status,
                   'workflow row absent while pending exists')
            ::{PHASE2_DISPOSITION_TYPE};
    END IF;

    SELECT wt.id, wt.workflow_id, wt.task_index, wt.status
    INTO v_node
    FROM horsies_workflow_tasks wt
    WHERE wt.id = v_pending.workflow_node_row_id
      AND wt.workflow_id = v_pending.workflow_id
    FOR UPDATE;
    IF NOT FOUND THEN
        RETURN ROW('SOURCE_STATE_CONFLICT', v_pending.workflow_id,
                   v_pending.workflow_node_row_id, NULL, v_wf.status,
                   v_wf.depth, v_wf.root_workflow_id, v_wf.on_error,
                   NULL, v_pending.terminal_status,
                   'node row absent while pending exists')
            ::{PHASE2_DISPOSITION_TYPE};
    END IF;

    PERFORM 1 FROM {WORKFLOW_PHASE2_PENDING}
    WHERE task_id = p_task_id
    FOR UPDATE;

    IF v_wf.status IN ('COMPLETED', 'FAILED', 'CANCELLED') THEN
        DELETE FROM {WORKFLOW_PHASE2_PENDING} WHERE task_id = p_task_id;
        IF v_pending.recovery_source = 'QUARANTINE' THEN
            DELETE FROM {WORKFLOW_PHASE2_QUARANTINE}
            WHERE task_id = v_pending.quarantine_task_id;
        END IF;
        RETURN ROW('SUPERSEDED_BY_WORKFLOW_TERMINAL',
                   v_pending.workflow_id, v_node.id, v_node.task_index,
                   v_wf.status, v_wf.depth, v_wf.root_workflow_id,
                   v_wf.on_error, v_node.status,
                   v_pending.terminal_status, NULL)
            ::{PHASE2_DISPOSITION_TYPE};
    END IF;

    IF v_pending.recovery_source = 'HISTORY' THEN
        -- One-leaf parent probe, NOT the rejected fan-out: the locator
        -- supplies both partition keys, so LIST (class) and RANGE
        -- (anchor) prune to exactly one leaf at plan time. The rejected
        -- mechanism carried a task-id predicate alone and planned every
        -- leaf; this read exists because the locator makes pruning
        -- possible.
        SELECT h.task_id, h.result_payload, h.result_digest,
               h.history_schema_version
        INTO v_source_task, v_payload, v_digest, v_version
        FROM {TASK_HISTORY_PARENT} h
        WHERE h.retention_class_key = v_pending.history_class
          AND h.retention_anchor_at = v_pending.history_anchor
          AND h.task_id = p_task_id;
        IF NOT FOUND THEN
            RETURN ROW('SOURCE_ABSENT', v_pending.workflow_id, v_node.id,
                       v_node.task_index, v_wf.status, v_wf.depth,
                       v_wf.root_workflow_id, v_wf.on_error, v_node.status,
                       v_pending.terminal_status,
                       'history row absent at locator')
                ::{PHASE2_DISPOSITION_TYPE};
        END IF;
    ELSE
        SELECT q.task_id, q.result_payload, q.result_digest,
               q.history_schema_version
        INTO v_source_task, v_payload, v_digest, v_version
        FROM {WORKFLOW_PHASE2_QUARANTINE} q
        WHERE q.task_id = v_pending.quarantine_task_id;
        IF NOT FOUND THEN
            RETURN ROW('SOURCE_ABSENT', v_pending.workflow_id, v_node.id,
                       v_node.task_index, v_wf.status, v_wf.depth,
                       v_wf.root_workflow_id, v_wf.on_error, v_node.status,
                       v_pending.terminal_status,
                       'quarantine row absent at locator')
                ::{PHASE2_DISPOSITION_TYPE};
        END IF;
    END IF;

    IF v_source_task <> p_task_id THEN
        RETURN ROW('SOURCE_STATE_CONFLICT', v_pending.workflow_id,
                   v_node.id, v_node.task_index, v_wf.status, v_wf.depth,
                   v_wf.root_workflow_id, v_wf.on_error, v_node.status,
                   v_pending.terminal_status,
                   'source row carries a different task identity')
            ::{PHASE2_DISPOSITION_TYPE};
    END IF;
    IF v_version IS DISTINCT FROM v_pending.history_schema_version
       OR v_version <> 1 THEN
        RETURN ROW('SOURCE_VERSION_CONFLICT', v_pending.workflow_id,
                   v_node.id, v_node.task_index, v_wf.status, v_wf.depth,
                   v_wf.root_workflow_id, v_wf.on_error, v_node.status,
                   v_pending.terminal_status,
                   'source schema version disagrees with locator')
            ::{PHASE2_DISPOSITION_TYPE};
    END IF;
    IF v_digest IS DISTINCT FROM v_pending.result_digest
       OR v_payload IS NULL
       OR sha256(v_payload) <> v_pending.result_digest THEN
        RETURN ROW('SOURCE_DIGEST_MISMATCH', v_pending.workflow_id,
                   v_node.id, v_node.task_index, v_wf.status, v_wf.depth,
                   v_wf.root_workflow_id, v_wf.on_error, v_node.status,
                   v_pending.terminal_status,
                   'result digest disagrees with locator or payload')
            ::{PHASE2_DISPOSITION_TYPE};
    END IF;

    UPDATE horsies_workflow_tasks wt
    SET status = p_terminal_node_status,
        result = convert_from(v_payload, 'UTF8'),
        completed_at = NOW()
    WHERE wt.id = v_node.id
      AND wt.status NOT IN ('COMPLETED', 'FAILED', 'CANCELLED', 'SKIPPED');
    v_cas_won := FOUND;

    DELETE FROM {WORKFLOW_PHASE2_PENDING} WHERE task_id = p_task_id;
    IF v_pending.recovery_source = 'QUARANTINE' THEN
        DELETE FROM {WORKFLOW_PHASE2_QUARANTINE}
        WHERE task_id = v_pending.quarantine_task_id;
    END IF;

    IF v_cas_won THEN
        RETURN ROW('APPLIED_TO_NODE', v_pending.workflow_id, v_node.id,
                   v_node.task_index, v_wf.status, v_wf.depth,
                   v_wf.root_workflow_id, v_wf.on_error,
                   p_terminal_node_status, v_pending.terminal_status, NULL)
            ::{PHASE2_DISPOSITION_TYPE};
    END IF;
    RETURN ROW('ALREADY_APPLIED', v_pending.workflow_id, v_node.id,
               v_node.task_index, v_wf.status, v_wf.depth,
               v_wf.root_workflow_id, v_wf.on_error, v_node.status,
               v_pending.terminal_status, NULL)
        ::{PHASE2_DISPOSITION_TYPE};
END
$function$
"""


def consumption_fragments() -> tuple[str, ...]:
    """The disposition type and consumption function, in install order."""
    return (PHASE2_DISPOSITION_TYPE_DDL, PHASE2_CONSUME_FUNCTION_DDL)


# ---------------------------------------------------------------------------
# Typed outcomes and client wrapper
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class Phase2Disposition:
    """One consumption verdict with the engine's progression context.

    Evidence-retaining dispositions (`SOURCE_*`, `SOURCE_STATE_CONFLICT`,
    `PENDING_ABSENT`) leave the pending row in place; the durable three
    (`APPLIED_TO_NODE`, `ALREADY_APPLIED`,
    `SUPERSEDED_BY_WORKFLOW_TERMINAL`) commit with the evidence deleted.
    """

    disposition: str
    workflow_id: str | None
    node_row_id: str | None
    task_index: int | None
    workflow_status: str | None
    workflow_depth: int | None
    root_workflow_id: str | None
    on_error: str | None
    node_status: str | None
    terminal_status: str | None
    detail: str | None


KNOWN_DISPOSITIONS: Final = frozenset(
    {
        'APPLIED_TO_NODE',
        'ALREADY_APPLIED',
        'SUPERSEDED_BY_WORKFLOW_TERMINAL',
        'SOURCE_STATE_CONFLICT',
        'PENDING_ABSENT',
        'SOURCE_ABSENT',
        'SOURCE_VERSION_CONFLICT',
        'SOURCE_DIGEST_MISMATCH',
    }
)

EVIDENCE_RETAINING_DISPOSITIONS: Final = frozenset(
    {
        'SOURCE_STATE_CONFLICT',
        'PENDING_ABSENT',
        'SOURCE_ABSENT',
        'SOURCE_VERSION_CONFLICT',
        'SOURCE_DIGEST_MISMATCH',
    }
)


async def consume_phase2(
    connection: AsyncConnection,
    *,
    task_id: str,
    terminal_node_status: str,
) -> Phase2Disposition:
    """Execute one consumption inside the caller-owned transaction."""
    row = (
        await connection.execute(
            text(
                f'SELECT * FROM {PHASE2_CONSUME_FUNCTION}('
                'CAST(:task_id AS uuid), :node_status)'
            ),
            {'task_id': task_id, 'node_status': terminal_node_status},
        )
    ).one()
    return decode_phase2_row(row)


def decode_phase2_row(row: Any) -> Phase2Disposition:
    """Decode one disposition row, failing closed on contract breaks."""
    disposition = row.disposition
    if disposition not in KNOWN_DISPOSITIONS:
        raise HistoryContractError(
            f'unknown phase-2 disposition {disposition!r}'
        )
    return Phase2Disposition(
        disposition=disposition,
        workflow_id=(
            str(row.workflow_id) if row.workflow_id is not None else None
        ),
        node_row_id=(
            str(row.node_row_id) if row.node_row_id is not None else None
        ),
        task_index=row.task_index,
        workflow_status=row.workflow_status,
        workflow_depth=row.workflow_depth,
        root_workflow_id=(
            str(row.root_workflow_id)
            if row.root_workflow_id is not None
            else None
        ),
        on_error=row.on_error,
        node_status=row.node_status,
        terminal_status=row.terminal_status,
        detail=row.detail,
    )
