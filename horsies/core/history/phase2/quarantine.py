"""Phase-2 quarantine: copy, verify, repoint one pending row.

Phase-2 recovery quarantines a pending row whose attempt count reached the
bound: the database function copies the minimum recovery projection out of
the history leaf into the quarantine table, verifies the copy, and repoints
the pending row at it. A failed copy or verification keeps the history
locator and returns a typed refusal; nothing deletes unresolved recovery
evidence.

The function locks only the pending row, a single-tier acquisition that can
take part in no lock cycle with consumption's workflow -> node -> pending
order. It reads one pruned history leaf, the node row, and the quarantine
table.
"""

from __future__ import annotations

from typing import Final

from ..names import (
    TASK_HISTORY_PARENT,
    WORKFLOW_PHASE2_PENDING,
    WORKFLOW_PHASE2_QUARANTINE,
)

PHASE2_QUARANTINE_VERDICT_TYPE: Final = 'horsies_phase2_quarantine_verdict'
PHASE2_QUARANTINE_FUNCTION: Final = 'horsies_phase2_quarantine_one'

PHASE2_QUARANTINE_VERDICT_TYPE_DDL = f"""
CREATE TYPE {PHASE2_QUARANTINE_VERDICT_TYPE} AS (
    verdict text,
    detail text
)
"""

PHASE2_QUARANTINE_FUNCTION_DDL = f"""
CREATE FUNCTION {PHASE2_QUARANTINE_FUNCTION}(
    p_task_id uuid,
    p_reason text
) RETURNS {PHASE2_QUARANTINE_VERDICT_TYPE}
LANGUAGE plpgsql
AS $function$
DECLARE
    v_pending {WORKFLOW_PHASE2_PENDING}%ROWTYPE;
    v_node_id text;
    v_hist record;
    v_copy record;
BEGIN
    -- The only lock this function takes: the pending row. Single-tier;
    -- consumption acquiring workflow -> node -> pending can wait on it
    -- but no cycle can form because nothing else is held here.
    SELECT * INTO v_pending
    FROM {WORKFLOW_PHASE2_PENDING}
    WHERE task_id = p_task_id
    FOR UPDATE;
    IF NOT FOUND THEN
        RETURN ROW('PENDING_GONE', 'no pending row; locator drained')
            ::{PHASE2_QUARANTINE_VERDICT_TYPE};
    END IF;
    IF v_pending.recovery_source <> 'HISTORY' THEN
        RETURN ROW('ALREADY_QUARANTINED',
                   'pending already repointed at quarantine')
            ::{PHASE2_QUARANTINE_VERDICT_TYPE};
    END IF;

    SELECT wt.node_id INTO v_node_id
    FROM horsies_workflow_tasks wt
    WHERE wt.id = v_pending.workflow_node_row_id
      AND wt.workflow_id = v_pending.workflow_id;
    IF NOT FOUND THEN
        RETURN ROW('NODE_ROW_ABSENT',
                   'node row absent while pending exists')
            ::{PHASE2_QUARANTINE_VERDICT_TYPE};
    END IF;
    IF v_node_id IS NULL THEN
        -- The quarantine projection requires a node identity and the
        -- node row carries none; refusing retains the history locator.
        RETURN ROW('NODE_IDENTITY_ABSENT',
                   'node row carries no node_id')
            ::{PHASE2_QUARANTINE_VERDICT_TYPE};
    END IF;

    -- One-leaf parent probe, NOT the rejected fan-out: the locator
    -- supplies both partition keys, so LIST (class) and RANGE (anchor)
    -- prune to exactly one leaf at plan time.
    SELECT h.task_id, h.task_name, h.status, h.terminalization_kind,
           h.terminal_at, h.history_schema_version,
           h.result_envelope_version, h.result_codec,
           h.result_content_type, h.result_payload, h.result_digest
    INTO v_hist
    FROM {TASK_HISTORY_PARENT} h
    WHERE h.retention_class_key = v_pending.history_class
      AND h.retention_anchor_at = v_pending.history_anchor
      AND h.task_id = p_task_id;
    IF NOT FOUND THEN
        RETURN ROW('SOURCE_ABSENT', 'history row absent at locator')
            ::{PHASE2_QUARANTINE_VERDICT_TYPE};
    END IF;

    BEGIN
        INSERT INTO {WORKFLOW_PHASE2_QUARANTINE} (
            task_id, workflow_id, workflow_node_row_id, node_id,
            task_name, terminal_status, terminalization_kind, terminal_at,
            history_schema_version, result_envelope_version,
            result_codec, result_content_type,
            result_payload, result_digest,
            source_history_class, source_history_anchor,
            quarantine_reason, quarantined_at
        ) VALUES (
            v_hist.task_id, v_pending.workflow_id,
            v_pending.workflow_node_row_id, v_node_id,
            v_hist.task_name, v_hist.status, v_hist.terminalization_kind,
            v_hist.terminal_at,
            v_hist.history_schema_version, v_hist.result_envelope_version,
            v_hist.result_codec, v_hist.result_content_type,
            v_hist.result_payload, v_hist.result_digest,
            v_pending.history_class, v_pending.history_anchor,
            p_reason, statement_timestamp()
        );

        -- Verification on the copy itself: read the row back and hold
        -- it against the pending locator, so a projection defect is a
        -- refusal, not a corrupt quarantine row.
        SELECT q.task_id, q.result_payload, q.result_digest,
               q.history_schema_version
        INTO STRICT v_copy
        FROM {WORKFLOW_PHASE2_QUARANTINE} q
        WHERE q.task_id = p_task_id;
        IF v_copy.task_id IS DISTINCT FROM p_task_id
           OR v_copy.result_digest IS DISTINCT FROM v_pending.result_digest
           OR v_copy.result_payload IS NULL
           OR sha256(v_copy.result_payload) <> v_copy.result_digest
           OR v_copy.history_schema_version
              IS DISTINCT FROM v_pending.history_schema_version
        THEN
            RAISE EXCEPTION 'quarantine copy disagrees with locator'
                USING ERRCODE = 'HQ001';
        END IF;
    EXCEPTION
        WHEN SQLSTATE 'HQ001' OR unique_violation OR not_null_violation
             OR check_violation THEN
            -- The sub-transaction rolls the copy back; pending keeps its
            -- history locator and the leaf stays pinned.
            RETURN ROW('COPY_VERIFICATION_FAILED', SQLERRM)
                ::{PHASE2_QUARANTINE_VERDICT_TYPE};
    END;

    UPDATE {WORKFLOW_PHASE2_PENDING}
    SET recovery_source = 'QUARANTINE',
        quarantine_task_id = p_task_id,
        history_class = NULL,
        history_anchor = NULL
    WHERE task_id = p_task_id;

    RETURN ROW('REPOINTED', NULL)::{PHASE2_QUARANTINE_VERDICT_TYPE};
END
$function$
"""


def quarantine_fragments() -> tuple[str, ...]:
    """The verdict type and quarantine function, in install order."""
    return (
        PHASE2_QUARANTINE_VERDICT_TYPE_DDL,
        PHASE2_QUARANTINE_FUNCTION_DDL,
    )


# ---------------------------------------------------------------------------
# Verdict vocabulary
# ---------------------------------------------------------------------------


REPOINTED: Final = 'REPOINTED'
DRAINED_VERDICTS: Final = frozenset({'PENDING_GONE', 'ALREADY_QUARANTINED'})
REFUSAL_VERDICTS: Final = frozenset(
    {
        'NODE_ROW_ABSENT',
        'SOURCE_ABSENT',
        'COPY_VERIFICATION_FAILED',
    }
)
KNOWN_QUARANTINE_VERDICTS: Final = (
    frozenset({REPOINTED}) | DRAINED_VERDICTS | REFUSAL_VERDICTS
)
