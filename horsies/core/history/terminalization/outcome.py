"""The 0.5.0 outcome row shape and the history-aware miss classifier.

The wire contract is unchanged from the consolidated vocabulary: one
composite row shape every operation returns, one decoder on the client
side, and one miss-classification order — absent, then already-applied,
then foreign terminalization, then lost claim, then conflict. What changes
underneath is where a terminal row lives: after the cutover a terminal
task is not a live row with a terminal status but a history row, so the
miss path classifies through the staged provenance lookup instead of the
live row's own columns. The live table cannot hold terminal rows at all
(its status CHECK forbids them), so the live branch classifies only claim
evidence.

A provenance hit reporting LIVE after the classifier's own locked live
read found nothing is a snapshot the contract does not allow; it raises
rather than guessing.
"""

from __future__ import annotations

from typing import Final

from ..names import LIVE_TASKS, TASK_PROVENANCE_FUNCTION

OUTCOME_TYPE: Final = 'horsies_terminalization_outcome'
MISS_CLASSIFIER_FUNCTION: Final = 'horsies_terminalization_miss'

OUTCOME_TYPE_DDL = f"""
CREATE TYPE {OUTCOME_TYPE} AS (
    task_id uuid,
    ordinality bigint,
    outcome text,
    terminal_at timestamptz,
    terminalization_kind text,
    observed_status text,
    observed_worker_id varchar,
    observed_claimed_at timestamptz,
    guard_kind text,
    observed_guard jsonb
)
"""

MISS_CLASSIFIER_DDL = f"""
CREATE FUNCTION {MISS_CLASSIFIER_FUNCTION}(
    p_task_id uuid,
    p_equivalent_kinds text[],
    p_worker_id text,
    p_claimed_at timestamptz
)
RETURNS SETOF {OUTCOME_TYPE}
LANGUAGE plpgsql
AS $function$
DECLARE
    v_row {LIVE_TASKS}%ROWTYPE;
    v_provenance record;
BEGIN
    SELECT * INTO v_row
    FROM {LIVE_TASKS}
    WHERE id = p_task_id
    FOR UPDATE;

    IF NOT FOUND THEN
        SELECT * INTO v_provenance
        FROM {TASK_PROVENANCE_FUNCTION}(p_task_id, FALSE);
        IF NOT v_provenance.found THEN
            RETURN QUERY SELECT
                p_task_id, NULL::bigint, 'TASK_ABSENT'::text,
                NULL::timestamptz, NULL::text,
                NULL::text, NULL::varchar, NULL::timestamptz,
                NULL::text, NULL::jsonb;
            RETURN;
        END IF;
        IF v_provenance.location <> 'HISTORY' THEN
            RAISE EXCEPTION
                'provenance reported % after a live miss', v_provenance.location
                USING ERRCODE = 'data_corrupted';
        END IF;
        IF v_provenance.terminalization_kind = ANY(p_equivalent_kinds) THEN
            RETURN QUERY SELECT
                p_task_id, NULL::bigint, 'ALREADY_APPLIED'::text,
                v_provenance.terminal_at, v_provenance.terminalization_kind,
                v_provenance.status, NULL::varchar, NULL::timestamptz,
                NULL::text, NULL::jsonb;
            RETURN;
        END IF;
        RETURN QUERY SELECT
            p_task_id, NULL::bigint, 'SOURCE_STATE_CONFLICT'::text,
            v_provenance.terminal_at, v_provenance.terminalization_kind,
            v_provenance.status, NULL::varchar, NULL::timestamptz,
            'FOREIGN_TERMINALIZATION'::text, NULL::jsonb;
        RETURN;
    END IF;

    IF p_worker_id IS NOT NULL AND (
        v_row.claimed_by_worker_id IS DISTINCT FROM CAST(p_worker_id AS VARCHAR)
        OR (p_claimed_at IS NOT NULL
            AND v_row.claimed_at IS DISTINCT FROM p_claimed_at)
    ) THEN
        RETURN QUERY SELECT
            p_task_id, NULL::bigint, 'LOST_CLAIM'::text,
            NULL::timestamptz, NULL::text,
            v_row.status::text, v_row.claimed_by_worker_id, v_row.claimed_at,
            NULL::text, NULL::jsonb;
        RETURN;
    END IF;

    RETURN QUERY SELECT
        p_task_id, NULL::bigint, 'SOURCE_STATE_CONFLICT'::text,
        NULL::timestamptz, NULL::text,
        v_row.status::text, v_row.claimed_by_worker_id, v_row.claimed_at,
        NULL::text, NULL::jsonb;
END
$function$
"""


def outcome_fragments() -> tuple[str, ...]:
    """The outcome type and miss classifier, in installation order."""
    return (OUTCOME_TYPE_DDL, MISS_CLASSIFIER_DDL)
