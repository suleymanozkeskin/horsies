-- The reference side of the dual-runs in test_history_moves.py and test_history_readers.py: the composite types
-- horsies_task_lookup and horsies_task_provenance and the array overload of horsies_terminalization_miss, the text of
-- 0003_interim_functions.sql at commit abdc49d4. The terminal writes before the reader switch call this overload.
--
-- The file runs after the baseline install. Do not edit a statement: the file is the record of the definitions
-- before the switch.

CREATE TYPE horsies_task_lookup AS (
	found boolean,
	location text,
	task_id uuid,
	fingerprint_version smallint,
	command_fingerprint bytea
);

CREATE TYPE horsies_task_provenance AS (
	found boolean,
	location text,
	task_id uuid,
	status text,
	terminal_at timestamp with time zone,
	terminalization_kind text
);

CREATE FUNCTION horsies_terminalization_miss(p_task_id uuid, p_equivalent_kinds text[], p_worker_id text, p_claimed_at timestamp with time zone) RETURNS SETOF horsies_terminalization_outcome
    LANGUAGE plpgsql
    AS $$
DECLARE
    v_row horsies_tasks%ROWTYPE;
    v_provenance record;
BEGIN
    SELECT * INTO v_row
    FROM horsies_tasks
    WHERE id = p_task_id
    FOR UPDATE;

    IF NOT FOUND THEN
        SELECT * INTO v_provenance
        FROM horsies_task_provenance_staged(p_task_id, FALSE);
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
$$;
