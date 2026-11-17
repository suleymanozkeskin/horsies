-- The reference side of the dual-runs whose reference calls an interim terminal write that
-- 0004_lifecycle_functions.sql replaced under another name: horsies_cancel_locked_task (test_cancel_task.py,
-- replaced by horsies_cancel_task) and horsies_fail_stale_task (test_stale_running.py, replaced by
-- horsies_finish_stale_attempt).
--
-- Each definition is the text of 0003_interim_functions.sql at commit 0c09b4b53, the last commit before the drop of
-- these functions from the baseline, unchanged except CREATE OR REPLACE. The file runs after the baseline install,
-- in the reference database only. Do not edit a body: the file is the record of the statements before the switch.

CREATE OR REPLACE FUNCTION horsies_cancel_locked_task(p_task_id uuid, p_permitted_source_statuses text[]) RETURNS SETOF horsies_terminalization_outcome
    LANGUAGE plpgsql
    AS $$
DECLARE
    v_task horsies_tasks;
    v_terminal_at timestamptz;
BEGIN
    PERFORM pg_advisory_xact_lock(
        hashtextextended(p_task_id::text, 731)
    );
    -- The caller chooses the permitted live source set; the database owns
    -- the fixed part: plain tasks only, live statuses only, and the
    -- operation's literals are not caller-supplied. The whole locked row:
    -- the move takes it as read here.
    SELECT t.*
    INTO v_task
    FROM horsies_tasks t
    WHERE t.id = p_task_id
      AND t.is_workflow_task = FALSE
      AND t.status IN ('PENDING', 'CLAIMED', 'RUNNING')
      AND t.status = ANY(p_permitted_source_statuses)
    FOR UPDATE;
    IF NOT FOUND THEN
        RETURN QUERY SELECT * FROM horsies_terminalization_miss(
            p_task_id, 'CANCEL_ADMIN', 'NONE',
            NULL::text, NULL::timestamptz
        );
        RETURN;
    END IF;

    v_terminal_at := NOW();
    PERFORM horsies_move_task_to_history(
        v_task, 'CANCELLED', 'CANCEL_ADMIN', v_terminal_at,
        NULL, 'TASK_CANCELLED', 'Cancelled via monitoring API'
    );
    RETURN QUERY SELECT
        p_task_id, NULL::bigint, 'APPLIED'::text,
        v_terminal_at, 'CANCEL_ADMIN'::text,
        v_task.status::text, v_task.claimed_by_worker_id, v_task.claimed_at,
        NULL::text, NULL::jsonb;
END
$$;

CREATE OR REPLACE FUNCTION horsies_fail_stale_task(p_task_id uuid, p_stale_after_ms integer, p_finalizing_stale_after_ms integer, p_result text, p_error_code text, p_failed_reason text) RETURNS SETOF horsies_terminalization_outcome
    LANGUAGE plpgsql
    AS $$
DECLARE
    v_capture record;
    v_task horsies_tasks;
    v_last_heartbeat timestamptz;
    v_evaluated_at timestamptz;
    v_terminal_at timestamptz;
BEGIN
    PERFORM pg_advisory_xact_lock(
        hashtextextended(p_task_id::text, 731)
    );
    -- One capture: the whole row locked (the move takes it as read here),
    -- the heartbeat read beside it, and the instant both arms are judged
    -- at. Nothing is reread after a refusal, so the evidence cannot show a
    -- heartbeat the guard never saw.
    SELECT t AS task,
           (
               -- Recency bound, derived not constant: a heartbeat older
               -- than stale_after satisfies the staleness comparison for
               -- every possible value, so excluding it cannot change the
               -- verdict; the conservative floor is the larger threshold.
               -- On partitioned heartbeat storage the bound prunes the
               -- probe to current leaves.
               SELECT h.sent_at
               FROM horsies_heartbeats h
               WHERE h.task_id = t.id AND h.role = 'runner'
                 AND h.sent_at >= NOW() - make_interval(
                     secs => GREATEST(
                         p_stale_after_ms, p_finalizing_stale_after_ms
                     )::double precision / 1000.0
                 )
               ORDER BY h.sent_at DESC
               LIMIT 1
           ) AS last_heartbeat,
           NOW() AS evaluated_at
    INTO v_capture
    FROM horsies_tasks t
    WHERE t.id = p_task_id
    FOR UPDATE;

    IF FOUND THEN
        v_task := v_capture.task;
        v_last_heartbeat := v_capture.last_heartbeat;
        v_evaluated_at := v_capture.evaluated_at;
    END IF;
    IF FOUND AND v_task.status = 'RUNNING' THEN
        IF v_task.started_at IS NOT NULL
           AND (
               v_task.finalizing_at IS NULL
               OR v_task.finalizing_at
                  < v_evaluated_at
                    - make_interval(
                        secs => p_finalizing_stale_after_ms::double precision
                            / 1000.0
                    )
           )
           AND COALESCE(v_last_heartbeat, v_task.started_at)
               < v_evaluated_at
                    - make_interval(
                        secs => p_stale_after_ms::double precision / 1000.0
                    )
        THEN
            v_terminal_at := NOW();
            PERFORM horsies_move_task_to_history(
                v_task, 'FAILED', 'FAIL_STALE', v_terminal_at,
                p_result, p_error_code, p_failed_reason
            );
            -- Cross-worker by design: the observed claim is whichever
            -- worker's silence the guard just judged, from the capture.
            RETURN QUERY SELECT
                p_task_id, NULL::bigint, 'APPLIED'::text,
                v_terminal_at, 'FAIL_STALE'::text,
                'RUNNING'::text, v_task.claimed_by_worker_id, v_task.claimed_at,
                NULL::text, NULL::jsonb;
            RETURN;
        END IF;

        RETURN QUERY SELECT
            p_task_id, NULL::bigint, 'SOURCE_STATE_CONFLICT'::text,
            NULL::timestamptz, NULL::text,
            v_task.status::text, v_task.claimed_by_worker_id, v_task.claimed_at,
            'STALENESS'::text,
            jsonb_build_object(
                'last_heartbeat_at', v_last_heartbeat,
                'started_at', v_task.started_at,
                'finalizing_at', v_task.finalizing_at,
                'stale_after_ms', p_stale_after_ms,
                'finalizing_stale_after_ms', p_finalizing_stale_after_ms,
                'evaluated_at', v_evaluated_at
            );
        RETURN;
    END IF;

    RETURN QUERY SELECT * FROM horsies_terminalization_miss(
        p_task_id, 'FAIL_STALE', 'NONE',
        NULL::text, NULL::timestamptz
    );
END
$$;
