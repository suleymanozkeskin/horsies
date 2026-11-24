-- Cases for the lifecycle record of migration 1001 (core/migrations/1001_lifecycle_record.sql), one session.
--
-- Run on a database that holds the migration chain, in one transaction that the caller rolls back
-- (core/src/sql_tests/lifecycle_record_sql.rs). NOW() is one instant for the whole script: the cases place row times
-- relative to it. Each case prints "ok: <case>"; a failed case raises and stops the script.
--
-- The time of a column in the attempt archive is its epoch microseconds (lr_us), as horsies_encode_attempt_archive
-- writes it. A statement does not see the writes of a function that it calls, so each check of what a writer wrote
-- reads it in a statement after the call.

SET client_min_messages = notice;

------------------------------------------------------------------------------------------------------------------------
-- Fixtures
------------------------------------------------------------------------------------------------------------------------

-- The published history readers (the duplicate guard of the moves, the detail reader of the monitoring read), as the
-- install publishes them. On a fresh install they probe the forever leaf.
SELECT outcome FROM horsies_publish_history_readers();

-- One heartbeat leaf around NOW(), with the probe index of the run-time leaves.
CREATE TABLE horsies_heartbeats_lr_window PARTITION OF horsies_heartbeats
    FOR VALUES FROM (now() - interval '2 days') TO (now() + interval '2 days');
CREATE INDEX horsies_heartbeats_lr_window_probe ON horsies_heartbeats_lr_window (task_id, role, sent_at DESC);

CREATE FUNCTION lr_check(p_case text, p_ok boolean) RETURNS void LANGUAGE plpgsql AS $$
BEGIN
    IF p_ok IS NOT TRUE THEN
        RAISE EXCEPTION 'case failed: %', p_case;
    END IF;
    RAISE NOTICE 'ok: %', p_case;
END
$$;

-- Runs p_statement in a subtransaction and checks that it fails with p_sqlstate. The subtransaction rolls the
-- statement back.
CREATE FUNCTION lr_expect_sqlstate(p_case text, p_statement text, p_sqlstate text) RETURNS void
LANGUAGE plpgsql AS $$
BEGIN
    BEGIN
        EXECUTE p_statement;
    EXCEPTION WHEN OTHERS THEN
        IF SQLSTATE <> p_sqlstate THEN
            RAISE EXCEPTION 'case failed: %: expected SQLSTATE %, got % (%)', p_case, p_sqlstate, SQLSTATE, SQLERRM;
        END IF;
        RAISE NOTICE 'ok: %', p_case;
        RETURN;
    END;
    RAISE EXCEPTION 'case failed: %: expected SQLSTATE %, the statement succeeded', p_case, p_sqlstate;
END
$$;

-- Runs p_statement in a subtransaction and checks that the CHECK p_constraint refuses it (23514). Any other error
-- fails the script.
CREATE FUNCTION lr_expect_check(p_case text, p_statement text, p_constraint text) RETURNS void
LANGUAGE plpgsql AS $$
DECLARE
    v_constraint text;
BEGIN
    BEGIN
        EXECUTE p_statement;
    EXCEPTION WHEN check_violation THEN
        GET STACKED DIAGNOSTICS v_constraint = CONSTRAINT_NAME;
        IF v_constraint IS DISTINCT FROM p_constraint THEN
            RAISE EXCEPTION 'case failed: %: expected the CHECK %, got %', p_case, p_constraint, v_constraint;
        END IF;
        RAISE NOTICE 'ok: %', p_case;
        RETURN;
    END;
    RAISE EXCEPTION 'case failed: %: expected the CHECK %, the statement succeeded', p_case, p_constraint;
END
$$;

-- The epoch microseconds of a time, as the attempt archive writes it.
CREATE FUNCTION lr_us(p_at timestamptz) RETURNS bigint LANGUAGE sql STABLE AS $$
    SELECT floor(extract(epoch FROM p_at) * 1000000)::bigint
$$;

-- The time p_us epoch microseconds after the epoch.
CREATE FUNCTION lr_at(p_us bigint) RETURNS timestamptz LANGUAGE sql STABLE AS $$
    SELECT timestamptz 'epoch' + p_us * interval '1 microsecond'
$$;

-- A live row with the given claim count of its current attempt. CLAIMED and RUNNING rows belong to p_worker at the
-- claim generation now() - 1 s; a CLAIMED row has a live lease; a RUNNING row started at now() - 500 ms. The row is
-- due since now() - 1 minute. Cases change single columns with UPDATE afterwards.
CREATE FUNCTION lr_task(
    p_id uuid, p_status text, p_claims integer, p_first_claimed_at timestamptz, p_queue text DEFAULT 'q_lr',
    p_is_workflow boolean DEFAULT FALSE, p_worker text DEFAULT 'w1'
) RETURNS void LANGUAGE sql AS $$
    INSERT INTO horsies_tasks (
        id, task_name, queue_name, priority, input, task_options, execution, retention_class_key, sent_at,
        created_at, command_fingerprint_version, command_fingerprint, idempotency_key_digest, status, enqueued_at,
        claimed_at, claimed_by_worker_id, claim_expires_at, started_at, finalizing_at, finalizing_by_worker_id,
        worker_pid, worker_hostname, worker_process_name, retry_count, max_retries, next_retry_at, good_until,
        is_workflow_task, updated_at, retain_rerun_input, rerun_of_task_id, rerun_root_task_id, trace_carrier,
        trace_id, attempt_claims, first_claimed_at
    ) VALUES (
        p_id, 'lr.task', p_queue, 100, '{"x":1}', NULL, 'ISOLATED', 'forever', now(),
        now(),
        CASE WHEN p_is_workflow THEN NULL ELSE 1::smallint END,
        CASE WHEN p_is_workflow THEN NULL ELSE sha256('lr'::bytea) END,
        NULL, p_status, now() - interval '1 minute',
        CASE WHEN p_status = 'PENDING' THEN NULL ELSE now() - interval '1 second' END,
        CASE WHEN p_status = 'PENDING' THEN NULL ELSE p_worker END,
        CASE WHEN p_status = 'CLAIMED' THEN now() + interval '1 minute' END,
        CASE WHEN p_status = 'RUNNING' THEN now() - interval '500 milliseconds' END,
        NULL, NULL,
        CASE WHEN p_status = 'RUNNING' THEN 7 END,
        CASE WHEN p_status = 'RUNNING' THEN 'host-a' END,
        CASE WHEN p_status = 'RUNNING' THEN 'worker-7' END,
        0, 3, NULL, NULL,
        p_is_workflow, now(), TRUE, NULL, NULL, NULL,
        NULL, p_claims, p_first_claimed_at
    )
$$;

-- One attempt row of a live task with the given phase record. The values of version 1 are fixed: FAILED, will
-- retry, started 1000 us and finished 2000 us after the epoch, error code E, worker w1 on host with pid 7 and
-- process proc.
CREATE FUNCTION lr_attempt(
    p_task uuid, p_attempt integer, p_version integer, p_enqueued_at timestamptz, p_claimed_at timestamptz,
    p_claims integer, p_first_claimed_at timestamptz, p_placed_at timestamptz, p_finalizing_at timestamptz,
    p_recorded_by text, p_last_seen_at timestamptz
) RETURNS void LANGUAGE sql AS $$
    INSERT INTO horsies_task_attempts (
        task_id, attempt, outcome, will_retry, started_at, finished_at, error_code, error_message, failed_reason,
        worker_id, worker_hostname, worker_pid, worker_process_name, phase_record_version, enqueued_at, claimed_at,
        attempt_claims, first_claimed_at, placed_at, finalizing_at, recorded_by, last_seen_at
    ) VALUES (
        p_task, p_attempt, 'FAILED', TRUE, lr_at(1000), lr_at(2000), 'E', NULL, NULL,
        'w1', 'host', 7, 'proc', p_version, p_enqueued_at, p_claimed_at,
        p_claims, p_first_claimed_at, p_placed_at, p_finalizing_at, p_recorded_by, p_last_seen_at
    )
$$;

CREATE FUNCTION lr_workflow(p_id uuid, p_status text) RETURNS void LANGUAGE sql AS $$
    INSERT INTO horsies_workflows (
        id, name, definition_key, status, on_error, output_task_index, success_policy, result, error,
        parent_workflow_id, parent_task_index, root_workflow_id, depth, sent_at, created_at, started_at,
        completed_at, updated_at
    ) VALUES (
        p_id, 'lr.workflow', NULL, p_status, 'fail', NULL, NULL, NULL, NULL,
        NULL, NULL, p_id, 0, now(), now(), now(), NULL, now()
    )
$$;

CREATE FUNCTION lr_node(p_id uuid, p_workflow uuid, p_index integer, p_status text, p_task uuid)
RETURNS void LANGUAGE sql AS $$
    INSERT INTO horsies_workflow_tasks (downstream_subworkflow,
        id, workflow_id, task_index, node_id, task_name, input, queue_name, priority, dependencies, args_from,
        workflow_ctx_from, allow_failed_deps, join_type, min_success, task_options, max_retries, execution,
        good_until, retention_class_key, status, task_id, is_subworkflow, sub_workflow_id, sub_workflow_name,
        sub_definition_key, sub_workflow_summary, result, error, created_at, started_at, completed_at
    ) VALUES ('NONE',
        p_id, p_workflow, p_index, 'node_' || p_index, 'lr.task', '{"x":1}', 'q_lr', 100, '{}', NULL,
        NULL, FALSE, 'all', NULL, NULL, 1, 'ISOLATED',
        NULL, 'forever', p_status, p_task, FALSE, NULL, NULL,
        NULL, NULL, NULL, NULL, now(),
        CASE WHEN p_status = 'RUNNING' THEN now() - interval '1 hour' END, NULL
    )
$$;

-- One unkeyed first send of lr.task to p_queue; p_lineage is FIRST_RUN or RERUN.
CREATE FUNCTION lr_send(p_id uuid, p_queue text, p_lineage text) RETURNS text LANGUAGE sql AS $$
    SELECT outcome FROM horsies_enqueue_task(
        p_id, 'lr.task', p_queue, 100, '{"x":1}', NULL, 'ISOLATED', 'forever', TRUE, 3, NULL,
        now(), 'NOW', NULL, NULL, 1::smallint, sha256(p_id::text::bytea),
        p_lineage,
        CASE WHEN p_lineage = 'RERUN' THEN '00000000-0000-7000-8000-0000000effff'::uuid END,
        CASE WHEN p_lineage = 'RERUN' THEN '00000000-0000-7000-8000-0000000efffe'::uuid END,
        'UNKEYED', NULL, NULL, NULL, 'FIRST', 'NO_CARRIER', NULL, NULL
    )
$$;

-- One claim pass of p_worker over p_queue: at most one ISOLATED row with a lease of one minute. Returns the claimed
-- ids, comma-separated.
CREATE FUNCTION lr_claim(p_worker text, p_queue text) RETURNS text LANGUAGE sql AS $$
    SELECT string_agg(c.id::text, ',') FROM horsies_claim(
        p_worker, jsonb_build_array(p_queue), '{}'::jsonb, '{}'::jsonb, FALSE,
        '{"ISOLATED": 1}'::jsonb, '{"ISOLATED": "ISOLATED"}'::jsonb,
        0, 1, 1, NULL, 60000, '[]'::jsonb
    ) AS c WHERE c.row_kind = 'CLAIMED'
$$;

-- The claim count and the first claim time of a live row, as 'count|first'.
CREATE FUNCTION lr_claims(p_id uuid) RETURNS text LANGUAGE sql AS $$
    SELECT concat_ws('|', coalesce(t.attempt_claims::text, 'null'), coalesce(t.first_claimed_at::text, 'null'))
    FROM horsies_tasks AS t WHERE t.id = p_id
$$;

-- The attempt p_attempt in the archive of the history row of p_task, as JSON.
CREATE FUNCTION lr_archived(p_task uuid, p_attempt integer) RETURNS jsonb LANGUAGE sql AS $$
    SELECT convert_from(h.attempt_snapshot, 'UTF8')::jsonb -> 0 -> (p_attempt - 1)
    FROM horsies_task_history AS h WHERE h.task_id = p_task
$$;

-- The open attempt in the archive of the history row of p_task, as JSON.
CREATE FUNCTION lr_archived_open(p_task uuid) RETURNS jsonb LANGUAGE sql AS $$
    SELECT convert_from(h.attempt_snapshot, 'UTF8')::jsonb -> 1
    FROM horsies_task_history AS h WHERE h.task_id = p_task
$$;

-- The nine phase values of an attempt in the archive.
CREATE FUNCTION lr_phase_values(p_attempt jsonb) RETURNS jsonb LANGUAGE sql AS $$
    SELECT jsonb_path_query_array(p_attempt, '$[12 to 20]')
$$;

-- The phase record of a live attempt row, in the order of the archive.
CREATE FUNCTION lr_attempt_phases(p_task uuid, p_attempt integer) RETURNS jsonb LANGUAGE sql AS $$
    SELECT jsonb_build_array(
        a.phase_record_version, lr_us(a.enqueued_at), lr_us(a.claimed_at), a.attempt_claims,
        lr_us(a.first_claimed_at), lr_us(a.placed_at), lr_us(a.finalizing_at), a.recorded_by, lr_us(a.last_seen_at)
    )
    FROM horsies_task_attempts AS a WHERE a.task_id = p_task AND a.attempt = p_attempt
$$;

------------------------------------------------------------------------------------------------------------------------
-- The CHECKs of the new columns
------------------------------------------------------------------------------------------------------------------------

SELECT lr_expect_check('tasks CHECK: no count with a first claim',
    $q$SELECT lr_task('00000000-0000-7000-8000-0000000e0001', 'PENDING', NULL, now())$q$,
    'horsies_tasks_attempt_claims_shape');
SELECT lr_expect_check('tasks CHECK: count 0 with a first claim',
    $q$SELECT lr_task('00000000-0000-7000-8000-0000000e0001', 'PENDING', 0, now())$q$,
    'horsies_tasks_attempt_claims_shape');
SELECT lr_expect_check('tasks CHECK: count 0 on a CLAIMED row',
    $q$SELECT lr_task('00000000-0000-7000-8000-0000000e0001', 'CLAIMED', 0, NULL)$q$,
    'horsies_tasks_attempt_claims_shape');
SELECT lr_expect_check('tasks CHECK: count 0 on a RUNNING row',
    $q$SELECT lr_task('00000000-0000-7000-8000-0000000e0001', 'RUNNING', 0, NULL)$q$,
    'horsies_tasks_attempt_claims_shape');
SELECT lr_expect_check('tasks CHECK: count 1 with no first claim',
    $q$SELECT lr_task('00000000-0000-7000-8000-0000000e0001', 'CLAIMED', 1, NULL)$q$,
    'horsies_tasks_attempt_claims_shape');
SELECT lr_expect_check('tasks CHECK: a negative count',
    $q$SELECT lr_task('00000000-0000-7000-8000-0000000e0001', 'CLAIMED', -1, now())$q$,
    'horsies_tasks_attempt_claims_shape');
-- The legal shapes: not counted, never claimed, released after two claims, claimed, running.
SELECT lr_task('00000000-0000-7000-8000-0000000e0002', 'PENDING', NULL, NULL);
SELECT lr_task('00000000-0000-7000-8000-0000000e0003', 'RUNNING', NULL, NULL);
SELECT lr_task('00000000-0000-7000-8000-0000000e0004', 'PENDING', 0, NULL);
SELECT lr_task('00000000-0000-7000-8000-0000000e0005', 'PENDING', 2, now() - interval '3 seconds');
SELECT lr_task('00000000-0000-7000-8000-0000000e0006', 'CLAIMED', 1, now() - interval '1 second');
SELECT lr_task('00000000-0000-7000-8000-0000000e0007', 'RUNNING', 3, now() - interval '3 seconds');
SELECT lr_check(
    'tasks CHECK: the legal shapes are accepted',
    (SELECT count(*) = 6 FROM horsies_tasks
     WHERE id BETWEEN '00000000-0000-7000-8000-0000000e0002' AND '00000000-0000-7000-8000-0000000e0007')
);

-- The parent of the attempt cases: each refused insert rolls back with its subtransaction.
SELECT lr_task('00000000-0000-7000-8000-0000000e0010', 'RUNNING', 1, now() - interval '1 second');
SELECT lr_expect_check('attempts CHECK: a phase value with no version',
    $q$SELECT lr_attempt('00000000-0000-7000-8000-0000000e0010', 1, NULL, now(), NULL, NULL, NULL, NULL, NULL,
        NULL, NULL)$q$,
    'horsies_task_attempts_phase_record_shape');
SELECT lr_expect_check('attempts CHECK: version 2',
    $q$SELECT lr_attempt('00000000-0000-7000-8000-0000000e0010', 1, 2, now(), now(), 1, now(), NULL, NULL,
        'WORKER', NULL)$q$,
    'horsies_task_attempts_phase_record_shape');
SELECT lr_expect_check('attempts CHECK: version 1 with no enqueued_at',
    $q$SELECT lr_attempt('00000000-0000-7000-8000-0000000e0010', 1, 1, NULL, now(), 1, now(), NULL, NULL,
        'WORKER', NULL)$q$,
    'horsies_task_attempts_phase_record_shape');
SELECT lr_expect_check('attempts CHECK: version 1 with no claimed_at',
    $q$SELECT lr_attempt('00000000-0000-7000-8000-0000000e0010', 1, 1, now(), NULL, 1, now(), NULL, NULL,
        'WORKER', NULL)$q$,
    'horsies_task_attempts_phase_record_shape');
SELECT lr_expect_check('attempts CHECK: a count with no first claim',
    $q$SELECT lr_attempt('00000000-0000-7000-8000-0000000e0010', 1, 1, now(), now(), 1, NULL, NULL, NULL,
        'WORKER', NULL)$q$,
    'horsies_task_attempts_phase_record_shape');
SELECT lr_expect_check('attempts CHECK: a first claim with no count',
    $q$SELECT lr_attempt('00000000-0000-7000-8000-0000000e0010', 1, 1, now(), now(), NULL, now(), NULL, NULL,
        'WORKER', NULL)$q$,
    'horsies_task_attempts_phase_record_shape');
SELECT lr_expect_check('attempts CHECK: a count of 0',
    $q$SELECT lr_attempt('00000000-0000-7000-8000-0000000e0010', 1, 1, now(), now(), 0, now(), NULL, NULL,
        'WORKER', NULL)$q$,
    'horsies_task_attempts_phase_record_shape');
SELECT lr_expect_check('attempts CHECK: no recorded_by',
    $q$SELECT lr_attempt('00000000-0000-7000-8000-0000000e0010', 1, 1, now(), now(), 1, now(), NULL, NULL,
        NULL, NULL)$q$,
    'horsies_task_attempts_phase_record_shape');
SELECT lr_expect_check('attempts CHECK: a recorded_by outside WORKER, RECOVERY and CANCEL',
    $q$SELECT lr_attempt('00000000-0000-7000-8000-0000000e0010', 1, 1, now(), now(), 1, now(), NULL, NULL,
        'OPERATOR', NULL)$q$,
    'horsies_task_attempts_phase_record_shape');
SELECT lr_expect_check('attempts CHECK: WORKER with a last_seen_at',
    $q$SELECT lr_attempt('00000000-0000-7000-8000-0000000e0010', 1, 1, now(), now(), 1, now(), NULL, NULL,
        'WORKER', now())$q$,
    'horsies_task_attempts_phase_record_shape');
SELECT lr_expect_check('attempts CHECK: RECOVERY with no last_seen_at',
    $q$SELECT lr_attempt('00000000-0000-7000-8000-0000000e0010', 1, 1, now(), now(), 1, now(), NULL, NULL,
        'RECOVERY', NULL)$q$,
    'horsies_task_attempts_phase_record_shape');
SELECT lr_expect_check('attempts CHECK: RECOVERY with a placed_at',
    $q$SELECT lr_attempt('00000000-0000-7000-8000-0000000e0010', 1, 1, now(), now(), 1, now(), now(), NULL,
        'RECOVERY', now())$q$,
    'horsies_task_attempts_phase_record_shape');
-- The legal shapes: no phase record; the worker, counted, placed and marked; the worker, not counted; recovery.
SELECT lr_attempt('00000000-0000-7000-8000-0000000e0010', 1, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
SELECT lr_attempt('00000000-0000-7000-8000-0000000e0010', 2, 1, now(), now(), 2, now(), now(), now(), 'WORKER', NULL);
SELECT lr_attempt('00000000-0000-7000-8000-0000000e0010', 3, 1, now(), now(), NULL, NULL, NULL, NULL, 'WORKER', NULL);
SELECT lr_attempt('00000000-0000-7000-8000-0000000e0010', 4, 1, now(), now(), 1, now(), NULL, now(), 'RECOVERY',
                  now());
SELECT lr_check(
    'attempts CHECK: the legal shapes are accepted',
    (SELECT count(*) = 4 FROM horsies_task_attempts WHERE task_id = '00000000-0000-7000-8000-0000000e0010')
);

------------------------------------------------------------------------------------------------------------------------
-- The functions of the migration
------------------------------------------------------------------------------------------------------------------------

SELECT lr_check(
    'functions: every function of the migration exists with its new argument list',
    to_regprocedure('horsies_complete_task_fused(uuid, text, timestamptz, text, boolean, text, bigint)') IS NOT NULL
    AND to_regprocedure(
        'horsies_complete_tasks_fused(uuid[], timestamptz[], text[], text, boolean, text[], bigint[])') IS NOT NULL
    AND to_regprocedure('horsies_finish_workflow_success(uuid, text, timestamptz, text, text, bigint)') IS NOT NULL
    AND to_regprocedure('horsies_finish_failed_attempt(uuid, text, timestamptz, text, integer, double precision, '
        || 'text, text, text, text, text, text, text, text, boolean, text, bigint)') IS NOT NULL
    AND to_regprocedure('horsies_encode_attempt_archive(horsies_tasks)') IS NOT NULL
    AND to_regprocedure('horsies_require_attempt_placement(text, text, bigint)') IS NOT NULL
);
SELECT lr_check(
    'functions: no argument list of migration 1000 remains',
    to_regprocedure('horsies_complete_task_fused(uuid, text, timestamptz, text, boolean)') IS NULL
    AND to_regprocedure('horsies_complete_tasks_fused(uuid[], timestamptz[], text[], text, boolean)') IS NULL
    AND to_regprocedure('horsies_finish_workflow_success(uuid, text, timestamptz, text)') IS NULL
    AND to_regprocedure('horsies_finish_failed_attempt(uuid, text, timestamptz, text, integer, double precision, '
        || 'text, text, text, text, text, text, text, text, boolean)') IS NULL
    AND to_regprocedure('horsies_encode_task_attempts(uuid)') IS NULL
    AND (SELECT count(*) FROM pg_proc AS p
         WHERE p.proname IN ('horsies_complete_task_fused', 'horsies_complete_tasks_fused',
                             'horsies_finish_workflow_success', 'horsies_finish_failed_attempt')
           AND p.pronamespace = (SELECT oid FROM pg_namespace WHERE nspname = current_schema())) = 4
);

------------------------------------------------------------------------------------------------------------------------
-- The claim count of the current attempt
------------------------------------------------------------------------------------------------------------------------

-- Every insert path: a send, a rerun, a schedule fire (the workflow start is in baseline_workflow_functions.sql).
SELECT lr_send('00000000-0000-7000-8000-0000000e0101', 'q_lr_send', 'FIRST_RUN');
SELECT lr_send('00000000-0000-7000-8000-0000000e0102', 'q_lr_send', 'RERUN');
INSERT INTO horsies_schedule_state (
    schedule_name, next_run_at, last_run_at, last_task_id, run_count, config_hash, updated_at
) VALUES ('lr-schedule', now() - interval '1 minute', NULL, NULL, 0, sha256('lr-schedule'::bytea), now());
CREATE TEMP TABLE lr_fired AS
    SELECT f.task_id FROM horsies_fire_schedule(
        'lr-schedule', now() - interval '1 minute', now() + interval '1 hour', ARRAY[now() - interval '1 minute'],
        'lr.task', 'q_lr_fire', 100, '{}', NULL, 'ISOLATED', 1, 'forever', TRUE, 1::smallint,
        sha256('lr-schedule'::bytea)
    ) AS f
    WHERE f.row_kind = 'SLOT';
SELECT lr_check(
    'insert: a send, a rerun and a schedule fire start their first attempt with count 0 and no first claim',
    (SELECT count(*) = 3 AND bool_and(t.status = 'PENDING' AND t.attempt_claims = 0 AND t.first_claimed_at IS NULL)
     FROM horsies_tasks AS t
     WHERE t.id IN ('00000000-0000-7000-8000-0000000e0101', '00000000-0000-7000-8000-0000000e0102')
        OR t.id IN (SELECT task_id FROM lr_fired))
);

-- One row through its claims: claimed, lease lost and claimed again, released, claimed, requeued as stale, claimed.
SELECT lr_send('00000000-0000-7000-8000-0000000e0111', 'q_lr_claim', 'FIRST_RUN');
UPDATE horsies_tasks SET enqueued_at = now() - interval '1 minute' WHERE id = '00000000-0000-7000-8000-0000000e0111';
SELECT lr_check(
    'claim: the first claim takes the row',
    lr_claim('w-lr-claim', 'q_lr_claim') = '00000000-0000-7000-8000-0000000e0111'
);
SELECT lr_check(
    'claim: the first claim counts 1 and keeps its claim generation as the first claim',
    (SELECT t.attempt_claims = 1 AND t.first_claimed_at = now() AND t.claimed_at = now()
     FROM horsies_tasks AS t WHERE t.id = '00000000-0000-7000-8000-0000000e0111')
);
-- The first claim moves one minute back, and its lease expires: the next claim is a new claim generation.
UPDATE horsies_tasks
SET claimed_at = now() - interval '1 minute', first_claimed_at = now() - interval '1 minute',
    claim_expires_at = now() - interval '1 second'
WHERE id = '00000000-0000-7000-8000-0000000e0111';
SELECT lr_check(
    'claim: the claim of the expired lease takes the row',
    lr_claim('w-lr-claim', 'q_lr_claim') = '00000000-0000-7000-8000-0000000e0111'
);
SELECT lr_check(
    'claim: a claim of an expired lease counts 2, keeps the first claim and writes a new claim generation',
    (SELECT t.attempt_claims = 2 AND t.first_claimed_at = now() - interval '1 minute' AND t.claimed_at = now()
     FROM horsies_tasks AS t WHERE t.id = '00000000-0000-7000-8000-0000000e0111')
);
SELECT lr_check(
    'release: the claim is released',
    (SELECT string_agg(outcome, ',') FROM horsies_release_claims(
        ARRAY['00000000-0000-7000-8000-0000000e0111']::uuid[], ARRAY[now()], 'w-lr-claim', 'SAME_CLASS')) = 'RELEASED'
);
SELECT lr_check(
    'release: the released row is PENDING and keeps the count and the first claim',
    (SELECT t.status = 'PENDING' AND t.claimed_at IS NULL AND t.attempt_claims = 2
            AND t.first_claimed_at = now() - interval '1 minute'
     FROM horsies_tasks AS t WHERE t.id = '00000000-0000-7000-8000-0000000e0111')
);
SELECT lr_check(
    'release: the next claim takes the row',
    lr_claim('w-lr-claim', 'q_lr_claim') = '00000000-0000-7000-8000-0000000e0111'
);
SELECT lr_check(
    'release: the next claim counts 3 on the same attempt',
    lr_claims('00000000-0000-7000-8000-0000000e0111') = '3|' || (now() - interval '1 minute')::text
);
SELECT lr_check(
    'stale requeue: the stale claim is requeued',
    (SELECT count(*) = 1 FROM horsies_requeue_stale_claims(
        'OWNER_GONE', NULL, 'w-lr-claim', now() + interval '1 second', 10))
);
SELECT lr_check(
    'stale requeue: the requeued row is PENDING and keeps the count and the first claim',
    (SELECT t.status = 'PENDING' AND t.attempt_claims = 3 AND t.first_claimed_at = now() - interval '1 minute'
     FROM horsies_tasks AS t WHERE t.id = '00000000-0000-7000-8000-0000000e0111')
);
SELECT lr_check(
    'stale requeue: the next claim takes the row',
    lr_claim('w-lr-claim', 'q_lr_claim') = '00000000-0000-7000-8000-0000000e0111'
);
SELECT lr_check(
    'stale requeue: the next claim counts 4 on the same attempt',
    lr_claims('00000000-0000-7000-8000-0000000e0111') = '4|' || (now() - interval '1 minute')::text
);

-- A row whose attempt began before the migration: its claims are not counted until its next attempt.
SELECT lr_task('00000000-0000-7000-8000-0000000e0121', 'PENDING', NULL, NULL, 'q_lr_legacy');
SELECT lr_check(
    'not counted: the claim takes the row',
    lr_claim('w-lr-legacy', 'q_lr_legacy') = '00000000-0000-7000-8000-0000000e0121'
);
SELECT lr_check(
    'not counted: a claim keeps the count and the first claim NULL',
    lr_claims('00000000-0000-7000-8000-0000000e0121') = 'null|null'
);
SELECT lr_check(
    'not counted: the attempt starts',
    (SELECT outcome FROM horsies_start_attempt(
        '00000000-0000-7000-8000-0000000e0121', 'w-lr-legacy', now(), 9, 'host-l', 'worker-9')) = 'STARTED'
);
SELECT lr_check(
    'not counted: the failed attempt schedules a retry',
    (SELECT outcome FROM horsies_finish_failed_attempt(
        '00000000-0000-7000-8000-0000000e0121', 'w-lr-legacy', now(), 'RETRY', 0, 5.0, 'SAME_CLASS', '{"e":1}', 'E',
        'r', 'FAILED', 'm', 'r', 'REFUSE', TRUE, 'NOT_OBSERVED', NULL)) = 'RETRY_SCHEDULED'
);
SELECT lr_check(
    'not counted: the retry starts the next attempt at count 0, and the attempt row is not counted',
    lr_claims('00000000-0000-7000-8000-0000000e0121') = '0|null'
    AND lr_attempt_phases('00000000-0000-7000-8000-0000000e0121', 1)
        = jsonb_build_array(1, lr_us(now() - interval '1 minute'), lr_us(now()), NULL, NULL, NULL, NULL, 'WORKER',
                            NULL)
);

------------------------------------------------------------------------------------------------------------------------
-- The phase record of each writer
------------------------------------------------------------------------------------------------------------------------

-- Plain success: placed 250 ms after the claim, a finalizing marker, two claims.
SELECT lr_task('00000000-0000-7000-8000-0000000e0201', 'RUNNING', 2, now() - interval '3 seconds');
UPDATE horsies_tasks SET finalizing_at = now() - interval '100 milliseconds', finalizing_by_worker_id = 'w1'
WHERE id = '00000000-0000-7000-8000-0000000e0201';
SELECT lr_check(
    'plain success: applied',
    (SELECT outcome FROM horsies_complete_task_fused(
        '00000000-0000-7000-8000-0000000e0201', 'w1', now() - interval '1 second', '{"ok":1}', TRUE, 'PLACED',
        250000)) = 'APPLIED'
);
SELECT lr_check(
    'plain success: the archived attempt has the phase record of the worker, the placement and the marker',
    lr_phase_values(lr_archived('00000000-0000-7000-8000-0000000e0201', 1)) = jsonb_build_array(
        1, lr_us(now() - interval '1 minute'), lr_us(now() - interval '1 second'), 2,
        lr_us(now() - interval '3 seconds'), lr_us(now() - interval '750 milliseconds'),
        lr_us(now() - interval '100 milliseconds'), 'WORKER', NULL)
);
SELECT lr_check(
    'move after a success: archive version 2 with no open attempt',
    (SELECT h.attempt_archive_version = 2 AND h.attempt_snapshot_digest = sha256(h.attempt_snapshot)
     FROM horsies_task_history AS h WHERE h.task_id = '00000000-0000-7000-8000-0000000e0201')
    AND lr_archived_open('00000000-0000-7000-8000-0000000e0201') = 'null'::jsonb
);

-- Batch success: each row its own placement.
SELECT lr_task('00000000-0000-7000-8000-0000000e0211', 'RUNNING', 1, now() - interval '1 second');
SELECT lr_task('00000000-0000-7000-8000-0000000e0212', 'RUNNING', 1, now() - interval '1 second');
SELECT lr_check(
    'batch success: each row applied',
    (SELECT string_agg(outcome, ',' ORDER BY ordinality) FROM horsies_complete_tasks_fused(
        ARRAY['00000000-0000-7000-8000-0000000e0212', '00000000-0000-7000-8000-0000000e0211']::uuid[],
        ARRAY[now() - interval '1 second', now() - interval '1 second'], ARRAY['{"ok":12}', '{"ok":11}'], 'w1', TRUE,
        ARRAY['NOT_OBSERVED', 'PLACED'], ARRAY[NULL, 1000]::bigint[])) = 'APPLIED,APPLIED'
);
SELECT lr_check(
    'batch success: each row keeps its own placement',
    lr_archived('00000000-0000-7000-8000-0000000e0211', 1) -> 17
        = to_jsonb(lr_us(now() - interval '1 second') + 1000)
    AND lr_archived('00000000-0000-7000-8000-0000000e0212', 1) -> 17 = 'null'::jsonb
    AND lr_archived('00000000-0000-7000-8000-0000000e0212', 1) -> 19 = '"WORKER"'::jsonb
);
SELECT lr_expect_sqlstate('batch success: fewer placement kinds than rows', $q$
    SELECT * FROM horsies_complete_tasks_fused(
        ARRAY[gen_random_uuid(), gen_random_uuid()], ARRAY[now(), now()], ARRAY['{}', '{}'], 'w1', TRUE,
        ARRAY['NOT_OBSERVED'], ARRAY[NULL, NULL]::bigint[])$q$, '22023');
SELECT lr_expect_sqlstate('batch success: more slot waits than rows', $q$
    SELECT * FROM horsies_complete_tasks_fused(
        ARRAY[gen_random_uuid(), gen_random_uuid()], ARRAY[now(), now()], ARRAY['{}', '{}'], 'w1', TRUE,
        ARRAY['NOT_OBSERVED', 'NOT_OBSERVED'], ARRAY[NULL, NULL, NULL]::bigint[])$q$, '22023');
SELECT lr_expect_sqlstate('batch success: a NULL placement array', $q$
    SELECT * FROM horsies_complete_tasks_fused(
        ARRAY[gen_random_uuid()], ARRAY[now()], ARRAY['{}'], 'w1', TRUE, NULL, ARRAY[NULL]::bigint[])$q$, '22023');

-- Workflow success: placed at the claim (a slot wait of 0), no marker.
SELECT lr_workflow('00000000-0000-7000-8000-0000000e0e01', 'RUNNING');
SELECT lr_task('00000000-0000-7000-8000-0000000e0221', 'RUNNING', 1, now() - interval '1 second', 'q_lr', TRUE);
SELECT lr_node('00000000-0000-7000-8000-0000000e0f21', '00000000-0000-7000-8000-0000000e0e01', 0, 'RUNNING',
               '00000000-0000-7000-8000-0000000e0221');
SELECT lr_check(
    'workflow success: applied',
    (SELECT outcome FROM horsies_finish_workflow_success(
        '00000000-0000-7000-8000-0000000e0221', 'w1', now() - interval '1 second', '{"ok":21}', 'PLACED', 0))
        = 'APPLIED'
);
SELECT lr_check(
    'workflow success: the archived attempt has the phase record of the worker',
    lr_phase_values(lr_archived('00000000-0000-7000-8000-0000000e0221', 1)) = jsonb_build_array(
        1, lr_us(now() - interval '1 minute'), lr_us(now() - interval '1 second'), 1,
        lr_us(now() - interval '1 second'), lr_us(now() - interval '1 second'), NULL, 'WORKER', NULL)
    AND lr_archived_open('00000000-0000-7000-8000-0000000e0221') = 'null'::jsonb
);

-- A failed attempt with a retry: the attempt row stays live with its phase record.
SELECT lr_task('00000000-0000-7000-8000-0000000e0231', 'RUNNING', 2, now() - interval '3 seconds');
SELECT lr_check(
    'failed attempt: a retry is scheduled',
    (SELECT outcome FROM horsies_finish_failed_attempt(
        '00000000-0000-7000-8000-0000000e0231', 'w1', now() - interval '1 second', 'RETRY', 0, 5.0, 'SAME_CLASS',
        '{"e":31}', 'E', 'r', 'FAILED', 'm', 'r', 'REFUSE', TRUE, 'PLACED', 400)) = 'RETRY_SCHEDULED'
);
SELECT lr_check(
    'failed attempt: the attempt row has the phase record of the worker, with no finalizing marker',
    lr_attempt_phases('00000000-0000-7000-8000-0000000e0231', 1) = jsonb_build_array(
        1, lr_us(now() - interval '1 minute'), lr_us(now() - interval '1 second'), 2,
        lr_us(now() - interval '3 seconds'), lr_us(now() - interval '1 second') + 400, NULL, 'WORKER', NULL)
);
SELECT lr_check(
    'failed attempt: the retry starts the next attempt at count 0 with no first claim',
    (SELECT t.status = 'PENDING' AND t.retry_count = 1 AND t.attempt_claims = 0 AND t.first_claimed_at IS NULL
     FROM horsies_tasks AS t WHERE t.id = '00000000-0000-7000-8000-0000000e0231')
);
SELECT lr_check(
    'failed attempt: NOT_OBSERVED gives no placed_at',
    (SELECT a.placed_at IS NULL AND a.phase_record_version = 1 AND a.recorded_by = 'WORKER'
     FROM horsies_task_attempts AS a WHERE a.task_id = '00000000-0000-7000-8000-0000000e0121' AND a.attempt = 1)
);

-- The release rule of a CLAIMED row: no attempt row; the count stays on the attempt.
SELECT lr_task('00000000-0000-7000-8000-0000000e0241', 'CLAIMED', 1, now() - interval '1 second');
SELECT lr_check(
    'failed attempt: RELEASE of a CLAIMED row releases it',
    (SELECT outcome FROM horsies_finish_failed_attempt(
        '00000000-0000-7000-8000-0000000e0241', 'w1', now() - interval '1 second', 'RETRY', 0, 1.0, 'SAME_CLASS',
        '{"e":41}', 'WORKER_CRASHED', 'r', 'WORKER_FAILURE', 'm', 'r', 'RELEASE', TRUE, 'PLACED', 10)) = 'RELEASED'
);
SELECT lr_check(
    'failed attempt: RELEASE of a CLAIMED row writes no attempt row and keeps the count',
    NOT EXISTS (SELECT 1 FROM horsies_task_attempts WHERE task_id = '00000000-0000-7000-8000-0000000e0241')
    AND lr_claims('00000000-0000-7000-8000-0000000e0241') = '1|' || (now() - interval '1 second')::text
    AND (SELECT status FROM horsies_tasks WHERE id = '00000000-0000-7000-8000-0000000e0241') = 'PENDING'
);

-- Every placement pair that a writer refuses (22023), for each of the four writers.
CREATE FUNCTION lr_placement_call(p_writer text, p_kind text, p_wait_us bigint) RETURNS text LANGUAGE sql AS $$
    SELECT CASE p_writer
        WHEN 'single' THEN format(
            $q$SELECT * FROM horsies_complete_task_fused(gen_random_uuid(), 'w1', now(), '{}', TRUE, %L, %L)$q$,
            p_kind, p_wait_us)
        WHEN 'batch' THEN format(
            $q$SELECT * FROM horsies_complete_tasks_fused(ARRAY[gen_random_uuid()], ARRAY[now()], ARRAY['{}'], 'w1',
                TRUE, ARRAY[%L]::text[], ARRAY[%L]::bigint[])$q$,
            p_kind, p_wait_us)
        WHEN 'workflow' THEN format(
            $q$SELECT * FROM horsies_finish_workflow_success(gen_random_uuid(), 'w1', now(), '{}', %L, %L)$q$,
            p_kind, p_wait_us)
        WHEN 'failed' THEN format(
            $q$SELECT * FROM horsies_finish_failed_attempt(gen_random_uuid(), 'w1', now(), 'NONE', NULL, NULL, NULL,
                '{}', 'E', 'r', 'FAILED', 'm', 'r', 'REFUSE', TRUE, %L, %L)$q$,
            p_kind, p_wait_us)
    END
$$;
DO $$
DECLARE
    v_writer text;
    v_pair record;
BEGIN
    FOREACH v_writer IN ARRAY ARRAY['single', 'batch', 'workflow', 'failed'] LOOP
        FOR v_pair IN
            SELECT p.kind, p.wait_us, p.label
            FROM (VALUES
                ('PLACED', -1::bigint, 'a negative slot wait'),
                ('PLACED', NULL::bigint, 'PLACED with no slot wait'),
                ('NOT_OBSERVED', 5::bigint, 'NOT_OBSERVED with a slot wait'),
                (NULL, NULL::bigint, 'no placement kind'),
                ('placed', 5::bigint, 'an unknown placement kind')
            ) AS p(kind, wait_us, label)
        LOOP
            PERFORM lr_expect_sqlstate(
                format('placement refused by the %s writer: %s', v_writer, v_pair.label),
                lr_placement_call(v_writer, v_pair.kind, v_pair.wait_us),
                '22023'
            );
        END LOOP;
    END LOOP;
END
$$;

------------------------------------------------------------------------------------------------------------------------
-- Stale RUNNING recovery
------------------------------------------------------------------------------------------------------------------------

-- BEAT_AGE, a runner beat older than the recency bound (120 s): the row is stale, and the beat is still the last
-- time the attempt was seen.
SELECT lr_task('00000000-0000-7000-8000-0000000e0301', 'RUNNING', 1, now() - interval '1 second');
UPDATE horsies_tasks SET started_at = now() - interval '10 minutes' WHERE id = '00000000-0000-7000-8000-0000000e0301';
INSERT INTO horsies_heartbeats (task_id, sender_id, role, sent_at, hostname, pid) VALUES
    ('00000000-0000-7000-8000-0000000e0301', 'w1:7', 'runner', now() - interval '9 minutes', 'host-a', 7);
SELECT lr_check(
    'stale attempt (BEAT_AGE): a beat outside the recency bound is stale, and a retry is scheduled',
    (SELECT outcome FROM horsies_finish_stale_attempt(
        '00000000-0000-7000-8000-0000000e0301', 'BEAT_AGE', 60000, 120000, NULL, NULL, 'RETRY', 0, 5.0,
        '{"err":"crashed"}', 'WORKER_CRASHED', 'r', 'm', 'r')) = 'RETRY_SCHEDULED'
);
SELECT lr_check(
    'stale attempt (BEAT_AGE): RECOVERY, no placement, last seen at the newest beat outside the recency bound',
    lr_attempt_phases('00000000-0000-7000-8000-0000000e0301', 1) = jsonb_build_array(
        1, lr_us(now() - interval '1 minute'), lr_us(now() - interval '1 second'), 1,
        lr_us(now() - interval '1 second'), NULL, NULL, 'RECOVERY', lr_us(now() - interval '9 minutes'))
    AND lr_claims('00000000-0000-7000-8000-0000000e0301') = '0|null'
);

-- BEAT_AGE, no beat, an old finalizing marker: last seen at the marker; the row moves.
SELECT lr_task('00000000-0000-7000-8000-0000000e0302', 'RUNNING', 1, now() - interval '1 second');
UPDATE horsies_tasks SET started_at = now() - interval '10 minutes', finalizing_at = now() - interval '5 minutes',
    finalizing_by_worker_id = 'w1'
WHERE id = '00000000-0000-7000-8000-0000000e0302';
SELECT lr_check(
    'stale attempt (BEAT_AGE): an old marker with no beat is stale, and the task fails',
    (SELECT outcome FROM horsies_finish_stale_attempt(
        '00000000-0000-7000-8000-0000000e0302', 'BEAT_AGE', 60000, 120000, NULL, NULL, 'NONE', NULL, NULL,
        '{"err":"crashed"}', 'WORKER_CRASHED', 'r', 'm', 'r')) = 'APPLIED'
);
SELECT lr_check(
    'stale attempt (BEAT_AGE): no beat, last seen at the greatest of the start and the marker',
    lr_phase_values(lr_archived('00000000-0000-7000-8000-0000000e0302', 1)) = jsonb_build_array(
        1, lr_us(now() - interval '1 minute'), lr_us(now() - interval '1 second'), 1,
        lr_us(now() - interval '1 second'), NULL, lr_us(now() - interval '5 minutes'), 'RECOVERY',
        lr_us(now() - interval '5 minutes'))
);

-- OWNER_GONE, a runner beat after the start: last seen at the beat.
SELECT lr_task('00000000-0000-7000-8000-0000000e0303', 'RUNNING', 2, now() - interval '3 seconds');
INSERT INTO horsies_heartbeats (task_id, sender_id, role, sent_at, hostname, pid) VALUES
    ('00000000-0000-7000-8000-0000000e0303', 'w1:7', 'runner', now() - interval '200 milliseconds', 'host-a', 7);
SELECT lr_check(
    'stale attempt (OWNER_GONE): a retry is scheduled',
    (SELECT outcome FROM horsies_finish_stale_attempt(
        '00000000-0000-7000-8000-0000000e0303', 'OWNER_GONE', NULL, NULL, 'w1', now() - interval '1 second', 'RETRY',
        0, 5.0, '{"err":"crashed"}', 'WORKER_CRASHED', 'r', 'm', 'r')) = 'RETRY_SCHEDULED'
);
SELECT lr_check(
    'stale attempt (OWNER_GONE): RECOVERY, last seen at the newest beat, and the retry resets the count',
    lr_attempt_phases('00000000-0000-7000-8000-0000000e0303', 1) = jsonb_build_array(
        1, lr_us(now() - interval '1 minute'), lr_us(now() - interval '1 second'), 2,
        lr_us(now() - interval '3 seconds'), NULL, NULL, 'RECOVERY', lr_us(now() - interval '200 milliseconds'))
    AND lr_claims('00000000-0000-7000-8000-0000000e0303') = '0|null'
);

-- The BEAT_AGE judgement and its evidence are those of migration 1000: a fresh finalizing marker keeps the row, and
-- a beat outside the recency bound shows as no beat.
SELECT lr_task('00000000-0000-7000-8000-0000000e0304', 'RUNNING', 1, now() - interval '1 second');
UPDATE horsies_tasks SET started_at = now() - interval '10 minutes', finalizing_at = now(),
    finalizing_by_worker_id = 'w1'
WHERE id = '00000000-0000-7000-8000-0000000e0304';
INSERT INTO horsies_heartbeats (task_id, sender_id, role, sent_at, hostname, pid) VALUES
    ('00000000-0000-7000-8000-0000000e0304', 'w1:7', 'runner', now() - interval '9 minutes', 'host-a', 7);
SELECT lr_check(
    'stale attempt (BEAT_AGE): a fresh marker is not stale, and the evidence has no beat outside the bound',
    (SELECT concat_ws('|', outcome, guard_kind, observed_guard -> 'last_heartbeat_at' = 'null'::jsonb)
     FROM horsies_finish_stale_attempt(
        '00000000-0000-7000-8000-0000000e0304', 'BEAT_AGE', 60000, 120000, NULL, NULL, 'NONE', NULL, NULL,
        '{"err":"crashed"}', 'WORKER_CRASHED', 'r', 'm', 'r')) = 'SOURCE_STATE_CONFLICT|STALENESS|t'
);
SELECT lr_check(
    'stale attempt (BEAT_AGE): a refusal writes no attempt row',
    NOT EXISTS (SELECT 1 FROM horsies_task_attempts WHERE task_id = '00000000-0000-7000-8000-0000000e0304')
    AND (SELECT status FROM horsies_tasks WHERE id = '00000000-0000-7000-8000-0000000e0304') = 'RUNNING'
);

------------------------------------------------------------------------------------------------------------------------
-- The attempt archive, version 2
------------------------------------------------------------------------------------------------------------------------

-- The values of core/src/core/history/archive/attempts.rs, test version_two_encodes_the_pinned_bytes: attempt 1 of
-- the worker (two claims, placed, marked), attempt 2 of recovery (not counted), attempt 3 with no phase record, and
-- attempt 4 open (retry_count 3) with one claim at 3000 us.
SELECT lr_task('00000000-0000-7000-8000-0000000e0401', 'PENDING', 1, lr_at(3000));
UPDATE horsies_tasks SET retry_count = 3 WHERE id = '00000000-0000-7000-8000-0000000e0401';
SELECT lr_attempt('00000000-0000-7000-8000-0000000e0401', 1, 1, lr_at(100), lr_at(300), 2, lr_at(200), lr_at(350),
                  lr_at(1500), 'WORKER', NULL);
SELECT lr_attempt('00000000-0000-7000-8000-0000000e0401', 2, 1, lr_at(2100), lr_at(2200), NULL, NULL, NULL, NULL,
                  'RECOVERY', lr_at(2400));
SELECT lr_attempt('00000000-0000-7000-8000-0000000e0401', 3, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
SELECT lr_check(
    'archive: the SQL encoder writes the bytes of the Rust codec',
    (SELECT convert_from(horsies_encode_attempt_archive(t), 'UTF8')
     FROM horsies_tasks AS t WHERE t.id = '00000000-0000-7000-8000-0000000e0401')
    = '[[[1,"FAILED",true,1000,2000,"E",null,null,"w1","host",7,"proc",1,100,300,2,200,350,1500,"WORKER",null],'
      || '[2,"FAILED",true,1000,2000,"E",null,null,"w1","host",7,"proc",1,2100,2200,null,null,null,null,"RECOVERY",'
      || '2400],'
      || '[3,"FAILED",true,1000,2000,"E",null,null,"w1","host",7,"proc",null,null,null,null,null,null,null,null,'
      || 'null]],[1,3000,null]]'
);

-- The cancel of a RUNNING task writes the attempt row before the move (migration 1003): the archive holds the
-- CANCELLED attempt with the phase record of the cancel, and no open attempt.
SELECT lr_task('00000000-0000-7000-8000-0000000e0411', 'RUNNING', 2, now() - interval '3 seconds');
UPDATE horsies_tasks SET finalizing_at = now() - interval '100 milliseconds', finalizing_by_worker_id = 'w1'
WHERE id = '00000000-0000-7000-8000-0000000e0411';
SELECT lr_check(
    'move by cancel: applied',
    (SELECT outcome FROM horsies_cancel_task('00000000-0000-7000-8000-0000000e0411', TRUE)) = 'APPLIED'
);
SELECT lr_check(
    'move by cancel: the cancelled attempt with its claims and its finalizing marker, recorded by the cancel',
    (SELECT h.attempt_archive_version = 2
                AND convert_from(h.attempt_snapshot, 'UTF8') = '[[[1,"CANCELLED",false,'
                    || lr_us(now() - interval '500 milliseconds') || ',' || lr_us(now())
                    || ',"TASK_CANCELLED","Cancelled via monitoring API",null,"w1","host-a",7,"worker-7",1,'
                    || lr_us(now() - interval '1 minute') || ',' || lr_us(now() - interval '1 second') || ',2,'
                    || lr_us(now() - interval '3 seconds') || ',null,'
                    || lr_us(now() - interval '100 milliseconds') || ',"CANCEL",null]],null]'
         FROM horsies_task_history AS h WHERE h.task_id = '00000000-0000-7000-8000-0000000e0411')
);

-- Moves that end an attempt with no row: the open record holds the claim facts of the live row.
SELECT lr_task('00000000-0000-7000-8000-0000000e0412', 'PENDING', 0, NULL);
UPDATE horsies_tasks SET good_until = now() - interval '1 second' WHERE id = '00000000-0000-7000-8000-0000000e0412';
SELECT lr_check(
    'move by pending expiry (batch move): applied',
    (SELECT count(*) = 1 FROM horsies_expire_pending_tasks(10, '{"e":"expired"}', 'TASK_EXPIRED'))
);
SELECT lr_check(
    'move by pending expiry (batch move): the open attempt with no claim',
    (SELECT h.attempt_archive_version = 2 AND convert_from(h.attempt_snapshot, 'UTF8') = '[[],[0,null,null]]'
         FROM horsies_task_history AS h WHERE h.task_id = '00000000-0000-7000-8000-0000000e0412')
);
SELECT lr_task('00000000-0000-7000-8000-0000000e0413', 'CLAIMED', 1, now() - interval '1 second');
UPDATE horsies_tasks SET good_until = now() - interval '1 second' WHERE id = '00000000-0000-7000-8000-0000000e0413';
SELECT lr_check(
    'move by claimed expiry: applied',
    (SELECT outcome FROM horsies_expire_owned_claim(
        '00000000-0000-7000-8000-0000000e0413', 'w1', '{"e":"expired"}', 'TASK_EXPIRED')) = 'APPLIED'
);
SELECT lr_check(
    'move by claimed expiry: the open attempt with its one claim',
    (SELECT h.attempt_archive_version = 2
                AND convert_from(h.attempt_snapshot, 'UTF8') = '[[],[1,' || lr_us(now() - interval '1 second')
                    || ',null]]'
         FROM horsies_task_history AS h WHERE h.task_id = '00000000-0000-7000-8000-0000000e0413')
);
SELECT lr_workflow('00000000-0000-7000-8000-0000000e0e02', 'PAUSED');
SELECT lr_task('00000000-0000-7000-8000-0000000e0414', 'CLAIMED', 1, now() - interval '1 second', 'q_lr', TRUE);
SELECT lr_node('00000000-0000-7000-8000-0000000e0f14', '00000000-0000-7000-8000-0000000e0e02', 0, 'ENQUEUED',
               '00000000-0000-7000-8000-0000000e0414');
SELECT lr_check(
    'move by pause abandon: applied',
    (SELECT outcome FROM horsies_abandon_owned_node(
        '00000000-0000-7000-8000-0000000e0414', 'w1', now() - interval '1 second')) = 'APPLIED'
);
SELECT lr_check(
    'move by pause abandon: the open attempt with its one claim',
    (SELECT h.attempt_archive_version = 2
                AND convert_from(h.attempt_snapshot, 'UTF8') = '[[],[1,' || lr_us(now() - interval '1 second')
                    || ',null]]'
         FROM horsies_task_history AS h WHERE h.task_id = '00000000-0000-7000-8000-0000000e0414')
);

------------------------------------------------------------------------------------------------------------------------
-- The monitoring detail
------------------------------------------------------------------------------------------------------------------------

-- A live task with attempt 1 recorded and attempt 2 open, claimed three times.
SELECT lr_task('00000000-0000-7000-8000-0000000e0501', 'CLAIMED', 3, now() - interval '3 seconds');
UPDATE horsies_tasks SET retry_count = 1 WHERE id = '00000000-0000-7000-8000-0000000e0501';
SELECT lr_attempt('00000000-0000-7000-8000-0000000e0501', 1, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
SELECT lr_check(
    'detail: a live task reads version 2 with its open attempt and no digest',
    d.location = 'LIVE' AND d.attempt_archive_version = 2 AND d.attempt_snapshot_digest IS NULL
    AND d.attempt_snapshot = (
        SELECT horsies_encode_attempt_archive(t) FROM horsies_tasks AS t
        WHERE t.id = '00000000-0000-7000-8000-0000000e0501')
    AND jsonb_array_length(convert_from(d.attempt_snapshot, 'UTF8')::jsonb -> 0) = 1
    AND (convert_from(d.attempt_snapshot, 'UTF8')::jsonb -> 1)
        = jsonb_build_array(3, lr_us(now() - interval '3 seconds'), NULL)
)
FROM horsies_monitoring_task_detail('00000000-0000-7000-8000-0000000e0501', FALSE, TRUE) AS d;
SELECT lr_check(
    'detail: a task in history reads the stored archive and its digest',
    d.location = 'HISTORY' AND d.attempt_archive_version = 2
    AND d.attempt_snapshot = h.attempt_snapshot AND d.attempt_snapshot_digest = h.attempt_snapshot_digest
    AND d.attempt_snapshot_digest = sha256(d.attempt_snapshot)
)
FROM horsies_monitoring_task_detail('00000000-0000-7000-8000-0000000e0201', FALSE, TRUE) AS d
CROSS JOIN horsies_task_history AS h
WHERE h.task_id = '00000000-0000-7000-8000-0000000e0201';
