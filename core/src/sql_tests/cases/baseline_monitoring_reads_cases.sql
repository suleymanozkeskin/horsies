-- Cases for the monitoring read functions of 0007_scheduler_monitoring_functions.sql: the status counts, the
-- breakdown, the workflow names, runs, run and node, and the schedule list.
--
-- Preconditions, set up by the runner (core/src/sql_tests/baseline_scheduler_monitoring_sql.rs):
-- - a fresh database that holds the baseline migration;
-- - the published history readers rendered for the forever leaf (horsies_task_detail_staged and
--   horsies_task_provenance_staged);
-- - the dblink extension, and the setting horsies_test.peer_connstr: a libpq connection string to this database,
--   which the two-session cases use for their second session.
--
-- Each case calls test_check or test_raises. A failed case raises 'CASE FAILED <name>: ...' and stops the script.
-- Each passed case takes one value of the sequence test_passed_cases (a sequence survives a rollback, so the cases
-- that run inside a rolled-back transaction count too). The script is plain SQL, so that psql and a driver's simple
-- query protocol run it the same way. Every BEGIN, and every hand-off to the second session, follows a COMMIT; psql
-- reports that COMMIT with the warning "there is no transaction in progress".
--
-- The expected answers of the read cases are those of the Python reads that the functions replace
-- (clients/python/horsies/monitoring/queries.py) on the seed below. The seed section is the part between the lines "seed: begin"
-- and "seed: end"; its codes are built-in codes of the given category or codes outside the taxonomy, so the small
-- taxonomy of these cases and the full one of the library classify them the same way.

-- seed: begin

CREATE SEQUENCE test_passed_cases;

CREATE FUNCTION test_check(p_case text, p_holds boolean, p_detail text) RETURNS void
    LANGUAGE plpgsql AS $$
BEGIN
    IF p_holds IS DISTINCT FROM TRUE THEN
        RAISE EXCEPTION 'CASE FAILED %: %', p_case, p_detail;
    END IF;
    PERFORM nextval('test_passed_cases');
    RAISE NOTICE 'ok %', p_case;
END;
$$;

-- Runs p_sql in a subtransaction and expects it to raise p_sqlstate.
CREATE FUNCTION test_raises(p_case text, p_sql text, p_sqlstate text) RETURNS void
    LANGUAGE plpgsql AS $$
BEGIN
    BEGIN
        EXECUTE p_sql;
    EXCEPTION WHEN OTHERS THEN
        IF SQLSTATE = p_sqlstate THEN
            PERFORM test_check(p_case, TRUE, '');
            RETURN;
        END IF;
        RAISE EXCEPTION 'CASE FAILED %: expected SQLSTATE %, got % (%)', p_case, p_sqlstate, SQLSTATE, SQLERRM;
    END;
    RAISE EXCEPTION 'CASE FAILED %: expected SQLSTATE %, the statement succeeded', p_case, p_sqlstate;
END;
$$;

-- The seed time. The history window of every case is [base - 1 day, base + 1 hour).
CREATE TABLE test_clock (base timestamp with time zone NOT NULL);
INSERT INTO test_clock SELECT now();

CREATE FUNCTION test_base() RETURNS timestamptz
    LANGUAGE sql STABLE AS $$ SELECT base FROM test_clock $$;
CREATE FUNCTION test_lower() RETURNS timestamptz
    LANGUAGE sql STABLE AS $$ SELECT base - interval '1 day' FROM test_clock $$;
CREATE FUNCTION test_upper() RETURNS timestamptz
    LANGUAGE sql STABLE AS $$ SELECT base + interval '1 hour' FROM test_clock $$;

-- A part of the built-in taxonomy, with the categories that the library gives these codes.
CREATE FUNCTION test_codes() RETURNS text[]
    LANGUAGE sql IMMUTABLE AS $$ SELECT ARRAY['WORKER_CRASHED', 'TASK_TIMEOUT', 'RESULT_NOT_READY'] $$;
CREATE FUNCTION test_code_categories() RETURNS text[]
    LANGUAGE sql IMMUTABLE AS $$ SELECT ARRAY['OPERATIONAL', 'OUTCOME', 'RETRIEVAL'] $$;

CREATE FUNCTION test_live_task(
    p_id uuid,
    p_status text,
    p_task_name text,
    p_queue text,
    p_worker text,
    p_retry_count integer,
    p_is_workflow_task boolean
) RETURNS void
    LANGUAGE sql AS $$
    INSERT INTO horsies_tasks (
        id, task_name, queue_name, priority, input, task_options, execution, retention_class_key, sent_at,
        created_at, command_fingerprint_version, command_fingerprint, idempotency_key_digest, status, enqueued_at,
        claimed_at, claimed_by_worker_id, claim_expires_at, started_at, finalizing_at, finalizing_by_worker_id,
        worker_pid, worker_hostname, worker_process_name, retry_count, max_retries, next_retry_at, good_until,
        is_workflow_task, updated_at, retain_rerun_input, rerun_of_task_id, rerun_root_task_id
    ) VALUES (
        p_id, p_task_name, p_queue, 50, '{}', NULL, 'SHARED_ASYNC', 'forever', test_base() - interval '2 hours',
        test_base() - interval '2 hours',
        CASE WHEN p_is_workflow_task THEN NULL ELSE 1 END,
        CASE WHEN p_is_workflow_task THEN NULL ELSE sha256('fixture'::bytea) END,
        NULL, p_status, test_base() - interval '2 hours',
        CASE WHEN p_worker IS NULL THEN NULL ELSE test_base() - interval '119 minutes' END,
        p_worker,
        CASE WHEN p_status = 'CLAIMED' THEN test_base() + interval '1 day' END,
        CASE WHEN p_status = 'RUNNING' THEN test_base() - interval '118 minutes' END,
        NULL, NULL,
        CASE WHEN p_worker IS NULL THEN NULL ELSE 4242 END,
        CASE WHEN p_worker IS NULL THEN NULL ELSE 'host-' || p_worker END,
        CASE WHEN p_worker IS NULL THEN NULL ELSE 'proc-live' END,
        p_retry_count, 3, NULL, test_base() + interval '1 day',
        p_is_workflow_task, test_base(), FALSE, NULL, NULL
    )
$$;

-- A history row. p_snapshot is the version-1 attempt snapshot text (an array of positional attempt rows).
CREATE FUNCTION test_history_task(
    p_id uuid,
    p_status text,
    p_task_name text,
    p_queue text,
    p_worker text,
    p_terminal_at timestamptz,
    p_retry_count integer,
    p_error_code text,
    p_workflow_id uuid,
    p_snapshot text
) RETURNS void
    LANGUAGE sql AS $$
    INSERT INTO horsies_task_history (
        task_id, task_name, queue_name, priority, command_fingerprint_version, command_fingerprint,
        retention_class_key, is_workflow_task, workflow_id, rerun_of_task_id, rerun_root_task_id, sent_at,
        created_at, enqueued_at, claimed_at, started_at, good_until, terminal_at, status, terminalization_kind,
        retry_count, max_retries, last_claimed_worker_id, last_worker_hostname, last_worker_pid,
        last_worker_process_name, error_code, final_failed_reason, result_envelope_version, result_payload,
        result_digest, history_schema_version, attempt_archive_version, attempt_snapshot, attempt_snapshot_digest,
        rerun_input_disposition, rerun_input_version, rerun_input_digest, rerun_input_inline
    ) VALUES (
        p_id, p_task_name, p_queue, 50,
        CASE WHEN p_workflow_id IS NULL THEN 1 END,
        CASE WHEN p_workflow_id IS NULL THEN sha256('fixture'::bytea) END,
        'forever', p_workflow_id IS NOT NULL, p_workflow_id, NULL, NULL, p_terminal_at - interval '10 minutes',
        p_terminal_at - interval '10 minutes', p_terminal_at - interval '10 minutes',
        CASE WHEN p_worker IS NULL THEN NULL ELSE p_terminal_at - interval '9 minutes' END,
        CASE WHEN p_worker IS NULL THEN NULL ELSE p_terminal_at - interval '8 minutes' END,
        NULL, p_terminal_at, p_status,
        CASE p_status
            WHEN 'COMPLETED' THEN 'COMPLETE_FUSED'
            WHEN 'FAILED' THEN 'FAIL_RUNNING'
            WHEN 'CANCELLED' THEN 'CANCEL_ADMIN'
            ELSE 'EXPIRE_PENDING'
        END,
        p_retry_count, 3, p_worker,
        CASE WHEN p_worker IS NULL THEN NULL ELSE 'host-' || p_worker END,
        CASE WHEN p_worker IS NULL THEN NULL ELSE 5151 END,
        CASE WHEN p_worker IS NULL THEN NULL ELSE 'proc-history' END,
        p_error_code, CASE WHEN p_status = 'FAILED' THEN 'reason of ' || p_task_name END, 1,
        CASE WHEN p_status = 'COMPLETED' THEN convert_to('{}', 'UTF8') END,
        CASE WHEN p_status = 'COMPLETED' THEN sha256(convert_to('{}', 'UTF8')) END,
        1, 1, convert_to(p_snapshot, 'UTF8'), sha256(convert_to(p_snapshot, 'UTF8')),
        'NEVER_ELIGIBLE', NULL, NULL, NULL
    )
$$;

CREATE FUNCTION test_workflow(
    p_id uuid,
    p_name text,
    p_status text,
    p_created_at timestamptz,
    p_parent_id uuid,
    p_parent_index integer
) RETURNS void
    LANGUAGE sql AS $$
    INSERT INTO horsies_workflows (
        id, name, definition_key, status, on_error, output_task_index, success_policy, result, error,
        parent_workflow_id, parent_task_index, root_workflow_id, depth, sent_at, created_at, started_at,
        completed_at, updated_at
    ) VALUES (
        p_id, p_name, CASE WHEN p_parent_id IS NULL THEN 'defs.' || p_name || '.v1' END, p_status, 'fail', NULL,
        NULL, NULL, NULL, p_parent_id, p_parent_index, p_parent_id,
        CASE WHEN p_parent_id IS NULL THEN 0 ELSE 1 END, p_created_at, p_created_at, p_created_at,
        CASE WHEN p_status IN ('COMPLETED', 'FAILED', 'CANCELLED') THEN p_created_at + interval '5 minutes' END,
        p_created_at
    )
$$;

-- A node. p_kind is 'TASK' (p_ref is the task id, NULL before the task exists) or 'SUB' (p_ref is the child run).
CREATE FUNCTION test_node(
    p_workflow_id uuid,
    p_index integer,
    p_kind text,
    p_ref uuid,
    p_status text,
    p_dependencies integer[],
    p_result text
) RETURNS void
    LANGUAGE sql AS $$
    INSERT INTO horsies_workflow_tasks (
        downstream_subworkflow, sub_workflow_params,
        id, workflow_id, task_index, node_id, task_name, input, queue_name, priority, dependencies, args_from,
        workflow_ctx_from, allow_failed_deps, join_type, min_success, task_options, max_retries, execution,
        good_until, retention_class_key, status, task_id, is_subworkflow, sub_workflow_id, sub_workflow_name,
        sub_definition_key, sub_workflow_summary, result, error, created_at, started_at, completed_at
    ) VALUES (
        'NONE', CASE WHEN p_kind = 'SUB' THEN 'NOT_GIVEN' END,
        gen_random_uuid(), p_workflow_id, p_index, CASE WHEN p_index % 2 = 0 THEN 'node-' || p_index END,
        'step_' || p_index, '{}', 'default', 50, p_dependencies, NULL, NULL, p_index = 1, 'all', NULL, NULL,
        CASE WHEN p_kind = 'TASK' THEN 3 END, CASE WHEN p_kind = 'TASK' THEN 'SHARED_ASYNC' END, NULL,
        CASE WHEN p_kind = 'TASK' THEN 'forever' END, p_status,
        CASE WHEN p_kind = 'TASK' THEN p_ref END, p_kind = 'SUB', CASE WHEN p_kind = 'SUB' THEN p_ref END,
        CASE WHEN p_kind = 'SUB' THEN 'child' END, NULL, NULL, p_result, NULL, test_base() - interval '3 hours',
        CASE WHEN p_status IN ('RUNNING', 'COMPLETED', 'FAILED') THEN test_base() - interval '170 minutes' END,
        CASE WHEN p_status IN ('COMPLETED', 'FAILED') THEN test_base() - interval '160 minutes' END
    )
$$;

CREATE FUNCTION test_schedule(p_name text, p_next_run_at timestamptz, p_run_count integer) RETURNS void
    LANGUAGE sql AS $$
    INSERT INTO horsies_schedule_state (
        schedule_name, next_run_at, last_run_at, last_task_id, run_count, config_hash, updated_at
    ) VALUES (
        p_name, p_next_run_at,
        CASE WHEN p_run_count > 0 THEN test_base() - interval '1 hour' END,
        CASE WHEN p_run_count > 0 THEN md5(p_name)::uuid END,
        p_run_count, sha256(convert_to(p_name, 'UTF8')), test_base()
    )
$$;

-- Tasks. L* live, H* history. Task names Zeta and gamma tie in the breakdown; in byte order Zeta comes first.
SELECT test_live_task('00000000-0000-4000-8000-0000000000a1', 'PENDING', 'alpha', 'default', NULL, 1, TRUE);
SELECT test_live_task('00000000-0000-4000-8000-0000000000a2', 'CLAIMED', 'alpha', 'fast', 'w-1', 1, FALSE);
SELECT test_live_task('00000000-0000-4000-8000-0000000000a3', 'RUNNING', 'beta', 'default', 'w-2', 0, FALSE);
SELECT test_live_task('00000000-0000-4000-8000-0000000000a4', 'RUNNING', 'Zeta', 'fast', 'w-1', 2, FALSE);
INSERT INTO horsies_task_attempts (
    task_id, attempt, outcome, will_retry, started_at, finished_at, error_code, error_message, failed_reason,
    worker_id, worker_hostname, worker_pid, worker_process_name
) VALUES (
    '00000000-0000-4000-8000-0000000000a1', 1, 'FAILED', TRUE, test_base() - interval '100 minutes',
    test_base() - interval '99 minutes', 'BOOM', 'first try failed', NULL, 'w-1', 'host-w-1', 4242, 'proc-live'
);

SELECT test_history_task('00000000-0000-4000-8000-0000000000b1', 'COMPLETED', 'alpha', 'default', 'w-1',
    test_base() - interval '2 hours', 0, NULL, NULL, '[]');
SELECT test_history_task('00000000-0000-4000-8000-0000000000b2', 'FAILED', 'beta', 'fast', 'w-2',
    test_base() - interval '90 minutes', 2, 'BOOM', '00000000-0000-4000-8000-00000000f001',
    '[[1,"FAILED",true,' || ((extract(epoch FROM test_base()) * 1000000)::bigint - 600000000) || ','
    || ((extract(epoch FROM test_base()) * 1000000)::bigint - 590000000)
    || ',"BOOM","try 1",null,"w-2","host-w-2",5151,"proc-history"],[2,"FAILED",false,'
    || ((extract(epoch FROM test_base()) * 1000000)::bigint - 580000000) || ','
    || ((extract(epoch FROM test_base()) * 1000000)::bigint - 570000000)
    || ',"BOOM","try 2","gave up","w-2","host-w-2",5151,"proc-history"]]');
SELECT test_history_task('00000000-0000-4000-8000-0000000000b3', 'FAILED', 'alpha', 'default', NULL,
    test_base() - interval '3 hours', 0, 'WORKER_CRASHED', NULL, '[]');
SELECT test_history_task('00000000-0000-4000-8000-0000000000b4', 'CANCELLED', 'beta', 'default', 'w-1',
    test_base() - interval '4 hours', 0, NULL, NULL, '[]');
SELECT test_history_task('00000000-0000-4000-8000-0000000000b5', 'EXPIRED', 'gamma', 'fast', NULL,
    test_base() - interval '5 hours', 0, '', NULL, '[]');
SELECT test_history_task('00000000-0000-4000-8000-0000000000b6', 'COMPLETED', 'Zeta', 'Slow', 'w-2',
    test_base() - interval '6 hours', 1, 'RESULT_NOT_READY', NULL, '[]');
SELECT test_history_task('00000000-0000-4000-8000-0000000000b7', 'FAILED', 'beta', 'fast', 'W-3',
    test_base() - interval '7 hours', 0, 'my_err', NULL, '[]');
-- Before the window, at its upper bound (outside: the bound is exclusive) and at its lower bound (inside).
SELECT test_history_task('00000000-0000-4000-8000-0000000000b8', 'COMPLETED', 'alpha', 'default', 'w-1',
    test_base() - interval '3 days', 0, NULL, NULL, '[]');
SELECT test_history_task('00000000-0000-4000-8000-0000000000b9', 'FAILED', 'alpha', 'default', 'w-1',
    test_base() + interval '1 hour', 0, 'BOOM', NULL, '[]');
SELECT test_history_task('00000000-0000-4000-8000-0000000000ba', 'FAILED', 'gamma', 'default', 'w-2',
    test_base() - interval '1 day', 0, 'TASK_TIMEOUT', NULL, '[]');

-- Workflows. f001 etl (root) holds every node kind; f004 and f005 are its child runs; f005 has no node.
SELECT test_workflow('00000000-0000-4000-8000-00000000f001', 'etl', 'RUNNING', test_base() - interval '3 hours',
    NULL, NULL);
SELECT test_workflow('00000000-0000-4000-8000-00000000f002', 'billing', 'COMPLETED',
    test_base() - interval '2 hours', NULL, NULL);
SELECT test_workflow('00000000-0000-4000-8000-00000000f003', 'etl', 'FAILED', test_base() - interval '1 hour', NULL,
    NULL);
SELECT test_workflow('00000000-0000-4000-8000-00000000f004', 'etl.child', 'RUNNING',
    test_base() - interval '170 minutes', '00000000-0000-4000-8000-00000000f001', 2);
SELECT test_workflow('00000000-0000-4000-8000-00000000f005', 'etl.child', 'PENDING',
    test_base() - interval '169 minutes', '00000000-0000-4000-8000-00000000f001', 3);
SELECT test_workflow('00000000-0000-4000-8000-00000000f006', 'Report', 'PAUSED',
    test_base() - interval '30 minutes', NULL, NULL);

SELECT test_node('00000000-0000-4000-8000-00000000f001', 0, 'TASK', '00000000-0000-4000-8000-0000000000a1',
    'ENQUEUED', '{}', NULL);
SELECT test_node('00000000-0000-4000-8000-00000000f001', 1, 'TASK', '00000000-0000-4000-8000-0000000000b2',
    'FAILED', '{0}', '{"err":{"error_code":"BOOM","message":"try 2"}}');
SELECT test_node('00000000-0000-4000-8000-00000000f001', 2, 'SUB', '00000000-0000-4000-8000-00000000f004',
    'RUNNING', '{0,7}', NULL);
SELECT test_node('00000000-0000-4000-8000-00000000f001', 3, 'SUB', '00000000-0000-4000-8000-00000000f005',
    'PENDING', '{2}', NULL);
-- A task in neither place (a UUIDv4 id: its absence is NOT_CLASSIFIED).
SELECT test_node('00000000-0000-4000-8000-00000000f001', 4, 'TASK', '00000000-0000-4000-8000-0000000000c4',
    'COMPLETED', '{}', NULL);
SELECT test_node('00000000-0000-4000-8000-00000000f001', 5, 'TASK', NULL, 'PENDING', '{4}', NULL);
SELECT test_node('00000000-0000-4000-8000-00000000f001', 6, 'SUB', NULL, 'PENDING', '{}', NULL);
SELECT test_node('00000000-0000-4000-8000-00000000f004', 0, 'TASK', NULL, 'FAILED', '{}', NULL);
SELECT test_node('00000000-0000-4000-8000-00000000f004', 1, 'TASK', NULL, 'COMPLETED', '{}', NULL);
SELECT test_node('00000000-0000-4000-8000-00000000f004', 2, 'TASK', NULL, 'FAILED', '{1}', NULL);
SELECT test_node('00000000-0000-4000-8000-00000000f003', 0, 'TASK', NULL, 'FAILED', '{}', NULL);

SELECT test_schedule('nightly', test_base() + interval '2 hours', 0);
SELECT test_schedule('hourly', test_base() + interval '10 minutes', 3);
SELECT test_schedule('zz-first', test_base() + interval '1 minute', 1);
SELECT test_schedule('Alpha', test_base() + interval '20 minutes', 0);

-- seed: end

------------------------------------------------------------------------------------------------------------------------
-- horsies_monitoring_task_stats
------------------------------------------------------------------------------------------------------------------------

-- The counts in row order. STABLE, so a call reads the snapshot of the calling statement.
CREATE FUNCTION test_stats(
    p_task_names text[],
    p_queues text[],
    p_workers text[],
    p_codes text[],
    p_categories text[],
    p_retried_only boolean
) RETURNS bigint[]
    LANGUAGE sql STABLE AS $$
    SELECT array_agg(s.task_count ORDER BY s.row_position)
    FROM horsies_monitoring_task_stats(
        test_lower(), test_upper(), p_task_names, p_queues, p_workers, p_codes, p_categories, p_retried_only,
        test_codes(), test_code_categories()
    ) AS s
$$;

SELECT test_check(
    'stats: seven statuses in the fixed order at positions 1..7',
    array_agg(s.status || '@' || s.row_position ORDER BY s.row_position)
        = ARRAY['PENDING@1', 'CLAIMED@2', 'RUNNING@3', 'COMPLETED@4', 'FAILED@5', 'CANCELLED@6', 'EXPIRED@7'],
    array_agg(s.status || '@' || s.row_position ORDER BY s.row_position)::text
)
FROM horsies_monitoring_task_stats(test_lower(), test_upper(), '{}', '{}', '{}', '{}', '{}', FALSE, test_codes(),
    test_code_categories()) AS s;

SELECT test_check(
    'stats: live statuses from the live table, terminal statuses from the window (lower bound in, upper bound out)',
    test_stats('{}', '{}', '{}', '{}', '{}', FALSE) = ARRAY[1, 1, 2, 2, 4, 1, 1]::bigint[],
    test_stats('{}', '{}', '{}', '{}', '{}', FALSE)::text
);
SELECT test_check(
    'stats: a task-name filter',
    test_stats('{alpha}', '{}', '{}', '{}', '{}', FALSE) = ARRAY[1, 1, 0, 1, 1, 0, 0]::bigint[],
    test_stats('{alpha}', '{}', '{}', '{}', '{}', FALSE)::text
);
SELECT test_check(
    'stats: a queue filter',
    test_stats('{}', '{fast}', '{}', '{}', '{}', FALSE) = ARRAY[0, 1, 1, 0, 2, 0, 1]::bigint[],
    test_stats('{}', '{fast}', '{}', '{}', '{}', FALSE)::text
);
SELECT test_check(
    'stats: a worker filter reads the current claim live and the last claim in history',
    test_stats('{}', '{}', '{w-1}', '{}', '{}', FALSE) = ARRAY[0, 1, 1, 1, 0, 1, 0]::bigint[],
    test_stats('{}', '{}', '{w-1}', '{}', '{}', FALSE)::text
);
SELECT test_check(
    'stats: an error-code filter drops the live side',
    test_stats('{}', '{}', '{}', '{BOOM}', '{}', FALSE) = ARRAY[0, 0, 0, 0, 1, 0, 0]::bigint[],
    test_stats('{}', '{}', '{}', '{BOOM}', '{}', FALSE)::text
);
SELECT test_check(
    'stats: a built-in category selects the codes of its family',
    test_stats('{}', '{}', '{}', '{}', '{OPERATIONAL}', FALSE) = ARRAY[0, 0, 0, 0, 1, 0, 0]::bigint[]
    AND test_stats('{}', '{}', '{}', '{}', '{OUTCOME}', FALSE) = ARRAY[0, 0, 0, 0, 1, 0, 0]::bigint[],
    test_stats('{}', '{}', '{}', '{}', '{OPERATIONAL}', FALSE)::text
);
SELECT test_check(
    'stats: DOMAIN selects the non-empty codes outside the taxonomy, and categories OR together',
    test_stats('{}', '{}', '{}', '{}', '{DOMAIN}', FALSE) = ARRAY[0, 0, 0, 0, 2, 0, 0]::bigint[]
    AND test_stats('{}', '{}', '{}', '{}', '{RETRIEVAL,DOMAIN}', FALSE) = ARRAY[0, 0, 0, 1, 2, 0, 0]::bigint[],
    test_stats('{}', '{}', '{}', '{}', '{RETRIEVAL,DOMAIN}', FALSE)::text
);
SELECT test_check(
    'stats: retried_only keeps rows with retry_count > 0',
    test_stats('{}', '{}', '{}', '{}', '{}', TRUE) = ARRAY[1, 1, 1, 1, 1, 0, 0]::bigint[],
    test_stats('{}', '{}', '{}', '{}', '{}', TRUE)::text
);
SELECT test_check(
    'stats: values OR in one dimension, dimensions AND together',
    test_stats('{alpha,beta}', '{fast,default}', '{}', '{}', '{}', TRUE) = ARRAY[1, 1, 0, 0, 1, 0, 0]::bigint[],
    test_stats('{alpha,beta}', '{fast,default}', '{}', '{}', '{}', TRUE)::text
);
SELECT test_check(
    'stats: a scope with no match is seven zeros',
    test_stats('{nope}', '{}', '{}', '{}', '{}', FALSE) = ARRAY[0, 0, 0, 0, 0, 0, 0]::bigint[],
    test_stats('{nope}', '{}', '{}', '{}', '{}', FALSE)::text
);

SELECT test_raises('stats refuse a NULL filter array',
    $q$SELECT * FROM horsies_monitoring_task_stats(test_lower(), test_upper(), NULL, '{}', '{}', '{}', '{}', FALSE,
        test_codes(), test_code_categories())$q$, '22004');
SELECT test_raises('stats refuse a NULL retried_only',
    $q$SELECT * FROM horsies_monitoring_task_stats(test_lower(), test_upper(), '{}', '{}', '{}', '{}', '{}', NULL,
        test_codes(), test_code_categories())$q$, '22004');
SELECT test_raises('stats refuse a window that does not increase',
    $q$SELECT * FROM horsies_monitoring_task_stats(test_upper(), test_lower(), '{}', '{}', '{}', '{}', '{}', FALSE,
        test_codes(), test_code_categories())$q$, '22023');
SELECT test_raises('stats refuse an unknown error category',
    $q$SELECT * FROM horsies_monitoring_task_stats(test_lower(), test_upper(), '{}', '{}', '{}', '{}', '{NOPE}',
        FALSE, test_codes(), test_code_categories())$q$, '22023');
SELECT test_raises('stats refuse a taxonomy whose arrays differ in length',
    $q$SELECT * FROM horsies_monitoring_task_stats(test_lower(), test_upper(), '{}', '{}', '{}', '{}', '{}', FALSE,
        test_codes(), ARRAY['OPERATIONAL'])$q$, '22023');

------------------------------------------------------------------------------------------------------------------------
-- horsies_monitoring_task_breakdown
------------------------------------------------------------------------------------------------------------------------

-- The rows as 'group=total/pending/claimed/running/completed/failed/cancelled/expired/retried', the TOTAL row first
-- as 'TOTAL:<group_count>=...', the UNCLAIMED group as '(unclaimed)=...'.
CREATE FUNCTION test_breakdown(
    p_group_by text,
    p_statuses text[],
    p_task_names text[],
    p_queues text[],
    p_workers text[],
    p_codes text[],
    p_categories text[],
    p_retried_only boolean,
    p_limit integer
) RETURNS text[]
    LANGUAGE sql STABLE AS $$
    SELECT array_agg(
        CASE
            WHEN b.row_kind = 'TOTAL' THEN 'TOTAL:' || b.group_count
            WHEN b.group_kind = 'UNCLAIMED' THEN '(unclaimed)'
            ELSE b.group_value
        END || '='
        || concat_ws('/', b.total, b.pending, b.claimed, b.running, b.completed, b.failed, b.cancelled, b.expired,
                     b.retried)
        ORDER BY b.row_position
    )
    FROM horsies_monitoring_task_breakdown(
        test_lower(), test_upper(), p_group_by, p_statuses, p_task_names, p_queues, p_workers, p_codes, p_categories,
        p_retried_only, test_codes(), test_code_categories(), p_limit
    ) AS b
$$;

SELECT test_check(
    'breakdown by worker: rows with no claim are the UNCLAIMED group; ties by total break in byte order',
    test_breakdown('worker', '{}', '{}', '{}', '{}', '{}', '{}', FALSE, 500) = ARRAY[
        'TOTAL:4=12/1/1/2/2/4/1/1/5', 'w-1=4/0/1/1/1/0/1/0/2', 'w-2=4/0/0/1/1/2/0/0/2',
        '(unclaimed)=3/1/0/0/0/1/0/1/1', 'W-3=1/0/0/0/0/1/0/0/0'],
    test_breakdown('worker', '{}', '{}', '{}', '{}', '{}', '{}', FALSE, 500)::text
);
SELECT test_check(
    'breakdown by task name: Zeta before gamma (byte order, not the database collation)',
    test_breakdown('task_name', '{}', '{}', '{}', '{}', '{}', '{}', FALSE, 500) = ARRAY[
        'TOTAL:4=12/1/1/2/2/4/1/1/5', 'alpha=4/1/1/0/1/1/0/0/2', 'beta=4/0/0/1/0/2/1/0/1', 'Zeta=2/0/0/1/1/0/0/0/2',
        'gamma=2/0/0/0/0/1/0/1/0'],
    test_breakdown('task_name', '{}', '{}', '{}', '{}', '{}', '{}', FALSE, 500)::text
);
SELECT test_check(
    'breakdown by queue',
    test_breakdown('queue', '{}', '{}', '{}', '{}', '{}', '{}', FALSE, 500) = ARRAY[
        'TOTAL:3=12/1/1/2/2/4/1/1/5', 'default=6/1/0/1/1/2/1/0/1', 'fast=5/0/1/1/0/2/0/1/3',
        'Slow=1/0/0/0/1/0/0/0/1'],
    test_breakdown('queue', '{}', '{}', '{}', '{}', '{}', '{}', FALSE, 500)::text
);
SELECT test_check(
    'breakdown: the limit caps the groups, not the TOTAL row or group_count',
    test_breakdown('worker', '{}', '{}', '{}', '{}', '{}', '{}', FALSE, 1) = ARRAY[
        'TOTAL:4=12/1/1/2/2/4/1/1/5', 'w-1=4/0/1/1/1/0/1/0/2'],
    test_breakdown('worker', '{}', '{}', '{}', '{}', '{}', '{}', FALSE, 1)::text
);
SELECT test_check(
    'breakdown: a status filter scopes the groups and the total',
    test_breakdown('worker', '{RUNNING,FAILED}', '{}', '{}', '{}', '{}', '{}', FALSE, 500) = ARRAY[
        'TOTAL:4=6/0/0/2/0/4/0/0/2', 'w-2=3/0/0/1/0/2/0/0/1', 'W-3=1/0/0/0/0/1/0/0/0',
        '(unclaimed)=1/0/0/0/0/1/0/0/0',
        'w-1=1/0/0/1/0/0/0/0/1'],
    test_breakdown('worker', '{RUNNING,FAILED}', '{}', '{}', '{}', '{}', '{}', FALSE, 500)::text
);
SELECT test_check(
    'breakdown: an error-code filter drops the live side',
    test_breakdown('queue', '{}', '{}', '{}', '{}', '{BOOM}', '{}', FALSE, 500) = ARRAY[
        'TOTAL:1=1/0/0/0/0/1/0/0/1', 'fast=1/0/0/0/0/1/0/0/1'],
    test_breakdown('queue', '{}', '{}', '{}', '{}', '{BOOM}', '{}', FALSE, 500)::text
);
SELECT test_check(
    'breakdown: a DOMAIN category filter',
    test_breakdown('task_name', '{}', '{}', '{}', '{}', '{}', '{DOMAIN}', FALSE, 500) = ARRAY[
        'TOTAL:1=2/0/0/0/0/2/0/0/1', 'beta=2/0/0/0/0/2/0/0/1'],
    test_breakdown('task_name', '{}', '{}', '{}', '{}', '{}', '{DOMAIN}', FALSE, 500)::text
);
SELECT test_check(
    'breakdown: filters combine across dimensions',
    test_breakdown('worker', '{}', '{alpha,beta}', '{default}', '{}', '{}', '{}', FALSE, 500) = ARRAY[
        'TOTAL:3=5/1/0/1/1/1/1/0/1', '(unclaimed)=2/1/0/0/0/1/0/0/1', 'w-1=2/0/0/0/1/0/1/0/0',
        'w-2=1/0/0/1/0/0/0/0/0'],
    test_breakdown('worker', '{}', '{alpha,beta}', '{default}', '{}', '{}', '{}', FALSE, 500)::text
);
SELECT test_check(
    'breakdown: an empty scope gives no group and a TOTAL row of zeros',
    test_breakdown('worker', '{}', '{nope}', '{}', '{}', '{}', '{}', FALSE, 500) = ARRAY['TOTAL:0=0/0/0/0/0/0/0/0/0'],
    test_breakdown('worker', '{}', '{nope}', '{}', '{}', '{}', '{}', FALSE, 500)::text
);
SELECT test_check(
    'breakdown: the TOTAL row comes first at position 0 and the groups at 1..n',
    array_agg(b.row_kind || '@' || b.row_position ORDER BY b.row_position)
        = ARRAY['TOTAL@0', 'GROUP@1', 'GROUP@2', 'GROUP@3'],
    array_agg(b.row_kind || '@' || b.row_position ORDER BY b.row_position)::text
)
FROM horsies_monitoring_task_breakdown(test_lower(), test_upper(), 'queue', '{}', '{}', '{}', '{}', '{}', '{}', FALSE,
    test_codes(), test_code_categories(), 500) AS b;

SELECT test_check(
    'breakdown: group_kind VALUE carries the value, UNCLAIMED has no value, TOTAL has neither',
    array_agg(b.row_kind || ':' || COALESCE(b.group_kind, '-') || ':' || COALESCE(b.group_value, '-')
        ORDER BY b.row_position)
        = ARRAY['TOTAL:-:-', 'GROUP:VALUE:w-1', 'GROUP:VALUE:w-2', 'GROUP:UNCLAIMED:-', 'GROUP:VALUE:W-3'],
    array_agg(b.row_kind || ':' || COALESCE(b.group_kind, '-') || ':' || COALESCE(b.group_value, '-')
        ORDER BY b.row_position)::text
)
FROM horsies_monitoring_task_breakdown(test_lower(), test_upper(), 'worker', '{}', '{}', '{}', '{}', '{}', '{}',
    FALSE, test_codes(), test_code_categories(), 500) AS b;
SELECT test_check(
    'breakdown: task name and queue groups are all VALUE groups',
    bool_and(b.group_kind = 'VALUE' AND b.group_value IS NOT NULL) FILTER (WHERE b.row_kind = 'GROUP'),
    array_agg(b.group_kind)::text
)
FROM (
    SELECT * FROM horsies_monitoring_task_breakdown(test_lower(), test_upper(), 'task_name', '{}', '{}', '{}', '{}',
        '{}', '{}', FALSE, test_codes(), test_code_categories(), 500)
    UNION ALL
    SELECT * FROM horsies_monitoring_task_breakdown(test_lower(), test_upper(), 'queue', '{}', '{}', '{}', '{}', '{}',
        '{}', FALSE, test_codes(), test_code_categories(), 500)
) AS b;

SELECT test_raises('breakdown refuses an unknown group column',
    $q$SELECT * FROM test_breakdown('host', '{}', '{}', '{}', '{}', '{}', '{}', FALSE, 10)$q$, '22023');
SELECT test_raises('breakdown refuses a limit of 0',
    $q$SELECT * FROM test_breakdown('worker', '{}', '{}', '{}', '{}', '{}', '{}', FALSE, 0)$q$, '22023');
SELECT test_raises('breakdown refuses a limit over 500',
    $q$SELECT * FROM test_breakdown('worker', '{}', '{}', '{}', '{}', '{}', '{}', FALSE, 501)$q$, '22023');
SELECT test_raises('breakdown refuses an unknown status',
    $q$SELECT * FROM test_breakdown('worker', '{DONE}', '{}', '{}', '{}', '{}', '{}', FALSE, 10)$q$, '22023');
SELECT test_raises('breakdown refuses a NULL limit',
    $q$SELECT * FROM test_breakdown('worker', '{}', '{}', '{}', '{}', '{}', '{}', FALSE, NULL)$q$, '22004');
SELECT test_raises('breakdown refuses a NULL group column',
    $q$SELECT * FROM test_breakdown(NULL, '{}', '{}', '{}', '{}', '{}', '{}', FALSE, 10)$q$, '22004');

------------------------------------------------------------------------------------------------------------------------
-- horsies_monitoring_workflow_names
------------------------------------------------------------------------------------------------------------------------

SELECT test_check(
    'workflow names: distinct root names in the database collation, child runs left out, then LIST_END',
    array_agg(n.row_kind || ':' || COALESCE(n.workflow_name, n.list_state) ORDER BY n.row_position NULLS LAST)
        = ARRAY(SELECT 'NAME:' || x FROM unnest(ARRAY['etl', 'billing', 'Report']) AS x ORDER BY x)
          || ARRAY['LIST_END:COMPLETE'],
    array_agg(n.row_kind || ':' || COALESCE(n.workflow_name, n.list_state) ORDER BY n.row_position NULLS LAST)::text
)
FROM horsies_monitoring_workflow_names() AS n;
SELECT test_check(
    'workflow names: positions 1..n and the bound on the LIST_END row',
    array_agg(n.row_position ORDER BY n.row_position NULLS LAST)::text = '{1,2,3,NULL}'
    AND max(n.row_bound) = 10000 AND count(n.row_bound) = 1,
    array_agg(n.row_position)::text
)
FROM horsies_monitoring_workflow_names() AS n;

------------------------------------------------------------------------------------------------------------------------
-- horsies_monitoring_workflow_runs
------------------------------------------------------------------------------------------------------------------------

CREATE FUNCTION test_runs(p_name text, p_status text, p_limit integer) RETURNS text[]
    LANGUAGE sql STABLE AS $$
    SELECT COALESCE(array_agg(right(r.workflow_id::text, 4) ORDER BY r.row_position), '{}')
    FROM horsies_monitoring_workflow_runs(p_name, p_status, p_limit) AS r
$$;

SELECT test_check(
    'workflow runs: root runs newest first, child runs left out',
    test_runs(NULL, NULL, 200) = ARRAY['f006', 'f003', 'f002', 'f001'],
    test_runs(NULL, NULL, 200)::text
);
SELECT test_check(
    'workflow runs: the name filter',
    test_runs('etl', NULL, 200) = ARRAY['f003', 'f001'],
    test_runs('etl', NULL, 200)::text
);
SELECT test_check(
    'workflow runs: the status filter (the RUNNING child run is left out)',
    test_runs(NULL, 'RUNNING', 200) = ARRAY['f001'],
    test_runs(NULL, 'RUNNING', 200)::text
);
SELECT test_check(
    'workflow runs: name and status AND together',
    test_runs('etl', 'FAILED', 200) = ARRAY['f003'] AND test_runs('billing', 'FAILED', 200) = '{}',
    test_runs('etl', 'FAILED', 200)::text
);
SELECT test_check(
    'workflow runs: a status that no run has gives no row',
    test_runs(NULL, 'NOPE', 200) = '{}',
    test_runs(NULL, 'NOPE', 200)::text
);
SELECT test_check(
    'workflow runs: the limit',
    test_runs(NULL, NULL, 2) = ARRAY['f006', 'f003'],
    test_runs(NULL, NULL, 2)::text
);
SELECT test_check(
    'workflow runs: the columns of one run',
    r.workflow_name = 'billing' AND r.definition_key = 'defs.billing.v1' AND r.status = 'COMPLETED'
    AND r.created_at = test_base() - interval '2 hours'
    AND r.completed_at = test_base() - interval '2 hours' + interval '5 minutes' AND r.row_position = 1,
    row_to_json(r)::text
)
FROM horsies_monitoring_workflow_runs('billing', NULL, 10) AS r;

SELECT test_raises('workflow runs refuse a limit of 0',
    $q$SELECT * FROM horsies_monitoring_workflow_runs(NULL, NULL, 0)$q$, '22023');
SELECT test_raises('workflow runs refuse a limit over 200',
    $q$SELECT * FROM horsies_monitoring_workflow_runs(NULL, NULL, 201)$q$, '22023');
SELECT test_raises('workflow runs refuse a NULL limit',
    $q$SELECT * FROM horsies_monitoring_workflow_runs(NULL, NULL, NULL)$q$, '22004');

------------------------------------------------------------------------------------------------------------------------
-- horsies_monitoring_workflow_run
------------------------------------------------------------------------------------------------------------------------

SELECT test_check(
    'workflow run: the RUN row, then the nodes in task_index order',
    array_agg(r.row_kind || '@' || r.row_position || ':' || COALESCE(r.task_index::text, '-') ORDER BY r.row_position)
        = ARRAY['RUN@0:-', 'NODE@1:0', 'NODE@2:1', 'NODE@3:2', 'NODE@4:3', 'NODE@5:4', 'NODE@6:5', 'NODE@7:6'],
    array_agg(r.row_kind || '@' || r.row_position || ':' || COALESCE(r.task_index::text, '-')
        ORDER BY r.row_position)::text
)
FROM horsies_monitoring_workflow_run('00000000-0000-4000-8000-00000000f001') AS r;
SELECT test_check(
    'workflow run: the run columns',
    r.workflow_id = '00000000-0000-4000-8000-00000000f001' AND r.workflow_name = 'etl'
    AND r.definition_key = 'defs.etl.v1' AND r.status = 'RUNNING' AND r.created_at = test_base() - interval '3 hours'
    AND r.completed_at IS NULL,
    row_to_json(r)::text
)
FROM horsies_monitoring_workflow_run('00000000-0000-4000-8000-00000000f001') AS r
WHERE r.row_kind = 'RUN';
SELECT test_check(
    'workflow run: child counts for a sub-workflow node; NULL for a task node, an empty child run and no child run',
    array_agg(r.task_index || '=' || COALESCE(r.child_total::text, '-') || '/' || COALESCE(r.child_failed::text, '-')
        ORDER BY r.task_index)
        = ARRAY['0=-/-', '1=-/-', '2=3/2', '3=-/-', '4=-/-', '5=-/-', '6=-/-'],
    array_agg(r.task_index || '=' || COALESCE(r.child_total::text, '-') || '/'
        || COALESCE(r.child_failed::text, '-') ORDER BY r.task_index)::text
)
FROM horsies_monitoring_workflow_run('00000000-0000-4000-8000-00000000f001') AS r
WHERE r.row_kind = 'NODE';
SELECT test_check(
    'workflow run: node columns as stored, dependencies as stored (the caller drops the unknown index 7)',
    r.node_id = 'node-2' AND r.task_name = 'step_2' AND r.status = 'RUNNING' AND r.dependencies = '{0,7}'
    AND NOT r.allow_failed_deps AND r.is_subworkflow
    AND r.sub_workflow_id = '00000000-0000-4000-8000-00000000f004'
    AND r.started_at = test_base() - interval '170 minutes' AND r.completed_at IS NULL
    AND r.workflow_id IS NULL AND r.created_at IS NULL,
    row_to_json(r)::text
)
FROM horsies_monitoring_workflow_run('00000000-0000-4000-8000-00000000f001') AS r
WHERE r.row_kind = 'NODE' AND r.task_index = 2;
SELECT test_check(
    'workflow run: a child run is read like a root run',
    array_agg(r.row_kind || ':' || r.status ORDER BY r.row_position)
        = ARRAY['RUN:RUNNING', 'NODE:FAILED', 'NODE:COMPLETED', 'NODE:FAILED'],
    array_agg(r.row_kind || ':' || r.status ORDER BY r.row_position)::text
)
FROM horsies_monitoring_workflow_run('00000000-0000-4000-8000-00000000f004') AS r;
SELECT test_check(
    'workflow run: a run with no node is the RUN row only',
    array_agg(r.row_kind) = ARRAY['RUN'],
    array_agg(r.row_kind)::text
)
FROM horsies_monitoring_workflow_run('00000000-0000-4000-8000-00000000f005') AS r;
SELECT test_check(
    'workflow run: an unknown id is one ABSENT row',
    array_agg(r.row_kind || ':' || r.workflow_id) = ARRAY['ABSENT:00000000-0000-4000-8000-00000000ffff'],
    array_agg(r.row_kind || ':' || r.workflow_id)::text
)
FROM horsies_monitoring_workflow_run('00000000-0000-4000-8000-00000000ffff') AS r;

SELECT test_raises('workflow run refuses a NULL id',
    $q$SELECT * FROM horsies_monitoring_workflow_run(NULL)$q$, '22004');

------------------------------------------------------------------------------------------------------------------------
-- horsies_monitoring_workflow_node
------------------------------------------------------------------------------------------------------------------------

SELECT test_check(
    'workflow node: a node of a live task carries the live row and its attempts (encoded as in history)',
    n.row_kind = 'NODE' AND n.node_task_index = 0 AND n.node_id = 'node-0' AND n.node_task_name = 'step_0'
    AND n.node_status = 'ENQUEUED' AND NOT n.node_is_subworkflow
    AND n.node_task_id = '00000000-0000-4000-8000-0000000000a1' AND n.node_result IS NULL
    AND n.location = 'LIVE' AND n.task_id = n.node_task_id AND n.status = 'PENDING' AND n.retry_count = 1
    AND n.execution = 'SHARED_ASYNC' AND n.worker_hostname IS NULL AND n.terminal_at IS NULL
    AND n.attempt_archive_version = 1
    AND jsonb_array_length(convert_from(n.attempt_snapshot, 'UTF8')::jsonb) = 1
    AND n.workflow_id = '00000000-0000-4000-8000-00000000f001' AND n.workflow_task_index = 0,
    row_to_json(n)::text
)
FROM horsies_monitoring_workflow_node('00000000-0000-4000-8000-00000000f001', 0) AS n;
SELECT test_check(
    'workflow node: a node of a finished task carries the history row, its snapshot and the node result',
    n.node_status = 'FAILED' AND n.node_result = '{"err":{"error_code":"BOOM","message":"try 2"}}'
    AND n.location = 'HISTORY' AND n.status = 'FAILED' AND n.error_code = 'BOOM'
    AND n.final_failed_reason = 'reason of beta' AND n.terminal_at = test_base() - interval '90 minutes'
    AND n.worker_hostname = 'host-w-2' AND n.execution IS NULL
    AND jsonb_array_length(convert_from(n.attempt_snapshot, 'UTF8')::jsonb) = 2
    AND n.attempt_snapshot_digest = sha256(n.attempt_snapshot)
    AND n.result_payload IS NULL AND n.result_digest IS NULL,
    row_to_json(n)::text
)
FROM horsies_monitoring_workflow_node('00000000-0000-4000-8000-00000000f001', 1) AS n;
SELECT test_check(
    'workflow node: a sub-workflow node has no task columns',
    n.row_kind = 'NODE' AND n.node_is_subworkflow AND n.node_task_id IS NULL AND n.location IS NULL
    AND n.task_id IS NULL AND n.attempt_snapshot IS NULL AND n.absence_reason IS NULL,
    row_to_json(n)::text
)
FROM horsies_monitoring_workflow_node('00000000-0000-4000-8000-00000000f001', 2) AS n;
SELECT test_check(
    'workflow node: a node whose task is in neither place resolves with location ABSENT',
    n.row_kind = 'NODE' AND n.node_task_id = '00000000-0000-4000-8000-0000000000c4' AND n.location = 'ABSENT'
    AND n.absence_reason = 'NOT_CLASSIFIED' AND n.attempt_snapshot IS NULL,
    row_to_json(n)::text
)
FROM horsies_monitoring_workflow_node('00000000-0000-4000-8000-00000000f001', 4) AS n;
SELECT test_check(
    'workflow node: a task node before its task exists has no task columns',
    n.row_kind = 'NODE' AND NOT n.node_is_subworkflow AND n.node_task_id IS NULL AND n.location IS NULL,
    row_to_json(n)::text
)
FROM horsies_monitoring_workflow_node('00000000-0000-4000-8000-00000000f001', 5) AS n;
SELECT test_check(
    'workflow node: no node at the index, or no run, is one ABSENT row',
    (SELECT array_agg(n.row_kind || ':' || COALESCE(n.node_task_name, '-'))
     FROM horsies_monitoring_workflow_node('00000000-0000-4000-8000-00000000f001', 99) AS n) = ARRAY['ABSENT:-']
    AND (SELECT array_agg(n.row_kind)
         FROM horsies_monitoring_workflow_node('00000000-0000-4000-8000-00000000ffff', 0) AS n) = ARRAY['ABSENT'],
    ''
);

SELECT test_raises('workflow node refuses a NULL workflow id',
    $q$SELECT * FROM horsies_monitoring_workflow_node(NULL, 0)$q$, '22004');
SELECT test_raises('workflow node refuses a NULL index',
    $q$SELECT * FROM horsies_monitoring_workflow_node('00000000-0000-4000-8000-00000000f001', NULL)$q$, '22004');

------------------------------------------------------------------------------------------------------------------------
-- horsies_monitoring_schedules
------------------------------------------------------------------------------------------------------------------------

SELECT test_check(
    'schedules: soonest next run first, then LIST_END',
    array_agg(s.row_kind || ':' || COALESCE(s.schedule_name, s.list_state) ORDER BY s.row_position NULLS LAST)
        = ARRAY['SCHEDULE:zz-first', 'SCHEDULE:hourly', 'SCHEDULE:Alpha', 'SCHEDULE:nightly', 'LIST_END:COMPLETE'],
    array_agg(s.row_kind || ':' || COALESCE(s.schedule_name, s.list_state) ORDER BY s.row_position NULLS LAST)::text
)
FROM horsies_monitoring_schedules() AS s;
SELECT test_check(
    'schedules: the columns of a fired and of a new schedule',
    (SELECT s.run_count = 3 AND s.last_run_at = test_base() - interval '1 hour' AND s.last_task_id = md5('hourly')::uuid
            AND s.next_run_at = test_base() + interval '10 minutes' AND s.updated_at = test_base()
     FROM horsies_monitoring_schedules() AS s WHERE s.schedule_name = 'hourly')
    AND (SELECT s.run_count = 0 AND s.last_run_at IS NULL AND s.last_task_id IS NULL
         FROM horsies_monitoring_schedules() AS s WHERE s.schedule_name = 'nightly'),
    ''
);

-- seed reads: end

------------------------------------------------------------------------------------------------------------------------
-- Rules that the seed does not show, each inside a rolled-back transaction
------------------------------------------------------------------------------------------------------------------------

COMMIT;
BEGIN;
SELECT test_live_task('00000000-0000-4000-8000-0000000000d1', 'CLAIMED', 'alpha', 'default', 'unknown', 0, FALSE);
SELECT test_check(
    'breakdown: a VALUE group ''unknown'' comes before the UNCLAIMED group of the same total',
    test_breakdown('worker', '{PENDING,CLAIMED}', '{}', '{}', '{}', '{}', '{}', FALSE, 500) = ARRAY[
        'TOTAL:3=3/1/2/0/0/0/0/0/2', 'unknown=1/0/1/0/0/0/0/0/0', '(unclaimed)=1/1/0/0/0/0/0/0/1',
        'w-1=1/0/1/0/0/0/0/0/1'],
    test_breakdown('worker', '{PENDING,CLAIMED}', '{}', '{}', '{}', '{}', '{}', FALSE, 500)::text
);
SELECT test_check(
    'breakdown: a worker named ''unknown'' is its own group, apart from the rows with no claim',
    test_breakdown('worker', '{}', '{}', '{}', '{}', '{}', '{}', FALSE, 500) = ARRAY[
        'TOTAL:5=13/1/2/2/2/4/1/1/5', 'w-1=4/0/1/1/1/0/1/0/2', 'w-2=4/0/0/1/1/2/0/0/2',
        '(unclaimed)=3/1/0/0/0/1/0/1/1', 'W-3=1/0/0/0/0/1/0/0/0', 'unknown=1/0/1/0/0/0/0/0/0'],
    test_breakdown('worker', '{}', '{}', '{}', '{}', '{}', '{}', FALSE, 500)::text
);
ROLLBACK;

-- The history table has no status CHECK. A stored status that is none of the seven makes the counts fail closed
-- with data_corrupted, and counts only in the breakdown total (as Python's breakdown).
BEGIN;
SELECT test_history_task('00000000-0000-4000-8000-0000000000d2', 'STALE', 'alpha', 'default', 'w-1',
    test_base() - interval '1 hour', 0, NULL, NULL, '[]');
SELECT test_raises('stats: a stored status outside the seven is data_corrupted',
    $q$SELECT * FROM horsies_monitoring_task_stats(test_lower(), test_upper(), '{}', '{}', '{}', '{}', '{}', FALSE,
        test_codes(), test_code_categories())$q$, 'XX001');
DO $$
BEGIN
    PERFORM * FROM horsies_monitoring_task_stats(test_lower(), test_upper(), '{}', '{}', '{}', '{}', '{}', FALSE,
        test_codes(), test_code_categories());
    PERFORM test_check('stats: the refusal names the status', FALSE, 'the call succeeded');
EXCEPTION WHEN data_corrupted THEN
    PERFORM test_check('stats: the refusal names the status', SQLERRM LIKE '%status STALE,%', SQLERRM);
END;
$$;
SELECT test_check(
    'stats: a stored status outside the scope does not refuse the counts',
    test_stats('{beta}', '{}', '{}', '{}', '{}', FALSE) = ARRAY[0, 0, 1, 0, 2, 1, 0]::bigint[],
    test_stats('{beta}', '{}', '{}', '{}', '{}', FALSE)::text
);
SELECT test_check(
    'breakdown: a stored status outside the seven counts only in total',
    test_breakdown('task_name', '{}', '{}', '{default}', '{w-1}', '{}', '{}', FALSE, 500) = ARRAY[
        'TOTAL:2=3/0/0/0/1/0/1/0/0', 'alpha=2/0/0/0/1/0/0/0/0', 'beta=1/0/0/0/0/0/1/0/0'],
    test_breakdown('task_name', '{}', '{}', '{default}', '{w-1}', '{}', '{}', FALSE, 500)::text
);
ROLLBACK;

-- Runs of one created_at come by id, highest first; schedules of one next_run_at come by name.
BEGIN;
SELECT test_workflow('00000000-0000-4000-8000-00000000f00a', 'tie', 'RUNNING', test_base(), NULL, NULL);
SELECT test_workflow('00000000-0000-4000-8000-00000000f00c', 'tie', 'RUNNING', test_base(), NULL, NULL);
SELECT test_workflow('00000000-0000-4000-8000-00000000f00b', 'tie', 'RUNNING', test_base(), NULL, NULL);
SELECT test_check(
    'workflow runs: runs of one created_at come by id, highest first',
    test_runs('tie', NULL, 200) = ARRAY['f00c', 'f00b', 'f00a'],
    test_runs('tie', NULL, 200)::text
);
SELECT test_schedule('tie-b', test_base() + interval '1 minute', 0);
SELECT test_schedule('tie-a', test_base() + interval '1 minute', 0);
SELECT test_check(
    'schedules: schedules of one next run come by name',
    array_agg(s.schedule_name ORDER BY s.row_position)
        = ARRAY(SELECT x::character varying FROM unnest(ARRAY['tie-a', 'tie-b', 'zz-first']) AS x ORDER BY x)
          || ARRAY['hourly', 'Alpha', 'nightly']::character varying[],
    array_agg(s.schedule_name ORDER BY s.row_position)::text
)
FROM horsies_monitoring_schedules() AS s
WHERE s.row_kind = 'SCHEDULE';
ROLLBACK;

-- The row bounds.
BEGIN;
INSERT INTO horsies_workflows (
    id, name, definition_key, status, on_error, output_task_index, success_policy, result, error, parent_workflow_id,
    parent_task_index, root_workflow_id, depth, sent_at, created_at, started_at, completed_at, updated_at
)
SELECT gen_random_uuid(), 'bulk-' || lpad(i::text, 5, '0'), NULL, 'COMPLETED', 'fail', NULL, NULL, NULL, NULL,
       NULL, NULL, NULL, 0, test_base(), test_base(), NULL, test_base(), test_base()
FROM generate_series(1, 10000) AS i;
SELECT test_check(
    'workflow names: past 10,000 names the list is cut at the bound and says TRUNCATED',
    count(*) FILTER (WHERE n.row_kind = 'NAME') = 10000
    AND max(n.row_position) = 10000
    AND bool_and(n.list_state = 'TRUNCATED' AND n.row_bound = 10000) FILTER (WHERE n.row_kind = 'LIST_END')
    AND count(*) FILTER (WHERE n.row_kind = 'LIST_END') = 1,
    count(*)::text
)
FROM horsies_monitoring_workflow_names() AS n;
ROLLBACK;

BEGIN;
INSERT INTO horsies_schedule_state (
    schedule_name, next_run_at, last_run_at, last_task_id, run_count, config_hash, updated_at
)
SELECT 'bulk-' || i, test_base() + make_interval(secs => i), NULL, NULL, 0, sha256(int4send(i)), test_base()
FROM generate_series(1, 9997) AS i;
SELECT test_check(
    'schedules: 10,001 rows: the list holds the 10,000 soonest and says TRUNCATED',
    count(*) FILTER (WHERE s.row_kind = 'SCHEDULE') = 10000
    AND bool_and(s.list_state = 'TRUNCATED') FILTER (WHERE s.row_kind = 'LIST_END')
    AND bool_or(s.schedule_name = 'nightly') AND NOT bool_or(s.schedule_name = 'bulk-9997'),
    count(*)::text
)
FROM horsies_monitoring_schedules() AS s;
DELETE FROM horsies_schedule_state WHERE schedule_name = 'bulk-9997';
SELECT test_check(
    'schedules: exactly 10,000 rows are COMPLETE',
    count(*) FILTER (WHERE s.row_kind = 'SCHEDULE') = 10000
    AND bool_and(s.list_state = 'COMPLETE') FILTER (WHERE s.row_kind = 'LIST_END'),
    count(*)::text
)
FROM horsies_monitoring_schedules() AS s;
ROLLBACK;

BEGIN;
INSERT INTO horsies_workflow_tasks (
    downstream_subworkflow,
    id, workflow_id, task_index, node_id, task_name, input, queue_name, priority, dependencies, args_from,
    workflow_ctx_from, allow_failed_deps, join_type, min_success, task_options, max_retries, execution, good_until,
    retention_class_key, status, task_id, is_subworkflow, sub_workflow_id, sub_workflow_name, sub_definition_key,
    sub_workflow_summary, result, error, created_at, started_at, completed_at
)
SELECT 'NONE',
       gen_random_uuid(), '00000000-0000-4000-8000-00000000f002', i, NULL, 'bulk', '{}', 'default', 50, '{}', NULL,
       NULL, FALSE, 'all', NULL, NULL, 0, 'SHARED_ASYNC', NULL, 'forever', 'PENDING', NULL, FALSE, NULL, NULL, NULL,
       NULL, NULL, NULL, test_base(), NULL, NULL
FROM generate_series(0, 10000) AS i;
SELECT test_raises('workflow run: a run past the 10,000-node bound of the workflow start is data_corrupted',
    $q$SELECT * FROM horsies_monitoring_workflow_run('00000000-0000-4000-8000-00000000f002')$q$, 'XX001');
DELETE FROM horsies_workflow_tasks
WHERE workflow_id = '00000000-0000-4000-8000-00000000f002' AND task_index = 10000;
SELECT test_check(
    'workflow run: a run of 10,000 nodes is read whole',
    count(*) FILTER (WHERE r.row_kind = 'NODE') = 10000 AND max(r.row_position) = 10000,
    count(*)::text
)
FROM horsies_monitoring_workflow_run('00000000-0000-4000-8000-00000000f002') AS r;
ROLLBACK;

------------------------------------------------------------------------------------------------------------------------
-- One snapshot (two sessions)
------------------------------------------------------------------------------------------------------------------------

COMMIT;
SELECT dblink_connect('peer', current_setting('horsies_test.peer_connstr'));

-- Session B moves the RUNNING task a3 to history (COMPLETED, inside the window) and commits after the counting
-- statement started. The statement counts the task once, as RUNNING, from its snapshot.
SELECT dblink_send_query(
    'peer',
    $q$
    DO $do$
    BEGIN
        PERFORM pg_sleep(0.5);
        DELETE FROM horsies_tasks WHERE id = '00000000-0000-4000-8000-0000000000a3';
        PERFORM test_history_task('00000000-0000-4000-8000-0000000000a3', 'COMPLETED', 'beta', 'default', 'w-2',
            now(), 0, NULL, NULL, '[]');
    END;
    $do$
    $q$
);
SELECT test_check(
    'stats: a move that commits during the statement counts the task once, as RUNNING',
    s.counts = ARRAY[1, 1, 2, 2, 4, 1, 1]::bigint[],
    s.counts::text
)
FROM (SELECT pg_sleep(1.5)) AS wait
CROSS JOIN LATERAL (SELECT test_stats('{}', '{}', '{}', '{}', '{}', FALSE) AS counts) AS s;
SELECT test_check(
    'stats: the peer move committed',
    array_agg(r.status) = ARRAY['DO'],
    array_agg(r.status)::text
)
FROM dblink_get_result('peer') AS r(status text);
SELECT 1 FROM dblink_get_result('peer') AS r(status text);
SELECT test_check(
    'stats: the next statement counts the moved task once, as COMPLETED',
    test_stats('{}', '{}', '{}', '{}', '{}', FALSE) = ARRAY[1, 1, 1, 3, 4, 1, 1]::bigint[],
    test_stats('{}', '{}', '{}', '{}', '{}', FALSE)::text
);

-- Session B moves the task a1 of node 0 to history (attempts deleted, live row deleted, history row inserted) and
-- commits after the node statement started. The node read answers LIVE with its attempt, from the statement's
-- snapshot.
COMMIT;
SELECT dblink_send_query(
    'peer',
    $q$
    DO $do$
    BEGIN
        PERFORM pg_sleep(0.5);
        DELETE FROM horsies_task_attempts WHERE task_id = '00000000-0000-4000-8000-0000000000a1';
        DELETE FROM horsies_tasks WHERE id = '00000000-0000-4000-8000-0000000000a1';
        PERFORM test_history_task('00000000-0000-4000-8000-0000000000a1', 'CANCELLED', 'alpha', 'default', NULL,
            now(), 1, NULL, '00000000-0000-4000-8000-00000000f001', '[]');
    END;
    $do$
    $q$
);
SELECT test_check(
    'workflow node: a move that commits during the statement leaves the answer LIVE with its attempt',
    n.location = 'LIVE' AND n.status = 'PENDING'
    AND jsonb_array_length(convert_from(n.attempt_snapshot, 'UTF8')::jsonb) = 1,
    row_to_json(n)::text
)
FROM (SELECT pg_sleep(1.5)) AS wait
CROSS JOIN LATERAL horsies_monitoring_workflow_node('00000000-0000-4000-8000-00000000f001', 0) AS n;
SELECT test_check(
    'workflow node: the peer move committed',
    array_agg(r.status) = ARRAY['DO'],
    array_agg(r.status)::text
)
FROM dblink_get_result('peer') AS r(status text);
SELECT 1 FROM dblink_get_result('peer') AS r(status text);
SELECT test_check(
    'workflow node: the next statement reads the moved task in HISTORY',
    n.location = 'HISTORY' AND n.status = 'CANCELLED'
    AND jsonb_array_length(convert_from(n.attempt_snapshot, 'UTF8')::jsonb) = 0,
    row_to_json(n)::text
)
FROM horsies_monitoring_workflow_node('00000000-0000-4000-8000-00000000f001', 0) AS n;

------------------------------------------------------------------------------------------------------------------------
-- Summary
------------------------------------------------------------------------------------------------------------------------

SELECT dblink_disconnect('peer');
SELECT 'passed cases: ' || last_value AS summary FROM test_passed_cases;
