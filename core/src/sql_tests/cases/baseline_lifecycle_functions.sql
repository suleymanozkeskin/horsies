-- The functions of 0004_lifecycle_functions.sql on a fresh install: each guarantee as a pass case and each refusal
-- as a fail case. Single-session cases only; the two-session cases (lock order, a row that moves during a send)
-- are in sql_tests/baseline_lifecycle_functions.rs. The caller runs the script in one transaction and rolls it back, so
-- NOW() is one instant for the whole script: the cases place row times relative to it.
--
-- Every check calls lc_expect or lc_expect_sqlstate, which raise on a mismatch with the case label.

------------------------------------------------------------------------------------------------------------------------
-- Fixtures
------------------------------------------------------------------------------------------------------------------------

-- The published provenance reader is rendered by horsies_publish_history_readers. This stand-in reads the
-- history parent: one row when history holds the id, no row otherwise.
CREATE FUNCTION horsies_task_provenance_staged(p_task_id uuid)
RETURNS TABLE (
    task_id uuid, status text, terminal_at timestamptz, terminalization_kind text, is_workflow_task boolean,
    command_fingerprint_version smallint, command_fingerprint bytea, sent_at timestamptz
)
LANGUAGE sql STABLE AS $$
    SELECT h.task_id, h.status::text, h.terminal_at, h.terminalization_kind::text, h.is_workflow_task,
        h.command_fingerprint_version, h.command_fingerprint, h.sent_at
    FROM horsies_task_history h
    WHERE h.task_id = p_task_id
    LIMIT 1
$$;

-- The duplicate guard of the moves is a published reader too. This stand-in answers TRUE when the provenance
-- stand-in finds the id or a clock exception records it.
CREATE FUNCTION horsies_task_history_duplicate_staged(p_task_id uuid)
RETURNS boolean LANGUAGE sql STABLE AS $$
    SELECT EXISTS (SELECT 1 FROM horsies_task_provenance_staged(p_task_id))
        OR EXISTS (SELECT 1 FROM horsies_task_history_clock_exceptions AS e WHERE e.task_id = p_task_id)
$$;

-- One heartbeat leaf around NOW(), with the probe index of the run-time leaves.
CREATE TABLE horsies_heartbeats_lc_window PARTITION OF horsies_heartbeats
    FOR VALUES FROM (now() - interval '2 days') TO (now() + interval '2 days');
CREATE INDEX horsies_heartbeats_lc_window_probe ON horsies_heartbeats_lc_window (task_id, role, sent_at DESC);

CREATE FUNCTION lc_expect(p_label text, p_actual text, p_expected text) RETURNS void LANGUAGE plpgsql AS $$
BEGIN
    IF p_actual IS DISTINCT FROM p_expected THEN
        RAISE EXCEPTION 'case %: expected %, got %', p_label, p_expected, p_actual;
    END IF;
END
$$;

-- Runs one statement and checks that it fails with the given SQLSTATE. The block rolls back what it wrote.
CREATE FUNCTION lc_expect_sqlstate(p_label text, p_statement text, p_sqlstate text) RETURNS void
LANGUAGE plpgsql AS $$
BEGIN
    BEGIN
        EXECUTE p_statement;
    EXCEPTION WHEN OTHERS THEN
        IF SQLSTATE <> p_sqlstate THEN
            RAISE EXCEPTION 'case %: expected SQLSTATE %, got % (%)', p_label, p_sqlstate, SQLSTATE, SQLERRM;
        END IF;
        RETURN;
    END;
    RAISE EXCEPTION 'case %: expected SQLSTATE %, the statement succeeded', p_label, p_sqlstate;
END
$$;

-- A valid live row. CLAIMED and RUNNING rows belong to p_worker at the claim generation now() - 1 s; a CLAIMED row
-- has a live lease. Cases change single columns with UPDATE afterwards.
CREATE FUNCTION lc_task(p_id uuid, p_status text, p_worker text DEFAULT 'w1', p_is_workflow boolean DEFAULT FALSE)
RETURNS void LANGUAGE sql AS $$
    INSERT INTO horsies_tasks (
        id, task_name, queue_name, priority, input, task_options, execution, retention_class_key, sent_at,
        created_at, command_fingerprint_version, command_fingerprint, idempotency_key_digest, status, enqueued_at,
        claimed_at, claimed_by_worker_id, claim_expires_at, started_at, finalizing_at, finalizing_by_worker_id,
        worker_pid, worker_hostname, worker_process_name, retry_count, max_retries, next_retry_at, good_until,
        is_workflow_task, updated_at, retain_rerun_input, rerun_of_task_id, rerun_root_task_id
    ) VALUES (
        p_id, 'lc.task', 'q_lc', 100, '{"x":1}', NULL, 'ISOLATED', 'forever', now(),
        now(),
        CASE WHEN p_is_workflow THEN NULL ELSE 1::smallint END,
        CASE WHEN p_is_workflow THEN NULL ELSE sha256('lc'::bytea) END,
        NULL, p_status, now() - interval '1 minute',
        CASE WHEN p_status = 'PENDING' THEN NULL ELSE now() - interval '1 second' END,
        CASE WHEN p_status = 'PENDING' THEN NULL ELSE p_worker END,
        CASE WHEN p_status = 'CLAIMED' THEN now() + interval '1 minute' END,
        CASE WHEN p_status = 'RUNNING' THEN now() - interval '1 second' END,
        NULL, NULL,
        CASE WHEN p_status = 'RUNNING' THEN 7 END,
        CASE WHEN p_status = 'RUNNING' THEN 'host-a' END,
        CASE WHEN p_status = 'RUNNING' THEN 'worker-7' END,
        0, 3, NULL, NULL,
        p_is_workflow, now(), TRUE, NULL, NULL
    )
$$;

CREATE FUNCTION lc_workflow(p_id uuid, p_status text) RETURNS void LANGUAGE sql AS $$
    INSERT INTO horsies_workflows (
        id, name, definition_key, status, on_error, output_task_index, success_policy, result, error,
        parent_workflow_id, parent_task_index, root_workflow_id, depth, sent_at, created_at, started_at,
        completed_at, updated_at
    ) VALUES (
        p_id, 'lc.workflow', NULL, p_status, 'fail', NULL, NULL, NULL, NULL,
        NULL, NULL, p_id, 0, now(), now(), now(), NULL, now()
    )
$$;

CREATE FUNCTION lc_node(
    p_id uuid, p_workflow uuid, p_index integer, p_status text, p_task uuid, p_dependencies integer[] DEFAULT '{}'
) RETURNS void LANGUAGE sql AS $$
    INSERT INTO horsies_workflow_tasks (downstream_subworkflow,
        id, workflow_id, task_index, node_id, task_name, input, queue_name, priority, dependencies, args_from,
        workflow_ctx_from, allow_failed_deps, join_type, min_success, task_options, max_retries, execution,
        good_until, retention_class_key, status, task_id, is_subworkflow, sub_workflow_id, sub_workflow_name,
        sub_definition_key, sub_workflow_summary, result, error, created_at, started_at, completed_at
    ) VALUES ('NONE',
        p_id, p_workflow, p_index, 'node_' || p_index, 'lc.task', '{"x":1}', 'q_lc', 100, p_dependencies, NULL,
        NULL, FALSE, 'all', NULL, NULL, 1, 'ISOLATED',
        NULL, 'forever', p_status, p_task, FALSE, NULL, NULL,
        NULL, NULL, NULL, NULL, now(),
        CASE WHEN p_status = 'RUNNING' THEN now() - interval '1 hour' END, NULL
    )
$$;

-- One unkeyed first send of lc.task with the fingerprint sha256(p_fingerprint_seed) and the given sent_at.
CREATE FUNCTION lc_send(p_id uuid, p_fingerprint_seed text, p_sent_at timestamptz, p_send_kind text DEFAULT 'FIRST')
RETURNS text LANGUAGE sql AS $$
    SELECT outcome FROM horsies_enqueue_task(
        p_id, 'lc.task', 'q_lc', 100, '{"x":1}', NULL, 'ISOLATED', 'forever', TRUE, 3, NULL,
        p_sent_at, 'NOW', NULL, NULL, 1::smallint, sha256(p_fingerprint_seed::bytea),
        'FIRST_RUN', NULL, NULL, 'UNKEYED', NULL, NULL, NULL, p_send_kind, 'NO_CARRIER', NULL, NULL
    )
$$;

-- One keyed first send with a reservation window of one hour; returns 'outcome:task_id'.
CREATE FUNCTION lc_send_keyed(p_id uuid, p_fingerprint_seed text, p_key_seed text, p_sent_at timestamptz)
RETURNS text LANGUAGE sql AS $$
    SELECT outcome || ':' || task_id FROM horsies_enqueue_task(
        p_id, 'lc.task', 'q_lc', 100, '{"x":1}', NULL, 'ISOLATED', 'forever', TRUE, 3, NULL,
        p_sent_at, 'NOW', NULL, NULL, 1::smallint, sha256(p_fingerprint_seed::bytea),
        'FIRST_RUN', NULL, NULL, 'KEYED', sha256(p_key_seed::bytea), 1::smallint, interval '1 hour', 'FIRST',
        'NO_CARRIER', NULL, NULL
    )
$$;

-- One keyed first send with the given reservation window; returns 'outcome:task_id'.
CREATE FUNCTION lc_send_keyed_window(p_id uuid, p_key_seed text, p_window interval)
RETURNS text LANGUAGE sql AS $$
    SELECT outcome || ':' || task_id FROM horsies_enqueue_task(
        p_id, 'lc.task', 'q_lc', 100, '{"x":1}', NULL, 'ISOLATED', 'forever', TRUE, 3, NULL,
        now(), 'NOW', NULL, NULL, 1::smallint, sha256(p_id::text::bytea),
        'FIRST_RUN', NULL, NULL, 'KEYED', sha256(p_key_seed::bytea), 1::smallint, p_window, 'FIRST',
        'NO_CARRIER', NULL, NULL
    )
$$;

-- One unkeyed first send of lc.task to q_trace with the given trace arguments.
CREATE FUNCTION lc_send_traced(
    p_id uuid, p_fingerprint_seed text, p_trace_kind text, p_trace_carrier text, p_trace_id bytea
) RETURNS text LANGUAGE sql AS $$
    SELECT outcome FROM horsies_enqueue_task(
        p_id, 'lc.task', 'q_trace', 100, '{"x":1}', NULL, 'ISOLATED', 'forever', TRUE, 3, NULL,
        now(), 'NOW', NULL, NULL, 1::smallint, sha256(p_fingerprint_seed::bytea),
        'FIRST_RUN', NULL, NULL, 'UNKEYED', NULL, NULL, NULL, 'FIRST', p_trace_kind, p_trace_carrier, p_trace_id
    )
$$;

------------------------------------------------------------------------------------------------------------------------
-- horsies_insert_task_rows
------------------------------------------------------------------------------------------------------------------------

SELECT lc_expect(
    'insert rows: two new rows, in input order',
    (SELECT string_agg(ordinality || ':' || outcome, ',' ORDER BY ordinality) FROM horsies_insert_task_rows(
        ARRAY['00000000-0000-7000-8000-000000000a02', '00000000-0000-7000-8000-000000000a01']::uuid[],
        ARRAY['lc.task', 'lc.task'], ARRAY['q_lc', 'q_lc'], ARRAY[100, 50], ARRAY['{}', '{"a":1}'],
        ARRAY[NULL, '{"o":1}'], ARRAY['ISOLATED', 'SHARED_ASYNC'], ARRAY['forever', 'forever'],
        ARRAY[TRUE, FALSE], ARRAY[0, 2], ARRAY[NULL, now() + interval '1 hour']::timestamptz[],
        ARRAY[now(), now()], ARRAY[now(), now() + interval '5 seconds'], ARRAY[FALSE, FALSE],
        ARRAY[1, 1]::smallint[], ARRAY[sha256('a2'::bytea), sha256('a1'::bytea)], ARRAY[NULL, NULL]::bytea[],
        ARRAY[NULL, NULL]::uuid[], ARRAY[NULL, NULL]::uuid[], ARRAY[NULL, NULL]::text[], ARRAY[NULL, NULL]::bytea[],
        now() - interval '7 seconds', 'NONE')),
    '1:INSERTED,2:INSERTED'
);
SELECT lc_expect(
    'insert rows: every value as given, the fixed values set, created_at and updated_at the row time',
    (SELECT string_agg(concat_ws('|', status, priority, input, task_options, execution, retain_rerun_input,
        max_retries, good_until IS NOT NULL, enqueued_at - now(), retry_count, claimed_at, claim_expires_at,
        started_at, next_retry_at, created_at - now(), updated_at - now()), ',' ORDER BY id)
     FROM horsies_tasks WHERE id IN ('00000000-0000-7000-8000-000000000a01', '00000000-0000-7000-8000-000000000a02')),
    'PENDING|50|{"a":1}|{"o":1}|SHARED_ASYNC|f|2|t|00:00:05|0|-00:00:07|-00:00:07,'
    || 'PENDING|100|{}|ISOLATED|t|0|f|00:00:00|0|-00:00:07|-00:00:07'
);
SELECT lc_expect(
    'insert rows: a conflict with the same command, another command, and a new row',
    (SELECT string_agg(ordinality || ':' || outcome, ',' ORDER BY ordinality) FROM horsies_insert_task_rows(
        ARRAY['00000000-0000-7000-8000-000000000a01', '00000000-0000-7000-8000-000000000a02',
              '00000000-0000-7000-8000-000000000a03']::uuid[],
        ARRAY['lc.task', 'lc.task', 'lc.task'], ARRAY['q_lc', 'q_lc', 'q_lc'], ARRAY[1, 1, 1],
        ARRAY['{}', '{}', '{}'], ARRAY[NULL, NULL, NULL]::text[], ARRAY['ISOLATED', 'ISOLATED', 'ISOLATED'],
        ARRAY['forever', 'forever', 'forever'], ARRAY[TRUE, TRUE, TRUE], ARRAY[0, 0, 0],
        ARRAY[NULL, NULL, NULL]::timestamptz[], ARRAY[now(), now(), now()], ARRAY[now(), now(), now()],
        ARRAY[FALSE, FALSE, FALSE], ARRAY[1, 1, 1]::smallint[],
        ARRAY[sha256('a1'::bytea), sha256('other'::bytea), sha256('a3'::bytea)], ARRAY[NULL, NULL, NULL]::bytea[],
        ARRAY[NULL, NULL, NULL]::uuid[], ARRAY[NULL, NULL, NULL]::uuid[], ARRAY[NULL, NULL, NULL]::text[],
        ARRAY[NULL, NULL, NULL]::bytea[], now(), 'FOR_UPDATE')),
    '1:SAME_COMMAND,2:PAYLOAD_MISMATCH,3:INSERTED'
);
SELECT lc_expect(
    'insert rows: a conflict changes nothing on the stored row',
    (SELECT priority || ':' || input FROM horsies_tasks WHERE id = '00000000-0000-7000-8000-000000000a01'),
    '50:{"a":1}'
);
-- An engine-minted row carries no fingerprint, so its conflict is never SAME_COMMAND.
SELECT lc_task('00000000-0000-7000-8000-000000000a04', 'PENDING', NULL, TRUE);
SELECT lc_expect(
    'insert rows: a row with no fingerprint never matches',
    (SELECT outcome FROM horsies_insert_task_rows(
        ARRAY['00000000-0000-7000-8000-000000000a04']::uuid[], ARRAY['lc.task'], ARRAY['q_lc'], ARRAY[100],
        ARRAY['{"x":1}'], ARRAY[NULL]::text[], ARRAY['ISOLATED'], ARRAY['forever'], ARRAY[TRUE], ARRAY[3],
        ARRAY[NULL]::timestamptz[], ARRAY[(SELECT sent_at FROM horsies_tasks
                                           WHERE id = '00000000-0000-7000-8000-000000000a04')],
        ARRAY[now()], ARRAY[TRUE], ARRAY[NULL]::smallint[], ARRAY[NULL]::bytea[], ARRAY[NULL]::bytea[],
        ARRAY[NULL]::uuid[], ARRAY[NULL]::uuid[], ARRAY[NULL]::text[], ARRAY[NULL]::bytea[], now(), 'NONE')),
    'PAYLOAD_MISMATCH'
);
SELECT lc_expect(
    'insert rows: empty arrays insert nothing',
    (SELECT count(*)::text FROM horsies_insert_task_rows(
        ARRAY[]::uuid[], ARRAY[]::text[], ARRAY[]::text[], ARRAY[]::integer[], ARRAY[]::text[], ARRAY[]::text[],
        ARRAY[]::text[], ARRAY[]::text[], ARRAY[]::boolean[], ARRAY[]::integer[], ARRAY[]::timestamptz[],
        ARRAY[]::timestamptz[], ARRAY[]::timestamptz[], ARRAY[]::boolean[], ARRAY[]::smallint[], ARRAY[]::bytea[],
        ARRAY[]::bytea[], ARRAY[]::uuid[], ARRAY[]::uuid[], ARRAY[]::text[], ARRAY[]::bytea[], now(), 'NONE')),
    '0'
);
SELECT lc_expect_sqlstate(
    'insert rows: arrays of different lengths',
    $q$SELECT * FROM horsies_insert_task_rows(
        ARRAY[gen_random_uuid()], ARRAY['lc.task', 'lc.task'], ARRAY['q_lc'], ARRAY[1], ARRAY['{}'],
        ARRAY[NULL]::text[], ARRAY['ISOLATED'], ARRAY['forever'], ARRAY[TRUE], ARRAY[0], ARRAY[NULL]::timestamptz[],
        ARRAY[now()], ARRAY[now()], ARRAY[FALSE], ARRAY[1]::smallint[], ARRAY[sha256('x'::bytea)],
        ARRAY[NULL]::bytea[], ARRAY[NULL]::uuid[], ARRAY[NULL]::uuid[], ARRAY[NULL]::text[], ARRAY[NULL]::bytea[],
        now(), 'NONE')$q$,
    '22023'
);
SELECT lc_expect(
    'insert rows: a row stores the trace pair it is given',
    (SELECT outcome FROM horsies_insert_task_rows(
        ARRAY['00000000-0000-7000-8000-000000000a05']::uuid[], ARRAY['lc.task'], ARRAY['q_trace'], ARRAY[1],
        ARRAY['{}'], ARRAY[NULL]::text[], ARRAY['ISOLATED'], ARRAY['forever'], ARRAY[TRUE], ARRAY[0],
        ARRAY[NULL]::timestamptz[], ARRAY[now()], ARRAY[now()], ARRAY[FALSE], ARRAY[1]::smallint[],
        ARRAY[sha256('a05'::bytea)], ARRAY[NULL]::bytea[], ARRAY[NULL]::uuid[], ARRAY[NULL]::uuid[],
        ARRAY['{"traceparent":"t"}'], ARRAY[decode('4bf92f3577b34da6a3ce929d0e0e4736', 'hex')], now(), 'NONE')),
    'INSERTED'
);
SELECT lc_expect(
    'insert rows: the stored trace pair',
    (SELECT trace_carrier || '|' || encode(trace_id, 'hex') FROM horsies_tasks
     WHERE id = '00000000-0000-7000-8000-000000000a05'),
    '{"traceparent":"t"}|4bf92f3577b34da6a3ce929d0e0e4736'
);
SELECT lc_expect_sqlstate(
    'insert rows: trace arrays of another length',
    $q$SELECT * FROM horsies_insert_task_rows(
        ARRAY[gen_random_uuid()], ARRAY['lc.task'], ARRAY['q_trace'], ARRAY[1], ARRAY['{}'],
        ARRAY[NULL]::text[], ARRAY['ISOLATED'], ARRAY['forever'], ARRAY[TRUE], ARRAY[0], ARRAY[NULL]::timestamptz[],
        ARRAY[now()], ARRAY[now()], ARRAY[FALSE], ARRAY[1]::smallint[], ARRAY[sha256('x'::bytea)],
        ARRAY[NULL]::bytea[], ARRAY[NULL]::uuid[], ARRAY[NULL]::uuid[], ARRAY[NULL, NULL]::text[],
        ARRAY[NULL]::bytea[], now(), 'NONE')$q$,
    '22023'
);
SELECT lc_expect_sqlstate(
    'insert rows: a carrier with no trace id (table CHECK)',
    $q$SELECT * FROM horsies_insert_task_rows(
        ARRAY[gen_random_uuid()], ARRAY['lc.task'], ARRAY['q_trace'], ARRAY[1], ARRAY['{}'],
        ARRAY[NULL]::text[], ARRAY['ISOLATED'], ARRAY['forever'], ARRAY[TRUE], ARRAY[0], ARRAY[NULL]::timestamptz[],
        ARRAY[now()], ARRAY[now()], ARRAY[FALSE], ARRAY[1]::smallint[], ARRAY[sha256('x'::bytea)],
        ARRAY[NULL]::bytea[], ARRAY[NULL]::uuid[], ARRAY[NULL]::uuid[], ARRAY['{}']::text[],
        ARRAY[NULL]::bytea[], now(), 'NONE')$q$,
    '23514'
);
SELECT lc_expect_sqlstate(
    'insert rows: a trace id that is not 16 bytes (table CHECK)',
    $q$SELECT * FROM horsies_insert_task_rows(
        ARRAY[gen_random_uuid()], ARRAY['lc.task'], ARRAY['q_trace'], ARRAY[1], ARRAY['{}'],
        ARRAY[NULL]::text[], ARRAY['ISOLATED'], ARRAY['forever'], ARRAY[TRUE], ARRAY[0], ARRAY[NULL]::timestamptz[],
        ARRAY[now()], ARRAY[now()], ARRAY[FALSE], ARRAY[1]::smallint[], ARRAY[sha256('x'::bytea)],
        ARRAY[NULL]::bytea[], ARRAY[NULL]::uuid[], ARRAY[NULL]::uuid[], ARRAY['{}']::text[],
        ARRAY['\x00'::bytea], now(), 'NONE')$q$,
    '23514'
);
SELECT lc_expect_sqlstate(
    'insert rows: a NULL element in a required column',
    $q$SELECT * FROM horsies_insert_task_rows(
        ARRAY[gen_random_uuid()], ARRAY[NULL]::text[], ARRAY['q_lc'], ARRAY[1], ARRAY['{}'],
        ARRAY[NULL]::text[], ARRAY['ISOLATED'], ARRAY['forever'], ARRAY[TRUE], ARRAY[0], ARRAY[NULL]::timestamptz[],
        ARRAY[now()], ARRAY[now()], ARRAY[FALSE], ARRAY[1]::smallint[], ARRAY[sha256('x'::bytea)],
        ARRAY[NULL]::bytea[], ARRAY[NULL]::uuid[], ARRAY[NULL]::uuid[], ARRAY[NULL]::text[], ARRAY[NULL]::bytea[],
        now(), 'NONE')$q$,
    '22023'
);
SELECT lc_expect_sqlstate(
    'insert rows: a repeated id',
    $q$SELECT * FROM horsies_insert_task_rows(
        ARRAY['00000000-0000-7000-8000-000000000aff', '00000000-0000-7000-8000-000000000aff']::uuid[],
        ARRAY['lc.task', 'lc.task'], ARRAY['q_lc', 'q_lc'], ARRAY[1, 1], ARRAY['{}', '{}'],
        ARRAY[NULL, NULL]::text[], ARRAY['ISOLATED', 'ISOLATED'], ARRAY['forever', 'forever'], ARRAY[TRUE, TRUE],
        ARRAY[0, 0], ARRAY[NULL, NULL]::timestamptz[], ARRAY[now(), now()], ARRAY[now(), now()],
        ARRAY[FALSE, FALSE], ARRAY[1, 1]::smallint[], ARRAY[sha256('x'::bytea), sha256('x'::bytea)],
        ARRAY[NULL, NULL]::bytea[], ARRAY[NULL, NULL]::uuid[], ARRAY[NULL, NULL]::uuid[], ARRAY[NULL, NULL]::text[],
        ARRAY[NULL, NULL]::bytea[], now(), 'NONE')$q$,
    '22023'
);
SELECT lc_expect_sqlstate(
    'insert rows: a NULL row time',
    $q$SELECT * FROM horsies_insert_task_rows(
        ARRAY[gen_random_uuid()], ARRAY['lc.task'], ARRAY['q_lc'], ARRAY[1], ARRAY['{}'],
        ARRAY[NULL]::text[], ARRAY['ISOLATED'], ARRAY['forever'], ARRAY[TRUE], ARRAY[0], ARRAY[NULL]::timestamptz[],
        ARRAY[now()], ARRAY[now()], ARRAY[FALSE], ARRAY[1]::smallint[], ARRAY[sha256('x'::bytea)],
        ARRAY[NULL]::bytea[], ARRAY[NULL]::uuid[], ARRAY[NULL]::uuid[], ARRAY[NULL]::text[], ARRAY[NULL]::bytea[],
        NULL, 'NONE')$q$,
    '22023'
);
SELECT lc_expect_sqlstate(
    'insert rows: an unknown conflict lock',
    $q$SELECT * FROM horsies_insert_task_rows(
        ARRAY[]::uuid[], ARRAY[]::text[], ARRAY[]::text[], ARRAY[]::integer[], ARRAY[]::text[], ARRAY[]::text[],
        ARRAY[]::text[], ARRAY[]::text[], ARRAY[]::boolean[], ARRAY[]::integer[], ARRAY[]::timestamptz[],
        ARRAY[]::timestamptz[], ARRAY[]::timestamptz[], ARRAY[]::boolean[], ARRAY[]::smallint[], ARRAY[]::bytea[],
        ARRAY[]::bytea[], ARRAY[]::uuid[], ARRAY[]::uuid[], ARRAY[]::text[], ARRAY[]::bytea[], now(), 'SKIP')$q$,
    '22023'
);

------------------------------------------------------------------------------------------------------------------------
-- horsies_enqueue_task
------------------------------------------------------------------------------------------------------------------------

SELECT lc_expect(
    'enqueue: a first send inserts', lc_send('00000000-0000-7000-8000-000000000b01', 'b01', now()), 'INSERTED'
);
SELECT lc_expect(
    'enqueue: the row names every value',
    (SELECT concat_ws('|', status, task_name, queue_name, priority, input, execution, retention_class_key,
        retain_rerun_input, max_retries, retry_count, enqueued_at = statement_timestamp(), sent_at = now(),
        is_workflow_task,
        command_fingerprint_version, command_fingerprint = sha256('b01'::bytea), idempotency_key_digest IS NULL,
        rerun_of_task_id IS NULL AND rerun_root_task_id IS NULL, claimed_at IS NULL AND claim_expires_at IS NULL)
     FROM horsies_tasks WHERE id = '00000000-0000-7000-8000-000000000b01'),
    'PENDING|lc.task|q_lc|100|{"x":1}|ISOLATED|forever|t|3|0|t|t|f|1|t|t|t|t'
);
-- The script is one message after its BEGIN, so statement_timestamp() is one instant later than NOW().
SELECT lc_expect(
    'enqueue: created_at and updated_at are the time of the send statement, not the start of the transaction',
    (SELECT concat_ws('|', created_at = statement_timestamp(), updated_at = statement_timestamp(),
        statement_timestamp() > now())
     FROM horsies_tasks WHERE id = '00000000-0000-7000-8000-000000000b01'),
    't|t|t'
);
SELECT lc_expect(
    'enqueue: a resend with the same command and sent_at is the same command',
    lc_send('00000000-0000-7000-8000-000000000b01', 'b01', now()), 'SAME_COMMAND'
);
SELECT lc_expect(
    'enqueue: a resend with another sent_at is a payload mismatch',
    lc_send('00000000-0000-7000-8000-000000000b01', 'b01', now() - interval '1 second'), 'PAYLOAD_MISMATCH'
);
SELECT lc_expect(
    'enqueue: a resend with another command is a payload mismatch',
    lc_send('00000000-0000-7000-8000-000000000b01', 'other', now()), 'PAYLOAD_MISMATCH'
);
SELECT lc_expect(
    'enqueue: a resend writes no second row',
    (SELECT count(*)::text FROM horsies_tasks WHERE id = '00000000-0000-7000-8000-000000000b01'), '1'
);
SELECT lc_expect(
    'enqueue: start AT stores the given time',
    (SELECT outcome FROM horsies_enqueue_task(
        '00000000-0000-7000-8000-000000000b02', 'lc.task', 'q_lc', 100, '{"x":1}', NULL, 'ISOLATED', 'forever',
        TRUE, 3, NULL, now(), 'AT', now() + interval '1 hour', NULL, 1::smallint, sha256('b02'::bytea),
        'FIRST_RUN', NULL, NULL, 'UNKEYED', NULL, NULL, NULL, 'FIRST', 'NO_CARRIER', NULL, NULL)),
    'INSERTED'
);
SELECT lc_expect(
    'enqueue: start AT, the stored time',
    (SELECT (enqueued_at - now())::text FROM horsies_tasks WHERE id = '00000000-0000-7000-8000-000000000b02'),
    '01:00:00'
);
SELECT lc_expect(
    'enqueue: start AFTER_DELAY stores now plus the delay',
    (SELECT outcome FROM horsies_enqueue_task(
        '00000000-0000-7000-8000-000000000b03', 'lc.task', 'q_lc', 100, '{"x":1}', NULL, 'ISOLATED', 'forever',
        TRUE, 3, NULL, now(), 'AFTER_DELAY', NULL, 30, 1::smallint, sha256('b03'::bytea),
        'FIRST_RUN', NULL, NULL, 'UNKEYED', NULL, NULL, NULL, 'FIRST', 'NO_CARRIER', NULL, NULL)),
    'INSERTED'
);
SELECT lc_expect(
    'enqueue: start AFTER_DELAY, the stored time',
    (SELECT (enqueued_at - statement_timestamp())::text
     FROM horsies_tasks WHERE id = '00000000-0000-7000-8000-000000000b03'),
    '00:00:30'
);
SELECT lc_expect(
    'enqueue: a rerun stores its lineage pair',
    (SELECT outcome FROM horsies_enqueue_task(
        '00000000-0000-7000-8000-000000000b04', 'lc.task', 'q_lc', 100, '{"x":1}', NULL, 'ISOLATED', 'forever',
        TRUE, 3, NULL, now(), 'NOW', NULL, NULL, 1::smallint, sha256('b04'::bytea),
        'RERUN', '00000000-0000-7000-8000-000000000bf1', '00000000-0000-7000-8000-000000000bf0',
        'UNKEYED', NULL, NULL, NULL, 'FIRST', 'NO_CARRIER', NULL, NULL)),
    'INSERTED'
);
SELECT lc_expect(
    'enqueue: a rerun, the stored lineage pair',
    (SELECT rerun_of_task_id || '>' || rerun_root_task_id FROM horsies_tasks
     WHERE id = '00000000-0000-7000-8000-000000000b04'),
    '00000000-0000-7000-8000-000000000bf1>00000000-0000-7000-8000-000000000bf0'
);

-- The trace pair.
SELECT lc_expect(
    'enqueue: a send with no carrier stores no pair',
    (SELECT (trace_carrier IS NULL AND trace_id IS NULL)::text FROM horsies_tasks
     WHERE id = '00000000-0000-7000-8000-000000000b01'),
    'true'
);
SELECT lc_expect(
    'enqueue: a send with a carrier inserts',
    lc_send_traced('00000000-0000-7000-8000-000000000b31', 'b31', 'CARRIER',
                   '{"traceparent":"00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"}',
                   decode('0af7651916cd43dd8448eb211c80319c', 'hex')),
    'INSERTED'
);
SELECT lc_expect(
    'enqueue: the row stores the carrier text and the trace id as sent',
    (SELECT trace_carrier || '|' || encode(trace_id, 'hex') FROM horsies_tasks
     WHERE id = '00000000-0000-7000-8000-000000000b31'),
    '{"traceparent":"00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"}|0af7651916cd43dd8448eb211c80319c'
);
SELECT lc_expect(
    'enqueue: a resend with another carrier is the same command',
    lc_send_traced('00000000-0000-7000-8000-000000000b31', 'b31', 'CARRIER', '{"b3":"other"}',
                   decode('4bf92f3577b34da6a3ce929d0e0e4736', 'hex')),
    'SAME_COMMAND'
);
SELECT lc_expect(
    'enqueue: a resend keeps the pair of the stored row',
    (SELECT encode(trace_id, 'hex') FROM horsies_tasks WHERE id = '00000000-0000-7000-8000-000000000b31'),
    '0af7651916cd43dd8448eb211c80319c'
);
SELECT lc_expect(
    'enqueue: a cancel of a traced task applies',
    (SELECT outcome FROM horsies_cancel_task('00000000-0000-7000-8000-000000000b31', FALSE)),
    'APPLIED'
);
SELECT lc_expect(
    'enqueue: the history row keeps the trace id of the live row',
    (SELECT encode(trace_id, 'hex') FROM horsies_task_history WHERE task_id = '00000000-0000-7000-8000-000000000b31'),
    '0af7651916cd43dd8448eb211c80319c'
);
SELECT lc_expect_sqlstate('enqueue: a NULL trace kind', $q$SELECT lc_send_traced(
    gen_random_uuid(), 'x', NULL, NULL, NULL)$q$, '22023');
SELECT lc_expect_sqlstate('enqueue: an unknown trace kind', $q$SELECT lc_send_traced(
    gen_random_uuid(), 'x', 'MAYBE', NULL, NULL)$q$, '22023');
SELECT lc_expect_sqlstate('enqueue: a carrier text with NO_CARRIER', $q$SELECT lc_send_traced(
    gen_random_uuid(), 'x', 'NO_CARRIER', '{}', NULL)$q$, '22023');
SELECT lc_expect_sqlstate('enqueue: CARRIER with no trace id', $q$SELECT lc_send_traced(
    gen_random_uuid(), 'x', 'CARRIER', '{}', NULL)$q$, '22023');
SELECT lc_expect_sqlstate('enqueue: CARRIER with no text', $q$SELECT lc_send_traced(
    gen_random_uuid(), 'x', 'CARRIER', NULL, decode('0af7651916cd43dd8448eb211c80319c', 'hex'))$q$, '22023');
SELECT lc_expect_sqlstate('enqueue: a trace id of 15 bytes', $q$SELECT lc_send_traced(
    gen_random_uuid(), 'x', 'CARRIER', '{}', decode('0af7651916cd43dd8448eb211c8031', 'hex'))$q$, '22023');

-- Keyed sends.
SELECT lc_expect(
    'enqueue: a keyed first send inserts',
    lc_send_keyed('00000000-0000-7000-8000-000000000b11', 'b11', 'key-1', now()),
    'INSERTED:00000000-0000-7000-8000-000000000b11'
);
SELECT lc_expect(
    'enqueue: a keyed send binds its digest and holds a LIVE reservation',
    (SELECT (t.idempotency_key_digest = sha256('key-1'::bytea))::text || ':' || r.disposition || ':' || r.task_id
     FROM horsies_tasks t JOIN horsies_key_reservations r ON r.idempotency_key_digest = t.idempotency_key_digest
     WHERE t.id = '00000000-0000-7000-8000-000000000b11'),
    'true:LIVE:00000000-0000-7000-8000-000000000b11'
);
SELECT lc_expect(
    'enqueue: the same keyed command at another time replays the first task',
    lc_send_keyed('00000000-0000-7000-8000-000000000b12', 'b11', 'key-1', now() + interval '5 seconds'),
    'KEY_REPLAY:00000000-0000-7000-8000-000000000b11'
);
SELECT lc_expect(
    'enqueue: another command on a held key is a key conflict naming the owner',
    lc_send_keyed('00000000-0000-7000-8000-000000000b13', 'other', 'key-1', now()),
    'KEY_CONFLICT:00000000-0000-7000-8000-000000000b11'
);
SELECT lc_expect(
    'enqueue: a replay and a conflict write no row',
    (SELECT count(*)::text FROM horsies_tasks
     WHERE id IN ('00000000-0000-7000-8000-000000000b12', '00000000-0000-7000-8000-000000000b13')),
    '0'
);
-- A keyed send on a live unbound row of the same command binds the key to it.
SELECT lc_send('00000000-0000-7000-8000-000000000b14', 'b14', now());
SELECT lc_expect(
    'enqueue: a keyed send on an unbound live row of the same command',
    lc_send_keyed('00000000-0000-7000-8000-000000000b14', 'b14', 'key-14', now()),
    'SAME_COMMAND:00000000-0000-7000-8000-000000000b14'
);
SELECT lc_expect(
    'enqueue: the bound row and its reservation',
    (SELECT (t.idempotency_key_digest = sha256('key-14'::bytea))::text || ':' || r.disposition
     FROM horsies_tasks t JOIN horsies_key_reservations r ON r.idempotency_key_digest = sha256('key-14'::bytea)
     WHERE t.id = '00000000-0000-7000-8000-000000000b14'),
    'true:LIVE'
);
SELECT lc_expect(
    'enqueue: a keyed send on a live row bound to another key is a payload mismatch',
    lc_send_keyed('00000000-0000-7000-8000-000000000b14', 'b14', 'key-14b', now()),
    'PAYLOAD_MISMATCH:00000000-0000-7000-8000-000000000b14'
);
SELECT lc_expect(
    'enqueue: a keyed send whose id holds another command is a payload mismatch',
    lc_send_keyed('00000000-0000-7000-8000-000000000b01', 'b01-changed', 'key-b01', now()),
    'PAYLOAD_MISMATCH:00000000-0000-7000-8000-000000000b01'
);
SELECT lc_expect(
    'enqueue: a refused keyed send leaves no reservation',
    (SELECT count(*)::text FROM horsies_key_reservations
     WHERE idempotency_key_digest IN (sha256('key-14b'::bytea), sha256('key-b01'::bytea))),
    '0'
);
-- The window is stored exact to the microsecond, also under one second.
SELECT lc_expect(
    'enqueue: a keyed send with a window of 1.5 s',
    lc_send_keyed_window('00000000-0000-7000-8000-000000000b15', 'key-1500ms', interval '1.5 seconds'),
    'INSERTED:00000000-0000-7000-8000-000000000b15'
);
SELECT lc_expect(
    'enqueue: a window of 1.5 s is stored as 1.5 s',
    (SELECT (r.reservation_window = interval '1.5 seconds')::text FROM horsies_key_reservations r
     WHERE r.idempotency_key_digest = sha256('key-1500ms'::bytea)),
    'true'
);
SELECT lc_expect(
    'enqueue: a keyed send with a window of 0.5 s is admitted',
    lc_send_keyed_window('00000000-0000-7000-8000-000000000b16', 'key-500ms', interval '0.5 seconds'),
    'INSERTED:00000000-0000-7000-8000-000000000b16'
);
SELECT lc_expect(
    'enqueue: a window of 0.5 s is stored as 0.5 s, and its reservation turns TERMINAL',
    (SELECT (r.reservation_window = interval '0.5 seconds')::text FROM horsies_key_reservations r
     WHERE r.idempotency_key_digest = sha256('key-500ms'::bytea))
    || ':' || horsies_key_reservation_terminalize(
        sha256('key-500ms'::bytea), '00000000-0000-7000-8000-000000000b16', now())::text,
    'true:true'
);
SELECT lc_expect(
    'enqueue: the TERMINAL reservation of a 0.5 s window expires 0.5 s after the terminal time',
    (SELECT (r.expires_at - now() = interval '0.5 seconds')::text FROM horsies_key_reservations r
     WHERE r.idempotency_key_digest = sha256('key-500ms'::bytea)),
    'true'
);

-- A repeat of a send whose task already moved to history.
SELECT lc_task('00000000-0000-7000-8000-000000000b21', 'PENDING');
UPDATE horsies_tasks SET command_fingerprint = sha256('b21'::bytea) WHERE id = '00000000-0000-7000-8000-000000000b21';
SELECT horsies_move_task_to_history(
    (SELECT t FROM horsies_tasks AS t WHERE t.id = '00000000-0000-7000-8000-000000000b21' FOR UPDATE),
    'CANCELLED', 'CANCEL_ADMIN', now(), NULL, 'TASK_CANCELLED', 'r');
SELECT lc_expect(
    'enqueue: a repeat of a finished task with the same command',
    lc_send('00000000-0000-7000-8000-000000000b21', 'b21', now(), 'REPEAT'), 'SAME_COMMAND'
);
SELECT lc_expect(
    'enqueue: a repeat of a finished task with another command',
    lc_send('00000000-0000-7000-8000-000000000b21', 'other', now(), 'REPEAT'), 'PAYLOAD_MISMATCH'
);
SELECT lc_expect(
    'enqueue: a repeat of a finished task with another sent_at',
    lc_send('00000000-0000-7000-8000-000000000b21', 'b21', now() - interval '1 second', 'REPEAT'), 'PAYLOAD_MISMATCH'
);
SELECT lc_expect(
    'enqueue: a repeat of a finished task never runs it again',
    (SELECT count(*)::text FROM horsies_tasks WHERE id = '00000000-0000-7000-8000-000000000b21'), '0'
);
SELECT lc_expect(
    'enqueue: a repeat of a send that never arrived inserts',
    lc_send('00000000-0000-7000-8000-000000000b22', 'b22', now(), 'REPEAT'), 'INSERTED'
);
SELECT lc_expect(
    'enqueue: a repeat of a live task with the same command',
    lc_send('00000000-0000-7000-8000-000000000b22', 'b22', now(), 'REPEAT'), 'SAME_COMMAND'
);

-- Argument refusals: 22023 before any write.
SELECT lc_expect_sqlstate('enqueue: unknown start kind', $q$SELECT * FROM horsies_enqueue_task(
    gen_random_uuid(), 'lc.task', 'q_lc', 100, '{}', NULL, 'ISOLATED', 'forever', TRUE, 3, NULL, now(),
    'LATER', NULL, NULL, 1::smallint, sha256('x'::bytea), 'FIRST_RUN', NULL, NULL, 'UNKEYED', NULL, NULL, NULL,
    'FIRST', 'NO_CARRIER', NULL, NULL)$q$, '22023');
SELECT lc_expect_sqlstate('enqueue: start AT without a time', $q$SELECT * FROM horsies_enqueue_task(
    gen_random_uuid(), 'lc.task', 'q_lc', 100, '{}', NULL, 'ISOLATED', 'forever', TRUE, 3, NULL, now(),
    'AT', NULL, NULL, 1::smallint, sha256('x'::bytea), 'FIRST_RUN', NULL, NULL, 'UNKEYED', NULL, NULL, NULL,
    'FIRST', 'NO_CARRIER', NULL, NULL)$q$, '22023');
SELECT lc_expect_sqlstate('enqueue: start NOW with a delay', $q$SELECT * FROM horsies_enqueue_task(
    gen_random_uuid(), 'lc.task', 'q_lc', 100, '{}', NULL, 'ISOLATED', 'forever', TRUE, 3, NULL, now(),
    'NOW', NULL, 5, 1::smallint, sha256('x'::bytea), 'FIRST_RUN', NULL, NULL, 'UNKEYED', NULL, NULL, NULL,
    'FIRST', 'NO_CARRIER', NULL, NULL)$q$, '22023');
SELECT lc_expect_sqlstate('enqueue: a negative delay', $q$SELECT * FROM horsies_enqueue_task(
    gen_random_uuid(), 'lc.task', 'q_lc', 100, '{}', NULL, 'ISOLATED', 'forever', TRUE, 3, NULL, now(),
    'AFTER_DELAY', NULL, -1, 1::smallint, sha256('x'::bytea), 'FIRST_RUN', NULL, NULL, 'UNKEYED', NULL, NULL,
    NULL, 'FIRST', 'NO_CARRIER', NULL, NULL)$q$, '22023');
SELECT lc_expect_sqlstate('enqueue: a rerun without its root', $q$SELECT * FROM horsies_enqueue_task(
    gen_random_uuid(), 'lc.task', 'q_lc', 100, '{}', NULL, 'ISOLATED', 'forever', TRUE, 3, NULL, now(),
    'NOW', NULL, NULL, 1::smallint, sha256('x'::bytea), 'RERUN', gen_random_uuid(), NULL, 'UNKEYED', NULL, NULL,
    NULL, 'FIRST', 'NO_CARRIER', NULL, NULL)$q$, '22023');
SELECT lc_expect_sqlstate('enqueue: a first run with lineage', $q$SELECT * FROM horsies_enqueue_task(
    gen_random_uuid(), 'lc.task', 'q_lc', 100, '{}', NULL, 'ISOLATED', 'forever', TRUE, 3, NULL, now(),
    'NOW', NULL, NULL, 1::smallint, sha256('x'::bytea), 'FIRST_RUN', gen_random_uuid(), gen_random_uuid(),
    'UNKEYED', NULL, NULL, NULL, 'FIRST', 'NO_CARRIER', NULL, NULL)$q$, '22023');
SELECT lc_expect_sqlstate('enqueue: a keyed send without its window', $q$SELECT * FROM horsies_enqueue_task(
    gen_random_uuid(), 'lc.task', 'q_lc', 100, '{}', NULL, 'ISOLATED', 'forever', TRUE, 3, NULL, now(),
    'NOW', NULL, NULL, 1::smallint, sha256('x'::bytea), 'FIRST_RUN', NULL, NULL, 'KEYED', sha256('k'::bytea),
    1::smallint, NULL, 'FIRST', 'NO_CARRIER', NULL, NULL)$q$, '22023');
SELECT lc_expect_sqlstate('enqueue: an unkeyed send with a digest', $q$SELECT * FROM horsies_enqueue_task(
    gen_random_uuid(), 'lc.task', 'q_lc', 100, '{}', NULL, 'ISOLATED', 'forever', TRUE, 3, NULL, now(),
    'NOW', NULL, NULL, 1::smallint, sha256('x'::bytea), 'FIRST_RUN', NULL, NULL, 'UNKEYED', sha256('k'::bytea),
    NULL, NULL, 'FIRST', 'NO_CARRIER', NULL, NULL)$q$, '22023');
SELECT lc_expect_sqlstate('enqueue: unknown key kind', $q$SELECT * FROM horsies_enqueue_task(
    gen_random_uuid(), 'lc.task', 'q_lc', 100, '{}', NULL, 'ISOLATED', 'forever', TRUE, 3, NULL, now(),
    'NOW', NULL, NULL, 1::smallint, sha256('x'::bytea), 'FIRST_RUN', NULL, NULL, 'MAYBE', NULL, NULL, NULL,
    'FIRST', 'NO_CARRIER', NULL, NULL)$q$, '22023');
SELECT lc_expect_sqlstate('enqueue: unknown send kind', $q$SELECT * FROM horsies_enqueue_task(
    gen_random_uuid(), 'lc.task', 'q_lc', 100, '{}', NULL, 'ISOLATED', 'forever', TRUE, 3, NULL, now(),
    'NOW', NULL, NULL, 1::smallint, sha256('x'::bytea), 'FIRST_RUN', NULL, NULL, 'UNKEYED', NULL, NULL, NULL,
    'AGAIN', 'NO_CARRIER', NULL, NULL)$q$, '22023');
SELECT lc_expect_sqlstate('enqueue: a fingerprint of 31 bytes', $q$SELECT * FROM horsies_enqueue_task(
    gen_random_uuid(), 'lc.task', 'q_lc', 100, '{}', NULL, 'ISOLATED', 'forever', TRUE, 3, NULL, now(),
    'NOW', NULL, NULL, 1::smallint, substring(sha256('x'::bytea) FROM 2), 'FIRST_RUN', NULL, NULL, 'UNKEYED',
    NULL, NULL, NULL, 'FIRST', 'NO_CARRIER', NULL, NULL)$q$, '22023');
SELECT lc_expect_sqlstate('enqueue: a NULL input', $q$SELECT * FROM horsies_enqueue_task(
    gen_random_uuid(), 'lc.task', 'q_lc', 100, NULL, NULL, 'ISOLATED', 'forever', TRUE, 3, NULL, now(),
    'NOW', NULL, NULL, 1::smallint, sha256('x'::bytea), 'FIRST_RUN', NULL, NULL, 'UNKEYED', NULL, NULL, NULL,
    'FIRST', 'NO_CARRIER', NULL, NULL)$q$, '22023');
SELECT lc_expect_sqlstate('enqueue: a window over 30 days (key claim)', $q$SELECT * FROM horsies_enqueue_task(
    gen_random_uuid(), 'lc.task', 'q_lc', 100, '{}', NULL, 'ISOLATED', 'forever', TRUE, 3, NULL, now(),
    'NOW', NULL, NULL, 1::smallint, sha256('x'::bytea), 'FIRST_RUN', NULL, NULL, 'KEYED', sha256('k31'::bytea),
    1::smallint, interval '720:00:00.000001', 'FIRST', 'NO_CARRIER', NULL, NULL)$q$, '22023');
SELECT lc_expect_sqlstate('enqueue: a window with a day field (key claim)', $q$SELECT * FROM horsies_enqueue_task(
    gen_random_uuid(), 'lc.task', 'q_lc', 100, '{}', NULL, 'ISOLATED', 'forever', TRUE, 3, NULL, now(),
    'NOW', NULL, NULL, 1::smallint, sha256('x'::bytea), 'FIRST_RUN', NULL, NULL, 'KEYED', sha256('kday'::bytea),
    1::smallint, interval '1 day', 'FIRST', 'NO_CARRIER', NULL, NULL)$q$, '22023');
-- A column shape that the table refuses: the queue name over 52 bytes (CHECK, 23514).
SELECT lc_expect_sqlstate('enqueue: a queue name over 52 bytes', $q$SELECT * FROM horsies_enqueue_task(
    gen_random_uuid(), 'lc.task', repeat('q', 53), 100, '{}', NULL, 'ISOLATED', 'forever', TRUE, 3, NULL, now(),
    'NOW', NULL, NULL, 1::smallint, sha256('x'::bytea), 'FIRST_RUN', NULL, NULL, 'UNKEYED', NULL, NULL, NULL,
    'FIRST', 'NO_CARRIER', NULL, NULL)$q$, '23514');
-- An error message carries no argument value.
DO $$
BEGIN
    PERFORM * FROM horsies_enqueue_task(
        gen_random_uuid(), 'lc.task', 'q_lc', 100, '{"secret":"s3cr3t"}', NULL, 'ISOLATED', 'forever', TRUE, 3,
        NULL, now(), 'AT', NULL, NULL, 1::smallint, sha256('x'::bytea), 'FIRST_RUN', NULL, NULL, 'UNKEYED',
        NULL, NULL, NULL, 'FIRST', 'NO_CARRIER', NULL, NULL);
    RAISE EXCEPTION 'case enqueue: error text: the refused call succeeded';
EXCEPTION WHEN invalid_parameter_value THEN
    IF SQLERRM LIKE '%s3cr3t%' THEN
        RAISE EXCEPTION 'case enqueue: error text: the message holds the input';
    END IF;
END
$$;

------------------------------------------------------------------------------------------------------------------------
-- horsies_claim, horsies_claim_in_flight, horsies_claim_candidates
------------------------------------------------------------------------------------------------------------------------

-- The queue q_claim holds: two due rows (priority 10 and 50), one not yet due, one past its deadline, one waiting
-- for its retry time, and a CLAIMED row of worker w9 whose lease expired.
SELECT lc_task('00000000-0000-7000-8000-000000000c01', 'PENDING');
SELECT lc_task('00000000-0000-7000-8000-000000000c02', 'PENDING');
SELECT lc_task('00000000-0000-7000-8000-000000000c03', 'PENDING');
SELECT lc_task('00000000-0000-7000-8000-000000000c04', 'PENDING');
SELECT lc_task('00000000-0000-7000-8000-000000000c05', 'PENDING');
SELECT lc_task('00000000-0000-7000-8000-000000000c06', 'CLAIMED', 'w9');
UPDATE horsies_tasks SET queue_name = 'q_claim', retry_count = 1, max_retries = 4
WHERE id BETWEEN '00000000-0000-7000-8000-000000000c01' AND '00000000-0000-7000-8000-000000000c06';
UPDATE horsies_tasks SET priority = 50 WHERE id = '00000000-0000-7000-8000-000000000c01';
UPDATE horsies_tasks SET priority = 10 WHERE id = '00000000-0000-7000-8000-000000000c02';
UPDATE horsies_tasks SET enqueued_at = now() + interval '1 hour' WHERE id = '00000000-0000-7000-8000-000000000c03';
UPDATE horsies_tasks SET good_until = now() - interval '1 second' WHERE id = '00000000-0000-7000-8000-000000000c04';
UPDATE horsies_tasks SET next_retry_at = now() + interval '1 hour' WHERE id = '00000000-0000-7000-8000-000000000c05';
UPDATE horsies_tasks SET claim_expires_at = now() - interval '1 second', priority = 90
WHERE id = '00000000-0000-7000-8000-000000000c06';
UPDATE horsies_tasks SET trace_carrier = '{"traceparent":"c02"}', trace_id = decode(repeat('c2', 16), 'hex')
WHERE id = '00000000-0000-7000-8000-000000000c02';

SELECT lc_expect(
    'claim in-flight: an expired claim does not count',
    (SELECT concat_ws('|', my_claimed, my_in_flight, global_in_flight)
     FROM horsies_claim_in_flight('w9', ARRAY['q_claim'], TRUE, '{"ISOLATED": "ISOLATED"}'::jsonb)),
    '0|0|0'
);
SELECT lc_expect_sqlstate('claim: a lease of 0 ms', $q$SELECT * FROM horsies_claim(
    'w1', '["q_claim"]'::jsonb, '{}'::jsonb, '{}'::jsonb, FALSE, '{"ISOLATED": 5}'::jsonb,
    '{"ISOLATED": "ISOLATED"}'::jsonb, 0, 5, 5, NULL, 0, '[]'::jsonb)$q$, '22023');
SELECT lc_expect_sqlstate('claim: a NULL lease', $q$SELECT * FROM horsies_claim(
    'w1', '["q_claim"]'::jsonb, '{}'::jsonb, '{}'::jsonb, FALSE, '{"ISOLATED": 5}'::jsonb,
    '{"ISOLATED": "ISOLATED"}'::jsonb, 0, 5, 5, NULL, NULL, '[]'::jsonb)$q$, '22023');
SELECT lc_expect_sqlstate('claim: a route to a budget that does not exist', $q$SELECT * FROM horsies_claim(
    'w1', '["q_claim"]'::jsonb, '{}'::jsonb, '{}'::jsonb, FALSE, '{"ISOLATED": 5}'::jsonb,
    '{"ISOLATED": "SHARED"}'::jsonb, 0, 5, 5, NULL, 60000, '[]'::jsonb)$q$, '22023');

CREATE TEMP TABLE lc_claimed ON COMMIT DROP AS
SELECT * FROM horsies_claim(
    'w1', '["q_claim"]'::jsonb, '{}'::jsonb, '{}'::jsonb, FALSE, '{"ISOLATED": 5}'::jsonb,
    '{"ISOLATED": "ISOLATED"}'::jsonb, 0, 5, 5, NULL, 45000, '[]'::jsonb
);
SELECT lc_expect(
    'claim: due rows and the expired claim, in priority order, with the retry facts',
    (SELECT string_agg(concat_ws('|', row_kind, right(id::text, 3), input, execution, is_workflow_task,
        claimed_at = now(), retry_count, max_retries), ',') FROM lc_claimed),
    'CLAIMED|c02|{"x":1}|ISOLATED|f|t|1|4,CLAIMED|c01|{"x":1}|ISOLATED|f|t|1|4,CLAIMED|c06|{"x":1}|ISOLATED|f|t|1|4'
);
SELECT lc_expect(
    'claim: each CLAIMED row returns its priority, its due time and its trace pair',
    (SELECT string_agg(concat_ws('|', right(id::text, 3), priority, enqueued_at = now() - interval '1 minute',
        coalesce(trace_carrier, 'none'), coalesce(encode(trace_id, 'hex'), 'none')), ',') FROM lc_claimed),
    'c02|10|t|{"traceparent":"c02"}|' || repeat('c2', 16) || ',c01|50|t|none|none,c06|90|t|none|none'
);
SELECT lc_expect(
    'claim: the result columns have built-in types',
    (SELECT pg_typeof(id)::text || ',' || pg_typeof(claimed_at)::text FROM lc_claimed LIMIT 1),
    'uuid,timestamp with time zone'
);
SELECT lc_expect(
    'claim: each taken row is CLAIMED by the worker with a lease from now',
    (SELECT string_agg(concat_ws('|', right(id::text, 3), status, claimed_by_worker_id,
        claim_expires_at - now(), finalizing_at IS NULL), ',' ORDER BY id)
     FROM horsies_tasks WHERE id IN (SELECT id FROM lc_claimed)),
    'c01|CLAIMED|w1|00:00:45|t,c02|CLAIMED|w1|00:00:45|t,c06|CLAIMED|w1|00:00:45|t'
);
SELECT lc_expect(
    'claim: rows not due stay PENDING',
    (SELECT string_agg(right(id::text, 3) || ':' || status, ',' ORDER BY id) FROM horsies_tasks
     WHERE id IN ('00000000-0000-7000-8000-000000000c03', '00000000-0000-7000-8000-000000000c04',
                  '00000000-0000-7000-8000-000000000c05')),
    'c03:PENDING,c04:PENDING,c05:PENDING'
);
SELECT lc_expect(
    'claim in-flight: live claims count for their worker',
    (SELECT concat_ws('|', my_claimed, my_in_flight, global_in_flight, queue_counts::text)
     FROM horsies_claim_in_flight('w1', ARRAY['q_claim'], TRUE, '{"ISOLATED": "ISOLATED"}'::jsonb)),
    '3|3|3|{"q_claim": 3}'
);
SELECT lc_expect(
    'claim: a worker at its per-worker limit takes nothing',
    (SELECT count(*)::text FROM horsies_claim(
        'w1', '["q_claim"]'::jsonb, '{}'::jsonb, '{}'::jsonb, FALSE, '{"ISOLATED": 5}'::jsonb,
        '{"ISOLATED": "ISOLATED"}'::jsonb, 0, 3, 5, NULL, 45000, '[]'::jsonb)),
    '0'
);

-- One budget per class (#711): the queue q_budget holds three due rows of each class. The worker's budgets take one
-- ISOLATED and two SHARED_ASYNC rows; it has no route for SHARED_THREAD, so those rows stay.
SELECT lc_task(('00000000-0000-7000-8000-00000000c1' || n)::uuid, 'PENDING') FROM generate_series(10, 18) n;
UPDATE horsies_tasks
SET queue_name = 'q_budget',
    execution = (ARRAY['ISOLATED', 'SHARED_ASYNC', 'SHARED_THREAD'])[1 + (right(id::text, 2)::int - 10) % 3]
WHERE id BETWEEN '00000000-0000-7000-8000-00000000c110' AND '00000000-0000-7000-8000-00000000c118';
CREATE TEMP TABLE lc_budget_claimed ON COMMIT DROP AS
SELECT * FROM horsies_claim(
    'wb', '["q_budget"]'::jsonb, '{}'::jsonb, '{}'::jsonb, FALSE, '{"ISOLATED": 1, "SHARED_ASYNC": 2}'::jsonb,
    '{"ISOLATED": "ISOLATED", "SHARED_ASYNC": "SHARED_ASYNC"}'::jsonb, 0, 10, 10, NULL, 45000, '[]'::jsonb
);
SELECT lc_expect(
    'claim (#711): each class takes its own budget, an unrouted class stays',
    (SELECT string_agg(execution || ':' || n, ',' ORDER BY execution) FROM (
        SELECT execution, count(*) AS n FROM lc_budget_claimed WHERE row_kind = 'CLAIMED' GROUP BY execution) c),
    'ISOLATED:1,SHARED_ASYNC:2'
);
SELECT lc_expect(
    'claim (#711): the budget counts of the worker after the pass',
    (SELECT budget_counts::text FROM horsies_claim_in_flight(
        'wb', ARRAY[]::text[], FALSE, '{"ISOLATED": "ISOLATED", "SHARED_ASYNC": "SHARED_ASYNC"}'::jsonb)),
    '{"ISOLATED": 1, "SHARED_ASYNC": 2}'
);
SELECT lc_expect(
    'claim (#711): a full budget takes nothing more of its class',
    (SELECT count(*)::text FROM horsies_claim(
        'wb', '["q_budget"]'::jsonb, '{}'::jsonb, '{}'::jsonb, FALSE, '{"ISOLATED": 1, "SHARED_ASYNC": 2}'::jsonb,
        '{"ISOLATED": "ISOLATED", "SHARED_ASYNC": "SHARED_ASYNC"}'::jsonb, 0, 10, 10, NULL, 45000, '[]'::jsonb)),
    '0'
);

------------------------------------------------------------------------------------------------------------------------
-- horsies_start_attempt
------------------------------------------------------------------------------------------------------------------------

SELECT lc_task('00000000-0000-7000-8000-000000000d01', 'CLAIMED');
SELECT lc_expect(
    'start: a held claim starts; a plain task has no workflow place',
    (SELECT concat_ws('|', outcome, miss_reason, started_at = now(),
        workflow_id IS NULL AND workflow_task_index IS NULL AND root_workflow_id IS NULL) FROM horsies_start_attempt(
        '00000000-0000-7000-8000-000000000d01', 'w1', now() - interval '1 second', 42, 'host-b', 'worker-42')),
    'STARTED|t|t'
);
SELECT lc_expect(
    'start: the row is RUNNING with its process facts and no lease',
    (SELECT concat_ws('|', status, claim_expires_at IS NULL, started_at = now(), worker_pid, worker_hostname,
        worker_process_name, claimed_by_worker_id)
     FROM horsies_tasks WHERE id = '00000000-0000-7000-8000-000000000d01'),
    'RUNNING|t|t|42|host-b|worker-42|w1'
);
SELECT lc_expect(
    'start: the start writes no heartbeat',
    (SELECT count(*)::text FROM horsies_heartbeats WHERE task_id = '00000000-0000-7000-8000-000000000d01'),
    '0'
);
SELECT lc_expect(
    'start: a started row does not start again',
    (SELECT concat_ws('|', outcome, miss_reason) FROM horsies_start_attempt(
        '00000000-0000-7000-8000-000000000d01', 'w1', now() - interval '1 second', 42, 'h', 'p')),
    'MISSED|NOT_HELD'
);
SELECT lc_task('00000000-0000-7000-8000-000000000d02', 'CLAIMED');
SELECT lc_expect(
    'start: another claim generation is not held',
    (SELECT concat_ws('|', outcome, miss_reason) FROM horsies_start_attempt(
        '00000000-0000-7000-8000-000000000d02', 'w1', now() - interval '2 seconds', 42, 'h', 'p')),
    'MISSED|NOT_HELD'
);
SELECT lc_expect(
    'start: another worker does not hold it',
    (SELECT concat_ws('|', outcome, miss_reason) FROM horsies_start_attempt(
        '00000000-0000-7000-8000-000000000d02', 'w2', now() - interval '1 second', 42, 'h', 'p')),
    'MISSED|NOT_HELD'
);
SELECT lc_expect(
    'start: an absent task is not held, with no start time and no workflow place',
    (SELECT concat_ws('|', outcome, miss_reason,
        started_at IS NULL AND workflow_id IS NULL AND workflow_task_index IS NULL AND root_workflow_id IS NULL)
     FROM horsies_start_attempt(
        '00000000-0000-7000-8000-000000000dff', 'w1', now(), 42, 'h', 'p')),
    'MISSED|NOT_HELD|t'
);
SELECT lc_expect(
    'start: a miss writes nothing',
    (SELECT status || ':' || (SELECT count(*) FROM horsies_heartbeats WHERE task_id = t.id)
     FROM horsies_tasks t WHERE t.id = '00000000-0000-7000-8000-000000000d02'),
    'CLAIMED:0'
);
UPDATE horsies_tasks SET claim_expires_at = now() - interval '1 millisecond'
WHERE id = '00000000-0000-7000-8000-000000000d02';
SELECT lc_expect(
    'start: an expired lease',
    (SELECT concat_ws('|', outcome, miss_reason) FROM horsies_start_attempt(
        '00000000-0000-7000-8000-000000000d02', 'w1', now() - interval '1 second', 42, 'h', 'p')),
    'MISSED|LEASE_EXPIRED'
);
UPDATE horsies_tasks SET good_until = now() WHERE id = '00000000-0000-7000-8000-000000000d02';
SELECT lc_expect(
    'start: a passed deadline comes first, at any claim generation of this worker',
    (SELECT concat_ws('|', outcome, miss_reason) FROM horsies_start_attempt(
        '00000000-0000-7000-8000-000000000d02', 'w1', now() - interval '9 seconds', 42, 'h', 'p')),
    'MISSED|DEADLINE_PASSED'
);
SELECT lc_expect(
    'start: a passed deadline of another worker is not held',
    (SELECT concat_ws('|', outcome, miss_reason) FROM horsies_start_attempt(
        '00000000-0000-7000-8000-000000000d02', 'w2', now() - interval '1 second', 42, 'h', 'p')),
    'MISSED|NOT_HELD'
);
SELECT lc_expect_sqlstate('start: a NULL claim generation', $q$SELECT * FROM horsies_start_attempt(
    '00000000-0000-7000-8000-000000000d02', 'w1', NULL, 42, 'h', 'p')$q$, '22023');

-- Workflow tasks.
SELECT lc_workflow('00000000-0000-7000-8000-000000000e01', 'RUNNING');
SELECT lc_task('00000000-0000-7000-8000-000000000d11', 'CLAIMED', 'w1', TRUE);
SELECT lc_node('00000000-0000-7000-8000-000000000f11', '00000000-0000-7000-8000-000000000e01', 0, 'ENQUEUED',
               '00000000-0000-7000-8000-000000000d11');
SELECT lc_expect(
    'start: a workflow task starts with its node, and returns the workflow id, the node index and the root',
    (SELECT concat_ws('|', outcome, workflow_id, workflow_task_index, root_workflow_id) FROM horsies_start_attempt(
        '00000000-0000-7000-8000-000000000d11', 'w1', now() - interval '1 second', 42, 'h', 'p')),
    'STARTED|00000000-0000-7000-8000-000000000e01|0|00000000-0000-7000-8000-000000000e01'
);
SELECT lc_expect(
    'start: the node is RUNNING from now',
    (SELECT status || ':' || (started_at = now())::text FROM horsies_workflow_tasks
     WHERE id = '00000000-0000-7000-8000-000000000f11'),
    'RUNNING:true'
);
SELECT lc_task('00000000-0000-7000-8000-000000000d12', 'CLAIMED', 'w1', TRUE);
UPDATE horsies_tasks SET retry_count = 1 WHERE id = '00000000-0000-7000-8000-000000000d12';
SELECT lc_node('00000000-0000-7000-8000-000000000f12', '00000000-0000-7000-8000-000000000e01', 1, 'RUNNING',
               '00000000-0000-7000-8000-000000000d12');
SELECT lc_expect(
    'start: a retried attempt keeps the first started_at of its node',
    (SELECT outcome FROM horsies_start_attempt(
        '00000000-0000-7000-8000-000000000d12', 'w1', now() - interval '1 second', 42, 'h', 'p')),
    'STARTED'
);
SELECT lc_expect(
    'start: the retried attempt left the node and its started_at',
    (SELECT status || ':' || (now() - started_at)::text FROM horsies_workflow_tasks
     WHERE id = '00000000-0000-7000-8000-000000000f12'),
    'RUNNING:01:00:00'
);
-- e04 is a sub-workflow of e01 at its node 1: a task of e04 returns e01 as its root. e05 is a top-level workflow
-- whose row stores no root: a task of e05 returns e05.
SELECT lc_workflow('00000000-0000-7000-8000-000000000e04', 'RUNNING');
UPDATE horsies_workflows
SET parent_workflow_id = '00000000-0000-7000-8000-000000000e01', parent_task_index = 1, depth = 1,
    root_workflow_id = '00000000-0000-7000-8000-000000000e01'
WHERE id = '00000000-0000-7000-8000-000000000e04';
SELECT lc_task('00000000-0000-7000-8000-000000000d17', 'CLAIMED', 'w1', TRUE);
SELECT lc_node('00000000-0000-7000-8000-000000000f17', '00000000-0000-7000-8000-000000000e04', 3, 'ENQUEUED',
               '00000000-0000-7000-8000-000000000d17');
SELECT lc_workflow('00000000-0000-7000-8000-000000000e05', 'RUNNING');
UPDATE horsies_workflows SET root_workflow_id = NULL WHERE id = '00000000-0000-7000-8000-000000000e05';
SELECT lc_task('00000000-0000-7000-8000-000000000d18', 'CLAIMED', 'w1', TRUE);
SELECT lc_node('00000000-0000-7000-8000-000000000f18', '00000000-0000-7000-8000-000000000e05', 0, 'ENQUEUED',
               '00000000-0000-7000-8000-000000000d18');
SELECT lc_expect(
    'start: a task of a sub-workflow returns the root of its tree; a workflow with no stored root is its own root',
    (SELECT string_agg(concat_ws('|', right(s.workflow_id::text, 3), s.workflow_task_index,
        right(s.root_workflow_id::text, 3)), ',' ORDER BY t.id)
     FROM (VALUES ('00000000-0000-7000-8000-000000000d17'::uuid), ('00000000-0000-7000-8000-000000000d18'::uuid)) t(id)
     CROSS JOIN LATERAL horsies_start_attempt(t.id, 'w1', now() - interval '1 second', 42, 'h', 'p') s),
    'e04|3|e01,e05|0|e05'
);
SELECT lc_workflow('00000000-0000-7000-8000-000000000e02', 'PAUSED');
SELECT lc_task('00000000-0000-7000-8000-000000000d13', 'CLAIMED', 'w1', TRUE);
SELECT lc_node('00000000-0000-7000-8000-000000000f13', '00000000-0000-7000-8000-000000000e02', 0, 'ENQUEUED',
               '00000000-0000-7000-8000-000000000d13');
SELECT lc_expect(
    'start: a paused workflow',
    (SELECT concat_ws('|', outcome, miss_reason) FROM horsies_start_attempt(
        '00000000-0000-7000-8000-000000000d13', 'w1', now() - interval '1 second', 42, 'h', 'p')),
    'MISSED|WORKFLOW_PAUSED'
);
SELECT lc_expect(
    'start: a paused workflow leaves the row and the node as they were',
    (SELECT t.status || ':' || n.status FROM horsies_tasks t JOIN horsies_workflow_tasks n ON n.task_id = t.id
     WHERE t.id = '00000000-0000-7000-8000-000000000d13'),
    'CLAIMED:ENQUEUED'
);
SELECT lc_workflow('00000000-0000-7000-8000-000000000e03', 'CANCELLED');
SELECT lc_task('00000000-0000-7000-8000-000000000d14', 'CLAIMED', 'w1', TRUE);
SELECT lc_node('00000000-0000-7000-8000-000000000f14', '00000000-0000-7000-8000-000000000e03', 0, 'ENQUEUED',
               '00000000-0000-7000-8000-000000000d14');
SELECT lc_expect(
    'start: a cancelled workflow',
    (SELECT concat_ws('|', outcome, miss_reason) FROM horsies_start_attempt(
        '00000000-0000-7000-8000-000000000d14', 'w1', now() - interval '1 second', 42, 'h', 'p')),
    'MISSED|WORKFLOW_CANCELLED'
);
SELECT lc_task('00000000-0000-7000-8000-000000000d15', 'CLAIMED', 'w1', TRUE);
SELECT lc_node('00000000-0000-7000-8000-000000000f15', '00000000-0000-7000-8000-000000000e01', 2, 'COMPLETED',
               '00000000-0000-7000-8000-000000000d15');
SELECT lc_expect(
    'start: a node in a final status is not linked',
    (SELECT concat_ws('|', outcome, miss_reason) FROM horsies_start_attempt(
        '00000000-0000-7000-8000-000000000d15', 'w1', now() - interval '1 second', 42, 'h', 'p')),
    'MISSED|NODE_NOT_LINKED'
);
SELECT lc_task('00000000-0000-7000-8000-000000000d16', 'CLAIMED', 'w1', TRUE);
SELECT lc_expect(
    'start: a workflow task with no node is not linked',
    (SELECT concat_ws('|', outcome, miss_reason) FROM horsies_start_attempt(
        '00000000-0000-7000-8000-000000000d16', 'w1', now() - interval '1 second', 42, 'h', 'p')),
    'MISSED|NODE_NOT_LINKED'
);

------------------------------------------------------------------------------------------------------------------------
-- horsies_mark_finalizing
------------------------------------------------------------------------------------------------------------------------

SELECT lc_task('00000000-0000-7000-8000-000000008001', 'RUNNING');
SELECT lc_expect(
    'marker: a held RUNNING row is marked',
    (SELECT outcome FROM horsies_mark_finalizing(
        '00000000-0000-7000-8000-000000008001', 'w1', now() - interval '1 second')),
    'MARKED'
);
SELECT lc_expect(
    'marker: the row carries the marker and stays RUNNING under its claim',
    (SELECT concat_ws('|', status, finalizing_at = now(), finalizing_by_worker_id, updated_at = now(),
        claimed_by_worker_id, claimed_at = now() - interval '1 second')
     FROM horsies_tasks WHERE id = '00000000-0000-7000-8000-000000008001'),
    'RUNNING|t|w1|t|w1|t'
);
SELECT lc_task('00000000-0000-7000-8000-000000008002', 'RUNNING');
UPDATE horsies_tasks SET updated_at = now() - interval '1 minute'
WHERE id = '00000000-0000-7000-8000-000000008002';
SELECT lc_expect(
    'marker: another claim generation is not held',
    (SELECT outcome FROM horsies_mark_finalizing(
        '00000000-0000-7000-8000-000000008002', 'w1', now() - interval '2 seconds')),
    'NOT_HELD'
);
SELECT lc_expect(
    'marker: another worker does not hold it',
    (SELECT outcome FROM horsies_mark_finalizing(
        '00000000-0000-7000-8000-000000008002', 'w2', now() - interval '1 second')),
    'NOT_HELD'
);
SELECT lc_expect(
    'marker: a miss writes nothing',
    (SELECT concat_ws('|', finalizing_at IS NULL, finalizing_by_worker_id IS NULL,
        updated_at = now() - interval '1 minute')
     FROM horsies_tasks WHERE id = '00000000-0000-7000-8000-000000008002'),
    't|t|t'
);
SELECT lc_task('00000000-0000-7000-8000-000000008003', 'CLAIMED');
SELECT lc_expect(
    'marker: a CLAIMED row is not held',
    (SELECT outcome FROM horsies_mark_finalizing(
        '00000000-0000-7000-8000-000000008003', 'w1', now() - interval '1 second')),
    'NOT_HELD'
);
SELECT lc_expect(
    'marker: an absent task is not held',
    (SELECT outcome FROM horsies_mark_finalizing(
        '00000000-0000-7000-8000-0000000080ff', 'w1', now() - interval '1 second')),
    'NOT_HELD'
);
SELECT lc_expect_sqlstate('marker: a NULL claim generation', $q$SELECT * FROM horsies_mark_finalizing(
    '00000000-0000-7000-8000-000000008002', 'w1', NULL)$q$, '22023');
SELECT lc_expect_sqlstate('marker: a NULL worker', $q$SELECT * FROM horsies_mark_finalizing(
    '00000000-0000-7000-8000-000000008002', NULL, now() - interval '1 second')$q$, '22023');
SELECT lc_expect_sqlstate('marker: a NULL task', $q$SELECT * FROM horsies_mark_finalizing(
    NULL, 'w1', now() - interval '1 second')$q$, '22023');

------------------------------------------------------------------------------------------------------------------------
-- Success: horsies_complete_task_fused, horsies_complete_tasks_fused, horsies_finish_workflow_success
------------------------------------------------------------------------------------------------------------------------

SELECT lc_task('00000000-0000-7000-8000-000000001001', 'RUNNING');
SELECT lc_expect(
    'success: a held RUNNING row completes',
    (SELECT concat_ws('|', outcome, terminalization_kind, observed_status, observed_worker_id,
        observed_claimed_at = now() - interval '1 second', terminal_at = now(), ordinality IS NULL)
     FROM horsies_complete_task_fused(
        '00000000-0000-7000-8000-000000001001', 'w1', now() - interval '1 second', '{"ok":1}', TRUE)),
    'APPLIED|COMPLETE_FUSED|RUNNING|w1|t|t|t'
);
SELECT lc_expect(
    'success: the history row and its one COMPLETED attempt',
    (SELECT concat_ws('|', status, terminalization_kind, convert_from(result_payload, 'UTF8'), retry_count,
        last_claimed_worker_id, octet_length(attempt_snapshot) > 0)
     FROM horsies_task_history WHERE task_id = '00000000-0000-7000-8000-000000001001'),
    'COMPLETED|COMPLETE_FUSED|{"ok":1}|0|w1|t'
);
SELECT lc_expect(
    'success: the live row and its attempts are gone',
    (SELECT count(*)::text FROM horsies_tasks WHERE id = '00000000-0000-7000-8000-000000001001')
    || ':' || (SELECT count(*)::text FROM horsies_task_attempts WHERE task_id = '00000000-0000-7000-8000-000000001001'),
    '0:0'
);
SELECT lc_expect(
    'success: a replay is already applied',
    (SELECT outcome || ':' || terminalization_kind FROM horsies_complete_task_fused(
        '00000000-0000-7000-8000-000000001001', 'w1', now() - interval '1 second', '{"ok":1}', TRUE)),
    'ALREADY_APPLIED:COMPLETE_FUSED'
);
SELECT lc_task('00000000-0000-7000-8000-000000001002', 'RUNNING');
SELECT lc_expect(
    'success: another claim generation is a lost claim',
    (SELECT outcome FROM horsies_complete_task_fused(
        '00000000-0000-7000-8000-000000001002', 'w1', now() - interval '2 seconds', '{"ok":1}', TRUE)),
    'LOST_CLAIM'
);
SELECT lc_expect(
    'success: another worker is a lost claim',
    (SELECT outcome FROM horsies_complete_task_fused(
        '00000000-0000-7000-8000-000000001002', 'w2', now() - interval '1 second', '{"ok":1}', TRUE)),
    'LOST_CLAIM'
);
SELECT lc_task('00000000-0000-7000-8000-000000001003', 'CLAIMED');
SELECT lc_expect(
    'success: a row that never started is a source state conflict',
    (SELECT outcome || ':' || observed_status FROM horsies_complete_task_fused(
        '00000000-0000-7000-8000-000000001003', 'w1', now() - interval '1 second', '{"ok":1}', TRUE)),
    'SOURCE_STATE_CONFLICT:CLAIMED'
);
SELECT lc_expect(
    'success: an absent task',
    (SELECT outcome FROM horsies_complete_task_fused(
        '00000000-0000-7000-8000-0000000010ff', 'w1', now(), '{"ok":1}', TRUE)),
    'TASK_ABSENT'
);
SELECT lc_expect(
    'success: a refusal writes nothing',
    (SELECT status FROM horsies_tasks WHERE id = '00000000-0000-7000-8000-000000001002')
    || ':' || (SELECT count(*)::text FROM horsies_task_attempts WHERE task_id = '00000000-0000-7000-8000-000000001002'),
    'RUNNING:0'
);
SELECT lc_expect_sqlstate('success: a NULL claim generation', $q$SELECT * FROM horsies_complete_task_fused(
    '00000000-0000-7000-8000-000000001002', 'w1', NULL, '{"ok":1}', TRUE)$q$, '22023');

-- The batch: rows given out of id order, one of them not held.
SELECT lc_task('00000000-0000-7000-8000-000000001013', 'RUNNING');
SELECT lc_task('00000000-0000-7000-8000-000000001011', 'RUNNING');
SELECT lc_task('00000000-0000-7000-8000-000000001012', 'RUNNING', 'w2');
SELECT lc_expect(
    'success batch: one outcome per row, at its input position',
    (SELECT string_agg(ordinality || ':' || right(task_id::text, 4) || ':' || outcome, ',' ORDER BY ordinality)
     FROM horsies_complete_tasks_fused(
        ARRAY['00000000-0000-7000-8000-000000001013', '00000000-0000-7000-8000-000000001012',
              '00000000-0000-7000-8000-000000001011']::uuid[],
        ARRAY[now() - interval '1 second', now() - interval '1 second', now() - interval '1 second'],
        ARRAY['{"ok":13}', '{"ok":12}', '{"ok":11}'], 'w1', TRUE)),
    '1:1013:APPLIED,2:1012:LOST_CLAIM,3:1011:APPLIED'
);
SELECT lc_expect(
    'success batch: each result went to its own row',
    (SELECT string_agg(right(task_id::text, 4) || '=' || convert_from(result_payload, 'UTF8'), ',' ORDER BY task_id)
     FROM horsies_task_history
     WHERE task_id IN ('00000000-0000-7000-8000-000000001011', '00000000-0000-7000-8000-000000001013')),
    '1011={"ok":11},1013={"ok":13}'
);
SELECT lc_expect_sqlstate('success batch: an empty batch', $q$SELECT * FROM horsies_complete_tasks_fused(
    ARRAY[]::uuid[], ARRAY[]::timestamptz[], ARRAY[]::text[], 'w1', TRUE)$q$, '22023');
SELECT lc_expect_sqlstate('success batch: 65 rows', $q$SELECT * FROM horsies_complete_tasks_fused(
    ARRAY(SELECT gen_random_uuid() FROM generate_series(1, 65)),
    ARRAY(SELECT now() FROM generate_series(1, 65)),
    ARRAY(SELECT '{}'::text FROM generate_series(1, 65)), 'w1', TRUE)$q$, '22023');
SELECT lc_expect_sqlstate('success batch: arrays of different lengths', $q$SELECT * FROM horsies_complete_tasks_fused(
    ARRAY[gen_random_uuid()], ARRAY[now(), now()], ARRAY['{}'], 'w1', TRUE)$q$, '22023');
SELECT lc_expect_sqlstate('success batch: a repeated id', $q$SELECT * FROM horsies_complete_tasks_fused(
    ARRAY['00000000-0000-7000-8000-000000001002', '00000000-0000-7000-8000-000000001002']::uuid[],
    ARRAY[now(), now()], ARRAY['{}', '{}'], 'w1', TRUE)$q$, '22023');
SELECT lc_expect_sqlstate('success batch: a NULL claim generation', $q$SELECT * FROM horsies_complete_tasks_fused(
    ARRAY[gen_random_uuid()], ARRAY[NULL]::timestamptz[], ARRAY['{}'], 'w1', TRUE)$q$, '22023');

-- A workflow task: index 0 has a dependent (index 1), index 2 has none.
SELECT lc_workflow('00000000-0000-7000-8000-000000001e01', 'RUNNING');
SELECT lc_task('00000000-0000-7000-8000-000000001021', 'RUNNING', 'w1', TRUE);
SELECT lc_node('00000000-0000-7000-8000-000000001f21', '00000000-0000-7000-8000-000000001e01', 0, 'RUNNING',
               '00000000-0000-7000-8000-000000001021');
SELECT lc_node('00000000-0000-7000-8000-000000001f22', '00000000-0000-7000-8000-000000001e01', 1, 'PENDING',
               NULL, ARRAY[0]);
SELECT lc_task('00000000-0000-7000-8000-000000001023', 'RUNNING', 'w1', TRUE);
SELECT lc_node('00000000-0000-7000-8000-000000001f23', '00000000-0000-7000-8000-000000001e01', 2, 'RUNNING',
               '00000000-0000-7000-8000-000000001023');
SELECT lc_expect(
    'workflow success: applied, with a dependent node',
    (SELECT concat_ws('|', outcome, terminalization_kind, queue_name, task_name, node_has_dependents)
     FROM horsies_finish_workflow_success(
        '00000000-0000-7000-8000-000000001021', 'w1', now() - interval '1 second', '{"ok":21}')),
    'APPLIED|COMPLETE_LOCKED|q_lc|lc.task|t'
);
SELECT lc_expect(
    'workflow success: the phase-2 record',
    (SELECT terminalization_kind || ':' || workflow_node_row_id FROM horsies_workflow_phase2_pending
     WHERE task_id = '00000000-0000-7000-8000-000000001021'),
    'COMPLETE_LOCKED:00000000-0000-7000-8000-000000001f21'
);
SELECT lc_expect(
    'workflow success: applied, with no dependent node',
    (SELECT outcome || ':' || node_has_dependents FROM horsies_finish_workflow_success(
        '00000000-0000-7000-8000-000000001023', 'w1', now() - interval '1 second', '{"ok":23}')),
    'APPLIED:false'
);
SELECT lc_expect(
    'workflow success: a replay is already applied with no dependents fact',
    (SELECT outcome || ':' || coalesce(node_has_dependents::text, 'null') FROM horsies_finish_workflow_success(
        '00000000-0000-7000-8000-000000001023', 'w1', now() - interval '1 second', '{"ok":23}')),
    'ALREADY_APPLIED:null'
);
SELECT lc_expect_sqlstate('workflow success: a plain task', $q$SELECT * FROM horsies_finish_workflow_success(
    '00000000-0000-7000-8000-000000001002', 'w1', now() - interval '1 second', '{}')$q$, '22023');
SELECT lc_expect_sqlstate('workflow success: a NULL result', $q$SELECT * FROM horsies_finish_workflow_success(
    '00000000-0000-7000-8000-000000001002', 'w2', now() - interval '1 second', NULL)$q$, '23502');
SELECT lc_expect_sqlstate('workflow success: a NULL claim generation', $q$SELECT * FROM horsies_finish_workflow_success(
    '00000000-0000-7000-8000-000000001002', 'w2', NULL, '{}')$q$, '22023');
SELECT lc_task('00000000-0000-7000-8000-000000001024', 'RUNNING', 'w1', TRUE);
SELECT lc_node('00000000-0000-7000-8000-000000001f24', '00000000-0000-7000-8000-000000001e01', 3, 'RUNNING',
               '00000000-0000-7000-8000-000000001024');
SELECT lc_expect_sqlstate('success: the plain success refuses a workflow task (the move)', $q$
    SELECT * FROM horsies_complete_task_fused(
        '00000000-0000-7000-8000-000000001024', 'w1', now() - interval '1 second', '{}', TRUE)$q$, '22023');

------------------------------------------------------------------------------------------------------------------------
-- horsies_finish_failed_attempt
------------------------------------------------------------------------------------------------------------------------

SELECT lc_task('00000000-0000-7000-8000-000000002001', 'RUNNING');
SELECT lc_expect(
    'failed attempt: no retry fails the task',
    (SELECT concat_ws('|', outcome, terminalization_kind, retry_disposition, is_workflow_task, queue_name,
        task_name, coalesce(node_has_dependents::text, 'null'))
     FROM horsies_finish_failed_attempt(
        '00000000-0000-7000-8000-000000002001', 'w1', now() - interval '1 second', 'NONE', NULL, NULL, NULL,
        '{"err":1}', 'BOOM', 'final reason', 'FAILED', 'attempt message', 'attempt reason', 'REFUSE', TRUE)),
    'APPLIED|FAIL_RUNNING|NOT_REQUESTED|f|q_lc|lc.task|null'
);
SELECT lc_expect(
    'failed attempt: the history row',
    (SELECT concat_ws('|', status, error_code, final_failed_reason, convert_from(result_payload, 'UTF8'))
     FROM horsies_task_history WHERE task_id = '00000000-0000-7000-8000-000000002001'),
    'FAILED|BOOM|final reason|{"err":1}'
);
SELECT lc_task('00000000-0000-7000-8000-000000002002', 'RUNNING');
UPDATE horsies_tasks SET execution = 'SHARED_ASYNC', retry_count = 1, finalizing_at = now(),
    finalizing_by_worker_id = 'w1'
WHERE id = '00000000-0000-7000-8000-000000002002';
SELECT lc_expect(
    'failed attempt: a retry in the same class',
    (SELECT concat_ws('|', outcome, retry_count, next_retry_at > now() + interval '9 seconds',
        next_retry_at < clock_timestamp() + interval '11 seconds', queue_name)
     FROM horsies_finish_failed_attempt(
        '00000000-0000-7000-8000-000000002002', 'w1', now() - interval '1 second', 'RETRY', 1, 10.0, 'SAME_CLASS',
        '{"err":2}', 'BOOM', 'r', 'FAILED', 'm', 'r', 'REFUSE', TRUE)),
    'RETRY_SCHEDULED|2|t|t|q_lc'
);
SELECT lc_expect(
    'failed attempt: the retried row is PENDING with every claim and run field cleared',
    (SELECT concat_ws('|', status, execution, retry_count, enqueued_at = next_retry_at,
        num_nulls(claimed_at, claimed_by_worker_id, claim_expires_at, started_at, worker_pid, worker_hostname,
                  worker_process_name, finalizing_at, finalizing_by_worker_id))
     FROM horsies_tasks WHERE id = '00000000-0000-7000-8000-000000002002'),
    'PENDING|SHARED_ASYNC|2|t|9'
);
SELECT lc_expect(
    'failed attempt: the attempt row of a retry',
    (SELECT concat_ws('|', attempt, outcome, will_retry, error_code, error_message, failed_reason, worker_id)
     FROM horsies_task_attempts WHERE task_id = '00000000-0000-7000-8000-000000002002'),
    '2|FAILED|t|BOOM|m|r|w1'
);
SELECT lc_task('00000000-0000-7000-8000-000000002003', 'RUNNING');
UPDATE horsies_tasks SET execution = 'SHARED_THREAD' WHERE id = '00000000-0000-7000-8000-000000002003';
SELECT lc_expect(
    'failed attempt: a retry placed ISOLATED',
    (SELECT outcome FROM horsies_finish_failed_attempt(
        '00000000-0000-7000-8000-000000002003', 'w1', now() - interval '1 second', 'RETRY', 0, 0.0, 'ISOLATED',
        '{"err":3}', 'BOOM', 'r', 'WORKER_FAILURE', 'm', 'r', 'REFUSE', TRUE)),
    'RETRY_SCHEDULED'
);
SELECT lc_expect(
    'failed attempt: the ISOLATED placement and the worker-failure attempt',
    (SELECT t.execution || ':' || a.outcome FROM horsies_tasks t JOIN horsies_task_attempts a ON a.task_id = t.id
     WHERE t.id = '00000000-0000-7000-8000-000000002003'),
    'ISOLATED:WORKER_FAILURE'
);
SELECT lc_task('00000000-0000-7000-8000-000000002004', 'RUNNING');
UPDATE horsies_tasks SET good_until = now() + interval '5 seconds' WHERE id = '00000000-0000-7000-8000-000000002004';
SELECT lc_expect(
    'failed attempt: a retry at or after the deadline fails the task',
    (SELECT concat_ws('|', outcome, retry_disposition, good_until = now() + interval '5 seconds')
     FROM horsies_finish_failed_attempt(
        '00000000-0000-7000-8000-000000002004', 'w1', now() - interval '1 second', 'RETRY', 0, 60.0, 'SAME_CLASS',
        '{"err":4}', 'BOOM', 'r', 'FAILED', 'm', 'r', 'REFUSE', TRUE)),
    'APPLIED|PAST_DEADLINE|t'
);
SELECT lc_task('00000000-0000-7000-8000-000000002005', 'RUNNING');
SELECT lc_expect(
    'failed attempt: a retry from another retry count is refused',
    (SELECT outcome || ':' || observed_status FROM horsies_finish_failed_attempt(
        '00000000-0000-7000-8000-000000002005', 'w1', now() - interval '1 second', 'RETRY', 4, 1.0, 'SAME_CLASS',
        '{"err":5}', 'BOOM', 'r', 'FAILED', 'm', 'r', 'REFUSE', TRUE)),
    'SOURCE_STATE_CONFLICT:RUNNING'
);
SELECT lc_expect(
    'failed attempt: another claim generation is a lost claim',
    (SELECT outcome FROM horsies_finish_failed_attempt(
        '00000000-0000-7000-8000-000000002005', 'w1', now() - interval '3 seconds', 'NONE', NULL, NULL, NULL,
        '{"err":5}', 'BOOM', 'r', 'FAILED', 'm', 'r', 'REFUSE', TRUE)),
    'LOST_CLAIM'
);
SELECT lc_expect(
    'failed attempt: a refusal writes nothing',
    (SELECT status || ':' || (SELECT count(*) FROM horsies_task_attempts WHERE task_id = t.id)
     FROM horsies_tasks t WHERE t.id = '00000000-0000-7000-8000-000000002005'),
    'RUNNING:0'
);
-- A killed child: the RELEASE rule.
SELECT lc_task('00000000-0000-7000-8000-000000002006', 'CLAIMED');
SELECT lc_expect(
    'failed attempt (#713): RELEASE returns a CLAIMED row of this generation with no attempt',
    (SELECT outcome || ':' || observed_status FROM horsies_finish_failed_attempt(
        '00000000-0000-7000-8000-000000002006', 'w1', now() - interval '1 second', 'RETRY', 0, 1.0, 'SAME_CLASS',
        '{"err":6}', 'WORKER_CRASHED', 'r', 'WORKER_FAILURE', 'm', 'r', 'RELEASE', TRUE)),
    'RELEASED:CLAIMED'
);
SELECT lc_expect(
    'failed attempt: the released row',
    (SELECT concat_ws('|', status, retry_count, num_nulls(claimed_at, claimed_by_worker_id, claim_expires_at),
        (SELECT count(*) FROM horsies_task_attempts WHERE task_id = t.id))
     FROM horsies_tasks t WHERE t.id = '00000000-0000-7000-8000-000000002006'),
    'PENDING|0|3|0'
);
SELECT lc_task('00000000-0000-7000-8000-00000000200a', 'RUNNING');
SELECT lc_expect(
    'failed attempt (#713): RELEASE on a row the child started writes the failure',
    (SELECT outcome || ':' || terminalization_kind FROM horsies_finish_failed_attempt(
        '00000000-0000-7000-8000-00000000200a', 'w1', now() - interval '1 second', 'NONE', NULL, NULL, NULL,
        '{"err":10}', 'WORKER_CRASHED', 'r', 'WORKER_FAILURE', 'm', 'r', 'RELEASE', TRUE)),
    'APPLIED:FAIL_RUNNING'
);
SELECT lc_task('00000000-0000-7000-8000-000000002007', 'CLAIMED');
SELECT lc_expect(
    'failed attempt (#713): RELEASE of another claim generation is a lost claim',
    (SELECT outcome FROM horsies_finish_failed_attempt(
        '00000000-0000-7000-8000-000000002007', 'w1', now() - interval '2 seconds', 'NONE', NULL, NULL, NULL,
        '{"err":7}', 'WORKER_CRASHED', 'r', 'WORKER_FAILURE', 'm', 'r', 'RELEASE', TRUE)),
    'LOST_CLAIM'
);
SELECT lc_expect(
    'failed attempt: REFUSE on a CLAIMED row is a source state conflict',
    (SELECT outcome || ':' || observed_status FROM horsies_finish_failed_attempt(
        '00000000-0000-7000-8000-000000002007', 'w1', now() - interval '1 second', 'NONE', NULL, NULL, NULL,
        '{"err":7}', 'BOOM', 'r', 'FAILED', 'm', 'r', 'REFUSE', TRUE)),
    'SOURCE_STATE_CONFLICT:CLAIMED'
);
-- A workflow task: the move records phase 2 and the dependents fact; no wake.
SELECT lc_task('00000000-0000-7000-8000-000000002008', 'RUNNING', 'w1', TRUE);
SELECT lc_node('00000000-0000-7000-8000-000000002f08', '00000000-0000-7000-8000-000000001e01', 4, 'RUNNING',
               '00000000-0000-7000-8000-000000002008');
SELECT lc_node('00000000-0000-7000-8000-000000002f09', '00000000-0000-7000-8000-000000001e01', 5, 'PENDING',
               NULL, ARRAY[4]);
SELECT lc_expect(
    'failed attempt: a workflow task owes phase 2 and reports its dependents',
    (SELECT concat_ws('|', outcome, is_workflow_task, node_has_dependents) FROM horsies_finish_failed_attempt(
        '00000000-0000-7000-8000-000000002008', 'w1', now() - interval '1 second', 'NONE', NULL, NULL, NULL,
        '{"err":8}', 'BOOM', 'r', 'FAILED', 'm', 'r', 'REFUSE', TRUE)),
    'APPLIED|t|t'
);
SELECT lc_expect(
    'failed attempt: the phase-2 record of the workflow task',
    (SELECT terminalization_kind FROM horsies_workflow_phase2_pending
     WHERE task_id = '00000000-0000-7000-8000-000000002008'),
    'FAIL_RUNNING'
);
SELECT lc_expect_sqlstate('failed attempt: NONE with a retry count', $q$SELECT * FROM horsies_finish_failed_attempt(
    gen_random_uuid(), 'w1', now(), 'NONE', 1, NULL, NULL, '{}', 'E', 'r', 'FAILED', 'm', 'r', 'REFUSE',
    TRUE)$q$, '22023');
SELECT lc_expect_sqlstate('failed attempt: RETRY without a placement', $q$SELECT * FROM horsies_finish_failed_attempt(
    gen_random_uuid(), 'w1', now(), 'RETRY', 1, 1.0, NULL, '{}', 'E', 'r', 'FAILED', 'm', 'r', 'REFUSE',
    TRUE)$q$, '22023');
SELECT lc_expect_sqlstate('failed attempt: an infinite delay', $q$SELECT * FROM horsies_finish_failed_attempt(
    gen_random_uuid(), 'w1', now(), 'RETRY', 1, 'Infinity'::float8, 'SAME_CLASS', '{}', 'E', 'r', 'FAILED', 'm',
    'r', 'REFUSE', TRUE)$q$, '22023');
SELECT lc_expect_sqlstate('failed attempt: a negative delay', $q$SELECT * FROM horsies_finish_failed_attempt(
    gen_random_uuid(), 'w1', now(), 'RETRY', 1, -1.0, 'SAME_CLASS', '{}', 'E', 'r', 'FAILED', 'm', 'r', 'REFUSE',
    TRUE)$q$, '22023');
SELECT lc_expect_sqlstate('failed attempt: an unknown placement', $q$SELECT * FROM horsies_finish_failed_attempt(
    gen_random_uuid(), 'w1', now(), 'RETRY', 1, 1.0, 'same_class', '{}', 'E', 'r', 'FAILED', 'm', 'r', 'REFUSE',
    TRUE)$q$, '22023');
SELECT lc_expect_sqlstate('failed attempt: a COMPLETED attempt outcome', $q$SELECT * FROM horsies_finish_failed_attempt(
    gen_random_uuid(), 'w1', now(), 'NONE', NULL, NULL, NULL, '{}', 'E', 'r', 'COMPLETED', 'm', 'r', 'REFUSE',
    TRUE)$q$, '22023');
SELECT lc_expect_sqlstate('failed attempt: an unknown claimed-row rule', $q$SELECT * FROM horsies_finish_failed_attempt(
    gen_random_uuid(), 'w1', now(), 'NONE', NULL, NULL, NULL, '{}', 'E', 'r', 'FAILED', 'm', 'r', 'KEEP',
    TRUE)$q$, '22023');
SELECT lc_expect_sqlstate('failed attempt: a NULL claim generation', $q$SELECT * FROM horsies_finish_failed_attempt(
    gen_random_uuid(), 'w1', NULL, 'NONE', NULL, NULL, NULL, '{}', 'E', 'r', 'FAILED', 'm', 'r', 'REFUSE',
    TRUE)$q$, '22023');
SELECT lc_expect_sqlstate('failed attempt: a NULL result', $q$SELECT * FROM horsies_finish_failed_attempt(
    gen_random_uuid(), 'w1', now(), 'NONE', NULL, NULL, NULL, NULL, 'E', 'r', 'FAILED', 'm', 'r', 'REFUSE',
    TRUE)$q$, '23502');

------------------------------------------------------------------------------------------------------------------------
-- Stale RUNNING recovery: horsies_stale_running_scan, horsies_finish_stale_attempt
------------------------------------------------------------------------------------------------------------------------

-- Queue q_stale: 3001 and 3002 started 10 minutes ago with no beat; 3003 has a fresh runner beat; 3004 has a
-- finalizing marker from now; 3005 has a beat 9 minutes old, inside the 10-minute recency bound but past the
-- threshold.
SELECT lc_task('00000000-0000-7000-8000-000000003001', 'RUNNING');
SELECT lc_task('00000000-0000-7000-8000-000000003002', 'RUNNING');
SELECT lc_task('00000000-0000-7000-8000-000000003003', 'RUNNING');
SELECT lc_task('00000000-0000-7000-8000-000000003004', 'RUNNING');
SELECT lc_task('00000000-0000-7000-8000-000000003005', 'RUNNING');
UPDATE horsies_tasks SET queue_name = 'q_stale', started_at = now() - interval '10 minutes', retry_count = 1
WHERE id BETWEEN '00000000-0000-7000-8000-000000003001' AND '00000000-0000-7000-8000-000000003005';
UPDATE horsies_tasks SET finalizing_at = now(), finalizing_by_worker_id = 'w1'
WHERE id = '00000000-0000-7000-8000-000000003004';
INSERT INTO horsies_heartbeats (task_id, sender_id, role, sent_at, hostname, pid) VALUES
    ('00000000-0000-7000-8000-000000003003', 'w1:7', 'runner', now(), 'host-a', 7),
    ('00000000-0000-7000-8000-000000003005', 'w1:7', 'runner', now() - interval '9 minutes', 'host-a', 7);

SELECT lc_expect(
    'stale scan: the silent rows, not the beaten or finalizing ones',
    (SELECT string_agg(right(task_id::text, 4), ',' ORDER BY task_id)
     FROM horsies_stale_running_scan('BEAT_AGE', 60000, 600000, NULL, NULL, 100)
     WHERE task_id::text LIKE '%300_'),
    '3001,3002,3005'
);
SELECT lc_expect(
    'stale scan: the facts of a candidate',
    (SELECT concat_ws('|', retry_count, max_retries, claimed_by_worker_id, worker_pid, worker_hostname,
        worker_process_name, started_at = now() - interval '10 minutes', db_now >= now())
     FROM horsies_stale_running_scan('BEAT_AGE', 60000, 600000, NULL, NULL, 100) WHERE task_id = '00000000-0000-7000-8000-000000003001'),
    '1|3|w1|7|host-a|worker-7|t|t'
);
SELECT lc_expect(
    'stale scan: the batch bound',
    (SELECT count(*)::text FROM horsies_stale_running_scan('BEAT_AGE', 60000, 600000, NULL, NULL, 2)),
    '2'
);
UPDATE horsies_tasks SET trace_carrier = '{"traceparent":"00-000102030405060708090a0b0c0d0e0f-0001020304050607-01"}',
    trace_id = decode('000102030405060708090a0b0c0d0e0f', 'hex')
WHERE id = '00000000-0000-7000-8000-000000003002';
SELECT lc_expect(
    'stale scan: the task name, the queue and the stored trace id of each candidate',
    (SELECT string_agg(concat_ws('|', right(task_id::text, 4), task_name, queue_name,
        coalesce(encode(trace_id, 'hex'), 'none')), ',' ORDER BY task_id)
     FROM horsies_stale_running_scan('BEAT_AGE', 60000, 600000, NULL, NULL, 100)
     WHERE task_id::text LIKE '%300_'),
    '3001|lc.task|q_stale|none,3002|lc.task|q_stale|000102030405060708090a0b0c0d0e0f,3005|lc.task|q_stale|none'
);
SELECT lc_expect_sqlstate('stale scan: a batch of 0', $q$SELECT * FROM horsies_stale_running_scan('BEAT_AGE', 1, 1, NULL, NULL, 0)$q$, '22023');
SELECT lc_expect_sqlstate('stale scan: a batch over 10000', $q$SELECT * FROM horsies_stale_running_scan('BEAT_AGE', 1, 1, NULL, NULL, 10001)$q$,
    '22023');
SELECT lc_expect_sqlstate('stale scan: a negative threshold', $q$SELECT * FROM horsies_stale_running_scan('BEAT_AGE', -1, 1, NULL, NULL, 10)$q$,
    '22023');

SELECT lc_expect(
    'stale attempt: no retry fails the task',
    (SELECT concat_ws('|', outcome, terminalization_kind, retry_disposition, observed_worker_id)
     FROM horsies_finish_stale_attempt(
        '00000000-0000-7000-8000-000000003001', 'BEAT_AGE', 60000, 600000, NULL, NULL, 'NONE', NULL, NULL,
        '{"err":"crashed"}', 'WORKER_CRASHED', 'Worker process crashed', 'crash message', 'crash reason')),
    'APPLIED|FAIL_STALE|NOT_REQUESTED|w1'
);
SELECT lc_expect(
    'stale attempt: the history row',
    (SELECT concat_ws('|', status, terminalization_kind, error_code, final_failed_reason)
     FROM horsies_task_history WHERE task_id = '00000000-0000-7000-8000-000000003001'),
    'FAILED|FAIL_STALE|WORKER_CRASHED|Worker process crashed'
);
SELECT lc_expect(
    'stale attempt: a replay is already applied',
    (SELECT outcome FROM horsies_finish_stale_attempt(
        '00000000-0000-7000-8000-000000003001', 'BEAT_AGE', 60000, 600000, NULL, NULL, 'NONE', NULL, NULL,
        '{"err":"crashed"}', 'WORKER_CRASHED', 'r', 'm', 'r')),
    'ALREADY_APPLIED'
);
SELECT lc_expect(
    'stale attempt: a crash retry by policy',
    (SELECT concat_ws('|', outcome, retry_count, next_retry_at > now() + interval '4 seconds', queue_name)
     FROM horsies_finish_stale_attempt(
        '00000000-0000-7000-8000-000000003002', 'BEAT_AGE', 60000, 600000, NULL, NULL, 'RETRY', 1, 5.0,
        '{"err":"crashed"}', 'WORKER_CRASHED', 'r', 'crash message', 'crash reason')),
    'RETRY_SCHEDULED|2|t|q_stale'
);
SELECT lc_expect(
    'stale attempt: the retried row and its FAILED attempt',
    (SELECT concat_ws('|', t.status, t.execution, t.retry_count, t.enqueued_at = t.next_retry_at,
        num_nulls(t.claimed_at, t.claimed_by_worker_id, t.started_at, t.worker_pid),
        a.attempt, a.outcome, a.will_retry, a.error_code, a.error_message, a.failed_reason, a.worker_id,
        a.started_at = now() - interval '10 minutes')
     FROM horsies_tasks t JOIN horsies_task_attempts a ON a.task_id = t.id
     WHERE t.id = '00000000-0000-7000-8000-000000003002'),
    'PENDING|ISOLATED|2|t|4|2|FAILED|t|WORKER_CRASHED|crash message|crash reason|w1|t'
);
SELECT lc_expect(
    'stale attempt: a fresh beat is not stale',
    (SELECT concat_ws('|', outcome, guard_kind, (observed_guard::jsonb ->> 'stale_after_ms'),
        (observed_guard::jsonb ? 'last_heartbeat_at'))
     FROM horsies_finish_stale_attempt(
        '00000000-0000-7000-8000-000000003003', 'BEAT_AGE', 60000, 600000, NULL, NULL, 'NONE', NULL, NULL,
        '{"err":"crashed"}', 'WORKER_CRASHED', 'r', 'm', 'r')),
    'SOURCE_STATE_CONFLICT|STALENESS|60000|t'
);
SELECT lc_expect(
    'stale attempt: a recent finalizing marker is not stale',
    (SELECT outcome || ':' || guard_kind FROM horsies_finish_stale_attempt(
        '00000000-0000-7000-8000-000000003004', 'BEAT_AGE', 60000, 600000, NULL, NULL, 'NONE', NULL, NULL,
        '{"err":"crashed"}', 'WORKER_CRASHED', 'r', 'm', 'r')),
    'SOURCE_STATE_CONFLICT:STALENESS'
);
SELECT lc_expect(
    'stale attempt: a retry from another retry count is refused',
    (SELECT outcome || ':' || guard_kind || ':' || (observed_guard::jsonb ->> 'retry_count')
     FROM horsies_finish_stale_attempt(
        '00000000-0000-7000-8000-000000003005', 'BEAT_AGE', 60000, 600000, NULL, NULL, 'RETRY', 0, 5.0,
        '{"err":"crashed"}', 'WORKER_CRASHED', 'r', 'm', 'r')),
    'SOURCE_STATE_CONFLICT:RETRY_COUNT:1'
);
SELECT lc_expect(
    'stale attempt: refusals write nothing',
    (SELECT string_agg(right(id::text, 4) || ':' || status, ',' ORDER BY id) FROM horsies_tasks
     WHERE id IN ('00000000-0000-7000-8000-000000003003', '00000000-0000-7000-8000-000000003004',
                  '00000000-0000-7000-8000-000000003005'))
    || ':' || (SELECT count(*)::text FROM horsies_task_attempts
               WHERE task_id IN ('00000000-0000-7000-8000-000000003003', '00000000-0000-7000-8000-000000003004',
                                 '00000000-0000-7000-8000-000000003005')),
    '3003:RUNNING,3004:RUNNING,3005:RUNNING:0'
);
UPDATE horsies_tasks SET good_until = now() + interval '2 seconds' WHERE id = '00000000-0000-7000-8000-000000003005';
SELECT lc_expect(
    'stale attempt: a crash retry at or after the deadline fails the task',
    (SELECT outcome || ':' || retry_disposition FROM horsies_finish_stale_attempt(
        '00000000-0000-7000-8000-000000003005', 'BEAT_AGE', 60000, 600000, NULL, NULL, 'RETRY', 1, 5.0,
        '{"err":"crashed"}', 'WORKER_CRASHED', 'r', 'm', 'r')),
    'APPLIED:PAST_DEADLINE'
);
SELECT lc_task('00000000-0000-7000-8000-000000003006', 'CLAIMED');
SELECT lc_expect(
    'stale attempt: a row that is not RUNNING',
    (SELECT outcome || ':' || observed_status FROM horsies_finish_stale_attempt(
        '00000000-0000-7000-8000-000000003006', 'BEAT_AGE', 0, 0, NULL, NULL, 'NONE', NULL, NULL, '{}', 'WORKER_CRASHED', 'r', 'm', 'r')),
    'SOURCE_STATE_CONFLICT:CLAIMED'
);
SELECT lc_expect(
    'stale attempt: an absent task',
    (SELECT outcome FROM horsies_finish_stale_attempt(
        '00000000-0000-7000-8000-0000000030ff', 'BEAT_AGE', 0, 0, NULL, NULL, 'NONE', NULL, NULL, '{}', 'WORKER_CRASHED', 'r', 'm', 'r')),
    'TASK_ABSENT'
);
SELECT lc_task('00000000-0000-7000-8000-000000003007', 'RUNNING', 'w1', TRUE);
UPDATE horsies_tasks SET started_at = now() - interval '10 minutes' WHERE id = '00000000-0000-7000-8000-000000003007';
SELECT lc_node('00000000-0000-7000-8000-000000003f07', '00000000-0000-7000-8000-000000001e01', 6, 'RUNNING',
               '00000000-0000-7000-8000-000000003007');
SELECT lc_expect(
    'stale attempt: a workflow task owes phase 2 and reports its dependents',
    (SELECT concat_ws('|', outcome, is_workflow_task, node_has_dependents) FROM horsies_finish_stale_attempt(
        '00000000-0000-7000-8000-000000003007', 'BEAT_AGE', 60000, 600000, NULL, NULL, 'NONE', NULL, NULL,
        '{"err":"crashed"}', 'WORKER_CRASHED', 'r', 'm', 'r')),
    'APPLIED|t|f'
);
SELECT lc_expect_sqlstate('stale attempt: NONE with a delay', $q$SELECT * FROM horsies_finish_stale_attempt(
    gen_random_uuid(), 'BEAT_AGE', 0, 0, NULL, NULL, 'NONE', NULL, 1.0, '{}', 'E', 'r', 'm', 'r')$q$, '22023');
SELECT lc_expect_sqlstate('stale attempt: RETRY without a count', $q$SELECT * FROM horsies_finish_stale_attempt(
    gen_random_uuid(), 'BEAT_AGE', 0, 0, NULL, NULL, 'RETRY', NULL, 1.0, '{}', 'E', 'r', 'm', 'r')$q$, '22023');
SELECT lc_expect_sqlstate('stale attempt: a negative threshold', $q$SELECT * FROM horsies_finish_stale_attempt(
    gen_random_uuid(), 'BEAT_AGE', -1, 0, NULL, NULL, 'NONE', NULL, NULL, '{}', 'E', 'r', 'm', 'r')$q$, '22023');
SELECT lc_expect_sqlstate('stale attempt: a NULL error code', $q$SELECT * FROM horsies_finish_stale_attempt(
    gen_random_uuid(), 'BEAT_AGE', 0, 0, NULL, NULL, 'NONE', NULL, NULL, '{}', NULL, 'r', 'm', 'r')$q$, '22023');
SELECT lc_expect_sqlstate('stale attempt: a NULL result', $q$SELECT * FROM horsies_finish_stale_attempt(
    gen_random_uuid(), 'BEAT_AGE', 0, 0, NULL, NULL, 'NONE', NULL, NULL, NULL, 'E', 'r', 'm', 'r')$q$, '23502');

-- Only the beats of this attempt count: a runner beat sent before started_at belongs to an earlier attempt, and the
-- start writes no beat. 3011: a retried task, CLAIMED again, whose earlier attempt left a runner beat 2 minutes ago
-- (inside the 600 s recency bound, past the 60 s threshold); its new attempt starts now. 3012 and 3013: RUNNING rows
-- whose earlier attempt left a beat 1 s before their start; 3012 started exactly 60 s ago (not stale: the test is
-- strict), 3013 started 1 ms before that (stale).
SELECT lc_task('00000000-0000-7000-8000-000000003011', 'CLAIMED');
UPDATE horsies_tasks SET retry_count = 1 WHERE id = '00000000-0000-7000-8000-000000003011';
SELECT lc_task('00000000-0000-7000-8000-000000003012', 'RUNNING');
SELECT lc_task('00000000-0000-7000-8000-000000003013', 'RUNNING');
UPDATE horsies_tasks SET started_at = now() - interval '60 seconds'
WHERE id = '00000000-0000-7000-8000-000000003012';
UPDATE horsies_tasks SET started_at = now() - interval '60.001 seconds'
WHERE id = '00000000-0000-7000-8000-000000003013';
INSERT INTO horsies_heartbeats (task_id, sender_id, role, sent_at, hostname, pid) VALUES
    ('00000000-0000-7000-8000-000000003011', 'w1', 'runner', now() - interval '2 minutes', 'host-a', 7),
    ('00000000-0000-7000-8000-000000003012', 'w1', 'runner', now() - interval '61 seconds', 'host-a', 7),
    ('00000000-0000-7000-8000-000000003013', 'w1', 'runner', now() - interval '61.001 seconds', 'host-a', 7);
SELECT lc_expect(
    'stale (retried attempt): the new attempt starts and writes no beat',
    (SELECT outcome FROM horsies_start_attempt(
        '00000000-0000-7000-8000-000000003011', 'w1', now() - interval '1 second', 42, 'host-b', 'worker-42'))
    || ':' || (SELECT string_agg((now() - sent_at)::text, ',') FROM horsies_heartbeats
               WHERE task_id = '00000000-0000-7000-8000-000000003011'),
    'STARTED:00:02:00'
);
SELECT lc_expect(
    'stale scan: a beat of an earlier attempt does not count for a retried attempt that started now',
    (SELECT count(*)::text FROM horsies_stale_running_scan('BEAT_AGE', 60000, 600000, NULL, NULL, 10000)
     WHERE task_id = '00000000-0000-7000-8000-000000003011'),
    '0'
);
SELECT lc_expect(
    'stale scan: started_at on the threshold is not stale, 1 ms past it is, whatever the beat of an earlier attempt',
    (SELECT string_agg(right(task_id::text, 4), ',' ORDER BY task_id)
     FROM horsies_stale_running_scan('BEAT_AGE', 60000, 600000, NULL, NULL, 10000)
     WHERE task_id IN ('00000000-0000-7000-8000-000000003012', '00000000-0000-7000-8000-000000003013')),
    '3013'
);
SELECT lc_expect(
    'stale attempt: a retried attempt that started now is judged from its start, with no beat in the evidence',
    (SELECT concat_ws('|', outcome, guard_kind, observed_guard::jsonb -> 'last_heartbeat_at' = 'null'::jsonb,
        (observed_guard::jsonb ->> 'started_at')::timestamptz = now())
     FROM horsies_finish_stale_attempt(
        '00000000-0000-7000-8000-000000003011', 'BEAT_AGE', 60000, 600000, NULL, NULL, 'NONE', NULL, NULL,
        '{"err":"crashed"}', 'WORKER_CRASHED', 'r', 'm', 'r')),
    'SOURCE_STATE_CONFLICT|STALENESS|t|t'
);
SELECT lc_expect(
    'stale attempt: started_at on the threshold is not stale, whatever the beat of an earlier attempt',
    (SELECT concat_ws('|', outcome, guard_kind, observed_guard::jsonb -> 'last_heartbeat_at' = 'null'::jsonb)
     FROM horsies_finish_stale_attempt(
        '00000000-0000-7000-8000-000000003012', 'BEAT_AGE', 60000, 600000, NULL, NULL, 'NONE', NULL, NULL,
        '{"err":"crashed"}', 'WORKER_CRASHED', 'r', 'm', 'r')),
    'SOURCE_STATE_CONFLICT|STALENESS|t'
);
SELECT lc_expect(
    'stale attempt: started_at 1 ms past the threshold is stale',
    (SELECT concat_ws('|', outcome, terminalization_kind) FROM horsies_finish_stale_attempt(
        '00000000-0000-7000-8000-000000003013', 'BEAT_AGE', 60000, 600000, NULL, NULL, 'NONE', NULL, NULL,
        '{"err":"crashed"}', 'WORKER_CRASHED', 'r', 'm', 'r')),
    'APPLIED|FAIL_STALE'
);
SELECT lc_expect(
    'stale attempt: the attempts that are not stale stay RUNNING',
    (SELECT string_agg(right(id::text, 4) || ':' || status, ',' ORDER BY id) FROM horsies_tasks
     WHERE id IN ('00000000-0000-7000-8000-000000003011', '00000000-0000-7000-8000-000000003012')),
    '3011:RUNNING,3012:RUNNING'
);

------------------------------------------------------------------------------------------------------------------------
-- horsies_requeue_stale_claims
------------------------------------------------------------------------------------------------------------------------

-- 4001: claimed 10 minutes ago, no beat. 4002: claimed 10 minutes ago, a fresh claimer beat. 4003: claimed now,
-- with only a beat of an earlier claim generation. 4004: an orphan workflow task. 4005: a workflow task with a
-- runnable node. 4006 and 4007: two more stale plain rows, for the batch bound.
SELECT lc_task('00000000-0000-7000-8000-000000004001', 'CLAIMED');
SELECT lc_task('00000000-0000-7000-8000-000000004002', 'CLAIMED');
SELECT lc_task('00000000-0000-7000-8000-000000004003', 'CLAIMED');
SELECT lc_task('00000000-0000-7000-8000-000000004004', 'CLAIMED', 'w1', TRUE);
SELECT lc_task('00000000-0000-7000-8000-000000004005', 'CLAIMED', 'w1', TRUE);
SELECT lc_task('00000000-0000-7000-8000-000000004006', 'CLAIMED');
SELECT lc_task('00000000-0000-7000-8000-000000004007', 'CLAIMED');
SELECT lc_node('00000000-0000-7000-8000-000000004f05', '00000000-0000-7000-8000-000000001e01', 7, 'ENQUEUED',
               '00000000-0000-7000-8000-000000004005');
UPDATE horsies_tasks SET claimed_at = now() - interval '10 minutes', finalizing_at = now(),
    finalizing_by_worker_id = 'w1'
WHERE id IN ('00000000-0000-7000-8000-000000004001', '00000000-0000-7000-8000-000000004002',
             '00000000-0000-7000-8000-000000004004', '00000000-0000-7000-8000-000000004005',
             '00000000-0000-7000-8000-000000004006', '00000000-0000-7000-8000-000000004007');
INSERT INTO horsies_heartbeats (task_id, sender_id, role, sent_at, hostname, pid) VALUES
    ('00000000-0000-7000-8000-000000004002', 'w1', 'claimer', now(), 'host-a', 7),
    ('00000000-0000-7000-8000-000000004003', 'w1', 'claimer', now() - interval '20 minutes', 'host-a', 7);
-- Every row outside this case is out of reach: the other CLAIMED rows are younger than 5 minutes.
CREATE TEMP TABLE lc_requeued AS
    SELECT r.* FROM horsies_requeue_stale_claims('BEAT_AGE', 300000, NULL, NULL, 2) AS r;
SELECT lc_expect(
    'requeue: the batch bound',
    (SELECT count(*)::text FROM lc_requeued),
    '2'
);
SELECT lc_expect(
    'requeue: the next batch takes the rest, then a short batch ends the pass',
    (SELECT count(*) FROM horsies_requeue_stale_claims('BEAT_AGE', 300000, NULL, NULL, 10))::text || ','
        || (SELECT count(*) FROM horsies_requeue_stale_claims('BEAT_AGE', 300000, NULL, NULL, 10))::text,
    '2,0'
);
SELECT lc_expect(
    'requeue: each returned row names a requeued row, its task name and its queue',
    (SELECT string_agg(concat_ws('|', t.status, r.task_name, r.queue_name), ',')
     FROM lc_requeued r JOIN horsies_tasks t ON t.id = r.task_id),
    'PENDING|lc.task|q_lc,PENDING|lc.task|q_lc'
);
DROP TABLE lc_requeued;
SELECT lc_expect(
    'requeue: silent claims go back; a beaten claim, a fresh claim and an orphan stay',
    (SELECT string_agg(right(id::text, 4) || ':' || status, ',' ORDER BY id) FROM horsies_tasks
     WHERE id BETWEEN '00000000-0000-7000-8000-000000004001' AND '00000000-0000-7000-8000-000000004007'),
    '4001:PENDING,4002:CLAIMED,4003:CLAIMED,4004:CLAIMED,4005:PENDING,4006:PENDING,4007:PENDING'
);
SELECT lc_expect(
    'requeue: a requeued row has every claim and run field cleared',
    (SELECT num_nulls(claimed_at, claimed_by_worker_id, claim_expires_at, started_at, worker_pid, worker_hostname,
                      worker_process_name, finalizing_at, finalizing_by_worker_id)::text
     FROM horsies_tasks WHERE id = '00000000-0000-7000-8000-000000004001'),
    '9'
);
SELECT lc_expect_sqlstate('requeue: a batch of 0', $q$SELECT horsies_requeue_stale_claims('BEAT_AGE', 1, NULL, NULL, 0)$q$, '22023');
SELECT lc_expect_sqlstate('requeue: a negative threshold', $q$SELECT horsies_requeue_stale_claims('BEAT_AGE', -1, NULL, NULL, 1)$q$, '22023');

------------------------------------------------------------------------------------------------------------------------
-- Owner gone: horsies_worker_liveness_key, horsies_unheld_worker_owners and the evidence OWNER_GONE
------------------------------------------------------------------------------------------------------------------------

SELECT lc_expect(
    'liveness key: hashtextextended of the prefixed worker id; NULL for no id',
    (horsies_worker_liveness_key('L-key') = hashtextextended('horsies-worker-liveness:L-key', 0))::text
        || ':' || (horsies_worker_liveness_key(NULL) IS NULL)::text,
    'true:true'
);

-- Owners L-a to L-e sort before the w owners of the other cases (C collation). L-a holds its lock in this session.
-- d001: RUNNING of L-a. d002, d009: CLAIMED of L-b, d009 with a fresh claimer beat. d010: CLAIMED of L-b, claimed
-- at now(). d011: an orphan workflow task of L-b. d003: RUNNING of L-c, with a fresh runner beat and a fresh
-- finalizing marker. d004: CLAIMED of L-c. d006: RUNNING of L-c, claimed at now(). d008: RUNNING of L-c, for the
-- retry. d005: PENDING with a stray owner L-d. d007, d012: RUNNING and CLAIMED of L-e.
SELECT lc_task('00000000-0000-7000-8000-00000000d001', 'RUNNING', 'L-a');
SELECT lc_task('00000000-0000-7000-8000-00000000d002', 'CLAIMED', 'L-b');
SELECT lc_task('00000000-0000-7000-8000-00000000d009', 'CLAIMED', 'L-b');
SELECT lc_task('00000000-0000-7000-8000-00000000d010', 'CLAIMED', 'L-b');
SELECT lc_task('00000000-0000-7000-8000-00000000d011', 'CLAIMED', 'L-b', TRUE);
SELECT lc_task('00000000-0000-7000-8000-00000000d003', 'RUNNING', 'L-c');
SELECT lc_task('00000000-0000-7000-8000-00000000d004', 'CLAIMED', 'L-c');
SELECT lc_task('00000000-0000-7000-8000-00000000d006', 'RUNNING', 'L-c');
SELECT lc_task('00000000-0000-7000-8000-00000000d008', 'RUNNING', 'L-c');
SELECT lc_task('00000000-0000-7000-8000-00000000d005', 'PENDING');
SELECT lc_task('00000000-0000-7000-8000-00000000d007', 'RUNNING', 'L-e');
SELECT lc_task('00000000-0000-7000-8000-00000000d012', 'CLAIMED', 'L-e');
UPDATE horsies_tasks SET claimed_by_worker_id = 'L-d' WHERE id = '00000000-0000-7000-8000-00000000d005';
UPDATE horsies_tasks SET claimed_at = now()
WHERE id IN ('00000000-0000-7000-8000-00000000d010', '00000000-0000-7000-8000-00000000d006');
UPDATE horsies_tasks SET finalizing_at = now(), finalizing_by_worker_id = 'L-c'
WHERE id = '00000000-0000-7000-8000-00000000d003';
INSERT INTO horsies_heartbeats (task_id, sender_id, role, sent_at, hostname, pid) VALUES
    ('00000000-0000-7000-8000-00000000d003', 'L-c', 'runner', now(), 'host-a', 7),
    ('00000000-0000-7000-8000-00000000d009', 'L-b', 'claimer', now(), 'host-a', 7);
SELECT pg_advisory_lock(horsies_worker_liveness_key('L-a'));

SELECT lc_expect(
    'unheld owners: each owner of CLAIMED and RUNNING rows whose lock no session holds, once',
    (SELECT string_agg(worker_id, ',' ORDER BY worker_id COLLATE "C") FROM horsies_unheld_worker_owners(10000)
     WHERE worker_id LIKE 'L-%'),
    'L-b,L-c,L-e'
);
SELECT lc_expect(
    'unheld owners: the batch bound takes the first owners in worker id order',
    (SELECT string_agg(worker_id, ',') FROM horsies_unheld_worker_owners(2)),
    'L-b,L-c'
);
SELECT lc_expect(
    'unheld owners: db_now is the clock of the read',
    (SELECT bool_and(db_now >= now())::text FROM horsies_unheld_worker_owners(10000)),
    'true'
);
SELECT lc_expect_sqlstate('unheld owners: a batch of 0', $q$SELECT * FROM horsies_unheld_worker_owners(0)$q$,
    '22023');
SELECT lc_expect_sqlstate('unheld owners: a batch over 10000',
    $q$SELECT * FROM horsies_unheld_worker_owners(10001)$q$, '22023');
SELECT pg_advisory_unlock(horsies_worker_liveness_key('L-a'));
SELECT lc_expect(
    'unheld owners: a released lock leaves its owner unheld',
    (SELECT string_agg(worker_id, ',' ORDER BY worker_id COLLATE "C") FROM horsies_unheld_worker_owners(10000)
     WHERE worker_id LIKE 'L-%'),
    'L-a,L-b,L-c,L-e'
);

-- The liveness generation: horsies_take_worker_liveness, the generation of horsies_unheld_worker_owners,
-- horsies_worker_liveness_generations and horsies_forget_ended_worker_liveness. Owners M-a to M-d sort after the L
-- owners and before the w owners (C collation); the block deletes its rows and releases its locks at its end. The
-- answer of a lock that another session holds is a two-session case (baseline_lifecycle_functions.rs).
SELECT lc_expect(
    'take liveness: a free lock is taken',
    horsies_take_worker_liveness('M-a')::text,
    'true'
);
CREATE TEMP TABLE lc_liveness_first AS
SELECT generation FROM horsies_worker_liveness WHERE worker_id = 'M-a';
SELECT lc_expect(
    'take liveness: the acquisition writes one generation for the worker',
    (SELECT count(*)::text FROM lc_liveness_first),
    '1'
);
SELECT lc_expect(
    'take liveness: the session holds the lock of the worker',
    (SELECT count(*)::text FROM pg_locks l
     WHERE l.locktype = 'advisory' AND l.granted AND l.objsubid = 1 AND l.pid = pg_backend_pid()
       AND ((l.classid::bigint << 32) | l.objid::bigint) = horsies_worker_liveness_key('M-a')),
    '1'
);
SELECT lc_expect(
    'take liveness: a lock that this session holds is taken again',
    horsies_take_worker_liveness('M-a')::text,
    'true'
);
SELECT lc_expect(
    'take liveness: each acquisition writes a new, larger generation',
    ((SELECT r.generation FROM horsies_worker_liveness r WHERE r.worker_id = 'M-a')
     > (SELECT f.generation FROM lc_liveness_first f))::text,
    'true'
);
SELECT lc_expect_sqlstate('take liveness: a NULL worker id',
    $q$SELECT horsies_take_worker_liveness(NULL)$q$, '22023');
-- The session took the lock of M-a twice: two releases free it.
SELECT pg_advisory_unlock(horsies_worker_liveness_key('M-a'));
SELECT pg_advisory_unlock(horsies_worker_liveness_key('M-a'));

-- e001: RUNNING of M-a (a recorded generation, lock free). e002: CLAIMED of M-b (no liveness row).
SELECT lc_task('00000000-0000-7000-8000-00000000e001', 'RUNNING', 'M-a');
SELECT lc_task('00000000-0000-7000-8000-00000000e002', 'CLAIMED', 'M-b');
SELECT lc_expect(
    'unheld owners: the generation of the latest acquisition; NULL for an owner with no liveness row',
    (SELECT string_agg(
         u.worker_id || ':'
             || COALESCE((u.generation = (SELECT r.generation FROM horsies_worker_liveness r
                                          WHERE r.worker_id = u.worker_id))::text, 'null'),
         ',' ORDER BY u.worker_id COLLATE "C")
     FROM horsies_unheld_worker_owners(10000) u WHERE u.worker_id LIKE 'M-%'),
    'M-a:true,M-b:null'
);
SELECT lc_expect(
    'liveness generations: the current generation of each given worker that has a row, once',
    (SELECT string_agg(
         g.worker_id || ':'
             || (g.generation = (SELECT r.generation FROM horsies_worker_liveness r
                                 WHERE r.worker_id = g.worker_id))::text,
         ',' ORDER BY g.worker_id COLLATE "C")
     FROM horsies_worker_liveness_generations(ARRAY['M-b', 'M-a', 'M-a']) g),
    'M-a:true'
);
SELECT lc_expect(
    'liveness generations: an empty list returns no row',
    (SELECT count(*)::text FROM horsies_worker_liveness_generations('{}'::text[])),
    '0'
);
SELECT lc_expect_sqlstate('liveness generations: a NULL list',
    $q$SELECT * FROM horsies_worker_liveness_generations(NULL)$q$, '22023');
SELECT lc_expect_sqlstate('liveness generations: more than 10000 ids',
    $q$SELECT * FROM horsies_worker_liveness_generations(array_fill('M-a'::text, ARRAY[10001]))$q$, '22023');

-- M-c and M-d own no row and their locks are free; M-b takes its lock and holds it; M-a owns e001.
SELECT horsies_take_worker_liveness('M-c');
SELECT horsies_take_worker_liveness('M-d');
SELECT pg_advisory_unlock(horsies_worker_liveness_key('M-c'));
SELECT pg_advisory_unlock(horsies_worker_liveness_key('M-d'));
SELECT horsies_take_worker_liveness('M-b');
DELETE FROM horsies_tasks WHERE id = '00000000-0000-7000-8000-00000000e002';
SELECT lc_expect(
    'forget liveness: the batch bound counts the deleted rows',
    horsies_forget_ended_worker_liveness(1)::text,
    '1'
);
SELECT lc_expect(
    'forget liveness: the batch takes the first ended worker in worker id order',
    (SELECT string_agg(r.worker_id, ',' ORDER BY r.worker_id COLLATE "C")
     FROM horsies_worker_liveness r WHERE r.worker_id LIKE 'M-%'),
    'M-a,M-b,M-d'
);
SELECT lc_expect(
    'forget liveness: a whole batch deletes every ended worker',
    horsies_forget_ended_worker_liveness(10000)::text,
    '1'
);
SELECT lc_expect(
    'forget liveness: a worker that owns a CLAIMED or RUNNING row, or whose lock is held, keeps its row',
    (SELECT string_agg(r.worker_id, ',' ORDER BY r.worker_id COLLATE "C")
     FROM horsies_worker_liveness r WHERE r.worker_id LIKE 'M-%'),
    'M-a,M-b'
);
SELECT lc_expect_sqlstate('forget liveness: a batch of 0',
    $q$SELECT horsies_forget_ended_worker_liveness(0)$q$, '22023');
SELECT lc_expect_sqlstate('forget liveness: a batch over 10000',
    $q$SELECT horsies_forget_ended_worker_liveness(10001)$q$, '22023');
SELECT pg_advisory_unlock(horsies_worker_liveness_key('M-b'));
DELETE FROM horsies_tasks WHERE id = '00000000-0000-7000-8000-00000000e001';
DELETE FROM horsies_worker_liveness WHERE worker_id LIKE 'M-%';
DROP TABLE lc_liveness_first;

SELECT lc_expect(
    'stale scan OWNER_GONE: the started rows of the owner claimed before the bound, whatever their beats',
    (SELECT string_agg(right(task_id::text, 4) || ':' || (claimed_at = now() - interval '1 second')::text, ','
                       ORDER BY task_id)
     FROM horsies_stale_running_scan('OWNER_GONE', NULL, NULL, 'L-c', now(), 100)),
    'd003:true,d008:true'
);
SELECT lc_expect(
    'stale scan OWNER_GONE: the batch bound',
    (SELECT count(*)::text FROM horsies_stale_running_scan('OWNER_GONE', NULL, NULL, 'L-c', now(), 1)),
    '1'
);
SELECT lc_expect_sqlstate('stale scan: OWNER_GONE with a threshold',
    $q$SELECT * FROM horsies_stale_running_scan('OWNER_GONE', 1, NULL, 'L-c', now(), 10)$q$, '22023');
SELECT lc_expect_sqlstate('stale scan: OWNER_GONE with no claim bound',
    $q$SELECT * FROM horsies_stale_running_scan('OWNER_GONE', NULL, NULL, 'L-c', NULL, 10)$q$, '22023');
SELECT lc_expect_sqlstate('stale scan: BEAT_AGE with an owner',
    $q$SELECT * FROM horsies_stale_running_scan('BEAT_AGE', 1, 1, 'L-c', NULL, 10)$q$, '22023');
SELECT lc_expect_sqlstate('stale scan: an unknown evidence',
    $q$SELECT * FROM horsies_stale_running_scan('HUNCH', 1, 1, NULL, NULL, 10)$q$, '22023');
SELECT lc_expect_sqlstate('stale scan: no evidence',
    $q$SELECT * FROM horsies_stale_running_scan(NULL, 1, 1, NULL, NULL, 10)$q$, '22023');

SELECT lc_expect(
    'stale attempt OWNER_GONE: another claim generation is a lost claim',
    (SELECT concat_ws('|', outcome, observed_worker_id) FROM horsies_finish_stale_attempt(
        '00000000-0000-7000-8000-00000000d003', 'OWNER_GONE', NULL, NULL, 'L-c', now() - interval '2 seconds',
        'NONE', NULL, NULL, '{"err":"gone"}', 'WORKER_CRASHED', 'r', 'm', 'r')),
    'LOST_CLAIM|L-c'
);
SELECT lc_expect(
    'stale attempt OWNER_GONE: another owner is a lost claim',
    (SELECT concat_ws('|', outcome, observed_worker_id) FROM horsies_finish_stale_attempt(
        '00000000-0000-7000-8000-00000000d003', 'OWNER_GONE', NULL, NULL, 'L-b', now() - interval '1 second',
        'NONE', NULL, NULL, '{"err":"gone"}', 'WORKER_CRASHED', 'r', 'm', 'r')),
    'LOST_CLAIM|L-c'
);
SELECT lc_expect(
    'stale attempt OWNER_GONE: a CLAIMED row of the owner is not RUNNING',
    (SELECT outcome || ':' || observed_status FROM horsies_finish_stale_attempt(
        '00000000-0000-7000-8000-00000000d004', 'OWNER_GONE', NULL, NULL, 'L-c', now() - interval '1 second',
        'NONE', NULL, NULL, '{}', 'WORKER_CRASHED', 'r', 'm', 'r')),
    'SOURCE_STATE_CONFLICT:CLAIMED'
);
SELECT lc_expect(
    'stale attempt OWNER_GONE: an absent task',
    (SELECT outcome FROM horsies_finish_stale_attempt(
        '00000000-0000-7000-8000-00000000d0ff', 'OWNER_GONE', NULL, NULL, 'L-c', now() - interval '1 second',
        'NONE', NULL, NULL, '{}', 'WORKER_CRASHED', 'r', 'm', 'r')),
    'TASK_ABSENT'
);
SELECT lc_expect(
    'stale attempt OWNER_GONE: the row of the owner fails, whatever its beat and its finalizing marker',
    (SELECT concat_ws('|', outcome, terminalization_kind, retry_disposition, observed_worker_id)
     FROM horsies_finish_stale_attempt(
        '00000000-0000-7000-8000-00000000d003', 'OWNER_GONE', NULL, NULL, 'L-c', now() - interval '1 second',
        'NONE', NULL, NULL, '{"err":"gone"}', 'WORKER_CRASHED', 'Worker process gone', 'gone message',
        'gone reason')),
    'APPLIED|FAIL_STALE|NOT_REQUESTED|L-c'
);
SELECT lc_expect(
    'stale attempt OWNER_GONE: the history row and the attempt',
    (SELECT concat_ws('|', h.status, h.terminalization_kind, h.error_code, h.final_failed_reason)
     FROM horsies_task_history h WHERE h.task_id = '00000000-0000-7000-8000-00000000d003'),
    'FAILED|FAIL_STALE|WORKER_CRASHED|Worker process gone'
);
SELECT lc_expect(
    'stale attempt OWNER_GONE: a crash retry by policy',
    (SELECT concat_ws('|', outcome, retry_count, queue_name) FROM horsies_finish_stale_attempt(
        '00000000-0000-7000-8000-00000000d008', 'OWNER_GONE', NULL, NULL, 'L-c', now() - interval '1 second',
        'RETRY', 0, 0.0, '{"err":"gone"}', 'WORKER_CRASHED', 'r', 'gone message', 'gone reason')),
    'RETRY_SCHEDULED|1|q_lc'
);
SELECT lc_expect(
    'stale attempt OWNER_GONE: the retried row and its FAILED attempt',
    (SELECT concat_ws('|', t.status, num_nulls(t.claimed_at, t.claimed_by_worker_id, t.started_at), a.attempt,
        a.outcome, a.will_retry, a.error_message, a.failed_reason, a.worker_id)
     FROM horsies_tasks t JOIN horsies_task_attempts a ON a.task_id = t.id
     WHERE t.id = '00000000-0000-7000-8000-00000000d008'),
    'PENDING|3|1|FAILED|t|gone message|gone reason|L-c'
);
SELECT lc_expect_sqlstate('stale attempt: OWNER_GONE with thresholds', $q$SELECT * FROM horsies_finish_stale_attempt(
    gen_random_uuid(), 'OWNER_GONE', 0, 0, 'L-c', now(), 'NONE', NULL, NULL, '{}', 'E', 'r', 'm', 'r')$q$, '22023');
SELECT lc_expect_sqlstate('stale attempt: OWNER_GONE with no claim generation',
    $q$SELECT * FROM horsies_finish_stale_attempt(
    gen_random_uuid(), 'OWNER_GONE', NULL, NULL, 'L-c', NULL, 'NONE', NULL, NULL, '{}', 'E', 'r', 'm', 'r')$q$,
    '22023');
SELECT lc_expect_sqlstate('stale attempt: BEAT_AGE with an owner', $q$SELECT * FROM horsies_finish_stale_attempt(
    gen_random_uuid(), 'BEAT_AGE', 0, 0, 'L-c', NULL, 'NONE', NULL, NULL, '{}', 'E', 'r', 'm', 'r')$q$, '22023');
SELECT lc_expect_sqlstate('stale attempt: an unknown evidence', $q$SELECT * FROM horsies_finish_stale_attempt(
    gen_random_uuid(), 'HUNCH', 0, 0, NULL, NULL, 'NONE', NULL, NULL, '{}', 'E', 'r', 'm', 'r')$q$, '22023');
SELECT lc_expect_sqlstate('stale attempt: no evidence', $q$SELECT * FROM horsies_finish_stale_attempt(
    gen_random_uuid(), NULL, 0, 0, NULL, NULL, 'NONE', NULL, NULL, '{}', 'E', 'r', 'm', 'r')$q$, '22023');

SELECT lc_expect(
    'requeue OWNER_GONE: the batch bound, the rest, then a short batch',
    (SELECT count(*) FROM horsies_requeue_stale_claims('OWNER_GONE', NULL, 'L-b', now(), 1))::text || ','
        || (SELECT count(*) FROM horsies_requeue_stale_claims('OWNER_GONE', NULL, 'L-b', now(), 10))::text || ','
        || (SELECT count(*) FROM horsies_requeue_stale_claims('OWNER_GONE', NULL, 'L-b', now(), 10))::text,
    '1,1,0'
);
SELECT lc_expect(
    'requeue OWNER_GONE: the claims of the owner before the bound go back, whatever their beats; a later claim, an '
        || 'orphan and the claim of another owner stay',
    (SELECT string_agg(right(id::text, 4) || ':' || status, ',' ORDER BY id) FROM horsies_tasks
     WHERE id IN ('00000000-0000-7000-8000-00000000d002', '00000000-0000-7000-8000-00000000d009',
                  '00000000-0000-7000-8000-00000000d010', '00000000-0000-7000-8000-00000000d011',
                  '00000000-0000-7000-8000-00000000d012')),
    'd002:PENDING,d009:PENDING,d010:CLAIMED,d011:CLAIMED,d012:CLAIMED'
);
SELECT lc_expect(
    'requeue OWNER_GONE: a requeued row has every claim and run field cleared',
    (SELECT num_nulls(claimed_at, claimed_by_worker_id, claim_expires_at, started_at, worker_pid, worker_hostname,
                      worker_process_name, finalizing_at, finalizing_by_worker_id)::text
     FROM horsies_tasks WHERE id = '00000000-0000-7000-8000-00000000d002'),
    '9'
);
SELECT lc_expect_sqlstate('requeue: OWNER_GONE with a threshold',
    $q$SELECT horsies_requeue_stale_claims('OWNER_GONE', 1, 'L-b', now(), 1)$q$, '22023');
SELECT lc_expect_sqlstate('requeue: OWNER_GONE with no claim bound',
    $q$SELECT horsies_requeue_stale_claims('OWNER_GONE', NULL, 'L-b', NULL, 1)$q$, '22023');
SELECT lc_expect_sqlstate('requeue: BEAT_AGE with an owner',
    $q$SELECT horsies_requeue_stale_claims('BEAT_AGE', 1, 'L-b', NULL, 1)$q$, '22023');
SELECT lc_expect_sqlstate('requeue: an unknown evidence',
    $q$SELECT horsies_requeue_stale_claims('HUNCH', 1, NULL, NULL, 1)$q$, '22023');
SELECT lc_expect_sqlstate('requeue: no evidence',
    $q$SELECT horsies_requeue_stale_claims(NULL, 1, NULL, NULL, 1)$q$, '22023');

------------------------------------------------------------------------------------------------------------------------
-- horsies_release_claims
------------------------------------------------------------------------------------------------------------------------

SELECT lc_task('00000000-0000-7000-8000-000000005003', 'CLAIMED');
SELECT lc_task('00000000-0000-7000-8000-000000005001', 'CLAIMED');
SELECT lc_task('00000000-0000-7000-8000-000000005002', 'CLAIMED');
SELECT lc_task('00000000-0000-7000-8000-000000005004', 'RUNNING');
UPDATE horsies_tasks SET execution = 'SHARED_ASYNC'
WHERE id BETWEEN '00000000-0000-7000-8000-000000005001' AND '00000000-0000-7000-8000-000000005004';
SELECT lc_expect(
    'release: held rows go back, another generation and a RUNNING row are not held',
    (SELECT string_agg(ordinality || ':' || right(task_id::text, 4) || ':' || outcome, ',' ORDER BY ordinality)
     FROM horsies_release_claims(
        ARRAY['00000000-0000-7000-8000-000000005003', '00000000-0000-7000-8000-000000005001',
              '00000000-0000-7000-8000-000000005002', '00000000-0000-7000-8000-000000005004',
              '00000000-0000-7000-8000-0000000050ff']::uuid[],
        ARRAY[now() - interval '1 second', now() - interval '1 second', now() - interval '5 seconds',
              now() - interval '1 second', now()],
        'w1', 'SAME_CLASS')),
    '1:5003:RELEASED,2:5001:RELEASED,3:5002:NOT_HELD,4:5004:NOT_HELD,5:50ff:NOT_HELD'
);
SELECT lc_expect(
    'release: the released rows keep their class; the others are untouched',
    (SELECT string_agg(concat_ws(':', right(id::text, 4), status, execution,
        num_nulls(claimed_at, claimed_by_worker_id, claim_expires_at)), ',' ORDER BY id)
     FROM horsies_tasks WHERE id BETWEEN '00000000-0000-7000-8000-000000005001'
                                     AND '00000000-0000-7000-8000-000000005004'),
    '5001:PENDING:SHARED_ASYNC:3,5002:CLAIMED:SHARED_ASYNC:0,5003:PENDING:SHARED_ASYNC:3,5004:RUNNING:SHARED_ASYNC:1'
);
SELECT lc_expect(
    'release: ISOLATED placement',
    (SELECT outcome FROM horsies_release_claims(
        ARRAY['00000000-0000-7000-8000-000000005002']::uuid[], ARRAY[now() - interval '1 second'], 'w1',
        'ISOLATED')),
    'RELEASED'
);
SELECT lc_expect(
    'release: the row returned as ISOLATED',
    (SELECT status || ':' || execution FROM horsies_tasks WHERE id = '00000000-0000-7000-8000-000000005002'),
    'PENDING:ISOLATED'
);
SELECT lc_expect_sqlstate('release: arrays of different lengths', $q$SELECT * FROM horsies_release_claims(
    ARRAY[gen_random_uuid()], ARRAY[now(), now()], 'w1', 'SAME_CLASS')$q$, '22023');
SELECT lc_expect_sqlstate('release: a repeated id', $q$SELECT * FROM horsies_release_claims(
    ARRAY['00000000-0000-7000-8000-000000005001', '00000000-0000-7000-8000-000000005001']::uuid[],
    ARRAY[now(), now()], 'w1', 'SAME_CLASS')$q$, '22023');
SELECT lc_expect_sqlstate('release: a NULL claim generation', $q$SELECT * FROM horsies_release_claims(
    ARRAY[gen_random_uuid()], ARRAY[NULL]::timestamptz[], 'w1', 'SAME_CLASS')$q$, '22023');
SELECT lc_expect_sqlstate('release: an unknown placement', $q$SELECT * FROM horsies_release_claims(
    ARRAY[gen_random_uuid()], ARRAY[now()], 'w1', 'ANYWHERE')$q$, '22023');
SELECT lc_expect_sqlstate('release: more than 1000 rows', $q$SELECT * FROM horsies_release_claims(
    ARRAY(SELECT gen_random_uuid() FROM generate_series(1, 1001)),
    ARRAY(SELECT now() FROM generate_series(1, 1001)), 'w1', 'SAME_CLASS')$q$, '22023');

------------------------------------------------------------------------------------------------------------------------
-- horsies_cancel_task
------------------------------------------------------------------------------------------------------------------------

SELECT lc_task('00000000-0000-7000-8000-000000006001', 'PENDING');
SELECT lc_task('00000000-0000-7000-8000-000000006002', 'CLAIMED');
SELECT lc_task('00000000-0000-7000-8000-000000006003', 'RUNNING');
SELECT lc_task('00000000-0000-7000-8000-000000006004', 'RUNNING');
SELECT lc_expect(
    'cancel: a PENDING task',
    (SELECT concat_ws('|', outcome, terminalization_kind, observed_status) FROM horsies_cancel_task(
        '00000000-0000-7000-8000-000000006001', FALSE)),
    'APPLIED|CANCEL_ADMIN|PENDING'
);
SELECT lc_expect(
    'cancel: the history row has the admin values and no result',
    (SELECT concat_ws('|', status, error_code, final_failed_reason, result_payload IS NULL)
     FROM horsies_task_history WHERE task_id = '00000000-0000-7000-8000-000000006001'),
    'CANCELLED|TASK_CANCELLED|Cancelled via monitoring API|t'
);
SELECT lc_expect(
    'cancel: a CLAIMED task',
    (SELECT concat_ws('|', outcome, observed_status, observed_worker_id) FROM horsies_cancel_task(
        '00000000-0000-7000-8000-000000006002', FALSE)),
    'APPLIED|CLAIMED|w1'
);
SELECT lc_expect(
    'cancel: a RUNNING task needs the caller''s permission',
    (SELECT concat_ws('|', outcome, observed_status, coalesce(guard_kind, 'none')) FROM horsies_cancel_task(
        '00000000-0000-7000-8000-000000006003', FALSE)),
    'SOURCE_STATE_CONFLICT|RUNNING|none'
);
SELECT lc_expect(
    'cancel: a refused RUNNING task stays live',
    (SELECT status FROM horsies_tasks WHERE id = '00000000-0000-7000-8000-000000006003'),
    'RUNNING'
);
SELECT lc_expect(
    'cancel: a RUNNING task with permission',
    (SELECT outcome || ':' || observed_status FROM horsies_cancel_task('00000000-0000-7000-8000-000000006004', TRUE)),
    'APPLIED:RUNNING'
);
SELECT lc_expect(
    'cancel: a second cancel is already applied',
    (SELECT concat_ws('|', outcome, observed_status, terminalization_kind) FROM horsies_cancel_task(
        '00000000-0000-7000-8000-000000006001', TRUE)),
    'ALREADY_APPLIED|CANCELLED|CANCEL_ADMIN'
);
SELECT lc_expect(
    'cancel: a task that ended another way',
    (SELECT concat_ws('|', outcome, observed_status, guard_kind) FROM horsies_cancel_task(
        '00000000-0000-7000-8000-000000001011', TRUE)),
    'SOURCE_STATE_CONFLICT|COMPLETED|FOREIGN_TERMINALIZATION'
);
SELECT lc_task('00000000-0000-7000-8000-000000006005', 'PENDING', NULL, TRUE);
SELECT lc_expect(
    'cancel: a live workflow task is refused',
    (SELECT concat_ws('|', outcome, observed_status, guard_kind) FROM horsies_cancel_task(
        '00000000-0000-7000-8000-000000006005', TRUE)),
    'SOURCE_STATE_CONFLICT|PENDING|WORKFLOW_TASK'
);
SELECT lc_expect(
    'cancel: a finished workflow task is refused too',
    (SELECT concat_ws('|', outcome, observed_status, guard_kind) FROM horsies_cancel_task(
        '00000000-0000-7000-8000-000000001021', TRUE)),
    'SOURCE_STATE_CONFLICT|COMPLETED|WORKFLOW_TASK'
);
SELECT lc_expect(
    'cancel: an absent task',
    (SELECT outcome FROM horsies_cancel_task('00000000-0000-7000-8000-0000000060ff', TRUE)),
    'TASK_ABSENT'
);
SELECT lc_expect_sqlstate('cancel: a NULL running choice', $q$SELECT * FROM horsies_cancel_task(
    gen_random_uuid(), NULL)$q$, '22023');

------------------------------------------------------------------------------------------------------------------------
-- The claim generation of the interim terminal functions (#697) and the history kind vocabulary (#695)
------------------------------------------------------------------------------------------------------------------------

-- Every owned-claim function refuses a NULL generation before any lock; the batch functions refuse a NULL element.
SELECT lc_expect_sqlstate('generation (#697): abandon_owned_node refuses NULL', $q$SELECT * FROM horsies_abandon_owned_node(
    gen_random_uuid(), 'w1', NULL)$q$, '22023');
SELECT lc_expect_sqlstate('generation (#697): cancel_owned_node refuses NULL', $q$SELECT * FROM horsies_cancel_owned_node(
    gen_random_uuid(), 'w1', NULL, TRUE)$q$, '22023');
SELECT lc_expect_sqlstate('generation (#697): cancel_owned_orphan refuses NULL', $q$SELECT * FROM horsies_cancel_owned_orphan(
    gen_random_uuid(), 'w1', NULL)$q$, '22023');
SELECT lc_expect_sqlstate('generation (#697): abandon_owned_nodes refuses a NULL element', $q$
    SELECT * FROM horsies_abandon_owned_nodes(ARRAY[gen_random_uuid()], ARRAY[NULL::timestamptz], 'w1')$q$, '22023');
SELECT lc_expect_sqlstate('generation (#697): cancel_owned_nodes refuses a NULL element', $q$
    SELECT * FROM horsies_cancel_owned_nodes(ARRAY[gen_random_uuid()], ARRAY[NULL::timestamptz], 'w1')$q$, '22023');

-- A claim of another generation is never ended by a stale dispatch of the same worker.
SELECT lc_task('00000000-0000-7000-8000-000000007001', 'CLAIMED', 'w1', TRUE);
SELECT lc_expect(
    'generation (#697): cancel_owned_orphan of another generation is a lost claim',
    (SELECT outcome FROM horsies_cancel_owned_orphan(
        '00000000-0000-7000-8000-000000007001', 'w1', now() - interval '9 seconds')),
    'LOST_CLAIM'
);
SELECT lc_expect(
    'generation (#697): the orphan of the current generation is cancelled',
    (SELECT outcome FROM horsies_cancel_owned_orphan(
        '00000000-0000-7000-8000-000000007001', 'w1', now() - interval '1 second')),
    'APPLIED'
);

-- The miss classifier (0008) takes the caller's kind and its fence as a closed kind.
SELECT lc_task('00000000-0000-7000-8000-000000007002', 'CLAIMED');
SELECT lc_expect(
    'miss (#697): OWNER held by the same worker at another generation is a conflict, not a lost claim',
    (SELECT outcome FROM horsies_terminalization_miss(
        '00000000-0000-7000-8000-000000007002', 'EXPIRE_CLAIMED', 'OWNER', 'w1', NULL)),
    'SOURCE_STATE_CONFLICT'
);
SELECT lc_expect(
    'miss (#697): OWNER of another worker is a lost claim',
    (SELECT outcome FROM horsies_terminalization_miss(
        '00000000-0000-7000-8000-000000007002', 'EXPIRE_CLAIMED', 'OWNER', 'w2', NULL)),
    'LOST_CLAIM'
);
SELECT lc_expect(
    'miss (#697): OWNER_AND_GENERATION of another generation is a lost claim',
    (SELECT outcome FROM horsies_terminalization_miss(
        '00000000-0000-7000-8000-000000007002', 'FAIL_RUNNING', 'OWNER_AND_GENERATION', 'w1',
        now() - interval '9 seconds')),
    'LOST_CLAIM'
);
SELECT lc_expect(
    'miss (#697): OWNER_AND_GENERATION that holds is a conflict',
    (SELECT outcome FROM horsies_terminalization_miss(
        '00000000-0000-7000-8000-000000007002', 'FAIL_RUNNING', 'OWNER_AND_GENERATION', 'w1',
        now() - interval '1 second')),
    'SOURCE_STATE_CONFLICT'
);
SELECT lc_expect(
    'miss (#697): NONE on a live row is a conflict',
    (SELECT outcome FROM horsies_terminalization_miss(
        '00000000-0000-7000-8000-000000007002', 'CANCEL_ADMIN', 'NONE', NULL, NULL)),
    'SOURCE_STATE_CONFLICT'
);
SELECT lc_expect_sqlstate('miss (#697): an unknown fence kind', $q$SELECT * FROM horsies_terminalization_miss(
    gen_random_uuid(), 'FAIL_RUNNING', 'WORKER', 'w1', NULL)$q$, '22023');
SELECT lc_expect_sqlstate('miss (#697): a NULL fence kind', $q$SELECT * FROM horsies_terminalization_miss(
    gen_random_uuid(), 'FAIL_RUNNING', NULL, NULL, NULL)$q$, '22023');
SELECT lc_expect_sqlstate('miss (#697): NONE with a worker', $q$SELECT * FROM horsies_terminalization_miss(
    gen_random_uuid(), 'FAIL_RUNNING', 'NONE', 'w1', NULL)$q$, '22023');
SELECT lc_expect_sqlstate('miss (#697): OWNER with a generation', $q$SELECT * FROM horsies_terminalization_miss(
    gen_random_uuid(), 'FAIL_RUNNING', 'OWNER', 'w1', now())$q$, '22023');
SELECT lc_expect_sqlstate('miss (#697): OWNER_AND_GENERATION without a generation', $q$
    SELECT * FROM horsies_terminalization_miss(
        gen_random_uuid(), 'FAIL_RUNNING', 'OWNER_AND_GENERATION', 'w1', NULL)$q$, '22023');

-- The owner-only expiry before start: a miss on a claim the worker still holds is a conflict.
SELECT lc_task('00000000-0000-7000-8000-000000007003', 'CLAIMED');
UPDATE horsies_tasks SET status = 'RUNNING', claim_expires_at = NULL, started_at = now(), worker_pid = 1,
    worker_hostname = 'h', worker_process_name = 'p'
WHERE id = '00000000-0000-7000-8000-000000007003';
SELECT lc_expect(
    'expiry (#697): a held row that is not CLAIMED is a conflict',
    (SELECT outcome FROM horsies_expire_owned_claim('00000000-0000-7000-8000-000000007003', 'w1', '{}', 'E')),
    'SOURCE_STATE_CONFLICT'
);
SELECT lc_expect(
    'expiry (#697): a row of another worker is a lost claim',
    (SELECT outcome FROM horsies_expire_owned_claim('00000000-0000-7000-8000-000000007003', 'w2', '{}', 'E')),
    'LOST_CLAIM'
);

-- History rows take only the 15 kinds (#695); every leaf inherits the CHECK.
CREATE FUNCTION lc_history_row(p_kind text) RETURNS void LANGUAGE sql AS $$
    INSERT INTO horsies_task_history (
        task_id, task_name, queue_name, priority, command_fingerprint_version, command_fingerprint,
        retention_class_key, is_workflow_task, workflow_id, rerun_of_task_id, rerun_root_task_id, sent_at, created_at,
        enqueued_at, claimed_at, started_at, good_until, terminal_at, status, terminalization_kind, retry_count,
        max_retries, last_claimed_worker_id, last_worker_hostname, last_worker_pid, last_worker_process_name,
        error_code, final_failed_reason, result_envelope_version, result_payload, result_digest,
        history_schema_version, attempt_archive_version, attempt_snapshot, attempt_snapshot_digest,
        rerun_input_disposition, rerun_input_version, rerun_input_digest, rerun_input_inline
    ) VALUES (
        gen_random_uuid(), 'lc.task', 'q_lc', 100, 1, sha256('k'::bytea),
        'forever', FALSE, NULL, NULL, NULL, now(), now(),
        now(), NULL, NULL, NULL, now(), 'CANCELLED', p_kind, 0,
        0, NULL, NULL, NULL, NULL,
        'TASK_CANCELLED', NULL, 1, NULL, NULL,
        1, 1, '\x5b5d'::bytea, sha256('\x5b5d'::bytea),
        'NEVER_ELIGIBLE', NULL, NULL, NULL
    )
$$;
SELECT lc_expect_sqlstate('history (#695): a kind outside the vocabulary',
    $q$SELECT lc_history_row('NOT_A_KIND')$q$, '23514');
SELECT lc_history_row('CANCEL_ORPHAN_SWEEP');
SELECT lc_expect(
    'history (#695): a kind of the vocabulary is stored',
    (SELECT count(*)::text FROM horsies_task_history_forever_open WHERE terminalization_kind = 'CANCEL_ORPHAN_SWEEP'),
    '1'
);
SELECT lc_expect(
    'history (#695): the CHECK lists the 15 kinds',
    (SELECT (
        SELECT count(*) FROM unnest(ARRAY[
            'COMPLETE_LOCKED', 'COMPLETE_FUSED', 'FAIL_RUNNING', 'FAIL_STALE', 'EXPIRE_CLAIMED', 'EXPIRE_PENDING',
            'CANCEL_ADMIN', 'CANCEL_ORPHAN', 'CANCEL_ORPHAN_SWEEP', 'PAUSE_ABANDON_CLAIM', 'PAUSE_ABANDON_CLAIM_BATCH',
            'PAUSE_ABANDON_WORKFLOW', 'WORKFLOW_CANCEL_CLAIM', 'WORKFLOW_CANCEL_CLAIM_BATCH', 'WORKFLOW_CANCEL_WORKFLOW'
        ]) AS k(kind)
        WHERE position('''' || k.kind || '''' IN pg_get_constraintdef(c.oid)) > 0
    )::text
     FROM pg_constraint c
     WHERE c.conname = 'horsies_task_history_kind_known' AND c.conrelid = 'horsies_task_history'::regclass),
    '15'
);
SELECT lc_expect(
    'history (#695): the forever leaf inherits the CHECK',
    (SELECT count(*)::text FROM pg_constraint c
     WHERE c.conname = 'horsies_task_history_kind_known'
       AND c.conrelid = 'horsies_task_history_forever_open'::regclass),
    '1'
);

------------------------------------------------------------------------------------------------------------------------
-- Expiry and orphan cancel: horsies_expire_owned_claim, horsies_expire_pending_tasks, horsies_cancel_owned_orphan,
-- horsies_cancel_orphaned_tasks
------------------------------------------------------------------------------------------------------------------------

SELECT lc_expect_sqlstate('expire claim: a NULL task', $q$SELECT * FROM horsies_expire_owned_claim(
    NULL, 'w1', '{}', 'E')$q$, '22023');
SELECT lc_expect_sqlstate('expire claim: a NULL worker', $q$SELECT * FROM horsies_expire_owned_claim(
    gen_random_uuid(), NULL, '{}', 'E')$q$, '22023');
SELECT lc_expect_sqlstate('expire claim: a NULL error code', $q$SELECT * FROM horsies_expire_owned_claim(
    gen_random_uuid(), 'w1', '{}', NULL)$q$, '22023');
SELECT lc_task('00000000-0000-7000-8000-000000009001', 'CLAIMED');
UPDATE horsies_tasks SET good_until = now() WHERE id = '00000000-0000-7000-8000-000000009001';
SELECT lc_expect(
    'expire claim: a passed deadline of this worker expires the row, at any claim generation',
    (SELECT concat_ws('|', outcome, terminalization_kind, observed_status, observed_worker_id,
        observed_claimed_at = now() - interval '1 second', terminal_at = now(), ordinality IS NULL,
        pg_typeof(observed_worker_id)::text)
     FROM horsies_expire_owned_claim('00000000-0000-7000-8000-000000009001', 'w1', '{"e":1}', 'TASK_EXPIRED')),
    'APPLIED|EXPIRE_CLAIMED|CLAIMED|w1|t|t|t|text'
);
SELECT lc_expect(
    'expire claim: the row is in history as EXPIRED, not live',
    (SELECT concat_ws('|', h.status, h.terminalization_kind, h.error_code,
        (SELECT count(*) FROM horsies_tasks WHERE id = h.task_id))
     FROM horsies_task_history h WHERE h.task_id = '00000000-0000-7000-8000-000000009001'),
    'EXPIRED|EXPIRE_CLAIMED|TASK_EXPIRED|0'
);
SELECT lc_task('00000000-0000-7000-8000-000000009002', 'CLAIMED');
UPDATE horsies_tasks SET good_until = now() + interval '1 hour' WHERE id = '00000000-0000-7000-8000-000000009002';
SELECT lc_expect(
    'expire claim: a deadline not passed is a DEADLINE conflict with its evidence',
    (SELECT concat_ws('|', outcome, guard_kind, observed_status,
        (observed_guard->>'good_until')::timestamptz = now() + interval '1 hour',
        (observed_guard->>'evaluated_at')::timestamptz = now())
     FROM horsies_expire_owned_claim('00000000-0000-7000-8000-000000009002', 'w1', '{}', 'E')),
    'SOURCE_STATE_CONFLICT|DEADLINE|CLAIMED|t|t'
);
SELECT lc_expect(
    'expire claim: an absent task',
    (SELECT outcome FROM horsies_expire_owned_claim('00000000-0000-7000-8000-0000000090ff', 'w1', '{}', 'E')),
    'TASK_ABSENT'
);

SELECT lc_expect_sqlstate('expire pending: a batch of 0', $q$SELECT * FROM horsies_expire_pending_tasks(
    0, '{}', 'E')$q$, '22023');
SELECT lc_expect_sqlstate('expire pending: a NULL batch', $q$SELECT * FROM horsies_expire_pending_tasks(
    NULL, '{}', 'E')$q$, '22023');
SELECT lc_expect_sqlstate('expire pending: a NULL error code', $q$SELECT * FROM horsies_expire_pending_tasks(
    10, '{}', NULL)$q$, '22023');
SELECT lc_task('00000000-0000-7000-8000-000000009011', 'PENDING');
SELECT lc_task('00000000-0000-7000-8000-000000009012', 'PENDING');
SELECT lc_task('00000000-0000-7000-8000-000000009013', 'PENDING');
UPDATE horsies_tasks SET good_until = now() - interval '1 second' WHERE id = '00000000-0000-7000-8000-000000009011';
UPDATE horsies_tasks SET good_until = now() - interval '2 seconds' WHERE id = '00000000-0000-7000-8000-000000009012';
UPDATE horsies_tasks SET good_until = now() + interval '1 hour' WHERE id = '00000000-0000-7000-8000-000000009013';
CREATE TEMP TABLE lc_expired AS
    SELECT row_number() OVER () AS position, e.*
    FROM horsies_expire_pending_tasks(100, '{"e":"expired"}', 'TASK_EXPIRED') AS e;
SELECT lc_expect(
    'expire pending: the due rows, earliest deadline first, with their kind and status',
    (SELECT string_agg(concat_ws('|', task_id, outcome, terminalization_kind, observed_status,
        terminal_at = now(), pg_typeof(observed_worker_id)::text), ',' ORDER BY position)
     FROM lc_expired
     WHERE task_id IN ('00000000-0000-7000-8000-000000009011', '00000000-0000-7000-8000-000000009012')),
    '00000000-0000-7000-8000-000000009012|APPLIED|EXPIRE_PENDING|PENDING|t|text,'
        || '00000000-0000-7000-8000-000000009011|APPLIED|EXPIRE_PENDING|PENDING|t|text'
);
SELECT lc_expect(
    'expire pending: each row names the task name and the queue of the moved row',
    (SELECT string_agg(concat_ws('|', right(task_id::text, 4), task_name, queue_name), ',' ORDER BY position)
     FROM lc_expired
     WHERE task_id IN ('00000000-0000-7000-8000-000000009011', '00000000-0000-7000-8000-000000009012')),
    '9012|lc.task|q_lc,9011|lc.task|q_lc'
);
SELECT lc_expect(
    'expire pending: a row not due stays PENDING',
    (SELECT status FROM horsies_tasks WHERE id = '00000000-0000-7000-8000-000000009013'),
    'PENDING'
);
SELECT lc_expect(
    'expire pending: the due rows are in history as EXPIRED',
    (SELECT string_agg(h.status || ':' || h.error_code, ',' ORDER BY h.task_id)
     FROM horsies_task_history h
     WHERE h.task_id IN ('00000000-0000-7000-8000-000000009011', '00000000-0000-7000-8000-000000009012')),
    'EXPIRED:TASK_EXPIRED,EXPIRED:TASK_EXPIRED'
);

SELECT lc_expect_sqlstate('cancel orphan: a NULL worker', $q$SELECT * FROM horsies_cancel_owned_orphan(
    gen_random_uuid(), NULL, now())$q$, '22023');
SELECT lc_expect_sqlstate('cancel orphan: a NULL task', $q$SELECT * FROM horsies_cancel_owned_orphan(
    NULL, 'w1', now())$q$, '22023');
SELECT lc_task('00000000-0000-7000-8000-000000009021', 'CLAIMED', 'w1', TRUE);
SELECT lc_expect(
    'cancel orphan: a held workflow task with no node is cancelled',
    (SELECT concat_ws('|', outcome, terminalization_kind, observed_status, observed_worker_id,
        pg_typeof(observed_worker_id)::text)
     FROM horsies_cancel_owned_orphan(
        '00000000-0000-7000-8000-000000009021', 'w1', now() - interval '1 second')),
    'APPLIED|CANCEL_ORPHAN|CLAIMED|w1|text'
);
SELECT lc_expect(
    'cancel orphan: the row is in history as CANCELLED with WORKFLOW_CHECK_FAILED',
    (SELECT h.status || ':' || h.error_code FROM horsies_task_history h
     WHERE h.task_id = '00000000-0000-7000-8000-000000009021'),
    'CANCELLED:WORKFLOW_CHECK_FAILED'
);
SELECT lc_workflow('00000000-0000-7000-8000-000000009e01', 'RUNNING');
SELECT lc_task('00000000-0000-7000-8000-000000009022', 'CLAIMED', 'w1', TRUE);
SELECT lc_node('00000000-0000-7000-8000-000000009f01', '00000000-0000-7000-8000-000000009e01', 0, 'ENQUEUED',
               '00000000-0000-7000-8000-000000009022');
SELECT lc_expect(
    'cancel orphan: a runnable node link is a WORKFLOW_LINK_STATE conflict with the node status',
    (SELECT concat_ws('|', outcome, guard_kind, observed_guard->>'node_status')
     FROM horsies_cancel_owned_orphan(
        '00000000-0000-7000-8000-000000009022', 'w1', now() - interval '1 second')),
    'SOURCE_STATE_CONFLICT|WORKFLOW_LINK_STATE|ENQUEUED'
);

SELECT lc_expect_sqlstate('orphan sweep: a batch of 0', $q$SELECT * FROM horsies_cancel_orphaned_tasks(0)$q$,
    '22023');
SELECT lc_task('00000000-0000-7000-8000-000000009031', 'PENDING', 'w1', TRUE);
CREATE TEMP TABLE lc_swept AS SELECT s.* FROM horsies_cancel_orphaned_tasks(1000) AS s;
SELECT lc_expect(
    'orphan sweep: a PENDING workflow task with no node is cancelled',
    (SELECT concat_ws('|', outcome, terminalization_kind, observed_status, pg_typeof(observed_worker_id)::text)
     FROM lc_swept WHERE task_id = '00000000-0000-7000-8000-000000009031'),
    'APPLIED|CANCEL_ORPHAN_SWEEP|PENDING|text'
);
SELECT lc_expect(
    'orphan sweep: each row names the task name and the queue of the moved row',
    (SELECT concat_ws('|', task_name, queue_name) FROM lc_swept
     WHERE task_id = '00000000-0000-7000-8000-000000009031'),
    'lc.task|q_lc'
);
SELECT lc_expect(
    'orphan sweep: a workflow task with a runnable node stays live',
    (SELECT count(*)::text FROM lc_swept WHERE task_id = '00000000-0000-7000-8000-000000009022'),
    '0'
);
SELECT lc_expect(
    'orphan sweep: the cursor records the page and completes the cycle',
    (SELECT concat_ws('|', last_candidate_rows = (SELECT count(*) FROM lc_swept), last_id IS NULL,
        completed_cycles >= 1, last_scan_at >= now())
     FROM horsies_recovery_scan_cursors WHERE scan_name = 'orphan_workflow_tasks'),
    't|t|t|t'
);
