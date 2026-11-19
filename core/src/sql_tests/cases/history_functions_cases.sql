-- Cases for core/baseline/0006_history_functions.sql, one session.
--
-- Run on a database that holds the baseline migration, in one transaction that the caller rolls back:
--   psql -v ON_ERROR_STOP=1 --single-transaction -f history_functions_cases.sql
-- Each case prints "ok: <case>"; a failed case raises and stops the script. The two-session cases are in
-- history_functions_concurrency.sql.
--
-- The published history readers are rendered at run time by the reader publication. The stand-ins below probe the
-- history parent instead of each leaf: same answers, other plans.

SET TimeZone = 'UTC';
SET client_min_messages = notice;

CREATE FUNCTION horsies_task_provenance_staged(p_task_id uuid)
RETURNS TABLE (
    task_id uuid, status text, terminal_at timestamptz, terminalization_kind text, is_workflow_task boolean,
    command_fingerprint_version smallint, command_fingerprint bytea, sent_at timestamptz
)
LANGUAGE sql STABLE AS $$
    SELECT h.task_id, h.status::text, h.terminal_at, h.terminalization_kind::text, h.is_workflow_task,
        h.command_fingerprint_version, h.command_fingerprint, h.sent_at
    FROM horsies_task_history AS h WHERE h.task_id = p_task_id LIMIT 1
$$;

CREATE FUNCTION horsies_task_history_duplicate_staged(p_task_id uuid)
RETURNS boolean LANGUAGE sql STABLE AS $$
    SELECT EXISTS (SELECT 1 FROM horsies_task_provenance_staged(p_task_id))
        OR EXISTS (SELECT 1 FROM horsies_task_history_clock_exceptions AS e WHERE e.task_id = p_task_id)
$$;

CREATE FUNCTION horsies_task_detail_staged(p_task_id uuid)
RETURNS TABLE (location text, task_row horsies_task_history) LANGUAGE plpgsql STABLE AS $$
DECLARE
    v_row horsies_task_history;
BEGIN
    IF EXISTS (SELECT 1 FROM horsies_tasks AS t WHERE t.id = p_task_id) THEN
        RETURN QUERY SELECT 'LIVE'::text, NULL::horsies_task_history;
        RETURN;
    END IF;
    SELECT h.* INTO v_row FROM horsies_task_history AS h WHERE h.task_id = p_task_id;
    IF FOUND THEN
        RETURN QUERY SELECT 'HISTORY'::text, v_row;
    END IF;
END
$$;

-- Assertions.

CREATE FUNCTION fixture_check(p_case text, p_ok boolean) RETURNS void LANGUAGE plpgsql AS $$
BEGIN
    IF p_ok IS NOT TRUE THEN
        RAISE EXCEPTION 'case failed: %', p_case;
    END IF;
    RAISE NOTICE 'ok: %', p_case;
END
$$;

-- Runs p_sql in a subtransaction and checks that it fails with p_sqlstate. The subtransaction rolls the
-- statement back, so the checks after it see what a refused call leaves behind.
CREATE FUNCTION fixture_expect_error(p_case text, p_sql text, p_sqlstate text) RETURNS void LANGUAGE plpgsql AS $$
DECLARE
    v_state text;
    v_message text;
BEGIN
    BEGIN
        EXECUTE p_sql;
    EXCEPTION WHEN OTHERS THEN
        GET STACKED DIAGNOSTICS v_state = RETURNED_SQLSTATE, v_message = MESSAGE_TEXT;
        IF v_state = p_sqlstate THEN
            RAISE NOTICE 'ok: % (% %)', p_case, v_state, v_message;
            RETURN;
        END IF;
        RAISE EXCEPTION 'case failed: %: expected SQLSTATE %, got % (%)', p_case, p_sqlstate, v_state, v_message;
    END;
    RAISE EXCEPTION 'case failed: %: expected SQLSTATE %, the statement succeeded', p_case, p_sqlstate;
END
$$;

-- Fixtures.

-- A UUIDv7 whose embedded birth is p_at (millisecond precision).
CREATE FUNCTION fixture_uuid7(p_at timestamptz) RETURNS uuid LANGUAGE sql VOLATILE AS $$
    SELECT encode(
        set_byte(
            overlay(
                uuid_send(gen_random_uuid())
                PLACING substring(int8send(floor(extract(epoch FROM p_at) * 1000)::bigint) FROM 3 FOR 6)
                FROM 1 FOR 6
            ),
            6, 112 | (get_byte(uuid_send(gen_random_uuid()), 6) & 15)
        ),
        'hex'
    )::uuid
$$;

CREATE FUNCTION fixture_task(
    p_id uuid, p_status text, p_class text, p_is_workflow boolean, p_retain boolean, p_input text,
    p_options text, p_key bytea
) RETURNS void LANGUAGE sql AS $$
    INSERT INTO horsies_tasks (
        id, task_name, queue_name, priority, input, task_options, execution, retention_class_key, sent_at,
        created_at, command_fingerprint_version, command_fingerprint, idempotency_key_digest, status, enqueued_at,
        claimed_at, claimed_by_worker_id, claim_expires_at, started_at, finalizing_at, finalizing_by_worker_id,
        worker_pid, worker_hostname, worker_process_name, retry_count, max_retries, next_retry_at, good_until,
        is_workflow_task, updated_at, retain_rerun_input, rerun_of_task_id, rerun_root_task_id
    ) VALUES (
        p_id, 'fixture.task', 'default', 50, p_input, p_options, 'ISOLATED', p_class, now(),
        now(),
        CASE WHEN p_is_workflow THEN NULL ELSE 1::smallint END,
        CASE WHEN p_is_workflow THEN NULL ELSE sha256(convert_to(p_id::text, 'UTF8')) END,
        p_key, p_status, now(),
        CASE WHEN p_status = 'PENDING' THEN NULL ELSE now() - interval '2 seconds' END,
        CASE WHEN p_status = 'PENDING' THEN NULL ELSE 'w1' END,
        CASE WHEN p_status = 'CLAIMED' THEN now() + interval '1 minute' END,
        CASE WHEN p_status = 'RUNNING' THEN now() - interval '1 second' END,
        NULL, NULL,
        CASE WHEN p_status = 'RUNNING' THEN 7 END,
        CASE WHEN p_status = 'RUNNING' THEN 'host' END,
        CASE WHEN p_status = 'RUNNING' THEN 'proc' END,
        0, 3, NULL, now() + interval '1 hour',
        p_is_workflow, now(), p_retain, NULL, NULL
    )
$$;

CREATE FUNCTION fixture_attempt(p_task uuid, p_attempt integer) RETURNS void LANGUAGE sql AS $$
    INSERT INTO horsies_task_attempts (
        task_id, attempt, outcome, will_retry, started_at, finished_at, error_code, error_message, failed_reason,
        worker_id, worker_hostname, worker_pid, worker_process_name
    ) VALUES (
        p_task, p_attempt, 'FAILED', TRUE, now() - interval '2 seconds', now() - interval '1 second', 'E1', 'msg',
        'reason', 'w1', 'host', 7, 'proc'
    )
$$;

CREATE FUNCTION fixture_workflow(p_id uuid) RETURNS void LANGUAGE sql AS $$
    INSERT INTO horsies_workflows (
        id, name, definition_key, status, on_error, output_task_index, success_policy, result, error,
        parent_workflow_id, parent_task_index, root_workflow_id, depth, sent_at, created_at, started_at,
        completed_at, updated_at
    ) VALUES (
        p_id, 'fixture.workflow', NULL, 'RUNNING', 'fail', NULL, NULL, NULL, NULL,
        NULL, NULL, p_id, 0, now(), now(), now(), NULL, now()
    )
$$;

CREATE FUNCTION fixture_node(p_id uuid, p_workflow uuid, p_index integer, p_task uuid) RETURNS void LANGUAGE sql AS $$
    INSERT INTO horsies_workflow_tasks (
        downstream_subworkflow, id, workflow_id, task_index, node_id, task_name, input, queue_name, priority,
        dependencies, args_from,
        workflow_ctx_from, allow_failed_deps, join_type, min_success, task_options, max_retries, execution,
        good_until, retention_class_key, status, task_id, is_subworkflow, sub_workflow_id, sub_workflow_name,
        sub_definition_key, sub_workflow_summary, result, error, created_at, started_at, completed_at
    ) VALUES (
        'NONE', p_id, p_workflow, p_index, 'node_' || p_index, 'fixture.task', '{"x":1}', 'default', 50, '{}', NULL,
        NULL, FALSE, 'all', NULL, NULL, 3, 'ISOLATED',
        NULL, 'forever', 'RUNNING', p_task, FALSE, NULL, NULL,
        NULL, NULL, NULL, NULL, now(), now(), NULL
    )
$$;

-- The caller side of a single move: the task advisory lock, the live row FOR UPDATE, then the move.
CREATE FUNCTION fixture_move(
    p_id uuid, p_status text, p_kind text, p_terminal_at timestamptz, p_result text, p_error text, p_reason text
) RETURNS void LANGUAGE plpgsql AS $$
DECLARE
    v_task horsies_tasks;
BEGIN
    PERFORM pg_advisory_xact_lock(hashtextextended(p_id::text, 731));
    SELECT * INTO STRICT v_task FROM horsies_tasks WHERE id = p_id FOR UPDATE;
    PERFORM horsies_move_task_to_history(v_task, p_status, p_kind, p_terminal_at, p_result, p_error, p_reason);
END
$$;

CREATE FUNCTION fixture_period_class(p_days integer) RETURNS text LANGUAGE plpgsql AS $$
DECLARE
    v_key text := 'standard_' || p_days || 'd';
BEGIN
    INSERT INTO horsies_retention_classes (class_key, class_kind, retention_days, heartbeat_horizon_hours, created_at)
    VALUES (v_key, 'PERIOD', p_days, NULL, now());
    EXECUTE format(
        'CREATE TABLE %I PARTITION OF horsies_task_history FOR VALUES IN (%L) PARTITION BY RANGE (terminal_at)',
        'horsies_task_history_' || v_key, v_key
    );
    RETURN v_key;
END
$$;

-- A daily leaf of a period class with its two indexes and its catalog row.
CREATE FUNCTION fixture_leaf(p_class text, p_lower timestamptz) RETURNS text LANGUAGE plpgsql AS $$
DECLARE
    v_leaf text := 'horsies_task_history_' || p_class || '_' || to_char(p_lower, 'YYYY_MM_DD');
BEGIN
    EXECUTE format(
        'CREATE TABLE %I PARTITION OF %I FOR VALUES FROM (%L) TO (%L)',
        v_leaf, 'horsies_task_history_' || p_class, p_lower, p_lower + interval '1 day'
    );
    EXECUTE format('CREATE INDEX %I ON %I USING btree (task_id)', v_leaf || '_task_idx', v_leaf);
    EXECUTE format('CREATE INDEX %I ON %I USING btree (enqueued_at)', v_leaf || '_enqueued_idx', v_leaf);
    INSERT INTO horsies_task_history_leaf_catalog (
        leaf_name, class_key, lower_bound_kind, lower_anchor, upper_anchor, index_schema_version, created_at,
        detached_at, dropped_at
    ) VALUES (v_leaf, p_class, 'AT', p_lower, p_lower + interval '1 day', 1, now(), NULL, NULL);
    RETURN v_leaf;
END
$$;

-- A catalog row only: for the absence rule, which reads nothing else.
CREATE FUNCTION fixture_catalog_row(
    p_leaf text, p_class text, p_lower timestamptz, p_upper timestamptz, p_detached boolean, p_dropped boolean
) RETURNS void LANGUAGE sql AS $$
    INSERT INTO horsies_task_history_leaf_catalog (
        leaf_name, class_key, lower_bound_kind, lower_anchor, upper_anchor, index_schema_version, created_at,
        detached_at, dropped_at
    ) VALUES (
        p_leaf, p_class, 'AT', p_lower, p_upper, 1, now(),
        CASE WHEN p_detached OR p_dropped THEN now() END, CASE WHEN p_dropped THEN now() END
    )
$$;

-- An hourly heartbeat leaf with its probe index and its catalog row.
CREATE FUNCTION fixture_heartbeat_leaf(p_lower timestamptz) RETURNS text LANGUAGE plpgsql AS $$
DECLARE
    v_leaf text := 'horsies_heartbeats_' || to_char(p_lower, 'YYYY_MM_DD_HH24');
BEGIN
    EXECUTE format(
        'CREATE TABLE %I PARTITION OF horsies_heartbeats FOR VALUES FROM (%L) TO (%L)',
        v_leaf, p_lower, p_lower + interval '1 hour'
    );
    EXECUTE format(
        'CREATE INDEX %I ON %I USING btree (task_id, role, sent_at DESC)', v_leaf || '_probe_idx', v_leaf
    );
    INSERT INTO horsies_task_history_leaf_catalog (
        leaf_name, class_key, lower_bound_kind, lower_anchor, upper_anchor, index_schema_version, created_at,
        detached_at, dropped_at
    ) VALUES (v_leaf, 'heartbeats', 'AT', p_lower, p_lower + interval '1 hour', 1, now(), NULL, NULL);
    RETURN v_leaf;
END
$$;

-- Whether this session holds the advisory lock of a leaf (a bigint key shows as classid and objid).
CREATE FUNCTION fixture_holds_leaf_lock(p_class text, p_lower timestamptz) RETURNS boolean LANGUAGE sql AS $$
    SELECT EXISTS (
        SELECT 1
        FROM pg_locks AS l
        CROSS JOIN LATERAL (SELECT horsies_task_history_leaf_lock_key(p_class, p_lower) AS key) AS k
        WHERE l.locktype = 'advisory' AND l.pid = pg_backend_pid() AND l.granted AND l.objsubid = 1
          AND l.classid::bigint = (k.key >> 32) & 4294967295
          AND l.objid::bigint = k.key & 4294967295
    )
$$;

------------------------------------------------------------------------------------------------------------------------
-- Move to history: one locked row
------------------------------------------------------------------------------------------------------------------------

CREATE TEMP TABLE ids (name text PRIMARY KEY, id uuid NOT NULL);
INSERT INTO ids VALUES
    ('plain', fixture_uuid7(now() - interval '1 minute')),
    ('completed', fixture_uuid7(now() - interval '1 minute')),
    ('declined', fixture_uuid7(now() - interval '1 minute')),
    ('over_bound', fixture_uuid7(now() - interval '1 minute')),
    ('skewed', fixture_uuid7(now() + interval '1 hour')),
    ('deferred', fixture_uuid7(now() - interval '1 minute')),
    ('orphan', fixture_uuid7(now() - interval '1 minute')),
    ('abandon', fixture_uuid7(now() - interval '1 minute')),
    ('keyed', fixture_uuid7(now() - interval '1 minute')),
    ('admin', fixture_uuid7(now() - interval '1 minute')),
    ('workflow', gen_random_uuid()),
    ('node', gen_random_uuid()),
    ('node_abandon', gen_random_uuid());

SELECT fixture_task((SELECT id FROM ids WHERE name = 'plain'), 'RUNNING', 'forever', FALSE, TRUE, '{"a":1}',
    '{"retry":null}', NULL);
SELECT fixture_attempt((SELECT id FROM ids WHERE name = 'plain'), 1);
SELECT fixture_attempt((SELECT id FROM ids WHERE name = 'plain'), 2);
CREATE TEMP TABLE expected_snapshot AS
    SELECT horsies_encode_task_attempts((SELECT id FROM ids WHERE name = 'plain')) AS snapshot;
SELECT fixture_move((SELECT id FROM ids WHERE name = 'plain'), 'FAILED', 'FAIL_RUNNING', now(), '{"error":"x"}',
    'E1', 'boom');
SELECT fixture_check(
    'move: a failed task becomes one history row with its envelope, result and attempt snapshot',
    (SELECT count(*) = 1
            AND bool_and(h.status = 'FAILED' AND h.terminalization_kind = 'FAIL_RUNNING' AND h.terminal_at = now())
            AND bool_and(h.error_code = 'E1' AND h.final_failed_reason = 'boom')
            AND bool_and(h.result_payload = convert_to('{"error":"x"}', 'UTF8')
                         AND h.result_digest = sha256(convert_to('{"error":"x"}', 'UTF8')))
            AND bool_and(h.rerun_input_disposition = 'INLINE' AND h.rerun_input_version = 1
                         AND convert_from(h.rerun_input_inline, 'UTF8')
                             = '{"input":{"a":1},"options":{"retry":null}}'
                         AND h.rerun_input_digest = sha256(h.rerun_input_inline))
            AND bool_and(h.attempt_snapshot = (SELECT snapshot FROM expected_snapshot)
                         AND h.attempt_snapshot_digest = sha256(h.attempt_snapshot))
            AND bool_and(h.last_claimed_worker_id = 'w1' AND h.last_worker_pid = 7 AND h.workflow_id IS NULL)
     FROM horsies_task_history AS h WHERE h.task_id = (SELECT id FROM ids WHERE name = 'plain'))
);
SELECT fixture_check(
    'move: the live row and its attempt rows are deleted, and no clock exception is written',
    NOT EXISTS (SELECT 1 FROM horsies_tasks WHERE id = (SELECT id FROM ids WHERE name = 'plain'))
    AND NOT EXISTS (SELECT 1 FROM horsies_task_attempts WHERE task_id = (SELECT id FROM ids WHERE name = 'plain'))
    AND NOT EXISTS (
        SELECT 1 FROM horsies_task_history_clock_exceptions WHERE task_id = (SELECT id FROM ids WHERE name = 'plain')
    )
);

SELECT fixture_task((SELECT id FROM ids WHERE name = 'completed'), 'RUNNING', 'forever', FALSE, TRUE, '{"a":2}',
    NULL, NULL);
SELECT fixture_move((SELECT id FROM ids WHERE name = 'completed'), 'COMPLETED', 'COMPLETE_FUSED', now(), '{"ok":1}',
    NULL, NULL);
SELECT fixture_check(
    'move: a COMPLETED task is NEVER_ELIGIBLE for rerun and keeps no envelope',
    (SELECT h.rerun_input_disposition = 'NEVER_ELIGIBLE' AND h.rerun_input_inline IS NULL
            AND h.rerun_input_digest IS NULL AND h.rerun_input_version IS NULL
     FROM horsies_task_history AS h WHERE h.task_id = (SELECT id FROM ids WHERE name = 'completed'))
);

SELECT fixture_task((SELECT id FROM ids WHERE name = 'declined'), 'RUNNING', 'forever', FALSE, FALSE, '{"a":3}',
    NULL, NULL);
SELECT fixture_move((SELECT id FROM ids WHERE name = 'declined'), 'FAILED', 'FAIL_RUNNING', now(), '{"e":1}', 'E',
    'r');
SELECT fixture_check(
    'move: retain_rerun_input off gives DECLINED_BY_POLICY with no envelope',
    (SELECT h.rerun_input_disposition = 'DECLINED_BY_POLICY' AND h.rerun_input_inline IS NULL
     FROM horsies_task_history AS h WHERE h.task_id = (SELECT id FROM ids WHERE name = 'declined'))
);

SELECT fixture_task((SELECT id FROM ids WHERE name = 'over_bound'), 'RUNNING', 'forever', FALSE, TRUE,
    '{"blob":"' || repeat('x', 65536) || '"}', NULL, NULL);
SELECT fixture_move((SELECT id FROM ids WHERE name = 'over_bound'), 'EXPIRED', 'EXPIRE_CLAIMED', now(), '{"e":1}',
    'E', NULL);
SELECT fixture_check(
    'move: an envelope over 65,536 bytes gives OVER_BOUND with no envelope',
    (SELECT h.rerun_input_disposition = 'OVER_BOUND' AND h.rerun_input_inline IS NULL
            AND h.rerun_input_digest IS NULL AND h.rerun_input_version IS NULL
     FROM horsies_task_history AS h WHERE h.task_id = (SELECT id FROM ids WHERE name = 'over_bound'))
);

SELECT fixture_task((SELECT id FROM ids WHERE name = 'skewed'), 'RUNNING', 'forever', FALSE, TRUE, '{"a":4}',
    NULL, NULL);
SELECT fixture_move((SELECT id FROM ids WHERE name = 'skewed'), 'FAILED', 'FAIL_STALE', now(), '{"e":1}', 'E', 'r');
SELECT fixture_check(
    'move: a birth more than 5 s after the terminal time records a clock exception',
    (SELECT count(*) = 1 AND bool_and(e.retention_class_key = 'forever' AND e.terminal_at = now())
     FROM horsies_task_history_clock_exceptions AS e WHERE e.task_id = (SELECT id FROM ids WHERE name = 'skewed'))
);

SELECT fixture_workflow((SELECT id FROM ids WHERE name = 'workflow'));
SELECT fixture_task((SELECT id FROM ids WHERE name = 'deferred'), 'RUNNING', 'forever', TRUE, TRUE, '{"a":5}',
    NULL, NULL);
SELECT fixture_node((SELECT id FROM ids WHERE name = 'node'), (SELECT id FROM ids WHERE name = 'workflow'), 0,
    (SELECT id FROM ids WHERE name = 'deferred'));
SELECT fixture_move((SELECT id FROM ids WHERE name = 'deferred'), 'FAILED', 'FAIL_RUNNING', now(), '{"e":5}', 'E',
    'r');
SELECT fixture_check(
    'move: a deferred kind of a workflow task writes the phase-2 pending row with the history digest',
    (SELECT h.workflow_id = (SELECT id FROM ids WHERE name = 'workflow')
            AND h.rerun_input_disposition = 'NEVER_ELIGIBLE'
            AND p.workflow_node_row_id = (SELECT id FROM ids WHERE name = 'node')
            AND p.workflow_id = h.workflow_id AND p.terminalization_kind = 'FAIL_RUNNING'
            AND p.recovery_source = 'HISTORY' AND p.history_class = 'forever' AND p.history_anchor = h.terminal_at
            AND p.result_digest = h.result_digest AND p.attempt_count = 0
     FROM horsies_task_history AS h
     JOIN horsies_workflow_phase2_pending AS p ON p.task_id = h.task_id
     WHERE h.task_id = (SELECT id FROM ids WHERE name = 'deferred'))
);

SELECT fixture_task((SELECT id FROM ids WHERE name = 'orphan'), 'CLAIMED', 'forever', TRUE, TRUE, '{"a":6}',
    NULL, NULL);
SELECT fixture_move((SELECT id FROM ids WHERE name = 'orphan'), 'CANCELLED', 'CANCEL_ORPHAN', now(), NULL,
    'WORKFLOW_CHECK_FAILED', 'orphan');
SELECT fixture_check(
    'move: an orphan kind moves a workflow task with no node row, with no workflow id and no pending row',
    (SELECT h.workflow_id IS NULL AND h.result_payload IS NULL
     FROM horsies_task_history AS h WHERE h.task_id = (SELECT id FROM ids WHERE name = 'orphan'))
    AND NOT EXISTS (
        SELECT 1 FROM horsies_workflow_phase2_pending WHERE task_id = (SELECT id FROM ids WHERE name = 'orphan')
    )
);

SELECT fixture_task((SELECT id FROM ids WHERE name = 'abandon'), 'CLAIMED', 'forever', TRUE, TRUE, '{"a":7}',
    NULL, NULL);
SELECT fixture_node((SELECT id FROM ids WHERE name = 'node_abandon'), (SELECT id FROM ids WHERE name = 'workflow'), 1,
    (SELECT id FROM ids WHERE name = 'abandon'));
SELECT fixture_move((SELECT id FROM ids WHERE name = 'abandon'), 'CANCELLED', 'PAUSE_ABANDON_CLAIM', now(), NULL,
    'TASK_CANCELLED', 'paused');
SELECT fixture_check(
    'move: a non-deferred kind with a node row stores the workflow id and writes no pending row',
    (SELECT h.workflow_id = (SELECT id FROM ids WHERE name = 'workflow')
     FROM horsies_task_history AS h WHERE h.task_id = (SELECT id FROM ids WHERE name = 'abandon'))
    AND NOT EXISTS (
        SELECT 1 FROM horsies_workflow_phase2_pending WHERE task_id = (SELECT id FROM ids WHERE name = 'abandon')
    )
);

SELECT fixture_task((SELECT id FROM ids WHERE name = 'keyed'), 'RUNNING', 'forever', FALSE, TRUE, '{"a":8}', NULL,
    sha256('key-1'::bytea));
SELECT fixture_check(
    'move: fixture reservation claim applies',
    (SELECT outcome = 'APPLIED' FROM horsies_key_reservation_claim(
        sha256('key-1'::bytea), 1::smallint, interval '1 hour', 1::smallint, sha256('fp'::bytea),
        (SELECT id FROM ids WHERE name = 'keyed')))
);
SELECT fixture_move((SELECT id FROM ids WHERE name = 'keyed'), 'FAILED', 'FAIL_RUNNING', now(), '{"e":8}', 'E', 'r');
SELECT fixture_check(
    'move: a keyed task ends its reservation: TERMINAL until terminal time plus the window',
    (SELECT r.disposition = 'TERMINAL' AND r.expires_at = now() + interval '1 hour'
     FROM horsies_key_reservations AS r WHERE r.idempotency_key_digest = sha256('key-1'::bytea))
);

SELECT fixture_task((SELECT id FROM ids WHERE name = 'admin'), 'PENDING', 'forever', FALSE, TRUE, '{"a":9}', NULL,
    NULL);
SELECT fixture_move((SELECT id FROM ids WHERE name = 'admin'), 'CANCELLED', 'CANCEL_ADMIN', now(), '{"ignored":1}',
    'TASK_CANCELLED', NULL);
SELECT fixture_check(
    'move: an administrative cancel stores no result',
    (SELECT h.result_payload IS NULL AND h.result_digest IS NULL AND h.rerun_input_disposition = 'INLINE'
     FROM horsies_task_history AS h WHERE h.task_id = (SELECT id FROM ids WHERE name = 'admin'))
);

-- Refusals of the single move. Each leaves the live row in place and writes nothing.

INSERT INTO ids VALUES
    ('refused', fixture_uuid7(now() - interval '1 minute')),
    ('refused_workflow', fixture_uuid7(now() - interval '1 minute')),
    ('refused_no_node', fixture_uuid7(now() - interval '1 minute')),
    ('refused_class', fixture_uuid7(now() - interval '1 minute')),
    ('refused_exception', fixture_uuid7(now() - interval '1 minute')),
    ('node_refused', gen_random_uuid());
SELECT fixture_task((SELECT id FROM ids WHERE name = 'refused'), 'RUNNING', 'forever', FALSE, TRUE, '{"r":1}', NULL,
    NULL);
SELECT fixture_task((SELECT id FROM ids WHERE name = 'refused_workflow'), 'RUNNING', 'forever', TRUE, TRUE,
    '{"r":2}', NULL, NULL);
SELECT fixture_node((SELECT id FROM ids WHERE name = 'node_refused'), (SELECT id FROM ids WHERE name = 'workflow'), 2,
    (SELECT id FROM ids WHERE name = 'refused_workflow'));
SELECT fixture_task((SELECT id FROM ids WHERE name = 'refused_no_node'), 'RUNNING', 'forever', TRUE, TRUE,
    '{"r":3}', NULL, NULL);
SELECT fixture_task((SELECT id FROM ids WHERE name = 'refused_class'), 'RUNNING', 'standard_9d', FALSE, TRUE,
    '{"r":4}', NULL, NULL);
SELECT fixture_task((SELECT id FROM ids WHERE name = 'refused_exception'), 'RUNNING', 'forever', FALSE, TRUE,
    '{"r":5}', NULL, NULL);
INSERT INTO horsies_task_history_clock_exceptions (task_id, retention_class_key, terminal_at)
VALUES ((SELECT id FROM ids WHERE name = 'refused_exception'), 'forever', now() - interval '1 day');

SELECT fixture_expect_error(
    'move refuses: an unknown kind',
    format($q$SELECT fixture_move(%L, 'FAILED', 'NO_SUCH_KIND', now(), '{}', 'E', 'r')$q$,
           (SELECT id FROM ids WHERE name = 'refused')),
    '22023'
);
SELECT fixture_expect_error(
    'move refuses: a kind whose status disagrees',
    format($q$SELECT fixture_move(%L, 'COMPLETED', 'FAIL_RUNNING', now(), '{}', 'E', 'r')$q$,
           (SELECT id FROM ids WHERE name = 'refused')),
    'P0001'
);
SELECT fixture_expect_error(
    'move refuses: a NULL terminal time',
    format($q$SELECT fixture_move(%L, 'FAILED', 'FAIL_RUNNING', NULL, '{}', 'E', 'r')$q$,
           (SELECT id FROM ids WHERE name = 'refused')),
    'P0001'
);
SELECT fixture_expect_error(
    'move refuses: a NULL row',
    $q$SELECT horsies_move_task_to_history(NULL::horsies_tasks, 'FAILED', 'FAIL_RUNNING', now(), '{}', 'E', 'r')$q$,
    'P0001'
);
SELECT fixture_expect_error(
    'move refuses: an id that a clock exception already records (duplicate guard)',
    format($q$SELECT fixture_move(%L, 'FAILED', 'FAIL_RUNNING', now(), '{}', 'E', 'r')$q$,
           (SELECT id FROM ids WHERE name = 'refused_exception')),
    'XX001'
);
SELECT fixture_expect_error(
    'move refuses: COMPLETE_FUSED on a workflow task',
    format($q$SELECT fixture_move(%L, 'COMPLETED', 'COMPLETE_FUSED', now(), '{}', NULL, NULL)$q$,
           (SELECT id FROM ids WHERE name = 'refused_workflow')),
    '22023'
);
SELECT fixture_expect_error(
    'move refuses: CANCEL_ADMIN on a workflow task',
    format($q$SELECT fixture_move(%L, 'CANCELLED', 'CANCEL_ADMIN', now(), NULL, NULL, NULL)$q$,
           (SELECT id FROM ids WHERE name = 'refused_workflow')),
    '22023'
);
SELECT fixture_expect_error(
    'move refuses: a deferred kind of a workflow task with no result',
    format($q$SELECT fixture_move(%L, 'FAILED', 'FAIL_RUNNING', now(), NULL, 'E', 'r')$q$,
           (SELECT id FROM ids WHERE name = 'refused_workflow')),
    '23502'
);
SELECT fixture_expect_error(
    'move refuses: a deferred kind of a workflow task with no node row',
    format($q$SELECT fixture_move(%L, 'EXPIRED', 'EXPIRE_CLAIMED', now(), '{}', 'E', NULL)$q$,
           (SELECT id FROM ids WHERE name = 'refused_no_node')),
    'P0002'
);
SELECT fixture_expect_error(
    'move refuses: a row that no history leaf takes (55H01, history leaf missing)',
    format($q$SELECT fixture_move(%L, 'FAILED', 'FAIL_RUNNING', now(), '{}', 'E', 'r')$q$,
           (SELECT id FROM ids WHERE name = 'refused_class')),
    '55H01'
);

-- The SQLSTATE, the message and the DETAIL of a refused statement, or 'succeeded'.
CREATE FUNCTION fixture_refusal_text(p_sql text) RETURNS text LANGUAGE plpgsql AS $$
DECLARE
    v_state text;
    v_message text;
    v_detail text;
BEGIN
    BEGIN
        EXECUTE p_sql;
    EXCEPTION WHEN OTHERS THEN
        GET STACKED DIAGNOSTICS v_state = RETURNED_SQLSTATE, v_message = MESSAGE_TEXT,
            v_detail = PG_EXCEPTION_DETAIL;
        RETURN v_state || ' ' || v_message || ' / ' || v_detail;
    END;
    RETURN 'succeeded';
END
$$;
SELECT fixture_check(
    'move refuses: the leaf refusal names the class, the missing UTC day and the missing class partition',
    fixture_refusal_text(format(
        $q$SELECT fixture_move(%L, 'FAILED', 'FAIL_RUNNING', '2026-03-04 23:30:00-05', '{}', 'E', 'r')$q$,
        (SELECT id FROM ids WHERE name = 'refused_class')))
    = '55H01 no task-history leaf of class standard_9d covers [2026-03-05T00:00:00Z, 2026-03-06T00:00:00Z) / '
      'the class partition horsies_task_history_standard_9d does not exist: the class is not registered'
);

-- A row the caller did not lock: a copy taken before the live row changed.
CREATE TEMP TABLE stale_row (LIKE horsies_tasks);
INSERT INTO stale_row SELECT * FROM horsies_tasks WHERE id = (SELECT id FROM ids WHERE name = 'refused');
UPDATE horsies_tasks SET updated_at = updated_at + interval '1 second'
WHERE id = (SELECT id FROM ids WHERE name = 'refused');
SELECT fixture_expect_error(
    'move refuses: a row that differs from the stored live row',
    $q$SELECT horsies_move_task_to_history(
        (SELECT ROW(s.*)::horsies_tasks FROM stale_row AS s), 'FAILED', 'FAIL_RUNNING', now(), '{}', 'E', 'r')$q$,
    'P0001'
);
UPDATE stale_row SET status = 'COMPLETED';
SELECT fixture_expect_error(
    'move refuses: a row whose status is not live',
    $q$SELECT horsies_move_task_to_history(
        (SELECT ROW(s.*)::horsies_tasks FROM stale_row AS s), 'FAILED', 'FAIL_RUNNING', now(), '{}', 'E', 'r')$q$,
    'P0001'
);

-- A second move of a moved id: the live row is inserted again, the guard finds the history row.
SELECT fixture_task((SELECT id FROM ids WHERE name = 'declined'), 'RUNNING', 'forever', FALSE, FALSE, '{"a":3}',
    NULL, NULL);
SELECT fixture_expect_error(
    'move refuses: an id that history already holds (duplicate guard)',
    format($q$SELECT fixture_move(%L, 'FAILED', 'FAIL_RUNNING', now(), '{}', 'E', 'r')$q$,
           (SELECT id FROM ids WHERE name = 'declined')),
    'XX001'
);
DELETE FROM horsies_tasks WHERE id = (SELECT id FROM ids WHERE name = 'declined');

SELECT fixture_check(
    'move refusals leave every live row in place and write no history, pending or exception row',
    (SELECT count(*) = 5 FROM horsies_tasks
     WHERE id IN (SELECT id FROM ids WHERE name LIKE 'refused%'))
    AND NOT EXISTS (
        SELECT 1 FROM horsies_task_history WHERE task_id IN (SELECT id FROM ids WHERE name LIKE 'refused%')
    )
    AND NOT EXISTS (
        SELECT 1 FROM horsies_workflow_phase2_pending
        WHERE task_id IN (SELECT id FROM ids WHERE name LIKE 'refused%')
    )
    AND (SELECT count(*) = 1 FROM horsies_task_history_clock_exceptions
         WHERE task_id IN (SELECT id FROM ids WHERE name LIKE 'refused%'))
);

------------------------------------------------------------------------------------------------------------------------
-- Move to history: a batch of locked rows
------------------------------------------------------------------------------------------------------------------------

INSERT INTO ids VALUES
    ('batch_plain', fixture_uuid7(now() - interval '1 minute')),
    ('batch_keyed', fixture_uuid7(now() - interval '1 minute')),
    ('batch_skewed', fixture_uuid7(now() + interval '1 hour')),
    ('batch_workflow', fixture_uuid7(now() - interval '1 minute')),
    ('batch_declined', fixture_uuid7(now() - interval '1 minute')),
    ('node_batch', gen_random_uuid());
SELECT fixture_task((SELECT id FROM ids WHERE name = 'batch_plain'), 'PENDING', 'forever', FALSE, TRUE, '{"b":1}',
    '{"o":1}', NULL);
SELECT fixture_task((SELECT id FROM ids WHERE name = 'batch_keyed'), 'PENDING', 'forever', FALSE, TRUE, '{"b":2}',
    NULL, sha256('key-2'::bytea));
SELECT fixture_attempt((SELECT id FROM ids WHERE name = 'batch_keyed'), 1);
SELECT fixture_check(
    'batch: fixture reservation claim applies',
    (SELECT outcome = 'APPLIED' FROM horsies_key_reservation_claim(
        sha256('key-2'::bytea), 1::smallint, interval '2 hours', 1::smallint, sha256('fp'::bytea),
        (SELECT id FROM ids WHERE name = 'batch_keyed')))
);
SELECT fixture_task((SELECT id FROM ids WHERE name = 'batch_skewed'), 'PENDING', 'forever', FALSE, TRUE, '{"b":3}',
    NULL, NULL);
SELECT fixture_task((SELECT id FROM ids WHERE name = 'batch_workflow'), 'PENDING', 'forever', TRUE, TRUE, '{"b":4}',
    NULL, NULL);
SELECT fixture_node((SELECT id FROM ids WHERE name = 'node_batch'), (SELECT id FROM ids WHERE name = 'workflow'), 3,
    (SELECT id FROM ids WHERE name = 'batch_workflow'));
SELECT fixture_task((SELECT id FROM ids WHERE name = 'batch_declined'), 'PENDING', 'forever', FALSE, FALSE,
    '{"b":5}', NULL, NULL);

SELECT horsies_move_tasks_to_history(
    ARRAY(SELECT id FROM ids WHERE name IN ('batch_plain', 'batch_keyed', 'batch_skewed', 'batch_workflow',
                                            'batch_declined') ORDER BY id),
    'EXPIRED', 'EXPIRE_PENDING', now(), '{"expired":true}', 'TASK_EXPIRED', NULL
);
SELECT fixture_check(
    'batch: every listed row moves with one terminal time and one result digest',
    (SELECT count(*) = 5
            AND count(DISTINCT h.terminal_at) = 1 AND min(h.terminal_at) = now()
            AND count(DISTINCT h.result_digest) = 1
            AND bool_and(h.result_digest = sha256(convert_to('{"expired":true}', 'UTF8')))
            AND bool_and(h.status = 'EXPIRED' AND h.terminalization_kind = 'EXPIRE_PENDING'
                         AND h.error_code = 'TASK_EXPIRED' AND h.final_failed_reason IS NULL)
     FROM horsies_task_history AS h WHERE h.task_id IN (SELECT id FROM ids WHERE name LIKE 'batch\_%'))
);
SELECT fixture_check(
    'batch: the envelope ladder per row (INLINE, DECLINED_BY_POLICY, NEVER_ELIGIBLE for the workflow task)',
    (SELECT convert_from(h.rerun_input_inline, 'UTF8') = '{"input":{"b":1},"options":{"o":1}}'
            AND h.rerun_input_digest = sha256(h.rerun_input_inline)
     FROM horsies_task_history AS h WHERE h.task_id = (SELECT id FROM ids WHERE name = 'batch_plain'))
    AND (SELECT h.rerun_input_disposition = 'DECLINED_BY_POLICY' AND h.rerun_input_inline IS NULL
         FROM horsies_task_history AS h WHERE h.task_id = (SELECT id FROM ids WHERE name = 'batch_declined'))
    AND (SELECT h.rerun_input_disposition = 'NEVER_ELIGIBLE'
                AND h.workflow_id = (SELECT id FROM ids WHERE name = 'workflow')
         FROM horsies_task_history AS h WHERE h.task_id = (SELECT id FROM ids WHERE name = 'batch_workflow'))
);
SELECT fixture_check(
    'batch: the pending row for the workflow task, the clock exception, the reservation window',
    (SELECT count(*) = 1 AND bool_and(p.task_id = (SELECT id FROM ids WHERE name = 'batch_workflow')
                                      AND p.workflow_node_row_id = (SELECT id FROM ids WHERE name = 'node_batch')
                                      AND p.terminalization_kind = 'EXPIRE_PENDING' AND p.history_anchor = now()
                                      AND p.result_digest = sha256(convert_to('{"expired":true}', 'UTF8')))
     FROM horsies_workflow_phase2_pending AS p
     WHERE p.task_id IN (SELECT id FROM ids WHERE name LIKE 'batch\_%'))
    AND (SELECT count(*) = 1 AND bool_and(e.task_id = (SELECT id FROM ids WHERE name = 'batch_skewed'))
         FROM horsies_task_history_clock_exceptions AS e
         WHERE e.task_id IN (SELECT id FROM ids WHERE name LIKE 'batch\_%'))
    AND (SELECT r.disposition = 'TERMINAL' AND r.expires_at = now() + interval '2 hours'
         FROM horsies_key_reservations AS r WHERE r.idempotency_key_digest = sha256('key-2'::bytea))
);
SELECT fixture_check(
    'batch: the live rows and the attempt rows are deleted; the attempt snapshot is kept in history',
    NOT EXISTS (SELECT 1 FROM horsies_tasks WHERE id IN (SELECT id FROM ids WHERE name LIKE 'batch\_%'))
    AND NOT EXISTS (
        SELECT 1 FROM horsies_task_attempts WHERE task_id IN (SELECT id FROM ids WHERE name LIKE 'batch\_%')
    )
    AND (SELECT convert_from(h.attempt_snapshot, 'UTF8') LIKE '[[1,"FAILED",true,%'
         FROM horsies_task_history AS h WHERE h.task_id = (SELECT id FROM ids WHERE name = 'batch_keyed'))
);
CREATE TEMP TABLE history_count AS SELECT count(*) AS n FROM horsies_task_history;
SELECT horsies_move_tasks_to_history('{}'::uuid[], 'EXPIRED', 'EXPIRE_PENDING', now(), NULL, NULL, NULL);
SELECT fixture_check(
    'batch: an empty id array moves nothing',
    (SELECT count(*) FROM horsies_task_history) = (SELECT n FROM history_count)
);

INSERT INTO ids VALUES
    ('sweep_orphan', fixture_uuid7(now() - interval '1 minute'));
SELECT fixture_task((SELECT id FROM ids WHERE name = 'sweep_orphan'), 'CLAIMED', 'forever', TRUE, TRUE, '{"s":1}',
    NULL, NULL);
SELECT horsies_move_tasks_to_history(
    ARRAY[(SELECT id FROM ids WHERE name = 'sweep_orphan')], 'CANCELLED', 'CANCEL_ORPHAN_SWEEP', now(), NULL,
    'WORKFLOW_CHECK_FAILED', 'Workflow task orphaned: no live workflow_task linkage'
);
SELECT fixture_check(
    'batch: a non-deferred kind moves a workflow task with no node row and writes no pending row',
    (SELECT h.workflow_id IS NULL AND h.status = 'CANCELLED'
     FROM horsies_task_history AS h WHERE h.task_id = (SELECT id FROM ids WHERE name = 'sweep_orphan'))
    AND NOT EXISTS (
        SELECT 1 FROM horsies_workflow_phase2_pending WHERE task_id = (SELECT id FROM ids WHERE name = 'sweep_orphan')
    )
);

-- Refusals of the batch move.
INSERT INTO ids VALUES
    ('batch_refused', fixture_uuid7(now() - interval '1 minute')),
    ('batch_refused_workflow', fixture_uuid7(now() - interval '1 minute')),
    ('batch_refused_no_node', fixture_uuid7(now() - interval '1 minute')),
    ('node_batch_refused', gen_random_uuid());
SELECT fixture_task((SELECT id FROM ids WHERE name = 'batch_refused'), 'PENDING', 'forever', FALSE, TRUE,
    '{"x":1}', NULL, NULL);
SELECT fixture_task((SELECT id FROM ids WHERE name = 'batch_refused_workflow'), 'PENDING', 'forever', TRUE, TRUE,
    '{"x":2}', NULL, NULL);
SELECT fixture_node((SELECT id FROM ids WHERE name = 'node_batch_refused'), (SELECT id FROM ids WHERE name = 'workflow'),
    4, (SELECT id FROM ids WHERE name = 'batch_refused_workflow'));
SELECT fixture_task((SELECT id FROM ids WHERE name = 'batch_refused_no_node'), 'PENDING', 'forever', TRUE, TRUE,
    '{"x":3}', NULL, NULL);

SELECT fixture_expect_error(
    'batch refuses: a NULL id array',
    $q$SELECT horsies_move_tasks_to_history(NULL, 'EXPIRED', 'EXPIRE_PENDING', now(), '{}', 'E', NULL)$q$,
    '22023'
);
SELECT fixture_expect_error(
    'batch refuses: an id listed twice',
    format($q$SELECT horsies_move_tasks_to_history(ARRAY[%L, %L]::uuid[], 'EXPIRED', 'EXPIRE_PENDING', now(),
           '{}', 'E', NULL)$q$,
           (SELECT id FROM ids WHERE name = 'batch_refused'), (SELECT id FROM ids WHERE name = 'batch_refused')),
    '22023'
);
SELECT fixture_expect_error(
    'batch refuses: a single-row kind',
    format($q$SELECT horsies_move_tasks_to_history(ARRAY[%L]::uuid[], 'FAILED', 'FAIL_RUNNING', now(), '{}', 'E',
           NULL)$q$, (SELECT id FROM ids WHERE name = 'batch_refused')),
    '22023'
);
SELECT fixture_expect_error(
    'batch refuses: a kind whose status disagrees',
    format($q$SELECT horsies_move_tasks_to_history(ARRAY[%L]::uuid[], 'CANCELLED', 'EXPIRE_PENDING', now(), '{}',
           'E', NULL)$q$, (SELECT id FROM ids WHERE name = 'batch_refused')),
    'P0001'
);
SELECT fixture_expect_error(
    'batch refuses: a NULL terminal time',
    format($q$SELECT horsies_move_tasks_to_history(ARRAY[%L]::uuid[], 'EXPIRED', 'EXPIRE_PENDING', NULL, '{}',
           'E', NULL)$q$, (SELECT id FROM ids WHERE name = 'batch_refused')),
    'P0001'
);
SELECT fixture_expect_error(
    'batch refuses: EXPIRE_PENDING of a workflow task with no result',
    format($q$SELECT horsies_move_tasks_to_history(ARRAY[%L, %L]::uuid[], 'EXPIRED', 'EXPIRE_PENDING', now(),
           NULL, 'E', NULL)$q$,
           (SELECT id FROM ids WHERE name = 'batch_refused'),
           (SELECT id FROM ids WHERE name = 'batch_refused_workflow')),
    '23502'
);
SELECT fixture_expect_error(
    'batch refuses: EXPIRE_PENDING of a workflow task with no node row',
    format($q$SELECT horsies_move_tasks_to_history(ARRAY[%L, %L]::uuid[], 'EXPIRED', 'EXPIRE_PENDING', now(),
           '{}', 'E', NULL)$q$,
           (SELECT id FROM ids WHERE name = 'batch_refused'),
           (SELECT id FROM ids WHERE name = 'batch_refused_no_node')),
    '23503'
);
SELECT fixture_expect_error(
    'batch refuses: a listed id that is not a live row',
    format($q$SELECT horsies_move_tasks_to_history(ARRAY[%L, %L]::uuid[], 'EXPIRED', 'EXPIRE_PENDING', now(),
           '{}', 'E', NULL)$q$,
           (SELECT id FROM ids WHERE name = 'batch_refused'), gen_random_uuid()),
    'P0001'
);
SELECT fixture_expect_error(
    'batch refuses: a row that no history leaf takes (55H01, history leaf missing)',
    format($q$SELECT horsies_move_tasks_to_history(ARRAY[%L, %L]::uuid[], 'EXPIRED', 'EXPIRE_PENDING', now(),
           '{}', 'E', NULL)$q$,
           (SELECT id FROM ids WHERE name = 'batch_refused'),
           (SELECT id FROM ids WHERE name = 'refused_class')),
    '55H01'
);
INSERT INTO horsies_task_history_clock_exceptions (task_id, retention_class_key, terminal_at)
VALUES ((SELECT id FROM ids WHERE name = 'batch_refused_no_node'), 'forever', now() - interval '1 day');
SELECT fixture_expect_error(
    'batch refuses: an id that a clock exception records (duplicate guard)',
    format($q$SELECT horsies_move_tasks_to_history(ARRAY[%L, %L]::uuid[], 'CANCELLED', 'CANCEL_ORPHAN_SWEEP',
           now(), NULL, 'E', NULL)$q$,
           (SELECT id FROM ids WHERE name = 'batch_refused'),
           (SELECT id FROM ids WHERE name = 'batch_refused_no_node')),
    'XX001'
);
SELECT fixture_check(
    'batch refusals leave every live row in place and write no history row',
    (SELECT count(*) = 3 FROM horsies_tasks WHERE id IN (SELECT id FROM ids WHERE name LIKE 'batch\_refused%'))
    AND NOT EXISTS (
        SELECT 1 FROM horsies_task_history WHERE task_id IN (SELECT id FROM ids WHERE name LIKE 'batch\_refused%')
    )
);

------------------------------------------------------------------------------------------------------------------------
-- Absence reason
------------------------------------------------------------------------------------------------------------------------

SELECT fixture_check(
    'absence: an id that is not a UUIDv7 is NOT_CLASSIFIED',
    horsies_task_absence_reason(gen_random_uuid()) = 'NOT_CLASSIFIED'
);
SELECT fixture_check(
    'absence: a NULL id gives NULL',
    horsies_task_absence_reason(NULL) IS NULL
);
SELECT fixture_check(
    'absence: with no detached leaf, every v7 id is WITHIN_RETAINED_HISTORY',
    horsies_task_absence_reason(fixture_uuid7('2001-01-01 00:00:00+00')) = 'WITHIN_RETAINED_HISTORY'
);

SELECT fixture_period_class(1);
SELECT fixture_period_class(2);
INSERT INTO horsies_retention_classes (class_key, class_kind, retention_days, heartbeat_horizon_hours, created_at)
VALUES ('heartbeats', 'HEARTBEAT', NULL, 2, now());
SELECT fixture_catalog_row('absence_1d_a', 'standard_1d', '2020-01-01 00:00:00+00', '2020-01-02 00:00:00+00',
    FALSE, TRUE);
SELECT fixture_catalog_row('absence_1d_b', 'standard_1d', '2020-01-02 00:00:00+00', '2020-01-03 00:00:00+00',
    TRUE, FALSE);
SELECT fixture_catalog_row('absence_1d_c', 'standard_1d', '2020-01-03 00:00:00+00', '2020-01-04 00:00:00+00',
    FALSE, FALSE);
SELECT fixture_check(
    'absence: a birth before the latest detached upper anchor plus 5 s PREDATES_RETAINED_HISTORY',
    horsies_task_absence_reason(fixture_uuid7('2020-01-01 12:00:00+00')) = 'PREDATES_RETAINED_HISTORY'
    AND horsies_task_absence_reason(fixture_uuid7('2020-01-03 00:00:04.999+00')) = 'PREDATES_RETAINED_HISTORY'
);
SELECT fixture_check(
    'absence: a birth at the latest detached upper anchor plus 5 s or later is WITHIN_RETAINED_HISTORY',
    horsies_task_absence_reason(fixture_uuid7('2020-01-03 00:00:05+00')) = 'WITHIN_RETAINED_HISTORY'
    AND horsies_task_absence_reason(fixture_uuid7('2020-01-03 12:00:00+00')) = 'WITHIN_RETAINED_HISTORY'
);
SELECT fixture_catalog_row('absence_2d_a', 'standard_2d', '2020-01-05 00:00:00+00', '2020-01-06 00:00:00+00',
    TRUE, FALSE);
SELECT fixture_check(
    'absence: the latest detached anchor is taken over every PERIOD class',
    horsies_task_absence_reason(fixture_uuid7('2020-01-05 12:00:00+00')) = 'PREDATES_RETAINED_HISTORY'
);
SELECT fixture_catalog_row('absence_hb', 'heartbeats', '2020-02-01 00:00:00+00', '2020-02-01 01:00:00+00',
    FALSE, TRUE);
SELECT fixture_catalog_row('absence_1d_d', 'standard_1d', '2020-03-01 00:00:00+00', '2020-03-02 00:00:00+00',
    FALSE, FALSE);
SELECT fixture_check(
    'absence: detached heartbeat leaves and attached history leaves do not count',
    horsies_task_absence_reason(fixture_uuid7('2020-01-20 00:00:00+00')) = 'WITHIN_RETAINED_HISTORY'
);

------------------------------------------------------------------------------------------------------------------------
-- Rerun read
------------------------------------------------------------------------------------------------------------------------

SELECT fixture_check(
    'rerun read: an absent v7 source is ABSENT with its reason; no source columns',
    (SELECT r.class_registered AND r.client_family IS NULL AND r.location = 'ABSENT'
            AND r.absence_reason = 'PREDATES_RETAINED_HISTORY' AND r.task_id IS NULL AND r.status IS NULL
     FROM horsies_rerun_source_read(fixture_uuid7('2020-01-01 06:00:00+00'), 'forever') AS r)
);
SELECT fixture_check(
    'rerun read: an absent non-v7 source is ABSENT and NOT_CLASSIFIED',
    (SELECT r.location = 'ABSENT' AND r.absence_reason = 'NOT_CLASSIFIED'
     FROM horsies_rerun_source_read(gen_random_uuid(), 'standard_1d') AS r)
);
SELECT fixture_check(
    'rerun read: an unregistered class is reported with the rest of the read',
    (SELECT NOT r.class_registered AND r.location = 'HISTORY'
     FROM horsies_rerun_source_read((SELECT id FROM ids WHERE name = 'plain'), 'standard_30d') AS r)
);
INSERT INTO horsies_client_family (only_row, family, recorded_at) VALUES (TRUE, 'PYTHON', now());
SELECT fixture_check(
    'rerun read: a live source is LIVE, with the client family and no source columns',
    (SELECT r.class_registered AND r.client_family = 'PYTHON' AND r.location = 'LIVE' AND r.absence_reason IS NULL
            AND r.task_id IS NULL
     FROM horsies_rerun_source_read((SELECT id FROM ids WHERE name = 'refused'), 'forever') AS r)
);
SELECT fixture_check(
    'rerun read: a history source returns its row: identity, status, lineage, snapshot and envelope',
    (SELECT r.location = 'HISTORY' AND r.absence_reason IS NULL AND r.task_id = h.task_id
            AND r.task_name = h.task_name AND r.queue_name = h.queue_name AND r.priority = h.priority
            AND r.max_retries = h.max_retries AND r.status = h.status
            AND r.terminalization_kind = h.terminalization_kind AND r.terminal_at = h.terminal_at
            AND NOT r.is_workflow_task AND r.rerun_root_task_id IS NULL
            AND r.attempt_archive_version = h.attempt_archive_version AND r.attempt_snapshot = h.attempt_snapshot
            AND r.attempt_snapshot_digest = h.attempt_snapshot_digest
            AND r.rerun_input_disposition = 'INLINE' AND r.rerun_input_version = 1
            AND r.rerun_input_digest = h.rerun_input_digest AND r.rerun_input_inline = h.rerun_input_inline
     FROM horsies_rerun_source_read((SELECT id FROM ids WHERE name = 'plain'), 'forever') AS r
     CROSS JOIN horsies_task_history AS h
     WHERE h.task_id = (SELECT id FROM ids WHERE name = 'plain'))
);
SELECT fixture_expect_error(
    'rerun read refuses: a NULL class key',
    format($q$SELECT * FROM horsies_rerun_source_read(%L, NULL)$q$, (SELECT id FROM ids WHERE name = 'plain')),
    '22023'
);

------------------------------------------------------------------------------------------------------------------------
-- Pruning: candidates, inspection, detach, drop
------------------------------------------------------------------------------------------------------------------------

-- standard_1d leaves: expired attached, expired detached (not recorded), expired detached and recorded, future.
SELECT fixture_leaf('standard_1d', '2021-01-01 00:00:00+00');
SELECT fixture_leaf('standard_1d', '2021-01-02 00:00:00+00');
SELECT fixture_leaf('standard_1d', '2021-01-03 00:00:00+00');
SELECT fixture_leaf('standard_1d', date_trunc('day', now()) + interval '1 day');
ALTER TABLE horsies_task_history_standard_1d DETACH PARTITION horsies_task_history_standard_1d_2021_01_02;
ALTER TABLE horsies_task_history_standard_1d DETACH PARTITION horsies_task_history_standard_1d_2021_01_03;
UPDATE horsies_task_history_leaf_catalog SET detached_at = now()
WHERE leaf_name = 'horsies_task_history_standard_1d_2021_01_03';
SELECT fixture_heartbeat_leaf('2021-01-01 05:00:00+00');
SELECT fixture_heartbeat_leaf(date_trunc('hour', now()) + interval '1 hour');

SELECT fixture_check(
    'candidates: expired PERIOD and HEARTBEAT leaves in lower-anchor order, with their attachment',
    (SELECT array_agg(c.leaf_name || ':' || c.attachment || ':' || c.detach_recorded
                      ORDER BY c.lower_anchor, c.leaf_name)
     FROM horsies_prune_candidates(100) AS c)
    -- The catalog rows of the absence cases have no relation (MISSING); the dropped ones are not candidates.
    = ARRAY[
        'absence_1d_b:MISSING:true',
        'absence_1d_c:MISSING:false',
        'absence_2d_a:MISSING:true',
        'absence_1d_d:MISSING:false',
        'horsies_task_history_standard_1d_2021_01_01:ATTACHED:false',
        'horsies_heartbeats_2021_01_01_05:ATTACHED:false',
        'horsies_task_history_standard_1d_2021_01_02:DETACHED:false',
        'horsies_task_history_standard_1d_2021_01_03:DETACHED:true'
    ]::text[]
);
SELECT fixture_check(
    'candidates: expires_at is the upper anchor plus the class retention',
    (SELECT c.expires_at = '2021-01-03 00:00:00+00'::timestamptz
     FROM horsies_prune_candidates(100) AS c WHERE c.leaf_name = 'horsies_task_history_standard_1d_2021_01_01')
    AND (SELECT c.expires_at = '2021-01-01 08:00:00+00'::timestamptz
         FROM horsies_prune_candidates(100) AS c WHERE c.leaf_name = 'horsies_heartbeats_2021_01_01_05')
);
SELECT fixture_check(
    'candidates: the limit keeps the oldest rows',
    (SELECT array_agg(c.leaf_name ORDER BY c.lower_anchor) FROM horsies_prune_candidates(2) AS c)
    = ARRAY['absence_1d_b', 'absence_1d_c']::text[]
);
SELECT fixture_expect_error('candidates refuse: a zero limit', 'SELECT * FROM horsies_prune_candidates(0)', '22023');
SELECT fixture_expect_error('candidates refuse: a NULL limit', 'SELECT * FROM horsies_prune_candidates(NULL)',
    '22023');

-- Inspection through the detach entry point. A call whose effect a check reads runs in its own statement: one
-- statement does not see the writes of a function it calls.
CREATE TEMP TABLE begin_detachable AS
    SELECT * FROM horsies_begin_leaf_detach('horsies_task_history_standard_1d_2021_01_01');
SELECT fixture_check(
    'begin detach: an expired attached leaf is DETACHABLE and the session keeps its leaf lock',
    (SELECT b.outcome = 'DETACHABLE' AND b.parent_name = 'horsies_task_history_standard_1d'
            AND b.expires_at = '2021-01-03 00:00:00+00'::timestamptz
     FROM begin_detachable AS b)
    AND fixture_holds_leaf_lock('standard_1d', '2021-01-01 00:00:00+00')
);
SELECT fixture_check(
    'begin detach: the leaf lock was taken by the call and is released by its owner',
    pg_advisory_unlock(horsies_task_history_leaf_lock_key('standard_1d', '2021-01-01 00:00:00+00'))
);
CREATE TEMP TABLE begin_not_expired AS
    SELECT * FROM horsies_begin_leaf_detach(
        'horsies_task_history_standard_1d_' || to_char(date_trunc('day', now()) + interval '1 day', 'YYYY_MM_DD')
    );
SELECT fixture_check(
    'begin detach: an attached leaf that has not expired is NOT_EXPIRED and leaves no lock',
    (SELECT b.outcome = 'NOT_EXPIRED' FROM begin_not_expired AS b)
    AND NOT fixture_holds_leaf_lock('standard_1d', date_trunc('day', now()) + interval '1 day')
);
-- A leaf detached out of band that the manifest does not name: begin records the detach and publishes nothing (a
-- publication stamps each reader with a comment; the stand-ins of this file have none).
SELECT fixture_leaf('standard_1d', '2021-01-04 00:00:00+00');
ALTER TABLE horsies_task_history_standard_1d DETACH PARTITION horsies_task_history_standard_1d_2021_01_04;
CREATE TEMP TABLE begin_detached AS
    SELECT * FROM horsies_begin_leaf_detach('horsies_task_history_standard_1d_2021_01_04');
SELECT fixture_check(
    'begin detach: a detached leaf is DETACHED, leaves no lock, and its detach is recorded',
    (SELECT b.outcome = 'DETACHED' FROM begin_detached AS b)
    AND NOT fixture_holds_leaf_lock('standard_1d', '2021-01-04 00:00:00+00')
    AND (SELECT detached_at IS NOT NULL FROM horsies_task_history_leaf_catalog
         WHERE leaf_name = 'horsies_task_history_standard_1d_2021_01_04')
    AND obj_description(to_regprocedure('horsies_task_detail_staged(uuid)'), 'pg_proc') IS NULL
);
SELECT fixture_check(
    'begin detach: an expired heartbeat leaf is DETACHABLE from horsies_heartbeats',
    (SELECT b.outcome = 'DETACHABLE' AND b.parent_name = 'horsies_heartbeats' AND b.class_kind = 'HEARTBEAT'
     FROM horsies_begin_leaf_detach('horsies_heartbeats_2021_01_01_05') AS b)
);
SELECT pg_advisory_unlock(horsies_task_history_leaf_lock_key('heartbeats', '2021-01-01 05:00:00+00'));
SELECT fixture_check(
    'begin detach: an unknown name with no relation is NOT_CATALOGED',
    (SELECT b.outcome = 'NOT_CATALOGED' FROM horsies_begin_leaf_detach('horsies_task_history_no_such_leaf') AS b)
);
CREATE TABLE horsies_task_history_standard_1d_2021_02_01 (task_id uuid);
SELECT fixture_check(
    'begin detach: a relation with no catalog row is CATALOG_CONFLICT RELATION_WITHOUT_CATALOG',
    (SELECT b.outcome = 'CATALOG_CONFLICT' AND b.conflict_kind = 'RELATION_WITHOUT_CATALOG'
     FROM horsies_begin_leaf_detach('horsies_task_history_standard_1d_2021_02_01') AS b)
);
SELECT fixture_check(
    'begin detach: a cataloged leaf whose relation is gone is MISSING; a recorded drop is ALREADY_DROPPED',
    (SELECT b.outcome = 'MISSING' FROM horsies_begin_leaf_detach('absence_1d_c') AS b)
    AND (SELECT b.outcome = 'ALREADY_DROPPED' FROM horsies_begin_leaf_detach('absence_1d_a') AS b)
);
SELECT fixture_expect_error(
    'begin detach refuses: the forever leaf',
    $q$SELECT * FROM horsies_begin_leaf_detach('horsies_task_history_forever_open')$q$,
    '22023'
);

-- Leaves in conflict with the catalog.
SELECT fixture_leaf('standard_1d', '2021-01-10 00:00:00+00');
DROP INDEX horsies_task_history_standard_1d_2021_01_10_task_idx;
SELECT fixture_check(
    'begin detach: an attached leaf without its task index is CATALOG_CONFLICT PHYSICAL_NONCONFORMANT',
    (SELECT b.outcome = 'CATALOG_CONFLICT' AND b.conflict_kind = 'PHYSICAL_NONCONFORMANT'
     FROM horsies_begin_leaf_detach('horsies_task_history_standard_1d_2021_01_10') AS b)
);
-- Index conformance of the inspection is the rule of horsies_history_leaf_indexes, applied to the id index only.
SELECT fixture_leaf('standard_1d', '2021-01-20 00:00:00+00');
DROP INDEX horsies_task_history_standard_1d_2021_01_20_task_idx;
CREATE INDEX horsies_task_history_standard_1d_2021_01_20_task_idx
    ON horsies_task_history_standard_1d_2021_01_20 USING btree (task_id DESC);
SELECT fixture_check(
    'inspect: a task index of another shape (task_id DESC) is CATALOG_CONFLICT PHYSICAL_NONCONFORMANT',
    (SELECT i.state = 'CATALOG_CONFLICT' AND i.conflict_kind = 'PHYSICAL_NONCONFORMANT'
     FROM horsies_inspect_history_leaf('horsies_task_history_standard_1d_2021_01_20') AS i)
    AND (SELECT NOT x.conformant
         FROM horsies_history_leaf_indexes('HISTORY', 'horsies_task_history_standard_1d_2021_01_20') AS x
         WHERE x.index_name = 'horsies_task_history_standard_1d_2021_01_20_task_idx')
);
SELECT fixture_leaf('standard_1d', '2021-01-21 00:00:00+00');
DROP INDEX horsies_task_history_standard_1d_2021_01_21_enqueued_idx;
SELECT fixture_check(
    'inspect: a leaf without its enqueued index is still classified by its task index (DETACHABLE)',
    (SELECT i.state = 'DETACHABLE'
     FROM horsies_inspect_history_leaf('horsies_task_history_standard_1d_2021_01_21') AS i)
);
SELECT fixture_leaf('standard_1d', '2021-01-11 00:00:00+00');
UPDATE horsies_task_history_leaf_catalog SET upper_anchor = '2021-01-11 12:00:00+00'
WHERE leaf_name = 'horsies_task_history_standard_1d_2021_01_11';
SELECT fixture_check(
    'begin detach: an attached leaf whose bound differs from the catalog is PHYSICAL_NONCONFORMANT',
    (SELECT b.outcome = 'CATALOG_CONFLICT' AND b.conflict_kind = 'PHYSICAL_NONCONFORMANT'
     FROM horsies_begin_leaf_detach('horsies_task_history_standard_1d_2021_01_11') AS b)
);
SELECT fixture_leaf('standard_2d', '2021-01-12 00:00:00+00');
UPDATE horsies_task_history_leaf_catalog SET class_key = 'standard_1d'
WHERE leaf_name = 'horsies_task_history_standard_2d_2021_01_12';
SELECT fixture_check(
    'begin detach: a leaf attached to another class parent is CATALOG_CONFLICT FOREIGN_PARENT',
    (SELECT b.outcome = 'CATALOG_CONFLICT' AND b.conflict_kind = 'FOREIGN_PARENT'
     FROM horsies_begin_leaf_detach('horsies_task_history_standard_2d_2021_01_12') AS b)
);
SELECT fixture_leaf('standard_1d', '2021-01-13 00:00:00+00');
UPDATE horsies_task_history_leaf_catalog SET detached_at = now()
WHERE leaf_name = 'horsies_task_history_standard_1d_2021_01_13';
SELECT fixture_check(
    'begin detach: a recorded detach of an attached leaf is CATALOG_CONFLICT DETACH_RECORDED_WHILE_ATTACHED',
    (SELECT b.outcome = 'CATALOG_CONFLICT' AND b.conflict_kind = 'DETACH_RECORDED_WHILE_ATTACHED'
     FROM horsies_begin_leaf_detach('horsies_task_history_standard_1d_2021_01_13') AS b)
);
SELECT fixture_leaf('standard_1d', '2021-01-14 00:00:00+00');
UPDATE horsies_task_history_leaf_catalog SET detached_at = now(), dropped_at = now()
WHERE leaf_name = 'horsies_task_history_standard_1d_2021_01_14';
SELECT fixture_check(
    'begin detach: a recorded drop of an existing relation is CATALOG_CONFLICT DROPPED_RELATION_EXISTS',
    (SELECT b.outcome = 'CATALOG_CONFLICT' AND b.conflict_kind = 'DROPPED_RELATION_EXISTS'
     FROM horsies_begin_leaf_detach('horsies_task_history_standard_1d_2021_01_14') AS b)
);

-- A phase-2 pending locator pins its leaf.
SELECT fixture_leaf('standard_1d', '2021-01-15 00:00:00+00');
INSERT INTO ids VALUES ('pinned_task', gen_random_uuid()), ('pinned_node', gen_random_uuid());
SELECT fixture_node((SELECT id FROM ids WHERE name = 'pinned_node'), (SELECT id FROM ids WHERE name = 'workflow'), 5,
    NULL);
INSERT INTO horsies_workflow_phase2_pending (
    task_id, workflow_id, workflow_node_row_id, terminalization_kind, recovery_source, history_class,
    history_anchor, history_schema_version, result_digest, quarantine_task_id, created_at, attempt_count,
    last_attempt_at, last_failure_class
) VALUES (
    (SELECT id FROM ids WHERE name = 'pinned_task'), (SELECT id FROM ids WHERE name = 'workflow'),
    (SELECT id FROM ids WHERE name = 'pinned_node'), 'FAIL_RUNNING', 'HISTORY', 'standard_1d',
    '2021-01-15 10:00:00+00', 1, sha256('r'::bytea), NULL, now(), 0, NULL, NULL
);
CREATE TEMP TABLE begin_pinned AS
    SELECT * FROM horsies_begin_leaf_detach('horsies_task_history_standard_1d_2021_01_15');
SELECT fixture_check(
    'begin detach: a leaf with a pending locator in its interval is PENDING_BLOCKED with the count',
    (SELECT b.outcome = 'PENDING_BLOCKED' AND b.blocker_count = 1 AND b.attachment = 'ATTACHED'
     FROM begin_pinned AS b)
    AND NOT fixture_holds_leaf_lock('standard_1d', '2021-01-15 00:00:00+00')
);

-- Record.
CREATE TEMP TABLE record_attached AS
    SELECT * FROM horsies_record_leaf_detached('horsies_task_history_standard_1d_2021_01_01');
SELECT fixture_check(
    'record: an attached leaf is STILL_ATTACHED and nothing changes',
    (SELECT r.outcome = 'STILL_ATTACHED' AND NOT r.manifest_names_leaf FROM record_attached AS r)
    AND (SELECT detached_at IS NULL FROM horsies_task_history_leaf_catalog
         WHERE leaf_name = 'horsies_task_history_standard_1d_2021_01_01')
);
INSERT INTO horsies_task_lookup_manifest (
    leaf_name, probe_position, lower_bound_kind, lower_anchor, upper_anchor, published_at
) VALUES (
    'horsies_task_history_standard_1d_2021_01_02', 100, 'AT', '2021-01-02 00:00:00+00', '2021-01-03 00:00:00+00', now()
);
CREATE TEMP TABLE record_detached AS
    SELECT * FROM horsies_record_leaf_detached('horsies_task_history_standard_1d_2021_01_02');
SELECT fixture_check(
    'record: a detached leaf is RECORDED, and the flag says the manifest still names it',
    (SELECT r.outcome = 'RECORDED' AND r.manifest_names_leaf FROM record_detached AS r)
    AND (SELECT detached_at IS NOT NULL
         FROM horsies_task_history_leaf_catalog WHERE leaf_name = 'horsies_task_history_standard_1d_2021_01_02')
);
CREATE TEMP TABLE record_again AS
    SELECT 'again' AS call, * FROM horsies_record_leaf_detached('horsies_task_history_standard_1d_2021_01_02')
    UNION ALL SELECT 'missing', * FROM horsies_record_leaf_detached('absence_1d_c')
    UNION ALL SELECT 'unknown', * FROM horsies_record_leaf_detached('no_such_leaf');
SELECT fixture_check(
    'record: a second record is ALREADY_RECORDED; a gone relation is MISSING; an unknown name is NOT_CATALOGED',
    (SELECT array_agg(r.call || ':' || r.outcome ORDER BY r.call) FROM record_again AS r)
        = ARRAY['again:ALREADY_RECORDED', 'missing:MISSING', 'unknown:NOT_CATALOGED']::text[]
    AND (SELECT detached_at IS NULL FROM horsies_task_history_leaf_catalog WHERE leaf_name = 'absence_1d_c')
);

-- Drop.
INSERT INTO horsies_task_history_clock_exceptions (task_id, retention_class_key, terminal_at) VALUES
    (gen_random_uuid(), 'standard_1d', '2021-01-02 10:00:00+00'),
    (gen_random_uuid(), 'standard_1d', '2021-01-03 10:00:00+00'),
    (gen_random_uuid(), 'standard_2d', '2021-01-02 10:00:00+00');
CREATE TEMP TABLE drop_attached AS
    SELECT * FROM horsies_drop_detached_leaf('horsies_task_history_standard_1d_2021_01_01');
SELECT fixture_check(
    'drop: an attached leaf is not dropped; the inspection DETACHABLE comes back',
    (SELECT d.outcome = 'DETACHABLE' FROM drop_attached AS d)
    AND to_regclass('horsies_task_history_standard_1d_2021_01_01') IS NOT NULL
);
CREATE TEMP TABLE drop_referenced AS
    SELECT * FROM horsies_drop_detached_leaf('horsies_task_history_standard_1d_2021_01_02');
SELECT fixture_check(
    'drop: a detached leaf that the manifest names is REFERENCED_BY_MANIFEST and stays',
    (SELECT d.outcome = 'REFERENCED_BY_MANIFEST' FROM drop_referenced AS d)
    AND to_regclass('horsies_task_history_standard_1d_2021_01_02') IS NOT NULL
);
DELETE FROM horsies_task_lookup_manifest WHERE leaf_name = 'horsies_task_history_standard_1d_2021_01_02';
CREATE TEMP TABLE drop_detached AS
    SELECT * FROM horsies_drop_detached_leaf('horsies_task_history_standard_1d_2021_01_02');
SELECT fixture_check(
    'drop: a detached leaf is DROPPED with the clock exceptions of its class and interval; the catalog keeps the row',
    (SELECT d.outcome = 'DROPPED' FROM drop_detached AS d)
    AND to_regclass('horsies_task_history_standard_1d_2021_01_02') IS NULL
    AND (SELECT dropped_at IS NOT NULL AND detached_at IS NOT NULL FROM horsies_task_history_leaf_catalog
         WHERE leaf_name = 'horsies_task_history_standard_1d_2021_01_02')
    AND (SELECT count(*) = 2 FROM horsies_task_history_clock_exceptions
         WHERE terminal_at IN ('2021-01-02 10:00:00+00', '2021-01-03 10:00:00+00'))
    AND NOT EXISTS (
        SELECT 1 FROM horsies_task_history_clock_exceptions
        WHERE retention_class_key = 'standard_1d' AND terminal_at = '2021-01-02 10:00:00+00'
    )
);
CREATE TEMP TABLE drop_again AS
    SELECT * FROM horsies_drop_detached_leaf('horsies_task_history_standard_1d_2021_01_02');
SELECT fixture_check(
    'drop: a dropped leaf is ALREADY_DROPPED on the next call',
    (SELECT d.outcome = 'ALREADY_DROPPED' FROM drop_again AS d)
);
SELECT fixture_leaf('standard_1d', '2021-01-16 00:00:00+00');
ALTER TABLE horsies_task_history_standard_1d DETACH PARTITION horsies_task_history_standard_1d_2021_01_16;
CREATE TEMP TABLE drop_unrecorded AS
    SELECT * FROM horsies_drop_detached_leaf('horsies_task_history_standard_1d_2021_01_16');
SELECT fixture_check(
    'drop: a detached leaf that the catalog did not record yet is DROPPED and gets both times',
    (SELECT d.outcome = 'DROPPED' FROM drop_unrecorded AS d)
    AND (SELECT detached_at IS NOT NULL AND dropped_at IS NOT NULL FROM horsies_task_history_leaf_catalog
         WHERE leaf_name = 'horsies_task_history_standard_1d_2021_01_16')
);
ALTER TABLE horsies_task_history_standard_1d DETACH PARTITION horsies_task_history_standard_1d_2021_01_15;
CREATE TEMP TABLE drop_pinned AS
    SELECT * FROM horsies_drop_detached_leaf('horsies_task_history_standard_1d_2021_01_15');
SELECT fixture_check(
    'drop: a detached leaf with a pending locator is PENDING_BLOCKED and stays',
    (SELECT d.outcome = 'PENDING_BLOCKED' AND d.attachment = 'DETACHED' AND d.blocker_count = 1
     FROM drop_pinned AS d)
    AND to_regclass('horsies_task_history_standard_1d_2021_01_15') IS NOT NULL
);
ALTER TABLE horsies_heartbeats DETACH PARTITION horsies_heartbeats_2021_01_01_05;
CREATE TEMP TABLE drop_heartbeat AS
    SELECT * FROM horsies_drop_detached_leaf('horsies_heartbeats_2021_01_01_05');
SELECT fixture_check(
    'drop: a detached heartbeat leaf is DROPPED',
    (SELECT d.outcome = 'DROPPED' FROM drop_heartbeat AS d)
    AND to_regclass('horsies_heartbeats_2021_01_01_05') IS NULL
);
SELECT fixture_check(
    'drop: dropped leaves are no longer candidates',
    NOT EXISTS (
        SELECT 1 FROM horsies_prune_candidates(100) AS c
        WHERE c.leaf_name IN ('horsies_task_history_standard_1d_2021_01_02', 'horsies_heartbeats_2021_01_01_05')
    )
);

-- The pending row of a moved workflow task pins its leaf until the quarantine copies the result and repoints the
-- row. The leaf then detaches and drops, and phase 2 writes the node from the copy. The move of this script holds
-- the leaf lock in shared mode until the end of the transaction; a lock of this session does not block its own
-- calls, so the lock checks of the cases above are not repeated here.
SELECT fixture_leaf('standard_1d', '2021-01-17 00:00:00+00');
INSERT INTO ids VALUES
    ('repoint_workflow', gen_random_uuid()),
    ('repoint_task', fixture_uuid7(now() - interval '1 minute')),
    ('repoint_node', gen_random_uuid());
SELECT fixture_workflow((SELECT id FROM ids WHERE name = 'repoint_workflow'));
SELECT fixture_task((SELECT id FROM ids WHERE name = 'repoint_task'), 'RUNNING', 'standard_1d', TRUE, TRUE,
    '{"a":9}', NULL, NULL);
SELECT fixture_node((SELECT id FROM ids WHERE name = 'repoint_node'),
    (SELECT id FROM ids WHERE name = 'repoint_workflow'), 0, (SELECT id FROM ids WHERE name = 'repoint_task'));
SELECT fixture_move((SELECT id FROM ids WHERE name = 'repoint_task'), 'FAILED', 'FAIL_RUNNING',
    '2021-01-17 10:00:00+00', '{"e":9}', 'E', 'r');
CREATE TEMP TABLE begin_repoint_blocked AS
    SELECT * FROM horsies_begin_leaf_detach('horsies_task_history_standard_1d_2021_01_17');
SELECT fixture_check(
    'quarantine repoint: the pending row of a moved workflow task makes its leaf PENDING_BLOCKED',
    (SELECT b.outcome = 'PENDING_BLOCKED' AND b.blocker_count = 1 AND b.attachment = 'ATTACHED'
     FROM begin_repoint_blocked AS b)
);
CREATE TEMP TABLE quarantine_repoint AS
    SELECT * FROM horsies_phase2_quarantine_one((SELECT id FROM ids WHERE name = 'repoint_task'));
SELECT fixture_check(
    'quarantine repoint: the verified copy is REPOINTED and the pending row no longer names the leaf',
    (SELECT q.verdict = 'REPOINTED' FROM quarantine_repoint AS q)
    AND (SELECT p.recovery_source = 'QUARANTINE' AND p.history_class IS NULL AND p.history_anchor IS NULL
                AND p.quarantine_task_id = p.task_id
         FROM horsies_workflow_phase2_pending AS p WHERE p.task_id = (SELECT id FROM ids WHERE name = 'repoint_task'))
    AND (SELECT convert_from(q.result_payload, 'UTF8') = '{"e":9}' AND q.retention_class_key = 'standard_1d'
                AND q.terminal_at = '2021-01-17 10:00:00+00'
         FROM horsies_workflow_phase2_quarantine AS q
         WHERE q.task_id = (SELECT id FROM ids WHERE name = 'repoint_task'))
);
CREATE TEMP TABLE begin_repointed AS
    SELECT * FROM horsies_begin_leaf_detach('horsies_task_history_standard_1d_2021_01_17');
SELECT fixture_check(
    'quarantine repoint: with the pending row repointed, the leaf is DETACHABLE',
    (SELECT b.outcome = 'DETACHABLE' FROM begin_repointed AS b)
);
ALTER TABLE horsies_task_history_standard_1d DETACH PARTITION horsies_task_history_standard_1d_2021_01_17;
CREATE TEMP TABLE record_repointed AS
    SELECT * FROM horsies_record_leaf_detached('horsies_task_history_standard_1d_2021_01_17');
SELECT fixture_check(
    'quarantine repoint: after the detach the history row is in the detached leaf, not in the history tree',
    (SELECT r.outcome = 'RECORDED' FROM record_repointed AS r)
    AND NOT EXISTS (
        SELECT 1 FROM horsies_task_history WHERE task_id = (SELECT id FROM ids WHERE name = 'repoint_task')
    )
    AND EXISTS (
        SELECT 1 FROM horsies_task_history_standard_1d_2021_01_17
        WHERE task_id = (SELECT id FROM ids WHERE name = 'repoint_task')
    )
);
SELECT pg_advisory_unlock(horsies_task_history_leaf_lock_key('standard_1d', '2021-01-17 00:00:00+00'));
CREATE TEMP TABLE drop_repointed AS
    SELECT * FROM horsies_drop_detached_leaf('horsies_task_history_standard_1d_2021_01_17');
SELECT fixture_check(
    'quarantine repoint: the detached leaf is DROPPED; the quarantine copy and the repointed pending row stay',
    (SELECT d.outcome = 'DROPPED' FROM drop_repointed AS d)
    AND to_regclass('horsies_task_history_standard_1d_2021_01_17') IS NULL
    AND EXISTS (
        SELECT 1 FROM horsies_workflow_phase2_quarantine
        WHERE task_id = (SELECT id FROM ids WHERE name = 'repoint_task')
    )
    AND (SELECT p.recovery_source = 'QUARANTINE' FROM horsies_workflow_phase2_pending AS p
         WHERE p.task_id = (SELECT id FROM ids WHERE name = 'repoint_task'))
);
CREATE TEMP TABLE consume_repointed AS
    SELECT * FROM horsies_phase2_consume((SELECT id FROM ids WHERE name = 'repoint_task'), 'FAILED');
SELECT fixture_check(
    'quarantine repoint: phase 2 writes the node from the copy after the drop and deletes both recovery rows',
    (SELECT c.disposition = 'APPLIED_TO_NODE' FROM consume_repointed AS c)
    AND (SELECT n.status = 'FAILED' AND n.result = '{"e":9}'
         FROM horsies_workflow_tasks AS n WHERE n.id = (SELECT id FROM ids WHERE name = 'repoint_node'))
    AND NOT EXISTS (
        SELECT 1 FROM horsies_workflow_phase2_pending WHERE task_id = (SELECT id FROM ids WHERE name = 'repoint_task')
    )
    AND NOT EXISTS (
        SELECT 1 FROM horsies_workflow_phase2_quarantine
        WHERE task_id = (SELECT id FROM ids WHERE name = 'repoint_task')
    )
);

------------------------------------------------------------------------------------------------------------------------
-- Phase 2 and the format versions of its source row
------------------------------------------------------------------------------------------------------------------------

-- horsies_phase2_consume and horsies_phase2_quarantine_one (0003_interim_functions.sql) read the rows that the move
-- writes. Each knows history schema version 1 and result envelope version 1 only. A later writer is played by an
-- UPDATE of the stored version; the CHECKs allow every positive version.

-- The record statement of the recovery pass for a disposition that keeps the pending row
-- (core/src/workflow_engine/phase2_recovery.rs, RECORD_RETAINING_ATTEMPT_SQL).
CREATE FUNCTION fixture_record_retaining_attempt(p_task uuid, p_failure_class text) RETURNS integer
LANGUAGE sql AS $$
    UPDATE horsies_workflow_phase2_pending
    SET attempt_count = attempt_count + 1,
        last_attempt_at = statement_timestamp(),
        last_failure_class = p_failure_class
    WHERE task_id = p_task
    RETURNING attempt_count
$$;

INSERT INTO ids VALUES
    ('version_workflow', gen_random_uuid()),
    ('version_history', fixture_uuid7(now() - interval '1 minute')),
    ('version_history_node', gen_random_uuid()),
    ('version_quarantine', fixture_uuid7(now() - interval '1 minute')),
    ('version_quarantine_node', gen_random_uuid());
SELECT fixture_workflow((SELECT id FROM ids WHERE name = 'version_workflow'));
SELECT fixture_task((SELECT id FROM ids WHERE name = 'version_history'), 'RUNNING', 'forever', TRUE, TRUE,
    '{"v":1}', NULL, NULL);
SELECT fixture_node((SELECT id FROM ids WHERE name = 'version_history_node'),
    (SELECT id FROM ids WHERE name = 'version_workflow'), 0, (SELECT id FROM ids WHERE name = 'version_history'));
SELECT fixture_move((SELECT id FROM ids WHERE name = 'version_history'), 'FAILED', 'FAIL_RUNNING', now(),
    '{"e":"v"}', 'E', 'r');
SELECT fixture_task((SELECT id FROM ids WHERE name = 'version_quarantine'), 'RUNNING', 'forever', TRUE, TRUE,
    '{"v":2}', NULL, NULL);
SELECT fixture_node((SELECT id FROM ids WHERE name = 'version_quarantine_node'),
    (SELECT id FROM ids WHERE name = 'version_workflow'), 1, (SELECT id FROM ids WHERE name = 'version_quarantine'));
SELECT fixture_move((SELECT id FROM ids WHERE name = 'version_quarantine'), 'FAILED', 'FAIL_RUNNING', now(),
    '{"e":"q"}', 'E', 'r');

-- A history source with result envelope version 2.
UPDATE horsies_task_history SET result_envelope_version = 2
WHERE task_id = (SELECT id FROM ids WHERE name = 'version_history');
CREATE TEMP TABLE consume_envelope AS
    SELECT * FROM horsies_phase2_consume((SELECT id FROM ids WHERE name = 'version_history'), 'FAILED');
SELECT fixture_check(
    'phase 2 consume: a history row with an unknown result envelope version is SOURCE_VERSION_CONFLICT',
    (SELECT c.disposition = 'SOURCE_VERSION_CONFLICT'
            AND c.detail = 'source result envelope version 2 is not known to this reader'
     FROM consume_envelope AS c)
);
SELECT fixture_check(
    'phase 2 consume: the refusal changes nothing: the node keeps its status and the pending row its locator',
    (SELECT n.status = 'RUNNING' AND n.result IS NULL
     FROM horsies_workflow_tasks AS n WHERE n.id = (SELECT id FROM ids WHERE name = 'version_history_node'))
    AND (SELECT p.recovery_source = 'HISTORY' AND p.attempt_count = 0 AND p.last_failure_class IS NULL
         FROM horsies_workflow_phase2_pending AS p
         WHERE p.task_id = (SELECT id FROM ids WHERE name = 'version_history'))
);
CREATE TEMP TABLE record_envelope AS
    SELECT fixture_record_retaining_attempt(
        (SELECT id FROM ids WHERE name = 'version_history'), (SELECT c.disposition FROM consume_envelope AS c)
    ) AS attempt_count;
SELECT fixture_check(
    'phase 2 recovery: the record statement stores SOURCE_VERSION_CONFLICT as the failure class',
    (SELECT r.attempt_count = 1 FROM record_envelope AS r)
    AND (SELECT p.last_failure_class = 'SOURCE_VERSION_CONFLICT' AND p.last_attempt_at IS NOT NULL
         FROM horsies_workflow_phase2_pending AS p
         WHERE p.task_id = (SELECT id FROM ids WHERE name = 'version_history'))
);
CREATE TEMP TABLE quarantine_envelope AS
    SELECT * FROM horsies_phase2_quarantine_one((SELECT id FROM ids WHERE name = 'version_history'));
SELECT fixture_check(
    'phase 2 quarantine: a history row with an unknown result envelope version is COPY_VERIFICATION_FAILED',
    (SELECT q.verdict = 'COPY_VERIFICATION_FAILED'
            AND q.detail = 'quarantine source result envelope version 2 is not known to this reader'
     FROM quarantine_envelope AS q)
    AND NOT EXISTS (
        SELECT 1 FROM horsies_workflow_phase2_quarantine
        WHERE task_id = (SELECT id FROM ids WHERE name = 'version_history')
    )
    AND (SELECT p.recovery_source = 'HISTORY' AND p.history_class = 'forever'
         FROM horsies_workflow_phase2_pending AS p
         WHERE p.task_id = (SELECT id FROM ids WHERE name = 'version_history'))
);

-- The same source with history schema version 2: the refusal that the envelope version now shares.
UPDATE horsies_task_history SET result_envelope_version = 1, history_schema_version = 2
WHERE task_id = (SELECT id FROM ids WHERE name = 'version_history');
SELECT fixture_check(
    'phase 2 consume: a history row with an unknown history schema version is SOURCE_VERSION_CONFLICT too',
    (SELECT c.disposition = 'SOURCE_VERSION_CONFLICT' AND c.detail = 'source schema version disagrees with locator'
     FROM horsies_phase2_consume((SELECT id FROM ids WHERE name = 'version_history'), 'FAILED') AS c)
);

-- Control: with both versions known again, the same pending row applies.
UPDATE horsies_task_history SET history_schema_version = 1
WHERE task_id = (SELECT id FROM ids WHERE name = 'version_history');
CREATE TEMP TABLE consume_known AS
    SELECT * FROM horsies_phase2_consume((SELECT id FROM ids WHERE name = 'version_history'), 'FAILED');
SELECT fixture_check(
    'phase 2 consume: with both versions known, the same row is APPLIED_TO_NODE',
    (SELECT c.disposition = 'APPLIED_TO_NODE' FROM consume_known AS c)
    AND NOT EXISTS (
        SELECT 1 FROM horsies_workflow_phase2_pending
        WHERE task_id = (SELECT id FROM ids WHERE name = 'version_history')
    )
);

-- A quarantine source with result envelope version 2.
SELECT fixture_check(
    'phase 2 quarantine: fixture copy of a known row is REPOINTED',
    (SELECT q.verdict = 'REPOINTED'
     FROM horsies_phase2_quarantine_one((SELECT id FROM ids WHERE name = 'version_quarantine')) AS q)
);
UPDATE horsies_workflow_phase2_quarantine SET result_envelope_version = 2
WHERE task_id = (SELECT id FROM ids WHERE name = 'version_quarantine');
SELECT fixture_check(
    'phase 2 consume: a quarantine row with an unknown result envelope version is SOURCE_VERSION_CONFLICT',
    (SELECT c.disposition = 'SOURCE_VERSION_CONFLICT'
            AND c.detail = 'source result envelope version 2 is not known to this reader'
     FROM horsies_phase2_consume((SELECT id FROM ids WHERE name = 'version_quarantine'), 'FAILED') AS c)
    AND EXISTS (
        SELECT 1 FROM horsies_workflow_phase2_quarantine
        WHERE task_id = (SELECT id FROM ids WHERE name = 'version_quarantine')
    )
    AND (SELECT n.status = 'RUNNING'
         FROM horsies_workflow_tasks AS n WHERE n.id = (SELECT id FROM ids WHERE name = 'version_quarantine_node'))
);
UPDATE horsies_workflow_phase2_quarantine SET result_envelope_version = 1
WHERE task_id = (SELECT id FROM ids WHERE name = 'version_quarantine');
SELECT fixture_check(
    'phase 2 consume: with the quarantine row known again, it is APPLIED_TO_NODE',
    (SELECT c.disposition = 'APPLIED_TO_NODE'
     FROM horsies_phase2_consume((SELECT id FROM ids WHERE name = 'version_quarantine'), 'FAILED') AS c)
);
