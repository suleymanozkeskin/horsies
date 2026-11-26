-- Cases for the functions of 0007_scheduler_monitoring_functions.sql.
--
-- Preconditions, set up by the runner (core/src/sql_tests/baseline_scheduler_monitoring_sql.rs):
-- - a fresh database that holds the baseline migration;
-- - the history readers published by horsies_publish_history_readers (on a fresh install they probe the forever
--   leaf);
-- - the dblink extension, and the setting horsies_test.peer_connstr: a libpq connection string to this database,
--   which the two-session cases use for their second session.
--
-- Each case calls test_check or test_raises. A failed case raises 'CASE FAILED <name>: ...' and stops the script.
-- Each passed case takes one value of the sequence test_passed_cases (a sequence survives a rollback, so the cases
-- that run inside a rolled-back transaction count too). The script is plain SQL, so that psql (one statement at a
-- time) and a driver's simple query protocol (the whole text as one message) run it the same way. In one message,
-- statements before a BEGIN join its transaction block, so every BEGIN, and every hand-off to the second session,
-- follows a COMMIT; psql reports that COMMIT with the warning "there is no transaction in progress".

------------------------------------------------------------------------------------------------------------------------
-- Test helpers
------------------------------------------------------------------------------------------------------------------------

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

-- Runs p_sql in a subtransaction and expects it to raise p_sqlstate. The subtransaction is rolled back, so the
-- case also proves that the refused call left nothing behind only when the caller checks the tables after it.
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

CREATE FUNCTION test_live_task(
    p_id uuid,
    p_status text,
    p_task_name text,
    p_queue text,
    p_worker text,
    p_enqueued_at timestamptz,
    p_started_at timestamptz,
    p_retry_count integer,
    p_good_until timestamptz
) RETURNS void
    LANGUAGE sql AS $$
    INSERT INTO horsies_tasks (
        id, task_name, queue_name, priority, input, task_options, execution, retention_class_key, sent_at,
        created_at, command_fingerprint_version, command_fingerprint, idempotency_key_digest, status, enqueued_at,
        claimed_at, claimed_by_worker_id, claim_expires_at, started_at, finalizing_at, finalizing_by_worker_id,
        worker_pid, worker_hostname, worker_process_name, retry_count, max_retries, next_retry_at, good_until,
        is_workflow_task, updated_at, retain_rerun_input, rerun_of_task_id, rerun_root_task_id
    ) VALUES (
        p_id, p_task_name, p_queue, 50, '{"x":1}', NULL, 'SHARED_ASYNC', 'forever', p_enqueued_at,
        p_enqueued_at, 1, sha256('fixture'::bytea), NULL, p_status, p_enqueued_at,
        CASE WHEN p_status = 'PENDING' THEN NULL ELSE p_enqueued_at END,
        CASE WHEN p_status = 'PENDING' THEN NULL ELSE p_worker END,
        CASE WHEN p_status = 'CLAIMED' THEN now() + interval '1 minute' END,
        CASE WHEN p_status = 'RUNNING' THEN p_started_at END,
        NULL, NULL,
        CASE WHEN p_status = 'RUNNING' THEN 4242 END,
        CASE WHEN p_status = 'RUNNING' THEN 'host-live' END,
        CASE WHEN p_status = 'RUNNING' THEN 'proc-live' END,
        p_retry_count, 3, NULL, p_good_until,
        FALSE, now(), TRUE, NULL, NULL
    )
$$;

CREATE FUNCTION test_history_task(
    p_id uuid,
    p_status text,
    p_task_name text,
    p_queue text,
    p_worker text,
    p_enqueued_at timestamptz,
    p_started_at timestamptz,
    p_terminal_at timestamptz,
    p_retry_count integer,
    p_error_code text
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
        p_id, p_task_name, p_queue, 50, 1, sha256('fixture'::bytea),
        'forever', FALSE, NULL, NULL, NULL, p_enqueued_at,
        p_enqueued_at, p_enqueued_at, p_started_at, p_started_at, NULL, p_terminal_at, p_status,
        CASE p_status
            WHEN 'COMPLETED' THEN 'COMPLETE_FUSED'
            WHEN 'FAILED' THEN 'FAIL_RUNNING'
            WHEN 'CANCELLED' THEN 'CANCEL_ADMIN'
            ELSE 'EXPIRE_CLAIMED'
        END,
        p_retry_count, 3, p_worker, 'host-history', 5151,
        'proc-history', p_error_code, CASE WHEN p_status = 'FAILED' THEN 'reason of ' || p_id END, 1,
        CASE WHEN p_status = 'CANCELLED' THEN NULL ELSE convert_to('{"ok":true}', 'UTF8') END,
        CASE WHEN p_status = 'CANCELLED' THEN NULL ELSE sha256(convert_to('{"ok":true}', 'UTF8')) END,
        1, 1, convert_to('[]', 'UTF8'), sha256(convert_to('[]', 'UTF8')),
        'NEVER_ELIGIBLE', NULL, NULL, NULL
    )
$$;

CREATE FUNCTION test_attempt(p_task_id uuid, p_attempt integer, p_outcome text, p_will_retry boolean)
RETURNS void
    LANGUAGE sql AS $$
    INSERT INTO horsies_task_attempts (
        task_id, attempt, outcome, will_retry, started_at, finished_at, error_code, error_message, failed_reason,
        worker_id, worker_hostname, worker_pid, worker_process_name
    ) VALUES (
        p_task_id, p_attempt, p_outcome, p_will_retry, now() - interval '1 minute', now(), 'E_' || p_attempt,
        'message ' || p_attempt, NULL, 'worker-a', 'host-a', 7, 'proc-a'
    )
$$;

-- A UUIDv7 with the given birth (random tail), for the absence cases.
CREATE FUNCTION test_uuid7_at(p_birth timestamptz) RETURNS uuid
    LANGUAGE sql AS $$
    SELECT encode(
        overlay(
            overlay(
                uuid_send(gen_random_uuid())
                PLACING substring(int8send(floor(extract(epoch FROM p_birth) * 1000)::bigint) FROM 3)
                FROM 1 FOR 6
            )
            PLACING '\x7000'::bytea FROM 7 FOR 2
        ),
        'hex'
    )::uuid
$$;

-- The arguments of one schedule fire: everything except the name, the times and the slots.
CREATE FUNCTION test_fire(
    p_name text,
    p_expected timestamptz,
    p_next timestamptz,
    p_slots timestamptz[],
    p_fingerprint bytea
) RETURNS TABLE (row_kind text, slot_at timestamptz, task_id uuid, slot_outcome text)
    LANGUAGE sql AS $$
    SELECT * FROM horsies_fire_schedule(
        p_name, p_expected, p_next, p_slots, 'app.scheduled', 'default', 70, '{"day":"monday"}',
        '{"timeout_ms":5000}', 'SHARED_THREAD', 2, 'standard_30d', TRUE, 1::smallint, p_fingerprint
    )
$$;

-- The rows of the last fire. A check reads the state row in a statement after the fire: a statement does not see
-- the writes of a function that it calls.
CREATE TABLE test_last_fire (row_kind text, slot_at timestamptz, task_id uuid, slot_outcome text);

CREATE FUNCTION test_schedule_row(p_name text, p_next_run_at timestamptz) RETURNS void
    LANGUAGE sql AS $$
    INSERT INTO horsies_schedule_state (
        schedule_name, next_run_at, last_run_at, last_task_id, run_count, config_hash, updated_at
    ) VALUES (p_name, p_next_run_at, NULL, NULL, 0, sha256(convert_to(p_name, 'UTF8')), now())
$$;

-- The PERIOD class that the fire writes into (horsies_tasks has no foreign key to classes; the row is for the
-- absence cases, which join the catalog to the classes).
INSERT INTO horsies_retention_classes (class_key, class_kind, retention_days, heartbeat_horizon_hours, created_at)
VALUES ('standard_30d', 'PERIOD', 30, NULL, now()),
       ('standard_7d', 'PERIOD', 7, NULL, now()),
       ('heartbeats', 'HEARTBEAT', NULL, 48, now());

-- One heartbeat leaf around now, with the probe index of the partition manager.
DO $$
BEGIN
    EXECUTE format(
        'CREATE TABLE test_heartbeats_now PARTITION OF horsies_heartbeats FOR VALUES FROM (%L) TO (%L)',
        date_trunc('hour', now()) - interval '3 days', date_trunc('hour', now()) + interval '1 day'
    );
    EXECUTE 'CREATE INDEX test_heartbeats_now_probe ON test_heartbeats_now (task_id, role, sent_at DESC)';
END;
$$;

------------------------------------------------------------------------------------------------------------------------
-- horsies_schedule_slot_task_id
------------------------------------------------------------------------------------------------------------------------

SELECT test_check(
    'slot id: the same name and slot give the same id; another name or slot gives another id',
    horsies_schedule_slot_task_id('daily', '2026-11-16 10:00:00+00')
        = horsies_schedule_slot_task_id('daily', '2026-11-16 12:00:00+02')
    AND horsies_schedule_slot_task_id('daily', '2026-11-16 10:00:00+00')
        <> horsies_schedule_slot_task_id('hourly', '2026-11-16 10:00:00+00')
    AND horsies_schedule_slot_task_id('daily', '2026-11-16 10:00:00+00')
        <> horsies_schedule_slot_task_id('daily', '2026-11-16 10:00:00.001+00')
    AND horsies_schedule_slot_task_id('daily', '2026-11-16 10:00:00+00')
        <> horsies_schedule_slot_task_id('daily', '2026-11-16 10:00:00.000001+00'),
    'slot ids are not a function of (name, slot)'
);

SELECT test_check(
    'slot id: UUIDv7 layout, version 7, variant 10, birth = slot time in milliseconds',
    (get_byte(uuid_send(id), 6) >> 4) = 7
    AND (get_byte(uuid_send(id), 8) & 192) = 128
    AND horsies_uuid7_birth(id) = '2026-11-16 10:00:00.123+00',
    id::text
)
FROM (SELECT horsies_schedule_slot_task_id('daily', '2026-11-16 10:00:00.123456+00') AS id) AS slot;

-- Pinned vector, computed outside the database from the layout in the function header (Python hashlib):
-- ms = 1794823200000, sha256(b'horsies.schedule_slot.v1\x00' + (1794823200000000).to_bytes(8, 'big') + b'daily'),
-- first 10 bytes, byte 0 = 0x70 | (byte 0 & 0x0F), byte 2 = 0x80 | (byte 2 & 0x3F), after the 6 bytes of ms.
SELECT test_check(
    'slot id: pinned vector for (daily, 2026-11-16 10:00:00+00)',
    horsies_schedule_slot_task_id('daily', '2026-11-16 10:00:00+00') = '01a1e3cc-9d00-7a2c-be43-740ce1fde7de'::uuid,
    horsies_schedule_slot_task_id('daily', '2026-11-16 10:00:00+00')::text
);

SELECT test_raises(
    'slot id refuses a slot before 1970',
    $q$SELECT horsies_schedule_slot_task_id('daily', '1969-12-31 23:59:59+00')$q$,
    '22023'
);
SELECT test_raises(
    'slot id refuses a NULL name',
    $q$SELECT horsies_schedule_slot_task_id(NULL, now())$q$,
    '22004'
);

------------------------------------------------------------------------------------------------------------------------
-- horsies_fire_schedule
------------------------------------------------------------------------------------------------------------------------

SELECT test_schedule_row('single', date_trunc('minute', now()) - interval '1 minute');

TRUNCATE test_last_fire;
INSERT INTO test_last_fire SELECT * FROM test_fire(
    'single', date_trunc('minute', now()) - interval '1 minute', date_trunc('minute', now()) + interval '1 minute',
    ARRAY[date_trunc('minute', now()) - interval '1 minute'], sha256('cmd-single'::bytea)
);
SELECT test_check(
    'fire: one due slot is inserted and the call answers ADVANCED then one SLOT row',
    array_agg(f.row_kind || ':' || COALESCE(f.slot_outcome, '-') ORDER BY f.row_kind) = ARRAY['ADVANCED:-', 'SLOT:INSERTED']
    AND bool_and(f.row_kind = 'ADVANCED' OR f.task_id = horsies_schedule_slot_task_id('single', f.slot_at)),
    array_agg(f.row_kind || ':' || COALESCE(f.slot_outcome, '-'))::text
)
FROM test_last_fire AS f;

SELECT test_check(
    'fire: the slot row is an unkeyed send with the task options, retry limit, class and fingerprint, and no trace',
    t.status = 'PENDING' AND t.sent_at = date_trunc('minute', now()) - interval '1 minute'
    AND t.enqueued_at = t.created_at AND t.updated_at = t.created_at AND t.created_at > now() - interval '1 minute'
    AND t.task_name = 'app.scheduled' AND t.queue_name = 'default' AND t.priority = 70
    AND t.input = '{"day":"monday"}' AND t.task_options = '{"timeout_ms":5000}' AND t.execution = 'SHARED_THREAD'
    AND t.max_retries = 2 AND t.retry_count = 0 AND t.retention_class_key = 'standard_30d' AND t.retain_rerun_input
    AND t.command_fingerprint_version = 1 AND t.command_fingerprint = sha256('cmd-single'::bytea)
    AND t.good_until IS NULL AND t.idempotency_key_digest IS NULL AND NOT t.is_workflow_task
    AND t.rerun_of_task_id IS NULL AND t.claimed_at IS NULL AND t.trace_carrier IS NULL AND t.trace_id IS NULL,
    row_to_json(t)::text
)
FROM horsies_tasks AS t
WHERE t.id = horsies_schedule_slot_task_id('single', date_trunc('minute', now()) - interval '1 minute');

SELECT test_check(
    'fire: the state row records one run with the database time of the fire and moves next_run_at',
    s.next_run_at = date_trunc('minute', now()) + interval '1 minute'
    AND s.last_run_at = s.updated_at
    AND s.last_run_at = (SELECT t.created_at FROM horsies_tasks AS t WHERE t.id = s.last_task_id)
    AND s.run_count = 1
    AND s.last_task_id = horsies_schedule_slot_task_id('single', date_trunc('minute', now()) - interval '1 minute'),
    row_to_json(s)::text
)
FROM horsies_schedule_state AS s
WHERE s.schedule_name = 'single';

SELECT test_schedule_row('catchup', date_trunc('minute', now()) - interval '3 minutes');

TRUNCATE test_last_fire;
INSERT INTO test_last_fire SELECT * FROM test_fire(
    'catchup', date_trunc('minute', now()) - interval '3 minutes', date_trunc('minute', now()),
    ARRAY[date_trunc('minute', now()) - interval '3 minutes', date_trunc('minute', now()) - interval '2 minutes',
          date_trunc('minute', now()) - interval '1 minute'],
    sha256('cmd-catchup'::bytea)
);
SELECT test_check(
    'fire: a catch-up fire inserts every slot in order and counts one run',
    array_agg(f.slot_outcome ORDER BY f.slot_at) FILTER (WHERE f.row_kind = 'SLOT')
        = ARRAY['INSERTED', 'INSERTED', 'INSERTED']
    AND (SELECT s.run_count = 1
            AND s.last_task_id = horsies_schedule_slot_task_id('catchup', date_trunc('minute', now()) - interval '1 minute')
            AND s.next_run_at = date_trunc('minute', now())
         FROM horsies_schedule_state AS s WHERE s.schedule_name = 'catchup'),
    array_agg(f.slot_outcome)::text
)
FROM test_last_fire AS f;

-- A slot whose task already exists with the same command: the state row was moved back by hand (as a restore
-- would do), and the fire repeats the slot.
UPDATE horsies_schedule_state
SET next_run_at = date_trunc('minute', now()) - interval '1 minute'
WHERE schedule_name = 'single';

TRUNCATE test_last_fire;
INSERT INTO test_last_fire SELECT * FROM test_fire(
    'single', date_trunc('minute', now()) - interval '1 minute', date_trunc('minute', now()) + interval '1 minute',
    ARRAY[date_trunc('minute', now()) - interval '1 minute'], sha256('cmd-single'::bytea)
);
SELECT test_check(
    'fire: a repeated slot with the same command is SAME_COMMAND and counts as fired',
    (array_agg(f.slot_outcome) FILTER (WHERE f.row_kind = 'SLOT')) = ARRAY['SAME_COMMAND']
    AND (SELECT count(*) FROM horsies_tasks AS t
         WHERE t.id = horsies_schedule_slot_task_id('single', date_trunc('minute', now()) - interval '1 minute')) = 1
    AND (SELECT s.run_count FROM horsies_schedule_state AS s WHERE s.schedule_name = 'single') = 2,
    array_agg(f.slot_outcome)::text
)
FROM test_last_fire AS f;

UPDATE horsies_schedule_state
SET next_run_at = date_trunc('minute', now()) - interval '1 minute'
WHERE schedule_name = 'single';

TRUNCATE test_last_fire;
INSERT INTO test_last_fire SELECT * FROM test_fire(
    'single', date_trunc('minute', now()) - interval '1 minute', date_trunc('minute', now()) + interval '1 minute',
    ARRAY[date_trunc('minute', now()) - interval '1 minute'], sha256('cmd-single-changed-options'::bytea)
);
SELECT test_check(
    'fire: a slot whose id holds another command is PAYLOAD_MISMATCH and counts as fired',
    (array_agg(f.slot_outcome) FILTER (WHERE f.row_kind = 'SLOT')) = ARRAY['PAYLOAD_MISMATCH']
    AND (SELECT t.command_fingerprint FROM horsies_tasks AS t
         WHERE t.id = horsies_schedule_slot_task_id('single', date_trunc('minute', now()) - interval '1 minute'))
        = sha256('cmd-single'::bytea)
    AND (SELECT s.run_count = 3 AND s.next_run_at = date_trunc('minute', now()) + interval '1 minute'
         FROM horsies_schedule_state AS s WHERE s.schedule_name = 'single'),
    array_agg(f.slot_outcome)::text
)
FROM test_last_fire AS f;

TRUNCATE test_last_fire;
INSERT INTO test_last_fire SELECT * FROM test_fire(
    'nobody', date_trunc('minute', now()), date_trunc('minute', now()) + interval '1 minute',
    ARRAY[date_trunc('minute', now())], sha256('cmd-nobody'::bytea)
);
SELECT test_check(
    'fire: no state row answers STATE_MISSING and writes nothing',
    array_agg(f.row_kind) = ARRAY['STATE_MISSING']
    AND NOT EXISTS (SELECT 1 FROM horsies_tasks AS t
                    WHERE t.id = horsies_schedule_slot_task_id('nobody', date_trunc('minute', now())))
    AND NOT EXISTS (SELECT 1 FROM horsies_schedule_state AS s WHERE s.schedule_name = 'nobody'),
    array_agg(f.row_kind)::text
)
FROM test_last_fire AS f;

TRUNCATE test_last_fire;
INSERT INTO test_last_fire SELECT * FROM test_fire(
    'single', date_trunc('minute', now()) - interval '5 minutes', date_trunc('minute', now()) - interval '4 minutes',
    ARRAY[date_trunc('minute', now()) - interval '5 minutes'], sha256('cmd-single'::bytea)
);
SELECT test_check(
    'fire: a moved state row answers STATE_MOVED and writes nothing',
    array_agg(f.row_kind) = ARRAY['STATE_MOVED']
    AND NOT EXISTS (SELECT 1 FROM horsies_tasks AS t
                    WHERE t.id = horsies_schedule_slot_task_id('single', date_trunc('minute', now()) - interval '5 minutes'))
    AND (SELECT s.run_count = 3 AND s.next_run_at = date_trunc('minute', now()) + interval '1 minute'
         FROM horsies_schedule_state AS s WHERE s.schedule_name = 'single'),
    array_agg(f.row_kind)::text
)
FROM test_last_fire AS f;

SELECT test_schedule_row('skip', date_trunc('minute', now()) - interval '2 minutes');

TRUNCATE test_last_fire;
INSERT INTO test_last_fire SELECT * FROM test_fire(
    'skip', date_trunc('minute', now()) - interval '2 minutes', date_trunc('minute', now()) - interval '1 minute',
    ARRAY[]::timestamptz[], sha256('cmd-skip'::bytea)
);
SELECT test_check(
    'fire: with no slots the call moves next_run_at and records no run',
    array_agg(f.row_kind) = ARRAY['ADVANCED']
    AND (SELECT s.next_run_at = date_trunc('minute', now()) - interval '1 minute' AND s.run_count = 0
            AND s.last_run_at IS NULL AND s.last_task_id IS NULL
         FROM horsies_schedule_state AS s WHERE s.schedule_name = 'skip'),
    array_agg(f.row_kind)::text
)
FROM test_last_fire AS f;

-- A refusal that repeats: the slot row breaks a CHECK of horsies_tasks (a queue name over 52 bytes). The statement
-- fails as a whole: no task row, and the state row does not move.
SELECT test_raises(
    'fire: a slot that the task table refuses fails the call',
    format(
        $q$SELECT * FROM horsies_fire_schedule('skip', %L, %L, ARRAY[%L]::timestamptz[], 'app.scheduled', %L, 70,
            '{}', NULL, 'ISOLATED', 0, 'forever', FALSE, 1::smallint, sha256('x'::bytea))$q$,
        date_trunc('minute', now()) - interval '1 minute', date_trunc('minute', now()),
        date_trunc('minute', now()) - interval '1 minute', repeat('q', 53)
    ),
    '23514'
);
SELECT test_check(
    'fire: after the refused call the state row and the task table are unchanged',
    (SELECT s.next_run_at = date_trunc('minute', now()) - interval '1 minute' AND s.run_count = 0
     FROM horsies_schedule_state AS s WHERE s.schedule_name = 'skip')
    AND NOT EXISTS (SELECT 1 FROM horsies_tasks AS t
                    WHERE t.id = horsies_schedule_slot_task_id('skip', date_trunc('minute', now()) - interval '1 minute')),
    'state moved or a task row was written'
);

-- The slot enqueue and the state move are one transaction: when the state move fails (a test trigger refuses it),
-- the slot rows are gone too. Run in a rolled-back transaction so the trigger does not stay.
COMMIT;
BEGIN;
CREATE FUNCTION test_refuse_state_update() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'state move refused by test' USING ERRCODE = 'P0001';
END;
$$;
CREATE TRIGGER test_refuse_state_update BEFORE UPDATE ON horsies_schedule_state
    FOR EACH ROW EXECUTE FUNCTION test_refuse_state_update();
SELECT test_raises(
    'fire: a failed state move fails the call',
    format(
        $q$SELECT * FROM test_fire('skip', %L, %L, ARRAY[%L]::timestamptz[], sha256('cmd-skip'::bytea))$q$,
        date_trunc('minute', now()) - interval '1 minute', date_trunc('minute', now()),
        date_trunc('minute', now()) - interval '1 minute'
    ),
    'P0001'
);
SELECT test_check(
    'fire: a failed state move leaves no slot task row',
    NOT EXISTS (SELECT 1 FROM horsies_tasks AS t
                WHERE t.id = horsies_schedule_slot_task_id('skip', date_trunc('minute', now()) - interval '1 minute')),
    'a slot row survived the failed state move'
);
ROLLBACK;

-- CONFLICT_UNVERIFIABLE needs a move to commit between the insert and the conflict read of the helper, which no
-- session can time from outside one statement. The case replaces the helper for one rolled-back transaction with a
-- stand-in that answers CONFLICT_UNVERIFIABLE, to check what the fire does with that outcome.
COMMIT;
BEGIN;
CREATE OR REPLACE FUNCTION horsies_insert_task_rows(
    p_ids uuid[], p_task_names text[], p_queue_names text[], p_priorities integer[], p_inputs text[],
    p_task_options text[], p_executions text[], p_retention_class_keys text[], p_retain_rerun_inputs boolean[],
    p_max_retries integer[], p_good_untils timestamp with time zone[], p_sent_ats timestamp with time zone[],
    p_enqueued_ats timestamp with time zone[], p_is_workflow_tasks boolean[], p_fingerprint_versions smallint[],
    p_fingerprints bytea[], p_key_digests bytea[], p_rerun_of_task_ids uuid[], p_rerun_root_task_ids uuid[],
    p_trace_carriers text[], p_trace_ids bytea[], p_row_time timestamp with time zone, p_conflict_lock text
) RETURNS TABLE(ordinality bigint, task_id uuid, outcome text)
    LANGUAGE sql AS $$
    SELECT u.position, u.id, 'CONFLICT_UNVERIFIABLE'::text FROM unnest(p_ids) WITH ORDINALITY AS u(id, position)
$$;
TRUNCATE test_last_fire;
INSERT INTO test_last_fire SELECT * FROM test_fire(
    'skip', date_trunc('minute', now()) - interval '1 minute', date_trunc('minute', now()),
    ARRAY[date_trunc('minute', now()) - interval '1 minute'], sha256('cmd-skip'::bytea)
);
SELECT test_check(
    'fire: a CONFLICT_UNVERIFIABLE slot moves the row past the slot and records no run',
    (array_agg(f.slot_outcome) FILTER (WHERE f.row_kind = 'SLOT')) = ARRAY['CONFLICT_UNVERIFIABLE']
    AND (SELECT s.next_run_at = date_trunc('minute', now()) AND s.run_count = 0 AND s.last_task_id IS NULL
         FROM horsies_schedule_state AS s WHERE s.schedule_name = 'skip'),
    array_agg(f.slot_outcome)::text
)
FROM test_last_fire AS f;
ROLLBACK;

SELECT test_raises(
    'fire refuses a next run that is not after the expected next run',
    $q$SELECT * FROM test_fire('skip', now() - interval '1 minute', now() - interval '1 minute',
        ARRAY[]::timestamptz[], sha256('x'::bytea))$q$,
    '22023'
);
SELECT test_raises(
    'fire refuses a slot after now',
    $q$SELECT * FROM test_fire('skip', now() - interval '1 minute', now() + interval '1 hour',
        ARRAY[now() + interval '1 minute'], sha256('x'::bytea))$q$,
    '22023'
);
SELECT test_raises(
    'fire refuses slot times that do not increase',
    $q$SELECT * FROM test_fire('skip', now() - interval '3 minutes', now(),
        ARRAY[now() - interval '1 minute', now() - interval '2 minutes'], sha256('x'::bytea))$q$,
    '22023'
);
SELECT test_raises(
    'fire refuses a slot before the expected next run',
    $q$SELECT * FROM test_fire('skip', now() - interval '1 minute', now() + interval '1 minute',
        ARRAY[now() - interval '2 minutes'], sha256('x'::bytea))$q$,
    '22023'
);
SELECT test_raises(
    'fire refuses more than 10,000 slots',
    $q$SELECT * FROM test_fire('skip', now() - interval '20000 seconds', now() + interval '1 minute',
        ARRAY(SELECT now() - interval '20000 seconds' + make_interval(secs => g) FROM generate_series(0, 10000) AS g),
        sha256('x'::bytea))$q$,
    '22023'
);
SELECT test_raises(
    'fire refuses a NULL fingerprint',
    $q$SELECT * FROM test_fire('skip', now() - interval '1 minute', now() + interval '1 minute',
        ARRAY[]::timestamptz[], NULL)$q$,
    '22004'
);

-- Two schedulers fire one slot. Session A fires in an open transaction and holds the state row; session B (dblink)
-- fires the same slot, waits on the state row, and after A commits finds the row moved.
SELECT test_schedule_row('race', date_trunc('minute', now()) - interval '1 minute');
SELECT dblink_connect('peer', current_setting('horsies_test.peer_connstr'));
CREATE TABLE test_peer (pid integer NOT NULL);
INSERT INTO test_peer SELECT p.pid FROM dblink('peer', 'SELECT pg_backend_pid()') AS p(pid integer);

COMMIT;
BEGIN;
TRUNCATE test_last_fire;
INSERT INTO test_last_fire SELECT * FROM test_fire(
    'race', date_trunc('minute', now()) - interval '1 minute', date_trunc('minute', now()) + interval '1 minute',
    ARRAY[date_trunc('minute', now()) - interval '1 minute'], sha256('cmd-race'::bytea)
);
SELECT test_check(
    'fire race: the first fire inserts the slot',
    (array_agg(f.slot_outcome) FILTER (WHERE f.row_kind = 'SLOT')) = ARRAY['INSERTED'],
    array_agg(f.row_kind)::text
)
FROM test_last_fire AS f;
SELECT dblink_send_query(
    'peer',
    format(
        $q$SELECT f.row_kind FROM test_fire('race', %L, %L, ARRAY[%L]::timestamptz[], sha256('cmd-race'::bytea)) AS f$q$,
        date_trunc('minute', now()) - interval '1 minute', date_trunc('minute', now()) + interval '1 minute',
        date_trunc('minute', now()) - interval '1 minute'
    )
);
DO $$
DECLARE
    v_peer integer := (SELECT pid FROM test_peer);
BEGIN
    FOR attempt IN 1..1000 LOOP
        EXIT WHEN pg_backend_pid() = ANY (pg_blocking_pids(v_peer));
        PERFORM pg_sleep(0.01);
    END LOOP;
    PERFORM test_check(
        'fire race: the second fire waits on the state row lock that the first holds',
        pg_backend_pid() = ANY (pg_blocking_pids(v_peer)),
        'the peer session is not blocked by this session'
    );
END;
$$;
COMMIT;
SELECT test_check(
    'fire race: the second fire answers STATE_MOVED',
    array_agg(r.row_kind) = ARRAY['STATE_MOVED'],
    array_agg(r.row_kind)::text
)
FROM dblink_get_result('peer') AS r(row_kind text);
SELECT test_check(
    'fire race: one task row and one recorded run',
    (SELECT count(*) FROM horsies_tasks AS t
     WHERE t.id = horsies_schedule_slot_task_id('race', date_trunc('minute', now()) - interval '1 minute')) = 1
    AND (SELECT s.run_count FROM horsies_schedule_state AS s WHERE s.schedule_name = 'race') = 1,
    'more than one task row or run'
);
SELECT 1 FROM dblink_get_result('peer') AS r(row_kind text);

------------------------------------------------------------------------------------------------------------------------
-- horsies_list_stale_tasks
------------------------------------------------------------------------------------------------------------------------

DELETE FROM horsies_tasks;

SELECT test_live_task('00000000-0000-4000-8000-0000000000a1', 'RUNNING', 'app.old_beat', 'default', 'w1',
                      now() - interval '2 hours', now() - interval '2 hours', 0, NULL);
SELECT test_live_task('00000000-0000-4000-8000-0000000000a2', 'RUNNING', 'app.no_beat', 'default', 'w1',
                      now() - interval '2 hours', now() - interval '30 minutes', 0, NULL);
SELECT test_live_task('00000000-0000-4000-8000-0000000000a3', 'RUNNING', 'app.fresh_beat', 'default', 'w1',
                      now() - interval '2 hours', now() - interval '2 hours', 0, NULL);
SELECT test_live_task('00000000-0000-4000-8000-0000000000a4', 'RUNNING', 'app.just_started', 'default', 'w1',
                      now() - interval '2 hours', now() - interval '1 minute', 0, NULL);
SELECT test_live_task('00000000-0000-4000-8000-0000000000a5', 'CLAIMED', 'app.claimed', 'default', 'w1',
                      now() - interval '2 hours', NULL, 0, NULL);
SELECT test_live_task('00000000-0000-4000-8000-0000000000a6', 'RUNNING', 'app.older_beat', 'default', 'w1',
                      now() - interval '3 hours', now() - interval '3 hours', 0, NULL);
INSERT INTO horsies_heartbeats (task_id, role, sent_at, sender_id, hostname, pid) VALUES
    ('00000000-0000-4000-8000-0000000000a1', 'runner', now() - interval '20 minutes', 'w1', 'h', 1),
    ('00000000-0000-4000-8000-0000000000a1', 'claimer', now() - interval '1 minute', 'w1', 'h', 1),
    ('00000000-0000-4000-8000-0000000000a3', 'runner', now() - interval '1 minute', 'w1', 'h', 1),
    ('00000000-0000-4000-8000-0000000000a6', 'runner', now() - interval '50 minutes', 'w1', 'h', 1);

SELECT test_check(
    'stale list: stale by beat age and with no beat, no beat first, then the oldest beat; fresh rows excluded',
    array_agg(s.task_id ORDER BY s.row_position) FILTER (WHERE s.row_kind = 'TASK') = ARRAY[
        '00000000-0000-4000-8000-0000000000a2', '00000000-0000-4000-8000-0000000000a6',
        '00000000-0000-4000-8000-0000000000a1'
    ]::uuid[]
    AND bool_and(s.last_heartbeat IS NULL) FILTER (WHERE s.task_id = '00000000-0000-4000-8000-0000000000a2')
    AND bool_and(s.last_heartbeat = (SELECT h.sent_at FROM horsies_heartbeats AS h
                                     WHERE h.task_id = s.task_id AND h.role = 'runner'))
        FILTER (WHERE s.task_id = '00000000-0000-4000-8000-0000000000a1')
    AND bool_and(s.worker_hostname = 'host-live' AND s.worker_pid = 4242 AND s.task_name IS NOT NULL)
        FILTER (WHERE s.row_kind = 'TASK')
    AND array_agg(s.list_state || '/' || s.row_bound) FILTER (WHERE s.row_kind = 'LIST_END') = ARRAY['COMPLETE/10000'],
    array_agg(s.row_kind || ':' || COALESCE(s.task_id::text, s.list_state))::text
)
FROM horsies_list_stale_tasks(10) AS s;

SELECT test_check(
    'stale list: threshold 0 lists every RUNNING row whose latest beat or start is in the past',
    count(*) FILTER (WHERE s.row_kind = 'TASK') = 5,
    count(*)::text
)
FROM horsies_list_stale_tasks(0) AS s;

SELECT test_raises('stale list refuses a negative threshold', 'SELECT * FROM horsies_list_stale_tasks(-1)', '22023');
SELECT test_raises('stale list refuses a threshold over one year',
                   'SELECT * FROM horsies_list_stale_tasks(525601)', '22023');
SELECT test_raises('stale list refuses a NULL threshold', 'SELECT * FROM horsies_list_stale_tasks(NULL)', '22023');

-- a7: a retried task whose new attempt started 30 minutes ago; the only runner beat is of its earlier attempt, 40
-- minutes ago. The row is removed after the check, so the bound cases below count the rows they expect.
SELECT test_live_task('00000000-0000-4000-8000-0000000000a7', 'RUNNING', 'app.restarted', 'default', 'w1',
                      now() - interval '2 hours', now() - interval '30 minutes', 1, NULL);
INSERT INTO horsies_heartbeats (task_id, role, sent_at, sender_id, hostname, pid) VALUES
    ('00000000-0000-4000-8000-0000000000a7', 'runner', now() - interval '40 minutes', 'w1', 'h', 1);
SELECT test_check(
    'stale list: a runner beat of an earlier attempt, older than the start, is not the last heartbeat',
    count(*) FILTER (WHERE s.task_id = '00000000-0000-4000-8000-0000000000a7') = 1
    AND bool_and(s.last_heartbeat IS NULL) FILTER (WHERE s.task_id = '00000000-0000-4000-8000-0000000000a7'),
    array_agg(s.row_kind || ':' || COALESCE(s.task_id::text, s.list_state) || ':'
              || COALESCE(s.last_heartbeat::text, 'none'))::text
)
FROM horsies_list_stale_tasks(10) AS s;
DELETE FROM horsies_heartbeats WHERE task_id = '00000000-0000-4000-8000-0000000000a7';
DELETE FROM horsies_tasks WHERE id = '00000000-0000-4000-8000-0000000000a7';

COMMIT;
BEGIN;
INSERT INTO horsies_tasks (
    id, task_name, queue_name, priority, input, task_options, execution, retention_class_key, sent_at, created_at,
    command_fingerprint_version, command_fingerprint, idempotency_key_digest, status, enqueued_at, claimed_at,
    claimed_by_worker_id, claim_expires_at, started_at, finalizing_at, finalizing_by_worker_id, worker_pid,
    worker_hostname, worker_process_name, retry_count, max_retries, next_retry_at, good_until, is_workflow_task,
    updated_at, retain_rerun_input, rerun_of_task_id, rerun_root_task_id
)
SELECT gen_random_uuid(), 'app.bulk', 'default', 50, '{}', NULL, 'ISOLATED', 'forever', now(), now(), 1,
       sha256('bulk'::bytea), NULL, 'RUNNING', now(), now(), 'w', NULL, now() - interval '1 day', NULL, NULL, 1,
       'h', 'p', 0, 0, NULL, NULL, FALSE, now(), FALSE, NULL, NULL
FROM generate_series(1, 9997);
SELECT test_check(
    'stale list: exactly 10,000 stale rows is COMPLETE',
    count(*) FILTER (WHERE s.row_kind = 'TASK') = 10000
    AND array_agg(s.list_state) FILTER (WHERE s.row_kind = 'LIST_END') = ARRAY['COMPLETE'],
    count(*)::text
)
FROM horsies_list_stale_tasks(10) AS s;
SELECT test_live_task('00000000-0000-4000-8000-0000000000af', 'RUNNING', 'app.one_more', 'default', 'w1',
                      now() - interval '2 hours', now() - interval '2 hours', 0, NULL);
SELECT test_check(
    'stale list: 10,001 stale rows give 10,000 rows and TRUNCATED',
    count(*) FILTER (WHERE s.row_kind = 'TASK') = 10000
    AND max(s.row_position) = 10000
    AND array_agg(s.list_state || '/' || s.row_bound) FILTER (WHERE s.row_kind = 'LIST_END') = ARRAY['TRUNCATED/10000'],
    count(*)::text
)
FROM horsies_list_stale_tasks(10) AS s;
ROLLBACK;

------------------------------------------------------------------------------------------------------------------------
-- horsies_list_expired_tasks
------------------------------------------------------------------------------------------------------------------------

DELETE FROM horsies_heartbeats;
DELETE FROM horsies_tasks;

SELECT test_live_task('00000000-0000-4000-8000-0000000000b1', 'PENDING', 'app.expired_early', 'q1', 'w1',
                      now() - interval '1 hour', NULL, 0, now() - interval '30 minutes');
SELECT test_live_task('00000000-0000-4000-8000-0000000000b2', 'PENDING', 'app.expired_late', 'q1', 'w1',
                      now() - interval '1 hour', NULL, 0, now() - interval '5 minutes');
SELECT test_live_task('00000000-0000-4000-8000-0000000000b3', 'PENDING', 'app.not_yet', 'q1', 'w1',
                      now() - interval '1 hour', NULL, 0, now() + interval '5 minutes');
SELECT test_live_task('00000000-0000-4000-8000-0000000000b4', 'PENDING', 'app.no_deadline', 'q1', 'w1',
                      now() - interval '1 hour', NULL, 0, NULL);
SELECT test_live_task('00000000-0000-4000-8000-0000000000b5', 'CLAIMED', 'app.claimed_expired', 'q1', 'w1',
                      now() - interval '1 hour', NULL, 0, now() - interval '40 minutes');
SELECT test_live_task('00000000-0000-4000-8000-0000000000b6', 'PENDING', 'app.expired_now', 'q1', 'w1',
                      now() - interval '1 hour', NULL, 0, now());

SELECT test_check(
    'expired list: PENDING rows at or past their deadline, oldest deadline first, with expired_for',
    array_agg(e.task_id ORDER BY e.row_position) FILTER (WHERE e.row_kind = 'TASK') = ARRAY[
        '00000000-0000-4000-8000-0000000000b1', '00000000-0000-4000-8000-0000000000b2',
        '00000000-0000-4000-8000-0000000000b6'
    ]::uuid[]
    AND bool_and(e.expired_for = now() - e.good_until AND e.expired_for > interval '29 minutes')
        FILTER (WHERE e.task_id = '00000000-0000-4000-8000-0000000000b1')
    AND bool_and(e.queue_name = 'q1' AND e.priority = 50 AND e.sent_at IS NOT NULL AND e.enqueued_at IS NOT NULL)
        FILTER (WHERE e.row_kind = 'TASK')
    AND array_agg(e.list_state || '/' || e.row_bound) FILTER (WHERE e.row_kind = 'LIST_END') = ARRAY['COMPLETE/10000'],
    array_agg(e.row_kind || ':' || COALESCE(e.task_id::text, e.list_state))::text
)
FROM horsies_list_expired_tasks() AS e;

COMMIT;
BEGIN;
INSERT INTO horsies_tasks (
    id, task_name, queue_name, priority, input, task_options, execution, retention_class_key, sent_at, created_at,
    command_fingerprint_version, command_fingerprint, idempotency_key_digest, status, enqueued_at, claimed_at,
    claimed_by_worker_id, claim_expires_at, started_at, finalizing_at, finalizing_by_worker_id, worker_pid,
    worker_hostname, worker_process_name, retry_count, max_retries, next_retry_at, good_until, is_workflow_task,
    updated_at, retain_rerun_input, rerun_of_task_id, rerun_root_task_id
)
SELECT gen_random_uuid(), 'app.bulk', 'default', 50, '{}', NULL, 'ISOLATED', 'forever', now(), now(), 1,
       sha256('bulk'::bytea), NULL, 'PENDING', now(), NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 0, 0,
       NULL, now() - interval '1 hour', FALSE, now(), FALSE, NULL, NULL
FROM generate_series(1, 9998);
SELECT test_check(
    'expired list: 10,001 expired rows give 10,000 rows and TRUNCATED',
    count(*) FILTER (WHERE e.row_kind = 'TASK') = 10000
    AND array_agg(e.list_state) FILTER (WHERE e.row_kind = 'LIST_END') = ARRAY['TRUNCATED'],
    count(*)::text
)
FROM horsies_list_expired_tasks() AS e;
ROLLBACK;

------------------------------------------------------------------------------------------------------------------------
-- horsies_monitoring_task_detail
------------------------------------------------------------------------------------------------------------------------

DELETE FROM horsies_tasks;

-- A live task with two attempts that backs a workflow node.
SELECT test_live_task('00000000-0000-4000-8000-0000000000c1', 'RUNNING', 'app.live', 'default', 'w-live',
                      now() - interval '10 minutes', now() - interval '5 minutes', 1, now() + interval '1 hour');
SELECT test_attempt('00000000-0000-4000-8000-0000000000c1', 2, 'FAILED', TRUE);
SELECT test_attempt('00000000-0000-4000-8000-0000000000c1', 1, 'WORKER_FAILURE', TRUE);
INSERT INTO horsies_workflows (
    id, name, definition_key, status, on_error, output_task_index, success_policy, result, error,
    parent_workflow_id, parent_task_index, root_workflow_id, depth, sent_at, created_at, started_at, completed_at,
    updated_at
) VALUES (
    '00000000-0000-4000-8000-0000000000f1', 'wf', NULL, 'RUNNING', 'fail', NULL, NULL, NULL, NULL,
    NULL, NULL, '00000000-0000-4000-8000-0000000000f1', 0, now(), now(), now(), NULL, now()
);
INSERT INTO horsies_workflow_tasks (
    id, workflow_id, task_index, node_id, task_name, input, queue_name, priority, dependencies, args_from,
    workflow_ctx_from, allow_failed_deps, join_type, min_success, task_options, max_retries, execution,
    good_until, retention_class_key, downstream_subworkflow, status, task_id, is_subworkflow, sub_workflow_id,
    sub_workflow_name, sub_definition_key, sub_workflow_params, sub_workflow_summary, result, error, created_at,
    started_at, completed_at
) VALUES (
    gen_random_uuid(), '00000000-0000-4000-8000-0000000000f1', 3, 'node_3', 'app.live', '{}', 'default', 50, '{}',
    NULL, NULL, FALSE, 'all', NULL, NULL, 3, 'SHARED_ASYNC', NULL, 'forever', 'NONE', 'RUNNING',
    '00000000-0000-4000-8000-0000000000c1', FALSE, NULL, NULL, NULL, NULL, NULL, NULL, NULL, now(), now(), NULL
);
UPDATE horsies_tasks SET trace_carrier = '{"traceparent":"c1"}', trace_id = decode(repeat('c1', 16), 'hex')
WHERE id = '00000000-0000-4000-8000-0000000000c1';

-- The archive is version 2, [attempts, open]: attempt 2 (retry_count + 1) has its row, so no attempt is open; the
-- attempt rows were written with no phase record, so each phase value is null.
SELECT test_check(
    'detail: LIVE carries the live columns, the node link, the trace id and the attempts in ascending order',
    d.location = 'LIVE' AND d.task_name = 'app.live' AND d.status = 'RUNNING' AND d.execution = 'SHARED_ASYNC'
    AND d.retry_count = 1 AND d.max_retries = 3 AND d.worker_id = 'w-live' AND d.worker_hostname = 'host-live'
    AND d.worker_pid = 4242 AND d.good_until IS NOT NULL
    AND d.workflow_id = '00000000-0000-4000-8000-0000000000f1' AND d.workflow_task_index = 3
    AND d.terminal_at IS NULL AND d.error_code IS NULL AND d.result_payload IS NULL AND d.absence_reason IS NULL
    AND d.attempt_archive_version = 2 AND d.attempt_snapshot_digest IS NULL
    AND (convert_from(d.attempt_snapshot, 'UTF8')::jsonb -> 0 -> 0 ->> 0) = '1'
    AND (convert_from(d.attempt_snapshot, 'UTF8')::jsonb -> 0 -> 1 ->> 0) = '2'
    AND (convert_from(d.attempt_snapshot, 'UTF8')::jsonb -> 0 -> 1 ->> 1) = 'FAILED'
    AND jsonb_array_length(convert_from(d.attempt_snapshot, 'UTF8')::jsonb -> 0 -> 1) = 21
    AND (convert_from(d.attempt_snapshot, 'UTF8')::jsonb -> 0 -> 1 -> 12) = 'null'::jsonb
    AND (convert_from(d.attempt_snapshot, 'UTF8')::jsonb -> 1) = 'null'::jsonb
    AND d.attempt_snapshot = horsies_encode_attempt_archive(
        (SELECT t FROM horsies_tasks AS t WHERE t.id = '00000000-0000-4000-8000-0000000000c1'))
    AND d.trace_id = decode(repeat('c1', 16), 'hex'),
    row_to_json(d)::text
)
FROM horsies_monitoring_task_detail('00000000-0000-4000-8000-0000000000c1', FALSE, TRUE) AS d;

SELECT test_check(
    'detail: LIVE without the attempt part reads no attempts',
    d.location = 'LIVE' AND d.attempt_snapshot IS NULL AND d.attempt_archive_version IS NULL,
    row_to_json(d)::text
)
FROM horsies_monitoring_task_detail('00000000-0000-4000-8000-0000000000c1', TRUE, FALSE) AS d;

SELECT test_history_task('00000000-0000-4000-8000-0000000000c2', 'FAILED', 'app.done', 'q-h', 'w-hist',
                         now() - interval '3 hours', now() - interval '2 hours', now() - interval '1 hour', 2, 'E_X');
UPDATE horsies_task_history SET trace_id = decode(repeat('c2', 16), 'hex')
WHERE task_id = '00000000-0000-4000-8000-0000000000c2';

SELECT test_check(
    'detail: HISTORY carries the history row, the result, the trace id and the attempt snapshot with its digest',
    d.location = 'HISTORY' AND d.task_name = 'app.done' AND d.queue_name = 'q-h' AND d.status = 'FAILED'
    AND d.terminalization_kind = 'FAIL_RUNNING'
    AND d.terminal_at = (SELECT h.terminal_at FROM horsies_task_history AS h WHERE h.task_id = d.task_id)
    AND d.error_code = 'E_X' AND d.final_failed_reason = 'reason of 00000000-0000-4000-8000-0000000000c2'
    AND d.worker_id = 'w-hist' AND d.worker_hostname = 'host-history' AND d.worker_pid = 5151
    AND d.retry_count = 2 AND d.execution IS NULL AND d.next_retry_at IS NULL
    AND d.result_envelope_version = 1 AND convert_from(d.result_payload, 'UTF8') = '{"ok":true}'
    AND d.result_digest = sha256(d.result_payload)
    AND d.attempt_archive_version = 1 AND convert_from(d.attempt_snapshot, 'UTF8') = '[]'
    AND d.attempt_snapshot_digest = sha256(d.attempt_snapshot)
    AND d.workflow_id IS NULL AND d.absence_reason IS NULL AND d.trace_id = decode(repeat('c2', 16), 'hex'),
    row_to_json(d)::text
)
FROM horsies_monitoring_task_detail('00000000-0000-4000-8000-0000000000c2', TRUE, TRUE) AS d;

SELECT test_check(
    'detail: HISTORY without the result and attempt parts reads neither',
    d.location = 'HISTORY' AND d.result_payload IS NULL AND d.result_digest IS NULL
    AND d.result_envelope_version IS NULL AND d.attempt_snapshot IS NULL AND d.attempt_snapshot_digest IS NULL
    AND d.final_failed_reason IS NOT NULL,
    row_to_json(d)::text
)
FROM horsies_monitoring_task_detail('00000000-0000-4000-8000-0000000000c2', FALSE, FALSE) AS d;

SELECT test_check(
    'detail: an id that is not a UUIDv7 is ABSENT with NOT_CLASSIFIED',
    d.location = 'ABSENT' AND d.task_id = '00000000-0000-4000-8000-0000000000c9'
    AND d.absence_reason = 'NOT_CLASSIFIED' AND d.task_name IS NULL AND d.trace_id IS NULL,
    row_to_json(d)::text
)
FROM horsies_monitoring_task_detail('00000000-0000-4000-8000-0000000000c9', TRUE, TRUE) AS d;

SELECT test_check(
    'detail: a UUIDv7 with no detached leaf is ABSENT with WITHIN_RETAINED_HISTORY',
    d.location = 'ABSENT' AND d.absence_reason = 'WITHIN_RETAINED_HISTORY',
    row_to_json(d)::text
)
FROM horsies_monitoring_task_detail(test_uuid7_at(now() - interval '400 days'), FALSE, TRUE) AS d;

-- Detached leaves: a PERIOD leaf up to 10 days ago, and a HEARTBEAT leaf up to 1 hour ago (not history).
INSERT INTO horsies_task_history_leaf_catalog (
    leaf_name, class_key, lower_bound_kind, lower_anchor, upper_anchor, index_schema_version, created_at,
    detached_at, dropped_at
) VALUES
    ('test_leaf_period_old', 'standard_7d', 'AT', date_trunc('day', now()) - interval '11 days',
     date_trunc('day', now()) - interval '10 days', 1, now(), now(), NULL),
    ('test_leaf_period_older', 'standard_30d', 'AT', date_trunc('day', now()) - interval '41 days',
     date_trunc('day', now()) - interval '40 days', 1, now(), now(), now()),
    ('test_leaf_heartbeat', 'heartbeats', 'AT', date_trunc('hour', now()) - interval '2 hours',
     date_trunc('hour', now()) - interval '1 hour', 1, now(), now(), NULL),
    ('test_leaf_period_attached', 'standard_7d', 'AT', date_trunc('day', now()) - interval '2 days',
     date_trunc('day', now()) - interval '1 day', 1, now(), NULL, NULL);

SELECT test_check(
    'detail: a UUIDv7 born before the latest detached PERIOD upper anchor is PREDATES_RETAINED_HISTORY',
    d.absence_reason = 'PREDATES_RETAINED_HISTORY',
    row_to_json(d)::text
)
FROM horsies_monitoring_task_detail(test_uuid7_at(date_trunc('day', now()) - interval '12 days'), FALSE, TRUE) AS d;

SELECT test_check(
    'detail: the 5 s clock allowance counts a birth just after the anchor as PREDATES_RETAINED_HISTORY',
    d.absence_reason = 'PREDATES_RETAINED_HISTORY',
    row_to_json(d)::text
)
FROM horsies_monitoring_task_detail(
    test_uuid7_at(date_trunc('day', now()) - interval '10 days' + interval '4 seconds'), FALSE, TRUE
) AS d;

SELECT test_check(
    'detail: a UUIDv7 born after the anchor plus 5 s is WITHIN_RETAINED_HISTORY (heartbeat and attached leaves ignored)',
    d.absence_reason = 'WITHIN_RETAINED_HISTORY',
    row_to_json(d)::text
)
FROM horsies_monitoring_task_detail(
    test_uuid7_at(date_trunc('day', now()) - interval '10 days' + interval '6 seconds'), FALSE, TRUE
) AS d;

SELECT test_raises(
    'detail refuses a NULL part flag',
    $q$SELECT * FROM horsies_monitoring_task_detail('00000000-0000-4000-8000-0000000000c1', NULL, TRUE)$q$,
    '22004'
);

-- A published reader that reports the task live after the live miss of the same snapshot breaks the history
-- contract. A stand-in reader for one rolled-back transaction gives that answer.
COMMIT;
BEGIN;
CREATE OR REPLACE FUNCTION horsies_task_detail_staged(p_task_id uuid)
RETURNS TABLE (location text, task_row horsies_task_history)
    LANGUAGE sql STABLE AS $$
    SELECT 'LIVE'::text, NULL::horsies_task_history
$$;
SELECT test_raises(
    'detail: a LIVE answer of the published reader after a live miss is data_corrupted',
    $q$SELECT * FROM horsies_monitoring_task_detail('00000000-0000-4000-8000-0000000000c8', FALSE, TRUE)$q$,
    'XX001'
);
ROLLBACK;

-- One snapshot: session B moves the live task to history (attempts deleted, live row deleted, history row
-- inserted) and commits after the detail statement started but before the function body runs. The detail still
-- answers LIVE with both attempts, from the statement's snapshot.
COMMIT;
SELECT dblink_send_query(
    'peer',
    $q$
    DO $do$
    BEGIN
        PERFORM pg_sleep(0.5);
        DELETE FROM horsies_task_attempts WHERE task_id = '00000000-0000-4000-8000-0000000000c1';
        DELETE FROM horsies_workflow_tasks WHERE task_id = '00000000-0000-4000-8000-0000000000c1';
        DELETE FROM horsies_tasks WHERE id = '00000000-0000-4000-8000-0000000000c1';
        PERFORM test_history_task('00000000-0000-4000-8000-0000000000c1', 'COMPLETED', 'app.live', 'default',
            'w-live', now() - interval '10 minutes', now() - interval '5 minutes', now(), 1, NULL);
    END;
    $do$
    $q$
);
SELECT test_check(
    'detail: a move that commits during the statement leaves the answer LIVE with every attempt',
    d.location = 'LIVE'
    AND jsonb_array_length(convert_from(d.attempt_snapshot, 'UTF8')::jsonb) = 2
    AND d.workflow_task_index = 3,
    row_to_json(d)::text
)
FROM (SELECT pg_sleep(1.5)) AS wait
CROSS JOIN LATERAL horsies_monitoring_task_detail('00000000-0000-4000-8000-0000000000c1', FALSE, TRUE) AS d;
SELECT test_check(
    'detail: the peer move committed',
    array_agg(r.status) = ARRAY['DO'],
    array_agg(r.status)::text
)
FROM dblink_get_result('peer') AS r(status text);
SELECT 1 FROM dblink_get_result('peer') AS r(status text);
SELECT test_check(
    'detail: the next statement sees the moved task in HISTORY',
    d.location = 'HISTORY' AND d.status = 'COMPLETED',
    row_to_json(d)::text
)
FROM horsies_monitoring_task_detail('00000000-0000-4000-8000-0000000000c1', FALSE, TRUE) AS d;

------------------------------------------------------------------------------------------------------------------------
-- horsies_monitoring_task_list
------------------------------------------------------------------------------------------------------------------------

DELETE FROM horsies_workflow_tasks;
DELETE FROM horsies_task_attempts;
DELETE FROM horsies_tasks;
DELETE FROM horsies_task_history;

CREATE FUNCTION test_list(
    p_statuses text[],
    p_error_codes text[],
    p_error_categories text[],
    p_retried_only boolean,
    p_sort_key text,
    p_direction text,
    p_offset integer,
    p_limit integer
) RETURNS SETOF record
    LANGUAGE sql AS $$
    SELECT l.row_kind, l.row_position, l.total_kind, l.live_total, l.history_total, l.history_estimate_plan,
           l.location, l.task_id, l.status, l.error_code
    FROM horsies_monitoring_task_list(
        now() - interval '1 day', now(), p_statuses, '{}', '{}', '{}', p_error_codes, p_error_categories,
        p_retried_only, ARRAY['WORKER_CRASHED', 'TASK_TIMEOUT', 'RESULT_NOT_READY'],
        ARRAY['OPERATIONAL', 'OPERATIONAL', 'RETRIEVAL'], p_sort_key, p_direction, p_offset, p_limit
    ) AS l
$$;

-- Live: d1 PENDING (enqueued 50 min ago), d2 RUNNING (40), d3 PENDING retried (30).
-- History in the window: e1 COMPLETED (enqueued 45), e2 FAILED WORKER_CRASHED (35), e3 FAILED custom code (25),
-- e4 FAILED empty code (20). Outside the window: e5 (terminal 2 days ago).
SELECT test_live_task('00000000-0000-4000-8000-0000000000d1', 'PENDING', 'app.a', 'q1', 'w1',
                      now() - interval '50 minutes', NULL, 0, NULL);
SELECT test_live_task('00000000-0000-4000-8000-0000000000d2', 'RUNNING', 'app.b', 'q1', 'w2',
                      now() - interval '40 minutes', now() - interval '39 minutes', 0, NULL);
SELECT test_live_task('00000000-0000-4000-8000-0000000000d3', 'PENDING', 'app.a', 'q2', 'w1',
                      now() - interval '30 minutes', NULL, 2, NULL);
SELECT test_history_task('00000000-0000-4000-8000-0000000000e1', 'COMPLETED', 'app.a', 'q1', 'w1',
                         now() - interval '45 minutes', now() - interval '44 minutes', now() - interval '43 minutes', 0, NULL);
SELECT test_history_task('00000000-0000-4000-8000-0000000000e2', 'FAILED', 'app.b', 'q1', 'w2',
                         now() - interval '35 minutes', now() - interval '34 minutes', now() - interval '20 minutes', 1, 'WORKER_CRASHED');
SELECT test_history_task('00000000-0000-4000-8000-0000000000e3', 'FAILED', 'app.c', 'q2', 'w2',
                         now() - interval '25 minutes', now() - interval '24 minutes', now() - interval '23 minutes', 0, 'MY_DOMAIN_ERROR');
SELECT test_history_task('00000000-0000-4000-8000-0000000000e4', 'FAILED', 'app.c', 'q2', 'w2',
                         now() - interval '20 minutes', now() - interval '19 minutes', now() - interval '18 minutes', 0, '');
SELECT test_history_task('00000000-0000-4000-8000-0000000000e5', 'COMPLETED', 'app.a', 'q1', 'w1',
                         now() - interval '3 days', now() - interval '3 days', now() - interval '2 days', 0, NULL);

SELECT test_check(
    'list: the default sort merges both sides by enqueued_at DESC, history rows outside the window excluded',
    array_agg(right(l.task_id::text, 2) ORDER BY l.row_position) FILTER (WHERE l.row_kind = 'TASK')
        = ARRAY['e4', 'e3', 'd3', 'e2', 'd2', 'e1', 'd1']
    AND array_agg(l.location ORDER BY l.row_position) FILTER (WHERE l.row_kind = 'TASK')
        = ARRAY['HISTORY', 'HISTORY', 'LIVE', 'HISTORY', 'LIVE', 'HISTORY', 'LIVE'],
    array_agg(right(COALESCE(l.task_id::text, 'TOTALS'), 6) ORDER BY l.row_position)::text
)
FROM test_list('{}', '{}', '{}', FALSE, 'enqueued_at', 'DESC', 0, 50)
    AS l(row_kind text, row_position integer, total_kind text, live_total bigint, history_total bigint,
         history_estimate_plan text, location text, task_id uuid, status text, error_code text);

SELECT test_check(
    'list: with no filter the totals are an estimate: reltuples of the live table and the EXPLAIN of the window',
    l.row_position = 0 AND l.total_kind = 'ESTIMATE_HISTORY_PLAN' AND l.history_total IS NULL
    AND jsonb_typeof(l.history_estimate_plan::jsonb -> 0 -> 'Plan' -> 'Plan Rows') = 'number'
    AND l.live_total = (SELECT CASE WHEN c.reltuples < 0 THEN 3 ELSE c.reltuples::bigint END
                        FROM pg_class AS c WHERE c.oid = 'horsies_tasks'::regclass),
    row_to_json(l)::text
)
FROM test_list('{}', '{}', '{}', FALSE, 'enqueued_at', 'DESC', 0, 50)
    AS l(row_kind text, row_position integer, total_kind text, live_total bigint, history_total bigint,
         history_estimate_plan text, location text, task_id uuid, status text, error_code text)
WHERE l.row_kind = 'TOTALS';

SELECT test_check(
    'list: page 2 continues page 1 with no row twice',
    (SELECT array_agg(right(l.task_id::text, 2) ORDER BY l.row_position) FILTER (WHERE l.row_kind = 'TASK')
     FROM test_list('{}', '{}', '{}', FALSE, 'enqueued_at', 'DESC', 0, 3)
         AS l(row_kind text, row_position integer, total_kind text, live_total bigint, history_total bigint,
              history_estimate_plan text, location text, task_id uuid, status text, error_code text))
    || (SELECT array_agg(right(l.task_id::text, 2) ORDER BY l.row_position) FILTER (WHERE l.row_kind = 'TASK')
        FROM test_list('{}', '{}', '{}', FALSE, 'enqueued_at', 'DESC', 3, 3)
            AS l(row_kind text, row_position integer, total_kind text, live_total bigint, history_total bigint,
                 history_estimate_plan text, location text, task_id uuid, status text, error_code text))
    = ARRAY['e4', 'e3', 'd3', 'e2', 'd2', 'e1'],
    'pages do not join'
);

SELECT test_check(
    'list: a status filter gives exact totals over both sides',
    array_agg(right(l.task_id::text, 2) ORDER BY l.row_position) FILTER (WHERE l.row_kind = 'TASK')
        = ARRAY['d3', 'e1', 'd1']
    AND bool_and(l.total_kind = 'EXACT' AND l.live_total = 2 AND l.history_total = 1)
        FILTER (WHERE l.row_kind = 'TOTALS'),
    array_agg(l.row_kind || ':' || COALESCE(right(l.task_id::text, 2), l.total_kind || l.live_total || '/' || l.history_total))::text
)
FROM test_list('{PENDING,COMPLETED}', '{}', '{}', FALSE, 'enqueued_at', 'DESC', 0, 50)
    AS l(row_kind text, row_position integer, total_kind text, live_total bigint, history_total bigint,
         history_estimate_plan text, location text, task_id uuid, status text, error_code text);

SELECT test_check(
    'list: an error-code filter drops the live side',
    array_agg(right(l.task_id::text, 2)) FILTER (WHERE l.row_kind = 'TASK') = ARRAY['e2']
    AND bool_and(l.live_total = 0 AND l.history_total = 1) FILTER (WHERE l.row_kind = 'TOTALS'),
    array_agg(l.row_kind || ':' || COALESCE(l.task_id::text, ''))::text
)
FROM test_list('{}', '{WORKER_CRASHED}', '{}', FALSE, 'enqueued_at', 'DESC', 0, 50)
    AS l(row_kind text, row_position integer, total_kind text, live_total bigint, history_total bigint,
         history_estimate_plan text, location text, task_id uuid, status text, error_code text);

SELECT test_check(
    'list: a built-in category selects its codes; DOMAIN selects non-empty codes outside the taxonomy',
    (SELECT array_agg(right(l.task_id::text, 2)) FILTER (WHERE l.row_kind = 'TASK')
     FROM test_list('{}', '{}', '{OPERATIONAL}', FALSE, 'enqueued_at', 'DESC', 0, 50)
         AS l(row_kind text, row_position integer, total_kind text, live_total bigint, history_total bigint,
              history_estimate_plan text, location text, task_id uuid, status text, error_code text))
        = ARRAY['e2']
    AND (SELECT array_agg(right(l.task_id::text, 2)) FILTER (WHERE l.row_kind = 'TASK')
         FROM test_list('{}', '{}', '{DOMAIN}', FALSE, 'enqueued_at', 'DESC', 0, 50)
             AS l(row_kind text, row_position integer, total_kind text, live_total bigint, history_total bigint,
                  history_estimate_plan text, location text, task_id uuid, status text, error_code text))
        = ARRAY['e3']
    AND (SELECT array_agg(right(l.task_id::text, 2) ORDER BY l.row_position) FILTER (WHERE l.row_kind = 'TASK')
         FROM test_list('{}', '{}', '{DOMAIN,OPERATIONAL}', FALSE, 'enqueued_at', 'DESC', 0, 50)
             AS l(row_kind text, row_position integer, total_kind text, live_total bigint, history_total bigint,
                  history_estimate_plan text, location text, task_id uuid, status text, error_code text))
        = ARRAY['e3', 'e2']
    AND (SELECT count(*) FILTER (WHERE l.row_kind = 'TASK')
         FROM test_list('{}', '{}', '{RETRIEVAL}', FALSE, 'enqueued_at', 'DESC', 0, 50)
             AS l(row_kind text, row_position integer, total_kind text, live_total bigint, history_total bigint,
                  history_estimate_plan text, location text, task_id uuid, status text, error_code text)) = 0,
    'category selection differs'
);

SELECT test_check(
    'list: retried_only keeps rows with retries on both sides',
    array_agg(right(l.task_id::text, 2) ORDER BY l.row_position) FILTER (WHERE l.row_kind = 'TASK') = ARRAY['d3', 'e2'],
    array_agg(l.task_id)::text
)
FROM test_list('{}', '{}', '{}', TRUE, 'enqueued_at', 'DESC', 0, 50)
    AS l(row_kind text, row_position integer, total_kind text, live_total bigint, history_total bigint,
         history_estimate_plan text, location text, task_id uuid, status text, error_code text);

-- completed_at: only e1 has a value; the NULL keys of d1..d3 (live) and e2..e4 (history, not COMPLETED) tie.
SELECT test_check(
    'list: completed_at sorts NULL keys last in both directions, ties LIVE before HISTORY then by id',
    (SELECT array_agg(right(l.task_id::text, 2) ORDER BY l.row_position) FILTER (WHERE l.row_kind = 'TASK')
     FROM test_list('{}', '{}', '{}', FALSE, 'completed_at', 'ASC', 0, 50)
         AS l(row_kind text, row_position integer, total_kind text, live_total bigint, history_total bigint,
              history_estimate_plan text, location text, task_id uuid, status text, error_code text))
        = ARRAY['e1', 'd1', 'd2', 'd3', 'e2', 'e3', 'e4']
    AND (SELECT array_agg(right(l.task_id::text, 2) ORDER BY l.row_position) FILTER (WHERE l.row_kind = 'TASK')
         FROM test_list('{}', '{}', '{}', FALSE, 'completed_at', 'DESC', 0, 50)
             AS l(row_kind text, row_position integer, total_kind text, live_total bigint, history_total bigint,
                  history_estimate_plan text, location text, task_id uuid, status text, error_code text))
        = ARRAY['e1', 'd1', 'd2', 'd3', 'e2', 'e3', 'e4'],
    'completed_at order differs'
);

-- task_name: app.a holds d1, d3 (live) and e1; app.b holds d2 (live) and e2; app.c holds e3 and e4.
SELECT test_check(
    'list: a text key ascending ties LIVE before HISTORY, then by id',
    array_agg(right(l.task_id::text, 2) ORDER BY l.row_position) FILTER (WHERE l.row_kind = 'TASK')
        = ARRAY['d1', 'd3', 'e1', 'd2', 'e2', 'e3', 'e4'],
    array_agg(right(l.task_id::text, 2) ORDER BY l.row_position)::text
)
FROM test_list('{}', '{}', '{}', FALSE, 'task_name', 'ASC', 0, 50)
    AS l(row_kind text, row_position integer, total_kind text, live_total bigint, history_total bigint,
         history_estimate_plan text, location text, task_id uuid, status text, error_code text);

SELECT test_check(
    'list: a text key descending keeps LIVE before HISTORY and the id ascending on ties',
    array_agg(right(l.task_id::text, 2) ORDER BY l.row_position) FILTER (WHERE l.row_kind = 'TASK')
        = ARRAY['e3', 'e4', 'd2', 'e2', 'd1', 'd3', 'e1'],
    array_agg(right(l.task_id::text, 2) ORDER BY l.row_position)::text
)
FROM test_list('{}', '{}', '{}', FALSE, 'task_name', 'DESC', 0, 50)
    AS l(row_kind text, row_position integer, total_kind text, live_total bigint, history_total bigint,
         history_estimate_plan text, location text, task_id uuid, status text, error_code text);

SELECT test_check(
    'list: a page cut inside a tie keeps the tie order across pages',
    (SELECT array_agg(right(l.task_id::text, 2) ORDER BY l.row_position) FILTER (WHERE l.row_kind = 'TASK')
     FROM test_list('{}', '{}', '{}', FALSE, 'task_name', 'ASC', 0, 2)
         AS l(row_kind text, row_position integer, total_kind text, live_total bigint, history_total bigint,
              history_estimate_plan text, location text, task_id uuid, status text, error_code text))
    || (SELECT array_agg(right(l.task_id::text, 2) ORDER BY l.row_position) FILTER (WHERE l.row_kind = 'TASK')
        FROM test_list('{}', '{}', '{}', FALSE, 'task_name', 'ASC', 2, 2)
            AS l(row_kind text, row_position integer, total_kind text, live_total bigint, history_total bigint,
                 history_estimate_plan text, location text, task_id uuid, status text, error_code text))
    = ARRAY['d1', 'd3', 'e1', 'd2'],
    'pages do not join'
);

SELECT test_raises('list refuses limit 0',
    $q$SELECT * FROM test_list('{}', '{}', '{}', FALSE, 'enqueued_at', 'DESC', 0, 0)
        AS l(a text, b integer, c text, d bigint, e bigint, f text, g text, h uuid, i text, j text)$q$, '22023');
SELECT test_raises('list refuses limit 201',
    $q$SELECT * FROM test_list('{}', '{}', '{}', FALSE, 'enqueued_at', 'DESC', 0, 201)
        AS l(a text, b integer, c text, d bigint, e bigint, f text, g text, h uuid, i text, j text)$q$, '22023');
SELECT test_raises('list refuses offset + limit over 500',
    $q$SELECT * FROM test_list('{}', '{}', '{}', FALSE, 'enqueued_at', 'DESC', 400, 101)
        AS l(a text, b integer, c text, d bigint, e bigint, f text, g text, h uuid, i text, j text)$q$, '22023');
SELECT test_raises('list refuses an unknown sort key',
    $q$SELECT * FROM test_list('{}', '{}', '{}', FALSE, 'input', 'DESC', 0, 10)
        AS l(a text, b integer, c text, d bigint, e bigint, f text, g text, h uuid, i text, j text)$q$, '22023');
SELECT test_raises('list refuses an unknown direction',
    $q$SELECT * FROM test_list('{}', '{}', '{}', FALSE, 'enqueued_at', 'desc; DROP TABLE x', 0, 10)
        AS l(a text, b integer, c text, d bigint, e bigint, f text, g text, h uuid, i text, j text)$q$, '22023');
SELECT test_raises('list refuses an unknown status',
    $q$SELECT * FROM test_list('{DONE}', '{}', '{}', FALSE, 'enqueued_at', 'DESC', 0, 10)
        AS l(a text, b integer, c text, d bigint, e bigint, f text, g text, h uuid, i text, j text)$q$, '22023');
SELECT test_raises('list refuses an unknown error category',
    $q$SELECT * FROM test_list('{}', '{}', '{USER}', FALSE, 'enqueued_at', 'DESC', 0, 10)
        AS l(a text, b integer, c text, d bigint, e bigint, f text, g text, h uuid, i text, j text)$q$, '22023');
SELECT test_raises('list refuses a taxonomy whose arrays differ in length',
    $q$SELECT * FROM horsies_monitoring_task_list(now() - interval '1 day', now(), '{}', '{}', '{}', '{}', '{}', '{}',
        FALSE, ARRAY['A', 'B'], ARRAY['OPERATIONAL'], 'enqueued_at', 'DESC', 0, 10)$q$, '22023');
SELECT test_raises('list refuses an empty window',
    $q$SELECT * FROM horsies_monitoring_task_list(now(), now(), '{}', '{}', '{}', '{}', '{}', '{}',
        FALSE, '{}', '{}', 'enqueued_at', 'DESC', 0, 10)$q$, '22023');

-- The text keys sort in the collation of their column, which is the database collation: the merge adds no
-- COLLATE of its own. The test server's collation sorts by code point, so the case gives task_name a collation
-- that does not (ICU en-US puts lowercase before uppercase) inside a rolled-back transaction.
COMMIT;
BEGIN;
ALTER TABLE horsies_tasks ALTER COLUMN task_name TYPE character varying(255) COLLATE "en-US-x-icu";
ALTER TABLE horsies_task_history ALTER COLUMN task_name TYPE character varying(255) COLLATE "en-US-x-icu";
SELECT test_live_task('00000000-0000-4000-8000-0000000000f1', 'PENDING', 'b_task', 'q1', 'w1',
                      now() - interval '5 minutes', NULL, 0, NULL);
SELECT test_history_task('00000000-0000-4000-8000-0000000000f2', 'COMPLETED', 'B_task', 'q1', 'w1',
                         now() - interval '5 minutes', now() - interval '4 minutes', now() - interval '3 minutes', 0, NULL);
SELECT test_live_task('00000000-0000-4000-8000-0000000000f3', 'PENDING', 'c_task', 'q1', 'w1',
                      now() - interval '5 minutes', NULL, 0, NULL);
SELECT test_check(
    'list: text keys merge in the collation of the column, not in code-point order',
    array_agg(l.task_name ORDER BY l.row_position) FILTER (WHERE l.row_kind = 'TASK')
        = ARRAY['b_task', 'B_task', 'c_task']::character varying[],
    array_agg(l.task_name ORDER BY l.row_position)::text
)
FROM horsies_monitoring_task_list(
    now() - interval '1 day', now(), '{}', '{b_task,B_task,c_task}', '{}', '{}', '{}', '{}', FALSE,
    '{}', '{}', 'task_name', 'ASC', 0, 50
) AS l;
ROLLBACK;

COMMIT;
BEGIN;
DELETE FROM horsies_task_history;
SELECT test_check(
    'list: with no filter and no history row the history total is 0 with no EXPLAIN',
    l.total_kind = 'ESTIMATE_HISTORY_EMPTY' AND l.history_total = 0 AND l.history_estimate_plan IS NULL,
    row_to_json(l)::text
)
FROM test_list('{}', '{}', '{}', FALSE, 'enqueued_at', 'DESC', 0, 50)
    AS l(row_kind text, row_position integer, total_kind text, live_total bigint, history_total bigint,
         history_estimate_plan text, location text, task_id uuid, status text, error_code text)
WHERE l.row_kind = 'TOTALS';
ROLLBACK;

------------------------------------------------------------------------------------------------------------------------
-- horsies_monitoring_task_facets
------------------------------------------------------------------------------------------------------------------------

CREATE FUNCTION test_facets(p_statuses text[], p_retried_only boolean)
RETURNS TABLE (facet text, facet_value text, facet_count bigint, category text, category_total bigint)
    LANGUAGE sql AS $$
    SELECT * FROM horsies_monitoring_task_facets(
        now() - interval '1 day', now(), p_statuses, p_retried_only,
        ARRAY['WORKER_CRASHED', 'TASK_TIMEOUT', 'RESULT_NOT_READY'], ARRAY['OPERATIONAL', 'OPERATIONAL', 'RETRIEVAL']
    )
$$;

SELECT test_check(
    'facets: live and history counts sum per value; NULL workers and empty codes are not values',
    (SELECT array_agg(f.facet_value || '=' || f.facet_count ORDER BY f.facet_value)
     FROM test_facets('{}', FALSE) AS f WHERE f.facet = 'task_name') = ARRAY['app.a=3', 'app.b=2', 'app.c=2']
    AND (SELECT array_agg(f.facet_value || '=' || f.facet_count ORDER BY f.facet_value)
         FROM test_facets('{}', FALSE) AS f WHERE f.facet = 'worker') = ARRAY['w1=1', 'w2=4']
    AND (SELECT array_agg(f.facet_value || '=' || f.facet_count || '/' || f.category || '/' || f.category_total
                          ORDER BY f.facet_value)
         FROM test_facets('{}', FALSE) AS f WHERE f.facet = 'error_code')
        = ARRAY['MY_DOMAIN_ERROR=1/DOMAIN/1', 'WORKER_CRASHED=1/OPERATIONAL/1']
    AND (SELECT bool_and(f.category IS NULL AND f.category_total IS NULL)
         FROM test_facets('{}', FALSE) AS f WHERE f.facet <> 'error_code'),
    (SELECT array_agg(f.facet || ':' || f.facet_value || '=' || f.facet_count) FROM test_facets('{}', FALSE) AS f)::text
);

SELECT test_check(
    'facets: status and retried_only scope every facet',
    (SELECT array_agg(f.facet_value || '=' || f.facet_count ORDER BY f.facet_value)
     FROM test_facets('{FAILED}', FALSE) AS f WHERE f.facet = 'task_name') = ARRAY['app.b=1', 'app.c=2']
    AND (SELECT array_agg(f.facet_value || '=' || f.facet_count ORDER BY f.facet_value)
         FROM test_facets('{}', TRUE) AS f WHERE f.facet = 'queue_name') = ARRAY['q1=1', 'q2=1'],
    'scoped facets differ'
);

-- Caps: 55 task names (cap 50) and 35 DOMAIN codes (cap 30 per category); the category total counts all 35.
COMMIT;
BEGIN;
SELECT test_history_task(gen_random_uuid(), 'FAILED', 'app.many_' || g, 'q9', 'w9',
                         now() - interval '10 minutes', now() - interval '9 minutes', now() - interval '8 minutes', 0,
                         'CODE_' || lpad(g::text, 2, '0'))
FROM generate_series(1, 35) AS g;
SELECT test_history_task(gen_random_uuid(), 'FAILED', 'app.many_' || g, 'q9', 'w9',
                         now() - interval '10 minutes', now() - interval '9 minutes', now() - interval '8 minutes', 0,
                         NULL)
FROM generate_series(36, 55) AS g;
SELECT test_check(
    'facets: at most 50 values per facet and 30 codes per category; the category total counts every code',
    (SELECT count(*) FROM test_facets('{}', FALSE) AS f WHERE f.facet = 'task_name') = 50
    AND (SELECT count(*) FROM test_facets('{}', FALSE) AS f
         WHERE f.facet = 'error_code' AND f.category = 'DOMAIN') = 30
    AND (SELECT bool_and(f.category_total = 36) FROM test_facets('{}', FALSE) AS f
         WHERE f.facet = 'error_code' AND f.category = 'DOMAIN')
    AND (SELECT array_agg(f.facet_value ORDER BY f.facet_value) FROM test_facets('{}', FALSE) AS f
         WHERE f.facet = 'error_code' AND f.category = 'OPERATIONAL') = ARRAY['WORKER_CRASHED'],
    (SELECT count(*) FROM test_facets('{}', FALSE) AS f WHERE f.facet = 'error_code')::text
);
ROLLBACK;

SELECT test_raises('facets refuse an unknown status',
    $q$SELECT * FROM test_facets('{DONE}', FALSE)$q$, '22023');
SELECT test_raises('facets refuse a NULL retried_only',
    $q$SELECT * FROM test_facets('{}', NULL)$q$, '22004');

------------------------------------------------------------------------------------------------------------------------
-- Summary
------------------------------------------------------------------------------------------------------------------------

SELECT dblink_disconnect('peer');
SELECT 'passed cases: ' || last_value AS summary FROM test_passed_cases;
