-- Two-session cases for core/baseline/0006_history_functions.sql.
--
-- Run on a throwaway database that holds the baseline migration and the dblink extension, in autocommit:
--   PGOPTIONS='-c horsies_test.peer_connstr=<libpq connection string of the same database>' \
--   psql -v ON_ERROR_STOP=1 -f history_functions_concurrency.sql
-- or send the whole file as one query message. The other sessions are dblink connections to the same database.
-- Until its last case, this session writes nothing and reads no table or catalog row: every write and read runs on
-- a peer in autocommit. So this session holds no transaction id and no lock that a peer waits on, also when the
-- file runs as one message (one transaction). The file commits fixtures and leaves them behind: use a database made
-- for this run. Each case prints "ok: <case>"; a failed case raises.
--
-- A remote call whose effect a check reads locally runs in a statement of its own, before the check: one statement
-- reads one catalog snapshot.

SET TimeZone = 'UTC';
SET client_min_messages = notice;

SELECT dblink_connect('a', current_setting('horsies_test.peer_connstr'));
SELECT dblink_connect('b', current_setting('horsies_test.peer_connstr'));
SELECT dblink_connect('maintenance', current_setting('horsies_test.peer_connstr'));
SELECT dblink_connect('holder', current_setting('horsies_test.peer_connstr'));
SELECT dblink_connect('listener', current_setting('horsies_test.peer_connstr'));

SELECT dblink_exec('a', $setup$
SET TimeZone = 'UTC';

-- Stand-ins for the published readers, as in history_functions_cases.sql.
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

CREATE FUNCTION fixture_check(p_case text, p_ok boolean) RETURNS void LANGUAGE plpgsql AS $$
BEGIN
    IF p_ok IS NOT TRUE THEN
        RAISE EXCEPTION 'case failed: %', p_case;
    END IF;
    RAISE NOTICE 'ok: %', p_case;
END
$$;

-- Polls until the backend p_pid waits on a heavyweight lock. The bound is 400 polls of 25 ms (10 s).
CREATE FUNCTION fixture_wait_for_lock_wait(p_pid integer) RETURNS boolean LANGUAGE plpgsql AS $$
DECLARE
    c_max_polls CONSTANT integer := 400;
BEGIN
    FOR poll IN 1..c_max_polls LOOP
        IF EXISTS (SELECT 1 FROM pg_stat_activity WHERE pid = p_pid AND wait_event_type = 'Lock') THEN
            RETURN TRUE;
        END IF;
        PERFORM pg_sleep(0.025);
    END LOOP;
    RETURN FALSE;
END
$$;

-- One NOWAIT attempt at the lock that a DROP or a DETACH needs; the lock ends with the calling statement.
CREATE FUNCTION fixture_try_access_exclusive(p_relation text) RETURNS text LANGUAGE plpgsql AS $$
BEGIN
    EXECUTE format('LOCK TABLE %I IN ACCESS EXCLUSIVE MODE NOWAIT', p_relation);
    RETURN 'ACQUIRED';
EXCEPTION WHEN lock_not_available THEN
    RETURN 'BUSY';
END
$$;

CREATE FUNCTION fixture_holds_advisory(p_pid integer, p_key bigint) RETURNS boolean LANGUAGE sql AS $$
    SELECT EXISTS (
        SELECT 1 FROM pg_locks AS l
        WHERE l.locktype = 'advisory' AND l.pid = p_pid AND l.granted AND l.objsubid = 1
          AND l.classid::bigint = (p_key >> 32) & 4294967295
          AND l.objid::bigint = p_key & 4294967295
    )
$$;

CREATE FUNCTION fixture_task(p_id uuid, p_status text, p_class text, p_is_workflow boolean) RETURNS void
LANGUAGE sql AS $$
    INSERT INTO horsies_tasks (
        id, task_name, queue_name, priority, input, task_options, execution, retention_class_key, sent_at,
        created_at, command_fingerprint_version, command_fingerprint, idempotency_key_digest, status, enqueued_at,
        claimed_at, claimed_by_worker_id, claim_expires_at, started_at, finalizing_at, finalizing_by_worker_id,
        worker_pid, worker_hostname, worker_process_name, retry_count, max_retries, next_retry_at, good_until,
        is_workflow_task, updated_at, retain_rerun_input, rerun_of_task_id, rerun_root_task_id
    ) VALUES (
        p_id, 'fixture.task', 'default', 50, '{"c":1}', NULL, 'ISOLATED', p_class, now(), now(),
        CASE WHEN p_is_workflow THEN NULL ELSE 1::smallint END,
        CASE WHEN p_is_workflow THEN NULL ELSE sha256(convert_to(p_id::text, 'UTF8')) END,
        NULL, p_status, now(),
        CASE WHEN p_status = 'PENDING' THEN NULL ELSE now() END,
        CASE WHEN p_status = 'PENDING' THEN NULL ELSE 'w1' END,
        CASE WHEN p_status = 'CLAIMED' THEN now() + interval '1 minute' END,
        CASE WHEN p_status = 'RUNNING' THEN now() END,
        NULL, NULL, NULL, NULL, NULL, 0, 3, NULL, NULL, p_is_workflow, now(), TRUE, NULL, NULL
    )
$$;

-- The caller side of a single move: the task advisory lock, the live row FOR UPDATE, then the move.
CREATE FUNCTION fixture_move(p_id uuid, p_status text, p_kind text, p_result text) RETURNS void
LANGUAGE plpgsql AS $$
DECLARE
    v_task horsies_tasks;
BEGIN
    PERFORM pg_advisory_xact_lock(hashtextextended(p_id::text, 731));
    SELECT * INTO STRICT v_task FROM horsies_tasks WHERE id = p_id FOR UPDATE;
    PERFORM horsies_move_task_to_history(v_task, p_status, p_kind, now(), p_result, 'E', NULL);
END
$$;

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

CREATE TABLE fixture_ids (name text PRIMARY KEY, id uuid NOT NULL);
INSERT INTO fixture_ids VALUES
    ('workflow', gen_random_uuid()), ('node', gen_random_uuid()), ('waiting', horsies_uuidv7()),
    ('notified', horsies_uuidv7()), ('batch_1', horsies_uuidv7()), ('batch_2', horsies_uuidv7()),
    ('source', horsies_uuidv7());

INSERT INTO horsies_workflows (
    id, name, definition_key, status, on_error, output_task_index, success_policy, result, error,
    parent_workflow_id, parent_task_index, root_workflow_id, depth, sent_at, created_at, started_at,
    completed_at, updated_at
) VALUES (
    (SELECT id FROM fixture_ids WHERE name = 'workflow'), 'fixture.workflow', NULL, 'RUNNING', 'fail', NULL, NULL,
    NULL, NULL, NULL, NULL, (SELECT id FROM fixture_ids WHERE name = 'workflow'), 0, now(), now(), now(), NULL, now()
);
SELECT fixture_task((SELECT id FROM fixture_ids WHERE name = 'waiting'), 'RUNNING', 'forever', TRUE);
INSERT INTO horsies_workflow_tasks (
    downstream_subworkflow, id, workflow_id, task_index, node_id, task_name, input, queue_name, priority,
    dependencies, args_from,
    workflow_ctx_from, allow_failed_deps, join_type, min_success, task_options, max_retries, execution,
    good_until, retention_class_key, status, task_id, is_subworkflow, sub_workflow_id, sub_workflow_name,
    sub_definition_key, sub_workflow_summary, result, error, created_at, started_at, completed_at
) VALUES (
    'NONE', (SELECT id FROM fixture_ids WHERE name = 'node'), (SELECT id FROM fixture_ids WHERE name = 'workflow'), 0,
    'node_0', 'fixture.task', '{"x":1}', 'default', 50, '{}', NULL, NULL, FALSE, 'all', NULL, NULL, 3, 'ISOLATED',
    NULL, 'forever', 'RUNNING', (SELECT id FROM fixture_ids WHERE name = 'waiting'), FALSE, NULL, NULL, NULL, NULL,
    NULL, NULL, now(), now(), NULL
);
SELECT fixture_task((SELECT id FROM fixture_ids WHERE name = 'notified'), 'RUNNING', 'forever', FALSE);
SELECT fixture_task((SELECT id FROM fixture_ids WHERE name = 'batch_1'), 'PENDING', 'forever', FALSE);
SELECT fixture_task((SELECT id FROM fixture_ids WHERE name = 'batch_2'), 'PENDING', 'forever', FALSE);

INSERT INTO horsies_retention_classes (class_key, class_kind, retention_days, heartbeat_horizon_hours, created_at)
VALUES ('standard_1d', 'PERIOD', 1, NULL, now());
CREATE TABLE horsies_task_history_standard_1d PARTITION OF horsies_task_history
    FOR VALUES IN ('standard_1d') PARTITION BY RANGE (terminal_at);
SELECT fixture_leaf('standard_1d', '2021-01-01 00:00:00+00');
SELECT fixture_leaf('standard_1d', '2021-01-02 00:00:00+00');
SELECT fixture_leaf('standard_1d', '2021-01-03 00:00:00+00');
SELECT fixture_leaf('standard_1d', date_trunc('day', now()));
SELECT fixture_task((SELECT id FROM fixture_ids WHERE name = 'source'), 'RUNNING', 'standard_1d', FALSE);
SELECT fixture_move((SELECT id FROM fixture_ids WHERE name = 'source'), 'FAILED', 'FAIL_RUNNING', '{"e":1}');

-- The move and the maintenance leaf lock: one leaf per case, older than the 1-day retention, so each is DETACHABLE.
SELECT fixture_leaf('standard_1d', '2021-02-01 00:00:00+00');
SELECT fixture_leaf('standard_1d', '2021-02-02 00:00:00+00');
SELECT fixture_leaf('standard_1d', '2021-02-03 00:00:00+00');
SELECT fixture_leaf('standard_1d', '2021-02-04 00:00:00+00');
INSERT INTO fixture_ids VALUES
    ('leaf_row', horsies_uuidv7()), ('leaf_row_node', gen_random_uuid()),
    ('leaf_batch', horsies_uuidv7()), ('leaf_batch_node', gen_random_uuid()),
    ('leaf_neighbour', horsies_uuidv7()), ('during_row', horsies_uuidv7());
SELECT fixture_task((SELECT id FROM fixture_ids WHERE name = 'leaf_row'), 'RUNNING', 'standard_1d', TRUE);
SELECT fixture_task((SELECT id FROM fixture_ids WHERE name = 'leaf_batch'), 'PENDING', 'standard_1d', TRUE);
SELECT fixture_task((SELECT id FROM fixture_ids WHERE name = 'leaf_neighbour'), 'RUNNING', 'standard_1d', FALSE);
SELECT fixture_task((SELECT id FROM fixture_ids WHERE name = 'during_row'), 'RUNNING', 'standard_1d', FALSE);
INSERT INTO horsies_workflow_tasks (
    downstream_subworkflow, id, workflow_id, task_index, node_id, task_name, input, queue_name, priority,
    dependencies, args_from,
    workflow_ctx_from, allow_failed_deps, join_type, min_success, task_options, max_retries, execution,
    good_until, retention_class_key, status, task_id, is_subworkflow, sub_workflow_id, sub_workflow_name,
    sub_definition_key, sub_workflow_summary, result, error, created_at, started_at, completed_at
)
SELECT
    'NONE', (SELECT id FROM fixture_ids WHERE name = n.task || '_node'), (SELECT id FROM fixture_ids WHERE name = 'workflow'),
    n.task_index, 'node_' || n.task_index, 'fixture.task', '{"x":1}', 'default', 50, '{}', NULL, NULL, FALSE, 'all',
    NULL, NULL, 3, 'ISOLATED', NULL, 'standard_1d', n.status, (SELECT id FROM fixture_ids WHERE name = n.task), FALSE,
    NULL, NULL, NULL, NULL, NULL, NULL, now(), now(), NULL
FROM (VALUES ('leaf_row', 1, 'RUNNING'), ('leaf_batch', 3, 'ENQUEUED'))
    AS n(task, task_index, status);

-- The caller side of a single move at a given terminal time (the horsies_tasks overload).
CREATE FUNCTION fixture_move_at(p_id uuid, p_terminal_at timestamptz) RETURNS void LANGUAGE plpgsql AS $$
DECLARE
    v_task horsies_tasks;
BEGIN
    PERFORM pg_advisory_xact_lock(hashtextextended(p_id::text, 731));
    SELECT * INTO STRICT v_task FROM horsies_tasks WHERE id = p_id FOR UPDATE;
    PERFORM horsies_move_task_to_history(v_task, 'FAILED', 'FAIL_RUNNING', p_terminal_at, '{"e":1}', 'E', NULL);
END
$$;

-- The caller side of a batch pending expiry: the live rows FOR UPDATE in id order, then the batch move.
CREATE FUNCTION fixture_expire_batch_at(p_ids uuid[], p_terminal_at timestamptz) RETURNS void LANGUAGE plpgsql AS $$
BEGIN
    PERFORM 1 FROM horsies_tasks WHERE id = ANY(p_ids) ORDER BY id FOR UPDATE;
    PERFORM horsies_move_tasks_to_history(p_ids, 'EXPIRED', 'EXPIRE_PENDING', p_terminal_at, '{"e":"x"}',
                                          'TASK_EXPIRED', NULL);
END
$$;

-- Runs p_sql in a subtransaction: 'OK', or the SQLSTATE it failed with.
CREATE FUNCTION fixture_sqlstate(p_sql text) RETURNS text LANGUAGE plpgsql AS $$
DECLARE
    v_state text;
BEGIN
    EXECUTE p_sql;
    RETURN 'OK';
EXCEPTION WHEN OTHERS THEN
    GET STACKED DIAGNOSTICS v_state = RETURNED_SQLSTATE;
    RETURN v_state;
END
$$;

-- The mode in which the backend p_pid holds the advisory lock p_key, or NULL when it holds none.
CREATE FUNCTION fixture_advisory_mode(p_pid integer, p_key bigint) RETURNS text LANGUAGE sql AS $$
    SELECT l.mode FROM pg_locks AS l
    WHERE l.locktype = 'advisory' AND l.pid = p_pid AND l.granted AND l.objsubid = 1
      AND l.classid::bigint = (p_key >> 32) & 4294967295
      AND l.objid::bigint = p_key & 4294967295
$$;

-- Whether the backend p_pid waits for the advisory lock p_key.
CREATE FUNCTION fixture_waits_for_advisory(p_pid integer, p_key bigint) RETURNS boolean LANGUAGE sql AS $$
    SELECT EXISTS (
        SELECT 1 FROM pg_locks AS l
        WHERE l.locktype = 'advisory' AND l.pid = p_pid AND NOT l.granted AND l.objsubid = 1
          AND l.classid::bigint = (p_key >> 32) & 4294967295
          AND l.objid::bigint = p_key & 4294967295
    )
$$;

-- Whether the backend p_pid holds or waits for a lock on any history relation (the partitioned parents and leaves).
CREATE FUNCTION fixture_locks_history(p_pid integer) RETURNS boolean LANGUAGE sql AS $$
    SELECT EXISTS (
        SELECT 1 FROM pg_locks AS l JOIN pg_class AS c ON c.oid = l.relation
        WHERE l.pid = p_pid AND (c.relname = 'horsies_task_history' OR c.relname LIKE 'horsies\_task\_history\_%')
          AND c.relname NOT IN ('horsies_task_history_clock_exceptions', 'horsies_task_history_leaf_catalog')
          AND c.relkind IN ('r', 'p')
    )
$$;

-- Where a task is: its live row, its history row and its pending row.
CREATE FUNCTION fixture_place(p_name text) RETURNS text LANGUAGE sql AS $$
    SELECT concat_ws(',',
        CASE WHEN EXISTS (SELECT 1 FROM horsies_tasks WHERE id = f.id) THEN 'live' END,
        CASE WHEN EXISTS (SELECT 1 FROM horsies_task_history WHERE task_id = f.id) THEN 'history' END,
        CASE WHEN EXISTS (SELECT 1 FROM horsies_workflow_phase2_pending WHERE task_id = f.id) THEN 'pending' END)
    FROM fixture_ids AS f WHERE f.name = p_name
$$;

-- The backend pid of one of the calling session's dblink connections.
CREATE FUNCTION fixture_peer_pid(p_connection text) RETURNS integer LANGUAGE sql AS $$
    SELECT pid FROM dblink(p_connection, 'SELECT pg_backend_pid()') AS t(pid integer)
$$;

-- dblink_exec takes the status of the last statement, which must not return rows.
SET TimeZone = 'UTC';
$setup$);

------------------------------------------------------------------------------------------------------------------------
-- The single move: lock order and notifications
------------------------------------------------------------------------------------------------------------------------

-- Session a holds the node row. Session b runs the caller and the move: it takes the task advisory lock and the
-- live row, then waits on the node row.
SELECT dblink_exec('a', 'BEGIN');
SELECT * FROM dblink('a', format(
    'SELECT 1 FROM horsies_workflow_tasks WHERE task_id = (SELECT id FROM fixture_ids WHERE name = %L) FOR UPDATE',
    'waiting'
)) AS t(one integer);
SELECT set_config('fixture.pid_b', fixture_peer_pid('b')::text, false);
SELECT dblink_send_query('b', $q$
    SELECT fixture_move((SELECT id FROM fixture_ids WHERE name = 'waiting'), 'FAILED', 'FAIL_RUNNING', '{"e":1}')
$q$);
SELECT fixture_check(
    'move lock order: the move waits on the node row that another session holds',
    fixture_wait_for_lock_wait(current_setting('fixture.pid_b')::integer)
    AND fixture_peer_pid('a') = ANY(pg_blocking_pids(current_setting('fixture.pid_b')::integer))
);
SELECT fixture_check(
    'move lock order: while it waits, the mover already holds the task advisory lock',
    fixture_holds_advisory(
        current_setting('fixture.pid_b')::integer,
        (SELECT key FROM dblink('a', $q$
            SELECT hashtextextended(id::text, 731) FROM fixture_ids WHERE name = 'waiting'
        $q$) AS t(key bigint))
    )
);
SELECT dblink_exec('a', 'COMMIT');
SELECT * FROM dblink_get_result('b') AS t(done text);
SELECT * FROM dblink_get_result('b') AS t(done text);
SELECT fixture_check(
    'move lock order: after the node row is free, the move completes with its pending row',
    (SELECT moved FROM dblink('a', $q$
        SELECT EXISTS (SELECT 1 FROM horsies_task_history WHERE task_id = f.id)
               AND EXISTS (SELECT 1 FROM horsies_workflow_phase2_pending WHERE task_id = f.id)
               AND NOT EXISTS (SELECT 1 FROM horsies_tasks WHERE id = f.id)
        FROM fixture_ids AS f WHERE f.name = 'waiting'
    $q$) AS t(moved boolean))
);

-- Notifications are delivered at commit: task_done and horsies_task_status for each moved task.
SELECT dblink_exec('listener', 'LISTEN task_done');
SELECT dblink_exec('listener', 'LISTEN horsies_task_status');
SELECT * FROM dblink('a', $q$
    SELECT fixture_move((SELECT id FROM fixture_ids WHERE name = 'notified'), 'COMPLETED', 'COMPLETE_FUSED',
                        '{"ok":1}')
$q$) AS t(done text);
SELECT * FROM dblink('a', $q$
    SELECT horsies_move_tasks_to_history(
        ARRAY[(SELECT id FROM fixture_ids WHERE name = 'batch_1'), (SELECT id FROM fixture_ids WHERE name = 'batch_2')],
        'EXPIRED', 'EXPIRE_PENDING', now(), NULL, 'TASK_EXPIRED', NULL)
$q$) AS t(done text);
SELECT pg_sleep(0.2);
SELECT fixture_check(
    'move notifications: one task_done and one horsies_task_status per task, sent at commit, in id order',
    (SELECT array_agg(n.notify_name || ':' || f.name ORDER BY n.position)
     FROM dblink_get_notify('listener') WITH ORDINALITY AS n(notify_name, be_pid, extra, position)
     JOIN dblink('a', 'SELECT name, id::text FROM fixture_ids') AS f(name text, id text) ON f.id = n.extra)
    = ARRAY[
        'task_done:notified', 'horsies_task_status:notified',
        'task_done:batch_1', 'task_done:batch_2', 'horsies_task_status:batch_1', 'horsies_task_status:batch_2'
    ]::text[]
);

------------------------------------------------------------------------------------------------------------------------
-- The rerun read keeps its source leaf
------------------------------------------------------------------------------------------------------------------------

SELECT dblink_exec('a', 'BEGIN');
SELECT * FROM dblink('a', $q$
    SELECT location FROM horsies_rerun_source_read((SELECT id FROM fixture_ids WHERE name = 'source'), 'forever')
$q$) AS t(location text);
SELECT fixture_check(
    'rerun read: the caller''s transaction holds ACCESS SHARE on the source leaf until it ends',
    EXISTS (
        SELECT 1 FROM pg_locks AS l
        WHERE l.pid = fixture_peer_pid('a') AND l.mode = 'AccessShareLock' AND l.granted
          AND l.relation = (SELECT oid FROM dblink('maintenance', format(
              'SELECT to_regclass(%L)::oid', 'horsies_task_history_standard_1d_' || to_char(now(), 'YYYY_MM_DD')
          )) AS t(oid oid))
    )
);
SELECT fixture_check(
    'rerun read: retention cannot take the source leaf while the read''s transaction is open',
    (SELECT attempt FROM dblink('maintenance', format(
        'SELECT fixture_try_access_exclusive(%L)', 'horsies_task_history_standard_1d_' || to_char(now(), 'YYYY_MM_DD')
    )) AS t(attempt text)) = 'BUSY'
);
SELECT dblink_exec('a', 'COMMIT');
SELECT fixture_check(
    'rerun read: once the read''s transaction ends, the source leaf lock is free',
    (SELECT attempt FROM dblink('maintenance', format(
        'SELECT fixture_try_access_exclusive(%L)', 'horsies_task_history_standard_1d_' || to_char(now(), 'YYYY_MM_DD')
    )) AS t(attempt text)) = 'ACQUIRED'
);

------------------------------------------------------------------------------------------------------------------------
-- The move and the maintenance leaf lock
------------------------------------------------------------------------------------------------------------------------

-- A move takes the leaf lock of its row's leaf in shared mode before it reads history. Each case moves into its own
-- leaf of 2021, which the 1-day retention makes DETACHABLE.
SELECT set_config('fixture.pid_maintenance', fixture_peer_pid('maintenance')::text, false);

-- A move that has not ended, before the detach starts (horsies_tasks overload, workflow task, deferred kind).
SELECT dblink_exec('b', 'BEGIN');
SELECT * FROM dblink('b', $q$
    SELECT fixture_move_at((SELECT id FROM fixture_ids WHERE name = 'leaf_row'), '2021-02-01 10:00:00+00')
$q$) AS t(done text);
SELECT fixture_check(
    'leaf lock: a move that has not ended holds the leaf lock of its row''s leaf in shared mode',
    fixture_advisory_mode(
        current_setting('fixture.pid_b')::integer,
        horsies_task_history_leaf_lock_key('standard_1d', '2021-02-01 10:00:00+00')
    ) = 'ShareLock'
);
SELECT fixture_check(
    'leaf lock: while the move has not ended, the detach of its leaf is BUSY and keeps no lock',
    (SELECT outcome FROM dblink('maintenance', $q$
        SELECT outcome FROM horsies_begin_leaf_detach('horsies_task_history_standard_1d_2021_02_01')
    $q$) AS t(outcome text)) = 'BUSY'
    AND fixture_advisory_mode(
        current_setting('fixture.pid_maintenance')::integer,
        horsies_task_history_leaf_lock_key('standard_1d', '2021-02-01 00:00:00+00')
    ) IS NULL
);
SELECT fixture_check(
    'leaf lock: a second move into the same leaf does not wait for the first',
    (SELECT state FROM dblink('a', $q$
        SET lock_timeout = '5s';
        SELECT fixture_sqlstate(format(
            'SELECT fixture_move_at(%L, %L)',
            (SELECT id FROM fixture_ids WHERE name = 'leaf_neighbour'), '2021-02-01 11:00:00+00'
        ));
    $q$) AS t(state text)) = 'OK'
);
SELECT dblink_exec('a', 'RESET lock_timeout');
SELECT dblink_exec('b', 'COMMIT');
SELECT fixture_check(
    'leaf lock: after the move commits, its phase-2 pending row pins the leaf: PENDING_BLOCKED',
    (SELECT outcome FROM dblink('maintenance', $q$
        SELECT outcome FROM horsies_begin_leaf_detach('horsies_task_history_standard_1d_2021_02_01')
    $q$) AS t(outcome text)) = 'PENDING_BLOCKED'
    AND (SELECT place FROM dblink('a', $q$SELECT fixture_place('leaf_row')$q$) AS t(place text)) = 'history,pending'
);

-- The same before the detach, through the batch move (pending expiry of a workflow task).
SELECT dblink_exec('b', 'BEGIN');
SELECT * FROM dblink('b', $q$
    SELECT fixture_expire_batch_at(ARRAY[(SELECT id FROM fixture_ids WHERE name = 'leaf_batch')],
                                   '2021-02-03 10:00:00+00')
$q$) AS t(done text);
SELECT fixture_check(
    'leaf lock, batch move: while the batch has not ended, the detach of its leaf is BUSY',
    fixture_advisory_mode(
        current_setting('fixture.pid_b')::integer,
        horsies_task_history_leaf_lock_key('standard_1d', '2021-02-03 10:00:00+00')
    ) = 'ShareLock'
    AND (SELECT outcome FROM dblink('maintenance', $q$
        SELECT outcome FROM horsies_begin_leaf_detach('horsies_task_history_standard_1d_2021_02_03')
    $q$) AS t(outcome text)) = 'BUSY'
);
SELECT dblink_exec('b', 'COMMIT');
SELECT fixture_check(
    'leaf lock, batch move: after the batch commits, the leaf is PENDING_BLOCKED',
    (SELECT outcome FROM dblink('maintenance', $q$
        SELECT outcome FROM horsies_begin_leaf_detach('horsies_task_history_standard_1d_2021_02_03')
    $q$) AS t(outcome text)) = 'PENDING_BLOCKED'
    AND (SELECT place FROM dblink('a', $q$SELECT fixture_place('leaf_batch')$q$) AS t(place text))
        = 'history,pending'
);

-- Moves while the detach runs. A reader of the class parent keeps the concurrent detach waiting in its second
-- transaction, with the maintenance session holding the leaf lock. A move then waits for that lock. The long timeouts keep the case independent of the speed of the machine.
SELECT dblink_exec('holder', 'BEGIN');
SELECT * FROM dblink('holder', 'SELECT count(*) FROM horsies_task_history_standard_1d') AS t(n bigint);
SELECT fixture_check(
    'detach during moves: begin returns DETACHABLE',
    (SELECT outcome FROM dblink('maintenance', $q$
        SELECT outcome FROM horsies_begin_leaf_detach('horsies_task_history_standard_1d_2021_02_04')
    $q$) AS t(outcome text)) = 'DETACHABLE'
);
SELECT dblink_exec('maintenance', $q$SET statement_timeout = '60s'; SET lock_timeout = '30s'$q$);
SELECT dblink_send_query('maintenance',
    'ALTER TABLE horsies_task_history_standard_1d DETACH PARTITION horsies_task_history_standard_1d_2021_02_04 CONCURRENTLY');
SELECT fixture_check(
    'detach during moves: the detach waits for the reader of the class parent',
    fixture_wait_for_lock_wait(current_setting('fixture.pid_maintenance')::integer)
    AND fixture_peer_pid('holder') = ANY(pg_blocking_pids(current_setting('fixture.pid_maintenance')::integer))
);
SELECT dblink_send_query('b', $q$
    SELECT fixture_sqlstate(format(
        'SELECT fixture_move_at(%L, %L)', (SELECT id FROM fixture_ids WHERE name = 'during_row'),
        '2021-02-04 10:00:00+00'
    ))
$q$);
SELECT fixture_check(
    'detach during moves: the move waits for the leaf lock that the detach holds',
    fixture_wait_for_lock_wait(current_setting('fixture.pid_b')::integer)
    AND fixture_waits_for_advisory(
        current_setting('fixture.pid_b')::integer,
        horsies_task_history_leaf_lock_key('standard_1d', '2021-02-04 00:00:00+00')
    )
);
SELECT fixture_check(
    'detach during moves: lock order: a waiting move already holds its task advisory lock and no history lock',
    fixture_advisory_mode(
        current_setting('fixture.pid_b')::integer,
        (SELECT key FROM dblink('a', $q$
            SELECT hashtextextended(id::text, 731) FROM fixture_ids WHERE name = 'during_row'
        $q$) AS t(key bigint))
    ) = 'ExclusiveLock'
    AND (SELECT locks FROM dblink('a', format(
        'SELECT fixture_locks_history(%s)', current_setting('fixture.pid_b')
    )) AS t(locks boolean)) = FALSE
);
SELECT dblink_exec('holder', 'ROLLBACK');
-- The detach completes while the move still waits: it does not wait for it, so no deadlock can form.
SELECT * FROM dblink_get_result('maintenance') AS t(done text);
SELECT * FROM dblink_get_result('maintenance') AS t(done text);
SELECT fixture_check(
    'detach during moves: the detach completes while the move waits for its leaf lock',
    (SELECT detached FROM dblink('a', $q$
        SELECT NOT EXISTS (
            SELECT 1 FROM pg_inherits WHERE inhrelid = to_regclass('horsies_task_history_standard_1d_2021_02_04')
        )
    $q$) AS t(detached boolean))
    AND fixture_waits_for_advisory(
        current_setting('fixture.pid_b')::integer,
        horsies_task_history_leaf_lock_key('standard_1d', '2021-02-04 00:00:00+00')
    )
);
SELECT fixture_check(
    'detach during moves: the record is RECORDED',
    (SELECT outcome FROM dblink('maintenance', $q$
        SELECT outcome FROM horsies_record_leaf_detached('horsies_task_history_standard_1d_2021_02_04')
    $q$) AS t(outcome text)) = 'RECORDED'
);
SELECT dblink_exec('maintenance', $q$RESET statement_timeout; RESET lock_timeout$q$);
SELECT fixture_check(
    'detach during moves: the owner releases the leaf lock',
    (SELECT unlocked FROM dblink('maintenance', $q$
        SELECT pg_advisory_unlock(horsies_task_history_leaf_lock_key('standard_1d', '2021-02-04 00:00:00+00'))
    $q$) AS t(unlocked boolean))
);
SELECT fixture_check(
    'detach during moves: the move then finds no leaf for its row: 23514',
    (SELECT state FROM dblink_get_result('b') AS t(state text)) = '23514'
);
SELECT * FROM dblink_get_result('b') AS t(state text);
SELECT fixture_check(
    'detach during moves: the task stays live, with no history row',
    (SELECT place FROM dblink('a', $q$SELECT fixture_place('during_row')$q$) AS t(place text)) = 'live'
);

-- After the detach, before the drop: the leaf has no parent.
SELECT fixture_check(
    'after detach: a move into the detached leaf raises 23514 and the task stays live',
    (SELECT states FROM dblink('a', $q$
        SELECT fixture_sqlstate(format('SELECT fixture_move_at(%L, %L)',
                   (SELECT id FROM fixture_ids WHERE name = 'during_row'), '2021-02-04 10:00:00+00'))
            || '/' || fixture_place('during_row')
    $q$) AS t(states text)) = '23514/live'
);

-- After the drop.
SELECT fixture_check(
    'after drop: the drop is DROPPED',
    (SELECT outcome FROM dblink('maintenance', $q$
        SELECT outcome FROM horsies_drop_detached_leaf('horsies_task_history_standard_1d_2021_02_04')
    $q$) AS t(outcome text)) = 'DROPPED'
);
SELECT fixture_check(
    'after drop: a move into the dropped leaf raises 55H01 (history leaf missing) and the task stays live',
    (SELECT states FROM dblink('a', $q$
        SELECT fixture_sqlstate(format('SELECT fixture_move_at(%L, %L)',
                   (SELECT id FROM fixture_ids WHERE name = 'during_row'), '2021-02-04 10:00:00+00'))
            || '/' || fixture_place('during_row')
    $q$) AS t(states text)) = '55H01/live'
);
SELECT fixture_check(
    'after drop: a later move at the database clock succeeds',
    (SELECT states FROM dblink('a', $q$
        SELECT fixture_sqlstate(format('SELECT fixture_move_at(%L, now())',
                   (SELECT id FROM fixture_ids WHERE name = 'during_row')))
            || '/' || fixture_place('during_row')
    $q$) AS t(states text)) = 'OK/history'
);

------------------------------------------------------------------------------------------------------------------------
-- Detach and drop under contention
------------------------------------------------------------------------------------------------------------------------

-- Another session holds the session leaf lock: the detach is BUSY.
SELECT * FROM dblink('holder', $q$
    SELECT pg_advisory_lock(horsies_task_history_leaf_lock_key('standard_1d', '2021-01-01 00:00:00+00'))::text
$q$) AS t(locked text);
SELECT fixture_check(
    'begin detach: another session holds the leaf lock: BUSY',
    (SELECT outcome FROM dblink('maintenance', $q$
        SELECT outcome FROM horsies_begin_leaf_detach('horsies_task_history_standard_1d_2021_01_01')
    $q$) AS t(outcome text)) = 'BUSY'
);
SELECT fixture_check(
    'begin detach: a BUSY outcome leaves no lock behind',
    NOT fixture_holds_advisory(
        fixture_peer_pid('maintenance'), horsies_task_history_leaf_lock_key('standard_1d', '2021-01-01 00:00:00+00')
    )
);
SELECT * FROM dblink('holder', $q$
    SELECT pg_advisory_unlock(horsies_task_history_leaf_lock_key('standard_1d', '2021-01-01 00:00:00+00'))::text
$q$) AS t(unlocked text);

-- The whole detach of one leaf, as the core runs it on its maintenance connection.
SELECT fixture_check(
    'detach flow: begin returns DETACHABLE',
    (SELECT outcome FROM dblink('maintenance', $q$
        SELECT outcome FROM horsies_begin_leaf_detach('horsies_task_history_standard_1d_2021_01_01')
    $q$) AS t(outcome text)) = 'DETACHABLE'
);
SELECT fixture_check(
    'detach flow: after DETACHABLE the maintenance session holds the leaf lock',
    fixture_holds_advisory(
        fixture_peer_pid('maintenance'), horsies_task_history_leaf_lock_key('standard_1d', '2021-01-01 00:00:00+00')
    )
);
SELECT dblink_exec('maintenance', $q$SET statement_timeout = '30s'; SET lock_timeout = '2s'$q$);
SELECT dblink_exec('maintenance',
    'ALTER TABLE horsies_task_history_standard_1d DETACH PARTITION horsies_task_history_standard_1d_2021_01_01 CONCURRENTLY');
SELECT fixture_check(
    'detach flow: after the concurrent detach, the record is RECORDED',
    (SELECT outcome FROM dblink('maintenance', $q$
        SELECT outcome FROM horsies_record_leaf_detached('horsies_task_history_standard_1d_2021_01_01')
    $q$) AS t(outcome text)) = 'RECORDED'
);
SELECT dblink_exec('maintenance', $q$RESET statement_timeout; RESET lock_timeout$q$);
SELECT fixture_check(
    'detach flow: the owner releases the leaf lock',
    (SELECT unlocked FROM dblink('maintenance', $q$
        SELECT pg_advisory_unlock(horsies_task_history_leaf_lock_key('standard_1d', '2021-01-01 00:00:00+00'))
    $q$) AS t(unlocked boolean))
);

-- Another transaction holds the leaf lock: the drop is BUSY.
SELECT dblink_exec('holder', 'BEGIN');
SELECT * FROM dblink('holder', $q$
    SELECT pg_advisory_xact_lock(horsies_task_history_leaf_lock_key('standard_1d', '2021-01-01 00:00:00+00'))::text
$q$) AS t(locked text);
SELECT fixture_check(
    'drop: another transaction holds the leaf lock: BUSY',
    (SELECT outcome FROM dblink('maintenance', $q$
        SELECT outcome FROM horsies_drop_detached_leaf('horsies_task_history_standard_1d_2021_01_01')
    $q$) AS t(outcome text)) = 'BUSY'
);
SELECT dblink_exec('holder', 'ROLLBACK');

-- A reader holds a lock on the detached relation: the drop does not wait, it is BUSY.
SELECT dblink_exec('holder', 'BEGIN');
SELECT * FROM dblink('holder', 'SELECT count(*) FROM horsies_task_history_standard_1d_2021_01_01')
    AS t(n bigint);
SELECT fixture_check(
    'drop: a reader holds the detached relation: BUSY at once',
    (SELECT outcome FROM dblink('maintenance', $q$
        SELECT outcome FROM horsies_drop_detached_leaf('horsies_task_history_standard_1d_2021_01_01')
    $q$) AS t(outcome text)) = 'BUSY'
);
SELECT fixture_check(
    'drop: after a BUSY drop the leaf stays',
    (SELECT present FROM dblink('a', $q$
        SELECT to_regclass('horsies_task_history_standard_1d_2021_01_01') IS NOT NULL
    $q$) AS t(present boolean))
);
SELECT dblink_exec('holder', 'ROLLBACK');
SELECT fixture_check(
    'drop: with the reader gone, the drop is DROPPED',
    (SELECT outcome FROM dblink('maintenance', $q$
        SELECT outcome FROM horsies_drop_detached_leaf('horsies_task_history_standard_1d_2021_01_01')
    $q$) AS t(outcome text)) = 'DROPPED'
);
SELECT fixture_check(
    'drop: after DROPPED the relation is gone',
    (SELECT gone FROM dblink('a', $q$
        SELECT to_regclass('horsies_task_history_standard_1d_2021_01_01') IS NULL
    $q$) AS t(gone boolean))
);

-- An interrupted concurrent detach: a reader with an old snapshot keeps the detach waiting, and the detach is
-- cancelled. The leaf stays pending finalization.
SELECT dblink_exec('holder', 'BEGIN ISOLATION LEVEL REPEATABLE READ');
SELECT * FROM dblink('holder', 'SELECT count(*) FROM horsies_task_history') AS t(n bigint);
SELECT dblink_send_query('b',
    'ALTER TABLE horsies_task_history_standard_1d DETACH PARTITION horsies_task_history_standard_1d_2021_01_02 CONCURRENTLY');
SELECT fixture_check(
    'interrupted detach: fixture detach waits on the old snapshot',
    fixture_wait_for_lock_wait(current_setting('fixture.pid_b')::integer)
);
SELECT pg_cancel_backend(current_setting('fixture.pid_b')::integer);
SELECT * FROM dblink_get_result('b', false) AS t(done text);
SELECT * FROM dblink_get_result('b', false) AS t(done text);
SELECT dblink_exec('holder', 'ROLLBACK');
SELECT fixture_check(
    'interrupted detach: fixture leaves the leaf pending finalization',
    (SELECT pending FROM dblink('a', $q$
        SELECT i.inhdetachpending FROM pg_inherits AS i
        WHERE i.inhrelid = to_regclass('horsies_task_history_standard_1d_2021_01_02')
    $q$) AS t(pending boolean))
);
SELECT fixture_check(
    'interrupted detach: begin returns DETACH_INTERRUPTED',
    (SELECT outcome FROM dblink('maintenance', $q$
        SELECT outcome FROM horsies_begin_leaf_detach('horsies_task_history_standard_1d_2021_01_02')
    $q$) AS t(outcome text)) = 'DETACH_INTERRUPTED'
);
SELECT fixture_check(
    'interrupted detach: after DETACH_INTERRUPTED the maintenance session holds the leaf lock',
    fixture_holds_advisory(
        fixture_peer_pid('maintenance'), horsies_task_history_leaf_lock_key('standard_1d', '2021-01-02 00:00:00+00')
    )
);
SELECT fixture_check(
    'interrupted detach: the candidates show it as DETACH_PENDING',
    (SELECT attachment FROM dblink('a', $q$
        SELECT c.attachment FROM horsies_prune_candidates(100) AS c
        WHERE c.leaf_name = 'horsies_task_history_standard_1d_2021_01_02'
    $q$) AS t(attachment text)) = 'DETACH_PENDING'
);
-- FINALIZE waits for every transaction of the database whose snapshot xmin is not newer than its own
-- (WaitForOlderSnapshots), so a session that waits for it inside one statement or one transaction never sees it
-- end. This session runs FINALIZE and the record itself, as the last writes of the file; the maintenance session
-- keeps the leaf lock that begin took.
ALTER TABLE horsies_task_history_standard_1d DETACH PARTITION horsies_task_history_standard_1d_2021_01_02 FINALIZE;
SELECT fixture_check(
    'interrupted detach: after FINALIZE the record is RECORDED',
    (SELECT r.outcome FROM horsies_record_leaf_detached('horsies_task_history_standard_1d_2021_01_02') AS r)
    = 'RECORDED'
);
SELECT fixture_check(
    'interrupted detach: the owner releases the leaf lock',
    (SELECT unlocked FROM dblink('maintenance', $q$
        SELECT pg_advisory_unlock(horsies_task_history_leaf_lock_key('standard_1d', '2021-01-02 00:00:00+00'))
    $q$) AS t(unlocked boolean))
);

SELECT dblink_disconnect('a');
SELECT dblink_disconnect('b');
SELECT dblink_disconnect('maintenance');
SELECT dblink_disconnect('holder');
SELECT dblink_disconnect('listener');
