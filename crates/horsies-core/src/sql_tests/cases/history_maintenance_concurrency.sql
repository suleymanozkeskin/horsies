-- Two-session cases for crates/horsies-core/baseline/0008_history_maintenance_functions.sql.
--
-- Run on a throwaway database that holds the baseline migration and the dblink extension, in autocommit:
--   PGOPTIONS='-c horsies_test.peer_connstr=<libpq connection string of the same database>' \
--   psql -v ON_ERROR_STOP=1 -f history_maintenance_concurrency.sql
-- or send the whole file as one query message. The other sessions are dblink connections to the same database.
-- This session writes nothing and reads no table: every write and read runs on a peer in autocommit, so this
-- session holds no lock that a peer waits on, also when the file runs as one message (one transaction). The file
-- commits fixtures and leaves them behind: use a database made for this run. Each case prints "ok: <case>"; a
-- failed case raises.
--
-- A remote call whose effect a check reads runs in a statement of its own, before the check.

SET TimeZone = 'UTC';
SET client_min_messages = notice;

SELECT dblink_connect('a', current_setting('horsies_test.peer_connstr'));
SELECT dblink_connect('b', current_setting('horsies_test.peer_connstr'));
SELECT dblink_connect('holder', current_setting('horsies_test.peer_connstr'));
SELECT dblink_connect('reader', current_setting('horsies_test.peer_connstr'));

SELECT dblink_exec('a', $setup$
SET TimeZone = 'UTC';

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

-- Whether the backend p_pid waits for the advisory lock p_key.
CREATE FUNCTION fixture_waits_for_advisory(p_pid integer, p_key bigint) RETURNS boolean LANGUAGE sql AS $$
    SELECT EXISTS (
        SELECT 1 FROM pg_locks AS l
        WHERE l.locktype = 'advisory' AND l.pid = p_pid AND NOT l.granted AND l.objsubid = 1
          AND l.classid::bigint = (p_key >> 32) & 4294967295
          AND l.objid::bigint = p_key & 4294967295
    )
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

CREATE FUNCTION fixture_day(p_offset integer) RETURNS timestamptz LANGUAGE sql STABLE AS $$
    SELECT date_trunc('day', now(), 'UTC') + p_offset * interval '1 day'
$$;

CREATE FUNCTION fixture_create(p_class text, p_offset integer) RETURNS text LANGUAGE sql AS $$
    SELECT c.outcome || COALESCE('/' || c.conflict_kind, '')
    FROM horsies_create_history_leaf(p_class, fixture_day(p_offset)) AS c
$$;

CREATE FUNCTION fixture_leaf_exists(p_class text, p_offset integer) RETURNS boolean LANGUAGE sql AS $$
    SELECT to_regclass(horsies_history_leaf_name(p_class, fixture_day(p_offset))) IS NOT NULL
        OR EXISTS (SELECT 1 FROM horsies_task_history_leaf_catalog
                   WHERE leaf_name = horsies_history_leaf_name(p_class, fixture_day(p_offset)))
$$;

CREATE FUNCTION fixture_task(p_id uuid, p_class text) RETURNS void LANGUAGE sql AS $$
    INSERT INTO horsies_tasks (
        id, task_name, queue_name, priority, input, task_options, execution, retention_class_key, sent_at,
        created_at, command_fingerprint_version, command_fingerprint, idempotency_key_digest, status, enqueued_at,
        claimed_at, claimed_by_worker_id, claim_expires_at, started_at, finalizing_at, finalizing_by_worker_id,
        worker_pid, worker_hostname, worker_process_name, retry_count, max_retries, next_retry_at, good_until,
        is_workflow_task, updated_at, retain_rerun_input, rerun_of_task_id, rerun_root_task_id
    ) VALUES (
        p_id, 'fixture.task', 'default', 50, '{"c":1}', NULL, 'ISOLATED', p_class, now(), now(),
        1, sha256(convert_to(p_id::text, 'UTF8')), NULL, 'RUNNING', now(), now(), 'w1', NULL, now(),
        NULL, NULL, NULL, NULL, NULL, 0, 3, NULL, NULL, FALSE, now(), TRUE, NULL, NULL
    )
$$;

-- The caller side of a single move: the task advisory lock, the live row FOR UPDATE, then the move.
CREATE FUNCTION fixture_move(p_id uuid, p_status text, p_kind text) RETURNS void LANGUAGE plpgsql AS $$
DECLARE
    v_task horsies_tasks;
BEGIN
    PERFORM pg_advisory_xact_lock(hashtextextended(p_id::text, 731));
    SELECT * INTO STRICT v_task FROM horsies_tasks WHERE id = p_id FOR UPDATE;
    PERFORM horsies_move_task_to_history(v_task, p_status, p_kind, now(), '{"r":1}', 'E1', NULL);
END
$$;

CREATE TABLE fixture_ids (name text PRIMARY KEY, id uuid NOT NULL);
INSERT INTO fixture_ids VALUES
    ('replayed', horsies_uuidv7()), ('overtaken', horsies_uuidv7()), ('blocked_move', horsies_uuidv7());

SELECT * FROM horsies_register_retention_classes('{7,1}', 6::smallint);
SELECT * FROM horsies_publish_history_readers();
SELECT fixture_create('standard_7d', 0);
SELECT fixture_task((SELECT id FROM fixture_ids WHERE name = 'replayed'), 'standard_7d');
SELECT fixture_task((SELECT id FROM fixture_ids WHERE name = 'overtaken'), 'standard_7d');
SELECT fixture_task((SELECT id FROM fixture_ids WHERE name = 'blocked_move'), 'forever');

-- dblink_exec takes the status of the last statement, which must not return rows.
SET TimeZone = 'UTC';
$setup$);

-- The backend pid of one of this session's dblink connections.
SELECT set_config('fixture.pid_a', (SELECT pid FROM dblink('a', 'SELECT pg_backend_pid()') AS t(pid integer))::text, false);
SELECT set_config('fixture.pid_b', (SELECT pid FROM dblink('b', 'SELECT pg_backend_pid()') AS t(pid integer))::text, false);
SELECT set_config('fixture.pid_holder',
    (SELECT pid FROM dblink('holder', 'SELECT pg_backend_pid()') AS t(pid integer))::text, false);

------------------------------------------------------------------------------------------------------------------------
-- Leaf creation: the leaf try-lock and the relation lock NOWAIT
------------------------------------------------------------------------------------------------------------------------

SELECT dblink_exec('holder', 'BEGIN');
SELECT * FROM dblink('holder', $q$
    SELECT pg_advisory_xact_lock(horsies_task_history_leaf_lock_key('standard_7d', fixture_day(5)))::text
$q$) AS t(locked text);
SELECT fixture_check(
    'create leaf: another transaction holds the leaf lock: BUSY',
    (SELECT outcome FROM dblink('a', $q$ SELECT fixture_create('standard_7d', 5) $q$) AS t(outcome text)) = 'BUSY'
);
SELECT fixture_check(
    'create leaf: a BUSY outcome created no relation and no catalog row',
    NOT (SELECT present FROM dblink('a', $q$ SELECT fixture_leaf_exists('standard_7d', 5) $q$) AS t(present boolean))
);
SELECT dblink_exec('holder', 'ROLLBACK');
SELECT fixture_check(
    'create leaf: once the leaf lock is free, CREATED',
    (SELECT outcome FROM dblink('a', $q$ SELECT fixture_create('standard_7d', 5) $q$) AS t(outcome text)) = 'CREATED'
);

-- A lock on the class parent: the create does not wait.
SELECT dblink_exec('holder', 'BEGIN');
SELECT dblink_exec('holder', 'LOCK TABLE ONLY horsies_task_history_standard_7d IN ACCESS SHARE MODE');
SELECT set_config('fixture.started', clock_timestamp()::text, false);
SELECT fixture_check(
    'create leaf: another transaction holds a lock on the class parent: BUSY at once (NOWAIT)',
    (SELECT outcome FROM dblink('a', $q$ SELECT fixture_create('standard_7d', 6) $q$) AS t(outcome text)) = 'BUSY'
    AND clock_timestamp() - current_setting('fixture.started')::timestamptz < interval '1 second'
);
SELECT fixture_check(
    'create leaf: the BUSY create under a parent lock created nothing',
    NOT (SELECT present FROM dblink('a', $q$ SELECT fixture_leaf_exists('standard_7d', 6) $q$) AS t(present boolean))
);
SELECT dblink_exec('holder', 'ROLLBACK');

-- A reader of a sibling leaf holds no lock on the parent: LOCK TABLE ONLY does not wait for it.
SELECT dblink_exec('holder', 'BEGIN');
SELECT * FROM dblink('holder', $q$
    SELECT count(*) FROM horsies_task_history_standard_7d_$q$ || to_char(now() AT TIME ZONE 'UTC', 'YYYY_MM_DD')
) AS t(n bigint);
SELECT fixture_check(
    'create leaf: a reader of a sibling leaf does not make the create busy: CREATED',
    (SELECT outcome FROM dblink('a', $q$ SELECT fixture_create('standard_7d', 6) $q$) AS t(outcome text)) = 'CREATED'
);
SELECT dblink_exec('holder', 'ROLLBACK');

-- Index repair waits for no reader of the leaf: BUSY, then INDEX_REPAIRED once the reader is gone.
SELECT dblink_exec('a', $q$
    DO $do$ BEGIN
        EXECUTE format('DROP INDEX %I', horsies_history_leaf_name('standard_7d', fixture_day(6)) || '_enqueued_idx');
    END $do$
$q$);
SELECT dblink_exec('holder', 'BEGIN');
SELECT * FROM dblink('holder', $q$
    SELECT count(*) FROM horsies_task_history_standard_7d_$q$
        || to_char((now() + interval '6 days') AT TIME ZONE 'UTC', 'YYYY_MM_DD')
) AS t(n bigint);
SELECT fixture_check(
    'repair: a reader holds a lock on the leaf: BUSY, and the index stays missing',
    (SELECT outcome FROM dblink('a', $q$ SELECT fixture_create('standard_7d', 6) $q$) AS t(outcome text)) = 'BUSY'
);
SELECT fixture_check(
    'repair: the BUSY repair changed nothing',
    (SELECT missing FROM dblink('a', $q$
        SELECT to_regclass(horsies_history_leaf_name('standard_7d', fixture_day(6)) || '_enqueued_idx') IS NULL
    $q$) AS t(missing boolean))
);
SELECT dblink_exec('holder', 'ROLLBACK');
SELECT fixture_check(
    'repair: once the reader is gone, INDEX_REPAIRED',
    (SELECT outcome FROM dblink('a', $q$ SELECT fixture_create('standard_7d', 6) $q$) AS t(outcome text))
        = 'INDEX_REPAIRED'
);

-- The rename guard of the repair. The repair read the damaged index under its canonical name and holds the leaf; a
-- holder renames that index away and gives the name to an index of another table, uncommitted. The repair's rename
-- waits on the index that the holder locked; after the holder commits, the name names the other table's index, so
-- the rename takes that index. The guard refuses it before the drop: the call raises, and the other table's index
-- keeps its name.
SELECT * FROM dblink('a', $q$ SELECT fixture_create('standard_7d', 8) $q$) AS t(outcome text);
SELECT dblink_exec('a', $q$
    DO $do$ BEGIN
        EXECUTE format('DROP INDEX %I', horsies_history_leaf_name('standard_7d', fixture_day(8)) || '_enqueued_idx');
        EXECUTE format('CREATE INDEX %I ON %I (task_id)',
                       horsies_history_leaf_name('standard_7d', fixture_day(8)) || '_enqueued_idx',
                       horsies_history_leaf_name('standard_7d', fixture_day(8)));
        CREATE TABLE fixture_foreign_index_target (x integer);
    END $do$
$q$);
SELECT dblink_exec('holder', 'BEGIN');
SELECT dblink_exec('holder', $q$
    DO $do$ BEGIN
        EXECUTE format('ALTER INDEX %I RENAME TO fixture_moved_enqueued_idx',
                       horsies_history_leaf_name('standard_7d', fixture_day(8)) || '_enqueued_idx');
        EXECUTE format('CREATE INDEX %I ON fixture_foreign_index_target (x)',
                       horsies_history_leaf_name('standard_7d', fixture_day(8)) || '_enqueued_idx');
    END $do$
$q$);
SELECT dblink_send_query('a', $q$
    SELECT fixture_sqlstate(format('SELECT fixture_create(%L, 8)', 'standard_7d'))
$q$);
SELECT fixture_check(
    'repair rename guard: the repair waits on the index that the holder renamed',
    (SELECT waits FROM dblink('reader', format('SELECT fixture_wait_for_lock_wait(%s)',
        current_setting('fixture.pid_a'))) AS t(waits boolean))
    AND current_setting('fixture.pid_holder')::integer = ANY(pg_blocking_pids(current_setting('fixture.pid_a')::integer))
);
SELECT dblink_exec('holder', 'COMMIT');
SELECT fixture_check(
    'repair rename guard: the rename took the index of another table, and the guard raised',
    (SELECT state FROM dblink_get_result('a') AS t(state text)) = 'P0001'
);
SELECT * FROM dblink_get_result('a') AS t(state text);
SELECT fixture_check(
    'repair rename guard: the index of the other table keeps its name, and the leaf keeps the moved index',
    (SELECT kept FROM dblink('reader', $q$
        SELECT EXISTS (
                   SELECT 1 FROM pg_index AS x
                   WHERE x.indexrelid = to_regclass(
                             horsies_history_leaf_name('standard_7d', fixture_day(8)) || '_enqueued_idx')
                     AND x.indrelid = 'fixture_foreign_index_target'::regclass
               )
           AND EXISTS (
                   SELECT 1 FROM pg_index AS x
                   WHERE x.indexrelid = to_regclass('fixture_moved_enqueued_idx')
                     AND x.indrelid = to_regclass(horsies_history_leaf_name('standard_7d', fixture_day(8)))
               )
           AND NOT EXISTS (SELECT 1 FROM pg_class WHERE relname LIKE 'horsies\_index\_repair\_%')
    $q$) AS t(kept boolean))
);
SELECT dblink_exec('a', $q$
    DO $do$ BEGIN
        DROP TABLE fixture_foreign_index_target;
        DROP INDEX fixture_moved_enqueued_idx;
    END $do$
$q$);
SELECT fixture_check(
    'repair rename guard: once the name is free, INDEX_REPAIRED',
    (SELECT outcome FROM dblink('a', $q$ SELECT fixture_create('standard_7d', 8) $q$) AS t(outcome text))
        = 'INDEX_REPAIRED'
);

-- Two creators of one leaf: the second is BUSY while the first is open, then ALREADY_CONFORMANT.
SELECT dblink_exec('a', 'BEGIN');
SELECT fixture_check(
    'two creators: the first creates the leaf in its open transaction',
    (SELECT outcome FROM dblink('a', $q$ SELECT fixture_create('standard_7d', 7) $q$) AS t(outcome text)) = 'CREATED'
);
SELECT fixture_check(
    'two creators: the second is BUSY while the first transaction is open',
    (SELECT outcome FROM dblink('b', $q$ SELECT fixture_create('standard_7d', 7) $q$) AS t(outcome text)) = 'BUSY'
);
SELECT dblink_exec('a', 'COMMIT');
SELECT fixture_check(
    'two creators: after the first commits, the second answers ALREADY_CONFORMANT',
    (SELECT outcome FROM dblink('b', $q$ SELECT fixture_create('standard_7d', 7) $q$) AS t(outcome text))
        = 'ALREADY_CONFORMANT'
);

------------------------------------------------------------------------------------------------------------------------
-- The publication lock
------------------------------------------------------------------------------------------------------------------------

-- Two creations of new leaves of two classes: the second publication waits for the first transaction, then reads
-- the catalog with a new snapshot, so its readers probe both leaves.
SELECT dblink_exec('a', 'BEGIN');
SELECT * FROM dblink('a', $q$ SELECT fixture_create('standard_7d', 10) $q$) AS t(outcome text);
SELECT dblink_exec('b', 'BEGIN');
SELECT dblink_send_query('b', $q$ SELECT fixture_create('standard_1d', 10) $q$);
SELECT fixture_check(
    'publication lock: the second creation waits on the publication lock that the first transaction holds',
    (SELECT waits FROM dblink('reader', format(
        'SELECT fixture_wait_for_lock_wait(%s) AND fixture_waits_for_advisory(%s, hashtextextended(%L, 1601))',
        current_setting('fixture.pid_b'), current_setting('fixture.pid_b'), 'horsies:reader-publication:v1'
    )) AS t(waits boolean))
    AND current_setting('fixture.pid_a')::integer = ANY(pg_blocking_pids(current_setting('fixture.pid_b')::integer))
);
SELECT dblink_exec('a', 'COMMIT');
SELECT fixture_check(
    'publication lock: after the first commits, the second creation completes: CREATED',
    (SELECT outcome FROM dblink_get_result('b') AS t(outcome text)) = 'CREATED'
);
SELECT * FROM dblink_get_result('b') AS t(outcome text);
SELECT dblink_exec('b', 'COMMIT');
SELECT fixture_check(
    'publication lock: the second publication saw the first leaf: the manifest and the guard name both, current',
    (SELECT ok FROM dblink('reader', $q$
        SELECT horsies_history_readers_current()
           AND (SELECT count(*) FROM horsies_task_lookup_manifest
                WHERE leaf_name IN (horsies_history_leaf_name('standard_7d', fixture_day(10)),
                                    horsies_history_leaf_name('standard_1d', fixture_day(10)))) = 2
           AND position(horsies_history_leaf_name('standard_7d', fixture_day(10))
                        IN pg_get_functiondef('horsies_task_history_duplicate_staged(uuid)'::regprocedure)) > 0
           AND position(horsies_history_leaf_name('standard_1d', fixture_day(10))
                        IN pg_get_functiondef('horsies_task_history_duplicate_staged(uuid)'::regprocedure)) > 0
    $q$) AS t(ok boolean))
);

-- A publication lock held longer than the 2 s lock timeout of the creation: BUSY, and the leaf is not created.
SELECT dblink_exec('holder', 'BEGIN');
SELECT * FROM dblink('holder', $q$
    SELECT pg_advisory_xact_lock(hashtextextended('horsies:reader-publication:v1', 1601))::text
$q$) AS t(locked text);
SELECT set_config('fixture.started', clock_timestamp()::text, false);
SELECT fixture_check(
    'publication lock timeout: a creation whose publication waits 2 s is BUSY',
    (SELECT outcome FROM dblink('a', $q$ SELECT fixture_create('standard_7d', 11) $q$) AS t(outcome text)) = 'BUSY'
    AND clock_timestamp() - current_setting('fixture.started')::timestamptz >= interval '2 seconds'
    AND clock_timestamp() - current_setting('fixture.started')::timestamptz < interval '10 seconds'
);
SELECT fixture_check(
    'publication lock timeout: the BUSY creation rolled back the leaf it had made',
    NOT (SELECT present FROM dblink('a', $q$ SELECT fixture_leaf_exists('standard_7d', 11) $q$) AS t(present boolean))
);
SELECT dblink_exec('holder', 'ROLLBACK');

-- Two publications of a stale state: the second waits, then finds the first one's work current.
SELECT dblink_exec('a', $q$ COMMENT ON FUNCTION horsies_task_result_staged(uuid) IS 'stale' $q$);
SELECT dblink_exec('a', 'BEGIN');
SELECT fixture_check(
    'two publications: the first is PUBLISHED in its open transaction',
    (SELECT outcome FROM dblink('a', $q$ SELECT outcome FROM horsies_publish_history_readers() $q$) AS t(outcome text))
        = 'PUBLISHED'
);
SELECT dblink_send_query('b', $q$ SELECT outcome FROM horsies_publish_history_readers() $q$);
SELECT fixture_check(
    'two publications: the second waits on the publication lock',
    (SELECT waits FROM dblink('reader', format(
        'SELECT fixture_wait_for_lock_wait(%s) AND fixture_waits_for_advisory(%s, hashtextextended(%L, 1601))',
        current_setting('fixture.pid_b'), current_setting('fixture.pid_b'), 'horsies:reader-publication:v1'
    )) AS t(waits boolean))
);
SELECT dblink_exec('a', 'COMMIT');
SELECT fixture_check(
    'two publications: after the first commits, the second is ALREADY_CURRENT and replaces nothing',
    (SELECT outcome FROM dblink_get_result('b') AS t(outcome text)) = 'ALREADY_CURRENT'
);
SELECT * FROM dblink_get_result('b') AS t(outcome text);

-- REPEATABLE READ would read the catalog with a snapshot from before the lock: refused.
SELECT dblink_exec('b', 'BEGIN ISOLATION LEVEL REPEATABLE READ');
SELECT fixture_check(
    'isolation: the publication refuses REPEATABLE READ with 25000',
    (SELECT state FROM dblink('b', $q$
        SELECT fixture_sqlstate('SELECT * FROM horsies_publish_history_readers()')
    $q$) AS t(state text)) = '25000'
);
SELECT fixture_check(
    'isolation: a history leaf creation under REPEATABLE READ fails with 25000 and creates nothing',
    (SELECT state FROM dblink('b', $q$
        SELECT fixture_sqlstate('SELECT * FROM horsies_create_history_leaf(''standard_7d'', fixture_day(12))')
    $q$) AS t(state text)) = '25000'
    AND NOT (SELECT present FROM dblink('b', $q$ SELECT fixture_leaf_exists('standard_7d', 12) $q$) AS t(present boolean))
);
SELECT dblink_exec('b', 'ROLLBACK');

------------------------------------------------------------------------------------------------------------------------
-- Registration
------------------------------------------------------------------------------------------------------------------------

-- Two registrations of one new class: one inserts, the other waits and verifies.
SELECT dblink_exec('a', 'BEGIN');
SELECT fixture_check(
    'registration race: the first registers standard_9d in its open transaction',
    (SELECT outcome FROM dblink('a', $q$
        SELECT outcome FROM horsies_register_retention_classes('{9}', 6::smallint) WHERE class_key = 'standard_9d'
    $q$) AS t(outcome text)) = 'REGISTERED'
);
SELECT dblink_send_query('b', $q$
    SELECT outcome FROM horsies_register_retention_classes('{9}', 6::smallint) WHERE class_key = 'standard_9d'
$q$);
SELECT fixture_check(
    'registration race: the second waits for the first transaction',
    (SELECT waits FROM dblink('reader', format('SELECT fixture_wait_for_lock_wait(%s)', current_setting('fixture.pid_b')))
        AS t(waits boolean))
    AND current_setting('fixture.pid_a')::integer = ANY(pg_blocking_pids(current_setting('fixture.pid_b')::integer))
);
SELECT dblink_exec('a', 'COMMIT');
SELECT fixture_check(
    'registration race: after the first commits, the second answers VERIFIED',
    (SELECT outcome FROM dblink_get_result('b') AS t(outcome text)) = 'VERIFIED'
);
SELECT * FROM dblink_get_result('b') AS t(outcome text);
SELECT fixture_check(
    'registration race: one class row and one parent',
    (SELECT ok FROM dblink('reader', $q$
        SELECT (SELECT count(*) FROM horsies_retention_classes WHERE class_key = 'standard_9d') = 1
           AND to_regclass('horsies_task_history_standard_9d') IS NOT NULL
    $q$) AS t(ok boolean))
);

-- A new class parent locks the history root until commit: a move waits for the registration.
SELECT dblink_exec('a', 'BEGIN');
SELECT * FROM dblink('a', $q$
    SELECT outcome FROM horsies_register_retention_classes('{10}', 6::smallint) WHERE class_key = 'standard_10d'
$q$) AS t(outcome text);
SELECT dblink_send_query('b', $q$
    SELECT fixture_move((SELECT id FROM fixture_ids WHERE name = 'blocked_move'), 'COMPLETED', 'COMPLETE_FUSED')
$q$);
SELECT fixture_check(
    'registration lock order: a move waits while a registration that created a class parent is open',
    (SELECT waits FROM dblink('reader', format('SELECT fixture_wait_for_lock_wait(%s)', current_setting('fixture.pid_b')))
        AS t(waits boolean))
    AND current_setting('fixture.pid_a')::integer = ANY(pg_blocking_pids(current_setting('fixture.pid_b')::integer))
);
SELECT dblink_exec('a', 'COMMIT');
SELECT * FROM dblink_get_result('b') AS t(done text);
SELECT * FROM dblink_get_result('b') AS t(done text);
SELECT fixture_check(
    'registration lock order: after the registration commits, the move completes',
    (SELECT moved FROM dblink('reader', $q$
        SELECT EXISTS (SELECT 1 FROM horsies_task_history
                       WHERE task_id = (SELECT id FROM fixture_ids WHERE name = 'blocked_move'))
    $q$) AS t(moved boolean))
);

-- The first heartbeat registration of two sessions: the second waits, then updates the horizon it found.
SELECT dblink_exec('a', $q$ DELETE FROM horsies_retention_classes WHERE class_key = 'heartbeats' $q$);
SELECT dblink_exec('a', 'BEGIN');
SELECT fixture_check(
    'heartbeat registration race: the first registers the heartbeat class with 6 hours',
    (SELECT outcome FROM dblink('a', $q$
        SELECT outcome FROM horsies_register_retention_classes('{}', 6::smallint) WHERE class_key = 'heartbeats'
    $q$) AS t(outcome text)) = 'REGISTERED'
);
SELECT dblink_send_query('b', $q$
    SELECT outcome || ':' || previous_heartbeat_horizon_hours
    FROM horsies_register_retention_classes('{}', 10::smallint) WHERE class_key = 'heartbeats'
$q$);
SELECT fixture_check(
    'heartbeat registration race: the second waits on the inserted row',
    (SELECT waits FROM dblink('reader', format('SELECT fixture_wait_for_lock_wait(%s)', current_setting('fixture.pid_b')))
        AS t(waits boolean))
);
SELECT dblink_exec('a', 'COMMIT');
SELECT fixture_check(
    'heartbeat registration race: the second answers HORIZON_UPDATED from 6',
    (SELECT outcome FROM dblink_get_result('b') AS t(outcome text)) = 'HORIZON_UPDATED:6'
);
SELECT * FROM dblink_get_result('b') AS t(outcome text);

-- An open registration that verified the heartbeat class holds its row FOR NO KEY UPDATE: a heartbeat leaf
-- creation (whose catalog insert takes KEY SHARE on that row) does not wait for it.
SELECT dblink_exec('a', 'BEGIN');
SELECT * FROM dblink('a', $q$
    SELECT outcome FROM horsies_register_retention_classes('{}', 10::smallint) WHERE class_key = 'heartbeats'
$q$) AS t(outcome text);
SELECT fixture_check(
    'registration lock order: an open heartbeat registration does not block a heartbeat leaf creation',
    (SELECT outcome FROM dblink('b', $q$
        SELECT outcome FROM horsies_create_heartbeat_leaf(date_trunc('hour', now(), 'UTC'))
    $q$) AS t(outcome text)) = 'CREATED'
);
SELECT dblink_exec('a', 'COMMIT');

------------------------------------------------------------------------------------------------------------------------
-- Miss classification after a concurrent terminal write
------------------------------------------------------------------------------------------------------------------------

-- Session a moves the task (COMPLETE_FUSED) and stays open; the classifier in b waits on the live row, then finds
-- the committed history row through a new snapshot.
SELECT dblink_exec('a', 'BEGIN');
SELECT * FROM dblink('a', $q$
    SELECT fixture_move((SELECT id FROM fixture_ids WHERE name = 'replayed'), 'COMPLETED', 'COMPLETE_FUSED')
$q$) AS t(done text);
SELECT dblink_send_query('b', $q$
    SELECT outcome FROM horsies_terminalization_miss(
        (SELECT id FROM fixture_ids WHERE name = 'replayed'), 'COMPLETE_LOCKED', 'OWNER', 'w1', NULL)
$q$);
SELECT fixture_check(
    'miss after a concurrent write: the classifier waits on the live row that the mover holds',
    (SELECT waits FROM dblink('reader', format('SELECT fixture_wait_for_lock_wait(%s)', current_setting('fixture.pid_b')))
        AS t(waits boolean))
    AND current_setting('fixture.pid_a')::integer = ANY(pg_blocking_pids(current_setting('fixture.pid_b')::integer))
);
SELECT dblink_exec('a', 'COMMIT');
SELECT fixture_check(
    'miss after a concurrent write of the same class: ALREADY_APPLIED',
    (SELECT outcome FROM dblink_get_result('b') AS t(outcome text)) = 'ALREADY_APPLIED'
);
SELECT * FROM dblink_get_result('b') AS t(outcome text);

SELECT dblink_exec('a', 'BEGIN');
SELECT * FROM dblink('a', $q$
    SELECT fixture_move((SELECT id FROM fixture_ids WHERE name = 'overtaken'), 'CANCELLED', 'CANCEL_ORPHAN')
$q$) AS t(done text);
SELECT dblink_send_query('b', $q$
    SELECT outcome || '/' || guard_kind FROM horsies_terminalization_miss(
        (SELECT id FROM fixture_ids WHERE name = 'overtaken'), 'COMPLETE_FUSED', 'OWNER', 'w1', NULL)
$q$);
SELECT fixture_check(
    'miss after a concurrent write of another class: the classifier waits on the live row',
    (SELECT waits FROM dblink('reader', format('SELECT fixture_wait_for_lock_wait(%s)', current_setting('fixture.pid_b')))
        AS t(waits boolean))
);
SELECT dblink_exec('a', 'COMMIT');
SELECT fixture_check(
    'miss after a concurrent write of another class: SOURCE_STATE_CONFLICT FOREIGN_TERMINALIZATION',
    (SELECT outcome FROM dblink_get_result('b') AS t(outcome text)) = 'SOURCE_STATE_CONFLICT/FOREIGN_TERMINALIZATION'
);
SELECT * FROM dblink_get_result('b') AS t(outcome text);

SELECT dblink_disconnect('a');
SELECT dblink_disconnect('b');
SELECT dblink_disconnect('holder');
SELECT dblink_disconnect('reader');
