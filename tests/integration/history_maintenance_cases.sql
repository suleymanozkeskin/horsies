-- Cases for crates/horsies-core/baseline/0008_history_maintenance_functions.sql, one session.
--
-- Run on a database that holds the baseline migration, in one transaction that the caller rolls back:
--   psql -v ON_ERROR_STOP=1 --single-transaction -f history_maintenance_cases.sql
-- Each case prints "ok: <case>"; a failed case raises and stops the script. The two-session cases are in
-- history_maintenance_concurrency.sql.
--
-- No stand-in readers: the cases publish the readers with horsies_publish_history_readers and read through them.
-- The session time zone is not UTC, so every leaf name and bound shows that the functions do not depend on it.

SET TimeZone = 'Asia/Kolkata';
SET client_min_messages = notice;

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

CREATE FUNCTION fixture_today() RETURNS timestamptz LANGUAGE sql STABLE AS $$
    SELECT date_trunc('day', now(), 'UTC')
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
        CASE WHEN p_status = 'PENDING' THEN NULL ELSE now() - interval '2 seconds' END,
        CASE WHEN p_status = 'PENDING' THEN NULL ELSE 'w1' END,
        CASE WHEN p_status = 'CLAIMED' THEN now() + interval '1 minute' END,
        CASE WHEN p_status = 'RUNNING' THEN now() - interval '1 second' END,
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
    PERFORM horsies_move_task_to_history(v_task, p_status, p_kind, now(), p_result, 'E1', 'reason');
END
$$;

-- The outcome of horsies_create_history_leaf as 'OUTCOME' or 'OUTCOME/CONFLICT_KIND'.
CREATE FUNCTION fixture_create(p_class text, p_lower timestamptz) RETURNS text LANGUAGE sql AS $$
    SELECT c.outcome || COALESCE('/' || c.conflict_kind, '') FROM horsies_create_history_leaf(p_class, p_lower) AS c
$$;

CREATE FUNCTION fixture_create_heartbeat(p_lower timestamptz) RETURNS text LANGUAGE sql AS $$
    SELECT c.outcome || COALESCE('/' || c.conflict_kind, '') FROM horsies_create_heartbeat_leaf(p_lower) AS c
$$;

-- Whether the published reader's text names the relation (as a probe target).
CREATE FUNCTION fixture_reader_names(p_signature text, p_relation text) RETURNS boolean LANGUAGE sql AS $$
    SELECT position(' ' || p_relation || ' ' IN pg_get_functiondef(to_regprocedure(p_signature))) > 0
$$;

-- The pg_proc row versions of the four readers: a replacement writes new ones.
CREATE FUNCTION fixture_reader_versions() RETURNS text LANGUAGE sql AS $$
    SELECT string_agg(p.ctid::text, ',' ORDER BY s.signature)
    FROM horsies_history_reader_signatures() AS s
    JOIN pg_proc AS p ON p.oid = to_regprocedure(s.signature)
$$;

CREATE FUNCTION fixture_classify(p_id uuid, p_kind text, p_fence_kind text, p_worker text, p_claimed_at timestamptz)
RETURNS text LANGUAGE sql AS $$
    SELECT m.outcome || COALESCE('/' || m.guard_kind, '')
    FROM horsies_terminalization_miss(p_id, p_kind, p_fence_kind, p_worker, p_claimed_at) AS m
$$;

------------------------------------------------------------------------------------------------------------------------
-- Period day counts (one rule for the probe and registration)
------------------------------------------------------------------------------------------------------------------------

SELECT fixture_check(
    'period days: the declared days and 30, distinct, ascending',
    horsies_retention_period_days('{365,7}') = '{7,30,365}'::smallint[]
    AND horsies_retention_period_days('{}') = '{30}'::smallint[]
    AND horsies_retention_period_days('{30}') = '{30}'::smallint[]
);
SELECT fixture_expect_error('period days: a NULL array is refused',
    'SELECT horsies_retention_period_days(NULL)', '22023');
SELECT fixture_expect_error('period days: a NULL element is refused',
    'SELECT horsies_retention_period_days(''{7,NULL}'')', '22023');
SELECT fixture_expect_error('period days: 0 days is refused',
    'SELECT horsies_retention_period_days(''{0}'')', '22023');
SELECT fixture_expect_error('period days: 1826 days is refused',
    'SELECT horsies_retention_period_days(''{1826}'')', '22023');
SELECT fixture_expect_error('period days: a day count listed twice is refused',
    'SELECT horsies_retention_period_days(''{7,7}'')', '22023');

------------------------------------------------------------------------------------------------------------------------
-- The probe of a fresh install
------------------------------------------------------------------------------------------------------------------------

CREATE TEMP TABLE probe_fresh AS SELECT * FROM horsies_coverage_probe('{7}', 7::smallint, 6::smallint);
SELECT fixture_check(
    'probe, fresh install: the forever leaf and 7 heartbeat hours, no period class yet',
    (SELECT count(*) FROM probe_fresh) = 8
    AND (SELECT count(*) FROM probe_fresh WHERE leaf_kind = 'heartbeat') = 7
    AND (SELECT count(*) FROM probe_fresh WHERE leaf_kind = 'history') = 1
);
SELECT fixture_check(
    'probe, fresh install: the static forever leaf is conformant; no heartbeat leaf exists',
    (SELECT conformant FROM probe_fresh WHERE leaf_name = 'horsies_task_history_forever_open')
    AND NOT EXISTS (SELECT 1 FROM probe_fresh WHERE leaf_kind = 'heartbeat' AND conformant)
);
SELECT fixture_check(
    'probe, fresh install: the forever leaf has OPEN lower and upper bounds and no anchors',
    (SELECT lower_bound_kind = 'OPEN' AND lower_anchor IS NULL AND upper_bound_kind = 'OPEN' AND upper_anchor IS NULL
     FROM probe_fresh WHERE leaf_name = 'horsies_task_history_forever_open')
);
SELECT fixture_check(
    'probe, fresh install: every heartbeat row has AT bounds one hour apart',
    (SELECT bool_and(lower_bound_kind = 'AT' AND upper_bound_kind = 'AT'
                     AND upper_anchor = lower_anchor + interval '1 hour')
     FROM probe_fresh WHERE leaf_kind = 'heartbeat')
);
SELECT fixture_check(
    'probe, fresh install: registration and publication are unhealthy, no leaf is absent',
    NOT bool_or(registration_healthy) AND NOT bool_or(publication_healthy) AND bool_and(absent_leaves = '{}')
) FROM probe_fresh;
SELECT fixture_check(
    'probe, fresh install: heartbeat rows are the UTC hours from now, named in UTC',
    (SELECT array_agg(leaf_name ORDER BY lower_anchor) FROM probe_fresh WHERE leaf_kind = 'heartbeat')
    = ARRAY(
        SELECT 'horsies_heartbeats_' || to_char((date_trunc('hour', now(), 'UTC') + h * interval '1 hour')
                                                 AT TIME ZONE 'UTC', 'YYYY_MM_DD_HH24')
        FROM generate_series(0, 6) AS h ORDER BY h
    )
);
SELECT fixture_expect_error('probe: a history horizon of 1 day is refused',
    'SELECT * FROM horsies_coverage_probe(''{}'', 1::smallint, 6::smallint)', '22023');
SELECT fixture_expect_error('probe: a history horizon of 15 days is refused',
    'SELECT * FROM horsies_coverage_probe(''{}'', 15::smallint, 6::smallint)', '22023');
SELECT fixture_expect_error('probe: a NULL history horizon is refused',
    'SELECT * FROM horsies_coverage_probe(''{}'', NULL, 6::smallint)', '22023');
SELECT fixture_expect_error('probe: a heartbeat horizon of 1 hour is refused',
    'SELECT * FROM horsies_coverage_probe(''{}'', 7::smallint, 1::smallint)', '22023');
SELECT fixture_expect_error('probe: a heartbeat horizon of 49 hours is refused',
    'SELECT * FROM horsies_coverage_probe(''{}'', 7::smallint, 49::smallint)', '22023');
SELECT fixture_expect_error('probe: an illegal declared day is refused',
    'SELECT * FROM horsies_coverage_probe(''{0}'', 7::smallint, 6::smallint)', '22023');

------------------------------------------------------------------------------------------------------------------------
-- Publication on a fresh install (the forever leaf only)
------------------------------------------------------------------------------------------------------------------------

SELECT fixture_expect_error(
    'publication: no history parent resolves (the session cannot see the history tables): refused',
    $sql$DO $do$
    BEGIN
        ALTER TABLE horsies_task_history_forever RENAME TO fixture_hidden_parent;
        PERFORM 1 FROM horsies_publish_history_readers();
    END
    $do$$sql$,
    '42P01'
);
SELECT fixture_check(
    'publication: the refusal changed nothing (no manifest row, no provenance reader)',
    NOT EXISTS (SELECT 1 FROM horsies_task_lookup_manifest)
    AND to_regprocedure('horsies_task_provenance_staged(uuid)') IS NULL
    AND to_regclass('horsies_task_history_forever') IS NOT NULL
);

CREATE TEMP TABLE publication_first AS SELECT * FROM horsies_publish_history_readers();
SELECT fixture_check(
    'publication, fresh install: PUBLISHED with the forever leaf only',
    (SELECT outcome = 'PUBLISHED' AND probed_leaves = 1 AND absent_leaves = '{}' FROM publication_first)
    AND (SELECT array_agg(leaf_name || ':' || probe_position || ':' || lower_bound_kind)
         FROM horsies_task_lookup_manifest) = ARRAY['horsies_task_history_forever_open:0:OPEN']
);
SELECT fixture_check(
    'publication: the stamp is horsies:manifest:2:<sha256 of the manifest>, on all four readers',
    (SELECT manifest_stamp ~ '^horsies:manifest:2:[0-9a-f]{64}$' FROM publication_first)
    AND (SELECT bool_and(obj_description(to_regprocedure(s.signature), 'pg_proc')
                         = (SELECT manifest_stamp FROM publication_first))
         FROM horsies_history_reader_signatures() AS s)
    AND (SELECT count(*) FROM horsies_history_reader_signatures()) = 4
);
SELECT fixture_check(
    'publication: the readers are current right after it',
    horsies_history_readers_current()
);
SELECT set_config('fixture.versions_first', fixture_reader_versions(), true);
SELECT fixture_check(
    'publication: a second call is ALREADY_CURRENT and replaces no reader',
    (SELECT outcome FROM horsies_publish_history_readers()) = 'ALREADY_CURRENT'
    AND fixture_reader_versions() = current_setting('fixture.versions_first')
);
SELECT fixture_check(
    'publication: the provenance reader has one argument and plain columns with is_workflow_task, the fingerprint '
    'and sent_at',
    pg_get_function_identity_arguments(to_regprocedure('horsies_task_provenance_staged(uuid)')) = 'p_task_id uuid'
    AND pg_get_function_result(to_regprocedure('horsies_task_provenance_staged(uuid)'))
        = 'TABLE(task_id uuid, status text, terminal_at timestamp with time zone, terminalization_kind text, '
          'is_workflow_task boolean, command_fingerprint_version smallint, command_fingerprint bytea, '
          'sent_at timestamp with time zone)'
    AND (SELECT count(*) FROM pg_proc AS p WHERE p.proname = 'horsies_task_provenance_staged') = 1
    AND to_regprocedure('horsies_task_lookup_staged(uuid)') IS NULL
);
SELECT fixture_check(
    'publication: the result reader returns the location, the status, the stored result columns and the task name',
    pg_get_function_result(to_regprocedure('horsies_task_result_staged(uuid)'))
        = 'TABLE(location text, status text, failed_reason text, '
          'result_envelope_version smallint, result_payload bytea, result_digest bytea, task_name text)'
);
SELECT fixture_check(
    'publication: every reader is STABLE, STRICT, with enable_seqscan off',
    (SELECT bool_and(p.provolatile = 's' AND p.proisstrict AND p.proconfig = '{enable_seqscan=off}')
     FROM horsies_history_reader_signatures() AS s JOIN pg_proc AS p ON p.oid = to_regprocedure(s.signature))
);
SELECT fixture_expect_error('render: an unknown reader is refused',
    'SELECT horsies_render_history_reader(''LOOKUP'')', '22023');

------------------------------------------------------------------------------------------------------------------------
-- Retention class registration
------------------------------------------------------------------------------------------------------------------------

CREATE TEMP TABLE registration_first AS SELECT * FROM horsies_register_retention_classes('{7}', 6::smallint);
SELECT fixture_check(
    'registration: a fresh install registers standard_7d, standard_30d and the heartbeat class',
    (SELECT array_agg(class_key || ':' || class_kind || ':' || outcome ORDER BY class_kind DESC, class_key DESC)
     FROM registration_first)
    = ARRAY['standard_7d:PERIOD:REGISTERED', 'standard_30d:PERIOD:REGISTERED', 'heartbeats:HEARTBEAT:REGISTERED']
    AND (SELECT bool_and(previous_heartbeat_horizon_hours IS NULL) FROM registration_first)
);
SELECT fixture_check(
    'registration: each period class row and its RANGE parent on terminal_at exist',
    (SELECT count(*) FROM horsies_retention_classes
     WHERE (class_key, class_kind, retention_days) IN (('standard_7d', 'PERIOD', 7), ('standard_30d', 'PERIOD', 30)))
       = 2
    AND (SELECT bool_and(
            c.relkind = 'p'
            AND pg_get_expr(c.relpartbound, c.oid) = format('FOR VALUES IN (%L)', k.key)
            AND pg_get_partkeydef(c.oid) = 'RANGE (terminal_at)'
            AND EXISTS (SELECT 1 FROM pg_inherits AS i
                        WHERE i.inhrelid = c.oid AND i.inhparent = 'horsies_task_history'::regclass))
         FROM (VALUES ('standard_7d'), ('standard_30d')) AS k(key)
         JOIN pg_class AS c ON c.oid = to_regclass('horsies_task_history_' || k.key))
    AND (SELECT heartbeat_horizon_hours FROM horsies_retention_classes WHERE class_key = 'heartbeats') = 6
);
SELECT fixture_check(
    'registration: a second call verifies every class and changes nothing',
    (SELECT array_agg(outcome ORDER BY class_kind DESC, class_key DESC)
     FROM horsies_register_retention_classes('{7}', 6::smallint)) = ARRAY['VERIFIED', 'VERIFIED', 'VERIFIED']
    AND (SELECT count(*) FROM horsies_retention_classes) = 4
);
CREATE TEMP TABLE registration_horizon AS SELECT * FROM horsies_register_retention_classes('{7}', 9::smallint);
SELECT fixture_check(
    'registration: another heartbeat horizon updates the row and names the previous value',
    (SELECT outcome || ':' || previous_heartbeat_horizon_hours FROM registration_horizon WHERE class_kind = 'HEARTBEAT')
        = 'HORIZON_UPDATED:6'
    AND (SELECT heartbeat_horizon_hours FROM horsies_retention_classes WHERE class_key = 'heartbeats') = 9
);
SELECT fixture_check(
    'registration: a new declared period is REGISTERED, the others VERIFIED',
    (SELECT array_agg(class_key || ':' || outcome ORDER BY class_kind DESC, class_key DESC)
     FROM horsies_register_retention_classes('{7,1}', 6::smallint))
    = ARRAY['standard_7d:VERIFIED', 'standard_30d:VERIFIED', 'standard_1d:REGISTERED', 'heartbeats:HORIZON_UPDATED']
);
SELECT fixture_expect_error('registration: a heartbeat horizon of 1 hour is refused',
    'SELECT * FROM horsies_register_retention_classes(''{2}'', 1::smallint)', '22023');
SELECT fixture_expect_error('registration: a heartbeat horizon of 49 hours is refused',
    'SELECT * FROM horsies_register_retention_classes(''{2}'', 49::smallint)', '22023');
SELECT fixture_expect_error('registration: a NULL heartbeat horizon is refused',
    'SELECT * FROM horsies_register_retention_classes(''{2}'', NULL)', '22023');
SELECT fixture_expect_error('registration: an illegal period is refused',
    'SELECT * FROM horsies_register_retention_classes(''{2,1826}'', 6::smallint)', '22023');
SELECT fixture_check(
    'registration: a refused call registered nothing',
    to_regclass('horsies_task_history_standard_2d') IS NULL
    AND NOT EXISTS (SELECT 1 FROM horsies_retention_classes WHERE class_key = 'standard_2d')
);

------------------------------------------------------------------------------------------------------------------------
-- History leaf creation
------------------------------------------------------------------------------------------------------------------------

SELECT set_config('fixture.manifest_before', (SELECT string_agg(leaf_name, ',' ORDER BY probe_position)
                                              FROM horsies_task_lookup_manifest), true);
SELECT fixture_check(
    'create history leaf: today''s leaf of standard_7d is CREATED',
    fixture_create('standard_7d', fixture_today()) = 'CREATED'
);
SELECT fixture_check(
    'create history leaf: the catalog row holds the class, the UTC day bounds and index shape version 1',
    EXISTS (
        SELECT 1 FROM horsies_task_history_leaf_catalog AS c
        WHERE c.leaf_name = 'horsies_task_history_standard_7d_' || to_char(fixture_today() AT TIME ZONE 'UTC', 'YYYY_MM_DD')
          AND c.class_key = 'standard_7d' AND c.lower_bound_kind = 'AT' AND c.lower_anchor = fixture_today()
          AND c.upper_anchor = fixture_today() + interval '1 day' AND c.index_schema_version = 1
          AND c.created_at IS NOT NULL AND c.detached_at IS NULL AND c.dropped_at IS NULL
    )
);
SELECT fixture_check(
    'create history leaf: the leaf is attached with its bound, both indexes conformant, and analyzed',
    (SELECT state FROM horsies_history_leaf_state(
        'HISTORY', 'standard_7d', 'horsies_task_history_standard_7d',
        horsies_history_leaf_name('standard_7d', fixture_today()), 'AT', fixture_today(),
        fixture_today() + interval '1 day')) = 'CONFORMANT'
    AND (SELECT reltuples = 0 FROM pg_class WHERE oid = to_regclass(horsies_history_leaf_name('standard_7d', fixture_today())))
);
SELECT fixture_check(
    'create history leaf: the readers are republished in the same call and probe the new leaf',
    horsies_history_readers_current()
    AND EXISTS (SELECT 1 FROM horsies_task_lookup_manifest
                WHERE leaf_name = horsies_history_leaf_name('standard_7d', fixture_today()))
    AND fixture_reader_names('horsies_task_history_duplicate_staged(uuid)',
                             horsies_history_leaf_name('standard_7d', fixture_today()))
);
SELECT set_config('fixture.outcome', fixture_create('standard_7d', fixture_today() + interval '3 days'), true);
SELECT fixture_check(
    'create history leaf: a future leaf is published in its own call too',
    current_setting('fixture.outcome') = 'CREATED'
    AND horsies_history_readers_current()
    AND EXISTS (SELECT 1 FROM horsies_task_lookup_manifest
                WHERE leaf_name = horsies_history_leaf_name('standard_7d', fixture_today() + interval '3 days'))
);
SELECT set_config('fixture.created_at', (SELECT created_at::text FROM horsies_task_history_leaf_catalog
    WHERE leaf_name = horsies_history_leaf_name('standard_7d', fixture_today())), true);
SELECT set_config('fixture.versions_before', fixture_reader_versions(), true);
SELECT fixture_check(
    'create history leaf: a conformant leaf is ALREADY_CONFORMANT; nothing changes',
    fixture_create('standard_7d', fixture_today()) = 'ALREADY_CONFORMANT'
    AND (SELECT created_at::text FROM horsies_task_history_leaf_catalog
         WHERE leaf_name = horsies_history_leaf_name('standard_7d', fixture_today())) = current_setting('fixture.created_at')
    AND fixture_reader_versions() = current_setting('fixture.versions_before')
);

-- Index repair. Each damage alone, and the repaired definition read with pg_get_indexdef, independent of the
-- function's own shape check.
SELECT fixture_create('standard_7d', fixture_today() + interval '1 day');
SELECT set_config('fixture.leaf_1', horsies_history_leaf_name('standard_7d', fixture_today() + interval '1 day'), true);
DO $$
BEGIN
    EXECUTE format('DROP INDEX %I', current_setting('fixture.leaf_1') || '_task_idx');
    EXECUTE format('CREATE INDEX %I ON %I USING btree (enqueued_at)', current_setting('fixture.leaf_1') || '_task_idx',
                   current_setting('fixture.leaf_1'));
END
$$;
SELECT fixture_check(
    'leaf state: an index name of the leaf on the wrong column is INDEX_DAMAGED',
    (SELECT state FROM horsies_history_leaf_state(
        'HISTORY', 'standard_7d', 'horsies_task_history_standard_7d', current_setting('fixture.leaf_1'), 'AT',
        fixture_today() + interval '1 day', fixture_today() + interval '2 days')) = 'INDEX_DAMAGED'
    AND (SELECT array_agg(index_name || ':' || holder || ':' || conformant::text ORDER BY index_name)
         FROM horsies_history_leaf_indexes('HISTORY', current_setting('fixture.leaf_1')))
        = ARRAY[current_setting('fixture.leaf_1') || '_enqueued_idx:LEAF:true',
                current_setting('fixture.leaf_1') || '_task_idx:LEAF:false']
);
SELECT set_config('fixture.outcome', fixture_create('standard_7d', fixture_today() + interval '1 day'), true);
SELECT fixture_check(
    'create history leaf: an index on the wrong column is INDEX_REPAIRED to (task_id)',
    current_setting('fixture.outcome') = 'INDEX_REPAIRED'
    AND pg_get_indexdef(to_regclass(current_setting('fixture.leaf_1') || '_task_idx')) LIKE '% USING btree (task_id)'
    AND fixture_create('standard_7d', fixture_today() + interval '1 day') = 'ALREADY_CONFORMANT'
);
DO $$
BEGIN
    EXECUTE format('DROP INDEX %I', current_setting('fixture.leaf_1') || '_enqueued_idx');
END
$$;
SELECT set_config('fixture.outcome', fixture_create('standard_7d', fixture_today() + interval '1 day'), true);
SELECT fixture_check(
    'create history leaf: a missing ordering index is INDEX_REPAIRED to (enqueued_at)',
    current_setting('fixture.outcome') = 'INDEX_REPAIRED'
    AND pg_get_indexdef(to_regclass(current_setting('fixture.leaf_1') || '_enqueued_idx'))
        LIKE '% USING btree (enqueued_at)'
    AND (SELECT array_agg(index_name || ':' || holder || ':' || conformant::text ORDER BY index_name)
         FROM horsies_history_leaf_indexes('HISTORY', current_setting('fixture.leaf_1')))
        = ARRAY[current_setting('fixture.leaf_1') || '_enqueued_idx:LEAF:true',
                current_setting('fixture.leaf_1') || '_task_idx:LEAF:true']
);
SELECT fixture_check(
    'create history leaf: the repair leaves no temporary index behind',
    NOT EXISTS (SELECT 1 FROM pg_class WHERE relname LIKE 'horsies\_index\_repair\_%')
);

-- Catalog conflicts: each is reported, never repaired.
CREATE TABLE horsies_task_history_standard_7d_2001_01_01 (x integer);
SELECT fixture_check(
    'create history leaf: a relation with the leaf name and no catalog row is RELATION_WITHOUT_CATALOG',
    fixture_create('standard_7d', '2001-01-01 00:00:00+00') = 'CATALOG_CONFLICT/RELATION_WITHOUT_CATALOG'
    AND NOT EXISTS (SELECT 1 FROM horsies_task_history_leaf_catalog
                    WHERE leaf_name = 'horsies_task_history_standard_7d_2001_01_01')
);
CREATE TABLE fixture_other_table (task_id uuid);
CREATE INDEX horsies_task_history_standard_7d_2001_01_02_task_idx ON fixture_other_table (task_id);
SELECT fixture_check(
    'create history leaf: a canonical index name held by another table is FOREIGN_INDEX; nothing is created',
    fixture_create('standard_7d', '2001-01-02 00:00:00+00') = 'CATALOG_CONFLICT/FOREIGN_INDEX'
    AND to_regclass('horsies_task_history_standard_7d_2001_01_02') IS NULL
);
SELECT fixture_create('standard_7d', '2001-01-03 00:00:00+00');
UPDATE horsies_task_history_leaf_catalog SET index_schema_version = 2
WHERE leaf_name = 'horsies_task_history_standard_7d_2001_01_03';
SELECT fixture_check(
    'create history leaf: a catalog row with other metadata is METADATA_MISMATCH',
    fixture_create('standard_7d', '2001-01-03 00:00:00+00') = 'CATALOG_CONFLICT/METADATA_MISMATCH'
);
SELECT fixture_create('standard_7d', '2001-01-04 00:00:00+00');
DROP TABLE horsies_task_history_standard_7d_2001_01_04;
SELECT fixture_check(
    'create history leaf: a cataloged leaf whose relation is gone is CATALOG_WITHOUT_RELATION',
    fixture_create('standard_7d', '2001-01-04 00:00:00+00') = 'CATALOG_CONFLICT/CATALOG_WITHOUT_RELATION'
    AND to_regclass('horsies_task_history_standard_7d_2001_01_04') IS NULL
);
SELECT fixture_create('standard_7d', '2001-01-05 00:00:00+00');
ALTER TABLE horsies_task_history_standard_7d DETACH PARTITION horsies_task_history_standard_7d_2001_01_05;
SELECT fixture_check(
    'create history leaf: a leaf detached behind the catalog is PHYSICAL_NONCONFORMANT',
    fixture_create('standard_7d', '2001-01-05 00:00:00+00') = 'CATALOG_CONFLICT/PHYSICAL_NONCONFORMANT'
);
SELECT fixture_create('standard_7d', '2001-01-07 00:00:00+00');
ALTER TABLE horsies_task_history_standard_7d DETACH PARTITION horsies_task_history_standard_7d_2001_01_07;
ALTER TABLE horsies_task_history_standard_7d ATTACH PARTITION horsies_task_history_standard_7d_2001_01_07
    FOR VALUES FROM ('2001-01-07 00:00:00+00') TO ('2001-01-09 00:00:00+00');
SELECT fixture_check(
    'create history leaf: an attached leaf whose partition bound differs from its catalog row is PHYSICAL_NONCONFORMANT',
    fixture_create('standard_7d', '2001-01-07 00:00:00+00') = 'CATALOG_CONFLICT/PHYSICAL_NONCONFORMANT'
);
SELECT fixture_create('standard_7d', '2001-01-06 00:00:00+00');
DROP INDEX horsies_task_history_standard_7d_2001_01_06_enqueued_idx;
CREATE INDEX horsies_task_history_standard_7d_2001_01_06_enqueued_idx ON fixture_other_table (task_id);
SELECT fixture_check(
    'create history leaf: an index name of the leaf held by another table is FOREIGN_INDEX, not repaired',
    fixture_create('standard_7d', '2001-01-06 00:00:00+00') = 'CATALOG_CONFLICT/FOREIGN_INDEX'
    AND (SELECT indrelid = 'fixture_other_table'::regclass FROM pg_index
         WHERE indexrelid = 'horsies_task_history_standard_7d_2001_01_06_enqueued_idx'::regclass)
);
-- Remove the conflict fixtures, so that the probe cases below see only leaves that the functions made.
DELETE FROM horsies_task_history_leaf_catalog WHERE leaf_name LIKE 'horsies\_task\_history\_standard\_7d\_2001\_%';
DROP TABLE horsies_task_history_standard_7d_2001_01_01, horsies_task_history_standard_7d_2001_01_03,
    horsies_task_history_standard_7d_2001_01_05, horsies_task_history_standard_7d_2001_01_06,
    horsies_task_history_standard_7d_2001_01_07, fixture_other_table;
SELECT outcome FROM horsies_publish_history_readers();

INSERT INTO horsies_retention_classes (class_key, class_kind, retention_days, heartbeat_horizon_hours, created_at)
VALUES ('standard_3d', 'PERIOD', 3, NULL, now());
SELECT fixture_check(
    'create history leaf: a class row without its parent is PARENT_MISSING',
    fixture_create('standard_3d', fixture_today()) = 'CATALOG_CONFLICT/PARENT_MISSING'
);
DELETE FROM horsies_retention_classes WHERE class_key = 'standard_3d';
SELECT fixture_check(
    'create history leaf: an unknown class is CLASS_ABSENT',
    fixture_create('standard_4d', fixture_today()) = 'CLASS_ABSENT'
);
SELECT fixture_expect_error('create history leaf: a NULL class key is refused',
    'SELECT * FROM horsies_create_history_leaf(NULL, now())', '22023');
SELECT fixture_expect_error('create history leaf: a NULL anchor of a period class is refused',
    'SELECT * FROM horsies_create_history_leaf(''standard_7d'', NULL)', '22023');
SELECT fixture_expect_error('create history leaf: an anchor that is not a UTC midnight is refused',
    $sql$SELECT * FROM horsies_create_history_leaf('standard_7d', '2026-01-01 00:00:00+05:30')$sql$, '22023');
SELECT fixture_expect_error('create history leaf: the heartbeat class is refused',
    'SELECT * FROM horsies_create_history_leaf(''heartbeats'', date_trunc(''hour'', now(), ''UTC''))', '22023');
SELECT fixture_expect_error('create history leaf: a forever day that is not a UTC midnight is refused',
    $sql$SELECT * FROM horsies_create_history_leaf('forever', '2026-01-01 12:00:00+00')$sql$, '22023');

-- The forever class: the static leaf is verified, never created or repaired.
SELECT fixture_check(
    'create history leaf: forever with the open bound verifies the static leaf: ALREADY_CONFORMANT',
    (SELECT outcome || ':' || leaf_name FROM horsies_create_history_leaf('forever', NULL))
        = 'ALREADY_CONFORMANT:horsies_task_history_forever_open'
);
SELECT fixture_check(
    'create history leaf: a forever day is COVERED_BY_OPEN_ENDED and creates nothing',
    fixture_create('forever', fixture_today()) = 'COVERED_BY_OPEN_ENDED'
    AND to_regclass(horsies_history_leaf_name('forever', fixture_today())) IS NULL
);
DROP INDEX horsies_task_history_forever_open_enqueued_idx;
SELECT fixture_check(
    'create history leaf: a damaged static forever leaf is a conflict for an operator, not repaired',
    fixture_create('forever', NULL) = 'CATALOG_CONFLICT/PHYSICAL_NONCONFORMANT'
    AND fixture_create('forever', fixture_today()) = 'CATALOG_CONFLICT/PHYSICAL_NONCONFORMANT'
    AND to_regclass('horsies_task_history_forever_open_enqueued_idx') IS NULL
);
CREATE INDEX horsies_task_history_forever_open_enqueued_idx ON horsies_task_history_forever_open USING btree (enqueued_at);

------------------------------------------------------------------------------------------------------------------------
-- Heartbeat leaf creation
------------------------------------------------------------------------------------------------------------------------

SELECT set_config('fixture.hour', date_trunc('hour', now(), 'UTC')::text, true);
SELECT set_config('fixture.manifest_before', (SELECT string_agg(leaf_name, ',' ORDER BY probe_position)
                                              FROM horsies_task_lookup_manifest), true);
SELECT set_config('fixture.outcome', fixture_create_heartbeat(current_setting('fixture.hour')::timestamptz), true);
SELECT fixture_check(
    'create heartbeat leaf: the current hour is CREATED with its catalog row and probe index',
    current_setting('fixture.outcome') = 'CREATED'
    AND EXISTS (
        SELECT 1 FROM horsies_task_history_leaf_catalog AS c
        WHERE c.leaf_name = horsies_heartbeat_leaf_name(current_setting('fixture.hour')::timestamptz)
          AND c.class_key = 'heartbeats' AND c.lower_bound_kind = 'AT'
          AND c.lower_anchor = current_setting('fixture.hour')::timestamptz
          AND c.upper_anchor = current_setting('fixture.hour')::timestamptz + interval '1 hour'
    )
    AND (SELECT array_agg(index_name || ':' || holder || ':' || conformant::text)
         FROM horsies_history_leaf_indexes('HEARTBEAT',
                                           horsies_heartbeat_leaf_name(current_setting('fixture.hour')::timestamptz)))
        = ARRAY[horsies_heartbeat_leaf_name(current_setting('fixture.hour')::timestamptz) || '_probe_idx:LEAF:true']
);
SELECT fixture_check(
    'create heartbeat leaf: a heartbeat leaf is neither published nor analyzed',
    (SELECT string_agg(leaf_name, ',' ORDER BY probe_position) FROM horsies_task_lookup_manifest)
        = current_setting('fixture.manifest_before')
    AND horsies_history_readers_current()
    AND (SELECT reltuples = -1 FROM pg_class
         WHERE oid = to_regclass(horsies_heartbeat_leaf_name(current_setting('fixture.hour')::timestamptz)))
);
SELECT fixture_check(
    'create heartbeat leaf: a conformant leaf is ALREADY_CONFORMANT',
    fixture_create_heartbeat(current_setting('fixture.hour')::timestamptz) = 'ALREADY_CONFORMANT'
);
DO $$
BEGIN
    EXECUTE format('DROP INDEX %I', horsies_heartbeat_leaf_name(current_setting('fixture.hour')::timestamptz) || '_probe_idx');
    EXECUTE format('CREATE INDEX %I ON %I (task_id, role varchar_pattern_ops, sent_at DESC)',
                   horsies_heartbeat_leaf_name(current_setting('fixture.hour')::timestamptz) || '_probe_idx',
                   horsies_heartbeat_leaf_name(current_setting('fixture.hour')::timestamptz));
END
$$;
SELECT fixture_check(
    'leaf indexes: a key column with another operator class than its type''s default is not conformant',
    (SELECT array_agg(index_name || ':' || holder || ':' || conformant::text)
     FROM horsies_history_leaf_indexes('HEARTBEAT',
                                       horsies_heartbeat_leaf_name(current_setting('fixture.hour')::timestamptz)))
        = ARRAY[horsies_heartbeat_leaf_name(current_setting('fixture.hour')::timestamptz) || '_probe_idx:LEAF:false']
);
SELECT set_config('fixture.outcome', fixture_create_heartbeat(current_setting('fixture.hour')::timestamptz), true);
SELECT fixture_check(
    'create heartbeat leaf: a probe index with another operator class is INDEX_REPAIRED',
    current_setting('fixture.outcome') = 'INDEX_REPAIRED'
    AND pg_get_indexdef(to_regclass(horsies_heartbeat_leaf_name(current_setting('fixture.hour')::timestamptz)
                                    || '_probe_idx')) LIKE '% USING btree (task_id, role, sent_at DESC)'
    AND fixture_create_heartbeat(current_setting('fixture.hour')::timestamptz) = 'ALREADY_CONFORMANT'
);
SELECT fixture_create_heartbeat(current_setting('fixture.hour')::timestamptz + interval '1 hour');
DO $$
BEGIN
    EXECUTE format('DROP INDEX %I',
        horsies_heartbeat_leaf_name(current_setting('fixture.hour')::timestamptz + interval '1 hour') || '_probe_idx');
END
$$;
SELECT fixture_check(
    'create heartbeat leaf: a missing probe index is INDEX_REPAIRED',
    fixture_create_heartbeat(current_setting('fixture.hour')::timestamptz + interval '1 hour') = 'INDEX_REPAIRED'
    AND fixture_create_heartbeat(current_setting('fixture.hour')::timestamptz + interval '1 hour') = 'ALREADY_CONFORMANT'
);
DO $$
BEGIN
    EXECUTE format('DROP INDEX %I',
        horsies_heartbeat_leaf_name(current_setting('fixture.hour')::timestamptz + interval '1 hour') || '_probe_idx');
    EXECUTE format('CREATE INDEX %I ON %I USING btree (task_id, role, sent_at)',
        horsies_heartbeat_leaf_name(current_setting('fixture.hour')::timestamptz + interval '1 hour') || '_probe_idx',
        horsies_heartbeat_leaf_name(current_setting('fixture.hour')::timestamptz + interval '1 hour'));
END
$$;
SELECT set_config('fixture.outcome',
    fixture_create_heartbeat(current_setting('fixture.hour')::timestamptz + interval '1 hour'), true);
SELECT fixture_check(
    'create heartbeat leaf: a probe index with sent_at ascending is INDEX_REPAIRED to sent_at DESC',
    current_setting('fixture.outcome') = 'INDEX_REPAIRED'
    AND pg_get_indexdef(to_regclass(
            horsies_heartbeat_leaf_name(current_setting('fixture.hour')::timestamptz + interval '1 hour') || '_probe_idx'))
        LIKE '% USING btree (task_id, role, sent_at DESC)'
);
SELECT fixture_expect_error('create heartbeat leaf: a NULL anchor is refused',
    'SELECT * FROM horsies_create_heartbeat_leaf(NULL)', '22023');
SELECT fixture_expect_error('create heartbeat leaf: an anchor that is not a UTC hour start is refused',
    $sql$SELECT * FROM horsies_create_heartbeat_leaf('2026-01-01 10:30:00+00')$sql$, '22023');
SELECT fixture_expect_error(
    'create heartbeat leaf: without the heartbeat class the answer is CLASS_ABSENT',
    $sql$DO $do$
    BEGIN
        DELETE FROM horsies_task_history_leaf_catalog WHERE class_key = 'heartbeats';
        DELETE FROM horsies_retention_classes WHERE class_key = 'heartbeats';
        IF (SELECT outcome FROM horsies_create_heartbeat_leaf(date_trunc('hour', now(), 'UTC') + interval '5 hours'))
           = 'CLASS_ABSENT' THEN
            RAISE EXCEPTION 'as expected' USING ERRCODE = 'HX001';
        END IF;
    END
    $do$$sql$,
    'HX001'
);

------------------------------------------------------------------------------------------------------------------------
-- The probe of a complete pass
------------------------------------------------------------------------------------------------------------------------

-- Register and create everything a pass with periods {7} and default horizons (7 days, 6 hours) expects.
SELECT * FROM horsies_register_retention_classes('{7}', 6::smallint);
CREATE TEMP TABLE pass_outcomes AS
SELECT p.leaf_name, CASE p.leaf_kind
        WHEN 'heartbeat' THEN fixture_create_heartbeat(p.lower_anchor)
        ELSE fixture_create(p.class_key, p.lower_anchor)
    END AS outcome
FROM horsies_coverage_probe('{7}', 7::smallint, 6::smallint) AS p
WHERE NOT p.conformant;
SELECT fixture_check(
    'probe-driven pass: every damaged leaf of the probe is CREATED',
    (SELECT bool_and(outcome = 'CREATED') AND count(*) > 0 FROM pass_outcomes)
);
CREATE TEMP TABLE probe_healthy AS SELECT * FROM horsies_coverage_probe('{7}', 7::smallint, 6::smallint);
SELECT fixture_check(
    'probe, after the pass: healthy (registration, publication, every leaf conformant, nothing absent)',
    (SELECT bool_and(conformant AND registration_healthy AND publication_healthy AND absent_leaves = '{}')
     FROM probe_healthy)
);
SELECT fixture_check(
    'probe, after the pass: rows are period classes x 8 days, the forever leaf, and 7 heartbeat hours',
    (SELECT count(*) FROM probe_healthy)
        = (SELECT count(*) FROM horsies_retention_classes WHERE class_kind = 'PERIOD') * 8 + 1 + 7
    AND (SELECT count(DISTINCT checked_at) FROM probe_healthy) = 1
);
SELECT fixture_check(
    'probe, after the pass: history rows of a class are the UTC days from today, named in UTC',
    (SELECT array_agg(leaf_name ORDER BY lower_anchor) FROM probe_healthy WHERE class_key = 'standard_7d')
    = ARRAY(SELECT horsies_history_leaf_name('standard_7d', fixture_today() + d * interval '1 day')
            FROM generate_series(0, 7) AS d ORDER BY d)
    AND (SELECT bool_and(parent_name = 'horsies_task_history_standard_7d' AND lower_bound_kind = 'AT'
                         AND upper_bound_kind = 'AT' AND upper_anchor = lower_anchor + interval '1 day')
         FROM probe_healthy WHERE class_key = 'standard_7d')
);
SET TimeZone = 'Pacific/Chatham';
CREATE TEMP TABLE probe_other_zone AS SELECT * FROM horsies_coverage_probe('{7}', 7::smallint, 6::smallint);
SET TimeZone = 'Asia/Kolkata';
SELECT fixture_check(
    'probe: the rows do not depend on the session time zone',
    NOT EXISTS (
        SELECT leaf_kind, class_key, leaf_name, lower_anchor, upper_anchor, conformant FROM probe_healthy
        EXCEPT
        SELECT leaf_kind, class_key, leaf_name, lower_anchor, upper_anchor, conformant FROM probe_other_zone
    )
    AND (SELECT count(*) FROM probe_other_zone) = (SELECT count(*) FROM probe_healthy)
);
SELECT fixture_check(
    'probe: another heartbeat horizon than the stored one is registration_healthy false',
    NOT (SELECT bool_or(registration_healthy) FROM horsies_coverage_probe('{7}', 7::smallint, 8::smallint))
);
SELECT fixture_check(
    'probe: a declared period without its class is registration_healthy false',
    NOT (SELECT bool_or(registration_healthy) FROM horsies_coverage_probe('{7,90}', 7::smallint, 6::smallint))
);
COMMENT ON FUNCTION horsies_task_result_staged(uuid) IS 'horsies:manifest:written-by-another-renderer';
SELECT fixture_check(
    'probe: a reader with another stamp is publication_healthy false',
    NOT (SELECT bool_or(publication_healthy) FROM horsies_coverage_probe('{7}', 7::smallint, 6::smallint))
);
SELECT set_config('fixture.outcome', (SELECT outcome FROM horsies_publish_history_readers()), true);
SELECT fixture_check(
    'publication: a stale stamp makes the next call PUBLISHED, and the probe is healthy again',
    current_setting('fixture.outcome') = 'PUBLISHED'
    AND (SELECT bool_and(publication_healthy) FROM horsies_coverage_probe('{7}', 7::smallint, 6::smallint))
);
DELETE FROM horsies_task_lookup_manifest WHERE probe_position = 1;
SELECT fixture_check(
    'probe: a manifest that differs from the catalog is publication_healthy false',
    NOT (SELECT bool_or(publication_healthy) FROM horsies_coverage_probe('{7}', 7::smallint, 6::smallint))
);
SELECT set_config('fixture.outcome', (SELECT outcome FROM horsies_publish_history_readers()), true);
SELECT fixture_check(
    'publication: a stale manifest makes the next call PUBLISHED',
    current_setting('fixture.outcome') = 'PUBLISHED' AND horsies_history_readers_current()
);
SELECT fixture_check(
    'publication: the manifest is every attached history leaf in lower anchor order, forever first, no heartbeat',
    (SELECT array_agg(leaf_name ORDER BY probe_position) FROM horsies_task_lookup_manifest)
    = (SELECT array_agg(c.leaf_name ORDER BY c.lower_anchor NULLS FIRST, c.leaf_name)
       FROM horsies_task_history_leaf_catalog AS c
       WHERE c.class_key <> 'heartbeats' AND c.detached_at IS NULL AND c.dropped_at IS NULL
         AND to_regclass(c.leaf_name) IS NOT NULL)
    AND (SELECT array_agg(probe_position ORDER BY probe_position) FROM horsies_task_lookup_manifest)
        = ARRAY(SELECT generate_series(0, (SELECT count(*)::integer - 1 FROM horsies_task_lookup_manifest)))
);
SELECT fixture_check(
    'publication: no reader names a partitioned table; each probes leaves, the live table or the clock exceptions',
    NOT EXISTS (
        SELECT 1
        FROM horsies_history_reader_signatures() AS s
        CROSS JOIN LATERAL regexp_matches(pg_get_functiondef(to_regprocedure(s.signature)),
                                          'FROM ([a-z0-9_]+)', 'g') AS m(target)
        JOIN pg_class AS c ON c.oid = to_regclass(m.target[1])
        WHERE c.relkind <> 'r'
    )
);

-- A leaf removed out of band: reported, left out of the readers, and the readers still run.
DO $$
BEGIN
    EXECUTE format('DROP TABLE %I', horsies_history_leaf_name('standard_7d', fixture_today() + interval '3 days'));
END
$$;
SELECT fixture_check(
    'probe: a leaf removed out of band is in absent_leaves, not conformant, publication unhealthy',
    (SELECT bool_and(absent_leaves = ARRAY[horsies_history_leaf_name('standard_7d', fixture_today() + interval '3 days')])
            AND NOT bool_or(publication_healthy)
     FROM horsies_coverage_probe('{7}', 7::smallint, 6::smallint))
    AND NOT (SELECT conformant FROM horsies_coverage_probe('{7}', 7::smallint, 6::smallint)
             WHERE leaf_name = horsies_history_leaf_name('standard_7d', fixture_today() + interval '3 days'))
);
CREATE TEMP TABLE publication_absent AS SELECT * FROM horsies_publish_history_readers();
SELECT fixture_check(
    'publication: a leaf removed out of band is left out of the manifest and the readers, and reported',
    (SELECT outcome = 'PUBLISHED'
            AND absent_leaves = ARRAY[horsies_history_leaf_name('standard_7d', fixture_today() + interval '3 days')]
     FROM publication_absent)
    AND NOT EXISTS (SELECT 1 FROM horsies_task_lookup_manifest
                    WHERE leaf_name = horsies_history_leaf_name('standard_7d', fixture_today() + interval '3 days'))
    AND NOT fixture_reader_names('horsies_task_history_duplicate_staged(uuid)',
                                 horsies_history_leaf_name('standard_7d', fixture_today() + interval '3 days'))
    AND horsies_task_history_duplicate_staged(gen_random_uuid()) = FALSE
);
SELECT fixture_check(
    'create history leaf: the leaf removed out of band is CATALOG_WITHOUT_RELATION, for an operator',
    fixture_create('standard_7d', fixture_today() + interval '3 days') = 'CATALOG_CONFLICT/CATALOG_WITHOUT_RELATION'
);

------------------------------------------------------------------------------------------------------------------------
-- The published readers
------------------------------------------------------------------------------------------------------------------------

CREATE TEMP TABLE ids (name text PRIMARY KEY, id uuid NOT NULL);
INSERT INTO ids VALUES
    ('moved', fixture_uuid7(now() - interval '1 minute')),
    ('workflow', fixture_uuid7(now() - interval '1 minute')),
    ('fast_clock', fixture_uuid7(now() + interval '2 days')),
    ('not_v7', gen_random_uuid()),
    ('forever', fixture_uuid7(now() - interval '1 minute')),
    ('live', fixture_uuid7(now() - interval '1 minute')),
    ('unknown', fixture_uuid7(now() - interval '1 minute')),
    ('fused', fixture_uuid7(now() - interval '1 minute')),
    ('claimed', fixture_uuid7(now() - interval '1 minute'));

SELECT fixture_task((SELECT id FROM ids WHERE name = 'moved'), 'RUNNING', 'standard_7d', FALSE);
SELECT fixture_move((SELECT id FROM ids WHERE name = 'moved'), 'FAILED', 'FAIL_RUNNING', '{"e":1}');
SELECT fixture_task((SELECT id FROM ids WHERE name = 'workflow'), 'RUNNING', 'standard_7d', TRUE);
SELECT fixture_move((SELECT id FROM ids WHERE name = 'workflow'), 'CANCELLED', 'CANCEL_ORPHAN', NULL);
SELECT fixture_task((SELECT id FROM ids WHERE name = 'fast_clock'), 'RUNNING', 'standard_7d', FALSE);
SELECT fixture_move((SELECT id FROM ids WHERE name = 'fast_clock'), 'FAILED', 'FAIL_RUNNING', '{"e":2}');
SELECT fixture_task((SELECT id FROM ids WHERE name = 'not_v7'), 'RUNNING', 'standard_7d', FALSE);
SELECT fixture_move((SELECT id FROM ids WHERE name = 'not_v7'), 'COMPLETED', 'COMPLETE_FUSED', '{"ok":1}');
SELECT fixture_task((SELECT id FROM ids WHERE name = 'forever'), 'RUNNING', 'forever', FALSE);
SELECT fixture_move((SELECT id FROM ids WHERE name = 'forever'), 'COMPLETED', 'COMPLETE_FUSED', '{"ok":2}');
SELECT fixture_task((SELECT id FROM ids WHERE name = 'live'), 'RUNNING', 'standard_7d', FALSE);

SELECT fixture_check(
    'moves through the published guard: the published guard lets a first move through',
    (SELECT count(*) FROM horsies_task_history WHERE task_id IN (SELECT id FROM ids)) = 5
);
SELECT fixture_check(
    'guard: TRUE for a moved id (birth-pruned probe), FALSE for an unknown id, NULL for NULL',
    horsies_task_history_duplicate_staged((SELECT id FROM ids WHERE name = 'moved'))
    AND horsies_task_history_duplicate_staged((SELECT id FROM ids WHERE name = 'forever'))
    AND NOT horsies_task_history_duplicate_staged((SELECT id FROM ids WHERE name = 'unknown'))
    AND NOT horsies_task_history_duplicate_staged((SELECT id FROM ids WHERE name = 'live'))
    AND horsies_task_history_duplicate_staged(NULL) IS NULL
);
SELECT fixture_check(
    'guard: an id that is not a UUIDv7 is found by the probe of every leaf',
    horsies_task_history_duplicate_staged((SELECT id FROM ids WHERE name = 'not_v7'))
);
SELECT fixture_check(
    'guard: a fast-clock id is found through its clock exception',
    horsies_task_history_duplicate_staged((SELECT id FROM ids WHERE name = 'fast_clock'))
    AND EXISTS (SELECT 1 FROM horsies_task_history_clock_exceptions
                WHERE task_id = (SELECT id FROM ids WHERE name = 'fast_clock'))
);
SELECT fixture_expect_error(
    'guard: it has no fallback probes; without the clock exception a fast-clock id is not found',
    $sql$DO $do$
    BEGIN
        DELETE FROM horsies_task_history_clock_exceptions WHERE task_id = (SELECT id FROM ids WHERE name = 'fast_clock');
        IF NOT horsies_task_history_duplicate_staged((SELECT id FROM ids WHERE name = 'fast_clock')) THEN
            RAISE EXCEPTION 'as expected' USING ERRCODE = 'HX001';
        END IF;
    END
    $do$$sql$,
    'HX001'
);
SELECT fixture_expect_error(
    'move through the published guard: a second move of one id is refused as data_corrupted',
    $sql$DO $do$
    BEGIN
        PERFORM fixture_task((SELECT id FROM ids WHERE name = 'moved'), 'RUNNING', 'standard_7d', FALSE);
        PERFORM fixture_move((SELECT id FROM ids WHERE name = 'moved'), 'FAILED', 'FAIL_RUNNING', '{"e":3}');
    END
    $do$$sql$,
    'XX001'
);

SELECT fixture_check(
    'provenance: one row with the terminal facts and is_workflow_task, for history only',
    (SELECT status || ':' || terminalization_kind || ':' || is_workflow_task::text || ':' || (terminal_at IS NOT NULL)::text
     FROM horsies_task_provenance_staged((SELECT id FROM ids WHERE name = 'moved'))) = 'FAILED:FAIL_RUNNING:false:true'
    AND (SELECT status || ':' || terminalization_kind || ':' || is_workflow_task::text
         FROM horsies_task_provenance_staged((SELECT id FROM ids WHERE name = 'workflow'))) = 'CANCELLED:CANCEL_ORPHAN:true'
);
SELECT fixture_check(
    'provenance: the fingerprint version, fingerprint and sent_at of the history row, from the same probe',
    (SELECT (p.command_fingerprint_version, p.command_fingerprint, p.sent_at)
            IS NOT DISTINCT FROM (h.command_fingerprint_version, h.command_fingerprint, h.sent_at)
            AND p.command_fingerprint IS NOT NULL
     FROM horsies_task_provenance_staged((SELECT id FROM ids WHERE name = 'moved')) AS p
     JOIN horsies_task_history AS h ON h.task_id = p.task_id)
    AND (SELECT p.command_fingerprint IS NULL AND p.command_fingerprint_version IS NULL AND p.sent_at IS NOT NULL
         FROM horsies_task_provenance_staged((SELECT id FROM ids WHERE name = 'workflow')) AS p)
);
SELECT fixture_check(
    'provenance: no live probe; a live id and an unknown id have no row',
    NOT EXISTS (SELECT 1 FROM horsies_task_provenance_staged((SELECT id FROM ids WHERE name = 'live')))
    AND NOT EXISTS (SELECT 1 FROM horsies_task_provenance_staged((SELECT id FROM ids WHERE name = 'unknown')))
);
SELECT fixture_check(
    'provenance: a fast-clock id is found by the fallback probes; a non-UUIDv7 id by the probe of every leaf',
    (SELECT count(*) FROM horsies_task_provenance_staged((SELECT id FROM ids WHERE name = 'fast_clock'))) = 1
    AND (SELECT terminalization_kind FROM horsies_task_provenance_staged((SELECT id FROM ids WHERE name = 'not_v7')))
        = 'COMPLETE_FUSED'
);
SELECT fixture_check(
    'detail: LIVE for a live id, HISTORY with the full row, no row for an unknown id',
    (SELECT location || ':' || (task_row IS NULL)::text
     FROM horsies_task_detail_staged((SELECT id FROM ids WHERE name = 'live'))) = 'LIVE:true'
    AND (SELECT location || ':' || (task_row).status || ':' || (task_row).retention_class_key
         FROM horsies_task_detail_staged((SELECT id FROM ids WHERE name = 'moved'))) = 'HISTORY:FAILED:standard_7d'
    AND (SELECT (task_row).task_id FROM horsies_task_detail_staged((SELECT id FROM ids WHERE name = 'fast_clock')))
        = (SELECT id FROM ids WHERE name = 'fast_clock')
    AND NOT EXISTS (SELECT 1 FROM horsies_task_detail_staged((SELECT id FROM ids WHERE name = 'unknown')))
);
SELECT fixture_check(
    'result: LIVE with status and task name only; HISTORY with the stored result; no row when absent',
    (SELECT location || ':' || status || ':' || task_name || ':' ||
            (failed_reason IS NULL AND result_envelope_version IS NULL
             AND result_payload IS NULL AND result_digest IS NULL)::text
     FROM horsies_task_result_staged((SELECT id FROM ids WHERE name = 'live'))) = 'LIVE:RUNNING:fixture.task:true'
    AND (SELECT location || ':' || status || ':' || failed_reason || ':' || result_envelope_version || ':'
                || convert_from(result_payload, 'UTF8') || ':' || (result_digest = sha256(result_payload))::text
         FROM horsies_task_result_staged((SELECT id FROM ids WHERE name = 'moved')))
        = 'HISTORY:FAILED:reason:1:{"e":1}:true'
    AND NOT EXISTS (SELECT 1 FROM horsies_task_result_staged((SELECT id FROM ids WHERE name = 'unknown')))
);
SELECT fixture_check(
    'readers: the detail reader serves the rerun read of 0006 and the monitoring detail of 0007',
    (SELECT location || ':' || status FROM horsies_rerun_source_read((SELECT id FROM ids WHERE name = 'moved'),
                                                                     'standard_7d')) = 'HISTORY:FAILED'
    AND (SELECT location FROM horsies_monitoring_task_detail((SELECT id FROM ids WHERE name = 'moved'), FALSE, FALSE))
        = 'HISTORY'
);

------------------------------------------------------------------------------------------------------------------------
-- Miss classification
------------------------------------------------------------------------------------------------------------------------

SELECT fixture_check(
    'miss classes: 15 kinds in 8 classes, as the Python equivalence classes',
    (SELECT count(*) FROM unnest(ARRAY[
        'COMPLETE_LOCKED', 'COMPLETE_FUSED', 'FAIL_RUNNING', 'FAIL_STALE', 'EXPIRE_CLAIMED', 'EXPIRE_PENDING',
        'CANCEL_ADMIN', 'CANCEL_ORPHAN', 'CANCEL_ORPHAN_SWEEP', 'PAUSE_ABANDON_CLAIM', 'PAUSE_ABANDON_CLAIM_BATCH',
        'PAUSE_ABANDON_WORKFLOW', 'WORKFLOW_CANCEL_CLAIM', 'WORKFLOW_CANCEL_CLAIM_BATCH', 'WORKFLOW_CANCEL_WORKFLOW'
    ]) AS k WHERE horsies_terminalization_family(k) IS NOT NULL) = 15
    AND (SELECT array_agg(DISTINCT horsies_terminalization_family(k) ORDER BY horsies_terminalization_family(k))
         FROM unnest(ARRAY[
            'COMPLETE_LOCKED', 'COMPLETE_FUSED', 'FAIL_RUNNING', 'FAIL_STALE', 'EXPIRE_CLAIMED', 'EXPIRE_PENDING',
            'CANCEL_ADMIN', 'CANCEL_ORPHAN', 'CANCEL_ORPHAN_SWEEP', 'PAUSE_ABANDON_CLAIM', 'PAUSE_ABANDON_CLAIM_BATCH',
            'PAUSE_ABANDON_WORKFLOW', 'WORKFLOW_CANCEL_CLAIM', 'WORKFLOW_CANCEL_CLAIM_BATCH', 'WORKFLOW_CANCEL_WORKFLOW'
         ]) AS k)
        = ARRAY['CANCEL_ADMIN', 'CANCEL_ORPHAN', 'COMPLETE', 'EXPIRE', 'FAIL_RUNNING', 'FAIL_STALE', 'PAUSE_ABANDON',
                'WORKFLOW_CANCEL']
    AND horsies_terminalization_family('COMPLETE_LOCKED') = horsies_terminalization_family('COMPLETE_FUSED')
    AND horsies_terminalization_family('EXPIRE_CLAIMED') = horsies_terminalization_family('EXPIRE_PENDING')
    AND horsies_terminalization_family('FAIL_RUNNING') <> horsies_terminalization_family('FAIL_STALE')
    AND horsies_terminalization_family('LEGACY_TERMINAL') IS NULL
);
SELECT fixture_check(
    'miss: no live row and no history row is TASK_ABSENT',
    fixture_classify((SELECT id FROM ids WHERE name = 'unknown'), 'COMPLETE_FUSED', 'OWNER', 'w1', NULL)
        = 'TASK_ABSENT'
);
SELECT fixture_check(
    'miss: history holds a kind of the same class: ALREADY_APPLIED, with the stored facts',
    fixture_classify((SELECT id FROM ids WHERE name = 'not_v7'), 'COMPLETE_LOCKED', 'OWNER', 'w1', NULL)
        = 'ALREADY_APPLIED'
    AND (SELECT terminalization_kind || ':' || observed_status || ':' || (terminal_at IS NOT NULL)::text
         FROM horsies_terminalization_miss((SELECT id FROM ids WHERE name = 'not_v7'), 'COMPLETE_FUSED', 'OWNER', 'w1',
                                       NULL))
        = 'COMPLETE_FUSED:COMPLETED:true'
    AND fixture_classify((SELECT id FROM ids WHERE name = 'workflow'), 'CANCEL_ORPHAN_SWEEP', 'NONE', NULL, NULL)
        = 'ALREADY_APPLIED'
);
SELECT fixture_check(
    'miss: history holds a kind of another class: SOURCE_STATE_CONFLICT FOREIGN_TERMINALIZATION',
    fixture_classify((SELECT id FROM ids WHERE name = 'moved'), 'FAIL_STALE', 'OWNER', 'w1', NULL)
        = 'SOURCE_STATE_CONFLICT/FOREIGN_TERMINALIZATION'
    AND fixture_classify((SELECT id FROM ids WHERE name = 'not_v7'), 'CANCEL_ADMIN', 'NONE', NULL, NULL)
        = 'SOURCE_STATE_CONFLICT/FOREIGN_TERMINALIZATION'
);
SELECT fixture_check(
    'miss: a live row claimed by another worker is LOST_CLAIM, with the observed claim',
    fixture_classify((SELECT id FROM ids WHERE name = 'live'), 'COMPLETE_FUSED', 'OWNER', 'w2', NULL) = 'LOST_CLAIM'
    AND (SELECT observed_worker_id || ':' || observed_status
         FROM horsies_terminalization_miss((SELECT id FROM ids WHERE name = 'live'), 'COMPLETE_FUSED', 'OWNER', 'w2',
                                       NULL))
        = 'w1:RUNNING'
);
SELECT fixture_check(
    'miss: the same worker with another claimed_at is LOST_CLAIM (fence OWNER_AND_GENERATION)',
    fixture_classify((SELECT id FROM ids WHERE name = 'live'), 'COMPLETE_FUSED', 'OWNER_AND_GENERATION', 'w1',
                     now() - interval '1 hour')
        = 'LOST_CLAIM'
);
SELECT fixture_check(
    'miss: the fence holds (OWNER_AND_GENERATION, OWNER, NONE): SOURCE_STATE_CONFLICT with the live state',
    fixture_classify((SELECT id FROM ids WHERE name = 'live'), 'COMPLETE_FUSED', 'OWNER_AND_GENERATION', 'w1',
                     (SELECT claimed_at FROM horsies_tasks WHERE id = (SELECT id FROM ids WHERE name = 'live')))
        = 'SOURCE_STATE_CONFLICT'
    AND fixture_classify((SELECT id FROM ids WHERE name = 'live'), 'COMPLETE_FUSED', 'OWNER', 'w1', NULL)
        = 'SOURCE_STATE_CONFLICT'
    AND fixture_classify((SELECT id FROM ids WHERE name = 'live'), 'EXPIRE_PENDING', 'NONE', NULL, NULL)
        = 'SOURCE_STATE_CONFLICT'
);
SELECT fixture_expect_error('miss: a NULL task id is refused',
    'SELECT * FROM horsies_terminalization_miss(NULL, ''COMPLETE_FUSED'', ''NONE'', NULL, NULL)', '22023');
SELECT fixture_expect_error('miss: a kind outside the 15 kinds is refused',
    $sql$SELECT * FROM horsies_terminalization_miss(gen_random_uuid(), 'LEGACY_TERMINAL', 'NONE', NULL, NULL)$sql$,
    '22023');
SELECT fixture_check(
    'miss: pg_proc holds one horsies_terminalization_miss, the overload that takes the kind and the fence kind',
    (SELECT array_agg(p.oid::regprocedure::text) FROM pg_proc AS p WHERE p.proname = 'horsies_terminalization_miss')
        = ARRAY['horsies_terminalization_miss(uuid,text,text,text,timestamp with time zone)']
);

------------------------------------------------------------------------------------------------------------------------
-- A leaf detached out of band, through retention
------------------------------------------------------------------------------------------------------------------------

-- An expired standard_7d leaf that horsies_create_history_leaf created and published, then detached outside
-- maintenance: the catalog records no detach, and the published manifest names the leaf.
SELECT set_config(
    'fixture.out_of_band', horsies_history_leaf_name('standard_7d', fixture_today() - interval '10 days'), true
);
SELECT fixture_check(
    'out-of-band detach: the expired leaf is CREATED and published',
    fixture_create('standard_7d', fixture_today() - interval '10 days') = 'CREATED'
);
DO $$
BEGIN
    EXECUTE format('ALTER TABLE horsies_task_history_standard_7d DETACH PARTITION %I',
                   current_setting('fixture.out_of_band'));
END
$$;
SELECT fixture_check(
    'out-of-band detach: the candidate is DETACHED with no record, and the manifest and the readers name it',
    (SELECT c.attachment = 'DETACHED' AND NOT c.detach_recorded
     FROM horsies_prune_candidates(100) AS c WHERE c.leaf_name = current_setting('fixture.out_of_band'))
    AND EXISTS (SELECT 1 FROM horsies_task_lookup_manifest WHERE leaf_name = current_setting('fixture.out_of_band'))
    AND fixture_reader_names('horsies_task_history_duplicate_staged(uuid)', current_setting('fixture.out_of_band'))
);
-- The next pruning pass reaches the leaf through horsies_begin_leaf_detach.
CREATE TEMP TABLE out_of_band_begin AS
    SELECT * FROM horsies_begin_leaf_detach(current_setting('fixture.out_of_band'));
SELECT fixture_check(
    'out-of-band detach: the next pass begins with DETACHED and records the detach',
    (SELECT b.outcome = 'DETACHED' FROM out_of_band_begin AS b)
    AND (SELECT detached_at IS NOT NULL AND dropped_at IS NULL FROM horsies_task_history_leaf_catalog
         WHERE leaf_name = current_setting('fixture.out_of_band'))
    AND (SELECT c.detach_recorded
         FROM horsies_prune_candidates(100) AS c WHERE c.leaf_name = current_setting('fixture.out_of_band'))
);
SELECT fixture_check(
    'out-of-band detach: the publication of the same call leaves the leaf out of the manifest and the readers',
    NOT EXISTS (SELECT 1 FROM horsies_task_lookup_manifest WHERE leaf_name = current_setting('fixture.out_of_band'))
    AND NOT fixture_reader_names('horsies_task_history_duplicate_staged(uuid)',
                                 current_setting('fixture.out_of_band'))
    AND horsies_history_readers_current()
);
CREATE TEMP TABLE out_of_band_drop AS
    SELECT * FROM horsies_drop_detached_leaf(current_setting('fixture.out_of_band'));
SELECT fixture_check(
    'out-of-band detach: the drop is DROPPED, the relation is gone, and the catalog keeps both times',
    (SELECT d.outcome = 'DROPPED' FROM out_of_band_drop AS d)
    AND to_regclass(current_setting('fixture.out_of_band')) IS NULL
    AND (SELECT detached_at IS NOT NULL AND dropped_at IS NOT NULL FROM horsies_task_history_leaf_catalog
         WHERE leaf_name = current_setting('fixture.out_of_band'))
);
