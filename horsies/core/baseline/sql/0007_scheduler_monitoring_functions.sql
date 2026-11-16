-- horsies baseline schema, part 7: the scheduler and monitoring functions.
--
-- Each function replaces a sequence of client statements with one call, so that both clients send the same SQL.
-- Rules that hold for every function of this file:
-- - Results are plain rows of built-in types. A result with parts of different shapes has a row_kind column.
-- - A time that decides behaviour comes from the database clock (now(), statement_timestamp()).
-- - An argument that breaks a stated precondition is a defect of the caller: the function raises
--   invalid_parameter_value (SQLSTATE 22023) or null_value_not_allowed (22004), names the argument, and changes
--   nothing. These errors are not refusals of the operation and are not in the outcome sets below.
-- - A function of this file takes no task advisory lock: none of them moves a task.
--
-- Part of migration 1000: the install concatenates the files of this folder in name order and computes one
-- checksum over the result.

------------------------------------------------------------------------------------------------------------------------
-- Scheduler
------------------------------------------------------------------------------------------------------------------------

-- horsies_schedule_slot_task_id: the task id of one schedule slot.
--
-- Guarantee: the id is a pure function of the schedule name and the slot time. It has the UUIDv7 layout, so
-- horsies_uuid7_birth reads the slot time (in whole milliseconds) back from it, and the history readers prune
-- their probes by it:
-- - bytes 0-5: the slot time in Unix milliseconds (48 bits, big endian);
-- - 4 bits: version 7; 2 bits: variant 10;
-- - the other 74 bits: the first bits of sha256('horsies.schedule_slot.v1' || 0x00 || slot time as a big-endian
--   int8 of Unix microseconds || schedule name as UTF-8).
-- Two slots get one id only when they share the millisecond and all 74 hash bits.
-- Lock order: none. Reads no table.
-- Returns: the id. Raises 22004 for a NULL argument and 22023 for a slot time before 1970 or past the 48-bit
-- millisecond range.
-- Replaces: horsies/core/utils/fingerprint.py:78-87 (schedule_slot_task_id, a UUIDv5 with no birth time).
CREATE FUNCTION horsies_schedule_slot_task_id(
    p_schedule_name text,
    p_slot_at timestamp with time zone
) RETURNS uuid
    LANGUAGE plpgsql IMMUTABLE PARALLEL SAFE
AS $$
DECLARE
    c_max_unix_microseconds CONSTANT bigint := (1::bigint << 48) * 1000;
    v_slot_us bigint;
    v_tail bytea;
BEGIN
    IF p_schedule_name IS NULL OR p_slot_at IS NULL THEN
        RAISE EXCEPTION 'horsies_schedule_slot_task_id: schedule name and slot time are required'
            USING ERRCODE = 'null_value_not_allowed';
    END IF;
    v_slot_us := floor(extract(epoch FROM p_slot_at) * 1000000)::bigint;
    IF v_slot_us < 0 OR v_slot_us >= c_max_unix_microseconds THEN
        RAISE EXCEPTION 'horsies_schedule_slot_task_id: slot time % of schedule % is outside the UUIDv7 time range',
            p_slot_at, p_schedule_name
            USING ERRCODE = 'invalid_parameter_value';
    END IF;
    v_tail := substring(
        sha256(
            convert_to('horsies.schedule_slot.v1', 'UTF8')
            || '\x00'::bytea
            || int8send(v_slot_us)
            || convert_to(p_schedule_name, 'UTF8')
        )
        FROM 1 FOR 10
    );
    v_tail := set_byte(v_tail, 0, 112 | (get_byte(v_tail, 0) & 15));
    v_tail := set_byte(v_tail, 2, 128 | (get_byte(v_tail, 2) & 63));
    RETURN encode(substring(int8send(v_slot_us / 1000) FROM 3) || v_tail, 'hex')::uuid;
END;
$$;

-- horsies_fire_schedule: fire the due slots of one schedule and move its state row, in one transaction.
--
-- Guarantee:
-- - The slot enqueue and the state move commit together or not at all, so a committed slot always has an advanced
--   state row and never fires again, also after its task finished and left horsies_tasks.
-- - Single run: the state row is locked FOR UPDATE and its next_run_at must equal p_expected_next_run_at, the value
--   that the caller computed the slots from. A second scheduler that fires the same slot waits for this statement,
--   then finds the row moved and writes nothing. No advisory lock and no second connection.
-- - Each slot's task id is horsies_schedule_slot_task_id(name, slot time) and its sent_at is the slot time, so a
--   repeated fire of one slot carries the same id, fingerprint and sent_at, and the exact-id check of
--   horsies_insert_task_rows sees the same command.
-- - Each slot row is the row of an unkeyed send: PENDING, enqueued_at now(), no deadline, no key, no lineage, the
--   task's options, retry limit, execution class and retention class, and the command fingerprint (one value for
--   every slot of a schedule: the version-1 fingerprint leaves sent_at out).
-- - Slot outcomes are those of horsies_insert_task_rows: INSERTED; SAME_COMMAND (an earlier fire of this slot
--   committed its task); PAYLOAD_MISMATCH (the slot's id holds another command, for example after the task's
--   options changed: the slot counts as fired); CONFLICT_UNVERIFIABLE (the slot's task existed and moved to history
--   between the conflict and the read: the slot is not enqueued again and is not recorded as a run).
-- - The state row: next_run_at = p_next_run_at and updated_at = now(). When at least one slot is recorded (every
--   outcome except CONFLICT_UNVERIFIABLE), also last_run_at = now(), last_task_id = the id of the last recorded slot
--   and run_count + 1 (a fire counts once, for any number of slots).
-- - With no slots, the call only moves next_run_at: the advance of a scan that reached its bound, and the skip past
--   a slot whose enqueue the database refuses on every try (for example a CHECK violation).
-- - A database error before the commit changes nothing: the slots stay due.
-- Preconditions (22004/22023 when broken): every argument except p_task_options is non-NULL; at most 10,000 slots
-- (the largest max_catch_up_runs); the slot times strictly increase, the first is not before
-- p_expected_next_run_at, the last is before p_next_run_at and not after now(); p_next_run_at is after
-- p_expected_next_run_at. The caller computes the slots and p_next_run_at from the database time of its due read
-- (statement_timestamp()), never from a host clock, and calls this function in autocommit after that read, so a
-- due slot is never after now().
-- Lock order: the state row (horsies_schedule_state_pkey), then the inserted task rows. A conflicting task row is
-- read, not locked. No task advisory lock: nothing moves.
-- Returns: first one row whose row_kind is the call outcome: 'ADVANCED' (the slots are written and the row moved),
-- 'STATE_MISSING' (no state row; nothing written) or 'STATE_MOVED' (next_run_at differs from
-- p_expected_next_run_at; nothing written). After 'ADVANCED', one row_kind 'SLOT' row per slot in slot order, with
-- slot_at, task_id and slot_outcome.
-- Replaces: horsies/core/scheduler/service.py:506-732 (_check_schedule: the advisory lock at :531-536, the state
-- read, the per-slot enqueues, the state update), :938-1025 (_enqueue_scheduled_task) and :907-936
-- (_count_existing_slot_as_fired); horsies/core/scheduler/state.py:12-35 and :242-420 (update_after_run,
-- update_next_run); horsies/core/brokers/postgres.py:2587-2709 (the unkeyed enqueue) and :2861-2925 (the conflict
-- read). One slot took 6 round trips on two connections, plus 1 per extra slot and 3 per conflicting slot.
CREATE FUNCTION horsies_fire_schedule(
    p_schedule_name text,
    p_expected_next_run_at timestamp with time zone,
    p_next_run_at timestamp with time zone,
    p_slot_ats timestamp with time zone[],
    p_task_name text,
    p_queue_name text,
    p_priority integer,
    p_input text,
    p_task_options text,
    p_execution text,
    p_max_retries integer,
    p_retention_class_key text,
    p_retain_rerun_input boolean,
    p_fingerprint_version smallint,
    p_fingerprint bytea
) RETURNS TABLE (
    row_kind text,
    slot_at timestamp with time zone,
    task_id uuid,
    slot_outcome text
)
    LANGUAGE plpgsql VOLATILE
AS $$
#variable_conflict use_column
DECLARE
    c_max_slots CONSTANT integer := 10000;
    v_now timestamp with time zone := now();
    v_slot_count integer;
    v_current_next_run_at timestamp with time zone;
    v_ids uuid[] := ARRAY[]::uuid[];
    v_outcomes text[] := ARRAY[]::text[];
    v_last_recorded uuid;
BEGIN
    IF num_nulls(
        p_schedule_name, p_expected_next_run_at, p_next_run_at, p_slot_ats, p_task_name, p_queue_name, p_priority,
        p_input, p_execution, p_max_retries, p_retention_class_key, p_retain_rerun_input, p_fingerprint_version,
        p_fingerprint
    ) <> 0 OR array_position(p_slot_ats, NULL) IS NOT NULL THEN
        RAISE EXCEPTION 'horsies_fire_schedule: every argument except p_task_options is required, and no slot time '
            'is NULL'
            USING ERRCODE = 'null_value_not_allowed';
    END IF;
    v_slot_count := cardinality(p_slot_ats);
    IF v_slot_count > c_max_slots THEN
        RAISE EXCEPTION 'horsies_fire_schedule: % slots for schedule %, more than %', v_slot_count,
            p_schedule_name, c_max_slots
            USING ERRCODE = 'invalid_parameter_value';
    END IF;
    IF p_next_run_at <= p_expected_next_run_at THEN
        RAISE EXCEPTION 'horsies_fire_schedule: next run % of schedule % is not after the expected next run %',
            p_next_run_at, p_schedule_name, p_expected_next_run_at
            USING ERRCODE = 'invalid_parameter_value';
    END IF;
    IF v_slot_count > 0 AND (
        p_slot_ats[1] < p_expected_next_run_at
        OR p_slot_ats[v_slot_count] >= p_next_run_at
        OR p_slot_ats[v_slot_count] > v_now
        OR EXISTS (
            SELECT 1 FROM generate_subscripts(p_slot_ats, 1) AS i
            WHERE i > 1 AND p_slot_ats[i] <= p_slot_ats[i - 1]
        )
    ) THEN
        RAISE EXCEPTION 'horsies_fire_schedule: slot times of schedule % do not increase from the expected next run % '
            'to before the next run % and now', p_schedule_name, p_expected_next_run_at, p_next_run_at
            USING ERRCODE = 'invalid_parameter_value';
    END IF;

    SELECT s.next_run_at INTO v_current_next_run_at
    FROM horsies_schedule_state AS s
    WHERE s.schedule_name = p_schedule_name
    FOR UPDATE;
    IF NOT FOUND THEN
        RETURN QUERY SELECT 'STATE_MISSING'::text, NULL::timestamptz, NULL::uuid, NULL::text;
        RETURN;
    END IF;
    IF v_current_next_run_at <> p_expected_next_run_at THEN
        RETURN QUERY SELECT 'STATE_MOVED'::text, NULL::timestamptz, NULL::uuid, NULL::text;
        RETURN;
    END IF;

    IF v_slot_count > 0 THEN
        v_ids := ARRAY(
            SELECT horsies_schedule_slot_task_id(p_schedule_name, u.slot_at)
            FROM unnest(p_slot_ats) WITH ORDINALITY AS u(slot_at, position)
            ORDER BY u.position
        );
        SELECT array_agg(r.outcome ORDER BY r.ordinality) INTO v_outcomes
        FROM horsies_insert_task_rows(
            v_ids,
            array_fill(p_task_name, ARRAY[v_slot_count]),
            array_fill(p_queue_name, ARRAY[v_slot_count]),
            array_fill(p_priority, ARRAY[v_slot_count]),
            array_fill(p_input, ARRAY[v_slot_count]),
            array_fill(p_task_options, ARRAY[v_slot_count]),
            array_fill(p_execution, ARRAY[v_slot_count]),
            array_fill(p_retention_class_key, ARRAY[v_slot_count]),
            array_fill(p_retain_rerun_input, ARRAY[v_slot_count]),
            array_fill(p_max_retries, ARRAY[v_slot_count]),
            array_fill(NULL::timestamptz, ARRAY[v_slot_count]),
            p_slot_ats,
            array_fill(v_now, ARRAY[v_slot_count]),
            array_fill(FALSE, ARRAY[v_slot_count]),
            array_fill(p_fingerprint_version, ARRAY[v_slot_count]),
            array_fill(p_fingerprint, ARRAY[v_slot_count]),
            array_fill(NULL::bytea, ARRAY[v_slot_count]),
            array_fill(NULL::uuid, ARRAY[v_slot_count]),
            array_fill(NULL::uuid, ARRAY[v_slot_count]),
            'NONE'
        ) AS r;
        IF cardinality(v_outcomes) IS DISTINCT FROM v_slot_count THEN
            RAISE EXCEPTION 'horsies_fire_schedule: the insert helper answered % outcomes for % slots',
                cardinality(v_outcomes), v_slot_count
                USING ERRCODE = 'internal_error';
        END IF;
        SELECT u.id INTO v_last_recorded
        FROM unnest(v_ids, v_outcomes) WITH ORDINALITY AS u(id, outcome, position)
        WHERE u.outcome <> 'CONFLICT_UNVERIFIABLE'
        ORDER BY u.position DESC
        LIMIT 1;
    END IF;

    UPDATE horsies_schedule_state AS s
    SET next_run_at = p_next_run_at,
        updated_at = v_now,
        last_run_at = CASE WHEN v_last_recorded IS NULL THEN s.last_run_at ELSE v_now END,
        last_task_id = COALESCE(v_last_recorded, s.last_task_id),
        run_count = s.run_count + CASE WHEN v_last_recorded IS NULL THEN 0 ELSE 1 END
    WHERE s.schedule_name = p_schedule_name;

    RETURN QUERY SELECT 'ADVANCED'::text, NULL::timestamptz, NULL::uuid, NULL::text;
    RETURN QUERY
    SELECT 'SLOT'::text, u.slot_at, u.id, u.outcome
    FROM unnest(p_slot_ats, v_ids, v_outcomes) WITH ORDINALITY AS u(slot_at, id, outcome, position)
    ORDER BY u.position;
END;
$$;

------------------------------------------------------------------------------------------------------------------------
-- Monitoring: stale and expired task lists
------------------------------------------------------------------------------------------------------------------------

-- horsies_list_stale_tasks: the RUNNING tasks whose runner stopped beating.
--
-- Guarantee: every RUNNING row that started before now() minus the threshold and has no runner beat since then,
-- ordered by its latest runner beat with rows that have no beat first, then by id. It changes nothing. This is
-- Python's rule (the latest beat, or started_at when there is no beat, before the window) for every row whose beats
-- are not older than its started_at; the start writes the first runner beat at started_at. A runner beat of an
-- earlier attempt, older than the restart, no longer makes a task that just restarted stale. The verdict reads only
-- the heartbeat leaves of the threshold window; the read of the latest beat (with no time bound) runs only for the
-- rows that are stale.
-- The list is bounded at 10,000 rows (c_row_bound); the statement reads one row more to know whether it cut.
-- Lock order: none (a plain read).
-- Returns: one row per stale task (row_kind 'TASK', row_position 1..n), then one row_kind 'LIST_END' row whose
-- list_state is 'COMPLETE' or 'TRUNCATED' and whose row_bound is the bound. last_heartbeat is NULL for a task
-- with no runner beat. Raises 22023 for a threshold outside 0..525600 minutes (one year).
-- Replaces: horsies/core/brokers/postgres.py:649-669 (GET_STALE_TASKS_SQL) and :3469-3492 (get_stale_tasks), which
-- ran in a session (BEGIN, SELECT, ROLLBACK) with no row bound and no threshold check.
CREATE FUNCTION horsies_list_stale_tasks(p_threshold_minutes integer)
RETURNS TABLE (
    row_kind text,
    row_position integer,
    task_id uuid,
    task_name character varying,
    worker_hostname character varying,
    worker_pid integer,
    worker_process_name character varying,
    started_at timestamp with time zone,
    last_heartbeat timestamp with time zone,
    list_state text,
    row_bound integer
)
    LANGUAGE plpgsql STABLE
AS $$
#variable_conflict use_column
DECLARE
    c_row_bound CONSTANT integer := 10000;
    c_max_threshold_minutes CONSTANT integer := 525600;
    v_window interval;
BEGIN
    IF p_threshold_minutes IS NULL
       OR p_threshold_minutes < 0
       OR p_threshold_minutes > c_max_threshold_minutes THEN
        RAISE EXCEPTION 'horsies_list_stale_tasks: threshold % is not a whole number of minutes from 0 to %',
            p_threshold_minutes, c_max_threshold_minutes
            USING ERRCODE = 'invalid_parameter_value';
    END IF;
    v_window := make_interval(mins => p_threshold_minutes);
    RETURN QUERY
    WITH stale AS MATERIALIZED (
        SELECT t.id, t.task_name, t.worker_hostname, t.worker_pid, t.worker_process_name, t.started_at
        FROM horsies_tasks AS t
        WHERE t.status = 'RUNNING'
          AND t.started_at < now() - v_window
          AND NOT EXISTS (
              SELECT 1
              FROM horsies_heartbeats AS recent
              WHERE recent.task_id = t.id
                AND recent.role = 'runner'
                AND recent.sent_at >= now() - v_window
          )
    ), page AS MATERIALIZED (
        SELECT s.*, beat.last_heartbeat
        FROM stale AS s
        LEFT JOIN LATERAL (
            SELECT h.sent_at AS last_heartbeat
            FROM horsies_heartbeats AS h
            WHERE h.task_id = s.id AND h.role = 'runner'
            ORDER BY h.sent_at DESC
            LIMIT 1
        ) AS beat ON TRUE
        ORDER BY beat.last_heartbeat NULLS FIRST, s.id
        LIMIT c_row_bound + 1
    ), numbered AS (
        SELECT p.*, (row_number() OVER (ORDER BY p.last_heartbeat NULLS FIRST, p.id))::integer AS list_position
        FROM page AS p
    )
    SELECT 'TASK'::text, n.list_position, n.id, n.task_name, n.worker_hostname, n.worker_pid, n.worker_process_name,
           n.started_at, n.last_heartbeat, NULL::text, NULL::integer
    FROM numbered AS n
    WHERE n.list_position <= c_row_bound
    UNION ALL
    SELECT 'LIST_END'::text, NULL::integer, NULL::uuid, NULL::character varying, NULL::character varying,
           NULL::integer, NULL::character varying, NULL::timestamptz, NULL::timestamptz,
           CASE WHEN (SELECT count(*) FROM page) > c_row_bound THEN 'TRUNCATED' ELSE 'COMPLETE' END,
           c_row_bound
    ORDER BY 2 NULLS LAST;
END;
$$;

-- horsies_list_expired_tasks: the PENDING tasks whose deadline passed before a worker claimed them.
--
-- Guarantee: every PENDING row whose good_until is at or before now(), oldest deadline first, then by id, with
-- expired_for = now() - good_until. It changes nothing. The comparison good_until <= now() implies the partial
-- predicate of horsies_tasks_pending_deadline (good_until IS NOT NULL), so the read rides that index.
-- The list is bounded at 10,000 rows (c_row_bound); the statement reads one row more to know whether it cut.
-- Lock order: none (a plain read).
-- Returns: one row per expired task (row_kind 'TASK', row_position 1..n), then one row_kind 'LIST_END' row whose
-- list_state is 'COMPLETE' or 'TRUNCATED' and whose row_bound is the bound.
-- Replaces: horsies/core/brokers/postgres.py:748-762 (GET_EXPIRED_TASKS_SQL) and :3913-3930 (get_expired_tasks),
-- which ran in a session (BEGIN, SELECT, ROLLBACK) with no row bound.
CREATE FUNCTION horsies_list_expired_tasks()
RETURNS TABLE (
    row_kind text,
    row_position integer,
    task_id uuid,
    task_name character varying,
    queue_name character varying,
    priority integer,
    sent_at timestamp with time zone,
    enqueued_at timestamp with time zone,
    good_until timestamp with time zone,
    expired_for interval,
    list_state text,
    row_bound integer
)
    LANGUAGE plpgsql STABLE
AS $$
#variable_conflict use_column
DECLARE
    c_row_bound CONSTANT integer := 10000;
BEGIN
    RETURN QUERY
    WITH page AS MATERIALIZED (
        SELECT t.id, t.task_name, t.queue_name, t.priority, t.sent_at, t.enqueued_at, t.good_until
        FROM horsies_tasks AS t
        WHERE t.status = 'PENDING' AND t.good_until <= now()
        ORDER BY t.good_until, t.id
        LIMIT c_row_bound + 1
    ), numbered AS (
        SELECT p.*, (row_number() OVER (ORDER BY p.good_until, p.id))::integer AS list_position
        FROM page AS p
    )
    SELECT 'TASK'::text, n.list_position, n.id, n.task_name, n.queue_name, n.priority, n.sent_at, n.enqueued_at,
           n.good_until, now() - n.good_until, NULL::text, NULL::integer
    FROM numbered AS n
    WHERE n.list_position <= c_row_bound
    UNION ALL
    SELECT 'LIST_END'::text, NULL::integer, NULL::uuid, NULL::character varying, NULL::character varying,
           NULL::integer, NULL::timestamptz, NULL::timestamptz, NULL::timestamptz, NULL::interval,
           CASE WHEN (SELECT count(*) FROM page) > c_row_bound THEN 'TRUNCATED' ELSE 'COMPLETE' END,
           c_row_bound
    ORDER BY 2 NULLS LAST;
END;
$$;

------------------------------------------------------------------------------------------------------------------------
-- Monitoring: task list and facets
------------------------------------------------------------------------------------------------------------------------

-- horsies_monitoring_check_scope: the precondition check of the task list and facet reads. Internal.
--
-- Guarantee: returns only when every argument holds its precondition:
-- - p_builtin_codes and p_builtin_categories are parallel arrays of one length: code i belongs to category i. Each
--   code is non-empty and appears once; each category is OPERATIONAL, CONTRACT, RETRIEVAL or OUTCOME. The caller
--   passes the built-in error-code taxonomy of the library; a code outside it is a DOMAIN code.
-- - p_statuses holds only task statuses; p_error_categories holds only the four built-in categories and DOMAIN.
-- - the window is non-empty: p_window_lower < p_window_upper.
-- Lock order: none. Reads no table.
-- Returns: nothing. Raises 22004 or 22023 naming the argument.
CREATE FUNCTION horsies_monitoring_check_scope(
    p_window_lower timestamp with time zone,
    p_window_upper timestamp with time zone,
    p_statuses text[],
    p_error_categories text[],
    p_builtin_codes text[],
    p_builtin_categories text[]
) RETURNS void
    LANGUAGE plpgsql IMMUTABLE PARALLEL SAFE
AS $$
BEGIN
    IF p_window_lower IS NULL OR p_window_upper IS NULL OR p_statuses IS NULL OR p_error_categories IS NULL
       OR p_builtin_codes IS NULL OR p_builtin_categories IS NULL THEN
        RAISE EXCEPTION 'monitoring scope: window bounds, filter arrays and the code taxonomy are required'
            USING ERRCODE = 'null_value_not_allowed';
    END IF;
    IF p_window_lower >= p_window_upper THEN
        RAISE EXCEPTION 'monitoring scope: window lower bound % is not before upper bound %',
            p_window_lower, p_window_upper
            USING ERRCODE = 'invalid_parameter_value';
    END IF;
    IF EXISTS (
        SELECT 1 FROM unnest(p_statuses) AS s(status)
        WHERE s.status IS NULL
           OR s.status NOT IN ('PENDING', 'CLAIMED', 'RUNNING', 'COMPLETED', 'FAILED', 'CANCELLED', 'EXPIRED')
    ) THEN
        RAISE EXCEPTION 'monitoring scope: p_statuses % holds a value that is not a task status', p_statuses
            USING ERRCODE = 'invalid_parameter_value';
    END IF;
    IF EXISTS (
        SELECT 1 FROM unnest(p_error_categories) AS c(category)
        WHERE c.category IS NULL
           OR c.category NOT IN ('OPERATIONAL', 'CONTRACT', 'RETRIEVAL', 'OUTCOME', 'DOMAIN')
    ) THEN
        RAISE EXCEPTION 'monitoring scope: p_error_categories % holds a value that is not an error category',
            p_error_categories
            USING ERRCODE = 'invalid_parameter_value';
    END IF;
    IF cardinality(p_builtin_codes) <> cardinality(p_builtin_categories) THEN
        RAISE EXCEPTION 'monitoring scope: % built-in codes but % categories', cardinality(p_builtin_codes),
            cardinality(p_builtin_categories)
            USING ERRCODE = 'invalid_parameter_value';
    END IF;
    IF EXISTS (
        SELECT 1 FROM unnest(p_builtin_codes, p_builtin_categories) AS m(code, category)
        WHERE m.code IS NULL OR m.code = ''
           OR m.category IS NULL OR m.category NOT IN ('OPERATIONAL', 'CONTRACT', 'RETRIEVAL', 'OUTCOME')
    ) OR (SELECT count(DISTINCT m.code) FROM unnest(p_builtin_codes) AS m(code)) <> cardinality(p_builtin_codes) THEN
        RAISE EXCEPTION 'monitoring scope: the built-in code taxonomy has an empty, repeated or unknown entry'
            USING ERRCODE = 'invalid_parameter_value';
    END IF;
END;
$$;

-- horsies_monitoring_task_list: one page of the monitoring task list.
--
-- Guarantee: one page of tasks from the live table and the history window [p_window_lower, p_window_upper) on
-- terminal_at, read in one statement and so in one snapshot: a task that moves to history during the read is on
-- the page once or, past the page, not at all; never twice.
-- - Filters: values in one dimension OR together; dimensions AND together; an empty array is no filter. An error
--   category selects the codes of its built-in family; DOMAIN selects every non-empty code outside the built-in
--   taxonomy. A live row has no error code, so an error-code or category filter drops the live side.
--   p_retried_only keeps rows with retry_count > 0.
-- - Sort: p_sort_key is one of enqueued_at, started_at, completed_at, failed_at, status, task_name, queue_name,
--   priority, retry_count, queue_s (started_at - enqueued_at), exec_s (terminal_at - started_at); p_sort_direction
--   is ASC or DESC. NULL keys come last in both directions. A live row sorts completed_at, failed_at and exec_s as
--   NULL. Ties break by side (HISTORY before LIVE), then by task id, inside each side and in the merge, so a page
--   does not reorder between polls. Text keys sort in the database collation on both sides and in the merge.
-- - Each side reads at most p_offset + p_limit rows in sort order; only nullable keys carry NULLS LAST, so the
--   default key rides idx_horsies_tasks_enqueued_at (live) and the leaf enqueued_at index (history) in both
--   directions. The id tie-break is an incremental sort on that index order: it sorts only rows that share one
--   enqueued_at, and the scan still stops at the page reach.
-- - Bounds: p_limit 1..200, p_offset >= 0, p_offset + p_limit <= 500. No payload column is read.
-- - Totals: with any filter, live_total and history_total are exact counts of the matches, in the same
--   statement. With no filter, live_total is the planner's reltuples of horsies_tasks (an exact count when the
--   table was never sampled, reltuples < 0), and the history side is 0 when the history table has no row, else
--   the EXPLAIN (FORMAT JSON) text of the window read, which the caller decodes. EXPLAIN is a utility statement,
--   which PostgreSQL refuses inside a STABLE function; this is why the function is VOLATILE. It writes nothing.
-- Lock order: none (a plain read).
-- Returns: row 0 (row_kind 'TOTALS'), then the page rows (row_kind 'TASK', row_position 1..n, in page order).
-- total_kind of the TOTALS row: 'EXACT' (live_total, history_total), 'ESTIMATE_HISTORY_EMPTY' (live_total,
-- history_total = 0), or 'ESTIMATE_HISTORY_PLAN' (live_total, history_estimate_plan). A TASK row has location
-- 'LIVE' (error_code and terminal_at NULL; worker columns from the current claim) or 'HISTORY' (worker columns from
-- the last claim). status is the stored text; the caller decodes it.
-- Replaces: horsies/monitoring/queries.py:1279-1401 (list_tasks), :1190-1221 (list_rows_statement), :1229-1276 (the
-- totals) and horsies/core/history/reads/pages.py (history_page_statement): one session of 6 (filtered) or 7
-- (unfiltered) round trips, four snapshots, and a merge in client code-point order.
CREATE FUNCTION horsies_monitoring_task_list(
    p_window_lower timestamp with time zone,
    p_window_upper timestamp with time zone,
    p_statuses text[],
    p_task_names text[],
    p_queue_names text[],
    p_worker_ids text[],
    p_error_codes text[],
    p_error_categories text[],
    p_retried_only boolean,
    p_builtin_codes text[],
    p_builtin_categories text[],
    p_sort_key text,
    p_sort_direction text,
    p_offset integer,
    p_limit integer
) RETURNS TABLE (
    row_kind text,
    row_position integer,
    total_kind text,
    live_total bigint,
    history_total bigint,
    history_estimate_plan text,
    location text,
    task_id uuid,
    task_name character varying,
    queue_name character varying,
    priority integer,
    status text,
    retry_count integer,
    max_retries integer,
    is_workflow_task boolean,
    error_code text,
    worker_id character varying,
    worker_hostname character varying,
    enqueued_at timestamp with time zone,
    started_at timestamp with time zone,
    terminal_at timestamp with time zone
)
    LANGUAGE plpgsql VOLATILE
AS $$
DECLARE
    c_max_limit CONSTANT integer := 200;
    c_max_reach CONSTANT integer := 500;
    v_live_key text;
    v_history_key text;
    v_nullable boolean;
    v_direction text;
    v_live_order text;
    v_history_order text;
    v_live_where text[] := ARRAY[]::text[];
    v_history_where text[] := ARRAY['h.terminal_at >= $1', 'h.terminal_at < $2'];
    v_category_arms text[] := ARRAY[]::text[];
    v_category_codes text[];
    v_filtered boolean;
    v_total_kind text;
    v_history_total bigint;
    v_plan text;
    v_totals text;
    v_live_where_sql text;
    v_history_where_sql text;
BEGIN
    PERFORM horsies_monitoring_check_scope(
        p_window_lower, p_window_upper, p_statuses, p_error_categories, p_builtin_codes, p_builtin_categories
    );
    IF p_task_names IS NULL OR p_queue_names IS NULL OR p_worker_ids IS NULL OR p_error_codes IS NULL
       OR p_retried_only IS NULL OR p_sort_key IS NULL OR p_sort_direction IS NULL
       OR p_offset IS NULL OR p_limit IS NULL THEN
        RAISE EXCEPTION 'horsies_monitoring_task_list: every argument is required'
            USING ERRCODE = 'null_value_not_allowed';
    END IF;
    IF p_limit < 1 OR p_limit > c_max_limit OR p_offset < 0 OR p_offset > c_max_reach - p_limit THEN
        RAISE EXCEPTION 'horsies_monitoring_task_list: offset % and limit % are outside the page bounds',
            p_offset, p_limit
            USING ERRCODE = 'invalid_parameter_value';
    END IF;
    CASE p_sort_direction
        WHEN 'ASC' THEN v_direction := 'ASC';
        WHEN 'DESC' THEN v_direction := 'DESC';
        ELSE
            RAISE EXCEPTION 'horsies_monitoring_task_list: sort direction % is not ASC or DESC', p_sort_direction
                USING ERRCODE = 'invalid_parameter_value';
    END CASE;
    -- (live expression, history expression, nullable). Only these fixed texts reach the statement.
    CASE p_sort_key
        WHEN 'enqueued_at' THEN
            v_live_key := 't.enqueued_at'; v_history_key := 'h.enqueued_at'; v_nullable := FALSE;
        WHEN 'started_at' THEN
            v_live_key := 't.started_at'; v_history_key := 'h.started_at'; v_nullable := TRUE;
        WHEN 'completed_at' THEN
            v_live_key := 'NULL::timestamptz';
            v_history_key := $k$CASE WHEN h.status = 'COMPLETED' THEN h.terminal_at END$k$;
            v_nullable := TRUE;
        WHEN 'failed_at' THEN
            v_live_key := 'NULL::timestamptz';
            v_history_key := $k$CASE WHEN h.status <> 'COMPLETED' THEN h.terminal_at END$k$;
            v_nullable := TRUE;
        WHEN 'status' THEN
            v_live_key := 't.status'; v_history_key := 'h.status'; v_nullable := FALSE;
        WHEN 'task_name' THEN
            v_live_key := 't.task_name'; v_history_key := 'h.task_name'; v_nullable := FALSE;
        WHEN 'queue_name' THEN
            v_live_key := 't.queue_name'; v_history_key := 'h.queue_name'; v_nullable := FALSE;
        WHEN 'priority' THEN
            v_live_key := 't.priority'; v_history_key := 'h.priority'; v_nullable := FALSE;
        WHEN 'retry_count' THEN
            v_live_key := 't.retry_count'; v_history_key := 'h.retry_count'; v_nullable := FALSE;
        WHEN 'queue_s' THEN
            v_live_key := '(t.started_at - t.enqueued_at)';
            v_history_key := '(h.started_at - h.enqueued_at)';
            v_nullable := TRUE;
        WHEN 'exec_s' THEN
            v_live_key := 'NULL::interval';
            v_history_key := '(h.terminal_at - h.started_at)';
            v_nullable := TRUE;
        ELSE
            RAISE EXCEPTION 'horsies_monitoring_task_list: % is not a sort key', p_sort_key
                USING ERRCODE = 'invalid_parameter_value';
    END CASE;
    v_live_order := v_live_key || ' ' || v_direction || CASE WHEN v_nullable THEN ' NULLS LAST' ELSE '' END;
    v_history_order := v_history_key || ' ' || v_direction || CASE WHEN v_nullable THEN ' NULLS LAST' ELSE '' END;

    -- Filters. Values travel as parameters; only fixed texts are concatenated.
    IF cardinality(p_statuses) > 0 THEN
        v_live_where := v_live_where || 't.status = ANY($3)'::text;
        v_history_where := v_history_where || 'h.status = ANY($3)'::text;
    END IF;
    IF cardinality(p_task_names) > 0 THEN
        v_live_where := v_live_where || 't.task_name = ANY($4)'::text;
        v_history_where := v_history_where || 'h.task_name = ANY($4)'::text;
    END IF;
    IF cardinality(p_queue_names) > 0 THEN
        v_live_where := v_live_where || 't.queue_name = ANY($5)'::text;
        v_history_where := v_history_where || 'h.queue_name = ANY($5)'::text;
    END IF;
    IF cardinality(p_worker_ids) > 0 THEN
        v_live_where := v_live_where || 't.claimed_by_worker_id = ANY($6)'::text;
        v_history_where := v_history_where || 'h.last_claimed_worker_id = ANY($6)'::text;
    END IF;
    IF cardinality(p_error_codes) > 0 THEN
        v_history_where := v_history_where || 'h.error_code = ANY($7)'::text;
    END IF;
    IF cardinality(p_error_categories) > 0 THEN
        v_category_codes := ARRAY(
            SELECT m.code
            FROM unnest(p_builtin_codes, p_builtin_categories) AS m(code, category)
            WHERE m.category = ANY(p_error_categories)
        );
        IF cardinality(v_category_codes) > 0 THEN
            v_category_arms := v_category_arms || 'h.error_code = ANY($8)'::text;
        END IF;
        IF 'DOMAIN' = ANY(p_error_categories) THEN
            v_category_arms := v_category_arms || $c$(h.error_code <> '' AND h.error_code <> ALL($9))$c$::text;
        END IF;
        IF cardinality(v_category_arms) = 0 THEN
            v_category_arms := ARRAY['FALSE'];
        END IF;
        v_history_where := v_history_where || ('(' || array_to_string(v_category_arms, ' OR ') || ')');
    END IF;
    IF cardinality(p_error_codes) > 0 OR cardinality(p_error_categories) > 0 THEN
        v_live_where := v_live_where || 'FALSE'::text;
    END IF;
    IF p_retried_only THEN
        v_live_where := v_live_where || 't.retry_count > 0'::text;
        v_history_where := v_history_where || 'h.retry_count > 0'::text;
    END IF;
    v_filtered := cardinality(p_statuses) > 0 OR cardinality(p_task_names) > 0 OR cardinality(p_queue_names) > 0
        OR cardinality(p_worker_ids) > 0 OR cardinality(p_error_codes) > 0 OR cardinality(p_error_categories) > 0
        OR p_retried_only;
    v_live_where_sql := CASE WHEN cardinality(v_live_where) = 0 THEN 'TRUE'
                             ELSE array_to_string(v_live_where, ' AND ') END;
    v_history_where_sql := array_to_string(v_history_where, ' AND ');

    IF v_filtered THEN
        v_totals := format(
            $t$'EXACT'::text,
               (SELECT count(*) FROM horsies_tasks AS t WHERE %1$s),
               (SELECT count(*) FROM horsies_task_history AS h WHERE %2$s),
               NULL::text$t$,
            v_live_where_sql, v_history_where_sql
        );
    ELSE
        IF EXISTS (SELECT 1 FROM horsies_task_history) THEN
            EXECUTE 'EXPLAIN (FORMAT JSON) SELECT 1 FROM horsies_task_history AS h '
                    'WHERE h.terminal_at >= $1 AND h.terminal_at < $2'
                INTO v_plan
                USING p_window_lower, p_window_upper;
            v_total_kind := 'ESTIMATE_HISTORY_PLAN';
            v_history_total := NULL;
        ELSE
            v_total_kind := 'ESTIMATE_HISTORY_EMPTY';
            v_history_total := 0;
        END IF;
        v_totals := $t$ $13::text,
               (SELECT CASE WHEN c.reltuples < 0 THEN (SELECT count(*) FROM horsies_tasks)
                            ELSE c.reltuples::bigint END
                FROM pg_class AS c WHERE c.oid = 'horsies_tasks'::regclass),
               $14::bigint,
               $15::text$t$;
    END IF;

    RETURN QUERY EXECUTE format(
        $q$
        WITH live AS (
            SELECT 'LIVE'::text AS location, t.id AS task_id, t.task_name, t.queue_name, t.priority,
                   t.status::text AS status, t.retry_count, t.max_retries, t.is_workflow_task,
                   NULL::text AS error_code, t.claimed_by_worker_id AS worker_id, t.worker_hostname,
                   t.enqueued_at, t.started_at, NULL::timestamptz AS terminal_at, %1$s AS sort_value
            FROM horsies_tasks AS t
            WHERE %3$s
            ORDER BY %5$s, t.id
            LIMIT $10
        ), history AS (
            SELECT 'HISTORY'::text, h.task_id, h.task_name, h.queue_name, h.priority,
                   h.status::text, h.retry_count, h.max_retries, h.is_workflow_task,
                   h.error_code, h.last_claimed_worker_id, h.last_worker_hostname,
                   h.enqueued_at, h.started_at, h.terminal_at, %2$s
            FROM horsies_task_history AS h
            WHERE %4$s
            ORDER BY %6$s, h.task_id
            LIMIT $10
        ), page AS (
            SELECT u.*,
                   row_number() OVER (ORDER BY u.sort_value %7$s NULLS LAST, u.location, u.task_id) AS list_position
            FROM (SELECT * FROM live UNION ALL SELECT * FROM history) AS u
        )
        SELECT 'TOTALS'::text, 0, %8$s,
               NULL::text, NULL::uuid, NULL::character varying, NULL::character varying, NULL::integer,
               NULL::text, NULL::integer, NULL::integer, NULL::boolean, NULL::text, NULL::character varying,
               NULL::character varying, NULL::timestamptz, NULL::timestamptz, NULL::timestamptz
        UNION ALL
        SELECT 'TASK'::text, (p.list_position - $12)::integer, NULL::text, NULL::bigint, NULL::bigint, NULL::text,
               p.location, p.task_id, p.task_name, p.queue_name, p.priority, p.status, p.retry_count,
               p.max_retries, p.is_workflow_task, p.error_code, p.worker_id, p.worker_hostname, p.enqueued_at,
               p.started_at, p.terminal_at
        FROM page AS p
        WHERE p.list_position > $12 AND p.list_position <= $12 + $11
        ORDER BY 2
        $q$,
        v_live_key, v_history_key, v_live_where_sql, v_history_where_sql, v_live_order, v_history_order,
        v_direction, v_totals
    )
    USING p_window_lower, p_window_upper, p_statuses, p_task_names, p_queue_names, p_worker_ids, p_error_codes,
          v_category_codes, p_builtin_codes, p_offset + p_limit, p_limit, p_offset, v_total_kind, v_history_total,
          v_plan;
END;
$$;

-- horsies_monitoring_task_facets: the filter options of the monitoring task list.
--
-- Guarantee: the distinct workers, task names and queues (at most 50 each, c_facet_value_cap) and error codes (at
-- most 30 in each category, c_error_facet_cap) with counts, over the live table and the history window
-- [p_window_lower, p_window_upper) on terminal_at, in one statement. Live and history counts sum before the rank;
-- ties follow the database collation. Each error code carries its category (its built-in family, else DOMAIN), and
-- each category carries the total of every code in it, also codes past the cap, so the result stays bounded for
-- any number of distinct codes. Only p_statuses and p_retried_only scope the facets. NULL values are not facet
-- values; an empty error code is not either (a user error code is free text, and the empty string passes the
-- writer's check). The live table has no error code, so the live side counts three facets.
-- Lock order: none (a plain read).
-- Returns: one row per facet value: facet ('worker', 'task_name', 'queue_name', 'error_code'), facet_value,
-- facet_count; category and category_total are set on error_code rows only. Ordered by facet, then rank.
-- Replaces: horsies/monitoring/queries.py:725-849 (facets_statement) and :942-1007 (task_facets): the same
-- statement inside a session (BEGIN, SELECT, ROLLBACK), with the taxonomy written into the text.
CREATE FUNCTION horsies_monitoring_task_facets(
    p_window_lower timestamp with time zone,
    p_window_upper timestamp with time zone,
    p_statuses text[],
    p_retried_only boolean,
    p_builtin_codes text[],
    p_builtin_categories text[]
) RETURNS TABLE (
    facet text,
    facet_value text,
    facet_count bigint,
    category text,
    category_total bigint
)
    LANGUAGE plpgsql STABLE
AS $$
DECLARE
    c_facet_value_cap CONSTANT integer := 50;
    c_error_facet_cap CONSTANT integer := 30;
BEGIN
    PERFORM horsies_monitoring_check_scope(
        p_window_lower, p_window_upper, p_statuses, ARRAY[]::text[], p_builtin_codes, p_builtin_categories
    );
    IF p_retried_only IS NULL THEN
        RAISE EXCEPTION 'horsies_monitoring_task_facets: p_retried_only is required'
            USING ERRCODE = 'null_value_not_allowed';
    END IF;
    -- EXECUTE plans the statement with the argument values, so an empty filter folds away.
    RETURN QUERY EXECUTE $q$
        WITH lifecycle_facets AS (
            SELECT CASE GROUPING(claimed_by_worker_id, task_name, queue_name)
                       WHEN 3 THEN 'worker' WHEN 5 THEN 'task_name' WHEN 6 THEN 'queue_name'
                   END AS facet,
                   CASE GROUPING(claimed_by_worker_id, task_name, queue_name)
                       WHEN 3 THEN claimed_by_worker_id::text WHEN 5 THEN task_name::text
                       WHEN 6 THEN queue_name::text
                   END AS facet_value,
                   sum(group_count) AS facet_count
            FROM (
                SELECT t.claimed_by_worker_id, t.task_name, t.queue_name, count(*) AS group_count
                FROM horsies_tasks AS t
                WHERE (cardinality($3) = 0 OR t.status = ANY($3)) AND (NOT $4 OR t.retry_count > 0)
                GROUP BY t.claimed_by_worker_id, t.task_name, t.queue_name
            ) AS live_groups
            GROUP BY GROUPING SETS ((claimed_by_worker_id), (task_name), (queue_name))
            UNION ALL
            SELECT CASE GROUPING(last_claimed_worker_id, task_name, queue_name, error_code)
                       WHEN 7 THEN 'worker' WHEN 11 THEN 'task_name' WHEN 13 THEN 'queue_name'
                       WHEN 14 THEN 'error_code'
                   END,
                   CASE GROUPING(last_claimed_worker_id, task_name, queue_name, error_code)
                       WHEN 7 THEN last_claimed_worker_id::text WHEN 11 THEN task_name::text
                       WHEN 13 THEN queue_name::text WHEN 14 THEN error_code
                   END,
                   sum(group_count)
            FROM (
                SELECT h.last_claimed_worker_id, h.task_name, h.queue_name, h.error_code, count(*) AS group_count
                FROM horsies_task_history AS h
                WHERE h.terminal_at >= $1 AND h.terminal_at < $2
                  AND (cardinality($3) = 0 OR h.status = ANY($3)) AND (NOT $4 OR h.retry_count > 0)
                GROUP BY h.last_claimed_worker_id, h.task_name, h.queue_name, h.error_code
            ) AS history_groups
            GROUP BY GROUPING SETS ((last_claimed_worker_id), (task_name), (queue_name), (error_code))
        ), combined AS (
            SELECT f.facet, f.facet_value, sum(f.facet_count)::bigint AS facet_count
            FROM lifecycle_facets AS f
            WHERE f.facet_value IS NOT NULL
              AND (f.facet <> 'error_code' OR f.facet_value <> '')
            GROUP BY f.facet, f.facet_value
        ), ranked AS (
            SELECT c.facet, c.facet_value, c.facet_count,
                   row_number() OVER (PARTITION BY c.facet ORDER BY c.facet_count DESC, c.facet_value) AS facet_rank,
                   CASE WHEN c.facet = 'error_code' THEN COALESCE(m.category, 'DOMAIN') END AS category
            FROM combined AS c
            LEFT JOIN unnest($5::text[], $6::text[]) AS m(code, category)
                ON c.facet = 'error_code' AND m.code = c.facet_value
        ), scoped AS (
            SELECT r.facet, r.facet_value, r.facet_count, r.facet_rank, r.category,
                   row_number() OVER (PARTITION BY r.facet, r.category ORDER BY r.facet_rank) AS category_rank,
                   sum(r.facet_count) OVER (PARTITION BY r.facet, r.category)::bigint AS category_total
            FROM ranked AS r
        )
        SELECT s.facet, s.facet_value, s.facet_count, s.category,
               CASE WHEN s.facet = 'error_code' THEN s.category_total END
        FROM scoped AS s
        WHERE CASE s.facet WHEN 'error_code' THEN s.category_rank <= $8 ELSE s.facet_rank <= $7 END
        ORDER BY s.facet, s.facet_rank
    $q$
    USING p_window_lower, p_window_upper, p_statuses, p_retried_only, p_builtin_codes, p_builtin_categories,
          c_facet_value_cap, c_error_facet_cap;
END;
$$;

------------------------------------------------------------------------------------------------------------------------
-- Monitoring and task info: one task with its parts
------------------------------------------------------------------------------------------------------------------------

-- horsies_monitoring_task_detail: one task, live or in history, with its attempts and its workflow link.
--
-- Guarantee: every part comes from one snapshot, with no REPEATABLE READ: the function is STABLE, so each of its
-- statements and the STABLE published reader horsies_task_detail_staged that it calls read the snapshot of the
-- statement that calls the function. A task that moves to history during the read is either LIVE with all its
-- attempts or HISTORY with its snapshot, never LIVE without attempts and never absent.
-- - LIVE: the live row. The live table has no terminal, error or result column. With p_with_attempts, the attempt
--   rows in ascending order, encoded by horsies_encode_task_attempts: the same version-1 bytes that the move stores
--   in history, so one decoder serves both locations.
-- - HISTORY: after a live miss, the history row through horsies_task_detail_staged (which prunes its leaf probes by
--   the id's UUIDv7 birth). With p_with_result, the stored result envelope; with p_with_attempts, the stored attempt
--   snapshot and its digest.
-- - Both: the workflow link (the node whose task_id is this task) by ix_horsies_workflow_tasks_task_id.
-- - ABSENT: no row in either place. absence_reason from horsies_task_absence_reason (0006_history_functions.sql),
--   the one rule of absence that the rerun read also uses, read in the same snapshot: NOT_CLASSIFIED when the id is
--   not a UUIDv7; PREDATES_RETAINED_HISTORY when its birth is before the latest upper anchor of the detached leaves
--   of PERIOD classes plus 5 s, so retention can have removed it; WITHIN_RETAINED_HISTORY otherwise, also when
--   retention has detached no leaf.
-- - The caller decodes the stored status text and the attempt snapshot, and checks the digests.
-- Lock order: none (a plain read; no row lock, no advisory lock).
-- Returns: one row. Columns by location:
-- - every location: location ('LIVE', 'HISTORY', 'ABSENT'), task_id;
-- - LIVE and HISTORY: task_name, queue_name, priority, status, is_workflow_task, retry_count, max_retries, sent_at,
--   enqueued_at, claimed_at, started_at, good_until, worker_id, worker_hostname, worker_pid, worker_process_name
--   (LIVE: the current claim; HISTORY: the last claim), workflow_id and workflow_task_index (both NULL for a task
--   that backs no node);
-- - LIVE only: next_retry_at, execution;
-- - HISTORY only: terminal_at, terminalization_kind, error_code, final_failed_reason; with p_with_result:
--   result_envelope_version, result_payload, result_digest;
-- - with p_with_attempts: attempt_archive_version and attempt_snapshot; HISTORY also attempt_snapshot_digest;
-- - ABSENT only: absence_reason.
-- Raises 22004 for a NULL argument, and data_corrupted (XX001) when the published reader reports the task live
-- after the live miss of the same snapshot, or answers a location it does not define.
-- Replaces: horsies/monitoring/queries.py:1404-1499 (get_task_detail) and :1516-1548 (_history_detail), which read
-- in REPEATABLE READ with 5 or 6 round trips; horsies/core/brokers/postgres.py:4768-4978 (get_task_info_async) and
-- :4625-4766 (_history_task_info), 4 or 5 round trips with a snapshot per statement.
CREATE FUNCTION horsies_monitoring_task_detail(
    p_task_id uuid,
    p_with_result boolean,
    p_with_attempts boolean
) RETURNS TABLE (
    location text,
    task_id uuid,
    task_name character varying,
    queue_name character varying,
    priority integer,
    status text,
    is_workflow_task boolean,
    retry_count integer,
    max_retries integer,
    sent_at timestamp with time zone,
    enqueued_at timestamp with time zone,
    claimed_at timestamp with time zone,
    started_at timestamp with time zone,
    good_until timestamp with time zone,
    worker_id character varying,
    worker_hostname character varying,
    worker_pid integer,
    worker_process_name character varying,
    next_retry_at timestamp with time zone,
    execution text,
    terminal_at timestamp with time zone,
    terminalization_kind text,
    error_code text,
    final_failed_reason text,
    result_envelope_version smallint,
    result_payload bytea,
    result_digest bytea,
    attempt_archive_version smallint,
    attempt_snapshot bytea,
    attempt_snapshot_digest bytea,
    workflow_id uuid,
    workflow_task_index integer,
    absence_reason text
)
    LANGUAGE plpgsql STABLE
AS $$
#variable_conflict use_column
DECLARE
    v_detail record;
    v_location text;
    v_row horsies_task_history;
BEGIN
    IF p_task_id IS NULL OR p_with_result IS NULL OR p_with_attempts IS NULL THEN
        RAISE EXCEPTION 'horsies_monitoring_task_detail: task id and both part flags are required'
            USING ERRCODE = 'null_value_not_allowed';
    END IF;

    RETURN QUERY
    SELECT 'LIVE'::text, t.id, t.task_name, t.queue_name, t.priority, t.status::text, t.is_workflow_task,
           t.retry_count, t.max_retries, t.sent_at, t.enqueued_at, t.claimed_at, t.started_at, t.good_until,
           t.claimed_by_worker_id, t.worker_hostname, t.worker_pid, t.worker_process_name, t.next_retry_at,
           t.execution, NULL::timestamptz, NULL::text, NULL::text, NULL::text,
           NULL::smallint, NULL::bytea, NULL::bytea,
           CASE WHEN p_with_attempts THEN 1::smallint END,
           CASE WHEN p_with_attempts THEN horsies_encode_task_attempts(t.id) END,
           NULL::bytea,
           link.workflow_id, link.task_index, NULL::text
    FROM horsies_tasks AS t
    LEFT JOIN horsies_workflow_tasks AS link ON link.task_id = t.id
    WHERE t.id = p_task_id;
    IF FOUND THEN
        RETURN;
    END IF;

    SELECT d.location, d.task_row INTO v_detail
    FROM horsies_task_detail_staged(p_task_id) AS d;
    IF FOUND THEN
        v_location := v_detail.location;
        v_row := v_detail.task_row;
        IF v_location IS DISTINCT FROM 'HISTORY' THEN
            RAISE EXCEPTION 'horsies_monitoring_task_detail: the published reader reports task % at location % after '
                'a live miss in the same snapshot', p_task_id, v_location
                USING ERRCODE = 'data_corrupted';
        END IF;
        RETURN QUERY
        SELECT 'HISTORY'::text, v_row.task_id, v_row.task_name, v_row.queue_name, v_row.priority,
               v_row.status::text, v_row.is_workflow_task, v_row.retry_count, v_row.max_retries, v_row.sent_at,
               v_row.enqueued_at, v_row.claimed_at, v_row.started_at, v_row.good_until,
               v_row.last_claimed_worker_id, v_row.last_worker_hostname, v_row.last_worker_pid,
               v_row.last_worker_process_name, NULL::timestamptz, NULL::text,
               v_row.terminal_at, v_row.terminalization_kind::text, v_row.error_code, v_row.final_failed_reason,
               CASE WHEN p_with_result THEN v_row.result_envelope_version END,
               CASE WHEN p_with_result THEN v_row.result_payload END,
               CASE WHEN p_with_result THEN v_row.result_digest END,
               CASE WHEN p_with_attempts THEN v_row.attempt_archive_version END,
               CASE WHEN p_with_attempts THEN v_row.attempt_snapshot END,
               CASE WHEN p_with_attempts THEN v_row.attempt_snapshot_digest END,
               link.workflow_id, link.task_index, NULL::text
        FROM (SELECT 1) AS one
        LEFT JOIN horsies_workflow_tasks AS link ON link.task_id = p_task_id;
        RETURN;
    END IF;

    RETURN QUERY
    SELECT 'ABSENT'::text, p_task_id, NULL::character varying, NULL::character varying, NULL::integer, NULL::text,
           NULL::boolean, NULL::integer, NULL::integer, NULL::timestamptz, NULL::timestamptz, NULL::timestamptz,
           NULL::timestamptz, NULL::timestamptz, NULL::character varying, NULL::character varying, NULL::integer,
           NULL::character varying, NULL::timestamptz, NULL::text, NULL::timestamptz, NULL::text, NULL::text,
           NULL::text, NULL::smallint, NULL::bytea, NULL::bytea, NULL::smallint, NULL::bytea, NULL::bytea,
           NULL::uuid, NULL::integer,
           horsies_task_absence_reason(p_task_id);
END;
$$;
