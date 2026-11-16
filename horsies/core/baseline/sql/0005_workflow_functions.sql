-- horsies baseline schema, part 5: the workflow engine functions.
--
-- One SQL function per workflow step. Each public function is one statement for its caller; it takes every lock,
-- writes every status and reads every fact that decides a transition. The caller (the core) passes only values
-- that are not stored yet: the workflow tree at start, the error values of the recovery bound, and the wake flag.
--
-- Rules that hold for every function of this file:
-- - Lock order: workflow rows (several rows in (depth, id) order: a parent before its children), then the task
--   advisory lock, then task rows, then node rows, then the phase-2 pending row, then the quarantine row. A function
--   that locks many task rows locks them in id order, or with SKIP LOCKED, and takes no advisory lock.
-- - Every function that changes node statuses for progression holds its workflow row FOR UPDATE first.
-- - Times that decide behaviour come from the database clock (now(), statement_timestamp()).
-- - The engine copies stored result envelopes byte for byte. The receiving task decodes them with its declared
--   type. SQL builds an envelope only for the engine errors, through horsies_task_error_envelope.
-- - Results are plain rows of built-in types. A step function returns SETOF horsies_workflow_step_row: one OUTCOME
--   row, and one CHILD_START row per owed child start with one SOURCE row per terminal dependency of that node.
-- - A task row that SQL mints has PENDING status, is_workflow_task TRUE, no command fingerprint (the named "no
--   fingerprint" variant), retain_rerun_input FALSE, retry_count 0, sent_at of its workflow, and max_retries,
--   execution, good_until and retention_class_key from its node row. Its id is horsies_uuidv7().
-- - Row-set arguments are parallel arrays of built-in types. A function refuses arrays of different lengths, a NULL
--   element where the column needs a value, and duplicate keys with SQLSTATE 22023.
-- - Every PL/pgSQL function that runs table statements uses generic plans (plan_cache_mode = force_generic_plan).
--   Each statement reads by an indexed key (workflow id, node index, task id), so the generic plan is the custom
--   plan, and planning at each call doubled the server time of a phase-2 step (one promoted task node: about 1.0 ms
--   with custom plans against 0.46 ms with generic plans, PostgreSQL 18, 10,000 nodes).
--
-- Part of migration 1000: the install concatenates the files of this folder in name order and computes one
-- checksum over the result.

------------------------------------------------------------------------------------------------------------------------
-- Step outcome rows
------------------------------------------------------------------------------------------------------------------------

-- The rows of a step function. row_kind decides which columns a row fills:
-- - OUTCOME (exactly one per call): disposition, workflow_id (the workflow of the step), workflow_status (after the
--   step), advance_child_workflow_id (a workflow that this step ended and that has a parent: the caller advances its
--   parent node after the commit with horsies_workflow_child_finish), detail.
-- - CHILD_START (one per owed child start): workflow_id (the parent workflow), task_index (the sub-workflow node),
--   node_input, args_from (JSON text or NULL), sub_workflow_name, sub_definition_key, parent_depth,
--   root_workflow_id.
-- - SOURCE (one per terminal dependency of a CHILD_START node): workflow_id, task_index (the CHILD_START node),
--   source_task_index, source_node_id, source_status, source_task_name, source_definition_key, source_result (the
--   stored text, copied), source_summary (the stored child summary text of a sub-workflow source).
-- A row type, not a function: no locks. Python: the composite horsies_phase2_disposition and the in-memory results of
-- the engine (core/workflows/engine.py); the rows replace the arrays of composite types of the first design.
CREATE TYPE horsies_workflow_step_row AS (
    row_kind text,
    disposition text,
    workflow_id uuid,
    workflow_status text,
    advance_child_workflow_id uuid,
    detail text,
    task_index integer,
    node_input text,
    args_from text,
    sub_workflow_name text,
    sub_definition_key text,
    parent_depth integer,
    root_workflow_id uuid,
    source_task_index integer,
    source_node_id text,
    source_status text,
    source_task_name text,
    source_definition_key text,
    source_result text,
    source_summary text
);

------------------------------------------------------------------------------------------------------------------------
-- Internal helpers: argument checks, envelopes, stored results
------------------------------------------------------------------------------------------------------------------------

-- Raises SQLSTATE 22023 with p_message when p_condition is not TRUE (FALSE or NULL). Returns nothing.
-- Locks: none. Python: the argument checks of the v61 batch functions (22023).
CREATE FUNCTION horsies_workflow_require(p_condition boolean, p_message text) RETURNS void
    LANGUAGE plpgsql IMMUTABLE
AS $$
BEGIN
    IF p_condition IS NOT TRUE THEN
        RAISE EXCEPTION '%', p_message USING ERRCODE = 'invalid_parameter_value';
    END IF;
END;
$$;

-- The stored text of an engine error: {"__h_task_result__":true,"ok":null,"err":{...}} with a built-in error code,
-- the shape of Python's serialize_error_payload (core/codec/error_payload.py:45-71). p_data is JSON text or NULL.
-- p_sub_workflow_id and p_sub_workflow_summary (JSON text) are set together, only for SUBWORKFLOW_FAILED: they are
-- top-level fields of the stored error, as SubWorkflowError.model_dump writes them (core/workflows/engine.py:3238-3281).
-- Refuses a code outside the engine's closed set, a NULL message, and a lone sub-workflow field (22023).
-- Locks: none. Returns the envelope text.
CREATE FUNCTION horsies_task_error_envelope(
    p_error_code text,
    p_message text,
    p_data text,
    p_sub_workflow_id uuid,
    p_sub_workflow_summary text
) RETURNS text
    LANGUAGE plpgsql IMMUTABLE
AS $$
BEGIN
    PERFORM horsies_workflow_require(
        p_error_code IN (
            'UPSTREAM_SKIPPED', 'WORKFLOW_OUTPUT_SKIPPED', 'WORKFLOW_SUCCESS_CASE_NOT_MET',
            'RESULT_DESERIALIZATION_ERROR', 'SUBWORKFLOW_FAILED', 'SUBWORKFLOW_DEPTH_LIMIT_REACHED',
            'WORKFLOW_ENQUEUE_FAILED'
        ),
        format('engine error code %L is not in the engine catalog', p_error_code)
    );
    PERFORM horsies_workflow_require(p_message IS NOT NULL, 'engine error message is NULL');
    PERFORM horsies_workflow_require(
        (p_sub_workflow_id IS NULL) = (p_sub_workflow_summary IS NULL)
        AND ((p_sub_workflow_id IS NOT NULL) = (p_error_code = 'SUBWORKFLOW_FAILED')),
        'sub_workflow_id and sub_workflow_summary are set together, only for SUBWORKFLOW_FAILED'
    );
    RETURN '{"__h_task_result__":true,"ok":null,"err":{"error_code":{"__builtin_task_code__":'
        || to_json(p_error_code)::text
        || '},"message":' || to_json(p_message)::text
        || ',"data":' || coalesce(p_data, 'null')
        || ',"exception":null'
        || CASE
            WHEN p_sub_workflow_id IS NULL THEN ''
            ELSE ',"sub_workflow_id":' || to_json(p_sub_workflow_id::text)::text
                || ',"sub_workflow_summary":' || p_sub_workflow_summary
        END
        || '}}';
END;
$$;

-- The err slot of an engine error envelope, as the bare error object text that the workflow error column stores.
-- Locks: none. Returns the error object text. Python: encode_value(TaskError(...)) for the workflow error column
-- (core/workflows/engine.py:3614-3627).
CREATE FUNCTION horsies_task_error_object(p_error_code text, p_message text, p_data text) RETURNS text
    LANGUAGE sql IMMUTABLE
AS $$
    SELECT (horsies_task_error_envelope(p_error_code, p_message, p_data, NULL, NULL)::json -> 'err')::text
$$;

-- p_text parsed as json, or NULL when it is NULL, empty, or not JSON. Used on cold paths only (completion, failure
-- policy, child completion): the parse runs in a subtransaction.
-- Locks: none. Returns json or NULL. Python: loads_json (core/codec/json_io.py) at the decode sites it serves.
CREATE FUNCTION horsies_workflow_json_or_null(p_text text) RETURNS json
    LANGUAGE plpgsql IMMUTABLE
AS $$
BEGIN
    IF p_text IS NULL OR p_text = '' THEN
        RETURN NULL;
    END IF;
    RETURN p_text::json;
EXCEPTION
    WHEN invalid_text_representation THEN
        RETURN NULL;
END;
$$;

-- The kind of a stored result text, by the strict envelope rule of Python's validate_task_result_envelope
-- (core/codec/typed.py:1133-1180): an object with exactly the keys __h_task_result__ (true), ok and err, and at most
-- one slot set. Returns:
-- - 'VALUE': a valid envelope with an empty err slot;
-- - 'ERROR': a valid envelope whose err slot is an object with an error_code;
-- - 'OUTPUTLESS': the outputless workflow envelope (__h_outputless_terminals__ true);
-- - 'ABSENT': NULL or empty text;
-- - 'UNREADABLE': anything else (not JSON, another shape, an err slot without an error_code).
-- Locks: none.
CREATE FUNCTION horsies_workflow_result_kind(p_text text) RETURNS text
    LANGUAGE plpgsql IMMUTABLE
AS $$
DECLARE
    v_value json := horsies_workflow_json_or_null(p_text);
    v_keys text[];
BEGIN
    IF p_text IS NULL OR p_text = '' THEN
        RETURN 'ABSENT';
    END IF;
    IF v_value IS NULL OR json_typeof(v_value) <> 'object' THEN
        RETURN 'UNREADABLE';
    END IF;
    IF json_typeof(v_value -> '__h_task_result__') IS DISTINCT FROM 'boolean'
        OR (v_value ->> '__h_task_result__') <> 'true' THEN
        RETURN 'UNREADABLE';
    END IF;
    v_keys := ARRAY(SELECT k FROM json_object_keys(v_value) AS k ORDER BY k);
    IF v_keys = ARRAY['__h_outputless_terminals__', '__h_task_result__', 'err', 'ok']
        AND (v_value ->> '__h_outputless_terminals__') = 'true' THEN
        RETURN 'OUTPUTLESS';
    END IF;
    IF v_keys IS DISTINCT FROM ARRAY['__h_task_result__', 'err', 'ok'] THEN
        RETURN 'UNREADABLE';
    END IF;
    IF json_typeof(v_value -> 'err') = 'null' THEN
        RETURN 'VALUE';
    END IF;
    IF json_typeof(v_value -> 'ok') <> 'null'
        OR json_typeof(v_value -> 'err') <> 'object'
        OR (v_value -> 'err' -> 'error_code') IS NULL THEN
        RETURN 'UNREADABLE';
    END IF;
    RETURN 'ERROR';
END;
$$;

-- The error of a stored node result, as the bare error object text, for the workflow error column. Python:
-- _decode_err_only (core/workflows/engine.py:284-337) and _recovered_failure_result
-- (core/workflows/phase2_recovery.py:233-292). Returns the err slot text unchanged for 'ERROR'; NULL for 'VALUE',
-- 'ABSENT' and 'OUTPUTLESS' (no error to report: the caller keeps the stored error); a RESULT_DESERIALIZATION_ERROR
-- object for 'UNREADABLE'.
-- Locks: none.
CREATE FUNCTION horsies_workflow_err_slot(p_result text) RETURNS text
    LANGUAGE plpgsql IMMUTABLE
AS $$
BEGIN
    CASE horsies_workflow_result_kind(p_result)
        WHEN 'ERROR' THEN
            RETURN (p_result::json -> 'err')::text;
        WHEN 'UNREADABLE' THEN
            RETURN horsies_task_error_object(
                'RESULT_DESERIALIZATION_ERROR', 'Stored task result does not decode as a task result envelope', NULL
            );
        WHEN 'VALUE', 'ABSENT', 'OUTPUTLESS' THEN
            RETURN NULL;
    END CASE;
END;
$$;

-- The value that a dependency gives a promoted node (the inner of an args_from envelope, an entry of the workflow
-- context). Python: get_dependency_results (core/workflows/engine.py:2762-2853).
-- - SKIPPED: UPSTREAM_SKIPPED with data {"dependency_index": <index>}.
-- - A stored result: the text, copied.
-- - COMPLETED or FAILED with no stored result: RESULT_DESERIALIZATION_ERROR naming the source node.
-- Locks: none. Returns the value text.
CREATE FUNCTION horsies_workflow_source_value(
    p_status text, p_result text, p_task_index integer, p_node_id text, p_task_name text
) RETURNS text
    LANGUAGE sql IMMUTABLE
AS $$
    SELECT CASE
        WHEN p_status = 'SKIPPED' THEN horsies_task_error_envelope(
            'UPSTREAM_SKIPPED', 'Upstream dependency was SKIPPED',
            '{"dependency_index":' || p_task_index || '}', NULL, NULL
        )
        WHEN p_result IS NOT NULL AND p_result <> '' THEN p_result
        ELSE horsies_task_error_envelope(
            'RESULT_DESERIALIZATION_ERROR',
            format(
                'Dependency node %s (index %s, task %s) is %s with no stored result',
                coalesce(p_node_id, '-'), p_task_index, p_task_name, p_status
            ),
            '{"dependency_index":' || p_task_index || '}', NULL, NULL
        )
    END
$$;

------------------------------------------------------------------------------------------------------------------------
-- Internal helpers: task rows of task nodes
------------------------------------------------------------------------------------------------------------------------

-- The input of the task row of an ENQUEUED task node: the node's static input object with these members appended,
-- in this order, as text operations on the stored bytes (no JSON value is rebuilt):
-- 1. one member per args_from key, in the stored key order of the jsonb map, for each source that is terminal:
--    {"__h_taskresult_envelope__":true,"source_task_name":...,"source_definition_key":...,"inner":<value>}, where
--    <value> is horsies_workflow_source_value; a source that is not terminal (join any or quorum) gives no member;
-- 2. "__h_workflow_ctx__" when the node has workflow_ctx_from: workflow_id, task_index, task_name, results_by_id,
--    task_name_by_id and definition_key_by_id over the terminal context nodes, and summaries_by_id (the stored
--    summary text as a JSON string) over the context sub-workflow nodes with a summary;
-- 3. "__h_workflow_meta__": workflow_id, task_index, task_name.
-- Python: _build_enqueued_task_params (core/workflows/engine.py:711-984) and _build_workflow_context_data
-- (core/workflows/engine.py:1629-1721), which decode and encode each value again; here the stored text is copied.
-- Precondition (asserted): the static input is a JSON object text. The workflow start writes it; no statement
-- changes it. A key conflict between args_from and the static input is refused before this function runs
-- (horsies_workflow_promote).
-- Locks: none (reads source nodes under the caller's workflow lock). Returns the input text.
CREATE FUNCTION horsies_workflow_task_input(p_node horsies_workflow_tasks) RETURNS text
    LANGUAGE plpgsql STABLE
    SET plan_cache_mode TO 'force_generic_plan'
AS $$
DECLARE
    v_members text[] := ARRAY[]::text[];
    v_member text;
    v_static text := rtrim(p_node.input, E' \t\n\r');
    v_body text;
BEGIN
    IF p_node.args_from IS NOT NULL THEN
        SELECT coalesce(array_agg(
            to_json(a.key)::text
            || ':{"__h_taskresult_envelope__":true,"source_task_name":' || to_json(s.task_name)::text
            || ',"source_definition_key":' || coalesce(to_json(s.sub_definition_key)::text, 'null')
            || ',"inner":' || horsies_workflow_source_value(s.status, s.result, s.task_index, s.node_id, s.task_name)
            || '}'
            ORDER BY a.position
        ), ARRAY[]::text[])
        INTO v_members
        FROM jsonb_each(p_node.args_from) WITH ORDINALITY AS a(key, value, position)
        -- One point read per key on (workflow_id, task_index): LATERAL keeps the plan a parameterized index probe
        -- whatever the statistics say, so a large workflow is never read whole per minted node.
        CROSS JOIN LATERAL (
            SELECT src.task_name, src.sub_definition_key, src.status, src.result, src.task_index, src.node_id
            FROM horsies_workflow_tasks src
            WHERE src.workflow_id = p_node.workflow_id
              AND src.task_index = (a.value #>> '{}')::integer
              AND src.status IN ('COMPLETED', 'FAILED', 'SKIPPED')
        ) AS s;
    END IF;

    IF p_node.workflow_ctx_from IS NOT NULL THEN
        SELECT '"__h_workflow_ctx__":{"workflow_id":' || to_json(p_node.workflow_id::text)::text
            || ',"task_index":' || p_node.task_index
            || ',"task_name":' || to_json(p_node.task_name)::text
            || ',"results_by_id":{' || coalesce(string_agg(
                to_json(s.node_id)::text || ':'
                || horsies_workflow_source_value(s.status, s.result, s.task_index, s.node_id, s.task_name),
                ',' ORDER BY s.task_index) FILTER (WHERE s.status IN ('COMPLETED', 'FAILED', 'SKIPPED')), '')
            || '},"task_name_by_id":{' || coalesce(string_agg(
                to_json(s.node_id)::text || ':' || to_json(s.task_name)::text,
                ',' ORDER BY s.task_index) FILTER (WHERE s.status IN ('COMPLETED', 'FAILED', 'SKIPPED')), '')
            || '},"definition_key_by_id":{' || coalesce(string_agg(
                to_json(s.node_id)::text || ':' || coalesce(to_json(s.sub_definition_key)::text, 'null'),
                ',' ORDER BY s.task_index) FILTER (WHERE s.status IN ('COMPLETED', 'FAILED', 'SKIPPED')), '')
            || '},"summaries_by_id":{' || coalesce(string_agg(
                to_json(s.node_id)::text || ':' || to_json(s.sub_workflow_summary)::text,
                ',' ORDER BY s.task_index) FILTER (WHERE s.is_subworkflow AND s.sub_workflow_summary IS NOT NULL), '')
            || '}}'
        INTO v_member
        FROM horsies_workflow_tasks s
        WHERE s.workflow_id = p_node.workflow_id
          AND s.task_index = ANY(p_node.dependencies)
          AND s.node_id = ANY(p_node.workflow_ctx_from);
        v_members := v_members || v_member;
    END IF;

    v_members := v_members || (
        '"__h_workflow_meta__":{"workflow_id":' || to_json(p_node.workflow_id::text)::text
        || ',"task_index":' || p_node.task_index
        || ',"task_name":' || to_json(p_node.task_name)::text || '}'
    );

    IF left(ltrim(v_static, E' \t\n\r'), 1) <> '{' OR right(v_static, 1) <> '}' THEN
        RAISE EXCEPTION 'workflow % node % input is not a JSON object text', p_node.workflow_id, p_node.task_index
            USING ERRCODE = 'data_corrupted';
    END IF;
    v_body := btrim(substr(ltrim(v_static, E' \t\n\r'), 2, length(ltrim(v_static, E' \t\n\r')) - 2), E' \t\n\r');
    RETURN '{' || v_body || CASE WHEN v_body = '' THEN '' ELSE ',' END || array_to_string(v_members, ',') || '}';
END;
$$;

-- Mints and links the task row of every task node of p_workflow_id whose index is in p_task_indexes and that is
-- ENQUEUED with no task: one INSERT into horsies_tasks (ids from horsies_uuidv7() in index order) and one link
-- UPDATE of the nodes. The insert trigger sends the wake of each queue. A conflict on a minted id is a defect and
-- fails the statement. Returns the number of rows minted.
-- Precondition: the caller holds the workflow row FOR UPDATE, so no other transaction links these nodes.
-- Python: the INSERT_AND_LINK_WORKFLOW_TASK_SQL executemany (core/workflows/sql.py:194-206;
-- core/workflows/engine.py:2406-2461), INSERT_TASK_FOR_WORKFLOW_SQL at start (core/workflows/lifecycle.py:758).
-- Locks: the new task rows and the linked node rows (the workflow row is held by the caller).
CREATE FUNCTION horsies_workflow_node_tasks_insert(p_workflow_id uuid, p_task_indexes integer[]) RETURNS integer
    LANGUAGE plpgsql
    SET plan_cache_mode TO 'force_generic_plan'
AS $$
DECLARE
    v_minted integer;
BEGIN
    IF p_task_indexes IS NULL OR cardinality(p_task_indexes) = 0 THEN
        RETURN 0;
    END IF;
    WITH built AS MATERIALIZED (
        SELECT n.id AS node_row_id,
               horsies_uuidv7() AS task_id,
               n.task_name, n.queue_name, n.priority,
               horsies_workflow_task_input(n) AS input,
               n.task_options, n.execution, n.retention_class_key, n.max_retries, n.good_until
        FROM horsies_workflow_tasks n
        WHERE n.workflow_id = p_workflow_id
          AND n.task_index = ANY(p_task_indexes)
          AND n.status = 'ENQUEUED'
          AND NOT n.is_subworkflow
          AND n.task_id IS NULL
        ORDER BY n.task_index
    ),
    inserted AS (
        INSERT INTO horsies_tasks (
            id, task_name, queue_name, priority, input, task_options, execution, retention_class_key, sent_at,
            created_at, command_fingerprint_version, command_fingerprint, idempotency_key_digest, status,
            enqueued_at, claimed_at, claimed_by_worker_id, claim_expires_at, started_at, finalizing_at,
            finalizing_by_worker_id, worker_pid, worker_hostname, worker_process_name, retry_count, max_retries,
            next_retry_at, good_until, is_workflow_task, updated_at, retain_rerun_input, rerun_of_task_id,
            rerun_root_task_id
        )
        SELECT b.task_id, b.task_name, b.queue_name, b.priority, b.input, b.task_options, b.execution,
               b.retention_class_key, w.sent_at,
               now(), NULL, NULL, NULL, 'PENDING',
               now(), NULL, NULL, NULL, NULL, NULL,
               NULL, NULL, NULL, NULL, 0, b.max_retries,
               NULL, b.good_until, TRUE, now(), FALSE, NULL,
               NULL
        FROM built b
        CROSS JOIN horsies_workflows w
        WHERE w.id = p_workflow_id
        RETURNING id
    )
    UPDATE horsies_workflow_tasks wt
    SET task_id = b.task_id
    FROM built b
    WHERE wt.id = b.node_row_id;
    GET DIAGNOSTICS v_minted = ROW_COUNT;
    RETURN v_minted;
END;
$$;

------------------------------------------------------------------------------------------------------------------------
-- Internal helpers: descendants, failure policy, promotion
------------------------------------------------------------------------------------------------------------------------

-- Moves the descendants of p_workflow_id that are in p_from_status to p_to_status, level by level, and returns their
-- ids. Each level reads the children of the rows that the previous level locked, so a child that a concurrent step
-- starts under a locked parent cannot be missed, and locks them FOR UPDATE in id order: with the levels in depth
-- order, rows are locked in (depth, id) order. Only children in p_from_status are followed, as Python's walks
-- (cascade_pause_to_children, core/workflows/lifecycle.py:996-1030; cascade_resume_to_children, :1265-1358).
-- The pairs are closed: RUNNING to PAUSED, PAUSED to RUNNING (22023 otherwise). Bounded by the depth limit (32).
-- Precondition: the caller holds p_workflow_id FOR UPDATE.
CREATE FUNCTION horsies_workflow_move_descendants(p_workflow_id uuid, p_from_status text, p_to_status text)
    RETURNS uuid[]
    LANGUAGE plpgsql
    SET plan_cache_mode TO 'force_generic_plan'
AS $$
DECLARE
    c_max_levels CONSTANT integer := 33;
    v_level uuid[] := ARRAY[p_workflow_id];
    v_next uuid[];
    v_all uuid[] := ARRAY[]::uuid[];
    v_levels integer := 0;
BEGIN
    PERFORM horsies_workflow_require(
        (p_from_status, p_to_status) IN (('RUNNING', 'PAUSED'), ('PAUSED', 'RUNNING')),
        format('descendant move %s to %s is not a workflow transition', p_from_status, p_to_status)
    );
    LOOP
        v_levels := v_levels + 1;
        IF v_levels > c_max_levels THEN
            RAISE EXCEPTION 'workflow % has descendants deeper than the depth limit', p_workflow_id
                USING ERRCODE = 'data_corrupted';
        END IF;
        WITH locked AS (
            SELECT w.id
            FROM horsies_workflows w
            WHERE w.parent_workflow_id = ANY(v_level)
              AND w.status = p_from_status
            ORDER BY w.id
            FOR UPDATE
        ),
        moved AS (
            UPDATE horsies_workflows w
            SET status = p_to_status, updated_at = now()
            FROM locked
            WHERE w.id = locked.id
              AND w.status = p_from_status
            RETURNING w.id
        )
        SELECT coalesce(array_agg(moved.id ORDER BY moved.id), ARRAY[]::uuid[]) INTO v_next FROM moved;
        EXIT WHEN cardinality(v_next) = 0;
        v_all := v_all || v_next;
        v_level := v_next;
    END LOOP;
    RETURN v_all;
END;
$$;

-- The failure policy of a workflow after one of its nodes went FAILED. Returns TRUE when progression goes on
-- (on_error fail, or a workflow that is not RUNNING with fail), FALSE when it stops (on_error pause).
-- - fail: the workflow error becomes the err slot of the first FAILED node by index, only while RUNNING; a node with
--   no readable error keeps the stored error.
-- - pause: RUNNING to PAUSED with the err slot of this node's stored result (a second failure keeps the first
--   error: the write needs RUNNING), and every RUNNING descendant to PAUSED. Claimed tasks of the paused tree are
--   abandoned later, at their start or claim-time check.
-- Precondition: the caller holds the workflow row FOR UPDATE and wrote the node FAILED.
-- Python: _handle_workflow_task_failure (core/workflows/engine.py:3990-4088).
-- Locks: the workflow row (held), then the RUNNING descendant rows level by level in (depth, id) order.
CREATE FUNCTION horsies_workflow_record_failure(p_workflow_id uuid, p_task_index integer) RETURNS boolean
    LANGUAGE plpgsql
    SET plan_cache_mode TO 'force_generic_plan'
AS $$
DECLARE
    v_on_error text;
    v_error text;
BEGIN
    SELECT w.on_error INTO v_on_error FROM horsies_workflows w WHERE w.id = p_workflow_id;
    CASE v_on_error
        WHEN 'fail' THEN
            SELECT horsies_workflow_err_slot(n.result) INTO v_error
            FROM horsies_workflow_tasks n
            WHERE n.workflow_id = p_workflow_id AND n.status = 'FAILED'
            ORDER BY n.task_index
            LIMIT 1;
            UPDATE horsies_workflows
            SET error = coalesce(v_error, error), updated_at = now()
            WHERE id = p_workflow_id AND status = 'RUNNING';
            RETURN TRUE;
        WHEN 'pause' THEN
            SELECT horsies_workflow_err_slot(n.result) INTO v_error
            FROM horsies_workflow_tasks n
            WHERE n.workflow_id = p_workflow_id AND n.task_index = p_task_index;
            UPDATE horsies_workflows
            SET status = 'PAUSED', error = coalesce(v_error, error), updated_at = now()
            WHERE id = p_workflow_id AND status = 'RUNNING';
            IF FOUND THEN
                PERFORM horsies_workflow_move_descendants(p_workflow_id, 'RUNNING', 'PAUSED');
            END IF;
            RETURN FALSE;
    END CASE;
END;
$$;

-- The nodes among p_task_indexes (ENQUEUED, of p_workflow_id) whose task row or child cannot be built, with the
-- engine error of each, in index order:
-- - a task node with an args_from key that is also a member of its static input: WORKFLOW_ENQUEUE_FAILED
--   (Python: _build_enqueued_task_params, core/workflows/engine.py:835-841; the spec refuses the overlap when it is
--   built, this check guards stored rows);
-- - a sub-workflow node of a workflow at depth 32: SUBWORKFLOW_DEPTH_LIMIT_REACHED (Python:
--   enqueue_subworkflow_task, core/workflows/engine.py:1052-1066).
-- data names the workflow and the node, as Python's _fail_enqueued_task (core/workflows/engine.py:472-555).
-- Locks: none. Returns (task_index, error_result) rows.
CREATE FUNCTION horsies_workflow_unbuildable_nodes(p_workflow_id uuid, p_task_indexes integer[])
    RETURNS TABLE (task_index integer, error_result text)
    LANGUAGE sql STABLE
AS $$
    SELECT n.task_index,
           horsies_task_error_envelope(
               CASE WHEN n.is_subworkflow THEN 'SUBWORKFLOW_DEPTH_LIMIT_REACHED' ELSE 'WORKFLOW_ENQUEUE_FAILED' END,
               CASE
                   WHEN n.is_subworkflow THEN format(
                       'Subworkflow not started: the child workflow would have depth %s, and the limit is 32 '
                       '(workflow %s:%s)', w.depth + 1, w.name, n.task_index)
                   ELSE format(
                       'args_from key %L conflicts with static kwarg (task %s)',
                       (SELECT k FROM jsonb_object_keys(n.args_from) AS k
                        WHERE n.input::jsonb ? k ORDER BY k LIMIT 1),
                       n.task_index)
               END,
               '{"workflow_id":' || to_json(p_workflow_id::text)::text || ',"task_index":' || n.task_index || '}',
               NULL, NULL
           )
    FROM horsies_workflow_tasks n
    JOIN horsies_workflows w ON w.id = n.workflow_id
    WHERE n.workflow_id = p_workflow_id
      AND n.task_index = ANY(p_task_indexes)
      AND n.status = 'ENQUEUED'
      AND (
          (n.is_subworkflow AND w.depth >= 32)
          OR (
              NOT n.is_subworkflow
              AND n.args_from IS NOT NULL
              AND n.input::jsonb ?| ARRAY(SELECT jsonb_object_keys(n.args_from))
          )
      )
    ORDER BY n.task_index
$$;

-- One level of promotion: the join verdict of every PENDING node of p_workflow_id that depends on a node of
-- p_sources or is in p_candidates, written in two updates. Returns the indexes that went SKIPPED (an impossible
-- join, or join all with a FAILED or SKIPPED dependency and no allow_failed_deps) and those that went ENQUEUED (a met
-- join). A node with workflow_ctx_from waits until its context nodes are terminal. The verdict table is Python's
-- _evaluate_join and _plan_level (core/workflows/engine.py:2223-2344); the ctx gate is
-- try_make_ready_and_enqueue's (core/workflows/engine.py:2671-2683).
-- Precondition: the caller holds the workflow row FOR UPDATE and the workflow is RUNNING.
-- Locks: the node rows that the two updates write (the workflow row is held by the caller).
CREATE FUNCTION horsies_workflow_promote_level(
    p_workflow_id uuid, p_sources integer[], p_candidates integer[],
    OUT skipped integer[], OUT enqueued integer[]
)
    LANGUAGE plpgsql
    SET plan_cache_mode TO 'force_generic_plan'
AS $$
BEGIN
    WITH evaluated AS (
        SELECT d.id,
               CASE d.join_type
                   WHEN 'all' THEN CASE WHEN c.terminal = c.total THEN 'MET' ELSE 'WAIT' END
                   WHEN 'any' THEN CASE
                       WHEN c.completed >= 1 THEN 'MET'
                       WHEN c.terminal = c.total THEN 'IMPOSSIBLE'
                       ELSE 'WAIT'
                   END
                   WHEN 'quorum' THEN CASE
                       WHEN c.completed >= d.min_success THEN 'MET'
                       WHEN c.completed + (c.total - c.terminal) < d.min_success THEN 'IMPOSSIBLE'
                       ELSE 'WAIT'
                   END
               END AS join_verdict,
               d.join_type = 'all' AND c.failed_or_skipped > 0 AND NOT d.allow_failed_deps AS skip_after_ready,
               d.workflow_ctx_from IS NOT NULL
                   AND c.ctx_terminal < cardinality(d.workflow_ctx_from) AS ctx_waiting
        FROM horsies_workflow_tasks d
        CROSS JOIN LATERAL (
            SELECT cardinality(d.dependencies) AS total,
                   count(*) FILTER (WHERE s.status IN ('COMPLETED', 'FAILED', 'SKIPPED')) AS terminal,
                   count(*) FILTER (WHERE s.status = 'COMPLETED') AS completed,
                   count(*) FILTER (WHERE s.status IN ('FAILED', 'SKIPPED')) AS failed_or_skipped,
                   count(*) FILTER (
                       WHERE s.status IN ('COMPLETED', 'FAILED', 'SKIPPED')
                         AND s.node_id = ANY(d.workflow_ctx_from)
                   ) AS ctx_terminal
            FROM horsies_workflow_tasks s
            WHERE s.workflow_id = d.workflow_id
              AND s.task_index = ANY(d.dependencies)
        ) c
        WHERE d.workflow_id = p_workflow_id
          AND d.status = 'PENDING'
          AND cardinality(d.dependencies) > 0
          AND (d.dependencies && p_sources OR d.task_index = ANY(p_candidates))
    ),
    skip_write AS (
        UPDATE horsies_workflow_tasks n
        SET status = 'SKIPPED', completed_at = now()
        FROM evaluated e
        WHERE n.id = e.id
          AND n.status = 'PENDING'
          AND (e.join_verdict = 'IMPOSSIBLE' OR (e.join_verdict = 'MET' AND NOT e.ctx_waiting AND e.skip_after_ready))
        RETURNING n.task_index
    ),
    enqueue_write AS (
        UPDATE horsies_workflow_tasks n
        SET status = 'ENQUEUED', started_at = now()
        FROM evaluated e
        WHERE n.id = e.id
          AND n.status = 'PENDING'
          AND e.join_verdict = 'MET' AND NOT e.ctx_waiting AND NOT e.skip_after_ready
        RETURNING n.task_index
    )
    SELECT (SELECT coalesce(array_agg(s.task_index ORDER BY s.task_index), ARRAY[]::integer[]) FROM skip_write s),
           (SELECT coalesce(array_agg(q.task_index ORDER BY q.task_index), ARRAY[]::integer[]) FROM enqueue_write q)
    INTO skipped, enqueued;
END;
$$;

-- Writes FAILED, with its engine error, every node among p_task_indexes that horsies_workflow_unbuildable_nodes
-- names; with p_on_error pause only the lowest index (the pause then stops the step). Returns the failed indexes in
-- order. Precondition: the caller holds the workflow row FOR UPDATE.
-- Locks: the node rows it writes. Python: _fail_enqueued_task (core/workflows/engine.py:472-555) per failed build.
CREATE FUNCTION horsies_workflow_fail_unbuildable(p_workflow_id uuid, p_task_indexes integer[], p_on_error text)
    RETURNS integer[]
    LANGUAGE plpgsql
    SET plan_cache_mode TO 'force_generic_plan'
AS $$
DECLARE
    v_failed integer[];
BEGIN
    WITH unbuildable AS (
        SELECT u.task_index, u.error_result
        FROM horsies_workflow_unbuildable_nodes(p_workflow_id, p_task_indexes) AS u
        ORDER BY u.task_index
        LIMIT CASE WHEN p_on_error = 'pause' THEN 1 END
    ),
    failed AS (
        UPDATE horsies_workflow_tasks n
        SET status = 'FAILED', result = u.error_result, completed_at = now()
        FROM unbuildable u
        WHERE n.workflow_id = p_workflow_id AND n.task_index = u.task_index AND n.status = 'ENQUEUED'
        RETURNING n.task_index
    )
    SELECT coalesce(array_agg(failed.task_index ORDER BY failed.task_index), ARRAY[]::integer[])
    INTO v_failed FROM failed;
    RETURN v_failed;
END;
$$;

-- The owed child starts among p_task_indexes: one CHILD_START row per ENQUEUED sub-workflow node with no child, then
-- one SOURCE row per terminal dependency of each (the stored result and summary texts, copied).
-- Locks: none. Returns CHILD_START and SOURCE rows. Python: the reads of enqueue_subworkflow_task before build_with
-- (core/workflows/engine.py:1032-1232).
CREATE FUNCTION horsies_workflow_child_start_rows(p_workflow_id uuid, p_task_indexes integer[])
    RETURNS SETOF horsies_workflow_step_row
    LANGUAGE sql STABLE
AS $$
    SELECT 'CHILD_START'::text, NULL::text, p_workflow_id, NULL::text, NULL::uuid, NULL::text,
           n.task_index, n.input, n.args_from::text, n.sub_workflow_name::text, n.sub_definition_key::text,
           w.depth, coalesce(w.root_workflow_id, w.id),
           NULL::integer, NULL::text, NULL::text, NULL::text, NULL::text, NULL::text, NULL::text
    FROM horsies_workflow_tasks n
    JOIN horsies_workflows w ON w.id = n.workflow_id
    WHERE n.workflow_id = p_workflow_id
      AND n.task_index = ANY(p_task_indexes)
      AND n.status = 'ENQUEUED'
      AND n.is_subworkflow
      AND n.sub_workflow_id IS NULL
    UNION ALL
    SELECT 'SOURCE'::text, NULL::text, p_workflow_id, NULL::text, NULL::uuid, NULL::text,
           n.task_index, NULL::text, NULL::text, NULL::text, NULL::text, NULL::integer, NULL::uuid,
           s.task_index, s.node_id::text, s.status::text, s.task_name::text, s.sub_definition_key::text,
           s.result, s.sub_workflow_summary
    FROM horsies_workflow_tasks n
    JOIN horsies_workflow_tasks s
      ON s.workflow_id = n.workflow_id
     AND s.task_index = ANY(n.dependencies)
     AND s.status IN ('COMPLETED', 'FAILED', 'SKIPPED')
    WHERE n.workflow_id = p_workflow_id
      AND n.task_index = ANY(p_task_indexes)
      AND n.status = 'ENQUEUED'
      AND n.is_subworkflow
      AND n.sub_workflow_id IS NULL
$$;

-- Promotion inside one held workflow lock, with the whole skip cascade. Inputs: p_ready_indexes are READY nodes to
-- enqueue as they are (resume, start roots); p_candidate_indexes are PENDING nodes to judge (resume); p_source_indexes
-- are nodes that just went terminal, whose PENDING dependents are judged (phase 2, child completion). Steps:
-- 1. READY nodes of p_ready_indexes to ENQUEUED (started_at = now()).
-- 2. Levels of horsies_workflow_promote_level; the nodes that a level skipped, and the nodes whose build fails
--    (horsies_workflow_unbuildable_nodes), are the sources of the next level. A build failure writes the node FAILED
--    with its engine error and runs the failure policy: with on_error fail every failure of the level goes FAILED;
--    with on_error pause only the lowest index goes FAILED and the workflow pauses. The levels end when a level
--    makes no node terminal; each level makes at least one node terminal, so there are at most node count + 1.
-- 3. A pause in step 2: every node that this call moved to ENQUEUED and that has no task and no child goes back to
--    READY with started_at NULL; nothing is minted and no child start is owed (the pause gate).
-- 4. Otherwise: the task rows of the ENQUEUED task nodes (horsies_workflow_node_tasks_insert), and one CHILD_START
--    row (with its SOURCE rows) per ENQUEUED sub-workflow node.
-- Does nothing for a workflow that is not RUNNING.
-- Precondition: the caller holds the workflow row FOR UPDATE.
-- Lock order: workflow row (held), node rows of each level, new task rows.
-- Python: _process_dependents, _promote_dependents_level, _write_promotions, _enqueue_promoted,
-- try_make_ready_and_enqueue, enqueue_workflow_task (core/workflows/engine.py:634-710, 2062-2738).
-- Returns: the CHILD_START and SOURCE rows of the owed child starts (no OUTCOME row).
CREATE FUNCTION horsies_workflow_promote(
    p_workflow_id uuid,
    p_source_indexes integer[],
    p_candidate_indexes integer[],
    p_ready_indexes integer[]
) RETURNS SETOF horsies_workflow_step_row
    LANGUAGE plpgsql
    SET plan_cache_mode TO 'force_generic_plan'
AS $$
DECLARE
    c_max_levels CONSTANT integer := 10002;
    v_wf record;
    v_sources integer[] := coalesce(p_source_indexes, ARRAY[]::integer[]);
    v_candidates integer[] := coalesce(p_candidate_indexes, ARRAY[]::integer[]);
    v_new integer[] := ARRAY[]::integer[];
    v_enqueued integer[] := ARRAY[]::integer[];
    v_failed integer[];
    v_level record;
    v_levels integer := 0;
BEGIN
    SELECT w.status, w.on_error
    INTO v_wf
    FROM horsies_workflows w
    WHERE w.id = p_workflow_id;
    IF NOT FOUND OR v_wf.status <> 'RUNNING' THEN
        RETURN;
    END IF;

    IF cardinality(p_ready_indexes) > 0 THEN
        WITH moved AS (
            UPDATE horsies_workflow_tasks n
            SET status = 'ENQUEUED', started_at = now()
            WHERE n.workflow_id = p_workflow_id AND n.task_index = ANY(p_ready_indexes) AND n.status = 'READY'
            RETURNING n.task_index
        )
        SELECT coalesce(array_agg(moved.task_index ORDER BY moved.task_index), ARRAY[]::integer[])
        INTO v_new FROM moved;
    END IF;

    LOOP
        v_levels := v_levels + 1;
        IF v_levels > c_max_levels THEN
            RAISE EXCEPTION 'workflow % promotion passed % levels', p_workflow_id, c_max_levels
                USING ERRCODE = 'program_limit_exceeded';
        END IF;
        IF cardinality(v_new) > 0 THEN
            v_enqueued := v_enqueued || v_new;
            v_failed := horsies_workflow_fail_unbuildable(p_workflow_id, v_new, v_wf.on_error);
            IF cardinality(v_failed) > 0 THEN
                IF NOT horsies_workflow_record_failure(p_workflow_id, v_failed[1]) THEN
                    UPDATE horsies_workflow_tasks n
                    SET status = 'READY', started_at = NULL
                    WHERE n.workflow_id = p_workflow_id
                      AND n.task_index = ANY(v_enqueued)
                      AND n.status = 'ENQUEUED'
                      AND n.task_id IS NULL
                      AND n.sub_workflow_id IS NULL;
                    RETURN;
                END IF;
                v_sources := v_sources || v_failed;
            END IF;
        END IF;
        EXIT WHEN cardinality(v_sources) = 0 AND cardinality(v_candidates) = 0;
        SELECT * INTO v_level FROM horsies_workflow_promote_level(p_workflow_id, v_sources, v_candidates);
        v_new := v_level.enqueued;
        v_sources := v_level.skipped;
        v_candidates := ARRAY[]::integer[];
    END LOOP;

    PERFORM horsies_workflow_node_tasks_insert(p_workflow_id, v_enqueued);
    RETURN QUERY SELECT * FROM horsies_workflow_child_start_rows(p_workflow_id, v_enqueued);
END;
$$;

------------------------------------------------------------------------------------------------------------------------
-- Internal helpers: completion
------------------------------------------------------------------------------------------------------------------------

-- The data of an output error: the workflow and the output node.
-- Locks: none. Returns JSON text. Python: the data of _output_error_envelope (core/workflows/engine.py:3728-3758).
CREATE FUNCTION horsies_workflow_output_error_data(
    p_workflow_id uuid, p_node_id text, p_task_index integer, p_task_name text
) RETURNS text
    LANGUAGE sql IMMUTABLE
AS $$
    SELECT '{"workflow_id":' || to_json(p_workflow_id::text)::text
        || ',"output_node_id":' || coalesce(to_json(p_node_id)::text, 'null')
        || ',"output_task_index":' || p_task_index
        || ',"output_task_name":' || to_json(p_task_name)::text || '}'
$$;

-- The stored result of a COMPLETED workflow with an explicit output node. Python: _explicit_output_result and
-- _stored_output_result (core/workflows/engine.py:3772-3868).
-- - SKIPPED output node: WORKFLOW_OUTPUT_SKIPPED.
-- - COMPLETED or FAILED with a stored result: that text, copied.
-- - COMPLETED or FAILED with a NULL or empty result: RESULT_DESERIALIZATION_ERROR.
-- An absent output row, or an output node that is not terminal, is a defect (the start writes the row; the caller
-- counted no live node): it raises with SQLSTATE XX001 (data_corrupted).
-- Locks: none. Returns the result text.
CREATE FUNCTION horsies_workflow_explicit_output(p_workflow_id uuid, p_output_task_index integer) RETURNS text
    LANGUAGE plpgsql STABLE
    SET plan_cache_mode TO 'force_generic_plan'
AS $$
DECLARE
    v_node record;
BEGIN
    SELECT n.node_id, n.task_name, n.status, n.result INTO v_node
    FROM horsies_workflow_tasks n
    WHERE n.workflow_id = p_workflow_id AND n.task_index = p_output_task_index;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'workflow % has output_task_index % and no node at that index',
            p_workflow_id, p_output_task_index USING ERRCODE = 'data_corrupted';
    END IF;
    CASE v_node.status
        WHEN 'SKIPPED' THEN
            RETURN horsies_task_error_envelope(
                'WORKFLOW_OUTPUT_SKIPPED',
                format('Workflow %s is COMPLETED and has no output: its output node %L is SKIPPED',
                       p_workflow_id, v_node.node_id),
                horsies_workflow_output_error_data(p_workflow_id, v_node.node_id, p_output_task_index, v_node.task_name),
                NULL, NULL
            );
        WHEN 'COMPLETED', 'FAILED' THEN
            IF v_node.result IS NOT NULL AND v_node.result <> '' THEN
                RETURN v_node.result;
            END IF;
            RETURN horsies_task_error_envelope(
                'RESULT_DESERIALIZATION_ERROR',
                format('Workflow %s is COMPLETED and its output node %L is %s with no stored result',
                       p_workflow_id, v_node.node_id, v_node.status),
                horsies_workflow_output_error_data(p_workflow_id, v_node.node_id, p_output_task_index, v_node.task_name),
                NULL, NULL
            );
        ELSE
            RAISE EXCEPTION 'workflow % output node % is % at completion', p_workflow_id, p_output_task_index,
                v_node.status USING ERRCODE = 'data_corrupted';
    END CASE;
END;
$$;

-- The stored result of a COMPLETED workflow with no output node: the outputless envelope
-- {"__h_task_result__":true,"__h_outputless_terminals__":true,"ok":{"results_by_id":...,"task_name_by_id":...,
-- "definition_key_by_id":...},"err":null} over the terminal nodes (no node depends on them) that have a node_id.
-- An entry is the stored envelope text, WORKFLOW_OUTPUT_SKIPPED for a SKIPPED node, or null for a result that is
-- absent or not an envelope. The terminal set is the node indexes minus the union of all dependency arrays, from two
-- array aggregates: no correlated probe per row. All stored results are parsed in one statement; only when one of
-- them is not JSON does each row get a parse of its own. Python: get_workflow_final_result, _terminal_entry,
-- _compute_terminal_indexes (core/workflows/engine.py:3712-3727, 3870-3988).
-- Locks: none. Returns the envelope text.
CREATE FUNCTION horsies_workflow_outputless_result(p_workflow_id uuid) RETURNS text
    LANGUAGE plpgsql STABLE
    SET plan_cache_mode TO 'force_generic_plan'
AS $$
DECLARE
    v_all integer[];
    v_referenced integer[];
    v_terminal integer[];
    v_all_json boolean := TRUE;
    v_result text;
BEGIN
    SELECT array_agg(n.task_index) INTO v_all FROM horsies_workflow_tasks n WHERE n.workflow_id = p_workflow_id;
    SELECT array_agg(DISTINCT d.dependency) INTO v_referenced
    FROM horsies_workflow_tasks n, unnest(n.dependencies) AS d(dependency)
    WHERE n.workflow_id = p_workflow_id;
    v_terminal := ARRAY(
        SELECT unnest(coalesce(v_all, ARRAY[]::integer[]))
        EXCEPT
        SELECT unnest(coalesce(v_referenced, ARRAY[]::integer[]))
    );
    BEGIN
        PERFORM count(n.result::json)
        FROM horsies_workflow_tasks n
        WHERE n.workflow_id = p_workflow_id AND n.task_index = ANY(v_terminal) AND n.result <> '';
    EXCEPTION
        WHEN invalid_text_representation THEN
            v_all_json := FALSE;
    END;
    WITH entries AS (
        SELECT n.task_index, n.node_id, n.task_name, n.sub_definition_key,
               CASE
                   WHEN n.status = 'SKIPPED' THEN horsies_task_error_envelope(
                       'WORKFLOW_OUTPUT_SKIPPED',
                       format('Workflow %s is COMPLETED and its terminal node %L is SKIPPED', p_workflow_id, n.node_id),
                       horsies_workflow_output_error_data(p_workflow_id, n.node_id, n.task_index, n.task_name),
                       NULL, NULL)
                   WHEN n.result IS NULL OR n.result = '' THEN 'null'
                   WHEN v_all_json
                        AND json_typeof(n.result::json) = 'object'
                        AND json_typeof(n.result::json -> '__h_task_result__') = 'boolean'
                        AND (n.result::json ->> '__h_task_result__') = 'true' THEN n.result
                   WHEN NOT v_all_json
                        AND json_typeof(horsies_workflow_json_or_null(n.result)) = 'object'
                        AND json_typeof(horsies_workflow_json_or_null(n.result) -> '__h_task_result__') = 'boolean'
                        AND (horsies_workflow_json_or_null(n.result) ->> '__h_task_result__') = 'true' THEN n.result
                   ELSE 'null'
               END AS entry
        FROM horsies_workflow_tasks n
        WHERE n.workflow_id = p_workflow_id AND n.task_index = ANY(v_terminal) AND n.node_id IS NOT NULL
    )
    SELECT '{"__h_task_result__":true,"__h_outputless_terminals__":true,"ok":{"results_by_id":{'
        || coalesce(string_agg(to_json(e.node_id)::text || ':' || e.entry, ',' ORDER BY e.task_index), '')
        || '},"task_name_by_id":{'
        || coalesce(string_agg(to_json(e.node_id)::text || ':' || to_json(e.task_name)::text, ','
                               ORDER BY e.task_index), '')
        || '},"definition_key_by_id":{'
        || coalesce(string_agg(to_json(e.node_id)::text || ':' || coalesce(to_json(e.sub_definition_key)::text, 'null'),
                               ',' ORDER BY e.task_index), '')
        || '}},"err":null}'
    INTO v_result
    FROM entries e;
    RETURN v_result;
END;
$$;

-- Why an array of node indexes of a stored success policy does not read, or NULL when it reads. It reads when it is
-- a JSON array whose items are integers from 0. jsonb keeps the written form of a number: 1.0 is not an index, and
-- 1e2 is stored as 100, which is one. Python's reader decodes the same jsonb text, so it sees the same numbers.
-- The cause text is the one of Python's read_stored_success_policy (core/workflows/stored_success_policy.py).
-- Precondition: p_indices is not SQL NULL (the caller checks that the key is present).
-- Locks: none.
CREATE FUNCTION horsies_workflow_policy_indices_cause(p_indices jsonb) RETURNS text
    LANGUAGE plpgsql IMMUTABLE
AS $$
DECLARE
    v_cause text;
BEGIN
    IF jsonb_typeof(p_indices) <> 'array' THEN
        RETURN format('a JSON %s, not an array', jsonb_typeof(p_indices));
    END IF;
    SELECT format('item %s %s', i.position - 1, CASE
               WHEN jsonb_typeof(i.item) = 'number' AND (i.item #>> '{}') ~ '^-[0-9]+$' THEN 'is below 0'
               ELSE format('is a JSON %s, not an index', jsonb_typeof(i.item))
           END)
    INTO v_cause
    FROM jsonb_array_elements(p_indices) WITH ORDINALITY AS i(item, position)
    WHERE jsonb_typeof(i.item) <> 'number' OR (i.item #>> '{}') !~ '^[0-9]+$'
    ORDER BY i.position
    LIMIT 1;
    RETURN v_cause;
END;
$$;

-- The success cases of a workflow's stored success policy (horsies_workflows.success_policy), read by the rule of
-- Python's read_stored_success_policy (core/workflows/stored_success_policy.py). Returns:
-- - NULL for no policy: SQL NULL or JSON null;
-- - the "cases" array for the form the workflow start stores: an object whose "cases" is an array of objects, each
--   with "required_indices", an array of integers from 0; "optional_indices", when present and not null, is the
--   same kind of array. Other keys are ignored.
-- Any other value raises SQLSTATE XX001 (data_corrupted) with the text
-- 'stored success_policy of workflow <id> does not parse: <cause>'. The step that reads it fails and its
-- transaction rolls back; the raise is persistent, so the workflow recovery limit counts it
-- (horsies_workflow_phase2_record_raise) and fails the workflow closed. No reader falls back to the default rule.
-- Locks: none.
CREATE FUNCTION horsies_workflow_success_cases(p_workflow_id uuid, p_success_policy jsonb) RETURNS jsonb
    LANGUAGE plpgsql IMMUTABLE
AS $$
DECLARE
    v_cause text;
BEGIN
    CASE
        WHEN p_success_policy IS NULL OR jsonb_typeof(p_success_policy) = 'null' THEN
            RETURN NULL;
        WHEN jsonb_typeof(p_success_policy) <> 'object' THEN
            v_cause := format('the policy is a JSON %s, not an object', jsonb_typeof(p_success_policy));
        WHEN NOT p_success_policy ? 'cases' THEN
            v_cause := 'the policy has no cases';
        WHEN jsonb_typeof(p_success_policy -> 'cases') <> 'array' THEN
            v_cause := format('cases is a JSON %s, not an array', jsonb_typeof(p_success_policy -> 'cases'));
        ELSE
            SELECT format('cases[%s]: %s', c.position - 1, CASE
                       WHEN jsonb_typeof(c.success_case) <> 'object'
                           THEN format('the case is a JSON %s, not an object', jsonb_typeof(c.success_case))
                       WHEN NOT c.success_case ? 'required_indices' THEN 'the case has no required_indices'
                       ELSE 'required_indices: '
                           || horsies_workflow_policy_indices_cause(c.success_case -> 'required_indices')
                   END)
            INTO v_cause
            FROM jsonb_array_elements(p_success_policy -> 'cases') WITH ORDINALITY AS c(success_case, position)
            WHERE jsonb_typeof(c.success_case) <> 'object'
               OR NOT c.success_case ? 'required_indices'
               OR horsies_workflow_policy_indices_cause(c.success_case -> 'required_indices') IS NOT NULL
            ORDER BY c.position
            LIMIT 1;
            IF v_cause IS NULL
               AND coalesce(jsonb_typeof(p_success_policy -> 'optional_indices'), 'null') <> 'null' THEN
                v_cause := 'optional_indices: '
                    || horsies_workflow_policy_indices_cause(p_success_policy -> 'optional_indices');
            END IF;
    END CASE;
    IF v_cause IS NOT NULL THEN
        RAISE EXCEPTION 'stored success_policy of workflow % does not parse: %', p_workflow_id, v_cause
            USING ERRCODE = 'data_corrupted';
    END IF;
    RETURN p_success_policy -> 'cases';
END;
$$;

-- The error of a workflow that completion fails, as the bare error object text, or NULL to keep the stored error.
-- Python: get_workflow_failure_error (core/workflows/engine.py). The stored policy is read with
-- horsies_workflow_success_cases; one that does not parse raises XX001.
-- - No success policy: the err slot of the first FAILED node by index (NULL when it has no readable error).
-- - A policy: the err slot of the first FAILED node by index that a case requires; WORKFLOW_SUCCESS_CASE_NOT_MET when
--   there is none or it has no readable error. Indexes are compared as jsonb numbers, so an index above the integer
--   range names no node.
-- Locks: none. Returns the error object text or NULL.
CREATE FUNCTION horsies_workflow_failure_error(p_workflow_id uuid, p_success_policy jsonb) RETURNS text
    LANGUAGE plpgsql STABLE
    SET plan_cache_mode TO 'force_generic_plan'
AS $$
DECLARE
    v_cases jsonb := horsies_workflow_success_cases(p_workflow_id, p_success_policy);
    v_required jsonb;
    v_error text;
BEGIN
    IF v_cases IS NULL THEN
        SELECT horsies_workflow_err_slot(n.result) INTO v_error
        FROM horsies_workflow_tasks n
        WHERE n.workflow_id = p_workflow_id AND n.status = 'FAILED'
        ORDER BY n.task_index
        LIMIT 1;
        RETURN v_error;
    END IF;
    SELECT coalesce(jsonb_agg(r.value), '[]'::jsonb) INTO v_required
    FROM jsonb_array_elements(v_cases) AS c(success_case)
    CROSS JOIN LATERAL jsonb_array_elements(c.success_case -> 'required_indices') AS r(value);
    SELECT horsies_workflow_err_slot(n.result) INTO v_error
    FROM horsies_workflow_tasks n
    WHERE n.workflow_id = p_workflow_id AND n.status = 'FAILED' AND v_required @> to_jsonb(n.task_index)
    ORDER BY n.task_index
    LIMIT 1;
    RETURN coalesce(
        v_error,
        horsies_task_error_object('WORKFLOW_SUCCESS_CASE_NOT_MET', 'No success case was satisfied', NULL)
    );
END;
$$;

-- Finalizes a RUNNING workflow with no live node, once. Returns the status it wrote (COMPLETED or FAILED), or NULL
-- when the workflow is not RUNNING or has a live node (PENDING, READY, ENQUEUED, RUNNING).
-- The stored policy is read with horsies_workflow_success_cases, once no node is live; one that does not parse raises
-- XX001 and nothing is written.
-- - No success policy: COMPLETED when no node FAILED and no error is stored; else FAILED.
-- - A policy: COMPLETED when every required node of some case with required nodes is COMPLETED; else FAILED.
--   Indexes are compared as jsonb numbers, so an index above the integer range names no node and its case is not met.
-- COMPLETED stores the output (horsies_workflow_explicit_output or horsies_workflow_outputless_result); FAILED
-- stores no result and the error of horsies_workflow_failure_error, or keeps the stored error. Both set
-- completed_at. The status trigger sends workflow_done at commit.
-- Precondition: the caller holds the workflow row FOR UPDATE.
-- Python: check_workflow_completion and evaluate_workflow_success (core/workflows/engine.py).
-- Locks: the workflow row (held by the caller); reads the nodes.
CREATE FUNCTION horsies_workflow_try_complete(p_workflow_id uuid) RETURNS text
    LANGUAGE plpgsql
    SET plan_cache_mode TO 'force_generic_plan'
AS $$
DECLARE
    v_wf record;
    v_live bigint;
    v_failed bigint;
    v_cases jsonb;
    v_completed jsonb;
    v_succeeded boolean;
BEGIN
    SELECT w.status, w.success_policy, w.output_task_index, w.error INTO v_wf
    FROM horsies_workflows w
    WHERE w.id = p_workflow_id;
    IF NOT FOUND OR v_wf.status <> 'RUNNING' THEN
        RETURN NULL;
    END IF;
    SELECT count(*) FILTER (WHERE n.status IN ('PENDING', 'READY', 'ENQUEUED', 'RUNNING')),
           count(*) FILTER (WHERE n.status = 'FAILED')
    INTO v_live, v_failed
    FROM horsies_workflow_tasks n
    WHERE n.workflow_id = p_workflow_id;
    IF v_live > 0 THEN
        RETURN NULL;
    END IF;

    v_cases := horsies_workflow_success_cases(p_workflow_id, v_wf.success_policy);
    IF v_cases IS NULL THEN
        v_succeeded := v_wf.error IS NULL AND v_failed = 0;
    ELSE
        -- The COMPLETED node indexes as one jsonb array; a case is met when it contains the case's indexes.
        SELECT coalesce(jsonb_agg(n.task_index), '[]'::jsonb) INTO v_completed
        FROM horsies_workflow_tasks n
        WHERE n.workflow_id = p_workflow_id AND n.status = 'COMPLETED';
        v_succeeded := EXISTS (
            SELECT 1
            FROM jsonb_array_elements(v_cases) AS c(success_case)
            WHERE jsonb_array_length(c.success_case -> 'required_indices') > 0
              AND (c.success_case -> 'required_indices') <@ v_completed
        );
    END IF;

    IF v_succeeded THEN
        UPDATE horsies_workflows
        SET status = 'COMPLETED',
            result = CASE
                WHEN v_wf.output_task_index IS NULL THEN horsies_workflow_outputless_result(p_workflow_id)
                ELSE horsies_workflow_explicit_output(p_workflow_id, v_wf.output_task_index)
            END,
            completed_at = now(),
            updated_at = now()
        WHERE id = p_workflow_id AND status = 'RUNNING';
        RETURN 'COMPLETED';
    END IF;
    UPDATE horsies_workflows
    SET status = 'FAILED',
        result = NULL,
        error = coalesce(horsies_workflow_failure_error(p_workflow_id, v_wf.success_policy), error),
        completed_at = now(),
        updated_at = now()
    WHERE id = p_workflow_id AND status = 'RUNNING';
    RETURN 'FAILED';
END;
$$;

-- Everything a workflow does after one of its nodes was written terminal: the failure policy for a FAILED node
-- (horsies_workflow_record_failure; a pause stops here), the paused guard, promotion of the node's dependents
-- (skipped when p_dependents is 'NONE': the caller knows that no node depends on this one), and the completion
-- check. Returns the owed child starts. p_dependents is 'NONE' or 'READ' (22023 otherwise).
-- Precondition: the caller holds the workflow row FOR UPDATE and wrote the node.
-- Python: apply_node_progression (core/workflows/engine.py:1995-2060).
-- Locks: as horsies_workflow_record_failure and horsies_workflow_promote, under the caller's workflow lock.
CREATE FUNCTION horsies_workflow_progress(p_workflow_id uuid, p_task_index integer, p_dependents text)
    RETURNS SETOF horsies_workflow_step_row
    LANGUAGE plpgsql
    SET plan_cache_mode TO 'force_generic_plan'
AS $$
DECLARE
    v_node_status text;
BEGIN
    PERFORM horsies_workflow_require(p_dependents IN ('NONE', 'READ'), 'dependents must be NONE or READ');
    SELECT n.status INTO v_node_status
    FROM horsies_workflow_tasks n
    WHERE n.workflow_id = p_workflow_id AND n.task_index = p_task_index;
    IF v_node_status = 'FAILED' THEN
        IF NOT horsies_workflow_record_failure(p_workflow_id, p_task_index) THEN
            RETURN;
        END IF;
    END IF;
    IF NOT EXISTS (SELECT 1 FROM horsies_workflows w WHERE w.id = p_workflow_id AND w.status = 'RUNNING') THEN
        RETURN;
    END IF;
    IF p_dependents = 'READ' THEN
        RETURN QUERY SELECT * FROM horsies_workflow_promote(p_workflow_id, ARRAY[p_task_index], NULL, NULL);
    END IF;
    PERFORM horsies_workflow_try_complete(p_workflow_id);
END;
$$;

-- The OUTCOME row of a step on p_workflow_id. advance_child_workflow_id is p_workflow_id when p_ended_check is TRUE,
-- the workflow is COMPLETED or FAILED now, and it has a parent: the step ended it, and the caller advances its
-- parent node after the commit.
-- Locks: none. Returns one OUTCOME row. Python: the return values of the engine entry points.
CREATE FUNCTION horsies_workflow_outcome_row(
    p_disposition text, p_workflow_id uuid, p_ended_check boolean, p_detail text
) RETURNS horsies_workflow_step_row
    LANGUAGE sql STABLE
AS $$
    SELECT ROW(
        'OUTCOME', p_disposition, p_workflow_id, w.status::text,
        CASE WHEN p_ended_check AND w.status IN ('COMPLETED', 'FAILED') AND w.parent_workflow_id IS NOT NULL
             THEN w.id END,
        p_detail,
        NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
    )::horsies_workflow_step_row
    FROM (SELECT 1) AS one
    LEFT JOIN horsies_workflows w ON w.id = p_workflow_id
$$;

------------------------------------------------------------------------------------------------------------------------
-- Sub-workflow completion into the parent node
------------------------------------------------------------------------------------------------------------------------

-- The parent node status and result of a terminal child, and the child summary. Python: _parse_child_output,
-- _completed_child_node, _failed_child_node and the summary of on_subworkflow_complete
-- (core/workflows/engine.py:3118-3281, 3342-3392).
-- - COMPLETED child with a value envelope: COMPLETED, the child's text copied.
-- - COMPLETED child with the outputless envelope: COMPLETED, {"__h_task_result__":true,"ok":null,"err":null}.
-- - COMPLETED child with an error envelope: FAILED, the child's text copied.
-- - COMPLETED child with a result that is absent or not an envelope: FAILED, RESULT_DESERIALIZATION_ERROR naming the
--   child.
-- - FAILED, CANCELLED or EXPIRED child: FAILED, SUBWORKFLOW_FAILED with sub_workflow_id and sub_workflow_summary.
-- The summary: status, output (the ok value of a value envelope, else null), the node counts, error_summary (the
-- message of a stored error object, the first 200 characters of a stored error that is not JSON, else null).
-- Locks: none (the child is terminal and does not change). Returns node_status, node_result and summary.
CREATE FUNCTION horsies_workflow_child_node_result(
    p_child_workflow_id uuid,
    OUT node_status text, OUT node_result text, OUT summary text
)
    LANGUAGE plpgsql STABLE
    SET plan_cache_mode TO 'force_generic_plan'
AS $$
DECLARE
    v_child record;
    v_kind text;
    v_error json;
    v_error_summary text;
BEGIN
    SELECT w.status, w.result, w.error,
           count(n.id) AS total,
           count(n.id) FILTER (WHERE n.status = 'COMPLETED') AS completed,
           count(n.id) FILTER (WHERE n.status = 'FAILED') AS failed,
           count(n.id) FILTER (WHERE n.status = 'SKIPPED') AS skipped
    INTO v_child
    FROM horsies_workflows w
    LEFT JOIN horsies_workflow_tasks n ON n.workflow_id = w.id
    WHERE w.id = p_child_workflow_id
    GROUP BY w.id;
    v_kind := horsies_workflow_result_kind(v_child.result);
    v_error := horsies_workflow_json_or_null(v_child.error);
    v_error_summary := CASE
        WHEN v_child.error IS NULL OR v_child.error = '' THEN NULL
        WHEN v_error IS NULL THEN left(v_child.error, 200)
        WHEN json_typeof(v_error) = 'object' AND json_typeof(v_error -> 'message') = 'string'
            THEN v_error ->> 'message'
        ELSE NULL
    END;
    summary := '{"status":' || to_json(v_child.status::text)::text
        || ',"output":' || CASE
            WHEN v_child.status = 'COMPLETED' AND v_kind = 'VALUE' THEN (v_child.result::json -> 'ok')::text
            ELSE 'null'
        END
        || ',"total_tasks":' || v_child.total
        || ',"completed_tasks":' || v_child.completed
        || ',"failed_tasks":' || v_child.failed
        || ',"skipped_tasks":' || v_child.skipped
        || ',"error_summary":' || coalesce(to_json(v_error_summary)::text, 'null') || '}';
    IF v_child.status <> 'COMPLETED' THEN
        node_status := 'FAILED';
        node_result := horsies_task_error_envelope(
            'SUBWORKFLOW_FAILED',
            format('Subworkflow %s failed with status %s', p_child_workflow_id, v_child.status),
            NULL, p_child_workflow_id, summary
        );
        RETURN;
    END IF;
    CASE v_kind
        WHEN 'VALUE' THEN
            node_status := 'COMPLETED';
            node_result := v_child.result;
        WHEN 'OUTPUTLESS' THEN
            node_status := 'COMPLETED';
            node_result := '{"__h_task_result__":true,"ok":null,"err":null}';
        WHEN 'ERROR' THEN
            node_status := 'FAILED';
            node_result := v_child.result;
        WHEN 'ABSENT', 'UNREADABLE' THEN
            node_status := 'FAILED';
            node_result := horsies_task_error_envelope(
                'RESULT_DESERIALIZATION_ERROR',
                format('Child workflow %s is COMPLETED and its stored result does not parse', p_child_workflow_id),
                '{"workflow_id":' || to_json(p_child_workflow_id::text)::text || '}',
                NULL, NULL
            );
    END CASE;
END;
$$;

-- Step function: advances the parent node of a terminal child workflow. One transaction per ancestor level: the
-- caller drives the next level from advance_child_workflow_id of the OUTCOME row. The start and the resume call
-- the same body inside their own statement (they drop its OUTCOME row).
-- Steps: read the child (not locked: it is terminal and does not change); lock the parent workflow row FOR UPDATE;
-- write the parent node where it is not terminal (status and result of horsies_workflow_child_node_result, the
-- summary, completed_at); then horsies_workflow_progress for the parent node.
-- Dispositions: APPLIED; ALREADY_APPLIED (the parent node is terminal; nothing written); CHILD_ABSENT; NOT_A_CHILD
-- (the workflow has no parent); CHILD_NOT_TERMINAL (nothing written); PARENT_ABSENT (nothing written).
-- Lock order: parent workflow row, parent node row, then the promotion locks.
-- Python: on_subworkflow_complete (core/workflows/engine.py:3283-3465), driven by core/worker/finalize.py:2248-2290.
-- Returns: the OUTCOME row and the owed CHILD_START and SOURCE rows of the parent's promotion.
CREATE FUNCTION horsies_workflow_child_finish(p_child_workflow_id uuid) RETURNS SETOF horsies_workflow_step_row
    LANGUAGE plpgsql
    SET plan_cache_mode TO 'force_generic_plan'
AS $$
DECLARE
    v_child record;
    v_node record;
BEGIN
    SELECT w.status, w.parent_workflow_id, w.parent_task_index INTO v_child
    FROM horsies_workflows w
    WHERE w.id = p_child_workflow_id;
    IF NOT FOUND THEN
        RETURN NEXT horsies_workflow_outcome_row('CHILD_ABSENT', p_child_workflow_id, FALSE, NULL);
        RETURN;
    END IF;
    IF v_child.parent_workflow_id IS NULL THEN
        RETURN NEXT horsies_workflow_outcome_row('NOT_A_CHILD', p_child_workflow_id, FALSE, NULL);
        RETURN;
    END IF;
    IF v_child.status NOT IN ('COMPLETED', 'FAILED', 'CANCELLED', 'EXPIRED') THEN
        RETURN NEXT horsies_workflow_outcome_row('CHILD_NOT_TERMINAL', p_child_workflow_id, FALSE, v_child.status);
        RETURN;
    END IF;
    PERFORM 1 FROM horsies_workflows w WHERE w.id = v_child.parent_workflow_id FOR UPDATE;
    IF NOT FOUND THEN
        RETURN NEXT horsies_workflow_outcome_row('PARENT_ABSENT', v_child.parent_workflow_id, FALSE, NULL);
        RETURN;
    END IF;
    SELECT * INTO v_node FROM horsies_workflow_child_node_result(p_child_workflow_id);
    UPDATE horsies_workflow_tasks n
    SET status = v_node.node_status, result = v_node.node_result, sub_workflow_summary = v_node.summary,
        completed_at = now()
    WHERE n.workflow_id = v_child.parent_workflow_id
      AND n.task_index = v_child.parent_task_index
      AND n.status NOT IN ('COMPLETED', 'FAILED', 'SKIPPED');
    IF NOT FOUND THEN
        RETURN NEXT horsies_workflow_outcome_row('ALREADY_APPLIED', v_child.parent_workflow_id, FALSE, NULL);
        RETURN;
    END IF;
    RETURN QUERY SELECT * FROM horsies_workflow_progress(v_child.parent_workflow_id, v_child.parent_task_index, 'READ');
    RETURN NEXT horsies_workflow_outcome_row('APPLIED', v_child.parent_workflow_id, TRUE, NULL);
END;
$$;

------------------------------------------------------------------------------------------------------------------------
-- Node finish, phase 2
------------------------------------------------------------------------------------------------------------------------

-- Internal: the result bytes of a phase-2 record, from the history row at its locator (both partition keys given,
-- so the parent probe prunes to one leaf) or from its quarantine copy, checked against the record. Returns failure
-- NULL and the payload, or a failure (SOURCE_ABSENT, SOURCE_STATE_CONFLICT, SOURCE_VERSION_CONFLICT,
-- SOURCE_DIGEST_MISMATCH) and payload NULL. Reads only.
-- Locks: none (the caller holds the pending row). Python: the source read of horsies_phase2_consume
-- (0003_interim_functions.sql).
CREATE FUNCTION horsies_workflow_phase2_source(
    p_pending horsies_workflow_phase2_pending, OUT failure text, OUT payload bytea
)
    LANGUAGE plpgsql STABLE
    SET plan_cache_mode TO 'force_generic_plan'
AS $$
DECLARE
    v_source record;
BEGIN
    IF p_pending.recovery_source = 'HISTORY' THEN
        SELECT h.task_id, h.result_payload AS payload, h.result_digest AS digest, h.history_schema_version AS version
        INTO v_source
        FROM horsies_task_history h
        WHERE h.retention_class_key = p_pending.history_class
          AND h.terminal_at = p_pending.history_anchor
          AND h.task_id = p_pending.task_id;
    ELSE
        SELECT q.task_id, q.result_payload AS payload, q.result_digest AS digest, q.history_schema_version AS version
        INTO v_source
        FROM horsies_workflow_phase2_quarantine q
        WHERE q.task_id = p_pending.quarantine_task_id;
    END IF;
    failure := CASE
        WHEN NOT FOUND THEN 'SOURCE_ABSENT'
        WHEN v_source.task_id <> p_pending.task_id THEN 'SOURCE_STATE_CONFLICT'
        WHEN v_source.version IS DISTINCT FROM p_pending.history_schema_version OR v_source.version <> 1
            THEN 'SOURCE_VERSION_CONFLICT'
        WHEN v_source.digest IS DISTINCT FROM p_pending.result_digest OR v_source.payload IS NULL
             OR sha256(v_source.payload) <> p_pending.result_digest THEN 'SOURCE_DIGEST_MISMATCH'
    END;
    payload := CASE WHEN failure IS NULL THEN v_source.payload END;
END;
$$;

-- Internal: applies the phase-2 record of a finished workflow task to its node. One body for the worker's phase 2
-- and phase-2 recovery. Steps:
-- 1. Read the pending row with no lock. Absent: classify a replay from the node (ALREADY_APPLIED for a terminal
--    node, PENDING_ABSENT otherwise); a replay sends no wake.
-- 2. Lock the workflow row FOR UPDATE, the node row, the pending row.
-- 3. When p_wake: pg_notify('task_queue_' || <queue of the node row>, 'capacity'), delivered at commit.
-- 4. Load the result from the history row at the locator (one pruned leaf) or from the quarantine copy; check task
--    identity, history schema version (1) and digest. A mismatch returns SOURCE_ABSENT, SOURCE_STATE_CONFLICT,
--    SOURCE_VERSION_CONFLICT or SOURCE_DIGEST_MISMATCH and writes nothing (the record is kept).
-- 5. Write the node where it is not terminal: COMPLETED for COMPLETE_LOCKED, FAILED for the other kinds, the result,
--    completed_at. Delete the pending row and the quarantine copy.
-- Dispositions: APPLIED (node written, workflow live), APPLIED_WORKFLOW_ENDED (node written; the workflow is
-- COMPLETED, FAILED, CANCELLED or EXPIRED, so nothing more runs), ALREADY_APPLIED, PENDING_ABSENT, and the four
-- retaining ones above.
-- Lock order: workflow row, node row, pending row.
-- Python: horsies_phase2_consume (renamed; v61 body in 0003_interim_functions.sql) with COMPLETE_WORKFLOW_TASK_SQL
-- (core/workflows/sql.py:337-383) for the wake and the ended workflow.
CREATE FUNCTION horsies_workflow_node_apply(p_task_id uuid, p_wake boolean)
    RETURNS TABLE (disposition text, workflow_id uuid, task_index integer, workflow_status text, node_status text)
    LANGUAGE plpgsql
    SET plan_cache_mode TO 'force_generic_plan'
AS $$
DECLARE
    v_pending horsies_workflow_phase2_pending%ROWTYPE;
    v_workflow_status text;
    v_node record;
    v_source record;
    v_status text;
BEGIN
    PERFORM horsies_workflow_require(p_wake IS NOT NULL, 'wake must not be NULL');
    SELECT * INTO v_pending FROM horsies_workflow_phase2_pending p WHERE p.task_id = p_task_id;
    IF NOT FOUND THEN
        SELECT n.workflow_id, n.task_index, n.status, w.status AS workflow_status INTO v_node
        FROM horsies_workflow_tasks n
        JOIN horsies_workflows w ON w.id = n.workflow_id
        WHERE n.task_id = p_task_id;
        RETURN QUERY SELECT
            CASE WHEN v_node.status IN ('COMPLETED', 'FAILED', 'SKIPPED') THEN 'ALREADY_APPLIED'
                 ELSE 'PENDING_ABSENT' END,
            v_node.workflow_id, v_node.task_index, v_node.workflow_status::text, v_node.status::text;
        RETURN;
    END IF;

    SELECT w.status INTO v_workflow_status FROM horsies_workflows w WHERE w.id = v_pending.workflow_id FOR UPDATE;
    SELECT n.id, n.task_index, n.status, n.queue_name INTO v_node
    FROM horsies_workflow_tasks n
    WHERE n.id = v_pending.workflow_node_row_id AND n.workflow_id = v_pending.workflow_id
    FOR UPDATE;
    PERFORM 1 FROM horsies_workflow_phase2_pending p WHERE p.task_id = p_task_id FOR UPDATE;
    IF NOT FOUND THEN
        -- A concurrent phase 2 of the same record committed while this call waited for the workflow row.
        RETURN QUERY SELECT
            CASE WHEN v_node.status IN ('COMPLETED', 'FAILED', 'SKIPPED') THEN 'ALREADY_APPLIED'
                 ELSE 'PENDING_ABSENT' END,
            v_pending.workflow_id, v_node.task_index, v_workflow_status, v_node.status::text;
        RETURN;
    END IF;
    IF p_wake THEN
        PERFORM pg_notify('task_queue_' || v_node.queue_name, 'capacity');
    END IF;

    SELECT * INTO v_source FROM horsies_workflow_phase2_source(v_pending);
    IF v_source.failure IS NOT NULL THEN
        RETURN QUERY SELECT v_source.failure, v_pending.workflow_id, v_node.task_index, v_workflow_status,
            v_node.status::text;
        RETURN;
    END IF;

    v_status := CASE WHEN v_pending.terminalization_kind = 'COMPLETE_LOCKED' THEN 'COMPLETED' ELSE 'FAILED' END;
    UPDATE horsies_workflow_tasks n
    SET status = v_status, result = convert_from(v_source.payload, 'UTF8'), completed_at = now()
    WHERE n.id = v_node.id AND n.status NOT IN ('COMPLETED', 'FAILED', 'SKIPPED');
    DELETE FROM horsies_workflow_phase2_pending p WHERE p.task_id = p_task_id;
    IF v_pending.recovery_source = 'QUARANTINE' THEN
        DELETE FROM horsies_workflow_phase2_quarantine q WHERE q.task_id = v_pending.quarantine_task_id;
    END IF;
    RETURN QUERY SELECT
        CASE
            WHEN v_node.status IN ('COMPLETED', 'FAILED', 'SKIPPED') THEN 'ALREADY_APPLIED'
            WHEN v_workflow_status IN ('COMPLETED', 'FAILED', 'CANCELLED', 'EXPIRED') THEN 'APPLIED_WORKFLOW_ENDED'
            ELSE 'APPLIED'
        END,
        v_pending.workflow_id, v_node.task_index, v_workflow_status,
        CASE WHEN v_node.status IN ('COMPLETED', 'FAILED', 'SKIPPED') THEN v_node.status::text ELSE v_status END;
END;
$$;

-- Phase 2 of a finished workflow task, in one statement in autocommit when no sub-workflow node is promoted:
-- the node write, the delete of the phase-2 record, the capacity wake of the task's queue (p_wake), the failure
-- policy, the paused guard, promotion with the task rows of promoted task nodes, and the completion check.
-- p_dependents: 'NONE' when phase 1 reported that no node depends on this node (no dependents read), 'READ'
-- otherwise (a replay or a retry, which do not know it). A promoted sub-workflow node is owed work: the caller runs
-- this call inside BEGIN, then horsies_workflow_child_start per CHILD_START row, then COMMIT.
-- Returns the OUTCOME row (the disposition of horsies_workflow_node_apply; advance_child_workflow_id when this step
-- ended a child workflow) and the CHILD_START and SOURCE rows. A retaining disposition writes nothing: the worker
-- treats it as a phase-2 error, and phase-2 recovery counts it.
-- Lock order: workflow row, node row, pending row, quarantine row, then the promotion locks (node rows, new task
-- rows) and, under on_error pause, the RUNNING descendant workflow rows in (depth, id) order.
-- Python: COMPLETE_WORKFLOW_TASK_SQL and apply_node_progression (core/worker/finalize.py:1618-1673;
-- core/workflows/sql.py:337-383; core/workflows/engine.py:1821-2060).
CREATE FUNCTION horsies_workflow_node_finish(p_task_id uuid, p_wake boolean, p_dependents text)
    RETURNS SETOF horsies_workflow_step_row
    LANGUAGE plpgsql
    SET plan_cache_mode TO 'force_generic_plan'
AS $$
DECLARE
    v_apply record;
BEGIN
    PERFORM horsies_workflow_require(p_task_id IS NOT NULL, 'task id must not be NULL');
    PERFORM horsies_workflow_require(p_dependents IN ('NONE', 'READ'), 'dependents must be NONE or READ');
    SELECT * INTO v_apply FROM horsies_workflow_node_apply(p_task_id, p_wake);
    IF v_apply.disposition = 'APPLIED' THEN
        RETURN QUERY SELECT * FROM horsies_workflow_progress(v_apply.workflow_id, v_apply.task_index, p_dependents);
        RETURN NEXT horsies_workflow_outcome_row('APPLIED', v_apply.workflow_id, TRUE, NULL);
        RETURN;
    END IF;
    RETURN NEXT horsies_workflow_outcome_row(v_apply.disposition, v_apply.workflow_id, FALSE, NULL);
END;
$$;

------------------------------------------------------------------------------------------------------------------------
-- Phase-2 recovery
------------------------------------------------------------------------------------------------------------------------

-- Internal: the writes of the recovery limit, with SQL only (no engine code, which is what raised): the node of the
-- raise goes FAILED with p_limit_node_result where it is not terminal, PENDING and READY nodes go SKIPPED, the
-- stored first error stays (else p_limit_error), and with no live node (ENQUEUED, RUNNING) the workflow goes FAILED
-- with no policy evaluation. Returns REFUSED (the workflow is not RUNNING: a pause or a cancel won; nothing written),
-- LIMIT_REACHED_FAILED, or LIMIT_REACHED_LIVE_NODES (the last live node ends the workflow).
-- Precondition: the caller holds the workflow row FOR UPDATE. p_task_index may be NULL (no node to fail).
-- Python: _write_limit (core/workflows/recovery_limit.py:392-431).
-- Locks: the workflow row (held by the caller), then the node rows it writes.
CREATE FUNCTION horsies_workflow_apply_recovery_limit(
    p_workflow_id uuid, p_task_index integer, p_limit_error text, p_limit_node_result text
) RETURNS text
    LANGUAGE plpgsql
    SET plan_cache_mode TO 'force_generic_plan'
AS $$
BEGIN
    PERFORM horsies_workflow_require(
        p_limit_error IS NOT NULL AND p_limit_node_result IS NOT NULL,
        'the recovery limit error and node result must not be NULL'
    );
    IF NOT EXISTS (SELECT 1 FROM horsies_workflows w WHERE w.id = p_workflow_id AND w.status = 'RUNNING') THEN
        RETURN 'REFUSED';
    END IF;
    IF p_task_index IS NOT NULL THEN
        UPDATE horsies_workflow_tasks n
        SET status = 'FAILED', result = p_limit_node_result, completed_at = now()
        WHERE n.workflow_id = p_workflow_id AND n.task_index = p_task_index
          AND n.status NOT IN ('COMPLETED', 'FAILED', 'SKIPPED');
    END IF;
    UPDATE horsies_workflow_tasks n
    SET status = 'SKIPPED', completed_at = now()
    WHERE n.workflow_id = p_workflow_id AND n.status IN ('PENDING', 'READY');
    UPDATE horsies_workflows
    SET error = coalesce(error, p_limit_error), updated_at = now()
    WHERE id = p_workflow_id AND status = 'RUNNING';
    IF EXISTS (
        SELECT 1 FROM horsies_workflow_tasks n
        WHERE n.workflow_id = p_workflow_id AND n.status IN ('ENQUEUED', 'RUNNING')
    ) THEN
        RETURN 'LIMIT_REACHED_LIVE_NODES';
    END IF;
    UPDATE horsies_workflows
    SET status = 'FAILED', result = NULL, completed_at = now(), updated_at = now()
    WHERE id = p_workflow_id AND status = 'RUNNING';
    RETURN 'LIMIT_REACHED_FAILED';
END;
$$;

-- Phase-2 recovery of one record, in the caller's transaction (BEGIN; this; owed child starts; COMMIT). The body of
-- horsies_workflow_node_finish with no wake and with the dependents read. A retaining disposition adds one attempt
-- to the record in the same statement (attempt_count, last_attempt_at, last_failure_class) and, at p_attempt_bound,
-- copies the evidence to the quarantine table (horsies_phase2_quarantine_one); detail holds the quarantine verdict.
-- p_attempt_bound is 3..1000 (22023 otherwise).
-- Lock order: as horsies_workflow_node_finish; the quarantine function locks only the pending row, already held.
-- Python: drive_phase2_recovery per row (core/workflows/phase2_recovery.py:390-440, 620-662).
-- Returns: the OUTCOME row (detail: the quarantine verdict at the bound) and the owed CHILD_START and SOURCE rows.
CREATE FUNCTION horsies_workflow_phase2_recover(p_task_id uuid, p_attempt_bound integer)
    RETURNS SETOF horsies_workflow_step_row
    LANGUAGE plpgsql
    SET plan_cache_mode TO 'force_generic_plan'
AS $$
DECLARE
    v_apply record;
    v_attempts integer;
    v_verdict horsies_phase2_quarantine_verdict;
BEGIN
    PERFORM horsies_workflow_require(p_task_id IS NOT NULL, 'task id must not be NULL');
    PERFORM horsies_workflow_require(p_attempt_bound BETWEEN 3 AND 1000, 'attempt bound must be 3..1000');
    SELECT * INTO v_apply FROM horsies_workflow_node_apply(p_task_id, FALSE);
    CASE v_apply.disposition
        WHEN 'APPLIED' THEN
            RETURN QUERY SELECT * FROM horsies_workflow_progress(v_apply.workflow_id, v_apply.task_index, 'READ');
            RETURN NEXT horsies_workflow_outcome_row('APPLIED', v_apply.workflow_id, TRUE, NULL);
        WHEN 'SOURCE_ABSENT', 'SOURCE_STATE_CONFLICT', 'SOURCE_VERSION_CONFLICT', 'SOURCE_DIGEST_MISMATCH' THEN
            UPDATE horsies_workflow_phase2_pending p
            SET attempt_count = p.attempt_count + 1,
                last_attempt_at = statement_timestamp(),
                last_failure_class = v_apply.disposition
            WHERE p.task_id = p_task_id
            RETURNING p.attempt_count INTO v_attempts;
            IF v_attempts >= p_attempt_bound THEN
                v_verdict := horsies_phase2_quarantine_one(p_task_id);
            END IF;
            RETURN NEXT horsies_workflow_outcome_row(
                v_apply.disposition, v_apply.workflow_id, FALSE,
                CASE WHEN v_verdict IS NULL THEN NULL
                     ELSE v_verdict.verdict || coalesce(': ' || v_verdict.detail, '') END
            );
        ELSE
            RETURN NEXT horsies_workflow_outcome_row(v_apply.disposition, v_apply.workflow_id, FALSE, NULL);
    END CASE;
END;
$$;

-- Counts one persistent raise of a phase-2 recovery record, in one statement in autocommit after the record's own
-- transaction rolled back. Lock order: the pending row read with no lock, then the workflow row FOR UPDATE, the node
-- row, the pending row (the phase-2 order). Outcomes:
-- - PENDING_ABSENT: the record is gone; nothing written.
-- - COUNTED: under p_attempt_bound; the attempt is added (last_failure_class RAISED).
-- - LIMIT_REACHED: at the bound with a RUNNING workflow; the recovery limit is written
--   (horsies_workflow_apply_recovery_limit with p_limit_error and p_limit_node_result), the attempt is added and the
--   evidence is quarantined. limit_outcome and the quarantine verdict are returned.
-- - NOT_RECORDED: at the bound with a workflow that is not RUNNING; nothing written (a later pass after a resume
--   tries again).
-- Python: _record_raised_row (core/workflows/phase2_recovery.py:533-608), which locked the pending row before the
-- workflow row.
CREATE FUNCTION horsies_workflow_phase2_record_raise(
    p_task_id uuid, p_attempt_bound integer, p_limit_error text, p_limit_node_result text
) RETURNS TABLE (outcome text, attempt_count integer, limit_outcome text, quarantine_verdict text)
    LANGUAGE plpgsql
    SET plan_cache_mode TO 'force_generic_plan'
AS $$
DECLARE
    v_pending record;
    v_workflow_status text;
    v_task_index integer;
    v_attempts integer;
    v_limit text;
    v_verdict horsies_phase2_quarantine_verdict;
BEGIN
    PERFORM horsies_workflow_require(p_attempt_bound BETWEEN 3 AND 1000, 'attempt bound must be 3..1000');
    PERFORM horsies_workflow_require(
        p_limit_error IS NOT NULL AND p_limit_node_result IS NOT NULL,
        'the recovery limit error and node result must not be NULL'
    );
    SELECT p.workflow_id, p.workflow_node_row_id INTO v_pending
    FROM horsies_workflow_phase2_pending p WHERE p.task_id = p_task_id;
    IF NOT FOUND THEN
        RETURN QUERY SELECT 'PENDING_ABSENT'::text, NULL::integer, NULL::text, NULL::text;
        RETURN;
    END IF;
    SELECT w.status INTO v_workflow_status FROM horsies_workflows w WHERE w.id = v_pending.workflow_id FOR UPDATE;
    SELECT n.task_index INTO v_task_index FROM horsies_workflow_tasks n
    WHERE n.id = v_pending.workflow_node_row_id FOR UPDATE;
    SELECT p.attempt_count INTO v_attempts FROM horsies_workflow_phase2_pending p
    WHERE p.task_id = p_task_id FOR UPDATE;
    IF NOT FOUND THEN
        RETURN QUERY SELECT 'PENDING_ABSENT'::text, NULL::integer, NULL::text, NULL::text;
        RETURN;
    END IF;
    IF v_attempts + 1 >= p_attempt_bound THEN
        IF v_workflow_status IS DISTINCT FROM 'RUNNING' THEN
            RETURN QUERY SELECT 'NOT_RECORDED'::text, v_attempts, NULL::text, NULL::text;
            RETURN;
        END IF;
        v_limit := horsies_workflow_apply_recovery_limit(
            v_pending.workflow_id, v_task_index, p_limit_error, p_limit_node_result
        );
    END IF;
    UPDATE horsies_workflow_phase2_pending p
    SET attempt_count = p.attempt_count + 1, last_attempt_at = statement_timestamp(), last_failure_class = 'RAISED'
    WHERE p.task_id = p_task_id
    RETURNING p.attempt_count INTO v_attempts;
    IF v_limit IS NULL THEN
        RETURN QUERY SELECT 'COUNTED'::text, v_attempts, NULL::text, NULL::text;
        RETURN;
    END IF;
    v_verdict := horsies_phase2_quarantine_one(p_task_id);
    RETURN QUERY SELECT 'LIMIT_REACHED'::text, v_attempts, v_limit,
        v_verdict.verdict || coalesce(': ' || v_verdict.detail, '');
END;
$$;

------------------------------------------------------------------------------------------------------------------------
-- Pause, resume, cancel
------------------------------------------------------------------------------------------------------------------------

-- Pauses a RUNNING workflow and its RUNNING descendants, in one statement in autocommit. Steps: RUNNING to PAUSED
-- for the workflow (its row lock); the RUNNING descendants, level by level, locked in (depth, id) order and paused
-- (horsies_workflow_move_descendants); every CLAIMED task behind an ENQUEUED or RUNNING node of the paused tree is
-- abandoned (horsies_abandon_nodes_of_paused_workflows: PAUSE_ABANDON_WORKFLOW, task rows with SKIP LOCKED); the
-- nodes of the abandoned tasks go back to READY with no task. The status trigger sends one workflow_done per paused
-- workflow at commit; this function sends none.
-- Outcomes: PAUSED (paused_workflow_ids, the root first; abandoned_task_ids); NOT_RUNNING (observed_status; nothing
-- written); NOT_FOUND.
-- Lock order: root row, descendant rows in (depth, id) order, task rows (SKIP LOCKED), node rows.
-- Python: pause_workflow (core/workflows/lifecycle.py:886-994) with cascade_pause_to_children (:996-1030).
-- Returns: one row (outcome, observed_status, paused_workflow_ids, abandoned_task_ids).
CREATE FUNCTION horsies_workflow_pause(p_workflow_id uuid)
    RETURNS TABLE (outcome text, observed_status text, paused_workflow_ids uuid[], abandoned_task_ids uuid[])
    LANGUAGE plpgsql
    SET plan_cache_mode TO 'force_generic_plan'
AS $$
DECLARE
    v_paused uuid[];
    v_abandoned uuid[];
    v_status text;
BEGIN
    PERFORM horsies_workflow_require(p_workflow_id IS NOT NULL, 'workflow id must not be NULL');
    UPDATE horsies_workflows w SET status = 'PAUSED', updated_at = now()
    WHERE w.id = p_workflow_id AND w.status = 'RUNNING';
    IF NOT FOUND THEN
        SELECT w.status INTO v_status FROM horsies_workflows w WHERE w.id = p_workflow_id;
        RETURN QUERY SELECT CASE WHEN v_status IS NULL THEN 'NOT_FOUND' ELSE 'NOT_RUNNING' END,
            v_status, NULL::uuid[], NULL::uuid[];
        RETURN;
    END IF;
    v_paused := ARRAY[p_workflow_id] || horsies_workflow_move_descendants(p_workflow_id, 'RUNNING', 'PAUSED');
    SELECT coalesce(array_agg(o.task_id ORDER BY o.task_id), ARRAY[]::uuid[]) INTO v_abandoned
    FROM horsies_abandon_nodes_of_paused_workflows(v_paused) AS o
    WHERE o.outcome = 'APPLIED';
    UPDATE horsies_workflow_tasks n
    SET status = 'READY', task_id = NULL, started_at = NULL
    WHERE n.task_id = ANY(v_abandoned) AND n.status IN ('ENQUEUED', 'RUNNING');
    RETURN QUERY SELECT 'PAUSED'::text, 'PAUSED'::text, v_paused, v_abandoned;
END;
$$;

-- Resumes a PAUSED workflow and its PAUSED descendants, in the caller's transaction (BEGIN; this; owed child
-- starts; COMMIT). Steps: PAUSED to RUNNING for the workflow and, level by level in (depth, id) order, its PAUSED
-- descendants; for each resumed workflow, in (depth, id) order, promotion with its PENDING nodes as candidates and
-- its READY nodes enqueued as they are (task rows minted; child starts owed for sub-workflow nodes); then the
-- completion check of each resumed workflow, deepest first; a resumed workflow that ends advances its parent node
-- in this statement when the parent is resumed too (horsies_workflow_child_finish), else the OUTCOME row names it.
-- Dispositions: RESUMED; NOT_PAUSED (detail: the observed status; nothing written); NOT_FOUND.
-- The tree recovery pass after the commit is the caller's (RESUME_RECOVERY_FAILED is reported there).
-- Lock order: root row, descendant rows in (depth, id) order, node rows, new task rows.
-- Python: resume_workflow (core/workflows/lifecycle.py:1084-1263) with cascade_resume_to_children (:1265-1358).
-- Returns: the OUTCOME row (advance_child_workflow_id when the resumed workflow ended and has a parent) and the
-- owed CHILD_START and SOURCE rows.
CREATE FUNCTION horsies_workflow_resume(p_workflow_id uuid) RETURNS SETOF horsies_workflow_step_row
    LANGUAGE plpgsql
    SET plan_cache_mode TO 'force_generic_plan'
AS $$
DECLARE
    v_resumed uuid[];
    v_status text;
    v_workflow record;
BEGIN
    PERFORM horsies_workflow_require(p_workflow_id IS NOT NULL, 'workflow id must not be NULL');
    UPDATE horsies_workflows w SET status = 'RUNNING', updated_at = now()
    WHERE w.id = p_workflow_id AND w.status = 'PAUSED';
    IF NOT FOUND THEN
        SELECT w.status INTO v_status FROM horsies_workflows w WHERE w.id = p_workflow_id;
        RETURN NEXT horsies_workflow_outcome_row(
            CASE WHEN v_status IS NULL THEN 'NOT_FOUND' ELSE 'NOT_PAUSED' END, p_workflow_id, FALSE, v_status
        );
        RETURN;
    END IF;
    v_resumed := ARRAY[p_workflow_id] || horsies_workflow_move_descendants(p_workflow_id, 'PAUSED', 'RUNNING');

    FOR v_workflow IN
        SELECT w.id FROM horsies_workflows w WHERE w.id = ANY(v_resumed) ORDER BY w.depth, w.id
    LOOP
        RETURN QUERY SELECT * FROM horsies_workflow_promote(
            v_workflow.id,
            NULL,
            ARRAY(SELECT n.task_index FROM horsies_workflow_tasks n
                  WHERE n.workflow_id = v_workflow.id AND n.status = 'PENDING' ORDER BY n.task_index),
            ARRAY(SELECT n.task_index FROM horsies_workflow_tasks n
                  WHERE n.workflow_id = v_workflow.id AND n.status = 'READY' ORDER BY n.task_index)
        );
    END LOOP;

    -- Deepest first. A workflow of the set that is COMPLETED or FAILED here ended in this statement (each was PAUSED
    -- before it), by its own check or by a deeper child's advance: its parent node is advanced.
    FOR v_workflow IN
        SELECT w.id FROM horsies_workflows w
        WHERE w.id = ANY(v_resumed) AND w.id <> p_workflow_id
        ORDER BY w.depth DESC, w.id
    LOOP
        PERFORM horsies_workflow_try_complete(v_workflow.id);
        IF EXISTS (
            SELECT 1 FROM horsies_workflows w WHERE w.id = v_workflow.id AND w.status IN ('COMPLETED', 'FAILED')
        ) THEN
            RETURN QUERY SELECT * FROM horsies_workflow_child_finish(v_workflow.id) AS r WHERE r.row_kind <> 'OUTCOME';
        END IF;
    END LOOP;
    PERFORM horsies_workflow_try_complete(p_workflow_id);
    RETURN NEXT horsies_workflow_outcome_row('RESUMED', p_workflow_id, TRUE, NULL);
END;
$$;

-- Cancels a workflow tree, in one statement in autocommit. Steps: lock the root row FOR UPDATE; lock its
-- cancellable descendants (PENDING, RUNNING, PAUSED), level by level in (depth, id) order (the walk passes through
-- descendants of any status, as Python's recursive read); lock the live backing task rows of every locked workflow
-- in id order; lock their live node rows in id order; set every locked workflow CANCELLED with completed_at; cancel
-- the tasks behind ENQUEUED nodes (horsies_cancel_nodes_of_cancelled_workflow: WORKFLOW_CANCEL_WORKFLOW, moved to
-- history); SKIPPED (with completed_at) for PENDING and READY nodes, for the ENQUEUED nodes of the cancelled tasks,
-- and for ENQUEUED sub-workflow nodes with no child. A node whose task already started stays RUNNING until the task
-- ends. With the workflow rows locked first, no task can be linked to a node of the tree meanwhile, and no worker can
-- claim one of its tasks between the lock and the status change.
-- Outcomes: CANCELLED (cancelled_workflow_ids, the root first; cancelled_task_ids); NOT_CANCELLABLE
-- (observed_status: the root is terminal; nothing written); NOT_FOUND.
-- Lock order: root row, descendant rows in (depth, id) order, task rows in id order, node rows in id order.
-- Python: WorkflowHandle.cancel_async (core/models/workflow/handle.py:1588-1765), which locks each descendant in its
-- own sequence and takes the task locks in no order.
-- Returns: one row (outcome, observed_status, cancelled_workflow_ids, cancelled_task_ids).
CREATE FUNCTION horsies_workflow_cancel(p_workflow_id uuid)
    RETURNS TABLE (outcome text, observed_status text, cancelled_workflow_ids uuid[], cancelled_task_ids uuid[])
    LANGUAGE plpgsql
    SET plan_cache_mode TO 'force_generic_plan'
AS $$
DECLARE
    c_max_levels CONSTANT integer := 33;
    v_status text;
    v_level uuid[] := ARRAY[p_workflow_id];
    v_tree uuid[] := ARRAY[p_workflow_id];
    v_locked uuid[];
    v_cancelled_tasks uuid[];
    v_levels integer := 0;
BEGIN
    PERFORM horsies_workflow_require(p_workflow_id IS NOT NULL, 'workflow id must not be NULL');
    SELECT w.status INTO v_status FROM horsies_workflows w WHERE w.id = p_workflow_id FOR UPDATE;
    IF NOT FOUND THEN
        RETURN QUERY SELECT 'NOT_FOUND'::text, NULL::text, NULL::uuid[], NULL::uuid[];
        RETURN;
    END IF;
    IF v_status NOT IN ('PENDING', 'RUNNING', 'PAUSED') THEN
        RETURN QUERY SELECT 'NOT_CANCELLABLE'::text, v_status, NULL::uuid[], NULL::uuid[];
        RETURN;
    END IF;
    LOOP
        v_levels := v_levels + 1;
        IF v_levels > c_max_levels THEN
            RAISE EXCEPTION 'workflow % has descendants deeper than the depth limit', p_workflow_id
                USING ERRCODE = 'data_corrupted';
        END IF;
        v_level := ARRAY(SELECT w.id FROM horsies_workflows w WHERE w.parent_workflow_id = ANY(v_level) ORDER BY w.id);
        EXIT WHEN cardinality(v_level) = 0;
        v_locked := ARRAY(
            SELECT w.id FROM horsies_workflows w
            WHERE w.id = ANY(v_level) AND w.status IN ('PENDING', 'RUNNING', 'PAUSED')
            ORDER BY w.id
            FOR UPDATE
        );
        v_tree := v_tree || v_locked;
    END LOOP;

    PERFORM 1 FROM horsies_tasks t
    JOIN horsies_workflow_tasks n ON n.task_id = t.id
    WHERE n.workflow_id = ANY(v_tree) AND n.status IN ('ENQUEUED', 'RUNNING')
    ORDER BY t.id
    FOR UPDATE OF t;
    PERFORM 1 FROM horsies_workflow_tasks n
    WHERE n.workflow_id = ANY(v_tree) AND n.status IN ('PENDING', 'READY', 'ENQUEUED', 'RUNNING')
    ORDER BY n.id
    FOR UPDATE;

    UPDATE horsies_workflows w
    SET status = 'CANCELLED', completed_at = now(), updated_at = now()
    WHERE w.id = ANY(v_tree) AND w.status IN ('PENDING', 'RUNNING', 'PAUSED');
    SELECT coalesce(array_agg(o.task_id ORDER BY o.task_id), ARRAY[]::uuid[]) INTO v_cancelled_tasks
    FROM horsies_cancel_nodes_of_cancelled_workflow(v_tree) AS o
    WHERE o.outcome = 'APPLIED';
    UPDATE horsies_workflow_tasks n
    SET status = 'SKIPPED', completed_at = now()
    WHERE n.workflow_id = ANY(v_tree)
      AND (
          n.status IN ('PENDING', 'READY')
          OR (n.status = 'ENQUEUED' AND n.task_id = ANY(v_cancelled_tasks))
          OR (n.status = 'ENQUEUED' AND n.is_subworkflow AND n.sub_workflow_id IS NULL)
      );
    RETURN QUERY SELECT 'CANCELLED'::text, 'CANCELLED'::text, v_tree, v_cancelled_tasks;
END;
$$;

------------------------------------------------------------------------------------------------------------------------
-- Workflow start
------------------------------------------------------------------------------------------------------------------------

-- Internal: refuses start arguments that do not describe one tree (22023): arrays of different lengths, NULL in a
-- column that needs a value, a repeated workflow id or (workflow, index) pair, a first row that is not the
-- top-level row (no parent, depth 0), a later row without a parent in the tree or with a depth that is not its
-- parent's plus one, a node or failed record of a workflow outside the tree.
-- Locks: none. Returns nothing. Python: the in-memory prevalidation of start_workflow_async
-- (core/workflows/lifecycle.py:298-351).
CREATE FUNCTION horsies_workflow_start_check(
    p_workflow_ids uuid[], p_names text[], p_on_errors text[], p_parent_workflow_ids uuid[],
    p_parent_task_indexes integer[], p_depths integer[], p_other_workflow_lengths integer[],
    p_node_workflow_ids uuid[], p_node_task_indexes integer[], p_node_required_lengths integer[],
    p_node_has_null boolean,
    p_failed_workflow_ids uuid[], p_failed_task_indexes integer[], p_failed_results text[]
) RETURNS void
    LANGUAGE plpgsql IMMUTABLE
AS $$
DECLARE
    v_workflows integer := cardinality(p_workflow_ids);
    v_nodes integer := cardinality(p_node_workflow_ids);
BEGIN
    PERFORM horsies_workflow_require(v_workflows >= 1, 'the workflow arrays must hold the top-level row');
    PERFORM horsies_workflow_require(
        cardinality(p_names) = v_workflows AND cardinality(p_on_errors) = v_workflows
        AND cardinality(p_parent_workflow_ids) = v_workflows AND cardinality(p_parent_task_indexes) = v_workflows
        AND cardinality(p_depths) = v_workflows
        AND (SELECT bool_and(l = v_workflows) FROM unnest(p_other_workflow_lengths) AS l),
        'the workflow arrays differ in length'
    );
    PERFORM horsies_workflow_require(
        array_position(p_workflow_ids, NULL) IS NULL AND array_position(p_names, NULL) IS NULL
        AND array_position(p_on_errors, NULL) IS NULL AND array_position(p_depths, NULL) IS NULL,
        'a workflow id, name, on_error or depth is NULL'
    );
    PERFORM horsies_workflow_require(
        (SELECT count(DISTINCT id) FROM unnest(p_workflow_ids) AS id) = v_workflows,
        'a workflow id repeats'
    );
    PERFORM horsies_workflow_require(
        p_parent_workflow_ids[1] IS NULL AND p_parent_task_indexes[1] IS NULL AND p_depths[1] = 0,
        'the first workflow row must be the top-level row (no parent, depth 0)'
    );
    PERFORM horsies_workflow_require(
        NOT EXISTS (
            SELECT 1
            FROM unnest(p_workflow_ids, p_parent_workflow_ids, p_parent_task_indexes, p_depths)
                 WITH ORDINALITY AS c(id, parent_id, parent_index, depth, position)
            WHERE c.position > 1
              AND (
                  c.parent_index IS NULL
                  OR NOT EXISTS (
                      SELECT 1 FROM unnest(p_workflow_ids, p_depths) AS p(id, depth)
                      WHERE p.id = c.parent_id AND p.depth + 1 = c.depth
                  )
              )
        ),
        'a child workflow row has no parent in the tree, or a depth that is not its parent''s plus one'
    );
    PERFORM horsies_workflow_require(
        (SELECT bool_and(l = v_nodes) FROM unnest(p_node_required_lengths) AS l) AND NOT p_node_has_null,
        'the node arrays differ in length, or a required node value is NULL'
    );
    PERFORM horsies_workflow_require(
        v_nodes = (SELECT count(DISTINCT (w, i)) FROM unnest(p_node_workflow_ids, p_node_task_indexes) AS n(w, i))
        AND NOT EXISTS (SELECT 1 FROM unnest(p_node_workflow_ids) AS w WHERE w <> ALL(p_workflow_ids)),
        'a node (workflow, index) pair repeats, or a node belongs to no workflow of the tree'
    );
    PERFORM horsies_workflow_require(
        cardinality(p_failed_task_indexes) = cardinality(p_failed_workflow_ids)
        AND cardinality(p_failed_results) = cardinality(p_failed_workflow_ids)
        AND array_position(p_failed_workflow_ids, NULL) IS NULL
        AND array_position(p_failed_task_indexes, NULL) IS NULL
        AND array_position(p_failed_results, NULL) IS NULL
        AND cardinality(p_failed_workflow_ids) = (
            SELECT count(DISTINCT (w, i)) FROM unnest(p_failed_workflow_ids, p_failed_task_indexes) AS f(w, i)
        ),
        'the failed-node arrays differ in length, hold NULL, or repeat a node'
    );
END;
$$;

-- Internal: inserts the node rows of a starting tree. A sub-workflow root with a pre-built child (a workflow row whose
-- parent pair names the node) goes in RUNNING, linked to the child, with started_at; any other node without
-- dependencies goes in READY; the rest PENDING. dependencies is a JSON array of indexes, workflow_ctx_from a JSON
-- array of node ids, args_from a JSON object of key to index. sub_workflow_params is the params choice of a
-- sub-workflow node (GIVEN or NOT_GIVEN; NULL on a task node; the table CHECK refuses another pairing). Refuses
-- (22023) a child workflow row that names no sub-workflow root node.
-- Locks: the new node rows. Returns nothing. Python: INSERT_WORKFLOW_TASKS_BULK_SQL (core/workflows/sql.py:46-58;
-- core/workflows/lifecycle.py:699) and LINK_SUB_WORKFLOW_SQL (core/workflows/sql.py:271-276).
CREATE FUNCTION horsies_workflow_start_nodes(
    p_workflow_ids uuid[], p_parent_workflow_ids uuid[], p_parent_task_indexes integer[],
    p_node_workflow_ids uuid[], p_node_task_indexes integer[], p_node_ids text[], p_node_task_names text[],
    p_node_inputs text[], p_node_queue_names text[], p_node_priorities integer[], p_node_dependencies text[],
    p_node_args_from text[], p_node_workflow_ctx_from text[], p_node_allow_failed_deps boolean[],
    p_node_join_types text[], p_node_min_successes integer[], p_node_task_options text[],
    p_node_max_retries integer[], p_node_executions text[], p_node_good_untils timestamp with time zone[],
    p_node_retention_class_keys text[], p_node_is_subworkflow boolean[], p_node_sub_workflow_names text[],
    p_node_sub_definition_keys text[], p_node_sub_workflow_params text[]
) RETURNS void
    LANGUAGE plpgsql
    SET plan_cache_mode TO 'force_generic_plan'
AS $$
BEGIN
    INSERT INTO horsies_workflow_tasks (
        id, workflow_id, task_index, node_id, task_name, input, queue_name, priority, dependencies, args_from,
        workflow_ctx_from, allow_failed_deps, join_type, min_success, task_options, max_retries, execution,
        good_until, retention_class_key, status, task_id, is_subworkflow, sub_workflow_id, sub_workflow_name,
        sub_definition_key, sub_workflow_params, sub_workflow_summary, result, error, created_at, started_at,
        completed_at
    )
    SELECT horsies_uuidv7(), n.workflow_id, n.task_index, n.node_id, n.task_name, n.input, n.queue_name, n.priority,
           d.dependencies, n.args_from::jsonb,
           CASE WHEN n.ctx_from IS NULL THEN NULL ELSE ARRAY(
               SELECT x.value FROM jsonb_array_elements_text(n.ctx_from::jsonb) WITH ORDINALITY AS x(value, position)
               ORDER BY x.position
           ) END,
           n.allow_failed_deps, n.join_type, n.min_success, n.task_options, n.max_retries, n.execution,
           n.good_until, n.retention_class_key,
           CASE WHEN c.id IS NOT NULL THEN 'RUNNING' WHEN cardinality(d.dependencies) = 0 THEN 'READY'
                ELSE 'PENDING' END,
           NULL, n.is_subworkflow, c.id, n.sub_workflow_name, n.sub_definition_key, n.sub_workflow_params,
           NULL, NULL, NULL, now(),
           CASE WHEN c.id IS NOT NULL THEN now() END, NULL
    FROM unnest(
            p_node_workflow_ids, p_node_task_indexes, p_node_ids, p_node_task_names, p_node_inputs,
            p_node_queue_names, p_node_priorities, p_node_dependencies, p_node_args_from, p_node_workflow_ctx_from,
            p_node_allow_failed_deps, p_node_join_types, p_node_min_successes, p_node_task_options,
            p_node_max_retries, p_node_executions, p_node_good_untils, p_node_retention_class_keys,
            p_node_is_subworkflow, p_node_sub_workflow_names, p_node_sub_definition_keys, p_node_sub_workflow_params
         ) AS n(workflow_id, task_index, node_id, task_name, input, queue_name, priority, dependencies, args_from,
                ctx_from, allow_failed_deps, join_type, min_success, task_options, max_retries, execution,
                good_until, retention_class_key, is_subworkflow, sub_workflow_name, sub_definition_key,
                sub_workflow_params)
    CROSS JOIN LATERAL (
        SELECT ARRAY(
            SELECT e.value::integer
            FROM jsonb_array_elements_text(n.dependencies::jsonb) WITH ORDINALITY AS e(value, position)
            ORDER BY e.position
        ) AS dependencies
    ) d
    LEFT JOIN unnest(p_workflow_ids, p_parent_workflow_ids, p_parent_task_indexes) AS c(id, parent_id, parent_index)
      ON c.parent_id = n.workflow_id AND c.parent_index = n.task_index
    ORDER BY n.workflow_id, n.task_index;
    PERFORM horsies_workflow_require(
        NOT EXISTS (
            SELECT 1
            FROM unnest(p_workflow_ids, p_parent_workflow_ids, p_parent_task_indexes)
                 WITH ORDINALITY AS c(id, parent_id, parent_index, position)
            WHERE c.position > 1
              AND NOT EXISTS (
                  SELECT 1 FROM horsies_workflow_tasks n
                  WHERE n.workflow_id = c.parent_id AND n.task_index = c.parent_index
                    AND n.is_subworkflow AND n.sub_workflow_id = c.id AND cardinality(n.dependencies) = 0
              )
        ),
        'a child workflow row names no sub-workflow root node of its parent'
    );
END;
$$;

-- Internal: inserts the workflow rows of a starting tree. The top-level row (the first) goes in with ON CONFLICT (id)
-- DO NOTHING; when it exists already, nothing is written and the function returns FALSE. Else the child rows go in
-- (RUNNING, root id of the tree, the start call time) and it returns TRUE.
-- Locks: the new rows and the unique index entry of the top-level id. Python: INSERT_WORKFLOW_SQL and
-- INSERT_CHILD_WORKFLOW_SQL (core/workflows/sql.py:23-34, 260-270; core/workflows/lifecycle.py:420-442).
CREATE FUNCTION horsies_workflow_start_workflows(
    p_workflow_ids uuid[], p_names text[], p_definition_keys text[], p_on_errors text[],
    p_output_task_indexes integer[], p_success_policies text[], p_parent_workflow_ids uuid[],
    p_parent_task_indexes integer[], p_depths integer[], p_sent_at timestamp with time zone
) RETURNS boolean
    LANGUAGE plpgsql
    SET plan_cache_mode TO 'force_generic_plan'
AS $$
DECLARE
    v_top uuid := p_workflow_ids[1];
    v_inserted uuid;
BEGIN
    INSERT INTO horsies_workflows (
        id, name, definition_key, status, on_error, output_task_index, success_policy, result, error,
        parent_workflow_id, parent_task_index, root_workflow_id, depth, sent_at, created_at, started_at,
        completed_at, updated_at
    ) VALUES (
        v_top, p_names[1], p_definition_keys[1], 'RUNNING', p_on_errors[1], p_output_task_indexes[1],
        p_success_policies[1]::jsonb, NULL, NULL, NULL, NULL, v_top, 0, p_sent_at, now(), now(), NULL, now()
    )
    ON CONFLICT (id) DO NOTHING
    RETURNING id INTO v_inserted;
    IF v_inserted IS NULL THEN
        RETURN FALSE;
    END IF;
    INSERT INTO horsies_workflows (
        id, name, definition_key, status, on_error, output_task_index, success_policy, result, error,
        parent_workflow_id, parent_task_index, root_workflow_id, depth, sent_at, created_at, started_at,
        completed_at, updated_at
    )
    SELECT c.id, c.name, c.definition_key, 'RUNNING', c.on_error, c.output_index, c.policy::jsonb, NULL, NULL,
           c.parent_id, c.parent_index, v_top, c.depth, p_sent_at, now(), now(), NULL, now()
    FROM unnest(p_workflow_ids, p_names, p_definition_keys, p_on_errors, p_output_task_indexes, p_success_policies,
                p_parent_workflow_ids, p_parent_task_indexes, p_depths)
         WITH ORDINALITY AS c(id, name, definition_key, on_error, output_index, policy, parent_id, parent_index,
                              depth, position)
    WHERE c.position > 1
    ORDER BY c.depth, c.id;
    RETURN TRUE;
END;
$$;

-- Internal: everything after the inserts of a start. Each failed-node record, deepest workflow first: the root node
-- goes FAILED with its envelope, then progression. Each RUNNING workflow: its READY nodes are enqueued with their task
-- rows minted (a workflow that a failure paused keeps them READY: the pause gate). Each workflow with a terminal node,
-- deepest first: the completion check, and the parent advance of a child that ended. Refuses (22023) a record that
-- names no sub-workflow root of the tree without a child. Returns the owed child starts.
-- Locks: the rows of the new tree only. Python: the slow-root loop, the drain and the root task insert or revert of
-- start_workflow_async (core/workflows/lifecycle.py:706-762).
CREATE FUNCTION horsies_workflow_start_progress(
    p_workflow_ids uuid[], p_failed_workflow_ids uuid[], p_failed_task_indexes integer[], p_failed_results text[]
) RETURNS SETOF horsies_workflow_step_row
    LANGUAGE plpgsql
    SET plan_cache_mode TO 'force_generic_plan'
AS $$
DECLARE
    v_failed integer;
    v_row record;
BEGIN
    UPDATE horsies_workflow_tasks n
    SET status = 'FAILED', result = f.result, completed_at = now()
    FROM unnest(p_failed_workflow_ids, p_failed_task_indexes, p_failed_results) AS f(workflow_id, task_index, result)
    WHERE n.workflow_id = f.workflow_id AND n.task_index = f.task_index AND n.status = 'READY'
      AND n.is_subworkflow AND n.sub_workflow_id IS NULL AND cardinality(n.dependencies) = 0;
    GET DIAGNOSTICS v_failed = ROW_COUNT;
    PERFORM horsies_workflow_require(
        v_failed = coalesce(cardinality(p_failed_workflow_ids), 0),
        'a failed-node record names no sub-workflow root of the tree without a child'
    );
    FOR v_row IN
        SELECT f.workflow_id, f.task_index
        FROM unnest(p_failed_workflow_ids, p_failed_task_indexes) AS f(workflow_id, task_index)
        JOIN horsies_workflows w ON w.id = f.workflow_id
        ORDER BY w.depth DESC, f.workflow_id, f.task_index
    LOOP
        RETURN QUERY SELECT * FROM horsies_workflow_progress(v_row.workflow_id, v_row.task_index, 'READ');
    END LOOP;

    FOR v_row IN
        SELECT w.id FROM horsies_workflows w
        WHERE w.id = ANY(p_workflow_ids) AND w.status = 'RUNNING'
        ORDER BY w.depth, w.id
    LOOP
        RETURN QUERY SELECT * FROM horsies_workflow_promote(
            v_row.id, NULL, NULL,
            ARRAY(SELECT n.task_index FROM horsies_workflow_tasks n
                  WHERE n.workflow_id = v_row.id AND n.status = 'READY' ORDER BY n.task_index)
        );
    END LOOP;

    -- Completion, deepest first, only for a workflow that has a terminal node (a failed record, or a build failure
    -- in the promotion above). A child that is COMPLETED or FAILED here ended in this statement: its parent node is
    -- advanced.
    FOR v_row IN
        SELECT w.id FROM horsies_workflows w
        WHERE w.id = ANY(p_workflow_ids)
          AND EXISTS (
              SELECT 1 FROM horsies_workflow_tasks n
              WHERE n.workflow_id = w.id AND n.status IN ('COMPLETED', 'FAILED', 'SKIPPED')
          )
        ORDER BY w.depth DESC, w.id
    LOOP
        PERFORM horsies_workflow_try_complete(v_row.id);
        IF v_row.id <> p_workflow_ids[1] AND EXISTS (
            SELECT 1 FROM horsies_workflows w WHERE w.id = v_row.id AND w.status IN ('COMPLETED', 'FAILED')
        ) THEN
            RETURN QUERY SELECT * FROM horsies_workflow_child_finish(v_row.id) AS r WHERE r.row_kind <> 'OUTCOME';
        END IF;
    END LOOP;
END;
$$;

-- Starts a workflow tree in one statement. Arguments: the workflow rows of the tree (the top-level row first, then
-- each child that the core pre-built for a sub-workflow root, with its parent, parent index and depth), the start
-- call time p_sent_at, every node row of the tree (dependencies, args_from and workflow_ctx_from as one JSON text per
-- node; the typed facts max_retries, execution, good_until and retention_class_key set on task nodes; the params
-- choice sub_workflow_params set on sub-workflow nodes), and the
-- failed-node records (a sub-workflow root whose child could not be built: its node, and its error envelope).
-- Steps:
-- 1. Insert the top-level row with ON CONFLICT (id) DO NOTHING. No row: return EXISTS with the stored name in
--    detail and write nothing (the core maps the same name to the existing handle and another name to
--    WORKFLOW_ID_CONFLICT). The unique key makes a concurrent second start wait for the first commit.
-- 2. Insert the child workflow rows (RUNNING, root id of the tree) and every node row: a sub-workflow node with a
--    pre-built child goes in RUNNING, linked, with started_at; any other node without dependencies goes in READY;
--    the rest PENDING. Node row ids come from horsies_uuidv7().
-- 3. Each failed-node record, deepest workflow first: the node goes FAILED with its envelope, then progression (the
--    failure policy, promotion of its dependents, completion).
-- 4. Each workflow of the tree that is still RUNNING: its READY nodes are enqueued, with their task rows minted
--    (horsies_workflow_promote). A workflow that a failure paused keeps its READY nodes with no task (the pause gate).
-- 5. Each workflow with a terminal node, deepest first: the completion check; a child that is COMPLETED or FAILED
--    now advances its parent node (horsies_workflow_child_finish), as Python's in-session drain does.
-- 6. Return STARTED.
-- Owed work: CHILD_START rows come only from step 3 or from a sub-workflow root with neither a child nor a failed
-- record; a caller that passes failed records runs this call inside BEGIN, then horsies_workflow_child_start per row,
-- then COMMIT. Any other start is one statement in autocommit.
-- Locks: the new rows only, and the unique index entry of the top-level id.
-- Python: start_workflow_async (core/workflows/lifecycle.py:250-803) and, per sub-workflow root,
-- enqueue_subworkflow_task (core/workflows/engine.py:989-1627).
CREATE FUNCTION horsies_workflow_start(
    p_workflow_ids uuid[],
    p_names text[],
    p_definition_keys text[],
    p_on_errors text[],
    p_output_task_indexes integer[],
    p_success_policies text[],
    p_parent_workflow_ids uuid[],
    p_parent_task_indexes integer[],
    p_depths integer[],
    p_sent_at timestamp with time zone,
    p_node_workflow_ids uuid[],
    p_node_task_indexes integer[],
    p_node_ids text[],
    p_node_task_names text[],
    p_node_inputs text[],
    p_node_queue_names text[],
    p_node_priorities integer[],
    p_node_dependencies text[],
    p_node_args_from text[],
    p_node_workflow_ctx_from text[],
    p_node_allow_failed_deps boolean[],
    p_node_join_types text[],
    p_node_min_successes integer[],
    p_node_task_options text[],
    p_node_max_retries integer[],
    p_node_executions text[],
    p_node_good_untils timestamp with time zone[],
    p_node_retention_class_keys text[],
    p_node_is_subworkflow boolean[],
    p_node_sub_workflow_names text[],
    p_node_sub_definition_keys text[],
    p_node_sub_workflow_params text[],
    p_failed_workflow_ids uuid[],
    p_failed_task_indexes integer[],
    p_failed_results text[]
) RETURNS SETOF horsies_workflow_step_row
    LANGUAGE plpgsql
    SET plan_cache_mode TO 'force_generic_plan'
AS $$
DECLARE
    v_top uuid := p_workflow_ids[1];
    v_name text;
BEGIN
    PERFORM horsies_workflow_require(p_sent_at IS NOT NULL, 'sent_at must not be NULL');
    PERFORM horsies_workflow_start_check(
        p_workflow_ids, p_names, p_on_errors, p_parent_workflow_ids, p_parent_task_indexes, p_depths,
        ARRAY[cardinality(p_definition_keys), cardinality(p_output_task_indexes), cardinality(p_success_policies)],
        p_node_workflow_ids, p_node_task_indexes,
        ARRAY[cardinality(p_node_task_indexes), cardinality(p_node_ids), cardinality(p_node_task_names),
              cardinality(p_node_inputs), cardinality(p_node_queue_names), cardinality(p_node_priorities),
              cardinality(p_node_dependencies), cardinality(p_node_args_from),
              cardinality(p_node_workflow_ctx_from), cardinality(p_node_allow_failed_deps),
              cardinality(p_node_join_types), cardinality(p_node_min_successes), cardinality(p_node_task_options),
              cardinality(p_node_max_retries), cardinality(p_node_executions), cardinality(p_node_good_untils),
              cardinality(p_node_retention_class_keys), cardinality(p_node_is_subworkflow),
              cardinality(p_node_sub_workflow_names), cardinality(p_node_sub_definition_keys),
              cardinality(p_node_sub_workflow_params)],
        array_position(p_node_workflow_ids, NULL) IS NOT NULL OR array_position(p_node_task_indexes, NULL) IS NOT NULL
        OR array_position(p_node_task_names, NULL) IS NOT NULL OR array_position(p_node_inputs, NULL) IS NOT NULL
        OR array_position(p_node_queue_names, NULL) IS NOT NULL OR array_position(p_node_priorities, NULL) IS NOT NULL
        OR array_position(p_node_dependencies, NULL) IS NOT NULL
        OR array_position(p_node_allow_failed_deps, NULL) IS NOT NULL
        OR array_position(p_node_join_types, NULL) IS NOT NULL
        OR array_position(p_node_is_subworkflow, NULL) IS NOT NULL,
        coalesce(p_failed_workflow_ids, ARRAY[]::uuid[]), coalesce(p_failed_task_indexes, ARRAY[]::integer[]),
        coalesce(p_failed_results, ARRAY[]::text[])
    );

    IF NOT horsies_workflow_start_workflows(
        p_workflow_ids, p_names, p_definition_keys, p_on_errors, p_output_task_indexes, p_success_policies,
        p_parent_workflow_ids, p_parent_task_indexes, p_depths, p_sent_at
    ) THEN
        SELECT w.name INTO v_name FROM horsies_workflows w WHERE w.id = v_top;
        RETURN NEXT horsies_workflow_outcome_row('EXISTS', v_top, FALSE, v_name);
        RETURN;
    END IF;
    PERFORM horsies_workflow_start_nodes(
        p_workflow_ids, p_parent_workflow_ids, p_parent_task_indexes,
        p_node_workflow_ids, p_node_task_indexes, p_node_ids, p_node_task_names, p_node_inputs, p_node_queue_names,
        p_node_priorities, p_node_dependencies, p_node_args_from, p_node_workflow_ctx_from,
        p_node_allow_failed_deps, p_node_join_types, p_node_min_successes, p_node_task_options, p_node_max_retries,
        p_node_executions, p_node_good_untils, p_node_retention_class_keys, p_node_is_subworkflow,
        p_node_sub_workflow_names, p_node_sub_definition_keys, p_node_sub_workflow_params
    );
    RETURN QUERY SELECT * FROM horsies_workflow_start_progress(
        p_workflow_ids, p_failed_workflow_ids, p_failed_task_indexes, p_failed_results
    );
    RETURN NEXT horsies_workflow_outcome_row('STARTED', v_top, FALSE, NULL);
END;
$$;
