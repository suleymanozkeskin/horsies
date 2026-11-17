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
--   fingerprint" variant), retain_rerun_input FALSE, retry_count 0, sent_at of its workflow, max_retries, execution
--   and retention_class_key from its node row, and good_until from the good_until of its stored task options. Its id
--   is horsies_uuidv7().
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
-- - CHILD_START (one per owed child start): workflow_id (the parent workflow), workflow_name (the name of the parent
--   workflow, never NULL: the errors of a child that cannot start name the workflow by it; Python reads it in the
--   node CAS as parent_workflow_name, core/workflows/engine.py:1014-1016), task_index (the sub-workflow node),
--   node_input, args_from (JSON text or NULL), sub_workflow_name, sub_definition_key, sub_workflow_params (GIVEN or
--   NOT_GIVEN), parent_depth, root_workflow_id.
-- - SOURCE (one per terminal dependency of a CHILD_START node): workflow_id, task_index (the CHILD_START node),
--   source_task_index, source_node_id, source_status, source_task_name, source_definition_key, source_result (the
--   stored text, copied), source_summary (the stored child summary text of a sub-workflow source).
-- workflow_name is NULL on OUTCOME and SOURCE rows.
-- A row type, not a function: no locks. Python: the composite horsies_phase2_disposition and the in-memory results of
-- the engine (core/workflows/engine.py); the rows replace the arrays of composite types of the first design.
CREATE TYPE horsies_workflow_step_row AS (
    row_kind text,
    disposition text,
    workflow_id uuid,
    workflow_name text,
    workflow_status text,
    advance_child_workflow_id uuid,
    detail text,
    task_index integer,
    node_input text,
    args_from text,
    sub_workflow_name text,
    sub_definition_key text,
    sub_workflow_params text,
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
-- Python's serialize_error_payload text (core/codec/error_payload.py:51-79): the error keys in the order of
-- encode_task_error, exception, error_code, data, message (tests/fixtures/stored_forms/error_data.json). p_data is
-- JSON text or NULL.
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
            'WORKFLOW_ENQUEUE_FAILED', 'WORKFLOW_RECOVERY_LIMIT_REACHED', 'WORKER_SERIALIZATION_ERROR'
        ),
        format('engine error code %L is not in the engine catalog', p_error_code)
    );
    PERFORM horsies_workflow_require(p_message IS NOT NULL, 'engine error message is NULL');
    PERFORM horsies_workflow_require(
        (p_sub_workflow_id IS NULL) = (p_sub_workflow_summary IS NULL)
        AND ((p_sub_workflow_id IS NOT NULL) = (p_error_code = 'SUBWORKFLOW_FAILED')),
        'sub_workflow_id and sub_workflow_summary are set together, only for SUBWORKFLOW_FAILED'
    );
    RETURN '{"__h_task_result__":true,"ok":null,"err":{"exception":null,"error_code":{"__builtin_task_code__":'
        || to_json(p_error_code)::text
        || '},"data":' || coalesce(p_data, 'null')
        || ',"message":' || to_json(p_message)::text
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

-- p_keys as Python's repr writes a list of str: '[' and ']' around the items, ', ' between them. Each item is quoted
-- as Python quotes a str: with "'", or with '"' when the text holds a "'" and no '"'. Inside the quotes a backslash,
-- the quote character, tab, newline and carriage return are escaped as Python escapes them, and the other characters
-- from U+0000 to U+001F and from U+007F to U+009F as \xNN. Python's repr also escapes the other characters that
-- str.isprintable() refuses (for example U+00A0, U+2028); this function writes those unchanged.
-- Locks: none.
CREATE FUNCTION horsies_workflow_python_str_list(p_keys text[]) RETURNS text
    LANGUAGE sql IMMUTABLE
AS $$
    SELECT '[' || coalesce(string_agg(quoted.item_text, ', ' ORDER BY item.item_position), '') || ']'
    FROM unnest(p_keys) WITH ORDINALITY AS item(value, item_position)
    CROSS JOIN LATERAL (
        SELECT CASE
            WHEN strpos(item.value, chr(39)) > 0 AND strpos(item.value, chr(34)) = 0 THEN chr(34)
            ELSE chr(39)
        END AS mark
    ) AS quote
    CROSS JOIN LATERAL (
        SELECT quote.mark || coalesce(string_agg(
            CASE
                WHEN c.ch = chr(92) THEN chr(92) || chr(92)
                WHEN c.ch = chr(39) AND quote.mark = chr(39) THEN chr(92) || chr(39)
                WHEN c.ch = chr(9) THEN chr(92) || 't'
                WHEN c.ch = chr(10) THEN chr(92) || 'n'
                WHEN c.ch = chr(13) THEN chr(92) || 'r'
                WHEN ascii(c.ch) < 32 OR ascii(c.ch) BETWEEN 127 AND 159
                    THEN chr(92) || 'x' || lpad(to_hex(ascii(c.ch)), 2, '0')
                ELSE c.ch
            END,
            '' ORDER BY c.char_position
        ), '') || quote.mark AS item_text
        FROM regexp_split_to_table(item.value, '') WITH ORDINALITY AS c(ch, char_position)
        WHERE c.ch <> ''
    ) AS quoted
$$;

-- The cause of a stored result text that horsies_workflow_result_kind reads as 'UNREADABLE', in the words and the
-- order of the checks of Python's _decode_err_only (core/workflows/engine.py:324-377), loads_json
-- (core/codec/json_io.py:97-137) and validate_task_result_envelope (core/codec/typed.py:1133-1180):
-- 1. text that the json input refuses: 'Stored task result is not JSON: JSON parse failed: <the database's text>';
--    Python writes the text of its json module there, which the database cannot give;
-- 2. a top-level object that repeats a key: 'Stored task result is not JSON: JSON parse failed: JSON object repeats
--    a key';
-- 3. a value that is not an object: 'Stored task result does not decode: TaskResult envelope must be a JSON object;
--    got <Python type name>';
-- 4. no __h_task_result__ marker, or a marker other than true: 'Stored task result does not decode: TaskResult
--    envelope missing '__h_task_result__' marker (or marker is not True); got keys <sorted keys>';
-- 5. a missing ok or err key: 'Stored task result does not decode: TaskResult envelope missing required keys
--    <sorted missing keys>; got <sorted keys>';
-- 6. another key: 'Stored task result does not decode: unexpected keys in TaskResult envelope: <sorted other keys>';
-- 7. both slots set: 'Stored task result does not decode: TaskResult envelope has both ok and err populated; exactly
--    one must be set';
-- 8. an err slot that is not an object with an error_code: 'Stored task error does not decode: the err slot is not
--    an object with an error_code'; Python writes the text of its model validation there.
-- Key lists are sorted by code point and written by horsies_workflow_python_str_list.
-- Precondition: horsies_workflow_result_kind(p_result) is 'UNREADABLE'. The parse runs in a subtransaction, so this
-- serves cold paths only (the workflow error of a failed node).
-- Locks: none.
CREATE FUNCTION horsies_workflow_unreadable_result_cause(p_result text) RETURNS text
    LANGUAGE plpgsql IMMUTABLE
AS $$
DECLARE
    c_shape_prefix CONSTANT text := 'Stored task result does not decode: ';
    v_value json;
    v_keys text[];
    v_missing text[];
    v_extra text[];
BEGIN
    BEGIN
        v_value := p_result::json;
    EXCEPTION
        WHEN invalid_text_representation THEN
            RETURN 'Stored task result is not JSON: JSON parse failed: ' || SQLERRM;
    END;
    IF json_typeof(v_value) <> 'object' THEN
        RETURN c_shape_prefix || 'TaskResult envelope must be a JSON object; got '
            || horsies_workflow_json_type_name(v_value);
    END IF;
    v_keys := ARRAY(
        SELECT DISTINCT k.key COLLATE "C" FROM json_object_keys(v_value) AS k(key) ORDER BY 1
    );
    IF (SELECT count(*) FROM json_object_keys(v_value)) <> cardinality(v_keys) THEN
        RETURN 'Stored task result is not JSON: JSON parse failed: JSON object repeats a key';
    END IF;
    IF json_typeof(v_value -> '__h_task_result__') IS DISTINCT FROM 'boolean'
        OR (v_value ->> '__h_task_result__') <> 'true' THEN
        RETURN c_shape_prefix || 'TaskResult envelope missing ''__h_task_result__'' marker (or marker is not True); '
            || 'got keys ' || horsies_workflow_python_str_list(v_keys);
    END IF;
    v_missing := ARRAY(
        SELECT e.key FROM unnest(ARRAY['__h_task_result__', 'err', 'ok']) AS e(key)
        WHERE e.key <> ALL (v_keys) ORDER BY e.key COLLATE "C"
    );
    IF cardinality(v_missing) > 0 THEN
        RETURN c_shape_prefix || 'TaskResult envelope missing required keys '
            || horsies_workflow_python_str_list(v_missing) || '; got ' || horsies_workflow_python_str_list(v_keys);
    END IF;
    v_extra := ARRAY(
        SELECT k.key FROM unnest(v_keys) AS k(key)
        WHERE k.key NOT IN ('__h_task_result__', 'err', 'ok') ORDER BY k.key COLLATE "C"
    );
    IF cardinality(v_extra) > 0 THEN
        RETURN c_shape_prefix || 'unexpected keys in TaskResult envelope: '
            || horsies_workflow_python_str_list(v_extra);
    END IF;
    IF json_typeof(v_value -> 'ok') <> 'null' AND json_typeof(v_value -> 'err') <> 'null' THEN
        RETURN c_shape_prefix || 'TaskResult envelope has both ok and err populated; exactly one must be set';
    END IF;
    RETURN 'Stored task error does not decode: the err slot is not an object with an error_code';
END;
$$;

-- The error of a stored node result, as the bare error object text, for the workflow error column. Python:
-- _decode_err_only (core/workflows/engine.py:324-377) and _recovered_failure_result
-- (core/workflows/phase2_recovery.py:233-292). Returns the err slot text unchanged for 'ERROR'; NULL for 'VALUE',
-- 'ABSENT' and 'OUTPUTLESS' (no error to report: the caller keeps the stored error); for 'UNREADABLE', a
-- RESULT_DESERIALIZATION_ERROR object whose message is the cause (horsies_workflow_unreadable_result_cause).
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
                'RESULT_DESERIALIZATION_ERROR', horsies_workflow_unreadable_result_cause(p_result), NULL
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
-- ENQUEUED with no task: the rows are built here (ids from horsies_uuidv7() in index order, the input of
-- horsies_workflow_task_input) and inserted through horsies_insert_task_rows, the one column list of a new task row,
-- with the values of a workflow task row: is_workflow_task TRUE, no command fingerprint (the "no fingerprint"
-- variant), no key digest, no lineage, retain_rerun_input FALSE, sent_at of the workflow, enqueued_at, created_at and
-- updated_at p_row_time, max_retries, execution and retention_class_key from the node row, and good_until cast from
-- the good_until member of the stored task options (NULL for no options, empty options, or a null or absent member).
-- Then one link UPDATE of the nodes. The insert trigger sends the wake of each queue.
-- p_row_time is the caller's: horsies_workflow_promote passes now(), the start of the transaction, as Python's
-- workflow task insert in its multi-statement transaction. A NULL p_row_time with a row to mint raises 22023.
-- Raises, and nothing commits:
-- - a good_until that the timestamptz input cannot read (22007 or 22008), as the database refuses Python's text;
-- - an outcome other than INSERTED (a conflict on a freshly minted id), a defect: unique_violation (23505), as the
--   plain insert did.
-- Returns the number of rows minted.
-- Precondition: the caller holds the workflow row FOR UPDATE, so no other transaction links these nodes. The stored
-- task options of each node parse (horsies_workflow_unbuildable_nodes failed every node whose options do not).
-- Python: the INSERT_AND_LINK_WORKFLOW_TASK_SQL executemany (core/workflows/sql.py:194-206;
-- core/workflows/engine.py:2406-2461), INSERT_TASK_FOR_WORKFLOW_SQL at start (core/workflows/lifecycle.py:758); the
-- task row's good_until is str() of the options member (core/workflows/engine.py:832-835).
-- Locks: the new task rows and the linked node rows (the workflow row is held by the caller).
CREATE FUNCTION horsies_workflow_node_tasks_insert(
    p_workflow_id uuid, p_task_indexes integer[], p_row_time timestamp with time zone
) RETURNS integer
    LANGUAGE plpgsql
    SET plan_cache_mode TO 'force_generic_plan'
AS $$
DECLARE
    v_minted bigint;
    v_refused bigint;
BEGIN
    IF p_task_indexes IS NULL OR cardinality(p_task_indexes) = 0 THEN
        RETURN 0;
    END IF;
    -- One statement: build, insert through the helper, link. The ORDER BY of the materialized build runs before its
    -- volatile target list, so the ids are minted in index order (horsies_uuidv7 strictly increases within one
    -- transaction). The aggregates of built_rows read the build in one scan, so their arrays are aligned element by
    -- element.
    -- The link has no foreign key to the task row, so its order against the insert does not matter.
    WITH built AS MATERIALIZED (
        SELECT n.id AS node_row_id,
               horsies_uuidv7() AS task_id,
               n.task_index, n.task_name, n.queue_name, n.priority,
               horsies_workflow_task_input(n) AS input,
               n.task_options, n.execution, n.retention_class_key, n.max_retries,
               (nullif(n.task_options, '')::jsonb ->> 'good_until')::timestamptz AS good_until
        FROM horsies_workflow_tasks n
        WHERE n.workflow_id = p_workflow_id
          AND n.task_index = ANY(p_task_indexes)
          AND n.status = 'ENQUEUED'
          AND NOT n.is_subworkflow
          AND n.task_id IS NULL
        ORDER BY n.task_index
    ),
    built_rows AS (
        SELECT array_agg(b.task_id) AS task_ids,
               array_agg(b.task_name::text) AS task_names,
               array_agg(b.queue_name::text) AS queue_names,
               array_agg(b.priority) AS priorities,
               array_agg(b.input) AS inputs,
               array_agg(b.task_options) AS task_options,
               array_agg(b.execution) AS executions,
               array_agg(b.retention_class_key::text) AS retention_class_keys,
               array_agg(b.max_retries) AS max_retries,
               array_agg(b.good_until) AS good_untils,
               count(*)::integer AS row_count,
               (SELECT w.sent_at FROM horsies_workflows w WHERE w.id = p_workflow_id) AS sent_at
        FROM built b
    ),
    inserted AS (
        SELECT r.outcome
        FROM built_rows b
        CROSS JOIN LATERAL horsies_insert_task_rows(
            b.task_ids, b.task_names, b.queue_names, b.priorities, b.inputs, b.task_options, b.executions,
            b.retention_class_keys, array_fill(FALSE, ARRAY[b.row_count]), b.max_retries, b.good_untils,
            array_fill(b.sent_at, ARRAY[b.row_count]), array_fill(p_row_time, ARRAY[b.row_count]),
            array_fill(TRUE, ARRAY[b.row_count]), array_fill(NULL::smallint, ARRAY[b.row_count]),
            array_fill(NULL::bytea, ARRAY[b.row_count]), array_fill(NULL::bytea, ARRAY[b.row_count]),
            array_fill(NULL::uuid, ARRAY[b.row_count]), array_fill(NULL::uuid, ARRAY[b.row_count]), p_row_time,
            'NONE'
        ) AS r
        WHERE b.row_count > 0
    ),
    linked AS (
        UPDATE horsies_workflow_tasks wt
        SET task_id = b.task_id
        FROM built b
        WHERE wt.id = b.node_row_id
        RETURNING wt.id
    )
    SELECT (SELECT count(*) FROM linked), (SELECT count(*) FROM inserted i WHERE i.outcome <> 'INSERTED')
    INTO v_minted, v_refused;
    IF v_refused > 0 THEN
        RAISE EXCEPTION 'workflow % minted % task ids that are already taken', p_workflow_id, v_refused
            USING ERRCODE = 'unique_violation';
    END IF;
    RETURN v_minted;
END;
$$;

------------------------------------------------------------------------------------------------------------------------
-- Internal helpers: the stored task options of a task node
------------------------------------------------------------------------------------------------------------------------

-- The name of the Python type that json.loads gives for p_value: dict, list, str, int, float, bool or NoneType; NULL
-- for NULL. A number is a float when its literal has a fraction or an exponent (the number rule of Python's json
-- module), else an int. The json type keeps the literal; jsonb does not (it reads 1e2 as 100).
-- Locks: none. Python: type(value).__name__ in the texts of core/codec/task_options.py.
CREATE FUNCTION horsies_workflow_json_type_name(p_value json) RETURNS text
    LANGUAGE sql IMMUTABLE
AS $$
    SELECT CASE json_typeof(p_value)
        WHEN 'object' THEN 'dict'
        WHEN 'array' THEN 'list'
        WHEN 'string' THEN 'str'
        WHEN 'boolean' THEN 'bool'
        WHEN 'null' THEN 'NoneType'
        WHEN 'number' THEN CASE WHEN p_value::text ~ '[.eE]' THEN 'float' ELSE 'int' END
    END
$$;

-- p_value as a number when it is a JSON integer literal (an int for Python's json module), else NULL.
-- Locks: none.
CREATE FUNCTION horsies_workflow_json_int(p_value json) RETURNS numeric
    LANGUAGE sql IMMUTABLE
AS $$
    SELECT CASE WHEN horsies_workflow_json_type_name(p_value) = 'int' THEN p_value::text::numeric END
$$;

-- TRUE when the parsed text p_doc breaks a rule of Python's loads_json (core/codec/json_io.py) that the PostgreSQL
-- json and jsonb inputs do not apply:
-- - an object repeats a key;
-- - an integer literal has more than 4300 digits (Python's limit for an int conversion);
-- - a float literal rounds to infinity: its magnitude is at least 2^1024 - 2^970, the midpoint between the largest
--   double and 2^1024;
-- - lists and objects nest deeper than 300 levels (MAX_JSON_DEPTH of core/codec/json_value.py).
-- The walk expands containers down to depth 300 only, so it is bounded by the size of p_doc.
-- Precondition: the text of p_doc also parses as jsonb, so every number literal casts to numeric.
-- Locks: none.
CREATE FUNCTION horsies_workflow_json_breaks_strict_rules(p_doc json) RETURNS boolean
    LANGUAGE sql IMMUTABLE
AS $$
    WITH RECURSIVE walk (value, kind, depth) AS (
        SELECT p_doc, json_typeof(p_doc), 1
        UNION ALL
        SELECT c.value, json_typeof(c.value), w.depth + 1
        FROM walk w
        CROSS JOIN LATERAL (
            SELECT e.value FROM json_each(CASE WHEN w.kind = 'object' THEN w.value END) AS e
            UNION ALL
            SELECT a.value FROM json_array_elements(CASE WHEN w.kind = 'array' THEN w.value END) AS a
        ) AS c
        WHERE w.kind IN ('object', 'array') AND w.depth <= 300
    )
    SELECT EXISTS (
        SELECT 1
        FROM walk w
        WHERE CASE w.kind
            WHEN 'object' THEN w.depth > 300
                OR (SELECT count(*) <> count(DISTINCT k) FROM json_object_keys(w.value) AS k)
            WHEN 'array' THEN w.depth > 300
            WHEN 'number' THEN CASE
                WHEN w.value::text ~ '[.eE]' THEN abs(w.value::text::numeric) >= '1797693134862315807937289714053034'
                    '1507993413271003782693617377898044496829276475094664901797758720709633028641669288791094655554'
                    '7851940402630657488671505820681908902000708383676273854845817711531764475730270069855571366959'
                    '622842914819860834936475292719074168444365510704342711559699508093042880177904174497792'::numeric
                ELSE length(regexp_replace(w.value::text, '[^0-9]', '', 'g')) > 4300
            END
            ELSE FALSE
        END
    )
$$;

-- The stored task options p_text parsed by the rules of Python's loads_json, or NULL when it does not parse. The text
-- must parse as json and as jsonb (jsonb also refuses an unpaired UTF-16 surrogate escape) and keep the rules of
-- horsies_workflow_json_breaks_strict_rules. The json value keeps the literals and the member order of the text.
-- The walk of those rules runs only when one of them can be broken. With the string literals removed from the text:
-- the member count (one ':' per member) differs from the member count of the jsonb value, which keeps one member per
-- key; or more than 300 '[' and '{' are left; or a digit is followed by '.', 'e' or 'E' (a float literal); or more
-- than 4300 characters are left.
-- PostgreSQL refuses two kinds of text that Python reads: the escape \u0000 (text cannot hold U+0000), and a number
-- literal whose exponent is outside the range of numeric (Python reads 1e-20000 as the float 0.0).
-- The parse runs in a subtransaction. Precondition: p_text is not NULL or empty (Python reads no options then).
-- Locks: none. Python: _deser_json (core/workflows/engine.py:415-423) and loads_json (core/codec/json_io.py:97-137).
CREATE FUNCTION horsies_workflow_options_json(p_text text) RETURNS json
    LANGUAGE plpgsql IMMUTABLE
AS $$
DECLARE
    c_max_depth CONSTANT integer := 300;
    c_max_int_digits CONSTANT integer := 4300;
    v_doc json;
    v_distinct_members integer;
    v_bare text;
BEGIN
    v_doc := p_text::json;
    v_distinct_members := jsonb_array_length(
        jsonb_path_query_array(p_text::jsonb, 'strict $.** ? (@.type() == "object").keyvalue()')
    );
    -- The E'' literal keeps its backslashes whatever standard_conforming_strings is.
    v_bare := regexp_replace(p_text, E'"(\\\\.|[^"\\\\])*"', '', 'g');
    IF length(v_bare) - length(replace(v_bare, ':', '')) <> v_distinct_members
       OR length(v_bare) - length(translate(v_bare, '[{', '')) > c_max_depth
       OR v_bare ~ '[0-9][.eE]'
       OR length(v_bare) > c_max_int_digits
    THEN
        IF horsies_workflow_json_breaks_strict_rules(v_doc) THEN
            RETURN NULL;
        END IF;
    END IF;
    RETURN v_doc;
EXCEPTION
    WHEN data_exception OR statement_too_complex THEN
        RETURN NULL;
END;
$$;

-- The validation entries of the intervals member p_intervals of a retry_policy object, as validation_summary writes
-- them: none when absent; one list_type entry for a value that is not a list; one too_long entry (with the length) for
-- a list longer than 20, whatever its items; one too_short entry for an empty list; else one entry per item that is
-- not an int from 1 to 86400, at 'intervals.<position from 0>'. Strict mode refuses a bool or a float for an int.
-- A list whose text holds only int literals from 1 to 86400 has no item entry, and its items are not walked.
-- Locks: none. Python: RetryPolicy.intervals (core/models/tasks.py:549-557).
CREATE FUNCTION horsies_workflow_retry_interval_entries(p_intervals json) RETURNS text[]
    LANGUAGE plpgsql IMMUTABLE
AS $$
BEGIN
    CASE
        WHEN p_intervals IS NULL THEN
            RETURN ARRAY[]::text[];
        WHEN json_typeof(p_intervals) <> 'array' THEN
            RETURN ARRAY['intervals: Input should be a valid list [list_type]'];
        WHEN json_array_length(p_intervals) > 20 THEN
            RETURN ARRAY[format(
                'intervals: List should have at most 20 items after validation, not %s [too_long]',
                json_array_length(p_intervals)
            )];
        WHEN json_array_length(p_intervals) = 0 THEN
            RETURN ARRAY['intervals: List should have at least 1 item after validation, not 0 [too_short]'];
        -- An int literal from 1 to 86400 is one of 1-9999, 10000-79999, 80000-85999, 86000-86399 and 86400.
        WHEN p_intervals::text ~ (
            '^[[][[:space:]]*([1-9][0-9]{0,3}|[1-7][0-9]{4}|8[0-5][0-9]{3}|86[0-3][0-9]{2}|86400)'
            '([[:space:]]*,[[:space:]]*([1-9][0-9]{0,3}|[1-7][0-9]{4}|8[0-5][0-9]{3}|86[0-3][0-9]{2}|86400))*'
            '[[:space:]]*[]]$'
        ) THEN
            RETURN ARRAY[]::text[];
        ELSE
            RETURN ARRAY(
                SELECT format('intervals.%s: %s', i.position - 1, CASE
                    WHEN horsies_workflow_json_int(i.value) IS NULL THEN 'Input should be a valid integer [int_type]'
                    WHEN horsies_workflow_json_int(i.value) < 1 THEN 'Input should be greater than 0 [greater_than]'
                    ELSE 'Input should be less than or equal to 86400 [less_than_equal]'
                END)
                FROM json_array_elements(p_intervals) WITH ORDINALITY AS i(value, position)
                WHERE horsies_workflow_json_int(i.value) IS NULL
                   OR horsies_workflow_json_int(i.value) NOT BETWEEN 1 AND 86400
                ORDER BY i.position
            );
    END CASE;
END;
$$;

-- TRUE when the text of p_codes is a list of one or more UPPER_SNAKE_CASE string literals with no escape: each item
-- is a str that matches ERROR_CODE_RE (core/exception_mapper.py:14) and, having no lowercase letter, cannot match
-- EXCEPTION_NAME_RE. FALSE says nothing: the caller then walks the items. Locks: none.
CREATE FUNCTION horsies_workflow_retry_codes_plain(p_codes json) RETURNS boolean
    LANGUAGE sql IMMUTABLE
AS $$
    SELECT p_codes::text ~ (
        '^[[][[:space:]]*"[A-Z][A-Z0-9_]*"([[:space:]]*,[[:space:]]*"[A-Z][A-Z0-9_]*")*[[:space:]]*[]]$'
    )
$$;

-- The validation entries of the auto_retry_for member p_codes of a retry_policy object, as validation_summary writes
-- them: one missing entry when absent; one list_type entry for a value that is not a list; one too_short entry for an
-- empty list; else five entries per item that is not a str, at 'auto_retry_for.<position from 0>': the str member and
-- the four str enums of BuiltInTaskCode of Union[str, BuiltInTaskCode], in pydantic's order. Plain codes
-- (horsies_workflow_retry_codes_plain) have no item entry, and their items are not walked.
-- Locks: none. Python: RetryPolicy.auto_retry_for (core/models/tasks.py:564-567), BuiltInTaskCode (:58).
CREATE FUNCTION horsies_workflow_retry_code_entries(p_codes json) RETURNS text[]
    LANGUAGE plpgsql IMMUTABLE
AS $$
BEGIN
    CASE
        WHEN p_codes IS NULL THEN
            RETURN ARRAY['auto_retry_for: Field required [missing]'];
        WHEN json_typeof(p_codes) <> 'array' THEN
            RETURN ARRAY['auto_retry_for: Input should be a valid list [list_type]'];
        WHEN json_array_length(p_codes) = 0 THEN
            RETURN ARRAY['auto_retry_for: List should have at least 1 item after validation, not 0 [too_short]'];
        WHEN horsies_workflow_retry_codes_plain(p_codes) THEN
            RETURN ARRAY[]::text[];
        ELSE
            RETURN ARRAY(
                SELECT format('auto_retry_for.%s.%s', c.position - 1, m.entry)
                FROM json_array_elements(p_codes) WITH ORDINALITY AS c(value, position)
                CROSS JOIN (
                    SELECT 0 AS member, 'str: Input should be a valid string [string_type]' AS entry
                    UNION ALL
                    SELECT e.member, format(
                        'union[str-enum[OperationalErrorCode],str-enum[ContractCode],str-enum[RetrievalCode],'
                        'str-enum[OutcomeCode]].str-enum[%1$s]: Input should be an instance of %1$s [is_instance_of]',
                        e.enum_name
                    )
                    FROM unnest(ARRAY['OperationalErrorCode', 'ContractCode', 'RetrievalCode', 'OutcomeCode'])
                        WITH ORDINALITY AS e(enum_name, member)
                ) AS m
                WHERE json_typeof(c.value) <> 'string'
                ORDER BY c.position, m.member
            );
    END CASE;
END;
$$;

-- The field entries of a retry_policy object p_policy, as Python's validation_summary writes the field errors of
-- RetryPolicy.model_validate(policy, strict=True) (core/codec/validation_errors.py:95-115): one '<loc>: <msg> [<type>]'
-- per error, the fields in declaration order (max_retries: an int from 1 to 20; intervals; backoff_strategy: 'fixed'
-- or 'exponential'; jitter: a bool; max_delay_seconds: null or an int above 0; auto_retry_for), then one entry per
-- member that is not a field, in the order of the text (an empty key has no '<loc>: '). Empty when the fields
-- validate. Precondition: p_policy is an object with no repeated key.
-- Locks: none. Python: RetryPolicy (core/models/tasks.py:522-567, extra='forbid').
CREATE FUNCTION horsies_workflow_retry_policy_entries(p_policy json) RETURNS text[]
    LANGUAGE plpgsql IMMUTABLE
AS $$
DECLARE
    c_fields CONSTANT text[] := ARRAY[
        'max_retries', 'intervals', 'backoff_strategy', 'jitter', 'max_delay_seconds', 'auto_retry_for'
    ];
    v_max_retries json := p_policy -> 'max_retries';
    v_backoff_strategy json := p_policy -> 'backoff_strategy';
    v_jitter json := p_policy -> 'jitter';
    v_max_delay_seconds json := p_policy -> 'max_delay_seconds';
    v_extras text[] := ARRAY[]::text[];
BEGIN
    IF p_policy::jsonb - c_fields <> '{}'::jsonb THEN
        v_extras := ARRAY(
            SELECT CASE WHEN e.key = '' THEN '' ELSE e.key || ': ' END
                || 'Extra inputs are not permitted [extra_forbidden]'
            FROM json_each(p_policy) WITH ORDINALITY AS e(key, value, position)
            WHERE e.key <> ALL (c_fields)
            ORDER BY e.position
        );
    END IF;
    RETURN array_remove(ARRAY[
               CASE
                   WHEN v_max_retries IS NULL THEN NULL
                   WHEN horsies_workflow_json_int(v_max_retries) IS NULL
                       THEN 'max_retries: Input should be a valid integer [int_type]'
                   WHEN horsies_workflow_json_int(v_max_retries) < 1
                       THEN 'max_retries: Input should be greater than or equal to 1 [greater_than_equal]'
                   WHEN horsies_workflow_json_int(v_max_retries) > 20
                       THEN 'max_retries: Input should be less than or equal to 20 [less_than_equal]'
               END
           ], NULL)
        || horsies_workflow_retry_interval_entries(p_policy -> 'intervals')
        || array_remove(ARRAY[
               CASE
                   WHEN v_backoff_strategy IS NULL THEN NULL
                   WHEN json_typeof(v_backoff_strategy) = 'string'
                        AND (v_backoff_strategy #>> '{}') IN ('fixed', 'exponential') THEN NULL
                   ELSE 'backoff_strategy: Input should be ''fixed'' or ''exponential'' [literal_error]'
               END,
               CASE
                   WHEN v_jitter IS NULL OR json_typeof(v_jitter) = 'boolean' THEN NULL
                   ELSE 'jitter: Input should be a valid boolean [bool_type]'
               END,
               CASE
                   WHEN v_max_delay_seconds IS NULL OR json_typeof(v_max_delay_seconds) = 'null' THEN NULL
                   WHEN horsies_workflow_json_int(v_max_delay_seconds) IS NULL
                       THEN 'max_delay_seconds: Input should be a valid integer [int_type]'
                   WHEN horsies_workflow_json_int(v_max_delay_seconds) < 1
                       THEN 'max_delay_seconds: Input should be greater than 0 [greater_than]'
               END
           ], NULL)
        || horsies_workflow_retry_code_entries(p_policy -> 'auto_retry_for')
        || v_extras;
END;
$$;

-- The model validator error of a retry_policy object p_policy whose fields validate, as validation_summary writes it
-- ('Value error, <text> [value_error]'), or NULL. The validators run in Python's order; the first error ends the
-- validation:
-- 1. validate_strategy_consistency: 'fixed' needs as many intervals as max_retries, 'exponential' needs one interval.
--    An absent field has its default: max_retries 3, intervals [60, 300, 900], backoff_strategy 'fixed'.
-- 2. validate_error_code_fields: each auto_retry_for code in order, by validate_error_code_string
--    (core/exception_mapper.py:64-84): not empty, not an exception class name (EXCEPTION_NAME_RE), UPPER_SNAKE_CASE
--    (ERROR_CODE_RE), and not in RESERVED_NON_TASK_CODES (WORKFLOW_STOPPED). Plain codes
--    (horsies_workflow_retry_codes_plain) other than WORKFLOW_STOPPED pass all four checks and are not walked.
-- Precondition: horsies_workflow_retry_policy_entries(p_policy) is empty.
-- Locks: none. Python: RetryPolicy (core/models/tasks.py:569-600).
CREATE FUNCTION horsies_workflow_retry_policy_value_error(p_policy json) RETURNS text
    LANGUAGE plpgsql IMMUTABLE
AS $$
DECLARE
    v_backoff_strategy text := coalesce(p_policy ->> 'backoff_strategy', 'fixed');
    v_max_retries integer := coalesce((p_policy ->> 'max_retries')::integer, 3);
    v_interval_count integer := coalesce(json_array_length(p_policy -> 'intervals'), 3);
    v_codes json := p_policy -> 'auto_retry_for';
    v_error text;
BEGIN
    v_error := CASE v_backoff_strategy
        WHEN 'fixed' THEN CASE WHEN v_interval_count <> v_max_retries THEN format(
            'Fixed backoff strategy requires intervals length (%s) to match max_retries (%s). '
            'Either adjust intervals list or use exponential strategy.', v_interval_count, v_max_retries) END
        WHEN 'exponential' THEN CASE WHEN v_interval_count <> 1 THEN format(
            'Exponential backoff strategy requires exactly one base interval, got %s intervals. '
            'Use intervals=[base_seconds] for exponential backoff.', v_interval_count) END
    END;
    IF v_error IS NULL AND NOT (
        horsies_workflow_retry_codes_plain(v_codes) AND strpos(v_codes::text, '"WORKFLOW_STOPPED"') = 0
    ) THEN
        SELECT CASE
            WHEN c.code = '' THEN 'auto_retry_for must be a non-empty string, got '''''
            WHEN c.code ~ '^[A-Z][A-Za-z0-9_]*(Error|Exception)$' THEN format(
                'auto_retry_for ''%s'' looks like an exception class name; retry matching is '
                'error-code-only, use UPPER_SNAKE_CASE code names', c.code)
            WHEN c.code !~ '^[A-Z][A-Z0-9_]*$' THEN format(
                'auto_retry_for ''%s'' is invalid; expected UPPER_SNAKE_CASE (e.g. RATE_LIMITED)', c.code)
            ELSE format(
                'auto_retry_for ''%s'' is a reserved error code name and is not a task code; '
                'use another UPPER_SNAKE_CASE code', c.code)
        END
        INTO v_error
        FROM json_array_elements_text(v_codes) WITH ORDINALITY AS c(code, position)
        WHERE c.code = '' OR c.code ~ '^[A-Z][A-Za-z0-9_]*(Error|Exception)$' OR c.code !~ '^[A-Z][A-Z0-9_]*$'
           OR c.code = 'WORKFLOW_STOPPED'
        ORDER BY c.position
        LIMIT 1;
    END IF;
    RETURN 'Value error, ' || v_error || ' [value_error]';
END;
$$;

-- The refusal of the retry_policy member p_policy of task options, as Python's stored_retry_policy writes it, or NULL
-- when it reads: none for an absent or null member; 'retry_policy is not an object: <type>' for a value that is not an
-- object; else the field entries of horsies_workflow_retry_policy_entries joined by '; ' (at most 10, then
-- '(+<count> more)', MAX_VALIDATION_ERRORS_IN_TEXT), or when the fields validate, the model validator error.
-- Locks: none. Python: stored_retry_policy and stored_max_retries (core/codec/task_options.py:85-136).
CREATE FUNCTION horsies_workflow_retry_policy_refusal(p_policy json) RETURNS text
    LANGUAGE plpgsql IMMUTABLE
AS $$
DECLARE
    c_max_entries CONSTANT integer := 10;
    v_entries text[];
BEGIN
    IF p_policy IS NULL OR json_typeof(p_policy) = 'null' THEN
        RETURN NULL;
    END IF;
    IF json_typeof(p_policy) <> 'object' THEN
        RETURN 'retry_policy is not an object: ' || horsies_workflow_json_type_name(p_policy);
    END IF;
    v_entries := horsies_workflow_retry_policy_entries(p_policy);
    IF cardinality(v_entries) = 0 THEN
        RETURN horsies_workflow_retry_policy_value_error(p_policy);
    END IF;
    RETURN array_to_string(v_entries[1:c_max_entries], '; ')
        || CASE
            WHEN cardinality(v_entries) > c_max_entries
                THEN format('; (+%s more)', cardinality(v_entries) - c_max_entries)
            ELSE ''
        END;
END;
$$;

-- The refusal of the execution class of a task options object p_options, as Python's stored_execution writes it, or
-- NULL when it reads. An absent execution member is ISOLATED; a present one must be one of the Execution values.
-- cancel_grace_ms (absent is null) must then fit the class: an int from 100 to 10000 for SHARED_ASYNC
-- (MIN_CANCEL_GRACE_MS and MAX_CANCEL_GRACE_MS, core/models/tasks.py:479-487), null for the other classes.
-- Locks: none. Python: stored_execution and its helpers (core/codec/task_options.py:147-240).
CREATE FUNCTION horsies_workflow_execution_refusal(p_options json) RETURNS text
    LANGUAGE plpgsql IMMUTABLE
AS $$
DECLARE
    c_min_grace_ms CONSTANT integer := 100;
    c_max_grace_ms CONSTANT integer := 10000;
    v_execution json := p_options -> 'execution';
    v_grace json := coalesce(p_options -> 'cancel_grace_ms', 'null');
    v_class text;
BEGIN
    IF v_execution IS NOT NULL AND json_typeof(v_execution) <> 'string' THEN
        RETURN format('execution is a JSON %s, not a string', horsies_workflow_json_type_name(v_execution));
    END IF;
    v_class := coalesce(v_execution #>> '{}', 'ISOLATED');
    CASE v_class
        WHEN 'ISOLATED', 'SHARED_THREAD' THEN
            RETURN CASE
                WHEN json_typeof(v_grace) = 'null' THEN NULL
                ELSE format('cancel_grace_ms is set for execution %s; only SHARED_ASYNC has one', v_class)
            END;
        WHEN 'SHARED_ASYNC' THEN
            RETURN CASE
                WHEN json_typeof(v_grace) = 'null' THEN 'cancel_grace_ms is absent for execution SHARED_ASYNC'
                WHEN json_typeof(v_grace) = 'boolean' THEN 'cancel_grace_ms is a bool, not an int'
                WHEN horsies_workflow_json_int(v_grace) IS NULL
                    THEN format('cancel_grace_ms is a JSON %s, not an int', horsies_workflow_json_type_name(v_grace))
                WHEN horsies_workflow_json_int(v_grace) < c_min_grace_ms
                    THEN format('cancel_grace_ms is below %s ms', c_min_grace_ms)
                WHEN horsies_workflow_json_int(v_grace) > c_max_grace_ms
                    THEN format('cancel_grace_ms is above %s ms', c_max_grace_ms)
            END;
        ELSE
            RETURN 'execution is not one of ISOLATED, SHARED_ASYNC, SHARED_THREAD';
    END CASE;
END;
$$;

-- The message with which the promotion fails a task node whose stored task options p_task_options do not read, as
-- Python's _build_enqueued_task_params writes it for node p_task_index, or NULL when they read (NULL and empty options
-- read as no options). The checks run in Python's order; the first refusal is the message:
-- 1. text that does not parse, or the JSON null (horsies_workflow_options_json): 'Corrupt task_options JSON for task
--    <index>';
-- 2. a value that is not an object: 'Invalid task_options payload for task <index>: expected object';
-- 3. the retry_policy member (horsies_workflow_retry_policy_refusal): 'Invalid retry_policy for task <index>:
--    <detail>';
-- 4. the execution class (horsies_workflow_execution_refusal): 'Invalid execution for task <index>: <detail>'.
-- The good_until member is not checked here: the task row insert casts it (horsies_workflow_node_tasks_insert).
-- Locks: none. Python: _build_enqueued_task_params (core/workflows/engine.py:795-835).
CREATE FUNCTION horsies_workflow_task_options_refusal(p_task_options text, p_task_index integer) RETURNS text
    LANGUAGE plpgsql IMMUTABLE
AS $$
DECLARE
    v_options json;
    v_detail text;
BEGIN
    IF p_task_options IS NULL OR p_task_options = '' THEN
        RETURN NULL;
    END IF;
    v_options := horsies_workflow_options_json(p_task_options);
    IF v_options IS NULL OR json_typeof(v_options) = 'null' THEN
        RETURN format('Corrupt task_options JSON for task %s', p_task_index);
    END IF;
    IF json_typeof(v_options) <> 'object' THEN
        RETURN format('Invalid task_options payload for task %s: expected object', p_task_index);
    END IF;
    v_detail := horsies_workflow_retry_policy_refusal(v_options -> 'retry_policy');
    IF v_detail IS NOT NULL THEN
        RETURN format('Invalid retry_policy for task %s: %s', p_task_index, v_detail);
    END IF;
    v_detail := horsies_workflow_execution_refusal(v_options);
    IF v_detail IS NOT NULL THEN
        RETURN format('Invalid execution for task %s: %s', p_task_index, v_detail);
    END IF;
    RETURN NULL;
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

-- The failure policy of a workflow after one of its nodes went FAILED. p_error is the error of this failure, as the
-- bare error object text (NULL: no readable error): the err slot of the node's stored result, or for a node whose
-- child workflow ended FAILED, CANCELLED or EXPIRED the error of horsies_workflow_child_node_result. Returns TRUE when
-- progression goes on (on_error fail, or a workflow that is not RUNNING with fail), FALSE when it stops (on_error
-- pause).
-- - fail: the workflow error becomes the err slot of the first FAILED node by index, only while RUNNING; a node with
--   no readable error keeps the stored error. p_error is not used.
-- - pause: RUNNING to PAUSED with p_error (NULL keeps the stored error; a second failure keeps the first error: the
--   write needs RUNNING), and every RUNNING descendant to PAUSED. Claimed tasks of the paused tree are abandoned
--   later, at their start or claim-time check.
-- Precondition: the caller holds the workflow row FOR UPDATE and wrote the node FAILED.
-- Python: _handle_workflow_task_failure (core/workflows/engine.py:4002-4100), which stores the error it is given.
-- Locks: the workflow row (held), then the RUNNING descendant rows level by level in (depth, id) order.
CREATE FUNCTION horsies_workflow_record_failure(p_workflow_id uuid, p_error text) RETURNS boolean
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
            UPDATE horsies_workflows
            SET status = 'PAUSED', error = coalesce(p_error, error), updated_at = now()
            WHERE id = p_workflow_id AND status = 'RUNNING';
            IF FOUND THEN
                PERFORM horsies_workflow_move_descendants(p_workflow_id, 'RUNNING', 'PAUSED');
            END IF;
            RETURN FALSE;
    END CASE;
END;
$$;

-- The nodes among p_task_indexes (ENQUEUED, of p_workflow_id) whose task row or child cannot be built, with the
-- engine error of each, in index order. The first refusal of a node in Python's order:
-- - a task node whose stored task options do not read: WORKER_SERIALIZATION_ERROR with the message of
--   horsies_workflow_task_options_refusal (Python: _build_enqueued_task_params, core/workflows/engine.py:795-831);
-- - a task node with an args_from key that is also a member of its static input: WORKFLOW_ENQUEUE_FAILED
--   (Python: _build_enqueued_task_params, core/workflows/engine.py:867-872; the spec refuses the overlap when it is
--   built, this check guards stored rows);
-- - a sub-workflow node of a workflow at depth 32: SUBWORKFLOW_DEPTH_LIMIT_REACHED (Python:
--   enqueue_subworkflow_task, core/workflows/engine.py:1052-1066).
-- data names the workflow and the node, as Python's _fail_enqueued_task (core/workflows/engine.py:514-555).
-- Locks: none. Returns (task_index, error_result) rows.
CREATE FUNCTION horsies_workflow_unbuildable_nodes(p_workflow_id uuid, p_task_indexes integer[])
    RETURNS TABLE (task_index integer, error_result text)
    LANGUAGE sql STABLE
AS $$
    SELECT n.task_index,
           horsies_task_error_envelope(
               CASE
                   WHEN n.is_subworkflow THEN 'SUBWORKFLOW_DEPTH_LIMIT_REACHED'
                   WHEN o.refusal IS NOT NULL THEN 'WORKER_SERIALIZATION_ERROR'
                   ELSE 'WORKFLOW_ENQUEUE_FAILED'
               END,
               CASE
                   WHEN n.is_subworkflow THEN format(
                       'Subworkflow not started: the child workflow would have depth %s, and the limit is 32 '
                       '(workflow %s:%s)', w.depth + 1, w.name, n.task_index)
                   WHEN o.refusal IS NOT NULL THEN o.refusal
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
    -- OFFSET 0 keeps the subquery apart, so the options are read once per node.
    CROSS JOIN LATERAL (
        SELECT CASE
            WHEN NOT n.is_subworkflow THEN horsies_workflow_task_options_refusal(n.task_options, n.task_index)
        END AS refusal
        OFFSET 0
    ) AS o
    WHERE n.workflow_id = p_workflow_id
      AND n.task_index = ANY(p_task_indexes)
      AND n.status = 'ENQUEUED'
      AND (
          (n.is_subworkflow AND w.depth >= 32)
          OR o.refusal IS NOT NULL
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
-- join). A node with workflow_ctx_from waits until its context nodes are terminal. A candidate with no dependencies
-- is judged like any other: 0 of 0 dependencies are terminal, so join all is met and the node is ENQUEUED (it
-- recovers a root that a start left PENDING); it is never a dependent of p_sources. The verdict table is Python's
-- _evaluate_join and _plan_level (core/workflows/engine.py:2223-2344); the ctx gate is
-- try_make_ready_and_enqueue's (core/workflows/engine.py:2671-2683). Python's try_make_ready_and_enqueue leaves a
-- node with no dependencies PENDING; the core's rule (enqueue it) is the decided one.
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
    SELECT 'CHILD_START'::text, NULL::text, p_workflow_id, w.name::text, NULL::text, NULL::uuid, NULL::text,
           n.task_index, n.input, n.args_from::text, n.sub_workflow_name::text, n.sub_definition_key::text,
           n.sub_workflow_params::text, w.depth, coalesce(w.root_workflow_id, w.id),
           NULL::integer, NULL::text, NULL::text, NULL::text, NULL::text, NULL::text, NULL::text
    FROM horsies_workflow_tasks n
    JOIN horsies_workflows w ON w.id = n.workflow_id
    WHERE n.workflow_id = p_workflow_id
      AND n.task_index = ANY(p_task_indexes)
      AND n.status = 'ENQUEUED'
      AND n.is_subworkflow
      AND n.sub_workflow_id IS NULL
    UNION ALL
    SELECT 'SOURCE'::text, NULL::text, p_workflow_id, NULL::text, NULL::text, NULL::uuid, NULL::text,
           n.task_index, NULL::text, NULL::text, NULL::text, NULL::text, NULL::text, NULL::integer, NULL::uuid,
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
-- 4. Otherwise: the task rows of the ENQUEUED task nodes (horsies_workflow_node_tasks_insert, dated now(): the start
--    of the transaction, as started_at in step 1 and Python's workflow task insert), and one CHILD_START row (with
--    its SOURCE rows) per ENQUEUED sub-workflow node.
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
                IF NOT horsies_workflow_record_failure(
                    p_workflow_id,
                    (SELECT horsies_workflow_err_slot(n.result) FROM horsies_workflow_tasks n
                     WHERE n.workflow_id = p_workflow_id AND n.task_index = v_failed[1])
                ) THEN
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

    PERFORM horsies_workflow_node_tasks_insert(p_workflow_id, v_enqueued, now());
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

-- What a workflow does after one of its nodes was written terminal and the failure policy let progression go on:
-- the paused guard, promotion of the node's dependents (skipped when p_dependents is 'NONE': the caller knows that no
-- node depends on this one), and the completion check. Returns the owed child starts. p_dependents is 'NONE' or
-- 'READ' (22023 otherwise).
-- Precondition: the caller holds the workflow row FOR UPDATE and wrote the node.
-- Python: apply_node_progression after _handle_workflow_task_failure (core/workflows/engine.py:1973-2040).
-- Locks: as horsies_workflow_promote, under the caller's workflow lock.
CREATE FUNCTION horsies_workflow_promote_and_complete(p_workflow_id uuid, p_task_index integer, p_dependents text)
    RETURNS SETOF horsies_workflow_step_row
    LANGUAGE plpgsql
    SET plan_cache_mode TO 'force_generic_plan'
AS $$
BEGIN
    PERFORM horsies_workflow_require(p_dependents IN ('NONE', 'READ'), 'dependents must be NONE or READ');
    IF NOT EXISTS (SELECT 1 FROM horsies_workflows w WHERE w.id = p_workflow_id AND w.status = 'RUNNING') THEN
        RETURN;
    END IF;
    IF p_dependents = 'READ' THEN
        RETURN QUERY SELECT * FROM horsies_workflow_promote(p_workflow_id, ARRAY[p_task_index], NULL, NULL);
    END IF;
    PERFORM horsies_workflow_try_complete(p_workflow_id);
END;
$$;

-- Everything a workflow does after one of its nodes was written terminal: for a FAILED node the failure policy with
-- the err slot of the node's stored result as the error of the failure (horsies_workflow_record_failure; a pause
-- stops here), then horsies_workflow_promote_and_complete. Returns the owed child starts. p_dependents is 'NONE' or
-- 'READ' (22023 otherwise).
-- Precondition: the caller holds the workflow row FOR UPDATE and wrote the node.
-- Python: apply_node_progression (core/workflows/engine.py:1973-2040).
-- Locks: as horsies_workflow_record_failure and horsies_workflow_promote, under the caller's workflow lock.
CREATE FUNCTION horsies_workflow_progress(p_workflow_id uuid, p_task_index integer, p_dependents text)
    RETURNS SETOF horsies_workflow_step_row
    LANGUAGE plpgsql
    SET plan_cache_mode TO 'force_generic_plan'
AS $$
DECLARE
    v_node record;
BEGIN
    PERFORM horsies_workflow_require(p_dependents IN ('NONE', 'READ'), 'dependents must be NONE or READ');
    SELECT n.status, n.result INTO v_node
    FROM horsies_workflow_tasks n
    WHERE n.workflow_id = p_workflow_id AND n.task_index = p_task_index;
    IF v_node.status = 'FAILED'
       AND NOT horsies_workflow_record_failure(p_workflow_id, horsies_workflow_err_slot(v_node.result)) THEN
        RETURN;
    END IF;
    RETURN QUERY SELECT * FROM horsies_workflow_promote_and_complete(p_workflow_id, p_task_index, p_dependents);
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
        'OUTCOME', p_disposition, p_workflow_id, NULL, w.status::text,
        CASE WHEN p_ended_check AND w.status IN ('COMPLETED', 'FAILED') AND w.parent_workflow_id IS NOT NULL
             THEN w.id END,
        p_detail,
        NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
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
-- message of a stored error object, the first 200 characters of a stored error that is not JSON or is the JSON null,
-- else null).
-- failure_error: the error that the failure policy of the parent records for a FAILED node, as the bare error object
-- text. For a FAILED, CANCELLED or EXPIRED child it is SUBWORKFLOW_FAILED with the message "Subworkflow <id> failed"
-- and the same sub_workflow_id and summary (Python: the error of _ParentNodeFailed); for a FAILED node of a COMPLETED
-- child, the err slot of node_result; NULL for a COMPLETED node.
-- Locks: none (the child is terminal and does not change). Returns node_status, node_result, summary and
-- failure_error.
CREATE FUNCTION horsies_workflow_child_node_result(
    p_child_workflow_id uuid,
    OUT node_status text, OUT node_result text, OUT summary text, OUT failure_error text
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
        WHEN v_error IS NULL OR json_typeof(v_error) = 'null' THEN left(v_child.error, 200)
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
        failure_error := (horsies_task_error_envelope(
            'SUBWORKFLOW_FAILED', format('Subworkflow %s failed', p_child_workflow_id), NULL, p_child_workflow_id,
            summary
        )::json -> 'err')::text;
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
    failure_error := horsies_workflow_err_slot(node_result);
END;
$$;

-- Step function: advances the parent node of a terminal child workflow. One transaction per ancestor level: the
-- caller drives the next level from advance_child_workflow_id of the OUTCOME row. The start and the resume call
-- the same body inside their own statement (they drop its OUTCOME row).
-- Steps: read the child (not locked: it is terminal and does not change); lock the parent workflow row FOR UPDATE;
-- write the parent node where it is not terminal (status and result of horsies_workflow_child_node_result, the
-- summary, completed_at); for a FAILED node the failure policy of the parent with the failure_error of
-- horsies_workflow_child_node_result (horsies_workflow_record_failure; a pause stops here); then
-- horsies_workflow_promote_and_complete for the parent node.
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
    IF v_node.node_status = 'FAILED'
       AND NOT horsies_workflow_record_failure(v_child.parent_workflow_id, v_node.failure_error) THEN
        RETURN NEXT horsies_workflow_outcome_row('APPLIED', v_child.parent_workflow_id, TRUE, NULL);
        RETURN;
    END IF;
    RETURN QUERY SELECT * FROM horsies_workflow_promote_and_complete(
        v_child.parent_workflow_id, v_child.parent_task_index, 'READ'
    );
    RETURN NEXT horsies_workflow_outcome_row('APPLIED', v_child.parent_workflow_id, TRUE, NULL);
END;
$$;

------------------------------------------------------------------------------------------------------------------------
-- Node finish, phase 2
------------------------------------------------------------------------------------------------------------------------

-- Internal: the result bytes of a phase-2 record, from the history row at its locator (both partition keys given,
-- so the parent probe prunes to one leaf) or from its quarantine copy, checked against the record. Returns failure
-- NULL and the payload, or a failure (SOURCE_ABSENT, SOURCE_STATE_CONFLICT, SOURCE_VERSION_CONFLICT,
-- SOURCE_DIGEST_MISMATCH) and payload NULL. SOURCE_VERSION_CONFLICT: the source's history schema version differs
-- from the record's or is not 1, or its result envelope version is not 1 (the only format this reader knows). Reads
-- only.
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
        SELECT h.task_id, h.result_payload AS payload, h.result_digest AS digest, h.history_schema_version AS version,
               h.result_envelope_version AS envelope_version
        INTO v_source
        FROM horsies_task_history h
        WHERE h.retention_class_key = p_pending.history_class
          AND h.terminal_at = p_pending.history_anchor
          AND h.task_id = p_pending.task_id;
    ELSE
        SELECT q.task_id, q.result_payload AS payload, q.result_digest AS digest, q.history_schema_version AS version,
               q.result_envelope_version AS envelope_version
        INTO v_source
        FROM horsies_workflow_phase2_quarantine q
        WHERE q.task_id = p_pending.quarantine_task_id;
    END IF;
    failure := CASE
        WHEN NOT FOUND THEN 'SOURCE_ABSENT'
        WHEN v_source.task_id <> p_pending.task_id THEN 'SOURCE_STATE_CONFLICT'
        WHEN v_source.version IS DISTINCT FROM p_pending.history_schema_version OR v_source.version <> 1
             OR v_source.envelope_version <> 1 THEN 'SOURCE_VERSION_CONFLICT'
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
--    identity, history schema version (1), result envelope version (1) and digest. A mismatch returns SOURCE_ABSENT,
--    SOURCE_STATE_CONFLICT, SOURCE_VERSION_CONFLICT or SOURCE_DIGEST_MISMATCH and writes nothing (the record is
--    kept).
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
-- otherwise (a replay or a retry, which do not know it). A promoted sub-workflow node is owed work, so the form of
-- the call follows the phase-1 outcome: node_has_dependents FALSE, or downstream_subworkflow NONE, is one statement
-- in autocommit; downstream_subworkflow REACHABLE runs BEGIN, this call, horsies_workflow_child_start per
-- CHILD_START row, COMMIT. A replay or a retry, which have no phase-1 outcome, runs the transaction form. The fact
-- is computed at start from the static graph, so a NONE node can never owe a child start: if one does, the call
-- raises (data_corrupted) and nothing commits.
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
    v_row horsies_workflow_step_row;
    v_owes_child_start boolean := FALSE;
BEGIN
    PERFORM horsies_workflow_require(p_task_id IS NOT NULL, 'task id must not be NULL');
    PERFORM horsies_workflow_require(p_dependents IN ('NONE', 'READ'), 'dependents must be NONE or READ');
    SELECT * INTO v_apply FROM horsies_workflow_node_apply(p_task_id, p_wake);
    IF v_apply.disposition <> 'APPLIED' THEN
        RETURN NEXT horsies_workflow_outcome_row(v_apply.disposition, v_apply.workflow_id, FALSE, NULL);
        RETURN;
    END IF;
    FOR v_row IN SELECT * FROM horsies_workflow_progress(v_apply.workflow_id, v_apply.task_index, p_dependents) LOOP
        v_owes_child_start := v_owes_child_start OR v_row.row_kind = 'CHILD_START';
        RETURN NEXT v_row;
    END LOOP;
    IF v_owes_child_start AND EXISTS (
        SELECT 1 FROM horsies_workflow_tasks n
        WHERE n.workflow_id = v_apply.workflow_id AND n.task_index = v_apply.task_index
          AND n.downstream_subworkflow = 'NONE'
    ) THEN
        RAISE EXCEPTION 'workflow % node % has downstream_subworkflow NONE and its phase 2 owes a child start',
            v_apply.workflow_id, v_apply.task_index USING ERRCODE = 'data_corrupted';
    END IF;
    RETURN NEXT horsies_workflow_outcome_row('APPLIED', v_apply.workflow_id, TRUE, NULL);
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
-- column that needs a value, a repeated workflow id or (workflow, index) pair, a first row that is not the top row
-- of the tree (its parent pair and depth are p_top_parent_workflow_id, p_top_parent_task_index and p_top_depth: no
-- parent and depth 0 for a top-level start; the sub-workflow node for a child start, whose caller checks the depth
-- against the parent workflow under its lock), a later row without a parent in the tree or with a depth that is not
-- its parent's plus one, a node or failed record of a workflow outside the tree.
-- Locks: none. Returns nothing. Python: the in-memory prevalidation of start_workflow_async
-- (core/workflows/lifecycle.py:298-351).
CREATE FUNCTION horsies_workflow_start_check(
    p_workflow_ids uuid[], p_names text[], p_on_errors text[], p_parent_workflow_ids uuid[],
    p_parent_task_indexes integer[], p_depths integer[], p_top_parent_workflow_id uuid,
    p_top_parent_task_index integer, p_top_depth integer, p_other_workflow_lengths integer[],
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
        p_parent_workflow_ids[1] IS NOT DISTINCT FROM p_top_parent_workflow_id
        AND p_parent_task_indexes[1] IS NOT DISTINCT FROM p_top_parent_task_index
        AND p_depths[1] = p_top_depth,
        'the first workflow row must be the top row of the tree (its parent pair and depth)'
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

-- Internal: inserts the node rows of the workflows in p_level (workflows of a starting tree; the node arrays hold the
-- whole tree). A node without dependencies goes in READY, the rest PENDING. A sub-workflow root with a pre-built child
-- goes in READY too: horsies_workflow_start_child_row links it when the child row goes in. dependencies is a JSON
-- array of indexes, workflow_ctx_from a JSON array of node ids, args_from a JSON object of key to index.
-- sub_workflow_params is the params choice of a sub-workflow node (GIVEN or NOT_GIVEN; NULL on a task node; the
-- table CHECK refuses another pairing).
-- downstream_subworkflow is computed here, once, from the static graph: REACHABLE for every node from which a
-- sub-workflow node of the same workflow is reachable along the dependency edges (an ancestor of one), NONE for the
-- rest. The walk goes up from each sub-workflow node through the dependencies; UNION keeps each node once, so it
-- ends after at most the node count of steps.
-- Locks: the new node rows. Returns nothing. Python: INSERT_WORKFLOW_TASKS_BULK_SQL (core/workflows/sql.py:46-58;
-- core/workflows/lifecycle.py:699).
CREATE FUNCTION horsies_workflow_start_nodes(
    p_level uuid[],
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
    WITH RECURSIVE n AS MATERIALIZED (
        SELECT u.*, ARRAY(
            SELECT e.value::integer
            FROM jsonb_array_elements_text(u.dependencies_json::jsonb) WITH ORDINALITY AS e(value, position)
            ORDER BY e.position
        ) AS dependencies
        FROM unnest(
                p_node_workflow_ids, p_node_task_indexes, p_node_ids, p_node_task_names, p_node_inputs,
                p_node_queue_names, p_node_priorities, p_node_dependencies, p_node_args_from,
                p_node_workflow_ctx_from, p_node_allow_failed_deps, p_node_join_types, p_node_min_successes,
                p_node_task_options, p_node_max_retries, p_node_executions, p_node_good_untils,
                p_node_retention_class_keys, p_node_is_subworkflow, p_node_sub_workflow_names,
                p_node_sub_definition_keys, p_node_sub_workflow_params
             ) AS u(workflow_id, task_index, node_id, task_name, input, queue_name, priority, dependencies_json,
                    args_from, ctx_from, allow_failed_deps, join_type, min_success, task_options, max_retries,
                    execution, good_until, retention_class_key, is_subworkflow, sub_workflow_name,
                    sub_definition_key, sub_workflow_params)
        WHERE u.workflow_id = ANY(p_level)
    ),
    edge AS MATERIALIZED (
        SELECT n.workflow_id, n.task_index, d.dependency
        FROM n, unnest(n.dependencies) AS d(dependency)
    ),
    upstream(workflow_id, task_index) AS (
        SELECT e.workflow_id, e.dependency
        FROM edge e
        JOIN n ON n.workflow_id = e.workflow_id AND n.task_index = e.task_index
        WHERE n.is_subworkflow
        UNION
        SELECT e.workflow_id, e.dependency
        FROM upstream r
        JOIN edge e ON e.workflow_id = r.workflow_id AND e.task_index = r.task_index
    )
    INSERT INTO horsies_workflow_tasks (
        id, workflow_id, task_index, node_id, task_name, input, queue_name, priority, dependencies, args_from,
        workflow_ctx_from, allow_failed_deps, join_type, min_success, task_options, max_retries, execution,
        good_until, retention_class_key, downstream_subworkflow, status, task_id, is_subworkflow, sub_workflow_id,
        sub_workflow_name, sub_definition_key, sub_workflow_params, sub_workflow_summary, result, error,
        created_at, started_at, completed_at
    )
    SELECT horsies_uuidv7(), n.workflow_id, n.task_index, n.node_id, n.task_name, n.input, n.queue_name, n.priority,
           n.dependencies, n.args_from::jsonb,
           CASE WHEN n.ctx_from IS NULL THEN NULL ELSE ARRAY(
               SELECT x.value FROM jsonb_array_elements_text(n.ctx_from::jsonb) WITH ORDINALITY AS x(value, position)
               ORDER BY x.position
           ) END,
           n.allow_failed_deps, n.join_type, n.min_success, n.task_options, n.max_retries, n.execution,
           n.good_until, n.retention_class_key,
           CASE WHEN EXISTS (
               SELECT 1 FROM upstream r WHERE r.workflow_id = n.workflow_id AND r.task_index = n.task_index
           ) THEN 'REACHABLE' ELSE 'NONE' END,
           CASE WHEN cardinality(n.dependencies) = 0 THEN 'READY' ELSE 'PENDING' END,
           NULL, n.is_subworkflow, NULL, n.sub_workflow_name, n.sub_definition_key, n.sub_workflow_params,
           NULL, NULL, NULL, now(), NULL, NULL
    FROM n
    ORDER BY n.workflow_id, n.task_index;
END;
$$;

-- Internal: inserts the top row of a starting tree (the first of the workflow arrays), RUNNING, with
-- p_root_workflow_id (the top-level id for a top-level start, the root of the parent's tree for a child start) and
-- the start call time, with ON CONFLICT (id) DO NOTHING. Returns FALSE when the id exists already (nothing written),
-- else TRUE. The other workflow rows go in one at a time, as their parent's roots are taken
-- (horsies_workflow_start_child_row).
-- Locks: the new row and the unique index entry of its id. Python: INSERT_WORKFLOW_SQL and INSERT_CHILD_WORKFLOW_SQL
-- (core/workflows/sql.py:23-34, 260-270; core/workflows/lifecycle.py:420-442).
CREATE FUNCTION horsies_workflow_start_top_row(
    p_workflow_ids uuid[], p_names text[], p_definition_keys text[], p_on_errors text[],
    p_output_task_indexes integer[], p_success_policies text[], p_parent_workflow_ids uuid[],
    p_parent_task_indexes integer[], p_depths integer[], p_sent_at timestamp with time zone,
    p_root_workflow_id uuid
) RETURNS boolean
    LANGUAGE plpgsql
    SET plan_cache_mode TO 'force_generic_plan'
AS $$
DECLARE
    v_inserted uuid;
BEGIN
    INSERT INTO horsies_workflows (
        id, name, definition_key, status, on_error, output_task_index, success_policy, result, error,
        parent_workflow_id, parent_task_index, root_workflow_id, depth, sent_at, created_at, started_at,
        completed_at, updated_at
    ) VALUES (
        p_workflow_ids[1], p_names[1], p_definition_keys[1], 'RUNNING', p_on_errors[1], p_output_task_indexes[1],
        p_success_policies[1]::jsonb, NULL, NULL, p_parent_workflow_ids[1], p_parent_task_indexes[1],
        p_root_workflow_id, p_depths[1], p_sent_at, now(), now(), NULL, now()
    )
    ON CONFLICT (id) DO NOTHING
    RETURNING id INTO v_inserted;
    RETURN v_inserted IS NOT NULL;
END;
$$;

-- Internal: the pre-built child at position p_position of the workflow arrays (a position after the first). The child
-- row goes in (RUNNING, p_root_workflow_id, the start call time), and its sub-workflow root node goes from READY to
-- RUNNING, linked to the child, with started_at.
-- Precondition: the parent workflow is RUNNING (horsies_workflow_start_tree reads it before the call).
-- horsies_workflow_tree_check refused a child row that names no sub-workflow root of its parent, or a node that
-- another child row names, so the link writes one row; another count raises internal_error (a defect).
-- Locks: the new child row and the linked node row (rows of this statement). Returns the child id.
-- Python: ENQUEUE_SUBWORKFLOW_TASK_SQL (READY to ENQUEUED, started_at), INSERT_CHILD_WORKFLOW_SQL and
-- LINK_SUB_WORKFLOW_SQL (core/workflows/sql.py:368-404).
CREATE FUNCTION horsies_workflow_start_child_row(
    p_position integer, p_workflow_ids uuid[], p_names text[], p_definition_keys text[], p_on_errors text[],
    p_output_task_indexes integer[], p_success_policies text[], p_parent_workflow_ids uuid[],
    p_parent_task_indexes integer[], p_depths integer[], p_sent_at timestamp with time zone,
    p_root_workflow_id uuid
) RETURNS uuid
    LANGUAGE plpgsql
    SET plan_cache_mode TO 'force_generic_plan'
AS $$
DECLARE
    v_child uuid := p_workflow_ids[p_position];
    v_linked integer;
BEGIN
    INSERT INTO horsies_workflows (
        id, name, definition_key, status, on_error, output_task_index, success_policy, result, error,
        parent_workflow_id, parent_task_index, root_workflow_id, depth, sent_at, created_at, started_at,
        completed_at, updated_at
    ) VALUES (
        v_child, p_names[p_position], p_definition_keys[p_position], 'RUNNING', p_on_errors[p_position],
        p_output_task_indexes[p_position], p_success_policies[p_position]::jsonb, NULL, NULL,
        p_parent_workflow_ids[p_position], p_parent_task_indexes[p_position], p_root_workflow_id,
        p_depths[p_position], p_sent_at, now(), now(), NULL, now()
    );
    UPDATE horsies_workflow_tasks n
    SET status = 'RUNNING', sub_workflow_id = v_child, started_at = now()
    WHERE n.workflow_id = p_parent_workflow_ids[p_position] AND n.task_index = p_parent_task_indexes[p_position]
      AND n.status = 'READY' AND n.is_subworkflow AND n.sub_workflow_id IS NULL AND cardinality(n.dependencies) = 0;
    GET DIAGNOSTICS v_linked = ROW_COUNT;
    IF v_linked <> 1 THEN
        RAISE EXCEPTION 'the start linked % nodes to child workflow %', v_linked, v_child
            USING ERRCODE = 'internal_error';
    END IF;
    RETURN v_child;
END;
$$;

-- Internal: the failed-node record of one sub-workflow root of a starting tree: the node goes from READY to FAILED
-- with p_result, started_at and completed_at, then progression (failure policy, promotion of its dependents,
-- completion: horsies_workflow_progress).
-- horsies_workflow_tree_check refused a record that names no sub-workflow root of the tree, or a root that a child row
-- names, so the write changes one row; another count raises internal_error (a defect).
-- Locks: the node row, then the progression locks. Returns the owed child starts.
-- Python: ENQUEUE_SUBWORKFLOW_TASK_SQL (READY to ENQUEUED, started_at), then _fail_enqueued_task
-- (core/workflows/engine.py:514-597).
CREATE FUNCTION horsies_workflow_start_failed_node(p_workflow_id uuid, p_task_index integer, p_result text)
    RETURNS SETOF horsies_workflow_step_row
    LANGUAGE plpgsql
    SET plan_cache_mode TO 'force_generic_plan'
AS $$
DECLARE
    v_failed integer;
BEGIN
    UPDATE horsies_workflow_tasks n
    SET status = 'FAILED', result = p_result, started_at = now(), completed_at = now()
    WHERE n.workflow_id = p_workflow_id AND n.task_index = p_task_index AND n.status = 'READY'
      AND n.is_subworkflow AND n.sub_workflow_id IS NULL AND cardinality(n.dependencies) = 0;
    GET DIAGNOSTICS v_failed = ROW_COUNT;
    IF v_failed <> 1 THEN
        RAISE EXCEPTION 'the start failed % nodes for the record of workflow % node %', v_failed, p_workflow_id,
            p_task_index USING ERRCODE = 'internal_error';
    END IF;
    RETURN QUERY SELECT * FROM horsies_workflow_progress(p_workflow_id, p_task_index, 'READ');
END;
$$;

-- Internal: the parent advance of a workflow of a starting tree after its roots, as Python's in-session drain
-- (drain_parent_propagations_in_session) after the root loop of each workflow. While the workflow is COMPLETED or
-- FAILED, is not p_top_workflow_id, and its parent node is not terminal, the parent node is advanced
-- (horsies_workflow_child_finish); a parent that the advance ends is advanced in turn. The advance stops at the top
-- row of the start: a top-level row has no parent, and the TREE child start advances its own node. At most 33
-- levels (depth 0 to 32); more raises internal_error (the tree check bounds the depth).
-- Locks: as horsies_workflow_child_finish, per level. Returns the owed child starts of the advances (no OUTCOME row).
CREATE FUNCTION horsies_workflow_start_advance(p_workflow_id uuid, p_top_workflow_id uuid)
    RETURNS SETOF horsies_workflow_step_row
    LANGUAGE plpgsql
    SET plan_cache_mode TO 'force_generic_plan'
AS $$
DECLARE
    c_max_levels CONSTANT integer := 33;
    v_child uuid := p_workflow_id;
    v_next uuid;
    v_levels integer := 0;
    v_row horsies_workflow_step_row;
BEGIN
    WHILE v_child IS NOT NULL AND v_child <> p_top_workflow_id AND EXISTS (
        SELECT 1
        FROM horsies_workflows w
        JOIN horsies_workflow_tasks n ON n.workflow_id = w.parent_workflow_id AND n.task_index = w.parent_task_index
        WHERE w.id = v_child AND w.status IN ('COMPLETED', 'FAILED')
          AND n.status NOT IN ('COMPLETED', 'FAILED', 'SKIPPED')
    ) LOOP
        v_levels := v_levels + 1;
        IF v_levels > c_max_levels THEN
            RAISE EXCEPTION 'the parent advance of workflow % passed % levels', p_workflow_id, c_max_levels
                USING ERRCODE = 'internal_error';
        END IF;
        v_next := NULL;
        FOR v_row IN SELECT * FROM horsies_workflow_child_finish(v_child) LOOP
            CASE v_row.row_kind
                WHEN 'OUTCOME' THEN
                    v_next := v_row.advance_child_workflow_id;
                ELSE
                    RETURN NEXT v_row;
            END CASE;
        END LOOP;
        v_child := v_next;
    END LOOP;
END;
$$;

-- Internal: everything after the row of p_workflow_id (the top row of the start, or a pre-built child that
-- horsies_workflow_start_child_row wrote), in the order of Python's start of one workflow:
-- 1. The node rows of the workflow (horsies_workflow_start_nodes): a node without dependencies READY, the rest PENDING.
-- 2. Its sub-workflow roots one at a time, in index order, each only while the workflow is RUNNING (Python's READY to
--    ENQUEUED write requires it). Once a failure pauses the workflow, the later roots stay READY with no child, no
--    error and no started_at; the resume builds and starts them.
--    - A root with a failed-node record goes FAILED, then progression (horsies_workflow_start_failed_node).
--    - A root with a pre-built child: the child row goes in and the root is linked
--      (horsies_workflow_start_child_row), then this function starts the child: its nodes, its own roots and their
--      children, its parent advance and its task rows, before the next root of this workflow.
--    - A root with neither stays READY; step 4 enqueues it and owes its child start.
-- 3. When the workflow is COMPLETED or FAILED now, its parent advance (horsies_workflow_start_advance). Python ran the
--    drain only after a loop over at least one sub-workflow root; a workflow without one cannot end at its start.
-- 4. When the workflow is still RUNNING: its READY nodes are enqueued, with their task rows minted
--    (horsies_workflow_promote). A workflow that a failed record or an ended child paused keeps them READY with no
--    task row (Python: the root task insert, or its revert to READY). The promotion at start enqueues roots only (a
--    root has no args_from), so no node goes terminal in it.
-- So the task rows of a child's roots go in before a later root of its parent fails and pauses the parent and the
-- child, as in Python.
-- p_level counts the levels of this start (1 for its top row); more than 33 raises internal_error (depth 0 to 32;
-- the tree check refuses a deeper tree).
-- Locks: the rows of the new tree, then the progression and advance locks. Returns the owed child starts.
-- Python: start_workflow_async after the top row (core/workflows/lifecycle.py, before the switch: the reference in
-- tests/dualrun/reference/workflow_start.py) and enqueue_subworkflow_task after its link
-- (core/workflows/engine.py:1531-1602).
CREATE FUNCTION horsies_workflow_start_tree(
    p_workflow_id uuid, p_level integer, p_top_workflow_id uuid,
    p_workflow_ids uuid[], p_names text[], p_definition_keys text[], p_on_errors text[],
    p_output_task_indexes integer[], p_success_policies text[], p_parent_workflow_ids uuid[],
    p_parent_task_indexes integer[], p_depths integer[], p_sent_at timestamp with time zone,
    p_root_workflow_id uuid,
    p_node_workflow_ids uuid[], p_node_task_indexes integer[], p_node_ids text[], p_node_task_names text[],
    p_node_inputs text[], p_node_queue_names text[], p_node_priorities integer[], p_node_dependencies text[],
    p_node_args_from text[], p_node_workflow_ctx_from text[], p_node_allow_failed_deps boolean[],
    p_node_join_types text[], p_node_min_successes integer[], p_node_task_options text[],
    p_node_max_retries integer[], p_node_executions text[], p_node_good_untils timestamp with time zone[],
    p_node_retention_class_keys text[], p_node_is_subworkflow boolean[], p_node_sub_workflow_names text[],
    p_node_sub_definition_keys text[], p_node_sub_workflow_params text[],
    p_failed_workflow_ids uuid[], p_failed_task_indexes integer[], p_failed_results text[]
) RETURNS SETOF horsies_workflow_step_row
    LANGUAGE plpgsql
    SET plan_cache_mode TO 'force_generic_plan'
AS $$
DECLARE
    c_max_levels CONSTANT integer := 33;
    v_root record;
    v_child uuid;
BEGIN
    IF p_level > c_max_levels THEN
        RAISE EXCEPTION 'the start of workflow % passed % levels', p_top_workflow_id, c_max_levels
            USING ERRCODE = 'internal_error';
    END IF;
    PERFORM horsies_workflow_start_nodes(
        ARRAY[p_workflow_id], p_node_workflow_ids, p_node_task_indexes, p_node_ids, p_node_task_names, p_node_inputs,
        p_node_queue_names, p_node_priorities, p_node_dependencies, p_node_args_from, p_node_workflow_ctx_from,
        p_node_allow_failed_deps, p_node_join_types, p_node_min_successes, p_node_task_options,
        p_node_max_retries, p_node_executions, p_node_good_untils, p_node_retention_class_keys,
        p_node_is_subworkflow, p_node_sub_workflow_names, p_node_sub_definition_keys, p_node_sub_workflow_params
    );

    FOR v_root IN
        SELECT n.task_index,
               (SELECT f.result
                FROM unnest(p_failed_workflow_ids, p_failed_task_indexes, p_failed_results) AS f(workflow_id, task_index,
                                                                                                 result)
                WHERE f.workflow_id = p_workflow_id AND f.task_index = n.task_index) AS failed_result,
               (SELECT c.position
                FROM unnest(p_parent_workflow_ids, p_parent_task_indexes)
                     WITH ORDINALITY AS c(parent_id, parent_index, position)
                WHERE c.position > 1 AND c.parent_id = p_workflow_id AND c.parent_index = n.task_index) AS child_position
        FROM horsies_workflow_tasks n
        WHERE n.workflow_id = p_workflow_id AND n.is_subworkflow AND cardinality(n.dependencies) = 0
        ORDER BY n.task_index
    LOOP
        EXIT WHEN NOT EXISTS (
            SELECT 1 FROM horsies_workflows w WHERE w.id = p_workflow_id AND w.status = 'RUNNING'
        );
        CASE
            WHEN v_root.failed_result IS NOT NULL THEN
                RETURN QUERY SELECT * FROM horsies_workflow_start_failed_node(
                    p_workflow_id, v_root.task_index, v_root.failed_result
                );
            WHEN v_root.child_position IS NOT NULL THEN
                v_child := horsies_workflow_start_child_row(
                    v_root.child_position::integer, p_workflow_ids, p_names, p_definition_keys, p_on_errors,
                    p_output_task_indexes, p_success_policies, p_parent_workflow_ids, p_parent_task_indexes, p_depths,
                    p_sent_at, p_root_workflow_id
                );
                RETURN QUERY SELECT * FROM horsies_workflow_start_tree(
                    v_child, p_level + 1, p_top_workflow_id,
                    p_workflow_ids, p_names, p_definition_keys, p_on_errors, p_output_task_indexes,
                    p_success_policies, p_parent_workflow_ids, p_parent_task_indexes, p_depths, p_sent_at,
                    p_root_workflow_id,
                    p_node_workflow_ids, p_node_task_indexes, p_node_ids, p_node_task_names, p_node_inputs,
                    p_node_queue_names, p_node_priorities, p_node_dependencies, p_node_args_from,
                    p_node_workflow_ctx_from, p_node_allow_failed_deps, p_node_join_types, p_node_min_successes,
                    p_node_task_options, p_node_max_retries, p_node_executions, p_node_good_untils,
                    p_node_retention_class_keys, p_node_is_subworkflow, p_node_sub_workflow_names,
                    p_node_sub_definition_keys, p_node_sub_workflow_params,
                    p_failed_workflow_ids, p_failed_task_indexes, p_failed_results
                );
            ELSE
                NULL;
        END CASE;
    END LOOP;

    RETURN QUERY SELECT * FROM horsies_workflow_start_advance(p_workflow_id, p_top_workflow_id);

    IF EXISTS (SELECT 1 FROM horsies_workflows w WHERE w.id = p_workflow_id AND w.status = 'RUNNING') THEN
        RETURN QUERY SELECT * FROM horsies_workflow_promote(
            p_workflow_id, NULL, NULL,
            ARRAY(SELECT n.task_index FROM horsies_workflow_tasks n
                  WHERE n.workflow_id = p_workflow_id AND n.status = 'READY' ORDER BY n.task_index)
        );
    END IF;
END;
$$;

-- Internal: the argument check of a tree (horsies_workflow_start_check over the lengths and NULL elements of every
-- array), for a top-level start (p_top_parent_workflow_id and p_top_parent_task_index NULL, p_top_depth 0) and for a
-- child start (the sub-workflow node; horsies_workflow_child_start checks the depth of the child row against the
-- parent workflow after it locks that row). Also refuses a child row that names no sub-workflow root node (no
-- dependencies) of its parent, two child rows that name one node, and a failed-node record that names no sub-workflow
-- root of the tree or a root that a child row names. These checks read the arrays, not the written rows, so they
-- hold for a child or record that the start does not write (under a workflow that a failed record paused).
-- Refuses with 22023.
-- Locks: none. Returns nothing.
CREATE FUNCTION horsies_workflow_tree_check(
    p_top_parent_workflow_id uuid,
    p_top_parent_task_index integer,
    p_top_depth integer,
    p_workflow_ids uuid[],
    p_names text[],
    p_definition_keys text[],
    p_on_errors text[],
    p_output_task_indexes integer[],
    p_success_policies text[],
    p_parent_workflow_ids uuid[],
    p_parent_task_indexes integer[],
    p_depths integer[],
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
) RETURNS void
    LANGUAGE plpgsql IMMUTABLE
AS $$
BEGIN
    PERFORM horsies_workflow_start_check(
        p_workflow_ids, p_names, p_on_errors, p_parent_workflow_ids, p_parent_task_indexes, p_depths,
        p_top_parent_workflow_id, p_top_parent_task_index, p_top_depth,
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
    PERFORM horsies_workflow_require(
        NOT EXISTS (
            SELECT 1
            FROM unnest(p_parent_workflow_ids, p_parent_task_indexes)
                 WITH ORDINALITY AS c(parent_id, parent_index, position)
            WHERE c.position > 1
              AND NOT EXISTS (
                  SELECT 1
                  FROM unnest(p_node_workflow_ids, p_node_task_indexes, p_node_is_subworkflow, p_node_dependencies)
                       AS n(workflow_id, task_index, is_subworkflow, dependencies)
                  WHERE n.workflow_id = c.parent_id AND n.task_index = c.parent_index
                    AND n.is_subworkflow AND n.dependencies::jsonb = '[]'::jsonb
              )
        )
        AND cardinality(p_workflow_ids) - 1 = (
            SELECT count(DISTINCT (c.parent_id, c.parent_index))
            FROM unnest(p_parent_workflow_ids, p_parent_task_indexes)
                 WITH ORDINALITY AS c(parent_id, parent_index, position)
            WHERE c.position > 1
        ),
        'a child workflow row names no sub-workflow root node of its parent, or a node that another child row names'
    );
    PERFORM horsies_workflow_require(
        NOT EXISTS (
            SELECT 1
            FROM unnest(p_failed_workflow_ids, p_failed_task_indexes) AS f(workflow_id, task_index)
            WHERE NOT EXISTS (
                      SELECT 1
                      FROM unnest(p_node_workflow_ids, p_node_task_indexes, p_node_is_subworkflow, p_node_dependencies)
                           AS n(workflow_id, task_index, is_subworkflow, dependencies)
                      WHERE n.workflow_id = f.workflow_id AND n.task_index = f.task_index
                        AND n.is_subworkflow AND n.dependencies::jsonb = '[]'::jsonb
                  )
               OR EXISTS (
                      SELECT 1
                      FROM unnest(p_parent_workflow_ids, p_parent_task_indexes)
                           WITH ORDINALITY AS c(parent_id, parent_index, position)
                      WHERE c.position > 1 AND c.parent_id = f.workflow_id AND c.parent_index = f.task_index
                  )
        ),
        'a failed-node record names no sub-workflow root of the tree without a child'
    );
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
-- 2. The rest of the tree, depth first from the top row, as Python's start (horsies_workflow_start_tree): the node
--    rows of a workflow (a node without dependencies READY, the rest PENDING; node row ids from horsies_uuidv7();
--    downstream_subworkflow from the static graph); then its sub-workflow roots one at a time in index order, each
--    only while the workflow is RUNNING: a failed-node record makes the node FAILED with its envelope, started_at and
--    completed_at, then progression (the failure policy, promotion of its dependents, completion); a pre-built child
--    goes in (RUNNING, root id of the tree), its node goes RUNNING, linked, with started_at, and the child is started
--    the same way before the next root. A later root of a workflow that a failure paused stays READY with no child:
--    the resume builds and starts it.
-- 3. After the roots of a workflow: its parent advance when it is COMPLETED or FAILED now
--    (horsies_workflow_child_finish; a parent that ends is advanced in turn, as Python's in-session drain does); then,
--    when it is still RUNNING, its READY nodes are enqueued, with their task rows minted (horsies_workflow_promote).
--    A workflow that a failed record or an ended child paused keeps its READY nodes with no task (the pause gate).
-- 4. Return STARTED.
-- Owed work: CHILD_START rows come only from the progression of a failed record or from a sub-workflow root with
-- neither a child nor a failed record; a caller that passes failed records runs this call inside BEGIN, then
-- horsies_workflow_child_start per row, then COMMIT. Any other start is one statement in autocommit.
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
    PERFORM horsies_workflow_tree_check(
        NULL, NULL, 0, p_workflow_ids, p_names, p_definition_keys, p_on_errors, p_output_task_indexes,
        p_success_policies, p_parent_workflow_ids, p_parent_task_indexes, p_depths, p_node_workflow_ids,
        p_node_task_indexes, p_node_ids, p_node_task_names, p_node_inputs, p_node_queue_names, p_node_priorities,
        p_node_dependencies, p_node_args_from, p_node_workflow_ctx_from, p_node_allow_failed_deps, p_node_join_types,
        p_node_min_successes, p_node_task_options, p_node_max_retries, p_node_executions, p_node_good_untils,
        p_node_retention_class_keys, p_node_is_subworkflow, p_node_sub_workflow_names, p_node_sub_definition_keys,
        p_node_sub_workflow_params, p_failed_workflow_ids, p_failed_task_indexes, p_failed_results
    );

    IF NOT horsies_workflow_start_top_row(
        p_workflow_ids, p_names, p_definition_keys, p_on_errors, p_output_task_indexes, p_success_policies,
        p_parent_workflow_ids, p_parent_task_indexes, p_depths, p_sent_at, v_top
    ) THEN
        SELECT w.name INTO v_name FROM horsies_workflows w WHERE w.id = v_top;
        RETURN NEXT horsies_workflow_outcome_row('EXISTS', v_top, FALSE, v_name);
        RETURN;
    END IF;
    RETURN QUERY SELECT * FROM horsies_workflow_start_tree(
        v_top, 1, v_top,
        p_workflow_ids, p_names, p_definition_keys, p_on_errors, p_output_task_indexes, p_success_policies,
        p_parent_workflow_ids, p_parent_task_indexes, p_depths, p_sent_at, v_top,
        p_node_workflow_ids, p_node_task_indexes, p_node_ids, p_node_task_names, p_node_inputs, p_node_queue_names,
        p_node_priorities, p_node_dependencies, p_node_args_from, p_node_workflow_ctx_from,
        p_node_allow_failed_deps, p_node_join_types, p_node_min_successes, p_node_task_options, p_node_max_retries,
        p_node_executions, p_node_good_untils, p_node_retention_class_keys, p_node_is_subworkflow,
        p_node_sub_workflow_names, p_node_sub_definition_keys, p_node_sub_workflow_params,
        p_failed_workflow_ids, p_failed_task_indexes, p_failed_results
    );
    RETURN NEXT horsies_workflow_outcome_row('STARTED', v_top, FALSE, NULL);
END;
$$;

------------------------------------------------------------------------------------------------------------------------
-- Sub-workflow start
------------------------------------------------------------------------------------------------------------------------

-- Step function: the owed child start of one sub-workflow node, in the transaction of the step that owes it (BEGIN;
-- the step function; the host build callback and this call for each CHILD_START row; COMMIT). The parent node, the
-- child rows and the link commit together.
-- p_outcome_kind (a kind text plus the arguments of each kind; each kind refuses the arguments of the other, 22023):
-- - TREE: the child tree that the core built, in the arrays of horsies_workflow_start: the child row first, with the
--   parent pair (p_parent_workflow_id, p_parent_task_index) and the depth of the parent workflow plus one, then the
--   pre-built children of its sub-workflow roots; every node of the tree; the failed-node records (a sub-workflow
--   root of the tree whose own child could not be built). p_sent_at is the call time of the child start.
-- - FAILURE: p_failure_result, the error envelope of a child that could not be built (a definition that does not
--   load, a failing build callback, an args_from key or value that the callback refuses, the depth limit). It must
--   be an error envelope (22023 otherwise).
-- Steps:
-- 1. Lock the parent workflow row FOR UPDATE (the step function of this transaction holds it already), then the
--    node row.
-- 2. The node is not an ENQUEUED sub-workflow node with no child: STALE, nothing written.
-- 3. The parent workflow is not RUNNING (a failure earlier in the same step paused it): the node goes back to READY
--    with started_at NULL, so the resume or recovery case 1.5 starts it later. WORKFLOW_NOT_RUNNING (detail: the
--    status). Without this the node would stay ENQUEUED with no child, which no recovery case selects.
-- 4. TREE: insert the child row (RUNNING, the root id of the parent's tree; horsies_workflow_start_top_row), link
--    the node (RUNNING, sub_workflow_id), then write the rest of the child tree as a start does
--    (horsies_workflow_start_tree: depth first, the roots of each workflow in index order while it is RUNNING, the
--    parent advance of a workflow that ends, then the task rows of the READY nodes of a workflow that is still
--    RUNNING). A child that is COMPLETED or FAILED now advances the node in this statement
--    (horsies_workflow_child_finish), as Python's in-session drain does. STARTED.
-- 5. FAILURE: the node goes FAILED with the envelope and completed_at, then horsies_workflow_progress for it
--    (failure policy, promotion of its dependents, completion). NODE_FAILED.
-- A refused argument raises 22023; a child id that exists already raises unique_violation (23505): the core mints
-- it, so a conflict is a defect. Nothing is written in either case.
-- Returns the OUTCOME row (workflow_id: the parent workflow; workflow_status: its status after the call;
-- advance_child_workflow_id: the parent workflow when this call ended it and it has a parent, for the advance after
-- the commit) and the CHILD_START and SOURCE rows that this call owes in turn.
-- Lock order: parent workflow row, node row, the new child rows, then the progression locks (node rows; under
-- on_error pause the RUNNING descendant rows in (depth, id) order).
-- Python: enqueue_subworkflow_task after its READY to ENQUEUED write (core/workflows/engine.py:1068-1627, with
-- INSERT_CHILD_WORKFLOW_SQL and LINK_SUB_WORKFLOW_SQL, core/workflows/sql.py:260-276), and _fail_enqueued_task
-- (core/workflows/engine.py:472-555).
CREATE FUNCTION horsies_workflow_child_start(
    p_parent_workflow_id uuid,
    p_parent_task_index integer,
    p_outcome_kind text,
    p_failure_result text,
    p_sent_at timestamp with time zone,
    p_workflow_ids uuid[],
    p_names text[],
    p_definition_keys text[],
    p_on_errors text[],
    p_output_task_indexes integer[],
    p_success_policies text[],
    p_parent_workflow_ids uuid[],
    p_parent_task_indexes integer[],
    p_depths integer[],
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
    v_parent record;
    v_node record;
    v_child uuid := p_workflow_ids[1];
BEGIN
    PERFORM horsies_workflow_require(
        p_parent_workflow_id IS NOT NULL AND p_parent_task_index IS NOT NULL,
        'the parent workflow id and task index must not be NULL'
    );
    PERFORM horsies_workflow_require(
        p_outcome_kind IN ('TREE', 'FAILURE'), 'the outcome kind must be TREE or FAILURE'
    );
    CASE p_outcome_kind
        WHEN 'TREE' THEN
            PERFORM horsies_workflow_require(
                p_failure_result IS NULL AND p_sent_at IS NOT NULL,
                'a TREE child start takes sent_at and no failure result'
            );
            PERFORM horsies_workflow_tree_check(
                p_parent_workflow_id, p_parent_task_index, p_depths[1],
                p_workflow_ids, p_names, p_definition_keys, p_on_errors, p_output_task_indexes, p_success_policies,
                p_parent_workflow_ids, p_parent_task_indexes, p_depths, p_node_workflow_ids, p_node_task_indexes,
                p_node_ids, p_node_task_names, p_node_inputs, p_node_queue_names, p_node_priorities,
                p_node_dependencies, p_node_args_from, p_node_workflow_ctx_from, p_node_allow_failed_deps,
                p_node_join_types, p_node_min_successes, p_node_task_options, p_node_max_retries, p_node_executions,
                p_node_good_untils, p_node_retention_class_keys, p_node_is_subworkflow, p_node_sub_workflow_names,
                p_node_sub_definition_keys, p_node_sub_workflow_params, p_failed_workflow_ids, p_failed_task_indexes,
                p_failed_results
            );
        WHEN 'FAILURE' THEN
            PERFORM horsies_workflow_require(
                p_sent_at IS NULL AND num_nonnulls(
                    p_workflow_ids, p_names, p_definition_keys, p_on_errors, p_output_task_indexes, p_success_policies,
                    p_parent_workflow_ids, p_parent_task_indexes, p_depths, p_node_workflow_ids, p_node_task_indexes,
                    p_node_ids, p_node_task_names, p_node_inputs, p_node_queue_names, p_node_priorities,
                    p_node_dependencies, p_node_args_from, p_node_workflow_ctx_from, p_node_allow_failed_deps,
                    p_node_join_types, p_node_min_successes, p_node_task_options, p_node_max_retries, p_node_executions,
                    p_node_good_untils, p_node_retention_class_keys, p_node_is_subworkflow, p_node_sub_workflow_names,
                    p_node_sub_definition_keys, p_node_sub_workflow_params, p_failed_workflow_ids,
                    p_failed_task_indexes, p_failed_results
                ) = 0,
                'a FAILURE child start takes the failure result alone'
            );
            PERFORM horsies_workflow_require(
                horsies_workflow_result_kind(p_failure_result) = 'ERROR',
                'the failure result of a child start must be an error envelope'
            );
    END CASE;

    SELECT w.status, w.depth, coalesce(w.root_workflow_id, w.id) AS root_workflow_id INTO v_parent
    FROM horsies_workflows w
    WHERE w.id = p_parent_workflow_id
    FOR UPDATE;
    SELECT n.id, n.status, n.is_subworkflow, n.sub_workflow_id INTO v_node
    FROM horsies_workflow_tasks n
    WHERE n.workflow_id = p_parent_workflow_id AND n.task_index = p_parent_task_index
    FOR UPDATE;
    IF v_node.id IS NULL OR NOT v_node.is_subworkflow OR v_node.status <> 'ENQUEUED'
       OR v_node.sub_workflow_id IS NOT NULL THEN
        RETURN NEXT horsies_workflow_outcome_row('STALE', p_parent_workflow_id, FALSE, v_node.status::text);
        RETURN;
    END IF;
    IF v_parent.status <> 'RUNNING' THEN
        UPDATE horsies_workflow_tasks n SET status = 'READY', started_at = NULL WHERE n.id = v_node.id;
        RETURN NEXT horsies_workflow_outcome_row(
            'WORKFLOW_NOT_RUNNING', p_parent_workflow_id, FALSE, v_parent.status::text
        );
        RETURN;
    END IF;

    CASE p_outcome_kind
        WHEN 'TREE' THEN
            PERFORM horsies_workflow_require(
                p_depths[1] = v_parent.depth + 1,
                'the child row must have the depth of its parent workflow plus one'
            );
            IF NOT horsies_workflow_start_top_row(
                p_workflow_ids, p_names, p_definition_keys, p_on_errors, p_output_task_indexes, p_success_policies,
                p_parent_workflow_ids, p_parent_task_indexes, p_depths, p_sent_at, v_parent.root_workflow_id
            ) THEN
                RAISE EXCEPTION 'child workflow % of workflow % node % exists already', v_child, p_parent_workflow_id,
                    p_parent_task_index USING ERRCODE = 'unique_violation';
            END IF;
            UPDATE horsies_workflow_tasks n SET status = 'RUNNING', sub_workflow_id = v_child WHERE n.id = v_node.id;
            RETURN QUERY SELECT * FROM horsies_workflow_start_tree(
                v_child, 1, v_child,
                p_workflow_ids, p_names, p_definition_keys, p_on_errors, p_output_task_indexes, p_success_policies,
                p_parent_workflow_ids, p_parent_task_indexes, p_depths, p_sent_at, v_parent.root_workflow_id,
                p_node_workflow_ids, p_node_task_indexes, p_node_ids, p_node_task_names, p_node_inputs,
                p_node_queue_names, p_node_priorities, p_node_dependencies, p_node_args_from,
                p_node_workflow_ctx_from, p_node_allow_failed_deps, p_node_join_types, p_node_min_successes,
                p_node_task_options, p_node_max_retries, p_node_executions, p_node_good_untils,
                p_node_retention_class_keys, p_node_is_subworkflow, p_node_sub_workflow_names,
                p_node_sub_definition_keys, p_node_sub_workflow_params, p_failed_workflow_ids, p_failed_task_indexes,
                p_failed_results
            );
            IF EXISTS (SELECT 1 FROM horsies_workflows w WHERE w.id = v_child AND w.status IN ('COMPLETED', 'FAILED'))
            THEN
                RETURN QUERY SELECT * FROM horsies_workflow_child_finish(v_child) AS r WHERE r.row_kind <> 'OUTCOME';
            END IF;
            RETURN NEXT horsies_workflow_outcome_row('STARTED', p_parent_workflow_id, TRUE, NULL);
        WHEN 'FAILURE' THEN
            UPDATE horsies_workflow_tasks n
            SET status = 'FAILED', result = p_failure_result, completed_at = now()
            WHERE n.id = v_node.id;
            RETURN QUERY SELECT * FROM horsies_workflow_progress(p_parent_workflow_id, p_parent_task_index, 'READ');
            RETURN NEXT horsies_workflow_outcome_row('NODE_FAILED', p_parent_workflow_id, TRUE, NULL);
    END CASE;
END;
$$;

------------------------------------------------------------------------------------------------------------------------
-- Claim-time check and the stop before start
------------------------------------------------------------------------------------------------------------------------

-- The claim-time workflow check of one claim pass, in one statement in autocommit after the claim committed.
-- Arguments: the claimed task ids, the claim generation of each (claimed_at), the worker. Refused (22023): a NULL
-- array, element or worker id, arrays of different lengths, a repeated id, more than 1000 ids (the bound of one
-- claim pass, as horsies_release_claims).
-- Steps:
-- 1. The lookup: the workflow of each claimed task (the node by task_id, the workflow by id), unlocked. Two index
--    probes per task, bounded by the claim pass. A task with no node, or whose workflow is neither PAUSED nor
--    CANCELLED, is dispatched; when there is none of the other kind, the statement ends here.
-- 2. The PAUSED or CANCELLED workflows of the batch, locked FOR SHARE in (depth, id) order, and their status read
--    again under the lock: a resume or a pause cannot commit meanwhile. A workflow that a resume made RUNNING since
--    the lookup has its tasks dispatched.
-- 3. The task rows of the blocked tasks, locked FOR UPDATE in id order in one pass (the batch rule).
-- 4. Tasks of a PAUSED workflow: abandoned (horsies_abandon_owned_nodes, PAUSE_ABANDON_CLAIM_BATCH, fenced to this
--    worker and each claim generation); the node of each abandoned task goes back to READY with no task.
-- 5. Tasks of a CANCELLED workflow: cancelled (horsies_cancel_owned_nodes, WORKFLOW_CANCEL_CLAIM_BATCH, same fence);
--    the node of each cancelled task goes SKIPPED with completed_at.
-- A blocked task is never dispatched, whether its move applied or found the row moved on (a lost claim, a task
-- already terminal): that row belongs to its current owner.
-- The guard of v61 (no workflow PAUSED or CANCELLED at all) is not used: it needs an index on the workflow status,
-- which the baseline does not have, and any CANCELLED workflow kept by retention makes it pass.
-- Returns one row per input id, in input order: DISPATCH or BLOCKED.
-- Lock order: workflow rows FOR SHARE in (depth, id) order, task rows in id order (no task advisory lock), node rows.
-- Python: _filter_nonrunnable_workflow_tasks (core/worker/claiming.py:503-620) with
-- horsies_find_non_runnable_workflow_tasks, horsies_abandon_owned_nodes, RESET_PAUSED_WORKFLOW_TASKS_SQL,
-- horsies_cancel_owned_nodes and SKIP_CANCELLED_WORKFLOW_TASKS_SQL (core/worker/sql.py:228-233, 286-299).
CREATE FUNCTION horsies_workflow_claimed_tasks_settle(
    p_task_ids uuid[], p_claimed_ats timestamp with time zone[], p_worker_id text
) RETURNS TABLE (task_id uuid, disposition text)
    LANGUAGE plpgsql
    SET plan_cache_mode TO 'force_generic_plan'
AS $$
#variable_conflict use_column
DECLARE
    c_max_tasks CONSTANT integer := 1000;
    v_lookup_workflows uuid[];
    v_paused_workflows uuid[];
    v_cancelled_workflows uuid[];
    v_paused_ids uuid[];
    v_paused_ats timestamp with time zone[];
    v_cancelled_ids uuid[];
    v_cancelled_ats timestamp with time zone[];
    v_abandoned uuid[];
    v_cancelled uuid[];
BEGIN
    PERFORM horsies_workflow_require(
        p_task_ids IS NOT NULL AND p_claimed_ats IS NOT NULL AND p_worker_id IS NOT NULL,
        'the task ids, claim generations and worker id must not be NULL'
    );
    PERFORM horsies_workflow_require(
        cardinality(p_task_ids) = cardinality(p_claimed_ats)
        AND array_position(p_task_ids, NULL) IS NULL AND array_position(p_claimed_ats, NULL) IS NULL,
        'the task id and claim generation arrays differ in length or hold NULL'
    );
    PERFORM horsies_workflow_require(cardinality(p_task_ids) <= c_max_tasks, 'at most 1000 task ids per call');
    PERFORM horsies_workflow_require(
        cardinality(p_task_ids) = (SELECT count(DISTINCT u.id) FROM unnest(p_task_ids) AS u(id)),
        'a task id repeats'
    );

    -- One probe of the node by task_id and one of its workflow by primary key per task: the LATERAL with OFFSET 0
    -- keeps each workflow read tied to its node, so no plan reads the workflows by status.
    SELECT array_agg(DISTINCT w.id) INTO v_lookup_workflows
    FROM horsies_workflow_tasks n
    CROSS JOIN LATERAL (
        SELECT workflow.id, workflow.status
        FROM horsies_workflows workflow
        WHERE workflow.id = n.workflow_id
        OFFSET 0
    ) AS w
    WHERE n.task_id = ANY(p_task_ids)
      AND w.status IN ('PAUSED', 'CANCELLED');
    IF v_lookup_workflows IS NULL THEN
        RETURN QUERY
        SELECT u.id, 'DISPATCH'::text FROM unnest(p_task_ids) WITH ORDINALITY AS u(id, position) ORDER BY u.position;
        RETURN;
    END IF;

    WITH locked AS (
        SELECT w.id, w.status
        FROM horsies_workflows w
        WHERE w.id = ANY(v_lookup_workflows)
        ORDER BY w.depth, w.id
        FOR SHARE
    )
    SELECT array_agg(l.id) FILTER (WHERE l.status = 'PAUSED'),
           array_agg(l.id) FILTER (WHERE l.status = 'CANCELLED')
    INTO v_paused_workflows, v_cancelled_workflows
    FROM locked l;

    SELECT array_agg(u.id ORDER BY u.id) FILTER (WHERE n.workflow_id = ANY(v_paused_workflows)),
           array_agg(u.claimed_at ORDER BY u.id) FILTER (WHERE n.workflow_id = ANY(v_paused_workflows)),
           array_agg(u.id ORDER BY u.id) FILTER (WHERE n.workflow_id = ANY(v_cancelled_workflows)),
           array_agg(u.claimed_at ORDER BY u.id) FILTER (WHERE n.workflow_id = ANY(v_cancelled_workflows))
    INTO v_paused_ids, v_paused_ats, v_cancelled_ids, v_cancelled_ats
    FROM unnest(p_task_ids, p_claimed_ats) AS u(id, claimed_at)
    JOIN horsies_workflow_tasks n ON n.task_id = u.id;

    PERFORM 1 FROM horsies_tasks t
    WHERE t.id = ANY(coalesce(v_paused_ids, ARRAY[]::uuid[]) || coalesce(v_cancelled_ids, ARRAY[]::uuid[]))
    ORDER BY t.id
    FOR UPDATE;

    IF v_paused_ids IS NOT NULL THEN
        SELECT coalesce(array_agg(o.task_id), ARRAY[]::uuid[]) INTO v_abandoned
        FROM horsies_abandon_owned_nodes(v_paused_ids, v_paused_ats, p_worker_id) AS o
        WHERE o.outcome = 'APPLIED';
        UPDATE horsies_workflow_tasks n
        SET status = 'READY', task_id = NULL, started_at = NULL
        WHERE n.task_id = ANY(v_abandoned) AND n.status IN ('ENQUEUED', 'RUNNING');
    END IF;
    IF v_cancelled_ids IS NOT NULL THEN
        SELECT coalesce(array_agg(o.task_id), ARRAY[]::uuid[]) INTO v_cancelled
        FROM horsies_cancel_owned_nodes(v_cancelled_ids, v_cancelled_ats, p_worker_id) AS o
        WHERE o.outcome = 'APPLIED';
        UPDATE horsies_workflow_tasks n
        SET status = 'SKIPPED', completed_at = now()
        WHERE n.task_id = ANY(v_cancelled) AND n.status IN ('PENDING', 'READY', 'ENQUEUED');
    END IF;

    RETURN QUERY
    SELECT u.id,
           CASE
               WHEN u.id = ANY(coalesce(v_paused_ids, ARRAY[]::uuid[]) || coalesce(v_cancelled_ids, ARRAY[]::uuid[]))
                   THEN 'BLOCKED'
               ELSE 'DISPATCH'
           END::text
    FROM unnest(p_task_ids) WITH ORDINALITY AS u(id, position)
    ORDER BY u.position;
END;
$$;

-- The stop of a workflow task whose start (horsies_start_attempt) missed, in one statement in autocommit sent after
-- the start statement ended, so no row lock of the start is held. The reason of the start's miss only chooses that
-- this call runs: the function judges again under its own locks, in Python's order.
-- Arguments: the task, the worker, the claim generation of the start, p_expiry_result (the TASK_EXPIRED envelope
-- built by the core) and p_orphan_self_heal. Every argument is required (22023).
-- Steps:
-- 1. The workflow row of the task's node (the node by task_id) locked FOR SHARE: a resume, a pause or a cancel of
--    that workflow cannot commit until this statement ends.
-- 2. The deadline (horsies_expire_owned_claim: the task advisory lock, the row lock, the owner fence, good_until
--    passed): when the expiry applies, or was applied already, EXPIRED. Phase 2 of the node is owed, in its long
--    form.
-- 3. The node of the task, locked, and the workflow status read under the lock of step 1:
--    - PAUSED: ABANDONED. The abandon runs (horsies_abandon_owned_node, PAUSE_ABANDON_CLAIM, fenced to the claim
--      generation); when it applies or was applied, the node goes back to READY with no task and no started_at.
--    - CANCELLED: CANCELLED. The cancel runs (horsies_cancel_owned_node, WORKFLOW_CANCEL_CLAIM, with the carve-out
--      for a row requeued to PENDING); when it applies or was applied, the node goes SKIPPED with completed_at.
--    - the claim is still startable (CLAIMED by this worker at this generation, lease and deadline not passed) and
--      the node is linked live (ENQUEUED, READY, PENDING or RUNNING) in a RUNNING workflow: RESUMED, nothing
--      written. A resume committed after the start read PAUSED; the worker runs the start once more.
-- 4. The claim is held by this worker at this generation and no node links the task live:
--    - p_orphan_self_heal: ORPHAN_CANCELLED. The orphan cancel runs (horsies_cancel_owned_orphan, CANCEL_ORPHAN).
--    - else NODE_NOT_LINKED, nothing written (the worker reports WORKFLOW_CHECK_FAILED).
-- 5. Anything else: NOT_HELD, with the classification of horsies_terminalization_miss.
-- Returns one row: outcome (the step that ran), and move_outcome, the outcome of the move it ran (APPLIED,
-- ALREADY_APPLIED, LOST_CLAIM, SOURCE_STATE_CONFLICT, TASK_ABSENT), NULL for RESUMED and NODE_NOT_LINKED.
-- Lock order: workflow row FOR SHARE, task advisory lock, task row, node row.
-- Python: the miss path of _confirm_ownership_and_set_running (core/worker/child_runner.py:1195-1235) with
-- _expire_claimed_task_before_start (:1066-1100) and _handle_workflow_stop_before_start (:960-1064), and the
-- orphan cancel of the finalize classification (core/lifecycle/classify.py:281-297).
CREATE FUNCTION horsies_workflow_task_stop_before_start(
    p_task_id uuid,
    p_worker_id text,
    p_claimed_at timestamp with time zone,
    p_expiry_result text,
    p_orphan_self_heal boolean
) RETURNS TABLE (outcome text, move_outcome text)
    LANGUAGE plpgsql
    SET plan_cache_mode TO 'force_generic_plan'
AS $$
#variable_conflict use_column
DECLARE
    v_workflow_id uuid;
    v_workflow_status text;
    v_move text;
    v_task record;
    v_node record;
    v_held boolean;
BEGIN
    PERFORM horsies_workflow_require(
        num_nulls(p_task_id, p_worker_id, p_claimed_at, p_expiry_result, p_orphan_self_heal) = 0,
        'every argument of the stop before start is required'
    );
    SELECT n.workflow_id INTO v_workflow_id FROM horsies_workflow_tasks n WHERE n.task_id = p_task_id;
    IF v_workflow_id IS NOT NULL THEN
        SELECT w.status INTO v_workflow_status FROM horsies_workflows w WHERE w.id = v_workflow_id FOR SHARE;
    END IF;

    SELECT o.outcome INTO v_move
    FROM horsies_expire_owned_claim(p_task_id, p_worker_id, p_expiry_result, 'TASK_EXPIRED') AS o;
    IF v_move IN ('APPLIED', 'ALREADY_APPLIED') THEN
        RETURN QUERY SELECT 'EXPIRED'::text, v_move;
        RETURN;
    END IF;

    -- The task row is locked by the expiry call (its fence or its miss classifier); the node row last.
    SELECT t.status, t.claimed_by_worker_id, t.claimed_at, t.claim_expires_at, t.good_until INTO v_task
    FROM horsies_tasks t WHERE t.id = p_task_id;
    SELECT n.id, n.workflow_id, n.status INTO v_node
    FROM horsies_workflow_tasks n WHERE n.task_id = p_task_id
    FOR UPDATE;
    v_held := v_task.status = 'CLAIMED' AND v_task.claimed_by_worker_id = p_worker_id
              AND v_task.claimed_at = p_claimed_at;

    IF v_node.workflow_id IS NOT DISTINCT FROM v_workflow_id AND v_workflow_status = 'PAUSED' THEN
        SELECT o.outcome INTO v_move FROM horsies_abandon_owned_node(p_task_id, p_worker_id, p_claimed_at) AS o;
        IF v_move IN ('APPLIED', 'ALREADY_APPLIED') THEN
            UPDATE horsies_workflow_tasks n
            SET status = 'READY', task_id = NULL, started_at = NULL
            WHERE n.task_id = p_task_id AND n.status IN ('ENQUEUED', 'RUNNING');
        END IF;
        RETURN QUERY SELECT 'ABANDONED'::text, v_move;
        RETURN;
    END IF;
    IF v_node.workflow_id IS NOT DISTINCT FROM v_workflow_id AND v_workflow_status = 'CANCELLED' THEN
        SELECT o.outcome INTO v_move
        FROM horsies_cancel_owned_node(p_task_id, p_worker_id, p_claimed_at, TRUE) AS o;
        IF v_move IN ('APPLIED', 'ALREADY_APPLIED') THEN
            UPDATE horsies_workflow_tasks n
            SET status = 'SKIPPED', completed_at = now()
            WHERE n.task_id = p_task_id AND n.status IN ('ENQUEUED', 'READY');
        END IF;
        RETURN QUERY SELECT 'CANCELLED'::text, v_move;
        RETURN;
    END IF;
    IF v_held
       AND v_task.claim_expires_at > now()
       AND (v_task.good_until IS NULL OR v_task.good_until > now())
       AND v_workflow_status = 'RUNNING'
       AND v_node.status IN ('ENQUEUED', 'READY', 'PENDING', 'RUNNING') THEN
        RETURN QUERY SELECT 'RESUMED'::text, NULL::text;
        RETURN;
    END IF;
    IF v_held AND v_node.status IS DISTINCT FROM 'ENQUEUED' AND v_node.status IS DISTINCT FROM 'READY'
       AND v_node.status IS DISTINCT FROM 'PENDING' AND v_node.status IS DISTINCT FROM 'RUNNING' THEN
        IF NOT p_orphan_self_heal THEN
            RETURN QUERY SELECT 'NODE_NOT_LINKED'::text, NULL::text;
            RETURN;
        END IF;
        SELECT o.outcome INTO v_move FROM horsies_cancel_owned_orphan(p_task_id, p_worker_id, p_claimed_at) AS o;
        RETURN QUERY SELECT 'ORPHAN_CANCELLED'::text, v_move;
        RETURN;
    END IF;
    -- The classifier takes one kind. EXPIRE_CLAIMED answers as an empty class would: a history row of the expire
    -- family ended this call at the expiry above (ALREADY_APPLIED there), and the task advisory lock that the expiry
    -- took keeps history unchanged until the transaction ends. Every other history row is FOREIGN_TERMINALIZATION.
    SELECT o.outcome INTO v_move
    FROM horsies_terminalization_miss(
        p_task_id, 'EXPIRE_CLAIMED', 'OWNER_AND_GENERATION', p_worker_id, p_claimed_at
    ) AS o;
    RETURN QUERY SELECT 'NOT_HELD'::text, v_move;
END;
$$;

------------------------------------------------------------------------------------------------------------------------
-- Paused workflow expiry
------------------------------------------------------------------------------------------------------------------------

-- Expires one PAUSED workflow older than the age policy, in one statement in autocommit; the caller reads the
-- candidates (idx_horsies_workflows_paused_age) and calls this once per workflow. The body of horsies_workflow_cancel
-- for one workflow, with EXPIRED: lock the workflow row FOR UPDATE; the status write needs PAUSED and updated_at
-- before now() minus p_older_than_us (a resume that committed first wins); lock the live backing task rows in id
-- order and the live node rows in id order; EXPIRED with p_error and completed_at; the tasks behind ENQUEUED nodes are
-- moved to history (horsies_cancel_nodes_of_cancelled_workflow, WORKFLOW_CANCEL_WORKFLOW); SKIPPED with completed_at
-- for PENDING and READY nodes, for the ENQUEUED nodes of the moved tasks, and for ENQUEUED sub-workflow nodes with no
-- child. No cascade: each paused child expires on its own age. The status trigger sends workflow_done at commit.
-- Arguments: p_older_than_us >= 0, the age in microseconds; p_error, the stored error object (WORKFLOW_EXPIRED) built
-- by the core. Refused (22023): a NULL argument, a negative age.
-- The age is a time interval with no day field (p_older_than_us * interval '1 microsecond'), so now() minus the age is
-- exactly that much time before now() in any session time zone, also across a daylight-saving change.
-- Outcomes: EXPIRED (expired_task_ids); NOT_EXPIRED (observed_status: not PAUSED, or PAUSED for less than the age;
-- nothing written); NOT_FOUND.
-- Lock order: workflow row, task rows in id order, node rows in id order.
-- Python: expire_paused_workflows (core/workflows/lifecycle.py:1408-1540), one transaction per workflow.
-- Returns one row (outcome, observed_status, expired_task_ids).
CREATE FUNCTION horsies_workflow_expire_paused(p_workflow_id uuid, p_older_than_us bigint, p_error text)
    RETURNS TABLE (outcome text, observed_status text, expired_task_ids uuid[])
    LANGUAGE plpgsql
    SET plan_cache_mode TO 'force_generic_plan'
AS $$
DECLARE
    v_workflow record;
    v_moved uuid[];
BEGIN
    PERFORM horsies_workflow_require(
        p_workflow_id IS NOT NULL AND p_error IS NOT NULL AND p_older_than_us >= 0,
        'the workflow id and error must not be NULL, and the age must be 0 or more microseconds'
    );
    SELECT w.status, w.updated_at INTO v_workflow FROM horsies_workflows w WHERE w.id = p_workflow_id FOR UPDATE;
    IF NOT FOUND THEN
        RETURN QUERY SELECT 'NOT_FOUND'::text, NULL::text, NULL::uuid[];
        RETURN;
    END IF;
    IF v_workflow.status <> 'PAUSED'
       OR v_workflow.updated_at >= now() - p_older_than_us * interval '1 microsecond' THEN
        RETURN QUERY SELECT 'NOT_EXPIRED'::text, v_workflow.status::text, NULL::uuid[];
        RETURN;
    END IF;

    PERFORM 1 FROM horsies_tasks t
    JOIN horsies_workflow_tasks n ON n.task_id = t.id
    WHERE n.workflow_id = p_workflow_id AND n.status IN ('ENQUEUED', 'RUNNING')
    ORDER BY t.id
    FOR UPDATE OF t;
    PERFORM 1 FROM horsies_workflow_tasks n
    WHERE n.workflow_id = p_workflow_id AND n.status IN ('PENDING', 'READY', 'ENQUEUED', 'RUNNING')
    ORDER BY n.id
    FOR UPDATE;

    UPDATE horsies_workflows w
    SET status = 'EXPIRED', error = p_error, completed_at = now(), updated_at = now()
    WHERE w.id = p_workflow_id;
    SELECT coalesce(array_agg(o.task_id ORDER BY o.task_id), ARRAY[]::uuid[]) INTO v_moved
    FROM horsies_cancel_nodes_of_cancelled_workflow(ARRAY[p_workflow_id]) AS o
    WHERE o.outcome = 'APPLIED';
    UPDATE horsies_workflow_tasks n
    SET status = 'SKIPPED', completed_at = now()
    WHERE n.workflow_id = p_workflow_id
      AND (
          n.status IN ('PENDING', 'READY')
          OR (n.status = 'ENQUEUED' AND n.task_id = ANY(v_moved))
          OR (n.status = 'ENQUEUED' AND n.is_subworkflow AND n.sub_workflow_id IS NULL)
      );
    RETURN QUERY SELECT 'EXPIRED'::text, 'EXPIRED'::text, v_moved;
END;
$$;

------------------------------------------------------------------------------------------------------------------------
-- Workflow recovery
------------------------------------------------------------------------------------------------------------------------

-- Step function of recovery case 0: a PENDING node of a RUNNING workflow whose dependencies are all terminal (a join
-- that two concurrent steps both missed), or that has no dependencies (a root that a start left PENDING: it goes
-- ENQUEUED). In the caller's transaction (BEGIN; this; owed child starts; COMMIT).
-- Steps: lock the workflow row FOR UPDATE; the join verdict of the node (horsies_workflow_promote with the node as
-- the candidate: SKIPPED with the skip cascade, ENQUEUED with its task row minted, or a child start owed); then the
-- completion check (horsies_workflow_try_complete).
-- Dispositions: APPLIED (the node left PENDING; detail: its status); WAITING (the join is not met; nothing written);
-- STALE (the node is absent or not PENDING; detail: its status; nothing written); NOT_RUNNING (detail: the workflow
-- status; nothing written); NOT_FOUND.
-- Lock order: workflow row, node rows, new task rows (the promotion locks).
-- Python: recovery case 0 with try_make_ready_and_enqueue (core/workflows/recovery.py:705-760;
-- core/workflows/engine.py:2547-2738).
-- Returns the OUTCOME row (advance_child_workflow_id when the step ended the workflow and it has a parent) and the
-- owed CHILD_START and SOURCE rows.
CREATE FUNCTION horsies_workflow_node_reevaluate(p_workflow_id uuid, p_task_index integer)
    RETURNS SETOF horsies_workflow_step_row
    LANGUAGE plpgsql
    SET plan_cache_mode TO 'force_generic_plan'
AS $$
DECLARE
    v_status text;
    v_node_status text;
BEGIN
    PERFORM horsies_workflow_require(
        p_workflow_id IS NOT NULL AND p_task_index IS NOT NULL, 'the workflow id and task index must not be NULL'
    );
    SELECT w.status INTO v_status FROM horsies_workflows w WHERE w.id = p_workflow_id FOR UPDATE;
    IF NOT FOUND THEN
        RETURN NEXT horsies_workflow_outcome_row('NOT_FOUND', p_workflow_id, FALSE, NULL);
        RETURN;
    END IF;
    IF v_status <> 'RUNNING' THEN
        RETURN NEXT horsies_workflow_outcome_row('NOT_RUNNING', p_workflow_id, FALSE, v_status);
        RETURN;
    END IF;
    SELECT n.status INTO v_node_status
    FROM horsies_workflow_tasks n WHERE n.workflow_id = p_workflow_id AND n.task_index = p_task_index;
    IF v_node_status IS DISTINCT FROM 'PENDING' THEN
        RETURN NEXT horsies_workflow_outcome_row('STALE', p_workflow_id, FALSE, v_node_status);
        RETURN;
    END IF;
    RETURN QUERY SELECT * FROM horsies_workflow_promote(p_workflow_id, NULL, ARRAY[p_task_index], NULL);
    PERFORM horsies_workflow_try_complete(p_workflow_id);
    SELECT n.status INTO v_node_status
    FROM horsies_workflow_tasks n WHERE n.workflow_id = p_workflow_id AND n.task_index = p_task_index;
    RETURN NEXT horsies_workflow_outcome_row(
        CASE WHEN v_node_status = 'PENDING' THEN 'WAITING' ELSE 'APPLIED' END, p_workflow_id, TRUE, v_node_status
    );
END;
$$;

-- Step function of recovery cases 1 and 1.5: READY nodes of a RUNNING workflow that have no work, a task node with
-- no task (case 1) or a sub-workflow node with no child (case 1.5). In the caller's transaction (BEGIN; this; owed
-- child starts; COMMIT).
-- Steps: lock the workflow row FOR UPDATE; of p_task_indexes, the nodes that are still READY with no task (task
-- node) or no child (sub-workflow node) move READY to ENQUEUED (horsies_workflow_promote: a task node gets its task
-- row with its dependency results read from the stored node rows; a sub-workflow node owes a child start, or fails
-- with SUBWORKFLOW_DEPTH_LIMIT_REACHED at depth 32); then the completion check (horsies_workflow_try_complete).
-- Arguments: p_task_indexes, at least one index, no NULL and no repeat (22023).
-- Dispositions: APPLIED (detail: the indexes moved, as a JSON array); STALE (none of the nodes is READY with no work;
-- nothing written); NOT_RUNNING (detail: the workflow status; nothing written); NOT_FOUND.
-- Lock order: workflow row, node rows, new task rows.
-- Python: recovery cases 1 and 1.5 with enqueue_workflow_task and enqueue_subworkflow_task
-- (core/workflows/recovery.py:762-902).
-- Returns the OUTCOME row (advance_child_workflow_id when the step ended the workflow and it has a parent) and the
-- owed CHILD_START and SOURCE rows.
CREATE FUNCTION horsies_workflow_nodes_enqueue_ready(p_workflow_id uuid, p_task_indexes integer[])
    RETURNS SETOF horsies_workflow_step_row
    LANGUAGE plpgsql
    SET plan_cache_mode TO 'force_generic_plan'
AS $$
DECLARE
    v_status text;
    v_ready integer[];
BEGIN
    PERFORM horsies_workflow_require(
        p_workflow_id IS NOT NULL AND cardinality(p_task_indexes) >= 1
        AND array_position(p_task_indexes, NULL) IS NULL
        AND cardinality(p_task_indexes) = (SELECT count(DISTINCT i) FROM unnest(p_task_indexes) AS i),
        'the workflow id must not be NULL, and the task indexes must be one or more distinct non-NULL values'
    );
    SELECT w.status INTO v_status FROM horsies_workflows w WHERE w.id = p_workflow_id FOR UPDATE;
    IF NOT FOUND THEN
        RETURN NEXT horsies_workflow_outcome_row('NOT_FOUND', p_workflow_id, FALSE, NULL);
        RETURN;
    END IF;
    IF v_status <> 'RUNNING' THEN
        RETURN NEXT horsies_workflow_outcome_row('NOT_RUNNING', p_workflow_id, FALSE, v_status);
        RETURN;
    END IF;
    v_ready := ARRAY(
        SELECT n.task_index
        FROM horsies_workflow_tasks n
        WHERE n.workflow_id = p_workflow_id
          AND n.task_index = ANY(p_task_indexes)
          AND n.status = 'READY'
          AND ((NOT n.is_subworkflow AND n.task_id IS NULL) OR (n.is_subworkflow AND n.sub_workflow_id IS NULL))
        ORDER BY n.task_index
    );
    IF cardinality(v_ready) = 0 THEN
        RETURN NEXT horsies_workflow_outcome_row('STALE', p_workflow_id, FALSE, NULL);
        RETURN;
    END IF;
    RETURN QUERY SELECT * FROM horsies_workflow_promote(p_workflow_id, NULL, NULL, v_ready);
    PERFORM horsies_workflow_try_complete(p_workflow_id);
    RETURN NEXT horsies_workflow_outcome_row('APPLIED', p_workflow_id, TRUE, to_json(v_ready)::text);
END;
$$;

-- One page of the audit of RUNNING workflows (recovery cases 2/3 and 4), in one statement in autocommit: Python's
-- audit statement as a function. The cursor row of 'running_workflows' is taken FOR UPDATE SKIP LOCKED, and only
-- when no live claim holds it. The page is the next p_max_rows RUNNING workflows in (created_at, id) order after the
-- cursor, up to the cycle upper bound fixed when the cycle began (a workflow created later waits for the next
-- cycle). Each is classified with two probes of the live-node index: completion candidates (nodes, none live) and
-- orphan candidates (no node). The cursor advances (a short page or the upper bound ends the cycle); when the page
-- has candidates, the cursor row carries the claim p_claim_token until p_claim_ttl_ms after the statement time, which
-- fences a second reaper and this one once the claim expires.
-- Arguments: p_max_rows 1..1000, p_claim_token, p_claim_ttl_ms 1..3,600,000 (22023 otherwise).
-- Outcomes: PAGE (scanned_count, completion_ids and orphan_ids in page order, orphan_names, claim_token when the page
-- has candidates, else NULL); CURSOR_HELD (another reaper holds the cursor row or its live claim; nothing written).
-- Locks: the cursor row. Reads horsies_workflows through idx_horsies_workflows_running_recovery_scan and the nodes
-- through idx_horsies_workflow_tasks_nonterminal and uq_horsies_workflow_task_index.
-- Python: GLOBAL_WORKFLOW_AUDIT_SQL (core/workflows/recovery.py:341-494).
CREATE FUNCTION horsies_workflow_recovery_audit_page(p_max_rows integer, p_claim_token uuid, p_claim_ttl_ms integer)
    RETURNS TABLE (
        outcome text, scanned_count bigint, completion_ids uuid[], orphan_ids uuid[], orphan_names text[],
        claim_token uuid
    )
    LANGUAGE plpgsql
    SET plan_cache_mode TO 'force_generic_plan'
AS $$
#variable_conflict use_column
DECLARE
    v_page record;
BEGIN
    PERFORM horsies_workflow_require(p_max_rows BETWEEN 1 AND 1000, 'the page size must be 1 to 1000');
    PERFORM horsies_workflow_require(p_claim_token IS NOT NULL, 'the claim token must not be NULL');
    PERFORM horsies_workflow_require(
        p_claim_ttl_ms BETWEEN 1 AND 3600000, 'the claim time to live must be 1 to 3,600,000 ms'
    );
    WITH cursor_row AS MATERIALIZED (
        SELECT c.last_created_at, c.last_id, c.cycle_upper_created_at, c.cycle_upper_id
        FROM horsies_recovery_scan_cursors c
        WHERE c.scan_name = 'running_workflows'
          AND (c.claim_token IS NULL OR c.claim_expires_at <= statement_timestamp())
        FOR UPDATE SKIP LOCKED
    ),
    upper_bound AS MATERIALIZED (
        SELECT coalesce(c.cycle_upper_created_at, latest.created_at) AS created_at,
               coalesce(c.cycle_upper_id, latest.id) AS id
        FROM cursor_row c
        LEFT JOIN LATERAL (
            SELECT w.created_at, w.id
            FROM horsies_workflows w
            WHERE c.cycle_upper_id IS NULL
              AND w.status = 'RUNNING'
            ORDER BY w.created_at DESC, w.id DESC
            LIMIT 1
        ) latest ON TRUE
    ),
    scanned AS MATERIALIZED (
        SELECT page.created_at, page.id, page.name
        FROM cursor_row c
        CROSS JOIN upper_bound u
        CROSS JOIN LATERAL (
            SELECT bounded.created_at, bounded.id, bounded.name
            FROM (
                (
                    SELECT w.created_at, w.id, w.name
                    FROM horsies_workflows w
                    WHERE c.last_id IS NULL
                      AND w.status = 'RUNNING'
                      AND u.id IS NOT NULL
                      AND (w.created_at, w.id) <= (u.created_at, u.id)
                    ORDER BY w.created_at, w.id
                    LIMIT p_max_rows
                )
                UNION ALL
                (
                    SELECT w.created_at, w.id, w.name
                    FROM horsies_workflows w
                    WHERE c.last_id IS NOT NULL
                      AND w.status = 'RUNNING'
                      AND u.id IS NOT NULL
                      AND (w.created_at, w.id) > (c.last_created_at, c.last_id)
                      AND (w.created_at, w.id) <= (u.created_at, u.id)
                    ORDER BY w.created_at, w.id
                    LIMIT p_max_rows
                )
            ) bounded
            ORDER BY bounded.created_at, bounded.id
            LIMIT p_max_rows
        ) page
    ),
    classified AS MATERIALIZED (
        SELECT s.created_at, s.id, s.name,
               nonterminal_task.found IS NOT NULL OR any_task.found IS NOT NULL AS has_tasks,
               nonterminal_task.found IS NULL AS all_tasks_terminal
        FROM scanned s
        LEFT JOIN LATERAL (
            SELECT TRUE AS found
            FROM horsies_workflow_tasks wt
            WHERE wt.workflow_id = s.id
              AND wt.status IN ('PENDING', 'READY', 'ENQUEUED', 'RUNNING')
            LIMIT 1
        ) nonterminal_task ON TRUE
        -- A live node already proves the workflow has nodes; the any-node probe runs only when the live probe found
        -- none.
        LEFT JOIN LATERAL (
            SELECT TRUE AS found
            FROM horsies_workflow_tasks wt
            WHERE nonterminal_task.found IS NULL
              AND wt.workflow_id = s.id
            LIMIT 1
        ) any_task ON TRUE
    ),
    summary AS MATERIALIZED (
        SELECT count(*)::bigint AS scanned_count,
               coalesce(array_agg(k.id ORDER BY k.created_at, k.id) FILTER (WHERE k.has_tasks AND k.all_tasks_terminal),
                        ARRAY[]::uuid[]) AS completion_ids,
               coalesce(array_agg(k.id ORDER BY k.created_at, k.id) FILTER (WHERE NOT k.has_tasks),
                        ARRAY[]::uuid[]) AS orphan_ids,
               coalesce(array_agg(k.name::text ORDER BY k.created_at, k.id) FILTER (WHERE NOT k.has_tasks),
                        ARRAY[]::text[]) AS orphan_names
        FROM classified k
    ),
    progress AS MATERIALIZED (
        SELECT s.scanned_count,
               last_row.created_at AS last_created_at,
               last_row.id AS last_id,
               s.scanned_count < p_max_rows OR (last_row.created_at, last_row.id) = (u.created_at, u.id)
                   AS cycle_complete
        FROM summary s
        CROSS JOIN upper_bound u
        LEFT JOIN LATERAL (
            SELECT scanned.created_at, scanned.id
            FROM scanned
            ORDER BY scanned.created_at DESC, scanned.id DESC
            LIMIT 1
        ) last_row ON TRUE
    ),
    advance AS (
        UPDATE horsies_recovery_scan_cursors c
        SET last_created_at = CASE WHEN progress.cycle_complete THEN NULL ELSE progress.last_created_at END,
            last_id = CASE WHEN progress.cycle_complete THEN NULL ELSE progress.last_id END,
            cycle_upper_created_at = CASE WHEN progress.cycle_complete THEN NULL ELSE upper_bound.created_at END,
            cycle_upper_id = CASE WHEN progress.cycle_complete THEN NULL ELSE upper_bound.id END,
            claim_token = CASE
                WHEN cardinality(summary.completion_ids) + cardinality(summary.orphan_ids) > 0 THEN p_claim_token
            END,
            claim_expires_at = CASE
                WHEN cardinality(summary.completion_ids) + cardinality(summary.orphan_ids) > 0
                    THEN statement_timestamp() + p_claim_ttl_ms * interval '1 millisecond'
            END,
            completed_cycles = c.completed_cycles + CASE WHEN progress.cycle_complete THEN 1 ELSE 0 END,
            last_scan_rows = summary.scanned_count::integer,
            last_candidate_rows = cardinality(summary.completion_ids) + cardinality(summary.orphan_ids),
            last_scan_at = statement_timestamp()
        FROM summary, progress, upper_bound
        WHERE c.scan_name = 'running_workflows'
          AND EXISTS (SELECT 1 FROM cursor_row)
        RETURNING c.claim_token
    )
    SELECT summary.scanned_count, summary.completion_ids, summary.orphan_ids, summary.orphan_names,
           (SELECT a.claim_token FROM advance a) AS claim_token
    INTO v_page
    FROM summary
    WHERE EXISTS (SELECT 1 FROM advance);
    IF NOT FOUND THEN
        RETURN QUERY SELECT 'CURSOR_HELD'::text, NULL::bigint, NULL::uuid[], NULL::uuid[], NULL::text[], NULL::uuid;
        RETURN;
    END IF;
    RETURN QUERY SELECT 'PAGE'::text, v_page.scanned_count, v_page.completion_ids, v_page.orphan_ids,
        v_page.orphan_names, v_page.claim_token;
END;
$$;

-- Internal: renews the audit page claim p_claim_token for p_claim_ttl_ms from the statement time. FALSE when the
-- claim is gone (released, expired, or taken by another reaper): the caller then does nothing. The renewal and the
-- candidate's work commit together, which fences an owner whose claim expired.
-- Locks: the cursor row of 'running_workflows'. Python: RENEW_GLOBAL_WORKFLOW_AUDIT_CLAIM_SQL and
-- LOCK_GLOBAL_WORKFLOW_AUDIT_CLAIM_SQL (core/workflows/recovery.py:496-513).
CREATE FUNCTION horsies_workflow_audit_claim_renew(p_claim_token uuid, p_claim_ttl_ms integer) RETURNS boolean
    LANGUAGE plpgsql
    SET plan_cache_mode TO 'force_generic_plan'
AS $$
BEGIN
    PERFORM horsies_workflow_require(p_claim_token IS NOT NULL, 'the claim token must not be NULL');
    PERFORM horsies_workflow_require(
        p_claim_ttl_ms BETWEEN 1 AND 3600000, 'the claim time to live must be 1 to 3,600,000 ms'
    );
    UPDATE horsies_recovery_scan_cursors c
    SET claim_expires_at = statement_timestamp() + p_claim_ttl_ms * interval '1 millisecond'
    WHERE c.scan_name = 'running_workflows'
      AND c.claim_token = p_claim_token
      AND c.claim_expires_at > statement_timestamp();
    RETURN FOUND;
END;
$$;

-- Recovery cases 2/3: the completion of one candidate of an audit page, in one statement in autocommit. Steps: renew
-- the page claim (horsies_workflow_audit_claim_renew; gone: AUDIT_CLAIM_LOST, nothing done); lock the workflow row
-- FOR UPDATE; the completion check (horsies_workflow_try_complete: only a RUNNING workflow with no live node is
-- finalized, with the success policy, the output and the error).
-- Outcomes: COMPLETED or FAILED (this call finalized the workflow; advance_child_workflow_id is the workflow when it
-- has a parent: the caller advances the parent node next, in its own transaction, as Python orders it);
-- UNCHANGED (workflow_status: the observed status, NULL when the workflow is gone; a live node, or not RUNNING);
-- AUDIT_CLAIM_LOST.
-- Lock order: the cursor row, then the workflow row (no other holder of the cursor row waits for a workflow row).
-- Python: _recover_global_completion_candidate (core/workflows/recovery.py:1115-1192).
CREATE FUNCTION horsies_workflow_recover_completion(p_workflow_id uuid, p_claim_token uuid, p_claim_ttl_ms integer)
    RETURNS TABLE (outcome text, workflow_status text, advance_child_workflow_id uuid)
    LANGUAGE plpgsql
    SET plan_cache_mode TO 'force_generic_plan'
AS $$
#variable_conflict use_column
DECLARE
    v_written text;
    v_workflow record;
BEGIN
    PERFORM horsies_workflow_require(p_workflow_id IS NOT NULL, 'the workflow id must not be NULL');
    IF NOT horsies_workflow_audit_claim_renew(p_claim_token, p_claim_ttl_ms) THEN
        RETURN QUERY SELECT 'AUDIT_CLAIM_LOST'::text, NULL::text, NULL::uuid;
        RETURN;
    END IF;
    PERFORM 1 FROM horsies_workflows w WHERE w.id = p_workflow_id FOR UPDATE;
    v_written := horsies_workflow_try_complete(p_workflow_id);
    SELECT w.status, w.parent_workflow_id INTO v_workflow FROM horsies_workflows w WHERE w.id = p_workflow_id;
    RETURN QUERY SELECT coalesce(v_written, 'UNCHANGED'), v_workflow.status::text,
        CASE WHEN v_written IS NOT NULL AND v_workflow.parent_workflow_id IS NOT NULL THEN p_workflow_id END;
END;
$$;

-- Recovery case 4: a RUNNING workflow with no node, from one audit page, in one statement in autocommit. With the
-- one-statement start this state comes only from a write outside the core. Steps: renew the page claim (gone:
-- AUDIT_CLAIM_LOST, nothing done); lock the workflow row FOR UPDATE; FAILED with p_error (the WORKFLOW_START_INCOMPLETE
-- error object built by the core) and completed_at, only while it is RUNNING and still has no node.
-- Outcomes: FAILED (advance_child_workflow_id is the workflow when it has a parent); UNCHANGED (workflow_status: the
-- observed status, NULL when the workflow is gone; not RUNNING, or it has a node); AUDIT_CLAIM_LOST.
-- Arguments: p_error must not be NULL (22023).
-- Lock order: the cursor row, then the workflow row.
-- Python: _recover_global_orphan_candidate with FAIL_ORPHANED_WORKFLOW_SQL (core/workflows/recovery.py:1194-1227,
-- 522-530), which stored the non-catalog code E400.
CREATE FUNCTION horsies_workflow_fail_orphaned(
    p_workflow_id uuid, p_claim_token uuid, p_claim_ttl_ms integer, p_error text
) RETURNS TABLE (outcome text, workflow_status text, advance_child_workflow_id uuid)
    LANGUAGE plpgsql
    SET plan_cache_mode TO 'force_generic_plan'
AS $$
#variable_conflict use_column
DECLARE
    v_workflow record;
BEGIN
    PERFORM horsies_workflow_require(
        p_workflow_id IS NOT NULL AND p_error IS NOT NULL, 'the workflow id and error must not be NULL'
    );
    IF NOT horsies_workflow_audit_claim_renew(p_claim_token, p_claim_ttl_ms) THEN
        RETURN QUERY SELECT 'AUDIT_CLAIM_LOST'::text, NULL::text, NULL::uuid;
        RETURN;
    END IF;
    UPDATE horsies_workflows w
    SET status = 'FAILED', error = p_error, completed_at = now(), updated_at = now()
    WHERE w.id = p_workflow_id
      AND w.status = 'RUNNING'
      AND NOT EXISTS (SELECT 1 FROM horsies_workflow_tasks n WHERE n.workflow_id = w.id)
    RETURNING w.status, w.parent_workflow_id INTO v_workflow;
    IF FOUND THEN
        RETURN QUERY SELECT 'FAILED'::text, 'FAILED'::text,
            CASE WHEN v_workflow.parent_workflow_id IS NOT NULL THEN p_workflow_id END;
        RETURN;
    END IF;
    RETURN QUERY SELECT 'UNCHANGED'::text, (SELECT w.status::text FROM horsies_workflows w WHERE w.id = p_workflow_id),
        NULL::uuid;
END;
$$;

-- Counts one persistent raise of a workflow recovery candidate, in one statement in autocommit after the candidate's
-- own transaction rolled back. A transient raise is not counted: the core does not call this for it.
-- Steps:
-- 1. Lock the workflow row FOR UPDATE (absent: WORKFLOW_ABSENT, nothing written). The row comes first, as in every
--    statement that writes rows keyed by a workflow, so this cannot deadlock against the retention delete, which
--    locks the workflow row and then cascades to the count row.
-- 2. The count, once per pass: an upsert of horsies_workflow_recovery_failures that adds one only when last_pass_id
--    is another pass (else ALREADY_COUNTED, nothing written). It stores the case, the node index and the error type.
-- 3. Under p_limit: COUNTED.
-- 4. At or over p_limit, in an inner block that rolls back alone (the count stays, as Python's two savepoints):
--    the workflow is not RUNNING: REFUSED (a pause or a cancel won; a later pass after a resume tries again);
--    else the limit writes of horsies_workflow_apply_recovery_limit with the WORKFLOW_RECOVERY_LIMIT_REACHED error,
--    built here so it names the stored count: LIMIT_REACHED_FAILED (no live node: the workflow is FAILED) or
--    LIMIT_REACHED_LIVE_NODES (the last live node ends it). A limit write that raises: LIMIT_WRITE_FAILED, with
--    limit_write_error (SQLSTATE and message), and the count stays.
-- The limit error: message 'Workflow recovery raised a persistent error in <n> passes. Last case: <case>. Last
-- raise: <error type>: <detail>', data {"workflow_id", "case", "task_index", "failed_passes", "exception_type"}; the
-- node of the raise stores the envelope, the workflow error column the err slot (Python's recovery_limit_error and
-- recovery_limit_payloads).
-- Arguments: p_pass_id (one value per reaper pass); p_case, one of the five stored cases, with p_task_index NULL
-- exactly for terminal_workflow_running; p_error_type (1..255 characters) and p_error_detail (0..500 characters),
-- cut and escaped by the core; p_limit 3..1000. Refused (22023).
-- Lock order: workflow row, count row, then the limit writes (node rows).
-- Python: record_recovery_raise with _count_recovery_raise and apply_recovery_limit
-- (core/workflows/recovery_limit.py:203-223, 341-470).
-- Returns one row (outcome, failed_passes: the count after this call, NULL when nothing was counted;
-- limit_write_error).
CREATE FUNCTION horsies_workflow_recovery_record_raise(
    p_workflow_id uuid,
    p_pass_id uuid,
    p_case text,
    p_task_index integer,
    p_error_type text,
    p_error_detail text,
    p_limit integer
) RETURNS TABLE (outcome text, failed_passes integer, limit_write_error text)
    LANGUAGE plpgsql
    SET plan_cache_mode TO 'force_generic_plan'
AS $$
#variable_conflict use_column
DECLARE
    v_status text;
    v_failed_passes integer;
    v_node_result text;
    v_outcome text;
BEGIN
    PERFORM horsies_workflow_require(
        p_workflow_id IS NOT NULL AND p_pass_id IS NOT NULL, 'the workflow id and pass id must not be NULL'
    );
    PERFORM horsies_workflow_require(
        p_case IN ('pending_terminal_deps', 'ready_not_enqueued', 'ready_subworkflow_not_started',
                   'completed_child_parent_not_updated', 'terminal_workflow_running')
        AND (p_case = 'terminal_workflow_running') = (p_task_index IS NULL)
        AND (p_task_index IS NULL OR p_task_index >= 0),
        'the case must be a counted recovery case, with a node index exactly when it names a node'
    );
    PERFORM horsies_workflow_require(
        char_length(p_error_type) BETWEEN 1 AND 255 AND char_length(p_error_detail) BETWEEN 0 AND 500,
        'the error type must be 1 to 255 characters and the detail 0 to 500'
    );
    PERFORM horsies_workflow_require(p_limit BETWEEN 3 AND 1000, 'the failure limit must be 3 to 1000');

    SELECT w.status INTO v_status FROM horsies_workflows w WHERE w.id = p_workflow_id FOR UPDATE;
    IF NOT FOUND THEN
        RETURN QUERY SELECT 'WORKFLOW_ABSENT'::text, NULL::integer, NULL::text;
        RETURN;
    END IF;
    INSERT INTO horsies_workflow_recovery_failures AS f (
        workflow_id, failed_passes, last_pass_id, first_failed_at, last_failed_at, last_case, last_task_index,
        last_error_type
    ) VALUES (
        p_workflow_id, 1, p_pass_id, statement_timestamp(), statement_timestamp(), p_case, p_task_index,
        p_error_type
    )
    ON CONFLICT (workflow_id) DO UPDATE
    SET failed_passes = f.failed_passes + 1,
        last_pass_id = EXCLUDED.last_pass_id,
        last_failed_at = EXCLUDED.last_failed_at,
        last_case = EXCLUDED.last_case,
        last_task_index = EXCLUDED.last_task_index,
        last_error_type = EXCLUDED.last_error_type
    WHERE f.last_pass_id <> EXCLUDED.last_pass_id
    RETURNING f.failed_passes INTO v_failed_passes;
    IF v_failed_passes IS NULL THEN
        RETURN QUERY SELECT 'ALREADY_COUNTED'::text, NULL::integer, NULL::text;
        RETURN;
    END IF;
    IF v_failed_passes < p_limit THEN
        RETURN QUERY SELECT 'COUNTED'::text, v_failed_passes, NULL::text;
        RETURN;
    END IF;
    IF v_status <> 'RUNNING' THEN
        RETURN QUERY SELECT 'REFUSED'::text, v_failed_passes, NULL::text;
        RETURN;
    END IF;

    v_node_result := horsies_task_error_envelope(
        'WORKFLOW_RECOVERY_LIMIT_REACHED',
        format('Workflow recovery raised a persistent error in %s passes. Last case: %s. Last raise: %s: %s',
               v_failed_passes, p_case, p_error_type, p_error_detail),
        '{"workflow_id":' || to_json(p_workflow_id::text)::text
            || ',"case":' || to_json(p_case)::text
            || ',"task_index":' || coalesce(p_task_index::text, 'null')
            || ',"failed_passes":' || v_failed_passes
            || ',"exception_type":' || to_json(p_error_type)::text || '}',
        NULL, NULL
    );
    BEGIN
        v_outcome := horsies_workflow_apply_recovery_limit(
            p_workflow_id, p_task_index, (v_node_result::json -> 'err')::text, v_node_result
        );
    EXCEPTION
        WHEN OTHERS THEN
            RETURN QUERY SELECT 'LIMIT_WRITE_FAILED'::text, v_failed_passes, SQLSTATE || ': ' || SQLERRM;
            RETURN;
    END;
    RETURN QUERY SELECT v_outcome, v_failed_passes, NULL::text;
END;
$$;
