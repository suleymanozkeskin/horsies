-- horsies schema migration 1009: one stored form for a COMPLETED workflow with no output node.
--
-- The install applies this text after the migration before it in the chain (MIGRATION_SOURCES,
-- core/src/broker/migrations.rs), in one READ COMMITTED transaction with its bookkeeping row. A database that applied
-- an older text of migration 1009 is refused by the checksum.
--
-- Changes:
-- - horsies_workflow_try_complete: a workflow with no output node stores the plain envelope
--   {"__h_task_result__":true,"ok":null,"err":null} as its result, the form of a sub-workflow node of such a child.
--   It stored an envelope with the extra key __h_outputless_terminals__ and a map of the terminal nodes in its ok slot.
--   The terminal nodes are read from the node rows (results, nodes, result_for).
-- - horsies_workflow_outputless_result: dropped. Nothing calls it.
-- - horsies_workflow_result_kind: the kind 'OUTPUTLESS' is gone. A result with the key __h_outputless_terminals__ is
--   'UNREADABLE'.
-- - horsies_workflow_err_slot and horsies_workflow_child_node_result: the kind 'OUTPUTLESS' is gone from both.
--
-- Rows written before this migration do not change, and the migration writes no row. A COMPLETED workflow with the old
-- envelope does not read: a parent node that takes it as the result of a child fails with
-- RESULT_DESERIALIZATION_ERROR, and every client refuses to read it as a result. Drain the workflows with no output
-- node before the upgrade.
--
-- Rules of the file, as migration 1000: no database default (every insert names every value), objects in the current
-- schema, no transaction control. Each function keeps its argument list and its result, so each one is replaced in
-- place. The comment above each function of this file is its contract from this migration on; the comment in
-- core/baseline/ and in the earlier migration is the contract before it.
--
-- Locks: none on a table. CREATE OR REPLACE FUNCTION and DROP FUNCTION lock only the function rows of the catalog.

------------------------------------------------------------------------------------------------------------------------
-- Result kind and err slot
------------------------------------------------------------------------------------------------------------------------

-- The kind of a stored result text, by the strict envelope rule of Python's validate_task_result_envelope
-- (core/codec/typed.py:1133-1180): an object with exactly the keys __h_task_result__ (true), ok and err, and at most
-- one slot set. Returns:
-- - 'VALUE': a valid envelope with an empty err slot;
-- - 'ERROR': a valid envelope whose err slot is an object with an error_code;
-- - 'ABSENT': NULL;
-- - 'UNREADABLE': anything else (not JSON, an empty text included, another shape, an err slot without an error_code).
--   No writer stores an empty text, so it is corrupt data, not "no result".
-- The keys sort by code point (COLLATE "C"): a default collation such as glibc's en_US.UTF-8 ignores '_' and sorts
-- '__h_task_result__' between 'err' and 'ok'.
-- An envelope of the form that migrations before 1009 stored for a workflow with no output node (an extra key
-- __h_outputless_terminals__) is 'UNREADABLE'.
-- Locks: none.
CREATE OR REPLACE FUNCTION horsies_workflow_result_kind(p_text text) RETURNS text
    LANGUAGE plpgsql IMMUTABLE
AS $$
DECLARE
    v_value json := horsies_workflow_json_or_null(p_text);
    v_keys text[];
BEGIN
    IF p_text IS NULL THEN
        RETURN 'ABSENT';
    END IF;
    IF v_value IS NULL OR json_typeof(v_value) <> 'object' THEN
        RETURN 'UNREADABLE';
    END IF;
    IF json_typeof(v_value -> '__h_task_result__') IS DISTINCT FROM 'boolean'
        OR (v_value ->> '__h_task_result__') <> 'true' THEN
        RETURN 'UNREADABLE';
    END IF;
    v_keys := ARRAY(SELECT k FROM json_object_keys(v_value) AS k ORDER BY k COLLATE "C");
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

-- The error of a stored node result, as the bare error object text, for the workflow error column. Python 0.7.0
-- (`v0.7.0` package): _decode_err_only (core/workflows/engine.py:284-337) and _recovered_failure_result
-- (core/workflows/phase2_recovery.py:222-287). Returns the err slot text unchanged for 'ERROR'; NULL for 'VALUE',
-- and 'ABSENT' (NULL); for 'UNREADABLE', an empty
-- text included, a RESULT_DESERIALIZATION_ERROR object whose message is the cause
-- (horsies_workflow_unreadable_result_cause).
-- Locks: none.
CREATE OR REPLACE FUNCTION horsies_workflow_err_slot(p_result text) RETURNS text
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
        WHEN 'VALUE', 'ABSENT' THEN
            RETURN NULL;
    END CASE;
END;
$$;

------------------------------------------------------------------------------------------------------------------------
-- Completion
------------------------------------------------------------------------------------------------------------------------

-- Finalizes a RUNNING workflow with no live node, once. Returns the status it wrote (COMPLETED or FAILED), or NULL
-- when the workflow is not RUNNING or has a live node (PENDING, READY, ENQUEUED, RUNNING).
-- The stored policy is read with horsies_workflow_success_cases, once no node is live; one that does not parse raises
-- XX001 and nothing is written.
-- - No success policy: COMPLETED when no node FAILED and no error is stored; else FAILED.
-- - A policy: COMPLETED when every required node of some case with required nodes is COMPLETED; else FAILED.
--   Indexes are compared as jsonb numbers, so an index above the integer range names no node and its case is not met.
-- COMPLETED stores the output (horsies_workflow_explicit_output) or, for a workflow with no output node, the
-- plain envelope {"__h_task_result__":true,"ok":null,"err":null}; FAILED
-- stores no result and the error of horsies_workflow_failure_error, or keeps the stored error. Both set
-- completed_at. The status trigger sends workflow_done at commit.
-- Precondition: the caller holds the workflow row FOR UPDATE.
-- Python: check_workflow_completion and evaluate_workflow_success (core/workflows/engine.py).
-- Locks: the workflow row (held by the caller); reads the nodes.
CREATE OR REPLACE FUNCTION horsies_workflow_try_complete(p_workflow_id uuid) RETURNS text
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
                WHEN v_wf.output_task_index IS NULL THEN '{"__h_task_result__":true,"ok":null,"err":null}'
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

-- Nothing calls it from this migration on.
DROP FUNCTION horsies_workflow_outputless_result(uuid);

------------------------------------------------------------------------------------------------------------------------
-- Sub-workflow completion into the parent node
------------------------------------------------------------------------------------------------------------------------

-- The parent node status and result of a terminal child, and the child summary. Python: _parse_child_output,
-- _completed_child_node, _failed_child_node and the summary of on_subworkflow_complete
-- (core/workflows/engine.py:3118-3281, 3342-3392).
-- - COMPLETED child with a value envelope (the plain envelope of a child with no output node included): COMPLETED, the
--   child's text copied.
-- - COMPLETED child with an error envelope: FAILED, the child's text copied.
-- - COMPLETED child with a result that is absent (NULL) or not an envelope (an empty text included): FAILED,
--   RESULT_DESERIALIZATION_ERROR naming the child.
-- - FAILED, CANCELLED or EXPIRED child: FAILED, SUB_WORKFLOW_FAILED with sub_workflow_id and sub_workflow_summary.
-- The summary: status, output (the ok value of a value envelope, else null), the node counts, error_summary (the
-- message of a stored error object, the first 200 characters of a stored error that is not JSON (an empty text
-- included: no writer stores one) or is the JSON null, null for a NULL error, else null).
-- failure_error: the error that the failure policy of the parent records for a FAILED node, as the bare error object
-- text. For a FAILED, CANCELLED or EXPIRED child it is SUB_WORKFLOW_FAILED with the message "Subworkflow <id> failed"
-- and the same sub_workflow_id and summary (Python: the error of _ParentNodeFailed); for a FAILED node of a COMPLETED
-- child, the err slot of node_result; NULL for a COMPLETED node.
-- Locks: none (the child is terminal and does not change). Returns node_status, node_result, summary and
-- failure_error.
CREATE OR REPLACE FUNCTION horsies_workflow_child_node_result(
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
        WHEN v_child.error IS NULL THEN NULL
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
        || ',"total_nodes":' || v_child.total
        || ',"completed_nodes":' || v_child.completed
        || ',"failed_nodes":' || v_child.failed
        || ',"skipped_nodes":' || v_child.skipped
        || ',"error_summary":' || coalesce(to_json(v_error_summary)::text, 'null') || '}';
    IF v_child.status <> 'COMPLETED' THEN
        node_status := 'FAILED';
        node_result := horsies_task_error_envelope(
            'SUB_WORKFLOW_FAILED',
            format('Subworkflow %s failed with status %s', p_child_workflow_id, v_child.status),
            NULL, p_child_workflow_id, summary
        );
        failure_error := (horsies_task_error_envelope(
            'SUB_WORKFLOW_FAILED', format('Subworkflow %s failed', p_child_workflow_id), NULL, p_child_workflow_id,
            summary
        )::json -> 'err')::text;
        RETURN;
    END IF;
    CASE v_kind
        WHEN 'VALUE' THEN
            node_status := 'COMPLETED';
            node_result := v_child.result;
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
