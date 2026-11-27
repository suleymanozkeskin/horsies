-- horsies schema migration 1007: the code text SUB_WORKFLOW_FAILED.
--
-- The install applies this text after migration 1006 (core/migrations/1006_typescript_client_family.sql), in one READ
-- COMMITTED transaction with its bookkeeping row. A database that applied an older text of migration 1007 is refused
-- by the checksum.
--
-- Changes:
-- - horsies_task_error_envelope: the closed set of engine codes holds SUB_WORKFLOW_FAILED in place of
--   SUBWORKFLOW_FAILED, and the sub-workflow fields are set only for SUB_WORKFLOW_FAILED. The core passes the code
--   text of OutcomeCode::SubWorkflowFailed (core/src/core/task/error.rs).
-- - horsies_workflow_child_node_result: the node result and the failure error of a FAILED, CANCELLED or EXPIRED child
--   hold the code SUB_WORKFLOW_FAILED.
--
-- The argument lists and the results of both functions do not change, so each function is replaced in place. The
-- comment above each function of this file is its contract from this migration on; the comment in core/baseline/ is
-- the contract before it.
--
-- Rows written before this migration keep the text SUBWORKFLOW_FAILED, and the migration writes no row. A read of such
-- a stored result, node result or workflow error fails closed: the built-in code reader of each client refuses the
-- text. Drain the workflows before the upgrade.
--
-- Rules of the file, as migration 1000: no database default (every insert names every value), objects in the current
-- schema, no transaction control.
--
-- Locks: CREATE OR REPLACE FUNCTION locks the catalog row of the function until the migration commits. No table lock.

------------------------------------------------------------------------------------------------------------------------
-- Engine error envelope
------------------------------------------------------------------------------------------------------------------------

-- The stored text of an engine error: {"__h_task_result__":true,"ok":null,"err":{...}} with a built-in error code,
-- Python's serialize_error_payload text (core/codec/error_payload.py:51-79): the error keys in the order of
-- encode_task_error, exception, error_code, data, message (tests/fixtures/stored_forms/error_data.json). p_data is
-- JSON text or NULL.
-- p_sub_workflow_id and p_sub_workflow_summary (JSON text) are set together, only for SUB_WORKFLOW_FAILED: they are
-- top-level fields of the stored error, as the Python SubWorkflowFailure writes them
-- (core/workflows/engine.py:3238-3281).
-- Refuses a code outside the engine's closed set, a NULL message, and a lone sub-workflow field (22023).
-- Locks: none. Returns the envelope text.
CREATE OR REPLACE FUNCTION horsies_task_error_envelope(
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
            'RESULT_DESERIALIZATION_ERROR', 'SUB_WORKFLOW_FAILED', 'SUBWORKFLOW_DEPTH_LIMIT_REACHED',
            'WORKFLOW_ENQUEUE_FAILED', 'WORKFLOW_RECOVERY_LIMIT_REACHED', 'WORKER_SERIALIZATION_ERROR'
        ),
        format('engine error code %L is not in the engine catalog', p_error_code)
    );
    PERFORM horsies_workflow_require(p_message IS NOT NULL, 'engine error message is NULL');
    PERFORM horsies_workflow_require(
        (p_sub_workflow_id IS NULL) = (p_sub_workflow_summary IS NULL)
        AND ((p_sub_workflow_id IS NOT NULL) = (p_error_code = 'SUB_WORKFLOW_FAILED')),
        'sub_workflow_id and sub_workflow_summary are set together, only for SUB_WORKFLOW_FAILED'
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

------------------------------------------------------------------------------------------------------------------------
-- Sub-workflow completion into the parent node
------------------------------------------------------------------------------------------------------------------------

-- The parent node status and result of a terminal child, and the child summary. Python: _parse_child_output,
-- _completed_child_node, _failed_child_node and the summary of on_subworkflow_complete
-- (core/workflows/engine.py:3118-3281, 3342-3392).
-- - COMPLETED child with a value envelope: COMPLETED, the child's text copied.
-- - COMPLETED child with the outputless envelope: COMPLETED, {"__h_task_result__":true,"ok":null,"err":null}.
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
        || ',"total_tasks":' || v_child.total
        || ',"completed_tasks":' || v_child.completed
        || ',"failed_tasks":' || v_child.failed
        || ',"skipped_tasks":' || v_child.skipped
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
