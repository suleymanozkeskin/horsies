-- Fixtures for the workflow engine cases: helper functions only, no rows. Loaded before
-- baseline_workflow_functions.sql (one transaction, rolled back) and before the two-session cases of
-- sql_tests/baseline_workflow_functions.rs (committed in their own database).

-- The published provenance reader is rendered by horsies_publish_history_readers; this stand-in answers "not found"
-- (no row), so the move's duplicate identity guard passes.
CREATE FUNCTION horsies_task_provenance_staged(p_task_id uuid)
RETURNS TABLE (
    task_id uuid, status text, terminal_at timestamptz, terminalization_kind text, is_workflow_task boolean,
    command_fingerprint_version smallint, command_fingerprint bytea, sent_at timestamptz
)
LANGUAGE sql AS $$
    SELECT NULL::uuid, NULL::text, NULL::timestamptz, NULL::text, NULL::boolean, NULL::smallint, NULL::bytea,
        NULL::timestamptz
    WHERE FALSE
$$;

-- The move's duplicate identity guard is a published reader too. This stand-in answers TRUE when the provenance
-- stand-in finds the id (never) or a clock exception records it.
CREATE FUNCTION horsies_task_history_duplicate_staged(p_task_id uuid)
RETURNS boolean LANGUAGE sql STABLE AS $$
    SELECT EXISTS (SELECT 1 FROM horsies_task_provenance_staged(p_task_id))
        OR EXISTS (SELECT 1 FROM horsies_task_history_clock_exceptions AS e WHERE e.task_id = p_task_id)
$$;

CREATE FUNCTION wft_check(p_condition boolean, p_case text) RETURNS void LANGUAGE plpgsql AS $$
BEGIN
    IF p_condition IS NOT TRUE THEN
        RAISE EXCEPTION 'workflow function case failed: %', p_case;
    END IF;
END;
$$;

-- Runs p_sql and checks that it raises p_sqlstate. The statement runs in a subtransaction, so nothing it wrote stays.
CREATE FUNCTION wft_refused(p_sql text, p_sqlstate text, p_case text) RETURNS void LANGUAGE plpgsql AS $$
BEGIN
    BEGIN
        EXECUTE p_sql;
    EXCEPTION WHEN OTHERS THEN
        IF SQLSTATE <> p_sqlstate THEN
            RAISE EXCEPTION 'workflow function case failed: % (SQLSTATE % instead of %: %)',
                p_case, SQLSTATE, p_sqlstate, SQLERRM;
        END IF;
        RETURN;
    END;
    RAISE EXCEPTION 'workflow function case failed: % (no error)', p_case;
END;
$$;

-- The arguments of a tree, as the arrays of horsies_workflow_start, from a JSON description. p_workflows: [{"id",
-- "name", "on_error", "parent", "parent_index", "depth", "output", "policy"}], the top row first. p_nodes: [{"wf",
-- "idx", "deps", "args_from", "ctx", "join", "min", "allow", "sub", "params", "input", "node_id", "good_until"}]; "wf"
-- defaults to the top row's id; "params" is the sub_workflow_params of a sub-workflow node (NOT_GIVEN when absent).
-- A task node stores the options {"retry":"none"}, with "good_until" added when the node has one, as the clients'
-- starts merge the deadline into the stored options.
-- p_failed: [{"wf", "idx", "result"}].
CREATE FUNCTION wft_tree(p_workflows jsonb, p_nodes jsonb, p_failed jsonb)
RETURNS TABLE (
    workflow_ids uuid[], names text[], definition_keys text[], on_errors text[], output_task_indexes integer[],
    success_policies text[], parent_workflow_ids uuid[], parent_task_indexes integer[], depths integer[],
    node_workflow_ids uuid[], node_task_indexes integer[], node_ids text[], node_task_names text[],
    node_inputs text[], node_queue_names text[], node_priorities integer[], node_dependencies text[],
    node_args_from text[], node_workflow_ctx_from text[], node_allow_failed_deps boolean[], node_join_types text[],
    node_min_successes integer[], node_task_options text[], node_max_retries integer[], node_executions text[],
    node_good_untils timestamp with time zone[], node_retention_class_keys text[], node_is_subworkflow boolean[],
    node_sub_workflow_names text[], node_sub_definition_keys text[], node_sub_workflow_params text[],
    failed_workflow_ids uuid[], failed_task_indexes integer[], failed_results text[]
) LANGUAGE sql AS $$
    WITH w AS (
        SELECT (x ->> 'id')::uuid AS id, coalesce(x ->> 'name', 'wf') AS name, x ->> 'on_error' AS on_error,
               (x ->> 'parent')::uuid AS parent, (x ->> 'parent_index')::integer AS parent_index,
               coalesce((x ->> 'depth')::integer, 0) AS depth, (x ->> 'output')::integer AS output,
               x ->> 'policy' AS policy, o AS position
        FROM jsonb_array_elements(p_workflows) WITH ORDINALITY AS a(x, o)
    ),
    n AS (
        SELECT coalesce((x ->> 'wf')::uuid, (p_workflows -> 0 ->> 'id')::uuid) AS wf,
               (x ->> 'idx')::integer AS idx,
               coalesce(x ->> 'node_id', 'n' || (x ->> 'idx')) AS node_id,
               coalesce(x ->> 'task_name', 't' || (x ->> 'idx')) AS task_name,
               coalesce(x ->> 'input', '{}') AS input,
               coalesce((x -> 'deps')::text, '[]') AS deps,
               (x -> 'args_from')::text AS args_from,
               (x -> 'ctx')::text AS ctx,
               coalesce((x ->> 'allow')::boolean, FALSE) AS allow,
               coalesce(x ->> 'join', 'all') AS join_type,
               (x ->> 'min')::integer AS min_success,
               coalesce((x ->> 'sub')::boolean, FALSE) AS sub,
               coalesce(x ->> 'params', 'NOT_GIVEN') AS params,
               (x ->> 'good_until')::timestamptz AS good_until,
               x -> 'good_until' AS good_until_json,
               o AS position
        FROM jsonb_array_elements(p_nodes) WITH ORDINALITY AS a(x, o)
    ),
    f AS (
        SELECT coalesce((x ->> 'wf')::uuid, (p_workflows -> 0 ->> 'id')::uuid) AS wf, (x ->> 'idx')::integer AS idx,
               x ->> 'result' AS result, o AS position
        FROM jsonb_array_elements(p_failed) WITH ORDINALITY AS a(x, o)
    )
    SELECT
        (SELECT array_agg(id ORDER BY position) FROM w), (SELECT array_agg(name ORDER BY position) FROM w),
        (SELECT array_agg(NULL::text ORDER BY position) FROM w), (SELECT array_agg(on_error ORDER BY position) FROM w),
        (SELECT array_agg(output ORDER BY position) FROM w), (SELECT array_agg(policy ORDER BY position) FROM w),
        (SELECT array_agg(parent ORDER BY position) FROM w), (SELECT array_agg(parent_index ORDER BY position) FROM w),
        (SELECT array_agg(depth ORDER BY position) FROM w),
        (SELECT array_agg(wf ORDER BY position) FROM n), (SELECT array_agg(idx ORDER BY position) FROM n),
        (SELECT array_agg(node_id ORDER BY position) FROM n), (SELECT array_agg(task_name ORDER BY position) FROM n),
        (SELECT array_agg(input ORDER BY position) FROM n),
        (SELECT array_agg('default'::text ORDER BY position) FROM n),
        (SELECT array_agg(50 ORDER BY position) FROM n), (SELECT array_agg(deps ORDER BY position) FROM n),
        (SELECT array_agg(args_from ORDER BY position) FROM n), (SELECT array_agg(ctx ORDER BY position) FROM n),
        (SELECT array_agg(allow ORDER BY position) FROM n), (SELECT array_agg(join_type ORDER BY position) FROM n),
        (SELECT array_agg(min_success ORDER BY position) FROM n),
        (SELECT array_agg(CASE
            WHEN sub THEN NULL
            WHEN good_until_json IS NULL THEN '{"retry":"none"}'
            ELSE '{"retry":"none","good_until":' || good_until_json::text || '}'
        END ORDER BY position) FROM n),
        (SELECT array_agg(CASE WHEN sub THEN NULL ELSE 2 END ORDER BY position) FROM n),
        (SELECT array_agg(CASE WHEN sub THEN NULL ELSE 'SHARED_ASYNC' END ORDER BY position) FROM n),
        (SELECT array_agg(CASE WHEN sub THEN NULL ELSE good_until END ORDER BY position) FROM n),
        (SELECT array_agg(CASE WHEN sub THEN NULL ELSE 'forever' END ORDER BY position) FROM n),
        (SELECT array_agg(sub ORDER BY position) FROM n),
        (SELECT array_agg(CASE WHEN sub THEN 'child.wf' END ORDER BY position) FROM n),
        (SELECT array_agg(CASE WHEN sub THEN 'child.key' END ORDER BY position) FROM n),
        (SELECT array_agg(CASE WHEN sub THEN params END ORDER BY position) FROM n),
        coalesce((SELECT array_agg(wf ORDER BY position) FROM f), ARRAY[]::uuid[]),
        coalesce((SELECT array_agg(idx ORDER BY position) FROM f), ARRAY[]::integer[]),
        coalesce((SELECT array_agg(result ORDER BY position) FROM f), ARRAY[]::text[])
$$;

-- Starts a tree from a JSON description (wft_tree). A start with p_trace_id carries the trace pair (CARRIER), a
-- start without one carries none (NO_CARRIER).
CREATE FUNCTION wft_start(p_workflows jsonb, p_nodes jsonb, p_failed jsonb DEFAULT '[]',
                          p_trace_carrier text DEFAULT NULL, p_trace_id bytea DEFAULT NULL)
RETURNS SETOF horsies_workflow_step_row LANGUAGE sql AS $$
    SELECT s.* FROM wft_tree(p_workflows, p_nodes, p_failed) AS a
    CROSS JOIN LATERAL horsies_workflow_start(
        a.workflow_ids, a.names, a.definition_keys, a.on_errors, a.output_task_indexes, a.success_policies,
        a.parent_workflow_ids, a.parent_task_indexes, a.depths, '2030-01-01 00:00:00+00',
        a.node_workflow_ids, a.node_task_indexes, a.node_ids, a.node_task_names, a.node_inputs, a.node_queue_names,
        a.node_priorities, a.node_dependencies, a.node_args_from, a.node_workflow_ctx_from, a.node_allow_failed_deps,
        a.node_join_types, a.node_min_successes, a.node_task_options, a.node_max_retries, a.node_executions,
        a.node_good_untils, a.node_retention_class_keys, a.node_is_subworkflow, a.node_sub_workflow_names,
        a.node_sub_definition_keys, a.node_sub_workflow_params, a.failed_workflow_ids, a.failed_task_indexes,
        a.failed_results, CASE WHEN p_trace_id IS NULL THEN 'NO_CARRIER' ELSE 'CARRIER' END, p_trace_carrier,
        p_trace_id
    ) AS s
$$;

-- The TREE child start of node p_index of p_parent from a JSON description (wft_tree): the first workflow row is the
-- child, with "parent", "parent_index" and "depth" set.
CREATE FUNCTION wft_child_start(p_parent uuid, p_index integer, p_workflows jsonb, p_nodes jsonb,
                                p_failed jsonb DEFAULT '[]')
RETURNS SETOF horsies_workflow_step_row LANGUAGE sql AS $$
    SELECT s.* FROM wft_tree(p_workflows, p_nodes, p_failed) AS a
    CROSS JOIN LATERAL horsies_workflow_child_start(
        p_parent, p_index, 'TREE', NULL, '2030-01-02 00:00:00+00',
        a.workflow_ids, a.names, a.definition_keys, a.on_errors, a.output_task_indexes, a.success_policies,
        a.parent_workflow_ids, a.parent_task_indexes, a.depths,
        a.node_workflow_ids, a.node_task_indexes, a.node_ids, a.node_task_names, a.node_inputs, a.node_queue_names,
        a.node_priorities, a.node_dependencies, a.node_args_from, a.node_workflow_ctx_from, a.node_allow_failed_deps,
        a.node_join_types, a.node_min_successes, a.node_task_options, a.node_max_retries, a.node_executions,
        a.node_good_untils, a.node_retention_class_keys, a.node_is_subworkflow, a.node_sub_workflow_names,
        a.node_sub_definition_keys, a.node_sub_workflow_params, a.failed_workflow_ids, a.failed_task_indexes,
        a.failed_results
    ) AS s
$$;

-- The FAILURE child start of node p_index of p_parent, with the error envelope p_result.
CREATE FUNCTION wft_child_fail(p_parent uuid, p_index integer, p_result text)
RETURNS SETOF horsies_workflow_step_row LANGUAGE sql AS $$
    SELECT * FROM horsies_workflow_child_start(
        p_parent, p_index, 'FAILURE', p_result, NULL,
        NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
        NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
        NULL, NULL, NULL, NULL,
        NULL, NULL, NULL
    )
$$;

-- A one-node workflow p_id started with the trace arguments as given, for the refusals of the trace variant.
CREATE FUNCTION wft_start_traced(p_id uuid, p_trace_kind text, p_trace_carrier text, p_trace_id bytea)
RETURNS SETOF horsies_workflow_step_row LANGUAGE sql AS $$
    SELECT s.* FROM wft_tree(
        jsonb_build_array(jsonb_build_object('id', p_id, 'on_error', 'fail')), '[{"idx":0}]', '[]'
    ) AS a
    CROSS JOIN LATERAL horsies_workflow_start(
        a.workflow_ids, a.names, a.definition_keys, a.on_errors, a.output_task_indexes, a.success_policies,
        a.parent_workflow_ids, a.parent_task_indexes, a.depths, '2030-01-01 00:00:00+00',
        a.node_workflow_ids, a.node_task_indexes, a.node_ids, a.node_task_names, a.node_inputs, a.node_queue_names,
        a.node_priorities, a.node_dependencies, a.node_args_from, a.node_workflow_ctx_from, a.node_allow_failed_deps,
        a.node_join_types, a.node_min_successes, a.node_task_options, a.node_max_retries, a.node_executions,
        a.node_good_untils, a.node_retention_class_keys, a.node_is_subworkflow, a.node_sub_workflow_names,
        a.node_sub_definition_keys, a.node_sub_workflow_params, a.failed_workflow_ids, a.failed_task_indexes,
        a.failed_results, p_trace_kind, p_trace_carrier, p_trace_id
    ) AS s
$$;

-- One workflow, no children.
CREATE FUNCTION wft_start1(p_id uuid, p_on_error text, p_nodes jsonb, p_output integer DEFAULT NULL,
                           p_policy text DEFAULT NULL)
RETURNS SETOF horsies_workflow_step_row LANGUAGE sql AS $$
    SELECT * FROM wft_start(
        jsonb_build_array(jsonb_build_object('id', p_id, 'on_error', p_on_error, 'output', p_output, 'policy', p_policy)),
        p_nodes
    )
$$;

CREATE FUNCTION wft_task_of(p_workflow uuid, p_index integer) RETURNS uuid LANGUAGE sql AS $$
    SELECT task_id FROM horsies_workflow_tasks WHERE workflow_id = p_workflow AND task_index = p_index
$$;

CREATE FUNCTION wft_node(p_workflow uuid, p_index integer) RETURNS horsies_workflow_tasks LANGUAGE sql AS $$
    SELECT * FROM horsies_workflow_tasks WHERE workflow_id = p_workflow AND task_index = p_index
$$;

CREATE FUNCTION wft_workflow(p_workflow uuid) RETURNS horsies_workflows LANGUAGE sql AS $$
    SELECT * FROM horsies_workflows WHERE id = p_workflow
$$;

-- A value result envelope, and an error envelope with a user code.
CREATE FUNCTION wft_ok(p_value text) RETURNS text LANGUAGE sql AS $$
    SELECT '{"__h_task_result__":true,"ok":' || p_value || ',"err":null}'
$$;
CREATE FUNCTION wft_err(p_code text) RETURNS text LANGUAGE sql AS $$
    SELECT '{"__h_task_result__":true,"ok":null,"err":{"error_code":"' || p_code
        || '","message":"boom","data":null,"exception":null}}'
$$;

-- Runs phase 1 of a workflow task: the task becomes RUNNING and moves to history with the phase-2 record
-- (COMPLETE_LOCKED for a value, FAIL_RUNNING for an error), as horsies_finish_workflow_success and
-- horsies_finish_failed_attempt do.
CREATE FUNCTION wft_phase1(p_task_id uuid, p_result text) RETURNS void LANGUAGE plpgsql AS $$
DECLARE
    v_failed boolean := (p_result::json -> 'err')::text <> 'null';
    v_task horsies_tasks;
BEGIN
    -- The move takes the row that the UPDATE returned. The stand-in claim counts one claim of the attempt.
    UPDATE horsies_tasks
    SET status = 'RUNNING', claimed_at = now(), claimed_by_worker_id = 'worker-1', started_at = now(),
        claim_expires_at = NULL, worker_pid = 1, worker_hostname = 'host', worker_process_name = 'proc',
        attempt_claims = 1, first_claimed_at = now()
    WHERE id = p_task_id
    RETURNING * INTO v_task;
    PERFORM wft_check(FOUND, 'phase-1 fixture finds task ' || p_task_id);
    PERFORM horsies_move_task_to_history(
        v_task,
        CASE WHEN v_failed THEN 'FAILED' ELSE 'COMPLETED' END,
        CASE WHEN v_failed THEN 'FAIL_RUNNING' ELSE 'COMPLETE_LOCKED' END,
        now(), p_result,
        CASE WHEN v_failed THEN 'USER_ERROR' END,
        CASE WHEN v_failed THEN 'boom' END
    );
END;
$$;

-- Phase 1 then phase 2 of the task of one node; returns the OUTCOME disposition.
CREATE FUNCTION wft_finish(p_workflow uuid, p_index integer, p_result text) RETURNS text LANGUAGE plpgsql AS $$
DECLARE
    v_task uuid := wft_task_of(p_workflow, p_index);
    v_disposition text;
BEGIN
    PERFORM wft_check(v_task IS NOT NULL, format('node %s of %s has a task', p_index, p_workflow));
    PERFORM wft_phase1(v_task, p_result);
    SELECT disposition INTO v_disposition
    FROM horsies_workflow_node_finish(v_task, TRUE, 'READ') WHERE row_kind = 'OUTCOME';
    RETURN v_disposition;
END;
$$;

-- Claims the task of a node for worker-1 at p_claimed_at, with a live lease and the first claim of its attempt, as
-- horsies_claim leaves it.
CREATE FUNCTION wft_claim(p_task_id uuid, p_claimed_at timestamptz) RETURNS void LANGUAGE plpgsql AS $$
BEGIN
    UPDATE horsies_tasks
    SET status = 'CLAIMED', claimed_at = p_claimed_at, claimed_by_worker_id = 'worker-1',
        claim_expires_at = now() + interval '1 minute', attempt_claims = 1, first_claimed_at = p_claimed_at
    WHERE id = p_task_id AND status = 'PENDING';
    PERFORM wft_check(FOUND, 'claim fixture finds the PENDING task ' || p_task_id);
END;
$$;

-- The history row of a task that moved: status and terminalization kind, as 'STATUS:KIND'; NULL when it did not move.
CREATE FUNCTION wft_moved(p_task_id uuid) RETURNS text LANGUAGE sql AS $$
    SELECT h.status || ':' || h.terminalization_kind FROM horsies_task_history h WHERE h.task_id = p_task_id
$$;

-- Phase 2 of the task of node p_index, then, in the same transaction, the TREE child start that it owes: the child
-- p_child is a one-node workflow. Returns the disposition of the child start. Fails the case when no child start is
-- owed.
CREATE FUNCTION wft_finish_with_child(p_workflow uuid, p_index integer, p_child uuid) RETURNS text
LANGUAGE plpgsql AS $$
DECLARE
    v_owed horsies_workflow_step_row;
    v_disposition text;
BEGIN
    SELECT * INTO v_owed FROM horsies_workflow_node_finish(wft_task_of(p_workflow, p_index), TRUE, 'READ') AS r
    WHERE r.row_kind = 'CHILD_START';
    PERFORM wft_check(v_owed.task_index IS NOT NULL, 'the phase 2 of node ' || p_index || ' owes a child start');
    SELECT s.disposition INTO v_disposition
    FROM wft_child_start(p_workflow, v_owed.task_index,
        jsonb_build_array(jsonb_build_object('id', p_child, 'on_error', 'fail', 'parent', p_workflow,
                                             'parent_index', v_owed.task_index, 'depth', v_owed.parent_depth + 1)),
        jsonb_build_array(jsonb_build_object('wf', p_child, 'idx', 0))) AS s
    WHERE s.row_kind = 'OUTCOME';
    RETURN v_disposition;
END;
$$;
