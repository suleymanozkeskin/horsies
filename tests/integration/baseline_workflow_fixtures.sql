-- Fixtures for the workflow engine cases: helper functions only, no rows. Loaded before
-- baseline_workflow_functions.sql (one transaction, rolled back) and before the two-session cases of
-- test_baseline_workflow_functions.py (committed in their own database).

-- The published provenance reader is rendered at run time; this stand-in answers "not found", so the move's
-- duplicate identity guard passes.
CREATE FUNCTION horsies_task_provenance_staged(p_task_id uuid, p_include_live boolean DEFAULT TRUE)
RETURNS horsies_task_provenance LANGUAGE sql AS $$
    SELECT ROW(FALSE, NULL, NULL, NULL, NULL, NULL)::horsies_task_provenance
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

-- Starts a tree from a JSON description. p_workflows: [{"id", "name", "on_error", "parent", "parent_index", "depth",
-- "output", "policy"}], the top-level row first. p_nodes: [{"wf", "idx", "deps", "args_from", "ctx", "join", "min",
-- "allow", "sub", "input", "node_id", "good_until"}]; "wf" defaults to the top-level id. p_failed: [{"wf", "idx",
-- "result"}].
CREATE FUNCTION wft_start(p_workflows jsonb, p_nodes jsonb, p_failed jsonb DEFAULT '[]')
RETURNS SETOF horsies_workflow_step_row LANGUAGE sql AS $$
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
               (x ->> 'good_until')::timestamptz AS good_until,
               o AS position
        FROM jsonb_array_elements(p_nodes) WITH ORDINALITY AS a(x, o)
    ),
    f AS (
        SELECT coalesce((x ->> 'wf')::uuid, (p_workflows -> 0 ->> 'id')::uuid) AS wf, (x ->> 'idx')::integer AS idx,
               x ->> 'result' AS result, o AS position
        FROM jsonb_array_elements(p_failed) WITH ORDINALITY AS a(x, o)
    )
    SELECT * FROM horsies_workflow_start(
        (SELECT array_agg(id ORDER BY position) FROM w), (SELECT array_agg(name ORDER BY position) FROM w),
        (SELECT array_agg(NULL::text ORDER BY position) FROM w), (SELECT array_agg(on_error ORDER BY position) FROM w),
        (SELECT array_agg(output ORDER BY position) FROM w), (SELECT array_agg(policy ORDER BY position) FROM w),
        (SELECT array_agg(parent ORDER BY position) FROM w), (SELECT array_agg(parent_index ORDER BY position) FROM w),
        (SELECT array_agg(depth ORDER BY position) FROM w),
        '2030-01-01 00:00:00+00',
        (SELECT array_agg(wf ORDER BY position) FROM n), (SELECT array_agg(idx ORDER BY position) FROM n),
        (SELECT array_agg(node_id ORDER BY position) FROM n), (SELECT array_agg(task_name ORDER BY position) FROM n),
        (SELECT array_agg(input ORDER BY position) FROM n),
        (SELECT array_agg('default'::text ORDER BY position) FROM n),
        (SELECT array_agg(50 ORDER BY position) FROM n), (SELECT array_agg(deps ORDER BY position) FROM n),
        (SELECT array_agg(args_from ORDER BY position) FROM n), (SELECT array_agg(ctx ORDER BY position) FROM n),
        (SELECT array_agg(allow ORDER BY position) FROM n), (SELECT array_agg(join_type ORDER BY position) FROM n),
        (SELECT array_agg(min_success ORDER BY position) FROM n),
        (SELECT array_agg(CASE WHEN sub THEN NULL ELSE '{"retry":"none"}' END ORDER BY position) FROM n),
        (SELECT array_agg(CASE WHEN sub THEN NULL ELSE 2 END ORDER BY position) FROM n),
        (SELECT array_agg(CASE WHEN sub THEN NULL ELSE 'SHARED_ASYNC' END ORDER BY position) FROM n),
        (SELECT array_agg(CASE WHEN sub THEN NULL ELSE good_until END ORDER BY position) FROM n),
        (SELECT array_agg(CASE WHEN sub THEN NULL ELSE 'forever' END ORDER BY position) FROM n),
        (SELECT array_agg(sub ORDER BY position) FROM n),
        (SELECT array_agg(CASE WHEN sub THEN 'child.wf' END ORDER BY position) FROM n),
        (SELECT array_agg(CASE WHEN sub THEN 'child.key' END ORDER BY position) FROM n),
        coalesce((SELECT array_agg(wf ORDER BY position) FROM f), ARRAY[]::uuid[]),
        coalesce((SELECT array_agg(idx ORDER BY position) FROM f), ARRAY[]::integer[]),
        coalesce((SELECT array_agg(result ORDER BY position) FROM f), ARRAY[]::text[])
    )
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
BEGIN
    UPDATE horsies_tasks
    SET status = 'RUNNING', claimed_at = now(), claimed_by_worker_id = 'worker-1', started_at = now(),
        claim_expires_at = NULL, worker_pid = 1, worker_hostname = 'host', worker_process_name = 'proc'
    WHERE id = p_task_id;
    PERFORM wft_check(FOUND, 'phase-1 fixture finds task ' || p_task_id);
    PERFORM horsies_move_task_to_history(
        p_task_id,
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
