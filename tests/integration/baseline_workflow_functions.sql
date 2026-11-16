-- Cases for the workflow engine functions of 0005_workflow_functions.sql, on a fresh install with the fixtures of
-- baseline_workflow_fixtures.sql. The caller runs both files in one transaction and rolls it back. Each case is one
-- DO block: a pass case checks the rows that the function wrote; a refusal case checks the SQLSTATE and that nothing
-- was written. A failed check raises with the case name. The two-session lock-order cases are in
-- test_baseline_workflow_functions.py.

-- Workflow start -----------------------------------------------------------------------------------------------------

DO $$
DECLARE
    v_wf uuid := '00000000-0000-7000-8000-000000000101';
    v_row horsies_workflow_step_row;
    v_task horsies_tasks;
BEGIN
    SELECT * INTO v_row FROM wft_start1(v_wf, 'fail', '[
        {"idx":0,"input":"{\"x\":1}"},
        {"idx":1,"deps":[0],"args_from":{"a":0}},
        {"idx":2,"good_until":"2031-01-01T00:00:00Z"}
    ]') WHERE row_kind = 'OUTCOME';
    PERFORM wft_check(v_row.disposition = 'STARTED' AND v_row.workflow_status = 'RUNNING', 'start: STARTED');
    PERFORM wft_check((wft_workflow(v_wf)).sent_at = '2030-01-01 00:00:00+00', 'start: sent_at is the call time');
    PERFORM wft_check((wft_workflow(v_wf)).root_workflow_id = v_wf AND (wft_workflow(v_wf)).depth = 0,
                      'start: top-level root id and depth');
    PERFORM wft_check((wft_node(v_wf, 0)).status = 'ENQUEUED' AND (wft_node(v_wf, 0)).started_at IS NOT NULL
                      AND (wft_node(v_wf, 2)).status = 'ENQUEUED', 'start: roots ENQUEUED with started_at');
    PERFORM wft_check((wft_node(v_wf, 1)).status = 'PENDING' AND (wft_node(v_wf, 1)).task_id IS NULL,
                      'start: dependent PENDING with no task');
    SELECT * INTO v_task FROM horsies_tasks WHERE id = wft_task_of(v_wf, 0);
    PERFORM wft_check(
        v_task.status = 'PENDING' AND v_task.is_workflow_task AND v_task.command_fingerprint IS NULL
        AND v_task.command_fingerprint_version IS NULL AND NOT v_task.retain_rerun_input
        AND v_task.sent_at = '2030-01-01 00:00:00+00' AND v_task.max_retries = 2
        AND v_task.execution = 'SHARED_ASYNC' AND v_task.retention_class_key = 'forever'
        AND v_task.task_options = '{"retry":"none"}' AND v_task.queue_name = 'default' AND v_task.priority = 50
        AND v_task.retry_count = 0 AND v_task.good_until IS NULL,
        'start: minted root task row carries the node facts and no fingerprint'
    );
    PERFORM wft_check(
        v_task.input = '{"x":1,"__h_workflow_meta__":{"workflow_id":"' || v_wf || '","task_index":0,"task_name":"t0"}}',
        'start: root input is the static input with the workflow meta'
    );
    PERFORM wft_check((SELECT good_until FROM horsies_tasks WHERE id = wft_task_of(v_wf, 2)) = '2031-01-01T00:00:00Z',
                      'start: good_until comes from the node');
    PERFORM wft_check(horsies_uuid7_birth(v_task.id) IS NOT NULL, 'start: minted id is a UUIDv7');
END $$;

DO $$
DECLARE
    v_wf uuid := '00000000-0000-7000-8000-000000000101';
    v_row horsies_workflow_step_row;
    v_nodes bigint := (SELECT count(*) FROM horsies_workflow_tasks);
    v_tasks bigint := (SELECT count(*) FROM horsies_tasks);
BEGIN
    SELECT * INTO v_row FROM wft_start1(v_wf, 'fail', '[{"idx":0}]') WHERE row_kind = 'OUTCOME';
    PERFORM wft_check(v_row.disposition = 'EXISTS' AND v_row.detail = 'wf', 'start: repeated id returns EXISTS');
    PERFORM wft_check((SELECT count(*) FROM horsies_workflow_tasks) = v_nodes
                      AND (SELECT count(*) FROM horsies_tasks) = v_tasks, 'start: repeated id writes nothing');
    SELECT * INTO v_row FROM wft_start(
        jsonb_build_array(jsonb_build_object('id', v_wf, 'name', 'other', 'on_error', 'fail')), '[{"idx":0}]'
    ) WHERE row_kind = 'OUTCOME';
    PERFORM wft_check(v_row.disposition = 'EXISTS' AND v_row.detail = 'wf',
                      'start: repeated id with another name returns the stored name');
END $$;

DO $$
BEGIN
    PERFORM wft_refused($q$
        SELECT * FROM horsies_workflow_start(
            ARRAY['00000000-0000-7000-8000-000000000102'::uuid], ARRAY['wf'], ARRAY[NULL::text], ARRAY['fail'],
            ARRAY[NULL::integer], ARRAY[NULL::text], ARRAY[NULL::uuid], ARRAY[NULL::integer], ARRAY[0], now(),
            ARRAY['00000000-0000-7000-8000-000000000102'::uuid], ARRAY[0, 1], ARRAY['n0'], ARRAY['t0'], ARRAY['{}'],
            ARRAY['default'], ARRAY[50], ARRAY['[]'], ARRAY[NULL::text], ARRAY[NULL::text], ARRAY[FALSE],
            ARRAY['all'], ARRAY[NULL::integer], ARRAY[NULL::text], ARRAY[0], ARRAY['ISOLATED'],
            ARRAY[NULL::timestamptz], ARRAY['forever'], ARRAY[FALSE], ARRAY[NULL::text], ARRAY[NULL::text],
            ARRAY[NULL::text], ARRAY[]::uuid[], ARRAY[]::integer[], ARRAY[]::text[])
    $q$, '22023', 'start refuses node arrays of different lengths');
    PERFORM wft_refused($q$
        SELECT * FROM wft_start1('00000000-0000-7000-8000-000000000102', 'fail', '[{"idx":0},{"idx":0}]')
    $q$, '22023', 'start refuses a repeated node index');
    PERFORM wft_refused($q$
        SELECT * FROM wft_start('[{"id":"00000000-0000-7000-8000-000000000102","on_error":"fail"},
                                  {"id":"00000000-0000-7000-8000-000000000103","on_error":"fail",
                                   "parent":"00000000-0000-7000-8000-000000000102","parent_index":0,"depth":2}]',
                                '[{"idx":0,"sub":true}]')
    $q$, '22023', 'start refuses a child depth that is not its parent''s plus one');
    PERFORM wft_refused($q$
        SELECT * FROM wft_start('[{"id":"00000000-0000-7000-8000-000000000102","on_error":"fail"}]',
                                '[{"idx":0},{"idx":1,"deps":[0]}]', '[{"idx":1,"result":"{}"}]')
    $q$, '22023', 'start refuses a failed record that names no root');
    PERFORM wft_refused($q$
        SELECT * FROM wft_start1('00000000-0000-7000-8000-000000000102', 'never', '[{"idx":0}]')
    $q$, '23514', 'start refuses an unknown on_error (table CHECK)');
    PERFORM wft_check(NOT EXISTS (SELECT 1 FROM horsies_workflows WHERE id = '00000000-0000-7000-8000-000000000102'),
                      'start refusals write nothing');
END $$;

DO $$
DECLARE
    v_wf uuid := '00000000-0000-7000-8000-000000000104';
BEGIN
    PERFORM * FROM wft_start1(v_wf, 'fail', '[{"idx":0},{"idx":1,"sub":true,"params":"GIVEN"},{"idx":2,"sub":true}]');
    PERFORM wft_check((wft_node(v_wf, 1)).sub_workflow_params = 'GIVEN'
                      AND (wft_node(v_wf, 2)).sub_workflow_params = 'NOT_GIVEN'
                      AND (wft_node(v_wf, 0)).sub_workflow_params IS NULL,
                      'start: sub_workflow_params is stored as passed on sub-workflow nodes, NULL on task nodes');
END $$;

DO $$
DECLARE
    v_wf uuid := '00000000-0000-7000-8000-000000000111';
    v_child uuid := '00000000-0000-7000-8000-000000000112';
    v_row horsies_workflow_step_row;
BEGIN
    SELECT * INTO v_row FROM wft_start(
        jsonb_build_array(
            jsonb_build_object('id', v_wf, 'on_error', 'fail'),
            jsonb_build_object('id', v_child, 'on_error', 'fail', 'parent', v_wf, 'parent_index', 0, 'depth', 1)),
        jsonb_build_array(
            jsonb_build_object('idx', 0, 'sub', true),
            jsonb_build_object('idx', 1, 'deps', jsonb_build_array(0)),
            jsonb_build_object('wf', v_child, 'idx', 0),
            jsonb_build_object('wf', v_child, 'idx', 1))
    ) WHERE row_kind = 'OUTCOME';
    PERFORM wft_check(v_row.disposition = 'STARTED', 'start with a child: STARTED');
    PERFORM wft_check((wft_node(v_wf, 0)).status = 'RUNNING' AND (wft_node(v_wf, 0)).sub_workflow_id = v_child
                      AND (wft_node(v_wf, 0)).started_at IS NOT NULL, 'start with a child: parent node linked');
    PERFORM wft_check((wft_workflow(v_child)).status = 'RUNNING' AND (wft_workflow(v_child)).root_workflow_id = v_wf
                      AND (wft_workflow(v_child)).depth = 1, 'start with a child: child row');
    PERFORM wft_check(wft_task_of(v_child, 0) IS NOT NULL AND wft_task_of(v_child, 1) IS NOT NULL,
                      'start with a child: child roots minted');
END $$;

DO $$
DECLARE
    v_wf uuid := '00000000-0000-7000-8000-000000000121';
    v_error text := horsies_task_error_envelope('WORKFLOW_ENQUEUE_FAILED', 'build failed', NULL, NULL, NULL);
BEGIN
    PERFORM * FROM wft_start(
        jsonb_build_array(jsonb_build_object('id', v_wf, 'on_error', 'fail')),
        '[{"idx":0,"sub":true},{"idx":1},{"idx":2,"deps":[0]},{"idx":3,"deps":[0],"allow":true}]',
        jsonb_build_array(jsonb_build_object('idx', 0, 'result', v_error))
    );
    PERFORM wft_check((wft_node(v_wf, 0)).status = 'FAILED' AND (wft_node(v_wf, 0)).result = v_error
                      AND (wft_node(v_wf, 0)).completed_at IS NOT NULL, 'start failed root (fail): node FAILED');
    PERFORM wft_check((wft_workflow(v_wf)).status = 'RUNNING'
                      AND (wft_workflow(v_wf)).error = (v_error::json -> 'err')::text,
                      'start failed root (fail): workflow error is the node error');
    PERFORM wft_check((wft_node(v_wf, 2)).status = 'SKIPPED' AND (wft_node(v_wf, 2)).completed_at IS NOT NULL,
                      'start failed root (fail): join all dependent SKIPPED');
    PERFORM wft_check((wft_node(v_wf, 3)).status = 'ENQUEUED' AND wft_task_of(v_wf, 3) IS NOT NULL,
                      'start failed root (fail): allow_failed_deps dependent enqueued');
    PERFORM wft_check(wft_task_of(v_wf, 1) IS NOT NULL, 'start failed root (fail): other root enqueued');
END $$;

DO $$
DECLARE
    v_wf uuid := '00000000-0000-7000-8000-000000000131';
    v_error text := horsies_task_error_envelope('WORKFLOW_ENQUEUE_FAILED', 'build failed', NULL, NULL, NULL);
BEGIN
    PERFORM * FROM wft_start(
        jsonb_build_array(jsonb_build_object('id', v_wf, 'on_error', 'pause')),
        '[{"idx":0,"sub":true},{"idx":1},{"idx":2}]',
        jsonb_build_array(jsonb_build_object('idx', 0, 'result', v_error))
    );
    PERFORM wft_check((wft_workflow(v_wf)).status = 'PAUSED'
                      AND (wft_workflow(v_wf)).error = (v_error::json -> 'err')::text,
                      'start failed root (pause): workflow PAUSED with the node error');
    PERFORM wft_check((wft_node(v_wf, 1)).status = 'READY' AND (wft_node(v_wf, 1)).task_id IS NULL
                      AND (wft_node(v_wf, 1)).started_at IS NULL AND (wft_node(v_wf, 2)).status = 'READY',
                      'start failed root (pause): the pause gate keeps the other roots READY');
    PERFORM wft_check(NOT EXISTS (SELECT 1 FROM horsies_tasks t JOIN horsies_workflow_tasks n ON n.task_id = t.id
                                  WHERE n.workflow_id = v_wf), 'start failed root (pause): no task row');
END $$;

DO $$
DECLARE
    v_wf uuid := '00000000-0000-7000-8000-000000000141';
    v_child uuid := '00000000-0000-7000-8000-000000000142';
    v_error text := horsies_task_error_envelope('WORKFLOW_ENQUEUE_FAILED', 'grandchild build failed', NULL, NULL, NULL);
BEGIN
    PERFORM * FROM wft_start(
        jsonb_build_array(
            jsonb_build_object('id', v_wf, 'on_error', 'fail'),
            jsonb_build_object('id', v_child, 'on_error', 'fail', 'parent', v_wf, 'parent_index', 0, 'depth', 1)),
        jsonb_build_array(
            jsonb_build_object('idx', 0, 'sub', true),
            jsonb_build_object('idx', 1, 'deps', jsonb_build_array(0), 'allow', true),
            jsonb_build_object('wf', v_child, 'idx', 0, 'sub', true)),
        jsonb_build_array(jsonb_build_object('wf', v_child, 'idx', 0, 'result', v_error))
    );
    PERFORM wft_check((wft_workflow(v_child)).status = 'FAILED' AND (wft_workflow(v_child)).completed_at IS NOT NULL,
                      'start, child that ends at once: child FAILED');
    PERFORM wft_check((wft_node(v_wf, 0)).status = 'FAILED'
                      AND ((wft_node(v_wf, 0)).result::json -> 'err' -> 'error_code' ->> '__builtin_task_code__')
                          = 'SUBWORKFLOW_FAILED'
                      AND ((wft_node(v_wf, 0)).result::json -> 'err' ->> 'sub_workflow_id') = v_child::text
                      AND ((wft_node(v_wf, 0)).sub_workflow_summary::json ->> 'failed_tasks') = '1',
                      'start, child that ends at once: parent node FAILED with SUBWORKFLOW_FAILED and summary');
    PERFORM wft_check((wft_node(v_wf, 1)).status = 'ENQUEUED' AND wft_task_of(v_wf, 1) IS NOT NULL,
                      'start, child that ends at once: parent dependent promoted in the same statement');
END $$;

-- Phase 2: promotion, task minting, input splice ----------------------------------------------------------------------

DO $$
DECLARE
    v_wf uuid := '00000000-0000-7000-8000-000000000201';
    v_rows bigint;
    v_input text;
BEGIN
    PERFORM * FROM wft_start1(v_wf, 'fail', '[
        {"idx":0},{"idx":1},
        {"idx":2,"deps":[0,1],"args_from":{"left":0,"right":1},"input":"{\"k\":\"v\"}"}
    ]');
    PERFORM wft_check(wft_finish(v_wf, 0, wft_ok('{"n":1}')) = 'APPLIED', 'phase 2: first dependency APPLIED');
    PERFORM wft_check((wft_node(v_wf, 0)).status = 'COMPLETED' AND (wft_node(v_wf, 0)).result = wft_ok('{"n":1}')
                      AND (wft_node(v_wf, 0)).completed_at IS NOT NULL, 'phase 2: node written from history');
    PERFORM wft_check(NOT EXISTS (SELECT 1 FROM horsies_workflow_phase2_pending
                                  WHERE workflow_id = v_wf), 'phase 2: pending row deleted');
    PERFORM wft_check((wft_node(v_wf, 2)).status = 'PENDING', 'phase 2: join all waits for every dependency');
    PERFORM wft_check(wft_finish(v_wf, 1, wft_err('USER')) = 'APPLIED', 'phase 2: second dependency APPLIED');
    PERFORM wft_check((wft_node(v_wf, 2)).status = 'SKIPPED', 'phase 2: join all with a FAILED dependency SKIPPED');
    PERFORM wft_check((wft_workflow(v_wf)).status = 'FAILED'
                      AND (wft_workflow(v_wf)).error = ((wft_err('USER'))::json -> 'err')::text
                      AND (wft_workflow(v_wf)).result IS NULL AND (wft_workflow(v_wf)).completed_at IS NOT NULL,
                      'phase 2: workflow FAILED with the first error, no result');
    SELECT count(*) INTO v_rows FROM horsies_workflow_node_finish(wft_task_of(v_wf, 1), TRUE, 'READ')
    WHERE row_kind = 'OUTCOME' AND disposition = 'ALREADY_APPLIED';
    PERFORM wft_check(v_rows = 1, 'phase 2: a replay is ALREADY_APPLIED');
    SELECT count(*) INTO v_rows FROM horsies_workflow_node_finish('00000000-0000-7000-8000-0000000002ff', TRUE, 'READ')
    WHERE row_kind = 'OUTCOME' AND disposition = 'PENDING_ABSENT';
    PERFORM wft_check(v_rows = 1, 'phase 2: an unknown task is PENDING_ABSENT');
END $$;

DO $$
DECLARE
    v_wf uuid := '00000000-0000-7000-8000-000000000211';
    v_input text;
    v_rows bigint;
BEGIN
    PERFORM * FROM wft_start1(v_wf, 'fail', '[
        {"idx":0},{"idx":1},{"idx":2},
        {"idx":3,"deps":[0,1,2],"args_from":{"a":0,"b":1,"c":2},"ctx":["n0","n2"],"allow":true,
         "input":"{\"k\":\"v\"}"}
    ]');
    PERFORM wft_finish(v_wf, 0, wft_ok('[1,2]'));
    PERFORM wft_finish(v_wf, 1, wft_err('USER'));
    PERFORM wft_finish(v_wf, 2, wft_ok('"text"'));
    PERFORM wft_check((wft_node(v_wf, 3)).status = 'ENQUEUED' AND wft_task_of(v_wf, 3) IS NOT NULL,
                      'phase 2: allow_failed_deps dependent enqueued with a task row');
    SELECT input INTO v_input FROM horsies_tasks WHERE id = wft_task_of(v_wf, 3);
    PERFORM wft_check(v_input::jsonb ->> 'k' = 'v', 'input: static member kept');
    PERFORM wft_check(
        position('"a":{"__h_taskresult_envelope__":true,"source_task_name":"t0","source_definition_key":null,"inner":'
                 || wft_ok('[1,2]') || '}' IN v_input) > 0,
        'input: args_from envelope copies the stored result byte for byte'
    );
    PERFORM wft_check(v_input::jsonb -> 'b' -> 'inner' = (wft_err('USER'))::jsonb,
                      'input: a FAILED source gives its stored error');
    PERFORM wft_check(v_input::jsonb -> '__h_workflow_ctx__' -> 'results_by_id' -> 'n0' = (wft_ok('[1,2]'))::jsonb
                      AND v_input::jsonb -> '__h_workflow_ctx__' -> 'task_name_by_id' ->> 'n2' = 't2'
                      AND NOT (v_input::jsonb -> '__h_workflow_ctx__' -> 'results_by_id') ? 'n1'
                      AND v_input::jsonb -> '__h_workflow_ctx__' ->> 'task_index' = '3',
                      'input: workflow context over the context nodes only');
    PERFORM wft_check(v_input::jsonb -> '__h_workflow_meta__' ->> 'workflow_id' = v_wf::text
                      AND v_input::jsonb -> '__h_workflow_meta__' ->> 'task_name' = 't3',
                      'input: workflow meta');
    SELECT count(*) INTO v_rows FROM jsonb_object_keys(v_input::jsonb);
    PERFORM wft_check(v_rows = 6, 'input: static, three args_from, context and meta members');
END $$;

DO $$
DECLARE
    v_wf uuid := '00000000-0000-7000-8000-000000000221';
    v_input text;
BEGIN
    PERFORM * FROM wft_start1(v_wf, 'fail', '[
        {"idx":0},{"idx":1},
        {"idx":2,"deps":[0,1],"join":"any","args_from":{"a":0,"b":1}},
        {"idx":3,"deps":[0,1],"join":"quorum","min":2}
    ]');
    PERFORM wft_finish(v_wf, 0, wft_err('USER'));
    PERFORM wft_check((wft_node(v_wf, 2)).status = 'PENDING', 'join any: a FAILED dependency does not meet it');
    PERFORM wft_check((wft_node(v_wf, 3)).status = 'SKIPPED', 'join quorum: an unreachable threshold SKIPPED');
    PERFORM wft_finish(v_wf, 1, wft_ok('7'));
    PERFORM wft_check((wft_node(v_wf, 2)).status = 'ENQUEUED', 'join any: met on the first COMPLETED dependency');
    SELECT input INTO v_input FROM horsies_tasks WHERE id = wft_task_of(v_wf, 2);
    PERFORM wft_check(v_input::jsonb -> 'b' -> 'inner' = (wft_ok('7'))::jsonb AND v_input::jsonb ? 'a',
                      'join any: terminal sources are spliced');
END $$;

DO $$
DECLARE
    v_wf uuid := '00000000-0000-7000-8000-000000000231';
    v_input text;
BEGIN
    PERFORM * FROM wft_start1(v_wf, 'fail', '[
        {"idx":0},{"idx":1},
        {"idx":2,"deps":[0,1],"join":"any","args_from":{"a":0,"b":1}}
    ]');
    PERFORM wft_finish(v_wf, 0, wft_ok('1'));
    SELECT input INTO v_input FROM horsies_tasks WHERE id = wft_task_of(v_wf, 2);
    PERFORM wft_check(v_input::jsonb ? 'a' AND NOT v_input::jsonb ? 'b',
                      'join any: a source that is not terminal gives no member');
END $$;

DO $$
DECLARE
    v_wf uuid := '00000000-0000-7000-8000-000000000241';
    v_input text;
BEGIN
    PERFORM * FROM wft_start1(v_wf, 'fail', '[
        {"idx":0},
        {"idx":1,"deps":[0],"join":"any"},
        {"idx":2,"deps":[1],"args_from":{"s":1},"allow":true},
        {"idx":3,"deps":[2]}
    ]');
    PERFORM wft_finish(v_wf, 0, wft_err('USER'));
    PERFORM wft_check((wft_node(v_wf, 1)).status = 'SKIPPED', 'skip cascade: level 1 SKIPPED');
    PERFORM wft_check((wft_node(v_wf, 2)).status = 'ENQUEUED', 'skip cascade: level 2 allow_failed_deps enqueued');
    SELECT input INTO v_input FROM horsies_tasks WHERE id = wft_task_of(v_wf, 2);
    PERFORM wft_check(v_input::jsonb -> 's' -> 'inner' -> 'err' -> 'error_code' ->> '__builtin_task_code__'
                      = 'UPSTREAM_SKIPPED'
                      AND v_input::jsonb -> 's' -> 'inner' -> 'err' -> 'data' ->> 'dependency_index' = '1',
                      'skip cascade: a SKIPPED source gives UPSTREAM_SKIPPED');
    PERFORM wft_check((wft_node(v_wf, 3)).status = 'PENDING', 'skip cascade: level 3 waits');
END $$;

DO $$
DECLARE
    v_wf uuid := '00000000-0000-7000-8000-000000000251';
BEGIN
    PERFORM * FROM wft_start1(v_wf, 'fail', '[
        {"idx":0},
        {"idx":1,"deps":[0],"args_from":{"k":0},"input":"{\"k\":1}"},
        {"idx":2,"deps":[0]},
        {"idx":3,"deps":[1],"allow":true}
    ]');
    PERFORM wft_finish(v_wf, 0, wft_ok('1'));
    PERFORM wft_check((wft_node(v_wf, 1)).status = 'FAILED'
                      AND ((wft_node(v_wf, 1)).result::json -> 'err' -> 'error_code' ->> '__builtin_task_code__')
                          = 'WORKFLOW_ENQUEUE_FAILED'
                      AND (wft_node(v_wf, 1)).task_id IS NULL,
                      'args_from key conflict (fail): node FAILED alone with no task');
    PERFORM wft_check((wft_node(v_wf, 2)).status = 'ENQUEUED' AND wft_task_of(v_wf, 2) IS NOT NULL,
                      'args_from key conflict (fail): sibling enqueued');
    PERFORM wft_check((wft_node(v_wf, 3)).status = 'ENQUEUED',
                      'args_from key conflict (fail): the failed node is a source in the same step');
    PERFORM wft_check(((wft_workflow(v_wf)).error::json -> 'error_code' ->> '__builtin_task_code__')
                      = 'WORKFLOW_ENQUEUE_FAILED', 'args_from key conflict (fail): workflow error');
END $$;

DO $$
DECLARE
    v_wf uuid := '00000000-0000-7000-8000-000000000261';
BEGIN
    PERFORM * FROM wft_start1(v_wf, 'pause', '[
        {"idx":0},
        {"idx":1,"deps":[0],"args_from":{"k":0},"input":"{\"k\":1}"},
        {"idx":2,"deps":[0],"args_from":{"k":0},"input":"{\"k\":1}"},
        {"idx":3,"deps":[0]}
    ]');
    PERFORM wft_finish(v_wf, 0, wft_ok('1'));
    PERFORM wft_check((wft_workflow(v_wf)).status = 'PAUSED', 'build failure (pause): workflow PAUSED');
    PERFORM wft_check((wft_node(v_wf, 1)).status = 'FAILED', 'build failure (pause): the lowest index FAILED');
    PERFORM wft_check((wft_node(v_wf, 2)).status = 'READY' AND (wft_node(v_wf, 3)).status = 'READY'
                      AND (wft_node(v_wf, 3)).task_id IS NULL AND (wft_node(v_wf, 3)).started_at IS NULL,
                      'build failure (pause): the other nodes of the step back to READY with no task');
END $$;

DO $$
DECLARE
    v_wf uuid := '00000000-0000-7000-8000-000000000271';
    v_rows bigint;
    v_sources bigint;
BEGIN
    PERFORM * FROM wft_start1(v_wf, 'fail', '[
        {"idx":0},{"idx":1,"deps":[0],"sub":true,"args_from":{"x":0},"input":"{\"p\":2}"}
    ]');
    PERFORM wft_phase1(wft_task_of(v_wf, 0), wft_ok('5'));
    SELECT count(*) FILTER (WHERE row_kind = 'CHILD_START' AND task_index = 1 AND node_input = '{"p":2}'
                            AND parent_depth = 0 AND root_workflow_id = v_wf AND sub_definition_key = 'child.key'),
           count(*) FILTER (WHERE row_kind = 'SOURCE' AND task_index = 1 AND source_task_index = 0
                            AND source_result = wft_ok('5') AND source_status = 'COMPLETED')
    INTO v_rows, v_sources
    FROM horsies_workflow_node_finish(wft_task_of(v_wf, 0), FALSE, 'READ');
    PERFORM wft_check(v_rows = 1 AND v_sources = 1, 'sub-workflow promotion: CHILD_START and SOURCE rows owed');
    PERFORM wft_check((wft_node(v_wf, 1)).status = 'ENQUEUED' AND (wft_node(v_wf, 1)).sub_workflow_id IS NULL,
                      'sub-workflow promotion: node ENQUEUED with no child');
END $$;

DO $$
DECLARE
    v_parent uuid;
    v_wf uuid := '00000000-0000-7000-8000-000000000281';
    v_depth integer;
BEGIN
    -- A chain of 32 workflows above v_wf, so v_wf has depth 32. v_wf is PAUSED with a READY sub-workflow node; the
    -- resume enqueues it.
    v_parent := NULL;
    FOR v_depth IN 0..31 LOOP
        INSERT INTO horsies_workflows (id, name, definition_key, status, on_error, output_task_index, success_policy,
            result, error, parent_workflow_id, parent_task_index, root_workflow_id, depth, sent_at, created_at,
            started_at, completed_at, updated_at)
        VALUES (('00000000-0000-7000-8000-0000000003' || lpad(to_hex(v_depth), 2, '0'))::uuid, 'deep', NULL,
            'RUNNING', 'fail', NULL, NULL, NULL, NULL, v_parent, CASE WHEN v_parent IS NOT NULL THEN 0 END,
            '00000000-0000-7000-8000-000000000300', v_depth, now(), now(), now(), NULL, now());
        v_parent := ('00000000-0000-7000-8000-0000000003' || lpad(to_hex(v_depth), 2, '0'))::uuid;
    END LOOP;
    INSERT INTO horsies_workflows (id, name, definition_key, status, on_error, output_task_index, success_policy,
        result, error, parent_workflow_id, parent_task_index, root_workflow_id, depth, sent_at, created_at,
        started_at, completed_at, updated_at)
    VALUES (v_wf, 'deep', NULL, 'PAUSED', 'fail', NULL, NULL, NULL, NULL, v_parent, 0,
        '00000000-0000-7000-8000-000000000300', 32, now(), now(), now(), NULL, now());
    INSERT INTO horsies_workflow_tasks (id, workflow_id, task_index, node_id, task_name, input, queue_name, priority,
        dependencies, args_from, workflow_ctx_from, allow_failed_deps, join_type, min_success, task_options,
        max_retries, execution, good_until, retention_class_key, downstream_subworkflow, status, task_id,
        is_subworkflow, sub_workflow_id, sub_workflow_name, sub_definition_key, sub_workflow_params,
        sub_workflow_summary, result, error, created_at, started_at, completed_at)
    VALUES (horsies_uuidv7(), v_wf, 0, 'n0', 'sub', '{}', 'default', 50, '{}', NULL, NULL, FALSE, 'all', NULL,
        NULL, NULL, NULL, NULL, NULL, 'NONE', 'READY', NULL,
        TRUE, NULL, 'child.wf', 'child.key', 'NOT_GIVEN',
        NULL, NULL, NULL, now(), NULL, NULL);
    PERFORM wft_check((SELECT advance_child_workflow_id FROM horsies_workflow_resume(v_wf) WHERE row_kind = 'OUTCOME')
                      = v_wf, 'depth limit: the resume that ends a child names it for the parent advance');
    PERFORM wft_check((wft_node(v_wf, 0)).status = 'FAILED'
                      AND ((wft_node(v_wf, 0)).result::json -> 'err' -> 'error_code' ->> '__builtin_task_code__')
                          = 'SUBWORKFLOW_DEPTH_LIMIT_REACHED',
                      'depth limit: a sub-workflow node at depth 32 fails with SUBWORKFLOW_DEPTH_LIMIT_REACHED');
    PERFORM wft_check((wft_workflow(v_wf)).status = 'FAILED', 'depth limit: the workflow fails closed');
END $$;

-- Phase 2: guards and completion ---------------------------------------------------------------------------------

DO $$
DECLARE
    v_wf uuid := '00000000-0000-7000-8000-000000000401';
BEGIN
    PERFORM * FROM wft_start1(v_wf, 'fail', '[{"idx":0},{"idx":1,"deps":[0]}]');
    UPDATE horsies_workflows SET status = 'PAUSED' WHERE id = v_wf;
    PERFORM wft_check(wft_finish(v_wf, 0, wft_ok('1')) = 'APPLIED', 'paused workflow: node APPLIED');
    PERFORM wft_check((wft_node(v_wf, 0)).status = 'COMPLETED' AND (wft_node(v_wf, 1)).status = 'PENDING',
                      'paused workflow: node written, nothing promoted');
END $$;

DO $$
DECLARE
    v_wf uuid := '00000000-0000-7000-8000-000000000411';
BEGIN
    PERFORM * FROM wft_start1(v_wf, 'fail', '[{"idx":0},{"idx":1,"deps":[0]}]');
    UPDATE horsies_workflows SET status = 'CANCELLED', completed_at = now() WHERE id = v_wf;
    PERFORM wft_check(wft_finish(v_wf, 0, wft_ok('1')) = 'APPLIED_WORKFLOW_ENDED', 'ended workflow: disposition');
    PERFORM wft_check((wft_node(v_wf, 0)).status = 'COMPLETED' AND (wft_node(v_wf, 1)).status = 'PENDING'
                      AND (wft_workflow(v_wf)).status = 'CANCELLED',
                      'ended workflow: node written, nothing promoted, status kept');
END $$;

DO $$
DECLARE
    v_wf uuid := '00000000-0000-7000-8000-000000000421';
    v_task uuid;
    v_row horsies_workflow_step_row;
BEGIN
    PERFORM * FROM wft_start1(v_wf, 'fail', '[{"idx":0}]');
    v_task := wft_task_of(v_wf, 0);
    PERFORM wft_phase1(v_task, wft_ok('1'));
    UPDATE horsies_workflow_phase2_pending SET result_digest = sha256('other'::bytea) WHERE task_id = v_task;
    SELECT * INTO v_row FROM horsies_workflow_node_finish(v_task, TRUE, 'READ') WHERE row_kind = 'OUTCOME';
    PERFORM wft_check(v_row.disposition = 'SOURCE_DIGEST_MISMATCH', 'retaining: digest mismatch');
    PERFORM wft_check((wft_node(v_wf, 0)).status = 'ENQUEUED'
                      AND EXISTS (SELECT 1 FROM horsies_workflow_phase2_pending WHERE task_id = v_task),
                      'retaining: node not written, record kept');
END $$;

DO $$
DECLARE
    v_wf uuid := '00000000-0000-7000-8000-000000000431';
    v_result json;
BEGIN
    PERFORM * FROM wft_start1(v_wf, 'fail', '[{"idx":0},{"idx":1,"deps":[0]},{"idx":2,"sub":false}]');
    PERFORM wft_finish(v_wf, 0, wft_ok('1'));
    PERFORM wft_finish(v_wf, 2, wft_ok('{"z":3}'));
    PERFORM wft_check((wft_workflow(v_wf)).status = 'RUNNING', 'completion: a live node keeps the workflow RUNNING');
    PERFORM wft_finish(v_wf, 1, wft_ok('2'));
    v_result := (wft_workflow(v_wf)).result::json;
    PERFORM wft_check((wft_workflow(v_wf)).status = 'COMPLETED' AND (wft_workflow(v_wf)).completed_at IS NOT NULL,
                      'completion: COMPLETED');
    PERFORM wft_check((v_result ->> '__h_outputless_terminals__') = 'true'
                      AND (v_result -> 'ok' -> 'results_by_id' -> 'n1')::jsonb = (wft_ok('2'))::jsonb
                      AND (v_result -> 'ok' -> 'results_by_id' -> 'n2')::jsonb = (wft_ok('{"z":3}'))::jsonb
                      AND (v_result -> 'ok' -> 'results_by_id' -> 'n0') IS NULL
                      AND (v_result -> 'ok' -> 'task_name_by_id' ->> 'n1') = 't1',
                      'completion: the outputless map holds the terminal nodes only');
END $$;

DO $$
DECLARE
    v_wf uuid := '00000000-0000-7000-8000-000000000441';
    v_skip uuid := '00000000-0000-7000-8000-000000000442';
BEGIN
    PERFORM * FROM wft_start1(v_wf, 'fail', '[{"idx":0},{"idx":1,"deps":[0]}]', 1);
    PERFORM wft_finish(v_wf, 0, wft_ok('1'));
    PERFORM wft_finish(v_wf, 1, wft_ok('"out"'));
    PERFORM wft_check((wft_workflow(v_wf)).result = wft_ok('"out"'), 'output node: the stored text, copied');
    PERFORM * FROM wft_start1(v_skip, 'fail', '[{"idx":0},{"idx":1,"deps":[0],"join":"any"},{"idx":2,"deps":[1],"allow":true}]',
                              1, '{"cases":[{"required_indices":[2]}]}');
    PERFORM wft_finish(v_skip, 0, wft_err('USER'));
    PERFORM wft_finish(v_skip, 2, wft_ok('3'));
    PERFORM wft_check((wft_workflow(v_skip)).status = 'COMPLETED'
                      AND ((wft_workflow(v_skip)).result::json -> 'err' -> 'error_code' ->> '__builtin_task_code__')
                          = 'WORKFLOW_OUTPUT_SKIPPED',
                      'output node SKIPPED under a met policy: WORKFLOW_OUTPUT_SKIPPED');
END $$;

DO $$
DECLARE
    v_met uuid := '00000000-0000-7000-8000-000000000451';
    v_unmet uuid := '00000000-0000-7000-8000-000000000452';
    v_required uuid := '00000000-0000-7000-8000-000000000453';
BEGIN
    PERFORM * FROM wft_start1(v_met, 'fail', '[{"idx":0},{"idx":1}]', NULL,
                              '{"cases":[{"required_indices":[0]}],"optional_indices":[1]}');
    PERFORM wft_finish(v_met, 1, wft_err('USER'));
    PERFORM wft_finish(v_met, 0, wft_ok('1'));
    PERFORM wft_check((wft_workflow(v_met)).status = 'COMPLETED', 'policy: a met case completes despite a failure');
    PERFORM * FROM wft_start1(v_unmet, 'fail', '[{"idx":0},{"idx":1,"deps":[0],"join":"any"}]', NULL,
                              '{"cases":[{"required_indices":[1]}]}');
    PERFORM wft_finish(v_unmet, 0, wft_err('USER'));
    PERFORM wft_check((wft_workflow(v_unmet)).status = 'FAILED'
                      AND ((wft_workflow(v_unmet)).error::json -> 'error_code' ->> '__builtin_task_code__')
                          = 'WORKFLOW_SUCCESS_CASE_NOT_MET',
                      'policy: no failed required node gives WORKFLOW_SUCCESS_CASE_NOT_MET');
    PERFORM * FROM wft_start1(v_required, 'fail', '[{"idx":0},{"idx":1}]', NULL,
                              '{"cases":[{"required_indices":[1]}]}');
    PERFORM wft_finish(v_required, 0, wft_err('FIRST'));
    PERFORM wft_finish(v_required, 1, wft_err('REQUIRED'));
    PERFORM wft_check((wft_workflow(v_required)).status = 'FAILED'
                      AND ((wft_workflow(v_required)).error::json ->> 'error_code') = 'REQUIRED',
                      'policy: the first FAILED required node gives the error');
END $$;

DO $$
DECLARE
    v_wf uuid := '00000000-0000-7000-8000-000000000461';
    v_child uuid := '00000000-0000-7000-8000-000000000462';
    v_grandchild uuid := '00000000-0000-7000-8000-000000000463';
BEGIN
    PERFORM * FROM wft_start(
        jsonb_build_array(
            jsonb_build_object('id', v_wf, 'on_error', 'pause'),
            jsonb_build_object('id', v_child, 'on_error', 'fail', 'parent', v_wf, 'parent_index', 1, 'depth', 1)),
        jsonb_build_array(
            jsonb_build_object('idx', 0), jsonb_build_object('idx', 1, 'sub', true),
            jsonb_build_object('idx', 2, 'deps', jsonb_build_array(0)),
            jsonb_build_object('wf', v_child, 'idx', 0))
    );
    PERFORM wft_finish(v_wf, 0, wft_err('USER'));
    PERFORM wft_check((wft_workflow(v_wf)).status = 'PAUSED'
                      AND ((wft_workflow(v_wf)).error::json ->> 'error_code') = 'USER',
                      'failure (pause): workflow PAUSED with the node error');
    PERFORM wft_check((wft_workflow(v_child)).status = 'PAUSED', 'failure (pause): RUNNING child PAUSED');
    PERFORM wft_check((wft_node(v_wf, 2)).status = 'PENDING', 'failure (pause): nothing promoted');
END $$;

DO $$
BEGIN
    PERFORM wft_refused($q$SELECT * FROM horsies_workflow_node_finish(gen_random_uuid(), TRUE, 'SOME')$q$, '22023',
                        'node finish refuses an unknown dependents value');
    PERFORM wft_refused($q$SELECT * FROM horsies_workflow_node_finish(gen_random_uuid(), NULL, 'READ')$q$, '22023',
                        'node finish refuses a NULL wake');
    PERFORM wft_refused($q$SELECT horsies_task_error_envelope('USER_CODE', 'm', NULL, NULL, NULL)$q$, '22023',
                        'the envelope helper refuses a code outside the engine catalog');
END $$;

-- Sub-workflow completion into the parent node -----------------------------------------------------------------------

DO $$
DECLARE
    v_wf uuid := '00000000-0000-7000-8000-000000000501';
    v_child uuid := '00000000-0000-7000-8000-000000000502';
    v_row horsies_workflow_step_row;
BEGIN
    PERFORM * FROM wft_start(
        jsonb_build_array(
            jsonb_build_object('id', v_wf, 'on_error', 'fail'),
            jsonb_build_object('id', v_child, 'on_error', 'fail', 'parent', v_wf, 'parent_index', 0, 'depth', 1,
                               'output', 0)),
        jsonb_build_array(
            jsonb_build_object('idx', 0, 'sub', true),
            jsonb_build_object('idx', 1, 'deps', jsonb_build_array(0), 'args_from', jsonb_build_object('c', 0)),
            jsonb_build_object('wf', v_child, 'idx', 0))
    );
    SELECT * INTO v_row FROM horsies_workflow_child_finish(v_child) WHERE row_kind = 'OUTCOME';
    PERFORM wft_check(v_row.disposition = 'CHILD_NOT_TERMINAL' AND (wft_node(v_wf, 0)).status = 'RUNNING',
                      'child finish: a live child is refused, nothing written');
    PERFORM wft_check(wft_finish(v_child, 0, wft_ok('{"v":9}')) = 'APPLIED', 'child finish: child node APPLIED');
    SELECT * INTO v_row FROM horsies_workflow_node_finish(wft_task_of(v_child, 0), FALSE, 'READ')
    WHERE row_kind = 'OUTCOME';
    PERFORM wft_check((wft_workflow(v_child)).status = 'COMPLETED', 'child finish: child COMPLETED');
    SELECT * INTO v_row FROM horsies_workflow_child_finish(v_child) WHERE row_kind = 'OUTCOME';
    PERFORM wft_check(v_row.disposition = 'APPLIED' AND v_row.workflow_id = v_wf,
                      'child finish: parent node advanced');
    PERFORM wft_check((wft_node(v_wf, 0)).status = 'COMPLETED' AND (wft_node(v_wf, 0)).result = wft_ok('{"v":9}')
                      AND ((wft_node(v_wf, 0)).sub_workflow_summary::json -> 'output' ->> 'v') = '9',
                      'child finish: parent node holds the child output and summary');
    PERFORM wft_check((wft_node(v_wf, 1)).status = 'ENQUEUED'
                      AND (SELECT input::jsonb -> 'c' -> 'inner' FROM horsies_tasks WHERE id = wft_task_of(v_wf, 1))
                          = (wft_ok('{"v":9}'))::jsonb,
                      'child finish: parent dependent promoted with the child output');
    SELECT * INTO v_row FROM horsies_workflow_child_finish(v_child) WHERE row_kind = 'OUTCOME';
    PERFORM wft_check(v_row.disposition = 'ALREADY_APPLIED', 'child finish: a second advance is ALREADY_APPLIED');
END $$;

DO $$
DECLARE
    v_wf uuid := '00000000-0000-7000-8000-000000000511';
    v_child uuid := '00000000-0000-7000-8000-000000000512';
    v_row horsies_workflow_step_row;
BEGIN
    PERFORM * FROM wft_start(
        jsonb_build_array(
            jsonb_build_object('id', v_wf, 'on_error', 'fail'),
            jsonb_build_object('id', v_child, 'on_error', 'fail', 'parent', v_wf, 'parent_index', 0, 'depth', 1)),
        jsonb_build_array(jsonb_build_object('idx', 0, 'sub', true), jsonb_build_object('wf', v_child, 'idx', 0))
    );
    SELECT * INTO v_row FROM (
        SELECT r.* FROM horsies_workflow_node_finish(
            (SELECT wft_task_of(v_child, 0) FROM (SELECT wft_phase1(wft_task_of(v_child, 0), wft_err('CHILD'))) AS p),
            FALSE, 'READ') AS r
    ) AS q WHERE row_kind = 'OUTCOME';
    PERFORM wft_check(v_row.advance_child_workflow_id = v_child,
                      'child finish: the step that ends a child names it for the parent advance');
    PERFORM * FROM horsies_workflow_child_finish(v_child);
    PERFORM wft_check((wft_node(v_wf, 0)).status = 'FAILED'
                      AND ((wft_node(v_wf, 0)).result::json -> 'err' -> 'error_code' ->> '__builtin_task_code__')
                          = 'SUBWORKFLOW_FAILED'
                      AND ((wft_node(v_wf, 0)).result::json -> 'err' -> 'sub_workflow_summary' ->> 'error_summary')
                          = 'boom',
                      'child finish: a FAILED child gives SUBWORKFLOW_FAILED with the summary');
    PERFORM wft_check((wft_workflow(v_wf)).status = 'FAILED', 'child finish: the parent completes FAILED');
END $$;

-- Pause, resume, cancel ---------------------------------------------------------------------------------------------

DO $$
DECLARE
    v_wf uuid := '00000000-0000-7000-8000-000000000601';
    v_child uuid := '00000000-0000-7000-8000-000000000602';
    v_claimed uuid;
    v_row record;
BEGIN
    PERFORM * FROM wft_start(
        jsonb_build_array(
            jsonb_build_object('id', v_wf, 'on_error', 'fail'),
            jsonb_build_object('id', v_child, 'on_error', 'fail', 'parent', v_wf, 'parent_index', 0, 'depth', 1)),
        jsonb_build_array(
            jsonb_build_object('idx', 0, 'sub', true), jsonb_build_object('idx', 1),
            jsonb_build_object('idx', 2, 'deps', jsonb_build_array(1)),
            jsonb_build_object('wf', v_child, 'idx', 0), jsonb_build_object('wf', v_child, 'idx', 1))
    );
    v_claimed := wft_task_of(v_child, 0);
    UPDATE horsies_tasks SET status = 'CLAIMED', claimed_at = now(), claimed_by_worker_id = 'worker-1',
        claim_expires_at = now() + interval '1 minute' WHERE id = v_claimed;
    SELECT * INTO v_row FROM horsies_workflow_pause(v_wf);
    PERFORM wft_check(v_row.outcome = 'PAUSED' AND v_row.paused_workflow_ids = ARRAY[v_wf, v_child]
                      AND v_row.abandoned_task_ids = ARRAY[v_claimed], 'pause: tree PAUSED, claimed task abandoned');
    PERFORM wft_check((wft_workflow(v_child)).status = 'PAUSED'
                      AND NOT EXISTS (SELECT 1 FROM horsies_tasks WHERE id = v_claimed)
                      AND (wft_node(v_child, 0)).status = 'READY' AND (wft_node(v_child, 0)).task_id IS NULL,
                      'pause: abandoned node back to READY with no task');
    PERFORM wft_check(EXISTS (SELECT 1 FROM horsies_tasks WHERE id = wft_task_of(v_child, 1) AND status = 'PENDING'),
                      'pause: an unclaimed task stays for the claim-time check');
    SELECT * INTO v_row FROM horsies_workflow_pause(v_wf);
    PERFORM wft_check(v_row.outcome = 'NOT_RUNNING' AND v_row.observed_status = 'PAUSED',
                      'pause: a PAUSED workflow is NOT_RUNNING');
    SELECT * INTO v_row FROM horsies_workflow_pause('00000000-0000-7000-8000-0000000006ff');
    PERFORM wft_check(v_row.outcome = 'NOT_FOUND', 'pause: unknown id');

    -- Resume: the READY node is enqueued again, the PENDING node is judged, the child is resumed.
    PERFORM wft_phase1(wft_task_of(v_wf, 1), wft_ok('1'));
    PERFORM * FROM horsies_workflow_node_finish(wft_task_of(v_wf, 1), FALSE, 'READ');
    PERFORM wft_check((wft_node(v_wf, 2)).status = 'PENDING', 'resume setup: nothing promoted while PAUSED');
    PERFORM wft_check((SELECT disposition FROM horsies_workflow_resume(v_wf) WHERE row_kind = 'OUTCOME') = 'RESUMED',
                      'resume: RESUMED');
    PERFORM wft_check((wft_workflow(v_wf)).status = 'RUNNING' AND (wft_workflow(v_child)).status = 'RUNNING',
                      'resume: tree RUNNING');
    PERFORM wft_check((wft_node(v_child, 0)).status = 'ENQUEUED' AND wft_task_of(v_child, 0) IS NOT NULL
                      AND wft_task_of(v_child, 0) <> v_claimed, 'resume: READY node enqueued with a new task');
    PERFORM wft_check((wft_node(v_wf, 2)).status = 'ENQUEUED' AND wft_task_of(v_wf, 2) IS NOT NULL,
                      'resume: PENDING node with a met join enqueued');
    PERFORM wft_check((SELECT disposition FROM horsies_workflow_resume(v_wf) WHERE row_kind = 'OUTCOME')
                      = 'NOT_PAUSED', 'resume: a RUNNING workflow is NOT_PAUSED');
    PERFORM wft_check((SELECT disposition FROM horsies_workflow_resume('00000000-0000-7000-8000-0000000006ff')
                       WHERE row_kind = 'OUTCOME') = 'NOT_FOUND', 'resume: unknown id');
END $$;

DO $$
DECLARE
    v_wf uuid := '00000000-0000-7000-8000-000000000611';
    v_child uuid := '00000000-0000-7000-8000-000000000612';
BEGIN
    PERFORM * FROM wft_start(
        jsonb_build_array(
            jsonb_build_object('id', v_wf, 'on_error', 'fail'),
            jsonb_build_object('id', v_child, 'on_error', 'fail', 'parent', v_wf, 'parent_index', 0, 'depth', 1)),
        jsonb_build_array(jsonb_build_object('idx', 0, 'sub', true), jsonb_build_object('wf', v_child, 'idx', 0))
    );
    PERFORM horsies_workflow_pause(v_wf);
    PERFORM wft_phase1(wft_task_of(v_child, 0), wft_ok('4'));
    PERFORM * FROM horsies_workflow_node_finish(wft_task_of(v_child, 0), FALSE, 'READ');
    PERFORM wft_check((wft_workflow(v_child)).status = 'PAUSED', 'resume completion setup: child stays PAUSED');
    PERFORM * FROM horsies_workflow_resume(v_wf);
    PERFORM wft_check((wft_workflow(v_child)).status = 'COMPLETED' AND (wft_node(v_wf, 0)).status = 'COMPLETED'
                      AND (wft_workflow(v_wf)).status = 'COMPLETED',
                      'resume: a resumed tree with every node terminal completes, child to parent');
END $$;

DO $$
DECLARE
    v_wf uuid := '00000000-0000-7000-8000-000000000621';
    v_child uuid := '00000000-0000-7000-8000-000000000622';
    v_running uuid;
    v_pending_task uuid;
    v_row record;
BEGIN
    PERFORM * FROM wft_start(
        jsonb_build_array(
            jsonb_build_object('id', v_wf, 'on_error', 'fail'),
            jsonb_build_object('id', v_child, 'on_error', 'fail', 'parent', v_wf, 'parent_index', 0, 'depth', 1)),
        jsonb_build_array(
            jsonb_build_object('idx', 0, 'sub', true), jsonb_build_object('idx', 1), jsonb_build_object('idx', 2),
            jsonb_build_object('idx', 3, 'deps', jsonb_build_array(1)),
            jsonb_build_object('wf', v_child, 'idx', 0))
    );
    v_running := wft_task_of(v_wf, 2);
    UPDATE horsies_tasks SET status = 'RUNNING', claimed_at = now(), claimed_by_worker_id = 'worker-1',
        started_at = now() WHERE id = v_running;
    UPDATE horsies_workflow_tasks SET status = 'RUNNING' WHERE workflow_id = v_wf AND task_index = 2;
    v_pending_task := wft_task_of(v_wf, 1);
    SELECT * INTO v_row FROM horsies_workflow_cancel(v_wf);
    PERFORM wft_check(v_row.outcome = 'CANCELLED' AND v_row.cancelled_workflow_ids = ARRAY[v_wf, v_child],
                      'cancel: the tree is CANCELLED');
    PERFORM wft_check((wft_workflow(v_wf)).completed_at IS NOT NULL AND (wft_workflow(v_child)).status = 'CANCELLED',
                      'cancel: completed_at set on every cancelled workflow');
    PERFORM wft_check(v_pending_task = ANY(v_row.cancelled_task_ids)
                      AND NOT EXISTS (SELECT 1 FROM horsies_tasks WHERE id = v_pending_task)
                      AND (wft_node(v_wf, 1)).status = 'SKIPPED' AND (wft_node(v_wf, 1)).completed_at IS NOT NULL,
                      'cancel: the task behind an ENQUEUED node is cancelled and its node SKIPPED');
    PERFORM wft_check((wft_node(v_wf, 3)).status = 'SKIPPED', 'cancel: PENDING node SKIPPED');
    PERFORM wft_check((wft_node(v_wf, 2)).status = 'RUNNING' AND EXISTS (SELECT 1 FROM horsies_tasks WHERE id = v_running),
                      'cancel: a started task keeps its RUNNING node');
    SELECT * INTO v_row FROM horsies_workflow_cancel(v_wf);
    PERFORM wft_check(v_row.outcome = 'NOT_CANCELLABLE' AND v_row.observed_status = 'CANCELLED',
                      'cancel: a terminal workflow is NOT_CANCELLABLE');
    SELECT * INTO v_row FROM horsies_workflow_cancel('00000000-0000-7000-8000-0000000006fe');
    PERFORM wft_check(v_row.outcome = 'NOT_FOUND', 'cancel: unknown id');
END $$;

-- Phase-2 recovery ----------------------------------------------------------------------------------------------------

DO $$
DECLARE
    v_wf uuid := '00000000-0000-7000-8000-000000000701';
    v_task uuid;
    v_row horsies_workflow_step_row;
    v_pending horsies_workflow_phase2_pending;
BEGIN
    PERFORM * FROM wft_start1(v_wf, 'fail', '[{"idx":0},{"idx":1,"deps":[0]}]');
    v_task := wft_task_of(v_wf, 0);
    PERFORM wft_phase1(v_task, wft_ok('1'));
    UPDATE horsies_workflow_phase2_pending SET result_digest = sha256('other'::bytea) WHERE task_id = v_task;
    SELECT * INTO v_row FROM horsies_workflow_phase2_recover(v_task, 3) WHERE row_kind = 'OUTCOME';
    SELECT * INTO v_pending FROM horsies_workflow_phase2_pending WHERE task_id = v_task;
    PERFORM wft_check(v_row.disposition = 'SOURCE_DIGEST_MISMATCH' AND v_pending.attempt_count = 1
                      AND v_pending.last_failure_class = 'SOURCE_DIGEST_MISMATCH'
                      AND v_pending.last_attempt_at IS NOT NULL, 'phase-2 recovery: a retaining outcome counts');
    PERFORM horsies_workflow_phase2_recover(v_task, 3);
    SELECT * INTO v_row FROM horsies_workflow_phase2_recover(v_task, 3) WHERE row_kind = 'OUTCOME';
    SELECT * INTO v_pending FROM horsies_workflow_phase2_pending WHERE task_id = v_task;
    PERFORM wft_check(v_pending.attempt_count = 3 AND v_row.detail LIKE 'COPY_VERIFICATION_FAILED%',
                      'phase-2 recovery: at the bound the quarantine runs (a bad digest refuses the copy)');
    UPDATE horsies_workflow_phase2_pending
    SET result_digest = (SELECT result_digest FROM horsies_task_history WHERE task_id = v_task)
    WHERE task_id = v_task;
    SELECT * INTO v_row FROM horsies_workflow_phase2_recover(v_task, 10) WHERE row_kind = 'OUTCOME';
    PERFORM wft_check(v_row.disposition = 'APPLIED' AND (wft_node(v_wf, 1)).status = 'ENQUEUED',
                      'phase-2 recovery: an applied record progresses');
    PERFORM wft_refused($q$SELECT * FROM horsies_workflow_phase2_recover(gen_random_uuid(), 2)$q$, '22023',
                        'phase-2 recovery refuses a bound under 3');
    PERFORM wft_refused($q$SELECT * FROM horsies_workflow_phase2_recover(gen_random_uuid(), 1001)$q$, '22023',
                        'phase-2 recovery refuses a bound over 1000');
END $$;

DO $$
DECLARE
    v_wf uuid := '00000000-0000-7000-8000-000000000711';
    v_task uuid;
    v_row record;
    v_limit_error text := horsies_task_error_object('WORKFLOW_ENQUEUE_FAILED', 'limit', NULL);
    v_limit_node text := horsies_task_error_envelope('WORKFLOW_ENQUEUE_FAILED', 'limit node', NULL, NULL, NULL);
BEGIN
    PERFORM * FROM wft_start1(v_wf, 'fail', '[{"idx":0},{"idx":1,"deps":[0]}]');
    v_task := wft_task_of(v_wf, 0);
    PERFORM wft_phase1(v_task, wft_ok('1'));
    SELECT * INTO v_row FROM horsies_workflow_phase2_record_raise(v_task, 3, v_limit_error, v_limit_node);
    PERFORM wft_check(v_row.outcome = 'COUNTED' AND v_row.attempt_count = 1
                      AND (SELECT last_failure_class FROM horsies_workflow_phase2_pending WHERE task_id = v_task)
                          = 'RAISED', 'raise record: COUNTED under the bound');
    PERFORM horsies_workflow_phase2_record_raise(v_task, 3, v_limit_error, v_limit_node);
    UPDATE horsies_workflows SET status = 'PAUSED' WHERE id = v_wf;
    SELECT * INTO v_row FROM horsies_workflow_phase2_record_raise(v_task, 3, v_limit_error, v_limit_node);
    PERFORM wft_check(v_row.outcome = 'NOT_RECORDED'
                      AND (SELECT attempt_count FROM horsies_workflow_phase2_pending WHERE task_id = v_task) = 2,
                      'raise record: NOT_RECORDED at the bound when the workflow is not RUNNING');
    UPDATE horsies_workflows SET status = 'RUNNING' WHERE id = v_wf;
    SELECT * INTO v_row FROM horsies_workflow_phase2_record_raise(v_task, 3, v_limit_error, v_limit_node);
    PERFORM wft_check(v_row.outcome = 'LIMIT_REACHED' AND v_row.limit_outcome = 'LIMIT_REACHED_FAILED'
                      AND v_row.quarantine_verdict = 'REPOINTED' AND v_row.attempt_count = 3,
                      'raise record: LIMIT_REACHED at the bound');
    PERFORM wft_check((wft_node(v_wf, 0)).status = 'FAILED' AND (wft_node(v_wf, 0)).result = v_limit_node
                      AND (wft_node(v_wf, 1)).status = 'SKIPPED' AND (wft_workflow(v_wf)).status = 'FAILED'
                      AND (wft_workflow(v_wf)).error = v_limit_error,
                      'raise record: the limit fails the node and the workflow closed');
    PERFORM wft_check((SELECT recovery_source FROM horsies_workflow_phase2_pending WHERE task_id = v_task)
                      = 'QUARANTINE', 'raise record: the evidence is repointed to the quarantine copy');
    SELECT * INTO v_row FROM horsies_workflow_phase2_record_raise(gen_random_uuid(), 3, v_limit_error, v_limit_node);
    PERFORM wft_check(v_row.outcome = 'PENDING_ABSENT', 'raise record: an unknown record');
    PERFORM wft_refused($q$SELECT * FROM horsies_workflow_phase2_record_raise(gen_random_uuid(), 3, NULL, '{}')$q$,
                        '22023', 'raise record refuses a NULL limit error');
END $$;

DO $$
DECLARE
    v_wf uuid := '00000000-0000-7000-8000-000000000721';
    v_task uuid;
    v_row horsies_workflow_step_row;
BEGIN
    PERFORM * FROM wft_start1(v_wf, 'fail', '[{"idx":0},{"idx":1,"deps":[0]}]');
    v_task := wft_task_of(v_wf, 0);
    PERFORM wft_phase1(v_task, wft_ok('1'));
    UPDATE horsies_workflow_phase2_pending SET attempt_count = 2, last_attempt_at = now(),
        last_failure_class = 'RAISED' WHERE task_id = v_task;
    PERFORM horsies_phase2_quarantine_one(v_task);
    SELECT * INTO v_row FROM horsies_workflow_phase2_recover(v_task, 3) WHERE row_kind = 'OUTCOME';
    PERFORM wft_check(v_row.disposition = 'APPLIED' AND (wft_node(v_wf, 0)).result = wft_ok('1')
                      AND NOT EXISTS (SELECT 1 FROM horsies_workflow_phase2_quarantine WHERE task_id = v_task),
                      'phase-2 recovery: a quarantined record applies from the copy and deletes it');
END $$;

-- Downstream sub-workflow fact and the two phase-2 forms ------------------------------------------------------------

DO $$
DECLARE
    v_wf uuid := '00000000-0000-7000-8000-000000000801';
    v_child uuid := '00000000-0000-7000-8000-000000000802';
BEGIN
    -- Top level: 0 -> 1 -> 2 (sub); 0 -> 5; 3 -> 4; 6 (sub root with a child). Child: 0 -> 1 (sub).
    PERFORM * FROM wft_start(
        jsonb_build_array(
            jsonb_build_object('id', v_wf, 'on_error', 'fail'),
            jsonb_build_object('id', v_child, 'on_error', 'fail', 'parent', v_wf, 'parent_index', 6, 'depth', 1)),
        jsonb_build_array(
            jsonb_build_object('idx', 0), jsonb_build_object('idx', 1, 'deps', jsonb_build_array(0)),
            jsonb_build_object('idx', 2, 'deps', jsonb_build_array(1), 'sub', true, 'params', 'GIVEN'),
            jsonb_build_object('idx', 3), jsonb_build_object('idx', 4, 'deps', jsonb_build_array(3)),
            jsonb_build_object('idx', 5, 'deps', jsonb_build_array(0)),
            jsonb_build_object('idx', 6, 'sub', true),
            jsonb_build_object('wf', v_child, 'idx', 0),
            jsonb_build_object('wf', v_child, 'idx', 1, 'deps', jsonb_build_array(0), 'sub', true))
    );
    PERFORM wft_check(
        (SELECT string_agg(task_index || '=' || downstream_subworkflow, ',' ORDER BY task_index)
         FROM horsies_workflow_tasks WHERE workflow_id = v_wf)
        = '0=REACHABLE,1=REACHABLE,2=NONE,3=NONE,4=NONE,5=NONE,6=NONE',
        'start: downstream_subworkflow is REACHABLE exactly for the ancestors of a sub-workflow node'
    );
    PERFORM wft_check(
        (SELECT string_agg(task_index || '=' || downstream_subworkflow, ',' ORDER BY task_index)
         FROM horsies_workflow_tasks WHERE workflow_id = v_child) = '0=REACHABLE,1=NONE',
        'start: a child workflow gets its own downstream_subworkflow facts'
    );
    PERFORM wft_check((wft_node(v_wf, 2)).sub_workflow_params = 'GIVEN'
                      AND (wft_node(v_wf, 6)).sub_workflow_params = 'NOT_GIVEN'
                      AND (wft_node(v_wf, 0)).sub_workflow_params IS NULL,
                      'start: sub_workflow_params stored on sub-workflow nodes only');
    PERFORM wft_refused($q$
        SELECT * FROM horsies_workflow_start(
            ARRAY['00000000-0000-7000-8000-000000000803'::uuid], ARRAY['wf'], ARRAY[NULL::text], ARRAY['fail'],
            ARRAY[NULL::integer], ARRAY[NULL::text], ARRAY[NULL::uuid], ARRAY[NULL::integer], ARRAY[0], now(),
            ARRAY['00000000-0000-7000-8000-000000000803'::uuid], ARRAY[0], ARRAY['n0'], ARRAY['s0'], ARRAY['{}'],
            ARRAY['default'], ARRAY[50], ARRAY['[]'], ARRAY[NULL::text], ARRAY[NULL::text], ARRAY[FALSE],
            ARRAY['all'], ARRAY[NULL::integer], ARRAY[NULL::text], ARRAY[NULL::integer], ARRAY[NULL::text],
            ARRAY[NULL::timestamptz], ARRAY[NULL::text], ARRAY[TRUE], ARRAY['child.wf'], ARRAY['child.key'],
            ARRAY[NULL::text], ARRAY[]::uuid[], ARRAY[]::integer[], ARRAY[]::text[])
    $q$, '23514', 'start refuses a sub-workflow node with no params choice (table CHECK)');
END $$;

DO $$
DECLARE
    v_wf uuid := '00000000-0000-7000-8000-000000000811';
    v_rows bigint;
    v_child_starts bigint;
BEGIN
    PERFORM * FROM wft_start1(v_wf, 'fail', '[
        {"idx":0},{"idx":1,"deps":[0]},
        {"idx":2},{"idx":3,"deps":[2],"sub":true,"params":"GIVEN"}
    ]');
    -- Node 0: downstream NONE, the one-statement form; it owes no child start.
    PERFORM wft_phase1(wft_task_of(v_wf, 0), wft_ok('1'));
    SELECT count(*), count(*) FILTER (WHERE row_kind = 'CHILD_START') INTO v_rows, v_child_starts
    FROM horsies_workflow_node_finish(wft_task_of(v_wf, 0), TRUE, 'READ');
    PERFORM wft_check(v_rows = 1 AND v_child_starts = 0 AND (wft_node(v_wf, 1)).status = 'ENQUEUED',
                      'downstream NONE: one statement, task node promoted, no child start owed');
    -- Node 2: downstream REACHABLE, the transaction form; it owes the child start of node 3.
    PERFORM wft_phase1(wft_task_of(v_wf, 2), wft_ok('2'));
    SELECT count(*) FILTER (WHERE row_kind = 'CHILD_START' AND task_index = 3 AND sub_workflow_params = 'GIVEN')
    INTO v_child_starts
    FROM horsies_workflow_node_finish(wft_task_of(v_wf, 2), TRUE, 'READ');
    PERFORM wft_check(v_child_starts = 1, 'downstream REACHABLE: the step owes the child start, with its params');
END $$;

DO $$
DECLARE
    v_wf uuid := '00000000-0000-7000-8000-000000000821';
    v_task uuid;
BEGIN
    PERFORM * FROM wft_start1(v_wf, 'fail', '[{"idx":0},{"idx":1,"deps":[0],"sub":true}]');
    v_task := wft_task_of(v_wf, 0);
    PERFORM wft_phase1(v_task, wft_ok('1'));
    UPDATE horsies_workflow_tasks SET downstream_subworkflow = 'NONE' WHERE workflow_id = v_wf AND task_index = 0;
    PERFORM wft_refused(
        format($q$SELECT * FROM horsies_workflow_node_finish(%L, TRUE, 'READ')$q$, v_task), 'XX001',
        'node finish refuses a child start owed by a node whose fact says NONE');
    PERFORM wft_check((wft_node(v_wf, 0)).status = 'ENQUEUED' AND (wft_node(v_wf, 1)).status = 'PENDING'
                      AND EXISTS (SELECT 1 FROM horsies_workflow_phase2_pending WHERE task_id = v_task),
                      'node finish refusal writes nothing');
END $$;

DO $$
DECLARE
    v_reach uuid := '00000000-0000-7000-8000-000000000831';
    v_none uuid := '00000000-0000-7000-8000-000000000832';
    v_leaf uuid := '00000000-0000-7000-8000-000000000833';
    v_task uuid;
    v_claimed timestamptz := now() - interval '1 second';
    v_out record;
BEGIN
    PERFORM * FROM wft_start1(v_reach, 'fail', '[{"idx":0},{"idx":1,"deps":[0],"sub":true}]');
    PERFORM * FROM wft_start1(v_none, 'fail', '[{"idx":0},{"idx":1,"deps":[0]}]');
    PERFORM * FROM wft_start1(v_leaf, 'fail', '[{"idx":0}]');
    v_task := wft_task_of(v_reach, 0);
    UPDATE horsies_tasks SET status = 'RUNNING', claimed_at = v_claimed, claimed_by_worker_id = 'worker-1',
        started_at = now(), worker_pid = 1, worker_hostname = 'host', worker_process_name = 'proc' WHERE id = v_task;
    SELECT * INTO v_out FROM horsies_finish_workflow_success(v_task, 'worker-1', v_claimed, wft_ok('1'));
    PERFORM wft_check(v_out.outcome = 'APPLIED' AND v_out.node_has_dependents
                      AND v_out.downstream_subworkflow = 'REACHABLE',
                      'phase 1 (success) returns downstream_subworkflow REACHABLE with node_has_dependents');
    v_task := wft_task_of(v_none, 0);
    UPDATE horsies_tasks SET status = 'RUNNING', claimed_at = v_claimed, claimed_by_worker_id = 'worker-1',
        started_at = now(), worker_pid = 1, worker_hostname = 'host', worker_process_name = 'proc' WHERE id = v_task;
    SELECT * INTO v_out FROM horsies_finish_failed_attempt(
        v_task, 'worker-1', v_claimed, 'NONE', NULL, NULL, NULL, wft_err('USER'), 'USER', 'boom',
        'FAILED', 'boom', 'boom', 'REFUSE', TRUE);
    PERFORM wft_check(v_out.outcome = 'APPLIED' AND v_out.node_has_dependents
                      AND v_out.downstream_subworkflow = 'NONE',
                      'phase 1 (failure) returns downstream_subworkflow NONE for task-only dependents');
    v_task := wft_task_of(v_leaf, 0);
    UPDATE horsies_tasks SET status = 'RUNNING', claimed_at = v_claimed, claimed_by_worker_id = 'worker-1',
        started_at = now(), worker_pid = 1, worker_hostname = 'host', worker_process_name = 'proc' WHERE id = v_task;
    SELECT * INTO v_out FROM horsies_finish_workflow_success(v_task, 'worker-1', v_claimed, wft_ok('1'));
    PERFORM wft_check(v_out.outcome = 'APPLIED' AND NOT v_out.node_has_dependents
                      AND v_out.downstream_subworkflow = 'NONE', 'phase 1 of a leaf: no dependents, NONE');
    SELECT * INTO v_out FROM horsies_finish_workflow_success(v_task, 'worker-1', v_claimed, wft_ok('1'));
    PERFORM wft_check(v_out.outcome <> 'APPLIED' AND v_out.node_has_dependents IS NULL
                      AND v_out.downstream_subworkflow IS NULL, 'phase 1 miss: both facts NULL');
END $$;
