-- Cases for the workflow engine functions of 0005_workflow_functions.sql, on a fresh install with the fixtures of
-- baseline_workflow_fixtures.sql. The caller runs both files in one transaction and rolls it back. Each case is one
-- DO block: a pass case checks the rows that the function wrote; a refusal case checks the SQLSTATE and that nothing
-- was written. A failed check raises with the case name. The two-session lock-order cases are in
-- sql_tests/baseline_workflow_functions.rs.

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
    PERFORM wft_check(v_task.attempt_claims = 0 AND v_task.first_claimed_at IS NULL,
                      'start: a node task starts its first attempt with count 0 and no first claim');
    PERFORM wft_check(
        v_task.input = '{"x":1,"__h_workflow_meta__":{"workflow_id":"' || v_wf || '","task_index":0,"task_name":"t0"}}',
        'start: root input is the static input with the workflow meta'
    );
    PERFORM wft_check((SELECT good_until FROM horsies_tasks WHERE id = wft_task_of(v_wf, 2)) = '2031-01-01T00:00:00Z',
                      'start: good_until comes from the stored task options');
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
            ARRAY[NULL::text], ARRAY[]::uuid[], ARRAY[]::integer[], ARRAY[]::text[], 'NO_CARRIER', NULL, NULL)
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

DO $$
DECLARE
    v_wf uuid := '00000000-0000-7000-8000-000000000151';
    v_child uuid := '00000000-0000-7000-8000-000000000152';
    v_error text := horsies_task_error_envelope('WORKFLOW_ENQUEUE_FAILED', 'build failed', NULL, NULL, NULL);
BEGIN
    -- A failed record of the top workflow and one of a pre-built child.
    PERFORM * FROM wft_start(
        jsonb_build_array(
            jsonb_build_object('id', v_wf, 'on_error', 'fail'),
            jsonb_build_object('id', v_child, 'on_error', 'fail', 'parent', v_wf, 'parent_index', 1, 'depth', 1)),
        jsonb_build_array(
            jsonb_build_object('idx', 0, 'sub', true),
            jsonb_build_object('idx', 1, 'sub', true),
            jsonb_build_object('wf', v_child, 'idx', 0, 'sub', true),
            jsonb_build_object('wf', v_child, 'idx', 1)),
        jsonb_build_array(
            jsonb_build_object('idx', 0, 'result', v_error),
            jsonb_build_object('wf', v_child, 'idx', 0, 'result', v_error))
    );
    PERFORM wft_check((wft_node(v_wf, 0)).status = 'FAILED' AND (wft_node(v_wf, 0)).started_at IS NOT NULL
                      AND (wft_node(v_wf, 0)).started_at = (wft_node(v_wf, 0)).completed_at,
                      'start failed record: the node has started_at, as an ENQUEUED write before the failure');
    PERFORM wft_check((wft_node(v_child, 0)).status = 'FAILED' AND (wft_node(v_child, 0)).started_at IS NOT NULL,
                      'start failed record of a pre-built child: the node has started_at');
END $$;

DO $$
DECLARE
    v_wf uuid := '00000000-0000-7000-8000-000000000161';
    v_child uuid := '00000000-0000-7000-8000-000000000162';
    v_grandchild uuid := '00000000-0000-7000-8000-000000000163';
    v_error text := horsies_task_error_envelope('WORKFLOW_ENQUEUE_FAILED', 'build failed', NULL, NULL, NULL);
BEGIN
    -- Three levels: the record at depth 2 ends the grandchild, whose advance ends the child, whose advance fails the
    -- top node and ends the top workflow.
    PERFORM * FROM wft_start(
        jsonb_build_array(
            jsonb_build_object('id', v_wf, 'on_error', 'fail'),
            jsonb_build_object('id', v_child, 'on_error', 'fail', 'parent', v_wf, 'parent_index', 0, 'depth', 1),
            jsonb_build_object('id', v_grandchild, 'on_error', 'fail', 'parent', v_child, 'parent_index', 0,
                               'depth', 2)),
        jsonb_build_array(
            jsonb_build_object('idx', 0, 'sub', true),
            jsonb_build_object('wf', v_child, 'idx', 0, 'sub', true),
            jsonb_build_object('wf', v_grandchild, 'idx', 0, 'sub', true)),
        jsonb_build_array(jsonb_build_object('wf', v_grandchild, 'idx', 0, 'result', v_error))
    );
    PERFORM wft_check((wft_workflow(v_grandchild)).status = 'FAILED' AND (wft_workflow(v_child)).status = 'FAILED',
                      'start, parent advance: the grandchild and the child end');
    PERFORM wft_check((wft_node(v_wf, 0)).status = 'FAILED'
                      AND ((wft_node(v_wf, 0)).result::json -> 'err' ->> 'sub_workflow_id') = v_child::text,
                      'start, parent advance: the advance of the ended child reaches the top node');
    PERFORM wft_check((wft_workflow(v_wf)).status = 'FAILED' AND (wft_workflow(v_wf)).completed_at IS NOT NULL,
                      'start, parent advance: the top workflow ends in the same statement');
END $$;

DO $$
DECLARE
    v_wf uuid := '00000000-0000-7000-8000-000000000171';
    v_child uuid := '00000000-0000-7000-8000-000000000172';
    v_error text := horsies_task_error_envelope('WORKFLOW_ENQUEUE_FAILED', 'build failed', NULL, NULL, NULL);
BEGIN
    -- on_error pause; a task root, and a sub-workflow root whose child (on_error fail) ends at once: its advance
    -- pauses the top workflow before the promotion.
    PERFORM * FROM wft_start(
        jsonb_build_array(
            jsonb_build_object('id', v_wf, 'on_error', 'pause'),
            jsonb_build_object('id', v_child, 'on_error', 'fail', 'parent', v_wf, 'parent_index', 1, 'depth', 1)),
        jsonb_build_array(
            jsonb_build_object('idx', 0),
            jsonb_build_object('idx', 1, 'sub', true),
            jsonb_build_object('wf', v_child, 'idx', 0, 'sub', true)),
        jsonb_build_array(jsonb_build_object('wf', v_child, 'idx', 0, 'result', v_error))
    );
    PERFORM wft_check((wft_workflow(v_child)).status = 'FAILED' AND (wft_node(v_wf, 1)).status = 'FAILED'
                      AND (wft_workflow(v_wf)).status = 'PAUSED',
                      'start, pause by an ended child: the child ends and the advance pauses the top workflow');
    PERFORM wft_check((wft_node(v_wf, 0)).status = 'READY' AND (wft_node(v_wf, 0)).task_id IS NULL
                      AND (wft_node(v_wf, 0)).started_at IS NULL,
                      'start, pause by an ended child: the pause gate keeps the task root READY with no task row');
END $$;

DO $$
DECLARE
    v_wf uuid := '00000000-0000-7000-8000-000000000181';
    v_child uuid := '00000000-0000-7000-8000-000000000182';
    v_grandchild uuid := '00000000-0000-7000-8000-000000000183';
    v_error text := horsies_task_error_envelope('WORKFLOW_ENQUEUE_FAILED', 'build failed', NULL, NULL, NULL);
    v_row horsies_workflow_step_row;
BEGIN
    -- on_error pause; root 0 has a failed record, root 1 a pre-built child with a pre-built grandchild and a failed
    -- record of its own, root 2 is a task.
    SELECT * INTO v_row FROM wft_start(
        jsonb_build_array(
            jsonb_build_object('id', v_wf, 'on_error', 'pause'),
            jsonb_build_object('id', v_child, 'on_error', 'fail', 'parent', v_wf, 'parent_index', 1, 'depth', 1),
            jsonb_build_object('id', v_grandchild, 'on_error', 'fail', 'parent', v_child, 'parent_index', 0,
                               'depth', 2)),
        jsonb_build_array(
            jsonb_build_object('idx', 0, 'sub', true),
            jsonb_build_object('idx', 1, 'sub', true),
            jsonb_build_object('idx', 2),
            jsonb_build_object('wf', v_child, 'idx', 0, 'sub', true),
            jsonb_build_object('wf', v_child, 'idx', 1, 'sub', true),
            jsonb_build_object('wf', v_grandchild, 'idx', 0)),
        jsonb_build_array(
            jsonb_build_object('idx', 0, 'result', v_error),
            jsonb_build_object('wf', v_child, 'idx', 1, 'result', v_error))
    ) WHERE row_kind = 'OUTCOME';
    PERFORM wft_check(v_row.disposition = 'STARTED' AND (wft_workflow(v_wf)).status = 'PAUSED',
                      'start, pre-built child under a pause: STARTED, the failed record pauses the workflow');
    PERFORM wft_check((wft_node(v_wf, 1)).status = 'READY' AND (wft_node(v_wf, 1)).sub_workflow_id IS NULL
                      AND (wft_node(v_wf, 1)).started_at IS NULL,
                      'start, pre-built child under a pause: the node stays READY with no child');
    PERFORM wft_check(NOT EXISTS (SELECT 1 FROM horsies_workflows WHERE id IN (v_child, v_grandchild))
                      AND NOT EXISTS (SELECT 1 FROM horsies_workflow_tasks
                                      WHERE workflow_id IN (v_child, v_grandchild)),
                      'start, pre-built child under a pause: the child and its subtree are not written');
    PERFORM wft_check((wft_node(v_wf, 2)).status = 'READY' AND (wft_node(v_wf, 2)).task_id IS NULL,
                      'start, pre-built child under a pause: the task root stays READY (the pause gate)');
    PERFORM wft_check(EXISTS (SELECT 1 FROM horsies_workflow_resume(v_wf) AS r
                              WHERE r.row_kind = 'CHILD_START' AND r.workflow_id = v_wf AND r.task_index = 1),
                      'start, pre-built child under a pause: the resume owes the child start of the node');
END $$;

DO $$
DECLARE
    v_wf uuid := '00000000-0000-7000-8000-000000000191';
    v_child uuid := '00000000-0000-7000-8000-000000000192';
    v_grandchild uuid := '00000000-0000-7000-8000-000000000193';
    v_error text := horsies_task_error_envelope('WORKFLOW_ENQUEUE_FAILED', 'build failed', NULL, NULL, NULL);
BEGIN
    -- A TREE child start: the child (on_error pause) has a failed record at root 0 and a pre-built grandchild at
    -- root 1.
    PERFORM * FROM wft_start1(v_wf, 'fail', '[{"idx":0},{"idx":1,"deps":[0],"sub":true}]');
    PERFORM wft_finish(v_wf, 0, wft_ok('1'));
    PERFORM * FROM wft_child_start(v_wf, 1,
        jsonb_build_array(
            jsonb_build_object('id', v_child, 'on_error', 'pause', 'parent', v_wf, 'parent_index', 1, 'depth', 1),
            jsonb_build_object('id', v_grandchild, 'on_error', 'fail', 'parent', v_child, 'parent_index', 1,
                               'depth', 2)),
        jsonb_build_array(
            jsonb_build_object('wf', v_child, 'idx', 0, 'sub', true),
            jsonb_build_object('wf', v_child, 'idx', 1, 'sub', true),
            jsonb_build_object('wf', v_grandchild, 'idx', 0)),
        jsonb_build_array(jsonb_build_object('wf', v_child, 'idx', 0, 'result', v_error))
    );
    PERFORM wft_check((wft_workflow(v_child)).status = 'PAUSED',
                      'child start TREE, pre-built grandchild under a pause: the failed record pauses the child');
    PERFORM wft_check((wft_node(v_child, 1)).status = 'READY' AND (wft_node(v_child, 1)).sub_workflow_id IS NULL
                      AND NOT EXISTS (SELECT 1 FROM horsies_workflows WHERE id = v_grandchild),
                      'child start TREE, pre-built grandchild under a pause: the grandchild is not written');
END $$;

DO $$
BEGIN
    PERFORM wft_refused($q$
        SELECT * FROM wft_start('[{"id":"00000000-0000-7000-8000-0000000001a1","on_error":"pause"},
                                  {"id":"00000000-0000-7000-8000-0000000001a2","on_error":"fail",
                                   "parent":"00000000-0000-7000-8000-0000000001a1","parent_index":1,"depth":1}]',
                                '[{"idx":0,"sub":true},{"idx":1,"sub":true},
                                  {"wf":"00000000-0000-7000-8000-0000000001a2","idx":0},
                                  {"wf":"00000000-0000-7000-8000-0000000001a2","idx":1,"deps":[0]}]',
                                '[{"idx":0,"result":"{}"},
                                  {"wf":"00000000-0000-7000-8000-0000000001a2","idx":1,"result":"{}"}]')
    $q$, '22023', 'start refuses a failed record that names no root, also in a child that a pause leaves unwritten');
    PERFORM wft_refused($q$
        SELECT * FROM wft_start('[{"id":"00000000-0000-7000-8000-0000000001a1","on_error":"fail"},
                                  {"id":"00000000-0000-7000-8000-0000000001a2","on_error":"fail",
                                   "parent":"00000000-0000-7000-8000-0000000001a1","parent_index":0,"depth":1},
                                  {"id":"00000000-0000-7000-8000-0000000001a3","on_error":"fail",
                                   "parent":"00000000-0000-7000-8000-0000000001a1","parent_index":0,"depth":1}]',
                                '[{"idx":0,"sub":true}]')
    $q$, '22023', 'start refuses two child rows that name one node');
    PERFORM wft_check(NOT EXISTS (SELECT 1 FROM horsies_workflows WHERE id = '00000000-0000-7000-8000-0000000001a1'),
                      'start refusals of the tree check write nothing');
END $$;

DO $$
DECLARE
    v_wf uuid := '00000000-0000-7000-8000-0000000001b1';
    v_child uuid := '00000000-0000-7000-8000-0000000001b2';
    v_error text := horsies_task_error_envelope('WORKFLOW_ENQUEUE_FAILED', 'build failed', NULL, NULL, NULL);
BEGIN
    -- on_error pause; root 0 has a pre-built child (a task root and its dependent), root 1 a failed record. The roots
    -- go in index order: the child and the task row of its root go in before root 1 fails and pauses the tree.
    PERFORM * FROM wft_start(
        jsonb_build_array(
            jsonb_build_object('id', v_wf, 'on_error', 'pause'),
            jsonb_build_object('id', v_child, 'on_error', 'fail', 'parent', v_wf, 'parent_index', 0, 'depth', 1)),
        jsonb_build_array(
            jsonb_build_object('idx', 0, 'sub', true),
            jsonb_build_object('idx', 1, 'sub', true),
            jsonb_build_object('wf', v_child, 'idx', 0),
            jsonb_build_object('wf', v_child, 'idx', 1, 'deps', jsonb_build_array(0))),
        jsonb_build_array(jsonb_build_object('idx', 1, 'result', v_error))
    );
    PERFORM wft_check((wft_node(v_wf, 0)).status = 'RUNNING' AND (wft_node(v_wf, 0)).sub_workflow_id = v_child,
                      'start, root order: root 0 is linked to its child before root 1 fails');
    PERFORM wft_check((wft_node(v_child, 0)).status = 'ENQUEUED'
                      AND EXISTS (SELECT 1 FROM horsies_tasks t
                                  WHERE t.id = (wft_node(v_child, 0)).task_id AND t.status = 'PENDING'),
                      'start, root order: the task root of the child is ENQUEUED with a PENDING task row');
    PERFORM wft_check((wft_node(v_wf, 1)).status = 'FAILED' AND (wft_workflow(v_wf)).status = 'PAUSED'
                      AND (wft_workflow(v_child)).status = 'PAUSED',
                      'start, root order: the failed root 1 pauses the workflow and the child');
END $$;

DO $$
DECLARE
    v_wf uuid := '00000000-0000-7000-8000-0000000001c1';
    v_failing uuid := '00000000-0000-7000-8000-0000000001c2';
    v_later uuid := '00000000-0000-7000-8000-0000000001c3';
    v_error text := horsies_task_error_envelope('WORKFLOW_ENQUEUE_FAILED', 'build failed', NULL, NULL, NULL);
BEGIN
    -- on_error pause; root 0 has a pre-built child (on_error fail) whose only root has a failed record, root 1 a
    -- pre-built child. Child 0 ends FAILED, its advance fails root 0 and pauses the workflow before root 1 is taken.
    PERFORM * FROM wft_start(
        jsonb_build_array(
            jsonb_build_object('id', v_wf, 'on_error', 'pause'),
            jsonb_build_object('id', v_failing, 'on_error', 'fail', 'parent', v_wf, 'parent_index', 0, 'depth', 1),
            jsonb_build_object('id', v_later, 'on_error', 'fail', 'parent', v_wf, 'parent_index', 1, 'depth', 1)),
        jsonb_build_array(
            jsonb_build_object('idx', 0, 'sub', true),
            jsonb_build_object('idx', 1, 'sub', true),
            jsonb_build_object('wf', v_failing, 'idx', 0, 'sub', true),
            jsonb_build_object('wf', v_later, 'idx', 0)),
        jsonb_build_array(jsonb_build_object('wf', v_failing, 'idx', 0, 'result', v_error))
    );
    PERFORM wft_check((wft_workflow(v_failing)).status = 'FAILED' AND (wft_node(v_wf, 0)).status = 'FAILED'
                      AND (wft_workflow(v_wf)).status = 'PAUSED',
                      'start, root order: the ended child 0 fails root 0 and pauses the workflow');
    PERFORM wft_check((wft_node(v_wf, 1)).status = 'READY' AND (wft_node(v_wf, 1)).sub_workflow_id IS NULL
                      AND (wft_node(v_wf, 1)).started_at IS NULL
                      AND NOT EXISTS (SELECT 1 FROM horsies_workflows WHERE id = v_later),
                      'start, root order: root 1 stays READY and its child is not written');
    PERFORM wft_check(((wft_workflow(v_wf)).error::json ->> 'message') = format('Subworkflow %s failed', v_failing)
                      AND ((wft_workflow(v_wf)).error::json -> 'error_code' ->> '__builtin_task_code__')
                          = 'SUBWORKFLOW_FAILED'
                      AND ((wft_workflow(v_wf)).error::json ->> 'sub_workflow_id') = v_failing::text,
                      'paused parent of a failed child: the workflow error is "Subworkflow <id> failed", as Python');
    PERFORM wft_check(((wft_node(v_wf, 0)).result::json -> 'err' ->> 'message')
                          = format('Subworkflow %s failed with status FAILED', v_failing),
                      'paused parent of a failed child: the node result keeps the message with the status');
END $$;

DO $$
DECLARE
    v_wf uuid := '00000000-0000-7000-8000-0000000001d1';
    v_error text := horsies_task_error_envelope('WORKFLOW_ENQUEUE_FAILED', 'build failed', NULL, NULL, NULL);
BEGIN
    -- on_error pause; roots 0 and 1 both have a failed record. Root 0 fails and pauses the workflow; root 1 is not
    -- taken.
    PERFORM * FROM wft_start(
        jsonb_build_array(jsonb_build_object('id', v_wf, 'on_error', 'pause')),
        jsonb_build_array(
            jsonb_build_object('idx', 0, 'sub', true),
            jsonb_build_object('idx', 1, 'sub', true)),
        jsonb_build_array(
            jsonb_build_object('idx', 0, 'result', v_error),
            jsonb_build_object('idx', 1, 'result', v_error))
    );
    PERFORM wft_check((wft_node(v_wf, 0)).status = 'FAILED' AND (wft_workflow(v_wf)).status = 'PAUSED',
                      'start, root order: the failed root 0 pauses the workflow');
    PERFORM wft_check((wft_node(v_wf, 1)).status = 'READY' AND (wft_node(v_wf, 1)).result IS NULL
                      AND (wft_node(v_wf, 1)).started_at IS NULL,
                      'start, root order: root 1 stays READY with no error and no started_at');
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

-- Stored task options of a promoted task node, read as Python's _build_enqueued_task_params reads them. The start
-- writes readable options; these cases change the stored options of a PENDING node after the start.

DO $$
DECLARE
    v_wf uuid := '00000000-0000-7000-8000-0000000002a1';
BEGIN
    PERFORM * FROM wft_start1(v_wf, 'fail', '[
        {"idx":0},
        {"idx":1,"deps":[0]},
        {"idx":2,"deps":[0]},
        {"idx":3,"deps":[0]},
        {"idx":4,"deps":[0]},
        {"idx":5,"deps":[0]},
        {"idx":6,"deps":[1],"allow":true}
    ]');
    UPDATE horsies_workflow_tasks
    SET task_options = CASE task_index
        WHEN 1 THEN '{"retry_policy":'
        WHEN 2 THEN '[1]'
        WHEN 3 THEN '{"retry_policy": 5}'
        WHEN 4 THEN '{"execution": "PARALLEL"}'
    END
    WHERE workflow_id = v_wf AND task_index BETWEEN 1 AND 4;
    PERFORM wft_finish(v_wf, 0, wft_ok('1'));
    PERFORM wft_check((wft_node(v_wf, 1)).status = 'FAILED' AND (wft_node(v_wf, 1)).task_id IS NULL
                      AND (wft_node(v_wf, 1)).result = horsies_task_error_envelope(
                          'WORKER_SERIALIZATION_ERROR', 'Corrupt task_options JSON for task 1',
                          '{"workflow_id":"' || v_wf || '","task_index":1}', NULL, NULL),
                      'stored options (fail): text that does not parse fails the node with Python''s error');
    PERFORM wft_check((wft_node(v_wf, 2)).status = 'FAILED' AND (wft_node(v_wf, 2)).task_id IS NULL
                      AND (wft_node(v_wf, 2)).result = horsies_task_error_envelope(
                          'WORKER_SERIALIZATION_ERROR', 'Invalid task_options payload for task 2: expected object',
                          '{"workflow_id":"' || v_wf || '","task_index":2}', NULL, NULL),
                      'stored options (fail): options that are not an object fail the node');
    PERFORM wft_check((wft_node(v_wf, 3)).status = 'FAILED' AND (wft_node(v_wf, 3)).task_id IS NULL
                      AND (wft_node(v_wf, 3)).result = horsies_task_error_envelope(
                          'WORKER_SERIALIZATION_ERROR',
                          'Invalid retry_policy for task 3: retry_policy is not an object: int',
                          '{"workflow_id":"' || v_wf || '","task_index":3}', NULL, NULL),
                      'stored options (fail): a retry_policy that does not validate fails the node');
    PERFORM wft_check((wft_node(v_wf, 4)).status = 'FAILED' AND (wft_node(v_wf, 4)).task_id IS NULL
                      AND (wft_node(v_wf, 4)).result = horsies_task_error_envelope(
                          'WORKER_SERIALIZATION_ERROR',
                          'Invalid execution for task 4: execution is not one of ISOLATED, SHARED_ASYNC, SHARED_THREAD',
                          '{"workflow_id":"' || v_wf || '","task_index":4}', NULL, NULL),
                      'stored options (fail): an execution that does not validate fails the node');
    PERFORM wft_check((wft_node(v_wf, 5)).status = 'ENQUEUED' AND wft_task_of(v_wf, 5) IS NOT NULL,
                      'stored options (fail): a sibling with readable options gets its task');
    PERFORM wft_check((wft_node(v_wf, 6)).status = 'ENQUEUED' AND wft_task_of(v_wf, 6) IS NOT NULL,
                      'stored options (fail): the failed node is a source in the same step');
    PERFORM wft_check((wft_workflow(v_wf)).status = 'RUNNING'
                      AND (wft_workflow(v_wf)).error = horsies_workflow_err_slot((wft_node(v_wf, 1)).result),
                      'stored options (fail): the workflow error is the error of the lowest failed node');
END $$;

DO $$
DECLARE
    v_wf uuid := '00000000-0000-7000-8000-0000000002a2';
    v_tasks bigint;
BEGIN
    PERFORM * FROM wft_start1(v_wf, 'pause', '[
        {"idx":0},
        {"idx":1,"deps":[0]},
        {"idx":2,"deps":[0]},
        {"idx":3,"deps":[0]}
    ]');
    UPDATE horsies_workflow_tasks SET task_options = '{"retry_policy": 5}'
    WHERE workflow_id = v_wf AND task_index IN (2, 3);
    v_tasks := (SELECT count(*) FROM horsies_tasks);
    PERFORM wft_finish(v_wf, 0, wft_ok('1'));
    PERFORM wft_check((wft_workflow(v_wf)).status = 'PAUSED'
                      AND (wft_workflow(v_wf)).error = horsies_workflow_err_slot((wft_node(v_wf, 2)).result),
                      'stored options (pause): the workflow pauses with the error of the lowest refused node');
    PERFORM wft_check((wft_node(v_wf, 2)).status = 'FAILED'
                      AND ((wft_node(v_wf, 2)).result::json -> 'err' ->> 'message')
                          = 'Invalid retry_policy for task 2: retry_policy is not an object: int',
                      'stored options (pause): only the lowest refused node FAILED');
    PERFORM wft_check((wft_node(v_wf, 1)).status = 'READY' AND (wft_node(v_wf, 1)).task_id IS NULL
                      AND (wft_node(v_wf, 3)).status = 'READY' AND (wft_node(v_wf, 3)).task_id IS NULL,
                      'stored options (pause): the other nodes of the step back to READY with no task');
    PERFORM wft_check((SELECT count(*) FROM horsies_tasks) = v_tasks - 1,
                      'stored options (pause): no task row minted (the finished root task moved to history)');
END $$;

DO $$
DECLARE
    v_wf uuid := '00000000-0000-7000-8000-0000000002a3';
BEGIN
    -- Python reads the stored options before the static input: a node with both refusals fails for its options.
    PERFORM * FROM wft_start1(v_wf, 'fail', '[
        {"idx":0},
        {"idx":1,"deps":[0],"args_from":{"k":0},"input":"{\"k\":1}"}
    ]');
    UPDATE horsies_workflow_tasks SET task_options = '{"execution": 3}' WHERE workflow_id = v_wf AND task_index = 1;
    PERFORM wft_finish(v_wf, 0, wft_ok('1'));
    PERFORM wft_check((wft_node(v_wf, 1)).status = 'FAILED'
                      AND ((wft_node(v_wf, 1)).result::json -> 'err' -> 'error_code' ->> '__builtin_task_code__')
                          = 'WORKER_SERIALIZATION_ERROR'
                      AND ((wft_node(v_wf, 1)).result::json -> 'err' ->> 'message')
                          = 'Invalid execution for task 1: execution is a JSON int, not a string',
                      'stored options: options are refused before an args_from conflict');
END $$;

DO $$
DECLARE
    v_wf uuid := '00000000-0000-7000-8000-0000000002a4';
BEGIN
    -- The minted task's good_until is the good_until member of the stored options, not the node column.
    PERFORM * FROM wft_start1(v_wf, 'fail', '[
        {"idx":0},
        {"idx":1,"deps":[0]},
        {"idx":2,"deps":[0],"good_until":"2031-01-01T00:00:00Z"},
        {"idx":3,"deps":[0]},
        {"idx":4,"deps":[0]}
    ]');
    UPDATE horsies_workflow_tasks
    SET task_options = CASE task_index
        WHEN 1 THEN '{"good_until":"2032-02-02T00:00:00+00:00"}'
        WHEN 2 THEN '{"good_until":null}'
        WHEN 3 THEN NULL
        WHEN 4 THEN ''
    END
    WHERE workflow_id = v_wf AND task_index BETWEEN 1 AND 4;
    PERFORM wft_finish(v_wf, 0, wft_ok('1'));
    PERFORM wft_check((SELECT good_until FROM horsies_tasks WHERE id = wft_task_of(v_wf, 1))
                      = '2032-02-02T00:00:00Z' AND (wft_node(v_wf, 1)).good_until IS NULL,
                      'stored deadline: the task takes the good_until of the stored options');
    PERFORM wft_check(wft_task_of(v_wf, 2) IS NOT NULL
                      AND (SELECT good_until FROM horsies_tasks WHERE id = wft_task_of(v_wf, 2)) IS NULL
                      AND (wft_node(v_wf, 2)).good_until = '2031-01-01T00:00:00Z',
                      'stored deadline: a null good_until gives no deadline whatever the node column holds');
    PERFORM wft_check(wft_task_of(v_wf, 3) IS NOT NULL
                      AND (SELECT good_until FROM horsies_tasks WHERE id = wft_task_of(v_wf, 3)) IS NULL,
                      'stored deadline: no options give no deadline');
    PERFORM wft_check((wft_node(v_wf, 4)).status = 'FAILED' AND (wft_node(v_wf, 4)).task_id IS NULL
                      AND (wft_node(v_wf, 4)).result = horsies_task_error_envelope(
                          'WORKER_SERIALIZATION_ERROR', 'Corrupt task_options JSON for task 4',
                          '{"workflow_id":"' || v_wf || '","task_index":4}', NULL, NULL),
                      'stored deadline: empty options are corrupt data, refused before a task is minted');
END $$;

DO $$
DECLARE
    v_wf uuid := '00000000-0000-7000-8000-0000000002a5';
    v_root_task uuid;
BEGIN
    -- A good_until that the database cannot read raises, as Python's insert of the text does: the step rolls back.
    PERFORM * FROM wft_start1(v_wf, 'fail', '[{"idx":0},{"idx":1,"deps":[0]},{"idx":2,"deps":[0]}]');
    v_root_task := wft_task_of(v_wf, 0);
    UPDATE horsies_workflow_tasks SET task_options = '{"good_until":"not-a-date"}'
    WHERE workflow_id = v_wf AND task_index = 1;
    PERFORM wft_refused(format('SELECT wft_finish(%L, 0, %L)', v_wf, wft_ok('1')), '22007',
                        'stored deadline: a good_until text that does not parse raises 22007');
    UPDATE horsies_workflow_tasks SET task_options = '{"good_until":12345}' WHERE workflow_id = v_wf AND task_index = 1;
    PERFORM wft_refused(format('SELECT wft_finish(%L, 0, %L)', v_wf, wft_ok('1')), '22007',
                        'stored deadline: a good_until number raises 22007');
    PERFORM wft_check((wft_node(v_wf, 1)).status = 'PENDING' AND (wft_node(v_wf, 1)).task_id IS NULL
                      AND (wft_node(v_wf, 2)).status = 'PENDING' AND (wft_node(v_wf, 2)).task_id IS NULL
                      AND (wft_node(v_wf, 0)).status = 'ENQUEUED'
                      AND EXISTS (SELECT 1 FROM horsies_tasks WHERE id = v_root_task),
                      'stored deadline: the refused step wrote nothing');
END $$;

-- Stored input and args_from of a promoted task node, read as Python's _build_enqueued_task_params reads them
-- (horsies_workflow_task_node_refusal). The start writes readable data; these cases change the stored data of PENDING
-- nodes after the start.

DO $$
DECLARE
    v_wf uuid := '00000000-0000-7000-8000-0000000002b1';
    v_case record;
BEGIN
    PERFORM * FROM wft_start1(v_wf, 'fail', (
        SELECT jsonb_agg(n) FROM (
            SELECT jsonb_build_object('idx', 0) AS n
            UNION ALL
            SELECT jsonb_build_object('idx', i, 'deps', jsonb_build_array(0)) FROM generate_series(1, 11) AS i
        ) AS nodes
    ));
    UPDATE horsies_workflow_tasks n
    SET input = c.input, args_from = c.args_from
    FROM (VALUES
        (1, 'broken{json', NULL::jsonb),
        (2, 'null', NULL),
        (3, '{"k":1', NULL),
        (4, '{"a":1,"a":2}', NULL),
        (5, '{}', '{"x":"not_an_int"}'),
        (6, '{}', '{"x":1.5}'),
        (7, '{}', '{"x":null,"y":0}'),
        (8, '{}', '"{broken"'),
        (9, '{}', '{"x":true}'),
        (10, '', NULL),
        (11, '{}', '""')
    ) AS c(task_index, input, args_from)
    WHERE n.workflow_id = v_wf AND n.task_index = c.task_index;
    PERFORM wft_finish(v_wf, 0, wft_ok('1'));
    FOR v_case IN
        SELECT * FROM (VALUES
            (1, 'Corrupt kwargs JSON for task 1'),
            (2, 'Corrupt kwargs JSON for task 2'),
            (3, 'Corrupt kwargs JSON for task 3'),
            (4, 'Corrupt kwargs JSON for task 4'),
            (5, 'args_from has non-int values for task 5'),
            (6, 'args_from has non-int values for task 6'),
            (7, 'args_from has non-int values for task 7'),
            (8, 'Corrupt args_from JSON for task 8'),
            (9, 'args_from has non-int values for task 9'),
            (10, 'Corrupt kwargs JSON for task 10'),
            (11, 'Corrupt args_from JSON for task 11')
        ) AS expected(task_index, message)
    LOOP
        PERFORM wft_check(
            (wft_node(v_wf, v_case.task_index)).status = 'FAILED'
            AND (wft_node(v_wf, v_case.task_index)).task_id IS NULL
            AND ((wft_node(v_wf, v_case.task_index)).result::json -> 'err' -> 'error_code'
                 ->> '__builtin_task_code__') = 'WORKER_SERIALIZATION_ERROR'
            AND ((wft_node(v_wf, v_case.task_index)).result::json -> 'err' ->> 'message') = v_case.message
            AND ((wft_node(v_wf, v_case.task_index)).result::json -> 'err' -> 'data')::jsonb
                = jsonb_build_object('workflow_id', v_wf::text, 'task_index', v_case.task_index),
            'stored node data: ' || v_case.message);
    END LOOP;
END $$;

DO $$
DECLARE
    v_wf uuid := '00000000-0000-7000-8000-0000000002b2';
    v_input jsonb;
BEGIN
    -- Data that Python reads with no refusal: an input that is not an object gives no static members; an args_from
    -- that is not an object gives no members; an index outside the integer range names no node; a jsonb string
    -- holding an object text is read as that object. An empty input and an empty args_from string are refused (the
    -- case above).
    PERFORM * FROM wft_start1(v_wf, 'fail', (
        SELECT jsonb_agg(n) FROM (
            SELECT jsonb_build_object('idx', 0) AS n
            UNION ALL
            SELECT jsonb_build_object('idx', i, 'deps', jsonb_build_array(0)) FROM generate_series(1, 8) AS i
        ) AS nodes
    ));
    UPDATE horsies_workflow_tasks n
    SET input = c.input, args_from = c.args_from
    FROM (VALUES
        (1, '[1,2]', '{"a":0}'::jsonb),
        (2, '7', NULL),
        (3, ' "text" ', NULL),
        (4, '{}', '{"a":99999999999,"b":-1}'),
        (5, '{}', to_jsonb('{"a":0}'::text)),
        (6, '{"k":1}', '[]'),
        (7, '{"k":1}', 'false'),
        (8, '{"k":1}', '1')
    ) AS c(task_index, input, args_from)
    WHERE n.workflow_id = v_wf AND n.task_index = c.task_index;
    PERFORM wft_finish(v_wf, 0, wft_ok('7'));
    PERFORM wft_check((SELECT bool_and(n.status = 'ENQUEUED' AND n.task_id IS NOT NULL)
                       FROM horsies_workflow_tasks n WHERE n.workflow_id = v_wf AND n.task_index BETWEEN 1 AND 8),
                      'readable node data: every node is enqueued with a task');
    v_input := (SELECT input::jsonb FROM horsies_tasks WHERE id = wft_task_of(v_wf, 1));
    PERFORM wft_check((SELECT array_agg(k ORDER BY k COLLATE "C") FROM jsonb_object_keys(v_input) AS k)
                      = ARRAY['__h_workflow_meta__', 'a']
                      AND v_input -> 'a' -> 'inner' = wft_ok('7')::jsonb,
                      'readable node data: an array input gives no static members');
    PERFORM wft_check((SELECT array_agg(k) FROM jsonb_object_keys(
                          (SELECT input::jsonb FROM horsies_tasks WHERE id = wft_task_of(v_wf, 2))) AS k)
                      = ARRAY['__h_workflow_meta__']
                      AND (SELECT array_agg(k) FROM jsonb_object_keys(
                          (SELECT input::jsonb FROM horsies_tasks WHERE id = wft_task_of(v_wf, 3))) AS k)
                      = ARRAY['__h_workflow_meta__'],
                      'readable node data: a number input and a string input give no static members');
    PERFORM wft_check((SELECT array_agg(k) FROM jsonb_object_keys(
                          (SELECT input::jsonb FROM horsies_tasks WHERE id = wft_task_of(v_wf, 4))) AS k)
                      = ARRAY['__h_workflow_meta__'],
                      'readable node data: indexes that name no node give no member');
    PERFORM wft_check((SELECT input::jsonb -> 'a' -> 'inner' FROM horsies_tasks WHERE id = wft_task_of(v_wf, 5))
                      = wft_ok('7')::jsonb,
                      'readable node data: a jsonb string holding an object text is read as that object');
    PERFORM wft_check((SELECT bool_and((SELECT array_agg(k ORDER BY k COLLATE "C") FROM jsonb_object_keys(t.input::jsonb) AS k)
                                       = ARRAY['__h_workflow_meta__', 'k'])
                       FROM horsies_tasks t
                       WHERE t.id IN (wft_task_of(v_wf, 6), wft_task_of(v_wf, 7), wft_task_of(v_wf, 8))),
                      'readable node data: an empty array, false and a number give no args_from members');
END $$;

DO $$
DECLARE
    v_wf uuid := '00000000-0000-7000-8000-0000000002b3';
BEGIN
    -- The conflict names the first args_from key in Python's order (the stored key order of the jsonb value: "b"
    -- before "aa"), quoted as Python's f-string quotes it.
    PERFORM * FROM wft_start1(v_wf, 'fail', '[{"idx":0},{"idx":1,"deps":[0]},{"idx":2,"deps":[0]}]');
    UPDATE horsies_workflow_tasks n
    SET input = c.input, args_from = c.args_from
    FROM (VALUES
        (1, '{"aa":1,"b":2}', '{"aa":0,"b":0}'::jsonb),
        (2, '{"it''s":1}', '{"it''s":0}')
    ) AS c(task_index, input, args_from)
    WHERE n.workflow_id = v_wf AND n.task_index = c.task_index;
    PERFORM wft_finish(v_wf, 0, wft_ok('1'));
    PERFORM wft_check(((wft_node(v_wf, 1)).result::json -> 'err' ->> 'message')
                      = 'args_from key ''b'' conflicts with static kwarg (task 1)',
                      'args_from key conflict: the first key in stored order');
    PERFORM wft_check(((wft_node(v_wf, 2)).result::json -> 'err' ->> 'message')
                      = 'args_from key ''it''s'' conflicts with static kwarg (task 2)',
                      'args_from key conflict: the key is quoted as Python quotes it');
END $$;

DO $$
DECLARE
    v_wf uuid := '00000000-0000-7000-8000-0000000002b4';
    v_owed horsies_workflow_step_row;
BEGIN
    -- The CHILD_START row gives a jsonb string args_from as its text, which the child build reads again as JSON.
    PERFORM * FROM wft_start1(v_wf, 'fail', '[{"idx":0},{"idx":1,"deps":[0],"sub":true}]');
    UPDATE horsies_workflow_tasks SET args_from = '"{broken"' WHERE workflow_id = v_wf AND task_index = 1;
    PERFORM wft_phase1(wft_task_of(v_wf, 0), wft_ok('1'));
    SELECT * INTO v_owed FROM horsies_workflow_node_finish(wft_task_of(v_wf, 0), TRUE, 'READ') AS r
    WHERE r.row_kind = 'CHILD_START';
    PERFORM wft_check(v_owed.args_from = '{broken', 'child start row: a jsonb string args_from as its text');
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
    v_wf uuid := '00000000-0000-7000-8000-000000000272';
    v_top uuid := '00000000-0000-7000-8000-000000000273';
    v_mid uuid := '00000000-0000-7000-8000-000000000274';
    v_child_starts bigint;
    v_named bigint;
    v_unnamed bigint;
BEGIN
    -- Phase 2 owes the child start of a promoted node: the CHILD_START row names the workflow of the node; the
    -- SOURCE and OUTCOME rows carry no name.
    PERFORM * FROM wft_start(
        jsonb_build_array(jsonb_build_object('id', v_wf, 'name', 'parent.wf', 'on_error', 'fail')),
        '[{"idx":0},{"idx":1,"deps":[0],"sub":true,"args_from":{"x":0}}]'
    );
    PERFORM wft_phase1(wft_task_of(v_wf, 0), wft_ok('5'));
    SELECT count(*) FILTER (WHERE row_kind = 'CHILD_START'),
           count(*) FILTER (WHERE row_kind = 'CHILD_START' AND workflow_name = 'parent.wf'),
           count(*) FILTER (WHERE row_kind IN ('OUTCOME', 'SOURCE') AND workflow_name IS NULL)
    INTO v_child_starts, v_named, v_unnamed
    FROM horsies_workflow_node_finish(wft_task_of(v_wf, 0), FALSE, 'READ');
    PERFORM wft_check(v_child_starts = 1 AND v_named = 1,
                      'workflow name: the CHILD_START row of a phase 2 carries the parent workflow name');
    PERFORM wft_check(v_unnamed = 2, 'workflow name: the OUTCOME and SOURCE rows carry NULL');

    -- A start owes the child start of a pre-built child's sub-workflow root: the row names the pre-built child that
    -- holds the node, not the top-level workflow.
    SELECT count(*) FILTER (WHERE row_kind = 'CHILD_START'),
           count(*) FILTER (WHERE row_kind = 'CHILD_START' AND workflow_id = v_mid AND workflow_name = 'mid.wf'),
           count(*) FILTER (WHERE row_kind = 'OUTCOME' AND workflow_name IS NULL)
    INTO v_child_starts, v_named, v_unnamed
    FROM wft_start(
        jsonb_build_array(
            jsonb_build_object('id', v_top, 'name', 'top.wf', 'on_error', 'fail'),
            jsonb_build_object('id', v_mid, 'name', 'mid.wf', 'on_error', 'fail', 'parent', v_top,
                               'parent_index', 0, 'depth', 1)),
        jsonb_build_array(
            jsonb_build_object('idx', 0, 'sub', true),
            jsonb_build_object('wf', v_mid, 'idx', 0, 'sub', true))
    );
    PERFORM wft_check(v_child_starts = 1 AND v_named = 1,
                      'workflow name: the CHILD_START row of a start names the workflow that holds the node');
    PERFORM wft_check(v_unnamed = 1, 'workflow name: the OUTCOME row of a start carries NULL');
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
    v_wf uuid := '00000000-0000-7000-8000-000000000422';
    v_task uuid;
    v_row horsies_workflow_step_row;
BEGIN
    PERFORM * FROM wft_start1(v_wf, 'fail', '[{"idx":0}]');
    v_task := wft_task_of(v_wf, 0);
    PERFORM wft_phase1(v_task, wft_ok('1'));
    UPDATE horsies_task_history SET result_envelope_version = 2 WHERE task_id = v_task;
    SELECT * INTO v_row FROM horsies_workflow_node_finish(v_task, TRUE, 'READ') WHERE row_kind = 'OUTCOME';
    PERFORM wft_check(v_row.disposition = 'SOURCE_VERSION_CONFLICT',
                      'retaining: a history source of result envelope version 2 is a version conflict');
    PERFORM wft_check((wft_node(v_wf, 0)).status = 'ENQUEUED'
                      AND EXISTS (SELECT 1 FROM horsies_workflow_phase2_pending WHERE task_id = v_task),
                      'retaining: unknown envelope version in history, node not written, record kept');
    UPDATE horsies_task_history SET result_envelope_version = 1 WHERE task_id = v_task;
    SELECT * INTO v_row FROM horsies_workflow_node_finish(v_task, TRUE, 'READ') WHERE row_kind = 'OUTCOME';
    PERFORM wft_check(v_row.disposition = 'APPLIED' AND (wft_node(v_wf, 0)).status = 'COMPLETED',
                      'the same history source of result envelope version 1 applies');
END $$;

DO $$
DECLARE
    v_wf uuid := '00000000-0000-7000-8000-000000000423';
    v_task uuid;
    v_row horsies_workflow_step_row;
BEGIN
    PERFORM * FROM wft_start1(v_wf, 'fail', '[{"idx":0}]');
    v_task := wft_task_of(v_wf, 0);
    PERFORM wft_phase1(v_task, wft_ok('1'));
    PERFORM wft_check((horsies_phase2_quarantine_one(v_task)).verdict = 'REPOINTED',
                      'quarantine source: the copy is taken');
    UPDATE horsies_workflow_phase2_quarantine SET result_envelope_version = 2 WHERE task_id = v_task;
    SELECT * INTO v_row FROM horsies_workflow_node_finish(v_task, TRUE, 'READ') WHERE row_kind = 'OUTCOME';
    PERFORM wft_check(v_row.disposition = 'SOURCE_VERSION_CONFLICT',
                      'retaining: a quarantine source of result envelope version 2 is a version conflict');
    PERFORM wft_check((wft_node(v_wf, 0)).status = 'ENQUEUED'
                      AND EXISTS (SELECT 1 FROM horsies_workflow_phase2_pending
                                  WHERE task_id = v_task AND recovery_source = 'QUARANTINE')
                      AND EXISTS (SELECT 1 FROM horsies_workflow_phase2_quarantine WHERE task_id = v_task),
                      'retaining: unknown envelope version in quarantine, node not written, record and copy kept');
    UPDATE horsies_workflow_phase2_quarantine SET result_envelope_version = 1 WHERE task_id = v_task;
    SELECT * INTO v_row FROM horsies_workflow_node_finish(v_task, TRUE, 'READ') WHERE row_kind = 'OUTCOME';
    PERFORM wft_check(v_row.disposition = 'APPLIED' AND (wft_node(v_wf, 0)).status = 'COMPLETED'
                      AND NOT EXISTS (SELECT 1 FROM horsies_workflow_phase2_quarantine WHERE task_id = v_task),
                      'the same quarantine source of result envelope version 1 applies and drops the copy');
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
    v_wf uuid := '00000000-0000-7000-8000-000000000471';
    v_form record;
    v_reader text;
    v_message text;
BEGIN
    -- A RUNNING workflow with no live node: the completion reads the stored policy. Each refused form raises XX001
    -- with the cause of Python's read_stored_success_policy, in both readers, and writes nothing.
    PERFORM * FROM wft_start1(v_wf, 'fail', '[{"idx":0}]');
    UPDATE horsies_workflow_tasks SET status = 'COMPLETED', result = wft_ok('1'), completed_at = now()
    WHERE workflow_id = v_wf AND task_index = 0;
    FOR v_form IN
        SELECT f.policy::jsonb AS policy, f.cause
        FROM (VALUES
            ('[1, 2, 3]', 'the policy is a JSON array, not an object'),
            ('"text"', 'the policy is a JSON string, not an object'),
            ('7', 'the policy is a JSON number, not an object'),
            ('true', 'the policy is a JSON boolean, not an object'),
            ('{}', 'the policy has no cases'),
            ('{"cases": "not a list"}', 'cases is a JSON string, not an array'),
            ('{"cases": null}', 'cases is a JSON null, not an array'),
            ('{"cases": [1]}', 'cases[0]: the case is a JSON number, not an object'),
            ('{"cases": [{}]}', 'cases[0]: the case has no required_indices'),
            ('{"cases": [{"required_indices": null}]}', 'cases[0]: required_indices: a JSON null, not an array'),
            ('{"cases": [{"required_indices": [0]}, {"required_indices": [true]}]}',
             'cases[1]: required_indices: item 0 is a JSON boolean, not an index'),
            ('{"cases": [{"required_indices": [0, -1]}]}', 'cases[0]: required_indices: item 1 is below 0'),
            ('{"cases": [{"required_indices": [1.0]}]}',
             'cases[0]: required_indices: item 0 is a JSON number, not an index'),
            ('{"cases": [{"required_indices": [-1.5]}]}',
             'cases[0]: required_indices: item 0 is a JSON number, not an index'),
            ('{"cases": [{"required_indices": ["0"]}]}',
             'cases[0]: required_indices: item 0 is a JSON string, not an index'),
            ('{"cases": [{"required_indices": [null]}]}',
             'cases[0]: required_indices: item 0 is a JSON null, not an index'),
            ('{"cases": [], "optional_indices": "x"}', 'optional_indices: a JSON string, not an array'),
            ('{"cases": [{"required_indices": [0]}], "optional_indices": [0, 0.5]}',
             'optional_indices: item 1 is a JSON number, not an index')
        ) AS f(policy, cause)
    LOOP
        UPDATE horsies_workflows SET success_policy = v_form.policy WHERE id = v_wf;
        FOREACH v_reader IN ARRAY ARRAY['try_complete', 'failure_error'] LOOP
            v_message := NULL;
            BEGIN
                CASE v_reader
                    WHEN 'try_complete' THEN PERFORM horsies_workflow_try_complete(v_wf);
                    WHEN 'failure_error' THEN PERFORM horsies_workflow_failure_error(v_wf, v_form.policy);
                END CASE;
            EXCEPTION
                WHEN data_corrupted THEN
                    v_message := SQLERRM;
            END;
            PERFORM wft_check(
                v_message = format('stored success_policy of workflow %s does not parse: %s', v_wf, v_form.cause),
                format('unreadable policy: %s refuses %s with %L (raised %L)', v_reader, v_form.policy,
                       v_form.cause, v_message)
            );
        END LOOP;
    END LOOP;
    PERFORM wft_check((wft_workflow(v_wf)).status = 'RUNNING' AND (wft_workflow(v_wf)).error IS NULL
                      AND (wft_workflow(v_wf)).completed_at IS NULL,
                      'unreadable policy: the workflow row is not written');
END $$;

DO $$
DECLARE
    v_wf uuid := '00000000-0000-7000-8000-000000000481';
    v_form record;
    v_row horsies_workflows;
BEGIN
    -- Node 0 COMPLETED, node 1 FAILED with the user code SECOND. Each accepted form reads as no policy or as its
    -- cases; the completion writes the expected status and error code.
    PERFORM * FROM wft_start1(v_wf, 'fail', '[{"idx":0},{"idx":1}]');
    UPDATE horsies_workflow_tasks SET status = 'COMPLETED', result = wft_ok('1'), completed_at = now()
    WHERE workflow_id = v_wf AND task_index = 0;
    UPDATE horsies_workflow_tasks SET status = 'FAILED', result = wft_err('SECOND'), completed_at = now()
    WHERE workflow_id = v_wf AND task_index = 1;
    FOR v_form IN
        SELECT f.policy::jsonb AS policy, f.status, f.code, f.name
        FROM (VALUES
            ('null', 'FAILED', 'SECOND', 'JSON null is no policy: the first FAILED node gives the error'),
            ('{"cases": [{"required_indices": [0]}], "optional_indices": null, "extra": 1}', 'COMPLETED', NULL,
             'a null optional_indices and an unknown policy key are accepted'),
            ('{"cases": [{"required_indices": [0], "extra": "x"}], "optional_indices": [1]}', 'COMPLETED', NULL,
             'an unknown case key is accepted'),
            ('{"cases": [{"required_indices": [0e0]}]}', 'COMPLETED', NULL,
             'jsonb stores 0e0 as the index 0'),
            ('{"cases": []}', 'FAILED', 'WORKFLOW_SUCCESS_CASE_NOT_MET', 'a policy with no case is not met'),
            ('{"cases": [{"required_indices": []}]}', 'FAILED', 'WORKFLOW_SUCCESS_CASE_NOT_MET',
             'a case with no required node is skipped'),
            ('{"cases": [{"required_indices": [0, 3000000000]}]}', 'FAILED', 'WORKFLOW_SUCCESS_CASE_NOT_MET',
             'an index above the integer range names no node'),
            ('{"cases": [{"required_indices": [3000000000]}, {"required_indices": [0]}]}', 'COMPLETED', NULL,
             'a case with an index above the integer range is not met; another case is'),
            ('{"cases": [{"required_indices": [1, 0]}]}', 'FAILED', 'SECOND',
             'the first FAILED required node gives the error')
        ) AS f(policy, status, code, name)
    LOOP
        UPDATE horsies_workflows
        SET status = 'RUNNING', success_policy = v_form.policy, result = NULL, error = NULL, completed_at = NULL
        WHERE id = v_wf;
        PERFORM wft_check(horsies_workflow_try_complete(v_wf) = v_form.status,
                          'accepted policy: ' || v_form.name || ' (status)');
        v_row := wft_workflow(v_wf);
        PERFORM wft_check(
            v_row.status = v_form.status
            AND coalesce(v_row.error::json -> 'error_code' ->> '__builtin_task_code__',
                         v_row.error::json ->> 'error_code') IS NOT DISTINCT FROM v_form.code,
            'accepted policy: ' || v_form.name || ' (error)'
        );
    END LOOP;
END $$;

DO $$
DECLARE
    v_wf uuid := '00000000-0000-7000-8000-000000000491';
    v_task uuid;
    v_pass integer;
    v_row record;
    v_limit_error text := horsies_task_error_object('WORKFLOW_ENQUEUE_FAILED', 'limit', NULL);
    v_limit_node text := horsies_task_error_envelope('WORKFLOW_ENQUEUE_FAILED', 'limit node', NULL, NULL, NULL);
BEGIN
    -- The phase-2 step of the last node reads a policy that does not parse: each pass raises and writes nothing,
    -- the caller counts the raise, and the recovery limit fails the workflow closed.
    PERFORM * FROM wft_start1(v_wf, 'fail', '[{"idx":0}]', NULL, '{"cases":[{"required_indices":[0]}]}');
    UPDATE horsies_workflows SET success_policy = '{"cases":[{"required_indices":["0"]}]}' WHERE id = v_wf;
    v_task := wft_task_of(v_wf, 0);
    PERFORM wft_phase1(v_task, wft_ok('1'));
    FOR v_pass IN 1..3 LOOP
        PERFORM wft_refused(format('SELECT * FROM horsies_workflow_node_finish(%L, TRUE, %L)', v_task, 'READ'),
                            'XX001', 'unreadable policy: the phase-2 step raises (pass ' || v_pass || ')');
        PERFORM wft_check((wft_node(v_wf, 0)).status = 'ENQUEUED' AND (wft_workflow(v_wf)).status = 'RUNNING'
                          AND EXISTS (SELECT 1 FROM horsies_workflow_phase2_pending WHERE task_id = v_task),
                          'unreadable policy: the raised step writes nothing (pass ' || v_pass || ')');
        SELECT * INTO v_row FROM horsies_workflow_phase2_record_raise(v_task, 3, v_limit_error, v_limit_node);
    END LOOP;
    PERFORM wft_check(v_row.outcome = 'LIMIT_REACHED' AND v_row.limit_outcome = 'LIMIT_REACHED_FAILED',
                      'unreadable policy: the third raise reaches the recovery limit');
    PERFORM wft_check((wft_node(v_wf, 0)).status = 'FAILED' AND (wft_workflow(v_wf)).status = 'FAILED'
                      AND (wft_workflow(v_wf)).error = v_limit_error,
                      'unreadable policy: the recovery limit fails the workflow closed');
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

DO $$
DECLARE
    v_wf uuid := '00000000-0000-7000-8000-000000000513';
    v_child uuid := '00000000-0000-7000-8000-000000000514';
    v_result record;
BEGIN
    PERFORM * FROM wft_start(
        jsonb_build_array(
            jsonb_build_object('id', v_wf, 'on_error', 'fail'),
            jsonb_build_object('id', v_child, 'on_error', 'fail', 'parent', v_wf, 'parent_index', 0, 'depth', 1)),
        jsonb_build_array(jsonb_build_object('idx', 0, 'sub', true), jsonb_build_object('wf', v_child, 'idx', 0))
    );
    -- A stored child error that is the JSON null is summarized as its text, as Python's reader takes the first 200
    -- characters of a stored error that does not read as a JSON value other than null.
    UPDATE horsies_workflows SET status = 'FAILED', error = 'null', completed_at = now(), updated_at = now()
    WHERE id = v_child;
    SELECT * INTO v_result FROM horsies_workflow_child_node_result(v_child);
    PERFORM wft_check(v_result.node_status = 'FAILED'
                      AND (v_result.summary::json -> 'error_summary')::text = '"null"'
                      AND (v_result.failure_error::json -> 'sub_workflow_summary' -> 'error_summary')::text = '"null"',
                      'child node result: a stored error null is summarized as the text null');
    UPDATE horsies_workflows SET error = ' null ' WHERE id = v_child;
    SELECT * INTO v_result FROM horsies_workflow_child_node_result(v_child);
    PERFORM wft_check((v_result.summary::json -> 'error_summary')::text = '" null "',
                      'child node result: a stored error null with spaces is summarized as its text');
    UPDATE horsies_workflows SET error = '7' WHERE id = v_child;
    SELECT * INTO v_result FROM horsies_workflow_child_node_result(v_child);
    PERFORM wft_check((v_result.summary::json -> 'error_summary')::text = 'null',
                      'child node result: a stored error that is JSON other than an object or null has no summary');
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
        claim_expires_at = now() + interval '1 minute', attempt_claims = 1, first_claimed_at = now()
    WHERE id = v_claimed;
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
        started_at = now(), attempt_claims = 1, first_claimed_at = now() WHERE id = v_running;
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
            ARRAY[NULL::text], ARRAY[]::uuid[], ARRAY[]::integer[], ARRAY[]::text[], 'NO_CARRIER', NULL, NULL)
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
        started_at = now(), worker_pid = 1, worker_hostname = 'host', worker_process_name = 'proc',
        attempt_claims = 1, first_claimed_at = v_claimed WHERE id = v_task;
    SELECT * INTO v_out FROM horsies_finish_workflow_success(
        v_task, 'worker-1', v_claimed, wft_ok('1'), 'NOT_OBSERVED', NULL);
    PERFORM wft_check(v_out.outcome = 'APPLIED' AND v_out.node_has_dependents
                      AND v_out.downstream_subworkflow = 'REACHABLE',
                      'phase 1 (success) returns downstream_subworkflow REACHABLE with node_has_dependents');
    v_task := wft_task_of(v_none, 0);
    UPDATE horsies_tasks SET status = 'RUNNING', claimed_at = v_claimed, claimed_by_worker_id = 'worker-1',
        started_at = now(), worker_pid = 1, worker_hostname = 'host', worker_process_name = 'proc',
        attempt_claims = 1, first_claimed_at = v_claimed WHERE id = v_task;
    SELECT * INTO v_out FROM horsies_finish_failed_attempt(
        v_task, 'worker-1', v_claimed, 'NONE', NULL, NULL, NULL, wft_err('USER'), 'USER', 'boom',
        'FAILED', 'boom', 'boom', 'REFUSE', TRUE, 'NOT_OBSERVED', NULL);
    PERFORM wft_check(v_out.outcome = 'APPLIED' AND v_out.node_has_dependents
                      AND v_out.downstream_subworkflow = 'NONE',
                      'phase 1 (failure) returns downstream_subworkflow NONE for task-only dependents');
    v_task := wft_task_of(v_leaf, 0);
    UPDATE horsies_tasks SET status = 'RUNNING', claimed_at = v_claimed, claimed_by_worker_id = 'worker-1',
        started_at = now(), worker_pid = 1, worker_hostname = 'host', worker_process_name = 'proc',
        attempt_claims = 1, first_claimed_at = v_claimed WHERE id = v_task;
    SELECT * INTO v_out FROM horsies_finish_workflow_success(
        v_task, 'worker-1', v_claimed, wft_ok('1'), 'NOT_OBSERVED', NULL);
    PERFORM wft_check(v_out.outcome = 'APPLIED' AND NOT v_out.node_has_dependents
                      AND v_out.downstream_subworkflow = 'NONE', 'phase 1 of a leaf: no dependents, NONE');
    SELECT * INTO v_out FROM horsies_finish_workflow_success(
        v_task, 'worker-1', v_claimed, wft_ok('1'), 'NOT_OBSERVED', NULL);
    PERFORM wft_check(v_out.outcome <> 'APPLIED' AND v_out.node_has_dependents IS NULL
                      AND v_out.downstream_subworkflow IS NULL, 'phase 1 miss: both facts NULL');
END $$;

-- Task rows minted through horsies_insert_task_rows -----------------------------------------------------------------

DO $$
DECLARE
    v_wf uuid := '00000000-0000-7000-8000-000000000901';
    v_two horsies_tasks;
    v_three horsies_tasks;
BEGIN
    PERFORM * FROM wft_start1(v_wf, 'fail', '[{"idx":0},{"idx":2,"deps":[0]},{"idx":3,"deps":[0]}]');
    PERFORM wft_finish(v_wf, 0, wft_ok('1'));
    SELECT * INTO v_two FROM horsies_tasks WHERE id = wft_task_of(v_wf, 2);
    SELECT * INTO v_three FROM horsies_tasks WHERE id = wft_task_of(v_wf, 3);
    PERFORM wft_check(v_two.id < v_three.id, 'node task insert: ids are minted in index order');
    PERFORM wft_check(
        v_two.status = 'PENDING' AND v_two.is_workflow_task AND v_two.command_fingerprint IS NULL
        AND v_two.command_fingerprint_version IS NULL AND v_two.idempotency_key_digest IS NULL
        AND v_two.rerun_of_task_id IS NULL AND v_two.rerun_root_task_id IS NULL AND NOT v_two.retain_rerun_input
        AND v_two.retry_count = 0 AND v_two.max_retries = 2 AND v_two.execution = 'SHARED_ASYNC'
        AND v_two.retention_class_key = 'forever' AND v_two.sent_at = '2030-01-01 00:00:00+00'
        AND v_two.enqueued_at = now() AND v_two.created_at = now() AND v_two.updated_at = now()
        AND v_two.claimed_at IS NULL,
        'node task insert: the helper writes the values of a workflow task row'
    );
    -- The cases are one message after their BEGIN, so statement_timestamp() is later than now().
    PERFORM wft_check(
        v_three.created_at = (wft_node(v_wf, 3)).started_at AND v_three.created_at < statement_timestamp(),
        'node task insert: the row time is the start of the transaction, as the started_at of the node'
    );
END $$;

DO $$
DECLARE
    v_wf uuid := '00000000-0000-7000-8000-000000000911';
    v_task uuid;
    v_tasks bigint;
BEGIN
    PERFORM * FROM wft_start1(v_wf, 'fail', '[{"idx":0},{"idx":1,"deps":[0]}]');
    v_task := wft_task_of(v_wf, 0);
    PERFORM wft_phase1(v_task, wft_ok('1'));
    v_tasks := (SELECT count(*) FROM horsies_tasks);
    -- A conflict on a minted id cannot be timed from outside one statement; a stand-in helper answers
    -- PAYLOAD_MISMATCH for one subtransaction, which rolls the stand-in back with everything else.
    BEGIN
        CREATE OR REPLACE FUNCTION horsies_insert_task_rows(
            p_ids uuid[], p_task_names text[], p_queue_names text[], p_priorities integer[], p_inputs text[],
            p_task_options text[], p_executions text[], p_retention_class_keys text[], p_retain_rerun_inputs boolean[],
            p_max_retries integer[], p_good_untils timestamp with time zone[], p_sent_ats timestamp with time zone[],
            p_enqueued_ats timestamp with time zone[], p_is_workflow_tasks boolean[],
            p_fingerprint_versions smallint[], p_fingerprints bytea[], p_key_digests bytea[],
            p_rerun_of_task_ids uuid[], p_rerun_root_task_ids uuid[], p_trace_carriers text[], p_trace_ids bytea[],
            p_row_time timestamp with time zone, p_conflict_lock text
        ) RETURNS TABLE(ordinality bigint, task_id uuid, outcome text) LANGUAGE sql AS $f$
            SELECT u.position, u.id, 'PAYLOAD_MISMATCH'::text FROM unnest(p_ids) WITH ORDINALITY AS u(id, position)
        $f$;
        PERFORM * FROM horsies_workflow_node_finish(v_task, TRUE, 'READ');
        RAISE EXCEPTION 'workflow function case failed: node task insert accepted a conflict (no error)';
    EXCEPTION
        WHEN unique_violation THEN
            NULL;
    END;
    PERFORM wft_check((wft_node(v_wf, 1)).status = 'PENDING' AND (wft_node(v_wf, 1)).task_id IS NULL
                      AND (wft_node(v_wf, 0)).status = 'ENQUEUED'
                      AND EXISTS (SELECT 1 FROM horsies_workflow_phase2_pending WHERE task_id = v_task)
                      AND (SELECT count(*) FROM horsies_tasks) = v_tasks,
                      'node task insert: a conflict on a minted id fails the step (23505), nothing written');
    PERFORM * FROM horsies_workflow_node_finish(v_task, TRUE, 'READ');
    PERFORM wft_check((wft_node(v_wf, 1)).status = 'ENQUEUED' AND wft_task_of(v_wf, 1) IS NOT NULL,
                      'node task insert: the same step applies once the helper inserts');
END $$;

-- Sub-workflow start: horsies_workflow_child_start -------------------------------------------------------------------

DO $$
DECLARE
    v_wf uuid := '00000000-0000-7000-8000-000000000921';
    v_child uuid := '00000000-0000-7000-8000-000000000922';
    v_grandchild uuid := '00000000-0000-7000-8000-000000000923';
    v_owed bigint;
    v_row horsies_workflow_step_row;
    v_started timestamptz;
BEGIN
    PERFORM * FROM wft_start1(v_wf, 'fail', '[
        {"idx":0},{"idx":1,"deps":[0],"sub":true,"params":"GIVEN","args_from":{"x":0}},{"idx":2,"deps":[1]}
    ]');
    PERFORM wft_phase1(wft_task_of(v_wf, 0), wft_ok('5'));
    SELECT count(*) FILTER (WHERE row_kind = 'CHILD_START' AND task_index = 1) INTO v_owed
    FROM horsies_workflow_node_finish(wft_task_of(v_wf, 0), FALSE, 'READ');
    PERFORM wft_check(v_owed = 1, 'child start setup: the step owes the child start of node 1');
    v_started := (wft_node(v_wf, 1)).started_at;
    SELECT * INTO v_row FROM wft_child_start(v_wf, 1,
        jsonb_build_array(
            jsonb_build_object('id', v_child, 'name', 'child', 'on_error', 'fail', 'parent', v_wf,
                               'parent_index', 1, 'depth', 1),
            jsonb_build_object('id', v_grandchild, 'on_error', 'fail', 'parent', v_child, 'parent_index', 0,
                               'depth', 2)),
        jsonb_build_array(
            jsonb_build_object('wf', v_child, 'idx', 0, 'sub', true),
            jsonb_build_object('wf', v_child, 'idx', 1, 'input', '{"y":2}'),
            jsonb_build_object('wf', v_child, 'idx', 2, 'deps', jsonb_build_array(1)),
            jsonb_build_object('wf', v_grandchild, 'idx', 0))
    ) WHERE row_kind = 'OUTCOME';
    PERFORM wft_check(v_row.disposition = 'STARTED' AND v_row.workflow_id = v_wf AND v_row.workflow_status = 'RUNNING'
                      AND v_row.advance_child_workflow_id IS NULL, 'child start TREE: STARTED');
    PERFORM wft_check((wft_node(v_wf, 1)).status = 'RUNNING' AND (wft_node(v_wf, 1)).sub_workflow_id = v_child
                      AND (wft_node(v_wf, 1)).started_at = v_started,
                      'child start TREE: the node is RUNNING, linked, and keeps its enqueue time');
    PERFORM wft_check((wft_workflow(v_child)).status = 'RUNNING' AND (wft_workflow(v_child)).parent_workflow_id = v_wf
                      AND (wft_workflow(v_child)).parent_task_index = 1 AND (wft_workflow(v_child)).depth = 1
                      AND (wft_workflow(v_child)).root_workflow_id = v_wf
                      AND (wft_workflow(v_child)).sent_at = '2030-01-02 00:00:00+00'
                      AND (wft_workflow(v_child)).name = 'child',
                      'child start TREE: the child row with its parent pair, depth, root and call time');
    PERFORM wft_check((wft_workflow(v_grandchild)).depth = 2 AND (wft_workflow(v_grandchild)).root_workflow_id = v_wf
                      AND (wft_node(v_child, 0)).status = 'RUNNING'
                      AND (wft_node(v_child, 0)).sub_workflow_id = v_grandchild,
                      'child start TREE: a pre-built grandchild is inserted and linked under the child');
    PERFORM wft_check((wft_node(v_child, 1)).status = 'ENQUEUED'
                      AND (SELECT t.sent_at = '2030-01-02 00:00:00+00'
                                  AND t.input = '{"y":2,"__h_workflow_meta__":{"workflow_id":"' || v_child
                                                || '","task_index":1,"task_name":"t1"}}'
                           FROM horsies_tasks t WHERE t.id = wft_task_of(v_child, 1))
                      AND wft_task_of(v_grandchild, 0) IS NOT NULL AND (wft_node(v_child, 2)).status = 'PENDING',
                      'child start TREE: the roots of the child tree get their task rows');
    PERFORM wft_check((wft_node(v_child, 1)).downstream_subworkflow = 'NONE'
                      AND (wft_node(v_child, 0)).sub_workflow_params = 'NOT_GIVEN',
                      'child start TREE: the node facts of the child tree are written');
    SELECT * INTO v_row FROM wft_child_start(v_wf, 1,
        jsonb_build_array(jsonb_build_object('id', '00000000-0000-7000-8000-000000000924', 'on_error', 'fail',
                                             'parent', v_wf, 'parent_index', 1, 'depth', 1)),
        '[{"idx":0}]'
    ) WHERE row_kind = 'OUTCOME';
    PERFORM wft_check(v_row.disposition = 'STALE' AND v_row.detail = 'RUNNING'
                      AND NOT EXISTS (SELECT 1 FROM horsies_workflows WHERE id = '00000000-0000-7000-8000-000000000924'),
                      'child start: a node that has its child already is STALE, nothing written');
    SELECT * INTO v_row FROM wft_child_fail(v_wf, 2, horsies_task_error_envelope('WORKFLOW_ENQUEUE_FAILED', 'x',
                                                                                 NULL, NULL, NULL))
    WHERE row_kind = 'OUTCOME';
    PERFORM wft_check(v_row.disposition = 'STALE' AND (wft_node(v_wf, 2)).status = 'PENDING',
                      'child start: a task node is STALE, nothing written');
END $$;

DO $$
DECLARE
    v_top uuid := '00000000-0000-7000-8000-000000000931';
    v_parent uuid := '00000000-0000-7000-8000-000000000932';
    v_child uuid := '00000000-0000-7000-8000-000000000933';
    v_owed bigint;
    v_row horsies_workflow_step_row;
    v_error text := horsies_task_error_envelope('SUBWORKFLOW_DEPTH_LIMIT_REACHED', 'deep', NULL, NULL, NULL);
BEGIN
    -- The start of the top workflow pre-builds the parent and owes the child start of the parent's sub-workflow
    -- root (no pre-built child, no failed record).
    SELECT count(*) FILTER (WHERE row_kind = 'CHILD_START' AND workflow_id = v_parent AND task_index = 0) INTO v_owed
    FROM wft_start(
        jsonb_build_array(
            jsonb_build_object('id', v_top, 'on_error', 'fail'),
            jsonb_build_object('id', v_parent, 'on_error', 'fail', 'parent', v_top, 'parent_index', 0, 'depth', 1)),
        jsonb_build_array(
            jsonb_build_object('idx', 0, 'sub', true),
            jsonb_build_object('wf', v_parent, 'idx', 0, 'sub', true))
    );
    PERFORM wft_check(v_owed = 1 AND (wft_node(v_parent, 0)).status = 'ENQUEUED',
                      'child start setup: the start owes the child start of a sub-workflow root');
    -- The child's only root fails (a failed record), so the child ends at once, the parent node fails, the parent
    -- completes FAILED, and the outcome names the parent for the advance of its own parent node.
    SELECT * INTO v_row FROM wft_child_start(v_parent, 0,
        jsonb_build_array(jsonb_build_object('id', v_child, 'on_error', 'fail', 'parent', v_parent,
                                             'parent_index', 0, 'depth', 2)),
        jsonb_build_array(jsonb_build_object('wf', v_child, 'idx', 0, 'sub', true)),
        jsonb_build_array(jsonb_build_object('wf', v_child, 'idx', 0, 'result', v_error))
    ) WHERE row_kind = 'OUTCOME';
    PERFORM wft_check((wft_workflow(v_child)).status = 'FAILED' AND (wft_node(v_child, 0)).result = v_error,
                      'child start TREE: a child whose root failed ends at once');
    PERFORM wft_check((wft_node(v_parent, 0)).status = 'FAILED' AND (wft_node(v_parent, 0)).sub_workflow_id = v_child
                      AND ((wft_node(v_parent, 0)).result::json -> 'err' -> 'error_code' ->> '__builtin_task_code__')
                          = 'SUBWORKFLOW_FAILED',
                      'child start TREE: the child that ended advances the node in the same statement');
    PERFORM wft_check(v_row.disposition = 'STARTED' AND v_row.workflow_status = 'FAILED'
                      AND v_row.advance_child_workflow_id = v_parent AND (wft_node(v_top, 0)).status = 'RUNNING',
                      'child start TREE: a parent that the call ended is named for its own parent advance');
END $$;

DO $$
DECLARE
    v_wf uuid := '00000000-0000-7000-8000-000000000941';
    v_child uuid := '00000000-0000-7000-8000-000000000942';
    v_rows bigint;
    v_error text := horsies_task_error_envelope('WORKFLOW_ENQUEUE_FAILED', 'grandchild build failed', NULL, NULL, NULL);
BEGIN
    PERFORM * FROM wft_start1(v_wf, 'fail', '[{"idx":0,"sub":true}]');
    -- Child: root 0 (sub) fails; node 1 (sub, allow_failed_deps) depends on it and is promoted: a child start owed
    -- in the child; root 2 is a task node. The child's on_error is fail.
    SELECT count(*) FILTER (WHERE row_kind = 'CHILD_START' AND workflow_id = v_child AND task_index = 1) INTO v_rows
    FROM wft_child_start(v_wf, 0,
        jsonb_build_array(jsonb_build_object('id', v_child, 'on_error', 'fail', 'parent', v_wf, 'parent_index', 0,
                                             'depth', 1)),
        jsonb_build_array(
            jsonb_build_object('wf', v_child, 'idx', 0, 'sub', true),
            jsonb_build_object('wf', v_child, 'idx', 1, 'sub', true, 'deps', jsonb_build_array(0), 'allow', true),
            jsonb_build_object('wf', v_child, 'idx', 2)),
        jsonb_build_array(jsonb_build_object('wf', v_child, 'idx', 0, 'result', v_error))
    );
    PERFORM wft_check(v_rows = 1 AND (wft_node(v_child, 1)).status = 'ENQUEUED'
                      AND (wft_workflow(v_child)).status = 'RUNNING' AND wft_task_of(v_child, 2) IS NOT NULL,
                      'child start TREE: a child start owed inside the child tree is returned');
END $$;

DO $$
DECLARE
    v_wf uuid := '00000000-0000-7000-8000-000000000951';
    v_child uuid := '00000000-0000-7000-8000-000000000952';
    v_error text := horsies_task_error_envelope('WORKFLOW_ENQUEUE_FAILED', 'grandchild build failed', NULL, NULL, NULL);
BEGIN
    PERFORM * FROM wft_start1(v_wf, 'fail', '[{"idx":0,"sub":true}]');
    PERFORM * FROM wft_child_start(v_wf, 0,
        jsonb_build_array(jsonb_build_object('id', v_child, 'on_error', 'pause', 'parent', v_wf, 'parent_index', 0,
                                             'depth', 1)),
        jsonb_build_array(
            jsonb_build_object('wf', v_child, 'idx', 0, 'sub', true),
            jsonb_build_object('wf', v_child, 'idx', 1)),
        jsonb_build_array(jsonb_build_object('wf', v_child, 'idx', 0, 'result', v_error))
    );
    PERFORM wft_check((wft_workflow(v_child)).status = 'PAUSED' AND (wft_node(v_child, 1)).status = 'READY'
                      AND (wft_node(v_child, 1)).task_id IS NULL AND (wft_node(v_wf, 0)).status = 'RUNNING'
                      AND (wft_node(v_wf, 0)).sub_workflow_id = v_child,
                      'child start TREE: a child paused by its own root keeps its other roots READY (the pause gate)');
END $$;

DO $$
DECLARE
    v_wf uuid := '00000000-0000-7000-8000-000000000961';
    v_row horsies_workflow_step_row;
    v_owed bigint;
    v_error text := horsies_task_error_envelope('WORKFLOW_ENQUEUE_FAILED', 'build failed',
                                                '{"workflow_id":"x","task_index":1}', NULL, NULL);
BEGIN
    PERFORM * FROM wft_start1(v_wf, 'fail', '[
        {"idx":0},{"idx":1,"deps":[0],"sub":true},{"idx":2,"deps":[1],"allow":true,"sub":true},{"idx":3,"deps":[1]}
    ]');
    PERFORM wft_phase1(wft_task_of(v_wf, 0), wft_ok('1'));
    PERFORM * FROM horsies_workflow_node_finish(wft_task_of(v_wf, 0), FALSE, 'READ');
    SELECT count(*) FILTER (WHERE row_kind = 'CHILD_START' AND task_index = 2) INTO v_owed
    FROM wft_child_fail(v_wf, 1, v_error);
    PERFORM wft_check((wft_node(v_wf, 1)).status = 'FAILED' AND (wft_node(v_wf, 1)).result = v_error
                      AND (wft_node(v_wf, 1)).completed_at IS NOT NULL AND (wft_node(v_wf, 1)).sub_workflow_id IS NULL
                      AND (wft_node(v_wf, 1)).started_at IS NOT NULL,
                      'child start FAILURE: the node is FAILED with the envelope and no child, and keeps its enqueue time');
    PERFORM wft_check((wft_workflow(v_wf)).error = (v_error::json -> 'err')::text
                      AND (wft_workflow(v_wf)).status = 'RUNNING',
                      'child start FAILURE: on_error fail stores the node error and keeps going');
    PERFORM wft_check(v_owed = 1 AND (wft_node(v_wf, 2)).status = 'ENQUEUED' AND (wft_node(v_wf, 3)).status = 'SKIPPED',
                      'child start FAILURE: the dependents are judged, and a promoted sub-workflow node is owed');
    SELECT * INTO v_row FROM wft_child_fail(v_wf, 1, v_error) WHERE row_kind = 'OUTCOME';
    PERFORM wft_check(v_row.disposition = 'STALE' AND v_row.detail = 'FAILED',
                      'child start FAILURE: a terminal node is STALE, its result kept');
END $$;

DO $$
DECLARE
    v_wf uuid := '00000000-0000-7000-8000-000000000971';
    v_row horsies_workflow_step_row;
    v_error text := horsies_task_error_envelope('WORKFLOW_ENQUEUE_FAILED', 'build failed', NULL, NULL, NULL);
BEGIN
    PERFORM * FROM wft_start1(v_wf, 'pause', '[
        {"idx":0},{"idx":1,"deps":[0],"sub":true},{"idx":2,"deps":[0],"sub":true},{"idx":3,"deps":[0]}
    ]');
    PERFORM wft_phase1(wft_task_of(v_wf, 0), wft_ok('1'));
    PERFORM * FROM horsies_workflow_node_finish(wft_task_of(v_wf, 0), FALSE, 'READ');
    PERFORM wft_check((wft_node(v_wf, 1)).status = 'ENQUEUED' AND (wft_node(v_wf, 2)).status = 'ENQUEUED',
                      'child start setup: two child starts owed');
    SELECT * INTO v_row FROM wft_child_fail(v_wf, 1, v_error) WHERE row_kind = 'OUTCOME';
    PERFORM wft_check(v_row.disposition = 'NODE_FAILED' AND v_row.workflow_status = 'PAUSED'
                      AND (wft_workflow(v_wf)).error = (v_error::json -> 'err')::text,
                      'child start FAILURE under on_error pause: the workflow is PAUSED');
    SELECT * INTO v_row FROM wft_child_start(v_wf, 2,
        jsonb_build_array(jsonb_build_object('id', '00000000-0000-7000-8000-000000000972', 'on_error', 'fail',
                                             'parent', v_wf, 'parent_index', 2, 'depth', 1)),
        '[{"wf":"00000000-0000-7000-8000-000000000972","idx":0}]'
    ) WHERE row_kind = 'OUTCOME';
    PERFORM wft_check(v_row.disposition = 'WORKFLOW_NOT_RUNNING' AND v_row.detail = 'PAUSED'
                      AND (wft_node(v_wf, 2)).status = 'READY' AND (wft_node(v_wf, 2)).started_at IS NULL
                      AND NOT EXISTS (SELECT 1 FROM horsies_workflows WHERE id = '00000000-0000-7000-8000-000000000972'),
                      'child start in a paused workflow: the node goes back to READY with no child');
    PERFORM wft_check((SELECT count(*) FROM horsies_workflow_resume(v_wf)
                       WHERE row_kind = 'CHILD_START' AND task_index = 2) = 1,
                      'child start in a paused workflow: the resume owes the child start again');
END $$;

DO $$
DECLARE
    v_wf uuid := '00000000-0000-7000-8000-000000000981';
    v_error text := horsies_task_error_envelope('WORKFLOW_ENQUEUE_FAILED', 'build failed', NULL, NULL, NULL);
BEGIN
    PERFORM * FROM wft_start1(v_wf, 'fail', '[{"idx":0,"sub":true}]');
    PERFORM wft_refused(format($q$SELECT * FROM horsies_workflow_child_start(%L, 0, 'OTHER', NULL, NULL,
        NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
        NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
        NULL, NULL, NULL, NULL, NULL, NULL, NULL)$q$, v_wf), '22023', 'child start refuses an unknown outcome kind');
    PERFORM wft_refused(format($q$SELECT * FROM wft_child_fail(%L, 0, wft_ok('1'))$q$, v_wf), '22023',
                        'child start refuses a FAILURE result that is not an error envelope');
    PERFORM wft_refused(format($q$SELECT * FROM wft_child_fail(NULL, 0, %L)$q$, v_error), '22023',
                        'child start refuses a NULL parent');
    PERFORM wft_refused(format($q$SELECT * FROM horsies_workflow_child_start(%L, 0, 'FAILURE', %L, now(),
        NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
        NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
        NULL, NULL, NULL, NULL, NULL, NULL, NULL)$q$, v_wf, v_error), '22023',
        'child start refuses a FAILURE with the arguments of a TREE');
    PERFORM wft_refused(format($q$SELECT * FROM horsies_workflow_child_start(%L, 0, 'TREE', %L, now(),
        NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
        NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL,
        NULL, NULL, NULL, NULL, NULL, NULL, NULL)$q$, v_wf, v_error), '22023',
        'child start refuses a TREE with a failure result');
    PERFORM wft_refused(format($q$SELECT * FROM wft_child_start(%L, 0,
        '[{"id":"00000000-0000-7000-8000-000000000982","on_error":"fail","parent":"%s","parent_index":1,"depth":1}]',
        '[{"idx":0}]')$q$, v_wf, v_wf), '22023', 'child start refuses a child row with another parent pair');
    PERFORM wft_refused(format($q$SELECT * FROM wft_child_start(%L, 0,
        '[{"id":"00000000-0000-7000-8000-000000000982","on_error":"fail","parent":"%s","parent_index":0,"depth":2}]',
        '[{"idx":0}]')$q$, v_wf, v_wf), '22023', 'child start refuses a child depth that is not the parent''s plus one');
    PERFORM wft_refused(format($q$SELECT * FROM wft_child_start(%L, 0,
        '[{"id":"%s","on_error":"fail","parent":"%s","parent_index":0,"depth":1}]',
        '[{"idx":0}]')$q$, v_wf, v_wf, v_wf), '23505', 'child start refuses a child id that exists (a defect)');
    PERFORM wft_check((wft_node(v_wf, 0)).status = 'ENQUEUED' AND (wft_node(v_wf, 0)).sub_workflow_id IS NULL
                      AND NOT EXISTS (SELECT 1 FROM horsies_workflows WHERE id = '00000000-0000-7000-8000-000000000982'),
                      'child start refusals write nothing');
END $$;

-- Claim-time workflow check: horsies_workflow_claimed_tasks_settle ---------------------------------------------------

DO $$
DECLARE
    v_running uuid := '00000000-0000-7000-8000-000000000b01';
    v_paused uuid := '00000000-0000-7000-8000-000000000b02';
    v_cancelled uuid := '00000000-0000-7000-8000-000000000b03';
    v_claimed timestamptz := now() - interval '1 second';
    v_run_task uuid;
    v_paused_task uuid;
    v_paused_lost uuid;
    v_cancelled_task uuid;
    v_plain uuid := '00000000-0000-7000-8000-000000000b04';
    v_result text;
BEGIN
    PERFORM * FROM wft_start1(v_running, 'fail', '[{"idx":0}]');
    PERFORM * FROM wft_start1(v_paused, 'fail', '[{"idx":0},{"idx":1}]');
    PERFORM * FROM wft_start1(v_cancelled, 'fail', '[{"idx":0},{"idx":1,"deps":[0]}]');
    v_run_task := wft_task_of(v_running, 0);
    v_paused_task := wft_task_of(v_paused, 0);
    v_paused_lost := wft_task_of(v_paused, 1);
    v_cancelled_task := wft_task_of(v_cancelled, 0);
    UPDATE horsies_workflows SET status = 'PAUSED' WHERE id = v_paused;
    UPDATE horsies_workflows SET status = 'CANCELLED', completed_at = now() WHERE id = v_cancelled;
    PERFORM wft_claim(v_run_task, v_claimed);
    PERFORM wft_claim(v_paused_task, v_claimed);
    PERFORM wft_claim(v_paused_lost, v_claimed - interval '1 second');
    PERFORM wft_claim(v_cancelled_task, v_claimed);

    SELECT string_agg(s.disposition, ',' ORDER BY s.position) INTO v_result
    FROM (SELECT r.*, row_number() OVER () AS position
          FROM horsies_workflow_claimed_tasks_settle(
              ARRAY[v_plain, v_run_task, v_paused_task, v_paused_lost, v_cancelled_task],
              ARRAY[v_claimed, v_claimed, v_claimed, v_claimed, v_claimed], 'worker-1') AS r) AS s;
    PERFORM wft_check(v_result = 'DISPATCH,DISPATCH,BLOCKED,BLOCKED,BLOCKED',
                      'settle: one disposition per input, in input order (' || coalesce(v_result, 'NULL') || ')');
    PERFORM wft_check((SELECT status FROM horsies_tasks WHERE id = v_run_task) = 'CLAIMED'
                      AND (wft_node(v_running, 0)).status = 'ENQUEUED',
                      'settle: a task of a RUNNING workflow is dispatched, nothing written');
    PERFORM wft_check(wft_moved(v_paused_task) = 'CANCELLED:PAUSE_ABANDON_CLAIM_BATCH'
                      AND (wft_node(v_paused, 0)).status = 'READY' AND (wft_node(v_paused, 0)).task_id IS NULL
                      AND (wft_node(v_paused, 0)).started_at IS NULL,
                      'settle: a claimed task of a PAUSED workflow is abandoned and its node goes back to READY');
    PERFORM wft_check((SELECT status FROM horsies_tasks WHERE id = v_paused_lost) = 'CLAIMED'
                      AND (wft_node(v_paused, 1)).status = 'ENQUEUED' AND wft_task_of(v_paused, 1) = v_paused_lost,
                      'settle: a claim of another generation is blocked and not written');
    PERFORM wft_check(wft_moved(v_cancelled_task) = 'CANCELLED:WORKFLOW_CANCEL_CLAIM_BATCH'
                      AND (wft_node(v_cancelled, 0)).status = 'SKIPPED'
                      AND (wft_node(v_cancelled, 0)).completed_at IS NOT NULL,
                      'settle: a claimed task of a CANCELLED workflow is cancelled and its node SKIPPED');
    SELECT string_agg(r.disposition, ',') INTO v_result
    FROM horsies_workflow_claimed_tasks_settle(ARRAY[]::uuid[], ARRAY[]::timestamptz[], 'worker-1') AS r;
    PERFORM wft_check(v_result IS NULL, 'settle: an empty batch returns no row');

    PERFORM wft_refused($q$SELECT * FROM horsies_workflow_claimed_tasks_settle(
        ARRAY[gen_random_uuid()], ARRAY[now(), now()], 'w')$q$, '22023', 'settle refuses arrays of different lengths');
    PERFORM wft_refused($q$SELECT * FROM horsies_workflow_claimed_tasks_settle(
        ARRAY[NULL::uuid], ARRAY[now()], 'w')$q$, '22023', 'settle refuses a NULL id');
    PERFORM wft_refused($q$SELECT * FROM horsies_workflow_claimed_tasks_settle(
        ARRAY['00000000-0000-7000-8000-000000000b09'::uuid, '00000000-0000-7000-8000-000000000b09'::uuid],
        ARRAY[now(), now()], 'w')$q$, '22023', 'settle refuses a repeated id');
    PERFORM wft_refused($q$SELECT * FROM horsies_workflow_claimed_tasks_settle(
        ARRAY[gen_random_uuid()], ARRAY[now()], NULL)$q$, '22023', 'settle refuses a NULL worker');
    PERFORM wft_refused($q$SELECT * FROM horsies_workflow_claimed_tasks_settle(
        ARRAY(SELECT gen_random_uuid() FROM generate_series(1, 1001)),
        ARRAY(SELECT now() FROM generate_series(1, 1001)), 'w')$q$, '22023', 'settle refuses more than 1000 ids');
END $$;

-- The stop before start: horsies_workflow_task_stop_before_start -----------------------------------------------------

DO $$
DECLARE
    v_wf uuid := '00000000-0000-7000-8000-000000000b11';
    v_claimed timestamptz := now() - interval '1 second';
    v_task uuid;
    v_row record;
    v_expired text := horsies_task_error_envelope('WORKFLOW_ENQUEUE_FAILED', 'stands in for TASK_EXPIRED', NULL, NULL,
                                                  NULL);
BEGIN
    PERFORM * FROM wft_start1(v_wf, 'fail', '[{"idx":0},{"idx":1},{"idx":2},{"idx":3},{"idx":4,"deps":[0]}]');

    -- Deadline passed: EXPIRED, phase 2 owed.
    v_task := wft_task_of(v_wf, 0);
    PERFORM wft_claim(v_task, v_claimed);
    UPDATE horsies_tasks SET good_until = now() - interval '1 second' WHERE id = v_task;
    SELECT * INTO v_row FROM horsies_workflow_task_stop_before_start(v_task, 'worker-1', v_claimed, v_expired, TRUE);
    PERFORM wft_check(v_row.outcome = 'EXPIRED' AND v_row.move_outcome = 'APPLIED'
                      AND wft_moved(v_task) = 'EXPIRED:EXPIRE_CLAIMED'
                      AND EXISTS (SELECT 1 FROM horsies_workflow_phase2_pending WHERE task_id = v_task),
                      'stop before start: a passed deadline expires the claim, phase 2 owed');

    -- RUNNING workflow, startable claim, linked node: RESUMED.
    v_task := wft_task_of(v_wf, 1);
    PERFORM wft_claim(v_task, v_claimed);
    SELECT * INTO v_row FROM horsies_workflow_task_stop_before_start(v_task, 'worker-1', v_claimed, v_expired, TRUE);
    PERFORM wft_check(v_row.outcome = 'RESUMED' AND v_row.move_outcome IS NULL
                      AND (SELECT status FROM horsies_tasks WHERE id = v_task) = 'CLAIMED',
                      'stop before start: a startable claim in a RUNNING workflow is RESUMED, nothing written');

    -- Another claim generation: NOT_HELD.
    SELECT * INTO v_row FROM horsies_workflow_task_stop_before_start(v_task, 'worker-1', v_claimed - interval '1 hour',
                                                                     v_expired, TRUE);
    PERFORM wft_check(v_row.outcome = 'NOT_HELD' AND v_row.move_outcome = 'LOST_CLAIM'
                      AND (SELECT status FROM horsies_tasks WHERE id = v_task) = 'CLAIMED',
                      'stop before start: a claim of another generation is NOT_HELD, LOST_CLAIM');

    -- Node not linked (a recovery limit failed it): the orphan, with and without self-heal.
    v_task := wft_task_of(v_wf, 2);
    PERFORM wft_claim(v_task, v_claimed);
    UPDATE horsies_workflow_tasks SET status = 'FAILED', completed_at = now(), result = wft_err('LIMIT')
    WHERE workflow_id = v_wf AND task_index = 2;
    SELECT * INTO v_row FROM horsies_workflow_task_stop_before_start(v_task, 'worker-1', v_claimed, v_expired, FALSE);
    PERFORM wft_check(v_row.outcome = 'NODE_NOT_LINKED' AND v_row.move_outcome IS NULL
                      AND (SELECT status FROM horsies_tasks WHERE id = v_task) = 'CLAIMED',
                      'stop before start: an unlinked claim with self-heal off is NODE_NOT_LINKED, nothing written');
    SELECT * INTO v_row FROM horsies_workflow_task_stop_before_start(v_task, 'worker-1', v_claimed, v_expired, TRUE);
    PERFORM wft_check(v_row.outcome = 'ORPHAN_CANCELLED' AND v_row.move_outcome = 'APPLIED'
                      AND wft_moved(v_task) = 'CANCELLED:CANCEL_ORPHAN' AND (wft_node(v_wf, 2)).status = 'FAILED',
                      'stop before start: an unlinked claim with self-heal on cancels the orphan');

    -- Unknown task: NOT_HELD, TASK_ABSENT.
    SELECT * INTO v_row FROM horsies_workflow_task_stop_before_start(
        '00000000-0000-7000-8000-000000000bff', 'worker-1', v_claimed, v_expired, TRUE);
    PERFORM wft_check(v_row.outcome = 'NOT_HELD' AND v_row.move_outcome = 'TASK_ABSENT',
                      'stop before start: an unknown task is NOT_HELD, TASK_ABSENT');

    -- PAUSED: the abandon.
    v_task := wft_task_of(v_wf, 3);
    PERFORM wft_claim(v_task, v_claimed);
    UPDATE horsies_workflows SET status = 'PAUSED' WHERE id = v_wf;
    SELECT * INTO v_row FROM horsies_workflow_task_stop_before_start(v_task, 'worker-1', v_claimed - interval '1 hour',
                                                                     v_expired, TRUE);
    PERFORM wft_check(v_row.outcome = 'ABANDONED' AND v_row.move_outcome = 'LOST_CLAIM'
                      AND (SELECT status FROM horsies_tasks WHERE id = v_task) = 'CLAIMED'
                      AND (wft_node(v_wf, 3)).status = 'ENQUEUED',
                      'stop before start: PAUSED with another claim generation abandons nothing');
    SELECT * INTO v_row FROM horsies_workflow_task_stop_before_start(v_task, 'worker-1', v_claimed, v_expired, TRUE);
    PERFORM wft_check(v_row.outcome = 'ABANDONED' AND v_row.move_outcome = 'APPLIED'
                      AND wft_moved(v_task) = 'CANCELLED:PAUSE_ABANDON_CLAIM'
                      AND (wft_node(v_wf, 3)).status = 'READY' AND (wft_node(v_wf, 3)).task_id IS NULL
                      AND (wft_node(v_wf, 3)).started_at IS NULL,
                      'stop before start: PAUSED abandons the claim and the node goes back to READY');
    PERFORM wft_refused(format($q$SELECT * FROM horsies_workflow_task_stop_before_start(%L, 'worker-1', now(), NULL,
                               TRUE)$q$, v_task), '22023', 'stop before start refuses a NULL argument');
END $$;

DO $$
DECLARE
    v_wf uuid := '00000000-0000-7000-8000-000000000b21';
    v_claimed timestamptz := now() - interval '1 second';
    v_held uuid;
    v_requeued uuid;
    v_row record;
BEGIN
    PERFORM * FROM wft_start1(v_wf, 'fail', '[{"idx":0},{"idx":1}]');
    v_held := wft_task_of(v_wf, 0);
    v_requeued := wft_task_of(v_wf, 1);
    PERFORM wft_claim(v_held, v_claimed);
    UPDATE horsies_workflows SET status = 'CANCELLED', completed_at = now() WHERE id = v_wf;
    SELECT * INTO v_row FROM horsies_workflow_task_stop_before_start(v_held, 'worker-1', v_claimed, '{}', TRUE);
    PERFORM wft_check(v_row.outcome = 'CANCELLED' AND v_row.move_outcome = 'APPLIED'
                      AND wft_moved(v_held) = 'CANCELLED:WORKFLOW_CANCEL_CLAIM'
                      AND (wft_node(v_wf, 0)).status = 'SKIPPED' AND (wft_node(v_wf, 0)).completed_at IS NOT NULL,
                      'stop before start: CANCELLED cancels the claim and skips the node');
    -- A row requeued to PENDING since the claim: the cancel's carve-out still cancels it.
    SELECT * INTO v_row FROM horsies_workflow_task_stop_before_start(v_requeued, 'worker-1', v_claimed, '{}', TRUE);
    PERFORM wft_check(v_row.outcome = 'CANCELLED' AND v_row.move_outcome = 'APPLIED'
                      AND wft_moved(v_requeued) = 'CANCELLED:WORKFLOW_CANCEL_CLAIM'
                      AND (wft_node(v_wf, 1)).status = 'SKIPPED',
                      'stop before start: CANCELLED also cancels a row requeued to PENDING');
END $$;

-- Paused workflow expiry: horsies_workflow_expire_paused -------------------------------------------------------------

DO $$
DECLARE
    v_wf uuid := '00000000-0000-7000-8000-000000000c01';
    v_child uuid := '00000000-0000-7000-8000-000000000c02';
    v_young uuid := '00000000-0000-7000-8000-000000000c03';
    v_error text := horsies_task_error_object('WORKFLOW_ENQUEUE_FAILED', 'stands in for WORKFLOW_EXPIRED', NULL);
    v_pending_task uuid;
    v_running_task uuid;
    v_row record;
BEGIN
    PERFORM * FROM wft_start(
        jsonb_build_array(
            jsonb_build_object('id', v_wf, 'on_error', 'fail'),
            jsonb_build_object('id', v_child, 'on_error', 'fail', 'parent', v_wf, 'parent_index', 0, 'depth', 1)),
        jsonb_build_array(
            jsonb_build_object('idx', 0, 'sub', true), jsonb_build_object('idx', 1), jsonb_build_object('idx', 2),
            jsonb_build_object('idx', 3, 'deps', jsonb_build_array(1)), jsonb_build_object('idx', 4, 'sub', true),
            jsonb_build_object('wf', v_child, 'idx', 0))
    );
    v_pending_task := wft_task_of(v_wf, 1);
    v_running_task := wft_task_of(v_wf, 2);
    UPDATE horsies_tasks SET status = 'RUNNING', claimed_at = now(), claimed_by_worker_id = 'worker-1',
        started_at = now(), attempt_claims = 1, first_claimed_at = now() WHERE id = v_running_task;
    UPDATE horsies_workflow_tasks SET status = 'RUNNING' WHERE workflow_id = v_wf AND task_index = 2;
    PERFORM horsies_workflow_pause(v_wf);
    UPDATE horsies_workflows SET updated_at = now() - interval '2 hours' WHERE id IN (v_wf, v_child);

    SELECT * INTO v_row FROM horsies_workflow_expire_paused(v_wf, 3600000000, v_error);
    PERFORM wft_check(v_row.outcome = 'EXPIRED' AND v_row.expired_task_ids = ARRAY[v_pending_task],
                      'expire paused: EXPIRED, the task behind an ENQUEUED node moved');
    PERFORM wft_check((wft_workflow(v_wf)).status = 'EXPIRED' AND (wft_workflow(v_wf)).error = v_error
                      AND (wft_workflow(v_wf)).completed_at IS NOT NULL,
                      'expire paused: the workflow is EXPIRED with the error and completed_at');
    PERFORM wft_check(wft_moved(v_pending_task) = 'CANCELLED:WORKFLOW_CANCEL_WORKFLOW'
                      AND (wft_node(v_wf, 1)).status = 'SKIPPED' AND (wft_node(v_wf, 3)).status = 'SKIPPED'
                      AND (wft_node(v_wf, 3)).completed_at IS NOT NULL AND (wft_node(v_wf, 4)).status = 'SKIPPED',
                      'expire paused: unstarted nodes SKIPPED with completed_at, the moved task''s node too');
    PERFORM wft_check((wft_node(v_wf, 2)).status = 'RUNNING'
                      AND EXISTS (SELECT 1 FROM horsies_tasks WHERE id = v_running_task),
                      'expire paused: a started task keeps its RUNNING node');
    PERFORM wft_check((wft_workflow(v_child)).status = 'PAUSED', 'expire paused: no cascade to a paused child');
    SELECT * INTO v_row FROM horsies_workflow_expire_paused(v_wf, 3600000000, v_error);
    PERFORM wft_check(v_row.outcome = 'NOT_EXPIRED' AND v_row.observed_status = 'EXPIRED',
                      'expire paused: a terminal workflow is NOT_EXPIRED');

    PERFORM * FROM wft_start1(v_young, 'fail', '[{"idx":0}]');
    PERFORM horsies_workflow_pause(v_young);
    SELECT * INTO v_row FROM horsies_workflow_expire_paused(v_young, 3600000000, v_error);
    PERFORM wft_check(v_row.outcome = 'NOT_EXPIRED' AND v_row.observed_status = 'PAUSED'
                      AND (wft_workflow(v_young)).status = 'PAUSED' AND wft_task_of(v_young, 0) IS NOT NULL,
                      'expire paused: a workflow paused for less than the age is NOT_EXPIRED, nothing written');
    PERFORM horsies_workflow_resume(v_young);
    SELECT * INTO v_row FROM horsies_workflow_expire_paused(v_young, 0, v_error);
    PERFORM wft_check(v_row.outcome = 'NOT_EXPIRED' AND v_row.observed_status = 'RUNNING',
                      'expire paused: a RUNNING workflow is NOT_EXPIRED');
    SELECT * INTO v_row FROM horsies_workflow_expire_paused('00000000-0000-7000-8000-000000000cff', 0, v_error);
    PERFORM wft_check(v_row.outcome = 'NOT_FOUND', 'expire paused: unknown id');
    PERFORM wft_refused(format($q$SELECT * FROM horsies_workflow_expire_paused(%L, -1, '{}')$q$, v_young), '22023',
                        'expire paused refuses a negative age');
    PERFORM wft_refused(format($q$SELECT * FROM horsies_workflow_expire_paused(%L, 0, NULL)$q$, v_young), '22023',
                        'expire paused refuses a NULL error');
END $$;

-- The age is exact to the microsecond: 500 us expires a workflow paused 600 us before now() and keeps one paused 400 us
-- before now(). now() is the start of this transaction, so the distances hold for every statement of the block.
DO $$
DECLARE
    v_older uuid := '00000000-0000-7000-8000-000000000c04';
    v_younger uuid := '00000000-0000-7000-8000-000000000c05';
    v_error text := horsies_task_error_object('WORKFLOW_ENQUEUE_FAILED', 'stands in for WORKFLOW_EXPIRED', NULL);
    v_row record;
BEGIN
    PERFORM * FROM wft_start1(v_older, 'fail', '[{"idx":0}]');
    PERFORM * FROM wft_start1(v_younger, 'fail', '[{"idx":0}]');
    PERFORM horsies_workflow_pause(v_older);
    PERFORM horsies_workflow_pause(v_younger);
    UPDATE horsies_workflows SET updated_at = now() - interval '600 microseconds' WHERE id = v_older;
    UPDATE horsies_workflows SET updated_at = now() - interval '400 microseconds' WHERE id = v_younger;

    SELECT * INTO v_row FROM horsies_workflow_expire_paused(v_older, 500, v_error);
    PERFORM wft_check(v_row.outcome = 'EXPIRED' AND (wft_workflow(v_older)).status = 'EXPIRED',
                      'expire paused: an age of 500 us expires a workflow paused 600 us ago');
    SELECT * INTO v_row FROM horsies_workflow_expire_paused(v_younger, 500, v_error);
    PERFORM wft_check(v_row.outcome = 'NOT_EXPIRED' AND v_row.observed_status = 'PAUSED'
                      AND (wft_workflow(v_younger)).status = 'PAUSED',
                      'expire paused: an age of 500 us keeps a workflow paused 400 us ago');
END $$;

-- An age of one day is 24 hours in a session time zone with a daylight-saving change in the last day. The zone is
-- built from now(): standard time is UTC, and daylight time (UTC+1) starts 12 hours before now(), so a day of wall
-- time before now() is 23 hours. A workflow paused 23 h 30 min ago stays PAUSED; one paused 24 h 30 min ago expires.
DO $$
DECLARE
    v_kept uuid := '00000000-0000-7000-8000-000000000c06';
    v_expired uuid := '00000000-0000-7000-8000-000000000c07';
    v_error text := horsies_task_error_object('WORKFLOW_ENQUEUE_FAILED', 'stands in for WORKFLOW_EXPIRED', NULL);
    v_one_day_us constant bigint := 86400000000;
    v_session_zone text := current_setting('TimeZone');
    v_change timestamp := (now() - interval '12 hours') AT TIME ZONE 'UTC';
    v_row record;
BEGIN
    PERFORM set_config('TimeZone', format('STD0DST,%s/%s,%s/0',
                                          extract(doy FROM v_change)::integer - 1,
                                          to_char(v_change, 'HH24:MI:SS'),
                                          (extract(doy FROM v_change)::integer - 1 + 180) % 365), true);
    PERFORM wft_check(now() - interval '1 day' = now() - interval '23 hours',
                      'expire paused across DST: the session zone has a daylight-saving change in the last day');

    PERFORM * FROM wft_start1(v_kept, 'fail', '[{"idx":0}]');
    PERFORM * FROM wft_start1(v_expired, 'fail', '[{"idx":0}]');
    PERFORM horsies_workflow_pause(v_kept);
    PERFORM horsies_workflow_pause(v_expired);
    UPDATE horsies_workflows SET updated_at = now() - interval '23 hours 30 minutes' WHERE id = v_kept;
    UPDATE horsies_workflows SET updated_at = now() - interval '24 hours 30 minutes' WHERE id = v_expired;

    SELECT * INTO v_row FROM horsies_workflow_expire_paused(v_kept, v_one_day_us, v_error);
    PERFORM wft_check(v_row.outcome = 'NOT_EXPIRED' AND v_row.observed_status = 'PAUSED'
                      AND (wft_workflow(v_kept)).status = 'PAUSED',
                      'expire paused across DST: an age of one day keeps a workflow paused 23 h 30 min ago');
    SELECT * INTO v_row FROM horsies_workflow_expire_paused(v_expired, v_one_day_us, v_error);
    PERFORM wft_check(v_row.outcome = 'EXPIRED' AND (wft_workflow(v_expired)).status = 'EXPIRED',
                      'expire paused across DST: an age of one day expires a workflow paused 24 h 30 min ago');
    PERFORM set_config('TimeZone', v_session_zone, true);
END $$;

-- Recovery cases 0, 1 and 1.5 ---------------------------------------------------------------------------------------

DO $$
DECLARE
    v_wf uuid := '00000000-0000-7000-8000-000000000c11';
    v_child uuid := '00000000-0000-7000-8000-000000000c12';
    v_row horsies_workflow_step_row;
    v_owed bigint;
BEGIN
    -- Two dependencies went terminal and both steps missed the join: node 2 is PENDING with its dependencies terminal.
    PERFORM * FROM wft_start1(v_wf, 'fail', '[
        {"idx":0},{"idx":1},{"idx":2,"deps":[0,1],"args_from":{"a":0}},{"idx":3,"deps":[0],"sub":true},
        {"idx":4,"deps":[1],"join":"any"},{"idx":5,"deps":[4]}
    ]');
    UPDATE horsies_workflow_tasks SET status = 'COMPLETED', result = wft_ok('7'), completed_at = now(), task_id = NULL
    WHERE workflow_id = v_wf AND task_index = 0;
    UPDATE horsies_workflow_tasks SET status = 'FAILED', result = wft_err('USER'), completed_at = now(), task_id = NULL
    WHERE workflow_id = v_wf AND task_index = 1;

    SELECT * INTO v_row FROM horsies_workflow_node_reevaluate(v_wf, 2) WHERE row_kind = 'OUTCOME';
    PERFORM wft_check(v_row.disposition = 'APPLIED' AND v_row.detail = 'SKIPPED' AND (wft_node(v_wf, 2)).status = 'SKIPPED',
                      'case 0: join all with a FAILED dependency is SKIPPED');
    SELECT count(*) FILTER (WHERE row_kind = 'CHILD_START' AND task_index = 3) INTO v_owed
    FROM horsies_workflow_node_reevaluate(v_wf, 3);
    PERFORM wft_check(v_owed = 1 AND (wft_node(v_wf, 3)).status = 'ENQUEUED',
                      'case 0: a met sub-workflow node owes its child start');
    SELECT * INTO v_row FROM horsies_workflow_node_reevaluate(v_wf, 4) WHERE row_kind = 'OUTCOME';
    PERFORM wft_check(v_row.disposition = 'APPLIED' AND (wft_node(v_wf, 4)).status = 'SKIPPED'
                      AND (wft_node(v_wf, 5)).status = 'SKIPPED',
                      'case 0: an impossible join any is SKIPPED, with the skip cascade');
    SELECT * INTO v_row FROM horsies_workflow_node_reevaluate(v_wf, 4) WHERE row_kind = 'OUTCOME';
    PERFORM wft_check(v_row.disposition = 'STALE' AND v_row.detail = 'SKIPPED', 'case 0: a node not PENDING is STALE');
    UPDATE horsies_workflows SET status = 'PAUSED' WHERE id = v_wf;
    SELECT * INTO v_row FROM horsies_workflow_node_reevaluate(v_wf, 2) WHERE row_kind = 'OUTCOME';
    PERFORM wft_check(v_row.disposition = 'NOT_RUNNING' AND v_row.detail = 'PAUSED', 'case 0: a PAUSED workflow');
    SELECT * INTO v_row FROM horsies_workflow_node_reevaluate('00000000-0000-7000-8000-000000000cfe', 0)
    WHERE row_kind = 'OUTCOME';
    PERFORM wft_check(v_row.disposition = 'NOT_FOUND', 'case 0: unknown workflow');
    PERFORM wft_refused($q$SELECT * FROM horsies_workflow_node_reevaluate(gen_random_uuid(), NULL)$q$, '22023',
                        'case 0 refuses a NULL task index');

    -- A child whose last PENDING node is skipped ends: the outcome names it for the parent advance.
    PERFORM * FROM wft_start(
        jsonb_build_array(
            jsonb_build_object('id', '00000000-0000-7000-8000-000000000c13', 'on_error', 'fail'),
            jsonb_build_object('id', v_child, 'on_error', 'fail', 'parent', '00000000-0000-7000-8000-000000000c13',
                               'parent_index', 0, 'depth', 1)),
        jsonb_build_array(
            jsonb_build_object('idx', 0, 'sub', true),
            jsonb_build_object('wf', v_child, 'idx', 0),
            jsonb_build_object('wf', v_child, 'idx', 1, 'deps', jsonb_build_array(0))));
    UPDATE horsies_workflow_tasks SET status = 'FAILED', result = wft_err('USER'), completed_at = now(), task_id = NULL
    WHERE workflow_id = v_child AND task_index = 0;
    SELECT * INTO v_row FROM horsies_workflow_node_reevaluate(v_child, 1) WHERE row_kind = 'OUTCOME';
    PERFORM wft_check(v_row.disposition = 'APPLIED' AND v_row.workflow_status = 'FAILED'
                      AND v_row.advance_child_workflow_id = v_child,
                      'case 0: a step that ends a child names it for the parent advance');
END $$;

DO $$
DECLARE
    v_wf uuid := '00000000-0000-7000-8000-000000000c14';
    v_lost_task uuid;
    v_row horsies_workflow_step_row;
BEGIN
    -- Root node 1 was left PENDING with no task by its start (simulated: its minted task row is gone).
    PERFORM * FROM wft_start1(v_wf, 'fail', '[{"idx":0},{"idx":1},{"idx":2,"deps":[0]}]');
    v_lost_task := wft_task_of(v_wf, 1);
    UPDATE horsies_workflow_tasks SET status = 'PENDING', task_id = NULL, started_at = NULL
    WHERE workflow_id = v_wf AND task_index = 1;
    DELETE FROM horsies_tasks WHERE id = v_lost_task;

    PERFORM wft_check(wft_finish(v_wf, 0, wft_ok('1')) = 'APPLIED', 'case 0 root: node 0 finishes');
    PERFORM wft_check((wft_node(v_wf, 2)).status = 'ENQUEUED' AND (wft_node(v_wf, 1)).status = 'PENDING'
                      AND (wft_node(v_wf, 1)).task_id IS NULL,
                      'case 0 root: a node with no dependencies is not a dependent of a finished source');

    SELECT * INTO v_row FROM horsies_workflow_node_reevaluate(v_wf, 1) WHERE row_kind = 'OUTCOME';
    PERFORM wft_check(v_row.disposition = 'APPLIED' AND v_row.detail = 'ENQUEUED'
                      AND (wft_node(v_wf, 1)).status = 'ENQUEUED' AND (wft_node(v_wf, 1)).started_at IS NOT NULL,
                      'case 0 root: a PENDING node with no dependencies is APPLIED, ENQUEUED (join all, 0 of 0)');
    PERFORM wft_check(EXISTS (SELECT 1 FROM horsies_tasks t
                              WHERE t.id = (wft_node(v_wf, 1)).task_id AND t.status = 'PENDING'
                                AND t.is_workflow_task AND t.id <> v_lost_task),
                      'case 0 root: the promotion mints the task row of the node');
    SELECT * INTO v_row FROM horsies_workflow_node_reevaluate(v_wf, 1) WHERE row_kind = 'OUTCOME';
    PERFORM wft_check(v_row.disposition = 'STALE' AND v_row.detail = 'ENQUEUED',
                      'case 0 root: a second pass finds the node ENQUEUED (STALE)');
END $$;

DO $$
DECLARE
    v_wf uuid := '00000000-0000-7000-8000-000000000c21';
    v_row horsies_workflow_step_row;
    v_owed bigint;
    v_input text;
BEGIN
    PERFORM * FROM wft_start1(v_wf, 'fail', '[
        {"idx":0},{"idx":1,"deps":[0],"args_from":{"a":0}},{"idx":2,"deps":[0],"sub":true},{"idx":3}
    ]');
    -- A crash between READY and the task insert: nodes 1 and 2 are READY with no work.
    UPDATE horsies_workflow_tasks SET status = 'COMPLETED', result = wft_ok('{"v":1}'), completed_at = now(),
        task_id = NULL
    WHERE workflow_id = v_wf AND task_index = 0;
    UPDATE horsies_workflow_tasks SET status = 'READY' WHERE workflow_id = v_wf AND task_index IN (1, 2);

    SELECT count(*) FILTER (WHERE row_kind = 'CHILD_START' AND task_index = 2),
           max(detail) FILTER (WHERE row_kind = 'OUTCOME' AND disposition = 'APPLIED')
    INTO v_owed, v_input
    FROM horsies_workflow_nodes_enqueue_ready(v_wf, ARRAY[1, 2, 3]);
    PERFORM wft_check(v_owed = 1 AND v_input = '[1,2]',
                      'cases 1 and 1.5: the READY nodes with no work move; node 3 (ENQUEUED) is left alone');
    SELECT input INTO v_input FROM horsies_tasks WHERE id = wft_task_of(v_wf, 1);
    PERFORM wft_check((wft_node(v_wf, 1)).status = 'ENQUEUED' AND v_input::jsonb -> 'a' -> 'inner' = (wft_ok('{"v":1}'))::jsonb,
                      'case 1: the task node gets its task row with the stored dependency result');
    PERFORM wft_check((wft_node(v_wf, 2)).status = 'ENQUEUED' AND (wft_node(v_wf, 2)).sub_workflow_id IS NULL,
                      'case 1.5: the sub-workflow node owes its child start');
    SELECT * INTO v_row FROM horsies_workflow_nodes_enqueue_ready(v_wf, ARRAY[1]) WHERE row_kind = 'OUTCOME';
    PERFORM wft_check(v_row.disposition = 'STALE', 'cases 1 and 1.5: a node with work is STALE');
    PERFORM wft_refused(format($q$SELECT * FROM horsies_workflow_nodes_enqueue_ready(%L, ARRAY[]::integer[])$q$, v_wf),
                        '22023', 'cases 1 and 1.5 refuse an empty index array');
    PERFORM wft_refused(format($q$SELECT * FROM horsies_workflow_nodes_enqueue_ready(%L, ARRAY[1, 1])$q$, v_wf),
                        '22023', 'cases 1 and 1.5 refuse a repeated index');
    PERFORM wft_refused(format($q$SELECT * FROM horsies_workflow_nodes_enqueue_ready(%L, ARRAY[1, NULL])$q$, v_wf),
                        '22023', 'cases 1 and 1.5 refuse a NULL index');
    UPDATE horsies_workflows SET status = 'PAUSED' WHERE id = v_wf;
    SELECT * INTO v_row FROM horsies_workflow_nodes_enqueue_ready(v_wf, ARRAY[1]) WHERE row_kind = 'OUTCOME';
    PERFORM wft_check(v_row.disposition = 'NOT_RUNNING' AND v_row.detail = 'PAUSED', 'cases 1 and 1.5: a PAUSED workflow');
    SELECT * INTO v_row FROM horsies_workflow_nodes_enqueue_ready('00000000-0000-7000-8000-000000000cfd', ARRAY[0])
    WHERE row_kind = 'OUTCOME';
    PERFORM wft_check(v_row.disposition = 'NOT_FOUND', 'cases 1 and 1.5: unknown workflow');
END $$;

-- Recovery cases 2/3 and 4: the audit page and its candidates ---------------------------------------------------------

DO $$
DECLARE
    v_done uuid := '00000000-0000-7000-8000-000000000d01';
    v_orphan uuid := '00000000-0000-7000-8000-000000000d02';
    v_live uuid := '00000000-0000-7000-8000-000000000d03';
    v_child uuid := '00000000-0000-7000-8000-000000000d05';
    v_token uuid := '00000000-0000-7000-8000-000000000dee';
    v_other uuid := '00000000-0000-7000-8000-000000000def';
    v_error text := horsies_task_error_object('WORKFLOW_ENQUEUE_FAILED', 'stands in for WORKFLOW_START_INCOMPLETE',
                                              NULL);
    v_page record;
    v_row record;
BEGIN
    -- Every RUNNING workflow of this transaction is on the page; the checks below name only these ones.
    PERFORM * FROM wft_start1(v_done, 'fail', '[{"idx":0},{"idx":1}]');
    UPDATE horsies_workflow_tasks SET status = 'COMPLETED', result = wft_ok('1'), completed_at = now()
    WHERE workflow_id = v_done;
    INSERT INTO horsies_workflows (id, name, definition_key, status, on_error, output_task_index, success_policy,
        result, error, parent_workflow_id, parent_task_index, root_workflow_id, depth, sent_at, created_at,
        started_at, completed_at, updated_at)
    VALUES (v_orphan, 'orphan', NULL, 'RUNNING', 'fail', NULL, NULL, NULL, NULL, NULL, NULL, v_orphan, 0, now(),
        now(), now(), NULL, now());
    PERFORM * FROM wft_start1(v_live, 'fail', '[{"idx":0}]');
    PERFORM * FROM wft_start(
        jsonb_build_array(
            jsonb_build_object('id', '00000000-0000-7000-8000-000000000d04', 'on_error', 'fail'),
            jsonb_build_object('id', v_child, 'on_error', 'fail', 'parent', '00000000-0000-7000-8000-000000000d04',
                               'parent_index', 0, 'depth', 1)),
        jsonb_build_array(jsonb_build_object('idx', 0, 'sub', true), jsonb_build_object('wf', v_child, 'idx', 0)));
    UPDATE horsies_workflow_tasks SET status = 'COMPLETED', result = wft_ok('2'), completed_at = now()
    WHERE workflow_id = v_child;
    UPDATE horsies_recovery_scan_cursors SET last_created_at = NULL, last_id = NULL, cycle_upper_created_at = NULL,
        cycle_upper_id = NULL, claim_token = NULL, claim_expires_at = NULL
    WHERE scan_name = 'running_workflows';

    SELECT * INTO v_page FROM horsies_workflow_recovery_audit_page(1000, v_token, 300000);
    PERFORM wft_check(v_page.outcome = 'PAGE' AND v_page.claim_token = v_token
                      AND v_page.scanned_count = (SELECT count(*) FROM horsies_workflows WHERE status = 'RUNNING'),
                      'audit page: one page holds every RUNNING workflow under the page size, claimed');
    PERFORM wft_check(v_done = ANY(v_page.completion_ids) AND v_child = ANY(v_page.completion_ids)
                      AND NOT v_live = ANY(v_page.completion_ids) AND NOT v_live = ANY(v_page.orphan_ids)
                      AND v_page.orphan_ids @> ARRAY[v_orphan]
                      AND v_page.orphan_names[array_position(v_page.orphan_ids, v_orphan)] = 'orphan',
                      'audit page: completion candidates (nodes, none live) and orphans (no node) with their names');
    PERFORM wft_check((SELECT c.claim_token = v_token AND c.claim_expires_at > now() AND c.last_id IS NULL
                              AND c.completed_cycles = 1 AND c.last_scan_rows = v_page.scanned_count
                       FROM horsies_recovery_scan_cursors c WHERE c.scan_name = 'running_workflows'),
                      'audit page: a short page ends the cycle, and the claim is stored');
    SELECT * INTO v_page FROM horsies_workflow_recovery_audit_page(1000, v_other, 300000);
    PERFORM wft_check(v_page.outcome = 'CURSOR_HELD' AND v_page.scanned_count IS NULL,
                      'audit page: a live claim holds the cursor for another reaper');

    SELECT * INTO v_row FROM horsies_workflow_recover_completion(v_done, v_other, 300000);
    PERFORM wft_check(v_row.outcome = 'AUDIT_CLAIM_LOST' AND (wft_workflow(v_done)).status = 'RUNNING',
                      'recover completion: another token is AUDIT_CLAIM_LOST, nothing done');
    SELECT * INTO v_row FROM horsies_workflow_recover_completion(v_done, v_token, 300000);
    PERFORM wft_check(v_row.outcome = 'COMPLETED' AND v_row.workflow_status = 'COMPLETED'
                      AND v_row.advance_child_workflow_id IS NULL
                      AND ((wft_workflow(v_done)).result::json ->> '__h_outputless_terminals__') = 'true',
                      'recover completion: an all-terminal workflow completes with its output');
    SELECT * INTO v_row FROM horsies_workflow_recover_completion(v_child, v_token, 300000);
    PERFORM wft_check(v_row.outcome = 'COMPLETED' AND v_row.advance_child_workflow_id = v_child,
                      'recover completion: a child that completes is named for the parent advance');
    SELECT * INTO v_row FROM horsies_workflow_recover_completion(v_live, v_token, 300000);
    PERFORM wft_check(v_row.outcome = 'UNCHANGED' AND v_row.workflow_status = 'RUNNING',
                      'recover completion: a workflow with a live node is UNCHANGED');
    SELECT * INTO v_row FROM horsies_workflow_recover_completion('00000000-0000-7000-8000-000000000dff', v_token, 300000);
    PERFORM wft_check(v_row.outcome = 'UNCHANGED' AND v_row.workflow_status IS NULL,
                      'recover completion: a workflow that is gone is UNCHANGED with no status');

    SELECT * INTO v_row FROM horsies_workflow_fail_orphaned(v_live, v_token, 300000, v_error);
    PERFORM wft_check(v_row.outcome = 'UNCHANGED' AND v_row.workflow_status = 'RUNNING'
                      AND (wft_workflow(v_live)).status = 'RUNNING',
                      'fail orphaned: a workflow with a node is UNCHANGED');
    SELECT * INTO v_row FROM horsies_workflow_fail_orphaned(v_orphan, v_token, 300000, v_error);
    PERFORM wft_check(v_row.outcome = 'FAILED' AND (wft_workflow(v_orphan)).status = 'FAILED'
                      AND (wft_workflow(v_orphan)).error = v_error AND (wft_workflow(v_orphan)).completed_at IS NOT NULL,
                      'fail orphaned: a RUNNING workflow with no node is FAILED with the error');

    UPDATE horsies_recovery_scan_cursors SET claim_expires_at = now() - interval '1 second'
    WHERE scan_name = 'running_workflows';
    SELECT * INTO v_row FROM horsies_workflow_fail_orphaned(v_live, v_token, 300000, v_error);
    PERFORM wft_check(v_row.outcome = 'AUDIT_CLAIM_LOST', 'fail orphaned: an expired claim is AUDIT_CLAIM_LOST');
    SELECT * INTO v_page FROM horsies_workflow_recovery_audit_page(1000, v_other, 300000);
    PERFORM wft_check(v_page.outcome = 'PAGE', 'audit page: an expired claim frees the cursor');

    PERFORM wft_refused($q$SELECT * FROM horsies_workflow_recovery_audit_page(0, gen_random_uuid(), 1000)$q$, '22023',
                        'audit page refuses a page size of 0');
    PERFORM wft_refused($q$SELECT * FROM horsies_workflow_recovery_audit_page(1001, gen_random_uuid(), 1000)$q$, '22023',
                        'audit page refuses a page size over 1000');
    PERFORM wft_refused($q$SELECT * FROM horsies_workflow_recovery_audit_page(10, NULL, 1000)$q$, '22023',
                        'audit page refuses a NULL token');
    PERFORM wft_refused($q$SELECT * FROM horsies_workflow_recover_completion(gen_random_uuid(), gen_random_uuid(), 0)$q$,
                        '22023', 'recover completion refuses a time to live of 0');
    PERFORM wft_refused($q$SELECT * FROM horsies_workflow_fail_orphaned(gen_random_uuid(), gen_random_uuid(), 1000,
                        NULL)$q$, '22023', 'fail orphaned refuses a NULL error');
END $$;

-- Recovery failure bound: horsies_workflow_recovery_record_raise ----------------------------------------------------

DO $$
DECLARE
    v_wf uuid := '00000000-0000-7000-8000-000000000d11';
    v_live uuid := '00000000-0000-7000-8000-000000000d12';
    v_done uuid := '00000000-0000-7000-8000-000000000d13';
    v_pass1 uuid := '00000000-0000-7000-8000-000000000d91';
    v_pass2 uuid := '00000000-0000-7000-8000-000000000d92';
    v_pass3 uuid := '00000000-0000-7000-8000-000000000d93';
    v_pass4 uuid := '00000000-0000-7000-8000-000000000d94';
    v_row record;
    v_failure horsies_workflow_recovery_failures;
    v_result json;
BEGIN
    PERFORM * FROM wft_start1(v_wf, 'fail', '[{"idx":0},{"idx":1,"deps":[0]},{"idx":2,"deps":[1]}]');
    UPDATE horsies_workflow_tasks SET status = 'COMPLETED', result = wft_ok('1'), completed_at = now(), task_id = NULL
    WHERE workflow_id = v_wf AND task_index = 0;
    -- The task of node 0 left the node: the limit then finds no live node.

    SELECT * INTO v_row FROM horsies_workflow_recovery_record_raise(v_wf, v_pass1, 'pending_terminal_deps', 1,
                                                                    'KeyError', 'boom', 3);
    SELECT * INTO v_failure FROM horsies_workflow_recovery_failures WHERE workflow_id = v_wf;
    PERFORM wft_check(v_row.outcome = 'COUNTED' AND v_row.failed_passes = 1 AND v_failure.failed_passes = 1
                      AND v_failure.last_pass_id = v_pass1 AND v_failure.last_case = 'pending_terminal_deps'
                      AND v_failure.last_task_index = 1 AND v_failure.last_error_type = 'KeyError',
                      'raise record: the first raise is COUNTED and stored');
    SELECT * INTO v_row FROM horsies_workflow_recovery_record_raise(v_wf, v_pass1, 'pending_terminal_deps', 1,
                                                                    'KeyError', 'boom', 3);
    PERFORM wft_check(v_row.outcome = 'ALREADY_COUNTED' AND v_row.failed_passes IS NULL
                      AND (SELECT failed_passes FROM horsies_workflow_recovery_failures WHERE workflow_id = v_wf) = 1,
                      'raise record: a second raise in the same pass is ALREADY_COUNTED');
    PERFORM horsies_workflow_recovery_record_raise(v_wf, v_pass2, 'pending_terminal_deps', 1, 'KeyError', 'boom', 3);
    UPDATE horsies_workflows SET status = 'PAUSED' WHERE id = v_wf;
    SELECT * INTO v_row FROM horsies_workflow_recovery_record_raise(v_wf, v_pass3, 'pending_terminal_deps', 1,
                                                                    'KeyError', 'boom', 3);
    PERFORM wft_check(v_row.outcome = 'REFUSED' AND v_row.failed_passes = 3
                      AND (SELECT failed_passes FROM horsies_workflow_recovery_failures WHERE workflow_id = v_wf) = 3
                      AND (wft_node(v_wf, 1)).status = 'PENDING' AND (wft_workflow(v_wf)).error IS NULL,
                      'raise record: at the limit a workflow that is not RUNNING refuses, the count stays');
    UPDATE horsies_workflows SET status = 'RUNNING' WHERE id = v_wf;
    SELECT * INTO v_row FROM horsies_workflow_recovery_record_raise(v_wf, v_pass4, 'pending_terminal_deps', 1,
                                                                    'KeyError', 'boom', 3);
    PERFORM wft_check(v_row.outcome = 'LIMIT_REACHED_FAILED' AND v_row.failed_passes = 4,
                      'raise record: over the limit with no live node, the workflow fails closed');
    v_result := (wft_node(v_wf, 1)).result::json;
    PERFORM wft_check(
        (wft_node(v_wf, 1)).status = 'FAILED'
        AND (v_result -> 'err' -> 'error_code' ->> '__builtin_task_code__') = 'WORKFLOW_RECOVERY_LIMIT_REACHED'
        AND (v_result -> 'err' ->> 'message')
            = 'Workflow recovery raised a persistent error in 4 passes. Last case: pending_terminal_deps. Last raise: '
              || 'KeyError: boom'
        AND (v_result -> 'err' -> 'data')::text
            = '{"workflow_id":"' || v_wf || '","case":"pending_terminal_deps","task_index":1,"failed_passes":4,'
              || '"exception_type":"KeyError"}',
        'raise record: the node of the raise stores the limit error, which names the stored count'
    );
    PERFORM wft_check((wft_node(v_wf, 2)).status = 'SKIPPED' AND (wft_workflow(v_wf)).status = 'FAILED'
                      AND (wft_workflow(v_wf)).error = (v_result -> 'err')::text,
                      'raise record: PENDING nodes SKIPPED, the workflow FAILED with the limit error');

    PERFORM * FROM wft_start1(v_live, 'fail', '[{"idx":0},{"idx":1}]');
    PERFORM horsies_workflow_recovery_record_raise(v_live, v_pass1, 'terminal_workflow_running', NULL, 'E', '', 3);
    PERFORM horsies_workflow_recovery_record_raise(v_live, v_pass2, 'terminal_workflow_running', NULL, 'E', '', 3);
    SELECT * INTO v_row FROM horsies_workflow_recovery_record_raise(v_live, v_pass3, 'terminal_workflow_running', NULL,
                                                                    'E', '', 3);
    PERFORM wft_check(v_row.outcome = 'LIMIT_REACHED_LIVE_NODES' AND (wft_workflow(v_live)).status = 'RUNNING'
                      AND ((wft_workflow(v_live)).error::json -> 'data' ->> 'task_index') IS NULL
                      AND (wft_node(v_live, 0)).status = 'ENQUEUED',
                      'raise record: with a live node the workflow keeps RUNNING with the error; no node is named');

    -- A limit write that raises: the count stays. A stand-in limit function raises for one subtransaction.
    PERFORM * FROM wft_start1(v_done, 'fail', '[{"idx":0}]');
    BEGIN
        CREATE OR REPLACE FUNCTION horsies_workflow_apply_recovery_limit(
            p_workflow_id uuid, p_task_index integer, p_limit_error text, p_limit_node_result text
        ) RETURNS text LANGUAGE plpgsql AS $f$
        BEGIN
            RAISE EXCEPTION 'limit write failed' USING ERRCODE = 'check_violation';
        END;
        $f$;
        PERFORM horsies_workflow_recovery_record_raise(v_done, v_pass1, 'ready_not_enqueued', 0, 'E', 'x', 3);
        PERFORM horsies_workflow_recovery_record_raise(v_done, v_pass2, 'ready_not_enqueued', 0, 'E', 'x', 3);
        SELECT * INTO v_row FROM horsies_workflow_recovery_record_raise(v_done, v_pass3, 'ready_not_enqueued', 0, 'E',
                                                                        'x', 3);
        PERFORM wft_check(v_row.outcome = 'LIMIT_WRITE_FAILED' AND v_row.failed_passes = 3
                          AND v_row.limit_write_error = '23514: limit write failed'
                          AND (SELECT failed_passes FROM horsies_workflow_recovery_failures WHERE workflow_id = v_done) = 3
                          AND (wft_workflow(v_done)).status = 'RUNNING',
                          'raise record: a limit write that raises keeps the count and writes nothing else');
        RAISE EXCEPTION USING ERRCODE = 'P0002', MESSAGE = 'unwind the stand-in';
    EXCEPTION
        WHEN no_data_found THEN
            NULL;
    END;

    SELECT * INTO v_row FROM horsies_workflow_recovery_record_raise('00000000-0000-7000-8000-000000000dfe', v_pass1,
                                                                    'terminal_workflow_running', NULL, 'E', '', 3);
    PERFORM wft_check(v_row.outcome = 'WORKFLOW_ABSENT', 'raise record: an unknown workflow is WORKFLOW_ABSENT');
    PERFORM wft_refused(format($q$SELECT * FROM horsies_workflow_recovery_record_raise(%L, %L, 'phase2_progression', 0,
                               'E', '', 3)$q$, v_live, v_pass4), '22023', 'raise record refuses a case it does not count');
    PERFORM wft_refused(format($q$SELECT * FROM horsies_workflow_recovery_record_raise(%L, %L, 'terminal_workflow_running',
                               0, 'E', '', 3)$q$, v_live, v_pass4), '22023',
                        'raise record refuses a node index on terminal_workflow_running');
    PERFORM wft_refused(format($q$SELECT * FROM horsies_workflow_recovery_record_raise(%L, %L, 'ready_not_enqueued',
                               NULL, 'E', '', 3)$q$, v_live, v_pass4), '22023',
                        'raise record refuses a node case with no index');
    PERFORM wft_refused(format($q$SELECT * FROM horsies_workflow_recovery_record_raise(%L, %L, 'ready_not_enqueued', 0,
                               'E', '', 2)$q$, v_live, v_pass4), '22023', 'raise record refuses a limit under 3');
    PERFORM wft_refused(format($q$SELECT * FROM horsies_workflow_recovery_record_raise(%L, %L, 'ready_not_enqueued', 0,
                               '', '', 3)$q$, v_live, v_pass4), '22023', 'raise record refuses an empty error type');
    PERFORM wft_refused(format($q$SELECT * FROM horsies_workflow_recovery_record_raise(%L, %L, 'ready_not_enqueued', 0,
                               'E', repeat('x', 501), 3)$q$, v_live, v_pass4), '22023',
                        'raise record refuses a detail over 500 characters');
    PERFORM wft_refused(format($q$SELECT * FROM horsies_workflow_recovery_record_raise(%L, NULL, 'ready_not_enqueued', 0,
                               'E', '', 3)$q$, v_live), '22023', 'raise record refuses a NULL pass id');
    PERFORM wft_check((SELECT failed_passes FROM horsies_workflow_recovery_failures WHERE workflow_id = v_live) = 3,
                      'raise record refusals write nothing');
END $$;

-- Empty stored texts --------------------------------------------------------------------------------------------------
-- No writer stores an empty text in a node result, a workflow error or node task options; NULL is the one stored form
-- of "no value". Each reader reads an empty text as corrupt data: the outcome of a text that does not read, not the
-- outcome of NULL.

DO $$
BEGIN
    PERFORM wft_check(horsies_workflow_json_or_null(NULL) IS NULL
                      AND horsies_workflow_json_or_null('') IS NULL
                      AND horsies_workflow_json_or_null('{bad') IS NULL
                      AND horsies_workflow_json_or_null('null')::text = 'null',
                      'empty text: json_or_null reads an empty text as a text that is not JSON');
END $$;

DO $$
DECLARE
    c_cause CONSTANT text := 'Stored task result is not JSON: JSON parse failed: the text is empty';
BEGIN
    PERFORM wft_check(horsies_workflow_result_kind(NULL) = 'ABSENT'
                      AND horsies_workflow_result_kind('') = 'UNREADABLE',
                      'empty text: the result kind of an empty text is UNREADABLE, of NULL ABSENT');
    PERFORM wft_check(horsies_workflow_unreadable_result_cause('') = c_cause,
                      'empty text: the cause of an empty result is the text of Python''s EmptyJsonTextError');
    PERFORM wft_check(horsies_workflow_err_slot(NULL) IS NULL
                      AND horsies_workflow_err_slot('')
                          = horsies_task_error_object('RESULT_DESERIALIZATION_ERROR', c_cause, NULL),
                      'empty text: the error of an empty result is RESULT_DESERIALIZATION_ERROR, of NULL none');
END $$;

DO $$
DECLARE
    v_wf uuid := '00000000-0000-7000-8000-000000000e01';
BEGIN
    -- The workflow error of a completion read from the first FAILED node (horsies_workflow_err_slot).
    PERFORM * FROM wft_start1(v_wf, 'fail', '[{"idx":0},{"idx":1}]');
    UPDATE horsies_workflow_tasks SET status = 'FAILED', result = '' WHERE workflow_id = v_wf AND task_index = 0;
    PERFORM wft_check(horsies_workflow_failure_error(v_wf, NULL) = horsies_task_error_object(
                          'RESULT_DESERIALIZATION_ERROR',
                          'Stored task result is not JSON: JSON parse failed: the text is empty', NULL),
                      'empty text: the failure error of a FAILED node with an empty result does not read');
    UPDATE horsies_workflow_tasks SET result = NULL WHERE workflow_id = v_wf AND task_index = 0;
    PERFORM wft_check(horsies_workflow_failure_error(v_wf, NULL) IS NULL,
                      'empty text: a FAILED node with no result gives no failure error');
END $$;

DO $$
DECLARE
    v_wf uuid := '00000000-0000-7000-8000-000000000e02';
    v_result json;
BEGIN
    PERFORM * FROM wft_start1(v_wf, 'fail', '[{"idx":0},{"idx":1},{"idx":2}]');
    UPDATE horsies_workflow_tasks
    SET status = 'COMPLETED', result = CASE task_index WHEN 0 THEN '' WHEN 1 THEN wft_ok('1') END
    WHERE workflow_id = v_wf;
    v_result := horsies_workflow_outputless_result(v_wf)::json;
    PERFORM wft_check(json_typeof(v_result -> 'ok' -> 'results_by_id' -> 'n0') = 'null'
                      AND (v_result -> 'ok' -> 'results_by_id' -> 'n1')::jsonb = (wft_ok('1'))::jsonb
                      AND json_typeof(v_result -> 'ok' -> 'results_by_id' -> 'n2') = 'null',
                      'empty text: an empty terminal result is the entry null of a result that is not an envelope');
END $$;

DO $$
DECLARE
    v_wf uuid := '00000000-0000-7000-8000-000000000e03';
    v_empty text;
    v_null text;
BEGIN
    PERFORM * FROM wft_start1(v_wf, 'fail', '[{"idx":0}]', 0);
    UPDATE horsies_workflow_tasks SET status = 'COMPLETED', result = '' WHERE workflow_id = v_wf AND task_index = 0;
    v_empty := horsies_workflow_explicit_output(v_wf, 0);
    UPDATE horsies_workflow_tasks SET result = NULL WHERE workflow_id = v_wf AND task_index = 0;
    v_null := horsies_workflow_explicit_output(v_wf, 0);
    PERFORM wft_check(v_empty = horsies_task_error_envelope(
                          'RESULT_DESERIALIZATION_ERROR',
                          format('Workflow %s is COMPLETED and its output node ''n0'' is COMPLETED with no stored result',
                                 v_wf),
                          horsies_workflow_output_error_data(v_wf, 'n0', 0, 't0'), NULL, NULL)
                      AND v_empty = v_null,
                      'empty text: an empty output result fails closed with the entry of a missing result');
END $$;

DO $$
BEGIN
    PERFORM wft_check(horsies_workflow_source_value('COMPLETED', '', 3, 'n3', 't3') = horsies_task_error_envelope(
                          'RESULT_DESERIALIZATION_ERROR',
                          'Dependency node n3 (index 3, task t3) is COMPLETED with no stored result',
                          '{"dependency_index":3}', NULL, NULL)
                      AND horsies_workflow_source_value('FAILED', '', 3, 'n3', 't3')
                          = horsies_workflow_source_value('FAILED', NULL, 3, 'n3', 't3'),
                      'empty text: an empty source result fails closed with the entry of a missing result');
END $$;

DO $$
DECLARE
    v_wf uuid := '00000000-0000-7000-8000-000000000e04';
    v_child uuid := '00000000-0000-7000-8000-000000000e05';
    v_result record;
BEGIN
    PERFORM * FROM wft_start(
        jsonb_build_array(
            jsonb_build_object('id', v_wf, 'on_error', 'fail'),
            jsonb_build_object('id', v_child, 'on_error', 'fail', 'parent', v_wf, 'parent_index', 0, 'depth', 1)),
        jsonb_build_array(jsonb_build_object('idx', 0, 'sub', true), jsonb_build_object('wf', v_child, 'idx', 0))
    );
    UPDATE horsies_workflows SET status = 'FAILED', error = '', completed_at = now(), updated_at = now()
    WHERE id = v_child;
    SELECT * INTO v_result FROM horsies_workflow_child_node_result(v_child);
    PERFORM wft_check(v_result.node_status = 'FAILED'
                      AND (v_result.summary::json -> 'error_summary')::text = '""'
                      AND (v_result.failure_error::json -> 'sub_workflow_summary' -> 'error_summary')::text = '""',
                      'empty text: an empty child error is summarized as its text, as a text that is not JSON');
    UPDATE horsies_workflows SET error = NULL WHERE id = v_child;
    SELECT * INTO v_result FROM horsies_workflow_child_node_result(v_child);
    PERFORM wft_check((v_result.summary::json -> 'error_summary')::text = 'null',
                      'empty text: a NULL child error has no summary');
    UPDATE horsies_workflows SET status = 'COMPLETED', result = '' WHERE id = v_child;
    SELECT * INTO v_result FROM horsies_workflow_child_node_result(v_child);
    PERFORM wft_check(v_result.node_status = 'FAILED'
                      AND v_result.node_result = horsies_task_error_envelope(
                          'RESULT_DESERIALIZATION_ERROR',
                          format('Child workflow %s is COMPLETED and its stored result does not parse', v_child),
                          '{"workflow_id":"' || v_child || '"}', NULL, NULL),
                      'empty text: a COMPLETED child with an empty result fails the parent node');
END $$;

DO $$
BEGIN
    PERFORM wft_check(horsies_workflow_task_options_refusal(NULL, 4) IS NULL
                      AND horsies_workflow_task_options_refusal('', 4) = 'Corrupt task_options JSON for task 4',
                      'empty text: empty node task options do not read; NULL options are no options');
END $$;

-- The output node at completion ------------------------------------------------------------------------------------
-- An absent output row, or an output node that is not terminal, is a defect of the caller, which counted no live
-- node: horsies_workflow_explicit_output raises data_corrupted (Python clients/python/tests/unit/test_workflow_output_skipped.py,
-- TestExplicitOutputResult).

DO $$
DECLARE
    v_wf uuid := '00000000-0000-7000-8000-000000000e06';
BEGIN
    PERFORM * FROM wft_start1(v_wf, 'fail', '[{"idx":0}]', 0);
    PERFORM wft_check((wft_node(v_wf, 0)).status = 'ENQUEUED', 'output node raise setup: the output node is live');
    PERFORM wft_refused(format('SELECT horsies_workflow_explicit_output(%L, 0)', v_wf), 'XX001',
                        'output node: a live output node raises data_corrupted');
    PERFORM wft_refused(format('SELECT horsies_workflow_explicit_output(%L, 5)', v_wf), 'XX001',
                        'output node: an absent output row raises data_corrupted');
END $$;

-- A stored error with a code and no message -----------------------------------------------------------------------
-- message is for a human: each reader takes an error with a code and no message as that error, as stored (Python
-- clients/python/tests/unit/test_task_error_stored_decode.py, TestErrorWithNoMessage).

DO $$
DECLARE
    v_wf uuid := '00000000-0000-7000-8000-000000000e07';
    v_child uuid := '00000000-0000-7000-8000-000000000e08';
    v_slot text;
    v_envelope text;
    v_result record;
BEGIN
    PERFORM * FROM wft_start(
        jsonb_build_array(
            jsonb_build_object('id', v_wf, 'on_error', 'fail'),
            jsonb_build_object('id', v_child, 'on_error', 'fail', 'parent', v_wf, 'parent_index', 0, 'depth', 1)),
        jsonb_build_array(jsonb_build_object('idx', 0, 'sub', true), jsonb_build_object('idx', 1),
                          jsonb_build_object('wf', v_child, 'idx', 0))
    );
    FOREACH v_slot IN ARRAY ARRAY[
        '{"error_code":"RATE_LIMITED","message":null,"data":null,"exception":null}',
        '{"error_code":"RATE_LIMITED","message":null}',
        '{"error_code":"RATE_LIMITED"}'
    ] LOOP
        v_envelope := '{"__h_task_result__":true,"ok":null,"err":' || v_slot || '}';
        PERFORM wft_check(horsies_workflow_result_kind(v_envelope) = 'ERROR'
                          AND horsies_workflow_err_slot(v_envelope) = v_slot,
                          'code-only error: the err slot is the stored error with its code: ' || v_slot);
        PERFORM wft_check(horsies_workflow_source_value('FAILED', v_envelope, 1, 'n1', 't1') = v_envelope,
                          'code-only error: a dependency gets the stored error: ' || v_slot);
        UPDATE horsies_workflow_tasks SET status = 'FAILED', result = v_envelope
        WHERE workflow_id = v_wf AND task_index = 1;
        PERFORM wft_check(horsies_workflow_failure_error(v_wf, NULL) = v_slot
                          AND horsies_workflow_failure_error(v_wf, '{"cases":[{"required_indices":[1]}]}') = v_slot,
                          'code-only error: the workflow error is the stored error: ' || v_slot);
        UPDATE horsies_workflows
        SET status = 'COMPLETED', result = v_envelope, completed_at = now(), updated_at = now()
        WHERE id = v_child;
        SELECT * INTO v_result FROM horsies_workflow_child_node_result(v_child);
        PERFORM wft_check(v_result.node_status = 'FAILED' AND v_result.node_result = v_envelope
                          AND v_result.failure_error = v_slot,
                          'code-only error: a child output with the error fails the parent node with it: ' || v_slot);
    END LOOP;
END $$;

-- A required FAILED node whose stored result does not read ----------------------------------------------------------
-- The workflow error under a success policy names the cause, as with no policy (Python
-- clients/python/tests/unit/test_task_error_stored_decode.py, test_the_workflow_error_of_a_required_task_names_the_cause).

DO $$
DECLARE
    v_wf uuid := '00000000-0000-7000-8000-000000000e09';
    v_stored text;
    v_error json;
BEGIN
    PERFORM * FROM wft_start1(v_wf, 'fail', '[{"idx":0},{"idx":1}]');
    FOREACH v_stored IN ARRAY ARRAY[
        'not json',
        '[1]',
        '{"__h_task_result__":true,"ok":null,"err":{"message":"no code"}}'
    ] LOOP
        UPDATE horsies_workflow_tasks SET status = 'FAILED', result = v_stored
        WHERE workflow_id = v_wf AND task_index = 1;
        v_error := horsies_workflow_failure_error(v_wf, '{"cases":[{"required_indices":[1]}]}')::json;
        PERFORM wft_check((v_error -> 'error_code' ->> '__builtin_task_code__') = 'RESULT_DESERIALIZATION_ERROR'
                          AND (v_error ->> 'message') = horsies_workflow_unreadable_result_cause(v_stored)
                          AND v_error::text = horsies_workflow_failure_error(v_wf, NULL),
                          'unreadable result: the workflow error of a required FAILED node names the cause: '
                          || v_stored);
    END LOOP;
END $$;

-- Trace pair: start, promotion, child start, moves --------------------------------------------------------------------

DO $$
DECLARE
    v_wf uuid := '00000000-0000-7000-8000-000000000fa1';
    v_child uuid := '00000000-0000-7000-8000-000000000fa2';
    v_carrier text := '{"traceparent":"00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"}';
    v_trace_id bytea := decode('0af7651916cd43dd8448eb211c80319c', 'hex');
    v_root_task uuid;
BEGIN
    -- Root 0 has a pre-built child; node 1 depends on root 2, so a later promotion mints its task row.
    PERFORM * FROM wft_start(
        jsonb_build_array(
            jsonb_build_object('id', v_wf, 'on_error', 'fail'),
            jsonb_build_object('id', v_child, 'on_error', 'fail', 'parent', v_wf, 'parent_index', 0, 'depth', 1)),
        jsonb_build_array(
            jsonb_build_object('idx', 0, 'sub', true),
            jsonb_build_object('idx', 1, 'deps', jsonb_build_array(2)),
            jsonb_build_object('idx', 2),
            jsonb_build_object('wf', v_child, 'idx', 0)),
        '[]', v_carrier, v_trace_id
    );
    PERFORM wft_check((wft_workflow(v_wf)).trace_carrier = v_carrier AND (wft_workflow(v_wf)).trace_id = v_trace_id
                      AND (wft_workflow(v_child)).trace_carrier = v_carrier
                      AND (wft_workflow(v_child)).trace_id = v_trace_id,
                      'trace: every workflow row of a started tree stores the pair of the start');
    PERFORM wft_check((SELECT count(*) FROM horsies_tasks
                       WHERE id IN (wft_task_of(v_wf, 2), wft_task_of(v_child, 0))
                         AND trace_carrier = v_carrier AND trace_id = v_trace_id) = 2,
                      'trace: the task rows of the roots copy the pair of their workflow');
    v_root_task := wft_task_of(v_wf, 2);
    PERFORM wft_finish(v_wf, 2, wft_ok('1'));
    PERFORM wft_check((SELECT trace_carrier = v_carrier AND trace_id = v_trace_id FROM horsies_tasks
                       WHERE id = wft_task_of(v_wf, 1)),
                      'trace: a task row that a later promotion mints copies the pair of its workflow');
    PERFORM wft_check((SELECT trace_id = v_trace_id FROM horsies_task_history WHERE task_id = v_root_task),
                      'trace: the move of one row copies the trace id to history');
END $$;

DO $$
DECLARE
    v_wf uuid := '00000000-0000-7000-8000-000000000fa3';
    v_child uuid := '00000000-0000-7000-8000-000000000fa4';
    v_carrier text := '{"b3":"80f198ee56343ba864fe8b2a57d3eff7-e457b5a2e4d86bd1-1"}';
    v_trace_id bytea := decode('80f198ee56343ba864fe8b2a57d3eff7', 'hex');
BEGIN
    -- Node 1 is a sub-workflow node behind root 0: its child starts in the step that owes it, with no trace argument.
    PERFORM * FROM wft_start(
        jsonb_build_array(jsonb_build_object('id', v_wf, 'on_error', 'fail')),
        '[{"idx":0},{"idx":1,"deps":[0],"sub":true}]', '[]', v_carrier, v_trace_id
    );
    PERFORM wft_finish(v_wf, 0, wft_ok('1'));
    PERFORM * FROM wft_child_start(v_wf, 1,
        jsonb_build_array(
            jsonb_build_object('id', v_child, 'on_error', 'fail', 'parent', v_wf, 'parent_index', 1, 'depth', 1)),
        jsonb_build_array(jsonb_build_object('wf', v_child, 'idx', 0))
    );
    PERFORM wft_check((wft_workflow(v_child)).trace_carrier = v_carrier
                      AND (wft_workflow(v_child)).trace_id = v_trace_id,
                      'trace: a deferred child start copies the pair of its parent workflow');
    PERFORM wft_check((SELECT trace_carrier = v_carrier AND trace_id = v_trace_id FROM horsies_tasks
                       WHERE id = wft_task_of(v_child, 0)),
                      'trace: the task rows of a deferred child copy the pair');
END $$;

DO $$
DECLARE
    v_wf uuid := '00000000-0000-7000-8000-000000000fa5';
    v_carrier text := '{"traceparent":"00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"}';
    v_trace_id bytea := decode('4bf92f3577b34da6a3ce929d0e0e4736', 'hex');
    v_tasks uuid[];
    v_row record;
BEGIN
    PERFORM * FROM wft_start(
        jsonb_build_array(jsonb_build_object('id', v_wf, 'on_error', 'fail')), '[{"idx":0},{"idx":1}]', '[]',
        v_carrier, v_trace_id
    );
    v_tasks := ARRAY[wft_task_of(v_wf, 0), wft_task_of(v_wf, 1)];
    SELECT * INTO v_row FROM horsies_workflow_cancel(v_wf);
    PERFORM wft_check(v_row.outcome = 'CANCELLED', 'trace: the workflow is cancelled');
    PERFORM wft_check((SELECT count(*) FROM horsies_task_history
                       WHERE task_id = ANY(v_tasks) AND trace_id = v_trace_id) = 2,
                      'trace: the batch move copies the trace id of each row to history');
END $$;

DO $$
DECLARE
    v_wf uuid := '00000000-0000-7000-8000-000000000fa6';
BEGIN
    PERFORM * FROM wft_start1(v_wf, 'fail', '[{"idx":0}]');
    PERFORM wft_check((wft_workflow(v_wf)).trace_carrier IS NULL AND (wft_workflow(v_wf)).trace_id IS NULL
                      AND (SELECT trace_carrier IS NULL AND trace_id IS NULL FROM horsies_tasks
                           WHERE id = wft_task_of(v_wf, 0)),
                      'trace: a start with no carrier stores no pair, and its task rows hold none');
    PERFORM wft_refused($q$
        UPDATE horsies_workflows SET trace_carrier = '{}' WHERE id = '00000000-0000-7000-8000-000000000fa6'
    $q$, '23514', 'trace: a workflow row refuses a carrier with no trace id (table CHECK)');
    PERFORM wft_refused($q$
        UPDATE horsies_workflows SET trace_carrier = '{}', trace_id = '\x00'::bytea
        WHERE id = '00000000-0000-7000-8000-000000000fa6'
    $q$, '23514', 'trace: a workflow row refuses a trace id that is not 16 bytes (table CHECK)');
END $$;

DO $$
BEGIN
    PERFORM wft_refused($q$
        SELECT * FROM wft_start_traced('00000000-0000-7000-8000-000000000fa7', 'MAYBE', NULL, NULL)
    $q$, '22023', 'trace: start refuses an unknown trace kind');
    PERFORM wft_refused($q$
        SELECT * FROM wft_start_traced('00000000-0000-7000-8000-000000000fa7', 'NO_CARRIER', '{}', NULL)
    $q$, '22023', 'trace: start refuses a carrier text with NO_CARRIER');
    PERFORM wft_refused($q$
        SELECT * FROM wft_start_traced('00000000-0000-7000-8000-000000000fa7', 'CARRIER', '{}', NULL)
    $q$, '22023', 'trace: start refuses CARRIER with no trace id');
    PERFORM wft_refused($q$
        SELECT * FROM wft_start_traced('00000000-0000-7000-8000-000000000fa7', 'CARRIER', '{}',
                                       decode('0af7651916cd43dd8448eb211c8031', 'hex'))
    $q$, '22023', 'trace: start refuses a trace id that is not 16 bytes');
    PERFORM wft_check(NOT EXISTS (SELECT 1 FROM horsies_workflows WHERE id = '00000000-0000-7000-8000-000000000fa7'),
                      'trace: start refusals write nothing');
END $$;
