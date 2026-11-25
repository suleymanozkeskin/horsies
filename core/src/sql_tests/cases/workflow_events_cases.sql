-- Cases for the workflow events of migration 1001 (core/migrations/1001_lifecycle_record.sql), one session.
--
-- Run on a database that holds the migration chain and the workflow fixtures (baseline_workflow_fixtures.sql), in one
-- transaction that the caller rolls back (core/src/sql_tests/workflow_events_sql.rs). Each case prints
-- "ok: <case>"; a failed case raises and stops the script. A statement does not see the writes of a function that it
-- calls, so each check of the events reads them in a statement after the call.

SET client_min_messages = notice;

------------------------------------------------------------------------------------------------------------------------
-- Fixtures
------------------------------------------------------------------------------------------------------------------------

CREATE FUNCTION we_check(p_case text, p_ok boolean) RETURNS void LANGUAGE plpgsql AS $$
BEGIN
    IF p_ok IS NOT TRUE THEN
        RAISE EXCEPTION 'case failed: %', p_case USING ERRCODE = 'assert_failure';
    END IF;
    RAISE NOTICE 'ok: %', p_case;
END
$$;

-- Runs p_statement in a subtransaction and checks that it fails with p_sqlstate on the constraint p_constraint. The
-- subtransaction rolls the statement back.
CREATE FUNCTION we_expect_refusal(p_case text, p_statement text, p_sqlstate text, p_constraint text) RETURNS void
LANGUAGE plpgsql AS $$
DECLARE
    v_constraint text;
BEGIN
    BEGIN
        EXECUTE p_statement;
    EXCEPTION WHEN OTHERS THEN
        GET STACKED DIAGNOSTICS v_constraint = CONSTRAINT_NAME;
        IF SQLSTATE <> p_sqlstate OR v_constraint IS DISTINCT FROM p_constraint THEN
            RAISE EXCEPTION 'case failed: %: expected % on %, got % on % (%)', p_case, p_sqlstate, p_constraint,
                SQLSTATE, v_constraint, SQLERRM USING ERRCODE = 'assert_failure';
        END IF;
        RAISE NOTICE 'ok: %', p_case;
        RETURN;
    END;
    RAISE EXCEPTION 'case failed: %: expected % on %, the statement succeeded', p_case, p_sqlstate, p_constraint
        USING ERRCODE = 'assert_failure';
END
$$;

-- One event row with the given values, for the cases of the CHECK and the foreign keys.
CREATE FUNCTION we_event(
    p_workflow uuid, p_kind text, p_node uuid, p_task uuid, p_class text, p_move_kind text
) RETURNS void LANGUAGE sql AS $$
    INSERT INTO horsies_workflow_events (
        workflow_id, event_id, event_kind, occurred_at, workflow_node_row_id, task_id, history_class,
        terminalization_kind
    ) VALUES (p_workflow, nextval('horsies_workflow_event_id'), p_kind, now(), p_node, p_task, p_class, p_move_kind)
$$;

-- The events of p_workflow in event_id order: 'KIND' for a run event, 'KIND@index' for a node event, with
-- '/terminalization kind' for TASK_MOVED; '' for no event.
CREATE FUNCTION we_events(p_workflow uuid) RETURNS text LANGUAGE sql AS $$
    SELECT coalesce(string_agg(
        e.event_kind || coalesce('@' || n.task_index, '') || coalesce('/' || e.terminalization_kind, ''),
        ',' ORDER BY e.event_id
    ), '')
    FROM horsies_workflow_events AS e
    LEFT JOIN horsies_workflow_tasks AS n ON n.id = e.workflow_node_row_id
    WHERE e.workflow_id = p_workflow
$$;

-- Whether the TASK_MOVED event of p_task is the locator of its history row: its class, its time (the terminal time)
-- and its task id find exactly one history row, with the kind of the event.
CREATE FUNCTION we_locates(p_task uuid) RETURNS boolean LANGUAGE sql AS $$
    SELECT count(*) = 1
    FROM horsies_workflow_events AS e
    JOIN horsies_task_history AS h
      ON h.retention_class_key = e.history_class AND h.terminal_at = e.occurred_at AND h.task_id = e.task_id
    WHERE e.task_id = p_task AND e.event_kind = 'TASK_MOVED' AND h.terminalization_kind = e.terminalization_kind
$$;

------------------------------------------------------------------------------------------------------------------------
-- The schema of the event table
------------------------------------------------------------------------------------------------------------------------

SELECT we_check(
    'schema: the table, its bigint sequence with no cycle, its generated run column and its two indexes exist',
    to_regclass('horsies_workflow_events') IS NOT NULL
    AND (SELECT s.data_type = 'bigint'::regtype AND NOT s.cycle
         FROM pg_sequences AS s
         WHERE s.schemaname = current_schema() AND s.sequencename = 'horsies_workflow_event_id')
    AND (SELECT a.attgenerated = 's'
         FROM pg_attribute AS a
         WHERE a.attrelid = 'horsies_workflow_events'::regclass AND a.attname = 'run_workflow_id')
    AND to_regclass('horsies_workflow_events_node_idx') IS NOT NULL
    AND to_regclass('horsies_workflow_events_run_idx') IS NOT NULL
);
SELECT we_check(
    'functions: each writer of an event has one definition, and that definition writes the event table',
    (SELECT count(*) = 10 AND bool_and(p.prosrc LIKE '%horsies_workflow_events%')
     FROM pg_proc AS p
     WHERE p.pronamespace = (SELECT oid FROM pg_namespace WHERE nspname = current_schema())
       AND p.proname IN (
           'horsies_move_task_to_history', 'horsies_move_tasks_to_history', 'horsies_workflow_record_failure',
           'horsies_workflow_promote_level', 'horsies_workflow_promote', 'horsies_workflow_pause',
           'horsies_workflow_resume', 'horsies_workflow_child_start', 'horsies_workflow_claimed_tasks_settle',
           'horsies_workflow_task_stop_before_start'
       ))
);

-- The CHECK and the foreign keys. The workflow ...f001 has node 0 and node 1; ...f002 is a second workflow.
SELECT count(*) FROM wft_start1('00000000-0000-7000-8000-00000000f001', 'fail', '[{"idx":0},{"idx":1}]');
SELECT count(*) FROM wft_start1('00000000-0000-7000-8000-00000000f002', 'fail', '[{"idx":0}]');
CREATE FUNCTION we_node_id(p_workflow uuid, p_index integer) RETURNS uuid LANGUAGE sql AS $$
    SELECT id FROM horsies_workflow_tasks WHERE workflow_id = p_workflow AND task_index = p_index
$$;

SELECT we_expect_refusal('CHECK: a run event with a node',
    $q$SELECT we_event('00000000-0000-7000-8000-00000000f001', 'WORKFLOW_PAUSED',
        we_node_id('00000000-0000-7000-8000-00000000f001', 0), NULL, NULL, NULL)$q$,
    '23514', 'horsies_workflow_events_kind_shape');
SELECT we_expect_refusal('CHECK: a run event with a task',
    $q$SELECT we_event('00000000-0000-7000-8000-00000000f001', 'WORKFLOW_RESUMED', NULL, gen_random_uuid(), NULL,
        NULL)$q$,
    '23514', 'horsies_workflow_events_kind_shape');
SELECT we_expect_refusal('CHECK: a run event with a move kind',
    $q$SELECT we_event('00000000-0000-7000-8000-00000000f001', 'WORKFLOW_PAUSED', NULL, NULL, NULL,
        'COMPLETE_LOCKED')$q$,
    '23514', 'horsies_workflow_events_kind_shape');
SELECT we_expect_refusal('CHECK: a node event with no node',
    $q$SELECT we_event('00000000-0000-7000-8000-00000000f001', 'NODE_READY', NULL, NULL, NULL, NULL)$q$,
    '23514', 'horsies_workflow_events_kind_shape');
SELECT we_expect_refusal('CHECK: a node event with a task',
    $q$SELECT we_event('00000000-0000-7000-8000-00000000f001', 'NODE_ENQUEUED',
        we_node_id('00000000-0000-7000-8000-00000000f001', 0), gen_random_uuid(), NULL, NULL)$q$,
    '23514', 'horsies_workflow_events_kind_shape');
SELECT we_expect_refusal('CHECK: a node event with a history class',
    $q$SELECT we_event('00000000-0000-7000-8000-00000000f001', 'NODE_READY',
        we_node_id('00000000-0000-7000-8000-00000000f001', 0), NULL, 'forever', NULL)$q$,
    '23514', 'horsies_workflow_events_kind_shape');
SELECT we_expect_refusal('CHECK: TASK_MOVED with no node',
    $q$SELECT we_event('00000000-0000-7000-8000-00000000f001', 'TASK_MOVED', NULL, gen_random_uuid(), 'forever',
        'COMPLETE_LOCKED')$q$,
    '23514', 'horsies_workflow_events_kind_shape');
SELECT we_expect_refusal('CHECK: TASK_MOVED with no task',
    $q$SELECT we_event('00000000-0000-7000-8000-00000000f001', 'TASK_MOVED',
        we_node_id('00000000-0000-7000-8000-00000000f001', 0), NULL, 'forever', 'COMPLETE_LOCKED')$q$,
    '23514', 'horsies_workflow_events_kind_shape');
SELECT we_expect_refusal('CHECK: TASK_MOVED with no history class',
    $q$SELECT we_event('00000000-0000-7000-8000-00000000f001', 'TASK_MOVED',
        we_node_id('00000000-0000-7000-8000-00000000f001', 0), gen_random_uuid(), NULL, 'COMPLETE_LOCKED')$q$,
    '23514', 'horsies_workflow_events_kind_shape');
SELECT we_expect_refusal('CHECK: TASK_MOVED with a history class of another form',
    $q$SELECT we_event('00000000-0000-7000-8000-00000000f001', 'TASK_MOVED',
        we_node_id('00000000-0000-7000-8000-00000000f001', 0), gen_random_uuid(), 'standard_0d',
        'COMPLETE_LOCKED')$q$,
    '23514', 'horsies_workflow_events_kind_shape');
SELECT we_expect_refusal('CHECK: TASK_MOVED with no move kind',
    $q$SELECT we_event('00000000-0000-7000-8000-00000000f001', 'TASK_MOVED',
        we_node_id('00000000-0000-7000-8000-00000000f001', 0), gen_random_uuid(), 'forever', NULL)$q$,
    '23514', 'horsies_workflow_events_kind_shape');
SELECT we_expect_refusal('CHECK: TASK_MOVED with a kind that the move refuses for a workflow task',
    $q$SELECT we_event('00000000-0000-7000-8000-00000000f001', 'TASK_MOVED',
        we_node_id('00000000-0000-7000-8000-00000000f001', 0), gen_random_uuid(), 'forever', 'COMPLETE_FUSED')$q$,
    '23514', 'horsies_workflow_events_kind_shape');
SELECT we_expect_refusal('CHECK: an unknown event kind',
    $q$SELECT we_event('00000000-0000-7000-8000-00000000f001', 'NODE_RUNNING',
        we_node_id('00000000-0000-7000-8000-00000000f001', 0), NULL, NULL, NULL)$q$,
    '23514', 'horsies_workflow_events_kind_shape');
SELECT we_expect_refusal('foreign key: a node event names a node of another workflow',
    $q$SELECT we_event('00000000-0000-7000-8000-00000000f002', 'NODE_READY',
        we_node_id('00000000-0000-7000-8000-00000000f001', 0), NULL, NULL, NULL)$q$,
    '23503', 'horsies_workflow_events_node_fkey');
SELECT we_expect_refusal('foreign key: a run event of a workflow that does not exist',
    $q$SELECT we_event('00000000-0000-7000-8000-00000000ffff', 'WORKFLOW_PAUSED', NULL, NULL, NULL, NULL)$q$,
    '23503', 'horsies_workflow_events_run_fkey');
-- The legal shapes: each kind once, TASK_MOVED with each class form.
SELECT we_event('00000000-0000-7000-8000-00000000f001', 'WORKFLOW_PAUSED', NULL, NULL, NULL, NULL);
SELECT we_event('00000000-0000-7000-8000-00000000f001', 'WORKFLOW_RESUMED', NULL, NULL, NULL, NULL);
SELECT we_event('00000000-0000-7000-8000-00000000f001', 'NODE_READY',
    we_node_id('00000000-0000-7000-8000-00000000f001', 0), NULL, NULL, NULL);
SELECT we_event('00000000-0000-7000-8000-00000000f001', 'NODE_ENQUEUED',
    we_node_id('00000000-0000-7000-8000-00000000f001', 1), NULL, NULL, NULL);
SELECT we_event('00000000-0000-7000-8000-00000000f001', 'TASK_MOVED',
    we_node_id('00000000-0000-7000-8000-00000000f001', 0), gen_random_uuid(), 'forever', 'COMPLETE_LOCKED');
SELECT we_event('00000000-0000-7000-8000-00000000f001', 'TASK_MOVED',
    we_node_id('00000000-0000-7000-8000-00000000f001', 1), gen_random_uuid(), 'standard_30d',
    'WORKFLOW_CANCEL_WORKFLOW');
SELECT we_check(
    'CHECK: the legal shapes are accepted; the run column holds the workflow of a run event only',
    we_events('00000000-0000-7000-8000-00000000f001')
        = 'WORKFLOW_PAUSED,WORKFLOW_RESUMED,NODE_READY@0,NODE_ENQUEUED@1,TASK_MOVED@0/COMPLETE_LOCKED,'
          'TASK_MOVED@1/WORKFLOW_CANCEL_WORKFLOW'
    AND (SELECT bool_and(
                 (e.run_workflow_id IS NOT DISTINCT FROM e.workflow_id)
                 = (e.event_kind IN ('WORKFLOW_PAUSED', 'WORKFLOW_RESUMED'))
             )
             AND count(*) FILTER (WHERE e.run_workflow_id IS NULL) = 4
         FROM horsies_workflow_events AS e
         WHERE e.workflow_id = '00000000-0000-7000-8000-00000000f001')
);

------------------------------------------------------------------------------------------------------------------------
-- The writers: the moves
------------------------------------------------------------------------------------------------------------------------

-- A task node from its start to its terminal status: only the move writes an event.
DO $$
DECLARE
    v_wf uuid := '00000000-0000-7000-8000-00000000f010';
    v_claimed timestamptz := now() - interval '1 second';
    v_task uuid;
BEGIN
    PERFORM * FROM wft_start1(v_wf, 'fail', '[{"idx":0},{"idx":1,"deps":[0]}]');
    PERFORM we_check('start: the node inserts and the task node that goes ENQUEUED write no event',
        (wft_node(v_wf, 0)).status = 'ENQUEUED' AND we_events(v_wf) = '');
    v_task := wft_task_of(v_wf, 0);
    PERFORM wft_claim(v_task, v_claimed);
    PERFORM we_check('start of the attempt: the task starts',
        (SELECT outcome FROM horsies_start_attempt(v_task, 'worker-1', v_claimed, 1, 'host', 'proc')) = 'STARTED');
    PERFORM we_check('start of the attempt: the node goes RUNNING with no event',
        (wft_node(v_wf, 0)).status = 'RUNNING' AND we_events(v_wf) = '');
    PERFORM wft_phase1(v_task, wft_ok('1'));
    PERFORM we_check('single move: the move of a task of a node writes one TASK_MOVED with the kind of the move',
        we_events(v_wf) = 'TASK_MOVED@0/COMPLETE_LOCKED');
    PERFORM we_check('single move: the event is the locator of the history row (class, terminal time, task id)',
        we_locates(v_task));
    PERFORM * FROM horsies_workflow_node_finish(v_task, TRUE, 'READ');
    PERFORM we_check('phase 2: the terminal node and the promoted task node write no event',
        (wft_node(v_wf, 0)).status = 'COMPLETED' AND (wft_node(v_wf, 1)).status = 'ENQUEUED'
        AND we_events(v_wf) = 'TASK_MOVED@0/COMPLETE_LOCKED');
END $$;

-- The batch move: the pending expiry of a task of a node.
DO $$
DECLARE
    v_wf uuid := '00000000-0000-7000-8000-00000000f020';
    v_task uuid;
BEGIN
    PERFORM * FROM wft_start1(v_wf, 'fail', '[{"idx":0}]');
    v_task := wft_task_of(v_wf, 0);
    UPDATE horsies_tasks SET good_until = now() - interval '1 second' WHERE id = v_task;
    PERFORM we_check('batch move: the pending expiry moves the task',
        (SELECT count(*) FROM horsies_expire_pending_tasks(10, wft_err('TASK_EXPIRED'), 'TASK_EXPIRED')) = 1);
    PERFORM we_check('batch move: the move of a task of a node writes one TASK_MOVED with the kind of the move',
        wft_moved(v_task) = 'EXPIRED:EXPIRE_PENDING' AND we_events(v_wf) = 'TASK_MOVED@0/EXPIRE_PENDING');
    PERFORM we_check('batch move: the event is the locator of the history row (class, terminal time, task id)',
        we_locates(v_task));
END $$;

-- The workflow cancel: its batch move writes the event; the SKIPPED nodes and the end of the run write none.
DO $$
DECLARE
    v_wf uuid := '00000000-0000-7000-8000-00000000f0a0';
    v_task uuid;
BEGIN
    PERFORM * FROM wft_start1(v_wf, 'fail', '[{"idx":0},{"idx":1,"deps":[0]}]');
    v_task := wft_task_of(v_wf, 0);
    PERFORM we_check('cancel: the workflow is cancelled',
        (SELECT outcome FROM horsies_workflow_cancel(v_wf)) = 'CANCELLED');
    PERFORM we_check('cancel: one TASK_MOVED of the cancelled task; the SKIPPED nodes write no event',
        (wft_node(v_wf, 0)).status = 'SKIPPED' AND (wft_node(v_wf, 1)).status = 'SKIPPED'
        AND we_events(v_wf) = 'TASK_MOVED@0/WORKFLOW_CANCEL_WORKFLOW' AND we_locates(v_task));
END $$;

-- Moves with no event: an orphan with no node, a plain task. An orphan whose node is terminal has its event.
DO $$
DECLARE
    v_wf uuid := '00000000-0000-7000-8000-00000000f0b0';
    v_claimed timestamptz := now() - interval '1 second';
    v_orphan uuid;
    v_terminal_link uuid;
    v_plain uuid := '00000000-0000-7000-8000-00000000f0b1';
BEGIN
    PERFORM * FROM wft_start1(v_wf, 'fail', '[{"idx":0},{"idx":1}]');
    v_orphan := wft_task_of(v_wf, 0);
    v_terminal_link := wft_task_of(v_wf, 1);
    PERFORM wft_claim(v_orphan, v_claimed);
    PERFORM wft_claim(v_terminal_link, v_claimed);
    DELETE FROM horsies_workflow_tasks WHERE workflow_id = v_wf AND task_index = 0;
    UPDATE horsies_workflow_tasks SET status = 'COMPLETED', completed_at = now()
    WHERE workflow_id = v_wf AND task_index = 1;
    PERFORM we_check('orphan move: the orphan cancel applies',
        (SELECT outcome FROM horsies_cancel_owned_orphan(v_orphan, 'worker-1', v_claimed)) = 'APPLIED');
    PERFORM we_check('orphan move: a workflow task that no node links writes no event',
        wft_moved(v_orphan) = 'CANCELLED:CANCEL_ORPHAN'
        AND NOT EXISTS (SELECT 1 FROM horsies_workflow_events WHERE task_id = v_orphan));
    PERFORM we_check('orphan move, terminal node: the orphan cancel applies',
        (SELECT outcome FROM horsies_cancel_owned_orphan(v_terminal_link, 'worker-1', v_claimed)) = 'APPLIED');
    PERFORM we_check('orphan move, terminal node: the node probe finds the node, so one TASK_MOVED',
        we_events(v_wf) = 'TASK_MOVED@1/CANCEL_ORPHAN' AND we_locates(v_terminal_link));

    INSERT INTO horsies_tasks (
        id, task_name, queue_name, priority, input, task_options, execution, retention_class_key, sent_at,
        created_at, command_fingerprint_version, command_fingerprint, idempotency_key_digest, status, enqueued_at,
        claimed_at, claimed_by_worker_id, claim_expires_at, started_at, finalizing_at, finalizing_by_worker_id,
        worker_pid, worker_hostname, worker_process_name, retry_count, max_retries, next_retry_at, good_until,
        is_workflow_task, updated_at, retain_rerun_input, rerun_of_task_id, rerun_root_task_id, trace_carrier,
        trace_id, attempt_claims, first_claimed_at
    ) VALUES (
        v_plain, 'we.plain', 'q_we', 100, '{}', NULL, 'ISOLATED', 'forever', now(),
        now(), 1, sha256('we.plain'::bytea), NULL, 'PENDING', now(),
        NULL, NULL, NULL, NULL, NULL, NULL,
        NULL, NULL, NULL, 0, 0, NULL, NULL,
        FALSE, now(), FALSE, NULL, NULL, NULL,
        NULL, 0, NULL
    );
    PERFORM we_check('plain move: the cancel of a plain task applies',
        (SELECT outcome FROM horsies_cancel_task(v_plain, TRUE)) = 'APPLIED');
    PERFORM we_check('plain move: a plain task writes no event',
        wft_moved(v_plain) = 'CANCELLED:CANCEL_ADMIN'
        AND NOT EXISTS (SELECT 1 FROM horsies_workflow_events WHERE task_id = v_plain));
END $$;

------------------------------------------------------------------------------------------------------------------------
-- The writers: the workflow steps
------------------------------------------------------------------------------------------------------------------------

-- Promotion of READY nodes (horsies_workflow_promote, step 1): a sub-workflow root at start.
DO $$
DECLARE
    v_wf uuid := '00000000-0000-7000-8000-00000000f030';
BEGIN
    PERFORM * FROM wft_start1(v_wf, 'fail', '[{"idx":0,"sub":true},{"idx":1}]');
    PERFORM we_check('promotion of READY nodes: a sub-workflow node writes NODE_ENQUEUED; a task node writes none',
        (wft_node(v_wf, 0)).status = 'ENQUEUED' AND (wft_node(v_wf, 1)).status = 'ENQUEUED'
        AND we_events(v_wf) = 'NODE_ENQUEUED@0');
END $$;

-- A promotion level, a failed child start under on_error pause, and a child start in the paused workflow.
DO $$
DECLARE
    v_wf uuid := '00000000-0000-7000-8000-00000000f040';
    v_task uuid;
    v_row horsies_workflow_step_row;
    v_error text := horsies_task_error_envelope('WORKFLOW_ENQUEUE_FAILED', 'build failed', NULL, NULL, NULL);
BEGIN
    PERFORM * FROM wft_start1(v_wf, 'pause', '[
        {"idx":0},{"idx":1,"deps":[0],"sub":true},{"idx":2,"deps":[0],"sub":true},{"idx":3,"deps":[0]}
    ]');
    v_task := wft_task_of(v_wf, 0);
    PERFORM wft_phase1(v_task, wft_ok('1'));
    PERFORM * FROM horsies_workflow_node_finish(v_task, FALSE, 'READ');
    PERFORM we_check('promotion level: each sub-workflow node that goes ENQUEUED writes NODE_ENQUEUED, in index order',
        (wft_node(v_wf, 3)).status = 'ENQUEUED'
        AND we_events(v_wf) = 'TASK_MOVED@0/COMPLETE_LOCKED,NODE_ENQUEUED@1,NODE_ENQUEUED@2');
    SELECT * INTO v_row FROM wft_child_fail(v_wf, 1, v_error) WHERE row_kind = 'OUTCOME';
    PERFORM we_check('failure policy pause: the failed child start pauses the workflow with one WORKFLOW_PAUSED',
        v_row.disposition = 'NODE_FAILED' AND (wft_workflow(v_wf)).status = 'PAUSED'
        AND we_events(v_wf) = 'TASK_MOVED@0/COMPLETE_LOCKED,NODE_ENQUEUED@1,NODE_ENQUEUED@2,WORKFLOW_PAUSED');
    SELECT * INTO v_row FROM wft_child_start(v_wf, 2,
        jsonb_build_array(jsonb_build_object('id', '00000000-0000-7000-8000-00000000f041', 'on_error', 'fail',
                                             'parent', v_wf, 'parent_index', 2, 'depth', 1)),
        '[{"wf":"00000000-0000-7000-8000-00000000f041","idx":0}]'
    ) WHERE row_kind = 'OUTCOME';
    PERFORM we_check('child start in a paused workflow: the node goes back to READY with one NODE_READY',
        v_row.disposition = 'WORKFLOW_NOT_RUNNING' AND (wft_node(v_wf, 2)).status = 'READY'
        AND we_events(v_wf)
            = 'TASK_MOVED@0/COMPLETE_LOCKED,NODE_ENQUEUED@1,NODE_ENQUEUED@2,WORKFLOW_PAUSED,NODE_READY@2');
END $$;

-- The pause gate of a promotion (horsies_workflow_promote, step 3), after a resume: a workflow at depth 32 with
-- on_error pause and two READY sub-workflow nodes. The promotion enqueues both, the build of the lower one fails
-- (the depth limit), the failure pauses the workflow, and the other node goes back to READY.
DO $$
DECLARE
    v_root uuid := '00000000-0000-7000-8000-00000000f050';
    v_wf uuid := '00000000-0000-7000-8000-00000000f051';
    v_row horsies_workflow_step_row;
BEGIN
    INSERT INTO horsies_workflows (id, name, definition_key, status, on_error, output_task_index, success_policy,
        result, error, parent_workflow_id, parent_task_index, root_workflow_id, depth, sent_at, created_at,
        started_at, completed_at, updated_at)
    VALUES
        (v_root, 'root', NULL, 'RUNNING', 'fail', NULL, NULL, NULL, NULL, NULL, NULL, v_root, 0, now(), now(),
         now(), NULL, now()),
        (v_wf, 'deep', NULL, 'PAUSED', 'pause', NULL, NULL, NULL, NULL, v_root, 0, v_root, 32, now(), now(),
         now(), NULL, now());
    INSERT INTO horsies_workflow_tasks (id, workflow_id, task_index, node_id, task_name, input, queue_name, priority,
        dependencies, args_from, workflow_ctx_from, allow_failed_deps, join_type, min_success, task_options,
        max_retries, execution, good_until, retention_class_key, downstream_subworkflow, status, task_id,
        is_subworkflow, sub_workflow_id, sub_workflow_name, sub_definition_key, sub_workflow_params,
        sub_workflow_summary, result, error, created_at, started_at, completed_at)
    SELECT horsies_uuidv7(), v_wf, i, 'n' || i, 'sub', '{}', 'default', 50, '{}', NULL, NULL, FALSE, 'all', NULL,
        NULL, NULL, NULL, NULL, NULL, 'NONE', 'READY', NULL,
        TRUE, NULL, 'child.wf', 'child.key', 'NOT_GIVEN',
        NULL, NULL, NULL, now(), NULL, NULL
    FROM generate_series(0, 1) AS i;
    SELECT * INTO v_row FROM horsies_workflow_resume(v_wf) WHERE row_kind = 'OUTCOME';
    PERFORM we_check('pause gate: the resume runs, the lower node fails, and the failure pauses the workflow',
        v_row.disposition = 'RESUMED' AND (wft_workflow(v_wf)).status = 'PAUSED'
        AND (wft_node(v_wf, 0)).status = 'FAILED' AND (wft_node(v_wf, 1)).status = 'READY');
    PERFORM we_check('pause gate: WORKFLOW_RESUMED, NODE_ENQUEUED of both nodes, WORKFLOW_PAUSED, then NODE_READY',
        we_events(v_wf) = 'WORKFLOW_RESUMED,NODE_ENQUEUED@0,NODE_ENQUEUED@1,WORKFLOW_PAUSED,NODE_READY@1');
END $$;

-- Pause and resume of a workflow with a RUNNING child: the run events cascade; the abandoned claim moves.
DO $$
DECLARE
    v_wf uuid := '00000000-0000-7000-8000-00000000f060';
    v_child uuid := '00000000-0000-7000-8000-00000000f061';
    v_task uuid;
BEGIN
    PERFORM * FROM wft_start(
        jsonb_build_array(
            jsonb_build_object('id', v_wf, 'on_error', 'fail'),
            jsonb_build_object('id', v_child, 'on_error', 'fail', 'parent', v_wf, 'parent_index', 0, 'depth', 1)
        ),
        jsonb_build_array(
            jsonb_build_object('idx', 0, 'sub', true),
            jsonb_build_object('idx', 1),
            jsonb_build_object('wf', v_child, 'idx', 0)
        )
    );
    PERFORM we_check('start with a pre-built child: the linked node and the child write no event',
        (wft_node(v_wf, 0)).status = 'RUNNING' AND we_events(v_wf) = '' AND we_events(v_child) = '');
    v_task := wft_task_of(v_wf, 1);
    PERFORM wft_claim(v_task, now() - interval '1 second');
    PERFORM we_check('pause: the workflow and its child go PAUSED, and the claim is abandoned',
        (SELECT outcome = 'PAUSED' AND paused_workflow_ids = ARRAY[v_wf, v_child]
                AND abandoned_task_ids = ARRAY[v_task]
         FROM horsies_workflow_pause(v_wf)));
    PERFORM we_check('pause: WORKFLOW_PAUSED, the TASK_MOVED of the abandoned claim, then NODE_READY of its node',
        we_events(v_wf) = 'WORKFLOW_PAUSED,TASK_MOVED@1/PAUSE_ABANDON_WORKFLOW,NODE_READY@1' AND we_locates(v_task));
    PERFORM we_check('pause: one WORKFLOW_PAUSED for the paused child, after the one of its parent',
        we_events(v_child) = 'WORKFLOW_PAUSED'
        AND (SELECT max(e.event_id) FILTER (WHERE e.workflow_id = v_wf AND e.event_kind = 'WORKFLOW_PAUSED')
                    < max(e.event_id) FILTER (WHERE e.workflow_id = v_child)
             FROM horsies_workflow_events AS e
             WHERE e.workflow_id IN (v_wf, v_child)));
    PERFORM we_check('resume: the workflow and its child go RUNNING',
        (SELECT count(*) FROM horsies_workflow_resume(v_wf) WHERE row_kind = 'OUTCOME' AND disposition = 'RESUMED')
            = 1);
    PERFORM we_check('resume: one WORKFLOW_RESUMED per resumed workflow; the new task of the READY node writes none',
        (wft_workflow(v_wf)).status = 'RUNNING' AND (wft_workflow(v_child)).status = 'RUNNING'
        AND (wft_node(v_wf, 1)).status = 'ENQUEUED' AND wft_task_of(v_wf, 1) IS NOT NULL
        AND we_events(v_wf) = 'WORKFLOW_PAUSED,TASK_MOVED@1/PAUSE_ABANDON_WORKFLOW,NODE_READY@1,WORKFLOW_RESUMED'
        AND we_events(v_child) = 'WORKFLOW_PAUSED,WORKFLOW_RESUMED');
END $$;

-- The failure policy pause through phase 2 (horsies_workflow_record_failure), with a RUNNING child.
DO $$
DECLARE
    v_wf uuid := '00000000-0000-7000-8000-00000000f070';
    v_child uuid := '00000000-0000-7000-8000-00000000f071';
    v_task uuid;
BEGIN
    PERFORM * FROM wft_start(
        jsonb_build_array(
            jsonb_build_object('id', v_wf, 'on_error', 'pause'),
            jsonb_build_object('id', v_child, 'on_error', 'fail', 'parent', v_wf, 'parent_index', 1, 'depth', 1)
        ),
        jsonb_build_array(
            jsonb_build_object('idx', 0),
            jsonb_build_object('idx', 1, 'sub', true),
            jsonb_build_object('wf', v_child, 'idx', 0)
        )
    );
    v_task := wft_task_of(v_wf, 0);
    PERFORM wft_phase1(v_task, wft_err('USER'));
    PERFORM * FROM horsies_workflow_node_finish(v_task, TRUE, 'READ');
    PERFORM we_check('failure policy pause: one WORKFLOW_PAUSED for the workflow and one for its RUNNING child',
        (wft_workflow(v_wf)).status = 'PAUSED' AND (wft_workflow(v_child)).status = 'PAUSED'
        AND we_events(v_wf) = 'TASK_MOVED@0/FAIL_RUNNING,WORKFLOW_PAUSED'
        AND we_events(v_child) = 'WORKFLOW_PAUSED');
END $$;

-- The claim-time check of a PAUSED workflow (horsies_workflow_claimed_tasks_settle).
DO $$
DECLARE
    v_wf uuid := '00000000-0000-7000-8000-00000000f080';
    v_claimed timestamptz := now() - interval '1 second';
    v_task uuid;
BEGIN
    PERFORM * FROM wft_start1(v_wf, 'fail', '[{"idx":0}]');
    v_task := wft_task_of(v_wf, 0);
    UPDATE horsies_workflows SET status = 'PAUSED' WHERE id = v_wf;
    PERFORM wft_claim(v_task, v_claimed);
    PERFORM we_check('claim-time check: the claim of a PAUSED workflow is BLOCKED',
        (SELECT disposition FROM horsies_workflow_claimed_tasks_settle(ARRAY[v_task], ARRAY[v_claimed], 'worker-1'))
            = 'BLOCKED');
    PERFORM we_check('claim-time check: the TASK_MOVED of the abandoned claim, then one NODE_READY of its node',
        (wft_node(v_wf, 0)).status = 'READY'
        AND we_events(v_wf) = 'TASK_MOVED@0/PAUSE_ABANDON_CLAIM_BATCH,NODE_READY@0' AND we_locates(v_task));
END $$;

-- The stop before start of a task of a PAUSED workflow (horsies_workflow_task_stop_before_start).
DO $$
DECLARE
    v_wf uuid := '00000000-0000-7000-8000-00000000f090';
    v_claimed timestamptz := now() - interval '1 second';
    v_task uuid;
    v_row record;
BEGIN
    PERFORM * FROM wft_start1(v_wf, 'fail', '[{"idx":0}]');
    v_task := wft_task_of(v_wf, 0);
    UPDATE horsies_workflows SET status = 'PAUSED' WHERE id = v_wf;
    PERFORM wft_claim(v_task, v_claimed);
    SELECT * INTO v_row
    FROM horsies_workflow_task_stop_before_start(v_task, 'worker-1', v_claimed, wft_err('TASK_EXPIRED'), TRUE);
    PERFORM we_check('stop before start: the claim of a PAUSED workflow is abandoned',
        v_row.outcome = 'ABANDONED' AND v_row.move_outcome = 'APPLIED');
    PERFORM we_check('stop before start: the TASK_MOVED of the abandoned claim, then one NODE_READY of its node',
        (wft_node(v_wf, 0)).status = 'READY'
        AND we_events(v_wf) = 'TASK_MOVED@0/PAUSE_ABANDON_CLAIM,NODE_READY@0' AND we_locates(v_task));
END $$;

------------------------------------------------------------------------------------------------------------------------
-- Rollback and the foreign-key cascades
------------------------------------------------------------------------------------------------------------------------

-- A pause that rolls back leaves no event. Inside the subtransaction the pause wrote its event; a check that fails
-- there raises assert_failure, which the handler does not take.
DO $$
DECLARE
    v_wf uuid := '00000000-0000-7000-8000-00000000f0c0';
BEGIN
    PERFORM * FROM wft_start1(v_wf, 'fail', '[{"idx":0}]');
    BEGIN
        PERFORM * FROM horsies_workflow_pause(v_wf);
        IF we_events(v_wf) <> 'WORKFLOW_PAUSED' THEN
            RAISE EXCEPTION 'case failed: rollback: the pause wrote %', we_events(v_wf)
                USING ERRCODE = 'assert_failure';
        END IF;
        RAISE EXCEPTION 'roll the pause back';
    EXCEPTION WHEN raise_exception THEN
        NULL;
    END;
    PERFORM we_check('rollback: a pause that rolls back leaves the workflow RUNNING and no event',
        (wft_workflow(v_wf)).status = 'RUNNING' AND we_events(v_wf) = '');
END $$;

-- The delete of a node deletes its events; the delete of a workflow deletes its run events.
DO $$
DECLARE
    v_wf uuid := '00000000-0000-7000-8000-00000000f0d0';
BEGIN
    PERFORM * FROM wft_start1(v_wf, 'fail', '[{"idx":0},{"idx":1}]');
    PERFORM wft_claim(wft_task_of(v_wf, 0), now() - interval '1 second');
    PERFORM * FROM horsies_workflow_pause(v_wf);
    PERFORM we_check('cascade: the pause writes a run event and two events of node 0',
        we_events(v_wf) = 'WORKFLOW_PAUSED,TASK_MOVED@0/PAUSE_ABANDON_WORKFLOW,NODE_READY@0');
    DELETE FROM horsies_workflow_tasks WHERE workflow_id = v_wf AND task_index = 0;
    PERFORM we_check('cascade: a deleted node deletes its events; the run event stays',
        we_events(v_wf) = 'WORKFLOW_PAUSED');
    DELETE FROM horsies_workflows WHERE id = v_wf;
    PERFORM we_check('cascade: a deleted workflow deletes its run events',
        NOT EXISTS (SELECT 1 FROM horsies_workflow_events WHERE workflow_id = v_wf));
END $$;
