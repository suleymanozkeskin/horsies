-- Calls every interim function of the baseline on a fresh install, and the functions of 0004_lifecycle_functions.sql
-- that replace interim ones by name, with their new arguments (baseline_lifecycle_functions.sql tests that file in
-- full). The caller runs the script in one transaction and rolls it back. PL/pgSQL checks a statement against the
-- tables only when the statement runs, so each function is called on a path that runs its statements that name
-- columns. Each call checks its outcome.

CREATE FUNCTION fixture_task(
    p_id uuid, p_status text, p_worker text, p_is_workflow boolean, p_good_until timestamptz DEFAULT NULL
) RETURNS void LANGUAGE sql AS $$
    INSERT INTO horsies_tasks (
        id, task_name, queue_name, priority, input, task_options, execution, retention_class_key, sent_at,
        created_at, command_fingerprint_version, command_fingerprint, idempotency_key_digest, status, enqueued_at,
        claimed_at, claimed_by_worker_id, claim_expires_at, started_at, finalizing_at, finalizing_by_worker_id,
        worker_pid, worker_hostname, worker_process_name, retry_count, max_retries, next_retry_at, good_until,
        is_workflow_task, updated_at, retain_rerun_input, rerun_of_task_id, rerun_root_task_id
    ) VALUES (
        p_id, 'fixture.task', 'default', 100, '{"x":1}', NULL, 'ISOLATED', 'forever', now(),
        now(),
        CASE WHEN p_is_workflow THEN NULL ELSE 1::smallint END,
        CASE WHEN p_is_workflow THEN NULL ELSE sha256('x'::bytea) END,
        NULL, p_status, now(),
        CASE WHEN p_status = 'PENDING' THEN NULL ELSE now() - interval '1 second' END,
        CASE WHEN p_status = 'PENDING' THEN NULL ELSE p_worker END,
        CASE WHEN p_status = 'CLAIMED' THEN now() + interval '1 minute' END,
        CASE WHEN p_status = 'RUNNING' THEN now() - interval '1 second' END,
        NULL, NULL,
        CASE WHEN p_status = 'RUNNING' THEN 1 END,
        CASE WHEN p_status = 'RUNNING' THEN 'host' END,
        CASE WHEN p_status = 'RUNNING' THEN 'proc' END,
        0, 1, NULL, p_good_until,
        p_is_workflow, now(), TRUE, NULL, NULL
    )
$$;

CREATE FUNCTION fixture_workflow(p_id uuid, p_status text) RETURNS void LANGUAGE sql AS $$
    INSERT INTO horsies_workflows (
        id, name, definition_key, status, on_error, output_task_index, success_policy, result, error,
        parent_workflow_id, parent_task_index, root_workflow_id, depth, sent_at, created_at, started_at,
        completed_at, updated_at
    ) VALUES (
        p_id, 'fixture.workflow', NULL, p_status, 'fail', NULL, NULL, NULL, NULL,
        NULL, NULL, p_id, 0, now(), now(), now(), NULL, now()
    )
$$;

CREATE FUNCTION fixture_node(p_id uuid, p_workflow uuid, p_index integer, p_status text, p_task uuid)
RETURNS void LANGUAGE sql AS $$
    INSERT INTO horsies_workflow_tasks (
        id, workflow_id, task_index, node_id, task_name, input, queue_name, priority, dependencies, args_from,
        workflow_ctx_from, allow_failed_deps, join_type, min_success, task_options, max_retries, execution,
        good_until, retention_class_key, downstream_subworkflow, status, task_id, is_subworkflow, sub_workflow_id,
        sub_workflow_name, sub_definition_key, sub_workflow_params, sub_workflow_summary, result, error,
        created_at, started_at, completed_at
    ) VALUES (
        p_id, p_workflow, p_index, 'node_' || p_index, 'fixture.task', '{"x":1}', 'default', 100, '{}', NULL,
        NULL, FALSE, 'all', NULL, NULL, 1, 'ISOLATED',
        NULL, 'forever', 'NONE', p_status, p_task, FALSE, NULL, NULL,
        NULL, NULL, NULL, NULL, NULL, now(), NULL,
        NULL
    )
$$;

CREATE FUNCTION expect_value(p_label text, p_actual text, p_expected text) RETURNS void LANGUAGE plpgsql AS $$
BEGIN
    IF p_actual IS DISTINCT FROM p_expected THEN
        RAISE EXCEPTION 'call %: expected %, got %', p_label, p_expected, p_actual;
    END IF;
END
$$;

-- The published readers (the duplicate guard of the moves, the provenance reader of the miss classifier), as the
-- install publishes them.
SELECT expect_value('publish_history_readers', (SELECT outcome FROM horsies_publish_history_readers()), 'PUBLISHED');

-- Helpers with no table reads.
SELECT expect_value('uuid7_birth', (horsies_uuid7_birth(horsies_uuidv7()) IS NOT NULL)::text, 'true');
SELECT expect_value('leaf_lock_key', (horsies_task_history_leaf_lock_key('forever', now()) IS NOT NULL)::text, 'true');
SELECT expect_value(
    'encode_rerun_input',
    convert_from(horsies_encode_rerun_input('{"a":1}', NULL), 'UTF8'),
    '{"input":{"a":1},"options":null}'
);

-- Claim: plan, in-flight count, candidates and the whole claim.
SELECT fixture_task('00000000-0000-7000-8000-000000000001', 'PENDING', NULL, FALSE);
SELECT * FROM horsies_claim_in_flight('w1', ARRAY['default'], FALSE, '{}'::jsonb);
SELECT expect_value(
    'claim',
    (SELECT string_agg(row_kind || ':' || input, ',') FROM horsies_claim(
        'w1', '["default"]'::jsonb, '{}'::jsonb, '{}'::jsonb, FALSE,
        '{"ISOLATED": 1}'::jsonb, '{"ISOLATED": "ISOLATED"}'::jsonb,
        0, 1, 1, NULL, 60000, '[]'::jsonb
    )),
    'CLAIMED:{"x":1}'
);

-- Non-runnable lookup and the success completions.
SELECT * FROM horsies_find_non_runnable_workflow_tasks(ARRAY['00000000-0000-7000-8000-000000000001'::uuid]);
UPDATE horsies_tasks SET status = 'RUNNING', claim_expires_at = NULL, started_at = now(),
    worker_pid = 1, worker_hostname = 'h', worker_process_name = 'p'
WHERE id = '00000000-0000-7000-8000-000000000001';
SELECT expect_value(
    'complete_task_fused',
    (SELECT outcome FROM horsies_complete_task_fused(
        '00000000-0000-7000-8000-000000000001', 'w1',
        (SELECT claimed_at FROM horsies_tasks WHERE id = '00000000-0000-7000-8000-000000000001'),
        '{"ok":1}', TRUE)),
    'APPLIED'
);
SELECT fixture_task('00000000-0000-7000-8000-000000000002', 'RUNNING', 'w1', FALSE);
SELECT expect_value(
    'complete_tasks_fused',
    (SELECT string_agg(outcome, ',') FROM horsies_complete_tasks_fused(
        ARRAY['00000000-0000-7000-8000-000000000002'::uuid],
        ARRAY[(SELECT claimed_at FROM horsies_tasks WHERE id = '00000000-0000-7000-8000-000000000002')],
        ARRAY['{"ok":2}'], 'w1', TRUE)),
    'APPLIED'
);

-- Failed attempt: a retry, then the final failure with a stored rerun envelope.
SELECT fixture_task('00000000-0000-7000-8000-000000000004', 'RUNNING', 'w1', FALSE);
SELECT expect_value(
    'finish_failed_attempt retry',
    (SELECT outcome FROM horsies_finish_failed_attempt(
        '00000000-0000-7000-8000-000000000004', 'w1',
        (SELECT claimed_at FROM horsies_tasks WHERE id = '00000000-0000-7000-8000-000000000004'),
        'RETRY', 0, 1.0, 'SAME_CLASS', '{"err":1}', 'E1', 'reason', 'FAILED', 'message', 'reason', 'REFUSE', TRUE)),
    'RETRY_SCHEDULED'
);
UPDATE horsies_tasks SET status = 'RUNNING', claimed_at = now(), claimed_by_worker_id = 'w1', started_at = now(),
    worker_pid = 1, worker_hostname = 'h', worker_process_name = 'p'
WHERE id = '00000000-0000-7000-8000-000000000004';
SELECT expect_value(
    'finish_failed_attempt final',
    (SELECT outcome FROM horsies_finish_failed_attempt(
        '00000000-0000-7000-8000-000000000004', 'w1',
        (SELECT claimed_at FROM horsies_tasks WHERE id = '00000000-0000-7000-8000-000000000004'),
        'NONE', NULL, NULL, NULL, '{"err":2}', 'E1', 'reason', 'FAILED', 'message', 'reason', 'REFUSE', TRUE)),
    'APPLIED'
);
SELECT expect_value(
    'rerun envelope in history',
    (SELECT rerun_input_disposition || ':' || convert_from(rerun_input_inline, 'UTF8')
     FROM horsies_task_history WHERE task_id = '00000000-0000-7000-8000-000000000004'),
    'INLINE:{"input":{"x":1},"options":null}'
);
SELECT expect_value(
    'encode_task_attempts',
    (SELECT (octet_length(attempt_snapshot) > 0)::text FROM horsies_task_history
     WHERE task_id = '00000000-0000-7000-8000-000000000004'),
    'true'
);

-- Claimed expiry.
SELECT fixture_task('00000000-0000-7000-8000-000000000007', 'CLAIMED', 'w1', FALSE, now() - interval '1 minute');
SELECT expect_value(
    'expire_owned_claim',
    (SELECT outcome FROM horsies_expire_owned_claim('00000000-0000-7000-8000-000000000007', 'w1', '{"e":1}', 'E')),
    'APPLIED'
);

-- Pending expiry: one plain task and one workflow task with its deferred phase-2 record.
SELECT fixture_task('00000000-0000-7000-8000-000000000009', 'PENDING', NULL, FALSE, now() - interval '1 minute');
SELECT fixture_workflow('00000000-0000-7000-8000-0000000000a1', 'RUNNING');
SELECT fixture_task('00000000-0000-7000-8000-00000000000a', 'PENDING', NULL, TRUE, now() - interval '1 minute');
SELECT fixture_node(
    '00000000-0000-7000-8000-0000000000b1', '00000000-0000-7000-8000-0000000000a1', 0, 'ENQUEUED',
    '00000000-0000-7000-8000-00000000000a'
);
SELECT expect_value(
    'expire_pending_tasks',
    (SELECT count(*)::text FROM horsies_expire_pending_tasks(10, '{"e":"expired"}', 'E')),
    '2'
);
SELECT expect_value(
    'pending record',
    (SELECT terminalization_kind || ':' || recovery_source FROM horsies_workflow_phase2_pending
     WHERE task_id = '00000000-0000-7000-8000-00000000000a'),
    'EXPIRE_PENDING:HISTORY'
);

-- Phase 2: quarantine the record, then consume it.
SELECT expect_value(
    'phase2_quarantine_one',
    (SELECT verdict FROM horsies_phase2_quarantine_one('00000000-0000-7000-8000-00000000000a')),
    'REPOINTED'
);
SELECT expect_value(
    'phase2_consume',
    (SELECT disposition FROM horsies_phase2_consume('00000000-0000-7000-8000-00000000000a', 'FAILED')),
    'APPLIED_TO_NODE'
);

-- A second deferred record, consumed from history.
SELECT fixture_task('00000000-0000-7000-8000-00000000000b', 'RUNNING', 'w1', TRUE);
SELECT fixture_node(
    '00000000-0000-7000-8000-0000000000b2', '00000000-0000-7000-8000-0000000000a1', 1, 'RUNNING',
    '00000000-0000-7000-8000-00000000000b'
);
-- The single move (0006) takes the live row that its caller locked.
SELECT horsies_move_task_to_history(
    (SELECT t FROM horsies_tasks AS t WHERE t.id = '00000000-0000-7000-8000-00000000000b' FOR UPDATE),
    'COMPLETED', 'COMPLETE_LOCKED', now(), '{"ok":1}', NULL, NULL
);
SELECT expect_value(
    'phase2_consume history',
    (SELECT disposition FROM horsies_phase2_consume('00000000-0000-7000-8000-00000000000b', 'COMPLETED')),
    'APPLIED_TO_NODE'
);

-- Workflow success in one statement.
SELECT fixture_task('00000000-0000-7000-8000-00000000000c', 'RUNNING', 'w1', TRUE);
SELECT fixture_node(
    '00000000-0000-7000-8000-0000000000b3', '00000000-0000-7000-8000-0000000000a1', 2, 'RUNNING',
    '00000000-0000-7000-8000-00000000000c'
);
SELECT expect_value(
    'finish_workflow_success',
    (SELECT outcome FROM horsies_finish_workflow_success(
        '00000000-0000-7000-8000-00000000000c', 'w1',
        (SELECT claimed_at FROM horsies_tasks WHERE id = '00000000-0000-7000-8000-00000000000c'), '{"ok":1}')),
    'APPLIED'
);

-- Owned-node pause abandons and workflow cancels, single and batch.
SELECT fixture_workflow('00000000-0000-7000-8000-0000000000a4', 'PAUSED');
SELECT fixture_task('00000000-0000-7000-8000-00000000000d', 'CLAIMED', 'w1', TRUE);
SELECT fixture_node(
    '00000000-0000-7000-8000-0000000000b4', '00000000-0000-7000-8000-0000000000a4', 0, 'ENQUEUED',
    '00000000-0000-7000-8000-00000000000d'
);
SELECT expect_value(
    'abandon_owned_node',
    (SELECT outcome FROM horsies_abandon_owned_node(
        '00000000-0000-7000-8000-00000000000d', 'w1', (SELECT claimed_at FROM horsies_tasks WHERE id = '00000000-0000-7000-8000-00000000000d'))),
    'APPLIED'
);
SELECT fixture_task('00000000-0000-7000-8000-00000000000e', 'CLAIMED', 'w1', TRUE);
SELECT fixture_node(
    '00000000-0000-7000-8000-0000000000b5', '00000000-0000-7000-8000-0000000000a4', 1, 'ENQUEUED',
    '00000000-0000-7000-8000-00000000000e'
);
SELECT expect_value(
    'abandon_owned_nodes',
    (SELECT string_agg(outcome, ',') FROM horsies_abandon_owned_nodes(
        ARRAY['00000000-0000-7000-8000-00000000000e'::uuid],
        ARRAY[(SELECT claimed_at FROM horsies_tasks WHERE id = '00000000-0000-7000-8000-00000000000e')], 'w1')),
    'APPLIED'
);
SELECT fixture_task('00000000-0000-7000-8000-00000000000f', 'CLAIMED', 'w1', TRUE);
SELECT fixture_node(
    '00000000-0000-7000-8000-0000000000b6', '00000000-0000-7000-8000-0000000000a1', 5, 'ENQUEUED',
    '00000000-0000-7000-8000-00000000000f'
);
SELECT expect_value(
    'cancel_owned_node',
    (SELECT outcome FROM horsies_cancel_owned_node(
        '00000000-0000-7000-8000-00000000000f', 'w1', (SELECT claimed_at FROM horsies_tasks WHERE id = '00000000-0000-7000-8000-00000000000f'), FALSE)),
    'APPLIED'
);
SELECT fixture_task('00000000-0000-7000-8000-000000000010', 'CLAIMED', 'w1', TRUE);
SELECT fixture_node(
    '00000000-0000-7000-8000-0000000000b7', '00000000-0000-7000-8000-0000000000a1', 6, 'ENQUEUED',
    '00000000-0000-7000-8000-000000000010'
);
SELECT expect_value(
    'cancel_owned_nodes',
    (SELECT string_agg(outcome, ',') FROM horsies_cancel_owned_nodes(
        ARRAY['00000000-0000-7000-8000-000000000010'::uuid],
        ARRAY[(SELECT claimed_at FROM horsies_tasks WHERE id = '00000000-0000-7000-8000-000000000010')], 'w1')),
    'APPLIED'
);
SELECT fixture_task('00000000-0000-7000-8000-000000000011', 'CLAIMED', 'w1', TRUE);
SELECT expect_value(
    'cancel_owned_orphan',
    (SELECT outcome FROM horsies_cancel_owned_orphan(
        '00000000-0000-7000-8000-000000000011', 'w1', (SELECT claimed_at FROM horsies_tasks WHERE id = '00000000-0000-7000-8000-000000000011'))),
    'APPLIED'
);

-- Workflow sweeps: a paused workflow and a cancelled workflow, each with one pending node task.
SELECT fixture_workflow('00000000-0000-7000-8000-0000000000a2', 'PAUSED');
SELECT fixture_task('00000000-0000-7000-8000-000000000012', 'CLAIMED', 'w1', TRUE);
SELECT fixture_node(
    '00000000-0000-7000-8000-0000000000b8', '00000000-0000-7000-8000-0000000000a2', 0, 'ENQUEUED',
    '00000000-0000-7000-8000-000000000012'
);
SELECT expect_value(
    'abandon_nodes_of_paused_workflows',
    (SELECT count(*)::text FROM horsies_abandon_nodes_of_paused_workflows(
        ARRAY['00000000-0000-7000-8000-0000000000a2'::uuid])),
    '1'
);
SELECT fixture_workflow('00000000-0000-7000-8000-0000000000a3', 'CANCELLED');
SELECT fixture_task('00000000-0000-7000-8000-000000000013', 'CLAIMED', 'w1', TRUE);
SELECT fixture_node(
    '00000000-0000-7000-8000-0000000000b9', '00000000-0000-7000-8000-0000000000a3', 0, 'ENQUEUED',
    '00000000-0000-7000-8000-000000000013'
);
SELECT expect_value(
    'cancel_nodes_of_cancelled_workflow',
    (SELECT count(*)::text FROM horsies_cancel_nodes_of_cancelled_workflow(
        ARRAY['00000000-0000-7000-8000-0000000000a3'::uuid])),
    '1'
);

-- Orphan sweep: a workflow task with no node row.
SELECT fixture_task('00000000-0000-7000-8000-000000000014', 'PENDING', NULL, TRUE);
UPDATE horsies_tasks SET created_at = now() - interval '1 hour' WHERE id = '00000000-0000-7000-8000-000000000014';
SELECT count(*) FROM horsies_cancel_orphaned_tasks(10);

-- The miss classifier on an absent task, the duplicate guard, and the key reservations.
SELECT expect_value(
    'terminalization_miss',
    (SELECT outcome FROM horsies_terminalization_miss(
        '00000000-0000-7000-8000-0000000000ff', 'FAIL_RUNNING', 'NONE', NULL, NULL)),
    'TASK_ABSENT'
);
SELECT expect_value(
    'duplicate_staged',
    horsies_task_history_duplicate_staged('00000000-0000-7000-8000-0000000000ff')::text,
    'false'
);
SELECT expect_value(
    'key_reservation_claim',
    (SELECT outcome FROM horsies_key_reservation_claim(
        sha256('k'::bytea), 1::smallint, interval '1 hour', 1::smallint, sha256('f'::bytea),
        '00000000-0000-7000-8000-0000000000fe')),
    'APPLIED'
);
SELECT horsies_key_reservation_terminalize(
    sha256('k'::bytea), '00000000-0000-7000-8000-0000000000fe', now() - interval '2 hours');
SELECT horsies_key_reservation_terminalize_batch(ARRAY[]::bytea[], ARRAY[]::uuid[], now());
SELECT expect_value('key_reservation_cleanup', horsies_key_reservation_cleanup(10)::text, '1');

