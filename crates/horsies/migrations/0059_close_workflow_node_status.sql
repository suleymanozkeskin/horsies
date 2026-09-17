-- Refuse invalid nodes before the constraint or function changes.
LOCK TABLE horsies_workflow_tasks IN SHARE ROW EXCLUSIVE MODE;
DO $migration$
BEGIN
    IF EXISTS (
        SELECT 1 FROM horsies_workflow_tasks
        WHERE status NOT IN (
            'PENDING', 'READY', 'ENQUEUED', 'RUNNING',
            'COMPLETED', 'FAILED', 'SKIPPED'
        )
    ) THEN
        RAISE EXCEPTION 'workflow nodes contain an unsupported status'
            USING ERRCODE = 'HN001';
    END IF;
    IF NOT EXISTS (
        SELECT 1 FROM pg_constraint
        WHERE conrelid = 'horsies_workflow_tasks'::regclass
          AND conname = 'horsies_workflow_tasks_status_check'
    ) THEN
        ALTER TABLE horsies_workflow_tasks
            ADD CONSTRAINT horsies_workflow_tasks_status_check CHECK (
                status IN ('PENDING', 'READY', 'ENQUEUED', 'RUNNING',
                           'COMPLETED', 'FAILED', 'SKIPPED')
            );
    END IF;
    IF to_regprocedure('horsies_phase2_consume(uuid,text)') IS NULL THEN
        RETURN;
    END IF;
    EXECUTE $node_status_program$CREATE OR REPLACE FUNCTION horsies_phase2_consume(
    p_task_id uuid,
    p_terminal_node_status text
) RETURNS horsies_phase2_disposition
LANGUAGE plpgsql
SET plan_cache_mode = force_generic_plan
AS $function$
DECLARE
    v_pending horsies_workflow_phase2_pending%ROWTYPE;
    v_wf record;
    v_node record;
    v_payload bytea;
    v_digest bytea;
    v_version smallint;
    v_source_task uuid;
    v_cas_won boolean;
BEGIN
    IF p_terminal_node_status IS NULL OR p_terminal_node_status NOT IN ('COMPLETED', 'FAILED') THEN
        RAISE EXCEPTION
            'terminal node status must be COMPLETED or FAILED'
            USING ERRCODE = 'invalid_parameter_value';
    END IF;

    SELECT * INTO v_pending
    FROM horsies_workflow_phase2_pending
    WHERE task_id = p_task_id;

    IF NOT FOUND THEN
        -- Idempotent replay after an uncertain commit: the first commit
        -- deleted pending. Classify from the node the task backs.
        SELECT wt.id, wt.workflow_id, wt.task_index, wt.status
        INTO v_node
        FROM horsies_workflow_tasks wt
        WHERE wt.task_id = p_task_id
        ORDER BY wt.id
        LIMIT 1;
        IF NOT FOUND THEN
            RETURN ROW('PENDING_ABSENT', NULL, NULL, NULL, NULL, NULL,
                       NULL, NULL, NULL, NULL,
                       'no pending row and no node linkage')
                ::horsies_phase2_disposition;
        END IF;
        SELECT w.status, w.depth, w.root_workflow_id, w.on_error
        INTO v_wf
        FROM horsies_workflows w
        WHERE w.id = v_node.workflow_id;
        IF v_node.status IN ('COMPLETED', 'FAILED', 'SKIPPED')
        THEN
            RETURN ROW('ALREADY_APPLIED', v_node.workflow_id, v_node.id,
                       v_node.task_index, v_wf.status, v_wf.depth,
                       v_wf.root_workflow_id, v_wf.on_error,
                       v_node.status, NULL, NULL)
                ::horsies_phase2_disposition;
        END IF;
        RETURN ROW('PENDING_ABSENT', v_node.workflow_id, v_node.id,
                   v_node.task_index, v_wf.status, v_wf.depth,
                   v_wf.root_workflow_id, v_wf.on_error,
                   v_node.status, NULL,
                   'no pending row; node not terminal')
            ::horsies_phase2_disposition;
    END IF;

    -- N6 order: workflow row first, node row second, pending third.
    SELECT w.status, w.depth, w.root_workflow_id, w.on_error
    INTO v_wf
    FROM horsies_workflows w
    WHERE w.id = v_pending.workflow_id
    FOR UPDATE;
    IF NOT FOUND THEN
        RETURN ROW('SOURCE_STATE_CONFLICT', v_pending.workflow_id,
                   v_pending.workflow_node_row_id, NULL, NULL, NULL,
                   NULL, NULL, NULL, v_pending.terminal_status,
                   'workflow row absent while pending exists')
            ::horsies_phase2_disposition;
    END IF;

    SELECT wt.id, wt.workflow_id, wt.task_index, wt.status
    INTO v_node
    FROM horsies_workflow_tasks wt
    WHERE wt.id = v_pending.workflow_node_row_id
      AND wt.workflow_id = v_pending.workflow_id
    FOR UPDATE;
    IF NOT FOUND THEN
        RETURN ROW('SOURCE_STATE_CONFLICT', v_pending.workflow_id,
                   v_pending.workflow_node_row_id, NULL, v_wf.status,
                   v_wf.depth, v_wf.root_workflow_id, v_wf.on_error,
                   NULL, v_pending.terminal_status,
                   'node row absent while pending exists')
            ::horsies_phase2_disposition;
    END IF;

    PERFORM 1 FROM horsies_workflow_phase2_pending
    WHERE task_id = p_task_id
    FOR UPDATE;

    IF v_wf.status IN ('COMPLETED', 'FAILED', 'CANCELLED', 'EXPIRED') THEN
        DELETE FROM horsies_workflow_phase2_pending WHERE task_id = p_task_id;
        IF v_pending.recovery_source = 'QUARANTINE' THEN
            DELETE FROM horsies_workflow_phase2_quarantine
            WHERE task_id = v_pending.quarantine_task_id;
        END IF;
        RETURN ROW('SUPERSEDED_BY_WORKFLOW_TERMINAL',
                   v_pending.workflow_id, v_node.id, v_node.task_index,
                   v_wf.status, v_wf.depth, v_wf.root_workflow_id,
                   v_wf.on_error, v_node.status,
                   v_pending.terminal_status, NULL)
            ::horsies_phase2_disposition;
    END IF;

    IF v_pending.recovery_source = 'HISTORY' THEN
        -- One-leaf parent probe, NOT the rejected fan-out: the locator
        -- supplies both partition keys, so LIST (class) and RANGE
        -- (anchor) prune to exactly one leaf at plan time. The rejected
        -- mechanism carried a task-id predicate alone and planned every
        -- leaf; this read exists because the locator makes pruning
        -- possible.
        SELECT h.task_id, h.result_payload, h.result_digest,
               h.history_schema_version
        INTO v_source_task, v_payload, v_digest, v_version
        FROM horsies_task_history h
        WHERE h.retention_class_key = v_pending.history_class
          AND h.retention_anchor_at = v_pending.history_anchor
          AND h.task_id = p_task_id;
        IF NOT FOUND THEN
            RETURN ROW('SOURCE_ABSENT', v_pending.workflow_id, v_node.id,
                       v_node.task_index, v_wf.status, v_wf.depth,
                       v_wf.root_workflow_id, v_wf.on_error, v_node.status,
                       v_pending.terminal_status,
                       'history row absent at locator')
                ::horsies_phase2_disposition;
        END IF;
    ELSE
        SELECT q.task_id, q.result_payload, q.result_digest,
               q.history_schema_version
        INTO v_source_task, v_payload, v_digest, v_version
        FROM horsies_workflow_phase2_quarantine q
        WHERE q.task_id = v_pending.quarantine_task_id;
        IF NOT FOUND THEN
            RETURN ROW('SOURCE_ABSENT', v_pending.workflow_id, v_node.id,
                       v_node.task_index, v_wf.status, v_wf.depth,
                       v_wf.root_workflow_id, v_wf.on_error, v_node.status,
                       v_pending.terminal_status,
                       'quarantine row absent at locator')
                ::horsies_phase2_disposition;
        END IF;
    END IF;

    IF v_source_task <> p_task_id THEN
        RETURN ROW('SOURCE_STATE_CONFLICT', v_pending.workflow_id,
                   v_node.id, v_node.task_index, v_wf.status, v_wf.depth,
                   v_wf.root_workflow_id, v_wf.on_error, v_node.status,
                   v_pending.terminal_status,
                   'source row carries a different task identity')
            ::horsies_phase2_disposition;
    END IF;
    IF v_version IS DISTINCT FROM v_pending.history_schema_version
       OR v_version <> 1 THEN
        RETURN ROW('SOURCE_VERSION_CONFLICT', v_pending.workflow_id,
                   v_node.id, v_node.task_index, v_wf.status, v_wf.depth,
                   v_wf.root_workflow_id, v_wf.on_error, v_node.status,
                   v_pending.terminal_status,
                   'source schema version disagrees with locator')
            ::horsies_phase2_disposition;
    END IF;
    IF v_digest IS DISTINCT FROM v_pending.result_digest
       OR v_payload IS NULL
       OR sha256(v_payload) <> v_pending.result_digest THEN
        RETURN ROW('SOURCE_DIGEST_MISMATCH', v_pending.workflow_id,
                   v_node.id, v_node.task_index, v_wf.status, v_wf.depth,
                   v_wf.root_workflow_id, v_wf.on_error, v_node.status,
                   v_pending.terminal_status,
                   'result digest disagrees with locator or payload')
            ::horsies_phase2_disposition;
    END IF;

    UPDATE horsies_workflow_tasks wt
    SET status = p_terminal_node_status,
        result = convert_from(v_payload, 'UTF8'),
        completed_at = NOW()
    WHERE wt.id = v_node.id
      AND wt.status NOT IN ('COMPLETED', 'FAILED', 'SKIPPED');
    v_cas_won := FOUND;

    DELETE FROM horsies_workflow_phase2_pending WHERE task_id = p_task_id;
    IF v_pending.recovery_source = 'QUARANTINE' THEN
        DELETE FROM horsies_workflow_phase2_quarantine
        WHERE task_id = v_pending.quarantine_task_id;
    END IF;

    IF v_cas_won THEN
        RETURN ROW('APPLIED_TO_NODE', v_pending.workflow_id, v_node.id,
                   v_node.task_index, v_wf.status, v_wf.depth,
                   v_wf.root_workflow_id, v_wf.on_error,
                   p_terminal_node_status, v_pending.terminal_status, NULL)
            ::horsies_phase2_disposition;
    END IF;
    RETURN ROW('ALREADY_APPLIED', v_pending.workflow_id, v_node.id,
               v_node.task_index, v_wf.status, v_wf.depth,
               v_wf.root_workflow_id, v_wf.on_error, v_node.status,
               v_pending.terminal_status, NULL)
        ::horsies_phase2_disposition;
END
$function$$node_status_program$;
END
$migration$;
