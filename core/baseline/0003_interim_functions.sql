-- horsies baseline schema, part 3: the interim SQL functions and their composite types.
--
-- These are the functions that the Python client calls today, adapted to the columns of 0001_tables.sql. Each
-- body is the 0.7.0 (schema v61) body with these changes only:
-- - removed, renamed or retyped columns (live task argument, result and terminal columns; the prepared rerun
--   columns; the history codec, content-type, prior-result and anchor columns; the clock-exception birth and
--   record times; the phase-2 terminal status, generation, node identity and reason columns);
-- - removed shims: the archive gate call, the REFERENCE rerun arm, the prior result of an administrative cancel;
-- - the move builds the rerun envelope from input and task_options (horsies_encode_rerun_input);
-- - each single terminal write passes the row it locked to horsies_move_task_to_history(horsies_tasks, ...), and
--   each batch terminal write calls the batch move stage horsies_move_tasks_to_history
--   (0006_history_functions.sql);
-- - each terminal write that misses passes its own kind and its fence kind (NONE, OWNER, OWNER_AND_GENERATION) to
--   horsies_terminalization_miss(uuid, text, text, text, timestamptz) (0008_history_maintenance_functions.sql),
--   which owns the equivalence classes of the kinds;
-- - the owned-claim functions require the claim generation (22023 on NULL, or on a NULL element of a batch) and
--   fence on it with no NULL arm;
-- - horsies_key_reservation_claim inserts with ON CONFLICT DO NOTHING and reads again, so concurrent first claims
--   of one key answer REPLAY or CONFLICT, not 23505.
-- The trigger functions are in 0001_tables.sql. The published history readers, among them the duplicate guard
-- horsies_task_history_duplicate_staged that the moves of 0006_history_functions.sql call, are rendered by
-- horsies_publish_history_readers (0008_history_maintenance_functions.sql); the install publishes them before it
-- returns.
--
-- Part of migration 1000: the install concatenates the files of this folder in name order and computes one
-- checksum over the result.

-- Composite types

CREATE TYPE horsies_terminalization_outcome AS (
	task_id uuid,
	ordinality bigint,
	outcome text,
	terminal_at timestamp with time zone,
	terminalization_kind text,
	observed_status text,
	observed_worker_id character varying,
	observed_claimed_at timestamp with time zone,
	guard_kind text,
	observed_guard jsonb
);

CREATE TYPE horsies_key_reservation_outcome AS (
	outcome text,
	task_id uuid,
	observed_fingerprint_version smallint
);

CREATE TYPE horsies_phase2_disposition AS (
	disposition text,
	workflow_id uuid,
	node_row_id uuid,
	task_index integer,
	workflow_status text,
	workflow_depth integer,
	root_workflow_id uuid,
	on_error text,
	node_status text,
	detail text
);

CREATE TYPE horsies_phase2_quarantine_verdict AS (
	verdict text,
	detail text
);

-- Functions

CREATE FUNCTION horsies_abandon_nodes_of_paused_workflows(p_workflow_ids uuid[]) RETURNS SETOF horsies_terminalization_outcome
    LANGUAGE plpgsql
    AS $$
DECLARE
    v_ids uuid[];
    v_terminal_at timestamptz;
BEGIN

    -- Discovery under the batch locking rule: SKIP LOCKED never waits on
    -- a row lock, so this batch cannot join any deadlock cycle; rows held
    -- by advisory-first singles are skipped and caught next sweep.
    SELECT array_agg(s.id) INTO v_ids
    FROM (
        SELECT t2.id FROM horsies_tasks t2
        WHERE t2.status = 'CLAIMED'
          AND EXISTS (
              SELECT 1
              FROM horsies_workflow_tasks wt
              JOIN horsies_workflows w ON w.id = wt.workflow_id
              WHERE wt.task_id = t2.id
                AND wt.workflow_id = ANY(p_workflow_ids)
                AND w.status = 'PAUSED'
                AND wt.status IN ('ENQUEUED', 'RUNNING')
          )
        FOR UPDATE OF t2 SKIP LOCKED
    ) s;
    IF v_ids IS NULL THEN
        RETURN;
    END IF;

    v_terminal_at := NOW();
    -- Outcome rows stream from the locked live rows before the move
    -- stage deletes them: reading them back through the partitioned
    -- parent by task id would be the rejected fan-out mechanism.
    -- RETURN QUERY materializes immediately; it does not end the
    -- function.
    RETURN QUERY SELECT
        t.id, NULL::bigint, 'APPLIED'::text,
        v_terminal_at, 'PAUSE_ABANDON_WORKFLOW'::text,
        t.status::text, t.claimed_by_worker_id::varchar, t.claimed_at,
        NULL::text, NULL::jsonb
    FROM horsies_tasks t
    WHERE t.id = ANY(v_ids);

    -- The batch move stage (0006_history_functions.sql) at the same
    -- terminal time.
    PERFORM horsies_move_tasks_to_history(
        v_ids, 'CANCELLED', 'PAUSE_ABANDON_WORKFLOW', v_terminal_at,
        NULL, 'TASK_CANCELLED', 'Workflow paused before task start'
    );
END
$$;

CREATE FUNCTION horsies_abandon_owned_node(p_task_id uuid, p_worker_id text, p_claimed_at timestamp with time zone) RETURNS SETOF horsies_terminalization_outcome
    LANGUAGE plpgsql
    AS $$
DECLARE
    v_task horsies_tasks;
    v_terminal_at timestamptz;
    v_workflow_id uuid;
    v_workflow_status text;
BEGIN
    IF p_claimed_at IS NULL THEN
        RAISE EXCEPTION 'claim generation must be non-NULL'
            USING ERRCODE = 'invalid_parameter_value';
    END IF;
    -- The workflow row first, in the workflow-then-task lock order. The
    -- caller read PAUSED before this call; a resume that committed since
    -- then shows here, and its status update waits for this lock.
    SELECT w.id, w.status
    INTO v_workflow_id, v_workflow_status
    FROM horsies_workflow_tasks wt
    JOIN horsies_workflows w ON w.id = wt.workflow_id
    WHERE wt.task_id = p_task_id
    ORDER BY w.id
    LIMIT 1
    FOR SHARE OF w;
    PERFORM pg_advisory_xact_lock(
        hashtextextended(p_task_id::text, 731)
    );
    -- The whole locked row: the move takes it as read here.
    SELECT t.*
    INTO v_task
    FROM horsies_tasks t
    WHERE t.id = p_task_id
      AND t.status = 'CLAIMED'
      AND t.claimed_by_worker_id = CAST(p_worker_id AS VARCHAR)
      AND t.claimed_at = p_claimed_at
    FOR UPDATE;
    IF NOT FOUND THEN
        RETURN QUERY SELECT * FROM horsies_terminalization_miss(
            p_task_id, 'PAUSE_ABANDON_CLAIM', 'OWNER_AND_GENERATION',
            p_worker_id, p_claimed_at
        );
        RETURN;
    END IF;

    -- Fence classified before guard: the claim is this caller's, and the
    -- workflow is no longer paused, so the task is runnable again.
    IF v_workflow_id IS NOT NULL AND v_workflow_status <> 'PAUSED' THEN
        RETURN QUERY SELECT
            p_task_id, NULL::bigint, 'SOURCE_STATE_CONFLICT'::text,
            NULL::timestamptz, NULL::text,
            v_task.status::text, v_task.claimed_by_worker_id, v_task.claimed_at,
            'WORKFLOW_STATUS'::text,
            jsonb_build_object(
                'workflow_id', v_workflow_id::text,
                'workflow_status', v_workflow_status
            );
        RETURN;
    END IF;

    v_terminal_at := NOW();
    PERFORM horsies_move_task_to_history(
        v_task, 'CANCELLED', 'PAUSE_ABANDON_CLAIM', v_terminal_at,
        NULL, 'TASK_CANCELLED', 'Workflow paused before task start'
    );
    RETURN QUERY SELECT
        p_task_id, NULL::bigint, 'APPLIED'::text,
        v_terminal_at, 'PAUSE_ABANDON_CLAIM'::text,
        v_task.status::text, v_task.claimed_by_worker_id, v_task.claimed_at,
        NULL::text, NULL::jsonb;
END
$$;

CREATE FUNCTION horsies_abandon_owned_nodes(p_ids uuid[], p_claimed_ats timestamp with time zone[], p_worker_id text) RETURNS SETOF horsies_terminalization_outcome
    LANGUAGE plpgsql
    AS $$
DECLARE
    v_applied_ids uuid[];
    v_refused_ids uuid[] := '{}';
    v_terminal_at timestamptz;
BEGIN
    IF p_ids IS NULL OR p_claimed_ats IS NULL THEN
        RAISE EXCEPTION 'batch arrays must be non-NULL'
            USING ERRCODE = 'invalid_parameter_value';
    END IF;
    IF cardinality(p_ids) <> cardinality(p_claimed_ats) THEN
        RAISE EXCEPTION
            'batch array lengths differ: ids=%, claimed_ats=%',
            cardinality(p_ids), cardinality(p_claimed_ats)
            USING ERRCODE = 'invalid_parameter_value';
    END IF;
    IF array_position(p_ids, NULL) IS NOT NULL THEN
        RAISE EXCEPTION 'batch task ids must be non-NULL'
            USING ERRCODE = 'invalid_parameter_value';
    END IF;
    IF array_position(p_claimed_ats, NULL) IS NOT NULL THEN
        RAISE EXCEPTION 'batch claim generations must be non-NULL'
            USING ERRCODE = 'invalid_parameter_value';
    END IF;
    IF cardinality(p_ids) <> (
        SELECT COUNT(DISTINCT item.id)
        FROM unnest(p_ids) AS item(id)
    ) THEN
        RAISE EXCEPTION 'batch task ids must be distinct'
            USING ERRCODE = 'invalid_parameter_value';
    END IF;

    -- Linked workflow rows first, in sorted id order.
    PERFORM 1 FROM (
        SELECT w.id FROM horsies_workflows w
        WHERE w.id IN (
            SELECT wt.workflow_id FROM horsies_workflow_tasks wt
            WHERE wt.task_id = ANY(p_ids)
        )
        ORDER BY w.id
        FOR SHARE
    ) locked_workflows;

    -- Waits-in-global-order: lock every present input row in sorted id
    -- order before anything is judged.
    PERFORM 1 FROM (
        SELECT t.id FROM horsies_tasks t
        WHERE t.id = ANY(p_ids)
        ORDER BY t.id
        FOR UPDATE
    ) locked;

    SELECT COALESCE(array_agg(t.id), '{}') INTO v_applied_ids
    FROM unnest(p_ids, p_claimed_ats)
        AS input(task_id, expected_claimed_at)
    JOIN horsies_tasks t ON t.id = input.task_id
    WHERE t.status = 'CLAIMED'
      AND t.claimed_by_worker_id = CAST(p_worker_id AS VARCHAR)
      AND t.claimed_at = input.expected_claimed_at
      AND NOT EXISTS (
          SELECT 1
          FROM horsies_workflow_tasks wt
          JOIN horsies_workflows w ON w.id = wt.workflow_id
          WHERE wt.task_id = t.id
            AND w.status <> 'PAUSED'
      );

    -- Fence classified before guard: claim-matched rows whose workflow
    -- is no longer PAUSED are refused with the workflow's evidence.
    SELECT COALESCE(array_agg(input.task_id), '{}') INTO v_refused_ids
    FROM unnest(p_ids, p_claimed_ats)
        AS input(task_id, expected_claimed_at)
    JOIN horsies_tasks t ON t.id = input.task_id
    WHERE t.status = 'CLAIMED'
      AND t.claimed_by_worker_id = CAST(p_worker_id AS VARCHAR)
      AND t.claimed_at = input.expected_claimed_at
      AND NOT (t.id = ANY(v_applied_ids));

    RETURN QUERY
    SELECT t.id, input.ordinality, 'SOURCE_STATE_CONFLICT'::text,
           NULL::timestamptz, NULL::text,
           t.status::text, t.claimed_by_worker_id::varchar, t.claimed_at,
           'WORKFLOW_STATUS'::text,
           jsonb_build_object(
               'workflow_id', moved_on.workflow_id::text,
               'workflow_status', moved_on.workflow_status
           )
    FROM horsies_tasks t
    JOIN unnest(p_ids) WITH ORDINALITY AS input(task_id, ordinality)
        ON input.task_id = t.id
    CROSS JOIN LATERAL (
        SELECT w.id AS workflow_id, w.status AS workflow_status
        FROM horsies_workflow_tasks wt
        JOIN horsies_workflows w ON w.id = wt.workflow_id
        WHERE wt.task_id = t.id
          AND w.status <> 'PAUSED'
        ORDER BY w.id
        LIMIT 1
    ) moved_on
    WHERE t.id = ANY(v_refused_ids);

    IF cardinality(v_applied_ids) > 0 THEN

    v_terminal_at := NOW();
    -- Outcome rows stream from the locked live rows before the move
    -- stage deletes them: reading them back through the partitioned
    -- parent by task id would be the rejected fan-out mechanism.
    -- RETURN QUERY materializes immediately; it does not end the
    -- function.
    RETURN QUERY SELECT
        t.id, input.ordinality, 'APPLIED'::text,
        v_terminal_at, 'PAUSE_ABANDON_CLAIM_BATCH'::text,
        t.status::text, t.claimed_by_worker_id::varchar, t.claimed_at,
        NULL::text, NULL::jsonb
    FROM horsies_tasks t
    JOIN unnest(p_ids) WITH ORDINALITY AS input(task_id, ordinality)
        ON input.task_id = t.id
    WHERE t.id = ANY(v_applied_ids);

    -- The batch move stage (0006_history_functions.sql) at the same
    -- terminal time.
    PERFORM horsies_move_tasks_to_history(
        v_applied_ids, 'CANCELLED', 'PAUSE_ABANDON_CLAIM_BATCH', v_terminal_at,
        NULL, 'TASK_CANCELLED', 'Workflow paused before task start'
    );
    END IF;

    -- Every other input gets its answer through the ONE miss
    -- classifier, at its own ordinality.
    RETURN QUERY
    SELECT input.task_id, input.ordinality,
           m.outcome, m.terminal_at, m.terminalization_kind,
           m.observed_status, m.observed_worker_id, m.observed_claimed_at,
           m.guard_kind, m.observed_guard
    FROM unnest(p_ids, p_claimed_ats) WITH ORDINALITY
        AS input(task_id, expected_claimed_at, ordinality)
    CROSS JOIN LATERAL horsies_terminalization_miss(
        input.task_id, 'PAUSE_ABANDON_CLAIM_BATCH', 'OWNER_AND_GENERATION',
        p_worker_id, input.expected_claimed_at
    ) m
    WHERE NOT (input.task_id = ANY(v_applied_ids))
      AND NOT (input.task_id = ANY(v_refused_ids));
END
$$;

CREATE FUNCTION horsies_cancel_nodes_of_cancelled_workflow(p_workflow_ids uuid[]) RETURNS SETOF horsies_terminalization_outcome
    LANGUAGE plpgsql
    AS $$
DECLARE
    v_ids uuid[];
    v_terminal_at timestamptz;
BEGIN

    -- Discovery under the batch locking rule: SKIP LOCKED never waits on
    -- a row lock, so this batch cannot join any deadlock cycle; rows held
    -- by advisory-first singles are skipped and caught next sweep.
    SELECT array_agg(s.id) INTO v_ids
    FROM (
        SELECT t2.id FROM horsies_tasks t2
        WHERE t2.status IN ('PENDING', 'CLAIMED', 'RUNNING')
          AND EXISTS (
              SELECT 1
              FROM horsies_workflow_tasks wt
              JOIN horsies_workflows w ON w.id = wt.workflow_id
              WHERE wt.task_id = t2.id
                AND wt.workflow_id = ANY(p_workflow_ids)
                -- EXPIRED propagates exactly as CANCELLED: one batch
                -- serves both, and the backing row carries the
                -- workflow-cancel kind either way.
                AND w.status IN ('CANCELLED', 'EXPIRED')
                AND wt.status = 'ENQUEUED'
          )
        FOR UPDATE OF t2 SKIP LOCKED
    ) s;
    IF v_ids IS NULL THEN
        RETURN;
    END IF;

    v_terminal_at := NOW();
    -- Outcome rows stream from the locked live rows before the move
    -- stage deletes them: reading them back through the partitioned
    -- parent by task id would be the rejected fan-out mechanism.
    -- RETURN QUERY materializes immediately; it does not end the
    -- function.
    RETURN QUERY SELECT
        t.id, NULL::bigint, 'APPLIED'::text,
        v_terminal_at, 'WORKFLOW_CANCEL_WORKFLOW'::text,
        t.status::text, t.claimed_by_worker_id::varchar, t.claimed_at,
        NULL::text, NULL::jsonb
    FROM horsies_tasks t
    WHERE t.id = ANY(v_ids);

    -- The batch move stage (0006_history_functions.sql) at the same
    -- terminal time.
    PERFORM horsies_move_tasks_to_history(
        v_ids, 'CANCELLED', 'WORKFLOW_CANCEL_WORKFLOW', v_terminal_at,
        NULL, NULL, NULL
    );
END
$$;

CREATE FUNCTION horsies_cancel_owned_node(p_task_id uuid, p_worker_id text, p_claimed_at timestamp with time zone, p_accepts_requeued_pending boolean) RETURNS SETOF horsies_terminalization_outcome
    LANGUAGE plpgsql
    AS $$
DECLARE
    v_task horsies_tasks;
    v_terminal_at timestamptz;
BEGIN
    IF p_claimed_at IS NULL THEN
        RAISE EXCEPTION 'claim generation must be non-NULL'
            USING ERRCODE = 'invalid_parameter_value';
    END IF;
    PERFORM pg_advisory_xact_lock(
        hashtextextended(p_task_id::text, 731)
    );
    -- Full ownership fence, with the explicit carve-out for a task this
    -- same child observed as requeued to PENDING, where no claim remains
    -- to fence; the carve-out is a property of this variant only. The
    -- whole locked row: the move takes it as read here.
    SELECT t.*
    INTO v_task
    FROM horsies_tasks t
    WHERE t.id = p_task_id
      AND (
          (
              t.status = 'CLAIMED'
              AND t.claimed_by_worker_id = CAST(p_worker_id AS VARCHAR)
              AND t.claimed_at = p_claimed_at
          )
          OR (p_accepts_requeued_pending AND t.status = 'PENDING')
      )
    FOR UPDATE;
    IF NOT FOUND THEN
        RETURN QUERY SELECT * FROM horsies_terminalization_miss(
            p_task_id, 'WORKFLOW_CANCEL_CLAIM', 'OWNER_AND_GENERATION',
            p_worker_id, p_claimed_at
        );
        RETURN;
    END IF;

    v_terminal_at := NOW();
    PERFORM horsies_move_task_to_history(
        v_task, 'CANCELLED', 'WORKFLOW_CANCEL_CLAIM', v_terminal_at,
        NULL, NULL, NULL
    );
    RETURN QUERY SELECT
        p_task_id, NULL::bigint, 'APPLIED'::text,
        v_terminal_at, 'WORKFLOW_CANCEL_CLAIM'::text,
        v_task.status::text, v_task.claimed_by_worker_id, v_task.claimed_at,
        NULL::text, NULL::jsonb;
END
$$;

CREATE FUNCTION horsies_cancel_owned_nodes(p_ids uuid[], p_claimed_ats timestamp with time zone[], p_worker_id text) RETURNS SETOF horsies_terminalization_outcome
    LANGUAGE plpgsql
    AS $$
DECLARE
    v_applied_ids uuid[];
    v_refused_ids uuid[] := '{}';
    v_terminal_at timestamptz;
BEGIN
    IF p_ids IS NULL OR p_claimed_ats IS NULL THEN
        RAISE EXCEPTION 'batch arrays must be non-NULL'
            USING ERRCODE = 'invalid_parameter_value';
    END IF;
    IF cardinality(p_ids) <> cardinality(p_claimed_ats) THEN
        RAISE EXCEPTION
            'batch array lengths differ: ids=%, claimed_ats=%',
            cardinality(p_ids), cardinality(p_claimed_ats)
            USING ERRCODE = 'invalid_parameter_value';
    END IF;
    IF array_position(p_ids, NULL) IS NOT NULL THEN
        RAISE EXCEPTION 'batch task ids must be non-NULL'
            USING ERRCODE = 'invalid_parameter_value';
    END IF;
    IF array_position(p_claimed_ats, NULL) IS NOT NULL THEN
        RAISE EXCEPTION 'batch claim generations must be non-NULL'
            USING ERRCODE = 'invalid_parameter_value';
    END IF;
    IF cardinality(p_ids) <> (
        SELECT COUNT(DISTINCT item.id)
        FROM unnest(p_ids) AS item(id)
    ) THEN
        RAISE EXCEPTION 'batch task ids must be distinct'
            USING ERRCODE = 'invalid_parameter_value';
    END IF;

    -- Waits-in-global-order: lock every present input row in sorted id
    -- order before anything is judged.
    PERFORM 1 FROM (
        SELECT t.id FROM horsies_tasks t
        WHERE t.id = ANY(p_ids)
        ORDER BY t.id
        FOR UPDATE
    ) locked;

    SELECT COALESCE(array_agg(t.id), '{}') INTO v_applied_ids
    FROM unnest(p_ids, p_claimed_ats)
        AS input(task_id, expected_claimed_at)
    JOIN horsies_tasks t ON t.id = input.task_id
    WHERE t.status = 'CLAIMED'
      AND t.claimed_by_worker_id = CAST(p_worker_id AS VARCHAR)
      AND t.claimed_at = input.expected_claimed_at;

    IF cardinality(v_applied_ids) > 0 THEN

    v_terminal_at := NOW();
    -- Outcome rows stream from the locked live rows before the move
    -- stage deletes them: reading them back through the partitioned
    -- parent by task id would be the rejected fan-out mechanism.
    -- RETURN QUERY materializes immediately; it does not end the
    -- function.
    RETURN QUERY SELECT
        t.id, input.ordinality, 'APPLIED'::text,
        v_terminal_at, 'WORKFLOW_CANCEL_CLAIM_BATCH'::text,
        t.status::text, t.claimed_by_worker_id::varchar, t.claimed_at,
        NULL::text, NULL::jsonb
    FROM horsies_tasks t
    JOIN unnest(p_ids) WITH ORDINALITY AS input(task_id, ordinality)
        ON input.task_id = t.id
    WHERE t.id = ANY(v_applied_ids);

    -- The batch move stage (0006_history_functions.sql) at the same
    -- terminal time.
    PERFORM horsies_move_tasks_to_history(
        v_applied_ids, 'CANCELLED', 'WORKFLOW_CANCEL_CLAIM_BATCH', v_terminal_at,
        NULL, NULL, NULL
    );
    END IF;

    -- Every other input gets its answer through the ONE miss
    -- classifier, at its own ordinality.
    RETURN QUERY
    SELECT input.task_id, input.ordinality,
           m.outcome, m.terminal_at, m.terminalization_kind,
           m.observed_status, m.observed_worker_id, m.observed_claimed_at,
           m.guard_kind, m.observed_guard
    FROM unnest(p_ids, p_claimed_ats) WITH ORDINALITY
        AS input(task_id, expected_claimed_at, ordinality)
    CROSS JOIN LATERAL horsies_terminalization_miss(
        input.task_id, 'WORKFLOW_CANCEL_CLAIM_BATCH', 'OWNER_AND_GENERATION',
        p_worker_id, input.expected_claimed_at
    ) m
    WHERE NOT (input.task_id = ANY(v_applied_ids))
      AND NOT (input.task_id = ANY(v_refused_ids));
END
$$;

CREATE FUNCTION horsies_claim_plan(p_total integer, p_available jsonb, p_routes jsonb, p_queues jsonb, p_queue_priority jsonb, p_queue_max_concurrency jsonb, p_queue_counts jsonb, p_max_claim_batch integer) RETURNS TABLE(queue_name text, row_class text, arm text, take integer)
    LANGUAGE plpgsql
    SET plan_cache_mode TO 'force_generic_plan'
    SET jit TO 'off'
    AS $$
#variable_conflict use_column
DECLARE
    -- Room left in each queue (by position in p_queues), in each budget
    -- (by position in v_budgets), and in the pass.
    v_queue_room int[];
    v_budgets text[];
    v_budget_room int[];
    v_total_room int := p_total;
    v_picked_queue text[] := ARRAY[]::text[];
    v_picked_class text[] := ARRAY[]::text[];
    v_picked_arm text[] := ARRAY[]::text[];
    v_row record;
BEGIN
    SELECT array_agg(LEAST(
               p_total,
               CASE WHEN p_queue_max_concurrency ? q.name THEN
                   GREATEST(0, (p_queue_max_concurrency ->> q.name)::int
                       - COALESCE((p_queue_counts ->> q.name)::int, 0))
                   ELSE p_total END,
               CASE WHEN p_max_claim_batch > 0 THEN p_max_claim_batch
                   ELSE p_total END
           ) ORDER BY q.slot)
    INTO v_queue_room
    FROM jsonb_array_elements_text(p_queues) WITH ORDINALITY AS q(name, slot);

    SELECT array_agg(b.budget_class ORDER BY b.budget_class),
           array_agg(b.available::int ORDER BY b.budget_class)
    INTO v_budgets, v_budget_room
    FROM jsonb_each_text(p_available) AS b(budget_class, available);

    FOR v_row IN
        SELECT s.queue_slot, s.budget_slot, s.queue_name, s.row_class, c.arm
        FROM (
            SELECT q.slot::int AS queue_slot, q.name AS queue_name,
                   COALESCE((p_queue_priority ->> q.name)::int, 100) AS qprio,
                   r.row_class, b.budget_slot,
                   LEAST(v_queue_room[q.slot::int],
                         v_budget_room[b.budget_slot]) AS head
            FROM jsonb_array_elements_text(p_queues) WITH ORDINALITY AS q(name, slot)
            CROSS JOIN jsonb_each_text(p_routes) AS r(row_class, budget_class)
            CROSS JOIN LATERAL (
                SELECT array_position(v_budgets, r.budget_class) AS budget_slot
            ) b
            WHERE LEAST(v_queue_room[q.slot::int],
                        v_budget_room[b.budget_slot]) > 0
        ) s
        CROSS JOIN LATERAL (
            SELECT m.arm, m.priority, m.enqueued_at, m.id
            FROM (
                (SELECT 'PENDING'::text AS arm, t.priority, t.enqueued_at, t.id
                 FROM horsies_tasks t
                 WHERE t.queue_name = s.queue_name
                   AND t.execution = s.row_class
                   AND t.status = 'PENDING'
                   AND t.enqueued_at <= now()
                   AND (t.next_retry_at IS NULL OR t.next_retry_at <= now())
                   AND (t.good_until IS NULL OR t.good_until > now())
                 ORDER BY t.priority ASC, t.enqueued_at ASC, t.id ASC
                 LIMIT s.head)
                UNION ALL
                (SELECT 'EXPIRED'::text AS arm, t.priority, t.enqueued_at, t.id
                 FROM horsies_tasks t
                 WHERE t.queue_name = s.queue_name
                   AND t.execution = s.row_class
                   AND t.status = 'CLAIMED'
                   AND t.claim_expires_at IS NOT NULL
                   AND t.claim_expires_at < now()
                   AND t.enqueued_at <= now()
                   AND (t.next_retry_at IS NULL OR t.next_retry_at <= now())
                   AND (t.good_until IS NULL OR t.good_until > now())
                 ORDER BY t.priority ASC, t.enqueued_at ASC, t.id ASC
                 LIMIT s.head)
            ) m
            ORDER BY m.priority ASC, m.enqueued_at ASC, m.id ASC
            LIMIT s.head
        ) c
        ORDER BY s.qprio ASC, c.priority ASC, c.enqueued_at ASC, c.id ASC
    LOOP
        EXIT WHEN v_total_room = 0;
        CONTINUE WHEN v_queue_room[v_row.queue_slot] = 0
            OR v_budget_room[v_row.budget_slot] = 0;
        v_queue_room[v_row.queue_slot] := v_queue_room[v_row.queue_slot] - 1;
        v_budget_room[v_row.budget_slot] := v_budget_room[v_row.budget_slot] - 1;
        v_total_room := v_total_room - 1;
        v_picked_queue := v_picked_queue || v_row.queue_name;
        v_picked_class := v_picked_class || v_row.row_class;
        v_picked_arm := v_picked_arm || v_row.arm;
    END LOOP;

    RETURN QUERY
    SELECT p.queue_name, p.row_class, p.arm, count(*)::int
    FROM unnest(v_picked_queue, v_picked_class, v_picked_arm)
        AS p(queue_name, row_class, arm)
    GROUP BY p.queue_name, p.row_class, p.arm;
END;
$$;

CREATE FUNCTION horsies_encode_rerun_input(p_input text, p_task_options text) RETURNS bytea
    LANGUAGE sql IMMUTABLE
    AS $$
    -- Rerun envelope version 1: the stored input and options texts, framed
    -- byte for byte. Both texts passed the strict JSON check at enqueue, so
    -- the envelope is valid JSON. A NULL input gives NULL.
    SELECT convert_to(
        '{"input":' || p_input || ',"options":'
        || COALESCE(p_task_options, 'null') || '}',
        'UTF8'
    )
$$;

CREATE FUNCTION horsies_encode_task_attempts(p_task_id uuid) RETURNS bytea
    LANGUAGE sql STABLE STRICT
    AS $$
    SELECT convert_to(
        '[' || COALESCE(
            string_agg(
                '[' || to_jsonb(a.attempt)::text || ',' ||
                to_jsonb(a.outcome)::text || ',' ||
                to_jsonb(a.will_retry)::text || ',' ||
                to_jsonb(
                    floor(
                        extract(epoch FROM a.started_at) * 1000000
                    )::bigint
                )::text || ',' ||
                to_jsonb(
                    floor(
                        extract(epoch FROM a.finished_at) * 1000000
                    )::bigint
                )::text || ',' ||
                COALESCE(to_jsonb(a.error_code)::text, 'null') || ',' ||
                COALESCE(to_jsonb(a.error_message)::text, 'null') || ',' ||
                COALESCE(to_jsonb(a.failed_reason)::text, 'null') || ',' ||
                COALESCE(to_jsonb(a.worker_id)::text, 'null') || ',' ||
                COALESCE(to_jsonb(a.worker_hostname)::text, 'null') || ',' ||
                COALESCE(to_jsonb(a.worker_pid)::text, 'null') || ',' ||
                COALESCE(
                    to_jsonb(a.worker_process_name)::text,
                    'null'
                ) || ']',
                ',' ORDER BY a.attempt
            ),
            ''
        ) || ']',
        'UTF8'
    )
    FROM horsies_task_attempts AS a
    WHERE a.task_id = p_task_id
$$;

CREATE FUNCTION horsies_find_non_runnable_workflow_tasks(p_task_ids uuid[]) RETURNS TABLE(id uuid, status character varying)
    LANGUAGE plpgsql STABLE
    SET plan_cache_mode TO 'force_generic_plan'
    AS $$
BEGIN
    RETURN QUERY
    WITH claimed_nodes AS MATERIALIZED (
        SELECT t.id, wt.workflow_id
        FROM horsies_tasks t
        JOIN horsies_workflow_tasks wt ON wt.task_id = t.id
        WHERE t.id = ANY(p_task_ids)
    )
    SELECT n.id, w.status
    FROM claimed_nodes n
    JOIN LATERAL (
        SELECT workflow.status
        FROM horsies_workflows workflow
        WHERE workflow.id = n.workflow_id
        -- Keep each workflow lookup tied to its claimed node before the
        -- status filter.
        OFFSET 0
    ) w ON w.status IN ('PAUSED', 'CANCELLED')
    WHERE EXISTS (
        SELECT 1 FROM horsies_workflows guard_workflow
        WHERE guard_workflow.status IN ('PAUSED', 'CANCELLED')
    );
END
$$;

-- The claim of an idempotency key by one send.
-- - A LIVE reservation of the key, or a TERMINAL one that expires after statement_timestamp(), answers REPLAY when
--   its fingerprint version and fingerprint equal the sent ones, else CONFLICT; both name its task_id and fingerprint
--   version. An expired TERMINAL reservation counts as absent: the claim deletes it. With no reservation the claim
--   inserts a LIVE one for p_task_id: APPLIED.
-- - Concurrent first claims of one key: the insert does nothing on a conflict. An insert that meets the uncommitted
--   reservation of another transaction waits for it; after a commit the next round reads that reservation in a new
--   statement (READ COMMITTED: a new snapshot) and answers REPLAY or CONFLICT, after a rollback the insert goes on.
--   One claim applies; every other claim is classified against it.
-- - A round is the read, the delete and the insert. A round ends with no answer only when the reservation that
--   stopped its insert was deleted or expired before the next read. After c_claim_rounds rounds with no answer the
--   claim raises 40001 (serialization_failure); a resend of the command claims again.
-- - Under REPEATABLE READ or SERIALIZABLE, an insert that meets a reservation committed after the transaction
--   snapshot raises 40001 (PostgreSQL's ON CONFLICT rule).
-- Refuses with 22023, before any write: a digest or fingerprint that is not 32 bytes; a window outside (0, 30 days];
-- a window with a year, month or day field (terminal time plus such a window depends on the session time zone; the
-- clients send a time interval, made from microseconds).
-- Locks the read reservation FOR UPDATE and the deleted expired reservation until the caller's transaction ends.
-- It does not commit.
CREATE FUNCTION horsies_key_reservation_claim(p_key_digest bytea, p_key_scope_version smallint, p_reservation_window interval, p_fingerprint_version smallint, p_fingerprint bytea, p_task_id uuid) RETURNS horsies_key_reservation_outcome
    LANGUAGE plpgsql
    AS $$
DECLARE
    c_claim_rounds CONSTANT integer := 3;
    v_task_id uuid;
    v_fingerprint_version smallint;
    v_fingerprint bytea;
BEGIN
    IF octet_length(p_key_digest) <> 32 THEN
        RAISE EXCEPTION USING ERRCODE = 'invalid_parameter_value',
            MESSAGE = 'key digest must be 32 bytes';
    END IF;
    IF octet_length(p_fingerprint) <> 32 THEN
        RAISE EXCEPTION USING ERRCODE = 'invalid_parameter_value',
            MESSAGE = 'fingerprint must be 32 bytes';
    END IF;
    IF p_reservation_window <= interval '0'
       OR p_reservation_window > interval '30 days' THEN
        RAISE EXCEPTION USING ERRCODE = 'invalid_parameter_value',
            MESSAGE = 'reservation window must be positive and at most 30 days';
    END IF;
    IF extract(year FROM p_reservation_window) <> 0 OR extract(month FROM p_reservation_window) <> 0
       OR extract(day FROM p_reservation_window) <> 0 THEN
        RAISE EXCEPTION USING ERRCODE = 'invalid_parameter_value',
            MESSAGE = 'reservation window must be a time interval with no year, month or day field',
            HINT = 'Make it from microseconds: <microseconds>::bigint * interval ''1 microsecond''.';
    END IF;

    FOR v_round IN 1..c_claim_rounds LOOP
        SELECT task_id, fingerprint_version, command_fingerprint
        INTO v_task_id, v_fingerprint_version, v_fingerprint
        FROM horsies_key_reservations
        WHERE idempotency_key_digest = p_key_digest
          AND (disposition = 'LIVE' OR expires_at > statement_timestamp())
        FOR UPDATE;
        IF FOUND THEN
            IF v_fingerprint = p_fingerprint
               AND v_fingerprint_version = p_fingerprint_version THEN
                RETURN ROW('REPLAY', v_task_id, v_fingerprint_version)
                    ::horsies_key_reservation_outcome;
            END IF;
            RETURN ROW('CONFLICT', v_task_id, v_fingerprint_version)
                ::horsies_key_reservation_outcome;
        END IF;

        DELETE FROM horsies_key_reservations
        WHERE idempotency_key_digest = p_key_digest
          AND disposition = 'TERMINAL'
          AND expires_at <= statement_timestamp();

        INSERT INTO horsies_key_reservations (
            idempotency_key_digest, key_scope_version,
            fingerprint_version, command_fingerprint, task_id,
            disposition, reservation_window, expires_at
        ) VALUES (
            p_key_digest, p_key_scope_version,
            p_fingerprint_version, p_fingerprint, p_task_id,
            'LIVE', p_reservation_window, NULL
        )
        ON CONFLICT (idempotency_key_digest) DO NOTHING;
        IF FOUND THEN
            RETURN ROW('APPLIED', p_task_id, NULL)
                ::horsies_key_reservation_outcome;
        END IF;
    END LOOP;

    RAISE EXCEPTION USING ERRCODE = 'serialization_failure',
        MESSAGE = 'key reservation claim: the reservation of the key changed in every round',
        DETAIL = format('key digest %s, %s rounds', encode(p_key_digest, 'hex'), c_claim_rounds),
        HINT = 'Send again: the next claim reads the reservation that is committed then.';
END
$$;

CREATE FUNCTION horsies_key_reservation_cleanup(p_batch_size integer) RETURNS integer
    LANGUAGE plpgsql
    AS $$
DECLARE
    v_deleted integer;
BEGIN
    IF p_batch_size <= 0 THEN
        RAISE EXCEPTION USING ERRCODE = 'invalid_parameter_value',
            MESSAGE = 'cleanup batch size must be positive';
    END IF;
    WITH targets AS (
        SELECT idempotency_key_digest
        FROM horsies_key_reservations
        WHERE disposition = 'TERMINAL'
          AND expires_at <= statement_timestamp()
        ORDER BY expires_at
        LIMIT p_batch_size
        FOR UPDATE SKIP LOCKED
    )
    DELETE FROM horsies_key_reservations AS reservations
    USING targets
    WHERE reservations.idempotency_key_digest = targets.idempotency_key_digest;
    GET DIAGNOSTICS v_deleted = ROW_COUNT;
    RETURN v_deleted;
END
$$;

CREATE FUNCTION horsies_key_reservation_terminalize(p_key_digest bytea, p_task_id uuid, p_terminal_at timestamp with time zone) RETURNS boolean
    LANGUAGE plpgsql
    AS $$
DECLARE
    v_updated integer;
BEGIN
    UPDATE horsies_key_reservations
    SET disposition = 'TERMINAL',
        expires_at = p_terminal_at + reservation_window
    WHERE idempotency_key_digest = p_key_digest
      AND task_id = p_task_id
      AND disposition = 'LIVE';
    GET DIAGNOSTICS v_updated = ROW_COUNT;
    RETURN v_updated = 1;
END
$$;

CREATE FUNCTION horsies_key_reservation_terminalize_batch(p_key_digests bytea[], p_task_ids uuid[], p_terminal_at timestamp with time zone) RETURNS integer
    LANGUAGE plpgsql
    AS $$
DECLARE
    v_updated integer;
BEGIN
    IF cardinality(p_key_digests) <> cardinality(p_task_ids) THEN
        RAISE EXCEPTION USING ERRCODE = 'invalid_parameter_value',
            MESSAGE = 'digest and task arrays must pair element-wise';
    END IF;
    UPDATE horsies_key_reservations r
    SET disposition = 'TERMINAL',
        expires_at = p_terminal_at + r.reservation_window
    FROM unnest(p_key_digests, p_task_ids) AS pair(key_digest, task_id)
    WHERE r.idempotency_key_digest = pair.key_digest
      AND r.task_id = pair.task_id
      AND r.disposition = 'LIVE';
    GET DIAGNOSTICS v_updated = ROW_COUNT;
    RETURN v_updated;
END
$$;

CREATE FUNCTION horsies_phase2_consume(p_task_id uuid, p_terminal_node_status text) RETURNS horsies_phase2_disposition
    LANGUAGE plpgsql
    SET plan_cache_mode TO 'force_generic_plan'
    AS $$
DECLARE
    v_pending horsies_workflow_phase2_pending%ROWTYPE;
    v_wf record;
    v_node record;
    v_payload bytea;
    v_digest bytea;
    v_version smallint;
    v_envelope_version smallint;
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
                       NULL, NULL, NULL,
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
                       v_node.status, NULL)
                ::horsies_phase2_disposition;
        END IF;
        RETURN ROW('PENDING_ABSENT', v_node.workflow_id, v_node.id,
                   v_node.task_index, v_wf.status, v_wf.depth,
                   v_wf.root_workflow_id, v_wf.on_error,
                   v_node.status,
                   'no pending row; node not terminal')
            ::horsies_phase2_disposition;
    END IF;

    -- Lock order: workflow row first, node row second, pending third.
    SELECT w.status, w.depth, w.root_workflow_id, w.on_error
    INTO v_wf
    FROM horsies_workflows w
    WHERE w.id = v_pending.workflow_id
    FOR UPDATE;
    IF NOT FOUND THEN
        RETURN ROW('SOURCE_STATE_CONFLICT', v_pending.workflow_id,
                   v_pending.workflow_node_row_id, NULL, NULL, NULL,
                   NULL, NULL, NULL,
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
                   NULL,
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
                   v_wf.on_error, v_node.status, NULL)
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
               h.history_schema_version, h.result_envelope_version
        INTO v_source_task, v_payload, v_digest, v_version, v_envelope_version
        FROM horsies_task_history h
        WHERE h.retention_class_key = v_pending.history_class
          AND h.terminal_at = v_pending.history_anchor
          AND h.task_id = p_task_id;
        IF NOT FOUND THEN
            RETURN ROW('SOURCE_ABSENT', v_pending.workflow_id, v_node.id,
                       v_node.task_index, v_wf.status, v_wf.depth,
                       v_wf.root_workflow_id, v_wf.on_error, v_node.status,
                       'history row absent at locator')
                ::horsies_phase2_disposition;
        END IF;
    ELSE
        SELECT q.task_id, q.result_payload, q.result_digest,
               q.history_schema_version, q.result_envelope_version
        INTO v_source_task, v_payload, v_digest, v_version, v_envelope_version
        FROM horsies_workflow_phase2_quarantine q
        WHERE q.task_id = v_pending.quarantine_task_id;
        IF NOT FOUND THEN
            RETURN ROW('SOURCE_ABSENT', v_pending.workflow_id, v_node.id,
                       v_node.task_index, v_wf.status, v_wf.depth,
                       v_wf.root_workflow_id, v_wf.on_error, v_node.status,
                       'quarantine row absent at locator')
                ::horsies_phase2_disposition;
        END IF;
    END IF;

    IF v_source_task <> p_task_id THEN
        RETURN ROW('SOURCE_STATE_CONFLICT', v_pending.workflow_id,
                   v_node.id, v_node.task_index, v_wf.status, v_wf.depth,
                   v_wf.root_workflow_id, v_wf.on_error, v_node.status,
                   'source row carries a different task identity')
            ::horsies_phase2_disposition;
    END IF;
    IF v_version IS DISTINCT FROM v_pending.history_schema_version
       OR v_version <> 1 THEN
        RETURN ROW('SOURCE_VERSION_CONFLICT', v_pending.workflow_id,
                   v_node.id, v_node.task_index, v_wf.status, v_wf.depth,
                   v_wf.root_workflow_id, v_wf.on_error, v_node.status,
                   'source schema version disagrees with locator')
            ::horsies_phase2_disposition;
    END IF;
    -- Result envelope version 1 is the only format this reader knows; the source keeps its row, and the recovery
    -- pass records SOURCE_VERSION_CONFLICT as the failure class, as for an unknown history schema version.
    IF v_envelope_version <> 1 THEN
        RETURN ROW('SOURCE_VERSION_CONFLICT', v_pending.workflow_id,
                   v_node.id, v_node.task_index, v_wf.status, v_wf.depth,
                   v_wf.root_workflow_id, v_wf.on_error, v_node.status,
                   format('source result envelope version %s is not known to this reader',
                          v_envelope_version))
            ::horsies_phase2_disposition;
    END IF;
    IF v_digest IS DISTINCT FROM v_pending.result_digest
       OR v_payload IS NULL
       OR sha256(v_payload) <> v_pending.result_digest THEN
        RETURN ROW('SOURCE_DIGEST_MISMATCH', v_pending.workflow_id,
                   v_node.id, v_node.task_index, v_wf.status, v_wf.depth,
                   v_wf.root_workflow_id, v_wf.on_error, v_node.status,
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
                   p_terminal_node_status, NULL)
            ::horsies_phase2_disposition;
    END IF;
    RETURN ROW('ALREADY_APPLIED', v_pending.workflow_id, v_node.id,
               v_node.task_index, v_wf.status, v_wf.depth,
               v_wf.root_workflow_id, v_wf.on_error, v_node.status, NULL)
        ::horsies_phase2_disposition;
END
$$;

CREATE FUNCTION horsies_phase2_quarantine_one(p_task_id uuid) RETURNS horsies_phase2_quarantine_verdict
    LANGUAGE plpgsql
    AS $$
DECLARE
    v_pending horsies_workflow_phase2_pending%ROWTYPE;
    v_hist record;
    v_copy record;
BEGIN
    -- The only lock this function takes: the pending row. Single-tier;
    -- consumption acquiring workflow -> node -> pending can wait on it
    -- but no cycle can form because nothing else is held here.
    SELECT * INTO v_pending
    FROM horsies_workflow_phase2_pending
    WHERE task_id = p_task_id
    FOR UPDATE;
    IF NOT FOUND THEN
        RETURN ROW('PENDING_GONE', 'no pending row; locator drained')
            ::horsies_phase2_quarantine_verdict;
    END IF;
    IF v_pending.recovery_source <> 'HISTORY' THEN
        RETURN ROW('ALREADY_QUARANTINED',
                   'pending already repointed at quarantine')
            ::horsies_phase2_quarantine_verdict;
    END IF;

    PERFORM 1
    FROM horsies_workflow_tasks wt
    WHERE wt.id = v_pending.workflow_node_row_id
      AND wt.workflow_id = v_pending.workflow_id;
    IF NOT FOUND THEN
        RETURN ROW('NODE_ROW_ABSENT',
                   'node row absent while pending exists')
            ::horsies_phase2_quarantine_verdict;
    END IF;

    -- One-leaf parent probe, NOT the rejected fan-out: the locator
    -- supplies both partition keys, so LIST (class) and RANGE (anchor)
    -- prune to exactly one leaf at plan time.
    SELECT h.task_id, h.terminalization_kind,
           h.terminal_at, h.history_schema_version,
           h.result_envelope_version, h.result_payload, h.result_digest
    INTO v_hist
    FROM horsies_task_history h
    WHERE h.retention_class_key = v_pending.history_class
      AND h.terminal_at = v_pending.history_anchor
      AND h.task_id = p_task_id;
    IF NOT FOUND THEN
        RETURN ROW('SOURCE_ABSENT', 'history row absent at locator')
            ::horsies_phase2_quarantine_verdict;
    END IF;

    BEGIN
        INSERT INTO horsies_workflow_phase2_quarantine (
            task_id, workflow_id, workflow_node_row_id,
            terminalization_kind, terminal_at,
            retention_class_key,
            history_schema_version, result_envelope_version,
            result_payload, result_digest,
            quarantined_at
        ) VALUES (
            v_hist.task_id, v_pending.workflow_id,
            v_pending.workflow_node_row_id,
            v_hist.terminalization_kind, v_hist.terminal_at,
            v_pending.history_class,
            v_hist.history_schema_version, v_hist.result_envelope_version,
            v_hist.result_payload, v_hist.result_digest,
            statement_timestamp()
        );

        -- Verification on the copy itself: read the row back and hold
        -- it against the pending locator, so a projection defect is a
        -- refusal, not a corrupt quarantine row.
        SELECT q.task_id, q.result_payload, q.result_digest,
               q.history_schema_version, q.result_envelope_version
        INTO STRICT v_copy
        FROM horsies_workflow_phase2_quarantine q
        WHERE q.task_id = p_task_id;
        IF v_copy.task_id IS DISTINCT FROM p_task_id
           OR v_copy.result_digest IS DISTINCT FROM v_pending.result_digest
           OR v_copy.result_payload IS NULL
           OR sha256(v_copy.result_payload) <> v_copy.result_digest
           OR v_copy.history_schema_version
              IS DISTINCT FROM v_pending.history_schema_version
        THEN
            RAISE EXCEPTION 'quarantine copy disagrees with locator'
                USING ERRCODE = 'HQ001';
        END IF;
        -- Result envelope version 1 is the only format this reader knows: a copy of another version is refused
        -- like a copy whose history schema version disagrees.
        IF v_copy.result_envelope_version <> 1 THEN
            RAISE EXCEPTION
                'quarantine source result envelope version % is not known to this reader',
                v_copy.result_envelope_version
                USING ERRCODE = 'HQ001';
        END IF;
    EXCEPTION
        WHEN SQLSTATE 'HQ001' OR unique_violation OR not_null_violation
             OR check_violation THEN
            -- The sub-transaction rolls the copy back; pending keeps its
            -- history locator and the leaf stays pinned.
            RETURN ROW('COPY_VERIFICATION_FAILED', SQLERRM)
                ::horsies_phase2_quarantine_verdict;
    END;

    UPDATE horsies_workflow_phase2_pending
    SET recovery_source = 'QUARANTINE',
        quarantine_task_id = p_task_id,
        history_class = NULL,
        history_anchor = NULL
    WHERE task_id = p_task_id;

    RETURN ROW('REPOINTED', NULL)::horsies_phase2_quarantine_verdict;
END
$$;

CREATE FUNCTION horsies_task_history_leaf_lock_key(p_class_key text, p_anchor timestamp with time zone) RETURNS bigint
    LANGUAGE sql STABLE STRICT
    AS $$
    SELECT hashtextextended(
        p_class_key || chr(31) ||
        extract(epoch FROM date_trunc('day', p_anchor, 'UTC'))
            ::bigint::text,
        1601
    )
$$;

CREATE FUNCTION horsies_uuid7_birth(p_id uuid) RETURNS timestamp with time zone
    LANGUAGE sql IMMUTABLE STRICT
    AS $$
    SELECT CASE
        WHEN (get_byte(uuid_send(p_id), 6) >> 4) = 7
         AND (get_byte(uuid_send(p_id), 8) & 192) = 128
        THEN to_timestamp((
                (get_byte(uuid_send(p_id), 0)::bigint << 40)
              | (get_byte(uuid_send(p_id), 1)::bigint << 32)
              | (get_byte(uuid_send(p_id), 2)::bigint << 24)
              | (get_byte(uuid_send(p_id), 3)::bigint << 16)
              | (get_byte(uuid_send(p_id), 4)::bigint << 8)
              | get_byte(uuid_send(p_id), 5)::bigint
             )::double precision / 1000.0)
    END
$$;
