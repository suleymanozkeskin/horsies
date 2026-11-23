-- horsies schema migration 1002: the start command of each top-level workflow run.
--
-- The install applies this text after migration 1001 (core/migrations/1001_lifecycle_record.sql), in one READ
-- COMMITTED transaction with its bookkeeping row. A database that applied an older text of migration 1002 is refused
-- by the checksum.
--
-- Changes:
-- - horsies_workflows: command_fingerprint_version and command_fingerprint keep the fingerprint of the start command of
--   a top-level run. The core computes it (StartCommandV1, core/src/workflow_engine/start_fingerprint.rs); SQL only
--   stores and compares it.
-- - horsies_workflow_start takes the fingerprint as two more arguments (40 in all). A start whose top-level id exists
--   locks the existing row, reads it, and names what holds the id: the same start command (EXISTS), a run with no
--   stored fingerprint (FINGERPRINT_NOT_STORED), a run of another name (OTHER_NAME), a sub-workflow (SUB_WORKFLOW),
--   or another start command (OTHER_COMMAND). It writes nothing in each of these cases.
-- - horsies_workflow_start_top_row writes the fingerprint pair, and on a conflict locks the existing row.
--   horsies_workflow_start_child_row and horsies_workflow_child_start write no fingerprint: a child row has no start
--   command.
--
-- Rows written before this migration keep NULL in both new columns, and the migration writes no value into them. A
-- start with the id of such a top-level run answers FINGERPRINT_NOT_STORED: its command is not compared.
--
-- Rules of the file, as migration 1000: no database default (every insert names every value), objects in the current
-- schema, no transaction control. A function whose argument list changes is dropped and created again; a function
-- whose argument list stays is replaced in place. The comment above each function of this file is its contract from
-- this migration on; the comment in core/baseline/ or in an earlier migration is the contract before it.
--
-- Locks: the ALTER TABLE takes ACCESS EXCLUSIVE on horsies_workflows until the migration commits. The new columns are
-- nullable with no default, so the table is not rewritten; the new CHECK reads the table once.

------------------------------------------------------------------------------------------------------------------------
-- Start command of a top-level run (horsies_workflows)
------------------------------------------------------------------------------------------------------------------------

-- command_fingerprint_version: the version of the canonical form of the start command (1: StartCommandV1).
-- command_fingerprint: SHA-256 of the canonical form, 32 bytes.
-- Both NULL: a child row (a parent workflow started it, so it has no start command), or a top-level row that was
-- written before this migration (its command is not known). Both set: a top-level row that horsies_workflow_start
-- wrote.
-- The CHECK cannot require the pair on each top-level row: the rows from before this migration have none. The one
-- writer of a top-level row, horsies_workflow_start, refuses a missing or malformed pair before any write.
-- No index: the start reads the row by its primary key.
ALTER TABLE horsies_workflows
    ADD COLUMN command_fingerprint_version smallint,
    ADD COLUMN command_fingerprint bytea,
    -- Each arm names IS NOT NULL: without it a NULL makes the arm NULL and the CHECK passes.
    ADD CONSTRAINT horsies_workflows_command_fingerprint_by_origin CHECK (
        (command_fingerprint IS NULL AND command_fingerprint_version IS NULL)
        OR (
            parent_workflow_id IS NULL
            AND command_fingerprint IS NOT NULL
            AND command_fingerprint_version IS NOT NULL
            AND octet_length(command_fingerprint) = 32
            AND command_fingerprint_version > 0
        )
    );

------------------------------------------------------------------------------------------------------------------------
-- Workflow start
------------------------------------------------------------------------------------------------------------------------

DROP FUNCTION horsies_workflow_start(
    uuid[], text[], text[], text[], integer[], text[], uuid[], integer[], integer[], timestamp with time zone,
    uuid[], integer[], text[], text[], text[], text[], integer[], text[], text[], text[], boolean[], text[],
    integer[], text[], integer[], text[], timestamp with time zone[], text[], boolean[], text[], text[], text[],
    uuid[], integer[], text[], text, text, bytea
);
DROP FUNCTION horsies_workflow_start_top_row(
    uuid[], text[], text[], text[], integer[], text[], uuid[], integer[], integer[], timestamp with time zone, uuid,
    text, bytea
);

-- Internal: inserts the top row of a starting tree (the first of the workflow arrays), RUNNING, with
-- p_root_workflow_id (the top-level id for a top-level start, the root of the parent's tree for a child start), the
-- start call time, the trace pair (p_trace_carrier and p_trace_id, both NULL for none) and the start command pair
-- (p_command_fingerprint_version and p_command_fingerprint: both set for a top-level start, both NULL for a child
-- start; the table CHECK refuses another pairing).
-- An id that exists already is a conflict: ON CONFLICT (id) DO UPDATE ... WHERE false updates no row and fires no
-- trigger, but it locks the existing row until the transaction ends (PostgreSQL locks every conflicting row that the
-- DO UPDATE action meets). So the row cannot go away before the caller reads it. A conflict with a row that an open
-- transaction writes or deletes waits for that transaction; after a committed delete the id is free and the insert
-- goes in.
-- Returns FALSE on a conflict (nothing written), else TRUE. The other workflow rows go in one at a time, as their
-- parent's roots are taken (horsies_workflow_start_child_row).
-- Locks: the new row and the unique index entry of its id, or the existing row of the id.
-- Python: INSERT_WORKFLOW_SQL and INSERT_CHILD_WORKFLOW_SQL (core/workflows/sql.py:23-34, 260-270;
-- core/workflows/lifecycle.py:420-442).
CREATE FUNCTION horsies_workflow_start_top_row(
    p_workflow_ids uuid[], p_names text[], p_definition_keys text[], p_on_errors text[],
    p_output_task_indexes integer[], p_success_policies text[], p_parent_workflow_ids uuid[],
    p_parent_task_indexes integer[], p_depths integer[], p_sent_at timestamp with time zone,
    p_root_workflow_id uuid, p_trace_carrier text, p_trace_id bytea,
    p_command_fingerprint_version smallint, p_command_fingerprint bytea
) RETURNS boolean
    LANGUAGE plpgsql
    SET plan_cache_mode TO 'force_generic_plan'
AS $$
DECLARE
    v_inserted uuid;
BEGIN
    INSERT INTO horsies_workflows (
        id, name, definition_key, status, on_error, output_task_index, success_policy, result, error,
        parent_workflow_id, parent_task_index, root_workflow_id, depth, sent_at, created_at, started_at,
        completed_at, updated_at, trace_carrier, trace_id, command_fingerprint_version, command_fingerprint
    ) VALUES (
        p_workflow_ids[1], p_names[1], p_definition_keys[1], 'RUNNING', p_on_errors[1], p_output_task_indexes[1],
        p_success_policies[1]::jsonb, NULL, NULL, p_parent_workflow_ids[1], p_parent_task_indexes[1],
        p_root_workflow_id, p_depths[1], p_sent_at, now(), now(), NULL, now(), p_trace_carrier, p_trace_id,
        p_command_fingerprint_version, p_command_fingerprint
    )
    ON CONFLICT (id) DO UPDATE SET updated_at = horsies_workflows.updated_at WHERE false
    RETURNING id INTO v_inserted;
    RETURN v_inserted IS NOT NULL;
END;
$$;

-- Internal: the pre-built child at position p_position of the workflow arrays (a position after the first). The child
-- row goes in (RUNNING, p_root_workflow_id, the start call time, the trace pair of the start, no start command pair: a
-- child row has no start command), and its sub-workflow root node goes from READY to RUNNING, linked to the child,
-- with started_at.
-- Precondition: the parent workflow is RUNNING (horsies_workflow_start_tree reads it before the call).
-- horsies_workflow_tree_check refused a child row that names no sub-workflow root of its parent, or a node that
-- another child row names, so the link writes one row; another count raises internal_error (a defect).
-- Locks: the new child row and the linked node row (rows of this statement). Returns the child id.
-- Python: ENQUEUE_SUBWORKFLOW_TASK_SQL (READY to ENQUEUED, started_at), INSERT_CHILD_WORKFLOW_SQL and
-- LINK_SUB_WORKFLOW_SQL (core/workflows/sql.py:368-404).
CREATE OR REPLACE FUNCTION horsies_workflow_start_child_row(
    p_position integer, p_workflow_ids uuid[], p_names text[], p_definition_keys text[], p_on_errors text[],
    p_output_task_indexes integer[], p_success_policies text[], p_parent_workflow_ids uuid[],
    p_parent_task_indexes integer[], p_depths integer[], p_sent_at timestamp with time zone,
    p_root_workflow_id uuid, p_trace_carrier text, p_trace_id bytea
) RETURNS uuid
    LANGUAGE plpgsql
    SET plan_cache_mode TO 'force_generic_plan'
AS $$
DECLARE
    v_child uuid := p_workflow_ids[p_position];
    v_linked integer;
BEGIN
    INSERT INTO horsies_workflows (
        id, name, definition_key, status, on_error, output_task_index, success_policy, result, error,
        parent_workflow_id, parent_task_index, root_workflow_id, depth, sent_at, created_at, started_at,
        completed_at, updated_at, trace_carrier, trace_id, command_fingerprint_version, command_fingerprint
    ) VALUES (
        v_child, p_names[p_position], p_definition_keys[p_position], 'RUNNING', p_on_errors[p_position],
        p_output_task_indexes[p_position], p_success_policies[p_position]::jsonb, NULL, NULL,
        p_parent_workflow_ids[p_position], p_parent_task_indexes[p_position], p_root_workflow_id,
        p_depths[p_position], p_sent_at, now(), now(), NULL, now(), p_trace_carrier, p_trace_id, NULL, NULL
    );
    UPDATE horsies_workflow_tasks n
    SET status = 'RUNNING', sub_workflow_id = v_child, started_at = now()
    WHERE n.workflow_id = p_parent_workflow_ids[p_position] AND n.task_index = p_parent_task_indexes[p_position]
      AND n.status = 'READY' AND n.is_subworkflow AND n.sub_workflow_id IS NULL AND cardinality(n.dependencies) = 0;
    GET DIAGNOSTICS v_linked = ROW_COUNT;
    IF v_linked <> 1 THEN
        RAISE EXCEPTION 'the start linked % nodes to child workflow %', v_linked, v_child
            USING ERRCODE = 'internal_error';
    END IF;
    RETURN v_child;
END;
$$;

-- Starts a workflow tree in one statement. Arguments: the workflow rows of the tree (the top-level row first, then
-- each child that the core pre-built for a sub-workflow root, with its parent, parent index and depth), the start
-- call time p_sent_at, every node row of the tree (dependencies, args_from and workflow_ctx_from as one JSON text per
-- node; the typed facts max_retries, execution, good_until and retention_class_key set on task nodes; the params
-- choice sub_workflow_params set on sub-workflow nodes), the failed-node records (a sub-workflow root whose child
-- could not be built: its node, and its error envelope), the trace of the start: p_trace_kind NO_CARRIER
-- (p_trace_carrier and p_trace_id NULL) or CARRIER (the canonical JSON text of the core's carrier encoder and a
-- 16-byte trace id), and the start command of the top-level row: p_command_fingerprint_version (above 0) and
-- p_command_fingerprint (32 bytes), which the core computes. Every workflow row of the tree stores the trace pair, and
-- the task rows of their nodes copy it (horsies_workflow_node_tasks_insert). Only the top-level row stores the start
-- command pair. A start that writes nothing keeps both pairs of the stored row.
-- Refused (22023), before any write: a trace pair that does not fit its kind, a start command pair that is NULL, a
-- version that is not above 0, a fingerprint that is not 32 bytes, and the refusals of horsies_workflow_tree_check.
-- Steps:
-- 1. Insert the top-level row (horsies_workflow_start_top_row). The unique key makes a second start with the same id
--    wait for the first to commit or roll back. An id that exists is a conflict: the existing row stays locked until
--    the statement's transaction ends, so the read of step 1a finds it.
--    1a. Read the name, the parent and the start command pair of the existing row (INTO STRICT: no row is a defect,
--        P0002), and return one OUTCOME row with the stored name in detail. Nothing is written. The disposition is
--        the first that applies, in this order:
--        - EXISTS: the stored version and fingerprint are the ones of this start (the same start command; the name is
--          part of it);
--        - OTHER_NAME: the stored name is another name;
--        - SUB_WORKFLOW: the row has a parent: a sub-workflow, which has no start command;
--        - FINGERPRINT_NOT_STORED: a top-level row with no start command pair (written before migration 1002);
--        - OTHER_COMMAND: a top-level row of the same name with another start command.
-- 2. The rest of the tree, depth first from the top row, as Python's start (horsies_workflow_start_tree): the node
--    rows of a workflow (a node without dependencies READY, the rest PENDING; node row ids from horsies_uuidv7();
--    downstream_subworkflow from the static graph); then its sub-workflow roots one at a time in index order, each
--    only while the workflow is RUNNING: a failed-node record makes the node FAILED with its envelope, started_at and
--    completed_at, then progression (the failure policy, promotion of its dependents, completion); a pre-built child
--    goes in (RUNNING, root id of the tree), its node goes RUNNING, linked, with started_at, and the child is started
--    the same way before the next root. A later root of a workflow that a failure paused stays READY with no child:
--    the resume builds and starts it.
-- 3. After the roots of a workflow: its parent advance when it is COMPLETED or FAILED now
--    (horsies_workflow_child_finish; a parent that ends is advanced in turn, as Python's in-session drain does); then,
--    when it is still RUNNING, its READY nodes are enqueued, with their task rows minted (horsies_workflow_promote).
--    A workflow that a failed record or an ended child paused keeps its READY nodes with no task (the pause gate).
-- 4. Return STARTED.
-- Owed work: CHILD_START rows come only from the progression of a failed record or from a sub-workflow root with
-- neither a child nor a failed record; a caller that passes failed records runs this call inside BEGIN, then
-- horsies_workflow_child_start per row, then COMMIT. Any other start is one statement in autocommit.
-- Locks: the new rows and the unique index entry of the top-level id; on a conflict, the existing top-level row only
-- (the first lock of the workflow lock order). A step that holds that row makes the conflict wait for its end.
-- Python: start_workflow_async (core/workflows/lifecycle.py:250-803) and, per sub-workflow root,
-- enqueue_subworkflow_task (core/workflows/engine.py:989-1627).
CREATE FUNCTION horsies_workflow_start(
    p_workflow_ids uuid[],
    p_names text[],
    p_definition_keys text[],
    p_on_errors text[],
    p_output_task_indexes integer[],
    p_success_policies text[],
    p_parent_workflow_ids uuid[],
    p_parent_task_indexes integer[],
    p_depths integer[],
    p_sent_at timestamp with time zone,
    p_node_workflow_ids uuid[],
    p_node_task_indexes integer[],
    p_node_ids text[],
    p_node_task_names text[],
    p_node_inputs text[],
    p_node_queue_names text[],
    p_node_priorities integer[],
    p_node_dependencies text[],
    p_node_args_from text[],
    p_node_workflow_ctx_from text[],
    p_node_allow_failed_deps boolean[],
    p_node_join_types text[],
    p_node_min_successes integer[],
    p_node_task_options text[],
    p_node_max_retries integer[],
    p_node_executions text[],
    p_node_good_untils timestamp with time zone[],
    p_node_retention_class_keys text[],
    p_node_is_subworkflow boolean[],
    p_node_sub_workflow_names text[],
    p_node_sub_definition_keys text[],
    p_node_sub_workflow_params text[],
    p_failed_workflow_ids uuid[],
    p_failed_task_indexes integer[],
    p_failed_results text[],
    p_trace_kind text,
    p_trace_carrier text,
    p_trace_id bytea,
    p_command_fingerprint_version smallint,
    p_command_fingerprint bytea
) RETURNS SETOF horsies_workflow_step_row
    LANGUAGE plpgsql
    SET plan_cache_mode TO 'force_generic_plan'
AS $$
DECLARE
    v_top uuid := p_workflow_ids[1];
    v_stored record;
BEGIN
    PERFORM horsies_workflow_require(p_sent_at IS NOT NULL, 'sent_at must not be NULL');
    PERFORM horsies_workflow_require(
        p_trace_kind IN ('NO_CARRIER', 'CARRIER'), 'the trace kind must be NO_CARRIER or CARRIER'
    );
    CASE p_trace_kind
        WHEN 'NO_CARRIER' THEN
            PERFORM horsies_workflow_require(
                num_nulls(p_trace_carrier, p_trace_id) = 2, 'a start with no carrier has no trace values'
            );
        WHEN 'CARRIER' THEN
            PERFORM horsies_workflow_require(
                num_nulls(p_trace_carrier, p_trace_id) = 0 AND octet_length(p_trace_id) = 16,
                'a start with a carrier names its text and its 16-byte trace id'
            );
    END CASE;
    PERFORM horsies_workflow_require(
        p_command_fingerprint_version IS NOT NULL AND p_command_fingerprint_version > 0
        AND p_command_fingerprint IS NOT NULL AND octet_length(p_command_fingerprint) = 32,
        'a top-level start names its start command: a version above 0 and a 32-byte fingerprint'
    );
    PERFORM horsies_workflow_tree_check(
        NULL, NULL, 0, p_workflow_ids, p_names, p_definition_keys, p_on_errors, p_output_task_indexes,
        p_success_policies, p_parent_workflow_ids, p_parent_task_indexes, p_depths, p_node_workflow_ids,
        p_node_task_indexes, p_node_ids, p_node_task_names, p_node_inputs, p_node_queue_names, p_node_priorities,
        p_node_dependencies, p_node_args_from, p_node_workflow_ctx_from, p_node_allow_failed_deps, p_node_join_types,
        p_node_min_successes, p_node_task_options, p_node_max_retries, p_node_executions, p_node_good_untils,
        p_node_retention_class_keys, p_node_is_subworkflow, p_node_sub_workflow_names, p_node_sub_definition_keys,
        p_node_sub_workflow_params, p_failed_workflow_ids, p_failed_task_indexes, p_failed_results
    );

    IF NOT horsies_workflow_start_top_row(
        p_workflow_ids, p_names, p_definition_keys, p_on_errors, p_output_task_indexes, p_success_policies,
        p_parent_workflow_ids, p_parent_task_indexes, p_depths, p_sent_at, v_top, p_trace_carrier, p_trace_id,
        p_command_fingerprint_version, p_command_fingerprint
    ) THEN
        -- The conflict locked the row, so it is still there.
        SELECT w.name, w.parent_workflow_id, w.command_fingerprint_version, w.command_fingerprint
        INTO STRICT v_stored
        FROM horsies_workflows w
        WHERE w.id = v_top;
        RETURN NEXT horsies_workflow_outcome_row(
            CASE
                WHEN v_stored.command_fingerprint_version = p_command_fingerprint_version
                     AND v_stored.command_fingerprint = p_command_fingerprint THEN 'EXISTS'
                WHEN v_stored.name <> p_names[1] THEN 'OTHER_NAME'
                WHEN v_stored.parent_workflow_id IS NOT NULL THEN 'SUB_WORKFLOW'
                WHEN v_stored.command_fingerprint IS NULL THEN 'FINGERPRINT_NOT_STORED'
                ELSE 'OTHER_COMMAND'
            END,
            v_top, FALSE, v_stored.name
        );
        RETURN;
    END IF;
    RETURN QUERY SELECT * FROM horsies_workflow_start_tree(
        v_top, 1, v_top,
        p_workflow_ids, p_names, p_definition_keys, p_on_errors, p_output_task_indexes, p_success_policies,
        p_parent_workflow_ids, p_parent_task_indexes, p_depths, p_sent_at, v_top, p_trace_carrier, p_trace_id,
        p_node_workflow_ids, p_node_task_indexes, p_node_ids, p_node_task_names, p_node_inputs, p_node_queue_names,
        p_node_priorities, p_node_dependencies, p_node_args_from, p_node_workflow_ctx_from,
        p_node_allow_failed_deps, p_node_join_types, p_node_min_successes, p_node_task_options, p_node_max_retries,
        p_node_executions, p_node_good_untils, p_node_retention_class_keys, p_node_is_subworkflow,
        p_node_sub_workflow_names, p_node_sub_definition_keys, p_node_sub_workflow_params,
        p_failed_workflow_ids, p_failed_task_indexes, p_failed_results
    );
    RETURN NEXT horsies_workflow_outcome_row('STARTED', v_top, FALSE, NULL);
END;
$$;

------------------------------------------------------------------------------------------------------------------------
-- Sub-workflow start
------------------------------------------------------------------------------------------------------------------------

-- Step function: the owed child start of one sub-workflow node, in the transaction of the step that owes it (BEGIN;
-- the step function; the host build callback and this call for each CHILD_START row; COMMIT). The parent node, the
-- child rows and the link commit together.
-- p_outcome_kind (a kind text plus the arguments of each kind; each kind refuses the arguments of the other, 22023):
-- - TREE: the child tree that the core built, in the arrays of horsies_workflow_start: the child row first, with the
--   parent pair (p_parent_workflow_id, p_parent_task_index) and the depth of the parent workflow plus one, then the
--   pre-built children of its sub-workflow roots; every node of the tree; the failed-node records (a sub-workflow
--   root of the tree whose own child could not be built). p_sent_at is the call time of the child start. Every
--   workflow row of the child tree copies the trace pair (trace_carrier, trace_id) of the parent workflow row, so
--   the tasks of a sub-workflow at any depth carry the trace of the top-level start.
-- - FAILURE: p_failure_result, the error envelope of a child that could not be built (a definition that does not
--   load, a failing build callback, an args_from key or value that the callback refuses, the depth limit). It must
--   be an error envelope (22023 otherwise).
-- Steps:
-- 1. Lock the parent workflow row FOR UPDATE (the step function of this transaction holds it already), then the
--    node row.
-- 2. The node is not an ENQUEUED sub-workflow node with no child: STALE, nothing written.
-- 3. The parent workflow is not RUNNING (a failure earlier in the same step paused it): the node goes back to READY
--    with started_at NULL and one NODE_READY event (horsies_workflow_events, at now()), so the resume or recovery
--    case 1.5 starts it later. WORKFLOW_NOT_RUNNING (detail: the status). Without this the node would stay
--    ENQUEUED with no child, which no recovery case selects.
-- 4. TREE: insert the child row (RUNNING, the root id of the parent's tree, no start command pair;
--    horsies_workflow_start_top_row), link the node (RUNNING, sub_workflow_id), then write the rest of the child
--    tree as a start does
--    (horsies_workflow_start_tree: depth first, the roots of each workflow in index order while it is RUNNING, the
--    parent advance of a workflow that ends, then the task rows of the READY nodes of a workflow that is still
--    RUNNING). A child that is COMPLETED or FAILED now advances the node in this statement
--    (horsies_workflow_child_finish), as Python's in-session drain does. STARTED.
-- 5. FAILURE: the node goes FAILED with the envelope and completed_at, then horsies_workflow_progress for it
--    (failure policy, promotion of its dependents, completion). NODE_FAILED.
-- A refused argument raises 22023; a child id that exists already raises unique_violation (23505): the core mints
-- it, so a conflict is a defect. Nothing is written in either case.
-- Returns the OUTCOME row (workflow_id: the parent workflow; workflow_status: its status after the call;
-- advance_child_workflow_id: the parent workflow when this call ended it and it has a parent, for the advance after
-- the commit) and the CHILD_START and SOURCE rows that this call owes in turn.
-- Lock order: parent workflow row, node row, the new child rows, then the progression locks (node rows; under
-- on_error pause the RUNNING descendant rows in (depth, id) order).
-- Python: enqueue_subworkflow_task after its READY to ENQUEUED write (core/workflows/engine.py:1068-1627, with
-- INSERT_CHILD_WORKFLOW_SQL and LINK_SUB_WORKFLOW_SQL, core/workflows/sql.py:260-276), and _fail_enqueued_task
-- (core/workflows/engine.py:472-555).
CREATE OR REPLACE FUNCTION horsies_workflow_child_start(
    p_parent_workflow_id uuid,
    p_parent_task_index integer,
    p_outcome_kind text,
    p_failure_result text,
    p_sent_at timestamp with time zone,
    p_workflow_ids uuid[],
    p_names text[],
    p_definition_keys text[],
    p_on_errors text[],
    p_output_task_indexes integer[],
    p_success_policies text[],
    p_parent_workflow_ids uuid[],
    p_parent_task_indexes integer[],
    p_depths integer[],
    p_node_workflow_ids uuid[],
    p_node_task_indexes integer[],
    p_node_ids text[],
    p_node_task_names text[],
    p_node_inputs text[],
    p_node_queue_names text[],
    p_node_priorities integer[],
    p_node_dependencies text[],
    p_node_args_from text[],
    p_node_workflow_ctx_from text[],
    p_node_allow_failed_deps boolean[],
    p_node_join_types text[],
    p_node_min_successes integer[],
    p_node_task_options text[],
    p_node_max_retries integer[],
    p_node_executions text[],
    p_node_good_untils timestamp with time zone[],
    p_node_retention_class_keys text[],
    p_node_is_subworkflow boolean[],
    p_node_sub_workflow_names text[],
    p_node_sub_definition_keys text[],
    p_node_sub_workflow_params text[],
    p_failed_workflow_ids uuid[],
    p_failed_task_indexes integer[],
    p_failed_results text[]
) RETURNS SETOF horsies_workflow_step_row
    LANGUAGE plpgsql
    SET plan_cache_mode TO 'force_generic_plan'
AS $$
DECLARE
    v_parent record;
    v_node record;
    v_child uuid := p_workflow_ids[1];
BEGIN
    PERFORM horsies_workflow_require(
        p_parent_workflow_id IS NOT NULL AND p_parent_task_index IS NOT NULL,
        'the parent workflow id and task index must not be NULL'
    );
    PERFORM horsies_workflow_require(
        p_outcome_kind IN ('TREE', 'FAILURE'), 'the outcome kind must be TREE or FAILURE'
    );
    CASE p_outcome_kind
        WHEN 'TREE' THEN
            PERFORM horsies_workflow_require(
                p_failure_result IS NULL AND p_sent_at IS NOT NULL,
                'a TREE child start takes sent_at and no failure result'
            );
            PERFORM horsies_workflow_tree_check(
                p_parent_workflow_id, p_parent_task_index, p_depths[1],
                p_workflow_ids, p_names, p_definition_keys, p_on_errors, p_output_task_indexes, p_success_policies,
                p_parent_workflow_ids, p_parent_task_indexes, p_depths, p_node_workflow_ids, p_node_task_indexes,
                p_node_ids, p_node_task_names, p_node_inputs, p_node_queue_names, p_node_priorities,
                p_node_dependencies, p_node_args_from, p_node_workflow_ctx_from, p_node_allow_failed_deps,
                p_node_join_types, p_node_min_successes, p_node_task_options, p_node_max_retries, p_node_executions,
                p_node_good_untils, p_node_retention_class_keys, p_node_is_subworkflow, p_node_sub_workflow_names,
                p_node_sub_definition_keys, p_node_sub_workflow_params, p_failed_workflow_ids, p_failed_task_indexes,
                p_failed_results
            );
        WHEN 'FAILURE' THEN
            PERFORM horsies_workflow_require(
                p_sent_at IS NULL AND num_nonnulls(
                    p_workflow_ids, p_names, p_definition_keys, p_on_errors, p_output_task_indexes, p_success_policies,
                    p_parent_workflow_ids, p_parent_task_indexes, p_depths, p_node_workflow_ids, p_node_task_indexes,
                    p_node_ids, p_node_task_names, p_node_inputs, p_node_queue_names, p_node_priorities,
                    p_node_dependencies, p_node_args_from, p_node_workflow_ctx_from, p_node_allow_failed_deps,
                    p_node_join_types, p_node_min_successes, p_node_task_options, p_node_max_retries, p_node_executions,
                    p_node_good_untils, p_node_retention_class_keys, p_node_is_subworkflow, p_node_sub_workflow_names,
                    p_node_sub_definition_keys, p_node_sub_workflow_params, p_failed_workflow_ids,
                    p_failed_task_indexes, p_failed_results
                ) = 0,
                'a FAILURE child start takes the failure result alone'
            );
            PERFORM horsies_workflow_require(
                horsies_workflow_result_kind(p_failure_result) = 'ERROR',
                'the failure result of a child start must be an error envelope'
            );
    END CASE;

    SELECT w.status, w.depth, coalesce(w.root_workflow_id, w.id) AS root_workflow_id, w.trace_carrier, w.trace_id
    INTO v_parent
    FROM horsies_workflows w
    WHERE w.id = p_parent_workflow_id
    FOR UPDATE;
    SELECT n.id, n.status, n.is_subworkflow, n.sub_workflow_id INTO v_node
    FROM horsies_workflow_tasks n
    WHERE n.workflow_id = p_parent_workflow_id AND n.task_index = p_parent_task_index
    FOR UPDATE;
    IF v_node.id IS NULL OR NOT v_node.is_subworkflow OR v_node.status <> 'ENQUEUED'
       OR v_node.sub_workflow_id IS NOT NULL THEN
        RETURN NEXT horsies_workflow_outcome_row('STALE', p_parent_workflow_id, FALSE, v_node.status::text);
        RETURN;
    END IF;
    IF v_parent.status <> 'RUNNING' THEN
        UPDATE horsies_workflow_tasks n SET status = 'READY', started_at = NULL WHERE n.id = v_node.id;
        INSERT INTO horsies_workflow_events (
            workflow_id, event_id, event_kind, occurred_at, workflow_node_row_id, task_id, history_class,
            terminalization_kind
        )
        VALUES (
            p_parent_workflow_id, nextval('horsies_workflow_event_id'), 'NODE_READY', now(), v_node.id, NULL, NULL,
            NULL
        );
        RETURN NEXT horsies_workflow_outcome_row(
            'WORKFLOW_NOT_RUNNING', p_parent_workflow_id, FALSE, v_parent.status::text
        );
        RETURN;
    END IF;

    CASE p_outcome_kind
        WHEN 'TREE' THEN
            PERFORM horsies_workflow_require(
                p_depths[1] = v_parent.depth + 1,
                'the child row must have the depth of its parent workflow plus one'
            );
            IF NOT horsies_workflow_start_top_row(
                p_workflow_ids, p_names, p_definition_keys, p_on_errors, p_output_task_indexes, p_success_policies,
                p_parent_workflow_ids, p_parent_task_indexes, p_depths, p_sent_at, v_parent.root_workflow_id,
                v_parent.trace_carrier, v_parent.trace_id, NULL, NULL
            ) THEN
                RAISE EXCEPTION 'child workflow % of workflow % node % exists already', v_child, p_parent_workflow_id,
                    p_parent_task_index USING ERRCODE = 'unique_violation';
            END IF;
            UPDATE horsies_workflow_tasks n SET status = 'RUNNING', sub_workflow_id = v_child WHERE n.id = v_node.id;
            RETURN QUERY SELECT * FROM horsies_workflow_start_tree(
                v_child, 1, v_child,
                p_workflow_ids, p_names, p_definition_keys, p_on_errors, p_output_task_indexes, p_success_policies,
                p_parent_workflow_ids, p_parent_task_indexes, p_depths, p_sent_at, v_parent.root_workflow_id,
                v_parent.trace_carrier, v_parent.trace_id,
                p_node_workflow_ids, p_node_task_indexes, p_node_ids, p_node_task_names, p_node_inputs,
                p_node_queue_names, p_node_priorities, p_node_dependencies, p_node_args_from,
                p_node_workflow_ctx_from, p_node_allow_failed_deps, p_node_join_types, p_node_min_successes,
                p_node_task_options, p_node_max_retries, p_node_executions, p_node_good_untils,
                p_node_retention_class_keys, p_node_is_subworkflow, p_node_sub_workflow_names,
                p_node_sub_definition_keys, p_node_sub_workflow_params, p_failed_workflow_ids, p_failed_task_indexes,
                p_failed_results
            );
            IF EXISTS (SELECT 1 FROM horsies_workflows w WHERE w.id = v_child AND w.status IN ('COMPLETED', 'FAILED'))
            THEN
                RETURN QUERY SELECT * FROM horsies_workflow_child_finish(v_child) AS r WHERE r.row_kind <> 'OUTCOME';
            END IF;
            RETURN NEXT horsies_workflow_outcome_row('STARTED', p_parent_workflow_id, TRUE, NULL);
        WHEN 'FAILURE' THEN
            UPDATE horsies_workflow_tasks n
            SET status = 'FAILED', result = p_failure_result, completed_at = now()
            WHERE n.id = v_node.id;
            RETURN QUERY SELECT * FROM horsies_workflow_progress(p_parent_workflow_id, p_parent_task_index, 'READ');
            RETURN NEXT horsies_workflow_outcome_row('NODE_FAILED', p_parent_workflow_id, TRUE, NULL);
    END CASE;
END;
$$;
