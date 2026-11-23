-- Cases for the timing rows of horsies_monitoring_workflow_run (core/migrations/1001_lifecycle_record.sql), one
-- session.
--
-- Run on a database that holds the migration chain and the workflow fixtures (baseline_workflow_fixtures.sql), in one
-- transaction that the caller rolls back (core/src/sql_tests/monitoring_timing_sql.rs). Each case prints
-- "ok: <case>"; a failed case raises and stops the script. A statement does not see the writes of a function that it
-- calls, so each check reads the run in a statement after the call. Every time of one transaction is one now(), so
-- these cases check the rows and their columns; the composition of the times is tested in
-- core/src/monitoring/timing_tests.rs.

SET client_min_messages = notice;

CREATE FUNCTION mt_check(p_case text, p_ok boolean) RETURNS void LANGUAGE plpgsql AS $$
BEGIN
    IF p_ok IS NOT TRUE THEN
        RAISE EXCEPTION 'case failed: %', p_case USING ERRCODE = 'assert_failure';
    END IF;
    RAISE NOTICE 'ok: %', p_case;
END
$$;

-- The rows of the run read of p_workflow in row order: 'KIND' for RUN and NODE, 'EVENT:<kind>@<index>' for an event
-- ('-' for a run event), 'TASK:<location>' for a task.
CREATE FUNCTION mt_rows(p_workflow uuid) RETURNS text LANGUAGE sql AS $$
    SELECT string_agg(
        CASE r.row_kind
            WHEN 'EVENT' THEN 'EVENT:' || r.event_kind || '@' || COALESCE(r.task_index::text, '-')
            WHEN 'TASK' THEN 'TASK:' || r.location
            ELSE r.row_kind
        END,
        ',' ORDER BY r.row_position
    )
    FROM horsies_monitoring_workflow_run(p_workflow) AS r
$$;

-- The runner applies the migration texts with no bookkeeping row; the record starts one day before these cases.
INSERT INTO horsies_migrations (version, description, installed_on, success, checksum, execution_time)
VALUES (1001, 'lifecycle record', now() - interval '1 day', TRUE, '\x00'::bytea, 0);

------------------------------------------------------------------------------------------------------------------------
-- One statement, no statement per node or task
------------------------------------------------------------------------------------------------------------------------

SELECT mt_check('bound: the run read has five RETURN QUERY statements and no loop, so its statements do not grow with '
                'its nodes, events or tasks',
    (SELECT (length(p.prosrc) - length(replace(p.prosrc, 'RETURN QUERY', ''))) / length('RETURN QUERY') = 5
            AND position('LOOP' IN p.prosrc) = 0 AND position('FOREACH' IN p.prosrc) = 0
     FROM pg_proc AS p
     WHERE p.proname = 'horsies_monitoring_workflow_run'));

------------------------------------------------------------------------------------------------------------------------
-- The rows of a run
------------------------------------------------------------------------------------------------------------------------

-- Node 0 completes (phase 1 and phase 2); node 1 is promoted with a live task.
DO $$
DECLARE
    v_wf uuid := '00000000-0000-7000-8000-00000000e010';
BEGIN
    PERFORM * FROM wft_start1(v_wf, 'fail', '[{"idx":0},{"idx":1,"deps":[0]}]');
    PERFORM mt_check('rows: node 0 completes', wft_finish(v_wf, 0, wft_ok('1')) IS NOT NULL);
END $$;

SELECT mt_check('rows: RUN, the nodes in index order, the events in event order, the live task, then the moved task',
    mt_rows('00000000-0000-7000-8000-00000000e010')
        = 'RUN,NODE,NODE,EVENT:TASK_MOVED@0,TASK:LIVE,TASK:HISTORY');
SELECT mt_check('rows: the positions run from 0 with no gap',
    (SELECT array_agg(r.row_position ORDER BY r.row_position) = ARRAY[0, 1, 2, 3, 4, 5]
     FROM horsies_monitoring_workflow_run('00000000-0000-7000-8000-00000000e010') AS r));
SELECT mt_check('rows: the RUN row holds the read time, the capture start and INCLUDED',
    (SELECT r.as_of >= now() AND r.captured_since = now() - interval '1 day' AND r.timing_rows = 'INCLUDED'
            AND r.created_at = (wft_workflow('00000000-0000-7000-8000-00000000e010')).created_at
     FROM horsies_monitoring_workflow_run('00000000-0000-7000-8000-00000000e010') AS r
     WHERE r.row_kind = 'RUN'));
SELECT mt_check('rows: each NODE row holds the node creation and the task it links',
    (SELECT bool_and(r.node_created_at = n.created_at AND r.node_task_id IS NOT DISTINCT FROM n.task_id
                     AND r.child_created_at IS NULL AND r.child_completed_at IS NULL)
     FROM horsies_monitoring_workflow_run('00000000-0000-7000-8000-00000000e010') AS r
     JOIN horsies_workflow_tasks AS n
       ON n.workflow_id = '00000000-0000-7000-8000-00000000e010' AND n.task_index = r.task_index
     WHERE r.row_kind = 'NODE'));
SELECT mt_check('rows: the TASK_MOVED row holds its event, the node index, the task and the move kind',
    (SELECT r.event_id = e.event_id AND r.occurred_at = e.occurred_at AND r.task_index = 0
            AND r.task_id = wft_task_of('00000000-0000-7000-8000-00000000e010', 0)
            AND r.terminalization_kind = 'COMPLETE_LOCKED'
     FROM horsies_monitoring_workflow_run('00000000-0000-7000-8000-00000000e010') AS r
     JOIN horsies_workflow_events AS e ON e.workflow_id = '00000000-0000-7000-8000-00000000e010'
     WHERE r.row_kind = 'EVENT'));
SELECT mt_check('rows: the LIVE row holds the live task and its version-2 archive with the open attempt',
    (SELECT r.task_id = t.id AND r.status = 'PENDING' AND r.created_at = t.created_at
            AND r.enqueued_at = t.enqueued_at AND r.claimed_at IS NULL AND r.started_at IS NULL
            AND r.attempt_archive_version = 2 AND r.attempt_snapshot = horsies_encode_attempt_archive(t)
            AND r.attempt_snapshot_digest IS NULL
            AND jsonb_array_length(convert_from(r.attempt_snapshot, 'UTF8')::jsonb) = 2
     FROM horsies_monitoring_workflow_run('00000000-0000-7000-8000-00000000e010') AS r
     JOIN horsies_tasks AS t ON t.id = wft_task_of('00000000-0000-7000-8000-00000000e010', 1)
     WHERE r.row_kind = 'TASK' AND r.location = 'LIVE'));
SELECT mt_check('rows: the HISTORY row is the history row at the locator of the event, with its archive and digest',
    (SELECT r.task_id = h.task_id AND r.status IS NULL AND r.created_at = h.created_at
            AND r.enqueued_at = h.enqueued_at AND r.claimed_at = h.claimed_at AND r.started_at = h.started_at
            AND r.attempt_archive_version = h.attempt_archive_version AND r.attempt_archive_version = 2
            AND r.attempt_snapshot = h.attempt_snapshot AND r.attempt_snapshot_digest = h.attempt_snapshot_digest
     FROM horsies_monitoring_workflow_run('00000000-0000-7000-8000-00000000e010') AS r
     JOIN horsies_task_history AS h ON h.task_id = wft_task_of('00000000-0000-7000-8000-00000000e010', 0)
     WHERE r.row_kind = 'TASK' AND r.location = 'HISTORY'));

-- Retention removed the history row of the moved task.
DELETE FROM horsies_task_history WHERE task_id = wft_task_of('00000000-0000-7000-8000-00000000e010', 0);
SELECT mt_check('rows: a TASK_MOVED event with no history row gives a PRUNED row with the task id only',
    (SELECT r.task_id = wft_task_of('00000000-0000-7000-8000-00000000e010', 0) AND r.status IS NULL
            AND r.created_at IS NULL AND r.enqueued_at IS NULL AND r.attempt_archive_version IS NULL
            AND r.attempt_snapshot IS NULL AND r.attempt_snapshot_digest IS NULL
     FROM horsies_monitoring_workflow_run('00000000-0000-7000-8000-00000000e010') AS r
     WHERE r.row_kind = 'TASK' AND r.location = 'PRUNED'));

------------------------------------------------------------------------------------------------------------------------
-- Run events and child runs
------------------------------------------------------------------------------------------------------------------------

-- A pause abandons the claim of node 1; the resume promotes the node again.
DO $$
DECLARE
    v_wf uuid := '00000000-0000-7000-8000-00000000e020';
BEGIN
    PERFORM * FROM wft_start1(v_wf, 'fail', '[{"idx":0},{"idx":1}]');
    PERFORM wft_claim(wft_task_of(v_wf, 1), now());
    PERFORM * FROM horsies_workflow_pause(v_wf);
    PERFORM * FROM horsies_workflow_resume(v_wf);
END $$;

SELECT mt_check('events: the run events have no node; the abandoned task is HISTORY and the new task is LIVE',
    mt_rows('00000000-0000-7000-8000-00000000e020')
        = 'RUN,NODE,NODE,EVENT:WORKFLOW_PAUSED@-,EVENT:TASK_MOVED@1,EVENT:NODE_READY@1,EVENT:WORKFLOW_RESUMED@-,'
          'TASK:LIVE,TASK:LIVE,TASK:HISTORY');

-- A pre-built child run: the sub-workflow node is RUNNING with its child.
DO $$
DECLARE
    v_wf uuid := '00000000-0000-7000-8000-00000000e030';
    v_child uuid := '00000000-0000-7000-8000-00000000e031';
BEGIN
    PERFORM * FROM wft_start(
        jsonb_build_array(
            jsonb_build_object('id', v_wf, 'on_error', 'fail'),
            jsonb_build_object('id', v_child, 'on_error', 'fail', 'parent', v_wf, 'parent_index', 0, 'depth', 1)
        ),
        jsonb_build_array(jsonb_build_object('idx', 0, 'sub', true), jsonb_build_object('wf', v_child, 'idx', 0))
    );
END $$;

SELECT mt_check('child: the NODE row of a sub-workflow node holds the times of its child run and no task',
    (SELECT r.node_task_id IS NULL AND r.child_created_at = c.created_at
            AND r.child_completed_at IS NOT DISTINCT FROM c.completed_at
     FROM horsies_monitoring_workflow_run('00000000-0000-7000-8000-00000000e030') AS r
     JOIN horsies_workflows AS c ON c.id = '00000000-0000-7000-8000-00000000e031'
     WHERE r.row_kind = 'NODE' AND r.task_index = 0));
SELECT mt_check('child: the child run is read with its own task',
    mt_rows('00000000-0000-7000-8000-00000000e031') = 'RUN,NODE,TASK:LIVE');

------------------------------------------------------------------------------------------------------------------------
-- Runs whose timing rows are not read
------------------------------------------------------------------------------------------------------------------------

UPDATE horsies_workflows SET created_at = now() - interval '2 days' WHERE id = '00000000-0000-7000-8000-00000000e020';
SELECT mt_check('before capture: a run created before the capture start has its RUN and NODE rows only',
    mt_rows('00000000-0000-7000-8000-00000000e020') = 'RUN,NODE,NODE'
    AND (SELECT r.timing_rows = 'BEFORE_CAPTURE'
         FROM horsies_monitoring_workflow_run('00000000-0000-7000-8000-00000000e020') AS r
         WHERE r.row_kind = 'RUN'));

-- 100,001 events: one more than the bound of one read.
INSERT INTO horsies_workflow_events (
    workflow_id, event_id, event_kind, occurred_at, workflow_node_row_id, task_id, history_class,
    terminalization_kind
)
SELECT '00000000-0000-7000-8000-00000000e010', nextval('horsies_workflow_event_id'), 'NODE_READY', now(),
       (wft_node('00000000-0000-7000-8000-00000000e010', 1)).id, NULL, NULL, NULL
FROM generate_series(1, 100000);
SELECT mt_check('bound: a run with more than 100,000 events has its RUN and NODE rows only, BOUND_EXCEEDED',
    mt_rows('00000000-0000-7000-8000-00000000e010') = 'RUN,NODE,NODE'
    AND (SELECT r.timing_rows = 'BOUND_EXCEEDED'
         FROM horsies_monitoring_workflow_run('00000000-0000-7000-8000-00000000e010') AS r
         WHERE r.row_kind = 'RUN'));

------------------------------------------------------------------------------------------------------------------------
-- The capture start
------------------------------------------------------------------------------------------------------------------------

DELETE FROM horsies_migrations WHERE version = 1001;
DO $$
BEGIN
    BEGIN
        PERFORM * FROM horsies_monitoring_workflow_run('00000000-0000-7000-8000-00000000e030');
    EXCEPTION WHEN data_corrupted THEN
        PERFORM mt_check('capture start: the read of an existing run with no bookkeeping row raises XX001', TRUE);
        RETURN;
    END;
    PERFORM mt_check('capture start: the read of an existing run with no bookkeeping row raises XX001', FALSE);
END $$;
SELECT mt_check('capture start: an unknown run id is ABSENT with no bookkeeping row',
    mt_rows('00000000-0000-7000-8000-00000000ffff') = 'ABSENT');
