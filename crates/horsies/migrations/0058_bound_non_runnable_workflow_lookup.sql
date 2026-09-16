DO $migration$
BEGIN
    IF to_regprocedure('horsies_phase2_consume(uuid,text)') IS NULL THEN
        RETURN;
    END IF;

    EXECUTE $definition$
CREATE OR REPLACE FUNCTION horsies_find_non_runnable_workflow_tasks(p_task_ids uuid[])
RETURNS TABLE(id uuid, status character varying)
LANGUAGE plpgsql
STABLE
SET plan_cache_mode = force_generic_plan
AS $function$
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
    -- Keep each workflow lookup tied to its claimed node before filtering status.
    OFFSET 0
) w ON w.status IN ('PAUSED', 'CANCELLED');
END
$function$;
$definition$;
END
$migration$;
