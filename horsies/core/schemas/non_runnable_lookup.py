"""The claimed-task lookup for workflows that cannot run.

After a claim, the worker asks which of the claimed workflow tasks belong to
a PAUSED or CANCELLED workflow. The function bounds that lookup by the
claimed ids:

- the task-to-node mapping for the supplied ids is materialized first;
- each workflow is then read by primary key before the status filter
  (``OFFSET 0`` keeps the planner from starting at the status side, which
  would walk every node of a wide paused workflow);
- an uncorrelated guard skips all of it when no workflow is PAUSED or
  CANCELLED, and stays effective under a generic plan.

The function binds the uuid identity, so it is installed only in the uuid
world: by the fresh-world install, by the cutover program, and by schema
activation on a uuid database. Results match the plain join for null,
duplicate, and absent ids.
"""

from __future__ import annotations

from typing import Final

NON_RUNNABLE_LOOKUP_FUNCTION: Final = 'horsies_find_non_runnable_workflow_tasks'

NON_RUNNABLE_LOOKUP_DDL: Final = f"""
CREATE OR REPLACE FUNCTION {NON_RUNNABLE_LOOKUP_FUNCTION}(p_task_ids uuid[])
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
        -- Keep each workflow lookup tied to its claimed node before the
        -- status filter.
        OFFSET 0
    ) w ON w.status IN ('PAUSED', 'CANCELLED')
    WHERE EXISTS (
        SELECT 1 FROM horsies_workflows guard_workflow
        WHERE guard_workflow.status IN ('PAUSED', 'CANCELLED')
    );
END
$function$
"""

DROP_NON_RUNNABLE_LOOKUP_DDL: Final = (
    f'DROP FUNCTION IF EXISTS {NON_RUNNABLE_LOOKUP_FUNCTION}(uuid[])'
)
