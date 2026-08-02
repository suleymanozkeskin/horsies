// Event -> query invalidation mapping.
//
// Kept as a pure lookup so the contract is asserted in tests rather than
// implied by whatever the SSE hook happens to call.

import { QUERY_ROOT, type QueryRoot } from '@/lib/query-keys';
import type { DataTopic, MonitoringEvent } from '@/events/types';

/**
 * Which query roots each topic invalidates.
 *
 * `tasks` reaches the workflow run/node details on purpose: node-level and
 * attempt changes have no trigger of their own, but every such change coincides
 * with a backing-task insert or status change, so the task channel is what
 * carries node progress.
 *
 * The task aggregates (stats, facets, breakdown) are deliberately absent:
 * they are whole-table aggregations whose value does not change meaningfully
 * per event, and the server debounce emits a `tasks` event up to 4x/s under
 * load. They refresh on their own timers (see use-tasks.ts), on the reconnect
 * sweep, and on explicit user action.
 */
export const TOPIC_INVALIDATIONS: Record<DataTopic, readonly QueryRoot[]> = {
  tasks: [
    QUERY_ROOT.taskList,
    QUERY_ROOT.taskDetail,
    QUERY_ROOT.workflowRun,
    QUERY_ROOT.workflowNode,
  ],
  workflows: [QUERY_ROOT.workflowRuns, QUERY_ROOT.workflowRun],
  workers: [QUERY_ROOT.workers, QUERY_ROOT.workerHistory],
};

/** Roots refreshed after a reconnect, when arbitrarily many events were missed.
 * Includes the task aggregates — they are event-decoupled, but a reconnect
 * means unbounded staleness, which is exactly when one refresh is due.
 * Deliberately excludes the manual liveness ping (an active round trip the
 * operator triggers) and the fetch-once workflow-name list. */
export const RECONNECT_SWEEP_ROOTS: readonly QueryRoot[] = [
  ...new Set([
    ...TOPIC_INVALIDATIONS.tasks,
    ...TOPIC_INVALIDATIONS.workflows,
    ...TOPIC_INVALIDATIONS.workers,
    QUERY_ROOT.taskStats,
    QUERY_ROOT.taskFacets,
    QUERY_ROOT.taskBreakdown,
  ]),
];

/** Query roots one event invalidates. `degraded` invalidates nothing — it is a
 * transport signal that switches the client to fallback polling. */
export function invalidationRootsFor(
  event: MonitoringEvent
): readonly QueryRoot[] {
  switch (event.topic) {
    case 'tasks':
    case 'workflows':
    case 'workers':
      return TOPIC_INVALIDATIONS[event.topic];
    case 'degraded':
      return [];
  }
}
