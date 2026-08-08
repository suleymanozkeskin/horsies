// Residual node states: rendered from data, never inferred as failure.
//
// horsies never kills a running task process. A node that was executing when
// its run was cancelled keeps going ("draining"); under a paused run it is
// simply allowed to finish ("finishing"). Both are normal, so neither is ever
// styled as an error.

export type ResidualState = 'draining' | 'finishing' | null;

const LIVE_LEAF_STATUSES: ReadonlySet<string> = new Set(['CLAIMED', 'RUNNING']);

/**
 * `leafStatus` is only available on the node-detail surface; the run payload
 * carries node status alone. Pass null there — a cancelled run's draining nodes
 * are still identified, only "finishing" needs the backing task.
 */
export function residualState(
  runStatus: string,
  nodeStatus: string,
  leafStatus: string | null
): ResidualState {
  if (nodeStatus !== 'RUNNING') {
    return null;
  }
  if (runStatus === 'CANCELLED' || runStatus === 'EXPIRED') {
    return 'draining';
  }
  if (
    runStatus === 'PAUSED' &&
    leafStatus !== null &&
    LIVE_LEAF_STATUSES.has(leafStatus)
  ) {
    return 'finishing';
  }
  return null;
}
