// The options of the nodes of the end-to-end workflows. The builds name what a node does in the words of `NodePlan`;
// only this module names the option fields of the workflow API.

import type { AnyNode, DefaultJoin, Join } from "../../../../horsies/src/index.ts";

/** What a node does besides its input. Each field left out takes the API's default. */
export type NodePlan<J extends Join> = {
  /** The nodes that the node waits for, besides the sources of its parameters. */
  readonly after?: readonly AnyNode[];
  /** When the node runs. Left out: when all its dependencies ended. */
  readonly join?: J;
  /** The node runs when a dependency failed; left out, it is SKIPPED. */
  readonly runsAfterFailure?: true;
  /** The node's task expires if no worker starts it before this instant. */
  readonly deadline?: Temporal.Instant;
  /** The queue of the node's task. Left out: the task's queue. */
  readonly queue?: string;
};

/** The options of a node of `NodePlan`. */
export type PlannedOptions<Id extends string, J extends Join> = {
  readonly id: Id;
  readonly waitsFor?: readonly AnyNode[];
  readonly join?: J;
  readonly allowFailedDeps?: boolean;
  readonly goodUntil?: Temporal.Instant;
  readonly queue?: string;
};

/** The options of a node with context sources: a task with `workflowContext: true`. */
export type ContextOptions<Id extends string, J extends Join> = PlannedOptions<Id, J> & { readonly ctxFrom: readonly AnyNode[] };

/**
 * The options of the node `id` that `plan` describes. The join type comes from `plan` only (`NoInfer`): a call inside
 * `w.task(...)` takes no join type from the option type that `w.task` expects. Pure.
 */
export function node<const Id extends string, const J extends Join = DefaultJoin>(
  id: Id,
  plan: NodePlan<J> = {},
): PlannedOptions<Id, NoInfer<J>> {
  return {
    id,
    ...(plan.after === undefined ? {} : { waitsFor: plan.after }),
    ...(plan.join === undefined ? {} : { join: plan.join }),
    ...(plan.runsAfterFailure === undefined ? {} : { allowFailedDeps: true }),
    ...(plan.deadline === undefined ? {} : { goodUntil: plan.deadline }),
    ...(plan.queue === undefined ? {} : { queue: plan.queue }),
  };
}

/** The options of the node `id` whose workflow context holds the results of `sources`. Pure. */
export function contextNode<const Id extends string, const J extends Join = DefaultJoin>(
  id: Id,
  sources: readonly AnyNode[],
  plan: NodePlan<J> = {},
): ContextOptions<Id, NoInfer<J>> {
  return { ...node(id, plan), ctxFrom: sources };
}
