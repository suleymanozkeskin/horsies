// The workflow context of a posted run input. For a task with `workflowContext: true`, the core's child host writes the
// context of the run's node into the input member `workflow_ctx` (`build_task_input`, `core/src/worker/execution.rs`)
// when the node names `ctxFrom`; for any other run of the task the input has no such member. The child takes the
// member out before the decode with the contract's parameters, which refuse an undeclared key, and gives its value as
// `runtime.workflowContext`. For a task that takes no context the core writes no member, and the input decodes as it
// is.

import { isTreeObject, treeObjectWithout, type TreeValue } from "../codec/number-text.ts";
import type { ContextUse } from "../contract/define-task.ts";
import { ok, type Result } from "../wire/result.ts";
import { unreachable } from "../wire/unreachable.ts";
import {
  type ContextRefused,
  NO_WORKFLOW_CONTEXT,
  type RuntimeWorkflowContext,
  workflowContextOfTree,
} from "../workflow/context.ts";

/** The input member that holds the workflow context (the core's `WORKFLOW_CTX_FIELD`). */
export const WORKFLOW_CONTEXT_MEMBER = "workflow_ctx";

/** A run input split into the parameters that the contract decodes and the workflow context of the runtime. */
export type WorkflowInput = { readonly parameters: TreeValue; readonly context: RuntimeWorkflowContext };

/**
 * The parameters and the workflow context of the input tree `tree` of a task with the context use `use`. A context
 * member that does not read is `ContextRefused`; the input then has no parameters to decode. Pure.
 */
export function workflowInputOf(tree: TreeValue, use: ContextUse): Result<WorkflowInput, ContextRefused> {
  switch (use) {
    case "noContext":
      return ok({ parameters: tree, context: NO_WORKFLOW_CONTEXT });
    case "takesContext":
      return contextInput(tree);
    default:
      return unreachable(use, "workflowInputOf");
  }
}

/** The split of the input of a task that takes the context: with its `workflow_ctx` member, or without one. Pure. */
function contextInput(tree: TreeValue): Result<WorkflowInput, ContextRefused> {
  // A tree object has no prototype and holds no `undefined`: a member that the text leaves out reads as `undefined`.
  const member = isTreeObject(tree) ? tree[WORKFLOW_CONTEXT_MEMBER] : undefined;

  if (member === undefined || !isTreeObject(tree)) {
    return ok({ parameters: tree, context: NO_WORKFLOW_CONTEXT });
  }

  const read = workflowContextOfTree(member);

  if (!read.ok) {
    return read;
  }

  return ok({
    parameters: treeObjectWithout(tree, WORKFLOW_CONTEXT_MEMBER),
    context: Object.freeze({ kind: "context", context: read.value }),
  });
}
