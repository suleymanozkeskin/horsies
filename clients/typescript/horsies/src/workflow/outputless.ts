// The read of the stored result of a COMPLETED workflow with no output node: the outputless envelope that the core
// writes (`horsies_workflow_outputless_result`). The client reads its `err` slot only; the `ok` map stays on the wire
// and is not read (the per-node values come from `results()`). Python and Rust read it the same way.
//
// The envelope: `{"__h_task_result__": true, "__h_outputless_terminals__": true, "ok": {...}, "err": null}`, or a
// non-null `err` with the workflow's error.

import { TASK_RESULT_ENVELOPE_KEY } from "../_catalog/codec_bounds.ts";
import { readErrorNode } from "../codec/envelope.ts";
import { isTreeObject, type TreeObject, type TreeValue } from "../codec/number-text.ts";
import { memberPath, refusalText, ROOT_PATH } from "../codec/refusal.ts";
import { err, ok, type Result } from "../wire/result.ts";
import type { TaskOutcome } from "../wire/task-error.ts";
import { NO_WORKFLOW_OUTPUT } from "./no-output.ts";
import type { NoWorkflowOutput } from "./node.ts";

/** The marker of the outputless envelope (Python's and Rust's `__h_outputless_terminals__`). */
export const OUTPUTLESS_TERMINALS_KEY = "__h_outputless_terminals__";

const ERR_SLOT = "err";

/** Why a stored outputless envelope does not read: the cause, with no stored value in it. */
export type OutputlessRefused = { readonly kind: "OutputlessRefused"; readonly cause: string };

/** Whether the stored result tree carries the outputless marker set to `true`. Pure. */
export function isOutputlessEnvelope(tree: TreeValue): boolean {
  return isTreeObject(tree) && memberOf(tree, OUTPUTLESS_TERMINALS_KEY) === true;
}

/**
 * The outcome of the outputless envelope `tree` of the COMPLETED workflow `workflowId`: the workflow's stored error
 * when the `err` slot is not null, else `NoWorkflowOutput`. The `ok` slot is not read. Refused: not an object, the
 * envelope marker missing or not `true`, and an error that does not read. Pure.
 */
export function outputlessOutcome(
  workflowId: string,
  tree: TreeValue,
): Result<TaskOutcome<NoWorkflowOutput, string>, OutputlessRefused> {
  if (!isTreeObject(tree) || memberOf(tree, TASK_RESULT_ENVELOPE_KEY) !== true) {
    return refused(`Workflow ${workflowId} result missing ${TASK_RESULT_ENVELOPE_KEY} envelope marker`);
  }

  const errorSlot = memberOf(tree, ERR_SLOT) ?? null;

  if (errorSlot === null) {
    return ok(Object.freeze({ kind: "succeeded", value: NO_WORKFLOW_OUTPUT }));
  }

  const error = readErrorNode(errorSlot, memberPath(ROOT_PATH, ERR_SLOT));

  return error.ok
    ? ok(Object.freeze({ kind: "failed", error: error.value }))
    : refused(`Workflow error payload does not read: ${refusalText(error.error)}`);
}

/** The own member `key` of a tree object, or `undefined` when it has none. Pure. */
function memberOf(node: TreeObject, key: string): TreeValue | undefined {
  return Object.hasOwn(node, key) ? node[key] : undefined;
}

function refused(cause: string): Result<never, OutputlessRefused> {
  return err(Object.freeze({ kind: "OutputlessRefused", cause }));
}
