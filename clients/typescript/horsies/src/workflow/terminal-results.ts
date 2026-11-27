// The read of the stored result of a COMPLETED workflow with no output node: the outputless envelope that the core
// writes (`horsies_workflow_outputless_result`), read as the Python handle (`_decode_outputless_ok`) and the Rust handle
// (`outputless_outcome`) read it.
//
// The envelope: `{"__h_task_result__": true, "__h_outputless_terminals__": true, "ok": {"results_by_id": {...},
// "task_name_by_id": {...}, "definition_key_by_id": {...}}, "err": null}`, or a non-null `err` with the workflow's
// error. Each entry of `results_by_id` is the stored result envelope of one terminal node.

import { OperationalErrorCode } from "../_catalog/task_codes.ts";
import { TASK_RESULT_ENVELOPE_KEY } from "../codec/bounds.ts";
import { readErrorNode } from "../codec/envelope.ts";
import type { JsonValue } from "../codec/json-value.ts";
import { isTreeObject, type TreeObject, type TreeValue } from "../codec/number-text.ts";
import { decodeEnvelopeNode } from "../codec/reader.ts";
import { memberPath, refusalText, ROOT_PATH } from "../codec/refusal.ts";
import { isString } from "../codec/values.ts";
import { JSON_KIND } from "../wire/kinds.ts";
import { err, ok, type Result } from "../wire/result.ts";
import type { TaskOutcome } from "../wire/task-error.ts";
import type { WorkflowTerminalResults } from "./node.ts";

/** The marker of the outputless envelope (Python's and Rust's `__h_outputless_terminals__`). */
export const OUTPUTLESS_TERMINALS_KEY = "__h_outputless_terminals__";

const OK_SLOT = "ok";
const ERR_SLOT = "err";
const RESULTS_BY_ID = "results_by_id";
const TASK_NAME_BY_ID = "task_name_by_id";
const DEFINITION_KEY_BY_ID = "definition_key_by_id";

// SAFETY: `Object.create(null)` returns an empty object with no prototype, the form of a tree object.
const EMPTY_MAP: TreeObject = Object.freeze(Object.create(null) as TreeObject);

/** Why a stored outputless envelope does not read: the cause, with no stored value in it. */
export type TerminalResultsRefused = { readonly kind: "TerminalResultsRefused"; readonly cause: string };

/** Whether the stored result tree carries the outputless marker set to `true`. Pure. */
export function isOutputlessEnvelope(tree: TreeValue): boolean {
  return isTreeObject(tree) && memberOf(tree, OUTPUTLESS_TERMINALS_KEY) === true;
}

/**
 * The outcome of the outputless envelope `tree` of the COMPLETED workflow `workflowId`: the workflow's stored error,
 * or the outcome of each node of `results_by_id`. `results_by_id`, `task_name_by_id` and `definition_key_by_id` are
 * objects when present; a missing one is empty. A node entry that is `null`, has no task name, or does not read is a
 * failed outcome with `RESULT_DESERIALIZATION_ERROR` and the node id in `data.node_id`; the other entries stay
 * readable. Refused: not an object, the envelope marker missing or not `true`, an error that does not read, an `ok`
 * slot that is not an object, and a map member that is not an object. Pure.
 */
export function terminalResultsOutcome(
  workflowId: string,
  tree: TreeValue,
): Result<TaskOutcome<WorkflowTerminalResults, string>, TerminalResultsRefused> {
  if (!isTreeObject(tree) || memberOf(tree, TASK_RESULT_ENVELOPE_KEY) !== true) {
    return refused(`Workflow ${workflowId} result missing ${TASK_RESULT_ENVELOPE_KEY} envelope marker`);
  }

  const errorSlot = memberOf(tree, ERR_SLOT) ?? null;

  if (errorSlot !== null) {
    const error = readErrorNode(errorSlot, memberPath(ROOT_PATH, ERR_SLOT));

    return error.ok
      ? ok(Object.freeze({ kind: "failed", error: error.value }))
      : refused(`Workflow error payload does not read: ${refusalText(error.error)}`);
  }

  const okSlot = memberOf(tree, OK_SLOT);

  if (okSlot === undefined || !isTreeObject(okSlot)) {
    return refused("Outputless workflow envelope ok slot malformed");
  }

  const entries = mapMember(okSlot, RESULTS_BY_ID);
  const taskNames = mapMember(okSlot, TASK_NAME_BY_ID);
  const definitionKeys = mapMember(okSlot, DEFINITION_KEY_BY_ID);

  if (entries === "notAnObject" || taskNames === "notAnObject" || definitionKeys === "notAnObject") {
    return refused(
      `Outputless workflow envelope missing or malformed ${RESULTS_BY_ID} / ${TASK_NAME_BY_ID} / ${DEFINITION_KEY_BY_ID}`,
    );
  }

  const byNodeId = new Map(
    Object.entries(entries).map(([nodeId, entry]) => {
      const named = isString(memberOf(taskNames, nodeId));

      return [nodeId, terminalEntry(workflowId, nodeId, entry, named)] as const;
    }),
  );

  return ok(Object.freeze({ kind: "succeeded", value: Object.freeze({ kind: "terminalResults", byNodeId }) }));
}

/**
 * The outcome of one entry of `results_by_id`; `named` tells whether `task_name_by_id` names its task. The value is
 * untyped JSON and the custom code is open. Pure.
 */
function terminalEntry(
  workflowId: string,
  nodeId: string,
  entry: TreeValue,
  named: boolean,
): TaskOutcome<JsonValue, string> {
  const nodeText = `Outputless workflow ${workflowId} node ${JSON.stringify(nodeId)}`;

  if (entry === null || !named) {
    return entryNotRead(nodeId, `${nodeText} missing payload or task name`);
  }

  const decoded = decodeEnvelopeNode(JSON_KIND, entry, memberPath(RESULTS_BY_ID, nodeId), "worker");

  // SAFETY: the reader decodes the value with the JSON kind, whose value type is `JsonValue`.
  return decoded.ok
    ? (decoded.value as TaskOutcome<JsonValue, string>)
    : entryNotRead(nodeId, `${nodeText} does not read: ${refusalText(decoded.error)}`);
}

/** The failed outcome of an entry that does not read: `RESULT_DESERIALIZATION_ERROR`, the node id in `data`. Pure. */
function entryNotRead(nodeId: string, message: string): TaskOutcome<JsonValue, string> {
  return Object.freeze({
    kind: "failed",
    error: Object.freeze({
      kind: "builtIn",
      code: OperationalErrorCode.RESULT_DESERIALIZATION_ERROR,
      message,
      data: Object.freeze({ node_id: nodeId }),
      exception: null,
      subWorkflow: null,
    }),
  });
}

/** The own member `key` of a tree object, or `undefined` when it has none. Pure. */
function memberOf(node: TreeObject, key: string): TreeValue | undefined {
  return Object.hasOwn(node, key) ? node[key] : undefined;
}

/** The object member `key`: empty when it is missing, `notAnObject` when it is another value. Pure. */
function mapMember(node: TreeObject, key: string): TreeObject | "notAnObject" {
  const member = memberOf(node, key);

  if (member === undefined) {
    return EMPTY_MAP;
  }

  return isTreeObject(member) ? member : "notAnObject";
}

function refused(cause: string): Result<never, TerminalResultsRefused> {
  return err(Object.freeze({ kind: "TerminalResultsRefused", cause }));
}
