// The workflow context of a task that takes it (`defineTask({ ..., workflowContext: true })`): the results of the
// dependencies that its node names in `ctxFrom`, by node id. The core writes it into the input member `workflow_ctx`
// (the core's `WorkflowContext`: `workflow_id`, `task_index`, `task_name`, `results_by_id`); the Node child reads that
// member with `workflowContextOf` and gives the value as `runtime.workflowContext`.

import { isTreeObject, readTree, type TreeObject, type TreeValue } from "../codec/number-text.ts";
import type { JsonText } from "../codec/json-text.ts";
import { decodeNode } from "../codec/reader.ts";
import { refusalText, ROOT_PATH } from "../codec/refusal.ts";
import { isNumber, isString, isUuidText } from "../codec/values.ts";
import type { ContextUse, ExecutionClass, TaskContract } from "../contract/define-task.ts";
import type { WorkflowId } from "../contract/ids.ts";
import type { OutputSchema, ParameterSchemas, SchemaValue } from "../contract/schema.ts";
import { type ReadForm, readFormOf } from "../read/handle.ts";
import { err, ok, type Result } from "../wire/result.ts";
import type { TaskOutcome } from "../wire/task-error.ts";
import { unreachable } from "../wire/unreachable.ts";

/** Why a context read gave no value. */
export type ContextReadError =
  /** The context holds no entry for the node: it is not in the node's `ctxFrom`, or the dependency did not end. */
  | { readonly kind: "notInContext"; readonly nodeId: string }
  /** The stored value does not read with the contract's types. `cause` holds no value. */
  | { readonly kind: "unreadable"; readonly nodeId: string; readonly cause: string }
  /** The stored error has a user code that the contract does not list. */
  | { readonly kind: "unknownUserCode"; readonly nodeId: string; readonly code: string };

/** The workflow context of one task run. */
export interface WorkflowContext {
  readonly workflowId: WorkflowId;
  /** The index of the task's node in its workflow. */
  readonly taskIndex: number;
  /** The task name of the node. */
  readonly taskName: string;
  /** The outcome of the context node `nodeId`, read with the output type and the codes of `contract`. */
  resultFor<
    P extends ParameterSchemas,
    O extends OutputSchema,
    E extends string,
    X extends ExecutionClass,
    C extends ContextUse,
  >(
    nodeId: string,
    contract: TaskContract<P, O, E, X, C>,
  ): Result<TaskOutcome<SchemaValue<O>, E>, ContextReadError>;
}

/** Why a `workflow_ctx` text is not a workflow context: the member and the rule. */
export type ContextRefused = { readonly kind: "ContextRefused"; readonly member: string; readonly detail: string };

const INDEX_MIN = 0;

/** The workflow context of a `workflow_ctx` text that passed the core's strict check. Pure. */
export function workflowContextOf(text: JsonText): Result<WorkflowContext, ContextRefused> {
  const tree = readTree(text);

  if (!isTreeObject(tree)) {
    return refused("$", "the context is not an object");
  }

  const workflowId = tree["workflow_id"];
  const taskIndex = tree["task_index"];
  const taskName = tree["task_name"];
  const results = tree["results_by_id"] ?? {};

  if (!isString(workflowId) || !isUuidText(workflowId)) {
    return refused("workflow_id", "not a UUID in the lower-case 8-4-4-4-12 form");
  }

  if (!isNumber(taskIndex) || !Number.isSafeInteger(taskIndex) || taskIndex < INDEX_MIN) {
    return refused("task_index", "not an integer of 0 or more");
  }

  if (!isString(taskName)) {
    return refused("task_name", "not a string");
  }

  if (!isTreeObject(results)) {
    return refused("results_by_id", "not an object");
  }

  // SAFETY: `workflowId` passed `isUuidText`, the rule of `WorkflowId.parse`.
  return ok(contextValue(workflowId as WorkflowId, taskIndex, taskName, results));
}

function refused(member: string, detail: string): Result<never, ContextRefused> {
  return err(Object.freeze({ kind: "ContextRefused", member, detail }));
}

function contextValue(
  workflowId: WorkflowId,
  taskIndex: number,
  taskName: string,
  results: TreeObject,
): WorkflowContext {
  const resultFor = (nodeId: string, contract: object): Result<TaskOutcome<unknown, string>, ContextReadError> => {
    const node = Object.hasOwn(results, nodeId) ? results[nodeId] : undefined;

    return node === undefined
      ? err(Object.freeze({ kind: "notInContext", nodeId }))
      : contextOutcome(nodeId, node, readFormOf(contract));
  };

  return Object.freeze({
    workflowId,
    taskIndex,
    taskName,
    // SAFETY: the reader decodes with `contract`: its output type and its codes are the result's types.
    resultFor: resultFor as WorkflowContext["resultFor"],
  });
}

/** The outcome of one stored envelope of the context, under a read form. Pure. */
function contextOutcome(
  nodeId: string,
  node: TreeValue,
  form: ReadForm,
): Result<TaskOutcome<unknown, string>, ContextReadError> {
  switch (form.kind) {
    case "unreadable":
      return err(Object.freeze({ kind: "unreadable", nodeId, cause: form.cause }));
    case "readable": {
      const decoded = decodeNode({ node: "resultOf", inner: form.output }, node, ROOT_PATH, 0, "worker");

      if (!decoded.ok) {
        return err(Object.freeze({ kind: "unreadable", nodeId, cause: refusalText(decoded.error) }));
      }

      // SAFETY: the reader decodes a `resultOf` kind into a task outcome.
      const outcome = decoded.value as TaskOutcome<unknown, string>;

      if (outcome.kind === "succeeded" || outcome.error.code.kind === "library" || form.codes.kind === "open") {
        return ok(outcome);
      }

      return form.codes.codes.includes(outcome.error.code.code)
        ? ok(outcome)
        : err(Object.freeze({ kind: "unknownUserCode", nodeId, code: outcome.error.code.code }));
    }

    default:
      return unreachable(form, "contextOutcome");
  }
}
