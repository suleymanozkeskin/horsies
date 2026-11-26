// The workflow context of a task that takes it (`defineTask({ ..., workflowContext: true })`): the results and the
// sub-workflow summaries of the dependencies that its node names in `ctxFrom`, by node id. The core writes it into the
// input member `workflow_ctx` (the core's `WorkflowContext`: `workflow_id`, `task_index`, `task_name`,
// `results_by_id`, `summaries_by_id`); the Node child takes that member out of the input, reads it with
// `workflowContextOfTree` and gives the value as `runtime.workflowContext` (`RuntimeWorkflowContext`). The reads are
// those of the Rust and Python clients: `resultFor`, `hasResult`, `summaryFor`, `outputFor`, `hasSummary`.

import { isTreeObject, readTree, type TreeObject, type TreeValue } from "../codec/number-text.ts";
import type { JsonText } from "../codec/json-text.ts";
import { decodeEnvelopeNode, decodeNode } from "../codec/reader.ts";
import { refusalText, ROOT_PATH } from "../codec/refusal.ts";
import { isNumber, isString, isUuidText } from "../codec/values.ts";
import type { ContextUse, ExecutionClass, TaskContract } from "../contract/define-task.ts";
import type { WorkflowId } from "../contract/ids.ts";
import type { OutputSchema, ParameterSchemas, SchemaValue } from "../contract/schema.ts";
import { type ReadForm, readFormOf } from "../read/handle.ts";
import type { Recorded } from "../read/info.ts";
import { readSubWorkflowSummary } from "../codec/sub-workflow.ts";
import { memberPath } from "../codec/refusal.ts";
import { err, ok, type Result } from "../wire/result.ts";
import type { SubWorkflowSummary, TaskOutcome } from "../wire/task-error.ts";
import { unreachable } from "../wire/unreachable.ts";
import type { WorkflowContract } from "./define.ts";
import type { BuildReturn, WorkflowOutput } from "./node.ts";
import { declaredOutputForm } from "./registry.ts";

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
  /** Whether the context holds the result of the node `nodeId`. */
  hasResult(nodeId: string): boolean;
  /** The summary of the context sub-workflow node `nodeId`, its output as stored. */
  summaryFor(nodeId: string): Result<SubWorkflowSummary, ContextReadError>;
  /**
   * The output of the context sub-workflow node `nodeId`, read with the declared output of `workflow`, its child:
   * `notRecorded` when the child stored no output (it did not complete with an ok value).
   */
  outputFor<P extends ParameterSchemas, R extends BuildReturn>(
    nodeId: string,
    workflow: WorkflowContract<P, R>,
  ): Result<Recorded<WorkflowOutput<R>>, ContextReadError>;
  /** Whether the context holds the summary of the node `nodeId`. */
  hasSummary(nodeId: string): boolean;
}

/**
 * The `workflowContext` member of the runtime of a task with `workflowContext: true`: the context that the core wrote
 * into the run's input, or `noContext`. The core writes it only for a workflow node that names `ctxFrom`; a direct
 * send and a node with no `ctxFrom` give `noContext`, as Python gives `workflow_ctx = None`.
 */
export type RuntimeWorkflowContext =
  | { readonly kind: "context"; readonly context: WorkflowContext }
  | { readonly kind: "noContext" };

/** The runtime member of a run whose input holds no workflow context. */
export const NO_WORKFLOW_CONTEXT: RuntimeWorkflowContext = Object.freeze({ kind: "noContext" });

/** One stored sub-workflow summary of the context: the summary, and its stored output for a typed read. */
type StoredSummary = { readonly summary: SubWorkflowSummary; readonly output: TreeValue };

/** Why a `workflow_ctx` text is not a workflow context: the member and the rule. */
export type ContextRefused = { readonly kind: "ContextRefused"; readonly member: string; readonly detail: string };

const INDEX_MIN = 0;

/** The workflow context of a `workflow_ctx` text that passed the core's strict check. Pure. */
export function workflowContextOf(text: JsonText): Result<WorkflowContext, ContextRefused> {
  return workflowContextOfTree(readTree(text));
}

/** The workflow context of the tree of a `workflow_ctx` member, read from a text that passed the strict check. Pure. */
export function workflowContextOfTree(tree: TreeValue): Result<WorkflowContext, ContextRefused> {
  if (!isTreeObject(tree)) {
    return refused("$", "the context is not an object");
  }

  const workflowId = tree["workflow_id"];
  const taskIndex = tree["task_index"];
  const taskName = tree["task_name"];
  const results = tree["results_by_id"] ?? {};
  const summaries = tree["summaries_by_id"] ?? {};

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

  const stored = storedSummaries(summaries);

  if (!stored.ok) {
    return stored;
  }

  // SAFETY: `workflowId` passed `isUuidText`, the rule of `WorkflowId.parse`.
  return ok(contextValue(workflowId as WorkflowId, taskIndex, taskName, results, stored.value));
}

/** The summaries of `summaries_by_id`, by node id, or the refusal of the first one that does not read. Pure. */
function storedSummaries(node: TreeValue): Result<ReadonlyMap<string, StoredSummary>, ContextRefused> {
  if (!isTreeObject(node)) {
    return refused("summaries_by_id", "not an object");
  }

  const read = new Map<string, StoredSummary>();

  for (const [nodeId, summaryNode] of Object.entries(node)) {
    const member = memberPath("summaries_by_id", nodeId);
    const summary = readSubWorkflowSummary(summaryNode, member);

    if (!summary.ok) {
      return refused(member, refusalText(summary.error));
    }

    const output = isTreeObject(summaryNode) ? (summaryNode["output"] ?? null) : null;

    read.set(nodeId, Object.freeze({ summary: summary.value, output }));
  }

  return ok(read);
}

function refused(member: string, detail: string): Result<never, ContextRefused> {
  return err(Object.freeze({ kind: "ContextRefused", member, detail }));
}

function contextValue(
  workflowId: WorkflowId,
  taskIndex: number,
  taskName: string,
  results: TreeObject,
  summaries: ReadonlyMap<string, StoredSummary>,
): WorkflowContext {
  const resultFor = (nodeId: string, contract: object): Result<TaskOutcome<unknown, string>, ContextReadError> => {
    const node = Object.hasOwn(results, nodeId) ? results[nodeId] : undefined;

    return node === undefined ? notInContext(nodeId) : contextOutcome(nodeId, node, readFormOf(contract));
  };

  const summaryFor = (nodeId: string): Result<SubWorkflowSummary, ContextReadError> => {
    const stored = summaries.get(nodeId);

    return stored === undefined ? notInContext(nodeId) : ok(stored.summary);
  };

  const outputFor = (nodeId: string, workflow: object): Result<Recorded<unknown>, ContextReadError> => {
    const stored = summaries.get(nodeId);

    return stored === undefined ? notInContext(nodeId) : childOutput(nodeId, stored.output, workflow);
  };

  return Object.freeze({
    workflowId,
    taskIndex,
    taskName,
    // SAFETY: the reader decodes with `contract`: its output type and its codes are the result's types.
    resultFor: resultFor as WorkflowContext["resultFor"],
    hasResult: (nodeId: string) => Object.hasOwn(results, nodeId),
    summaryFor,
    // SAFETY: the reader decodes with the declared output of `workflow`, whose value type is `WorkflowOutput<R>`.
    outputFor: outputFor as WorkflowContext["outputFor"],
    hasSummary: (nodeId: string) => summaries.has(nodeId),
  });
}

function notInContext(nodeId: string): Result<never, ContextReadError> {
  return err(Object.freeze({ kind: "notInContext", nodeId }));
}

/**
 * The stored output of a child, read with the declared output of `workflow`: `notRecorded` for no output (the core
 * stores none unless the child completed with an ok value). Pure apart from the first compile of the contract.
 */
function childOutput(nodeId: string, output: TreeValue, workflow: object): Result<Recorded<unknown>, ContextReadError> {
  if (output === null) {
    return ok(Object.freeze({ kind: "notRecorded" }));
  }

  const form = declaredOutputForm(workflow);

  switch (form.kind) {
    case "unreadable":
      return err(Object.freeze({ kind: "unreadable", nodeId, cause: form.cause }));
    case "readable": {
      const decoded = decodeNode(form.output, output, ROOT_PATH, 0, "worker");

      return decoded.ok
        ? ok(Object.freeze({ kind: "recorded", value: decoded.value }))
        : err(Object.freeze({ kind: "unreadable", nodeId, cause: refusalText(decoded.error) }));
    }

    default:
      return unreachable(form, "childOutput");
  }
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
      const decoded = decodeEnvelopeNode(form.output, node, ROOT_PATH, "worker");

      if (!decoded.ok) {
        return err(Object.freeze({ kind: "unreadable", nodeId, cause: refusalText(decoded.error) }));
      }

      const outcome = decoded.value;

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
