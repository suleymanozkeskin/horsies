// The handle of a started workflow: its status, its result (`get`), the result of one node (`resultFor`), the results
// and the nodes, and pause, resume and cancel. Built by a start, by `retryStart`, or by `app.workflowHandle` from a
// stored id; it does no I/O until a call.
//
// Every stored text that a read gives passed the core's strict JSON check in the addon; the codec reader then decodes
// it with the types of the handle. `get` reads open user codes (a workflow holds the codes of many tasks); `resultFor`
// reads one node with that node's contract.

import { WorkflowTaskStatus } from "../_catalog/status.ts";
import type { HandleCoreError } from "../_catalog/workflow_handle_errors.ts";
import { jsonTextCheckedByCore } from "../codec/json-text.ts";
import { readStoredError } from "../codec/envelope.ts";
import type { JsonValue } from "../codec/json-value.ts";
import { readTree } from "../codec/number-text.ts";
import { decodeEnvelope } from "../codec/reader.ts";
import { refusalText } from "../codec/refusal.ts";
import { readSubWorkflow } from "../codec/sub-workflow.ts";
import type { ExecutionClass, TaskContract, ContextUse } from "../contract/define-task.ts";
import type { WorkflowId } from "../contract/ids.ts";
import type { OutputSchema, ParameterSchemas, SchemaValue } from "../contract/schema.ts";
import type { CheckedText, StoredNodeRecord, StoredTextFact, WorkflowWaitRead } from "../native/workflows/addon.ts";
import type { GetOptions, ReadForm } from "../read/handle.ts";
import { instantOfMicros, type Recorded } from "../read/info.ts";
import { JSON_KIND } from "../wire/kinds.ts";
import { err, ok, type Result } from "../wire/result.ts";
import type { SubWorkflowSummary, TaskOutcome, WorkflowStatus } from "../wire/task-error.ts";
import { unreachable } from "../wire/unreachable.ts";
import type { WorkflowCore } from "./core.ts";
import {
  handleErrorOfRaw,
  readErrorOfHandle,
  storedDataInvalid,
  type WorkflowReadError,
  workflowNotFound,
  workflowWait,
} from "./handle-error.ts";
import { isWorkflowContract, type WorkflowContract } from "./define.ts";
import type { NodeTarget } from "./node-checks.ts";
import type { BuildReturn, NodeById, NodeCode, NodeOutput, ReturnedNodes, WorkflowOutput } from "./node.ts";
import { nodeTargetForm, workflowOutputForm, type WorkflowRegistry } from "./registry.ts";

/** The stored result of one node read with no type: its outcome with open codes, or why it does not read. */
export type NodeResult =
  | { readonly kind: "outcome"; readonly outcome: TaskOutcome<JsonValue, string> }
  | { readonly kind: "unreadable"; readonly cause: string };

/** One node of a workflow. */
export interface WorkflowTaskInfo {
  /** The node id; a node stored without one has none. */
  readonly nodeId: Recorded<string>;
  readonly index: number;
  /** The task name, or the child's workflow name. */
  readonly name: string;
  readonly status: WorkflowTaskStatus;
  readonly result: Recorded<NodeResult>;
  readonly startedAt: Recorded<Temporal.Instant>;
  readonly completedAt: Recorded<Temporal.Instant>;
  /** The child workflow of a sub-workflow node. */
  readonly subWorkflowId: Recorded<string>;
  /** The summary of the ended child of a sub-workflow node. */
  readonly subWorkflowSummary: Recorded<SubWorkflowSummary>;
}

/** The handle operations that every handle has. */
export interface WorkflowHandleBase<R extends BuildReturn> {
  readonly workflowId: WorkflowId;
  status(): Promise<Result<WorkflowStatus, HandleCoreError>>;
  /** The workflow's outcome, waiting while it runs, up to `timeoutMs`. The user code is open. */
  get(options?: GetOptions): Promise<Result<TaskOutcome<WorkflowOutput<R>, string>, WorkflowReadError>>;
  /** The stored result of each node that has one, by node id. */
  results(): Promise<Result<ReadonlyMap<string, NodeResult>, HandleCoreError>>;
  /** Every node, in index order. */
  tasks(): Promise<Result<readonly WorkflowTaskInfo[], HandleCoreError>>;
  /** Pause a RUNNING workflow: `true` when it paused. */
  pause(): Promise<Result<boolean, HandleCoreError>>;
  /** Resume a PAUSED workflow: `true` when it resumed. */
  resume(): Promise<Result<boolean, HandleCoreError>>;
  /** Cancel the workflow and its descendants. */
  cancel(): Promise<Result<void, HandleCoreError>>;
}

/** The handle of a started workflow: `resultFor` takes the id of a node of the returned object. */
export interface WorkflowHandle<R extends BuildReturn> extends WorkflowHandleBase<R> {
  /** The outcome of one node, typed by its contract. Not a wait. */
  resultFor<const Id extends ReturnedNodes<R>["id"]>(
    id: Id,
  ): Promise<
    Result<
      TaskOutcome<NodeOutput<NodeById<ReturnedNodes<R>, Id>>, NodeCode<NodeById<ReturnedNodes<R>, Id>>>,
      WorkflowReadError
    >
  >;
}

/**
 * The handle of a stored workflow id: it has no build, so `resultFor` takes what the node runs: a task contract, or
 * the workflow contract of a sub-workflow node (read with the child's declared output and open user codes; the app
 * lists the child, else `workflowNotListed`).
 */
export interface WorkflowHandleById<R extends BuildReturn> extends WorkflowHandleBase<R> {
  /** The outcome of the task node `id`, read with the output type and the codes of `contract`. Not a wait. */
  resultFor<
    P extends ParameterSchemas,
    O extends OutputSchema,
    E extends string,
    X extends ExecutionClass,
    C extends ContextUse,
  >(
    id: string,
    contract: TaskContract<P, O, E, X, C>,
  ): Promise<Result<TaskOutcome<SchemaValue<O>, E>, WorkflowReadError>>;
  /** The outcome of the sub-workflow node `id`, read with the declared output of `workflow`, its child. Not a wait. */
  resultFor<P extends ParameterSchemas, C extends BuildReturn>(
    id: string,
    workflow: WorkflowContract<P, C>,
  ): Promise<Result<TaskOutcome<WorkflowOutput<C>, string>, WorkflowReadError>>;
}

/** What a handle reads with: the core, the app's workflows, the workflow contract, and the nodes of its build. */
export type HandleFacts = {
  readonly core: WorkflowCore;
  readonly registry: WorkflowRegistry;
  readonly workflow: object;
  readonly workflowId: WorkflowId;
  /** What each node id runs: the nodes of the build that started the workflow; empty for a handle by id. */
  readonly targets: ReadonlyMap<string, NodeTarget>;
};

type ReadOutcome = Result<TaskOutcome<unknown, string>, WorkflowReadError>;

const WORKFLOW_PAUSED_MESSAGE = "workflow is paused awaiting intervention";

/** A handle of a started workflow. Pure. */
export function workflowHandleOf<R extends BuildReturn>(facts: HandleFacts): WorkflowHandle<R> {
  const base = handleBase<R>(facts);

  return Object.freeze({
    ...base,
    resultFor: async (id: string) => {
      const target = facts.targets.get(id);

      // SAFETY: the reader decodes with the node's contract: its output type and its codes are the node's types.
      return (
        target === undefined ? err(Object.freeze({ kind: "resultNotReady" })) : targetOutcome(facts, id, target)
      ) as never;
    },
  });
}

/** A handle of a stored workflow id. Pure. */
export function workflowHandleByIdOf<R extends BuildReturn>(facts: HandleFacts): WorkflowHandleById<R> {
  return Object.freeze({
    ...handleBase<R>(facts),
    resultFor: async (id: string, contract: object) =>
      // SAFETY: the reader decodes with what `contract` declares: its output type and its codes are the result's types.
      (await targetOutcome(facts, id, targetOf(contract))) as never,
  });
}

function handleBase<R extends BuildReturn>(facts: HandleFacts): WorkflowHandleBase<R> {
  const id = facts.workflowId;

  return {
    workflowId: id,
    status: async () => {
      const read = await facts.core.status(id);

      return read.kind === "status" ? ok(read.status) : err(handleErrorOfRaw(read.error));
    },
    // SAFETY: the reader decodes with the output form of the workflow, whose value type is `WorkflowOutput<R>`.
    get: async (options: GetOptions = {}) => (await workflowOutcome(facts, options)) as never,
    results: async () => nodeResults(facts),
    tasks: async () => nodeInfos(facts),
    pause: async () => changed(await facts.core.pause(id)),
    resume: async () => changed(await facts.core.resume(id)),
    cancel: async () => {
      const done = await facts.core.cancel(id);

      return done.kind === "done" ? ok(undefined) : err(handleErrorOfRaw(done.error));
    },
  };
}

function changed(read: Awaited<ReturnType<WorkflowCore["pause"]>>): Result<boolean, HandleCoreError> {
  return read.kind === "changed" ? ok(read.changed) : err(handleErrorOfRaw(read.error));
}

/**
 * `get`: the wait, then the outcome of the ended workflow. `invalidWait`, `workflowNotListed` and `waitInsideTask`
 * are decided before any statement.
 */
async function workflowOutcome(facts: HandleFacts, options: GetOptions): Promise<ReadOutcome> {
  const wait = workflowWait(options.timeoutMs);

  if (!wait.ok) {
    return wait;
  }

  const output = workflowOutputForm(facts.workflow, facts.registry);

  switch (output.kind) {
    case "workflowNotListed":
      return err(notListedError(output.workflowName));
    case "form":
      break;
    default:
      return unreachable(output, "workflowOutcome");
  }

  if ((await facts.core.processRole()) === "RUNS_TASK_BODIES") {
    return err(Object.freeze({ kind: "waitInsideTask" }));
  }

  const read = await facts.core.waitEnd(facts.workflowId, wait.value);

  return endOutcome(read, output.form);
}

/** The outcome of one node of the build: the read with its form, or `workflowNotListed` at once, with no read. */
async function targetOutcome(facts: HandleFacts, nodeId: string, target: NodeTarget): Promise<ReadOutcome> {
  const output = nodeTargetForm(target, facts.registry);

  switch (output.kind) {
    case "form":
      return nodeOutcome(facts, nodeId, output.form);
    case "workflowNotListed":
      return err(notListedError(output.workflowName));
    default:
      return unreachable(output, "targetOutcome");
  }
}

/** What a node of a handle by id runs: the workflow contract of a sub-workflow node, else a task contract. Pure. */
function targetOf(contract: object): NodeTarget {
  return isWorkflowContract(contract)
    ? Object.freeze({ kind: "subWorkflow", workflow: contract })
    : Object.freeze({ kind: "task", contract });
}

/** The read error of a workflow that the app does not list. Pure. */
function notListedError(workflowName: string): WorkflowReadError {
  return Object.freeze({ kind: "workflowNotListed", workflowName });
}

/** The outcome of what a wait found, read with the output form `form`. Pure. */
export function endOutcome(read: WorkflowWaitRead, form: ReadForm): ReadOutcome {
  switch (read.kind) {
    case "completed":
      return completedOutcome(read.result, form);
    case "endedWithError":
      return erroredOutcome(read.status, read.error);
    case "paused":
      return ok(libraryFailure("WORKFLOW_PAUSED", WORKFLOW_PAUSED_MESSAGE));
    case "timedOut":
      return err(Object.freeze({ kind: "waitTimedOut" }));
    case "notFound":
      return err(Object.freeze({ kind: "workflowNotFound" }));
    case "failed":
      return err(readErrorOfHandle(handleErrorOfRaw(read.error)));
    default:
      return unreachable(read, "endOutcome");
  }
}

function completedOutcome(result: StoredTextFact, form: ReadForm): ReadOutcome {
  switch (result.kind) {
    case "none":
      return unreadable("the workflow is COMPLETED and its stored result is NULL");
    case "refused":
      return unreadable(result.cause);
    case "checked":
      return envelopeOutcome(result.text, form, "open");
    default:
      return unreachable(result, "completedOutcome");
  }
}

function erroredOutcome(status: "FAILED" | "CANCELLED" | "EXPIRED", error: StoredTextFact): ReadOutcome {
  switch (error.kind) {
    case "none":
      return ok(libraryFailure(`WORKFLOW_${status}`, statusMessage(status)));
    case "refused":
      return unreadable(error.cause);
    case "checked": {
      const stored = readStoredError(jsonTextCheckedByCore(error.text));

      return stored.ok
        ? ok(Object.freeze({ kind: "failed", error: stored.value }))
        : unreadable(refusalText(stored.error));
    }

    default:
      return unreachable(error, "erroredOutcome");
  }
}

/** Python's message of an ended status with no stored error. Pure. */
function statusMessage(status: "FAILED" | "CANCELLED" | "EXPIRED"): string {
  switch (status) {
    case "FAILED":
      return "Workflow failed";
    case "CANCELLED":
      return "Workflow cancelled";
    case "EXPIRED":
      return "Workflow expired";
    default:
      return unreachable(status, "statusMessage");
  }
}

function libraryFailure(
  code: "WORKFLOW_PAUSED" | "WORKFLOW_FAILED" | "WORKFLOW_CANCELLED" | "WORKFLOW_EXPIRED",
  message: string,
): TaskOutcome<unknown, string> {
  return Object.freeze({
    kind: "failed",
    error: Object.freeze({
      code: Object.freeze({ kind: "library", code }),
      message,
      data: null,
      exception: null,
      subWorkflow: null,
    }),
  });
}

/** Codes of the reader: open (every user code), or the declared codes of the read form. */
type CodeRule = "open" | "declared";

/** The outcome of a stored envelope under a read form. Pure. */
function envelopeOutcome(text: string, form: ReadForm, codes: CodeRule): ReadOutcome {
  switch (form.kind) {
    case "unreadable":
      return unreadable(form.cause);
    case "readable": {
      const decoded = decodeEnvelope(form.output, jsonTextCheckedByCore(text), "worker");

      if (!decoded.ok) {
        return unreadable(refusalText(decoded.error));
      }

      const outcome = decoded.value;

      if (
        codes === "open" ||
        outcome.kind === "succeeded" ||
        outcome.error.code.kind === "library" ||
        form.codes.kind === "open"
      ) {
        return ok(outcome);
      }

      return form.codes.codes.includes(outcome.error.code.code)
        ? ok(outcome)
        : err(Object.freeze({ kind: "unknownUserCode", code: outcome.error.code.code }));
    }

    default:
      return unreachable(form, "envelopeOutcome");
  }
}

/** `resultFor`: the stored result of one node, under the node's read form. Not a wait. */
async function nodeOutcome(facts: HandleFacts, nodeId: string, form: ReadForm): Promise<ReadOutcome> {
  const read = await facts.core.nodeResult(facts.workflowId, nodeId);

  switch (read.kind) {
    case "notReady":
      return err(Object.freeze({ kind: "resultNotReady" }));
    case "failed":
      return err(readErrorOfHandle(handleErrorOfRaw(read.error)));
    case "stored":
      return checkedOutcome(read.result, form);
    default:
      return unreachable(read, "nodeOutcome");
  }
}

function checkedOutcome(result: CheckedText, form: ReadForm): ReadOutcome {
  return result.kind === "checked" ? envelopeOutcome(result.text, form, "declared") : unreadable(result.cause);
}

function unreadable(cause: string): ReadOutcome {
  return err(Object.freeze({ kind: "storedResultUnreadable", cause }));
}

/** The result of one node with open codes and a JSON value. Pure. */
function nodeResultOf(result: CheckedText): NodeResult {
  if (result.kind === "refused") {
    return Object.freeze({ kind: "unreadable", cause: result.cause });
  }

  const decoded = decodeEnvelope(JSON_KIND, jsonTextCheckedByCore(result.text), "worker");

  // SAFETY: the reader decodes the value with the JSON kind, whose value type is `JsonValue`.
  return decoded.ok
    ? Object.freeze({ kind: "outcome", outcome: decoded.value as TaskOutcome<JsonValue, string> })
    : Object.freeze({ kind: "unreadable", cause: refusalText(decoded.error) });
}

async function nodeResults(facts: HandleFacts): Promise<Result<ReadonlyMap<string, NodeResult>, HandleCoreError>> {
  const read = await facts.core.nodeResults(facts.workflowId);

  switch (read.kind) {
    case "nodes":
      return ok(
        new Map(
          read.nodes.flatMap((node) =>
            node.nodeId.kind === "recorded" ? [[node.nodeId.text, nodeResultOf(node.result)] as const] : [],
          ),
        ),
      );
    case "workflowAbsent":
      return err(workflowNotFound(facts.workflowId));
    case "failed":
      return err(handleErrorOfRaw(read.error));
    default:
      return unreachable(read, "nodeResults");
  }
}

async function nodeInfos(facts: HandleFacts): Promise<Result<readonly WorkflowTaskInfo[], HandleCoreError>> {
  const read = await facts.core.nodes(facts.workflowId);

  switch (read.kind) {
    case "nodes": {
      const infos: WorkflowTaskInfo[] = [];

      for (const node of read.nodes) {
        const info = nodeInfoOf(facts.workflowId, node);

        if (!info.ok) {
          return info;
        }

        infos.push(info.value);
      }

      return ok(Object.freeze(infos));
    }

    case "workflowAbsent":
      return err(workflowNotFound(facts.workflowId));
    case "failed":
      return err(handleErrorOfRaw(read.error));
    default:
      return unreachable(read, "nodeInfos");
  }
}

const NOT_RECORDED: Recorded<never> = Object.freeze({ kind: "notRecorded" });

function recorded<T>(value: T): Recorded<T> {
  return Object.freeze({ kind: "recorded", value });
}

const NODE_STATUSES: ReadonlySet<string> = new Set(Object.values(WorkflowTaskStatus));

function isNodeStatus(status: string): status is WorkflowTaskStatus {
  return NODE_STATUSES.has(status);
}

/** One node, or `STORED_DATA_INVALID` for a stored status or summary that does not read. Pure. */
function nodeInfoOf(workflowId: string, node: StoredNodeRecord): Result<WorkflowTaskInfo, HandleCoreError> {
  if (!isNodeStatus(node.status)) {
    return err(
      storedDataInvalid(
        workflowId,
        `node ${node.index} has the stored status "${node.status}", which is not a node status`,
      ),
    );
  }

  const summary = summaryOf(node);

  if (!summary.ok) {
    return err(
      storedDataInvalid(
        workflowId,
        `node ${node.index} has a stored sub-workflow summary that does not read: ${summary.error}`,
      ),
    );
  }

  return ok(
    Object.freeze({
      nodeId: node.nodeId.kind === "recorded" ? recorded(node.nodeId.text) : NOT_RECORDED,
      index: node.index,
      name: node.name,
      status: node.status,
      result: node.result.kind === "none" ? NOT_RECORDED : recorded(nodeResultOf(node.result)),
      startedAt:
        node.startedAt.kind === "recorded" ? recorded(instantOfMicros(node.startedAt.epochMicros)) : NOT_RECORDED,
      completedAt:
        node.completedAt.kind === "recorded" ? recorded(instantOfMicros(node.completedAt.epochMicros)) : NOT_RECORDED,
      subWorkflowId: node.subWorkflowId.kind === "recorded" ? recorded(node.subWorkflowId.text) : NOT_RECORDED,
      subWorkflowSummary: summary.value,
    }),
  );
}

/** The summary of a sub-workflow node, read with its child id; none when the node stores none. Pure. */
function summaryOf(node: StoredNodeRecord): Result<Recorded<SubWorkflowSummary>, string> {
  switch (node.subWorkflowSummary.kind) {
    case "none":
      return ok(NOT_RECORDED);
    case "refused":
      return err(node.subWorkflowSummary.cause);
    case "checked": {
      const idNode = node.subWorkflowId.kind === "recorded" ? node.subWorkflowId.text : undefined;
      const read = readSubWorkflow(idNode, readTree(jsonTextCheckedByCore(node.subWorkflowSummary.text)), "$");

      return read.ok ? ok(recorded(read.value.summary)) : err(refusalText(read.error));
    }

    default:
      return unreachable(node.subWorkflowSummary, "summaryOf");
  }
}
