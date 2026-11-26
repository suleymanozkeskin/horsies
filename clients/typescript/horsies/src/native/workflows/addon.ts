// The functions of the `workflows` area of the addon, their records, and the accessor.
//
// - `prepareClientSpec` is pure: the core's parse of a spec data text. It starts no core.
// - `registerChildSpecSource` sets the child spec source of a core handle, once, with the definition keys of the
//   workflows that the app lists. The core calls the source inside a workflow step; a node whose key is not listed
//   fails with `SUBWORKFLOW_LOAD_FAILED` before its input is read; else the source calls the package's build function
//   on the JavaScript thread and waits for its answer up to the core's `LOOP_BLOCK_LIMIT`.
// - `startWorkflow` and the handle operations run on a core handle: they start the core of the process at the first
//   call and run on its runtime.
// - Every stored text that a read gives passed the core's strict JSON check (`check_json_text`) in the addon; a text
//   that the check refuses crosses as `refused` with the cause, never as text.

import { requireAddon } from "../call/addon.ts";
import type { NativeEnvelope } from "../call/envelope.ts";
import type { NativeCoreHandle } from "../handle/addon.ts";
import type {
  NativeSendRules,
  NativeTaskOptions,
  RecordedInstant,
  RecordedText,
  SendPriority,
  WaitBound,
} from "../sends/addon.ts";
import type { RawCatalogError, TraceArgument } from "../values/records.ts";

/** A spec that the core read from spec data (the core's `ClientSpec`). Only the core reads it. */
export interface NativeClientSpec {
  /** The workflow name of the spec. */
  workflowName(): NativeEnvelope<string>;
}

/**
 * The outcome of the core's parse of a spec data text: the spec, or the refusal with the field path of the text
 * (`nodes[2].join.min_success`) and the cause.
 */
export type ClientSpecPrepare =
  | { readonly kind: "prepared"; readonly spec: NativeClientSpec }
  | { readonly kind: "refused"; readonly fieldPath: string; readonly cause: string };

/** Why a start returns the run that holds its id: the same start command, or a run with no stored command. */
export type ExistingRunName = "SAME_COMMAND" | "FINGERPRINT_NOT_STORED";

/**
 * The outcome of one workflow start:
 * - `started`: the tree was written;
 * - `exists`: a run with this id exists and the start returns it; nothing was written;
 * - `failed`: a class of the workflow start catalog (`WorkflowStartIdConflict`, `WorkflowStartCommandConflict`,
 *   `WorkflowStartValidationFailed`, `WorkflowStartEnqueueFailed`).
 */
export type WorkflowStartOutcome =
  | { readonly kind: "started" }
  | { readonly kind: "exists"; readonly existing: ExistingRunName }
  | { readonly kind: "failed"; readonly error: RawCatalogError };

/** The facts of one child build that the core asks for. `inputText` passed the core's strict JSON check. */
export interface ChildSpecCall {
  /** The definition key of the sub-workflow node. */
  readonly definitionKey: string;
  /** The node's params with its `args_from` envelopes merged (the core's `merged_child_input`). */
  readonly inputText: string;
}

/** The code of a build refusal: a catalog code (`HRS-nnn`), or none (a throw of the user's build). */
export type BuildRefusalCode = { readonly kind: "catalog"; readonly code: string } | { readonly kind: "noCode" };

/**
 * The answer of the package's build function to one `ChildSpecCall`. The function never throws; a throw is a defect
 * of the package.
 * - `built`: the spec data text of the child;
 * - `notListed`: the app lists no workflow with the definition key (the node stores `SUBWORKFLOW_LOAD_FAILED`);
 * - `memberRefused`: a member of the input does not decode, or no param takes it (`WORKER_SERIALIZATION_ERROR`);
 * - `paramsUndecodable`: the input does not decode as the params as a whole (`WORKFLOW_ENQUEUE_FAILED`);
 * - `buildFailed`: the build threw, returned a thenable, or its spec failed a check (`WORKFLOW_ENQUEUE_FAILED`).
 */
export type ChildSpecAnswer =
  | { readonly kind: "built"; readonly specData: string }
  | { readonly kind: "notListed" }
  | { readonly kind: "memberRefused"; readonly key: string; readonly cause: string }
  | { readonly kind: "paramsUndecodable"; readonly cause: string }
  | { readonly kind: "buildFailed"; readonly code: BuildRefusalCode; readonly message: string };

/** The package's wrapper of the build function, in the settle form of the callback primitive. */
export type ChildSpecSourceFunction = (callId: bigint, call: ChildSpecCall) => void;

/** How a registration ended: `registered`, or `alreadySet` when the handle holds a source. */
export type SourceRegistration = "registered" | "alreadySet";

/** The status of a workflow, as the core names it. */
export type WorkflowStatusName = "PENDING" | "RUNNING" | "PAUSED" | "COMPLETED" | "FAILED" | "CANCELLED" | "EXPIRED";

/** The status of one node of a workflow, as the core names it. */
export type NodeStatusName = "PENDING" | "READY" | "ENQUEUED" | "RUNNING" | "COMPLETED" | "FAILED" | "SKIPPED";

/** The status of a workflow that ended with an error. */
export type ErrorEndStatusName = "FAILED" | "CANCELLED" | "EXPIRED";

/** A stored text after the core's strict JSON check: the text, or the cause of the refusal. */
export type CheckedText =
  | { readonly kind: "checked"; readonly text: string }
  | { readonly kind: "refused"; readonly cause: string };

/** A stored text that a row may not hold (`NULL`), after the strict check when it holds one. */
export type StoredTextFact = { readonly kind: "none" } | CheckedText;

/** The status read of a workflow. `failed` holds a class of the handle catalog. */
export type WorkflowStatusRead =
  | { readonly kind: "status"; readonly status: WorkflowStatusName }
  | { readonly kind: "failed"; readonly error: RawCatalogError };

/** One stored node result: the node id (none for a node stored without one) and the result envelope. */
export interface StoredNodeResultRecord {
  readonly nodeId: RecordedText;
  readonly result: CheckedText;
}

/** The stored results of the nodes that have one, in index order. */
export type NodeResultsRead =
  | { readonly kind: "nodes"; readonly nodes: StoredNodeResultRecord[] }
  | { readonly kind: "workflowAbsent" }
  | { readonly kind: "failed"; readonly error: RawCatalogError };

/** One stored node of a workflow. `status` is the stored text: the package maps it to `NodeStatusName`. */
export interface StoredNodeRecord {
  readonly nodeId: RecordedText;
  readonly index: number;
  /** The task name, or the child's workflow name. */
  readonly name: string;
  readonly status: string;
  readonly result: StoredTextFact;
  readonly startedAt: RecordedInstant;
  readonly completedAt: RecordedInstant;
  /** The child workflow id of a sub-workflow node. */
  readonly subWorkflowId: RecordedText;
  /** The summary of the ended child of a sub-workflow node. */
  readonly subWorkflowSummary: StoredTextFact;
}

/** Every node of a workflow, in index order. */
export type NodesRead =
  | { readonly kind: "nodes"; readonly nodes: StoredNodeRecord[] }
  | { readonly kind: "workflowAbsent" }
  | { readonly kind: "failed"; readonly error: RawCatalogError };

/** The outcome of a handle operation with no value: done, or a class of the handle catalog. */
export type HandleDone = { readonly kind: "done" } | { readonly kind: "failed"; readonly error: RawCatalogError };

/** The outcome of `pause` and `resume`: whether the status changed, or a class of the handle catalog. */
export type HandleChange =
  | { readonly kind: "changed"; readonly changed: boolean }
  | { readonly kind: "failed"; readonly error: RawCatalogError };

/**
 * What a wait for the end of a workflow found:
 * - `completed`: the stored result envelope;
 * - `endedWithError`: FAILED, CANCELLED or EXPIRED, with the stored error object;
 * - `paused`: the workflow waits for a resume;
 * - `timedOut`: the workflow is PENDING or RUNNING after the wait;
 * - `notFound`: no workflow has the id;
 * - `failed`: a class of the handle catalog.
 */
export type WorkflowWaitRead =
  | { readonly kind: "completed"; readonly result: StoredTextFact }
  | { readonly kind: "endedWithError"; readonly status: ErrorEndStatusName; readonly error: StoredTextFact }
  | { readonly kind: "paused" }
  | { readonly kind: "timedOut" }
  | { readonly kind: "notFound" }
  | { readonly kind: "failed"; readonly error: RawCatalogError };

/** The stored result of one node by its id: the envelope, or none yet (or no node of the id). Not a wait. */
export type NodeResultRead =
  | { readonly kind: "stored"; readonly result: CheckedText }
  | { readonly kind: "notReady" }
  | { readonly kind: "failed"; readonly error: RawCatalogError };

/** The functions of the `workflows` area. */
export interface WorkflowsAddon {
  /**
   * The retention class key of each queue, in the order of `queueNames`: the queue mapping of the retention config of
   * `rules`, else the default class (the core's `resolve_queue_retention(queue).class_key()`). Pure.
   */
  queueRetentionClasses(rules: NativeSendRules, queueNames: string[]): NativeEnvelope<string[]>;
  /**
   * The priority of each queue, in the order of `queueNames`: the one queue of the default queue mode (the package
   * holds its priority), or a custom queue with its priority in the accepted config. Pure.
   */
  queuePriorities(rules: NativeSendRules, queueNames: string[]): NativeEnvelope<SendPriority[]>;
  /**
   * The stored options text of a node of the task with the checked `options` and the deadline `epochMicros` (the
   * core's `stored_send_options_text`). An instant outside the core's range is a `CLIENT_DEFECT`. Pure.
   */
  nodeTaskOptionsText(options: NativeTaskOptions, epochMicros: bigint): NativeEnvelope<string>;
  prepareClientSpec(specDataText: string): NativeEnvelope<ClientSpecPrepare>;
  registerChildSpecSource(
    handle: NativeCoreHandle,
    source: ChildSpecSourceFunction,
    definitionKeys: string[],
  ): NativeEnvelope<SourceRegistration>;
  startWorkflow(
    handle: NativeCoreHandle,
    spec: NativeClientSpec,
    workflowId: string,
    sentAtMicros: bigint,
    trace: TraceArgument,
    resendOnTransientErr: boolean,
  ): Promise<NativeEnvelope<WorkflowStartOutcome>>;
  workflowStatus(handle: NativeCoreHandle, workflowId: string): Promise<NativeEnvelope<WorkflowStatusRead>>;
  workflowNodeResults(handle: NativeCoreHandle, workflowId: string): Promise<NativeEnvelope<NodeResultsRead>>;
  workflowNodes(handle: NativeCoreHandle, workflowId: string): Promise<NativeEnvelope<NodesRead>>;
  cancelWorkflow(handle: NativeCoreHandle, workflowId: string): Promise<NativeEnvelope<HandleDone>>;
  pauseWorkflow(handle: NativeCoreHandle, workflowId: string): Promise<NativeEnvelope<HandleChange>>;
  resumeWorkflow(handle: NativeCoreHandle, workflowId: string): Promise<NativeEnvelope<HandleChange>>;
  waitWorkflowEnd(
    handle: NativeCoreHandle,
    workflowId: string,
    wait: WaitBound,
  ): Promise<NativeEnvelope<WorkflowWaitRead>>;
  nodeResult(handle: NativeCoreHandle, workflowId: string, nodeId: string): Promise<NativeEnvelope<NodeResultRead>>;
}

/** The `workflows` functions of the addon. Throws as `requireAddon` does. */
export function workflowsAddon(): WorkflowsAddon {
  // SAFETY: the addon registers these functions with these signatures; the declaration test of
  // this area compares this interface with the declarations that the native build writes.
  return requireAddon() as WorkflowsAddon;
}
