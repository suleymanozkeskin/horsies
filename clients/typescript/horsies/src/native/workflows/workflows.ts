// The functions of the `workflows` area of the addon through the call wrapper: a throw or a rejection of a call is a
// defect under the defect rule, and a thrown envelope is its error.
//
// The caller checks each value first: workflow ids in their lower-case hyphenated form, deadlines in Python's
// `datetime` range. A value that breaks such a rule is a `ClientDefect` of the addon.

import { nativeValue, nativeValueAsync } from "../call/envelope.ts";
import type { CoreHandle } from "../handle/core-handle.ts";
import type { NativeSendRules, NativeTaskOptions, SendPriority, WaitBound } from "../sends/addon.ts";
import type { TraceArgument } from "../values/records.ts";
import {
  type ChildSpecSourceFunction,
  type ClientSpecPrepare,
  type HandleChange,
  type HandleDone,
  type NativeClientSpec,
  type NodeResultRead,
  type NodeResultsRead,
  type NodesRead,
  type SourceRegistration,
  type WorkflowStartOutcome,
  type WorkflowStatusRead,
  type WorkflowWaitRead,
  workflowsAddon,
} from "./addon.ts";

/** The retention class key of each queue of `queueNames`, in order. Pure. */
export function queueRetentionClasses(rules: NativeSendRules, queueNames: readonly string[]): readonly string[] {
  return nativeValue("queueRetentionClasses", () => workflowsAddon().queueRetentionClasses(rules, [...queueNames]));
}

/** The priority of each queue of `queueNames`, in order. Pure. */
export function queuePriorities(rules: NativeSendRules, queueNames: readonly string[]): readonly SendPriority[] {
  return nativeValue("queuePriorities", () => workflowsAddon().queuePriorities(rules, [...queueNames]));
}

/** The stored options text of a node of a task with the checked `options` and a deadline. Pure. */
export function nodeTaskOptionsText(options: NativeTaskOptions, epochMicros: bigint): string {
  return nativeValue("nodeTaskOptionsText", () => workflowsAddon().nodeTaskOptionsText(options, epochMicros));
}

/** The core's parse of a spec data text: the spec, or the refusal with its field path. Pure. */
export function prepareClientSpec(specDataText: string): ClientSpecPrepare {
  return nativeValue("prepareClientSpec", () => workflowsAddon().prepareClientSpec(specDataText));
}

/** Set the child spec source of `handle`, once. On the JavaScript thread; starts nothing. */
export function registerChildSpecSource(handle: CoreHandle, source: ChildSpecSourceFunction): SourceRegistration {
  return nativeValue("registerChildSpecSource", () => workflowsAddon().registerChildSpecSource(handle.native, source));
}

/**
 * One workflow start on `handle` with its child spec source. Starts the core of the process at the first call.
 * Writes: when a stop of the core ends it early, the thrown `CoreStopped` has the state `outcomeUnknown`.
 */
export async function startWorkflow(
  handle: CoreHandle,
  spec: NativeClientSpec,
  workflowId: string,
  sentAtMicros: bigint,
  trace: TraceArgument,
  resendOnTransientErr: boolean,
): Promise<WorkflowStartOutcome> {
  return nativeValueAsync("startWorkflow", () =>
    workflowsAddon().startWorkflow(handle.native, spec, workflowId, sentAtMicros, trace, resendOnTransientErr),
  );
}

/** The status of a workflow. Changes nothing. */
export async function workflowStatus(handle: CoreHandle, workflowId: string): Promise<WorkflowStatusRead> {
  return nativeValueAsync("workflowStatus", () => workflowsAddon().workflowStatus(handle.native, workflowId));
}

/** The stored results of the nodes that have one. Changes nothing. */
export async function workflowNodeResults(handle: CoreHandle, workflowId: string): Promise<NodeResultsRead> {
  return nativeValueAsync("workflowNodeResults", () => workflowsAddon().workflowNodeResults(handle.native, workflowId));
}

/** Every node of a workflow. Changes nothing. */
export async function workflowNodes(handle: CoreHandle, workflowId: string): Promise<NodesRead> {
  return nativeValueAsync("workflowNodes", () => workflowsAddon().workflowNodes(handle.native, workflowId));
}

/** Cancel a workflow and its descendants. */
export async function cancelWorkflow(handle: CoreHandle, workflowId: string): Promise<HandleDone> {
  return nativeValueAsync("cancelWorkflow", () => workflowsAddon().cancelWorkflow(handle.native, workflowId));
}

/** Pause a RUNNING workflow. */
export async function pauseWorkflow(handle: CoreHandle, workflowId: string): Promise<HandleChange> {
  return nativeValueAsync("pauseWorkflow", () => workflowsAddon().pauseWorkflow(handle.native, workflowId));
}

/** Resume a PAUSED workflow, with the handle's child spec source. */
export async function resumeWorkflow(handle: CoreHandle, workflowId: string): Promise<HandleChange> {
  return nativeValueAsync("resumeWorkflow", () => workflowsAddon().resumeWorkflow(handle.native, workflowId));
}

/** Wait for the end of a workflow, up to `wait`. Changes nothing. */
export async function waitWorkflowEnd(
  handle: CoreHandle,
  workflowId: string,
  wait: WaitBound,
): Promise<WorkflowWaitRead> {
  return nativeValueAsync("waitWorkflowEnd", () => workflowsAddon().waitWorkflowEnd(handle.native, workflowId, wait));
}

/** The stored result of the node `nodeId` of a workflow. Not a wait; changes nothing. */
export async function nodeResult(handle: CoreHandle, workflowId: string, nodeId: string): Promise<NodeResultRead> {
  return nativeValueAsync("nodeResult", () => workflowsAddon().nodeResult(handle.native, workflowId, nodeId));
}
