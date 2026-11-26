// The generated declarations of the `workflows` area match the hand-kept ones, function by function and
// method by method (arguments and result), and type by type.

import type * as Generated from "../../bridge/index.js";
import type {
  BuildRefusalCode,
  CheckedText,
  ChildSpecAnswer,
  ChildSpecCall,
  ClientSpecPrepare,
  ErrorEndStatusName,
  ExistingRunName,
  HandleChange,
  HandleDone,
  NativeClientSpec,
  NodeResultRead,
  NodeResultsRead,
  NodesRead,
  SourceRegistration,
  StoredNodeRecord,
  StoredNodeResultRecord,
  StoredTextFact,
  WorkflowStartOutcome,
  WorkflowStatusName,
  WorkflowStatusRead,
  WorkflowWaitRead,
  WorkflowsAddon,
} from "../../horsies/src/native/workflows/addon.ts";
import type { WorkflowsChildProducerProbeAddon } from "../runtime/native/workflows-child-producer-probe.ts";
import type { Mutual, SameFunction } from "./mutual.ts";

export const queueRetentionClassesMatches: SameFunction<
  typeof Generated.queueRetentionClasses,
  WorkflowsAddon["queueRetentionClasses"]
> = true;

export const nodeTaskOptionsTextMatches: SameFunction<
  typeof Generated.nodeTaskOptionsText,
  WorkflowsAddon["nodeTaskOptionsText"]
> = true;

export const prepareClientSpecMatches: SameFunction<
  typeof Generated.prepareClientSpec,
  WorkflowsAddon["prepareClientSpec"]
> = true;

export const registerChildSpecSourceMatches: SameFunction<
  typeof Generated.registerChildSpecSource,
  WorkflowsAddon["registerChildSpecSource"]
> = true;

export const startWorkflowMatches: SameFunction<typeof Generated.startWorkflow, WorkflowsAddon["startWorkflow"]> =
  true;

export const workflowStatusMatches: SameFunction<
  typeof Generated.workflowStatus,
  WorkflowsAddon["workflowStatus"]
> = true;

export const workflowNodeResultsMatches: SameFunction<
  typeof Generated.workflowNodeResults,
  WorkflowsAddon["workflowNodeResults"]
> = true;

export const workflowNodesMatches: SameFunction<typeof Generated.workflowNodes, WorkflowsAddon["workflowNodes"]> =
  true;

export const cancelWorkflowMatches: SameFunction<typeof Generated.cancelWorkflow, WorkflowsAddon["cancelWorkflow"]> =
  true;

export const pauseWorkflowMatches: SameFunction<typeof Generated.pauseWorkflow, WorkflowsAddon["pauseWorkflow"]> =
  true;

export const resumeWorkflowMatches: SameFunction<typeof Generated.resumeWorkflow, WorkflowsAddon["resumeWorkflow"]> =
  true;

export const waitWorkflowEndMatches: SameFunction<
  typeof Generated.waitWorkflowEnd,
  WorkflowsAddon["waitWorkflowEnd"]
> = true;

export const queuePrioritiesMatches: SameFunction<typeof Generated.queuePriorities, WorkflowsAddon["queuePriorities"]> =
  true;

export const nodeResultMatches: SameFunction<typeof Generated.nodeResult, WorkflowsAddon["nodeResult"]> = true;

// The class, method by method, and its member set: no method on one side only.
export const workflowNameMatches: SameFunction<
  Generated.NativeClientSpec["workflowName"],
  NativeClientSpec["workflowName"]
> = true;

export const clientSpecMatches: Mutual<Generated.NativeClientSpec, NativeClientSpec> = true;

export const prepareMatches: Mutual<Generated.ClientSpecPrepare, ClientSpecPrepare> = true;
export const existingRunMatches: Mutual<Generated.ExistingRunName, ExistingRunName> = true;
export const startOutcomeMatches: Mutual<Generated.WorkflowStartOutcome, WorkflowStartOutcome> = true;
export const childSpecCallMatches: Mutual<Generated.ChildSpecCall, ChildSpecCall> = true;
export const buildRefusalCodeMatches: Mutual<Generated.BuildRefusalCode, BuildRefusalCode> = true;
export const childSpecAnswerMatches: Mutual<Generated.ChildSpecAnswer, ChildSpecAnswer> = true;
export const registrationMatches: Mutual<Generated.SourceRegistration, SourceRegistration> = true;
export const workflowStatusNameMatches: Mutual<Generated.WorkflowStatusName, WorkflowStatusName> = true;
export const errorEndStatusMatches: Mutual<Generated.ErrorEndStatusName, ErrorEndStatusName> = true;
export const checkedTextMatches: Mutual<Generated.CheckedText, CheckedText> = true;
export const storedTextMatches: Mutual<Generated.StoredTextFact, StoredTextFact> = true;
export const statusReadMatches: Mutual<Generated.WorkflowStatusRead, WorkflowStatusRead> = true;
export const nodeResultRecordMatches: Mutual<Generated.StoredNodeResultRecord, StoredNodeResultRecord> = true;
export const nodeResultsReadMatches: Mutual<Generated.NodeResultsRead, NodeResultsRead> = true;
export const nodeRecordMatches: Mutual<Generated.StoredNodeRecord, StoredNodeRecord> = true;
export const nodesReadMatches: Mutual<Generated.NodesRead, NodesRead> = true;
export const handleDoneMatches: Mutual<Generated.HandleDone, HandleDone> = true;
export const handleChangeMatches: Mutual<Generated.HandleChange, HandleChange> = true;
export const waitReadMatches: Mutual<Generated.WorkflowWaitRead, WorkflowWaitRead> = true;
export const nodeResultReadMatches: Mutual<Generated.NodeResultRead, NodeResultRead> = true;
// The test exports of a build with the cargo feature `test-probes` (`build:dev`).
export const probeStartWorkflowInChildMatches: SameFunction<
  typeof Generated.probeStartWorkflowInChild,
  WorkflowsChildProducerProbeAddon["probeStartWorkflowInChild"]
> = true;

export const probeWorkflowStatusInChildMatches: SameFunction<
  typeof Generated.probeWorkflowStatusInChild,
  WorkflowsChildProducerProbeAddon["probeWorkflowStatusInChild"]
> = true;
