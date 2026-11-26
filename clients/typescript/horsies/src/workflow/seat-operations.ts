// The seat operations of the workflows area: each workflow operation that a seat isolate sends to its child's main
// thread, and its reply. The area adds its entry to `SeatOperationMap` here (declaration merging); the seats package
// serves each one on the main thread with `serveWorkflowsSeatOperation`.

import type {
  HandleChange,
  HandleDone,
  NodeResultRead,
  NodeResultsRead,
  NodesRead,
  OutputNodeFact,
  WorkflowStatusRead,
} from "../native/workflows/addon.ts";
import type { RestartRequest, StartCall, StartRequest } from "./core.ts";

/** One workflow operation that a seat isolate asks its child's main thread for. */
export type WorkflowsSeatOperation =
  | { readonly kind: "start"; readonly request: StartRequest }
  | { readonly kind: "restart"; readonly request: RestartRequest }
  | { readonly kind: "status"; readonly workflowId: string }
  | { readonly kind: "nodeResults"; readonly workflowId: string }
  | { readonly kind: "nodes"; readonly workflowId: string }
  | { readonly kind: "cancel"; readonly workflowId: string }
  | { readonly kind: "pause"; readonly workflowId: string }
  | { readonly kind: "resume"; readonly workflowId: string }
  | { readonly kind: "nodeResult"; readonly workflowId: string; readonly nodeId: string }
  | { readonly kind: "outputNode"; readonly workflowId: string };

/** The answer to a `WorkflowsSeatOperation`, of the same `kind`. */
export type WorkflowsSeatReply =
  | { readonly kind: "start"; readonly call: StartCall }
  | { readonly kind: "restart"; readonly call: StartCall }
  | { readonly kind: "status"; readonly read: WorkflowStatusRead }
  | { readonly kind: "nodeResults"; readonly read: NodeResultsRead }
  | { readonly kind: "nodes"; readonly read: NodesRead }
  | { readonly kind: "cancel"; readonly done: HandleDone }
  | { readonly kind: "pause"; readonly change: HandleChange }
  | { readonly kind: "resume"; readonly change: HandleChange }
  | { readonly kind: "nodeResult"; readonly read: NodeResultRead }
  | { readonly kind: "outputNode"; readonly read: OutputNodeFact };

declare module "../app/core-access.ts" {
  interface SeatOperationMap {
    readonly workflows: { readonly operation: WorkflowsSeatOperation; readonly reply: WorkflowsSeatReply };
  }
}
