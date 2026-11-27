// A fake of the core operations of workflow starts and handles, and raw records of the workflow start and handle
// catalogs, for the tests of the start, the handle and the seat operations.

import type { ProcessRoleName, WaitBound } from "../../../horsies/src/native/sends/addon.ts";
import type { RawCatalogError } from "../../../horsies/src/native/values/records.ts";
import type {
  HandleChange,
  HandleDone,
  NodeResultRead,
  NodeResultsRead,
  NodesRead,
  WorkflowStartOutcome,
  WorkflowStatusRead,
  WorkflowWaitRead,
} from "../../../horsies/src/native/workflows/addon.ts";
import type { AddonWorkflowCore, SpecRefusal, StartRequest } from "../../../horsies/src/workflow/core.ts";
import type { CheckedSpec } from "../../../horsies/src/workflow/findings.ts";

/** A canonical workflow id of the fakes. */
export const WORKFLOW_ID = "0190a5b2-7c3d-7e4f-8a9b-0c1d2e3f4a5b";

/** The spec data text that the fake core starts. */
export const SPEC_DATA = '{"data_version":1}';

/** The scripted answers of a fake core. Each answer left out is a quiet default. */
export type CoreScript = {
  readonly started?: readonly WorkflowStartOutcome[];
  /** The answer of each prepare: the writer or the core's parse refused the spec, so no start follows. */
  readonly refusedPrepare?: SpecRefusal;
  readonly status?: WorkflowStatusRead;
  readonly nodeResults?: NodeResultsRead;
  readonly nodes?: NodesRead;
  readonly cancel?: HandleDone;
  readonly change?: HandleChange;
  readonly wait?: WorkflowWaitRead;
  readonly nodeResult?: NodeResultRead;
  readonly role?: ProcessRoleName;
};

/** A fake core with its recorded calls. */
export type FakeCore = AddonWorkflowCore & {
  /** Each spec that a start prepared. */
  readonly prepares: CheckedSpec[];
  /** Each write of a spec data text: the starts and the retries, in order. */
  readonly starts: StartRequest[];
  readonly calls: string[];
  readonly waits: WaitBound[];
};

/** A fake of the core operations: each call is recorded by name; a start answers the scripted outcomes in order. */
export function fakeCore(script: CoreScript = {}): FakeCore {
  const prepares: CheckedSpec[] = [];
  const starts: StartRequest[] = [];
  const calls: string[] = [];
  const waits: WaitBound[] = [];

  const outcome = (index: number): WorkflowStartOutcome =>
    script.started?.[Math.min(index, (script.started?.length ?? 1) - 1)] ?? { kind: "started" };

  const record = <T>(name: string, value: T): T => {
    calls.push(name);

    return value;
  };

  return {
    prepares,
    starts,
    calls,
    waits,
    prepare: async (spec) => {
      prepares.push(spec);

      return script.refusedPrepare ?? { kind: "prepared", specData: SPEC_DATA };
    },
    start: async (request) => {
      starts.push(request);

      return { kind: "outcome", specData: request.specData, outcome: outcome(starts.length - 1) };
    },
    status: async () => record("status", script.status ?? { kind: "status", status: "RUNNING" }),
    nodeResults: async () => record("nodeResults", script.nodeResults ?? { kind: "nodes", nodes: [] }),
    nodes: async () => record("nodes", script.nodes ?? { kind: "nodes", nodes: [] }),
    cancel: async () => record("cancel", script.cancel ?? { kind: "done" }),
    pause: async () => record("pause", script.change ?? { kind: "changed", changed: true }),
    resume: async () => record("resume", script.change ?? { kind: "changed", changed: true }),
    waitEnd: async (_workflowId, wait) => {
      waits.push(wait);

      return record("waitEnd", script.wait ?? { kind: "timedOut" });
    },
    nodeResult: async () => record("nodeResult", script.nodeResult ?? { kind: "notReady" }),
    processRole: async () => record("processRole", script.role ?? "RUNS_NO_TASK_BODIES"),
  };
}

/** The slots shared by the start classes. */
function startSlots(workflowName: string): RawCatalogError["slots"] {
  return [
    { name: "message", value: { kind: "text", text: `start of ${workflowName}` } },
    { name: "workflow_name", value: { kind: "text", text: workflowName } },
    { name: "workflow_id", value: { kind: "uuid", text: WORKFLOW_ID } },
  ];
}

/** A raw `WorkflowStartEnqueueFailed`. */
export function rawStartEnqueueFailed(workflowName: string, retryable: boolean): RawCatalogError {
  return {
    class: "WorkflowStartEnqueueFailed",
    slots: [
      ...startSlots(workflowName),
      { name: "retryable", value: { kind: "flag", value: retryable } },
      {
        name: "cause",
        value: {
          kind: "exception",
          exception: {
            kind: "coreFailure",
            cause: { kind: "connection", subject: "start", message: "connection refused", retryable },
          },
        },
      },
    ],
  };
}

/** A raw `WorkflowStartIdConflict`. */
export function rawIdConflict(workflowName: string, existing: string): RawCatalogError {
  return {
    class: "WorkflowStartIdConflict",
    slots: [...startSlots(workflowName), { name: "existing_workflow_name", value: { kind: "text", text: existing } }],
  };
}

/** A raw `WorkflowStartValidationFailed` of the core, with no cause. */
export function rawValidationFailed(workflowName: string): RawCatalogError {
  return {
    class: "WorkflowStartValidationFailed",
    slots: [
      ...startSlots(workflowName),
      { name: "cause", value: { kind: "exception", exception: { kind: "noCause" } } },
    ],
  };
}

/** A raw `WorkflowStartCommandConflict`. */
export function rawCommandConflict(workflowName: string): RawCatalogError {
  return { class: "WorkflowStartCommandConflict", slots: startSlots(workflowName) };
}

/** A raw `WorkflowHandleDatabaseOperationFailed`. */
export function rawDbFailed(): RawCatalogError {
  return {
    class: "WorkflowHandleDatabaseOperationFailed",
    slots: [
      { name: "message", value: { kind: "text", text: "connection refused" } },
      { name: "retryable", value: { kind: "flag", value: true } },
      { name: "workflow_id", value: { kind: "uuid", text: WORKFLOW_ID } },
      {
        name: "cause",
        value: {
          kind: "exception",
          exception: {
            kind: "coreFailure",
            cause: { kind: "connection", subject: "status", message: "connection refused", retryable: true },
          },
        },
      },
    ],
  };
}

/** A stored result envelope of a success with `okText`. */
export function okEnvelope(okText: string): string {
  return `{"__h_task_result__":true,"ok":${okText},"err":null}`;
}

/** A stored result envelope of a failure with the custom code `code`. */
export function customErrorEnvelope(code: string): string {
  return `{"__h_task_result__":true,"ok":null,"err":${customErrorText(code)}}`;
}

/** A stored error object with the custom code `code`. */
export function customErrorText(code: string): string {
  return `{"exception":null,"error_code":"${code}","data":null,"message":"failed"}`;
}
