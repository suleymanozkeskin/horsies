// The facade calls of a seat isolate: the monitoring reads, the health reads, the database ping, the cancel and the
// rerun in the app's transaction. A seat isolate never loads the addon, so each call of a facade there goes through the
// seat relay to the main thread of its child, which runs the same addon function with its own core handle.
//
// - The operation is plain data: the part of the addon, the function, and its arguments after the core handle. The
//   reply is the envelope that the function resolved with. The facade code of the isolate reads it as it reads the
//   addon's envelope on a main thread, so each call keeps its own `Result`.
// - The relayed addons ignore the handle that the facade code passes: the main thread uses its own. The relayed core
//   handle relays `pingDatabase`; its other members are calls that a facade of a seat body never makes, and each answers
//   with a thrown `CLIENT_DEFECT` envelope.
// - Past the bound of the relay, each call gets the retryable database failure of its own type, with a cause that names
//   the bound (`facadeBoundAnswer`).
// - `rerunIn` takes the caller's transaction object, which cannot cross a thread: it is not relayed.

import type { SeatRelay } from "../app/core-access.ts";
import { RerunErrorCode } from "../_catalog/history.ts";
import type { NativeEnvelope } from "../native/call/envelope.ts";
import type { CancelAddon } from "../native/facades/cancel.ts";
import type { HealthAddon } from "../native/facades/health.ts";
import type { MonitoringReadsAddon } from "../native/facades/monitoring-reads.ts";
import type { RerunAddon } from "../native/facades/rerun.ts";
import type { NativeCoreHandle } from "../native/handle/addon.ts";
import type { ExceptionRecord, RawCatalogError } from "../native/values/records.ts";
import { unreachable } from "../wire/unreachable.ts";

/** The functions of an addon part that take the core handle first and give a Promise, with their arguments after it. */
type HandleCall<A> = {
  readonly [M in keyof A & string]: A[M] extends (handle: NativeCoreHandle, ...args: infer P) => Promise<unknown>
    ? { readonly method: M; readonly args: P }
    : never;
}[keyof A & string];

/** The envelopes that the functions of an addon part resolve with. */
type HandleAnswer<A> = {
  readonly [M in keyof A]: A[M] extends (...args: never[]) => Promise<infer R> ? R : never;
}[keyof A];

/** The envelope of the database ping of a core handle. */
type DatabasePingAnswer = Awaited<ReturnType<NativeCoreHandle["pingDatabase"]>>;

/** One facade call of a seat isolate. */
export type FacadesSeatOperation =
  | { readonly part: "reads"; readonly call: HandleCall<MonitoringReadsAddon> }
  | { readonly part: "health"; readonly call: HandleCall<HealthAddon> }
  | { readonly part: "cancel"; readonly call: HandleCall<CancelAddon> }
  | { readonly part: "rerun"; readonly call: HandleCall<RerunAddon> }
  | { readonly part: "database"; readonly call: { readonly method: "pingDatabase"; readonly args: [] } };

/** The reply to a facade call: the envelope that the addon function resolved with on the main thread. */
export type FacadesSeatReply = {
  readonly envelope:
    | HandleAnswer<MonitoringReadsAddon>
    | HandleAnswer<HealthAddon>
    | HandleAnswer<CancelAddon>
    | HandleAnswer<RerunAddon>
    | DatabasePingAnswer;
};

/** The facade addons of a seat isolate, on its relay. */
export interface RelayedFacades {
  readonly reads: MonitoringReadsAddon;
  readonly health: HealthAddon;
  readonly cancel: CancelAddon;
  readonly rerun: Pick<RerunAddon, "rerunTask">;
  readonly handle: NativeCoreHandle;
}

/** Send `operation` through `relay`, and give the envelope of the reply. */
async function relayed<T>(relay: SeatRelay, operation: FacadesSeatOperation): Promise<T> {
  const reply = await relay.request("facades", operation);

  // SAFETY: the main thread ran the addon function that `operation` names, so the envelope has that function's type.
  return reply.envelope as T;
}

/** The facade addons of a seat isolate whose relay is `relay`. Starts nothing. */
export function relayedFacades(relay: SeatRelay): RelayedFacades {
  return Object.freeze({
    reads: relayedReads(relay),
    health: relayedHealth(relay),
    cancel: Object.freeze({
      cancelTask: async (_handle, taskId, policy) => relayed(relay, { part: "cancel", call: { method: "cancelTask", args: [taskId, policy] } }),
    } satisfies CancelAddon),
    rerun: Object.freeze({
      rerunTask: async (_handle, command, policy, trace) =>
        relayed(relay, { part: "rerun", call: { method: "rerunTask", args: [command, policy, trace] } }),
    } satisfies Pick<RerunAddon, "rerunTask">),
    handle: relayedHandle(relay),
  });
}

function relayedReads(relay: SeatRelay): MonitoringReadsAddon {
  const read = async <T>(call: HandleCall<MonitoringReadsAddon>): Promise<T> => relayed(relay, { part: "reads", call });

  return Object.freeze({
    readTaskStats: async (_handle, query) => read({ method: "readTaskStats", args: [query] }),
    readTaskFacets: async (_handle, query) => read({ method: "readTaskFacets", args: [query] }),
    readTaskBreakdown: async (_handle, query) => read({ method: "readTaskBreakdown", args: [query] }),
    readTaskList: async (_handle, query) => read({ method: "readTaskList", args: [query] }),
    readWorkflowNames: async () => read({ method: "readWorkflowNames", args: [] }),
    readWorkflowRuns: async (_handle, query) => read({ method: "readWorkflowRuns", args: [query] }),
    readWorkflowRun: async (_handle, workflowId) => read({ method: "readWorkflowRun", args: [workflowId] }),
    readWorkflowNode: async (_handle, workflowId, taskIndex) => read({ method: "readWorkflowNode", args: [workflowId, taskIndex] }),
    readSchedules: async () => read({ method: "readSchedules", args: [] }),
    readTaskDetail: async (_handle, taskId) => read({ method: "readTaskDetail", args: [taskId] }),
  } satisfies MonitoringReadsAddon);
}

function relayedHealth(relay: SeatRelay): HealthAddon {
  const health = async <T>(call: HandleCall<HealthAddon>): Promise<T> => relayed(relay, { part: "health", call });

  return Object.freeze({
    pingWorkers: async (_handle, target, timeoutMs, minResponses) =>
      health({ method: "pingWorkers", args: [target, timeoutMs, minResponses] }),
    listWorkerStates: async () => health({ method: "listWorkerStates", args: [] }),
    getWorkerState: async (_handle, workerId) => health({ method: "getWorkerState", args: [workerId] }),
    getWorkerStateHistory: async (_handle, workerId, limit) => health({ method: "getWorkerStateHistory", args: [workerId, limit] }),
    getStaleTasks: async (_handle, thresholdMinutes) => health({ method: "getStaleTasks", args: [thresholdMinutes] }),
    getExpiredTasks: async () => health({ method: "getExpiredTasks", args: [] }),
    getClassCoverage: async () => health({ method: "getClassCoverage", args: [] }),
    getClassReport: async () => health({ method: "getClassReport", args: [] }),
  } satisfies HealthAddon);
}

/** The thrown envelope of a core handle member that does not cross to the main thread. */
function notRelayed(member: string): NativeEnvelope<never> {
  return {
    kind: "thrown",
    thrown: {
      code: "CLIENT_DEFECT",
      subject: `seat core handle: ${member}`,
      rule: "the main thread of the child holds the core handle; a seat isolate relays its database ping only",
    },
  };
}

/** The core handle of the facade code of a seat isolate: the database ping goes through the relay. */
function relayedHandle(relay: SeatRelay): NativeCoreHandle {
  const handle: NativeCoreHandle = Object.freeze({
    manifestVersion: () => notRelayed("manifestVersion"),
    withInstallCoverage: () => notRelayed("withInstallCoverage"),
    close: async () => notRelayed("close"),
    installSchema: async () => notRelayed("installSchema"),
    schemaStatus: async () => notRelayed("schemaStatus"),
    pingDatabase: async () => relayed<DatabasePingAnswer>(relay, { part: "database", call: { method: "pingDatabase", args: [] } }),
  });

  return handle;
}

/** The subject of the failures that the relay of a seat isolate gives. */
const RELAY_SUBJECT = "seat relay";

/** The cause of a failure at the bound of the relay: a precondition of the call that can clear. Pure. */
function boundException(message: string): ExceptionRecord {
  return { kind: "coreFailure", cause: { kind: "preconditionFailed", subject: RELAY_SUBJECT, message, retryable: true } };
}

/** `MonitoringQueryDbOperationFailed`, retryable: the failure of a monitoring read. Pure. */
function monitoringReadFailed(message: string): RawCatalogError {
  return {
    class: "MonitoringQueryDbOperationFailed",
    slots: [
      { name: "message", value: { kind: "text", text: message } },
      { name: "retryable", value: { kind: "flag", value: true } },
      { name: "exception", value: { kind: "exception", exception: boundException(message) } },
    ],
  };
}

/** `BrokerOperationFailed` with `code`, retryable: the failure of a health read or a ping. Pure. */
function brokerOperationFailed(code: string, message: string): RawCatalogError {
  return {
    class: "BrokerOperationFailed",
    slots: [
      { name: "code", value: { kind: "codeMember", catalog: "BrokerOperationFailedCode", value: code } },
      { name: "message", value: { kind: "text", text: message } },
      { name: "retryable", value: { kind: "flag", value: true } },
      { name: "exception", value: { kind: "exception", exception: boundException(message) } },
    ],
  };
}

/** `TaskActionDbOperationFailed` of `taskId`, retryable: the failure of a cancel. Pure. */
function cancelFailed(taskId: string, message: string): RawCatalogError {
  return {
    class: "TaskActionDbOperationFailed",
    slots: [
      { name: "message", value: { kind: "text", text: message } },
      { name: "retryable", value: { kind: "flag", value: true } },
      { name: "task_id", value: { kind: "uuid", text: taskId } },
      { name: "exception", value: { kind: "exception", exception: boundException(message) } },
    ],
  };
}

/** The broker code of the failure of a health call: a worker ping fails as a ping, every other read as a query. Pure. */
function healthFailureCode(call: HandleCall<HealthAddon>): "WORKER_PING_FAILED" | "MONITORING_QUERY_FAILED" {
  switch (call.method) {
    case "pingWorkers":
      return "WORKER_PING_FAILED";
    case "listWorkerStates":
    case "getWorkerState":
    case "getWorkerStateHistory":
    case "getStaleTasks":
    case "getExpiredTasks":
    case "getClassCoverage":
    case "getClassReport":
      return "MONITORING_QUERY_FAILED";
    default:
      return unreachable(call, "healthFailureCode");
  }
}

/**
 * The envelope that a facade call gets past the bound of the relay: the retryable failure of its own type, with the
 * cause `message`. Nothing reached the main thread, so nothing was read or written. Pure.
 */
export function facadeBoundAnswer(operation: FacadesSeatOperation, message: string): FacadesSeatReply {
  switch (operation.part) {
    case "reads":
      return { envelope: { kind: "value", value: { kind: "failed", error: monitoringReadFailed(message) } } };
    case "health":
      return {
        envelope: { kind: "value", value: { kind: "failed", error: brokerOperationFailed(healthFailureCode(operation.call), message) } },
      };

    case "cancel":
      return { envelope: { kind: "value", value: { kind: "failed", error: cancelFailed(operation.call.args[0], message) } } };
    case "rerun":
      return {
        envelope: {
          kind: "value",
          value: {
            kind: "failed",
            error: {
              code: RerunErrorCode.CONNECT_FAILED,
              message,
              cause: { kind: "preconditionFailed", subject: RELAY_SUBJECT, message, retryable: true },
            },
          },
        },
      };
    case "database":
      return { envelope: { kind: "value", value: { kind: "failed", error: brokerOperationFailed("DB_PING_FAILED", message) } } };
    default:
      return unreachable(operation, "facadeBoundAnswer");
  }
}
