// The facade calls of a seat isolate: the monitoring reads, the health reads, the database ping, the cancel and the
// rerun in the app's transaction. A seat isolate never loads the addon, so each call of a facade there goes through the
// seat relay to the main thread of its child, which runs the same addon function with its own core handle.
//
// - The operation is plain data: the part of the addon, the function, and its arguments after the core handle. The
//   reply is the envelope that the function resolved with. The facade code of the isolate reads it as it reads the
//   addon's envelope on a main thread, so each call keeps its own `Result`.
// - The relayed addons ignore the handle that the facade code passes: the main thread uses its own. The database ping
//   goes through `ping`. The members of the relayed core handle are calls that a facade of a seat body never makes, and
//   each answers with a thrown `CLIENT_DEFECT` envelope.
// - Past the bound of the relay nothing is sent: the envelope holds the relay's `seatRelayFull`, and the facade code
//   gives `SEAT_RELAY_FULL` through the error union of the call (`RelayedAddon`).
// - `rerunIn` takes the caller's transaction object, which cannot cross a thread: it is not relayed.

import type { SeatRelay } from "../app/core-access.ts";
import type { NativeEnvelope } from "../native/call/envelope.ts";
import type { CancelAddon } from "../native/facades/cancel.ts";
import type { HealthAddon } from "../native/facades/health.ts";
import type { MonitoringReadsAddon } from "../native/facades/monitoring-reads.ts";
import type { RerunAddon } from "../native/facades/rerun.ts";
import type { DatabasePingRecord, NativeCoreHandle } from "../native/handle/addon.ts";
import type { NativeOutcome } from "../native/values/records.ts";
import type { SeatRelayFullOutcome } from "../seats/relay-full.ts";
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

/**
 * The functions of an addon part in the form that a thread of any kind gives them: the value of each envelope may be
 * the seat relay's `seatRelayFull` (in a seat isolate, past the bound of the relay). The addon of a main thread has
 * this form too, with no `seatRelayFull`.
 */
export type RelayedAddon<A> = {
  readonly [M in keyof A]: A[M] extends (...args: infer P) => Promise<NativeEnvelope<infer V>>
    ? (...args: P) => Promise<NativeEnvelope<V | SeatRelayFullOutcome>>
    : never;
};

/** The database ping of the app's core handle, in the form of `RelayedAddon`. */
export type RelayedPing = () => Promise<NativeEnvelope<NativeOutcome<DatabasePingRecord> | SeatRelayFullOutcome>>;

/** The facade addons of a seat isolate, on its relay. */
export interface RelayedFacades {
  readonly reads: RelayedAddon<MonitoringReadsAddon>;
  readonly health: RelayedAddon<HealthAddon>;
  readonly cancel: RelayedAddon<CancelAddon>;
  readonly rerun: RelayedAddon<Pick<RerunAddon, "rerunTask">>;
  readonly ping: RelayedPing;
  readonly handle: NativeCoreHandle;
}

/** Send `operation` through `relay`, and give the envelope of the reply, or the relay's `seatRelayFull` in one. */
async function relayed<V>(relay: SeatRelay, operation: FacadesSeatOperation): Promise<NativeEnvelope<V | SeatRelayFullOutcome>> {
  const answer = await relay.request("facades", operation);

  switch (answer.kind) {
    case "reply":
      // SAFETY: the main thread ran the addon function that `operation` names, so the envelope has that function's type.
      return answer.reply.envelope as NativeEnvelope<V>;
    case "seatRelayFull":
      return { kind: "value", value: answer };
    default:
      return unreachable(answer, "relayed");
  }
}

/** The facade addons of a seat isolate whose relay is `relay`. Starts nothing. */
export function relayedFacades(relay: SeatRelay): RelayedFacades {
  return Object.freeze({
    reads: relayedReads(relay),
    health: relayedHealth(relay),
    cancel: Object.freeze({
      cancelTask: async (_handle, taskId, policy) => relayed(relay, { part: "cancel", call: { method: "cancelTask", args: [taskId, policy] } }),
    } satisfies RelayedAddon<CancelAddon>),
    rerun: Object.freeze({
      rerunTask: async (_handle, command, policy, trace) =>
        relayed(relay, { part: "rerun", call: { method: "rerunTask", args: [command, policy, trace] } }),
    } satisfies RelayedAddon<Pick<RerunAddon, "rerunTask">>),
    ping: async () => relayed<NativeOutcome<DatabasePingRecord>>(relay, { part: "database", call: { method: "pingDatabase", args: [] } }),
    handle: relayedHandle(),
  });
}

function relayedReads(relay: SeatRelay): RelayedAddon<MonitoringReadsAddon> {
  const read = async <V>(call: HandleCall<MonitoringReadsAddon>): Promise<NativeEnvelope<V | SeatRelayFullOutcome>> =>
    relayed<V>(relay, { part: "reads", call });

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
  } satisfies RelayedAddon<MonitoringReadsAddon>);
}

function relayedHealth(relay: SeatRelay): RelayedAddon<HealthAddon> {
  const health = async <V>(call: HandleCall<HealthAddon>): Promise<NativeEnvelope<V | SeatRelayFullOutcome>> =>
    relayed<V>(relay, { part: "health", call });

  return Object.freeze({
    pingWorkers: async (_handle, target, waitMs, minResponses) =>
      health({ method: "pingWorkers", args: [target, waitMs, minResponses] }),
    listWorkerStates: async () => health({ method: "listWorkerStates", args: [] }),
    getWorkerState: async (_handle, workerId) => health({ method: "getWorkerState", args: [workerId] }),
    getWorkerStateHistory: async (_handle, workerId, limit) => health({ method: "getWorkerStateHistory", args: [workerId, limit] }),
    getStaleTasks: async (_handle, thresholdMinutes) => health({ method: "getStaleTasks", args: [thresholdMinutes] }),
    getExpiredTasks: async () => health({ method: "getExpiredTasks", args: [] }),
    getClassCoverage: async () => health({ method: "getClassCoverage", args: [] }),
    getClassReport: async () => health({ method: "getClassReport", args: [] }),
  } satisfies RelayedAddon<HealthAddon>);
}

/** The thrown envelope of a core handle member that does not cross to the main thread. */
function notRelayed(member: string): NativeEnvelope<never> {
  return {
    kind: "thrown",
    thrown: {
      code: "CLIENT_DEFECT",
      subject: `seat core handle: ${member}`,
      rule: "the main thread of the child holds the core handle; a seat isolate relays its database ping through `ping`",
    },
  };
}

/** The core handle of the facade code of a seat isolate: the facade code passes it, and the relayed addons ignore it. */
function relayedHandle(): NativeCoreHandle {
  const handle: NativeCoreHandle = Object.freeze({
    manifestVersion: () => notRelayed("manifestVersion"),
    withInstallCoverage: () => notRelayed("withInstallCoverage"),
    close: async () => notRelayed("close"),
    installSchema: async () => notRelayed("installSchema"),
    schemaStatus: async () => notRelayed("schemaStatus"),
    pingDatabase: async () => notRelayed("pingDatabase"),
  });

  return handle;
}
