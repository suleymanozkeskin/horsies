// The operations of the seat isolates, served on the main thread of a `SHARED_THREAD` child: each area's operation
// runs on the main thread's own core of the app, as the same call of that thread would run it.
//
// - `sends`: the prepared facts of a send, an enqueue, a task info read (`ProducerCore`).
// - `facades`: one function of a facade part of the addon (the monitoring and health reads, the cancel, the rerun),
//   or the database ping of the core handle, with the app's own core handle; the reply is the envelope that it gave.
// - `workflows`: a workflow start, restart, read or handle change (`ProducerCore.workflows`).
// - A core call that throws a `CoreError` (a process-state refusal) answers `refused` with the facts of the error; the
//   relay of the isolate throws the same class. A defect of the package rejects: the pool gives it to the defect rule.

import { appCoreHandle, type CoreAccess, type ProducerCore, type SendsSeatOperation, type SendsSeatReply } from "../app/core-access.ts";
import type { FacadesSeatOperation, FacadesSeatReply } from "../monitoring/seat-calls.ts";
import { cancelAddon, type CancelAddon } from "../native/facades/cancel.ts";
import { healthAddon, type HealthAddon } from "../native/facades/health.ts";
import { monitoringReadsAddon, type MonitoringReadsAddon } from "../native/facades/monitoring-reads.ts";
import { rerunAddon, type RerunAddon } from "../native/facades/rerun.ts";
import type { NativeCoreHandle } from "../native/handle/addon.ts";
import {
  ClientDefect,
  CoreError,
  type RuntimeStartFailed,
  type CoreStopped,
  type ForkedAfterCoreStart,
  type TooManyPendingCalls,
} from "../thrown-errors.ts";
import { unreachable } from "../wire/unreachable.ts";
import { serveWorkflowsSeatOperation } from "../workflow/core.ts";
import type { RelayedRefusal, SeatAnswer, SeatReply, SeatRequest } from "./protocol.ts";

/** The facade parts of the addon that the main thread runs for the isolates, each loaded at its first use. */
export interface FacadeAddons {
  readonly reads: () => MonitoringReadsAddon;
  readonly health: () => HealthAddon;
  readonly cancel: () => CancelAddon;
  readonly rerun: () => Pick<RerunAddon, "rerunTask">;
}

/** The facade parts of the native addon. */
export const NATIVE_FACADE_ADDONS: FacadeAddons = Object.freeze({
  reads: monitoringReadsAddon,
  health: healthAddon,
  cancel: cancelAddon,
  rerun: rerunAddon,
});

/** Run `request` on the core of the main thread, `core`, and give its answer. Rejects only with a defect. */
export async function serveSeatOperation(
  core: CoreAccess,
  request: SeatRequest,
  facades: FacadeAddons = NATIVE_FACADE_ADDONS,
): Promise<SeatAnswer> {
  try {
    return { kind: "reply", reply: await areaReply(core, facades, request) };
  } catch (cause) {
    if (cause instanceof CoreError) {
      return { kind: "refused", refusal: relayedRefusal(cause) };
    }

    throw cause;
  }
}

/** The producer of the main thread's app. Another core access is a defect: a child's main thread holds the addon. */
function producerOf(core: CoreAccess): ProducerCore {
  switch (core.kind) {
    case "addon":
      return core.producer;
    case "seatRelay":
    case "noCore":
      throw new ClientDefect({ subject: "seat operation", rule: "the main thread of a child reaches the core through the addon" });
    default:
      return unreachable(core, "producerOf");
  }
}

async function areaReply(core: CoreAccess, facades: FacadeAddons, request: SeatRequest): Promise<SeatReply> {
  const area = request.area;

  switch (area) {
    case "sends":
      return { area, reply: await sendsReply(producerOf(core), request.operation) };
    case "facades":
      return { area, reply: await facadesReply(appCoreHandle(core).native, facades, request.operation) };
    case "workflows":
      return { area, reply: await serveWorkflowsSeatOperation(producerOf(core).workflows, request.operation) };
    default:
      return unreachable(area, "areaReply");
  }
}

/** The envelope of one facade call on the main thread's core handle `handle`. */
async function facadesReply(handle: NativeCoreHandle, facades: FacadeAddons, operation: FacadesSeatOperation): Promise<FacadesSeatReply> {
  switch (operation.part) {
    case "reads":
      return { envelope: await partCall(facades.reads(), handle, operation.call) };
    case "health":
      return { envelope: await partCall(facades.health(), handle, operation.call) };
    case "cancel":
      return { envelope: await partCall(facades.cancel(), handle, operation.call) };
    case "rerun":
      return { envelope: await partCall(facades.rerun(), handle, operation.call) };
    case "database":
      return { envelope: await handle.pingDatabase() };
    default:
      return unreachable(operation, "facadesReply");
  }
}

/** Call the function `call.method` of the addon part `part` with `handle` first, then the arguments of `call`. */
async function partCall(
  part: object,
  handle: NativeCoreHandle,
  call: { readonly method: string; readonly args: readonly unknown[] },
): Promise<FacadesSeatReply["envelope"]> {
  // SAFETY: `call` names a function of `part` that takes the core handle first and resolves with the envelope of its
  // facade (`HandleCall` of `monitoring/seat-calls.ts`); the isolate built `call` with that function's arguments.
  const method = Reflect.get(part, call.method) as (handle: NativeCoreHandle, ...args: readonly unknown[]) => Promise<FacadesSeatReply["envelope"]>;

  return Reflect.apply(method, part, [handle, ...call.args]);
}

async function sendsReply(producer: ProducerCore, operation: SendsSeatOperation): Promise<SendsSeatReply> {
  switch (operation.kind) {
    case "prepareSend":
      return { kind: "prepareSend", prepared: await producer.prepareSend(operation.taskName, operation.facts) };
    case "enqueue":
      return {
        kind: "enqueue",
        outcome: await producer.enqueue(operation.request, operation.sendKind, operation.trace),
      };
    case "taskInfo":
      return { kind: "taskInfo", read: await producer.taskInfo(operation.taskId) };
    default:
      return unreachable(operation, "sendsReply");
  }
}

/** The facts of `error`, by its code. Pure. */
export function relayedRefusal(error: CoreError): RelayedRefusal {
  switch (error.code) {
    case "CORE_STOPPED": {
      // SAFETY: CORE_STOPPED is the code of `CoreStopped` only.
      const stopped = error as CoreStopped;

      return { code: error.code, subject: stopped.subject, cause: stopped.cause, stateAfter: stopped.stateAfter };
    }

    case "RUNTIME_START_FAILED":
      // SAFETY: RUNTIME_START_FAILED is the code of `RuntimeStartFailed` only.
      return { code: error.code, cause: (error as RuntimeStartFailed).cause };
    case "TOO_MANY_PENDING_CALLS": {
      // SAFETY: TOO_MANY_PENDING_CALLS is the code of `TooManyPendingCalls` only.
      const pending = error as TooManyPendingCalls;

      return { code: error.code, subject: pending.subject, bound: pending.bound };
    }

    case "FORKED_AFTER_CORE_START": {
      // SAFETY: FORKED_AFTER_CORE_START is the code of `ForkedAfterCoreStart` only.
      const forked = error as ForkedAfterCoreStart;

      return { code: error.code, corePid: forked.corePid, thisPid: forked.thisPid };
    }

    default:
      return unreachable(error.code, "relayedRefusal");
  }
}
