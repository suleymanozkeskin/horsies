// The rerun area of the app: the members `app.rerunTask` and `app.rerunTaskIn`. The app hub builds it once
// with `createRerunArea(host)` and gives one line per member.

import { appCoreHandle, type CoreAccess } from "../app/core-access.ts";
import { type RelayedAddon, relayedFacades } from "../monitoring/seat-calls.ts";
import { CoreHandle } from "../native/handle/core-handle.ts";
import { unreachable } from "../wire/unreachable.ts";
import type { TraceArgument } from "../native/values/records.ts";
import { nativeValueAsync } from "../native/call/envelope.ts";
import type { AppRerunAnswer, RegisteredTaskCodesArg, RerunAddon } from "../native/facades/rerun.ts";
import type { SeatRelayFullOutcome } from "../seats/relay-full.ts";
import { addonRerunBridge } from "./bridge.ts";
import type { RerunRequest } from "./parse.ts";
import type { RerunBridge } from "./bridge.ts";
import type { CallerTransaction } from "./caller.ts";
import { rerunInApp, rerunInCaller } from "./rerun.ts";
import type { AppRerunResult, CallerRerunResult } from "./rerun.ts";
import type { RerunCommand, RerunEnqueuePolicy } from "./types.ts";

/** What the rerun area needs from the app. */
export interface RerunHost {
  /** The core handle of this process: the app's handle, or in a worker child the child's producer handle. */
  coreHandle(): CoreHandle;
  /**
   * The trace carrier of the caller's active context, which the new task stores. It never throws. With
   * no tracing, `NO_ACTIVE_CARRIER`.
   */
  activeCarrier(): TraceArgument;
  /**
   * The declared custom codes of the tasks that the app lists, each task once: the core checks the stored retry
   * list of a rerun's source against them.
   */
  registeredCodes(): RegisteredTaskCodesArg;
}

/** The rerun members of the app. */
export interface RerunArea {
  /**
   * Rerun a retained terminal task in a transaction of the app's handle, and commit it. The handle
   * connects and installs the schema once, as a send does. Every outcome but `enqueued` writes nothing.
   */
  rerunTask(command: RerunCommand, policy: RerunEnqueuePolicy): Promise<AppRerunResult>;
  /**
   * Rerun a retained terminal task in `transaction`, which the caller owns, so the rerun commits or
   * rolls back with the caller's own writes. Nothing commits here. After an error, the caller does not
   * commit.
   */
  rerunTaskIn(transaction: CallerTransaction, command: RerunCommand, policy: RerunEnqueuePolicy): Promise<CallerRerunResult>;
}

/** The trace argument of a call with no active context. */
export const NO_ACTIVE_CARRIER: TraceArgument = { kind: "noCarrier" };

/**
 * The rerun area of an app whose thread reaches the core through `core`. In a seat isolate `rerunTask` goes through the
 * seat relay to the main thread of its child; `rerunTaskIn` does not cross a thread (the caller's transaction object stays
 * in the isolate), so there it throws `AddonOutsideMainThread`. Starts nothing.
 */
export function rerunAreaOfCore(
  core: CoreAccess,
  activeCarrier: () => TraceArgument,
  registeredCodes: () => RegisteredTaskCodesArg,
): RerunArea {
  switch (core.kind) {
    case "seatRelay": {
      const relayed = relayedFacades(core.relay);
      const handle = new CoreHandle(relayed.handle);

      return rerunAreaOn({ coreHandle: () => handle, activeCarrier, registeredCodes }, relayedRerunBridge(relayed.rerun));
    }

    case "addon":
    case "noCore":
      return createRerunArea({ coreHandle: () => appCoreHandle(core), activeCarrier, registeredCodes });
    default:
      return unreachable(core, "rerunAreaOfCore");
  }
}

/** The rerun bridge of a seat isolate: `rerunTask` on the relayed addon; the caller steps on the loaded addon. */
function relayedRerunBridge(rerun: RelayedAddon<Pick<RerunAddon, "rerunTask">>): RerunBridge {
  return {
    rerunTask: async (
      handle: CoreHandle,
      request: RerunRequest,
      trace: TraceArgument,
      codes: RegisteredTaskCodesArg,
    ): Promise<AppRerunAnswer | SeatRelayFullOutcome> =>
      nativeValueAsync("rerunTask", () => rerun.rerunTask(handle.native, request.command, request.policy, trace, codes)),
    readStatement: addonRerunBridge.readStatement,
    prepareWrite: addonRerunBridge.prepareWrite,
  };
}

/** The rerun area of the app that `host` describes, on the loaded addon. Starts nothing. */
export function createRerunArea(host: RerunHost): RerunArea {
  return rerunAreaOn(host, addonRerunBridge);
}

/** The rerun area of `host` on `bridge`. Starts nothing. */
export function rerunAreaOn(host: RerunHost, bridge: RerunBridge): RerunArea {
  return {
    rerunTask: async (command: RerunCommand, policy: RerunEnqueuePolicy): Promise<AppRerunResult> =>
      rerunInApp(bridge, host.coreHandle(), host.activeCarrier(), host.registeredCodes(), command, policy),
    rerunTaskIn: async (
      transaction: CallerTransaction,
      command: RerunCommand,
      policy: RerunEnqueuePolicy,
    ): Promise<CallerRerunResult> =>
      rerunInCaller(bridge, host.activeCarrier(), host.registeredCodes(), transaction, command, policy),
  };
}
