// The rerun area of the app: the members `app.rerun` and `app.rerunIn`. The app hub builds it once
// with `createRerunArea(host)` and gives one line per member.

import type { CoreHandle } from "../native/handle/core-handle.ts";
import type { TraceArgument } from "../native/values/records.ts";
import { addonRerunBridge } from "./bridge.ts";
import type { RerunBridge } from "./bridge.ts";
import type { CallerTransaction } from "./caller.ts";
import { rerunInApp, rerunInCaller } from "./rerun.ts";
import type { AppRerunResult, CallerRerunResult } from "./rerun.ts";
import type { RerunCommand, RerunPolicy } from "./types.ts";

/** What the rerun area needs from the app. */
export interface RerunHost {
  /** The core handle of this process: the app's handle, or in a worker child the child's producer handle. */
  coreHandle(): CoreHandle;
  /**
   * The trace carrier of the caller's active context, which the new task stores. It never throws. With
   * no tracing, `NO_ACTIVE_CARRIER`.
   */
  activeCarrier(): TraceArgument;
}

/** The rerun members of the app. */
export interface RerunArea {
  /**
   * Rerun a retained terminal task in a transaction of the app's handle, and commit it. The handle
   * connects and installs the schema once, as a send does. Every outcome but `enqueued` writes nothing.
   */
  rerun(command: RerunCommand, policy: RerunPolicy): Promise<AppRerunResult>;
  /**
   * Rerun a retained terminal task in `transaction`, which the caller owns, so the rerun commits or
   * rolls back with the caller's own writes. Nothing commits here. After an error, the caller does not
   * commit.
   */
  rerunIn(transaction: CallerTransaction, command: RerunCommand, policy: RerunPolicy): Promise<CallerRerunResult>;
}

/** The trace argument of a call with no active context. */
export const NO_ACTIVE_CARRIER: TraceArgument = { kind: "noCarrier" };

/** The rerun area of the app that `host` describes, on the loaded addon. Starts nothing. */
export function createRerunArea(host: RerunHost): RerunArea {
  return rerunAreaOn(host, addonRerunBridge);
}

/** The rerun area of `host` on `bridge`. Starts nothing. */
export function rerunAreaOn(host: RerunHost, bridge: RerunBridge): RerunArea {
  return {
    rerun: async (command: RerunCommand, policy: RerunPolicy): Promise<AppRerunResult> =>
      rerunInApp(bridge, host.coreHandle(), host.activeCarrier(), command, policy),
    rerunIn: async (
      transaction: CallerTransaction,
      command: RerunCommand,
      policy: RerunPolicy,
    ): Promise<CallerRerunResult> => rerunInCaller(bridge, host.activeCarrier(), transaction, command, policy),
  };
}
