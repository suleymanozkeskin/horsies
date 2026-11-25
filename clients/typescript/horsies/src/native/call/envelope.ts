// The call envelope of the native addon, and the reader that throws the class of a thrown code.
//
// Every addon function returns, or resolves with, a `NativeEnvelope`. The addon never builds a
// JavaScript `Error` and never rejects a promise: this module is the one place that turns a thrown
// code into its class, so the class identity lives in the package only.

import { threadId } from "node:worker_threads";

import {
  AddonOutsideMainThread,
  ClientDefect,
  CorePanic,
  CoreStartFailed,
  CoreStopped,
  ForkedAfterCoreStart,
  ManifestRefused,
  PanicRuleFixed,
  TooManyPendingCalls,
} from "../../thrown-errors.ts";
import type { CoreDefect, CoreError, PanicRule, StateAfter } from "../../thrown-errors.ts";
import { unreachable } from "./never.ts";

/** Why an addon call threw, with the facts of the cause. */
export type NativeThrown =
  | {
      readonly code: "CORE_STOPPED";
      readonly subject: string;
      readonly cause: string;
      readonly stateAfter: StateAfter;
    }
  | { readonly code: "CORE_START_FAILED"; readonly cause: string }
  | { readonly code: "TOO_MANY_PENDING_CALLS"; readonly subject: string; readonly bound: number }
  | { readonly code: "FORKED_AFTER_CORE_START"; readonly corePid: number; readonly thisPid: number }
  | { readonly code: "MANIFEST_REFUSED"; readonly fieldPath: string; readonly cause: string }
  | { readonly code: "PANIC_RULE_FIXED"; readonly current: PanicRule }
  | { readonly code: "CORE_PANIC"; readonly subject: string; readonly cause: string }
  | { readonly code: "ADDON_OUTSIDE_MAIN_THREAD"; readonly environments: number }
  | { readonly code: "CLIENT_DEFECT"; readonly subject: string; readonly rule: string };

/** The answer of an addon function: its value, or why it threw. */
export type NativeEnvelope<T> =
  | { readonly kind: "value"; readonly value: T }
  | { readonly kind: "thrown"; readonly thrown: NativeThrown };

/** The error class of a thrown code, with its facts. Pure. */
export function thrownError(thrown: NativeThrown): CoreError | CoreDefect {
  switch (thrown.code) {
    case "CORE_STOPPED":
      return new CoreStopped(thrown);
    case "CORE_START_FAILED":
      return new CoreStartFailed(thrown);
    case "TOO_MANY_PENDING_CALLS":
      return new TooManyPendingCalls(thrown);
    case "FORKED_AFTER_CORE_START":
      return new ForkedAfterCoreStart(thrown);
    case "MANIFEST_REFUSED":
      return new ManifestRefused(thrown);
    case "PANIC_RULE_FIXED":
      return new PanicRuleFixed(thrown);
    case "CORE_PANIC":
      return new CorePanic(thrown);
    case "ADDON_OUTSIDE_MAIN_THREAD":
      return new AddonOutsideMainThread({ threadId });
    case "CLIENT_DEFECT":
      return new ClientDefect(thrown);
    default:
      return unreachable(thrown);
  }
}

/** The value of an envelope. Throws the class of the thrown code when the call threw. */
export function envelopeValue<T>(envelope: NativeEnvelope<T>): T {
  switch (envelope.kind) {
    case "value":
      return envelope.value;
    case "thrown":
      throw thrownError(envelope.thrown);
    default:
      return unreachable(envelope);
  }
}
