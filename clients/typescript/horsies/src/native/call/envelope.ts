// The call envelope of the native addon, and the reader that throws the class of a thrown code.
//
// Every addon function returns, or resolves with, a `NativeEnvelope`. The addon never builds a
// JavaScript `Error` and never rejects a promise: this module is the one place that turns a thrown
// code into its class, so the class identity lives in the package only.

import {
  ClientDefect,
  CoreError,
  CorePanic,
  CoreStartFailed,
  CoreStopped,
  ForkedAfterCoreStart,
  ManifestRefused,
  PanicRuleFixed,
  TooManyPendingCalls,
} from "../../thrown-errors.ts";
import type { CoreDefect, PanicRule, StateAfter } from "../../thrown-errors.ts";
import { loadAddon } from "../load.ts";
import { processCoreView } from "../process-core/addon.ts";
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
    case "CLIENT_DEFECT":
      return new ClientDefect(thrown);
    default:
      return unreachable(thrown);
  }
}

/**
 * The value of an envelope. Throws the class of the thrown code when the call threw.
 *
 * A defect is reported to the addon first. Under the panic rule `ABORT_PROCESS` (a worker process)
 * the addon writes it to stderr and to the log sink, drains the log for a bounded time, and aborts
 * the process: the throw is not reached. Under `STOP_CORE` the defect is thrown.
 */
export function envelopeValue<T>(envelope: NativeEnvelope<T>): T {
  switch (envelope.kind) {
    case "value":
      return envelope.value;
    case "thrown": {
      const error = thrownError(envelope.thrown);

      if (!(error instanceof CoreError)) {
        reportDefect(error);
      }

      throw error;
    }

    default:
      return unreachable(envelope);
  }
}

/** Report a defect to the loaded addon. Without a loaded addon there is no rule to apply. */
function reportDefect(defect: CoreDefect): void {
  const load = loadAddon();

  switch (load.kind) {
    case "loaded":
      // The answer is not read: under `ABORT_PROCESS` the call does not return, and under
      // `STOP_CORE` the caller throws the defect in every case.
      processCoreView(load.exports).reportDefect(defect.code, defect.message);

      return;
    case "outsideMainThread":
    case "platformUnsupported":
    case "loadFailed":
      return;
    default:
      unreachable(load);
  }
}
