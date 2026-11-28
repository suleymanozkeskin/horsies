// The call envelope of the native addon, and the reader that throws the class of a thrown code.
//
// Every addon function returns, or resolves with, a `NativeEnvelope`. The addon never builds a
// JavaScript `Error` and never rejects a promise: this module is the one place that turns a thrown
// code into its class, so the class identity lives in the package only.

import {
  ClientDefect,
  CoreError,
  CorePanic,
  RuntimeStartFailed,
  CoreStopped,
  ForkedAfterCoreStart,
  ManifestRefused,
  PanicRuleFixed,
  TooManyPendingCalls,
} from "../../thrown-errors.ts";
import type { CoreDefect, PanicRule, StateAfter } from "../../thrown-errors.ts";
import { raiseDefect } from "./defect.ts";
import { unreachable } from "./never.ts";
import { thrownFacts } from "./thrown-facts.ts";

/** Why an addon call threw, with the facts of the cause. */
export type NativeThrown =
  | {
      readonly code: "CORE_STOPPED";
      readonly subject: string;
      readonly cause: string;
      readonly stateAfter: StateAfter;
    }
  | { readonly code: "RUNTIME_START_FAILED"; readonly cause: string }
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
    case "RUNTIME_START_FAILED":
      return new RuntimeStartFailed(thrown);
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
 * The value of an envelope. Throws the class of the thrown code when the call threw. A defect follows
 * the defect rule first (`raiseDefect`): in a worker process the throw is not reached.
 */
export function envelopeValue<T>(envelope: NativeEnvelope<T>): T {
  switch (envelope.kind) {
    case "value":
      return envelope.value;
    case "thrown": {
      const error = thrownError(envelope.thrown);

      if (error instanceof CoreError) {
        throw error;
      }

      return raiseDefect(error);
    }

    default:
      return unreachable(envelope);
  }
}

/**
 * The value of one sync addon call. A throw of the call itself (a Node-API `TypeError` of an argument
 * of the wrong type) is a defect of the package: `ClientDefect` with `subject`, under the defect rule.
 */
export function nativeValue<T>(subject: string, call: () => NativeEnvelope<T>): T {
  let envelope: NativeEnvelope<T>;

  try {
    envelope = call();
  } catch (cause) {
    return raiseDefect(callDefect(subject, cause));
  }

  return envelopeValue(envelope);
}

/**
 * The value of one async addon call. A throw or a rejection of the call (a Promise that Node-API
 * rejects) is a defect of the package: `ClientDefect` with `subject`, under the defect rule.
 */
export async function nativeValueAsync<T>(subject: string, call: () => Promise<NativeEnvelope<T>>): Promise<T> {
  let envelope: NativeEnvelope<T>;

  try {
    envelope = await call();
  } catch (cause) {
    return raiseDefect(callDefect(subject, cause));
  }

  return envelopeValue(envelope);
}

/** The defect of an addon call that threw or rejected. */
function callDefect(subject: string, cause: unknown): CoreDefect {
  const facts = thrownFacts(cause);

  return new ClientDefect({ subject, rule: `the addon call threw ${facts.name}: ${facts.message}` });
}
