// The JavaScript side of the callback primitive: the wrapper that runs a function for the core
// and settles the call, and the table of the values that such a function threw.
//
// - The core calls the wrapper with a call id and an argument. The wrapper runs the function, sync
//   or async, and settles the call: `settleCallbackReturned(callId, value)` or
//   `settleCallbackThrew(callId, name, message)`.
// - The wrapper never throws and never rejects: a throw into Node-API ends the process.
// - A thrown value stays in this module, keyed by the call id. The core gets its name and message;
//   when the operation ends, the package takes the value back (`takeThrown`) and throws it again.

import { requireAddon } from "./addon.ts";
import type { NativeEnvelope } from "./envelope.ts";
import { unreachable } from "./never.ts";

/** How a settle call ended: `unknownCall` when the core no longer waits for the call. */
export type SettleOutcome = "settled" | "unknownCall";

/** Why a call into JavaScript was not run. */
export type NotRunCause = "queueFull" | "envClosing";

/** The functions of the `call` area. */
export interface CallAddon {
  settleCallbackReturned<Answer>(callId: bigint, value: Answer): NativeEnvelope<SettleOutcome>;
  settleCallbackThrew(callId: bigint, name: string, message: string): NativeEnvelope<SettleOutcome>;
}

/** The `call` functions of the addon. Throws as `requireAddon` does. */
export function callAddon(): CallAddon {
  // SAFETY: the addon registers these functions with these signatures; the declaration test of
  // this area compares this interface with the declarations that the native build writes.
  return requireAddon() as CallAddon;
}

/** Where a wrapper settles its calls. Neither method throws. */
export interface CallbackSettlement {
  returned<Answer>(callId: bigint, value: Answer): void;
  threw(callId: bigint, cause: unknown): void;
}

/**
 * The wrapper of `callback` for a registration: it runs `callback` with the argument of each call,
 * awaits a returned Promise, and settles the call. It never throws and never rejects.
 */
export function settlingCallback<Argument, Answer>(
  callback: (argument: Argument) => Answer | PromiseLike<Answer>,
  settlement: CallbackSettlement,
): (callId: bigint, argument: Argument) => void {
  return (callId, argument) => {
    let returned: Answer | PromiseLike<Answer>;

    try {
      returned = callback(argument);
    } catch (cause) {
      settlement.threw(callId, cause);

      return;
    }

    Promise.resolve(returned).then(
      (value) => {
        settlement.returned(callId, value);
      },
      (cause: unknown) => {
        settlement.threw(callId, cause);
      },
    );
  };
}

/** The settlement through the addon, with the thrown values kept in this module. */
export const addonSettlement: CallbackSettlement = {
  returned(callId, value) {
    settleSafely(() => callAddon().settleCallbackReturned(callId, value));
  },
  threw(callId, cause) {
    keepThrown(callId, cause);

    const facts = thrownFacts(cause);
    const outcome = settleSafely(() => callAddon().settleCallbackThrew(callId, facts.name, facts.message));

    switch (outcome) {
      case "unknownCall":
      case "notSettled":
        // Nobody will ask for the value: the core no longer waits for the call.
        dropThrown(callId);

        return;
      case "settled":
        return;
      default:
        unreachable(outcome);
    }
  },
};

/** Run one settle call; never throw. A failure goes to stderr as one line. */
function settleSafely(settle: () => NativeEnvelope<SettleOutcome>): SettleOutcome | "notSettled" {
  try {
    const envelope = settle();

    switch (envelope.kind) {
      case "value":
        return envelope.value;
      case "thrown":
        writeStderrSafely(`horsies: a settle call of a callback threw ${envelope.thrown.code}\n`);

        return "notSettled";
      default:
        return unreachable(envelope);
    }
  } catch (cause) {
    writeStderrSafely(`horsies: a settle call of a callback failed: ${thrownFacts(cause).message}\n`);

    return "notSettled";
  }
}

function writeStderrSafely(line: string): void {
  try {
    process.stderr.write(line);
  } catch {
    // Nothing more can report it; the wrapper must not throw into the addon.
  }
}

/** The name and the message of a thrown value. A value that is not an `Error` has the name `NotAnError`. */
export function thrownFacts(cause: unknown): { readonly name: string; readonly message: string } {
  if (cause instanceof Error) {
    return { name: cause.name, message: cause.message };
  }

  try {
    return { name: "NotAnError", message: String(cause) };
  } catch {
    return { name: "NotAnError", message: "a thrown value with no text" };
  }
}

/** The most thrown values kept at once. At the bound, the oldest kept value is dropped. */
export const MAX_KEPT_THROWN = 65_536;

const keptThrown = new Map<bigint, unknown>();

/** A kept thrown value, or `missing`: never kept, taken before, or dropped at the bound. */
export type KeptThrown = { readonly kind: "kept"; readonly value: unknown } | { readonly kind: "missing" };

/** Keep the thrown value of the call `callId`. */
export function keepThrown(callId: bigint, cause: unknown): void {
  if (keptThrown.size >= MAX_KEPT_THROWN) {
    const oldest = keptThrown.keys().next();

    if (oldest.done !== true) {
      keptThrown.delete(oldest.value);
    }
  }

  keptThrown.set(callId, cause);
}

/** Take back the thrown value of the call `callId`; it is no longer kept. */
export function takeThrown(callId: bigint): KeptThrown {
  if (!keptThrown.has(callId)) {
    return { kind: "missing" };
  }

  const value = keptThrown.get(callId);

  keptThrown.delete(callId);

  return { kind: "kept", value };
}

function dropThrown(callId: bigint): void {
  keptThrown.delete(callId);
}
