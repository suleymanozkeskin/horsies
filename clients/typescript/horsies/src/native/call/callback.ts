// The JavaScript side of the callback primitive: the wrapper that runs a function for the core
// and settles the call, and the table of the values that such a function threw.
//
// - The core calls the wrapper with a call id and an argument. The wrapper runs the function, sync
//   or async, and settles the call: `settleCallbackReturned(callId, value)` or
//   `settleCallbackThrew(callId, name, message)`.
// - The wrapper never throws and never rejects: a throw into Node-API ends the process.
// - A thrown value stays in this module, keyed by the call id. The core gets its name and message;
//   when the operation ends, the package takes the value back (`takeThrown`) and throws it again.

import { ClientDefect, CoreError } from "../../thrown-errors.ts";
import type { CoreDefect } from "../../thrown-errors.ts";
import { requireAddon } from "./addon.ts";
import { reportDefect } from "./defect.ts";
import { thrownError } from "./envelope.ts";
import type { NativeEnvelope } from "./envelope.ts";
import { unreachable } from "./never.ts";
import { thrownFacts } from "./thrown-facts.ts";

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

/** The settle calls of the addon. */
export type SettleAddon = Pick<CallAddon, "settleCallbackReturned" | "settleCallbackThrew">;

/** How a settle call ended for the wrapper. `notSettled`: the settle call failed, a defect. */
export type SettleEnd = SettleOutcome | "notSettled";

/**
 * The settlement through the addon that `addon` gives, with the thrown values kept in this module.
 * A settle call that throws, or that answers a thrown code, is a defect of the package: `onDefect`
 * gets it, with no throw (in a worker process the defect rule aborts). A thrown value that no core
 * call waits for any more (`unknownCall`, `notSettled`) is dropped.
 */
export function settlementVia(addon: () => SettleAddon, onDefect: (defect: CoreDefect) => void): CallbackSettlement {
  return {
    returned(callId, value) {
      settleSafely(() => addon().settleCallbackReturned(callId, value), onDefect);
    },
    threw(callId, cause) {
      keepThrown(callId, cause);

      const facts = thrownFacts(cause);
      const end = settleSafely(() => addon().settleCallbackThrew(callId, facts.name, facts.message), onDefect);

      switch (end) {
        case "unknownCall":
        case "notSettled":
          dropThrown(callId);

          return;
        case "settled":
          return;
        default:
          unreachable(end);
      }
    },
  };
}

/** The settlement of the registrations of the package: the addon, and the defect rule. */
export const addonSettlement: CallbackSettlement = settlementVia(callAddon, reportDefect);

/** Run one settle call; never throw. A failure is a defect, which goes to `onDefect`. */
function settleSafely(settle: () => NativeEnvelope<SettleOutcome>, onDefect: (defect: CoreDefect) => void): SettleEnd {
  let envelope: NativeEnvelope<SettleOutcome>;

  try {
    envelope = settle();
  } catch (cause) {
    const facts = thrownFacts(cause);

    onDefect(new ClientDefect({ subject: "settleCallback", rule: `the settle call threw ${facts.name}: ${facts.message}` }));

    return "notSettled";
  }

  switch (envelope.kind) {
    case "value":
      return envelope.value;
    case "thrown": {
      const error = thrownError(envelope.thrown);

      onDefect(
        error instanceof CoreError
          ? new ClientDefect({ subject: "settleCallback", rule: `the settle call answered ${error.code}: ${error.message}` })
          : error,
      );

      return "notSettled";
    }

    default:
      return unreachable(envelope);
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
