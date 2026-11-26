// The functions of the `handler_child` area of the addon through the call wrapper: a throw of a call is a defect
// under the defect rule, and a thrown envelope is its error.

import { nativeValue } from "../call/envelope.ts";
import type { StartHookCall } from "../child/addon.ts";
import { handlerChildAddon } from "./addon.ts";
import type { EndedAttemptRecord, HandlerChildPlan, HandlerEventCall } from "./addon.ts";

/**
 * Put the handler child app of `plan` in the handler slot of the child host, before `serveChild`. The three functions
 * never throw: a throw into Node-API ends the process. A second fill, or a fill after the serve, throws `ClientDefect`.
 */
export function fillHandlerChild(
  plan: HandlerChildPlan,
  deliver: (callId: bigint, call: HandlerEventCall) => void,
  hook: (callId: bigint, call: StartHookCall) => void,
  endedSpan: (attempt: EndedAttemptRecord) => void,
): void {
  nativeValue("fillHandlerChild", () => handlerChildAddon().fillHandlerChild(plan, deliver, hook, endedSpan));
}
