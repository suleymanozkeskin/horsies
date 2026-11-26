// The functions of the `handler_child` area of the addon, their records, and the accessor.
//
// `fillHandlerChild` runs on the JavaScript thread before `serveChild`: it builds the core's handler child app of the
// plan and puts it in the handler slot of the child host. The core calls `deliver` for each event of a handler and
// `hook` for each start hook of the handler child (the callback primitive), and posts the facts of each span of an
// attempt that the worker ended to `endedSpan` (no answer).

import { requireAddon } from "../call/addon.ts";
import type { NativeEnvelope } from "../call/envelope.ts";
import type { BodyCarrierRecord, BodyExecution, BodyWorkflowRecord, StartHookCall } from "../child/addon.ts";

/** One handler of the plan: its name and the wire tag of its event type. */
export interface HandlerEntry {
  readonly name: string;
  readonly eventType: string;
}

/** Whether the handler child makes the span of each attempt that the worker ended. */
export type EndedSpansMode = "NO_SPANS" | "MAKE_SPANS";

/** What the package gives the handler child. */
export interface HandlerChildPlan {
  /** The handlers in registration order: handler `index` of the dispatcher is the entry at `index`. */
  readonly handlers: HandlerEntry[];
  /** The name of each `onHandlerChildStart` hook, in run order. */
  readonly startHooks: string[];
  readonly endedSpans: EndedSpansMode;
}

/** One call of the event dispatcher: the handler at `index` takes the event of the wire text `wireJson`. */
export interface HandlerEventCall {
  readonly index: number;
  readonly wireJson: string;
}

/** The answer of the event dispatcher: `failed` carries the text that the core logs with the handler's name. */
export type HandlerEventAnswer = { readonly kind: "handled" } | { readonly kind: "failed"; readonly cause: string };

/** The workflow node of an attempt that the worker ended: known, or not known (the span has no workflow attribute). */
export type EndedWorkflowRecord =
  | { readonly kind: "known"; readonly workflow: BodyWorkflowRecord }
  | { readonly kind: "workflowNotKnown" };

/**
 * The facts of the span of one attempt that the worker ended. `attempt` is from 1; `execution` is the kind of the
 * child that ran it; `errorCode` is the code of the attempt row; the times are epoch microseconds.
 */
export interface EndedAttemptRecord {
  readonly taskId: string;
  readonly taskName: string;
  readonly queueName: string;
  readonly attempt: number;
  readonly workerId: string;
  readonly execution: BodyExecution;
  readonly workflow: EndedWorkflowRecord;
  readonly carrier: BodyCarrierRecord;
  readonly errorCode: string;
  readonly startedAtMicros: bigint;
  readonly endedAtMicros: bigint;
}

/** The functions of the `handler_child` area. */
export interface HandlerChildAddon {
  fillHandlerChild(
    plan: HandlerChildPlan,
    deliver: (callId: bigint, call: HandlerEventCall) => void,
    hook: (callId: bigint, call: StartHookCall) => void,
    endedSpan: (attempt: EndedAttemptRecord) => void,
  ): NativeEnvelope<undefined>;
}

/** The `handler_child` functions of the addon. Throws as `requireAddon` does. */
export function handlerChildAddon(): HandlerChildAddon {
  // SAFETY: the addon registers these functions with these signatures; the declaration test of
  // this area compares this interface with the declarations that the native build writes.
  return requireAddon() as HandlerChildAddon;
}
