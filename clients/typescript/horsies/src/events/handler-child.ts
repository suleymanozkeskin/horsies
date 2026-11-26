// The handler child of an app: the same entry, started by the worker in child mode with the handler kind. Before the
// serve, child mode fills the core's handler slot with the app's handlers, the `onHandlerChildStart` hooks and the
// span maker of the attempts that the worker ended; a task child reads none of it.
//
// - Event delivery: the core runs each handler on a thread of its own, with a queue of 10,000 events
//   (`HANDLER_QUEUE_EVENTS`); a full queue drops the event for that handler and counts it. For each event the core
//   calls the dispatcher with the handler's index and the event's wire text, and waits for the answer: one event at a
//   time per handler. The dispatcher decodes the text with the generated decoder and runs the handler. A throw or a
//   rejection answers `failed` with its type and message: the core counts it in `horsies.telemetry.handler.errors`
//   and logs it with the handler's name, and the handler takes its next event.
// - Every handler of the child runs on its one JavaScript event loop: a handler that holds the loop with sync work
//   delays the other handlers and the worker-ended spans of the child. Each handler's queue bound still holds, and no
//   task is affected.
// - Start hooks: in registration order, each for at most 10 s (the core keeps the bound). A throw, a rejection or a
//   hook past its bound ends the handler child with 173; the worker runs its tasks on.

import { decodeEvent, type EventDecodeError } from "../_catalog/events.ts";
import type { AppContext } from "../app/context.ts";
import { thrownRecord } from "../child/thrown-error.ts";
import type { CallbackSettlement } from "../native/call/callback.ts";
import { settlingCallback } from "../native/call/callback.ts";
import { raiseDefect } from "../native/call/defect.ts";
import type { StartHookCall, StartHookEnd } from "../native/child/addon.ts";
import type {
  EndedAttemptRecord,
  EndedSpansMode,
  HandlerChildPlan,
  HandlerEventAnswer,
  HandlerEventCall,
} from "../native/handler-child/addon.ts";
import type { RaiseDefect } from "../child/state.ts";
import { tracingPartsOfApp } from "../telemetry/create-hook.ts";
import type { EndedSpans } from "../telemetry/ended-span.ts";
import { ClientDefect } from "../thrown-errors.ts";
import { unreachable } from "../wire/unreachable.ts";
import { type EventMembersView, type HandlerChildHookMember, type RegisteredHandler, servedEventMembers } from "./register.ts";
import { wireItemsOf } from "./wire-text.ts";

/** The addon call of the handler child. */
export interface HandlerChildNative {
  fillHandlerChild(
    plan: HandlerChildPlan,
    deliver: (callId: bigint, call: HandlerEventCall) => void,
    hook: (callId: bigint, call: StartHookCall) => void,
    endedSpan: (attempt: EndedAttemptRecord) => void,
  ): void;
}

const HANDLED: HandlerEventAnswer = Object.freeze({ kind: "handled" });

/**
 * Close the event members of `context` and fill the handler slot of the child host with them, before the serve.
 * Nothing is filled when the app has no handler, no start hook of the handler child and no worker-ended spans: the
 * worker then starts no handler child.
 */
export function fillHandlerChildOf(context: AppContext, native: HandlerChildNative, settlement: CallbackSettlement): void {
  const members = servedEventMembers(context);
  const { endedSpans } = tracingPartsOfApp(context);

  if (members.handlers.length === 0 && members.hooks.length === 0 && endedSpans.kind === "noSpans") {
    return;
  }

  native.fillHandlerChild(
    handlerChildPlan(members, endedSpans),
    settlingCallback(eventDispatch(members.handlers), settlement),
    settlingCallback(hookDispatch(members.hooks), settlement),
    spanMaker(endedSpans),
  );
}

/** The plan of the handler child of `members`. Pure. */
export function handlerChildPlan(members: EventMembersView, endedSpans: EndedSpans): HandlerChildPlan {
  return {
    handlers: members.handlers.map((handler) => ({ name: handler.name, eventType: handler.eventType })),
    startHooks: members.hooks.map((hook) => hook.name),
    endedSpans: endedSpansMode(endedSpans),
  };
}

function endedSpansMode(endedSpans: EndedSpans): EndedSpansMode {
  switch (endedSpans.kind) {
    case "makeSpans":
      return "MAKE_SPANS";
    case "noSpans":
      return "NO_SPANS";
    default:
      return unreachable(endedSpans, "endedSpansMode");
  }
}

/** The span maker that the core calls: the one of the user's SDK; with no spans the core calls none. */
function spanMaker(endedSpans: EndedSpans): (attempt: EndedAttemptRecord) => void {
  switch (endedSpans.kind) {
    case "makeSpans":
      return endedSpans.make;
    case "noSpans":
      return (): void => undefined;
    default:
      return unreachable(endedSpans, "spanMaker");
  }
}

/**
 * The event dispatcher of `handlers`: the handler at the call's index takes the decoded event. A call with no handler
 * at its index is a defect of the package (`raise`).
 */
export function eventDispatch(
  handlers: readonly RegisteredHandler[],
  raise: RaiseDefect = raiseDefect,
): (call: HandlerEventCall) => Promise<HandlerEventAnswer> {
  return async (call) => {
    const handler = handlers[call.index] ?? raise(new ClientDefect({ subject: "eventDispatch", rule: `the plan has a handler at index ${String(call.index)}` }));
    const read = wireItemsOf(call.wireJson);

    if (read.kind === "notItems") {
      return failed(`the event text of the core does not read: ${read.cause}`);
    }

    const decoded = decodeEvent(read.items);

    if (!decoded.ok) {
      return failed(`the event of the core does not decode: ${decodeRefusalText(decoded.error)}`);
    }

    if (decoded.value.kind !== handler.eventType) {
      return failed(`the event is a ${decoded.value.kind}; the handler takes ${handler.eventType}`);
    }

    try {
      await handler.handler(decoded.value);

      return HANDLED;
    } catch (cause) {
      const record = thrownRecord(cause);

      return failed(`${record.type}: ${record.message}`);
    }
  };
}

function failed(cause: string): HandlerEventAnswer {
  return { kind: "failed", cause };
}

/** The text of a decode refusal. Pure. */
export function decodeRefusalText(error: EventDecodeError): string {
  switch (error.kind) {
    case "notAnEvent":
      return "the items are empty, or the first item is not a text";
    case "unknownEventType":
      return `the event type ${error.tag} is not in this contract version`;
    case "malformedEvent":
      return `${error.event} at [${error.path.join(".")}]: ${error.cause.kind}`;
    default:
      return unreachable(error, "decodeRefusalText");
  }
}

/**
 * The hook dispatcher of the start hooks of the handler child: the hook at the call's index runs; a throw or a
 * rejection reaches the settlement, and the core ends the child with 173.
 */
export function hookDispatch(
  hooks: readonly HandlerChildHookMember[],
  raise: RaiseDefect = raiseDefect,
): (call: StartHookCall) => Promise<StartHookEnd> {
  return async (call) => {
    const member = hooks[call.index] ?? raise(new ClientDefect({ subject: "hookDispatch", rule: `the plan has a start hook at index ${String(call.index)}` }));

    await member.hook();

    return "ended";
  };
}
