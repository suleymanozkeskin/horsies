// The event registration of an app: `app.onEvent(type, { name }, handler)` and `app.onHandlerChildStart(hook)`. Each
// call records the member and calls nothing; it never throws. A refused registration goes to the refusal sink (area
// `events`), which the `check` command and the worker start read: a worker refuses to start while it holds one.
//
// The checks of `onEvent`, in order, each with the code of the Python client:
// - `HRS-222`: the type is not an event type of the event contract (a JavaScript caller; TypeScript refuses it);
// - `HRS-223`: the handler is not a function;
// - `HRS-225`: the options have no name that is a text, or the name is empty, longer than `MAX_HANDLER_NAME_CHARS`
//   characters, or `(unknown)`;
// - `HRS-224`: an earlier handler has the name;
// - `HRS-226`: the app has `MAX_EVENT_HANDLERS` handlers.
//
// `onHandlerChildStart` refuses a hook that is not a function with `HRS-214`, the code of the Python client's
// `on_handler_child_start` for the same input. The checks read a JavaScript caller's value too: a value outside the
// static types gives the refusal, never a `TypeError`.
//
// Once the handler child serves, a new member is a defect: the members of the child are fixed.

import { ErrorCode } from "../_catalog/error_codes.ts";
import { isObjectLike, isString } from "../codec/values.ts";
import { type AnyEvent, EVENT_TYPES } from "../_catalog/events.ts";
import type { EventTypeName } from "../_catalog/worker_manifest.ts";
import type { AppContext } from "../app/context.ts";
import { refusal, type Refusal } from "../app/refusal.ts";
import type { HandlerRegistration } from "../app/telemetry.ts";
import type { RaiseDefect } from "../child/state.ts";
import { raiseDefect } from "../native/call/defect.ts";
import { ClientDefect } from "../thrown-errors.ts";
import { unreachable } from "../wire/unreachable.ts";
import { MAX_EVENT_HANDLERS, MAX_HANDLER_NAME_CHARS, UNKNOWN_HANDLER_NAME } from "./bounds.ts";
import type { HandlerChildStartHook } from "./types.ts";

/** A handler with the event type erased: the dispatch gives it only events of its type. */
export type ErasedEventHandler = (event: AnyEvent) => void | Promise<void>;

/** One registered handler: its name, the event type that it takes, and its function. */
export type RegisteredHandler = {
  readonly name: string;
  readonly eventType: EventTypeName;
  readonly handler: ErasedEventHandler;
};

/** One `onHandlerChildStart` hook, with the name that its failure gives. */
export type HandlerChildHookMember = { readonly name: string; readonly hook: HandlerChildStartHook };

/** The event members of an app. `phase` is `serving` once the handler child serves. */
type EventMembers = {
  readonly handlers: RegisteredHandler[];
  readonly hooks: HandlerChildHookMember[];
  phase: "open" | "serving";
};

/** The event members of a served handler child, in registration order. */
export type EventMembersView = {
  readonly handlers: readonly RegisteredHandler[];
  readonly hooks: readonly HandlerChildHookMember[];
};

const members = new WeakMap<AppContext, EventMembers>();

function membersOf(context: AppContext): EventMembers {
  const kept = members.get(context);

  if (kept !== undefined) {
    return kept;
  }

  const created: EventMembers = { handlers: [], hooks: [], phase: "open" };

  members.set(context, created);

  return created;
}

/** The members of `context`, open for a new member. A new member of a served handler child is a defect. */
function openMembers(context: AppContext, subject: string, raise: RaiseDefect): EventMembers {
  const kept = membersOf(context);

  switch (kept.phase) {
    case "open":
      return kept;
    case "serving":
      return raise(new ClientDefect({ subject, rule: `${subject} is called before cli.run serves the handler child` }));
    default:
      return unreachable(kept.phase, "openMembers");
  }
}

/**
 * Register `handler` as `name` for the events of `eventType`, or record the refusal of the registration. Calls
 * nothing. A defect goes to `raise` (the defect rule).
 */
export function onEventFor(
  context: AppContext,
  eventType: string,
  options: { readonly name: string },
  handler: object,
  raise: RaiseDefect = raiseDefect,
): void {
  const open = openMembers(context, "app.onEvent", raise);
  const checked = checkedHandler(open.handlers, eventType, nameOfOptions(options), handler);

  switch (checked.kind) {
    case "accepted":
      open.handlers.push(checked.registered);
      break;
    case "refused":
      context.refusals.record("events", checked.refusal);
      break;
    default:
      unreachable(checked, "onEventFor");
  }
}

/** The name of a registration's options: a text, or why the options give none. */
export type OptionsName = { readonly kind: "name"; readonly name: string } | { readonly kind: "noName"; readonly reason: string };

/** The name of `options`, as a JavaScript caller may pass them: an object with a `name` that is a text. Pure. */
function nameOfOptions(options: { readonly name: string }): OptionsName {
  if (!isObjectLike(options)) {
    return { kind: "noName", reason: "the options are not an object with a name" };
  }

  const name: unknown = Reflect.get(options, "name");

  return isString(name) ? { kind: "name", name } : { kind: "noName", reason: "the name is not a text" };
}

/** A registration that passed every check, or its refusal. */
type CheckedHandler =
  | { readonly kind: "accepted"; readonly registered: RegisteredHandler }
  | { readonly kind: "refused"; readonly refusal: Refusal };

/** The checks of one registration against the handlers before it, in the order of the module docs. Pure. */
export function checkedHandler(
  before: readonly RegisteredHandler[],
  eventType: string,
  optionsName: OptionsName,
  handler: object,
): CheckedHandler {
  const path = `onEvent ${String(eventType)} ${optionsName.kind === "name" ? optionsName.name : "(no name)"}`;
  const known = EVENT_TYPES.find((type) => type === eventType);

  if (known === undefined) {
    return refused(ErrorCode.EVENT_HANDLER_UNKNOWN_EVENT_TYPE, path, `${String(eventType)} is not an event type`, [
      `take one of ${EVENT_TYPES.join(", ")}`,
    ]);
  }

  if (!(handler instanceof Function)) {
    return refused(ErrorCode.EVENT_HANDLER_INVALID_SIGNATURE, path, "the handler is not a function", [
      "pass a function that takes one event",
    ]);
  }

  const named = nameRule(optionsName);

  if (named.kind === "refused") {
    return refused(ErrorCode.EVENT_HANDLER_INVALID_NAME, path, named.reason, [
      `give a name of 1 to ${String(MAX_HANDLER_NAME_CHARS)} characters, other than ${UNKNOWN_HANDLER_NAME}`,
    ]);
  }

  const name = named.name;

  if (before.some((earlier) => earlier.name === name)) {
    return refused(ErrorCode.EVENT_HANDLER_DUPLICATE_NAME, path, "an earlier handler has the name", ["give each handler its own name"]);
  }

  if (before.length >= MAX_EVENT_HANDLERS) {
    return refused(ErrorCode.EVENT_HANDLER_LIMIT, path, `the app has ${String(MAX_EVENT_HANDLERS)} event handlers, the most`, [
      "let one handler take more work",
    ]);
  }

  // SAFETY: the handler is a function, and the dispatch calls it only with events of `known`, its declared type.
  const erased = handler as ErasedEventHandler;

  return { kind: "accepted", registered: Object.freeze({ name, eventType: known, handler: erased }) };
}

/** How a handler name meets the core's name rule: the accepted name, or why not. */
type NameRule = { readonly kind: "accepted"; readonly name: string } | { readonly kind: "refused"; readonly reason: string };

/** Whether the name of the options meets the core's name rule, and why not. Characters are Unicode scalar values. Pure. */
function nameRule(optionsName: OptionsName): NameRule {
  if (optionsName.kind === "noName") {
    return { kind: "refused", reason: optionsName.reason };
  }

  const { name } = optionsName;
  const characters = [...name].length;

  if (characters === 0) {
    return { kind: "refused", reason: "the name is empty" };
  }

  if (name === UNKNOWN_HANDLER_NAME) {
    return { kind: "refused", reason: `the name is ${UNKNOWN_HANDLER_NAME}, the label value of a name outside the declared handlers` };
  }

  if (characters > MAX_HANDLER_NAME_CHARS) {
    return { kind: "refused", reason: `the name has ${String(characters)} characters; the most is ${String(MAX_HANDLER_NAME_CHARS)}` };
  }

  return { kind: "accepted", name };
}

function refused(code: Refusal["code"], path: string, message: string, help: readonly string[]): CheckedHandler {
  return { kind: "refused", refusal: refusal(code, path, message, help) };
}

/**
 * Add a start hook of the handler child to `context`, or record the refusal `HRS-214` of a hook that is not a
 * function. Calls nothing. A defect goes to `raise` (the defect rule).
 */
export function onHandlerChildStartFor(context: AppContext, hook: HandlerChildStartHook, raise: RaiseDefect = raiseDefect): void {
  const open = openMembers(context, "app.onHandlerChildStart", raise);
  const position = `onHandlerChildStart #${String(open.hooks.length + 1)}`;

  if (!(hook instanceof Function)) {
    context.refusals.record(
      "events",
      refusal(ErrorCode.CONFIG_INVALID_CHILD_HOOK, position, "the hook is not a function", [
        "pass a function with no parameters, sync or async",
      ]),
    );

    return;
  }

  open.hooks.push(Object.freeze({ name: `${position} ${hook.name === "" ? "(anonymous)" : hook.name}`, hook }));
}

/** The name and the event type of each handler of `context`, in registration order: the manifest's `handlers`. */
export function registeredHandlers(context: AppContext): readonly HandlerRegistration[] {
  return Object.freeze(membersOf(context).handlers.map(({ name, eventType }) => Object.freeze({ name, eventType })));
}

/** Close the event members of `context` for the serve of the handler child, and give them. */
export function servedEventMembers(context: AppContext): EventMembersView {
  const kept = membersOf(context);

  kept.phase = "serving";

  return Object.freeze({ handlers: Object.freeze([...kept.handlers]), hooks: Object.freeze([...kept.hooks]) });
}
