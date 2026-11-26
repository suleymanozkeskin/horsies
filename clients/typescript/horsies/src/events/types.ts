// The types of the event registration: the event of one type, a handler of it, its options, and a start hook of the
// handler child. The event types themselves are generated from the core's event catalog (`_catalog/events.ts`).

import type { AnyEvent } from "../_catalog/events.ts";
import type { EventTypeName } from "../_catalog/worker_manifest.ts";

/** The event of the type named `T`: the member of `AnyEvent` whose `kind` is `T`. */
export type EventOf<T extends EventTypeName> = Extract<AnyEvent, { readonly kind: T }>;

/**
 * A handler of the events of one type, sync or async. It runs in the handler child of a worker, never in the worker
 * parent, one event at a time, in the order of the worker. A throw or a rejection is counted and logged with the
 * handler's name; the handler takes its next event.
 */
export type EventHandler<T extends EventTypeName> = (event: EventOf<T>) => void | Promise<void>;

/**
 * The options of a handler. `name`: 1 to 255 characters, not `(unknown)`, once per app. It is the `handler` label of
 * the handler metrics (`horsies.telemetry.handler.errors`, `horsies.telemetry.handler.dropped`).
 */
export type EventHandlerOptions = { readonly name: string };

/** A start hook of the handler child, sync or async. */
export type HandlerChildStartHook = () => void | Promise<void>;
