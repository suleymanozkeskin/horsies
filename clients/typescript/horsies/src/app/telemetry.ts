// The telemetry facts of an app that the worker parent and its children read: the event handlers that the app
// registered (`app.onEvent`, `src/events/register.ts`), and its tracing (`tracing` of `Horsies.create`,
// `src/telemetry/create-hook.ts`).

import type { EventTypeName } from "../_catalog/worker_manifest.ts";
import type { TracingSetting } from "../telemetry/tracing-config.ts";

/** One event handler of the app: its unique name and the one event type that it takes. */
export type HandlerRegistration = { readonly name: string; readonly eventType: EventTypeName };

/** The telemetry facts of one app. */
export interface AppTelemetry {
  /** The event handlers that the app registered, in registration order, at the time of the call. */
  handlers(): readonly HandlerRegistration[];
  readonly tracing: TracingSetting;
}
