// The telemetry facts of an app that the worker parent and its children read: the event handlers that the app
// registered, and its tracing. The event-registration part and the tracing part give their own source in
// `Horsies.create`; until then the app has no handler and no tracing, so the worker starts no handler child.

import type { EventTypeName } from "../_catalog/worker_manifest.ts";
import { NO_TRACING, type TracingSetting } from "../telemetry/tracing-config.ts";

/** One event handler of the app: its unique name and the one event type that it takes. */
export type HandlerRegistration = { readonly name: string; readonly eventType: EventTypeName };

/** The telemetry facts of one app. */
export interface AppTelemetry {
  /** The event handlers that the app registered, in registration order, at the time of the call. */
  handlers(): readonly HandlerRegistration[];
  readonly tracing: TracingSetting;
}

/** An app with no event handler and no tracing. */
export const NO_TELEMETRY: AppTelemetry = Object.freeze({
  handlers: (): readonly HandlerRegistration[] => [],
  tracing: NO_TRACING,
});
