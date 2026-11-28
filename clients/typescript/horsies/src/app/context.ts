// The facts of an app that are fixed after `Horsies.create`, and the state that its registrations add.

import type { AppConfig } from "../config/app-config.ts";
import type { ImplementationParts } from "../contract/implementation.ts";
import type { SendContext } from "../send/send.ts";
import type { ListedEntry } from "./contract-list.ts";
import type { CoreAccess } from "./core-access.ts";
import type { SendObserver } from "./observers.ts";
import type { RefusalSink } from "./refusals.ts";
import type { WorkflowRegistry } from "../workflow/registry.ts";
import type { SuppressionState } from "./suppression.ts";
import type { AppTelemetry } from "./telemetry.ts";

/** The context of one app. */
export type AppContext = {
  readonly config: AppConfig;
  /** The listed contracts, in list order. */
  readonly listed: readonly ListedEntry[];
  readonly core: CoreAccess;
  readonly refusals: RefusalSink;
  readonly observer: SendObserver;
  readonly suppression: SuppressionState;
  /** What the send path reads. */
  readonly send: SendContext;
  /** The implementations, by task name. */
  readonly implementations: Map<string, ImplementationParts>;
  /** The listed workflows. */
  readonly workflows: WorkflowRegistry;
  /** The event handlers and the tracing of the app, which the worker manifest and the children read. */
  readonly telemetry: AppTelemetry;
};
