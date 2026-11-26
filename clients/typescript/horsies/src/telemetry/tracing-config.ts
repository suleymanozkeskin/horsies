// The tracing setting of an app (`tracing` of `Horsies.create`): OpenTelemetry spans and producer metrics, or none.
// The defaults of the optional fields are the Python client's (`horsies.core.models.tracing`).

import { unreachable } from "../wire/unreachable.ts";

/**
 * How the span of an attempt relates to the span that sent its task:
 * - `childOfSend`: the attempt span is a child of the send span; the send and its attempts are one trace.
 * - `linkedToSend`: the attempt span starts a trace of its own, with a link to the send span.
 *
 * A task sent with no span gets an attempt span with no parent in both.
 */
export type AttemptParentage = "childOfSend" | "linkedToSend";

/**
 * Whether the handler child of a worker makes the span of each attempt that the worker ended (an isolated task killed
 * at its timeout, a shared child killed for a blocked loop, a child lost with its exit seen): the task child of such
 * an attempt is gone, so its own span is never exported.
 * - `makeSpans`: the handler child makes the span. The worker then starts its handler child also when the app has no
 *   event handler.
 * - `noSpans`: no span for these attempts.
 */
export type WorkerEndedSpans = "makeSpans" | "noSpans";

/**
 * The tracing of an app:
 * - `none` (the default): no span and no producer metric; the package never loads the OpenTelemetry API.
 * - `otel`: spans and producer metrics through the OpenTelemetry API (`@opentelemetry/api` 1.x, an optional peer
 *   dependency) and the SDK that the process sets. `parentage` defaults to `childOfSend`, `workerEndedSpans` to
 *   `makeSpans`.
 */
export type TracingConfigInput =
  | { readonly kind: "none" }
  | { readonly kind: "otel"; readonly parentage?: AttemptParentage; readonly workerEndedSpans?: WorkerEndedSpans };

/** The tracing of an app, with every default filled. */
export type TracingSetting =
  | { readonly kind: "none" }
  | { readonly kind: "otel"; readonly parentage: AttemptParentage; readonly workerEndedSpans: WorkerEndedSpans };

/** The tracing of an app that leaves `tracing` out. */
export const NO_TRACING: TracingSetting = Object.freeze({ kind: "none" });

/** The setting of `input`, with the defaults filled; `undefined` is `none`. Pure. */
export function tracingSettingOf(input: TracingConfigInput | undefined): TracingSetting {
  if (input === undefined) {
    return NO_TRACING;
  }

  switch (input.kind) {
    case "none":
      return NO_TRACING;
    case "otel":
      return Object.freeze({
        kind: "otel",
        parentage: input.parentage ?? "childOfSend",
        workerEndedSpans: input.workerEndedSpans ?? "makeSpans",
      });
    default:
      return unreachable(input, "tracingSettingOf");
  }
}
