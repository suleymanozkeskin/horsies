// The span of each attempt that the worker ended, made in the handler child: an isolated task killed at its timeout,
// a shared child killed for a blocked loop or past its deadlines, a child lost with its exit seen. The task child of
// such an attempt is gone, so the span that it opened is never exported.
//
// - `run <task_name>`, kind CONSUMER, as the span of a child; the parent from the stored carrier by the parentage.
// - It starts when the task reached its child and ends when the end reached the worker: high-resolution times from
//   the epoch microseconds of the worker's clock.
// - Status ERROR with `error.type` = the code of the attempt row; the attributes of the span of a child, with the kind
//   of the child as `horsies.task.execution`, and `horsies.span.source` = `worker`. A workflow node that the handler
//   child could not read gives no workflow attribute.
// - The maker never throws: a throw of the user's SDK is a span not made: one WARNING record per process on
//   `horsies.telemetry` (`not-made.ts`), the form of the handler child's own report of a span that it could not hand
//   to JavaScript.

import type { EndedAttemptRecord, EndedWorkflowRecord } from "../native/handler-child/addon.ts";
import { unreachable } from "../wire/unreachable.ts";
import { type AttemptSpanDeps, runAttributes, spanPlace, workflowAttributes } from "./attempt-span.ts";
import { RUN_OPERATION, SPAN_SOURCE_KEY, WORKER_SPAN_SOURCE } from "./attributes.ts";
import {
  INSTRUMENTATION_SCOPE,
  INSTRUMENTATION_SCOPE_VERSION,
  type OtelAttributes,
  type OtelHighResolutionTime,
} from "./otel-api.ts";
import { markError } from "./send-span.ts";

/** The span maker of a handler child: the one of the user's SDK, or none. */
export type EndedSpans =
  | { readonly kind: "noSpans" }
  | { readonly kind: "makeSpans"; readonly make: (attempt: EndedAttemptRecord) => void };

/** The handler child makes no span of an attempt that the worker ended. */
export const NO_ENDED_SPANS: EndedSpans = Object.freeze({ kind: "noSpans" });

const MICROSECONDS_PER_SECOND = 1_000_000n;
const NANOSECONDS_PER_MICROSECOND = 1000n;

/** The high-resolution time of `micros` epoch microseconds: seconds and nanoseconds, with no loss. Pure. */
export function highResolutionTimeOf(micros: bigint): OtelHighResolutionTime {
  const remainder = ((micros % MICROSECONDS_PER_SECOND) + MICROSECONDS_PER_SECOND) % MICROSECONDS_PER_SECOND;
  const seconds = (micros - remainder) / MICROSECONDS_PER_SECOND;

  return [Number(seconds), Number(remainder * NANOSECONDS_PER_MICROSECOND)];
}

/** The workflow attributes of the node of an ended attempt; none when the handler child has no node. Pure. */
function endedWorkflowAttributes(workflow: EndedWorkflowRecord): OtelAttributes {
  switch (workflow.kind) {
    case "known":
      return workflowAttributes(workflow.workflow);
    case "workflowNotKnown":
      return {};
    default:
      return unreachable(workflow, "endedWorkflowAttributes");
  }
}

/** The span maker of a handler child of an app with OpenTelemetry tracing. */
export function otelEndedSpans(deps: AttemptSpanDeps): EndedSpans {
  return Object.freeze({
    kind: "makeSpans",
    make: (attempt: EndedAttemptRecord): void => {
      try {
        makeEndedSpan(deps, attempt);
      } catch (cause) {
        deps.notMade.report({ kind: "workerEnded", taskId: attempt.taskId }, cause);
      }
    },
  });
}

/** Make and end the span of `attempt`. May throw what the user's SDK throws. */
function makeEndedSpan(deps: AttemptSpanDeps, attempt: EndedAttemptRecord): void {
  const { api } = deps;
  const place = spanPlace(api, deps.parentage, attempt.carrier);

  const attributes: OtelAttributes = {
    ...runAttributes(attempt),
    ...endedWorkflowAttributes(attempt.workflow),
    [SPAN_SOURCE_KEY]: WORKER_SPAN_SOURCE,
  };

  const span = api.trace.getTracer(INSTRUMENTATION_SCOPE, INSTRUMENTATION_SCOPE_VERSION).startSpan(
    `${RUN_OPERATION} ${attempt.taskName}`,
    {
      kind: api.SpanKind.CONSUMER,
      attributes,
      links: place.links,
      startTime: highResolutionTimeOf(attempt.startedAtMicros),
    },
    place.parent,
  );

  markError(api, span, attempt.errorCode);
  span.end(highResolutionTimeOf(attempt.endedAtMicros));
}
