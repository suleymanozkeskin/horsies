// The OpenTelemetry start observer: one span of kind PRODUCER around the core's start of each `app.start` and
// `app.retryStart` call (`start <workflow_name>`), and the carrier that the global propagator injects for that span.
// The core stores the carrier on every row of the workflow tree, so the attempt spans of the nodes join the trace of
// the start. The span and its attributes are those of the Python and Rust clients.
//
// - The parent of the span is the caller's active context.
// - A start error sets the status ERROR and `error.type` to its code; a success leaves the status unset.
// - A throw of the start path (a defect) ends the span with ERROR and `error.type` = the thrown value's type
//   (`thrownRecord`), and passes on.
// - A start refused before the core (an unlisted workflow, a workflow id that is not a UUID, a build that fails its
//   checks) calls no observer, so it opens no span, as in the Rust client.
// - Telemetry never changes the start: a throw of the user's SDK at the open gives no span and no carrier; a throw at
//   the end is written once per process (`TelemetryWarnings`).

import { thrownRecord } from "../child/thrown-error.ts";
import type { TraceArgument } from "../native/values/records.ts";
import { unreachable } from "../wire/unreachable.ts";
import type { StartCallResult, StartObserver, StartSpanFacts } from "../workflow/start.ts";
import {
  MESSAGING_OPERATION_NAME_KEY,
  MESSAGING_OPERATION_TYPE_KEY,
  MESSAGING_SYSTEM,
  MESSAGING_SYSTEM_KEY,
  SEND_OPERATION,
  START_OPERATION,
  WORKFLOW_ID_KEY,
  WORKFLOW_NAME_KEY,
} from "./attributes.ts";
import { NO_CARRIER, sendTraceOf } from "./carrier.ts";
import { INSTRUMENTATION_SCOPE, type OtelApi, type OtelSpan } from "./otel-api.ts";
import { markError } from "./send-span.ts";
import type { TelemetryWarnings } from "./warnings.ts";

/** What the start observer reads. */
export type StartTelemetryDeps = {
  readonly api: OtelApi;
  readonly warnings: TelemetryWarnings;
};

/** The span of one start, or none when the user's SDK threw at the open. */
type OpenedStart = { readonly kind: "open"; readonly span: OtelSpan; readonly trace: TraceArgument } | { readonly kind: "notOpen" };

/** How the core's start of one start call ended. */
type StartCallEnd =
  | { readonly kind: "started" }
  | { readonly kind: "failed"; readonly code: string }
  | { readonly kind: "threw"; readonly errorType: string };

/** The start observer of an app with OpenTelemetry tracing. */
export function otelStartObserver(deps: StartTelemetryDeps): StartObserver {
  return Object.freeze({
    observeStart: async <T extends StartCallResult>(facts: StartSpanFacts, run: (trace: TraceArgument) => Promise<T>): Promise<T> =>
      observedStart(deps, facts, run),
  });
}

async function observedStart<T extends StartCallResult>(
  deps: StartTelemetryDeps,
  facts: StartSpanFacts,
  run: (trace: TraceArgument) => Promise<T>,
): Promise<T> {
  const opened = openStartSpan(deps, facts);

  switch (opened.kind) {
    case "notOpen":
      return run(NO_CARRIER);
    case "open":
      break;
    default:
      return unreachable(opened, "observedStart");
  }

  let result: T;

  try {
    result = await run(opened.trace);
  } catch (cause) {
    endStartSpan(deps, opened.span, { kind: "threw", errorType: thrownRecord(cause).type });

    throw cause;
  }

  endStartSpan(deps, opened.span, result.ok ? { kind: "started" } : { kind: "failed", code: result.error.code });

  return result;
}

/** The span of the start of `facts`, a child of the active context, and its carrier. */
function openStartSpan(deps: StartTelemetryDeps, facts: StartSpanFacts): OpenedStart {
  const { api } = deps;

  try {
    const parent = api.context.active();

    const span = api.trace.getTracer(INSTRUMENTATION_SCOPE).startSpan(
      `${START_OPERATION} ${facts.workflowName}`,
      {
        kind: api.SpanKind.PRODUCER,
        attributes: {
          [MESSAGING_SYSTEM_KEY]: MESSAGING_SYSTEM,
          [MESSAGING_OPERATION_TYPE_KEY]: SEND_OPERATION,
          [MESSAGING_OPERATION_NAME_KEY]: START_OPERATION,
          [WORKFLOW_ID_KEY]: facts.workflowId,
          [WORKFLOW_NAME_KEY]: facts.workflowName,
        },
      },
      parent,
    );

    return { kind: "open", span, trace: sendTraceOf(api, api.trace.setSpan(parent, span)) };
  } catch (cause) {
    deps.warnings.warnOnce("startSpanOpen", cause);

    return { kind: "notOpen" };
  }
}

/** End `span` by how the core's start ended. */
function endStartSpan(deps: StartTelemetryDeps, span: OtelSpan, end: StartCallEnd): void {
  try {
    switch (end.kind) {
      case "started":
        break;
      case "failed":
        markError(deps.api, span, end.code);
        break;
      case "threw":
        markError(deps.api, span, end.errorType);
        break;
      default:
        unreachable(end, "endStartSpan");
    }

    span.end();
  } catch (cause) {
    deps.warnings.warnOnce("startSpanEnd", cause);
  }
}
