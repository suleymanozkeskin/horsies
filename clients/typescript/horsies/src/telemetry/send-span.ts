// The OpenTelemetry send observer: one span of kind PRODUCER around the database calls of each send call
// (`send <task_name>`, a resend of a transient failure included, so every call carries one carrier), the carrier that
// the global propagator injects for that span, and one point of each producer metric. A rerun opens no span: it gets
// the carrier of the caller's active context.
//
// - The parent of the span is the caller's active context.
// - A result that failed sets the status ERROR and `error.type` to the error code; a success leaves the status unset.
// - A throw of the send path (a defect) ends the span with ERROR and `error.type` = the thrown value's type
//   (`thrownRecord`), records no metric, and passes on.
// - Telemetry never changes the send: a throw of the user's SDK at the open gives no span, no carrier and no metric.
//   A throw at the open or the end is a span not made: one WARNING record per process on `horsies.telemetry`
//   (`not-made.ts`). A throw of a metric or of the carrier of a rerun is one warning per process (`TelemetryWarnings`).

import type { SendCallResult, SendObserver, SendSpanFacts } from "../app/observers.ts";
import { thrownRecord } from "../child/thrown-error.ts";
import type { TraceArgument } from "../native/values/records.ts";
import {
  ERROR_TYPE_KEY,
  MESSAGING_DESTINATION_NAME_KEY,
  MESSAGING_MESSAGE_ID_KEY,
  MESSAGING_OPERATION_NAME_KEY,
  MESSAGING_OPERATION_TYPE_KEY,
  MESSAGING_SYSTEM,
  MESSAGING_SYSTEM_KEY,
  QUEUE_KEY,
  SEND_OPERATION,
  TASK_ID_KEY,
  TASK_NAME_KEY,
} from "./attributes.ts";
import { NO_CARRIER, sendTraceOf } from "./carrier.ts";
import type { SpansNotMade } from "./not-made.ts";
import { INSTRUMENTATION_SCOPE, INSTRUMENTATION_SCOPE_VERSION, type OtelApi, type OtelSpan } from "./otel-api.ts";
import { recordSendPoint, type SendPoint, type SendResultLabel } from "./producer-metrics.ts";
import type { TelemetryWarnings } from "./warnings.ts";

/** The clock of the send duration: milliseconds on the monotonic clock. */
export type MonotonicClock = () => number;

/** What the send observer reads. */
export type SendTelemetryDeps = {
  readonly api: OtelApi;
  readonly warnings: TelemetryWarnings;
  readonly notMade: SpansNotMade;
  readonly clock: MonotonicClock;
};

/** The span of one send, or none when the user's SDK threw at the open. */
type OpenedSend = { readonly kind: "open"; readonly span: OtelSpan; readonly trace: TraceArgument } | { readonly kind: "notOpen" };

/** How the database calls of one send call ended. */
type SendCallEnd = { readonly kind: "returned"; readonly result: SendResultLabel } | { readonly kind: "threw"; readonly errorType: string };

const MILLISECONDS_PER_SECOND = 1000;

/** The send observer of an app with OpenTelemetry tracing. */
export function otelSendObserver(deps: SendTelemetryDeps): SendObserver {
  return Object.freeze({
    observeSend: async (
      facts: SendSpanFacts,
      run: (trace: TraceArgument) => Promise<SendCallResult>,
    ): Promise<SendCallResult> => observedSend(deps, facts, run),
    activeCarrier: (): TraceArgument => activeCarrier(deps),
  });
}

async function observedSend(
  deps: SendTelemetryDeps,
  facts: SendSpanFacts,
  run: (trace: TraceArgument) => Promise<SendCallResult>,
): Promise<SendCallResult> {
  const opened = openSendSpan(deps, facts);

  if (opened.kind === "notOpen") {
    return run(NO_CARRIER);
  }

  const started = deps.clock();
  let result: SendCallResult;

  try {
    result = await run(opened.trace);
  } catch (cause) {
    endSendSpan(deps, opened.span, facts.taskId, { kind: "threw", errorType: thrownRecord(cause).type });

    throw cause;
  }

  const seconds = (deps.clock() - started) / MILLISECONDS_PER_SECOND;
  const label: SendResultLabel = result.ok ? { kind: "sent" } : { kind: "failed", code: result.error.code };

  endSendSpan(deps, opened.span, facts.taskId, { kind: "returned", result: label });
  recordPoint(deps, { taskName: facts.taskName, queueName: facts.queueName, result: label, seconds });

  return result;
}

/** The span of the send of `facts`, a child of the active context, and its carrier. */
function openSendSpan(deps: SendTelemetryDeps, facts: SendSpanFacts): OpenedSend {
  const { api } = deps;

  try {
    const parent = api.context.active();

    const span = api.trace.getTracer(INSTRUMENTATION_SCOPE, INSTRUMENTATION_SCOPE_VERSION).startSpan(
      `send ${facts.taskName}`,
      {
        kind: api.SpanKind.PRODUCER,
        attributes: {
          [MESSAGING_SYSTEM_KEY]: MESSAGING_SYSTEM,
          [MESSAGING_OPERATION_TYPE_KEY]: SEND_OPERATION,
          [MESSAGING_OPERATION_NAME_KEY]: SEND_OPERATION,
          [MESSAGING_DESTINATION_NAME_KEY]: facts.queueName,
          [MESSAGING_MESSAGE_ID_KEY]: facts.taskId,
          [TASK_ID_KEY]: facts.taskId,
          [TASK_NAME_KEY]: facts.taskName,
          [QUEUE_KEY]: facts.queueName,
        },
      },
      parent,
    );

    return { kind: "open", span, trace: sendTraceOf(api, api.trace.setSpan(parent, span)) };
  } catch (cause) {
    deps.notMade.report({ kind: "send", taskId: facts.taskId }, cause);

    return { kind: "notOpen" };
  }
}

/** End `span` of a send of task `taskId` by how the send call ended. */
function endSendSpan(deps: SendTelemetryDeps, span: OtelSpan, taskId: string, end: SendCallEnd): void {
  try {
    switch (end.kind) {
      case "returned":
        if (end.result.kind === "failed") {
          markError(deps.api, span, end.result.code);
        }

        break;
      case "threw":
        markError(deps.api, span, end.errorType);
        break;
      default: {
        const unexpected: never = end;

        throw new TypeError(`endSendSpan: an end outside its type: ${String(unexpected)}`);
      }
    }

    span.end();
  } catch (cause) {
    deps.notMade.report({ kind: "send", taskId }, cause);
  }
}

function recordPoint(deps: SendTelemetryDeps, point: SendPoint): void {
  try {
    recordSendPoint(deps.api, point);
  } catch (cause) {
    deps.warnings.warnOnce("producerMetric", cause);
  }
}

/** The carrier of the caller's active context, for a rerun, which opens no span. */
function activeCarrier(deps: SendTelemetryDeps): TraceArgument {
  try {
    return sendTraceOf(deps.api, deps.api.context.active());
  } catch (cause) {
    deps.warnings.warnOnce("rerunCarrier", cause);

    return NO_CARRIER;
  }
}

/** Set the status ERROR and `error.type` on `span`. */
export function markError(api: OtelApi, span: OtelSpan, errorType: string): void {
  span.setAttribute(ERROR_TYPE_KEY, errorType);
  span.setStatus({ code: api.SpanStatusCode.ERROR });
}
