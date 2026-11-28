// The OpenTelemetry SDK of the processes of a `worker_ended_spans` worker: each task child and the handler child set
// it at their start (Python `clients/python/tests/e2e/tasks/instance_worker_ended_spans.py`). Its exporter appends one
// span line per ended span to the marker `span-lines` (`span-lines.ts`), so the spans of the task children and the
// spans that the handler child makes go to one file. The W3C propagator reads the carrier that a send stored, so an
// attempt span joins the trace of its send.
//
// Only the start hooks of that instance import this module (`instance-members.ts`), so the processes of the other
// instances load no SDK.

import * as otel from "@opentelemetry/api";
import { AsyncLocalStorageContextManager } from "@opentelemetry/context-async-hooks";
import { type ExportResult, ExportResultCode, W3CTraceContextPropagator } from "@opentelemetry/core";
import { type ReadableSpan, SimpleSpanProcessor, type SpanExporter, TracerProvider } from "@opentelemetry/sdk-trace";

import { appendLine, markerErrorText } from "./markers.ts";
import { type SpanLine, SPAN_LINES_MARKER, type SpanLineStatus } from "./span-lines.ts";

const NANOSECONDS_PER_SECOND = 1_000_000_000n;

/** Nanoseconds since 1970 of a time of the API, as decimal text. Pure. */
function nanosText(time: otel.HrTime): string {
  const [seconds, nanoseconds] = time;

  return String(BigInt(seconds) * NANOSECONDS_PER_SECOND + BigInt(nanoseconds));
}

/** The name of a status code. Pure. */
function statusOf(code: otel.SpanStatusCode): SpanLineStatus {
  switch (code) {
    case otel.SpanStatusCode.UNSET:
      return "UNSET";
    case otel.SpanStatusCode.OK:
      return "OK";
    case otel.SpanStatusCode.ERROR:
      return "ERROR";
  }
}

/** A span line as the exporter writes it: the attributes as the SDK holds them, where JSON leaves out an absent one. */
type WrittenSpanLine = Omit<SpanLine, "attributes"> & { readonly attributes: otel.Attributes };

/** The line text of `span`. Pure. */
function spanLineText(span: ReadableSpan): string {
  const parentSpanId = span.parentSpanContext?.spanId;

  const line: WrittenSpanLine = {
    name: span.name,
    traceId: span.spanContext().traceId,
    spanId: span.spanContext().spanId,
    parent: parentSpanId === undefined ? { kind: "root" } : { kind: "parent", spanId: parentSpanId },
    startNanos: nanosText(span.startTime),
    endNanos: nanosText(span.endTime),
    status: statusOf(span.status.code),
    attributes: span.attributes,
  };

  return JSON.stringify(line);
}

/** Appends one span line per ended span to the marker `span-lines`. */
class SpanLineExporter implements SpanExporter {
  export(spans: ReadableSpan[], resultCallback: (result: ExportResult) => void): void {
    for (const span of spans) {
      const appended = appendLine(SPAN_LINES_MARKER, spanLineText(span));

      if (!appended.ok) {
        resultCallback({ code: ExportResultCode.FAILED, error: new Error(`the span line of ${span.name}: ${markerErrorText(appended.error)}`) });

        return;
      }
    }

    resultCallback({ code: ExportResultCode.SUCCESS });
  }

  async shutdown(): Promise<void> {
    return undefined;
  }
}

/** Set the SDK of this process as the globals of the API: the span line exporter, the W3C propagator and the context manager. */
export function installSpanLines(): void {
  const provider = new TracerProvider({ spanProcessors: [new SimpleSpanProcessor({ exporter: new SpanLineExporter() })] });

  otel.context.setGlobalContextManager(new AsyncLocalStorageContextManager().enable());
  otel.propagation.setGlobalPropagator(new W3CTraceContextPropagator());
  otel.trace.setGlobalTracerProvider(provider);
}
