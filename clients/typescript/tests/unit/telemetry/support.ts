// The OpenTelemetry SDK of the telemetry tests: an in-memory span exporter, an in-memory metric reader, the W3C trace
// context propagator and the `AsyncLocalStorage` context manager, set as the globals of the API, and the API module
// as the package's `OtelApi`.

import * as otel from "@opentelemetry/api";
import { AsyncLocalStorageContextManager } from "@opentelemetry/context-async-hooks";
import { W3CTraceContextPropagator } from "@opentelemetry/core";
import { AggregationTemporality, MeterProvider, MetricReader, type ResourceMetrics } from "@opentelemetry/sdk-metrics";
import { InMemorySpanExporter, type ReadableSpan, SimpleSpanProcessor, TracerProvider } from "@opentelemetry/sdk-trace";

import { type OtelApi, requireOtelApi } from "../../../horsies/src/telemetry/otel-api.ts";
import { TelemetryWarnings } from "../../../horsies/src/telemetry/warnings.ts";

/** The API module as the package reads it: the assignment fails typecheck if a member does not fit. */
export const API: OtelApi = otel;

/**
 * The API as the package loads it (`require`, so the CommonJS build), while the tests set the SDK through the ES
 * module build, as an app does: the two builds share the API's globals.
 */
export function loadedApi(): OtelApi {
  const loaded = requireOtelApi();

  if (loaded.kind !== "loaded") {
    throw new Error(`the OpenTelemetry API does not load: ${loaded.cause}`);
  }

  return loaded.api;
}

/** A metric reader that gives what the meter provider holds when the test asks. */
class TestMetricReader extends MetricReader {
  constructor() {
    super({ aggregationTemporalitySelector: () => AggregationTemporality.CUMULATIVE });
  }

  protected override async onForceFlush(): Promise<void> {
    return undefined;
  }

  protected override async onShutdown(): Promise<void> {
    return undefined;
  }
}

/** The SDK of one test. */
export type TestSdk = {
  readonly provider: TracerProvider;
  /** The spans that ended, in end order. */
  spans(): readonly ReadableSpan[];
  /** The metrics that the meter provider holds. */
  metrics(): Promise<ResourceMetrics>;
  /** Remove every global of the API. */
  remove(): void;
};

/** Set an SDK as the globals of the API. Call `remove` after the test. */
export function installSdk(): TestSdk {
  const exporter = new InMemorySpanExporter();
  const provider = new TracerProvider({ spanProcessors: [new SimpleSpanProcessor({ exporter })] });
  const reader = new TestMetricReader();
  const meters = new MeterProvider({ readers: [reader] });

  otel.context.setGlobalContextManager(new AsyncLocalStorageContextManager().enable());
  otel.propagation.setGlobalPropagator(new W3CTraceContextPropagator());
  otel.trace.setGlobalTracerProvider(provider);
  otel.metrics.setGlobalMeterProvider(meters);

  return {
    provider,
    spans: () => exporter.getFinishedSpans(),
    metrics: async () => {
      const collected = await reader.collect();

      return collected.resourceMetrics;
    },
    remove: () => {
      otel.trace.disable();
      otel.metrics.disable();
      otel.propagation.disable();
      otel.context.disable();
    },
  };
}

/** Warnings that the test reads. */
export function recordedWarnings(): { readonly warnings: TelemetryWarnings; readonly lines: string[] } {
  const lines: string[] = [];

  return { warnings: new TelemetryWarnings((line) => lines.push(line)), lines };
}

/** The one span of `spans` named `name`. */
export function onlySpan(spans: readonly ReadableSpan[], name: string): ReadableSpan {
  const named = spans.filter((span) => span.name === name);
  const [first] = named;

  if (named.length !== 1 || first === undefined) {
    throw new Error(`one span named ${name} ended, not ${String(named.length)}`);
  }

  return first;
}
