// The OpenTelemetry SDK of the telemetry tests: an in-memory span exporter, an in-memory metric reader, the W3C trace
// context propagator and the `AsyncLocalStorage` context manager, set as the globals of the API, and the API module
// as the package's `OtelApi`.

import * as otel from "@opentelemetry/api";
import { AsyncLocalStorageContextManager } from "@opentelemetry/context-async-hooks";
import { W3CTraceContextPropagator } from "@opentelemetry/core";
import { AggregationTemporality, MeterProvider, MetricReader, type ResourceMetrics } from "@opentelemetry/sdk-metrics";
import {
  type IdGenerator,
  InMemorySpanExporter,
  type ReadableSpan,
  SimpleSpanProcessor,
  TracerProvider,
} from "@opentelemetry/sdk-trace";

import { type NotMadeSpan, SpansNotMade } from "../../../horsies/src/telemetry/not-made.ts";
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

/** The ids that an SDK gives: random lower-case hex (the SDK's own), or upper-case hex, which the API also takes. */
export type SdkIds = "sdkIds" | "upperCaseIds";

/** An id generator that gives upper-case hex ids, each new. */
export function upperCaseIdGenerator(): IdGenerator {
  let next = 0;

  const hexOf = (digits: number): string => {
    next += 1;

    return next.toString(16).toUpperCase().padStart(digits, "A");
  };

  return { generateTraceId: () => hexOf(32), generateSpanId: () => hexOf(16) };
}

/** A tracer provider that exports to `exporter`, with the ids of `ids`. */
function providerOf(ids: SdkIds, exporter: InMemorySpanExporter): TracerProvider {
  const spanProcessors = [new SimpleSpanProcessor({ exporter })];

  switch (ids) {
    case "sdkIds":
      return new TracerProvider({ spanProcessors });
    case "upperCaseIds":
      return new TracerProvider({ spanProcessors, idGenerator: upperCaseIdGenerator() });
    default: {
      const unexpected: never = ids;

      throw new TypeError(`providerOf: ids outside their type: ${String(unexpected)}`);
    }
  }
}

/** The members of an API module that set and remove its globals: the module of any minor of the API 1.x has them. */
export interface ApiGlobals {
  readonly context: { setGlobalContextManager(manager: never): boolean; disable(): void };
  readonly propagation: { setGlobalPropagator(propagator: never): boolean; disable(): void };
  readonly trace: { setGlobalTracerProvider(provider: never): boolean; disable(): void };
  readonly metrics: { setGlobalMeterProvider(provider: never): boolean; disable(): void };
}

/**
 * Set an SDK as the globals of the API module `globals` (the installed API by default; a test of another minor gives
 * that minor's module). Call `remove` after the test.
 */
export function installSdk(ids: SdkIds = "sdkIds", globals: ApiGlobals = otel): TestSdk {
  const exporter = new InMemorySpanExporter();
  const provider = providerOf(ids, exporter);
  const reader = new TestMetricReader();
  const meters = new MeterProvider({ readers: [reader] });

  // SAFETY: the parameters of `ApiGlobals` are `never` so that the module of any minor fits; each minor's setter takes
  // the objects of the SDK that is installed here (the same shape in every minor of the API 1.x).
  globals.context.setGlobalContextManager(new AsyncLocalStorageContextManager().enable() as never);
  // SAFETY: as above.
  globals.propagation.setGlobalPropagator(new W3CTraceContextPropagator() as never);
  // SAFETY: as above.
  globals.trace.setGlobalTracerProvider(provider as never);
  // SAFETY: as above.
  globals.metrics.setGlobalMeterProvider(meters as never);

  return {
    provider,
    spans: () => exporter.getFinishedSpans(),
    metrics: async () => {
      const collected = await reader.collect();

      return collected.resourceMetrics;
    },
    remove: () => {
      globals.trace.disable();
      globals.metrics.disable();
      globals.propagation.disable();
      globals.context.disable();
    },
  };
}

/** Warnings that the test reads. */
export function recordedWarnings(): { readonly warnings: TelemetryWarnings; readonly lines: string[] } {
  const lines: string[] = [];

  return { warnings: new TelemetryWarnings((line) => lines.push(line)), lines };
}

/** The id of a span that was not made: the workflow id of a start, the task id of the other kinds. */
function notMadeId(span: NotMadeSpan): string {
  switch (span.kind) {
    case "workflowStart":
      return span.workflowId;
    case "send":
    case "attempt":
    case "seatAttempt":
    case "workerEnded":
      return span.taskId;
    default: {
      const unexpected: never = span;

      throw new TypeError(`notMadeId: a span outside its type: ${String(unexpected)}`);
    }
  }
}

/** Reports of spans that were not made that the test reads: each written one as `<kind> <id> <cause>`. */
export function recordedNotMade(): { readonly notMade: SpansNotMade; readonly written: string[] } {
  const written: string[] = [];

  return {
    notMade: new SpansNotMade((span, cause) => {
      written.push(`${span.kind} ${notMadeId(span)} ${cause}`);
    }),
    written,
  };
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

/** The text of the throw of a span that cannot end. */
export const SPAN_END_BROKEN = "the span end is broken";

/** `span` with an `end` that throws; every other member is the span's own. */
function withBrokenEnd<T extends object>(span: T): T {
  return new Proxy(span, {
    get(target, member) {
      if (member === "end") {
        return () => {
          throw new Error(SPAN_END_BROKEN);
        };
      }

      const value: unknown = Reflect.get(target, member);

      return value instanceof Function ? value.bind(target) : value;
    },
  });
}

/** The API of the installed SDK whose spans throw at their end. */
export function apiWithBrokenSpanEnd(): OtelApi {
  const api = loadedApi();

  return {
    ...api,
    trace: {
      ...api.trace,
      getTracer: (name, version) => {
        const tracer = api.trace.getTracer(name, version);

        return { startSpan: (spanName, options, context) => withBrokenEnd(tracer.startSpan(spanName, options, context)) };
      },
    },
  };
}
