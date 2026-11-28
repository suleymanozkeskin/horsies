// The OpenTelemetry API as the package uses it, and its load.
//
// - The API is `@opentelemetry/api` 1.x, an optional peer dependency. The package loads it only for an app with
//   `tracing: { kind: "otelTracing" }`, with a synchronous `require` from this module, because `Horsies.create` is sync.
// - The package never names a type of the API module: the interfaces below hold the members that it calls, so the
//   published types do not need the API. A test checks that the API module fits them.
// - The tracer, the meter and the propagator are the global ones, taken at each use, so a provider that the user sets
//   after `Horsies.create` applies.

import { createRequire } from "node:module";

import { PACKAGE_VERSION } from "../_catalog/package_version.ts";

/** The module name of the OpenTelemetry API. */
export const OTEL_API_MODULE = "@opentelemetry/api";

/** The instrumentation scope of every span and metric of the package. */
export const INSTRUMENTATION_SCOPE = "horsies";

/** The version of the instrumentation scope: the version of the package (the generated catalog). */
export const INSTRUMENTATION_SCOPE_VERSION: string = PACKAGE_VERSION;

/** A value that a context holds; the package reads none. */
export type OtelContextValue = OtelAttributeValue | object | undefined;

/**
 * An OpenTelemetry context: the API makes it; the package passes it back. It has the members of the API's `Context`,
 * so the API takes it again; the package calls none of them.
 */
export interface OtelContext {
  getValue(key: symbol): unknown;
  setValue(key: symbol, value: OtelContextValue): OtelContext;
  deleteValue(key: symbol): OtelContext;
}

/** A value of a span or metric attribute. */
export type OtelAttributeValue = string | number | boolean;

/** The attributes of a span or a metric point. */
export type OtelAttributes = Readonly<Record<string, OtelAttributeValue>>;

/** A time of the API: seconds and nanoseconds since the epoch. */
export type OtelHighResolutionTime = [seconds: number, nanoseconds: number];

/** The identity of a span. `traceId` is 32 lower-case hex digits, `spanId` 16. */
export interface OtelSpanContext {
  readonly traceId: string;
  readonly spanId: string;
  readonly traceFlags: number;
}

/** The status of a span: `code` is a member of `SpanStatusCode`. */
export interface OtelSpanStatus {
  readonly code: number;
  readonly message?: string;
}

/** An open span. */
export interface OtelSpan {
  setAttribute(key: string, value: OtelAttributeValue): OtelSpan;
  setStatus(status: OtelSpanStatus): OtelSpan;
  recordException(exception: Error | string): void;
  end(endTime?: OtelHighResolutionTime): void;
  spanContext(): OtelSpanContext;
}

/** A link from a span to the span of `context`. */
export interface OtelLink {
  readonly context: OtelSpanContext;
}

/** The options of a new span. `kind` is a member of `SpanKind`. */
export interface OtelSpanOptions {
  readonly kind: number;
  readonly attributes: OtelAttributes;
  readonly links?: OtelLink[];
  readonly startTime?: OtelHighResolutionTime;
}

/** A tracer of the global tracer provider. */
export interface OtelTracer {
  startSpan(name: string, options: OtelSpanOptions, context: OtelContext): OtelSpan;
}

/** The global tracer provider (the API's proxy). */
export interface OtelTracerProvider {
  getTracer(name: string, version: string): OtelTracer;
}

/** The trace members of the API. */
export interface OtelTraceApi {
  getTracer(name: string, version: string): OtelTracer;
  getTracerProvider(): OtelTracerProvider;
  setSpan(context: OtelContext, span: OtelSpan): OtelContext;
  getSpan(context: OtelContext): OtelSpan | undefined;
  deleteSpan(context: OtelContext): OtelContext;
  isSpanContextValid(spanContext: OtelSpanContext): boolean;
}

/** The context members of the API. */
export interface OtelContextApi {
  active(): OtelContext;
  with<T>(context: OtelContext, call: () => T): T;
}

/** How the propagator writes into a carrier. */
export interface OtelCarrierSetter {
  set(carrier: Map<string, string>, key: string, value: string): void;
}

/** How the propagator reads a carrier. */
export interface OtelCarrierGetter {
  get(carrier: ReadonlyMap<string, string>, key: string): string | undefined;
  keys(carrier: ReadonlyMap<string, string>): string[];
}

/** The propagation members of the API. */
export interface OtelPropagationApi {
  inject(context: OtelContext, carrier: Map<string, string>, setter: OtelCarrierSetter): void;
  extract(context: OtelContext, carrier: ReadonlyMap<string, string>, getter: OtelCarrierGetter): OtelContext;
}

/** The options of an instrument. */
export interface OtelInstrumentOptions {
  readonly unit: string;
  readonly description: string;
}

/** The options of a counter: the value type of its points. */
export interface OtelCounterOptions extends OtelInstrumentOptions {
  readonly valueType: number;
}

/** The options of a histogram: its bucket bounds as advice. */
export interface OtelHistogramOptions extends OtelInstrumentOptions {
  readonly advice: { readonly explicitBucketBoundaries: number[] };
}

/** A counter. */
export interface OtelCounter {
  add(value: number, attributes: OtelAttributes): void;
}

/** A histogram. */
export interface OtelHistogram {
  record(value: number, attributes: OtelAttributes): void;
}

/** A meter of the global meter provider. */
export interface OtelMeter {
  createCounter(name: string, options: OtelCounterOptions): OtelCounter;
  createHistogram(name: string, options: OtelHistogramOptions): OtelHistogram;
}

/** The metrics members of the API. */
export interface OtelMetricsApi {
  getMeter(name: string, version: string): OtelMeter;
}

/** The members of the API that the package uses. */
export interface OtelApi {
  readonly trace: OtelTraceApi;
  readonly context: OtelContextApi;
  readonly propagation: OtelPropagationApi;
  readonly metrics: OtelMetricsApi;
  readonly ROOT_CONTEXT: OtelContext;
  readonly SpanKind: { readonly PRODUCER: number; readonly CONSUMER: number };
  readonly SpanStatusCode: { readonly OK: number; readonly ERROR: number };
  readonly ValueType: { readonly INT: number };
}

/** Why the API did not load: the cause of the failed `require`. */
export type OtelApiMissing = { readonly kind: "otelApiMissing"; readonly cause: string };

/** The API, or why it did not load. */
export type OtelApiLoad = { readonly kind: "loaded"; readonly api: OtelApi } | OtelApiMissing;

/** Loads the API module. A test gives its own. */
export type OtelApiLoader = () => OtelApiLoad;

/**
 * Load the API with a synchronous `require` from this module. Effects: reads the module graph. A module that cannot be
 * loaded gives `otelApiMissing` with the cause; nothing throws.
 */
export function requireOtelApi(): OtelApiLoad {
  try {
    // SAFETY: the module of this name is the OpenTelemetry API 1.x (the peer dependency); a test checks that its
    // members fit `OtelApi`.
    const api = createRequire(import.meta.url)(OTEL_API_MODULE) as OtelApi;

    return { kind: "loaded", api };
  } catch (cause) {
    return { kind: "otelApiMissing", cause: cause instanceof Error ? cause.message : String(cause) };
  }
}
