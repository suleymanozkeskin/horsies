// The telemetry flush of a child before its exit: `forceFlush()` of the global tracer provider, so the last spans of
// the child are exported. The exit rule bounds the wait (`CHILD_EXIT_FLUSH_BOUND_MS`, 1 s, the Python client's
// `ATTEMPT_SPAN_FLUSH_BOUND_S`).
//
// The API's global provider is a proxy; its delegate is the provider that the user set. A provider with no
// `forceFlush` (no SDK, or an SDK that has none) has nothing to flush.

import type { OtelApi } from "./otel-api.ts";
import type { TelemetryWarnings } from "./warnings.ts";

/** A provider that can flush. */
type FlushingProvider = { forceFlush(): Promise<void> };

/** A proxy provider that names its delegate. */
type ProxyProvider = { getDelegate(): object };

/** Whether `provider` has a `forceFlush` method. */
function canFlush(provider: object): provider is FlushingProvider {
  return "forceFlush" in provider && provider.forceFlush instanceof Function;
}

/** Whether `provider` is a proxy with a delegate. */
function isProxy(provider: object): provider is ProxyProvider {
  return "getDelegate" in provider && provider.getDelegate instanceof Function;
}

/** Flush the global tracer provider. A flush that throws or rejects is written once per process; it never rejects. */
export async function flushTracerProvider(deps: { readonly api: OtelApi; readonly warnings: TelemetryWarnings }): Promise<void> {
  try {
    const global: object = deps.api.trace.getTracerProvider();
    const provider = isProxy(global) ? global.getDelegate() : global;

    if (canFlush(provider)) {
      await provider.forceFlush();
    }
  } catch (cause) {
    deps.warnings.warnOnce("flush", cause);
  }
}
