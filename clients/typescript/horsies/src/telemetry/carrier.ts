// The trace carrier between the propagator and the core: the inject at a send and a rerun, and the extract in a
// child.
//
// The core stores each key as the propagator wrote it. The extract finds a key as written first, then with ASCII case
// ignored, as HTTP header names compare, so a carrier that another client's propagator wrote reads too.

import type { TraceArgument } from "../native/values/records.ts";
import type { OtelApi, OtelCarrierGetter, OtelContext } from "./otel-api.ts";

/** The trace of a send with no carrier. */
export const NO_CARRIER: TraceArgument = Object.freeze({ kind: "noCarrier" });

/** The setter of a carrier map: each key as the propagator wrote it. */
const MAP_SETTER = Object.freeze({
  set(carrier: Map<string, string>, key: string, value: string): void {
    carrier.set(key, value);
  },
});

/** The getter of a stored carrier: a key as written, else the one key that equals it with ASCII case ignored. */
export const CARRIER_GETTER: OtelCarrierGetter = Object.freeze({
  get(carrier: ReadonlyMap<string, string>, key: string): string | undefined {
    const exact = carrier.get(key);

    if (exact !== undefined) {
      return exact;
    }

    const wanted = asciiLowerCase(key);

    for (const [stored, value] of carrier) {
      if (asciiLowerCase(stored) === wanted) {
        return value;
      }
    }

    return undefined;
  },
  keys(carrier: ReadonlyMap<string, string>): string[] {
    return [...carrier.keys()];
  },
});

/** `text` with the ASCII capitals `A` to `Z` in lower case and every other character kept. Pure. */
function asciiLowerCase(text: string): string {
  return text.replaceAll(/[A-Z]/gu, (capital) => capital.toLowerCase());
}

/**
 * The trace that a send or a rerun hands the core for the span of `context`: the map that the global propagator
 * injects, with the trace id of the span. No carrier when the context holds no valid span (no SDK, or a span that is
 * not sampled and has no parent) or when the propagator writes no key. The core checks the bounds of the map. May throw
 * what the user's propagator throws.
 */
export function sendTraceOf(api: OtelApi, context: OtelContext): TraceArgument {
  const span = api.trace.getSpan(context);

  if (span === undefined) {
    return NO_CARRIER;
  }

  const spanContext = span.spanContext();

  if (!api.trace.isSpanContextValid(spanContext)) {
    return NO_CARRIER;
  }

  const carrier = new Map<string, string>();

  api.propagation.inject(context, carrier, MAP_SETTER);

  if (carrier.size === 0) {
    return NO_CARRIER;
  }

  return Object.freeze({ kind: "carrier", carrier: Object.freeze(Object.fromEntries(carrier)), traceIdHex: spanContext.traceId });
}

/** The context that the global propagator extracts from a stored carrier, over the root context. */
export function extractedContextOf(api: OtelApi, carrier: ReadonlyMap<string, string>): OtelContext {
  return api.propagation.extract(api.ROOT_CONTEXT, carrier, CARRIER_GETTER);
}
