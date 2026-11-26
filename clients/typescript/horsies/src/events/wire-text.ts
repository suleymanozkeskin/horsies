// The wire items of an event from its wire text (`WireEvent::of` of the core): the input of the generated decoder,
// where every integer is a `bigint`. The core writes the text in the same process with its own encoder: texts, whole
// numbers and arrays only, at most `MAX_EVENT_WIRE_BYTES` bytes.

import type { WireJson } from "../_catalog/wire_json.ts";

/** The source text of a JSON integer: no fraction and no exponent. */
const INTEGER_SOURCE = /^-?\d+$/u;

/** What `JSON.parse` gives a reviver for a primitive value: its source text. */
type ReviverContext = { readonly source?: string };

/** The wire items of `text`, or why the text is not a JSON array. Never throws. */
export type WireItemsRead =
  | { readonly kind: "items"; readonly items: ReadonlyArray<WireJson> }
  | { readonly kind: "notItems"; readonly cause: string };

/**
 * A `JSON.parse` reviver: an integer becomes the `bigint` of its source text, so a value above 2^53 stays exact; an
 * object becomes a `Map` with its keys in order. Every other value stays.
 */
function wireValue(_key: string, value: WireJson | Readonly<Record<string, WireJson>>, context?: ReviverContext): WireJson {
  const source = context?.source;

  if (source !== undefined && INTEGER_SOURCE.test(source)) {
    return BigInt(source);
  }

  if (value instanceof Array || value instanceof Map || !(value instanceof Object)) {
    return value;
  }

  return new Map(Object.entries(value));
}

/** The wire items of the wire text `text`. A text that is not JSON, or not an array, is `notItems` with the cause. */
export function wireItemsOf(text: string): WireItemsRead {
  let parsed: WireJson;

  try {
    // SAFETY: the reviver gives a `WireJson` for every value of the text.
    parsed = JSON.parse(text, wireValue) as WireJson;
  } catch (cause) {
    return { kind: "notItems", cause: cause instanceof Error ? cause.message : String(cause) };
  }

  return parsed instanceof Array ? { kind: "items", items: parsed } : { kind: "notItems", cause: "the wire text is not a JSON array" };
}
