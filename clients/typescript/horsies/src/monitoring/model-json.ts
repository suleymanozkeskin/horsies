// The JSON text that the core writes for a monitoring read, as the values that the decoders take.
//
// The core and the package ship in one addon version, so a text of the core always parses and decodes. A text that
// does not is a defect: it crashes loud and names the read, the model and the place in the text.

import type { MonitoringDecodeError } from "../_catalog/monitoring_models.ts";
import type { DecodeResult, WireJson } from "../_catalog/wire_json.ts";
import type { JsonValue } from "../codec/json-value.ts";
import { raiseDefect } from "../native/call/defect.ts";
import { thrownFacts } from "../native/call/thrown-facts.ts";
import { ClientDefect } from "../thrown-errors.ts";

/** A decoder of one monitoring model. */
export type ModelDecoder<T> = (value: WireJson) => DecodeResult<T, MonitoringDecodeError>;

/** An integer text: the reviver keeps its digits as a `bigint`. */
const INTEGER_TEXT = /^-?\d+$/;

/** A leaf value as `JSON.parse` gives it to a reviver. */
type RevivedLeaf = null | boolean | number | string;

/** A list as `JSON.parse` gives it to a reviver. */
type RevivedList<T> = ReadonlyArray<T>;

/** An object as `JSON.parse` gives it to a reviver: an own data property per key. */
type RevivedObject<T> = { readonly [key: string]: T };

/** The third argument of a reviver: the source text of a leaf. */
interface ReviverContext {
  readonly source?: string;
}

/**
 * The `WireJson` of the core's `text`: an integer is a `bigint`, an object a `Map`. A text that is not JSON is a
 * defect.
 */
export function wireJsonOfCoreText(read: string, text: string): WireJson {
  return parseCoreText(read, () => {
    const tree: WireJson = JSON.parse(text, reviveWire);

    return tree;
  });
}

/** The `JsonValue` of the core's text `text`: an integer outside the safe range is a `bigint`. */
export function jsonValueOfCoreText(read: string, text: string): JsonValue {
  return parseCoreText(read, () => {
    const tree: JsonValue = JSON.parse(text, reviveJsonValue);

    return tree;
  });
}

/** The value of `text` under the decoder of its model. A text that does not decode is a defect. */
export function decodeCoreModel<T>(read: string, text: string, decode: ModelDecoder<T>): T {
  return decodedOrDefect(read, decode(wireJsonOfCoreText(read, text)));
}

/** The decoded value, or the defect of the read `read` with the model and the place of the refusal. */
export function decodedOrDefect<T>(read: string, result: DecodeResult<T, MonitoringDecodeError>): T {
  if (result.ok) {
    return result.value;
  }

  return raiseDefect(
    new ClientDefect({
      subject: read,
      rule: `the core's ${result.error.model} does not decode: ${result.error.kind} at ${result.error.path}`,
    }),
  );
}

/** The items of a JSON list of the core. A value that is not a list is a defect. */
export function listItems(read: string, value: WireJson): ReadonlyArray<WireJson> {
  if (Array.isArray(value)) {
    // SAFETY: `Array.isArray` holds, and a list member of `WireJson` is a `ReadonlyArray<WireJson>`.
    return value as ReadonlyArray<WireJson>;
  }

  return raiseDefect(new ClientDefect({ subject: read, rule: "the core's answer is not a JSON list" }));
}

/** The text of a JSON string of the core. A value that is not a string is a defect. */
export function textItem(read: string, value: WireJson): string {
  const text = String(value);

  // A `String` of any other member of `WireJson` differs from the value: `null`, a number, a bigint, a boolean, a list
  // and a map are not equal to their text.
  if (text === value) {
    return text;
  }

  return raiseDefect(
    new ClientDefect({ subject: read, rule: "a list item of the core's answer is not a JSON string" }),
  );
}

/** The value that `parse` gives. A text that is not JSON is a defect of the core. */
function parseCoreText<T>(read: string, parse: () => T): T {
  try {
    return parse();
  } catch (cause) {
    return raiseDefect(
      new ClientDefect({ subject: read, rule: `the core's answer is not JSON: ${thrownFacts(cause).message}` }),
    );
  }
}

/** One `JSON.parse` value as `WireJson`: an integer text becomes a `bigint`, an object a `Map`. */
function reviveWire(
  _key: string,
  value: RevivedLeaf | RevivedList<WireJson> | RevivedObject<WireJson>,
  context?: ReviverContext,
): WireJson {
  const source = context?.source;

  if (source === undefined) {
    return isRevivedList(value) ? value : new Map(Object.entries(objectOf(value)));
  }

  return INTEGER_TEXT.test(source) ? BigInt(source) : leafOf(value);
}

/** One `JSON.parse` value as `JsonValue`: an integer text outside the safe range becomes a `bigint`. */
function reviveJsonValue(
  _key: string,
  value: RevivedLeaf | RevivedList<JsonValue> | RevivedObject<JsonValue>,
  context?: ReviverContext,
): JsonValue {
  const source = context?.source;

  if (source === undefined) {
    return isRevivedList(value) ? value : objectOf(value);
  }

  if (INTEGER_TEXT.test(source)) {
    const integer = BigInt(source);

    const safe = integer >= BigInt(Number.MIN_SAFE_INTEGER) && integer <= BigInt(Number.MAX_SAFE_INTEGER);

    return safe ? Number(source) : integer;
  }

  return leafOf(value);
}

/** Whether a revived container is a list. */
function isRevivedList<T>(value: RevivedLeaf | RevivedList<T> | RevivedObject<T>): value is RevivedList<T> {
  return Array.isArray(value);
}

/** A revived container that is not a list: `JSON.parse` gives the source text of every leaf, so it is an object. */
function objectOf<T>(value: RevivedLeaf | RevivedObject<T>): RevivedObject<T> {
  // SAFETY: the reviver calls this only for a container (no source text) that is not a list: a JSON object.
  return value as RevivedObject<T>;
}

/** A revived leaf: `JSON.parse` gives source text only for a leaf. */
function leafOf<T>(value: RevivedLeaf | RevivedList<T> | RevivedObject<T>): RevivedLeaf {
  // SAFETY: the reviver calls this only when `JSON.parse` gave the source text, which it gives for a leaf only.
  return value as RevivedLeaf;
}
