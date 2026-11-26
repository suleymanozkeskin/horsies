// The JSON text of a manifest wire value: the form that the generated `toWire` functions give, written for the
// core's manifest parser.

import type { WireJson } from "../_catalog/wire_json.ts";

/** The most nesting levels of a manifest wire value. The worker manifest nests at most 8 levels. */
export const MAX_MANIFEST_DEPTH = 32;

/**
 * The compact JSON text of `value`: map members in their order, a `bigint` as its digits, a text through
 * `JSON.stringify`. A manifest wire value holds no float, so a `number`, and a value that nests deeper than
 * `MAX_MANIFEST_DEPTH`, is a defect of the package: `TypeError`. Pure.
 */
export function manifestText(value: WireJson): string {
  return valueText(value, 0);
}

function valueText(value: WireJson, depth: number): string {
  if (depth > MAX_MANIFEST_DEPTH) {
    throw new TypeError(`manifestText: the value nests deeper than ${String(MAX_MANIFEST_DEPTH)} levels`);
  }

  if (value === null) {
    return "null";
  }

  switch (typeof value) {
    case "boolean":
      return value ? "true" : "false";
    case "bigint":
      return value.toString();
    case "string":
      return JSON.stringify(value);
    case "number":
      throw new TypeError(`manifestText: ${String(value)} is a number, which a manifest wire value never holds`);
    case "object":
      return containerText(value, depth);
    default: {
      const unexpected: never = value;

      throw new TypeError(`manifestText: a value outside its type: ${String(unexpected)}`);
    }
  }
}

function containerText(value: ReadonlyArray<WireJson> | ReadonlyMap<string, WireJson>, depth: number): string {
  if ("size" in value) {
    const members = Array.from(value, ([key, member]) => `${JSON.stringify(key)}:${valueText(member, depth + 1)}`);

    return `{${members.join(",")}}`;
  }

  return `[${value.map((item) => valueText(item, depth + 1)).join(",")}]`;
}
