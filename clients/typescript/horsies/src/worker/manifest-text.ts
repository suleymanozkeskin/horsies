// The JSON text of a manifest wire value: the form that the generated `toWire` functions give, written for the
// core's manifest parser.

import type { WireJson } from "../_catalog/wire_json.ts";
import { raiseDefect } from "../native/call/defect.ts";
import { unreachable } from "../native/call/never.ts";
import { ClientDefect } from "../thrown-errors.ts";

/** The most nesting levels of a manifest wire value. The worker manifest nests at most 8 levels. */
export const MAX_MANIFEST_DEPTH = 32;

/**
 * The compact JSON text of `value`: map members in their order, a `bigint` as its digits, a text through
 * `JSON.stringify`. A manifest wire value holds no float, so a `number`, and a value that nests deeper than
 * `MAX_MANIFEST_DEPTH`, is a defect of the package: `ClientDefect`, under the defect rule. Pure.
 */
export function manifestText(value: WireJson): string {
  return valueText(value, 0);
}

function valueText(value: WireJson, depth: number): string {
  if (depth > MAX_MANIFEST_DEPTH) {
    return raiseDefect(
      new ClientDefect({ subject: "manifestText", rule: `a manifest wire value nests at most ${String(MAX_MANIFEST_DEPTH)} levels` }),
    );
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
      return raiseDefect(
        new ClientDefect({ subject: "manifestText", rule: `a manifest wire value holds no number, and ${String(value)} is one` }),
      );
    case "object":
      return containerText(value, depth);
    default:
      return unreachable(value);
  }
}

function containerText(value: ReadonlyArray<WireJson> | ReadonlyMap<string, WireJson>, depth: number): string {
  if ("size" in value) {
    const members = Array.from(value, ([key, member]) => `${JSON.stringify(key)}:${valueText(member, depth + 1)}`);

    return `{${members.join(",")}}`;
  }

  return `[${value.map((item) => valueText(item, depth + 1)).join(",")}]`;
}
