import type { WireJson } from "../../../../horsies/src/_catalog/wire_json.ts";

/** The most nesting levels of a wire value that the writer follows. */
const MAX_WIRE_DEPTH = 32;

/**
 * The compact JSON text of a manifest wire value, as Python's `json.dumps`
 * with `separators=(",", ":")` writes it for ASCII text: map members in their
 * order, a `bigint` as its digits, a text through `JSON.stringify`.
 *
 * Test support only, not the package's codec writer. It refuses a `number`:
 * a manifest wire value holds no float, and its integers are `bigint`.
 */
export function wireText(value: WireJson): string {
  return valueText(value, 0);
}

function valueText(value: WireJson, depth: number): string {
  if (depth > MAX_WIRE_DEPTH) {
    throw new Error(`wireText: the value nests deeper than ${String(MAX_WIRE_DEPTH)} levels`);
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
      throw new Error(`wireText: ${String(value)} is a number, which a manifest wire value never holds`);
    case "object":
      return containerText(value, depth);
    default: {
      const unreachable: never = value;

      return unreachable;
    }
  }
}

function containerText(
  value: ReadonlyArray<WireJson> | ReadonlyMap<string, WireJson>,
  depth: number,
): string {
  if ("size" in value) {
    const members = Array.from(
      value,
      ([key, member]) => `${JSON.stringify(key)}:${valueText(member, depth + 1)}`,
    );

    return `{${members.join(",")}}`;
  }

  const items = value.map((item) => valueText(item, depth + 1));

  return `[${items.join(",")}]`;
}
