// The text of a spec data document: the wire value that the generated `workflowSpecDataToWire` gives, written as
// compact JSON with the codec's string and integer texts (Python's `json.dumps` with `(",", ":")` separators). Map
// members keep their order, so an integer-like queue name keeps its place.

import type { WireJson } from "../_catalog/wire_json.ts";
import { bigintText, stringText } from "../codec/scalar-text.ts";
import { isArray, isBigint, isBoolean, isNumber, isString } from "../codec/values.ts";
import { err, ok, type Result } from "../wire/result.ts";

/** The deepest nesting of a spec data document: the document, a node, its body, its task options. */
export const MAX_SPEC_DATA_DEPTH = 8;

/** Why a spec data value has no text: a string with a lone surrogate, at `path`. */
export type SpecTextRefused = { readonly kind: "LoneSurrogate"; readonly path: string; readonly detail: string };

type Written = Result<string, SpecTextRefused>;

/**
 * The text of a spec data wire value. A `number`, a nesting past `MAX_SPEC_DATA_DEPTH` and an integer that the codec
 * refuses are defects of the conversion (`TypeError`): the document holds `bigint` integers of node indexes and
 * bounds only. Pure.
 */
export function specDataText(value: WireJson): Written {
  return valueText(value, "$", 0);
}

function valueText(value: WireJson, path: string, depth: number): Written {
  if (depth > MAX_SPEC_DATA_DEPTH) {
    throw new TypeError(`specDataText: the value nests deeper than ${MAX_SPEC_DATA_DEPTH} levels at ${path}`);
  }

  if (value === null) {
    return ok("null");
  }

  if (isBoolean(value)) {
    return ok(value ? "true" : "false");
  }

  if (isString(value)) {
    const text = stringText(value);

    return text.ok ? text : err({ kind: "LoneSurrogate", path, detail: text.error.detail });
  }

  if (isBigint(value)) {
    const text = bigintText(value);

    if (!text.ok) {
      throw new TypeError(`specDataText: the integer at ${path} has no text: ${text.error.detail}`);
    }

    return text;
  }

  if (isNumber(value)) {
    throw new TypeError(`specDataText: a number at ${path}; spec data holds bigint integers only`);
  }

  return isArray(value) ? itemsText(value, path, depth) : membersText(value, path, depth);
}

function itemsText(items: readonly WireJson[], path: string, depth: number): Written {
  const parts: string[] = [];

  for (const [index, item] of items.entries()) {
    const text = valueText(item, `${path}[${index}]`, depth + 1);

    if (!text.ok) {
      return text;
    }

    parts.push(text.value);
  }

  return ok(`[${parts.join(",")}]`);
}

function membersText(members: ReadonlyMap<string, WireJson>, path: string, depth: number): Written {
  const parts: string[] = [];

  for (const [key, member] of members) {
    const at = `${path}.${key}`;
    const keyText = stringText(key);

    if (!keyText.ok) {
      return err({ kind: "LoneSurrogate", path: at, detail: keyText.error.detail });
    }

    const text = valueText(member, at, depth + 1);

    if (!text.ok) {
      return text;
    }

    parts.push(`${keyText.value}:${text.value}`);
  }

  return ok(`{${parts.join(",")}}`);
}
