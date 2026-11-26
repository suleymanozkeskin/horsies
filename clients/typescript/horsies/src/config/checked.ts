// The parts of the config parse: a checked value with its refusals, the JSON values of the core's config reader, and
// the shape checks of numbers and texts at the boundary.
//
// The parse is the boundary where the config enters the package. It checks only what the core's JSON reader needs
// before it can read the text: a whole number in the range of the Rust field type, and well-formed text. Every other
// rule of the config is the core's (`checkAppConfig` of the bridge). Each refusal has the catalog code of the config
// section of its field.

import type { HorsiesErrorCode } from "../_catalog/error_codes.ts";
import { refusal, type Refusal } from "../app/refusal.ts";
import { unreachable } from "../wire/unreachable.ts";

/** A JSON value of the core's config text. */
export type ConfigJson = string | number | boolean | null | readonly ConfigJson[] | ConfigObject;

/** A JSON object of the core's config text. */
export type ConfigObject = { readonly [key: string]: ConfigJson };

/** One member of a JSON object: the key and the value. */
export type JsonEntry = readonly [string, ConfigJson];

/** A value that the parse built, or every refusal that it found, in field order. */
export type Checked<T> =
  | { readonly kind: "accepted"; readonly value: T }
  | { readonly kind: "refused"; readonly refusals: readonly Refusal[] };

/** The range of a whole number, and the catalog code of a refusal of a value outside it. */
export type IntegerBound = { readonly min: number; readonly max: number; readonly code: HorsiesErrorCode };

/** The largest value of a Rust `u32`. */
export const U32_MAX: number = 2 ** 32 - 1;

/**
 * The largest value of a Rust `u64` field that the parse admits: a JavaScript number holds a whole number exactly up
 * to `Number.MAX_SAFE_INTEGER`.
 */
export const U64_SAFE_MAX: number = Number.MAX_SAFE_INTEGER;

/** A Rust `u32` field with no other bound in its type, in the config section of `code`. Pure. */
export function u32Field(code: HorsiesErrorCode): IntegerBound {
  return Object.freeze({ min: 0, max: U32_MAX, code });
}

/** A Rust `u64` field with no other bound in its type, in the config section of `code`. Pure. */
export function u64Field(code: HorsiesErrorCode): IntegerBound {
  return Object.freeze({ min: 0, max: U64_SAFE_MAX, code });
}

/** A value with no refusal. Pure. */
export function accepted<T>(value: T): Checked<T> {
  return { kind: "accepted", value };
}

/** One refusal. Pure. */
export function refused<T>(one: Refusal): Checked<T> {
  return { kind: "refused", refusals: [one] };
}

/** `value` when it is a whole number inside `bound`, else a refusal at `path` that names the bound. Pure. */
export function checkedInteger(value: number, bound: IntegerBound, path: string): Checked<number> {
  if (Number.isSafeInteger(value) && value >= bound.min && value <= bound.max) {
    return accepted(value);
  }

  return refused(
    refusal(bound.code, path, `must be a whole number from ${bound.min} to ${bound.max}`, [
      `give a whole number from ${bound.min} to ${bound.max}`,
    ]),
  );
}

/**
 * `text` when it is well-formed (no lone surrogate), else a refusal at `path` with `code`, the code of the config
 * section of the field. The message holds no part of the text: the text can be a secret.
 */
export function checkedText(text: string, code: HorsiesErrorCode, path: string): Checked<string> {
  if (text.isWellFormed()) {
    return accepted(text);
  }

  return refused(refusal(code, path, "holds a lone surrogate", ["give well-formed text"]));
}

/** The accepted value mapped to another value; a refusal stays. Pure apart from `map`. */
export function mapChecked<T, U>(checked: Checked<T>, map: (value: T) => U): Checked<U> {
  switch (checked.kind) {
    case "accepted":
      return accepted(map(checked.value));
    case "refused":
      return checked;
    default:
      return unreachable(checked, "mapChecked");
  }
}

/** One JSON member of a checked value. Pure. */
export function entry(key: string, checked: Checked<ConfigJson>): Checked<readonly JsonEntry[]> {
  return mapChecked(checked, (value): readonly JsonEntry[] => [[key, value]]);
}

/** The JSON member of an optional field: no member when the input leaves the field out. Pure apart from `write`. */
export function optionalEntry<T>(
  key: string,
  input: T | undefined,
  write: (value: T) => Checked<ConfigJson>,
): Checked<readonly JsonEntry[]> {
  if (input === undefined) {
    return accepted([]);
  }

  return entry(key, write(input));
}

/**
 * The members of the parts, in order, for a section whose fields are members of the enclosing object; or every refusal
 * of the parts in order. Pure.
 */
export function checkedEntries(parts: readonly Checked<readonly JsonEntry[]>[]): Checked<readonly JsonEntry[]> {
  return mapChecked(checkedAll(parts), (lists): readonly JsonEntry[] => lists.flat());
}

/** The accepted values in order, or every refusal of the parts in order. Pure. */
export function checkedAll<T>(parts: readonly Checked<T>[]): Checked<readonly T[]> {
  const refusals = parts.flatMap((part) => (part.kind === "refused" ? part.refusals : []));

  if (refusals.length > 0) {
    return { kind: "refused", refusals };
  }

  return accepted(parts.flatMap((part) => (part.kind === "accepted" ? [part.value] : [])));
}

/** A JSON object of the members, in order, or every refusal of the members in order. Pure. */
export function checkedObject(members: readonly Checked<readonly JsonEntry[]>[]): Checked<ConfigObject> {
  return mapChecked(checkedEntries(members), (entries) => Object.freeze(Object.fromEntries(entries)));
}

/** A JSON array of the items, in order, or every refusal of the items in order. Pure. */
export function checkedArray(items: readonly Checked<ConfigJson>[]): Checked<readonly ConfigJson[]> {
  return mapChecked(checkedAll(items), (values) => Object.freeze([...values]));
}

/** The path of a member of the object at `path`. Pure. */
export function memberPath(path: string, name: string): string {
  return path === "" ? name : `${path}.${name}`;
}

/** The path of an item of the list at `path`. Pure. */
export function itemPath(path: string, index: number): string {
  return `${path}[${index}]`;
}

/** The path of the value of `key` in the record at `path`: `path["key"]`. Pure. */
export function keyPath(path: string, key: string): string {
  return `${path}[${JSON.stringify(key)}]`;
}
