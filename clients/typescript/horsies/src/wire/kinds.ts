// The kind tree: the declared wire type that drives the writer and the reader. Each `h.*` call builds one frozen
// node. The writer and the reader walk the tree and the value together, and never look at the JavaScript value to
// choose a stored form: `7` under `h.float()` is `7.0`, under `h.int()` it is `7`.

import type { JsonText } from "../codec/json-text.ts";
import type { EncodeRefused } from "../codec/refusal.ts";

/** `null`. */
export type NullKind = { readonly node: "null" };

/** `true` or `false`. */
export type BooleanKind = { readonly node: "boolean" };

/** An integer in the safe range of a JavaScript `number`. */
export type IntKind = { readonly node: "int" };

/** An integer of any size, at most the core's digit bound, as a `bigint`. */
export type BigintKind = { readonly node: "bigint" };

/** A finite binary64 float, stored in Python's float text. */
export type FloatKind = { readonly node: "float" };

/** A well-formed string. */
export type StringKind = { readonly node: "string" };

/** A `Temporal.Instant`, stored as a UTC date-time: `YYYY-MM-DDTHH:MM:SS[.f]Z`. */
export type InstantKind = { readonly node: "instant" };

/** A `Temporal.ZonedDateTime` with a fixed-offset zone, stored with its offset (`Z` for offset 0). */
export type OffsetDateTimeKind = { readonly node: "offsetDateTime" };

/** A `Temporal.PlainDateTime`, stored without an offset. */
export type PlainDateTimeKind = { readonly node: "plainDateTime" };

/** A `Temporal.PlainDate`, stored as `YYYY-MM-DD`. */
export type PlainDateKind = { readonly node: "plainDate" };

/** `null`, or a value of `inner`. */
export type NullableKind = { readonly node: "nullable"; readonly inner: Kind };

/** A JSON array of values of one kind. */
export type ArrayKind = { readonly node: "array"; readonly item: Kind };

/** A JSON array with one value per item kind, in order. */
export type TupleKind = { readonly node: "tuple"; readonly items: readonly Kind[] };

/** A JSON object with any string keys and values of one kind. Stored in the value's own key order. */
export type RecordKind = { readonly node: "record"; readonly value: Kind };

/** The stored text of a `.default(value)`, or the writer's refusal of that value. */
export type DefaultText =
  | { readonly state: "stored"; readonly text: JsonText }
  | { readonly state: "refused"; readonly refusal: EncodeRefused };

/** How an object member is present in a stored text. */
export type MemberPresence =
  /** Always present. */
  | { readonly mode: "required" }
  /** `.optional()`: present, or left out. */
  | { readonly mode: "optional" }
  /** `.default(value)`: a sender may leave it out; a reader in worker form fills `fallback`. */
  | { readonly mode: "defaulted"; readonly fallback: DefaultText };

/** A declared member of an object. */
export type Member = { readonly name: string; readonly kind: Kind; readonly presence: MemberPresence };

/** A JSON object with declared members, stored in declaration order. */
export type ObjectKind = {
  readonly node: "object";
  readonly members: readonly Member[];
  readonly byName: ReadonlyMap<string, Member>;
};

/** A declared wire type. */
export type Kind =
  | NullKind
  | BooleanKind
  | IntKind
  | BigintKind
  | FloatKind
  | StringKind
  | InstantKind
  | OffsetDateTimeKind
  | PlainDateTimeKind
  | PlainDateKind
  | NullableKind
  | ArrayKind
  | TupleKind
  | RecordKind
  | ObjectKind;

export const NULL_KIND: NullKind = Object.freeze({ node: "null" });
export const BOOLEAN_KIND: BooleanKind = Object.freeze({ node: "boolean" });
export const INT_KIND: IntKind = Object.freeze({ node: "int" });
export const BIGINT_KIND: BigintKind = Object.freeze({ node: "bigint" });
export const FLOAT_KIND: FloatKind = Object.freeze({ node: "float" });
export const STRING_KIND: StringKind = Object.freeze({ node: "string" });
export const INSTANT_KIND: InstantKind = Object.freeze({ node: "instant" });
export const OFFSET_DATE_TIME_KIND: OffsetDateTimeKind = Object.freeze({ node: "offsetDateTime" });
export const PLAIN_DATE_TIME_KIND: PlainDateTimeKind = Object.freeze({ node: "plainDateTime" });
export const PLAIN_DATE_KIND: PlainDateKind = Object.freeze({ node: "plainDate" });
export const REQUIRED: MemberPresence = Object.freeze({ mode: "required" });
export const OPTIONAL: MemberPresence = Object.freeze({ mode: "optional" });

/** `null` or `inner`. A nullable kind stays as it is. Pure. */
export function nullableKind(inner: Kind): Kind {
  return inner.node === "nullable" ? inner : Object.freeze({ node: "nullable", inner });
}

/** An object kind of the members, in their order. Pure. */
export function objectKind(members: readonly Member[]): ObjectKind {
  const frozen = Object.freeze(members.map((member) => Object.freeze({ ...member })));

  return Object.freeze({
    node: "object",
    members: frozen,
    byName: new Map(frozen.map((member) => [member.name, member])),
  });
}
