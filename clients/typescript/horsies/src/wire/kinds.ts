// The kind tree: the declared wire type that drives the writer and the reader. Each `h.*` call builds one frozen
// node. The writer and the reader walk the tree and the value together, and never look at the JavaScript value to
// choose a stored form: `7` under `h.float()` is `7.0`, under `h.int()` it is `7`.

import type { DecodeConversion, EncodeConversion, ValueCheck } from "../codec/conversion.ts";
import type { JsonText } from "../codec/json-text.ts";
import type { EncodeRefused } from "../codec/refusal.ts";
import { isString } from "../codec/values.ts";

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

/** A UUID text in the lower-case 8-4-4-4-12 form. */
export type UuidKind = { readonly node: "uuid" };

/** A `Decimal`, stored as a JSON string: Python's `str(Decimal)`. */
export type DecimalKind = { readonly node: "decimal" };

/** A `Temporal.Instant`, stored as a UTC date-time: `YYYY-MM-DDTHH:MM:SS[.f]Z`. */
export type InstantKind = { readonly node: "instant" };

/** A `Temporal.ZonedDateTime` with a fixed-offset zone, stored with its offset (`Z` for offset 0). */
export type OffsetDateTimeKind = { readonly node: "offsetDateTime" };

/** A `Temporal.PlainDateTime`, stored without an offset. */
export type PlainDateTimeKind = { readonly node: "plainDateTime" };

/** A `Temporal.PlainDate`, stored as `YYYY-MM-DD`. */
export type PlainDateKind = { readonly node: "plainDate" };

/** A `Temporal.PlainTime`, a time of day stored as `HH:MM:SS[.f]`. */
export type PlainTimeKind = { readonly node: "plainTime" };

/** A member of a literal kind: a string, a number (a safe integer), a boolean or `null`. */
export type LiteralMember = string | number | boolean | null;

/** One of the listed values, stored as its JSON text. */
export type LiteralKind = { readonly node: "literal"; readonly members: readonly LiteralMember[] };

/** A value of one of the member kinds, each a primitive kind; the first member that takes a value writes it. */
export type UnionKind = { readonly node: "union"; readonly members: readonly Kind[] };

/** An object of one of the variants, told apart by the string literal of the member `tag`. */
export type TaggedUnionKind = {
  readonly node: "taggedUnion";
  readonly tag: string;
  readonly variants: readonly ObjectKind[];
  readonly byTag: ReadonlyMap<string, ObjectKind>;
};

/** Untyped JSON that the type names explicitly: a `JsonValue`. */
export type JsonKind = { readonly node: "json" };

/**
 * The outcome of an upstream task, which a workflow node gives to a whole input parameter: the stored result
 * envelope, whose `ok` value has the kind `inner`.
 */
export type ResultOfKind = { readonly node: "resultOf"; readonly inner: Kind };

/**
 * A value that a user-side conversion turns into the stored value of `stored` and back (a zod codec). The stored text
 * is the text of `stored`.
 */
export type ConversionKind = {
  readonly node: "conversion";
  readonly stored: Kind;
  readonly decode: DecodeConversion;
  readonly encode: EncodeConversion;
};

/** A value of `inner` that a user-side check must accept (a zod refinement). It changes no stored text. */
export type CheckedKind = { readonly node: "checked"; readonly inner: Kind; readonly check: ValueCheck };

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
  | UuidKind
  | DecimalKind
  | InstantKind
  | OffsetDateTimeKind
  | PlainDateTimeKind
  | PlainDateKind
  | PlainTimeKind
  | LiteralKind
  | UnionKind
  | TaggedUnionKind
  | NullableKind
  | ArrayKind
  | TupleKind
  | RecordKind
  | ObjectKind
  | JsonKind
  | ResultOfKind
  | ConversionKind
  | CheckedKind;

export const NULL_KIND: NullKind = Object.freeze({ node: "null" });
export const BOOLEAN_KIND: BooleanKind = Object.freeze({ node: "boolean" });
export const INT_KIND: IntKind = Object.freeze({ node: "int" });
export const BIGINT_KIND: BigintKind = Object.freeze({ node: "bigint" });
export const FLOAT_KIND: FloatKind = Object.freeze({ node: "float" });
export const STRING_KIND: StringKind = Object.freeze({ node: "string" });
export const UUID_KIND: UuidKind = Object.freeze({ node: "uuid" });
export const DECIMAL_KIND: DecimalKind = Object.freeze({ node: "decimal" });
export const INSTANT_KIND: InstantKind = Object.freeze({ node: "instant" });
export const OFFSET_DATE_TIME_KIND: OffsetDateTimeKind = Object.freeze({ node: "offsetDateTime" });
export const PLAIN_DATE_TIME_KIND: PlainDateTimeKind = Object.freeze({ node: "plainDateTime" });
export const PLAIN_DATE_KIND: PlainDateKind = Object.freeze({ node: "plainDate" });
export const PLAIN_TIME_KIND: PlainTimeKind = Object.freeze({ node: "plainTime" });
export const JSON_KIND: JsonKind = Object.freeze({ node: "json" });
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

/** The string values of the tag member of a variant, or an empty list when it has none. Pure. */
export function tagValues(variant: ObjectKind, tag: string): readonly string[] {
  const member = variant.byName.get(tag);

  if (member === undefined || member.kind.node !== "literal") {
    return [];
  }

  return member.kind.members.filter(isString);
}

/** A tagged union of the variants; a tag value maps to the first variant that names it. Pure. */
export function taggedUnionKind(tag: string, variants: readonly ObjectKind[]): TaggedUnionKind {
  const byTag = new Map<string, ObjectKind>();

  for (const variant of variants) {
    for (const value of tagValues(variant, tag)) {
      if (!byTag.has(value)) {
        byTag.set(value, variant);
      }
    }
  }

  return Object.freeze({ node: "taggedUnion", tag, variants: Object.freeze([...variants]), byTag });
}

/** A kind whose values a user-side conversion turns into values of `stored` and back. Pure. */
export function conversionKind(stored: Kind, decode: DecodeConversion, encode: EncodeConversion): ConversionKind {
  return Object.freeze({ node: "conversion", stored, decode, encode });
}

/** A kind whose values a user-side check must accept. Pure. */
export function checkedKind(inner: Kind, check: ValueCheck): CheckedKind {
  return Object.freeze({ node: "checked", inner, check });
}
