// The `h.*` builders of the wire types.

import type { JsonValue } from "../codec/json-value.ts";
import type { Decimal } from "./decimal.ts";
import type {
  AnyWire,
  ObjectSent,
  ParameterWires,
  ObjectValue,
  RequiredWire,
  SentOf,
  TupleSent,
  TupleValue,
  ValueOf,
  WireMembers,
} from "./infer.ts";
import {
  BIGINT_KIND,
  BOOLEAN_KIND,
  DECIMAL_KIND,
  FLOAT_KIND,
  INSTANT_KIND,
  INT_KIND,
  JSON_KIND,
  type Kind,
  type LiteralMember,
  type Member,
  NULL_KIND,
  type ObjectKind,
  objectKind,
  OFFSET_DATE_TIME_KIND,
  PLAIN_DATE_KIND,
  PLAIN_DATE_TIME_KIND,
  PLAIN_TIME_KIND,
  STRING_KIND,
  taggedUnionKind,
  UUID_KIND,
} from "./kinds.ts";
import { isResultOfKind, type ResultOfWire, resultOfWire } from "./result-of.ts";
import { kindOf, presenceOf, type Wire, wireOf } from "./wire-type.ts";

/** A list with at least one item. */
export type NonEmpty<T> = readonly [T, ...T[]];

/** A wire type of an object value: a member of a tagged union. */
export type ObjectWire = Wire<object, object, "required">;

/** The builders of the wire types. */
export interface WireBuilders {
  /** `null`, stored as `null`. */
  null(): Wire<null>;
  /** `true` or `false`. */
  boolean(): Wire<boolean>;
  /** A well-formed string, stored as a JSON string. */
  string(): Wire<string>;
  /** An integer in the safe range (|n| ≤ 2^53 - 1), stored as decimal digits. */
  int(): Wire<number>;
  /** An integer of any size up to 4,300 digits, stored as exact decimal digits. */
  bigint(): Wire<bigint>;
  /** A finite float, stored in Python's float text: `7.0`, `1e-05`, `1e+16`, `-0.0`. */
  float(): Wire<number>;
  /** A UUID text in the lower-case 8-4-4-4-12 form (`crypto.randomUUID()` gives it), stored as it is. */
  uuid(): Wire<string>;
  /** An exact decimal, stored as a JSON string: Python's `str(Decimal)` text. */
  decimal(): Wire<Decimal>;
  /** A `Temporal.Instant`, stored in UTC: `2026-08-12T09:10:12.654321Z`. */
  instant(): Wire<Temporal.Instant>;
  /**
   * A `Temporal.ZonedDateTime` whose zone is a fixed offset, stored with the offset:
   * `2026-08-12T09:10:12.654321+02:00` (`Z` for offset 0). A named zone, `UTC` too, is refused.
   */
  offsetDateTime(): Wire<Temporal.ZonedDateTime>;
  /** A `Temporal.PlainDateTime`, stored without an offset: `2026-08-12T09:10:12.654321`. */
  plainDateTime(): Wire<Temporal.PlainDateTime>;
  /** A `Temporal.PlainDate`, stored as `2026-08-12`. */
  plainDate(): Wire<Temporal.PlainDate>;
  /** A `Temporal.PlainTime`, a time of day stored as `09:10:12.654321`. */
  plainTime(): Wire<Temporal.PlainTime>;
  /**
   * An object with declared members, stored in the key order of `members` (`Object.keys`: JavaScript puts an
   * integer-like name such as `"10"` first, in ascending order, then the other names in source order). An undeclared
   * key is refused.
   */
  object<const M extends WireMembers>(members: M): Wire<ObjectValue<M>, ObjectSent<M>>;
  /**
   * Untyped JSON that the type names explicitly. A `bigint` and a safe integer are stored as integers; any other
   * `number` in Python's float text. A key that only the engine writes is refused at every depth.
   */
  json(): Wire<JsonValue>;
  /** One of the listed values: strings, safe integers, booleans or `null`. Stored as the value. */
  literal<const L extends NonEmpty<LiteralMember>>(...members: L): Wire<L[number]>;
  /** One of the listed strings. Stored as the string. */
  enum<const L extends NonEmpty<string>>(members: L): Wire<L[number]>;
  /**
   * A value of one of the member types, each a primitive type, at most one per stored JSON type (`Horsies.create`
   * refuses `h.int()` with `h.float()`: one `number` cannot say which it is). Stored as the member's text.
   */
  union<const T extends NonEmpty<RequiredWire>>(members: T): Wire<ValueOf<T[number]>, SentOf<T[number]>>;
  /**
   * An object of one of the member types, told apart by the member `tag`, which each member declares as a string
   * literal. Stored as the member object, in its declaration order.
   */
  union<const K extends string, const T extends NonEmpty<ObjectWire>>(
    tag: K,
    members: T,
  ): Wire<ValueOf<T[number]>, SentOf<T[number]>>;
  /**
   * A task input parameter that a workflow node fills with the outcome of an upstream task: `{ kind: "succeeded",
   * value }` or `{ kind: "failed", error }`. Valid only as a whole parameter (optional or nullable too); a sender
   * never gives it.
   */
  resultOf<W extends RequiredWire>(inner: W): ResultOfWire<W>;
  /** A JSON array of one item type. */
  array<W extends RequiredWire>(item: W): Wire<ValueOf<W>[], SentOf<W>[]>;
  /** A JSON array with one value per item type. */
  tuple<const T extends readonly RequiredWire[]>(items: T): Wire<TupleValue<T>, TupleSent<T>>;
  /** A JSON object with any string keys, stored in the value's own key order (integer-like keys first). */
  record<W extends RequiredWire>(
    value: W,
  ): Wire<Readonly<Record<string, ValueOf<W>>>, Readonly<Record<string, SentOf<W>>>>;
}

/** The kind of a plain value. An upstream outcome there is a defect that the static types refuse. */
function valueKindOf(wire: AnyWire, place: string): Kind {
  const kind = kindOf(wire);

  if (isResultOfKind(kind)) {
    throw new TypeError(`h.${place}: h.resultOf is valid only as a whole task input parameter`);
  }

  return kind;
}

/** The kind of a wire type that must be present: an array, tuple or record item. Another one is a defect. */
function requiredKindOf(wire: AnyWire, place: string): Kind {
  if (presenceOf(wire).mode !== "required") {
    throw new TypeError(`h.${place}: an item is always present; .optional() and .default() are for object members`);
  }

  return valueKindOf(wire, place);
}

/** The members of an object or a task input, in the key order of `members` (`Object.keys`). Pure. */
function membersOf(members: ParameterWires, kindOfMember: (wire: AnyWire) => Kind): readonly Member[] {
  return Object.keys(members).map((name) => {
    const wire = members[name];

    if (wire === undefined) {
      throw new TypeError(`h.object: the member ${name} has no wire type`);
    }

    return { name, kind: kindOfMember(wire), presence: presenceOf(wire) };
  });
}

/** A primitive union of the members. */
function primitiveUnion<const T extends NonEmpty<RequiredWire>>(
  members: T,
): Wire<ValueOf<T[number]>, SentOf<T[number]>> {
  const kinds = Object.freeze(members.map((member) => requiredKindOf(member, "union")));

  return wireOf(Object.freeze({ node: "union", members: kinds }));
}

/** A tagged union of object members. A member that is not an object is a defect that the static types refuse. */
function taggedUnion<const K extends string, const T extends NonEmpty<ObjectWire>>(
  tag: K,
  members: T,
): Wire<ValueOf<T[number]>, SentOf<T[number]>> {
  const variants = members.map((member) => {
    const kind = requiredKindOf(member, "union");

    if (kind.node !== "object") {
      throw new TypeError("h.union(tag, members): each member is an h.object");
    }

    return kind;
  });

  return wireOf(taggedUnionKind(tag, variants));
}

function union<const T extends NonEmpty<RequiredWire>>(members: T): Wire<ValueOf<T[number]>, SentOf<T[number]>>;
function union<const K extends string, const T extends NonEmpty<ObjectWire>>(
  tag: K,
  members: T,
): Wire<ValueOf<T[number]>, SentOf<T[number]>>;
function union(
  first: string | NonEmpty<RequiredWire>,
  second: NonEmpty<ObjectWire> | readonly [] = [],
): Wire<unknown, unknown> {
  if (isWireList(first)) {
    return primitiveUnion(first);
  }

  const [head, ...rest] = second;

  if (head === undefined) {
    throw new TypeError("h.union(tag, members): the members list is empty");
  }

  return taggedUnion(first, [head, ...rest]);
}

/** Whether the first argument of `h.union` is the member list (the form without a tag). */
function isWireList(first: string | NonEmpty<RequiredWire>): first is NonEmpty<RequiredWire> {
  return Array.isArray(first);
}

/** A literal kind of the members. An empty list is a defect that the static types refuse. */
function literalWire<const L extends NonEmpty<LiteralMember>>(members: L): Wire<L[number]> {
  if (members.length === 0) {
    throw new TypeError("h.literal: give at least one member");
  }

  return wireOf(Object.freeze({ node: "literal", members: Object.freeze([...members]) }));
}

/** The kind of a task input: one member per parameter, in declaration order. A parameter may be `h.resultOf`. */
export function parametersKind(parameters: ParameterWires): ObjectKind {
  return objectKind(membersOf(parameters, kindOf));
}

export const h: WireBuilders = Object.freeze({
  null: (): Wire<null> => wireOf(NULL_KIND),
  boolean: (): Wire<boolean> => wireOf(BOOLEAN_KIND),
  string: (): Wire<string> => wireOf(STRING_KIND),
  int: (): Wire<number> => wireOf(INT_KIND),
  bigint: (): Wire<bigint> => wireOf(BIGINT_KIND),
  float: (): Wire<number> => wireOf(FLOAT_KIND),
  uuid: (): Wire<string> => wireOf(UUID_KIND),
  decimal: (): Wire<Decimal> => wireOf(DECIMAL_KIND),
  instant: (): Wire<Temporal.Instant> => wireOf(INSTANT_KIND),
  offsetDateTime: (): Wire<Temporal.ZonedDateTime> => wireOf(OFFSET_DATE_TIME_KIND),
  plainDateTime: (): Wire<Temporal.PlainDateTime> => wireOf(PLAIN_DATE_TIME_KIND),
  plainDate: (): Wire<Temporal.PlainDate> => wireOf(PLAIN_DATE_KIND),
  plainTime: (): Wire<Temporal.PlainTime> => wireOf(PLAIN_TIME_KIND),
  json: (): Wire<JsonValue> => wireOf(JSON_KIND),
  literal: <const L extends NonEmpty<LiteralMember>>(...members: L): Wire<L[number]> => literalWire(members),
  enum: <const L extends NonEmpty<string>>(members: L): Wire<L[number]> => literalWire(members),
  union,
  resultOf: <W extends RequiredWire>(inner: W): ResultOfWire<W> => resultOfWire(inner),
  object: <const M extends WireMembers>(members: M): Wire<ObjectValue<M>, ObjectSent<M>> =>
    wireOf(objectKind(membersOf(members, (wire) => valueKindOf(wire, "object")))),
  array: <W extends RequiredWire>(item: W): Wire<ValueOf<W>[], SentOf<W>[]> =>
    wireOf(Object.freeze({ node: "array", item: requiredKindOf(item, "array") })),
  tuple: <const T extends readonly RequiredWire[]>(items: T): Wire<TupleValue<T>, TupleSent<T>> =>
    wireOf(Object.freeze({ node: "tuple", items: Object.freeze(items.map((item) => requiredKindOf(item, "tuple"))) })),
  record: <W extends RequiredWire>(
    value: W,
  ): Wire<Readonly<Record<string, ValueOf<W>>>, Readonly<Record<string, SentOf<W>>>> =>
    wireOf(Object.freeze({ node: "record", value: requiredKindOf(value, "record") })),
});

// The static helpers of `h`. A namespace with types only: no code at run time.
export declare namespace h {
  /** The value that a reader gives for a wire type: `h.Infer<typeof Order>`. */
  export type Infer<W extends AnyWire> = ValueOf<W>;
}
