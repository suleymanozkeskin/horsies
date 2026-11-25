// The `h.*` builders of the wire types.

import type {
  AnyWire,
  ObjectSent,
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
  FLOAT_KIND,
  INSTANT_KIND,
  INT_KIND,
  type Kind,
  type Member,
  NULL_KIND,
  type ObjectKind,
  objectKind,
  OFFSET_DATE_TIME_KIND,
  PLAIN_DATE_KIND,
  PLAIN_DATE_TIME_KIND,
  STRING_KIND,
} from "./kinds.ts";
import { kindOf, presenceOf, type Wire, wireOf } from "./wire-type.ts";

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
  /** An object with declared members, stored in declaration order. An undeclared key is refused. */
  object<const M extends WireMembers>(members: M): Wire<ObjectValue<M>, ObjectSent<M>>;
  /** A JSON array of one item type. */
  array<W extends RequiredWire>(item: W): Wire<ValueOf<W>[], SentOf<W>[]>;
  /** A JSON array with one value per item type. */
  tuple<const T extends readonly RequiredWire[]>(items: T): Wire<TupleValue<T>, TupleSent<T>>;
  /** A JSON object with any string keys, stored in the value's own key order (integer-like keys first). */
  record<W extends RequiredWire>(
    value: W,
  ): Wire<Readonly<Record<string, ValueOf<W>>>, Readonly<Record<string, SentOf<W>>>>;
}

/** The kind of a wire type that must be present: an array, tuple or record item. Another one is a defect. */
function requiredKindOf(wire: AnyWire, place: string): Kind {
  if (presenceOf(wire).mode !== "required") {
    throw new TypeError(`h.${place}: an item is always present; .optional() and .default() are for object members`);
  }

  return kindOf(wire);
}

/** The members of an object, in declaration order. Pure. */
function membersOf(members: WireMembers): readonly Member[] {
  return Object.keys(members).map((name) => {
    const wire = members[name];

    if (wire === undefined) {
      throw new TypeError(`h.object: the member ${name} has no wire type`);
    }

    return { name, kind: kindOf(wire), presence: presenceOf(wire) };
  });
}

/** The kind of a task input: one member per parameter, in declaration order. */
export function parametersKind(parameters: WireMembers): ObjectKind {
  return objectKind(membersOf(parameters));
}

export const h: WireBuilders = Object.freeze({
  null: (): Wire<null> => wireOf(NULL_KIND),
  boolean: (): Wire<boolean> => wireOf(BOOLEAN_KIND),
  string: (): Wire<string> => wireOf(STRING_KIND),
  int: (): Wire<number> => wireOf(INT_KIND),
  bigint: (): Wire<bigint> => wireOf(BIGINT_KIND),
  float: (): Wire<number> => wireOf(FLOAT_KIND),
  instant: (): Wire<Temporal.Instant> => wireOf(INSTANT_KIND),
  offsetDateTime: (): Wire<Temporal.ZonedDateTime> => wireOf(OFFSET_DATE_TIME_KIND),
  plainDateTime: (): Wire<Temporal.PlainDateTime> => wireOf(PLAIN_DATE_TIME_KIND),
  plainDate: (): Wire<Temporal.PlainDate> => wireOf(PLAIN_DATE_KIND),
  object: <const M extends WireMembers>(members: M): Wire<ObjectValue<M>, ObjectSent<M>> =>
    wireOf(objectKind(membersOf(members))),
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
