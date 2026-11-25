// The `h.*` wire types. A wire type is the stored form of a value and its TypeScript types: `V`, the value that a
// reader gives, and `S`, the value that a sender gives. At run time a wire type holds one node of the kind tree.

import type { AnyWire, ValueOf } from "./infer.ts";
import {
  BIGINT_KIND,
  BOOLEAN_KIND,
  FLOAT_KIND,
  INT_KIND,
  type Kind,
  NULL_KIND,
  STRING_KIND,
} from "./kinds.ts";

/** Type-only key of the value type. It does not exist at run time. */
export declare const wireValue: unique symbol;

/** Type-only key of the sent type. It does not exist at run time. */
export declare const wireSent: unique symbol;

/** A wire type: `V` is the value that a reader gives, `S` the value that a sender gives. */
export interface Wire<V, S = V> {
  readonly [wireValue]: V;
  readonly [wireSent]: S;
}

class WireType<V, S> implements Wire<V, S> {
  declare readonly [wireValue]: V;
  declare readonly [wireSent]: S;
  readonly #kind: Kind;

  constructor(kind: Kind) {
    this.#kind = kind;
    Object.freeze(this);
  }

  static kindOf(wire: AnyWire): Kind {
    if (!(#kind in wire)) {
      throw new TypeError("kindOf: the value is not a wire type of horsies (build it with h.*)");
    }

    return wire.#kind;
  }
}

/** The kind tree of a wire type. A value that `h.*` did not build is a defect: `TypeError`. */
export function kindOf(wire: AnyWire): Kind {
  return WireType.kindOf(wire);
}

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
}

export const h: WireBuilders = Object.freeze({
  null: (): Wire<null> => new WireType<null, null>(NULL_KIND),
  boolean: (): Wire<boolean> => new WireType<boolean, boolean>(BOOLEAN_KIND),
  string: (): Wire<string> => new WireType<string, string>(STRING_KIND),
  int: (): Wire<number> => new WireType<number, number>(INT_KIND),
  bigint: (): Wire<bigint> => new WireType<bigint, bigint>(BIGINT_KIND),
  float: (): Wire<number> => new WireType<number, number>(FLOAT_KIND),
});

// The static helpers of `h`. A namespace with types only: no code at run time.
export declare namespace h {
  /** The value that a reader gives for a wire type: `h.Infer<typeof Order>`. */
  export type Infer<W extends AnyWire> = ValueOf<W>;
}
