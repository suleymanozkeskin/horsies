// A wire type: the stored form of a value and its TypeScript types. At run time it holds one node of the kind tree
// and how the value is present when it is an object member.

import { isFunction } from "../codec/values.ts";
import { encodeValue } from "../codec/writer.ts";
import type { AnyWire, ValueWire } from "./infer.ts";
import { type DefaultText, type Kind, type MemberPresence, nullableKind, OPTIONAL, REQUIRED } from "./kinds.ts";

/** Type-only key of the value type. It does not exist at run time. */
export declare const wireValue: unique symbol;

/** Type-only key of the sent type. It does not exist at run time. */
export declare const wireSent: unique symbol;

/** Type-only key of the presence. It does not exist at run time. */
export declare const wirePresence: unique symbol;

/** Type-only key of the role. It does not exist at run time. */
export declare const wireRole: unique symbol;

/** How an object member is present: always, `.optional()` (it may be left out), or `.default(v)`. */
export type Presence = "required" | "optional" | "defaulted";

/** A plain value, or an upstream outcome that only a workflow node gives (`h.taskOutcome`), as a whole parameter. */
export type Role = "value" | "taskOutcome";

/** A wire type: `V` is the value that a reader gives, `S` the value that a sender gives. */
export interface Wire<V, S = V, P extends Presence = "required", R extends Role = "value"> {
  readonly [wireValue]: V;
  readonly [wireSent]: S;
  readonly [wirePresence]: P;
  readonly [wireRole]: R;
  /** A member that a value may leave out. The key is left out of the stored text; it is never `null`. */
  optional(): Wire<V, S, "optional", R>;
  /** `null` or the value. */
  nullable(): Wire<V | null, S | null, P, R>;
  /**
   * A member that a sender may leave out; a reader in worker form fills `value`. A function (`() => crypto.randomUUID()`)
   * runs at each such decode, so each decode gets a new value (Python's `default_factory`). The sender never stores
   * the default.
   */
  default(value: S | (() => S)): Wire<V, S, "defaulted", R>;
}

class WireType<V, S, P extends Presence, R extends Role> implements Wire<V, S, P, R> {
  declare readonly [wireValue]: V;
  declare readonly [wireSent]: S;
  declare readonly [wirePresence]: P;
  declare readonly [wireRole]: R;
  readonly #kind: Kind;
  readonly #presence: MemberPresence;

  constructor(kind: Kind, presence: MemberPresence) {
    this.#kind = kind;
    this.#presence = presence;
    Object.freeze(this);
  }

  static kindOf(wire: AnyWire): Kind {
    return WireType.checked(wire).#kind;
  }

  static presenceOf(wire: AnyWire): MemberPresence {
    return WireType.checked(wire).#presence;
  }

  static checked(wire: AnyWire): WireType<unknown, unknown, Presence, Role> {
    if (!(#kind in wire)) {
      throw new TypeError("kindOf: the value is not a wire type of horsies (build it with h.*)");
    }

    return wire;
  }

  optional(): Wire<V, S, "optional", R> {
    return new WireType<V, S, "optional", R>(this.#kind, OPTIONAL);
  }

  nullable(): Wire<V | null, S | null, P, R> {
    return new WireType<V | null, S | null, P, R>(nullableKind(this.#kind), this.#presence);
  }

  default(value: S | (() => S)): Wire<V, S, "defaulted", R> {
    const fallback = isFunction(value) ? computedDefault(this.#kind, value) : defaultText(this.#kind, value);
    const presence: MemberPresence = Object.freeze({ mode: "defaulted", fallback });

    return new WireType<V, S, "defaulted", R>(this.#kind, presence);
  }
}

/** The stored text of a default value, or the writer's refusal, which `checkKind` reports. Pure. */
function defaultText<S>(kind: Kind, value: S): DefaultText {
  const written = encodeValue(kind, value);

  return Object.freeze(
    written.ok ? { state: "stored", text: written.value } : { state: "refused", refusal: written.error },
  );
}

/**
 * The default of a default function: the reader calls it at each worker-form decode and writes its value under
 * `kind`. The function does not run here, as Python does not call a `default_factory` at the definition. Pure.
 */
function computedDefault(kind: Kind, factory: () => unknown): DefaultText {
  return Object.freeze({ state: "computed", source: factory, compute: () => encodeValue(kind, factory()) });
}

/** A required wire type of `kind`. */
export function wireOf<V, S = V, R extends Role = "value">(kind: Kind): Wire<V, S, "required", R> {
  return new WireType<V, S, "required", R>(kind, REQUIRED);
}

/** A wire type of `kind` with `presence`, as the zod adapter compiles an object member or a parameter. */
export function memberWireOf(kind: Kind, presence: MemberPresence): ValueWire {
  return new WireType<unknown, unknown, Presence, "value">(kind, presence);
}

/** The kind tree of a wire type. A value that `h.*` did not build is a defect: `TypeError`. */
export function kindOf(wire: AnyWire): Kind {
  return WireType.kindOf(wire);
}

/** How a wire type is present as an object member. A value that `h.*` did not build is a defect: `TypeError`. */
export function presenceOf(wire: AnyWire): MemberPresence {
  return WireType.presenceOf(wire);
}
