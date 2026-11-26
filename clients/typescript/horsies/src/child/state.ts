// App state: `app.provide(factory)` returns a key; `runtime.state(key)` returns the value that the provider built in
// this child (or in this seat isolate). A key exists only with its provider, so `state` returns the value, not a
// Result. A key of another app is a defect.

import { raiseDefect } from "../native/call/defect.ts";
import { ClientDefect, type CoreDefect } from "../thrown-errors.ts";

/** The defect rule: in a worker process it writes stderr and the log sink, then aborts; else it throws. */
export type RaiseDefect = (defect: CoreDefect) => never;

/** Type-only key of the value type of a state key. It does not exist at run time. */
export declare const stateValueType: unique symbol;

/** Type-only key of the mark that only `app.provide` gives a key. It does not exist at run time. */
export declare const stateKeyMark: unique symbol;

/** A key of app state of any value type: a key that `app.provide` returned. */
export interface AnyStateKey {
  readonly kind: "stateKey";
  readonly name: string;
  readonly [stateKeyMark]: "StateKey";
}

/** The key of one provider of app state, whose value has the type `T`. `name` names the provider in logs. */
export interface StateKey<T> extends AnyStateKey {
  readonly [stateValueType]?: (value: T) => T;
}

/** The value type of the key `K`. */
export type StateValueOf<K extends AnyStateKey> = K extends StateKey<infer T> ? T : never;

/** A key as the store holds it: its identity and its name, with no value type. Every `StateKey<T>` is one. */
export type StateKeyIdentity = { readonly kind: "stateKey"; readonly name: string };

/** A new key named `name`. Frozen. Only `app.provide` calls it. */
export function stateKeyNamed<T>(name: string): StateKey<T> {
  // SAFETY: the mark and the value type are type-only facts: this function is the one maker of a key, and the
  // provider that the key belongs to builds a value of `T`.
  return Object.freeze({ kind: "stateKey", name }) as StateKey<T>;
}

/** The state of one value: its provider has not run in this process yet, or it ran and built the value. */
type StateSlot = { readonly kind: "notBuilt" } | { readonly kind: "built"; readonly value: unknown };

/** The values of the providers of one app in this process, by the identity of their keys. */
export class StateValues {
  readonly #slots = new Map<object, StateSlot>();
  readonly #raise: RaiseDefect;

  /** The values of the providers with the keys `keys`: none built yet. A defect goes to `raise`. */
  constructor(keys: readonly StateKeyIdentity[], raise: RaiseDefect = raiseDefect) {
    this.#raise = raise;

    for (const key of keys) {
      this.#slots.set(key, { kind: "notBuilt" });
    }
  }

  /**
   * Keep the value that the provider of `key` built. The provider's factory gave it, so it is of the key's value
   * type. A key of another app is a defect: `ClientDefect`.
   */
  keep<T>(key: StateKeyIdentity, value: T): void {
    if (!this.#slots.has(key)) {
      this.#raise(new ClientDefect({ subject: "provide", rule: `the state key ${key.name} is not a key of this app` }));
    }

    this.#slots.set(key, { kind: "built", value });
  }

  /**
   * The value of `key`. A key of another app, or of a provider that has not run in this process, is a defect:
   * `ClientDefect` (the providers run before the child reports ready, so a body never meets one).
   */
  value<T>(key: StateKey<T>): T {
    const slot = this.#slots.get(key);

    if (slot === undefined) {
      return this.#raise(new ClientDefect({ subject: "runtime.state", rule: `the state key ${key.name} belongs to another app` }));
    }

    switch (slot.kind) {
      case "built":
        // SAFETY: `keep` is the only writer, and it takes a value of the key's type `T`.
        return slot.value as T;
      case "notBuilt":
        return this.#raise(
          new ClientDefect({ subject: "runtime.state", rule: `the provider of ${key.name} has not run in this process` }),
        );
      default: {
        const unexpected: never = slot;

        throw new ClientDefect({ subject: "exhaustive switch", rule: `a state slot outside its type: ${String(unexpected)}` });
      }
    }
  }
}
