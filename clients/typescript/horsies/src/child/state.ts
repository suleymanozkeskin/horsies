// App state: `app.provide(factory)` returns a key; `runtime.state(key)` returns the value that the provider built in
// this child (or in this seat isolate). A key exists only with its provider, so `state` returns the value, not a
// Result. A key of another app is a defect.

import { ClientDefect } from "../thrown-errors.ts";

/** Type-only key of the value type of a state key. It does not exist at run time. */
export declare const stateValueType: unique symbol;

/** The key of one provider of app state. `name` names the provider in logs and in defects. */
export interface StateKey<T> {
  readonly kind: "stateKey";
  readonly name: string;
  readonly [stateValueType]?: (value: T) => T;
}

/** A key as the store holds it: its identity and its name, with no value type. Every `StateKey<T>` is one. */
export type StateKeyIdentity = { readonly kind: "stateKey"; readonly name: string };

/** A new key named `name`. Frozen. */
export function stateKeyNamed<T>(name: string): StateKey<T> {
  return Object.freeze({ kind: "stateKey", name });
}

/** The state of one value: its provider has not run in this process yet, or it ran and built the value. */
type StateSlot = { readonly kind: "notBuilt" } | { readonly kind: "built"; readonly value: unknown };

/** The values of the providers of one app in this process, by the identity of their keys. */
export class StateValues {
  readonly #slots = new Map<object, StateSlot>();

  /** The values of the providers with the keys `keys`: none built yet. */
  constructor(keys: readonly StateKeyIdentity[]) {
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
      throw new ClientDefect({ subject: "provide", rule: `the state key ${key.name} is not a key of this app` });
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
      throw new ClientDefect({ subject: "runtime.state", rule: `the state key ${key.name} belongs to another app` });
    }

    switch (slot.kind) {
      case "built":
        // SAFETY: `keep` is the only writer, and it takes a value of the key's type `T`.
        return slot.value as T;
      case "notBuilt":
        throw new ClientDefect({ subject: "runtime.state", rule: `the provider of ${key.name} has not run in this process` });
      default: {
        const unexpected: never = slot;

        throw new ClientDefect({ subject: "exhaustive switch", rule: `a state slot outside its type: ${String(unexpected)}` });
      }
    }
  }
}
