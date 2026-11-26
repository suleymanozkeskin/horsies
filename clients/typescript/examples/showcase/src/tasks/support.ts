// What the implementations share: the key of the store that the app provides in each task child, the result
// constructors that a function of a body takes, and the parts of a `STORE_UNAVAILABLE` error.

import type { JsonValue, OutputSchema, ResultConstructors, StateKey, TaskContract } from "horsies";

import { type Store, type StoreFailure, storeFailureText } from "../store/connection.ts";

/** The key of the Acme store of a task child: `runtime.state(key)` gives the store of the process. */
export type StoreKey = StateKey<Store>;

/**
 * The result constructors of a body of the task contract `C`: its `ok` and `err`. A function that builds the result
 * of a body takes them from the body's second argument; a test gives its own.
 */
export type ResultsOf<C> =
  C extends TaskContract<infer _P, infer O extends OutputSchema, infer E extends string, infer _X, infer _C>
    ? ResultConstructors<O, E>
    : never;

/** The message and the data of a `STORE_UNAVAILABLE` error for `failure`. Pure. */
export function storeFailure(failure: StoreFailure): { readonly message: string; readonly data: JsonValue } {
  return { message: storeFailureText(failure), data: { operation: failure.operation } };
}
