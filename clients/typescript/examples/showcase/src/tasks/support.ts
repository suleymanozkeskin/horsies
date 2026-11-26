// What the implementations share: the key of the store that the app provides in each task child, and the parts of a
// `STORE_UNAVAILABLE` error.

import type { JsonValue, StateKey } from "horsies";

import { type Store, type StoreFailure, storeFailureText } from "../store/connection.ts";

/** The key of the Acme store of a task child: `runtime.state(key)` gives the store of the process. */
export type StoreKey = StateKey<Store>;

/** The message and the data of a `STORE_UNAVAILABLE` error for `failure`. Pure. */
export function storeFailure(failure: StoreFailure): { readonly message: string; readonly data: JsonValue } {
  return { message: storeFailureText(failure), data: { operation: failure.operation } };
}
