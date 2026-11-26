// Compile-pass cases: the value type of h.json().

import { h, type JsonValue } from "../../../horsies/src/index.ts";

type Equal<A, B> = (<T>() => T extends A ? 1 : 2) extends <T>() => T extends B ? 1 : 2 ? true : false;

function expectType<T extends true>(): T | undefined {
  return undefined;
}

const payload = h.json();

expectType<Equal<h.Infer<typeof payload>, JsonValue>>();

/** A JsonValue holds a bigint for an integer outside the safe range. */
export const large: JsonValue = { count: 2n ** 64n, items: [1, "a", null, true] };
