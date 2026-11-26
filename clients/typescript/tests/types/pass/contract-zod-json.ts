// Compile-pass cases: the value of `z.json()` in a contract, with the `undefined` test of `IsZodJson` (no top type in
// the public declarations).

import { z } from "zod";

import type { BodyInput, JsonValue, ZodSchemaLike, ZodValueOf } from "../../../horsies/src/index.ts";

type Equal<A, B> = (<T>() => T extends A ? 1 : 2) extends <T>() => T extends B ? 1 : 2 ? true : false;

function expectType<T extends true>(): T | undefined {
  return undefined;
}

const json = z.json();
const optionalJson = z.json().optional();
const untyped = z.unknown();

// `z.json()` reads as `JsonValue` (an integer outside the safe range is a `bigint`).
expectType<Equal<ZodValueOf<typeof json>, JsonValue>>();

// An optional `z.json()` member keeps `undefined`.
expectType<Equal<ZodValueOf<typeof optionalJson>, JsonValue | undefined>>();

// zod's output of `z.unknown()` is not read as `JsonValue` (the adapter refuses it at `Horsies.create`).
expectType<Equal<ZodValueOf<typeof untyped> extends JsonValue ? true : false, false>>();

// A zod schema fits the parameter constraint, which names only the definition type.
const schema: ZodSchemaLike = json;

expectType<Equal<BodyInput<{ data: typeof json; note: typeof optionalJson }>, { data: JsonValue; note?: JsonValue | undefined }>>();

export { schema };
