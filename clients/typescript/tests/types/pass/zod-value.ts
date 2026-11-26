// Compile-pass cases: the value that a reader gives for a zod schema, read from the schema's type with no zod import.
// It is zod's output type, except that z.json() gives JsonValue (with bigint), at every depth.

import { z } from "zod";

import type { JsonValue } from "../../../horsies/src/index.ts";
import { hz } from "../../../horsies/src/zod.ts";
import type { ZodValueOf } from "../../../horsies/src/zod/infer.ts";

type Equal<A, B> = (<T>() => T extends A ? 1 : 2) extends <T>() => T extends B ? 1 : 2 ? true : false;

function expectType<T extends true>(): T | undefined {
  return undefined;
}

const order = z.object({
  id: z.int(),
  note: z.string().optional(),
  retries: z.int().default(3),
  parent: z.int().nullable(),
  lines: z.array(z.object({ sku: z.string(), quantity: z.int().default(1) })),
  pair: z.tuple([z.string(), z.number()]),
  counts: z.record(z.string(), z.bigint()),
  flags: z.partialRecord(z.enum(["a", "b"]), z.boolean()),
  kind: z.union([z.literal("x"), z.int()]),
  tags: z.array(z.string()).readonly(),
  at: hz.instant(),
  later: z.lazy(() => z.object({ size: z.int() })),
  price: z.codec(z.int(), z.custom<{ cents: number }>(), { decode: (cents) => ({ cents }), encode: (v) => v.cents }),
});

// Without z.json(), the value is zod's output type.
expectType<Equal<ZodValueOf<typeof order>, z.output<typeof order>>>();

// z.json() gives JsonValue, which holds a bigint for an integer outside the safe range.
expectType<Equal<ZodValueOf<ReturnType<typeof z.json>>, JsonValue>>();

const event = z.object({ data: z.json(), batch: z.array(z.json()).optional() });

expectType<Equal<ZodValueOf<typeof event>, { data: JsonValue; batch?: JsonValue[] | undefined }>>();

const big: ZodValueOf<typeof event> = { data: { n: 12_345_678_901_234_567_890n } };

export { big };
