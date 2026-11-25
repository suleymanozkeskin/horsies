// Compile-pass cases: the value and sent types of objects, arrays, tuples, records and the modifiers.

import { h } from "../../../horsies/src/index.ts";
import type { SentOf } from "../../../horsies/src/wire/infer.ts";

type Equal<A, B> = (<T>() => T extends A ? 1 : 2) extends <T>() => T extends B ? 1 : 2 ? true : false;

function expectType<T extends true>(): T | undefined {
  return undefined;
}

const order = h.object({
  id: h.int(),
  note: h.string().optional(),
  retries: h.int().default(3),
  parent: h.int().nullable(),
});

// A reader gets the defaulted member filled; the optional member may be absent.
expectType<Equal<h.Infer<typeof order>, { id: number; retries: number; parent: number | null; note?: string }>>();

// A sender may leave out the optional and the defaulted members.
expectType<Equal<SentOf<typeof order>, { id: number; parent: number | null; note?: string; retries?: number }>>();

const lines = h.array(h.tuple([h.string(), h.float()]));

expectType<Equal<h.Infer<typeof lines>, [string, number][]>>();

const counts = h.record(h.bigint());

expectType<Equal<h.Infer<typeof counts>, Readonly<Record<string, bigint>>>>();

const sent: SentOf<typeof order> = { id: 1, parent: null };

export { sent };
