// Compile-pass cases: a recursive wire type (h.lazy), a tuple with a rest item, and a default function.

import { h, type Wire } from "../../../horsies/src/index.ts";
import type { SentOf } from "../../../horsies/src/wire/infer.ts";

type Equal<A, B> = (<T>() => T extends A ? 1 : 2) extends <T>() => T extends B ? 1 : 2 ? true : false;

function expectType<T extends true>(): T | undefined {
  return undefined;
}

type Category = { name: string; children: Category[]; parent?: Category | null };

// A recursive constant has its declared type; h.lazy gives the type of the wire type that its getter returns.
const category: Wire<Category> = h.object({
  name: h.string(),
  children: h.array(h.lazy(() => category)),
  parent: h.lazy(() => category).nullable().optional(),
});

expectType<Equal<h.Infer<typeof category>, Category>>();

const children = h.array(h.lazy(() => category));

expectType<Equal<h.Infer<typeof children>, Category[]>>();

// A tuple with a rest item: the fixed items, then any number of rest items.
const row = h.tuple([h.int(), h.string()], h.boolean());

expectType<Equal<h.Infer<typeof row>, [number, string, ...boolean[]]>>();
expectType<Equal<SentOf<typeof row>, [number, string, ...boolean[]]>>();

const flags = h.tuple([], h.boolean());

expectType<Equal<h.Infer<typeof flags>, boolean[]>>();

// A default function gives a value of the sent type; the member is defaulted.
const item = h.object({ id: h.uuid().default(() => crypto.randomUUID()), name: h.string() });

expectType<Equal<h.Infer<typeof item>, { id: string; name: string }>>();
expectType<Equal<SentOf<typeof item>, { name: string; id?: string }>>();

const rows: h.Infer<typeof row>[] = [[1, "a"], [2, "b", true, false]];

export { rows };
