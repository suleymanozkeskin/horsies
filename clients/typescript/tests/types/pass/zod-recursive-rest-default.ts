// Compile-pass cases: the value and the sent value of a recursive zod schema, a tuple with a rest item and a function
// default, read from the schema's type with no zod import.

import { z } from "zod";

import type { ZodSentOf, ZodValueOf } from "../../../horsies/src/zod/infer.ts";

type Equal<A, B> = (<T>() => T extends A ? 1 : 2) extends <T>() => T extends B ? 1 : 2 ? true : false;

function expectType<T extends true>(): T | undefined {
  return undefined;
}

type Tree = { name: string; children: Tree[] };

const tree: z.ZodType<Tree> = z.lazy(() => z.object({ name: z.string(), children: z.array(tree) }));

expectType<Equal<ZodValueOf<typeof tree>, Tree>>();

const row = z.tuple([z.int(), z.string()], z.boolean());

expectType<Equal<ZodValueOf<typeof row>, [number, string, ...boolean[]]>>();
expectType<Equal<ZodSentOf<typeof row>, [number, string, ...boolean[]]>>();

const item = z.object({ id: z.uuid().default(() => crypto.randomUUID()), name: z.string() });

expectType<Equal<ZodValueOf<typeof item>, { id: string; name: string }>>();
expectType<Equal<ZodSentOf<typeof item>, { name: string; id?: string }>>();

// A getter member that returns a schema of the object itself (zod's recursive object form).
const category = z.object({
  name: z.string(),
  get children(): z.ZodArray<typeof category> {
    return z.array(category);
  },
});

type CategoryValue = ZodValueOf<typeof category>;

expectType<Equal<CategoryValue["name"], string>>();
expectType<Equal<CategoryValue["children"][number]["children"][number]["name"], string>>();
