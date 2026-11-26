// Compile-pass cases: the value that a sender gives for a zod schema, against the value that a reader gets.

import { z } from "zod";

import type { EmptyObject } from "../../../horsies/src/index.ts";
import type { ZodSentOf, ZodValueOf } from "../../../horsies/src/zod/infer.ts";

type Equal<A, B> = (<T>() => T extends A ? 1 : 2) extends <T>() => T extends B ? 1 : 2 ? true : false;

function expectType<T extends true>(): T | undefined {
  return undefined;
}

const order = z.object({
  id: z.int(),
  note: z.string().optional(),
  retries: z.int().default(3),
  channel: z.enum(["mail", "sms"]).prefault("mail"),
  parent: z.int().nullable(),
  lines: z.array(z.object({ sku: z.string(), quantity: z.int().default(1) })),
  pair: z.tuple([z.string(), z.number()]),
  counts: z.record(z.string(), z.bigint()),
  flags: z.partialRecord(z.enum(["a", "b"]), z.boolean()),
  kind: z.union([z.literal("x"), z.int()]),
  tags: z.array(z.string()).readonly(),
});

// A reader gets each default filled; an optional member may be absent.
expectType<
  Equal<
    ZodValueOf<typeof order>,
    {
      id: number;
      note?: string | undefined;
      retries: number;
      channel: "mail" | "sms";
      parent: number | null;
      lines: { sku: string; quantity: number }[];
      pair: [string, number];
      counts: Record<string, bigint>;
      flags: Partial<Record<"a" | "b", boolean>>;
      kind: "x" | number;
      tags: readonly string[];
    }
  >
>();

// A sender may leave out an optional, a defaulted and a prefaulted member, at every depth; it never gives undefined.
expectType<
  Equal<
    ZodSentOf<typeof order>,
    {
      id: number;
      note?: string;
      retries?: number;
      channel?: "mail" | "sms";
      parent: number | null;
      lines: { sku: string; quantity?: number }[];
      pair: [string, number];
      counts: { [key: string]: bigint };
      flags: { a?: boolean; b?: boolean };
      kind: "x" | number;
      tags: readonly string[];
    }
  >
>();

const empty = z.object({});

// An object with no members takes only `{}`, as h.object({}).
expectType<Equal<ZodSentOf<typeof empty>, EmptyObject>>();

const sent: ZodSentOf<typeof order> = {
  id: 1,
  parent: null,
  lines: [{ sku: "a" }],
  pair: ["a", 1.5],
  counts: {},
  flags: { a: true },
  kind: "x",
  tags: [],
};

export { sent };
