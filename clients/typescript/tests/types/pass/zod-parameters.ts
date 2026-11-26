// Compile-pass cases: one task input that mixes h.* wire types and zod schemas.

import { z } from "zod";

import { h } from "../../../horsies/src/index.ts";
import type { EmptyObject } from "../../../horsies/src/index.ts";
import type { ParametersSent, ParametersValue } from "../../../horsies/src/zod/infer.ts";

type Equal<A, B> = (<T>() => T extends A ? 1 : 2) extends <T>() => T extends B ? 1 : 2 ? true : false;

function expectType<T extends true>(): T | undefined {
  return undefined;
}

const input = {
  orderId: h.uuid(),
  priority: h.int().default(5),
  note: h.string().optional(),
  order: z.object({ sku: z.string(), quantity: z.int().default(1) }),
  channel: z.enum(["mail", "sms"]).default("mail"),
  memo: z.string().optional(),
};

// A body gets every parameter; an optional one may be absent; a defaulted one is filled.
expectType<
  Equal<
    ParametersValue<typeof input>,
    {
      orderId: string;
      priority: number;
      order: { sku: string; quantity: number };
      channel: "mail" | "sms";
      note?: string;
      memo?: string | undefined;
    }
  >
>();

// A sender may leave out an optional and a defaulted parameter, of either kind, at every depth.
expectType<
  Equal<
    ParametersSent<typeof input>,
    {
      orderId: string;
      order: { sku: string; quantity?: number };
      priority?: number;
      note?: string;
      channel?: "mail" | "sms";
      memo?: string;
    }
  >
>();

// A task with no parameters takes only {}.
expectType<Equal<ParametersSent<{}>, EmptyObject>>();

const sent: ParametersSent<typeof input> = { orderId: "00000000-0000-4000-8000-000000000000", order: { sku: "a" } };

export { sent };
