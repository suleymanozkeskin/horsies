// A user of the entry point `horsies/zod`: the hz.* codecs and hz.wire() in a zod object.

import { h } from "horsies";
import { hz, type WireSchema, type ZodRule, type ZodSchemaRefused } from "horsies/zod";
import { z } from "zod";

type Equal<A, B> = (<T>() => T extends A ? 1 : 2) extends <T>() => T extends B ? 1 : 2 ? true : false;

function expectType<T extends true>(): T | undefined {
  return undefined;
}

const line = h.object({ sku: h.string(), count: h.int() });

export const event = z.object({
  at: hz.instant(),
  day: hz.plainDate(),
  total: hz.decimal(),
  line: hz.wire(line),
});

expectType<Equal<z.input<ReturnType<typeof hz.instant>>, string>>();

expectType<Equal<z.output<typeof event>["at"], Temporal.Instant>>();

expectType<Equal<z.output<typeof event>["line"], { sku: string; count: number }>>();

export const lineSchema: WireSchema<typeof line> = hz.wire(line);

export function refusalText(refusal: ZodSchemaRefused): ZodRule {
  return refusal.rule;
}
