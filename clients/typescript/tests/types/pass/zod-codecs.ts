// Compile-pass cases: the value types of the hz.* codecs and of hz.wire().

import { z } from "zod";

import { Decimal, h } from "../../../horsies/src/index.ts";
import { hz } from "../../../horsies/src/zod.ts";
import type { ZodSentOf } from "../../../horsies/src/zod/infer.ts";

type Equal<A, B> = (<T>() => T extends A ? 1 : 2) extends <T>() => T extends B ? 1 : 2 ? true : false;

function expectType<T extends true>(): T | undefined {
  return undefined;
}

const event = z.object({
  at: hz.instant(),
  local: hz.offsetDateTime(),
  wall: hz.plainDateTime(),
  day: hz.plainDate(),
  time: hz.plainTime(),
  total: hz.decimal(),
});

// A reader and a body get the Temporal and Decimal values.
expectType<
  Equal<
    z.output<typeof event>,
    {
      at: Temporal.Instant;
      local: Temporal.ZonedDateTime;
      wall: Temporal.PlainDateTime;
      day: Temporal.PlainDate;
      time: Temporal.PlainTime;
      total: Decimal;
    }
  >
>();

// zod's input side is the stored text.
expectType<Equal<z.input<ReturnType<typeof hz.instant>>, string>>();

// A sender gives the same values as a reader.
expectType<Equal<ZodSentOf<typeof event>, z.output<typeof event>>>();

const order = h.object({ id: h.uuid(), retries: h.int().default(3) });
const wrapped = z.object({ order: hz.wire(order), note: hz.wire(h.string()).optional() });

// hz.wire() gives the wire type's value to a reader, and takes its sent value from a sender.
expectType<Equal<z.output<typeof wrapped>, { order: { id: string; retries: number }; note?: string | undefined }>>();

expectType<Equal<ZodSentOf<typeof wrapped>, { order: { id: string; retries?: number }; note?: string }>>();

const total = Decimal.parse("19.90");

const sent: ZodSentOf<typeof event> | undefined = total.ok
  ? {
      at: Temporal.Instant.fromEpochMilliseconds(0),
      local: Temporal.ZonedDateTime.from("2026-08-12T09:10:12+02:00[+02:00]"),
      wall: Temporal.PlainDateTime.from("2026-08-12T09:10:12"),
      day: Temporal.PlainDate.from("2026-08-12"),
      time: Temporal.PlainTime.from("09:10:12"),
      total: total.value,
    }
  : undefined;

export { sent };
