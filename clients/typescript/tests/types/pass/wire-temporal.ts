// Compile-pass cases: the value type of each date-time wire type.

import { h } from "../../../horsies/src/index.ts";

type Equal<A, B> = (<T>() => T extends A ? 1 : 2) extends <T>() => T extends B ? 1 : 2 ? true : false;

function expectType<T extends true>(): T | undefined {
  return undefined;
}

const event = h.object({
  at: h.instant(),
  local: h.offsetDateTime(),
  wall: h.plainDateTime(),
  day: h.plainDate(),
});

expectType<
  Equal<
    h.Infer<typeof event>,
    {
      at: Temporal.Instant;
      local: Temporal.ZonedDateTime;
      wall: Temporal.PlainDateTime;
      day: Temporal.PlainDate;
    }
  >
>();
