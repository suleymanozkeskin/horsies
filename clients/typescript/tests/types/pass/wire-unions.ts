// Compile-pass cases: the value types of literals, enums and the two union forms.

import { h } from "../../../horsies/src/index.ts";

type Equal<A, B> = (<T>() => T extends A ? 1 : 2) extends <T>() => T extends B ? 1 : 2 ? true : false;

function expectType<T extends true>(): T | undefined {
  return undefined;
}

const level = h.literal(1, 3);
const color = h.enum(["red", "green"]);
const amount = h.union([h.string(), h.int(), h.null()]);

const payment = h.union("method", [
  h.object({ method: h.literal("card"), last4: h.string() }),
  h.object({ method: h.literal("bank"), iban: h.string() }),
]);

expectType<Equal<h.Infer<typeof level>, 1 | 3>>();

expectType<Equal<h.Infer<typeof color>, "red" | "green">>();

expectType<Equal<h.Infer<typeof amount>, string | number | null>>();

expectType<
  Equal<h.Infer<typeof payment>, { method: "card"; last4: string } | { method: "bank"; iban: string }>
>();

/** A tagged union narrows on its tag. */
export function last4Of(value: h.Infer<typeof payment>): string {
  switch (value.method) {
    case "card":
      return value.last4;
    case "bank":
      return value.iban.slice(-4);
  }
}
