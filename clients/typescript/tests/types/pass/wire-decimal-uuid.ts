// Compile-pass cases: the value types of h.decimal() and h.uuid().

import { Decimal, h } from "../../../horsies/src/index.ts";

type Equal<A, B> = (<T>() => T extends A ? 1 : 2) extends <T>() => T extends B ? 1 : 2 ? true : false;

function expectType<T extends true>(): T | undefined {
  return undefined;
}

const payment = h.object({ id: h.uuid(), total: h.decimal() });

expectType<Equal<h.Infer<typeof payment>, { id: string; total: Decimal }>>();

const parsed = Decimal.parse("19.90");

if (parsed.ok) {
  expectType<Equal<typeof parsed.value, Decimal>>();
}
