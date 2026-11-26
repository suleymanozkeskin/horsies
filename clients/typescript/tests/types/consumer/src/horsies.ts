// A user of the entry point `horsies`: wire types, their value and sent types, Decimal, Result and TaskOutcome.

import { Decimal, h, type JsonValue, type Result, type TaskOutcome, type Wire } from "horsies";

type Equal<A, B> = (<T>() => T extends A ? 1 : 2) extends <T>() => T extends B ? 1 : 2 ? true : false;

function expectType<T extends true>(): T | undefined {
  return undefined;
}

const order = h.object({
  id: h.uuid(),
  placedAt: h.instant(),
  total: h.decimal(),
  lines: h.array(h.object({ sku: h.string(), count: h.int() })),
  note: h.string().optional(),
  retries: h.int().default(3),
  extra: h.json(),
  tags: h.record(h.string()),
  state: h.enum(["open", "closed"]),
});

type Order = h.Infer<typeof order>;

expectType<
  Equal<
    Order,
    {
      id: string;
      placedAt: Temporal.Instant;
      total: Decimal;
      lines: { sku: string; count: number }[];
      note?: string;
      retries: number;
      extra: JsonValue;
      tags: Readonly<Record<string, string>>;
      state: "open" | "closed";
    }
  >
>();

/** A user's helper over any required wire type. */
export function describeWire<V>(wire: Wire<V>): Wire<V | null> {
  return wire.nullable();
}

export function totalText(text: string): string {
  const parsed: Result<Decimal, unknown> = Decimal.parse(text);

  return parsed.ok ? parsed.value.toString() : "not a decimal";
}

export function outcomeText(outcome: TaskOutcome<Order, "OUT_OF_STOCK">): string {
  switch (outcome.kind) {
    case "succeeded":
      return outcome.value.id;
    case "failed":
      return outcome.error.code.kind === "user" ? outcome.error.code.code : outcome.error.code.code;
  }
}

export const nullableCount: Wire<number | null> = describeWire(h.int());
